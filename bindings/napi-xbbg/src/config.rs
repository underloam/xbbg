//! N-API input/output configuration and diagnostic structs.

use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::{
    Buffer, Either, Error, FromNapiValue, Status, ToNapiValue, TypeName, Uint8Array,
    ValidateNapiValue,
};
use napi_derive::napi;
use xbbg_async::engine::{EngineConfig, TlsMaterial};
use xbbg_async::{FeedInfo, SessionConnections};
use xbbg_host::config::{
    AuthConfigSpec, EngineConfigSpec, RetryPolicySpec, ServerAddressSpec, Socks5Spec,
    TlsConfigSpec, TlsMaterialSpec,
};

use crate::errors::host_error_to_napi;

pub(crate) fn to_i64_saturating(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}

#[napi(object)]
pub struct StringPair {
    pub key: String,
    pub value: String,
}

#[napi(object)]
pub struct SecurityOverridesInput {
    pub security: String,
    pub overrides: Vec<StringPair>,
}

#[napi(object)]
pub struct ServerAddressInput {
    pub host: String,
    pub port: u16,
}

#[napi(object)]
pub struct AuthConfigInput {
    /// Auth method: "none", "user", "app", "userapp", "dir", "manual", or "token".
    /// Omit `auth` for the default (no auth). Also accepts "directory" as an alias for "dir" and
    /// the empty string as "none".
    pub method: String,
    pub app_name: Option<String>,
    pub dir_property: Option<String>,
    pub user_id: Option<String>,
    pub ip_address: Option<String>,
    pub token: Option<String>,
}

#[napi(object)]
pub struct TlsConfigInput {
    /// File path, or DER PKCS#12 bytes. Must match the representation of trustMaterial.
    #[napi(ts_type = "string | Buffer | Uint8Array")]
    pub client_credentials: Option<TlsMaterialInput>,
    pub client_credentials_password: Option<String>,
    /// File path, or DER PKCS#7 bytes. Required together with clientCredentials.
    #[napi(ts_type = "string | Buffer | Uint8Array")]
    pub trust_material: Option<TlsMaterialInput>,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
}

/// Own TLS material while extracting synchronous JS arguments, before an async
/// factory can move its configuration to Tokio. No JS memory survives this boundary.
pub struct TlsMaterialInput(pub(crate) TlsMaterial);

impl TlsMaterialInput {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self(TlsMaterial::Blob(Arc::from(bytes)))
    }

    /// The host takes the owned material as is: a blob is shared, never re-encoded.
    fn into_spec(self) -> TlsMaterialSpec {
        TlsMaterialSpec::ready(self.0)
    }
}

impl TypeName for TlsMaterialInput {
    fn type_name() -> &'static str {
        "string | Buffer | Uint8Array"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

impl ValidateNapiValue for TlsMaterialInput {
    unsafe fn validate(
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
    ) -> napi::Result<napi::sys::napi_value> {
        unsafe { Either::<String, Uint8Array>::validate(env, value) }
    }
}

impl FromNapiValue for TlsMaterialInput {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
    ) -> napi::Result<Self> {
        if unsafe { String::validate(env, value) }.is_ok() {
            return unsafe { String::from_napi_value(env, value) }
                .map(|path| Self(TlsMaterial::Path(path)));
        }

        let mut is_typed_array = false;
        napi::check_status!(unsafe {
            napi::sys::napi_is_typedarray(env, value, &mut is_typed_array)
        })?;
        if !is_typed_array {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS material must be a string or Buffer/Uint8Array",
            ));
        }

        let mut array_type = 0;
        let mut len = 0;
        let mut data = std::ptr::null_mut();
        let mut backing = std::ptr::null_mut();
        napi::check_status!(
            unsafe {
                napi::sys::napi_get_typedarray_info(
                    env,
                    value,
                    &mut array_type,
                    &mut len,
                    &mut data,
                    &mut backing,
                    std::ptr::null_mut(),
                )
            },
            "TLS material must use a non-detached ArrayBuffer, not SharedArrayBuffer",
        )?;
        if array_type != napi::sys::TypedarrayType::uint8_array {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS blob material must be a Buffer or Uint8Array",
            ));
        }
        if backing.is_null() {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS material must not use detached ArrayBuffer backing",
            ));
        }
        let mut is_array_buffer = false;
        napi::check_status!(unsafe {
            napi::sys::napi_is_arraybuffer(env, backing, &mut is_array_buffer)
        })?;
        if !is_array_buffer {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS material must not use SharedArrayBuffer backing",
            ));
        }
        let mut is_detached = false;
        napi::check_status!(unsafe {
            napi::sys::napi_is_detached_arraybuffer(env, backing, &mut is_detached)
        })?;
        if is_detached || (len != 0 && data.is_null()) {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS material must not use detached ArrayBuffer backing",
            ));
        }
        if len > i32::MAX as usize {
            return Err(Error::new(
                Status::InvalidArg,
                "TLS blob exceeds the SDK maximum length",
            ));
        }

        // No JS callbacks occur between checking this non-shared backing and
        // copying the exact view. In particular, do not construct a Rust slice
        // over SharedArrayBuffer memory that another JS worker can mutate.
        let bytes = if len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }
        };
        Ok(Self::from_bytes(bytes))
    }
}

impl ToNapiValue for TlsMaterialInput {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        match value.0 {
            TlsMaterial::Path(path) => unsafe { String::to_napi_value(env, path) },
            TlsMaterial::Blob(blob) => unsafe {
                Buffer::to_napi_value(env, Buffer::from(blob.as_ref().to_vec()))
            },
        }
    }
}

#[napi(object)]
pub struct RetryPolicyInput {
    pub max_retries: Option<u32>,
    pub initial_delay_ms: Option<i64>,
    pub backoff_factor: Option<f64>,
    pub max_delay_ms: Option<i64>,
}

#[napi(object)]
pub struct Socks5ConfigInput {
    pub host: String,
    pub port: u16,
}

#[napi(object)]
pub struct EngineConfigInput {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub servers: Option<Vec<ServerAddressInput>>,
    /// ZFP remote: "8194" or "8196". Default: unset (direct transport).
    pub zfp_remote: Option<String>,
    pub request_pool_size: Option<u32>,
    pub subscription_pool_size: Option<u32>,
    pub runtime_worker_threads: Option<u32>,
    pub max_subscription_sessions: Option<u32>,
    pub shard_requests: Option<bool>,
    pub shard_threshold: Option<u32>,
    pub shard_chunk_size: Option<u32>,
    pub shard_max_concurrent: Option<u32>,
    /// Validation mode: "disabled" (default), "lenient", or "strict".
    /// Also accepts "off" and "none" as aliases for "disabled".
    pub validation_mode: Option<String>,
    /// Result column holding each row's security: "ticker" (default, deprecated)
    /// or "security", the default from the next major release.
    pub key_column: Option<String>,
    pub subscription_flush_threshold: Option<u32>,
    pub max_event_queue_size: Option<u32>,
    pub command_queue_size: Option<u32>,
    pub subscription_stream_capacity: Option<u32>,
    /// Overflow policy: "drop_newest" (default), "block", or gap-aware "drop_oldest".
    /// Also accepts "dropnewest" as an alias for "drop_newest".
    pub overflow_policy: Option<String>,
    pub warmup_services: Option<Vec<String>>,
    pub field_cache_path: Option<String>,
    /// Default BQL clientContext.appName; independent of session authentication.
    pub bql_app_name: Option<String>,
    pub auth: Option<AuthConfigInput>,
    pub tls: Option<TlsConfigInput>,
    pub num_start_attempts: Option<u32>,
    pub auto_restart_on_disconnection: Option<bool>,
    pub retry_policy: Option<RetryPolicyInput>,
    pub request_timeout_ms: Option<i64>,
    pub streams_deactivated_warn_ms: Option<i64>,
    pub reconnect_grace_ms: Option<i64>,
    pub keep_alive_enabled: Option<bool>,
    pub keep_alive_inactivity_ms: Option<i32>,
    pub keep_alive_response_timeout_ms: Option<i32>,
    pub slow_consumer_hi_water_mark: Option<f64>,
    pub slow_consumer_lo_water_mark: Option<f64>,
    /// Bloomberg SDK log level: "off" (default), "fatal", "error", "warn", "info", "debug",
    /// or "trace". Also accepts "warning" as an alias for "warn".
    pub sdk_log_level: Option<String>,
    pub socks5: Option<Socks5ConfigInput>,
}

#[napi(object)]
pub struct RequestInput {
    pub service: String,
    pub operation: String,
    pub request_operation: Option<String>,
    pub request_id: Option<String>,
    pub extractor: Option<String>,
    pub securities: Option<Vec<String>>,
    pub security: Option<String>,
    pub fields: Option<Vec<String>>,
    pub overrides: Option<Vec<StringPair>>,
    pub security_overrides: Option<Vec<SecurityOverridesInput>>,
    pub elements: Option<Vec<StringPair>>,
    pub kwargs: Option<Vec<StringPair>>,
    pub json_elements: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_datetime: Option<String>,
    pub end_datetime: Option<String>,
    pub request_tz: Option<String>,
    pub output_tz: Option<String>,
    pub event_type: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub interval: Option<u32>,
    pub options: Option<Vec<StringPair>>,
    pub field_types: Option<Vec<StringPair>>,
    pub include_security_errors: Option<bool>,
    pub return_eids: Option<bool>,
    /// Delayed-data policy: "warn", "raise", or "ignore". Omitted skips the check.
    pub on_delayed: Option<String>,
    pub validate_fields: Option<bool>,
    pub search_spec: Option<String>,
    pub field_ids: Option<Vec<String>>,
    pub format: Option<String>,
}

#[napi(object)]
pub struct FieldInfoOutput {
    pub field_id: String,
    pub arrow_type: String,
    pub description: String,
    pub category: String,
}

#[napi(object)]
pub struct FeedInfoOutput {
    pub service: String,
    pub topic: String,
    pub options: Vec<String>,
    pub fields: Vec<String>,
    pub consumers: i64,
    pub delayed: Option<bool>,
    pub state: String,
    pub isolated: bool,
    pub field_errors: HashMap<String, String>,
}

#[napi(object)]
pub struct SessionConnectionsOutput {
    pub pool: String,
    pub worker: i64,
    pub servers: Vec<String>,
}

#[napi(object)]
pub struct EntitlementReport {
    pub entitled: bool,
    pub failed_eids: Vec<i32>,
}

impl From<FeedInfo> for FeedInfoOutput {
    fn from(feed: FeedInfo) -> Self {
        Self {
            service: feed.service,
            topic: feed.topic,
            options: feed.options,
            fields: feed.fields,
            consumers: to_i64_saturating(feed.consumers as u64),
            delayed: feed.delayed,
            state: feed.state,
            isolated: feed.isolated,
            field_errors: feed.field_errors,
        }
    }
}

impl From<SessionConnections> for SessionConnectionsOutput {
    fn from(session: SessionConnections) -> Self {
        Self {
            pool: session.pool.as_str().to_string(),
            worker: to_i64_saturating(session.worker_id as u64),
            servers: session.servers,
        }
    }
}

impl From<EngineConfigInput> for EngineConfigSpec {
    fn from(input: EngineConfigInput) -> Self {
        Self {
            host: input.host,
            port: input.port,
            servers: input.servers.map(|servers| {
                servers
                    .into_iter()
                    .map(|server| ServerAddressSpec {
                        host: server.host,
                        port: server.port,
                    })
                    .collect()
            }),
            zfp_remote: input.zfp_remote,
            request_pool_size: input.request_pool_size,
            subscription_pool_size: input.subscription_pool_size,
            runtime_worker_threads: input.runtime_worker_threads,
            max_subscription_sessions: input.max_subscription_sessions,
            shard_requests: input.shard_requests,
            shard_threshold: input.shard_threshold,
            shard_chunk_size: input.shard_chunk_size,
            shard_max_concurrent: input.shard_max_concurrent,
            validation_mode: input.validation_mode,
            key_column: input.key_column,
            subscription_flush_threshold: input.subscription_flush_threshold,
            max_event_queue_size: input.max_event_queue_size,
            command_queue_size: input.command_queue_size,
            subscription_stream_capacity: input.subscription_stream_capacity,
            overflow_policy: input.overflow_policy,
            warmup_services: input.warmup_services,
            field_cache_path: input.field_cache_path,
            bql_app_name: input.bql_app_name,
            auth: input.auth.map(|auth| AuthConfigSpec {
                method: auth.method,
                app_name: auth.app_name,
                dir_property: auth.dir_property,
                user_id: auth.user_id,
                ip_address: auth.ip_address,
                token: auth.token,
            }),
            tls: input.tls.map(|tls| TlsConfigSpec {
                client_credentials: tls.client_credentials.map(TlsMaterialInput::into_spec),
                client_credentials_password: tls.client_credentials_password,
                trust_material: tls.trust_material.map(TlsMaterialInput::into_spec),
                handshake_timeout_ms: tls.handshake_timeout_ms,
                crl_fetch_timeout_ms: tls.crl_fetch_timeout_ms,
            }),
            num_start_attempts: input.num_start_attempts,
            auto_restart_on_disconnection: input.auto_restart_on_disconnection,
            retry_policy: input.retry_policy.map(|retry| RetryPolicySpec {
                max_retries: retry.max_retries,
                initial_delay_ms: retry.initial_delay_ms,
                backoff_factor: retry.backoff_factor,
                max_delay_ms: retry.max_delay_ms,
            }),
            request_timeout_ms: input.request_timeout_ms,
            streams_deactivated_warn_ms: input.streams_deactivated_warn_ms,
            reconnect_grace_ms: input.reconnect_grace_ms,
            keep_alive_enabled: input.keep_alive_enabled,
            keep_alive_inactivity_ms: input.keep_alive_inactivity_ms,
            keep_alive_response_timeout_ms: input.keep_alive_response_timeout_ms,
            slow_consumer_hi_water_mark: input.slow_consumer_hi_water_mark,
            slow_consumer_lo_water_mark: input.slow_consumer_lo_water_mark,
            sdk_log_level: input.sdk_log_level,
            socks5: input.socks5.map(|proxy| Socks5Spec {
                host: proxy.host,
                port: proxy.port,
            }),
        }
    }
}

impl TryFrom<EngineConfigInput> for EngineConfig {
    type Error = Error;

    fn try_from(input: EngineConfigInput) -> Result<Self, Self::Error> {
        EngineConfigSpec::from(input)
            .into_engine_config()
            .map_err(host_error_to_napi)
    }
}
