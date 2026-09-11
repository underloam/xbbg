mod arrow_zero_copy;
mod ext;
mod request;
pub use ext::*;
use request::pairs_to_map;

use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_zero_copy::NativeArrowBatch;
use napi::bindgen_prelude::{create_custom_tokio_runtime, Buffer, Error, Status};
use napi_derive::napi;
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, SubscriptionArrowBatcher, SubscriptionReceiver, SubscriptionSender,
    SubscriptionUpdate, UpdateValue,
};
use xbbg_async::engine::{
    Engine, EngineConfig, OverflowPolicy, RequestParams, ServerAddr, SharedSubscriptionStatus,
    Socks5Proxy, TlsConfig, Transport,
};
use xbbg_async::{BlpAsyncError, ValidationMode};
use xbbg_core::{AuthConfig, BlpError};

type StreamBatchResult = std::result::Result<SubscriptionUpdate, BlpError>;
type SharedStreamReceiver = Arc<Mutex<Option<SubscriptionReceiver>>>;
type SharedPendingStreamItems = Arc<StdMutex<VecDeque<StreamBatchResult>>>;
const MAX_SUBSCRIPTION_BATCH_CAPACITY_HINT: usize = 4096;

fn subscription_batch_capacity_hint(limit: usize) -> usize {
    limit.clamp(1, MAX_SUBSCRIPTION_BATCH_CAPACITY_HINT)
}

fn subscription_layouts_match(current: &Arc<FieldLayout>, next: &Arc<FieldLayout>) -> bool {
    Arc::ptr_eq(current, next)
        || (current.version == next.version
            && current.fields.len() == next.fields.len()
            && current
                .fields
                .iter()
                .zip(next.fields.iter())
                .all(|(current_field, next_field)| {
                    current_field.index == next_field.index
                        && current_field.kind == next_field.kind
                        && current_field.name == next_field.name
                }))
}

#[napi_derive::module_init]
fn init_async_runtime() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create NAPI async bridge runtime");
    create_custom_tokio_runtime(runtime);
}

struct SubscriptionStreamHandle {
    tx: SubscriptionSender,
    claim: Option<xbbg_async::engine::SessionClaim>,
    fields: Vec<String>,
    all_fields: bool,
    service: String,
    options: Vec<String>,
    flush_threshold: Option<usize>,
    overflow_policy: Option<OverflowPolicy>,
    status: SharedSubscriptionStatus,
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
    pub client_credentials: Option<String>,
    pub client_credentials_password: Option<String>,
    pub trust_material: Option<String>,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
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
    pub subscription_flush_threshold: Option<u32>,
    pub max_event_queue_size: Option<u32>,
    pub command_queue_size: Option<u32>,
    pub subscription_stream_capacity: Option<u32>,
    /// Overflow policy: "drop_newest" (default) or "block".
    /// Also accepts "dropnewest" as an alias for "drop_newest".
    pub overflow_policy: Option<String>,
    pub warmup_services: Option<Vec<String>>,
    pub field_cache_path: Option<String>,
    pub auth: Option<AuthConfigInput>,
    pub tls: Option<TlsConfigInput>,
    pub num_start_attempts: Option<u32>,
    pub auto_restart_on_disconnection: Option<bool>,
    pub retry_policy: Option<RetryPolicyInput>,
    pub request_timeout_ms: Option<i64>,
    pub streams_deactivated_warn_ms: Option<i64>,
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
pub struct SubscriptionStats {
    pub messages_received: i64,
    pub dropped_batches: i64,
    pub batches_sent: i64,
    pub slow_consumer: bool,
}

#[napi(object)]
pub struct NativeSubscriptionLayout {
    pub version: u32,
    pub fields: Vec<String>,
    pub kinds: Vec<String>,
}

#[napi(object)]
pub struct NativeSubscriptionRow {
    pub topic: String,
    pub topic_id: u32,
    pub timestamp_us: i64,
    pub layout_version: u32,
    pub field_indices: Vec<u32>,
    pub bool_values: Vec<Option<bool>>,
    pub i32_values: Vec<Option<i32>>,
    pub f64_values: Vec<Option<f64>>,
    pub string_values: Vec<Option<String>>,
    pub i64_values: Vec<Option<String>>,
}

#[napi(object)]
pub struct NativeSubscriptionUpdateBatch {
    pub kind: String,
    pub layout: Option<NativeSubscriptionLayout>,
    pub updates: Vec<NativeSubscriptionRow>,
}

#[napi(object)]
pub struct EntitlementReport {
    pub entitled: bool,
    pub failed_eids: Vec<i32>,
}

fn to_i64_saturating(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}

fn require_auth_value(value: Option<&String>, field: &str, method: &str) -> Result<String, Error> {
    value
        .cloned()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                Status::InvalidArg,
                format!("auth.{field} is required for auth.method='{method}'"),
            )
        })
}

fn require_non_negative_duration(value: i64, field: &str) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{field} must be a non-negative integer number of milliseconds"),
        )
    })
}

fn require_non_negative_timeout(value: i32, field: &str) -> Result<i32, Error> {
    if value < 0 {
        return Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be a non-negative integer number of milliseconds"),
        ));
    }
    Ok(value)
}

fn build_auth_config(input: Option<&AuthConfigInput>) -> Result<Option<AuthConfig>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };

    let method = input.method.trim().to_ascii_lowercase();
    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: require_auth_value(input.app_name.as_ref(), "appName", &method)?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: require_auth_value(input.app_name.as_ref(), "appName", &method)?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: require_auth_value(input.dir_property.as_ref(), "dirProperty", &method)?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: require_auth_value(input.app_name.as_ref(), "appName", &method)?,
            user_id: require_auth_value(input.user_id.as_ref(), "userId", &method)?,
            ip_address: require_auth_value(input.ip_address.as_ref(), "ipAddress", &method)?,
        }),
        "token" => Some(AuthConfig::Token {
            token: require_auth_value(input.token.as_ref(), "token", &method)?,
        }),
        other => {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "invalid auth.method: {other}. Must be one of ['none', 'user', 'app', 'userapp', 'dir', 'directory', 'manual', 'token']",
                ),
            ));
        }
    };

    Ok(auth)
}

fn resolve_transport_input(
    host: Option<&str>,
    port: Option<u16>,
    servers: Option<&Vec<ServerAddressInput>>,
    zfp_remote: Option<&str>,
    socks5: Option<&Socks5ConfigInput>,
) -> Result<Transport, Error> {
    let zfp = zfp_remote
        .map(|s| s.parse::<xbbg_core::zfp::ZfpRemote>())
        .transpose()
        .map_err(|e: String| Error::new(Status::InvalidArg, e))?;

    let proxy = socks5.map(|s| Socks5Proxy {
        host: s.host.clone(),
        port: s.port,
    });

    let explicit_servers = servers.map(|s| !s.is_empty()).unwrap_or(false);
    let explicit_hostport = host.is_some() || port.is_some();

    if let Some(remote) = zfp {
        if explicit_servers || explicit_hostport {
            return Err(Error::new(
                Status::InvalidArg,
                "zfpRemote cannot be combined with host/port/servers — \
                 ZFP supplies Bloomberg endpoints via the leased-line path",
            ));
        }
        if proxy.is_some() {
            return Err(Error::new(
                Status::InvalidArg,
                "zfpRemote cannot be combined with socks5",
            ));
        }
        return Ok(Transport::Zfp(remote));
    }

    let raw: Vec<(String, u16)> = if explicit_servers {
        servers
            .unwrap()
            .iter()
            .map(|s| (s.host.clone(), s.port))
            .collect()
    } else {
        vec![(
            host.unwrap_or("localhost").to_string(),
            port.unwrap_or(8194),
        )]
    };
    let addrs = raw
        .into_iter()
        .map(|(h, p)| ServerAddr {
            host: h,
            port: p,
            proxy: proxy.clone(),
        })
        .collect();
    Ok(Transport::Direct(addrs))
}

fn resolve_tls_input(input: Option<&TlsConfigInput>) -> Result<Option<TlsConfig>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    match (
        input.client_credentials.as_deref(),
        input.trust_material.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(creds), Some(trust)) => Ok(Some(TlsConfig {
            client_credentials: creds.to_string(),
            client_credentials_password: input
                .client_credentials_password
                .clone()
                .unwrap_or_default(),
            trust_material: trust.to_string(),
            handshake_timeout_ms: input
                .handshake_timeout_ms
                .map(|v| require_non_negative_timeout(v, "tls.handshakeTimeoutMs"))
                .transpose()?,
            crl_fetch_timeout_ms: input
                .crl_fetch_timeout_ms
                .map(|v| require_non_negative_timeout(v, "tls.crlFetchTimeoutMs"))
                .transpose()?,
        })),
        (Some(_), None) => Err(Error::new(
            Status::InvalidArg,
            "tls.clientCredentials set without tls.trustMaterial",
        )),
        (None, Some(_)) => Err(Error::new(
            Status::InvalidArg,
            "tls.trustMaterial set without tls.clientCredentials",
        )),
    }
}

impl TryFrom<EngineConfigInput> for EngineConfig {
    type Error = Error;

    fn try_from(input: EngineConfigInput) -> Result<Self, Self::Error> {
        let mut config = EngineConfig::default();
        let auth = build_auth_config(input.auth.as_ref())?;

        let validation_mode = match input.validation_mode {
            Some(mode) => ValidationMode::from_str(&mode)
                .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?,
            None => config.validation_mode,
        };

        let overflow_policy = match input.overflow_policy {
            Some(policy) => OverflowPolicy::from_str(&policy)
                .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?,
            None => config.overflow_policy,
        };

        let transport = resolve_transport_input(
            input.host.as_deref(),
            input.port,
            input.servers.as_ref(),
            input.zfp_remote.as_deref(),
            input.socks5.as_ref(),
        )?;
        config.transport = transport;
        if let Some(size) = input.request_pool_size {
            config.request_pool_size = size as usize;
        }
        if let Some(size) = input.subscription_pool_size {
            config.subscription_pool_size = size as usize;
        }
        if let Some(size) = input.runtime_worker_threads {
            if size == 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "runtimeWorkerThreads must be greater than zero",
                ));
            }
            config.runtime_worker_threads = size as usize;
        }
        if let Some(size) = input.max_subscription_sessions {
            if size == 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "maxSubscriptionSessions must be greater than zero",
                ));
            }
            config.max_subscription_sessions = size as usize;
        }
        if config.max_subscription_sessions < config.subscription_pool_size {
            return Err(Error::new(
                Status::InvalidArg,
                "maxSubscriptionSessions must be greater than or equal to subscriptionPoolSize",
            ));
        }
        if let Some(enabled) = input.shard_requests {
            config.shard_requests = enabled;
        }
        if let Some(size) = input.shard_threshold {
            config.shard_threshold = size as usize;
        }
        if let Some(size) = input.shard_chunk_size {
            config.shard_chunk_size = size as usize;
        }
        if let Some(size) = input.shard_max_concurrent {
            config.shard_max_concurrent = size as usize;
        }
        if let Some(size) = input.subscription_flush_threshold {
            if size == 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "subscriptionFlushThreshold must be greater than zero",
                ));
            }
            config.subscription_flush_threshold = size as usize;
        }
        if let Some(size) = input.max_event_queue_size {
            config.max_event_queue_size = size as usize;
        }
        if let Some(size) = input.command_queue_size {
            config.command_queue_size = size as usize;
        }
        if let Some(size) = input.subscription_stream_capacity {
            if size == 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "subscriptionStreamCapacity must be greater than zero",
                ));
            }
            config.subscription_stream_capacity = size as usize;
        }
        if let Some(services) = input.warmup_services {
            config.warmup_services = services;
        }
        if let Some(field_cache_path) = input.field_cache_path {
            config.field_cache_path = Some(field_cache_path.into());
        }
        config.tls = resolve_tls_input(input.tls.as_ref())?;
        if let Some(num_start_attempts) = input.num_start_attempts {
            config.num_start_attempts = num_start_attempts as usize;
        }
        if let Some(auto_restart) = input.auto_restart_on_disconnection {
            config.auto_restart_on_disconnection = auto_restart;
        }
        if let Some(retry_policy) = input.retry_policy {
            if let Some(max_retries) = retry_policy.max_retries {
                config.retry_policy.max_retries = max_retries;
            }
            if let Some(initial_delay_ms) = retry_policy.initial_delay_ms {
                config.retry_policy.initial_delay_ms =
                    require_non_negative_duration(initial_delay_ms, "retryPolicy.initialDelayMs")?;
            }
            if let Some(backoff_factor) = retry_policy.backoff_factor {
                config.retry_policy.backoff_factor = backoff_factor;
            }
            if let Some(max_delay_ms) = retry_policy.max_delay_ms {
                config.retry_policy.max_delay_ms =
                    require_non_negative_duration(max_delay_ms, "retryPolicy.maxDelayMs")?;
            }
        }
        if let Some(request_timeout_ms) = input.request_timeout_ms {
            config.request_timeout_ms =
                require_non_negative_duration(request_timeout_ms, "requestTimeoutMs")?;
        }
        if let Some(streams_deactivated_warn_ms) = input.streams_deactivated_warn_ms {
            config.streams_deactivated_warn_ms = require_non_negative_duration(
                streams_deactivated_warn_ms,
                "streamsDeactivatedWarnMs",
            )?;
        }
        if let Some(keep_alive_enabled) = input.keep_alive_enabled {
            config.keep_alive_enabled = keep_alive_enabled;
        }
        if let Some(v) = input.keep_alive_inactivity_ms {
            if v < 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "keepAliveInactivityMs must be non-negative".to_string(),
                ));
            }
            config.keep_alive_inactivity_ms = Some(v);
        }
        if let Some(v) = input.keep_alive_response_timeout_ms {
            if v < 0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "keepAliveResponseTimeoutMs must be non-negative".to_string(),
                ));
            }
            config.keep_alive_response_timeout_ms = Some(v);
        }
        if let Some(v) = input.slow_consumer_hi_water_mark {
            if !(0.0..=1.0).contains(&v) {
                return Err(Error::new(
                    Status::InvalidArg,
                    "slowConsumerHiWaterMark must be in 0.0..=1.0".to_string(),
                ));
            }
            config.slow_consumer_hi_water_mark = Some(v as f32);
        }
        if let Some(v) = input.slow_consumer_lo_water_mark {
            if !(0.0..1.0).contains(&v) {
                return Err(Error::new(
                    Status::InvalidArg,
                    "slowConsumerLoWaterMark must be in 0.0..1.0".to_string(),
                ));
            }
            config.slow_consumer_lo_water_mark = Some(v as f32);
        }
        if let Some(sdk_log_level) = input.sdk_log_level {
            config.sdk_log_level = sdk_log_level
                .parse()
                .map_err(|e: String| Error::new(Status::InvalidArg, e))?;
        }
        config.validation_mode = validation_mode;
        config.overflow_policy = overflow_policy;
        config.auth = auth;

        Ok(config)
    }
}

fn to_ipc_buffer(batch: RecordBatch) -> napi::Result<Buffer> {
    let schema = batch.schema();
    let mut cursor = Cursor::new(Vec::<u8>::new());

    {
        let mut writer = StreamWriter::try_new(&mut cursor, &schema).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC writer init failed: {e}"),
            )
        })?;

        writer.write(&batch).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC write failed: {e}"),
            )
        })?;

        writer.finish().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC finalize failed: {e}"),
            )
        })?;
    }

    Ok(Buffer::from(cursor.into_inner()))
}

fn to_native_record_batch(batch: RecordBatch) -> napi::Result<NativeArrowBatch> {
    NativeArrowBatch::from_record_batch(batch)
}

fn field_kind_label(kind: FieldKind) -> &'static str {
    match kind {
        FieldKind::Unknown => "unknown",
        FieldKind::Bool => "bool",
        FieldKind::I32 => "i32",
        FieldKind::I64 => "i64",
        FieldKind::F64 => "f64",
        FieldKind::Str => "str",
        FieldKind::Date32 => "date32",
        FieldKind::Time64Micros => "time64_us",
        FieldKind::TimestampMicros => "timestamp_us",
    }
}

fn to_native_layout(update: &SubscriptionUpdate) -> NativeSubscriptionLayout {
    let fields = update
        .layout
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect();
    let kinds = update
        .layout
        .fields
        .iter()
        .map(|field| field_kind_label(field.kind).to_string())
        .collect();
    NativeSubscriptionLayout {
        version: update.layout.version,
        fields,
        kinds,
    }
}

fn to_native_row(update: SubscriptionUpdate) -> NativeSubscriptionRow {
    let len = update.values.len();
    let mut field_indices = Vec::with_capacity(len);
    let mut bool_values = Vec::with_capacity(len);
    let mut i32_values = Vec::with_capacity(len);
    let mut f64_values = Vec::with_capacity(len);
    let mut string_values = Vec::with_capacity(len);
    let mut i64_values = Vec::with_capacity(len);

    for field in update.values.iter() {
        field_indices.push(u32::from(field.index));
        let effective_kind = update
            .layout
            .fields
            .get(usize::from(field.index))
            .map_or(FieldKind::Unknown, |meta| meta.kind);
        if matches!(effective_kind, FieldKind::Unknown | FieldKind::Str)
            && !matches!(field.value, UpdateValue::Null)
        {
            bool_values.push(None);
            i32_values.push(None);
            f64_values.push(None);
            string_values.push(field.value.as_string_lossy());
            i64_values.push(None);
            continue;
        }
        match &field.value {
            UpdateValue::Null => {
                bool_values.push(None);
                i32_values.push(None);
                f64_values.push(None);
                string_values.push(None);
                i64_values.push(None);
            }
            UpdateValue::Bool(v) => {
                bool_values.push(Some(*v));
                i32_values.push(None);
                f64_values.push(None);
                string_values.push(None);
                i64_values.push(None);
            }
            UpdateValue::I32(v) | UpdateValue::Date32(v) => {
                bool_values.push(None);
                i32_values.push(Some(*v));
                f64_values.push(None);
                string_values.push(None);
                i64_values.push(None);
            }
            UpdateValue::I64(v)
            | UpdateValue::Time64Micros(v)
            | UpdateValue::TimestampMicros(v) => {
                bool_values.push(None);
                i32_values.push(None);
                f64_values.push(None);
                string_values.push(None);
                i64_values.push(Some(v.to_string()));
            }
            UpdateValue::F64(v) => {
                bool_values.push(None);
                i32_values.push(None);
                f64_values.push(Some(*v));
                string_values.push(None);
                i64_values.push(None);
            }
            UpdateValue::Str(v) => {
                bool_values.push(None);
                i32_values.push(None);
                f64_values.push(None);
                string_values.push(Some(v.to_string()));
                i64_values.push(None);
            }
        }
    }

    NativeSubscriptionRow {
        topic: update.topic.to_string(),
        topic_id: update.topic_id,
        timestamp_us: update.timestamp_us,
        layout_version: update.layout.version,
        field_indices,
        bool_values,
        i32_values,
        f64_values,
        string_values,
        i64_values,
    }
}

fn to_native_update_batch(
    updates: Vec<SubscriptionUpdate>,
    last_layout_sent: &mut Option<Arc<FieldLayout>>,
) -> Option<NativeSubscriptionUpdateBatch> {
    let mut iter = updates.into_iter();
    let first = iter.next()?;
    let layout = if last_layout_sent
        .as_ref()
        .is_some_and(|current| subscription_layouts_match(current, &first.layout))
    {
        None
    } else {
        *last_layout_sent = Some(first.layout.clone());
        Some(to_native_layout(&first))
    };
    let mut rows = Vec::with_capacity(iter.size_hint().0 + 1);
    rows.push(to_native_row(first));
    rows.extend(iter.map(to_native_row));
    Some(NativeSubscriptionUpdateBatch {
        kind: "batch".to_string(),
        layout,
        updates: rows,
    })
}

fn subscription_limit(
    value: Option<u32>,
    label: &str,
    default_limit: usize,
) -> napi::Result<usize> {
    match value {
        Some(0) => Err(Error::new(
            Status::InvalidArg,
            format!("{label} must be greater than zero"),
        )),
        Some(value) => Ok(value as usize),
        None => Ok(default_limit),
    }
}

async fn wait_for_subscription_close(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }
    while close_rx.changed().await.is_ok() {
        if *close_rx.borrow() {
            return;
        }
    }
}

async fn receive_stream_item(
    rx: &mut SubscriptionReceiver,
    close_rx: &mut watch::Receiver<bool>,
    deadline: Option<Instant>,
) -> Option<StreamBatchResult> {
    if *close_rx.borrow() {
        return None;
    }
    if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
        return rx.try_recv().ok();
    }

    let receive = async {
        tokio::select! {
            biased;
            _ = wait_for_subscription_close(close_rx) => None,
            item = rx.recv() => item,
        }
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, receive)
            .await
            .unwrap_or(None),
        None => receive.await,
    }
}

struct PendingUpdateBatch<'a> {
    pending: &'a StdMutex<VecDeque<StreamBatchResult>>,
    updates: Vec<SubscriptionUpdate>,
    committed: bool,
}

impl<'a> PendingUpdateBatch<'a> {
    fn new(pending: &'a StdMutex<VecDeque<StreamBatchResult>>, capacity: usize) -> Self {
        Self {
            pending,
            updates: Vec::with_capacity(capacity),
            committed: false,
        }
    }

    fn commit(mut self) -> Vec<SubscriptionUpdate> {
        self.committed = true;
        std::mem::take(&mut self.updates)
    }
}

impl Drop for PendingUpdateBatch<'_> {
    fn drop(&mut self) {
        if self.committed || self.updates.is_empty() {
            return;
        }
        let mut pending = self
            .pending
            .lock()
            .expect("subscription pending queue poisoned");
        for update in self.updates.drain(..).rev() {
            pending.push_front(Ok(update));
        }
    }
}

async fn receive_subscription_updates(
    rx: &mut SubscriptionReceiver,
    pending: &StdMutex<VecDeque<StreamBatchResult>>,
    close_rx: &mut watch::Receiver<bool>,
    limit: usize,
    max_wait_ms: Option<u32>,
) -> Result<Option<Vec<SubscriptionUpdate>>, Box<BlpError>> {
    if *close_rx.borrow() {
        return Ok(None);
    }
    let deadline =
        max_wait_ms.map(|wait_ms| Instant::now() + Duration::from_millis(u64::from(wait_ms)));
    let mut layout = None;
    let mut batch = PendingUpdateBatch::new(pending, subscription_batch_capacity_hint(limit));
    loop {
        let queued = pending
            .lock()
            .expect("subscription pending queue poisoned")
            .pop_front();
        let item = match queued {
            Some(item) => Some(item),
            None if batch.updates.is_empty() || deadline.is_some() => {
                receive_stream_item(rx, close_rx, deadline).await
            }
            None => rx.try_recv().ok(),
        };

        match item {
            Some(Ok(update)) => {
                if layout
                    .as_ref()
                    .is_some_and(|current| !subscription_layouts_match(current, &update.layout))
                {
                    pending
                        .lock()
                        .expect("subscription pending queue poisoned")
                        .push_front(Ok(update));
                    break;
                }
                layout.get_or_insert_with(|| update.layout.clone());
                batch.updates.push(update);
                if batch.updates.len() == limit {
                    break;
                }
            }
            Some(Err(error)) if batch.updates.is_empty() => return Err(Box::new(error)),
            Some(Err(error)) => {
                pending
                    .lock()
                    .expect("subscription pending queue poisoned")
                    .push_front(Err(error));
                break;
            }
            None => break,
        }
    }

    if batch.updates.is_empty() {
        Ok(None)
    } else {
        Ok(Some(batch.commit()))
    }
}

async fn drain_forwarder_into_pending(
    claim: &xbbg_async::engine::SessionClaim,
    rx: &mut SubscriptionReceiver,
    pending: &StdMutex<VecDeque<StreamBatchResult>>,
) -> Result<(), Box<BlpAsyncError>> {
    let barrier = claim.drain_forwarder();
    tokio::pin!(barrier);
    let barrier_result = loop {
        tokio::select! {
            biased;
            item = rx.recv() => {
                match item {
                    Some(item) => pending
                        .lock()
                        .expect("subscription pending queue poisoned")
                        .push_back(item),
                    None => break barrier.await,
                }
            }
            result = &mut barrier => break result,
        }
    };
    while let Ok(item) = rx.try_recv() {
        pending
            .lock()
            .expect("subscription pending queue poisoned")
            .push_back(item);
    }
    barrier_result.map_err(Box::new)
}

/// Stable machine-readable error code embedded in every native error message
/// as a `[XBBG:<CODE>] ` prefix. The js-xbbg wrapper parses this prefix to
/// construct typed error classes; the human-readable message follows it.
/// Codes: SESSION, REQUEST, LIMIT, DATALOSS, VALIDATION, TIMEOUT, CANCELLED, INTERNAL.
fn coded(status: Status, code: &str, msg: impl AsRef<str>) -> Error {
    Error::new(status, format!("[XBBG:{code}] {}", msg.as_ref()))
}

fn request_failure_code(label: Option<&str>) -> &'static str {
    match label {
        Some(label)
            if label.contains("category=LIMIT")
                || label.contains("DAILY_CAPACITY_REACHED")
                || label.contains("subcategory=DAILY_CAPACITY_REACHED") =>
        {
            "LIMIT"
        }
        _ => "REQUEST",
    }
}

fn blp_error_to_napi(e: BlpError) -> Error {
    match e {
        BlpError::SessionStart { source, label } => {
            let msg = format_error_msg("Session start failed", label.as_deref(), source.as_deref());
            coded(Status::GenericFailure, "SESSION", msg)
        }
        BlpError::OpenService {
            service,
            source,
            label,
        } => {
            let msg = format!(
                "Failed to open service '{service}': {}",
                format_error_msg("", label.as_deref(), source.as_deref())
            );
            coded(Status::GenericFailure, "SESSION", msg)
        }
        BlpError::RequestFailure {
            service,
            operation,
            cid,
            label,
            request_id,
            source,
        } => {
            let mut msg = format!("Request failed on {service}");
            if let Some(op) = operation {
                msg.push_str(&format!("::{op}"));
            }
            if let Some(c) = cid {
                msg.push_str(&format!(" (cid={c})"));
            }
            if let Some(rid) = request_id {
                msg.push_str(&format!(" [request_id={rid}]"));
            }
            let code = request_failure_code(label.as_deref());
            if let Some(l) = label {
                msg.push_str(&format!(" - {l}"));
            }
            if let Some(s) = source {
                msg.push_str(&format!(": {s}"));
            }
            coded(Status::GenericFailure, code, msg)
        }
        BlpError::InvalidArgument { detail } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Invalid argument: {detail}"),
        ),
        BlpError::Timeout => coded(Status::GenericFailure, "TIMEOUT", "Request timed out"),
        BlpError::TemplateTerminated { cid } => {
            let msg = match cid {
                Some(c) => format!("Request template terminated (cid={c})"),
                None => "Request template terminated".to_string(),
            };
            coded(Status::GenericFailure, "REQUEST", msg)
        }
        BlpError::SubscriptionDataLoss { topic, detail } => coded(
            Status::GenericFailure,
            "DATALOSS",
            format!("Subscription data loss [topic={topic}]: {detail}"),
        ),
        BlpError::SubscriptionFailure { cid, label } => {
            let mut msg = "Subscription failed".to_string();
            if let Some(c) = cid {
                msg.push_str(&format!(" (cid={c})"));
            }
            if let Some(l) = label {
                msg.push_str(&format!(": {l}"));
            }
            coded(Status::GenericFailure, "REQUEST", msg)
        }
        BlpError::Internal { detail } => coded(
            Status::GenericFailure,
            "INTERNAL",
            format!("Internal error: {detail}"),
        ),
        BlpError::SchemaOperationNotFound { service, operation } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Operation not found: {service}::{operation}"),
        ),
        BlpError::SchemaElementNotFound { parent, name } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Schema element not found: {parent}.{name}"),
        ),
        BlpError::SchemaTypeMismatch {
            element,
            expected,
            found,
        } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Schema type mismatch at {element}: expected {expected}, found {found}"),
        ),
        BlpError::SchemaUnsupported { element, detail } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Unsupported schema construct at {element}: {detail}"),
        ),
        BlpError::Validation { message, errors } => {
            let details: Vec<String> = errors
                .iter()
                .map(|e| match &e.suggestion {
                    Some(suggestion) => format!("{e} (did you mean '{suggestion}'?)"),
                    None => e.to_string(),
                })
                .collect();
            let msg = if details.is_empty() {
                message
            } else {
                format!("{message}: {}", details.join("; "))
            };
            coded(Status::InvalidArg, "VALIDATION", msg)
        }
    }
}

fn blp_async_error_to_napi(e: BlpAsyncError) -> Error {
    match e {
        BlpAsyncError::Blp(blp_err) => blp_error_to_napi(blp_err),
        BlpAsyncError::BlpError(blp_err) => blp_error_to_napi(blp_err),
        BlpAsyncError::ConfigError { detail } => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Configuration error: {detail}"),
        ),
        BlpAsyncError::ChannelClosed => coded(
            Status::GenericFailure,
            "INTERNAL",
            "Channel closed unexpectedly",
        ),
        BlpAsyncError::StreamFull => coded(
            Status::GenericFailure,
            "INTERNAL",
            "Stream buffer full - consumer too slow",
        ),
        BlpAsyncError::Cancelled => {
            coded(Status::GenericFailure, "CANCELLED", "Request was cancelled")
        }
        BlpAsyncError::Timeout => coded(Status::GenericFailure, "TIMEOUT", "Request timed out"),
        BlpAsyncError::SessionLost {
            worker_id,
            in_flight_count,
        } => coded(
            Status::GenericFailure,
            "SESSION",
            format!(
                "Session lost on worker {worker_id}; {in_flight_count} in-flight requests failed"
            ),
        ),
        BlpAsyncError::AllWorkersDown { pool_size } => coded(
            Status::GenericFailure,
            "SESSION",
            format!("All {pool_size} request workers are down"),
        ),
        BlpAsyncError::Internal(msg) => coded(Status::GenericFailure, "INTERNAL", msg),
    }
}

fn recipe_error_to_napi(e: xbbg_recipes::RecipeError) -> Error {
    match e {
        // Delegate engine errors so they carry their precise code.
        xbbg_recipes::RecipeError::Engine(inner) => blp_async_error_to_napi(*inner),
        xbbg_recipes::RecipeError::InvalidArgument(detail) => coded(
            Status::InvalidArg,
            "VALIDATION",
            format!("Invalid argument: {detail}"),
        ),
        other => coded(Status::GenericFailure, "INTERNAL", other.to_string()),
    }
}

fn format_error_msg(
    base: &str,
    label: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) -> String {
    let mut msg = base.to_string();
    if let Some(l) = label {
        if !msg.is_empty() {
            msg.push_str(": ");
        }
        msg.push_str(l);
    }
    if let Some(s) = source {
        if !msg.is_empty() {
            msg.push_str(" - ");
        }
        msg.push_str(&s.to_string());
    }
    if msg.is_empty() {
        "Unknown error".to_string()
    } else {
        msg
    }
}

#[napi]
pub fn version() -> String {
    xbbg_core::version().to_string()
}

#[napi]
pub fn set_log_level(level: String) -> napi::Result<()> {
    let lvl = xbbg_log::parse_level(&level).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            format!("Invalid log level '{level}'. Expected: trace, debug, info, warn, error"),
        )
    })?;
    xbbg_log::set_level(lvl);
    Ok(())
}

#[napi]
pub fn get_log_level() -> String {
    match xbbg_log::current_level() {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
    .to_string()
}

#[derive(Default)]
struct SchemaJsonCache {
    services: HashMap<String, String>,
    operations: HashMap<(String, String), String>,
}

#[napi]
pub struct JsEngine {
    engine: Arc<Engine>,
    schema_json_cache: Arc<StdMutex<SchemaJsonCache>>,
    subscription_batch_items: usize,
}

#[napi]
impl JsEngine {
    #[napi(constructor)]
    pub fn new(host: Option<String>, port: Option<u16>) -> napi::Result<Self> {
        let host = host.unwrap_or_else(|| "localhost".to_string());
        let port = port.unwrap_or(8194);
        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };
        Self::start_engine(config)
    }

    #[napi(factory)]
    pub fn with_config(config: EngineConfigInput) -> napi::Result<Self> {
        Self::start_engine(config.try_into()?)
    }

    /// Connect asynchronously with host/port defaults.
    ///
    /// Engine startup (Bloomberg session connect plus service warmup —
    /// seconds, up to the 30s session timeout) runs on the blocking pool
    /// instead of the JS thread. Prefer this over `new JsEngine(...)`, which
    /// freezes the Node event loop for the duration of the connect.
    #[napi(factory)]
    pub async fn connect(host: Option<String>, port: Option<u16>) -> napi::Result<Self> {
        let host = host.unwrap_or_else(|| "localhost".to_string());
        let port = port.unwrap_or(8194);
        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };
        Self::start_engine_async(config).await
    }

    /// Connect asynchronously from a full configuration.
    /// See [`JsEngine::connect`].
    #[napi(factory)]
    pub async fn connect_with_config(config: EngineConfigInput) -> napi::Result<Self> {
        Self::start_engine_async(config.try_into()?).await
    }

    #[napi]
    pub async fn request(&self, params: RequestInput) -> napi::Result<NativeArrowBatch> {
        let rust_params: RequestParams = params.try_into()?;
        let batch = self
            .engine
            .request(rust_params)
            .await
            .map_err(blp_async_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn request_raw(&self, params: RequestInput) -> napi::Result<Buffer> {
        let rust_params: RequestParams = params.try_into()?;
        let batch = self
            .engine
            .request(rust_params)
            .await
            .map_err(blp_async_error_to_napi)?;
        to_ipc_buffer(batch)
    }

    /// Return the Bloomberg identity seat type: "BPS", "NONBPS", or "INVALID".
    ///
    /// Identity operations authorize lazily using the engine auth config when
    /// configured, otherwise the Desktop terminal OS-logon user. The first call
    /// may block for a few seconds and transient authorization failures are
    /// retryable by calling again.
    #[napi]
    pub async fn seat_type(&self) -> napi::Result<String> {
        self.engine
            .seat_type()
            .await
            .map(|seat| seat.as_str().to_string())
            .map_err(blp_async_error_to_napi)
    }

    /// Check whether the authorized identity is entitled to all supplied EIDs.
    ///
    /// Identity operations authorize lazily using the engine auth config when
    /// configured, otherwise the Desktop terminal OS-logon user. The first call
    /// may block for a few seconds and transient authorization failures are
    /// retryable by calling again.
    #[napi]
    pub async fn check_entitlements(
        &self,
        service: String,
        eids: Vec<i32>,
    ) -> napi::Result<EntitlementReport> {
        self.engine
            .check_entitlements(&service, &eids)
            .await
            .map(|report| EntitlementReport {
                entitled: report.entitled,
                failed_eids: report.failed_eids,
            })
            .map_err(blp_async_error_to_napi)
    }

    /// Return whether the authorized identity may use the Bloomberg service.
    ///
    /// Identity operations authorize lazily using the engine auth config when
    /// configured, otherwise the Desktop terminal OS-logon user. The first call
    /// may block for a few seconds and transient authorization failures are
    /// retryable by calling again.
    #[napi]
    pub async fn identity_is_authorized(&self, service: String) -> napi::Result<bool> {
        self.engine
            .identity_is_authorized(&service)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub async fn resolve_field_types(
        &self,
        fields: Vec<String>,
        overrides: Option<Vec<StringPair>>,
        default_type: Option<String>,
    ) -> napi::Result<Vec<StringPair>> {
        let map = self
            .engine
            .resolve_field_types(
                &fields,
                pairs_to_map(overrides).as_ref(),
                default_type.as_deref().unwrap_or("string"),
            )
            .await
            .map_err(blp_async_error_to_napi)?;

        Ok(map
            .into_iter()
            .map(|(key, value)| StringPair { key, value })
            .collect())
    }

    #[napi]
    pub fn get_field_info(&self, field: String) -> Option<FieldInfoOutput> {
        self.engine
            .get_field_info(&field)
            .map(|info| FieldInfoOutput {
                field_id: info.field_id,
                arrow_type: info.arrow_type,
                description: info.description,
                category: info.category,
            })
    }

    #[napi]
    pub fn clear_field_cache(&self) -> napi::Result<()> {
        self.engine
            .clear_field_cache()
            .map_err(|error| coded(Status::GenericFailure, "INTERNAL", error))
    }

    #[napi]
    pub fn save_field_cache(&self) -> napi::Result<()> {
        self.engine
            .save_field_cache()
            .map_err(|e| Error::new(Status::GenericFailure, e))
    }

    #[napi]
    pub async fn validate_fields(&self, fields: Vec<String>) -> napi::Result<Vec<String>> {
        self.engine
            .validate_fields(&fields)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn is_field_validation_enabled(&self) -> bool {
        self.engine.is_field_validation_enabled()
    }

    #[napi]
    pub async fn get_schema(&self, service: String) -> napi::Result<String> {
        if let Some(cached) = self
            .schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .services
            .get(&service)
            .cloned()
        {
            return Ok(cached);
        }

        let schema = self
            .engine
            .get_schema(&service)
            .await
            .map_err(blp_async_error_to_napi)?;
        let serialized = serde_json::to_string(&*schema)
            .map_err(|e| Error::new(Status::GenericFailure, format!("serialize schema: {e}")))?;
        self.schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .services
            .insert(service, serialized.clone());
        Ok(serialized)
    }

    #[napi]
    pub async fn get_operation(&self, service: String, operation: String) -> napi::Result<String> {
        let key = (service.clone(), operation.clone());
        if let Some(cached) = self
            .schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .operations
            .get(&key)
            .cloned()
        {
            return Ok(cached);
        }

        let op = self
            .engine
            .get_operation(&service, &operation)
            .await
            .map_err(blp_async_error_to_napi)?;
        let serialized = serde_json::to_string(&op)
            .map_err(|e| Error::new(Status::GenericFailure, format!("serialize operation: {e}")))?;
        self.schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .operations
            .insert(key, serialized.clone());
        Ok(serialized)
    }

    #[napi]
    pub async fn list_operations(&self, service: String) -> napi::Result<Vec<String>> {
        self.engine
            .list_operations(&service)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn get_cached_schema(&self, service: String) -> Option<String> {
        if let Some(cached) = self
            .schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .services
            .get(&service)
            .cloned()
        {
            return Some(cached);
        }

        let serialized = self
            .engine
            .get_cached_schema(&service)
            .and_then(|s| serde_json::to_string(&*s).ok())?;
        self.schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned")
            .services
            .insert(service, serialized.clone());
        Some(serialized)
    }

    #[napi]
    pub fn invalidate_schema(&self, service: String) -> napi::Result<()> {
        self.engine
            .invalidate_schema(&service)
            .map_err(|error| coded(Status::GenericFailure, "INTERNAL", error))?;
        let mut cache = self
            .schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned");
        cache.services.remove(&service);
        cache
            .operations
            .retain(|(cached_service, _), _| cached_service != &service);
        Ok(())
    }

    #[napi]
    pub fn clear_schema_cache(&self) -> napi::Result<()> {
        self.engine
            .clear_schema_cache()
            .map_err(|error| coded(Status::GenericFailure, "INTERNAL", error))?;
        *self
            .schema_json_cache
            .lock()
            .expect("schema JSON cache poisoned") = SchemaJsonCache::default();
        Ok(())
    }

    #[napi]
    pub fn list_cached_schemas(&self) -> Vec<String> {
        self.engine.list_cached_schemas()
    }

    #[napi]
    pub async fn get_enum_values(
        &self,
        service: String,
        operation: String,
        element: String,
    ) -> napi::Result<Option<Vec<String>>> {
        self.engine
            .get_enum_values(&service, &operation, &element)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub async fn list_valid_elements(
        &self,
        service: String,
        operation: String,
    ) -> napi::Result<Option<Vec<String>>> {
        self.engine
            .list_valid_elements(&service, &operation)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub async fn subscribe(
        &self,
        tickers: Vec<String>,
        fields: Vec<String>,
        all_fields: Option<bool>,
    ) -> napi::Result<JsSubscription> {
        let all_fields = all_fields.unwrap_or(false);
        let stream = self
            .engine
            .subscribe_with_options(
                "//blp/mktdata".to_string(),
                tickers.clone(),
                fields.clone(),
                all_fields,
                vec![],
                None,
                None,
                None,
            )
            .await
            .map_err(blp_async_error_to_napi)?;

        JsSubscription::from_stream(stream, tickers, fields, self.subscription_batch_items)
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_with_options(
        &self,
        service: String,
        tickers: Vec<String>,
        fields: Vec<String>,
        options: Option<Vec<String>>,
        flush_threshold: Option<u32>,
        overflow_policy: Option<String>,
        stream_capacity: Option<u32>,
        all_fields: Option<bool>,
    ) -> napi::Result<JsSubscription> {
        let overflow = match overflow_policy {
            Some(policy) => Some(
                OverflowPolicy::from_str(&policy)
                    .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?,
            ),
            None => None,
        };
        let all_fields = all_fields.unwrap_or(false);

        if stream_capacity == Some(0) {
            return Err(Error::new(
                Status::InvalidArg,
                "streamCapacity must be greater than zero",
            ));
        }
        if flush_threshold == Some(0) {
            return Err(Error::new(
                Status::InvalidArg,
                "flushThreshold must be greater than zero",
            ));
        }
        let consumer_batch_items = flush_threshold
            .map(|value| value as usize)
            .unwrap_or(self.subscription_batch_items);

        let stream = self
            .engine
            .subscribe_with_options(
                service,
                tickers.clone(),
                fields.clone(),
                all_fields,
                options.unwrap_or_default(),
                stream_capacity.map(|v| v as usize),
                flush_threshold.map(|v| v as usize),
                overflow,
            )
            .await
            .map_err(blp_async_error_to_napi)?;

        JsSubscription::from_stream(stream, tickers, fields, consumer_batch_items)
    }

    #[napi]
    pub fn signal_shutdown(&self) {
        self.engine.signal_shutdown();
    }

    /// Whether at least one request worker session is healthy.
    ///
    /// Mirrors pyo3's `is_connected()`; previously hardcoded `true`.
    #[napi]
    pub fn is_available(&self) -> bool {
        self.engine
            .request_pool_health()
            .iter()
            .any(|(_, health)| *health == xbbg_async::engine::WorkerHealth::Healthy)
    }

    #[napi]
    pub async fn recipe_bqr(
        &self,
        ticker: String,
        start_datetime: String,
        end_datetime: String,
        event_types: Option<Vec<String>>,
        include_broker_codes: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_bqr(
            &engine,
            ticker,
            start_datetime,
            end_datetime,
            event_types,
            include_broker_codes.unwrap_or(true),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn recipe_yas(
        &self,
        tickers: Vec<String>,
        fields: Vec<String>,
        settle_dt: Option<String>,
        yield_type: Option<u8>,
        spread: Option<f64>,
        yield_val: Option<f64>,
        price: Option<f64>,
        benchmark: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let yt = yield_type
            .and_then(|v| xbbg_ext::transforms::fixed_income::YieldType::try_from(v).ok());
        let batch = xbbg_recipes::fixed_income::recipe_yas(
            &engine, tickers, fields, settle_dt, yt, spread, yield_val, price, benchmark,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_preferreds(
        &self,
        equity_ticker: String,
        fields: Option<Vec<String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_preferreds(&engine, equity_ticker, fields)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_corporate_bonds(
        &self,
        ticker: String,
        ccy: Option<String>,
        fields: Option<Vec<String>>,
        active_only: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_corporate_bonds(
            &engine,
            ticker,
            ccy,
            fields,
            active_only.unwrap_or(true),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_fut_ticker(
        &self,
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_fut_ticker(&engine, gen_ticker, dt, freq)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_active_futures(
        &self,
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_active_futures(&engine, gen_ticker, dt, freq)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_futures_curve(
        &self,
        gen_ticker: String,
        asof: Option<String>,
        chain_field: Option<String>,
        fields: Option<Vec<String>>,
        max_contracts: Option<i32>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_futures_curve(
            &engine,
            gen_ticker,
            asof,
            chain_field,
            fields,
            max_contracts,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_cdx_ticker(
        &self,
        gen_ticker: String,
        dt: String,
        versionless: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_cdx_ticker_with_options(
            &engine,
            gen_ticker,
            dt,
            versionless.unwrap_or(false),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_active_cdx(
        &self,
        gen_ticker: String,
        dt: String,
        lookback_days: Option<i32>,
        versionless: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_active_cdx_with_options(
            &engine,
            gen_ticker,
            dt,
            lookback_days,
            versionless.unwrap_or(false),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_dividend(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dvd_type: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_dividend(
            &engine, tickers, dvd_type, start_date, end_date,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_dividend_yield(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dividend_types: Option<Vec<String>>,
        window_days: Option<i32>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_dividend_yield(
            &engine,
            tickers,
            start_date,
            end_date,
            dividend_types,
            window_days,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_turnover(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        ccy: Option<String>,
        factor: Option<f64>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_turnover(
            &engine, tickers, start_date, end_date, ccy, factor,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_holdings(
        &self,
        etf_ticker: String,
        fields: Option<Vec<String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_etf_holdings(&engine, etf_ticker, fields)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn recipe_vol_surface(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        presets: Option<Vec<String>>,
        field_specs: Option<Vec<String>>,
        as_decimal: Option<bool>,
        include_derived: Option<bool>,
        risk_free_rate: Option<f64>,
        dividend_yield_field: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::volatility::recipe_vol_surface(
            &engine,
            tickers,
            start_date,
            end_date,
            presets,
            field_specs,
            as_decimal,
            include_derived,
            risk_free_rate,
            dividend_yield_field,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_index_members(
        &self,
        index: String,
        field: Option<String>,
        asof: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::indices::recipe_index_members(&engine, index, field, asof)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_resolve_isins(&self, isins: Vec<String>) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::identifiers::recipe_resolve_isins(&engine, isins)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_issuer_isins(
        &self,
        bond_isins: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::identifiers::recipe_issuer_isins(&engine, bond_isins)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_relationships(
        &self,
        tickers: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::etf::recipe_etf_nav_relationships(&engine, tickers)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_snapshot(
        &self,
        tickers: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::etf::recipe_etf_nav_snapshot(&engine, tickers)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_history(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch =
            xbbg_recipes::etf::recipe_etf_nav_history(&engine, tickers, start_date, end_date)
                .await
                .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_currency_conversion(
        &self,
        ticker: String,
        target_ccy: String,
        start_date: String,
        end_date: String,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::currency::recipe_currency_conversion(
            &engine, ticker, target_ccy, start_date, end_date,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    fn start_engine(config: EngineConfig) -> napi::Result<Self> {
        let subscription_batch_items = config.subscription_flush_threshold;
        let engine = Engine::start(config).map_err(blp_async_error_to_napi)?;
        Ok(Self {
            engine: Arc::new(engine),
            schema_json_cache: Arc::new(StdMutex::new(SchemaJsonCache::default())),
            subscription_batch_items,
        })
    }

    async fn start_engine_async(config: EngineConfig) -> napi::Result<Self> {
        let subscription_batch_items = config.subscription_flush_threshold;
        let engine =
            napi::tokio::task::spawn_blocking(move || Engine::start(config).map_err(Box::new))
                .await
                .map_err(|join_error| {
                    coded(
                        Status::GenericFailure,
                        "INTERNAL",
                        format!("engine startup task failed: {join_error}"),
                    )
                })?
                .map_err(|error| blp_async_error_to_napi(*error))?;
        Ok(Self {
            engine: Arc::new(engine),
            schema_json_cache: Arc::new(StdMutex::new(SchemaJsonCache::default())),
            subscription_batch_items,
        })
    }
}

#[napi]
pub struct JsSubscription {
    rx: SharedStreamReceiver,
    close_signal: watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    mutation: Arc<Mutex<()>>,
    stream: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    stream_sender: SubscriptionSender,
    pending: SharedPendingStreamItems,
    scalar_layout: Arc<StdMutex<Option<Arc<FieldLayout>>>>,
    arrow_batcher: Arc<StdMutex<(usize, SubscriptionArrowBatcher)>>,
    arrow_ready: Arc<StdMutex<VecDeque<RecordBatch>>>,
    batch_items: usize,
    fields_snapshot: Arc<Vec<String>>,
    status: SharedSubscriptionStatus,
}

#[napi]
impl JsSubscription {
    fn from_stream(
        stream: xbbg_async::engine::SubscriptionStream,
        _tickers: Vec<String>,
        fields: Vec<String>,
        batch_items: usize,
    ) -> napi::Result<Self> {
        let (rx, tx, claim, status, ft, op_policy, service, options, all_fields) =
            stream.into_parts().map_err(blp_error_to_napi)?;
        let fields_snapshot = Arc::new(fields.clone());
        let status_snapshot = status.clone();
        let stream_sender = tx.clone();
        let handle = SubscriptionStreamHandle {
            tx,
            claim: Some(claim),
            fields,
            all_fields,
            service,
            options,
            flush_threshold: ft,
            overflow_policy: op_policy,
            status,
        };
        let (close_signal, _) = watch::channel(false);
        Ok(Self {
            rx: Arc::new(Mutex::new(Some(rx))),
            close_signal,
            closed: Arc::new(AtomicBool::new(false)),
            mutation: Arc::new(Mutex::new(())),
            stream: Arc::new(Mutex::new(Some(handle))),
            stream_sender,
            pending: Arc::new(StdMutex::new(VecDeque::new())),
            scalar_layout: Arc::new(StdMutex::new(None)),
            arrow_batcher: Arc::new(StdMutex::new((
                batch_items,
                SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(
                    batch_items,
                )),
            ))),
            arrow_ready: Arc::new(StdMutex::new(VecDeque::new())),
            batch_items,
            fields_snapshot,
            status: status_snapshot,
        })
    }

    #[napi]
    pub async fn next_updates(
        &self,
        max_items: Option<u32>,
        max_wait_ms: Option<u32>,
    ) -> napi::Result<Option<NativeSubscriptionUpdateBatch>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let limit = subscription_limit(max_items, "maxItems", self.batch_items)?;
        let mut close_rx = self.close_signal.subscribe();
        let mut rx_guard = self.rx.lock().await;
        let Some(rx) = rx_guard.as_mut() else {
            return Ok(None);
        };
        let updates = receive_subscription_updates(
            rx,
            self.pending.as_ref(),
            &mut close_rx,
            limit,
            max_wait_ms,
        )
        .await
        .map_err(|error| blp_error_to_napi(*error))?;

        let Some(updates) = updates else {
            return Ok(None);
        };
        let batch = {
            let mut last_layout = self
                .scalar_layout
                .lock()
                .expect("subscription scalar layout poisoned");
            to_native_update_batch(updates, &mut last_layout)
        };
        drop(rx_guard);
        Ok(batch)
    }

    #[napi]
    pub async fn next_arrow_batch(
        &self,
        max_rows: Option<u32>,
        max_wait_ms: Option<u32>,
    ) -> napi::Result<Option<NativeArrowBatch>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let limit = subscription_limit(max_rows, "maxRows", self.batch_items)?;
        if let Some(batch) = self
            .arrow_ready
            .lock()
            .expect("subscription Arrow output queue poisoned")
            .pop_front()
        {
            return Ok(Some(to_native_record_batch(batch)?));
        }
        let mut close_rx = self.close_signal.subscribe();
        let mut rx_guard = self.rx.lock().await;
        let Some(rx) = rx_guard.as_mut() else {
            return Ok(None);
        };
        let updates = receive_subscription_updates(
            rx,
            self.pending.as_ref(),
            &mut close_rx,
            limit,
            max_wait_ms,
        )
        .await
        .map_err(|error| blp_error_to_napi(*error))?;

        let Some(updates) = updates else {
            return Ok(None);
        };
        let produced = {
            let mut batcher_state = self
                .arrow_batcher
                .lock()
                .expect("subscription Arrow batcher poisoned");
            let mut produced = Vec::new();
            if batcher_state.0 != limit {
                if let Some(batch) = batcher_state.1.flush() {
                    produced.push(batch);
                }
                *batcher_state = (
                    limit,
                    SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(
                        limit,
                    )),
                );
            }
            let batcher = &mut batcher_state.1;
            for update in updates {
                if let Some(batch) = batcher.append(&update) {
                    produced.push(batch);
                }
            }
            if let Some(batch) = batcher.flush() {
                produced.push(batch);
            }
            produced
        };
        let batch = {
            let mut ready = self
                .arrow_ready
                .lock()
                .expect("subscription Arrow output queue poisoned");
            ready.extend(produced);
            ready
                .pop_front()
                .expect("a non-empty update set must produce an Arrow batch")
        };
        drop(rx_guard);
        Ok(Some(to_native_record_batch(batch)?))
    }

    #[napi]
    pub async fn add(&self, tickers: Vec<String>) -> napi::Result<()> {
        let _mutation = self.mutation.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new(Status::GenericFailure, "subscription closed"));
        }
        let (
            command,
            new_topics,
            service,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            tx,
            status,
        ) = {
            let guard = self.stream.lock().await;
            let handle = guard
                .as_ref()
                .ok_or_else(|| Error::new(Status::GenericFailure, "subscription closed"))?;
            if handle.tx.is_closed() {
                return Err(coded(
                    Status::GenericFailure,
                    "INTERNAL",
                    "subscription stream is closed",
                ));
            }

            let new_topics: Vec<String> = {
                let snapshot = handle.status.load();
                let mut seen = std::collections::HashSet::new();
                tickers
                    .into_iter()
                    .filter(|ticker| {
                        seen.insert(ticker.clone()) && !snapshot.topic_to_key().contains_key(ticker)
                    })
                    .collect()
            };
            if new_topics.is_empty() {
                return Ok(());
            }

            let command = handle
                .claim
                .as_ref()
                .ok_or_else(|| Error::new(Status::GenericFailure, "subscription already closed"))?
                .command_handle()
                .map_err(blp_async_error_to_napi)?;

            (
                command,
                new_topics,
                handle.service.clone(),
                handle.fields.clone(),
                handle.all_fields,
                handle.options.clone(),
                handle.flush_threshold,
                handle.overflow_policy,
                handle.tx.clone(),
                handle.status.clone(),
            )
        };

        command
            .add_topics(
                service,
                new_topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                tx,
                status,
            )
            .await
            .map_err(blp_async_error_to_napi)?;
        Ok(())
    }

    #[napi]
    pub async fn remove(&self, tickers: Vec<String>) -> napi::Result<()> {
        let _mutation = self.mutation.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new(Status::GenericFailure, "subscription closed"));
        }
        let (command, status) = {
            let guard = self.stream.lock().await;
            let handle = guard
                .as_ref()
                .ok_or_else(|| Error::new(Status::GenericFailure, "subscription closed"))?;
            let command = handle
                .claim
                .as_ref()
                .ok_or_else(|| Error::new(Status::GenericFailure, "subscription already closed"))?
                .command_handle()
                .map_err(blp_async_error_to_napi)?;
            (command, handle.status.clone())
        };

        let (keys_to_remove, topics_to_remove) = {
            let snapshot = status.load();
            let mut seen_keys = std::collections::HashSet::new();
            let mut keys_to_remove = Vec::new();
            let mut topics_to_remove = Vec::new();
            for ticker in tickers {
                if let Some(&key) = snapshot.topic_to_key().get(&ticker) {
                    if seen_keys.insert(key) {
                        keys_to_remove.push(key);
                        topics_to_remove.push(ticker);
                    }
                }
            }
            (keys_to_remove, topics_to_remove)
        };
        if keys_to_remove.is_empty() {
            return Ok(());
        }

        command
            .unsubscribe(keys_to_remove)
            .await
            .map_err(blp_async_error_to_napi)?;

        if self.stream.lock().await.is_some() {
            status.update(|state| {
                for ticker in &topics_to_remove {
                    state.drop_topic(ticker);
                }
            });
        }

        Ok(())
    }

    #[napi(getter)]
    pub fn tickers(&self) -> Vec<String> {
        self.status.load().topics().to_vec()
    }

    #[napi(getter)]
    pub fn fields(&self) -> Vec<String> {
        self.fields_snapshot.as_ref().clone()
    }

    #[napi(getter)]
    pub fn is_active(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && !self.stream_sender.is_closed()
            && self.status.load().has_active_topics()
    }

    #[napi(getter)]
    pub fn stats(&self) -> SubscriptionStats {
        let snapshot = self.status.load();
        let metrics: Vec<_> = snapshot.fields_metrics().values().cloned().collect();
        SubscriptionStats {
            messages_received: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.messages_received.load(Ordering::Relaxed))
                    .sum(),
            ),
            dropped_batches: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.dropped_batches.load(Ordering::Relaxed))
                    .sum(),
            ),
            batches_sent: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.batches_sent.load(Ordering::Relaxed))
                    .sum(),
            ),
            slow_consumer: metrics
                .iter()
                .any(|metric| metric.slow_consumer.load(Ordering::Relaxed)),
        }
    }

    #[napi]
    pub async fn unsubscribe(
        &self,
        drain: Option<bool>,
    ) -> napi::Result<Option<Vec<NativeSubscriptionUpdateBatch>>> {
        let drain = drain.unwrap_or(false);
        let (_mutation, close_result) = self.close_for_unsubscribe(drain).await;
        let mut rx_guard = self.rx.lock().await;
        let rx = rx_guard.take();
        drop(rx_guard);
        let drained_updates = Self::take_drained_updates(self.pending.as_ref(), rx, drain);
        self.arrow_ready
            .lock()
            .expect("subscription Arrow output queue poisoned")
            .clear();
        let drained_updates = drained_updates.map_err(|error| blp_error_to_napi(*error))?;
        close_result?;

        let mut remaining = Vec::new();
        if !drained_updates.is_empty() {
            let mut last_layout = self
                .scalar_layout
                .lock()
                .expect("subscription scalar layout poisoned");
            let mut current =
                Vec::with_capacity(subscription_batch_capacity_hint(self.batch_items));
            let mut current_layout: Option<Arc<FieldLayout>> = None;
            for update in drained_updates {
                if !current.is_empty()
                    && (current.len() == self.batch_items
                        || current_layout.as_ref().is_some_and(|layout| {
                            !subscription_layouts_match(layout, &update.layout)
                        }))
                {
                    if let Some(batch) = to_native_update_batch(current, &mut last_layout) {
                        remaining.push(batch);
                    }
                    current =
                        Vec::with_capacity(subscription_batch_capacity_hint(self.batch_items));
                }
                current_layout = Some(update.layout.clone());
                current.push(update);
            }
            if let Some(batch) = to_native_update_batch(current, &mut last_layout) {
                remaining.push(batch);
            }
        }

        if remaining.is_empty() {
            Ok(None)
        } else {
            Ok(Some(remaining))
        }
    }

    #[napi]
    pub async fn unsubscribe_arrow(
        &self,
        drain: Option<bool>,
    ) -> napi::Result<Option<Vec<NativeArrowBatch>>> {
        let drain = drain.unwrap_or(false);
        let (_mutation, close_result) = self.close_for_unsubscribe(drain).await;
        let mut rx_guard = self.rx.lock().await;
        let rx = rx_guard.take();
        drop(rx_guard);
        let drained_updates = Self::take_drained_updates(self.pending.as_ref(), rx, drain);

        let queued_batches: Vec<RecordBatch> = {
            let mut ready = self
                .arrow_ready
                .lock()
                .expect("subscription Arrow output queue poisoned");
            if drain {
                ready.drain(..).collect()
            } else {
                ready.clear();
                Vec::new()
            }
        };
        let drained_updates = drained_updates.map_err(|error| blp_error_to_napi(*error))?;
        close_result?;
        let mut remaining = queued_batches
            .into_iter()
            .map(to_native_record_batch)
            .collect::<napi::Result<Vec<_>>>()?;
        if !drained_updates.is_empty() {
            let mut batcher_state = self
                .arrow_batcher
                .lock()
                .expect("subscription Arrow batcher poisoned");
            if batcher_state.0 != self.batch_items {
                if let Some(batch) = batcher_state.1.flush() {
                    remaining.push(to_native_record_batch(batch)?);
                }
                *batcher_state = (
                    self.batch_items,
                    SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(
                        self.batch_items,
                    )),
                );
            }
            let batcher = &mut batcher_state.1;
            for update in drained_updates {
                if let Some(batch) = batcher.append(&update) {
                    remaining.push(to_native_record_batch(batch)?);
                }
                if batcher.rows() == self.batch_items {
                    if let Some(batch) = batcher.flush() {
                        remaining.push(to_native_record_batch(batch)?);
                    }
                }
            }
            if let Some(batch) = batcher.flush() {
                remaining.push(to_native_record_batch(batch)?);
            }
        }

        if remaining.is_empty() {
            Ok(None)
        } else {
            Ok(Some(remaining))
        }
    }
}

impl JsSubscription {
    async fn close_for_unsubscribe(
        &self,
        drain: bool,
    ) -> (tokio::sync::OwnedMutexGuard<()>, napi::Result<()>) {
        self.closed.store(true, Ordering::Release);
        self.close_signal.send_replace(true);
        let mutation = self.mutation.clone().lock_owned().await;
        let mut stream_guard = self.stream.lock().await;
        let mut close_result = if let Some(handle) = stream_guard.as_ref() {
            if let Some(claim) = handle.claim.as_ref() {
                let keys = handle.status.load().keys().to_vec();
                if keys.is_empty() {
                    Ok(())
                } else {
                    claim
                        .unsubscribe(keys)
                        .await
                        .map_err(blp_async_error_to_napi)
                }
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        if close_result.is_ok() {
            if let Some(handle) = stream_guard.as_mut() {
                handle.status.update(|state| state.clear_active());
            }
        }
        if drain {
            if let Some(claim) = stream_guard
                .as_ref()
                .and_then(|handle| handle.claim.as_ref())
            {
                let mut rx_guard = self.rx.lock().await;
                let forwarding_result = match rx_guard.as_mut() {
                    Some(rx) => {
                        let result = drain_forwarder_into_pending(claim, rx, self.pending.as_ref())
                            .await
                            .map_err(|error| blp_async_error_to_napi(*error));
                        rx.close();
                        result
                    }
                    None => claim
                        .drain_forwarder()
                        .await
                        .map_err(blp_async_error_to_napi),
                };
                if close_result.is_ok() {
                    close_result = forwarding_result;
                }
            }
        }

        // Reaching here means every started await completed. On success the
        // claim is spent; on error dropping it quarantines the dead worker and
        // releases its session permit. Cancellation retains the shared handle
        // and receiver so a later close can resume cleanup.
        stream_guard.take();
        drop(stream_guard);
        (mutation, close_result)
    }

    fn take_drained_updates(
        pending: &StdMutex<VecDeque<StreamBatchResult>>,
        rx: Option<SubscriptionReceiver>,
        drain: bool,
    ) -> Result<Vec<SubscriptionUpdate>, Box<BlpError>> {
        let mut updates = Vec::new();
        let mut terminal_error = None;
        let mut pending = pending.lock().expect("subscription pending queue poisoned");
        if drain {
            while let Some(item) = pending.pop_front() {
                match item {
                    Ok(update) => updates.push(update),
                    Err(error) => {
                        terminal_error.get_or_insert(error);
                    }
                }
            }
        } else {
            pending.clear();
        }
        drop(pending);

        if drain {
            if let Some(mut rx) = rx {
                while let Ok(item) = rx.try_recv() {
                    match item {
                        Ok(update) => updates.push(update),
                        Err(error) => {
                            terminal_error.get_or_insert(error);
                        }
                    }
                }
            }
        }
        match terminal_error {
            Some(error) => Err(Box::new(error)),
            None => Ok(updates),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xbbg_async::engine::state::{subscription_channel, FieldLayout, FieldMeta, UpdateField};
    use xbbg_async::services::ExtractorType;

    fn minimal_input() -> EngineConfigInput {
        EngineConfigInput {
            host: None,
            port: None,
            servers: None,
            zfp_remote: None,
            request_pool_size: None,
            subscription_pool_size: None,
            runtime_worker_threads: None,
            max_subscription_sessions: None,
            shard_requests: None,
            shard_threshold: None,
            shard_chunk_size: None,
            shard_max_concurrent: None,
            validation_mode: None,
            subscription_flush_threshold: None,
            max_event_queue_size: None,
            command_queue_size: None,
            subscription_stream_capacity: None,
            overflow_policy: None,
            warmup_services: None,
            field_cache_path: None,
            auth: None,
            tls: None,
            num_start_attempts: None,
            auto_restart_on_disconnection: None,
            retry_policy: None,
            request_timeout_ms: None,
            streams_deactivated_warn_ms: None,
            keep_alive_enabled: None,
            keep_alive_inactivity_ms: None,
            keep_alive_response_timeout_ms: None,
            slow_consumer_hi_water_mark: None,
            slow_consumer_lo_water_mark: None,
            sdk_log_level: None,
            socks5: None,
        }
    }

    fn direct_servers(config: &EngineConfig) -> &[ServerAddr] {
        match &config.transport {
            Transport::Direct(s) => s.as_slice(),
            other => panic!("expected Direct, got {other}"),
        }
    }
    fn pair(key: &str, value: &str) -> StringPair {
        StringPair {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn subscription_update(layout_version: u32, timestamp_us: i64) -> SubscriptionUpdate {
        subscription_update_with_layout(
            Arc::new(FieldLayout::new(layout_version, Vec::new())),
            timestamp_us,
        )
    }

    fn subscription_update_with_layout(
        layout: Arc<FieldLayout>,
        timestamp_us: i64,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from("IBM US Equity"),
            layout,
            values: Default::default(),
        }
    }

    #[test]
    fn promoted_string_layout_serializes_every_concrete_scalar_into_the_string_slot() {
        let layout = Arc::new(FieldLayout::new(
            7,
            vec![
                FieldMeta::new("BOOL", 0, FieldKind::Str),
                FieldMeta::new("I32", 1, FieldKind::Str),
                FieldMeta::new("I64", 2, FieldKind::Str),
                FieldMeta::new("F64", 3, FieldKind::Str),
                FieldMeta::new("DATE", 4, FieldKind::Str),
            ],
        ));
        let update = SubscriptionUpdate {
            timestamp_us: 1,
            topic_id: 1,
            topic: Arc::from("IBM US Equity"),
            layout,
            values: [
                UpdateField {
                    index: 0,
                    value: UpdateValue::Bool(true),
                },
                UpdateField {
                    index: 1,
                    value: UpdateValue::I32(42),
                },
                UpdateField {
                    index: 2,
                    value: UpdateValue::I64(4_000_000_000),
                },
                UpdateField {
                    index: 3,
                    value: UpdateValue::F64(1.25),
                },
                UpdateField {
                    index: 4,
                    value: UpdateValue::Date32(20_000),
                },
            ]
            .into_iter()
            .collect(),
        };

        let row = to_native_row(update);

        assert_eq!(
            row.string_values,
            vec![
                Some("true".to_string()),
                Some("42".to_string()),
                Some("4000000000".to_string()),
                Some("1.25".to_string()),
                Some("20000".to_string()),
            ]
        );
        assert_eq!(row.bool_values, vec![None; 5]);
        assert_eq!(row.i32_values, vec![None; 5]);
        assert_eq!(row.i64_values, vec![None; 5]);
        assert_eq!(row.f64_values, vec![None; 5]);
    }

    #[test]
    fn subscription_data_loss_uses_the_structured_native_error_code() {
        let error = blp_error_to_napi(BlpError::SubscriptionDataLoss {
            topic: "IBM US Equity".to_string(),
            detail: "stream queue reached capacity".to_string(),
        });

        assert_eq!(
            error.reason,
            "[XBBG:DATALOSS] Subscription data loss [topic=IBM US Equity]: stream queue reached capacity"
        );
    }

    #[test]
    fn request_input_preserves_all_request_fields() {
        let params = RequestParams::try_from(RequestInput {
            service: "//blp/refdata".to_string(),
            operation: String::new(),
            request_operation: Some("ReferenceDataRequest".to_string()),
            request_id: Some("req-123".to_string()),
            extractor: Some("refdata".to_string()),
            securities: Some(vec!["IBM US Equity".to_string()]),
            security: Some("IBM US Equity".to_string()),
            fields: Some(vec!["PX_LAST".to_string()]),
            overrides: Some(vec![pair("EQY_FUND_CRNCY", "USD")]),
            security_overrides: Some(vec![SecurityOverridesInput {
                security: "IBM US Equity".to_string(),
                overrides: vec![pair("CRNCY", "EUR")],
            }]),
            elements: Some(vec![pair("returnEids", "true")]),
            kwargs: Some(vec![pair("Period", "D")]),
            json_elements: Some(r#"{"nested":{"flag":true},"count":3}"#.to_string()),
            start_date: Some("20240101".to_string()),
            end_date: Some("20240131".to_string()),
            start_datetime: Some("2024-01-01T09:30:00".to_string()),
            end_datetime: Some("2024-01-01T10:00:00".to_string()),
            request_tz: Some("NY".to_string()),
            output_tz: Some("UTC".to_string()),
            event_type: Some("TRADE".to_string()),
            event_types: Some(vec!["TRADE".to_string(), "BID".to_string()]),
            interval: Some(5),
            options: Some(vec![pair("includeConditionCodes", "true")]),
            field_types: Some(vec![pair("PX_LAST", "Float64")]),
            include_security_errors: Some(true),
            return_eids: Some(true),
            validate_fields: Some(false),
            search_spec: Some("price".to_string()),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            format: Some("long_typed".to_string()),
        })
        .expect("request input");

        assert_eq!(params.service, "//blp/refdata");
        assert_eq!(params.operation, "");
        assert_eq!(
            params.request_operation.as_deref(),
            Some("ReferenceDataRequest")
        );
        assert_eq!(params.request_id.as_deref(), Some("req-123"));
        assert_eq!(params.extractor, ExtractorType::RefData);
        assert!(params.extractor_set);
        assert_eq!(
            params.securities.as_deref(),
            Some(&["IBM US Equity".to_string()][..])
        );
        assert_eq!(params.security.as_deref(), Some("IBM US Equity"));
        assert_eq!(params.fields.as_deref(), Some(&["PX_LAST".to_string()][..]));
        assert_eq!(
            params.overrides.as_deref(),
            Some(&[("EQY_FUND_CRNCY".to_string(), "USD".to_string())][..])
        );
        assert_eq!(
            params.security_overrides.as_deref(),
            Some(
                &[(
                    "IBM US Equity".to_string(),
                    vec![("CRNCY".to_string(), "EUR".to_string())]
                )][..]
            )
        );
        let elements = params.elements.as_ref().expect("elements");
        assert!(elements.contains(&("returnEids".to_string(), "true".to_string())));
        assert!(elements.contains(&("nested.flag".to_string(), "true".to_string())));
        assert!(elements.contains(&("count".to_string(), "3".to_string())));
        assert_eq!(
            params
                .kwargs
                .as_ref()
                .and_then(|values| values.get("Period")),
            Some(&"D".to_string())
        );
        assert_eq!(params.start_date.as_deref(), Some("20240101"));
        assert_eq!(params.end_date.as_deref(), Some("20240131"));
        assert_eq!(
            params.start_datetime.as_deref(),
            Some("2024-01-01T09:30:00")
        );
        assert_eq!(params.end_datetime.as_deref(), Some("2024-01-01T10:00:00"));
        assert_eq!(params.request_tz.as_deref(), Some("NY"));
        assert_eq!(params.output_tz.as_deref(), Some("UTC"));
        assert_eq!(params.event_type.as_deref(), Some("TRADE"));
        assert_eq!(
            params.event_types.as_deref(),
            Some(&["TRADE".to_string(), "BID".to_string()][..])
        );
        assert_eq!(params.interval, Some(5));
        assert_eq!(
            params.options.as_deref(),
            Some(&[("includeConditionCodes".to_string(), "true".to_string())][..])
        );
        assert_eq!(
            params
                .field_types
                .as_ref()
                .and_then(|values| values.get("PX_LAST")),
            Some(&"Float64".to_string())
        );
        assert!(params.include_security_errors);
        assert!(params.return_eids);
        assert_eq!(params.validate_fields, Some(false));
        assert_eq!(params.search_spec.as_deref(), Some("price"));
        assert_eq!(
            params.field_ids.as_deref(),
            Some(&["PX_LAST".to_string()][..])
        );
        assert_eq!(params.format.as_deref(), Some("long_typed"));
    }

    #[test]
    fn engine_config_input_rejects_invalid_resource_limits() {
        let err = EngineConfig::try_from(EngineConfigInput {
            runtime_worker_threads: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero runtime worker count should fail");
        assert!(err.to_string().contains("runtimeWorkerThreads"));

        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_pool_size: Some(4),
            max_subscription_sessions: Some(3),
            ..minimal_input()
        })
        .err()
        .expect("subscription prewarm above session cap should fail");
        assert!(err.to_string().contains("maxSubscriptionSessions"));
    }

    #[test]
    fn engine_config_input_defaults_leave_auth_unset() {
        let config =
            EngineConfig::try_from(minimal_input()).expect("default config should convert");

        assert_eq!(config.auth, None);
        let servers = direct_servers(&config);
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].host, "localhost");
        assert_eq!(servers[0].port, 8194);
        assert!(servers[0].proxy.is_none());
        assert_eq!(config.runtime_worker_threads, 2);
        assert_eq!(config.max_subscription_sessions, 32);
    }

    #[test]
    fn engine_config_input_rejects_zero_subscription_stream_capacity() {
        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_stream_capacity: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero subscription stream capacity should fail");

        assert!(
            err.to_string().contains("subscriptionStreamCapacity"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zero_subscription_flush_threshold() {
        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_flush_threshold: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero subscription flush threshold should fail");
        assert!(err.to_string().contains("subscriptionFlushThreshold"));
    }

    #[test]
    fn engine_config_input_maps_bpipe_servers_with_socks5() {
        let config = EngineConfig::try_from(EngineConfigInput {
            servers: Some(vec![
                ServerAddressInput {
                    host: "primary.example.com".to_string(),
                    port: 8194,
                },
                ServerAddressInput {
                    host: "secondary.example.com".to_string(),
                    port: 8196,
                },
            ]),
            request_pool_size: Some(4),
            subscription_pool_size: Some(2),
            shard_requests: Some(true),
            shard_threshold: Some(5),
            shard_chunk_size: Some(3),
            shard_max_concurrent: Some(2),
            validation_mode: Some("strict".to_string()),
            subscription_flush_threshold: Some(8),
            max_event_queue_size: Some(16_000),
            command_queue_size: Some(512),
            subscription_stream_capacity: Some(1024),
            overflow_policy: Some("block".to_string()),
            warmup_services: Some(vec!["//blp/refdata".to_string()]),
            field_cache_path: Some("/tmp/xbbg-field-cache.json".to_string()),
            auth: Some(AuthConfigInput {
                method: "manual".to_string(),
                app_name: Some("app-name".to_string()),
                dir_property: None,
                user_id: Some("123456".to_string()),
                ip_address: Some("10.0.0.1".to_string()),
                token: None,
            }),
            num_start_attempts: Some(5),
            auto_restart_on_disconnection: Some(false),
            retry_policy: Some(RetryPolicyInput {
                max_retries: Some(3),
                initial_delay_ms: Some(250),
                backoff_factor: Some(1.5),
                max_delay_ms: Some(5_000),
            }),
            request_timeout_ms: Some(12_000),
            streams_deactivated_warn_ms: Some(45_000),
            keep_alive_enabled: Some(false),
            keep_alive_inactivity_ms: Some(25_000),
            keep_alive_response_timeout_ms: Some(11_000),
            slow_consumer_hi_water_mark: Some(0.8),
            slow_consumer_lo_water_mark: Some(0.4),
            sdk_log_level: Some("warn".to_string()),
            socks5: Some(Socks5ConfigInput {
                host: "proxy.example.com".to_string(),
                port: 1080,
            }),
            ..minimal_input()
        })
        .expect("direct+SOCKS5 config should convert");

        let servers = direct_servers(&config);
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].host, "primary.example.com");
        assert_eq!(servers[0].port, 8194);
        assert_eq!(servers[1].host, "secondary.example.com");
        assert_eq!(servers[1].port, 8196);
        // SOCKS5 is broadcast across every server.
        for s in servers {
            let proxy = s.proxy.as_ref().expect("proxy should be set");
            assert_eq!(proxy.host, "proxy.example.com");
            assert_eq!(proxy.port, 1080);
        }
        assert!(config.tls.is_none());
        assert_eq!(
            config.auth,
            Some(AuthConfig::Manual {
                app_name: "app-name".to_string(),
                user_id: "123456".to_string(),
                ip_address: "10.0.0.1".to_string(),
            })
        );
        assert_eq!(
            config.field_cache_path,
            Some(std::path::PathBuf::from("/tmp/xbbg-field-cache.json"))
        );
        assert_eq!(config.num_start_attempts, 5);
        assert!(!config.auto_restart_on_disconnection);
        assert!(config.shard_requests);
        assert_eq!(config.shard_threshold, 5);
        assert_eq!(config.shard_chunk_size, 3);
        assert_eq!(config.shard_max_concurrent, 2);
        assert_eq!(config.retry_policy.max_retries, 3);
        assert_eq!(config.retry_policy.initial_delay_ms, 250);
        assert_eq!(config.retry_policy.backoff_factor, 1.5);
        assert_eq!(config.retry_policy.max_delay_ms, 5_000);
        assert_eq!(config.request_timeout_ms, 12_000);
        assert_eq!(config.streams_deactivated_warn_ms, 45_000);
    }

    #[test]
    fn engine_config_input_zfp_with_tls_resolves_zfp_transport() {
        let config = EngineConfig::try_from(EngineConfigInput {
            zfp_remote: Some("8194".to_string()),
            tls: Some(TlsConfigInput {
                client_credentials: Some("/tmp/client.p12".to_string()),
                client_credentials_password: Some("secret".to_string()),
                trust_material: Some("/tmp/trust.p7".to_string()),
                handshake_timeout_ms: Some(2000),
                crl_fetch_timeout_ms: Some(3000),
            }),
            ..minimal_input()
        })
        .expect("ZFP+TLS config should convert");

        match &config.transport {
            Transport::Zfp(remote) => {
                assert_eq!(*remote, xbbg_core::zfp::ZfpRemote::Remote8194);
            }
            other => panic!("expected Zfp, got {other}"),
        }
        let tls = config.tls.as_ref().expect("tls should be set");
        assert_eq!(tls.client_credentials, "/tmp/client.p12");
        assert_eq!(tls.client_credentials_password, "secret");
        assert_eq!(tls.trust_material, "/tmp/trust.p7");
        assert_eq!(tls.handshake_timeout_ms, Some(2000));
        assert_eq!(tls.crl_fetch_timeout_ms, Some(3000));
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_host() {
        let err = EngineConfig::try_from(EngineConfigInput {
            host: Some("bpipe.firm.com".to_string()),
            zfp_remote: Some("8194".to_string()),
            ..minimal_input()
        })
        .err()
        .expect("zfp + host should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_servers() {
        let err = EngineConfig::try_from(EngineConfigInput {
            servers: Some(vec![ServerAddressInput {
                host: "x".to_string(),
                port: 8194,
            }]),
            zfp_remote: Some("8196".to_string()),
            ..minimal_input()
        })
        .err()
        .expect("zfp + servers should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_socks5() {
        let err = EngineConfig::try_from(EngineConfigInput {
            zfp_remote: Some("8194".to_string()),
            socks5: Some(Socks5ConfigInput {
                host: "proxy".to_string(),
                port: 1080,
            }),
            ..minimal_input()
        })
        .err()
        .expect("zfp + socks5 should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_requires_auth_fields_for_selected_method() {
        let err = match EngineConfig::try_from(EngineConfigInput {
            auth: Some(AuthConfigInput {
                method: "app".to_string(),
                app_name: None,
                dir_property: None,
                user_id: None,
                ip_address: None,
                token: None,
            }),
            ..minimal_input()
        }) {
            Ok(_) => panic!("missing appName should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("auth.appName is required"));
    }

    #[test]
    fn default_subscription_reads_one_update() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update(1, 20)))
                .await
                .expect("second");
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);

            let updates = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1, None)
                .await
                .expect("read")
                .expect("updates");

            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].timestamp_us, 10);
            assert_eq!(
                rx.try_recv()
                    .expect("second remains")
                    .expect("update")
                    .timestamp_us,
                20
            );
        });
    }

    #[test]
    fn bounded_subscription_read_flushes_sparse_partial_batch() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("update");
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);

            let updates =
                receive_subscription_updates(&mut rx, &pending, &mut close_rx, 4, Some(5))
                    .await
                    .expect("read")
                    .expect("partial batch");

            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].timestamp_us, 10);
        });
    }

    #[test]
    fn layout_change_is_deferred_without_losing_update() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update(2, 20)))
                .await
                .expect("second");
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);

            let first = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, None)
                .await
                .expect("first read")
                .expect("first batch");
            let second = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, Some(0))
                .await
                .expect("second read")
                .expect("second batch");

            assert_eq!(first[0].layout.version, 1);
            assert_eq!(second[0].layout.version, 2);
        });
    }

    #[test]
    fn same_version_schema_change_is_split_and_republished() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let first_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("PX_LAST", 0, FieldKind::F64)],
            ));
            let second_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("BID", 0, FieldKind::F64)],
            ));
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update_with_layout(first_layout, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update_with_layout(second_layout, 20)))
                .await
                .expect("second");
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);

            let first = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, None)
                .await
                .expect("first read")
                .expect("first batch");
            let second = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, Some(0))
                .await
                .expect("second read")
                .expect("second batch");
            let mut last_layout = None;
            let first = to_native_update_batch(first, &mut last_layout).expect("first output");
            let second = to_native_update_batch(second, &mut last_layout).expect("second output");

            assert_eq!(first.updates[0].timestamp_us, 10);
            assert!(first.layout.is_some());
            assert_eq!(second.updates[0].timestamp_us, 20);
            assert!(second.layout.is_some());
        });
    }

    #[test]
    fn cancelled_partial_read_restores_consumed_updates() {
        use std::future::Future;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(1);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first update");
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);
            let mut reader = Box::pin(receive_subscription_updates(
                &mut rx,
                &pending,
                &mut close_rx,
                2,
                Some(60_000),
            ));
            // Stop deterministically while the partial batch awaits its second
            // update, rather than racing abort against a completed read.
            std::future::poll_fn(|cx| {
                assert!(reader.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(reader);

            tx.send(Ok(subscription_update(1, 20)))
                .await
                .expect("second update");
            let restored =
                receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, Some(0))
                    .await
                    .expect("resumed read")
                    .expect("restored batch");
            assert_eq!(
                restored
                    .iter()
                    .map(|update| update.timestamp_us)
                    .collect::<Vec<_>>(),
                [10, 20],
            );
        });
    }

    #[test]
    fn close_before_read_is_observed_without_a_lost_wakeup() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (_tx, mut rx) = subscription_channel(1);
            let pending = StdMutex::new(VecDeque::new());
            let (close_tx, initial_rx) = watch::channel(false);
            drop(initial_rx);
            close_tx.send_replace(true);
            let mut close_rx = close_tx.subscribe();

            let updates = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1, None)
                .await
                .expect("read");
            assert!(updates.is_none());
        });
    }

    #[test]
    fn full_queue_terminal_error_is_delivered_after_buffered_data_and_before_end() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(1);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("update");
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "IBM US Equity".to_string(),
                detail: "stream queue reached capacity".to_string(),
            });
            let pending = StdMutex::new(VecDeque::new());
            let (_close_tx, mut close_rx) = watch::channel(false);

            let updates = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2, None)
                .await
                .expect("buffered update")
                .expect("batch");
            assert_eq!(updates[0].timestamp_us, 10);

            let error = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1, None)
                .await
                .expect_err("terminal data-loss error");
            assert!(matches!(
                *error,
                BlpError::SubscriptionDataLoss {
                    ref topic,
                    ref detail,
                } if topic == "IBM US Equity" && detail == "stream queue reached capacity"
            ));

            let end = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1, None)
                .await
                .expect("end");
            assert!(end.is_none());
        });
    }

    #[test]
    fn errors_only_drain_returns_the_terminal_failure() {
        let (tx, rx) = subscription_channel(1);
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "ES1 Index".to_string(),
            detail: "Bloomberg reported DATALOSS".to_string(),
        });
        let pending = StdMutex::new(VecDeque::new());

        let error = JsSubscription::take_drained_updates(&pending, Some(rx), true)
            .expect_err("errors-only drain must fail");

        assert!(matches!(
            *error,
            BlpError::SubscriptionDataLoss {
                ref topic,
                ref detail,
            } if topic == "ES1 Index" && detail == "Bloomberg reported DATALOSS"
        ));
    }
}
