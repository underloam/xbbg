//! Engine configuration from host JSON.
//!
//! [`EngineConfigSpec`] is the full `@xbbg/core` `EngineConfig` object
//! (camelCase members, unknown members rejected). Absent members keep
//! [`EngineConfig::default`]; present members overlay it with napi's exact
//! semantics: `validationMode`, `overflowPolicy` and `keyColumn` are parsed
//! first, numeric members overlay next, `retryPolicy` overlays field by field,
//! the signed millisecond durations must be non-negative, `sdkLogLevel` is
//! parsed, and [`xbbg_async::config::normalize`] then checks the flush
//! threshold, keep-alive values, watermarks, auth, transport, TLS and the
//! engine invariants, with every engine field named by its camelCase member.
//! `host`/`port` count as explicit (which matters for `zfpRemote`) when the
//! member is present.
//!
//! TLS materials are `{"path": "<file>"}` or `{"bytes": "<standard base64>"}`;
//! messages never echo paths, passwords or bytes.
//!
//! Errors: a malformed object (unknown member, wrong type, bad base64) is
//! ARGUMENT; a well-formed value the engine rejects is VALIDATION with the
//! napi text (no `Configuration error:` prefix) and, where one member is at
//! fault before normalization, that member as the context `element`.

use std::str::FromStr;
use std::sync::Arc;

use base64::Engine as _;
use serde::Deserialize;
use xbbg_async::BlpAsyncError;
use xbbg_async::KeyColumn;
use xbbg_async::config::{AuthInput, ConfigInput, TlsInput, TransportInput};
use xbbg_async::engine::{EngineConfig, OverflowPolicy, TlsMaterial, ValidationMode};
use xbbg_async::sdk_logging::SdkLogLevel;

use crate::errors::{HostError, parse_args};

/// The host's `EngineConfig` object.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineConfigSpec {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub servers: Option<Vec<ServerAddressSpec>>,
    pub zfp_remote: Option<String>,
    pub request_pool_size: Option<u32>,
    pub subscription_pool_size: Option<u32>,
    pub runtime_worker_threads: Option<u32>,
    pub max_subscription_sessions: Option<u32>,
    pub shard_requests: Option<bool>,
    pub shard_threshold: Option<u32>,
    pub shard_chunk_size: Option<u32>,
    pub shard_max_concurrent: Option<u32>,
    pub validation_mode: Option<String>,
    pub key_column: Option<String>,
    pub subscription_flush_threshold: Option<u32>,
    pub max_event_queue_size: Option<u32>,
    pub command_queue_size: Option<u32>,
    pub subscription_stream_capacity: Option<u32>,
    pub overflow_policy: Option<String>,
    pub warmup_services: Option<Vec<String>>,
    pub field_cache_path: Option<String>,
    pub bql_app_name: Option<String>,
    pub auth: Option<AuthConfigSpec>,
    pub tls: Option<TlsConfigSpec>,
    pub num_start_attempts: Option<u32>,
    pub auto_restart_on_disconnection: Option<bool>,
    pub retry_policy: Option<RetryPolicySpec>,
    pub request_timeout_ms: Option<i64>,
    pub streams_deactivated_warn_ms: Option<i64>,
    pub keep_alive_enabled: Option<bool>,
    pub keep_alive_inactivity_ms: Option<i32>,
    pub keep_alive_response_timeout_ms: Option<i32>,
    pub slow_consumer_hi_water_mark: Option<f64>,
    pub slow_consumer_lo_water_mark: Option<f64>,
    pub sdk_log_level: Option<String>,
    pub socks5: Option<Socks5Spec>,
}

/// One `servers[]` entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerAddressSpec {
    pub host: String,
    pub port: u16,
}

/// The `socks5` proxy applied to every server.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Socks5Spec {
    pub host: String,
    pub port: u16,
}

/// The `auth` object; `method` is required when the object is present.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthConfigSpec {
    pub method: String,
    pub app_name: Option<String>,
    pub dir_property: Option<String>,
    pub user_id: Option<String>,
    pub ip_address: Option<String>,
    pub token: Option<String>,
}

/// The `tls` object.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TlsConfigSpec {
    pub client_credentials: Option<TlsMaterialSpec>,
    pub client_credentials_password: Option<String>,
    pub trust_material: Option<TlsMaterialSpec>,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
}

impl std::fmt::Debug for TlsConfigSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfigSpec")
            .field("client_credentials", &self.client_credentials)
            .field(
                "client_credentials_password",
                &self
                    .client_credentials_password
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("trust_material", &self.trust_material)
            .field("handshake_timeout_ms", &self.handshake_timeout_ms)
            .field("crl_fetch_timeout_ms", &self.crl_fetch_timeout_ms)
            .finish()
    }
}

/// A TLS file path or DER blob; exactly one member is set.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TlsMaterialSpec {
    pub path: Option<String>,
    /// Standard base64 (with padding) of the DER bytes.
    pub bytes: Option<String>,
}

impl std::fmt::Debug for TlsMaterialSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match (&self.path, &self.bytes) {
            (Some(_), None) => "path",
            (None, Some(_)) => "bytes",
            _ => "invalid",
        };
        write!(f, "TlsMaterialSpec({kind}, <redacted>)")
    }
}

impl TlsMaterialSpec {
    /// Own the material before any engine check; blobs are decoded once.
    fn into_material(self, label: &str) -> Result<TlsMaterial, HostError> {
        match (self.path, self.bytes) {
            (Some(path), None) => Ok(TlsMaterial::Path(path)),
            (None, Some(encoded)) => base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map(|bytes| TlsMaterial::Blob(Arc::from(bytes)))
                .map_err(|_| {
                    HostError::argument(format!("{label}.bytes must be standard base64"))
                        .with_element(label)
                }),
            _ => Err(HostError::argument(format!(
                "{label} must set exactly one of 'path' or 'bytes'"
            ))
            .with_element(label)),
        }
    }
}

/// `retryPolicy`; each present member overlays `RetryPolicy::default()`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryPolicySpec {
    pub max_retries: Option<u32>,
    pub initial_delay_ms: Option<i64>,
    pub backoff_factor: Option<f64>,
    pub max_delay_ms: Option<i64>,
}

/// Host labels for engine field identifiers in configuration messages
/// (napi `config_field_name`).
pub fn config_field_name(field: &str) -> &str {
    match field {
        "auth_method" => "auth.method",
        "app_name" => "auth.appName",
        "bql_app_name" => "bqlAppName",
        "dir_property" => "auth.dirProperty",
        "user_id" => "auth.userId",
        "ip_address" => "auth.ipAddress",
        "token" => "auth.token",
        "tls_client_credentials" => "tls.clientCredentials",
        "tls_trust_material" => "tls.trustMaterial",
        "tls_handshake_timeout_ms" => "tls.handshakeTimeoutMs",
        "tls_crl_fetch_timeout_ms" => "tls.crlFetchTimeoutMs",
        "zfp_remote" => "zfpRemote",
        "socks5_host" => "socks5.host",
        "socks5_port" => "socks5.port",
        "socks5_host/socks5_port" => "socks5",
        "request_pool_size" => "requestPoolSize",
        "runtime_worker_threads" => "runtimeWorkerThreads",
        "max_subscription_sessions" => "maxSubscriptionSessions",
        "subscription_pool_size" => "subscriptionPoolSize",
        "command_queue_size" => "commandQueueSize",
        "subscription_stream_capacity" => "subscriptionStreamCapacity",
        "subscription_flush_threshold" => "subscriptionFlushThreshold",
        "shard_threshold" => "shardThreshold",
        "shard_chunk_size" => "shardChunkSize",
        "shard_max_concurrent" => "shardMaxConcurrent",
        "keep_alive_inactivity_ms" => "keepAliveInactivityMs",
        "keep_alive_response_timeout_ms" => "keepAliveResponseTimeoutMs",
        "slow_consumer_hi_water_mark" => "slowConsumerHiWaterMark",
        "slow_consumer_lo_water_mark" => "slowConsumerLoWaterMark",
        other => other,
    }
}

fn non_negative_duration(value: i64, label: &str) -> Result<u64, HostError> {
    u64::try_from(value).map_err(|_| {
        HostError::validation(format!(
            "{label} must be a non-negative integer number of milliseconds"
        ))
        .with_element(label)
    })
}

fn parse_member<T, E: ToString>(
    value: Option<String>,
    default: T,
    label: &str,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<T, HostError> {
    match value {
        Some(value) => parse(&value)
            .map_err(|error| HostError::validation(error.to_string()).with_element(label)),
        None => Ok(default),
    }
}

impl EngineConfigSpec {
    /// Parse the host JSON object (ARGUMENT on a malformed object).
    pub fn from_json(json: &str) -> Result<Self, HostError> {
        parse_args(json)
    }

    /// Overlay on [`EngineConfig::default`] and normalize, in napi's check order.
    pub fn into_engine_config(self) -> Result<EngineConfig, HostError> {
        let mut tls = self.tls;
        let client_credentials = tls
            .as_mut()
            .and_then(|tls| tls.client_credentials.take())
            .map(|material| material.into_material("tls.clientCredentials"))
            .transpose()?;
        let trust_material = tls
            .as_mut()
            .and_then(|tls| tls.trust_material.take())
            .map(|material| material.into_material("tls.trustMaterial"))
            .transpose()?;

        let mut config = EngineConfig::default();
        let validation_mode = parse_member(
            self.validation_mode,
            config.validation_mode,
            "validationMode",
            ValidationMode::from_str,
        )?;
        let overflow_policy = parse_member(
            self.overflow_policy,
            config.overflow_policy,
            "overflowPolicy",
            OverflowPolicy::from_str,
        )?;
        let key_column = parse_member(
            self.key_column,
            config.key_column,
            "keyColumn",
            KeyColumn::from_str,
        )?;

        let usize_members = [
            (self.request_pool_size, &mut config.request_pool_size),
            (
                self.subscription_pool_size,
                &mut config.subscription_pool_size,
            ),
            (
                self.runtime_worker_threads,
                &mut config.runtime_worker_threads,
            ),
            (
                self.max_subscription_sessions,
                &mut config.max_subscription_sessions,
            ),
            (self.shard_threshold, &mut config.shard_threshold),
            (self.shard_chunk_size, &mut config.shard_chunk_size),
            (self.shard_max_concurrent, &mut config.shard_max_concurrent),
            (
                self.subscription_flush_threshold,
                &mut config.subscription_flush_threshold,
            ),
            (self.max_event_queue_size, &mut config.max_event_queue_size),
            (self.command_queue_size, &mut config.command_queue_size),
            (
                self.subscription_stream_capacity,
                &mut config.subscription_stream_capacity,
            ),
            (self.num_start_attempts, &mut config.num_start_attempts),
        ];
        for (value, target) in usize_members {
            if let Some(value) = value {
                *target = value as usize;
            }
        }
        if let Some(enabled) = self.shard_requests {
            config.shard_requests = enabled;
        }
        if let Some(services) = self.warmup_services {
            config.warmup_services = services;
        }
        if let Some(path) = self.field_cache_path {
            config.field_cache_path = Some(path.into());
        }
        config.bql_app_name = self.bql_app_name;
        if let Some(auto_restart) = self.auto_restart_on_disconnection {
            config.auto_restart_on_disconnection = auto_restart;
        }
        if let Some(retry) = self.retry_policy {
            if let Some(max_retries) = retry.max_retries {
                config.retry_policy.max_retries = max_retries;
            }
            if let Some(delay) = retry.initial_delay_ms {
                config.retry_policy.initial_delay_ms =
                    non_negative_duration(delay, "retryPolicy.initialDelayMs")?;
            }
            if let Some(factor) = retry.backoff_factor {
                config.retry_policy.backoff_factor = factor;
            }
            if let Some(delay) = retry.max_delay_ms {
                config.retry_policy.max_delay_ms =
                    non_negative_duration(delay, "retryPolicy.maxDelayMs")?;
            }
        }
        if let Some(timeout) = self.request_timeout_ms {
            config.request_timeout_ms = non_negative_duration(timeout, "requestTimeoutMs")?;
        }
        if let Some(warn) = self.streams_deactivated_warn_ms {
            config.streams_deactivated_warn_ms =
                non_negative_duration(warn, "streamsDeactivatedWarnMs")?;
        }
        if let Some(enabled) = self.keep_alive_enabled {
            config.keep_alive_enabled = enabled;
        }
        if let Some(value) = self.keep_alive_inactivity_ms {
            config.keep_alive_inactivity_ms = Some(value);
        }
        if let Some(value) = self.keep_alive_response_timeout_ms {
            config.keep_alive_response_timeout_ms = Some(value);
        }
        config.sdk_log_level = parse_member(
            self.sdk_log_level,
            config.sdk_log_level,
            "sdkLogLevel",
            SdkLogLevel::from_str,
        )?;
        config.validation_mode = validation_mode;
        config.overflow_policy = overflow_policy;
        config.key_column = key_column;

        let auth = self.auth.as_ref();
        let tls = tls.as_ref();
        let servers = self.servers.as_deref().unwrap_or_default();
        xbbg_async::config::normalize(
            config,
            ConfigInput {
                auth: AuthInput {
                    method: auth.map(|auth| auth.method.as_str()),
                    app_name: auth.and_then(|auth| auth.app_name.as_deref()),
                    dir_property: auth.and_then(|auth| auth.dir_property.as_deref()),
                    user_id: auth.and_then(|auth| auth.user_id.as_deref()),
                    ip_address: auth.and_then(|auth| auth.ip_address.as_deref()),
                    token: auth.and_then(|auth| auth.token.as_deref()),
                },
                tls: TlsInput {
                    client_credentials,
                    client_credentials_password: tls
                        .and_then(|tls| tls.client_credentials_password.as_deref()),
                    trust_material,
                    handshake_timeout_ms: tls.and_then(|tls| tls.handshake_timeout_ms),
                    crl_fetch_timeout_ms: tls.and_then(|tls| tls.crl_fetch_timeout_ms),
                },
                transport: TransportInput {
                    host: self.host.as_deref().unwrap_or("localhost"),
                    port: self.port.unwrap_or(8194),
                    explicit_host_port: self.host.is_some() || self.port.is_some(),
                    zfp_remote: self.zfp_remote.as_deref(),
                    socks5_host: self.socks5.as_ref().map(|proxy| proxy.host.as_str()),
                    socks5_port: self.socks5.as_ref().map(|proxy| proxy.port),
                },
                slow_consumer_hi_water_mark: self.slow_consumer_hi_water_mark,
                slow_consumer_lo_water_mark: self.slow_consumer_lo_water_mark,
            },
            servers
                .iter()
                .map(|server| (server.host.as_str(), server.port)),
            config_field_name,
        )
        .map_err(|error| match error {
            BlpAsyncError::ConfigError { detail } => HostError::validation(detail),
            other => HostError::from_async(other),
        })
    }
}

#[cfg(test)]
mod tests {
    use xbbg_async::engine::Transport;
    use xbbg_core::AuthConfig;

    use super::*;
    use crate::errors::HostErrorKind;

    fn convert(json: &str) -> Result<EngineConfig, HostError> {
        EngineConfigSpec::from_json(json)?.into_engine_config()
    }

    fn rejected(json: &str) -> HostError {
        match convert(json) {
            Ok(_) => panic!("config should be rejected: {json}"),
            Err(error) => error,
        }
    }

    fn validation(json: &str) -> HostError {
        let error = rejected(json);
        assert_eq!(error.kind, HostErrorKind::Validation, "{error:?}");
        error
    }

    /// `(host, port, socks5 proxy)` per direct server.
    type Server = (String, u16, Option<(String, u16)>);

    fn direct(config: &EngineConfig) -> Vec<Server> {
        let Transport::Direct(servers) = &config.transport else {
            panic!("expected direct transport");
        };
        servers
            .iter()
            .map(|server| {
                (
                    server.host.clone(),
                    server.port,
                    server
                        .proxy
                        .as_ref()
                        .map(|proxy| (proxy.host.clone(), proxy.port)),
                )
            })
            .collect()
    }

    #[test]
    fn empty_object_keeps_engine_defaults() {
        let config = convert("{}").unwrap();
        let defaults = EngineConfig::default();
        assert_eq!(direct(&config), [("localhost".to_string(), 8194, None)]);
        assert_eq!(config.request_pool_size, defaults.request_pool_size);
        assert_eq!(config.subscription_pool_size, 1);
        assert_eq!(config.runtime_worker_threads, 2);
        assert_eq!(config.max_subscription_sessions, 32);
        assert_eq!(config.subscription_flush_threshold, 16);
        assert_eq!(config.subscription_stream_capacity, 256);
        assert_eq!(config.command_queue_size, 256);
        assert_eq!(config.max_event_queue_size, 10_000);
        assert_eq!(config.validation_mode, ValidationMode::Disabled);
        assert_eq!(config.overflow_policy, OverflowPolicy::DropNewest);
        assert_eq!(config.key_column, KeyColumn::Ticker);
        assert_eq!(config.warmup_services, defaults.warmup_services);
        assert_eq!(config.retry_policy.max_retries, 0);
        assert_eq!(config.retry_policy.initial_delay_ms, 1000);
        assert_eq!(config.retry_policy.backoff_factor, 2.0);
        assert_eq!(config.retry_policy.max_delay_ms, 30_000);
        assert_eq!(config.request_timeout_ms, 0);
        assert_eq!(config.streams_deactivated_warn_ms, 30_000);
        assert!(config.keep_alive_enabled);
        assert!(config.auto_restart_on_disconnection);
        assert_eq!(config.num_start_attempts, 3);
        assert!(config.auth.is_none());
        assert!(config.tls.is_none());
        assert!(config.bql_app_name.is_none());
        assert_eq!(config.sdk_log_level, SdkLogLevel::Off);
        assert_eq!(config.slow_consumer_hi_water_mark, None);
    }

    #[test]
    fn present_members_overlay_defaults() {
        let config = convert(
            r#"{
                "host": "bpipe.example", "port": 8196,
                "requestPoolSize": 3, "subscriptionPoolSize": 0, "runtimeWorkerThreads": 4,
                "maxSubscriptionSessions": 8, "shardRequests": true, "shardThreshold": 30,
                "shardChunkSize": 10, "shardMaxConcurrent": 2, "validationMode": "STRICT",
                "keyColumn": "Security", "subscriptionFlushThreshold": 32,
                "maxEventQueueSize": 500, "commandQueueSize": 64,
                "subscriptionStreamCapacity": 128, "overflowPolicy": "drop_oldest",
                "warmupServices": ["//blp/mktdata"], "fieldCachePath": "cache/fields.json",
                "bqlAppName": "bql-app", "numStartAttempts": 5,
                "autoRestartOnDisconnection": false,
                "retryPolicy": {"maxRetries": 2, "backoffFactor": 1.5},
                "requestTimeoutMs": 9000, "streamsDeactivatedWarnMs": 0,
                "keepAliveEnabled": false, "keepAliveInactivityMs": 40000,
                "keepAliveResponseTimeoutMs": 15000, "slowConsumerHiWaterMark": 0.9,
                "slowConsumerLoWaterMark": 0.25, "sdkLogLevel": "warning",
                "auth": {"method": " App ", "appName": "my-app"}
            }"#,
        )
        .unwrap();
        assert_eq!(direct(&config), [("bpipe.example".to_string(), 8196, None)]);
        assert_eq!(config.request_pool_size, 3);
        assert_eq!(config.subscription_pool_size, 0);
        assert_eq!(config.runtime_worker_threads, 4);
        assert_eq!(config.max_subscription_sessions, 8);
        assert!(config.shard_requests);
        assert_eq!(config.shard_threshold, 30);
        assert_eq!(config.shard_chunk_size, 10);
        assert_eq!(config.shard_max_concurrent, 2);
        assert_eq!(config.validation_mode, ValidationMode::Strict);
        assert_eq!(config.key_column, KeyColumn::Security);
        assert_eq!(config.subscription_flush_threshold, 32);
        assert_eq!(config.max_event_queue_size, 500);
        assert_eq!(config.command_queue_size, 64);
        assert_eq!(config.subscription_stream_capacity, 128);
        assert_eq!(config.overflow_policy, OverflowPolicy::DropOldest);
        assert_eq!(config.warmup_services, ["//blp/mktdata"]);
        assert_eq!(
            config.field_cache_path.as_deref(),
            Some(std::path::Path::new("cache/fields.json"))
        );
        assert_eq!(config.bql_app_name.as_deref(), Some("bql-app"));
        assert_eq!(config.num_start_attempts, 5);
        assert!(!config.auto_restart_on_disconnection);
        assert_eq!(config.retry_policy.max_retries, 2);
        assert_eq!(config.retry_policy.initial_delay_ms, 1000);
        assert_eq!(config.retry_policy.backoff_factor, 1.5);
        assert_eq!(config.retry_policy.max_delay_ms, 30_000);
        assert_eq!(config.request_timeout_ms, 9000);
        assert_eq!(config.streams_deactivated_warn_ms, 0);
        assert!(!config.keep_alive_enabled);
        assert_eq!(config.keep_alive_inactivity_ms, Some(40_000));
        assert_eq!(config.keep_alive_response_timeout_ms, Some(15_000));
        assert_eq!(config.slow_consumer_hi_water_mark, Some(0.9));
        assert_eq!(config.slow_consumer_lo_water_mark, Some(0.25));
        assert_eq!(config.sdk_log_level, SdkLogLevel::Warn);
        assert!(matches!(
            config.auth,
            Some(AuthConfig::App { ref app_name }) if app_name == "my-app"
        ));
    }

    #[test]
    fn servers_replace_host_and_carry_the_socks5_proxy() {
        let config = convert(
            r#"{"host": "ignored", "servers": [{"host": "a", "port": 1}, {"host": "b", "port": 2}],
                "socks5": {"host": "proxy", "port": 1080}}"#,
        )
        .unwrap();
        assert_eq!(
            direct(&config),
            [
                ("a".to_string(), 1, Some(("proxy".to_string(), 1080))),
                ("b".to_string(), 2, Some(("proxy".to_string(), 1080))),
            ]
        );
    }

    #[test]
    fn zfp_conflicts_use_presence_of_host_or_port() {
        let message = validation(r#"{"zfpRemote": "8194", "port": 8194}"#).message;
        assert!(message.starts_with("zfpRemote cannot be combined with host/port/servers"));
        let message = validation(r#"{"zfpRemote": "8194", "host": "localhost"}"#).message;
        assert!(message.starts_with("zfpRemote cannot be combined with host/port/servers"));
        let message =
            validation(r#"{"zfpRemote": "8194", "socks5": {"host": "p", "port": 1}}"#).message;
        assert_eq!(message, "zfpRemote cannot be combined with socks5");
        let message = validation(r#"{"zfpRemote": "9999"}"#).message;
        assert!(message.contains("invalid ZFP remote port"), "{message}");
    }

    #[test]
    fn zfp_with_blob_tls_resolves_zfp_transport() {
        let config = convert(
            r#"{"zfpRemote": "8196", "tls": {
                "clientCredentials": {"bytes": "AAEC"}, "clientCredentialsPassword": "pw",
                "trustMaterial": {"bytes": "AwQ="}, "handshakeTimeoutMs": 5000}}"#,
        )
        .unwrap();
        assert!(matches!(config.transport, Transport::Zfp(_)));
        let tls = config.tls.expect("tls normalized");
        assert!(
            matches!(&tls.client_credentials, TlsMaterial::Blob(bytes) if bytes[..] == [0, 1, 2])
        );
        assert!(matches!(&tls.trust_material, TlsMaterial::Blob(bytes) if bytes[..] == [3, 4]));
        assert_eq!(tls.client_credentials_password, "pw");
        assert_eq!(tls.handshake_timeout_ms, Some(5000));
    }

    #[test]
    fn tls_pairs_and_shapes_are_checked_without_echoing_material() {
        let message =
            validation(r#"{"tls": {"clientCredentials": {"path": "secret/client.p12"}}}"#).message;
        assert_eq!(
            message,
            "tls.clientCredentials set without tls.trustMaterial"
        );
        let message = validation(
            r#"{"tls": {"clientCredentials": {"path": "secret/a"}, "trustMaterial": {"bytes": "AAEC"}}}"#,
        )
        .message;
        assert_eq!(
            message,
            "tls.clientCredentials and tls.trustMaterial must both be file paths or both be blobs"
        );
        assert!(!message.contains("secret"));

        let error = rejected(r#"{"tls": {"trustMaterial": {"bytes": "not base64!"}}}"#);
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert_eq!(
            error.message,
            "tls.trustMaterial.bytes must be standard base64"
        );
        assert_eq!(error.context.element.as_deref(), Some("tls.trustMaterial"));

        let error = rejected(r#"{"tls": {"trustMaterial": {"path": "p", "bytes": "AAEC"}}}"#);
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert_eq!(
            error.message,
            "tls.trustMaterial must set exactly one of 'path' or 'bytes'"
        );
        let error = rejected(r#"{"tls": {"clientCredentials": {}}}"#);
        assert_eq!(error.kind, HostErrorKind::Argument);

        let message = validation(r#"{"tls": {"crlFetchTimeoutMs": -1}}"#).message;
        assert_eq!(
            message,
            "tls.crlFetchTimeoutMs must be a non-negative integer number of milliseconds"
        );
    }

    #[test]
    fn auth_rules_and_aliases() {
        let config =
            convert(r#"{"auth": {"method": " DIRECTORY ", "dirProperty": "mail"}}"#).unwrap();
        assert!(matches!(
            config.auth,
            Some(AuthConfig::Directory { ref property_name }) if property_name == "mail"
        ));
        assert!(
            convert(r#"{"auth": {"method": "none"}}"#)
                .unwrap()
                .auth
                .is_none()
        );
        assert!(
            convert(r#"{"auth": {"method": ""}}"#)
                .unwrap()
                .auth
                .is_none()
        );
        assert_eq!(
            validation(r#"{"auth": {"method": "app"}}"#).message,
            "auth.appName is required for auth.method='app'"
        );
        assert_eq!(
            validation(r#"{"auth": {"method": "manual", "appName": "a", "userId": "u"}}"#).message,
            "auth.ipAddress is required for auth.method='manual'"
        );
        assert!(
            validation(r#"{"auth": {"method": "kerberos"}}"#)
                .message
                .starts_with("Invalid auth.method: kerberos. Must be one of")
        );
        let missing = rejected(r#"{"auth": {"appName": "a"}}"#);
        assert_eq!(missing.kind, HostErrorKind::Argument);
        assert_eq!(missing.message, "missing field 'method'");
    }

    #[test]
    fn engine_invariants_use_camel_case_labels() {
        for (json, expected) in [
            (
                r#"{"requestPoolSize": 0}"#,
                "requestPoolSize must be greater than zero",
            ),
            (
                r#"{"runtimeWorkerThreads": 0}"#,
                "runtimeWorkerThreads must be greater than zero",
            ),
            (
                r#"{"subscriptionPoolSize": 4, "maxSubscriptionSessions": 2}"#,
                "maxSubscriptionSessions must be greater than or equal to subscriptionPoolSize",
            ),
            (
                r#"{"subscriptionStreamCapacity": 0}"#,
                "subscriptionStreamCapacity must be greater than zero",
            ),
            (
                r#"{"subscriptionFlushThreshold": 0}"#,
                "subscriptionFlushThreshold must be greater than zero",
            ),
            (
                r#"{"shardThreshold": 1}"#,
                "shardThreshold must be at least 2",
            ),
            (
                r#"{"bqlAppName": "  "}"#,
                "bqlAppName must be non-empty when configured",
            ),
            (
                r#"{"keepAliveInactivityMs": -1}"#,
                "keepAliveInactivityMs must be non-negative",
            ),
            (
                r#"{"slowConsumerHiWaterMark": 1.5}"#,
                "slowConsumerHiWaterMark must be in 0.0..=1.0",
            ),
            (
                r#"{"slowConsumerLoWaterMark": 1.0}"#,
                "slowConsumerLoWaterMark must be in 0.0..1.0",
            ),
            (r#"{"socks5": {"host": "p", "port": 1}, "servers": []}"#, ""),
        ] {
            if expected.is_empty() {
                convert(json).expect("socks5 with default host converts");
                continue;
            }
            let error = validation(json);
            assert_eq!(error.message, expected, "{json}");
            assert_eq!(error.context.element, None);
        }
    }

    #[test]
    fn host_only_checks_name_their_member() {
        for (json, message, element) in [
            (
                r#"{"validationMode": "loose"}"#,
                "unknown validation mode 'loose': expected strict, lenient, or disabled",
                "validationMode",
            ),
            (
                r#"{"overflowPolicy": "DROP"}"#,
                "unknown overflow policy 'DROP': expected 'drop_newest', 'drop_oldest', or 'block'",
                "overflowPolicy",
            ),
            (
                r#"{"keyColumn": "isin"}"#,
                "unknown key column 'isin': expected security or ticker",
                "keyColumn",
            ),
            (
                r#"{"retryPolicy": {"initialDelayMs": -1}}"#,
                "retryPolicy.initialDelayMs must be a non-negative integer number of milliseconds",
                "retryPolicy.initialDelayMs",
            ),
            (
                r#"{"retryPolicy": {"maxDelayMs": -5}}"#,
                "retryPolicy.maxDelayMs must be a non-negative integer number of milliseconds",
                "retryPolicy.maxDelayMs",
            ),
            (
                r#"{"requestTimeoutMs": -1}"#,
                "requestTimeoutMs must be a non-negative integer number of milliseconds",
                "requestTimeoutMs",
            ),
            (
                r#"{"streamsDeactivatedWarnMs": -1}"#,
                "streamsDeactivatedWarnMs must be a non-negative integer number of milliseconds",
                "streamsDeactivatedWarnMs",
            ),
            (
                r#"{"sdkLogLevel": "verbose"}"#,
                "invalid SDK log level: verbose (expected off/fatal/error/warn/info/debug/trace)",
                "sdkLogLevel",
            ),
        ] {
            let error = validation(json);
            assert_eq!(error.message, message, "{json}");
            assert_eq!(error.context.element.as_deref(), Some(element), "{json}");
        }
    }

    #[test]
    fn check_order_matches_napi() {
        // Host enum parsing runs before every numeric or engine check.
        assert_eq!(
            validation(r#"{"keyColumn": "x", "overflowPolicy": "y", "validationMode": "z"}"#)
                .context
                .element
                .as_deref(),
            Some("validationMode")
        );
        assert_eq!(
            validation(r#"{"keyColumn": "x", "overflowPolicy": "y"}"#)
                .context
                .element
                .as_deref(),
            Some("overflowPolicy")
        );
        // Duration signs run before the SDK log level and before normalize.
        assert_eq!(
            validation(
                r#"{"requestPoolSize": 0, "sdkLogLevel": "bad", "streamsDeactivatedWarnMs": -1,
                    "requestTimeoutMs": -1, "retryPolicy": {"maxDelayMs": -1, "initialDelayMs": -1}}"#
            )
            .context
            .element
            .as_deref(),
            Some("retryPolicy.initialDelayMs")
        );
        assert_eq!(
            validation(r#"{"requestPoolSize": 0, "sdkLogLevel": "bad"}"#)
                .context
                .element
                .as_deref(),
            Some("sdkLogLevel")
        );
        // normalize: flush threshold before keep-alive before watermarks before auth.
        assert_eq!(
            validation(
                r#"{"subscriptionFlushThreshold": 0, "keepAliveInactivityMs": -1,
                    "slowConsumerHiWaterMark": 2.0, "auth": {"method": "bogus"}}"#
            )
            .message,
            "subscriptionFlushThreshold must be greater than zero"
        );
        assert_eq!(
            validation(r#"{"keepAliveResponseTimeoutMs": -1, "slowConsumerHiWaterMark": 2.0}"#)
                .message,
            "keepAliveResponseTimeoutMs must be non-negative"
        );
        // auth before transport before TLS before engine invariants.
        assert!(
            validation(
                r#"{"auth": {"method": "bogus"}, "zfpRemote": "1", "requestPoolSize": 0,
                    "tls": {"trustMaterial": {"path": "p"}}}"#
            )
            .message
            .starts_with("Invalid auth.method")
        );
        assert!(
            validation(
                r#"{"zfpRemote": "1", "requestPoolSize": 0, "tls": {"trustMaterial": {"path": "p"}}}"#
            )
            .message
            .contains("invalid ZFP remote port")
        );
        assert_eq!(
            validation(r#"{"requestPoolSize": 0, "tls": {"trustMaterial": {"path": "p"}}}"#)
                .message,
            "tls.trustMaterial set without tls.clientCredentials"
        );
        // TLS material shape errors precede every value check.
        assert_eq!(
            rejected(r#"{"validationMode": "bad", "tls": {"trustMaterial": {}}}"#).kind,
            HostErrorKind::Argument
        );
    }

    #[test]
    fn malformed_objects_are_argument_errors() {
        for (json, message) in [
            (r#"{"hots": "x"}"#, "unknown field 'hots'"),
            (
                r#"{"retryPolicy": {"retries": 1}}"#,
                "unknown field 'retries'",
            ),
            (
                r#"{"port": 70000}"#,
                "invalid value: integer '70000', expected u16",
            ),
            (
                r#"{"requestPoolSize": -1}"#,
                "invalid value: integer '-1', expected u32",
            ),
            (r#"{"servers": [{"host": "a"}]}"#, "missing field 'port'"),
        ] {
            let error = rejected(json);
            assert_eq!(error.kind, HostErrorKind::Argument, "{json}");
            assert_eq!(error.message, message, "{json}");
        }
    }

    #[test]
    fn debug_output_redacts_tls_secrets() {
        let spec = EngineConfigSpec::from_json(
            r#"{"tls": {"clientCredentials": {"path": "secret/path.p12"},
                "clientCredentialsPassword": "hunter2", "trustMaterial": {"bytes": "c2VjcmV0"}}}"#,
        )
        .unwrap();
        let debug = format!("{spec:?}");
        assert!(!debug.contains("secret"), "{debug}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("c2VjcmV0"), "{debug}");
    }
}
