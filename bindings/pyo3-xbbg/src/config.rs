//! Python engine configuration wrapper validated via `xbbg_host::config`.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_async::config::{AuthInput, TlsInput};
use xbbg_async::engine::{EngineConfig, TlsMaterial, Transport};
use xbbg_host::config::{EngineConfigSpec, FlatConfigParts, RetryPolicySpec, ServerAddressSpec};

/// Python's `str | bytes` TLS input, with immutable owned storage after extraction.
#[derive(Clone)]
pub struct PyTlsMaterial(pub(crate) TlsMaterial);

impl FromPyObject<'_, '_> for PyTlsMaterial {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(path) = value.cast::<PyString>() {
            return Ok(Self(TlsMaterial::Path(path.to_str()?.to_owned())));
        }
        if let Ok(blob) = value.cast::<PyBytes>() {
            return Ok(Self(TlsMaterial::Blob(Arc::from(blob.as_bytes()))));
        }
        Err(pyo3::exceptions::PyTypeError::new_err(
            "TLS material must be str (file path) or bytes (DER-encoded material)",
        ))
    }
}

impl<'py> IntoPyObject<'py> for &PyTlsMaterial {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match &self.0 {
            TlsMaterial::Path(path) => PyString::new(py, path).into_any(),
            TlsMaterial::Blob(blob) => PyBytes::new(py, blob).into_any(),
        })
    }
}

impl<'py> IntoPyObject<'py> for PyTlsMaterial {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (&self).into_pyobject(py)
    }
}

#[cfg(feature = "stub-gen")]
impl pyo3_stub_gen::PyStubType for PyTlsMaterial {
    fn type_output() -> pyo3_stub_gen::TypeInfo {
        pyo3_stub_gen::TypeInfo::builtin("str") | pyo3_stub_gen::TypeInfo::builtin("bytes")
    }
}

/// Python configuration for the xbbg Engine.
///
/// All settings have sensible defaults - you only need to specify what you want to change.
///
/// The defaults are derived from `EngineConfig::default()` in xbbg-async, so they
/// stay in sync automatically.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct PyEngineConfig {
    /// Bloomberg server host (default: "localhost")
    #[pyo3(get, set)]
    pub host: String,
    /// Bloomberg server port (default: 8194)
    #[pyo3(get, set)]
    pub port: u16,
    /// Multiple servers for failover: list of (host, port) tuples. Overrides host/port when set.
    #[pyo3(get, set)]
    pub servers: Vec<(String, u16)>,
    /// ZFP remote: "8194" or "8196". Default: None (direct transport).
    #[pyo3(get, set)]
    pub zfp_remote: Option<String>,
    /// Number of pre-warmed request workers (default: 2)
    #[pyo3(get, set)]
    pub request_pool_size: usize,
    /// Number of pre-warmed subscription sessions (default: 1)
    #[pyo3(get, set)]
    pub subscription_pool_size: usize,
    /// Tokio worker threads used by this Engine (default: 2)
    #[pyo3(get, set)]
    pub runtime_worker_threads: usize,
    /// Maximum concurrently allocated subscription sessions (default: 32)
    #[pyo3(get, set)]
    pub max_subscription_sessions: usize,
    /// Enable request sharding for eligible multi-security reference/history requests.
    #[pyo3(get, set)]
    pub shard_requests: bool,
    /// Minimum securities before request sharding applies (default: 20).
    #[pyo3(get, set)]
    pub shard_threshold: usize,
    /// Maximum securities per sharded request (default: 16).
    #[pyo3(get, set)]
    pub shard_chunk_size: usize,
    /// Maximum concurrent shard requests per user request (default: 4).
    #[pyo3(get, set)]
    pub shard_max_concurrent: usize,
    /// Validation mode: "disabled" (default), "lenient", or "strict".
    /// Also accepts "off" and "none" as aliases for "disabled".
    #[pyo3(get, set)]
    pub validation_mode: String,
    /// Name of the result column holding each row's security: "ticker"
    /// (default, deprecated) or "security", which becomes the default in the
    /// next major release.
    #[pyo3(get, set)]
    pub key_column: String,
    /// Maximum number of already-queued subscription updates combined into one
    /// batch (default: 16). Reads never wait for more updates: a read returns as
    /// soon as one update is available, so quiet streams still yield one-row batches.
    #[pyo3(get, set)]
    pub subscription_flush_threshold: usize,
    /// Bloomberg SDK event queue size (default: 10000)
    #[pyo3(get, set)]
    pub max_event_queue_size: usize,
    /// Internal command channel capacity (default: 256)
    #[pyo3(get, set)]
    pub command_queue_size: usize,
    /// Subscription stream backpressure capacity (default: 256)
    #[pyo3(get, set)]
    pub subscription_stream_capacity: usize,
    /// Overflow policy for slow consumers: "drop_newest" (default) or "block".
    /// Also accepts "dropnewest" as an alias for "drop_newest".
    #[pyo3(get, set)]
    pub overflow_policy: String,
    /// Services to pre-warm on startup (default: ["//blp/refdata", "//blp/apiflds"])
    #[pyo3(get, set)]
    pub warmup_services: Vec<String>,
    /// Custom path for field cache JSON file (default: `~/.xbbg/field_cache.v<N>.json`,
    /// where `<N>` is the cache format version)
    /// Set to None to use the default path.
    #[pyo3(get, set)]
    pub field_cache_path: Option<String>,
    /// Optional BQL clientContext.appName request default.
    /// Independent of authentication; None leaves the service default unchanged.
    #[pyo3(get, set)]
    pub bql_app_name: Option<String>,
    /// Optional auth method: "none", "user", "app", "userapp", "dir", "manual", or "token".
    /// Default: None (no auth). Also accepts "directory" as an alias for "dir" and the empty
    /// string as "none".
    #[pyo3(get, set)]
    pub auth_method: Option<String>,
    /// Bloomberg application name for app/userapp/manual auth.
    #[pyo3(get, set)]
    pub app_name: Option<String>,
    /// Active Directory property for dir auth.
    #[pyo3(get, set)]
    pub dir_property: Option<String>,
    /// Manual Bloomberg user id for manual auth.
    #[pyo3(get, set)]
    pub user_id: Option<String>,
    /// Manual Bloomberg ip address for manual auth.
    #[pyo3(get, set)]
    pub ip_address: Option<String>,
    #[pyo3(get, set)]
    pub token: Option<String>,
    /// Client credentials: file path (`str`) or DER PKCS#12 material (`bytes`).
    /// Must use the same representation as `tls_trust_material`.
    #[pyo3(get, set)]
    pub tls_client_credentials: Option<PyTlsMaterial>,
    #[pyo3(get, set)]
    pub tls_client_credentials_password: Option<String>,
    /// Trust material: file path (`str`) or DER PKCS#7 material (`bytes`).
    /// Must be supplied together with `tls_client_credentials`.
    #[pyo3(get, set)]
    pub tls_trust_material: Option<PyTlsMaterial>,
    #[pyo3(get, set)]
    pub tls_handshake_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub tls_crl_fetch_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub num_start_attempts: usize,
    /// Whether Bloomberg should auto-restart the session on disconnect (default: True).
    #[pyo3(get, set)]
    pub auto_restart_on_disconnection: bool,
    #[pyo3(get, set)]
    pub retry_max_retries: u32,
    #[pyo3(get, set)]
    pub retry_initial_delay_ms: u64,
    #[pyo3(get, set)]
    pub retry_backoff_factor: f64,
    #[pyo3(get, set)]
    pub retry_max_delay_ms: u64,
    /// Hard per-request timeout in ms; 0 disables. Default: 0 (no timeout).
    #[pyo3(get, set)]
    pub request_timeout_ms: u64,
    /// Warn threshold for a subscription's streams staying deactivated, in ms;
    /// 0 disables. Default: 30_000.
    #[pyo3(get, set)]
    pub streams_deactivated_warn_ms: u64,
    /// How long a request session may stay without any server connection before
    /// its in-flight requests are failed, in ms; 0 fails them as soon as the
    /// last connection drops. Default: 10_000.
    #[pyo3(get, set)]
    pub reconnect_grace_ms: u64,
    /// Enable BLPAPI keep-alive pings. SDK default: True.
    #[pyo3(get, set)]
    pub keep_alive_enabled: bool,
    /// Milliseconds of inactivity before keep-alive ping is sent. None = SDK default (20_000).
    #[pyo3(get, set)]
    pub keep_alive_inactivity_ms: Option<i32>,
    /// Milliseconds to wait for keep-alive response before declaring the connection dead.
    /// None = SDK default (10_000).
    #[pyo3(get, set)]
    pub keep_alive_response_timeout_ms: Option<i32>,
    /// Slow-consumer hi water mark as fraction of max_event_queue_size. None = SDK default (0.75).
    #[pyo3(get, set)]
    pub slow_consumer_hi_water_mark: Option<f32>,
    /// Slow-consumer lo water mark as fraction of max_event_queue_size. None = SDK default (0.5).
    #[pyo3(get, set)]
    pub slow_consumer_lo_water_mark: Option<f32>,
    /// Bloomberg SDK log level: "off" (default), "fatal", "error", "warn", "info", "debug",
    /// or "trace". Also accepts "warning" as an alias for "warn".
    #[pyo3(get, set)]
    pub sdk_log_level: String,
    /// SOCKS5 proxy hostname for Bloomberg connections.
    #[pyo3(get, set)]
    pub socks5_host: Option<String>,
    /// SOCKS5 proxy port (required when socks5_host is set).
    #[pyo3(get, set)]
    pub socks5_port: Option<u16>,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PyEngineConfig {
    /// Create a new configuration with defaults.
    ///
    /// All defaults are derived from the Rust EngineConfig to stay in sync.
    #[new]
    #[pyo3(signature = (**kwargs))]
    pub(crate) fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let defaults = EngineConfig::default();
        let (default_host, default_port) = default_direct_host_port(&defaults);
        let mut config = Self {
            host: default_host,
            port: default_port,
            servers: Vec::new(),
            zfp_remote: None,
            request_pool_size: defaults.request_pool_size,
            subscription_pool_size: defaults.subscription_pool_size,
            runtime_worker_threads: defaults.runtime_worker_threads,
            max_subscription_sessions: defaults.max_subscription_sessions,
            shard_requests: defaults.shard_requests,
            shard_threshold: defaults.shard_threshold,
            shard_chunk_size: defaults.shard_chunk_size,
            shard_max_concurrent: defaults.shard_max_concurrent,
            validation_mode: defaults.validation_mode.to_string(),
            key_column: defaults.key_column.to_string(),
            subscription_flush_threshold: defaults.subscription_flush_threshold,
            max_event_queue_size: defaults.max_event_queue_size,
            command_queue_size: defaults.command_queue_size,
            subscription_stream_capacity: defaults.subscription_stream_capacity,
            overflow_policy: defaults.overflow_policy.to_string(),
            bql_app_name: defaults.bql_app_name,
            warmup_services: defaults.warmup_services,
            field_cache_path: None,
            auth_method: None,
            app_name: None,
            dir_property: None,
            user_id: None,
            ip_address: None,
            token: None,
            tls_client_credentials: None,
            tls_client_credentials_password: None,
            tls_trust_material: None,
            tls_handshake_timeout_ms: None,
            tls_crl_fetch_timeout_ms: None,
            num_start_attempts: defaults.num_start_attempts,
            auto_restart_on_disconnection: defaults.auto_restart_on_disconnection,
            retry_max_retries: 0,
            retry_initial_delay_ms: 1000,
            retry_backoff_factor: 2.0,
            retry_max_delay_ms: 30_000,
            request_timeout_ms: defaults.request_timeout_ms,
            streams_deactivated_warn_ms: defaults.streams_deactivated_warn_ms,
            reconnect_grace_ms: defaults.reconnect_grace_ms,
            keep_alive_enabled: defaults.keep_alive_enabled,
            keep_alive_inactivity_ms: defaults.keep_alive_inactivity_ms,
            keep_alive_response_timeout_ms: defaults.keep_alive_response_timeout_ms,
            slow_consumer_hi_water_mark: defaults.slow_consumer_hi_water_mark,
            slow_consumer_lo_water_mark: defaults.slow_consumer_lo_water_mark,
            sdk_log_level: "off".to_string(),
            socks5_host: None,
            socks5_port: None,
        };

        if let Some(kw) = kwargs {
            if let Some(v) = kw.get_item("host")? {
                config.host = v.extract()?;
            }
            if let Some(v) = kw.get_item("port")? {
                config.port = v.extract()?;
            }
            if let Some(v) = kw.get_item("servers")? {
                config.servers = v.extract()?;
            }
            if let Some(v) = kw.get_item("zfp_remote")? {
                config.zfp_remote = v.extract()?;
            }
            if let Some(v) = kw.get_item("request_pool_size")? {
                config.request_pool_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_pool_size")? {
                config.subscription_pool_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("runtime_worker_threads")? {
                config.runtime_worker_threads = v.extract()?;
            }
            if let Some(v) = kw.get_item("max_subscription_sessions")? {
                config.max_subscription_sessions = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_requests")? {
                config.shard_requests = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_threshold")? {
                config.shard_threshold = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_chunk_size")? {
                config.shard_chunk_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_max_concurrent")? {
                config.shard_max_concurrent = v.extract()?;
            }
            if let Some(v) = kw.get_item("validation_mode")? {
                config.validation_mode = v.extract()?;
            }
            if let Some(v) = kw.get_item("key_column")? {
                config.key_column = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_flush_threshold")? {
                config.subscription_flush_threshold = v.extract()?;
            }
            if let Some(v) = kw.get_item("max_event_queue_size")? {
                config.max_event_queue_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("command_queue_size")? {
                config.command_queue_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_stream_capacity")? {
                config.subscription_stream_capacity = v.extract()?;
            }
            if let Some(v) = kw.get_item("overflow_policy")? {
                config.overflow_policy = v.extract()?;
            }
            if let Some(v) = kw.get_item("warmup_services")? {
                config.warmup_services = v.extract()?;
            }
            if let Some(v) = kw.get_item("field_cache_path")? {
                config.field_cache_path = v.extract()?;
            }
            if let Some(v) = kw.get_item("bql_app_name")? {
                config.bql_app_name = v.extract()?;
            }
            if let Some(v) = kw.get_item("auth_method")? {
                config.auth_method = v.extract()?;
            }
            if let Some(v) = kw.get_item("app_name")? {
                config.app_name = v.extract()?;
            }
            if let Some(v) = kw.get_item("dir_property")? {
                config.dir_property = v.extract()?;
            }
            if let Some(v) = kw.get_item("user_id")? {
                config.user_id = v.extract()?;
            }
            if let Some(v) = kw.get_item("ip_address")? {
                config.ip_address = v.extract()?;
            }
            if let Some(v) = kw.get_item("token")? {
                config.token = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_client_credentials")? {
                config.tls_client_credentials = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_client_credentials_password")? {
                config.tls_client_credentials_password = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_trust_material")? {
                config.tls_trust_material = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_handshake_timeout_ms")? {
                config.tls_handshake_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_crl_fetch_timeout_ms")? {
                config.tls_crl_fetch_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("num_start_attempts")? {
                config.num_start_attempts = v.extract()?;
            }
            if let Some(v) = kw.get_item("auto_restart_on_disconnection")? {
                config.auto_restart_on_disconnection = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_max_retries")? {
                config.retry_max_retries = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_initial_delay_ms")? {
                config.retry_initial_delay_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_backoff_factor")? {
                config.retry_backoff_factor = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_max_delay_ms")? {
                config.retry_max_delay_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("request_timeout_ms")? {
                config.request_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("streams_deactivated_warn_ms")? {
                config.streams_deactivated_warn_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("reconnect_grace_ms")? {
                config.reconnect_grace_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_enabled")? {
                config.keep_alive_enabled = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_inactivity_ms")? {
                config.keep_alive_inactivity_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_response_timeout_ms")? {
                config.keep_alive_response_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("slow_consumer_hi_water_mark")? {
                config.slow_consumer_hi_water_mark = v.extract()?;
            }
            if let Some(v) = kw.get_item("slow_consumer_lo_water_mark")? {
                config.slow_consumer_lo_water_mark = v.extract()?;
            }
            if let Some(v) = kw.get_item("sdk_log_level")? {
                config.sdk_log_level = v.extract()?;
            }
            if let Some(v) = kw.get_item("socks5_host")? {
                config.socks5_host = v.extract()?;
            }
            if let Some(v) = kw.get_item("socks5_port")? {
                config.socks5_port = v.extract()?;
            }
        }

        Ok(config)
    }

    pub(crate) fn __repr__(&self) -> String {
        let fcp_display = self.field_cache_path.as_deref().unwrap_or("default");
        let auth_method = self.auth_method.as_deref().unwrap_or("none");
        format!(
            "EngineConfig(host='{}', port={}, request_pool_size={}, subscription_pool_size={}, \
             runtime_worker_threads={}, max_subscription_sessions={}, shard_requests={}, \
             shard_threshold={}, shard_chunk_size={}, shard_max_concurrent={}, validation_mode='{}', \
             key_column='{}', overflow_policy='{}', auth_method='{}', field_cache_path='{}', \
             warmup_services={:?})",
            self.host,
            self.port,
            self.request_pool_size,
            self.subscription_pool_size,
            self.runtime_worker_threads,
            self.max_subscription_sessions,
            self.shard_requests,
            self.shard_threshold,
            self.shard_chunk_size,
            self.shard_max_concurrent,
            self.validation_mode,
            self.key_column,
            self.overflow_policy,
            auth_method,
            fcp_display,
            self.warmup_services
        )
    }
}

/// Expose `(host, port)` of the first server in the default `Transport::Direct`,
/// so `PyEngineConfig`'s Python-visible defaults stay in lockstep with the
/// Rust-side default.
fn default_direct_host_port(defaults: &EngineConfig) -> (String, u16) {
    match &defaults.transport {
        Transport::Direct(servers) => servers
            .first()
            .map(|s| (s.host.clone(), s.port))
            .unwrap_or_else(|| ("localhost".to_string(), 8194)),
        Transport::Zfp(_) => ("localhost".to_string(), 8194),
    }
}

/// The engine's numeric members are 32-bit in the shared config spec.
fn config_u32(value: usize, field: &str) -> PyResult<u32> {
    u32::try_from(value)
        .map_err(|_| PyValueError::new_err(format!("{field} must not exceed {}", u32::MAX)))
}

/// The engine's millisecond durations are signed 64-bit in the shared config spec.
fn config_millis(value: u64, field: &str) -> PyResult<i64> {
    i64::try_from(value)
        .map_err(|_| PyValueError::new_err(format!("{field} must not exceed {}", i64::MAX)))
}

impl TryFrom<&PyEngineConfig> for EngineConfig {
    type Error = PyErr;

    /// Parse and validate through `xbbg_host::config`; every failure is a
    /// `ValueError` carrying the host's message, with engine fields named by
    /// their Python attribute names.
    fn try_from(py_config: &PyEngineConfig) -> Result<Self, Self::Error> {
        // Python infers an explicit host/port from non-default values, so the
        // spec carries only members that differ from the defaults.
        let spec = EngineConfigSpec {
            host: (py_config.host != "localhost").then(|| py_config.host.clone()),
            port: (py_config.port != 8194).then_some(py_config.port),
            servers: Some(
                py_config
                    .servers
                    .iter()
                    .map(|(host, port)| ServerAddressSpec {
                        host: host.clone(),
                        port: *port,
                    })
                    .collect(),
            ),
            zfp_remote: py_config.zfp_remote.clone(),
            request_pool_size: Some(config_u32(
                py_config.request_pool_size,
                "request_pool_size",
            )?),
            subscription_pool_size: Some(config_u32(
                py_config.subscription_pool_size,
                "subscription_pool_size",
            )?),
            runtime_worker_threads: Some(config_u32(
                py_config.runtime_worker_threads,
                "runtime_worker_threads",
            )?),
            max_subscription_sessions: Some(config_u32(
                py_config.max_subscription_sessions,
                "max_subscription_sessions",
            )?),
            shard_requests: Some(py_config.shard_requests),
            shard_threshold: Some(config_u32(py_config.shard_threshold, "shard_threshold")?),
            shard_chunk_size: Some(config_u32(py_config.shard_chunk_size, "shard_chunk_size")?),
            shard_max_concurrent: Some(config_u32(
                py_config.shard_max_concurrent,
                "shard_max_concurrent",
            )?),
            validation_mode: Some(py_config.validation_mode.clone()),
            key_column: Some(py_config.key_column.clone()),
            subscription_flush_threshold: Some(config_u32(
                py_config.subscription_flush_threshold,
                "subscription_flush_threshold",
            )?),
            max_event_queue_size: Some(config_u32(
                py_config.max_event_queue_size,
                "max_event_queue_size",
            )?),
            command_queue_size: Some(config_u32(
                py_config.command_queue_size,
                "command_queue_size",
            )?),
            subscription_stream_capacity: Some(config_u32(
                py_config.subscription_stream_capacity,
                "subscription_stream_capacity",
            )?),
            overflow_policy: Some(py_config.overflow_policy.clone()),
            warmup_services: Some(py_config.warmup_services.clone()),
            field_cache_path: py_config.field_cache_path.clone(),
            bql_app_name: py_config.bql_app_name.clone(),
            num_start_attempts: Some(config_u32(
                py_config.num_start_attempts,
                "num_start_attempts",
            )?),
            auto_restart_on_disconnection: Some(py_config.auto_restart_on_disconnection),
            retry_policy: Some(RetryPolicySpec {
                max_retries: Some(py_config.retry_max_retries),
                initial_delay_ms: Some(config_millis(
                    py_config.retry_initial_delay_ms,
                    "retry_initial_delay_ms",
                )?),
                backoff_factor: Some(py_config.retry_backoff_factor),
                max_delay_ms: Some(config_millis(
                    py_config.retry_max_delay_ms,
                    "retry_max_delay_ms",
                )?),
            }),
            request_timeout_ms: Some(config_millis(
                py_config.request_timeout_ms,
                "request_timeout_ms",
            )?),
            streams_deactivated_warn_ms: Some(config_millis(
                py_config.streams_deactivated_warn_ms,
                "streams_deactivated_warn_ms",
            )?),
            reconnect_grace_ms: Some(config_millis(
                py_config.reconnect_grace_ms,
                "reconnect_grace_ms",
            )?),
            keep_alive_enabled: Some(py_config.keep_alive_enabled),
            keep_alive_inactivity_ms: py_config.keep_alive_inactivity_ms,
            keep_alive_response_timeout_ms: py_config.keep_alive_response_timeout_ms,
            slow_consumer_hi_water_mark: py_config.slow_consumer_hi_water_mark.map(f64::from),
            slow_consumer_lo_water_mark: py_config.slow_consumer_lo_water_mark.map(f64::from),
            sdk_log_level: Some(py_config.sdk_log_level.clone()),
            auth: None,
            tls: None,
            socks5: None,
        };
        let parts = FlatConfigParts {
            auth: AuthInput {
                method: py_config.auth_method.as_deref(),
                app_name: py_config.app_name.as_deref(),
                dir_property: py_config.dir_property.as_deref(),
                user_id: py_config.user_id.as_deref(),
                ip_address: py_config.ip_address.as_deref(),
                token: py_config.token.as_deref(),
            },
            tls: TlsInput {
                client_credentials: py_config
                    .tls_client_credentials
                    .as_ref()
                    .map(|material| material.0.clone()),
                client_credentials_password: py_config.tls_client_credentials_password.as_deref(),
                trust_material: py_config
                    .tls_trust_material
                    .as_ref()
                    .map(|material| material.0.clone()),
                handshake_timeout_ms: py_config.tls_handshake_timeout_ms,
                crl_fetch_timeout_ms: py_config.tls_crl_fetch_timeout_ms,
            },
            socks5_host: py_config.socks5_host.as_deref(),
            socks5_port: py_config.socks5_port,
        };
        spec.into_engine_config_flat(parts)
            .map_err(|error| PyValueError::new_err(error.message))
    }
}
