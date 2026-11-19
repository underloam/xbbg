//! Python wrapper for `xbbg_async::engine::Engine` and entitlements report.
//!
//! Schema JSON caching delegates to `xbbg_host::schema::SchemaJsonCache`, shared
//! with Node and Java.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_async::engine::{Engine, EngineConfig, ServerAddr, Transport};
use xbbg_ext::MarketTiming;
use xbbg_host::schema::{self as host_schema, SchemaJsonCache};
use xbbg_host::subscribe::PreparedSubscribe;
use xbbg_log::{debug, info, warn};

use crate::completion::{
    ExchangeInfoDict, MarketInfoDict, record_batch_future, shutdown_safe_future,
};
use crate::config::PyEngineConfig;
use crate::errors::{blp_async_error_to_pyerr, host_error_to_pyerr};
use crate::request::dict_to_request_params;
use crate::subscribe::{SubscribeParams, prepare as prepare_subscribe_params};
use crate::subscriptions::PySubscription;
use crate::teardown::EngineOwner;

/// Result of a Bloomberg entitlement check.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct EntitlementReport {
    #[pyo3(get)]
    pub(crate) entitled: bool,
    #[pyo3(get)]
    pub(crate) failed_eids: Vec<i32>,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl EntitlementReport {
    fn __repr__(&self) -> String {
        format!(
            "EntitlementReport(entitled={}, failed_eids={:?})",
            self.entitled, self.failed_eids
        )
    }
}

/// Python wrapper for the xbbg Engine.
///
/// Releasing the last reference stops the engine's Bloomberg sessions on a
/// native background thread.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(frozen)]
pub(crate) struct PyEngine {
    /// Shared with in-flight futures; the last release defers native teardown.
    pub(crate) engine: Arc<EngineOwner>,
    subscription_batch_items: usize,
    schema_cache: Arc<SchemaJsonCache>,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PyEngine {
    /// Create a new Engine with optional host/port configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    /// For more configuration options, use `Engine.with_config()`.
    #[new]
    #[pyo3(signature = (host="localhost", port=8194))]
    fn new(py: Python<'_>, host: &str, port: u16) -> PyResult<Self> {
        info!(
            host = host,
            port = port,
            "PyEngine: connecting to Bloomberg"
        );

        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };

        Self::start_engine(py, config)
    }

    /// Create a new Engine with full configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    ///
    /// Example:
    /// ```python
    /// config = EngineConfig(
    ///     host="localhost",
    ///     port=8194,
    ///     request_pool_size=4,
    ///     subscription_pool_size=8,
    ///     overflow_policy="drop_newest",
    /// )
    /// engine = Engine.with_config(config)
    /// ```
    #[staticmethod]
    fn with_config(py: Python<'_>, config: PyRef<'_, PyEngineConfig>) -> PyResult<Self> {
        info!(
            host = %config.host,
            port = config.port,
            request_pool_size = config.request_pool_size,
            subscription_pool_size = config.subscription_pool_size,
            "PyEngine: connecting with custom config"
        );

        let rust_config = EngineConfig::try_from(&*config)?;
        // Other threads may read or update the config while this one connects.
        drop(config);

        Self::start_engine(py, rust_config)
    }

    // =========================================================================
    // Generic Request API
    // =========================================================================

    /// Generic async Bloomberg request.
    ///
    /// Accepts a dictionary of parameters and returns an xbbg ArrowRecordBatch.
    ///
    /// Required keys:
    /// - service: Bloomberg service URI (e.g., "//blp/refdata")
    /// - operation: Request operation name (e.g., "ReferenceDataRequest")
    ///   Use "" / Operation.RAW_REQUEST together with request_operation for raw mode.
    ///
    /// Optional keys:
    /// - extractor: Extractor type hint (e.g., "refdata", "histdata", "intraday_bar")
    ///   If omitted, Rust resolves a default from `operation`.
    /// - request_operation: Actual Bloomberg operation name when operation=""
    ///
    /// Optional keys (depend on request type):
    /// - securities: List of security identifiers
    /// - security: Single security identifier
    /// - fields: List of field names
    /// - overrides: List of (name, value) tuples
    /// - start_date, end_date: For historical requests
    /// - start_datetime, end_datetime: For intraday requests
    /// - event_type: For intraday bars (TRADE, BID, ASK)
    /// - interval: Bar interval in minutes
    /// - options: Additional Bloomberg options
    #[pyo3(signature = (params))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        params: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        // Extract and convert params to Rust struct
        let rust_params = dict_to_request_params(params)?;

        debug!(
            service = %rust_params.service,
            operation = %rust_params.operation,
            extractor = ?rust_params.extractor,
            securities = ?rust_params.securities,
            fields = ?rust_params.fields,
            "PyEngine: sending request"
        );

        record_batch_future(py, async move {
            let batch = engine.request(rust_params).await.map_err(|e| {
                warn!(error = %e, "PyEngine: request failed");
                blp_async_error_to_pyerr(e)
            })?;

            debug!(num_rows = batch.num_rows(), "PyEngine: request completed");
            Ok(batch)
        })
    }

    /// Return the seat type for the lazily authorized identity.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    fn seat_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, async move {
            let seat_type = engine.seat_type().await.map_err(blp_async_error_to_pyerr)?;
            Ok(seat_type.as_str())
        })
    }

    /// Check EID entitlements for the lazily authorized identity.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    #[pyo3(signature = (service, eids))]
    fn check_entitlements<'py>(
        &self,
        py: Python<'py>,
        service: String,
        eids: Vec<i32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, async move {
            let report = engine
                .check_entitlements(&service, &eids)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Ok(EntitlementReport {
                entitled: report.entitled,
                failed_eids: report.failed_eids,
            })
        })
    }

    /// Return whether the lazily authorized identity is authorized for a service.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    #[pyo3(signature = (service))]
    fn identity_is_authorized<'py>(
        &self,
        py: Python<'py>,
        service: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, async move {
            engine
                .identity_is_authorized(&service)
                .await
                .map_err(blp_async_error_to_pyerr)
        })
    }

    /// Resolve exchange metadata using override -> cache -> Bloomberg waterfall.
    fn resolve_exchange<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, async move {
            Ok(ExchangeInfoDict(engine.resolve_exchange(&ticker).await))
        })
    }

    /// Fetch market-level metadata (exchange, timezone, futures cycle info).
    fn fetch_market_info<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, async move {
            let info = engine
                .fetch_market_info(&ticker)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Ok(MarketInfoDict(info))
        })
    }

    /// Resolve market timing (BOD/EOD/FINISHED) for a ticker/date.
    #[pyo3(signature = (ticker, date, timing="EOD", tz=None))]
    fn market_timing<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
        date: String,
        timing: &str,
        tz: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let timing = MarketTiming::parse(timing)
            .ok_or_else(|| PyValueError::new_err("timing must be one of: BOD, EOD, FINISHED"))?;
        let date = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .map_err(|_| PyValueError::new_err("date must be YYYY-MM-DD"))?;

        shutdown_safe_future(py, async move {
            engine
                .resolve_market_timing(&ticker, date, timing, tz.as_deref())
                .await
                .map_err(blp_async_error_to_pyerr)
        })
    }

    // =========================================================================
    // Field Type Resolution API
    // =========================================================================

    /// Resolve field types for a list of fields.
    #[pyo3(signature = (fields, overrides=None, default_type="string"))]
    fn resolve_field_types<'py>(
        &self,
        py: Python<'py>,
        fields: Vec<String>,
        overrides: Option<HashMap<String, String>>,
        default_type: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let default = default_type.to_string();

        shutdown_safe_future(py, async move {
            engine
                .resolve_field_types(&fields, overrides.as_ref(), &default)
                .await
                .map_err(blp_async_error_to_pyerr)
        })
    }

    /// Get field info from cache.
    fn get_field_info(&self, field: &str) -> Option<HashMap<String, String>> {
        self.engine.get_field_info(field).map(|info| {
            let mut map = HashMap::new();
            map.insert("field_id".to_string(), info.field_id);
            map.insert("arrow_type".to_string(), info.arrow_type);
            map.insert("description".to_string(), info.description);
            map.insert("category".to_string(), info.category);
            map
        })
    }

    /// Clear the field type cache, including its persisted file.
    fn clear_field_cache(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.engine.clear_field_cache())
            .map_err(PyRuntimeError::new_err)
    }

    /// Get field cache statistics including the active cache path.
    fn field_cache_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (entry_count, cache_path) = self.engine.field_cache_stats();
        let dict = PyDict::new(py);
        dict.set_item("entry_count", entry_count)?;
        dict.set_item("cache_path", cache_path.to_string_lossy().into_owned())?;
        Ok(dict.into())
    }

    // =========================================================================
    // Schema Cache API (via xbbg_host::schema)
    // =========================================================================

    /// Get service schema (from cache or introspect).
    ///
    /// Returns a JSON string with schema information including operations.
    /// First checks memory/disk cache; if not cached, introspects the service.
    #[pyo3(signature = (service))]
    fn get_schema<'py>(&self, py: Python<'py>, service: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let cache = self.schema_cache.clone();

        shutdown_safe_future(py, async move {
            cache
                .schema(&engine, &service)
                .await
                .map(|json| json.to_string())
                .map_err(host_error_to_pyerr)
        })
    }

    /// Get a specific operation schema.
    ///
    /// Returns operation details including request/response element definitions.
    #[pyo3(signature = (service, operation))]
    fn get_operation<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let cache = self.schema_cache.clone();

        shutdown_safe_future(py, async move {
            cache
                .operation(&engine, &service, &operation)
                .await
                .map(|json| json.to_string())
                .map_err(host_error_to_pyerr)
        })
    }

    /// List all operations for a service.
    #[pyo3(signature = (service))]
    fn list_operations<'py>(
        &self,
        py: Python<'py>,
        service: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, async move {
            host_schema::list_operations(&engine, &service)
                .await
                .map_err(host_error_to_pyerr)
        })
    }

    /// Invalidate a cached schema.
    fn invalidate_schema(&self, py: Python<'_>, service: &str) -> PyResult<()> {
        py.detach(|| self.schema_cache.invalidate(&self.engine, service))
            .map_err(|error| PyRuntimeError::new_err(error.message))
    }

    /// Clear all cached schemas.
    fn clear_schema_cache(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.schema_cache.clear(&self.engine))
            .map_err(|error| PyRuntimeError::new_err(error.message))
    }

    /// List all cached service URIs.
    fn list_cached_schemas(&self, py: Python<'_>) -> Vec<String> {
        py.detach(|| host_schema::list_cached_schemas(&self.engine))
    }

    // =========================================================================
    // Schema Validation API
    // =========================================================================

    /// Get valid enum values for an element.
    ///
    /// Returns a list of valid enum values, or None if the element is not an enum.
    #[pyo3(signature = (service, operation, element))]
    fn get_enum_values<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
        element: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, async move {
            host_schema::enum_values(&engine, &service, &operation, &element)
                .await
                .map_err(host_error_to_pyerr)
        })
    }

    /// List all valid element names for an operation.
    #[pyo3(signature = (service, operation))]
    fn list_valid_elements<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, async move {
            host_schema::valid_elements(&engine, &service, &operation)
                .await
                .map_err(host_error_to_pyerr)
        })
    }

    // =========================================================================
    // Subscription API
    // =========================================================================

    /// Subscribe to real-time market data.
    ///
    /// Returns a PySubscription that supports async iteration and dynamic add/remove.
    /// GIL is released during async operations; iteration and add/remove use separate
    /// locks to avoid contention.
    /// flush_threshold caps how many already-queued updates one read combines
    /// into a batch (default: the engine's subscription_flush_threshold, 16).
    /// Reads never wait for more updates, so quiet streams still yield one-row
    /// batches.
    /// Aliases map Bloomberg topics to consumer labels. Delayed streams warn by
    /// default; on_delayed also accepts raise and ignore. Isolated subscriptions
    /// do not share upstream feeds with other consumers.
    /// Set rows=False for an image-only subscription read through latest().
    /// on_field_error accepts warn, raise, or ignore. zero_as_null masks numeric
    /// zero values only in latest(), leaving stream rows unchanged.
    /// session_wait_ms optionally bounds pool-session acquisition; expiry raises
    /// BlpValidationError starting with "Configuration error: session_wait:".
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK'])
    /// async for batch in sub:
    ///     print(batch)
    /// await sub.unsubscribe()
    /// ```
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tickers, fields, flush_threshold=None, overflow_policy=None, stream_capacity=None, all_fields=false, aliases=None, on_delayed="warn", isolated=false, rows=true, on_field_error="warn", zero_as_null=None, session_wait_ms=None, delivery="deltas"))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tickers: Vec<String>,
        fields: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        all_fields: bool,
        aliases: Option<HashMap<String, String>>,
        on_delayed: &str,
        isolated: bool,
        rows: bool,
        on_field_error: &str,
        zero_as_null: Option<Vec<String>>,
        session_wait_ms: Option<u64>,
        delivery: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let PreparedSubscribe {
            request,
            batch_items,
        } = prepare_subscribe_params(
            SubscribeParams {
                service: None,
                tickers,
                fields,
                options: None,
                flush_threshold,
                overflow_policy,
                stream_capacity,
                all_fields,
                aliases,
                on_delayed,
                isolated,
                rows,
                on_field_error,
                zero_as_null,
                session_wait_ms,
                delivery,
            },
            self.subscription_batch_items,
        )?;

        debug!(
            tickers = ?request.topics,
            fields = ?request.fields,
            "PyEngine: creating subscription"
        );

        shutdown_safe_future(py, async move {
            let stream = engine
                .subscribe(request)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription created");

            Ok(PySubscription::from_hosted_stream(
                stream,
                batch_items,
                &engine,
            ))
        })
    }

    /// Subscribe to real-time data with custom service and options.
    ///
    /// This is the generic subscription method for services like //blp/mktvwap.
    ///
    /// Args:
    ///     service: Bloomberg service URI (e.g., "//blp/mktvwap")
    ///     tickers: List of securities to subscribe to
    ///     fields: List of fields to subscribe to
    ///     options: List of subscription options (e.g., ["VWAP_START_TIME=09:30"])
    ///     flush_threshold: Maximum number of already-queued updates combined into
    ///         one batch (default: the engine's subscription_flush_threshold, 16).
    ///         Reads never wait for more updates.
    ///     aliases: Optional Bloomberg topic to consumer label mapping
    ///     on_delayed: Delayed stream policy: warn, raise, or ignore
    ///     isolated: Disable sharing of upstream market-data feeds
    ///     rows: Deliver rows for iteration; False supports latest() only
    ///     on_field_error: Field rejection policy: warn, raise, or ignore
    ///     zero_as_null: Fields whose numeric zeros become null in latest()
    ///     session_wait_ms: Optional pool-session claim deadline in milliseconds;
    ///         expiry raises BlpValidationError("Configuration error: session_wait: ...")
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe_with_options(
    ///     '//blp/mktvwap',
    ///     ['//blp/mktvwap/ticker/IBM US Equity'],
    ///     ['VWAP'],
    ///     ['VWAP_START_TIME=10:00', 'VWAP_END_TIME=16:00']
    /// )
    /// async for batch in sub:
    ///     print(batch)
    /// ```
    #[pyo3(signature = (service, tickers, fields, options=None, flush_threshold=None, overflow_policy=None, stream_capacity=None, all_fields=false, aliases=None, on_delayed="warn", isolated=false, rows=true, on_field_error="warn", zero_as_null=None, session_wait_ms=None, delivery="deltas"))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe_with_options<'py>(
        &self,
        py: Python<'py>,
        service: String,
        tickers: Vec<String>,
        fields: Vec<String>,
        options: Option<Vec<String>>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        all_fields: bool,
        aliases: Option<HashMap<String, String>>,
        on_delayed: &str,
        isolated: bool,
        rows: bool,
        on_field_error: &str,
        zero_as_null: Option<Vec<String>>,
        session_wait_ms: Option<u64>,
        delivery: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let PreparedSubscribe {
            request,
            batch_items,
        } = prepare_subscribe_params(
            SubscribeParams {
                service: Some(service),
                tickers,
                fields,
                options,
                flush_threshold,
                overflow_policy,
                stream_capacity,
                all_fields,
                aliases,
                on_delayed,
                isolated,
                rows,
                on_field_error,
                zero_as_null,
                session_wait_ms,
                delivery,
            },
            self.subscription_batch_items,
        )?;

        debug!(
            service = %request.service,
            tickers = ?request.topics,
            fields = ?request.fields,
            options = ?request.options,
            "PyEngine: creating subscription with options"
        );

        shutdown_safe_future(py, async move {
            let stream = engine
                .subscribe(request)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription with options created");

            Ok(PySubscription::from_hosted_stream(
                stream,
                batch_items,
                &engine,
            ))
        })
    }

    /// List retained upstream feeds without authentication or transport details.
    fn subscription_feeds(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let feeds = py.detach(|| self.engine.subscription_feeds());
        feeds
            .into_iter()
            .map(|feed| {
                let row = PyDict::new(py);
                row.set_item("service", feed.service)?;
                row.set_item("topic", feed.topic)?;
                row.set_item("options", feed.options)?;
                row.set_item("fields", feed.fields)?;
                row.set_item("consumers", feed.consumers)?;
                row.set_item("delayed", feed.delayed)?;
                row.set_item("state", feed.state)?;
                row.set_item("isolated", feed.isolated)?;
                row.set_item("field_errors", feed.field_errors)?;
                Ok(row.unbind())
            })
            .collect()
    }

    // =========================================================================
    // Lifecycle Management
    // =========================================================================

    /// Signal engine shutdown.
    ///
    /// Signals all worker threads to stop; they terminate when they finish
    /// their current work or see the shutdown signal. Pending Python operations
    /// observe the shutdown. Workers are not joined, but this can wait for
    /// native lifecycle locks and in-flight partial-response decoding, and it
    /// stops idle pooled Bloomberg sessions; the interpreter is released while
    /// it runs.
    fn signal_shutdown(&self, py: Python<'_>) {
        info!("PyEngine: signal_shutdown called");
        py.detach(|| self.engine.signal_shutdown());
    }

    fn worker_health(&self) -> PyResult<Vec<(usize, String)>> {
        Ok(self
            .engine
            .request_pool_health()
            .into_iter()
            .map(|(id, h)| (id, h.as_str().to_string()))
            .collect())
    }

    /// Servers each Bloomberg session is connected to, without credentials.
    ///
    /// One dict per session (`pool`, `worker`, `servers`): request workers
    /// first, then subscription workers.
    fn connections(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let sessions = py.detach(|| self.engine.connections());
        sessions
            .into_iter()
            .map(|session| {
                let row = PyDict::new(py);
                row.set_item("pool", session.pool.as_str())?;
                row.set_item("worker", session.worker_id)?;
                row.set_item("servers", session.servers)?;
                Ok(row.unbind())
            })
            .collect()
    }

    /// Check if the Bloomberg connection is healthy.
    ///
    /// Returns True if at least one worker has a live Bloomberg session.
    fn is_connected(&self) -> bool {
        self.engine
            .request_pool_health()
            .iter()
            .any(|(_, h)| h.as_str() == "healthy")
    }
}

impl PyEngine {
    /// Shared helper: release GIL and start Engine on a blocking thread.
    #[allow(clippy::result_large_err)]
    fn start_engine(py: Python<'_>, config: EngineConfig) -> PyResult<Self> {
        // Release GIL during blocking Engine::start().
        // Engine::start() creates Bloomberg sessions and waits for them to connect,
        // which can take seconds — must not hold GIL during this.
        let subscription_batch_items = config.subscription_flush_threshold;
        let engine = py.detach(|| Engine::start(config)).map_err(|e| {
            warn!(error = %e, "PyEngine: connection failed");
            blp_async_error_to_pyerr(e)
        })?;

        info!("PyEngine: connected successfully");

        Ok(Self {
            engine: Arc::new(EngineOwner::new(engine)),
            subscription_batch_items,
            schema_cache: Arc::new(SchemaJsonCache::new()),
        })
    }
}
