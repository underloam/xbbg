//! Private offline drivers, compiled only with `test-support`: a synthetic
//! subscription engine, a request-completion driver and async-bridge probes.

use std::sync::Once;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use xbbg_async::error_presentation::RequestErrorContext;
use xbbg_async::testing::SubscriptionFixture;

use super::*;

/// Ticks of the bridge probe task; advances only while a bridge worker is free.
static BRIDGE_TICKS: AtomicU64 = AtomicU64::new(0);
static BRIDGE_TICKER: Once = Once::new();

#[pyclass(name = "_SubscriptionFixture", module = "xbbg._core", frozen)]
struct PySubscriptionFixture {
    fixture: Arc<SubscriptionFixture>,
    batch_items: usize,
    legacy_read_started: Arc<tokio::sync::Notify>,
}

impl PySubscriptionFixture {
    fn start_subscribe<'py>(
        &self,
        py: Python<'py>,
        request: SubscribeRequest,
    ) -> PyResult<Bound<'py, PyAny>> {
        let fixture = self.fixture.clone();
        let legacy_read_started = self.legacy_read_started.clone();
        let batch_items = request.flush_threshold.unwrap_or(self.batch_items);
        if batch_items == 0 {
            return Err(PyValueError::new_err(
                "flush_threshold must be greater than zero",
            ));
        }
        shutdown_safe_future(py, async move {
            let stream = fixture
                .subscribe(request)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            let (rx, handle) = stream.into_parts();
            Ok(PySubscription {
                legacy_arrow: (!rx.requires_deliveries())
                    .then(|| Arc::new(LegacySubscriptionArrow::new(batch_items))),
                batch_items,
                status: handle.status(),
                deliver_rows: handle.delivers_rows(),
                overflow_policy: handle.overflow_policy(),
                consumer: Arc::new(SubscriptionOwner::unhosted(SubscriptionConsumer::new(
                    rx, handle,
                ))),
                engine_shutdown: watch::channel(false).1,
                _test_fixture: Some(fixture),
                _test_legacy_read_started: Some(legacy_read_started),
            })
        })
    }
}

#[pymethods]
impl PySubscriptionFixture {
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
        self.subscribe_with_options(
            py,
            "//blp/mktdata".to_owned(),
            tickers,
            fields,
            None,
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (service, tickers, fields, options=None, flush_threshold=None, overflow_policy=None, stream_capacity=None, all_fields=false, aliases=None, on_delayed="warn", isolated=false, rows=true, on_field_error="warn", zero_as_null=None, session_wait_ms=None, delivery="deltas"))]
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
        let overflow_policy = overflow_policy
            .as_deref()
            .map(|value| {
                value
                    .parse::<OverflowPolicy>()
                    .map_err(PyValueError::new_err)
            })
            .transpose()?;
        self.start_subscribe(
            py,
            SubscribeRequest {
                service,
                topics: tickers,
                fields,
                all_fields,
                options: options.unwrap_or_default(),
                aliases: aliases.unwrap_or_default().into_iter().collect(),
                delayed_policy: parse_delayed_policy(on_delayed)?,
                field_error_policy: parse_field_error_policy(on_field_error)?,
                deliver_rows: rows,
                delivery: delivery
                    .parse::<SubscriptionDeliveryMode>()
                    .map_err(blp_async_error_to_pyerr)?,
                zero_as_null: zero_as_null.unwrap_or_default(),
                session_wait: session_wait_ms.map(std::time::Duration::from_millis),
                isolated,
                stream_capacity,
                flush_threshold,
                overflow_policy,
            },
        )
    }

    fn publish_json(&self, py: Python<'_>, topic: &str, payload: &str) -> PyResult<()> {
        py.detach(|| {
            self.fixture
                .publish_json(topic, payload)
                .map_err(blp_async_error_to_pyerr)
        })
    }

    fn data_loss(&self, py: Python<'_>, topic: &str) -> PyResult<()> {
        py.detach(|| {
            self.fixture
                .data_loss(topic)
                .map_err(blp_async_error_to_pyerr)
        })
    }

    fn settle<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let fixture = self.fixture.clone();
        shutdown_safe_future(py, async move {
            fixture.settle().await;
            Ok(())
        })
    }

    fn wait_legacy_read_started<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let signal = self.legacy_read_started.clone();
        shutdown_safe_future(py, async move {
            signal.notified().await;
            Ok(())
        })
    }

    fn terminate(&self, py: Python<'_>) {
        py.detach(|| self.fixture.terminate());
    }

    /// Busy-wait for `ms` milliseconds while attached to the interpreter.
    ///
    /// On GIL builds no other thread can attach meanwhile, which saturates
    /// every completion waiting for the interpreter.
    fn hold_attached(&self, _py: Python<'_>, ms: u64) {
        let deadline = Instant::now() + Duration::from_millis(ms);
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
    }

    /// Ticks of a 1 ms task on the async bridge runtime, started by the first call.
    ///
    /// The task only advances while a bridge worker is free to poll it.
    fn bridge_ticks(&self) -> u64 {
        BRIDGE_TICKER.call_once(|| {
            pyo3_async_runtimes::tokio::get_runtime().spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    BRIDGE_TICKS.fetch_add(1, Ordering::Relaxed);
                }
            });
        });
        BRIDGE_TICKS.load(Ordering::Relaxed)
    }
}

#[pyfunction]
#[pyo3(signature = (config=None))]
fn _test_subscription_fixture<'py>(
    py: Python<'py>,
    config: Option<PyRef<'_, PyEngineConfig>>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = match config {
        Some(config) => EngineConfig::try_from(&*config)?,
        None => EngineConfig::default(),
    };
    config.validate().map_err(blp_async_error_to_pyerr)?;
    let batch_items = config.subscription_flush_threshold;
    shutdown_safe_future(py, async move {
        Ok(PySubscriptionFixture {
            fixture: Arc::new(SubscriptionFixture::new(config)),
            batch_items,
            legacy_read_started: Arc::new(tokio::sync::Notify::new()),
        })
    })
}

/// Request-completion driver: synthetic Arrow results or chosen engine errors,
/// completed through the same carrier as `PyEngine.request`.
#[pyclass(name = "_RequestFixture", module = "xbbg._core", frozen)]
struct PyRequestFixture {
    /// Release generation; a gated request waits for a later generation.
    releases: watch::Sender<u64>,
}

#[pymethods]
impl PyRequestFixture {
    /// Resolve to a synthetic `rows`-row ArrowRecordBatch, or fail with the
    /// ErrorPresentation kind `error` (session, request, limit, data_loss,
    /// validation, timeout, internal). Any of `service`, `operation`,
    /// `request_id` or `bloomberg_request_ids` adds request context. Gated
    /// requests wait for the next `release_requests()`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (rows=1, *, gated=false, error=None, message=None, service=None, operation=None, request_id=None, bloomberg_request_ids=None))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        rows: usize,
        gated: bool,
        error: Option<&str>,
        message: Option<String>,
        service: Option<String>,
        operation: Option<String>,
        request_id: Option<String>,
        bloomberg_request_ids: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let failure = match error {
            None => None,
            Some(kind) => {
                let has_context = service.is_some()
                    || operation.is_some()
                    || request_id.is_some()
                    || bloomberg_request_ids.is_some();
                let request_context = has_context.then(|| RequestErrorContext {
                    service,
                    operation,
                    request_id,
                    bloomberg_request_ids: bloomberg_request_ids
                        .unwrap_or_default()
                        .into_iter()
                        .map(Arc::from)
                        .collect(),
                });
                Some(ErrorPresentation {
                    kind: parse_error_kind(kind)?,
                    message: message.unwrap_or_else(|| "synthetic request failure".to_owned()),
                    data_loss: None,
                    request_context,
                })
            }
        };
        let gate = gated.then(|| (*self.releases.borrow(), self.releases.subscribe()));
        record_batch_future(py, async move {
            if let Some((generation, mut releases)) = gate {
                let released = releases
                    .wait_for(|current| *current > generation)
                    .await
                    .is_ok();
                if !released {
                    return Err(PyRuntimeError::new_err(
                        "request fixture dropped before release",
                    ));
                }
            }
            match failure {
                Some(presentation) => Err(error_presentation_to_pyerr(presentation)),
                None => synthetic_request_batch(rows),
            }
        })
    }

    /// Release every gated request created so far, all at once.
    fn release_requests(&self) {
        self.releases.send_modify(|generation| *generation += 1);
    }
}

fn parse_error_kind(kind: &str) -> PyResult<ErrorKind> {
    Ok(match kind {
        "session" => ErrorKind::Session,
        "request" => ErrorKind::Request,
        "limit" => ErrorKind::Limit,
        "data_loss" => ErrorKind::DataLoss,
        "validation" => ErrorKind::Validation,
        "timeout" => ErrorKind::Timeout,
        "internal" => ErrorKind::Internal,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown ErrorPresentation kind: {other}"
            )));
        }
    })
}

fn synthetic_request_batch(rows: usize) -> PyResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("row", DataType::Int64, false),
    ]));
    let tickers: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("SYNTHETIC {row} Equity")),
    ));
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|row| row as i64),
    ));
    RecordBatch::try_new(schema, vec![tickers, values])
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
fn _test_request_fixture() -> PyRequestFixture {
    PyRequestFixture {
        releases: watch::channel(0).0,
    }
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySubscriptionFixture>()?;
    module.add_class::<PyRequestFixture>()?;
    module.add_function(wrap_pyfunction!(_test_subscription_fixture, module)?)?;
    module.add_function(wrap_pyfunction!(_test_request_fixture, module)?)?;
    Ok(())
}
