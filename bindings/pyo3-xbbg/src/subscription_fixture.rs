//! Private offline subscription driver, compiled only with `test-support`.

use super::*;
use xbbg_async::testing::SubscriptionFixture;

#[pyclass(name = "_SubscriptionFixture", module = "xbbg._core")]
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
            PySubscription {
                legacy_arrow: (!rx.requires_deliveries())
                    .then(|| Arc::new(LegacySubscriptionArrow::new(batch_items))),
                batch_items,
                status: handle.status(),
                deliver_rows: handle.delivers_rows(),
                overflow_policy: handle.overflow_policy(),
                consumer: Arc::new(SubscriptionConsumer::new(rx, handle)),
                engine_shutdown: watch::channel(false).1,
                _test_fixture: Some(fixture),
                _test_legacy_read_started: Some(legacy_read_started),
            }
            .into_python()
            .await
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
        let fixture = Arc::new(SubscriptionFixture::new(config));
        try_attach_or_suspend(move |py| {
            Py::new(
                py,
                PySubscriptionFixture {
                    fixture,
                    batch_items,
                    legacy_read_started: Arc::new(tokio::sync::Notify::new()),
                },
            )
            .map(Py::into_any)
        })
        .await
    })
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySubscriptionFixture>()?;
    module.add_function(wrap_pyfunction!(_test_subscription_fixture, module)?)?;
    Ok(())
}
