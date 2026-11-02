//! Completion boundary between native futures and Python awaitables.
//!
//! Async bindings hand [`shutdown_safe_future`] a future that yields an owned
//! Rust payload. The bridge runtime's workers only poll that future; they
//! never attach to the interpreter. pyo3-async-runtimes later attaches once per
//! completion on the bridge's blocking pool, skips awaitables that were already
//! cancelled, and converts the payload through `IntoPyObject`. Python objects,
//! marked-delivery converter calls and publication receipts are therefore all
//! created there. Dropping an unconverted payload (cancellation, interpreter
//! finalization) releases its guards and rolls back its native receipt without
//! the interpreter.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};

use arrow_array::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDate, PyDateTime, PyDict, PyList, PyTime, PyTzInfo};
use pyo3_async_runtimes::err::RustPanic;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::watch;
use xbbg_async::engine::SubscriptionDelivery;
use xbbg_async::engine::state::{SubscriptionUpdate, UpdateValue};
use xbbg_async::subscription_consumer::{
    SubscriptionDeliveryAck, SubscriptionDeliveryDrainAck, SubscriptionDeliveryDrainLease,
    SubscriptionDeliveryLease, wait_for_subscription_close,
};
use xbbg_ext::{ExchangeInfo, MarketInfo};

use crate::native_arrow::{ArrowRecordBatch, record_batch_to_arrow_record_batch};
use crate::teardown::SubscriptionOwner;
use crate::{blp_async_error_to_pyerr, blp_error_to_pyerr};

// =============================================================================
// Interpreter-finalization gate
// =============================================================================

static INTERPRETER_SHUTDOWN: LazyLock<watch::Sender<bool>> =
    LazyLock::new(|| watch::channel(false).0);

/// Record that interpreter finalization has begun; later completions are dropped.
pub(crate) fn signal_interpreter_shutdown() {
    INTERPRETER_SHUTDOWN.send_replace(true);
}

async fn complete_unless_interpreter_finalizing<F>(
    finalizing_rx: &mut watch::Receiver<bool>,
    fut: F,
) -> F::Output
where
    F: Future,
{
    if *finalizing_rx.borrow() {
        return std::future::pending::<F::Output>().await;
    }
    tokio::select! {
        biased;
        _ = wait_for_subscription_close(finalizing_rx) => {
            std::future::pending::<F::Output>().await
        }
        result = fut => result,
    }
}

/// Whether the interpreter can still receive a completion, checked without
/// attaching. Before 3.13 only initialization is observable from native code.
fn interpreter_accepts_completion() -> bool {
    // SAFETY: Py_IsInitialized only reads runtime state and may be called
    // without an attached thread state; PyO3 calls it before attaching.
    if unsafe { pyo3::ffi::Py_IsInitialized() } == 0 {
        return false;
    }
    // SAFETY: Py_IsFinalizing (3.13+) atomically reads the runtime's
    // finalization flag and, like Py_IsInitialized, needs no thread state.
    #[cfg(Py_3_13)]
    if unsafe { pyo3::ffi::Py_IsFinalizing() } != 0 {
        return false;
    }
    true
}

/// Gate one completed result. After finalization starts (signalled, or
/// observed natively) the result is dropped here, unattached, and the
/// awaitable stays unresolved.
async fn complete_for_python<F, T>(
    mut finalizing_rx: watch::Receiver<bool>,
    fut: F,
) -> PyResult<Completed<T>>
where
    F: Future<Output = PyResult<T>>,
{
    let result = complete_unless_interpreter_finalizing(&mut finalizing_rx, fut).await;
    let accepted = !*finalizing_rx.borrow() && interpreter_accepts_completion();
    if accepted {
        return result.map(Completed);
    }
    drop(result);
    std::future::pending().await
}

/// Interpreter-finalization-safe wrapper for [`future_into_py`].
///
/// Ordinary engine shutdown is deliberately not a completion barrier: request
/// errors, subscription termination, and buffered drains must still reach the
/// caller. Only interpreter finalization suppresses completion. The atexit
/// signal is checked before polling and the finalization branch is biased;
/// every completed result is then re-gated on that signal and on the
/// interpreter's own state (`Py_IsInitialized`, plus `Py_IsFinalizing` on
/// 3.13+) without attaching. `T` is converted only in pyo3-async-runtimes'
/// completion attach.
pub(crate) fn shutdown_safe_future<'py, F, T>(
    py: Python<'py>,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    future_into_py(
        py,
        complete_for_python(INTERPRETER_SHUTDOWN.subscribe(), fut),
    )
}

/// Arrow completion shared by requests, recipes and the request fixture: the
/// future yields an owned batch, wrapped as `ArrowRecordBatch` on completion.
pub(crate) fn record_batch_future<'py, F>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<RecordBatch>> + Send + 'static,
{
    shutdown_safe_future(py, async move { fut.await.map(ArrowRecordBatch::new) })
}

// =============================================================================
// Payload carriers
// =============================================================================

/// Converts a payload inside pyo3-async-runtimes' completion attach.
///
/// That conversion runs in a blocking task whose join handle the library
/// discards, so an escaping panic would leave the awaitable pending forever.
/// With unwinding it is raised as `RustPanic` instead, matching panics in the
/// polled future itself.
struct Completed<T>(T);

impl<'py, T> IntoPyObject<'py> for Completed<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Self(payload) = self;
        match catch_unwind(AssertUnwindSafe(move || payload.into_bound_py_any(py))) {
            Ok(converted) => converted,
            Err(panic) => Err(RustPanic::new_err(format!(
                "rust future panicked during completion: {}",
                panic_message(panic.as_ref())
            ))),
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = panic.downcast_ref::<&str>() {
        message
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message
    } else {
        "unknown panic"
    }
}

/// A payload whose native ordering guards are released only after its Python
/// result exists, or when the unconverted payload is dropped.
///
/// Plain reads keep their receiver/publication guards and unsubscribe keeps
/// its operation guard this way, as they did when conversion ran inside the
/// future.
pub(crate) struct Guarded<T, G> {
    value: T,
    guards: G,
}

impl<T, G> Guarded<T, G> {
    pub(crate) fn new(value: T, guards: G) -> Self {
        Self { value, guards }
    }
}

impl<'py, T, G> IntoPyObject<'py> for Guarded<T, G>
where
    T: IntoPyObject<'py>,
{
    type Target = T::Target;
    type Output = T::Output;
    type Error = T::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let Self { value, guards } = self;
        let converted = value.into_pyobject(py);
        drop(guards);
        converted
    }
}

/// Exchange metadata shaped as the `resolve_exchange` dict.
pub(crate) struct ExchangeInfoDict(pub(crate) ExchangeInfo);

impl<'py> IntoPyObject<'py> for ExchangeInfoDict {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let Self(info) = self;
        let dict = PyDict::new(py);
        dict.set_item("ticker", info.ticker)?;
        dict.set_item("mic", info.mic)?;
        dict.set_item("exch_code", info.exch_code)?;
        dict.set_item("timezone", info.timezone)?;
        dict.set_item("utc_offset", info.utc_offset)?;
        dict.set_item("source", info.source.as_str())?;
        dict.set_item("day", info.sessions.day)?;
        dict.set_item("allday", info.sessions.allday)?;
        dict.set_item("pre", info.sessions.pre)?;
        dict.set_item("post", info.sessions.post)?;
        dict.set_item("am", info.sessions.am)?;
        dict.set_item("pm", info.sessions.pm)?;
        Ok(dict)
    }
}

/// Market metadata shaped as the `fetch_market_info` dict.
pub(crate) struct MarketInfoDict(pub(crate) MarketInfo);

impl<'py> IntoPyObject<'py> for MarketInfoDict {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let Self(info) = self;
        let dict = PyDict::new(py);
        dict.set_item("exch", info.exch)?;
        dict.set_item("tz", info.tz)?;
        dict.set_item("freq", info.freq)?;
        dict.set_item("is_fut", info.is_fut)?;
        Ok(dict)
    }
}

/// One update shaped as the tick-mode dict: UTC-aware `timestamp`, `topic`,
/// present fields only, and explicit nulls as `None`.
pub(crate) struct TickDict(pub(crate) SubscriptionUpdate);

impl<'py> IntoPyObject<'py> for TickDict {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let Self(update) = self;
        let dict = PyDict::new(py);
        dict.set_item(
            "timestamp",
            timestamp_micros_to_py(py, update.timestamp_us)?,
        )?;
        dict.set_item("topic", update.topic.as_ref())?;
        for field in update.values.iter() {
            let Some(meta) = update.layout.fields.get(field.index as usize) else {
                continue;
            };
            let name = meta.name.as_ref();
            match &field.value {
                UpdateValue::Null => dict.set_item(name, py.None())?,
                UpdateValue::Bool(v) => dict.set_item(name, *v)?,
                UpdateValue::I32(v) => dict.set_item(name, *v)?,
                UpdateValue::I64(v) => dict.set_item(name, *v)?,
                UpdateValue::F64(v) => dict.set_item(name, *v)?,
                UpdateValue::Str(v) => dict.set_item(name, v.as_ref())?,
                UpdateValue::Date32(v) => dict.set_item(name, date32_to_py(py, *v)?)?,
                UpdateValue::Time64Micros(v) => {
                    dict.set_item(name, time64_micros_to_py(py, *v)?)?
                }
                UpdateValue::TimestampMicros(v) => {
                    dict.set_item(name, timestamp_micros_to_py(py, *v)?)?
                }
            }
        }
        Ok(dict)
    }
}

fn date32_to_py(py: Python<'_>, days: i32) -> PyResult<Py<PyAny>> {
    let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        return Ok(py.None());
    };
    let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) else {
        return Ok(py.None());
    };
    Ok(
        PyDate::new(py, date.year(), date.month() as u8, date.day() as u8)?
            .into_any()
            .unbind(),
    )
}

fn time64_micros_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    if !(0..86_400_000_000).contains(&micros) {
        return Ok(py.None());
    }
    let seconds = micros / 1_000_000;
    let microsecond = (micros % 1_000_000) as u32;
    let hour = (seconds / 3_600) as u8;
    let minute = ((seconds % 3_600) / 60) as u8;
    let second = (seconds % 60) as u8;
    Ok(PyTime::new(py, hour, minute, second, microsecond, None)?
        .into_any()
        .unbind())
}

fn timestamp_micros_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    let Some(dt) = DateTime::from_timestamp_micros(micros) else {
        return Ok(py.None());
    };
    let utc = PyTzInfo::utc(py)?;
    Ok(PyDateTime::new(
        py,
        dt.year(),
        dt.month() as u8,
        dt.day() as u8,
        dt.hour() as u8,
        dt.minute() as u8,
        dt.second() as u8,
        dt.timestamp_subsec_micros(),
        Some(&utc),
    )?
    .into_any()
    .unbind())
}

/// `PySubscription.unsubscribe` result in the requested drain shape.
pub(crate) enum Unsubscribed {
    /// Close without draining: `None`.
    Closed,
    /// Tick-mode drain: one dict per unread update.
    Ticks(Vec<SubscriptionUpdate>),
    /// Arrow drain: one `ArrowRecordBatch` per batch.
    Batches(Vec<RecordBatch>),
}

impl<'py> IntoPyObject<'py> for Unsubscribed {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Closed => Ok(py.None().into_bound(py)),
            Self::Ticks(updates) => {
                let ticks = PyList::new(py, updates.into_iter().map(TickDict))?;
                Ok(ticks.into_any())
            }
            Self::Batches(batches) => {
                let batches = PyList::new(py, batches.into_iter().map(ArrowRecordBatch::new))?;
                Ok(batches.into_any())
            }
        }
    }
}

// =============================================================================
// Marked deliveries
// =============================================================================

enum SubscriptionPublicationLease {
    ReadAck(SubscriptionDeliveryAck),
    ReadError(SubscriptionDeliveryLease),
    DrainAck(SubscriptionDeliveryDrainAck),
    DrainError(SubscriptionDeliveryDrainLease),
}

impl SubscriptionPublicationLease {
    fn commit(self) -> PyResult<()> {
        match self {
            Self::ReadAck(ack) => {
                ack.commit();
                Ok(())
            }
            Self::ReadError(lease) => lease.commit().map(|_| ()).map_err(blp_error_to_pyerr),
            Self::DrainAck(ack) => {
                ack.commit();
                Ok(())
            }
            Self::DrainError(lease) => lease
                .commit()
                .map(|_| ())
                .map_err(|error| blp_async_error_to_pyerr(*error)),
        }
    }
}

/// Retains the native receipt until the consuming Python task actually resumes.
/// Dropping an unpublished result restores updates/gaps, re-dirties state, or
/// retains a frozen drain for a later publication attempt.
#[pyclass(frozen)]
pub(crate) struct PySubscriptionDeliveryReceipt {
    lease: StdMutex<Option<SubscriptionPublicationLease>>,
    result: Option<Py<PyAny>>,
    /// Released after the lease (fields drop in order), so the subscription's
    /// own drop decides whether native teardown runs inline or deferred.
    _owner: Arc<SubscriptionOwner>,
}

impl PySubscriptionDeliveryReceipt {
    fn new(
        publication: SubscriptionPublicationLease,
        result: Option<Py<PyAny>>,
        owner: Arc<SubscriptionOwner>,
    ) -> Self {
        Self {
            lease: StdMutex::new(Some(publication)),
            result,
            _owner: owner,
        }
    }
}

#[pymethods]
impl PySubscriptionDeliveryReceipt {
    fn publish(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let lease = self
            .lease
            .lock()
            .expect("subscription delivery receipt poisoned")
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("subscription delivery already published"))?;
        lease.commit()?;
        Ok(self
            .result
            .as_ref()
            .expect("a successful receipt has a converted result")
            .clone_ref(py))
    }

    #[getter]
    fn has_error(&self) -> bool {
        self.result.is_none()
    }

    fn discard(&self) {
        let lease = self
            .lease
            .lock()
            .expect("subscription delivery receipt poisoned")
            .take();
        drop(lease);
    }
}

/// One marked delivery and its optional converter.
///
/// Conversion prepares the lease, runs the converter and wraps both in a
/// publication receipt; the commit still happens in `publish()`. A failed
/// conversion or a dropped unconverted payload rolls the lease back. The
/// subscription owner outlives the lease, so a receipt or payload released
/// after its engine never runs SDK teardown inline.
pub(crate) struct DeliveryPayload {
    lease: SubscriptionDeliveryLease,
    converter: Option<Py<PyAny>>,
    owner: Arc<SubscriptionOwner>,
}

impl DeliveryPayload {
    pub(crate) fn new(
        lease: SubscriptionDeliveryLease,
        converter: Option<Py<PyAny>>,
        owner: Arc<SubscriptionOwner>,
    ) -> Self {
        Self {
            lease,
            converter,
            owner,
        }
    }
}

impl<'py> IntoPyObject<'py> for DeliveryPayload {
    type Target = PySubscriptionDeliveryReceipt;
    type Output = Bound<'py, PySubscriptionDeliveryReceipt>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let Self {
            lease,
            converter,
            owner,
        } = self;
        let (publication, result) = match lease.prepare_success() {
            Ok((delivery, ack)) => {
                let result = subscription_delivery_to_python(py, delivery, converter.as_ref())?;
                (SubscriptionPublicationLease::ReadAck(ack), Some(result))
            }
            Err(lease) => (SubscriptionPublicationLease::ReadError(lease), None),
        };
        Bound::new(
            py,
            PySubscriptionDeliveryReceipt::new(publication, result, owner),
        )
    }
}

/// A frozen marked drain and its optional converter, converted like
/// [`DeliveryPayload`]. Dropping it unconverted retains the drain for retry.
pub(crate) struct DrainPayload {
    lease: SubscriptionDeliveryDrainLease,
    converter: Option<Py<PyAny>>,
    owner: Arc<SubscriptionOwner>,
}

impl DrainPayload {
    pub(crate) fn new(
        lease: SubscriptionDeliveryDrainLease,
        converter: Option<Py<PyAny>>,
        owner: Arc<SubscriptionOwner>,
    ) -> Self {
        Self {
            lease,
            converter,
            owner,
        }
    }
}

impl<'py> IntoPyObject<'py> for DrainPayload {
    type Target = PySubscriptionDeliveryReceipt;
    type Output = Bound<'py, PySubscriptionDeliveryReceipt>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let Self {
            lease,
            converter,
            owner,
        } = self;
        let (publication, result) = match lease.prepare_success() {
            Ok((deliveries, ack)) => {
                let list = PyList::empty(py);
                for delivery in deliveries {
                    list.append(subscription_delivery_to_python(
                        py,
                        delivery,
                        converter.as_ref(),
                    )?)?;
                }
                (
                    SubscriptionPublicationLease::DrainAck(ack),
                    Some(list.into_any().unbind()),
                )
            }
            Err(lease) => (SubscriptionPublicationLease::DrainError(lease), None),
        };
        Bound::new(
            py,
            PySubscriptionDeliveryReceipt::new(publication, result, owner),
        )
    }
}

fn subscription_delivery_to_python(
    py: Python<'_>,
    delivery: SubscriptionDelivery,
    converter: Option<&Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let SubscriptionDelivery {
        kind,
        data,
        gap_before,
    } = delivery;
    let batch = record_batch_to_arrow_record_batch(py, data)?;
    let dropped_updates = gap_before.map(|gap| gap.dropped_updates);
    let args = (kind.as_str(), batch, dropped_updates);
    match converter {
        Some(converter) => Ok(converter.bind(py).call1(args)?.unbind()),
        None => Ok(args.into_pyobject(py)?.into_any().unbind()),
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use pyo3::exceptions::PyValueError;
    use pyo3::types::{PyCFunction, PyTuple};
    use xbbg_async::OverflowPolicy;
    use xbbg_async::engine::state::{
        FieldKind, FieldLayout, FieldMeta, UpdateField, subscription_channel,
        subscription_channel_with_policy,
    };
    use xbbg_async::subscription_consumer::SubscriptionConsumer;
    use xbbg_core::BlpError;

    use super::*;
    use crate::BlpSubscriptionDataLossError;
    use crate::tests::run_while_interpreter_held;

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn subscription_update_with_layout(
        layout: Arc<FieldLayout>,
        timestamp_us: i64,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from("SYNTHETIC Equity"),
            layout,
            values: Default::default(),
        }
    }

    fn subscription_update(timestamp_us: i64) -> SubscriptionUpdate {
        subscription_update_with_layout(Arc::new(FieldLayout::new(1, Vec::new())), timestamp_us)
    }

    /// A live producer and a subscription whose only unread update follows one
    /// dropped update.
    fn consumer_with_gap() -> (impl Sized, Arc<SubscriptionOwner>) {
        let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        for timestamp_us in [10, 20] {
            let mut update = subscription_update(timestamp_us);
            update.topic = Arc::from("SYNTHETIC Index");
            tx.try_send(Ok(update)).expect("synthetic update");
        }
        (tx, owned(SubscriptionConsumer::from_receiver(rx)))
    }

    fn owned(consumer: SubscriptionConsumer) -> Arc<SubscriptionOwner> {
        Arc::new(SubscriptionOwner::unhosted(consumer))
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PanicsDuringConversion;

    impl<'py> IntoPyObject<'py> for PanicsDuringConversion {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, _py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            panic!("synthetic conversion panic")
        }
    }

    /// Converts to whether its lock was held at conversion time.
    struct LockProbe(Arc<tokio::sync::Mutex<()>>);

    impl<'py> IntoPyObject<'py> for LockProbe {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            self.0.try_lock().is_err().into_bound_py_any(py)
        }
    }

    #[test]
    fn completion_respects_finalization_signal() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (finalizing, finalizing_rx) = watch::channel(false);
            let Completed(value) =
                complete_for_python(finalizing_rx.clone(), async { Ok::<_, PyErr>(7) })
                    .await
                    .expect("live completion");
            assert_eq!(value, 7);

            finalizing.send_replace(true);
            let suppressed = tokio::time::timeout(
                Duration::from_millis(1),
                complete_for_python(finalizing_rx, async { Ok::<_, PyErr>(8) }),
            )
            .await;
            assert!(suppressed.is_err());
        });
    }

    #[test]
    fn gate_drops_results_completed_after_the_finalization_watch_flips() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (finalizing, finalizing_rx) = watch::channel(false);
            let dropped = Arc::new(AtomicBool::new(false));
            let payload = DropFlag(dropped.clone());
            let gated = complete_for_python(finalizing_rx, async move {
                finalizing.send_replace(true);
                Ok::<_, PyErr>(payload)
            });
            let outcome = tokio::time::timeout(Duration::from_millis(20), gated).await;
            assert!(outcome.is_err(), "a rejected completion never resolves");
            assert!(
                dropped.load(Ordering::SeqCst),
                "the rejected result is dropped natively"
            );
        });
    }

    #[test]
    fn engine_shutdown_completes_but_interpreter_finalization_suppresses() {
        current_thread_runtime().block_on(async {
            let (engine_shutdown, mut engine_shutdown_rx) = watch::channel(false);
            let (_finalizing, mut finalizing_rx) = watch::channel(false);
            engine_shutdown.send_replace(true);
            let completed =
                complete_unless_interpreter_finalizing(&mut finalizing_rx, async move {
                    wait_for_subscription_close(&mut engine_shutdown_rx).await;
                    42
                })
                .await;
            assert_eq!(completed, 42);

            let (finalizing, mut finalizing_rx) = watch::channel(false);
            finalizing.send_replace(true);
            let suppressed = tokio::time::timeout(
                Duration::from_millis(1),
                complete_unless_interpreter_finalizing(&mut finalizing_rx, async { 7 }),
            )
            .await;
            assert!(suppressed.is_err());
        });
    }

    #[test]
    fn completion_panics_surface_as_rust_panic() {
        Python::initialize();
        Python::attach(|py| {
            let error = Completed(PanicsDuringConversion)
                .into_pyobject(py)
                .expect_err("a conversion panic resolves the awaitable with an error");
            assert!(error.is_instance_of::<RustPanic>(py));
            assert!(
                error
                    .value(py)
                    .to_string()
                    .contains("synthetic conversion panic")
            );
        });
    }

    #[test]
    fn guarded_payloads_release_guards_only_after_conversion() {
        Python::initialize();
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let converted = Guarded::new(
            LockProbe(lock.clone()),
            lock.clone().try_lock_owned().expect("unlocked"),
        );
        Python::attach(|py| {
            let held: bool = converted.into_pyobject(py).unwrap().extract().unwrap();
            assert!(held, "the guard is held while the value converts");
        });
        assert!(lock.try_lock().is_ok(), "conversion releases the guard");

        let unconverted = Guarded::new(
            LockProbe(lock.clone()),
            lock.clone().try_lock_owned().expect("unlocked"),
        );
        run_while_interpreter_held(move || drop(unconverted));
        assert!(
            lock.try_lock().is_ok(),
            "dropping releases the guard natively"
        );
    }

    #[test]
    fn timestamp_micros_to_py_returns_utc_aware_datetime() {
        Python::initialize();
        Python::attach(|py| {
            let value = timestamp_micros_to_py(py, 1_704_067_200_123_456).expect("timestamp");
            let tzinfo = value.bind(py).getattr("tzinfo").expect("tzinfo");
            assert!(
                tzinfo
                    .eq(PyTzInfo::utc(py).expect("utc"))
                    .expect("tz equality")
            );
        });
    }

    #[test]
    fn tick_dict_preserves_present_null_and_omits_absent_fields() {
        Python::initialize();
        Python::attach(|py| {
            let layout = Arc::new(FieldLayout::new(
                1,
                vec![
                    FieldMeta::new("PX_LAST", 0, FieldKind::F64),
                    FieldMeta::new("BID", 1, FieldKind::F64),
                ],
            ));
            let mut update = subscription_update_with_layout(layout, 10);
            update.values.push(UpdateField {
                index: 0,
                value: UpdateValue::Null,
            });

            let dict = TickDict(update).into_pyobject(py).expect("tick dict");
            assert!(dict.contains("PX_LAST").expect("present field"));
            assert!(
                dict.get_item("PX_LAST")
                    .expect("lookup")
                    .expect("present value")
                    .is_none()
            );
            assert!(!dict.contains("BID").expect("absent field"));
        });
    }

    #[test]
    fn delivery_conversion_failure_restores_real_native_batch_and_gap() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (producer, consumer) = consumer_with_gap();
            let lease = consumer.next_delivery(1).await.expect("first receipt");
            Python::attach(|py| {
                let failing = PyCFunction::new_closure(
                    py,
                    None,
                    None,
                    |_args: &Bound<'_, PyTuple>,
                     _kwargs: Option<&Bound<'_, PyDict>>|
                     -> PyResult<()> {
                        Err(PyValueError::new_err(
                            "synthetic backend conversion failure",
                        ))
                    },
                )
                .expect("converter");
                let error = DeliveryPayload::new(
                    lease,
                    Some(failing.into_any().unbind()),
                    consumer.clone(),
                )
                .into_pyobject(py)
                .expect_err("conversion failure");
                assert!(error.is_instance_of::<PyValueError>(py));
            });

            let lease = consumer.next_delivery(1).await.expect("restored receipt");
            assert_eq!(
                lease.as_ref().unwrap().gap_before.unwrap().dropped_updates,
                1
            );
            Python::attach(|py| {
                let receipt = DeliveryPayload::new(lease, None, consumer.clone())
                    .into_pyobject(py)
                    .expect("converted delivery");
                let output = receipt.call_method0("publish").unwrap();
                assert_eq!(
                    output.get_item(0).unwrap().extract::<String>().unwrap(),
                    "deltas"
                );
                assert_eq!(output.get_item(2).unwrap().extract::<u64>().unwrap(), 1);
                let rows = output
                    .get_item(1)
                    .unwrap()
                    .call_method0("to_pylist")
                    .unwrap();
                assert_eq!(rows.len().unwrap(), 1);
                assert_eq!(
                    rows.get_item(0)
                        .unwrap()
                        .get_item("topic")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "SYNTHETIC Index",
                );
            });
            drop(producer);
            assert!(consumer.next_delivery(1).await.is_none());
        });
    }

    #[test]
    fn delivery_error_publication_preserves_context_and_commits_once() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (tx, rx) = subscription_channel(1);
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "SYNTHETIC Index".into(),
                detail: "synthetic SDK DATALOSS".into(),
            });
            let consumer = owned(SubscriptionConsumer::from_receiver(rx));
            let lease = consumer.next_delivery(1).await.expect("terminal receipt");
            let called = Arc::new(AtomicBool::new(false));
            let flag = called.clone();
            Python::attach(|py| {
                let converter = PyCFunction::new_closure(
                    py,
                    None,
                    None,
                    move |_args: &Bound<'_, PyTuple>,
                          _kwargs: Option<&Bound<'_, PyDict>>|
                          -> PyResult<()> {
                        flag.store(true, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .expect("converter");
                let receipt = DeliveryPayload::new(
                    lease,
                    Some(converter.into_any().unbind()),
                    consumer.clone(),
                )
                .into_pyobject(py)
                .expect("prepared terminal receipt");
                assert!(receipt.get().has_error());
                let error = receipt
                    .call_method0("publish")
                    .expect_err("terminal source loss");
                assert!(error.is_instance_of::<BlpSubscriptionDataLossError>(py));
                for (attribute, expected) in [
                    ("topic", "SYNTHETIC Index"),
                    ("detail", "synthetic SDK DATALOSS"),
                ] {
                    assert_eq!(
                        error
                            .value(py)
                            .getattr(attribute)
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        expected
                    );
                }
                assert!(receipt.call_method0("publish").is_err());
            });
            assert!(
                !called.load(Ordering::SeqCst),
                "terminal receipts must not run the batch converter"
            );
            assert!(consumer.next_delivery(1).await.is_none());
        });
    }

    #[test]
    fn dropping_unpublished_python_receipt_restores_data_and_gap() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (_producer, consumer) = consumer_with_gap();
            let lease = consumer.next_delivery(1).await.unwrap();
            Python::attach(|py| {
                let receipt = DeliveryPayload::new(lease, None, consumer.clone())
                    .into_pyobject(py)
                    .unwrap();
                drop(receipt);
            });
            let restored = consumer
                .next_delivery(1)
                .await
                .expect("unpublished receipt restored");
            assert_eq!(
                restored
                    .as_ref()
                    .unwrap()
                    .gap_before
                    .unwrap()
                    .dropped_updates,
                1
            );
            assert_eq!(restored.commit().unwrap().data.num_rows(), 1);
        });
    }

    #[test]
    fn dropping_unconverted_delivery_payload_restores_data_and_gap_without_python() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (_producer, consumer) = consumer_with_gap();
            let lease = consumer.next_delivery(1).await.expect("first receipt");
            let converter = Python::attach(|py| py.None());
            let payload = DeliveryPayload::new(lease, Some(converter), consumer.clone());
            run_while_interpreter_held(move || drop(payload));
            let restored = consumer
                .next_delivery(1)
                .await
                .expect("unconverted payload restored");
            assert_eq!(
                restored
                    .as_ref()
                    .unwrap()
                    .gap_before
                    .unwrap()
                    .dropped_updates,
                1
            );
            assert_eq!(restored.commit().unwrap().data.num_rows(), 1);
        });
    }

    #[test]
    fn dropping_unconverted_drain_payload_retains_the_drain_without_python() {
        Python::initialize();
        current_thread_runtime().block_on(async {
            let (_producer, consumer) = consumer_with_gap();
            let lease = consumer.prepare_unsubscribe_deliveries(true, None).await;
            assert_eq!(lease.as_ref().expect("drained deliveries").len(), 1);
            let payload = DrainPayload::new(lease, None, consumer.clone());
            run_while_interpreter_held(move || drop(payload));
            let retried = consumer
                .prepare_unsubscribe_deliveries(true, None)
                .await
                .commit()
                .expect("retained drain");
            assert_eq!(retried.len(), 1);
            assert_eq!(retried[0].gap_before.unwrap().dropped_updates, 1);
            assert_eq!(retried[0].data.num_rows(), 1);
        });
    }
}
