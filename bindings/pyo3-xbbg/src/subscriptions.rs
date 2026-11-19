//! Python subscription handle and async iterator for real-time market data.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};

use arrow_array::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::PyDict;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use tokio::sync::watch;
use xbbg_async::engine::state::{SubscriptionArrowBatcher, SubscriptionMetrics};
use xbbg_async::engine::{
    AdminStatusInfo, ServiceStatusInfo, SessionStatusInfo, SharedSubscriptionStatus,
    SubscriptionEventInfo, SubscriptionFailureInfo, TopicStatusInfo,
};
use xbbg_async::subscription_consumer::{
    SubscriptionConsumer, SubscriptionRead, receive_subscription_updates,
    subscription_batch_capacity_hint,
};
use xbbg_async::{BlpAsyncError, OverflowPolicy, SubscriptionHandle, SubscriptionStream};
use xbbg_log::debug;

use crate::completion::{
    DeliveryPayload, DrainPayload, Guarded, TickDict, TickDictKeys, Unsubscribed,
    shutdown_safe_future,
};
use crate::errors::{
    blp_async_error_to_pyerr, blp_error_to_pyerr, subscription_control_error_to_pyerr,
};
use crate::native_arrow;
use crate::teardown::{EngineOwner, SubscriptionOwner};

pub(crate) type SubscriptionMetricsMap = HashMap<usize, Arc<SubscriptionMetrics>>;
pub(crate) type SubscriptionEventTuple =
    (i64, String, String, String, Option<String>, Option<String>);
pub(crate) type SubscriptionTopicStateTuple = (String, String, i64, Option<bool>, String);

pub(crate) fn subscription_metrics_totals(
    metrics: &SubscriptionMetricsMap,
) -> (u64, u64, u64, bool, u64, u64, u64) {
    let messages_received = metrics
        .values()
        .map(|m| m.messages_received.load(Ordering::Relaxed))
        .sum();
    let dropped_batches = metrics
        .values()
        .map(|m| m.dropped_batches.load(Ordering::Relaxed))
        .sum();
    let batches_sent = metrics
        .values()
        .map(|m| m.batches_sent.load(Ordering::Relaxed))
        .sum();
    let slow_consumer = metrics
        .values()
        .any(|m| m.slow_consumer.load(Ordering::Relaxed));
    let data_loss_events = metrics
        .values()
        .map(|m| m.data_loss_events.load(Ordering::Relaxed))
        .sum();
    let last_message_us = metrics
        .values()
        .map(|m| m.last_message_us.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);
    let last_data_loss_us = metrics
        .values()
        .map(|m| m.last_data_loss_us.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);

    (
        messages_received,
        dropped_batches,
        batches_sent,
        slow_consumer,
        data_loss_events,
        last_message_us,
        last_data_loss_us,
    )
}

/// Python subscription handle for real-time market data.
///
/// Supports:
/// - Async iteration (`async for batch in sub`)
/// - Dynamic add/remove of ticker labels and expansion of requested fields
/// - Explicit unsubscribe with optional drain
/// - Context manager (`async with`)
/// Data arrives as native `SubscriptionUpdate`s and is batched into Rust Arrow
/// arrays on the consumer side before Python wrappers are attached.
/// Image-only subscriptions (`rows=False`) expose latest() but reject iteration.
/// Warning and status diagnostics remain available after unsubscribe.
///
/// Design: Uses separate locks for rx (data receiving) vs stream (metadata snapshots),
/// plus a dedicated operation lock to serialize add/remove/unsubscribe without holding
/// the stream metadata lock across Bloomberg awaits.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(frozen)]
pub struct PySubscription {
    /// Shared with in-flight futures; after the engine is released, the last
    /// release defers to a teardown thread (`teardown::HostedDrop`).
    pub(crate) consumer: Arc<SubscriptionOwner>,
    /// Existing strict Arrow batching; advanced readers use native receipts.
    pub(crate) legacy_arrow: Option<Arc<LegacySubscriptionArrow>>,
    /// Maximum immediately available updates returned by one Arrow iteration.
    pub(crate) batch_items: usize,
    /// Status remains available after the owning control has been released.
    pub(crate) status: SharedSubscriptionStatus,
    /// Immutable row-delivery mode; guards must not contend with async controls.
    pub(crate) deliver_rows: bool,
    /// Effective policy is immutable and remains diagnostic after unsubscribe.
    pub(crate) overflow_policy: OverflowPolicy,
    /// Engine shutdown is not iteration EOF; this only prevents close commands
    /// from being sent after global engine teardown has already cleaned up.
    pub(crate) engine_shutdown: watch::Receiver<bool>,
    /// Tick-dict field keys, reused while the update layout is unchanged.
    pub(crate) tick_dict_keys: Arc<StdMutex<TickDictKeys>>,
    /// Retain the synthetic engine while a real test subscription is alive.
    #[cfg(feature = "test-support")]
    pub(crate) _test_fixture: Option<Arc<xbbg_async::testing::SubscriptionFixture>>,
    #[cfg(feature = "test-support")]
    pub(crate) _test_legacy_read_started: Option<Arc<tokio::sync::Notify>>,
}

pub(crate) struct LegacySubscriptionArrow {
    batcher: StdMutex<SubscriptionArrowBatcher>,
    ready: StdMutex<VecDeque<RecordBatch>>,
}

impl LegacySubscriptionArrow {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            batcher: StdMutex::new(SubscriptionArrowBatcher::with_capacity(
                subscription_batch_capacity_hint(limit),
            )),
            ready: StdMutex::new(VecDeque::new()),
        }
    }
}

pub(crate) struct SubscriptionSnapshot {
    pub(crate) present: bool,
    pub(crate) topics: Vec<String>,
    pub(crate) fields: Vec<String>,
    pub(crate) is_active: bool,
    pub(crate) all_failed: bool,
    pub(crate) messages_received: u64,
    pub(crate) dropped_batches: u64,
    pub(crate) batches_sent: u64,
    pub(crate) slow_consumer: bool,
    pub(crate) data_loss_events: u64,
    pub(crate) last_message_us: u64,
    pub(crate) last_data_loss_us: u64,
    pub(crate) failures: Vec<SubscriptionFailureInfo>,
    pub(crate) topic_states: Vec<TopicStatusInfo>,
    pub(crate) session: SessionStatusInfo,
    pub(crate) services: Vec<ServiceStatusInfo>,
    pub(crate) admin: AdminStatusInfo,
    pub(crate) events: Vec<SubscriptionEventInfo>,
    pub(crate) effective_overflow_policy: String,
}

impl PySubscription {
    /// Wrap a live engine subscription stream, deferring native cleanup through `engine`.
    pub(crate) fn from_hosted_stream(
        stream: SubscriptionStream,
        batch_items: usize,
        engine: &Arc<EngineOwner>,
    ) -> Self {
        let (rx, handle) = stream.into_parts();
        let legacy_arrow = (!rx.requires_deliveries())
            .then(|| Arc::new(LegacySubscriptionArrow::new(batch_items)));
        let status = handle.status();
        let deliver_rows = handle.delivers_rows();
        let overflow_policy = handle.overflow_policy();
        Self {
            legacy_arrow,
            batch_items,
            status,
            deliver_rows,
            overflow_policy,
            consumer: Arc::new(SubscriptionOwner::hosted(
                SubscriptionConsumer::new(rx, handle),
                engine,
            )),
            engine_shutdown: engine.shutdown_receiver(),
            tick_dict_keys: Arc::default(),
            #[cfg(feature = "test-support")]
            _test_fixture: None,
            #[cfg(feature = "test-support")]
            _test_legacy_read_started: None,
        }
    }

    /// Wrap a synthetic fixture stream for `test-support` offline tests.
    #[cfg(feature = "test-support")]
    pub(crate) fn from_test_stream(
        stream: SubscriptionStream,
        batch_items: usize,
        fixture: Arc<xbbg_async::testing::SubscriptionFixture>,
        legacy_read_started: Arc<tokio::sync::Notify>,
    ) -> Self {
        let (rx, handle) = stream.into_parts();
        let legacy_arrow = (!rx.requires_deliveries())
            .then(|| Arc::new(LegacySubscriptionArrow::new(batch_items)));
        let status = handle.status();
        let deliver_rows = handle.delivers_rows();
        let overflow_policy = handle.overflow_policy();
        Self {
            legacy_arrow,
            batch_items,
            status,
            deliver_rows,
            overflow_policy,
            consumer: Arc::new(SubscriptionOwner::unhosted(SubscriptionConsumer::new(
                rx, handle,
            ))),
            engine_shutdown: watch::channel(false).1,
            tick_dict_keys: Arc::default(),
            _test_fixture: Some(fixture),
            _test_legacy_read_started: Some(legacy_read_started),
        }
    }

    pub(crate) fn snapshot_from_stream(&self) -> SubscriptionSnapshot {
        let control = self.consumer.handle();
        let handle = control.as_ref();
        let snapshot = self.status.load();
        let (
            messages_received,
            dropped_batches,
            batches_sent,
            slow_consumer,
            data_loss_events,
            last_message_us,
            last_data_loss_us,
        ) = subscription_metrics_totals(snapshot.fields_metrics());
        let mut topic_states: Vec<TopicStatusInfo> =
            snapshot.topic_statuses().values().cloned().collect();
        topic_states.sort_by(|left, right| left.topic.cmp(&right.topic));

        let mut services: Vec<ServiceStatusInfo> = snapshot.services().values().cloned().collect();
        services.sort_by(|left, right| left.service.cmp(&right.service));

        SubscriptionSnapshot {
            present: handle.is_some(),
            topics: snapshot.topics().to_vec(),
            fields: handle.map(SubscriptionHandle::fields).unwrap_or_default(),
            is_active: handle.is_some_and(SubscriptionHandle::is_active),
            all_failed: !snapshot.has_active_topics() && !snapshot.failures().is_empty(),
            messages_received,
            dropped_batches,
            batches_sent,
            slow_consumer,
            data_loss_events,
            last_message_us,
            last_data_loss_us,
            failures: snapshot.failures().to_vec(),
            topic_states,
            session: snapshot.session().clone(),
            services,
            admin: snapshot.admin().clone(),
            events: snapshot.events().iter().cloned().collect(),
            effective_overflow_policy: self.overflow_policy.to_string(),
        }
    }

    fn snapshot(&self, py: Python<'_>) -> SubscriptionSnapshot {
        py.detach(|| self.snapshot_from_stream())
    }

    fn ensure_rows(&self) -> PyResult<()> {
        if self.deliver_rows {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(
                "rows=False subscriptions cannot be iterated; use latest() instead",
            ))
        }
    }

    fn ensure_plain_rows(&self) -> PyResult<()> {
        self.ensure_rows()?;
        self.consumer
            .ensure_plain_delivery()
            .map_err(blp_error_to_pyerr)
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PySubscription {
    /// Async iterator protocol; image-only subscriptions must use latest().
    fn __aiter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.ensure_plain_rows()?;
        Ok(slf)
    }

    /// Get next batch of data.
    /// Legacy-only reads lock only rx; mixed readers also share the existing publication gate.
    ///
    /// Returns an xbbg ArrowRecordBatch on success.
    /// Raises a Python exception (BlpRequestError, BlpInternalError, etc.) on error.
    /// Raises StopAsyncIteration when the subscription is closed.
    /// Raises RuntimeError for rows=False; use latest() for image-only consumers.
    pub(crate) fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_plain_rows()?;
        let consumer = self.consumer.clone();
        let arrow = self
            .legacy_arrow
            .clone()
            .expect("plain Arrow iteration has a legacy batcher");
        let batch_items = self.batch_items;
        #[cfg(feature = "test-support")]
        let read_started = self._test_legacy_read_started.clone();

        shutdown_safe_future(py, async move {
            let mut close_rx = consumer.close_signal.subscribe();
            if *close_rx.borrow() {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            }
            #[cfg(feature = "test-support")]
            if let Some(signal) = read_started {
                signal.notify_one();
            }
            let (mut guard, publication) = consumer
                .lock_plain_receiver(&mut close_rx)
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
            let ready_batch = arrow
                .ready
                .lock()
                .expect("subscription Arrow output queue poisoned")
                .pop_front();
            if let Some(batch) = ready_batch {
                // Without a gate, rx itself owns the first-use publication boundary.
                // The owner is released last, after the guards it backs.
                let receiver = publication.is_none().then_some(guard);
                return Ok(Guarded::new(
                    native_arrow::ArrowRecordBatch::new(batch),
                    (receiver, publication, consumer),
                ));
            }
            let rx_ref = guard
                .as_mut()
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
            let read = receive_subscription_updates(
                rx_ref,
                consumer.pending.as_ref(),
                &mut close_rx,
                batch_items,
            )
            .await;
            match read {
                SubscriptionRead::Updates(updates) => {
                    let produced = {
                        let mut batcher = arrow
                            .batcher
                            .lock()
                            .expect("subscription Arrow batcher poisoned");
                        let mut produced = Vec::new();
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
                        let mut ready = arrow
                            .ready
                            .lock()
                            .expect("subscription Arrow output queue poisoned");
                        ready.extend(produced);
                        ready
                            .pop_front()
                            .expect("a non-empty update set must produce an Arrow batch")
                    };
                    // Retain rx through conversion unless the publication gate owns ordering.
                    let receiver = publication.is_none().then_some(guard);
                    Ok(Guarded::new(
                        native_arrow::ArrowRecordBatch::new(batch),
                        (receiver, publication, consumer),
                    ))
                }
                SubscriptionRead::Error(error) => Err(blp_error_to_pyerr(error)),
                SubscriptionRead::Ended => Err(PyStopAsyncIteration::new_err("subscription ended")),
                SubscriptionRead::Closed => {
                    Err(PyStopAsyncIteration::new_err("subscription closed"))
                }
            }
        })
    }

    /// Lease the next marked batch as an unpublished receipt. Python converts
    /// `receipt.value` on its event loop, then `publish()` commits the batch or
    /// `discard()` restores it. The receipt is built on a native completion
    /// thread; no backend conversion runs on the async bridge, and a read
    /// cancelled before completion leaves the batch unread.
    pub(crate) fn next_delivery<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_rows()?;
        let consumer = self.consumer.clone();
        let batch_items = self.batch_items;

        shutdown_safe_future(py, async move {
            let lease = consumer
                .next_delivery(batch_items)
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription ended"))?;
            Ok(DeliveryPayload::new(lease, consumer))
        })
    }

    /// Get next update as a Python dict without building Arrow.
    /// Raises RuntimeError for rows=False; use latest() for image-only consumers.
    pub(crate) fn __anext_tick_dict__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_plain_rows()?;
        let consumer = self.consumer.clone();
        let tick_keys = Arc::clone(&self.tick_dict_keys);
        #[cfg(feature = "test-support")]
        let read_started = self._test_legacy_read_started.clone();

        shutdown_safe_future(py, async move {
            let mut close_rx = consumer.close_signal.subscribe();
            #[cfg(feature = "test-support")]
            if let Some(signal) = read_started {
                signal.notify_one();
            }
            let (mut guard, publication) = consumer
                .lock_plain_receiver(&mut close_rx)
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
            let rx_ref = guard
                .as_mut()
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
            let read =
                receive_subscription_updates(rx_ref, consumer.pending.as_ref(), &mut close_rx, 1)
                    .await;

            match read {
                SubscriptionRead::Updates(mut updates) => {
                    let update = updates
                        .pop()
                        .expect("one-update read must contain an update");
                    // Retain rx through conversion unless the publication gate owns ordering.
                    let receiver = publication.is_none().then_some(guard);
                    Ok(Guarded::new(
                        TickDict::new(update, tick_keys),
                        (receiver, publication, consumer),
                    ))
                }
                SubscriptionRead::Error(error) => Err(blp_error_to_pyerr(error)),
                SubscriptionRead::Ended => Err(PyStopAsyncIteration::new_err("subscription ended")),
                SubscriptionRead::Closed => {
                    Err(PyStopAsyncIteration::new_err("subscription closed"))
                }
            }
        })
    }

    /// Add tickers to the subscription dynamically.
    /// Iteration can continue while Bloomberg work is in flight.
    #[pyo3(signature = (tickers, aliases=None))]
    fn add<'py>(
        &self,
        py: Python<'py>,
        tickers: Vec<String>,
        aliases: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let consumer = self.consumer.clone();
        let ops = self.consumer.operations.clone();
        let close_signal = self.consumer.close_signal.clone();

        debug!(tickers = ?tickers, "PySubscription: adding tickers");

        shutdown_safe_future(py, async move {
            let _op_guard = ops.lock().await;
            if *close_signal.subscribe().borrow() {
                return Err(PyRuntimeError::new_err("subscription closed"));
            }

            let handle = consumer
                .handle()
                .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;

            handle
                .add(tickers, aliases.unwrap_or_default().into_iter().collect())
                .await
                .map_err(subscription_control_error_to_pyerr)?;

            Ok(())
        })
    }

    /// Add fields to this consumer's projection and expand its upstream feeds.
    #[pyo3(signature = (fields))]
    fn add_fields<'py>(&self, py: Python<'py>, fields: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let consumer = self.consumer.clone();
        let ops = self.consumer.operations.clone();
        let close_signal = self.consumer.close_signal.clone();

        shutdown_safe_future(py, async move {
            let _op_guard = ops.lock().await;
            if *close_signal.subscribe().borrow() {
                return Err(PyRuntimeError::new_err("subscription closed"));
            }

            let handle = consumer
                .handle()
                .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;

            handle
                .add_fields(fields)
                .await
                .map_err(subscription_control_error_to_pyerr)?;
            Ok(())
        })
    }

    /// Remove tickers from the subscription dynamically.
    /// Iteration can continue while Bloomberg work is in flight.
    #[pyo3(signature = (tickers))]
    fn remove<'py>(&self, py: Python<'py>, tickers: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let consumer = self.consumer.clone();
        let ops = self.consumer.operations.clone();
        let close_signal = self.consumer.close_signal.clone();

        debug!(tickers = ?tickers, "PySubscription: removing tickers");

        shutdown_safe_future(py, async move {
            let _op_guard = ops.lock().await;
            if *close_signal.subscribe().borrow() {
                return Err(PyRuntimeError::new_err("subscription closed"));
            }

            let handle = consumer
                .handle()
                .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;

            handle
                .remove(tickers)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Ok(())
        })
    }

    /// Get the materialized latest values as an xbbg ArrowRecordBatch.
    /// Closed or terminated subscriptions raise instead of returning an empty batch.
    #[allow(clippy::result_large_err)]
    pub(crate) fn latest(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batch = py
            .detach(|| {
                if self.consumer.is_closed() {
                    return Err(BlpAsyncError::ChannelClosed);
                }
                self.consumer
                    .handle()
                    .ok_or(BlpAsyncError::ChannelClosed)?
                    .latest()
            })
            .map_err(subscription_control_error_to_pyerr)?;
        native_arrow::record_batch_to_arrow_record_batch(py, batch)
    }

    /// Drain new DelayedStream and FieldException warnings, preserving event history.
    ///
    /// Each row has at_us, category, level, message_type, topic, and detail keys.
    pub(crate) fn take_warnings(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let events = self.status.take_warnings();
        events
            .into_iter()
            .map(|event| {
                let row = PyDict::new(py);
                row.set_item("at_us", event.at_us)?;
                row.set_item("category", event.category.as_str())?;
                row.set_item("level", event.level.as_str())?;
                row.set_item("message_type", event.message_type)?;
                row.set_item("topic", event.topic)?;
                row.set_item("detail", event.detail)?;
                Ok(row.unbind())
            })
            .collect()
    }

    /// Get field rejection categories indexed by consumer label and field.
    #[getter]
    fn field_errors(&self) -> HashMap<String, HashMap<String, String>> {
        self.status.load().field_errors().clone()
    }

    /// Whether this handle delivers rows; False requires latest() instead of iteration.
    #[getter]
    fn delivers_rows(&self) -> bool {
        self.deliver_rows
    }

    /// Guard plain iteration without reproducing native effective-policy logic.
    fn ensure_plain_delivery(&self) -> PyResult<()> {
        self.ensure_plain_rows()
    }

    /// Delivery semantics remain available after this handle is closed.
    #[getter]
    fn delivery(&self) -> &'static str {
        self.consumer.delivery_mode().as_str()
    }

    /// Whether batch-envelope consumption is required by the effective mode.
    #[getter]
    fn requires_deliveries(&self) -> bool {
        self.consumer.requires_deliveries()
    }

    /// Get the currently subscribed tickers.
    #[getter]
    fn tickers(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).topics
    }

    /// Get the subscribed fields.
    #[getter]
    fn fields(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).fields
    }

    /// Check if the subscription is still active.
    #[getter]
    fn is_active(&self, py: Python<'_>) -> bool {
        !*self.consumer.close_signal.subscribe().borrow() && self.snapshot(py).is_active
    }

    #[getter]
    fn all_failed(&self, py: Python<'_>) -> bool {
        self.snapshot(py).all_failed
    }

    /// Get subscription metrics.
    ///
    /// Returns a dict with keys:
    /// - messages_received: int — total messages received from Bloomberg
    /// - dropped_batches: int — batches dropped due to overflow
    /// - batches_sent: int — batches successfully sent to Python
    /// - slow_consumer: bool — True if DATALOSS was received
    #[getter]
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("messages_received", snapshot.messages_received)?;
        dict.set_item("dropped_batches", snapshot.dropped_batches)?;
        dict.set_item("batches_sent", snapshot.batches_sent)?;
        dict.set_item("slow_consumer", snapshot.slow_consumer)?;
        dict.set_item("data_loss_events", snapshot.data_loss_events)?;
        dict.set_item("last_message_us", snapshot.last_message_us)?;
        dict.set_item("last_data_loss_us", snapshot.last_data_loss_us)?;
        dict.set_item(
            "effective_overflow_policy",
            snapshot.effective_overflow_policy,
        )?;
        Ok(dict.into())
    }

    #[getter]
    fn session_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("state", snapshot.session.state.as_str())?;
        dict.set_item("last_change_us", snapshot.session.last_change_us)?;
        dict.set_item("disconnect_count", snapshot.session.disconnect_count)?;
        dict.set_item("reconnect_count", snapshot.session.reconnect_count)?;
        Ok(dict.into())
    }

    #[getter]
    fn admin_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item(
            "slow_consumer_warning_active",
            snapshot.admin.slow_consumer_warning_active,
        )?;
        dict.set_item(
            "slow_consumer_warning_count",
            snapshot.admin.slow_consumer_warning_count,
        )?;
        dict.set_item(
            "slow_consumer_cleared_count",
            snapshot.admin.slow_consumer_cleared_count,
        )?;
        dict.set_item("data_loss_count", snapshot.admin.data_loss_count)?;
        dict.set_item("last_warning_us", snapshot.admin.last_warning_us)?;
        dict.set_item("last_cleared_us", snapshot.admin.last_cleared_us)?;
        dict.set_item("last_data_loss_us", snapshot.admin.last_data_loss_us)?;
        Ok(dict.into())
    }

    #[getter]
    fn service_status(&self, py: Python<'_>) -> Vec<(String, bool, i64)> {
        self.snapshot(py)
            .services
            .into_iter()
            .map(|service| (service.service, service.up, service.last_change_us))
            .collect()
    }

    #[getter]
    fn topic_states(&self, py: Python<'_>) -> Vec<SubscriptionTopicStateTuple> {
        self.snapshot(py)
            .topic_states
            .into_iter()
            .map(|topic| {
                (
                    topic.topic,
                    topic.state.as_str().to_string(),
                    topic.last_change_us,
                    topic.delayed,
                    topic.feed_topic,
                )
            })
            .collect()
    }

    /// Recent events, including DelayedStream and FieldException warnings.
    #[getter]
    pub(crate) fn events(&self, py: Python<'_>) -> Vec<SubscriptionEventTuple> {
        self.snapshot(py)
            .events
            .into_iter()
            .map(|event| {
                (
                    event.at_us,
                    event.category.as_str().to_string(),
                    event.level.as_str().to_string(),
                    event.message_type,
                    event.topic,
                    event.detail,
                )
            })
            .collect()
    }

    #[getter]
    fn failed_tickers(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py)
            .failures
            .into_iter()
            .map(|failure| failure.topic)
            .collect()
    }

    #[getter]
    fn failures(&self, py: Python<'_>) -> Vec<(String, String, String)> {
        self.snapshot(py)
            .failures
            .into_iter()
            .map(|failure| {
                (
                    failure.topic,
                    failure.reason,
                    failure.kind.as_str().to_string(),
                )
            })
            .collect()
    }

    /// Unsubscribe and close the stream.
    ///
    /// If drain=True, returns all unread values in the selected iteration mode.
    #[pyo3(signature = (drain = false, tick_mode = false))]
    pub(crate) fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        drain: bool,
        tick_mode: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let consumer = self.consumer.clone();
        let legacy_arrow = self.legacy_arrow.clone();
        let batch_items = self.batch_items;
        let engine_shutdown = self.engine_shutdown.clone();
        let tick_keys = Arc::clone(&self.tick_dict_keys);

        debug!(drain, tick_mode, "PySubscription: unsubscribing");

        shutdown_safe_future(py, async move {
            let (operation, remaining) = consumer.unsubscribe(drain, Some(&engine_shutdown)).await;
            if !drain {
                if let Some(arrow) = &legacy_arrow {
                    arrow
                        .ready
                        .lock()
                        .expect("subscription Arrow output queue poisoned")
                        .clear();
                }
                remaining.map_err(|error| blp_async_error_to_pyerr(*error))?;
                return Ok(Guarded::new(Unsubscribed::Closed, (operation, consumer)));
            }
            let remaining = match remaining {
                Ok(remaining) => remaining,
                Err(error) => {
                    if let Some(arrow) = &legacy_arrow {
                        arrow
                            .ready
                            .lock()
                            .expect("subscription Arrow output queue poisoned")
                            .clear();
                    }
                    return Err(blp_async_error_to_pyerr(*error));
                }
            };
            if tick_mode {
                if let Some(arrow) = &legacy_arrow {
                    arrow
                        .ready
                        .lock()
                        .expect("subscription Arrow output queue poisoned")
                        .clear();
                }
                return Ok(Guarded::new(
                    Unsubscribed::Ticks(remaining, tick_keys),
                    (operation, consumer),
                ));
            }
            let arrow = legacy_arrow.expect("plain Arrow drains have a legacy batcher");
            let queued_batches: Vec<RecordBatch> = arrow
                .ready
                .lock()
                .expect("subscription Arrow output queue poisoned")
                .drain(..)
                .collect();
            let remaining = {
                let mut batches = queued_batches;
                let mut batcher = arrow
                    .batcher
                    .lock()
                    .expect("subscription Arrow batcher poisoned");
                for update in remaining {
                    if let Some(batch) = batcher.append(&update) {
                        batches.push(batch);
                    }
                    if batcher.rows() == batch_items
                        && let Some(batch) = batcher.flush()
                    {
                        batches.push(batch);
                    }
                }
                if let Some(batch) = batcher.flush() {
                    batches.push(batch);
                }
                batches
            };
            Ok(Guarded::new(
                Unsubscribed::Batches(remaining),
                (operation, consumer),
            ))
        })
    }

    /// Close and optionally drain marked batches without losing gap metadata.
    /// A draining close returns an unpublished receipt like `next_delivery()`.
    #[pyo3(signature = (drain=false))]
    pub(crate) fn unsubscribe_deliveries<'py>(
        &self,
        py: Python<'py>,
        drain: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let consumer = self.consumer.clone();
        let legacy_arrow = self.legacy_arrow.clone();
        let engine_shutdown = self.engine_shutdown.clone();
        shutdown_safe_future(py, async move {
            if !drain {
                let (operation, remaining) =
                    consumer.unsubscribe(false, Some(&engine_shutdown)).await;
                if let Some(arrow) = legacy_arrow {
                    arrow
                        .ready
                        .lock()
                        .expect("subscription Arrow output queue poisoned")
                        .clear();
                }
                remaining.map_err(|error| blp_async_error_to_pyerr(*error))?;
                return Ok(Guarded::new(None, (Some(operation), consumer)));
            }
            let lease = consumer
                .prepare_unsubscribe_deliveries(true, Some(&engine_shutdown))
                .await;
            let owner = consumer.clone();
            Ok(Guarded::new(
                Some(DrainPayload::new(lease, owner)),
                (None, consumer),
            ))
        })
    }

    /// Context manager entry.
    fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    /// Context manager exit - unsubscribes automatically.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_val: Option<Bound<'py, PyAny>>,
        _exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.unsubscribe(py, false, false)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let snapshot = self.snapshot(py);
        if snapshot.present {
            format!(
                "Subscription(tickers={:?}, fields={:?}, active={})",
                snapshot.topics, snapshot.fields, snapshot.is_active
            )
        } else {
            "Subscription(closed)".to_string()
        }
    }
}
