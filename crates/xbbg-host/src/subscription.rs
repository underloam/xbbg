//! Host-neutral subscription facade over `xbbg_async::subscription_consumer`.
//!
//! [`HostSubscription`] is what the napi `JsSubscription` does without any host
//! types: row guards, plain scalar/Arrow reads with limits and the napi
//! `maxWaitMs` semantics, marked deliveries with two-phase publication,
//! drains in the three shapes (scalar batches, Arrow batches, deliveries),
//! membership controls, `latest`, warnings and status snapshots.
//!
//! # Two-phase publication
//!
//! A marked delivery read returns a [`DeliveryRead`]. The host calls
//! [`DeliveryRead::prepare`]; when close already won, the lease is dropped and
//! the source is restored for the draining owner. Otherwise the host converts
//! the moved-out Arrow data, then calls [`PreparedDelivery::publish`], which
//! re-checks close and registers a data-free receipt. The host's final
//! publication point settles it with [`HostSubscription::commit_delivery`]
//! (`false` when close won) or [`HostSubscription::rollback_delivery`].
//! Dropping anything before commit restores data, gaps and terminal errors.
//! Delivery drains follow the same rule through [`PreparedDrain`].
//!
//! Outstanding receipts are rolled back when a close starts, so a host that
//! loses a receipt can never wedge the read gate or the close.
//!
//! # Lifetime
//!
//! [`HostSubscription::keep_alive`] attaches an owner (an engine, or a test
//! fixture) that is dropped only after the consumer, so a subscription keeps
//! its producer alive. Engine shutdown is observed through the engine's
//! shutdown receiver: reads then fail with `Internal error: subscription engine
//! shut down`, and close cleanup errors caused by the shutdown are suppressed.

mod receive;
pub mod status;
#[cfg(all(test, feature = "test-support"))]
mod tests;

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio::sync::{OwnedMutexGuard, watch};
use xbbg_async::engine::state::{
    FieldLayout, SubscriptionArrowBatcher, SubscriptionReceiver, SubscriptionUpdate,
};
use xbbg_async::engine::{
    SharedSubscriptionStatus, SubscriptionEventInfo, SubscriptionStatusState,
};
use xbbg_async::subscription_consumer::{
    SubscriptionConsumer, SubscriptionDeliveryAck, SubscriptionDeliveryDrainAck,
    SubscriptionDeliveryLease, subscription_batch_capacity_hint, subscription_layouts_match,
};
use xbbg_async::{
    SubscriptionDelivery, SubscriptionDeliveryMode, SubscriptionGap, SubscriptionHandle,
    SubscriptionStream,
};

use crate::errors::HostError;

pub use receive::ReadOutcome;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One layout-homogeneous batch of scalar rows.
#[derive(Debug)]
pub struct ScalarBatch {
    /// The batch layout, present only when it differs from the last one sent.
    pub layout: Option<Arc<FieldLayout>>,
    pub layout_version: u32,
    pub updates: Vec<SubscriptionUpdate>,
}

/// A plain read result that keeps the receiver (or an established marked-read
/// guard) until the host finished converting it, so a first marked reader
/// cannot overtake rows that are still being published.
pub struct GuardedRead<T> {
    value: T,
    _receiver: Option<OwnedMutexGuard<Option<SubscriptionReceiver>>>,
    _publication: Option<OwnedMutexGuard<()>>,
}

impl<T> GuardedRead<T> {
    fn new(
        value: T,
        receiver: OwnedMutexGuard<Option<SubscriptionReceiver>>,
        publication: Option<OwnedMutexGuard<()>>,
    ) -> Self {
        let receiver = if publication.is_none() {
            Some(receiver)
        } else {
            drop(receiver);
            None
        };
        Self {
            value,
            _receiver: receiver,
            _publication: publication,
        }
    }

    /// The value to convert; the guards are released when this read drops.
    pub fn value(&self) -> &T {
        &self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for GuardedRead<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedRead")
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

struct ArrowBatching {
    limit: usize,
    batcher: SubscriptionArrowBatcher,
    ready: VecDeque<RecordBatch>,
}

impl ArrowBatching {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            batcher: SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(
                limit,
            )),
            ready: VecDeque::new(),
        }
    }

    /// Flush rows built for another limit before switching to `limit`.
    fn use_limit(&mut self, limit: usize, produced: &mut Vec<RecordBatch>) {
        if self.limit != limit {
            produced.extend(self.batcher.flush());
            self.limit = limit;
            self.batcher =
                SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(limit));
        }
    }
}

enum PendingDelivery {
    Data(SubscriptionDeliveryAck),
    Error(SubscriptionDeliveryLease),
}

#[derive(Default)]
struct Publications {
    next: u64,
    deliveries: HashMap<u64, PendingDelivery>,
    drains: HashMap<u64, SubscriptionDeliveryDrainAck>,
}

impl Publications {
    fn next_id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }
}

/// A marked delivery read awaiting host publication.
pub struct DeliveryRead {
    lease: SubscriptionDeliveryLease,
    close_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for DeliveryRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliveryRead").finish_non_exhaustive()
    }
}

/// The first publication phase of a marked delivery.
pub enum PreparedDelivery {
    /// Close won before conversion; the lease was dropped and the source restored.
    Closed,
    /// Data to convert, then [`PreparedDelivery::publish`].
    Data(DeliveryData),
    /// A terminal stream error; publishing registers a receipt whose commit
    /// returns the error exactly once.
    Error(DeliveryError),
}

/// Moved-out delivery data plus its data-free acknowledgement.
pub struct DeliveryData {
    pub kind: SubscriptionDeliveryMode,
    pub data: RecordBatch,
    pub gap_before: Option<SubscriptionGap>,
    ack: SubscriptionDeliveryAck,
    close_rx: watch::Receiver<bool>,
}

/// A terminal stream error awaiting publication.
pub struct DeliveryError {
    lease: SubscriptionDeliveryLease,
    close_rx: watch::Receiver<bool>,
}

impl DeliveryRead {
    /// Check close, then move the delivery out of the lease.
    pub fn prepare(self) -> PreparedDelivery {
        let Self { lease, close_rx } = self;
        if *close_rx.borrow() {
            return PreparedDelivery::Closed;
        }
        match lease.prepare_success() {
            Ok((delivery, ack)) => PreparedDelivery::Data(DeliveryData {
                kind: delivery.kind,
                data: delivery.data,
                gap_before: delivery.gap_before,
                ack,
                close_rx,
            }),
            Err(lease) => PreparedDelivery::Error(DeliveryError { lease, close_rx }),
        }
    }
}

impl DeliveryData {
    /// Register the acknowledgement after conversion; `None` (rolled back)
    /// when close won during conversion.
    pub fn publish(self, subscription: &HostSubscription) -> Option<u64> {
        subscription.register(PendingDelivery::Data(self.ack), &self.close_rx)
    }
}

impl DeliveryError {
    /// Register the terminal error; `None` (restored) when close won.
    pub fn publish(self, subscription: &HostSubscription) -> Option<u64> {
        subscription.register(PendingDelivery::Error(self.lease), &self.close_rx)
    }
}

/// A frozen delivery drain awaiting host publication.
pub struct PreparedDrain {
    pub deliveries: Vec<SubscriptionDelivery>,
    ack: Option<SubscriptionDeliveryDrainAck>,
}

impl PreparedDrain {
    /// Register the whole-drain acknowledgement after every delivery was
    /// converted. `None` when there was nothing to acknowledge.
    pub fn publish(mut self, subscription: &HostSubscription) -> Option<u64> {
        let ack = self.ack.take()?;
        let mut publications = lock(&subscription.publications);
        let id = publications.next_id();
        publications.drains.insert(id, ack);
        Some(id)
    }
}

/// Test-support hook that parks the next marked delivery after its lease is
/// taken and before any host conversion.
#[cfg(feature = "test-support")]
#[derive(Default)]
pub struct PublicationPause {
    ready: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
impl PublicationPause {
    /// Resolve once a read is parked (immediately if one already is).
    pub async fn ready(&self) {
        self.ready.notified().await;
    }

    /// Let the parked read continue (or let the next one pass without parking).
    pub fn release(&self) {
        self.release.notify_one();
    }

    async fn hold(&self) {
        self.ready.notify_one();
        self.release.notified().await;
    }
}

/// One consumer of a subscription stream (see the module docs).
pub struct HostSubscription {
    consumer: SubscriptionConsumer,
    status: SharedSubscriptionStatus,
    deliver_rows: bool,
    batch_items: usize,
    engine_shutdown: Option<watch::Receiver<bool>>,
    scalar_layout: Mutex<Option<Arc<FieldLayout>>>,
    arrow: Mutex<ArrowBatching>,
    publications: Mutex<Publications>,
    #[cfg(feature = "test-support")]
    publication_pause: Mutex<Option<Arc<PublicationPause>>>,
    // Declared last: the owner outlives the consumer.
    keep_alive: Option<Box<dyn Any + Send + Sync>>,
}

fn read_limit(value: Option<usize>, label: &str, default: usize) -> Result<usize, HostError> {
    match value {
        Some(0) => Err(HostError::argument(format!(
            "{label} must be greater than zero"
        ))),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

fn rows_error() -> HostError {
    HostError::validation(
        "image-only subscription (rows: false) does not deliver rows; use latest() instead",
    )
    .with_element("rows")
}

impl HostSubscription {
    /// Wrap a new stream. `batch_items` is the default read and drain batch
    /// size (`flushThreshold` or the engine flush threshold);
    /// `engine_shutdown` is the producing engine's shutdown receiver.
    pub fn from_stream(
        stream: SubscriptionStream,
        batch_items: usize,
        engine_shutdown: Option<watch::Receiver<bool>>,
    ) -> Self {
        let (rx, handle) = stream.into_parts();
        let status = handle.status();
        let deliver_rows = handle.delivers_rows();
        let batch_items = batch_items.max(1);
        Self {
            consumer: SubscriptionConsumer::new(rx, handle),
            status,
            deliver_rows,
            batch_items,
            engine_shutdown,
            scalar_layout: Mutex::new(None),
            arrow: Mutex::new(ArrowBatching::new(batch_items)),
            publications: Mutex::new(Publications::default()),
            #[cfg(feature = "test-support")]
            publication_pause: Mutex::new(None),
            keep_alive: None,
        }
    }

    /// Keep `owner` alive until this subscription (and its consumer) is dropped.
    pub fn keep_alive(mut self, owner: Box<dyn Any + Send + Sync>) -> Self {
        self.keep_alive = Some(owner);
        self
    }

    /// Default read and drain batch size.
    pub fn batch_items(&self) -> usize {
        self.batch_items
    }

    /// Stop publication of pending native completions; the following close
    /// still owns detach, drain and cleanup.
    pub fn signal_closing(&self) {
        self.consumer.close_signal.send_replace(true);
    }

    /// Whether close has been signalled or completed.
    pub fn is_closed(&self) -> bool {
        self.consumer.is_closed()
    }

    fn require_delivery_rows(&self) -> Result<(), HostError> {
        if self.deliver_rows {
            Ok(())
        } else {
            Err(rows_error())
        }
    }

    fn require_rows(&self) -> Result<(), HostError> {
        self.require_delivery_rows()?;
        self.consumer
            .ensure_plain_delivery()
            .map_err(HostError::from_blp)
    }

    fn scalar_batch(&self, updates: Vec<SubscriptionUpdate>) -> Option<ScalarBatch> {
        let first = updates.first()?;
        let layout_version = first.layout.version;
        let mut last = lock(&self.scalar_layout);
        let layout = if last
            .as_ref()
            .is_some_and(|sent| subscription_layouts_match(sent, &first.layout))
        {
            None
        } else {
            *last = Some(Arc::clone(&first.layout));
            Some(Arc::clone(&first.layout))
        };
        Some(ScalarBatch {
            layout,
            layout_version,
            updates,
        })
    }

    /// Next scalar rows (napi `nextUpdates`): `max_items` defaults to the
    /// batch size; with `max_wait` the read fills until the limit or deadline.
    pub async fn next_updates(
        &self,
        max_items: Option<usize>,
        max_wait: Option<Duration>,
    ) -> Result<ReadOutcome<GuardedRead<ScalarBatch>>, HostError> {
        self.require_rows()?;
        if self.consumer.is_closed() {
            return Ok(ReadOutcome::Ended);
        }
        let limit = read_limit(max_items, "maxItems", self.batch_items)?;
        let mut close_rx = self.consumer.close_signal.subscribe();
        let Some((mut receiver, publication)) =
            self.consumer.lock_plain_receiver(&mut close_rx).await
        else {
            return Ok(ReadOutcome::Ended);
        };
        let Some(rx) = receiver.as_mut() else {
            return Ok(ReadOutcome::Ended);
        };
        let outcome = receive::receive_updates(
            rx,
            self.consumer.pending.as_ref(),
            &mut close_rx,
            limit,
            max_wait,
        )
        .await
        .map_err(|error| HostError::from_blp(*error))?;
        Ok(match outcome {
            ReadOutcome::Ready(updates) => match self.scalar_batch(updates) {
                Some(batch) => ReadOutcome::Ready(GuardedRead::new(batch, receiver, publication)),
                None => ReadOutcome::Ended,
            },
            ReadOutcome::Ended => ReadOutcome::Ended,
            ReadOutcome::TimedOut => ReadOutcome::TimedOut,
        })
    }

    /// Next Arrow rows (napi `nextArrowBatch`); one batch per layout run.
    pub async fn next_arrow_batch(
        &self,
        max_rows: Option<usize>,
        max_wait: Option<Duration>,
    ) -> Result<ReadOutcome<GuardedRead<RecordBatch>>, HostError> {
        self.require_rows()?;
        if self.consumer.is_closed() {
            return Ok(ReadOutcome::Ended);
        }
        let limit = read_limit(max_rows, "maxRows", self.batch_items)?;
        let mut close_rx = self.consumer.close_signal.subscribe();
        let Some((mut receiver, publication)) =
            self.consumer.lock_plain_receiver(&mut close_rx).await
        else {
            return Ok(ReadOutcome::Ended);
        };
        if let Some(batch) = lock(&self.arrow).ready.pop_front() {
            return Ok(ReadOutcome::Ready(GuardedRead::new(
                batch,
                receiver,
                publication,
            )));
        }
        let Some(rx) = receiver.as_mut() else {
            return Ok(ReadOutcome::Ended);
        };
        let outcome = receive::receive_updates(
            rx,
            self.consumer.pending.as_ref(),
            &mut close_rx,
            limit,
            max_wait,
        )
        .await
        .map_err(|error| HostError::from_blp(*error))?;
        let updates = match outcome {
            ReadOutcome::Ready(updates) => updates,
            ReadOutcome::Ended => return Ok(ReadOutcome::Ended),
            ReadOutcome::TimedOut => return Ok(ReadOutcome::TimedOut),
        };
        let batch = {
            let mut arrow = lock(&self.arrow);
            let mut produced = Vec::new();
            arrow.use_limit(limit, &mut produced);
            for update in &updates {
                produced.extend(arrow.batcher.append(update));
            }
            produced.extend(arrow.batcher.flush());
            arrow.ready.extend(produced);
            arrow.ready.pop_front()
        };
        Ok(match batch {
            Some(batch) => ReadOutcome::Ready(GuardedRead::new(batch, receiver, publication)),
            None => ReadOutcome::Ended,
        })
    }

    /// Next marked delivery (napi `nextDelivery`). With `max_wait` an expired
    /// wait consumes nothing.
    pub async fn next_delivery(
        &self,
        max_rows: Option<usize>,
        max_wait: Option<Duration>,
    ) -> Result<ReadOutcome<DeliveryRead>, HostError> {
        self.require_delivery_rows()?;
        let limit = read_limit(max_rows, "maxRows", self.batch_items)?;
        let close_rx = self.consumer.close_signal.subscribe();
        let read = self.consumer.next_delivery(limit);
        let lease = match max_wait {
            Some(wait) => match tokio::time::timeout(wait, read).await {
                Ok(lease) => lease,
                Err(_) => return Ok(ReadOutcome::TimedOut),
            },
            None => read.await,
        };
        let Some(lease) = lease else {
            return Ok(ReadOutcome::Ended);
        };
        #[cfg(feature = "test-support")]
        {
            let pause = lock(&self.publication_pause).take();
            if let Some(pause) = pause {
                pause.hold().await;
            }
        }
        Ok(ReadOutcome::Ready(DeliveryRead { lease, close_rx }))
    }

    fn register(&self, pending: PendingDelivery, close_rx: &watch::Receiver<bool>) -> Option<u64> {
        let mut publications = lock(&self.publications);
        // Checked under the table lock: a close that starts afterwards drains
        // the table, so a registered receipt can never be stranded.
        if *close_rx.borrow() {
            return None;
        }
        let id = publications.next_id();
        publications.deliveries.insert(id, pending);
        Some(id)
    }

    /// Acknowledge a published delivery. `Ok(false)` when close won (the
    /// receipt was rolled back); an error receipt returns its terminal error once.
    pub fn commit_delivery(&self, receipt: u64) -> Result<bool, HostError> {
        let Some(pending) = lock(&self.publications).deliveries.remove(&receipt) else {
            return Ok(false);
        };
        if self.consumer.is_closed() {
            return Ok(false);
        }
        match pending {
            PendingDelivery::Data(ack) => {
                ack.commit();
                Ok(true)
            }
            PendingDelivery::Error(lease) => match lease.commit() {
                Err(error) => Err(HostError::from_blp(error)),
                Ok(_) => Err(HostError::internal(
                    "subscription error receipt committed without an error",
                )),
            },
        }
    }

    /// Roll back a published delivery: its data, gap or error is restored.
    pub fn rollback_delivery(&self, receipt: u64) {
        let pending = lock(&self.publications).deliveries.remove(&receipt);
        drop(pending);
    }

    /// Acknowledge a published drain. `false` when it was already settled.
    pub fn commit_drain(&self, receipt: u64) -> bool {
        let Some(ack) = lock(&self.publications).drains.remove(&receipt) else {
            return false;
        };
        ack.commit();
        true
    }

    /// Roll back a published drain; the frozen source stays available for a retry.
    pub fn rollback_drain(&self, receipt: u64) {
        let ack = lock(&self.publications).drains.remove(&receipt);
        drop(ack);
    }

    /// Roll back every outstanding receipt (restoring their sources).
    fn settle_publications(&self) {
        let (deliveries, drains) = {
            let mut publications = lock(&self.publications);
            (
                std::mem::take(&mut publications.deliveries),
                std::mem::take(&mut publications.drains),
            )
        };
        drop(deliveries);
        drop(drains);
    }

    fn begin_close(&self) {
        self.signal_closing();
        self.settle_publications();
    }

    async fn guard_plain_drain(&self, drain: bool) -> Result<(), HostError> {
        if drain && let Err(error) = self.consumer.ensure_plain_delivery() {
            let (_operation, cleanup) = self
                .consumer
                .unsubscribe_deliveries(true, self.engine_shutdown.as_ref())
                .await;
            lock(&self.arrow).ready.clear();
            cleanup.map_err(|error| HostError::from_async(*error))?;
            return Err(HostError::from_blp(error));
        }
        Ok(())
    }

    /// Close and return the unread rows as scalar batches of at most the batch
    /// size, split at layout changes (napi `unsubscribe`).
    pub async fn unsubscribe_scalar(&self, drain: bool) -> Result<Vec<ScalarBatch>, HostError> {
        self.begin_close();
        self.guard_plain_drain(drain).await?;
        let (_operation, drained) = self
            .consumer
            .unsubscribe(drain, self.engine_shutdown.as_ref())
            .await;
        lock(&self.arrow).ready.clear();
        let drained = drained.map_err(|error| HostError::from_async(*error))?;
        let mut batches = Vec::new();
        let mut current: Vec<SubscriptionUpdate> = Vec::new();
        for update in drained {
            let boundary = current.first().is_some_and(|first| {
                current.len() == self.batch_items
                    || !subscription_layouts_match(&first.layout, &update.layout)
            });
            if boundary {
                batches.extend(self.scalar_batch(std::mem::take(&mut current)));
            }
            current.push(update);
        }
        batches.extend(self.scalar_batch(current));
        Ok(batches)
    }

    /// Close and return queued Arrow batches, then the unread rows batched by
    /// the batch size (napi `unsubscribeArrow`).
    pub async fn unsubscribe_arrow(&self, drain: bool) -> Result<Vec<RecordBatch>, HostError> {
        self.begin_close();
        self.guard_plain_drain(drain).await?;
        let (_operation, drained) = self
            .consumer
            .unsubscribe(drain, self.engine_shutdown.as_ref())
            .await;
        let queued: Vec<RecordBatch> = {
            let mut arrow = lock(&self.arrow);
            if drain {
                arrow.ready.drain(..).collect()
            } else {
                arrow.ready.clear();
                Vec::new()
            }
        };
        let drained = drained.map_err(|error| HostError::from_async(*error))?;
        let mut batches = queued;
        if !drained.is_empty() {
            let mut arrow = lock(&self.arrow);
            arrow.use_limit(self.batch_items, &mut batches);
            for update in &drained {
                batches.extend(arrow.batcher.append(update));
                if arrow.batcher.rows() == self.batch_items {
                    batches.extend(arrow.batcher.flush());
                }
            }
            batches.extend(arrow.batcher.flush());
        }
        Ok(batches)
    }

    /// Close and freeze the marked drain (napi `unsubscribeDeliveries`). A
    /// terminal error is consumed and returned; an empty drain is acknowledged.
    pub async fn unsubscribe_deliveries(&self, drain: bool) -> Result<PreparedDrain, HostError> {
        self.begin_close();
        let lease = self
            .consumer
            .prepare_unsubscribe_deliveries(drain, self.engine_shutdown.as_ref())
            .await;
        lock(&self.arrow).ready.clear();
        match lease.prepare_success() {
            Ok((deliveries, ack)) if deliveries.is_empty() => {
                ack.commit();
                Ok(PreparedDrain {
                    deliveries,
                    ack: None,
                })
            }
            Ok((deliveries, ack)) => Ok(PreparedDrain {
                deliveries,
                ack: Some(ack),
            }),
            Err(lease) => match lease.commit() {
                Err(error) => Err(HostError::from_async(*error)),
                Ok(_) => Err(HostError::internal(
                    "subscription drain failure committed without an error",
                )),
            },
        }
    }

    fn open_handle(&self) -> Result<SubscriptionHandle, HostError> {
        if self.consumer.is_closed() {
            return Err(HostError::subscription_closed());
        }
        self.consumer
            .handle()
            .ok_or_else(HostError::subscription_closed)
    }

    /// Add upstream topics with optional `topic -> label` aliases.
    pub async fn add(
        &self,
        topics: Vec<String>,
        aliases: Vec<(String, String)>,
    ) -> Result<(), HostError> {
        let _mutation = self.consumer.operations.lock().await;
        let handle = self.open_handle()?;
        handle
            .add(topics, aliases)
            .await
            .map_err(HostError::from_subscription_control)
    }

    /// Grow this consumer's projection and the shared feed's field union.
    pub async fn add_fields(&self, fields: Vec<String>) -> Result<(), HostError> {
        let _mutation = self.consumer.operations.lock().await;
        let handle = self.open_handle()?;
        handle
            .add_fields(fields)
            .await
            .map_err(HostError::from_subscription_control)
    }

    /// Remove consumer labels.
    pub async fn remove(&self, labels: Vec<String>) -> Result<(), HostError> {
        let _mutation = self.consumer.operations.lock().await;
        let handle = self.open_handle()?;
        handle
            .remove(labels)
            .await
            .map_err(HostError::from_subscription_control)
    }

    /// Materialized latest images of the active labels; no read mode involved.
    pub fn latest(&self) -> Result<RecordBatch, HostError> {
        self.open_handle()?
            .latest()
            .map_err(HostError::from_subscription_control)
    }

    /// New delayed-data and rejected-field events since the last call.
    pub fn take_warnings(&self) -> Vec<SubscriptionEventInfo> {
        self.status.take_warnings()
    }

    /// One consistent status snapshot.
    pub fn status_snapshot(&self) -> Arc<SubscriptionStatusState> {
        let snapshot = self.status.load();
        Arc::clone(&snapshot)
    }

    /// Consumer labels.
    pub fn tickers(&self) -> Vec<String> {
        self.status.load().topics().to_vec()
    }

    /// The live field projection (empty after close).
    pub fn fields(&self) -> Vec<String> {
        self.consumer
            .handle()
            .as_ref()
            .map(SubscriptionHandle::fields)
            .unwrap_or_default()
    }

    /// Whether this consumer delivers rows (`rows: true`); unchanged by close.
    pub fn delivers_rows(&self) -> bool {
        self.deliver_rows
    }

    /// Deltas or state; unchanged by close.
    pub fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        self.consumer.delivery_mode()
    }

    /// Whether plain reads are refused (state and drop_oldest consumers).
    pub fn requires_deliveries(&self) -> bool {
        self.consumer.requires_deliveries()
    }

    /// Open, streaming, and subscribed to at least one active topic.
    pub fn is_active(&self) -> bool {
        !self.consumer.is_closed()
            && self
                .consumer
                .handle()
                .as_ref()
                .is_some_and(SubscriptionHandle::is_active)
    }

    /// Arm a one-shot pause for the next marked delivery.
    #[cfg(feature = "test-support")]
    pub fn arm_publication_pause(&self, pause: Arc<PublicationPause>) -> Result<(), HostError> {
        let mut slot = lock(&self.publication_pause);
        if slot.is_some() {
            return Err(HostError::validation("a delivery pause is already armed"));
        }
        *slot = Some(pause);
        Ok(())
    }
}

impl Drop for HostSubscription {
    fn drop(&mut self) {
        // Receipts hold read and operation guards of the consumer; restore
        // their sources before the consumer, then the owner, drop.
        self.settle_publications();
    }
}
