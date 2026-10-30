//! Consumer-side subscription ordering and lifecycle shared by language adapters.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::sync::{Mutex, OwnedMutexGuard, watch};
use xbbg_core::BlpError;

use crate::engine::state::{
    FieldLayout, StateDeliveryReceipt, SubscriptionArrowBatcher, SubscriptionEvent,
    SubscriptionReceiver, SubscriptionUpdate, plain_delivery_error,
};
use crate::{BlpAsyncError, SubscriptionDelivery, SubscriptionDeliveryMode, SubscriptionHandle};

pub type StreamItem = Result<SubscriptionUpdate, BlpError>;

pub fn subscription_batch_capacity_hint(limit: usize) -> usize {
    limit.clamp(1, 4096)
}

pub fn subscription_layouts_match(current: &Arc<FieldLayout>, next: &Arc<FieldLayout>) -> bool {
    Arc::ptr_eq(current, next)
        || (current.version == next.version
            && current.fields.len() == next.fields.len()
            && current
                .fields
                .iter()
                .zip(next.fields.iter())
                .all(|(left, right)| {
                    left.index == right.index && left.kind == right.kind && left.name == right.name
                }))
}

pub async fn wait_for_subscription_close(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }
    while close_rx.changed().await.is_ok() {
        if *close_rx.borrow() {
            return;
        }
    }
}

/// Ordered unread items, including errors and updates deferred at a layout boundary.
#[derive(Default)]
pub struct PendingUpdates(StdMutex<VecDeque<SubscriptionEvent>>);

impl PendingUpdates {
    pub fn pop_front(&self) -> Option<StreamItem> {
        let mut pending = self.0.lock().expect("subscription pending queue poisoned");
        if matches!(
            pending.front(),
            Some(SubscriptionEvent::Update {
                gap_before: Some(_),
                ..
            })
        ) {
            return Some(Err(plain_delivery_error()));
        }
        pending.pop_front().map(SubscriptionEvent::into_legacy)
    }

    fn pop_event(&self) -> Option<SubscriptionEvent> {
        self.0
            .lock()
            .expect("subscription pending queue poisoned")
            .pop_front()
    }

    fn push_front(&self, event: SubscriptionEvent) {
        self.0
            .lock()
            .expect("subscription pending queue poisoned")
            .push_front(event);
    }

    fn restore_events(&self, events: impl DoubleEndedIterator<Item = SubscriptionEvent>) {
        let mut pending = self.0.lock().expect("subscription pending queue poisoned");
        for event in events.rev() {
            pending.push_front(event);
        }
    }

    /// Append an item to a layout-limited batch, deferring its boundary/error.
    /// Returns false when the current batch must be delivered before this item.
    pub fn append_to_batch(
        &self,
        updates: &mut Vec<SubscriptionUpdate>,
        item: StreamItem,
    ) -> Result<bool, BlpError> {
        match item {
            Ok(update)
                if updates.first().is_none_or(|first| {
                    subscription_layouts_match(&first.layout, &update.layout)
                }) =>
            {
                updates.push(update);
                Ok(true)
            }
            Err(error) if updates.is_empty() => Err(error),
            item => {
                self.0
                    .lock()
                    .expect("subscription pending queue poisoned")
                    .push_front(item.into());
                Ok(false)
            }
        }
    }

    /// Restore an adapter's uncommitted batch after a deadline-aware read is cancelled.
    pub fn restore(&self, updates: impl DoubleEndedIterator<Item = SubscriptionUpdate>) {
        self.restore_events(updates.map(|update| SubscriptionEvent::from(Ok(update))));
    }

    pub fn collect_unread(
        &self,
        mut rx: Option<SubscriptionReceiver>,
        drain: bool,
    ) -> Result<Vec<SubscriptionUpdate>, Box<BlpError>> {
        let mut pending = self.0.lock().expect("subscription pending queue poisoned");
        if !drain {
            pending.clear();
            return Ok(Vec::new());
        }
        let mut updates = Vec::new();
        let mut first_error = None;
        let mut advanced = rx
            .as_ref()
            .is_some_and(SubscriptionReceiver::requires_deliveries);
        let mut collect = |item| match item {
            SubscriptionEvent::Update {
                update,
                gap_before: None,
            } => updates.push(update),
            SubscriptionEvent::Update {
                gap_before: Some(_),
                ..
            } => advanced = true,
            SubscriptionEvent::Error(error) if first_error.is_none() => first_error = Some(error),
            _ => {}
        };
        while let Some(item) = pending.pop_front() {
            collect(item);
        }
        drop(pending);
        if let Some(rx) = rx.as_mut() {
            while let Ok(item) = rx.try_recv_event() {
                collect(item);
            }
        }
        match first_error {
            Some(error) => Err(Box::new(error)),
            None if advanced => Err(Box::new(plain_delivery_error())),
            None => Ok(updates),
        }
    }

    async fn drain_forwarder(
        &self,
        barrier: impl Future<Output = Result<(), BlpAsyncError>>,
        rx: &mut SubscriptionReceiver,
    ) -> Result<(), BlpAsyncError> {
        tokio::pin!(barrier);
        let result = loop {
            tokio::select! {
                biased;
                item = rx.recv_event() => match item {
                    Some(item) => self.0.lock().expect("subscription pending queue poisoned").push_back(item),
                    None => break barrier.await,
                },
                result = &mut barrier => break result,
            }
        };
        while let Ok(item) = rx.try_recv_event() {
            self.0
                .lock()
                .expect("subscription pending queue poisoned")
                .push_back(item);
        }
        result
    }
}

pub enum SubscriptionRead {
    Updates(Vec<SubscriptionUpdate>),
    Error(BlpError),
    Ended,
    Closed,
}

/// Wait for the first update, then consume only immediately available same-layout rows.
/// Deadline-aware adapters use the same pending queue and batch boundary operation.
/// When sharing a consumer with marked reads, acquire its existing publication
/// guard before borrowing the receiver and retain it through publication.
pub async fn receive_subscription_updates(
    rx: &mut SubscriptionReceiver,
    pending: &PendingUpdates,
    close_rx: &mut watch::Receiver<bool>,
    limit: usize,
) -> SubscriptionRead {
    if let Err(error) = rx.ensure_plain_delivery() {
        return SubscriptionRead::Error(error);
    }
    if *close_rx.borrow() {
        return SubscriptionRead::Closed;
    }
    let first = match pending.pop_front() {
        Some(item) => Some(item),
        None => tokio::select! {
            biased;
            _ = wait_for_subscription_close(close_rx) => return SubscriptionRead::Closed,
            item = rx.recv() => item,
        },
    };
    let Some(first) = first else {
        return SubscriptionRead::Ended;
    };
    let first = match first {
        Ok(update) => update,
        Err(error) => return SubscriptionRead::Error(error),
    };
    let mut updates = Vec::with_capacity(subscription_batch_capacity_hint(limit));
    updates.push(first);
    while updates.len() < limit {
        let Some(item) = pending.pop_front().or_else(|| rx.try_recv().ok()) else {
            break;
        };
        match pending.append_to_batch(&mut updates, item) {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => return SubscriptionRead::Error(error),
        }
    }
    SubscriptionRead::Updates(updates)
}

struct RawDeliveryReceipt {
    value: Option<Result<SubscriptionDelivery, BlpError>>,
    events: Vec<SubscriptionEvent>,
    pending: Arc<PendingUpdates>,
}

impl RawDeliveryReceipt {
    fn as_ref(&self) -> Result<&SubscriptionDelivery, &BlpError> {
        self.value
            .as_ref()
            .expect("uncommitted subscription delivery")
            .as_ref()
    }

    fn commit(mut self) -> Result<SubscriptionDelivery, BlpError> {
        self.events.clear();
        self.value
            .take()
            .expect("uncommitted subscription delivery")
    }

    fn take_success(&mut self) -> Option<SubscriptionDelivery> {
        match self.value.take()? {
            Ok(delivery) => Some(delivery),
            Err(error) => {
                self.value = Some(Err(error));
                None
            }
        }
    }

    fn take_error(&mut self) -> Option<BlpError> {
        match self.value.take()? {
            Err(error) => Some(error),
            Ok(delivery) => {
                self.value = Some(Ok(delivery));
                None
            }
        }
    }

    fn restore_error(&mut self, error: BlpError) {
        self.value = Some(Err(error));
    }

    fn acknowledge(mut self) {
        self.events.clear();
        drop(self.value.take());
    }
}

impl Drop for RawDeliveryReceipt {
    fn drop(&mut self) {
        if let Some(Err(error)) = self.value.take() {
            self.pending.push_front(SubscriptionEvent::Error(error));
        }
        self.pending.restore_events(self.events.drain(..));
    }
}

enum DeliveryReceipt {
    Raw(RawDeliveryReceipt),
    State(StateDeliveryReceipt),
}

impl DeliveryReceipt {
    fn take_success(&mut self) -> Option<SubscriptionDelivery> {
        match self {
            Self::Raw(receipt) => receipt.take_success(),
            Self::State(receipt) => receipt.take_success(),
        }
    }

    fn take_error(&mut self) -> Option<BlpError> {
        match self {
            Self::Raw(receipt) => receipt.take_error(),
            Self::State(receipt) => receipt.take_error(),
        }
    }

    fn restore_error(&mut self, error: BlpError) {
        match self {
            Self::Raw(receipt) => receipt.restore_error(error),
            Self::State(receipt) => receipt.restore_error(error),
        }
    }

    fn acknowledge(self) {
        match self {
            Self::Raw(receipt) => receipt.acknowledge(),
            Self::State(receipt) => receipt.acknowledge(),
        }
    }
}

/// Transactional delivery: adapters convert the borrowed batch before committing
/// at their publication point. Dropping a lease restores continuity and data.
pub struct SubscriptionDeliveryLease {
    receipt: Option<DeliveryReceipt>,
    _reader: OwnedMutexGuard<()>,
}

impl SubscriptionDeliveryLease {
    pub fn as_ref(&self) -> Result<&SubscriptionDelivery, &BlpError> {
        match self
            .receipt
            .as_ref()
            .expect("uncommitted subscription delivery")
        {
            DeliveryReceipt::Raw(receipt) => receipt.as_ref(),
            DeliveryReceipt::State(receipt) => receipt.as_ref(),
        }
    }

    pub fn commit(mut self) -> Result<SubscriptionDelivery, BlpError> {
        match self
            .receipt
            .take()
            .expect("uncommitted subscription delivery")
        {
            DeliveryReceipt::Raw(receipt) => receipt.commit(),
            DeliveryReceipt::State(receipt) => receipt.commit(),
        }
    }

    /// Move uniquely owned Arrow buffers into an exporter without acknowledging
    /// the source. Terminal errors retain the original lease for error publication.
    pub fn prepare_success(
        mut self,
    ) -> Result<(SubscriptionDelivery, SubscriptionDeliveryAck), Self> {
        let delivery = match self
            .receipt
            .as_mut()
            .expect("uncommitted subscription delivery")
        {
            DeliveryReceipt::Raw(receipt) => receipt.take_success(),
            DeliveryReceipt::State(receipt) => receipt.take_success(),
        };
        let Some(delivery) = delivery else {
            return Err(self);
        };
        let Self { receipt, _reader } = self;
        Ok((
            delivery,
            SubscriptionDeliveryAck {
                receipt: receipt.expect("uncommitted subscription delivery"),
                _reader,
            },
        ))
    }
}

/// Data-free publication receipt. Dropping it restores native updates/gaps or
/// re-dirties state topics; it never retains the exported Arrow buffers.
pub struct SubscriptionDeliveryAck {
    receipt: DeliveryReceipt,
    _reader: OwnedMutexGuard<()>,
}

impl SubscriptionDeliveryAck {
    pub fn commit(self) {
        match self.receipt {
            DeliveryReceipt::Raw(receipt) => receipt.acknowledge(),
            DeliveryReceipt::State(receipt) => receipt.acknowledge(),
        }
    }
}

struct ClosedDeliverySource {
    receiver: Option<SubscriptionReceiver>,
    _handle: Option<SubscriptionHandle>,
    cleanup_error: Option<Box<BlpAsyncError>>,
}

#[derive(Default)]
struct DeliveryDrainRecovery {
    source: Option<ClosedDeliverySource>,
    cleanup_error: Option<Box<BlpAsyncError>>,
}

enum DrainErrorOrigin {
    Stream(usize),
    Cleanup,
}

struct DrainFailure {
    error: Box<BlpAsyncError>,
    origin: DrainErrorOrigin,
}

struct DeliveryDrainTransaction {
    source: Option<ClosedDeliverySource>,
    recovery: Arc<StdMutex<DeliveryDrainRecovery>>,
    receipts: Vec<DeliveryReceipt>,
    deliveries: Vec<SubscriptionDelivery>,
    failure: Option<DrainFailure>,
}

impl DeliveryDrainTransaction {
    fn push(&mut self, mut receipt: DeliveryReceipt) {
        if let Some(delivery) = receipt.take_success() {
            self.deliveries.push(delivery);
        } else if self.failure.is_none()
            && let Some(error) = receipt.take_error()
        {
            self.failure = Some(DrainFailure {
                error: Box::new(BlpAsyncError::Blp(error)),
                origin: DrainErrorOrigin::Stream(self.receipts.len()),
            });
        }
        self.receipts.push(receipt);
    }

    fn acknowledge(&mut self) {
        for receipt in self.receipts.drain(..) {
            receipt.acknowledge();
        }
        // State receipts must finish while their frozen owner is still alive.
        drop(self.source.take());
    }

    fn commit(mut self) -> Result<Vec<SubscriptionDelivery>, Box<BlpAsyncError>> {
        let result = match self.failure.take() {
            Some(failure) => Err(failure.error),
            None => Ok(std::mem::take(&mut self.deliveries)),
        };
        self.acknowledge();
        result
    }
}

impl Drop for DeliveryDrainTransaction {
    fn drop(&mut self) {
        let Some(source) = self.source.as_mut() else {
            return;
        };
        if let Some(failure) = self.failure.take() {
            match failure.origin {
                DrainErrorOrigin::Stream(index) => {
                    let BlpAsyncError::Blp(error) = *failure.error else {
                        unreachable!("stream failures retain their native error")
                    };
                    self.receipts[index].restore_error(error);
                }
                DrainErrorOrigin::Cleanup => source.cleanup_error = Some(failure.error),
            }
        }
        if matches!(self.receipts.first(), Some(DeliveryReceipt::State(_))) {
            // State rollback appends dirty memberships at the FIFO tail.
            for receipt in self.receipts.drain(..) {
                drop(receipt);
            }
        } else {
            // Raw rollback restores at the front, so unwind batches in reverse.
            while let Some(receipt) = self.receipts.pop() {
                drop(receipt);
            }
        }
        self.recovery
            .lock()
            .expect("subscription drain recovery poisoned")
            .source = self.source.take();
    }
}

/// Frozen, transactional marked drain. A closed consumer can retry after this
/// lease is dropped; no accepted rows, gaps, or native terminal error are consumed.
pub struct SubscriptionDeliveryDrainLease {
    transaction: DeliveryDrainTransaction,
    _operation: OwnedMutexGuard<()>,
}

impl SubscriptionDeliveryDrainLease {
    pub fn as_ref(&self) -> Result<&[SubscriptionDelivery], &BlpAsyncError> {
        match &self.transaction.failure {
            Some(failure) => Err(failure.error.as_ref()),
            None => Ok(&self.transaction.deliveries),
        }
    }

    pub fn prepare_success(
        mut self,
    ) -> Result<(Vec<SubscriptionDelivery>, SubscriptionDeliveryDrainAck), Self> {
        if self.transaction.failure.is_some() {
            return Err(self);
        }
        let deliveries = std::mem::take(&mut self.transaction.deliveries);
        let Self {
            transaction,
            _operation,
        } = self;
        Ok((
            deliveries,
            SubscriptionDeliveryDrainAck {
                transaction,
                _operation,
            },
        ))
    }

    pub fn commit(self) -> Result<Vec<SubscriptionDelivery>, Box<BlpAsyncError>> {
        self.commit_with_guard().1
    }

    fn commit_with_guard(
        self,
    ) -> (
        OwnedMutexGuard<()>,
        Result<Vec<SubscriptionDelivery>, Box<BlpAsyncError>>,
    ) {
        let Self {
            transaction,
            _operation,
        } = self;
        (_operation, transaction.commit())
    }
}

/// Data-free whole-drain acknowledgment. Exporters may move and mutate Arrow
/// buffers; rollback rebuilds them from native updates or frozen final state.
pub struct SubscriptionDeliveryDrainAck {
    transaction: DeliveryDrainTransaction,
    _operation: OwnedMutexGuard<()>,
}

impl SubscriptionDeliveryDrainAck {
    pub fn commit(mut self) {
        self.transaction.acknowledge();
    }
}

/// Owns the receiver and control independently, so reads cannot block mutations.
/// Hosts retain conversion, interpreter attachment and deadline policies.
pub struct SubscriptionConsumer {
    pub rx: Arc<Mutex<Option<SubscriptionReceiver>>>,
    pub pending: Arc<PendingUpdates>,
    pub operations: Arc<Mutex<()>>,
    pub close_signal: watch::Sender<bool>,
    delivery_read: OnceLock<Arc<Mutex<()>>>,
    delivery_drain: OnceLock<Arc<StdMutex<DeliveryDrainRecovery>>>,
    handle: StdMutex<Option<SubscriptionHandle>>,
    delivery_mode: SubscriptionDeliveryMode,
    requires_deliveries: bool,
    batch_limit: usize,
    batcher: OnceLock<StdMutex<SubscriptionArrowBatcher>>,
}

impl Default for SubscriptionConsumer {
    /// An already-closed consumer with no receiver or control.
    fn default() -> Self {
        Self::from_parts(None, None)
    }
}

impl SubscriptionConsumer {
    pub fn new(rx: SubscriptionReceiver, handle: SubscriptionHandle) -> Self {
        Self::from_parts(Some(rx), Some(handle))
    }

    /// Consume a native producer without an engine-side control handle.
    pub fn from_receiver(rx: SubscriptionReceiver) -> Self {
        let consumer = Self::from_parts(Some(rx), None);
        consumer.close_signal.send_replace(false);
        consumer
    }

    fn from_parts(rx: Option<SubscriptionReceiver>, handle: Option<SubscriptionHandle>) -> Self {
        let delivery_mode = rx.as_ref().map_or(
            SubscriptionDeliveryMode::Deltas,
            SubscriptionReceiver::delivery_mode,
        );
        let requires_deliveries = rx
            .as_ref()
            .is_some_and(SubscriptionReceiver::requires_deliveries);
        let batch_limit = handle
            .as_ref()
            .map_or(1, SubscriptionHandle::delivery_batch_limit);
        Self {
            rx: Arc::new(Mutex::new(rx)),
            pending: Arc::new(PendingUpdates::default()),
            operations: Arc::new(Mutex::new(())),
            close_signal: watch::channel(handle.is_none()).0,
            delivery_read: OnceLock::new(),
            delivery_drain: OnceLock::new(),
            handle: StdMutex::new(handle),
            delivery_mode,
            requires_deliveries,
            batch_limit,
            batcher: OnceLock::new(),
        }
    }

    pub fn handle(&self) -> Option<SubscriptionHandle> {
        self.handle
            .lock()
            .expect("subscription control poisoned")
            .clone()
    }

    pub fn is_closed(&self) -> bool {
        *self.close_signal.borrow()
    }

    pub fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        self.delivery_mode
    }

    pub fn requires_deliveries(&self) -> bool {
        self.requires_deliveries
    }

    pub fn ensure_plain_delivery(&self) -> Result<(), BlpError> {
        if self.requires_deliveries {
            Err(plain_delivery_error())
        } else {
            Ok(())
        }
    }

    /// Join marked-read serialization only after that API has been used.
    /// Legacy-only readers do not initialize or allocate a publication gate.
    /// Return early on close; callers recheck close before reading and retain
    /// any returned guard through conversion/publication.
    pub async fn existing_delivery_read_guard(&self) -> Option<OwnedMutexGuard<()>> {
        let reader = self.delivery_read.get()?.clone();
        let mut close_rx = self.close_signal.subscribe();
        tokio::select! {
            biased;
            _ = wait_for_subscription_close(&mut close_rx) => None,
            reader = reader.lock_owned() => Some(reader),
        }
    }

    pub fn has_delivery_read_gate(&self) -> bool {
        self.delivery_read.get().is_some()
    }

    /// Lock a plain reader without allocating a gate for legacy-only use.
    /// Recheck initialization under the receiver lock before consuming pending
    /// or ready rows: the first marked reader may have appeared during the wait.
    ///
    /// Keep publication through conversion. When it is absent, keep the receiver
    /// through publication so a newly initialized marked reader cannot overtake it.
    pub async fn lock_plain_receiver(
        &self,
        close_rx: &mut watch::Receiver<bool>,
    ) -> Option<(
        OwnedMutexGuard<Option<SubscriptionReceiver>>,
        Option<OwnedMutexGuard<()>>,
    )> {
        let mut publication = None;
        loop {
            if publication.is_none()
                && let Some(reader) = self.delivery_read.get()
            {
                publication = Some(tokio::select! {
                    biased;
                    _ = wait_for_subscription_close(close_rx) => return None,
                    reader = reader.clone().lock_owned() => reader,
                });
            }
            let receiver = tokio::select! {
                biased;
                _ = wait_for_subscription_close(close_rx) => return None,
                receiver = self.rx.clone().lock_owned() => receiver,
            };
            if publication.is_none() && self.has_delivery_read_gate() {
                drop(receiver);
                continue;
            }
            if *close_rx.borrow() {
                return None;
            }
            return Some((receiver, publication));
        }
    }

    /// Wait for one layout- and continuity-bounded delivery. This future can be
    /// cancelled at any await without consuming updates, gaps, or terminal errors.
    /// Read/publication serialization is independent of the control-operation
    /// coordinator, so an idle reader cannot block add/remove/add_fields.
    pub async fn next_delivery(&self, limit: usize) -> Option<SubscriptionDeliveryLease> {
        let mut close_rx = self.close_signal.subscribe();
        if *close_rx.borrow() {
            return None;
        }
        let delivery_read = self.delivery_read.get_or_init(|| Arc::new(Mutex::new(())));
        let reader = tokio::select! {
            biased;
            _ = wait_for_subscription_close(&mut close_rx) => return None,
            reader = delivery_read.clone().lock_owned() => reader,
        };
        let mut receiver = tokio::select! {
            biased;
            _ = wait_for_subscription_close(&mut close_rx) => return None,
            receiver = self.rx.lock() => receiver,
        };
        let rx = receiver.as_mut()?;
        let receipt = if self.delivery_mode == SubscriptionDeliveryMode::State {
            let receipt = tokio::select! {
                biased;
                _ = wait_for_subscription_close(&mut close_rx) => return None,
                receipt = rx.recv_state(limit.max(1)) => receipt?,
            };
            DeliveryReceipt::State(receipt)
        } else {
            let first = match self.pending.pop_event() {
                Some(item) => item,
                None => tokio::select! {
                    biased;
                    _ = wait_for_subscription_close(&mut close_rx) => return None,
                    item = rx.recv_event() => item?,
                },
            };
            DeliveryReceipt::Raw(self.read_ready_delivery(first, Some(rx), limit))
        };
        drop(receiver);
        Some(SubscriptionDeliveryLease {
            receipt: Some(receipt),
            _reader: reader,
        })
    }

    fn read_ready_delivery(
        &self,
        first: SubscriptionEvent,
        mut rx: Option<&mut SubscriptionReceiver>,
        limit: usize,
    ) -> RawDeliveryReceipt {
        if let SubscriptionEvent::Error(error) = first {
            return RawDeliveryReceipt {
                value: Some(Err(error)),
                events: Vec::new(),
                pending: Arc::clone(&self.pending),
            };
        }
        let mut events = Vec::with_capacity(subscription_batch_capacity_hint(limit));
        events.push(first);
        while events.len() < limit.max(1) {
            let Some(item) = self
                .pending
                .pop_event()
                .or_else(|| rx.as_deref_mut().and_then(|rx| rx.try_recv_event().ok()))
            else {
                break;
            };
            let same_epoch_and_layout = match (&events[0], &item) {
                (
                    SubscriptionEvent::Update { update: first, .. },
                    SubscriptionEvent::Update {
                        update,
                        gap_before: None,
                    },
                ) => subscription_layouts_match(&first.layout, &update.layout),
                _ => false,
            };
            if !same_epoch_and_layout {
                self.pending.push_front(item);
                break;
            }
            events.push(item);
        }
        let gap_before = match &events[0] {
            SubscriptionEvent::Update { gap_before, .. } => *gap_before,
            SubscriptionEvent::Error(_) => unreachable!("first error handled above"),
        };
        let mut batcher = self
            .batcher
            .get_or_init(|| {
                StdMutex::new(SubscriptionArrowBatcher::with_capacity(
                    subscription_batch_capacity_hint(self.batch_limit),
                ))
            })
            .lock()
            .expect("subscription arrow batcher poisoned");
        for event in &events {
            if let SubscriptionEvent::Update { update, .. } = event {
                let boundary = batcher.append(update);
                debug_assert!(
                    boundary.is_none(),
                    "delivery cannot cross a layout boundary"
                );
            }
        }
        let data = batcher
            .flush()
            .expect("a delivery contains at least one update");
        RawDeliveryReceipt {
            value: Some(Ok(SubscriptionDelivery {
                kind: SubscriptionDeliveryMode::Deltas,
                data,
                gap_before,
            })),
            events,
            pending: Arc::clone(&self.pending),
        }
    }

    /// Close monotonically, retain ownership across awaits, then collect unread items.
    /// The returned guard serializes host-side draining with subsequent close calls.
    /// Stream errors take precedence over control failures; shutdown suppression is
    /// an explicit host policy and never discards a stream error.
    pub async fn unsubscribe(
        &self,
        drain: bool,
        engine_shutdown: Option<&watch::Receiver<bool>>,
    ) -> (
        OwnedMutexGuard<()>,
        Result<Vec<SubscriptionUpdate>, Box<BlpAsyncError>>,
    ) {
        if drain && self.requires_deliveries {
            let (operation, result) = self.unsubscribe_deliveries(true, engine_shutdown).await;
            return (
                operation,
                result.and_then(|_| Err(Box::new(BlpAsyncError::Blp(plain_delivery_error())))),
            );
        }
        let (operation, rx, cleanup_error, _handle) =
            self.close_receiver(drain, engine_shutdown, None).await;
        let remaining = self
            .pending
            .collect_unread(rx, drain)
            .map_err(|error| Box::new(BlpAsyncError::Blp(*error)));
        let result = remaining.and_then(|updates| match cleanup_error {
            Some(error) => Err(Box::new(error)),
            None => Ok(updates),
        });
        (operation, result)
    }

    /// Drain complete delivery envelopes, preserving every gap and the first
    /// terminal error even when detach/forwarder cleanup also fails.
    pub async fn unsubscribe_deliveries(
        &self,
        drain: bool,
        engine_shutdown: Option<&watch::Receiver<bool>>,
    ) -> (
        OwnedMutexGuard<()>,
        Result<Vec<SubscriptionDelivery>, Box<BlpAsyncError>>,
    ) {
        self.prepare_unsubscribe_deliveries(drain, engine_shutdown)
            .await
            .commit_with_guard()
    }

    /// Freeze/detach once, then retain the entire drain until host publication.
    /// Dropping either the lease or its data-free Ack permits a marked retry
    /// after close, rebuilding Arrow from native updates or frozen state.
    pub async fn prepare_unsubscribe_deliveries(
        &self,
        drain: bool,
        engine_shutdown: Option<&watch::Receiver<bool>>,
    ) -> SubscriptionDeliveryDrainLease {
        let recovery = self
            .delivery_drain
            .get_or_init(|| Arc::new(StdMutex::new(DeliveryDrainRecovery::default())))
            .clone();
        let (operation, rx, cleanup_error, handle) = self
            .close_receiver(drain, engine_shutdown, Some(&recovery))
            .await;
        let recovered = std::mem::take(
            &mut *recovery
                .lock()
                .expect("subscription drain recovery poisoned"),
        );
        let mut source = recovered.source.unwrap_or(ClosedDeliverySource {
            receiver: rx,
            _handle: handle,
            cleanup_error: cleanup_error.map(Box::new),
        });
        if source.cleanup_error.is_none() {
            source.cleanup_error = recovered.cleanup_error;
        }
        let mut transaction = DeliveryDrainTransaction {
            source: Some(source),
            recovery,
            receipts: Vec::new(),
            deliveries: Vec::new(),
            failure: None,
        };
        if !drain {
            self.pending
                .collect_unread(None, false)
                .expect("discard cannot fail");
        } else if self.delivery_mode == SubscriptionDeliveryMode::State {
            loop {
                let receipt = transaction.source.as_mut().and_then(|source| {
                    source
                        .receiver
                        .as_mut()
                        .and_then(|rx| rx.try_recv_state(self.batch_limit))
                });
                let Some(receipt) = receipt else { break };
                transaction.push(DeliveryReceipt::State(receipt));
            }
        } else {
            loop {
                let first = self.pending.pop_event().or_else(|| {
                    transaction.source.as_mut().and_then(|source| {
                        source
                            .receiver
                            .as_mut()
                            .and_then(|rx| rx.try_recv_event().ok())
                    })
                });
                let Some(first) = first else { break };
                let rx = transaction
                    .source
                    .as_mut()
                    .and_then(|source| source.receiver.as_mut());
                let receipt = self.read_ready_delivery(first, rx, self.batch_limit);
                transaction.push(DeliveryReceipt::Raw(receipt));
            }
        }
        if transaction.failure.is_none()
            && let Some(error) = transaction
                .source
                .as_mut()
                .and_then(|source| source.cleanup_error.take())
        {
            transaction.failure = Some(DrainFailure {
                error,
                origin: DrainErrorOrigin::Cleanup,
            });
        }
        SubscriptionDeliveryDrainLease {
            transaction,
            _operation: operation,
        }
    }

    async fn close_receiver(
        &self,
        drain: bool,
        engine_shutdown: Option<&watch::Receiver<bool>>,
        recovery: Option<&Arc<StdMutex<DeliveryDrainRecovery>>>,
    ) -> (
        OwnedMutexGuard<()>,
        Option<SubscriptionReceiver>,
        Option<BlpAsyncError>,
        Option<SubscriptionHandle>,
    ) {
        self.close_signal.send_replace(true);
        let operation = self.operations.clone().lock_owned().await;
        let _reader = match self.delivery_read.get() {
            Some(reader) => Some(reader.lock().await),
            None => None,
        };
        if !drain && let Some(recovery) = self.delivery_drain.get() {
            let abandoned = std::mem::take(
                &mut *recovery
                    .lock()
                    .expect("subscription drain recovery poisoned"),
            );
            drop(abandoned);
        }
        if drain && let Some(rx) = self.rx.lock().await.as_mut() {
            rx.prepare_close();
        }
        let handle = self.handle();
        let shutting_down = || engine_shutdown.is_some_and(|signal| *signal.borrow());
        let result = if let Some(handle) = &handle {
            handle.unsubscribe().await
        } else {
            Ok(())
        };
        let retain_cleanup = |error: Option<BlpAsyncError>| match recovery {
            None => error,
            Some(recovery) => {
                if let Some(error) = error {
                    recovery
                        .lock()
                        .expect("subscription drain recovery poisoned")
                        .cleanup_error
                        .get_or_insert_with(|| Box::new(error));
                }
                None
            }
        };
        let mut cleanup_error = retain_cleanup(if shutting_down() { None } else { result.err() });
        if drain && let Some(handle) = &handle {
            let mut receiver = self.rx.lock().await;
            let result = match receiver.as_mut() {
                Some(rx) if self.delivery_mode == SubscriptionDeliveryMode::Deltas => {
                    self.pending
                        .drain_forwarder(handle.drain_forwarder(), rx)
                        .await
                }
                _ => handle.drain_forwarder().await,
            };
            if cleanup_error.is_none() && !shutting_down() {
                cleanup_error = retain_cleanup(result.err());
            }
        }
        let mut receiver = self.rx.lock().await;
        if !(drain && self.delivery_mode == SubscriptionDeliveryMode::State)
            && let Some(rx) = receiver.as_mut()
        {
            rx.close();
        }
        self.handle
            .lock()
            .expect("subscription control poisoned")
            .take();
        (operation, receiver.take(), cleanup_error, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OverflowPolicy;
    use crate::engine::state::{
        FieldKind, FieldMeta, subscription_channel, subscription_channel_with_policy,
    };
    use arrow_array::TimestampMicrosecondArray;
    use std::task::{Context, Poll, Waker};

    fn update(field: &str, timestamp_us: i64) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from("TEST Index"),
            layout: Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new(field, 0, FieldKind::F64)],
            )),
            values: Default::default(),
        }
    }

    fn timestamps(delivery: &SubscriptionDelivery) -> Vec<i64> {
        delivery
            .data
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn batch_capacity_is_bounded_without_changing_the_read_limit() {
        assert_eq!(subscription_batch_capacity_hint(0), 1);
        assert_eq!(subscription_batch_capacity_hint(1), 1);
        assert_eq!(subscription_batch_capacity_hint(usize::MAX), 4096);
    }

    #[tokio::test]
    async fn reads_preserve_same_version_layout_boundaries_and_error_order() {
        let (tx, mut rx) = subscription_channel(4);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("ASK", 2))).unwrap();
        tx.fail(BlpError::Timeout);
        let pending = PendingUpdates::default();
        let (_close, mut close_rx) = watch::channel(false);

        for (field, timestamp) in [("BID", 1), ("ASK", 2)] {
            let SubscriptionRead::Updates(updates) =
                receive_subscription_updates(&mut rx, &pending, &mut close_rx, 10).await
            else {
                panic!("expected the next layout batch");
            };
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].timestamp_us, timestamp);
            assert_eq!(updates[0].layout.fields[0].name.as_ref(), field);
        }
        assert!(matches!(
            receive_subscription_updates(&mut rx, &pending, &mut close_rx, 10).await,
            SubscriptionRead::Error(BlpError::Timeout)
        ));
        assert!(matches!(
            receive_subscription_updates(&mut rx, &pending, &mut close_rx, 10).await,
            SubscriptionRead::Ended
        ));
    }

    #[test]
    fn rollback_restores_updates_before_the_deferred_boundary() {
        let pending = PendingUpdates::default();
        let mut batch = vec![update("BID", 1), update("BID", 2)];
        assert!(
            !pending
                .append_to_batch(&mut batch, Ok(update("ASK", 3)))
                .unwrap()
        );
        pending.restore(batch.into_iter());
        let updates = pending.collect_unread(None, true).unwrap();
        assert_eq!(
            updates
                .iter()
                .map(|update| update.timestamp_us)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[tokio::test]
    async fn forwarding_barrier_drains_a_full_queue_without_deadlock() {
        let (tx, mut rx) = subscription_channel(1);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        let barrier = async {
            tx.send(Ok(update("BID", 2))).await.unwrap();
            tx.send(Ok(update("ASK", 3))).await.unwrap();
            Ok(())
        };
        let pending = PendingUpdates::default();
        pending.drain_forwarder(barrier, &mut rx).await.unwrap();
        let updates = pending.collect_unread(Some(rx), true).unwrap();
        assert_eq!(
            updates
                .iter()
                .map(|update| update.timestamp_us)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[tokio::test]
    async fn cancelled_forwarding_keeps_accepted_items_for_the_next_close() {
        let (tx, mut rx) = subscription_channel(1);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        let pending = PendingUpdates::default();
        {
            let mut draining = Box::pin(pending.drain_forwarder(std::future::pending(), &mut rx));
            assert!(matches!(
                draining
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        tx.try_send(Ok(update("BID", 2))).unwrap();
        pending
            .drain_forwarder(async { Ok(()) }, &mut rx)
            .await
            .unwrap();
        let updates = pending.collect_unread(Some(rx), true).unwrap();
        assert_eq!(
            updates
                .iter()
                .map(|update| update.timestamp_us)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn drains_report_the_first_unread_error_instead_of_partial_success() {
        let pending = PendingUpdates::default();
        let mut batch = vec![update("BID", 1)];
        assert!(
            !pending
                .append_to_batch(&mut batch, Err(BlpError::Timeout))
                .unwrap()
        );
        pending.restore(batch.into_iter());
        let (tx, rx) = subscription_channel(1);
        tx.fail(BlpError::Internal {
            detail: "later error".into(),
        });
        assert!(matches!(
            *pending.collect_unread(Some(rx), true).unwrap_err(),
            BlpError::Timeout
        ));
        assert!(pending.pop_front().is_none());
    }

    #[tokio::test]
    async fn close_wakes_reads_before_serialization_and_survives_cancellation() {
        let (tx, rx) = subscription_channel(1);
        let consumer = SubscriptionConsumer::from_parts(Some(rx), None);
        consumer.close_signal.send_replace(false);
        let operation = consumer.operations.lock().await;
        let mut signal = consumer.close_signal.subscribe();
        {
            let mut closing = Box::pin(consumer.unsubscribe(true, None));
            assert!(matches!(
                closing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            wait_for_subscription_close(&mut signal).await;
            assert!(consumer.rx.try_lock().unwrap().is_some());
        }
        drop(operation);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        let (guard, updates) = consumer.unsubscribe(true, None).await;
        assert_eq!(updates.unwrap().len(), 1);
        assert!(consumer.rx.try_lock().unwrap().is_none());
        assert!(consumer.operations.try_lock().is_err());
        drop(guard);
        let (_guard, updates) = consumer.unsubscribe(true, None).await;
        assert!(updates.unwrap().is_empty());
    }

    #[tokio::test]
    async fn movable_delivery_keeps_unique_arrow_buffers_and_rolls_back_until_acknowledged() {
        let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        tx.fail(BlpError::Timeout);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let (exported, ack) = consumer
            .next_delivery(8)
            .await
            .unwrap()
            .prepare_success()
            .unwrap_or_else(|_| panic!("expected successful delivery"));
        assert_eq!(Arc::strong_count(exported.data.column(0)), 1);
        assert_eq!(timestamps(&exported), [2]);
        assert_eq!(exported.gap_before.unwrap().dropped_updates, 1);
        drop(ack);

        let (retry, ack) = consumer
            .next_delivery(8)
            .await
            .unwrap()
            .prepare_success()
            .unwrap_or_else(|_| panic!("expected restored delivery"));
        assert_eq!(timestamps(&retry), [2]);
        assert_eq!(retry.gap_before.unwrap().dropped_updates, 1);
        ack.commit();
        let terminal = match consumer.next_delivery(8).await.unwrap().prepare_success() {
            Err(terminal) => terminal,
            Ok(_) => panic!("terminal errors must retain their publication lease"),
        };
        assert!(matches!(terminal.as_ref(), Err(BlpError::Timeout)));
        assert!(matches!(terminal.commit(), Err(BlpError::Timeout)));
        assert!(consumer.next_delivery(8).await.is_none());
    }

    #[tokio::test]
    async fn cancelled_delivery_preserves_updates_gap_and_terminal_until_publication() {
        let (tx, rx) = subscription_channel_with_policy(2, OverflowPolicy::DropOldest);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        {
            let mut read = Box::pin(consumer.next_delivery(8));
            assert!(matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        for time in 1..=4 {
            tx.try_send(Ok(update("BID", time))).unwrap();
        }
        tx.fail(BlpError::Timeout);
        let lease = consumer.next_delivery(8).await.unwrap();
        assert_eq!(timestamps(lease.as_ref().unwrap()), [3, 4]);
        assert_eq!(
            lease.as_ref().unwrap().gap_before.unwrap().dropped_updates,
            2
        );
        drop(lease);
        let delivery = consumer.next_delivery(8).await.unwrap().commit().unwrap();
        assert_eq!(timestamps(&delivery), [3, 4]);
        assert_eq!(delivery.gap_before.unwrap().dropped_updates, 2);
        let terminal = consumer.next_delivery(8).await.unwrap();
        assert!(matches!(terminal.as_ref(), Err(BlpError::Timeout)));
        drop(terminal);
        assert!(matches!(
            consumer.next_delivery(8).await.unwrap().commit(),
            Err(BlpError::Timeout)
        ));
        assert!(consumer.next_delivery(8).await.is_none());
    }

    #[tokio::test]
    async fn overflow_during_conversion_does_not_mix_pre_gap_and_post_gap_deltas() {
        for first_epoch_end in [2, 4] {
            let (tx, rx) = subscription_channel_with_policy(2, OverflowPolicy::DropOldest);
            for time in 1..=first_epoch_end {
                tx.try_send(Ok(update("BID", time))).unwrap();
            }
            let consumer = SubscriptionConsumer::from_receiver(rx);
            let cancelled = consumer.next_delivery(1).await.unwrap();
            tx.try_send(Ok(update("BID", first_epoch_end + 1))).unwrap();
            tx.try_send(Ok(update("BID", first_epoch_end + 2))).unwrap();
            drop(cancelled);

            let before = consumer.next_delivery(8).await.unwrap().commit().unwrap();
            assert_eq!(timestamps(&before), [first_epoch_end - 1]);
            assert_eq!(
                before.gap_before.map(|gap| gap.dropped_updates),
                (first_epoch_end > 2).then_some((first_epoch_end - 2) as u64)
            );
            let after = consumer.next_delivery(8).await.unwrap().commit().unwrap();
            assert_eq!(
                timestamps(&after),
                [first_epoch_end + 1, first_epoch_end + 2]
            );
            assert_eq!(after.gap_before.unwrap().dropped_updates, 1);
        }
    }

    #[tokio::test]
    async fn gap_is_attached_once_even_when_post_gap_layout_changes() {
        let (tx, rx) = subscription_channel_with_policy(2, OverflowPolicy::DropOldest);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        tx.try_send(Ok(update("ASK", 3))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let first = consumer.next_delivery(8).await.unwrap().commit().unwrap();
        assert_eq!(timestamps(&first), [2]);
        assert_eq!(first.gap_before.unwrap().dropped_updates, 1);
        assert_eq!(first.data.schema().field(2).name(), "BID");
        let second = consumer.next_delivery(8).await.unwrap().commit().unwrap();
        assert_eq!(timestamps(&second), [3]);
        assert!(second.gap_before.is_none());
        assert_eq!(second.data.schema().field(2).name(), "ASK");
    }

    #[tokio::test]
    async fn cancelled_strict_delivery_is_visible_to_legacy_tick_reads() {
        let (tx, rx) = subscription_channel(2);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("ASK", 2))).unwrap();
        tx.fail(BlpError::Timeout);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        drop(consumer.next_delivery(8).await.unwrap());
        let mut receiver = consumer.rx.lock().await;
        let rx = receiver.as_mut().unwrap();
        let mut close_rx = consumer.close_signal.subscribe();
        for expected in [1, 2] {
            let SubscriptionRead::Updates(updates) =
                receive_subscription_updates(rx, &consumer.pending, &mut close_rx, 8).await
            else {
                panic!("expected restored strict update");
            };
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].timestamp_us, expected);
        }
        assert!(matches!(
            receive_subscription_updates(rx, &consumer.pending, &mut close_rx, 8).await,
            SubscriptionRead::Error(BlpError::Timeout)
        ));
    }

    #[tokio::test]
    async fn delivery_drain_retains_gap_and_configured_batch_limit_after_cancelled_read() {
        let (tx, rx) = subscription_channel_with_policy(3, OverflowPolicy::DropOldest);
        for time in 1..=6 {
            tx.try_send(Ok(update("BID", time))).unwrap();
        }
        let mut consumer = SubscriptionConsumer::from_receiver(rx);
        consumer.batch_limit = 2;
        drop(consumer.next_delivery(8).await.unwrap());
        let (_guard, result) = consumer.unsubscribe_deliveries(true, None).await;
        let deliveries = result.unwrap();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(timestamps(&deliveries[0]), [4, 5]);
        assert_eq!(deliveries[0].gap_before.unwrap().dropped_updates, 3);
        assert_eq!(timestamps(&deliveries[1]), [6]);
        assert!(deliveries[1].gap_before.is_none());
        assert!(tx.is_closed());
    }

    #[tokio::test]
    async fn forbidden_plain_drain_cleans_up_and_never_hides_terminal_loss() {
        for terminal in [false, true] {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            tx.try_send(Ok(update("BID", 1))).unwrap();
            tx.try_send(Ok(update("BID", 2))).unwrap();
            if terminal {
                tx.fail(BlpError::SubscriptionDataLoss {
                    topic: "TEST Index".into(),
                    detail: "source loss is terminal even in lossy mode".into(),
                });
            }
            let consumer = SubscriptionConsumer::from_receiver(rx);
            assert!(consumer.ensure_plain_delivery().is_err());
            let (_shutdown, shutdown_rx) = watch::channel(true);
            let (_guard, result) = consumer.unsubscribe(true, Some(&shutdown_rx)).await;
            let error = result.unwrap_err();
            if terminal {
                assert!(matches!(
                    *error,
                    BlpAsyncError::Blp(BlpError::SubscriptionDataLoss { .. })
                ));
            } else {
                assert!(matches!(
                    *error,
                    BlpAsyncError::Blp(BlpError::Validation { .. })
                ));
            }
            assert!(tx.is_closed());
            assert!(consumer.rx.try_lock().unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn first_marked_read_initialization_cannot_bypass_a_waiting_plain_reader() {
        let (tx, rx) = subscription_channel(2);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let occupied = consumer.rx.lock().await;
        let mut plain = Box::pin(async {
            let mut close_rx = consumer.close_signal.subscribe();
            let (mut receiver, _publication) =
                consumer.lock_plain_receiver(&mut close_rx).await.unwrap();
            receive_subscription_updates(
                receiver.as_mut().unwrap(),
                &consumer.pending,
                &mut close_rx,
                2,
            )
            .await
        });
        assert!(matches!(
            plain.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let mut marked = Box::pin(consumer.next_delivery(1));
        assert!(matches!(
            marked
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(occupied);
        // The plain waiter gets rx first, discovers the newly initialized gate,
        // and relinquishes rx instead of consuming ahead of marked publication.
        assert!(matches!(
            plain.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let marked = marked.await.unwrap();
        assert_eq!(timestamps(marked.as_ref().unwrap()), [1]);
        assert!(matches!(
            plain.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(marked);
        let SubscriptionRead::Updates(updates) = plain.await else {
            panic!("expected restored first update ahead of queued second update");
        };
        assert_eq!(
            updates
                .iter()
                .map(|update| update.timestamp_us)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn plain_read_waits_for_marked_rollback_before_consuming_the_shared_pending_queue() {
        let (tx, rx) = subscription_channel(2);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let marked = consumer.next_delivery(1).await.unwrap();
        let mut plain = Box::pin(async {
            let _publication = consumer.existing_delivery_read_guard().await;
            let mut receiver = consumer.rx.lock().await;
            let mut close_rx = consumer.close_signal.subscribe();
            receive_subscription_updates(
                receiver.as_mut().unwrap(),
                &consumer.pending,
                &mut close_rx,
                2,
            )
            .await
        });
        assert!(matches!(
            plain.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        // Restoring pending updates does not send a receiver notification.
        // Publication-gate wakeup must make the plain reader reconsider them.
        drop(marked);
        let SubscriptionRead::Updates(updates) = plain.await else {
            panic!("expected marked rollback ahead of queued strict data");
        };
        assert_eq!(
            updates
                .iter()
                .map(|update| update.timestamp_us)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn close_releases_plain_reader_waiting_on_marked_publication_without_consuming() {
        let (tx, rx) = subscription_channel(2);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let marked = consumer.next_delivery(1).await.unwrap();
        let mut plain = Box::pin(async {
            let _publication = consumer.existing_delivery_read_guard().await;
            if consumer.is_closed() {
                return SubscriptionRead::Closed;
            }
            let mut receiver = consumer.rx.lock().await;
            let mut close_rx = consumer.close_signal.subscribe();
            receive_subscription_updates(
                receiver.as_mut().unwrap(),
                &consumer.pending,
                &mut close_rx,
                2,
            )
            .await
        });
        assert!(matches!(
            plain.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        consumer.close_signal.send_replace(true);
        assert!(matches!(plain.await, SubscriptionRead::Closed));
        drop(marked);
        let frames = consumer
            .prepare_unsubscribe_deliveries(true, None)
            .await
            .commit()
            .unwrap();
        assert_eq!(
            frames.iter().flat_map(timestamps).collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn idle_delivery_reader_does_not_block_serialized_controls() {
        let (tx, rx) = subscription_channel(2);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let mut reading = Box::pin(consumer.next_delivery(8));
        assert!(matches!(
            reading
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let mut control = Box::pin(async {
            let _mutation = consumer.operations.lock().await;
            tx.try_send(Ok(update("ASK", 1))).unwrap();
            tx.try_send(Ok(update("ASK", 2))).unwrap();
        });
        assert!(matches!(
            control
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(())
        ));
        let delivery = reading.await.unwrap().commit().unwrap();
        assert_eq!(timestamps(&delivery), [1, 2]);
        assert_eq!(delivery.data.schema().field(2).name(), "ASK");
    }

    #[tokio::test]
    async fn close_waits_for_ready_acknowledgment_before_draining_retained_epochs() {
        for published in [false, true] {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            tx.try_send(Ok(update("BID", 1))).unwrap();
            tx.try_send(Ok(update("BID", 2))).unwrap();
            let consumer = SubscriptionConsumer::from_receiver(rx);
            let (_exported, ack) = consumer
                .next_delivery(1)
                .await
                .unwrap()
                .prepare_success()
                .unwrap_or_else(|_| panic!("expected successful delivery"));
            let mut closing = Box::pin(consumer.unsubscribe_deliveries(true, None));
            assert!(matches!(
                closing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            assert!(consumer.next_delivery(1).await.is_none());
            tx.try_send(Ok(update("BID", 3))).unwrap();
            tx.try_send(Ok(update("BID", 4))).unwrap();
            if published {
                ack.commit();
            } else {
                drop(ack);
            }
            let (_guard, result) = closing.await;
            let deliveries = result.unwrap();
            let expected = if published { vec![4] } else { vec![2, 4] };
            assert_eq!(
                deliveries.iter().flat_map(timestamps).collect::<Vec<_>>(),
                expected
            );
            assert!(deliveries.iter().all(|delivery| {
                delivery
                    .gap_before
                    .is_some_and(|gap| gap.dropped_updates == 1)
            }));
            assert!(tx.is_closed());
        }
    }

    #[tokio::test]
    async fn failed_drain_conversion_restores_all_frames_after_unique_buffers_were_mutated() {
        let (tx, rx) = subscription_channel_with_policy(3, OverflowPolicy::DropOldest);
        for time in 1..=6 {
            tx.try_send(Ok(update("BID", time))).unwrap();
        }
        let consumer = SubscriptionConsumer::from_receiver(rx);
        for fail_at in [0, 1, 2] {
            let lease = consumer.prepare_unsubscribe_deliveries(true, None).await;
            let frames = lease.as_ref().unwrap();
            assert_eq!(
                frames.iter().flat_map(timestamps).collect::<Vec<_>>(),
                [4, 5, 6]
            );
            assert_eq!(frames[0].gap_before.unwrap().dropped_updates, 3);
            let convert = || -> Result<(), &'static str> {
                let (frames, ack) = lease
                    .prepare_success()
                    .unwrap_or_else(|_| panic!("expected successful drain"));
                for (index, frame) in frames.into_iter().enumerate() {
                    let values = frame
                        .data
                        .column(0)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .clone();
                    drop(frame);
                    let mut exported = values
                        .into_builder()
                        .unwrap_or_else(|_| panic!("Ack must not retain exported Arrow buffers"));
                    exported.values_slice_mut()[0] = -999;
                    if index == fail_at {
                        return Err("synthetic backend conversion failure");
                    }
                }
                ack.commit();
                Ok(())
            };
            assert!(convert().is_err());
            assert!(consumer.is_closed());
        }
        let frames = consumer
            .prepare_unsubscribe_deliveries(true, None)
            .await
            .commit()
            .unwrap();
        assert_eq!(
            frames.iter().flat_map(timestamps).collect::<Vec<_>>(),
            [4, 5, 6]
        );
        assert_eq!(frames[0].gap_before.unwrap().dropped_updates, 3);
        assert!(frames[1..].iter().all(|frame| frame.gap_before.is_none()));
        assert!(
            consumer
                .prepare_unsubscribe_deliveries(true, None)
                .await
                .commit()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dropped_error_drain_restores_partial_rows_gap_and_terminal_until_error_publication() {
        let (tx, rx) = subscription_channel_with_policy(2, OverflowPolicy::DropOldest);
        for time in 1..=4 {
            tx.try_send(Ok(update("BID", time))).unwrap();
        }
        tx.fail(BlpError::Timeout);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let lease = consumer.prepare_unsubscribe_deliveries(true, None).await;
        assert!(matches!(
            lease.as_ref(),
            Err(BlpAsyncError::Blp(BlpError::Timeout))
        ));
        assert_eq!(
            lease
                .transaction
                .deliveries
                .iter()
                .flat_map(timestamps)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        assert_eq!(
            lease.transaction.deliveries[0]
                .gap_before
                .unwrap()
                .dropped_updates,
            2
        );
        drop(lease);
        let retry = consumer.prepare_unsubscribe_deliveries(true, None).await;
        assert!(matches!(
            retry.as_ref(),
            Err(BlpAsyncError::Blp(BlpError::Timeout))
        ));
        assert_eq!(
            retry
                .transaction
                .deliveries
                .iter()
                .flat_map(timestamps)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        let retry = match retry.prepare_success() {
            Err(retry) => retry,
            Ok(_) => panic!("an unread terminal error must prevent partial publication"),
        };
        assert!(matches!(
            *retry.commit().unwrap_err(),
            BlpAsyncError::Blp(BlpError::Timeout)
        ));
        assert!(
            consumer
                .prepare_unsubscribe_deliveries(true, None)
                .await
                .commit()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cancelled_marked_close_keeps_closed_drain_available_for_retry() {
        let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let operation = consumer.operations.lock().await;
        {
            let mut closing = Box::pin(consumer.prepare_unsubscribe_deliveries(true, None));
            assert!(matches!(
                closing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert!(consumer.is_closed());
        drop(operation);
        let lease = consumer.prepare_unsubscribe_deliveries(true, None).await;
        assert_eq!(timestamps(&lease.as_ref().unwrap()[0]), [2]);
        assert_eq!(
            lease.as_ref().unwrap()[0]
                .gap_before
                .unwrap()
                .dropped_updates,
            1
        );
        drop(lease);
        let frames = consumer
            .prepare_unsubscribe_deliveries(true, None)
            .await
            .commit()
            .unwrap();
        assert_eq!(timestamps(&frames[0]), [2]);
        assert_eq!(frames[0].gap_before.unwrap().dropped_updates, 1);
    }

    #[tokio::test]
    async fn universal_cleanup_discards_an_unpublished_closed_marked_drain() {
        let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        tx.try_send(Ok(update("BID", 1))).unwrap();
        tx.try_send(Ok(update("BID", 2))).unwrap();
        let consumer = SubscriptionConsumer::from_receiver(rx);
        drop(consumer.prepare_unsubscribe_deliveries(true, None).await);
        let (guard, result) = consumer.unsubscribe(false, None).await;
        assert!(result.unwrap().is_empty());
        drop(guard);
        assert!(
            consumer
                .prepare_unsubscribe_deliveries(true, None)
                .await
                .commit()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn explicit_close_wakes_an_idle_delivery_reader_before_waiting_for_publication() {
        let (_tx, rx) = subscription_channel(1);
        let consumer = SubscriptionConsumer::from_receiver(rx);
        let mut reading = Box::pin(consumer.next_delivery(1));
        assert!(matches!(
            reading
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let mut closing = Box::pin(consumer.unsubscribe_deliveries(false, None));
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert!(reading.await.is_none());
        let (_guard, result) = closing.await;
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_does_not_suppress_an_unread_stream_error() {
        let (tx, rx) = subscription_channel(1);
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "TEST Index".into(),
            detail: "terminal loss".into(),
        });
        let consumer = SubscriptionConsumer::from_parts(Some(rx), None);
        let (_shutdown, shutdown_rx) = watch::channel(true);
        let (_guard, result) = consumer.unsubscribe(true, Some(&shutdown_rx)).await;
        assert!(matches!(
            *result.unwrap_err(),
            BlpAsyncError::Blp(BlpError::SubscriptionDataLoss { .. })
        ));
    }
}
