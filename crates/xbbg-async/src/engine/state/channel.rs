//! Bounded subscription transport with durable terminal failures and optional gaps.
//!
//! Strict queues retain the semaphore-backed legacy path. Overwrite queues own
//! their gap counter separately, so strict reads never build delivery metadata.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};
use tokio::sync::{Notify, Semaphore, TryAcquireError};
use xbbg_core::BlpError;

use super::update::SubscriptionUpdate;
use crate::engine::shared_subscriptions::state_delivery::{
    StateDeliveryReceipt, StateDeliveryReceiver,
};
use crate::engine::{OverflowPolicy, SubscriptionDeliveryMode, SubscriptionGap};

/// Create a bounded subscription channel whose terminal error cannot be
/// displaced by queued data.
pub fn subscription_channel(capacity: usize) -> (SubscriptionSender, SubscriptionReceiver) {
    let shared = Arc::new(Shared::new(capacity, Semaphore::new(capacity)));
    (
        SubscriptionSender {
            shared: SenderShared::Channel(Arc::clone(&shared)),
        },
        SubscriptionReceiver {
            shared: ReceiverShared::Channel(shared),
        },
    )
}

/// Create a strict bounded queue, or an explicitly gap-aware overwrite queue.
pub fn subscription_channel_with_policy(
    capacity: usize,
    policy: OverflowPolicy,
) -> (SubscriptionSender, SubscriptionReceiver) {
    match policy {
        OverflowPolicy::DropNewest | OverflowPolicy::Block => subscription_channel(capacity),
        OverflowPolicy::DropOldest => {
            let shared = Arc::new(Shared::new(capacity, DropOldest { capacity }));
            (
                SubscriptionSender {
                    shared: SenderShared::DropOldest(Arc::clone(&shared)),
                },
                SubscriptionReceiver {
                    shared: ReceiverShared::DropOldest(shared),
                },
            )
        }
    }
}

struct Shared<Q: QueueStrategy = Semaphore> {
    accepting: AtomicBool,
    state: Mutex<ChannelState<Q::Gap>>,
    slots: Q,
    sender_count: AtomicUsize,
    ready: Notify,
    on_closed: OnceLock<Arc<dyn Fn() + Send + Sync>>,
}

/// Statically specialize capacity closure and gap storage, not each data operation.
trait QueueStrategy {
    type Gap: Default;

    fn close(&self);
}

impl QueueStrategy for Semaphore {
    type Gap = ();

    fn close(&self) {
        Semaphore::close(self);
    }
}

struct DropOldest {
    capacity: usize,
}

impl QueueStrategy for DropOldest {
    type Gap = u64;

    fn close(&self) {
        // Overwrite producers never wait for capacity.
    }
}

struct ChannelState<G> {
    queue: VecDeque<SubscriptionUpdate>,
    pending_gap: G,
    terminal_error: Option<BlpError>,
    terminal_delivered: bool,
    receiver_closed: bool,
}

impl<G> ChannelState<G> {
    fn accepts_data(&self) -> bool {
        !self.receiver_closed && self.terminal_error.is_none() && !self.terminal_delivered
    }
}

impl Shared<DropOldest> {
    fn send(
        &self,
        item: Result<SubscriptionUpdate, BlpError>,
    ) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        match item {
            Ok(update) => self
                .try_send_lossy(update)
                .map(|_| ())
                .map_err(|error| Ok(error.into_inner())),
            Err(error) => self.send_terminal(error),
        }
    }

    fn try_send_lossy(
        &self,
        update: SubscriptionUpdate,
    ) -> Result<u64, TrySendError<SubscriptionUpdate>> {
        let mut state = self.state.lock();
        if !state.accepts_data() || !self.accepting.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(update));
        }
        let evicted = if state.queue.len() == self.slots.capacity {
            state.pending_gap = state.pending_gap.saturating_add(1);
            state.queue.pop_front()
        } else {
            None
        };
        let dropped = u64::from(evicted.is_some());
        state.queue.push_back(update);
        drop(state);
        // Potentially large update payloads are released outside the producer lock.
        drop(evicted);
        self.ready.notify_one();
        Ok(dropped)
    }

    async fn recv_event(&self) -> Option<SubscriptionEvent> {
        loop {
            let notified = self.ready.notified();
            match self.try_recv_event() {
                Ok(item) => return Some(item),
                Err(TryRecvError::Disconnected) => return None,
                Err(TryRecvError::Empty) => notified.await,
            }
        }
    }

    fn try_recv_event(&self) -> Result<SubscriptionEvent, TryRecvError> {
        let mut state = self.state.lock();
        if let Some(update) = state.queue.pop_front() {
            let gap_before = match std::mem::take(&mut state.pending_gap) {
                0 => None,
                dropped_updates => Some(SubscriptionGap { dropped_updates }),
            };
            drop(state);
            return Ok(SubscriptionEvent::Update { update, gap_before });
        }
        if let Some(error) = state.terminal_error.take() {
            state.terminal_delivered = true;
            return Ok(SubscriptionEvent::Error(error));
        }
        if state.receiver_closed
            || state.terminal_delivered
            || self.sender_count.load(Ordering::Acquire) == 0
        {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<Q: QueueStrategy> Shared<Q> {
    fn new(capacity: usize, slots: Q) -> Self {
        assert!(
            capacity > 0,
            "subscription channel capacity must be greater than zero"
        );
        Self {
            state: Mutex::new(ChannelState {
                queue: VecDeque::with_capacity(capacity),
                pending_gap: Q::Gap::default(),
                terminal_error: None,
                terminal_delivered: false,
                receiver_closed: false,
            }),
            accepting: AtomicBool::new(true),
            sender_count: AtomicUsize::new(1),
            slots,
            ready: Notify::new(),
            on_closed: OnceLock::new(),
        }
    }

    fn notify_closed(&self) {
        if let Some(callback) = self.on_closed.get() {
            callback();
        }
    }

    fn send_terminal(&self, error: BlpError) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        let mut state = self.state.lock();
        if self.sender_count.load(Ordering::Acquire) == 0 || !state.accepts_data() {
            return Err(Err(error));
        }
        state.terminal_error = Some(error);
        self.accepting.store(false, Ordering::Release);
        drop(state);
        self.slots.close();
        self.ready.notify_one();
        self.notify_closed();
        Ok(())
    }

    fn close(&self) {
        let mut state = self.state.lock();
        if state.receiver_closed {
            return;
        }
        state.receiver_closed = true;
        self.accepting.store(false, Ordering::Release);
        drop(state);
        self.slots.close();
        self.ready.notify_one();
        self.notify_closed();
    }

    fn sender_dropped(&self) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.accepting.store(false, Ordering::Release);
            self.ready.notify_one();
            self.notify_closed();
        }
    }

    fn receiver_dropped(&self) {
        let mut state = self.state.lock();
        state.receiver_closed = true;
        self.accepting.store(false, Ordering::Release);
        let queued = std::mem::take(&mut state.queue);
        let error = state.terminal_error.take();
        drop(state);
        drop((queued, error));
        self.slots.close();
        self.ready.notify_one();
        self.notify_closed();
    }
}

type FieldErrorCallback = dyn Fn(&BlpError, bool) + Send + Sync;

struct CallbackShared {
    callback: Arc<dyn Fn(Result<SubscriptionUpdate, BlpError>) + Send + Sync>,
    accepting: AtomicBool,
    keep_open_on_data_loss: AtomicBool,
    sender_count: AtomicUsize,
    /// Serialize delivery with terminal failure so no update follows the error.
    delivery: Mutex<()>,
    field_error: OnceLock<Arc<FieldErrorCallback>>,
    stream_availability: OnceLock<Arc<dyn Fn(bool) + Send + Sync>>,
    on_closed: OnceLock<Arc<dyn Fn() + Send + Sync>>,
}

impl CallbackShared {
    fn notify_closed(&self) {
        if let Some(callback) = self.on_closed.get() {
            callback();
        }
    }

    fn send(
        &self,
        item: Result<SubscriptionUpdate, BlpError>,
    ) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(item);
        }
        let _delivery = self.delivery.lock();
        if !self.accepting.load(Ordering::Acquire) {
            return Err(item);
        }
        let terminal = item.is_err()
            && !(matches!(&item, Err(BlpError::SubscriptionDataLoss { .. }))
                && self.keep_open_on_data_loss.load(Ordering::Relaxed));
        if terminal {
            self.accepting.store(false, Ordering::Release);
        }
        (self.callback)(item);
        drop(_delivery);
        if terminal {
            self.notify_closed();
        }
        Ok(())
    }
}

enum SenderShared {
    Channel(Arc<Shared>),
    Callback(Arc<CallbackShared>),
    DropOldest(Arc<Shared<DropOldest>>),
}

/// Sending half of a subscription channel, or an inline engine callback.
pub struct SubscriptionSender {
    shared: SenderShared,
}

/// Weak capability that can only publish subscription failures.
///
/// It is deliberately not a sender: retaining it neither keeps the channel
/// connected nor permits data publication.
#[derive(Clone)]
pub(crate) struct SubscriptionTerminator {
    shared: TerminatorShared,
}

#[derive(Clone)]
enum TerminatorShared {
    Channel(Weak<Shared>),
    Callback(Weak<CallbackShared>),
    DropOldest(Weak<Shared<DropOldest>>),
}

impl SubscriptionTerminator {
    pub(crate) fn is_callback(&self) -> bool {
        matches!(self.shared, TerminatorShared::Callback(_))
    }

    pub(crate) fn fail(&self, error: BlpError) {
        match &self.shared {
            TerminatorShared::Channel(shared) => {
                if let Some(shared) = shared.upgrade() {
                    let _ = shared.send_terminal(error);
                }
            }
            TerminatorShared::DropOldest(shared) => {
                if let Some(shared) = shared.upgrade() {
                    let _ = shared.send_terminal(error);
                }
            }
            TerminatorShared::Callback(shared) => {
                if let Some(shared) = shared.upgrade() {
                    let _ = shared.send(Err(error));
                }
            }
        }
    }
}

impl SubscriptionSender {
    /// Notify engine ownership when a queue fails asynchronously (notably a
    /// Block-policy timeout). Installed before the first consumer is attached.
    pub(crate) fn on_closed(&self, callback: Arc<dyn Fn() + Send + Sync>) {
        match &self.shared {
            SenderShared::Channel(shared) => {
                let _ = shared.on_closed.set(callback);
                if !shared.accepting.load(Ordering::Acquire) {
                    shared.notify_closed();
                }
            }
            SenderShared::DropOldest(shared) => {
                let _ = shared.on_closed.set(callback);
                if !shared.accepting.load(Ordering::Acquire) {
                    shared.notify_closed();
                }
            }
            SenderShared::Callback(shared) => {
                let _ = shared.on_closed.set(callback);
                if !shared.accepting.load(Ordering::Acquire) {
                    shared.notify_closed();
                }
            }
        }
    }

    /// Deliver inline without a queue. The callback must not re-enter this sender.
    pub(crate) fn callback(
        callback: Arc<dyn Fn(Result<SubscriptionUpdate, BlpError>) + Send + Sync>,
    ) -> Self {
        Self {
            shared: SenderShared::Callback(Arc::new(CallbackShared {
                callback,
                accepting: AtomicBool::new(true),
                keep_open_on_data_loss: AtomicBool::new(false),
                sender_count: AtomicUsize::new(1),
                delivery: Mutex::new(()),
                field_error: OnceLock::new(),
                stream_availability: OnceLock::new(),
                on_closed: OnceLock::new(),
            })),
        }
    }

    pub(crate) fn is_callback(&self) -> bool {
        matches!(self.shared, SenderShared::Callback(_))
    }

    pub(crate) fn on_field_error(&self, callback: Arc<FieldErrorCallback>) {
        if let SenderShared::Callback(shared) = &self.shared {
            let _ = shared.field_error.set(callback);
        }
    }
    /// Exact SDK availability notifications, independent of status-event dedup.
    pub(crate) fn on_stream_availability(&self, callback: Arc<dyn Fn(bool) + Send + Sync>) {
        if let SenderShared::Callback(shared) = &self.shared {
            let _ = shared.stream_availability.set(callback);
        }
    }

    pub(crate) fn report_stream_availability(&self, active: bool) {
        if let SenderShared::Callback(shared) = &self.shared
            && shared.accepting.load(Ordering::Acquire)
            && let Some(callback) = shared.stream_availability.get()
        {
            callback(active);
        }
    }

    /// Let an inline feed callback own DATALOSS recovery and teardown.
    ///
    /// Install before delivery starts; like delivery, this must not re-enter the
    /// callback sender. Other errors stay terminal and bounded queues are unchanged.
    pub(crate) fn keep_open_on_data_loss(&self) {
        if let SenderShared::Callback(shared) = &self.shared {
            let _delivery = shared.delivery.lock();
            shared.keep_open_on_data_loss.store(true, Ordering::Relaxed);
        }
    }

    /// Return true when the engine attributed a decode error to consumers.
    pub(crate) fn report_field_error(&self, error: &BlpError, scalar: bool) -> bool {
        if let SenderShared::Callback(shared) = &self.shared
            && let Some(callback) = shared.field_error.get()
        {
            callback(error, scalar);
            return true;
        }
        false
    }

    /// Send an update, waiting for bounded capacity when necessary.
    ///
    /// Sending an `Err` terminates the channel through [`Self::fail`] semantics
    /// instead of competing with data for bounded capacity.
    pub async fn send(
        &self,
        item: Result<SubscriptionUpdate, BlpError>,
    ) -> Result<(), SendError<Result<SubscriptionUpdate, BlpError>>> {
        let shared = match &self.shared {
            SenderShared::Channel(shared) => shared,
            SenderShared::Callback(shared) => return shared.send(item).map_err(SendError),
            SenderShared::DropOldest(shared) => return shared.send(item).map_err(SendError),
        };
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(SendError(item));
        }
        let update = match item {
            Ok(update) => update,
            Err(error) => return self.send_terminal(error).map_err(SendError),
        };

        let permit = match shared.slots.acquire().await {
            Ok(permit) => permit,
            Err(_) => return Err(SendError(Ok(update))),
        };
        let mut state = shared.state.lock();
        if !state.accepts_data() {
            return Err(SendError(Ok(update)));
        }
        state.queue.push_back(update);
        permit.forget();
        drop(state);
        shared.ready.notify_one();
        Ok(())
    }

    /// Try to send an update without waiting for bounded capacity.
    ///
    /// Sending an `Err` reliably terminates the channel even when the data queue
    /// is full.
    pub fn try_send(
        &self,
        item: Result<SubscriptionUpdate, BlpError>,
    ) -> Result<(), TrySendError<Result<SubscriptionUpdate, BlpError>>> {
        let shared = match &self.shared {
            SenderShared::Channel(shared) => shared,
            SenderShared::Callback(shared) => {
                return shared.send(item).map_err(TrySendError::Closed);
            }
            SenderShared::DropOldest(shared) => {
                return shared.send(item).map_err(TrySendError::Closed);
            }
        };
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(item));
        }
        let update = match item {
            Ok(update) => update,
            Err(error) => return self.send_terminal(error).map_err(TrySendError::Closed),
        };

        let permit = match shared.slots.try_acquire() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => return Err(TrySendError::Closed(Ok(update))),
            Err(TryAcquireError::NoPermits) => {
                if self.is_closed() {
                    return Err(TrySendError::Closed(Ok(update)));
                }
                return Err(TrySendError::Full(Ok(update)));
            }
        };
        let mut state = shared.state.lock();
        if !state.accepts_data() {
            return Err(TrySendError::Closed(Ok(update)));
        }
        state.queue.push_back(update);
        permit.forget();
        drop(state);
        shared.ready.notify_one();
        Ok(())
    }

    /// Publish one delta and return the exact number evicted by this send.
    pub(crate) fn try_send_with_eviction(
        &self,
        update: SubscriptionUpdate,
    ) -> Result<u64, TrySendError<SubscriptionUpdate>> {
        if let SenderShared::DropOldest(shared) = &self.shared {
            return shared.try_send_lossy(update);
        }
        self.try_send(Ok(update))
            .map(|()| 0)
            .map_err(|error| match error {
                TrySendError::Closed(Ok(update)) => TrySendError::Closed(update),
                TrySendError::Full(Ok(update)) => TrySendError::Full(update),
                TrySendError::Closed(Err(_)) | TrySendError::Full(Err(_)) => {
                    unreachable!("only an update was sent")
                }
            })
    }

    /// Store the first terminal failure outside bounded data capacity.
    ///
    /// Accepted data remains ordered ahead of the error. Subsequent failures
    /// and sends are ignored or rejected, preserving the first error.
    pub fn fail(&self, error: BlpError) {
        let _ = self.send_terminal(error);
    }

    pub(crate) fn terminator(&self) -> SubscriptionTerminator {
        SubscriptionTerminator {
            shared: match &self.shared {
                SenderShared::Channel(shared) => TerminatorShared::Channel(Arc::downgrade(shared)),
                SenderShared::DropOldest(shared) => {
                    TerminatorShared::DropOldest(Arc::downgrade(shared))
                }
                SenderShared::Callback(shared) => {
                    TerminatorShared::Callback(Arc::downgrade(shared))
                }
            },
        }
    }

    /// Reject future sends and wake the receiver to drain accepted data and any
    /// terminal error, then reach EOF even while senders remain alive.
    ///
    /// Callback sinks close without an error and must not re-enter this sender.
    pub(crate) fn close(&self) {
        match &self.shared {
            SenderShared::Channel(shared) => shared.close(),
            SenderShared::DropOldest(shared) => shared.close(),
            SenderShared::Callback(shared) => {
                let _delivery = shared.delivery.lock();
                shared.accepting.store(false, Ordering::Release);
                drop(_delivery);
                shared.notify_closed();
            }
        }
    }

    /// Whether this sender can no longer accept data.
    pub fn is_closed(&self) -> bool {
        match &self.shared {
            SenderShared::Channel(shared) => !shared.accepting.load(Ordering::Acquire),
            SenderShared::DropOldest(shared) => !shared.accepting.load(Ordering::Acquire),
            SenderShared::Callback(shared) => !shared.accepting.load(Ordering::Acquire),
        }
    }

    fn send_terminal(&self, error: BlpError) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        match &self.shared {
            SenderShared::Channel(shared) => shared.send_terminal(error),
            SenderShared::DropOldest(shared) => shared.send_terminal(error),
            SenderShared::Callback(shared) => shared.send(Err(error)),
        }
    }
}

impl Clone for SubscriptionSender {
    fn clone(&self) -> Self {
        let shared = match &self.shared {
            SenderShared::Channel(shared) => {
                shared.sender_count.fetch_add(1, Ordering::Relaxed);
                SenderShared::Channel(Arc::clone(shared))
            }
            SenderShared::DropOldest(shared) => {
                shared.sender_count.fetch_add(1, Ordering::Relaxed);
                SenderShared::DropOldest(Arc::clone(shared))
            }
            SenderShared::Callback(shared) => {
                shared.sender_count.fetch_add(1, Ordering::Relaxed);
                SenderShared::Callback(Arc::clone(shared))
            }
        };
        Self { shared }
    }
}

impl Drop for SubscriptionSender {
    fn drop(&mut self) {
        match &self.shared {
            SenderShared::Channel(shared) => shared.sender_dropped(),
            SenderShared::DropOldest(shared) => shared.sender_dropped(),
            SenderShared::Callback(shared) => {
                if shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                    shared.accepting.store(false, Ordering::Release);
                    shared.notify_closed();
                }
            }
        }
    }
}

pub(crate) enum SubscriptionEvent {
    Update {
        update: SubscriptionUpdate,
        gap_before: Option<SubscriptionGap>,
    },
    Error(BlpError),
}

impl From<Result<SubscriptionUpdate, BlpError>> for SubscriptionEvent {
    fn from(item: Result<SubscriptionUpdate, BlpError>) -> Self {
        match item {
            Ok(update) => Self::Update {
                update,
                gap_before: None,
            },
            Err(error) => Self::Error(error),
        }
    }
}

impl SubscriptionEvent {
    pub(crate) fn into_legacy(self) -> Result<SubscriptionUpdate, BlpError> {
        match self {
            Self::Update { update, gap_before } => {
                debug_assert!(gap_before.is_none(), "legacy reads cannot discard a gap");
                Ok(update)
            }
            Self::Error(error) => Err(error),
        }
    }
}

pub(crate) fn plain_delivery_error() -> BlpError {
    BlpError::Validation {
        message: "state and drop_oldest subscriptions require .deliveries(); use \
                 .deliveries().unsubscribe(drain=true) to drain without losing delivery metadata"
            .into(),
        errors: Vec::new(),
    }
}

enum ReceiverShared {
    Channel(Arc<Shared>),
    State(StateDeliveryReceiver),
    DropOldest(Arc<Shared<DropOldest>>),
}

/// Receiving half of a subscription channel.
pub struct SubscriptionReceiver {
    shared: ReceiverShared,
}

impl SubscriptionReceiver {
    pub(crate) fn from_state(receiver: StateDeliveryReceiver) -> Self {
        Self {
            shared: ReceiverShared::State(receiver),
        }
    }

    pub fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        match &self.shared {
            ReceiverShared::Channel(_) | ReceiverShared::DropOldest(_) => {
                SubscriptionDeliveryMode::Deltas
            }
            ReceiverShared::State(_) => SubscriptionDeliveryMode::State,
        }
    }

    pub fn requires_deliveries(&self) -> bool {
        !matches!(self.shared, ReceiverShared::Channel(_))
    }

    pub fn ensure_plain_delivery(&self) -> Result<(), BlpError> {
        if self.requires_deliveries() {
            Err(plain_delivery_error())
        } else {
            Ok(())
        }
    }

    /// Receive strict deltas, preserving the terminal failure exactly once.
    /// Advanced modes are rejected before any data or continuity is consumed.
    pub async fn recv(&mut self) -> Option<Result<SubscriptionUpdate, BlpError>> {
        let ReceiverShared::Channel(shared) = &self.shared else {
            return Some(Err(plain_delivery_error()));
        };
        loop {
            let notified = shared.ready.notified();
            {
                let mut state = shared.state.lock();
                if let Some(update) = state.queue.pop_front() {
                    drop(state);
                    shared.slots.add_permits(1);
                    return Some(Ok(update));
                }
                if let Some(error) = state.terminal_error.take() {
                    state.terminal_delivered = true;
                    return Some(Err(error));
                }
                if state.receiver_closed
                    || state.terminal_delivered
                    || shared.sender_count.load(Ordering::Acquire) == 0
                {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub fn try_recv(&mut self) -> Result<Result<SubscriptionUpdate, BlpError>, TryRecvError> {
        let ReceiverShared::Channel(shared) = &self.shared else {
            return Ok(Err(plain_delivery_error()));
        };
        let mut state = shared.state.lock();
        if let Some(update) = state.queue.pop_front() {
            drop(state);
            shared.slots.add_permits(1);
            return Ok(Ok(update));
        }
        if let Some(error) = state.terminal_error.take() {
            state.terminal_delivered = true;
            return Ok(Err(error));
        }
        if state.receiver_closed
            || state.terminal_delivered
            || shared.sender_count.load(Ordering::Acquire) == 0
        {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub(crate) async fn recv_event(&mut self) -> Option<SubscriptionEvent> {
        match &self.shared {
            ReceiverShared::Channel(_) => self.recv().await.map(SubscriptionEvent::from),
            ReceiverShared::DropOldest(shared) => shared.recv_event().await,
            ReceiverShared::State(_) => Some(SubscriptionEvent::Error(plain_delivery_error())),
        }
    }

    pub(crate) fn try_recv_event(&mut self) -> Result<SubscriptionEvent, TryRecvError> {
        match &self.shared {
            ReceiverShared::Channel(_) => self.try_recv().map(SubscriptionEvent::from),
            ReceiverShared::DropOldest(shared) => shared.try_recv_event(),
            ReceiverShared::State(_) => Err(TryRecvError::Disconnected),
        }
    }

    pub(crate) async fn recv_state(&mut self, limit: usize) -> Option<StateDeliveryReceipt> {
        match &mut self.shared {
            ReceiverShared::State(receiver) => receiver.recv(limit).await,
            ReceiverShared::Channel(_) | ReceiverShared::DropOldest(_) => None,
        }
    }

    pub(crate) fn try_recv_state(&mut self, limit: usize) -> Option<StateDeliveryReceipt> {
        match &mut self.shared {
            ReceiverShared::State(receiver) => receiver.try_recv(limit),
            ReceiverShared::Channel(_) | ReceiverShared::DropOldest(_) => None,
        }
    }

    /// Freeze final dirty state while memberships and projections still exist.
    pub(crate) fn prepare_close(&mut self) {
        if let ReceiverShared::State(receiver) = &mut self.shared {
            receiver.prepare_close();
        }
    }

    /// Reject future sends while retaining accepted data for draining.
    pub fn close(&mut self) {
        match &mut self.shared {
            ReceiverShared::Channel(shared) => shared.close(),
            ReceiverShared::DropOldest(shared) => shared.close(),
            ReceiverShared::State(receiver) => receiver.close(),
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.shared {
            ReceiverShared::Channel(shared) => !shared.accepting.load(Ordering::Acquire),
            ReceiverShared::DropOldest(shared) => !shared.accepting.load(Ordering::Acquire),
            ReceiverShared::State(receiver) => receiver.is_closed(),
        }
    }

    /// Buffered updates or dirty state topics, excluding terminal failures.
    pub fn len(&self) -> usize {
        match &self.shared {
            ReceiverShared::Channel(shared) => shared.state.lock().queue.len(),
            ReceiverShared::DropOldest(shared) => shared.state.lock().queue.len(),
            ReceiverShared::State(receiver) => receiver.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) {
        match &self.shared {
            ReceiverShared::Channel(shared) => shared.receiver_dropped(),
            ReceiverShared::DropOldest(shared) => shared.receiver_dropped(),
            ReceiverShared::State(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::engine::state::FieldLayout;

    fn update(topic_id: u32) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us: i64::from(topic_id),
            topic_id,
            topic: Arc::from("TEST"),
            layout: Arc::new(FieldLayout::new(1, Vec::new())),
            values: Default::default(),
        }
    }

    fn failure(detail: &str) -> BlpError {
        BlpError::Internal {
            detail: detail.to_string(),
        }
    }

    #[tokio::test]
    async fn callback_delivers_inline_and_closes_all_sender_clones_on_first_failure() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&received);
        let tx = SubscriptionSender::callback(Arc::new(move |item| {
            output.lock().push(item);
        }));
        let retained = tx.clone();
        let terminator = tx.terminator();

        tx.try_send(Ok(update(1))).expect("inline delivery");
        assert_eq!(received.lock()[0].as_ref().unwrap().topic_id, 1);
        retained.send(Ok(update(2))).await.expect("async sender");
        terminator.fail(BlpError::Timeout);
        tx.fail(failure("second failure"));

        assert!(tx.is_closed());
        assert!(retained.is_closed());
        assert!(matches!(
            tx.try_send(Ok(update(3))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        assert!(retained.send(Ok(update(4))).await.is_err());
        let received = received.lock();
        assert_eq!(received.len(), 3);
        assert_eq!(received[1].as_ref().unwrap().topic_id, 2);
        assert!(matches!(received[2], Err(BlpError::Timeout)));
    }

    #[tokio::test]
    async fn callback_data_loss_opt_in_allows_recovery_until_a_terminal_error() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&received);
        let tx = SubscriptionSender::callback(Arc::new(move |item| {
            output.lock().push(item);
        }));
        let retained = tx.clone();
        retained.keep_open_on_data_loss();
        let terminator = tx.terminator();

        tx.try_send(Ok(update(1))).expect("initial data");
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "TEST".into(),
            detail: "synthetic data loss".into(),
        });
        assert!(!tx.is_closed());
        assert!(!retained.is_closed());
        retained.send(Ok(update(2))).await.expect("recovery data");
        terminator.fail(BlpError::Timeout);
        tx.fail(failure("later failure"));
        tx.keep_open_on_data_loss();

        assert!(tx.is_closed());
        assert!(retained.is_closed());
        assert!(matches!(
            tx.try_send(Ok(update(3))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        let received = received.lock();
        assert_eq!(received.len(), 4);
        assert_eq!(received[0].as_ref().unwrap().topic_id, 1);
        assert!(matches!(
            &received[1],
            Err(BlpError::SubscriptionDataLoss { topic, .. }) if topic == "TEST"
        ));
        assert_eq!(received[2].as_ref().unwrap().topic_id, 2);
        assert!(matches!(received[3], Err(BlpError::Timeout)));
    }

    #[test]
    fn callback_data_loss_remains_terminal_without_opt_in() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&received);
        let tx = SubscriptionSender::callback(Arc::new(move |item| {
            output.lock().push(item);
        }));
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "TEST".into(),
            detail: "synthetic data loss".into(),
        });
        tx.keep_open_on_data_loss();
        assert!(tx.is_closed());
        assert!(matches!(
            tx.try_send(Ok(update(1))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        let received = received.lock();
        assert_eq!(received.len(), 1);
        assert!(matches!(
            received[0],
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
    }

    #[test]
    fn bounded_queue_dataloss_stays_terminal_despite_callback_recovery_opt_in() {
        let (tx, mut rx) = subscription_channel(1);
        tx.keep_open_on_data_loss();
        tx.try_send(Ok(update(1))).expect("fill bounded queue");
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "TEST".into(),
            detail: "synthetic data loss".into(),
        });

        assert!(tx.is_closed());
        assert!(matches!(
            tx.try_send(Ok(update(2))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        assert_eq!(rx.try_recv().unwrap().unwrap().topic_id, 1);
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[tokio::test]
    async fn sender_close_drains_accepted_data_and_preserves_a_pending_terminal_error() {
        for with_error in [false, true] {
            let (tx, mut rx) = subscription_channel(1);
            let retained = tx.clone();
            tx.try_send(Ok(update(1))).expect("accepted data");
            if with_error {
                tx.fail(BlpError::Timeout);
            }

            tx.close();
            retained.close();
            retained.fail(failure("after close"));
            assert!(retained.is_closed());
            assert!(matches!(
                retained.try_send(Ok(update(2))),
                Err(TrySendError::Closed(Ok(_)))
            ));
            assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 1);
            if with_error {
                assert!(matches!(rx.recv().await.unwrap(), Err(BlpError::Timeout)));
            }
            assert!(rx.recv().await.is_none());
        }
    }

    #[tokio::test]
    async fn sender_close_wakes_a_pending_reader_with_senders_alive() {
        let (tx, mut rx) = subscription_channel(1);
        let close = async {
            tokio::task::yield_now().await;
            tx.close();
        };
        let (item, ()) = tokio::join!(rx.recv(), close);
        assert!(item.is_none());
        assert!(tx.is_closed());
    }

    #[test]
    fn callback_close_rejects_future_delivery_without_publishing_an_error() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&received);
        let tx = SubscriptionSender::callback(Arc::new(move |item| {
            output.lock().push(item);
        }));
        tx.keep_open_on_data_loss();
        tx.try_send(Ok(update(1))).expect("accepted data");
        tx.close();
        tx.fail(BlpError::Timeout);
        assert!(matches!(
            tx.try_send(Ok(update(2))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        let received = received.lock();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].as_ref().unwrap().topic_id, 1);
    }

    #[test]
    fn callback_weak_terminator_cannot_deliver_after_last_sender_drops() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&received);
        let tx = SubscriptionSender::callback(Arc::new(move |item| {
            output.lock().push(item);
        }));
        let retained = tx.clone();
        let terminator = tx.terminator();
        drop(tx);
        retained.try_send(Ok(update(1))).expect("retained sender");
        drop(retained);
        terminator.fail(BlpError::Timeout);

        let received = received.lock();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].as_ref().unwrap().topic_id, 1);
    }

    #[tokio::test]
    async fn full_queue_drains_before_reliable_terminal_error_and_eof() {
        let (tx, mut rx) = subscription_channel(1);
        let retained = tx.clone();
        tx.try_send(Ok(update(1))).expect("fill bounded queue");

        tx.fail(failure("terminal"));

        assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 1);
        assert!(matches!(
            rx.recv().await.unwrap(),
            Err(BlpError::Internal { .. })
        ));
        assert!(rx.recv().await.is_none());
        assert!(retained.is_closed());
    }

    #[tokio::test]
    async fn terminal_failure_wakes_pending_reader_with_sender_clones_alive() {
        let (tx, mut rx) = subscription_channel(1);
        let retained = tx.clone();
        let wake = async {
            tokio::task::yield_now().await;
            tx.fail(failure("wake"));
        };
        let (item, ()) = tokio::join!(rx.recv(), wake);

        assert!(matches!(item.unwrap(), Err(BlpError::Internal { .. })));
        assert!(rx.recv().await.is_none());
        assert!(retained.is_closed());
    }

    #[tokio::test]
    async fn first_terminal_error_is_preserved_and_delivered_once() {
        let (tx, mut rx) = subscription_channel(1);
        tx.fail(BlpError::Timeout);
        tx.fail(failure("second"));

        assert!(matches!(rx.recv().await.unwrap(), Err(BlpError::Timeout)));
        assert!(rx.recv().await.is_none());
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn weak_terminator_does_not_retain_sender_lifetime_or_publish_after_eof() {
        let (tx, mut rx) = subscription_channel(1);
        let terminator = tx.terminator();
        drop(tx);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
        terminator.fail(failure("too late"));
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[tokio::test]
    async fn cancelled_receive_does_not_consume_the_next_value() {
        let (tx, mut rx) = subscription_channel(1);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), rx.recv())
                .await
                .is_err()
        );

        tx.send(Ok(update(7)))
            .await
            .expect("send after cancellation");
        assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 7);
    }

    #[tokio::test]
    async fn blocked_send_is_rejected_when_channel_fails() {
        let (tx, mut rx) = subscription_channel(1);
        tx.send(Ok(update(1))).await.expect("fill bounded queue");
        let blocked = {
            let tx = tx.clone();
            tokio::spawn(async move { tx.send(Ok(update(2))).await })
        };
        tokio::task::yield_now().await;

        tx.fail(failure("stop blocked sender"));

        assert!(blocked.await.expect("sender task").is_err());
        assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 1);
        assert!(rx.recv().await.unwrap().is_err());
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn overwrite_keeps_newest_updates_and_durable_gap_before_terminal_error() {
        let (tx, mut rx) = subscription_channel_with_policy(3, OverflowPolicy::DropOldest);
        for id in 1..=6 {
            assert_eq!(
                tx.try_send_with_eviction(update(id)).unwrap(),
                u64::from(id > 3)
            );
        }
        tx.fail(BlpError::Timeout);
        tx.fail(failure("must not replace first terminal"));
        assert!(tx.try_send(Ok(update(7))).is_err());
        for id in 4..=6 {
            let SubscriptionEvent::Update { update, gap_before } = rx.try_recv_event().unwrap()
            else {
                panic!("expected retained delta");
            };
            assert_eq!(update.topic_id, id);
            assert_eq!(
                gap_before.map(|gap| gap.dropped_updates),
                (id == 4).then_some(3)
            );
        }
        assert!(matches!(
            rx.try_recv_event(),
            Ok(SubscriptionEvent::Error(BlpError::Timeout))
        ));
        assert!(matches!(
            rx.try_recv_event(),
            Err(TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn strict_plain_and_delivery_reads_each_release_bounded_capacity() {
        for policy in [OverflowPolicy::DropNewest, OverflowPolicy::Block] {
            let (tx, mut rx) = subscription_channel_with_policy(1, policy);
            assert!(!rx.requires_deliveries());
            assert_eq!(rx.delivery_mode(), SubscriptionDeliveryMode::Deltas);
            tx.try_send(Ok(update(1))).unwrap();
            assert!(matches!(
                rx.try_recv_event().unwrap(),
                SubscriptionEvent::Update { update, gap_before: None } if update.topic_id == 1
            ));
            tx.try_send(Ok(update(2))).unwrap();
            assert!(matches!(
                rx.recv_event().await.unwrap(),
                SubscriptionEvent::Update { update, gap_before: None } if update.topic_id == 2
            ));
            tx.send(Ok(update(3))).await.unwrap();
            assert_eq!(rx.try_recv().unwrap().unwrap().topic_id, 3);
            tx.send(Ok(update(4))).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 4);
            assert!(matches!(rx.try_recv_event(), Err(TryRecvError::Empty)));

            tx.terminator().fail(BlpError::Timeout);
            assert!(matches!(
                rx.try_recv_event(),
                Ok(SubscriptionEvent::Error(BlpError::Timeout))
            ));
            assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
        }
    }

    #[tokio::test]
    async fn overwrite_close_drains_gap_and_preserves_the_first_terminal_error() {
        for sender_close in [false, true] {
            for with_error in [false, true] {
                let (tx, mut rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
                let retained = tx.clone();
                let terminator = tx.terminator();
                assert!(rx.requires_deliveries());
                assert_eq!(rx.delivery_mode(), SubscriptionDeliveryMode::Deltas);
                tx.send(Ok(update(1))).await.unwrap();
                retained.try_send(Ok(update(2))).unwrap();
                if with_error {
                    terminator.fail(BlpError::Timeout);
                }
                if sender_close {
                    tx.close();
                } else {
                    rx.close();
                }
                terminator.fail(failure("after close"));
                assert!(rx.is_closed());
                assert!(retained.is_closed());
                assert!(matches!(
                    retained.try_send_with_eviction(update(3)),
                    Err(TrySendError::Closed(_))
                ));
                let SubscriptionEvent::Update { update, gap_before } =
                    rx.recv_event().await.unwrap()
                else {
                    panic!("expected retained delta");
                };
                assert_eq!(update.topic_id, 2);
                assert_eq!(gap_before.unwrap().dropped_updates, 1);
                if with_error {
                    assert!(matches!(
                        rx.recv_event().await,
                        Some(SubscriptionEvent::Error(BlpError::Timeout))
                    ));
                }
                assert!(rx.recv_event().await.is_none());
            }
        }
    }

    #[tokio::test]
    async fn overwrite_last_sender_wakes_reader_and_weak_terminator_cannot_reopen() {
        let (tx, mut rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        let retained = tx.clone();
        let terminator = tx.terminator();
        drop(tx);
        let close = async move {
            tokio::task::yield_now().await;
            drop(retained);
        };
        let (item, ()) = tokio::join!(rx.recv_event(), close);
        assert!(item.is_none());
        terminator.fail(BlpError::Timeout);
        assert!(matches!(
            rx.try_recv_event(),
            Err(TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn overwrite_receiver_drop_notifies_and_rejects_retained_senders() {
        let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        let closed = Arc::new(AtomicUsize::new(0));
        let on_closed = Arc::clone(&closed);
        tx.on_closed(Arc::new(move || {
            on_closed.fetch_add(1, Ordering::Relaxed);
        }));
        let retained = tx.clone();
        let terminator = tx.terminator();
        tx.try_send(Ok(update(1))).unwrap();
        drop(rx);
        assert_eq!(closed.load(Ordering::Relaxed), 1);
        assert!(retained.is_closed());
        assert!(retained.send(Ok(update(2))).await.is_err());
        assert!(matches!(
            retained.try_send(Ok(update(3))),
            Err(TrySendError::Closed(Ok(_)))
        ));
        terminator.fail(BlpError::Timeout);
        tx.close();
        assert_eq!(closed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn concurrent_overwrite_counts_each_evicted_update_exactly_once() {
        const CAPACITY: usize = 16;
        const PER_PRODUCER: u32 = 128;
        const PRODUCERS: u32 = 4;
        let (tx, mut rx) = subscription_channel_with_policy(CAPACITY, OverflowPolicy::DropOldest);
        let dropped = std::thread::scope(|scope| {
            let mut producers = Vec::new();
            for producer in 0..PRODUCERS {
                let tx = tx.clone();
                producers.push(scope.spawn(move || {
                    (1..=PER_PRODUCER)
                        .map(|sequence| {
                            tx.try_send_with_eviction(update(producer * 1000 + sequence))
                                .unwrap()
                        })
                        .sum::<u64>()
                }));
            }
            producers
                .into_iter()
                .map(|producer| producer.join().unwrap())
                .sum::<u64>()
        });
        let expected = u64::from(PER_PRODUCER * PRODUCERS) - CAPACITY as u64;
        assert_eq!(dropped, expected);
        assert_eq!(rx.len(), CAPACITY);
        let mut previous = [0; PRODUCERS as usize];
        for position in 0..CAPACITY {
            let SubscriptionEvent::Update { update, gap_before } = rx.try_recv_event().unwrap()
            else {
                panic!("expected retained delta");
            };
            let producer = (update.topic_id / 1000) as usize;
            let sequence = update.topic_id % 1000;
            assert!(
                sequence > previous[producer],
                "producer order must remain FIFO"
            );
            previous[producer] = sequence;
            assert_eq!(
                gap_before.map(|gap| gap.dropped_updates),
                (position == 0).then_some(expected)
            );
        }
        assert!(matches!(rx.try_recv_event(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn advanced_legacy_reads_reject_without_consuming_gaps_or_data() {
        let (tx, mut rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
        tx.try_send(Ok(update(1))).unwrap();
        tx.try_send(Ok(update(2))).unwrap();
        assert!(rx.recv().await.unwrap().is_err());
        assert!(rx.try_recv().unwrap().is_err());
        let SubscriptionEvent::Update { update, gap_before } = rx.recv_event().await.unwrap()
        else {
            panic!("expected retained delta");
        };
        assert_eq!(update.topic_id, 2);
        assert_eq!(gap_before.unwrap().dropped_updates, 1);
        rx.close();
        assert!(rx.recv_event().await.is_none());
    }

    #[test]
    fn callback_close_notifications_cover_terminal_explicit_and_last_sender_close() {
        for close_kind in 0..3 {
            let closed = Arc::new(AtomicUsize::new(0));
            let on_closed = Arc::clone(&closed);
            let tx = SubscriptionSender::callback(Arc::new(|_| {}));
            tx.on_closed(Arc::new(move || {
                on_closed.fetch_add(1, Ordering::Relaxed);
            }));
            tx.keep_open_on_data_loss();
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "TEST".into(),
                detail: "recoverable callback loss".into(),
            });
            assert_eq!(closed.load(Ordering::Relaxed), 0);
            match close_kind {
                0 => tx.fail(BlpError::Timeout),
                1 => tx.close(),
                _ => drop(tx),
            }
            assert!(closed.load(Ordering::Relaxed) > 0);
        }
    }

    #[tokio::test]
    async fn sending_error_uses_terminal_path_when_queue_is_full() {
        let (tx, mut rx) = subscription_channel(1);
        tx.try_send(Ok(update(1))).expect("fill bounded queue");
        tx.try_send(Err(failure("sent terminal")))
            .expect("terminal send bypasses capacity");

        assert_eq!(rx.recv().await.unwrap().unwrap().topic_id, 1);
        assert!(rx.recv().await.unwrap().is_err());
        assert!(rx.recv().await.is_none());
    }
}
