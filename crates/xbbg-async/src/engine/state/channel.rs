//! Reliable bounded channel for subscription updates and terminal failures.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};
use tokio::sync::{Notify, Semaphore, TryAcquireError};
use xbbg_core::BlpError;

use super::update::SubscriptionUpdate;

/// Create a bounded subscription channel whose terminal error cannot be
/// displaced by queued data.
pub fn subscription_channel(capacity: usize) -> (SubscriptionSender, SubscriptionReceiver) {
    assert!(
        capacity > 0,
        "subscription channel capacity must be greater than zero"
    );
    let shared = Arc::new(Shared {
        state: Mutex::new(ChannelState {
            queue: VecDeque::with_capacity(capacity),
            terminal_error: None,
            terminal_delivered: false,
            receiver_closed: false,
        }),
        accepting: AtomicBool::new(true),
        sender_count: AtomicUsize::new(1),
        slots: Semaphore::new(capacity),
        ready: Notify::new(),
        on_closed: OnceLock::new(),
    });
    (
        SubscriptionSender {
            shared: SenderShared::Channel(Arc::clone(&shared)),
        },
        SubscriptionReceiver { shared },
    )
}

struct Shared {
    accepting: AtomicBool,
    state: Mutex<ChannelState>,
    slots: Semaphore,
    sender_count: AtomicUsize,
    ready: Notify,
    on_closed: OnceLock<Arc<dyn Fn() + Send + Sync>>,
}

struct ChannelState {
    queue: VecDeque<SubscriptionUpdate>,
    terminal_error: Option<BlpError>,
    terminal_delivered: bool,
    receiver_closed: bool,
}

impl ChannelState {
    fn accepts_data(&self) -> bool {
        !self.receiver_closed && self.terminal_error.is_none() && !self.terminal_delivered
    }
}

impl Shared {
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
}

impl CallbackShared {
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
        if item.is_err()
            && !(matches!(&item, Err(BlpError::SubscriptionDataLoss { .. }))
                && self.keep_open_on_data_loss.load(Ordering::Relaxed))
        {
            self.accepting.store(false, Ordering::Release);
        }
        (self.callback)(item);
        Ok(())
    }
}

enum SenderShared {
    Channel(Arc<Shared>),
    Callback(Arc<CallbackShared>),
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
        if let SenderShared::Channel(shared) = &self.shared {
            let _ = shared.on_closed.set(callback);
            if !shared.accepting.load(Ordering::Acquire) {
                shared.notify_closed();
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
        if let SenderShared::Callback(shared) = &self.shared {
            if let Some(callback) = shared.field_error.get() {
                callback(error, scalar);
                return true;
            }
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
            SenderShared::Callback(shared) => {
                let _delivery = shared.delivery.lock();
                shared.accepting.store(false, Ordering::Release);
            }
        }
    }

    /// Whether this sender can no longer accept data.
    pub fn is_closed(&self) -> bool {
        match &self.shared {
            SenderShared::Channel(shared) => !shared.accepting.load(Ordering::Acquire),
            SenderShared::Callback(shared) => !shared.accepting.load(Ordering::Acquire),
        }
    }

    fn send_terminal(&self, error: BlpError) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        match &self.shared {
            SenderShared::Channel(shared) => shared.send_terminal(error),
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
            SenderShared::Channel(shared) => {
                let last = shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1;
                if last {
                    shared.accepting.store(false, Ordering::Release);
                    shared.ready.notify_one();
                    shared.notify_closed();
                }
            }
            SenderShared::Callback(shared) => {
                if shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                    shared.accepting.store(false, Ordering::Release);
                }
            }
        }
    }
}

/// Receiving half of a subscription channel.
pub struct SubscriptionReceiver {
    shared: Arc<Shared>,
}

impl SubscriptionReceiver {
    /// Receive accepted data, then the terminal error exactly once, then EOF.
    ///
    /// The operation is cancellation-safe: cancelling this future does not
    /// consume either a queued update or the terminal error.
    pub async fn recv(&mut self) -> Option<Result<SubscriptionUpdate, BlpError>> {
        loop {
            let notified = self.shared.ready.notified();
            {
                let mut state = self.shared.state.lock();
                if let Some(update) = state.queue.pop_front() {
                    drop(state);
                    self.shared.slots.add_permits(1);
                    return Some(Ok(update));
                }
                if let Some(error) = state.terminal_error.take() {
                    state.terminal_delivered = true;
                    return Some(Err(error));
                }
                if state.receiver_closed
                    || state.terminal_delivered
                    || self.shared.sender_count.load(Ordering::Acquire) == 0
                {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Try to receive without waiting.
    pub fn try_recv(&mut self) -> Result<Result<SubscriptionUpdate, BlpError>, TryRecvError> {
        let mut state = self.shared.state.lock();
        if let Some(update) = state.queue.pop_front() {
            drop(state);
            self.shared.slots.add_permits(1);
            return Ok(Ok(update));
        }
        if let Some(error) = state.terminal_error.take() {
            state.terminal_delivered = true;
            return Ok(Err(error));
        }
        if state.receiver_closed
            || state.terminal_delivered
            || self.shared.sender_count.load(Ordering::Acquire) == 0
        {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Reject future sends while retaining already accepted data for draining.
    pub fn close(&mut self) {
        self.shared.close();
    }

    /// Whether no further values can be accepted into this receiver.
    pub fn is_closed(&self) -> bool {
        !self.shared.accepting.load(Ordering::Acquire)
    }

    /// Number of buffered data updates, excluding the out-of-band terminal error.
    pub fn len(&self) -> usize {
        self.shared.state.lock().queue.len()
    }

    /// Whether no data is buffered; a terminal error may still be pending.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        state.receiver_closed = true;
        self.shared.accepting.store(false, Ordering::Release);
        state.queue.clear();
        state.terminal_error.take();
        drop(state);
        self.shared.slots.close();
        self.shared.ready.notify_one();
        self.shared.notify_closed();
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
        assert!(tokio::time::timeout(Duration::from_millis(1), rx.recv())
            .await
            .is_err());

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
