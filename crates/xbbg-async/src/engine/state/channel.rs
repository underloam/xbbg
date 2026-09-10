//! Reliable bounded channel for subscription updates and terminal failures.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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
    });
    (
        SubscriptionSender {
            shared: Arc::clone(&shared),
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
        Ok(())
    }
}

/// Sending half of a subscription channel.
pub struct SubscriptionSender {
    shared: Arc<Shared>,
}

/// Weak capability that can only publish a terminal subscription failure.
///
/// It is deliberately not a sender: retaining it neither keeps the channel
/// connected nor permits data publication.
#[derive(Clone)]
pub(crate) struct SubscriptionTerminator {
    shared: Weak<Shared>,
}

impl SubscriptionTerminator {
    pub(crate) fn fail(&self, error: BlpError) {
        if let Some(shared) = self.shared.upgrade() {
            let _ = shared.send_terminal(error);
        }
    }
}

impl SubscriptionSender {
    /// Send an update, waiting for bounded capacity when necessary.
    ///
    /// Sending an `Err` terminates the channel through [`Self::fail`] semantics
    /// instead of competing with data for bounded capacity.
    pub async fn send(
        &self,
        item: Result<SubscriptionUpdate, BlpError>,
    ) -> Result<(), SendError<Result<SubscriptionUpdate, BlpError>>> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(SendError(item));
        }
        let update = match item {
            Ok(update) => update,
            Err(error) => return self.send_terminal(error).map_err(SendError),
        };

        let permit = match self.shared.slots.acquire().await {
            Ok(permit) => permit,
            Err(_) => return Err(SendError(Ok(update))),
        };
        let mut state = self.shared.state.lock();
        if !state.accepts_data() {
            return Err(SendError(Ok(update)));
        }
        state.queue.push_back(update);
        permit.forget();
        drop(state);
        self.shared.ready.notify_one();
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
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(item));
        }
        let update = match item {
            Ok(update) => update,
            Err(error) => return self.send_terminal(error).map_err(TrySendError::Closed),
        };

        let permit = match self.shared.slots.try_acquire() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => return Err(TrySendError::Closed(Ok(update))),
            Err(TryAcquireError::NoPermits) => {
                if self.is_closed() {
                    return Err(TrySendError::Closed(Ok(update)));
                }
                return Err(TrySendError::Full(Ok(update)));
            }
        };
        let mut state = self.shared.state.lock();
        if !state.accepts_data() {
            return Err(TrySendError::Closed(Ok(update)));
        }
        state.queue.push_back(update);
        permit.forget();
        drop(state);
        self.shared.ready.notify_one();
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
            shared: Arc::downgrade(&self.shared),
        }
    }

    /// Whether this sender can no longer accept data.
    pub fn is_closed(&self) -> bool {
        !self.shared.accepting.load(Ordering::Acquire)
    }

    fn send_terminal(&self, error: BlpError) -> Result<(), Result<SubscriptionUpdate, BlpError>> {
        self.shared.send_terminal(error)
    }
}

impl Clone for SubscriptionSender {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for SubscriptionSender {
    fn drop(&mut self) {
        let last = self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1;
        if last {
            self.shared.accepting.store(false, Ordering::Release);
            self.shared.ready.notify_one();
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
        let mut state = self.shared.state.lock();
        if state.receiver_closed {
            return;
        }
        state.receiver_closed = true;
        self.shared.accepting.store(false, Ordering::Release);
        drop(state);
        self.shared.slots.close();
        self.shared.ready.notify_one();
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
