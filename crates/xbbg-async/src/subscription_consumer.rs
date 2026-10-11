//! Consumer-side subscription ordering and lifecycle shared by language adapters.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{Mutex, OwnedMutexGuard, watch};
use xbbg_core::BlpError;

use crate::engine::state::{FieldLayout, SubscriptionReceiver, SubscriptionUpdate};
use crate::{BlpAsyncError, SubscriptionHandle};

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
pub struct PendingUpdates(StdMutex<VecDeque<StreamItem>>);

impl PendingUpdates {
    pub fn pop_front(&self) -> Option<StreamItem> {
        self.0
            .lock()
            .expect("subscription pending queue poisoned")
            .pop_front()
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
                    .push_front(item);
                Ok(false)
            }
        }
    }

    /// Restore an adapter's uncommitted batch after a deadline-aware read is cancelled.
    pub fn restore(&self, updates: impl DoubleEndedIterator<Item = SubscriptionUpdate>) {
        let mut pending = self.0.lock().expect("subscription pending queue poisoned");
        for update in updates.rev() {
            pending.push_front(Ok(update));
        }
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
        let mut collect = |item| match item {
            Ok(update) => updates.push(update),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        };
        while let Some(item) = pending.pop_front() {
            collect(item);
        }
        drop(pending);
        if let Some(rx) = rx.as_mut() {
            while let Ok(item) = rx.try_recv() {
                collect(item);
            }
        }
        match first_error {
            Some(error) => Err(Box::new(error)),
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
                item = rx.recv() => match item {
                    Some(item) => self.0.lock().expect("subscription pending queue poisoned").push_back(item),
                    None => break barrier.await,
                },
                result = &mut barrier => break result,
            }
        };
        while let Ok(item) = rx.try_recv() {
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
pub async fn receive_subscription_updates(
    rx: &mut SubscriptionReceiver,
    pending: &PendingUpdates,
    close_rx: &mut watch::Receiver<bool>,
    limit: usize,
) -> SubscriptionRead {
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

/// Owns the receiver and control independently, so reads cannot block mutations.
/// Hosts retain conversion, interpreter attachment and deadline policies.
pub struct SubscriptionConsumer {
    pub rx: Arc<Mutex<Option<SubscriptionReceiver>>>,
    pub pending: Arc<PendingUpdates>,
    pub operations: Arc<Mutex<()>>,
    pub close_signal: watch::Sender<bool>,
    handle: StdMutex<Option<SubscriptionHandle>>,
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

    fn from_parts(rx: Option<SubscriptionReceiver>, handle: Option<SubscriptionHandle>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
            pending: Arc::new(PendingUpdates::default()),
            operations: Arc::new(Mutex::new(())),
            close_signal: watch::channel(handle.is_none()).0,
            handle: StdMutex::new(handle),
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
        self.close_signal.send_replace(true);
        let operation = self.operations.clone().lock_owned().await;
        let handle = self.handle();
        let shutting_down = || engine_shutdown.is_some_and(|signal| *signal.borrow());
        let result = if let Some(handle) = &handle {
            handle.unsubscribe().await
        } else {
            Ok(())
        };
        let mut cleanup_error = if shutting_down() { None } else { result.err() };
        if drain && let Some(handle) = &handle {
            let mut receiver = self.rx.lock().await;
            let result = match receiver.as_mut() {
                Some(rx) => {
                    self.pending
                        .drain_forwarder(handle.drain_forwarder(), rx)
                        .await
                }
                None => handle.drain_forwarder().await,
            };
            if cleanup_error.is_none() && !shutting_down() {
                cleanup_error = result.err();
            }
        }
        let mut receiver = self.rx.lock().await;
        if let Some(rx) = receiver.as_mut() {
            rx.close();
        }
        self.handle
            .lock()
            .expect("subscription control poisoned")
            .take();
        let rx = receiver.take();
        drop(receiver);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::{FieldKind, FieldMeta, subscription_channel};
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
