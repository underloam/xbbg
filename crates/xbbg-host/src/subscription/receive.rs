//! Deadline-aware plain reads (the napi `nextUpdates`/`nextArrowBatch` loop).
//!
//! Without a deadline a read waits for the first update, then takes only rows
//! that are already queued. With a deadline it keeps waiting until the limit
//! fills or the deadline passes; a zero wait is a non-blocking drain. Rows of
//! a dropped (cancelled) read return to the front of the pending queue.

use std::time::Duration;

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::watch;
use tokio::time::Instant;
use xbbg_async::engine::state::{SubscriptionReceiver, SubscriptionUpdate};
use xbbg_async::subscription_consumer::{
    PendingUpdates, StreamItem, subscription_batch_capacity_hint, wait_for_subscription_close,
};
use xbbg_core::BlpError;

/// Result of a read that may end, time out, or produce a value.
#[derive(Debug)]
pub enum ReadOutcome<T> {
    Ready(T),
    /// Closed consumer or finished stream: nothing more will arrive.
    Ended,
    /// The wait limit passed without data; nothing was consumed.
    TimedOut,
}

/// Why a read stopped taking items.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Closed,
    Ended,
    Elapsed,
    /// Nothing more is queued right now (non-waiting take).
    Empty,
    /// The batch is full or reached a layout/error boundary.
    Boundary,
}

async fn receive_stream_item(
    rx: &mut SubscriptionReceiver,
    close_rx: &mut watch::Receiver<bool>,
    deadline: Option<Instant>,
) -> Result<StreamItem, Stop> {
    if *close_rx.borrow() {
        return Err(Stop::Closed);
    }
    if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
        return rx.try_recv().map_err(|error| match error {
            TryRecvError::Empty => Stop::Elapsed,
            TryRecvError::Disconnected => Stop::Ended,
        });
    }
    let receive = async {
        tokio::select! {
            biased;
            _ = wait_for_subscription_close(close_rx) => Err(Stop::Closed),
            item = rx.recv() => item.ok_or(Stop::Ended),
        }
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, receive)
            .await
            .unwrap_or(Err(Stop::Elapsed)),
        None => receive.await,
    }
}

/// Uncommitted rows of an in-progress read; dropping restores them in order.
struct PendingUpdateBatch<'a> {
    pending: &'a PendingUpdates,
    updates: Vec<SubscriptionUpdate>,
    committed: bool,
}

impl<'a> PendingUpdateBatch<'a> {
    fn new(pending: &'a PendingUpdates, capacity: usize) -> Self {
        Self {
            pending,
            updates: Vec::with_capacity(capacity),
            committed: false,
        }
    }

    fn commit(mut self) -> Vec<SubscriptionUpdate> {
        self.committed = true;
        std::mem::take(&mut self.updates)
    }
}

impl Drop for PendingUpdateBatch<'_> {
    fn drop(&mut self) {
        if !self.committed && !self.updates.is_empty() {
            self.pending.restore(self.updates.drain(..));
        }
    }
}

/// Read one layout-homogeneous batch of at most `limit` rows.
///
/// A terminal error is returned once no earlier row is left in this batch; an
/// error behind rows is deferred to the next read.
pub(crate) async fn receive_updates(
    rx: &mut SubscriptionReceiver,
    pending: &PendingUpdates,
    close_rx: &mut watch::Receiver<bool>,
    limit: usize,
    max_wait: Option<Duration>,
) -> Result<ReadOutcome<Vec<SubscriptionUpdate>>, Box<BlpError>> {
    if *close_rx.borrow() {
        return Ok(ReadOutcome::Ended);
    }
    let deadline = max_wait.map(|wait| Instant::now() + wait);
    let mut batch = PendingUpdateBatch::new(pending, subscription_batch_capacity_hint(limit));
    let stop = loop {
        let received = match pending.pop_front() {
            Some(item) => Ok(item),
            None if batch.updates.is_empty() || deadline.is_some() => {
                receive_stream_item(rx, close_rx, deadline).await
            }
            None => rx.try_recv().map_err(|error| match error {
                TryRecvError::Empty => Stop::Empty,
                TryRecvError::Disconnected => Stop::Ended,
            }),
        };
        let item = match received {
            Ok(item) => item,
            Err(stop) => break stop,
        };
        let appended = pending
            .append_to_batch(&mut batch.updates, item)
            .map_err(Box::new)?;
        if !appended || batch.updates.len() == limit {
            break Stop::Boundary;
        }
    };
    if !batch.updates.is_empty() {
        return Ok(ReadOutcome::Ready(batch.commit()));
    }
    Ok(if stop == Stop::Elapsed {
        ReadOutcome::TimedOut
    } else {
        ReadOutcome::Ended
    })
}
