//! Shared startup and service-open bookkeeping for asynchronous sessions.
//!
//! Workers retain their own dispatch and locking policy. Register and resolve
//! service attempts under the worker's lock; never hold that lock while awaiting
//! a receiver. Cancellation and timeouts only retire the matching generation.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use tokio::sync::oneshot;
use xbbg_core::BlpError;

use super::dispatch::SERVICE_OPEN_CID_TAG;

const SERVICE_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct StartupOutcome {
    resolved: bool,
    result: Option<Result<(), BlpError>>,
}

/// The first startup status wins; the creating thread consumes its outcome.
#[derive(Default)]
pub(super) struct StartupLatch {
    outcome: Mutex<StartupOutcome>,
    changed: Condvar,
}

impl StartupLatch {
    pub(super) fn resolve(&self, result: Result<(), BlpError>) {
        let mut outcome = self.outcome.lock();
        if !outcome.resolved {
            outcome.resolved = true;
            outcome.result = Some(result);
            self.changed.notify_all();
        }
    }

    pub(super) fn wait(&self, timeout: Duration) -> Result<(), BlpError> {
        let deadline = Instant::now() + timeout;
        let mut outcome = self.outcome.lock();
        while outcome.result.is_none() {
            if self.changed.wait_until(&mut outcome, deadline).timed_out() {
                return Err(BlpError::Timeout);
            }
        }
        outcome.result.take().expect("checked above")
    }
}

pub(super) struct PendingServiceOpen {
    cid: i64,
    waiters: Vec<oneshot::Sender<Result<(), BlpError>>>,
}

impl PendingServiceOpen {
    pub(super) fn complete(self, mut outcome: impl FnMut() -> Result<(), BlpError>) {
        for waiter in self.waiters {
            let _ = waiter.send(outcome());
        }
    }
}

/// Mutated only under the caller's lock, including generation allocation.
#[derive(Default)]
pub(super) struct PendingServiceOpens {
    next_id: i64,
    by_service: HashMap<String, PendingServiceOpen>,
}

impl PendingServiceOpens {
    pub(super) fn register(
        &mut self,
        service: &str,
    ) -> (bool, i64, oneshot::Receiver<Result<(), BlpError>>) {
        let (tx, rx) = oneshot::channel();
        if let Some(open) = self.by_service.get_mut(service) {
            open.waiters.retain(|waiter| !waiter.is_closed());
            open.waiters.push(tx);
            return (false, open.cid, rx);
        }
        self.next_id = self.next_id.wrapping_add(1);
        let cid = SERVICE_OPEN_CID_TAG | (self.next_id & (SERVICE_OPEN_CID_TAG - 1));
        self.by_service.insert(
            service.to_string(),
            PendingServiceOpen {
                cid,
                waiters: vec![tx],
            },
        );
        (true, cid, rx)
    }

    pub(super) fn remove(&mut self, service: &str, cid: i64) -> Option<PendingServiceOpen> {
        if self
            .by_service
            .get(service)
            .is_some_and(|open| open.cid == cid)
        {
            self.by_service.remove(service)
        } else {
            None
        }
    }

    pub(super) fn remove_by_cid(&mut self, cid: i64) -> Option<(String, PendingServiceOpen)> {
        let service = self
            .by_service
            .iter()
            .find_map(|(service, open)| (open.cid == cid).then(|| service.clone()))?;
        self.by_service.remove_entry(&service)
    }

    pub(super) fn prune(&mut self, service: &str, cid: i64) {
        if let Some(open) = self.by_service.get_mut(service) {
            if open.cid != cid {
                return;
            }
            open.waiters.retain(|waiter| !waiter.is_closed());
            if open.waiters.is_empty() {
                self.by_service.remove(service);
            }
        }
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = (String, PendingServiceOpen)> + '_ {
        self.by_service.drain()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_service.is_empty()
    }
}

/// Own the receiver so it closes before cancellation pruning takes the lock.
/// The callback leaves each worker's lock selection and ordering local.
pub(super) struct PendingWaiter<F: Fn()> {
    receiver: Option<oneshot::Receiver<Result<(), BlpError>>>,
    prune: F,
}

impl<F: Fn()> PendingWaiter<F> {
    pub(super) fn new(receiver: oneshot::Receiver<Result<(), BlpError>>, prune: F) -> Self {
        Self {
            receiver: Some(receiver),
            prune,
        }
    }

    pub(super) fn receiver(&mut self) -> &mut oneshot::Receiver<Result<(), BlpError>> {
        self.receiver.as_mut().expect("receiver is present")
    }

    pub(super) async fn wait_service_open(
        mut self,
        remove_attempt: impl FnOnce() -> Option<PendingServiceOpen>,
        dropped_error: impl FnOnce() -> BlpError,
    ) -> Result<(), BlpError> {
        match tokio::time::timeout(SERVICE_OPEN_TIMEOUT, self.receiver()).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(dropped_error()),
            Err(_) => {
                if let Some(open) = remove_attempt() {
                    open.complete(|| Err(BlpError::Timeout));
                }
                Err(BlpError::Timeout)
            }
        }
    }
}

impl<F: Fn()> Drop for PendingWaiter<F> {
    fn drop(&mut self) {
        self.receiver.take();
        (self.prune)();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVICE: &str = "//blp/refdata";

    #[test]
    fn startup_latch_preserves_first_outcome_after_consumption() {
        let startup = StartupLatch::default();
        startup.resolve(Err(BlpError::Internal {
            detail: "first status".to_string(),
        }));
        startup.resolve(Ok(()));
        assert!(matches!(
            startup.wait(Duration::ZERO),
            Err(BlpError::Internal { detail }) if detail == "first status"
        ));
        startup.resolve(Ok(()));
        assert!(matches!(
            startup.wait(Duration::ZERO),
            Err(BlpError::Timeout)
        ));
    }

    #[test]
    fn startup_timeout_does_not_resolve_the_latch() {
        let startup = StartupLatch::default();
        assert!(matches!(
            startup.wait(Duration::ZERO),
            Err(BlpError::Timeout)
        ));
        startup.resolve(Ok(()));
        assert!(startup.wait(Duration::ZERO).is_ok());
    }

    #[test]
    fn startup_status_wakes_the_creating_thread() {
        let startup = StartupLatch::default();
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| startup.wait(Duration::from_secs(1)));
            startup.resolve(Ok(()));
            assert!(waiting.join().unwrap().is_ok());
        });
    }

    #[test]
    fn service_opens_coalesce_and_prune_closed_receivers() {
        let mut pending = PendingServiceOpens::default();
        let (first, cid, receiver) = pending.register(SERVICE);
        assert!(first);
        drop(receiver);
        let (second, joined_cid, mut joined) = pending.register(SERVICE);
        assert!(!second);
        assert_eq!(cid, joined_cid);
        assert_eq!(pending.by_service[SERVICE].waiters.len(), 1);
        let (service, open) = pending.remove_by_cid(cid).unwrap();
        assert_eq!(service, SERVICE);
        open.complete(|| Ok(()));
        assert!(matches!(joined.try_recv(), Ok(Ok(()))));
        assert!(pending.is_empty());
    }

    #[test]
    fn cancellation_closes_receiver_before_pruning_and_preserves_other_waiters() {
        let pending = Mutex::new(PendingServiceOpens::default());
        let (_, cid, first) = pending.lock().register(SERVICE);
        let (_, _, mut second) = pending.lock().register(SERVICE);
        drop(PendingWaiter::new(first, || {
            pending.lock().prune(SERVICE, cid)
        }));
        assert_eq!(pending.lock().by_service[SERVICE].waiters.len(), 1);
        pending
            .lock()
            .remove(SERVICE, cid)
            .unwrap()
            .complete(|| Ok(()));
        assert!(matches!(second.try_recv(), Ok(Ok(()))));
        let (_, cid, last) = pending.lock().register(SERVICE);
        drop(PendingWaiter::new(last, || {
            pending.lock().prune(SERVICE, cid)
        }));
        assert!(pending.lock().is_empty());
    }

    #[test]
    fn stale_pruning_and_replies_cannot_remove_a_new_generation() {
        let mut pending = PendingServiceOpens::default();
        let (_, first_cid, first) = pending.register(SERVICE);
        pending.remove(SERVICE, first_cid).unwrap();
        let (_, next_cid, next) = pending.register(SERVICE);
        assert_ne!(first_cid, next_cid);
        drop(first);
        drop(next);
        pending.prune(SERVICE, first_cid);
        assert!(pending.remove(SERVICE, first_cid).is_none());
        assert!(pending.remove_by_cid(first_cid).is_none());
        assert!(pending.remove(SERVICE, next_cid).is_some());
    }

    #[tokio::test]
    async fn cancelling_the_wait_future_prunes_its_receiver() {
        let pending = Mutex::new(PendingServiceOpens::default());
        let (_, cid, receiver) = pending.lock().register(SERVICE);
        let mut waiting = Box::pin(
            PendingWaiter::new(receiver, || pending.lock().prune(SERVICE, cid)).wait_service_open(
                || panic!("cancellation does not time out"),
                || panic!("the attempt still owns its senders"),
            ),
        );
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        assert!(pending.lock().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn service_timeout_fails_every_waiter_and_allows_retry() {
        let pending = Mutex::new(PendingServiceOpens::default());
        let (_, cid, receiver) = pending.lock().register(SERVICE);
        let (_, _, mut joined) = pending.lock().register(SERVICE);
        let started = tokio::time::Instant::now();
        let result = PendingWaiter::new(receiver, || pending.lock().prune(SERVICE, cid))
            .wait_service_open(
                || pending.lock().remove(SERVICE, cid),
                || panic!("the attempt still owns its senders"),
            )
            .await;
        assert!(matches!(result, Err(BlpError::Timeout)));
        assert!(started.elapsed() >= SERVICE_OPEN_TIMEOUT);
        assert!(matches!(joined.try_recv(), Ok(Err(BlpError::Timeout))));
        assert!(pending.lock().is_empty());
        let (must_open, retry_cid, _retry) = pending.lock().register(SERVICE);
        assert!(must_open);
        assert_ne!(cid, retry_cid);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_timeout_and_guard_cannot_remove_a_new_generation() {
        let pending = Mutex::new(PendingServiceOpens::default());
        let (_, cid, receiver) = pending.lock().register(SERVICE);
        let old = pending.lock().remove(SERVICE, cid).unwrap();
        let (_, next_cid, mut next) = pending.lock().register(SERVICE);
        let result = PendingWaiter::new(receiver, || pending.lock().prune(SERVICE, cid))
            .wait_service_open(
                || pending.lock().remove(SERVICE, cid),
                || panic!("the old attempt still owns its senders"),
            )
            .await;
        assert!(matches!(result, Err(BlpError::Timeout)));
        pending
            .lock()
            .remove(SERVICE, next_cid)
            .unwrap()
            .complete(|| Ok(()));
        assert!(matches!(next.try_recv(), Ok(Ok(()))));
        drop(old);
    }

    #[tokio::test]
    async fn resolved_service_and_dropped_sender_keep_their_outcomes() {
        let pending = Mutex::new(PendingServiceOpens::default());
        let (_, cid, receiver) = pending.lock().register(SERVICE);
        pending.lock().remove(SERVICE, cid).unwrap().complete(|| {
            Err(BlpError::OpenService {
                service: SERVICE.to_string(),
                source: None,
                label: Some("service unavailable".to_string()),
            })
        });
        let result = PendingWaiter::new(receiver, || pending.lock().prune(SERVICE, cid))
            .wait_service_open(
                || panic!("resolved attempts do not time out"),
                || panic!("the attempt sent an outcome"),
            )
            .await;
        assert!(matches!(result, Err(BlpError::OpenService { .. })));

        let (_, cid, receiver) = pending.lock().register(SERVICE);
        pending.lock().remove(SERVICE, cid).unwrap();
        let result = PendingWaiter::new(receiver, || pending.lock().prune(SERVICE, cid))
            .wait_service_open(
                || panic!("closed attempts do not time out"),
                || BlpError::Internal {
                    detail: "worker-specific cancellation".to_string(),
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(BlpError::Internal { detail }) if detail == "worker-specific cancellation"
        ));
    }
}
