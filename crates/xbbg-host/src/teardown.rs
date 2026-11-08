//! Native teardown off host threads.
//!
//! Dropping the last owner of an engine signals shutdown, joins the engine's
//! Tokio runtime and stops its SDK sessions; each stop waits for in-flight
//! callbacks before the session is destroyed. That can take unbounded time and
//! the last owner may be released on any host thread (a JVM cleaner, a bridge
//! worker, a caller). Owners therefore hand such values to [`defer_teardown`],
//! which drops each one on its own native thread named `xbbg-teardown`.
//! Stop-before-destroy order is unchanged; destruction only happens later.
//!
//! Invariant: deferred values contain no host objects, so teardown threads
//! never attach to a host runtime.
//!
//! Hosts wait for outstanding teardown at exit with [`wait_idle`] or
//! [`wait_idle_timeout`]. Counting is per process: a forked child never waits
//! for its parent's teardown threads.

use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use xbbg_log::warn;

/// Name of every teardown thread.
pub const THREAD_NAME: &str = "xbbg-teardown";
const WAIT_LOG_INTERVAL: Duration = Duration::from_secs(5);

static TEARDOWN: Teardown = Teardown::new();

/// Drop `value` on a new `xbbg-teardown` thread instead of the caller's.
pub fn defer_teardown<T: Send + 'static>(value: T) {
    TEARDOWN.defer(value);
}

/// Block until no deferred drop is outstanding, logging every 5 s.
pub fn wait_idle() {
    TEARDOWN.wait_idle(None);
}

/// Block until no deferred drop is outstanding or `timeout` elapses.
/// Returns whether teardown finished.
pub fn wait_idle_timeout(timeout: Duration) -> bool {
    TEARDOWN.wait_idle(Some(timeout))
}

/// Deferred drops still running in this process.
pub fn pending() -> usize {
    TEARDOWN.pending()
}

/// Outstanding deferred drops, so callers can wait for them.
struct Teardown {
    pending: Mutex<Outstanding>,
    idle: Condvar,
}

/// Jobs started by one process. A forked child inherits the parent's count but
/// none of its teardown threads, so it never waits for (or counts) them.
struct Outstanding {
    jobs: usize,
    pid: u32,
}

impl Outstanding {
    fn owned_by_current_process(&self) -> bool {
        self.jobs > 0 && self.pid == std::process::id()
    }
}

impl Teardown {
    const fn new() -> Self {
        Self {
            pending: Mutex::new(Outstanding { jobs: 0, pid: 0 }),
            idle: Condvar::new(),
        }
    }

    /// One thread per value: a slow engine teardown never delays another.
    fn defer<T: Send + 'static>(&'static self, value: T) {
        let job = Job {
            _value: value,
            _pending: Pending::begin(self),
        };
        let spawned = std::thread::Builder::new()
            .name(THREAD_NAME.to_owned())
            .spawn(move || drop(job));
        if let Err(error) = spawned {
            // std drops a closure it cannot start, so the job was released here.
            warn!(
                error = %error,
                "native teardown thread unavailable; released on the calling thread"
            );
        }
    }

    /// Waits until idle; with a timeout, returns `false` if it elapsed first.
    fn wait_idle(&self, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut pending = self.lock();
        while pending.owned_by_current_process() {
            let slice = match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return false;
                    }
                    left.min(WAIT_LOG_INTERVAL)
                }
                None => WAIT_LOG_INTERVAL,
            };
            let (guard, wait) = self
                .idle
                .wait_timeout(pending, slice)
                .unwrap_or_else(PoisonError::into_inner);
            pending = guard;
            if wait.timed_out() && slice == WAIT_LOG_INTERVAL && pending.owned_by_current_process()
            {
                warn!(
                    pending = pending.jobs,
                    "waiting for native teardown; Bloomberg sessions are still stopping"
                );
            }
        }
        true
    }

    fn pending(&self) -> usize {
        let pending = self.lock();
        if pending.pid == std::process::id() {
            pending.jobs
        } else {
            0
        }
    }

    fn lock(&self) -> MutexGuard<'_, Outstanding> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One deferred drop. Fields drop in declaration order, so the value is gone
/// before the job stops counting as pending, even if its destructor panics.
struct Job<T> {
    _value: T,
    _pending: Pending,
}

/// Counts one job as pending from creation until drop.
struct Pending(&'static Teardown);

impl Pending {
    fn begin(teardown: &'static Teardown) -> Self {
        let mut pending = teardown.lock();
        let pid = std::process::id();
        if pending.pid != pid {
            // First job in this process (or in a forked child): any inherited
            // count belongs to threads that do not exist here.
            pending.jobs = 0;
            pending.pid = pid;
        }
        pending.jobs += 1;
        Self(teardown)
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let mut pending = self.0.lock();
        pending.jobs -= 1;
        if pending.jobs == 0 {
            self.0.idle.notify_all();
        }
    }
}

/// Owns a value whose drop may block on native teardown; dropping the owner
/// moves the value to a teardown thread.
pub struct DeferredDrop<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> DeferredDrop<T> {
    pub fn new(value: T) -> Self {
        Self(Some(value))
    }
}

impl<T: Send + 'static> Deref for DeferredDrop<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0
            .as_ref()
            .expect("the value is only taken while dropping")
    }
}

impl<T: Send + 'static> Drop for DeferredDrop<T> {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            defer_teardown(value);
        }
    }
}

/// Owns a value whose drop stays cheap only while its host lives.
///
/// While the host can be upgraded the value drops inline, with the host kept
/// alive until it has; releasing the host afterwards is the host's own concern
/// ([`DeferredDrop`] defers it). Once the host is gone the value's drop is
/// deferred too. A value without a host always drops inline.
pub struct HostedDrop<T: Send + 'static, H> {
    value: Option<T>,
    host: Option<Weak<H>>,
}

impl<T: Send + 'static, H> HostedDrop<T, H> {
    /// A value whose cheap drop depends on `host` staying alive.
    pub fn hosted(value: T, host: &Arc<H>) -> Self {
        Self {
            value: Some(value),
            host: Some(Arc::downgrade(host)),
        }
    }

    /// A value with no host; it always drops inline.
    pub fn unhosted(value: T) -> Self {
        Self {
            value: Some(value),
            host: None,
        }
    }
}

impl<T: Send + 'static, H> Deref for HostedDrop<T, H> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value
            .as_ref()
            .expect("the value is only taken while dropping")
    }
}

impl<T: Send + 'static, H> Drop for HostedDrop<T, H> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };
        match self.host.as_ref().map(Weak::upgrade) {
            None => drop(value),
            Some(Some(host)) => {
                drop(value);
                drop(host);
            }
            Some(None) => defer_teardown(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::thread::{self, ThreadId};

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(10);

    type DroppedOn = (ThreadId, Option<String>);

    /// Reports the thread that drops it.
    struct Probe {
        label: &'static str,
        dropped: mpsc::Sender<DroppedOn>,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            let current = thread::current();
            let _ = self
                .dropped
                .send((current.id(), current.name().map(str::to_owned)));
        }
    }

    fn probe(label: &'static str) -> (Probe, mpsc::Receiver<DroppedOn>) {
        let (dropped, on) = mpsc::channel();
        (Probe { label, dropped }, on)
    }

    fn assert_dropped_on_teardown_thread(on: &mpsc::Receiver<DroppedOn>) {
        let (dropped_on, name) = on.recv_timeout(TIMEOUT).expect("deferred drop finished");
        assert_ne!(dropped_on, thread::current().id());
        assert_eq!(name.as_deref(), Some(THREAD_NAME));
    }

    fn assert_dropped_inline(on: &mpsc::Receiver<DroppedOn>) {
        let (dropped_on, _) = on.try_recv().expect("dropped before returning");
        assert_eq!(dropped_on, thread::current().id());
    }

    /// Blocks its drop until released.
    struct Gate {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Drop for Gate {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    fn gate(executor: &'static Teardown) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered, started) = mpsc::channel();
        let (release, released) = mpsc::channel();
        executor.defer(Gate {
            entered,
            release: released,
        });
        started.recv_timeout(TIMEOUT).expect("teardown started");
        (started, release)
    }

    #[test]
    fn deferred_value_drops_on_a_named_teardown_thread() {
        static EXECUTOR: Teardown = Teardown::new();
        let (value, on) = probe("deferred");

        EXECUTOR.defer(value);

        assert_dropped_on_teardown_thread(&on);
        assert!(EXECUTOR.wait_idle(None));
        assert_eq!(EXECUTOR.pending(), 0);
    }

    #[test]
    fn wait_idle_blocks_until_a_deferred_drop_finishes() {
        static EXECUTOR: Teardown = Teardown::new();
        let (_started, release) = gate(&EXECUTOR);
        assert_eq!(EXECUTOR.pending(), 1);

        let (idle, became_idle) = mpsc::channel();
        let waiter = thread::spawn(move || {
            EXECUTOR.wait_idle(None);
            idle.send(()).expect("test still listening");
        });
        assert_eq!(
            became_idle.recv_timeout(Duration::from_millis(200)),
            Err(RecvTimeoutError::Timeout),
            "wait_idle returned while a drop was still running"
        );

        release.send(()).expect("gate still waiting");
        became_idle
            .recv_timeout(TIMEOUT)
            .expect("wait_idle returns once the drop finished");
        waiter.join().expect("waiter");
    }

    #[test]
    fn wait_with_timeout_reports_unfinished_teardown() {
        static EXECUTOR: Teardown = Teardown::new();
        let (_started, release) = gate(&EXECUTOR);

        let started = Instant::now();
        assert!(!EXECUTOR.wait_idle(Some(Duration::from_millis(100))));
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(!EXECUTOR.wait_idle(Some(Duration::ZERO)));

        release.send(()).expect("gate still waiting");
        assert!(EXECUTOR.wait_idle(Some(TIMEOUT)));
        assert!(EXECUTOR.wait_idle(Some(Duration::ZERO)));
    }

    #[test]
    fn concurrent_defers_are_all_drained_before_wait_idle_returns() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 16;
        static EXECUTOR: Teardown = Teardown::new();
        static DROPPED: AtomicUsize = AtomicUsize::new(0);

        struct Counted;

        impl Drop for Counted {
            fn drop(&mut self) {
                thread::sleep(Duration::from_millis(5));
                DROPPED.fetch_add(1, Ordering::SeqCst);
            }
        }

        let start = Arc::new(Barrier::new(THREADS));
        let producers: Vec<_> = (0..THREADS)
            .map(|_| {
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..PER_THREAD {
                        EXECUTOR.defer(Counted);
                    }
                })
            })
            .collect();
        for producer in producers {
            producer.join().expect("producer");
        }

        EXECUTOR.wait_idle(None);

        assert_eq!(DROPPED.load(Ordering::SeqCst), THREADS * PER_THREAD);
    }

    #[test]
    fn deferred_owner_derefs_and_defers_its_last_release() {
        let (value, on) = probe("engine");
        let owner = Arc::new(DeferredDrop::new(value));
        let future_clone = Arc::clone(&owner);
        assert_eq!(future_clone.label, "engine");

        drop(owner);
        assert_eq!(on.try_recv(), Err(mpsc::TryRecvError::Empty));
        drop(future_clone);

        assert_dropped_on_teardown_thread(&on);
    }

    /// Holds the only outside reference to its host while being dropped and
    /// reports whether the host outlived that release.
    struct HostHolder {
        host: Option<Arc<DeferredDrop<Probe>>>,
        weak: Weak<DeferredDrop<Probe>>,
        host_alive: mpsc::Sender<bool>,
    }

    impl Drop for HostHolder {
        fn drop(&mut self) {
            drop(self.host.take());
            let _ = self.host_alive.send(self.weak.upgrade().is_some());
        }
    }

    #[test]
    fn hosted_value_keeps_its_host_alive_while_dropping_inline() {
        let (host_value, host_on) = probe("host");
        let host = Arc::new(DeferredDrop::new(host_value));
        let (host_alive, observed) = mpsc::channel();
        let value = HostedDrop::hosted(
            HostHolder {
                host: Some(Arc::clone(&host)),
                weak: Arc::downgrade(&host),
                host_alive,
            },
            &host,
        );
        drop(host);

        drop(value);

        assert!(observed.try_recv().expect("value dropped inline"));
        assert_dropped_on_teardown_thread(&host_on);
    }

    #[test]
    fn hosted_value_is_deferred_once_its_host_is_released() {
        let (host_value, host_on) = probe("host");
        let host = Arc::new(DeferredDrop::new(host_value));
        let (value, on) = probe("consumer");
        let value = HostedDrop::hosted(value, &host);
        assert_eq!(value.label, "consumer");
        drop(host);
        assert_dropped_on_teardown_thread(&host_on);

        drop(value);

        assert_dropped_on_teardown_thread(&on);
    }

    #[test]
    fn unhosted_value_drops_inline() {
        let (value, on) = probe("fixture");

        drop(HostedDrop::<_, ()>::unhosted(value));

        assert_dropped_inline(&on);
    }
}
