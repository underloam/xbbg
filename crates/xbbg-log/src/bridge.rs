//! Opt-in capture of log events into a bounded queue for a foreign-language consumer.
//!
//! The bridge is language-neutral: it never touches Python (or any other
//! runtime). A host binding enables it, then runs its own consumer thread that
//! calls [`next_batch`] and forwards the [`LogRecord`]s it receives.
//!
//! # Guarantees for emitting threads
//!
//! ```text
//! tracing::warn!("...")
//!   → AtomicLevelFilter (one Relaxed atomic load)
//!   → BridgeFilter      (one Relaxed atomic load)
//!   → BridgeLayer       (format message, try-push into the bounded queue)
//! ```
//!
//! - The emitting thread never waits for queue capacity: when the queue is
//!   full the record is dropped and counted (see [`dropped_total`]).
//! - Emitting threads take no lock. The queue is a lock-free bounded
//!   [`ArrayQueue`], reached through an [`ArcSwapOption`] load. The only
//!   other work a push can cause is waking the consumer: when it is asleep
//!   (or about to sleep), the emitter briefly takes the consumer's wake-up
//!   mutex and notifies its condition variable; a consumer that is busy
//!   draining costs emitters nothing.
//! - The queue's memory (one slot per record of capacity) is reserved when
//!   the bridge is enabled.
//! - While the bridge is enabled, events go to the queue **instead of**
//!   stderr; while it is disabled, the stderr path is unchanged.
//!
//! # Lifecycle
//!
//! 1. [`enable`] opens the queue and starts capture.
//! 2. The consumer loops on [`next_batch`].
//! 3. [`disable`] stops capture and wakes the consumer. The consumer keeps
//!    receiving what is already queued; the batch that empties the queue has
//!    [`LogBatch::closed`] set and the consumer should exit.
//! 4. A consumer that is stuck (or gone) leaves records behind;
//!    [`discard_queued`] drops them and counts them as dropped.
//!
//! There is a single queue, so there must be a single consumer at a time.
//!
//! [`ArrayQueue`]: crossbeam_queue::ArrayQueue
//! [`ArcSwapOption`]: arc_swap::ArcSwapOption

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwapOption;
use crossbeam_queue::ArrayQueue;
use tracing::{Event, Subscriber};
use tracing_core::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Filter, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::Level;

/// One captured log event, owned and independent of the emitting thread.
///
/// `target`, `file` and `line` come from the event's static callsite metadata,
/// so capture does not copy them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Event message followed by its fields as ` name=value` pairs, formatted
    /// like the message part of the stderr line.
    pub message: String,
    /// Event level.
    pub level: Level,
    /// Event target, e.g. `xbbg_async::engine` or `xbbg.sdk`.
    pub target: &'static str,
    /// Source file of the callsite, when known.
    pub file: Option<&'static str>,
    /// Source line of the callsite, when known.
    pub line: Option<u32>,
    /// Wall-clock time at which the event was emitted.
    pub timestamp: SystemTime,
    /// Name of the emitting thread; `ThreadId(n)` for unnamed threads.
    pub thread_name: String,
}

/// What [`next_batch`] returns.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogBatch {
    /// Records in emission order; empty when the wait timed out.
    pub records: Vec<LogRecord>,
    /// Records dropped (queue full, or discarded) since the previous batch.
    pub dropped: u64,
    /// The bridge is disabled and the queue is empty: nothing more will arrive
    /// until the next [`enable`].
    pub closed: bool,
}

/// One enabled run of the bridge: its queue and whether capture has stopped.
struct Channel {
    queue: ArrayQueue<LogRecord>,
    /// Capture was switched off; the queue may still hold records to drain.
    closed: AtomicBool,
}

/// The current channel. It stays published after [`disable`] so the consumer
/// can drain it, and is replaced by the next [`enable`].
static CHANNEL: ArcSwapOption<Channel> = ArcSwapOption::const_empty();
/// Serializes [`enable`], [`disable`] and [`discard_queued`] with one another.
/// Emitting threads never take it.
static CONTROL: Mutex<()> = Mutex::new(());
/// The consumer's wake-up mutex and condition variable. Emitters touch them
/// only when [`SLEEPING`] is set.
static WAKE: Mutex<()> = Mutex::new(());
static READY: Condvar = Condvar::new();
/// Set by the consumer when it is asleep or about to sleep.
static SLEEPING: AtomicBool = AtomicBool::new(false);
/// Mirrors "a channel is open" so the filters read it without loading the channel.
static ENABLED: AtomicBool = AtomicBool::new(false);
/// Records dropped since the process started.
static DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Records dropped since the last batch was handed to the consumer.
static DROPPED_UNREPORTED: AtomicU64 = AtomicU64::new(0);

fn lock(mutex: &'static Mutex<()>) -> std::sync::MutexGuard<'static, ()> {
    // The guarded data is `()`, so a poisoned lock is still usable.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn add_dropped(count: u64) {
    DROPPED_TOTAL.fetch_add(count, Ordering::Relaxed);
    DROPPED_UNREPORTED.fetch_add(count, Ordering::Relaxed);
}

/// Pop everything from `channel` and count it as dropped; returns how many.
fn discard_all(channel: &Channel) -> usize {
    let mut count = 0;
    while channel.queue.pop().is_some() {
        count += 1;
    }
    if count > 0 {
        add_dropped(count as u64);
    }
    count
}

/// Wake the consumer whether or not it announced that it sleeps; for control
/// operations, which are not on the emit path.
fn wake_consumer() {
    drop(lock(&WAKE));
    READY.notify_all();
}

/// Whether capture is active.
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Total number of records dropped since the process started, whether the
/// queue was full or leftovers were discarded.
pub fn dropped_total() -> u64 {
    DROPPED_TOTAL.load(Ordering::Relaxed)
}

/// Start capturing into a queue that holds at most `capacity` records
/// (a capacity of 0 is treated as 1).
///
/// Returns `true` if capture was started, `false` if it was already active; in
/// that case the existing queue and its capacity are left untouched. Records
/// left over from a previous run that nobody drained are discarded (and counted
/// as dropped) before the new queue opens.
pub fn enable(capacity: usize) -> bool {
    let _control = lock(&CONTROL);
    if ENABLED.load(Ordering::Relaxed) {
        return false;
    }
    if let Some(previous) = CHANNEL.load().as_ref() {
        discard_all(previous);
    }
    CHANNEL.store(Some(Arc::new(Channel {
        queue: ArrayQueue::new(capacity.max(1)),
        closed: AtomicBool::new(false),
    })));
    ENABLED.store(true, Ordering::Relaxed);
    true
}

/// Stop capturing and wake a waiting consumer.
///
/// Already queued records stay available to [`next_batch`]; once they are
/// consumed the consumer receives a batch with [`LogBatch::closed`] set.
/// Events emitted from now on go to stderr again.
pub fn disable() {
    let _control = lock(&CONTROL);
    ENABLED.store(false, Ordering::Relaxed);
    if let Some(channel) = CHANNEL.load().as_ref() {
        channel.closed.store(true, Ordering::SeqCst);
    }
    fence(Ordering::SeqCst);
    wake_consumer();
}

/// Drop everything still queued and count it as dropped; returns how many.
///
/// For a consumer that did not finish draining after [`disable`].
pub fn discard_queued() -> usize {
    let _control = lock(&CONTROL);
    let count = CHANNEL
        .load()
        .as_ref()
        .map_or(0, |channel| discard_all(channel));
    wake_consumer();
    count
}

/// Count `count` records that the consumer took but did not deliver, e.g.
/// the rest of a batch it abandoned. They are reported like any other drop.
pub fn count_dropped(count: u64) {
    if count > 0 {
        add_dropped(count);
    }
}

/// Wait up to `timeout` for queued records and take at most `max_records`.
///
/// Returns as soon as records are available, or when the bridge is disabled
/// and the queue is empty (`closed`), or when the timeout passes (empty
/// `records`). `dropped` counts records lost since the previous batch and is
/// reported even when `records` is empty, so a consumer learns about drops
/// without a later record arriving. `max_records` of 0 is treated as 1.
///
/// Blocks the calling thread, and must be called from one consumer thread at
/// a time. Emitting threads never wait for it.
pub fn next_batch(timeout: Duration, max_records: usize) -> LogBatch {
    let deadline = Instant::now().checked_add(timeout);
    let max_records = max_records.max(1);
    loop {
        if let Some(batch) = try_take(max_records) {
            return batch;
        }
        let remaining = match deadline {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(3600),
        };
        if remaining.is_zero() {
            return LogBatch {
                records: Vec::new(),
                dropped: DROPPED_UNREPORTED.swap(0, Ordering::Relaxed),
                closed: false,
            };
        }

        // Sleep protocol. Consumer: announce, fence, re-check, wait.
        // Emitter: push, fence, read the announcement, notify. Because of the
        // two fences either the re-check sees the record (or the closed flag)
        // or the emitter sees `SLEEPING`; and the emitter's notify takes
        // `WAKE`, which the consumer holds from announcing until `wait` has
        // released it, so the notify cannot fall between re-check and wait.
        let guard = lock(&WAKE);
        SLEEPING.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some(batch) = try_take(max_records) {
            SLEEPING.store(false, Ordering::SeqCst);
            return batch;
        }
        drop(
            READY
                .wait_timeout(guard, remaining)
                .unwrap_or_else(PoisonError::into_inner),
        );
        SLEEPING.store(false, Ordering::SeqCst);
    }
}

/// Take up to `max_records`; `None` when there is nothing to report yet
/// (no records and capture not closed).
fn try_take(max_records: usize) -> Option<LogBatch> {
    let channel = CHANNEL.load();
    let mut records = Vec::new();
    let closed = match channel.as_ref() {
        // Never enabled: nothing will arrive.
        None => true,
        Some(channel) => {
            // Read the flag first: records pushed before capture stopped are
            // then all visible to the emptiness check below.
            let closed = channel.closed.load(Ordering::SeqCst);
            while records.len() < max_records {
                match channel.queue.pop() {
                    Some(record) => records.push(record),
                    None => break,
                }
            }
            closed && channel.queue.is_empty()
        }
    };
    if records.is_empty() && !closed {
        return None;
    }
    Some(LogBatch {
        records,
        dropped: DROPPED_UNREPORTED.swap(0, Ordering::Relaxed),
        closed,
    })
}

/// Try to queue a record without taking a lock; a full queue drops it and
/// counts it.
fn push(record: LogRecord) {
    let channel = CHANNEL.load();
    let Some(channel) = channel.as_ref() else {
        return;
    };
    if channel.closed.load(Ordering::Relaxed) {
        // Capture was switched off after the filter ran. A record that slips
        // in between this check and the push is discarded, and counted, by
        // the next `enable` or `discard_queued`.
        return;
    }
    if channel.queue.push(record).is_err() {
        add_dropped(1);
        return;
    }
    fence(Ordering::SeqCst);
    // Only the first emitter after the consumer went to sleep notifies.
    if SLEEPING.swap(false, Ordering::SeqCst) {
        drop(lock(&WAKE));
        READY.notify_one();
    }
}

// ---------------------------------------------------------------------------
// Filters and layer
// ---------------------------------------------------------------------------

/// Per-layer filter of the stderr layer: the level filter, but only while the
/// bridge is off.
pub(crate) struct StderrFilter;

impl<S> Filter<S> for StderrFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &tracing_core::Metadata<'_>, cx: &Context<'_, S>) -> bool {
        !is_enabled() && crate::AtomicLevelFilter.enabled(meta, cx)
    }

    fn max_level_hint(&self) -> Option<tracing_core::LevelFilter> {
        Some(tracing_core::LevelFilter::TRACE)
    }
}

/// Per-layer filter of the bridge layer: the level filter, but only while the
/// bridge is on.
pub(crate) struct BridgeFilter;

impl<S> Filter<S> for BridgeFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &tracing_core::Metadata<'_>, cx: &Context<'_, S>) -> bool {
        is_enabled() && crate::AtomicLevelFilter.enabled(meta, cx)
    }

    fn max_level_hint(&self) -> Option<tracing_core::LevelFilter> {
        Some(tracing_core::LevelFilter::TRACE)
    }
}

/// Layer that turns events into [`LogRecord`]s and queues them.
pub(crate) struct BridgeLayer;

impl<S: Subscriber> Layer<S> for BridgeLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        let thread = std::thread::current();
        let thread_name = match thread.name() {
            Some(name) => name.to_owned(),
            None => format!("{:?}", thread.id()),
        };

        push(LogRecord {
            message: visitor.buf,
            level: *meta.level(),
            target: meta.target(),
            file: meta.file(),
            line: meta.line(),
            timestamp: SystemTime::now(),
            thread_name,
        });
    }
}

/// Formats the message and fields the way `tracing_subscriber::fmt`'s default
/// field formatter does: the message as is, then ` name=value` with `Debug`
/// values (so strings are quoted).
#[derive(Default)]
struct MessageVisitor {
    buf: String,
}

impl MessageVisitor {
    fn separate(&mut self) {
        if !self.buf.is_empty() {
            self.buf.push(' ');
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.separate();
        // Writing into a `String` cannot fail.
        let _ = match field.name() {
            "message" => write!(self.buf, "{value:?}"),
            name => write!(
                self.buf,
                "{}={value:?}",
                name.strip_prefix("r#").unwrap_or(name)
            ),
        };
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, MutexGuard};
    use std::thread;

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::{TEST_LOCK, build_subscriber, set_level};

    /// Serializes tests that use the process-global bridge and level; resets
    /// both on entry and exit.
    struct Guard(#[allow(dead_code)] MutexGuard<'static, ()>);

    impl Guard {
        fn new() -> Self {
            let guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
            reset();
            Guard(guard)
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            reset();
        }
    }

    fn reset() {
        disable();
        discard_queued();
        // Take any pending drop report so the next test starts clean.
        next_batch(Duration::ZERO, 1);
        set_level(Level::WARN);
    }

    #[derive(Clone, Default)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl SharedBuf {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuf {
        type Writer = SharedBuf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn captures_formatted_fields_and_metadata() {
        let _guard = Guard::new();
        set_level(Level::TRACE);
        let stderr = SharedBuf::default();
        let before = SystemTime::now();
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(stderr.clone()), || {
            let expected_line = line!() + 1;
            crate::warn!(ticker = "TEST Equity", count = 3, "refused {}", "security");
            let batch = next_batch(Duration::from_secs(5), 16);
            assert_eq!(batch.records.len(), 1);
            let record = &batch.records[0];
            assert_eq!(
                record.message,
                "refused security ticker=\"TEST Equity\" count=3"
            );
            assert_eq!(record.level, Level::WARN);
            assert_eq!(record.target, module_path!());
            assert_eq!(record.file, Some(file!()));
            assert_eq!(record.line, Some(expected_line));
            assert!(record.timestamp >= before && record.timestamp <= SystemTime::now());
            assert_eq!(
                record.thread_name,
                thread::current().name().expect("test threads are named")
            );
            assert_eq!(batch.dropped, 0);
            assert!(!batch.closed);
        });
        assert_eq!(stderr.contents(), "", "bridge replaces stderr");
    }

    #[test]
    fn explicit_target_and_levels() {
        let _guard = Guard::new();
        set_level(Level::TRACE);
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::trace!(target: "xbbg.sdk", "t");
            crate::debug!(target: "xbbg.sdk", "d");
            crate::info!(target: "xbbg.sdk", "i");
            crate::warn!(target: "xbbg.sdk", "w");
            crate::error!(target: "xbbg.sdk", category = "x", "e");
        });
        let batch = next_batch(Duration::ZERO, 16);
        let seen: Vec<_> = batch
            .records
            .iter()
            .map(|r| (r.level, r.target, r.message.as_str()))
            .collect();
        assert_eq!(
            seen,
            vec![
                (Level::TRACE, "xbbg.sdk", "t"),
                (Level::DEBUG, "xbbg.sdk", "d"),
                (Level::INFO, "xbbg.sdk", "i"),
                (Level::WARN, "xbbg.sdk", "w"),
                (Level::ERROR, "xbbg.sdk", "e category=\"x\""),
            ]
        );
    }

    #[test]
    fn level_filter_runs_before_capture() {
        let _guard = Guard::new();
        set_level(Level::WARN);
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::info!("below threshold");
            crate::warn!("at threshold");
        });
        let batch = next_batch(Duration::ZERO, 16);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].message, "at threshold");
    }

    #[test]
    fn full_queue_drops_and_counts() {
        let _guard = Guard::new();
        assert!(enable(2));
        let total_before = dropped_total();
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            for i in 0..5 {
                crate::error!("record {i}");
            }
        });
        assert_eq!(dropped_total() - total_before, 3);

        let batch = next_batch(Duration::ZERO, 16);
        let messages: Vec<_> = batch.records.iter().map(|r| r.message.as_str()).collect();
        assert_eq!(
            messages,
            ["record 0", "record 1"],
            "newest records are dropped"
        );
        assert_eq!(batch.dropped, 3);

        // The drop report is per batch; the running total keeps counting.
        let batch = next_batch(Duration::ZERO, 16);
        assert_eq!(batch.dropped, 0);
        assert_eq!(dropped_total() - total_before, 3);
    }

    #[test]
    fn drops_are_reported_without_a_later_record() {
        let _guard = Guard::new();
        assert!(enable(1));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("kept");
            crate::error!("dropped");
        });
        let first = next_batch(Duration::ZERO, 16);
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.dropped, 1);

        // Drops that happen while the queue is empty-but-full-then-drained
        // are still reported by a timed-out batch.
        add_dropped(2);
        let empty = next_batch(Duration::from_millis(10), 16);
        assert!(empty.records.is_empty());
        assert_eq!(empty.dropped, 2);
        assert!(!empty.closed);
    }

    #[test]
    fn disabled_queues_nothing_and_uses_stderr() {
        let _guard = Guard::new();
        set_level(Level::TRACE);
        let stderr = SharedBuf::default();
        tracing::subscriber::with_default(build_subscriber(stderr.clone()), || {
            crate::warn!(ticker = "TEST Equity", "kept on stderr");
        });
        let output = stderr.contents();
        assert!(output.contains("WARN"), "{output}");
        assert!(output.contains(module_path!()), "{output}");
        assert!(
            output.contains("kept on stderr ticker=\"TEST Equity\""),
            "{output}"
        );
        assert!(output.ends_with('\n'));

        // Nothing was captured while disabled, even once enabled afterwards.
        assert!(enable(16));
        let batch = next_batch(Duration::ZERO, 16);
        assert!(batch.records.is_empty());
        assert_eq!(batch.dropped, 0);
    }

    #[test]
    fn disable_restores_stderr_without_duplicates() {
        let _guard = Guard::new();
        let stderr = SharedBuf::default();
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(stderr.clone()), || {
            crate::error!("while enabled");
            disable();
            crate::error!("after disable");
        });
        let output = stderr.contents();
        assert!(!output.contains("while enabled"), "{output}");
        assert!(output.contains("after disable"), "{output}");
        let batch = next_batch(Duration::ZERO, 16);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].message, "while enabled");
        assert!(batch.closed);
    }

    #[test]
    fn enable_is_idempotent_and_keeps_capacity() {
        let _guard = Guard::new();
        assert!(enable(1));
        assert!(!enable(100));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("a");
            crate::error!("b");
        });
        let batch = next_batch(Duration::ZERO, 16);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.dropped, 1);
    }

    #[test]
    fn disable_drains_queue_then_closes() {
        let _guard = Guard::new();
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            for i in 0..3 {
                crate::error!("record {i}");
            }
        });
        disable();

        let first = next_batch(Duration::from_secs(5), 2);
        assert_eq!(first.records.len(), 2);
        assert!(!first.closed, "one record is still queued");
        let second = next_batch(Duration::from_secs(5), 2);
        assert_eq!(second.records.len(), 1);
        assert!(second.closed);
        let third = next_batch(Duration::from_secs(5), 2);
        assert!(third.records.is_empty());
        assert!(third.closed);
    }

    #[test]
    fn disable_unblocks_a_waiting_consumer() {
        let _guard = Guard::new();
        assert!(enable(16));
        let consumer = thread::spawn(|| {
            let started = Instant::now();
            let batch = next_batch(Duration::from_secs(60), 16);
            (batch, started.elapsed())
        });
        // Give the consumer time to start waiting; correctness does not depend
        // on it (a consumer that starts late sees the closed queue at once).
        thread::sleep(Duration::from_millis(50));
        disable();
        let (batch, waited) = consumer.join().unwrap();
        assert!(batch.closed);
        assert!(batch.records.is_empty());
        assert!(waited < Duration::from_secs(30), "waited {waited:?}");
    }

    #[test]
    fn record_unblocks_a_waiting_consumer() {
        let _guard = Guard::new();
        assert!(enable(16));
        let consumer = thread::spawn(|| next_batch(Duration::from_secs(60), 16));
        thread::sleep(Duration::from_millis(50));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("wake up");
        });
        let batch = consumer.join().unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].message, "wake up");
    }

    #[test]
    fn discard_queued_counts_leftovers() {
        let _guard = Guard::new();
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("one");
            crate::error!("two");
        });
        disable();
        let total_before = dropped_total();
        assert_eq!(discard_queued(), 2);
        assert_eq!(dropped_total() - total_before, 2);
        let batch = next_batch(Duration::ZERO, 16);
        assert!(batch.records.is_empty());
        assert_eq!(batch.dropped, 2);
        assert!(batch.closed);
    }

    #[test]
    fn consumer_can_count_undelivered_records() {
        let _guard = Guard::new();
        let total_before = dropped_total();
        count_dropped(0);
        count_dropped(4);
        assert_eq!(dropped_total() - total_before, 4);
        assert_eq!(next_batch(Duration::ZERO, 1).dropped, 4);
    }

    #[test]
    fn enable_discards_unconsumed_leftovers() {
        let _guard = Guard::new();
        assert!(enable(16));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("old");
        });
        disable();
        let total_before = dropped_total();
        assert!(enable(16));
        assert_eq!(dropped_total() - total_before, 1);
        let batch = next_batch(Duration::ZERO, 16);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn emitting_does_not_wait_for_a_stalled_consumer() {
        let _guard = Guard::new();
        assert!(enable(4));
        let total_before = dropped_total();
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            for i in 0..1000 {
                crate::error!("burst {i}");
            }
        });
        assert_eq!(dropped_total() - total_before, 996);
    }

    /// Emit `per_thread` records from each of `threads` threads, named
    /// `t{thread}-{index}`.
    fn emit_concurrently(threads: usize, per_thread: usize) -> Vec<thread::JoinHandle<()>> {
        (0..threads)
            .map(|id| {
                thread::spawn(move || {
                    tracing::subscriber::with_default(
                        build_subscriber(SharedBuf::default()),
                        || {
                            for index in 0..per_thread {
                                crate::error!("t{id}-{index}");
                            }
                        },
                    );
                })
            })
            .collect()
    }

    #[test]
    fn concurrent_emitters_deliver_every_record_exactly_once() {
        const THREADS: usize = 4;
        const PER_THREAD: usize = 5_000;
        let _guard = Guard::new();
        assert!(enable(THREADS * PER_THREAD));
        let total_before = dropped_total();

        let emitters = emit_concurrently(THREADS, PER_THREAD);
        let mut messages = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        while messages.len() < THREADS * PER_THREAD && Instant::now() < deadline {
            let batch = next_batch(Duration::from_millis(100), 64);
            messages.extend(batch.records.into_iter().map(|r| r.message));
        }
        for emitter in emitters {
            emitter.join().unwrap();
        }

        assert_eq!(messages.len(), THREADS * PER_THREAD);
        assert_eq!(dropped_total(), total_before);
        // Each emitter's records arrive once and in its own order.
        let mut next = [0usize; THREADS];
        for message in &messages {
            let (id, index) = message[1..].split_once('-').unwrap();
            let id: usize = id.parse().unwrap();
            assert_eq!(index.parse::<usize>().unwrap(), next[id], "{message}");
            next[id] += 1;
        }
        assert_eq!(next, [PER_THREAD; THREADS]);
    }

    #[test]
    fn concurrent_emitters_delivered_plus_dropped_equals_emitted() {
        const THREADS: usize = 4;
        const PER_THREAD: usize = 5_000;
        let _guard = Guard::new();
        assert!(enable(8));
        let total_before = dropped_total();

        let delivered = Arc::new(AtomicUsize::new(0));
        let consumer = {
            let delivered = delivered.clone();
            thread::spawn(move || {
                loop {
                    let batch = next_batch(Duration::from_millis(50), 4);
                    delivered.fetch_add(batch.records.len(), Ordering::Relaxed);
                    if batch.closed {
                        return;
                    }
                }
            })
        };
        for emitter in emit_concurrently(THREADS, PER_THREAD) {
            emitter.join().unwrap();
        }
        disable();
        consumer.join().unwrap();

        let dropped = (dropped_total() - total_before) as usize;
        assert_eq!(
            delivered.load(Ordering::Relaxed) + dropped,
            THREADS * PER_THREAD
        );
        assert!(
            dropped > 0,
            "a capacity of 8 should overflow under this burst"
        );
    }

    #[test]
    fn a_sleeping_consumer_wakes_promptly_for_one_record() {
        let _guard = Guard::new();
        assert!(enable(16));
        let consumer = thread::spawn(|| {
            let batch = next_batch(Duration::from_secs(60), 16);
            (batch, Instant::now())
        });
        // Let the consumer go to sleep; a consumer that is late finds the
        // record without sleeping, which is fine too.
        thread::sleep(Duration::from_millis(100));
        let pushed = Instant::now();
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            crate::error!("wake");
        });
        let (batch, returned) = consumer.join().unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(
            returned.duration_since(pushed) < Duration::from_secs(5),
            "woke after {:?}",
            returned.duration_since(pushed)
        );
    }

    #[test]
    fn ping_pong_never_waits_for_the_timeout() {
        const ROUNDS: usize = 300;
        let _guard = Guard::new();
        assert!(enable(16));
        let (ack_tx, ack_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let consumer = thread::spawn(move || {
            for _ in 0..ROUNDS {
                // A lost wake-up would cost the whole 10 s timeout.
                let batch = next_batch(Duration::from_secs(10), 16);
                assert_eq!(batch.records.len(), 1);
                ack_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            }
        });
        let started = Instant::now();
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            for _ in 0..ROUNDS {
                crate::error!("ping");
                ack_rx.recv_timeout(Duration::from_secs(30)).unwrap();
                go_tx.send(()).unwrap();
            }
        });
        consumer.join().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn re_enable_at_another_capacity_replaces_the_queue() {
        let _guard = Guard::new();
        assert!(enable(1));
        disable();
        assert!(enable(3));
        tracing::subscriber::with_default(build_subscriber(SharedBuf::default()), || {
            for i in 0..4 {
                crate::error!("record {i}");
            }
        });
        let batch = next_batch(Duration::ZERO, 16);
        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.dropped, 1);
        assert!(!batch.closed);
    }
}
