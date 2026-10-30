use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;
use std::{
    future::{Future, poll_fn},
    pin::pin,
};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use crossbeam_queue::ArrayQueue;
use serde_json::{Value, json};
use xbbg_async::engine::OverflowPolicy;
use xbbg_async::engine::state::{
    SubscriptionSender, SubscriptionUpdate, subscription_channel_with_policy,
};
use xbbg_async::subscription_consumer::SubscriptionConsumer;

use super::delivery_common::{Config, FIELDS, Payloads, WAIT, assert_payload, distribution, ns};

#[derive(Clone, Copy)]
enum Kind {
    Production,
    AsyncChannel,
    ArrayQueue,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Production => "production_drop_oldest_adapter",
            Self::AsyncChannel => "async_channel_force_send_primitive",
            Self::ArrayQueue => "crossbeam_array_queue_force_push_primitive",
        }
    }

    fn queue(self, capacity: usize) -> (Sender, Receiver) {
        match self {
            Self::Production => {
                let (tx, rx) =
                    subscription_channel_with_policy(capacity, OverflowPolicy::DropOldest);
                (
                    Sender::Production(tx),
                    Receiver::Production(Box::new(SubscriptionConsumer::from_receiver(rx))),
                )
            }
            Self::AsyncChannel => {
                let (tx, rx) = async_channel::bounded(capacity);
                (Sender::AsyncChannel(tx), Receiver::AsyncChannel(rx))
            }
            Self::ArrayQueue => {
                let queue = Arc::new(ArrayQueue::new(capacity));
                (
                    Sender::ArrayQueue(queue.clone()),
                    Receiver::ArrayQueue(queue),
                )
            }
        }
    }
}

#[derive(Clone)]
enum Sender {
    Production(SubscriptionSender),
    AsyncChannel(async_channel::Sender<SubscriptionUpdate>),
    ArrayQueue(Arc<ArrayQueue<SubscriptionUpdate>>),
}

impl Sender {
    // Every adapter takes identical prebuilt owned updates. In particular the
    // Option returned by force_send/force_push is destroyed here, not deferred
    // to setup/cleanup. Production destroys its eviction inside try_send.
    fn push(&self, update: SubscriptionUpdate) -> usize {
        let displaced = match self {
            Self::Production(tx) => {
                tx.try_send(Ok(update))
                    .expect("production overwrite enqueue");
                return 0; // Its authoritative eviction count is delivery.gap_before.
            }
            Self::AsyncChannel(tx) => tx.force_send(update).expect("async-channel enqueue"),
            Self::ArrayQueue(queue) => queue.force_push(update),
        };
        let evicted = usize::from(displaced.is_some());
        drop(displaced);
        evicted
    }
}

enum Receiver {
    // Queue setup is outside every measured phase; keep primitive variants small.
    Production(Box<SubscriptionConsumer>),
    AsyncChannel(async_channel::Receiver<SubscriptionUpdate>),
    ArrayQueue(Arc<ArrayQueue<SubscriptionUpdate>>),
}

struct Read {
    ids: Vec<usize>,
    gap: u64,
    read_ns: u64,
    output_drop_ns: u64,
}

fn assert_batch(batch: &RecordBatch) -> Vec<usize> {
    for field in FIELDS.into_iter().chain(["timestamp", "topic"]) {
        assert_eq!(
            batch
                .column_by_name(field)
                .expect("payload column")
                .null_count(),
            0
        );
    }
    let timestamp = batch
        .column_by_name("timestamp")
        .expect("timestamp")
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp micros");
    let topic = batch
        .column_by_name("topic")
        .expect("topic")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string topic");
    let seq = batch
        .column_by_name("SEQ")
        .expect("SEQ")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 SEQ");
    let price = batch
        .column_by_name("PRICE")
        .expect("PRICE")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float PRICE");
    let size = batch
        .column_by_name("SIZE")
        .expect("SIZE")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 SIZE");
    let active = batch
        .column_by_name("ACTIVE")
        .expect("ACTIVE")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool ACTIVE");
    let venue = batch
        .column_by_name("VENUE")
        .expect("VENUE")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string VENUE");
    let condition = batch
        .column_by_name("CONDITION")
        .expect("CONDITION")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string CONDITION");
    let date = batch
        .column_by_name("DATE")
        .expect("DATE")
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date DATE");
    let clock = batch
        .column_by_name("CLOCK")
        .expect("CLOCK")
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .expect("time CLOCK");
    (0..batch.num_rows())
        .map(|row| {
            let id = seq.value(row) as usize;
            assert_eq!(timestamp.value(row), id as i64);
            assert_eq!(topic.value(row), "SYNTH_DELIVERY_0");
            assert_eq!(price.value(row), id as f64 * 0.001);
            assert_eq!(size.value(row), (id % 10_000) as i32);
            assert_eq!(active.value(row), id.is_multiple_of(2));
            assert_eq!(venue.value(row), "SYNTHETIC_VENUE");
            assert_eq!(condition.value(row), format!("SYNTHETIC_CONDITION_{id:08}"));
            assert_eq!(date.value(row), 20_000 + (id % 31) as i32);
            assert_eq!(clock.value(row), id as i64 * 100);
            id
        })
        .collect()
}

impl Receiver {
    async fn drain(&self, expected: usize) -> Read {
        match self {
            Self::Production(consumer) => {
                let mut batches = Vec::new();
                let mut rows = 0;
                let mut gap = 0;
                let started = Instant::now();
                while rows < expected {
                    let delivery =
                        tokio::time::timeout(WAIT, consumer.next_delivery(expected - rows))
                            .await
                            .expect("production drain deadline")
                            .expect("production delivery")
                            .commit()
                            .expect("production delivery success");
                    assert!(delivery.data.num_rows() > 0);
                    rows += delivery.data.num_rows();
                    gap += delivery.gap_before.map_or(0, |gap| gap.dropped_updates);
                    batches.push(delivery);
                }
                let read_ns = ns(started);
                assert_eq!(rows, expected);
                let ids = batches
                    .iter()
                    .flat_map(|delivery| assert_batch(&delivery.data))
                    .collect();
                let started = Instant::now();
                drop(black_box(batches));
                Read {
                    ids,
                    gap,
                    read_ns,
                    output_drop_ns: ns(started),
                }
            }
            Self::AsyncChannel(_) | Self::ArrayQueue(_) => {
                let mut updates = Vec::with_capacity(expected);
                let started = Instant::now();
                for _ in 0..expected {
                    updates.push(match self {
                        Self::AsyncChannel(rx) => rx.try_recv().expect("primitive ready update"),
                        Self::ArrayQueue(queue) => queue.pop().expect("primitive ready update"),
                        Self::Production(_) => unreachable!(),
                    });
                }
                let read_ns = ns(started);
                let ids = updates
                    .iter()
                    .map(|update| {
                        assert_payload(update);
                        update.timestamp_us as usize
                    })
                    .collect();
                let started = Instant::now();
                drop(black_box(updates));
                Read {
                    ids,
                    gap: 0,
                    read_ns,
                    output_drop_ns: ns(started),
                }
            }
        }
    }
}

struct Worker {
    latencies: Vec<u64>,
    evicted: usize,
}

// IDs are logical workload IDs, not a fabricated global enqueue timestamp.
// With concurrent writers each writer's retained suffix must be contiguous and
// end at its final ID. Their inter-writer merge depends on real linearization.
fn validate_tail(ids: &[usize], first: usize, end: usize, producers: usize, capacity: usize) {
    assert_eq!(ids.len(), (end - first).min(capacity));
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    let mut last = vec![None; producers];
    for &id in ids {
        assert!((first..end).contains(&id));
        assert!(seen.insert(id), "duplicate survivor");
        let producer = (id - 1) % producers;
        if let Some(previous) = last[producer] {
            assert_eq!(id, previous + producers, "writer FIFO suffix");
        }
        last[producer] = Some(id);
    }
    for (producer, last) in last.into_iter().enumerate() {
        if let Some(last) = last {
            let final_id = end - 1 - ((end - 2 + producers - producer) % producers);
            assert_eq!(
                last, final_id,
                "retained writer must include its newest update"
            );
        }
    }
    if producers == 1 {
        assert!(
            ids.iter().copied().eq(end - ids.len()..end),
            "exact newest N in FIFO order"
        );
    }
}

async fn iteration(
    kind: Kind,
    producers: usize,
    burst: bool,
    config: &Config,
    sample: usize,
) -> (Value, Vec<u64>) {
    let payloads = Payloads::new(1);
    let phase_size = if burst {
        2 * config.capacity
    } else {
        config.updates
    };
    let phases = config.updates.div_ceil(phase_size);
    let mut plans: Vec<Vec<Vec<SubscriptionUpdate>>> = (0..producers)
        .map(|_| (0..phases).map(|_| Vec::new()).collect())
        .collect();
    for index in 0..config.updates {
        plans[index % producers][index / phase_size].push(payloads.full(index + 1, 0));
    }
    let (sender, receiver) = kind.queue(config.capacity);
    let ready = Arc::new(Barrier::new(producers + 1));
    let start = Arc::new(Barrier::new(producers + 1));
    let finish = Arc::new(Barrier::new(producers + 1));
    let mut workers = Vec::with_capacity(producers);
    for (producer, plan) in plans.into_iter().enumerate() {
        let sender = sender.clone();
        let ready = ready.clone();
        let start = start.clone();
        let finish = finish.clone();
        let sample_every = config.sample_every;
        let capacity = config.updates.div_ceil(sample_every) + 1;
        workers.push(std::thread::spawn(move || {
            let mut result = Worker {
                latencies: Vec::with_capacity(capacity),
                evicted: 0,
            };
            let mut index = 0;
            ready.wait();
            for phase in plan {
                start.wait();
                for update in phase {
                    if (index + sample + producer).is_multiple_of(sample_every) {
                        let started = Instant::now();
                        let evicted = sender.push(black_box(update));
                        let elapsed = ns(started);
                        result.latencies.push(elapsed);
                        result.evicted += evicted;
                    } else {
                        result.evicted += sender.push(black_box(update));
                    }
                    index += 1;
                }
                finish.wait();
            }
            result
        }));
    }
    // All threads have started and reserved their latency buffers before the
    // first timed release; scheduler startup must not inflate that phase.
    ready.wait();
    let mut phase_wall_ns = 0;
    let mut read_ns = 0;
    let mut output_drop_ns = 0;
    let mut gap = 0;
    let mut checksum = 0u64;
    let mut delivered = 0;
    for phase in 0..phases {
        let first = phase * phase_size + 1;
        let end = ((phase + 1) * phase_size).min(config.updates) + 1;
        let expected = (end - first).min(config.capacity);
        let started = Instant::now();
        start.wait();
        finish.wait();
        phase_wall_ns += ns(started);
        let read = receiver.drain(expected).await;
        validate_tail(&read.ids, first, end, producers, config.capacity);
        if matches!(kind, Kind::Production) {
            assert_eq!(
                read.gap,
                (end - first - expected) as u64,
                "exact production overflow gap"
            );
        }
        checksum = checksum.wrapping_add(read.ids.iter().map(|&id| id as u64).sum::<u64>());
        delivered += read.ids.len();
        read_ns += read.read_ns;
        output_drop_ns += read.output_drop_ns;
        gap += read.gap;
    }
    let workers: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("producer thread"))
        .collect();
    let evicted: usize = workers.iter().map(|worker| worker.evicted).sum();
    if !matches!(kind, Kind::Production) {
        assert_eq!(
            evicted,
            config.updates - delivered,
            "primitive eviction accounting"
        );
    }
    // Deterministic post-contention suffix establishes exact latest-N behavior
    // without pretending that workload IDs reveal concurrent enqueue order.
    let mut suffix_evictions = 0;
    for index in 0..2 * config.capacity {
        suffix_evictions += sender.push(payloads.full(config.updates + index + 1, 0));
    }
    let suffix = receiver.drain(config.capacity).await;
    if matches!(kind, Kind::Production) {
        assert_eq!(suffix.gap, config.capacity as u64);
    } else {
        assert_eq!(suffix.gap, 0);
        assert_eq!(suffix_evictions, config.capacity);
    }
    assert!(
        suffix
            .ids
            .iter()
            .copied()
            .eq(config.updates + config.capacity + 1..config.updates + 2 * config.capacity + 1)
    );
    let latencies = workers
        .into_iter()
        .flat_map(|worker| worker.latencies)
        .collect();
    (
        json!({
            "sample": sample, "attempted_updates": config.updates,
            "producer_phase_wall_ns": phase_wall_ns,
            "producer_updates_per_second": config.updates as f64 * 1e9 / phase_wall_ns as f64,
            "read_or_materialization_ns": read_ns, "read_output_drop_ns": output_drop_ns,
            "delivered_rows": delivered, "dropped_updates": config.updates - delivered,
            "production_gap_dropped_updates": matches!(kind, Kind::Production).then_some(gap),
            "primitive_evictions": (!matches!(kind, Kind::Production)).then_some(evicted),
            "survivor_checksum": checksum,
            "correctness": "all eight fields; unique survivors; per-writer newest contiguous FIFO suffix; exact single-writer newest-N; deterministic post-contention newest-N; exact eviction/gap accounting",
        }),
        latencies,
    )
}

struct WakeCounter {
    calls: AtomicU64,
    task: Waker,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.task.wake_by_ref();
    }
}

async fn registered_wake<F: Future>(
    waiting: F,
    sender: &Sender,
    update: SubscriptionUpdate,
    counter: &Arc<WakeCounter>,
) -> (F::Output, u64, u64) {
    let mut waiting = pin!(waiting);
    let waker = Waker::from(counter.clone());
    // Only the untimed idle registration bypasses cooperative scheduling:
    // Pending must mean a registered receive, not an exhausted task budget.
    assert!(
        pin!(tokio::task::coop::unconstrained(waiting.as_mut()))
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "idle receive future must register Pending"
    );
    let wakes_before = counter.calls.load(Ordering::Relaxed);
    let started = Instant::now();
    sender.push(update);
    let enqueue_ns = ns(started);
    let send_wakes = counter.calls.load(Ordering::Relaxed) - wakes_before;
    assert!(
        send_wakes > 0,
        "send must wake its registered receive future"
    );

    // A wake requests another poll; it does not promise Ready. Forward wakes
    // to Tokio so Pending (including cooperative yields) is executor-driven,
    // rather than repeatedly polling with a counter-only/no-op waker.
    let output = tokio::time::timeout(
        WAIT,
        poll_fn(|_| waiting.as_mut().poll(&mut Context::from_waker(&waker))),
    )
    .await
    .expect("woken receive future did not complete before deadline");
    (output, enqueue_ns, send_wakes)
}

async fn wake_iteration(kind: Kind, config: &Config, sample: usize) -> (Value, Vec<u64>) {
    let payloads = Payloads::new(1);
    let updates: Vec<_> = (1..=config.wakeups)
        .map(|id| payloads.full(id, 0))
        .collect();
    let (sender, receiver) = kind.queue(config.capacity);
    let task = poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
    let counter = Arc::new(WakeCounter {
        calls: AtomicU64::new(0),
        task,
    });
    let mut latencies = Vec::with_capacity(config.wakeups);
    let mut checksum = 0u64;
    let mut send_waker_calls = 0u64;
    let started = Instant::now();
    for update in updates {
        let expected = update.timestamp_us as u64;
        match &receiver {
            Receiver::Production(consumer) => {
                let (lease, enqueue_ns, send_wakes) =
                    registered_wake(consumer.next_delivery(1), &sender, update, &counter).await;
                latencies.push(enqueue_ns);
                send_waker_calls += send_wakes;
                let lease = lease.expect("woken production receiver must deliver an update");
                let delivery = lease.commit().expect("wake delivery");
                assert_eq!(delivery.data.num_rows(), 1);
                let seq = delivery
                    .data
                    .column_by_name("SEQ")
                    .expect("SEQ")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 SEQ");
                assert_eq!(seq.value(0) as u64, expected);
                assert!(delivery.gap_before.is_none());
                checksum += expected;
            }
            Receiver::AsyncChannel(rx) => {
                let (update, enqueue_ns, send_wakes) =
                    registered_wake(rx.recv(), &sender, update, &counter).await;
                latencies.push(enqueue_ns);
                send_waker_calls += send_wakes;
                let update = update.expect("woken async-channel receiver must deliver an update");
                assert_eq!(update.timestamp_us as u64, expected);
                checksum += expected;
            }
            Receiver::ArrayQueue(_) => unreachable!("bare ArrayQueue has no waiting/wakeup API"),
        }
    }
    let elapsed = ns(started);
    assert_eq!(checksum, (config.wakeups * (config.wakeups + 1) / 2) as u64);
    let wakes = counter.calls.load(Ordering::Relaxed);
    assert!(
        wakes >= config.wakeups as u64,
        "pending futures must actually receive wakes"
    );
    (
        json!({ "sample": sample, "cycles": config.wakeups, "wall_ns": elapsed,
        "waker_calls": wakes, "send_waker_calls": send_waker_calls, "checksum": checksum }),
        latencies,
    )
}

fn destruction_iteration(config: &Config, sample: usize) -> (Value, Vec<u64>) {
    let payloads = Payloads::new(1);
    let updates: Vec<_> = (1..=config.updates)
        .map(|id| payloads.full(id, 0))
        .collect();
    let mut latencies = Vec::with_capacity(config.updates.div_ceil(config.sample_every));
    let started = Instant::now();
    for (index, update) in updates.into_iter().enumerate() {
        if (index + sample).is_multiple_of(config.sample_every) {
            let started = Instant::now();
            drop(black_box(update));
            latencies.push(ns(started));
        } else {
            drop(black_box(update));
        }
    }
    (
        json!({ "sample": sample, "updates": config.updates, "wall_ns": ns(started) }),
        latencies,
    )
}

pub(crate) async fn run(config: &Config) -> Value {
    let mut cases = Vec::new();
    for burst in [false, true] {
        for producers in [1, config.producers] {
            // Rotate adapter order per iteration rather than always rewarding
            // one adapter with the same thermal/cache position.
            let kinds = [Kind::Production, Kind::AsyncChannel, Kind::ArrayQueue];
            let mut results: [Vec<Value>; 3] = std::array::from_fn(|_| Vec::new());
            let mut latencies: [Vec<u64>; 3] = std::array::from_fn(|_| Vec::new());
            for iteration_index in 0..config.warmup + config.samples {
                for offset in 0..3 {
                    let index = (offset + iteration_index) % 3;
                    let (result, sampled) =
                        iteration(kinds[index], producers, burst, config, iteration_index).await;
                    if iteration_index >= config.warmup {
                        results[index].push(result);
                        latencies[index].extend(sampled);
                    }
                }
            }
            for (index, kind) in kinds.into_iter().enumerate() {
                cases.push(json!({
                "implementation": kind.name(),
                "scenario": if burst { "burst_twice_capacity_then_drain" } else { "stalled_consumer_all_updates_then_drain" },
                "producers": producers, "consumers": 1,
                "sample_count": results[index].len(),
                "producer_latency": distribution(&latencies[index]),
                "samples": results[index],
            }));
            }
        }
    }
    let mut wakeups = Vec::new();
    for kind in [Kind::Production, Kind::AsyncChannel] {
        let mut results = Vec::new();
        let mut latencies = Vec::new();
        for iteration_index in 0..config.warmup + config.samples {
            let (result, sampled) = wake_iteration(kind, config, iteration_index).await;
            if iteration_index >= config.warmup {
                results.push(result);
                latencies.extend(sampled);
            }
        }
        wakeups.push(json!({ "implementation": kind.name(), "producer_latency": distribution(&latencies), "samples": results }));
    }
    let mut drops = Vec::new();
    let mut drop_latencies = Vec::new();
    for iteration_index in 0..config.warmup + config.samples {
        let (result, sampled) = destruction_iteration(config, iteration_index);
        if iteration_index >= config.warmup {
            drops.push(result);
            drop_latencies.extend(sampled);
        }
    }
    json!({
        "comparison_scope": "producer enqueue paths, NOT apples-to-apples complete subscription implementations: production includes mutex/continuity gap bookkeeping/Notify/terminal and receiver closure semantics; async-channel is a bounded MPMC primitive with its own wake/closure protocol but no subscription gap or terminal error; ArrayQueue is only a bounded queue and has no wait/wake/closure protocol",
        "setup_scope": "eight mixed scalar/Arc fields per update; all updates and uniquely owned condition strings prebuilt before timing; topic/layout/venue Arcs shared; ownership is moved exactly once, not cloned in timed loops; a separate readiness barrier excludes worker startup and latency-buffer allocation; joins excluded",
        "producer_timing_scope": "barrier release through all writers finishing each enqueue phase, including scheduler/barrier and sampled timestamp overhead; eviction payload destruction included for every implementation; no consumer read overlaps the phase; checks, queue construction, payload generation, deterministic suffix check and read/materialization excluded",
        "read_timing_scope": "reported separately and NOT compared as equivalent work: production next_delivery plus lease.commit builds Arrow and releases raw payloads; primitives only pop into a preallocated owned-update vector; full-field checks excluded; returned Arrow/update-container destruction separately reported",
        "concurrency_correctness": "multi-producer IDs encode per-writer issue order, not global enqueue order; observed survivor checksums may legitimately differ between adapters; each retained writer suffix must be contiguous and end at that writer's newest ID; a quiescent deterministic suffix additionally checks exact newest-N for every adapter",
        "cases": cases,
        "idle_wakeups": {
            "scope": "register the actual idle native/async receive future Pending with cooperative budgeting disabled only for that untimed registration; send once and await readiness with a per-cycle deadline, normally inline but with executor-driven repolls whenever Pending is returned; producer_latency measures enqueue plus counting/forwarding the registered wake, not receive completion or OS context switches; wall_ns additionally includes registration, per-cycle timeout setup, timestamps, any scheduling, receive/materialization, validation and output destruction; waker_calls counts calls to the counting receiver waker (send wakes and any receiver self-wakes), not timer or forwarded task-waker calls; send_waker_calls counts calls during enqueue, asserted nonzero for every cycle; ArrayQueue omitted because it has no notification API",
            "cases": wakeups,
        },
        "payload_destruction_control": {
            "scope": "same prebuilt owned eight-field update without any queue; final unique condition Arc free plus shared Arc decrements included; no allocation/setup timed; control is informative, not subtracted from queue latency",
            "drop_latency": distribution(&drop_latencies), "samples": drops,
        },
    })
}
