//! Subscription read latency by batch limit.
//!
//! A producer thread enqueues sparse `SubscriptionUpdate`s on a fixed schedule
//! into the production subscription channel. The consumer runs the read path
//! the bindings use (`receive_subscription_updates` with a batch limit, then
//! `SubscriptionArrowBatcher`) and records, for every update, the time from
//! enqueue to its RecordBatch being ready. No Bloomberg session, no Python/JS.
//!
//! Run:
//!   LATENCY_LIMITS=1,16,64,1024 cargo bench --package xbbg-bench --bench subscription_read_latency

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionArrowBatcher, SubscriptionUpdate, UpdateField,
    UpdateValue, subscription_channel,
};
use xbbg_async::subscription_consumer::{
    PendingUpdates, SubscriptionRead, receive_subscription_updates,
    subscription_batch_capacity_hint,
};
use xbbg_bench::write_json;

const FIELDS: [(&str, FieldKind); 8] = [
    ("LAST_PRICE", FieldKind::F64),
    ("BID", FieldKind::F64),
    ("ASK", FieldKind::F64),
    ("BID_SIZE", FieldKind::I32),
    ("ASK_SIZE", FieldKind::I64),
    ("TRADE_TIME", FieldKind::TimestampMicros),
    ("CONDITION_CODE", FieldKind::Str),
    ("VOLUME", FieldKind::I64),
];
/// Default per-stream queue capacity (`EngineConfig::subscription_stream_capacity`).
const DEFAULT_STREAM_CAPACITY: usize = 256;
/// The probe queue is large enough to measure queueing without data loss.
const PROBE_CAPACITY: usize = 1 << 20;
/// Producer start delay so the consumer is parked before the first update.
const START_DELAY_NS: i64 = 20_000_000;

struct Scenario {
    name: &'static str,
    /// Send offsets in nanoseconds from the scenario start.
    schedule: Vec<i64>,
}

struct Outcome {
    limit: usize,
    updates: usize,
    batches: usize,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    max_queued: usize,
}

fn scenarios() -> Vec<Scenario> {
    let quiet = (0..20_000).map(|i| i as i64 * 100_000).collect();
    let burst = (0..20_i64)
        .flat_map(|b| (0..2_000_i64).map(move |i| b * 20_000_000 + i * 1_000))
        .collect();
    let sustained = (0..100_000).map(|i| i as i64 * 4_000).collect();
    vec![
        Scenario {
            name: "quiet: 10k updates/s (100 us apart)",
            schedule: quiet,
        },
        Scenario {
            name: "burst: 20 x 2,000 updates 1 us apart",
            schedule: burst,
        },
        Scenario {
            name: "sustained: 250k updates/s (4 us apart)",
            schedule: sustained,
        },
    ]
}

fn limits() -> Vec<usize> {
    std::env::var("LATENCY_LIMITS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|value| value.trim().parse().ok())
                .filter(|value| *value > 0)
                .collect::<Vec<usize>>()
        })
        .filter(|limits| !limits.is_empty())
        .unwrap_or_else(|| vec![1, 16, 64, 1024])
}

/// Quote and trade deltas carrying four of the eight fields, as live ticks do.
fn make_updates(n: usize) -> Vec<SubscriptionUpdate> {
    let layout = Arc::new(FieldLayout::new(
        1,
        FIELDS
            .iter()
            .enumerate()
            .map(|(index, &(name, kind))| FieldMeta::new(name, index as _, kind))
            .collect(),
    ));
    let topics: Vec<Arc<str>> = (0..100)
        .map(|i| Arc::<str>::from(format!("SYN{i:03} US Equity")))
        .collect();
    let condition: Arc<str> = Arc::from("REGULAR");
    (0..n)
        .map(|i| {
            let mut update = SubscriptionUpdate {
                timestamp_us: 0,
                topic_id: (i % topics.len()) as _,
                topic: Arc::clone(&topics[i % topics.len()]),
                layout: Arc::clone(&layout),
                values: Default::default(),
            };
            let price = 100.0 + (i % 1_000) as f64 * 0.01;
            let values: [(usize, UpdateValue); 4] = if i % 3 == 0 {
                [
                    (0, UpdateValue::F64(price)),
                    (
                        5,
                        UpdateValue::TimestampMicros(1_700_000_000_000_000 + i as i64),
                    ),
                    (6, UpdateValue::Str(Arc::clone(&condition))),
                    (7, UpdateValue::I64(1_000_000 + i as i64)),
                ]
            } else {
                [
                    (1, UpdateValue::F64(price - 0.01)),
                    (2, UpdateValue::F64(price + 0.01)),
                    (3, UpdateValue::I32((i % 500) as i32 + 1)),
                    (4, UpdateValue::I64((i % 700) as i64 + 1)),
                ]
            };
            for (index, value) in values {
                update.values.push(UpdateField {
                    index: index as _,
                    value,
                });
            }
            update
        })
        .collect()
}

fn run(schedule: &[i64], limit: usize) -> Outcome {
    let clock = Instant::now();
    let now_ns = move || clock.elapsed().as_nanos() as i64;
    let updates = make_updates(schedule.len());
    let schedule = schedule.to_vec();
    let (tx, mut rx) = subscription_channel(PROBE_CAPACITY);

    // `timestamp_us` carries the enqueue instant in nanoseconds for this probe.
    let producer = std::thread::spawn(move || {
        let start = now_ns() + START_DELAY_NS;
        for (mut update, offset) in updates.into_iter().zip(schedule) {
            let target = start + offset;
            let mut now = now_ns();
            while now < target {
                std::hint::spin_loop();
                now = now_ns();
            }
            update.timestamp_us = now;
            assert!(
                tx.try_send(Ok(update)).is_ok(),
                "probe queue must not reject updates"
            );
        }
    });

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("consumer runtime");
    let (mut latencies, batches, max_queued) = runtime.block_on(async move {
        let pending = PendingUpdates::default();
        let (_close_tx, mut close_rx) = watch::channel(false);
        let mut batcher =
            SubscriptionArrowBatcher::with_capacity(subscription_batch_capacity_hint(limit));
        let mut latencies = Vec::new();
        let mut batches = 0;
        let mut max_queued = 0;
        loop {
            match receive_subscription_updates(&mut rx, &pending, &mut close_rx, limit).await {
                SubscriptionRead::Updates(updates) => {
                    max_queued = max_queued.max(updates.len() + rx.len());
                    for update in &updates {
                        if let Some(batch) = batcher.append(update) {
                            black_box(batch);
                        }
                    }
                    let batch = batcher.flush().expect("a read returns at least one update");
                    let ready = now_ns();
                    black_box(&batch);
                    latencies.extend(updates.iter().map(|update| ready - update.timestamp_us));
                    batches += 1;
                }
                SubscriptionRead::Ended | SubscriptionRead::Closed => break,
                SubscriptionRead::Error(error) => panic!("probe stream failed: {error}"),
            }
        }
        (latencies, batches, max_queued)
    });
    producer.join().expect("producer thread");

    latencies.sort_unstable();
    let percentile = |p: f64| {
        let index = ((latencies.len() - 1) as f64 * p).round() as usize;
        latencies[index] as f64 / 1_000.0
    };
    Outcome {
        limit,
        updates: latencies.len(),
        batches,
        p50_us: percentile(0.50),
        p99_us: percentile(0.99),
        p999_us: percentile(0.999),
        max_us: percentile(1.0),
        max_queued,
    }
}

fn main() {
    let limits = limits();
    let mut report = Vec::new();
    for scenario in scenarios() {
        println!(
            "\n## {} ({} updates)",
            scenario.name,
            scenario.schedule.len()
        );
        println!(
            "| limit | batches | rows/batch | p50 us | p99 us | p99.9 us | max us | max queued | exceeds default capacity |"
        );
        println!("|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        let mut outcomes = Vec::new();
        for &limit in &limits {
            let outcome = run(&scenario.schedule, limit);
            println!(
                "| {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {} |",
                outcome.limit,
                outcome.batches,
                outcome.updates as f64 / outcome.batches as f64,
                outcome.p50_us,
                outcome.p99_us,
                outcome.p999_us,
                outcome.max_us,
                outcome.max_queued,
                if outcome.max_queued > DEFAULT_STREAM_CAPACITY {
                    "yes (DropNewest ends the stream)"
                } else {
                    "no"
                }
            );
            outcomes.push(outcome);
        }
        report.push((scenario.name, outcomes));
    }
    write_results(&limits, &report);
}

fn write_results(limits: &[usize], report: &[(&str, Vec<Outcome>)]) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after UNIX_EPOCH")
        .as_secs();
    let scenarios = report
        .iter()
        .map(|(name, outcomes)| {
            let rows = outcomes
                .iter()
                .map(|o| {
                    format!(
                        r#"        {{"limit": {}, "updates": {}, "batches": {}, "p50_us": {:.2}, "p99_us": {:.2}, "p999_us": {:.2}, "max_us": {:.2}, "max_queued": {}}}"#,
                        o.limit, o.updates, o.batches, o.p50_us, o.p99_us, o.p999_us, o.max_us, o.max_queued
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n");
            format!("    {{\"scenario\": \"{name}\", \"results\": [\n{rows}\n    ]}}")
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let limits_list = limits
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let json = format!(
        r#"{{
  "schema_version": 1,
  "timestamp": {timestamp},
  "crate": "xbbg-async",
  "benchmark_type": "subscription_read_latency",
  "offline": true,
  "uses_bloomberg_session": false,
  "coverage": "production subscription channel, receive_subscription_updates batch limit and SubscriptionArrowBatcher",
  "timing_scope": "per update: enqueue to RecordBatch ready on the consumer; excludes SDK decode, feed fan-out and language bindings",
  "provenance": {},
  "scenarios": [
{scenarios}
  ]
}}"#,
        xbbg_bench::benchmark_provenance_json(&format!(
            "implementation=subscription_read_latency;limits={limits_list};fields=8;values_per_update=4"
        )),
    );
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    write_json(
        &dir.join(format!("subscription_read_latency_{timestamp}.json")),
        &json,
    );
    write_json(&dir.join("subscription_read_latency_latest.json"), &json);
}
