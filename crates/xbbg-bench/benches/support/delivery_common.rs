use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionUpdate, UpdateField, UpdateValue,
};

pub(crate) const FIELDS: [&str; 8] = [
    "SEQ",
    "PRICE",
    "SIZE",
    "ACTIVE",
    "VENUE",
    "CONDITION",
    "DATE",
    "CLOCK",
];
pub(crate) const WAIT: Duration = Duration::from_secs(10);

pub(crate) struct Config {
    pub updates: usize,
    pub capacity: usize,
    pub samples: usize,
    pub warmup: usize,
    pub producers: usize,
    pub sample_every: usize,
    pub wakeups: usize,
    pub replay_updates: usize,
    pub read_every: usize,
    pub quiet_every: usize,
    pub topics: usize,
    pub only: String,
    pub output: Option<PathBuf>,
}

impl Config {
    pub(crate) fn parse() -> Self {
        let mut config = Self {
            updates: 65_536,
            capacity: 256,
            samples: 5,
            warmup: 1,
            producers: 4,
            sample_every: 16,
            wakeups: 1_000,
            replay_updates: 16_384,
            read_every: 256,
            quiet_every: 1_024,
            topics: 8,
            only: "all".into(),
            output: None,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--bench" {
                continue;
            }
            if arg == "--help" {
                println!(
                    "subscription_delivery [--updates 4096..=262144] [--capacity 1..=16384] [--samples 3..=30] [--warmup 0..=10] [--producers 2..=16] [--sample-every 1..=64] [--wakeups 1000..=10000] [--replay-updates 4096..=131072] [--read-every 16..=4096] [--quiet-every 32..=65536] [--topics 2..=128] [--only all|queues|replay] [--output PATH]"
                );
                std::process::exit(0);
            }
            let value = args
                .next()
                .unwrap_or_else(|| panic!("missing value for {arg}"));
            match arg.as_str() {
                "--updates" => config.updates = bounded(&arg, &value, 4_096, 262_144),
                "--capacity" => config.capacity = bounded(&arg, &value, 1, 16_384),
                "--samples" => config.samples = bounded(&arg, &value, 3, 30),
                "--warmup" => config.warmup = bounded(&arg, &value, 0, 10),
                "--producers" => config.producers = bounded(&arg, &value, 2, 16),
                "--sample-every" => config.sample_every = bounded(&arg, &value, 1, 64),
                "--wakeups" => config.wakeups = bounded(&arg, &value, 1_000, 10_000),
                "--replay-updates" => config.replay_updates = bounded(&arg, &value, 4_096, 131_072),
                "--read-every" => config.read_every = bounded(&arg, &value, 16, 4_096),
                "--quiet-every" => config.quiet_every = bounded(&arg, &value, 32, 65_536),
                "--topics" => config.topics = bounded(&arg, &value, 2, 128),
                "--only" => {
                    assert!(
                        ["all", "queues", "replay"].contains(&value.as_str()),
                        "invalid --only"
                    );
                    config.only = value;
                }
                "--output" => config.output = Some(value.into()),
                _ => panic!("unknown argument {arg}"),
            }
        }
        assert!(
            config.updates >= 2 * config.capacity,
            "--updates must be >= twice --capacity for overflow"
        );
        assert!(
            config.updates.is_multiple_of(config.producers),
            "--updates must divide evenly across --producers"
        );
        assert!(
            config.updates / config.producers / config.sample_every
                * config.producers
                * config.samples
                >= 1_000,
            "at least 1000 queue producer samples required: increase --updates/--samples or decrease --sample-every"
        );
        assert!(
            config.replay_updates / config.sample_every * config.samples >= 1_000,
            "at least 1000 replay producer samples required: increase --replay-updates/--samples or decrease --sample-every"
        );
        assert!(
            config.quiet_every >= 2 * config.read_every,
            "--quiet-every must be >= twice --read-every"
        );
        assert!(
            config.replay_updates >= config.quiet_every,
            "replay must include a quiet-topic update"
        );
        assert!(
            config.updates * (config.samples + config.warmup) <= 8_000_000,
            "queue update budget per scenario must not exceed 8000000"
        );
        assert!(
            config.replay_updates * (config.samples + config.warmup) <= 4_000_000,
            "replay update budget per mode must not exceed 4000000"
        );
        config
    }

    pub(crate) fn json(&self) -> Value {
        json!({
            "updates": self.updates, "capacity": self.capacity,
            "samples": self.samples, "warmup": self.warmup,
            "multi_producers": self.producers, "sample_every": self.sample_every,
            "wakeups_per_sample": self.wakeups, "replay_updates": self.replay_updates,
            "read_every_updates": self.read_every, "quiet_every_updates": self.quiet_every,
            "topics": self.topics, "only": self.only,
        })
    }
}

fn bounded(name: &str, value: &str, min: usize, max: usize) -> usize {
    let parsed: usize = value
        .parse()
        .unwrap_or_else(|_| panic!("{name} expects an integer"));
    assert!(
        (min..=max).contains(&parsed),
        "{name} must be in {min}..={max}"
    );
    parsed
}

pub(crate) fn ns(started: Instant) -> u64 {
    started
        .elapsed()
        .as_nanos()
        .try_into()
        .expect("benchmark duration fits u64")
}

pub(crate) fn distribution(values: &[u64]) -> Value {
    assert!(!values.is_empty(), "nonempty distribution");
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let quantile = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100).max(1) - 1];
    json!({
        "sample_count": sorted.len(),
        "min_ns": sorted[0],
        "mean_ns": sorted.iter().map(|&value| value as f64).sum::<f64>() / sorted.len() as f64,
        "p50_ns": quantile(50),
        "p99_ns": (sorted.len() >= 1_000).then(|| quantile(99)),
        "max_ns": sorted[sorted.len() - 1],
    })
}

pub(crate) fn timer_overhead() -> Value {
    let values: Vec<_> = (0..10_000)
        .map(|_| {
            let started = Instant::now();
            std::hint::black_box(());
            ns(started)
        })
        .collect();
    json!({
        "instant_pair": distribution(&values),
        "policy": "gross producer latency includes Instant::now plus elapsed timestamp acquisition; timer control is reported, never subtracted; only one in sample_every queue sends is instrumented, phase rotated per iteration/producer; instrumentation and scheduler contention perturb wall throughput",
        "percentiles": "nearest rank over individual sampled operations pooled across measured iterations/producers; p99 requires >=1000 samples; not a confidence interval or a tail guarantee",
    })
}

pub(crate) struct Payloads {
    pub layout: Arc<FieldLayout>,
    pub topics: Vec<Arc<str>>,
    venue: Arc<str>,
}

impl Payloads {
    pub(crate) fn new(topics: usize) -> Self {
        let kinds = [
            FieldKind::I64,
            FieldKind::F64,
            FieldKind::I32,
            FieldKind::Bool,
            FieldKind::Str,
            FieldKind::Str,
            FieldKind::Date32,
            FieldKind::Time64Micros,
        ];
        Self {
            layout: Arc::new(FieldLayout::new(
                1,
                FIELDS
                    .into_iter()
                    .zip(kinds)
                    .enumerate()
                    .map(|(index, (name, kind))| FieldMeta::new(name, index as _, kind))
                    .collect(),
            )),
            topics: (0..topics)
                .map(|index| Arc::from(format!("SYNTH_DELIVERY_{index}")))
                .collect(),
            venue: Arc::from("SYNTHETIC_VENUE"),
        }
    }

    pub(crate) fn full(&self, sequence: usize, topic: usize) -> SubscriptionUpdate {
        let values = [
            UpdateValue::I64(sequence as i64),
            UpdateValue::F64(sequence as f64 * 0.001),
            UpdateValue::I32((sequence % 10_000) as i32),
            UpdateValue::Bool(sequence.is_multiple_of(2)),
            UpdateValue::Str(self.venue.clone()),
            UpdateValue::Str(Arc::from(format!("SYNTHETIC_CONDITION_{sequence:08}"))),
            UpdateValue::Date32(20_000 + (sequence % 31) as i32),
            UpdateValue::Time64Micros(sequence as i64 * 100),
        ];
        SubscriptionUpdate {
            timestamp_us: sequence as i64,
            topic_id: topic as _,
            topic: self.topics[topic].clone(),
            layout: self.layout.clone(),
            values: values
                .into_iter()
                .enumerate()
                .map(|(index, value)| UpdateField {
                    index: index as _,
                    value,
                })
                .collect(),
        }
    }
}

pub(crate) fn assert_payload(update: &SubscriptionUpdate) {
    let sequence = update.timestamp_us as usize;
    assert_eq!(update.layout.fields.len(), 8);
    assert_eq!(update.values.len(), 8);
    for (index, field) in update.values.iter().enumerate() {
        assert_eq!(field.index as usize, index);
        let expected = match index {
            0 => UpdateValue::I64(sequence as i64),
            1 => UpdateValue::F64(sequence as f64 * 0.001),
            2 => UpdateValue::I32((sequence % 10_000) as i32),
            3 => UpdateValue::Bool(sequence.is_multiple_of(2)),
            4 => UpdateValue::Str(Arc::from("SYNTHETIC_VENUE")),
            5 => UpdateValue::Str(Arc::from(format!("SYNTHETIC_CONDITION_{sequence:08}"))),
            6 => UpdateValue::Date32(20_000 + (sequence % 31) as i32),
            7 => UpdateValue::Time64Micros(sequence as i64 * 100),
            _ => unreachable!(),
        };
        assert_eq!(field.value, expected);
    }
}
