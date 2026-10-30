//! Offline, v1.7-compatible default-queue regression benchmark.
//!
//! Copy this file and its [[bench]] registration unchanged into the frozen
//! baseline. It deliberately uses no delivery modes, policy factories, fixtures,
//! or native delivery consumers. Run both trees with the same release-profile
//! compiler, flags, arguments, and alternating/repeated execution order.
//!
//! cargo bench -p xbbg-bench --bench subscription_queue_default --
//!   --updates 100000 --samples 30 --warmup 3 --capacity 256
//!
//! JSON goes to stdout, or only the explicitly supplied --output PATH. No
//! Bloomberg session is created. Cloning the immutable eight-field template,
//! enqueue/dequeue, checksum accumulation, and payload destruction are timed;
//! channel/template setup, full-payload preflight, and reporting are not.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{Value, json};
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionUpdate, UpdateField, UpdateValue,
    subscription_channel,
};

struct Config {
    updates: usize,
    samples: usize,
    warmup: usize,
    capacity: usize,
    output: Option<PathBuf>,
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

fn config() -> Config {
    let mut config = Config {
        updates: 100_000,
        samples: 30,
        warmup: 3,
        capacity: 256,
        output: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--bench" {
            continue; // Cargo may supply this to harness=false targets.
        }
        if arg == "--help" {
            println!(
                "subscription_queue_default [--updates 1000..=10000000] [--samples 3..=300] [--warmup 0..=30] [--capacity 1..=65536] [--output PATH]"
            );
            std::process::exit(0);
        }
        let value = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {arg}"));
        match arg.as_str() {
            "--updates" => config.updates = bounded(&arg, &value, 1_000, 10_000_000),
            "--samples" => config.samples = bounded(&arg, &value, 3, 300),
            "--warmup" => config.warmup = bounded(&arg, &value, 0, 30),
            "--capacity" => config.capacity = bounded(&arg, &value, 1, 65_536),
            "--output" => config.output = Some(value.into()),
            _ => panic!("unknown argument {arg}"),
        }
    }
    assert!(
        config.updates * (config.samples + config.warmup) <= 100_000_000,
        "total timed and warmup roundtrips must not exceed 100000000"
    );
    config
}

fn template() -> SubscriptionUpdate {
    let kinds = [
        ("SEQ", FieldKind::I64),
        ("PRICE", FieldKind::F64),
        ("SIZE", FieldKind::I32),
        ("ACTIVE", FieldKind::Bool),
        ("VENUE", FieldKind::Str),
        ("CONDITION", FieldKind::Str),
        ("DATE", FieldKind::Date32),
        ("CLOCK", FieldKind::Time64Micros),
    ];
    let values = [
        UpdateValue::I64(7),
        UpdateValue::F64(0.125),
        UpdateValue::I32(17),
        UpdateValue::Bool(true),
        UpdateValue::Str(Arc::from("SYNTHETIC_VENUE")),
        UpdateValue::Str(Arc::from("SYNTHETIC_CONDITION")),
        UpdateValue::Date32(20_000),
        UpdateValue::Time64Micros(12_345_678),
    ];
    SubscriptionUpdate {
        timestamp_us: 7,
        topic_id: 0,
        topic: Arc::from("SYNTH_QUEUE_0"),
        layout: Arc::new(FieldLayout::new(
            1,
            kinds
                .into_iter()
                .enumerate()
                .map(|(index, (name, kind))| FieldMeta::new(name, index as _, kind))
                .collect(),
        )),
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

fn preflight(payload: &SubscriptionUpdate, capacity: usize) {
    let (tx, mut rx) = subscription_channel(capacity);
    tx.try_send(Ok(payload.clone())).expect("preflight enqueue");
    let actual = rx
        .try_recv()
        .expect("preflight dequeue")
        .expect("preflight update");
    assert_eq!(actual.timestamp_us, payload.timestamp_us);
    assert_eq!(actual.topic_id, payload.topic_id);
    assert_eq!(actual.topic, payload.topic);
    assert_eq!(actual.layout.fields.len(), 8);
    assert_eq!(actual.values.len(), payload.values.len());
    for (actual, expected) in actual.values.iter().zip(&payload.values) {
        assert_eq!(actual.index, expected.index);
        assert_eq!(actual.value, expected.value);
    }
    assert!(
        rx.try_recv().is_err(),
        "roundtrip must leave an empty queue"
    );
}

fn sample(config: &Config, payload: &SubscriptionUpdate) -> (f64, u64) {
    let (tx, mut rx) = subscription_channel(config.capacity);
    let mut checksum = 0u64;
    let started = Instant::now();
    for _ in 0..config.updates {
        tx.try_send(Ok(black_box(payload).clone()))
            .expect("no-overflow enqueue");
        let update = rx.try_recv().expect("ready dequeue").expect("update");
        checksum = checksum.wrapping_add(black_box(update.timestamp_us) as u64);
        drop(black_box(update));
    }
    let elapsed_ns = started.elapsed().as_secs_f64() * 1e9;
    assert_eq!(
        checksum,
        config.updates as u64 * payload.timestamp_us as u64
    );
    assert!(rx.try_recv().is_err());
    (elapsed_ns, checksum)
}

fn percentile(sorted: &[f64], percent: usize) -> f64 {
    let rank = (sorted.len() * percent).div_ceil(100).max(1);
    sorted[rank - 1]
}

// Kept inline so this file alone can be copied into the frozen baseline.
// Mirror the allowlist in support/delivery_provenance.rs, not the raw helper.
fn fingerprint(raw: &Value, field: &str) -> Value {
    let value = raw[field].as_str().unwrap_or("unknown");
    let checksum = value
        .bytes()
        .fold(0xcbf29ce484222325_u64, |checksum, byte| {
            (checksum ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    json!({ "algorithm": "fnv1a64", "value": format!("{checksum:016x}"), "known": value != "unknown" })
}

fn checksum(raw: &Value, field: &str) -> Option<Value> {
    let value = raw[field]["value"].as_str()?;
    (value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| json!({ "algorithm": "fnv1a64", "value": value }))
}

fn compiler_version(raw: &str) -> Option<&str> {
    let version = raw.strip_prefix("rustc ")?.split_whitespace().next()?;
    let (base, channel) = version
        .split_once('-')
        .map_or((version, None), |(base, channel)| (base, Some(channel)));
    let mut numbers = base.split('.');
    if !(0..3).all(|_| {
        numbers.next().is_some_and(|number| {
            !number.is_empty()
                && number.len() <= 10
                && number.bytes().all(|byte| byte.is_ascii_digit())
        })
    }) || numbers.next().is_some()
    {
        return None;
    }
    let valid_channel = match channel {
        None | Some("nightly" | "dev" | "beta") => true,
        Some(channel) => channel.strip_prefix("beta.").is_some_and(|number| {
            !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
        }),
    };
    valid_channel.then_some(version)
}

fn private_provenance(descriptor: &str) -> Value {
    let raw: Value = serde_json::from_str(&xbbg_bench::benchmark_provenance_json(descriptor))
        .expect("benchmark provenance JSON");
    let profile = raw["profile"]
        .as_str()
        .filter(|value| matches!(*value, "release" | "bench" | "dev" | "test"));
    // Custom target names can identify a workstation; retain only their digest.
    let target = raw["target"].as_str().filter(|value| {
        matches!(
            *value,
            "x86_64-pc-windows-msvc"
                | "aarch64-pc-windows-msvc"
                | "i686-pc-windows-msvc"
                | "x86_64-pc-windows-gnu"
                | "x86_64-unknown-linux-gnu"
                | "aarch64-unknown-linux-gnu"
                | "x86_64-unknown-linux-musl"
                | "aarch64-unknown-linux-musl"
                | "x86_64-apple-darwin"
                | "aarch64-apple-darwin"
        )
    });
    let opt_level = raw["opt_level"]
        .as_str()
        .filter(|value| matches!(*value, "0" | "1" | "2" | "3" | "s" | "z"));
    let git_commit = raw["git_commit"]
        .as_str()
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()));
    json!({
        "benchmark_crate": "xbbg-bench",
        "benchmark_crate_version": env!("CARGO_PKG_VERSION"),
        "profile": profile,
        "debug_build": raw["debug_build"].as_bool(),
        "target": target,
        "rustc_version": compiler_version(raw["rustc_version"].as_str().unwrap_or("")),
        "opt_level": opt_level,
        "git_commit": git_commit,
        "artifact_size_bytes": raw["artifact_size_bytes"].as_u64(),
        "artifact_checksum": checksum(&raw, "artifact_checksum"),
        "cargo_lock_checksum": checksum(&raw, "cargo_lock_checksum"),
        "input_descriptor": descriptor,
        "input_checksum": checksum(&raw, "input_checksum"),
        "configuration_fingerprints": {
            "rustc_build": fingerprint(&raw, "rustc_version"),
            "target": fingerprint(&raw, "target"),
            "target_cpu": fingerprint(&raw, "target_cpu"),
            "target_features": fingerprint(&raw, "target_features"),
            "rustflags": fingerprint(&raw, "rustflags"),
        },
        "privacy_policy": "allowlisted standard compiler version/profile/target and checksums only; configuration strings are noncryptographic comparison fingerprints, never raw flags; no executable/SDK/output paths, host identifiers, RUST_LOG or raw SDK environment values",
    })
}

fn main() {
    let config = config();
    let payload = template();
    preflight(&payload, config.capacity);
    for _ in 0..config.warmup {
        black_box(sample(&config, &payload));
    }
    let mut elapsed = Vec::with_capacity(config.samples);
    let mut samples = Vec::with_capacity(config.samples);
    for index in 0..config.samples {
        let (elapsed_ns, checksum) = sample(&config, &payload);
        elapsed.push(elapsed_ns / config.updates as f64);
        samples.push(json!({
            "sample": index,
            "elapsed_ns": elapsed_ns,
            "ns_per_roundtrip": elapsed_ns / config.updates as f64,
            "roundtrips_per_second": config.updates as f64 * 1e9 / elapsed_ns,
            "checksum": checksum,
        }));
    }
    elapsed.sort_unstable_by(f64::total_cmp);
    let descriptor = format!(
        "subscription_queue_default_v1;updates={};samples={};warmup={};capacity={};fields=8;ownership=shared_immutable_template_clone;single_thread;roundtrip;no_overflow",
        config.updates, config.samples, config.warmup, config.capacity,
    );
    let provenance = private_provenance(&descriptor);
    let report = json!({
        "schema_version": 1,
        "benchmark": "subscription_queue_default",
        "offline": true,
        "uses_bloomberg_session": false,
        "provenance": provenance,
        "config": { "updates": config.updates, "samples": config.samples,
            "warmup": config.warmup, "capacity": config.capacity },
        "setup_scope": "channel and immutable mixed eight-field payload construction; preflight full-field equality; excluded from each sample",
        "timing_scope": "single-thread try_send(Ok(template.clone())) then try_recv; all Arc increments/decrements, checksum, payload drop included; no overflow, wait, SDK decode, network, or Arrow materialization",
        "ownership": "layout/topic/two string Arcs shared with a retained immutable template; inline eight-field values cloned per operation; not final-Arc deallocation",
        "metric_definitions": {
            "ns_per_roundtrip": "elapsed sample wall time / successful enqueue+dequeue pairs; not one-way producer latency",
            "sample_percentiles": "nearest-rank quantiles of per-sample mean roundtrip costs, not per-operation latency; p95 requires >=20 samples, p99 >=100",
            "comparison": "compare unchanged source and arguments on baseline/current with identical compiler/profile/CPU flags and repeated alternating run order; no cross-target inference"
        },
        "build_flags": {
            "portable": "leave RUSTFLAGS unset in both trees (compiler target defaults)",
            "host_tuned": "set RUSTFLAGS=-C target-cpu=native in both trees; never compare against a portable run",
            "recorded": "allowlisted compiler version/profile/target plus configuration fingerprints, artifact and lock checksums; raw flags, paths and SDK environment values are omitted"
        },
        "summary": {
            "sample_count": config.samples,
            "operations_per_sample": config.updates,
            "mean_ns_per_roundtrip": elapsed.iter().sum::<f64>() / elapsed.len() as f64,
            "min_ns_per_roundtrip": elapsed[0],
            "p50_ns_per_roundtrip": percentile(&elapsed, 50),
            "p95_ns_per_roundtrip": (elapsed.len() >= 20).then(|| percentile(&elapsed, 95)),
            "p99_ns_per_roundtrip": (elapsed.len() >= 100).then(|| percentile(&elapsed, 99)),
            "correctness": "full payload preflight and exact per-sample checksum passed"
        },
        "samples": samples,
    });
    let output = serde_json::to_string_pretty(&report).expect("report JSON");
    if let Some(path) = config.output {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).expect("create benchmark output directory");
        }
        std::fs::write(path, output).expect("write benchmark report");
    } else {
        println!("{output}");
    }
}
