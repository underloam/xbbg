//! Synthetic subscription replay benchmark for the xbbg-async Arrow builder path.
//!
//! This benchmark is fully offline: it does not create a Bloomberg session or
//! traverse live SDK events. Deterministic sparse `SubscriptionUpdate` values
//! pass through the production `SubscriptionArrowBatcher`, including mixed
//! field types, late layout changes, presence bitmaps, and bounded row batches.
//!
//! Run:
//!   SUB_REPLAY_ROWS=100000 SUB_REPLAY_FLUSH=1024 SUB_REPLAY_ITERATIONS=5 \
//!     cargo bench --package xbbg-bench --bench async_subscription_replay

use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use xbbg_bench::write_json;

#[path = "support/subscription_replay.rs"]
mod subscription_replay;
use subscription_replay::{replay, LATE_FIELDS, REQUESTED_FIELDS};

const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_FLUSH: usize = 1_024;
const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_WARMUP: usize = 2;

#[derive(Debug)]
struct BenchConfig {
    rows: usize,
    flush_threshold: usize,
    iterations: usize,
    warmup_iterations: usize,
}

#[derive(Debug)]
struct IterationResult {
    iteration: usize,
    rows: usize,
    batches: usize,
    columns: usize,
    elapsed_us: u128,
    rows_per_sec: f64,
    batches_per_sec: f64,
}

fn run_iteration(iteration: usize, config: &BenchConfig) -> IterationResult {
    let mut rows = 0;
    let mut batches = 0;
    let mut columns = 0;
    let started = Instant::now();
    replay(config.rows, config.flush_threshold, |batch| {
        rows += batch.num_rows();
        batches += 1;
        columns += batch.num_columns();
        std::hint::black_box(batch);
    });

    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let rows_per_sec = rows as f64 / elapsed_secs;
    let batches_per_sec = batches as f64 / elapsed_secs;

    IterationResult {
        iteration,
        rows,
        batches,
        columns,
        elapsed_us: elapsed.as_micros(),
        rows_per_sec,
        batches_per_sec,
    }
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn percentile_elapsed_us(results: &[IterationResult], percentile: f64) -> f64 {
    let mut values: Vec<u128> = results.iter().map(|result| result.elapsed_us).collect();
    values.sort_unstable();
    let idx = (((values.len() - 1) as f64) * percentile / 100.0).round() as usize;
    values[idx] as f64
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_secs()
}

fn write_results(config: &BenchConfig, timestamp: u64, results: &[IterationResult]) {
    let best_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .fold(0.0, f64::max);
    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let mean_elapsed_us =
        results.iter().map(|result| result.elapsed_us).sum::<u128>() as f64 / results.len() as f64;
    let min_elapsed_us = results
        .iter()
        .map(|result| result.elapsed_us)
        .min()
        .unwrap_or_default() as f64;
    let p50_elapsed_us = percentile_elapsed_us(results, 50.0);
    let input_descriptor = format!(
        "implementation=subscription_update_arrow_batcher;rows={};flush_threshold={};iterations={};warmup={};requested_fields={};late_fields={}",
        config.rows,
        config.flush_threshold,
        config.iterations,
        config.warmup_iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len(),
    );
    let provenance = xbbg_bench::benchmark_provenance_json(&input_descriptor);

    let iterations_json = results
        .iter()
        .map(|result| {
            format!(
                r#"    {{
      "iteration": {},
      "rows": {},
      "batches": {},
      "columns_finalized": {},
      "elapsed_us": {},
      "rows_per_sec": {:.2},
      "batches_per_sec": {:.2}
    }}"#,
                result.iteration,
                result.rows,
                result.batches,
                result.columns,
                result.elapsed_us,
                result.rows_per_sec,
                result.batches_per_sec
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let json = format!(
        r#"{{
  "schema_version": 2,
  "timestamp": {},
  "crate": "xbbg-async",
  "benchmark_type": "synthetic_subscription_replay",
  "offline": true,
  "uses_bloomberg_session": false,
  "coverage": "production SubscriptionUpdate to SubscriptionArrowBatcher with layout changes and presence bitmaps; no bounded transport queue, Bloomberg SDK event, or network coverage",
  "timing_scope": "deterministic fixture setup, sparse update generation, production Arrow append and RecordBatch finalization",
  "percentile_policy": "p50 reported; p95 requires at least 20 samples and p99 at least 100",
  "provenance": {},
  "config": {{
    "rows": {},
    "flush_threshold": {},
    "iterations": {},
    "warmup_iterations": {},
    "requested_fields": {},
    "late_fields": {}
  }},
  "summary": {{
    "sample_count": {},
    "mean_elapsed_us": {:.2},
    "min_elapsed_us": {:.2},
    "p50_elapsed_us": {:.2},
    "avg_rows_per_sec": {:.2},
    "best_rows_per_sec": {:.2}
  }},
  "iterations": [
{}
  ]
}}"#,
        timestamp,
        provenance,
        config.rows,
        config.flush_threshold,
        config.iterations,
        config.warmup_iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len(),
        results.len(),
        mean_elapsed_us,
        min_elapsed_us,
        p50_elapsed_us,
        avg_rows_per_sec,
        best_rows_per_sec,
        iterations_json
    );

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    let timestamped = dir.join(format!("async_subscription_replay_{timestamp}.json"));
    let latest = dir.join("async_subscription_replay_latest.json");
    write_json(&timestamped, &json);
    write_json(&latest, &json);
}

fn print_results(config: &BenchConfig, results: &[IterationResult]) {
    println!("\n{:=<88}", "");
    println!("  xbbg-async Synthetic Subscription Replay Benchmark");
    println!("{:=<88}\n", "");
    println!(
        "  rows={} flush={} warmup={} iterations={} requested_fields={} late_fields={}",
        config.rows,
        config.flush_threshold,
        config.warmup_iterations,
        config.iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len()
    );
    println!(
        "  {:>9} {:>12} {:>9} {:>12} {:>14} {:>14}",
        "Iteration", "Rows", "Batches", "Columns", "Rows/sec", "Elapsed (us)"
    );
    println!("  {:-<84}", "");

    for result in results {
        println!(
            "  {:>9} {:>12} {:>9} {:>12} {:>14.0} {:>14}",
            result.iteration,
            result.rows,
            result.batches,
            result.columns,
            result.rows_per_sec,
            result.elapsed_us
        );
    }

    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let min_elapsed_us = results
        .iter()
        .map(|result| result.elapsed_us)
        .min()
        .unwrap_or_default();
    let p50_elapsed_us = percentile_elapsed_us(results, 50.0);
    println!("\n  Average rows/sec: {:.0}", avg_rows_per_sec);
    println!(
        "  Elapsed us min/mean/p50: {}/{:.2}/{:.2}",
        min_elapsed_us,
        results.iter().map(|result| result.elapsed_us).sum::<u128>() as f64 / results.len() as f64,
        p50_elapsed_us
    );
    println!("{:=<88}\n", "");
}

fn main() {
    let config = BenchConfig {
        rows: parse_env_usize("SUB_REPLAY_ROWS", DEFAULT_ROWS),
        flush_threshold: parse_env_usize("SUB_REPLAY_FLUSH", DEFAULT_FLUSH),
        iterations: parse_env_usize("SUB_REPLAY_ITERATIONS", DEFAULT_ITERATIONS),
        warmup_iterations: parse_env_usize("BENCH_WARMUP", DEFAULT_WARMUP),
    };

    for _ in 0..config.warmup_iterations {
        std::hint::black_box(run_iteration(0, &config));
    }

    let mut results = Vec::with_capacity(config.iterations);
    for iteration in 1..=config.iterations {
        let result = run_iteration(iteration, &config);
        std::hint::black_box(&result);
        results.push(result);
    }

    print_results(&config, &results);
    write_results(&config, unix_timestamp(), &results);
}
