//! Offline production subscription delivery measurements (no Terminal/network).
//!
//! cargo bench -p xbbg-bench --bench subscription_delivery -- --output .omc/subscription_delivery.json
//!
//! Default: 65,536 queue updates, capacity 256, one/four producers, five measured
//! samples after one warmup, every 16th enqueue sampled, 1,000 idle-wakeup cycles;
//! 16,384 decoded replay updates, eight topics, reads every 256 inputs, one quiet
//! update every 1,024 inputs. --help lists bounded overrides and --only selection.
//!
//! This target requires xbbg-async/bench-internals -> test-support. Its fixture is
//! the existing SDK-backed test transport, never a production fake fallback.
//! Queue overwrite comparisons distinguish full production semantics from bare
//! primitives. Read/materialization and payload destruction are separate metrics.
//! The default-path baseline lives in the standalone subscription_queue_default
//! target and deliberately does not use any of these new APIs/support files.

#[path = "support/delivery_common.rs"]
mod delivery_common;
#[path = "support/delivery_provenance.rs"]
mod delivery_provenance;
#[path = "support/delivery_queues.rs"]
mod delivery_queues;
#[path = "support/delivery_replay.rs"]
mod delivery_replay;

use delivery_common::Config;
use serde_json::json;

fn main() {
    let config = Config::parse();
    let timer = delivery_common::timer_overhead();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("offline benchmark runtime");
    let (queues, replay) = runtime.block_on(async {
        let queues = if config.only != "replay" {
            Some(delivery_queues::run(&config).await)
        } else {
            None
        };
        let replay = if config.only != "queues" {
            Some(delivery_replay::run(&config).await)
        } else {
            None
        };
        (queues, replay)
    });
    let descriptor = format!(
        "subscription_delivery_v1;{};offline;synthetic_only;owned_queue_updates;decoded_actual_feed_replay",
        config.json()
    );
    let provenance = delivery_provenance::private_provenance(&descriptor);
    let report = json!({
        "schema_version": 1,
        "benchmark": "subscription_delivery",
        "offline": true,
        "uses_live_bloomberg_session": false,
        "synthetic_sdk_session_for_decode_preflight": config.only != "queues",
        "config": config.json(),
        "provenance": provenance,
        "available_parallelism": std::thread::available_parallelism().map(|count| count.get()).ok(),
        "build_flags": {
            "portable": "leave RUSTFLAGS unset; use identical compiler/profile/target defaults for all compared runs",
            "host_tuned": "RUSTFLAGS=-C target-cpu=native; use consistently, never mix with portable results",
            "recorded": "allowlisted compiler version/profile/target plus configuration fingerprints, artifact and lock checksums; raw flags, paths and SDK environment values are omitted",
        },
        "measurement_policy": {
            "acceptance": "no result emitted until every measured/warmup correctness check passes; panic rejects the run rather than emitting partial accepted timing",
            "ordering": "queue adapter order rotates across iterations; replay delta/state order alternates; repeat entire release-profile runs in different baseline/current process orders for regression analysis",
            "setup": "all fixtures, subscriptions, payloads, layouts, owned string allocation and thread startup outside reported timed phases",
            "sampling": "individual producer latency samples are gross wall nanoseconds, including the timestamp-pair cost below; p99 only with >=1000 samples; operation sampling is deterministic phase-rotated, not random",
            "limits": "bounded CLI and total-work budgets prevent accidental unbounded memory/runtime; no affinity or scheduler tuning is performed; CPU topology/frequency/other load still affect observations",
            "claims": "microbenchmark evidence only: no market-data rate prediction, no claim that primitive-only queues provide production subscription semantics, no SDK/network throughput extrapolation from decoded replay",
        },
        "timer_control": timer,
        "queue_comparisons": queues,
        "shared_feed_replay": replay,
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
