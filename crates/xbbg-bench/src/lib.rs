//! Shared benchmark helpers for xbbg.
//!
//! Provides reusable session setup, deterministic BQL fixtures, and result
//! writing utilities used across the benchmark targets.

use std::time::{Duration, Instant};

use xbbg_core::{EventType, Session, SessionOptions};

#[cfg(test)]
#[path = "../benches/support/subscription_replay.rs"]
mod subscription_replay;
#[cfg(test)]
#[path = "../benches/support/synthetic_subscriptions.rs"]
mod synthetic_subscriptions;

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Create and start a Bloomberg session, waiting for `SessionStarted`.
///
/// Reads `BLP_HOST` (default `127.0.0.1`) and `BLP_PORT` (default `8194`)
/// from the environment.
pub fn setup_session() -> Session {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194);

    let mut opts = SessionOptions::new().expect("failed to create session options");
    opts.set_server_host(&host).expect("failed to set host");
    opts.set_server_port(port);

    let sess = Session::new(&opts).expect("failed to create session");
    sess.start_and_wait(30_000)
        .expect("failed to start session within 30 seconds");

    sess
}

/// Open a Bloomberg service and wait for `ServiceStatus`.
pub fn open_service(sess: &Session, uri: &str) {
    sess.open_service(uri)
        .unwrap_or_else(|e| panic!("failed to open service {uri}: {e}"));

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let event = sess
            .next_event(Some(1_000))
            .unwrap_or_else(|e| panic!("failed while waiting for service {uri}: {e}"));
        if event.event_type() != EventType::ServiceStatus {
            continue;
        }
        let mut messages = event.messages();
        while let Some(message) = messages.next() {
            match message.message_type().as_str() {
                "ServiceOpened" => return,
                "ServiceOpenFailure" => panic!("Bloomberg rejected service open for {uri}"),
                _ => {}
            }
        }
    }
    panic!("timed out after 30 seconds waiting for service {uri}");
}

/// Number of messages in `event`.
pub fn message_count(event: &xbbg_core::Event) -> usize {
    let mut messages = event.messages();
    let mut count = 0;
    while messages.next().is_some() {
        count += 1;
    }
    count
}

// ---------------------------------------------------------------------------
// Shared BQL workloads
// ---------------------------------------------------------------------------

/// Scenario names and shapes shared by the parser and extractor benchmarks.
pub const BQL_JSON_SCENARIOS: [(&str, usize, &[&str]); 3] = [
    ("json_simple_1x1", 1, &["px_last"]),
    (
        "json_wide_1x5",
        1,
        &["px_last", "px_open", "px_high", "px_low", "px_volume"],
    ),
    ("json_rows_1000x2", 1_000, &["px_last", "px_volume"]),
];

/// Generate the same deterministic Bloomberg-shaped input for both BQL adapters.
pub fn bql_json_fixture(rows: usize, fields: &[&str]) -> String {
    let ids = (0..rows)
        .map(|i| format!("\"TICKER{i} US Equity\""))
        .collect::<Vec<_>>()
        .join(",");
    let dates = (0..rows)
        .map(|i| format!("\"2026-04-{:02}\"", (i % 28) + 1))
        .collect::<Vec<_>>()
        .join(",");
    let currencies = (0..rows).map(|_| "\"USD\"").collect::<Vec<_>>().join(",");

    let field_json = fields
        .iter()
        .enumerate()
        .map(|(field_idx, field)| {
            let values = (0..rows)
                .map(|i| format!("{}", 100.0 + field_idx as f64 + i as f64 / 100.0))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                r#""{field}":{{"idColumn":{{"name":"ID","type":"STRING","values":[{ids}]}} ,"valuesColumn":{{"name":"VALUE","type":"DOUBLE","values":[{values}]}} ,"secondaryColumns":[{{"name":"DATE","type":"DATE","values":[{dates}]}},{{"name":"CURRENCY","type":"STRING","values":[{currencies}]}}],"responseExceptions":[],"partialErrorMap":{{"errorIterator":null}}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        r#"{{"clientContext":{{"clientRequestId":"offline-bql-benchmark"}},"responseExceptions":null,"results":{{{field_json}}}}}"#
    )
}

// ---------------------------------------------------------------------------
// Result writing
// ---------------------------------------------------------------------------

/// Write benchmark results to a JSON file.
///
/// Creates the parent directory if needed.
pub fn write_json(path: &std::path::Path, json: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create results directory");
    }
    std::fs::write(path, json).expect("failed to write results");
    println!("Results written to: {}", path.display());
}

/// Compile- and run-time build metadata stamped into benchmark result files.
#[derive(Clone, Debug)]
pub struct BuildMode {
    pub profile: &'static str,
    pub target: &'static str,
    pub host: &'static str,
    pub target_cpu: &'static str,
    pub target_features: &'static str,
    pub rustflags: &'static str,
    pub rustc_version: &'static str,
    pub opt_level: &'static str,
    pub allocator: &'static str,
    pub debug_build: bool,
}

/// Return compiler-produced metadata without inferring build options at runtime.
pub fn build_mode() -> BuildMode {
    BuildMode {
        profile: option_env!("XBBG_BUILD_PROFILE").unwrap_or("unknown"),
        target: option_env!("XBBG_BUILD_TARGET").unwrap_or("unknown"),
        host: option_env!("XBBG_BUILD_HOST").unwrap_or("unknown"),
        target_cpu: option_env!("XBBG_BUILD_TARGET_CPU").unwrap_or("unknown"),
        target_features: option_env!("XBBG_BUILD_TARGET_FEATURES").unwrap_or("unknown"),
        rustflags: option_env!("XBBG_BUILD_RUSTFLAGS").unwrap_or("unknown"),
        rustc_version: option_env!("XBBG_BUILD_RUSTC_VERSION").unwrap_or("unknown"),
        opt_level: option_env!("XBBG_BUILD_OPT_LEVEL").unwrap_or("unknown"),
        allocator: option_env!("XBBG_BUILD_ALLOCATOR").unwrap_or("unknown"),
        debug_build: cfg!(debug_assertions),
    }
}

/// Render shared benchmark provenance as a JSON object.
///
/// `input_descriptor` should contain the complete stable fixture/config identity;
/// the helper records it together with an FNV-1a checksum. Artifact checksums use
/// the same explicitly-labelled non-cryptographic algorithm without adding a
/// benchmark-only hashing dependency.
pub fn benchmark_provenance_json(input_descriptor: &str) -> String {
    let build = build_mode();
    let executable = std::env::current_exe().ok();
    let artifact_path = executable
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let artifact_size = executable
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len().to_string())
        .unwrap_or_else(|| "null".to_string());
    let artifact_checksum = executable
        .as_ref()
        .and_then(|path| fnv1a64_file(path).ok())
        .map(|checksum| format!("{checksum:016x}"))
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        concat!(
            "{{",
            "\"benchmark_crate\":\"{}\",",
            "\"benchmark_crate_version\":\"{}\",",
            "\"profile\":\"{}\",",
            "\"debug_build\":{},",
            "\"target\":\"{}\",",
            "\"host\":\"{}\",",
            "\"target_cpu\":\"{}\",",
            "\"target_features\":\"{}\",",
            "\"rustflags\":\"{}\",",
            "\"rustc_version\":\"{}\",",
            "\"opt_level\":\"{}\",",
            "\"git_commit\":\"{}\",",
            "\"allocator\":\"{}\",",
            "\"artifact_path\":\"{}\",",
            "\"artifact_size_bytes\":{},",
            "\"artifact_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{}\"}},",
            "\"cargo_lock_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{:016x}\"}},",
            "\"sdk_version\":\"{}\",",
            "\"sdk_root\":\"{}\",",
            "\"rust_log\":\"{}\",",
            "\"input_descriptor\":\"{}\",",
            "\"input_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{:016x}\"}}",
            "}}"
        ),
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        json_escape(build.profile),
        build.debug_build,
        json_escape(build.target),
        json_escape(build.host),
        json_escape(build.target_cpu),
        json_escape(build.target_features),
        json_escape(build.rustflags),
        json_escape(build.rustc_version),
        json_escape(build.opt_level),
        json_escape(option_env!("XBBG_BUILD_GIT_COMMIT").unwrap_or("unknown")),
        json_escape(build.allocator),
        json_escape(&artifact_path),
        artifact_size,
        artifact_checksum,
        fnv1a64(include_bytes!("../../../Cargo.lock")),
        json_escape(&runtime_env_or_unknown("BLPAPI_VERSION")),
        json_escape(&runtime_env_or_unknown("BLPAPI_ROOT")),
        json_escape(&runtime_env_or_unknown("RUST_LOG")),
        json_escape(input_descriptor),
        fnv1a64(input_descriptor.as_bytes()),
    )
}

fn runtime_env_or_unknown(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn fnv1a64_file(path: &std::path::Path) -> std::io::Result<u64> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path)?;
    let mut checksum = 0xcbf29ce484222325_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            return Ok(checksum);
        }
        for byte in &buffer[..count] {
            checksum ^= u64::from(*byte);
            checksum = checksum.wrapping_mul(0x100000001b3);
        }
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |checksum, byte| {
        (checksum ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                write!(&mut escaped, "\\u{:04x}", character as u32)
                    .expect("writing to String should not fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::subscription_replay::replay;
    use super::synthetic_subscriptions::{batch_updates, BATCH_SIZE};
    use super::{bql_json_fixture, BQL_JSON_SCENARIOS};
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use xbbg_async::engine::BqlState;

    #[test]
    fn replay_uses_production_layout_flushes_and_presence_metadata() {
        let mut batches = Vec::new();
        replay(10, 8, |batch| batches.push(batch));
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 6);
        assert_eq!(batches[0].num_columns(), 11);
        assert_eq!(batches[1].num_columns(), 15);
        for batch in &batches {
            assert_eq!(
                batch.schema().metadata()["xbbg.subscription_presence"],
                "column=__xbbg_present;encoding=binary-lsb-first;mapping=bit-i-to-schema-field-(i+2)"
            );
        }

        let first = &batches[0];
        let presence = first
            .column_by_name("__xbbg_present")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(presence.value(0), &[0b0010_0000]);
        assert_eq!(presence.value(1), &[0xff]);
        let prices = first
            .column_by_name("LAST_PRICE")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(prices.is_null(0));
        assert_eq!(prices.value(1), 100.01);
        let bid_sizes = first
            .column_by_name("BID_SIZE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(bid_sizes.value(1), 2);
        let ask_sizes = first
            .column_by_name("ASK_SIZE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ask_sizes.value(1), 2);
        let delayed = first
            .column_by_name("IS_DELAYED_STREAM")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(delayed.value(0));
        let conditions = first
            .column_by_name("CONDITION_CODE")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(conditions.value(1), "OPEN");

        let late = &batches[1];
        let presence = late
            .column_by_name("__xbbg_present")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(presence.value(0), &[0xff, 0b0000_1101]);
        let times = late
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 1_700_000_000_001_000);
        let topics = late
            .column_by_name("topic")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(topics.value(0), "ES1 Index");
        assert_eq!(topics.value(4), "IBM US Equity");
    }

    #[test]
    fn replay_flushes_partial_batches_without_losing_or_duplicating_rows() {
        for rows in [0, 1, 3, 13] {
            for flush_threshold in [1, 2, 4, 8, 64] {
                let mut timestamps = Vec::new();
                replay(rows, flush_threshold, |batch| {
                    assert!(batch.num_rows() > 0);
                    assert!(batch.num_rows() <= flush_threshold);
                    let times = batch
                        .column_by_name("timestamp")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    timestamps.extend_from_slice(times.values());
                });
                assert_eq!(timestamps.len(), rows);
                for (row, timestamp) in timestamps.into_iter().enumerate() {
                    assert_eq!(timestamp, 1_700_000_000_000_000 + row as i64 * 250);
                }
            }
        }
    }
    #[test]
    fn synthetic_subscriptions_materialize_values_and_partial_final_batch() {
        let mut batches = Vec::new();
        batch_updates(BATCH_SIZE + 3, 3, 3, |batch| batches.push(batch));
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), BATCH_SIZE);
        assert_eq!(batches[1].num_rows(), 3);
        let mut row = 0;
        for batch in &batches {
            assert_eq!(batch.num_columns(), 6);
            assert_eq!(
                batch.schema().metadata()["xbbg.subscription_presence"],
                "column=__xbbg_present;encoding=binary-lsb-first;mapping=bit-i-to-schema-field-(i+2)"
            );
            let topics = batch
                .column_by_name("topic")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let times = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let presence = batch
                .column_by_name("__xbbg_present")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for offset in 0..batch.num_rows() {
                let topic = row % 3;
                assert_eq!(topics.value(offset), format!("SYN{topic:05} US Equity"));
                assert_eq!(
                    times.value(offset),
                    1_700_000_000_000_000 + row as i64 * 250
                );
                assert_eq!(presence.value(offset), &[0b0000_0111]);
                for index in 0..3 {
                    let values = batch
                        .column(index + 2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    assert_eq!(values.null_count(), 0);
                    assert_eq!(
                        values.value(offset),
                        ((topic + index + row) % 10_000) as f64 * 0.0001
                    );
                }
                row += 1;
            }
        }
        assert_eq!(row, BATCH_SIZE + 3);
    }

    #[test]
    fn synthetic_subscriptions_emit_no_empty_or_extra_batches() {
        for messages in [0, 1, BATCH_SIZE, BATCH_SIZE * 2] {
            let mut rows = 0;
            let mut batches = 0;
            batch_updates(messages, 1, 1, |batch| {
                assert!(batch.num_rows() > 0);
                rows += batch.num_rows();
                batches += 1;
            });
            assert_eq!(rows, messages);
            assert_eq!(batches, messages.div_ceil(BATCH_SIZE));
        }
    }

    #[test]
    fn shared_bql_scenarios_have_deterministic_columns() {
        for (_, rows, fields) in BQL_JSON_SCENARIOS {
            let fixture = bql_json_fixture(rows, fields);
            assert_eq!(fixture, bql_json_fixture(rows, fields));
            let json: serde_json::Value = serde_json::from_str(&fixture).unwrap();
            let results = json["results"].as_object().unwrap();
            assert_eq!(results.len(), fields.len());
            for (field_idx, field) in fields.iter().enumerate() {
                let result = &results[*field];
                let ids = result["idColumn"]["values"].as_array().unwrap();
                let values = result["valuesColumn"]["values"].as_array().unwrap();
                let secondary = result["secondaryColumns"].as_array().unwrap();
                assert_eq!(ids.len(), rows);
                assert_eq!(values.len(), rows);
                assert_eq!(secondary[0]["values"].as_array().unwrap().len(), rows);
                assert_eq!(secondary[1]["values"].as_array().unwrap().len(), rows);
                assert_eq!(ids[0], "TICKER0 US Equity");
                assert_eq!(ids[rows - 1], format!("TICKER{} US Equity", rows - 1));
                assert_eq!(values[0].as_f64().unwrap(), 100.0 + field_idx as f64);
                assert_eq!(
                    values[rows - 1].as_f64().unwrap(),
                    100.0 + field_idx as f64 + (rows - 1) as f64 / 100.0
                );
                assert_eq!(secondary[0]["values"][0], "2026-04-01");
                assert_eq!(
                    secondary[0]["values"][rows - 1],
                    format!("2026-04-{:02}", ((rows - 1) % 28) + 1)
                );
                assert_eq!(secondary[1]["values"][rows - 1], "USD");
            }
        }
    }

    #[test]
    fn shared_bql_scenarios_materialize_through_production_extractor() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let state = BqlState::new(tx);
        for (_, rows, fields) in BQL_JSON_SCENARIOS {
            let batch = state
                .parse_bql_json_for_bench(&bql_json_fixture(rows, fields))
                .unwrap();
            assert_eq!(batch.num_rows(), rows);
            assert_eq!(batch.num_columns(), fields.len() + 3);
        }
    }
}
