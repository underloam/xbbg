//! Offline request-decode benchmark over real SDK messages.
//!
//! Builds Bloomberg-shaped responses with the SDK TestUtil fixtures (no
//! session, no network) and replays them through the production extractor
//! states in every output mode. Timing covers state creation, element
//! traversal, value extraction, Arrow append and RecordBatch finalization.
//!
//! Run:
//!   DECODE_ITERATIONS=9 cargo bench --package xbbg-bench --bench request_decode_replay

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use chrono::{Duration as ChronoDuration, NaiveDate};
use tokio::sync::oneshot;
use xbbg_async::engine::state::typed_builder::ArrowType;
use xbbg_async::engine::{BulkDataState, HistDataState, LongMode, OutputFormat, RefDataState};
use xbbg_bench::write_json;
use xbbg_core::test_support::TestEvent;
use xbbg_core::{BlpError, EventType, Message};

const HIST_FIELDS: [&str; 3] = ["PX_LAST", "PX_OPEN", "PX_VOLUME"];
const REF_FIELDS: [&str; 5] = ["PX_LAST", "NAME", "CRNCY", "VOLUME", "CUR_MKT_CAP"];
const BULK_FIELD: &str = "INDX_MWEIGHT";
const REF_SECURITIES_PER_MESSAGE: usize = 100;
const BULK_SECURITIES_PER_MESSAGE: usize = 10;

const HIST_SCHEMA: &str = r#"<ServiceDefinition name="xbbg.bench.history" version="1.0.0.0">
    <service name="//xbbg/bench/history" version="1.0.0.0">
        <event name="HistoricalDataResponse" eventType="HistoryResponse"/>
    </service>
    <schema>
        <sequenceType name="HistoryResponse">
            <element name="securityData" type="HistorySecurity"/>
        </sequenceType>
        <sequenceType name="HistorySecurity">
            <element name="security" type="String"/>
            <element name="fieldData" type="HistoryRow" minOccurs="0" maxOccurs="unbounded"/>
        </sequenceType>
        <sequenceType name="HistoryRow">
            <element name="date" type="Date"/>
            <element name="PX_LAST" type="Float64" minOccurs="0"/>
            <element name="PX_OPEN" type="Float64" minOccurs="0"/>
            <element name="PX_VOLUME" type="Int64" minOccurs="0"/>
        </sequenceType>
    </schema>
</ServiceDefinition>"#;

const REF_SCHEMA: &str = r#"<ServiceDefinition name="xbbg.bench.reference" version="1.0.0.0">
    <service name="//xbbg/bench/reference" version="1.0.0.0">
        <event name="ReferenceDataResponse" eventType="ReferenceResponse"/>
    </service>
    <schema>
        <sequenceType name="ReferenceResponse">
            <element name="securityData" type="ReferenceSecurity" maxOccurs="unbounded"/>
        </sequenceType>
        <sequenceType name="ReferenceSecurity">
            <element name="security" type="String"/>
            <element name="fieldData" type="ReferenceFields"/>
        </sequenceType>
        <sequenceType name="ReferenceFields">
            <element name="PX_LAST" type="Float64" minOccurs="0"/>
            <element name="NAME" type="String" minOccurs="0"/>
            <element name="CRNCY" type="String" minOccurs="0"/>
            <element name="VOLUME" type="Int64" minOccurs="0"/>
            <element name="CUR_MKT_CAP" type="Float64" minOccurs="0"/>
        </sequenceType>
    </schema>
</ServiceDefinition>"#;

const BULK_SCHEMA: &str = r#"<ServiceDefinition name="xbbg.bench.bulk" version="1.0.0.0">
    <service name="//xbbg/bench/bulk" version="1.0.0.0">
        <event name="ReferenceDataResponse" eventType="BulkResponse"/>
    </service>
    <schema>
        <sequenceType name="BulkResponse">
            <element name="securityData" type="BulkSecurity" maxOccurs="unbounded"/>
        </sequenceType>
        <sequenceType name="BulkSecurity">
            <element name="security" type="String"/>
            <element name="fieldData" type="BulkFields"/>
        </sequenceType>
        <sequenceType name="BulkFields">
            <element name="INDX_MWEIGHT" type="BulkRow" maxOccurs="unbounded"/>
        </sequenceType>
        <sequenceType name="BulkRow">
            <element name="MEMBER" type="String" minOccurs="0"/>
            <element name="WEIGHT" type="Float64" minOccurs="0"/>
            <element name="SHARES" type="Int64" minOccurs="0"/>
        </sequenceType>
    </schema>
</ServiceDefinition>"#;

#[derive(Clone, Copy, Debug)]
struct Shape {
    hist_securities: usize,
    hist_dates: usize,
    ref_securities: usize,
    bulk_securities: usize,
    bulk_rows: usize,
    iterations: usize,
    warmup: usize,
}

impl Shape {
    fn from_env() -> Self {
        Self {
            hist_securities: env_usize("DECODE_HIST_SECURITIES", 400),
            hist_dates: env_usize("DECODE_HIST_DATES", 252),
            ref_securities: env_usize("DECODE_REF_SECURITIES", 4_000),
            bulk_securities: env_usize("DECODE_BULK_SECURITIES", 100),
            bulk_rows: env_usize("DECODE_BULK_ROWS", 500),
            iterations: env_usize("DECODE_ITERATIONS", 9),
            warmup: env_usize("BENCH_WARMUP", 2),
        }
    }

    fn descriptor(&self) -> String {
        format!(
            "implementation=request_decode_replay;hist={}x{}x{};ref={}x{};bulk={}x{}x3;iterations={};warmup={}",
            self.hist_securities,
            self.hist_dates,
            HIST_FIELDS.len(),
            self.ref_securities,
            REF_FIELDS.len(),
            self.bulk_securities,
            self.bulk_rows,
            self.iterations,
            self.warmup,
        )
    }
}

/// One cached response: partial events followed by the final response event.
struct Fixture {
    partials: Vec<TestEvent>,
    last: TestEvent,
    values: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn events(schema: &str, message_type: &str, payloads: Vec<String>, values: usize) -> Fixture {
    let mut payloads = payloads.into_iter();
    let first = payloads.next().expect("at least one payload");
    let rest: Vec<String> = payloads.collect();
    if rest.is_empty() {
        return Fixture {
            partials: Vec::new(),
            last: TestEvent::with_schema(schema, EventType::Response, message_type, &[], |f| {
                f.json(&first)
            }),
            values,
        };
    }
    let (middle, last) = rest.split_at(rest.len() - 1);
    let mut partial =
        TestEvent::with_schema(schema, EventType::PartialResponse, message_type, &[], |f| {
            f.json(&first)
        });
    for payload in middle {
        partial.append_message(message_type, &[], |f| f.json(payload));
    }
    Fixture {
        partials: vec![partial],
        last: TestEvent::with_schema(schema, EventType::Response, message_type, &[], |f| {
            f.json(&last[0])
        }),
        values,
    }
}

fn hist_fixture(shape: &Shape) -> Fixture {
    let base = NaiveDate::from_ymd_opt(2024, 1, 1).expect("valid date");
    let payloads = (0..shape.hist_securities)
        .map(|s| {
            let rows: Vec<serde_json::Value> = (0..shape.hist_dates)
                .map(|d| {
                    let date = base + ChronoDuration::days(d as i64);
                    serde_json::json!({
                        "date": date.format("%Y-%m-%d").to_string(),
                        "PX_LAST": 100.0 + s as f64 * 0.01 + d as f64 * 0.25,
                        "PX_OPEN": 99.5 + s as f64 * 0.01 + d as f64 * 0.25,
                        "PX_VOLUME": 1_000_000_i64 + (s * 1_000 + d) as i64,
                    })
                })
                .collect();
            serde_json::json!({
                "securityData": {
                    "security": format!("SYN{s:06} US Equity"),
                    "fieldData": rows,
                }
            })
            .to_string()
        })
        .collect();
    events(
        HIST_SCHEMA,
        "HistoricalDataResponse",
        payloads,
        shape.hist_securities * shape.hist_dates * HIST_FIELDS.len(),
    )
}

fn ref_fixture(shape: &Shape) -> Fixture {
    let securities: Vec<serde_json::Value> = (0..shape.ref_securities)
        .map(|s| {
            serde_json::json!({
                "security": format!("SYN{s:06} US Equity"),
                "fieldData": {
                    "PX_LAST": 100.0 + s as f64 * 0.01,
                    "NAME": format!("SYNTHETIC COMPANY {s}"),
                    "CRNCY": if s % 3 == 0 { "EUR" } else { "USD" },
                    "VOLUME": 10_000_i64 + s as i64,
                    "CUR_MKT_CAP": 1.0e9 + s as f64 * 1.0e6,
                },
            })
        })
        .collect();
    let payloads = securities
        .chunks(REF_SECURITIES_PER_MESSAGE)
        .map(|chunk| serde_json::json!({ "securityData": chunk }).to_string())
        .collect();
    events(
        REF_SCHEMA,
        "ReferenceDataResponse",
        payloads,
        shape.ref_securities * REF_FIELDS.len(),
    )
}

fn bulk_fixture(shape: &Shape) -> Fixture {
    let securities: Vec<serde_json::Value> = (0..shape.bulk_securities)
        .map(|s| {
            let rows: Vec<serde_json::Value> = (0..shape.bulk_rows)
                .map(|r| {
                    serde_json::json!({
                        "MEMBER": format!("M{r:05} UN"),
                        "WEIGHT": (r % 97) as f64 * 0.01,
                        "SHARES": 1_000_000_i64 + r as i64,
                    })
                })
                .collect();
            serde_json::json!({
                "security": format!("IDX{s:04} Index"),
                "fieldData": { BULK_FIELD: rows },
            })
        })
        .collect();
    let payloads = securities
        .chunks(BULK_SECURITIES_PER_MESSAGE)
        .map(|chunk| serde_json::json!({ "securityData": chunk }).to_string())
        .collect();
    events(
        BULK_SCHEMA,
        "ReferenceDataResponse",
        payloads,
        shape.bulk_securities * shape.bulk_rows * 3,
    )
}

/// Production extractor state under test.
enum State {
    Hist(HistDataState),
    Ref(RefDataState),
    Bulk(BulkDataState),
}

impl State {
    fn on_partial(&mut self, msg: &Message<'_>) {
        match self {
            Self::Hist(state) => state.on_partial(msg),
            Self::Ref(state) => state.on_partial(msg),
            Self::Bulk(state) => state.on_partial(msg),
        }
    }

    fn finish(self, msg: &Message<'_>) {
        match self {
            Self::Hist(state) => state.finish(msg),
            Self::Ref(state) => state.finish(msg),
            Self::Bulk(state) => state.finish(msg),
        }
    }
}

type Reply = oneshot::Receiver<Result<RecordBatch, BlpError>>;

struct Case {
    name: &'static str,
    fixture: usize,
    make: Box<dyn Fn() -> (State, Reply)>,
}

struct Outcome {
    name: &'static str,
    values: usize,
    best_ms: f64,
    median_ms: f64,
    rows: usize,
    columns: usize,
    bytes: usize,
}

fn fields(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

fn numeric_hist_hints() -> HashMap<String, ArrowType> {
    [
        ("PX_LAST".to_string(), ArrowType::Float64),
        ("PX_OPEN".to_string(), ArrowType::Float64),
        ("PX_VOLUME".to_string(), ArrowType::Int64),
    ]
    .into_iter()
    .collect()
}

fn hist_case(
    name: &'static str,
    format: OutputFormat,
    mode: LongMode,
    hints: Option<HashMap<String, ArrowType>>,
) -> Case {
    Case {
        name,
        fixture: 0,
        make: Box::new(move || {
            let (tx, rx) = oneshot::channel();
            let state =
                HistDataState::with_format(fields(&HIST_FIELDS), format, mode, hints.clone(), tx);
            (State::Hist(state), rx)
        }),
    }
}

fn ref_case(name: &'static str, format: OutputFormat, mode: LongMode) -> Case {
    Case {
        name,
        fixture: 1,
        make: Box::new(move || {
            let (tx, rx) = oneshot::channel();
            let state =
                RefDataState::with_format(fields(&REF_FIELDS), format, mode, None, false, tx);
            (State::Ref(state), rx)
        }),
    }
}

fn cases() -> Vec<Case> {
    vec![
        hist_case(
            "bdh long (default, no hints)",
            OutputFormat::Long,
            LongMode::String,
            None,
        ),
        hist_case(
            "bdh long (default, numeric hints)",
            OutputFormat::Long,
            LongMode::String,
            Some(numeric_hist_hints()),
        ),
        hist_case("bdh long_typed", OutputFormat::Long, LongMode::Typed, None),
        hist_case(
            "bdh long_metadata",
            OutputFormat::Long,
            LongMode::WithMetadata,
            None,
        ),
        hist_case("bdh semi_long", OutputFormat::Wide, LongMode::String, None),
        ref_case("bdp long (default)", OutputFormat::Long, LongMode::String),
        ref_case("bdp long_typed", OutputFormat::Long, LongMode::Typed),
        ref_case(
            "bdp long_metadata",
            OutputFormat::Long,
            LongMode::WithMetadata,
        ),
        ref_case("bdp semi_long", OutputFormat::Wide, LongMode::String),
        Case {
            name: "bds bulk",
            fixture: 2,
            make: Box::new(|| {
                let (tx, rx) = oneshot::channel();
                (
                    State::Bulk(BulkDataState::new(BULK_FIELD.to_string(), tx)),
                    rx,
                )
            }),
        },
    ]
}

fn replay(fixture: &Fixture, make: &dyn Fn() -> (State, Reply)) -> RecordBatch {
    let (mut state, mut rx) = make();
    for event in &fixture.partials {
        let mut messages = event.event().messages();
        while let Some(msg) = messages.next() {
            state.on_partial(&msg);
        }
    }
    if let Some(msg) = fixture.last.event().messages().next() {
        state.finish(&msg);
    }
    rx.try_recv()
        .expect("extractor replied")
        .expect("extractor produced a batch")
}

fn run(case: &Case, fixture: &Fixture, shape: &Shape) -> Outcome {
    for _ in 0..shape.warmup {
        black_box(replay(fixture, case.make.as_ref()));
    }
    let mut samples = Vec::with_capacity(shape.iterations);
    let mut last = None;
    for _ in 0..shape.iterations {
        let start = Instant::now();
        let batch = replay(fixture, case.make.as_ref());
        samples.push(start.elapsed().as_secs_f64() * 1e3);
        last = Some(black_box(batch));
    }
    samples.sort_by(f64::total_cmp);
    let batch = last.expect("at least one iteration");
    Outcome {
        name: case.name,
        values: fixture.values,
        best_ms: samples[0],
        median_ms: samples[samples.len() / 2],
        rows: batch.num_rows(),
        columns: batch.num_columns(),
        bytes: batch.get_array_memory_size(),
    }
}

fn main() {
    let shape = Shape::from_env();
    let build = Instant::now();
    let fixtures = [
        hist_fixture(&shape),
        ref_fixture(&shape),
        bulk_fixture(&shape),
    ];
    println!(
        "\nrequest_decode_replay: fixtures built in {:.0} ms ({})",
        build.elapsed().as_secs_f64() * 1e3,
        shape.descriptor()
    );
    println!("| case | best ms | median ms | ns/value | rows | cols | Arrow bytes |");
    println!("|---|---:|---:|---:|---:|---:|---:|");
    let mut outcomes = Vec::new();
    for case in cases() {
        let outcome = run(&case, &fixtures[case.fixture], &shape);
        println!(
            "| {} | {:.2} | {:.2} | {:.1} | {} | {} | {} |",
            outcome.name,
            outcome.best_ms,
            outcome.median_ms,
            outcome.best_ms * 1e6 / outcome.values as f64,
            outcome.rows,
            outcome.columns,
            outcome.bytes
        );
        outcomes.push(outcome);
    }
    write_results(&shape, &outcomes);
}

fn write_results(shape: &Shape, outcomes: &[Outcome]) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after UNIX_EPOCH")
        .as_secs();
    let cases = outcomes
        .iter()
        .map(|outcome| {
            format!(
                r#"    {{"case": "{}", "values": {}, "best_ms": {:.3}, "median_ms": {:.3}, "best_ns_per_value": {:.2}, "rows": {}, "columns": {}, "arrow_bytes": {}}}"#,
                outcome.name,
                outcome.values,
                outcome.best_ms,
                outcome.median_ms,
                outcome.best_ms * 1e6 / outcome.values as f64,
                outcome.rows,
                outcome.columns,
                outcome.bytes
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let json = format!(
        r#"{{
  "schema_version": 1,
  "timestamp": {timestamp},
  "crate": "xbbg-async",
  "benchmark_type": "request_decode_replay",
  "offline": true,
  "uses_bloomberg_session": false,
  "coverage": "SDK TestUtil response messages through production HistDataState, RefDataState and BulkDataState in every output mode",
  "timing_scope": "state creation, element traversal, value extraction, Arrow append and RecordBatch finalization; fixture construction excluded",
  "provenance": {},
  "cases": [
{cases}
  ]
}}"#,
        xbbg_bench::benchmark_provenance_json(&shape.descriptor()),
    );
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    write_json(
        &dir.join(format!("request_decode_replay_{timestamp}.json")),
        &json,
    );
    write_json(&dir.join("request_decode_replay_latest.json"), &json);
}
