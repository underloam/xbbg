//! One-command xbbg benchmark suite.
//!
//! Runs tiny live Bloomberg probes plus large synthetic workloads in one report.
//! Live probes intentionally keep Bloomberg data usage low; synthetic workloads
//! provide scale without additional Bloomberg requests.
#![allow(clippy::result_large_err, clippy::too_many_arguments)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{Datelike, Duration as ChronoDuration, Local, NaiveDate, Weekday};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use xbbg_async::engine::state::typed_builder::{ArrowType, TypedBuilder};
use xbbg_async::engine::state::{
    subscription_channel, FieldKind, FieldLayout, FieldMeta, MessageOutcome,
    SubscriptionArrowBatcher, SubscriptionUpdate, UpdateField, UpdateValue,
};
use xbbg_async::engine::{
    BqlState, BulkDataState, Engine, EngineConfig, ExtractorType, HistDataState, IntradayTickState,
    LongMode, OutputFormat, RefDataState, RequestParams, ServerAddr, SubscriptionState, Transport,
};
use xbbg_async::BlpAsyncError;
use xbbg_bench::{open_service, setup_session};
use xbbg_core::{
    BlpError, CorrelationId, DataType as BlpDataType, Element, Event, EventType, Message, Name,
    SubscriptionList,
};

fn subscription_update_shape(update: &SubscriptionUpdate) -> (usize, usize) {
    (1, update.layout.fields.len() + 3)
}
struct TrackingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[derive(Clone, Copy, Debug)]
enum BenchProfile {
    Smoke,
    Standard,
    Stress,
}

impl BenchProfile {
    fn from_env() -> Self {
        match std::env::var("BENCH_PROFILE")
            .unwrap_or_else(|_| "standard".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "smoke" => Self::Smoke,
            "stress" => Self::Stress,
            _ => Self::Standard,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Standard => "standard",
            Self::Stress => "stress",
        }
    }

    fn synthetic_shape(self) -> SyntheticShape {
        match self {
            Self::Smoke => SyntheticShape {
                bdp_securities: 1_000,
                bdp_fields: 5,
                bdh_securities: 100,
                bdh_dates: 20,
                bdh_fields: 3,
                bdtick_ticks: 100_000,
                bql_rows: 10_000,
                bql_columns: 10,
                sub_messages: 100_000,
                sub_topics: 10,
                sub_fields: 3,
            },
            Self::Standard => SyntheticShape {
                bdp_securities: 10_000,
                bdp_fields: 10,
                bdh_securities: 1_000,
                bdh_dates: 252,
                bdh_fields: 3,
                bdtick_ticks: 1_000_000,
                bql_rows: 100_000,
                bql_columns: 10,
                sub_messages: 1_000_000,
                sub_topics: 100,
                sub_fields: 3,
            },
            Self::Stress => SyntheticShape {
                bdp_securities: 100_000,
                bdp_fields: 20,
                bdh_securities: 10_000,
                bdh_dates: 252,
                bdh_fields: 5,
                bdtick_ticks: 10_000_000,
                bql_rows: 1_000_000,
                bql_columns: 10,
                sub_messages: 10_000_000,
                sub_topics: 1_000,
                sub_fields: 3,
            },
        }
    }

    fn subscription_collect_ms(self) -> u64 {
        let default = match self {
            Self::Smoke => 2_000,
            Self::Standard => 5_000,
            Self::Stress => 10_000,
        };
        env_u64("BENCH_SUB_COLLECT_MS", default)
    }

    fn replay_iterations(self) -> usize {
        let default = match self {
            Self::Smoke => 100,
            Self::Standard => 1_000,
            Self::Stress => 5_000,
        };
        env_usize("BENCH_REPLAY_ITERATIONS", default)
    }

    fn bql_json_iterations(self) -> usize {
        let default = match self {
            Self::Smoke => 10_000,
            Self::Standard => 50_000,
            Self::Stress => 200_000,
        };
        env_usize("BENCH_BQL_JSON_ITERATIONS", default)
    }

    fn subscription_replay_messages(self) -> usize {
        let default = match self {
            Self::Smoke => 10_000,
            Self::Standard => 1_000_000,
            Self::Stress => 10_000_000,
        };
        env_usize("BENCH_SUB_REPLAY_MESSAGES", default)
    }

    fn subscription_replay_topics(self) -> usize {
        let default = match self {
            Self::Smoke => 10,
            Self::Standard => 1_000,
            Self::Stress => 10_000,
        };
        env_usize("BENCH_SUB_REPLAY_TOPICS", default)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileMode {
    Normal,
    Detail,
}

impl ProfileMode {
    fn from_env() -> Self {
        match std::env::var("BENCH_PROFILE_MODE")
            .unwrap_or_else(|_| "none".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "detail" => Self::Detail,
            _ => Self::Normal,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "none",
            Self::Detail => "detail",
        }
    }

    fn is_detail(self) -> bool {
        self == Self::Detail
    }
}

#[derive(Clone, Debug)]
struct SuiteConfig {
    profile: BenchProfile,
    profile_mode: ProfileMode,
    only: Option<String>,
}

impl SuiteConfig {
    fn from_env() -> Self {
        Self {
            profile: BenchProfile::from_env(),
            profile_mode: ProfileMode::from_env(),
            only: std::env::var("BENCH_ONLY")
                .ok()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty()),
        }
    }

    fn should_run(&self, suite: &str, scenario: &str) -> bool {
        let Some(only) = &self.only else {
            return true;
        };
        suite.to_ascii_lowercase().contains(only) || scenario.to_ascii_lowercase().contains(only)
    }

    fn should_run_explicit(&self, scenario: &str) -> bool {
        self.only.as_deref() == Some(&scenario.to_ascii_lowercase())
    }
}

#[derive(Clone, Copy, Debug)]
struct SyntheticShape {
    bdp_securities: usize,
    bdp_fields: usize,
    bdh_securities: usize,
    bdh_dates: usize,
    bdh_fields: usize,
    bdtick_ticks: usize,
    bql_rows: usize,
    bql_columns: usize,
    sub_messages: usize,
    sub_topics: usize,
    sub_fields: usize,
}

#[derive(Clone, Debug)]
struct PhaseMetric {
    name: &'static str,
    elapsed_us: u128,
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocSnapshot {
    alloc_count: u64,
    alloc_bytes: u64,
    dealloc_count: u64,
    dealloc_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct AllocDelta {
    alloc_count: u64,
    alloc_bytes: u64,
    dealloc_count: u64,
    dealloc_bytes: u64,
    net_alloc_bytes: i128,
    allocs_per_row: f64,
    bytes_per_row: f64,
    allocs_per_value: f64,
    bytes_per_value: f64,
}

#[derive(Debug)]
struct BenchRecord {
    suite: &'static str,
    scenario: String,
    status: String,
    elapsed_us: u128,
    rows: usize,
    columns: usize,
    values: usize,
    throughput_name: &'static str,
    throughput_per_sec: f64,
    detail: String,
    phases: Vec<PhaseMetric>,
    allocations: Option<AllocDelta>,
}

impl BenchRecord {
    fn ok(
        suite: &'static str,
        scenario: impl Into<String>,
        elapsed: Duration,
        rows: usize,
        columns: usize,
        values: usize,
        throughput_name: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        let elapsed_secs = elapsed.as_secs_f64().max(0.000_001);
        let throughput_per_sec = values as f64 / elapsed_secs;
        Self {
            suite,
            scenario: scenario.into(),
            status: "ok".to_string(),
            elapsed_us: elapsed.as_micros(),
            rows,
            columns,
            values,
            throughput_name,
            throughput_per_sec,
            detail: detail.into(),
            phases: Vec::new(),
            allocations: None,
        }
    }

    fn error(
        suite: &'static str,
        scenario: impl Into<String>,
        elapsed: Duration,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            suite,
            scenario: scenario.into(),
            status: "error".to_string(),
            elapsed_us: elapsed.as_micros(),
            rows: 0,
            columns: 0,
            values: 0,
            throughput_name: "items",
            throughput_per_sec: 0.0,
            detail: detail.into(),
            phases: Vec::new(),
            allocations: None,
        }
    }
}

fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        alloc_count: ALLOC_COUNT.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_count: DEALLOC_COUNT.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn alloc_delta(
    before: AllocSnapshot,
    after: AllocSnapshot,
    rows: usize,
    values: usize,
) -> AllocDelta {
    let alloc_count = after.alloc_count.saturating_sub(before.alloc_count);
    let alloc_bytes = after.alloc_bytes.saturating_sub(before.alloc_bytes);
    let dealloc_count = after.dealloc_count.saturating_sub(before.dealloc_count);
    let dealloc_bytes = after.dealloc_bytes.saturating_sub(before.dealloc_bytes);
    let row_divisor = rows.max(1) as f64;
    let value_divisor = values.max(1) as f64;
    AllocDelta {
        alloc_count,
        alloc_bytes,
        dealloc_count,
        dealloc_bytes,
        net_alloc_bytes: alloc_bytes as i128 - dealloc_bytes as i128,
        allocs_per_row: alloc_count as f64 / row_divisor,
        bytes_per_row: alloc_bytes as f64 / row_divisor,
        allocs_per_value: alloc_count as f64 / value_divisor,
        bytes_per_value: alloc_bytes as f64 / value_divisor,
    }
}

fn profile_record<F>(
    config: &SuiteConfig,
    suite: &'static str,
    scenario: &str,
    mut run: F,
) -> BenchRecord
where
    F: FnMut(bool) -> BenchRecord,
{
    if !config.profile_mode.is_detail() {
        return run(false);
    }

    // Keep reported timings free of allocator-counter traffic. A second,
    // untimed execution supplies allocation metrics for the same scenario.
    let mut timing_record = run(true);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    let before = alloc_snapshot();
    let allocation_record = run(false);
    let after = alloc_snapshot();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);

    if timing_record.status != allocation_record.status
        || timing_record.rows != allocation_record.rows
        || timing_record.values != allocation_record.values
    {
        timing_record.status = "error".to_string();
        timing_record.detail = format!(
            "{}; separate allocation observation changed outcome for {scenario}",
            timing_record.detail
        );
    }
    if timing_record.phases.is_empty() {
        timing_record.phases.push(PhaseMetric {
            name: suite,
            elapsed_us: timing_record.elapsed_us,
        });
    }
    timing_record.allocations = Some(alloc_delta(
        before,
        after,
        allocation_record.rows,
        allocation_record.values,
    ));
    timing_record
}

fn phase(name: &'static str, elapsed: Duration) -> PhaseMetric {
    PhaseMetric {
        name,
        elapsed_us: elapsed.as_micros(),
    }
}

fn main() {
    let config = SuiteConfig::from_env();
    let profile = config.profile;
    let shape = profile.synthetic_shape();
    let timestamp = now_secs();
    let git_sha = git_sha();

    println!("xbbg benchmark suite");
    println!("====================\n");
    println!("Profile: {}", profile.as_str());
    println!("Profile mode: {}", config.profile_mode.as_str());
    if let Some(only) = &config.only {
        println!("Scenario filter: {only}");
    }
    println!("Git SHA: {}", git_sha);
    println!("Bloomberg: {}:{}", blp_host(), blp_port());
    suppress_blpapi_warnings();
    println!();
    print_usage(profile, shape);

    let mut records = Vec::new();

    if should_run_live(&config) {
        println!("\n[1/4] Live Bloomberg probes");
        let rt = Runtime::new().expect("tokio runtime");
        let live_records = match create_engine() {
            Ok(engine) => {
                let records = rt.block_on(run_live_suite(&engine, &config));
                drop(engine);
                records
            }
            Err(err) => {
                let detail = format!("failed to start engine: {err}");
                [
                    "bdp_smoke",
                    "bdh_smoke",
                    "bdtick_smoke",
                    "bql_smoke",
                    "subscription_live",
                ]
                .into_iter()
                .filter(|scenario| {
                    config.should_run("live", scenario)
                        || config.should_run("live_requests", scenario)
                        || config.should_run("live_subscriptions", scenario)
                })
                .map(|scenario| {
                    BenchRecord::error("live", scenario, Duration::ZERO, detail.clone())
                })
                .collect()
            }
        };
        records.extend(live_records);
    } else {
        println!("\n[1/4] Live Bloomberg probes skipped by BENCH_ONLY");
    }

    if should_run_replay(&config) {
        println!("\n[2/4] Cached Bloomberg event replay");
        records.extend(run_replay_suite(&config));
    } else {
        println!("\n[2/4] Cached Bloomberg event replay skipped by BENCH_ONLY");
    }

    println!("\n[3/4] Synthetic massive workloads and offline extractor replays");
    records.extend(run_bql_json_suite(&config));
    if config.should_run(
        "synthetic_bdp",
        &format!("bdp_{}s_{}f", shape.bdp_securities, shape.bdp_fields),
    ) {
        records.push(profile_record(
            &config,
            "synthetic_bdp",
            "synthetic_bdp",
            |_| synthetic_bdp(shape, config.profile_mode.is_detail()),
        ));
    }
    if config.should_run(
        "synthetic_bdh",
        &format!(
            "bdh_{}s_{}d_{}f",
            shape.bdh_securities, shape.bdh_dates, shape.bdh_fields
        ),
    ) {
        records.push(profile_record(
            &config,
            "synthetic_bdh",
            "synthetic_bdh",
            |_| synthetic_bdh(shape, config.profile_mode.is_detail()),
        ));
    }
    if config.should_run(
        "synthetic_bdtick",
        &format!("bdtick_{}ticks", shape.bdtick_ticks),
    ) {
        records.push(profile_record(
            &config,
            "synthetic_bdtick",
            "synthetic_bdtick",
            |_| synthetic_bdtick(shape, config.profile_mode.is_detail()),
        ));
    }
    if config.should_run(
        "synthetic_bql",
        &format!("bql_{}r_{}c", shape.bql_rows, shape.bql_columns),
    ) {
        records.push(profile_record(
            &config,
            "synthetic_bql",
            "synthetic_bql",
            |_| synthetic_bql(shape, config.profile_mode.is_detail()),
        ));
    }
    if config.should_run(
        "synthetic_subscriptions",
        &format!(
            "sub_{}topics_{}messages_{}fields",
            shape.sub_topics, shape.sub_messages, shape.sub_fields
        ),
    ) {
        records.push(profile_record(
            &config,
            "synthetic_subscriptions",
            "synthetic_subscriptions",
            |_| synthetic_subscriptions(shape, config.profile_mode.is_detail()),
        ));
    }

    println!("\n[4/4] Summary");
    print_summary(&records);

    let json = render_json(&config, timestamp, &git_sha, shape, &records);
    let markdown = render_markdown(&config, timestamp, &git_sha, shape, &records);
    write_results(timestamp, &json, &markdown);
}

fn suppress_blpapi_warnings() {
    unsafe {
        let _ = xbbg_core::ffi::blpapi_Logging_registerCallback(
            None,
            xbbg_core::ffi::blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_ERROR
                as xbbg_core::ffi::blpapi_Logging_Severity_t,
        );
    }
}

fn should_run_live(config: &SuiteConfig) -> bool {
    config.should_run("live", "bdp_smoke")
        || config.should_run("live_requests", "bdp_smoke")
        || config.should_run("live_requests", "bdh_smoke")
        || config.should_run("live_requests", "bdtick_smoke")
        || config.should_run("live_requests", "bql_smoke")
        || config.should_run("live_subscriptions", "sub_3_topics_3_fields")
}

fn should_run_replay(config: &SuiteConfig) -> bool {
    config.should_run("replay", "bdp_refdata")
        || config.should_run("replay", "bdp_refdata_typed_hints")
        || config.should_run_explicit("bdp_refdata_wide")
        || config.should_run_explicit("bdp_refdata_wide_typed_hints")
        || config.should_run_explicit("bdp_refdata_output_wide")
        || config.should_run_explicit("bdp_refdata_output_wide_typed_hints")
        || config.should_run("replay", "bdh_historical")
        || config.should_run("replay", "bdh_historical_typed_hints")
        || config.should_run_explicit("bdh_historical_wide")
        || config.should_run_explicit("bdh_historical_wide_typed_hints")
        || config.should_run_explicit("bdh_historical_output_wide")
        || config.should_run_explicit("bdh_historical_output_wide_typed_hints")
        || config.should_run("replay", "bds_bulk_late_fields")
        || should_run_bdtick_replay(config)
        || config.should_run("replay", "bql_response")
        || config.should_run("subscription_components", "requested_fields")
        || config.should_run("subscription_components", "all_fields")
        || config.should_run("subscription_replay", "requested_fields")
        || config.should_run("subscription_replay", "all_fields")
        || config.should_run("subscription_replay", "high_message_count")
        || config.should_run("subscription_replay", "high_topic_count")
}

const BDTICK_REPLAY_SCENARIOS: [&str; 4] = [
    "bdtick_optional_fields_prod",
    "bdtick_optional_fields_core_prefix",
    "bdtick_optional_fields_layout_cache",
    "bdtick_optional_fields_unsafe_observed_layout",
];

fn should_run_bdtick_replay(config: &SuiteConfig) -> bool {
    BDTICK_REPLAY_SCENARIOS
        .iter()
        .any(|scenario| config.should_run("replay", scenario))
}

fn run_replay_suite(config: &SuiteConfig) -> Vec<BenchRecord> {
    let mut records = Vec::new();
    let iterations = config.profile.replay_iterations();

    let sess = setup_session();
    open_service(&sess, "//blp/refdata");

    let run_bdp_refdata = config.should_run("replay", "bdp_refdata");
    let run_bdp_typed_hints = config.should_run("replay", "bdp_refdata_typed_hints");
    if run_bdp_refdata || run_bdp_typed_hints {
        match fetch_bdp_events(&sess) {
            Ok(events) => {
                if run_bdp_refdata {
                    records.push(profile_record(config, "replay", "bdp_refdata", |_| {
                        replay_request_events("bdp_refdata", &events, iterations, || {
                            make_refdata_state(vec!["PX_LAST".to_string(), "VOLUME".to_string()])
                        })
                    }));
                }
                if run_bdp_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdp_refdata_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdp_refdata_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_refdata_state_with_types(
                                        vec!["PX_LAST".to_string(), "VOLUME".to_string()],
                                        numeric_field_types(["PX_LAST", "VOLUME"]),
                                    )
                                },
                            )
                        },
                    ));
                }
            }
            Err(err) => {
                if run_bdp_refdata {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdp_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata_typed_hints",
                        Duration::ZERO,
                        err,
                    ));
                }
            }
        }
    }

    let run_bdp_wide = config.should_run_explicit("bdp_refdata_wide");
    let run_bdp_wide_typed_hints = config.should_run_explicit("bdp_refdata_wide_typed_hints");
    let run_bdp_output_wide = config.should_run_explicit("bdp_refdata_output_wide");
    let run_bdp_output_wide_typed_hints =
        config.should_run_explicit("bdp_refdata_output_wide_typed_hints");
    if run_bdp_wide
        || run_bdp_wide_typed_hints
        || run_bdp_output_wide
        || run_bdp_output_wide_typed_hints
    {
        match fetch_bdp_wide_events(&sess) {
            Ok(events) => {
                if run_bdp_wide {
                    records.push(profile_record(config, "replay", "bdp_refdata_wide", |_| {
                        replay_request_events("bdp_refdata_wide", &events, iterations, || {
                            make_refdata_state(field_vec(&WIDE_BDP_FIELDS))
                        })
                    }));
                }
                if run_bdp_wide_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdp_refdata_wide_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdp_refdata_wide_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_refdata_state_with_types(
                                        field_vec(&WIDE_BDP_FIELDS),
                                        numeric_field_types(WIDE_BDP_FIELDS),
                                    )
                                },
                            )
                        },
                    ));
                }
                if run_bdp_output_wide {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdp_refdata_output_wide",
                        |_| {
                            replay_request_events(
                                "bdp_refdata_output_wide",
                                &events,
                                iterations,
                                || {
                                    make_refdata_output_wide_state(
                                        field_vec(&WIDE_BDP_FIELDS),
                                        None,
                                    )
                                },
                            )
                        },
                    ));
                }
                if run_bdp_output_wide_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdp_refdata_output_wide_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdp_refdata_output_wide_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_refdata_output_wide_state(
                                        field_vec(&WIDE_BDP_FIELDS),
                                        numeric_field_types(WIDE_BDP_FIELDS),
                                    )
                                },
                            )
                        },
                    ));
                }
            }
            Err(err) => {
                if run_bdp_wide {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata_wide",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdp_wide_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata_wide_typed_hints",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdp_output_wide {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata_output_wide",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdp_output_wide_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdp_refdata_output_wide_typed_hints",
                        Duration::ZERO,
                        err,
                    ));
                }
            }
        }
    }

    let run_bdh_historical = config.should_run("replay", "bdh_historical");
    let run_bdh_typed_hints = config.should_run("replay", "bdh_historical_typed_hints");
    if run_bdh_historical || run_bdh_typed_hints {
        match fetch_bdh_events(&sess) {
            Ok(events) => {
                if run_bdh_historical {
                    records.push(profile_record(config, "replay", "bdh_historical", |_| {
                        replay_request_events("bdh_historical", &events, iterations, || {
                            make_histdata_state(vec!["PX_LAST".to_string(), "VOLUME".to_string()])
                        })
                    }));
                }
                if run_bdh_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdh_historical_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdh_historical_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_histdata_state_with_types(
                                        vec!["PX_LAST".to_string(), "VOLUME".to_string()],
                                        numeric_field_types(["PX_LAST", "VOLUME"]),
                                    )
                                },
                            )
                        },
                    ));
                }
            }
            Err(err) => {
                if run_bdh_historical {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdh_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical_typed_hints",
                        Duration::ZERO,
                        err,
                    ));
                }
            }
        }
    }

    let run_bdh_wide = config.should_run_explicit("bdh_historical_wide");
    let run_bdh_wide_typed_hints = config.should_run_explicit("bdh_historical_wide_typed_hints");
    let run_bdh_output_wide = config.should_run_explicit("bdh_historical_output_wide");
    let run_bdh_output_wide_typed_hints =
        config.should_run_explicit("bdh_historical_output_wide_typed_hints");
    if run_bdh_wide
        || run_bdh_wide_typed_hints
        || run_bdh_output_wide
        || run_bdh_output_wide_typed_hints
    {
        match fetch_bdh_wide_events(&sess) {
            Ok(events) => {
                if run_bdh_wide {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdh_historical_wide",
                        |_| {
                            replay_request_events(
                                "bdh_historical_wide",
                                &events,
                                iterations,
                                || make_histdata_state(field_vec(&WIDE_BDH_FIELDS)),
                            )
                        },
                    ));
                }
                if run_bdh_wide_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdh_historical_wide_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdh_historical_wide_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_histdata_state_with_types(
                                        field_vec(&WIDE_BDH_FIELDS),
                                        numeric_field_types(WIDE_BDH_FIELDS),
                                    )
                                },
                            )
                        },
                    ));
                }
                if run_bdh_output_wide {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdh_historical_output_wide",
                        |_| {
                            replay_request_events(
                                "bdh_historical_output_wide",
                                &events,
                                iterations,
                                || {
                                    make_histdata_output_wide_state(
                                        field_vec(&WIDE_BDH_FIELDS),
                                        None,
                                    )
                                },
                            )
                        },
                    ));
                }
                if run_bdh_output_wide_typed_hints {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdh_historical_output_wide_typed_hints",
                        |_| {
                            replay_request_events(
                                "bdh_historical_output_wide_typed_hints",
                                &events,
                                iterations,
                                || {
                                    make_histdata_output_wide_state(
                                        field_vec(&WIDE_BDH_FIELDS),
                                        numeric_field_types(WIDE_BDH_FIELDS),
                                    )
                                },
                            )
                        },
                    ));
                }
            }
            Err(err) => {
                if run_bdh_wide {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical_wide",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdh_wide_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical_wide_typed_hints",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdh_output_wide {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical_output_wide",
                        Duration::ZERO,
                        err.clone(),
                    ));
                }
                if run_bdh_output_wide_typed_hints {
                    records.push(BenchRecord::error(
                        "replay",
                        "bdh_historical_output_wide_typed_hints",
                        Duration::ZERO,
                        err,
                    ));
                }
            }
        }
    }

    if config.should_run("replay", "bds_bulk_late_fields") {
        records.push(match fetch_bds_events(&sess) {
            Ok(events) => profile_record(config, "replay", "bds_bulk_late_fields", |_| {
                replay_request_events("bds_bulk_late_fields", &events, iterations, || {
                    make_bulkdata_state("INDX_MEMBERS".to_string())
                })
            }),
            Err(err) => BenchRecord::error("replay", "bds_bulk_late_fields", Duration::ZERO, err),
        });
    }

    if should_run_bdtick_replay(config) {
        match fetch_bdtick_events(&sess) {
            Ok(events) => {
                let diagnostics = analyze_bdtick_events(&events);
                if config.should_run("replay", "bdtick_optional_fields_prod") {
                    records.push(profile_record(
                        config,
                        "replay",
                        "bdtick_optional_fields_prod",
                        |_| replay_bdtick_prod_events(&events, iterations, &diagnostics),
                    ));
                }
                for variant in BdtickBenchVariant::all() {
                    if config.should_run("replay", variant.scenario()) {
                        records.push(profile_record(config, "replay", variant.scenario(), |_| {
                            replay_bdtick_variant_events(&events, iterations, variant, &diagnostics)
                        }));
                    }
                }
            }
            Err(err) => {
                for scenario in BDTICK_REPLAY_SCENARIOS {
                    if config.should_run("replay", scenario) {
                        records.push(BenchRecord::error(
                            "replay",
                            scenario,
                            Duration::ZERO,
                            err.clone(),
                        ));
                    }
                }
            }
        }
    }

    if config.should_run("replay", "bql_response") {
        open_service(&sess, "//blp/bqlsvc");
        records.push(match fetch_bql_events(&sess) {
            Ok(events) => profile_record(config, "replay", "bql_response", |_| {
                replay_request_events("bql_response", &events, iterations, make_bql_state)
            }),
            Err(err) => BenchRecord::error("replay", "bql_response", Duration::ZERO, err),
        });
    }

    if should_run_subscription_replay(config) {
        open_service(&sess, "//blp/mktdata");
        let collect_ms = config.profile.subscription_collect_ms();
        match fetch_subscription_events(&sess, collect_ms) {
            Ok(events) => {
                if config.should_run("subscription_components", "requested_fields") {
                    records.push(profile_record(
                        config,
                        "subscription_components",
                        "requested_fields",
                        |_| {
                            profile_subscription_components(
                                "requested_fields",
                                &events,
                                config.profile.subscription_replay_messages().min(100_000),
                                false,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
                if config.should_run("subscription_components", "all_fields") {
                    records.push(profile_record(
                        config,
                        "subscription_components",
                        "all_fields",
                        |_| {
                            profile_subscription_components(
                                "all_fields",
                                &events,
                                config.profile.subscription_replay_messages().min(100_000),
                                true,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
                if config.should_run("subscription_replay", "requested_fields") {
                    records.push(profile_record(
                        config,
                        "subscription_replay",
                        "requested_fields",
                        |_| {
                            replay_subscription_events(
                                "requested_fields",
                                &events,
                                config.profile.subscription_replay_messages().min(100_000),
                                1,
                                false,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
                if config.should_run("subscription_replay", "all_fields") {
                    records.push(profile_record(
                        config,
                        "subscription_replay",
                        "all_fields",
                        |_| {
                            replay_subscription_events(
                                "all_fields",
                                &events,
                                config.profile.subscription_replay_messages().min(100_000),
                                1,
                                true,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
                if config.should_run("subscription_replay", "high_message_count") {
                    records.push(profile_record(
                        config,
                        "subscription_replay",
                        "high_message_count",
                        |_| {
                            replay_subscription_events(
                                "high_message_count",
                                &events,
                                config.profile.subscription_replay_messages(),
                                1,
                                false,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
                if config.should_run("subscription_replay", "high_topic_count") {
                    records.push(profile_record(
                        config,
                        "subscription_replay",
                        "high_topic_count",
                        |_| {
                            replay_subscription_events(
                                "high_topic_count",
                                &events,
                                config.profile.subscription_replay_messages().min(100_000),
                                config.profile.subscription_replay_topics(),
                                false,
                                &["LAST_PRICE", "BID", "ASK"],
                            )
                        },
                    ));
                }
            }
            Err(err) => records.push(BenchRecord::error(
                "subscription_replay",
                "capture",
                Duration::ZERO,
                err,
            )),
        }
    }

    sess.stop();
    records
}

fn should_run_subscription_replay(config: &SuiteConfig) -> bool {
    config.should_run("subscription_components", "requested_fields")
        || config.should_run("subscription_components", "all_fields")
        || config.should_run("subscription_replay", "requested_fields")
        || config.should_run("subscription_replay", "all_fields")
        || config.should_run("subscription_replay", "high_message_count")
        || config.should_run("subscription_replay", "high_topic_count")
}

enum ReplayState {
    RefData(RefDataState),
    HistData(HistDataState),
    BulkData(BulkDataState),
    IntradayTick(IntradayTickState),
    Bql(BqlState),
}

impl ReplayState {
    fn on_partial(&mut self, msg: &xbbg_core::Message<'_>) {
        match self {
            Self::RefData(state) => state.on_partial(msg),
            Self::HistData(state) => state.on_partial(msg),
            Self::BulkData(state) => state.on_partial(msg),
            Self::IntradayTick(state) => state.on_partial(msg),
            Self::Bql(state) => state.on_partial(msg),
        }
    }

    fn finish(self, msg: &xbbg_core::Message<'_>) {
        match self {
            Self::RefData(state) => state.finish(msg),
            Self::HistData(state) => state.finish(msg),
            Self::BulkData(state) => state.finish(msg),
            Self::IntradayTick(state) => state.finish(msg),
            Self::Bql(state) => state.finish(msg),
        }
    }
}

fn make_refdata_state(
    fields: Vec<String>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    make_refdata_state_with_types(fields, None)
}

fn make_refdata_state_with_types(
    fields: Vec<String>,
    field_types: Option<HashMap<String, String>>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        ReplayState::RefData(RefDataState::with_format(
            fields,
            OutputFormat::Long,
            LongMode::String,
            field_types,
            false,
            tx,
        )),
        rx,
    )
}

fn make_refdata_output_wide_state(
    fields: Vec<String>,
    field_types: Option<HashMap<String, String>>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        ReplayState::RefData(RefDataState::with_format(
            fields,
            OutputFormat::Wide,
            LongMode::String,
            field_types,
            false,
            tx,
        )),
        rx,
    )
}

fn make_histdata_state(
    fields: Vec<String>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    make_histdata_state_with_types(fields, None)
}

fn make_histdata_state_with_types(
    fields: Vec<String>,
    field_types: Option<HashMap<String, String>>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        ReplayState::HistData(HistDataState::with_format(
            fields,
            OutputFormat::Long,
            LongMode::String,
            field_types,
            tx,
        )),
        rx,
    )
}

fn make_histdata_output_wide_state(
    fields: Vec<String>,
    field_types: Option<HashMap<String, String>>,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        ReplayState::HistData(HistDataState::with_format(
            fields,
            OutputFormat::Wide,
            LongMode::String,
            field_types,
            tx,
        )),
        rx,
    )
}

const WIDE_BDP_SECURITIES: [&str; 10] = [
    "IBM US Equity",
    "MSFT US Equity",
    "AAPL US Equity",
    "AMZN US Equity",
    "GOOGL US Equity",
    "META US Equity",
    "NVDA US Equity",
    "TSLA US Equity",
    "JPM US Equity",
    "XOM US Equity",
];

const WIDE_BDP_FIELDS: [&str; 10] = [
    "PX_LAST",
    "VOLUME",
    "BID",
    "ASK",
    "PX_OPEN",
    "PX_HIGH",
    "PX_LOW",
    "CHG_PCT_1D",
    "EQY_SH_OUT",
    "CUR_MKT_CAP",
];

const WIDE_BDH_SECURITIES: [&str; 3] = ["IBM US Equity", "MSFT US Equity", "AAPL US Equity"];
const WIDE_BDH_FIELDS: [&str; 5] = ["PX_LAST", "VOLUME", "PX_OPEN", "PX_HIGH", "PX_LOW"];

fn field_vec(fields: &[&str]) -> Vec<String> {
    fields.iter().map(|field| (*field).to_string()).collect()
}

fn numeric_field_types(
    fields: impl IntoIterator<Item = &'static str>,
) -> Option<HashMap<String, String>> {
    Some(
        fields
            .into_iter()
            .map(|field| (field.to_string(), "float64".to_string()))
            .collect(),
    )
}

fn make_bulkdata_state(
    field: String,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (ReplayState::BulkData(BulkDataState::new(field, tx)), rx)
}

fn make_intradaytick_state(
    ticker: String,
) -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (
        ReplayState::IntradayTick(IntradayTickState::new(ticker, tx)),
        rx,
    )
}

fn make_bql_state() -> (
    ReplayState,
    oneshot::Receiver<Result<RecordBatch, BlpError>>,
) {
    let (tx, rx) = oneshot::channel();
    (ReplayState::Bql(BqlState::new(tx)), rx)
}

const BDTICK_TICKER_COLUMN: &str = "ticker";
const BDTICK_CORE_NAMES: [&str; 4] = ["time", "type", "value", "size"];
const BDTICK_CORE_FIELDS: [(&str, ArrowType); 4] = [
    ("time", ArrowType::TimestampMicros),
    ("type", ArrowType::String),
    ("value", ArrowType::Float64),
    ("size", ArrowType::Int64),
];
const BDTICK_UNSAFE_OPTIONAL_FIELDS: [(&str, ArrowType); 2] = [
    ("conditionCodes", ArrowType::String),
    ("exchangeCode", ArrowType::String),
];

#[derive(Clone, Debug)]
struct BdtickReplayDiagnostics {
    rows: usize,
    child_elements: usize,
    unique_layouts: usize,
    layout_counts: Vec<(String, usize)>,
    optional_fields: Vec<String>,
    missing_optional_appends: usize,
    core_prefix_matches: usize,
    core_prefix_mismatches: usize,
}

impl BdtickReplayDiagnostics {
    fn describe(&self) -> String {
        let layouts = self
            .layout_counts
            .iter()
            .take(4)
            .map(|(layout, count)| format!("{count}x[{layout}]"))
            .collect::<Vec<_>>()
            .join("; ");
        let optional_fields = if self.optional_fields.is_empty() {
            "none".to_string()
        } else {
            self.optional_fields.join(",")
        };
        format!(
            "source_rows={}, child_elements={}, unique_layouts={}, layouts={}, optional_fields={}, missing_optional_appends={}, core_prefix_matches={}, core_prefix_mismatches={}",
            self.rows,
            self.child_elements,
            self.unique_layouts,
            layouts,
            optional_fields,
            self.missing_optional_appends,
            self.core_prefix_matches,
            self.core_prefix_mismatches,
        )
    }
}

fn analyze_bdtick_events(events: &[Event]) -> BdtickReplayDiagnostics {
    let mut rows = 0usize;
    let mut child_elements = 0usize;
    let mut core_prefix_matches = 0usize;
    let mut core_prefix_mismatches = 0usize;
    let mut layout_counts = BTreeMap::<String, usize>::new();
    let mut optional_fields = BTreeSet::<String>::new();
    let mut row_field_sets = Vec::<BTreeSet<String>>::new();

    for event in events {
        if !matches!(
            event.event_type(),
            EventType::PartialResponse | EventType::Response
        ) {
            continue;
        }

        for msg in event.messages() {
            for_each_bdtick_row(&msg, |tick| {
                rows += 1;
                let mut names = Vec::with_capacity(tick.num_children());
                let mut row_fields = BTreeSet::new();

                for child in tick.children() {
                    child_elements += 1;
                    let name = child.name().as_str().to_string();
                    if !BDTICK_CORE_NAMES.contains(&name.as_str()) {
                        optional_fields.insert(name.clone());
                    }
                    row_fields.insert(name.clone());
                    names.push(name);
                }

                if names.len() >= BDTICK_CORE_NAMES.len()
                    && BDTICK_CORE_NAMES
                        .iter()
                        .enumerate()
                        .all(|(idx, expected)| names[idx].as_str() == *expected)
                {
                    core_prefix_matches += 1;
                } else {
                    core_prefix_mismatches += 1;
                }

                *layout_counts.entry(names.join(",")).or_default() += 1;
                row_field_sets.push(row_fields);
            });
        }
    }

    let optional_fields: Vec<_> = optional_fields.into_iter().collect();
    let missing_optional_appends = row_field_sets
        .iter()
        .map(|fields| {
            optional_fields
                .iter()
                .filter(|field| !fields.contains(field.as_str()))
                .count()
        })
        .sum();
    let layout_counts: Vec<_> = layout_counts.into_iter().collect();

    BdtickReplayDiagnostics {
        rows,
        child_elements,
        unique_layouts: layout_counts.len(),
        layout_counts,
        optional_fields,
        missing_optional_appends,
        core_prefix_matches,
        core_prefix_mismatches,
    }
}

fn for_each_bdtick_row<F>(msg: &Message, mut visit: F)
where
    F: FnMut(Element<'_>),
{
    let root = msg.elements();
    let Some(tick_data_outer) = root.get_by_str("tickData") else {
        return;
    };
    let Some(tick_data) = tick_data_outer.get_by_str("tickData") else {
        return;
    };

    for i in 0..tick_data.len() {
        if let Some(tick) = tick_data.get_element(i) {
            visit(tick);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BdtickBenchVariant {
    CorePrefix,
    LayoutCache,
    UnsafeObservedLayout,
}

impl BdtickBenchVariant {
    fn all() -> [Self; 3] {
        [
            Self::CorePrefix,
            Self::LayoutCache,
            Self::UnsafeObservedLayout,
        ]
    }

    fn scenario(self) -> &'static str {
        match self {
            Self::CorePrefix => "bdtick_optional_fields_core_prefix",
            Self::LayoutCache => "bdtick_optional_fields_layout_cache",
            Self::UnsafeObservedLayout => "bdtick_optional_fields_unsafe_observed_layout",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::CorePrefix => "core_prefix",
            Self::LayoutCache => "layout_cache",
            Self::UnsafeObservedLayout => "unsafe_observed_layout",
        }
    }
}

#[derive(Default)]
struct BdtickReplayPhaseTotals {
    create_state: Duration,
    partial_process: Duration,
    response_process_or_finish: Duration,
    finish_batch: Duration,
    receive_batch: Duration,
}

impl BdtickReplayPhaseTotals {
    fn as_metrics(&self, total: Duration) -> Vec<PhaseMetric> {
        vec![
            phase("create_state", self.create_state),
            phase("partial_process", self.partial_process),
            phase(
                "response_process_or_finish",
                self.response_process_or_finish,
            ),
            phase("finish_batch", self.finish_batch),
            phase("receive_batch", self.receive_batch),
            phase("total", total),
        ]
    }
}

fn replay_bdtick_prod_events(
    events: &[Event],
    iterations: usize,
    diagnostics: &BdtickReplayDiagnostics,
) -> BenchRecord {
    let start = Instant::now();
    let mut phases = BdtickReplayPhaseTotals::default();
    let mut rows = 0usize;
    let mut columns = 0usize;
    let mut ok_iterations = 0usize;
    let mut last_error: Option<String> = None;

    for _ in 0..iterations {
        let create_start = Instant::now();
        let (mut state, rx) = make_intradaytick_state("IBM US Equity".to_string());
        phases.create_state += create_start.elapsed();

        let mut finished = false;
        'events: for event in events {
            match event.event_type() {
                EventType::PartialResponse => {
                    let phase_start = Instant::now();
                    for msg in event.messages() {
                        state.on_partial(&msg);
                    }
                    phases.partial_process += phase_start.elapsed();
                }
                EventType::Response => {
                    let phase_start = Instant::now();
                    if let Some(msg) = event.messages().next() {
                        state.finish(&msg);
                        finished = true;
                    }
                    phases.response_process_or_finish += phase_start.elapsed();
                    break 'events;
                }
                _ => {}
            }
        }

        if !finished {
            last_error = Some("no response message in cached BDTICK events".to_string());
            continue;
        }

        let receive_start = Instant::now();
        match rx.blocking_recv() {
            Ok(Ok(batch)) => {
                phases.receive_batch += receive_start.elapsed();
                rows += batch.num_rows();
                columns = batch.num_columns();
                ok_iterations += 1;
                black_box(batch);
            }
            Ok(Err(err)) => {
                phases.receive_batch += receive_start.elapsed();
                last_error = Some(err.to_string());
            }
            Err(err) => {
                phases.receive_batch += receive_start.elapsed();
                last_error = Some(err.to_string());
            }
        }
    }

    let elapsed = start.elapsed();
    if ok_iterations == 0 {
        return BenchRecord::error(
            "replay",
            "bdtick_optional_fields_prod",
            elapsed,
            last_error.unwrap_or_else(|| "no successful iterations".to_string()),
        );
    }

    let mut record = BenchRecord::ok(
        "replay",
        "bdtick_optional_fields_prod",
        elapsed,
        rows,
        columns,
        rows,
        "rows",
        format!(
            "iterations={ok_iterations}, cached_events={}, variant=prod, {}",
            events.len(),
            diagnostics.describe(),
        ),
    );
    record.phases = phases.as_metrics(elapsed);
    record
}

struct BenchTickField {
    output_name: String,
    lookup_name: Name,
    builder: TypedBuilder,
}

impl BenchTickField {
    fn new(name: &str, arrow_type: ArrowType) -> Self {
        Self {
            output_name: name.to_string(),
            lookup_name: Name::get_or_intern(name),
            builder: TypedBuilder::new(arrow_type),
        }
    }
}

struct BenchTickLayout {
    names: Vec<Name>,
    field_indexes: Vec<Option<usize>>,
}

struct BenchBdtickState {
    ticker: String,
    mode: BdtickBenchVariant,
    column_name_set: HashSet<String>,
    ticker_builder: TypedBuilder,
    tick_fields: Vec<BenchTickField>,
    lookup_names: Vec<Name>,
    seen_fields: Vec<bool>,
    row_count: usize,
    layout_cache: Vec<BenchTickLayout>,
    layout_hits: usize,
    layout_misses: usize,
    core_prefix_hits: usize,
    core_prefix_fallbacks: usize,
    unsafe_rows: usize,
    unsafe_unhandled_layouts: usize,
}

impl BenchBdtickState {
    fn new(ticker: String, mode: BdtickBenchVariant) -> Self {
        let mut state = Self {
            ticker,
            mode,
            column_name_set: std::iter::once(BDTICK_TICKER_COLUMN.to_string()).collect(),
            ticker_builder: TypedBuilder::new(ArrowType::String),
            tick_fields: Vec::new(),
            lookup_names: Vec::new(),
            seen_fields: Vec::new(),
            row_count: 0,
            layout_cache: Vec::new(),
            layout_hits: 0,
            layout_misses: 0,
            core_prefix_hits: 0,
            core_prefix_fallbacks: 0,
            unsafe_rows: 0,
            unsafe_unhandled_layouts: 0,
        };

        for (name, arrow_type) in BDTICK_CORE_FIELDS {
            state.add_static_field(name, arrow_type);
        }
        if mode == BdtickBenchVariant::UnsafeObservedLayout {
            for (name, arrow_type) in BDTICK_UNSAFE_OPTIONAL_FIELDS {
                state.add_static_field(name, arrow_type);
            }
        }
        state
    }

    fn add_static_field(&mut self, name: &str, arrow_type: ArrowType) {
        if self.column_name_set.insert(name.to_string()) {
            let field = BenchTickField::new(name, arrow_type);
            self.lookup_names.push(field.lookup_name.clone());
            self.tick_fields.push(field);
            self.seen_fields.push(false);
        }
    }

    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();
        let Some(tick_data_outer) = root.get_by_str("tickData") else {
            return;
        };
        let Some(tick_data) = tick_data_outer.get_by_str("tickData") else {
            return;
        };

        for i in 0..tick_data.len() {
            let Some(tick) = tick_data.get_element(i) else {
                continue;
            };
            self.append_tick(&tick);
        }
    }

    fn append_tick(&mut self, tick: &Element<'_>) {
        self.ticker_builder.append_str(&self.ticker);
        self.seen_fields.clear();
        self.seen_fields.resize(self.tick_fields.len(), false);

        match self.mode {
            BdtickBenchVariant::CorePrefix => self.append_tick_core_prefix(tick),
            BdtickBenchVariant::LayoutCache => self.append_tick_layout_cache(tick),
            BdtickBenchVariant::UnsafeObservedLayout => self.append_tick_unsafe_observed(tick),
        }

        for (idx, field) in self.tick_fields.iter_mut().enumerate() {
            if !self.seen_fields[idx] {
                field.builder.append_null();
            }
        }
        self.row_count += 1;
    }

    fn append_tick_core_prefix(&mut self, tick: &Element<'_>) {
        if self.try_append_core_prefix(tick) {
            self.core_prefix_hits += 1;
            self.append_tick_child_scan_from(tick, BDTICK_CORE_FIELDS.len());
        } else {
            self.core_prefix_fallbacks += 1;
            self.append_tick_child_scan_from(tick, 0);
        }
    }

    fn try_append_core_prefix(&mut self, tick: &Element<'_>) -> bool {
        if tick.num_children() < BDTICK_CORE_FIELDS.len() {
            return false;
        }

        let time = unsafe { tick.get_at_unchecked(0) };
        let event_type = unsafe { tick.get_at_unchecked(1) };
        let value = unsafe { tick.get_at_unchecked(2) };
        let size = unsafe { tick.get_at_unchecked(3) };
        if !time.name_eq(&self.lookup_names[0])
            || !event_type.name_eq(&self.lookup_names[1])
            || !value.name_eq(&self.lookup_names[2])
            || !size.name_eq(&self.lookup_names[3])
        {
            return false;
        }

        self.append_child_at_field_index(0, &time);
        self.append_child_at_field_index(1, &event_type);
        self.append_child_at_field_index(2, &value);
        self.append_child_at_field_index(3, &size);
        true
    }

    fn append_tick_layout_cache(&mut self, tick: &Element<'_>) {
        if let Some(layout_index) = self.find_matching_layout(tick) {
            self.layout_hits += 1;
            self.append_cached_layout(tick, layout_index);
        } else {
            self.layout_misses += 1;
            self.append_and_cache_layout(tick);
        }
    }

    fn find_matching_layout(&self, tick: &Element<'_>) -> Option<usize> {
        self.layout_cache
            .iter()
            .position(|layout| self.layout_matches(tick, layout))
    }

    fn layout_matches(&self, tick: &Element<'_>, layout: &BenchTickLayout) -> bool {
        if tick.num_children() != layout.names.len() {
            return false;
        }
        for (idx, expected_name) in layout.names.iter().enumerate() {
            let child = unsafe { tick.get_at_unchecked(idx) };
            if !child.name_eq(expected_name) {
                return false;
            }
        }
        true
    }

    fn append_cached_layout(&mut self, tick: &Element<'_>, layout_index: usize) {
        let len = self.layout_cache[layout_index].field_indexes.len();
        for child_pos in 0..len {
            let Some(field_index) = self.layout_cache[layout_index].field_indexes[child_pos] else {
                continue;
            };
            let child = unsafe { tick.get_at_unchecked(child_pos) };
            self.append_child_at_field_index(field_index, &child);
        }
    }

    fn append_and_cache_layout(&mut self, tick: &Element<'_>) {
        let mut names = Vec::with_capacity(tick.num_children());
        let mut field_indexes = Vec::with_capacity(tick.num_children());
        for child in tick.children() {
            names.push(child.name());
            field_indexes.push(self.append_child_by_name(&child));
        }
        self.layout_cache.push(BenchTickLayout {
            names,
            field_indexes,
        });
    }

    fn append_tick_unsafe_observed(&mut self, tick: &Element<'_>) {
        if tick.num_children() < BDTICK_CORE_FIELDS.len() {
            self.unsafe_unhandled_layouts += 1;
            self.append_tick_child_scan_from(tick, 0);
            return;
        }

        self.unsafe_rows += 1;
        let time = unsafe { tick.get_at_unchecked(0) };
        let event_type = unsafe { tick.get_at_unchecked(1) };
        let value = unsafe { tick.get_at_unchecked(2) };
        let size = unsafe { tick.get_at_unchecked(3) };
        self.append_child_at_field_index(0, &time);
        self.append_child_at_field_index(1, &event_type);
        self.append_child_at_field_index(2, &value);
        self.append_child_at_field_index(3, &size);

        match tick.num_children() {
            5 => {
                let exchange = unsafe { tick.get_at_unchecked(4) };
                self.append_child_at_field_index(5, &exchange);
            }
            n if n >= 6 => {
                let condition = unsafe { tick.get_at_unchecked(4) };
                let exchange = unsafe { tick.get_at_unchecked(5) };
                self.append_child_at_field_index(4, &condition);
                self.append_child_at_field_index(5, &exchange);
                if n > 6 {
                    self.unsafe_unhandled_layouts += 1;
                }
            }
            _ => {}
        }
    }

    fn append_tick_child_scan_from(&mut self, tick: &Element<'_>, start_index: usize) {
        for child_pos in start_index..tick.num_children() {
            let child = unsafe { tick.get_at_unchecked(child_pos) };
            self.append_child_by_name(&child);
        }
    }

    fn append_child_by_name(&mut self, child: &Element<'_>) -> Option<usize> {
        let field_index = match self.find_tick_field(child) {
            Some(idx) => idx,
            None => {
                if !should_emit_bdtick_scalar_field(child) {
                    return None;
                }
                self.discover_tick_field(child)?
            }
        };
        self.append_child_at_field_index(field_index, child);
        Some(field_index)
    }

    fn append_child_at_field_index(&mut self, field_index: usize, child: &Element<'_>) {
        if field_index >= self.seen_fields.len() {
            self.seen_fields.resize(self.tick_fields.len(), false);
        }
        if self.seen_fields[field_index] {
            return;
        }
        self.seen_fields[field_index] = true;
        let field = &mut self.tick_fields[field_index];
        append_bdtick_child_value(&mut field.builder, child);
    }

    fn find_tick_field(&self, child: &Element<'_>) -> Option<usize> {
        child.name_index(&self.lookup_names)
    }

    fn discover_tick_field(&mut self, child: &Element<'_>) -> Option<usize> {
        let lookup_name = child.name();
        let name = lookup_name.as_str();
        if name == BDTICK_TICKER_COLUMN || self.column_name_set.contains(name) {
            return None;
        }

        let output_name = name.to_string();
        self.column_name_set.insert(output_name.clone());

        let mut builder = TypedBuilder::new(arrow_type_for_bdtick_element(child));
        for _ in 0..self.row_count {
            builder.append_null();
        }

        self.tick_fields.push(BenchTickField {
            output_name,
            lookup_name: lookup_name.clone(),
            builder,
        });
        self.lookup_names.push(lookup_name);
        Some(self.tick_fields.len() - 1)
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let mut fields = Vec::with_capacity(1 + self.tick_fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + self.tick_fields.len());

        fields.push(Field::new(
            BDTICK_TICKER_COLUMN,
            self.ticker_builder.data_type(),
            true,
        ));
        arrays.push(self.ticker_builder.finish());

        for field in &mut self.tick_fields {
            fields.push(Field::new(
                field.output_name.as_str(),
                field.builder.data_type(),
                true,
            ));
            arrays.push(field.builder.finish());
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| {
            BlpError::Internal {
                detail: format!("build benchmark BDTICK RecordBatch: {e}"),
            }
        })
    }

    fn describe_variant_stats(&self) -> String {
        format!(
            "variant={}, core_prefix_hits={}, core_prefix_fallbacks={}, layout_hits={}, layout_misses={}, layout_cache_size={}, unsafe_rows={}, unsafe_unhandled_layouts={}",
            self.mode.label(),
            self.core_prefix_hits,
            self.core_prefix_fallbacks,
            self.layout_hits,
            self.layout_misses,
            self.layout_cache.len(),
            self.unsafe_rows,
            self.unsafe_unhandled_layouts,
        )
    }
}

fn replay_bdtick_variant_events(
    events: &[Event],
    iterations: usize,
    variant: BdtickBenchVariant,
    diagnostics: &BdtickReplayDiagnostics,
) -> BenchRecord {
    let start = Instant::now();
    let mut phases = BdtickReplayPhaseTotals::default();
    let mut rows = 0usize;
    let mut columns = 0usize;
    let mut ok_iterations = 0usize;
    let mut last_error: Option<String> = None;
    let mut last_variant_stats = String::new();

    for _ in 0..iterations {
        let create_start = Instant::now();
        let mut state = BenchBdtickState::new("IBM US Equity".to_string(), variant);
        phases.create_state += create_start.elapsed();

        let mut finished = false;
        'events: for event in events {
            match event.event_type() {
                EventType::PartialResponse => {
                    let phase_start = Instant::now();
                    for msg in event.messages() {
                        state.process_message(&msg);
                    }
                    phases.partial_process += phase_start.elapsed();
                }
                EventType::Response => {
                    let phase_start = Instant::now();
                    for msg in event.messages() {
                        state.process_message(&msg);
                        finished = true;
                    }
                    phases.response_process_or_finish += phase_start.elapsed();
                    break 'events;
                }
                _ => {}
            }
        }

        if !finished {
            last_error = Some("no response message in cached BDTICK events".to_string());
            continue;
        }

        let finish_start = Instant::now();
        match state.finish_batch() {
            Ok(batch) => {
                phases.finish_batch += finish_start.elapsed();
                rows += batch.num_rows();
                columns = batch.num_columns();
                ok_iterations += 1;
                last_variant_stats = state.describe_variant_stats();
                black_box(batch);
            }
            Err(err) => {
                phases.finish_batch += finish_start.elapsed();
                last_error = Some(err.to_string());
            }
        }
    }

    let elapsed = start.elapsed();
    if ok_iterations == 0 {
        return BenchRecord::error(
            "replay",
            variant.scenario(),
            elapsed,
            last_error.unwrap_or_else(|| "no successful iterations".to_string()),
        );
    }

    let mut record = BenchRecord::ok(
        "replay",
        variant.scenario(),
        elapsed,
        rows,
        columns,
        rows,
        "rows",
        format!(
            "iterations={ok_iterations}, cached_events={}, {}, {}",
            events.len(),
            last_variant_stats,
            diagnostics.describe(),
        ),
    );
    record.phases = phases.as_metrics(elapsed);
    record
}

fn should_emit_bdtick_scalar_field(element: &Element<'_>) -> bool {
    !element.is_array()
        && !matches!(
            element.datatype(),
            BlpDataType::Sequence
                | BlpDataType::Choice
                | BlpDataType::ByteArray
                | BlpDataType::CorrelationId,
        )
}

fn arrow_type_for_bdtick_element(element: &Element<'_>) -> ArrowType {
    match element.datatype() {
        BlpDataType::Bool => ArrowType::Bool,
        BlpDataType::Char | BlpDataType::Byte | BlpDataType::Int32 => ArrowType::Int32,
        BlpDataType::Int64 => ArrowType::Int64,
        BlpDataType::Float32 | BlpDataType::Float64 | BlpDataType::Decimal => ArrowType::Float64,
        BlpDataType::String | BlpDataType::Enumeration => ArrowType::String,
        BlpDataType::Date => ArrowType::Date32,
        BlpDataType::Time => ArrowType::Time64Micros,
        BlpDataType::Datetime => ArrowType::TimestampMicros,
        BlpDataType::Sequence
        | BlpDataType::Choice
        | BlpDataType::ByteArray
        | BlpDataType::CorrelationId => ArrowType::String,
    }
}

fn append_bdtick_child_value(builder: &mut TypedBuilder, child: &Element<'_>) {
    match builder {
        TypedBuilder::Float64(builder) => {
            if let Some(value) = child.get_f64(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::Int64(builder) => {
            if let Some(value) = child.get_i64(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::Int32(builder) => {
            if let Some(value) = child.get_i32(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::String(builder) => {
            if let Some(value) = child.get_str(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::Bool(builder) => {
            if let Some(value) = child.get_bool(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::Date32(builder) => {
            if let Some(value) = child.get_date32(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::TimestampMicros(builder) => {
            if let Some(value) = child.get_timestamp_us(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        TypedBuilder::Time64Micros(builder) => {
            if let Some(value) = child.get_datetime(0).map(|dt| dt.to_time_micros()) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
    }
}

fn replay_request_events<F>(
    scenario: &'static str,
    events: &[Event],
    iterations: usize,
    mut make_state: F,
) -> BenchRecord
where
    F: FnMut() -> (
        ReplayState,
        oneshot::Receiver<Result<RecordBatch, BlpError>>,
    ),
{
    let start = Instant::now();
    let mut phases = BdtickReplayPhaseTotals::default();
    let mut rows = 0usize;
    let mut columns = 0usize;
    let mut ok_iterations = 0usize;
    let mut last_error: Option<String> = None;

    for _ in 0..iterations {
        let create_start = Instant::now();
        let (mut state, rx) = make_state();
        phases.create_state += create_start.elapsed();

        let mut finished = false;
        'events: for event in events {
            match event.event_type() {
                EventType::PartialResponse => {
                    let phase_start = Instant::now();
                    for msg in event.messages() {
                        state.on_partial(&msg);
                    }
                    phases.partial_process += phase_start.elapsed();
                }
                EventType::Response => {
                    let phase_start = Instant::now();
                    if let Some(msg) = event.messages().next() {
                        state.finish(&msg);
                        finished = true;
                    }
                    phases.response_process_or_finish += phase_start.elapsed();
                    break 'events;
                }
                _ => {}
            }
        }

        if !finished {
            last_error = Some("no response message in cached events".to_string());
            continue;
        }

        let receive_start = Instant::now();
        match rx.blocking_recv() {
            Ok(Ok(batch)) => {
                phases.receive_batch += receive_start.elapsed();
                rows += batch.num_rows();
                columns = batch.num_columns();
                ok_iterations += 1;
                black_box(batch);
            }
            Ok(Err(err)) => {
                phases.receive_batch += receive_start.elapsed();
                last_error = Some(err.to_string());
            }
            Err(err) => {
                phases.receive_batch += receive_start.elapsed();
                last_error = Some(err.to_string());
            }
        }
    }

    let elapsed = start.elapsed();
    if ok_iterations == 0 {
        return BenchRecord::error(
            "replay",
            scenario,
            elapsed,
            last_error.unwrap_or_else(|| "all replay iterations failed".to_string()),
        );
    }

    let mut record = BenchRecord::ok(
        "replay",
        scenario,
        elapsed,
        rows,
        columns,
        rows,
        "rows",
        format!("iterations={ok_iterations}, cached_events={}", events.len()),
    );
    record.phases = phases.as_metrics(elapsed);
    record
}

fn collect_response_events<F>(
    sess: &xbbg_core::Session,
    service: &str,
    operation: &str,
    build: F,
) -> Result<Vec<Event>, String>
where
    F: FnOnce(&mut xbbg_core::Request) -> Result<(), BlpError>,
{
    let svc = sess
        .get_service(service)
        .map_err(|err| format!("get_service {service}: {err}"))?;
    let mut req = svc
        .create_request(operation)
        .map_err(|err| format!("create_request {operation}: {err}"))?;
    build(&mut req).map_err(|err| format!("build {operation}: {err}"))?;
    sess.send_request(&req, None, None)
        .map_err(|err| format!("send_request {operation}: {err}"))?;

    let mut events = Vec::new();
    loop {
        let event = sess
            .next_event(Some(10_000))
            .map_err(|err| format!("next_event {operation}: {err}"))?;
        match event.event_type() {
            EventType::PartialResponse => events.push(event),
            EventType::Response => {
                events.push(event);
                return Ok(events);
            }
            EventType::RequestStatus => {
                return Err(format!("request status before response for {operation}"));
            }
            _ => {}
        }
    }
}

fn fetch_bdp_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/refdata", "ReferenceDataRequest", |req| {
        req.append_str("securities", "IBM US Equity")?;
        req.append_str("fields", "PX_LAST")?;
        req.append_str("fields", "VOLUME")?;
        Ok(())
    })
}

fn fetch_bdh_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/refdata", "HistoricalDataRequest", |req| {
        req.append_str("securities", "IBM US Equity")?;
        req.append_str("fields", "PX_LAST")?;
        req.append_str("fields", "VOLUME")?;
        req.set_str("startDate", "20241202")?;
        req.set_str("endDate", "20241206")?;
        Ok(())
    })
}

fn fetch_bdp_wide_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/refdata", "ReferenceDataRequest", |req| {
        for security in WIDE_BDP_SECURITIES {
            req.append_str("securities", security)?;
        }
        for field in WIDE_BDP_FIELDS {
            req.append_str("fields", field)?;
        }
        Ok(())
    })
}

fn fetch_bdh_wide_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/refdata", "HistoricalDataRequest", |req| {
        for security in WIDE_BDH_SECURITIES {
            req.append_str("securities", security)?;
        }
        for field in WIDE_BDH_FIELDS {
            req.append_str("fields", field)?;
        }
        req.set_str("startDate", "20240401")?;
        req.set_str("endDate", "20240430")?;
        Ok(())
    })
}

fn fetch_bds_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/refdata", "ReferenceDataRequest", |req| {
        req.append_str("securities", "INDU Index")?;
        req.append_str("fields", "INDX_MEMBERS")?;
        Ok(())
    })
}

fn fetch_bdtick_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    let date = previous_weekday().format("%Y-%m-%d").to_string();
    collect_response_events(sess, "//blp/refdata", "IntradayTickRequest", |req| {
        req.set_str("security", "IBM US Equity")?;
        req.append_str("eventTypes", "TRADE")?;
        req.set_datetime("startDateTime", &format!("{date}T14:30:00"))?;
        req.set_datetime("endDateTime", &format!("{date}T14:31:00"))?;
        req.set_bool(&Name::get_or_intern("includeConditionCodes"), true)?;
        req.set_bool(&Name::get_or_intern("includeExchangeCodes"), true)?;
        Ok(())
    })
}

fn fetch_bql_events(sess: &xbbg_core::Session) -> Result<Vec<Event>, String> {
    collect_response_events(sess, "//blp/bqlsvc", "sendQuery", |req| {
        req.set_str("expression", "get(px_last) for(['IBM US Equity'])")?;
        Ok(())
    })
}

fn fetch_subscription_events(
    sess: &xbbg_core::Session,
    collect_ms: u64,
) -> Result<Vec<Event>, String> {
    let mut sub_list = SubscriptionList::new();
    let cid = CorrelationId::new_int(10_001);
    sub_list
        .add("IBM US Equity", &["LAST_PRICE", "BID", "ASK"], "", &cid)
        .map_err(|err| format!("add subscription: {err}"))?;
    sess.subscribe(&sub_list, None)
        .map_err(|err| format!("subscribe: {err}"))?;

    let deadline = Instant::now() + Duration::from_millis(collect_ms);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = remaining.as_millis().clamp(1, u32::MAX as u128) as u32;
        if let Ok(event) = sess.next_event(Some(timeout)) {
            match event.event_type() {
                EventType::SubscriptionData => events.push(event),
                EventType::SubscriptionStatus => {}
                _ => {}
            }
        }
    }
    let _ = sess.unsubscribe(&sub_list);

    if events.is_empty() {
        Err("subscription capture produced no data events".to_string())
    } else {
        Ok(events)
    }
}

fn subscription_cached_message_count(events: &[Event]) -> usize {
    events
        .iter()
        .map(|event| event.messages().count())
        .sum::<usize>()
        .max(1)
}

fn subscription_repeats_per_message(events: &[Event], target_messages: usize) -> usize {
    (target_messages / subscription_cached_message_count(events)).max(1)
}

fn for_each_subscription_component_message<F>(
    events: &[Event],
    target_messages: usize,
    mut f: F,
) -> usize
where
    F: for<'a> FnMut(&xbbg_core::Message<'a>),
{
    let repeats_per_message = subscription_repeats_per_message(events, target_messages);
    let mut processed = 0usize;
    while processed < target_messages {
        for event in events {
            for msg in event.messages() {
                for _ in 0..repeats_per_message {
                    f(&msg);
                    processed += 1;
                    if processed >= target_messages {
                        break;
                    }
                }
            }
            if processed >= target_messages {
                break;
            }
        }
    }
    processed
}

fn component_should_capture_datatype(datatype: BlpDataType) -> bool {
    !matches!(
        datatype,
        BlpDataType::Sequence
            | BlpDataType::Choice
            | BlpDataType::ByteArray
            | BlpDataType::CorrelationId
    )
}

fn component_should_capture_field(element: &Element<'_>) -> bool {
    !element.is_array() && component_should_capture_datatype(element.datatype())
}

fn estimate_all_field_count(events: &[Event]) -> usize {
    events
        .iter()
        .flat_map(|event| event.messages())
        .find_map(|msg| {
            let elem = msg.elements();
            let mut count = 0usize;
            for child_idx in 0..elem.num_children() {
                let Some(child) = elem.get_at(child_idx) else {
                    continue;
                };
                if component_should_capture_field(&child) {
                    count += 1;
                }
            }
            (count > 0).then_some(count)
        })
        .unwrap_or(1)
}

fn component_layout(field_count: usize) -> Arc<FieldLayout> {
    Arc::new(FieldLayout::new(
        1,
        (0..field_count)
            .map(|idx| FieldMeta::new(format!("field_{idx}"), idx as _, FieldKind::F64))
            .collect(),
    ))
}

fn component_schema(field_count: usize) -> Arc<Schema> {
    component_record_batch(1, field_count).schema()
}

fn component_record_batch(rows: usize, field_count: usize) -> RecordBatch {
    component_record_batch_with_values(rows, field_count, true)
}

fn component_null_record_batch(rows: usize, field_count: usize) -> RecordBatch {
    component_record_batch_with_values(rows, field_count, false)
}

fn component_record_batch_with_values(
    rows: usize,
    field_count: usize,
    include_values: bool,
) -> RecordBatch {
    let layout = component_layout(field_count);
    let mut update = SubscriptionUpdate {
        timestamp_us: 0,
        topic_id: 0,
        topic: Arc::from("SYN00000 US Equity"),
        layout,
        values: Default::default(),
    };
    if include_values {
        update
            .values
            .extend((0..field_count).map(|field_idx| UpdateField {
                index: field_idx as _,
                value: UpdateValue::F64(field_idx as f64),
            }));
    }
    let mut batcher = SubscriptionArrowBatcher::with_capacity(rows);
    for row in 0..rows {
        update.timestamp_us = row as i64;
        for field in &mut update.values {
            field.value = UpdateValue::F64((row + field.index as usize) as f64);
        }
        assert!(batcher.append(&update).is_none());
    }
    batcher
        .flush()
        .expect("component profile batch must contain at least one row")
}

fn run_bql_json_suite(config: &SuiteConfig) -> Vec<BenchRecord> {
    let base_iterations = config.profile.bql_json_iterations();
    let cases: [(&'static str, usize, &'static [&'static str]); 3] = [
        ("json_simple_1x1", 1, &["px_last"]),
        (
            "json_wide_1x5",
            1,
            &["px_last", "px_open", "px_high", "px_low", "px_volume"],
        ),
        ("json_rows_1000x2", 1_000, &["px_last", "px_volume"]),
    ];
    let mut records = Vec::new();
    for (scenario, rows, fields) in cases {
        if !config.should_run("bql_json", scenario) {
            continue;
        }
        let iterations = if rows > 1 {
            (base_iterations / 20).max(10)
        } else {
            base_iterations
        };
        let json = bql_json_fixture(rows, fields);
        records.push(profile_record(config, "bql_json", scenario, move |_| {
            replay_bql_json_fixture(scenario, &json, rows, fields.len(), iterations)
        }));
    }
    records
}

fn bql_json_fixture(rows: usize, fields: &[&str]) -> String {
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

fn replay_bql_json_fixture(
    scenario: &'static str,
    json: &str,
    rows_per_iteration: usize,
    field_count: usize,
    iterations: usize,
) -> BenchRecord {
    let (tx, _rx) = oneshot::channel();
    let state = BqlState::new(tx);
    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut columns = 0usize;

    for _ in 0..iterations {
        match state.parse_bql_json_for_bench(json) {
            Ok(batch) => {
                total_rows += batch.num_rows();
                columns = batch.num_columns();
                black_box(batch);
            }
            Err(err) => {
                return BenchRecord::error("bql_json", scenario, start.elapsed(), err.to_string());
            }
        }
    }

    let elapsed = start.elapsed();
    let values = total_rows.saturating_mul(columns);
    let mut record = BenchRecord::ok(
        "bql_json",
        scenario,
        elapsed,
        total_rows,
        columns,
        values,
        "cells",
        format!(
            "iterations={iterations}, rows_per_iteration={rows_per_iteration}, fields={field_count}, fixture_bytes={}",
            json.len()
        ),
    );
    record.phases = vec![
        phase("parse_bql_json_to_arrow", elapsed),
        phase("total", Duration::from_micros(record.elapsed_us as u64)),
    ];
    record
}

fn profile_subscription_components(
    scenario: &'static str,
    events: &[Event],
    target_messages: usize,
    all_fields: bool,
    fields: &[&str],
) -> BenchRecord {
    let start = Instant::now();
    if events.is_empty() {
        return BenchRecord::error(
            "subscription_components",
            scenario,
            Duration::ZERO,
            "no cached subscription events",
        );
    }

    let field_count = if all_fields {
        estimate_all_field_count(events)
    } else {
        fields.len()
    };
    let names = fields
        .iter()
        .map(|field| Name::get_or_intern(field))
        .collect::<Vec<_>>();

    let repeats_per_message = subscription_repeats_per_message(events, target_messages);
    let message_iteration_start = Instant::now();
    let mut iterated = 0usize;
    while iterated < target_messages {
        for event in events {
            for _msg in event.messages() {
                for _ in 0..repeats_per_message {
                    iterated += 1;
                    if iterated >= target_messages {
                        break;
                    }
                }
            }
            if iterated >= target_messages {
                break;
            }
        }
    }
    let message_iteration_elapsed = message_iteration_start.elapsed();

    let msg_elements_start = Instant::now();
    for_each_subscription_component_message(events, target_messages, |msg| {
        black_box(msg.elements());
    });
    let msg_elements_elapsed = msg_elements_start.elapsed();

    let timestamp_topic_start = Instant::now();
    let mut timestamp_builder = TimestampMicrosecondBuilder::new();
    let mut topic_builder = StringBuilder::new();
    for_each_subscription_component_message(events, target_messages, |msg| {
        timestamp_builder.append_value(msg.time_received_us().unwrap_or_default());
        topic_builder.append_value("SYN00000 US Equity");
    });
    black_box(timestamp_builder.finish());
    black_box(topic_builder.finish());
    let timestamp_topic_elapsed = timestamp_topic_start.elapsed();

    let requested_lookup_start = Instant::now();
    if !all_fields {
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for name in &names {
                black_box(elem.get(name));
            }
        });
    }
    let requested_lookup_elapsed = requested_lookup_start.elapsed();

    let all_fields_get_at_start = Instant::now();
    if all_fields {
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for child_idx in 0..elem.num_children() {
                black_box(elem.get_at(child_idx));
            }
        });
    }
    let all_fields_get_at_elapsed = all_fields_get_at_start.elapsed();

    let all_fields_datatype_start = Instant::now();
    if all_fields {
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for child_idx in 0..elem.num_children() {
                let Some(child) = elem.get_at(child_idx) else {
                    continue;
                };
                black_box(component_should_capture_field(&child));
            }
        });
    }
    let all_fields_datatype_elapsed = all_fields_datatype_start.elapsed();

    let all_fields_name_cache_start = Instant::now();
    if all_fields {
        let requested_name_keys = names
            .iter()
            .map(|name| name.as_ptr() as usize)
            .collect::<HashSet<_>>();
        let mut slots: Vec<Option<(usize, BlpDataType, bool)>> = Vec::new();
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for child_idx in 0..elem.num_children() {
                let Some(child) = elem.get_at(child_idx) else {
                    continue;
                };
                let key = child.name_key();
                if child.is_array() {
                    black_box(requested_name_keys.contains(&key));
                    continue;
                }
                let datatype = child.datatype();
                if let Some(Some((cached_key, cached_datatype, captured))) =
                    slots.get(child_idx).copied()
                {
                    if cached_key == key && cached_datatype == datatype {
                        black_box(captured);
                        continue;
                    }
                }
                let captured = component_should_capture_datatype(datatype);
                if !captured {
                    let requested = requested_name_keys.contains(&key);
                    black_box(requested);
                    if requested {
                        continue;
                    }
                }
                if child_idx >= slots.len() {
                    slots.resize(child_idx + 1, None);
                }
                slots[child_idx] = Some((key, datatype, captured));
                black_box(captured);
            }
        });
    }
    let all_fields_name_cache_elapsed = all_fields_name_cache_start.elapsed();

    let value_getter_start = Instant::now();
    if all_fields {
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for child_idx in 0..elem.num_children() {
                let Some(child) = elem.get_at(child_idx) else {
                    continue;
                };
                if child.is_array() {
                    continue;
                }
                let datatype = child.datatype();
                if !component_should_capture_datatype(datatype) {
                    continue;
                }
                black_box(child.get_value_fast_with_datatype(0, datatype));
            }
        });
    } else {
        for_each_subscription_component_message(events, target_messages, |msg| {
            let elem = msg.elements();
            for name in &names {
                if let Some(field) = elem.get(name) {
                    let datatype = field.datatype();
                    if field.is_array() || !component_should_capture_datatype(datatype) {
                        black_box(false);
                    } else {
                        black_box(field.get_value_fast_with_datatype(0, datatype));
                    }
                }
            }
        });
    }
    let value_getter_elapsed = value_getter_start.elapsed();

    let arrow_append_start = Instant::now();
    black_box(component_record_batch(target_messages, field_count));
    let arrow_append_elapsed = arrow_append_start.elapsed();

    let null_padding_start = Instant::now();
    black_box(component_null_record_batch(target_messages, field_count));
    let null_padding_elapsed = null_padding_start.elapsed();

    let flush_schema_start = Instant::now();
    black_box(component_schema(field_count));
    let flush_schema_elapsed = flush_schema_start.elapsed();

    let flush_rows = target_messages.min(100_000);
    let flush_start = Instant::now();
    black_box(component_record_batch(flush_rows, field_count));
    let flush_arrays_elapsed = flush_start.elapsed();

    let channel_start = Instant::now();
    let send_count = target_messages.min(1_000);
    let batch = component_record_batch(1, field_count.clamp(1, 8));
    let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, BlpError>>(send_count + 1);
    for _ in 0..send_count {
        tx.try_send(Ok(batch.clone()))
            .expect("component channel send");
    }
    while let Ok(item) = rx.try_recv() {
        let _ = black_box(item);
    }
    let channel_elapsed = channel_start.elapsed();

    let mut record = BenchRecord::ok(
        "subscription_components",
        scenario,
        start.elapsed(),
        target_messages,
        field_count + 3,
        target_messages * field_count,
        "field_ops",
        format!(
            "target_messages={target_messages}, all_fields={all_fields}, field_count={field_count}, output_columns={}, presence=production_non_null_binary_lsb_first, cached_events={}, component phases are independent microbenchmarks",
            field_count + 3,
            events.len()
        ),
    );
    record.phases = vec![
        phase("message_iteration", message_iteration_elapsed),
        phase("msg_elements", msg_elements_elapsed),
        phase("timestamp_topic_append", timestamp_topic_elapsed),
        phase("requested_field_lookup", requested_lookup_elapsed),
        phase("all_fields_get_at", all_fields_get_at_elapsed),
        phase("all_fields_scalar_filter", all_fields_datatype_elapsed),
        phase("all_fields_shape_type_cache", all_fields_name_cache_elapsed),
        phase("value_getter", value_getter_elapsed),
        phase("arrow_append", arrow_append_elapsed),
        phase("null_padding", null_padding_elapsed),
        phase("flush_schema", flush_schema_elapsed),
        phase("flush_arrays", flush_arrays_elapsed),
        phase("channel_send", channel_elapsed),
        phase("total", Duration::from_micros(record.elapsed_us as u64)),
    ];
    record
}

#[derive(Default)]
struct SubscriptionReplayDrain {
    accepted_rows: usize,
    accepted_batches: usize,
    columns: usize,
    terminal_error_count: usize,
    data_loss_error_count: usize,
    unexpected_error_count: usize,
    terminal_error_details: Vec<String>,
    terminal_eof_observed: bool,
}

impl SubscriptionReplayDrain {
    fn observe_error(&mut self, error: BlpError) {
        self.terminal_error_count += 1;
        match error {
            BlpError::SubscriptionDataLoss { topic, detail } => {
                self.data_loss_error_count += 1;
                self.terminal_error_details
                    .push(format!("subscription_data_loss({topic}): {detail}"));
            }
            error => {
                self.unexpected_error_count += 1;
                self.terminal_error_details.push(error.to_string());
            }
        }
    }
}

fn drain_subscription_updates(
    rx: &mut xbbg_async::engine::state::SubscriptionReceiver,
    limit: usize,
    drain: &mut SubscriptionReplayDrain,
) -> bool {
    let initial_terminal_errors = drain.terminal_error_count;
    let mut accepted = 0usize;
    while accepted < limit {
        match rx.try_recv() {
            Ok(Ok(update)) => {
                let (update_rows, update_columns) = subscription_update_shape(&update);
                drain.accepted_rows += update_rows;
                drain.columns = drain.columns.max(update_columns);
                drain.accepted_batches += 1;
                accepted += 1;
                black_box(update);
            }
            Ok(Err(error)) => drain.observe_error(error),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                drain.terminal_eof_observed = true;
                break;
            }
        }
    }
    drain.terminal_error_count > initial_terminal_errors
}

fn replay_subscription_events(
    scenario: &'static str,
    events: &[Event],
    target_messages: usize,
    topic_count: usize,
    all_fields: bool,
    fields: &[&str],
) -> BenchRecord {
    let start = Instant::now();
    if events.is_empty() {
        return BenchRecord::error(
            "subscription_replay",
            scenario,
            Duration::ZERO,
            "no cached subscription events",
        );
    }

    let cached_messages = events
        .iter()
        .map(|event| event.messages().count())
        .sum::<usize>();
    if cached_messages == 0 {
        return BenchRecord::error(
            "subscription_replay",
            scenario,
            Duration::ZERO,
            "cached subscription events contain no messages",
        );
    }
    let repeats_per_message = (target_messages / cached_messages).max(1);

    let topic_count = topic_count.max(1);
    let (tx, mut rx) = subscription_channel(topic_count.saturating_mul(4).max(16));
    let field_vec = fields
        .iter()
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let mut states = (0..topic_count)
        .map(|idx| {
            SubscriptionState::new(
                format!("SYN{idx:05} US Equity"),
                field_vec.clone(),
                tx.clone(),
                1,
                all_fields,
            )
        })
        .collect::<Vec<_>>();
    drop(tx);

    let process_start = Instant::now();
    let mut processed = 0usize;
    let mut stop_reason = "completed";
    let mut fatal_stream = false;
    let mut drain = SubscriptionReplayDrain::default();
    'produce: while processed < target_messages {
        for event in events {
            for msg in event.messages() {
                for _ in 0..repeats_per_message {
                    let idx = processed % topic_count;
                    let outcome = states[idx].on_message(&msg);
                    processed += 1;

                    if drain_subscription_updates(&mut rx, 1, &mut drain) {
                        fatal_stream = true;
                        stop_reason = "consumer_error";
                    }
                    match outcome {
                        MessageOutcome::Normal { .. } => {}
                        MessageOutcome::DataLoss => {
                            fatal_stream = true;
                            stop_reason = "message_data_loss";
                        }
                        MessageOutcome::Closed => {
                            fatal_stream = true;
                            stop_reason = "message_closed";
                        }
                    }
                    if fatal_stream || processed >= target_messages {
                        break 'produce;
                    }
                }
            }
        }
    }
    let process_elapsed = process_start.elapsed();

    let flush_start = Instant::now();
    for state in &mut states {
        state.flush();
    }
    let flush_elapsed = flush_start.elapsed();
    let dropped_batches = states
        .iter()
        .map(|state| state.dropped_batches)
        .sum::<u64>();
    drop(states);

    let drain_start = Instant::now();
    drain_subscription_updates(&mut rx, usize::MAX, &mut drain);
    let drain_elapsed = drain_start.elapsed();

    let expected_data_loss = dropped_batches > 0 || stop_reason == "message_data_loss";
    let gap_outcome = if expected_data_loss
        && drain.terminal_error_count == 1
        && drain.data_loss_error_count == 1
        && drain.unexpected_error_count == 0
        && drain.terminal_eof_observed
    {
        "expected_data_loss_observed"
    } else if expected_data_loss {
        "expected_data_loss_mismatch"
    } else if drain.terminal_error_count > 0 {
        "unexpected_terminal_error"
    } else if stop_reason == "message_closed" {
        "closed_without_terminal_error"
    } else {
        "no_gap"
    };
    let error_detail = drain.terminal_error_details.join(" | ");

    let mut record = BenchRecord::ok(
        "subscription_replay",
        scenario,
        start.elapsed(),
        drain.accepted_rows,
        drain.columns,
        processed,
        "messages",
        format!(
            "target_messages={target_messages}, processed_messages={processed}, accepted_rows={}, topics={topic_count}, batches={}, dropped_batches={dropped_batches}, stop_reason={stop_reason}, expected_data_loss={expected_data_loss}, gap_outcome={gap_outcome}, terminal_errors={}, data_loss_errors={}, unexpected_errors={}, terminal_eof={}, error_detail={error_detail:?}, all_fields={all_fields}, cached_events={}, cached_messages={cached_messages}, repeats_per_message={repeats_per_message}, compatibility_flush_threshold=1 (does not batch output), queue_capacity_units=SubscriptionUpdate batches, producer interleaved with consumer dequeue",
            drain.accepted_rows,
            drain.accepted_batches,
            drain.terminal_error_count,
            drain.data_loss_error_count,
            drain.unexpected_error_count,
            drain.terminal_eof_observed,
            events.len()
        ),
    );
    if !matches!(gap_outcome, "no_gap" | "expected_data_loss_observed") {
        record.status = "error".to_string();
    }
    record.phases = vec![
        phase(
            "process_messages_and_interleaved_consumer_dequeue",
            process_elapsed,
        ),
        phase("compatibility_flush_call", flush_elapsed),
        phase(
            "final_queue_drain_and_terminal_classification",
            drain_elapsed,
        ),
        phase("total", Duration::from_micros(record.elapsed_us as u64)),
    ];
    record
}

async fn run_live_suite(engine: &Engine, config: &SuiteConfig) -> Vec<BenchRecord> {
    let mut records = Vec::new();

    if config.should_run("live_requests", "bdp_smoke") {
        records.push(live_request(engine, "bdp_smoke", bdp_params()).await);
    }
    if config.should_run("live_requests", "bdh_smoke") {
        records.push(live_request(engine, "bdh_smoke", bdh_params()).await);
    }
    if config.should_run("live_requests", "bdtick_smoke") {
        records.push(live_request(engine, "bdtick_smoke", bdtick_params()).await);
    }
    if config.should_run("live_requests", "bql_smoke") {
        records.push(live_request(engine, "bql_smoke", bql_params()).await);
    }
    if config.should_run("live_subscriptions", "sub_3_topics_3_fields") {
        records.push(live_subscription(engine, config.profile.subscription_collect_ms()).await);
    }

    records
}

fn create_engine() -> Result<Engine, BlpAsyncError> {
    let config = EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new(blp_host(), blp_port())]),
        ..Default::default()
    };
    Engine::start(config)
}

async fn live_request(
    engine: &Engine,
    scenario: &'static str,
    params: RequestParams,
) -> BenchRecord {
    let start = Instant::now();
    match engine.request(params).await {
        Ok(batch) => {
            let elapsed = start.elapsed();
            let rows = batch.num_rows();
            let columns = batch.num_columns();
            BenchRecord::ok(
                "live_requests",
                scenario,
                elapsed,
                rows,
                columns,
                rows.saturating_mul(columns),
                "cells",
                format!("schema={}", schema_summary(&batch)),
            )
        }
        Err(err) => BenchRecord::error("live_requests", scenario, start.elapsed(), err.to_string()),
    }
}

async fn live_subscription(engine: &Engine, collect_ms: u64) -> BenchRecord {
    let topics = vec![
        "IBM US Equity".to_string(),
        "AAPL US Equity".to_string(),
        "MSFT US Equity".to_string(),
    ];
    let fields = vec![
        "LAST_PRICE".to_string(),
        "BID".to_string(),
        "ASK".to_string(),
    ];
    let start = Instant::now();
    let mut stream = match engine
        .subscribe(topics.clone(), fields.clone(), false)
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            return BenchRecord::error(
                "live_subscriptions",
                "sub_3_topics_3_fields",
                start.elapsed(),
                err.to_string(),
            )
        }
    };

    let mut batches = 0usize;
    let mut rows = 0usize;
    let deadline = Instant::now() + Duration::from_millis(collect_ms);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(update))) => {
                let (update_rows, _) = subscription_update_shape(&update);
                batches += 1;
                rows += update_rows;
            }
            Ok(Some(Err(err))) => {
                let _ = stream.unsubscribe(true).await;
                return BenchRecord::error(
                    "live_subscriptions",
                    "sub_3_topics_3_fields",
                    start.elapsed(),
                    err.to_string(),
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
    let elapsed = start.elapsed();
    let _ = stream.unsubscribe(true).await;
    BenchRecord::ok(
        "live_subscriptions",
        "sub_3_topics_3_fields",
        elapsed,
        rows,
        fields.len(),
        rows,
        "rows",
        format!(
            "topics={}, fields={}, batches={}, collect_ms={}",
            topics.len(),
            fields.len(),
            batches,
            collect_ms
        ),
    )
}

fn bdp_params() -> RequestParams {
    RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    }
}

fn bdh_params() -> RequestParams {
    RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "HistoricalDataRequest".to_string(),
        extractor: ExtractorType::HistData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some("20241202".to_string()),
        end_date: Some("20241206".to_string()),
        ..Default::default()
    }
}

fn bdtick_params() -> RequestParams {
    let date = previous_weekday().format("%Y-%m-%d").to_string();
    RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "IntradayTickRequest".to_string(),
        extractor: ExtractorType::IntradayTick,
        security: Some("IBM US Equity".to_string()),
        start_datetime: Some(format!("{date}T14:30:00")),
        end_datetime: Some(format!("{date}T14:31:00")),
        event_types: Some(vec!["TRADE".to_string()]),
        request_tz: Some("UTC".to_string()),
        output_tz: Some("UTC".to_string()),
        ..Default::default()
    }
}

fn bql_params() -> RequestParams {
    RequestParams {
        service: "//blp/bqlsvc".to_string(),
        operation: "sendQuery".to_string(),
        extractor: ExtractorType::Bql,
        elements: Some(vec![(
            "expression".to_string(),
            "get(px_last) for(['IBM US Equity'])".to_string(),
        )]),
        ..Default::default()
    }
}

fn previous_weekday() -> NaiveDate {
    let mut date = Local::now().date_naive() - ChronoDuration::days(1);
    while matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
        date -= ChronoDuration::days(1);
    }
    date
}

fn synthetic_bdp(shape: SyntheticShape, detail: bool) -> BenchRecord {
    let start = Instant::now();
    let rows = shape.bdp_securities.saturating_mul(shape.bdp_fields);
    let generate_start = Instant::now();
    let mut tickers = Vec::with_capacity(rows);
    let mut fields = Vec::with_capacity(rows);
    let mut nums = Vec::with_capacity(rows);
    let mut strings = Vec::with_capacity(rows);
    for s in 0..shape.bdp_securities {
        let ticker = format!("SYN{s:06} US Equity");
        for f in 0..shape.bdp_fields {
            tickers.push(ticker.clone());
            fields.push(format!("FIELD_{f:02}"));
            if f % 7 == 0 {
                nums.push(None);
                strings.push(Some(format!("TXT_{}", s % 97)));
            } else {
                nums.push(Some((s as f64 * 0.01) + f as f64));
                strings.push(None);
            }
        }
    }
    let generate_elapsed = generate_start.elapsed();
    let build_start = Instant::now();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value_num", DataType::Float64, true),
            Field::new("value_str", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(tickers)) as ArrayRef,
            Arc::new(StringArray::from(fields)) as ArrayRef,
            Arc::new(Float64Array::from(nums)) as ArrayRef,
            Arc::new(StringArray::from(strings)) as ArrayRef,
        ],
    )
    .expect("synthetic bdp batch");
    let build_elapsed = build_start.elapsed();
    black_box(&batch);
    let mut record = BenchRecord::ok(
        "synthetic_bdp",
        format!("bdp_{}s_{}f", shape.bdp_securities, shape.bdp_fields),
        start.elapsed(),
        batch.num_rows(),
        batch.num_columns(),
        rows,
        "values",
        "generated mixed numeric/string/null reference-data rows",
    );
    if detail {
        record.phases = vec![
            phase("generate_values", generate_elapsed),
            phase("build_arrow_batch", build_elapsed),
            phase("total", Duration::from_micros(record.elapsed_us as u64)),
        ];
    }
    record
}

fn synthetic_bdh(shape: SyntheticShape, detail: bool) -> BenchRecord {
    let start = Instant::now();
    let output_rows = shape.bdh_securities.saturating_mul(shape.bdh_dates);
    let values = output_rows.saturating_mul(shape.bdh_fields);
    let keys_start = Instant::now();
    let mut tickers = Vec::with_capacity(output_rows);
    let mut dates = Vec::with_capacity(output_rows);
    let base_date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("valid base date");
    for s in 0..shape.bdh_securities {
        let ticker = format!("SYN{s:06} US Equity");
        for d in 0..shape.bdh_dates {
            tickers.push(ticker.clone());
            let date = base_date + ChronoDuration::days(d as i64);
            dates.push(format!(
                "{:04}{:02}{:02}",
                date.year(),
                date.month(),
                date.day()
            ));
        }
    }
    let keys_elapsed = keys_start.elapsed();

    let values_start = Instant::now();
    let mut schema_fields = vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(tickers)) as ArrayRef,
        Arc::new(StringArray::from(dates)) as ArrayRef,
    ];
    for f in 0..shape.bdh_fields {
        schema_fields.push(Field::new(
            format!("HIST_FIELD_{f:02}"),
            DataType::Float64,
            true,
        ));
        let column: Vec<Option<f64>> = (0..output_rows)
            .map(|row| {
                if (row + f) % 23 == 0 {
                    None
                } else {
                    Some(row as f64 * 0.1 + f as f64)
                }
            })
            .collect();
        arrays.push(Arc::new(Float64Array::from(column)) as ArrayRef);
    }
    let values_elapsed = values_start.elapsed();

    let build_start = Instant::now();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), arrays)
        .expect("synthetic bdh batch");
    let build_elapsed = build_start.elapsed();
    black_box(&batch);
    let mut record = BenchRecord::ok(
        "synthetic_bdh",
        format!(
            "bdh_{}s_{}d_{}f",
            shape.bdh_securities, shape.bdh_dates, shape.bdh_fields
        ),
        start.elapsed(),
        batch.num_rows(),
        batch.num_columns(),
        values,
        "values",
        "generated wide historical rows with sparse nulls",
    );
    if detail {
        record.phases = vec![
            phase("generate_keys", keys_elapsed),
            phase("generate_field_values", values_elapsed),
            phase("build_record_batch", build_elapsed),
            phase("total", Duration::from_micros(record.elapsed_us as u64)),
        ];
    }
    record
}

fn synthetic_bdtick(shape: SyntheticShape, detail: bool) -> BenchRecord {
    let start = Instant::now();
    let rows = shape.bdtick_ticks;
    let generate_start = Instant::now();
    let base = 1_735_564_200_000_000_i64;
    let mut times = Vec::with_capacity(rows);
    let mut event_types = Vec::with_capacity(rows);
    let mut values = Vec::with_capacity(rows);
    let mut sizes = Vec::with_capacity(rows);
    for i in 0..rows {
        times.push(base + i as i64 * 1_000);
        event_types.push(match i % 3 {
            0 => "TRADE",
            1 => "BID",
            _ => "ASK",
        });
        values.push(100.0 + (i % 10_000) as f64 * 0.0001);
        sizes.push((i % 1_000) as i64 + 1);
    }
    let generate_elapsed = generate_start.elapsed();
    let build_start = Instant::now();
    let time_array = TimestampMicrosecondArray::from(times).with_timezone("UTC");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("size", DataType::Int64, false),
        ])),
        vec![
            Arc::new(time_array) as ArrayRef,
            Arc::new(StringArray::from(event_types)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(sizes)) as ArrayRef,
        ],
    )
    .expect("synthetic bdtick batch");
    let build_elapsed = build_start.elapsed();
    black_box(&batch);
    let mut record = BenchRecord::ok(
        "synthetic_bdtick",
        format!("bdtick_{}ticks", rows),
        start.elapsed(),
        batch.num_rows(),
        batch.num_columns(),
        rows,
        "ticks",
        "generated mixed TRADE/BID/ASK tick rows",
    );
    if detail {
        record.phases = vec![
            phase("generate_ticks", generate_elapsed),
            phase("build_arrow_batch", build_elapsed),
            phase("total", Duration::from_micros(record.elapsed_us as u64)),
        ];
    }
    record
}

fn synthetic_bql(shape: SyntheticShape, detail: bool) -> BenchRecord {
    let start = Instant::now();
    let rows = shape.bql_rows;
    let columns = shape.bql_columns;
    let generate_start = Instant::now();
    let mut schema_fields = Vec::with_capacity(columns + 1);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns + 1);
    let ids: Vec<String> = (0..rows).map(|i| format!("ID_{i:08}")).collect();
    schema_fields.push(Field::new("id", DataType::Utf8, false));
    arrays.push(Arc::new(StringArray::from(ids)) as ArrayRef);
    for c in 0..columns {
        schema_fields.push(Field::new(format!("value_{c:02}"), DataType::Float64, true));
        let values: Vec<Option<f64>> = (0..rows)
            .map(|r| {
                if (r + c) % 29 == 0 {
                    None
                } else {
                    Some(r as f64 * 0.01 + c as f64)
                }
            })
            .collect();
        arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
    }
    let generate_elapsed = generate_start.elapsed();
    let build_start = Instant::now();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), arrays)
        .expect("synthetic bql batch");
    let build_elapsed = build_start.elapsed();
    black_box(&batch);
    let mut record = BenchRecord::ok(
        "synthetic_bql",
        format!("bql_{}r_{}c", rows, columns),
        start.elapsed(),
        batch.num_rows(),
        batch.num_columns(),
        rows.saturating_mul(columns),
        "cells",
        "generated dynamic-column BQL-style table",
    );
    if detail {
        record.phases = vec![
            phase("generate_columns", generate_elapsed),
            phase("build_record_batch", build_elapsed),
            phase("total", Duration::from_micros(record.elapsed_us as u64)),
        ];
    }
    record
}

fn synthetic_subscriptions(shape: SyntheticShape, detail: bool) -> BenchRecord {
    let start = Instant::now();
    let process_start = Instant::now();
    let mut checksum = 0.0f64;
    for i in 0..shape.sub_messages {
        let topic_id = i % shape.sub_topics;
        for f in 0..shape.sub_fields {
            checksum += ((topic_id + f + i) % 10_000) as f64 * 0.0001;
        }
    }
    let process_elapsed = process_start.elapsed();
    black_box(checksum);
    let mut record = BenchRecord::ok(
        "synthetic_subscriptions",
        format!(
            "sub_{}topics_{}messages_{}fields",
            shape.sub_topics, shape.sub_messages, shape.sub_fields
        ),
        start.elapsed(),
        shape.sub_messages,
        shape.sub_fields,
        shape.sub_messages,
        "messages",
        format!("checksum={checksum:.4}"),
    );
    if detail {
        record.phases = vec![
            phase("process_messages", process_elapsed),
            phase("total", Duration::from_micros(record.elapsed_us as u64)),
        ];
    }
    record
}

fn schema_summary(batch: &RecordBatch) -> String {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join("|")
}

fn print_usage(profile: BenchProfile, shape: SyntheticShape) {
    println!("Estimated Bloomberg usage:");
    println!("  BDP:      1 request / 1 data point");
    println!("  BDH:      1 request / ~5 data points");
    println!("  BDTICK:   1 short intraday request");
    println!("  BQL:      1 tiny query");
    println!(
        "  SUB:      1 live subscription window / {}ms",
        profile.subscription_collect_ms()
    );
    println!("  Synthetic: no Bloomberg data usage");
    println!("  Replay:    1 seed request per selected replay case, then cached SDK Event replay");
    println!("Replay scale:");
    println!(
        "  Request event replay iterations: {}",
        profile.replay_iterations()
    );
    println!(
        "  Subscription replay messages: {}",
        profile.subscription_replay_messages()
    );
    println!(
        "  Subscription replay topics: {}",
        profile.subscription_replay_topics()
    );
    println!(
        "  BQL JSON extraction iterations: {}",
        profile.bql_json_iterations()
    );
    println!("Synthetic scale:");
    println!(
        "  BDP:    {} securities × {} fields",
        shape.bdp_securities, shape.bdp_fields
    );
    println!(
        "  BDH:    {} securities × {} dates × {} fields",
        shape.bdh_securities, shape.bdh_dates, shape.bdh_fields
    );
    println!("  BDTICK: {} ticks", shape.bdtick_ticks);
    println!(
        "  BQL:    {} rows × {} columns",
        shape.bql_rows, shape.bql_columns
    );
    println!(
        "  SUB:    {} messages × {} fields",
        shape.sub_messages, shape.sub_fields
    );
}

fn print_summary(records: &[BenchRecord]) {
    println!(
        "{:<28} {:<34} {:<8} {:>12} {:>12} {:>14}",
        "suite", "scenario", "status", "elapsed_ms", "rows", "throughput"
    );
    println!("{:-<112}", "");
    for r in records {
        println!(
            "{:<28} {:<34} {:<8} {:>12.2} {:>12} {:>10.2} {}/s",
            r.suite,
            truncate(&r.scenario, 34),
            r.status,
            r.elapsed_us as f64 / 1000.0,
            r.rows,
            r.throughput_per_sec,
            r.throughput_name
        );
    }
}

fn render_phases_json(phases: &[PhaseMetric]) -> String {
    if phases.is_empty() {
        return "[]".to_string();
    }
    let items = phases
        .iter()
        .map(|p| {
            format!(
                "{{\"name\":\"{}\",\"elapsed_us\":{}}}",
                escape_json(p.name),
                p.elapsed_us
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{}]", items)
}

fn render_allocations_json(allocations: Option<AllocDelta>) -> String {
    match allocations {
        Some(a) => format!(
            "{{\"scope\":\"separate untimed execution with global allocator counters; reported timing excludes allocator counters\",\"sample_count\":1,\"alloc_count\":{},\"alloc_bytes\":{},\"dealloc_count\":{},\"dealloc_bytes\":{},\"net_alloc_bytes\":{},\"allocs_per_row\":{:.8},\"bytes_per_row\":{:.4},\"allocs_per_value\":{:.8},\"bytes_per_value\":{:.4}}}",
            a.alloc_count,
            a.alloc_bytes,
            a.dealloc_count,
            a.dealloc_bytes,
            a.net_alloc_bytes,
            a.allocs_per_row,
            a.bytes_per_row,
            a.allocs_per_value,
            a.bytes_per_value
        ),
        None => "null".to_string(),
    }
}

fn render_optional_string(value: Option<&str>) -> String {
    value
        .map(|v| format!("\"{}\"", escape_json(v)))
        .unwrap_or_else(|| "null".to_string())
}

fn render_json(
    config: &SuiteConfig,
    timestamp: u64,
    git_sha: &str,
    shape: SyntheticShape,
    records: &[BenchRecord],
) -> String {
    let records_json = records
        .iter()
        .map(|r| {
            let phases_json = render_phases_json(&r.phases);
            let allocations_json = render_allocations_json(r.allocations);
            format!(
                "    {{\n      \"suite\": \"{}\",\n      \"scenario\": \"{}\",\n      \"status\": \"{}\",\n      \"elapsed_us\": {},\n      \"rows\": {},\n      \"columns\": {},\n      \"values\": {},\n      \"throughput_name\": \"{}\",\n      \"throughput_per_sec\": {:.4},\n      \"detail\": \"{}\",\n      \"phases\": {},\n      \"allocations\": {}\n    }}",
                escape_json(r.suite),
                escape_json(&r.scenario),
                escape_json(&r.status),
                r.elapsed_us,
                r.rows,
                r.columns,
                r.values,
                escape_json(r.throughput_name),
                r.throughput_per_sec,
                escape_json(&r.detail),
                phases_json,
                allocations_json
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let input_descriptor = format!(
        "profile={};profile_mode={};bench_only={:?};bdp={}x{};bdh={}x{}x{};bdtick={};bql={}x{};sub={}x{}x{}",
        config.profile.as_str(),
        config.profile_mode.as_str(),
        config.only,
        shape.bdp_securities,
        shape.bdp_fields,
        shape.bdh_securities,
        shape.bdh_dates,
        shape.bdh_fields,
        shape.bdtick_ticks,
        shape.bql_rows,
        shape.bql_columns,
        shape.sub_messages,
        shape.sub_topics,
        shape.sub_fields,
    );
    let provenance = xbbg_bench::benchmark_provenance_json(&input_descriptor);
    format!(
        "{{\n  \"schema_version\": 2,\n  \"suite\": \"xbbg_benchmark_suite\",\n  \"timestamp\": {},\n  \"profile\": \"{}\",\n  \"profile_mode\": \"{}\",\n  \"bench_only\": {},\n  \"source_checkout_git_commit\": \"{}\",\n  \"coverage\": \"mixed suite: live SDK/network probes are explicitly separate from cached Event replay and synthetic Arrow workloads\",\n  \"timing_scope\": \"scenario-specific warm measurements; detail allocations come from a separate untimed execution\",\n  \"provenance\": {},\n  \"bloomberg\": {{\n    \"host\": \"{}\",\n    \"port\": {}\n  }},\n  \"synthetic_shape\": {{\n    \"bdp_securities\": {},\n    \"bdp_fields\": {},\n    \"bdh_securities\": {},\n    \"bdh_dates\": {},\n    \"bdh_fields\": {},\n    \"bdtick_ticks\": {},\n    \"bql_rows\": {},\n    \"bql_columns\": {},\n    \"sub_messages\": {},\n    \"sub_topics\": {},\n    \"sub_fields\": {}\n  }},\n  \"benchmarks\": [\n{}\n  ]\n}}\n",
        timestamp,
        config.profile.as_str(),
        config.profile_mode.as_str(),
        render_optional_string(config.only.as_deref()),
        escape_json(git_sha),
        provenance,
        escape_json(&blp_host()),
        blp_port(),
        shape.bdp_securities,
        shape.bdp_fields,
        shape.bdh_securities,
        shape.bdh_dates,
        shape.bdh_fields,
        shape.bdtick_ticks,
        shape.bql_rows,
        shape.bql_columns,
        shape.sub_messages,
        shape.sub_topics,
        shape.sub_fields,
        records_json
    )
}

fn render_markdown(
    config: &SuiteConfig,
    timestamp: u64,
    git_sha: &str,
    shape: SyntheticShape,
    records: &[BenchRecord],
) -> String {
    let mut out = String::new();
    out.push_str("# xbbg Benchmark Suite\n\n");
    out.push_str(&format!("- Timestamp: `{timestamp}`\n"));
    out.push_str(&format!("- Profile: `{}`\n", config.profile.as_str()));
    out.push_str(&format!(
        "- Profile mode: `{}`\n",
        config.profile_mode.as_str()
    ));
    if let Some(only) = &config.only {
        out.push_str(&format!("- Scenario filter: `{only}`\n"));
    }
    out.push_str(&format!("- Git SHA: `{git_sha}`\n"));
    out.push_str(&format!("- Bloomberg: `{}:{}`\n\n", blp_host(), blp_port()));
    out.push_str("## Synthetic Shape\n\n");
    out.push_str(&format!(
        "- BDP: {} securities × {} fields\n",
        shape.bdp_securities, shape.bdp_fields
    ));
    out.push_str(&format!(
        "- BDH: {} securities × {} dates × {} fields\n",
        shape.bdh_securities, shape.bdh_dates, shape.bdh_fields
    ));
    out.push_str(&format!("- BDTICK: {} ticks\n", shape.bdtick_ticks));
    out.push_str(&format!(
        "- BQL: {} rows × {} columns\n",
        shape.bql_rows, shape.bql_columns
    ));
    out.push_str(&format!(
        "- SUB: {} messages × {} fields\n\n",
        shape.sub_messages, shape.sub_fields
    ));
    out.push_str("## Results\n\n");
    out.push_str("| Suite | Scenario | Status | Elapsed ms | Rows | Throughput | Alloc bytes |\n");
    out.push_str("|---|---|---:|---:|---:|---:|---:|\n");
    for r in records {
        let alloc_bytes = r
            .allocations
            .map(|a| a.alloc_bytes.to_string())
            .unwrap_or_else(|| "-".to_string());
        out.push_str(&format!(
            "| {} | {} | {} | {:.2} | {} | {:.2} {}/s | {} |\n",
            r.suite,
            r.scenario,
            r.status,
            r.elapsed_us as f64 / 1000.0,
            r.rows,
            r.throughput_per_sec,
            r.throughput_name,
            alloc_bytes
        ));
    }
    if config.profile_mode.is_detail() {
        out.push_str("\n## Detail profiling\n\n");
        for r in records {
            out.push_str(&format!("### {} / {}\n\n", r.suite, r.scenario));
            if !r.phases.is_empty() {
                out.push_str("Phase timings:\n\n");
                for p in &r.phases {
                    out.push_str(&format!("- `{}`: {} µs\n", p.name, p.elapsed_us));
                }
                out.push('\n');
            }
            if let Some(a) = r.allocations {
                out.push_str(&format!(
                    "Allocations: {} allocs / {} bytes; {:.4} allocs/row; {:.2} bytes/row; {:.4} allocs/value; {:.2} bytes/value\n\n",
                    a.alloc_count, a.alloc_bytes, a.allocs_per_row, a.bytes_per_row, a.allocs_per_value, a.bytes_per_value
                ));
            }
        }
    }
    out
}

fn write_results(timestamp: u64, json: &str, markdown: &str) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    fs::create_dir_all(&dir).expect("create benchmark results directory");
    let json_path = dir.join(format!("xbbg_benchmark_suite_{timestamp}.json"));
    let json_latest = dir.join("xbbg_benchmark_suite_latest.json");
    let md_path = dir.join(format!("xbbg_benchmark_suite_{timestamp}.md"));
    let md_latest = dir.join("xbbg_benchmark_suite_latest.md");
    write_file(&json_path, json);
    write_file(&json_latest, json);
    write_file(&md_path, markdown);
    write_file(&md_latest, markdown);
    println!("\nResults written:");
    println!("  {}", json_path.display());
    println!("  {}", json_latest.display());
    println!("  {}", md_path.display());
    println!("  {}", md_latest.display());
}

fn write_file(path: &Path, content: &str) {
    fs::write(path, content)
        .unwrap_or_else(|err| panic!("failed to write {}: {err}", path.display()));
}

fn blp_host() -> String {
    std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn blp_port() -> u16 {
    std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(out.stdout)
            } else {
                None
            }
        })
        .and_then(|stdout| String::from_utf8(stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn escape_json(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
