//! Cached real Bloomberg subscription message -> xbbg-async SubscriptionState -> Arrow benchmark.
//!
//! Bloomberg `Event`s in memory, then replays those cached events many times
//! through the real `SubscriptionState::on_message` path. Producer and consumer
//! progress are scheduled independently against the exact bounded channel
//! capacity, so overflow, cancellation, drain, and discard behavior remain
//! executable without issuing additional Bloomberg requests.
//!
//! It does not change production hot paths.
//!
//! Run:
//!   CACHED_SUB_TICKER="XBTUSD Curncy" CACHED_SUB_CAPTURE_MESSAGES=25 \
//!     CACHED_SUB_REPLAY_LOOPS=1000 cargo bench -p xbbg-bench --bench cached_subscription_arrow

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use xbbg_async::engine::state::{
    subscription_channel, subscription_update_to_record_batch, MessageOutcome,
    SubscriptionReceiver, SubscriptionState,
};
use xbbg_async::engine::OverflowPolicy;
use xbbg_bench::write_json;
use xbbg_core::{CorrelationId, Event, EventType, Session, SessionOptions, SubscriptionList};

const DEFAULT_TICKER: &str = "XBTUSD Curncy";
const DEFAULT_FIELDS: &str = "LAST_PRICE,BID,ASK";
const DEFAULT_CAPTURE_MESSAGES: usize = 25;
const DEFAULT_CAPTURE_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_REPLAY_LOOPS: usize = 1_000;
const DEFAULT_COMPATIBILITY_FLUSH_THRESHOLD: usize = 1_024;
const DEFAULT_CHANNEL_CAPACITY: usize = 1_024;
const DEFAULT_CONSUMER_POLL_MESSAGES: usize = 1;
const DEFAULT_CONSUMER_BATCH: usize = 1;
const DEFAULT_ITERATIONS: usize = 5;
const LIVE_ENABLE_ENV: &str = "CACHED_SUB_ENABLE_LIVE";

#[derive(Debug)]
struct BenchConfig {
    ticker: String,
    fields: Vec<String>,
    capture_messages: usize,
    capture_timeout_ms: u64,
    replay_loops: usize,
    compatibility_flush_threshold: usize,
    channel_capacity: usize,
    consumer_poll_messages: usize,
    consumer_batch: usize,
    consumer_delay_us: u64,
    cancel_after_messages: usize,
    drain_on_cancel: bool,
    iterations: usize,
    capture_all_fields: bool,
}

struct CaptureResult {
    events: Vec<Event>,
    messages: usize,
    capture_elapsed_ms: u128,
}

#[derive(Debug)]
struct ReplayResult {
    iteration: usize,
    input_events: usize,
    processed_messages: usize,
    configured_replay_loops: usize,
    completed_replay_loops: usize,
    accepted_rows: usize,
    accepted_batches: usize,
    total_columns_emitted: usize,
    elapsed_us: u128,
    messages_per_sec: f64,
    rows_per_sec: f64,
    cells_per_sec: f64,
    avg_columns_per_batch: f64,
    ns_per_message: f64,
    channel_capacity: usize,
    max_queue_depth: usize,
    dropped_batches: u64,
    post_cancel_drained_batches: usize,
    delivered_after_drop: usize,
    discarded_batches: usize,
    terminal_error_count: usize,
    data_loss_error_count: usize,
    unexpected_error_count: usize,
    terminal_error_detail: String,
    terminal_eof_observed: bool,
    expected_data_loss: bool,
    gap_outcome: &'static str,
    stop_reason: &'static str,
    status: &'static str,
    cancelled: bool,
    drain_on_cancel: bool,
}
fn main() {
    let config = BenchConfig::from_env();

    println!("xbbg cached subscription -> Arrow benchmark");
    println!("============================================\n");
    println!(
        "capture ticker={} fields={:?} target_messages={} timeout_ms={} all_fields={}",
        config.ticker,
        config.fields,
        config.capture_messages,
        config.capture_timeout_ms,
        config.capture_all_fields
    );
    println!(
        "replay loops={} compatibility_flush_threshold={} iterations={} channel_capacity={} consumer_poll_messages={} consumer_batch={} consumer_delay_us={} cancel_after_messages={} drain_on_cancel={}\n",
        config.replay_loops,
        config.compatibility_flush_threshold,
        config.iterations,
        config.channel_capacity,
        config.consumer_poll_messages,
        config.consumer_batch,
        config.consumer_delay_us,
        config.cancel_after_messages,
        config.drain_on_cancel,
    );

    if !live_capture_enabled() {
        println!(
            "Skipping cached subscription benchmark: set {LIVE_ENABLE_ENV}=1 to enable the live Bloomberg capture."
        );
        return;
    }

    let session = match setup_subscription_session() {
        Ok(session) => session,
        Err(err) => {
            println!(
                "Skipping cached subscription benchmark: Bloomberg session unavailable ({err})."
            );
            return;
        }
    };
    let capture = match capture_subscription_events(&session, &config) {
        Ok(capture) => capture,
        Err(err) => {
            session.stop();
            println!("Skipping cached subscription benchmark: capture unavailable ({err}).");
            return;
        }
    };
    session.stop();

    if capture.messages == 0 {
        println!(
            "Skipping cached subscription benchmark: captured zero subscription messages for {}; ensure Bloomberg is running and ticker is active.",
            config.ticker
        );
        return;
    }

    println!(
        "captured {} events / {} messages in {}ms\n",
        capture.events.len(),
        capture.messages,
        capture.capture_elapsed_ms
    );

    let mut results = Vec::with_capacity(config.iterations);
    for iteration in 1..=config.iterations {
        let result = replay_cached_events(iteration, &config, &capture.events);
        std::hint::black_box(&result);
        results.push(result);
    }

    print_results(&results);
    write_results(&config, &capture, &results);
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            ticker: std::env::var("CACHED_SUB_TICKER")
                .unwrap_or_else(|_| DEFAULT_TICKER.to_string()),
            fields: parse_fields(
                &std::env::var("CACHED_SUB_FIELDS").unwrap_or_else(|_| DEFAULT_FIELDS.to_string()),
            ),
            capture_messages: env_usize("CACHED_SUB_CAPTURE_MESSAGES", DEFAULT_CAPTURE_MESSAGES),
            capture_timeout_ms: env_u64(
                "CACHED_SUB_CAPTURE_TIMEOUT_MS",
                DEFAULT_CAPTURE_TIMEOUT_MS,
            ),
            replay_loops: env_usize("CACHED_SUB_REPLAY_LOOPS", DEFAULT_REPLAY_LOOPS),
            compatibility_flush_threshold: env_usize(
                "CACHED_SUB_COMPAT_FLUSH_THRESHOLD",
                DEFAULT_COMPATIBILITY_FLUSH_THRESHOLD,
            ),
            channel_capacity: env_usize("CACHED_SUB_CHANNEL_CAPACITY", DEFAULT_CHANNEL_CAPACITY),
            consumer_poll_messages: env_usize(
                "CACHED_SUB_CONSUMER_POLL_MESSAGES",
                DEFAULT_CONSUMER_POLL_MESSAGES,
            ),
            consumer_batch: env_usize("CACHED_SUB_CONSUMER_BATCH", DEFAULT_CONSUMER_BATCH),
            consumer_delay_us: env_u64("CACHED_SUB_CONSUMER_DELAY_US", 0),
            cancel_after_messages: env_usize("CACHED_SUB_CANCEL_AFTER_MESSAGES", 0),
            drain_on_cancel: env_bool("CACHED_SUB_DRAIN_ON_CANCEL", false),
            iterations: env_usize("CACHED_SUB_ITERATIONS", DEFAULT_ITERATIONS),
            capture_all_fields: env_bool("CACHED_SUB_ALL_FIELDS", false),
        }
    }
}

fn parse_fields(raw: &str) -> Vec<String> {
    let fields: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if fields.is_empty() {
        DEFAULT_FIELDS.split(',').map(ToOwned::to_owned).collect()
    } else {
        fields
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
fn live_capture_enabled() -> bool {
    env_bool(LIVE_ENABLE_ENV, false)
        || env_bool("XBBG_BENCH_LIVE", false)
        || env_bool("XBBG_LIVE_BENCHMARKS", false)
}

fn setup_subscription_session() -> Result<Session, String> {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8194);

    let mut options = SessionOptions::new()
        .map_err(|err| format!("failed to create session options: {err:?}"))?;
    options
        .set_server_host(&host)
        .map_err(|err| format!("failed to set host: {err:?}"))?;
    options.set_server_port(port);
    options.set_record_subscription_receive_times(true);

    let session =
        Session::new(&options).map_err(|err| format!("failed to create session: {err:?}"))?;
    session
        .start_and_wait(30_000)
        .map_err(|err| format!("failed to start subscription benchmark session: {err:?}"))?;
    Ok(session)
}

fn capture_subscription_events(
    session: &xbbg_core::Session,
    config: &BenchConfig,
) -> Result<CaptureResult, String> {
    let mut subscription_list = SubscriptionList::new();
    let cid = CorrelationId::new_int(1);
    let field_refs: Vec<&str> = config.fields.iter().map(String::as_str).collect();
    subscription_list
        .add(&config.ticker, &field_refs, "", &cid)
        .map_err(|err| format!("failed to add subscription: {err:?}"))?;

    session
        .subscribe(&subscription_list, None)
        .map_err(|err| format!("failed to subscribe: {err:?}"))?;

    let started = Instant::now();
    let deadline = started + Duration::from_millis(config.capture_timeout_ms);
    let mut events = Vec::new();
    let mut messages = 0usize;

    while messages < config.capture_messages {
        let timeout_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u32;
        if timeout_ms == 0 {
            break;
        }

        let Ok(event) = session.next_event(Some(timeout_ms)) else {
            continue;
        };

        if event.event_type() == EventType::SubscriptionData {
            let event_messages = event.messages().count();
            if event_messages > 0 {
                messages += event_messages;
                events.push(event);
            }
        }
    }

    session
        .unsubscribe(&subscription_list)
        .map_err(|err| format!("failed to unsubscribe after capture: {err:?}"))?;

    Ok(CaptureResult {
        events,
        messages,
        capture_elapsed_ms: started.elapsed().as_millis(),
    })
}

#[derive(Default)]
struct DrainCounters {
    rows: usize,
    batches: usize,
    columns: usize,
    cells: usize,
    terminal_error_count: usize,
    data_loss_error_count: usize,
    unexpected_error_count: usize,
    terminal_error_details: Vec<String>,
    adapter_error_details: Vec<String>,
    terminal_eof_observed: bool,
}

impl DrainCounters {
    fn observe_terminal_error(&mut self, error: xbbg_core::BlpError) {
        self.terminal_error_count += 1;
        match error {
            xbbg_core::BlpError::SubscriptionDataLoss { topic, detail } => {
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

    fn observe_adapter_error(&mut self, error: xbbg_core::BlpError) {
        self.unexpected_error_count += 1;
        self.adapter_error_details.push(error.to_string());
    }

    fn error_detail(&self) -> String {
        self.terminal_error_details
            .iter()
            .map(|detail| format!("stream: {detail}"))
            .chain(
                self.adapter_error_details
                    .iter()
                    .map(|detail| format!("arrow: {detail}")),
            )
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

#[derive(Clone, Copy)]
struct DrainResult {
    accepted_batches: usize,
    fatal: bool,
}

fn replay_cached_events(iteration: usize, config: &BenchConfig, events: &[Event]) -> ReplayResult {
    let (tx, mut rx) = subscription_channel(config.channel_capacity);
    let mut state = SubscriptionState::with_policy(
        config.ticker.clone(),
        config.fields.clone(),
        tx,
        config.compatibility_flush_threshold,
        OverflowPolicy::DropNewest,
        config.capture_all_fields,
    );
    let started = Instant::now();
    let mut processed_messages = 0usize;
    let mut completed_replay_loops = 0usize;
    let mut max_queue_depth = 0usize;
    let mut cancelled = false;
    let mut fatal_stream = false;
    let mut stop_reason = "completed";
    let mut saw_drop = false;
    let mut delivered_after_drop = 0usize;
    let mut counters = DrainCounters::default();

    'produce: for _ in 0..config.replay_loops {
        for event in events {
            for message in event.messages() {
                let outcome = state.on_message(&message);
                processed_messages += 1;
                saw_drop |= state.dropped_batches > 0;
                max_queue_depth = max_queue_depth.max(rx.len());

                if processed_messages.is_multiple_of(config.consumer_poll_messages) {
                    let drained = drain_updates(&mut rx, config.consumer_batch, &mut counters);
                    if saw_drop {
                        delivered_after_drop += drained.accepted_batches;
                    }
                    if drained.accepted_batches > 0 && config.consumer_delay_us > 0 {
                        std::thread::sleep(Duration::from_micros(config.consumer_delay_us));
                    }
                    if drained.fatal {
                        fatal_stream = true;
                        stop_reason = "consumer_error";
                    }
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
                if fatal_stream {
                    break 'produce;
                }

                if config.cancel_after_messages > 0
                    && processed_messages >= config.cancel_after_messages
                {
                    cancelled = true;
                    stop_reason = "cancelled";
                    break 'produce;
                }
            }
        }
        completed_replay_loops += 1;
    }

    state.flush();
    max_queue_depth = max_queue_depth.max(rx.len());
    let dropped_batches = state.dropped_batches;
    drop(state);

    let (discarded_batches, post_cancel_drained_batches) =
        if cancelled && !config.drain_on_cancel && !fatal_stream {
            rx.close();
            (discard_updates(&mut rx, &mut counters), 0)
        } else {
            let drained = drain_updates(&mut rx, usize::MAX, &mut counters);
            if saw_drop {
                delivered_after_drop += drained.accepted_batches;
            }
            (
                0,
                if cancelled {
                    drained.accepted_batches
                } else {
                    0
                },
            )
        };
    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);

    let expected_data_loss = dropped_batches > 0 || stop_reason == "message_data_loss";
    let gap_outcome = if expected_data_loss
        && counters.terminal_error_count == 1
        && counters.data_loss_error_count == 1
        && counters.unexpected_error_count == 0
        && counters.terminal_eof_observed
    {
        "expected_data_loss_observed"
    } else if expected_data_loss {
        "expected_data_loss_mismatch"
    } else if counters.terminal_error_count > 0 {
        "unexpected_terminal_error"
    } else if stop_reason == "message_closed" {
        "closed_without_terminal_error"
    } else {
        "no_gap"
    };
    let status = if matches!(gap_outcome, "no_gap" | "expected_data_loss_observed")
        && counters.adapter_error_details.is_empty()
    {
        "ok"
    } else {
        "error"
    };

    ReplayResult {
        iteration,
        input_events: events.len(),
        processed_messages,
        configured_replay_loops: config.replay_loops,
        completed_replay_loops,
        accepted_rows: counters.rows,
        accepted_batches: counters.batches,
        total_columns_emitted: counters.columns,
        elapsed_us: elapsed.as_micros(),
        messages_per_sec: processed_messages as f64 / elapsed_secs,
        rows_per_sec: counters.rows as f64 / elapsed_secs,
        cells_per_sec: counters.cells as f64 / elapsed_secs,
        avg_columns_per_batch: average_columns(counters.columns, counters.batches),
        ns_per_message: elapsed.as_nanos() as f64 / processed_messages.max(1) as f64,
        channel_capacity: config.channel_capacity,
        max_queue_depth,
        dropped_batches,
        discarded_batches,
        post_cancel_drained_batches,
        delivered_after_drop,
        terminal_error_count: counters.terminal_error_count,
        data_loss_error_count: counters.data_loss_error_count,
        unexpected_error_count: counters.unexpected_error_count,
        terminal_error_detail: counters.error_detail(),
        terminal_eof_observed: counters.terminal_eof_observed,
        expected_data_loss,
        gap_outcome,
        stop_reason,
        status,
        cancelled,
        drain_on_cancel: config.drain_on_cancel,
    }
}

fn drain_updates(
    rx: &mut SubscriptionReceiver,
    limit: usize,
    counters: &mut DrainCounters,
) -> DrainResult {
    let initial_terminal_errors = counters.terminal_error_count;
    let initial_adapter_errors = counters.adapter_error_details.len();
    let mut accepted_batches = 0usize;
    while accepted_batches < limit {
        match rx.try_recv() {
            Ok(Ok(update)) => match subscription_update_to_record_batch(&update) {
                Ok(batch) => {
                    counters.rows += batch.num_rows();
                    counters.batches += 1;
                    counters.columns += batch.num_columns();
                    counters.cells += batch.num_rows().saturating_mul(batch.num_columns());
                    accepted_batches += 1;
                    std::hint::black_box(batch);
                }
                Err(error) => {
                    counters.observe_adapter_error(error);
                    break;
                }
            },
            Ok(Err(error)) => counters.observe_terminal_error(error),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                counters.terminal_eof_observed = true;
                break;
            }
        }
    }
    DrainResult {
        accepted_batches,
        fatal: counters.terminal_error_count > initial_terminal_errors
            || counters.adapter_error_details.len() > initial_adapter_errors,
    }
}

fn discard_updates(rx: &mut SubscriptionReceiver, counters: &mut DrainCounters) -> usize {
    let mut discarded = 0usize;
    loop {
        match rx.try_recv() {
            Ok(Ok(_)) => discarded += 1,
            Ok(Err(error)) => counters.observe_terminal_error(error),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                counters.terminal_eof_observed = true;
                break;
            }
        }
    }
    discarded
}

fn average_columns(total_columns: usize, batches: usize) -> f64 {
    if batches == 0 {
        0.0
    } else {
        total_columns as f64 / batches as f64
    }
}

fn print_results(results: &[ReplayResult]) {
    println!("{:=<152}", "");
    println!("  cached real subscription Message -> SubscriptionState -> Arrow");
    println!("{:=<152}\n", "");
    println!(
        "  {:>9} {:>11} {:>11} {:>9} {:>9} {:>10} {:>13} {:>11} {:>7} {:>19}",
        "Iteration",
        "Processed",
        "Accepted",
        "Batches",
        "Cols/B",
        "Elapsed",
        "Rows/sec",
        "ns/msg",
        "Status",
        "Stop"
    );
    println!("  {:-<148}", "");

    for result in results {
        println!(
            "  {:>9} {:>11} {:>11} {:>9} {:>9.2} {:>9}us {:>13.0} {:>11.1} {:>7} {:>19}",
            result.iteration,
            result.processed_messages,
            result.accepted_rows,
            result.accepted_batches,
            result.avg_columns_per_batch,
            result.elapsed_us,
            result.rows_per_sec,
            result.ns_per_message,
            result.status,
            result.stop_reason,
        );
        println!(
            "             gap={}; terminal_errors={}; data_loss_errors={}; unexpected_errors={}; eof={}",
            result.gap_outcome,
            result.terminal_error_count,
            result.data_loss_error_count,
            result.unexpected_error_count,
            result.terminal_eof_observed,
        );
        if !result.terminal_error_detail.is_empty() {
            println!("             error={}", result.terminal_error_detail);
        }
    }

    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let avg_ns_per_message = results
        .iter()
        .map(|result| result.ns_per_message)
        .sum::<f64>()
        / results.len() as f64;
    println!("\n  Average rows/sec: {:.0}", avg_rows_per_sec);
    println!("  Average ns/processed message: {:.1}", avg_ns_per_message);
    let channel_capacity = results
        .first()
        .map(|result| result.channel_capacity)
        .unwrap_or_default();
    let max_queue_depth = results
        .iter()
        .map(|result| result.max_queue_depth)
        .max()
        .unwrap_or_default();
    let dropped_batches: u64 = results.iter().map(|result| result.dropped_batches).sum();
    let discarded_batches: usize = results.iter().map(|result| result.discarded_batches).sum();
    let delivered_after_drop: usize = results
        .iter()
        .map(|result| result.delivered_after_drop)
        .sum();
    let matched_gaps = results
        .iter()
        .filter(|result| result.gap_outcome == "expected_data_loss_observed")
        .count();
    let expected_gaps = results
        .iter()
        .filter(|result| result.expected_data_loss)
        .count();
    println!("  Channel capacity / max depth: {channel_capacity} / {max_queue_depth}");
    println!("  Dropped / discarded batches: {dropped_batches} / {discarded_batches}");
    println!("  Delivered committed prefix after first drop: {delivered_after_drop}");
    println!("  Expected data-loss outcomes observed: {matched_gaps} / {expected_gaps}");
    println!("{:=<152}\n", "");
}

fn write_results(config: &BenchConfig, capture: &CaptureResult, results: &[ReplayResult]) {
    let timestamp = unix_timestamp();
    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let best_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .fold(0.0, f64::max);
    let avg_ns_per_message = results
        .iter()
        .map(|result| result.ns_per_message)
        .sum::<f64>()
        / results.len() as f64;
    let max_queue_depth = results
        .iter()
        .map(|result| result.max_queue_depth)
        .max()
        .unwrap_or_default();
    let total_dropped_batches: u64 = results.iter().map(|result| result.dropped_batches).sum();
    let total_discarded_batches: usize =
        results.iter().map(|result| result.discarded_batches).sum();
    let total_delivered_after_drop: usize = results
        .iter()
        .map(|result| result.delivered_after_drop)
        .sum();
    let total_processed_messages: usize =
        results.iter().map(|result| result.processed_messages).sum();
    let total_accepted_rows: usize = results.iter().map(|result| result.accepted_rows).sum();
    let total_terminal_errors: usize = results
        .iter()
        .map(|result| result.terminal_error_count)
        .sum();
    let total_data_loss_errors: usize = results
        .iter()
        .map(|result| result.data_loss_error_count)
        .sum();
    let total_unexpected_errors: usize = results
        .iter()
        .map(|result| result.unexpected_error_count)
        .sum();
    let expected_data_loss_outcomes = results
        .iter()
        .filter(|result| result.expected_data_loss)
        .count();
    let matched_data_loss_outcomes = results
        .iter()
        .filter(|result| result.gap_outcome == "expected_data_loss_observed")
        .count();
    let input_descriptor = format!(
        "ticker={};fields={};captured_events={};captured_messages={};replay_loops={};compatibility_flush_threshold={};channel_capacity={};consumer_poll_messages={};consumer_batch={};consumer_delay_us={};cancel_after_messages={};drain_on_cancel={}",
        config.ticker,
        config.fields.join("|"),
        capture.events.len(),
        capture.messages,
        config.replay_loops,
        config.compatibility_flush_threshold,
        config.channel_capacity,
        config.consumer_poll_messages,
        config.consumer_batch,
        config.consumer_delay_us,
        config.cancel_after_messages,
        config.drain_on_cancel,
    );
    let provenance = xbbg_bench::benchmark_provenance_json(&input_descriptor);

    let mut json = String::new();
    writeln!(&mut json, "{{").unwrap();
    writeln!(&mut json, "  \"schema_version\": 3,").unwrap();
    writeln!(&mut json, "  \"timestamp\": {timestamp},").unwrap();
    writeln!(&mut json, "  \"crate\": \"xbbg-async\",").unwrap();
    writeln!(
        &mut json,
        "  \"benchmark_type\": \"cached_subscription_arrow\","
    )
    .unwrap();
    writeln!(&mut json, "  \"uses_bloomberg_session\": true,").unwrap();
    writeln!(
        &mut json,
        "  \"coverage\": \"one bounded live SDK capture followed by cached Event replay through real SubscriptionState and Arrow adaptation\","
    )
    .unwrap();
    writeln!(
        &mut json,
        "  \"output_model\": \"one sparse SubscriptionUpdate/RecordBatch per accepted source message, including the trailing presence bitmap; terminal data loss preserves the accepted prefix, then emits one classified error and EOF\","
    )
    .unwrap();
    writeln!(
        &mut json,
        "  \"timing_scope\": \"cached replay producer, configured consumer scheduling/delay, bounded channel, and Arrow adaptation; live capture excluded\","
    )
    .unwrap();
    writeln!(&mut json, "  \"provenance\": {provenance},").unwrap();
    writeln!(&mut json, "  \"config\": {{").unwrap();
    writeln!(
        &mut json,
        "    \"ticker\": \"{}\",",
        json_escape(&config.ticker)
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"fields\": [{}],",
        json_string_array(&config.fields)
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"capture_messages\": {},",
        config.capture_messages
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"capture_timeout_ms\": {},",
        config.capture_timeout_ms
    )
    .unwrap();
    writeln!(&mut json, "    \"replay_loops\": {},", config.replay_loops).unwrap();
    writeln!(
        &mut json,
        "    \"compatibility_flush_threshold\": {},",
        config.compatibility_flush_threshold
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"channel_capacity_batches\": {},",
        config.channel_capacity
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"consumer_poll_messages\": {},",
        config.consumer_poll_messages
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"consumer_batch\": {},",
        config.consumer_batch
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"consumer_delay_us\": {},",
        config.consumer_delay_us
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"cancel_after_messages\": {},",
        config.cancel_after_messages
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"drain_on_cancel\": {},",
        config.drain_on_cancel
    )
    .unwrap();
    writeln!(&mut json, "    \"iterations\": {},", config.iterations).unwrap();
    writeln!(
        &mut json,
        "    \"capture_all_fields\": {}",
        config.capture_all_fields
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"capture\": {{").unwrap();
    writeln!(&mut json, "    \"events\": {},", capture.events.len()).unwrap();
    writeln!(&mut json, "    \"messages\": {},", capture.messages).unwrap();
    writeln!(
        &mut json,
        "    \"elapsed_ms\": {}",
        capture.capture_elapsed_ms
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"summary\": {{").unwrap();
    writeln!(&mut json, "    \"sample_count\": {},", results.len()).unwrap();
    writeln!(
        &mut json,
        "    \"avg_rows_per_sec\": {:.2},",
        avg_rows_per_sec
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"best_rows_per_sec\": {:.2},",
        best_rows_per_sec
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"avg_ns_per_message\": {:.2},",
        avg_ns_per_message
    )
    .unwrap();
    writeln!(&mut json, "    \"max_queue_depth\": {max_queue_depth},").unwrap();
    writeln!(
        &mut json,
        "    \"processed_messages\": {total_processed_messages},"
    )
    .unwrap();
    writeln!(&mut json, "    \"accepted_rows\": {total_accepted_rows},").unwrap();
    writeln!(
        &mut json,
        "    \"terminal_errors\": {total_terminal_errors},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"data_loss_errors\": {total_data_loss_errors},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"unexpected_errors\": {total_unexpected_errors},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"expected_data_loss_outcomes\": {expected_data_loss_outcomes},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"matched_data_loss_outcomes\": {matched_data_loss_outcomes},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"dropped_batches\": {total_dropped_batches},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"delivered_after_drop\": {total_delivered_after_drop},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"discarded_batches\": {total_discarded_batches}"
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"iterations\": [").unwrap();

    for (idx, result) in results.iter().enumerate() {
        let comma = if idx + 1 == results.len() { "" } else { "," };
        writeln!(&mut json, "    {{").unwrap();
        writeln!(&mut json, "      \"iteration\": {},", result.iteration).unwrap();
        writeln!(
            &mut json,
            "      \"input_events\": {},",
            result.input_events
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"processed_messages\": {},",
            result.processed_messages
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"configured_replay_loops\": {},",
            result.configured_replay_loops
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"completed_replay_loops\": {},",
            result.completed_replay_loops
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"accepted_rows\": {},",
            result.accepted_rows
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"accepted_batches\": {},",
            result.accepted_batches
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"total_columns_emitted\": {},",
            result.total_columns_emitted
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"avg_columns_per_batch\": {:.3},",
            result.avg_columns_per_batch
        )
        .unwrap();
        writeln!(&mut json, "      \"elapsed_us\": {},", result.elapsed_us).unwrap();
        writeln!(
            &mut json,
            "      \"messages_per_sec\": {:.2},",
            result.messages_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"rows_per_sec\": {:.2},",
            result.rows_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"cells_per_sec\": {:.2},",
            result.cells_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"ns_per_message\": {:.2},",
            result.ns_per_message
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"channel_capacity_batches\": {},",
            result.channel_capacity
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"max_queue_depth\": {},",
            result.max_queue_depth
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"dropped_batches\": {},",
            result.dropped_batches
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"delivered_after_drop\": {},",
            result.delivered_after_drop
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"discarded_batches\": {},",
            result.discarded_batches
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"post_cancel_drained_batches\": {},",
            result.post_cancel_drained_batches
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"terminal_error_count\": {},",
            result.terminal_error_count
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"data_loss_error_count\": {},",
            result.data_loss_error_count
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"unexpected_error_count\": {},",
            result.unexpected_error_count
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"terminal_error_detail\": \"{}\",",
            json_escape(&result.terminal_error_detail)
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"terminal_eof_observed\": {},",
            result.terminal_eof_observed
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"expected_data_loss\": {},",
            result.expected_data_loss
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"gap_outcome\": \"{}\",",
            result.gap_outcome
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"stop_reason\": \"{}\",",
            result.stop_reason
        )
        .unwrap();
        writeln!(&mut json, "      \"status\": \"{}\",", result.status).unwrap();
        writeln!(&mut json, "      \"cancelled\": {},", result.cancelled).unwrap();
        writeln!(
            &mut json,
            "      \"drain_on_cancel\": {}",
            result.drain_on_cancel
        )
        .unwrap();
        writeln!(&mut json, "    }}{comma}").unwrap();
    }

    writeln!(&mut json, "  ]").unwrap();
    writeln!(&mut json, "}}").unwrap();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    let timestamped = dir.join(format!("cached_subscription_arrow_{timestamp}.json"));
    let latest = dir.join("cached_subscription_arrow_latest.json");
    write_json(&timestamped, &json);
    write_json(&latest, &json);
}

fn json_string_array(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => write!(&mut escaped, "\\u{:04x}", ch as u32).unwrap(),
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_secs()
}
