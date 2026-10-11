//! Reconnect behavior probe.
//!
//! Subscribes to a live ticker and logs every event with timestamp + message type
//! for 5 minutes. The goal is to observe Bloomberg SDK behavior across a network
//! disruption so we can decide how xbbg-async should handle SessionConnectionDown/Up.
//!
//! How to run:
//!   cargo run -p xbbg_core --example reconnect_probe --no-default-features --features live
//!
//! How to produce a transient disconnection (pick one):
//!   1. macOS host → block port 8194 for ~20s:
//!      sudo pfctl -E
//!      echo "block drop proto tcp from any to any port 8194" | sudo pfctl -f -
//!      # wait ~20s
//!      sudo pfctl -F rules
//!   2. Linux: sudo iptables -A OUTPUT -p tcp --dport 8194 -j DROP   (revert with -D)
//!   3. Yank the VM network cable / suspend the bridge for ~10-20s.
//!   4. Toggle Wi-Fi off/on.
//!
//! What we want to learn (with evidence):
//!   Q1: Does SessionConnectionDown fire? With a `reason` element?
//!   Q2: Does SessionConnectionUp fire when the network comes back?
//!   Q3: Do SubscriptionTerminated/Failure events fire for the active subs during the blip?
//!   Q4: After Up, do SubscriptionData events resume on their own WITHOUT any
//!       app-side resubscribe? (answers the central design question)
//!   Q5: If we don't get data after Up, what happens if we call subscribe() with
//!       the same correlation IDs? errors with CorrelationIdError, or succeeds?
//!
//! Env vars:
//!   BLP_HOST         default: localhost
//!   BLP_PORT         default: 8194
//!   PROBE_TOPIC      default: ES1 Index   (must be a live, streaming ticker)
//!   PROBE_DURATION   seconds, default: 300
//!   PROBE_RESUBSCRIBE_AFTER_UP  if "1", call subscribe() again with same CIDs
//!                                after the first SessionConnectionUp that follows
//!                                a SessionConnectionDown. Answers Q5.

use std::time::{Duration, Instant};

use xbbg_core::{CorrelationId, EventType, SessionOptions, SubscriptionList, session::Session};

const FIELDS: &[&str] = &["LAST_PRICE", "BID", "ASK"];

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

fn ts(start: Instant) -> String {
    format!("{:>7.3}s", start.elapsed().as_secs_f64())
}

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    let host = env_string("BLP_HOST", "localhost");
    let port: u16 = env_u64("BLP_PORT", 8194) as u16;
    let topic = env_string("PROBE_TOPIC", "ES1 Index");
    let duration_secs = env_u64("PROBE_DURATION", 300);
    let resubscribe_after_up = env_bool("PROBE_RESUBSCRIBE_AFTER_UP");

    eprintln!("=== Reconnect Behavior Probe ===");
    eprintln!("Host        : {}:{}", host, port);
    eprintln!("Topic       : {}", topic);
    eprintln!("Duration    : {}s", duration_secs);
    eprintln!("Fields      : {:?}", FIELDS);
    eprintln!(
        "Post-Up test: {}",
        if resubscribe_after_up {
            "will re-issue subscribe() with same CIDs after first Down/Up cycle (answers Q5)"
        } else {
            "passive observation only (answers Q4)"
        }
    );
    eprintln!(
        "Trigger a network blip at any time in the next {}s.",
        duration_secs
    );
    eprintln!();

    let mut opts = SessionOptions::new()?;
    opts.set_server_host(&host)?;
    opts.set_server_port(port);
    opts.set_auto_restart_on_disconnection(true);
    opts.set_num_start_attempts(3)?;
    opts.set_record_subscription_receive_times(true);

    let sess = Session::new(&opts)?;
    sess.start()?;

    let start = Instant::now();

    // Wait for SessionStarted (bounded).
    let session_start_deadline = Instant::now() + Duration::from_secs(10);
    'wait_started: loop {
        if Instant::now() > session_start_deadline {
            eprintln!("[{}] session did not start in 10s, bailing out", ts(start));
            return Ok(());
        }
        if let Ok(ev) = sess.next_event(Some(500))
            && ev.event_type() == EventType::SessionStatus
        {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                let mt = msg.message_type();
                eprintln!("[{}] session :: {}", ts(start), mt.as_str());
                if mt.as_str() == "SessionStarted" {
                    break 'wait_started;
                }
                if mt.as_str() == "SessionStartupFailure" || mt.as_str() == "SessionTerminated" {
                    eprintln!("[{}] session could not start, bailing out", ts(start));
                    return Ok(());
                }
            }
        }
    }

    // Open //blp/mktdata and wait for ServiceOpened.
    sess.open_service("//blp/mktdata")?;
    let service_deadline = Instant::now() + Duration::from_secs(10);
    'wait_svc: loop {
        if Instant::now() > service_deadline {
            eprintln!("[{}] service did not open in 10s, bailing out", ts(start));
            return Ok(());
        }
        if let Ok(ev) = sess.next_event(Some(500))
            && ev.event_type() == EventType::ServiceStatus
        {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                eprintln!("[{}] service :: {}", ts(start), msg.message_type().as_str());
                if msg.message_type().as_str() == "ServiceOpened" {
                    break 'wait_svc;
                }
            }
        }
    }

    // Subscribe. CorrelationId = 42 so it's easy to spot in output.
    let cid_value: i64 = 42;
    let mut sub_list = SubscriptionList::new();
    let cid = CorrelationId::Int(cid_value);
    sub_list.add(&topic, FIELDS, "", &cid)?;
    sess.subscribe(&sub_list, None)?;
    eprintln!("[{}] issued subscribe(cid={})", ts(start), cid_value);
    eprintln!();

    let end_at = Instant::now() + Duration::from_secs(duration_secs);
    let mut data_count = 0u64;
    let mut last_data_at = Instant::now();
    let mut seen_down = false;
    let mut seen_up_after_down = false;
    let mut resubscribed_already = false;

    // Silence-detector reporting cadence.
    let mut next_silence_report = Instant::now() + Duration::from_secs(15);

    while Instant::now() < end_at {
        let ev = match sess.next_event(Some(500)) {
            Ok(ev) => ev,
            Err(_) => {
                // Timeout — report silence if we haven't seen data in a while.
                if Instant::now() >= next_silence_report {
                    let since_data = last_data_at.elapsed().as_secs_f64();
                    eprintln!(
                        "[{}] (silence: {:.1}s since last SubscriptionData; data_count={}, \
                         seen_down={}, seen_up_after_down={})",
                        ts(start),
                        since_data,
                        data_count,
                        seen_down,
                        seen_up_after_down
                    );
                    next_silence_report = Instant::now() + Duration::from_secs(15);
                }
                continue;
            }
        };

        let et = ev.event_type();
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            let mt_name = msg.message_type();
            let mt = mt_name.as_str();

            match et {
                EventType::SubscriptionData => {
                    data_count += 1;
                    last_data_at = Instant::now();
                    if data_count <= 3 || data_count.is_power_of_two() {
                        eprintln!(
                            "[{}] DATA #{} :: cid={:?}",
                            ts(start),
                            data_count,
                            msg.correlation_id(0),
                        );
                    }
                }
                EventType::SessionStatus => {
                    let reason = extract_reason(&msg);
                    eprintln!(
                        "[{}] SESSION :: {} {}",
                        ts(start),
                        mt,
                        reason
                            .as_deref()
                            .map(|r| format!("reason=\"{}\"", r))
                            .unwrap_or_default(),
                    );
                    if mt == "SessionConnectionDown" {
                        seen_down = true;
                    }
                    if mt == "SessionConnectionUp" && seen_down && !seen_up_after_down {
                        seen_up_after_down = true;
                        eprintln!(
                            "[{}] >>> transition: Down -> Up observed. \
                             Watching for data to resume on its own (Q4)...",
                            ts(start)
                        );
                        if resubscribe_after_up && !resubscribed_already {
                            resubscribed_already = true;
                            eprintln!(
                                "[{}] >>> PROBE_RESUBSCRIBE_AFTER_UP=1 set. \
                                 Calling subscribe() again with SAME cid={} (Q5)...",
                                ts(start),
                                cid_value
                            );
                            let mut again = SubscriptionList::new();
                            let cid2 = CorrelationId::Int(cid_value);
                            again.add(&topic, FIELDS, "", &cid2)?;
                            match sess.subscribe(&again, Some("probe-reissue")) {
                                Ok(_) => eprintln!(
                                    "[{}] >>> subscribe() re-issue returned Ok",
                                    ts(start)
                                ),
                                Err(e) => eprintln!(
                                    "[{}] >>> subscribe() re-issue returned Err: {}",
                                    ts(start),
                                    e
                                ),
                            }
                        }
                    }
                    if mt == "SessionTerminated" {
                        eprintln!(
                            "[{}] >>> SessionTerminated — SDK gave up. Ending probe.",
                            ts(start)
                        );
                        return finalize(sess, start, data_count, seen_down, seen_up_after_down);
                    }
                }
                EventType::SubscriptionStatus => {
                    let reason = extract_reason(&msg);
                    eprintln!(
                        "[{}] SUB_STATUS :: {} cid={:?} {}",
                        ts(start),
                        mt,
                        msg.correlation_id(0),
                        reason
                            .as_deref()
                            .map(|r| format!("reason=\"{}\"", r))
                            .unwrap_or_default(),
                    );
                }
                EventType::ServiceStatus => {
                    eprintln!("[{}] SERVICE :: {}", ts(start), mt);
                }
                EventType::Admin => {
                    eprintln!("[{}] ADMIN :: {}", ts(start), mt);
                }
                _ => {
                    eprintln!("[{}] {:?} :: {}", ts(start), et, mt);
                }
            }
        }

        if Instant::now() >= next_silence_report {
            let since_data = last_data_at.elapsed().as_secs_f64();
            eprintln!(
                "[{}] heartbeat: data_count={}, since_last_data={:.1}s, \
                 seen_down={}, seen_up_after_down={}",
                ts(start),
                data_count,
                since_data,
                seen_down,
                seen_up_after_down
            );
            next_silence_report = Instant::now() + Duration::from_secs(15);
        }
    }

    finalize(sess, start, data_count, seen_down, seen_up_after_down)
}

#[allow(clippy::result_large_err)]
fn finalize(
    sess: Session,
    start: Instant,
    data_count: u64,
    seen_down: bool,
    seen_up_after_down: bool,
) -> xbbg_core::Result<()> {
    eprintln!();
    eprintln!("=== Summary ===");
    eprintln!(
        "Elapsed              : {:.1}s",
        start.elapsed().as_secs_f64()
    );
    eprintln!("Total SubscriptionData: {}", data_count);
    eprintln!("Saw SessionConnectionDown  : {}", seen_down);
    eprintln!("Saw SessionConnectionUp after Down: {}", seen_up_after_down);
    if seen_down && seen_up_after_down {
        eprintln!();
        eprintln!("Interpretation guide:");
        eprintln!(
            "  - If you saw SUB_STATUS SubscriptionStreamsDeactivated during the blip and \
             SubscriptionStreamsActivated after Up, followed by DATA resuming on its own, \
             the SDK is auto-recovering subs exactly as BLPAPI ChangeLog v3.11.6 describes."
        );
        eprintln!(
            "  - If data did NOT resume after Up -> app must resubscribe. \
             Re-run with PROBE_RESUBSCRIBE_AFTER_UP=1 to see whether subscribe() accepts \
             the same CID or returns CorrelationIdError."
        );
    } else if seen_down && !seen_up_after_down {
        eprintln!("No Up seen after Down — SDK may still be retrying, or gave up.");
    } else if !seen_down {
        eprintln!("No Down event observed — either no blip happened or SDK kept connection alive.");
    }
    sess.stop();
    Ok(())
}

fn extract_reason(msg: &xbbg_core::Message<'_>) -> Option<String> {
    let reason = msg.elements().get_by_str("reason")?;
    for key in ["description", "category", "message"] {
        if let Some(s) = reason.get_by_str(key).and_then(|e| e.get_str(0)) {
            return Some(s.to_string());
        }
    }
    None
}
