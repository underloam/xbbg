//! Raw subscription probe — dumps every field's datatype + value from Bloomberg.
//!
//! This bypasses Arrow entirely and shows exactly what the SDK hands us.
//!
//! Run with:
//!   cargo run -p xbbg_core --example raw_sub_probe --no-default-features --features live
//!
//! Requires Bloomberg Terminal or BPIPE connection.

use std::time::Instant;
use xbbg_core::{session::Session, CorrelationId, EventType, SessionOptions, SubscriptionList};

/// Fields that mix types: float, int, string, datetime
const FIELDS: &[&str] = &[
    "LAST_PRICE",
    "BID",
    "ASK",
    "VOLUME",
    "NUM_TRADES_RT",
    "BID_SIZE",
    "ASK_SIZE",
    "TRADE_UPDATE_STAMP_RT",
    "EXCH_CODE_LAST",
    "TRADING_DT_REALTIME",
    "LAST_TRADE_TIME_TODAY_RT",
    "RT_PX_CHG_PCT_1D",
];

const TOPICS: &[&str] = &["ES1 Index", "UX1 Index", "NQ1 Index"];

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    eprintln!("=== Raw Subscription Probe ===");
    eprintln!("Topics: {:?}", TOPICS);
    eprintln!("Fields: {:?}", FIELDS);
    eprintln!();

    let mut opts = SessionOptions::new()?;
    opts.set_server_host("localhost")?;
    opts.set_server_port(8194);
    opts.set_record_subscription_receive_times(true);

    let sess = Session::new(&opts)?;
    sess.start()?;

    // Wait for session start
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::SessionStatus {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                let mt = msg.message_type();
                eprintln!("[session] {}", mt.as_str());
                if mt.as_str() == "SessionStarted" || mt.as_str() == "SessionConnectionUp" {
                    break;
                }
            }
            break;
        }
    }

    // Open mktdata service
    sess.open_service("//blp/mktdata")?;
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::ServiceStatus {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                eprintln!("[service] {}", msg.message_type().as_str());
            }
            break;
        }
    }

    // Subscribe
    let mut sub_list = SubscriptionList::new();
    for (i, topic) in TOPICS.iter().enumerate() {
        let cid = CorrelationId::Int(i as i64);
        sub_list.add(topic, FIELDS, "", &cid)?;
    }
    sess.subscribe(&sub_list, None)?;
    eprintln!("Subscribed. Waiting for data...\n");

    let start = Instant::now();
    let mut msg_count = 0u32;
    let max_messages = 30;

    loop {
        let ev = sess.next_event(Some(2000))?;
        let et = ev.event_type();

        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            match et {
                EventType::SubscriptionData => {
                    msg_count += 1;
                    let cid = msg.correlation_id(0);
                    let elem = msg.elements();

                    println!(
                        "--- msg #{} | cid={:?} | elapsed={:.1}s ---",
                        msg_count,
                        cid,
                        start.elapsed().as_secs_f64()
                    );

                    // Iterate ALL children of the root element
                    let n = elem.num_children();
                    println!("  num_children={}", n);

                    for i in 0..n {
                        if let Some(child) = elem.get_at(i) {
                            let name = child.name();
                            let dt = child.datatype();
                            let is_null = child.is_null();
                            let num_vals = child.len();

                            // Try every extraction method and report what works
                            let f64_val = child.get_f64(0);
                            let i64_val = child.get_i64(0);
                            let i32_val = child.get_i32(0);
                            let str_val = child.get_str(0);
                            let bool_val = child.get_bool(0);
                            let dt_val = child.get_datetime(0);
                            let dyn_val = child.get_value(0);

                            println!("  [{:2}] {:30} | type={:12?} | null={} | nvals={} | \
                                f64={:?} | i64={:?} | i32={:?} | str={:?} | bool={:?} | dt={:?} | value={:?}",
                                i,
                                name.as_str(),
                                dt,
                                is_null,
                                num_vals,
                                f64_val,
                                i64_val,
                                i32_val,
                                str_val,
                                bool_val,
                                dt_val.as_ref().map(|d| format!("{:?}", d)),
                                dyn_val,
                            );
                        }
                    }
                    println!();
                }
                EventType::SubscriptionStatus => {
                    let mt = msg.message_type();
                    eprintln!(
                        "[sub_status] {} | cid={:?}",
                        mt.as_str(),
                        msg.correlation_id(0)
                    );
                }
                EventType::SessionStatus => {
                    eprintln!("[session] {}", msg.message_type().as_str());
                }
                _ => {}
            }
        }

        if msg_count >= max_messages {
            eprintln!("\nCollected {} messages, stopping.", max_messages);
            break;
        }

        if start.elapsed().as_secs() > 30 {
            eprintln!("\nTimeout after 30s with {} messages.", msg_count);
            break;
        }
    }

    sess.stop();
    Ok(())
}
