//! Diagnostic probe: compare get_by_str vs get_at for TRADE_UPDATE_STAMP_RT
//!
//! Tests whether name-based lookup (`get_by_str`) returns the same result
//! as index-based iteration (`get_at`) for the same field in the same message.
//!
//! Run with:
//!   cargo run -p xbbg_core --example get_by_str_probe --no-default-features --features live

use xbbg_core::{
    session::Session, CorrelationId, EventType, Name, SessionOptions, SubscriptionList,
};

const FIELDS: &[&str] = &[
    "LAST_PRICE",
    "BID",
    "ASK",
    "VOLUME",
    "TRADE_UPDATE_STAMP_RT",
    "TRADING_DT_REALTIME",
    "LAST_TRADE_TIME_TODAY_RT",
];

const TOPICS: &[&str] = &["ES1 Index"];

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    eprintln!("=== get_by_str vs get_at Diagnostic Probe ===\n");

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

    // Pre-intern names (like the subscription code does via get_or_intern)
    let interned_names: Vec<Name> = FIELDS.iter().map(|f| Name::get_or_intern(f)).collect();

    // Subscribe
    let mut sub_list = SubscriptionList::new();
    for (i, topic) in TOPICS.iter().enumerate() {
        let cid = CorrelationId::Int(i as i64);
        sub_list.add(topic, FIELDS, "", &cid)?;
    }
    sess.subscribe(&sub_list, None)?;
    eprintln!("Subscribed. Waiting for data...\n");

    let start = std::time::Instant::now();
    let mut msg_count = 0u32;

    loop {
        let ev = sess.next_event(Some(3000))?;
        let et = ev.event_type();

        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            if et == EventType::SubscriptionData {
                msg_count += 1;
                let cid = msg.correlation_id(0);
                let elem = msg.elements();
                let n = elem.num_children();

                println!(
                    "--- msg #{} | cid={:?} | children={} ---",
                    msg_count, cid, n
                );

                // METHOD 1: get_by_str (what subscription code uses)
                println!("\n  METHOD 1: get_by_str()");
                for field_name in FIELDS {
                    let result = elem.get_by_str(field_name);
                    match result {
                        Some(child) => {
                            let dt = child.datatype();
                            let is_null = child.is_null();
                            let value = child.get_value(0);
                            println!(
                                "    {:30} => FOUND | type={:12?} | null={} | value={:?}",
                                field_name, dt, is_null, value
                            );
                        }
                        None => {
                            println!(
                                "    {:30} => NOT FOUND (get_by_str returned None)",
                                field_name
                            );
                        }
                    }
                }

                // METHOD 2: get(&Name) with pre-interned names
                println!("\n  METHOD 2: get(&Name) with pre-interned names");
                for (field_name, interned) in FIELDS.iter().zip(interned_names.iter()) {
                    let result = elem.get(interned);
                    match result {
                        Some(child) => {
                            let dt = child.datatype();
                            let is_null = child.is_null();
                            let value = child.get_value(0);
                            println!(
                                "    {:30} => FOUND | type={:12?} | null={} | value={:?}",
                                field_name, dt, is_null, value
                            );
                        }
                        None => {
                            println!(
                                "    {:30} => NOT FOUND (get(&Name) returned None)",
                                field_name
                            );
                        }
                    }
                }

                // METHOD 3: iterate ALL children by index (dump everything for INITPAINT)
                println!("\n  METHOD 3: iterate all {} children by index", n);
                let mut found_trade_stamp = false;
                for i in 0..n {
                    if let Some(child) = elem.get_at(i) {
                        let name = child.name();
                        let name_str = name.as_str();

                        if name_str == "TRADE_UPDATE_STAMP_RT" {
                            found_trade_stamp = true;
                            let dt = child.datatype();
                            let is_null = child.is_null();
                            let value = child.get_value(0);
                            println!("    [{:3}] {:30} => type={:12?} | null={} | value={:?}  *** TARGET ***",
                                i, name_str, dt, is_null, value);

                            // Cross-check: look it up by the exact name we got from the element
                            let lookup = elem.get(&name);
                            println!(
                                "          get(&name) with element's own Name => {:?}",
                                lookup.map(|e| format!("FOUND type={:?}", e.datatype()))
                            );
                        } else if n > 50 {
                            // INITPAINT: dump all children so we can inspect the full message
                            let dt = child.datatype();
                            let is_null = child.is_null();
                            let value = child.get_value(0);
                            println!(
                                "    [{:3}] {:30} => type={:12?} | null={} | value={:?}",
                                i, name_str, dt, is_null, value
                            );
                        }
                        // For small messages (quote updates), only print if it's the target field
                    }
                }
                if !found_trade_stamp {
                    println!("    TRADE_UPDATE_STAMP_RT not found in any child element");
                }

                println!();
            } else if et == EventType::SubscriptionStatus {
                eprintln!("[sub_status] {}", msg.message_type().as_str());
            }
        }

        if msg_count >= 30 {
            eprintln!("\nCollected {} messages, stopping.", msg_count);
            break;
        }

        if start.elapsed().as_secs() > 45 {
            eprintln!("\nTimeout after 45s with {} messages.", msg_count);
            break;
        }
    }

    sess.stop();
    Ok(())
}
