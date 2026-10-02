//! Test IntradayTickRequest at xbbg-core level
//!
//! Run with: cargo run -p xbbg_core --example bdtick --features live

use xbbg_core::{session::Session, EventType, SessionOptions};

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    println!("=== xbbg-core IntradayTickRequest Test ===\n");

    // Create session
    let opts = SessionOptions::new()?;
    let sess = Session::new(&opts)?;
    sess.start()?;

    // Wait for session
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::SessionStatus {
            break;
        }
    }

    // Open refdata service
    sess.open_service("//blp/refdata")?;
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::ServiceStatus {
            break;
        }
    }

    // Create IntradayTickRequest
    let svc = sess.get_service("//blp/refdata")?;
    let mut req = svc.create_request("IntradayTickRequest")?;

    // Set parameters - use UTC times (14:30 UTC = 9:30 ET)
    req.set_str("security", "IBM US Equity")?;
    req.set_datetime("startDateTime", "2026-01-28T14:30:00")?;
    req.set_datetime("endDateTime", "2026-01-28T14:35:00")?; // Just 5 mins

    // THIS IS KEY - eventTypes is required!
    req.append_str("eventTypes", "TRADE")?;

    // Try to enable condition codes (set as string "true")
    let _ = req.set_str("includeConditionCodes", "true");
    let _ = req.set_str("includeExchangeCodes", "true");

    println!("Request built. Sending...\n");
    sess.send_request(&req, None, None)?;

    // Process response
    let mut tick_count = 0;
    loop {
        let ev = sess.next_event(Some(30000))?;
        let ev_type = ev.event_type();

        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            let root = msg.elements();

            // Print raw message structure for first message
            if tick_count == 0 {
                println!("=== Raw Message Structure ===");
                print_element(&root, 0);
                println!("\n=== End Raw Structure ===\n");
            }

            // Count ticks
            if let Some(tick_data_outer) = root.get_by_str("tickData") {
                if let Some(tick_data) = tick_data_outer.get_by_str("tickData") {
                    let n = tick_data.len();
                    let prev_count = tick_count;
                    tick_count += n;

                    // Print first few ticks with ALL fields
                    if prev_count == 0 {
                        for i in 0..std::cmp::min(n, 5) {
                            if let Some(tick) = tick_data.get_element(i) {
                                println!("  Tick {}:", i);

                                // Try known fields
                                for field in &[
                                    "time",
                                    "type",
                                    "value",
                                    "size",
                                    "conditionCodes",
                                    "cc",
                                    "exchangeCode",
                                    "tradeCondition",
                                ] {
                                    if let Some(f) = tick.get_by_str(field) {
                                        if let Some(s) = f.get_str(0) {
                                            println!("    {}: \"{}\"", field, s);
                                        } else if let Some(v) = f.get_f64(0) {
                                            println!("    {}: {}", field, v);
                                        } else if let Some(v) = f.get_i64(0) {
                                            println!("    {}: {}", field, v);
                                        } else {
                                            println!("    {}: <present but empty>", field);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if ev_type == EventType::Response {
            break;
        }
    }

    println!("\nTotal ticks received: {}", tick_count);
    sess.stop();
    Ok(())
}

fn print_element(elem: &xbbg_core::Element, indent: usize) {
    let prefix = "  ".repeat(indent);
    let name = elem.name().as_str().to_string();
    let n = elem.len();

    if n == 0 {
        // Leaf node - try to get value
        if let Some(v) = elem.get_str(0) {
            println!("{}{}: \"{}\"", prefix, name, v);
        } else if let Some(v) = elem.get_f64(0) {
            println!("{}{}: {}", prefix, name, v);
        } else if let Some(v) = elem.get_i64(0) {
            println!("{}{}: {}", prefix, name, v);
        } else {
            println!("{}{}: <empty>", prefix, name);
        }
    } else {
        println!("{}{} ({} children):", prefix, name, n);
        // Only print first few children to avoid overwhelming output
        for i in 0..std::cmp::min(n, 3) {
            if let Some(child) = elem.get_element(i) {
                print_element(&child, indent + 1);
            }
        }
        if n > 3 {
            println!("{}  ... and {} more", prefix, n - 3);
        }
    }
}
