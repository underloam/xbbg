//! Simple Bloomberg Data Point (BDP) example
//!
//! Run with: cargo run -p xbbg_core --example bdp --no-default-features --features live
//!
//! Requires Bloomberg Terminal or BPIPE connection.

use xbbg_core::{EventType, SessionOptions, session::Session};

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    // Create session options (default: localhost:8194)
    let opts = SessionOptions::new()?;

    // Create and start session
    let sess = Session::new(&opts)?;
    sess.start()?;

    // Wait for session to connect
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::SessionStatus {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                let msg_type = msg.message_type();
                if msg_type.as_str() == "SessionStarted"
                    || msg_type.as_str() == "SessionConnectionUp"
                {
                    break;
                }
            }
            break;
        }
    }

    // Open refdata service
    sess.open_service("//blp/refdata")?;

    // Wait for service to open
    loop {
        let ev = sess.next_event(Some(5000))?;
        if ev.event_type() == EventType::ServiceStatus {
            break;
        }
    }

    // Create request
    let svc = sess.get_service("//blp/refdata")?;
    let mut req = svc.create_request("ReferenceDataRequest")?;

    // Add securities and fields (using string-based API)
    req.append_str("securities", "IBM US Equity")?;
    req.append_str("securities", "AAPL US Equity")?;
    req.append_str("fields", "PX_LAST")?;
    req.append_str("fields", "SECURITY_NAME")?;

    // Send request
    sess.send_request(&req, None, None)?;

    // Process response
    loop {
        let ev = sess.next_event(Some(10000))?;
        let ev_type = ev.event_type();

        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            // Navigate to securityData array using string-based lookups
            if let Some(security_data) = msg.elements().get_by_str("securityData") {
                for i in 0..security_data.len() {
                    if let Some(sec) = security_data.get_element(i) {
                        // Get security ticker
                        let ticker = sec
                            .get_by_str("security")
                            .and_then(|e| e.get_str(0))
                            .unwrap_or("?");

                        // Get field data
                        if let Some(field_data) = sec.get_by_str("fieldData") {
                            let px_last = field_data
                                .get_by_str("PX_LAST")
                                .and_then(|e| e.get_f64(0))
                                .map(|v| format!("{:.2}", v))
                                .unwrap_or_else(|| "N/A".to_string());

                            let name = field_data
                                .get_by_str("SECURITY_NAME")
                                .and_then(|e| e.get_str(0))
                                .unwrap_or("N/A");

                            println!("{}: {} ({})", ticker, px_last, name);
                        }
                    }
                }
            }
        }

        if ev_type == EventType::Response {
            break;
        }
    }

    sess.stop();
    Ok(())
}
