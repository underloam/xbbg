//! Raw Bloomberg TA study request — tests //blp/tasvc directly
//!
//! Run with: cargo run -p xbbg_core --example tasvc --no-default-features --features live

use xbbg_core::{DataType, Element, EventType, SessionOptions, session::Session};

fn dump_element(elem: &Element, indent: usize) {
    let pad = " ".repeat(indent);
    let name = elem.name();
    let dt = elem.datatype();
    println!(
        "{}{}: dt={:?} is_array={} len={} num_children={}",
        pad,
        name.as_str(),
        dt,
        elem.is_array(),
        elem.len(),
        elem.num_children(),
    );
    match dt {
        DataType::Sequence => {
            if elem.is_array() {
                for i in 0..std::cmp::min(elem.len(), 5) {
                    if let Some(item) = elem.get_element(i) {
                        println!("{}  [{}]:", pad, i);
                        dump_element(&item, indent + 4);
                    }
                }
                if elem.len() > 5 {
                    println!("{}  ... ({} more)", pad, elem.len() - 5);
                }
            } else {
                for child in elem.children() {
                    dump_element(&child, indent + 2);
                }
            }
        }
        DataType::Choice => {
            for child in elem.children() {
                dump_element(&child, indent + 2);
            }
        }
        _ => {
            if let Some(val) = elem.get_value(0) {
                println!("{}  = {:?}", pad, val);
            }
        }
    }
}

fn wait_session(sess: &Session) {
    loop {
        if sess.next_event(Some(5000)).unwrap().event_type() == EventType::SessionStatus {
            break;
        }
    }
}

fn wait_service(sess: &Session) {
    loop {
        if sess.next_event(Some(5000)).unwrap().event_type() == EventType::ServiceStatus {
            break;
        }
    }
}

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    let opts = SessionOptions::new()?;
    let sess = Session::new(&opts)?;
    sess.start()?;
    wait_session(&sess);
    sess.open_service("//blp/tasvc")?;
    wait_service(&sess);
    let svc = sess.get_service("//blp/tasvc")?;

    // SMA study on ES1 Index
    println!("\n===== studyRequest: SMA on ES1 Index =====");
    let mut req = svc.create_request("studyRequest")?;

    // Set nested elements using dotted paths
    req.set_nested_str("priceSource.securityName", "ES1 Index")?;
    req.set_nested_str("priceSource.dataRange.historical.startDate", "20260117")?;
    req.set_nested_str("priceSource.dataRange.historical.endDate", "20260216")?;
    req.set_nested_str(
        "priceSource.dataRange.historical.periodicitySelection",
        "DAILY",
    )?;
    req.set_nested_int("studyAttributes.smavgStudyAttributes.period", 20)?;
    req.set_nested_str(
        "studyAttributes.smavgStudyAttributes.priceSourceClose",
        "PX_LAST",
    )?;

    // Print the request element tree before sending
    println!("--- Request elements ---");
    dump_element(&req.elements(), 0);
    println!("--- Sending ---");

    sess.send_request(&req, None, None)?;
    loop {
        let ev = sess.next_event(Some(15000)).unwrap();
        let ev_type = ev.event_type();
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            println!("msg_type: {}", msg.message_type().as_str());
            dump_element(&msg.elements(), 0);
        }
        if ev_type == EventType::Response {
            break;
        }
    }

    // Also try RSI
    println!("\n===== studyRequest: RSI on ES1 Index =====");
    let mut req2 = svc.create_request("studyRequest")?;
    req2.set_nested_str("priceSource.securityName", "ES1 Index")?;
    req2.set_nested_str("priceSource.dataRange.historical.startDate", "20260117")?;
    req2.set_nested_str("priceSource.dataRange.historical.endDate", "20260216")?;
    req2.set_nested_str(
        "priceSource.dataRange.historical.periodicitySelection",
        "DAILY",
    )?;
    req2.set_nested_int("studyAttributes.rsiStudyAttributes.period", 14)?;
    req2.set_nested_str(
        "studyAttributes.rsiStudyAttributes.priceSourceClose",
        "PX_LAST",
    )?;

    println!("--- Request elements ---");
    dump_element(&req2.elements(), 0);
    println!("--- Sending ---");

    sess.send_request(&req2, None, None)?;
    loop {
        let ev = sess.next_event(Some(15000)).unwrap();
        let ev_type = ev.event_type();
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            println!("msg_type: {}", msg.message_type().as_str());
            dump_element(&msg.elements(), 0);
        }
        if ev_type == EventType::Response {
            break;
        }
    }

    println!("\nDone.");
    Ok(())
}
