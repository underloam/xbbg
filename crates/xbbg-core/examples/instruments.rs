//! Raw Bloomberg instruments service test — dumps element trees
//!
//! Run with: cargo run -p xbbg_core --example instruments --no-default-features --features live

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
                for i in 0..elem.len() {
                    if let Some(item) = elem.get_element(i) {
                        println!("{}  [{}]:", pad, i);
                        dump_element(&item, indent + 4);
                    }
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

fn send_and_dump(sess: &Session, req: &xbbg_core::Request) {
    sess.send_request(req, None, None).unwrap();
    loop {
        let ev = sess.next_event(Some(10000)).unwrap();
        let ev_type = ev.event_type();
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            println!("msg_type: {}", msg.message_type().as_str());
            let root = msg.elements();
            if let Some(results) = root.get_by_str("results") {
                println!(
                    "  results: len={} is_array={}",
                    results.len(),
                    results.is_array()
                );
                for i in 0..std::cmp::min(results.len(), 3) {
                    if let Some(item) = results.get_element(i) {
                        println!("  [{}]:", i);
                        dump_element(&item, 4);
                    }
                }
            } else {
                dump_element(&root, 0);
            }
        }
        if ev_type == EventType::Response {
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
    sess.open_service("//blp/instruments")?;
    wait_service(&sess);
    let svc = sess.get_service("//blp/instruments")?;

    // govtListRequest with various tickers
    for ticker in &["GT2", "GT10", "T", "T 4", "US91282CGH88", "CT2", "GT2 Govt"] {
        println!("\n===== govtListRequest ticker={} =====", ticker);
        let mut req = svc.create_request("govtListRequest")?;
        req.set_str("ticker", ticker)?;
        let _ = req.set_str("partialMatch", "true");
        send_and_dump(&sess, &req);
    }

    // govtListRequest with query element
    for query in &["treasury", "bond", "GT2 Govt", "US Treasury", "T 4.5"] {
        println!("\n===== govtListRequest query={} =====", query);
        let mut req = svc.create_request("govtListRequest")?;
        req.set_str("query", query)?;
        send_and_dump(&sess, &req);
    }

    // curveListRequest variants
    for (k, v) in &[
        ("query", "swap"),
        ("query", "USD"),
        ("currencyCode", "USD"),
        ("type", "GOVT"),
    ] {
        println!("\n===== curveListRequest {}={} =====", k, v);
        let mut req = svc.create_request("curveListRequest")?;
        req.set_str(k, v)?;
        send_and_dump(&sess, &req);
    }

    sess.stop();
    Ok(())
}
