//! Raw response probe: send one minimal ReferenceDataRequest and dump every
//! event/message element tree verbatim. Use to diagnose terminal state
//! (logged out, data limits, entitlement failures) where data requests
//! "succeed" but carry responseError instead of securityData.
//!
//! Run with:
//!   cargo run -p xbbg_core --example raw_response_probe --no-default-features --features live

use xbbg_core::{DataType, Element, EventType, SessionOptions, session::Session};

fn dump_element(elem: &Element<'_>, indent: usize) {
    let pad = "  ".repeat(indent);
    let dt = elem.datatype();
    match dt {
        DataType::Sequence | DataType::Choice => {
            if elem.len() > 1 || (elem.len() == 1 && elem.num_children() == 0) {
                // Array of complex values.
                println!("{pad}{} ({:?})[{}]:", elem.name_str(), dt, elem.len());
                for child in elem.values() {
                    println!("{pad}  -");
                    for sub in child.children() {
                        dump_element(&sub, indent + 2);
                    }
                }
            } else {
                println!("{pad}{} ({:?}):", elem.name_str(), dt);
                for child in elem.children() {
                    dump_element(&child, indent + 1);
                }
            }
        }
        _ => {
            let values: Vec<String> = (0..elem.len().max(1))
                .map(|i| {
                    if elem.is_null() {
                        "<null>".to_string()
                    } else {
                        match elem.get_value(i) {
                            Some(value) => format!("{value:?}"),
                            None => "<unset>".to_string(),
                        }
                    }
                })
                .collect();
            println!(
                "{pad}{} ({:?}) = {}",
                elem.name_str(),
                dt,
                values.join(", ")
            );
        }
    }
}

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    let opts = SessionOptions::new()?;
    let sess = Session::new(&opts)?;
    sess.start_and_wait(10_000)?;
    println!("=== session started ===");

    sess.open_service("//blp/refdata")?;
    println!("=== service opened ===");

    let svc = sess.get_service("//blp/refdata")?;
    let mut req = svc.create_request("ReferenceDataRequest")?;
    req.append_str("securities", "IBM US Equity")?;
    req.append_str("fields", "PX_LAST")?;

    sess.send_request(&req, None, None)?;
    println!("=== request sent: ReferenceDataRequest IBM US Equity / PX_LAST ===");

    loop {
        let ev = sess.next_event(Some(15_000))?;
        let ev_type = ev.event_type();
        println!("--- event: {ev_type:?} ---");

        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            println!("message type: {}", msg.message_type().as_str());
            for elem in msg.elements().children() {
                dump_element(&elem, 1);
            }
        }

        if ev_type == EventType::Response || ev_type == EventType::Timeout {
            break;
        }
    }

    sess.stop();
    Ok(())
}
