//! Schema introspection test for IntradayTickRequest
//!
//! Run with: cargo run -p xbbg_core --example schema_tick --features live

use xbbg_core::{EventType, SessionOptions, session::Session};

#[allow(clippy::result_large_err)]
fn main() -> xbbg_core::Result<()> {
    println!("=== Schema Introspection for IntradayTickRequest ===\n");

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

    let svc = sess.get_service("//blp/refdata")?;

    println!("Service has {} operations\n", svc.num_operations());

    // Find IntradayTickRequest operation
    for op in svc.operations() {
        let name = op.name();
        if name == "IntradayTickRequest" {
            println!("Found: {}", name);
            println!("Description: {}", op.description());

            // Get request schema
            if let Ok(req_def) = op.request_definition() {
                println!("\nRequest Schema Elements:");
                let type_def = req_def.type_definition();
                print_type_def(&type_def, 1);
            }

            // Get response schemas
            println!(
                "\nResponse Schemas ({} types):",
                op.num_response_definitions()
            );
            for i in 0..op.num_response_definitions() {
                if let Ok(resp_def) = op.response_definition(i) {
                    println!("  Response {}:", i);
                    let type_def = resp_def.type_definition();
                    print_type_def(&type_def, 2);
                }
            }
            break;
        }
    }

    sess.stop();
    Ok(())
}

fn print_type_def(type_def: &xbbg_core::schema::SchemaTypeDefinition, indent: usize) {
    let prefix = "  ".repeat(indent);

    println!(
        "{}Type: {} ({} elements)",
        prefix,
        type_def.name_str(),
        type_def.num_element_definitions()
    );

    for elem_def in type_def.element_definitions() {
        let elem_name = elem_def.name_str();
        let status = elem_def.status();
        let inner_type = elem_def.type_definition();
        let type_name = inner_type.name_str();
        let required = if elem_def.is_required() {
            "required"
        } else {
            "optional"
        };
        let array = if elem_def.is_array() { "[]" } else { "" };

        println!(
            "{}  - {}{} : {} ({}, {:?})",
            prefix, elem_name, array, type_name, required, status
        );
    }
}
