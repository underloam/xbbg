//! Live integration tests for xbbg-core.
//!
//! Tests needing a Bloomberg connection are ignored by default.
//!
//! Run live tests with: cargo test --package xbbg_core --test live -- --ignored --nocapture
//!
//! Environment variables:
//! - BLP_HOST: Bloomberg API host (default: 127.0.0.1)
//! - BLP_PORT: Bloomberg API port (default: 8194)

use std::time::{Duration, Instant};
use xbbg_core::{EventType, Name, Session, SessionOptions};

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a session with default options.
fn create_session() -> Session {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194);

    let mut opts = SessionOptions::new().expect("failed to create session options");
    opts.set_server_host(&host).expect("failed to set host");
    opts.set_server_port(port);
    opts.set_connect_timeout_ms(10_000)
        .expect("failed to set timeout");

    Session::new(&opts).expect("failed to create session")
}

/// Wait for session to start.
fn wait_for_session_started(sess: &Session, timeout_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    while Instant::now() < deadline {
        if let Some(ev) = sess.try_next_event() {
            if ev.event_type() == EventType::SessionStatus {
                let mut messages = ev.messages();
                while let Some(msg) = messages.next() {
                    let ty = msg.message_type();
                    let name = ty.as_str();
                    if name == "SessionStarted" {
                        return;
                    }
                }
            }
        } else {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    panic!("Session did not start within {}ms", timeout_ms);
}

// ============================================================================
// Basic Connectivity Tests
// ============================================================================

#[test]
fn live_builds() {
    // Verify crate compiles with live feature
    assert!(!xbbg_core::version().is_empty());
}

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_session_start_stop() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);
    sess.stop();
}

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_open_refdata_service() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open refdata service");
    let _svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get refdata service");

    sess.stop();
}

// ============================================================================
// Reference Data Tests
// ============================================================================

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_bdp_single_field() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get service");

    // Pre-intern names
    let securities = Name::get_or_intern("securities");
    let fields = Name::get_or_intern("fields");
    let security_data = Name::get_or_intern("securityData");
    let field_data = Name::get_or_intern("fieldData");
    let px_last = Name::get_or_intern("PX_LAST");

    // Create request
    let mut req = svc
        .create_request("ReferenceDataRequest")
        .expect("failed to create request");
    req.append_string(&securities, "IBM US Equity")
        .expect("failed to add security");
    req.append_string(&fields, "PX_LAST")
        .expect("failed to add field");

    // Send request
    sess.send_request(&req, None, None)
        .expect("failed to send request");

    // Get response
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got_response = false;

    while Instant::now() < deadline && !got_response {
        if let Ok(ev) = sess.next_event(Some(1000))
            && ev.event_type() == EventType::Response
        {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                println!("Message type: {}", msg.message_type().as_str());

                let root = msg.elements();
                if let Some(sd) = root.get(&security_data)
                    && let Some(first) = sd.get_element(0)
                    && let Some(fd) = first.get(&field_data)
                    && let Some(px) = fd.get(&px_last)
                    && let Some(value) = px.get_f64(0)
                {
                    println!("PX_LAST = {}", value);
                    assert!(value > 0.0, "PX_LAST should be positive");
                    got_response = true;
                }
            }
        }
    }

    assert!(got_response, "Did not receive response within timeout");
    sess.stop();
}

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_bdp_multiple_fields() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get service");

    // Pre-intern names
    let securities = Name::get_or_intern("securities");
    let fields = Name::get_or_intern("fields");
    let security_data = Name::get_or_intern("securityData");
    let field_data = Name::get_or_intern("fieldData");
    let px_last = Name::get_or_intern("PX_LAST");
    let name_field = Name::get_or_intern("NAME");

    // Create request with multiple fields
    let mut req = svc
        .create_request("ReferenceDataRequest")
        .expect("failed to create request");
    req.append_string(&securities, "AAPL US Equity")
        .expect("failed to add security");
    req.append_string(&fields, "PX_LAST")
        .expect("failed to add field");
    req.append_string(&fields, "NAME")
        .expect("failed to add field");

    // Send request
    sess.send_request(&req, None, None)
        .expect("failed to send request");

    // Get response
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got_px_last = false;
    let mut got_name = false;

    while Instant::now() < deadline && !(got_px_last && got_name) {
        if let Ok(ev) = sess.next_event(Some(1000))
            && ev.event_type() == EventType::Response
        {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                let root = msg.elements();
                if let Some(sd) = root.get(&security_data)
                    && let Some(first) = sd.get_element(0)
                    && let Some(fd) = first.get(&field_data)
                {
                    // Extract PX_LAST (numeric)
                    if let Some(px) = fd.get(&px_last)
                        && let Some(value) = px.get_f64(0)
                    {
                        println!("PX_LAST = {}", value);
                        got_px_last = true;
                    }

                    // Extract NAME (string)
                    if let Some(nm) = fd.get(&name_field)
                        && let Some(value) = nm.get_str(0)
                    {
                        println!("NAME = {}", value);
                        got_name = true;
                    }
                }
            }
        }
    }

    assert!(got_px_last, "Did not get PX_LAST");
    assert!(got_name, "Did not get NAME");
    sess.stop();
}

// ============================================================================
// Value Extraction Tests
// ============================================================================

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_get_value_dynamic_extraction() {
    use xbbg_core::Value;

    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get service");

    // Pre-intern names
    let securities = Name::get_or_intern("securities");
    let fields = Name::get_or_intern("fields");
    let security_data = Name::get_or_intern("securityData");
    let field_data = Name::get_or_intern("fieldData");
    let px_last = Name::get_or_intern("PX_LAST");

    // Create request
    let mut req = svc
        .create_request("ReferenceDataRequest")
        .expect("failed to create request");
    req.append_string(&securities, "IBM US Equity")
        .expect("failed to add security");
    req.append_string(&fields, "PX_LAST")
        .expect("failed to add field");

    // Send request
    sess.send_request(&req, None, None)
        .expect("failed to send request");

    // Get response and use get_value()
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got_response = false;

    while Instant::now() < deadline && !got_response {
        if let Ok(ev) = sess.next_event(Some(1000))
            && ev.event_type() == EventType::Response
        {
            let mut messages = ev.messages();
            while let Some(msg) = messages.next() {
                let root = msg.elements();
                if let Some(sd) = root.get(&security_data)
                    && let Some(first) = sd.get_element(0)
                    && let Some(fd) = first.get(&field_data)
                    && let Some(px) = fd.get(&px_last)
                {
                    // Use get_value() for dynamic extraction
                    if let Some(value) = px.get_value(0) {
                        println!("get_value() returned: {:?}", value);
                        match value {
                            Value::Float64(v) => {
                                println!("Extracted as Float64: {}", v);
                                assert!(v > 0.0);
                                got_response = true;
                            }
                            other => {
                                panic!("Expected Float64, got {:?}", other);
                            }
                        }
                    }
                }
            }
        }
    }

    assert!(got_response, "Did not receive response within timeout");
    sess.stop();
}

// ============================================================================
// Name Cache Tests
// ============================================================================

#[test]
fn live_name_cache_works() {
    // Clear cache first
    xbbg_core::clear_name_cache();
    assert_eq!(xbbg_core::name_cache_size(), 0);

    // Intern some names
    let _n1 = Name::get_or_intern("PX_LAST");
    let _n2 = Name::get_or_intern("PX_OPEN");
    let _n3 = Name::get_or_intern("PX_HIGH");

    assert_eq!(xbbg_core::name_cache_size(), 3);

    // Second call should use cache (same size)
    let _n1_again = Name::get_or_intern("PX_LAST");
    assert_eq!(xbbg_core::name_cache_size(), 3);

    // Clear and verify
    xbbg_core::clear_name_cache();
    assert_eq!(xbbg_core::name_cache_size(), 0);
}

// ============================================================================
// Schema Introspection Tests
// ============================================================================

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_schema_introspection_service() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open refdata service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get refdata service");

    // Test service metadata
    let name = svc.name();
    let description = svc.description();
    let num_ops = svc.num_operations();

    println!("Service: {}", name);
    println!("Description: {} (may be empty)", description);
    println!("Number of operations: {}", num_ops);

    assert_eq!(name, "//blp/refdata");
    // Note: description may be empty for some services
    assert!(num_ops > 0, "Service should have at least one operation");

    // Verify we can iterate operations
    let ops: Vec<_> = svc.operations().collect();
    assert_eq!(ops.len(), num_ops);

    sess.stop();
}

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_schema_introspection_operations() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open refdata service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get refdata service");

    // Find ReferenceDataRequest operation
    let mut found_refdata_request = false;

    for op in svc.operations() {
        let op_name = op.name();
        let op_desc = op.description();

        println!("Operation: {} - {}", op_name, op_desc);

        if op_name == "ReferenceDataRequest" {
            found_refdata_request = true;

            // Test request definition
            let req_def = op
                .request_definition()
                .expect("should have request definition");
            let req_type = req_def.type_definition();

            println!("  Request type: {}", req_type.name_str());
            println!("  Is complex: {}", req_type.is_complex_type());

            assert!(req_type.is_complex_type(), "Request should be complex type");

            // Iterate request elements
            let num_elements = req_type.num_element_definitions();
            println!("  Number of elements: {}", num_elements);
            assert!(num_elements > 0, "Request should have elements");

            for elem in req_type.element_definitions() {
                let elem_name = elem.name_str();
                let min_vals = elem.min_values();
                let max_vals = elem.max_values();
                let elem_type = elem.type_definition();

                println!(
                    "    - {} (min={}, max={}, type={})",
                    elem_name,
                    min_vals,
                    max_vals,
                    elem_type.name_str()
                );
            }

            // Test response definitions
            // Note: num_response_definitions may return 0 for some operations
            // where the response schema is not pre-defined
            let num_responses = op.num_response_definitions();
            println!("  Number of response types: {}", num_responses);

            for i in 0..num_responses {
                if let Ok(resp_def) = op.response_definition(i) {
                    println!("  Response[{}]: {}", i, resp_def.name_str());
                }
            }
        }
    }

    assert!(
        found_refdata_request,
        "Should find ReferenceDataRequest operation"
    );
    sess.stop();
}

#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_schema_introspection_element_details() {
    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    sess.open_service("//blp/refdata")
        .expect("failed to open refdata service");
    let svc = sess
        .get_service("//blp/refdata")
        .expect("failed to get refdata service");

    // Get ReferenceDataRequest operation
    let op = svc
        .get_operation_at(0)
        .expect("should have at least one operation");

    println!("First operation: {}", op.name());

    if let Ok(req_def) = op.request_definition() {
        let req_type = req_def.type_definition();

        // Test element details
        if let Some(first_elem) = req_type.element_definitions().next() {
            let name = first_elem.name_str();
            let desc = first_elem.description();
            let min = first_elem.min_values();
            let max = first_elem.max_values();
            let is_optional = first_elem.is_optional();
            let is_array = first_elem.is_array();

            println!("First element: {}", name);
            println!("  Description: {}", desc);
            println!("  Min values: {}", min);
            println!("  Max values: {}", max);
            println!("  Is optional: {}", is_optional);
            println!("  Is array: {}", is_array);

            // Get nested type info
            let elem_type = first_elem.type_definition();
            println!("  Type name: {}", elem_type.name_str());
            println!("  Is simple: {}", elem_type.is_simple_type());
            println!("  Is complex: {}", elem_type.is_complex_type());
            println!("  Is enumeration: {}", elem_type.is_enumeration_type());
            println!("  Datatype: {:?}", elem_type.datatype());
        }
    }

    sess.stop();
}

// ============================================================================
// Identity / Entitlement Probe (classic token flow)
// ============================================================================

/// Decisive probe for whether THIS endpoint supports the classic manual
/// authorization flow (generateToken -> //blp/apiauth AuthorizationRequest ->
/// sendAuthorizationRequest). Desktop API endpoints are expected to fail at
/// token generation; SAPI/B-PIPE succeeds and yields a real seat type and
/// entitlement answers. Prints every step — run with --nocapture.
#[test]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
fn live_probe_classic_token_authorization() {
    use xbbg_core::CorrelationId;

    let sess = create_session();
    sess.start().expect("failed to start session");
    wait_for_session_started(&sess, 5000);

    // Step 1: token generation for the logged-in user.
    let token_cid = CorrelationId::Int(7_777);
    sess.generate_token(Some(&token_cid))
        .expect("generateToken call should enqueue");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut token: Option<String> = None;
    let mut token_failure: Option<String> = None;
    while Instant::now() < deadline && token.is_none() && token_failure.is_none() {
        let Some(ev) = sess.try_next_event() else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            let ty = msg.message_type();
            let name = ty.as_str();
            println!("[token pump] event={:?} message={name}", ev.event_type());
            match name {
                "TokenGenerationSuccess" => {
                    token = msg
                        .elements()
                        .get_by_str("token")
                        .and_then(|e| e.get_str(0))
                        .map(str::to_string);
                    println!(
                        "[token pump] token generated ({} chars)",
                        token.as_deref().map_or(0, str::len)
                    );
                }
                "TokenGenerationFailure" => {
                    let reason = msg
                        .elements()
                        .get_by_str("reason")
                        .and_then(|r| r.get_by_str("description"))
                        .and_then(|e| e.get_str(0))
                        .map(str::to_string);
                    token_failure =
                        Some(reason.unwrap_or_else(|| "TokenGenerationFailure".to_string()));
                }
                _ => {}
            }
        }
    }

    let Some(token) = token else {
        println!(
            "=== PROBE RESULT: token generation unavailable on this endpoint: {} ===",
            token_failure.as_deref().unwrap_or("timed out")
        );
        sess.stop();
        return;
    };

    // Step 2: authorization request against //blp/apiauth.
    sess.open_service("//blp/apiauth")
        .expect("open //blp/apiauth");
    let auth_svc = sess.get_service("//blp/apiauth").expect("get apiauth");
    let mut auth_req = auth_svc
        .create_request("AuthorizationRequest")
        .expect("create AuthorizationRequest");
    auth_req.set_str("token", &token).expect("set token");

    let mut identity = sess.create_identity().expect("createIdentity");
    let auth_cid = CorrelationId::Int(7_778);
    sess.send_authorization_request(&auth_req, &mut identity, Some(&auth_cid))
        .expect("sendAuthorizationRequest");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut authorized: Option<bool> = None;
    while Instant::now() < deadline && authorized.is_none() {
        let Some(ev) = sess.try_next_event() else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            let ty = msg.message_type();
            let name = ty.as_str();
            println!("[auth pump] event={:?} message={name}", ev.event_type());
            match name {
                "AuthorizationSuccess" => authorized = Some(true),
                "AuthorizationFailure" => {
                    let reason = msg
                        .elements()
                        .get_by_str("reason")
                        .and_then(|r| r.get_by_str("description"))
                        .and_then(|e| e.get_str(0))
                        .map(str::to_string);
                    println!(
                        "[auth pump] failure detail: {}",
                        reason.as_deref().unwrap_or("(none)")
                    );
                    authorized = Some(false);
                }
                _ => {}
            }
        }
    }

    if authorized != Some(true) {
        println!("=== PROBE RESULT: authorization failed or timed out ===");
        sess.stop();
        return;
    }

    // Step 3: the identity is real — exercise seat type + entitlements.
    let seat = identity.seat_type().expect("seat_type");
    println!(
        "=== PROBE RESULT: AUTHORIZED; seat_type={} ===",
        seat.as_str()
    );

    sess.open_service("//blp/refdata").expect("open refdata");
    let refdata = sess.get_service("//blp/refdata").expect("get refdata");
    println!(
        "is_authorized(//blp/refdata) = {}",
        identity.is_authorized(&refdata)
    );
    let real = identity
        .check_entitlements(&refdata, &[14003, 14080])
        .expect("check_entitlements real");
    println!(
        "real EIDs entitled={} failed={:?}",
        real.entitled, real.failed_eids
    );
    let bogus = identity
        .check_entitlements(&refdata, &[999_999_999])
        .expect("check_entitlements bogus");
    println!(
        "bogus EID entitled={} failed={:?}",
        bogus.entitled, bogus.failed_eids
    );

    sess.stop();
}
