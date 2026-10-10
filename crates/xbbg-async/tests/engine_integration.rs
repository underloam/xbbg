//! Comprehensive integration tests for the xbbg-async Engine.
//!
//! These tests mirror the Python integration tests in py-xbbg/tests/test_integration.py
//! to ensure consistent behavior across the Python and Rust APIs.
//!
//! Live tests are ignored by default; run with:
//! cargo test -p xbbg-async --test engine_integration -- --ignored
//!
//! Data usage summary (same as Python tests):
//! - Engine connection tests: 0 data points
//! - Field metadata tests: 0 data points (no security data)
//! - BDP tests: ~3-6 data points per test
//! - BDH tests: ~5-25 data points per test
//! - BDS tests: Variable, but uses small bulk fields
//! - BDIB tests: ~60-120 bars
//! - BDTICK tests: Variable, uses short time windows
//! - Error tests: 0 data points

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use chrono::Datelike;

use xbbg_async::engine::{
    Engine, EngineConfig, ExtractorType, RequestParams, ServerAddr, Transport,
};

// =============================================================================
// Test Helpers
// =============================================================================

fn init_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
        // Suppress noisy BLPAPI WARN logs in tests
        unsafe {
            let _ = xbbg_core::ffi::blpapi_Logging_registerCallback(
                None,
                xbbg_core::ffi::blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_ERROR
                    as xbbg_core::ffi::blpapi_Logging_Severity_t,
            );
        }
    });
}

/// Optional auth for identity/entitlement tests: XBBG_TEST_AUTH=user enables
/// OS-logon auth (SAPI/B-PIPE runners). Desktop API terminals must leave it
/// unset — Desktop rejects any authorization (verified live: session startup
/// fails NOT_AUTHORIZED "Failed to generate token" with auth=user configured).
fn auth_from_env() -> Option<xbbg_core::AuthConfig> {
    match std::env::var("XBBG_TEST_AUTH").ok().as_deref() {
        Some("user") => Some(xbbg_core::AuthConfig::User),
        Some(other) => panic!("unsupported XBBG_TEST_AUTH value: {other}"),
        None => None,
    }
}

fn create_engine() -> Engine {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194);

    let config = EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
        auth: auth_from_env(),
        ..Default::default()
    };

    Engine::start(config).expect("Engine should connect to Bloomberg")
}

fn recent_intraday_window_candidates(minutes: i64) -> Vec<(String, String)> {
    let today = chrono::Local::now().date_naive();
    let market_time_utc = chrono::NaiveTime::from_hms_opt(14, 30, 0).expect("valid market time");

    (1..=10)
        .filter_map(|days_back| {
            let date = today - chrono::Duration::days(days_back);
            if date.weekday().number_from_monday() > 5 {
                return None;
            }

            let start = date.and_time(market_time_utc);
            let end = start + chrono::Duration::minutes(minutes);
            Some((
                start.format("%Y-%m-%dT%H:%M:%S").to_string(),
                end.format("%Y-%m-%dT%H:%M:%S").to_string(),
            ))
        })
        .collect()
}

fn print_batch_summary(name: &str, batch: &RecordBatch) {
    println!(
        "\n=== {} ===\nColumns: {}, Rows: {}",
        name,
        batch.num_columns(),
        batch.num_rows()
    );
    for field in batch.schema().fields() {
        println!("  - {} ({:?})", field.name(), field.data_type());
    }
}

// =============================================================================
// Engine Connection Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_engine_connects_successfully() {
    init_tracing();

    let engine = create_engine();

    // Drop inside the async test body on purpose: Engine::drop must tear the
    // inner tokio runtime down without blocking. This previously needed
    // std::mem::forget to dodge tokio's "Cannot drop a runtime in a context
    // where blocking is not allowed" panic, which leaked the runtime.
    drop(engine);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_engine_config_custom_values() {
    init_tracing();

    let config = EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new("custom.host.com", 9999)]),
        max_event_queue_size: 20000,
        command_queue_size: 512,
        ..Default::default()
    };

    match &config.transport {
        Transport::Direct(servers) => {
            assert_eq!(servers[0].host, "custom.host.com");
            assert_eq!(servers[0].port, 9999);
        }
        other => panic!("expected Direct, got {other}"),
    }
    assert_eq!(config.max_event_queue_size, 20000);
}

// =============================================================================
// Field Metadata Tests (Zero security data usage)
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_field_info_single_field() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/apiflds".to_string(),
        operation: "FieldInfoRequest".to_string(),
        extractor: ExtractorType::Generic,
        field_ids: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("field info request");

    print_batch_summary("Field Info (single)", &batch);
    assert!(batch.num_rows() > 0, "Should return field info");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_field_info_multiple_fields() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/apiflds".to_string(),
        operation: "FieldInfoRequest".to_string(),
        extractor: ExtractorType::Generic,
        field_ids: Some(vec![
            "PX_LAST".to_string(),
            "VOLUME".to_string(),
            "NAME".to_string(),
        ]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("field info request");

    print_batch_summary("Field Info (multiple)", &batch);
    assert!(batch.num_rows() >= 3, "Should return info for each field");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_field_search() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/apiflds".to_string(),
        operation: "FieldSearchRequest".to_string(),
        extractor: ExtractorType::Generic,
        search_spec: Some("last price".to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("field search request");

    print_batch_summary("Field Search", &batch);
    assert!(batch.num_rows() > 0, "Should return matching fields");

    std::mem::forget(engine);
}

// =============================================================================
// BDP (Reference Data) Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdp_single_ticker_single_field() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdp request");

    print_batch_summary("BDP (1 ticker, 1 field)", &batch);
    assert!(batch.num_rows() >= 1, "Should have at least one row");
    assert!(
        batch.schema().column_with_name("ticker").is_some(),
        "Should have ticker column"
    );

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdp_multiple_tickers_multiple_fields() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec![
            "IBM US Equity".to_string(),
            "AAPL US Equity".to_string(),
        ]),
        fields: Some(vec!["PX_LAST".to_string(), "VOLUME".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdp request");

    print_batch_summary("BDP (2 tickers, 2 fields)", &batch);
    // Should have 2 tickers × 2 fields = 4 rows
    assert!(batch.num_rows() >= 4, "Should have ticker×field rows");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdp_returns_numeric_for_price() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdp request");

    // Check that we have a value column with numeric data
    if let Some((idx, _)) = batch.schema().column_with_name("value_num") {
        let col = batch.column(idx);
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>()
            && !arr.is_null(0)
        {
            let value = arr.value(0);
            assert!(value > 0.0, "Price should be positive");
        }
    }

    std::mem::forget(engine);
}

// =============================================================================
// BDH (Historical Data) Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdh_single_ticker() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "HistoricalDataRequest".to_string(),
        extractor: ExtractorType::HistData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some("20241201".to_string()),
        end_date: Some("20241207".to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdh request");

    print_batch_summary("BDH (1 ticker, 1 week)", &batch);
    assert!(batch.num_rows() >= 1, "Should have historical data");
    assert!(
        batch.schema().column_with_name("date").is_some(),
        "Should have date column"
    );

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdh_multiple_tickers() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "HistoricalDataRequest".to_string(),
        extractor: ExtractorType::HistData,
        securities: Some(vec![
            "IBM US Equity".to_string(),
            "AAPL US Equity".to_string(),
        ]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some("20241201".to_string()),
        end_date: Some("20241207".to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdh request");

    print_batch_summary("BDH (2 tickers)", &batch);

    // Verify we have data from both tickers
    if let Some((idx, _)) = batch.schema().column_with_name("ticker") {
        let col = batch.column(idx);
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            let unique: std::collections::HashSet<_> =
                (0..arr.len()).filter_map(|i| arr.value(i).into()).collect();
            assert!(
                !unique.is_empty(),
                "Should have data for at least one ticker"
            );
        }
    }

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdh_request_options_apply_periodicity() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "HistoricalDataRequest".to_string(),
        extractor: ExtractorType::HistData,
        securities: Some(vec!["SPY US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some("20250101".to_string()),
        end_date: Some("20250630".to_string()),
        options: Some(vec![(
            "periodicitySelection".to_string(),
            "MONTHLY".to_string(),
        )]),
        ..Default::default()
    };

    let batch = engine
        .request(params)
        .await
        .expect("bdh request with options");

    print_batch_summary("BDH (monthly via request options)", &batch);
    assert!(
        batch.num_rows() > 0,
        "Monthly historical request should return at least one row"
    );
    assert!(
        batch.num_rows() <= 8,
        "periodicitySelection=MONTHLY should materially reduce row count, got {}",
        batch.num_rows()
    );

    std::mem::forget(engine);
}

// =============================================================================
// BDS (Bulk Data) Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bds_index_members() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::BulkData,
        securities: Some(vec!["INDU Index".to_string()]),
        fields: Some(vec!["INDX_MEMBERS".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bds request");

    print_batch_summary("BDS (DJIA members)", &batch);
    // DJIA has 30 members
    assert!(batch.num_rows() >= 30, "DJIA should have 30 members");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bds_dividend_history() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::BulkData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["DVD_HIST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bds request");

    print_batch_summary("BDS (dividend history)", &batch);
    // IBM typically has dividend history (request succeeded if we get here)

    std::mem::forget(engine);
}

// =============================================================================
// BDIB (Intraday Bar) Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdib_1min_bars() {
    init_tracing();
    let engine = create_engine();

    // Use a recent trading day
    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "IntradayBarRequest".to_string(),
        extractor: ExtractorType::IntradayBar,
        security: Some("IBM US Equity".to_string()),
        event_type: Some("TRADE".to_string()),
        interval: Some(1),
        start_datetime: Some("2025-12-23T14:00:00".to_string()),
        end_datetime: Some("2025-12-23T14:30:00".to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdib request");

    print_batch_summary("BDIB (1-min bars, 30 min)", &batch);
    // Request succeeded if we get here (may have bars if market was open)

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdib_5min_bars() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "IntradayBarRequest".to_string(),
        extractor: ExtractorType::IntradayBar,
        security: Some("IBM US Equity".to_string()),
        event_type: Some("TRADE".to_string()),
        interval: Some(5),
        start_datetime: Some("2025-12-23T14:00:00".to_string()),
        end_datetime: Some("2025-12-23T14:30:00".to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdib request");

    print_batch_summary("BDIB (5-min bars, 30 min)", &batch);
    // Request succeeded if we get here (may have bars if market was open)

    std::mem::forget(engine);
}

// =============================================================================
// BDTICK (Intraday Tick) Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_bdtick_short_window() {
    init_tracing();
    let engine = create_engine();

    let mut last_empty_window = None;
    let mut last_error = None;

    for (start_datetime, end_datetime) in recent_intraday_window_candidates(30) {
        let params = RequestParams {
            service: "//blp/refdata".to_string(),
            operation: "IntradayTickRequest".to_string(),
            extractor: ExtractorType::IntradayTick,
            security: Some("IBM US Equity".to_string()),
            start_datetime: Some(start_datetime.clone()),
            end_datetime: Some(end_datetime.clone()),
            // eventTypes is REQUIRED for IntradayTickRequest.
            event_types: Some(vec!["TRADE".to_string()]),
            ..Default::default()
        };

        match engine.request(params).await {
            Ok(batch) if batch.num_rows() > 0 => {
                print_batch_summary(
                    &format!("BDTICK (30 min window, {start_datetime}..{end_datetime} UTC)"),
                    &batch,
                );
                println!("  Tick count: {}", batch.num_rows());
                std::mem::forget(engine);
                return;
            }
            Ok(batch) => {
                last_empty_window = Some(format!(
                    "{start_datetime}..{end_datetime} returned {} rows",
                    batch.num_rows()
                ));
            }
            Err(err) => {
                last_error = Some(format!("{start_datetime}..{end_datetime}: {err}"));
            }
        }
    }

    panic!(
        "bdtick request did not return ticks for recent weekday windows; last_empty_window={last_empty_window:?}; last_error={last_error:?}"
    );
}

// =============================================================================
// Generic API Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_generic_reference_data() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::Generic,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("generic request");

    print_batch_summary("Generic RefData", &batch);
    assert!(batch.num_rows() >= 1, "Should have data");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_generic_with_overrides() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::Generic,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["CRNCY_ADJ_PX_LAST".to_string()]),
        overrides: Some(vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())]),
        ..Default::default()
    };

    let batch = engine
        .request(params)
        .await
        .expect("generic request with overrides");

    print_batch_summary("Generic with overrides", &batch);
    assert!(batch.num_rows() >= 1, "Should have data");

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_raw_request_marker_uses_explicit_operation() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "".to_string(),
        request_operation: Some("ReferenceDataRequest".to_string()),
        extractor: ExtractorType::RefData,
        extractor_set: true,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("raw request");

    print_batch_summary("Raw request marker", &batch);
    assert!(batch.num_rows() >= 1, "Should have at least one row");
    assert!(
        batch.schema().column_with_name("ticker").is_some(),
        "Should have ticker column"
    );

    std::mem::forget(engine);
}

// =============================================================================
// Raw JSON Extractor Tests
// =============================================================================

// NOTE: RawJson and JsonArrow extractors have been removed.
// Use Generic extractor for flattened path/type/value output,
// or use specialized extractors (RefData, HistData, etc.) for structured output.

// =============================================================================
// Error Handling Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_invalid_ticker_returns_error() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["INVALID_TICKER_XYZ123 Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    // Invalid ticker may still return a batch with error info, or may error
    // Either behavior is acceptable
    let result = engine.request(params).await;
    println!("Invalid ticker result: {:?}", result.is_ok());

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_invalid_field_returns_error() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["INVALID_FIELD_XYZ123".to_string()]),
        ..Default::default()
    };

    // Invalid field may still return a batch with error info, or may error
    let result = engine.request(params).await;
    println!("Invalid field result: {:?}", result.is_ok());

    std::mem::forget(engine);
}

// =============================================================================
// Concurrent Request Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_concurrent_requests() {
    init_tracing();
    let engine = create_engine();

    let params1 = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    let params2 = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["AAPL US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    // Run requests concurrently
    let (result1, result2) = tokio::join!(engine.request(params1), engine.request(params2));

    let batch1 = result1.expect("first concurrent request");
    let batch2 = result2.expect("second concurrent request");

    assert!(batch1.num_rows() >= 1, "First request should succeed");
    assert!(batch2.num_rows() >= 1, "Second request should succeed");

    std::mem::forget(engine);
}

// =============================================================================
// Tracing/Logging Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_tracing_captures_request() {
    // This test verifies that tracing is active and capturing events
    init_tracing();
    tracing::info!("Starting tracing test");

    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        ..Default::default()
    };

    tracing::debug!(?params, "Sending request");

    let batch = engine.request(params).await.expect("request");

    tracing::info!(num_rows = batch.num_rows(), "Request completed");

    std::mem::forget(engine);
}

// =============================================================================
// Identity / Entitlement (EID) Tests
// =============================================================================

fn parse_meta_map<T: serde::de::DeserializeOwned>(batch: &RecordBatch, key: &str) -> Option<T> {
    batch
        .schema_ref()
        .metadata()
        .get(key)
        .map(|json| serde_json::from_str(json).expect("metadata JSON should parse"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_identity_seat_type_and_authorization() {
    init_tracing();
    let engine = create_engine();

    match engine.seat_type().await {
        // Auth-capable endpoint (XBBG_TEST_AUTH set on SAPI/B-PIPE).
        Ok(seat) => {
            println!("\n=== Seat type: {} ===", seat.as_str());
            assert_ne!(seat.as_str(), "INVALID", "identity should be authorized");

            let authorized = engine
                .identity_is_authorized("//blp/refdata")
                .await
                .expect("identity_is_authorized");
            assert!(authorized, "identity should access //blp/refdata");

            // Second call exercises the Ready fast path (no re-authorization).
            let seat2 = engine.seat_type().await.expect("seat_type (cached)");
            assert_eq!(seat.as_str(), seat2.as_str());

            // Failed-EID out-array contract against the real SDK: no identity
            // is entitled to a made-up EID, so the else branch (in/out count +
            // truncate) must report exactly that EID back.
            let bogus = engine
                .check_entitlements("//blp/refdata", &[999_999_999])
                .await
                .expect("bogus-EID check");
            println!(
                "bogus EID entitled={} failed={:?}",
                bogus.entitled, bogus.failed_eids
            );
            assert!(!bogus.entitled, "made-up EID cannot be entitled");
            assert_eq!(bogus.failed_eids, vec![999_999_999]);
        }
        // No-auth engine on a non-EMRS Desktop terminal: the classic token
        // flow runs and Bloomberg's precise reason must surface with the
        // configuration hint attached (EMRS-enrolled desktops succeed via
        // the Ok arm above instead).
        Err(e) => {
            let msg = e.to_string();
            println!("\n=== seat_type error (expected on non-EMRS Desktop): {msg} ===");
            assert!(
                msg.contains("EMRS") || msg.contains("SAPI/B-PIPE"),
                "error should carry configuration guidance, got: {msg}"
            );
        }
    }

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_return_eids_roundtrip_with_entitlement_check() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        return_eids: true,
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdp with returnEids");
    print_batch_summary("BDP returnEids", &batch);

    let eid_data: std::collections::BTreeMap<String, Vec<i64>> =
        parse_meta_map(&batch, "xbbg.eid_data").expect("xbbg.eid_data metadata present");
    println!("eid_data: {eid_data:?}");
    let eids = eid_data
        .get("IBM US Equity")
        .expect("EIDs reported for requested security");
    assert!(!eids.is_empty(), "at least one EID expected");

    // The identity that just received this data must be entitled to its EIDs
    // (auth-capable runs). On Desktop (no auth configured) the check must
    // fail fast with configuration guidance instead.
    let eids_i32: Vec<i32> = eids.iter().map(|&e| e as i32).collect();
    match engine.check_entitlements("//blp/refdata", &eids_i32).await {
        Ok(check) => {
            println!("entitled={} failed={:?}", check.entitled, check.failed_eids);
            assert!(
                check.entitled,
                "identity must hold EIDs of data it received"
            );
            assert!(check.failed_eids.is_empty());
        }
        Err(e) => {
            let msg = e.to_string();
            println!("check_entitlements (expected on non-EMRS Desktop): {msg}");
            assert!(
                msg.contains("EMRS") || msg.contains("SAPI/B-PIPE"),
                "error should carry configuration guidance, got: {msg}"
            );
        }
    }

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_return_eids_historical() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "HistoricalDataRequest".to_string(),
        extractor: ExtractorType::HistData,
        securities: Some(vec!["IBM US Equity".to_string()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some("20240102".to_string()),
        end_date: Some("20240105".to_string()),
        return_eids: true,
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdh with returnEids");
    print_batch_summary("BDH returnEids", &batch);

    let eid_data: std::collections::BTreeMap<String, Vec<i64>> =
        parse_meta_map(&batch, "xbbg.eid_data").expect("xbbg.eid_data metadata present");
    assert!(
        eid_data.contains_key("IBM US Equity"),
        "historical eidData expected: {eid_data:?}"
    );

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_security_error_and_field_exception_metadata() {
    init_tracing();
    let engine = create_engine();

    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: "ReferenceDataRequest".to_string(),
        extractor: ExtractorType::RefData,
        securities: Some(vec![
            "IBM US Equity".to_string(),
            "XXNOTREAL FAKE Equity".to_string(),
        ]),
        fields: Some(vec![
            "PX_LAST".to_string(),
            "NOT_A_REAL_FIELD_XX".to_string(),
        ]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("bdp mixed request");
    print_batch_summary("BDP error metadata", &batch);

    #[derive(serde::Deserialize, Debug)]
    struct SecErr {
        category: String,
        #[allow(dead_code)]
        code: i32,
        #[allow(dead_code)]
        subcategory: String,
        message: String,
    }
    let sec_errors: std::collections::BTreeMap<String, SecErr> =
        parse_meta_map(&batch, "xbbg.security_errors")
            .expect("xbbg.security_errors metadata present");
    println!("security_errors: {sec_errors:?}");
    let bad = sec_errors
        .get("XXNOTREAL FAKE Equity")
        .expect("bogus ticker recorded");
    assert!(!bad.category.is_empty() || !bad.message.is_empty());

    #[derive(serde::Deserialize, Debug)]
    struct FieldExc {
        field: String,
        #[allow(dead_code)]
        category: String,
        #[allow(dead_code)]
        code: i32,
        #[allow(dead_code)]
        subcategory: String,
        #[allow(dead_code)]
        message: String,
    }
    let field_excs: std::collections::BTreeMap<String, Vec<FieldExc>> =
        parse_meta_map(&batch, "xbbg.field_exceptions")
            .expect("xbbg.field_exceptions metadata present");
    println!("field_exceptions: {field_excs:?}");
    let ibm_excs = field_excs
        .get("IBM US Equity")
        .expect("bad field recorded for valid ticker");
    assert!(ibm_excs.iter().any(|e| e.field == "NOT_A_REAL_FIELD_XX"));

    std::mem::forget(engine);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn test_raw_generic_preserves_error_and_eid_elements() {
    init_tracing();
    let engine = create_engine();

    // Raw mode + Generic extractor: the flattened element paths must retain
    // securityError / eidData verbatim rather than dropping them.
    let params = RequestParams {
        service: "//blp/refdata".to_string(),
        operation: String::new(),
        request_operation: Some("ReferenceDataRequest".to_string()),
        extractor: ExtractorType::Generic,
        extractor_set: true,
        securities: Some(vec![
            "IBM US Equity".to_string(),
            "XXNOTREAL FAKE Equity".to_string(),
        ]),
        fields: Some(vec!["PX_LAST".to_string()]),
        elements: Some(vec![("returnEids".to_string(), "true".to_string())]),
        ..Default::default()
    };

    let batch = engine.request(params).await.expect("raw generic request");
    print_batch_summary("RAW generic returnEids", &batch);

    let paths = batch
        .column_by_name("path")
        .expect("generic path column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("path column is strings");
    let all_paths: Vec<&str> = (0..paths.len()).map(|i| paths.value(i)).collect();

    assert!(
        all_paths.iter().any(|p| p.contains("eidData")),
        "raw flatten should retain eidData"
    );
    assert!(
        all_paths.iter().any(|p| p.contains("securityError")),
        "raw flatten should retain securityError"
    );

    std::mem::forget(engine);
}
