//! Live Bloomberg tests for the auction venue recipes.
//!
//! Enable with: cargo test -p xbbg-recipes --features live --test auction_live
//!
//! Data usage summary:
//! - Venue resolution: up to 3 securities x 10 routing fields, then 3 venues x 3 validation fields
//! - Auction snapshot: up to 1 security x 10 routing fields, then 1 venue x 11 fields (9 data + 2 validation)
//! - Optional preferred routing (only when `XBBG_LIVE_PFD_ISIN` is set): 1 security
//!
//! Assertions only rely on routing metadata and column types, so they hold during and
//! outside market hours.

#![cfg(feature = "live")]

use std::collections::HashMap;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, TimeUnit};
use xbbg_async::engine::{Engine, EngineConfig, ServerAddr, Transport};
use xbbg_recipes::{recipe_auction_snapshot, recipe_resolve_venues};

fn create_engine() -> Engine {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8194);
    let config = EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
        ..Default::default()
    };
    Engine::start(config)
        .unwrap_or_else(|_| panic!("Bloomberg connection failed (details redacted)"))
}

fn strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let array = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column {column} must be Utf8"));
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row).to_string()))
        .collect()
}

fn text(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
    strings(batch, column).swap_remove(row)
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_venues_routes_composites_and_isins_to_primary_listings() {
    let engine = create_engine();
    let securities = vec![
        "IBM US Equity".to_string(),
        "US0378331005".to_string(),
        "SPY UP Equity".to_string(),
    ];
    let batch = recipe_resolve_venues(&engine, securities.clone(), HashMap::new())
        .await
        .expect("venue resolution succeeds");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        strings(&batch, "security"),
        securities.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(
        strings(&batch, "status"),
        vec![Some("resolved".to_string()); 3],
        "errors: {:?}",
        strings(&batch, "error")
    );

    // Composite equity ticker -> primary exchange listing.
    assert_eq!(
        text(&batch, "venue_topic", 0).as_deref(),
        Some("IBM UN Equity")
    );
    assert_eq!(
        text(&batch, "method", 0).as_deref(),
        Some("exchange_ticker")
    );
    assert_eq!(text(&batch, "exch_code", 0).as_deref(), Some("UN"));
    assert_eq!(
        text(&batch, "composite", 0).as_deref(),
        Some("IBM US Equity")
    );
    assert!(text(&batch, "venue_figi", 0).is_some());

    // Bare equity ISIN -> composite lookup -> primary exchange listing.
    assert_eq!(
        text(&batch, "lookup", 1).as_deref(),
        Some("/isin/US0378331005")
    );
    assert_eq!(
        text(&batch, "venue_topic", 1).as_deref(),
        Some("AAPL UW Equity")
    );
    assert_eq!(
        text(&batch, "method", 1).as_deref(),
        Some("exchange_ticker")
    );

    // Explicit exchange ticker is respected.
    assert_eq!(
        text(&batch, "venue_topic", 2).as_deref(),
        Some("SPY UP Equity")
    );
    assert_eq!(text(&batch, "method", 2).as_deref(), Some("as_is"));
    assert_eq!(text(&batch, "pricing_source", 2).as_deref(), Some("UP"));
}

#[tokio::test(flavor = "multi_thread")]
async fn auction_snapshot_returns_validated_typed_columns() {
    let engine = create_engine();
    let fields = vec![
        "IMBALANCE_INDIC_RT".to_string(),
        "OFFICIAL_CLOSE_AUCTION_PRICE_RT".to_string(),
        "OFFICIAL_CLOSE_AUCTION_VOLUME_RT".to_string(),
        "IN_AUCTION_RT".to_string(),
        "AUCTION_TYPE_REALTIME".to_string(),
        "IMBALANCE_TIMESTAMP_RT".to_string(),
        "TIME_AUCTION_CALL_CONCLUSION_RT".to_string(),
        "CLOSING_AUCTION_VOLUME_DATE_RT".to_string(),
        "THEORETICAL_TIME_TODAY_RT".to_string(),
    ];
    let batch = recipe_auction_snapshot(
        &engine,
        vec!["IBM US Equity".to_string()],
        fields.clone(),
        HashMap::new(),
    )
    .await
    .expect("auction snapshot succeeds");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        text(&batch, "status", 0).as_deref(),
        Some("resolved"),
        "error: {:?}",
        text(&batch, "error", 0)
    );
    assert_eq!(
        text(&batch, "venue_topic", 0).as_deref(),
        Some("IBM UN Equity")
    );

    let schema = batch.schema();
    let column_type = |name: &str| {
        schema
            .field_with_name(name)
            .expect(name)
            .data_type()
            .clone()
    };
    assert_eq!(column_type("IMBALANCE_INDIC_RT"), DataType::Utf8);
    assert_eq!(
        column_type("OFFICIAL_CLOSE_AUCTION_PRICE_RT"),
        DataType::Float64
    );
    assert_eq!(
        column_type("OFFICIAL_CLOSE_AUCTION_VOLUME_RT"),
        DataType::Int64
    );
    assert_eq!(column_type("IN_AUCTION_RT"), DataType::Boolean);
    assert_eq!(column_type("AUCTION_TYPE_REALTIME"), DataType::Utf8);
    for field in [
        "IMBALANCE_TIMESTAMP_RT",
        "TIME_AUCTION_CALL_CONCLUSION_RT",
        "THEORETICAL_TIME_TODAY_RT",
    ] {
        assert_eq!(column_type(field), DataType::Time64(TimeUnit::Microsecond));
    }
    assert_eq!(
        column_type("CLOSING_AUCTION_VOLUME_DATE_RT"),
        DataType::Date32
    );
}

/// Preferred routing needs a real preferred ISIN, which this repository does not hard-code.
/// Set `XBBG_LIVE_PFD_ISIN` to a New York-listed preferred to run it.
#[tokio::test(flavor = "multi_thread")]
async fn preferred_isin_routes_through_venue_pricing_source() {
    let Ok(isin) = std::env::var("XBBG_LIVE_PFD_ISIN") else {
        eprintln!("skipping: XBBG_LIVE_PFD_ISIN is not set");
        return;
    };
    let engine = create_engine();
    let batch = recipe_resolve_venues(&engine, vec![isin.clone()], HashMap::new())
        .await
        .unwrap_or_else(|_| panic!("preferred venue resolution failed (details redacted)"));

    assert!(
        text(&batch, "kind", 0).as_deref() == Some("pfd"),
        "preferred input did not resolve to the expected market sector"
    );
    assert!(
        text(&batch, "status", 0).as_deref() == Some("resolved"),
        "preferred venue validation failed (details redacted)"
    );
    assert!(
        text(&batch, "method", 0).as_deref() == Some("pcs_suffix"),
        "preferred input did not use PCS-suffix routing"
    );
    let venue = text(&batch, "venue_topic", 0).expect("venue topic");
    assert!(
        venue.starts_with(&format!("/isin/{}@", isin.trim())),
        "preferred venue topic did not match the expected routing prefix (details redacted)"
    );
}
