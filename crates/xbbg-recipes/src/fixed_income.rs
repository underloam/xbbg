//! Fixed income recipe functions.
//!
//! High-level recipes for Bloomberg fixed income data queries.

use arrow_array::RecordBatch;
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::bql::{build_corporate_bonds_query, build_preferreds_query};
use xbbg_ext::transforms::fixed_income::{build_yas_overrides, YieldType};

use crate::error::Result;

/// YAS (Yield & Spread Analysis) recipe.
///
/// Retrieves Bloomberg YAS data with optional yield type and pricing parameters.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `tickers` - Securities to query
/// * `fields` - Fields to retrieve
/// * `settle_dt` - Settlement date (YYYYMMDD format)
/// * `yield_type` - Yield calculation type (YTM, YTC, etc.)
/// * `spread` - Yield spread override
/// * `yield_val` - Yield value override
/// * `price` - Price override
/// * `benchmark` - Benchmark security for spread calculation
///
/// # Returns
///
/// Arrow RecordBatch with YAS data in canonical long format
///
/// # Example
///
/// ```ignore
/// let batch = recipe_yas(
///     &engine,
///     vec!["US912810SV17 Govt".to_string()],
///     vec!["YAS_BOND_YLD".to_string(), "YAS_YLD_SPREAD".to_string()],
///     Some("20240115".to_string()),
///     Some(YieldType::YTM),
///     None,
///     None,
///     Some(99.5),
///     None,
/// ).await?;
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn recipe_yas(
    engine: &Engine,
    tickers: Vec<String>,
    fields: Vec<String>,
    settle_dt: Option<String>,
    yield_type: Option<YieldType>,
    spread: Option<f64>,
    yield_val: Option<f64>,
    price: Option<f64>,
    benchmark: Option<String>,
) -> Result<RecordBatch> {
    // Build YAS overrides using xbbg-ext helper
    let overrides = build_yas_overrides(
        settle_dt.as_deref(),
        yield_type,
        spread,
        yield_val,
        price,
        benchmark.as_deref(),
    );

    // Build request parameters using canonical enums
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(tickers),
        fields: Some(fields),
        overrides: Some(overrides),
        ..Default::default()
    };

    // Call engine directly (no recursion)
    let batch = engine.request(params).await?;
    Ok(batch)
}

/// Find preferred stocks for a company via BQL.
///
/// Uses Bloomberg's debt filter to find preferred stock issues
/// associated with a given equity ticker.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `equity_ticker` - Company equity ticker (e.g., "BAC US Equity")
/// * `fields` - Fields to retrieve (default: id, name)
///
/// # Returns
///
/// Arrow RecordBatch with preferred stock data
pub async fn recipe_preferreds(
    engine: &Engine,
    equity_ticker: String,
    fields: Option<Vec<String>>,
) -> Result<RecordBatch> {
    // Query construction lives in xbbg-ext (single source of truth). The
    // builder appends " US Equity" to bare tickers and dedupes extra fields
    // against the defaults (id, name).
    let extra = fields.unwrap_or_default();
    let extra_refs: Vec<&str> = extra.iter().map(String::as_str).collect();
    let bql_query = build_preferreds_query(&equity_ticker, &extra_refs);

    let params = RequestParams {
        service: Service::BqlSvc.to_string(),
        operation: Operation::BqlSendQuery.to_string(),
        elements: Some(vec![("expression".to_string(), bql_query)]),
        ..Default::default()
    };

    engine.request(params).await.map_err(Into::into)
}

/// Find corporate bonds for a company via BQL.
///
/// Uses Bloomberg's `debt()` universe to find corporate bond issues
/// for a given company via its equity ticker. Works across all markets.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `ticker` - Company equity ticker (e.g., "AAPL", "9984 JT Equity").
///   If no suffix is provided, " US Equity" is appended.
/// * `ccy` - Currency filter (e.g., "USD"). None for all currencies.
/// * `fields` - Fields to retrieve (default: id)
///
/// # Returns
///
/// Arrow RecordBatch with corporate bond data
pub async fn recipe_corporate_bonds(
    engine: &Engine,
    ticker: String,
    ccy: Option<String>,
    fields: Option<Vec<String>>,
) -> Result<RecordBatch> {
    // Query construction lives in xbbg-ext (single source of truth). The
    // builder normalizes bare tickers, dedupes extra fields against the
    // default (id), and applies the optional CRNCY filter.
    let extra = fields.unwrap_or_default();
    let extra_refs: Vec<&str> = extra.iter().map(String::as_str).collect();
    let bql_query = build_corporate_bonds_query(&ticker, ccy.as_deref(), &extra_refs);

    let params = RequestParams {
        service: Service::BqlSvc.to_string(),
        operation: Operation::BqlSendQuery.to_string(),
        elements: Some(vec![("expression".to_string(), bql_query)]),
        ..Default::default()
    };

    engine.request(params).await.map_err(Into::into)
}

/// Bloomberg Quote Request — dealer quotes via IntradayTick.
///
/// Retrieves intraday tick data with broker/dealer codes for a security.
/// Useful for analyzing dealer activity and market making.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `ticker` - Security ticker (e.g., "US912810TM69 Govt")
/// * `start_datetime` - Start datetime (ISO format)
/// * `end_datetime` - End datetime (ISO format)
/// * `event_types` - Event types to retrieve (default: BID, ASK)
/// * `include_broker_codes` - Include broker/dealer codes (default: true)
///
/// # Returns
///
/// Arrow RecordBatch with quote data including broker codes
pub async fn recipe_bqr(
    engine: &Engine,
    ticker: String,
    start_datetime: String,
    end_datetime: String,
    event_types: Option<Vec<String>>,
    include_broker_codes: bool,
) -> Result<RecordBatch> {
    let params = bqr_request(
        ticker,
        start_datetime,
        end_datetime,
        event_types,
        include_broker_codes,
    );
    engine.request(params).await.map_err(Into::into)
}

fn bqr_request(
    ticker: String,
    start_datetime: String,
    end_datetime: String,
    event_types: Option<Vec<String>>,
    include_broker_codes: bool,
) -> RequestParams {
    let evts = event_types.unwrap_or_else(|| vec!["BID".to_string(), "ASK".to_string()]);

    let mut options = vec![];
    if include_broker_codes {
        options.push(("includeBrokerCodes".to_string(), "true".to_string()));
    }

    RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::IntradayTick.to_string(),
        security: Some(ticker),
        start_datetime: Some(start_datetime),
        end_datetime: Some(end_datetime),
        event_types: Some(evts),
        options: if options.is_empty() {
            None
        } else {
            Some(options)
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recipe_yas_builds_correct_params() {
        // This test verifies parameter building logic
        // Actual engine calls require Bloomberg connection (integration test)

        let overrides = build_yas_overrides(
            Some("20240115"),
            Some(YieldType::YTM),
            None,
            None,
            Some(99.5),
            None,
        );

        // Verify YAS overrides are built correctly
        assert!(overrides
            .iter()
            .any(|(k, v)| k == "YAS_SETTLE_DT" && v == "20240115"));
        assert!(overrides
            .iter()
            .any(|(k, v)| k == "YAS_YLD_FLAG" && v == "1"));
        assert!(overrides
            .iter()
            .any(|(k, v)| k == "YAS_BOND_PX" && v == "99.5"));
    }

    #[test]
    fn test_recipe_preferreds_default_fields() {
        assert_eq!(
            build_preferreds_query("BAC", &[]),
            "get(id, name) for(filter(debt(['BAC US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Preferreds'))"
        );
    }

    #[test]
    fn test_recipe_preferreds_custom_fields() {
        let query = build_preferreds_query("BAC US Equity", &["ID", "px_last", "dvd_yld"]);
        assert!(query.starts_with("get(id, name, px_last, dvd_yld)"));
        assert!(query.contains("debt(['BAC US Equity']"));
    }

    #[test]
    fn test_recipe_corporate_bonds_filter_building() {
        assert_eq!(
            build_corporate_bonds_query("AAPL", Some("USD"), &["ID", "cpn"]),
            "get(id, cpn) for(filter(debt(['AAPL US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Corporates' AND CRNCY=='USD'))"
        );
    }

    #[test]
    fn test_recipe_corporate_bonds_no_ccy() {
        assert_eq!(
            build_corporate_bonds_query("9984 JT Equity", None, &[]),
            "get(id) for(filter(debt(['9984 JT Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Corporates'))"
        );
    }

    #[test]
    fn test_recipe_bqr_default_request() {
        let params = bqr_request(
            "US912810TM69 Govt".to_string(),
            "2024-01-15T09:00:00".to_string(),
            "2024-01-15T10:00:00".to_string(),
            None,
            true,
        );
        assert_eq!(params.service, Service::RefData.as_str());
        assert_eq!(params.operation, Operation::IntradayTick.as_str());
        assert_eq!(params.security.as_deref(), Some("US912810TM69 Govt"));
        assert_eq!(
            params.start_datetime.as_deref(),
            Some("2024-01-15T09:00:00")
        );
        assert_eq!(params.end_datetime.as_deref(), Some("2024-01-15T10:00:00"));
        assert_eq!(params.event_types.unwrap(), ["BID", "ASK"]);
        assert_eq!(
            params.options.unwrap(),
            [("includeBrokerCodes".to_string(), "true".to_string())]
        );
    }

    #[test]
    fn test_recipe_bqr_custom_request() {
        let params = bqr_request(
            "US912810TM69 Govt".to_string(),
            "2024-01-15T09:00:00".to_string(),
            "2024-01-15T10:00:00".to_string(),
            Some(vec!["TRADE".to_string()]),
            false,
        );
        assert_eq!(params.event_types.unwrap(), ["TRADE"]);
        assert!(params.options.is_none());
    }
}
