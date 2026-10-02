//! PyO3 bindings for xbbg-ext extension utilities.
//!
//! Exposes high-performance Rust implementations to Python.

use crate::native_arrow::{record_batch_to_arrow_record_batch, ArrowRecordBatch};
use chrono::Datelike;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;

use xbbg_ext::auction::{field_group, field_group_names, imbalance_side, ZERO_PRICE_FIELDS};
use xbbg_ext::constants::{DVD_TYPES, FUTURES_MONTHS, MONTH_CODES};
use xbbg_ext::resolvers::cdx::{
    cdx_series_from_ticker, gen_to_specific, parse_cdx_ticker, previous_series_ticker,
};
use xbbg_ext::resolvers::futures::{
    contract_index, filter_candidates_by_cycle, filter_valid_contracts,
    generate_futures_candidates, validate_generic_ticker, RollFrequency,
};
use xbbg_ext::transforms::bql::{
    build_corporate_bonds_query, build_etf_holdings_query, build_preferreds_query,
};
use xbbg_ext::transforms::currency::{build_fx_pair, currencies_needing_conversion, same_currency};
use xbbg_ext::transforms::fixed_income::{build_yas_overrides, YieldType};
use xbbg_ext::transforms::historical::{
    build_earning_header_rename, calculate_level_percentages, rename_dividend_columns,
    rename_etf_columns,
};
use xbbg_ext::utils::date::{default_bqr_datetimes, default_turnover_dates, fmt_date, parse_date};
use xbbg_ext::utils::pivot::{is_long_format, pivot_to_wide};
use xbbg_ext::utils::ticker::{
    build_futures_ticker, filter_equity_tickers, is_specific_contract, normalize_tickers,
    parse_ticker_parts,
};

fn string_refs(values: &[String]) -> Vec<&str> {
    values.iter().map(String::as_str).collect()
}

fn date_from_parts(year: i32, month: u32, day: u32) -> PyResult<chrono::NaiveDate> {
    chrono::NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| PyValueError::new_err(format!("invalid date: {year}-{month}-{day}")))
}

macro_rules! register_pyfunctions {
    ($module:expr; $($func:ident),+ $(,)?) => {{
        $( $module.add_function(wrap_pyfunction!($func, $module)?)?; )+
        Ok(())
    }};
}

// =============================================================================
// Date Utilities
// =============================================================================

/// Parse a date string into components (year, month, day).
///
/// Supports: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD, DD-MM-YYYY, DD/MM/YYYY
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (date_str))]
fn ext_parse_date(date_str: &str) -> PyResult<(i32, u32, u32)> {
    let d = parse_date(date_str).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((d.year(), d.month(), d.day()))
}

/// Format a date to string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (year, month, day, fmt=None))]
fn ext_fmt_date(year: i32, month: u32, day: u32, fmt: Option<&str>) -> PyResult<String> {
    let d = date_from_parts(year, month, day)?;
    Ok(fmt_date(d, fmt))
}

// =============================================================================
// Pivot Utilities
// =============================================================================

/// Pivot an xbbg ArrowRecordBatch from long to wide format.
///
/// Input: ArrowRecordBatch with columns (ticker, field, value)
/// Output: ArrowRecordBatch with columns (ticker, field1, field2, ...)
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_pivot_to_wide(py: Python<'_>, batch: PyRef<'_, ArrowRecordBatch>) -> PyResult<Py<PyAny>> {
    let rust_batch = batch.batch().clone();

    // Release the GIL while running the Arrow transform on owned Rust data.
    let result = py
        .detach(move || pivot_to_wide(&rust_batch))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    record_batch_to_arrow_record_batch(py, result)
}

/// Check if a RecordBatch is in long format (ticker, field, value).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_is_long_format(py: Python<'_>, batch: PyRef<'_, ArrowRecordBatch>) -> PyResult<bool> {
    let rust_batch = batch.batch().clone();
    Ok(py.detach(move || is_long_format(&rust_batch)))
}

// =============================================================================
// Ticker Utilities
// =============================================================================

/// Parse a Bloomberg ticker into components.
///
/// Returns: (prefix, index, asset, exchange)
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_parse_ticker(ticker: &str) -> PyResult<(String, u32, String, Option<String>)> {
    let parts = parse_ticker_parts(ticker).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((parts.prefix, parts.index, parts.asset, parts.exchange))
}

/// Check if a ticker is a specific contract (not generic).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_is_specific_contract(ticker: &str) -> bool {
    is_specific_contract(ticker)
}

/// Build a futures ticker from components.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_build_futures_ticker(prefix: &str, month_code: &str, year: &str, asset: &str) -> String {
    build_futures_ticker(prefix, month_code, year, asset)
}

/// Normalize tickers to a list.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_normalize_tickers(tickers: Vec<String>) -> Vec<String> {
    let refs = string_refs(&tickers);
    normalize_tickers(&refs)
}

/// Filter to equity tickers only.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_filter_equity_tickers(tickers: Vec<String>) -> Vec<String> {
    let refs = string_refs(&tickers);
    filter_equity_tickers(&refs)
}

// =============================================================================
// Futures Resolution
// =============================================================================

/// Generate futures contract candidates.
///
/// Returns list of (ticker, year, month) tuples.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (gen_ticker, year, month, day, freq="M", count=4))]
fn ext_generate_futures_candidates(
    gen_ticker: &str,
    year: i32,
    month: u32,
    day: u32,
    freq: &str,
    count: usize,
) -> PyResult<Vec<(String, i32, u32)>> {
    let dt = date_from_parts(year, month, day)?;

    use std::str::FromStr;
    let roll_freq = RollFrequency::from_str(freq).unwrap_or(RollFrequency::Monthly);

    let candidates = generate_futures_candidates(gen_ticker, dt, roll_freq, count)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(candidates
        .into_iter()
        .map(|c| (c.ticker, c.month.year(), c.month.month()))
        .collect())
}

/// Validate that a ticker is generic (not specific).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_validate_generic_ticker(ticker: &str) -> PyResult<()> {
    validate_generic_ticker(ticker).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get the contract index from a generic ticker (0-based).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_contract_index(gen_ticker: &str) -> PyResult<usize> {
    contract_index(gen_ticker).map_err(|e| PyValueError::new_err(e.to_string()))
}

// =============================================================================
// CDX Resolution
// =============================================================================

/// Parse a CDX ticker.
///
/// Returns: (index, series, tenor, asset, is_generic, series_num)
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_parse_cdx_ticker(
    ticker: &str,
) -> PyResult<(String, String, String, String, bool, Option<u32>)> {
    let info = cdx_series_from_ticker(ticker).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((
        info.index,
        info.series,
        info.tenor,
        info.asset,
        info.is_generic,
        info.series_num,
    ))
}

/// Parse the explicit version from a CDX ticker.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_parse_cdx_version(ticker: &str) -> PyResult<Option<u32>> {
    let info = parse_cdx_ticker(ticker).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(info.version.map(|version| version.get()))
}

/// Get the previous series ticker for a CDX index.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_previous_cdx_series(ticker: &str) -> PyResult<Option<String>> {
    previous_series_ticker(ticker).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Convert a generic CDX ticker to specific series.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_cdx_gen_to_specific(gen_ticker: &str, series: u32) -> PyResult<String> {
    gen_to_specific(gen_ticker, series).map_err(|e| PyValueError::new_err(e.to_string()))
}

// =============================================================================
// Currency Utilities
// =============================================================================

/// Build an FX pair ticker for currency conversion.
///
/// Returns: (fx_pair, factor, from_ccy, to_ccy)
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_build_fx_pair(from_ccy: &str, to_ccy: &str) -> (String, f64, String, String) {
    let info = build_fx_pair(from_ccy, to_ccy);
    (info.fx_pair, info.factor, info.from_ccy, info.to_ccy)
}

/// Check if two currencies are effectively the same.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_same_currency(ccy1: &str, ccy2: &str) -> bool {
    same_currency(ccy1, ccy2)
}

/// Get currencies that need FX conversion.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_currencies_needing_conversion(currencies: Vec<String>, target: &str) -> Vec<String> {
    let refs = string_refs(&currencies);
    currencies_needing_conversion(&refs, target)
}

// =============================================================================
// Column Renaming
// =============================================================================

/// Get dividend column rename mapping.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_rename_dividend_columns(columns: Vec<String>) -> Vec<(String, String)> {
    let refs = string_refs(&columns);
    rename_dividend_columns(&refs)
}

/// Get ETF holdings column rename mapping.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_rename_etf_columns(columns: Vec<String>) -> Vec<(String, String)> {
    let refs = string_refs(&columns);
    rename_etf_columns(&refs)
}

// =============================================================================
// Constants
// =============================================================================

/// Get futures month code for a month name.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_month_code(month: &str) -> Option<String> {
    FUTURES_MONTHS.get(month).map(|s| s.to_string())
}

/// Get month name for a futures month code.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_month_name(code: &str) -> Option<String> {
    MONTH_CODES.get(code).map(|s| s.to_string())
}

/// Get all futures month mappings.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_futures_months(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let dict = PyDict::new(py);
    for (k, v) in FUTURES_MONTHS.entries() {
        dict.set_item(*k, *v)?;
    }
    Ok(dict)
}

/// Get dividend type field name.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_dvd_type(typ: &str) -> Option<String> {
    DVD_TYPES.get(typ).map(|s| s.to_string())
}

/// Get all dividend type mappings.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_dvd_types(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let dict = PyDict::new(py);
    for (k, v) in DVD_TYPES.entries() {
        dict.set_item(*k, *v)?;
    }
    Ok(dict)
}

// =============================================================================
// Auction Utilities
// =============================================================================

/// Get named auction field groups as lists in their canonical field order.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_auction_field_groups(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let groups = PyDict::new(py);
    for &name in field_group_names() {
        let fields = field_group(name).expect("auction group names must resolve");
        groups.set_item(name, PyList::new(py, fields.iter().copied())?)?;
    }
    Ok(groups)
}

/// Get auction price fields whose numeric zeros mean no price, not a named field group.
#[cfg_attr(
    feature = "stub-gen",
    gen_stub_pyfunction(python = "def ext_auction_zero_price_fields() -> list[str]: ...")
)]
#[pyfunction]
fn ext_auction_zero_price_fields() -> &'static [&'static str] {
    ZERO_PRICE_FIELDS
}

/// Normalize a Bloomberg imbalance indicator to buy, sell, none, or None.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_imbalance_side(code: &str) -> Option<&'static str> {
    imbalance_side(code).map(|side| side.as_str())
}

// =============================================================================
// Futures Filtering
// =============================================================================

/// Filter futures candidates by a cycle-months string from Bloomberg FUT_GEN_MONTH.
///
/// Args:
///     candidates: List of (ticker, year, month) tuples from ext_generate_futures_candidates.
///     cycle: Month-code string from Bloomberg (e.g., "HMUZ").
///
/// Returns: Filtered list preserving original order.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_filter_candidates_by_cycle(
    candidates: Vec<(String, i32, u32)>,
    cycle: &str,
) -> Vec<(String, i32, u32)> {
    filter_candidates_by_cycle(&candidates, cycle)
}

/// Filter and sort futures contracts by maturity date.
///
/// Args:
///     contracts: List of (ticker, maturity_date_str) pairs.
///     year, month, day: Reference date components.
///
/// Returns: List of ticker strings for contracts maturing after the reference date,
///     sorted by maturity date ascending.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_filter_valid_contracts(
    contracts: Vec<(String, String)>,
    year: i32,
    month: u32,
    day: u32,
) -> PyResult<Vec<String>> {
    let ref_date = date_from_parts(year, month, day)?;

    Ok(filter_valid_contracts(&contracts, ref_date))
}

// =============================================================================
// YAS Overrides
// =============================================================================

/// Build Bloomberg YAS override key-value pairs.
///
/// Args:
///     settle_dt: Settlement date string (optional).
///     yield_type: Yield type flag value 1-9 (optional).
///     spread: Spread value (optional).
///     yield_val: Yield value (optional).
///     price: Price value (optional).
///     benchmark: Benchmark security (optional).
///
/// Returns: List of (key, value) string pairs for Bloomberg overrides.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (settle_dt=None, yield_type=None, spread=None, yield_val=None, price=None, benchmark=None))]
fn ext_build_yas_overrides(
    settle_dt: Option<&str>,
    yield_type: Option<u8>,
    spread: Option<f64>,
    yield_val: Option<f64>,
    price: Option<f64>,
    benchmark: Option<&str>,
) -> PyResult<Vec<(String, String)>> {
    let yt = match yield_type {
        Some(v) => Some(YieldType::try_from(v).map_err(|e| PyValueError::new_err(e.to_string()))?),
        None => None,
    };

    Ok(build_yas_overrides(
        settle_dt, yt, spread, yield_val, price, benchmark,
    ))
}

// =============================================================================
// Earnings Utilities
// =============================================================================

/// Build column rename mapping from earnings header values.
///
/// Args:
///     header_row: List of (column_name, header_value) pairs from the header DataFrame.
///     data_columns: Column names from the data DataFrame.
///
/// Returns: List of (old_name, new_name) pairs for columns that need renaming.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_build_earning_header_rename(
    header_row: Vec<(String, String)>,
    data_columns: Vec<String>,
) -> Vec<(String, String)> {
    let refs = string_refs(&data_columns);
    build_earning_header_rename(&header_row, &refs)
}

/// Calculate level-based percentages for earnings data.
///
/// Args:
///     values: List of optional float values.
///     levels: List of optional integer levels (1 = top level, 2 = sub-level).
///
/// Returns: List of optional percentage values (0-100).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_calculate_level_percentages(
    values: Vec<Option<f64>>,
    levels: Vec<Option<i64>>,
) -> Vec<Option<f64>> {
    calculate_level_percentages(&values, &levels)
}

// =============================================================================
// BQL Query Builders
// =============================================================================

/// Build a BQL query for preferred stocks.
///
/// Args:
///     equity_ticker: Company equity ticker (e.g., "BAC US Equity" or "BAC").
///     extra_fields: Additional fields beyond defaults (id, name).
///
/// Returns: Complete BQL query string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (equity_ticker, extra_fields=vec![]))]
fn ext_build_preferreds_query(equity_ticker: &str, extra_fields: Vec<String>) -> String {
    let refs = string_refs(&extra_fields);
    build_preferreds_query(equity_ticker, &refs)
}

/// Build a BQL query for corporate bonds.
///
/// Args:
///     ticker: Company ticker without suffix (e.g., "AAPL").
///     ccy: Currency filter (None for all currencies).
///     extra_fields: Additional fields beyond default (id).
///     active_only: If true, only return active bonds.
///
/// Returns: Complete BQL query string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (ticker, ccy=None, extra_fields=vec![], active_only=true))]
fn ext_build_corporate_bonds_query(
    ticker: &str,
    ccy: Option<&str>,
    extra_fields: Vec<String>,
    active_only: bool,
) -> String {
    let refs = string_refs(&extra_fields);
    build_corporate_bonds_query(ticker, ccy, &refs, active_only)
}

/// Build a BQL query for ETF holdings.
///
/// Args:
///     etf_ticker: ETF ticker (e.g., "SPY US Equity" or "SPY").
///     extra_fields: Additional fields beyond defaults (id_isin, weights, id().position).
///
/// Returns: Complete BQL query string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (etf_ticker, extra_fields=vec![]))]
fn ext_build_etf_holdings_query(etf_ticker: &str, extra_fields: Vec<String>) -> String {
    let refs = string_refs(&extra_fields);
    build_etf_holdings_query(etf_ticker, &refs)
}

// =============================================================================
// DateTime Default Ranges
// =============================================================================

/// Compute default date range for turnover queries.
///
/// Args:
///     start_date: Start date string (optional, default: 30 days before end).
///     end_date: End date string (optional, default: yesterday).
///
/// Returns: Tuple of (start_date, end_date) as ISO-8601 date strings.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (start_date=None, end_date=None))]
fn ext_default_turnover_dates(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> (String, String) {
    default_turnover_dates(start_date, end_date)
}

/// Compute default datetime range for BQR (quote request) queries.
///
/// Args:
///     start_datetime: Start datetime string (optional, default: 1 hour before end).
///     end_datetime: End datetime string (optional, default: now).
///
/// Returns: Tuple of (start_datetime, end_datetime) as ISO-8601 datetime strings.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (start_datetime=None, end_datetime=None))]
fn ext_default_bqr_datetimes(
    start_datetime: Option<&str>,
    end_datetime: Option<&str>,
) -> (String, String) {
    default_bqr_datetimes(start_datetime, end_datetime)
}

// =============================================================================
// Module Registration
// =============================================================================

/// Register ext functions with the module.
pub fn register_ext_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(
        m;
        ext_parse_date,
        ext_fmt_date,
        ext_pivot_to_wide,
        ext_is_long_format,
        ext_parse_ticker,
        ext_is_specific_contract,
        ext_build_futures_ticker,
        ext_normalize_tickers,
        ext_filter_equity_tickers,
        ext_generate_futures_candidates,
        ext_validate_generic_ticker,
        ext_contract_index,
        ext_parse_cdx_ticker,
        ext_parse_cdx_version,
        ext_previous_cdx_series,
        ext_cdx_gen_to_specific,
        ext_build_fx_pair,
        ext_same_currency,
        ext_currencies_needing_conversion,
        ext_rename_dividend_columns,
        ext_rename_etf_columns,
        ext_get_month_code,
        ext_get_month_name,
        ext_get_futures_months,
        ext_get_dvd_type,
        ext_get_dvd_types,
        ext_auction_field_groups,
        ext_auction_zero_price_fields,
        ext_imbalance_side,
        ext_filter_candidates_by_cycle,
        ext_filter_valid_contracts,
        ext_build_yas_overrides,
        ext_build_earning_header_rename,
        ext_calculate_level_percentages,
        ext_build_preferreds_query,
        ext_build_corporate_bonds_query,
        ext_build_etf_holdings_query,
        ext_default_turnover_dates,
        ext_default_bqr_datetimes,
    )
}
