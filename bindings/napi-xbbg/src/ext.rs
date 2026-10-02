//! N-API bindings for xbbg-ext extension utilities.

use std::io::Cursor;
use std::str::FromStr;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use napi::bindgen_prelude::{Buffer, Error, Status};
use napi_derive::napi;

use xbbg_ext::constants::{DVD_COLS, DVD_TYPES, ETF_COLS, FUTURES_MONTHS, MONTH_CODES};
use xbbg_ext::markets::{self, sessions};
use xbbg_ext::resolvers::cdx::{gen_to_specific, parse_cdx_ticker, previous_series_ticker};
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
use xbbg_ext::{ExchangeInfo, OverridePatch, SessionWindows};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ext_err(e: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, e.to_string())
}

fn string_refs(values: &[String]) -> Vec<&str> {
    values.iter().map(String::as_str).collect()
}

fn date_from_parts(year: i32, month: u32, day: u32) -> napi::Result<chrono::NaiveDate> {
    chrono::NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            format!("invalid date: {year}-{month}-{day}"),
        )
    })
}

fn ipc_to_batch(buf: &[u8]) -> napi::Result<RecordBatch> {
    let cursor = Cursor::new(buf);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::new(Status::InvalidArg, format!("invalid Arrow IPC: {e}")))?;
    reader
        .next()
        .ok_or_else(|| Error::new(Status::InvalidArg, "empty Arrow IPC stream"))?
        .map_err(|e| Error::new(Status::GenericFailure, format!("Arrow read failed: {e}")))
}

fn batch_to_ipc(batch: RecordBatch) -> napi::Result<Buffer> {
    let schema = batch.schema();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    {
        let mut writer = StreamWriter::try_new(&mut cursor, &schema).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC writer init: {e}"),
            )
        })?;
        writer
            .write(&batch)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Arrow IPC write: {e}")))?;
        writer
            .finish()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Arrow IPC finish: {e}")))?;
    }
    Ok(Buffer::from(cursor.into_inner()))
}

fn session_pair(pair: &Option<(String, String)>) -> Option<TimeRange> {
    pair.as_ref().map(|(s, e)| TimeRange {
        start: s.clone(),
        end: e.clone(),
    })
}

fn exchange_info_to_output(info: ExchangeInfo) -> ExchangeInfoOutput {
    ExchangeInfoOutput {
        ticker: info.ticker,
        mic: info.mic,
        exch_code: info.exch_code,
        timezone: info.timezone,
        utc_offset: info.utc_offset,
        source: info.source.as_str().to_string(),
        day: session_pair(&info.sessions.day),
        allday: session_pair(&info.sessions.allday),
        pre: session_pair(&info.sessions.pre),
        post: session_pair(&info.sessions.post),
        am: session_pair(&info.sessions.am),
        pm: session_pair(&info.sessions.pm),
    }
}

fn input_pair(range: &Option<TimeRange>) -> Option<(String, String)> {
    range.as_ref().map(|r| (r.start.clone(), r.end.clone()))
}

// ---------------------------------------------------------------------------
// Output structs
// ---------------------------------------------------------------------------

#[napi(object)]
pub struct TimeRange {
    pub start: String,
    pub end: String,
}

#[napi(object)]
pub struct TickerPartsOutput {
    pub prefix: String,
    pub index: u32,
    pub asset: String,
    pub exchange: Option<String>,
}

#[napi(object)]
pub struct FuturesCandidateOutput {
    pub ticker: String,
    pub year: i32,
    pub month: u32,
}

#[napi(object)]
pub struct CdxTickerInfoOutput {
    pub index: String,
    pub series: String,
    pub version: Option<u32>,
    pub tenor: String,
    pub asset: String,
    pub is_generic: bool,
    pub series_num: Option<u32>,
}

#[napi(object)]
pub struct FxPairInfoOutput {
    pub fx_pair: String,
    pub factor: f64,
    pub from_ccy: String,
    pub to_ccy: String,
}

#[napi(object)]
pub struct SessionWindowsOutput {
    pub day: Option<TimeRange>,
    pub allday: Option<TimeRange>,
    pub pre: Option<TimeRange>,
    pub post: Option<TimeRange>,
    pub am: Option<TimeRange>,
    pub pm: Option<TimeRange>,
}

#[napi(object)]
pub struct MarketRuleOutput {
    pub pre_minutes: i32,
    pub post_minutes: i32,
    pub lunch_start_min: Option<i32>,
    pub lunch_end_min: Option<i32>,
    pub is_continuous: bool,
}

#[napi(object)]
pub struct ExchangeInfoOutput {
    pub ticker: String,
    pub mic: Option<String>,
    pub exch_code: Option<String>,
    pub timezone: String,
    pub utc_offset: Option<f64>,
    pub source: String,
    pub day: Option<TimeRange>,
    pub allday: Option<TimeRange>,
    pub pre: Option<TimeRange>,
    pub post: Option<TimeRange>,
    pub am: Option<TimeRange>,
    pub pm: Option<TimeRange>,
}

#[napi(object)]
pub struct ExchangeOverrideInput {
    pub timezone: Option<String>,
    pub mic: Option<String>,
    pub exch_code: Option<String>,
    pub day: Option<TimeRange>,
    pub allday: Option<TimeRange>,
    pub pre: Option<TimeRange>,
    pub post: Option<TimeRange>,
    pub am: Option<TimeRange>,
    pub pm: Option<TimeRange>,
}

// Re-use StringPair from the main module via the `super` import.
use super::StringPair;

// =============================================================================
// Date Utilities
// =============================================================================

/// Parse a date string into { year, month, day }.
///
/// Supports: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD, DD-MM-YYYY, DD/MM/YYYY
#[napi]
pub fn ext_parse_date(date_str: String) -> napi::Result<Vec<i32>> {
    let d = parse_date(&date_str).map_err(ext_err)?;
    Ok(vec![d.year(), d.month() as i32, d.day() as i32])
}

/// Format a date to string.
#[napi]
pub fn ext_fmt_date(year: i32, month: u32, day: u32, fmt: Option<String>) -> napi::Result<String> {
    let d = date_from_parts(year, month, day)?;
    Ok(fmt_date(d, fmt.as_deref()))
}

// =============================================================================
// Pivot Utilities
// =============================================================================

/// Pivot an Arrow IPC buffer from long to wide format.
///
/// Input: Arrow IPC stream with columns (ticker, field, value).
/// Output: Arrow IPC stream with columns (ticker, field1, field2, ...).
#[napi]
pub fn ext_pivot_to_wide(ipc_buffer: Buffer) -> napi::Result<Buffer> {
    let batch = ipc_to_batch(&ipc_buffer)?;
    let result = pivot_to_wide(&batch).map_err(ext_err)?;
    batch_to_ipc(result)
}

/// Check if an Arrow IPC buffer is in long format (ticker, field, value).
#[napi]
pub fn ext_is_long_format(ipc_buffer: Buffer) -> napi::Result<bool> {
    let batch = ipc_to_batch(&ipc_buffer)?;
    Ok(is_long_format(&batch))
}

// =============================================================================
// Ticker Utilities
// =============================================================================

/// Parse a Bloomberg ticker into components.
#[napi]
pub fn ext_parse_ticker(ticker: String) -> napi::Result<TickerPartsOutput> {
    let parts = parse_ticker_parts(&ticker).map_err(ext_err)?;
    Ok(TickerPartsOutput {
        prefix: parts.prefix,
        index: parts.index,
        asset: parts.asset,
        exchange: parts.exchange,
    })
}

/// Check if a ticker is a specific contract (not generic).
#[napi]
pub fn ext_is_specific_contract(ticker: String) -> bool {
    is_specific_contract(&ticker)
}

/// Build a futures ticker from components.
#[napi]
pub fn ext_build_futures_ticker(
    prefix: String,
    month_code: String,
    year: String,
    asset: String,
) -> String {
    build_futures_ticker(&prefix, &month_code, &year, &asset)
}

/// Normalize tickers to a list.
#[napi]
pub fn ext_normalize_tickers(tickers: Vec<String>) -> Vec<String> {
    let refs = string_refs(&tickers);
    normalize_tickers(&refs)
}

/// Filter to equity tickers only.
#[napi]
pub fn ext_filter_equity_tickers(tickers: Vec<String>) -> Vec<String> {
    let refs = string_refs(&tickers);
    filter_equity_tickers(&refs)
}

// =============================================================================
// Futures Resolution
// =============================================================================

/// Generate futures contract candidates.
#[napi]
pub fn ext_generate_futures_candidates(
    gen_ticker: String,
    year: i32,
    month: u32,
    day: u32,
    freq: Option<String>,
    count: Option<u32>,
) -> napi::Result<Vec<FuturesCandidateOutput>> {
    let dt = date_from_parts(year, month, day)?;
    let roll_freq = freq
        .as_deref()
        .and_then(|f| RollFrequency::from_str(f).ok())
        .unwrap_or(RollFrequency::Monthly);

    let candidates =
        generate_futures_candidates(&gen_ticker, dt, roll_freq, count.unwrap_or(4) as usize)
            .map_err(ext_err)?;

    Ok(candidates
        .into_iter()
        .map(|c| FuturesCandidateOutput {
            ticker: c.ticker,
            year: c.month.year(),
            month: c.month.month(),
        })
        .collect())
}

/// Validate that a ticker is generic (not specific).
#[napi]
pub fn ext_validate_generic_ticker(ticker: String) -> napi::Result<()> {
    validate_generic_ticker(&ticker).map_err(ext_err)
}

/// Get the contract index from a generic ticker (0-based).
#[napi]
pub fn ext_contract_index(gen_ticker: String) -> napi::Result<u32> {
    contract_index(&gen_ticker)
        .map(|v| v as u32)
        .map_err(ext_err)
}

/// Filter futures candidates by a cycle-months string (e.g., "HMUZ").
#[napi]
pub fn ext_filter_candidates_by_cycle(
    candidates: Vec<FuturesCandidateOutput>,
    cycle: String,
) -> Vec<FuturesCandidateOutput> {
    let tuples: Vec<(String, i32, u32)> = candidates
        .iter()
        .map(|c| (c.ticker.clone(), c.year, c.month))
        .collect();
    let filtered = filter_candidates_by_cycle(&tuples, &cycle);
    filtered
        .into_iter()
        .map(|(ticker, year, month)| FuturesCandidateOutput {
            ticker,
            year,
            month,
        })
        .collect()
}

/// Filter and sort futures contracts by maturity date.
#[napi]
pub fn ext_filter_valid_contracts(
    contracts: Vec<StringPair>,
    year: i32,
    month: u32,
    day: u32,
) -> napi::Result<Vec<String>> {
    let ref_date = date_from_parts(year, month, day)?;
    let pairs: Vec<(String, String)> = contracts.into_iter().map(|p| (p.key, p.value)).collect();
    Ok(filter_valid_contracts(&pairs, ref_date))
}

// =============================================================================
// CDX Resolution
// =============================================================================

/// Parse a CDX ticker.
#[napi]
pub fn ext_parse_cdx_ticker(ticker: String) -> napi::Result<CdxTickerInfoOutput> {
    let info = parse_cdx_ticker(&ticker).map_err(ext_err)?;
    Ok(CdxTickerInfoOutput {
        index: info.index,
        series: info.series,
        version: info.version.map(|version| version.get()),
        tenor: info.tenor,
        asset: info.asset,
        is_generic: info.is_generic,
        series_num: info.series_num,
    })
}

/// Get the previous series ticker for a CDX index.
#[napi]
pub fn ext_previous_cdx_series(ticker: String) -> napi::Result<Option<String>> {
    previous_series_ticker(&ticker).map_err(ext_err)
}

/// Convert a generic CDX ticker to specific series.
#[napi]
pub fn ext_cdx_gen_to_specific(gen_ticker: String, series: u32) -> napi::Result<String> {
    gen_to_specific(&gen_ticker, series).map_err(ext_err)
}

// =============================================================================
// Auction Utilities
// =============================================================================

/// Look up the stream-valid auction field group (trimmed, case-insensitive).
#[napi]
pub fn ext_auction_field_group(name: String) -> Option<Vec<String>> {
    xbbg_ext::auction::field_group(&name)
        .map(|fields| fields.iter().map(|field| (*field).to_string()).collect())
}

/// List the available auction field-group names in their canonical order.
#[napi]
pub fn ext_auction_field_group_names() -> Vec<String> {
    xbbg_ext::auction::field_group_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

/// Price fields whose numeric zero sentinel can be masked in latest snapshots.
#[napi]
pub fn ext_auction_zero_price_fields() -> Vec<String> {
    xbbg_ext::auction::ZERO_PRICE_FIELDS
        .iter()
        .map(|field| (*field).to_string())
        .collect()
}

/// Normalize an imbalance code to buy, sell, or none; unknown sides remain null.
#[napi]
pub fn ext_imbalance_side(code: String) -> Option<String> {
    xbbg_ext::auction::imbalance_side(&code).map(|side| side.as_str().to_string())
}

// =============================================================================
// Currency Utilities
// =============================================================================

/// Build an FX pair ticker for currency conversion.
#[napi]
pub fn ext_build_fx_pair(from_ccy: String, to_ccy: String) -> FxPairInfoOutput {
    let info = build_fx_pair(&from_ccy, &to_ccy);
    FxPairInfoOutput {
        fx_pair: info.fx_pair,
        factor: info.factor,
        from_ccy: info.from_ccy,
        to_ccy: info.to_ccy,
    }
}

/// Check if two currencies are effectively the same.
#[napi]
pub fn ext_same_currency(ccy1: String, ccy2: String) -> bool {
    same_currency(&ccy1, &ccy2)
}

/// Get currencies that need FX conversion.
#[napi]
pub fn ext_currencies_needing_conversion(currencies: Vec<String>, target: String) -> Vec<String> {
    let refs = string_refs(&currencies);
    currencies_needing_conversion(&refs, &target)
}

// =============================================================================
// Column Renaming
// =============================================================================

/// Get dividend column rename mapping.
#[napi]
pub fn ext_rename_dividend_columns(columns: Vec<String>) -> Vec<StringPair> {
    let refs = string_refs(&columns);
    rename_dividend_columns(&refs)
        .into_iter()
        .map(|(key, value)| StringPair { key, value })
        .collect()
}

/// Get ETF holdings column rename mapping.
#[napi]
pub fn ext_rename_etf_columns(columns: Vec<String>) -> Vec<StringPair> {
    let refs = string_refs(&columns);
    rename_etf_columns(&refs)
        .into_iter()
        .map(|(key, value)| StringPair { key, value })
        .collect()
}

// =============================================================================
// Constants
// =============================================================================

/// Get futures month code for a month name.
#[napi]
pub fn ext_get_month_code(month_name: String) -> Option<String> {
    FUTURES_MONTHS
        .get(month_name.as_str())
        .map(|s| s.to_string())
}

/// Get month name for a futures month code.
#[napi]
pub fn ext_get_month_name(code: String) -> Option<String> {
    MONTH_CODES.get(code.as_str()).map(|s| s.to_string())
}

/// Get all futures month mappings (month name -> code).
#[napi]
pub fn ext_get_futures_months() -> Vec<StringPair> {
    FUTURES_MONTHS
        .entries()
        .map(|(k, v)| StringPair {
            key: k.to_string(),
            value: v.to_string(),
        })
        .collect()
}

/// Get dividend type field name.
#[napi]
pub fn ext_get_dvd_type(typ: String) -> Option<String> {
    DVD_TYPES.get(typ.as_str()).map(|s| s.to_string())
}

/// Get all dividend type mappings.
#[napi]
pub fn ext_get_dvd_types() -> Vec<StringPair> {
    DVD_TYPES
        .entries()
        .map(|(k, v)| StringPair {
            key: k.to_string(),
            value: v.to_string(),
        })
        .collect()
}

/// Get all dividend column mappings.
#[napi]
pub fn ext_get_dvd_cols() -> Vec<StringPair> {
    DVD_COLS
        .entries()
        .map(|(k, v)| StringPair {
            key: k.to_string(),
            value: v.to_string(),
        })
        .collect()
}

/// Get all ETF column mappings.
#[napi]
pub fn ext_get_etf_cols() -> Vec<StringPair> {
    ETF_COLS
        .entries()
        .map(|(k, v)| StringPair {
            key: k.to_string(),
            value: v.to_string(),
        })
        .collect()
}

// =============================================================================
// YAS / Fixed Income
// =============================================================================

/// Build Bloomberg YAS override key-value pairs.
#[napi]
pub fn ext_build_yas_overrides(
    settle_dt: Option<String>,
    yield_type: Option<u32>,
    spread: Option<f64>,
    yield_val: Option<f64>,
    price: Option<f64>,
    benchmark: Option<String>,
) -> napi::Result<Vec<StringPair>> {
    let yt = match yield_type {
        Some(v) => Some(
            YieldType::try_from(v as u8)
                .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?,
        ),
        None => None,
    };
    Ok(build_yas_overrides(
        settle_dt.as_deref(),
        yt,
        spread,
        yield_val,
        price,
        benchmark.as_deref(),
    )
    .into_iter()
    .map(|(key, value)| StringPair { key, value })
    .collect())
}

// =============================================================================
// Earnings Utilities
// =============================================================================

/// Build column rename mapping from earnings header values.
#[napi]
pub fn ext_build_earning_header_rename(
    header_row: Vec<StringPair>,
    data_columns: Vec<String>,
) -> Vec<StringPair> {
    let pairs: Vec<(String, String)> = header_row.into_iter().map(|p| (p.key, p.value)).collect();
    let refs = string_refs(&data_columns);
    build_earning_header_rename(&pairs, &refs)
        .into_iter()
        .map(|(key, value)| StringPair { key, value })
        .collect()
}

/// Calculate level-based percentages for earnings data.
#[napi]
pub fn ext_calculate_level_percentages(
    values: Vec<Option<f64>>,
    levels: Vec<Option<i64>>,
) -> Vec<Option<f64>> {
    calculate_level_percentages(&values, &levels)
}

// =============================================================================
// BQL Query Builders
// =============================================================================

/// Build a BQL query for preferred stocks.
#[napi]
pub fn ext_build_preferreds_query(
    equity_ticker: String,
    extra_fields: Option<Vec<String>>,
) -> String {
    let fields = extra_fields.unwrap_or_default();
    let refs = string_refs(&fields);
    build_preferreds_query(&equity_ticker, &refs)
}

/// Build a BQL query for corporate bonds.
#[napi]
pub fn ext_build_corporate_bonds_query(
    ticker: String,
    ccy: Option<String>,
    extra_fields: Option<Vec<String>>,
    active_only: Option<bool>,
) -> String {
    let fields = extra_fields.unwrap_or_default();
    let refs = string_refs(&fields);
    build_corporate_bonds_query(&ticker, ccy.as_deref(), &refs, active_only.unwrap_or(true))
}

/// Build a BQL query for ETF holdings.
#[napi]
pub fn ext_build_etf_holdings_query(
    etf_ticker: String,
    extra_fields: Option<Vec<String>>,
) -> String {
    let fields = extra_fields.unwrap_or_default();
    let refs = string_refs(&fields);
    build_etf_holdings_query(&etf_ticker, &refs)
}

// =============================================================================
// DateTime Default Ranges
// =============================================================================

/// Compute default date range for turnover queries.
#[napi]
pub fn ext_default_turnover_dates(
    start_date: Option<String>,
    end_date: Option<String>,
) -> TimeRange {
    let (start, end) = default_turnover_dates(start_date.as_deref(), end_date.as_deref());
    TimeRange { start, end }
}

/// Compute default datetime range for BQR queries.
#[napi]
pub fn ext_default_bqr_datetimes(
    start_datetime: Option<String>,
    end_datetime: Option<String>,
) -> TimeRange {
    let (start, end) = default_bqr_datetimes(start_datetime.as_deref(), end_datetime.as_deref());
    TimeRange { start, end }
}

// =============================================================================
// Markets — Sessions & Timezone
// =============================================================================

/// Derive session windows from regular trading hours.
#[napi]
pub fn ext_derive_sessions(
    day_start: String,
    day_end: String,
    mic: Option<String>,
    exch_code: Option<String>,
) -> SessionWindowsOutput {
    let sw = sessions::derive_sessions(&day_start, &day_end, mic.as_deref(), exch_code.as_deref());
    SessionWindowsOutput {
        day: session_pair(&sw.day),
        allday: session_pair(&sw.allday),
        pre: session_pair(&sw.pre),
        post: session_pair(&sw.post),
        am: session_pair(&sw.am),
        pm: session_pair(&sw.pm),
    }
}

/// Look up a market rule by MIC code or Bloomberg exchange code.
#[napi]
pub fn ext_get_market_rule(
    mic: Option<String>,
    exch_code: Option<String>,
) -> Option<MarketRuleOutput> {
    let rule = sessions::get_market_rule(mic.as_deref(), exch_code.as_deref())?;
    Some(MarketRuleOutput {
        pre_minutes: rule.pre_minutes,
        post_minutes: rule.post_minutes,
        lunch_start_min: rule.lunch_start_min,
        lunch_end_min: rule.lunch_end_min,
        is_continuous: rule.is_continuous,
    })
}

/// Infer IANA timezone from country ISO code.
#[napi]
pub fn ext_infer_timezone(country_iso: String) -> Option<String> {
    sessions::infer_timezone_from_country(&country_iso).map(String::from)
}

/// Set a runtime exchange override for a ticker.
#[napi]
pub fn ext_set_exchange_override(ticker: String, input: ExchangeOverrideInput) -> napi::Result<()> {
    let sessions = if input.day.is_some()
        || input.allday.is_some()
        || input.pre.is_some()
        || input.post.is_some()
        || input.am.is_some()
        || input.pm.is_some()
    {
        Some(SessionWindows {
            day: input_pair(&input.day),
            allday: input_pair(&input.allday),
            pre: input_pair(&input.pre),
            post: input_pair(&input.post),
            am: input_pair(&input.am),
            pm: input_pair(&input.pm),
        })
    } else {
        None
    };

    let patch = OverridePatch {
        timezone: input.timezone,
        mic: input.mic,
        exch_code: input.exch_code,
        sessions,
    };

    markets::set_exchange_override(&ticker, patch).map_err(ext_err)
}

/// Get runtime override for a ticker.
#[napi]
pub fn ext_get_exchange_override(ticker: String) -> napi::Result<Option<ExchangeInfoOutput>> {
    markets::get_exchange_override(&ticker)
        .map(|info| info.map(exchange_info_to_output))
        .map_err(ext_err)
}

/// Clear one override (or all when ticker is null/undefined).
#[napi]
pub fn ext_clear_exchange_override(ticker: Option<String>) -> napi::Result<()> {
    markets::clear_exchange_override(ticker.as_deref()).map_err(ext_err)
}

/// List all runtime overrides.
#[napi]
pub fn ext_list_exchange_overrides() -> napi::Result<Vec<ExchangeInfoOutput>> {
    markets::list_exchange_overrides()
        .map(|overrides| {
            overrides
                .into_values()
                .map(exchange_info_to_output)
                .collect()
        })
        .map_err(ext_err)
}

/// Convert local exchange session times to UTC ISO timestamps.
#[napi]
pub fn ext_session_times_to_utc(
    start_time: String,
    end_time: String,
    exchange_tz: String,
    date: String,
) -> napi::Result<TimeRange> {
    let dt = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d").map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("invalid date '{date}', expected YYYY-MM-DD"),
        )
    })?;

    let (start, end) =
        markets::session_times_to_utc(&start_time, &end_time, &exchange_tz, dt).map_err(ext_err)?;
    Ok(TimeRange {
        start: start.format("%Y-%m-%dT%H:%M:%S").to_string(),
        end: end.format("%Y-%m-%dT%H:%M:%S").to_string(),
    })
}
