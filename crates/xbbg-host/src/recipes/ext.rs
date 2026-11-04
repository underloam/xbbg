//! Synchronous `ext` utilities as named JSON calls.
//!
//! Function names are the napi `ext*` names without the prefix, camelCase
//! (`parseDate`, `buildYasOverrides`, …); the argument object carries the napi
//! positional parameters as named camelCase fields (`{"dateStr": …}`,
//! `{"genTicker": …, "year": …, "month": …, "day": …}`). Unknown fields are
//! rejected. [`call`] returns the function's natural result ([`ExtKind`]);
//! structured results are JSON mirroring the napi output objects (absent
//! optional members are omitted, `string | null` results are JSON). The Arrow
//! functions `pivotToWide` and `isLongFormat` take Arrow IPC stream bytes
//! through [`call_bytes`]; `isLongFormat` answers with one byte (`1` or `0`).
//!
//! Errors: rejected input is ARGUMENT (the `xbbg-ext` message, or napi's own
//! text for invalid dates and Arrow input), infrastructure failures are
//! INTERNAL. Differences from napi, all deliberate: `fmtDate` rejects a format
//! the date cannot render instead of panicking, `buildYasOverrides` rejects a
//! yield type outside 1–9 instead of truncating it to a byte, and
//! `listExchangeOverrides` is sorted by ticker instead of hash order.

use std::fmt::Write as _;
use std::io::Cursor;
use std::str::FromStr;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use xbbg_ext::constants::{DVD_COLS, DVD_TYPES, ETF_COLS, FUTURES_MONTHS, MONTH_CODES};
use xbbg_ext::markets::{self, sessions};
use xbbg_ext::resolvers::cdx::{gen_to_specific, parse_cdx_ticker, previous_series_ticker};
use xbbg_ext::resolvers::futures::{
    RollFrequency, contract_index, filter_candidates_by_cycle, filter_valid_contracts,
    generate_futures_candidates, validate_generic_ticker,
};
use xbbg_ext::transforms::bql::{
    build_corporate_bonds_query, build_etf_holdings_query, build_preferreds_query,
};
use xbbg_ext::transforms::currency::{build_fx_pair, currencies_needing_conversion, same_currency};
use xbbg_ext::transforms::fixed_income::{YieldType, build_yas_overrides};
use xbbg_ext::transforms::historical::{
    build_earning_header_rename, calculate_level_percentages, rename_dividend_columns,
    rename_etf_columns,
};
use xbbg_ext::utils::date::{default_bqr_datetimes, default_turnover_dates, parse_date};
use xbbg_ext::utils::pivot::{is_long_format, pivot_to_wide};
use xbbg_ext::utils::ticker::{
    build_futures_ticker, filter_equity_tickers, is_specific_contract, normalize_tickers,
    parse_ticker_parts,
};
use xbbg_ext::{ExchangeInfo, ExtError, OverridePatch, SessionWindows};

use crate::errors::{HostError, parse_args};

/// Natural result of an `ext` function called through [`call`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtKind {
    /// No value.
    Void,
    /// A boolean.
    Bool,
    /// A non-null string.
    Str,
    /// JSON text (objects, arrays, numbers and `string | null` results).
    Json,
}

/// The value an `ext` function returned.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtValue {
    Void,
    Bool(bool),
    Str(String),
    Json(String),
}

impl ExtValue {
    /// The kind of this value.
    pub fn kind(&self) -> ExtKind {
        match self {
            Self::Void => ExtKind::Void,
            Self::Bool(_) => ExtKind::Bool,
            Self::Str(_) => ExtKind::Str,
            Self::Json(_) => ExtKind::Json,
        }
    }
}

/// Every function [`call`] accepts, with its natural result kind.
pub const FUNCTIONS: &[(&str, ExtKind)] = &[
    ("parseDate", ExtKind::Json),
    ("fmtDate", ExtKind::Str),
    ("parseTicker", ExtKind::Json),
    ("isSpecificContract", ExtKind::Bool),
    ("buildFuturesTicker", ExtKind::Str),
    ("normalizeTickers", ExtKind::Json),
    ("filterEquityTickers", ExtKind::Json),
    ("generateFuturesCandidates", ExtKind::Json),
    ("validateGenericTicker", ExtKind::Void),
    ("contractIndex", ExtKind::Json),
    ("filterCandidatesByCycle", ExtKind::Json),
    ("filterValidContracts", ExtKind::Json),
    ("parseCdxTicker", ExtKind::Json),
    ("previousCdxSeries", ExtKind::Json),
    ("cdxGenToSpecific", ExtKind::Str),
    ("auctionFieldGroup", ExtKind::Json),
    ("auctionZeroPriceFields", ExtKind::Json),
    ("imbalanceSide", ExtKind::Json),
    ("buildFxPair", ExtKind::Json),
    ("sameCurrency", ExtKind::Bool),
    ("currenciesNeedingConversion", ExtKind::Json),
    ("renameDividendColumns", ExtKind::Json),
    ("renameEtfColumns", ExtKind::Json),
    ("getMonthCode", ExtKind::Json),
    ("getMonthName", ExtKind::Json),
    ("getFuturesMonths", ExtKind::Json),
    ("getDvdType", ExtKind::Json),
    ("getDvdTypes", ExtKind::Json),
    ("getDvdCols", ExtKind::Json),
    ("getEtfCols", ExtKind::Json),
    ("buildYasOverrides", ExtKind::Json),
    ("buildEarningHeaderRename", ExtKind::Json),
    ("calculateLevelPercentages", ExtKind::Json),
    ("buildPreferredsQuery", ExtKind::Str),
    ("buildCorporateBondsQuery", ExtKind::Str),
    ("buildEtfHoldingsQuery", ExtKind::Str),
    ("defaultTurnoverDates", ExtKind::Json),
    ("defaultBqrDatetimes", ExtKind::Json),
    ("deriveSessions", ExtKind::Json),
    ("getMarketRule", ExtKind::Json),
    ("inferTimezone", ExtKind::Json),
    ("setExchangeOverride", ExtKind::Void),
    ("getExchangeOverride", ExtKind::Json),
    ("clearExchangeOverride", ExtKind::Void),
    ("listExchangeOverrides", ExtKind::Json),
    ("sessionTimesToUtc", ExtKind::Json),
];

/// Every function [`call_bytes`] accepts.
pub const BYTE_FUNCTIONS: &[&str] = &["pivotToWide", "isLongFormat"];

/// The natural result kind of `function`, or `None` if [`call`] does not know it.
pub fn kind_of(function: &str) -> Option<ExtKind> {
    FUNCTIONS
        .iter()
        .find(|(name, _)| *name == function)
        .map(|(_, kind)| *kind)
}

/// `{start, end}`, used for session windows and default ranges.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StringPair {
    key: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FuturesCandidate {
    ticker: String,
    year: i32,
    month: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TickerPartsOutput {
    prefix: String,
    index: u32,
    asset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exchange: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CdxTickerInfoOutput {
    index: String,
    series: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u32>,
    tenor: String,
    asset: String,
    is_generic: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    series_num: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FxPairInfoOutput {
    fx_pair: String,
    factor: f64,
    from_ccy: String,
    to_ccy: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionWindowsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    day: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allday: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    am: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pm: Option<TimeRange>,
}

impl From<&SessionWindows> for SessionWindowsOutput {
    fn from(windows: &SessionWindows) -> Self {
        Self {
            day: time_range(&windows.day),
            allday: time_range(&windows.allday),
            pre: time_range(&windows.pre),
            post: time_range(&windows.post),
            am: time_range(&windows.am),
            pm: time_range(&windows.pm),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketRuleOutput {
    pre_minutes: i32,
    post_minutes: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    lunch_start_min: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lunch_end_min: Option<i32>,
    is_continuous: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfoOutput {
    ticker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exch_code: Option<String>,
    timezone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    utc_offset: Option<f64>,
    source: &'static str,
    #[serde(flatten)]
    sessions: SessionWindowsOutput,
}

impl From<ExchangeInfo> for ExchangeInfoOutput {
    fn from(info: ExchangeInfo) -> Self {
        Self {
            sessions: SessionWindowsOutput::from(&info.sessions),
            ticker: info.ticker,
            mic: info.mic,
            exch_code: info.exch_code,
            timezone: info.timezone,
            utc_offset: info.utc_offset,
            source: info.source.as_str(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DateStrArgs {
    date_str: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FmtDateArgs {
    year: i32,
    month: u32,
    day: u32,
    #[serde(default)]
    fmt: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TickerArgs {
    ticker: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OptionalTickerArgs {
    #[serde(default)]
    ticker: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GenTickerArgs {
    gen_ticker: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TickersArgs {
    tickers: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BuildFuturesTickerArgs {
    prefix: String,
    month_code: String,
    year: String,
    asset: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GenerateFuturesCandidatesArgs {
    gen_ticker: String,
    year: i32,
    month: u32,
    day: u32,
    #[serde(default)]
    freq: Option<String>,
    #[serde(default)]
    count: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterCandidatesByCycleArgs {
    candidates: Vec<FuturesCandidate>,
    cycle: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterValidContractsArgs {
    contracts: Vec<StringPair>,
    year: i32,
    month: u32,
    day: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CdxGenToSpecificArgs {
    gen_ticker: String,
    series: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NameArgs {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CodeArgs {
    code: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FxPairArgs {
    from_ccy: String,
    to_ccy: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SameCurrencyArgs {
    ccy1: String,
    ccy2: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CurrenciesArgs {
    currencies: Vec<String>,
    target: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnsArgs {
    columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MonthNameArgs {
    month_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DvdTypeArgs {
    typ: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct YasOverridesArgs {
    #[serde(default)]
    settle_dt: Option<String>,
    #[serde(default)]
    yield_type: Option<i64>,
    #[serde(default)]
    spread: Option<f64>,
    #[serde(default)]
    yield_val: Option<f64>,
    #[serde(default)]
    price: Option<f64>,
    #[serde(default)]
    benchmark: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EarningHeaderRenameArgs {
    header_row: Vec<StringPair>,
    data_columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LevelPercentagesArgs {
    values: Vec<Option<f64>>,
    levels: Vec<Option<i64>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreferredsQueryArgs {
    equity_ticker: String,
    #[serde(default)]
    extra_fields: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorporateBondsQueryArgs {
    ticker: String,
    #[serde(default)]
    ccy: Option<String>,
    #[serde(default)]
    extra_fields: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EtfHoldingsQueryArgs {
    etf_ticker: String,
    #[serde(default)]
    extra_fields: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TurnoverDatesArgs {
    #[serde(default)]
    start_date: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BqrDatetimesArgs {
    #[serde(default)]
    start_datetime: Option<String>,
    #[serde(default)]
    end_datetime: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeriveSessionsArgs {
    day_start: String,
    day_end: String,
    #[serde(default)]
    mic: Option<String>,
    #[serde(default)]
    exch_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MarketRuleArgs {
    #[serde(default)]
    mic: Option<String>,
    #[serde(default)]
    exch_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CountryArgs {
    country_iso: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetExchangeOverrideArgs {
    ticker: String,
    input: ExchangeOverrideInput,
}

/// `setExchangeOverride` patch; sessions are replaced as a whole when any window is given.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExchangeOverrideInput {
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    mic: Option<String>,
    #[serde(default)]
    exch_code: Option<String>,
    #[serde(default)]
    day: Option<TimeRange>,
    #[serde(default)]
    allday: Option<TimeRange>,
    #[serde(default)]
    pre: Option<TimeRange>,
    #[serde(default)]
    post: Option<TimeRange>,
    #[serde(default)]
    am: Option<TimeRange>,
    #[serde(default)]
    pm: Option<TimeRange>,
}

impl ExchangeOverrideInput {
    fn into_patch(self) -> OverridePatch {
        let has_sessions = [
            &self.day,
            &self.allday,
            &self.pre,
            &self.post,
            &self.am,
            &self.pm,
        ]
        .iter()
        .any(|window| window.is_some());
        let sessions = has_sessions.then(|| SessionWindows {
            day: self.day.map(TimeRange::into_pair),
            allday: self.allday.map(TimeRange::into_pair),
            pre: self.pre.map(TimeRange::into_pair),
            post: self.post.map(TimeRange::into_pair),
            am: self.am.map(TimeRange::into_pair),
            pm: self.pm.map(TimeRange::into_pair),
        });
        OverridePatch {
            timezone: self.timezone,
            mic: self.mic,
            exch_code: self.exch_code,
            sessions,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionTimesArgs {
    start_time: String,
    end_time: String,
    exchange_tz: String,
    date: String,
}

impl TimeRange {
    fn into_pair(self) -> (String, String) {
        (self.start, self.end)
    }
}

fn time_range(pair: &Option<(String, String)>) -> Option<TimeRange> {
    pair.as_ref().map(|(start, end)| TimeRange {
        start: start.clone(),
        end: end.clone(),
    })
}

/// Call `function` with its JSON argument object.
pub fn call(function: &str, args: &str) -> Result<ExtValue, HostError> {
    match function {
        "parseDate" => {
            let args: DateStrArgs = parse_args(args)?;
            let date = parse_date(&args.date_str).map_err(ext_error)?;
            json(&[date.year(), date.month() as i32, date.day() as i32])
        }
        "fmtDate" => {
            let args: FmtDateArgs = parse_args(args)?;
            let date = date_from_parts(args.year, args.month, args.day)?;
            format_date(date, args.fmt.as_deref()).map(ExtValue::Str)
        }
        "parseTicker" => {
            let args: TickerArgs = parse_args(args)?;
            let parts = parse_ticker_parts(&args.ticker).map_err(ext_error)?;
            json(&TickerPartsOutput {
                prefix: parts.prefix,
                index: parts.index,
                asset: parts.asset,
                exchange: parts.exchange,
            })
        }
        "isSpecificContract" => {
            let args: TickerArgs = parse_args(args)?;
            Ok(ExtValue::Bool(is_specific_contract(&args.ticker)))
        }
        "buildFuturesTicker" => {
            let args: BuildFuturesTickerArgs = parse_args(args)?;
            Ok(ExtValue::Str(build_futures_ticker(
                &args.prefix,
                &args.month_code,
                &args.year,
                &args.asset,
            )))
        }
        "normalizeTickers" => {
            let args: TickersArgs = parse_args(args)?;
            json(&normalize_tickers(args.tickers))
        }
        "filterEquityTickers" => {
            let args: TickersArgs = parse_args(args)?;
            json(&filter_equity_tickers(args.tickers))
        }
        "generateFuturesCandidates" => {
            let args: GenerateFuturesCandidatesArgs = parse_args(args)?;
            let date = date_from_parts(args.year, args.month, args.day)?;
            let frequency = args
                .freq
                .as_deref()
                .and_then(|freq| RollFrequency::from_str(freq).ok())
                .unwrap_or(RollFrequency::Monthly);
            let count = args.count.unwrap_or(4) as usize;
            let candidates = generate_futures_candidates(&args.gen_ticker, date, frequency, count)
                .map_err(ext_error)?;
            json(
                &candidates
                    .into_iter()
                    .map(|candidate| FuturesCandidate {
                        ticker: candidate.ticker,
                        year: candidate.month.year(),
                        month: candidate.month.month(),
                    })
                    .collect::<Vec<_>>(),
            )
        }
        "validateGenericTicker" => {
            let args: TickerArgs = parse_args(args)?;
            validate_generic_ticker(&args.ticker).map_err(ext_error)?;
            Ok(ExtValue::Void)
        }
        "contractIndex" => {
            let args: GenTickerArgs = parse_args(args)?;
            json(&contract_index(&args.gen_ticker).map_err(ext_error)?)
        }
        "filterCandidatesByCycle" => {
            let args: FilterCandidatesByCycleArgs = parse_args(args)?;
            let candidates = args
                .candidates
                .into_iter()
                .map(|candidate| (candidate.ticker, candidate.year, candidate.month))
                .collect();
            json(
                &filter_candidates_by_cycle(candidates, &args.cycle)
                    .into_iter()
                    .map(|(ticker, year, month)| FuturesCandidate {
                        ticker,
                        year,
                        month,
                    })
                    .collect::<Vec<_>>(),
            )
        }
        "filterValidContracts" => {
            let args: FilterValidContractsArgs = parse_args(args)?;
            let reference = date_from_parts(args.year, args.month, args.day)?;
            let contracts = args
                .contracts
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect();
            json(&filter_valid_contracts(contracts, reference))
        }
        "parseCdxTicker" => {
            let args: TickerArgs = parse_args(args)?;
            let info = parse_cdx_ticker(&args.ticker).map_err(ext_error)?;
            json(&CdxTickerInfoOutput {
                index: info.index,
                series: info.series,
                version: info.version.map(|version| version.get()),
                tenor: info.tenor,
                asset: info.asset,
                is_generic: info.is_generic,
                series_num: info.series_num,
            })
        }
        "previousCdxSeries" => {
            let args: TickerArgs = parse_args(args)?;
            json(&previous_series_ticker(&args.ticker).map_err(ext_error)?)
        }
        "cdxGenToSpecific" => {
            let args: CdxGenToSpecificArgs = parse_args(args)?;
            gen_to_specific(&args.gen_ticker, args.series)
                .map(ExtValue::Str)
                .map_err(ext_error)
        }
        "auctionFieldGroup" => {
            let args: NameArgs = parse_args(args)?;
            json(&xbbg_ext::auction::field_group(&args.name))
        }
        "auctionZeroPriceFields" => {
            let _: NoArgs = parse_args(args)?;
            json(&xbbg_ext::auction::ZERO_PRICE_FIELDS)
        }
        "imbalanceSide" => {
            let args: CodeArgs = parse_args(args)?;
            json(&xbbg_ext::auction::imbalance_side(&args.code).map(|side| side.as_str()))
        }
        "buildFxPair" => {
            let args: FxPairArgs = parse_args(args)?;
            let info = build_fx_pair(&args.from_ccy, &args.to_ccy);
            json(&FxPairInfoOutput {
                fx_pair: info.fx_pair,
                factor: info.factor,
                from_ccy: info.from_ccy,
                to_ccy: info.to_ccy,
            })
        }
        "sameCurrency" => {
            let args: SameCurrencyArgs = parse_args(args)?;
            Ok(ExtValue::Bool(same_currency(&args.ccy1, &args.ccy2)))
        }
        "currenciesNeedingConversion" => {
            let args: CurrenciesArgs = parse_args(args)?;
            json(&currencies_needing_conversion(
                &args.currencies,
                &args.target,
            ))
        }
        "renameDividendColumns" => {
            let args: ColumnsArgs = parse_args(args)?;
            json(&pairs(rename_dividend_columns(args.columns)))
        }
        "renameEtfColumns" => {
            let args: ColumnsArgs = parse_args(args)?;
            json(&pairs(rename_etf_columns(args.columns)))
        }
        "getMonthCode" => {
            let args: MonthNameArgs = parse_args(args)?;
            json(&FUTURES_MONTHS.get(args.month_name.as_str()))
        }
        "getMonthName" => {
            let args: CodeArgs = parse_args(args)?;
            json(&MONTH_CODES.get(args.code.as_str()))
        }
        "getFuturesMonths" => constant_pairs(args, FUTURES_MONTHS.entries()),
        "getDvdType" => {
            let args: DvdTypeArgs = parse_args(args)?;
            json(&DVD_TYPES.get(args.typ.as_str()))
        }
        "getDvdTypes" => constant_pairs(args, DVD_TYPES.entries()),
        "getDvdCols" => constant_pairs(args, DVD_COLS.entries()),
        "getEtfCols" => constant_pairs(args, ETF_COLS.entries()),
        "buildYasOverrides" => {
            let args: YasOverridesArgs = parse_args(args)?;
            let yield_type = args
                .yield_type
                .map(|code| {
                    u8::try_from(code)
                        .map_err(|_| ExtError::UnknownYieldType(code.to_string()))
                        .and_then(YieldType::try_from)
                        .map_err(ext_error)
                })
                .transpose()?;
            json(&pairs(build_yas_overrides(
                args.settle_dt.as_deref(),
                yield_type,
                args.spread,
                args.yield_val,
                args.price,
                args.benchmark.as_deref(),
            )))
        }
        "buildEarningHeaderRename" => {
            let args: EarningHeaderRenameArgs = parse_args(args)?;
            let header = args
                .header_row
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect::<Vec<_>>();
            json(&pairs(build_earning_header_rename(
                &header,
                &args.data_columns,
            )))
        }
        "calculateLevelPercentages" => {
            let args: LevelPercentagesArgs = parse_args(args)?;
            json(&calculate_level_percentages(&args.values, &args.levels))
        }
        "buildPreferredsQuery" => {
            let args: PreferredsQueryArgs = parse_args(args)?;
            let extra = args.extra_fields.unwrap_or_default();
            Ok(ExtValue::Str(build_preferreds_query(
                &args.equity_ticker,
                &str_refs(&extra),
            )))
        }
        "buildCorporateBondsQuery" => {
            let args: CorporateBondsQueryArgs = parse_args(args)?;
            let extra = args.extra_fields.unwrap_or_default();
            Ok(ExtValue::Str(build_corporate_bonds_query(
                &args.ticker,
                args.ccy.as_deref(),
                &str_refs(&extra),
            )))
        }
        "buildEtfHoldingsQuery" => {
            let args: EtfHoldingsQueryArgs = parse_args(args)?;
            let extra = args.extra_fields.unwrap_or_default();
            Ok(ExtValue::Str(build_etf_holdings_query(
                &args.etf_ticker,
                &str_refs(&extra),
            )))
        }
        "defaultTurnoverDates" => {
            let args: TurnoverDatesArgs = parse_args(args)?;
            let (start, end) =
                default_turnover_dates(args.start_date.as_deref(), args.end_date.as_deref());
            json(&TimeRange { start, end })
        }
        "defaultBqrDatetimes" => {
            let args: BqrDatetimesArgs = parse_args(args)?;
            let (start, end) =
                default_bqr_datetimes(args.start_datetime.as_deref(), args.end_datetime.as_deref());
            json(&TimeRange { start, end })
        }
        "deriveSessions" => {
            let args: DeriveSessionsArgs = parse_args(args)?;
            let windows = sessions::derive_sessions(
                &args.day_start,
                &args.day_end,
                args.mic.as_deref(),
                args.exch_code.as_deref(),
            );
            json(&SessionWindowsOutput::from(&windows))
        }
        "getMarketRule" => {
            let args: MarketRuleArgs = parse_args(args)?;
            let rule = sessions::get_market_rule(args.mic.as_deref(), args.exch_code.as_deref());
            json(&rule.map(|rule| MarketRuleOutput {
                pre_minutes: rule.pre_minutes,
                post_minutes: rule.post_minutes,
                lunch_start_min: rule.lunch_start_min,
                lunch_end_min: rule.lunch_end_min,
                is_continuous: rule.is_continuous,
            }))
        }
        "inferTimezone" => {
            let args: CountryArgs = parse_args(args)?;
            json(&sessions::infer_timezone_from_country(&args.country_iso))
        }
        "setExchangeOverride" => {
            let args: SetExchangeOverrideArgs = parse_args(args)?;
            markets::set_exchange_override(&args.ticker, args.input.into_patch())
                .map_err(ext_error)?;
            Ok(ExtValue::Void)
        }
        "getExchangeOverride" => {
            let args: TickerArgs = parse_args(args)?;
            let info = markets::get_exchange_override(&args.ticker).map_err(ext_error)?;
            json(&info.map(ExchangeInfoOutput::from))
        }
        "clearExchangeOverride" => {
            let args: OptionalTickerArgs = parse_args(args)?;
            markets::clear_exchange_override(args.ticker.as_deref()).map_err(ext_error)?;
            Ok(ExtValue::Void)
        }
        "listExchangeOverrides" => {
            let _: NoArgs = parse_args(args)?;
            let mut overrides = markets::list_exchange_overrides()
                .map_err(ext_error)?
                .into_iter()
                .collect::<Vec<_>>();
            overrides.sort_by(|(left, _), (right, _)| left.cmp(right));
            json(
                &overrides
                    .into_iter()
                    .map(|(_, info)| ExchangeInfoOutput::from(info))
                    .collect::<Vec<_>>(),
            )
        }
        "sessionTimesToUtc" => {
            let args: SessionTimesArgs = parse_args(args)?;
            let date = NaiveDate::parse_from_str(&args.date, "%Y-%m-%d").map_err(|_| {
                HostError::argument(format!("invalid date '{}', expected YYYY-MM-DD", args.date))
            })?;
            let (start, end) = markets::session_times_to_utc(
                &args.start_time,
                &args.end_time,
                &args.exchange_tz,
                date,
            )
            .map_err(ext_error)?;
            json(&TimeRange {
                start: start.format("%Y-%m-%dT%H:%M:%S").to_string(),
                end: end.format("%Y-%m-%dT%H:%M:%S").to_string(),
            })
        }
        other => Err(unknown_function(other)),
    }
}

/// Call an Arrow `ext` function on Arrow IPC stream bytes.
///
/// `pivotToWide` returns the wide table as an IPC stream; `isLongFormat`
/// returns `[1]` for a long `(key, field, value)` table and `[0]` otherwise.
/// Only the first batch of the input stream is read.
pub fn call_bytes(function: &str, input: &[u8]) -> Result<Vec<u8>, HostError> {
    match function {
        "pivotToWide" => {
            let batch = ipc_to_batch(input)?;
            let wide = pivot_to_wide(&batch).map_err(ext_error)?;
            batch_to_ipc_stream(&wide)
        }
        "isLongFormat" => {
            let batch = ipc_to_batch(input)?;
            Ok(vec![u8::from(is_long_format(&batch))])
        }
        other => Err(unknown_function(other)),
    }
}

/// Classify an `xbbg-ext` failure: rejected input is ARGUMENT, the rest INTERNAL.
pub fn ext_error(error: ExtError) -> HostError {
    match error {
        ExtError::Arrow(_) | ExtError::Internal(_) => HostError::internal(error.to_string()),
        ExtError::InvalidInput(_)
        | ExtError::DateParse(_)
        | ExtError::InvalidTicker(_)
        | ExtError::SpecificTicker(_)
        | ExtError::UnknownYieldType(_)
        | ExtError::CandidateCountTooLarge { .. }
        | ExtError::MissingColumn(_) => HostError::argument(error.to_string()),
    }
}

fn unknown_function(function: &str) -> HostError {
    HostError::unknown_method("ext", function)
}

fn json<T: Serialize + ?Sized>(value: &T) -> Result<ExtValue, HostError> {
    serde_json::to_string(value)
        .map(ExtValue::Json)
        .map_err(|error| HostError::internal(format!("serialize ext result: {error}")))
}

fn pairs(pairs: Vec<(String, String)>) -> Vec<StringPair> {
    pairs
        .into_iter()
        .map(|(key, value)| StringPair { key, value })
        .collect()
}

/// A constant table as `{key, value}` pairs in its native (phf) entry order.
fn constant_pairs<'a>(
    args: &str,
    entries: impl Iterator<Item = (&'a &'static str, &'a &'static str)>,
) -> Result<ExtValue, HostError> {
    let _: NoArgs = parse_args(args)?;
    json(
        &entries
            .map(|(key, value)| StringPair {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect::<Vec<_>>(),
    )
}

fn str_refs(values: &[String]) -> Vec<&str> {
    values.iter().map(String::as_str).collect()
}

fn date_from_parts(year: i32, month: u32, day: u32) -> Result<NaiveDate, HostError> {
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| HostError::argument(format!("invalid date: {year}-{month}-{day}")))
}

/// `fmt_date` without its panic on a format the date cannot render.
fn format_date(date: NaiveDate, fmt: Option<&str>) -> Result<String, HostError> {
    let fmt = fmt.unwrap_or("%Y%m%d");
    let mut out = String::new();
    write!(out, "{}", date.format(fmt))
        .map_err(|_| HostError::argument(format!("invalid date format '{fmt}'")))?;
    Ok(out)
}

fn ipc_to_batch(input: &[u8]) -> Result<RecordBatch, HostError> {
    let mut reader = StreamReader::try_new(Cursor::new(input), None)
        .map_err(|error| HostError::argument(format!("invalid Arrow IPC: {error}")))?;
    reader
        .next()
        .ok_or_else(|| HostError::argument("empty Arrow IPC stream"))?
        .map_err(|error| HostError::argument(format!("Arrow read failed: {error}")))
}

fn batch_to_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, HostError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
        .map_err(|error| HostError::internal(format!("Arrow IPC writer init failed: {error}")))?;
    writer
        .write(batch)
        .map_err(|error| HostError::internal(format!("Arrow IPC write failed: {error}")))?;
    writer
        .finish()
        .map_err(|error| HostError::internal(format!("Arrow IPC finalize failed: {error}")))?;
    drop(writer);
    Ok(buffer)
}

#[cfg(test)]
mod tests;
