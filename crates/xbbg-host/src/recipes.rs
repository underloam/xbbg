//! Recipe dispatch shared by language bindings.
//!
//! A binding passes the `@xbbg/core` recipe method name and one JSON object
//! that carries the JavaScript positional arguments and options as named
//! camelCase fields. [`RecipeCall::parse`] rejects unknown fields and malformed
//! values before any Bloomberg work and applies the JavaScript defaults;
//! [`RecipeCall::run`] then runs the matching `xbbg-recipes` workflow with
//! default request options (exactly like napi) and maps failures through
//! [`HostError::from_recipe`]. [`ext`] does the same for the synchronous `ext`
//! utilities.
//!
//! # Argument objects
//!
//! `*` marks a required field. Dates arrive already formatted by the host
//! (`YYYYMMDD`, ISO 8601, …). A date that JavaScript sends as `''` when it is
//! missing (`formatDate(dt) ?? ''`) is `""` when absent or `null`; the recipe
//! then applies its own rule for an empty date. Booleans and lists with a
//! JavaScript default take that default when absent or `null`.
//!
//! | Method | Fields |
//! |---|---|
//! | `bqr` | `ticker*`, `startDatetime`, `endDatetime`, `eventTypes`, `includeBrokerCodes` (`true`) |
//! | `yas` | `tickers*`, `fields*`, `settleDt`, `yieldType` (1–9), `spread`, `yieldVal`, `price`, `benchmark` |
//! | `preferreds` | `equityTicker*`, `fields` |
//! | `corporateBonds` | `ticker*`, `ccy`, `fields` |
//! | `futTicker`, `activeFutures` | `genTicker*`, `dt` (`""`), `freq` |
//! | `futuresCurve` | `genTicker*`, `asof`, `chainField`, `fields`, `maxContracts` |
//! | `cdxTicker` | `genTicker*`, `dt` (`""`), `versionless` (`false`) |
//! | `activeCdx` | `genTicker*`, `dt` (`""`), `lookbackDays`, `versionless` (`false`) |
//! | `dividend` | `tickers*`, `startDate` (`""`), `endDate` (`""`), `dvdType` |
//! | `dividendYield` | `tickers*`, `startDate` (`""`), `endDate` (`""`), `dividendTypes`, `windowDays` |
//! | `turnover` | `tickers*`, `startDate` (`""`), `endDate` (`""`), `ccy`, `factor` |
//! | `etfHoldings` | `etfTicker*`, `fields` |
//! | `volSurface` | `tickers*`, `startDate` (`""`), `endDate` (`""`), `preset`, `fields`, `asDecimal` (`true`), `includeDerived` (`false`), `riskFreeRate`, `dividendYieldField` |
//! | `indexMembers` | `index*`, `field`, `asof` |
//! | `resolveIsins` | `isins*` |
//! | `resolveVenues` | `securities*`, `pcsOverrides` (`{}`) |
//! | `auctionSnapshot` | `securities*`, `fields` (`[]`: the default auction group), `pcsOverrides` (`{}`) |
//! | `delayStatus` | `tickers*`, `checkIdentity` (`false`) |
//! | `issuerIsins` | `bondIsins*` |
//! | `etfNavRelationships`, `etfNavSnapshot` | `tickers*` (each trimmed) |
//! | `etfNavHistory` | `tickers*` (each trimmed), `startDate` (`""`), `endDate` (`""`) |
//! | `currencyConversion` | `ticker*`, `targetCcy*`, `startDate` (`""`), `endDate` (`""`) |
//!
//! Differences from the JavaScript package, all deliberate fixes:
//!
//! - `bqr` without datetimes works: absent values reach `recipe_bqr` as `None`,
//!   which uses end = now (a UTC instant) and start = end − 1 h. JavaScript
//!   passes `undefined` where napi requires a string and fails.
//! - `volSurface` `preset: null` selects no preset (JavaScript turns it into
//!   `MONEYNESS_30D`); an absent `preset` still means `MONEYNESS_30D`. `preset`
//!   is a name, an array of names or `null`; `fields` is an array of field
//!   names or an object mapping each field to a spec (`metric`, `tenor`,
//!   `pointType`, `point`) or `null`, encoded as described in [`vol`].
//! - `yieldType` outside 1–9 is rejected with the original value instead of
//!   being truncated to a byte first.

pub mod ext;
pub mod vol;

use std::collections::{BTreeMap, HashMap};

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Deserializer, Serialize};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_ext::ExtError;
use xbbg_ext::transforms::fixed_income::YieldType;
use xbbg_recipes::{
    currency, etf, fixed_income, futures, historical, identifiers, indices, volatility,
};

use crate::errors::{HostError, parse_args};

/// Every recipe method name, in the order of the `@xbbg/core` `Engine` class.
pub const METHODS: &[&str] = &[
    "bqr",
    "yas",
    "preferreds",
    "corporateBonds",
    "futTicker",
    "activeFutures",
    "futuresCurve",
    "cdxTicker",
    "activeCdx",
    "dividend",
    "dividendYield",
    "turnover",
    "etfHoldings",
    "volSurface",
    "indexMembers",
    "resolveIsins",
    "resolveVenues",
    "auctionSnapshot",
    "delayStatus",
    "issuerIsins",
    "etfNavRelationships",
    "etfNavSnapshot",
    "etfNavHistory",
    "currencyConversion",
];

/// A validated recipe invocation with every JavaScript default applied.
///
/// Serializes (with a `method` tag) to exactly the values [`RecipeCall::run`]
/// hands to the recipe, so hosts can test argument handling without an engine.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "method", rename_all = "camelCase")]
pub enum RecipeCall {
    Bqr(BqrArgs),
    Yas(YasArgs),
    Preferreds(PreferredsArgs),
    CorporateBonds(CorporateBondsArgs),
    FutTicker(FuturesResolveArgs),
    ActiveFutures(FuturesResolveArgs),
    FuturesCurve(FuturesCurveArgs),
    CdxTicker(CdxTickerArgs),
    ActiveCdx(ActiveCdxArgs),
    Dividend(DividendArgs),
    DividendYield(DividendYieldArgs),
    Turnover(TurnoverArgs),
    EtfHoldings(EtfHoldingsArgs),
    VolSurface(VolSurfaceArgs),
    IndexMembers(IndexMembersArgs),
    ResolveIsins(ResolveIsinsArgs),
    ResolveVenues(ResolveVenuesArgs),
    AuctionSnapshot(AuctionSnapshotArgs),
    DelayStatus(DelayStatusArgs),
    IssuerIsins(IssuerIsinsArgs),
    EtfNavRelationships(EtfNavArgs),
    EtfNavSnapshot(EtfNavArgs),
    EtfNavHistory(EtfNavHistoryArgs),
    CurrencyConversion(CurrencyConversionArgs),
}

/// `bqr(ticker, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BqrArgs {
    pub ticker: String,
    #[serde(default)]
    pub start_datetime: Option<String>,
    #[serde(default)]
    pub end_datetime: Option<String>,
    #[serde(default)]
    pub event_types: Option<Vec<String>>,
    #[serde(default = "yes", deserialize_with = "or_true")]
    pub include_broker_codes: bool,
}

/// `yas(tickers, fields, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct YasArgs {
    pub tickers: Vec<String>,
    pub fields: Vec<String>,
    #[serde(default)]
    pub settle_dt: Option<String>,
    /// Bloomberg `YAS_YLD_FLAG` code; validated by [`RecipeCall::parse`].
    #[serde(default)]
    pub yield_type: Option<i64>,
    #[serde(default)]
    pub spread: Option<f64>,
    #[serde(default)]
    pub yield_val: Option<f64>,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub benchmark: Option<String>,
}

/// `preferreds(equityTicker, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreferredsArgs {
    pub equity_ticker: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}

/// `corporateBonds(ticker, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CorporateBondsArgs {
    pub ticker: String,
    #[serde(default)]
    pub ccy: Option<String>,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}

/// `futTicker(genTicker, dt, options)` and `activeFutures(genTicker, dt, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FuturesResolveArgs {
    pub gen_ticker: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub dt: String,
    #[serde(default)]
    pub freq: Option<String>,
}

/// `futuresCurve(genTicker, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FuturesCurveArgs {
    pub gen_ticker: String,
    #[serde(default)]
    pub asof: Option<String>,
    #[serde(default)]
    pub chain_field: Option<String>,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub max_contracts: Option<i32>,
}

/// `cdxTicker(genTicker, dt, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CdxTickerArgs {
    pub gen_ticker: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub dt: String,
    #[serde(default, deserialize_with = "or_false")]
    pub versionless: bool,
}

/// `activeCdx(genTicker, dt, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveCdxArgs {
    pub gen_ticker: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub dt: String,
    #[serde(default)]
    pub lookback_days: Option<i32>,
    #[serde(default, deserialize_with = "or_false")]
    pub versionless: bool,
}

/// `dividend(tickers, startDate, endDate, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DividendArgs {
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
    #[serde(default)]
    pub dvd_type: Option<String>,
}

/// `dividendYield(tickers, startDate, endDate, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DividendYieldArgs {
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
    #[serde(default)]
    pub dividend_types: Option<Vec<String>>,
    #[serde(default)]
    pub window_days: Option<i32>,
}

/// `turnover(tickers, startDate, endDate, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnoverArgs {
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
    #[serde(default)]
    pub ccy: Option<String>,
    #[serde(default)]
    pub factor: Option<f64>,
}

/// `etfHoldings(etfTicker, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EtfHoldingsArgs {
    pub etf_ticker: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}

/// `volSurface(tickers, startDate, endDate, options)`.
///
/// Input `preset` becomes `presets` and input `fields` becomes the encoded
/// `fieldSpecs` (see [`vol`]).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VolSurfaceArgs {
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
    #[serde(
        rename(deserialize = "preset", serialize = "presets"),
        default = "vol::default_presets",
        deserialize_with = "vol::presets"
    )]
    pub presets: Option<Vec<String>>,
    #[serde(
        rename(deserialize = "fields", serialize = "fieldSpecs"),
        default,
        deserialize_with = "vol::field_specs"
    )]
    pub field_specs: Option<Vec<String>>,
    #[serde(default = "yes", deserialize_with = "or_true")]
    pub as_decimal: bool,
    #[serde(default, deserialize_with = "or_false")]
    pub include_derived: bool,
    #[serde(default)]
    pub risk_free_rate: Option<f64>,
    #[serde(default)]
    pub dividend_yield_field: Option<String>,
}

/// `indexMembers(index, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexMembersArgs {
    pub index: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub asof: Option<String>,
}

/// `resolveIsins(isins, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveIsinsArgs {
    pub isins: Vec<String>,
}

/// `resolveVenues(securities, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveVenuesArgs {
    pub securities: Vec<String>,
    #[serde(default, deserialize_with = "or_empty_map")]
    pub pcs_overrides: BTreeMap<String, String>,
}

/// `auctionSnapshot(securities, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuctionSnapshotArgs {
    pub securities: Vec<String>,
    /// Empty selects the default auction field group natively.
    #[serde(default, deserialize_with = "or_empty_list")]
    pub fields: Vec<String>,
    #[serde(default, deserialize_with = "or_empty_map")]
    pub pcs_overrides: BTreeMap<String, String>,
}

/// `delayStatus(tickers, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DelayStatusArgs {
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_false")]
    pub check_identity: bool,
}

/// `issuerIsins(bondIsins, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IssuerIsinsArgs {
    pub bond_isins: Vec<String>,
}

/// `etfNavRelationships(tickers, options)` and `etfNavSnapshot(tickers, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EtfNavArgs {
    #[serde(deserialize_with = "trimmed")]
    pub tickers: Vec<String>,
}

/// `etfNavHistory(tickers, startDate, endDate, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EtfNavHistoryArgs {
    #[serde(deserialize_with = "trimmed")]
    pub tickers: Vec<String>,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
}

/// `currencyConversion(ticker, targetCcy, startDate, endDate, options)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CurrencyConversionArgs {
    pub ticker: String,
    pub target_ccy: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub start_date: String,
    #[serde(default, deserialize_with = "or_empty")]
    pub end_date: String,
}

impl RecipeCall {
    /// Parse and validate the JSON argument object of recipe `method`.
    ///
    /// Errors: ARGUMENT for a malformed argument object (unknown field, wrong
    /// type, missing required field), VALIDATION for a rejected value, and
    /// INTERNAL for a method that is not a recipe.
    pub fn parse(method: &str, args: &str) -> Result<Self, HostError> {
        Ok(match method {
            "bqr" => Self::Bqr(parse_args(args)?),
            "yas" => {
                let args: YasArgs = parse_args(args)?;
                if let Some(code) = args.yield_type {
                    yield_type(code)?;
                }
                Self::Yas(args)
            }
            "preferreds" => Self::Preferreds(parse_args(args)?),
            "corporateBonds" => Self::CorporateBonds(parse_args(args)?),
            "futTicker" => Self::FutTicker(parse_args(args)?),
            "activeFutures" => Self::ActiveFutures(parse_args(args)?),
            "futuresCurve" => Self::FuturesCurve(parse_args(args)?),
            "cdxTicker" => Self::CdxTicker(parse_args(args)?),
            "activeCdx" => Self::ActiveCdx(parse_args(args)?),
            "dividend" => Self::Dividend(parse_args(args)?),
            "dividendYield" => Self::DividendYield(parse_args(args)?),
            "turnover" => Self::Turnover(parse_args(args)?),
            "etfHoldings" => Self::EtfHoldings(parse_args(args)?),
            "volSurface" => Self::VolSurface(parse_args(args)?),
            "indexMembers" => Self::IndexMembers(parse_args(args)?),
            "resolveIsins" => Self::ResolveIsins(parse_args(args)?),
            "resolveVenues" => Self::ResolveVenues(parse_args(args)?),
            "auctionSnapshot" => Self::AuctionSnapshot(parse_args(args)?),
            "delayStatus" => Self::DelayStatus(parse_args(args)?),
            "issuerIsins" => Self::IssuerIsins(parse_args(args)?),
            "etfNavRelationships" => Self::EtfNavRelationships(parse_args(args)?),
            "etfNavSnapshot" => Self::EtfNavSnapshot(parse_args(args)?),
            "etfNavHistory" => Self::EtfNavHistory(parse_args(args)?),
            "currencyConversion" => Self::CurrencyConversion(parse_args(args)?),
            other => return Err(HostError::unknown_method("engine", other)),
        })
    }

    /// The `@xbbg/core` method name of this call.
    pub fn method(&self) -> &'static str {
        match self {
            Self::Bqr(_) => "bqr",
            Self::Yas(_) => "yas",
            Self::Preferreds(_) => "preferreds",
            Self::CorporateBonds(_) => "corporateBonds",
            Self::FutTicker(_) => "futTicker",
            Self::ActiveFutures(_) => "activeFutures",
            Self::FuturesCurve(_) => "futuresCurve",
            Self::CdxTicker(_) => "cdxTicker",
            Self::ActiveCdx(_) => "activeCdx",
            Self::Dividend(_) => "dividend",
            Self::DividendYield(_) => "dividendYield",
            Self::Turnover(_) => "turnover",
            Self::EtfHoldings(_) => "etfHoldings",
            Self::VolSurface(_) => "volSurface",
            Self::IndexMembers(_) => "indexMembers",
            Self::ResolveIsins(_) => "resolveIsins",
            Self::ResolveVenues(_) => "resolveVenues",
            Self::AuctionSnapshot(_) => "auctionSnapshot",
            Self::DelayStatus(_) => "delayStatus",
            Self::IssuerIsins(_) => "issuerIsins",
            Self::EtfNavRelationships(_) => "etfNavRelationships",
            Self::EtfNavSnapshot(_) => "etfNavSnapshot",
            Self::EtfNavHistory(_) => "etfNavHistory",
            Self::CurrencyConversion(_) => "currencyConversion",
        }
    }

    /// The normalized arguments as JSON text (the serialized form of `self`).
    pub fn to_json(&self) -> Result<String, HostError> {
        serde_json::to_string(self)
            .map_err(|error| HostError::internal(format!("serialize recipe call: {error}")))
    }

    /// Run the recipe on `engine`.
    pub async fn run(self, engine: &Engine) -> Result<RecordBatch, HostError> {
        let result = match self {
            Self::Bqr(args) => {
                fixed_income::recipe_bqr(
                    engine,
                    args.ticker,
                    args.start_datetime,
                    args.end_datetime,
                    args.event_types,
                    args.include_broker_codes,
                    RequestParams::default(),
                )
                .await
            }
            Self::Yas(args) => {
                let yield_type = args.yield_type.map(yield_type).transpose()?;
                fixed_income::recipe_yas(
                    engine,
                    args.tickers,
                    args.fields,
                    args.settle_dt,
                    yield_type,
                    args.spread,
                    args.yield_val,
                    args.price,
                    args.benchmark,
                    RequestParams::default(),
                )
                .await
            }
            Self::Preferreds(args) => {
                fixed_income::recipe_preferreds(
                    engine,
                    args.equity_ticker,
                    args.fields,
                    RequestParams::default(),
                )
                .await
            }
            Self::CorporateBonds(args) => {
                fixed_income::recipe_corporate_bonds(
                    engine,
                    args.ticker,
                    args.ccy,
                    args.fields,
                    RequestParams::default(),
                )
                .await
            }
            Self::FutTicker(args) => {
                futures::recipe_fut_ticker(
                    engine,
                    args.gen_ticker,
                    args.dt,
                    args.freq,
                    RequestParams::default(),
                )
                .await
            }
            Self::ActiveFutures(args) => {
                futures::recipe_active_futures(
                    engine,
                    args.gen_ticker,
                    args.dt,
                    args.freq,
                    RequestParams::default(),
                )
                .await
            }
            Self::FuturesCurve(args) => {
                futures::recipe_futures_curve(
                    engine,
                    args.gen_ticker,
                    args.asof,
                    args.chain_field,
                    args.fields,
                    args.max_contracts,
                )
                .await
            }
            Self::CdxTicker(args) => {
                futures::recipe_cdx_ticker_with_options(
                    engine,
                    args.gen_ticker,
                    args.dt,
                    args.versionless,
                )
                .await
            }
            Self::ActiveCdx(args) => {
                futures::recipe_active_cdx_with_options(
                    engine,
                    args.gen_ticker,
                    args.dt,
                    args.lookback_days,
                    args.versionless,
                )
                .await
            }
            Self::Dividend(args) => {
                historical::recipe_dividend(
                    engine,
                    args.tickers,
                    args.dvd_type,
                    args.start_date,
                    args.end_date,
                    RequestParams::default(),
                )
                .await
            }
            Self::DividendYield(args) => {
                historical::recipe_dividend_yield(
                    engine,
                    args.tickers,
                    args.start_date,
                    args.end_date,
                    args.dividend_types,
                    args.window_days,
                )
                .await
            }
            Self::Turnover(args) => {
                historical::recipe_turnover(
                    engine,
                    args.tickers,
                    args.start_date,
                    args.end_date,
                    args.ccy,
                    args.factor,
                    RequestParams::default(),
                )
                .await
            }
            Self::EtfHoldings(args) => {
                historical::recipe_etf_holdings(
                    engine,
                    args.etf_ticker,
                    args.fields,
                    RequestParams::default(),
                )
                .await
            }
            Self::VolSurface(args) => {
                volatility::recipe_vol_surface(
                    engine,
                    args.tickers,
                    args.start_date,
                    args.end_date,
                    args.presets,
                    args.field_specs,
                    Some(args.as_decimal),
                    Some(args.include_derived),
                    args.risk_free_rate,
                    args.dividend_yield_field,
                )
                .await
            }
            Self::IndexMembers(args) => {
                indices::recipe_index_members(engine, args.index, args.field, args.asof).await
            }
            Self::ResolveIsins(args) => identifiers::recipe_resolve_isins(engine, args.isins).await,
            Self::ResolveVenues(args) => {
                xbbg_recipes::recipe_resolve_venues(
                    engine,
                    args.securities,
                    hash_map(args.pcs_overrides),
                )
                .await
            }
            Self::AuctionSnapshot(args) => {
                xbbg_recipes::recipe_auction_snapshot(
                    engine,
                    args.securities,
                    args.fields,
                    hash_map(args.pcs_overrides),
                )
                .await
            }
            Self::DelayStatus(args) => {
                xbbg_recipes::recipe_delay_status(engine, args.tickers, args.check_identity).await
            }
            Self::IssuerIsins(args) => {
                identifiers::recipe_issuer_isins(engine, args.bond_isins).await
            }
            Self::EtfNavRelationships(args) => {
                etf::recipe_etf_nav_relationships(engine, args.tickers).await
            }
            Self::EtfNavSnapshot(args) => etf::recipe_etf_nav_snapshot(engine, args.tickers).await,
            Self::EtfNavHistory(args) => {
                etf::recipe_etf_nav_history(engine, args.tickers, args.start_date, args.end_date)
                    .await
            }
            Self::CurrencyConversion(args) => {
                currency::recipe_currency_conversion(
                    engine,
                    args.ticker,
                    args.target_ccy,
                    args.start_date,
                    args.end_date,
                )
                .await
            }
        };
        result.map_err(HostError::from_recipe)
    }
}

/// Validate a Bloomberg `YAS_YLD_FLAG` code (1 = YTM … 9 = YTAL).
///
/// Out-of-range codes are VALIDATION errors with the `xbbg-ext` message for the
/// original value, never a truncated byte.
pub fn yield_type(code: i64) -> Result<YieldType, HostError> {
    u8::try_from(code)
        .map_err(|_| ExtError::UnknownYieldType(code.to_string()))
        .and_then(YieldType::try_from)
        .map_err(|error| HostError::validation(error.to_string()).with_element("yieldType"))
}

fn hash_map(map: BTreeMap<String, String>) -> HashMap<String, String> {
    map.into_iter().collect()
}

const fn yes() -> bool {
    true
}

fn or_true<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(true))
}

fn or_false<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(false))
}

fn or_empty<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn or_empty_list<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

fn or_empty_map<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    Ok(Option::<BTreeMap<String, String>>::deserialize(deserializer)?.unwrap_or_default())
}

fn trimmed<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    Ok(Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|ticker| ticker.trim().to_string())
        .collect())
}

#[cfg(test)]
mod tests;
