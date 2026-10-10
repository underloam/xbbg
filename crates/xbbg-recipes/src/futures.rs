//! Futures resolution recipes.
//!
//! These recipes resolve generic futures tickers to specific contract tickers,
//! determine active contracts, and handle CDX series resolution.
//!
//! # Recipes
//!
//! - [`recipe_fut_ticker`]: Resolve generic ticker to specific contract
//! - [`recipe_active_futures`]: Find most active futures contract
//! - [`recipe_cdx_ticker_with_options`]: Resolve CDX series
//! - [`recipe_active_cdx_with_options`]: Find most active CDX series

use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int32Builder, StringBuilder};
use arrow_array::{Array, Date32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Datelike, Duration, NaiveDate};
use xbbg_async::engine::{Engine, ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::resolvers::cdx::{
    build_resolved_cdx_ticker_with_options, cdx_next_series_window, cdx_prior_series_window,
    gen_to_specific, parse_cdx_ticker, ResolvedCdxInfo,
};
use xbbg_ext::resolvers::futures::{
    contract_index, generate_futures_candidates, validate_generic_ticker, RollFrequency,
};
use xbbg_ext::{fmt_date, parse_date, parse_ticker_parts};

use crate::error::{RecipeError, Result};
use crate::utils::{
    array_value_as_date, array_value_as_f64, array_value_as_string, as_string_col, canonical_name,
    date32_to_naive, find_column, naive_to_date32, parse_any_date,
};

/// Resolve a generic futures ticker to a specific contract ticker for a date.
///
/// Generated candidates are resolved using `LAST_TRADEABLE_DT`. If that window
/// cannot supply the requested contract, use Bloomberg's historical
/// `FUT_CHAIN_LAST_TRADE_DATES` chain with `CHAIN_DATE=dt`. Contracts expiring on
/// or before `dt` are excluded, and the remainder are ordered by maturity.
///
/// Returns exactly one non-null UTF-8 `ticker` column and one row. Invalid inputs
/// or an insufficient chain return an error, never an empty ticker.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `gen_ticker` - Generic futures ticker (e.g. `ES1 Index`, `CL2 Comdty`)
/// * `dt` - Reference date accepted by [`parse_date`]
/// * `freq` - Candidate frequency (`Q`/`QE` quarterly; otherwise monthly)
/// * `options` - Request overrides and options applied to every Bloomberg request.
///   Internal reference/history responses use long format; bulk chains keep their native shape.
pub async fn recipe_fut_ticker(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    freq: Option<String>,
    mut options: RequestParams,
) -> Result<RecordBatch> {
    let dt_parsed = parse_date(&dt)?;
    let idx = contract_index(&gen_ticker)?;
    let freq = parse_roll_frequency(freq.as_deref());
    let candidate_tickers = futures_candidate_tickers(&gen_ticker, dt_parsed, freq, idx)?;
    options.format = None;
    let maturities = request_futures_maturities(engine, candidate_tickers, &options).await?;
    let mut valid_contracts = valid_futures_contracts(maturities, dt_parsed);
    if valid_contracts.len() <= idx {
        valid_contracts =
            request_futures_chain_maturities(engine, &gen_ticker, dt_parsed, freq, &options)
                .await?;
    }
    let (selected, _) = select_futures_contract(&valid_contracts, &gen_ticker, &dt, idx)?;

    build_single_ticker_batch(selected)
}

/// Resolve the most active futures contract around a reference date.
///
/// Workflow:
/// 1. Validate generic ticker input and parse the reference date.
/// 2. Query historical `FUT_CUR_GEN_TICKER` up to `dt` and return Bloomberg's
///    own generic mapping when available.
/// 3. Otherwise resolve front and second contracts from one shared candidate
///    maturity request, falling back to the historical futures chain if needed.
/// 4. Keep the front contract before its maturity month, including across years.
/// 5. Within the maturity month, compare each contract's latest non-null `VOLUME`
///    in a 10-calendar-day window. Ties or no observations keep the front.
/// 6. Return the selected active ticker as a single-row `RecordBatch`.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `gen_ticker` - Generic futures ticker (e.g. `ES1 Index`)
/// * `dt` - Reference date accepted by [`parse_date`]
/// * `freq` - Candidate frequency (`Q`/`QE` quarterly; otherwise monthly)
/// * `options` - Request overrides and options applied to every Bloomberg request.
///   Internal reference/history responses use long format; bulk chains keep their native shape.
pub async fn recipe_active_futures(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    freq: Option<String>,
    mut options: RequestParams,
) -> Result<RecordBatch> {
    validate_generic_ticker(&gen_ticker)?;
    let dt_parsed = parse_date(&dt)?;
    options.format = None;

    if let Ok(Some(mapped_ticker)) =
        bloomberg_current_generic_ticker(engine, &gen_ticker, dt_parsed, &options).await
    {
        return build_single_ticker_batch(mapped_ticker);
    }

    let front_gen = with_generic_index(&gen_ticker, 1)?;
    let second_gen = with_generic_index(&gen_ticker, 2)?;
    let second_idx = contract_index(&second_gen)?;
    let freq = parse_roll_frequency(freq.as_deref());
    let front_candidate_count = futures_candidate_count(&front_gen, 0)?;
    let candidate_tickers = futures_candidate_tickers(&front_gen, dt_parsed, freq, second_idx)?;
    let front_candidate_tickers = candidate_tickers
        .iter()
        .take(front_candidate_count)
        .cloned()
        .collect::<Vec<_>>();
    let maturities = request_futures_maturities(engine, candidate_tickers, &options).await?;
    let front_contracts =
        valid_futures_contracts_for_candidates(&maturities, &front_candidate_tickers, dt_parsed);
    let second_contracts = valid_futures_contracts(maturities, dt_parsed);
    let mut pair = select_active_futures_pair(
        &front_contracts,
        &second_contracts,
        &front_gen,
        &second_gen,
        &dt,
        second_idx,
    );
    if pair.as_ref().map_or(true, |(_, second)| second.is_none()) {
        if let Ok(chain) =
            request_futures_chain_maturities(engine, &front_gen, dt_parsed, freq, &options).await
        {
            // A missing bulk field must not discard an otherwise usable front.
            if chain.len() > second_idx || pair.is_err() {
                pair = select_active_futures_pair(
                    &chain,
                    &chain,
                    &front_gen,
                    &second_gen,
                    &dt,
                    second_idx,
                );
            }
        }
    }
    let ((front_ticker, front_maturity), second_contract) = pair?;
    let Some((second_ticker, _)) = second_contract else {
        return build_single_ticker_batch(front_ticker);
    };

    if before_futures_roll_month(dt_parsed, front_maturity) {
        return build_single_ticker_batch(front_ticker);
    }

    let volume_params = futures_history_request(
        vec![front_ticker.clone(), second_ticker.clone()],
        "VOLUME",
        dt_parsed,
        &options,
    );
    let volume_batch = engine.request(volume_params).await?;
    active_futures_volume_batch(&volume_batch, front_ticker, second_ticker)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FuturesCurveRow {
    source_ticker: String,
    contract_ticker: String,
    generic_number: i32,
    px_bid: Option<f64>,
    px_ask: Option<f64>,
    last_tradeable_dt: Option<NaiveDate>,
    mid: Option<f64>,
    annualized_carry: Option<f64>,
    extra_values: Vec<Option<String>>,
}

/// Build a futures chain table with contract metadata, mid, and annualized carry.
pub async fn recipe_futures_curve(
    engine: &Engine,
    gen_ticker: String,
    asof: Option<String>,
    chain_field: Option<String>,
    fields: Option<Vec<String>>,
    max_contracts: Option<i32>,
) -> Result<RecordBatch> {
    validate_generic_ticker(&gen_ticker)?;
    let chain_field = chain_field.unwrap_or_else(|| "FUT_CHAIN_LAST_TRADE_DATES".to_string());

    let overrides = match asof {
        Some(date) => Some(vec![(
            "CHAIN_DATE".to_string(),
            fmt_date(parse_date(&date)?, None),
        )]),
        None => None,
    };
    let chain_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(vec![gen_ticker.clone()]),
        fields: Some(vec![chain_field.clone()]),
        overrides,
        ..Default::default()
    };
    let chain_batch = engine.request(chain_params).await?;
    let mut contracts = extract_chain_contracts(&chain_batch, max_contracts)?;
    if contracts.is_empty() {
        return Err(RecipeError::Other(format!(
            "Bloomberg returned no futures contracts for '{gen_ticker}' using field '{chain_field}'"
        )));
    }

    let mut request_fields = fields.unwrap_or_else(|| {
        vec![
            "PX_BID".to_string(),
            "PX_ASK".to_string(),
            "LAST_TRADEABLE_DT".to_string(),
        ]
    });
    ensure_field(&mut request_fields, "PX_BID");
    ensure_field(&mut request_fields, "PX_ASK");
    ensure_field(&mut request_fields, "LAST_TRADEABLE_DT");

    let metadata_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(
            contracts
                .iter()
                .map(|contract| contract.ticker.clone())
                .collect(),
        ),
        fields: Some(request_fields.clone()),
        ..Default::default()
    };
    let metadata_batch = engine.request(metadata_params).await?;
    let metadata = extract_refdata_values(&metadata_batch)?;

    for contract in &mut contracts {
        if contract.expiry.is_none() {
            contract.expiry = metadata
                .get(&contract.ticker)
                .and_then(|fields| fields.get("LAST_TRADEABLE_DT"))
                .and_then(|value| parse_any_date(value));
        }
    }

    contracts.sort_by(|left, right| {
        left.expiry
            .cmp(&right.expiry)
            .then(left.order.cmp(&right.order))
            .then(left.ticker.cmp(&right.ticker))
    });

    let extra_fields = request_fields
        .iter()
        .filter(|field| {
            !matches!(
                field.to_ascii_uppercase().as_str(),
                "PX_BID" | "PX_ASK" | "LAST_TRADEABLE_DT"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let rows = build_futures_curve_rows(&gen_ticker, &contracts, &metadata, &extra_fields);
    build_futures_curve_batch(&rows, &extra_fields)
}

/// Bloomberg field carrying a CDS index series' first accrual date.
///
/// This is the authoritative start of a series: the series that applies on a
/// date is the highest one whose accrual date is on or before it.
const CDX_ACCRUAL_FIELD: &str = "CDS_FIRST_ACCRUAL_START_DATE";

/// Resolve a generic CDX ticker (`GEN`) to the series that applies on a date.
///
/// Pass `versionless=false` to include the explicit version, including V1.
/// `versionless` affects only the returned ticker; resolution still requires
/// Bloomberg `VERSION` metadata.
pub async fn recipe_cdx_ticker_with_options(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    versionless: bool,
) -> Result<RecordBatch> {
    let resolved = resolve_cdx_series_at(engine, &gen_ticker, parse_date(&dt)?).await?;
    build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
        &resolved.info,
        versionless,
    ))
}

/// Resolve the latest CDX series that had started *and* traded by a date.
///
/// Identical to [`recipe_cdx_ticker_with_options`] except between a roll and the
/// new series' first print, when the preceding series is still the traded one.
/// CDX.NA.HY.46 started 2026-03-20 but first printed 2026-03-27, so those five
/// business days resolve to S45.
///
/// Pass `versionless=false` to include the explicit version.
/// `versionless` affects only the selected output ticker.
pub async fn recipe_active_cdx_with_options(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    lookback_days: Option<i32>,
    versionless: bool,
) -> Result<RecordBatch> {
    let dt_parsed = parse_date(&dt)?;
    let current = resolve_cdx_series_at(engine, &gen_ticker, dt_parsed).await?;

    let Some(previous_alias) = current.info.previous_alias() else {
        return build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
            &current.info,
            versionless,
        ));
    };
    let previous = resolve_specific_cdx(engine, &previous_alias).await?;
    let current_ticker = build_resolved_cdx_ticker_with_options(&current.info, false);
    let previous_ticker = build_resolved_cdx_ticker_with_options(&previous, false);

    let start_date = cdx_activity_window_start(dt_parsed, lookback_days, current.accrual_start)?;
    let price_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(vec![current_ticker.clone(), previous_ticker.clone()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some(fmt_date(start_date, None)),
        end_date: Some(fmt_date(dt_parsed, None)),
        ..Default::default()
    };

    let price_batch = engine.request(price_params).await?;
    let selected = if has_history_value(&price_batch, &current_ticker, "PX_LAST")? {
        &current.info
    } else if has_history_value(&price_batch, &previous_ticker, "PX_LAST")? {
        &previous
    } else {
        return Err(RecipeError::Other(format!(
            "neither '{current_ticker}' nor '{previous_ticker}' reported PX_LAST between {} and {}",
            fmt_date(start_date, None),
            fmt_date(dt_parsed, None)
        )));
    };

    build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
        selected,
        versionless,
    ))
}

/// Start of the window used to decide whether a series has traded.
///
/// The window always reaches back to the series' first accrual date, so "has
/// this series printed yet" can only ever flip false -> true as `dt` advances.
/// A purely sliding window would let a series drop out of view after a quiet
/// stretch and send the answer backwards.
fn cdx_activity_window_start(
    dt: NaiveDate,
    lookback_days: Option<i32>,
    accrual_start: NaiveDate,
) -> Result<NaiveDate> {
    let lookback_days = i64::from(lookback_days.unwrap_or(10).max(1));
    let lookback = Duration::try_days(lookback_days).ok_or_else(|| {
        RecipeError::InvalidArgument(format!("lookback_days={lookback_days} is out of range"))
    })?;
    let windowed = dt.checked_sub_signed(lookback).ok_or_else(|| {
        RecipeError::InvalidArgument(format!(
            "lookback_days={lookback_days} is out of range for {dt}"
        ))
    })?;
    Ok(windowed.min(accrual_start))
}

/// A CDX series resolved against a reference date, with the accrual date that
/// makes it the applicable series.
struct CdxSeriesAt {
    info: ResolvedCdxInfo,
    accrual_start: NaiveDate,
}

/// Resolve the CDX series whose accrual period contains `dt`.
///
/// The answer is `max { N : first_accrual(N) <= dt }`, read from Bloomberg's
/// `CDS_FIRST_ACCRUAL_START_DATE`. `ROLLING_SERIES` on the generic ticker is
/// point-in-time -- it reports today's series whatever `dt` is -- so it only
/// anchors the top of the search. The semi-annual roll cadence only sizes the
/// candidate window, which is why business-day-adjusted rolls resolve exactly:
/// CDX.NA.IG.45 starts 2025-09-22, so 2025-09-21 still resolves to S44.
///
/// Monotone in `dt` by construction: accrual dates are fixed contract terms, so
/// a later date can never resolve to an earlier series.
async fn resolve_cdx_series_at(
    engine: &Engine,
    gen_ticker: &str,
    dt: NaiveDate,
) -> Result<CdxSeriesAt> {
    let parsed = parse_cdx_ticker(gen_ticker)?;
    if !parsed.is_generic {
        return Err(RecipeError::InvalidArgument(format!(
            "'{gen_ticker}' must be a generic CDX ticker containing GEN"
        )));
    }

    let batch = request_cdx_metadata(engine, &[gen_ticker.to_string()]).await?;
    let series = required_cdx_number(&batch, gen_ticker, "ROLLING_SERIES")?;
    let accrual_start = required_cdx_date(&batch, gen_ticker, CDX_ACCRUAL_FIELD)?;

    if dt >= accrual_start {
        let version = required_cdx_number(&batch, gen_ticker, "VERSION")?;
        return Ok(CdxSeriesAt {
            info: ResolvedCdxInfo::resolve(parsed, series, version)?,
            accrual_start,
        });
    }

    let mut window = cdx_prior_series_window(series, accrual_start, dt).ok_or_else(|| {
        RecipeError::InvalidArgument(format!(
            "{dt} precedes S{series}, the first series of '{gen_ticker}'"
        ))
    })?;

    loop {
        let (lo, hi) = window;
        if let Some(resolved) = resolve_cdx_ladder(engine, gen_ticker, lo, hi, dt).await? {
            return Ok(resolved);
        }
        window = cdx_next_series_window(lo).ok_or_else(|| {
            RecipeError::InvalidArgument(format!("no '{gen_ticker}' series had started by {dt}"))
        })?;
    }
}

/// Highest series in `lo..=hi` that had started by `dt`, or `None` when the
/// whole window starts later.
///
/// Walks down from `hi`. Every series above the answer must report an accrual
/// date, and those dates must strictly decrease -- a hole or an out-of-order
/// date could hide a later-starting series and silently return an older one.
async fn resolve_cdx_ladder(
    engine: &Engine,
    gen_ticker: &str,
    lo: u32,
    hi: u32,
    dt: NaiveDate,
) -> Result<Option<CdxSeriesAt>> {
    let aliases = (lo..=hi)
        .map(|series| Ok(gen_to_specific(gen_ticker, series)?))
        .collect::<Result<Vec<String>>>()?;
    let batch = request_cdx_metadata(engine, &aliases).await?;
    pick_cdx_ladder_series(&batch, gen_ticker, &aliases, lo, dt)
}

/// Decide which rung of a fetched ladder applies on `dt`.
///
/// Split from the request so the ordering and completeness rules are testable
/// without a Bloomberg session. `aliases` is ordered by ascending series,
/// starting at `lo`.
fn pick_cdx_ladder_series(
    batch: &RecordBatch,
    gen_ticker: &str,
    aliases: &[String],
    lo: u32,
    dt: NaiveDate,
) -> Result<Option<CdxSeriesAt>> {
    let mut above: Option<(u32, NaiveDate)> = None;

    for (offset, alias) in aliases.iter().enumerate().rev() {
        let series = lo + offset as u32;
        let accrual_start = required_cdx_date(batch, alias, CDX_ACCRUAL_FIELD)?;

        if let Some((above_series, above_accrual)) = above {
            if accrual_start >= above_accrual {
                return Err(RecipeError::Other(format!(
                    "'{gen_ticker}' S{series} starts {accrual_start}, not before S{above_series} at {above_accrual}"
                )));
            }
        }
        above = Some((series, accrual_start));

        if accrual_start <= dt {
            let version = required_cdx_number(batch, alias, "VERSION")?;
            return Ok(Some(CdxSeriesAt {
                info: ResolvedCdxInfo::resolve(parse_cdx_ticker(alias)?, series, version)?,
                accrual_start,
            }));
        }
    }

    Ok(None)
}

async fn request_cdx_metadata(engine: &Engine, securities: &[String]) -> Result<RecordBatch> {
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(securities.to_vec()),
        fields: Some(vec![
            "ROLLING_SERIES".to_string(),
            "VERSION".to_string(),
            CDX_ACCRUAL_FIELD.to_string(),
        ]),
        ..Default::default()
    };
    Ok(engine.request(params).await?)
}

async fn resolve_specific_cdx(engine: &Engine, ticker: &str) -> Result<ResolvedCdxInfo> {
    let parsed = parse_cdx_ticker(ticker)?;
    if parsed.is_generic {
        return Err(RecipeError::InvalidArgument(format!(
            "'{ticker}' must be a specific CDX ticker"
        )));
    }
    let series = parsed
        .series_num
        .ok_or_else(|| RecipeError::Other(format!("missing parsed CDX series for '{ticker}'")))?;
    let batch = request_cdx_metadata(engine, &[ticker.to_string()]).await?;
    let version = required_cdx_number(&batch, ticker, "VERSION")?;
    ResolvedCdxInfo::resolve(parsed, series, version).map_err(Into::into)
}

fn required_cdx_number(batch: &RecordBatch, ticker: &str, field: &str) -> Result<u32> {
    let raw = extract_refdata_string_for_ticker(batch, ticker, field)?.ok_or_else(|| {
        RecipeError::Other(format!(
            "missing {field} for '{ticker}' in Bloomberg response"
        ))
    })?;
    parse_series_number(&raw).ok_or_else(|| {
        RecipeError::Other(format!("unable to parse {field}='{raw}' for '{ticker}'"))
    })
}

fn required_cdx_date(batch: &RecordBatch, ticker: &str, field: &str) -> Result<NaiveDate> {
    extract_refdata_date_for_ticker(batch, ticker, field)?.ok_or_else(|| {
        RecipeError::Other(format!(
            "missing {field} for '{ticker}' in Bloomberg response"
        ))
    })
}

fn has_history_value(batch: &RecordBatch, ticker: &str, field: &str) -> Result<bool> {
    Ok(latest_history_numeric_point(batch, ticker, field)?.is_some())
}

fn parse_roll_frequency(freq: Option<&str>) -> RollFrequency {
    use std::str::FromStr;
    freq.and_then(|value| RollFrequency::from_str(value).ok())
        .unwrap_or(RollFrequency::Monthly)
}

fn futures_candidate_tickers(
    gen_ticker: &str,
    dt: NaiveDate,
    freq: RollFrequency,
    highest_index: usize,
) -> Result<Vec<String>> {
    let count = futures_candidate_count(gen_ticker, highest_index)?;
    let candidates = generate_futures_candidates(gen_ticker, dt, freq, count)?;
    if candidates.is_empty() {
        return Err(RecipeError::Other(format!(
            "no futures candidates generated for '{gen_ticker}'"
        )));
    }
    Ok(candidates
        .into_iter()
        .map(|candidate| candidate.ticker)
        .collect())
}

fn futures_maturity_request(
    candidate_tickers: Vec<String>,
    options: &RequestParams,
) -> RequestParams {
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(candidate_tickers),
        fields: Some(vec!["LAST_TRADEABLE_DT".to_string()]),
        format: Some("long".to_string()),
        ..Default::default()
    };
    crate::utils::apply_request_options(&mut params, options);
    params
}

async fn request_futures_maturities(
    engine: &Engine,
    candidate_tickers: Vec<String>,
    options: &RequestParams,
) -> Result<Vec<(String, NaiveDate)>> {
    let maturity_batch = engine
        .request(futures_maturity_request(candidate_tickers, options))
        .await?;
    extract_refdata_date_values(&maturity_batch, "LAST_TRADEABLE_DT")
}

fn futures_chain_request(
    gen_ticker: &str,
    dt: NaiveDate,
    options: &RequestParams,
) -> RequestParams {
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(vec![gen_ticker.to_string()]),
        fields: Some(vec!["FUT_CHAIN_LAST_TRADE_DATES".to_string()]),
        overrides: Some(vec![("CHAIN_DATE".to_string(), fmt_date(dt, None))]),
        ..Default::default()
    };
    crate::utils::apply_request_options(&mut params, options);
    params.format = None;
    params
}

async fn request_futures_chain_maturities(
    engine: &Engine,
    gen_ticker: &str,
    dt: NaiveDate,
    freq: RollFrequency,
    options: &RequestParams,
) -> Result<Vec<(String, NaiveDate)>> {
    let batch = engine
        .request(futures_chain_request(gen_ticker, dt, options))
        .await?;
    extract_futures_chain_maturities(&batch, dt, freq)
}

fn extract_futures_chain_maturities(
    batch: &RecordBatch,
    dt: NaiveDate,
    freq: RollFrequency,
) -> Result<Vec<(String, NaiveDate)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let ticker_name = find_column(
        batch,
        &[
            "future's ticker",
            "futures ticker",
            "future ticker",
            "security description",
            "contract ticker",
        ],
    )
    .or_else(|| find_column(batch, &["ticker"]))
    .ok_or_else(|| RecipeError::Other("futures chain is missing contract tickers".to_string()))?;
    let expiry_name = find_chain_expiry_column(batch)
        .or_else(|| find_column(batch, &["date"]))
        .ok_or_else(|| RecipeError::Other("futures chain is missing expiry dates".to_string()))?;
    let ticker_col = batch.column_by_name(&ticker_name).expect("matched column");
    let expiry_col = batch.column_by_name(&expiry_name).expect("matched column");
    let contracts = (0..batch.num_rows())
        .filter_map(|row| {
            let ticker = array_value_as_string(ticker_col, row)?;
            let trimmed = ticker.trim();
            if trimmed.is_empty() {
                return None;
            }
            if freq == RollFrequency::Quarterly {
                let contract = trimmed.split_whitespace().next()?;
                let root_month = contract.trim_end_matches(|c: char| c.is_ascii_digit());
                if root_month.len() == contract.len()
                    || !matches!(
                        root_month.as_bytes().last(),
                        Some(b'H' | b'M' | b'U' | b'Z')
                    )
                {
                    return None;
                }
            }
            let expiry = array_value_as_date(expiry_col, row)?;
            let ticker = if trimmed.len() == ticker.len() {
                ticker
            } else {
                trimmed.to_string()
            };
            Some((ticker, expiry))
        })
        .collect();
    Ok(valid_futures_contracts(contracts, dt))
}

fn futures_history_request(
    securities: Vec<String>,
    field: &str,
    dt: NaiveDate,
    options: &RequestParams,
) -> RequestParams {
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(securities),
        fields: Some(vec![field.to_string()]),
        start_date: Some(fmt_date(dt - Duration::days(10), None)),
        end_date: Some(fmt_date(dt, None)),
        format: Some("long".to_string()),
        ..Default::default()
    };
    crate::utils::apply_request_options(&mut params, options);
    params
}

fn before_futures_roll_month(dt: NaiveDate, maturity: NaiveDate) -> bool {
    (dt.year(), dt.month()) < (maturity.year(), maturity.month())
}

fn active_futures_volume_batch(
    batch: &RecordBatch,
    front_ticker: String,
    second_ticker: String,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return build_single_ticker_batch(front_ticker);
    }
    let front_volume = latest_history_numeric_point(batch, &front_ticker, "VOLUME")?;
    let second_volume = latest_history_numeric_point(batch, &second_ticker, "VOLUME")?;
    let selected = match (front_volume, second_volume) {
        (Some((_, f)), Some((_, s))) if s > f => second_ticker,
        (Some(_), Some(_)) => front_ticker,
        (Some(_), None) => front_ticker,
        (None, Some(_)) => second_ticker,
        (None, None) => front_ticker,
    };
    build_single_ticker_batch(selected)
}

fn valid_futures_contracts(
    mut contracts: Vec<(String, NaiveDate)>,
    dt: NaiveDate,
) -> Vec<(String, NaiveDate)> {
    contracts.retain(|(_, maturity)| *maturity > dt);
    contracts.sort_by_key(|(_, maturity)| *maturity);
    contracts
}

fn valid_futures_contracts_for_candidates(
    contracts: &[(String, NaiveDate)],
    candidate_tickers: &[String],
    dt: NaiveDate,
) -> Vec<(String, NaiveDate)> {
    let candidate_tickers = candidate_tickers
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    valid_futures_contracts(
        contracts
            .iter()
            .filter(|(ticker, _)| candidate_tickers.contains(ticker.as_str()))
            .cloned()
            .collect(),
        dt,
    )
}

fn select_futures_contract(
    valid_contracts: &[(String, NaiveDate)],
    gen_ticker: &str,
    dt: &str,
    idx: usize,
) -> Result<(String, NaiveDate)> {
    valid_contracts.get(idx).cloned().ok_or_else(|| {
        RecipeError::Other(format!(
            "unable to resolve futures contract for '{gen_ticker}' on {dt}: requested index {} but only {} valid maturities",
            idx + 1,
            valid_contracts.len()
        ))
    })
}

type DatedFuturesContract = (String, NaiveDate);

fn select_active_futures_pair(
    front_contracts: &[DatedFuturesContract],
    second_contracts: &[DatedFuturesContract],
    front_gen: &str,
    second_gen: &str,
    dt: &str,
    second_idx: usize,
) -> Result<(DatedFuturesContract, Option<DatedFuturesContract>)> {
    let front = select_futures_contract(front_contracts, front_gen, dt, 0)?;
    let second = select_futures_contract(second_contracts, second_gen, dt, second_idx).ok();
    Ok((front, second))
}

async fn bloomberg_current_generic_ticker(
    engine: &Engine,
    gen_ticker: &str,
    dt: NaiveDate,
    options: &RequestParams,
) -> Result<Option<String>> {
    let params = futures_history_request(
        vec![gen_ticker.to_string()],
        "FUT_CUR_GEN_TICKER",
        dt,
        options,
    );

    let batch = engine.request(params).await?;
    let Some((_, mapped_root)) =
        latest_history_string_point(&batch, gen_ticker, "FUT_CUR_GEN_TICKER")?
    else {
        return Ok(None);
    };

    normalize_mapped_generic_ticker(&mapped_root, gen_ticker)
}

fn normalize_mapped_generic_ticker(raw: &str, gen_ticker: &str) -> Result<Option<String>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    if raw.split_whitespace().count() > 1 {
        return Ok(Some(raw.to_string()));
    }

    let parts = parse_ticker_parts(gen_ticker)?;
    let ticker = match parts.asset.as_str() {
        "Equity" => {
            let exchange = parts.exchange.unwrap_or_else(|| "US".to_string());
            format!("{raw} {exchange} {}", parts.asset)
        }
        _ => format!("{raw} {}", parts.asset),
    };

    Ok(Some(ticker))
}

fn futures_candidate_count(gen_ticker: &str, idx: usize) -> Result<usize> {
    let parts = parse_ticker_parts(gen_ticker)?;
    let month_ext = if parts.asset == "Comdty" { 4 } else { 2 };
    Ok(std::cmp::max(idx + month_ext, 3))
}

fn with_generic_index(gen_ticker: &str, index: u32) -> Result<String> {
    let parts = parse_ticker_parts(gen_ticker)?;

    let ticker = match parts.asset.as_str() {
        "Equity" => {
            let exchange = parts.exchange.unwrap_or_else(|| "US".to_string());
            format!("{}{} {} {}", parts.prefix, index, exchange, parts.asset)
        }
        _ => format!("{}{} {}", parts.prefix, index, parts.asset),
    };

    Ok(ticker)
}

#[derive(Debug, Clone, PartialEq)]
struct ChainContract {
    order: usize,
    ticker: String,
    expiry: Option<NaiveDate>,
}

fn ensure_field(fields: &mut Vec<String>, required: &str) {
    if !fields
        .iter()
        .any(|field| field.eq_ignore_ascii_case(required))
    {
        fields.push(required.to_string());
    }
}

fn extract_chain_contracts(
    batch: &RecordBatch,
    max_contracts: Option<i32>,
) -> Result<Vec<ChainContract>> {
    let contract_col_name = find_contract_column(batch).ok_or_else(|| {
        RecipeError::Other(
            "futures chain response did not include a contract ticker column".to_string(),
        )
    })?;
    let contract_col = batch
        .column_by_name(&contract_col_name)
        .ok_or_else(|| RecipeError::Other(format!("missing '{contract_col_name}' column")))?;
    let expiry_col = find_chain_expiry_column(batch).and_then(|name| batch.column_by_name(&name));
    let limit = max_contracts
        .and_then(|value| (value > 0).then_some(value as usize))
        .unwrap_or(usize::MAX);

    let mut seen = std::collections::HashSet::new();
    let mut contracts = Vec::new();
    for row in 0..batch.num_rows() {
        if contracts.len() >= limit {
            break;
        }
        let Some(raw_ticker) = crate::utils::array_value_as_string(contract_col, row) else {
            continue;
        };
        let ticker = raw_ticker.trim();
        if ticker.is_empty() || !seen.insert(ticker.to_string()) {
            continue;
        }
        let expiry = expiry_col.and_then(|col| crate::utils::array_value_as_date(col, row));
        contracts.push(ChainContract {
            order: contracts.len(),
            ticker: ticker.to_string(),
            expiry,
        });
    }

    Ok(contracts)
}

fn find_contract_column(batch: &RecordBatch) -> Option<String> {
    let candidates = [
        "future's ticker",
        "futures ticker",
        "future ticker",
        "security description",
        "contract ticker",
        "ticker",
    ];
    let wanted = candidates
        .iter()
        .map(|candidate| canonical_name(candidate))
        .collect::<Vec<_>>();
    batch
        .schema()
        .fields()
        .iter()
        .find_map(|field| {
            let name = field.name();
            if name == "ticker" || name == "field" {
                return None;
            }
            let key = canonical_name(name);
            wanted
                .iter()
                .any(|candidate| candidate == &key)
                .then(|| name.to_string())
        })
        .or_else(|| {
            batch.schema().fields().iter().find_map(|field| {
                let name = field.name();
                (name != "ticker" && name != "field").then(|| name.to_string())
            })
        })
}

fn find_chain_expiry_column(batch: &RecordBatch) -> Option<String> {
    find_column(
        batch,
        &[
            "last trade date",
            "last tradeable dt",
            "last_tradeable_dt",
            "expiry",
            "expiration date",
        ],
    )
}

fn extract_refdata_values(
    batch: &RecordBatch,
) -> Result<std::collections::HashMap<String, std::collections::HashMap<String, String>>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let mut out = std::collections::HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        let Some(value) = array_value_as_string(value_col, row) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.entry(ticker_col.value(row).to_string())
            .or_insert_with(std::collections::HashMap::new)
            .insert(field_col.value(row).to_ascii_uppercase(), value.to_string());
    }

    Ok(out)
}

fn build_futures_curve_rows(
    source_ticker: &str,
    contracts: &[ChainContract],
    metadata: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    extra_fields: &[String],
) -> Vec<FuturesCurveRow> {
    let mut rows = Vec::with_capacity(contracts.len());
    let mut previous_mid: Option<f64> = None;
    let mut previous_expiry: Option<NaiveDate> = None;

    for (idx, contract) in contracts.iter().enumerate() {
        let fields = metadata.get(&contract.ticker);
        let px_bid = fields
            .and_then(|map| map.get("PX_BID"))
            .and_then(|value| crate::utils::parse_f64_like(value));
        let px_ask = fields
            .and_then(|map| map.get("PX_ASK"))
            .and_then(|value| crate::utils::parse_f64_like(value));
        let expiry = contract.expiry.or_else(|| {
            fields
                .and_then(|map| map.get("LAST_TRADEABLE_DT"))
                .and_then(|value| parse_any_date(value))
        });
        let mid = match (px_bid, px_ask) {
            (Some(bid), Some(ask)) if bid.is_finite() && ask.is_finite() => Some((bid + ask) / 2.0),
            _ => None,
        };
        let annualized_carry = match (previous_mid, mid, previous_expiry, expiry) {
            (Some(prev_mid), Some(cur_mid), Some(prev_expiry), Some(cur_expiry))
                if prev_mid != 0.0 && cur_expiry > prev_expiry =>
            {
                let year_delta = (cur_expiry - prev_expiry).num_days() as f64 / 365.25;
                (year_delta > 0.0).then_some((cur_mid / prev_mid - 1.0) / year_delta)
            }
            _ => None,
        };
        let extra_values = extra_fields
            .iter()
            .map(|field| {
                fields
                    .and_then(|map| map.get(&field.to_ascii_uppercase()))
                    .cloned()
            })
            .collect::<Vec<_>>();

        rows.push(FuturesCurveRow {
            source_ticker: source_ticker.to_string(),
            contract_ticker: contract.ticker.clone(),
            generic_number: (idx + 1) as i32,
            px_bid,
            px_ask,
            last_tradeable_dt: expiry,
            mid,
            annualized_carry,
            extra_values,
        });

        if let Some(value) = mid {
            previous_mid = Some(value);
        }
        if let Some(value) = expiry {
            previous_expiry = Some(value);
        }
    }

    rows
}

fn build_futures_curve_batch(
    rows: &[FuturesCurveRow],
    extra_fields: &[String],
) -> Result<RecordBatch> {
    let mut source = StringBuilder::new();
    let mut contract = StringBuilder::new();
    let mut generic = Int32Builder::new();
    let mut bid = Float64Builder::new();
    let mut ask = Float64Builder::new();
    let mut expiry = arrow_array::builder::Date32Builder::new();
    let mut mid = Float64Builder::new();
    let mut carry = Float64Builder::new();
    let mut extra_builders = extra_fields
        .iter()
        .map(|_| StringBuilder::new())
        .collect::<Vec<_>>();

    for row in rows {
        source.append_value(&row.source_ticker);
        contract.append_value(&row.contract_ticker);
        generic.append_value(row.generic_number);
        bid.append_option(row.px_bid);
        ask.append_option(row.px_ask);
        expiry.append_option(row.last_tradeable_dt.map(naive_to_date32));
        mid.append_option(row.mid);
        carry.append_option(row.annualized_carry);
        for (builder, value) in extra_builders.iter_mut().zip(row.extra_values.iter()) {
            builder.append_option(value.as_deref());
        }
    }

    let mut schema_fields = vec![
        Field::new("source_ticker", DataType::Utf8, false),
        Field::new("contract_ticker", DataType::Utf8, false),
        Field::new("generic_number", DataType::Int32, false),
        Field::new("px_bid", DataType::Float64, true),
        Field::new("px_ask", DataType::Float64, true),
        Field::new("last_tradeable_dt", DataType::Date32, true),
        Field::new("mid", DataType::Float64, true),
        Field::new("annualized_carry", DataType::Float64, true),
    ];
    for field in extra_fields {
        schema_fields.push(Field::new(canonical_name(field), DataType::Utf8, true));
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(source.finish()),
        Arc::new(contract.finish()),
        Arc::new(generic.finish()),
        Arc::new(bid.finish()),
        Arc::new(ask.finish()),
        Arc::new(expiry.finish()),
        Arc::new(mid.finish()),
        Arc::new(carry.finish()),
    ];
    for mut builder in extra_builders {
        columns.push(Arc::new(builder.finish()));
    }

    let schema = Arc::new(Schema::new(schema_fields));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn build_single_ticker_batch(ticker: String) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ticker",
        DataType::Utf8,
        false,
    )]));
    let ticker_array = StringArray::from(vec![ticker]);
    RecordBatch::try_new(schema, vec![Arc::new(ticker_array)]).map_err(Into::into)
}

fn extract_refdata_string_for_ticker(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<String>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(raw) = array_value_as_string(value_col, row) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }

        return Ok(Some(raw.to_string()));
    }

    Ok(None)
}

fn extract_refdata_date_values(
    batch: &RecordBatch,
    field: &str,
) -> Result<Vec<(String, NaiveDate)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let mut output = Vec::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(parsed) = array_value_as_date(value_col, row) else {
            continue;
        };

        output.push((ticker_col.value(row).to_string(), parsed));
    }

    Ok(output)
}

fn extract_refdata_date_for_ticker(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<NaiveDate>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) {
            continue;
        }
        if ticker_col.value(row) == ticker && field_col.value(row).eq_ignore_ascii_case(field) {
            if let Some(date) = array_value_as_date(value_col, row) {
                return Ok(Some(date));
            }
            // Preserve the metadata rule: the first nonblank value is
            // authoritative even when it is not a parseable date.
            if array_value_as_string(value_col, row).is_some_and(|raw| !raw.trim().is_empty()) {
                return Ok(None);
            }
        }
    }
    Ok(None)
}

fn latest_history_numeric_point(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<(NaiveDate, f64)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let date32_col = date_col.as_any().downcast_ref::<Date32Array>();
    let date_str_col = date_col.as_any().downcast_ref::<StringArray>();

    let mut best: Option<(NaiveDate, f64)> = None;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(value) = array_value_as_f64(value_col, row) else {
            continue;
        };

        let row_date = if let Some(col) = date32_col {
            if col.is_null(row) {
                None
            } else {
                date32_to_naive(col.value(row))
            }
        } else if let Some(col) = date_str_col {
            if col.is_null(row) {
                None
            } else {
                parse_any_date(col.value(row))
            }
        } else {
            None
        };

        let Some(row_date) = row_date else {
            continue;
        };

        match best {
            Some((best_date, _)) if row_date < best_date => {}
            _ => best = Some((row_date, value)),
        }
    }

    Ok(best)
}

fn latest_history_string_point(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<(NaiveDate, String)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let date32_col = date_col.as_any().downcast_ref::<Date32Array>();
    let date_str_col = date_col.as_any().downcast_ref::<StringArray>();

    let mut best: Option<(NaiveDate, String)> = None;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(raw_value) = array_value_as_string(value_col, row) else {
            continue;
        };
        let raw_value = raw_value.trim();
        if raw_value.is_empty() {
            continue;
        }

        let row_date = if let Some(col) = date32_col {
            if col.is_null(row) {
                None
            } else {
                date32_to_naive(col.value(row))
            }
        } else if let Some(col) = date_str_col {
            if col.is_null(row) {
                None
            } else {
                parse_any_date(col.value(row))
            }
        } else {
            None
        };

        let Some(row_date) = row_date else {
            continue;
        };

        match best {
            Some((best_date, _)) if row_date < best_date => {}
            _ => best = Some((row_date, raw_value.to_string())),
        }
    }

    Ok(best)
}

fn parse_series_number(value: &str) -> Option<u32> {
    let value = value.trim();

    if let Ok(v) = value.parse::<u32>() {
        return (v > 0).then_some(v);
    }

    let parsed = value.parse::<f64>().ok()?;
    if !parsed.is_finite() || parsed < 1.0 || parsed > f64::from(u32::MAX) || parsed.fract() != 0.0
    {
        return None;
    }

    Some(parsed as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_single_ticker(batch: &RecordBatch, ticker: &str) {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(as_string_col(batch, "ticker").unwrap().value(0), ticker);
    }

    #[test]
    fn test_futures_requests_keep_recipe_identity_and_forward_options() {
        let dt = NaiveDate::from_ymd_opt(2024, 3, 12).unwrap();
        let options = RequestParams {
            service: "ignored service".to_string(),
            operation: "ignored operation".to_string(),
            securities: Some(vec!["ignored security".to_string()]),
            fields: Some(vec!["ignored field".to_string()]),
            overrides: Some(vec![("PRICING_SOURCE".to_string(), "BGN".to_string())]),
            security_overrides: Some(vec![(
                "ESH24 Index".to_string(),
                vec![("CRNCY".to_string(), "USD".to_string())],
            )]),
            elements: Some(vec![(
                "periodicitySelection".to_string(),
                "DAILY".to_string(),
            )]),
            options: Some(vec![(
                "nonTradingDayFillOption".to_string(),
                "ACTIVE_DAYS_ONLY".to_string(),
            )]),
            kwargs: Some(std::collections::HashMap::from([(
                "Currency".to_string(),
                "USD".to_string(),
            )])),
            field_types: Some(std::collections::HashMap::from([(
                "VOLUME".to_string(),
                "float64".to_string(),
            )])),
            return_eids: true,
            validate_fields: Some(false),
            request_tz: Some("UTC".to_string()),
            output_tz: Some("Europe/London".to_string()),
            ..Default::default()
        };
        let candidates = vec!["ESH24 Index".to_string(), "ESM24 Index".to_string()];
        let maturity = futures_maturity_request(candidates.clone(), &options);
        let chain = futures_chain_request("ES1 Index", dt, &options);
        let mapping = futures_history_request(
            vec!["ES1 Index".to_string()],
            "FUT_CUR_GEN_TICKER",
            dt,
            &options,
        );
        let volume = futures_history_request(candidates.clone(), "VOLUME", dt, &options);

        for params in [&maturity, &chain, &mapping, &volume] {
            assert_eq!(params.service, Service::RefData.to_string());
            assert_eq!(params.elements, options.elements);
            assert_eq!(params.options, options.options);
            assert_eq!(params.kwargs, options.kwargs);
            assert_eq!(params.field_types, options.field_types);
            assert_eq!(params.validate_fields, Some(false));
            assert!(params.return_eids);
            assert_eq!(params.request_tz, options.request_tz);
            assert_eq!(params.output_tz, options.output_tz);
            assert_eq!(
                params.format.as_deref(),
                if params.extractor == ExtractorType::BulkData {
                    None
                } else {
                    Some("long")
                }
            );
            assert!(params
                .overrides
                .as_ref()
                .unwrap()
                .contains(&("PRICING_SOURCE".to_string(), "BGN".to_string())));
        }
        assert_eq!(maturity.security_overrides, options.security_overrides);
        assert_eq!(volume.security_overrides, options.security_overrides);
        assert!(chain.security_overrides.is_none());
        assert!(mapping.security_overrides.is_none());
        assert_eq!(maturity.operation, Operation::ReferenceData.to_string());
        assert_eq!(maturity.securities.as_ref().unwrap(), &candidates);
        assert_eq!(maturity.fields.as_ref().unwrap(), &["LAST_TRADEABLE_DT"]);
        assert_eq!(chain.operation, Operation::ReferenceData.to_string());
        assert_eq!(chain.securities.as_ref().unwrap(), &["ES1 Index"]);
        assert_eq!(
            chain.fields.as_ref().unwrap(),
            &["FUT_CHAIN_LAST_TRADE_DATES"]
        );
        assert_eq!(chain.extractor, ExtractorType::BulkData);
        assert!(chain.extractor_set);
        assert!(chain
            .overrides
            .as_ref()
            .unwrap()
            .contains(&("CHAIN_DATE".to_string(), "20240312".to_string())));
        assert_eq!(mapping.fields.as_ref().unwrap(), &["FUT_CUR_GEN_TICKER"]);
        assert_eq!(mapping.securities.as_ref().unwrap(), &["ES1 Index"]);
        assert_eq!(volume.fields.as_ref().unwrap(), &["VOLUME"]);
        assert_eq!(volume.securities.as_ref().unwrap(), &candidates);
        for history in [&mapping, &volume] {
            assert_eq!(history.operation, Operation::HistoricalData.to_string());
            assert_eq!(history.start_date.as_deref(), Some("20240302"));
            assert_eq!(history.end_date.as_deref(), Some("20240312"));
        }
    }

    #[test]
    fn test_futures_chain_request_allows_caller_override_of_chain_date() {
        let options = RequestParams {
            overrides: Some(vec![("CHAIN_DATE".to_string(), "20240112".to_string())]),
            format: Some("wide".to_string()),
            ..Default::default()
        };
        let params = futures_chain_request(
            "ES1 Index",
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            &options,
        );
        assert_eq!(params.overrides, options.overrides);
        assert!(
            params.format.is_none(),
            "BulkData requests cannot specify an output format"
        );
    }

    #[test]
    fn test_futures_chain_labels_dates_and_index_shape_native_result() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for (ticker_label, expiry_label) in [
            ("Future's Ticker", "Last Trade Date"),
            ("future's_ticker", "last_trade_date"),
            ("Futures Ticker", "LAST_TRADEABLE_DT"),
            ("Security Description", "date"),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new("field", DataType::Utf8, false),
                Field::new(ticker_label, DataType::Utf8, true),
                Field::new(expiry_label, DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["ES1 Index"; 7])),
                    Arc::new(StringArray::from(vec!["FUT_CHAIN_LAST_TRADE_DATES"; 7])),
                    Arc::new(StringArray::from(vec![
                        Some(" ESM24 Index "),
                        Some("ESF24 Index"),
                        Some("ESH24 Index"),
                        None,
                        Some(""),
                        Some("invalid expiry"),
                        Some("null expiry"),
                    ])),
                    Arc::new(StringArray::from(vec![
                        Some("2024-06-21T16:30:00"),
                        Some("2024-01-15"),
                        Some("20240315"),
                        Some("2024-02-01"),
                        Some("2024-02-01"),
                        Some("N/A"),
                        None,
                    ])),
                ],
            )
            .unwrap();
            let contracts =
                extract_futures_chain_maturities(&batch, dt, RollFrequency::Monthly).unwrap();
            assert_eq!(
                contracts,
                vec![
                    (
                        "ESH24 Index".to_string(),
                        NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
                    ),
                    (
                        "ESM24 Index".to_string(),
                        NaiveDate::from_ymd_opt(2024, 6, 21).unwrap(),
                    ),
                ]
            );
            let (selected, _) =
                select_futures_contract(&contracts, "ES2 Index", "20240115", 1).unwrap();
            assert_single_ticker(&build_single_ticker_batch(selected).unwrap(), "ESM24 Index");
            assert!(select_futures_contract(&contracts, "ES3 Index", "20240115", 2).is_err());
        }
    }

    #[test]
    fn test_futures_chain_accepts_date32_and_empty_response() {
        let expiry = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("Last Trade Date", DataType::Date32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ESH24 Index"), None])),
                Arc::new(Date32Array::from(vec![Some(naive_to_date32(expiry)), None])),
            ],
        )
        .unwrap();
        assert_eq!(
            extract_futures_chain_maturities(&batch, dt, RollFrequency::Monthly).unwrap(),
            vec![("ESH24 Index".to_string(), expiry)]
        );
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(
            extract_futures_chain_maturities(&empty, dt, RollFrequency::Monthly)
                .unwrap()
                .is_empty()
        );
        let missing_expiry = batch.project(&[0]).unwrap();
        assert!(
            extract_futures_chain_maturities(&missing_expiry, dt, RollFrequency::Monthly).is_err()
        );
    }

    #[test]
    fn test_quarterly_chain_uses_contract_code_not_expiry_month() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("Future's Ticker", DataType::Utf8, false),
                Field::new("Last Trade Date", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    "ABCG24 Index",
                    "ABCH24 Index",
                    "ABCJ24 Index",
                    "ABCM24 Index",
                ])),
                Arc::new(StringArray::from(vec![
                    "2024-01-29",
                    "2024-02-27",
                    "2024-03-28",
                    "2024-05-29",
                ])),
            ],
        )
        .unwrap();
        let monthly = extract_futures_chain_maturities(&batch, dt, RollFrequency::Monthly).unwrap();
        assert_eq!(monthly.len(), 4);
        let quarterly =
            extract_futures_chain_maturities(&batch, dt, RollFrequency::Quarterly).unwrap();
        assert_eq!(
            quarterly
                .iter()
                .map(|(ticker, _)| ticker.as_str())
                .collect::<Vec<_>>(),
            vec!["ABCH24 Index", "ABCM24 Index"]
        );
        let pair = select_active_futures_pair(
            &quarterly,
            &quarterly,
            "ABC1 Index",
            "ABC2 Index",
            "20240115",
            1,
        )
        .unwrap();
        assert_eq!(pair.0 .0, "ABCH24 Index");
        assert_eq!(pair.1.unwrap().0, "ABCM24 Index");
    }

    #[test]
    fn test_native_futures_frequency_defaults_and_validation() {
        assert_eq!(parse_roll_frequency(None), RollFrequency::Monthly);
        assert_eq!(
            parse_roll_frequency(Some("unexpected")),
            RollFrequency::Monthly
        );
        assert_eq!(parse_roll_frequency(Some(" qE ")), RollFrequency::Quarterly);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for ticker in [
            "UXZ5 Index",
            "UXZ24 Index",
            "ESH24 Index",
            "SPYH24 US Equity",
        ] {
            assert!(validate_generic_ticker(ticker).is_err());
            assert!(futures_candidate_tickers(ticker, dt, RollFrequency::Monthly, 0).is_err());
        }
        assert!(contract_index("invalid").is_err());
        assert_eq!(contract_index("ES2 Index").unwrap(), 1);
        let candidates =
            futures_candidate_tickers("ES1 Index", dt, RollFrequency::Quarterly, 0).unwrap();
        assert_eq!(candidates, ["ESH24 Index", "ESM24 Index", "ESU24 Index"]);
        assert!(parse_date("not-a-date").is_err());
    }

    #[test]
    fn test_active_futures_roll_month_comparison_crosses_year_end() {
        let january = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        assert!(before_futures_roll_month(
            NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(),
            january,
        ));
        assert!(!before_futures_roll_month(
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            january,
        ));
    }

    #[test]
    fn test_active_futures_volume_shapes_latest_valid_observation_as_one_ticker() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("date", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "ESH24 Index",
                    "ESH24 Index",
                    "ESH24 Index",
                    "ESM24 Index",
                    "ESM24 Index",
                ])),
                Arc::new(StringArray::from(vec![
                    "VOLUME", "VOLUME", "VOLUME", "volume", "PX_LAST",
                ])),
                Arc::new(StringArray::from(vec![
                    "20240308", "20240312", "20240311", "20240311", "20240312",
                ])),
                Arc::new(arrow_array::Float64Array::from(vec![
                    Some(500.0),
                    None,
                    Some(100.0),
                    Some(200.0),
                    Some(1.0),
                ])),
            ],
        )
        .unwrap();
        let result = active_futures_volume_batch(
            &batch,
            "ESH24 Index".to_string(),
            "ESM24 Index".to_string(),
        )
        .unwrap();
        assert_single_ticker(&result, "ESM24 Index");
    }

    #[test]
    fn test_active_futures_volume_ties_and_missing_observations() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let dt = naive_to_date32(NaiveDate::from_ymd_opt(2024, 3, 12).unwrap());
        for (front, second, expected) in [
            (Some("100"), Some("100"), "ESH24 Index"),
            (Some("100"), None, "ESH24 Index"),
            (None, Some("100"), "ESM24 Index"),
            (None, None, "ESH24 Index"),
        ] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["ESH24 Index", "ESM24 Index"])),
                    Arc::new(StringArray::from(vec!["VOLUME"; 2])),
                    Arc::new(Date32Array::from(vec![dt; 2])),
                    Arc::new(StringArray::from(vec![front, second])),
                ],
            )
            .unwrap();
            let result = active_futures_volume_batch(
                &batch,
                "ESH24 Index".to_string(),
                "ESM24 Index".to_string(),
            )
            .unwrap();
            assert_single_ticker(&result, expected);
        }
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert_single_ticker(
            &active_futures_volume_batch(
                &empty,
                "ESH24 Index".to_string(),
                "ESM24 Index".to_string(),
            )
            .unwrap(),
            "ESH24 Index",
        );
    }

    #[test]
    fn test_required_cdx_date_reads_date32_without_string_conversion() {
        let dt = NaiveDate::from_ymd_opt(2025, 9, 22).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Date32, false),
        ]));
        let ticker = "CDX IG CDSI S45 5Y Corp";
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ticker])),
                Arc::new(StringArray::from(vec![CDX_ACCRUAL_FIELD])),
                Arc::new(Date32Array::from(vec![naive_to_date32(dt)])),
            ],
        )
        .unwrap();
        assert_eq!(
            required_cdx_date(&batch, ticker, CDX_ACCRUAL_FIELD).unwrap(),
            dt
        );
    }

    #[test]
    fn test_cdx_date_keeps_first_nonblank_metadata_value_authoritative() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let ticker = "CDX IG CDSI S45 5Y Corp";
        for (first, expected) in [("N/A", None), ("", NaiveDate::from_ymd_opt(2025, 9, 22))] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![ticker; 2])),
                    Arc::new(StringArray::from(vec![CDX_ACCRUAL_FIELD; 2])),
                    Arc::new(StringArray::from(vec![first, "2025-09-22"])),
                ],
            )
            .unwrap();
            assert_eq!(
                extract_refdata_date_for_ticker(&batch, ticker, CDX_ACCRUAL_FIELD).unwrap(),
                expected,
            );
        }
    }

    #[test]
    fn test_futures_candidate_count_rules() {
        let idx0 = futures_candidate_count("ES1 Index", 0).unwrap();
        let idx2 = futures_candidate_count("CL3 Comdty", 2).unwrap();

        assert_eq!(idx0, 3);
        assert_eq!(idx2, 6);
    }

    #[test]
    fn test_active_futures_pair_uses_sorted_valid_maturities_and_front_fallback() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let valid = valid_futures_contracts(
            vec![
                (
                    "ESM24 Index".to_string(),
                    NaiveDate::from_ymd_opt(2024, 6, 21).unwrap(),
                ),
                (
                    "ESF24 Index".to_string(),
                    NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
                ),
                (
                    "ESH24 Index".to_string(),
                    NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
                ),
            ],
            dt,
        );

        let (front, second) =
            select_active_futures_pair(&valid, &valid, "ES1 Index", "ES2 Index", "20240115", 1)
                .unwrap();
        assert_eq!(front.0, "ESH24 Index");
        assert_eq!(front.1, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
        assert_eq!(second.unwrap().0, "ESM24 Index");

        let (front, second) = select_active_futures_pair(
            &valid[..1],
            &valid[..1],
            "ES1 Index",
            "ES2 Index",
            "20240115",
            1,
        )
        .unwrap();
        assert_eq!(front.0, "ESH24 Index");
        assert_eq!(second, None);

        let error =
            select_active_futures_pair(&[], &valid, "ES1 Index", "ES2 Index", "20240115", 1)
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Recipe error: unable to resolve futures contract for 'ES1 Index' on 20240115: \
             requested index 1 but only 0 valid maturities"
        );
    }

    #[test]
    fn test_shared_maturity_lookup_preserves_front_candidate_scope() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let maturities = vec![
            (
                "ESN24 Index".to_string(),
                NaiveDate::from_ymd_opt(2024, 2, 15).unwrap(),
            ),
            (
                "ESH24 Index".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            ),
            (
                "ESM24 Index".to_string(),
                NaiveDate::from_ymd_opt(2024, 6, 21).unwrap(),
            ),
        ];
        let front_candidates = vec!["ESH24 Index".to_string(), "ESM24 Index".to_string()];

        let front = valid_futures_contracts_for_candidates(&maturities, &front_candidates, dt);
        let second = valid_futures_contracts(maturities, dt);

        assert_eq!(front[0].0, "ESH24 Index");
        assert_eq!(second[0].0, "ESN24 Index");
    }

    #[test]
    fn test_with_generic_index_preserves_asset_shape() {
        assert_eq!(with_generic_index("ES1 Index", 2).unwrap(), "ES2 Index");
        assert_eq!(
            with_generic_index("SPY1 US Equity", 2).unwrap(),
            "SPY2 US Equity"
        );
    }

    #[test]
    fn test_normalize_mapped_generic_ticker_adds_asset_suffix() {
        assert_eq!(
            normalize_mapped_generic_ticker("UXK6", "UX1 Index")
                .unwrap()
                .unwrap(),
            "UXK6 Index"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("CLM6", "CL1 Comdty")
                .unwrap()
                .unwrap(),
            "CLM6 Comdty"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("SPYH6", "SPY1 US Equity")
                .unwrap()
                .unwrap(),
            "SPYH6 US Equity"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("UXK6 Index", "UX1 Index")
                .unwrap()
                .unwrap(),
            "UXK6 Index"
        );
    }

    #[test]
    fn test_extract_refdata_date_values_parses_iso_dates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ESH24 Index", "ESM24 Index"])),
                Arc::new(StringArray::from(vec![
                    "LAST_TRADEABLE_DT",
                    "LAST_TRADEABLE_DT",
                ])),
                Arc::new(StringArray::from(vec!["2024-03-15", "2024-06-21"])),
            ],
        )
        .unwrap();

        let parsed = extract_refdata_date_values(&batch, "LAST_TRADEABLE_DT").unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "ESH24 Index");
        assert_eq!(parsed[0].1, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
    }

    #[test]
    fn test_extract_refdata_date_values_accepts_typed_date_value_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Date32, false),
        ]));
        let d1 = (NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 6, 21).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ESH24 Index", "ESM24 Index"])),
                Arc::new(StringArray::from(vec![
                    "LAST_TRADEABLE_DT",
                    "LAST_TRADEABLE_DT",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2)])),
            ],
        )
        .unwrap();

        let parsed = extract_refdata_date_values(&batch, "LAST_TRADEABLE_DT").unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "ESH24 Index");
        assert_eq!(parsed[0].1, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
    }

    #[test]
    fn test_latest_history_numeric_point_uses_latest_non_null_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "ESH24 Index",
                    "ESH24 Index",
                    "ESM24 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec!["VOLUME", "VOLUME", "VOLUME"])),
                Arc::new(StringArray::from(vec!["100", "250", "175"])),
            ],
        )
        .unwrap();

        let latest = latest_history_numeric_point(&batch, "ESH24 Index", "VOLUME")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(latest.1, 250.0);
    }

    #[test]
    fn test_latest_history_numeric_point_accepts_typed_value_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "UXK24 Index",
                    "UXK24 Index",
                    "UXM24 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec!["VOLUME", "VOLUME", "VOLUME"])),
                Arc::new(arrow_array::Float64Array::from(vec![
                    Some(100.0),
                    Some(250.0),
                    Some(300.0),
                ])),
            ],
        )
        .unwrap();

        let latest = latest_history_numeric_point(&batch, "UXK24 Index", "VOLUME")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(latest.1, 250.0);
    }

    #[test]
    fn test_latest_history_string_point_uses_latest_mapping_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2026, 4, 15).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2026, 4, 16).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "UX1 Index",
                    "UX1 Index",
                    "UX2 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec![
                    "FUT_CUR_GEN_TICKER",
                    "FUT_CUR_GEN_TICKER",
                    "FUT_CUR_GEN_TICKER",
                ])),
                Arc::new(StringArray::from(vec!["UXJ6", "UXK6", "UXM6"])),
            ],
        )
        .unwrap();

        let latest = latest_history_string_point(&batch, "UX1 Index", "FUT_CUR_GEN_TICKER")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2026, 4, 16).unwrap());
        assert_eq!(latest.1, "UXK6");
    }

    /// Real CDX.NA.IG accrual dates, including the business-day-adjusted rolls
    /// that a nominal 20-Mar/20-Sep cadence would get wrong.
    const IG_LADDER: &[(u32, &str)] = &[
        (42, "2024-03-20"),
        (43, "2024-09-20"),
        (44, "2025-03-20"),
        (45, "2025-09-22"),
    ];

    fn ig_alias(series: u32) -> String {
        format!("CDX IG CDSI S{series} 5Y Corp")
    }

    fn ladder_batch(rungs: &[(u32, &str)], skip_accrual: Option<u32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let mut tickers = Vec::new();
        let mut fields = Vec::new();
        let mut values = Vec::new();
        for (series, accrual) in rungs {
            if skip_accrual != Some(*series) {
                tickers.push(ig_alias(*series));
                fields.push(CDX_ACCRUAL_FIELD.to_string());
                values.push((*accrual).to_string());
            }
            tickers.push(ig_alias(*series));
            fields.push("VERSION".to_string());
            values.push("1".to_string());
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tickers)),
                Arc::new(StringArray::from(fields)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    fn pick_ig_ladder(dt: &str, skip_accrual: Option<u32>) -> Result<Option<u32>> {
        let aliases: Vec<String> = IG_LADDER.iter().map(|(n, _)| ig_alias(*n)).collect();
        let batch = ladder_batch(IG_LADDER, skip_accrual);
        let dt = NaiveDate::parse_from_str(dt, "%Y-%m-%d").unwrap();
        Ok(pick_cdx_ladder_series(
            &batch,
            "CDX IG CDSI GEN 5Y Corp",
            &aliases,
            IG_LADDER[0].0,
            dt,
        )?
        .map(|found| found.info.series()))
    }

    #[test]
    fn test_pick_cdx_ladder_series_uses_actual_accrual_dates() {
        // S45 starts 2025-09-22, so the nominal 20 September roll is still S44.
        assert_eq!(pick_ig_ladder("2025-09-19", None).unwrap(), Some(44));
        assert_eq!(pick_ig_ladder("2025-09-21", None).unwrap(), Some(44));
        assert_eq!(pick_ig_ladder("2025-09-22", None).unwrap(), Some(45));
        assert_eq!(pick_ig_ladder("2024-06-01", None).unwrap(), Some(42));
        assert_eq!(pick_ig_ladder("2026-07-27", None).unwrap(), Some(45));
    }

    #[test]
    fn test_pick_cdx_ladder_series_reports_window_starting_after_date() {
        assert_eq!(pick_ig_ladder("2024-01-01", None).unwrap(), None);
    }

    #[test]
    fn test_pick_cdx_ladder_series_rejects_hole_above_the_match() {
        // A missing rung above the match could hide a later-starting series.
        assert!(pick_ig_ladder("2024-06-01", Some(44)).is_err());
        // Below the match it cannot change the answer, so it is never read.
        assert_eq!(pick_ig_ladder("2025-09-22", Some(42)).unwrap(), Some(45));
    }

    #[test]
    fn test_pick_cdx_ladder_series_rejects_non_decreasing_accrual_dates() {
        // S44 claiming a later start than S45 means the walk down can no longer
        // trust that the first qualifying rung is the highest one.
        let scrambled: &[(u32, &str)] = &[(44, "2026-09-01"), (45, "2026-08-01")];
        let aliases: Vec<String> = scrambled.iter().map(|(n, _)| ig_alias(*n)).collect();
        let batch = ladder_batch(scrambled, None);
        let dt = NaiveDate::from_ymd_opt(2026, 7, 1).unwrap();
        assert!(
            pick_cdx_ladder_series(&batch, "CDX IG CDSI GEN 5Y Corp", &aliases, 44, dt).is_err()
        );
    }

    #[test]
    fn test_parse_series_number_accepts_int_and_decimal_text() {
        // Bloomberg returns ROLLING_SERIES and VERSION as decimal text.
        assert_eq!(parse_series_number("45"), Some(45));
        assert_eq!(parse_series_number("45.0"), Some(45));
        assert_eq!(parse_series_number("45.5"), None);
        assert_eq!(parse_series_number("abc"), None);
        assert_eq!(parse_series_number("4294967296.0"), None);
    }

    #[test]
    fn test_cdx_activity_window_start_reaches_back_to_accrual_start() {
        let dt = NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        let accrual = NaiveDate::from_ymd_opt(2026, 3, 20).unwrap();

        // The window always covers the series' whole life, never just the
        // trailing lookback, so "has it traded yet" cannot flip back to false.
        assert_eq!(
            cdx_activity_window_start(dt, None, accrual).unwrap(),
            accrual
        );
        // A lookback reaching further back than the accrual date still wins.
        assert_eq!(
            cdx_activity_window_start(dt, Some(365), accrual).unwrap(),
            dt - Duration::days(365)
        );
        assert!(cdx_activity_window_start(dt, Some(i32::MAX), accrual).is_err());
    }

    #[test]
    fn test_required_cdx_date_requires_present_metadata() {
        let batch = ladder_batch(&[(45, "2025-09-22")], None);
        assert_eq!(
            required_cdx_date(&batch, &ig_alias(45), CDX_ACCRUAL_FIELD).unwrap(),
            NaiveDate::from_ymd_opt(2025, 9, 22).unwrap()
        );
        assert!(required_cdx_date(&batch, &ig_alias(44), CDX_ACCRUAL_FIELD).is_err());
    }

    #[test]
    fn test_required_cdx_number_requires_present_positive_metadata() {
        let ticker = "CDX HY CDSI GEN 5Y Corp";
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ticker])),
                Arc::new(StringArray::from(vec!["VERSION"])),
                Arc::new(StringArray::from(vec!["2"])),
            ],
        )
        .unwrap();

        assert_eq!(required_cdx_number(&batch, ticker, "VERSION").unwrap(), 2);
        assert!(required_cdx_number(&batch, ticker, "ROLLING_SERIES").is_err());
        assert_eq!(parse_series_number("0"), None);
    }

    #[test]
    fn test_futures_curve_rows_compute_mid_and_carry() {
        let front_expiry = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let second_expiry = NaiveDate::from_ymd_opt(2024, 6, 21).unwrap();
        let contracts = vec![
            ChainContract {
                order: 0,
                ticker: "ESH24 Index".to_string(),
                expiry: Some(front_expiry),
            },
            ChainContract {
                order: 1,
                ticker: "ESM24 Index".to_string(),
                expiry: Some(second_expiry),
            },
        ];
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "ESH24 Index".to_string(),
            std::collections::HashMap::from([
                ("PX_BID".to_string(), "4999".to_string()),
                ("PX_ASK".to_string(), "5001".to_string()),
            ]),
        );
        metadata.insert(
            "ESM24 Index".to_string(),
            std::collections::HashMap::from([
                ("PX_BID".to_string(), "5049".to_string()),
                ("PX_ASK".to_string(), "5051".to_string()),
            ]),
        );

        let rows = build_futures_curve_rows("ES1 Index", &contracts, &metadata, &[]);

        assert_eq!(rows[0].generic_number, 1);
        assert_eq!(rows[0].mid, Some(5000.0));
        assert_eq!(rows[0].annualized_carry, None);
        assert_eq!(rows[1].mid, Some(5050.0));
        assert!(rows[1].annualized_carry.unwrap() > 0.0);
    }

    #[test]
    fn test_futures_curve_missing_bid_or_ask_keeps_mid_null() {
        let contracts = vec![ChainContract {
            order: 0,
            ticker: "ESH24 Index".to_string(),
            expiry: Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()),
        }];
        let metadata = std::collections::HashMap::from([(
            "ESH24 Index".to_string(),
            std::collections::HashMap::from([("PX_BID".to_string(), "4999".to_string())]),
        )]);

        let rows = build_futures_curve_rows("ES1 Index", &contracts, &metadata, &[]);

        assert_eq!(rows[0].mid, None);
        assert_eq!(rows[0].annualized_carry, None);
    }
}
