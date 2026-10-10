//! ETF NAV / iNAV relationship recipes.
//!
//! Resolves Bloomberg's authoritative `ETF_NAV_TICKER` (FD251) and
//! `ETF_INAV_TICKER` (FD249) relationship fields instead of guessing
//! `NV`/`IV` suffix conventions, then validates each returned target as a
//! genuine Index security before serving values.
//!
//! # Recipes
//!
//! - [`recipe_etf_nav_relationships`]: One row per input ETF with the
//!   normalized, validated NAV and iNAV Index targets.
//! - [`recipe_etf_nav_snapshot`]: Current NAV/iNAV levels; mapped targets use
//!   `PX_LAST`, ETFs without a daily NAV target fall back to the source
//!   fund's `FUND_NET_ASSET_VAL`.
//! - [`recipe_etf_nav_history`]: Daily NAV/iNAV history over a date range
//!   with the same per-relationship source selection.
//!
//! NAV and iNAV are independently nullable: a missing relationship is `None`
//! without affecting the other leg, and relationship values are normalized to
//! exactly one trailing ` Index` token. Tickers are never derived from the
//! source ETF symbol.

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::hash_map::Entry as HashEntry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{Date32Builder, Float64Builder, Int32Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use xbbg_async::engine::state::{
    FieldExceptionMeta, SecurityErrorMeta, METADATA_KEY_FIELD_EXCEPTIONS,
    METADATA_KEY_SECURITY_ERRORS,
};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::{fmt_date, parse_date};

use crate::error::{RecipeError, Result};
use crate::utils::{
    array_value_as_date, array_value_as_f64, array_value_as_string, as_string_col,
    clean_bloomberg_text, naive_to_date32, parse_f64_like,
};

const FIELD_ETF_NAV_TICKER: &str = "ETF_NAV_TICKER";
const FIELD_ETF_INAV_TICKER: &str = "ETF_INAV_TICKER";
const FIELD_MARKET_SECTOR_DES: &str = "MARKET_SECTOR_DES";
const FIELD_NAME: &str = "NAME";
const FIELD_PX_LAST: &str = "PX_LAST";
const FIELD_FUND_NET_ASSET_VAL: &str = "FUND_NET_ASSET_VAL";

/// Which fields the shared resolver requests when validating targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolveMode {
    /// Identity fields only (`MARKET_SECTOR_DES`, `NAME`).
    Relationships,
    /// Identity fields plus `PX_LAST` so the snapshot reuses one request.
    Snapshot,
}

/// One normalized relationship target with validation state.
#[derive(Clone, Debug, PartialEq)]
struct TargetResolution {
    ticker: String,
    market_sector_des: Option<String>,
    name: Option<String>,
    validation_error: Option<String>,
    px_last: Option<f64>,
}

/// Per-input resolution; `nav` and `inav` are independently nullable.
#[derive(Clone, Debug, PartialEq)]
struct EtfNavResolution {
    input_order: i32,
    etf_ticker: String,
    nav: Option<TargetResolution>,
    inav: Option<TargetResolution>,
}

/// Normalized relationship tickers returned by Bloomberg for one source ETF.
#[derive(Clone, Debug, Default, PartialEq)]
struct RelationshipTargets {
    nav: Option<String>,
    inav: Option<String>,
}

/// Security errors and field exceptions decoded from batch schema metadata.
#[derive(Debug, Default)]
struct ResponseDiagnostics {
    security_errors: BTreeMap<String, SecurityErrorMeta>,
    field_exceptions: BTreeMap<String, Vec<FieldExceptionMeta>>,
}

/// One output row of the daily history recipe.
#[derive(Clone, Debug, PartialEq)]
struct HistoryRow {
    input_order: i32,
    etf_ticker: String,
    date: NaiveDate,
    nav_ticker: Option<String>,
    nav_value: Option<f64>,
    nav_source_ticker: String,
    nav_source_field: &'static str,
    inav_ticker: Option<String>,
    inav_value: Option<f64>,
}

type RefValues = HashMap<String, HashMap<String, String>>;
type HistorySeries = HashMap<(String, String), BTreeMap<NaiveDate, f64>>;

/// Resolve NAV/iNAV relationship targets for `etfs`.
///
/// Emits one row per input in input order with the exact schema
/// `input_order`, `etf_ticker`, `nav_ticker`, `nav_market_sector_des`,
/// `nav_name`, `nav_validation_error`, `inav_ticker`,
/// `inav_market_sector_des`, `inav_name`, `inav_validation_error`.
/// A missing relationship leaves its ticker, identity, and validation
/// columns all null.
pub async fn recipe_etf_nav_relationships(
    engine: &Engine,
    etfs: Vec<String>,
) -> Result<RecordBatch> {
    if etfs.is_empty() {
        return build_relationships_batch(&[]);
    }
    let resolutions =
        resolve_etf_nav_relationships(engine, &etfs, ResolveMode::Relationships).await?;
    build_relationships_batch(&resolutions)
}

/// Fetch current NAV/iNAV levels for `etfs`.
///
/// Mapped targets reuse `PX_LAST` from the resolver's validation request;
/// ETFs without a daily NAV relationship fall back to one additional BDP for
/// the source fund's `FUND_NET_ASSET_VAL`. Any target validation error
/// aborts before value requests are issued.
pub async fn recipe_etf_nav_snapshot(engine: &Engine, etfs: Vec<String>) -> Result<RecordBatch> {
    if etfs.is_empty() {
        return build_snapshot_batch(&[], &HashMap::new());
    }
    let resolutions = resolve_etf_nav_relationships(engine, &etfs, ResolveMode::Snapshot).await?;
    ensure_targets_valid(&resolutions)?;

    let fallback_sources = missing_nav_sources(&resolutions);
    let mut fallback_values: HashMap<String, f64> = HashMap::new();
    if !fallback_sources.is_empty() {
        let batch = engine
            .request(build_snapshot_fallback_request(fallback_sources.clone()))
            .await?;
        let diagnostics = parse_response_diagnostics(&batch)?;
        check_value_source_diagnostics(&fallback_sources, &diagnostics)?;
        let values = strict_refdata_value_map(&batch)?;
        for source in &fallback_sources {
            let value = values
                .get(source)
                .and_then(|fields| fields.get(FIELD_FUND_NET_ASSET_VAL))
                .and_then(|value| parse_f64_like(value));
            if let Some(value) = value {
                fallback_values.insert(source.clone(), value);
            }
        }
    }
    build_snapshot_batch(&resolutions, &fallback_values)
}

/// Fetch daily NAV/iNAV history for `etfs` between `start_date` and
/// `end_date` (inclusive).
///
/// Issues at most two daily `HistoricalDataRequest`s: one `PX_LAST` request
/// over the union of valid NAV then iNAV targets and one
/// `FUND_NET_ASSET_VAL` request over source ETFs without a daily NAV target.
/// Each ETF emits the sorted union of dates observed on its NAV source or
/// iNAV target; calendar dates are never synthesized.
pub async fn recipe_etf_nav_history(
    engine: &Engine,
    etfs: Vec<String>,
    start_date: String,
    end_date: String,
) -> Result<RecordBatch> {
    let (start, end) = parse_history_range(&start_date, &end_date)?;
    if etfs.is_empty() {
        return build_history_batch(&[]);
    }
    let resolutions =
        resolve_etf_nav_relationships(engine, &etfs, ResolveMode::Relationships).await?;
    ensure_targets_valid(&resolutions)?;

    let px_targets = px_history_targets(&resolutions);
    let fallback_sources = missing_nav_sources(&resolutions);

    // Relationship and target validation have completed, so these disjoint
    // value requests can run concurrently. Results are unwrapped in request
    // order below so PX_LAST retains deterministic error precedence.
    let (px_result, fund_result) = tokio::join!(
        request_history_series(engine, px_targets, FIELD_PX_LAST, &start, &end),
        request_history_series(
            engine,
            fallback_sources,
            FIELD_FUND_NET_ASSET_VAL,
            &start,
            &end
        )
    );
    let (px_history, fund_history) = ordered_history_results(px_result, fund_result)?;

    let rows = build_history_rows(&resolutions, &px_history, &fund_history);
    build_history_batch(&rows)
}

/// Shared resolver: one relationship BDP plus one target validation BDP.
async fn resolve_etf_nav_relationships(
    engine: &Engine,
    etfs: &[String],
    mode: ResolveMode,
) -> Result<Vec<EtfNavResolution>> {
    let inputs = prepare_etf_inputs(etfs)?;
    let sources = stable_unique(&inputs);

    let relationship_batch = engine
        .request(build_relationship_request(sources.clone()))
        .await?;
    let relationship_diagnostics = parse_response_diagnostics(&relationship_batch)?;
    check_source_diagnostics(&sources, &relationship_diagnostics)?;
    let relationship_values = strict_refdata_value_map(&relationship_batch)?;
    let targets = map_relationship_targets(&sources, &relationship_values);

    let validation_targets = validation_securities(&sources, &targets);
    let mut target_info: HashMap<String, TargetResolution> =
        HashMap::with_capacity(validation_targets.len());
    if !validation_targets.is_empty() {
        let validation_batch = engine
            .request(build_validation_request(validation_targets.clone(), mode))
            .await?;
        let validation_diagnostics = parse_response_diagnostics(&validation_batch)?;
        let validation_values = strict_refdata_value_map(&validation_batch)?;
        for target in &validation_targets {
            target_info.insert(
                target.clone(),
                build_target_resolution(target, &validation_values, &validation_diagnostics, mode),
            );
        }
        if mode == ResolveMode::Snapshot {
            enforce_snapshot_value_exceptions(
                &validation_targets,
                &target_info,
                &validation_diagnostics,
            )?;
        }
    }

    Ok(build_resolutions(&inputs, &targets, &target_info))
}

/// Trim inputs once and reject blank entries by input position.
fn prepare_etf_inputs(etfs: &[String]) -> Result<Vec<String>> {
    etfs.iter()
        .enumerate()
        .map(|(index, raw)| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(RecipeError::InvalidArgument(format!(
                    "ETF ticker at input_order {index} is empty"
                )));
            }
            Ok(trimmed.to_string())
        })
        .collect()
}

/// Deduplicate while preserving stable first-seen order.
fn stable_unique(values: &[String]) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(values.len());
    values
        .iter()
        .filter(|value| seen.insert(value.as_str()))
        .cloned()
        .collect()
}

/// Validate history date arguments and canonicalize to `YYYYMMDD`.
fn parse_history_range(start_date: &str, end_date: &str) -> Result<(String, String)> {
    let start = parse_date(start_date)?;
    let end = parse_date(end_date)?;
    if end < start {
        return Err(RecipeError::InvalidArgument(format!(
            "end_date {end_date} is before start_date {start_date}"
        )));
    }
    Ok((fmt_date(start, None), fmt_date(end, None)))
}

/// Normalize a Bloomberg relationship value to exactly one ` Index` suffix.
///
/// Removes every case-insensitive trailing standalone `Index` token, trims,
/// returns `None` when nothing remains (or the value is a blank/sentinel),
/// otherwise appends exactly ` Index`.
fn normalize_index_ticker(value: &str) -> Option<String> {
    let cleaned = clean_bloomberg_text(value)?;
    let mut base = cleaned.as_str();
    while let Some(stripped) = strip_one_trailing_index(base) {
        base = stripped;
    }
    if base.is_empty() {
        None
    } else {
        Some(format!("{base} Index"))
    }
}

/// Strip one trailing standalone `Index` token (case-insensitive), if any.
fn strip_one_trailing_index(value: &str) -> Option<&str> {
    let tail_start = value.len().checked_sub("Index".len())?;
    let tail = value.get(tail_start..)?;
    if !tail.eq_ignore_ascii_case("Index") {
        return None;
    }
    let head = &value[..tail_start];
    if head.is_empty() || head.ends_with(char::is_whitespace) {
        Some(head.trim_end())
    } else {
        None
    }
}

/// Map each source ETF to its normalized NAV/iNAV targets.
fn map_relationship_targets(
    sources: &[String],
    values: &RefValues,
) -> HashMap<String, RelationshipTargets> {
    sources
        .iter()
        .map(|source| {
            let fields = values.get(source);
            let nav = fields
                .and_then(|fields| fields.get(FIELD_ETF_NAV_TICKER))
                .and_then(|value| normalize_index_ticker(value));
            let inav = fields
                .and_then(|fields| fields.get(FIELD_ETF_INAV_TICKER))
                .and_then(|value| normalize_index_ticker(value));
            (source.clone(), RelationshipTargets { nav, inav })
        })
        .collect()
}

/// Stable first-seen unique union of NAV targets followed by iNAV targets.
fn validation_securities(
    sources: &[String],
    targets: &HashMap<String, RelationshipTargets>,
) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut securities = Vec::new();
    let nav_targets = sources
        .iter()
        .filter_map(|source| targets.get(source).and_then(|t| t.nav.as_deref()));
    let inav_targets = sources
        .iter()
        .filter_map(|source| targets.get(source).and_then(|t| t.inav.as_deref()));
    for target in nav_targets.chain(inav_targets) {
        if seen.insert(target) {
            securities.push(target.to_string());
        }
    }
    securities
}

/// Build one validated [`TargetResolution`] from the validation response.
fn build_target_resolution(
    ticker: &str,
    values: &RefValues,
    diagnostics: &ResponseDiagnostics,
    mode: ResolveMode,
) -> TargetResolution {
    let fields = values.get(ticker);
    let market_sector_des = fields
        .and_then(|fields| fields.get(FIELD_MARKET_SECTOR_DES))
        .cloned();
    let name = fields.and_then(|fields| fields.get(FIELD_NAME)).cloned();
    let px_last = match mode {
        ResolveMode::Snapshot => fields
            .and_then(|fields| fields.get(FIELD_PX_LAST))
            .and_then(|value| parse_f64_like(value)),
        ResolveMode::Relationships => None,
    };

    let mut reasons: Vec<String> = Vec::new();
    let sector_ok = market_sector_des
        .as_deref()
        .is_some_and(|sector| sector.trim().eq_ignore_ascii_case("Index"));
    if !sector_ok {
        reasons.push(format!(
            "MARKET_SECTOR_DES must equal Index (got {})",
            market_sector_des.as_deref().unwrap_or("<null>")
        ));
    }
    if name.is_none() {
        reasons.push("NAME is missing".to_string());
    }
    if let Some(error) = diagnostics.security_errors.get(ticker) {
        reasons.push(format!(
            "Bloomberg rejected target: {}",
            fmt_security_error(error)
        ));
    }
    if let Some(exceptions) = diagnostics.field_exceptions.get(ticker) {
        for exception in exceptions {
            if exception
                .field
                .eq_ignore_ascii_case(FIELD_MARKET_SECTOR_DES)
                || exception.field.eq_ignore_ascii_case(FIELD_NAME)
            {
                reasons.push(format!(
                    "Bloomberg field {} failed: {}",
                    exception.field,
                    fmt_field_exception(exception)
                ));
            }
        }
    }

    TargetResolution {
        ticker: ticker.to_string(),
        market_sector_des,
        name,
        validation_error: if reasons.is_empty() {
            None
        } else {
            Some(reasons.join("; "))
        },
        px_last,
    }
}

/// Combine per-input resolutions in input order, preserving duplicates.
fn build_resolutions(
    inputs: &[String],
    targets: &HashMap<String, RelationshipTargets>,
    target_info: &HashMap<String, TargetResolution>,
) -> Vec<EtfNavResolution> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, etf)| {
            let relationship = targets.get(etf);
            let resolve = |ticker: Option<&String>| {
                ticker.and_then(|ticker| target_info.get(ticker)).cloned()
            };
            EtfNavResolution {
                input_order: index as i32,
                etf_ticker: etf.clone(),
                nav: resolve(relationship.and_then(|t| t.nav.as_ref())),
                inav: resolve(relationship.and_then(|t| t.inav.as_ref())),
            }
        })
        .collect()
}

/// Aggregate every non-null validation error in input order, NAV before
/// iNAV, and abort before any value request is issued.
fn ensure_targets_valid(resolutions: &[EtfNavResolution]) -> Result<()> {
    let mut failures: Vec<String> = Vec::new();
    for resolution in resolutions {
        let legs = [
            ("NAV", resolution.nav.as_ref()),
            ("iNAV", resolution.inav.as_ref()),
        ];
        for (label, target) in legs {
            let Some(target) = target else { continue };
            if let Some(error) = &target.validation_error {
                failures.push(format!(
                    "{} {label} {}: {error}",
                    resolution.etf_ticker, target.ticker
                ));
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(RecipeError::Other(format!(
            "ETF NAV target validation failed: {}",
            failures.join("; ")
        )))
    }
}

/// Stable-unique source ETFs whose daily NAV relationship is missing.
fn missing_nav_sources(resolutions: &[EtfNavResolution]) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    resolutions
        .iter()
        .filter(|resolution| resolution.nav.is_none())
        .filter(|resolution| seen.insert(resolution.etf_ticker.as_str()))
        .map(|resolution| resolution.etf_ticker.clone())
        .collect()
}

/// Stable-unique union of valid NAV then iNAV targets for `PX_LAST` history.
fn px_history_targets(resolutions: &[EtfNavResolution]) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut targets = Vec::new();
    let nav_targets = resolutions
        .iter()
        .filter_map(|resolution| resolution.nav.as_ref().map(|t| t.ticker.as_str()));
    let inav_targets = resolutions
        .iter()
        .filter_map(|resolution| resolution.inav.as_ref().map(|t| t.ticker.as_str()));
    for target in nav_targets.chain(inav_targets) {
        if seen.insert(target) {
            targets.push(target.to_string());
        }
    }
    targets
}

/// Merge per-input date unions across the NAV source and iNAV series.
fn build_history_rows(
    resolutions: &[EtfNavResolution],
    px_history: &HistorySeries,
    fund_history: &HistorySeries,
) -> Vec<HistoryRow> {
    let empty = BTreeMap::new();
    let mut rows = Vec::new();
    for resolution in resolutions {
        let (nav_source_ticker, nav_source_field, nav_series) = match resolution.nav.as_ref() {
            Some(nav) => (
                nav.ticker.clone(),
                FIELD_PX_LAST,
                px_history
                    .get(&(nav.ticker.clone(), FIELD_PX_LAST.to_string()))
                    .unwrap_or(&empty),
            ),
            None => (
                resolution.etf_ticker.clone(),
                FIELD_FUND_NET_ASSET_VAL,
                fund_history
                    .get(&(
                        resolution.etf_ticker.clone(),
                        FIELD_FUND_NET_ASSET_VAL.to_string(),
                    ))
                    .unwrap_or(&empty),
            ),
        };
        let inav_series = resolution
            .inav
            .as_ref()
            .and_then(|inav| px_history.get(&(inav.ticker.clone(), FIELD_PX_LAST.to_string())))
            .unwrap_or(&empty);

        let dates: BTreeSet<NaiveDate> = nav_series
            .keys()
            .chain(inav_series.keys())
            .copied()
            .collect();
        for date in dates {
            rows.push(HistoryRow {
                input_order: resolution.input_order,
                etf_ticker: resolution.etf_ticker.clone(),
                date,
                nav_ticker: resolution.nav.as_ref().map(|t| t.ticker.clone()),
                nav_value: nav_series.get(&date).copied(),
                nav_source_ticker: nav_source_ticker.clone(),
                nav_source_field,
                inav_ticker: resolution.inav.as_ref().map(|t| t.ticker.clone()),
                inav_value: inav_series.get(&date).copied(),
            });
        }
    }
    rows
}

// --- Request builders -----------------------------------------------------

/// Relationship BDP: `ETF_NAV_TICKER`, `ETF_INAV_TICKER` for source ETFs.
fn build_relationship_request(securities: Vec<String>) -> RequestParams {
    RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(securities),
        fields: Some(vec![
            FIELD_ETF_NAV_TICKER.to_string(),
            FIELD_ETF_INAV_TICKER.to_string(),
        ]),
        ..Default::default()
    }
}

/// Target validation BDP; snapshot mode appends `PX_LAST`.
fn build_validation_request(securities: Vec<String>, mode: ResolveMode) -> RequestParams {
    let mut fields = vec![FIELD_MARKET_SECTOR_DES.to_string(), FIELD_NAME.to_string()];
    if mode == ResolveMode::Snapshot {
        fields.push(FIELD_PX_LAST.to_string());
    }
    RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(securities),
        fields: Some(fields),
        ..Default::default()
    }
}

/// Snapshot fallback BDP: `FUND_NET_ASSET_VAL` for sources without a NAV.
fn build_snapshot_fallback_request(securities: Vec<String>) -> RequestParams {
    RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(securities),
        fields: Some(vec![FIELD_FUND_NET_ASSET_VAL.to_string()]),
        ..Default::default()
    }
}

/// Daily historical request; `periodicitySelection` is a request element.
fn build_history_request(
    securities: Vec<String>,
    field: &str,
    start_date: &str,
    end_date: &str,
) -> RequestParams {
    RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(securities),
        fields: Some(vec![field.to_string()]),
        start_date: Some(start_date.to_string()),
        end_date: Some(end_date.to_string()),
        elements: Some(vec![(
            "periodicitySelection".to_string(),
            "DAILY".to_string(),
        )]),
        ..Default::default()
    }
}

async fn request_history_series(
    engine: &Engine,
    securities: Vec<String>,
    field: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HistorySeries> {
    if securities.is_empty() {
        return Ok(HistorySeries::new());
    }

    let batch = engine
        .request(build_history_request(
            securities.clone(),
            field,
            start_date,
            end_date,
        ))
        .await?;
    let diagnostics = parse_response_diagnostics(&batch)?;
    check_value_source_diagnostics(&securities, &diagnostics)?;
    strict_history_value_map(&batch)
}

fn ordered_history_results(
    px_result: Result<HistorySeries>,
    fund_result: Result<HistorySeries>,
) -> Result<(HistorySeries, HistorySeries)> {
    let px_history = px_result?;
    let fund_history = fund_result?;
    Ok((px_history, fund_history))
}

// --- Response parsing -----------------------------------------------------

/// Decode security errors and field exceptions from batch schema metadata.
///
/// Missing keys become empty maps; malformed JSON aborts.
fn parse_response_diagnostics(batch: &RecordBatch) -> Result<ResponseDiagnostics> {
    let metadata = batch.schema_ref().metadata();
    let security_errors = match metadata.get(METADATA_KEY_SECURITY_ERRORS) {
        None => BTreeMap::new(),
        Some(raw) => serde_json::from_str(raw).map_err(|err| {
            RecipeError::Other(format!(
                "invalid {METADATA_KEY_SECURITY_ERRORS} metadata: {err}"
            ))
        })?,
    };
    let field_exceptions = match metadata.get(METADATA_KEY_FIELD_EXCEPTIONS) {
        None => BTreeMap::new(),
        Some(raw) => serde_json::from_str(raw).map_err(|err| {
            RecipeError::Other(format!(
                "invalid {METADATA_KEY_FIELD_EXCEPTIONS} metadata: {err}"
            ))
        })?,
    };
    Ok(ResponseDiagnostics {
        security_errors,
        field_exceptions,
    })
}

fn fmt_security_error(error: &SecurityErrorMeta) -> String {
    format!(
        "{}/{}/{}: {}",
        error.category, error.code, error.subcategory, error.message
    )
}

fn fmt_field_exception(exception: &FieldExceptionMeta) -> String {
    format!(
        "{}/{}/{}: {}",
        exception.category, exception.code, exception.subcategory, exception.message
    )
}

/// Bloomberg reports "this security has no such relationship" as a
/// `BAD_FLD`/`NOT_APPLICABLE_TO_REF_DATA` field exception (observed live for
/// `ETF_NAV_TICKER` on `AT1 LN Equity`); that is a legitimate null
/// relationship, not a failure.
fn is_not_applicable(exception: &FieldExceptionMeta) -> bool {
    exception
        .subcategory
        .eq_ignore_ascii_case("NOT_APPLICABLE_TO_REF_DATA")
}

/// A source ETF security error or relationship-field exception aborts; it
/// must never masquerade as a legitimate null relationship. The only
/// exception is Bloomberg's explicit not-applicable marker, which *is* the
/// null relationship.
fn check_source_diagnostics(sources: &[String], diagnostics: &ResponseDiagnostics) -> Result<()> {
    for source in sources {
        if let Some(error) = diagnostics.security_errors.get(source) {
            return Err(RecipeError::Other(format!(
                "Bloomberg rejected ETF {source}: {}",
                fmt_security_error(error)
            )));
        }
        if let Some(exceptions) = diagnostics.field_exceptions.get(source) {
            for exception in exceptions {
                let relationship_field = exception.field.eq_ignore_ascii_case(FIELD_ETF_NAV_TICKER)
                    || exception.field.eq_ignore_ascii_case(FIELD_ETF_INAV_TICKER);
                if relationship_field && !is_not_applicable(exception) {
                    return Err(RecipeError::Other(format!(
                        "Bloomberg relationship field {} failed for {source}: {}",
                        exception.field,
                        fmt_field_exception(exception)
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Value-source diagnostics abort snapshot/history value requests.
fn check_value_source_diagnostics(
    securities: &[String],
    diagnostics: &ResponseDiagnostics,
) -> Result<()> {
    for security in securities {
        if let Some(error) = diagnostics.security_errors.get(security) {
            return Err(RecipeError::Other(format!(
                "Bloomberg rejected value source {security}: {}",
                fmt_security_error(error)
            )));
        }
        if let Some(exceptions) = diagnostics.field_exceptions.get(security) {
            for exception in exceptions {
                if exception.field.eq_ignore_ascii_case(FIELD_PX_LAST)
                    || exception
                        .field
                        .eq_ignore_ascii_case(FIELD_FUND_NET_ASSET_VAL)
                {
                    return Err(RecipeError::Other(format!(
                        "Bloomberg value field {} failed for {security}: {}",
                        exception.field,
                        fmt_field_exception(exception)
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Abort on a snapshot `PX_LAST` exception only when no target has a
/// validation error: validation failure wins deterministically.
fn enforce_snapshot_value_exceptions(
    targets: &[String],
    target_info: &HashMap<String, TargetResolution>,
    diagnostics: &ResponseDiagnostics,
) -> Result<()> {
    if target_info
        .values()
        .any(|target| target.validation_error.is_some())
    {
        return Ok(());
    }
    for target in targets {
        if let Some(exceptions) = diagnostics.field_exceptions.get(target) {
            for exception in exceptions {
                if exception.field.eq_ignore_ascii_case(FIELD_PX_LAST) {
                    return Err(RecipeError::Other(format!(
                        "Bloomberg value field {} failed for {target}: {}",
                        exception.field,
                        fmt_field_exception(exception)
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Strict generic long `ticker`/`field`/`value` parse.
///
/// Identical duplicate rows coalesce; conflicting duplicate values abort.
/// Blank and sentinel values are dropped so they surface as `None`.
fn strict_refdata_value_map(batch: &RecordBatch) -> Result<RefValues> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let mut values: RefValues = HashMap::new();

    for row in 0..batch.num_rows() {
        use arrow_array::Array;
        if ticker_col.is_null(row) || field_col.is_null(row) {
            continue;
        }
        let Some(raw_value) = array_value_as_string(value_col, row) else {
            continue;
        };
        let Some(value) = clean_bloomberg_text(&raw_value) else {
            continue;
        };
        let ticker = ticker_col.value(row);
        let field = field_col.value(row).to_ascii_uppercase();
        let fields = values.entry(ticker.to_string()).or_default();
        match fields.entry(field) {
            HashEntry::Occupied(entry) => {
                if entry.get() != &value {
                    return Err(RecipeError::Other(format!(
                        "conflicting values for {ticker} {}: {} vs {value}",
                        entry.key(),
                        entry.get()
                    )));
                }
            }
            HashEntry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    Ok(values)
}

/// Strict historical long parse keyed by `(ticker, date, field)`.
fn strict_history_value_map(batch: &RecordBatch) -> Result<HistorySeries> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let mut values = HistorySeries::new();

    for row in 0..batch.num_rows() {
        use arrow_array::Array;
        if ticker_col.is_null(row) || field_col.is_null(row) {
            continue;
        }
        let Some(date) = array_value_as_date(date_col, row) else {
            continue;
        };
        let Some(value) = array_value_as_f64(value_col, row) else {
            continue;
        };
        let ticker = ticker_col.value(row);
        let field = field_col.value(row).to_ascii_uppercase();
        let series = values.entry((ticker.to_string(), field)).or_default();
        match series.entry(date) {
            BTreeEntry::Occupied(entry) => {
                if *entry.get() != value {
                    return Err(RecipeError::Other(format!(
                        "conflicting values for {ticker} {} on {date}: {} vs {value}",
                        field_col.value(row).to_ascii_uppercase(),
                        entry.get()
                    )));
                }
            }
            BTreeEntry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    Ok(values)
}

// --- Batch builders -------------------------------------------------------

fn relationships_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("etf_ticker", DataType::Utf8, false),
        Field::new("nav_ticker", DataType::Utf8, true),
        Field::new("nav_market_sector_des", DataType::Utf8, true),
        Field::new("nav_name", DataType::Utf8, true),
        Field::new("nav_validation_error", DataType::Utf8, true),
        Field::new("inav_ticker", DataType::Utf8, true),
        Field::new("inav_market_sector_des", DataType::Utf8, true),
        Field::new("inav_name", DataType::Utf8, true),
        Field::new("inav_validation_error", DataType::Utf8, true),
    ]))
}

fn snapshot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("etf_ticker", DataType::Utf8, false),
        Field::new("nav_ticker", DataType::Utf8, true),
        Field::new("nav_value", DataType::Float64, true),
        Field::new("nav_source_ticker", DataType::Utf8, false),
        Field::new("nav_source_field", DataType::Utf8, false),
        Field::new("inav_ticker", DataType::Utf8, true),
        Field::new("inav_value", DataType::Float64, true),
    ]))
}

fn history_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("etf_ticker", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("nav_ticker", DataType::Utf8, true),
        Field::new("nav_value", DataType::Float64, true),
        Field::new("nav_source_ticker", DataType::Utf8, false),
        Field::new("nav_source_field", DataType::Utf8, false),
        Field::new("inav_ticker", DataType::Utf8, true),
        Field::new("inav_value", DataType::Float64, true),
    ]))
}

fn append_target(
    ticker: &mut StringBuilder,
    sector: &mut StringBuilder,
    name: &mut StringBuilder,
    error: &mut StringBuilder,
    target: Option<&TargetResolution>,
) {
    match target {
        Some(target) => {
            ticker.append_value(&target.ticker);
            sector.append_option(target.market_sector_des.as_deref());
            name.append_option(target.name.as_deref());
            error.append_option(target.validation_error.as_deref());
        }
        None => {
            ticker.append_null();
            sector.append_null();
            name.append_null();
            error.append_null();
        }
    }
}

fn build_relationships_batch(resolutions: &[EtfNavResolution]) -> Result<RecordBatch> {
    let mut input_order = Int32Builder::with_capacity(resolutions.len());
    let mut etf_ticker = StringBuilder::new();
    let mut nav_ticker = StringBuilder::new();
    let mut nav_sector = StringBuilder::new();
    let mut nav_name = StringBuilder::new();
    let mut nav_error = StringBuilder::new();
    let mut inav_ticker = StringBuilder::new();
    let mut inav_sector = StringBuilder::new();
    let mut inav_name = StringBuilder::new();
    let mut inav_error = StringBuilder::new();

    for resolution in resolutions {
        input_order.append_value(resolution.input_order);
        etf_ticker.append_value(&resolution.etf_ticker);
        append_target(
            &mut nav_ticker,
            &mut nav_sector,
            &mut nav_name,
            &mut nav_error,
            resolution.nav.as_ref(),
        );
        append_target(
            &mut inav_ticker,
            &mut inav_sector,
            &mut inav_name,
            &mut inav_error,
            resolution.inav.as_ref(),
        );
    }

    Ok(RecordBatch::try_new(
        relationships_schema(),
        vec![
            Arc::new(input_order.finish()),
            Arc::new(etf_ticker.finish()),
            Arc::new(nav_ticker.finish()),
            Arc::new(nav_sector.finish()),
            Arc::new(nav_name.finish()),
            Arc::new(nav_error.finish()),
            Arc::new(inav_ticker.finish()),
            Arc::new(inav_sector.finish()),
            Arc::new(inav_name.finish()),
            Arc::new(inav_error.finish()),
        ],
    )?)
}

fn build_snapshot_batch(
    resolutions: &[EtfNavResolution],
    fallback_values: &HashMap<String, f64>,
) -> Result<RecordBatch> {
    let mut input_order = Int32Builder::with_capacity(resolutions.len());
    let mut etf_ticker = StringBuilder::new();
    let mut nav_ticker = StringBuilder::new();
    let mut nav_value = Float64Builder::with_capacity(resolutions.len());
    let mut nav_source_ticker = StringBuilder::new();
    let mut nav_source_field = StringBuilder::new();
    let mut inav_ticker = StringBuilder::new();
    let mut inav_value = Float64Builder::with_capacity(resolutions.len());

    for resolution in resolutions {
        input_order.append_value(resolution.input_order);
        etf_ticker.append_value(&resolution.etf_ticker);
        match resolution.nav.as_ref() {
            Some(nav) => {
                nav_ticker.append_value(&nav.ticker);
                nav_value.append_option(nav.px_last);
                nav_source_ticker.append_value(&nav.ticker);
                nav_source_field.append_value(FIELD_PX_LAST);
            }
            None => {
                nav_ticker.append_null();
                nav_value.append_option(fallback_values.get(&resolution.etf_ticker).copied());
                nav_source_ticker.append_value(&resolution.etf_ticker);
                nav_source_field.append_value(FIELD_FUND_NET_ASSET_VAL);
            }
        }
        match resolution.inav.as_ref() {
            Some(inav) => {
                inav_ticker.append_value(&inav.ticker);
                inav_value.append_option(inav.px_last);
            }
            None => {
                inav_ticker.append_null();
                inav_value.append_null();
            }
        }
    }

    Ok(RecordBatch::try_new(
        snapshot_schema(),
        vec![
            Arc::new(input_order.finish()),
            Arc::new(etf_ticker.finish()),
            Arc::new(nav_ticker.finish()),
            Arc::new(nav_value.finish()),
            Arc::new(nav_source_ticker.finish()),
            Arc::new(nav_source_field.finish()),
            Arc::new(inav_ticker.finish()),
            Arc::new(inav_value.finish()),
        ],
    )?)
}

fn build_history_batch(rows: &[HistoryRow]) -> Result<RecordBatch> {
    let mut input_order = Int32Builder::with_capacity(rows.len());
    let mut etf_ticker = StringBuilder::new();
    let mut date = Date32Builder::with_capacity(rows.len());
    let mut nav_ticker = StringBuilder::new();
    let mut nav_value = Float64Builder::with_capacity(rows.len());
    let mut nav_source_ticker = StringBuilder::new();
    let mut nav_source_field = StringBuilder::new();
    let mut inav_ticker = StringBuilder::new();
    let mut inav_value = Float64Builder::with_capacity(rows.len());

    for row in rows {
        input_order.append_value(row.input_order);
        etf_ticker.append_value(&row.etf_ticker);
        date.append_value(naive_to_date32(row.date));
        nav_ticker.append_option(row.nav_ticker.as_deref());
        nav_value.append_option(row.nav_value);
        nav_source_ticker.append_value(&row.nav_source_ticker);
        nav_source_field.append_value(row.nav_source_field);
        inav_ticker.append_option(row.inav_ticker.as_deref());
        inav_value.append_option(row.inav_value);
    }

    Ok(RecordBatch::try_new(
        history_schema(),
        vec![
            Arc::new(input_order.finish()),
            Arc::new(etf_ticker.finish()),
            Arc::new(date.finish()),
            Arc::new(nav_ticker.finish()),
            Arc::new(nav_value.finish()),
            Arc::new(nav_source_ticker.finish()),
            Arc::new(nav_source_field.finish()),
            Arc::new(inav_ticker.finish()),
            Arc::new(inav_value.finish()),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Date32Array, Float64Array, Int32Array, StringArray};

    const QQQ: &str = "QQQ US Equity";
    const AT1: &str = "AT1 LN Equity";
    const QQQ_NAV: &str = "QQQNV Index";
    const QQQ_INAV: &str = "QXV Index";
    const AT1_INAV: &str = "AT1IN Index";

    fn refdata_batch(rows: &[(&str, &str, Option<&str>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(t, _, _)| *t).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, f, _)| *f).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn history_batch(rows: &[(&str, &str, Option<f64>, NaiveDate)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new("date", DataType::Date32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(t, _, _, _)| *t).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, f, _, _)| *f).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|(_, _, v, _)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(Date32Array::from(
                    rows.iter()
                        .map(|(_, _, _, d)| Some(naive_to_date32(*d)))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn with_metadata(batch: &RecordBatch, entries: &[(&str, &str)]) -> RecordBatch {
        let mut metadata = batch.schema_ref().metadata().clone();
        for (key, value) in entries {
            metadata.insert((*key).to_string(), (*value).to_string());
        }
        let schema = Arc::new(batch.schema_ref().as_ref().clone().with_metadata(metadata));
        RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap()
    }

    fn target(ticker: &str, error: Option<&str>, px_last: Option<f64>) -> TargetResolution {
        TargetResolution {
            ticker: ticker.to_string(),
            market_sector_des: Some("Index".to_string()),
            name: Some(format!("{ticker} name")),
            validation_error: error.map(str::to_string),
            px_last,
        }
    }

    fn resolution(
        input_order: i32,
        etf: &str,
        nav: Option<TargetResolution>,
        inav: Option<TargetResolution>,
    ) -> EtfNavResolution {
        EtfNavResolution {
            input_order,
            etf_ticker: etf.to_string(),
            nav,
            inav,
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn string_at(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
        let array = batch.column_by_name(column).unwrap();
        array_value_as_string(array, row)
    }

    fn f64_at(batch: &RecordBatch, column: &str, row: usize) -> Option<f64> {
        let array = batch.column_by_name(column).unwrap();
        array_value_as_f64(array, row)
    }

    #[test]
    fn schemas_are_exact() {
        let expected_relationships = [
            ("input_order", DataType::Int32, false),
            ("etf_ticker", DataType::Utf8, false),
            ("nav_ticker", DataType::Utf8, true),
            ("nav_market_sector_des", DataType::Utf8, true),
            ("nav_name", DataType::Utf8, true),
            ("nav_validation_error", DataType::Utf8, true),
            ("inav_ticker", DataType::Utf8, true),
            ("inav_market_sector_des", DataType::Utf8, true),
            ("inav_name", DataType::Utf8, true),
            ("inav_validation_error", DataType::Utf8, true),
        ];
        let expected_snapshot = [
            ("input_order", DataType::Int32, false),
            ("etf_ticker", DataType::Utf8, false),
            ("nav_ticker", DataType::Utf8, true),
            ("nav_value", DataType::Float64, true),
            ("nav_source_ticker", DataType::Utf8, false),
            ("nav_source_field", DataType::Utf8, false),
            ("inav_ticker", DataType::Utf8, true),
            ("inav_value", DataType::Float64, true),
        ];
        let expected_history = [
            ("input_order", DataType::Int32, false),
            ("etf_ticker", DataType::Utf8, false),
            ("date", DataType::Date32, false),
            ("nav_ticker", DataType::Utf8, true),
            ("nav_value", DataType::Float64, true),
            ("nav_source_ticker", DataType::Utf8, false),
            ("nav_source_field", DataType::Utf8, false),
            ("inav_ticker", DataType::Utf8, true),
            ("inav_value", DataType::Float64, true),
        ];
        for (schema, expected) in [
            (relationships_schema(), expected_relationships.as_slice()),
            (snapshot_schema(), expected_snapshot.as_slice()),
            (history_schema(), expected_history.as_slice()),
        ] {
            assert_eq!(schema.fields().len(), expected.len());
            for (field, (name, data_type, nullable)) in schema.fields().iter().zip(expected) {
                assert_eq!(field.name(), name);
                assert_eq!(field.data_type(), data_type);
                assert_eq!(field.is_nullable(), *nullable, "nullability of {name}");
            }
        }
    }

    #[test]
    fn empty_inputs_build_zero_row_batches() {
        let relationships = build_relationships_batch(&[]).unwrap();
        assert_eq!(relationships.num_rows(), 0);
        assert_eq!(relationships.schema(), relationships_schema());

        let snapshot = build_snapshot_batch(&[], &HashMap::new()).unwrap();
        assert_eq!(snapshot.num_rows(), 0);
        assert_eq!(snapshot.schema(), snapshot_schema());

        let history = build_history_batch(&[]).unwrap();
        assert_eq!(history.num_rows(), 0);
        assert_eq!(history.schema(), history_schema());
    }

    #[test]
    fn normalize_index_ticker_handles_suffixes_and_sentinels() {
        assert_eq!(normalize_index_ticker("QXV"), Some("QXV Index".to_string()));
        assert_eq!(
            normalize_index_ticker("QXV Index"),
            Some("QXV Index".to_string())
        );
        assert_eq!(
            normalize_index_ticker("  qxv index  "),
            Some("qxv Index".to_string())
        );
        assert_eq!(
            normalize_index_ticker("QXV INDEX Index"),
            Some("QXV Index".to_string())
        );
        assert_eq!(
            normalize_index_ticker("SPINDEX"),
            Some("SPINDEX Index".to_string())
        );
        assert_eq!(normalize_index_ticker("Index"), None);
        assert_eq!(normalize_index_ticker("  index  "), None);
        assert_eq!(normalize_index_ticker("Index Index"), None);
        assert_eq!(normalize_index_ticker(""), None);
        assert_eq!(normalize_index_ticker("   "), None);
        assert_eq!(normalize_index_ticker("nan"), None);
        assert_eq!(normalize_index_ticker("N/A"), None);
        assert_eq!(normalize_index_ticker("#N/A"), None);
        assert_eq!(normalize_index_ticker("null"), None);
    }

    #[test]
    fn prepare_etf_inputs_trims_and_rejects_blank() {
        let inputs = prepare_etf_inputs(&[format!("  {QQQ}  ")]).unwrap();
        assert_eq!(inputs, vec![QQQ.to_string()]);

        let error = prepare_etf_inputs(&[QQQ.to_string(), "   ".to_string()]).unwrap_err();
        assert!(matches!(error, RecipeError::InvalidArgument(_)));
        assert!(error
            .to_string()
            .contains("ETF ticker at input_order 1 is empty"));
    }

    #[test]
    fn stable_unique_keeps_first_seen_order() {
        let values = vec![
            QQQ.to_string(),
            AT1.to_string(),
            QQQ.to_string(),
            AT1.to_string(),
        ];
        assert_eq!(
            stable_unique(&values),
            vec![QQQ.to_string(), AT1.to_string()]
        );
    }

    #[test]
    fn parse_history_range_validates_and_canonicalizes() {
        let (start, end) = parse_history_range("2026-06-01", "2026-07-01").unwrap();
        assert_eq!(start, "20260601");
        assert_eq!(end, "20260701");

        let error = parse_history_range("2026-07-01", "2026-06-01").unwrap_err();
        assert!(matches!(error, RecipeError::InvalidArgument(_)));
        assert!(error
            .to_string()
            .contains("end_date 2026-06-01 is before start_date 2026-07-01"));

        assert!(parse_history_range("not-a-date", "2026-07-01").is_err());
    }

    #[test]
    fn request_builders_use_exact_parameters() {
        let relationship = build_relationship_request(vec![QQQ.to_string()]);
        assert_eq!(relationship.service, "//blp/refdata");
        assert_eq!(relationship.operation, "ReferenceDataRequest");
        assert_eq!(relationship.securities, Some(vec![QQQ.to_string()]));
        assert_eq!(
            relationship.fields,
            Some(vec![
                "ETF_NAV_TICKER".to_string(),
                "ETF_INAV_TICKER".to_string()
            ])
        );
        assert!(relationship.elements.is_none());
        assert!(relationship.overrides.is_none());

        let validation =
            build_validation_request(vec![QQQ_NAV.to_string()], ResolveMode::Relationships);
        assert_eq!(
            validation.fields,
            Some(vec!["MARKET_SECTOR_DES".to_string(), "NAME".to_string()])
        );

        let snapshot_validation =
            build_validation_request(vec![QQQ_NAV.to_string()], ResolveMode::Snapshot);
        assert_eq!(
            snapshot_validation.fields,
            Some(vec![
                "MARKET_SECTOR_DES".to_string(),
                "NAME".to_string(),
                "PX_LAST".to_string()
            ])
        );

        let fallback = build_snapshot_fallback_request(vec![AT1.to_string()]);
        assert_eq!(fallback.operation, "ReferenceDataRequest");
        assert_eq!(
            fallback.fields,
            Some(vec!["FUND_NET_ASSET_VAL".to_string()])
        );

        let history = build_history_request(
            vec![QQQ_NAV.to_string()],
            FIELD_PX_LAST,
            "20260601",
            "20260701",
        );
        assert_eq!(history.service, "//blp/refdata");
        assert_eq!(history.operation, "HistoricalDataRequest");
        assert_eq!(history.fields, Some(vec!["PX_LAST".to_string()]));
        assert_eq!(history.start_date, Some("20260601".to_string()));
        assert_eq!(history.end_date, Some("20260701".to_string()));
        assert_eq!(
            history.elements,
            Some(vec![(
                "periodicitySelection".to_string(),
                "DAILY".to_string()
            )])
        );
        assert!(history.options.is_none());
    }

    #[test]
    fn concurrent_history_errors_keep_px_last_precedence() {
        let px_result: Result<HistorySeries> =
            Err(RecipeError::Other("PX_LAST request failed".to_string()));
        let fund_result: Result<HistorySeries> = Err(RecipeError::Other(
            "FUND_NET_ASSET_VAL request failed".to_string(),
        ));

        let error = ordered_history_results(px_result, fund_result).unwrap_err();
        assert_eq!(error.to_string(), "Recipe error: PX_LAST request failed");
    }

    #[test]
    fn strict_refdata_map_coalesces_identical_and_rejects_conflicts() {
        let identical = refdata_batch(&[
            (QQQ, "ETF_NAV_TICKER", Some("QQQNV")),
            (QQQ, "ETF_NAV_TICKER", Some("QQQNV")),
        ]);
        let values = strict_refdata_value_map(&identical).unwrap();
        assert_eq!(values[QQQ]["ETF_NAV_TICKER"], "QQQNV");

        let conflicting = refdata_batch(&[
            (QQQ, "ETF_NAV_TICKER", Some("QQQNV")),
            (QQQ, "etf_nav_ticker", Some("OTHER")),
        ]);
        let error = strict_refdata_value_map(&conflicting).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
    }

    #[test]
    fn strict_refdata_map_requires_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![QQQ])),
                Arc::new(StringArray::from(vec!["ETF_NAV_TICKER"])),
            ],
        )
        .unwrap();
        let error = strict_refdata_value_map(&batch).unwrap_err();
        assert!(error.to_string().contains("missing 'value' column"));

        let wrong_type = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Int32, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            wrong_type,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["ETF_NAV_TICKER"])),
                Arc::new(StringArray::from(vec!["QQQNV"])),
            ],
        )
        .unwrap();
        let error = strict_refdata_value_map(&batch).unwrap_err();
        assert!(error.to_string().contains("'ticker' column must be Utf8"));
    }

    #[test]
    fn strict_history_map_coalesces_identical_and_rejects_conflicts() {
        let day = date(2026, 7, 1);
        let identical = history_batch(&[
            (QQQ_NAV, "PX_LAST", Some(500.25), day),
            (QQQ_NAV, "PX_LAST", Some(500.25), day),
        ]);
        let values = strict_history_value_map(&identical).unwrap();
        assert_eq!(
            values[&(QQQ_NAV.to_string(), "PX_LAST".to_string())][&day],
            500.25
        );

        let conflicting = history_batch(&[
            (QQQ_NAV, "PX_LAST", Some(500.25), day),
            (QQQ_NAV, "PX_LAST", Some(501.0), day),
        ]);
        let error = strict_history_value_map(&conflicting).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));

        let missing_date = refdata_batch(&[(QQQ_NAV, "PX_LAST", Some("500.25"))]);
        let error = strict_history_value_map(&missing_date).unwrap_err();
        assert!(error.to_string().contains("missing 'date' column"));
    }

    #[test]
    fn diagnostics_parse_missing_and_malformed_metadata() {
        let batch = refdata_batch(&[(QQQ, "ETF_NAV_TICKER", Some("QQQNV"))]);
        let diagnostics = parse_response_diagnostics(&batch).unwrap();
        assert!(diagnostics.security_errors.is_empty());
        assert!(diagnostics.field_exceptions.is_empty());

        let malformed = with_metadata(&batch, &[(METADATA_KEY_SECURITY_ERRORS, "{not json")]);
        let error = parse_response_diagnostics(&malformed).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid xbbg.security_errors metadata:"));

        let malformed = with_metadata(&batch, &[(METADATA_KEY_FIELD_EXCEPTIONS, "[42]")]);
        let error = parse_response_diagnostics(&malformed).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid xbbg.field_exceptions metadata:"));
    }

    #[test]
    fn diagnostics_parse_populated_metadata() {
        let batch = refdata_batch(&[(QQQ, "ETF_NAV_TICKER", Some("QQQNV"))]);
        let security = format!(
            r#"{{"{AT1}": {{"category": "BAD_SEC", "code": 15, "subcategory": "INVALID_SECURITY", "message": "Unknown/Invalid Security"}}}}"#
        );
        let exceptions = format!(
            r#"{{"{QQQ}": [{{"field": "ETF_INAV_TICKER", "category": "BAD_FLD", "code": 9, "subcategory": "NOT_APPLICABLE_TO_REF_DATA", "message": "Field not applicable"}}]}}"#
        );
        let batch = with_metadata(
            &batch,
            &[
                (METADATA_KEY_SECURITY_ERRORS, security.as_str()),
                (METADATA_KEY_FIELD_EXCEPTIONS, exceptions.as_str()),
            ],
        );
        let diagnostics = parse_response_diagnostics(&batch).unwrap();
        assert_eq!(diagnostics.security_errors[AT1].code, 15);
        assert_eq!(
            diagnostics.field_exceptions[QQQ][0].field,
            "ETF_INAV_TICKER"
        );
    }

    fn security_error(message: &str) -> SecurityErrorMeta {
        SecurityErrorMeta {
            category: "BAD_SEC".to_string(),
            code: 15,
            subcategory: "INVALID_SECURITY".to_string(),
            message: message.to_string(),
        }
    }

    fn field_exception(field: &str, message: &str) -> FieldExceptionMeta {
        field_exception_with(field, "NOT_APPLICABLE_TO_REF_DATA", message)
    }

    fn field_exception_with(field: &str, subcategory: &str, message: &str) -> FieldExceptionMeta {
        FieldExceptionMeta {
            field: field.to_string(),
            category: "BAD_FLD".to_string(),
            code: 9,
            subcategory: subcategory.to_string(),
            message: message.to_string(),
        }
    }

    #[test]
    fn source_security_error_aborts_with_exact_message() {
        let diagnostics = ResponseDiagnostics {
            security_errors: BTreeMap::from([(AT1.to_string(), security_error("Unknown"))]),
            field_exceptions: BTreeMap::new(),
        };
        let error = check_source_diagnostics(&[QQQ.to_string(), AT1.to_string()], &diagnostics)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: Bloomberg rejected ETF {AT1}: BAD_SEC/15/INVALID_SECURITY: Unknown"
            )
        );
    }

    #[test]
    fn source_relationship_field_exception_aborts() {
        let diagnostics = ResponseDiagnostics {
            security_errors: BTreeMap::new(),
            field_exceptions: BTreeMap::from([(
                QQQ.to_string(),
                vec![field_exception_with(
                    "ETF_INAV_TICKER",
                    "INVALID_FIELD",
                    "Field unknown",
                )],
            )]),
        };
        let error = check_source_diagnostics(&[QQQ.to_string()], &diagnostics).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: Bloomberg relationship field ETF_INAV_TICKER failed for {QQQ}: \
                 BAD_FLD/9/INVALID_FIELD: Field unknown"
            )
        );

        let unrelated = ResponseDiagnostics {
            security_errors: BTreeMap::new(),
            field_exceptions: BTreeMap::from([(
                QQQ.to_string(),
                vec![field_exception_with("PX_LAST", "INVALID_FIELD", "ignored")],
            )]),
        };
        assert!(check_source_diagnostics(&[QQQ.to_string()], &unrelated).is_ok());
    }

    #[test]
    fn source_not_applicable_relationship_exception_is_null() {
        // Live contract: Bloomberg flags AT1's missing daily NAV with a
        // NOT_APPLICABLE_TO_REF_DATA exception on ETF_NAV_TICKER; the
        // resolver must treat it as a legitimate null relationship.
        let diagnostics = ResponseDiagnostics {
            security_errors: BTreeMap::new(),
            field_exceptions: BTreeMap::from([(
                AT1.to_string(),
                vec![field_exception(
                    "ETF_NAV_TICKER",
                    "Field not applicable to security",
                )],
            )]),
        };
        assert!(check_source_diagnostics(&[AT1.to_string()], &diagnostics).is_ok());
    }

    #[test]
    fn value_source_diagnostics_abort() {
        let rejected = ResponseDiagnostics {
            security_errors: BTreeMap::from([(AT1.to_string(), security_error("Unknown"))]),
            field_exceptions: BTreeMap::new(),
        };
        let error = check_value_source_diagnostics(&[AT1.to_string()], &rejected).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: Bloomberg rejected value source {AT1}: \
                 BAD_SEC/15/INVALID_SECURITY: Unknown"
            )
        );

        let failed_field = ResponseDiagnostics {
            security_errors: BTreeMap::new(),
            field_exceptions: BTreeMap::from([(
                AT1.to_string(),
                vec![field_exception("FUND_NET_ASSET_VAL", "No data")],
            )]),
        };
        let error = check_value_source_diagnostics(&[AT1.to_string()], &failed_field).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: Bloomberg value field FUND_NET_ASSET_VAL failed for {AT1}: \
                 BAD_FLD/9/NOT_APPLICABLE_TO_REF_DATA: No data"
            )
        );
    }

    #[test]
    fn target_validation_reasons_join_in_exact_order() {
        let values: RefValues = HashMap::from([(
            QQQ_NAV.to_string(),
            HashMap::from([(FIELD_MARKET_SECTOR_DES.to_string(), "Equity".to_string())]),
        )]);
        let diagnostics = ResponseDiagnostics {
            security_errors: BTreeMap::from([(QQQ_NAV.to_string(), security_error("Rejected"))]),
            field_exceptions: BTreeMap::from([(
                QQQ_NAV.to_string(),
                vec![
                    field_exception("NAME", "Name failed"),
                    field_exception("PX_LAST", "excluded from validation"),
                ],
            )]),
        };
        let target =
            build_target_resolution(QQQ_NAV, &values, &diagnostics, ResolveMode::Relationships);
        assert_eq!(
            target.validation_error.as_deref(),
            Some(
                "MARKET_SECTOR_DES must equal Index (got Equity); \
                 NAME is missing; \
                 Bloomberg rejected target: BAD_SEC/15/INVALID_SECURITY: Rejected; \
                 Bloomberg field NAME failed: BAD_FLD/9/NOT_APPLICABLE_TO_REF_DATA: Name failed"
            )
        );
        assert_eq!(target.market_sector_des.as_deref(), Some("Equity"));
        assert!(target.name.is_none());
    }

    #[test]
    fn target_validation_accepts_index_sector_and_reports_null() {
        let values: RefValues = HashMap::from([(
            QQQ_NAV.to_string(),
            HashMap::from([
                (FIELD_MARKET_SECTOR_DES.to_string(), "index".to_string()),
                (FIELD_NAME.to_string(), "NASDAQ 100 NAV".to_string()),
                (FIELD_PX_LAST.to_string(), "500.25".to_string()),
            ]),
        )]);
        let diagnostics = ResponseDiagnostics::default();
        let valid = build_target_resolution(QQQ_NAV, &values, &diagnostics, ResolveMode::Snapshot);
        assert!(valid.validation_error.is_none());
        assert_eq!(valid.px_last, Some(500.25));
        assert_eq!(valid.name.as_deref(), Some("NASDAQ 100 NAV"));

        let relationships_mode =
            build_target_resolution(QQQ_NAV, &values, &diagnostics, ResolveMode::Relationships);
        assert_eq!(relationships_mode.px_last, None);

        let missing =
            build_target_resolution(AT1_INAV, &values, &diagnostics, ResolveMode::Snapshot);
        assert_eq!(
            missing.validation_error.as_deref(),
            Some("MARKET_SECTOR_DES must equal Index (got <null>); NAME is missing")
        );
        assert_eq!(missing.px_last, None);
    }

    #[test]
    fn snapshot_px_exception_defers_to_validation_error() {
        let diagnostics = ResponseDiagnostics {
            security_errors: BTreeMap::new(),
            field_exceptions: BTreeMap::from([(
                QQQ_NAV.to_string(),
                vec![field_exception("PX_LAST", "No price")],
            )]),
        };
        let targets = vec![QQQ_NAV.to_string()];

        let invalid_info = HashMap::from([(
            QQQ_NAV.to_string(),
            target(QQQ_NAV, Some("NAME is missing"), None),
        )]);
        assert!(
            enforce_snapshot_value_exceptions(&targets, &invalid_info, &diagnostics).is_ok(),
            "validation failure must win over the PX_LAST exception"
        );

        let resolutions = vec![resolution(
            0,
            QQQ,
            Some(target(QQQ_NAV, Some("NAME is missing"), None)),
            None,
        )];
        let error = ensure_targets_valid(&resolutions).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: ETF NAV target validation failed: {QQQ} NAV {QQQ_NAV}: NAME is missing"
            )
        );

        let valid_info = HashMap::from([(QQQ_NAV.to_string(), target(QQQ_NAV, None, None))]);
        let error =
            enforce_snapshot_value_exceptions(&targets, &valid_info, &diagnostics).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: Bloomberg value field PX_LAST failed for {QQQ_NAV}: \
                 BAD_FLD/9/NOT_APPLICABLE_TO_REF_DATA: No price"
            )
        );
    }

    #[test]
    fn ensure_targets_valid_orders_failures_nav_before_inav() {
        let resolutions = vec![
            resolution(
                0,
                QQQ,
                Some(target(QQQ_NAV, None, None)),
                Some(target(QQQ_INAV, Some("NAME is missing"), None)),
            ),
            resolution(
                1,
                AT1,
                Some(target(
                    "BAD Index",
                    Some("MARKET_SECTOR_DES must equal Index (got Equity)"),
                    None,
                )),
                Some(target(AT1_INAV, Some("NAME is missing"), None)),
            ),
        ];
        let error = ensure_targets_valid(&resolutions).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Recipe error: ETF NAV target validation failed: \
                 {QQQ} iNAV {QQQ_INAV}: NAME is missing; \
                 {AT1} NAV BAD Index: MARKET_SECTOR_DES must equal Index (got Equity); \
                 {AT1} iNAV {AT1_INAV}: NAME is missing"
            )
        );
    }

    #[test]
    fn relationship_mapping_keeps_navs_independent() {
        let batch = refdata_batch(&[
            (QQQ, "ETF_NAV_TICKER", Some("QQQNV")),
            (QQQ, "ETF_INAV_TICKER", Some("QXV")),
            (AT1, "ETF_NAV_TICKER", Some("nan")),
            (AT1, "ETF_INAV_TICKER", Some("AT1IN Index")),
        ]);
        let values = strict_refdata_value_map(&batch).unwrap();
        let sources = vec![QQQ.to_string(), AT1.to_string()];
        let targets = map_relationship_targets(&sources, &values);

        assert_eq!(targets[QQQ].nav.as_deref(), Some(QQQ_NAV));
        assert_eq!(targets[QQQ].inav.as_deref(), Some(QQQ_INAV));
        assert_eq!(targets[AT1].nav, None);
        assert_eq!(targets[AT1].inav.as_deref(), Some(AT1_INAV));

        let validation = validation_securities(&sources, &targets);
        assert_eq!(
            validation,
            vec![
                QQQ_NAV.to_string(),
                QQQ_INAV.to_string(),
                AT1_INAV.to_string()
            ]
        );
    }

    #[test]
    fn build_resolutions_preserves_input_order_and_duplicates() {
        let inputs = vec![QQQ.to_string(), AT1.to_string(), QQQ.to_string()];
        let targets = HashMap::from([
            (
                QQQ.to_string(),
                RelationshipTargets {
                    nav: Some(QQQ_NAV.to_string()),
                    inav: Some(QQQ_INAV.to_string()),
                },
            ),
            (
                AT1.to_string(),
                RelationshipTargets {
                    nav: None,
                    inav: Some(AT1_INAV.to_string()),
                },
            ),
        ]);
        let target_info = HashMap::from([
            (QQQ_NAV.to_string(), target(QQQ_NAV, None, Some(500.25))),
            (QQQ_INAV.to_string(), target(QQQ_INAV, None, Some(500.5))),
            (AT1_INAV.to_string(), target(AT1_INAV, None, Some(9.75))),
        ]);
        let resolutions = build_resolutions(&inputs, &targets, &target_info);

        assert_eq!(resolutions.len(), 3);
        assert_eq!(
            resolutions
                .iter()
                .map(|r| r.input_order)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(resolutions[0].nav.as_ref().unwrap().ticker, QQQ_NAV);
        assert!(resolutions[1].nav.is_none());
        assert_eq!(resolutions[1].inav.as_ref().unwrap().ticker, AT1_INAV);
        assert_eq!(resolutions[2], {
            let mut duplicate = resolutions[0].clone();
            duplicate.input_order = 2;
            duplicate
        });

        let batch = build_relationships_batch(&resolutions).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(string_at(&batch, "etf_ticker", 1).as_deref(), Some(AT1));
        assert_eq!(string_at(&batch, "nav_ticker", 1), None);
        assert_eq!(string_at(&batch, "nav_market_sector_des", 1), None);
        assert_eq!(string_at(&batch, "nav_validation_error", 1), None);
        assert_eq!(
            string_at(&batch, "inav_ticker", 1).as_deref(),
            Some(AT1_INAV)
        );
        assert_eq!(
            string_at(&batch, "inav_market_sector_des", 1).as_deref(),
            Some("Index")
        );
        assert_eq!(string_at(&batch, "nav_ticker", 2).as_deref(), Some(QQQ_NAV));
    }

    #[test]
    fn snapshot_fallback_is_planned_only_for_missing_navs() {
        let resolutions = vec![
            resolution(0, QQQ, Some(target(QQQ_NAV, None, Some(500.25))), None),
            resolution(1, AT1, None, Some(target(AT1_INAV, None, Some(9.75)))),
            resolution(2, AT1, None, Some(target(AT1_INAV, None, Some(9.75)))),
        ];
        assert_eq!(missing_nav_sources(&resolutions), vec![AT1.to_string()]);

        let all_mapped = vec![resolution(0, QQQ, Some(target(QQQ_NAV, None, None)), None)];
        assert!(
            missing_nav_sources(&all_mapped).is_empty(),
            "no fallback request may be built when every NAV target exists"
        );
    }

    #[test]
    fn snapshot_batch_reuses_px_last_and_falls_back_only_when_missing() {
        let resolutions = vec![
            resolution(
                0,
                QQQ,
                Some(target(QQQ_NAV, None, Some(500.25))),
                Some(target(QQQ_INAV, None, Some(500.5))),
            ),
            resolution(1, AT1, None, Some(target(AT1_INAV, None, Some(9.75)))),
            resolution(
                2,
                "MISSING US Equity",
                Some(target("M Index", None, None)),
                None,
            ),
        ];
        let fallback = HashMap::from([(AT1.to_string(), 10.5)]);
        let batch = build_snapshot_batch(&resolutions, &fallback).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(string_at(&batch, "nav_ticker", 0).as_deref(), Some(QQQ_NAV));
        assert_eq!(f64_at(&batch, "nav_value", 0), Some(500.25));
        assert_eq!(
            string_at(&batch, "nav_source_ticker", 0).as_deref(),
            Some(QQQ_NAV)
        );
        assert_eq!(
            string_at(&batch, "nav_source_field", 0).as_deref(),
            Some("PX_LAST")
        );
        assert_eq!(f64_at(&batch, "inav_value", 0), Some(500.5));

        assert_eq!(string_at(&batch, "nav_ticker", 1), None);
        assert_eq!(f64_at(&batch, "nav_value", 1), Some(10.5));
        assert_eq!(
            string_at(&batch, "nav_source_ticker", 1).as_deref(),
            Some(AT1)
        );
        assert_eq!(
            string_at(&batch, "nav_source_field", 1).as_deref(),
            Some("FUND_NET_ASSET_VAL")
        );
        assert_eq!(
            string_at(&batch, "inav_ticker", 1).as_deref(),
            Some(AT1_INAV)
        );
        assert_eq!(f64_at(&batch, "inav_value", 1), Some(9.75));

        // A mapped NAV with a missing PX_LAST stays on PX_LAST; it never
        // falls back to FUND_NET_ASSET_VAL.
        assert_eq!(
            string_at(&batch, "nav_ticker", 2).as_deref(),
            Some("M Index")
        );
        assert_eq!(f64_at(&batch, "nav_value", 2), None);
        assert_eq!(
            string_at(&batch, "nav_source_ticker", 2).as_deref(),
            Some("M Index")
        );
        assert_eq!(
            string_at(&batch, "nav_source_field", 2).as_deref(),
            Some("PX_LAST")
        );
        assert_eq!(string_at(&batch, "inav_ticker", 2), None);
        assert_eq!(f64_at(&batch, "inav_value", 2), None);
    }

    #[test]
    fn history_planning_unions_nav_then_inav_targets() {
        let resolutions = vec![
            resolution(
                0,
                QQQ,
                Some(target(QQQ_NAV, None, None)),
                Some(target(QQQ_INAV, None, None)),
            ),
            resolution(1, AT1, None, Some(target(AT1_INAV, None, None))),
        ];
        assert_eq!(
            px_history_targets(&resolutions),
            vec![
                QQQ_NAV.to_string(),
                QQQ_INAV.to_string(),
                AT1_INAV.to_string()
            ]
        );
        assert_eq!(missing_nav_sources(&resolutions), vec![AT1.to_string()]);
    }

    #[test]
    fn history_rows_union_dates_without_synthesis() {
        let d1 = date(2026, 7, 1);
        let d2 = date(2026, 7, 2);
        let d3 = date(2026, 7, 3);
        let px_batch = history_batch(&[
            (QQQ_NAV, "PX_LAST", Some(500.0), d1),
            (QQQ_NAV, "PX_LAST", Some(501.0), d2),
            (QQQ_INAV, "PX_LAST", Some(500.5), d2),
            (QQQ_INAV, "PX_LAST", Some(501.5), d3),
            (AT1_INAV, "PX_LAST", Some(9.9), d1),
        ]);
        let fund_batch = history_batch(&[(AT1, "FUND_NET_ASSET_VAL", Some(10.5), d2)]);
        let px_history = strict_history_value_map(&px_batch).unwrap();
        let fund_history = strict_history_value_map(&fund_batch).unwrap();

        let resolutions = vec![
            resolution(
                0,
                QQQ,
                Some(target(QQQ_NAV, None, None)),
                Some(target(QQQ_INAV, None, None)),
            ),
            resolution(1, AT1, None, Some(target(AT1_INAV, None, None))),
            resolution(2, AT1, None, Some(target(AT1_INAV, None, None))),
        ];
        let rows = build_history_rows(&resolutions, &px_history, &fund_history);

        let qqq_rows: Vec<_> = rows.iter().filter(|row| row.input_order == 0).collect();
        assert_eq!(
            qqq_rows.iter().map(|row| row.date).collect::<Vec<_>>(),
            vec![d1, d2, d3],
            "sorted union of NAV and iNAV dates"
        );
        assert_eq!(qqq_rows[0].nav_value, Some(500.0));
        assert_eq!(qqq_rows[0].inav_value, None);
        assert_eq!(qqq_rows[1].nav_value, Some(501.0));
        assert_eq!(qqq_rows[1].inav_value, Some(500.5));
        assert_eq!(qqq_rows[2].nav_value, None);
        assert_eq!(qqq_rows[2].inav_value, Some(501.5));
        assert!(qqq_rows
            .iter()
            .all(|row| row.nav_source_ticker == QQQ_NAV && row.nav_source_field == "PX_LAST"));

        let at1_rows: Vec<_> = rows.iter().filter(|row| row.input_order == 1).collect();
        assert_eq!(
            at1_rows.iter().map(|row| row.date).collect::<Vec<_>>(),
            vec![d1, d2],
            "AT1 unions fund NAV dates with iNAV dates"
        );
        assert_eq!(at1_rows[0].nav_value, None);
        assert_eq!(at1_rows[0].inav_value, Some(9.9));
        assert_eq!(at1_rows[1].nav_value, Some(10.5));
        assert_eq!(at1_rows[1].inav_value, None);
        assert!(at1_rows.iter().all(|row| {
            row.nav_ticker.is_none()
                && row.nav_source_ticker == AT1
                && row.nav_source_field == "FUND_NET_ASSET_VAL"
                && row.inav_ticker.as_deref() == Some(AT1_INAV)
        }));

        // Duplicate inputs re-emit the same ordered series.
        let duplicate_rows: Vec<_> = rows.iter().filter(|row| row.input_order == 2).collect();
        assert_eq!(duplicate_rows.len(), at1_rows.len());
        assert!(duplicate_rows
            .iter()
            .zip(&at1_rows)
            .all(|(a, b)| a.date == b.date && a.nav_value == b.nav_value));

        let batch = build_history_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), rows.len());
        assert_eq!(
            string_at(&batch, "nav_source_field", 3).as_deref(),
            Some("FUND_NET_ASSET_VAL")
        );
        let date_col = batch.column_by_name("date").unwrap();
        assert!(!date_col.is_null(0));
    }
}
