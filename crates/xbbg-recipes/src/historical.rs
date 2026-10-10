//! Historical data recipes.
//!
//! Convenience recipes for dividend, earnings, turnover, and ETF holdings data.
//!
//! # Recipes
//!
//! - [`recipe_dividend`]: Fetch dividend history
//! - [`recipe_earning`]: Fetch earnings data with hierarchical percentages
//! - [`recipe_turnover`]: Fetch volume/turnover data
//! - [`recipe_etf_holdings`]: Fetch ETF constituent holdings via BQL

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use crate::error::{RecipeError, Result};
use crate::utils::{
    apply_request_options, array_value_as_date, array_value_as_f64, array_value_as_string,
    as_string_col, canonical_name, find_column, naive_to_date32,
};
use arrow_array::RecordBatch;
use arrow_array::builder::{Date32Builder, Float64Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    new_null_array,
};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::take::take_record_batch;
use chrono::{Datelike, Duration, NaiveDate};
use xbbg_async::engine::{Engine, ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::constants::DVD_TYPES;
use xbbg_ext::transforms::bql::build_etf_holdings_query;
use xbbg_ext::transforms::historical::{apply_column_renames, calculate_level_percentages};
use xbbg_ext::{fmt_date, parse_date};

/// Fetch dividend or split bulk data using a dividend alias or Bloomberg field.
///
/// Empty dates omit the corresponding override. Bloomberg sub-field labels and
/// all supplied securities are preserved; no host-language column aliases or
/// equity-only filtering are applied. `DVD_TYPE` remains available in `options`.
pub async fn recipe_dividend(
    engine: &Engine,
    tickers: Vec<String>,
    dvd_type: Option<String>,
    start_date: String,
    end_date: String,
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = build_dividend_request(tickers, dvd_type, &start_date, &end_date, &options)?;
    engine.request(params).await.map_err(Into::into)
}

fn build_dividend_request(
    tickers: Vec<String>,
    dvd_type: Option<String>,
    start_date: &str,
    end_date: &str,
    options: &RequestParams,
) -> Result<RequestParams> {
    let field = match dvd_type {
        Some(typ) => match DVD_TYPES.get(typ.as_str()) {
            Some(field) => (*field).to_string(),
            None => typ,
        },
        None => "DVD_Hist_All".to_string(),
    };
    let mut overrides = Vec::new();
    if field.eq_ignore_ascii_case("Eqy_DVD_Adjust_Fact") {
        overrides.push((
            "Corporate_Actions_Filter".to_string(),
            "NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE".to_string(),
        ));
    }
    for (name, date) in [("DVD_Start_Dt", start_date), ("DVD_End_Dt", end_date)] {
        if !date.is_empty() {
            overrides.push((name.to_string(), fmt_date(parse_date(date)?, None)));
        }
    }
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers),
        fields: Some(vec![field]),
        overrides: to_option_overrides(overrides),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    Ok(params)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DividendEvent {
    ticker: String,
    ex_date: NaiveDate,
    declared_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    payable_date: Option<NaiveDate>,
    dividend_type: Option<String>,
    amount: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DividendYieldRow {
    ticker: String,
    date: NaiveDate,
    dividend_amount: Option<f64>,
    trailing_dividend_amount: Option<f64>,
    price: Option<f64>,
    dividend_yield: Option<f64>,
    dividend_type: Option<String>,
    declared_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    payable_date: Option<NaiveDate>,
}

/// Compute trailing realized dividend amount and trailing dividend yield.
pub async fn recipe_dividend_yield(
    engine: &Engine,
    tickers: Vec<String>,
    start_date: String,
    end_date: String,
    dividend_types: Option<Vec<String>>,
    window_days: Option<i32>,
) -> Result<RecordBatch> {
    let start = parse_date(&start_date)?;
    let end = parse_date(&end_date)?;
    if end < start {
        return Err(RecipeError::InvalidArgument(format!(
            "end_date {end_date} is before start_date {start_date}"
        )));
    }
    let window_days = window_days.unwrap_or(365).max(1);
    let event_start = start - Duration::days(window_days as i64);
    let dividend_type_filter = normalize_dividend_type_filter(dividend_types);

    let dividend_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers.clone()),
        fields: Some(vec!["DVD_HIST_ALL".to_string()]),
        overrides: Some(vec![
            ("DVD_Start_Dt".to_string(), fmt_date(event_start, None)),
            ("DVD_End_Dt".to_string(), fmt_date(end, None)),
        ]),
        ..Default::default()
    };
    let dividend_batch = engine.request(dividend_params).await?;
    let events = aggregate_dividend_events(extract_dividend_events(
        &dividend_batch,
        dividend_type_filter.as_ref(),
    )?);

    let price_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(tickers.clone()),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some(fmt_date(start, None)),
        end_date: Some(fmt_date(end, None)),
        ..Default::default()
    };
    let price_batch = engine.request(price_params).await?;
    let prices = extract_price_history(&price_batch)?;
    let rows = build_dividend_yield_rows(&tickers, start, end, window_days, &events, &prices);
    build_dividend_yield_batch(&rows)
}

fn normalize_dividend_type_filter(dividend_types: Option<Vec<String>>) -> Option<HashSet<String>> {
    dividend_types.map(|types| {
        types
            .into_iter()
            .map(|typ| canonical_name(&typ))
            .filter(|typ| !typ.is_empty())
            .collect::<HashSet<_>>()
    })
}

fn dividend_type_allowed(value: Option<&str>, filter: Option<&HashSet<String>>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.is_empty() {
        return true;
    }
    let Some(value) = value else {
        return false;
    };
    filter.contains(&canonical_name(value))
}

pub(crate) fn extract_dividend_events(
    batch: &RecordBatch,
    dividend_type_filter: Option<&HashSet<String>>,
) -> Result<Vec<DividendEvent>> {
    let ticker_col = batch
        .column_by_name("ticker")
        .ok_or_else(|| RecipeError::Other("missing 'ticker' column".to_string()))?;
    let ex_date_col = find_column(
        batch,
        &[
            "ex date",
            "ex-date",
            "ex_date",
            "dvd ex dt",
            "dividend ex date",
        ],
    )
    .and_then(|name| batch.column_by_name(&name))
    .ok_or_else(|| {
        RecipeError::Other("DVD_HIST_ALL response missing ex-date column".to_string())
    })?;
    let amount_col = find_column(
        batch,
        &[
            "amount",
            "dividend amount",
            "dvd amount",
            "cash amount",
            "gross amount",
        ],
    )
    .and_then(|name| batch.column_by_name(&name));
    let type_col = find_column(
        batch,
        &["dividend type", "dvd type", "type", "distribution type"],
    )
    .and_then(|name| batch.column_by_name(&name));
    let declared_col = find_column(
        batch,
        &[
            "declared date",
            "declaration date",
            "declared_date",
            "announcement date",
        ],
    )
    .and_then(|name| batch.column_by_name(&name));
    let record_col = find_column(batch, &["record date", "record_date"])
        .and_then(|name| batch.column_by_name(&name));
    let payable_col = find_column(
        batch,
        &["payable date", "payment date", "pay date", "payable_date"],
    )
    .and_then(|name| batch.column_by_name(&name));

    let mut events = Vec::new();
    for row in 0..batch.num_rows() {
        let Some(ticker) =
            array_value_as_string(ticker_col, row).map(|value| value.trim().to_string())
        else {
            continue;
        };
        if ticker.is_empty() {
            continue;
        }
        let Some(ex_date) = array_value_as_date(ex_date_col, row) else {
            continue;
        };
        let dividend_type = type_col
            .and_then(|col| array_value_as_string(col, row))
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if !dividend_type_allowed(dividend_type.as_deref(), dividend_type_filter) {
            continue;
        }

        events.push(DividendEvent {
            ticker,
            ex_date,
            declared_date: declared_col.and_then(|col| array_value_as_date(col, row)),
            record_date: record_col.and_then(|col| array_value_as_date(col, row)),
            payable_date: payable_col.and_then(|col| array_value_as_date(col, row)),
            dividend_type,
            amount: amount_col.and_then(|col| array_value_as_f64(col, row)),
        });
    }

    Ok(events)
}

pub(crate) fn aggregate_dividend_events(events: Vec<DividendEvent>) -> Vec<DividendEvent> {
    type EventKey = (
        String,
        NaiveDate,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<String>,
    );

    let mut grouped: HashMap<EventKey, DividendEvent> = HashMap::new();
    for event in events {
        let key = (
            event.ticker.clone(),
            event.ex_date,
            event.declared_date,
            event.record_date,
            event.payable_date,
            event.dividend_type.clone(),
        );
        grouped
            .entry(key)
            .and_modify(|existing| {
                existing.amount = match (existing.amount, event.amount) {
                    (Some(left), Some(right)) => Some(left + right),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    (None, None) => None,
                };
            })
            .or_insert(event);
    }

    let mut output = grouped.into_values().collect::<Vec<_>>();
    output.sort_by(|left, right| {
        left.ticker
            .cmp(&right.ticker)
            .then(left.ex_date.cmp(&right.ex_date))
            .then(left.declared_date.cmp(&right.declared_date))
            .then(left.record_date.cmp(&right.record_date))
            .then(left.payable_date.cmp(&right.payable_date))
            .then(left.dividend_type.cmp(&right.dividend_type))
    });
    output
}

fn extract_price_history(batch: &RecordBatch) -> Result<HashMap<(String, NaiveDate), f64>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let mut prices = HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        if !field_col.value(row).eq_ignore_ascii_case("PX_LAST") {
            continue;
        }
        let Some(date) = array_value_as_date(date_col, row) else {
            continue;
        };
        let Some(price) = array_value_as_f64(value_col, row) else {
            continue;
        };
        prices.insert((ticker_col.value(row).to_string(), date), price);
    }

    Ok(prices)
}

pub(crate) fn build_dividend_yield_rows(
    tickers: &[String],
    start: NaiveDate,
    end: NaiveDate,
    window_days: i32,
    events: &[DividendEvent],
    prices: &HashMap<(String, NaiveDate), f64>,
) -> Vec<DividendYieldRow> {
    // Sort references rather than cloning events. The ordinal keeps the prior
    // input order for same-day events, which defines the representative event.
    let mut events_by_ticker: HashMap<&str, Vec<(usize, &DividendEvent)>> = HashMap::new();
    for (ordinal, event) in events.iter().enumerate() {
        events_by_ticker
            .entry(event.ticker.as_str())
            .or_default()
            .push((ordinal, event));
    }
    for ticker_events in events_by_ticker.values_mut() {
        ticker_events.sort_by(|(left_ordinal, left), (right_ordinal, right)| {
            left.ex_date
                .cmp(&right.ex_date)
                .then(left_ordinal.cmp(right_ordinal))
        });
    }

    // Pre-group prices once: scanning every (ticker, date) price key per
    // ticker is O(tickers x total entries), and the tuple lookup below would
    // clone the ticker String per row.
    let mut prices_by_ticker: HashMap<&str, HashMap<NaiveDate, f64>> = HashMap::new();
    for ((price_ticker, date), price) in prices {
        prices_by_ticker
            .entry(price_ticker.as_str())
            .or_default()
            .insert(*date, *price);
    }

    let mut rows = Vec::new();
    for ticker in tickers {
        let ticker_events = events_by_ticker
            .get(ticker.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let ticker_prices = prices_by_ticker.get(ticker.as_str());
        let mut dates = BTreeSet::new();
        if let Some(ticker_prices) = ticker_prices {
            for date in ticker_prices.keys() {
                if *date >= start && *date <= end {
                    dates.insert(*date);
                }
            }
        }
        for (_, event) in ticker_events {
            if event.ex_date >= start && event.ex_date <= end {
                dates.insert(event.ex_date);
            }
        }

        let mut window_start = 0;
        let mut window_end = 0;
        let mut window_amount_count = 0usize;
        let mut window_total = 0.0;
        let mut same_day_start = 0;

        for date in dates {
            while window_end < ticker_events.len() && ticker_events[window_end].1.ex_date <= date {
                if let Some(amount) = ticker_events[window_end].1.amount {
                    window_total += amount;
                    window_amount_count += 1;
                }
                window_end += 1;
            }

            let trailing_start = date - Duration::days(window_days as i64);
            let previous_window_start = window_start;
            while window_start < window_end
                && ticker_events[window_start].1.ex_date <= trailing_start
            {
                window_start += 1;
            }

            // Floating-point subtraction cannot undo an earlier rounded add
            // (and cannot recover after NaN/infinity expires). Re-fold only
            // when the left edge moves; dates without expiries stay O(1).
            if window_start != previous_window_start {
                window_amount_count = 0;
                window_total = 0.0;
                for (_, event) in &ticker_events[window_start..window_end] {
                    if let Some(amount) = event.amount {
                        window_total += amount;
                        window_amount_count += 1;
                    }
                }
            }

            while same_day_start < ticker_events.len()
                && ticker_events[same_day_start].1.ex_date < date
            {
                same_day_start += 1;
            }
            let mut same_day_end = same_day_start;
            while same_day_end < ticker_events.len()
                && ticker_events[same_day_end].1.ex_date == date
            {
                same_day_end += 1;
            }
            let same_day_events = &ticker_events[same_day_start..same_day_end];
            let dividend_amount =
                sum_optional(same_day_events.iter().filter_map(|(_, event)| event.amount));
            let trailing_dividend_amount = (window_amount_count != 0).then_some(window_total);
            let price = ticker_prices.and_then(|prices| prices.get(&date)).copied();
            let dividend_yield = match (trailing_dividend_amount, price) {
                (Some(amount), Some(price)) if price != 0.0 => Some(amount / price),
                _ => None,
            };
            let representative = same_day_events.first().map(|(_, event)| *event);

            rows.push(DividendYieldRow {
                ticker: ticker.clone(),
                date,
                dividend_amount,
                trailing_dividend_amount,
                price,
                dividend_yield,
                dividend_type: representative.and_then(|event| event.dividend_type.clone()),
                declared_date: representative.and_then(|event| event.declared_date),
                record_date: representative.and_then(|event| event.record_date),
                payable_date: representative.and_then(|event| event.payable_date),
            });
        }
    }

    rows.sort_by(|left, right| {
        left.ticker
            .cmp(&right.ticker)
            .then(left.date.cmp(&right.date))
    });
    rows
}

fn sum_optional(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut seen = false;
    let mut total = 0.0;
    for value in values {
        seen = true;
        total += value;
    }
    seen.then_some(total)
}

fn build_dividend_yield_batch(rows: &[DividendYieldRow]) -> Result<RecordBatch> {
    let mut ticker = StringBuilder::new();
    let mut date = Date32Builder::new();
    let mut amount = Float64Builder::new();
    let mut trailing = Float64Builder::new();
    let mut price = Float64Builder::new();
    let mut yield_builder = Float64Builder::new();
    let mut dividend_type = StringBuilder::new();
    let mut declared = Date32Builder::new();
    let mut record = Date32Builder::new();
    let mut payable = Date32Builder::new();

    for row in rows {
        ticker.append_value(&row.ticker);
        date.append_value(naive_to_date32(row.date));
        amount.append_option(row.dividend_amount);
        trailing.append_option(row.trailing_dividend_amount);
        price.append_option(row.price);
        yield_builder.append_option(row.dividend_yield);
        dividend_type.append_option(row.dividend_type.as_deref());
        declared.append_option(row.declared_date.map(naive_to_date32));
        record.append_option(row.record_date.map(naive_to_date32));
        payable.append_option(row.payable_date.map(naive_to_date32));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("dividend_amount", DataType::Float64, true),
        Field::new("trailing_dividend_amount", DataType::Float64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("dividend_yield", DataType::Float64, true),
        Field::new("dividend_type", DataType::Utf8, true),
        Field::new("declared_date", DataType::Date32, true),
        Field::new("record_date", DataType::Date32, true),
        Field::new("payable_date", DataType::Date32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ticker.finish()),
            Arc::new(date.finish()),
            Arc::new(amount.finish()),
            Arc::new(trailing.finish()),
            Arc::new(price.finish()),
            Arc::new(yield_builder.finish()),
            Arc::new(dividend_type.finish()),
            Arc::new(declared.finish()),
            Arc::new(record.finish()),
            Arc::new(payable.finish()),
        ],
    )
    .map_err(Into::into)
}

/// Fetch earnings bulk data and derive hierarchical percentages.
///
/// Workflow:
/// 1. Query `PG_Bulk_Header` using the bulk extractor to discover dynamic period labels.
/// 2. Query `PG_{typ}` for the actual earnings values.
/// 3. Rename period columns using header-derived names (for example, `Period 1 Value` -> `fy2024`).
/// 4. Add `{period}_pct` columns using hierarchy semantics:
///    - level 1 rows: percentage of total level 1 sum
///    - level 2 rows: percentage of parent level 1 group sum
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `tickers` - Securities to query
/// * `by` - Breakdown (`Geo`/`Product`) or period granularity (`Q`/`A`)
/// * `typ` - `IS`/`BS`/`CF`, `Revenue`, `Operating_Income`, `Assets`,
///   `Gross_Profit`, or `Capital_Expenditures` (an optional `PG_` prefix is accepted)
/// * `ccy` - Currency override
/// * `level` - Optional hierarchy level filter (`1` or `2`)
/// * `year` - Fiscal year override; zero omits the override
/// * `periods` - Number of periods; zero omits the override
/// * `options` - Additional request options applied to both bulk requests
#[allow(clippy::too_many_arguments)]
pub async fn recipe_earning(
    engine: &Engine,
    tickers: Vec<String>,
    by: Option<String>,
    typ: String,
    ccy: Option<String>,
    level: Option<i32>,
    year: Option<i32>,
    periods: Option<i32>,
    options: RequestParams,
) -> Result<RecordBatch> {
    let (header_params, data_params) = build_earning_requests(
        tickers,
        by.as_deref(),
        &typ,
        ccy.as_deref(),
        level,
        year,
        periods,
        &options,
    )?;
    let header_batch = engine.request(header_params).await?;
    let data_batch = engine.request(data_params).await?;
    shape_earning_output(&header_batch, data_batch)
}

#[allow(clippy::too_many_arguments)]
fn build_earning_requests(
    tickers: Vec<String>,
    by: Option<&str>,
    typ: &str,
    ccy: Option<&str>,
    level: Option<i32>,
    year: Option<i32>,
    periods: Option<i32>,
    options: &RequestParams,
) -> Result<(RequestParams, RequestParams)> {
    let typ = normalize_earning_type(typ)?;
    let (header_overrides, data_overrides) =
        build_earning_overrides(by, ccy, level, year, periods)?;
    let mut header = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers.clone()),
        fields: Some(vec!["PG_Bulk_Header".to_string()]),
        overrides: to_option_overrides(header_overrides),
        ..Default::default()
    };
    let mut data = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers),
        fields: Some(vec![format!("PG_{typ}")]),
        overrides: to_option_overrides(data_overrides),
        ..Default::default()
    };
    for params in [&mut header, &mut data] {
        apply_request_options(params, options);
        // Bulk sub-fields are needed for the header join and hierarchy calculation.
        params.format = None;
    }
    Ok((header, data))
}

fn shape_earning_output(
    header_batch: &RecordBatch,
    mut data_batch: RecordBatch,
) -> Result<RecordBatch> {
    if header_batch.num_rows() > 0 && data_batch.num_rows() > 0 {
        let renames = build_earning_header_rename(header_batch, &data_batch);
        if !renames.is_empty() {
            data_batch = apply_column_renames(&data_batch, &renames)?;
        }
    }
    add_earning_percentage_columns(data_batch)
}

fn normalize_earning_type(typ: &str) -> Result<String> {
    let normalized = typ.trim().to_ascii_uppercase();
    let normalized = normalized.strip_prefix("PG_").unwrap_or(&normalized);
    match normalized {
        "IS" | "BS" | "CF" => Ok(normalized.to_string()),
        "REVENUE" => Ok("Revenue".to_string()),
        "OPERATING_INCOME" => Ok("Operating_Income".to_string()),
        "ASSETS" => Ok("Assets".to_string()),
        "GROSS_PROFIT" => Ok("Gross_Profit".to_string()),
        "CAPITAL_EXPENDITURES" => Ok("Capital_Expenditures".to_string()),
        _ => Err(RecipeError::InvalidArgument(format!(
            "unsupported earning type '{typ}', expected IS/BS/CF, Revenue, \
             Operating_Income, Assets, Gross_Profit, or Capital_Expenditures"
        ))),
    }
}

type OverridePairs = Vec<(String, String)>;
type EarningOverrides = (OverridePairs, OverridePairs);

fn build_earning_overrides(
    by: Option<&str>,
    ccy: Option<&str>,
    level: Option<i32>,
    year: Option<i32>,
    periods: Option<i32>,
) -> Result<EarningOverrides> {
    let mut header_overrides = Vec::new();
    let breakdown = by.unwrap_or("").trim().to_ascii_uppercase();
    match breakdown.as_str() {
        "" => {}
        "Q" | "A" => header_overrides.push(("PER".to_string(), breakdown.clone())),
        "GEO" | "PRODUCT" => header_overrides.push((
            "Product_Geo_Override".to_string(),
            if breakdown == "GEO" { "G" } else { "P" }.to_string(),
        )),
        _ => {
            return Err(RecipeError::InvalidArgument(format!(
                "unsupported by='{breakdown}', expected Geo, Product, Q, or A"
            )));
        }
    }
    for (name, value) in [("Eqy_Fund_Year", year), ("Number_Of_Periods", periods)] {
        if let Some(value) = value.filter(|value| *value != 0) {
            header_overrides.push((name.to_string(), value.to_string()));
        }
    }
    let mut data_overrides = header_overrides.clone();
    if let Some(currency) = ccy {
        let currency = currency.trim().to_ascii_uppercase();
        if !currency.is_empty() {
            let field = if breakdown == "GEO" || breakdown == "PRODUCT" {
                "Eqy_Fund_Crncy"
            } else {
                "CURRENCY"
            };
            data_overrides.push((field.to_string(), currency));
        }
    }
    if let Some(hierarchy_level) = level {
        if hierarchy_level != 1 && hierarchy_level != 2 {
            return Err(RecipeError::InvalidArgument(format!(
                "unsupported level='{hierarchy_level}', expected 1 or 2"
            )));
        }
        data_overrides.push((
            "PG_Hierarchy_Level".to_string(),
            hierarchy_level.to_string(),
        ));
    }
    Ok((header_overrides, data_overrides))
}

fn to_option_overrides(overrides: Vec<(String, String)>) -> Option<Vec<(String, String)>> {
    if overrides.is_empty() {
        None
    } else {
        Some(overrides)
    }
}

fn build_earning_header_rename(
    header_batch: &RecordBatch,
    data_batch: &RecordBatch,
) -> Vec<(String, String)> {
    let mut header_values: HashMap<String, String> = HashMap::new();

    for field in header_batch.schema().fields() {
        let column_name = field.name();
        if column_name == "ticker" || column_name == "field" {
            continue;
        }

        let Some(column) = header_batch.column_by_name(column_name) else {
            continue;
        };

        let first_value = (0..header_batch.num_rows())
            .find_map(|idx| array_value_as_string(column, idx))
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty());

        if let Some(value) = first_value {
            header_values.insert(column_name.to_string(), value);
        }
    }

    if header_values.is_empty() {
        return Vec::new();
    }

    let mut used_names: HashSet<String> = data_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect();

    let mut renames = Vec::new();
    for field in data_batch.schema().fields() {
        let data_col = field.name();
        if data_col == "ticker" || data_col == "field" {
            continue;
        }

        let header_col = if let Some(period_col) = data_col.strip_suffix(" Value") {
            format!("{period_col} Header")
        } else {
            format!("{data_col} Header")
        };

        let Some(raw_header_value) = header_values.get(&header_col) else {
            continue;
        };

        let normalized_name = normalize_earning_header_value(raw_header_value);
        if normalized_name.is_empty() {
            continue;
        }

        let normalized_key = normalized_name.to_ascii_lowercase();
        if normalized_key == data_col.to_ascii_lowercase() || used_names.contains(&normalized_key) {
            continue;
        }

        renames.push((data_col.to_string(), normalized_name.clone()));
        used_names.insert(normalized_key);
    }

    renames
}

fn normalize_earning_header_value(value: &str) -> String {
    let mut normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '/', '.'], "_");

    while normalized.contains("__") {
        normalized = normalized.replace("__", "_");
    }

    normalized = normalized.replace("_20", "20");
    normalized.trim_matches('_').to_string()
}

fn add_earning_percentage_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let Some(level_col_name) = find_column(&batch, &["level"]) else {
        return Ok(batch);
    };

    let Some(level_col) = batch.column_by_name(&level_col_name) else {
        return Ok(batch);
    };
    let levels = extract_level_values(level_col);

    if levels.iter().all(Option::is_none) {
        return Ok(batch);
    }

    let value_cols = earning_value_columns(&batch);
    let mut output = batch;

    for value_col in value_cols {
        let pct_col = format!("{value_col}_pct");
        if output.column_by_name(&pct_col).is_some() {
            continue;
        }

        let Some(values_col) = output.column_by_name(&value_col) else {
            continue;
        };

        let values = extract_numeric_values(values_col);
        if values.iter().all(Option::is_none) {
            continue;
        }

        let percentages = calculate_level_percentages(&values, &levels);
        output = insert_pct_column_after(&output, &value_col, &pct_col, percentages)?;
    }

    Ok(output)
}

fn earning_value_columns(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name())
        .filter(|name| is_earning_value_column(name))
        .cloned()
        .collect()
}

fn is_earning_value_column(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    (lower.starts_with("fy") && !lower.ends_with("_pct")) || lower.ends_with(" value")
}

fn extract_numeric_values(array: &ArrayRef) -> Vec<Option<f64>> {
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as f64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as f64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_f64_like(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_f64_like(arr.value(idx))
                }
            })
            .collect();
    }

    vec![None; array.len()]
}

fn extract_level_values(array: &ArrayRef) -> Vec<Option<i64>> {
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as i64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    let v = arr.value(idx);
                    if v.is_finite() && v.fract() == 0.0 {
                        Some(v as i64)
                    } else {
                        None
                    }
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_i64_like(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_i64_like(arr.value(idx))
                }
            })
            .collect();
    }

    vec![None; array.len()]
}

fn parse_f64_like(value: &str) -> Option<f64> {
    let cleaned = value.trim().replace(',', "");
    if cleaned.is_empty() {
        None
    } else {
        cleaned.parse::<f64>().ok()
    }
}

fn parse_i64_like(value: &str) -> Option<i64> {
    let cleaned = value.trim();
    if cleaned.is_empty() {
        return None;
    }

    if let Ok(parsed) = cleaned.parse::<i64>() {
        return Some(parsed);
    }

    let parsed = cleaned.parse::<f64>().ok()?;
    if parsed.is_finite() && parsed.fract() == 0.0 {
        Some(parsed as i64)
    } else {
        None
    }
}

fn insert_pct_column_after(
    batch: &RecordBatch,
    after_col: &str,
    pct_col: &str,
    percentages: Vec<Option<f64>>,
) -> Result<RecordBatch> {
    if percentages.len() != batch.num_rows() {
        return Err(crate::error::RecipeError::Other(format!(
            "percentage length mismatch for '{pct_col}'"
        )));
    }

    let insert_after_idx = batch
        .schema()
        .index_of(after_col)
        .map_err(|_| crate::error::RecipeError::Other(format!("missing '{after_col}' column")))?;

    let pct_array: ArrayRef = Arc::new(Float64Array::from(percentages));

    let mut fields = Vec::with_capacity(batch.num_columns() + 1);
    let mut columns = Vec::with_capacity(batch.num_columns() + 1);

    for idx in 0..batch.num_columns() {
        fields.push(batch.schema().field(idx).as_ref().clone());
        columns.push(batch.column(idx).clone());

        if idx == insert_after_idx {
            fields.push(Field::new(pct_col, DataType::Float64, true));
            columns.push(pct_array.clone());
        }
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

/// Fetch turnover, falling back to volume × VWAP for securities with no rows.
///
/// Empty dates default to yesterday and 30 days before the end date. `None`
/// currency means local currency; `None` factor means 1. Conversion precedes
/// scaling. Internal requests use long format; the requested output shape is
/// applied after calculations. Request failures propagate, and missing FX rows
/// become null rather than mixing local and converted values.
///
/// Output uses `TURNOVER` and Date32 dates throughout. Long values are strings
/// unless the direct request supplied numeric values; wide and typed values are
/// Float64. Malformed direct text becomes null. Fallback rows are ordered by
/// requested ticker and date, with missing or malformed pairs omitted.
/// Excel-style date visibility, period labels, sorting and orientation controls
/// are consumed locally, after fallback, conversion and scaling. An explicit
/// output format takes precedence over orientation.
pub async fn recipe_turnover(
    engine: &Engine,
    tickers: Vec<String>,
    start_date: String,
    end_date: String,
    ccy: Option<String>,
    factor: Option<f64>,
    mut options: RequestParams,
) -> Result<RecordBatch> {
    let (start, end) = turnover_dates(&start_date, &end_date)?;
    let factor = turnover_factor(factor)?;
    let presentation = take_turnover_presentation(&mut options)?;
    let format = TurnoverFormat::parse(options.format.as_deref())?;
    let params = build_turnover_request(tickers.clone(), &start, &end, false, &options);
    let direct = engine.request(params).await?;
    let numeric_long = direct
        .column_by_name("value")
        .is_some_and(|column| column.data_type().is_numeric());
    let mut metadata = direct.schema().metadata().clone();
    let mut rows = extract_turnover_rows(&direct)?;
    let missing = missing_turnover_tickers(&tickers, &rows);
    if !missing.is_empty() {
        let params = build_turnover_request(missing.clone(), &start, &end, true, &options);
        let fallback = engine.request(params).await?;
        append_turnover_fallback(&mut rows, &missing, &fallback)?;
        merge_turnover_metadata(&mut metadata, fallback.schema().metadata());
    }
    let mut data = build_turnover_batch(rows, metadata)?;
    if let Some(target) = ccy.filter(|value| !value.eq_ignore_ascii_case("local")) {
        data =
            crate::currency::recipe_adjust_ccy(engine, data, target, start, end, options).await?;
    }
    let output = shape_turnover_output(data, factor, format, numeric_long)?;
    apply_turnover_presentation(output, &presentation)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum TurnoverDateFormat {
    #[default]
    Date,
    Periodic,
    Both,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum TurnoverPeriodicity {
    #[default]
    Daily,
    Weekly,
    Monthly,
    Quarterly,
    SemiAnnual,
    Yearly,
}

impl TurnoverPeriodicity {
    fn parse(value: Option<&str>) -> Self {
        let value = value.unwrap_or("DAILY");
        if ["YEARLY", "Y"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            Self::Yearly
        } else if ["SEMI_ANNUALLY", "SEMIANNUALLY", "S"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            Self::SemiAnnual
        } else if ["QUARTERLY", "Q"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            Self::Quarterly
        } else if ["MONTHLY", "M"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            Self::Monthly
        } else if ["WEEKLY", "W"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            Self::Weekly
        } else {
            Self::Daily
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TurnoverPresentation {
    show_date: bool,
    date_format: TurnoverDateFormat,
    descending: Option<bool>,
    periodicity: TurnoverPeriodicity,
}

fn take_turnover_presentation(options: &mut RequestParams) -> Result<TurnoverPresentation> {
    // The engine applies elements, routed kwargs, then explicit options.
    let periodicity = options
        .options
        .iter()
        .flatten()
        .rev()
        .find(|(name, _)| name.eq_ignore_ascii_case("periodicitySelection"))
        .map(|(_, value)| value.as_str())
        .or_else(|| {
            options
                .kwargs
                .as_ref()?
                .get("periodicitySelection")
                .map(String::as_str)
        })
        .or_else(|| {
            options
                .elements
                .iter()
                .flatten()
                .rev()
                .find(|(name, _)| name.eq_ignore_ascii_case("periodicitySelection"))
                .map(|(_, value)| value.as_str())
        });
    let mut presentation = TurnoverPresentation {
        show_date: true,
        date_format: TurnoverDateFormat::Date,
        descending: None,
        periodicity: TurnoverPeriodicity::parse(periodicity),
    };
    let Some(kwargs) = options.kwargs.as_mut() else {
        return Ok(presentation);
    };
    if let Some(value) = take_turnover_alias(kwargs, &["Dts", "Dates", "show_date"]) {
        presentation.show_date = if ["Show", "S", "True", "1"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            true
        } else if ["Hide", "H", "False", "0"]
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            false
        } else {
            return Err(RecipeError::InvalidArgument(format!(
                "unsupported show_date '{value}', expected Show/Hide or true/false"
            )));
        };
    }
    if let Some(value) = take_turnover_alias(kwargs, &["DtFmt", "DateFormat", "date_format"]) {
        presentation.date_format =
            if value.eq_ignore_ascii_case("B") || value.eq_ignore_ascii_case("BOTH") {
                TurnoverDateFormat::Both
            } else if value.eq_ignore_ascii_case("P") || value.eq_ignore_ascii_case("PERIODIC") {
                TurnoverDateFormat::Periodic
            } else {
                TurnoverDateFormat::Date
            };
    }
    if let Some(value) = take_turnover_alias(kwargs, &["Sort", "sort"]) {
        presentation.descending = Some(
            ["R", "D", "Descend", "Reverse", "True", "1", "DESCENDING"]
                .iter()
                .any(|alias| value.eq_ignore_ascii_case(alias)),
        );
    }
    if let Some(value) =
        take_turnover_alias(kwargs, &["Orientation", "Direction", "Dir", "orientation"])
        && options.format.is_none()
    {
        if value.eq_ignore_ascii_case("H") || value.eq_ignore_ascii_case("HORIZONTAL") {
            options.format = Some("wide".to_string());
        } else if value.eq_ignore_ascii_case("V") || value.eq_ignore_ascii_case("VERTICAL") {
            options.format = Some("long".to_string());
        }
    }
    Ok(presentation)
}

fn take_turnover_alias(kwargs: &mut HashMap<String, String>, aliases: &[&str]) -> Option<String> {
    let mut selected = None;
    // RequestParams uses a HashMap, so canonical keys take precedence over
    // aliases rather than depending on lost caller insertion order.
    for alias in aliases {
        if let Some(value) = kwargs.remove(*alias) {
            selected = Some(value);
        }
    }
    selected
}

fn apply_turnover_presentation(
    mut batch: RecordBatch,
    presentation: &TurnoverPresentation,
) -> Result<RecordBatch> {
    if let Some(descending) = presentation.descending
        && batch.num_rows() > 1
        && batch.column_by_name("date").is_some()
    {
        let mut columns = Vec::with_capacity(3);
        for (name, descending) in [("ticker", false), ("date", descending), ("field", false)] {
            if let Some(column) = batch.column_by_name(name) {
                columns.push(SortColumn {
                    values: column.clone(),
                    options: Some(SortOptions {
                        descending,
                        nulls_first: true,
                    }),
                });
            }
        }
        let indices = lexsort_to_indices(&columns, None)?;
        if indices
            .values()
            .iter()
            .enumerate()
            .any(|(row, &index)| row != index as usize)
        {
            batch = take_record_batch(&batch, &indices)?;
        }
    }
    if !presentation.show_date {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (!matches!(field.name().as_str(), "date" | "period")).then_some(index)
            })
            .collect::<Vec<_>>();
        return batch.project(&columns).map_err(Into::into);
    }
    if presentation.date_format == TurnoverDateFormat::Date {
        return Ok(batch);
    }
    let Ok(date_index) = batch.schema().index_of("date") else {
        return Ok(batch);
    };
    let dates = batch.column(date_index);
    let mut periods =
        StringBuilder::with_capacity(batch.num_rows(), batch.num_rows().saturating_mul(10));
    let mut label = String::with_capacity(10);
    for row in 0..batch.num_rows() {
        format_turnover_period_label(dates, row, presentation.periodicity, &mut label);
        periods.append_value(&label);
    }
    let period_field = Arc::new(Field::new("period", DataType::Utf8, true));
    let period_values: ArrayRef = Arc::new(periods.finish());
    let mut fields = batch.schema().fields().to_vec();
    let mut columns = batch.columns().to_vec();
    if presentation.date_format == TurnoverDateFormat::Periodic {
        fields[date_index] = period_field;
        columns[date_index] = period_values;
    } else {
        fields.insert(date_index + 1, period_field);
        columns.insert(date_index + 1, period_values);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(Into::into)
}

fn format_turnover_period_label(
    dates: &ArrayRef,
    row: usize,
    periodicity: TurnoverPeriodicity,
    label: &mut String,
) {
    label.clear();
    let Some(date) = array_value_as_date(dates, row) else {
        if row < dates.len() && !dates.is_null(row) {
            if let Some(strings) = dates.as_any().downcast_ref::<StringArray>() {
                label.push_str(strings.value(row));
            } else if let Some(strings) = dates.as_any().downcast_ref::<LargeStringArray>() {
                label.push_str(strings.value(row));
            } else if let Some(value) = array_value_as_string(dates, row) {
                label.push_str(&value);
            }
        }
        return;
    };
    match periodicity {
        TurnoverPeriodicity::Yearly => write!(label, "{:04}", date.year()),
        TurnoverPeriodicity::SemiAnnual => {
            write!(
                label,
                "{:04}H{}",
                date.year(),
                if date.month() <= 6 { 1 } else { 2 }
            )
        }
        TurnoverPeriodicity::Quarterly => {
            write!(label, "{:04}Q{}", date.year(), ((date.month() - 1) / 3) + 1)
        }
        TurnoverPeriodicity::Monthly => write!(label, "{:04}-{:02}", date.year(), date.month()),
        TurnoverPeriodicity::Weekly => {
            let week = date.iso_week();
            write!(label, "{:04}-W{:02}", week.year(), week.week())
        }
        TurnoverPeriodicity::Daily => write!(label, "{date}"),
    }
    .expect("formatting into a String cannot fail");
}

fn turnover_dates(start_date: &str, end_date: &str) -> Result<(String, String)> {
    let end = if end_date.is_empty() {
        chrono::Local::now().date_naive() - Duration::days(1)
    } else {
        parse_date(end_date)?
    };
    let start = if start_date.is_empty() {
        end - Duration::days(30)
    } else {
        parse_date(start_date)?
    };
    Ok((fmt_date(start, None), fmt_date(end, None)))
}

fn turnover_factor(factor: Option<f64>) -> Result<f64> {
    let factor = factor.unwrap_or(1.0);
    if !factor.is_finite() || factor == 0.0 {
        return Err(RecipeError::InvalidArgument(
            "turnover factor must be finite and nonzero".to_string(),
        ));
    }
    Ok(factor)
}

fn build_turnover_request(
    tickers: Vec<String>,
    start: &str,
    end: &str,
    fallback: bool,
    options: &RequestParams,
) -> RequestParams {
    let fields = if fallback {
        vec!["EQY_WEIGHTED_AVG_PX".to_string(), "VOLUME".to_string()]
    } else {
        vec!["TURNOVER".to_string()]
    };
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(tickers),
        fields: Some(fields),
        start_date: Some(start.to_string()),
        end_date: Some(end.to_string()),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    params.format = Some("long".to_string());
    params
}

#[derive(Debug, PartialEq)]
struct TurnoverRow {
    ticker: String,
    date: Option<i32>,
    value: Option<f64>,
}

fn extract_turnover_rows(batch: &RecordBatch) -> Result<Vec<TurnoverRow>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let tickers = as_string_col(batch, "ticker")?;
    let fields = as_string_col(batch, "field")?;
    let dates = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("turnover response missing 'date'".to_string()))?;
    let values = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("turnover response missing 'value'".to_string()))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if tickers.is_null(row)
            || fields.is_null(row)
            || !fields.value(row).eq_ignore_ascii_case("TURNOVER")
        {
            continue;
        }
        rows.push(TurnoverRow {
            ticker: tickers.value(row).to_string(),
            date: array_value_as_date(dates, row).map(naive_to_date32),
            value: array_value_as_f64(values, row),
        });
    }
    Ok(rows)
}

fn missing_turnover_tickers(tickers: &[String], rows: &[TurnoverRow]) -> Vec<String> {
    let mut seen: HashSet<&str> = rows.iter().map(|row| row.ticker.as_str()).collect();
    tickers
        .iter()
        .filter(|ticker| seen.insert(ticker.as_str()))
        .cloned()
        .collect()
}

#[derive(Default)]
struct TurnoverInput {
    value: Option<f64>,
    present: bool,
}

fn append_turnover_fallback(
    rows: &mut Vec<TurnoverRow>,
    missing: &[String],
    batch: &RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let tickers = as_string_col(batch, "ticker")?;
    let fields = as_string_col(batch, "field")?;
    let dates = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("volume response missing 'date'".to_string()))?;
    let values = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("volume response missing 'value'".to_string()))?;
    let mut inputs: HashMap<&str, BTreeMap<i32, [TurnoverInput; 2]>> = missing
        .iter()
        .map(|ticker| (ticker.as_str(), BTreeMap::new()))
        .collect();
    for row in 0..batch.num_rows() {
        if tickers.is_null(row) || fields.is_null(row) {
            continue;
        }
        let Some(by_date) = inputs.get_mut(tickers.value(row)) else {
            continue;
        };
        let field = fields.value(row);
        let index = if field.eq_ignore_ascii_case("EQY_WEIGHTED_AVG_PX") {
            0
        } else if field.eq_ignore_ascii_case("VOLUME") {
            1
        } else {
            continue;
        };
        let Some(date) = array_value_as_date(dates, row) else {
            continue;
        };
        let input = &mut by_date.entry(naive_to_date32(date)).or_default()[index];
        input.value = array_value_as_f64(values, row);
        input.present = if let Some(strings) = values.as_any().downcast_ref::<StringArray>() {
            !strings.is_null(row) && !strings.value(row).trim().is_empty()
        } else if let Some(strings) = values.as_any().downcast_ref::<LargeStringArray>() {
            !strings.is_null(row) && !strings.value(row).trim().is_empty()
        } else {
            !values.is_null(row)
        };
    }
    let mut malformed_rows = 0usize;
    for ticker in missing {
        let Some(by_date) = inputs.remove(ticker.as_str()) else {
            continue;
        };
        for (date, [vwap, volume]) in by_date {
            match (vwap.value, volume.value) {
                (Some(vwap), Some(volume)) => rows.push(TurnoverRow {
                    ticker: ticker.clone(),
                    date: Some(date),
                    value: Some(vwap * volume),
                }),
                _ if vwap.present && volume.present => malformed_rows += 1,
                _ => {}
            }
        }
    }
    if malformed_rows > 0 {
        xbbg_log::warn!(
            malformed_rows,
            "Turnover volume fallback skipped malformed values"
        );
    }
    Ok(())
}

fn merge_turnover_metadata(
    metadata: &mut HashMap<String, String>,
    extra: &HashMap<String, String>,
) {
    for (key, value) in extra {
        if let Some(previous) = metadata.get_mut(key) {
            if key.starts_with("xbbg.")
                && let (
                    Ok(serde_json::Value::Object(mut left)),
                    Ok(serde_json::Value::Object(right)),
                ) = (serde_json::from_str(previous), serde_json::from_str(value))
            {
                left.extend(right);
                *previous = serde_json::Value::Object(left).to_string();
            }
        } else {
            metadata.insert(key.clone(), value.clone());
        }
    }
}

fn build_turnover_batch(
    rows: Vec<TurnoverRow>,
    metadata: HashMap<String, String>,
) -> Result<RecordBatch> {
    let mut tickers = StringBuilder::with_capacity(rows.len(), rows.len() * 24);
    let mut dates = Date32Builder::with_capacity(rows.len());
    let mut fields = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut values = Float64Builder::with_capacity(rows.len());
    for row in rows {
        tickers.append_value(row.ticker);
        dates.append_option(row.date);
        fields.append_value("TURNOVER");
        values.append_option(row.value);
    }
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ],
        metadata,
    );
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(tickers.finish()),
            Arc::new(dates.finish()),
            Arc::new(fields.finish()),
            Arc::new(values.finish()),
        ],
    )
    .map_err(Into::into)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TurnoverFormat {
    Long,
    Wide,
    Typed,
    Metadata,
}

impl TurnoverFormat {
    fn parse(format: Option<&str>) -> Result<Self> {
        match format {
            None | Some("long") => Ok(Self::Long),
            Some("wide" | "semi_long") => Ok(Self::Wide),
            Some("long_typed" | "typed") => Ok(Self::Typed),
            Some("long_metadata" | "metadata" | "with_metadata") => Ok(Self::Metadata),
            Some(other) => Err(RecipeError::InvalidArgument(format!(
                "unsupported turnover format '{other}'"
            ))),
        }
    }
}

fn shape_turnover_output(
    batch: RecordBatch,
    factor: f64,
    format: TurnoverFormat,
    numeric_long: bool,
) -> Result<RecordBatch> {
    let values = batch
        .column_by_name("value")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| {
            RecipeError::Other("turnover calculation requires Float64 values".to_string())
        })?;
    let scaled: ArrayRef = if factor == 1.0 {
        batch
            .column_by_name("value")
            .expect("checked above")
            .clone()
    } else {
        Arc::new(Float64Array::from_iter(
            values.iter().map(|value| value.map(|value| value / factor)),
        ))
    };
    let values = scaled
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 scaling");
    let mut fields = vec![
        batch.schema().field(0).as_ref().clone(),
        batch.schema().field(1).as_ref().clone(),
    ];
    let mut columns = vec![batch.column(0).clone(), batch.column(1).clone()];
    if format != TurnoverFormat::Wide {
        fields.push(batch.schema().field(2).as_ref().clone());
        columns.push(batch.column(2).clone());
    }
    match format {
        TurnoverFormat::Wide | TurnoverFormat::Typed => {
            let name = if format == TurnoverFormat::Wide {
                "TURNOVER"
            } else {
                "value_f64"
            };
            fields.push(Field::new(name, DataType::Float64, true));
            columns.push(scaled.clone());
            if format == TurnoverFormat::Typed {
                for (name, typ) in [
                    ("value_i64", DataType::Int64),
                    ("value_str", DataType::Utf8),
                    ("value_bool", DataType::Boolean),
                    ("value_date", DataType::Date32),
                    (
                        "value_ts",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    ),
                    ("value_time", DataType::Time64(TimeUnit::Microsecond)),
                ] {
                    columns.push(new_null_array(&typ, batch.num_rows()));
                    fields.push(Field::new(name, typ, true));
                }
            }
        }
        TurnoverFormat::Long if numeric_long => {
            fields.push(Field::new("value", DataType::Float64, true));
            columns.push(scaled.clone());
        }
        TurnoverFormat::Long | TurnoverFormat::Metadata => {
            let mut strings = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    Some(value) => strings.append_value(value.to_string()),
                    None => strings.append_null(),
                }
            }
            fields.push(Field::new("value", DataType::Utf8, true));
            columns.push(Arc::new(strings.finish()));
            if format == TurnoverFormat::Metadata {
                fields.push(Field::new("dtype", DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from_iter_values(
                    values
                        .iter()
                        .map(|value| if value.is_some() { "float64" } else { "null" }),
                )));
            }
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(Into::into)
}

/// Fetch ETF constituent holdings via BQL.
///
/// Uses Bloomberg Query Language to retrieve holdings for an ETF including
/// ISIN, weights, and position IDs.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `etf_ticker` - ETF ticker (e.g., "SPY US Equity")
/// * `fields` - Additional fields to retrieve beyond defaults (id_isin, weights, id().position)
///
/// # Returns
///
/// Arrow RecordBatch with native BQL columns, without host-language aliases.
pub async fn recipe_etf_holdings(
    engine: &Engine,
    etf_ticker: String,
    fields: Option<Vec<String>>,
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = build_etf_holdings_request(&etf_ticker, fields, &options);
    engine.request(params).await.map_err(Into::into)
}

fn build_etf_holdings_request(
    etf_ticker: &str,
    fields: Option<Vec<String>>,
    options: &RequestParams,
) -> RequestParams {
    let extra = fields.unwrap_or_default();
    let extra_refs: Vec<&str> = extra.iter().map(String::as_str).collect();
    let bql_query = build_etf_holdings_query(etf_ticker, &extra_refs);
    let mut params = RequestParams {
        service: Service::BqlSvc.to_string(),
        operation: Operation::BqlSendQuery.to_string(),
        elements: Some(vec![("expression".to_string(), bql_query)]),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    params
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_build_earning_header_rename() {
        let header_schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Period 1 Header", DataType::Utf8, true),
            Field::new("Period 2 Header", DataType::Utf8, true),
        ]));

        let header_batch = RecordBatch::try_new(
            header_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL US Equity"])),
                Arc::new(StringArray::from(vec!["PG_Bulk_Header"])),
                Arc::new(StringArray::from(vec!["FY 2023"])),
                Arc::new(StringArray::from(vec!["FY 2024"])),
            ],
        )
        .unwrap();

        let data_schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Period 1 Value", DataType::Float64, true),
            Field::new("Period 2 Value", DataType::Float64, true),
        ]));

        let data_batch = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL US Equity"])),
                Arc::new(StringArray::from(vec!["PG_IS"])),
                Arc::new(Float64Array::from(vec![Some(100.0)])),
                Arc::new(Float64Array::from(vec![Some(120.0)])),
            ],
        )
        .unwrap();

        let rename_map = build_earning_header_rename(&header_batch, &data_batch);

        assert!(rename_map.contains(&("Period 1 Value".to_string(), "fy2023".to_string())));
        assert!(rename_map.contains(&("Period 2 Value".to_string(), "fy2024".to_string())));
    }

    #[test]
    fn test_add_earning_percentage_columns_fy_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("fy2023", DataType::Float64, true),
            Field::new("fy2024", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                ])),
                Arc::new(StringArray::from(vec!["PG_IS", "PG_IS", "PG_IS", "PG_IS"])),
                Arc::new(StringArray::from(vec!["1", "1", "2", "2"])),
                Arc::new(Float64Array::from(vec![
                    Some(100.0),
                    Some(200.0),
                    Some(50.0),
                    Some(50.0),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(300.0),
                    Some(100.0),
                    Some(60.0),
                    Some(40.0),
                ])),
            ],
        )
        .unwrap();

        let output = add_earning_percentage_columns(batch).unwrap();

        let fy23_idx = output.schema().index_of("fy2023").unwrap();
        let fy23_pct_idx = output.schema().index_of("fy2023_pct").unwrap();
        assert_eq!(fy23_pct_idx, fy23_idx + 1);

        let fy23_pct = output
            .column_by_name("fy2023_pct")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((fy23_pct.value(0) - 33.333).abs() < 0.01);
        assert!((fy23_pct.value(1) - 66.667).abs() < 0.01);
        assert!((fy23_pct.value(2) - 50.0).abs() < 0.01);
        assert!((fy23_pct.value(3) - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_add_earning_percentage_columns_case_insensitive_level() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Level", DataType::Int32, true),
            Field::new("fy2023", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                ])),
                Arc::new(StringArray::from(vec!["PG_IS", "PG_IS", "PG_IS"])),
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2)])),
                Arc::new(StringArray::from(vec!["100", "200", "50"])),
            ],
        )
        .unwrap();

        let output = add_earning_percentage_columns(batch).unwrap();
        let pct_col = output
            .column_by_name("fy2023_pct")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((pct_col.value(0) - 33.333).abs() < 0.01);
        assert!((pct_col.value(1) - 66.667).abs() < 0.01);
        assert!((pct_col.value(2) - 100.0).abs() < 0.01);
    }

    fn historical_batch(rows: &[(&str, &str, &str, Option<&str>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(arrow_array::Date32Array::from_iter_values(
                    rows.iter()
                        .map(|row| naive_to_date32(parse_date(row.1).unwrap())),
                )),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
                Arc::new(StringArray::from_iter(rows.iter().map(|row| row.3))),
            ],
        )
        .unwrap()
    }

    fn override_value<'a>(params: &'a RequestParams, name: &str) -> Option<&'a str> {
        params
            .overrides
            .as_ref()?
            .iter()
            .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.as_str()))
    }

    #[test]
    fn test_dividend_request_aliases_and_raw_fields() {
        let options = RequestParams::default();
        for (alias, expected) in DVD_TYPES.entries() {
            let params = build_dividend_request(
                vec!["SYNTHETIC Index".to_string()],
                Some((*alias).to_string()),
                "",
                "",
                &options,
            )
            .unwrap();
            assert_eq!(params.fields.as_ref().unwrap(), &vec![expected.to_string()]);
            assert_eq!(params.securities.unwrap(), vec!["SYNTHETIC Index"]);
            assert!(matches!(params.extractor, ExtractorType::BulkData));
            assert!(params.extractor_set);
        }
        let params = build_dividend_request(
            vec!["ABC US Equity".to_string()],
            Some("CUSTOM_DIVIDEND_FIELD".to_string()),
            "",
            "",
            &options,
        )
        .unwrap();
        assert_eq!(params.fields.unwrap(), vec!["CUSTOM_DIVIDEND_FIELD"]);
        let params = build_dividend_request(Vec::new(), None, "", "", &options).unwrap();
        assert_eq!(params.fields.unwrap(), vec!["DVD_Hist_All"]);
    }

    #[test]
    fn test_dividend_request_dates_adjust_default_and_options() {
        let default = build_dividend_request(
            vec!["ABC US Equity".to_string()],
            Some("adjust".to_string()),
            "2024-01-02",
            "20240203",
            &RequestParams::default(),
        )
        .unwrap();
        assert_eq!(override_value(&default, "DVD_Start_Dt"), Some("20240102"));
        assert_eq!(override_value(&default, "DVD_End_Dt"), Some("20240203"));
        assert_eq!(
            override_value(&default, "Corporate_Actions_Filter"),
            Some("NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE")
        );
        let options = RequestParams {
            securities: Some(vec!["NOT_THE_RECIPE_SECURITY".to_string()]),
            fields: Some(vec!["NOT_THE_RECIPE_FIELD".to_string()]),
            overrides: Some(vec![
                (
                    "corporate_actions_filter".to_string(),
                    "CAPITAL_CHANGE".to_string(),
                ),
                ("DVD_TYPE".to_string(), "Regular Cash".to_string()),
            ]),
            kwargs: Some(HashMap::from([(
                "CUSTOM_OVERRIDE".to_string(),
                "1".to_string(),
            )])),
            validate_fields: Some(false),
            return_eids: true,
            ..Default::default()
        };
        let params = build_dividend_request(
            vec!["ABC US Equity".to_string()],
            Some("adjust".to_string()),
            "",
            "",
            &options,
        )
        .unwrap();
        assert_eq!(
            override_value(&params, "Corporate_Actions_Filter"),
            Some("CAPITAL_CHANGE")
        );
        assert_eq!(override_value(&params, "DVD_TYPE"), Some("Regular Cash"));
        assert_eq!(params.securities.unwrap(), vec!["ABC US Equity"]);
        assert_eq!(params.fields.unwrap(), vec!["Eqy_DVD_Adjust_Fact"]);
        assert_eq!(params.kwargs, options.kwargs);
        assert_eq!(params.validate_fields, Some(false));
        assert!(params.return_eids);
        assert!(build_dividend_request(Vec::new(), None, "invalid", "", &options).is_err());
    }

    #[test]
    fn test_earning_request_union_and_overrides() {
        let options = RequestParams {
            overrides: Some(vec![("CUSTOM_OVERRIDE".to_string(), "2".to_string())]),
            kwargs: Some(HashMap::from([(
                "ANOTHER_OVERRIDE".to_string(),
                "3".to_string(),
            )])),
            format: Some("wide".to_string()),
            return_eids: true,
            validate_fields: Some(false),
            ..Default::default()
        };
        for typ in [
            "Revenue",
            "Operating_Income",
            "Assets",
            "Gross_Profit",
            "Capital_Expenditures",
            "IS",
            "BS",
            "CF",
        ] {
            for by in ["Geo", "Product", "Q", "A"] {
                let (header, data) = build_earning_requests(
                    vec!["ABC US Equity".to_string()],
                    Some(by),
                    typ,
                    Some(" eur "),
                    Some(2),
                    Some(2024),
                    Some(5),
                    &options,
                )
                .unwrap();
                assert_eq!(header.fields.as_ref().unwrap(), &vec!["PG_Bulk_Header"]);
                assert_eq!(data.fields.as_ref().unwrap(), &vec![format!("PG_{typ}")]);
                for request in [&header, &data] {
                    assert!(matches!(request.extractor, ExtractorType::BulkData));
                    assert!(request.extractor_set);
                    assert_eq!(request.format, None);
                    assert_eq!(override_value(request, "Eqy_Fund_Year"), Some("2024"));
                    assert_eq!(override_value(request, "Number_Of_Periods"), Some("5"));
                    assert_eq!(override_value(request, "CUSTOM_OVERRIDE"), Some("2"));
                    assert_eq!(request.kwargs, options.kwargs);
                    assert_eq!(request.validate_fields, Some(false));
                    assert!(request.return_eids);
                }
                let currency_field = if by == "Geo" || by == "Product" {
                    assert_eq!(
                        override_value(&data, "Product_Geo_Override"),
                        Some(if by == "Geo" { "G" } else { "P" })
                    );
                    "Eqy_Fund_Crncy"
                } else {
                    assert_eq!(override_value(&data, "PER"), Some(by));
                    "CURRENCY"
                };
                assert_eq!(override_value(&header, currency_field), None);
                assert_eq!(override_value(&data, currency_field), Some("EUR"));
                assert_eq!(override_value(&header, "PG_Hierarchy_Level"), None);
                assert_eq!(override_value(&data, "PG_Hierarchy_Level"), Some("2"));
            }
        }
    }

    #[test]
    fn test_earning_native_validation_and_optional_defaults() {
        assert_eq!(
            normalize_earning_type(" pg_operating_income ").unwrap(),
            "Operating_Income"
        );
        assert_eq!(normalize_earning_type("pg_is").unwrap(), "IS");
        assert!(normalize_earning_type("unsupported").is_err());
        assert!(build_earning_overrides(Some("anything"), None, None, None, None).is_err());
        for level in [0, 3, -1] {
            assert!(build_earning_overrides(Some("Geo"), None, Some(level), None, None).is_err());
        }
        let (header, data) =
            build_earning_overrides(None, Some(" "), None, Some(0), Some(0)).unwrap();
        assert!(header.is_empty());
        assert!(data.is_empty());
    }

    #[test]
    fn test_earning_output_renames_and_shapes_native_percentages() {
        let header = RecordBatch::try_from_iter(vec![(
            "Period 1 Header",
            Arc::new(StringArray::from(vec!["FY 2024"])) as ArrayRef,
        )])
        .unwrap();
        let data = RecordBatch::try_from_iter(vec![
            ("Level", Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef),
            (
                "Period 1 Value",
                Arc::new(StringArray::from(vec!["25", "75"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let output = shape_earning_output(&header, data.clone()).unwrap();
        assert_eq!(
            output
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["Level", "fy2024", "fy2024_pct"]
        );
        let percentages = output
            .column_by_name("fy2024_pct")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(percentages.values().as_ref(), &[25.0, 75.0]);
        let output = shape_earning_output(&header.slice(0, 0), data).unwrap();
        assert!(output.column_by_name("Period 1 Value_pct").is_some());
    }

    #[test]
    fn test_turnover_dates_and_factor_defaults_validation() {
        assert_eq!(
            turnover_dates("", "2024-06-15").unwrap(),
            ("20240516".to_string(), "20240615".to_string())
        );
        assert_eq!(
            turnover_dates("2024/01/02", "20240203").unwrap(),
            ("20240102".to_string(), "20240203".to_string())
        );
        let (start, end) = turnover_dates("", "").unwrap();
        assert_eq!(
            parse_date(&end).unwrap() - parse_date(&start).unwrap(),
            Duration::days(30)
        );
        assert_eq!(
            parse_date(&end).unwrap(),
            chrono::Local::now().date_naive() - Duration::days(1)
        );
        assert!(turnover_dates("invalid", "20240615").is_err());
        assert!(turnover_dates("20240101", "invalid").is_err());
        assert_eq!(turnover_factor(None).unwrap(), 1.0);
        assert_eq!(turnover_factor(Some(1e6)).unwrap(), 1e6);
        assert_eq!(turnover_factor(Some(-2.0)).unwrap(), -2.0);
        for value in [0.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(turnover_factor(Some(value)).is_err());
        }
    }

    #[test]
    fn test_turnover_requests_keep_options_and_internal_long_shape() {
        let options = RequestParams {
            overrides: Some(vec![("CUSTOM_OVERRIDE".to_string(), "4".to_string())]),
            elements: Some(vec![(
                "periodicitySelection".to_string(),
                "WEEKLY".to_string(),
            )]),
            field_types: Some(HashMap::from([(
                "TURNOVER".to_string(),
                "float64".to_string(),
            )])),
            format: Some("wide".to_string()),
            request_tz: Some("UTC".to_string()),
            output_tz: Some("UTC".to_string()),
            validate_fields: Some(false),
            return_eids: true,
            ..Default::default()
        };
        for fallback in [false, true] {
            let params = build_turnover_request(
                vec!["ABC US Equity".to_string()],
                "20240101",
                "20240131",
                fallback,
                &options,
            );
            assert_eq!(
                params.fields.as_ref().unwrap(),
                &if fallback {
                    vec!["EQY_WEIGHTED_AVG_PX", "VOLUME"]
                } else {
                    vec!["TURNOVER"]
                }
            );
            assert_eq!(params.service, Service::RefData.to_string());
            assert_eq!(params.operation, Operation::HistoricalData.to_string());
            assert_eq!(params.start_date.as_deref(), Some("20240101"));
            assert_eq!(params.end_date.as_deref(), Some("20240131"));
            assert_eq!(override_value(&params, "CUSTOM_OVERRIDE"), Some("4"));
            assert_eq!(params.elements, options.elements);
            assert_eq!(params.field_types, options.field_types);
            assert_eq!(params.request_tz, options.request_tz);
            assert_eq!(params.output_tz, options.output_tz);
            assert_eq!(params.format.as_deref(), Some("long"));
            assert_eq!(params.validate_fields, Some(false));
            assert!(params.return_eids);
        }
    }

    #[test]
    fn test_turnover_missing_tickers_use_presence_not_value_and_deduplicate() {
        let batch = historical_batch(&[
            ("ABC US Equity", "20240101", "TURNOVER", Some("12")),
            ("XYZ US Equity", "20240101", "turnover", None),
        ]);
        let rows = extract_turnover_rows(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].value, None);
        assert_eq!(
            missing_turnover_tickers(
                &[
                    "ABC US Equity",
                    "XYZ US Equity",
                    "NEW US Equity",
                    "NEW US Equity"
                ]
                .map(str::to_string),
                &rows,
            ),
            vec!["NEW US Equity"]
        );
    }

    #[test]
    fn test_turnover_fallback_pairs_by_ticker_date_and_skips_sparse_or_malformed() {
        let batch = historical_batch(&[
            (
                "ABC US Equity",
                "20240103",
                "eqy_weighted_avg_px",
                Some("3"),
            ),
            ("ABC US Equity", "20240103", "volume", Some("4")),
            (
                "ABC US Equity",
                "20240101",
                "EQY_WEIGHTED_AVG_PX",
                Some("10"),
            ),
            ("ABC US Equity", "20240101", "VOLUME", Some("bad")),
            ("ABC US Equity", "20240102", "VOLUME", Some("8")),
            (
                "ABC US Equity",
                "20240104",
                "EQY_WEIGHTED_AVG_PX",
                Some("0"),
            ),
            ("ABC US Equity", "20240104", "VOLUME", Some("100")),
            ("ABC US Equity", "20240105", "EQY_WEIGHTED_AVG_PX", None),
            ("ABC US Equity", "20240105", "VOLUME", Some("100")),
            (
                "XYZ US Equity",
                "20240103",
                "EQY_WEIGHTED_AVG_PX",
                Some("20"),
            ),
            ("XYZ US Equity", "20240103", "VOLUME", Some("10")),
        ]);
        let mut rows = Vec::new();
        append_turnover_fallback(&mut rows, &["ABC US Equity".to_string()], &batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].value, Some(12.0));
        assert_eq!(rows[1].value, Some(0.0));
        assert!(rows.iter().all(|row| row.ticker == "ABC US Equity"));
        assert!(rows[0].date < rows[1].date);
        let output = build_turnover_batch(rows, HashMap::new()).unwrap();
        assert_eq!(
            as_string_col(&output, "field").unwrap().value(0),
            "TURNOVER"
        );
    }

    #[test]
    fn test_turnover_output_formats_scale_values_preserve_nulls_and_metadata() {
        let source = build_turnover_batch(
            vec![
                TurnoverRow {
                    ticker: "ABC US Equity".to_string(),
                    date: Some(19723),
                    value: Some(12.0),
                },
                TurnoverRow {
                    ticker: "ABC US Equity".to_string(),
                    date: None,
                    value: None,
                },
            ],
            HashMap::from([(
                "xbbg.eid_data".to_string(),
                r#"{"ABC US Equity":[1]}"#.to_string(),
            )]),
        )
        .unwrap();
        for (name, expected_columns) in [
            ("long", vec!["ticker", "date", "field", "value"]),
            ("wide", vec!["ticker", "date", "TURNOVER"]),
            (
                "long_typed",
                vec![
                    "ticker",
                    "date",
                    "field",
                    "value_f64",
                    "value_i64",
                    "value_str",
                    "value_bool",
                    "value_date",
                    "value_ts",
                    "value_time",
                ],
            ),
            (
                "long_metadata",
                vec!["ticker", "date", "field", "value", "dtype"],
            ),
        ] {
            let format = TurnoverFormat::parse(Some(name)).unwrap();
            let output = shape_turnover_output(source.clone(), 3.0, format, false).unwrap();
            assert_eq!(
                output
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                expected_columns
            );
            assert_eq!(output.schema().metadata(), source.schema().metadata());
            assert_eq!(output.column(1).null_count(), 1);
            let value_name = match format {
                TurnoverFormat::Wide => "TURNOVER",
                TurnoverFormat::Typed => "value_f64",
                _ => "value",
            };
            let values = output.column_by_name(value_name).unwrap();
            assert_eq!(array_value_as_f64(values, 0), Some(4.0));
            assert!(values.is_null(1));
            if format == TurnoverFormat::Metadata {
                let dtype = as_string_col(&output, "dtype").unwrap();
                assert_eq!(dtype.value(0), "float64");
                assert_eq!(dtype.value(1), "null");
            }
            let empty = shape_turnover_output(source.slice(0, 0), 1.0, format, false).unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.schema(), output.schema());
        }
        let numeric = shape_turnover_output(source, 1.0, TurnoverFormat::Long, true).unwrap();
        assert_eq!(
            numeric.column_by_name("value").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            TurnoverFormat::parse(Some("typed")).unwrap(),
            TurnoverFormat::Typed
        );
        assert_eq!(
            TurnoverFormat::parse(Some("semi_long")).unwrap(),
            TurnoverFormat::Wide
        );
        assert!(TurnoverFormat::parse(Some("unsupported")).is_err());
    }

    fn turnover_presentation_options(pairs: &[(&str, &str)]) -> RequestParams {
        RequestParams {
            kwargs: Some(
                pairs
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn test_turnover_presentation_show_date_aliases_and_validation() {
        for key in ["Dts", "Dates", "show_date"] {
            for (value, expected) in [
                ("Show", true),
                ("S", true),
                ("True", true),
                ("true", true),
                ("1", true),
                ("Hide", false),
                ("H", false),
                ("False", false),
                ("false", false),
                ("0", false),
            ] {
                let mut options = turnover_presentation_options(&[(key, value), ("CUSTOM", "1")]);
                let presentation = take_turnover_presentation(&mut options).unwrap();
                assert_eq!(presentation.show_date, expected);
                assert_eq!(
                    options.kwargs.unwrap(),
                    HashMap::from([("CUSTOM".into(), "1".into())])
                );
            }
        }
        let mut invalid = turnover_presentation_options(&[("show_date", "invalid")]);
        assert!(take_turnover_presentation(&mut invalid).is_err());
    }

    #[test]
    fn test_turnover_presentation_date_sort_and_orientation_aliases() {
        for key in ["DtFmt", "DateFormat", "date_format"] {
            for (value, expected) in [
                ("B", TurnoverDateFormat::Both),
                ("both", TurnoverDateFormat::Both),
                ("P", TurnoverDateFormat::Periodic),
                ("Periodic", TurnoverDateFormat::Periodic),
                ("D", TurnoverDateFormat::Date),
                ("Date", TurnoverDateFormat::Date),
                ("unknown", TurnoverDateFormat::Date),
            ] {
                let mut options = turnover_presentation_options(&[(key, value)]);
                assert_eq!(
                    take_turnover_presentation(&mut options)
                        .unwrap()
                        .date_format,
                    expected
                );
                assert!(options.kwargs.unwrap().is_empty());
            }
        }
        for key in ["Sort", "sort"] {
            for (value, descending) in [
                ("C", false),
                ("A", false),
                ("Ascend", false),
                ("Chronological", false),
                ("False", false),
                ("0", false),
                ("ASCENDING", false),
                ("unknown", false),
                ("R", true),
                ("D", true),
                ("Descend", true),
                ("Reverse", true),
                ("True", true),
                ("1", true),
                ("descending", true),
            ] {
                let mut options = turnover_presentation_options(&[(key, value)]);
                assert_eq!(
                    take_turnover_presentation(&mut options).unwrap().descending,
                    Some(descending)
                );
                assert!(options.kwargs.unwrap().is_empty());
            }
        }
        for key in ["Orientation", "Direction", "Dir", "orientation"] {
            for (value, expected) in [
                ("H", Some("wide")),
                ("horizontal", Some("wide")),
                ("V", Some("long")),
                ("Vertical", Some("long")),
                ("unknown", None),
            ] {
                let mut options = turnover_presentation_options(&[(key, value)]);
                take_turnover_presentation(&mut options).unwrap();
                assert_eq!(options.format.as_deref(), expected);
                assert!(options.kwargs.as_ref().unwrap().is_empty());
                options.format = Some("long_typed".to_string());
                options
                    .kwargs
                    .as_mut()
                    .unwrap()
                    .insert(key.to_string(), value.to_string());
                take_turnover_presentation(&mut options).unwrap();
                assert_eq!(options.format.as_deref(), Some("long_typed"));
                assert!(options.kwargs.unwrap().is_empty());
            }
        }
    }

    #[test]
    fn test_turnover_presentation_consumes_aliases_before_every_request() {
        let mut options = turnover_presentation_options(&[
            ("Dts", "H"),
            ("Dates", "Hide"),
            ("show_date", "True"),
            ("DtFmt", "D"),
            ("DateFormat", "P"),
            ("date_format", "Both"),
            ("Sort", "D"),
            ("sort", "A"),
            ("Orientation", "V"),
            ("Direction", "Vertical"),
            ("Dir", "V"),
            ("orientation", "H"),
            ("periodicitySelection", "MONTHLY"),
            ("CUSTOM_OVERRIDE", "2"),
        ]);
        let presentation = take_turnover_presentation(&mut options).unwrap();
        assert!(presentation.show_date);
        assert_eq!(presentation.date_format, TurnoverDateFormat::Both);
        assert_eq!(presentation.descending, Some(false));
        assert_eq!(presentation.periodicity, TurnoverPeriodicity::Monthly);
        assert_eq!(options.format.as_deref(), Some("wide"));
        assert_eq!(options.kwargs.as_ref().unwrap().len(), 2);
        for fallback in [false, true] {
            let request = build_turnover_request(
                vec!["ABC US Equity".to_string()],
                "20240101",
                "20241231",
                fallback,
                &options,
            );
            assert_eq!(request.kwargs, options.kwargs);
            assert_eq!(request.format.as_deref(), Some("long"));
        }
        // The same consumed options are passed to the currency recipe.
        assert!(
            options
                .kwargs
                .unwrap()
                .keys()
                .all(|key| { matches!(key.as_str(), "periodicitySelection" | "CUSTOM_OVERRIDE") })
        );
    }

    #[test]
    fn test_turnover_periodicity_follows_request_element_precedence() {
        let mut options = turnover_presentation_options(&[("periodicitySelection", "MONTHLY")]);
        options.elements = Some(vec![("periodicitySelection".into(), "QUARTERLY".into())]);
        options.options = Some(vec![("periodicitySelection".into(), "YEARLY".into())]);
        assert_eq!(
            take_turnover_presentation(&mut options)
                .unwrap()
                .periodicity,
            TurnoverPeriodicity::Yearly
        );
        options.options = None;
        assert_eq!(
            take_turnover_presentation(&mut options)
                .unwrap()
                .periodicity,
            TurnoverPeriodicity::Monthly
        );
        options.kwargs = None;
        assert_eq!(
            take_turnover_presentation(&mut options)
                .unwrap()
                .periodicity,
            TurnoverPeriodicity::Quarterly
        );
        options.elements = None;
        assert_eq!(
            take_turnover_presentation(&mut options)
                .unwrap()
                .periodicity,
            TurnoverPeriodicity::Daily
        );
    }

    #[test]
    fn test_turnover_presentation_sorts_after_scaling_and_adds_period_labels() {
        let source = build_turnover_batch(
            vec![
                TurnoverRow {
                    ticker: "XYZ US Equity".into(),
                    date: Some(naive_to_date32(parse_date("20240705").unwrap())),
                    value: Some(60.0),
                },
                TurnoverRow {
                    ticker: "ABC US Equity".into(),
                    date: Some(naive_to_date32(parse_date("20240102").unwrap())),
                    value: Some(12.0),
                },
                TurnoverRow {
                    ticker: "ABC US Equity".into(),
                    date: Some(naive_to_date32(parse_date("20240705").unwrap())),
                    value: Some(24.0),
                },
                TurnoverRow {
                    ticker: "ABC US Equity".into(),
                    date: None,
                    value: None,
                },
            ],
            HashMap::from([("xbbg.eid_data".into(), r#"{"ABC US Equity":[1]}"#.into())]),
        )
        .unwrap();
        let metadata = source.schema().metadata().clone();
        let mut options = turnover_presentation_options(&[
            ("Sort", "Reverse"),
            ("DtFmt", "Both"),
            ("periodicitySelection", "MONTHLY"),
        ]);
        let presentation = take_turnover_presentation(&mut options).unwrap();
        let shaped = shape_turnover_output(source, 2.0, TurnoverFormat::Long, false).unwrap();
        let output = apply_turnover_presentation(shaped, &presentation).unwrap();
        assert_eq!(
            output
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["ticker", "date", "period", "field", "value"]
        );
        assert_eq!(
            as_string_col(&output, "ticker")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![
                Some("ABC US Equity"),
                Some("ABC US Equity"),
                Some("ABC US Equity"),
                Some("XYZ US Equity")
            ]
        );
        assert_eq!(
            as_string_col(&output, "period")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(""), Some("2024-07"), Some("2024-01"), Some("2024-07")]
        );
        assert_eq!(
            as_string_col(&output, "value")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some("12"), Some("6"), Some("30")]
        );
        assert_eq!(output.schema().metadata(), &metadata);
        assert!(output.column_by_name("date").unwrap().is_null(0));
    }

    #[test]
    fn test_turnover_presentation_periodic_labels_match_calendar_conventions() {
        let source = historical_batch(&[
            ("ABC US Equity", "20210101", "TURNOVER", Some("1")),
            ("ABC US Equity", "20240701", "TURNOVER", Some("2")),
        ]);
        for (periodicity, expected) in [
            ("Y", ["2021", "2024"]),
            ("YEARLY", ["2021", "2024"]),
            ("S", ["2021H1", "2024H2"]),
            ("SEMI_ANNUALLY", ["2021H1", "2024H2"]),
            ("SEMIANNUALLY", ["2021H1", "2024H2"]),
            ("Q", ["2021Q1", "2024Q3"]),
            ("QUARTERLY", ["2021Q1", "2024Q3"]),
            ("M", ["2021-01", "2024-07"]),
            ("MONTHLY", ["2021-01", "2024-07"]),
            ("W", ["2020-W53", "2024-W27"]),
            ("WEEKLY", ["2020-W53", "2024-W27"]),
            ("DAILY", ["2021-01-01", "2024-07-01"]),
            ("unknown", ["2021-01-01", "2024-07-01"]),
        ] {
            let mut options = turnover_presentation_options(&[
                ("date_format", "PERIODIC"),
                ("periodicitySelection", periodicity),
            ]);
            let presentation = take_turnover_presentation(&mut options).unwrap();
            let output = apply_turnover_presentation(source.clone(), &presentation).unwrap();
            assert_eq!(
                output
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["ticker", "period", "field", "value"]
            );
            let periods = as_string_col(&output, "period").unwrap();
            assert_eq!(periods.value(0), expected[0]);
            assert_eq!(periods.value(1), expected[1]);
            let empty = apply_turnover_presentation(source.slice(0, 0), &presentation).unwrap();
            assert_eq!(empty.schema(), output.schema());
            assert_eq!(empty.num_rows(), 0);
        }
        let dates: ArrayRef = Arc::new(StringArray::from(vec![None, Some("not-a-date")]));
        let mut label = "previous label".to_string();
        format_turnover_period_label(&dates, 0, TurnoverPeriodicity::Daily, &mut label);
        assert_eq!(label, "");
        format_turnover_period_label(&dates, 1, TurnoverPeriodicity::Daily, &mut label);
        assert_eq!(label, "not-a-date");
    }

    #[test]
    fn test_turnover_presentation_hides_dates_last_for_every_output_format() {
        let source = build_turnover_batch(
            extract_turnover_rows(&historical_batch(&[
                ("ABC US Equity", "20240102", "TURNOVER", Some("12")),
                ("ABC US Equity", "20240705", "TURNOVER", Some("24")),
            ]))
            .unwrap(),
            HashMap::new(),
        )
        .unwrap();
        for format in ["long", "wide", "long_typed", "long_metadata"] {
            let mut options = turnover_presentation_options(&[
                ("show_date", "False"),
                ("date_format", "Both"),
                ("sort", "D"),
                ("orientation", "H"),
            ]);
            options.format = Some(format.to_string());
            let presentation = take_turnover_presentation(&mut options).unwrap();
            let shaped = shape_turnover_output(
                source.clone(),
                2.0,
                TurnoverFormat::parse(options.format.as_deref()).unwrap(),
                false,
            )
            .unwrap();
            let output = apply_turnover_presentation(shaped, &presentation).unwrap();
            assert_eq!(output.num_rows(), 2);
            assert!(output.column_by_name("date").is_none());
            assert!(output.column_by_name("period").is_none());
            let values = output
                .column_by_name(match format {
                    "wide" => "TURNOVER",
                    "long_typed" => "value_f64",
                    _ => "value",
                })
                .unwrap();
            assert_eq!(array_value_as_f64(values, 0), Some(12.0));
            assert_eq!(array_value_as_f64(values, 1), Some(6.0));
        }
    }

    #[test]
    fn test_turnover_presentation_defaults_preserve_date_arrays() {
        let source = historical_batch(&[("ABC US Equity", "20240102", "TURNOVER", Some("12"))]);
        for pairs in [Vec::new(), vec![("Dates", "Show"), ("DateFormat", "D")]] {
            let mut options = turnover_presentation_options(&pairs);
            let presentation = take_turnover_presentation(&mut options).unwrap();
            let output = apply_turnover_presentation(source.clone(), &presentation).unwrap();
            assert_eq!(output.schema(), source.schema());
            for (original, result) in source.columns().iter().zip(output.columns()) {
                assert!(Arc::ptr_eq(original, result));
            }
        }
    }

    #[test]
    fn test_turnover_metadata_unions_fallback_entitlements() {
        let mut metadata = HashMap::from([(
            "xbbg.eid_data".to_string(),
            r#"{"ABC US Equity":[1]}"#.to_string(),
        )]);
        merge_turnover_metadata(
            &mut metadata,
            &HashMap::from([(
                "xbbg.eid_data".to_string(),
                r#"{"XYZ US Equity":[2]}"#.to_string(),
            )]),
        );
        let value: serde_json::Value = serde_json::from_str(&metadata["xbbg.eid_data"]).unwrap();
        assert_eq!(value["ABC US Equity"][0], 1);
        assert_eq!(value["XYZ US Equity"][0], 2);
    }

    #[test]
    fn test_etf_holdings_request_default_custom_and_deduplicated_fields() {
        let options = RequestParams {
            overrides: Some(vec![("mode".to_string(), "cached".to_string())]),
            kwargs: Some(HashMap::from([(
                "CUSTOM_OPTION".to_string(),
                "5".to_string(),
            )])),
            ..Default::default()
        };
        for (fields, expected) in [
            (None, "id_isin, weights, id().position"),
            (
                Some(vec!["name".to_string(), "px_last".to_string()]),
                "id_isin, weights, id().position, name, px_last",
            ),
            (
                Some(vec![
                    "id_isin".to_string(),
                    "name".to_string(),
                    "name".to_string(),
                ]),
                "id_isin, weights, id().position, name",
            ),
        ] {
            let params = build_etf_holdings_request("SYNTH", fields, &options);
            assert_eq!(params.service, Service::BqlSvc.to_string());
            assert_eq!(params.operation, Operation::BqlSendQuery.to_string());
            assert!(params.elements.as_ref().unwrap().contains(&(
                "expression".to_string(),
                format!("get({expected}) for(holdings('SYNTH US Equity'))"),
            )));
            assert!(
                params
                    .elements
                    .unwrap()
                    .contains(&("mode".to_string(), "cached".to_string()))
            );
            assert_eq!(params.kwargs, options.kwargs);
        }
        let params = build_etf_holdings_request("SYNTH LN Equity", None, &options);
        assert!(params.elements.unwrap()[0].1.contains("SYNTH LN Equity"));
    }

    #[test]
    fn test_dividend_yield_aggregates_duplicates_and_rolls_window() {
        let ticker = "AAPL US Equity".to_string();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 4, 10).unwrap();
        let price_date = NaiveDate::from_ymd_opt(2024, 4, 11).unwrap();
        let events = aggregate_dividend_events(vec![
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d1,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.24),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d1,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.01),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d2,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.25),
            },
        ]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].amount, Some(0.25));

        let prices = HashMap::from([((ticker.clone(), price_date), 100.0)]);
        let rows = build_dividend_yield_rows(
            &[ticker],
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            price_date,
            365,
            &events,
            &prices,
        );
        let priced = rows.iter().find(|row| row.date == price_date).unwrap();
        assert_eq!(priced.trailing_dividend_amount, Some(0.50));
        assert_eq!(priced.dividend_yield, Some(0.005));
    }

    #[test]
    fn test_dividend_yield_missing_price_keeps_yield_null() {
        let ticker = "AAPL US Equity".to_string();
        let ex_date = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let events = vec![DividendEvent {
            ticker: ticker.clone(),
            ex_date,
            declared_date: None,
            record_date: None,
            payable_date: None,
            dividend_type: Some("Regular Cash".to_string()),
            amount: Some(0.25),
        }];
        let rows = build_dividend_yield_rows(
            &[ticker],
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 31).unwrap(),
            365,
            &events,
            &HashMap::new(),
        );
        assert_eq!(rows[0].price, None);
        assert_eq!(rows[0].dividend_yield, None);
    }

    #[test]
    fn test_dividend_yield_rolling_window_matches_date_predicate_boundaries() {
        let ticker = "AAPL US Equity".to_string();
        let boundary = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let inside = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let evaluation = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap();
        let after = NaiveDate::from_ymd_opt(2024, 1, 12).unwrap();
        let representative_declared = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let events = vec![
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: evaluation,
                declared_date: Some(representative_declared),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Special Cash".to_string()),
                amount: None,
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: boundary,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Old".to_string()),
                amount: Some(10.0),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: evaluation,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(2.0),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: inside,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(1.0),
            },
        ];
        let prices = HashMap::from([
            ((ticker.clone(), evaluation), 0.0),
            ((ticker.clone(), after), 100.0),
        ]);

        let rows = build_dividend_yield_rows(
            std::slice::from_ref(&ticker),
            boundary,
            after,
            10,
            &events,
            &prices,
        );
        assert_eq!(rows.len(), 4);

        // Differential oracle for the previous definition: every date uses
        // events in original order and the calendar predicate
        // (date - window_days, date].
        for row in &rows {
            let same_day = events
                .iter()
                .filter(|event| event.ex_date == row.date)
                .collect::<Vec<_>>();
            let expected_amount = sum_optional(same_day.iter().filter_map(|event| event.amount));
            let trailing_start = row.date - Duration::days(10);
            let expected_trailing = sum_optional(events.iter().filter_map(|event| {
                (event.ex_date > trailing_start && event.ex_date <= row.date)
                    .then_some(event.amount)
                    .flatten()
            }));
            let expected_price = prices.get(&(ticker.clone(), row.date)).copied();
            let expected_yield = match (expected_trailing, expected_price) {
                (Some(amount), Some(price)) if price != 0.0 => Some(amount / price),
                _ => None,
            };
            let representative = same_day.first().copied();

            assert_eq!(row.dividend_amount, expected_amount);
            assert_eq!(row.trailing_dividend_amount, expected_trailing);
            assert_eq!(row.price, expected_price);
            assert_eq!(row.dividend_yield, expected_yield);
            assert_eq!(
                row.dividend_type,
                representative.and_then(|event| event.dividend_type.clone())
            );
            assert_eq!(
                row.declared_date,
                representative.and_then(|event| event.declared_date)
            );
        }

        let evaluation_row = rows.iter().find(|row| row.date == evaluation).unwrap();
        assert_eq!(evaluation_row.trailing_dividend_amount, Some(3.0));
        assert_eq!(evaluation_row.dividend_amount, Some(2.0));
        assert_eq!(evaluation_row.dividend_yield, None);
        assert_eq!(
            evaluation_row.dividend_type.as_deref(),
            Some("Special Cash")
        );
        assert_eq!(evaluation_row.declared_date, Some(representative_declared));

        let after_row = rows.iter().find(|row| row.date == after).unwrap();
        assert_eq!(after_row.trailing_dividend_amount, Some(2.0));
        assert_eq!(after_row.dividend_yield, Some(0.02));
    }

    fn trailing_after_boundary_expiry(expiring_amount: f64) -> Option<f64> {
        let ticker = "AAPL US Equity".to_string();
        let boundary = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let retained = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let evaluation = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap();
        let event = |ex_date, amount| DividendEvent {
            ticker: ticker.clone(),
            ex_date,
            declared_date: None,
            record_date: None,
            payable_date: None,
            dividend_type: None,
            amount: Some(amount),
        };
        let events = vec![event(boundary, expiring_amount), event(retained, 1.0)];
        let prices = HashMap::from([((ticker.clone(), evaluation), 100.0)]);
        let rows = build_dividend_yield_rows(&[ticker], boundary, evaluation, 10, &events, &prices);
        rows.iter()
            .find(|row| row.date == evaluation)
            .and_then(|row| row.trailing_dividend_amount)
    }

    #[test]
    fn test_dividend_yield_refolds_after_finite_cancellation_expires() {
        assert_eq!(trailing_after_boundary_expiry(1.0e16), Some(1.0));
    }

    #[test]
    fn test_dividend_yield_refolds_after_nonfinite_amount_expires() {
        for expiring_amount in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(trailing_after_boundary_expiry(expiring_amount), Some(1.0));
        }
    }
}
