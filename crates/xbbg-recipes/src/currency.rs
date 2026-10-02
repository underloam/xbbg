//! Currency conversion recipe.
//!
//! Adjusts data columns by fetching FX rates from Bloomberg and applying
//! conversion factors via Arrow compute operations.
//!
//! # Recipes
//!
//! - [`recipe_adjust_ccy`]: Convert data values to a target currency

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_arith::numeric::{div, mul};
use arrow_array::{
    Array, ArrayRef, Date32Array, Datum, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::currency::{build_fx_pair, same_currency, FxConversionInfo};
use xbbg_ext::{fmt_date, parse_date};

use crate::error::{RecipeError, Result};
use crate::utils::{array_value_as_string, as_string_col, date32_to_naive};

const DATE_COL: &str = "date";
const FX_FIELD: &str = "PX_LAST";
const CURRENCY_FIELD: &str = "CRNCY";

type FxRatesByPair = HashMap<String, HashMap<i32, f64>>;

/// Adjust a wide historical RecordBatch into a target currency.
///
/// Workflow:
/// 1. Extract ticker names from value column names.
/// 2. Query local currency (`CRNCY`) for each ticker.
/// 3. Build FX pair requirements with `xbbg-ext` helpers.
/// 4. Query FX history for the batch date range.
/// 5. Convert value columns with Arrow compute (`div` + optional `mul` factor).
///
/// # Errors
/// Propagates Bloomberg/engine failures from the currency and FX-rate
/// queries: a conversion that cannot be performed errors out instead of
/// silently returning unconverted (mixed-currency) data. Tickers whose
/// currency already matches the target pass through unchanged; missing FX
/// rows convert to null and log a warning.
pub async fn recipe_adjust_ccy(
    engine: &Engine,
    data: RecordBatch,
    target_ccy: String,
    start_date: String,
    end_date: String,
) -> Result<RecordBatch> {
    if data.num_rows() == 0 || data.num_columns() == 0 {
        return Ok(data);
    }

    if target_ccy.eq_ignore_ascii_case("local") {
        return Ok(data);
    }

    let (column_tickers, tickers) = extract_ticker_columns(&data);
    if tickers.is_empty() {
        return Ok(data);
    }

    // Propagate fetch failures: silently returning the original batch here
    // would hand the caller local-currency values labeled as converted.
    let ticker_currencies = fetch_ticker_currencies(engine, &tickers).await?;

    if ticker_currencies.is_empty() {
        return Ok(data);
    }

    let (fx_by_ticker, fx_pairs) = build_fx_requirements(&tickers, &ticker_currencies, &target_ccy);
    if fx_pairs.is_empty() {
        return Ok(data);
    }

    let Some(date_keys) = extract_date_keys(&data)? else {
        return Ok(data);
    };

    if !date_keys.iter().any(Option::is_some) {
        return Ok(data);
    }

    let (fx_start, fx_end) = resolve_fx_query_dates(&date_keys, &start_date, &end_date);
    let fx_rates = fetch_fx_rates(engine, &fx_pairs, &fx_start, &fx_end).await?;

    if fx_rates.is_empty() {
        return Ok(data);
    }

    apply_fx_conversion(data, &column_tickers, &date_keys, &fx_by_ticker, &fx_rates)
}

pub async fn recipe_currency_conversion(
    engine: &Engine,
    ticker: String,
    target_ccy: String,
    start_date: String,
    end_date: String,
) -> Result<RecordBatch> {
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(vec![ticker]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some(start_date),
        end_date: Some(end_date),
        overrides: Some(vec![("CRNCY".to_string(), target_ccy)]),
        ..Default::default()
    };
    engine.request(params).await.map_err(Into::into)
}

fn extract_ticker_columns(data: &RecordBatch) -> (HashMap<usize, String>, Vec<String>) {
    let mut col_to_ticker = HashMap::new();
    let mut tickers = Vec::new();
    let mut seen = HashSet::new();

    for (idx, field) in data.schema().fields().iter().enumerate() {
        let name = field.name();
        if !is_value_column(name) {
            continue;
        }

        let Some(ticker) = ticker_from_column_name(name) else {
            continue;
        };

        col_to_ticker.insert(idx, ticker.clone());
        if seen.insert(ticker.clone()) {
            tickers.push(ticker);
        }
    }

    (col_to_ticker, tickers)
}

fn is_value_column(name: &str) -> bool {
    !(name.eq_ignore_ascii_case("date")
        || name.eq_ignore_ascii_case("ticker")
        || name.eq_ignore_ascii_case("field")
        || name.eq_ignore_ascii_case("value")
        || name.eq_ignore_ascii_case("value_str")
        || name.eq_ignore_ascii_case("value_f64")
        || name.eq_ignore_ascii_case("value_i64")
        || name.eq_ignore_ascii_case("value_bool")
        || name.eq_ignore_ascii_case("value_date")
        || name.eq_ignore_ascii_case("value_ts")
        || name.eq_ignore_ascii_case("value_time")
        || name.eq_ignore_ascii_case("dtype"))
}

fn ticker_from_column_name(name: &str) -> Option<String> {
    if let Some((ticker, _)) = name.split_once('|') {
        let trimmed = ticker.trim();
        return (!trimmed.is_empty()).then(|| trimmed.to_string());
    }

    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.to_string())
}

fn build_fx_requirements(
    tickers: &[String],
    ticker_currencies: &HashMap<String, String>,
    target_ccy: &str,
) -> (HashMap<String, FxConversionInfo>, Vec<String>) {
    let mut fx_by_ticker = HashMap::new();
    let mut fx_pairs = HashSet::new();

    for ticker in tickers {
        let Some(local_ccy) = ticker_currencies.get(ticker) else {
            continue;
        };

        if local_ccy.trim().is_empty() || same_currency(local_ccy, target_ccy) {
            continue;
        }

        let fx_info = build_fx_pair(local_ccy, target_ccy);
        fx_pairs.insert(fx_info.fx_pair.clone());
        fx_by_ticker.insert(ticker.clone(), fx_info);
    }

    let mut unique_pairs = fx_pairs.into_iter().collect::<Vec<_>>();
    unique_pairs.sort();

    (fx_by_ticker, unique_pairs)
}

async fn fetch_ticker_currencies(
    engine: &Engine,
    tickers: &[String],
) -> Result<HashMap<String, String>> {
    if tickers.is_empty() {
        return Ok(HashMap::new());
    }

    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(tickers.to_vec()),
        fields: Some(vec![CURRENCY_FIELD.to_string()]),
        ..Default::default()
    };

    let batch = engine.request(params).await?;
    parse_currency_batch(&batch)
}

fn parse_currency_batch(batch: &RecordBatch) -> Result<HashMap<String, String>> {
    if batch.num_rows() == 0 {
        return Ok(HashMap::new());
    }

    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = find_value_column(batch)?;

    let mut out = HashMap::new();
    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) {
            continue;
        }

        if !field_col.value(row).eq_ignore_ascii_case(CURRENCY_FIELD) {
            continue;
        }

        let Some(value) = array_value_as_string(value_col, row) else {
            continue;
        };

        let currency = value.trim();
        if currency.is_empty() {
            continue;
        }

        out.insert(ticker_col.value(row).to_string(), currency.to_string());
    }

    Ok(out)
}

async fn fetch_fx_rates(
    engine: &Engine,
    fx_pairs: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<FxRatesByPair> {
    if fx_pairs.is_empty() {
        return Ok(HashMap::new());
    }

    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(fx_pairs.to_vec()),
        fields: Some(vec![FX_FIELD.to_string()]),
        start_date: Some(start_date.to_string()),
        end_date: Some(end_date.to_string()),
        ..Default::default()
    };

    let batch = engine.request(params).await?;
    parse_fx_rate_batch(&batch)
}

fn parse_fx_rate_batch(batch: &RecordBatch) -> Result<FxRatesByPair> {
    if batch.num_rows() == 0 {
        return Ok(HashMap::new());
    }

    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = find_value_column(batch)?;
    let date_keys = extract_date_keys(batch)?.ok_or_else(|| {
        RecipeError::Other("FX rate response missing required 'date' column".to_string())
    })?;

    if date_keys.len() != batch.num_rows() {
        return Err(RecipeError::Other(
            "FX rate response date column length mismatch".to_string(),
        ));
    }

    let mut out: FxRatesByPair = HashMap::new();
    for (row, date_key) in date_keys.iter().copied().enumerate() {
        if ticker_col.is_null(row) || field_col.is_null(row) {
            continue;
        }

        if !field_col.value(row).eq_ignore_ascii_case(FX_FIELD) {
            continue;
        }

        let Some(date_key) = date_key else {
            continue;
        };

        let Some(rate) = array_value_as_f64(value_col, row) else {
            continue;
        };

        if rate.abs() <= f64::EPSILON {
            continue;
        }

        out.entry(ticker_col.value(row).to_string())
            .or_default()
            .insert(date_key, rate);
    }

    Ok(out)
}

fn apply_fx_conversion(
    data: RecordBatch,
    column_tickers: &HashMap<usize, String>,
    date_keys: &[Option<i32>],
    fx_by_ticker: &HashMap<String, FxConversionInfo>,
    fx_rates: &FxRatesByPair,
) -> Result<RecordBatch> {
    if date_keys.len() != data.num_rows() {
        return Err(RecipeError::Other(
            "input date column length does not match row count".to_string(),
        ));
    }

    let schema = data.schema();
    let mut new_columns = Vec::with_capacity(data.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let input_col = data.column(idx).clone();

        let Some(ticker) = column_tickers.get(&idx) else {
            new_columns.push(input_col);
            continue;
        };

        let Some(fx_info) = fx_by_ticker.get(ticker) else {
            new_columns.push(input_col);
            continue;
        };

        let Some(values) = input_col.as_any().downcast_ref::<Float64Array>() else {
            new_columns.push(input_col);
            continue;
        };

        let Some(rates_by_date) = fx_rates.get(&fx_info.fx_pair) else {
            xbbg_log::warn!(
                ticker = ticker.as_str(),
                fx_pair = fx_info.fx_pair.as_str(),
                "no FX rates returned for pair; column left in local currency"
            );
            new_columns.push(input_col);
            continue;
        };

        let fx_rate_array = build_fx_rate_array(values.len(), date_keys, rates_by_date);
        let null_count = fx_rate_array.null_count();
        let len = fx_rate_array.len();
        if null_count == len {
            xbbg_log::warn!(
                ticker = ticker.as_str(),
                fx_pair = fx_info.fx_pair.as_str(),
                "FX rates missing for every row date; column left in local currency"
            );
            new_columns.push(input_col);
            continue;
        }
        if null_count > 0 {
            xbbg_log::warn!(
                ticker = ticker.as_str(),
                fx_pair = fx_info.fx_pair.as_str(),
                missing = null_count,
                total = len,
                "FX rates missing for some dates; converted values are null for those rows"
            );
        }

        let denominator: ArrayRef = if (fx_info.factor - 1.0).abs() > f64::EPSILON {
            let factor_array = Float64Array::from(vec![Some(fx_info.factor); values.len()]);
            mul(&fx_rate_array, &factor_array).map_err(|err| {
                RecipeError::Other(format!(
                    "failed to scale FX rates for '{ticker}' ({}): {err}",
                    fx_info.fx_pair
                ))
            })?
        } else {
            Arc::new(fx_rate_array)
        };

        let denominator_datum: &dyn Datum = &denominator.as_ref();
        let converted = div(values, denominator_datum).map_err(|err| {
            RecipeError::Other(format!(
                "failed to convert column '{}' using FX pair '{}': {err}",
                field.name(),
                fx_info.fx_pair
            ))
        })?;

        new_columns.push(converted);
    }

    RecordBatch::try_new(schema, new_columns).map_err(Into::into)
}

fn build_fx_rate_array(
    num_rows: usize,
    date_keys: &[Option<i32>],
    rates_by_date: &HashMap<i32, f64>,
) -> Float64Array {
    let mut values = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let rate = date_keys
            .get(row)
            .copied()
            .flatten()
            .and_then(|date_key| rates_by_date.get(&date_key).copied());
        values.push(rate);
    }
    Float64Array::from(values)
}

fn resolve_fx_query_dates(
    date_keys: &[Option<i32>],
    fallback_start: &str,
    fallback_end: &str,
) -> (String, String) {
    let min_key = date_keys.iter().flatten().copied().min();
    let max_key = date_keys.iter().flatten().copied().max();

    if let (Some(min_date), Some(max_date)) = (min_key, max_key) {
        if let (Some(start), Some(end)) = (date32_to_naive(min_date), date32_to_naive(max_date)) {
            return (fmt_date(start, None), fmt_date(end, None));
        }
    }

    (fallback_start.to_string(), fallback_end.to_string())
}

fn extract_date_keys(batch: &RecordBatch) -> Result<Option<Vec<Option<i32>>>> {
    let Some(date_col) = batch.column_by_name(DATE_COL) else {
        return Ok(None);
    };

    if let Some(col) = date_col.as_any().downcast_ref::<Date32Array>() {
        let mut values = Vec::with_capacity(col.len());
        for row in 0..col.len() {
            values.push((!col.is_null(row)).then(|| col.value(row)));
        }
        return Ok(Some(values));
    }

    if let Some(col) = date_col.as_any().downcast_ref::<StringArray>() {
        let mut values = Vec::with_capacity(col.len());
        for row in 0..col.len() {
            if col.is_null(row) {
                values.push(None);
            } else {
                values.push(parse_date_key(col.value(row)));
            }
        }
        return Ok(Some(values));
    }

    Err(RecipeError::Other(format!(
        "'date' column must be Date32 or Utf8, got {:?}",
        date_col.data_type()
    )))
}

fn parse_date_key(raw: &str) -> Option<i32> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let parsed = parse_date(trimmed).ok().or_else(|| {
        trimmed
            .get(..10)
            .filter(|prefix| prefix.len() == 10)
            .and_then(|prefix| parse_date(prefix).ok())
    })?;

    naive_to_date32(parsed)
}

fn naive_to_date32(date: chrono::NaiveDate) -> Option<i32> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let days = (date - epoch).num_days();
    i32::try_from(days).ok()
}

fn find_value_column(batch: &RecordBatch) -> Result<&ArrayRef> {
    batch
        .column_by_name("value")
        .or_else(|| batch.column_by_name("value_f64"))
        .or_else(|| batch.column_by_name("value_i64"))
        .or_else(|| batch.column_by_name("value_str"))
        .ok_or_else(|| {
            RecipeError::Other(
                "response batch missing value column (value/value_f64/value_i64/value_str)"
                    .to_string(),
            )
        })
}

fn array_value_as_f64(array: &ArrayRef, row: usize) -> Option<f64> {
    if let Some(col) = array.as_any().downcast_ref::<Float64Array>() {
        return (!col.is_null(row)).then(|| col.value(row));
    }

    if let Some(col) = array.as_any().downcast_ref::<Int64Array>() {
        return (!col.is_null(row)).then(|| col.value(row) as f64);
    }

    if let Some(col) = array.as_any().downcast_ref::<Int32Array>() {
        return (!col.is_null(row)).then(|| col.value(row) as f64);
    }

    if let Some(col) = array.as_any().downcast_ref::<StringArray>() {
        if col.is_null(row) {
            return None;
        }
        let raw = col.value(row).trim();
        if raw.is_empty() {
            return None;
        }
        return raw.parse::<f64>().ok();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_extract_ticker_columns_from_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, true),
            Field::new("AAPL US Equity|PX_LAST", DataType::Float64, true),
            Field::new("VOD LN Equity|PX_LAST", DataType::Float64, true),
            Field::new("MSFT US Equity", DataType::Float64, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(19_723)])),
                Arc::new(Float64Array::from(vec![Some(150.0)])),
                Arc::new(Float64Array::from(vec![Some(72.5)])),
                Arc::new(Float64Array::from(vec![Some(300.0)])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let (col_tickers, tickers) = extract_ticker_columns(&batch);
        assert_eq!(tickers.len(), 3);
        assert_eq!(tickers[0], "AAPL US Equity");
        assert_eq!(tickers[1], "VOD LN Equity");
        assert_eq!(tickers[2], "MSFT US Equity");
        assert_eq!(col_tickers.get(&1), Some(&"AAPL US Equity".to_string()));
        assert_eq!(col_tickers.get(&2), Some(&"VOD LN Equity".to_string()));
        assert_eq!(col_tickers.get(&3), Some(&"MSFT US Equity".to_string()));
    }

    #[test]
    fn test_build_fx_requirements_deduplicates_pairs_and_preserves_factors() {
        let tickers = vec![
            "AAPL US Equity".to_string(),
            "VOD LN Equity".to_string(),
            "BARC LN Equity".to_string(),
        ];
        let currencies = HashMap::from([
            ("AAPL US Equity".to_string(), "USD".to_string()),
            ("VOD LN Equity".to_string(), "GBP".to_string()),
            ("BARC LN Equity".to_string(), "GBp".to_string()),
        ]);

        let (fx_by_ticker, fx_pairs) = build_fx_requirements(&tickers, &currencies, "USD");
        assert_eq!(fx_pairs, vec!["USDGBP Curncy".to_string()]);
        assert_eq!(fx_by_ticker.len(), 2);
        assert_eq!(fx_by_ticker["VOD LN Equity"].factor, 1.0);
        assert_eq!(fx_by_ticker["BARC LN Equity"].factor, 100.0);
    }

    #[test]
    fn test_resolve_fx_query_dates_prefers_batch_dates() {
        let d1 = parse_date_key("2024-01-02").unwrap();
        let d2 = parse_date_key("2024-01-10").unwrap();
        let date_keys = vec![Some(d2), None, Some(d1)];

        let (start, end) = resolve_fx_query_dates(&date_keys, "20230101", "20230131");
        assert_eq!(start, "20240102");
        assert_eq!(end, "20240110");
    }

    #[test]
    fn test_apply_fx_conversion_uses_divide_and_factor() {
        let d1 = parse_date_key("2024-01-01").unwrap();
        let d2 = parse_date_key("2024-01-02").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, true),
            Field::new("VOD LN Equity|PX_LAST", DataType::Float64, true),
        ]));

        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2)])),
                Arc::new(Float64Array::from(vec![Some(72.5), Some(73.0)])),
            ],
        )
        .unwrap();

        let column_tickers = HashMap::from([(1usize, "VOD LN Equity".to_string())]);
        let fx_by_ticker =
            HashMap::from([("VOD LN Equity".to_string(), build_fx_pair("GBp", "USD"))]);
        let fx_rates = HashMap::from([(
            "USDGBP Curncy".to_string(),
            HashMap::from([(d1, 1.25), (d2, 1.25)]),
        )]);

        let converted = apply_fx_conversion(
            data,
            &column_tickers,
            &[Some(d1), Some(d2)],
            &fx_by_ticker,
            &fx_rates,
        )
        .unwrap();

        let values = converted
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // 72.5 / (1.25 * 100) = 0.58, 73.0 / 125 = 0.584
        assert!((values.value(0) - 0.58).abs() < 1e-10);
        assert!((values.value(1) - 0.584).abs() < 1e-10);
    }

    #[test]
    fn test_apply_fx_conversion_partial_rates_nulls_missing_rows() {
        let d1 = parse_date_key("2024-01-01").unwrap();
        let d2 = parse_date_key("2024-01-02").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, true),
            Field::new("VOD LN Equity|PX_LAST", DataType::Float64, true),
        ]));

        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2)])),
                Arc::new(Float64Array::from(vec![Some(72.5), Some(73.0)])),
            ],
        )
        .unwrap();

        let column_tickers = HashMap::from([(1usize, "VOD LN Equity".to_string())]);
        let fx_by_ticker =
            HashMap::from([("VOD LN Equity".to_string(), build_fx_pair("GBp", "USD"))]);
        // Only d1 has a rate; d2 is missing so its converted value must be null.
        let fx_rates = HashMap::from([("USDGBP Curncy".to_string(), HashMap::from([(d1, 1.25)]))]);

        let converted = apply_fx_conversion(
            data,
            &column_tickers,
            &[Some(d1), Some(d2)],
            &fx_by_ticker,
            &fx_rates,
        )
        .unwrap();

        // Shape is preserved: same row and column count as the input.
        assert_eq!(converted.num_rows(), 2);
        assert_eq!(converted.num_columns(), 2);

        let values = converted
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // 72.5 / (1.25 * 100) = 0.58 for d1; d2 has no rate and is null.
        assert!(!values.is_null(0));
        assert!((values.value(0) - 0.58).abs() < 1e-10);
        assert!(values.is_null(1));
    }
}
