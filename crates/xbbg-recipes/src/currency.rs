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

use arrow_arith::numeric::div;
use arrow_array::{
    builder::GenericStringBuilder, Array, ArrayRef, Date32Array, Float64Array, GenericStringArray,
    LargeStringArray, OffsetSizeTrait, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::currency::{build_fx_pair, same_currency, FxConversionInfo};
use xbbg_ext::{fmt_date, parse_date};

use crate::error::{RecipeError, Result};
use crate::utils::{
    apply_request_options, array_value_as_f64, array_value_as_string, as_string_col,
    date32_to_naive,
};

const DATE_COL: &str = "date";
const FX_FIELD: &str = "PX_LAST";
const CURRENCY_FIELD: &str = "CRNCY";

type FxRatesByPair = HashMap<String, HashMap<i32, f64>>;

/// Adjust wide or long historical data into a target currency.
///
/// Wide columns identify tickers by `ticker` or `ticker|field`; long data uses
/// `ticker`, `date`, and a value column. String values remain strings and numeric
/// values become Float64. Dictionary columns are decoded; other columns retain
/// their types. Date32, Date64, zoned timestamps, and date strings are accepted.
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
    options: RequestParams,
) -> Result<RecordBatch> {
    if data.num_rows() == 0 || data.num_columns() == 0 || target_ccy.eq_ignore_ascii_case("local") {
        return Ok(data);
    }

    let data = decode_currency_dictionaries(data)?;
    let long = data.column_by_name("ticker").is_some() && find_value_column(&data).is_ok();
    if long && find_value_column(&data)?.data_type() == &DataType::Null {
        return Ok(data);
    }
    let (column_tickers, tickers) = if long {
        let ticker_col = data.column_by_name("ticker").expect("long ticker column");
        let mut seen = HashSet::new();
        let tickers = (0..data.num_rows())
            .filter_map(|row| text_value(ticker_col, row))
            .filter(|ticker| !ticker.is_empty() && seen.insert(*ticker))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        (HashMap::new(), tickers)
    } else {
        extract_ticker_columns(&data)
    };
    if tickers.is_empty() {
        return Ok(data);
    }
    let Some(date_keys) = extract_date_keys(&data)? else {
        return Ok(data);
    };
    if !date_keys.iter().any(Option::is_some) {
        return Ok(data);
    }

    let ticker_currencies = fetch_ticker_currencies(engine, &tickers, &options).await?;
    let (fx_by_ticker, fx_pairs) = build_fx_requirements(&tickers, &ticker_currencies, &target_ccy);
    if fx_pairs.is_empty() {
        return Ok(data);
    }
    let (fx_start, fx_end) = resolve_fx_query_dates(&date_keys, &start_date, &end_date);
    let fx_rates = fetch_fx_rates(engine, &fx_pairs, &fx_start, &fx_end, &options).await?;

    if long {
        apply_long_fx_conversion(data, &date_keys, &fx_by_ticker, &fx_rates)
    } else {
        apply_fx_conversion(data, &column_tickers, &date_keys, &fx_by_ticker, &fx_rates)
    }
}

fn decode_currency_dictionaries(data: RecordBatch) -> Result<RecordBatch> {
    let schema = data.schema();
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Dictionary(_, _)))
    {
        return Ok(data);
    }
    let mut fields = schema.fields().to_vec();
    let mut columns = data.columns().to_vec();
    for (idx, field) in schema.fields().iter().enumerate() {
        if let DataType::Dictionary(_, value_type) = field.data_type() {
            columns[idx] = arrow_cast::cast(data.column(idx), value_type)?;
            fields[idx] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(value_type.as_ref().clone())
                    .with_nullable(field.is_nullable() || columns[idx].null_count() > 0),
            );
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(Into::into)
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
    options: &RequestParams,
) -> Result<HashMap<String, String>> {
    let batch = engine.request(currency_request(tickers, options)).await?;
    parse_currency_batch(&batch)
}

fn currency_request(tickers: &[String], options: &RequestParams) -> RequestParams {
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(tickers.to_vec()),
        fields: Some(vec![CURRENCY_FIELD.to_string()]),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    params.format = Some("long".into());
    params
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
    options: &RequestParams,
) -> Result<FxRatesByPair> {
    let batch = engine
        .request(fx_request(fx_pairs, start_date, end_date, options))
        .await?;
    parse_fx_rate_batch(&batch)
}

fn fx_request(
    fx_pairs: &[String],
    start_date: &str,
    end_date: &str,
    options: &RequestParams,
) -> RequestParams {
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(fx_pairs.to_vec()),
        fields: Some(vec![FX_FIELD.to_string()]),
        start_date: Some(start_date.to_string()),
        end_date: Some(end_date.to_string()),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    params.format = Some("long".into());
    params
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

        if !rate.is_finite() || rate.abs() <= f64::EPSILON {
            continue;
        }

        out.entry(ticker_col.value(row).to_string())
            .or_default()
            .insert(date_key, rate);
    }

    Ok(out)
}

fn apply_long_fx_conversion(
    data: RecordBatch,
    date_keys: &[Option<i32>],
    fx_by_ticker: &HashMap<String, FxConversionInfo>,
    fx_rates: &FxRatesByPair,
) -> Result<RecordBatch> {
    let tickers = data.column_by_name("ticker").ok_or_else(|| {
        RecipeError::InvalidArgument("currency data requires a ticker column".into())
    })?;
    let schema = data.schema();
    let value_idx = ["value", "value_f64", "value_i64", "value_str"]
        .iter()
        .find_map(|name| schema.index_of(name).ok())
        .ok_or_else(|| {
            RecipeError::InvalidArgument("currency data requires a value column".into())
        })?;
    let source_values = data.column(value_idx);
    if source_values.data_type() == &DataType::Null {
        return Ok(data);
    }
    let values = if source_values.data_type() == &DataType::Utf8View {
        arrow_cast::cast(source_values, &DataType::Utf8)?
    } else {
        source_values.clone()
    };
    let converted: ArrayRef = if let Some(strings) = values.as_any().downcast_ref::<StringArray>() {
        Arc::new(convert_long_strings(
            strings,
            tickers,
            date_keys,
            fx_by_ticker,
            fx_rates,
        ))
    } else if let Some(strings) = values.as_any().downcast_ref::<LargeStringArray>() {
        Arc::new(convert_long_strings(
            strings,
            tickers,
            date_keys,
            fx_by_ticker,
            fx_rates,
        ))
    } else if values.data_type().is_numeric() {
        let numeric = arrow_cast::cast(&values, &DataType::Float64)?;
        let numeric = numeric
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 cast");
        Arc::new(Float64Array::from_iter((0..data.num_rows()).map(|row| {
            if numeric.is_null(row) {
                return None;
            }
            let value = numeric.value(row);
            let ticker = text_value(tickers, row);
            match ticker.and_then(|ticker| fx_by_ticker.get(ticker)) {
                Some(info) => {
                    fx_denominator(info, date_keys[row], fx_rates).map(|rate| value / rate)
                }
                None => Some(value),
            }
        })))
    } else {
        return Err(RecipeError::InvalidArgument(format!(
            "currency value column must be numeric or text, got {:?}",
            values.data_type()
        )));
    };
    let missing = converted.null_count().saturating_sub(values.null_count());
    if missing > 0 {
        xbbg_log::warn!(
            missing,
            "FX rates missing or invalid; converted values are null for those rows"
        );
    }
    let mut fields = schema.fields().to_vec();
    fields[value_idx] = Arc::new(
        Field::new(
            fields[value_idx].name(),
            converted.data_type().clone(),
            true,
        )
        .with_metadata(fields[value_idx].metadata().clone()),
    );
    let mut columns = data.columns().to_vec();
    columns[value_idx] = converted;
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(Into::into)
}

fn convert_long_strings<O: OffsetSizeTrait>(
    values: &GenericStringArray<O>,
    tickers: &ArrayRef,
    dates: &[Option<i32>],
    fx_by_ticker: &HashMap<String, FxConversionInfo>,
    fx_rates: &FxRatesByPair,
) -> GenericStringArray<O> {
    let mut converted =
        GenericStringBuilder::<O>::with_capacity(values.len(), values.value_data().len());
    for (row, value) in values.iter().enumerate() {
        let info = text_value(tickers, row).and_then(|ticker| fx_by_ticker.get(ticker));
        match (value, info) {
            (Some(value), Some(info)) => {
                if let Ok(number) = value.trim().parse::<f64>() {
                    converted.append_option(
                        fx_denominator(info, dates[row], fx_rates)
                            .filter(|_| number.is_finite())
                            .map(|rate| (number / rate).to_string()),
                    );
                } else {
                    converted.append_value(value);
                }
            }
            _ => converted.append_option(value),
        }
    }
    converted.finish()
}

fn text_value(array: &ArrayRef, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        Some(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        Some(values.value(row))
    } else {
        array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|values| values.value(row))
    }
}

fn fx_denominator(
    info: &FxConversionInfo,
    date: Option<i32>,
    rates: &FxRatesByPair,
) -> Option<f64> {
    let rate = *rates.get(&info.fx_pair)?.get(&date?)?;
    let denominator = rate * info.factor;
    (denominator.is_finite() && denominator.abs() > f64::EPSILON).then_some(denominator)
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
    let mut fields = schema.fields().to_vec();
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

        if input_col.data_type() == &DataType::Null {
            new_columns.push(input_col);
            continue;
        }
        if !input_col.data_type().is_numeric() {
            return Err(RecipeError::InvalidArgument(format!(
                "currency column '{}' must be numeric, got {:?}",
                field.name(),
                input_col.data_type()
            )));
        }
        let numeric = arrow_cast::cast(&input_col, &DataType::Float64)?;
        let values = numeric
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 cast");

        let rates_by_date = fx_rates.get(&fx_info.fx_pair);
        let fx_rate_array =
            build_fx_rate_array(values.len(), date_keys, rates_by_date, fx_info.factor);
        let null_count = fx_rate_array.null_count();
        let len = fx_rate_array.len();
        if null_count > 0 {
            xbbg_log::warn!(
                ticker = ticker.as_str(),
                fx_pair = fx_info.fx_pair.as_str(),
                missing = null_count,
                total = len,
                "FX rates missing for some dates; converted values are null for those rows"
            );
        }

        let converted = div(values, &fx_rate_array).map_err(|err| {
            RecipeError::Other(format!(
                "failed to convert column '{}' using FX pair '{}': {err}",
                field.name(),
                fx_info.fx_pair
            ))
        })?;
        fields[idx] = Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(DataType::Float64)
                .with_nullable(field.is_nullable() || converted.null_count() > 0),
        );

        new_columns.push(converted);
    }

    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        new_columns,
    )
    .map_err(Into::into)
}

fn build_fx_rate_array(
    num_rows: usize,
    date_keys: &[Option<i32>],
    rates_by_date: Option<&HashMap<i32, f64>>,
    factor: f64,
) -> Float64Array {
    Float64Array::from_iter((0..num_rows).map(|row| {
        let date = date_keys.get(row).copied().flatten()?;
        let denominator = rates_by_date?.get(&date)? * factor;
        (denominator.is_finite() && denominator.abs() > f64::EPSILON).then_some(denominator)
    }))
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
    if date_col.data_type() == &DataType::Null || date_col.null_count() == date_col.len() {
        return Ok(Some(vec![None; date_col.len()]));
    }

    if let Some(col) = date_col.as_any().downcast_ref::<Date32Array>() {
        let mut values = Vec::with_capacity(col.len());
        for row in 0..col.len() {
            values.push((!col.is_null(row)).then(|| col.value(row)));
        }
        return Ok(Some(values));
    }

    if matches!(
        date_col.data_type(),
        DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        let dates = arrow_cast::cast(date_col, &DataType::Date32)?;
        let dates = dates
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Date32 cast");
        return Ok(Some(dates.iter().collect()));
    }

    if matches!(
        date_col.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Ok(Some(
            (0..date_col.len())
                .map(|row| text_value(date_col, row).and_then(parse_date_key))
                .collect(),
        ));
    }

    Err(RecipeError::Other(format!(
        "'date' column must be a date, timestamp, or Utf8, got {:?}",
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

    #[test]
    fn long_conversion_keeps_text_rows_and_nulls_missing_rates() {
        let d1 = parse_date_key("2024-01-02").unwrap();
        let d2 = d1 + 1;
        let data = RecordBatch::try_from_iter(vec![
            (
                "ticker",
                Arc::new(StringArray::from(vec![
                    "ABC LN Equity",
                    "ABC LN Equity",
                    "ABC LN Equity",
                    "XYZ US Equity",
                ])) as ArrayRef,
            ),
            ("date", Arc::new(Date32Array::from(vec![d1, d2, d1, d1]))),
            ("field", Arc::new(StringArray::from(vec!["PX_LAST"; 4]))),
            (
                "value",
                Arc::new(StringArray::from(vec!["100", "200", "N/A", "30"])),
            ),
        ])
        .unwrap();
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBp", "USD"))]);
        let rates = HashMap::from([("USDGBP Curncy".into(), HashMap::from([(d1, 2.0)]))]);
        let converted = apply_long_fx_conversion(
            data.clone(),
            &[Some(d1), Some(d2), Some(d1), Some(d1)],
            &info,
            &rates,
        )
        .unwrap();
        let values = as_string_col(&converted, "value").unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some("0.5"), None, Some("N/A"), Some("30")]
        );
        for name in ["ticker", "date", "field"] {
            assert!(Arc::ptr_eq(
                data.column_by_name(name).unwrap(),
                converted.column_by_name(name).unwrap()
            ));
        }
        let no_rates = apply_long_fx_conversion(
            data,
            &[Some(d1), Some(d2), Some(d1), Some(d1)],
            &info,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            as_string_col(&no_rates, "value")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None, Some("N/A"), Some("30")]
        );
    }

    #[test]
    fn long_integer_conversion_preserves_fractional_results() {
        let d1 = parse_date_key("2024-01-02").unwrap();
        let data = RecordBatch::try_from_iter(vec![
            (
                "ticker",
                Arc::new(StringArray::from(vec!["ABC LN Equity"])) as ArrayRef,
            ),
            ("date", Arc::new(Date32Array::from(vec![d1]))),
            ("value", Arc::new(arrow_array::Int64Array::from(vec![5]))),
        ])
        .unwrap();
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBP", "USD"))]);
        let rates = HashMap::from([("USDGBP Curncy".into(), HashMap::from([(d1, 2.0)]))]);
        let converted = apply_long_fx_conversion(data, &[Some(d1)], &info, &rates).unwrap();
        let values = converted
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 2.5);
    }

    #[test]
    fn currency_requests_preserve_controls_and_require_long_results() {
        let options = RequestParams {
            kwargs: Some(HashMap::from([(
                "periodicitySelection".into(),
                "WEEKLY".into(),
            )])),
            overrides: Some(vec![("CUSTOM_OVERRIDE".into(), "1".into())]),
            validate_fields: Some(false),
            return_eids: true,
            format: Some("wide".into()),
            ..Default::default()
        };
        let currency = currency_request(&["ABC LN Equity".into()], &options);
        assert_eq!(currency.fields.unwrap(), vec!["CRNCY"]);
        assert_eq!(currency.format.as_deref(), Some("long"));
        assert_eq!(currency.overrides, options.overrides);
        assert_eq!(currency.validate_fields, Some(false));
        let fx = fx_request(&["USDGBP Curncy".into()], "20240102", "20240105", &options);
        assert_eq!(fx.fields.unwrap(), vec!["PX_LAST"]);
        assert_eq!(fx.start_date.as_deref(), Some("20240102"));
        assert_eq!(fx.end_date.as_deref(), Some("20240105"));
        assert_eq!(fx.kwargs, options.kwargs);
        assert!(fx.return_eids);
        assert_eq!(fx.format.as_deref(), Some("long"));
    }

    #[test]
    fn currency_dates_accept_foreign_dataframe_timestamp_units() {
        let days = parse_date_key("2024-01-02").unwrap();
        let timestamps: ArrayRef = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            Some(i64::from(days) * 86_400_000_000_000 + 1),
            Some(-1),
            None,
        ]));
        let batch = RecordBatch::try_from_iter(vec![("date", timestamps)]).unwrap();
        assert_eq!(
            extract_date_keys(&batch).unwrap().unwrap(),
            vec![Some(days), Some(-1), None]
        );
    }

    #[test]
    fn long_conversion_accepts_foreign_arrow_string_layouts() {
        let date = parse_date_key("2024-01-02").unwrap();
        let data = RecordBatch::try_from_iter(vec![
            (
                "ticker",
                Arc::new(LargeStringArray::from(vec!["ABC LN Equity"])) as ArrayRef,
            ),
            ("date", Arc::new(StringViewArray::from(vec!["2024-01-02"]))),
            ("value", Arc::new(StringViewArray::from(vec!["5"]))),
        ])
        .unwrap();
        let keys = extract_date_keys(&data).unwrap().unwrap();
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBP", "USD"))]);
        let rates = HashMap::from([("USDGBP Curncy".into(), HashMap::from([(date, 2.0)]))]);
        let converted = apply_long_fx_conversion(data, &keys, &info, &rates).unwrap();
        assert_eq!(as_string_col(&converted, "value").unwrap().value(0), "2.5");
    }

    #[test]
    fn long_conversion_preserves_untyped_null_values_and_dates() {
        let data = RecordBatch::try_from_iter(vec![
            (
                "ticker",
                Arc::new(StringArray::from(vec!["ABC LN Equity"])) as ArrayRef,
            ),
            ("date", Arc::new(arrow_array::NullArray::new(1))),
            ("value", Arc::new(arrow_array::NullArray::new(1))),
        ])
        .unwrap();
        let dates = extract_date_keys(&data).unwrap().unwrap();
        assert_eq!(dates, vec![None]);
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBP", "USD"))]);
        let converted =
            apply_long_fx_conversion(data.clone(), &dates, &info, &HashMap::new()).unwrap();
        assert!(Arc::ptr_eq(
            data.column_by_name("value").unwrap(),
            converted.column_by_name("value").unwrap()
        ));
        assert_eq!(converted.schema(), data.schema());
    }

    #[test]
    fn wide_integer_currency_values_convert_without_truncating() {
        let date = parse_date_key("2024-01-02").unwrap();
        let data = RecordBatch::try_from_iter(vec![
            ("date", Arc::new(Date32Array::from(vec![date])) as ArrayRef),
            (
                "ABC LN Equity",
                Arc::new(arrow_array::Int32Array::from(vec![5])),
            ),
        ])
        .unwrap();
        let (columns, _) = extract_ticker_columns(&data);
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBP", "USD"))]);
        let rates = HashMap::from([("USDGBP Curncy".into(), HashMap::from([(date, 2.0)]))]);
        let converted = apply_fx_conversion(data, &columns, &[Some(date)], &info, &rates).unwrap();
        let values = converted
            .column_by_name("ABC LN Equity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 2.5);
        assert_eq!(converted.schema().field(1).data_type(), &DataType::Float64);
    }

    #[test]
    fn categorical_currency_inputs_are_decoded_before_conversion() {
        let mut ticker =
            arrow_array::builder::StringDictionaryBuilder::<arrow_array::types::Int8Type>::new();
        ticker.append("ABC LN Equity").unwrap();
        let mut value =
            arrow_array::builder::StringDictionaryBuilder::<arrow_array::types::Int8Type>::new();
        value.append("5").unwrap();
        let date = parse_date_key("2024-01-02").unwrap();
        let data = RecordBatch::try_from_iter(vec![
            ("ticker", Arc::new(ticker.finish()) as ArrayRef),
            ("date", Arc::new(Date32Array::from(vec![date]))),
            ("value", Arc::new(value.finish())),
        ])
        .unwrap();
        let data = decode_currency_dictionaries(data).unwrap();
        assert_eq!(
            text_value(data.column_by_name("ticker").unwrap(), 0),
            Some("ABC LN Equity")
        );
        let info = HashMap::from([("ABC LN Equity".into(), build_fx_pair("GBP", "USD"))]);
        let rates = HashMap::from([("USDGBP Curncy".into(), HashMap::from([(date, 2.0)]))]);
        let converted = apply_long_fx_conversion(data, &[Some(date)], &info, &rates).unwrap();
        assert_eq!(as_string_col(&converted, "value").unwrap().value(0), "2.5");
    }

    #[test]
    fn timestamp_fx_dates_use_the_local_calendar_date() {
        let date = parse_date_key("2024-01-02").unwrap();
        let timestamps = arrow_array::TimestampNanosecondArray::from(vec![
            (i64::from(date) * 86_400 - 9 * 3_600) * 1_000_000_000,
        ])
        .with_timezone("Asia/Tokyo");
        let data =
            RecordBatch::try_from_iter(vec![("date", Arc::new(timestamps) as ArrayRef)]).unwrap();
        assert_eq!(extract_date_keys(&data).unwrap().unwrap(), vec![Some(date)]);
    }

    #[test]
    fn typed_fx_rates_reject_zero_and_nonfinite_values() {
        let date = parse_date_key("2024-01-02").unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "ticker",
                Arc::new(StringArray::from(vec!["USDGBP Curncy"; 4])) as ArrayRef,
            ),
            (
                "date",
                Arc::new(Date32Array::from(vec![date, date + 1, date + 2, date + 3])),
            ),
            ("field", Arc::new(StringArray::from(vec!["PX_LAST"; 4]))),
            (
                "value",
                Arc::new(Float64Array::from(vec![2.0, 0.0, f64::NAN, f64::INFINITY])),
            ),
        ])
        .unwrap();
        assert_eq!(
            parse_fx_rate_batch(&batch).unwrap(),
            HashMap::from([("USDGBP Curncy".into(), HashMap::from([(date, 2.0)])),])
        );
    }

    #[test]
    fn wide_fx_denominators_null_invalid_or_overflowing_rates() {
        let rates = HashMap::from([(1, 2.0), (2, f64::MAX), (3, f64::INFINITY), (4, 0.0)]);
        let values = build_fx_rate_array(
            5,
            &[Some(1), Some(2), Some(3), Some(4), None],
            Some(&rates),
            100.0,
        );
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(200.0), None, None, None, None]
        );
    }
}
