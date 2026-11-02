//! Fixed income recipe functions.
//!
//! High-level recipes for Bloomberg fixed income data queries.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Float16Array, Float32Array, Float64Array, RecordBatch, RecordBatchOptions,
    UInt64Array, new_empty_array, new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_ord::sort::{SortOptions, sort_to_indices};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::{take, take_record_batch};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::bql::{build_corporate_bonds_query, build_preferreds_query};
use xbbg_ext::transforms::fixed_income::{YieldType, build_yas_overrides};
use xbbg_ext::utils::date::parse_date;

use crate::error::{RecipeError, Result};
use crate::utils::{TextValues, apply_request_options};

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
/// * `options` - Request controls; explicit overrides take precedence over YAS defaults
///
/// # Returns
///
/// Arrow RecordBatch in the requested native reference-data format (long by default).
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
///     RequestParams::default(),
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
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = yas_request(
        tickers, fields, settle_dt, yield_type, spread, yield_val, price, benchmark, &options,
    );
    engine.request(params).await.map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn yas_request(
    tickers: Vec<String>,
    fields: Vec<String>,
    settle_dt: Option<String>,
    yield_type: Option<YieldType>,
    spread: Option<f64>,
    yield_val: Option<f64>,
    price: Option<f64>,
    benchmark: Option<String>,
    options: &RequestParams,
) -> RequestParams {
    let overrides = build_yas_overrides(
        settle_dt.as_deref(),
        yield_type,
        spread,
        yield_val,
        price,
        benchmark.as_deref(),
    );

    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(tickers),
        fields: Some(fields),
        overrides: Some(overrides),
        ..Default::default()
    };

    apply_request_options(&mut params, options);
    params
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
/// * `options` - BQL request elements, overrides, and engine controls
///
/// # Returns
///
/// Arrow RecordBatch with preferred stock data
pub async fn recipe_preferreds(
    engine: &Engine,
    equity_ticker: String,
    fields: Option<Vec<String>>,
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = preferreds_request(
        &equity_ticker,
        fields.as_deref().unwrap_or_default(),
        &options,
    );
    engine.request(params).await.map_err(Into::into)
}

fn preferreds_request(
    equity_ticker: &str,
    fields: &[String],
    options: &RequestParams,
) -> RequestParams {
    let extra_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
    bql_request(build_preferreds_query(equity_ticker, &extra_refs), options)
}

fn bql_request(bql_query: String, options: &RequestParams) -> RequestParams {
    let mut params = RequestParams {
        service: Service::BqlSvc.to_string(),
        operation: Operation::BqlSendQuery.to_string(),
        elements: Some(vec![("expression".to_string(), bql_query)]),
        ..Default::default()
    };

    apply_request_options(&mut params, options);
    params
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
/// * `options` - BQL request elements, overrides, and engine controls
///
/// # Returns
///
/// Arrow RecordBatch with corporate bond data
pub async fn recipe_corporate_bonds(
    engine: &Engine,
    ticker: String,
    ccy: Option<String>,
    fields: Option<Vec<String>>,
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = corporate_bonds_request(
        &ticker,
        ccy.as_deref(),
        fields.as_deref().unwrap_or_default(),
        &options,
    );
    engine.request(params).await.map_err(Into::into)
}

fn corporate_bonds_request(
    ticker: &str,
    ccy: Option<&str>,
    fields: &[String],
    options: &RequestParams,
) -> RequestParams {
    let extra_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
    bql_request(
        build_corporate_bonds_query(ticker, ccy, &extra_refs),
        options,
    )
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
/// * `start_datetime` - ISO datetime; defaults to one hour before the end
/// * `end_datetime` - ISO datetime; defaults to the current UTC instant
/// * `event_types` - Event types to retrieve (default: BID, ASK)
/// * `include_broker_codes` - Request and require broker attribution (default: true)
/// * `options` - Extra include flags, request/output timezones, and engine controls
///
/// # Returns
///
/// Quote rows shaped by [`shape_bqr_quotes`]: time-sorted, retaining extra
/// include fields, timestamp timezones and schema metadata, with Bloomberg
/// names normalized to `event_type`, `price`, `broker_buy`, `broker_sell`,
/// `spread_price`, `condition_codes`, and `exchange`. Nonempty results without
/// broker attribution fail when it is required.
pub async fn recipe_bqr(
    engine: &Engine,
    ticker: String,
    start_datetime: Option<String>,
    end_datetime: Option<String>,
    event_types: Option<Vec<String>>,
    include_broker_codes: bool,
    options: RequestParams,
) -> Result<RecordBatch> {
    let params = bqr_request(
        ticker.clone(),
        start_datetime,
        end_datetime,
        event_types,
        include_broker_codes,
        &options,
    )?;
    let batch = engine.request(params).await?;
    shape_bqr_quotes(
        batch,
        &ticker,
        engine.key_column().as_str(),
        include_broker_codes,
    )
}

fn bqr_request(
    ticker: String,
    start_datetime: Option<String>,
    end_datetime: Option<String>,
    event_types: Option<Vec<String>>,
    include_broker_codes: bool,
    options: &RequestParams,
) -> Result<RequestParams> {
    let end = normalize_bqr_datetime(end_datetime.unwrap_or_else(|| Utc::now().to_rfc3339()));
    let start = match start_datetime {
        Some(value) => normalize_bqr_datetime(value),
        None => bqr_hour_before(&end)?,
    };
    let mut params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::IntradayTick.to_string(),
        security: Some(ticker),
        start_datetime: Some(start),
        end_datetime: Some(end),
        event_types: Some(
            event_types.unwrap_or_else(|| vec!["BID".to_string(), "ASK".to_string()]),
        ),
        options: include_broker_codes
            .then(|| vec![("includeBrokerCodes".to_string(), "true".to_string())]),
        ..Default::default()
    };
    apply_request_options(&mut params, options);
    if let Some(kwargs) = params.kwargs.as_mut() {
        for (alias, name) in [
            ("include_broker_codes", "includeBrokerCodes"),
            ("include_condition_codes", "includeConditionCodes"),
            ("include_exchange_codes", "includeExchangeCodes"),
            ("include_spread_price", "includeSpreadPrice"),
            ("include_yield", "includeYield"),
        ] {
            if let Some(value) = kwargs.remove(alias) {
                kwargs.entry(name.to_string()).or_insert(value);
            }
        }
    }
    Ok(params)
}

fn normalize_bqr_datetime(mut value: String) -> String {
    if value.contains(' ') {
        value = value.replace(' ', "T");
    }
    if value.len() == 16 && value.contains('T') {
        value.push_str(":00");
    }
    value
}

fn bqr_hour_before(end: &str) -> Result<String> {
    let invalid = || RecipeError::InvalidArgument(format!("invalid BQR end datetime: {end}"));
    if let Ok(value) = DateTime::parse_from_rfc3339(end) {
        return value
            .checked_sub_signed(Duration::hours(1))
            .map(|start| start.to_rfc3339())
            .ok_or_else(invalid);
    }
    let value = NaiveDateTime::parse_from_str(end, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| parse_date(end).ok()?.and_hms_opt(0, 0, 0))
        .ok_or_else(invalid)?;
    value
        .checked_sub_signed(Duration::hours(1))
        .map(|start| start.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        .ok_or_else(invalid)
}

/// Shape raw BQR IntradayTick output into time-sorted, normalized quote rows.
///
/// The one BQR shaper, used by [`recipe_bqr`] (Node and `xbbg.ext`) and by
/// Python's `blp.abqr`:
///
/// 1. Generic extractor output (with a `path` column) is pivoted by
///    [`reshape_bqr_generic`], with the security in the engine's `key`
///    column; typed output passes through.
/// 2. With `enforce_broker_codes`, nonempty quotes without any nonempty broker
///    code fail with [`RecipeError::Other`].
/// 3. Rows are sorted by `time` ascending, nulls last.
/// 4. Bloomberg names become `event_type`, `price`, `broker_buy`,
///    `broker_sell`, `spread_price`, `condition_codes` and `exchange`.
///
/// Other columns, column types and schema metadata are kept.
pub fn shape_bqr_quotes(
    mut batch: RecordBatch,
    ticker: &str,
    key: &str,
    enforce_broker_codes: bool,
) -> Result<RecordBatch> {
    if batch.column_by_name("path").is_some() {
        batch = reshape_bqr_generic(&batch, ticker, key)?;
    }
    if enforce_broker_codes && batch.num_rows() > 0 && !bqr_has_broker_codes(&batch) {
        return Err(RecipeError::Other(
            "BQR returned quote rows without broker attribution. Use a fixed-income ticker \
             with a dealer quote pricing source such as '@MSG1 Corp', or pass \
             include_broker_codes=False if raw quote ticks without dealer codes are intentional."
                .to_string(),
        ));
    }
    if batch.num_rows() > 1
        && let Some(time) = batch.column_by_name("time")
    {
        let indices = sort_to_indices(
            time.as_ref(),
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
        )?;
        if indices
            .values()
            .iter()
            .enumerate()
            .any(|(row, &index)| row != index as usize)
        {
            batch = take_record_batch(&batch, &indices)?;
        }
    }
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = match field.name().as_str() {
                "type" => "event_type",
                "value" => "price",
                "brokerBuyCode" => "broker_buy",
                "brokerSellCode" => "broker_sell",
                "spreadPrice" => "spread_price",
                "conditionCodes" => "condition_codes",
                "exchangeCode" => "exchange",
                _ => return Arc::clone(field),
            };
            Arc::new(field.as_ref().clone().with_name(name))
        })
        .collect::<Vec<_>>();
    let schema = Schema::new_with_metadata(fields, batch.schema().metadata().clone());
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema),
        batch.columns().to_vec(),
        &options,
    )?)
}

fn bqr_has_broker_codes(batch: &RecordBatch) -> bool {
    [
        "brokerBuyCode",
        "brokerSellCode",
        "broker_buy",
        "broker_sell",
    ]
    .iter()
    .filter_map(|name| batch.column_by_name(name))
    .any(|column| match TextValues::new(column.as_ref()) {
        TextValues::Other => column.null_count() < column.len(),
        codes => (0..column.len()).any(|row| codes.get(row).is_some_and(|code| !code.is_empty())),
    })
}

/// The tick index and field name of a generic `...tickData[i].<field>` path.
fn bqr_path(path: &str) -> Option<(usize, &str)> {
    let start = path.find("tickData[")? + "tickData[".len();
    let rest = &path[start..];
    let end = rest.find(']')?;
    let row = rest[..end].parse().ok()?;
    let field = rest.get(end + 1..)?.strip_prefix('.')?;
    let len = field
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    (len > 0).then_some((row, &field[..len]))
}

/// Tick columns of the generic BQR reshape, in output order after the key column.
const BQR_TICK_COLUMNS: [&str; 4] = ["time", "type", "value", "size"];

/// Pivot generic BQR `path`/`value_str`/`value_num` rows into one row per tick.
///
/// The one generic BQR reshape: [`shape_bqr_quotes`] (Python `blp.abqr`,
/// `xbbg.ext.abqr`, Node `bqr`) and Python's `ArrowTable.reshape_bqr_generic`
/// use it.
///
/// A row whose `path` names `tickData[i].<field>` sets `<field>` of tick `i`;
/// a later row for the same field wins, and other rows are skipped. Ticks are
/// ordered by index. Columns are the security in `key`, then `time`, `type`,
/// `value` and `size` when present, then the other fields by name; a field
/// named `key` is dropped.
///
/// A field's value is the row's nonempty `value_str`, otherwise its
/// `value_num`. A field holding any text, including a string `value_num`, is
/// UTF-8: floats render in Rust's decimal notation and other values as Arrow
/// displays them. A field holding only `value_num` values keeps the
/// `value_num` type, Float64 for the generic extractor. A field without values
/// is all-null UTF-8. Without tick rows, including without a string `path`
/// column, the result is the empty five-column UTF-8 quote table. Schema
/// metadata is kept.
///
/// # Errors
///
/// [`RecipeError::InvalidArgument`] when `value_str` is not a string column.
pub fn reshape_bqr_generic(batch: &RecordBatch, ticker: &str, key: &str) -> Result<RecordBatch> {
    let strings = match batch.column_by_name("value_str") {
        None => TextValues::Other,
        Some(column) => match TextValues::new(column.as_ref()) {
            TextValues::Other => {
                return Err(RecipeError::InvalidArgument(format!(
                    "generic BQR value_str must be a string column, not {}",
                    column.data_type()
                )));
            }
            text => text,
        },
    };
    let numbers = batch
        .column_by_name("value_num")
        .map(|column| BqrNumbers::new(column.as_ref()));

    // `(field, tick, row)` per tick path row, field names interned in
    // first-seen order.
    let mut field_ids = HashMap::new();
    let mut fields = Vec::new();
    let mut entries = Vec::new();
    if let Some(paths) = batch.column_by_name("path") {
        let paths = TextValues::new(paths.as_ref());
        for row in 0..batch.num_rows() {
            let Some((tick, name)) = paths.get(row).and_then(bqr_path) else {
                continue;
            };
            let field = *field_ids.entry(name).or_insert_with(|| {
                fields.push(name);
                fields.len() - 1
            });
            entries.push((field, tick, row));
        }
    }

    let metadata = batch.schema().metadata().clone();
    if entries.is_empty() {
        let fields = std::iter::once(key)
            .chain(BQR_TICK_COLUMNS)
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>();
        let columns = vec![new_empty_array(&DataType::Utf8); fields.len()];
        let schema = Schema::new_with_metadata(fields, metadata);
        return Ok(RecordBatch::try_new(Arc::new(schema), columns)?);
    }

    // Each field's entries become contiguous and tick-ordered, with the last
    // row of a tick last.
    entries.sort_unstable();
    let mut ticks = entries.iter().map(|&(_, tick, _)| tick).collect::<Vec<_>>();
    ticks.sort_unstable();
    ticks.dedup();

    let mut order = BQR_TICK_COLUMNS
        .iter()
        .filter_map(|name| field_ids.get(name).copied())
        .collect::<Vec<_>>();
    let mut others = (0..fields.len())
        .filter(|&field| fields[field] != key && !BQR_TICK_COLUMNS.contains(&fields[field]))
        .collect::<Vec<_>>();
    others.sort_unstable_by_key(|&field| fields[field]);
    order.extend(others);

    let mut keys = StringBuilder::with_capacity(ticks.len(), ticks.len() * ticker.len());
    for _ in &ticks {
        keys.append_value(ticker);
    }
    let mut out_fields = vec![Field::new(key, DataType::Utf8, true)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(keys.finish())];
    let mut rows = Vec::with_capacity(ticks.len());
    let mut scratch = String::new();
    for field in order {
        let start = entries.partition_point(|&(id, _, _)| id < field);
        let end = entries.partition_point(|&(id, _, _)| id <= field);
        let mut field_entries = entries[start..end].iter().peekable();
        rows.clear();
        for &tick in &ticks {
            let mut row = None;
            while let Some(&(_, _, source)) = field_entries.next_if(|&&(_, at, _)| at == tick) {
                row = Some(source);
            }
            rows.push(row);
        }
        let array = bqr_field_array(&rows, strings, numbers.as_ref(), &mut scratch)?;
        out_fields.push(Field::new(fields[field], array.data_type().clone(), true));
        columns.push(array);
    }
    let schema = Schema::new_with_metadata(out_fields, metadata);
    Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
}

/// The `value_num` column of generic BQR output.
struct BqrNumbers<'a> {
    values: &'a dyn Array,
    /// Logical nulls, so `Null`-typed and dictionary columns read as null
    /// where Arrow reads them so.
    nulls: Option<NullBuffer>,
    /// Whether the values are strings, which make a field text.
    is_text: bool,
}

impl<'a> BqrNumbers<'a> {
    fn new(values: &'a dyn Array) -> Self {
        Self {
            values,
            nulls: values.logical_nulls(),
            is_text: !matches!(TextValues::new(values), TextValues::Other),
        }
    }

    fn is_valid(&self, row: usize) -> bool {
        self.nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row))
    }
}

/// Text of the non-null `value_num` cells of a UTF-8 reshaped field.
enum NumberText<'a> {
    Text(TextValues<'a>),
    Float16(&'a Float16Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Display(ArrayFormatter<'a>),
}

impl<'a> NumberText<'a> {
    /// Floats in Rust's decimal notation, strings as they are, other values
    /// as Arrow displays them.
    fn new(values: &'a dyn Array) -> Result<Self> {
        Ok(match values.data_type() {
            DataType::Float16 => Self::Float16(values.as_primitive()),
            DataType::Float32 => Self::Float32(values.as_primitive()),
            DataType::Float64 => Self::Float64(values.as_primitive()),
            _ => match TextValues::new(values) {
                TextValues::Other => {
                    Self::Display(ArrayFormatter::try_new(values, &FormatOptions::new())?)
                }
                text => Self::Text(text),
            },
        })
    }

    /// Append the text of the non-null cell at `row`.
    fn write(&self, row: usize, out: &mut String) -> Result<()> {
        let written = match self {
            Self::Text(values) => {
                out.push_str(values.get(row).unwrap_or_default());
                Ok(())
            }
            Self::Float16(values) => write!(out, "{}", values.value(row)),
            Self::Float32(values) => write!(out, "{}", values.value(row)),
            Self::Float64(values) => write!(out, "{}", values.value(row)),
            Self::Display(formatter) => return Ok(formatter.value(row).write(out)?),
        };
        written.expect("writing to a String cannot fail");
        Ok(())
    }
}

/// One reshaped field from the source row chosen for each tick, if any.
fn bqr_field_array(
    rows: &[Option<usize>],
    strings: TextValues<'_>,
    numbers: Option<&BqrNumbers<'_>>,
    scratch: &mut String,
) -> Result<ArrayRef> {
    let text = |row: usize| strings.get(row).filter(|text| !text.is_empty());
    let number = |row: usize| numbers.is_some_and(|numbers| numbers.is_valid(row));
    let (mut has_text, mut has_number) = (false, false);
    for &row in rows.iter().flatten() {
        if text(row).is_some() {
            has_text = true;
        } else {
            has_number |= number(row);
        }
    }
    if !has_text && !has_number {
        return Ok(new_null_array(&DataType::Utf8, rows.len()));
    }
    if let Some(numbers) = numbers.filter(|numbers| !has_text && !numbers.is_text) {
        let indices = rows
            .iter()
            .map(|row| row.map(|row| row as u64))
            .collect::<UInt64Array>();
        return Ok(take(numbers.values, &indices, None)?);
    }

    let number_text = match numbers {
        Some(numbers) if has_number => Some(NumberText::new(numbers.values)?),
        _ => None,
    };
    let mut builder = StringBuilder::with_capacity(rows.len(), 0);
    for &row in rows {
        let Some(row) = row else {
            builder.append_null();
            continue;
        };
        if let Some(text) = text(row) {
            builder.append_value(text);
        } else if let Some(number_text) = number_text.as_ref().filter(|_| number(row)) {
            scratch.clear();
            number_text.write(row, scratch)?;
            builder.append_value(scratch.as_str());
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use arrow_array::types::{Date32Type, UInt64Type};
    use arrow_array::{
        Date32Array, Float64Array, Int64Array, LargeStringArray, NullArray, StringArray,
        StringViewArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};

    use super::*;
    use crate::utils::{array_value_as_f64, as_string_col};

    #[test]
    fn yas_merges_caller_overrides_without_replacing_request_identity() {
        let options = RequestParams {
            service: "ignored".to_string(),
            securities: Some(vec!["ignored".to_string()]),
            fields: Some(vec!["ignored".to_string()]),
            overrides: Some(vec![
                ("YAS_BOND_PX".to_string(), "101.25".to_string()),
                ("YAS_CALC_TYPE".to_string(), "1".to_string()),
            ]),
            kwargs: Some(HashMap::from([(
                "pricingOption".to_string(),
                "price".to_string(),
            )])),
            security_overrides: Some(vec![(
                "TEST Govt".to_string(),
                vec![("YAS_BOND_PX".to_string(), "102".to_string())],
            )]),
            field_types: Some(HashMap::from([(
                "YAS_BOND_YLD".to_string(),
                "float64".to_string(),
            )])),
            validate_fields: Some(false),
            return_eids: true,
            format: Some("wide".to_string()),
            ..Default::default()
        };
        let params = yas_request(
            vec!["TEST Govt".to_string()],
            vec!["YAS_BOND_YLD".to_string()],
            Some("20240115".to_string()),
            Some(YieldType::YTM),
            Some(50.0),
            Some(4.5),
            Some(99.5),
            Some("BENCHMARK Govt".to_string()),
            &options,
        );
        assert_eq!(params.service, Service::RefData.as_str());
        assert_eq!(params.operation, Operation::ReferenceData.as_str());
        assert_eq!(params.securities.unwrap(), ["TEST Govt"]);
        assert_eq!(params.fields.unwrap(), ["YAS_BOND_YLD"]);
        let overrides = params.overrides.unwrap();
        for (key, value) in [
            ("YAS_SETTLE_DT", "20240115"),
            ("YAS_YLD_FLAG", "1"),
            ("YAS_BOND_PX", "101.25"),
            ("YAS_CALC_TYPE", "1"),
        ] {
            assert!(overrides.iter().any(|(k, v)| k == key && v == value));
        }
        assert_eq!(
            overrides
                .iter()
                .filter(|(key, _)| key == "YAS_BOND_PX")
                .count(),
            1
        );
        assert_eq!(params.kwargs, options.kwargs);
        assert_eq!(params.security_overrides, options.security_overrides);
        assert_eq!(params.field_types, options.field_types);
        assert_eq!(params.validate_fields, Some(false));
        assert!(params.return_eids);
        assert_eq!(params.format.as_deref(), Some("wide"));
    }

    #[test]
    fn preferreds_request_normalizes_ticker_and_deduplicates_fields() {
        let options = RequestParams {
            overrides: Some(vec![("currency".to_string(), "USD".to_string())]),
            kwargs: Some(HashMap::from([("mode".to_string(), "cached".to_string())])),
            ..Default::default()
        };
        let params = preferreds_request(
            "TEST",
            &["ID".to_string(), "name".to_string(), "px_last".to_string()],
            &options,
        );
        assert_eq!(params.service, Service::BqlSvc.as_str());
        assert_eq!(params.operation, Operation::BqlSendQuery.as_str());
        let elements = params.elements.unwrap();
        assert_eq!(
            elements[0],
            ("expression".to_string(),
             "get(id, name, px_last) for(filter(debt(['TEST US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Preferreds'))".to_string())
        );
        assert!(elements.contains(&("currency".to_string(), "USD".to_string())));
        assert_eq!(params.kwargs, options.kwargs);
        assert!(params.overrides.is_none());
    }

    #[test]
    fn preferreds_request_keeps_default_fields() {
        let params = preferreds_request("TEST", &[], &RequestParams::default());
        assert!(params.elements.unwrap()[0].1.starts_with("get(id, name)"));
    }

    #[test]
    fn corporate_bonds_request_keeps_currency_and_bql_options() {
        let options = RequestParams {
            elements: Some(vec![("mode".to_string(), "cached".to_string())]),
            validate_fields: Some(false),
            ..Default::default()
        };
        let params = corporate_bonds_request(
            "TEST",
            Some("USD"),
            &["ID".to_string(), "cpn".to_string()],
            &options,
        );
        assert_eq!(params.service, Service::BqlSvc.as_str());
        assert_eq!(params.operation, Operation::BqlSendQuery.as_str());
        let elements = params.elements.unwrap();
        assert_eq!(
            elements[0].1,
            "get(id, cpn) for(filter(debt(['TEST US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Corporates' AND CRNCY=='USD'))"
        );
        assert!(elements.contains(&("mode".to_string(), "cached".to_string())));
        assert_eq!(params.validate_fields, Some(false));
    }

    #[test]
    fn corporate_bonds_request_keeps_non_us_ticker_without_currency_filter() {
        let params =
            corporate_bonds_request("TEST JT Equity", None, &[], &RequestParams::default());
        assert_eq!(
            params.elements.unwrap()[0].1,
            "get(id) for(filter(debt(['TEST JT Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Corporates'))"
        );
    }

    #[test]
    fn bqr_defaults_start_to_hour_before_end_and_requests_dealer_quotes() {
        let params = bqr_request(
            "TEST Govt".to_string(),
            None,
            Some("2024-01-15 10:00".to_string()),
            None,
            true,
            &RequestParams::default(),
        )
        .unwrap();
        assert_eq!(params.service, Service::RefData.as_str());
        assert_eq!(params.operation, Operation::IntradayTick.as_str());
        assert_eq!(params.security.as_deref(), Some("TEST Govt"));
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
    fn bqr_default_window_is_one_hour_in_utc() {
        let before = Utc::now();
        let params = bqr_request(
            "TEST Govt".to_string(),
            None,
            None,
            None,
            true,
            &RequestParams::default(),
        )
        .unwrap();
        let after = Utc::now();
        let start =
            DateTime::parse_from_rfc3339(params.start_datetime.as_deref().unwrap()).unwrap();
        let end = DateTime::parse_from_rfc3339(params.end_datetime.as_deref().unwrap()).unwrap();
        assert_eq!(end.offset().local_minus_utc(), 0);
        assert_eq!(end - start, Duration::hours(1));
        assert!(end >= before && end <= after);
    }

    #[test]
    fn bqr_omitted_start_preserves_explicit_end_offset_and_fractional_seconds() {
        let params = bqr_request(
            "TEST Govt".to_string(),
            None,
            Some("2024-01-15T10:00:00.125-05:00".to_string()),
            None,
            true,
            &RequestParams::default(),
        )
        .unwrap();
        assert_eq!(
            params.start_datetime.as_deref(),
            Some("2024-01-15T09:00:00.125-05:00")
        );
        assert_eq!(
            params.end_datetime.as_deref(),
            Some("2024-01-15T10:00:00.125-05:00")
        );
    }

    #[test]
    fn bqr_invalid_end_does_not_substitute_current_time() {
        let error = bqr_request(
            "TEST Govt".to_string(),
            None,
            Some("not-a-date".to_string()),
            None,
            true,
            &RequestParams::default(),
        )
        .unwrap_err();
        assert!(matches!(error, RecipeError::InvalidArgument(_)));
    }

    #[test]
    fn bqr_request_keeps_extra_flags_timezones_and_explicit_events() {
        let options = RequestParams {
            options: Some(vec![
                ("includeConditionCodes".to_string(), "true".to_string()),
                ("includeExchangeCodes".to_string(), "true".to_string()),
                ("includeSpreadPrice".to_string(), "true".to_string()),
                ("includeYield".to_string(), "true".to_string()),
            ]),
            kwargs: Some(HashMap::from([(
                "maxDataPoints".to_string(),
                "5".to_string(),
            )])),
            request_tz: Some("America/New_York".to_string()),
            output_tz: Some("Asia/Tokyo".to_string()),
            return_eids: true,
            ..Default::default()
        };
        let params = bqr_request(
            "TEST Govt".to_string(),
            Some("2024-01-15 09:00".to_string()),
            Some("2024-01-15 10:00".to_string()),
            Some(vec!["TRADE".to_string()]),
            false,
            &options,
        )
        .unwrap();
        assert_eq!(params.event_types.unwrap(), ["TRADE"]);
        assert_eq!(params.options, options.options);
        assert_eq!(params.kwargs, options.kwargs);
        assert_eq!(params.request_tz, options.request_tz);
        assert_eq!(params.output_tz, options.output_tz);
        assert!(params.return_eids);
        assert_eq!(
            params.start_datetime.as_deref(),
            Some("2024-01-15T09:00:00")
        );
    }

    #[test]
    fn bqr_normalizes_extra_include_kwargs_in_rust() {
        let options = RequestParams {
            kwargs: Some(HashMap::from([
                ("include_condition_codes".to_string(), "true".to_string()),
                ("include_exchange_codes".to_string(), "true".to_string()),
                ("include_spread_price".to_string(), "true".to_string()),
                ("include_yield".to_string(), "true".to_string()),
                ("includeYield".to_string(), "false".to_string()),
            ])),
            ..Default::default()
        };
        let params = bqr_request(
            "TEST Govt".to_string(),
            None,
            Some("2024-01-15T10:00:00".to_string()),
            None,
            true,
            &options,
        )
        .unwrap();
        assert_eq!(
            params.kwargs.unwrap(),
            HashMap::from([
                ("includeConditionCodes".to_string(), "true".to_string()),
                ("includeExchangeCodes".to_string(), "true".to_string()),
                ("includeSpreadPrice".to_string(), "true".to_string()),
                ("includeYield".to_string(), "false".to_string()),
            ])
        );
    }

    fn typed_quotes(brokers: [Option<&str>; 2]) -> RecordBatch {
        let time =
            TimestampMicrosecondArray::from(vec![2_000_000, 1_000_000]).with_timezone("Asia/Tokyo");
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["TEST Corp", "TEST Corp"])),
            Arc::new(time),
            Arc::new(StringArray::from(vec!["ASK", "BID"])),
            Arc::new(Float64Array::from(vec![101.0, 100.0])),
            Arc::new(Int64Array::from(vec![2_000, 1_000])),
            Arc::new(StringArray::from(brokers.to_vec())),
            Arc::new(Float64Array::from(vec![21.0, 20.0])),
            Arc::new(StringArray::from(vec!["R", "S"])),
            Arc::new(StringArray::from(vec!["TEST", "TEST"])),
            Arc::new(Float64Array::from(vec![4.1, 4.2])),
        ];
        let names = [
            "ticker",
            "time",
            "type",
            "value",
            "size",
            "brokerSellCode",
            "spreadPrice",
            "conditionCodes",
            "exchangeCode",
            "yield",
        ];
        let fields = names
            .iter()
            .zip(&columns)
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                HashMap::from([("xbbg.eid_data".to_string(), "{}".to_string())]),
            )),
            columns,
        )
        .unwrap()
    }

    #[test]
    fn bqr_shapes_typed_quotes_preserving_extra_fields_types_and_metadata() {
        let batch = shape_bqr_quotes(
            typed_quotes([Some("DLRA"), Some("DLRB")]),
            "TEST Corp",
            "ticker",
            true,
        )
        .unwrap();
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "ticker",
                "time",
                "event_type",
                "price",
                "size",
                "broker_sell",
                "spread_price",
                "condition_codes",
                "exchange",
                "yield"
            ]
        );
        assert_eq!(as_string_col(&batch, "event_type").unwrap().value(0), "BID");
        assert_eq!(
            as_string_col(&batch, "broker_sell").unwrap().value(0),
            "DLRB"
        );
        assert_eq!(
            as_string_col(&batch, "condition_codes").unwrap().value(0),
            "S"
        );
        assert_eq!(
            array_value_as_f64(batch.column_by_name("price").unwrap(), 0),
            Some(100.0)
        );
        assert_eq!(
            array_value_as_f64(batch.column_by_name("spread_price").unwrap(), 0),
            Some(20.0)
        );
        assert_eq!(
            array_value_as_f64(batch.column_by_name("yield").unwrap(), 0),
            Some(4.2)
        );
        assert_eq!(
            batch.column_by_name("size").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            batch.column_by_name("time").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Tokyo".into()))
        );
        assert_eq!(
            batch
                .schema()
                .metadata()
                .get("xbbg.eid_data")
                .map(String::as_str),
            Some("{}")
        );
    }

    #[test]
    fn bqr_requires_attribution_only_for_nonempty_attributed_requests() {
        let batch = typed_quotes([None, Some("")]);
        let error = shape_bqr_quotes(batch.clone(), "TEST Corp", "ticker", true).unwrap_err();
        assert!(error.to_string().contains("without broker attribution"));
        assert_eq!(
            shape_bqr_quotes(batch.clone(), "TEST Corp", "ticker", false)
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            shape_bqr_quotes(batch.slice(0, 0), "TEST Corp", "ticker", true)
                .unwrap()
                .num_rows(),
            0
        );
        assert!(
            shape_bqr_quotes(
                typed_quotes([None, Some("DLRA")]),
                "TEST Corp",
                "ticker",
                true
            )
            .is_ok()
        );
    }

    #[test]
    fn bqr_shapes_generic_quotes_without_turning_metadata_into_quote_rows() {
        let paths = vec![
            "tickData[1].time",
            "tickData[1].type",
            "tickData[1].value",
            "tickData[1].brokerBuyCode",
            "tickData[1].spreadPrice",
            "tickData[0].time",
            "tickData[0].type",
            "tickData[0].value",
            "tickData[0].brokerBuyCode",
            "tickData.eidData[0]",
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T09:00:00"),
                Some("BID"),
                None,
                Some("DLRA"),
                None,
                Some("2024-01-15T10:00:00"),
                Some("ASK"),
                None,
                None,
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                None,
                None,
                Some(100.0),
                None,
                Some(20.0),
                None,
                None,
                Some(101.0),
                None,
                Some(7.0),
            ])),
        ];
        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("value_str", DataType::Utf8, true),
            Field::new("value_num", DataType::Float64, true),
        ]);
        let source = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        let batch = shape_bqr_quotes(source, "TEST Corp", "security", true).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema_ref().field(0).name(), "security");
        assert_eq!(
            as_string_col(&batch, "security").unwrap().value(0),
            "TEST Corp"
        );
        assert_eq!(as_string_col(&batch, "event_type").unwrap().value(0), "BID");
        assert_eq!(
            as_string_col(&batch, "broker_buy").unwrap().value(0),
            "DLRA"
        );
        assert!(as_string_col(&batch, "broker_buy").unwrap().is_null(1));
        assert_eq!(
            array_value_as_f64(batch.column_by_name("price").unwrap(), 0),
            Some(100.0)
        );
        assert_eq!(
            array_value_as_f64(batch.column_by_name("spread_price").unwrap(), 0),
            Some(20.0)
        );
        assert!(batch.column_by_name("spread_price").unwrap().is_null(1));
    }

    fn synthetic_metadata() -> HashMap<String, String> {
        HashMap::from([("xbbg.eid_data".to_string(), "{}".to_string())])
    }

    fn generic_ticks(
        rows: &[(Option<&str>, Option<&str>, Option<f64>)],
        metadata: HashMap<String, String>,
    ) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ];
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("value_str", DataType::Utf8, true),
                Field::new("value_num", DataType::Float64, true),
            ],
            metadata,
        );
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn column_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn texts<'a>(batch: &'a RecordBatch, name: &str) -> Vec<Option<&'a str>> {
        as_string_col(batch, name).unwrap().iter().collect()
    }

    fn floats(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn bqr_generic_reshape_types_fields_and_keeps_metadata() {
        let source = generic_ticks(
            &[
                (
                    Some("result.tickData[2].time"),
                    Some("2024-01-15T09:00:02"),
                    None,
                ),
                (Some("result.tickData[2].value"), Some(""), Some(101.5)),
                (Some("result.tickData[2].conditionCodes"), None, Some(7.0)),
                (
                    Some("result.tickData[0].time"),
                    Some("2024-01-15T09:00:00"),
                    None,
                ),
                (Some("result.tickData[0].type"), Some("BID"), None),
                (Some("result.tickData[0].value"), None, Some(100.25)),
                (Some("result.tickData[0].size"), None, Some(5.0)),
                (Some("result.tickData[0].brokerBuyCode"), Some("DLRA"), None),
                (Some("result.tickData[0].conditionCodes"), Some("R"), None),
                (Some("summary.count"), None, Some(2.0)),
                (None, Some("ignored"), Some(1.0)),
            ],
            synthetic_metadata(),
        );

        let reshaped = reshape_bqr_generic(&source, "SYN1 Corp", "ticker").unwrap();
        assert_eq!(
            column_names(&reshaped),
            [
                "ticker",
                "time",
                "type",
                "value",
                "size",
                "brokerBuyCode",
                "conditionCodes"
            ]
        );
        let types = reshaped
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            [
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Float64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Utf8,
            ]
        );
        assert_eq!(
            texts(&reshaped, "ticker"),
            [Some("SYN1 Corp"), Some("SYN1 Corp")]
        );
        assert_eq!(
            texts(&reshaped, "time"),
            [Some("2024-01-15T09:00:00"), Some("2024-01-15T09:00:02")]
        );
        assert_eq!(texts(&reshaped, "type"), [Some("BID"), None]);
        // An empty `value_str` falls back to `value_num`.
        assert_eq!(floats(&reshaped, "value"), [Some(100.25), Some(101.5)]);
        assert_eq!(floats(&reshaped, "size"), [Some(5.0), None]);
        assert_eq!(texts(&reshaped, "brokerBuyCode"), [Some("DLRA"), None]);
        // Mixed text and numbers stay UTF-8, numbers formatted as text.
        assert_eq!(texts(&reshaped, "conditionCodes"), [Some("R"), Some("7")]);
        assert_eq!(reshaped.schema().metadata(), &synthetic_metadata());

        let shaped = shape_bqr_quotes(source, "SYN1 Corp", "ticker", true).unwrap();
        assert_eq!(
            column_names(&shaped),
            [
                "ticker",
                "time",
                "event_type",
                "price",
                "size",
                "broker_buy",
                "condition_codes"
            ]
        );
        assert_eq!(floats(&shaped, "price"), [Some(100.25), Some(101.5)]);
        assert_eq!(shaped.schema().metadata(), &synthetic_metadata());
    }

    #[test]
    fn bqr_generic_reshape_without_ticks_is_the_empty_quote_table() {
        let no_ticks = generic_ticks(
            &[
                (Some("summary.count"), None, Some(1.0)),
                (None, Some("x"), None),
            ],
            synthetic_metadata(),
        );
        let no_paths = no_ticks.project(&[1, 2]).unwrap();
        for source in [no_ticks.clone(), no_paths, no_ticks.slice(0, 0)] {
            let reshaped = reshape_bqr_generic(&source, "SYN1 Corp", "ticker").unwrap();
            assert_eq!(
                column_names(&reshaped),
                ["ticker", "time", "type", "value", "size"]
            );
            assert_eq!(reshaped.num_rows(), 0);
            assert!(
                reshaped
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| field.data_type() == &DataType::Utf8)
            );
            assert_eq!(reshaped.schema().metadata(), &synthetic_metadata());
        }

        let shaped = shape_bqr_quotes(no_ticks, "SYN1 Corp", "ticker", true).unwrap();
        assert_eq!(
            column_names(&shaped),
            ["ticker", "time", "event_type", "price", "size"]
        );
        assert_eq!(shaped.num_rows(), 0);

        // Empty responses without columns (as mocked responses are) stay empty.
        let no_columns = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let shaped = shape_bqr_quotes(no_columns, "SYN1 Corp", "ticker", true).unwrap();
        assert_eq!((shaped.num_rows(), shaped.num_columns()), (0, 0));
    }

    #[test]
    fn bqr_reads_every_arrow_string_layout() {
        let utf8 = generic_ticks(
            &[
                (Some("tickData[0].type"), Some("BID"), None),
                (Some("tickData[0].brokerSellCode"), Some("DLRB"), None),
                (Some("tickData[0].value"), None, Some(99.5)),
            ],
            HashMap::new(),
        );
        let path = as_string_col(&utf8, "path").unwrap();
        let value_str = as_string_col(&utf8, "value_str").unwrap();
        let schema = Schema::new(vec![
            Field::new("path", DataType::LargeUtf8, true),
            Field::new("value_str", DataType::Utf8View, true),
            Field::new("value_num", DataType::Float64, true),
        ]);
        let other_layouts = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(path.iter().collect::<LargeStringArray>()),
                Arc::new(value_str.iter().collect::<StringViewArray>()),
                Arc::clone(utf8.column(2)),
            ],
        )
        .unwrap();
        assert_eq!(
            reshape_bqr_generic(&other_layouts, "SYN1 Corp", "ticker").unwrap(),
            reshape_bqr_generic(&utf8, "SYN1 Corp", "ticker").unwrap()
        );

        let view_codes = |codes: Vec<Option<&str>>| {
            let schema = Schema::new(vec![Field::new("brokerSellCode", DataType::Utf8View, true)]);
            let codes: ArrayRef = Arc::new(StringViewArray::from(codes));
            RecordBatch::try_new(Arc::new(schema), vec![codes]).unwrap()
        };
        let shape = |batch| shape_bqr_quotes(batch, "SYN1 Corp", "ticker", true);
        assert!(shape(view_codes(vec![Some(""), None])).is_err());
        assert!(shape(view_codes(vec![Some(""), Some("DLRA")])).is_ok());
    }

    #[test]
    fn bqr_generic_reshape_keeps_value_num_types_and_renders_them_beside_text() {
        let generic = |numbers: ArrayRef, rows: &[(&str, Option<&str>)]| {
            let schema = Schema::new(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("value_str", DataType::Utf8, true),
                Field::new("value_num", numbers.data_type().clone(), true),
            ]);
            let paths = StringArray::from_iter_values(rows.iter().map(|row| row.0));
            let text = rows.iter().map(|row| row.1).collect::<StringArray>();
            let columns: Vec<ArrayRef> = vec![Arc::new(paths), Arc::new(text), numbers];
            RecordBatch::try_new(Arc::new(schema), columns).unwrap()
        };

        // Unsigned counts stay exact; beside text they render losslessly.
        let counts = generic(
            Arc::new(UInt64Array::from(vec![
                Some(u64::MAX),
                Some(3),
                Some(u64::MAX),
                None,
            ])),
            &[
                ("tickData[0].count", None),
                ("tickData[1].count", None),
                ("tickData[0].note", None),
                ("tickData[1].note", Some("n/a")),
            ],
        );
        let reshaped = reshape_bqr_generic(&counts, "SYN1 Corp", "ticker").unwrap();
        let count = reshaped.column_by_name("count").unwrap();
        assert_eq!(count.as_primitive::<UInt64Type>().values(), &[u64::MAX, 3]);
        assert_eq!(
            texts(&reshaped, "note"),
            [Some("18446744073709551615"), Some("n/a")]
        );

        // Dates keep Date32, and render as Arrow displays them beside text.
        let dates = generic(
            Arc::new(Date32Array::from(vec![Some(19_000), None, Some(19_001)])),
            &[
                ("tickData[0].settle", None),
                ("tickData[1].settle", Some("TBD")),
                ("tickData[1].due", None),
            ],
        );
        let reshaped = reshape_bqr_generic(&dates, "SYN1 Corp", "ticker").unwrap();
        assert_eq!(
            texts(&reshaped, "settle"),
            [Some("2022-01-08"), Some("TBD")]
        );
        let due = reshaped.column_by_name("due").unwrap();
        assert_eq!(
            due.as_primitive::<Date32Type>(),
            &Date32Array::from(vec![None, Some(19_001)])
        );

        // A `Null`-typed value_num holds no values.
        let nulls = generic(
            Arc::new(NullArray::new(1)),
            &[("tickData[0].time", Some(""))],
        );
        let reshaped = reshape_bqr_generic(&nulls, "SYN1 Corp", "ticker").unwrap();
        let time = reshaped.column_by_name("time").unwrap();
        assert_eq!((time.data_type(), time.null_count()), (&DataType::Utf8, 1));

        // value_str must hold strings, so its values are never dropped.
        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("value_str", DataType::Int64, true),
        ]);
        let numeric_text = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["tickData[0].size"])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap();
        assert!(matches!(
            reshape_bqr_generic(&numeric_text, "SYN1 Corp", "ticker"),
            Err(RecipeError::InvalidArgument(message)) if message.contains("value_str")
        ));
    }

    #[test]
    fn bqr_missing_attribution_keeps_the_python_message() {
        match shape_bqr_quotes(typed_quotes([None, Some("")]), "TEST Corp", "ticker", true) {
            Err(RecipeError::Other(message)) => assert_eq!(
                message,
                "BQR returned quote rows without broker attribution. Use a fixed-income ticker \
                 with a dealer quote pricing source such as '@MSG1 Corp', or pass \
                 include_broker_codes=False if raw quote ticks without dealer codes are \
                 intentional."
            ),
            other => panic!("expected the broker-attribution failure, got {other:?}"),
        }
    }

    #[test]
    fn bqr_sorts_quotes_by_time_with_nulls_last() {
        let time = TimestampMicrosecondArray::from(vec![Some(2_000_000), None, Some(1_000_000)]);
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("time", time.data_type().clone(), true),
                Field::new("brokerBuyCode", DataType::Utf8, true),
            ],
            synthetic_metadata(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(time),
                Arc::new(StringArray::from(vec!["DLRB", "DLRN", "DLRA"])),
            ],
        )
        .unwrap();

        let shaped = shape_bqr_quotes(batch, "SYN1 Corp", "ticker", true).unwrap();

        assert_eq!(
            texts(&shaped, "broker_buy"),
            [Some("DLRA"), Some("DLRB"), Some("DLRN")]
        );
        assert!(shaped.column_by_name("time").unwrap().is_null(2));
        assert_eq!(shaped.schema().metadata(), &synthetic_metadata());
    }
}
