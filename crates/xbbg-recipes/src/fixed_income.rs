//! Fixed income recipe functions.
//!
//! High-level recipes for Bloomberg fixed income data queries.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray};
use arrow_ord::sort::{sort_to_indices, SortOptions};
use arrow_schema::{Field, Schema};
use arrow_select::take::take_record_batch;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::bql::{build_corporate_bonds_query, build_preferreds_query};
use xbbg_ext::transforms::fixed_income::{build_yas_overrides, YieldType};
use xbbg_ext::utils::date::parse_date;

use crate::error::{RecipeError, Result};
use crate::utils::{apply_request_options, array_value_as_f64, as_string_col};

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
/// Time-sorted Arrow quote rows, retaining extra include fields and timestamp
/// timezones. Bloomberg names are normalized to `event_type`, `price`,
/// `broker_buy`, `broker_sell`, `spread_price`, `condition_codes`, and `exchange`.
/// Nonempty results without broker attribution fail when it is required.
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
    shape_bqr_quotes(batch, &ticker, include_broker_codes)
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

fn shape_bqr_quotes(
    mut batch: RecordBatch,
    ticker: &str,
    enforce_broker_codes: bool,
) -> Result<RecordBatch> {
    if batch.column_by_name("path").is_some() {
        batch = reshape_bqr_generic(&batch, ticker)?;
    }
    if enforce_broker_codes && batch.num_rows() > 0 && !bqr_has_broker_codes(&batch) {
        return Err(RecipeError::Other(
            "BQR returned quote rows without broker attribution. Use a fixed-income ticker \
             with a dealer quote pricing source such as '@MSG1 Corp', or pass \
             include_broker_codes=False if raw quote ticks without dealer codes are intentional."
                .to_string(),
        ));
    }
    if batch.num_rows() > 1 {
        if let Some(time) = batch.column_by_name("time") {
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
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        batch.columns().to_vec(),
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
    .any(|column| {
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            values.iter().flatten().any(|value| !value.is_empty())
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            values.iter().flatten().any(|value| !value.is_empty())
        } else {
            column.null_count() < column.len()
        }
    })
}

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

fn reshape_bqr_generic(batch: &RecordBatch, ticker: &str) -> Result<RecordBatch> {
    let paths = as_string_col(batch, "path")?;
    let strings = batch
        .column_by_name("value_str")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>());
    let numbers = batch.column_by_name("value_num");
    let text = |row| {
        strings
            .filter(|array| array.is_valid(row))
            .map(|array| array.value(row))
            .filter(|value| !value.is_empty())
    };
    let number = |row| numbers.and_then(|array| array_value_as_f64(array, row));
    let mut fields = BTreeSet::new();
    let mut records: BTreeMap<usize, HashMap<&str, usize>> = BTreeMap::new();
    for (row, path) in paths.iter().enumerate() {
        let Some((index, field)) = path.and_then(bqr_path) else {
            continue;
        };
        fields.insert(field);
        records.entry(index).or_default().insert(field, row);
    }
    let priority = ["ticker", "time", "type", "value", "size"];
    let mut names = vec!["ticker"];
    names.extend(
        priority[1..]
            .iter()
            .copied()
            .filter(|name| fields.contains(name)),
    );
    names.extend(fields.into_iter().filter(|name| !priority.contains(name)));
    if records.is_empty() {
        names = priority.to_vec();
    }
    let mut out_fields = Vec::with_capacity(names.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(names.len());
    for name in names {
        let array: ArrayRef = if name == "ticker" {
            let mut builder =
                StringBuilder::with_capacity(records.len(), records.len() * ticker.len());
            for _ in 0..records.len() {
                builder.append_value(ticker);
            }
            Arc::new(builder.finish())
        } else {
            let source_rows = records.values().map(|record| record.get(name).copied());
            let has_text = source_rows.clone().flatten().any(|row| text(row).is_some());
            let has_number = source_rows
                .clone()
                .flatten()
                .any(|row| number(row).is_some());
            if !has_text && has_number {
                let mut builder = Float64Builder::with_capacity(records.len());
                for row in source_rows {
                    builder.append_option(row.and_then(number));
                }
                Arc::new(builder.finish())
            } else {
                let mut builder = StringBuilder::new();
                for row in source_rows {
                    if let Some(value) = row.and_then(text) {
                        builder.append_value(value);
                    } else if let Some(value) = row.and_then(number) {
                        builder.append_value(value.to_string());
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
        };
        out_fields.push(Field::new(name, array.data_type().clone(), true));
        columns.push(array);
    }
    let schema = Schema::new_with_metadata(out_fields, batch.schema().metadata().clone());
    Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit};

    use super::*;

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
        let error = shape_bqr_quotes(batch.clone(), "TEST Corp", true).unwrap_err();
        assert!(error.to_string().contains("without broker attribution"));
        assert_eq!(
            shape_bqr_quotes(batch.clone(), "TEST Corp", false)
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            shape_bqr_quotes(batch.slice(0, 0), "TEST Corp", true)
                .unwrap()
                .num_rows(),
            0
        );
        assert!(shape_bqr_quotes(typed_quotes([None, Some("DLRA")]), "TEST Corp", true).is_ok());
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
        let batch = shape_bqr_quotes(source, "TEST Corp", true).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            as_string_col(&batch, "ticker").unwrap().value(0),
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
}
