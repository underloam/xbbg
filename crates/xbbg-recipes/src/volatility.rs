//! Volatility surface recipes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{Date32Builder, Float64Builder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};

use crate::error::{RecipeError, Result};
use crate::utils::{
    array_value_as_date, array_value_as_f64, as_string_col, naive_to_date32, parse_f64_like,
};

const DEFAULT_PRESET: &str = "MONEYNESS_30D";
const DEFAULT_DIVIDEND_YIELD_FIELD: &str = "EQY_DVD_YLD_12M";
const SPOT_FIELD: &str = "PX_LAST";

type PointSpec<'a> = (&'a str, &'a str, &'a str, f64);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VolFieldSpec {
    field: String,
    metric: String,
    tenor: Option<String>,
    point_type: Option<String>,
    point: Option<f64>,
    years: Option<f64>,
    scale_decimal: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SurfaceRow {
    ticker: String,
    date: NaiveDate,
    metric: String,
    tenor: Option<String>,
    point_type: Option<String>,
    point: Option<f64>,
    field: String,
    value: f64,
}

/// Build a tidy historical implied-volatility surface.
///
/// `as_decimal` (default true) divides percent-quoted Bloomberg fields
/// (implied vols, dividend yields) by 100 unconditionally — Bloomberg quotes
/// these in percent regardless of magnitude, so a 0.5% yield arrives as `0.5`
/// and must still be scaled. `risk_free_rate` is a user input: values with
/// `abs() > 1` are treated as percent, smaller values as decimal.
#[allow(clippy::too_many_arguments)]
pub async fn recipe_vol_surface(
    engine: &Engine,
    tickers: Vec<String>,
    start_date: String,
    end_date: String,
    presets: Option<Vec<String>>,
    field_specs: Option<Vec<String>>,
    as_decimal: Option<bool>,
    include_derived: Option<bool>,
    risk_free_rate: Option<f64>,
    dividend_yield_field: Option<String>,
) -> Result<RecordBatch> {
    let as_decimal = as_decimal.unwrap_or(true);
    let include_derived = include_derived.unwrap_or(false);
    let mut specs = expand_surface_specs(presets, field_specs)?;
    if specs.is_empty() {
        specs = expand_surface_specs(Some(vec![DEFAULT_PRESET.to_string()]), None)?;
    }

    let dividend_yield_field =
        dividend_yield_field.unwrap_or_else(|| DEFAULT_DIVIDEND_YIELD_FIELD.to_string());
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    for spec in &specs {
        if seen.insert(spec.field.to_ascii_uppercase()) {
            fields.push(spec.field.clone());
        }
    }
    if include_derived {
        for field in [SPOT_FIELD, dividend_yield_field.as_str()] {
            if seen.insert(field.to_ascii_uppercase()) {
                fields.push(field.to_string());
            }
        }
    }

    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(tickers),
        fields: Some(fields),
        start_date: Some(start_date),
        end_date: Some(end_date),
        ..Default::default()
    };
    let batch = engine.request(params).await?;
    let rows = build_vol_surface_rows(
        &batch,
        &specs,
        as_decimal,
        include_derived,
        risk_free_rate,
        &dividend_yield_field,
    )?;
    build_surface_batch(&rows)
}

pub(crate) fn expand_surface_specs(
    presets: Option<Vec<String>>,
    field_specs: Option<Vec<String>>,
) -> Result<Vec<VolFieldSpec>> {
    let mut specs = Vec::new();

    if let Some(presets) = presets {
        for preset in presets {
            specs.extend(expand_preset(&preset)?);
        }
    }

    if let Some(raw_specs) = field_specs {
        for raw in raw_specs {
            specs.push(parse_field_spec(&raw));
        }
    }

    let mut seen = HashSet::new();
    specs.retain(|spec| seen.insert(spec.field.to_ascii_uppercase()));
    Ok(specs)
}

fn expand_preset(preset: &str) -> Result<Vec<VolFieldSpec>> {
    let normalized = preset.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "DELTA_1M_2M" => Ok([
            ("1MTH_PUT_IMPVOL_25DELTA_DF", "1M", "delta_put", -25.0),
            ("1MTH_CALL_IMPVOL_25DELTA_DF", "1M", "delta_call", 25.0),
            ("2MTH_PUT_IMPVOL_25DELTA_DF", "2M", "delta_put", -25.0),
            ("2MTH_CALL_IMPVOL_25DELTA_DF", "2M", "delta_call", 25.0),
        ]
        .into_iter()
        .map(delta_spec)
        .collect()),
        "MONEYNESS_30D" => Ok(moneyness_specs("30DAY", "30D", 30.0 / 365.25)),
        "MONEYNESS_60D" => Ok(moneyness_specs("60DAY", "60D", 60.0 / 365.25)),
        "MONEYNESS_3M" => Ok(moneyness_specs("3MTH", "3M", 0.25)),
        "MONEYNESS_6M" => Ok(moneyness_specs("6MTH", "6M", 0.5)),
        "MONEYNESS_12M" => Ok(moneyness_specs("12MTH", "12M", 1.0)),
        _ => Err(RecipeError::InvalidArgument(format!(
            "unsupported vol surface preset '{preset}'"
        ))),
    }
}

fn delta_spec((field, tenor, point_type, point): PointSpec<'_>) -> VolFieldSpec {
    VolFieldSpec {
        field: field.to_string(),
        metric: "implied_volatility".to_string(),
        tenor: Some(tenor.to_string()),
        point_type: Some(point_type.to_string()),
        point: Some(point),
        years: tenor_to_years(tenor),
        scale_decimal: true,
    }
}

fn moneyness_specs(prefix: &str, tenor: &str, years: f64) -> Vec<VolFieldSpec> {
    [80.0, 90.0, 95.0, 100.0, 105.0, 110.0, 120.0]
        .into_iter()
        .map(|point| VolFieldSpec {
            field: format!("{prefix}_IMPVOL_{point:.1}%MNY_DF"),
            metric: "implied_volatility".to_string(),
            tenor: Some(tenor.to_string()),
            point_type: Some("moneyness".to_string()),
            point: Some(point),
            years: Some(years),
            scale_decimal: true,
        })
        .collect()
}

fn parse_field_spec(raw: &str) -> VolFieldSpec {
    let parts = raw.split('|').map(str::trim).collect::<Vec<_>>();
    let field = parts.first().copied().unwrap_or_default().to_string();
    let metric = parts
        .get(1)
        .filter(|value| !value.is_empty())
        .map(|value| (*value).to_string())
        .unwrap_or_else(|| infer_metric(&field));
    let tenor = parts
        .get(2)
        .filter(|value| !value.is_empty())
        .map(|value| (*value).to_string())
        .or_else(|| infer_tenor(&field));
    let point_type = parts
        .get(3)
        .filter(|value| !value.is_empty())
        .map(|value| (*value).to_string())
        .or_else(|| infer_point_type(&field));
    let point = parts
        .get(4)
        .and_then(|value| parse_f64_like(value))
        .or_else(|| infer_point(&field));
    let years = tenor.as_deref().and_then(tenor_to_years);
    let scale_decimal = matches!(
        metric.as_str(),
        "implied_volatility" | "risk_free_rate" | "dividend_yield"
    );

    VolFieldSpec {
        field,
        metric,
        tenor,
        point_type,
        point,
        years,
        scale_decimal,
    }
}

fn infer_metric(field: &str) -> String {
    let upper = field.to_ascii_uppercase();
    if upper.contains("IMPVOL") || upper.contains("VOL") {
        "implied_volatility".to_string()
    } else if upper.contains("YLD") || upper.contains("RATE") {
        "rate".to_string()
    } else {
        "value".to_string()
    }
}

fn infer_tenor(field: &str) -> Option<String> {
    let upper = field.to_ascii_uppercase();
    for (prefix, tenor) in [
        ("30DAY", "30D"),
        ("60DAY", "60D"),
        ("1MTH", "1M"),
        ("2MTH", "2M"),
        ("3MTH", "3M"),
        ("6MTH", "6M"),
        ("12MTH", "12M"),
    ] {
        if upper.contains(prefix) {
            return Some(tenor.to_string());
        }
    }
    None
}

fn infer_point_type(field: &str) -> Option<String> {
    let upper = field.to_ascii_uppercase();
    if upper.contains("%MNY") {
        Some("moneyness".to_string())
    } else if upper.contains("DELTA") {
        Some("delta".to_string())
    } else {
        None
    }
}

fn infer_point(field: &str) -> Option<f64> {
    let upper = field.to_ascii_uppercase();
    if let Some(idx) = upper.find("%MNY") {
        let before = &upper[..idx];
        let start = before
            .rfind(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
            .map(|idx| idx + 1)
            .unwrap_or(0);
        return parse_f64_like(&before[start..]);
    }
    if upper.contains("25DELTA") {
        if upper.contains("PUT") {
            Some(-25.0)
        } else {
            Some(25.0)
        }
    } else {
        None
    }
}

fn tenor_to_years(tenor: &str) -> Option<f64> {
    let upper = tenor.trim().to_ascii_uppercase();
    if let Some(days) = upper.strip_suffix('D') {
        return parse_f64_like(days).map(|value| value / 365.25);
    }
    if let Some(months) = upper.strip_suffix('M') {
        return parse_f64_like(months).map(|value| value / 12.0);
    }
    if let Some(years) = upper.strip_suffix('Y') {
        return parse_f64_like(years);
    }
    None
}

pub(crate) fn build_vol_surface_rows(
    batch: &RecordBatch,
    specs: &[VolFieldSpec],
    as_decimal: bool,
    include_derived: bool,
    risk_free_rate: Option<f64>,
    dividend_yield_field: &str,
) -> Result<Vec<SurfaceRow>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;

    let spec_by_field = specs
        .iter()
        .map(|spec| (spec.field.to_ascii_uppercase(), spec))
        .collect::<HashMap<_, _>>();
    let mut rows = Vec::new();
    let mut spot: HashMap<(String, NaiveDate), f64> = HashMap::new();
    let mut dividend_yield: HashMap<(String, NaiveDate), f64> = HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        let Some(date) = array_value_as_date(date_col, row) else {
            continue;
        };
        let field = field_col.value(row);
        let field_key = field.to_ascii_uppercase();
        let Some(raw_value) = array_value_as_f64(value_col, row) else {
            continue;
        };
        let ticker = ticker_col.value(row).to_string();

        if include_derived && field_key == SPOT_FIELD {
            spot.insert((ticker, date), raw_value);
            continue;
        }
        if include_derived && field.eq_ignore_ascii_case(dividend_yield_field) {
            // Bloomberg quotes yields in percent regardless of magnitude:
            // 0.5% arrives as 0.5 and still needs scaling. A magnitude
            // heuristic here turned sub-1% yields into ~100x errors.
            let value = if as_decimal {
                raw_value / 100.0
            } else {
                raw_value
            };
            dividend_yield.insert((ticker, date), value);
            continue;
        }

        let Some(spec) = spec_by_field.get(&field_key) else {
            continue;
        };
        let value = if as_decimal && spec.scale_decimal {
            raw_value / 100.0
        } else {
            raw_value
        };
        rows.push(SurfaceRow {
            ticker,
            date,
            metric: spec.metric.clone(),
            tenor: spec.tenor.clone(),
            point_type: spec.point_type.clone(),
            point: spec.point,
            field: spec.field.clone(),
            value,
        });
    }

    if include_derived {
        append_derived_rows(
            &mut rows,
            specs,
            spot,
            dividend_yield,
            normalize_rate(risk_free_rate),
            dividend_yield_field,
        );
    }

    rows.sort_by(|left, right| {
        left.ticker
            .cmp(&right.ticker)
            .then(left.date.cmp(&right.date))
            .then(left.metric.cmp(&right.metric))
            .then(left.tenor.cmp(&right.tenor))
            .then(left.point_type.cmp(&right.point_type))
            .then(
                left.point
                    .partial_cmp(&right.point)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
    });
    Ok(rows)
}

/// Normalize the user-supplied risk-free rate.
///
/// Unlike Bloomberg fields (always percent-quoted), this is a caller-provided
/// number with an explicitly documented dual convention: `abs() > 1` is
/// percent (5.0 -> 0.05), otherwise already decimal (0.005 stays 0.5%).
fn normalize_rate(rate: Option<f64>) -> Option<f64> {
    rate.map(|value| {
        if value.abs() > 1.0 {
            value / 100.0
        } else {
            value
        }
    })
}

fn append_derived_rows(
    rows: &mut Vec<SurfaceRow>,
    specs: &[VolFieldSpec],
    spot: HashMap<(String, NaiveDate), f64>,
    dividend_yield: HashMap<(String, NaiveDate), f64>,
    risk_free_rate: Option<f64>,
    dividend_yield_field: &str,
) {
    let tenors = specs
        .iter()
        .filter_map(|spec| Some((spec.tenor.clone()?, spec.years?)))
        .collect::<Vec<_>>();

    for ((ticker, date), spot_value) in spot {
        rows.push(SurfaceRow {
            ticker: ticker.clone(),
            date,
            metric: "spot".to_string(),
            tenor: None,
            point_type: None,
            point: None,
            field: SPOT_FIELD.to_string(),
            value: spot_value,
        });

        let q = dividend_yield.get(&(ticker.clone(), date)).copied();
        if let Some(dividend_value) = q {
            rows.push(SurfaceRow {
                ticker: ticker.clone(),
                date,
                metric: "dividend_yield".to_string(),
                tenor: None,
                point_type: None,
                point: None,
                field: dividend_yield_field.to_string(),
                value: dividend_value,
            });
        }

        let Some(r) = risk_free_rate else {
            continue;
        };
        rows.push(SurfaceRow {
            ticker: ticker.clone(),
            date,
            metric: "risk_free_rate".to_string(),
            tenor: None,
            point_type: None,
            point: None,
            field: "risk_free_rate".to_string(),
            value: r,
        });

        let dividend_rate = q.unwrap_or(0.0);
        for (tenor, years) in &tenors {
            if *years <= 0.0 || !years.is_finite() {
                continue;
            }
            let discount = (-r * *years).exp();
            let forward = spot_value * ((r - dividend_rate) * *years).exp();
            rows.push(SurfaceRow {
                ticker: ticker.clone(),
                date,
                metric: "forward".to_string(),
                tenor: Some(tenor.clone()),
                point_type: None,
                point: None,
                field: "derived_forward".to_string(),
                value: forward,
            });
            rows.push(SurfaceRow {
                ticker: ticker.clone(),
                date,
                metric: "discount_factor".to_string(),
                tenor: Some(tenor.clone()),
                point_type: None,
                point: None,
                field: "derived_discount_factor".to_string(),
                value: discount,
            });
        }
    }
}

fn build_surface_batch(rows: &[SurfaceRow]) -> Result<RecordBatch> {
    let mut ticker = StringBuilder::new();
    let mut date = Date32Builder::new();
    let mut metric = StringBuilder::new();
    let mut tenor = StringBuilder::new();
    let mut point_type = StringBuilder::new();
    let mut point = Float64Builder::new();
    let mut field = StringBuilder::new();
    let mut value = Float64Builder::new();

    for row in rows {
        ticker.append_value(&row.ticker);
        date.append_value(naive_to_date32(row.date));
        metric.append_value(&row.metric);
        tenor.append_option(row.tenor.as_ref());
        point_type.append_option(row.point_type.as_ref());
        point.append_option(row.point);
        field.append_value(&row.field);
        value.append_value(row.value);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("tenor", DataType::Utf8, true),
        Field::new("point_type", DataType::Utf8, true),
        Field::new("point", DataType::Float64, true),
        Field::new("field", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ticker.finish()),
            Arc::new(date.finish()),
            Arc::new(metric.finish()),
            Arc::new(tenor.finish()),
            Arc::new(point_type.finish()),
            Arc::new(point.finish()),
            Arc::new(field.finish()),
            Arc::new(value.finish()),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, StringArray};

    #[test]
    fn preset_expansion_is_tidy_metadata() {
        let specs = expand_surface_specs(Some(vec!["MONEYNESS_30D".to_string()]), None).unwrap();
        assert_eq!(specs.len(), 7);
        assert_eq!(specs[0].metric, "implied_volatility");
        assert_eq!(specs[0].tenor.as_deref(), Some("30D"));
        assert_eq!(specs[0].point_type.as_deref(), Some("moneyness"));
    }

    #[test]
    fn raw_encoded_field_spec_overrides_metadata() {
        let spec = parse_field_spec("CUSTOM_FIELD|custom_metric|9M|custom_point|42");
        assert_eq!(spec.field, "CUSTOM_FIELD");
        assert_eq!(spec.metric, "custom_metric");
        assert_eq!(spec.tenor.as_deref(), Some("9M"));
        assert_eq!(spec.point, Some(42.0));
    }

    #[test]
    fn builds_decimal_vol_rows_and_derived_rows() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let date32 = naive_to_date32(date);
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "SPX Index",
                    "SPX Index",
                    "SPX Index",
                ])),
                Arc::new(Date32Array::from(vec![date32, date32, date32])),
                Arc::new(StringArray::from(vec![
                    "30DAY_IMPVOL_100.0%MNY_DF",
                    "PX_LAST",
                    "EQY_DVD_YLD_12M",
                ])),
                Arc::new(StringArray::from(vec!["20", "5000", "1.5"])),
            ],
        )
        .unwrap();
        let specs = vec![parse_field_spec(
            "30DAY_IMPVOL_100.0%MNY_DF|implied_volatility|30D|moneyness|100",
        )];
        let rows = build_vol_surface_rows(&batch, &specs, true, true, Some(5.0), "EQY_DVD_YLD_12M")
            .unwrap();
        assert!(rows
            .iter()
            .any(|row| row.metric == "implied_volatility" && row.value == 0.20));
        assert!(rows.iter().any(|row| row.metric == "forward"));
        assert!(rows.iter().any(|row| row.metric == "discount_factor"));
    }

    #[test]
    fn sub_one_percent_dividend_yield_still_scales_to_decimal() {
        // Regression: a 0.5% yield arrives from Bloomberg as raw 0.5; the old
        // magnitude heuristic (`abs() > 1.0`) skipped scaling and fed 50% into
        // the forward calculation.
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let date32 = naive_to_date32(date);
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["SPX Index", "SPX Index"])),
                Arc::new(Date32Array::from(vec![date32, date32])),
                Arc::new(StringArray::from(vec!["PX_LAST", "EQY_DVD_YLD_12M"])),
                Arc::new(StringArray::from(vec!["5000", "0.5"])),
            ],
        )
        .unwrap();
        let specs = vec![parse_field_spec(
            "30DAY_IMPVOL_100.0%MNY_DF|implied_volatility|30D|moneyness|100",
        )];
        let rows = build_vol_surface_rows(&batch, &specs, true, true, Some(5.0), "EQY_DVD_YLD_12M")
            .unwrap();
        let yield_row = rows
            .iter()
            .find(|row| row.metric == "dividend_yield")
            .expect("dividend_yield row");
        assert!((yield_row.value - 0.005).abs() < 1e-12);
    }
}
