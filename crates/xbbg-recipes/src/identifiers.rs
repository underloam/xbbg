//! Identifier resolution recipes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};

use crate::error::{RecipeError, Result};
use crate::utils::{array_value_as_string, as_string_col, clean_bloomberg_text};

const EQUITY_RESOLVE_FIELDS: &[&str] = &["PARSEKYABLE_DES", "EQY_PRIM_SECURITY_COMP_EXCH"];
const BOND_ISSUER_FIELDS: &[&str] = &[
    "PARSEKYABLE_DES",
    "ISSUER_PARENT_EQY_TICKER",
    "ISSUER_PARENT_EQY_EXCH_CODE",
    "ULT_PARENT_TICKER_EXCHANGE",
    "EQY_PRIM_SECURITY_TICKER",
    "EQY_PRIM_SECURITY_COMP_EXCH",
    "ID_ISIN",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IsinResolution {
    input_order: i32,
    input_isin: String,
    lookup_ticker: String,
    parsekyable_des: Option<String>,
    primary_exchange: Option<String>,
    resolved_ticker: Option<String>,
    status: String,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IssuerIsinResolution {
    input_order: i32,
    input_isin: String,
    lookup_ticker: String,
    bond_ticker: Option<String>,
    parent_ticker: Option<String>,
    issuer_equity_isin: Option<String>,
    status: String,
    error: Option<String>,
}

/// Resolve equity ISINs using Bloomberg's `/ISIN/<id>` lookup namespace.
pub async fn recipe_resolve_isins(engine: &Engine, isins: Vec<String>) -> Result<RecordBatch> {
    let lookups = isins
        .iter()
        .map(|isin| isin_lookup_ticker(isin))
        .collect::<Vec<_>>();
    let fields = EQUITY_RESOLVE_FIELDS
        .iter()
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(lookups.clone()),
        fields: Some(fields),
        ..Default::default()
    };
    let batch = engine.request(params).await?;
    let values = refdata_value_map(&batch)?;
    let rows = build_resolve_isin_rows(&isins, &lookups, &values);
    build_resolve_isins_batch(&rows)
}

/// Resolve bond ISINs to parent equity tickers and issuer equity ISINs.
pub async fn recipe_issuer_isins(engine: &Engine, bond_isins: Vec<String>) -> Result<RecordBatch> {
    let lookups = bond_isins
        .iter()
        .map(|isin| isin_lookup_ticker(isin))
        .collect::<Vec<_>>();
    let fields = BOND_ISSUER_FIELDS
        .iter()
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(lookups.clone()),
        fields: Some(fields),
        ..Default::default()
    };
    let bond_batch = engine.request(params).await?;
    let bond_values = refdata_value_map(&bond_batch)?;

    let mut rows = build_issuer_rows_without_isin(&bond_isins, &lookups, &bond_values);
    let parent_tickers = rows
        .iter()
        .filter_map(|row| row.parent_ticker.clone())
        .collect::<Vec<_>>();

    if !parent_tickers.is_empty() {
        let params = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            securities: Some(parent_tickers),
            fields: Some(vec!["ID_ISIN".to_string()]),
            ..Default::default()
        };
        let parent_batch = engine.request(params).await?;
        let parent_values = refdata_value_map(&parent_batch)?;
        for row in &mut rows {
            let Some(parent_ticker) = row.parent_ticker.as_ref() else {
                continue;
            };
            row.issuer_equity_isin = parent_values
                .get(parent_ticker)
                .and_then(|fields| fields.get("ID_ISIN"))
                .and_then(|value| clean_bloomberg_text(value));
            if row.issuer_equity_isin.is_some() {
                row.status = "resolved".to_string();
                row.error = None;
            } else {
                row.status = "unresolved".to_string();
                row.error = Some("issuer equity ISIN was not returned".to_string());
            }
        }
    }

    build_issuer_isins_batch(&rows)
}

pub(crate) fn isin_lookup_ticker(isin: &str) -> String {
    format!("/ISIN/{}", isin.trim())
}

pub(crate) fn build_equity_ticker(parsed: Option<&str>, exchange: Option<&str>) -> Option<String> {
    let parsed = clean_bloomberg_text(parsed?)?;
    if non_equity_sector(&parsed).is_some() {
        return None;
    }
    if parsed.split_whitespace().any(|token| token == "Equity") {
        return Some(parsed);
    }

    let token_count = parsed.split_whitespace().count();
    if token_count == 0 {
        return None;
    }
    if token_count == 1 {
        let exchange = exchange.and_then(clean_bloomberg_text)?;
        return Some(format!("{parsed} {exchange} Equity"));
    }

    Some(format!("{parsed} Equity"))
}

fn non_equity_sector(parsed: &str) -> Option<&str> {
    let sector = parsed.split_whitespace().last()?;
    [
        "Pfd", "Corp", "Govt", "Comdty", "Index", "Curncy", "Mtge", "Muni", "M-Mkt",
    ]
    .iter()
    .any(|key| sector.eq_ignore_ascii_case(key))
    .then_some(sector)
}

pub(crate) fn build_resolve_isin_rows(
    isins: &[String],
    lookups: &[String],
    values: &HashMap<String, HashMap<String, String>>,
) -> Vec<IsinResolution> {
    isins
        .iter()
        .zip(lookups.iter())
        .enumerate()
        .map(|(idx, (isin, lookup))| {
            let fields = values.get(lookup);
            let parsed = fields.and_then(|map| map.get("PARSEKYABLE_DES").map(String::as_str));
            let exchange =
                fields.and_then(|map| map.get("EQY_PRIM_SECURITY_COMP_EXCH").map(String::as_str));
            let resolved_ticker = build_equity_ticker(parsed, exchange);
            let (status, error) = if let Some(sector) = parsed.and_then(non_equity_sector) {
                (
                    "unresolved".to_string(),
                    Some(format!("market sector '{sector}' is not Equity")),
                )
            } else if resolved_ticker.is_some() {
                ("resolved".to_string(), None)
            } else {
                (
                    "unresolved".to_string(),
                    Some("Bloomberg did not return a parseable equity ticker".to_string()),
                )
            };
            IsinResolution {
                input_order: idx as i32,
                input_isin: isin.clone(),
                lookup_ticker: lookup.clone(),
                parsekyable_des: parsed.and_then(clean_bloomberg_text),
                primary_exchange: exchange.and_then(clean_bloomberg_text),
                resolved_ticker,
                status,
                error,
            }
        })
        .collect()
}

pub(crate) fn build_issuer_rows_without_isin(
    isins: &[String],
    lookups: &[String],
    values: &HashMap<String, HashMap<String, String>>,
) -> Vec<IssuerIsinResolution> {
    isins
        .iter()
        .zip(lookups.iter())
        .enumerate()
        .map(|(idx, (isin, lookup))| {
            let fields = values.get(lookup);
            let bond_ticker = first_clean_field(fields, &["PARSEKYABLE_DES"]);
            let parent_root = first_clean_field(
                fields,
                &[
                    "ISSUER_PARENT_EQY_TICKER",
                    "ULT_PARENT_TICKER_EXCHANGE",
                    "EQY_PRIM_SECURITY_TICKER",
                ],
            );
            let exchange = first_clean_field(
                fields,
                &["ISSUER_PARENT_EQY_EXCH_CODE", "EQY_PRIM_SECURITY_COMP_EXCH"],
            );
            let parent_ticker = build_equity_ticker(parent_root.as_deref(), exchange.as_deref());
            let (status, error) = if parent_ticker.is_some() {
                ("pending".to_string(), None)
            } else {
                (
                    "unresolved".to_string(),
                    Some("Bloomberg did not return a parent equity ticker".to_string()),
                )
            };
            IssuerIsinResolution {
                input_order: idx as i32,
                input_isin: isin.clone(),
                lookup_ticker: lookup.clone(),
                bond_ticker,
                parent_ticker,
                issuer_equity_isin: None,
                status,
                error,
            }
        })
        .collect()
}

fn first_clean_field(fields: Option<&HashMap<String, String>>, names: &[&str]) -> Option<String> {
    fields.and_then(|map| {
        names
            .iter()
            .find_map(|name| map.get(*name).and_then(|value| clean_bloomberg_text(value)))
    })
}

pub(crate) fn refdata_value_map(
    batch: &RecordBatch,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let mut values: HashMap<String, HashMap<String, String>> = HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        let Some(raw_value) = array_value_as_string(value_col, row) else {
            continue;
        };
        // These are Bloomberg missing-value markers. Literal "nan"/"null" can
        // be identifiers; callers clean their own optional numeric/text fields.
        let value = raw_value.trim();
        if value.is_empty()
            || ["N/A", "N.A.", "#N/A"]
                .iter()
                .any(|missing| value.eq_ignore_ascii_case(missing))
        {
            continue;
        }
        let value = if value.len() == raw_value.len() {
            raw_value
        } else {
            value.to_string()
        };
        values
            .entry(ticker_col.value(row).to_string())
            .or_default()
            .insert(field_col.value(row).to_ascii_uppercase(), value);
    }

    Ok(values)
}

fn append_opt(builder: &mut StringBuilder, value: Option<&String>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn build_resolve_isins_batch(rows: &[IsinResolution]) -> Result<RecordBatch> {
    let mut order = Int32Builder::new();
    let mut input = StringBuilder::new();
    let mut lookup = StringBuilder::new();
    let mut parsed = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut resolved = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut error = StringBuilder::new();

    for row in rows {
        order.append_value(row.input_order);
        input.append_value(&row.input_isin);
        lookup.append_value(&row.lookup_ticker);
        append_opt(&mut parsed, row.parsekyable_des.as_ref());
        append_opt(&mut exchange, row.primary_exchange.as_ref());
        append_opt(&mut resolved, row.resolved_ticker.as_ref());
        status.append_value(&row.status);
        append_opt(&mut error, row.error.as_ref());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("input_isin", DataType::Utf8, false),
        Field::new("lookup_ticker", DataType::Utf8, false),
        Field::new("parsekyable_des", DataType::Utf8, true),
        Field::new("primary_exchange", DataType::Utf8, true),
        Field::new("resolved_ticker", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(order.finish()),
            Arc::new(input.finish()),
            Arc::new(lookup.finish()),
            Arc::new(parsed.finish()),
            Arc::new(exchange.finish()),
            Arc::new(resolved.finish()),
            Arc::new(status.finish()),
            Arc::new(error.finish()),
        ],
    )
    .map_err(Into::into)
}

fn build_issuer_isins_batch(rows: &[IssuerIsinResolution]) -> Result<RecordBatch> {
    let mut order = Int32Builder::new();
    let mut input = StringBuilder::new();
    let mut lookup = StringBuilder::new();
    let mut bond = StringBuilder::new();
    let mut parent = StringBuilder::new();
    let mut issuer_isin = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut error = StringBuilder::new();

    for row in rows {
        order.append_value(row.input_order);
        input.append_value(&row.input_isin);
        lookup.append_value(&row.lookup_ticker);
        append_opt(&mut bond, row.bond_ticker.as_ref());
        append_opt(&mut parent, row.parent_ticker.as_ref());
        append_opt(&mut issuer_isin, row.issuer_equity_isin.as_ref());
        status.append_value(&row.status);
        append_opt(&mut error, row.error.as_ref());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("input_isin", DataType::Utf8, false),
        Field::new("lookup_ticker", DataType::Utf8, false),
        Field::new("bond_ticker", DataType::Utf8, true),
        Field::new("parent_ticker", DataType::Utf8, true),
        Field::new("issuer_equity_isin", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(order.finish()),
            Arc::new(input.finish()),
            Arc::new(lookup.finish()),
            Arc::new(bond.finish()),
            Arc::new(parent.finish()),
            Arc::new(issuer_isin.finish()),
            Arc::new(status.finish()),
            Arc::new(error.finish()),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    #[test]
    fn resolve_isins_rejects_non_equity_yellow_keys() {
        let isin = (0..10)
            .map(|digit| format!("ZZ000000001{digit}"))
            .find(|candidate| xbbg_ext::is_valid_isin(candidate))
            .unwrap();
        let lookup = isin_lookup_ticker(&isin);
        for sector in [
            "Pfd", "Corp", "Govt", "Comdty", "Index", "Curncy", "Mtge", "Muni", "M-Mkt",
        ] {
            let values = HashMap::from([(
                lookup.clone(),
                HashMap::from([
                    (
                        "PARSEKYABLE_DES".to_string(),
                        format!("SYNTHETIC     {sector}"),
                    ),
                    ("EQY_PRIM_SECURITY_COMP_EXCH".to_string(), "US".to_string()),
                ]),
            )]);
            let rows = build_resolve_isin_rows(
                std::slice::from_ref(&isin),
                std::slice::from_ref(&lookup),
                &values,
            );
            let batch = build_resolve_isins_batch(&rows).unwrap();
            assert_eq!(
                as_string_col(&batch, "status").unwrap().value(0),
                "unresolved"
            );
            assert!(batch.column_by_name("resolved_ticker").unwrap().is_null(0));
            assert!(as_string_col(&batch, "error")
                .unwrap()
                .value(0)
                .contains(sector));
        }
    }

    #[test]
    fn resolve_isins_preserves_order_and_unresolved_rows() {
        let mut values = HashMap::new();
        values.insert(
            "/ISIN/US0378331005".to_string(),
            HashMap::from([
                ("PARSEKYABLE_DES".to_string(), "AAPL US".to_string()),
                ("EQY_PRIM_SECURITY_COMP_EXCH".to_string(), "US".to_string()),
            ]),
        );
        let isins = vec!["US0378331005".to_string(), "BAD".to_string()];
        let lookups = isins
            .iter()
            .map(|isin| isin_lookup_ticker(isin))
            .collect::<Vec<_>>();
        let rows = build_resolve_isin_rows(&isins, &lookups, &values);

        assert_eq!(rows[0].resolved_ticker.as_deref(), Some("AAPL US Equity"));
        assert_eq!(rows[1].input_order, 1);
        assert_eq!(rows[1].status, "unresolved");
    }

    #[test]
    fn issuer_rows_never_build_nan_equity() {
        let mut values = HashMap::new();
        values.insert(
            "/ISIN/BAD".to_string(),
            HashMap::from([
                ("PARSEKYABLE_DES".to_string(), "Bad Bond".to_string()),
                ("EQY_PRIM_SECURITY_TICKER".to_string(), "nan".to_string()),
                ("EQY_PRIM_SECURITY_COMP_EXCH".to_string(), "US".to_string()),
            ]),
        );
        let isins = vec!["BAD".to_string()];
        let lookups = vec!["/ISIN/BAD".to_string()];
        let rows = build_issuer_rows_without_isin(&isins, &lookups, &values);
        assert!(rows[0].parent_ticker.is_none());
        assert_eq!(rows[0].status, "unresolved");
    }

    #[test]
    fn issuer_rows_use_parent_equity_fallback_fields() {
        let mut values = HashMap::new();
        values.insert(
            "/ISIN/US037833FB15".to_string(),
            HashMap::from([
                (
                    "PARSEKYABLE_DES".to_string(),
                    "YO223460     Corp".to_string(),
                ),
                ("EQY_PRIM_SECURITY_TICKER".to_string(), "nan".to_string()),
                ("EQY_PRIM_SECURITY_COMP_EXCH".to_string(), "nan".to_string()),
                (
                    "ISSUER_PARENT_EQY_TICKER".to_string(),
                    "AAPL US".to_string(),
                ),
            ]),
        );
        let isins = vec!["US037833FB15".to_string()];
        let lookups = vec!["/ISIN/US037833FB15".to_string()];
        let rows = build_issuer_rows_without_isin(&isins, &lookups, &values);

        assert_eq!(rows[0].bond_ticker.as_deref(), Some("YO223460     Corp"));
        assert_eq!(rows[0].parent_ticker.as_deref(), Some("AAPL US Equity"));
        assert_eq!(rows[0].status, "pending");
    }

    #[test]
    fn refdata_value_map_extracts_long_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["/ISIN/US0378331005"])),
                Arc::new(StringArray::from(vec!["PARSEKYABLE_DES"])),
                Arc::new(StringArray::from(vec!["AAPL US"])),
            ],
        )
        .unwrap();
        let mapped = refdata_value_map(&batch).unwrap();
        assert_eq!(mapped["/ISIN/US0378331005"]["PARSEKYABLE_DES"], "AAPL US");
    }
}
