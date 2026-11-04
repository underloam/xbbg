//! Atomic "resolve, then subscribe" preflight for auction venues and ETF iNAVs.
//!
//! Ports the JavaScript `subscribeAuction`, `streamAuction` and
//! `subscribeEtfInav` checks (`js-xbbg/src/index.ts`): every input resolves
//! one-to-one before a single subscription opens, with the same messages and
//! elements. Resolution tables come from `xbbg_recipes`.
//!
//! # JSON arguments
//!
//! `subscribeAuction`:
//! `{"securities": string[], "pcsOverrides"?: {string: string}, "streamAuction"?: bool,
//!   "options"?: StreamOptions}`. `streamAuction: true` is the `streamAuction` entry
//! point (it rejects `options.rows`). `options.aliases` is rejected because the
//! aliases are derived from the inputs; `options.fields` defaults to the auction
//! `default` field group and `options.zeroAsNull` to the requested zero-price fields.
//!
//! `subscribeEtfInav`: `{"tickers": string[], "options"?: StreamOptions}`;
//! `options.fields` defaults to `["LAST_PRICE"]` (an explicit empty list stays empty).
//!
//! Unlike JavaScript, `subscribeEtfInav` validates its stream options before the
//! relationship lookup, so invalid options never cost a Bloomberg request.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::Value;
use xbbg_async::Engine;

use crate::errors::HostError;
use crate::subscribe::{
    PreparedSubscribe, StreamOptionsSpec, SubscribeArgs, js_trim, prepare_subscribe,
    validate_stream_controls,
};

/// Arguments of `subscribeAuction` / `streamAuction` (see the module docs).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuctionSubscribeArgs {
    pub securities: Vec<String>,
    pub pcs_overrides: Option<HashMap<String, String>>,
    #[serde(default)]
    pub stream_auction: bool,
    #[serde(default)]
    pub options: StreamOptionsSpec,
}

/// Arguments of `subscribeEtfInav` (see the module docs).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EtfInavSubscribeArgs {
    pub tickers: Vec<String>,
    #[serde(default)]
    pub options: StreamOptionsSpec,
}

/// Auction inputs checked before venue resolution.
#[derive(Clone, Debug)]
pub struct AuctionPlan {
    /// Unique inputs in first-seen order.
    pub inputs: Vec<String>,
    pub pcs_overrides: HashMap<String, String>,
    /// Requested stream fields.
    pub fields: Vec<String>,
    options: StreamOptionsSpec,
}

/// ETF inputs checked before relationship resolution.
#[derive(Clone, Debug)]
pub struct EtfInavPlan {
    /// Trimmed, unique ETF tickers in input order.
    pub etfs: Vec<String>,
    /// Requested stream fields.
    pub fields: Vec<String>,
    options: StreamOptionsSpec,
}

fn invalid(element: &str, message: impl Into<String>) -> HostError {
    HostError::validation(message).with_element(element)
}

/// Steps before venue resolution: rows guard (streamAuction), stream controls,
/// derived aliases, first-seen dedup, non-empty inputs, default fields.
pub fn plan_auction(args: AuctionSubscribeArgs) -> Result<AuctionPlan, HostError> {
    let AuctionSubscribeArgs {
        securities,
        pcs_overrides,
        stream_auction,
        mut options,
    } = args;
    if stream_auction && options.rows.as_ref().is_some_and(|rows| !rows.is_null()) {
        return Err(invalid(
            "rows",
            "streamAuction always delivers rows; use subscribeAuction for rows=false",
        ));
    }
    validate_stream_controls(&options)?;
    if options
        .aliases
        .as_ref()
        .is_some_and(|aliases| !aliases.is_null())
    {
        return Err(invalid(
            "aliases",
            "Auction aliases are derived from input securities",
        ));
    }
    let mut seen = HashSet::new();
    let inputs: Vec<String> = securities
        .into_iter()
        .filter(|security| seen.insert(security.clone()))
        .collect();
    if inputs.is_empty() {
        return Err(invalid("securities", "securities must not be empty"));
    }
    let fields = match options.fields.take() {
        Some(fields) if !fields.is_empty() => fields,
        _ => xbbg_ext::auction::DEFAULT
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
    };
    Ok(AuctionPlan {
        inputs,
        pcs_overrides: pcs_overrides.unwrap_or_default(),
        fields,
        options,
    })
}

/// Steps before relationship resolution: trim, non-empty, no duplicates,
/// stream controls, default fields.
pub fn plan_etf_inav(args: EtfInavSubscribeArgs) -> Result<EtfInavPlan, HostError> {
    let EtfInavSubscribeArgs {
        tickers,
        mut options,
    } = args;
    let etfs: Vec<String> = tickers
        .iter()
        .map(|ticker| js_trim(ticker).to_string())
        .collect();
    if etfs.is_empty() {
        return Err(invalid("tickers", "etfs must not be empty"));
    }
    let duplicates = first_seen_duplicates(&etfs);
    if !duplicates.is_empty() {
        return Err(invalid(
            "tickers",
            format!(
                "Duplicate ETF inputs are not allowed: {}",
                duplicates.join(", ")
            ),
        ));
    }
    validate_stream_controls(&options)?;
    let fields = options
        .fields
        .take()
        .unwrap_or_else(|| vec!["LAST_PRICE".to_string()]);
    Ok(EtfInavPlan {
        etfs,
        fields,
        options,
    })
}

fn first_seen_duplicates(values: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut duplicates: Vec<String> = Vec::new();
    for value in values {
        if !seen.insert(value.as_str()) && !duplicates.contains(value) {
            duplicates.push(value.clone());
        }
    }
    duplicates
}

/// Numeric cells as `f64` (`None` for null); `None` when the column is not numeric.
fn numbers(array: &ArrayRef) -> Option<Vec<Option<f64>>> {
    if !array.data_type().is_numeric() {
        return None;
    }
    let floats = cast(array, &DataType::Float64).ok()?;
    let floats = floats
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()?;
    Some(floats.iter().collect())
}

/// String cells (`None` for null); `None` when the column is not a string column.
fn strings(array: &ArrayRef) -> Option<StringArray> {
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let utf8 = cast(array, &DataType::Utf8).ok()?;
            utf8.as_any().downcast_ref::<StringArray>().cloned()
        }
        _ => None,
    }
}

fn text(column: &StringArray, row: usize) -> Option<&str> {
    (!column.is_null(row)).then(|| column.value(row))
}

/// Map each `input_order` value to its row; `None` for a missing, null,
/// non-numeric or duplicated order.
fn rows_by_order(order: &ArrayRef) -> Option<HashMap<u64, usize>> {
    let values = numbers(order)?;
    let mut rows = HashMap::with_capacity(values.len());
    for (row, value) in values.into_iter().enumerate() {
        let value = value?;
        let key = if value == 0.0 { 0 } else { value.to_bits() };
        if rows.insert(key, row).is_some() {
            return None;
        }
    }
    Some(rows)
}

fn row_for(rows: &HashMap<u64, usize>, index: usize) -> Option<usize> {
    let value = index as f64;
    let key = if value == 0.0 { 0 } else { value.to_bits() };
    rows.get(&key).copied()
}

/// Validated venue topics and their aliases back to the inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedVenues {
    /// Trimmed venue topics in input order.
    pub topics: Vec<String>,
    /// `venue topic -> input` labels.
    pub aliases: Vec<(String, String)>,
}

/// The JavaScript `validatedAuctionTopics`: resolved, trimmed venue topics in
/// input order and their `topic -> input` aliases.
pub fn validated_auction_topics(
    securities: &[String],
    table: &RecordBatch,
) -> Result<ResolvedVenues, HostError> {
    let malformed = || {
        invalid(
            "securities",
            format!(
                "Venue resolution is not one-to-one with securities: {}",
                securities.join(", ")
            ),
        )
    };
    let columns = (
        table.column_by_name("input_order"),
        table.column_by_name("security").and_then(strings),
        table.column_by_name("venue_topic").and_then(strings),
        table.column_by_name("status").and_then(strings),
        table.column_by_name("error").and_then(strings),
    );
    let (Some(order), Some(security), Some(venue), Some(status), Some(error)) = columns else {
        return Err(malformed());
    };
    if table.num_rows() != securities.len() {
        return Err(malformed());
    }
    let rows = rows_by_order(order).ok_or_else(malformed)?;
    let mut topics = Vec::new();
    let mut labels: Vec<(String, String)> = Vec::new();
    let mut failures = Vec::new();
    for (index, input) in securities.iter().enumerate() {
        let Some(row) = row_for(&rows, index) else {
            return Err(malformed());
        };
        if text(&security, row) != Some(input.as_str()) {
            return Err(malformed());
        }
        let topic = text(&venue, row);
        let detail = text(&error, row);
        let resolution = text(&status, row);
        let blank_topic = topic.is_none_or(|topic| js_trim(topic).is_empty());
        let has_error = detail.is_some_and(|detail| !js_trim(detail).is_empty());
        if resolution != Some("resolved") || blank_topic || has_error {
            let reason = detail.or(resolution).unwrap_or("null");
            failures.push(format!("{input}: {reason}"));
            continue;
        }
        let normalized = js_trim(topic.unwrap_or_default()).to_string();
        if let Some((_, previous)) = labels.iter().find(|(known, _)| *known == normalized) {
            failures.push(format!("Ambiguous venue {normalized}: {previous}, {input}"));
        } else {
            labels.push((normalized.clone(), input.clone()));
            topics.push(normalized);
        }
    }
    if !failures.is_empty() {
        return Err(invalid(
            "securities",
            format!("Auction preflight failed: {}", failures.join("; ")),
        ));
    }
    Ok(ResolvedVenues {
        topics,
        aliases: labels,
    })
}

/// The JavaScript `validatedInavTickers`: one validated iNAV ticker per ETF.
pub fn validated_inav_tickers(
    etfs: &[String],
    table: &RecordBatch,
) -> Result<Vec<String>, HostError> {
    let malformed = || {
        invalid(
            "tickers",
            "ETF NAV relationship result is not one-to-one with requested ETFs",
        )
    };
    let columns = (
        table.column_by_name("input_order"),
        table.column_by_name("etf_ticker").and_then(strings),
        table.column_by_name("inav_ticker").and_then(strings),
        table
            .column_by_name("inav_validation_error")
            .and_then(strings),
    );
    let (Some(order), Some(etf_ticker), Some(inav_ticker), Some(validation_error)) = columns else {
        return Err(malformed());
    };
    if table.num_rows() != etfs.len() {
        return Err(malformed());
    }
    let order = rows_by_order(order).ok_or_else(malformed)?;
    let mut rows = Vec::with_capacity(etfs.len());
    for (index, etf) in etfs.iter().enumerate() {
        let Some(row) = row_for(&order, index) else {
            return Err(malformed());
        };
        if text(&etf_ticker, row) != Some(etf.as_str()) {
            return Err(malformed());
        }
        rows.push((etf, row));
    }
    for (etf, row) in &rows {
        if let Some(error) = text(&validation_error, *row)
            && !js_trim(error).is_empty()
        {
            return Err(invalid(
                "tickers",
                format!("Invalid iNAV relationship for ETF {etf}: {error}"),
            ));
        }
    }
    let pairs: Vec<(&String, Option<String>)> = rows
        .iter()
        .map(|(etf, row)| {
            let inav = text(&inav_ticker, *row)
                .map(js_trim)
                .filter(|inav| !inav.is_empty())
                .map(str::to_string);
            (*etf, inav)
        })
        .collect();
    let missing: Vec<&str> = pairs
        .iter()
        .filter(|(_, inav)| inav.is_none())
        .map(|(etf, _)| etf.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(invalid(
            "tickers",
            format!(
                "Missing valid iNAV relationship for ETFs: {}",
                missing.join(", ")
            ),
        ));
    }
    let resolved: Vec<(&String, String)> = pairs
        .into_iter()
        .filter_map(|(etf, inav)| inav.map(|inav| (etf, inav)))
        .collect();
    let mut owners: HashMap<&str, Vec<&str>> = HashMap::new();
    for (etf, inav) in &resolved {
        owners.entry(inav.as_str()).or_default().push(etf.as_str());
    }
    for (_, inav) in &resolved {
        if let Some(etfs) = owners.get(inav.as_str())
            && etfs.len() > 1
        {
            return Err(invalid(
                "tickers",
                format!(
                    "Ambiguous iNAV reverse mapping for {inav}: {}",
                    etfs.join(", ")
                ),
            ));
        }
    }
    Ok(resolved.into_iter().map(|(_, inav)| inav).collect())
}

/// The `subscribe` arguments for resolved venues: derived aliases and the
/// zero-price `zeroAsNull` default unless the caller supplied one.
pub fn auction_subscribe_args(plan: AuctionPlan, resolved: ResolvedVenues) -> SubscribeArgs {
    let AuctionPlan {
        fields,
        mut options,
        ..
    } = plan;
    if options.zero_as_null.as_ref().is_none_or(Value::is_null) {
        let zero_price: Vec<Value> = xbbg_ext::auction::ZERO_PRICE_FIELDS
            .iter()
            .filter(|field| fields.iter().any(|requested| requested == **field))
            .map(|field| Value::String((*field).to_string()))
            .collect();
        options.zero_as_null = Some(Value::Array(zero_price));
    }
    options.aliases = Some(Value::Object(
        resolved
            .aliases
            .into_iter()
            .map(|(topic, input)| (topic, Value::String(input)))
            .collect(),
    ));
    SubscribeArgs {
        service: None,
        tickers: resolved.topics,
        fields,
        raw_options: false,
        options,
    }
}

/// The `subscribe` arguments for validated iNAV tickers (no aliases back to the ETFs).
pub fn etf_inav_subscribe_args(plan: EtfInavPlan, inav_tickers: Vec<String>) -> SubscribeArgs {
    SubscribeArgs {
        service: None,
        tickers: inav_tickers,
        fields: plan.fields,
        raw_options: false,
        options: plan.options,
    }
}

/// Resolve every input to its primary venue, then prepare one venue subscription.
pub async fn prepare_auction_subscribe(
    engine: &Engine,
    args: AuctionSubscribeArgs,
    engine_batch_items: usize,
) -> Result<PreparedSubscribe, HostError> {
    let plan = plan_auction(args)?;
    let venues = xbbg_recipes::recipe_resolve_venues(
        engine,
        plan.inputs.clone(),
        plan.pcs_overrides.clone(),
    )
    .await
    .map_err(HostError::from_recipe)?;
    let resolved = validated_auction_topics(&plan.inputs, &venues)?;
    prepare_subscribe(auction_subscribe_args(plan, resolved), engine_batch_items)
}

/// Resolve each ETF's iNAV ticker, then prepare one iNAV subscription.
pub async fn prepare_etf_inav_subscribe(
    engine: &Engine,
    args: EtfInavSubscribeArgs,
    engine_batch_items: usize,
) -> Result<PreparedSubscribe, HostError> {
    let plan = plan_etf_inav(args)?;
    let relationships = xbbg_recipes::etf::recipe_etf_nav_relationships(engine, plan.etfs.clone())
        .await
        .map_err(HostError::from_recipe)?;
    let inav_tickers = validated_inav_tickers(&plan.etfs, &relationships)?;
    prepare_subscribe(
        etf_inav_subscribe_args(plan, inav_tickers),
        engine_batch_items,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use serde_json::json;

    use super::*;
    use crate::errors::HostErrorKind;

    fn strings_column(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// `(input_order, security, venue_topic, status, error)`.
    type VenueRow<'a> = (i32, &'a str, Option<&'a str>, &'a str, Option<&'a str>);

    fn venues(rows: &[VenueRow<'_>]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("input_order", DataType::Int32, false),
            Field::new("security", DataType::Utf8, false),
            Field::new("venue_topic", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
            Field::new("error", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                strings_column(&rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>()),
                strings_column(&rows.iter().map(|row| row.2).collect::<Vec<_>>()),
                strings_column(&rows.iter().map(|row| Some(row.3)).collect::<Vec<_>>()),
                strings_column(&rows.iter().map(|row| row.4).collect::<Vec<_>>()),
            ],
        )
        .expect("venue table")
    }

    fn relationships(order: ArrayRef, rows: &[(&str, Option<&str>, Option<&str>)]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("input_order", order.data_type().clone(), true),
            Field::new("etf_ticker", DataType::Utf8, false),
            Field::new("inav_ticker", DataType::Utf8, true),
            Field::new("inav_validation_error", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                order,
                strings_column(&rows.iter().map(|row| Some(row.0)).collect::<Vec<_>>()),
                strings_column(&rows.iter().map(|row| row.1).collect::<Vec<_>>()),
                strings_column(&rows.iter().map(|row| row.2).collect::<Vec<_>>()),
            ],
        )
        .expect("relationship table")
    }

    fn inputs(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn assert_validation(error: HostError, element: &str, message: &str) {
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(error.message, message);
        assert_eq!(error.context.element.as_deref(), Some(element));
    }

    fn auction_args(value: serde_json::Value) -> AuctionSubscribeArgs {
        serde_json::from_value(value).expect("auction args")
    }

    #[test]
    fn auction_plan_checks_rows_then_controls_then_aliases_then_inputs() {
        assert_validation(
            plan_auction(auction_args(json!({
                "securities": [], "streamAuction": true,
                "options": {"rows": true, "aliases": 1}
            })))
            .expect_err("rows"),
            "rows",
            "streamAuction always delivers rows; use subscribeAuction for rows=false",
        );
        assert_validation(
            plan_auction(auction_args(
                json!({"securities": [], "options": {"aliases": 1}}),
            ))
            .expect_err("controls"),
            "aliases",
            "aliases must map topic strings to label strings",
        );
        assert_validation(
            plan_auction(auction_args(json!({
                "securities": ["IBM US Equity"], "options": {"aliases": {}}
            })))
            .expect_err("aliases"),
            "aliases",
            "Auction aliases are derived from input securities",
        );
        assert_validation(
            plan_auction(auction_args(json!({"securities": []}))).expect_err("empty"),
            "securities",
            "securities must not be empty",
        );
    }

    #[test]
    fn auction_plan_dedups_in_first_seen_order_and_defaults_fields() {
        let plan = plan_auction(auction_args(json!({
            "securities": ["VOD LN Equity", "IBM US Equity", "VOD LN Equity"],
            "options": {"fields": []}
        })))
        .expect("plan");
        assert_eq!(plan.inputs, inputs(&["VOD LN Equity", "IBM US Equity"]));
        assert_eq!(
            plan.fields,
            xbbg_ext::auction::DEFAULT
                .iter()
                .map(|field| field.to_string())
                .collect::<Vec<_>>()
        );
        let plan = plan_auction(auction_args(json!({
            "securities": ["IBM US Equity"], "options": {"fields": ["THEO_PRICE", "BID"]}
        })))
        .expect("explicit fields");
        assert_eq!(plan.fields, inputs(&["THEO_PRICE", "BID"]));
    }

    #[test]
    fn resolved_venues_become_trimmed_topics_with_input_aliases() {
        let securities = inputs(&["IBM US Equity", "/isin/GB00BH4HKS39"]);
        let table = venues(&[
            (
                1,
                "/isin/GB00BH4HKS39",
                Some(" VOD LN Equity "),
                "resolved",
                Some("  "),
            ),
            (0, "IBM US Equity", Some("IBM UN Equity"), "resolved", None),
        ]);
        let resolved = validated_auction_topics(&securities, &table).expect("resolved");
        assert_eq!(
            resolved,
            ResolvedVenues {
                topics: inputs(&["IBM UN Equity", "VOD LN Equity"]),
                aliases: vec![
                    ("IBM UN Equity".to_string(), "IBM US Equity".to_string()),
                    (
                        "VOD LN Equity".to_string(),
                        "/isin/GB00BH4HKS39".to_string()
                    ),
                ],
            }
        );
    }

    #[test]
    fn every_venue_failure_and_ambiguity_is_reported_together() {
        let securities = inputs(&["A", "B", "C", "D", "E"]);
        let table = venues(&[
            (0, "A", Some("X Equity"), "resolved", None),
            (1, "B", None, "unresolved", None),
            (2, "C", Some("Y Equity"), "mismatch", Some("wrong exchange")),
            (3, "D", Some(" X Equity"), "resolved", None),
            (4, "E", Some("Z Equity"), "resolved", Some("")),
        ]);
        assert_validation(
            validated_auction_topics(&securities, &table).expect_err("failures"),
            "securities",
            "Auction preflight failed: B: unresolved; C: wrong exchange; Ambiguous venue X Equity: A, D",
        );
        // An explicit (blank) error string is the reported reason, as in JavaScript.
        let table = venues(&[(0, "A", Some(" "), "resolved", Some(""))]);
        assert_validation(
            validated_auction_topics(&inputs(&["A"]), &table).expect_err("blank"),
            "securities",
            "Auction preflight failed: A: ",
        );
    }

    #[test]
    fn malformed_venue_tables_are_not_one_to_one() {
        let message = "Venue resolution is not one-to-one with securities: A, B";
        let securities = inputs(&["A", "B"]);
        let short = venues(&[(0, "A", Some("X"), "resolved", None)]);
        let duplicate_order = venues(&[
            (0, "A", Some("X"), "resolved", None),
            (0, "B", Some("Y"), "resolved", None),
        ]);
        let wrong_security = venues(&[
            (0, "A", Some("X"), "resolved", None),
            (1, "C", Some("Y"), "resolved", None),
        ]);
        let gap = venues(&[
            (0, "A", Some("X"), "resolved", None),
            (2, "B", Some("Y"), "resolved", None),
        ]);
        for table in [short, duplicate_order, wrong_security, gap] {
            assert_validation(
                validated_auction_topics(&securities, &table).expect_err("malformed"),
                "securities",
                message,
            );
        }
        let missing_column = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "input_order",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![0, 1]))],
        )
        .expect("table");
        assert_validation(
            validated_auction_topics(&securities, &missing_column).expect_err("columns"),
            "securities",
            message,
        );
    }

    #[test]
    fn auction_subscribe_args_derive_aliases_and_zero_price_defaults() {
        let plan = plan_auction(auction_args(json!({
            "securities": ["IBM US Equity"],
            "options": {"fields": ["THEO_PRICE", "BID", "INDICATIVE_NEAR"], "flushThreshold": 2}
        })))
        .expect("plan");
        let args = auction_subscribe_args(
            plan,
            ResolvedVenues {
                topics: inputs(&["IBM UN Equity"]),
                aliases: vec![("IBM UN Equity".into(), "IBM US Equity".into())],
            },
        );
        assert_eq!(args.tickers, inputs(&["IBM UN Equity"]));
        assert_eq!(
            args.fields,
            inputs(&["THEO_PRICE", "BID", "INDICATIVE_NEAR"])
        );
        assert_eq!(args.options.flush_threshold, Some(2));
        assert_eq!(
            args.options.zero_as_null,
            Some(json!(["THEO_PRICE", "INDICATIVE_NEAR"]))
        );
        assert_eq!(
            args.options.aliases,
            Some(json!({"IBM UN Equity": "IBM US Equity"}))
        );
        let prepared = prepare_subscribe(args, 16).expect("prepared");
        assert_eq!(
            prepared.request.aliases,
            vec![("IBM UN Equity".to_string(), "IBM US Equity".to_string())]
        );
        assert_eq!(
            prepared.request.zero_as_null,
            inputs(&["THEO_PRICE", "INDICATIVE_NEAR"])
        );

        let plan = plan_auction(auction_args(json!({
            "securities": ["IBM US Equity"], "options": {"zeroAsNull": []}
        })))
        .expect("explicit zeroAsNull");
        let args = auction_subscribe_args(
            plan,
            ResolvedVenues {
                topics: inputs(&["IBM UN Equity"]),
                aliases: Vec::new(),
            },
        );
        assert_eq!(args.options.zero_as_null, Some(json!([])));
    }

    #[test]
    fn etf_plan_trims_rejects_empty_and_duplicates_then_defaults_fields() {
        let plan = |value| plan_etf_inav(serde_json::from_value(value).expect("args"));
        assert_validation(
            plan(json!({"tickers": []})).expect_err("empty"),
            "tickers",
            "etfs must not be empty",
        );
        assert_validation(
            plan(json!({"tickers": ["SPY US Equity", " QQQ US Equity", "SPY US Equity ", "QQQ US Equity"]}))
                .expect_err("duplicates"),
            "tickers",
            "Duplicate ETF inputs are not allowed: SPY US Equity, QQQ US Equity",
        );
        assert_validation(
            plan(json!({"tickers": ["SPY US Equity"], "options": {"rows": "x"}}))
                .expect_err("controls before lookup"),
            "rows",
            "rows must be a boolean",
        );
        let defaulted = plan(json!({"tickers": [" SPY US Equity "]})).expect("plan");
        assert_eq!(defaulted.etfs, inputs(&["SPY US Equity"]));
        assert_eq!(defaulted.fields, inputs(&["LAST_PRICE"]));
        let empty_fields =
            plan(json!({"tickers": ["SPY US Equity"], "options": {"fields": []}})).expect("plan");
        assert!(empty_fields.fields.is_empty());
    }

    #[test]
    fn inav_relationships_validate_in_javascript_order() {
        let etfs = inputs(&["SPY US Equity", "QQQ US Equity"]);
        let order = || Arc::new(Int32Array::from(vec![1, 0])) as ArrayRef;
        let table = relationships(
            order(),
            &[
                ("QQQ US Equity", Some(" QQQIV Index "), None),
                ("SPY US Equity", Some("SPYIV Index"), Some(" ")),
            ],
        );
        assert_eq!(
            validated_inav_tickers(&etfs, &table).expect("tickers"),
            inputs(&["SPYIV Index", "QQQIV Index"])
        );

        let table = relationships(
            order(),
            &[
                ("QQQ US Equity", None, Some("not an index")),
                ("SPY US Equity", None, None),
            ],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &table).expect_err("invalid first"),
            "tickers",
            "Invalid iNAV relationship for ETF QQQ US Equity: not an index",
        );

        let table = relationships(
            order(),
            &[
                ("QQQ US Equity", Some("  "), None),
                ("SPY US Equity", None, None),
            ],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &table).expect_err("missing"),
            "tickers",
            "Missing valid iNAV relationship for ETFs: SPY US Equity, QQQ US Equity",
        );

        let table = relationships(
            order(),
            &[
                ("QQQ US Equity", Some("SAME Index"), None),
                ("SPY US Equity", Some(" SAME Index"), None),
            ],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &table).expect_err("ambiguous"),
            "tickers",
            "Ambiguous iNAV reverse mapping for SAME Index: SPY US Equity, QQQ US Equity",
        );
    }

    #[test]
    fn inav_tables_must_be_one_to_one_and_numeric_orders_are_accepted() {
        let etfs = inputs(&["SPY US Equity"]);
        let message = "ETF NAV relationship result is not one-to-one with requested ETFs";
        let null_order = relationships(
            Arc::new(Int64Array::from(vec![None])),
            &[("SPY US Equity", Some("SPYIV Index"), None)],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &null_order).expect_err("null order"),
            "tickers",
            message,
        );
        let wrong_etf = relationships(
            Arc::new(Int32Array::from(vec![0])),
            &[("QQQ US Equity", Some("QQQIV Index"), None)],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &wrong_etf).expect_err("wrong etf"),
            "tickers",
            message,
        );
        let float_order = relationships(
            Arc::new(Float64Array::from(vec![0.0])),
            &[("SPY US Equity", Some("SPYIV Index"), None)],
        );
        assert_eq!(
            validated_inav_tickers(&etfs, &float_order).expect("float order"),
            inputs(&["SPYIV Index"])
        );
        let fractional = relationships(
            Arc::new(Float64Array::from(vec![0.5])),
            &[("SPY US Equity", Some("SPYIV Index"), None)],
        );
        assert_validation(
            validated_inav_tickers(&etfs, &fractional).expect_err("fractional order"),
            "tickers",
            message,
        );
    }

    #[test]
    fn inav_subscribe_args_keep_the_resolved_topics_without_aliases() {
        let plan = plan_etf_inav(
            serde_json::from_value(json!({
                "tickers": ["SPY US Equity"], "options": {"delivery": "state"}
            }))
            .expect("args"),
        )
        .expect("plan");
        let args = etf_inav_subscribe_args(plan, inputs(&["SPYIV Index"]));
        let prepared = prepare_subscribe(args, 16).expect("prepared");
        assert_eq!(prepared.request.topics, inputs(&["SPYIV Index"]));
        assert_eq!(prepared.request.fields, inputs(&["LAST_PRICE"]));
        assert!(prepared.request.aliases.is_empty());
        assert_eq!(
            prepared.request.delivery,
            xbbg_async::SubscriptionDeliveryMode::State
        );
    }
}
