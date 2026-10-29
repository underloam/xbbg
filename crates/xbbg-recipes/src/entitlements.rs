//! Per-security delayed-data and feed-entitlement status.
//!
//! One ReferenceDataRequest answers "does this session get delayed data?" on any
//! Desktop, Server API, or B-PIPE session: Bloomberg evaluates
//! `IS_DELAYED_STREAM` for the requester. `FEED_EID1..4` and `FEED_DELAYED_EID1`
//! name the real-time and delayed feed entitlements even when `returnEids`
//! withholds the EIDs a requester lacks, so `check_identity` can test them
//! against the engine's authorized identity.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use xbbg_async::DelayedPolicy;
use xbbg_async::engine::state::{
    BloombergRequestIds, DelayedMeta, METADATA_KEY_SECURITY_ERRORS, SecurityErrorMeta,
    delayed_metadata,
};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};

use crate::error::Result;
use crate::identifiers::refdata_value_map;

type ReferenceValues = HashMap<String, HashMap<String, String>>;

const EXCHANGE_DELAY: &str = "EXCHANGE_DELAY";
const EXCH_CODE: &str = "EXCH_CODE";
const REALTIME_EID_FIELDS: [&str; 4] = ["FEED_EID1", "FEED_EID2", "FEED_EID3", "FEED_EID4"];
const DELAYED_EID_FIELD: &str = "FEED_DELAYED_EID1";
const FIELDS: [&str; 8] = [
    "IS_DELAYED_STREAM",
    EXCHANGE_DELAY,
    EXCH_CODE,
    REALTIME_EID_FIELDS[0],
    REALTIME_EID_FIELDS[1],
    REALTIME_EID_FIELDS[2],
    REALTIME_EID_FIELDS[3],
    DELAYED_EID_FIELD,
];
/// Feed EIDs gate market data, so the identity check uses that service.
const IDENTITY_SERVICE: &str = "//blp/mktdata";

#[derive(Debug, Default, PartialEq)]
struct DelayStatusRow {
    ticker: String,
    delayed: Option<bool>,
    delay_minutes: Option<u32>,
    exchange_delay: Option<String>,
    exch_code: Option<String>,
    realtime_eids: [Option<i64>; 4],
    delayed_eid: Option<i64>,
    realtime_entitled: Option<bool>,
    delayed_entitled: Option<bool>,
    error: Option<String>,
}

/// Delayed-data status for each security, plus optional identity entitlements.
///
/// Columns: `ticker`, `delayed` (this session receives delayed data),
/// `delay_minutes` (minutes behind real time; null when real time),
/// `exchange_delay` (Bloomberg's `EXCHANGE_DELAY` text), `exch_code`,
/// `realtime_eid1..4` (`FEED_EID1..4`), `delayed_eid` (`FEED_DELAYED_EID1`),
/// `realtime_entitled` / `delayed_entitled`, and `error` (securityError message).
///
/// `check_identity` checks the feed EIDs against the engine's authorized identity
/// on `//blp/mktdata`, which needs SAPI/B-PIPE authorization or an EMRS-enrolled
/// Desktop user; the entitlement columns stay null without it. Input order is
/// kept and duplicates are removed.
pub async fn recipe_delay_status(
    engine: &Engine,
    securities: Vec<String>,
    check_identity: bool,
) -> Result<RecordBatch> {
    let mut seen = HashSet::new();
    let tickers: Vec<String> = securities
        .into_iter()
        .filter(|ticker| seen.insert(ticker.clone()))
        .collect();
    if tickers.is_empty() {
        return build_delay_status_batch(&[]);
    }

    let batch = engine
        .request(RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            securities: Some(tickers.clone()),
            fields: Some(FIELDS.map(str::to_string).to_vec()),
            // Text values keep EID and delay parsing in one place below.
            field_types: Some(
                FIELDS
                    .iter()
                    .map(|field| ((*field).to_string(), "string".to_string()))
                    .collect(),
            ),
            format: Some("long".to_string()),
            validate_fields: Some(false),
            on_delayed: Some(DelayedPolicy::Ignore),
            ..Default::default()
        })
        .await?;
    let mut request_ids = BloombergRequestIds::default();
    request_ids.collect(&batch);
    let result: Result<_> = async {
        let mut rows = delay_status_rows(
            &tickers,
            &refdata_value_map(&batch)?,
            &delayed_metadata(&batch).unwrap_or_default(),
            &security_error_messages(&batch),
        );

        if check_identity {
            let eids = identity_eids(&rows);
            if !eids.is_empty() {
                let check = engine.check_entitlements(IDENTITY_SERVICE, &eids).await?;
                // `false` with nothing listed means the identity is not authorized
                // for the service at all, so no EID is entitled.
                let denied_all = !check.entitled && check.failed_eids.is_empty();
                apply_identity_check(&mut rows, &check.failed_eids, denied_all);
            }
        }
        build_delay_status_batch(&rows)
    }
    .await;
    let output =
        result.map_err(|error| error.with_bloomberg_request_ids(request_ids.iter().cloned()))?;
    Ok(request_ids.attach(output))
}

fn delay_status_rows(
    tickers: &[String],
    values: &ReferenceValues,
    status: &BTreeMap<String, DelayedMeta>,
    errors: &HashMap<String, String>,
) -> Vec<DelayStatusRow> {
    tickers
        .iter()
        .map(|ticker| {
            let fields = values.get(ticker);
            let text = |field: &str| fields.and_then(|fields| fields.get(field)).cloned();
            let eid = |field: &str| {
                fields
                    .and_then(|fields| fields.get(field))
                    .and_then(|value| parse_eid(value))
            };
            let status = status.get(ticker);
            DelayStatusRow {
                ticker: ticker.clone(),
                delayed: status.map(|status| status.delayed),
                delay_minutes: status.and_then(|status| status.delay_minutes),
                exchange_delay: text(EXCHANGE_DELAY),
                exch_code: text(EXCH_CODE),
                realtime_eids: REALTIME_EID_FIELDS.map(eid),
                delayed_eid: eid(DELAYED_EID_FIELD),
                realtime_entitled: None,
                delayed_entitled: None,
                error: errors.get(ticker).cloned(),
            }
        })
        .collect()
}

/// Feed EIDs are positive integers; Bloomberg reports `0` for an unused slot.
fn parse_eid(value: &str) -> Option<i64> {
    let value = value.trim();
    let eid = value.parse::<i64>().ok().or_else(|| {
        value
            .parse::<f64>()
            .ok()
            .filter(|eid| eid.fract() == 0.0 && *eid >= 1.0 && *eid <= i64::MAX as f64)
            .map(|eid| eid as i64)
    })?;
    (eid > 0).then_some(eid)
}

fn security_error_messages(batch: &RecordBatch) -> HashMap<String, String> {
    batch
        .schema_ref()
        .metadata()
        .get(METADATA_KEY_SECURITY_ERRORS)
        .and_then(|json| serde_json::from_str::<BTreeMap<String, SecurityErrorMeta>>(json).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|(ticker, error)| (ticker, error.message))
        .collect()
}

/// Unique feed EIDs across all rows, for one identity check.
fn identity_eids(rows: &[DelayStatusRow]) -> Vec<i32> {
    let mut seen = HashSet::new();
    rows.iter()
        .flat_map(|row| row.realtime_eids.iter().chain([&row.delayed_eid]))
        .flatten()
        .filter_map(|eid| i32::try_from(*eid).ok())
        .filter(|eid| seen.insert(*eid))
        .collect()
}

fn apply_identity_check(rows: &mut [DelayStatusRow], failed_eids: &[i32], denied_all: bool) {
    let failed: HashSet<i64> = failed_eids.iter().map(|eid| i64::from(*eid)).collect();
    let entitled = |eid: &i64| !denied_all && !failed.contains(eid);
    for row in rows {
        let mut realtime = row.realtime_eids.iter().flatten().peekable();
        if realtime.peek().is_some() {
            row.realtime_entitled = Some(realtime.all(entitled));
        }
        row.delayed_entitled = row.delayed_eid.as_ref().map(entitled);
    }
}

fn build_delay_status_batch(rows: &[DelayStatusRow]) -> Result<RecordBatch> {
    let mut ticker = StringBuilder::new();
    let mut delayed = BooleanBuilder::new();
    let mut delay_minutes = Int32Builder::new();
    let mut exchange_delay = StringBuilder::new();
    let mut exch_code = StringBuilder::new();
    let mut realtime_eids: [Int64Builder; 4] = std::array::from_fn(|_| Int64Builder::new());
    let mut delayed_eid = Int64Builder::new();
    let mut realtime_entitled = BooleanBuilder::new();
    let mut delayed_entitled = BooleanBuilder::new();
    let mut error = StringBuilder::new();

    for row in rows {
        ticker.append_value(&row.ticker);
        delayed.append_option(row.delayed);
        delay_minutes.append_option(
            row.delay_minutes
                .and_then(|minutes| i32::try_from(minutes).ok()),
        );
        exchange_delay.append_option(row.exchange_delay.as_deref());
        exch_code.append_option(row.exch_code.as_deref());
        for (builder, eid) in realtime_eids.iter_mut().zip(row.realtime_eids) {
            builder.append_option(eid);
        }
        delayed_eid.append_option(row.delayed_eid);
        realtime_entitled.append_option(row.realtime_entitled);
        delayed_entitled.append_option(row.delayed_entitled);
        error.append_option(row.error.as_deref());
    }

    let mut fields = vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("delayed", DataType::Boolean, true),
        Field::new("delay_minutes", DataType::Int32, true),
        Field::new("exchange_delay", DataType::Utf8, true),
        Field::new("exch_code", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(ticker.finish()),
        Arc::new(delayed.finish()),
        Arc::new(delay_minutes.finish()),
        Arc::new(exchange_delay.finish()),
        Arc::new(exch_code.finish()),
    ];
    for (slot, mut builder) in realtime_eids.into_iter().enumerate() {
        fields.push(Field::new(
            format!("realtime_eid{}", slot + 1),
            DataType::Int64,
            true,
        ));
        columns.push(Arc::new(builder.finish()));
    }
    fields.extend([
        Field::new("delayed_eid", DataType::Int64, true),
        Field::new("realtime_entitled", DataType::Boolean, true),
        Field::new("delayed_entitled", DataType::Boolean, true),
        Field::new("error", DataType::Utf8, true),
    ]);
    columns.extend([
        Arc::new(delayed_eid.finish()) as ArrayRef,
        Arc::new(realtime_entitled.finish()),
        Arc::new(delayed_entitled.finish()),
        Arc::new(error.finish()),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};

    use super::*;

    fn values(entries: &[(&str, &[(&str, &str)])]) -> ReferenceValues {
        entries
            .iter()
            .map(|(ticker, fields)| {
                (
                    (*ticker).to_string(),
                    fields
                        .iter()
                        .map(|(field, value)| ((*field).to_string(), (*value).to_string()))
                        .collect(),
                )
            })
            .collect()
    }

    fn status(delayed: bool, delay_minutes: Option<u32>) -> DelayedMeta {
        DelayedMeta {
            delayed,
            delay_minutes,
        }
    }

    fn sample_rows() -> Vec<DelayStatusRow> {
        let tickers = ["VG1 Index", "VOD LN Equity", "EURUSD Curncy", "BAD Equity"]
            .map(str::to_string)
            .to_vec();
        let values = values(&[
            (
                "VG1 Index",
                &[
                    ("EXCHANGE_DELAY", "15 Minutes"),
                    ("EXCH_CODE", "EUX"),
                    ("FEED_EID1", "14111"),
                    ("FEED_EID2", "14111"),
                    ("FEED_EID3", "0"),
                    ("FEED_DELAYED_EID1", "74555"),
                ],
            ),
            (
                "VOD LN Equity",
                &[
                    ("EXCHANGE_DELAY", "15 Minutes"),
                    ("EXCH_CODE", "LN"),
                    ("FEED_EID1", "14014"),
                    ("FEED_EID2", "14076.0"),
                    ("FEED_DELAYED_EID1", "40066"),
                ],
            ),
            (
                "EURUSD Curncy",
                &[("FEED_EID1", "33828"), ("FEED_DELAYED_EID1", "0")],
            ),
        ]);
        let status = BTreeMap::from([
            ("VG1 Index".to_string(), status(false, None)),
            ("VOD LN Equity".to_string(), status(true, Some(15))),
            ("EURUSD Curncy".to_string(), status(false, None)),
        ]);
        let errors = HashMap::from([(
            "BAD Equity".to_string(),
            "Unknown/Invalid Security".to_string(),
        )]);
        delay_status_rows(&tickers, &values, &status, &errors)
    }

    #[test]
    fn rows_keep_feed_eids_as_returned_and_null_out_unused_slots() {
        let rows = sample_rows();
        assert_eq!(
            rows[0].realtime_eids,
            [Some(14111), Some(14111), None, None]
        );
        assert_eq!(rows[0].delayed_eid, Some(74555));
        assert_eq!(
            rows[1].realtime_eids,
            [Some(14014), Some(14076), None, None]
        );
        assert_eq!(
            (rows[1].delayed, rows[1].delay_minutes),
            (Some(true), Some(15))
        );
        assert_eq!(rows[2].delayed_eid, None);
        assert_eq!(
            rows[3],
            DelayStatusRow {
                ticker: "BAD Equity".to_string(),
                error: Some("Unknown/Invalid Security".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(
            identity_eids(&rows),
            [14111, 74555, 14014, 14076, 40066, 33828]
        );
    }

    #[test]
    fn identity_check_marks_entitlement_per_feed() {
        let mut rows = sample_rows();
        apply_identity_check(&mut rows, &[14076, 74555], false);
        let entitlements: Vec<_> = rows
            .iter()
            .map(|row| (row.realtime_entitled, row.delayed_entitled))
            .collect();
        assert_eq!(
            entitlements,
            [
                (Some(true), Some(false)),
                // One failed real-time EID is enough to lose real-time data.
                (Some(false), Some(true)),
                (Some(true), None),
                (None, None),
            ]
        );

        let mut rows = sample_rows();
        apply_identity_check(&mut rows, &[], true);
        assert_eq!(
            (rows[1].realtime_entitled, rows[1].delayed_entitled),
            (Some(false), Some(false))
        );
    }

    #[test]
    fn batch_has_one_scalar_column_per_status_field() {
        let mut rows = sample_rows();
        apply_identity_check(&mut rows, &[14076], false);
        let batch = build_delay_status_batch(&rows).unwrap();
        let names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(
            names,
            [
                "ticker",
                "delayed",
                "delay_minutes",
                "exchange_delay",
                "exch_code",
                "realtime_eid1",
                "realtime_eid2",
                "realtime_eid3",
                "realtime_eid4",
                "delayed_eid",
                "realtime_entitled",
                "delayed_entitled",
                "error",
            ]
        );
        let column = |name: &str| batch.column_by_name(name).unwrap();
        let delayed = column("delayed")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            delayed.iter().collect::<Vec<_>>(),
            [Some(false), Some(true), Some(false), None]
        );
        let minutes = column("delay_minutes")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            minutes.iter().collect::<Vec<_>>(),
            [None, Some(15), None, None]
        );
        let eid2 = column("realtime_eid2")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            eid2.iter().collect::<Vec<_>>(),
            [Some(14111), Some(14076), None, None]
        );
        let error = column("error")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(error.null_count(), 3);
        assert_eq!(error.value(3), "Unknown/Invalid Security");

        let empty = build_delay_status_batch(&[]).unwrap();
        assert_eq!((empty.num_rows(), empty.num_columns()), (0, 13));
    }
}
