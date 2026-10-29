//! Delayed-data policy shared by subscriptions and request/response calls.
//!
//! Bloomberg reports whether *this session* receives delayed data through
//! `IS_DELAYED_STREAM`: subscriptions see it in their initial paint, and the
//! same mnemonic works as a reference field. `EXCHANGE_DELAY` carries the
//! exchange's standard delay as text ("15 Minutes", "10 Minutes and Not
//! Available"). Requests opt in with [`RequestParams::on_delayed`]:
//!
//! - ReferenceData (bdp) appends the status fields the caller did not request
//!   to the same wire request. The RefData state records them under the
//!   `xbbg.delayed` metadata key and never emits them as rows.
//! - HistoricalData, IntradayBar, and IntradayTick run one concurrent
//!   ReferenceDataRequest for the same securities, but only when the request
//!   window reaches the last 24 hours: older data is complete whatever the
//!   entitlement.
//!
//! `raise` fails the call once the status is known. `warn` is left to the host
//! binding, which owns its warning channel; `ignore` keeps the metadata only.

use std::collections::HashSet;
use std::fmt::Write;
use std::str::FromStr;

use arrow_array::RecordBatch;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use xbbg_core::BlpError;

use crate::errors::BlpAsyncError;
use crate::services::{ExtractorType, Operation, Service};

use super::intraday_timezone::parse_utc_instant;
use super::state::{ResponseMetadata, delayed_metadata};
use super::{Engine, PlannedRequestShape, PreparedRequest, RequestParams};

/// What to do when Bloomberg delivers delayed rather than real-time data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DelayedPolicy {
    /// Record the status and let the host warn: once per subscription topic,
    /// or once per delayed security in a request result.
    #[default]
    Warn,
    /// Reject the subscription topic, or fail the whole request.
    Raise,
    /// Record the status only.
    Ignore,
}

impl FromStr for DelayedPolicy {
    type Err = BlpAsyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(Self::Warn),
            "raise" => Ok(Self::Raise),
            "ignore" => Ok(Self::Ignore),
            _ => Err(BlpAsyncError::ConfigError {
                detail: "on_delayed must be warn, raise, or ignore".to_string(),
            }),
        }
    }
}

/// Per-session delayed flag; also the subscription initial-paint field.
pub(crate) const IS_DELAYED_STREAM: &str = "IS_DELAYED_STREAM";
/// The exchange's standard delay as text.
pub(crate) const EXCHANGE_DELAY: &str = "EXCHANGE_DELAY";
const STATUS_FIELDS: [&str; 2] = [IS_DELAYED_STREAM, EXCHANGE_DELAY];
/// Historical and intraday windows that ended before this are complete.
const RECENT_WINDOW_HOURS: i64 = 24;
/// Delayed securities named in an `on_delayed=raise` error before truncating.
const RAISE_TICKER_LIMIT: usize = 10;

/// Status fields to append to a bdp wire request because the caller did not
/// request them. Empty unless the request is bdp-shaped with `on_delayed`.
pub(crate) fn hidden_refdata_fields(request: &PreparedRequest) -> Vec<&'static str> {
    let params = request.params();
    if params.on_delayed.is_none() || !matches!(request.shape(), PlannedRequestShape::RefData(_)) {
        return Vec::new();
    }
    let requested = params.fields.as_deref().unwrap_or_default();
    STATUS_FIELDS
        .into_iter()
        .filter(|status| {
            !requested
                .iter()
                .any(|field| field.trim().eq_ignore_ascii_case(status))
        })
        .collect()
}

/// Parse Bloomberg `EXCHANGE_DELAY` text ("15 Minutes", "10 Minutes and Not
/// Available", "1 Hour") into whole minutes.
pub(crate) fn parse_exchange_delay_minutes(text: &str) -> Option<u32> {
    let mut tokens = text.split_whitespace();
    while let Some(token) = tokens.next() {
        let Ok(amount) = token.parse::<u32>() else {
            continue;
        };
        let unit = tokens.next()?.to_ascii_lowercase();
        return match unit.as_str() {
            "minute" | "minutes" | "min" | "mins" => Some(amount),
            "hour" | "hours" | "hr" | "hrs" => amount.checked_mul(60),
            _ => None,
        };
    }
    None
}

/// Securities whose status a historical or intraday request looks up
/// separately. `None` for bdp (checked inline), without a policy, or when the
/// window ended more than a day ago.
pub(crate) fn lookup_securities(
    request: &PreparedRequest,
    now: DateTime<Utc>,
) -> Option<Vec<String>> {
    let params = request.params();
    params.on_delayed?;
    let recent = match request.operation() {
        Operation::HistoricalData => params
            .end_date
            .as_deref()
            .and_then(parse_request_date)
            .is_none_or(|end| end >= (now - Duration::days(1)).date_naive()),
        Operation::IntradayBar | Operation::IntradayTick => params
            .end_datetime
            .as_deref()
            .and_then(parse_utc_instant)
            .is_none_or(|end| end >= now - Duration::hours(RECENT_WINDOW_HOURS)),
        _ => return None,
    };
    if !recent {
        return None;
    }
    let mut seen = HashSet::new();
    let securities: Vec<String> = params
        .securities
        .iter()
        .flatten()
        .chain(params.security.iter())
        .filter(|security| seen.insert(security.as_str()))
        .cloned()
        .collect();
    (!securities.is_empty()).then_some(securities)
}

/// Run the status ReferenceDataRequest for a historical or intraday request.
pub(crate) async fn lookup(
    engine: &Engine,
    securities: Vec<String>,
) -> Result<ResponseMetadata, BlpAsyncError> {
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::RefData,
        extractor_set: true,
        securities: Some(securities),
        fields: Some(STATUS_FIELDS.map(str::to_string).to_vec()),
        validate_fields: Some(false),
        on_delayed: Some(DelayedPolicy::Ignore),
        ..Default::default()
    };
    let batch = engine.request_without_intraday_transform(params).await?;
    let mut metadata = ResponseMetadata::with_delayed(delayed_metadata(&batch).unwrap_or_default());
    metadata.collect_bloomberg_request_ids(&batch);
    Ok(metadata)
}

/// Retain completed contributors' IDs without changing fail-fast cancellation.
pub(crate) async fn combine(
    params: &RequestParams,
    response: impl Future<Output = Result<RecordBatch, BlpAsyncError>>,
    lookup: impl Future<Output = Result<ResponseMetadata, BlpAsyncError>>,
) -> Result<RecordBatch, BlpAsyncError> {
    let completed = parking_lot::Mutex::new(super::state::BloombergRequestIds::default());
    let response = async {
        let result = response.await;
        let mut ids = completed.lock();
        match &result {
            Ok(batch) => ids.collect(batch),
            Err(error) => ids.extend(error.bloomberg_request_ids().iter().cloned()),
        }
        result
    };
    let lookup = async {
        let result = lookup.await;
        let mut ids = completed.lock();
        match &result {
            Ok(metadata) => ids.extend(metadata.request_ids()),
            Err(error) => ids.extend(error.bloomberg_request_ids().iter().cloned()),
        }
        result
    };
    match tokio::try_join!(response, lookup) {
        Ok((batch, mut metadata)) => {
            metadata.extend_request_ids(completed.into_inner().iter().cloned());
            Ok(metadata.attach(batch))
        }
        Err(error) => Err(super::request_pool::enrich_async_request_error(
            error,
            params,
            &completed.lock().iter().cloned().collect::<Vec<_>>(),
        )),
    }
}

/// Apply `on_delayed=raise` to a finished result: fail when any security is
/// delayed. Other policies return the batch with its metadata untouched.
pub(crate) fn enforce(
    params: &RequestParams,
    operation: &str,
    batch: RecordBatch,
) -> Result<RecordBatch, BlpAsyncError> {
    if params.on_delayed != Some(DelayedPolicy::Raise) {
        return Ok(batch);
    }
    let status = delayed_metadata(&batch).unwrap_or_default();
    let delayed: Vec<_> = status.iter().filter(|(_, meta)| meta.delayed).collect();
    if delayed.is_empty() {
        return Ok(batch);
    }
    let mut label = String::from("Bloomberg is delivering delayed data; on_delayed=raise:");
    for (index, (ticker, meta)) in delayed.iter().take(RAISE_TICKER_LIMIT).enumerate() {
        label.push_str(if index == 0 { " " } else { ", " });
        label.push_str(ticker);
        if let Some(minutes) = meta.delay_minutes {
            write!(label, " ({minutes} minutes behind real time)").expect("writing to a String");
        }
    }
    if delayed.len() > RAISE_TICKER_LIMIT {
        write!(label, ", and {} more", delayed.len() - RAISE_TICKER_LIMIT)
            .expect("writing to a String");
    }
    Err(BlpError::RequestFailure {
        service: params.service.clone(),
        operation: Some(operation.to_string()),
        cid: None,
        label: Some(label),
        request_id: params.request_id.clone(),
        bloomberg_request_ids: ResponseMetadata::bloomberg_request_ids(&batch),
        source: None,
    }
    .into())
}

fn parse_request_date(value: &str) -> Option<NaiveDate> {
    let value = value.trim();
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
        .ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use chrono::TimeZone;

    use super::*;
    use crate::engine::state::{DelayedMeta, ResponseMetadata};
    use crate::error_presentation::{ErrorKind, ErrorPresentation, ErrorStyle};
    use crate::schema::SchemaCache;

    #[tokio::test]
    async fn delayed_lookup_composition_keeps_success_and_failure_provenance() {
        let params = RequestParams {
            service: "//synthetic/refdata".into(),
            operation: "SyntheticRequest".into(),
            request_id: Some("req-label".into()),
            ..Default::default()
        };
        let empty = || RecordBatch::new_empty(Arc::new(Schema::empty()));
        let data = || ResponseMetadata::attach_bloomberg_request_id(empty(), "sdk-synthetic-data");
        let status =
            || ResponseMetadata::with_bloomberg_request_ids([Arc::from("sdk-synthetic-status")]);
        let success = combine(
            &params,
            std::future::ready(Ok(data())),
            std::future::ready(Ok(status())),
        )
        .await
        .unwrap();
        assert_eq!(
            ResponseMetadata::bloomberg_request_ids(&success),
            vec![
                Arc::<str>::from("sdk-synthetic-data"),
                Arc::from("sdk-synthetic-status"),
            ]
        );
        let error = combine(
            &params,
            std::future::ready(Ok(data())),
            std::future::ready(Err(BlpError::RequestTimeout {
                request_id: None,
                bloomberg_request_ids: vec![Arc::from("sdk-synthetic-status")],
            }
            .into())),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.bloomberg_request_ids(),
            [
                Arc::<str>::from("sdk-synthetic-data"),
                Arc::from("sdk-synthetic-status"),
            ]
        );
        let failed = std::future::ready(Err(BlpError::RequestTimeout {
            request_id: None,
            bloomberg_request_ids: vec![Arc::from("sdk-synthetic-data")],
        }
        .into()));
        let pending = std::future::pending::<Result<ResponseMetadata, BlpAsyncError>>();
        let error = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            combine(&params, failed, pending),
        )
        .await
        .expect("request failures remain fail-fast")
        .unwrap_err();
        assert_eq!(
            error.bloomberg_request_ids(),
            [Arc::<str>::from("sdk-synthetic-data")]
        );
    }

    fn prepared(params: RequestParams) -> PreparedRequest {
        PreparedRequest::prepare(params, &SchemaCache::new()).unwrap()
    }

    fn bdp(fields: &[&str], on_delayed: Option<DelayedPolicy>) -> RequestParams {
        RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            securities: Some(vec!["VOD LN Equity".to_string()]),
            fields: Some(fields.iter().map(|field| (*field).to_string()).collect()),
            on_delayed,
            ..Default::default()
        }
    }

    fn bdh(end_date: &str) -> RequestParams {
        RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::HistoricalData.to_string(),
            securities: Some(vec![
                "VOD LN Equity".to_string(),
                "AAPL US Equity".to_string(),
                "VOD LN Equity".to_string(),
            ]),
            fields: Some(vec!["PX_LAST".to_string()]),
            start_date: Some("20240101".to_string()),
            end_date: Some(end_date.to_string()),
            on_delayed: Some(DelayedPolicy::Warn),
            ..Default::default()
        }
    }

    fn bdib(end_datetime: &str) -> RequestParams {
        RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::IntradayBar.to_string(),
            security: Some("BHP AT Equity".to_string()),
            event_type: Some("TRADE".to_string()),
            interval: Some(1),
            start_datetime: Some("2024-01-02T09:00:00".to_string()),
            end_datetime: Some(end_datetime.to_string()),
            on_delayed: Some(DelayedPolicy::Ignore),
            ..Default::default()
        }
    }

    fn status_batch(status: &[(&str, bool, Option<u32>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ticker",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["row"]))]).unwrap();
        let delayed = status
            .iter()
            .map(|(ticker, delayed, delay_minutes)| {
                (
                    (*ticker).to_string(),
                    DelayedMeta {
                        delayed: *delayed,
                        delay_minutes: *delay_minutes,
                    },
                )
            })
            .collect();
        ResponseMetadata::with_delayed(delayed).attach(batch)
    }

    #[test]
    fn exchange_delay_text_parses_to_minutes() {
        for (text, minutes) in [
            ("15 Minutes", Some(15)),
            ("20 Minutes", Some(20)),
            ("10 Minutes and Not Available", Some(10)),
            ("1 Hour", Some(60)),
            ("None", None),
            ("Not Available", None),
            ("", None),
        ] {
            assert_eq!(parse_exchange_delay_minutes(text), minutes, "{text:?}");
        }
    }

    #[test]
    fn bdp_appends_only_status_fields_the_caller_did_not_request() {
        let request = prepared(bdp(&["PX_LAST"], Some(DelayedPolicy::Warn)));
        assert_eq!(
            hidden_refdata_fields(&request),
            [IS_DELAYED_STREAM, EXCHANGE_DELAY]
        );

        let request = prepared(bdp(
            &["px_last", " is_delayed_stream "],
            Some(DelayedPolicy::Raise),
        ));
        assert_eq!(hidden_refdata_fields(&request), [EXCHANGE_DELAY]);

        assert!(hidden_refdata_fields(&prepared(bdp(&["PX_LAST"], None))).is_empty());
        let history = prepared(bdh("20240102"));
        assert!(hidden_refdata_fields(&history).is_empty());
    }

    #[test]
    fn status_lookup_covers_only_recent_history_and_intraday_windows() {
        let now = Utc.with_ymd_and_hms(2024, 1, 3, 12, 0, 0).unwrap();

        assert_eq!(
            lookup_securities(&prepared(bdh("20240102")), now),
            Some(vec![
                "VOD LN Equity".to_string(),
                "AAPL US Equity".to_string()
            ])
        );
        assert_eq!(lookup_securities(&prepared(bdh("20240101")), now), None);
        assert!(lookup_securities(&prepared(bdh("not a date")), now).is_some());

        assert_eq!(
            lookup_securities(&prepared(bdib("2024-01-02T12:00:00")), now),
            Some(vec!["BHP AT Equity".to_string()])
        );
        assert_eq!(
            lookup_securities(&prepared(bdib("2024-01-02T11:59:59Z")), now),
            None
        );

        let mut unchecked = bdh("20240103");
        unchecked.on_delayed = None;
        assert_eq!(lookup_securities(&prepared(unchecked), now), None);
        // bdp carries the status inline, so it never needs the extra request.
        let refdata = prepared(bdp(&["PX_LAST"], Some(DelayedPolicy::Warn)));
        assert_eq!(lookup_securities(&refdata, now), None);
    }

    fn presented(error: BlpAsyncError) -> ErrorPresentation {
        ErrorPresentation::from_async(error, ErrorStyle::Python)
    }

    #[test]
    fn raise_fails_on_delayed_securities_and_other_policies_keep_the_batch() {
        let batch = status_batch(&[
            ("AAPL US Equity", false, None),
            ("BHP AT Equity", true, Some(20)),
            ("VOD LN Equity", true, None),
        ]);
        let mut params = bdp(&["PX_LAST"], Some(DelayedPolicy::Raise));

        let error = enforce(&params, "ReferenceDataRequest", batch.clone()).unwrap_err();
        let error = presented(error);
        assert_eq!(error.kind, ErrorKind::Request);
        assert_eq!(
            error.message,
            "Request failed on //blp/refdata::ReferenceDataRequest - Bloomberg is delivering \
             delayed data; on_delayed=raise: BHP AT Equity (20 minutes behind real time), \
             VOD LN Equity"
        );

        let real_time = status_batch(&[("AAPL US Equity", false, None)]);
        assert!(enforce(&params, "ReferenceDataRequest", real_time).is_ok());

        for policy in [DelayedPolicy::Warn, DelayedPolicy::Ignore] {
            params.on_delayed = Some(policy);
            let kept = enforce(&params, "ReferenceDataRequest", batch.clone()).unwrap();
            assert_eq!(delayed_metadata(&kept), delayed_metadata(&batch));
        }
    }

    #[test]
    fn raise_error_truncates_long_ticker_lists() {
        let tickers: Vec<String> = (0..12).map(|index| format!("T{index:02} Equity")).collect();
        let status: Vec<_> = tickers
            .iter()
            .map(|ticker| (ticker.as_str(), true, None))
            .collect();
        let params = bdp(&["PX_LAST"], Some(DelayedPolicy::Raise));
        let error = enforce(&params, "ReferenceDataRequest", status_batch(&status)).unwrap_err();
        let message = presented(error).message;
        assert!(message.ends_with("T09 Equity, and 2 more"), "{message}");
        assert!(!message.contains("T10 Equity"), "{message}");
    }
}
