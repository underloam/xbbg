//! Planners for the typed request wrappers except `bta` (see `ta`), ported from
//! `js-xbbg/src/index.ts` (`Engine.bdp` … `Engine.bgovts`, `fieldInfo`,
//! `fieldSearch`, and the `ext.cdx` helpers).

use serde::Deserialize;
use xbbg_async::services::{CDX_INFO_FIELDS, CDX_PRICING_FIELDS, CDX_RISK_FIELDS};

use super::values::{Ordered, Scalar, js_number_string};
use super::{RequestInput, SecurityOverride, StringPair};
use crate::errors::HostError;

const REFDATA: &str = "//blp/refdata";
const BQLSVC: &str = "//blp/bqlsvc";
const EXRSVC: &str = "//blp/exrsvc";
const APIFLDS: &str = "//blp/apiflds";
const INSTRUMENTS: &str = "//blp/instruments";

const PER_SECURITY_MESSAGE: &str =
    "Per-security overrides are only supported by bdp(), bdh(), and bds()";

/// TS `kwargs` (`OverridesMap`): ordered `string | number | boolean` values.
pub(super) type Kwargs = Ordered<Scalar>;

/// TS `mapObjectToPairs`: `None` stays absent, `{}` becomes `[]`.
pub(super) fn kwargs_pairs(kwargs: Option<Kwargs>) -> Option<Vec<StringPair>> {
    kwargs.map(|kwargs| {
        kwargs
            .into_entries()
            .into_iter()
            .map(|(key, value)| StringPair {
                key,
                value: value.to_js_string(),
            })
            .collect()
    })
}

/// TS `upsertStringPair`: replace the first pair with `key`, else append.
fn upsert_pair(pairs: &mut Vec<StringPair>, key: &str, value: String) {
    match pairs.iter_mut().find(|pair| pair.key == key) {
        Some(pair) => pair.value = value,
        None => pairs.push(StringPair::new(key, value)),
    }
}

/// TS `mapOverridesToRequestParts`: a list is sent only when non-empty.
fn non_empty<T>(list: Option<Vec<T>>) -> Option<Vec<T>> {
    list.filter(|list| !list.is_empty())
}

/// TS `mapOverridesToPairs`: global pairs only; per-security overrides are an
/// ARGUMENT error (a raw `TypeError` in JS).
fn global_overrides(
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<&[SecurityOverride]>,
) -> Result<Vec<StringPair>, HostError> {
    if security_overrides.is_some_and(|entries| !entries.is_empty()) {
        return Err(HostError::argument(PER_SECURITY_MESSAGE));
    }
    Ok(overrides.unwrap_or_default())
}

fn pair(key: &str, value: impl Into<String>) -> StringPair {
    StringPair::new(key, value)
}

/// `bdp` arguments (also the `acdxInfo` options).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BdpArgs {
    tickers: Vec<String>,
    fields: Vec<String>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
    include_security_errors: Option<bool>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
    validate_fields: Option<bool>,
}

/// `Engine.bdp`: ReferenceDataRequest with the `refdata` extractor.
pub(super) fn bdp(args: BdpArgs) -> RequestInput {
    RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("ReferenceDataRequest".to_owned()),
        extractor: Some("refdata".to_owned()),
        securities: Some(args.tickers),
        fields: Some(args.fields),
        overrides: non_empty(args.overrides),
        security_overrides: non_empty(args.security_overrides),
        kwargs: kwargs_pairs(args.kwargs),
        include_security_errors: Some(args.include_security_errors.unwrap_or(false)),
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        validate_fields: args.validate_fields,
        format: args.format,
        ..RequestInput::default()
    }
}

/// `bds` arguments: the `bdp` options without `includeSecurityErrors`/`onDelayed`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BdsArgs {
    tickers: Vec<String>,
    fields: Vec<String>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
    return_eids: Option<bool>,
    validate_fields: Option<bool>,
}

/// `Engine.bds`: ReferenceDataRequest with the `bulk` extractor.
pub(super) fn bds(args: BdsArgs) -> RequestInput {
    RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("ReferenceDataRequest".to_owned()),
        extractor: Some("bulk".to_owned()),
        securities: Some(args.tickers),
        fields: Some(args.fields),
        overrides: non_empty(args.overrides),
        security_overrides: non_empty(args.security_overrides),
        kwargs: kwargs_pairs(args.kwargs),
        return_eids: args.return_eids,
        validate_fields: args.validate_fields,
        format: args.format,
        ..RequestInput::default()
    }
}

/// `bdh` arguments; `start`/`end` are host-formatted `YYYYMMDD` dates.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BdhArgs {
    tickers: Vec<String>,
    fields: Vec<String>,
    start: Option<String>,
    end: Option<String>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
    validate_fields: Option<bool>,
}

/// `Engine.bdh`: HistoricalDataRequest with the `histdata` extractor.
pub(super) fn bdh(args: BdhArgs) -> RequestInput {
    RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("HistoricalDataRequest".to_owned()),
        extractor: Some("histdata".to_owned()),
        securities: Some(args.tickers),
        fields: Some(args.fields),
        overrides: non_empty(args.overrides),
        security_overrides: non_empty(args.security_overrides),
        kwargs: kwargs_pairs(args.kwargs),
        start_date: args.start,
        end_date: args.end,
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        validate_fields: args.validate_fields,
        format: args.format,
        ..RequestInput::default()
    }
}

/// `bdib` arguments; `start`/`end` are host-formatted datetimes, `dt` a
/// host-formatted `YYYYMMDD` day that expands to the full-day range.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BdibArgs {
    ticker: String,
    start: Option<String>,
    end: Option<String>,
    dt: Option<String>,
    request_tz: Option<String>,
    output_tz: Option<String>,
    event_type: Option<String>,
    interval: Option<u32>,
    kwargs: Option<Kwargs>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
}

/// TS `fullDayRange` over an already formatted day.
fn full_day_range(dt: &str) -> Result<(String, String), HostError> {
    if dt.trim().is_empty() {
        return Err(HostError::argument(
            "dt must be a non-empty date-like value",
        ));
    }
    if dt.len() != 8 || !dt.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HostError::argument(format!(
            "dt must be a YYYYMMDD date, got '{dt}'"
        )));
    }
    let day = format!("{}-{}-{}", &dt[..4], &dt[4..6], &dt[6..]);
    Ok((format!("{day}T00:00:00"), format!("{day}T23:59:59")))
}

/// `Engine.bdib`: IntradayBarRequest, `eventType` `TRADE` and `interval` 1 by default.
pub(super) fn bdib(args: BdibArgs) -> Result<RequestInput, HostError> {
    let (start, end) = match args.dt.as_deref() {
        None => (args.start, args.end),
        Some(dt) => {
            if args.start.is_some() || args.end.is_some() {
                return Err(HostError::argument(
                    "bdib dt cannot be combined with start/end options",
                ));
            }
            let (start, end) = full_day_range(dt)?;
            (Some(start), Some(end))
        }
    };
    Ok(RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("IntradayBarRequest".to_owned()),
        extractor: Some("intraday_bar".to_owned()),
        security: Some(args.ticker),
        kwargs: kwargs_pairs(args.kwargs),
        start_datetime: start,
        end_datetime: end,
        request_tz: args.request_tz,
        output_tz: args.output_tz,
        event_type: Some(args.event_type.unwrap_or_else(|| "TRADE".to_owned())),
        interval: Some(args.interval.unwrap_or(1)),
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        ..RequestInput::default()
    })
}

/// `bdtick` arguments; `start`/`end` are host-formatted datetimes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BdtickArgs {
    ticker: String,
    start: Option<String>,
    end: Option<String>,
    request_tz: Option<String>,
    output_tz: Option<String>,
    event_types: Option<Vec<String>>,
    include_condition_codes: Option<bool>,
    include_exchange_codes: Option<bool>,
    include_broker_codes: Option<bool>,
    include_rps_codes: Option<bool>,
    include_bic_mic_codes: Option<bool>,
    include_non_plottable_events: Option<bool>,
    include_bloomberg_standard_condition_codes: Option<bool>,
    kwargs: Option<Kwargs>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
}

/// `Engine.bdtick`: IntradayTickRequest, `eventTypes` `["TRADE"]` by default; the
/// typed include flags become kwargs and beat a kwarg of the same name
/// (`buildBdtickKwargs`, which drops an empty result).
pub(super) fn bdtick(args: BdtickArgs) -> RequestInput {
    let flags = [
        ("includeConditionCodes", args.include_condition_codes),
        ("includeExchangeCodes", args.include_exchange_codes),
        ("includeBrokerCodes", args.include_broker_codes),
        ("includeRpsCodes", args.include_rps_codes),
        ("includeBicMicCodes", args.include_bic_mic_codes),
        (
            "includeNonPlottableEvents",
            args.include_non_plottable_events,
        ),
        (
            "includeBloombergStandardConditionCodes",
            args.include_bloomberg_standard_condition_codes,
        ),
    ];
    let mut kwargs = kwargs_pairs(args.kwargs).unwrap_or_default();
    for (name, value) in flags {
        if let Some(value) = value {
            upsert_pair(&mut kwargs, name, value.to_string());
        }
    }
    RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("IntradayTickRequest".to_owned()),
        extractor: Some("intraday_tick".to_owned()),
        security: Some(args.ticker),
        kwargs: non_empty(Some(kwargs)),
        start_datetime: args.start,
        end_datetime: args.end,
        request_tz: args.request_tz,
        output_tz: args.output_tz,
        event_types: Some(args.event_types.unwrap_or_else(|| vec!["TRADE".to_owned()])),
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        ..RequestInput::default()
    }
}

/// `bql` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BqlArgs {
    query: String,
    app_name: Option<String>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.bql`: `sendQuery` on `//blp/bqlsvc`; `appName` upserts
/// `clientContext.appName` ahead of a kwarg of that name.
pub(super) fn bql(args: BqlArgs) -> RequestInput {
    let mut kwargs = kwargs_pairs(args.kwargs);
    if let Some(app_name) = args.app_name {
        upsert_pair(
            kwargs.get_or_insert_with(Vec::new),
            "clientContext.appName",
            app_name,
        );
    }
    RequestInput {
        service: BQLSVC.to_owned(),
        operation: Some("sendQuery".to_owned()),
        extractor: Some("bql".to_owned()),
        elements: Some(vec![pair("expression", args.query)]),
        kwargs,
        format: args.format,
        ..RequestInput::default()
    }
}

/// `beqs` arguments; `asof` is a host-formatted `YYYYMMDD` date.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BeqsArgs {
    screen: String,
    asof: Option<String>,
    screen_type: Option<String>,
    group: Option<String>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.beqs`: BeqsRequest, `screenType` `PRIVATE` and `Group` `General` by
/// default, `asOfDate` only for a non-blank `asof`.
pub(super) fn beqs(args: BeqsArgs) -> Result<RequestInput, HostError> {
    let overrides = global_overrides(args.overrides, args.security_overrides.as_deref())?;
    let mut elements = vec![
        pair("screenName", args.screen),
        pair(
            "screenType",
            args.screen_type.unwrap_or_else(|| "PRIVATE".to_owned()),
        ),
        pair("Group", args.group.unwrap_or_else(|| "General".to_owned())),
    ];
    if let Some(asof) = args.asof.filter(|asof| !asof.trim().is_empty()) {
        elements.push(pair("asOfDate", asof));
    }
    Ok(RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("BeqsRequest".to_owned()),
        extractor: Some("generic".to_owned()),
        overrides: non_empty(Some(overrides)),
        elements: Some(elements),
        kwargs: kwargs_pairs(args.kwargs),
        format: args.format,
        ..RequestInput::default()
    })
}

/// `bsrch` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BsrchArgs {
    search_spec: String,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.bsrch`: ExcelGetGridRequest; overrides then kwargs become grid
/// elements after `Domain`, not Bloomberg overrides.
pub(super) fn bsrch(args: BsrchArgs) -> Result<RequestInput, HostError> {
    let overrides = global_overrides(args.overrides, args.security_overrides.as_deref())?;
    let mut elements = vec![pair("Domain", args.search_spec)];
    elements.extend(overrides);
    elements.extend(kwargs_pairs(args.kwargs).unwrap_or_default());
    Ok(RequestInput {
        service: EXRSVC.to_owned(),
        operation: Some("ExcelGetGridRequest".to_owned()),
        extractor: Some("bsrch".to_owned()),
        elements: Some(elements),
        format: args.format,
        ..RequestInput::default()
    })
}

/// `bflds` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BfldsArgs {
    fields: Option<Vec<String>>,
    search_spec: Option<String>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

fn field_search_input(
    search_spec: String,
    kwargs: Option<Kwargs>,
    format: Option<String>,
) -> RequestInput {
    RequestInput {
        service: APIFLDS.to_owned(),
        operation: Some("FieldSearchRequest".to_owned()),
        kwargs: kwargs_pairs(kwargs),
        search_spec: Some(search_spec),
        format,
        ..RequestInput::default()
    }
}

fn field_info_input(
    fields: Vec<String>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
) -> RequestInput {
    RequestInput {
        service: APIFLDS.to_owned(),
        operation: Some("FieldInfoRequest".to_owned()),
        kwargs: kwargs_pairs(kwargs),
        field_ids: Some(fields),
        format,
        ..RequestInput::default()
    }
}

/// `Engine.bflds`: FieldSearchRequest when `searchSpec` is set (fields are then
/// unused), else FieldInfoRequest over `fields` (`[]` when absent). No extractor.
pub(super) fn bflds(args: BfldsArgs) -> RequestInput {
    match args.search_spec {
        Some(search_spec) => field_search_input(search_spec, args.kwargs, args.format),
        None => field_info_input(args.fields.unwrap_or_default(), args.kwargs, args.format),
    }
}

/// `fieldInfo` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FieldInfoArgs {
    fields: Vec<String>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.fieldInfo`: `bflds` over the given fields.
pub(super) fn field_info(args: FieldInfoArgs) -> RequestInput {
    field_info_input(args.fields, args.kwargs, args.format)
}

/// `fieldSearch` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FieldSearchArgs {
    search_spec: String,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.fieldSearch`: `bflds` with the given search spec.
pub(super) fn field_search(args: FieldSearchArgs) -> RequestInput {
    field_search_input(args.search_spec, args.kwargs, args.format)
}

/// `blkp` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BlkpArgs {
    query: String,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.blkp`: instrumentListRequest on `//blp/instruments`. No extractor.
pub(super) fn blkp(args: BlkpArgs) -> RequestInput {
    RequestInput {
        service: INSTRUMENTS.to_owned(),
        operation: Some("instrumentListRequest".to_owned()),
        elements: Some(vec![pair("query", args.query)]),
        kwargs: kwargs_pairs(args.kwargs),
        format: args.format,
        ..RequestInput::default()
    }
}

/// `bport` arguments.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BportArgs {
    portfolio: String,
    fields: Vec<String>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.bport`: PortfolioDataRequest with the portfolio as `security`.
pub(super) fn bport(args: BportArgs) -> Result<RequestInput, HostError> {
    let overrides = global_overrides(args.overrides, args.security_overrides.as_deref())?;
    Ok(RequestInput {
        service: REFDATA.to_owned(),
        operation: Some("PortfolioDataRequest".to_owned()),
        security: Some(args.portfolio),
        fields: Some(args.fields),
        overrides: non_empty(Some(overrides)),
        kwargs: kwargs_pairs(args.kwargs),
        format: args.format,
        ..RequestInput::default()
    })
}

/// `bcurves`/`bgovts` arguments (TS `RequestOptions`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InstrumentListArgs {
    ticker: String,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
}

/// `Engine.bcurves`/`Engine.bgovts`: `operation` on `//blp/instruments` with the
/// ticker as `query`. Overrides do not exist for these operations, so non-empty
/// ones are rejected (JS drops them silently).
pub(super) fn instrument_list(
    method: &str,
    operation: &str,
    args: InstrumentListArgs,
) -> Result<RequestInput, HostError> {
    let has_overrides = args.overrides.as_ref().is_some_and(|list| !list.is_empty())
        || args
            .security_overrides
            .as_ref()
            .is_some_and(|list| !list.is_empty());
    if has_overrides {
        return Err(HostError::argument(format!(
            "{method}() does not support overrides"
        )));
    }
    Ok(RequestInput {
        service: INSTRUMENTS.to_owned(),
        operation: Some(operation.to_owned()),
        elements: Some(vec![pair("query", args.ticker)]),
        kwargs: kwargs_pairs(args.kwargs),
        format: args.format,
        ..RequestInput::default()
    })
}

/// `acdxInfo` arguments: the ticker plus the `bdp` options.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CdxInfoArgs {
    ticker: String,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
    include_security_errors: Option<bool>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
    validate_fields: Option<bool>,
}

fn cdx_fields(fields: &[&str]) -> Vec<String> {
    fields.iter().map(|field| (*field).to_owned()).collect()
}

/// `ext.cdx.acdx_info`: `bdp` of the ticker over `CDX_INFO_FIELDS`.
pub(super) fn acdx_info(args: CdxInfoArgs) -> RequestInput {
    bdp(BdpArgs {
        tickers: vec![args.ticker],
        fields: cdx_fields(CDX_INFO_FIELDS),
        overrides: args.overrides,
        security_overrides: args.security_overrides,
        kwargs: args.kwargs,
        format: args.format,
        include_security_errors: args.include_security_errors,
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        validate_fields: args.validate_fields,
    })
}

/// `acdxPricing`/`acdxRisk` arguments (TS `CdxOptions`): `recoveryRate` is a
/// percent (40 means 40%).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CdxArgs {
    ticker: String,
    recovery_rate: Option<f64>,
    overrides: Option<Vec<StringPair>>,
    security_overrides: Option<Vec<SecurityOverride>>,
    kwargs: Option<Kwargs>,
    format: Option<String>,
    include_security_errors: Option<bool>,
    return_eids: Option<bool>,
    on_delayed: Option<String>,
    validate_fields: Option<bool>,
}

/// TS `normalizeRecoveryOptions` + `bdp`: a recovery rate upserts the global
/// `CDS_RR` override (beating an explicit one, keeping its position).
fn cdx_with_recovery(args: CdxArgs, fields: &[&str]) -> RequestInput {
    let mut overrides = args.overrides;
    if let Some(rate) = args.recovery_rate {
        upsert_pair(
            overrides.get_or_insert_with(Vec::new),
            "CDS_RR",
            js_number_string(rate),
        );
    }
    bdp(BdpArgs {
        tickers: vec![args.ticker],
        fields: cdx_fields(fields),
        overrides,
        security_overrides: args.security_overrides,
        kwargs: args.kwargs,
        format: args.format,
        include_security_errors: args.include_security_errors,
        return_eids: args.return_eids,
        on_delayed: args.on_delayed,
        validate_fields: args.validate_fields,
    })
}

/// `ext.cdx.acdx_pricing`: `bdp` over `CDX_PRICING_FIELDS`.
pub(super) fn acdx_pricing(args: CdxArgs) -> RequestInput {
    cdx_with_recovery(args, CDX_PRICING_FIELDS)
}

/// `ext.cdx.acdx_risk`: `bdp` over `CDX_RISK_FIELDS`.
pub(super) fn acdx_risk(args: CdxArgs) -> RequestInput {
    cdx_with_recovery(args, CDX_RISK_FIELDS)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::errors::HostErrorKind;
    use crate::request::plan;

    /// Plan `method` and compare the planned `RequestInput` JSON with `expected`.
    ///
    /// `args` is `json!` (keys sorted) or a raw JSON string where key order matters.
    fn assert_plan(method: &str, args: impl std::fmt::Display, expected: Value) {
        let args = args.to_string();
        let planned =
            plan(method, &args).unwrap_or_else(|error| panic!("{method} {args}: {error:?}"));
        let planned: Value = serde_json::from_str(&planned.to_json()).unwrap();
        assert_eq!(planned, expected, "{method} {args}");
    }

    fn plan_error(method: &str, args: Value) -> crate::errors::HostError {
        plan(method, &args.to_string()).expect_err("planning must fail")
    }

    fn kwargs_of(method: &str, args: Value) -> Value {
        let planned = plan(method, &args.to_string()).unwrap();
        serde_json::from_str::<Value>(&planned.to_json()).unwrap()["kwargs"].clone()
    }

    #[test]
    fn bdp_defaults_match_the_ts_wrapper() {
        assert_plan(
            "bdp",
            json!({"tickers": ["IBM US Equity"], "fields": ["PX_LAST"]}),
            json!({
                "service": "//blp/refdata",
                "operation": "ReferenceDataRequest",
                "extractor": "refdata",
                "securities": ["IBM US Equity"],
                "fields": ["PX_LAST"],
                "includeSecurityErrors": false
            }),
        );
    }

    #[test]
    fn bdp_forwards_every_option() {
        assert_plan(
            "bdp",
            r#"{
                "tickers": ["IBM US Equity", "MSFT US Equity"],
                "fields": ["PX_LAST", "NAME"],
                "overrides": [{"key": "EQY_FUND_CRNCY", "value": "USD"}],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "EQY_FUND_CRNCY", "value": "EUR"}]},
                    {"security": "MSFT US Equity", "overrides": [{"key": "USER_LOCAL_TRADE_DATE", "value": "20240102"}]}
                ],
                "kwargs": {"b": 2.0, "a": true, "c": "x"},
                "format": "long_typed",
                "includeSecurityErrors": true,
                "returnEids": true,
                "onDelayed": "warn",
                "validateFields": true
            }"#,
            json!({
                "service": "//blp/refdata",
                "operation": "ReferenceDataRequest",
                "extractor": "refdata",
                "securities": ["IBM US Equity", "MSFT US Equity"],
                "fields": ["PX_LAST", "NAME"],
                "overrides": [{"key": "EQY_FUND_CRNCY", "value": "USD"}],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "EQY_FUND_CRNCY", "value": "EUR"}]},
                    {"security": "MSFT US Equity", "overrides": [{"key": "USER_LOCAL_TRADE_DATE", "value": "20240102"}]}
                ],
                "kwargs": [
                    {"key": "b", "value": "2"},
                    {"key": "a", "value": "true"},
                    {"key": "c", "value": "x"}
                ],
                "includeSecurityErrors": true,
                "returnEids": true,
                "onDelayed": "warn",
                "validateFields": true,
                "format": "long_typed"
            }),
        );
    }

    #[test]
    fn empty_override_lists_are_dropped_like_map_overrides_to_request_parts() {
        assert_plan(
            "bdp",
            json!({"tickers": [], "fields": [], "overrides": [], "securityOverrides": [], "kwargs": {}}),
            json!({
                "service": "//blp/refdata",
                "operation": "ReferenceDataRequest",
                "extractor": "refdata",
                "securities": [],
                "fields": [],
                "kwargs": [],
                "includeSecurityErrors": false
            }),
        );
    }

    #[test]
    fn bds_uses_the_bulk_extractor_and_rejects_bdp_only_options() {
        assert_plan(
            "bds",
            json!({
                "tickers": ["IBM US Equity"],
                "fields": ["DVD_HIST_ALL"],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "DVD_Start_Dt", "value": "20240101"}]}
                ],
                "returnEids": true,
                "validateFields": false
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "ReferenceDataRequest",
                "extractor": "bulk",
                "securities": ["IBM US Equity"],
                "fields": ["DVD_HIST_ALL"],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "DVD_Start_Dt", "value": "20240101"}]}
                ],
                "returnEids": true,
                "validateFields": false
            }),
        );
        for option in ["includeSecurityErrors", "onDelayed"] {
            let mut args = json!({"tickers": ["X"], "fields": ["Y"]});
            args[option] = json!("warn");
            let error = plan_error("bds", args);
            assert_eq!(error.kind, HostErrorKind::Argument);
            assert_eq!(error.message, format!("unknown field '{option}'"));
        }
    }

    #[test]
    fn bdh_maps_start_and_end_to_request_dates() {
        assert_plan(
            "bdh",
            json!({
                "tickers": ["IBM US Equity"],
                "fields": ["PX_LAST"],
                "start": "20240101",
                "end": "20240102",
                "overrides": [{"key": "EQY_FUND_CRNCY", "value": "USD"}],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "CRNCY", "value": "EUR"}]}
                ],
                "kwargs": {"periodicitySelection": "DAILY"},
                "returnEids": true,
                "onDelayed": "raise",
                "validateFields": true,
                "format": "long"
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "HistoricalDataRequest",
                "extractor": "histdata",
                "securities": ["IBM US Equity"],
                "fields": ["PX_LAST"],
                "overrides": [{"key": "EQY_FUND_CRNCY", "value": "USD"}],
                "securityOverrides": [
                    {"security": "IBM US Equity", "overrides": [{"key": "CRNCY", "value": "EUR"}]}
                ],
                "kwargs": [{"key": "periodicitySelection", "value": "DAILY"}],
                "startDate": "20240101",
                "endDate": "20240102",
                "returnEids": true,
                "onDelayed": "raise",
                "validateFields": true,
                "format": "long"
            }),
        );
        assert_plan(
            "bdh",
            json!({"tickers": ["IBM US Equity"], "fields": ["PX_LAST"]}),
            json!({
                "service": "//blp/refdata",
                "operation": "HistoricalDataRequest",
                "extractor": "histdata",
                "securities": ["IBM US Equity"],
                "fields": ["PX_LAST"]
            }),
        );
        assert_eq!(
            plan_error(
                "bdh",
                json!({"tickers": ["X"], "fields": ["Y"], "includeSecurityErrors": true})
            )
            .message,
            "unknown field 'includeSecurityErrors'"
        );
    }

    #[test]
    fn bdib_defaults_event_type_and_interval() {
        assert_plan(
            "bdib",
            json!({"ticker": "IBM US Equity", "returnEids": true}),
            json!({
                "service": "//blp/refdata",
                "operation": "IntradayBarRequest",
                "extractor": "intraday_bar",
                "security": "IBM US Equity",
                "eventType": "TRADE",
                "interval": 1,
                "returnEids": true
            }),
        );
        assert_plan(
            "bdib",
            json!({
                "ticker": "IBM US Equity",
                "start": "2024-01-02T09:30:00",
                "end": "2024-01-02T10:00:00",
                "requestTz": "NY",
                "outputTz": "exchange",
                "eventType": "BID",
                "interval": 5,
                "kwargs": {"gapFillInitialBar": true},
                "onDelayed": "warn"
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "IntradayBarRequest",
                "extractor": "intraday_bar",
                "security": "IBM US Equity",
                "kwargs": [{"key": "gapFillInitialBar", "value": "true"}],
                "startDatetime": "2024-01-02T09:30:00",
                "endDatetime": "2024-01-02T10:00:00",
                "requestTz": "NY",
                "outputTz": "exchange",
                "eventType": "BID",
                "interval": 5,
                "onDelayed": "warn"
            }),
        );
    }

    #[test]
    fn bdib_dt_expands_to_the_full_naive_day() {
        assert_plan(
            "bdib",
            json!({"ticker": "IBM US Equity", "dt": "20240102", "interval": 10}),
            json!({
                "service": "//blp/refdata",
                "operation": "IntradayBarRequest",
                "extractor": "intraday_bar",
                "security": "IBM US Equity",
                "startDatetime": "2024-01-02T00:00:00",
                "endDatetime": "2024-01-02T23:59:59",
                "eventType": "TRADE",
                "interval": 10
            }),
        );
        for conflict in [
            json!({"start": "2024-01-02T09:30:00"}),
            json!({"end": "2024-01-02T10:00:00"}),
        ] {
            let mut args = json!({"ticker": "IBM US Equity", "dt": "20240102"});
            for (key, value) in conflict.as_object().unwrap() {
                args[key] = value.clone();
            }
            let error = plan_error("bdib", args);
            assert_eq!(error.kind, HostErrorKind::Argument);
            assert_eq!(
                error.message,
                "bdib dt cannot be combined with start/end options"
            );
        }
        let blank = plan_error("bdib", json!({"ticker": "X", "dt": "  "}));
        assert_eq!(blank.message, "dt must be a non-empty date-like value");
        let unformatted = plan_error("bdib", json!({"ticker": "X", "dt": "2024-01-02"}));
        assert_eq!(
            unformatted.message,
            "dt must be a YYYYMMDD date, got '2024-01-02'"
        );
        assert_eq!(
            plan_error("bdib", json!({"ticker": "X", "interval": -1})).kind,
            HostErrorKind::Argument
        );
    }

    #[test]
    fn bdtick_defaults_and_typed_flags_beat_kwargs() {
        assert_plan(
            "bdtick",
            json!({"ticker": "IBM US Equity"}),
            json!({
                "service": "//blp/refdata",
                "operation": "IntradayTickRequest",
                "extractor": "intraday_tick",
                "security": "IBM US Equity",
                "eventTypes": ["TRADE"]
            }),
        );
        assert_plan(
            "bdtick",
            json!({
                "ticker": "IBM US Equity",
                "start": "2024-01-02T09:30:00",
                "end": "2024-01-02T10:00:00",
                "includeConditionCodes": true,
                "includeExchangeCodes": true,
                "kwargs": {"customOption": "customValue"},
                "outputTz": "NY",
                "requestTz": "NY",
                "eventTypes": ["BID", "ASK"],
                "returnEids": true,
                "onDelayed": "ignore"
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "IntradayTickRequest",
                "extractor": "intraday_tick",
                "security": "IBM US Equity",
                "kwargs": [
                    {"key": "customOption", "value": "customValue"},
                    {"key": "includeConditionCodes", "value": "true"},
                    {"key": "includeExchangeCodes", "value": "true"}
                ],
                "startDatetime": "2024-01-02T09:30:00",
                "endDatetime": "2024-01-02T10:00:00",
                "requestTz": "NY",
                "outputTz": "NY",
                "eventTypes": ["BID", "ASK"],
                "returnEids": true,
                "onDelayed": "ignore"
            }),
        );
        assert_eq!(
            kwargs_of(
                "bdtick",
                json!({
                    "ticker": "IBM US Equity",
                    "includeConditionCodes": false,
                    "kwargs": {"includeConditionCodes": true, "other": 1}
                })
            ),
            json!([
                {"key": "includeConditionCodes", "value": "false"},
                {"key": "other", "value": "1"}
            ])
        );
        assert_eq!(
            kwargs_of(
                "bdtick",
                json!({
                    "ticker": "T",
                    "includeBloombergStandardConditionCodes": false,
                    "includeNonPlottableEvents": true,
                    "includeBicMicCodes": false,
                    "includeRpsCodes": true,
                    "includeBrokerCodes": false
                })
            ),
            json!([
                {"key": "includeBrokerCodes", "value": "false"},
                {"key": "includeRpsCodes", "value": "true"},
                {"key": "includeBicMicCodes", "value": "false"},
                {"key": "includeNonPlottableEvents", "value": "true"},
                {"key": "includeBloombergStandardConditionCodes", "value": "false"}
            ])
        );
        assert_eq!(
            kwargs_of("bdtick", json!({"ticker": "T", "kwargs": {}})),
            Value::Null
        );
    }

    #[test]
    fn bql_upserts_app_name_into_kwargs() {
        assert_plan(
            "bql",
            json!({"query": "get(px_last) for('IBM US Equity')"}),
            json!({
                "service": "//blp/bqlsvc",
                "operation": "sendQuery",
                "extractor": "bql",
                "elements": [{"key": "expression", "value": "get(px_last) for('IBM US Equity')"}]
            }),
        );
        assert_plan(
            "bql",
            json!({
                "query": "q",
                "appName": "per-call",
                "kwargs": {"clientContext.appName": "from-kwargs", "x": 1},
                "format": "long"
            }),
            json!({
                "service": "//blp/bqlsvc",
                "operation": "sendQuery",
                "extractor": "bql",
                "elements": [{"key": "expression", "value": "q"}],
                "kwargs": [
                    {"key": "clientContext.appName", "value": "per-call"},
                    {"key": "x", "value": "1"}
                ],
                "format": "long"
            }),
        );
        assert_eq!(
            kwargs_of("bql", json!({"query": "q", "appName": "app"})),
            json!([{"key": "clientContext.appName", "value": "app"}])
        );
        assert_eq!(
            kwargs_of("bql", json!({"query": "q", "kwargs": {}})),
            json!([])
        );
    }

    #[test]
    fn beqs_builds_screen_elements_with_defaults() {
        assert_plan(
            "beqs",
            json!({"screen": "Core Capital Ratios"}),
            json!({
                "service": "//blp/refdata",
                "operation": "BeqsRequest",
                "extractor": "generic",
                "elements": [
                    {"key": "screenName", "value": "Core Capital Ratios"},
                    {"key": "screenType", "value": "PRIVATE"},
                    {"key": "Group", "value": "General"}
                ]
            }),
        );
        assert_plan(
            "beqs",
            json!({
                "screen": "S",
                "asof": "20240131",
                "screenType": "GLOBAL",
                "group": "Mine",
                "overrides": [{"key": "PRICING_SOURCE", "value": "BGN"}],
                "kwargs": {"languageId": "ENGLISH"},
                "format": "long"
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "BeqsRequest",
                "extractor": "generic",
                "overrides": [{"key": "PRICING_SOURCE", "value": "BGN"}],
                "elements": [
                    {"key": "screenName", "value": "S"},
                    {"key": "screenType", "value": "GLOBAL"},
                    {"key": "Group", "value": "Mine"},
                    {"key": "asOfDate", "value": "20240131"}
                ],
                "kwargs": [{"key": "languageId", "value": "ENGLISH"}],
                "format": "long"
            }),
        );
        let blank_asof = plan("beqs", &json!({"screen": "S", "asof": " "}).to_string()).unwrap();
        assert_eq!(blank_asof.elements.unwrap().len(), 3);
    }

    #[test]
    fn per_security_overrides_are_rejected_where_ts_rejects_them() {
        let security =
            json!([{"security": "IBM US Equity", "overrides": [{"key": "A", "value": "1"}]}]);
        for (method, mut args) in [
            ("beqs", json!({"screen": "S"})),
            ("bsrch", json!({"searchSpec": "COMDTY:SEARCH"})),
            (
                "bport",
                json!({"portfolio": "U123-1 Client", "fields": ["PORTFOLIO_MWEIGHT"]}),
            ),
        ] {
            args["securityOverrides"] = security.clone();
            let error = plan_error(method, args);
            assert_eq!(error.kind, HostErrorKind::Argument, "{method}");
            assert_eq!(
                error.message,
                "Per-security overrides are only supported by bdp(), bdh(), and bds()"
            );
        }
    }

    #[test]
    fn bsrch_turns_overrides_and_kwargs_into_grid_elements() {
        assert_plan(
            "bsrch",
            json!({
                "searchSpec": "FI:SRCHEX",
                "overrides": [{"key": "LIMIT", "value": "20"}],
                "kwargs": {"Ticker": "IBM"},
                "format": "long"
            }),
            json!({
                "service": "//blp/exrsvc",
                "operation": "ExcelGetGridRequest",
                "extractor": "bsrch",
                "elements": [
                    {"key": "Domain", "value": "FI:SRCHEX"},
                    {"key": "LIMIT", "value": "20"},
                    {"key": "Ticker", "value": "IBM"}
                ],
                "format": "long"
            }),
        );
        assert_plan(
            "bsrch",
            json!({"searchSpec": "FI:SRCHEX"}),
            json!({
                "service": "//blp/exrsvc",
                "operation": "ExcelGetGridRequest",
                "extractor": "bsrch",
                "elements": [{"key": "Domain", "value": "FI:SRCHEX"}]
            }),
        );
    }

    #[test]
    fn bflds_picks_search_or_info_like_the_ts_wrapper() {
        assert_plan(
            "bflds",
            json!({"searchSpec": "last price", "fields": ["IGNORED"], "kwargs": {"returnFieldDocumentation": false}}),
            json!({
                "service": "//blp/apiflds",
                "operation": "FieldSearchRequest",
                "kwargs": [{"key": "returnFieldDocumentation", "value": "false"}],
                "searchSpec": "last price"
            }),
        );
        assert_plan(
            "bflds",
            json!({"fields": ["PX_LAST", "NAME"], "format": "long"}),
            json!({
                "service": "//blp/apiflds",
                "operation": "FieldInfoRequest",
                "fieldIds": ["PX_LAST", "NAME"],
                "format": "long"
            }),
        );
        assert_plan(
            "bflds",
            json!({}),
            json!({"service": "//blp/apiflds", "operation": "FieldInfoRequest", "fieldIds": []}),
        );
    }

    #[test]
    fn field_info_and_field_search_take_only_their_positional_value() {
        assert_plan(
            "fieldInfo",
            json!({"fields": ["PX_LAST"], "kwargs": {"x": 1}}),
            json!({
                "service": "//blp/apiflds",
                "operation": "FieldInfoRequest",
                "kwargs": [{"key": "x", "value": "1"}],
                "fieldIds": ["PX_LAST"]
            }),
        );
        assert_plan(
            "fieldSearch",
            json!({"searchSpec": "vwap", "format": "long"}),
            json!({
                "service": "//blp/apiflds",
                "operation": "FieldSearchRequest",
                "searchSpec": "vwap",
                "format": "long"
            }),
        );
        assert_eq!(
            plan_error("fieldInfo", json!({"fields": ["A"], "searchSpec": "x"})).message,
            "unknown field 'searchSpec'"
        );
        assert_eq!(
            plan_error("fieldSearch", json!({"searchSpec": "x", "fields": ["A"]})).message,
            "unknown field 'fields'"
        );
    }

    #[test]
    fn instrument_lookups_match_the_ts_wrappers() {
        for (method, operation) in [
            ("blkp", "instrumentListRequest"),
            ("bcurves", "curveListRequest"),
            ("bgovts", "govtListRequest"),
        ] {
            let key = if method == "blkp" { "query" } else { "ticker" };
            assert_plan(
                method,
                json!({key: "IBM", "kwargs": {"maxResults": 10}, "format": "long"}),
                json!({
                    "service": "//blp/instruments",
                    "operation": operation,
                    "elements": [{"key": "query", "value": "IBM"}],
                    "kwargs": [{"key": "maxResults", "value": "10"}],
                    "format": "long"
                }),
            );
        }
        for method in ["bcurves", "bgovts"] {
            let error = plan_error(
                method,
                json!({"ticker": "US", "overrides": [{"key": "A", "value": "1"}]}),
            );
            assert_eq!(error.kind, HostErrorKind::Argument);
            assert_eq!(
                error.message,
                format!("{method}() does not support overrides")
            );
            assert!(
                plan(
                    method,
                    &json!({"ticker": "US", "overrides": [], "securityOverrides": []}).to_string()
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn bport_sends_the_portfolio_as_security() {
        assert_plan(
            "bport",
            json!({
                "portfolio": "U12345-1 Client",
                "fields": ["PORTFOLIO_MWEIGHT"],
                "overrides": [{"key": "REFERENCE_DATE", "value": "20240131"}],
                "kwargs": {"k": "v"}
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "PortfolioDataRequest",
                "security": "U12345-1 Client",
                "fields": ["PORTFOLIO_MWEIGHT"],
                "overrides": [{"key": "REFERENCE_DATE", "value": "20240131"}],
                "kwargs": [{"key": "k", "value": "v"}]
            }),
        );
    }

    #[test]
    fn cdx_helpers_plan_bdp_over_the_generated_field_bundles() {
        let info = plan(
            "acdxInfo",
            &json!({"ticker": "CDX IG CDSI GEN 5Y Corp"}).to_string(),
        )
        .unwrap();
        assert_eq!(
            info.securities,
            Some(vec!["CDX IG CDSI GEN 5Y Corp".to_owned()])
        );
        assert_eq!(
            info.fields.as_deref().unwrap(),
            [
                "ROLLING_SERIES",
                "VERSION",
                "ON_THE_RUN_CURRENT_BD_INDICATOR",
                "CDS_FIRST_ACCRUAL_START_DATE",
                "NAME",
                "NUM_CURRENT_COMPANIES_CCY_TKR",
                "NUM_ORIG_COMPANIES_CRNCY_TKR",
                "PX_LAST"
            ]
        );
        assert_eq!(info.extractor.as_deref(), Some("refdata"));
        assert_eq!(info.include_security_errors, Some(false));
        assert_eq!(
            plan_error("acdxInfo", json!({"ticker": "T", "recoveryRate": 40})).message,
            "unknown field 'recoveryRate'"
        );

        let risk = plan("acdxRisk", &json!({"ticker": "T"}).to_string()).unwrap();
        assert_eq!(risk.fields.as_ref().map(Vec::len), Some(6));
        assert_eq!(risk.overrides, None);
        let pricing = plan(
            "acdxPricing",
            &json!({"ticker": "T", "onDelayed": "warn"}).to_string(),
        )
        .unwrap();
        assert_eq!(pricing.fields.as_ref().map(Vec::len), Some(10));
        assert_eq!(pricing.on_delayed.as_deref(), Some("warn"));
    }

    #[test]
    fn cdx_recovery_rate_upserts_cds_rr_as_a_js_number() {
        assert_plan(
            "acdxPricing",
            json!({
                "ticker": "T",
                "recoveryRate": 40.0,
                "overrides": [{"key": "CDS_RR", "value": "25"}, {"key": "X", "value": "1"}],
                "securityOverrides": [{"security": "T", "overrides": [{"key": "Y", "value": "2"}]}],
                "returnEids": true
            }),
            json!({
                "service": "//blp/refdata",
                "operation": "ReferenceDataRequest",
                "extractor": "refdata",
                "securities": ["T"],
                "fields": [
                    "PX_LAST", "PX_BID", "PX_ASK", "UPFRONT_LAST", "UPFRONT_BID", "UPFRONT_ASK",
                    "CDS_FLAT_SPREAD", "UPFRONT_FEE", "PV_CDS_PREMIUM_LEG", "PV_CDS_DEFAULT_LEG"
                ],
                "overrides": [{"key": "CDS_RR", "value": "40"}, {"key": "X", "value": "1"}],
                "securityOverrides": [{"security": "T", "overrides": [{"key": "Y", "value": "2"}]}],
                "includeSecurityErrors": false,
                "returnEids": true
            }),
        );
        let appended = plan(
            "acdxRisk",
            &json!({"ticker": "T", "recoveryRate": 37.5, "overrides": [{"key": "X", "value": "1"}]}).to_string(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(appended.overrides).unwrap(),
            json!([{"key": "X", "value": "1"}, {"key": "CDS_RR", "value": "37.5"}])
        );
    }

    #[test]
    fn required_arguments_are_named_in_argument_errors() {
        for (method, missing) in [
            ("bdp", "tickers"),
            ("bds", "tickers"),
            ("bdh", "tickers"),
            ("bdib", "ticker"),
            ("bdtick", "ticker"),
            ("bql", "query"),
            ("beqs", "screen"),
            ("bsrch", "searchSpec"),
            ("fieldInfo", "fields"),
            ("fieldSearch", "searchSpec"),
            ("blkp", "query"),
            ("bport", "portfolio"),
            ("bcurves", "ticker"),
            ("bgovts", "ticker"),
            ("acdxInfo", "ticker"),
            ("acdxPricing", "ticker"),
            ("acdxRisk", "ticker"),
        ] {
            let error = plan_error(method, json!({}));
            assert_eq!(error.kind, HostErrorKind::Argument, "{method}");
            assert_eq!(
                error.message,
                format!("missing field '{missing}'"),
                "{method}"
            );
        }
    }

    #[test]
    fn kwargs_values_must_be_scalars() {
        let error = plan_error(
            "bdp",
            json!({"tickers": [], "fields": [], "kwargs": {"a": null}}),
        );
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert!(
            error.message.contains("a string, number or boolean"),
            "{}",
            error.message
        );
    }
}
