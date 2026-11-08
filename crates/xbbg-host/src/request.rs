//! Request planning shared by host bindings.
//!
//! Two layers:
//! - [`RequestInput`]: the JSON form of the TS `RequestInput` (inventory §2.2), the
//!   argument of the generic `request` call. [`RequestInput::into_request_params`]
//!   applies the TS `request()` checks and napi's conversion (`jsonElements`
//!   flattening, pair lists, `RequestParamsInput::into_request_params`).
//! - [`plan`]: the typed request wrappers (`bdp`, `bdh`, `bta`, ...). Each takes one
//!   JSON argument object and returns the [`RequestInput`] the TS wrapper passes to
//!   `request()`, after `mapOverridesToRequestParts`.
//!
//! # Argument objects
//!
//! Keys are camelCase; unknown keys are rejected (ARGUMENT). JS positional
//! arguments become named keys; every other key is the TS option of the same
//! name. Shared value shapes:
//! - `overrides`: `[{"key", "value"}]` global pairs; `securityOverrides`:
//!   `[{"security", "overrides": [{"key", "value"}]}]`. Both come already merged
//!   by the host's `ovr()` port; empty lists are dropped from the plan.
//! - `kwargs`: an object of `string | number | boolean` values, kept in key order.
//!   Values are stringified like JavaScript `String(value)` (`2.0` → `"2"`).
//! - Dates are host-formatted before they arrive: `YYYYMMDD` for date options
//!   (`start`, `end`, `asof`, `dt`, `startDate`, `endDate`, the bta `kwargs`
//!   date keys), the host's `formatDateTime` text for datetime options. The
//!   planners never parse dates.
//! - `onDelayed`: `"warn" | "raise" | "ignore"`, checked exactly by
//!   [`RequestInput::into_request_params`].
//!
//! | Method | Required keys | Optional keys |
//! |---|---|---|
//! | `bdp` | `tickers`, `fields` | `overrides`, `securityOverrides`, `kwargs`, `format`, `includeSecurityErrors`, `returnEids`, `onDelayed`, `validateFields` |
//! | `bds` | `tickers`, `fields` | `overrides`, `securityOverrides`, `kwargs`, `format`, `returnEids`, `validateFields` |
//! | `bdh` | `tickers`, `fields` | `start`, `end`, `overrides`, `securityOverrides`, `kwargs`, `format`, `returnEids`, `onDelayed`, `validateFields` |
//! | `bdib` | `ticker` | `start`, `end`, `dt`, `requestTz`, `outputTz`, `eventType`, `interval`, `kwargs`, `returnEids`, `onDelayed` |
//! | `bdtick` | `ticker` | `start`, `end`, `requestTz`, `outputTz`, `eventTypes`, `includeConditionCodes`, `includeExchangeCodes`, `includeBrokerCodes`, `includeRpsCodes`, `includeBicMicCodes`, `includeNonPlottableEvents`, `includeBloombergStandardConditionCodes`, `kwargs`, `returnEids`, `onDelayed` |
//! | `bql` | `query` | `appName`, `kwargs`, `format` |
//! | `beqs` | `screen` | `asof`, `screenType`, `group`, `overrides`, `securityOverrides`, `kwargs`, `format` |
//! | `bsrch` | `searchSpec` | `overrides`, `securityOverrides`, `kwargs`, `format` |
//! | `bta` | `ticker`, `study` (string, or an object of study params) | `studyParams`, `kwargs`, `startDate`, `endDate`, `periodicity`, `interval`, `format` |
//! | `bflds` | — | `fields`, `searchSpec`, `kwargs`, `format` |
//! | `fieldInfo` | `fields` | `kwargs`, `format` |
//! | `fieldSearch` | `searchSpec` | `kwargs`, `format` |
//! | `blkp` | `query` | `kwargs`, `format` |
//! | `bport` | `portfolio`, `fields` | `overrides`, `securityOverrides`, `kwargs`, `format` |
//! | `bcurves`, `bgovts` | `ticker` | `overrides`, `securityOverrides` (must be empty), `kwargs`, `format` |
//! | `acdxInfo` | `ticker` | the `bdp` options |
//! | `acdxPricing`, `acdxRisk` | `ticker` | the `bdp` options, `recoveryRate` (percent) |
//!
//! # Decisions where the TS behaviour is a quirk (inventory Appendix B)
//! - `bdib` `dt` (the JS `abdib` positional day): sets the full-day range
//!   `YYYY-MM-DDT00:00:00`..`T23:59:59`; combining it with `start`/`end` is an
//!   ARGUMENT error instead of silently ignoring `dt`. `interval` is an explicit key,
//!   so no positional default can override it (B10).
//! - `bdh` `start`/`end` are explicit keys; nothing positional replaces them (B10).
//! - `fieldInfo`/`fieldSearch` take their positional value only; `searchSpec`
//!   respectively `fields` are rejected instead of overriding it (B8).
//! - `bcurves`/`bgovts` reject non-empty overrides instead of dropping them (B9).
//! - `bta`: `interval` from `kwargs` or the study object is consumed by the intraday
//!   data range instead of also leaking into the raw elements or study attributes,
//!   and `studyParams.length` aliases `period` like the study object's `length`
//!   (B7).

mod ta;
mod typed;
mod values;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use xbbg_async::engine::{RequestParams, RequestParamsInput};

use crate::errors::HostError;

/// One `{key, value}` pair (TS `StringPair`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StringPair {
    pub key: String,
    pub value: String,
}

impl StringPair {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Overrides that apply to one security only (TS `SecurityOverrideSpec`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecurityOverride {
    pub security: String,
    pub overrides: Vec<StringPair>,
}

/// The generic request (TS `RequestInput` as napi receives it, without `backend`).
///
/// Serializes to the same camelCase JSON it reads, omitting absent fields.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestInput {
    #[serde(default)]
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub securities: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overrides: Option<Vec<StringPair>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_overrides: Option<Vec<SecurityOverride>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elements: Option<Vec<StringPair>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<Vec<StringPair>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_elements: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_datetime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_datetime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_tz: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tz: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_types: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<StringPair>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_types: Option<Vec<StringPair>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_security_errors: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_eids: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_delayed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_fields: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_spec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

const ON_DELAYED_MESSAGE: &str = "onDelayed must be warn, raise, or ignore";

impl RequestInput {
    /// Parse the JSON argument of the generic `request` call (ARGUMENT on a bad shape).
    pub fn from_json(args: &str) -> Result<Self, HostError> {
        crate::errors::parse_args(args)
    }

    /// The planned request as JSON (what `requests.plan.*` test hooks return).
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("RequestInput serializes to JSON")
    }

    /// Apply the TS `request()` checks, then napi's conversion to the engine input.
    ///
    /// `onDelayed` must be exactly `warn`, `raise` or `ignore` (VALIDATION, element
    /// `onDelayed`). `jsonElements` is flattened after `elements` with napi's rules;
    /// a bad payload is VALIDATION with element `jsonElements`. `kwargs` and
    /// `fieldTypes` become maps (a repeated key keeps its last value).
    pub fn into_params_input(self) -> Result<RequestParamsInput, HostError> {
        if let Some(policy) = self.on_delayed.as_deref()
            && !matches!(policy, "warn" | "raise" | "ignore")
        {
            return Err(HostError::validation(ON_DELAYED_MESSAGE).with_element("onDelayed"));
        }

        let mut elements = pairs_to_tuples(self.elements);
        if let Some(raw_json) = self.json_elements.as_deref() {
            let flattened = flatten_json_elements(raw_json)?;
            elements.get_or_insert_with(Vec::new).extend(flattened);
        }

        Ok(RequestParamsInput {
            service: self.service,
            operation: self.operation,
            request_operation: self.request_operation,
            request_id: self.request_id,
            extractor: self.extractor,
            securities: self.securities,
            security: self.security,
            fields: self.fields,
            overrides: pairs_to_tuples(self.overrides),
            security_overrides: self.security_overrides.map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| {
                        (
                            entry.security,
                            pairs_to_tuples(Some(entry.overrides)).unwrap_or_default(),
                        )
                    })
                    .collect()
            }),
            elements,
            kwargs: pairs_to_map(self.kwargs),
            start_date: self.start_date,
            end_date: self.end_date,
            start_datetime: self.start_datetime,
            end_datetime: self.end_datetime,
            request_tz: self.request_tz,
            output_tz: self.output_tz,
            event_type: self.event_type,
            event_types: self.event_types,
            interval: self.interval,
            options: pairs_to_tuples(self.options),
            field_types: pairs_to_map(self.field_types),
            include_security_errors: self.include_security_errors,
            return_eids: self.return_eids,
            on_delayed: self.on_delayed,
            validate_fields: self.validate_fields,
            search_spec: self.search_spec,
            field_ids: self.field_ids,
            format: self.format,
        })
    }

    /// [`Self::into_params_input`], then the engine's own normalization and
    /// defaults; its input errors are VALIDATION with the engine's text.
    pub fn into_request_params(self) -> Result<RequestParams, HostError> {
        self.into_params_input()?
            .into_request_params()
            .map_err(|error| HostError::validation(error.detail()))
    }
}

/// Typed request methods [`plan`] accepts (the contract's V1 dispatch names).
pub const METHODS: &[&str] = &[
    "bdp",
    "bds",
    "bdh",
    "bdib",
    "bdtick",
    "bql",
    "beqs",
    "bsrch",
    "bta",
    "bflds",
    "fieldInfo",
    "fieldSearch",
    "blkp",
    "bport",
    "bcurves",
    "bgovts",
    "acdxInfo",
    "acdxPricing",
    "acdxRisk",
];

/// Plan the typed request `method` from its JSON argument object (module docs).
///
/// Errors: ARGUMENT for a malformed argument object or a combination the wrapper
/// rejects (per-security overrides where unsupported, `dt` with `start`/`end`, …);
/// INTERNAL `unknown engine method '<name>'` for a name outside [`METHODS`].
pub fn plan(method: &str, args: &str) -> Result<RequestInput, HostError> {
    use crate::errors::parse_args;

    match method {
        "bdp" => Ok(typed::bdp(parse_args(args)?)),
        "bds" => Ok(typed::bds(parse_args(args)?)),
        "bdh" => Ok(typed::bdh(parse_args(args)?)),
        "bdib" => typed::bdib(parse_args(args)?),
        "bdtick" => Ok(typed::bdtick(parse_args(args)?)),
        "bql" => Ok(typed::bql(parse_args(args)?)),
        "beqs" => typed::beqs(parse_args(args)?),
        "bsrch" => typed::bsrch(parse_args(args)?),
        "bta" => Ok(ta::bta(parse_args(args)?)),
        "bflds" => Ok(typed::bflds(parse_args(args)?)),
        "fieldInfo" => Ok(typed::field_info(parse_args(args)?)),
        "fieldSearch" => Ok(typed::field_search(parse_args(args)?)),
        "blkp" => Ok(typed::blkp(parse_args(args)?)),
        "bport" => typed::bport(parse_args(args)?),
        "bcurves" => typed::instrument_list("bcurves", "curveListRequest", parse_args(args)?),
        "bgovts" => typed::instrument_list("bgovts", "govtListRequest", parse_args(args)?),
        "acdxInfo" => Ok(typed::acdx_info(parse_args(args)?)),
        "acdxPricing" => Ok(typed::acdx_pricing(parse_args(args)?)),
        "acdxRisk" => Ok(typed::acdx_risk(parse_args(args)?)),
        other => Err(HostError::unknown_method("engine", other)),
    }
}

fn pairs_to_tuples(pairs: Option<Vec<StringPair>>) -> Option<Vec<(String, String)>> {
    pairs.map(|pairs| {
        pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect()
    })
}

fn pairs_to_map(pairs: Option<Vec<StringPair>>) -> Option<HashMap<String, String>> {
    pairs.map(|pairs| {
        pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect()
    })
}

/// napi `jsonElements` flattening: nested objects become dotted keys (in the
/// order `serde_json` iterates them: sorted by key), an array at a top-level key
/// repeats the key once per item, an array below the top level becomes one JSON
/// text value, scalars become text (`null` → `"null"`), empty objects emit nothing.
fn flatten_json_elements(raw_json: &str) -> Result<Vec<(String, String)>, HostError> {
    let value: serde_json::Value = serde_json::from_str(raw_json).map_err(|error| {
        HostError::validation(format!("invalid jsonElements payload: {error}"))
            .with_element("jsonElements")
    })?;
    let mut out = Vec::new();
    flatten_json_value(None, &value, &mut out)?;
    Ok(out)
}

fn flatten_json_value(
    path: Option<&str>,
    value: &serde_json::Value,
    out: &mut Vec<(String, String)>,
) -> Result<(), HostError> {
    let top_level_error = || {
        HostError::validation("jsonElements must be a JSON object at the top level")
            .with_element("jsonElements")
    };
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, child) in entries {
                let next_path = match path {
                    Some(prefix) if !prefix.is_empty() => format!("{prefix}.{key}"),
                    _ => key.clone(),
                };
                flatten_json_value(Some(&next_path), child, out)?;
            }
            Ok(())
        }
        serde_json::Value::Array(items) => {
            let path = path.ok_or_else(top_level_error)?;
            if path.contains('.') {
                out.push((path.to_owned(), value.to_string()));
            } else {
                for item in items {
                    out.push((path.to_owned(), json_scalar_text(item)));
                }
            }
            Ok(())
        }
        _ => {
            let path = path.ok_or_else(top_level_error)?;
            out.push((path.to_owned(), json_scalar_text(value)));
            Ok(())
        }
    }
}

fn json_scalar_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::HostErrorKind;

    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn request_input_round_trips_camel_case_json_without_absent_fields() {
        let json = r#"{"service":"//blp/refdata","operation":"ReferenceDataRequest","securities":["IBM US Equity"],"fields":["PX_LAST"],"overrides":[{"key":"EQY_FUND_CRNCY","value":"EUR"}],"securityOverrides":[{"security":"IBM US Equity","overrides":[{"key":"CRNCY","value":"USD"}]}],"kwargs":[{"key":"a","value":"1"}],"jsonElements":"{}","startDate":"20240101","endDate":"20240131","interval":5,"fieldTypes":[{"key":"PX_LAST","value":"float64"}],"includeSecurityErrors":true,"returnEids":false,"onDelayed":"warn","validateFields":true,"format":"long"}"#;
        let input = RequestInput::from_json(json).unwrap();
        assert_eq!(
            input.security_overrides.as_ref().unwrap()[0].security,
            "IBM US Equity"
        );
        assert_eq!(input.interval, Some(5));
        assert_eq!(input.to_json(), json);
        assert_eq!(RequestInput::default().to_json(), r#"{"service":""}"#);
    }

    #[test]
    fn request_input_rejects_unknown_fields_and_bad_types_as_argument() {
        let unknown = RequestInput::from_json(r#"{"service":"s","backend":"arrow"}"#).unwrap_err();
        assert_eq!(unknown.kind, HostErrorKind::Argument);
        assert_eq!(unknown.message, "unknown field 'backend'");

        let negative = RequestInput::from_json(r#"{"service":"s","interval":-1}"#).unwrap_err();
        assert_eq!(negative.kind, HostErrorKind::Argument);
    }

    #[test]
    fn on_delayed_is_checked_exactly_like_the_ts_request_guard() {
        for policy in ["warn", "raise", "ignore"] {
            let input = RequestInput {
                service: "//blp/refdata".to_owned(),
                operation: Some("ReferenceDataRequest".to_owned()),
                on_delayed: Some(policy.to_owned()),
                ..RequestInput::default()
            };
            assert!(input.into_params_input().is_ok(), "{policy}");
        }
        for policy in ["WARN", " warn", "", "skip"] {
            let error = RequestInput {
                on_delayed: Some(policy.to_owned()),
                ..RequestInput::default()
            }
            .into_params_input()
            .unwrap_err();
            assert_eq!(error.kind, HostErrorKind::Validation, "{policy:?}");
            assert_eq!(error.message, ON_DELAYED_MESSAGE);
            assert_eq!(error.context.element.as_deref(), Some("onDelayed"));
        }
    }

    #[test]
    fn conversion_keeps_pairs_in_order_and_maps_kwargs_last_wins() {
        let input = RequestInput {
            service: "//blp/refdata".to_owned(),
            operation: Some("ReferenceDataRequest".to_owned()),
            overrides: Some(vec![StringPair::new("B", "1"), StringPair::new("A", "2")]),
            security_overrides: Some(vec![SecurityOverride {
                security: "IBM US Equity".to_owned(),
                overrides: vec![StringPair::new("C", "3")],
            }]),
            elements: Some(vec![StringPair::new("e", "1")]),
            kwargs: Some(vec![
                StringPair::new("k", "old"),
                StringPair::new("k", "new"),
            ]),
            options: Some(vec![StringPair::new("o", "v")]),
            field_types: Some(vec![StringPair::new("PX_LAST", "float64")]),
            ..RequestInput::default()
        };
        let params = input.into_params_input().unwrap();
        assert_eq!(params.overrides, Some(pairs(&[("B", "1"), ("A", "2")])));
        assert_eq!(
            params.security_overrides,
            Some(vec![("IBM US Equity".to_owned(), pairs(&[("C", "3")]))])
        );
        assert_eq!(params.elements, Some(pairs(&[("e", "1")])));
        assert_eq!(params.kwargs.unwrap()["k"], "new");
        assert_eq!(params.options, Some(pairs(&[("o", "v")])));
        assert_eq!(params.field_types.unwrap()["PX_LAST"], "float64");
    }

    #[test]
    fn json_elements_flatten_after_elements_with_napi_rules() {
        let input = RequestInput {
            elements: Some(vec![StringPair::new("first", "1")]),
            json_elements: Some(
                r#"{"z": {"b": 2, "a": [1, {"x": null}]}, "list": ["s", 3, true, null, [1], {"k": 1}], "flag": false, "empty": {}, "none": null}"#
                    .to_owned(),
            ),
            ..RequestInput::default()
        };
        let params = input.into_params_input().unwrap();
        assert_eq!(
            params.elements,
            Some(pairs(&[
                ("first", "1"),
                ("flag", "false"),
                ("list", "s"),
                ("list", "3"),
                ("list", "true"),
                ("list", "null"),
                ("list", "[1]"),
                ("list", r#"{"k":1}"#),
                ("none", "null"),
                ("z.a", r#"[1,{"x":null}]"#),
                ("z.b", "2"),
            ]))
        );

        let empty = RequestInput {
            json_elements: Some("{}".to_owned()),
            ..RequestInput::default()
        };
        assert_eq!(
            empty.into_params_input().unwrap().elements,
            Some(Vec::new())
        );
    }

    #[test]
    fn json_elements_errors_are_validation_with_napi_text() {
        let invalid = RequestInput {
            json_elements: Some("{".to_owned()),
            ..RequestInput::default()
        }
        .into_params_input()
        .unwrap_err();
        assert_eq!(invalid.kind, HostErrorKind::Validation);
        assert!(
            invalid
                .message
                .starts_with("invalid jsonElements payload: "),
            "{}",
            invalid.message
        );
        assert_eq!(invalid.context.element.as_deref(), Some("jsonElements"));

        for top_level in ["[1]", "3", "\"x\"", "null"] {
            let error = RequestInput {
                json_elements: Some(top_level.to_owned()),
                ..RequestInput::default()
            }
            .into_params_input()
            .unwrap_err();
            assert_eq!(
                error.message, "jsonElements must be a JSON object at the top level",
                "{top_level}"
            );
        }
    }

    #[test]
    fn into_request_params_reports_engine_input_errors_as_validation() {
        let error = RequestInput {
            service: "//blp/refdata".to_owned(),
            ..RequestInput::default()
        }
        .into_request_params()
        .unwrap_err();
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(
            error.message,
            "operation is required unless request_operation is used for RawRequest"
        );

        let extractor = RequestInput {
            service: "//blp/refdata".to_owned(),
            operation: Some("ReferenceDataRequest".to_owned()),
            extractor: Some("nope".to_owned()),
            ..RequestInput::default()
        }
        .into_request_params()
        .unwrap_err();
        assert_eq!(extractor.message, "invalid extractor type: nope");

        let params = RequestInput {
            service: String::new(),
            operation: Some("ReferenceDataRequest".to_owned()),
            securities: Some(vec!["IBM US Equity".to_owned()]),
            fields: Some(vec!["PX_LAST".to_owned()]),
            on_delayed: Some("raise".to_owned()),
            ..RequestInput::default()
        }
        .into_request_params()
        .unwrap();
        assert_eq!(params.service, "//blp/refdata");
        assert!(params.on_delayed.is_some());
    }

    #[test]
    fn plan_rejects_unknown_methods_as_internal() {
        let error = plan("bogus", "{}").unwrap_err();
        assert_eq!(error.kind, HostErrorKind::Internal);
        assert_eq!(error.message, "unknown engine method 'bogus'");
    }

    #[test]
    fn every_method_name_is_planned() {
        for method in METHODS {
            let error = plan(method, "3").unwrap_err();
            assert_eq!(
                error.kind,
                HostErrorKind::Argument,
                "{method}: {}",
                error.message
            );
        }
    }
}
