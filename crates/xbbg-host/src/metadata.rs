//! Decoded result metadata.
//!
//! [`decode`] reads the `xbbg.*` schema-metadata keys of a result once,
//! natively, replacing `@xbbg/core`'s `attachResultMetadata`. The raw keys are
//! still passed to the host unchanged; this is the parsed view.
//!
//! [`DecodedMetadata::to_json`] produces one camelCase object; members are
//! omitted when their key is absent or (for the lenient members) malformed:
//!
//! ```json
//! {
//!   "bloombergRequestId": "sdk-1",
//!   "bloombergRequestIds": ["sdk-1"],
//!   "eidData": {"IBM US Equity": [14005, 35009]},
//!   "delayed": {"IBM US Equity": {"delayed": true, "delayMinutes": 15}},
//!   "securityErrors": {"BAD Equity": {"category": "BAD_SEC", "code": "43",
//!                      "subcategory": "INVALID_SECURITY", "message": "Unknown/Invalid security"}},
//!   "fieldExceptions": {"IBM US Equity": [{"field": "BAD_FLD", "category": "BAD_FLD",
//!                       "code": 9, "subcategory": "INVALID_FIELD", "message": "Field not valid"}]},
//!   "fieldExceptionsError": "Invalid xbbg.field_exceptions metadata: ...",
//!   "fieldErrors": {"BAD_FLD": {"category": "BAD_FLD", "code": 9, "subcategory": "NOT_FOUND",
//!                   "message": "Unknown field", "source": "apiflds"}},
//!   "fieldErrorsError": "Invalid xbbg.field_errors metadata: ...",
//!   "typeMisses": {"PX_LAST": 2}
//! }
//! ```
//!
//! - `bloombergRequestId`: `xbbg.bloomberg_request_id` verbatim.
//! - `bloombergRequestIds`: `[bloombergRequestId]` when the singular key is
//!   present, otherwise the `xbbg.bloomberg_request_ids` string array.
//! - `eidData` (`xbbg.eid_data`): security to integer EIDs.
//! - `delayed` (`xbbg.delayed`): present only when the request used
//!   `onDelayed`; `delayMinutes` is `null` when real time or not reported.
//! - `securityErrors` (`xbbg.security_errors`): each member optional; a numeric
//!   `code` is rendered as decimal text.
//! - `fieldExceptions` (`xbbg.field_exceptions`): the version-1 dictionary
//!   envelope (or the legacy per-security map) expanded to ordered occurrence
//!   lists per security, decoded with [`FieldExceptions::from_json`].
//! - `fieldErrors` (`xbbg.field_errors`): field id to its refusal.
//! - `typeMisses` (`xbbg.type_misses`): field to the count of non-null values
//!   that could not convert to the column type.
//!
//! Lenient members (`bloombergRequestIds`, `eidData`, `delayed`,
//! `securityErrors`, `typeMisses`) are omitted when malformed, as in
//! `@xbbg/core`. The strict members (`fieldExceptions`, `fieldErrors`) instead
//! report malformed input in `fieldExceptionsError` / `fieldErrorsError` with
//! the decoding error, so a host can raise it when the member is read rather
//! than failing the whole result.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use serde_json::{Map, Value};
use xbbg_async::diagnostics::{
    FieldErrorMeta, FieldExceptionMeta, FieldExceptions, METADATA_KEY_FIELD_ERRORS,
    METADATA_KEY_FIELD_EXCEPTIONS,
};
use xbbg_async::engine::state::{
    METADATA_KEY_BLOOMBERG_REQUEST_ID, METADATA_KEY_BLOOMBERG_REQUEST_IDS, METADATA_KEY_DELAYED,
    METADATA_KEY_EID_DATA, METADATA_KEY_SECURITY_ERRORS, METADATA_KEY_TYPE_MISSES,
};

/// Delayed-data status for one security.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayedInfo {
    pub delayed: bool,
    pub delay_minutes: Option<i64>,
}

/// A per-security `securityError`; every member is optional.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityError {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subcategory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// The parsed view of a result's `xbbg.*` schema metadata.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecodedMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloomberg_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloomberg_request_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eid_data: Option<BTreeMap<String, Vec<i64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delayed: Option<BTreeMap<String, DelayedInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_errors: Option<BTreeMap<String, SecurityError>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_exceptions: Option<BTreeMap<String, Vec<FieldExceptionMeta>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_exceptions_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_errors: Option<BTreeMap<String, FieldErrorMeta>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_errors_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_misses: Option<BTreeMap<String, u64>>,
}

impl DecodedMetadata {
    /// The camelCase JSON object described in the module docs.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("decoded metadata strings and numbers serialize")
    }
}

/// Decode every known `xbbg.*` key; unknown keys are ignored.
pub fn decode(metadata: &HashMap<String, String>) -> DecodedMetadata {
    let bloomberg_request_id = metadata.get(METADATA_KEY_BLOOMBERG_REQUEST_ID).cloned();
    let bloomberg_request_ids = match &bloomberg_request_id {
        Some(id) => Some(vec![id.clone()]),
        None => lenient(metadata, METADATA_KEY_BLOOMBERG_REQUEST_IDS, string_array),
    };
    let (field_exceptions, field_exceptions_error) = match field_exceptions(metadata) {
        Some(Ok(value)) => (Some(value), None),
        Some(Err(error)) => (None, Some(error)),
        None => (None, None),
    };
    let (field_errors, field_errors_error) = match field_errors(metadata) {
        Some(Ok(value)) => (Some(value), None),
        Some(Err(error)) => (None, Some(error)),
        None => (None, None),
    };
    DecodedMetadata {
        bloomberg_request_id,
        bloomberg_request_ids,
        eid_data: lenient(metadata, METADATA_KEY_EID_DATA, |value| {
            object_of(value, |entry| {
                entry.as_array()?.iter().map(Value::as_i64).collect()
            })
        }),
        delayed: lenient(metadata, METADATA_KEY_DELAYED, |value| {
            object_of(value, delayed_info)
        }),
        security_errors: lenient(metadata, METADATA_KEY_SECURITY_ERRORS, |value| {
            object_of(value, security_error)
        }),
        field_exceptions,
        field_exceptions_error,
        field_errors,
        field_errors_error,
        type_misses: lenient(metadata, METADATA_KEY_TYPE_MISSES, |value| {
            object_of(value, Value::as_u64)
        }),
    }
}

/// Parse `key` as JSON and map it, treating any failure as absent.
fn lenient<T>(
    metadata: &HashMap<String, String>,
    key: &str,
    map: impl FnOnce(&Value) -> Option<T>,
) -> Option<T> {
    let value: Value = serde_json::from_str(metadata.get(key)?).ok()?;
    map(&value)
}

fn string_array(value: &Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|id| id.as_str().map(str::to_owned))
        .collect()
}

/// A JSON object whose every value maps; one failing value rejects all.
fn object_of<T>(
    value: &Value,
    mut map: impl FnMut(&Value) -> Option<T>,
) -> Option<BTreeMap<String, T>> {
    value
        .as_object()?
        .iter()
        .map(|(key, entry)| Some((key.clone(), map(entry)?)))
        .collect()
}

fn delayed_info(value: &Value) -> Option<DelayedInfo> {
    let object = value.as_object()?;
    let delayed = object.get("delayed")?.as_bool()?;
    let delay_minutes = match object.get("delay_minutes")? {
        Value::Null => None,
        minutes => Some(minutes.as_i64()?),
    };
    Some(DelayedInfo {
        delayed,
        delay_minutes,
    })
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Option<Option<String>> {
    match object.get(key) {
        None => Some(None),
        Some(Value::String(text)) => Some(Some(text.clone())),
        Some(_) => None,
    }
}

fn security_error(value: &Value) -> Option<SecurityError> {
    let object = value.as_object()?;
    let code = match object.get("code") {
        None => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(_) => return None,
    };
    Some(SecurityError {
        category: optional_string(object, "category")?,
        code,
        subcategory: optional_string(object, "subcategory")?,
        message: optional_string(object, "message")?,
    })
}

fn field_exceptions(
    metadata: &HashMap<String, String>,
) -> Option<Result<BTreeMap<String, Vec<FieldExceptionMeta>>, String>> {
    let raw = metadata.get(METADATA_KEY_FIELD_EXCEPTIONS)?;
    let invalid =
        |detail: &str| format!("Invalid {METADATA_KEY_FIELD_EXCEPTIONS} metadata: {detail}");
    Some(match serde_json::from_str::<Value>(raw) {
        Err(_) => Err(invalid("expected valid JSON")),
        Ok(value) if !value.is_object() => Err(invalid("expected an object")),
        Ok(_) => FieldExceptions::from_json(raw)
            .map(|exceptions| exceptions.expanded())
            .map_err(|error| invalid(&error.to_string())),
    })
}

fn field_errors(
    metadata: &HashMap<String, String>,
) -> Option<Result<BTreeMap<String, FieldErrorMeta>, String>> {
    let raw = metadata.get(METADATA_KEY_FIELD_ERRORS)?;
    let invalid = |detail: &str| format!("Invalid {METADATA_KEY_FIELD_ERRORS} metadata: {detail}");
    Some(match serde_json::from_str::<Value>(raw) {
        Err(_) => Err(invalid("expected valid JSON")),
        Ok(value) => serde_json::from_value(value).map_err(|_| invalid("malformed field errors")),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn metadata(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn decoded_json(pairs: &[(&str, &str)]) -> Value {
        serde_json::from_str(&decode(&metadata(pairs)).to_json()).unwrap()
    }

    #[test]
    fn absent_keys_decode_to_an_empty_object() {
        assert_eq!(decode(&HashMap::new()), DecodedMetadata::default());
        assert_eq!(decoded_json(&[("unrelated", "x")]), json!({}));
    }

    #[test]
    fn singular_request_id_wins_over_the_array() {
        assert_eq!(
            decoded_json(&[
                (METADATA_KEY_BLOOMBERG_REQUEST_ID, "sdk-1"),
                (METADATA_KEY_BLOOMBERG_REQUEST_IDS, r#"["a","b"]"#),
            ]),
            json!({"bloombergRequestId": "sdk-1", "bloombergRequestIds": ["sdk-1"]})
        );
        assert_eq!(
            decoded_json(&[(METADATA_KEY_BLOOMBERG_REQUEST_IDS, r#"["a","b"]"#)]),
            json!({"bloombergRequestIds": ["a", "b"]})
        );
        assert_eq!(
            decoded_json(&[(METADATA_KEY_BLOOMBERG_REQUEST_IDS, r#"["a",1]"#)]),
            json!({})
        );
    }

    #[test]
    fn lenient_members_decode_and_drop_malformed_input() {
        let decoded = decoded_json(&[
            (
                METADATA_KEY_EID_DATA,
                r#"{"IBM US Equity":[14005,35009],"EMPTY":[]}"#,
            ),
            (
                METADATA_KEY_DELAYED,
                r#"{"A":{"delayed":true,"delay_minutes":15},"B":{"delayed":false,"delay_minutes":null}}"#,
            ),
            (
                METADATA_KEY_SECURITY_ERRORS,
                r#"{"BAD":{"category":"BAD_SEC","code":43,"subcategory":"INVALID","message":"Unknown","extra":1},"ODD":{"code":"X1"}}"#,
            ),
            (METADATA_KEY_TYPE_MISSES, r#"{"PX_LAST":2}"#),
        ]);
        assert_eq!(
            decoded,
            json!({
                "eidData": {"EMPTY": [], "IBM US Equity": [14005, 35009]},
                "delayed": {
                    "A": {"delayed": true, "delayMinutes": 15},
                    "B": {"delayed": false, "delayMinutes": null}
                },
                "securityErrors": {
                    "BAD": {"category": "BAD_SEC", "code": "43", "subcategory": "INVALID", "message": "Unknown"},
                    "ODD": {"code": "X1"}
                },
                "typeMisses": {"PX_LAST": 2}
            })
        );

        for (key, raw) in [
            (METADATA_KEY_EID_DATA, r#"{"A":[1,"2"]}"#),
            (METADATA_KEY_EID_DATA, "not json"),
            (METADATA_KEY_DELAYED, r#"{"A":{"delayed":true}}"#),
            (
                METADATA_KEY_DELAYED,
                r#"{"A":{"delayed":"yes","delay_minutes":null}}"#,
            ),
            (METADATA_KEY_SECURITY_ERRORS, r#"{"A":{"category":1}}"#),
            (METADATA_KEY_SECURITY_ERRORS, r#"{"A":{"code":true}}"#),
            (METADATA_KEY_SECURITY_ERRORS, r#"["A"]"#),
            (METADATA_KEY_TYPE_MISSES, r#"{"A":-1}"#),
        ] {
            assert_eq!(decoded_json(&[(key, raw)]), json!({}), "{key}: {raw}");
        }
    }

    #[test]
    fn field_exception_envelope_expands_in_occurrence_order() {
        let envelope = json!({
            "version": 1,
            "entries": [
                {"field": "F1", "category": "BAD_FLD", "code": 9, "subcategory": "INVALID", "message": "m1"},
                {"field": "F2", "category": "NOT_APPLICABLE", "code": 4, "subcategory": "NA", "message": "m2"}
            ],
            "securities": {"IBM": [1, 0, 1], "AAPL": [0]}
        })
        .to_string();
        let decoded = decoded_json(&[(METADATA_KEY_FIELD_EXCEPTIONS, &envelope)]);
        let f1 = json!({"field": "F1", "category": "BAD_FLD", "code": 9, "subcategory": "INVALID", "message": "m1"});
        let f2 = json!({"field": "F2", "category": "NOT_APPLICABLE", "code": 4, "subcategory": "NA", "message": "m2"});
        assert_eq!(
            decoded,
            json!({"fieldExceptions": {"AAPL": [f1.clone()], "IBM": [f2.clone(), f1, f2]}})
        );
    }

    #[test]
    fn legacy_field_exception_map_is_accepted() {
        let legacy = json!({"IBM": [{"field": "F1", "category": "C", "code": 1, "subcategory": "S", "message": "M"}]})
            .to_string();
        assert_eq!(
            decoded_json(&[(METADATA_KEY_FIELD_EXCEPTIONS, &legacy)]),
            json!({"fieldExceptions": {"IBM": [{"field": "F1", "category": "C", "code": 1, "subcategory": "S", "message": "M"}]}})
        );
    }

    #[test]
    fn malformed_field_exceptions_are_reported_not_dropped() {
        for (raw, expected) in [
            (
                "{",
                "Invalid xbbg.field_exceptions metadata: expected valid JSON",
            ),
            (
                "[]",
                "Invalid xbbg.field_exceptions metadata: expected an object",
            ),
            (
                r#"{"version":2,"entries":[],"securities":{}}"#,
                "Invalid xbbg.field_exceptions metadata: unsupported field-exception metadata version 2",
            ),
            (
                r#"{"version":1,"entries":[],"securities":{"IBM":[0]}}"#,
                "Invalid xbbg.field_exceptions metadata: field-exception dictionary index out of range",
            ),
        ] {
            let decoded = decode(&metadata(&[(METADATA_KEY_FIELD_EXCEPTIONS, raw)]));
            assert_eq!(decoded.field_exceptions, None, "{raw}");
            assert_eq!(
                decoded.field_exceptions_error.as_deref(),
                Some(expected),
                "{raw}"
            );
        }
    }

    #[test]
    fn field_errors_are_strict() {
        let decoded = decoded_json(&[(
            METADATA_KEY_FIELD_ERRORS,
            r#"{"BAD":{"category":"BAD_FLD","code":9,"subcategory":"NOT_FOUND","message":"Unknown","source":"apiflds"},"X":{"category":"c","code":1,"subcategory":"s","message":"m"}}"#,
        )]);
        assert_eq!(
            decoded,
            json!({"fieldErrors": {
                "BAD": {"category": "BAD_FLD", "code": 9, "subcategory": "NOT_FOUND", "message": "Unknown", "source": "apiflds"},
                "X": {"category": "c", "code": 1, "subcategory": "s", "message": "m"}
            }})
        );
        for (raw, expected) in [
            (
                "nope",
                "Invalid xbbg.field_errors metadata: expected valid JSON",
            ),
            (
                r#"{"BAD":{"category":"c","code":1.5,"subcategory":"s","message":"m"}}"#,
                "Invalid xbbg.field_errors metadata: malformed field errors",
            ),
            (
                r#"{"BAD":{"category":"c","code":1}}"#,
                "Invalid xbbg.field_errors metadata: malformed field errors",
            ),
        ] {
            let decoded = decode(&metadata(&[(METADATA_KEY_FIELD_ERRORS, raw)]));
            assert_eq!(decoded.field_errors, None);
            assert_eq!(
                decoded.field_errors_error.as_deref(),
                Some(expected),
                "{raw}"
            );
        }
    }
}
