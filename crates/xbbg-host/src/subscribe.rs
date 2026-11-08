//! Subscription argument assembly shared by host bindings.
//!
//! This module owns the checks the JavaScript package runs before any native
//! subscribe call (`validateStreamControls` and `buildStreamSubscriptionOptions`
//! in `js-xbbg/src/subscriptions.ts`), with the same messages and elements, and
//! the napi `subscribe`/`subscribeWithOptions` conversion into one
//! [`SubscribeRequest`] (defaults, enum parsing and check order).
//!
//! # JSON arguments of the unified `subscribe` call
//!
//! ```json
//! {
//!   "service": "//blp/mktdata",
//!   "tickers": ["IBM US Equity"],
//!   "fields": ["LAST_PRICE"],
//!   "rawOptions": false,
//!   "options": { "delivery": "deltas", "options": ["interval=5"], "conflate": false,
//!                "flushThreshold": 16, "overflowPolicy": "drop_newest", "streamCapacity": 256,
//!                "allFields": false, "fields": [], "aliases": {"IBM US Equity": "ibm"},
//!                "onDelayed": "warn", "isolated": false, "rows": true,
//!                "onFieldError": "warn", "zeroAsNull": ["BID"] }
//! }
//! ```
//!
//! `service` defaults to `//blp/mktdata`; `options` is the TS `StreamOptions`
//! object (every member optional, absent means JavaScript `undefined`).
//! `options.fields` is accepted and ignored here: only `mktbar`, `depth`,
//! `chains` and the preflight entry points read it, and they resolve the
//! positional `fields` themselves. `rawOptions: true` is the
//! `subscribeWithOptions` form: Bloomberg options pass through unchanged and
//! `conflate=true` is rejected instead of being silently ignored.

use std::str::FromStr;

use serde::Deserialize;
use serde_json::Value;
use xbbg_async::{
    DelayedPolicy, FieldErrorPolicy, OverflowPolicy, SubscribeRequest, SubscriptionDeliveryMode,
};

use crate::errors::HostError;

/// Default Bloomberg market-data service.
pub const MKTDATA_SERVICE: &str = "//blp/mktdata";

/// The TS `StreamOptions` object. Members validated with JavaScript messages
/// are kept as raw JSON so a wrong type reports the same text as JavaScript.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamOptionsSpec {
    pub delivery: Option<Value>,
    pub options: Option<Vec<String>>,
    pub conflate: Option<bool>,
    pub flush_threshold: Option<i64>,
    pub overflow_policy: Option<String>,
    pub stream_capacity: Option<i64>,
    pub all_fields: Option<bool>,
    pub fields: Option<Vec<String>>,
    pub aliases: Option<Value>,
    pub on_delayed: Option<Value>,
    pub isolated: Option<Value>,
    pub rows: Option<Value>,
    pub on_field_error: Option<Value>,
    pub zero_as_null: Option<Value>,
}

/// Arguments of the unified `subscribe` call (see the module docs).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscribeArgs {
    pub service: Option<String>,
    pub tickers: Vec<String>,
    pub fields: Vec<String>,
    #[serde(default)]
    pub raw_options: bool,
    #[serde(default)]
    pub options: StreamOptionsSpec,
}

/// Stream controls after the JavaScript checks, still in wire form.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamControls {
    /// Upstream topic to consumer label, in JSON object order.
    pub aliases: Vec<(String, String)>,
    pub on_delayed: Option<String>,
    pub isolated: Option<bool>,
    pub rows: Option<bool>,
    pub delivery: Option<String>,
    pub on_field_error: Option<String>,
    pub zero_as_null: Option<Vec<String>>,
}

/// A validated engine request and the host batch size for its reads.
#[derive(Debug)]
pub struct PreparedSubscribe {
    pub request: SubscribeRequest,
    /// `flushThreshold`, or the engine's subscription flush threshold.
    pub batch_items: usize,
}

fn invalid(element: &str, message: impl Into<String>) -> HostError {
    HostError::validation(message).with_element(element)
}

/// JavaScript `String.prototype.trim`: ECMAScript white space and line terminators.
pub fn js_trim(value: &str) -> &str {
    value.trim_matches(|c: char| {
        matches!(
            c,
            '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
                ..='\u{200a}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202f}'
                    | '\u{205f}'
                    | '\u{3000}'
                    | '\u{feff}'
        )
    })
}

/// Validate an `aliases` value: a JSON object whose values are all strings.
pub fn validate_aliases(aliases: Option<&Value>) -> Result<Vec<(String, String)>, HostError> {
    match aliases {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(topic, label)| match label {
                Value::String(label) => Ok((topic.clone(), label.clone())),
                _ => Err(aliases_error()),
            })
            .collect(),
        Some(_) => Err(aliases_error()),
    }
}

fn aliases_error() -> HostError {
    invalid("aliases", "aliases must map topic strings to label strings")
}

fn policy(value: Option<&Value>, element: &str) -> Result<Option<String>, HostError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if matches!(text.as_str(), "warn" | "raise" | "ignore") => {
            Ok(Some(text.clone()))
        }
        Some(_) => Err(invalid(
            element,
            format!("{element} must be warn, raise, or ignore"),
        )),
    }
}

fn boolean(value: Option<&Value>, element: &str) -> Result<Option<bool>, HostError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(flag)) => Ok(Some(*flag)),
        Some(_) => Err(invalid(element, format!("{element} must be a boolean"))),
    }
}

/// The JavaScript `validateStreamControls` checks, in the same order.
pub fn validate_stream_controls(options: &StreamOptionsSpec) -> Result<StreamControls, HostError> {
    let aliases = validate_aliases(options.aliases.as_ref())?;
    let on_delayed = policy(options.on_delayed.as_ref(), "onDelayed")?;
    let isolated = boolean(options.isolated.as_ref(), "isolated")?;
    let rows = boolean(options.rows.as_ref(), "rows")?;
    let delivery = match options.delivery.as_ref() {
        None | Some(Value::Null) => None,
        Some(Value::String(mode)) if matches!(mode.as_str(), "deltas" | "state") => {
            Some(mode.clone())
        }
        Some(_) => return Err(invalid("delivery", "delivery must be deltas or state")),
    };
    if delivery.as_deref() == Some("state") {
        if rows == Some(false) {
            return Err(invalid(
                "rows",
                "delivery=state cannot be combined with rows=false",
            ));
        }
        if options.overflow_policy.is_some() {
            return Err(invalid(
                "overflowPolicy",
                "delivery=state does not accept overflowPolicy",
            ));
        }
        if options.stream_capacity.is_some() {
            return Err(invalid(
                "streamCapacity",
                "delivery=state does not accept streamCapacity",
            ));
        }
    }
    let on_field_error = policy(options.on_field_error.as_ref(), "onFieldError")?;
    let zero_as_null = match options.zero_as_null.as_ref() {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .map(|item| match item {
                    Value::String(field) => Ok(field.clone()),
                    _ => Err(zero_as_null_error()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => return Err(zero_as_null_error()),
    };
    Ok(StreamControls {
        aliases,
        on_delayed,
        isolated,
        rows,
        delivery,
        on_field_error,
        zero_as_null,
    })
}

fn zero_as_null_error() -> HostError {
    invalid("zeroAsNull", "zeroAsNull must be an array of field names")
}

/// Trim and strip leading `&` separators, as JavaScript does for named options.
pub fn normalize_subscription_option(option: &str) -> &str {
    let mut clean = js_trim(option);
    while let Some(rest) = clean.strip_prefix('&') {
        clean = js_trim(rest);
    }
    clean
}

fn subscription_option_key(option: &str) -> String {
    let normalized = normalize_subscription_option(option);
    let key = normalized.split('=').next().unwrap_or_default();
    js_trim(key).to_lowercase()
}

/// The JavaScript `buildStreamSubscriptionOptions`: `None` when no options were
/// given and `conflate` is not true; empties dropped; `conflate` appended once.
pub fn build_stream_subscription_options(
    service: &str,
    options: Option<&[String]>,
    conflate: Option<bool>,
) -> Result<Option<Vec<String>>, HostError> {
    let conflate = conflate == Some(true);
    if options.is_none() && !conflate {
        return Ok(None);
    }
    let mut subscription_options: Vec<String> = options
        .unwrap_or_default()
        .iter()
        .map(|option| normalize_subscription_option(option))
        .filter(|option| !option.is_empty())
        .map(str::to_string)
        .collect();
    if conflate {
        if service != MKTDATA_SERVICE {
            return Err(invalid(
                "conflate",
                "conflate=true is only supported for //blp/mktdata subscriptions",
            ));
        }
        if subscription_options
            .iter()
            .any(|option| subscription_option_key(option) == "interval")
        {
            return Err(invalid(
                "conflate",
                "conflate=true cannot be combined with interval options; intervalization overrides conflation",
            ));
        }
        if !subscription_options
            .iter()
            .any(|option| subscription_option_key(option) == "conflate")
        {
            subscription_options.push("conflate".to_string());
        }
    }
    if subscription_options.is_empty() && options.is_none() {
        Ok(None)
    } else {
        Ok(Some(subscription_options))
    }
}

fn positive(value: Option<i64>, element: &str) -> Result<Option<usize>, HostError> {
    match value {
        None => Ok(None),
        Some(value) if value <= 0 => Err(invalid(
            element,
            format!("{element} must be greater than zero"),
        )),
        Some(value) => usize::try_from(value)
            .map(Some)
            .map_err(|_| HostError::argument(format!("{element} {value} is out of range"))),
    }
}

/// Validate and assemble one engine request (the JavaScript checks, option
/// building, then the napi `subscribeWithOptions` checks and defaults).
pub fn prepare_subscribe(
    args: SubscribeArgs,
    engine_batch_items: usize,
) -> Result<PreparedSubscribe, HostError> {
    let SubscribeArgs {
        service,
        tickers,
        fields,
        raw_options,
        options,
    } = args;
    let controls = validate_stream_controls(&options)?;
    let service = service.unwrap_or_else(|| MKTDATA_SERVICE.to_string());
    let subscription_options = if raw_options {
        if options.conflate == Some(true) {
            return Err(invalid(
                "conflate",
                "conflate=true is not supported with raw subscription options; use subscribe or stream",
            ));
        }
        options.options.clone()
    } else {
        build_stream_subscription_options(&service, options.options.as_deref(), options.conflate)?
    };
    let overflow_policy = options
        .overflow_policy
        .as_deref()
        .map(OverflowPolicy::from_str)
        .transpose()
        .map_err(|message| invalid("overflowPolicy", message))?;
    let delayed_policy = controls
        .on_delayed
        .as_deref()
        .map(DelayedPolicy::from_str)
        .transpose()
        .map_err(HostError::from_async)?
        .unwrap_or_default();
    let field_error_policy = controls
        .on_field_error
        .as_deref()
        .map(FieldErrorPolicy::from_str)
        .transpose()
        .map_err(HostError::from_async)?
        .unwrap_or_default();
    let delivery = controls
        .delivery
        .as_deref()
        .map(SubscriptionDeliveryMode::from_str)
        .transpose()
        .map_err(HostError::from_async)?
        .unwrap_or_default();
    let stream_capacity = positive(options.stream_capacity, "streamCapacity")?;
    let flush_threshold = positive(options.flush_threshold, "flushThreshold")?;
    let batch_items = flush_threshold.unwrap_or(engine_batch_items);
    Ok(PreparedSubscribe {
        request: SubscribeRequest {
            service,
            topics: tickers,
            fields,
            all_fields: options.all_fields.unwrap_or(false),
            options: subscription_options.unwrap_or_default(),
            aliases: controls.aliases,
            delayed_policy,
            field_error_policy,
            deliver_rows: controls.rows.unwrap_or(true),
            delivery,
            zero_as_null: controls.zero_as_null.unwrap_or_default(),
            session_wait: None,
            isolated: controls.isolated.unwrap_or(false),
            stream_capacity,
            flush_threshold,
            overflow_policy,
        },
        batch_items,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::errors::HostErrorKind;

    fn options(value: Value) -> StreamOptionsSpec {
        serde_json::from_value(value).expect("stream options")
    }

    fn rejected(value: Value) -> HostError {
        validate_stream_controls(&options(value)).expect_err("rejected")
    }

    fn assert_validation(error: &HostError, element: &str, message: &str) {
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(error.message, message);
        assert_eq!(error.context.element.as_deref(), Some(element));
    }

    fn args(value: Value) -> SubscribeArgs {
        serde_json::from_value(value).expect("subscribe args")
    }

    #[test]
    fn rejects_each_invalid_control_with_the_javascript_message_and_element() {
        let cases = [
            (
                json!({"aliases": ["IBM"]}),
                "aliases",
                "aliases must map topic strings to label strings",
            ),
            (
                json!({"aliases": {"IBM US Equity": 3}}),
                "aliases",
                "aliases must map topic strings to label strings",
            ),
            (
                json!({"onDelayed": "WARN"}),
                "onDelayed",
                "onDelayed must be warn, raise, or ignore",
            ),
            (
                json!({"isolated": "yes"}),
                "isolated",
                "isolated must be a boolean",
            ),
            (json!({"rows": 1}), "rows", "rows must be a boolean"),
            (
                json!({"delivery": "State"}),
                "delivery",
                "delivery must be deltas or state",
            ),
            (
                json!({"delivery": "state", "rows": false}),
                "rows",
                "delivery=state cannot be combined with rows=false",
            ),
            (
                json!({"delivery": "state", "overflowPolicy": "drop_newest"}),
                "overflowPolicy",
                "delivery=state does not accept overflowPolicy",
            ),
            (
                json!({"delivery": "state", "streamCapacity": 4}),
                "streamCapacity",
                "delivery=state does not accept streamCapacity",
            ),
            (
                json!({"onFieldError": " warn"}),
                "onFieldError",
                "onFieldError must be warn, raise, or ignore",
            ),
            (
                json!({"zeroAsNull": "BID"}),
                "zeroAsNull",
                "zeroAsNull must be an array of field names",
            ),
            (
                json!({"zeroAsNull": ["BID", 7]}),
                "zeroAsNull",
                "zeroAsNull must be an array of field names",
            ),
        ];
        for (input, element, message) in cases {
            assert_validation(&rejected(input), element, message);
        }
    }

    #[test]
    fn checks_run_in_javascript_order() {
        // aliases before onDelayed, delivery before the state checks, state before onFieldError.
        let error = rejected(json!({"aliases": 1, "onDelayed": "x"}));
        assert_eq!(error.context.element.as_deref(), Some("aliases"));
        let error = rejected(json!({"onDelayed": "x", "rows": "x"}));
        assert_eq!(error.context.element.as_deref(), Some("onDelayed"));
        let error = rejected(json!({
            "delivery": "state", "rows": false, "overflowPolicy": "block", "onFieldError": "x"
        }));
        assert_eq!(error.context.element.as_deref(), Some("rows"));
        let error = rejected(json!({
            "delivery": "state", "overflowPolicy": "block", "streamCapacity": 2
        }));
        assert_eq!(error.context.element.as_deref(), Some("overflowPolicy"));
    }

    #[test]
    fn accepts_valid_controls_without_defaulting_them() {
        let controls = validate_stream_controls(&options(json!({
            "aliases": {"IBM US Equity": "ibm", "MSFT US Equity": "msft"},
            "onDelayed": "raise",
            "isolated": true,
            "rows": false,
            "delivery": "deltas",
            "onFieldError": "ignore",
            "zeroAsNull": ["BID"],
        })))
        .expect("valid controls");
        assert_eq!(
            controls.aliases,
            vec![
                ("IBM US Equity".to_string(), "ibm".to_string()),
                ("MSFT US Equity".to_string(), "msft".to_string()),
            ]
        );
        assert_eq!(controls.on_delayed.as_deref(), Some("raise"));
        assert_eq!(controls.rows, Some(false));
        assert_eq!(controls.zero_as_null, Some(vec!["BID".to_string()]));
        assert_eq!(
            validate_stream_controls(&StreamOptionsSpec::default()).expect("empty"),
            StreamControls::default()
        );
    }

    #[test]
    fn normalizes_named_options_like_javascript() {
        assert_eq!(
            normalize_subscription_option("  && interval=5 "),
            "interval=5"
        );
        assert_eq!(normalize_subscription_option("\u{feff}&\u{a0}x"), "x");
        let built = build_stream_subscription_options(
            MKTDATA_SERVICE,
            Some(&[
                "&interval=5".into(),
                "   ".into(),
                "&".into(),
                " fields=BID ".into(),
            ]),
            None,
        )
        .expect("options");
        assert_eq!(
            built,
            Some(vec!["interval=5".to_string(), "fields=BID".to_string()])
        );
        // Given but empty stays an explicit empty list; absent stays absent.
        assert_eq!(
            build_stream_subscription_options(MKTDATA_SERVICE, Some(&[" ".into()]), None)
                .expect("empty"),
            Some(Vec::new())
        );
        assert_eq!(
            build_stream_subscription_options(MKTDATA_SERVICE, None, Some(false)).expect("none"),
            None
        );
    }

    #[test]
    fn conflate_appends_once_and_rejects_other_services_and_intervals() {
        assert_eq!(
            build_stream_subscription_options(MKTDATA_SERVICE, None, Some(true)).expect("conflate"),
            Some(vec!["conflate".to_string()])
        );
        assert_eq!(
            build_stream_subscription_options(
                MKTDATA_SERVICE,
                Some(&[" & CONFLATE ".into()]),
                Some(true)
            )
            .expect("existing conflate"),
            Some(vec!["CONFLATE".to_string()])
        );
        assert_validation(
            &build_stream_subscription_options("//blp/mktbar", None, Some(true))
                .expect_err("service"),
            "conflate",
            "conflate=true is only supported for //blp/mktdata subscriptions",
        );
        assert_validation(
            &build_stream_subscription_options(
                MKTDATA_SERVICE,
                Some(&["&Interval = 5".into()]),
                Some(true),
            )
            .expect_err("interval"),
            "conflate",
            "conflate=true cannot be combined with interval options; intervalization overrides conflation",
        );
    }

    #[test]
    fn prepares_napi_defaults_and_the_engine_batch_size() {
        let prepared = prepare_subscribe(
            args(json!({"tickers": ["IBM US Equity"], "fields": ["BID"]})),
            16,
        )
        .expect("prepared");
        let request = prepared.request;
        assert_eq!(prepared.batch_items, 16);
        assert_eq!(request.service, MKTDATA_SERVICE);
        assert_eq!(request.topics, vec!["IBM US Equity"]);
        assert_eq!(request.fields, vec!["BID"]);
        assert!(request.options.is_empty());
        assert!(request.deliver_rows);
        assert!(!request.all_fields);
        assert!(!request.isolated);
        assert_eq!(request.delivery, SubscriptionDeliveryMode::Deltas);
        assert_eq!(request.delayed_policy, DelayedPolicy::Warn);
        assert_eq!(request.field_error_policy, FieldErrorPolicy::Warn);
        assert_eq!(request.stream_capacity, None);
        assert_eq!(request.flush_threshold, None);
        assert_eq!(request.overflow_policy, None);
        assert_eq!(request.session_wait, None);
    }

    #[test]
    fn prepares_every_explicit_option() {
        let prepared = prepare_subscribe(
            args(json!({
                "service": "//blp/mktvwap",
                "tickers": ["IBM US Equity"],
                "fields": ["VWAP"],
                "options": {
                    "options": [" &VWAP_START_TIME=10:00 "],
                    "flushThreshold": 4,
                    "overflowPolicy": "DropOldest",
                    "streamCapacity": 32,
                    "allFields": true,
                    "aliases": {"IBM US Equity": "ibm"},
                    "onDelayed": "ignore",
                    "isolated": true,
                    "onFieldError": "raise",
                    "zeroAsNull": ["VWAP"],
                    "fields": ["ignored by subscribe"]
                }
            })),
            16,
        )
        .expect("prepared");
        let request = prepared.request;
        assert_eq!(prepared.batch_items, 4);
        assert_eq!(request.service, "//blp/mktvwap");
        assert_eq!(request.fields, vec!["VWAP"]);
        assert_eq!(request.options, vec!["VWAP_START_TIME=10:00"]);
        assert_eq!(request.flush_threshold, Some(4));
        assert_eq!(request.stream_capacity, Some(32));
        assert_eq!(request.overflow_policy, Some(OverflowPolicy::DropOldest));
        assert!(request.all_fields && request.isolated);
        assert_eq!(
            request.aliases,
            vec![("IBM US Equity".to_string(), "ibm".to_string())]
        );
        assert_eq!(request.delayed_policy, DelayedPolicy::Ignore);
        assert_eq!(request.field_error_policy, FieldErrorPolicy::Raise);
        assert_eq!(request.zero_as_null, vec!["VWAP"]);
    }

    #[test]
    fn raw_options_pass_unchanged_and_reject_conflate() {
        let prepared = prepare_subscribe(
            args(json!({
                "service": "//blp/mktbar",
                "tickers": ["IBM US Equity"],
                "fields": [],
                "rawOptions": true,
                "options": {"options": ["  &interval=5", ""]}
            })),
            16,
        )
        .expect("raw");
        assert_eq!(prepared.request.options, vec!["  &interval=5", ""]);
        let error = prepare_subscribe(
            args(json!({
                "tickers": ["IBM US Equity"], "fields": [], "rawOptions": true,
                "options": {"conflate": true}
            })),
            16,
        )
        .expect_err("raw conflate");
        assert_validation(
            &error,
            "conflate",
            "conflate=true is not supported with raw subscription options; use subscribe or stream",
        );
    }

    #[test]
    fn native_checks_keep_the_napi_order_and_messages() {
        let error = prepare_subscribe(
            args(json!({
                "tickers": [], "fields": [],
                "options": {"overflowPolicy": "fastest", "streamCapacity": 0, "flushThreshold": 0}
            })),
            16,
        )
        .expect_err("overflow first");
        assert_validation(
            &error,
            "overflowPolicy",
            "unknown overflow policy 'fastest': expected 'drop_newest', 'drop_oldest', or 'block'",
        );
        let error = prepare_subscribe(
            args(json!({
                "tickers": [], "fields": [],
                "options": {"streamCapacity": 0, "flushThreshold": 0}
            })),
            16,
        )
        .expect_err("capacity before flush");
        assert_validation(
            &error,
            "streamCapacity",
            "streamCapacity must be greater than zero",
        );
        let error = prepare_subscribe(
            args(json!({"tickers": [], "fields": [], "options": {"flushThreshold": -3}})),
            16,
        )
        .expect_err("flush");
        assert_validation(
            &error,
            "flushThreshold",
            "flushThreshold must be greater than zero",
        );
    }

    #[test]
    fn javascript_checks_precede_option_building() {
        // validateStreamControls runs before conflate is examined.
        let error = prepare_subscribe(
            args(json!({
                "service": "//blp/mktbar", "tickers": [], "fields": [],
                "options": {"conflate": true, "rows": "no"}
            })),
            16,
        )
        .expect_err("rows first");
        assert_eq!(error.context.element.as_deref(), Some("rows"));
    }

    #[test]
    fn unknown_members_are_rejected_by_the_json_shape() {
        let error = serde_json::from_value::<SubscribeArgs>(json!({
            "tickers": [], "fields": [], "options": {"conflated": true}
        }))
        .expect_err("unknown option");
        assert!(error.to_string().contains("unknown field `conflated`"));
    }
}
