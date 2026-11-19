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
use crate::style::{Style, python_str_repr};

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

fn policy(value: Option<&Value>, name: &str, style: Style) -> Result<Option<String>, HostError> {
    let rejected = || invalid(name, format!("{name} must be warn, raise, or ignore"));
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => {
            let text = if style.lenient_values() {
                text.trim().to_ascii_lowercase()
            } else {
                text.clone()
            };
            if matches!(text.as_str(), "warn" | "raise" | "ignore") {
                Ok(Some(text))
            } else {
                Err(rejected())
            }
        }
        Some(_) => Err(rejected()),
    }
}

fn boolean(value: Option<&Value>, element: &str) -> Result<Option<bool>, HostError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(flag)) => Ok(Some(*flag)),
        Some(_) => Err(invalid(element, format!("{element} must be a boolean"))),
    }
}

/// The delivery mode `text` names, in its canonical spelling.
fn delivery_mode(text: &str, style: Style) -> Option<&'static str> {
    ["deltas", "state"].into_iter().find(|mode| {
        if style.lenient_values() {
            text.trim().eq_ignore_ascii_case(mode)
        } else {
            text == *mode
        }
    })
}

fn delivery_error(value: &Value, style: Style) -> HostError {
    match (style, value) {
        (Style::Python, Value::String(text)) => invalid(
            "delivery",
            format!(
                "delivery must be one of deltas, state, got {}",
                python_str_repr(text)
            ),
        ),
        (Style::Python, _) => invalid("delivery", "delivery must be one of deltas, state"),
        (Style::JavaScript, _) => invalid("delivery", "delivery must be deltas or state"),
    }
}

/// The JavaScript `validateStreamControls` checks, in the same order.
pub fn validate_stream_controls(options: &StreamOptionsSpec) -> Result<StreamControls, HostError> {
    validate_stream_controls_as(options, Style::JavaScript)
}

/// [`validate_stream_controls`] with the wording and strictness of `style`.
///
/// [`Style::Python`] names members by their snake_case parameters, accepts
/// `onDelayed`, `onFieldError` and `delivery` in any case and with surrounding
/// white space (returned in canonical lower case), and words the `delivery=state`
/// conflicts the way Python callers read them.
pub fn validate_stream_controls_as(
    options: &StreamOptionsSpec,
    style: Style,
) -> Result<StreamControls, HostError> {
    let on_delayed_name = style.name("onDelayed", "on_delayed");
    let on_field_error_name = style.name("onFieldError", "on_field_error");
    let overflow_policy_name = style.name("overflowPolicy", "overflow_policy");
    let stream_capacity_name = style.name("streamCapacity", "stream_capacity");
    let zero_as_null_name = style.name("zeroAsNull", "zero_as_null");

    let aliases = validate_aliases(options.aliases.as_ref())?;
    let on_delayed = policy(options.on_delayed.as_ref(), on_delayed_name, style)?;
    let isolated = boolean(options.isolated.as_ref(), "isolated")?;
    let rows = boolean(options.rows.as_ref(), "rows")?;
    let delivery = match options.delivery.as_ref() {
        None | Some(Value::Null) => None,
        Some(value @ Value::String(mode)) => {
            Some(delivery_mode(mode, style).ok_or_else(|| delivery_error(value, style))?)
        }
        Some(value) => return Err(delivery_error(value, style)),
    };
    if delivery == Some("state") {
        if rows == Some(false) {
            return Err(invalid(
                "rows",
                match style {
                    Style::JavaScript => "delivery=state cannot be combined with rows=false",
                    Style::Python => {
                        "delivery='state' cannot be combined with rows=False; use latest() instead"
                    }
                },
            ));
        }
        for (given, name) in [
            (options.overflow_policy.is_some(), overflow_policy_name),
            (options.stream_capacity.is_some(), stream_capacity_name),
        ] {
            if given {
                return Err(invalid(
                    name,
                    match style {
                        Style::JavaScript => format!("delivery=state does not accept {name}"),
                        Style::Python => {
                            "delivery='state' cannot be combined with explicit raw queue controls"
                                .to_owned()
                        }
                    },
                ));
            }
        }
    }
    let on_field_error = policy(options.on_field_error.as_ref(), on_field_error_name, style)?;
    let zero_as_null = match options.zero_as_null.as_ref() {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .map(|item| match item {
                    Value::String(field) => Ok(field.clone()),
                    _ => Err(zero_as_null_error(zero_as_null_name)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => return Err(zero_as_null_error(zero_as_null_name)),
    };
    Ok(StreamControls {
        aliases,
        on_delayed,
        isolated,
        rows,
        delivery: delivery.map(str::to_owned),
        on_field_error,
        zero_as_null,
    })
}

fn zero_as_null_error(name: &str) -> HostError {
    invalid(name, format!("{name} must be an array of field names"))
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
    build_stream_subscription_options_as(service, options, conflate, Style::JavaScript)
}

/// [`build_stream_subscription_options`] with the wording of `style`.
pub fn build_stream_subscription_options_as(
    service: &str,
    options: Option<&[String]>,
    conflate: Option<bool>,
    style: Style,
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
        let literal = style.flag(true);
        if service != MKTDATA_SERVICE {
            return Err(invalid(
                "conflate",
                format!("conflate={literal} is only supported for //blp/mktdata subscriptions"),
            ));
        }
        if subscription_options
            .iter()
            .any(|option| subscription_option_key(option) == "interval")
        {
            return Err(invalid(
                "conflate",
                format!(
                    "conflate={literal} cannot be combined with interval options; intervalization overrides conflation"
                ),
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

fn positive(value: Option<i64>, name: &str, style: Style) -> Result<Option<usize>, HostError> {
    match value {
        None => Ok(None),
        Some(value) if value <= 0 => Err(invalid(
            name,
            match style {
                Style::JavaScript => format!("{name} must be greater than zero"),
                Style::Python => format!("{name} must be >= 1"),
            },
        )),
        Some(value) => usize::try_from(value)
            .map(Some)
            .map_err(|_| HostError::argument(format!("{name} {value} is out of range"))),
    }
}

/// Validate and assemble one engine request (the JavaScript checks, option
/// building, then the napi `subscribeWithOptions` checks and defaults).
pub fn prepare_subscribe(
    args: SubscribeArgs,
    engine_batch_items: usize,
) -> Result<PreparedSubscribe, HostError> {
    prepare_subscribe_as(args, engine_batch_items, Style::JavaScript)
}

/// [`prepare_subscribe`] with the wording and strictness of `style`.
pub fn prepare_subscribe_as(
    args: SubscribeArgs,
    engine_batch_items: usize,
    style: Style,
) -> Result<PreparedSubscribe, HostError> {
    let SubscribeArgs {
        service,
        tickers,
        fields,
        raw_options,
        mut options,
    } = args;
    let controls = validate_stream_controls_as(&options, style)?;
    let service = service.unwrap_or_else(|| MKTDATA_SERVICE.to_string());
    let subscription_options = if raw_options {
        if options.conflate == Some(true) {
            return Err(invalid(
                "conflate",
                format!(
                    "conflate={} is not supported with raw subscription options; use subscribe or stream",
                    style.flag(true)
                ),
            ));
        }
        options.options.take()
    } else {
        build_stream_subscription_options_as(
            &service,
            options.options.as_deref(),
            options.conflate,
            style,
        )?
    };
    let overflow_policy = options
        .overflow_policy
        .as_deref()
        .map(|text| {
            OverflowPolicy::from_str(text).map_err(|message| match style {
                Style::JavaScript => invalid("overflowPolicy", message),
                Style::Python => invalid(
                    "overflow_policy",
                    format!(
                        "overflow_policy must be one of block, drop_newest, drop_oldest, got {}",
                        python_str_repr(text)
                    ),
                ),
            })
        })
        .transpose()?;
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
    let stream_capacity = positive(
        options.stream_capacity,
        style.name("streamCapacity", "stream_capacity"),
        style,
    )?;
    let flush_threshold = positive(
        options.flush_threshold,
        style.name("flushThreshold", "flush_threshold"),
        style,
    )?;
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

    fn python_rejected(value: Value) -> HostError {
        validate_stream_controls_as(&options(value), Style::Python).expect_err("rejected")
    }

    #[test]
    fn python_style_names_members_by_parameter_and_words_conflicts_for_python() {
        let cases = [
            (
                json!({"onDelayed": "x"}),
                "on_delayed",
                "on_delayed must be warn, raise, or ignore",
            ),
            (
                json!({"onFieldError": 1}),
                "on_field_error",
                "on_field_error must be warn, raise, or ignore",
            ),
            (
                json!({"delivery": "fast"}),
                "delivery",
                "delivery must be one of deltas, state, got 'fast'",
            ),
            (
                json!({"delivery": "it's"}),
                "delivery",
                "delivery must be one of deltas, state, got \"it's\"",
            ),
            (
                json!({"delivery": "state", "rows": false}),
                "rows",
                "delivery='state' cannot be combined with rows=False; use latest() instead",
            ),
            (
                json!({"delivery": "state", "overflowPolicy": "block"}),
                "overflow_policy",
                "delivery='state' cannot be combined with explicit raw queue controls",
            ),
            (
                json!({"delivery": "state", "streamCapacity": 4}),
                "stream_capacity",
                "delivery='state' cannot be combined with explicit raw queue controls",
            ),
            (
                json!({"zeroAsNull": "BID"}),
                "zero_as_null",
                "zero_as_null must be an array of field names",
            ),
        ];
        for (input, element, message) in cases {
            assert_validation(&python_rejected(input), element, message);
        }
    }

    #[test]
    fn python_style_accepts_the_engines_spellings_in_canonical_form() {
        let controls = validate_stream_controls_as(
            &options(json!({
                "onDelayed": " RAISE ", "onFieldError": "Ignore", "delivery": " State "
            })),
            Style::Python,
        )
        .expect("lenient");
        assert_eq!(controls.on_delayed.as_deref(), Some("raise"));
        assert_eq!(controls.on_field_error.as_deref(), Some("ignore"));
        assert_eq!(controls.delivery.as_deref(), Some("state"));
        // JavaScript stays exact.
        for input in [
            json!({"onDelayed": " RAISE "}),
            json!({"onFieldError": "Ignore"}),
            json!({"delivery": " State "}),
        ] {
            assert!(validate_stream_controls(&options(input)).is_err());
        }
    }

    #[test]
    fn python_style_spells_conflate_literals_the_python_way() {
        assert_validation(
            &build_stream_subscription_options_as("//blp/mktbar", None, Some(true), Style::Python)
                .expect_err("service"),
            "conflate",
            "conflate=True is only supported for //blp/mktdata subscriptions",
        );
        assert_validation(
            &build_stream_subscription_options_as(
                MKTDATA_SERVICE,
                Some(&["interval=5".into()]),
                Some(true),
                Style::Python,
            )
            .expect_err("interval"),
            "conflate",
            "conflate=True cannot be combined with interval options; intervalization overrides conflation",
        );
    }

    #[test]
    fn python_style_native_checks_use_parameter_names() {
        let prepare = |value: Value| {
            prepare_subscribe_as(args(value), 16, Style::Python).expect_err("rejected")
        };
        assert_validation(
            &prepare(json!({
                "tickers": [], "fields": [], "options": {"overflowPolicy": "fast"}
            })),
            "overflow_policy",
            "overflow_policy must be one of block, drop_newest, drop_oldest, got 'fast'",
        );
        assert_validation(
            &prepare(json!({
                "tickers": [], "fields": [], "options": {"streamCapacity": 0}
            })),
            "stream_capacity",
            "stream_capacity must be >= 1",
        );
        assert_validation(
            &prepare(json!({
                "tickers": [], "fields": [], "options": {"flushThreshold": -2}
            })),
            "flush_threshold",
            "flush_threshold must be >= 1",
        );
        let prepared = prepare_subscribe_as(
            args(json!({
                "tickers": ["IBM US Equity"], "fields": [], "rawOptions": true,
                "options": {"overflowPolicy": "DropOldest", "delivery": "Deltas"}
            })),
            16,
            Style::Python,
        )
        .expect("engine spellings");
        assert_eq!(
            prepared.request.overflow_policy,
            Some(OverflowPolicy::DropOldest)
        );
    }
}
