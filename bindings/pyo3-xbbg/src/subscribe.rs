//! Python subscribe arguments, validated and assembled by `xbbg_host::subscribe`.
//!
//! `PyEngine.subscribe`, `PyEngine.subscribe_with_options` and the offline
//! fixture all build their engine request through [`prepare`], and `asubscribe`
//! checks its arguments before an engine exists through the two pyfunctions
//! below. Failures are `ValueError` with the host's Python-style message.

use std::collections::HashMap;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use serde_json::{Map, Value};
use xbbg_host::style::Style;
use xbbg_host::subscribe::{
    MKTDATA_SERVICE, PreparedSubscribe, StreamOptionsSpec, SubscribeArgs,
    build_stream_subscription_options_as, prepare_subscribe_as,
};

use crate::preflight::host_error;

/// The arguments of `PyEngine.subscribe` / `subscribe_with_options`.
///
/// `options` are Bloomberg subscription options that pass through unchanged:
/// `asubscribe` normalizes them with [`normalize_subscription_options`] first.
pub(crate) struct SubscribeParams<'a> {
    pub service: Option<String>,
    pub tickers: Vec<String>,
    pub fields: Vec<String>,
    pub options: Option<Vec<String>>,
    pub flush_threshold: Option<usize>,
    pub overflow_policy: Option<String>,
    pub stream_capacity: Option<usize>,
    pub all_fields: bool,
    pub aliases: Option<HashMap<String, String>>,
    pub on_delayed: &'a str,
    pub isolated: bool,
    pub rows: bool,
    pub on_field_error: &'a str,
    pub zero_as_null: Option<Vec<String>>,
    pub session_wait_ms: Option<u64>,
    pub delivery: &'a str,
}

fn count(name: &str, value: usize) -> PyResult<i64> {
    i64::try_from(value)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err(format!("{name} is out of range")))
}

fn string(value: &str) -> Option<Value> {
    Some(Value::String(value.to_owned()))
}

/// Validate and assemble the engine request, with Python's wording.
///
/// `engine_batch_items` is the engine's `subscription_flush_threshold`, used as
/// the read batch size when no `flush_threshold` is given.
pub(crate) fn prepare(
    params: SubscribeParams<'_>,
    engine_batch_items: usize,
) -> PyResult<PreparedSubscribe> {
    let SubscribeParams {
        service,
        tickers,
        fields,
        options,
        flush_threshold,
        overflow_policy,
        stream_capacity,
        all_fields,
        aliases,
        on_delayed,
        isolated,
        rows,
        on_field_error,
        zero_as_null,
        session_wait_ms,
        delivery,
    } = params;
    let spec = StreamOptionsSpec {
        delivery: string(delivery),
        options,
        conflate: None,
        flush_threshold: flush_threshold
            .map(|value| count("flush_threshold", value))
            .transpose()?,
        overflow_policy,
        stream_capacity: stream_capacity
            .map(|value| count("stream_capacity", value))
            .transpose()?,
        all_fields: Some(all_fields),
        fields: None,
        aliases: aliases.map(|aliases| {
            Value::Object(
                aliases
                    .into_iter()
                    .map(|(topic, label)| (topic, Value::String(label)))
                    .collect::<Map<_, _>>(),
            )
        }),
        on_delayed: string(on_delayed),
        isolated: Some(Value::Bool(isolated)),
        rows: Some(Value::Bool(rows)),
        on_field_error: string(on_field_error),
        zero_as_null: zero_as_null
            .map(|fields| Value::Array(fields.into_iter().map(Value::String).collect::<Vec<_>>())),
    };
    let mut prepared = prepare_subscribe_as(
        SubscribeArgs {
            service,
            tickers,
            fields,
            raw_options: true,
            options: spec,
        },
        engine_batch_items,
        Style::Python,
    )
    .map_err(host_error)?;
    prepared.request.session_wait = session_wait_ms.map(Duration::from_millis);
    Ok(prepared)
}

/// Check the stream controls `asubscribe` validates before it needs an engine.
///
/// Raises `ValueError` for an unknown `delivery` or `overflow_policy`, for
/// `delivery="state"` combined with `rows=False` or explicit queue controls, and
/// for a `flush_threshold` or `stream_capacity` below 1.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (*, delivery="deltas", rows=true, flush_threshold=None, stream_capacity=None, overflow_policy=None))]
fn validate_subscription_controls(
    delivery: &str,
    rows: bool,
    flush_threshold: Option<i64>,
    stream_capacity: Option<i64>,
    overflow_policy: Option<String>,
) -> PyResult<()> {
    let spec = StreamOptionsSpec {
        delivery: string(delivery),
        flush_threshold,
        overflow_policy,
        stream_capacity,
        rows: Some(Value::Bool(rows)),
        ..StreamOptionsSpec::default()
    };
    prepare_subscribe_as(
        SubscribeArgs {
            service: None,
            tickers: Vec::new(),
            fields: Vec::new(),
            raw_options: true,
            options: spec,
        },
        1,
        Style::Python,
    )
    .map(drop)
    .map_err(host_error)
}

/// Normalize high-level subscription options before they go to Bloomberg.
///
/// Options are trimmed, a leading `&` is stripped and empties dropped. With
/// `conflate`, `conflate` is appended once; that is rejected for any service but
/// `//blp/mktdata` and when an `interval` option is present. Returns `None` when
/// no options were given and `conflate` is false.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (options, conflate=false, service=None))]
fn normalize_subscription_options(
    options: Option<Vec<String>>,
    conflate: bool,
    service: Option<&str>,
) -> PyResult<Option<Vec<String>>> {
    build_stream_subscription_options_as(
        service
            .filter(|service| !service.is_empty())
            .unwrap_or(MKTDATA_SERVICE),
        options.as_deref(),
        Some(conflate),
        Style::Python,
    )
    .map_err(|error| PyValueError::new_err(error.message))
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_subscription_controls, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_subscription_options, m)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use xbbg_async::{DelayedPolicy, FieldErrorPolicy, OverflowPolicy, SubscriptionDeliveryMode};

    use super::*;

    fn params(on_delayed: &'static str, on_field_error: &'static str) -> SubscribeParams<'static> {
        SubscribeParams {
            service: None,
            tickers: vec!["IBM US Equity".to_owned()],
            fields: vec!["LAST_PRICE".to_owned()],
            options: None,
            flush_threshold: None,
            overflow_policy: None,
            stream_capacity: None,
            all_fields: false,
            aliases: None,
            on_delayed,
            isolated: false,
            rows: true,
            on_field_error,
            zero_as_null: None,
            session_wait_ms: None,
            delivery: "deltas",
        }
    }

    fn message(error: &PyErr) -> String {
        Python::attach(|py| {
            assert!(error.is_instance_of::<PyValueError>(py));
            error.value(py).to_string()
        })
    }

    #[test]
    fn invalid_delayed_policy_is_a_python_value_error() {
        Python::initialize();
        for policy in ["", "delayed", "warning"] {
            let error = prepare(params(policy, "warn"), 16).expect_err("invalid delayed policy");
            assert_eq!(message(&error), "on_delayed must be warn, raise, or ignore");
        }
    }

    #[test]
    fn field_error_policy_rejects_invalid_values_and_normalizes_valid_policies() {
        Python::initialize();
        for policy in ["", "delayed", "warning"] {
            let error =
                prepare(params("warn", policy), 16).expect_err("invalid field-error policy");
            assert_eq!(
                message(&error),
                "on_field_error must be warn, raise, or ignore"
            );
        }
        for (policy, expected) in [
            ("warn", FieldErrorPolicy::Warn),
            (" RAISE ", FieldErrorPolicy::Raise),
            ("Ignore", FieldErrorPolicy::Ignore),
        ] {
            let prepared = prepare(params("warn", policy), 16).expect("valid policy");
            assert_eq!(prepared.request.field_error_policy, expected);
        }
        let prepared = prepare(params(" Raise", "warn"), 16).expect("valid policy");
        assert_eq!(prepared.request.delayed_policy, DelayedPolicy::Raise);
    }

    #[test]
    fn prepare_assembles_the_engine_request() {
        Python::initialize();
        let mut input = params("ignore", "raise");
        input.service = Some("//blp/mktvwap".to_owned());
        input.options = Some(vec!["VWAP_START_TIME=10:00".to_owned(), "".to_owned()]);
        input.flush_threshold = Some(4);
        input.overflow_policy = Some("dropoldest".to_owned());
        input.stream_capacity = Some(32);
        input.all_fields = true;
        input.aliases = Some(HashMap::from([(
            "IBM US Equity".to_owned(),
            "ibm".to_owned(),
        )]));
        input.isolated = true;
        input.rows = false;
        input.zero_as_null = Some(vec!["BID".to_owned()]);
        input.session_wait_ms = Some(250);
        input.delivery = "Deltas";
        let prepared = prepare(input, 16).expect("prepared");
        let request = prepared.request;
        // Python normalizes options first, so the engine request keeps them as given.
        assert_eq!(request.options, ["VWAP_START_TIME=10:00", ""]);
        assert_eq!(prepared.batch_items, 4);
        assert_eq!(request.service, "//blp/mktvwap");
        assert_eq!(request.topics, ["IBM US Equity"]);
        assert_eq!(request.fields, ["LAST_PRICE"]);
        assert_eq!(request.flush_threshold, Some(4));
        assert_eq!(request.stream_capacity, Some(32));
        assert_eq!(request.overflow_policy, Some(OverflowPolicy::DropOldest));
        assert!(request.all_fields && request.isolated && !request.deliver_rows);
        assert_eq!(
            request.aliases,
            [("IBM US Equity".to_owned(), "ibm".to_owned())]
        );
        assert_eq!(request.delayed_policy, DelayedPolicy::Ignore);
        assert_eq!(request.field_error_policy, FieldErrorPolicy::Raise);
        assert_eq!(request.delivery, SubscriptionDeliveryMode::Deltas);
        assert_eq!(request.zero_as_null, ["BID"]);
        assert_eq!(request.session_wait, Some(Duration::from_millis(250)));
    }

    #[test]
    fn prepare_defaults_to_mktdata_and_the_engine_batch_size() {
        Python::initialize();
        let prepared = prepare(params("warn", "warn"), 16).expect("prepared");
        assert_eq!(prepared.batch_items, 16);
        assert_eq!(prepared.request.service, MKTDATA_SERVICE);
        assert!(prepared.request.options.is_empty());
        assert_eq!(prepared.request.session_wait, None);
    }

    #[test]
    fn prepare_rejects_bad_controls_with_python_wording() {
        Python::initialize();
        for (edit, expected) in [
            (
                (|p: &mut SubscribeParams<'_>| p.flush_threshold = Some(0))
                    as fn(&mut SubscribeParams<'_>),
                "flush_threshold must be >= 1",
            ),
            (
                |p| p.stream_capacity = Some(0),
                "stream_capacity must be >= 1",
            ),
            (
                |p| p.overflow_policy = Some("invalid_policy".to_owned()),
                "overflow_policy must be one of block, drop_newest, drop_oldest, got 'invalid_policy'",
            ),
            (
                |p| p.delivery = "fast",
                "delivery must be one of deltas, state, got 'fast'",
            ),
            (
                |p| {
                    p.delivery = "state";
                    p.rows = false;
                },
                "delivery='state' cannot be combined with rows=False; use latest() instead",
            ),
            (
                |p| {
                    p.delivery = "state";
                    p.stream_capacity = Some(8);
                },
                "delivery='state' cannot be combined with explicit raw queue controls",
            ),
        ] {
            let mut input = params("warn", "warn");
            edit(&mut input);
            let error = prepare(input, 16).expect_err(expected);
            assert_eq!(message(&error), expected);
        }
    }

    #[test]
    fn validate_subscription_controls_runs_the_same_rules_without_an_engine() {
        Python::initialize();
        validate_subscription_controls("state", true, None, None, None).expect("state");
        let error =
            validate_subscription_controls("deltas", true, Some(0), None, None).expect_err("flush");
        assert_eq!(message(&error), "flush_threshold must be >= 1");
        let error =
            validate_subscription_controls("state", false, None, None, None).expect_err("rows");
        assert_eq!(
            message(&error),
            "delivery='state' cannot be combined with rows=False; use latest() instead"
        );
    }

    #[test]
    fn normalize_subscription_options_trims_and_applies_conflate() {
        Python::initialize();
        let given = |options: &[&str]| Some(options.iter().map(|o| (*o).to_owned()).collect());
        assert_eq!(
            normalize_subscription_options(None, false, None).unwrap(),
            None
        );
        assert_eq!(
            normalize_subscription_options(given(&[" &interval=5 ", " ", "&"]), false, None)
                .unwrap(),
            Some(vec!["interval=5".to_owned()])
        );
        assert_eq!(
            normalize_subscription_options(given(&["&conflate", "delayed"]), true, Some(""))
                .unwrap(),
            Some(vec!["conflate".to_owned(), "delayed".to_owned()])
        );
        assert_eq!(
            normalize_subscription_options(None, true, Some(MKTDATA_SERVICE)).unwrap(),
            Some(vec!["conflate".to_owned()])
        );
        let error =
            normalize_subscription_options(None, true, Some("//blp/mktvwap")).expect_err("service");
        assert_eq!(
            message(&error),
            "conflate=True is only supported for //blp/mktdata subscriptions"
        );
        let error = normalize_subscription_options(given(&["interval=5"]), true, None)
            .expect_err("interval");
        assert_eq!(
            message(&error),
            "conflate=True cannot be combined with interval options; intervalization overrides conflation"
        );
    }
}
