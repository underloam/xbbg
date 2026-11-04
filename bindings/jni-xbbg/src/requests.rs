//! Typed request wrappers (`bdp`, `bdh`, `bql`, ...): the argument object is
//! planned into a generic request by [`xbbg_host::request::plan`] (argument
//! shapes in that module's docs), then runs exactly like core `request`,
//! honouring the requested table kind (TABLE, IPC_STREAM or IPC_FILE).

use std::sync::Arc;

use xbbg_host::errors::HostError;
use xbbg_host::request;

use crate::engine::{EngineEntry, run_request};
use crate::payload::{Payload, ResultKind};

/// Async engine methods dispatched here: every typed request wrapper.
pub(crate) const ASYNC: &[&str] = request::METHODS;
/// Sync engine methods dispatched here (none: every wrapper talks to Bloomberg).
pub(crate) const SYNC: &[&str] = &[];

pub(crate) async fn call(
    engine: Arc<EngineEntry>,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    kind.expect_table(method)?;
    let params = request::plan(method, args)?.into_request_params()?;
    run_request(&engine, params, kind).await
}

pub(crate) fn call_sync(
    _engine: &EngineEntry,
    method: &str,
    _args: &str,
    _kind: ResultKind,
) -> Result<Payload, HostError> {
    Err(HostError::unknown_method("engine", method))
}

/// Test-support `requests.<name>` hook (the prefix is stripped):
/// - `plan.<method>`: plan a typed request from its argument object, run the
///   same validation as a real call, and return the planned `RequestInput` JSON;
/// - `input`: parse and validate a generic `RequestInput` like core `request`
///   and return it as JSON (absent fields omitted).
///
/// Both answer `JSON` only and never open a session.
#[cfg(feature = "test-support")]
pub(crate) fn test_call(name: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    let input = if let Some(method) = name.strip_prefix("plan.") {
        kind.expect(ResultKind::Json, name)?;
        request::plan(method, args)?
    } else if name == "input" {
        kind.expect(ResultKind::Json, name)?;
        request::RequestInput::from_json(args)?
    } else {
        return Err(HostError::unknown_method("test-support", name));
    };
    input.clone().into_request_params()?;
    Ok(Payload::Json(input.to_json()))
}

/// Async test-support `requests.<name>` hook: no async hooks exist.
#[cfg(feature = "test-support")]
pub(crate) async fn test_call_async(
    name: &str,
    _args: &str,
    _kind: ResultKind,
) -> Result<Payload, HostError> {
    Err(HostError::unknown_method("test-support", name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_typed_wrapper_is_an_async_engine_method() {
        for method in [
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
        ] {
            assert!(ASYNC.contains(&method), "{method}");
        }
        assert!(SYNC.is_empty());
    }

    #[cfg(feature = "test-support")]
    mod hooks {
        use xbbg_host::errors::HostErrorKind;

        use super::*;

        fn json(payload: Payload) -> serde_json::Value {
            match payload {
                Payload::Json(text) => serde_json::from_str(&text).unwrap(),
                other => panic!("expected JSON, got {other:?}"),
            }
        }

        #[test]
        fn plan_hook_returns_the_planned_request() {
            let planned = json(
                test_call(
                    "plan.bdp",
                    r#"{"tickers":["IBM US Equity"],"fields":["PX_LAST"],"onDelayed":"warn"}"#,
                    ResultKind::Json,
                )
                .unwrap(),
            );
            assert_eq!(
                planned,
                serde_json::json!({
                    "service": "//blp/refdata",
                    "operation": "ReferenceDataRequest",
                    "extractor": "refdata",
                    "securities": ["IBM US Equity"],
                    "fields": ["PX_LAST"],
                    "includeSecurityErrors": false,
                    "onDelayed": "warn"
                })
            );
        }

        #[test]
        fn plan_hook_reports_the_same_errors_as_a_real_call() {
            let delayed = test_call(
                "plan.bdh",
                r#"{"tickers":["X"],"fields":["Y"],"onDelayed":"WARN"}"#,
                ResultKind::Json,
            )
            .unwrap_err();
            assert_eq!(delayed.kind, HostErrorKind::Validation);
            assert_eq!(delayed.context.element.as_deref(), Some("onDelayed"));

            let shape = test_call("plan.bdp", r#"{"tickers":[]}"#, ResultKind::Json).unwrap_err();
            assert_eq!(shape.kind, HostErrorKind::Argument);
            assert_eq!(shape.message, "missing field 'fields'");

            let unknown = test_call("plan.nope", "{}", ResultKind::Json).unwrap_err();
            assert_eq!(unknown.message, "unknown engine method 'nope'");
        }

        #[test]
        fn hooks_answer_json_only() {
            let error = test_call(
                "plan.bdp",
                r#"{"tickers":[],"fields":[]}"#,
                ResultKind::Table,
            )
            .unwrap_err();
            assert_eq!(error.kind, HostErrorKind::Internal);
            assert_eq!(error.message, "method 'plan.bdp' returns JSON, not TABLE");
            let missing = test_call("nope", "{}", ResultKind::Json).unwrap_err();
            assert_eq!(missing.message, "unknown test-support method 'nope'");
        }

        #[test]
        fn input_hook_validates_generic_requests() {
            let echoed = json(
                test_call(
                    "input",
                    r#"{"service":"//blp/refdata","operation":"ReferenceDataRequest","jsonElements":"{\"a\":1}"}"#,
                    ResultKind::Json,
                )
                .unwrap(),
            );
            assert_eq!(echoed["jsonElements"], "{\"a\":1}");
            let invalid = test_call(
                "input",
                r#"{"service":"//blp/refdata","operation":"ReferenceDataRequest","jsonElements":"[]"}"#,
                ResultKind::Json,
            )
            .unwrap_err();
            assert_eq!(invalid.kind, HostErrorKind::Validation);
            assert_eq!(
                invalid.message,
                "jsonElements must be a JSON object at the top level"
            );
        }
    }
}
