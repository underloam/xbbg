//! Recipe engine methods (`bqr`, `yas`, `futTicker`, … — every recipe of the
//! `@xbbg/core` `Engine`).
//!
//! All recipes are async and table-producing (TABLE / IPC_STREAM / IPC_FILE).
//! Argument objects, JavaScript defaults, the `volSurface` encoding and the
//! error mapping live in [`xbbg_host::recipes`] (its module docs list every
//! field). The argument object is parsed and validated before any Bloomberg
//! work; the recipe then runs on the engine and the batch is shaped for the
//! requested kind.
//!
//! Test-support hooks (`NativeTestSupport.call("recipes.<name>", …)`):
//!
//! | name | args | result |
//! |---|---|---|
//! | `prepare.<method>` | `<method>`'s argument object | JSON: the normalized call (a `method` tag plus every value handed to the recipe, defaults applied), parsed exactly like `engineCall` does but without an engine |
//! | `extLongTable` | `{key?}` (`"ticker"`, the default, or `"security"`) | TABLE / IPC_STREAM / IPC_FILE: a long `(key, field, value)` table with rows `AAPL US Equity`/`PX_LAST`/150, `AAPL US Equity`/`VOLUME`/1000000, `MSFT US Equity`/`PX_LAST`/300, input for `pivotToWide` / `isLongFormat` |

use std::sync::Arc;

use xbbg_host::errors::HostError;
use xbbg_host::recipes::{METHODS, RecipeCall};

use crate::engine::EngineEntry;
use crate::payload::{Payload, ResultKind};

/// Async engine methods dispatched here: every recipe.
pub(crate) const ASYNC: &[&str] = METHODS;
/// Sync engine methods dispatched here: none.
pub(crate) const SYNC: &[&str] = &[];

pub(crate) async fn call(
    engine: Arc<EngineEntry>,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    kind.expect_table(method)?;
    let call = RecipeCall::parse(method, args)?;
    let batch = call.run(engine.engine()?).await?;
    Payload::table(batch, kind)
}

pub(crate) fn call_sync(
    _engine: &EngineEntry,
    method: &str,
    _args: &str,
    _kind: ResultKind,
) -> Result<Payload, HostError> {
    Err(HostError::unknown_method("engine", method))
}

/// Test-support `recipes.<name>` hook (the prefix is stripped).
#[cfg(feature = "test-support")]
pub(crate) fn test_call(name: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    if name == "extLongTable" {
        kind.expect_table(name)?;
        let args: LongTableArgs = xbbg_host::errors::parse_args(args)?;
        return Payload::table(long_table(args.key.as_deref().unwrap_or("ticker"))?, kind);
    }
    let Some(method) = name.strip_prefix("prepare.") else {
        return Err(HostError::unknown_method("test-support", name));
    };
    kind.expect(ResultKind::Json, name)?;
    Ok(Payload::Json(RecipeCall::parse(method, args)?.to_json()?))
}

#[cfg(feature = "test-support")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LongTableArgs {
    #[serde(default)]
    key: Option<String>,
}

/// The long `(key, field, value)` sample behind `recipes.extLongTable`.
#[cfg(feature = "test-support")]
fn long_table(key: &str) -> Result<arrow::record_batch::RecordBatch, HostError> {
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    if key != "ticker" && key != "security" {
        return Err(HostError::argument(format!(
            "key must be 'ticker' or 'security', not '{key}'"
        )));
    }
    let schema = Schema::new(vec![
        Field::new(key, DataType::Utf8, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]);
    arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(vec![
                "AAPL US Equity",
                "AAPL US Equity",
                "MSFT US Equity",
            ])),
            Arc::new(StringArray::from(vec!["PX_LAST", "VOLUME", "PX_LAST"])),
            Arc::new(Float64Array::from(vec![150.0, 1_000_000.0, 300.0])),
        ],
    )
    .map_err(|error| HostError::internal(format!("long sample table: {error}")))
}

/// Async test-support `recipes.<name>` hook (the prefix is stripped).
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
    #[cfg(feature = "test-support")]
    use xbbg_host::errors::HostErrorKind;

    use super::*;

    #[test]
    fn every_recipe_is_async_and_none_is_sync() {
        assert_eq!(ASYNC.len(), 24);
        assert!(ASYNC.contains(&"volSurface"));
        assert!(ASYNC.contains(&"delayStatus"));
        assert!(SYNC.is_empty());
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn prepare_returns_the_normalized_call() {
        let payload = test_call(
            "prepare.volSurface",
            r#"{"tickers":["SPX Index"],"preset":null,"fields":{"F":{"tenor":"1M"}}}"#,
            ResultKind::Json,
        )
        .expect("prepared");
        let Payload::Json(text) = payload else {
            panic!("expected JSON, got {payload:?}");
        };
        let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
        assert_eq!(json["method"], "volSurface");
        assert_eq!(json["presets"], serde_json::Value::Null);
        assert_eq!(json["fieldSpecs"], serde_json::json!(["F||1M||"]));
        assert_eq!(json["asDecimal"], true);
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn prepare_reports_parse_errors_and_rejects_other_kinds() {
        let error = test_call(
            "prepare.bqr",
            r#"{"ticker":"X","backend":"json"}"#,
            ResultKind::Json,
        )
        .expect_err("unknown field");
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert_eq!(error.message, "unknown field 'backend'");

        let error = test_call("prepare.bqr", r#"{"ticker":"X"}"#, ResultKind::Table)
            .expect_err("wrong kind");
        assert_eq!(error.kind, HostErrorKind::Internal);

        let error = test_call("bqr", "{}", ResultKind::Json).expect_err("no prefix");
        assert_eq!(error.message, "unknown test-support method 'bqr'");
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn ext_long_table_feeds_the_arrow_ext_functions() {
        for key in ["ticker", "security"] {
            let payload = test_call(
                "extLongTable",
                &format!(r#"{{"key":"{key}"}}"#),
                ResultKind::IpcStream,
            )
            .expect("table");
            let Payload::Bytes(ipc) = payload else {
                panic!("expected bytes, got {payload:?}");
            };
            assert_eq!(
                crate::ext::call_bytes("isLongFormat", &ipc).expect("checked"),
                [1]
            );
            let wide = crate::ext::call_bytes("pivotToWide", &ipc).expect("pivoted");
            assert_eq!(
                crate::ext::call_bytes("isLongFormat", &wide).expect("checked"),
                [0]
            );
        }
        let error =
            test_call("extLongTable", r#"{"key":"isin"}"#, ResultKind::Table).expect_err("bad key");
        assert_eq!(error.kind, HostErrorKind::Argument);
        let error =
            test_call("extLongTable", "{}", ResultKind::Json).expect_err("not a table kind");
        assert_eq!(error.kind, HostErrorKind::Internal);
    }
}
