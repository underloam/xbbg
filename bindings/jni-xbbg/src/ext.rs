//! Engine-free `ext` utilities (`NativeXbbg.extCall` / `extCallBytes`).
//!
//! Function names, argument objects and results are those of
//! [`xbbg_host::recipes::ext`] (the 48 napi `ext*` functions without the
//! prefix). Each function has one natural result kind (VOID, BOOL, STRING or
//! JSON); asking for another kind is an INTERNAL error raised before the
//! function runs, so a mismatched call never mutates the exchange-override
//! registry. `pivotToWide` and `isLongFormat` take Arrow IPC stream bytes
//! through `extCallBytes`.

use xbbg_host::errors::HostError;
use xbbg_host::recipes::ext::{self, ExtKind, ExtValue};

use crate::payload::{Payload, ResultKind};

/// `extCall(fn, argsJson, kind)`.
pub(crate) fn call(function: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    let natural =
        ext::kind_of(function).ok_or_else(|| HostError::unknown_method("ext", function))?;
    kind.expect(result_kind(natural), function)?;
    Ok(match ext::call(function, args)? {
        ExtValue::Void => Payload::Void,
        ExtValue::Bool(value) => Payload::Bool(value),
        ExtValue::Str(text) => Payload::Str(text),
        ExtValue::Json(text) => Payload::Json(text),
    })
}

/// `extCallBytes(fn, input)`: Arrow IPC stream in, bytes out.
pub(crate) fn call_bytes(function: &str, input: &[u8]) -> Result<Vec<u8>, HostError> {
    ext::call_bytes(function, input)
}

const fn result_kind(kind: ExtKind) -> ResultKind {
    match kind {
        ExtKind::Void => ResultKind::Void,
        ExtKind::Bool => ResultKind::Bool,
        ExtKind::Str => ResultKind::Str,
        ExtKind::Json => ResultKind::Json,
    }
}

#[cfg(test)]
mod tests {
    use xbbg_host::errors::HostErrorKind;

    use super::*;

    #[test]
    fn results_use_their_natural_kind() {
        assert!(matches!(
            call("fmtDate", r#"{"year":2024,"month":1,"day":5}"#, ResultKind::Str),
            Ok(Payload::Str(text)) if text == "20240105"
        ));
        assert!(matches!(
            call(
                "sameCurrency",
                r#"{"ccy1":"usd","ccy2":"USD"}"#,
                ResultKind::Bool
            ),
            Ok(Payload::Bool(true))
        ));
        assert!(matches!(
            call("getMonthCode", r#"{"monthName":"March"}"#, ResultKind::Json),
            Ok(Payload::Json(text)) if text == "null"
        ));
        assert!(matches!(
            call(
                "validateGenericTicker",
                r#"{"ticker":"ES1 Index"}"#,
                ResultKind::Void
            ),
            Ok(Payload::Void)
        ));
    }

    #[test]
    fn a_wrong_kind_fails_before_the_function_runs() {
        let ticker = "JNI_EXT_KIND_TEST Equity";
        let set = format!(r#"{{"ticker":"{ticker}","input":{{"timezone":"Asia/Tokyo"}}}}"#);
        let error = call("setExchangeOverride", &set, ResultKind::Json).expect_err("wrong kind");
        assert_eq!(error.kind, HostErrorKind::Internal);
        assert_eq!(
            error.message,
            "method 'setExchangeOverride' returns VOID, not JSON"
        );
        let get = format!(r#"{{"ticker":"{ticker}"}}"#);
        assert!(matches!(
            call("getExchangeOverride", &get, ResultKind::Json),
            Ok(Payload::Json(text)) if text == "null"
        ));
    }

    #[test]
    fn unknown_functions_are_internal() {
        let error = call("nope", "{}", ResultKind::Json).expect_err("unknown");
        assert_eq!(error.kind, HostErrorKind::Internal);
        assert_eq!(error.message, "unknown ext method 'nope'");
        let error = call_bytes("fmtDate", &[]).expect_err("not a byte function");
        assert_eq!(error.message, "unknown ext method 'fmtDate'");
    }

    #[test]
    fn byte_functions_reject_bad_input_as_argument_errors() {
        let error = call_bytes("isLongFormat", b"nope").expect_err("garbage");
        assert_eq!(error.kind, HostErrorKind::Argument);
    }
}
