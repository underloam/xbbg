//! Error coding and mapping between `xbbg-async` / `xbbg-host` / `xbbg-recipes` and N-API.

use napi::bindgen_prelude::{Error, Status};
use xbbg_async::BlpAsyncError;
use xbbg_async::error_presentation::{ErrorKind, ErrorPresentation, ErrorStyle};
#[cfg(test)]
use xbbg_core::BlpError;
use xbbg_host::errors::{HostError, HostErrorKind};

/// Prefix an N-API error message with `[XBBG:<CODE>] ` so the JS layer can
/// construct typed error classes; the human-readable message follows it.
/// Codes: SESSION, REQUEST, LIMIT, DATALOSS, VALIDATION, TIMEOUT, INTERNAL.
/// DELIVERY_CONVERSION marks an uncommitted, retryable host-export failure.
pub(crate) fn coded(status: Status, code: &str, msg: impl AsRef<str>) -> Error {
    Error::new(status, format!("[XBBG:{code}] {}", msg.as_ref()))
}

pub(crate) fn error_presentation_to_napi(presentation: ErrorPresentation) -> Error {
    let (status, code) = match presentation.kind {
        ErrorKind::Session => (Status::GenericFailure, "SESSION"),
        ErrorKind::Request => (Status::GenericFailure, "REQUEST"),
        ErrorKind::Limit => (Status::GenericFailure, "LIMIT"),
        ErrorKind::DataLoss => (Status::GenericFailure, "DATALOSS"),
        ErrorKind::Validation => (Status::InvalidArg, "VALIDATION"),
        ErrorKind::Timeout => (Status::GenericFailure, "TIMEOUT"),
        ErrorKind::Internal => (Status::GenericFailure, "INTERNAL"),
    };
    if let Some(context) = presentation.request_context
        && !context.bloomberg_request_ids.is_empty()
    {
        let ids: Vec<&str> = context
            .bloomberg_request_ids
            .iter()
            .map(AsRef::as_ref)
            .collect();
        // One JSON line is unambiguous even when labels or IDs contain brackets.
        // The JS adapter removes the transport envelope from the user message.
        let context = serde_json::json!({
            "service": context.service,
            "operation": context.operation,
            "request_id": context.request_id,
            "bloomberg_request_id": if ids.len() == 1 { Some(ids[0]) } else { None },
            "bloomberg_request_ids": ids,
        });
        return coded(
            status,
            code,
            format!("[XBBG_CONTEXT]{context}\n{}", presentation.message),
        );
    }
    coded(status, code, presentation.message)
}

#[cfg(test)]
pub(crate) fn blp_error_to_napi(error: BlpError) -> Error {
    error_presentation_to_napi(ErrorPresentation::from_blp(error, ErrorStyle::JavaScript))
}

pub(crate) fn blp_async_error_to_napi(error: BlpAsyncError) -> Error {
    error_presentation_to_napi(ErrorPresentation::from_async(error, ErrorStyle::JavaScript))
}

/// Encode a host error as `[XBBG:<KIND>] message`. A non-empty context rides in
/// the `[XBBG_CONTEXT]` JSON line the js-xbbg adapter decodes and strips.
pub(crate) fn host_error_to_napi(error: HostError) -> Error {
    let status = match error.kind {
        HostErrorKind::Validation | HostErrorKind::Argument => Status::InvalidArg,
        _ => Status::GenericFailure,
    };
    let code = error.kind.name();
    let context = *error.context;
    if context.is_empty() {
        return coded(status, code, error.message);
    }
    let mut json = serde_json::Map::new();
    let mut put = |key: &str, value: Option<String>| {
        if let Some(value) = value {
            json.insert(key.to_owned(), value.into());
        }
    };
    put("service", context.service);
    put("operation", context.operation);
    put("request_id", context.request_id);
    put("topic", context.topic);
    put("detail", context.detail);
    put("element", context.element);
    put("suggestion", context.suggestion);
    put("code", context.code);
    if let [id] = context.bloomberg_request_ids.as_slice() {
        json.insert("bloomberg_request_id".to_owned(), id.clone().into());
    }
    if !context.bloomberg_request_ids.is_empty() {
        json.insert(
            "bloomberg_request_ids".to_owned(),
            context.bloomberg_request_ids.into(),
        );
    }
    // One JSON line is unambiguous even when messages or ids contain brackets.
    coded(
        status,
        code,
        format!(
            "[XBBG_CONTEXT]{}\n{}",
            serde_json::Value::Object(json),
            error.message
        ),
    )
}

#[cfg(test)]
pub(crate) fn subscription_closed_error() -> Error {
    coded(
        Status::InvalidArg,
        "VALIDATION",
        "subscription already closed",
    )
}

#[cfg(test)]
pub(crate) fn subscription_control_error_to_napi(error: BlpAsyncError) -> Error {
    match error {
        BlpAsyncError::ChannelClosed => subscription_closed_error(),
        error => blp_async_error_to_napi(error),
    }
}

pub(crate) fn recipe_error_to_napi(e: xbbg_recipes::RecipeError) -> Error {
    let (source, request_ids) = e.into_request_context();
    let mut presentation = match source {
        // Classify the original engine error, not its recipe support context.
        xbbg_recipes::RecipeError::Engine(inner) => {
            ErrorPresentation::from_async(*inner, ErrorStyle::JavaScript)
        }
        xbbg_recipes::RecipeError::InvalidArgument(detail) => ErrorPresentation {
            kind: ErrorKind::Validation,
            message: format!("Invalid argument: {detail}"),
            data_loss: None,
            request_context: None,
        },
        other => ErrorPresentation {
            kind: ErrorKind::Internal,
            message: other.to_string(),
            data_loss: None,
            request_context: None,
        },
    };
    if !request_ids.is_empty() {
        let context = presentation.request_context.get_or_insert_with(|| {
            xbbg_async::error_presentation::RequestErrorContext {
                service: None,
                operation: None,
                request_id: None,
                bloomberg_request_ids: Vec::new(),
            }
        });
        context.bloomberg_request_ids = request_ids.iter().cloned().collect();
    }
    error_presentation_to_napi(presentation)
}
