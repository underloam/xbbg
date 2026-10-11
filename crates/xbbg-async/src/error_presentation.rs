//! Shared error classification and messages for native host adapters.
//!
//! Adapters choose their exception class or status code and attach host-specific
//! attributes. Existing message differences are limited to [`ErrorStyle`].

use std::fmt::Write;

use xbbg_core::BlpError;

use crate::BlpAsyncError;

/// Host-independent exception families emitted by the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Session,
    Request,
    Limit,
    DataLoss,
    Validation,
    Timeout,
    Internal,
}

/// Preserve established host wording without duplicating domain classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorStyle {
    Python,
    JavaScript,
}

/// Structured subscription context retained for host exception attributes.
#[derive(Debug)]
pub struct SubscriptionDataLossContext {
    pub topic: String,
    pub detail: String,
}

/// A classified error ready for a host adapter to turn into an exception.
#[derive(Debug)]
pub struct ErrorPresentation {
    pub kind: ErrorKind,
    pub message: String,
    pub data_loss: Option<SubscriptionDataLossContext>,
}

impl ErrorPresentation {
    /// Consume a core error, retaining its domain context in the message.
    pub fn from_blp(error: BlpError, style: ErrorStyle) -> Self {
        let (kind, message) = match error {
            BlpError::SessionStart { source, label } => (
                ErrorKind::Session,
                format_error_msg("Session start failed", label.as_deref(), source.as_deref()),
            ),
            BlpError::OpenService {
                service,
                source,
                label,
            } => {
                let mut message = format!("Failed to open service '{service}': ");
                append_error_msg(&mut message, "", label.as_deref(), source.as_deref());
                (ErrorKind::Session, message)
            }
            BlpError::RequestFailure {
                service,
                operation,
                cid,
                label,
                request_id,
                source,
            } => {
                let kind = if request_failure_is_limit(label.as_deref()) {
                    ErrorKind::Limit
                } else {
                    ErrorKind::Request
                };
                let mut message = format!("Request failed on {service}");
                if let Some(operation) = operation {
                    write!(message, "::{operation}").expect("writing to a String");
                }
                if let Some(cid) = cid {
                    write!(message, " (cid={cid})").expect("writing to a String");
                }
                if let Some(request_id) = request_id {
                    write!(message, " [request_id={request_id}]").expect("writing to a String");
                }
                if let Some(label) = label {
                    write!(message, " - {label}").expect("writing to a String");
                }
                if let Some(source) = source {
                    write!(message, ": {source}").expect("writing to a String");
                }
                (kind, message)
            }
            BlpError::InvalidArgument { detail } => {
                (ErrorKind::Validation, format!("Invalid argument: {detail}"))
            }
            BlpError::Timeout => (ErrorKind::Timeout, "Request timed out".into()),
            BlpError::TemplateTerminated { cid } => {
                let mut message = "Request template terminated".to_string();
                if let Some(cid) = cid {
                    write!(message, " (cid={cid})").expect("writing to a String");
                }
                (ErrorKind::Request, message)
            }
            BlpError::SubscriptionFailure { cid, label } => {
                let mut message = "Subscription failed".to_string();
                if let Some(cid) = cid {
                    write!(message, " (cid={cid})").expect("writing to a String");
                }
                if let Some(label) = label {
                    write!(message, ": {label}").expect("writing to a String");
                }
                (ErrorKind::Request, message)
            }
            BlpError::SubscriptionDataLoss { topic, detail } => {
                let message = match style {
                    ErrorStyle::Python => {
                        format!("Subscription data loss for '{topic}': {detail}")
                    }
                    ErrorStyle::JavaScript => {
                        format!("Subscription data loss [topic={topic}]: {detail}")
                    }
                };
                return Self {
                    kind: ErrorKind::DataLoss,
                    message,
                    data_loss: Some(SubscriptionDataLossContext { topic, detail }),
                };
            }
            BlpError::Internal { detail } => {
                (ErrorKind::Internal, format!("Internal error: {detail}"))
            }
            BlpError::SchemaOperationNotFound { service, operation } => (
                ErrorKind::Validation,
                format!("Operation not found: {service}::{operation}"),
            ),
            BlpError::SchemaElementNotFound { parent, name } => (
                ErrorKind::Validation,
                format!("Schema element not found: {parent}.{name}"),
            ),
            BlpError::SchemaTypeMismatch {
                element,
                expected,
                found,
            } => (
                ErrorKind::Validation,
                match style {
                    ErrorStyle::Python => format!(
                        "Schema type mismatch at {element}: expected {expected:?}, found {found:?}"
                    ),
                    ErrorStyle::JavaScript => format!(
                        "Schema type mismatch at {element}: expected {expected}, found {found}"
                    ),
                },
            ),
            BlpError::SchemaUnsupported { element, detail } => (
                ErrorKind::Validation,
                format!("Unsupported schema construct at {element}: {detail}"),
            ),
            BlpError::Validation {
                mut message,
                errors,
            } => {
                for (index, error) in errors.into_iter().enumerate() {
                    message.push_str(if index == 0 { ": " } else { "; " });
                    write!(message, "{error}").expect("writing to a String");
                    if let Some(suggestion) = error.suggestion {
                        write!(message, " (did you mean '{suggestion}'?)")
                            .expect("writing to a String");
                    }
                }
                (ErrorKind::Validation, message)
            }
        };
        Self {
            kind,
            message,
            data_loss: None,
        }
    }

    /// Consume an async error using the same core classification as stream errors.
    pub fn from_async(error: BlpAsyncError, style: ErrorStyle) -> Self {
        let (kind, message) = match error {
            BlpAsyncError::Blp(error) => return Self::from_blp(error, style),
            BlpAsyncError::Internal(message) => (ErrorKind::Internal, message),
            BlpAsyncError::ConfigError { detail } => (
                ErrorKind::Validation,
                format!("Configuration error: {detail}"),
            ),
            BlpAsyncError::ChannelClosed => {
                (ErrorKind::Internal, "Channel closed unexpectedly".into())
            }
            BlpAsyncError::AllWorkersDown { pool_size } => (
                ErrorKind::Session,
                match style {
                    ErrorStyle::Python => format!(
                        "all {pool_size} request workers are dead — no healthy worker available"
                    ),
                    ErrorStyle::JavaScript => {
                        format!("All {pool_size} request workers are down")
                    }
                },
            ),
        };
        Self {
            kind,
            message,
            data_loss: None,
        }
    }
}

fn request_failure_is_limit(label: Option<&str>) -> bool {
    label.is_some_and(|label| {
        label.contains("category=LIMIT") || label.contains("DAILY_CAPACITY_REACHED")
    })
}

fn format_error_msg(
    base: &str,
    label: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) -> String {
    let mut message = String::new();
    append_error_msg(&mut message, base, label, source);
    message
}

// Service errors add a prefix before the same label/source suffix. Append into
// the final buffer rather than allocating a second formatted message.
fn append_error_msg(
    message: &mut String,
    base: &str,
    label: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) {
    let start = message.len();
    message.push_str(base);
    if let Some(label) = label {
        if message.len() > start {
            message.push_str(": ");
        }
        message.push_str(label);
    }
    if let Some(source) = source {
        if message.len() > start {
            message.push_str(" - ");
        }
        write!(message, "{source}").expect("writing to a String");
    }
    if message.len() == start {
        message.push_str("Unknown error");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xbbg_core::errors::{CorrelationContext, ValidationError};

    #[test]
    fn optional_label_and_source_keep_identical_separators_and_fallback() {
        let source = std::io::Error::other("synthetic cause");
        for (base, label, with_source, expected) in [
            ("", None, false, "Unknown error"),
            ("", Some(""), false, "Unknown error"),
            ("", Some("label"), false, "label"),
            ("", None, true, "synthetic cause"),
            ("", Some("label"), true, "label - synthetic cause"),
            ("base", None, false, "base"),
            ("base", Some(""), false, "base: "),
            ("base", Some("label"), false, "base: label"),
            ("base", None, true, "base - synthetic cause"),
            ("base", Some("label"), true, "base: label - synthetic cause"),
        ] {
            let cause = with_source.then_some(&source as &(dyn std::error::Error + Send + Sync));
            assert_eq!(format_error_msg(base, label, cause), expected);
        }
    }

    #[test]
    fn request_limits_share_classification_and_full_context() {
        for style in [ErrorStyle::Python, ErrorStyle::JavaScript] {
            for (label, kind) in [
                (None, ErrorKind::Request),
                (Some("category=BAD_ARGS"), ErrorKind::Request),
                (Some("category=LIMIT"), ErrorKind::Limit),
                (Some("DAILY_CAPACITY_REACHED"), ErrorKind::Limit),
                (Some("subcategory=DAILY_CAPACITY_REACHED"), ErrorKind::Limit),
            ] {
                let presentation = ErrorPresentation::from_blp(
                    BlpError::RequestFailure {
                        service: "//blp/refdata".into(),
                        operation: Some("ReferenceDataRequest".into()),
                        cid: Some(CorrelationContext::U64(42)),
                        label: label.map(str::to_owned),
                        request_id: Some("synthetic-request".into()),
                        source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                    },
                    style,
                );
                let mut expected = "Request failed on //blp/refdata::ReferenceDataRequest (cid=42) [request_id=synthetic-request]".to_string();
                if let Some(label) = label {
                    write!(expected, " - {label}").unwrap();
                }
                expected.push_str(": synthetic cause");
                assert_eq!(presentation.kind, kind);
                assert_eq!(presentation.message, expected);
                assert!(presentation.data_loss.is_none());
            }
        }
    }

    #[test]
    fn core_errors_keep_their_domain_and_message() {
        for style in [ErrorStyle::Python, ErrorStyle::JavaScript] {
            let cases = [
                (
                    BlpError::SessionStart {
                        source: None,
                        label: Some("synthetic start failure".into()),
                    },
                    ErrorKind::Session,
                    "Session start failed: synthetic start failure",
                ),
                (
                    BlpError::OpenService {
                        service: "//blp/refdata".into(),
                        source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                        label: Some("unavailable".into()),
                    },
                    ErrorKind::Session,
                    "Failed to open service '//blp/refdata': unavailable - synthetic cause",
                ),
                (
                    BlpError::OpenService {
                        service: "//blp/refdata".into(),
                        source: None,
                        label: None,
                    },
                    ErrorKind::Session,
                    "Failed to open service '//blp/refdata': Unknown error",
                ),
                (
                    BlpError::RequestFailure {
                        service: "//blp/refdata".into(),
                        operation: None,
                        cid: None,
                        label: None,
                        request_id: None,
                        source: None,
                    },
                    ErrorKind::Request,
                    "Request failed on //blp/refdata",
                ),
                (
                    BlpError::InvalidArgument {
                        detail: "fields required".into(),
                    },
                    ErrorKind::Validation,
                    "Invalid argument: fields required",
                ),
                (BlpError::Timeout, ErrorKind::Timeout, "Request timed out"),
                (
                    BlpError::TemplateTerminated {
                        cid: Some(CorrelationContext::Tag("synthetic".into())),
                    },
                    ErrorKind::Request,
                    "Request template terminated (cid=synthetic)",
                ),
                (
                    BlpError::TemplateTerminated { cid: None },
                    ErrorKind::Request,
                    "Request template terminated",
                ),
                (
                    BlpError::SubscriptionFailure {
                        cid: Some(CorrelationContext::U64(42)),
                        label: Some("synthetic subscription failure".into()),
                    },
                    ErrorKind::Request,
                    "Subscription failed (cid=42): synthetic subscription failure",
                ),
                (
                    BlpError::SubscriptionFailure {
                        cid: None,
                        label: None,
                    },
                    ErrorKind::Request,
                    "Subscription failed",
                ),
                (
                    BlpError::Internal {
                        detail: "session connection dropped (worker=2)".into(),
                    },
                    ErrorKind::Internal,
                    "Internal error: session connection dropped (worker=2)",
                ),
                (
                    BlpError::SchemaOperationNotFound {
                        service: "//blp/refdata".into(),
                        operation: "SyntheticRequest".into(),
                    },
                    ErrorKind::Validation,
                    "Operation not found: //blp/refdata::SyntheticRequest",
                ),
                (
                    BlpError::SchemaElementNotFound {
                        parent: "request".into(),
                        name: "synthetic".into(),
                    },
                    ErrorKind::Validation,
                    "Schema element not found: request.synthetic",
                ),
                (
                    BlpError::SchemaUnsupported {
                        element: "request.synthetic".into(),
                        detail: "choice".into(),
                    },
                    ErrorKind::Validation,
                    "Unsupported schema construct at request.synthetic: choice",
                ),
                (
                    BlpError::Validation {
                        message: "invalid request".into(),
                        errors: Vec::new(),
                    },
                    ErrorKind::Validation,
                    "invalid request",
                ),
                (
                    BlpError::Validation {
                        message: "invalid request".into(),
                        errors: vec![
                            ValidationError {
                                path: "fields[0]".into(),
                                message: "unknown field".into(),
                                suggestion: Some("PX_LAST".into()),
                            },
                            ValidationError {
                                path: "securities".into(),
                                message: "required".into(),
                                suggestion: None,
                            },
                        ],
                    },
                    ErrorKind::Validation,
                    "invalid request: fields[0]: unknown field (did you mean 'PX_LAST'?); securities: required",
                ),
            ];
            for (error, kind, message) in cases {
                let presentation = ErrorPresentation::from_blp(error, style);
                assert_eq!(presentation.kind, kind);
                assert_eq!(presentation.message, message);
                assert!(presentation.data_loss.is_none());
            }
        }
    }

    #[test]
    fn host_wording_does_not_change_domain_or_structured_context() {
        for (style, data_loss_message, schema_message, session_message) in [
            (
                ErrorStyle::Python,
                "Subscription data loss for 'SYNTHETIC Equity': synthetic overflow",
                "Schema type mismatch at fields: expected \"String\", found \"Int32\"",
                "all 2 request workers are dead — no healthy worker available",
            ),
            (
                ErrorStyle::JavaScript,
                "Subscription data loss [topic=SYNTHETIC Equity]: synthetic overflow",
                "Schema type mismatch at fields: expected String, found Int32",
                "All 2 request workers are down",
            ),
        ] {
            let presentation = ErrorPresentation::from_async(
                BlpError::SubscriptionDataLoss {
                    topic: "SYNTHETIC Equity".into(),
                    detail: "synthetic overflow".into(),
                }
                .into(),
                style,
            );
            assert_eq!(presentation.kind, ErrorKind::DataLoss);
            assert_eq!(presentation.message, data_loss_message);
            let context = presentation.data_loss.unwrap();
            assert_eq!(context.topic, "SYNTHETIC Equity");
            assert_eq!(context.detail, "synthetic overflow");

            let presentation = ErrorPresentation::from_blp(
                BlpError::SchemaTypeMismatch {
                    element: "fields".into(),
                    expected: "String".into(),
                    found: "Int32".into(),
                },
                style,
            );
            assert_eq!(presentation.kind, ErrorKind::Validation);
            assert_eq!(presentation.message, schema_message);

            let presentation = ErrorPresentation::from_async(
                BlpAsyncError::AllWorkersDown { pool_size: 2 },
                style,
            );
            assert_eq!(presentation.kind, ErrorKind::Session);
            assert_eq!(presentation.message, session_message);
        }
    }

    #[test]
    fn async_errors_keep_their_domain_and_message() {
        for style in [ErrorStyle::Python, ErrorStyle::JavaScript] {
            for (error, kind, message) in [
                (
                    BlpError::Timeout.into(),
                    ErrorKind::Timeout,
                    "Request timed out",
                ),
                (
                    BlpAsyncError::Internal("synthetic failure".into()),
                    ErrorKind::Internal,
                    "synthetic failure",
                ),
                (
                    BlpAsyncError::ConfigError {
                        detail: "synthetic config failure".into(),
                    },
                    ErrorKind::Validation,
                    "Configuration error: synthetic config failure",
                ),
                (
                    BlpAsyncError::ChannelClosed,
                    ErrorKind::Internal,
                    "Channel closed unexpectedly",
                ),
            ] {
                let presentation = ErrorPresentation::from_async(error, style);
                assert_eq!(presentation.kind, kind);
                assert_eq!(presentation.message, message);
                assert!(presentation.data_loss.is_none());
            }
        }
    }
}
