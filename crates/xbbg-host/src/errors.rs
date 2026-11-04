//! Host-neutral error classification for language bindings.
//!
//! Every native failure a binding reports is a [`HostError`]: a stable
//! [`HostErrorKind`], the user-facing message and optional structured
//! [`ErrorContext`]. Engine, core and recipe errors are classified through
//! [`ErrorPresentation`] with [`ErrorStyle::JavaScript`] wording, so messages
//! match `@xbbg/core` without its `[XBBG:CODE]` transport prefix. Host-only
//! failures (malformed arguments, closed handles, cancellation) use the
//! explicit constructors; nothing is classified by parsing message text.
//!
//! Kind codes (stable; the JVM binding maps them to exception classes):
//!
//! | code | kind | meaning |
//! |---|---|---|
//! | 1 | `SESSION` | session start, service open, every worker down |
//! | 2 | `REQUEST` | Bloomberg request or subscription failure |
//! | 3 | `LIMIT` | request failure caused by a capacity limit |
//! | 4 | `DATALOSS` | terminal subscription data loss |
//! | 5 | `VALIDATION` | the engine rejected caller input (config, request, schema) |
//! | 6 | `TIMEOUT` | request or lifecycle timeout |
//! | 7 | `INTERNAL` | internal failure, including native panics |
//! | 8 | `ARGUMENT` | the argument JSON itself is malformed |
//! | 9 | `STATE` | a closed engine or subscription handle |
//! | 10 | `CANCELLED` | the caller cancelled the call |
//! | 11 | `SDK_NOT_FOUND` | no Bloomberg runtime could be located |
//!
//! [`HostError::context_json`] renders the context as one camelCase JSON
//! object with only the present keys: `service`, `operation`, `requestId`,
//! `bloombergRequestIds` (string array), `topic`, `detail`, `element`,
//! `suggestion`, `code`.

use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;
use xbbg_async::BlpAsyncError;
use xbbg_async::error_presentation::{ErrorKind, ErrorPresentation, ErrorStyle};
use xbbg_core::BlpError;
use xbbg_recipes::RecipeError;

/// Stable error families shared by every binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HostErrorKind {
    Session,
    Request,
    Limit,
    DataLoss,
    Validation,
    Timeout,
    Internal,
    Argument,
    State,
    Cancelled,
    SdkNotFound,
}

impl HostErrorKind {
    /// Every kind, in code order.
    pub const ALL: [Self; 11] = [
        Self::Session,
        Self::Request,
        Self::Limit,
        Self::DataLoss,
        Self::Validation,
        Self::Timeout,
        Self::Internal,
        Self::Argument,
        Self::State,
        Self::Cancelled,
        Self::SdkNotFound,
    ];

    /// The stable integer code (1..=11) a host uses to pick its error class.
    pub const fn code(self) -> i32 {
        match self {
            Self::Session => 1,
            Self::Request => 2,
            Self::Limit => 3,
            Self::DataLoss => 4,
            Self::Validation => 5,
            Self::Timeout => 6,
            Self::Internal => 7,
            Self::Argument => 8,
            Self::State => 9,
            Self::Cancelled => 10,
            Self::SdkNotFound => 11,
        }
    }

    /// The stable upper-case name (`SESSION`, `REQUEST`, ...).
    pub const fn name(self) -> &'static str {
        match self {
            Self::Session => "SESSION",
            Self::Request => "REQUEST",
            Self::Limit => "LIMIT",
            Self::DataLoss => "DATALOSS",
            Self::Validation => "VALIDATION",
            Self::Timeout => "TIMEOUT",
            Self::Internal => "INTERNAL",
            Self::Argument => "ARGUMENT",
            Self::State => "STATE",
            Self::Cancelled => "CANCELLED",
            Self::SdkNotFound => "SDK_NOT_FOUND",
        }
    }

    /// The kind with this code, if any.
    pub fn from_code(code: i32) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.code() == code)
    }

    /// The kind with this name (exact, upper case), if any.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }
}

impl From<ErrorKind> for HostErrorKind {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Session => Self::Session,
            ErrorKind::Request => Self::Request,
            ErrorKind::Limit => Self::Limit,
            ErrorKind::DataLoss => Self::DataLoss,
            ErrorKind::Validation => Self::Validation,
            ErrorKind::Timeout => Self::Timeout,
            ErrorKind::Internal => Self::Internal,
        }
    }
}

impl fmt::Display for HostErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Structured attributes attached to a [`HostError`]; every member is optional.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// xbbg's request label (`requestId` request option), not an SDK id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Bloomberg support request ids, sorted and deduplicated by the engine.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bloomberg_request_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl ErrorContext {
    /// Whether no attribute is present.
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// A classified native failure, ready for a host to raise.
///
/// The context is boxed so `Result<_, HostError>` stays small.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostError {
    pub kind: HostErrorKind,
    pub message: String,
    pub context: Box<ErrorContext>,
}

impl HostError {
    /// An error of `kind` without context.
    pub fn new(kind: HostErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            context: Box::default(),
        }
    }

    /// The argument JSON is malformed (wrong shape, unknown field, bad type).
    pub fn argument(message: impl Into<String>) -> Self {
        Self::new(HostErrorKind::Argument, message)
    }

    /// The engine rejects caller input (configuration, request, options).
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(HostErrorKind::Validation, message)
    }

    /// An internal failure that no caller input explains.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(HostErrorKind::Internal, message)
    }

    /// The target handle is closed or was never valid.
    pub fn state(message: impl Into<String>) -> Self {
        Self::new(HostErrorKind::State, message)
    }

    /// The caller cancelled the call before it completed.
    pub fn cancelled() -> Self {
        Self::new(HostErrorKind::Cancelled, "cancelled")
    }

    /// No Bloomberg runtime could be located or provisioned.
    pub fn sdk_not_found(message: impl Into<String>) -> Self {
        Self::new(HostErrorKind::SdkNotFound, message)
    }

    /// An engine handle that is closed or unknown.
    pub fn engine_closed() -> Self {
        Self::state("engine is closed")
    }

    /// A control call on a subscription whose consumer already closed (napi text).
    pub fn subscription_closed() -> Self {
        Self::validation("subscription already closed")
    }

    /// A dispatch name that no area module implements.
    pub fn unknown_method(scope: &str, method: &str) -> Self {
        Self::internal(format!("unknown {scope} method '{method}'"))
    }

    /// A Rust panic caught at a native boundary.
    pub fn native_panic(payload: &(dyn std::any::Any + Send)) -> Self {
        let message = if let Some(text) = payload.downcast_ref::<&'static str>() {
            (*text).to_owned()
        } else if let Some(text) = payload.downcast_ref::<String>() {
            text.clone()
        } else {
            "unknown panic payload".to_owned()
        };
        Self::internal(format!("native panic: {message}"))
    }

    /// Attach the offending option or element name.
    pub fn with_element(mut self, element: impl Into<String>) -> Self {
        self.context.element = Some(element.into());
        self
    }

    /// Attach a suggested replacement value.
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.context.suggestion = Some(suggestion.into());
        self
    }

    /// Attach the Bloomberg service URI.
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.context.service = Some(service.into());
        self
    }

    /// Attach the Bloomberg operation name.
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.context.operation = Some(operation.into());
        self
    }

    /// Attach xbbg's request label.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.context.request_id = Some(request_id.into());
        self
    }

    /// Attach Bloomberg support request ids (replaces any present).
    pub fn with_bloomberg_request_ids<I, S>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.context.bloomberg_request_ids = ids.into_iter().map(Into::into).collect();
        self
    }

    /// Attach a subscription topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.context.topic = Some(topic.into());
        self
    }

    /// Attach a data-loss or failure detail.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.context.detail = Some(detail.into());
        self
    }

    /// Attach a host-specific error code.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.context.code = Some(code.into());
        self
    }

    /// Convert an engine classification, keeping request and data-loss context.
    pub fn from_presentation(presentation: ErrorPresentation) -> Self {
        let mut context = ErrorContext::default();
        if let Some(request) = presentation.request_context {
            context.service = request.service;
            context.operation = request.operation;
            context.request_id = request.request_id;
            context.bloomberg_request_ids = request
                .bloomberg_request_ids
                .iter()
                .map(|id| id.to_string())
                .collect();
        }
        if let Some(data_loss) = presentation.data_loss {
            context.topic = Some(data_loss.topic);
            context.detail = Some(data_loss.detail);
        }
        Self {
            kind: presentation.kind.into(),
            message: presentation.message,
            context: Box::new(context),
        }
    }

    /// Classify a core error; validation suggestions become context.
    pub fn from_blp(error: BlpError) -> Self {
        let suggestion = match &error {
            BlpError::Validation { errors, .. } => {
                errors.iter().find_map(|error| error.suggestion.clone())
            }
            _ => None,
        };
        let mut host =
            Self::from_presentation(ErrorPresentation::from_blp(error, ErrorStyle::JavaScript));
        host.context.suggestion = suggestion;
        host
    }

    /// Classify an engine error with the same rules as subscription streams.
    pub fn from_async(error: BlpAsyncError) -> Self {
        match error {
            BlpAsyncError::Blp(error) => Self::from_blp(error),
            other => Self::from_presentation(ErrorPresentation::from_async(
                other,
                ErrorStyle::JavaScript,
            )),
        }
    }

    /// Classify a subscription control failure: a closed channel means the
    /// consumer is gone (napi `subscription_control_error_to_napi`).
    pub fn from_subscription_control(error: BlpAsyncError) -> Self {
        match error {
            BlpAsyncError::ChannelClosed => Self::subscription_closed(),
            other => Self::from_async(other),
        }
    }

    /// Classify a recipe failure exactly like napi `recipe_error_to_napi`:
    /// engine errors keep their engine classification, invalid arguments are
    /// VALIDATION `Invalid argument: {detail}`, every other recipe failure is
    /// INTERNAL with its display text, and contributor support ids are kept.
    pub fn from_recipe(error: RecipeError) -> Self {
        let (source, request_ids) = error.into_request_context();
        let mut host = match source {
            RecipeError::Engine(inner) => Self::from_async(*inner),
            RecipeError::InvalidArgument(detail) => {
                Self::validation(format!("Invalid argument: {detail}"))
            }
            other => Self::internal(other.to_string()),
        };
        if !request_ids.is_empty() {
            host.context.bloomberg_request_ids =
                request_ids.iter().map(|id| id.to_string()).collect();
        }
        host
    }

    /// The context as one JSON object, or `None` when no attribute is present.
    pub fn context_json(&self) -> Option<String> {
        if self.context.is_empty() {
            return None;
        }
        Some(serde_json::to_string(&self.context).expect("error context strings serialize"))
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HostError {}

impl From<BlpAsyncError> for HostError {
    fn from(error: BlpAsyncError) -> Self {
        Self::from_async(error)
    }
}

impl From<BlpError> for HostError {
    fn from(error: BlpError) -> Self {
        Self::from_blp(error)
    }
}

impl From<RecipeError> for HostError {
    fn from(error: RecipeError) -> Self {
        Self::from_recipe(error)
    }
}

/// Deserialize a binding's JSON argument object.
///
/// Every failure is ARGUMENT: an unknown member reads `unknown field '<name>'`;
/// other serde texts keep their wording with backticks shown as quotes and
/// without the line/column suffix (the JSON is generated by the host).
pub fn parse_args<T: DeserializeOwned>(json: &str) -> Result<T, HostError> {
    serde_json::from_str(json).map_err(|error| HostError::from_json(&error))
}

impl HostError {
    /// An ARGUMENT error for malformed argument JSON (see [`parse_args`]).
    pub fn from_json(error: &serde_json::Error) -> Self {
        Self::argument(json_error_message(error))
    }
}

/// The user-facing text of a JSON argument error (see [`parse_args`]).
pub fn json_error_message(error: &serde_json::Error) -> String {
    let mut text = error.to_string();
    if error.line() != 0 {
        let suffix = format!(" at line {} column {}", error.line(), error.column());
        if text.ends_with(&suffix) {
            text.truncate(text.len() - suffix.len());
        }
    }
    if let Some(rest) = text.strip_prefix("unknown field `")
        && let Some(end) = rest.find('`')
    {
        return format!("unknown field '{}'", &rest[..end]);
    }
    text.replace('`', "'")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Deserialize;
    use xbbg_core::errors::ValidationError;

    use super::*;

    #[test]
    fn kind_codes_and_names_are_stable_and_round_trip() {
        let expected = [
            (1, "SESSION"),
            (2, "REQUEST"),
            (3, "LIMIT"),
            (4, "DATALOSS"),
            (5, "VALIDATION"),
            (6, "TIMEOUT"),
            (7, "INTERNAL"),
            (8, "ARGUMENT"),
            (9, "STATE"),
            (10, "CANCELLED"),
            (11, "SDK_NOT_FOUND"),
        ];
        for (kind, (code, name)) in HostErrorKind::ALL.into_iter().zip(expected) {
            assert_eq!((kind.code(), kind.name()), (code, name));
            assert_eq!(HostErrorKind::from_code(code), Some(kind));
            assert_eq!(HostErrorKind::from_name(name), Some(kind));
        }
        assert_eq!(HostErrorKind::from_code(0), None);
        assert_eq!(HostErrorKind::from_code(12), None);
        assert_eq!(HostErrorKind::from_name("session"), None);
    }

    #[test]
    fn request_failure_keeps_structured_context_and_javascript_text() {
        let error = HostError::from_blp(BlpError::RequestFailure {
            service: "//blp/refdata".into(),
            operation: Some("ReferenceDataRequest".into()),
            cid: None,
            label: Some("category=BAD_SEC".into()),
            request_id: Some("label-1".into()),
            bloomberg_request_ids: vec![Arc::from("sdk-a"), Arc::from("sdk-b")],
            source: None,
        });
        assert_eq!(error.kind, HostErrorKind::Request);
        assert_eq!(
            error.message,
            "Request failed on //blp/refdata::ReferenceDataRequest [request_id=label-1] - category=BAD_SEC"
        );
        assert_eq!(error.context.service.as_deref(), Some("//blp/refdata"));
        assert_eq!(
            error.context.operation.as_deref(),
            Some("ReferenceDataRequest")
        );
        assert_eq!(error.context.request_id.as_deref(), Some("label-1"));
        assert_eq!(error.context.bloomberg_request_ids, ["sdk-a", "sdk-b"]);
        assert_eq!(
            error.context_json().as_deref(),
            Some(
                r#"{"service":"//blp/refdata","operation":"ReferenceDataRequest","requestId":"label-1","bloombergRequestIds":["sdk-a","sdk-b"]}"#
            )
        );
    }

    #[test]
    fn capacity_failures_are_limit_errors() {
        let error = HostError::from_blp(BlpError::RequestFailure {
            service: "//blp/refdata".into(),
            operation: None,
            cid: None,
            label: Some("subcategory=DAILY_CAPACITY_REACHED".into()),
            request_id: None,
            bloomberg_request_ids: Vec::new(),
            source: None,
        });
        assert_eq!(error.kind, HostErrorKind::Limit);
        assert_eq!(error.context.service.as_deref(), Some("//blp/refdata"));
        assert_eq!(error.context.operation, None);
    }

    #[test]
    fn data_loss_uses_javascript_wording_and_structured_topic() {
        let error = HostError::from_async(BlpAsyncError::Blp(BlpError::SubscriptionDataLoss {
            topic: "IBM US Equity".into(),
            detail: "queue full".into(),
        }));
        assert_eq!(error.kind, HostErrorKind::DataLoss);
        assert_eq!(
            error.message,
            "Subscription data loss [topic=IBM US Equity]: queue full"
        );
        assert_eq!(error.context.topic.as_deref(), Some("IBM US Equity"));
        assert_eq!(error.context.detail.as_deref(), Some("queue full"));
    }

    #[test]
    fn async_variants_map_to_their_kinds() {
        let cases = [
            (
                BlpAsyncError::ConfigError {
                    detail: "bad".into(),
                },
                HostErrorKind::Validation,
                "Configuration error: bad",
            ),
            (
                BlpAsyncError::Internal("boom".into()),
                HostErrorKind::Internal,
                "boom",
            ),
            (
                BlpAsyncError::ChannelClosed,
                HostErrorKind::Internal,
                "Channel closed unexpectedly",
            ),
            (
                BlpAsyncError::AllWorkersDown { pool_size: 2 },
                HostErrorKind::Session,
                "All 2 request workers are down",
            ),
            (
                BlpAsyncError::Blp(BlpError::SessionStart {
                    source: None,
                    label: Some("refused".into()),
                }),
                HostErrorKind::Session,
                "Session start failed: refused",
            ),
            (
                BlpAsyncError::Blp(BlpError::Timeout),
                HostErrorKind::Timeout,
                "Request timed out",
            ),
            (
                BlpAsyncError::Blp(BlpError::InvalidArgument {
                    detail: "nope".into(),
                }),
                HostErrorKind::Validation,
                "Invalid argument: nope",
            ),
        ];
        for (error, kind, message) in cases {
            let host = HostError::from_async(error);
            assert_eq!((host.kind, host.message.as_str()), (kind, message));
            assert_eq!(host.context_json(), None);
        }
    }

    #[test]
    fn timeout_with_request_label_carries_ids() {
        let error = HostError::from_blp(BlpError::RequestTimeout {
            request_id: Some("r1".into()),
            bloomberg_request_ids: vec![Arc::from("sdk-1")],
        });
        assert_eq!(error.kind, HostErrorKind::Timeout);
        assert_eq!(error.message, "Request timed out [request_id=r1]");
        assert_eq!(error.context.request_id.as_deref(), Some("r1"));
        assert_eq!(error.context.bloomberg_request_ids, ["sdk-1"]);
        assert_eq!(error.context.service, None);
    }

    #[test]
    fn validation_suggestion_is_structured() {
        let error = HostError::from_blp(BlpError::Validation {
            message: "invalid request".into(),
            errors: vec![
                ValidationError {
                    path: "fields[0]".into(),
                    message: "unknown field".into(),
                    suggestion: None,
                },
                ValidationError {
                    path: "fields[1]".into(),
                    message: "unknown field".into(),
                    suggestion: Some("PX_LAST".into()),
                },
            ],
        });
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(
            error.message,
            "invalid request: fields[0]: unknown field; fields[1]: unknown field (did you mean 'PX_LAST'?)"
        );
        assert_eq!(error.context.suggestion.as_deref(), Some("PX_LAST"));
        assert_eq!(error.context.element, None);
    }

    #[test]
    fn recipe_errors_follow_napi_rules() {
        let invalid = HostError::from_recipe(RecipeError::InvalidArgument("dates".into()));
        assert_eq!(invalid.kind, HostErrorKind::Validation);
        assert_eq!(invalid.message, "Invalid argument: dates");

        let other = HostError::from_recipe(RecipeError::Other("empty".into()));
        assert_eq!(other.kind, HostErrorKind::Internal);
        assert_eq!(other.message, "Recipe error: empty");

        let engine = HostError::from_recipe(RecipeError::Engine(Box::new(
            BlpAsyncError::AllWorkersDown { pool_size: 1 },
        )));
        assert_eq!(engine.kind, HostErrorKind::Session);

        let with_ids = HostError::from_recipe(
            RecipeError::Other("partial".into())
                .with_bloomberg_request_ids([Arc::from("sdk-z"), Arc::from("sdk-a")]),
        );
        assert_eq!(with_ids.kind, HostErrorKind::Internal);
        assert_eq!(with_ids.message, "Recipe error: partial");
        assert_eq!(with_ids.context.bloomberg_request_ids, ["sdk-a", "sdk-z"]);
    }

    #[test]
    fn subscription_control_maps_closed_channel() {
        let closed = HostError::from_subscription_control(BlpAsyncError::ChannelClosed);
        assert_eq!(closed, HostError::subscription_closed());
        assert_eq!(closed.kind, HostErrorKind::Validation);
        let other = HostError::from_subscription_control(BlpAsyncError::Internal("x".into()));
        assert_eq!(other.kind, HostErrorKind::Internal);
    }

    #[test]
    fn host_constructors_and_context_builders() {
        let error = HostError::validation("bad option")
            .with_element("overflowPolicy")
            .with_suggestion("drop_oldest")
            .with_code("E1");
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(
            error.context_json().as_deref(),
            Some(r#"{"element":"overflowPolicy","suggestion":"drop_oldest","code":"E1"}"#)
        );
        assert_eq!(HostError::engine_closed().kind, HostErrorKind::State);
        assert_eq!(HostError::engine_closed().message, "engine is closed");
        assert_eq!(HostError::cancelled().kind, HostErrorKind::Cancelled);
        assert_eq!(
            HostError::unknown_method("engine", "nope").message,
            "unknown engine method 'nope'"
        );
        let panic = HostError::native_panic(&String::from("index out of bounds"));
        assert_eq!(panic.kind, HostErrorKind::Internal);
        assert_eq!(panic.message, "native panic: index out of bounds");
        assert_eq!(
            HostError::native_panic(&"static text").message,
            "native panic: static text"
        );
        assert_eq!(
            HostError::native_panic(&42_u8).message,
            "native panic: unknown panic payload"
        );
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    #[allow(dead_code)]
    struct Args {
        ticker: String,
        max_rows: Option<u32>,
    }

    #[test]
    fn parse_args_reports_argument_errors_without_positions() {
        let unknown = parse_args::<Args>(r#"{"ticker":"IBM","bogus":1}"#).unwrap_err();
        assert_eq!(unknown.kind, HostErrorKind::Argument);
        assert_eq!(unknown.message, "unknown field 'bogus'");

        let missing = parse_args::<Args>("{}").unwrap_err();
        assert_eq!(missing.message, "missing field 'ticker'");

        let wrong = parse_args::<Args>(r#"{"ticker":"IBM","maxRows":-1}"#).unwrap_err();
        assert_eq!(wrong.message, "invalid value: integer '-1', expected u32");

        let syntax = parse_args::<Args>("{").unwrap_err();
        assert_eq!(syntax.kind, HostErrorKind::Argument);
        assert!(!syntax.message.contains("line"), "{}", syntax.message);

        let parsed = parse_args::<Args>(r#"{"ticker":"IBM","maxRows":5}"#).unwrap();
        assert_eq!((parsed.ticker.as_str(), parsed.max_rows), ("IBM", Some(5)));
    }
}
