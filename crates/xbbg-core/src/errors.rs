use std::fmt;
use std::sync::Arc;

use thiserror::Error;

/// Unified result type for the core crate.
///
/// All fallible operations return this type for consistent error handling.
pub type Result<T, E = BlpError> = std::result::Result<T, E>;

/// Lightweight CorrelationId context display (string or number).
#[derive(Debug, Clone)]
pub enum CorrelationContext {
    U64(u64),
    Tag(String),
}

impl fmt::Display for CorrelationContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrelationContext::U64(v) => write!(f, "{v}"),
            CorrelationContext::Tag(s) => write!(f, "{s}"),
        }
    }
}

/// Common error type surfaced by xbbg-core.
///
/// All errors from Bloomberg API operations are wrapped in this enum.
/// Use pattern matching or the `thiserror` traits to handle specific error cases.
#[derive(Debug, Error)]
pub enum BlpError {
    #[error("session start failed")]
    SessionStart {
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        label: Option<String>,
    },

    #[error("open service failed")]
    OpenService {
        service: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        label: Option<String>,
    },

    #[error("request failed")]
    RequestFailure {
        service: String,
        operation: Option<String>,
        cid: Option<CorrelationContext>,
        label: Option<String>,
        request_id: Option<String>,
        /// SDK-assigned IDs, separate from the xbbg request label above.
        bloomberg_request_ids: Vec<Arc<str>>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("invalid argument")]
    InvalidArgument { detail: String },

    #[error("operation timed out")]
    Timeout,

    /// A sent data request timed out; lifecycle timeouts use `Timeout`.
    #[error("operation timed out")]
    RequestTimeout {
        request_id: Option<String>,
        bloomberg_request_ids: Vec<Arc<str>>,
    },

    #[error("request template terminated")]
    TemplateTerminated { cid: Option<CorrelationContext> },

    #[error("subscription failure")]
    SubscriptionFailure {
        cid: Option<CorrelationContext>,
        label: Option<String>,
    },

    #[error("subscription data loss for {topic}: {detail}")]
    SubscriptionDataLoss { topic: String, detail: String },

    #[error("internal error: {detail}")]
    Internal { detail: String },

    // Schema validation errors
    #[error("operation not found: {service}::{operation}")]
    SchemaOperationNotFound { service: String, operation: String },

    #[error("schema element not found: {parent}.{name}")]
    SchemaElementNotFound { parent: String, name: String },

    #[error("schema type mismatch at {element}: expected {expected}, found {found}")]
    SchemaTypeMismatch {
        element: String,
        expected: String,
        found: String,
    },

    #[error("unsupported schema construct at {element}: {detail}")]
    SchemaUnsupported { element: String, detail: String },

    #[error("validation error: {message}")]
    Validation {
        message: String,
        errors: Vec<ValidationError>,
    },
}

impl BlpError {
    /// SDK support IDs retained by a failed data request (not correlation IDs).
    pub fn bloomberg_request_ids(&self) -> &[Arc<str>] {
        match self {
            Self::RequestFailure {
                bloomberg_request_ids,
                ..
            }
            | Self::RequestTimeout {
                bloomberg_request_ids,
                ..
            } => bloomberg_request_ids,
            _ => &[],
        }
    }

    /// Union IDs when composing multiple underlying data requests.
    pub fn extend_bloomberg_request_ids(&mut self, ids: impl IntoIterator<Item = Arc<str>>) {
        match self {
            Self::RequestFailure {
                bloomberg_request_ids,
                ..
            }
            | Self::RequestTimeout {
                bloomberg_request_ids,
                ..
            } => {
                bloomberg_request_ids.extend(ids);
                bloomberg_request_ids.sort_unstable();
                bloomberg_request_ids.dedup();
            }
            _ => {}
        }
    }
}

/// Individual validation error with optional suggestion.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
    pub suggestion: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}
