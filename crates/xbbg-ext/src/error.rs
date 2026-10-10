//! Error types for xbbg-ext.

use thiserror::Error;

/// Result type alias for xbbg-ext operations.
pub type Result<T> = std::result::Result<T, ExtError>;

/// Errors that can occur in xbbg-ext operations.
#[derive(Debug, Error)]
pub enum ExtError {
    /// Invalid user input for market/exchange utilities.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Failed to parse a date string.
    #[error("failed to parse date '{0}': expected format YYYY-MM-DD, YYYYMMDD, or YYYY/MM/DD")]
    DateParse(String),

    /// Invalid ticker format.
    #[error("invalid ticker format '{0}': expected 'PREFIX ASSET' (e.g., 'ES1 Index')")]
    InvalidTicker(String),

    /// Ticker appears to be specific rather than generic.
    #[error("'{0}' appears to be a specific contract, not generic. Use generic ticker like 'ES1 Index' instead")]
    SpecificTicker(String),

    /// Unknown yield type.
    #[error(
        "unknown yield type '{0}': expected YTM, YTC, YTR, YTP, YTW, YTWR, EYTW, EYTWR, or YTAL"
    )]
    UnknownYieldType(String),

    /// Arrow error during DataFrame operations.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Missing required column in DataFrame.
    #[error("missing required column '{0}'")]
    MissingColumn(String),

    /// Internal invariant or infrastructure failure.
    #[error("internal error: {0}")]
    Internal(String),
}
