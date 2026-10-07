//! Error types for xbbg Arrow carrier helpers.

use arrow_schema::ArrowError;
use thiserror::Error;

/// Result type alias for pure Arrow carrier operations.
pub type Result<T> = std::result::Result<T, ArrowCoreError>;

/// Errors produced by xbbg's pure Arrow carrier helpers.
#[derive(Debug, Error)]
pub enum ArrowCoreError {
    /// Error returned by arrow-rs while constructing or transforming data.
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// Record batches in one logical table must share the exact same schema.
    #[error("all batches must have identical schemas")]
    IncompatibleSchemas,

    /// Concatenated tables must share the exact same schema.
    #[error("all tables must have identical schemas")]
    IncompatibleTableSchemas,

    /// A named column was requested but does not exist.
    #[error("unknown column: {0}")]
    UnknownColumn(String),

    /// A column index was outside the schema bounds.
    #[error("column index out of range")]
    ColumnIndexOutOfRange,

    /// A row index was outside the table/column bounds.
    #[error("row index out of range")]
    RowIndexOutOfRange,

    /// A supplied column has a different length than the table row count.
    #[error("column length {actual} does not match table row count {expected}")]
    ColumnLengthMismatch { actual: usize, expected: usize },
}
