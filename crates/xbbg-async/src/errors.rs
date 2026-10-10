use thiserror::Error;

use xbbg_core::BlpError;

#[derive(Debug, Error)]
pub enum BlpAsyncError {
    /// Wraps a core BlpError, preserving all structured error context.
    #[error(transparent)]
    Blp(#[from] BlpError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("configuration error: {detail}")]
    ConfigError { detail: String },

    #[error("channel closed")]
    ChannelClosed,

    /// All request workers in the pool are dead.
    ///
    /// No healthy worker is available to accept requests. The pool needs
    /// worker replacement (Phase 4) or manual intervention.
    #[error("all {pool_size} request workers are dead — no healthy worker available")]
    AllWorkersDown { pool_size: usize },
}
