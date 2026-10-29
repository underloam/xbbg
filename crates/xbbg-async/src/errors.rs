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

impl BlpAsyncError {
    /// SDK support identifiers carried by a failed data request.
    pub fn bloomberg_request_ids(&self) -> &[std::sync::Arc<str>] {
        match self {
            Self::Blp(error) => error.bloomberg_request_ids(),
            _ => &[],
        }
    }

    /// Merge support IDs only into sent-request failures and timeouts.
    ///
    /// Returns false for lifecycle and non-request errors, leaving their kind
    /// unchanged so callers can retain contributor context separately.
    pub fn try_extend_bloomberg_request_ids(
        &mut self,
        ids: impl IntoIterator<Item = std::sync::Arc<str>>,
    ) -> bool {
        let Self::Blp(error) = self else {
            return false;
        };
        if !matches!(
            error,
            BlpError::RequestFailure { .. } | BlpError::RequestTimeout { .. }
        ) {
            return false;
        }
        error.extend_bloomberg_request_ids(ids);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn sent_request_context_merges_without_changing_error_kind() {
        for core in [
            BlpError::RequestFailure {
                service: "//synthetic/refdata".into(),
                operation: Some("SyntheticRequest".into()),
                cid: None,
                label: None,
                request_id: Some("synthetic-label".into()),
                bloomberg_request_ids: vec![Arc::from("synthetic-request")],
                source: None,
            },
            BlpError::RequestTimeout {
                request_id: Some("synthetic-label".into()),
                bloomberg_request_ids: vec![Arc::from("synthetic-request")],
            },
        ] {
            let mut error = BlpAsyncError::from(core);
            let message = error.to_string();
            assert!(error.try_extend_bloomberg_request_ids([
                Arc::from("synthetic-lookup"),
                Arc::from("synthetic-request"),
            ]));
            assert_eq!(error.to_string(), message);
            assert_eq!(
                error.bloomberg_request_ids(),
                [
                    Arc::<str>::from("synthetic-lookup"),
                    Arc::from("synthetic-request")
                ]
            );
        }
    }

    #[test]
    fn lifecycle_timeout_declines_sent_request_enrichment() {
        let mut error = BlpAsyncError::from(BlpError::Timeout);
        assert!(!error.try_extend_bloomberg_request_ids([Arc::from("synthetic-prior")]));
        assert!(matches!(error, BlpAsyncError::Blp(BlpError::Timeout)));
        assert!(error.bloomberg_request_ids().is_empty());
    }
}
