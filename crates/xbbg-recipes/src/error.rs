//! Recipe error types.

use std::sync::Arc;

use thiserror::Error;
use xbbg_async::engine::state::BloombergRequestIds;

/// Errors that can occur during recipe execution.
#[derive(Debug, Error)]
pub enum RecipeError {
    /// Error from the Bloomberg async engine.
    #[error("Bloomberg engine error: {0}")]
    Engine(#[source] Box<xbbg_async::BlpAsyncError>),

    /// Error from xbbg-ext utilities.
    #[error("Extension utility error: {0}")]
    Ext(#[from] xbbg_ext::ExtError),

    /// Arrow data error.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Invalid argument provided to a recipe.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// General recipe error.
    #[error("Recipe error: {0}")]
    Other(String),

    /// SDK support context for a recipe failure whose original kind is unchanged.
    #[error("{source}")]
    RequestContext {
        #[source]
        source: Box<RecipeError>,
        bloomberg_request_ids: Vec<Arc<str>>,
    },
}

impl RecipeError {
    /// Add contributing SDK support identifiers without changing the failure kind.
    ///
    /// Sent engine request failures are enriched in place. Other failures retain
    /// their original source in one context wrapper, including lifecycle timeouts.
    pub fn with_bloomberg_request_ids(self, ids: impl IntoIterator<Item = Arc<str>>) -> Self {
        let (mut source, mut request_ids) = self.into_request_context();
        request_ids.extend(ids);
        if request_ids.is_empty() {
            return source;
        }
        if let Self::Engine(error) = &mut source
            && error.try_extend_bloomberg_request_ids(request_ids.iter().cloned())
        {
            return source;
        }
        Self::RequestContext {
            source: Box::new(source),
            bloomberg_request_ids: request_ids.iter().cloned().collect(),
        }
    }

    pub(crate) fn with_bloomberg_request_ids_from_batch(
        self,
        batch: &arrow_array::RecordBatch,
    ) -> Self {
        let mut request_ids = BloombergRequestIds::default();
        request_ids.collect(batch);
        self.with_bloomberg_request_ids(request_ids.iter().cloned())
    }

    /// Borrow the support identifiers retained by this recipe failure.
    pub fn bloomberg_request_ids(&self) -> &[Arc<str>] {
        match self {
            Self::Engine(error) => error.bloomberg_request_ids(),
            Self::RequestContext {
                bloomberg_request_ids,
                ..
            } => bloomberg_request_ids,
            _ => &[],
        }
    }

    /// Peel recipe context for host classification and merge all contributor IDs.
    pub fn into_request_context(self) -> (Self, BloombergRequestIds) {
        let mut source = self;
        let mut request_ids = BloombergRequestIds::default();
        while let Self::RequestContext {
            source: inner,
            bloomberg_request_ids,
        } = source
        {
            request_ids.extend(bloomberg_request_ids);
            source = *inner;
        }
        request_ids.extend(source.bloomberg_request_ids().iter().cloned());
        (source, request_ids)
    }
}

impl From<xbbg_async::BlpAsyncError> for RecipeError {
    fn from(value: xbbg_async::BlpAsyncError) -> Self {
        Self::Engine(Box::new(value))
    }
}

/// Result type alias for recipe operations.
pub type Result<T> = std::result::Result<T, RecipeError>;

#[cfg(test)]
mod tests {
    use super::*;
    use xbbg_async::BlpAsyncError;

    #[test]
    fn recipe_context_merges_without_nesting_or_changing_display() {
        let source = RecipeError::Other("synthetic validation rejection".into());
        let message = source.to_string();
        let error = source
            .with_bloomberg_request_ids([Arc::from("synthetic_z"), Arc::from("synthetic_a")])
            .with_bloomberg_request_ids([Arc::from("synthetic_a"), Arc::from("synthetic_b")]);
        assert_eq!(error.to_string(), message);
        assert_eq!(
            error
                .bloomberg_request_ids()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["synthetic_a", "synthetic_b", "synthetic_z"]
        );
        let RecipeError::RequestContext { source, .. } = error else {
            panic!("generic recipe failures need context");
        };
        assert!(matches!(*source, RecipeError::Other(_)));
    }

    #[test]
    fn non_request_engine_failure_keeps_its_original_kind() {
        let source = RecipeError::from(BlpAsyncError::ChannelClosed);
        let message = source.to_string();
        let error = source.with_bloomberg_request_ids([Arc::from("synthetic_completed_request")]);
        assert_eq!(error.to_string(), message);
        let (source, request_ids) = error.into_request_context();
        let RecipeError::Engine(engine) = source else {
            panic!("the engine classification must survive");
        };
        assert!(matches!(*engine, BlpAsyncError::ChannelClosed));
        assert_eq!(
            request_ids.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            ["synthetic_completed_request"]
        );
    }

    #[test]
    fn empty_context_keeps_original_variant() {
        let error = RecipeError::InvalidArgument("synthetic argument".into())
            .with_bloomberg_request_ids([]);
        assert!(matches!(error, RecipeError::InvalidArgument(_)));
    }
}
