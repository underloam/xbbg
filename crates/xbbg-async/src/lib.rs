// Allow large error types - BlpError contains rich context for debugging
#![allow(clippy::result_large_err)]

mod cache_io;
pub mod engine;
mod errors;
pub mod field_cache;
pub mod request_builder;
pub mod schema;
pub mod sdk_logging;
pub mod services;

pub use errors::BlpAsyncError;

// Worker-pool Engine — the primary API
pub use engine::{
    DelayedPolicy, Engine, EngineConfig, FeedInfo, FieldErrorPolicy, OverflowPolicy, RequestStream,
    SlabKey, SubscribeRequest, SubscriptionHandle, SubscriptionStream, ValidationMode,
};

// Identity / entitlement types surfaced by Engine::seat_type and
// Engine::check_entitlements.
pub use xbbg_core::{EntitlementCheck, SeatType};

// Request building and validation
pub use request_builder::{RequestBuilder, RoutedParams};

// Schema introspection and caching
pub use schema::{ElementInfo, OperationSchema, SchemaCache, ServiceSchema};

#[cfg(test)]
mod tests;
