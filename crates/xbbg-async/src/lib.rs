// Allow large error types - BlpError contains rich context for debugging
#![allow(clippy::result_large_err)]

mod cache_io;
pub mod config;
pub mod diagnostics;
pub mod engine;
pub mod error_presentation;
mod errors;
pub mod field_cache;
pub mod request_builder;
pub mod schema;
pub mod sdk_logging;
pub mod services;
pub mod subscription_consumer;

pub use errors::BlpAsyncError;

// Worker-pool Engine — the primary API
pub use engine::{
    DelayedPolicy, Engine, EngineConfig, FeedInfo, FieldErrorPolicy, KeyColumn, OverflowPolicy,
    RequestStream, SessionConnections, SessionPool, SlabKey, SubscribeRequest,
    SubscriptionDeliveries, SubscriptionDelivery, SubscriptionDeliveryMode, SubscriptionGap,
    SubscriptionHandle, SubscriptionStream, ValidationMode,
};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use engine::testing;

// Identity / entitlement types surfaced by Engine::seat_type and
// Engine::check_entitlements.
pub use xbbg_core::{EntitlementCheck, SeatType};

// Runtime loading: `Engine::start` fails with `BlpAsyncError::SdkNotLoaded` until `load` has
// loaded the Bloomberg runtime.
pub use xbbg_core::{SdkLoadError, load, loaded_sdk_path};

// Request building and validation
pub use request_builder::{RequestBuilder, RoutedParams};

// Schema introspection and caching
pub use schema::{ElementInfo, OperationSchema, SchemaCache, ServiceSchema};

#[cfg(test)]
mod tests;

// The unit tests drive BLPAPI directly (SDK-built fixtures), so they need the runtime loaded
// before the first one runs.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn load_sdk_for_tests() {
    let path = xbbg_sdk::locate::locate(&xbbg_sdk::locate::LocateRequest::default())
        .into_first_path()
        .expect("unit tests need a Bloomberg runtime: set BLPAPI_LIB_DIR or BLPAPI_ROOT");
    if let Err(error) = xbbg_core::load(path) {
        panic!("unit tests need a Bloomberg runtime: {error}");
    }
}
