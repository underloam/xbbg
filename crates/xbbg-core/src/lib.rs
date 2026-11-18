//! xbbg-core: Zero-allocation Bloomberg API wrapper
//!
//! High-performance Rust bindings for the Bloomberg C++ SDK.
//!
//! - Zero-allocation hot paths
//! - Direct typed access (no JSON serialization)
//! - Sub-microsecond field extraction
//!
//! # Loading the Bloomberg runtime
//!
//! Nothing links the Bloomberg runtime. Call [`load`] with the absolute path of the BLPAPI library
//! before any other call into this crate; until then they panic with a message saying so. Choosing
//! the path (the search order) is the caller's job.

#![allow(clippy::result_large_err)]

// The BLPAPI function table in src/loader.rs comes from `blpapi_sys`, so a build
// without `live` has no `blpapi_sys` dependency to resolve it against. This guard
// replaces the one that used to live in the removed `xbbg-sys` shim and turns
// that configuration into one clear error instead of dozens of import failures.
#[cfg(not(feature = "live"))]
compile_error!("The 'live' feature must be enabled");

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// Runtime loading
mod loader;
pub use loader::{SdkLoadError, load, loaded_sdk_path, sdk};

// The unit tests call BLPAPI directly, so they need the runtime before the first one runs.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn load_sdk_for_tests() {
    let path = xbbg_sdk::Platform::host()
        .and_then(xbbg_sdk::locate::from_env)
        .expect("unit tests need a Bloomberg runtime: set BLPAPI_LIB_DIR or BLPAPI_ROOT");
    if let Err(error) = load(path) {
        panic!("unit tests need a Bloomberg runtime: {error}");
    }
}

// Core types
pub mod auth;
pub mod datatype;
pub mod datetime;
pub mod element;
pub mod event;
pub mod ffi;
pub mod message;
pub mod name;
pub mod value;

// Session API
pub mod async_session;
pub mod correlation;
pub mod errors;
pub mod identity;
pub mod options;
pub mod request;
pub mod schema;
pub mod service;
pub mod session;
mod session_operations;
pub mod socks5;
pub mod subscription;
pub mod tls;
pub mod zfp;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support;

// Re-exports for convenience
pub use async_session::AsyncSession;
pub use auth::{
    AuthApplication, AuthConfig, AuthOptions, AuthToken, AuthUser, apply_session_identity_options,
};
pub use correlation::CorrelationId;
pub use datatype::DataType;
pub use datetime::HighPrecisionDatetime;
pub use element::{ChildrenIter, Element, ValuesIter};
pub use errors::{BlpError, Result};
pub use event::{Event, EventType};
pub use identity::{EntitlementCheck, Identity, SeatType};
pub use message::Message;
pub use name::{Name, clear_name_cache, name_cache_size};
pub use request::Request;
pub use service::Service;
pub use session::{Session, SessionOptions};
pub use socks5::Socks5Config;
pub use subscription::SubscriptionList;
pub use value::{OwnedValue, Value};

// Schema introspection types
pub use schema::{
    Constant, ConstantList, Operation, SchemaElementDefinition, SchemaStatus, SchemaTypeDefinition,
};

/// The loaded runtime's version, `(major, minor, patch, build)`.
///
/// # Panics
///
/// If no runtime is loaded (see [`load`]).
pub fn sdk_version() -> (i32, i32, i32, i32) {
    let (mut major, mut minor, mut patch, mut build) = (0, 0, 0, 0);
    // SAFETY: the four out-parameters are valid for writes for the duration of the call.
    unsafe { sdk().blpapi_getVersionInfo(&mut major, &mut minor, &mut patch, &mut build) };
    (major, minor, patch, build)
}
