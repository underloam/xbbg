//! xbbg-core: Zero-allocation Bloomberg API wrapper
//!
//! High-performance Rust bindings for the Bloomberg C++ SDK.
//!
//! - Zero-allocation hot paths
//! - Direct typed access (no JSON serialization)
//! - Sub-microsecond field extraction

#![allow(clippy::result_large_err)]

// src/ffi.rs re-exports the BLPAPI symbols unconditionally, so a build without
// `live` has no `blpapi_sys` dependency to resolve them against. This guard
// replaces the one that used to live in the removed `xbbg-sys` shim and turns
// that configuration into one clear error instead of dozens of import failures.
#[cfg(not(feature = "live"))]
compile_error!("The 'live' feature must be enabled");

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
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
    apply_session_identity_options, AuthApplication, AuthConfig, AuthOptions, AuthToken, AuthUser,
};
pub use correlation::CorrelationId;
pub use datatype::DataType;
pub use datetime::HighPrecisionDatetime;
pub use element::{ChildrenIter, Element, ValuesIter};
pub use errors::{BlpError, Result};
pub use event::{Event, EventType};
pub use identity::{EntitlementCheck, Identity, SeatType};
pub use message::Message;
pub use name::{clear_name_cache, name_cache_size, Name};
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

pub fn sdk_version() -> (i32, i32, i32, i32) {
    let (mut major, mut minor, mut patch, mut build) = (0, 0, 0, 0);
    unsafe { ffi::blpapi_getVersionInfo(&mut major, &mut minor, &mut patch, &mut build) };
    (major, minor, patch, build)
}
