//! Host-neutral glue shared by xbbg language bindings.
//!
//! A binding (JNI first) calls these modules instead of re-implementing request
//! building, subscription assembly, error presentation, metadata decoding and
//! teardown. Nothing here depends on a host framework.

pub mod config;
pub mod errors;
pub mod metadata;
pub mod preflight;
pub mod recipes;
pub mod request;
pub mod schema;
pub mod subscribe;
pub mod subscription;
pub mod teardown;
