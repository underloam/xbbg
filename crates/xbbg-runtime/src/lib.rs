//! Select and verify a provisioned Bloomberg BLPAPI runtime in the xbbg cache.
//!
//! This crate knows the platform library names, the cache layout and how to check an install
//! against its `provenance.json`. It returns an absolute library path and nothing more: no
//! networking, no download, and no link to Bloomberg (it must never depend on
//! `xbbg-blpapi-sys`, directly or transitively), so a host can run it before an engine library
//! loads. Provisioning lives in `xbbg-sdk`; each language binding keeps its own search order and
//! loads the chosen library in its own process.
//!
//! ```text
//! <cache>/
//!   sdk/<version>/<platform>/<pypi|conda|artifact>-<archive sha256[..12]>/
//!       lib/<blpapi3_64.dll | libblpapi3_64.so>
//!       licenses/...
//!       provenance.json   written last; directories without it are incomplete
//! ```
//!
//! An install is usable when `provenance.json` parses, its library path is relative and stays
//! inside the install directory, the file is a regular file whose SHA-256 matches the recorded
//! hash, and the platform matches. [`scan_cache`] lists the verified installs best first and the
//! rest with the reason each was refused; [`resolve_cached`] picks the best one.

mod cache;
mod platform;
mod provenance;

pub use cache::{
    CacheScan, CachedRuntime, INSTALLS_DIR, Rejected, compare_versions, default_cache_root,
    resolve_cached, scan_cache, scan_cache_all, verify_install,
};
pub use platform::{ParsePlatformError, Platform};
pub use provenance::{PROVENANCE_FILE, Provenance, safe_relative};
