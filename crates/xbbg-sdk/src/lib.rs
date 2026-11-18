//! Locate and provision the Bloomberg BLPAPI runtime shared by xbbg surfaces.
//!
//! This crate must never depend on `xbbg-blpapi-sys` (directly or transitively):
//! hosts load it before the Bloomberg runtime exists on the machine.
//!
//! # Features
//!
//! * **default (none): the resolver.** Platform library names, the SDK directory layouts and the
//!   environment lookup ([`locate`]), the cache root, the
//!   `provenance.json` record and the verification of an installed runtime. It returns an
//!   absolute library path and nothing more: no networking, no download, and only `serde`,
//!   `serde_json`, `sha2` and `hex` as dependencies, so a base package can embed it in a few
//!   hundred kilobytes. Each language binding keeps its own search order and loads the chosen
//!   library in its own process.
//! * **`provision`: the downloader.** Everything below "Provisioning", with the PyPI, conda and
//!   archive stacks (uv, rattler, reqwest, tokio, archive readers) behind it. Enabling it in a
//!   resolver-only binding grows that binary by an order of magnitude; the package size budgets
//!   in CI exist to catch exactly that.
//!
//! # The cache
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
//!
//! # Provisioning (feature `provision`)
//!
//! A `Provisioner` installs the Bloomberg native library from one of three `Source`s:
//!
//! * a PyPI simple index (PEP 503 HTML or PEP 691 JSON, read by uv's registry client), such as
//!   Bloomberg's own `BLOOMBERG_PYPI_INDEX`: the library comes out of the `blpapi` wheel;
//! * a conda channel (rattler's gateway, sharded repodata where offered): the `libblpapi`
//!   package;
//! * a single `.whl`, `.conda` or `.tar.bz2` archive.
//!
//! Every install is verified twice: the archive against the SHA-256 the index or repodata
//! publishes (or the one the caller supplies), and the library against the wheel `RECORD` or
//! the conda `info/paths.json`. Installs are staged under `<cache>/tmp`, committed by an atomic
//! rename under a per-install file lock, and described by a `provenance.json` written last (see
//! `installed` for the layout readers rely on). Network settings are explicit
//! (`NetworkOptions`): no environment variable is changed or, apart from `ProxyConfig::System`,
//! read.

mod cache;
pub mod locate;
mod platform;
mod provenance;

#[cfg(feature = "provision")]
mod conda;
#[cfg(feature = "provision")]
mod error;
#[cfg(feature = "provision")]
mod http;
#[cfg(feature = "provision")]
mod install;
#[cfg(feature = "provision")]
mod options;
#[cfg(feature = "provision")]
mod provisioner;
#[cfg(feature = "provision")]
mod pypi;
#[cfg(feature = "provision")]
mod source;
#[cfg(feature = "provision")]
mod wheel;

#[cfg(feature = "provision")]
pub mod blocking;

pub use cache::{
    CacheScan, CachedRuntime, INSTALLS_DIR, Rejected, compare_versions, default_cache_root,
    resolve_cached, scan_cache, scan_cache_all, verify_install,
};
pub use platform::{ParsePlatformError, Platform};
pub use provenance::{PROVENANCE_FILE, Provenance, safe_relative};

#[cfg(feature = "provision")]
pub use error::{Error, Result};
#[cfg(feature = "provision")]
pub use install::{Provisioned, installed};
#[cfg(feature = "provision")]
pub use options::{
    Credential, NetworkOptions, ProvisionOptions, ProxyConfig, TlsConfig, VersionSpec,
};
#[cfg(feature = "provision")]
pub use provisioner::{Provisioner, Resolved};
#[cfg(feature = "provision")]
pub use source::{
    BLOOMBERG_PYPI_INDEX, DEFAULT_CONDA_PACKAGE, DEFAULT_PYPI_PROJECT, MIN_PYPI_VERSION, Source,
    SourceKind,
};
