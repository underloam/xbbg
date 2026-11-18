//! Bloomberg BLPAPI runtime provisioning shared by xbbg surfaces.
//!
//! This crate must never depend on `xbbg-blpapi-sys` (directly or transitively):
//! hosts load it before the Bloomberg runtime exists on the machine.
//!
//! A [`Provisioner`] installs the Bloomberg native library from one of three [`Source`]s:
//!
//! * a PyPI simple index (PEP 503 HTML or PEP 691 JSON, read by uv's registry client), such as
//!   Bloomberg's own [`BLOOMBERG_PYPI_INDEX`]: the library comes out of the `blpapi` wheel;
//! * a conda channel (rattler's gateway, sharded repodata where offered): the `libblpapi`
//!   package;
//! * a single `.whl`, `.conda` or `.tar.bz2` archive.
//!
//! Every install is verified twice: the archive against the SHA-256 the index or repodata
//! publishes (or the one the caller supplies), and the library against the wheel `RECORD` or
//! the conda `info/paths.json`. Installs are staged under `<cache>/tmp`, committed by an atomic
//! rename under a per-install file lock, and described by a `provenance.json` written last (see
//! [`installed`] for the layout readers rely on). Network settings are explicit
//! ([`NetworkOptions`]): no environment variable is changed or, apart from
//! [`ProxyConfig::System`], read.

mod conda;
mod error;
mod http;
mod install;
mod options;
mod platform;
mod provisioner;
mod pypi;
mod source;
mod wheel;

pub mod blocking;

pub use error::{Error, Result};
pub use install::{Provisioned, installed};
pub use options::{Credential, NetworkOptions, ProvisionOptions, ProxyConfig, TlsConfig};
pub use platform::VersionSpec;
pub use provisioner::{Provisioner, Resolved};
pub use source::{
    BLOOMBERG_PYPI_INDEX, DEFAULT_CONDA_PACKAGE, DEFAULT_PYPI_PROJECT, MIN_PYPI_VERSION, Source,
    SourceKind,
};
pub use xbbg_runtime::{Platform, default_cache_root};
