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

use std::path::PathBuf;

pub use error::{Error, Result};
pub use install::{Provisioned, installed};
pub use options::{Credential, NetworkOptions, ProvisionOptions, ProxyConfig, TlsConfig};
pub use platform::{Platform, VersionSpec};
pub use provisioner::{Provisioner, Resolved};
pub use source::{
    BLOOMBERG_PYPI_INDEX, DEFAULT_CONDA_PACKAGE, DEFAULT_PYPI_PROJECT, MIN_PYPI_VERSION, Source,
    SourceKind,
};

/// The shared xbbg cache root: `XBBG_CACHE_DIR` if set, else `%LOCALAPPDATA%\xbbg` (falling
/// back to `%USERPROFILE%\AppData\Local\xbbg`) on Windows, `~/Library/Caches/xbbg` on macOS,
/// and `$XDG_CACHE_HOME/xbbg` or `~/.cache/xbbg` elsewhere. `None` when no home directory is
/// known. JVM hosts consult the `xbbg.cache.dir` system property before calling this.
pub fn default_cache_root() -> Option<PathBuf> {
    cache_root_from(|name| {
        std::env::var_os(name)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    })
}

fn cache_root_from(var: impl Fn(&str) -> Option<PathBuf>) -> Option<PathBuf> {
    if let Some(dir) = var("XBBG_CACHE_DIR") {
        return Some(dir);
    }
    if cfg!(windows) {
        var("LOCALAPPDATA")
            .or_else(|| var("USERPROFILE").map(|home| home.join("AppData").join("Local")))
            .map(|base| base.join("xbbg"))
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| home.join("Library").join("Caches").join("xbbg"))
    } else {
        var("XDG_CACHE_HOME")
            .filter(|dir| dir.is_absolute())
            .or_else(|| var("HOME").map(|home| home.join(".cache")))
            .map(|base| base.join("xbbg"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;

    fn root(vars: &[(&str, &str)]) -> Option<PathBuf> {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        cache_root_from(|name| vars.get(name).map(PathBuf::from))
    }

    #[test]
    fn explicit_cache_dir_wins() {
        let dir = Path::new("custom").join("cache");
        let found = root(&[
            ("XBBG_CACHE_DIR", dir.to_str().unwrap()),
            ("LOCALAPPDATA", "x"),
            ("HOME", "y"),
        ]);
        assert_eq!(found, Some(dir));
    }

    #[test]
    #[cfg(windows)]
    fn windows_prefers_local_app_data_then_user_profile() {
        assert_eq!(
            root(&[("LOCALAPPDATA", r"C:\Local"), ("USERPROFILE", r"C:\Home")]),
            Some(PathBuf::from(r"C:\Local\xbbg"))
        );
        assert_eq!(
            root(&[("USERPROFILE", r"C:\Home")]),
            Some(PathBuf::from(r"C:\Home\AppData\Local\xbbg"))
        );
        assert_eq!(root(&[]), None);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn linux_uses_xdg_cache_home_when_absolute() {
        assert_eq!(
            root(&[("XDG_CACHE_HOME", "/xdg"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/xdg/xbbg"))
        );
        assert_eq!(
            root(&[("XDG_CACHE_HOME", "relative"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/home/u/.cache/xbbg"))
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_uses_library_caches() {
        assert_eq!(
            root(&[("HOME", "/Users/u")]),
            Some(PathBuf::from("/Users/u/Library/Caches/xbbg"))
        );
    }
}
