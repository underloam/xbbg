//! Resolver addon for the xbbg Node package.
//!
//! This addon never links Bloomberg's runtime (`deny.toml` keeps `xbbg-blpapi-sys` out of its
//! dependency tree), so Node can load it before the engine addon and use it to locate and preload
//! the verified runtime from the xbbg cache.

mod preload;

use std::path::{Path, PathBuf};
use std::str::FromStr;

use napi::{Error, Result, Status};
use napi_derive::napi;
use xbbg_runtime::Platform;

/// A verified runtime install in the xbbg cache.
#[napi(object)]
pub struct CachedRuntime {
    pub version: String,
    pub platform: String,
    pub lib_path: String,
    pub lib_sha256: String,
    pub source: String,
    pub install_dir: String,
}

/// A cache install directory that cannot be used, and why.
#[napi(object)]
pub struct RejectedRuntime {
    pub install_dir: String,
    pub reason: String,
}

/// What the cache holds: verified runtimes (best first) and refused install directories.
#[napi(object)]
pub struct CacheScan {
    pub runtimes: Vec<CachedRuntime>,
    pub rejected: Vec<RejectedRuntime>,
}

fn text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The xbbg cache root (`XBBG_CACHE_DIR`, else the platform default), or `null` when no home
/// directory is known.
#[napi]
pub fn default_cache_root() -> Option<String> {
    xbbg_runtime::default_cache_root().map(|root| text(&root))
}

/// Scan `cacheRoot` (default: the xbbg cache root) for `platform` (default: this host).
///
/// Every install is verified against its `provenance.json`; installs that fail are listed in
/// `rejected` with the reason. An unknown host platform or cache root yields an empty scan.
#[napi]
pub fn scan_cache(cache_root: Option<String>, platform: Option<String>) -> Result<CacheScan> {
    let platform = match platform {
        Some(name) => Some(Platform::from_str(&name).map_err(|error| {
            Error::new(Status::InvalidArg, format!("invalid platform: {error}"))
        })?),
        None => Platform::host(),
    };
    let root = match cache_root {
        Some(root) => Some(PathBuf::from(root)),
        None => xbbg_runtime::default_cache_root(),
    };
    let (Some(root), Some(platform)) = (root, platform) else {
        return Ok(CacheScan {
            runtimes: Vec::new(),
            rejected: Vec::new(),
        });
    };
    let scan = xbbg_runtime::scan_cache(&root, platform).map_err(|error| {
        Error::new(
            Status::GenericFailure,
            format!("cannot scan the xbbg cache {}: {error}", root.display()),
        )
    })?;
    Ok(CacheScan {
        runtimes: scan
            .runtimes
            .into_iter()
            .map(|runtime| CachedRuntime {
                version: runtime.version,
                platform: runtime.platform.as_str().to_owned(),
                lib_path: text(&runtime.lib_path),
                lib_sha256: runtime.lib_sha256,
                source: runtime.source,
                install_dir: text(&runtime.install_dir),
            })
            .collect(),
        rejected: scan
            .rejected
            .into_iter()
            .map(|rejected| RejectedRuntime {
                install_dir: text(&rejected.install_dir),
                reason: rejected.reason,
            })
            .collect(),
    })
}

/// Load the library at the absolute `path` (`RTLD_NOW | RTLD_GLOBAL` on Unix, `LoadLibraryExW`
/// on Windows) and keep it loaded for the process lifetime, so the engine addon's link to it is
/// already satisfied. Throws with the operating system's error text on failure.
#[napi]
pub fn preload_library(path: String) -> Result<()> {
    preload::preload(Path::new(&path))
        .map_err(|message| Error::new(Status::GenericFailure, message))
}
