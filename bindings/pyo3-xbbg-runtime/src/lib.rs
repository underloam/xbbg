//! `xbbg._runtime`: the Python face of the shared Bloomberg runtime resolver.
//!
//! `xbbg._core` links the Bloomberg runtime when the interpreter loads it, so nothing inside it
//! can look for that runtime first. This module is the small native step before it: it reads the
//! xbbg install cache through `xbbg-sdk`'s resolver (hash-verified, no networking) and never links
//! Bloomberg code, so it always imports. `xbbg._sdk` preloads the library it names.

use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use xbbg_sdk::{CacheScan, Platform};

/// The xbbg cache root (`XBBG_CACHE_DIR`, else the platform default), or `None` without a home
/// directory.
#[pyfunction]
fn default_cache_root() -> Option<OsString> {
    xbbg_sdk::default_cache_root().map(PathBuf::into_os_string)
}

/// Verified runtimes and rejected installs under `cache_root` for `platform`.
///
/// `cache_root` defaults to the xbbg cache root and `platform` to the host platform. Returns
/// `{"runtimes": [...], "rejected": [...]}`: `runtimes` lists the verified installs, best first,
/// each with `version`, `platform`, `lib_path`, `lib_sha256`, `source` and `install_dir`;
/// `rejected` lists `install_dir` and `reason` for every install that failed verification.
/// Raises `ValueError` for an unknown platform and `OSError` when the cache cannot be listed.
#[pyfunction]
#[pyo3(signature = (cache_root=None, platform=None))]
fn scan_cache<'py>(
    py: Python<'py>,
    cache_root: Option<PathBuf>,
    platform: Option<&str>,
) -> PyResult<Bound<'py, PyDict>> {
    let platform = platform
        .map(|name| {
            Platform::from_str(name).map_err(|error| PyValueError::new_err(error.to_string()))
        })
        .transpose()?;
    // Hashing a library is disk work: keep the interpreter free for other threads.
    let scan = py.detach(move || read_cache(cache_root, platform))?;
    render(py, scan)
}

/// Reads the cache; an unknown root or platform (no home directory, unsupported host) holds
/// nothing.
fn read_cache(
    cache_root: Option<PathBuf>,
    platform: Option<Platform>,
) -> std::io::Result<CacheScan> {
    let root = cache_root.or_else(xbbg_sdk::default_cache_root);
    let platform = platform.or_else(Platform::host);
    match (root, platform) {
        (Some(root), Some(platform)) => xbbg_sdk::scan_cache(&root, platform),
        _ => Ok(CacheScan::default()),
    }
}

fn render(py: Python<'_>, scan: CacheScan) -> PyResult<Bound<'_, PyDict>> {
    let runtimes = PyList::empty(py);
    for runtime in scan.runtimes {
        let entry = PyDict::new(py);
        entry.set_item("version", runtime.version)?;
        entry.set_item("platform", runtime.platform.as_str())?;
        entry.set_item("lib_path", runtime.lib_path.into_os_string())?;
        entry.set_item("lib_sha256", runtime.lib_sha256)?;
        entry.set_item("source", runtime.source)?;
        entry.set_item("install_dir", runtime.install_dir.into_os_string())?;
        runtimes.append(entry)?;
    }
    let rejected = PyList::empty(py);
    for refused in scan.rejected {
        let entry = PyDict::new(py);
        entry.set_item("install_dir", refused.install_dir.into_os_string())?;
        entry.set_item("reason", refused.reason)?;
        rejected.append(entry)?;
    }
    let result = PyDict::new(py);
    result.set_item("runtimes", runtimes)?;
    result.set_item("rejected", rejected)?;
    Ok(result)
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_runtime")]
fn runtime_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(default_cache_root, m)?)?;
    m.add_function(wrap_pyfunction!(scan_cache, m)?)?;
    Ok(())
}
