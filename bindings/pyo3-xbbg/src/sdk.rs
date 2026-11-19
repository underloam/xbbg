//! Loading and provisioning the Bloomberg runtime from Python.
//!
//! `xbbg._core` links no Bloomberg code, so it always imports. The Python package calls
//! [`load_sdk`] right after the import with the two things only Python knows (the directory
//! `xbbg.set_sdk_path` recorded and the directory of Bloomberg's `blpapi` wheel); the search
//! order, the verification, optional `XBBG_SDK_SOURCE` cache provisioning and the load itself
//! live in Rust (`xbbg-sdk` and `xbbg-host`).
//!
//! Requests and results cross the boundary as JSON-shaped dicts/lists with the camelCase keys of
//! `LocateRequest`, `LoadedSdk`, `Located`, `ProvisionRequest` and `Provisioned`; paths are strings.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use pyo3::exceptions::{PyImportError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use serde::Serialize;
use serde_json::json;
use xbbg_host::errors::{HostError, HostErrorKind};
use xbbg_host::sdk::provision::{
    ProvisionFallback, bloomberg_pypi_index, cli_run, default_sdk_cache_root, installed_json,
    locate_and_load_or_provision, provision_json_blocking,
};
use xbbg_host::sdk::{LoadedSdk, LocateRequest, locate};

/// The runtime this module loaded; `xbbg_core` knows only the path.
static LOADED: OnceLock<LoadedSdk> = OnceLock::new();

fn request_from_py(py: Python<'_>, request: Option<&Bound<'_, PyDict>>) -> PyResult<LocateRequest> {
    let Some(request) = request else {
        return Ok(LocateRequest::default());
    };
    // Paths and other `os.PathLike` values become strings, as `str()` renders them.
    let options = PyDict::new(py);
    options.set_item("default", py.get_type::<PyString>())?;
    let json = py
        .import("json")?
        .call_method("dumps", (request,), Some(&options))?
        .extract::<String>()?;
    serde_json::from_str(&json).map_err(|error| {
        PyValueError::new_err(format!("invalid Bloomberg runtime request: {error}"))
    })
}

/// `value` as the dict `json.loads` builds from its JSON.
fn to_py_dict<'py>(py: Python<'py>, value: &impl Serialize) -> PyResult<Bound<'py, PyDict>> {
    let text = serde_json::to_string(value)
        .map_err(|error| PyRuntimeError::new_err(format!("cannot encode the result: {error}")))?;
    parse_py_dict(py, &text)
}

fn parse_py_dict<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyDict>> {
    py.import("json")?
        .call_method1("loads", (text,))?
        .cast_into::<PyDict>()
        .map_err(PyErr::from)
}

fn parse_py_list<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyList>> {
    py.import("json")?
        .call_method1("loads", (text,))?
        .cast_into::<PyList>()
        .map_err(PyErr::from)
}

fn sdk_host_error_to_pyerr(error: HostError) -> PyErr {
    let message = match error.context.code.as_deref() {
        Some(code) => format!("[{code}] {}", error.message),
        None => error.message,
    };
    match error.kind {
        HostErrorKind::Argument | HostErrorKind::Validation => PyValueError::new_err(message),
        _ => PyRuntimeError::new_err(message),
    }
}

/// Load the Bloomberg runtime into this process and describe it.
///
/// `request` holds the optional keys of `LocateRequest` (`explicit`, `hints`, `env`,
/// `cacheRoot`, `repoRoot`, `platform`); `None` searches everything with the process
/// environment. Candidates are tried in the single search order; one that fails to load
/// (including a runtime too old for xbbg) is reported and the next is tried. When no
/// explicit path is set and no local or cached runtime loads, `sdk_source` (or
/// `XBBG_SDK_SOURCE`) provisions one into the shared xbbg cache and loads it. A runtime
/// that is already loaded is returned unchanged.
///
/// Returns `{"path", "source", "detail", "version", "rejected"}`. Raises `ImportError`
/// listing every rejected candidate when no runtime loads.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (request=None, sdk_source=None))]
pub(crate) fn load_sdk<'py>(
    py: Python<'py>,
    request: Option<&Bound<'py, PyDict>>,
    sdk_source: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = request_from_py(py, request)?;
    let fallback = ProvisionFallback {
        source: sdk_source,
        version: None,
    };
    // Hashing a cached library, provisioning, and opening the runtime are disk/network work.
    let loaded = py
        .detach(move || locate_and_load_or_provision(&request, &fallback))
        .map_err(|error| PyImportError::new_err(error.to_string()))?;
    to_py_dict(py, LOADED.get_or_init(|| loaded))
}

/// Search for the Bloomberg runtime without loading it.
///
/// Takes the same `request` as `load_sdk`. Returns `{"candidates", "rejected"}`: the library
/// files the search found, in order (`path`, `source`, `detail`, `version`), and the places
/// refused with the reason (`path`, `source`, `detail`, `reason`).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (request=None))]
pub(crate) fn locate_sdk<'py>(
    py: Python<'py>,
    request: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = request_from_py(py, request)?;
    // Verifying cache installs hashes their libraries.
    let located = py.detach(move || locate(&request));
    to_py_dict(py, &located)
}

/// The Bloomberg runtime loaded into this process, or `None` before `load_sdk` succeeded.
///
/// Returns `{"path", "source", "detail", "version", "rejected"}`. A runtime loaded without
/// `load_sdk` (a test harness) reports its `path` and `version` with `source` `None`.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
pub(crate) fn loaded_sdk(py: Python<'_>) -> PyResult<Option<Bound<'_, PyDict>>> {
    if let Some(sdk) = LOADED.get() {
        return to_py_dict(py, sdk).map(Some);
    }
    let Some(path) = xbbg_core::loaded_sdk_path() else {
        return Ok(None);
    };
    let (major, minor, patch, build) = xbbg_core::sdk_version();
    to_py_dict(
        py,
        &json!({
            "path": path,
            "source": null,
            "detail": "",
            "version": format!("{major}.{minor}.{patch}.{build}"),
            "rejected": [],
        }),
    )
    .map(Some)
}

/// The loaded runtime's version, `(major, minor, patch, build)`.
///
/// Raises `RuntimeError` when no runtime is loaded.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
pub(crate) fn sdk_version() -> PyResult<(i32, i32, i32, i32)> {
    if xbbg_core::loaded_sdk_path().is_none() {
        return Err(PyRuntimeError::new_err(
            "the Bloomberg runtime is not loaded; xbbg loads it when xbbg._core is first imported",
        ));
    }
    Ok(xbbg_core::sdk_version())
}

/// Provision a Bloomberg C++ SDK runtime into the shared xbbg cache without requiring
/// any Bloomberg runtime to be loaded first.
///
/// `options_json` is a camelCase JSON object matching `xbbg_sdk::request::ProvisionRequest`.
/// Returns a dict with `version`, `platform`, `libraryPath`, `licensePaths`, `source`,
/// `artifactUrl`, `artifactSha256`, `libSha256`, and `cacheHit`.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
pub(crate) fn provision_sdk(py: Python<'_>, options_json: String) -> PyResult<Bound<'_, PyDict>> {
    let json = py
        .detach(move || provision_json_blocking(&options_json))
        .map_err(sdk_host_error_to_pyerr)?;
    parse_py_dict(py, &json)
}

/// List valid Bloomberg SDK runtimes installed in the shared xbbg cache.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (cache_dir=None, platform=None))]
pub(crate) fn installed_sdks(
    py: Python<'_>,
    cache_dir: Option<String>,
    platform: Option<String>,
) -> PyResult<Bound<'_, PyList>> {
    let json = py
        .detach(move || installed_json(cache_dir.as_deref().map(Path::new), platform.as_deref()))
        .map_err(sdk_host_error_to_pyerr)?;
    parse_py_list(py, &json)
}

/// Default shared xbbg cache root (`XBBG_CACHE_DIR` or platform user cache).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
pub(crate) fn default_sdk_cache_dir() -> Option<String> {
    default_sdk_cache_root().map(|path| path.to_string_lossy().into_owned())
}

/// Bloomberg's public PyPI simple index URL.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
pub(crate) fn sdk_bloomberg_pypi_index() -> &'static str {
    bloomberg_pypi_index()
}

/// Run the shared xbbg SDK cache provisioning CLI without requiring a Bloomberg runtime.
///
/// Returns `(exit_code, stdout, stderr)`.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (prog, args, env=None))]
pub(crate) fn run_sdk_cli(
    py: Python<'_>,
    prog: String,
    args: Vec<String>,
    env: Option<HashMap<String, String>>,
) -> PyResult<(i32, String, String)> {
    let (code, stdout, stderr) = py.detach(move || {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = cli_run(
            &prog,
            &args,
            &|key| match &env {
                Some(map) => {
                    if let Some(value) = map.get(key) {
                        return Some(value.clone());
                    }
                    if cfg!(windows) {
                        map.iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(key))
                            .map(|(_, v)| v.clone())
                    } else {
                        None
                    }
                }
                None => std::env::var(key).ok(),
            },
            &mut out,
            &mut err,
        );
        (
            code,
            String::from_utf8_lossy(&out).into_owned(),
            String::from_utf8_lossy(&err).into_owned(),
        )
    });
    Ok((code, stdout, stderr))
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(locate_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(loaded_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(sdk_version, module)?)?;
    module.add_function(wrap_pyfunction!(provision_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(installed_sdks, module)?)?;
    module.add_function(wrap_pyfunction!(default_sdk_cache_dir, module)?)?;
    module.add_function(wrap_pyfunction!(sdk_bloomberg_pypi_index, module)?)?;
    module.add_function(wrap_pyfunction!(run_sdk_cli, module)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn dict<'py>(py: Python<'py>, code: &std::ffi::CStr) -> Bound<'py, PyDict> {
        py.eval(code, None, None)
            .expect("evaluate")
            .cast_into::<PyDict>()
            .expect("dict")
    }

    #[test]
    fn a_missing_request_searches_everything() {
        Python::initialize();
        Python::attach(|py| {
            assert_eq!(
                request_from_py(py, None).expect("request"),
                LocateRequest::default()
            );
        });
    }

    #[test]
    fn a_request_dict_becomes_a_locate_request_with_paths_as_strings() {
        Python::initialize();
        Python::attach(|py| {
            let request = dict(
                py,
                c"{'explicit': __import__('pathlib').PurePosixPath('/sdk'), 'hints': [{'label': 'wheel', 'dir': '/w'}], 'env': {'BLPAPI_ROOT': '/r'}}",
            );

            let request = request_from_py(py, Some(&request)).expect("request");

            assert_eq!(request.explicit, Some("/sdk".into()));
            assert_eq!(request.hints.len(), 1);
            assert_eq!(request.hints[0].label, "wheel");
            assert_eq!(
                request
                    .env
                    .expect("env")
                    .get("BLPAPI_ROOT")
                    .map(String::as_str),
                Some("/r")
            );
        });
    }

    #[test]
    fn an_unknown_request_key_is_a_value_error() {
        Python::initialize();
        Python::attach(|py| {
            let request = dict(py, c"{'explicitly': '/sdk'}");

            let error = request_from_py(py, Some(&request)).expect_err("unknown key");

            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().contains("explicitly"));
        });
    }

    #[test]
    fn locate_sdk_reports_candidates_and_rejections() {
        Python::initialize();
        Python::attach(|py| {
            let request = dict(py, c"{'env': {'HOME': '/nonexistent-home'}}");

            let located = locate_sdk(py, Some(&request)).expect("located");

            assert!(located.contains("candidates").expect("key"));
            assert!(located.contains("rejected").expect("key"));
        });
    }

    #[test]
    fn installed_sdks_on_empty_cache_returns_empty_list() {
        let dir = std::env::temp_dir().join(format!(
            "xbbg-pyo3-sdk-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Python::initialize();
        Python::attach(|py| {
            let installs =
                installed_sdks(py, Some(dir.display().to_string()), None).expect("installed");
            assert!(installs.is_empty());
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_sdk_cli_help_succeeds_without_runtime() {
        Python::initialize();
        Python::attach(|py| {
            let (code, stdout, stderr) =
                run_sdk_cli(py, "xbbg-sdk".into(), vec!["--help".into()], None).expect("cli");
            assert_eq!(code, 0, "stderr: {stderr}");
            assert!(stdout.contains("usage: xbbg-sdk"));
            assert!(stdout.contains("XBBG_SDK_SOURCE"));
        });
    }
}
