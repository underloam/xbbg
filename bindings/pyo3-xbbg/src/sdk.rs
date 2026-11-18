//! Loading the Bloomberg runtime from Python.
//!
//! `xbbg._core` links no Bloomberg code, so it always imports. The Python package calls
//! [`load_sdk`] right after the import with the two things only Python knows (the directory
//! `xbbg.set_sdk_path` recorded and the directory of Bloomberg's `blpapi` wheel); the search
//! order, the verification and the load itself live in Rust (`xbbg-sdk` and `xbbg-host`).
//!
//! Requests and results cross the boundary as JSON-shaped dicts with the camelCase keys of
//! `LocateRequest`, `LoadedSdk` and `Located`; paths are strings.

use std::sync::OnceLock;

use pyo3::exceptions::{PyImportError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use serde::Serialize;
use serde_json::json;
use xbbg_host::sdk::{LoadedSdk, LocateRequest, locate, locate_and_load};

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
    py.import("json")?
        .call_method1("loads", (text,))?
        .cast_into::<PyDict>()
        .map_err(PyErr::from)
}

/// Load the Bloomberg runtime into this process and describe it.
///
/// `request` holds the optional keys of `LocateRequest` (`explicit`, `hints`, `env`,
/// `cacheRoot`, `repoRoot`, `platform`); `None` searches everything with the process
/// environment. Candidates are tried in the single search order; one that fails to load
/// (including a runtime too old for xbbg) is reported and the next is tried. A runtime that is
/// already loaded is returned unchanged.
///
/// Returns `{"path", "source", "detail", "version", "rejected"}`. Raises `ImportError`
/// listing every rejected candidate when no runtime loads.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (request=None))]
pub(crate) fn load_sdk<'py>(
    py: Python<'py>,
    request: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = request_from_py(py, request)?;
    // Hashing a cached library and opening the runtime are disk work.
    let loaded = py
        .detach(move || locate_and_load(&request))
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

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(locate_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(loaded_sdk, module)?)?;
    module.add_function(wrap_pyfunction!(sdk_version, module)?)?;
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
}
