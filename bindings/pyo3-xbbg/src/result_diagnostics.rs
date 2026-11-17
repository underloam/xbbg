//! Result diagnostics as values, for offline results.
//!
//! `xbbg.testing.create_mock_response` takes the diagnostics a result explains
//! itself with, in the shapes the `ArrowTable` accessors return, and writes
//! them as schema metadata. The encoding is `xbbg_async::diagnostics`'s: the
//! code that attaches the metadata to engine results, not a copy of it.

use std::collections::{BTreeMap, HashMap};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_arrow::TableData;
use xbbg_async::diagnostics::{FieldErrorMeta, FieldExceptionMeta, ResultDiagnostics};
use xbbg_async::engine::state::{DelayedMeta, SecurityErrorMeta};

use crate::native_arrow::{ArrowTable, core_err_to_py};

#[derive(FromPyObject)]
struct SecurityErrorArg {
    #[pyo3(item)]
    category: String,
    #[pyo3(item)]
    code: i32,
    #[pyo3(item)]
    subcategory: String,
    #[pyo3(item)]
    message: String,
}

#[derive(FromPyObject)]
struct FieldExceptionArg {
    #[pyo3(item)]
    field: String,
    #[pyo3(item)]
    category: String,
    #[pyo3(item)]
    code: i32,
    #[pyo3(item)]
    subcategory: String,
    #[pyo3(item)]
    message: String,
}

#[derive(FromPyObject)]
struct FieldErrorArg {
    #[pyo3(item)]
    category: String,
    #[pyo3(item)]
    code: i32,
    #[pyo3(item)]
    subcategory: String,
    #[pyo3(item)]
    message: String,
    #[pyo3(item, default)]
    source: Option<String>,
}

#[derive(FromPyObject)]
struct DelayedArg {
    #[pyo3(item)]
    delayed: bool,
    #[pyo3(item, default)]
    delay_minutes: Option<u32>,
}

fn extract_map<'py, T>(
    name: &str,
    value: Option<&Bound<'py, PyAny>>,
) -> PyResult<BTreeMap<String, T>>
where
    T: for<'a> FromPyObject<'a, 'py>,
    for<'a> <T as FromPyObject<'a, 'py>>::Error: std::fmt::Display,
{
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(BTreeMap::new());
    };
    value
        .extract::<BTreeMap<String, T>>()
        .map_err(|error| PyValueError::new_err(format!("invalid {name}: {error}")))
}

/// Encode result diagnostics as the `xbbg.*` schema metadata the engine attaches.
///
/// Every argument is optional and takes the shape its `ArrowTable` accessor
/// returns: `security_errors` (`{security: {category, code, subcategory,
/// message}}`), `field_exceptions` (`{security: [{field, category, code,
/// subcategory, message}, ...]}`), `delayed` (`{security: {delayed,
/// delay_minutes}}`), `type_misses` (`{field: count}`), `field_errors` (`{id:
/// {category, code, subcategory, message, source?}}`), `eid_data` (`{security:
/// [eid, ...]}`) and `bloomberg_request_id`. Empty or `None` arguments leave
/// their key absent.
///
/// Returns:
///     The schema-metadata map, key to value, exactly as engine results carry it.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (*, eid_data=None, security_errors=None, field_exceptions=None, delayed=None, type_misses=None, field_errors=None, bloomberg_request_id=None))]
fn _encode_result_diagnostics(
    eid_data: Option<&Bound<'_, PyAny>>,
    security_errors: Option<&Bound<'_, PyAny>>,
    field_exceptions: Option<&Bound<'_, PyAny>>,
    delayed: Option<&Bound<'_, PyAny>>,
    type_misses: Option<&Bound<'_, PyAny>>,
    field_errors: Option<&Bound<'_, PyAny>>,
    bloomberg_request_id: Option<String>,
) -> PyResult<HashMap<String, String>> {
    let mut diagnostics = ResultDiagnostics::default();
    diagnostics.eid_data = extract_map::<Vec<i64>>("eid_data", eid_data)?;
    diagnostics.security_errors =
        extract_map::<SecurityErrorArg>("security_errors", security_errors)?
            .into_iter()
            .map(|(security, error)| {
                (
                    security,
                    SecurityErrorMeta {
                        category: error.category,
                        code: error.code,
                        subcategory: error.subcategory,
                        message: error.message,
                    },
                )
            })
            .collect();
    diagnostics.field_exceptions =
        extract_map::<Vec<FieldExceptionArg>>("field_exceptions", field_exceptions)?
            .into_iter()
            .map(|(security, exceptions)| {
                let exceptions = exceptions
                    .into_iter()
                    .map(|exception| FieldExceptionMeta {
                        field: exception.field,
                        category: exception.category,
                        code: exception.code,
                        subcategory: exception.subcategory,
                        message: exception.message,
                    })
                    .collect();
                (security, exceptions)
            })
            .collect();
    diagnostics.delayed = extract_map::<DelayedArg>("delayed", delayed)?
        .into_iter()
        .map(|(security, status)| {
            (
                security,
                DelayedMeta {
                    delayed: status.delayed,
                    delay_minutes: status.delay_minutes,
                },
            )
        })
        .collect();
    diagnostics.type_misses = extract_map::<u64>("type_misses", type_misses)?;
    diagnostics.field_errors = extract_map::<FieldErrorArg>("field_errors", field_errors)?
        .into_iter()
        .map(|(id, error)| {
            (
                id,
                FieldErrorMeta {
                    category: error.category,
                    code: error.code,
                    subcategory: error.subcategory,
                    message: error.message,
                    source: error.source,
                },
            )
        })
        .collect();
    diagnostics.bloomberg_request_id = bloomberg_request_id;
    diagnostics.into_schema_metadata().map_err(|error| {
        PyValueError::new_err(format!("cannot encode result diagnostics: {error}"))
    })
}

/// Return `table` with `metadata` added to its schema metadata.
///
/// Keys already present are replaced; rows are shared, not copied. An
/// empty table gains an empty batch to carry the metadata.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _with_schema_metadata(
    table: PyRef<'_, ArrowTable>,
    metadata: HashMap<String, String>,
) -> PyResult<ArrowTable> {
    let data = table.table_data();
    let mut batches = data.batches.clone();
    if batches.is_empty() {
        batches.push(arrow_array::RecordBatch::new_empty(data.schema.clone()));
    }
    batches[0].schema_metadata_mut().extend(metadata);
    TableData::try_new(batches)
        .map(ArrowTable::from_data)
        .map_err(core_err_to_py)
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_encode_result_diagnostics, m)?)?;
    m.add_function(wrap_pyfunction!(_with_schema_metadata, m)?)?;
    Ok(())
}
