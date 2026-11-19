use std::collections::HashMap;

use pyo3::conversion::FromPyObjectOwned;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use xbbg_async::engine::RequestParams;
use xbbg_host::request::{RequestInput, SecurityOverride, StringPair};
use xbbg_host::style::Style;

/// The value stored under `key`, converted to `T`; `None` when the key is absent.
fn item<'py, T: FromPyObjectOwned<'py>>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<T>> {
    dict.get_item(key)?
        .map(|value| value.extract::<T>().map_err(Into::into))
        .transpose()
}

/// A required key: a missing one is a `RuntimeError` naming it.
fn required(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<String> {
    item(dict, key)?
        .ok_or_else(|| PyRuntimeError::new_err(format!("missing required field: {key}")))
}

fn pairs(values: Vec<(String, String)>) -> Vec<StringPair> {
    values
        .into_iter()
        .map(|(key, value)| StringPair { key, value })
        .collect()
}

fn pairs_of_map(values: HashMap<String, String>) -> Vec<StringPair> {
    pairs(values.into_iter().collect())
}

/// Convert a Python request dictionary (snake_case keys) to engine `RequestParams`.
///
/// The dictionary supplies the generic request members; `xbbg-host` applies the
/// engine's normalization and defaults. `service` and `operation` must be
/// present, and every failure is a `RuntimeError` carrying the engine's message.
pub(crate) fn dict_to_request_params(dict: &Bound<'_, PyDict>) -> PyResult<RequestParams> {
    let service = required(dict, "service")?;
    let operation = required(dict, "operation")?;
    RequestInput {
        service,
        operation: Some(operation),
        request_operation: item(dict, "request_operation")?,
        request_id: item(dict, "request_id")?,
        extractor: item(dict, "extractor")?,
        securities: item(dict, "securities")?,
        security: item(dict, "security")?,
        fields: item(dict, "fields")?,
        overrides: item(dict, "overrides")?.map(pairs),
        security_overrides: item::<Vec<(String, Vec<(String, String)>)>>(
            dict,
            "security_overrides",
        )?
        .map(|entries| {
            entries
                .into_iter()
                .map(|(security, overrides)| SecurityOverride {
                    security,
                    overrides: pairs(overrides),
                })
                .collect()
        }),
        elements: item(dict, "elements")?.map(pairs),
        kwargs: item(dict, "kwargs")?.map(pairs_of_map),
        json_elements: None,
        start_date: item(dict, "start_date")?,
        end_date: item(dict, "end_date")?,
        start_datetime: item(dict, "start_datetime")?,
        end_datetime: item(dict, "end_datetime")?,
        request_tz: item(dict, "request_tz")?,
        output_tz: item(dict, "output_tz")?,
        event_type: item(dict, "event_type")?,
        event_types: item(dict, "event_types")?,
        interval: item(dict, "interval")?,
        options: item(dict, "options")?.map(pairs),
        field_types: item(dict, "field_types")?.map(pairs_of_map),
        include_security_errors: item(dict, "include_security_errors")?,
        return_eids: item(dict, "return_eids")?,
        // An explicit None means "not set".
        on_delayed: item::<Option<String>>(dict, "on_delayed")?.flatten(),
        validate_fields: item(dict, "validate_fields")?,
        search_spec: item(dict, "search_spec")?,
        field_ids: item(dict, "field_ids")?,
        format: item(dict, "format")?,
    }
    .into_request_params_as(Style::Python)
    .map_err(|error| PyRuntimeError::new_err(error.message))
}
