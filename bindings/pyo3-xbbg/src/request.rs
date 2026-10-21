use std::collections::HashMap;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use xbbg_async::engine::{RequestParams, RequestParamsInput, SecurityOverridePairs};

/// Convert a Python dictionary to Rust RequestParams.
pub(crate) fn dict_to_request_params(dict: &Bound<'_, PyDict>) -> PyResult<RequestParams> {
    // Required fields
    let service: String = dict
        .get_item("service")?
        .ok_or_else(|| PyRuntimeError::new_err("missing required field: service"))?
        .extract()?;

    let operation: String = dict
        .get_item("operation")?
        .ok_or_else(|| PyRuntimeError::new_err("missing required field: operation"))?
        .extract()?;

    let extractor: Option<String> = dict
        .get_item("extractor")?
        .map(|v| v.extract())
        .transpose()?;

    let request_operation: Option<String> = dict
        .get_item("request_operation")?
        .map(|v| v.extract())
        .transpose()?;

    let request_id: Option<String> = dict
        .get_item("request_id")?
        .map(|v| v.extract())
        .transpose()?;

    // Optional fields
    let securities: Option<Vec<String>> = dict
        .get_item("securities")?
        .map(|v| v.extract())
        .transpose()?;

    let security: Option<String> = dict
        .get_item("security")?
        .map(|v| v.extract())
        .transpose()?;

    let fields: Option<Vec<String>> = dict.get_item("fields")?.map(|v| v.extract()).transpose()?;

    let overrides: Option<Vec<(String, String)>> = dict
        .get_item("overrides")?
        .map(|v| v.extract())
        .transpose()?;

    let security_overrides: Option<SecurityOverridePairs> = dict
        .get_item("security_overrides")?
        .map(|v| v.extract())
        .transpose()?;

    let elements: Option<Vec<(String, String)>> = dict
        .get_item("elements")?
        .map(|v| v.extract())
        .transpose()?;

    let kwargs: Option<HashMap<String, String>> =
        dict.get_item("kwargs")?.map(|v| v.extract()).transpose()?;

    let start_date: Option<String> = dict
        .get_item("start_date")?
        .map(|v| v.extract())
        .transpose()?;

    let end_date: Option<String> = dict
        .get_item("end_date")?
        .map(|v| v.extract())
        .transpose()?;

    let start_datetime: Option<String> = dict
        .get_item("start_datetime")?
        .map(|v| v.extract())
        .transpose()?;

    let end_datetime: Option<String> = dict
        .get_item("end_datetime")?
        .map(|v| v.extract())
        .transpose()?;

    let event_type: Option<String> = dict
        .get_item("event_type")?
        .map(|v| v.extract())
        .transpose()?;

    let event_types: Option<Vec<String>> = dict
        .get_item("event_types")?
        .map(|v| v.extract())
        .transpose()?;

    let interval: Option<u32> = dict
        .get_item("interval")?
        .map(|v| v.extract())
        .transpose()?;

    let options: Option<Vec<(String, String)>> =
        dict.get_item("options")?.map(|v| v.extract()).transpose()?;

    let field_types: Option<HashMap<String, String>> = dict
        .get_item("field_types")?
        .map(|v| v.extract())
        .transpose()?;

    let include_security_errors: Option<bool> = dict
        .get_item("include_security_errors")?
        .map(|v| v.extract())
        .transpose()?;
    let return_eids: Option<bool> = dict
        .get_item("return_eids")?
        .map(|v| v.extract())
        .transpose()?;
    let on_delayed: Option<String> = dict
        .get_item("on_delayed")?
        .map(|v| v.extract::<Option<String>>())
        .transpose()?
        .flatten();

    let validate_fields: Option<bool> = dict
        .get_item("validate_fields")?
        .map(|v| v.extract())
        .transpose()?;

    let search_spec: Option<String> = dict
        .get_item("search_spec")?
        .map(|v| v.extract())
        .transpose()?;

    let field_ids: Option<Vec<String>> = dict
        .get_item("field_ids")?
        .map(|v| v.extract())
        .transpose()?;

    let format: Option<String> = dict.get_item("format")?.map(|v| v.extract()).transpose()?;

    let request_tz: Option<String> = dict
        .get_item("request_tz")?
        .map(|v| v.extract())
        .transpose()?;
    let output_tz: Option<String> = dict
        .get_item("output_tz")?
        .map(|v| v.extract())
        .transpose()?;

    RequestParamsInput {
        service,
        operation: Some(operation),
        request_operation,
        request_id,
        extractor,
        securities,
        security,
        fields,
        overrides,
        security_overrides,
        elements,
        kwargs,
        start_date,
        end_date,
        start_datetime,
        end_datetime,
        request_tz,
        output_tz,
        event_type,
        event_types,
        interval,
        options,
        field_types,
        include_security_errors,
        return_eids,
        on_delayed,
        validate_fields,
        search_spec,
        field_ids,
        format,
    }
    .into_request_params()
    .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}
