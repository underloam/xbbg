use std::collections::HashMap;

use napi::bindgen_prelude::{Error, Status};
use xbbg_async::engine::{
    RequestParams, RequestParamsInput, RequestParamsInputError, SecurityOverridePairs,
};

use crate::{RequestInput, SecurityOverridesInput, StringPair};

impl TryFrom<RequestInput> for RequestParams {
    type Error = Error;

    fn try_from(input: RequestInput) -> Result<Self, Self::Error> {
        let mut elements = pairs_to_tuples(input.elements);
        if let Some(raw_json) = input.json_elements {
            let value: serde_json::Value = serde_json::from_str(&raw_json).map_err(|e| {
                Error::new(
                    Status::InvalidArg,
                    format!("invalid jsonElements payload: {e}"),
                )
            })?;
            let flattened = elements.get_or_insert_with(Vec::new);
            flatten_json_elements(None, &value, flattened)?;
        }

        RequestParamsInput {
            service: input.service,
            operation: Some(input.operation),
            request_operation: input.request_operation,
            request_id: input.request_id,
            extractor: input.extractor,
            securities: input.securities,
            security: input.security,
            fields: input.fields,
            overrides: pairs_to_tuples(input.overrides),
            security_overrides: security_overrides_to_tuples(input.security_overrides),
            elements,
            kwargs: pairs_to_map(input.kwargs),
            start_date: input.start_date,
            end_date: input.end_date,
            start_datetime: input.start_datetime,
            end_datetime: input.end_datetime,
            request_tz: input.request_tz,
            output_tz: input.output_tz,
            event_type: input.event_type,
            event_types: input.event_types,
            interval: input.interval,
            options: pairs_to_tuples(input.options),
            field_types: pairs_to_map(input.field_types),
            include_security_errors: input.include_security_errors,
            return_eids: input.return_eids,
            on_delayed: input.on_delayed,
            validate_fields: input.validate_fields,
            search_spec: input.search_spec,
            field_ids: input.field_ids,
            format: input.format,
        }
        .into_request_params()
        .map_err(input_error)
    }
}

fn input_error(error: RequestParamsInputError) -> Error {
    Error::new(Status::InvalidArg, error.to_string())
}

fn flatten_json_elements(
    path: Option<&str>,
    value: &serde_json::Value,
    out: &mut Vec<(String, String)>,
) -> Result<(), Error> {
    match value {
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return Ok(());
            }
            for (key, child) in map {
                let next_path = match path {
                    Some(prefix) if !prefix.is_empty() => format!("{prefix}.{key}"),
                    _ => key.clone(),
                };
                flatten_json_elements(Some(&next_path), child, out)?;
            }
            Ok(())
        }
        serde_json::Value::Array(items) => {
            let path = path.ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    "jsonElements must be a JSON object at the top level",
                )
            })?;

            if path.contains('.') {
                out.push((
                    path.to_string(),
                    serde_json::to_string(items).map_err(|e| {
                        Error::new(
                            Status::GenericFailure,
                            format!("failed to serialize nested jsonElements array: {e}"),
                        )
                    })?,
                ));
            } else {
                for item in items {
                    out.push((path.to_string(), json_value_to_string(item)?));
                }
            }

            Ok(())
        }
        _ => {
            let path = path.ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    "jsonElements must be a JSON object at the top level",
                )
            })?;
            out.push((path.to_string(), json_value_to_string(value)?));
            Ok(())
        }
    }
}

fn json_value_to_string(value: &serde_json::Value) -> Result<String, Error> {
    match value {
        serde_json::Value::Null => Ok("null".to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::String(text) => Ok(text.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(value)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to serialize jsonElements value: {e}"),
                )
            }),
    }
}

fn pairs_to_tuples(input: Option<Vec<StringPair>>) -> Option<Vec<(String, String)>> {
    input.map(|pairs| {
        pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect()
    })
}

fn security_overrides_to_tuples(
    input: Option<Vec<SecurityOverridesInput>>,
) -> Option<SecurityOverridePairs> {
    input.map(|entries| {
        entries
            .into_iter()
            .map(|entry| {
                (
                    entry.security,
                    pairs_to_tuples(Some(entry.overrides)).unwrap_or_default(),
                )
            })
            .collect()
    })
}

pub(crate) fn pairs_to_map(input: Option<Vec<StringPair>>) -> Option<HashMap<String, String>> {
    input.map(|pairs| {
        pairs
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect()
    })
}
