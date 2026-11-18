//! Request building: the napi object form of the host's request types.
//!
//! The flattening, `onDelayed` check and engine normalization live in
//! `xbbg_host::request`; this module only maps napi's `#[napi(object)]` types onto the
//! host's and encodes a host failure the way the rest of the addon does.

use std::collections::HashMap;

use napi::bindgen_prelude::Error;
use napi_derive::napi;
use xbbg_async::engine::RequestParams;
use xbbg_host::request::{self, SecurityOverride};

use crate::{RequestInput, SecurityOverridesInput, StringPair, host_error_to_napi};

impl TryFrom<RequestInput> for RequestParams {
    type Error = Error;

    fn try_from(input: RequestInput) -> Result<Self, Self::Error> {
        host_request(input)
            .into_request_params()
            .map_err(host_error_to_napi)
    }
}

fn host_request(input: RequestInput) -> request::RequestInput {
    request::RequestInput {
        service: input.service,
        operation: Some(input.operation),
        request_operation: input.request_operation,
        request_id: input.request_id,
        extractor: input.extractor,
        securities: input.securities,
        security: input.security,
        fields: input.fields,
        overrides: host_pairs(input.overrides),
        security_overrides: input.security_overrides.map(|entries| {
            entries
                .into_iter()
                .map(
                    |SecurityOverridesInput {
                         security,
                         overrides,
                     }| SecurityOverride {
                        security,
                        overrides: host_pairs(Some(overrides)).unwrap_or_default(),
                    },
                )
                .collect()
        }),
        elements: host_pairs(input.elements),
        kwargs: host_pairs(input.kwargs),
        json_elements: input.json_elements,
        start_date: input.start_date,
        end_date: input.end_date,
        start_datetime: input.start_datetime,
        end_datetime: input.end_datetime,
        request_tz: input.request_tz,
        output_tz: input.output_tz,
        event_type: input.event_type,
        event_types: input.event_types,
        interval: input.interval,
        options: host_pairs(input.options),
        field_types: host_pairs(input.field_types),
        include_security_errors: input.include_security_errors,
        return_eids: input.return_eids,
        on_delayed: input.on_delayed,
        validate_fields: input.validate_fields,
        search_spec: input.search_spec,
        field_ids: input.field_ids,
        format: input.format,
    }
}

fn host_pairs(input: Option<Vec<StringPair>>) -> Option<Vec<request::StringPair>> {
    input.map(|pairs| {
        pairs
            .into_iter()
            .map(|StringPair { key, value }| request::StringPair { key, value })
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

/// Plan a typed request (`bdp`, `bdh`, `bta`, ...) with the host planner.
///
/// `args` is the planner's camelCase JSON argument object (see
/// `xbbg_host::request`); dates arrive already formatted. Returns the planned
/// `RequestInput` as JSON, which the caller passes to `request`/`requestRaw`/
/// `requestIpcFile`.
#[napi]
pub fn plan_request(method: String, args: String) -> napi::Result<String> {
    request::plan(&method, &args)
        .map(|planned| planned.to_json())
        .map_err(host_error_to_napi)
}
