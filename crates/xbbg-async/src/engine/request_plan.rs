//! Canonical request preparation for worker dispatch.
//!
//! `RequestParams` stays as the public edge DTO. This module is the single
//! boundary that applies operation defaults, validates required fields, routes
//! kwargs, and classifies the request shape consumed by workers.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::errors::BlpAsyncError;
use crate::request_builder::RequestBuilder;
use crate::schema::SchemaCache;
use crate::services::{ExtractorType, Format, Operation};

use super::state::{LongMode, OutputFormat};
use super::{OverridePairs, RequestParams, SecurityOverridePairs};

fn parse_operation(operation: &str) -> Operation {
    match Operation::from_str(operation) {
        Ok(operation) => operation,
        Err(never) => match never {},
    }
}

fn effective_operation(params: &RequestParams) -> Result<(Operation, bool), BlpAsyncError> {
    let outer = parse_operation(&params.operation);
    if matches!(outer, Operation::RawRequest) {
        let request_operation = params
            .request_operation
            .as_deref()
            .filter(|operation| !operation.is_empty())
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "request_operation is required for RawRequest".to_string(),
            })?;
        return Ok((parse_operation(request_operation), true));
    }
    if params.operation.is_empty() {
        return Err(BlpAsyncError::ConfigError {
            detail: "operation is required".to_string(),
        });
    }
    Ok((outer, false))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlannedOutput {
    pub(crate) format: OutputFormat,
    pub(crate) long_mode: LongMode,
}

impl PlannedOutput {
    fn parse(format: Option<&str>) -> Result<Self, BlpAsyncError> {
        let format = format
            .map(Format::from_str)
            .transpose()
            .map_err(|detail| BlpAsyncError::ConfigError { detail })?
            .unwrap_or_default();
        Ok(match format {
            Format::Long => Self {
                format: OutputFormat::Long,
                long_mode: LongMode::String,
            },
            Format::SemiLong => Self {
                format: OutputFormat::Wide,
                long_mode: LongMode::String,
            },
            Format::LongTyped => Self {
                format: OutputFormat::Long,
                long_mode: LongMode::Typed,
            },
            Format::LongWithMetadata => Self {
                format: OutputFormat::Long,
                long_mode: LongMode::WithMetadata,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlannedRequestShape {
    RefData(PlannedOutput),
    HistData(PlannedOutput),
    BulkData,
    Generic,
    Bql,
    Bsrch,
    FieldInfo,
    IntradayBar,
    IntradayTick,
}

impl PlannedRequestShape {
    fn from_params(
        operation: &Operation,
        raw: bool,
        params: &RequestParams,
    ) -> Result<Self, BlpAsyncError> {
        let extractor = selected_extractor(operation, raw, params)?;
        match extractor {
            ExtractorType::RefData => Ok(Self::RefData(PlannedOutput::parse(
                params.format.as_deref(),
            )?)),
            ExtractorType::HistData => Ok(Self::HistData(PlannedOutput::parse(
                params.format.as_deref(),
            )?)),
            ExtractorType::BulkData => {
                reject_format_for_shape(params, "BulkData")?;
                Ok(Self::BulkData)
            }
            ExtractorType::Generic => {
                reject_format_for_shape(params, "Generic")?;
                Ok(Self::Generic)
            }
            ExtractorType::Bql => {
                reject_format_for_shape(params, "Bql")?;
                Ok(Self::Bql)
            }
            ExtractorType::Bsrch => {
                reject_format_for_shape(params, "Bsrch")?;
                Ok(Self::Bsrch)
            }
            ExtractorType::FieldInfo => {
                reject_format_for_shape(params, "FieldInfo")?;
                Ok(Self::FieldInfo)
            }
            ExtractorType::IntradayBar => {
                reject_format_for_shape(params, "IntradayBar")?;
                Ok(Self::IntradayBar)
            }
            ExtractorType::IntradayTick => {
                reject_format_for_shape(params, "IntradayTick")?;
                Ok(Self::IntradayTick)
            }
        }
    }
}

fn selected_extractor(
    operation: &Operation,
    raw: bool,
    params: &RequestParams,
) -> Result<ExtractorType, BlpAsyncError> {
    let extractor = if params.extractor_set {
        params.extractor
    } else {
        operation.default_extractor()
    };
    validate_extractor_compatibility(operation, raw, extractor)?;
    Ok(extractor)
}

fn validate_extractor_compatibility(
    operation: &Operation,
    raw: bool,
    extractor: ExtractorType,
) -> Result<(), BlpAsyncError> {
    if raw || matches!(operation, Operation::Custom(_)) {
        return Ok(());
    }

    if extractor == operation.default_extractor()
        || matches!(extractor, ExtractorType::Generic)
        || matches!(
            (operation, extractor),
            (Operation::ReferenceData, ExtractorType::BulkData)
                | (Operation::FieldSearch, ExtractorType::FieldInfo)
        )
    {
        return Ok(());
    }

    Err(BlpAsyncError::ConfigError {
        detail: format!(
            "extractor {:?} is not compatible with operation {}",
            extractor,
            operation.as_str()
        ),
    })
}

fn reject_format_for_shape(params: &RequestParams, shape: &str) -> Result<(), BlpAsyncError> {
    if params.format.is_some() {
        return Err(BlpAsyncError::ConfigError {
            detail: format!("format is not supported for {shape} output"),
        });
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRequest {
    params: RequestParams,
    shape: PlannedRequestShape,
    operation: Operation,
    raw: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRequestBuilder {
    params: RequestParams,
    operation: Operation,
    raw: bool,
}

impl PreparedRequestBuilder {
    pub(crate) fn prepare(
        mut params: RequestParams,
        schema_cache: &SchemaCache,
    ) -> Result<Self, BlpAsyncError> {
        normalize_request_params(&mut params);
        let (operation, raw) = effective_operation(&params)?;
        apply_request_defaults_for_operation(&mut params, &operation);
        validate_request_params_with_operation(&params, &operation, raw)?;
        route_request_params(&mut params, schema_cache, &operation)?;
        Ok(Self {
            params,
            operation,
            raw,
        })
    }

    pub(crate) fn shape(&self) -> Result<PlannedRequestShape, BlpAsyncError> {
        PlannedRequestShape::from_params(&self.operation, self.raw, &self.params)
    }

    pub(crate) fn params(&self) -> &RequestParams {
        &self.params
    }

    pub(crate) fn set_field_types(&mut self, field_types: HashMap<String, String>) {
        self.params.field_types = Some(field_types);
    }

    pub(crate) fn set_intraday_datetimes(&mut self, start_datetime: String, end_datetime: String) {
        self.params.start_datetime = Some(start_datetime);
        self.params.end_datetime = Some(end_datetime);
    }

    pub(crate) fn finalize(self) -> Result<PreparedRequest, BlpAsyncError> {
        let shape = self.shape()?;
        Ok(PreparedRequest {
            params: self.params,
            shape,
            operation: self.operation,
            raw: self.raw,
        })
    }
}

impl PreparedRequest {
    #[cfg(test)]
    pub(crate) fn prepare(
        params: RequestParams,
        schema_cache: &SchemaCache,
    ) -> Result<Self, BlpAsyncError> {
        PreparedRequestBuilder::prepare(params, schema_cache)?.finalize()
    }
    pub(crate) fn shape(&self) -> PlannedRequestShape {
        self.shape
    }

    pub(crate) fn params(&self) -> &RequestParams {
        &self.params
    }
    /// Construct a shard without cloning the original full security vector.
    ///
    /// All immutable request semantics are copied directly from the prepared
    /// request while the shard-owned security and override vectors are supplied
    /// by the sharding pass.
    pub(crate) fn for_security_shard(
        &self,
        securities: Vec<String>,
        overrides: Option<OverridePairs>,
    ) -> Self {
        let params = &self.params;
        Self {
            params: RequestParams {
                service: params.service.clone(),
                operation: params.operation.clone(),
                request_operation: params.request_operation.clone(),
                request_id: params.request_id.clone(),
                extractor: params.extractor,
                extractor_set: params.extractor_set,
                securities: Some(securities),
                security: params.security.clone(),
                fields: params.fields.clone(),
                overrides,
                security_overrides: None,
                elements: params.elements.clone(),
                kwargs: params.kwargs.clone(),
                start_date: params.start_date.clone(),
                end_date: params.end_date.clone(),
                start_datetime: params.start_datetime.clone(),
                end_datetime: params.end_datetime.clone(),
                request_tz: params.request_tz.clone(),
                output_tz: params.output_tz.clone(),
                event_type: params.event_type.clone(),
                event_types: params.event_types.clone(),
                interval: params.interval,
                options: params.options.clone(),
                field_types: params.field_types.clone(),
                include_security_errors: params.include_security_errors,
                return_eids: params.return_eids,
                validate_fields: params.validate_fields,
                search_spec: params.search_spec.clone(),
                field_ids: params.field_ids.clone(),
                format: params.format.clone(),
            },
            shape: self.shape,
            operation: self.operation.clone(),
            raw: self.raw,
        }
    }

    pub(crate) fn operation(&self) -> &Operation {
        &self.operation
    }

    pub(crate) fn is_raw(&self) -> bool {
        self.raw
    }

    pub(crate) fn effective_operation(&self) -> &str {
        self.operation.as_str()
    }

    pub(crate) fn uses_intraday_security_element(&self) -> bool {
        matches!(
            self.operation(),
            Operation::IntradayBar | Operation::IntradayTick
        )
    }

    pub(crate) fn is_excel_get_grid_request(&self) -> bool {
        matches!(self.operation(), Operation::ExcelGetGrid)
    }
}

pub(crate) fn apply_request_defaults(params: &mut RequestParams) {
    if let Ok((operation, _)) = effective_operation(params) {
        apply_request_defaults_for_operation(params, &operation);
    }
}

fn apply_request_defaults_for_operation(params: &mut RequestParams, operation: &Operation) {
    if params.extractor_set {
        return;
    }

    if params.extractor != ExtractorType::default() {
        params.extractor_set = true;
        return;
    }

    params.extractor = operation.default_extractor();
}

pub(crate) fn validate_request_params(
    params: &RequestParams,
) -> Result<(Operation, bool), BlpAsyncError> {
    let (operation, raw) = effective_operation(params)?;
    validate_request_params_with_operation(params, &operation, raw)?;
    Ok((operation, raw))
}

fn validate_request_params_with_operation(
    params: &RequestParams,
    operation: &Operation,
    raw: bool,
) -> Result<(), BlpAsyncError> {
    if params.service.is_empty() {
        return Err(BlpAsyncError::ConfigError {
            detail: "service is required".to_string(),
        });
    }

    validate_field_metadata_aliases(params, operation)?;
    validate_security_overrides(params, operation, raw)?;
    validate_return_eids(params, operation)?;
    PlannedRequestShape::from_params(operation, raw, params)?;

    if raw {
        return Ok(());
    }

    match operation {
        Operation::ReferenceData => validate_reference_data(params)?,
        Operation::HistoricalData => validate_historical_data(params)?,
        Operation::IntradayBar => validate_intraday_bar(params)?,
        Operation::IntradayTick => validate_intraday_tick(params)?,
        Operation::FieldInfo | Operation::FieldSearch => validate_field_request(params, operation)?,
        // Unknown/custom operations run in power-user mode.
        Operation::Beqs
        | Operation::PortfolioData
        | Operation::InstrumentList
        | Operation::CurveList
        | Operation::GovtList
        | Operation::BqlSendQuery
        | Operation::ExcelGetGrid
        | Operation::StudyRequest
        | Operation::RawRequest
        | Operation::Custom(_) => {}
    }
    Ok(())
}

/// `returnEids` is supported by Bloomberg's reference, historical, intraday
/// bar, and intraday tick requests. Reject the first-class flag elsewhere;
/// generic requests can still pass an explicit `returnEids` element.
fn validate_return_eids(
    params: &RequestParams,
    operation: &Operation,
) -> Result<(), BlpAsyncError> {
    if !params.return_eids {
        return Ok(());
    }
    match operation {
        Operation::ReferenceData
        | Operation::HistoricalData
        | Operation::IntradayBar
        | Operation::IntradayTick => Ok(()),
        other => Err(BlpAsyncError::ConfigError {
            detail: format!(
                "return_eids is only supported for ReferenceData, HistoricalData, IntradayBar, \
                 and IntradayTick requests (got {other}); generic requests may pass the \
                 returnEids element explicitly"
            ),
        }),
    }
}

fn route_request_params(
    params: &mut RequestParams,
    schema_cache: &SchemaCache,
    operation: &Operation,
) -> Result<(), BlpAsyncError> {
    normalize_field_metadata_aliases(params, operation);

    let kwargs = params.kwargs.take().unwrap_or_default();
    if params.is_excel_get_grid_request() {
        normalize_excel_grid_params(params, kwargs);
        return Ok(());
    }

    if params.is_raw_request() {
        merge_raw_kwargs_into_elements(params, kwargs);
        return Ok(());
    }

    let routed = RequestBuilder::route_kwargs(
        schema_cache,
        &params.service,
        &params.operation,
        kwargs,
        params.overrides.take(),
    );

    if !routed.elements.is_empty() {
        params
            .elements
            .get_or_insert_with(Vec::new)
            .extend(routed.elements);
    }

    params.overrides = if routed.overrides.is_empty() {
        None
    } else {
        Some(routed.overrides)
    };

    for warning in routed.warnings {
        xbbg_log::warn!(
            service = %params.service,
            operation = %params.operation,
            warning = %warning,
            "request parameter routing warning"
        );
    }
    Ok(())
}

fn validate_field_metadata_aliases(
    params: &RequestParams,
    operation: &Operation,
) -> Result<(), BlpAsyncError> {
    match operation {
        Operation::FieldInfo
            if has_fields(params)
                && params.field_ids.as_ref().is_some_and(|ids| !ids.is_empty()) =>
        {
            return Err(BlpAsyncError::ConfigError {
                detail: "FieldInfoRequest accepts either fields or field_ids, not both".to_string(),
            });
        }
        Operation::FieldSearch => {
            let has_search_spec = params
                .search_spec
                .as_ref()
                .is_some_and(|spec| !spec.is_empty());
            if has_fields(params) && has_search_spec {
                return Err(BlpAsyncError::ConfigError {
                    detail: "FieldSearchRequest accepts either fields or search_spec, not both"
                        .to_string(),
                });
            }
            if !has_search_spec
                && let Some(field_values) =
                    params.fields.as_ref().filter(|values| !values.is_empty())
                && field_values.len() != 1
            {
                return Err(BlpAsyncError::ConfigError {
                            detail: "FieldSearchRequest requires exactly one field value when fields is used as a search alias".to_string(),
                        });
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn normalize_request_params(params: &mut RequestParams) {
    normalize_vec(&mut params.securities);
    normalize_vec(&mut params.fields);
    normalize_vec(&mut params.event_types);
    normalize_vec(&mut params.field_ids);
    normalize_pairs(&mut params.overrides);
    normalize_security_overrides(&mut params.security_overrides);
    normalize_pairs(&mut params.elements);
    normalize_pairs(&mut params.options);
    normalize_map(&mut params.kwargs);
    normalize_map(&mut params.field_types);
    normalize_string(&mut params.request_operation);
    normalize_string(&mut params.request_id);
    normalize_string(&mut params.security);
    normalize_string(&mut params.start_date);
    normalize_string(&mut params.end_date);
    normalize_string(&mut params.start_datetime);
    normalize_string(&mut params.end_datetime);
    normalize_string(&mut params.request_tz);
    normalize_string(&mut params.output_tz);
    normalize_string(&mut params.event_type);
    normalize_string(&mut params.search_spec);
    normalize_string(&mut params.format);
}

fn normalize_vec<T>(value: &mut Option<Vec<T>>) {
    if value.as_ref().is_some_and(Vec::is_empty) {
        *value = None;
    }
}

fn normalize_pairs(value: &mut Option<Vec<(String, String)>>) {
    if value.as_ref().is_some_and(Vec::is_empty) {
        *value = None;
    }
}

fn normalize_security_overrides(value: &mut Option<SecurityOverridePairs>) {
    let Some(entries) = value else {
        return;
    };

    for (security, overrides) in entries.iter_mut() {
        trim_string(security);
        for (key, _) in overrides.iter_mut() {
            trim_string(key);
        }
        overrides.retain(|(key, _)| !key.is_empty());
    }
    entries.retain(|(security, overrides)| !security.is_empty() && !overrides.is_empty());
    if entries.is_empty() {
        *value = None;
    }
}

fn trim_string(value: &mut String) {
    let trimmed = value.trim();
    if trimmed.len() != value.len() {
        *value = trimmed.to_string();
    }
}

fn normalize_map(value: &mut Option<HashMap<String, String>>) {
    if value.as_ref().is_some_and(HashMap::is_empty) {
        *value = None;
    }
}

fn normalize_string(value: &mut Option<String>) {
    if value.as_ref().is_some_and(|value| value.is_empty()) {
        *value = None;
    }
}

fn normalize_field_metadata_aliases(params: &mut RequestParams, operation: &Operation) {
    match operation {
        Operation::FieldInfo if params.field_ids.is_none() => {
            params.field_ids = params.fields.take();
        }
        Operation::FieldSearch if params.search_spec.is_none() => {
            if let Some(mut field_values) = params.fields.take() {
                debug_assert_eq!(field_values.len(), 1);
                params.search_spec = field_values.pop();
            }
        }
        _ => {}
    }
}

fn normalize_excel_grid_params(params: &mut RequestParams, kwargs: HashMap<String, String>) {
    let mut domain: Option<String> = None;
    let mut grid_overrides: Vec<(String, String)> = Vec::new();

    fn route_pair(
        domain: &mut Option<String>,
        grid_overrides: &mut Vec<(String, String)>,
        key: String,
        value: String,
    ) {
        if key.eq_ignore_ascii_case("Domain") {
            *domain = Some(value);
        } else if !key.is_empty() && !key.eq_ignore_ascii_case("Overrides") {
            grid_overrides.push((key, value));
        }
    }

    for (key, value) in params.elements.take().unwrap_or_default() {
        route_pair(&mut domain, &mut grid_overrides, key, value);
    }

    for (key, value) in params.overrides.take().unwrap_or_default() {
        route_pair(&mut domain, &mut grid_overrides, key, value);
    }

    let mut keys: Vec<String> = kwargs.keys().cloned().collect();
    keys.sort();
    for key in keys {
        if let Some(value) = kwargs.get(&key) {
            route_pair(&mut domain, &mut grid_overrides, key, value.clone());
        }
    }

    params.elements = domain.map(|value| vec![("Domain".to_string(), value)]);
    params.overrides = (!grid_overrides.is_empty()).then_some(grid_overrides);
}

fn merge_raw_kwargs_into_elements(params: &mut RequestParams, kwargs: HashMap<String, String>) {
    if kwargs.is_empty() {
        return;
    }

    let mut keys: Vec<String> = kwargs.keys().cloned().collect();
    keys.sort();

    let elements = params.elements.get_or_insert_with(Vec::new);
    for key in keys {
        if let Some(value) = kwargs.get(&key) {
            elements.push((key, value.clone()));
        }
    }
}

fn validate_security_overrides(
    params: &RequestParams,
    operation: &Operation,
    raw: bool,
) -> Result<(), BlpAsyncError> {
    let Some(security_overrides) = params
        .security_overrides
        .as_ref()
        .filter(|entries| !entries.is_empty())
    else {
        return Ok(());
    };

    if raw {
        return Err(BlpAsyncError::ConfigError {
            detail: "security_overrides are not supported for RawRequest".to_string(),
        });
    }

    if !matches!(
        operation,
        Operation::ReferenceData | Operation::HistoricalData
    ) {
        return Err(BlpAsyncError::ConfigError {
            detail: "security_overrides are supported only for ReferenceDataRequest and HistoricalDataRequest".to_string(),
        });
    }

    let securities = params
        .securities
        .as_ref()
        .ok_or_else(|| BlpAsyncError::ConfigError {
            detail: "securities are required when security_overrides are provided".to_string(),
        })?;
    let requested: HashSet<&str> = securities.iter().map(String::as_str).collect();
    let mut seen = HashSet::with_capacity(security_overrides.len());
    for (security, overrides) in security_overrides {
        if overrides.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: format!("security_overrides entry for {security} has no overrides"),
            });
        }
        if !requested.contains(security.as_str()) {
            return Err(BlpAsyncError::ConfigError {
                detail: format!("security_overrides contains security not in request: {security}"),
            });
        }
        if !seen.insert(security.as_str()) {
            return Err(BlpAsyncError::ConfigError {
                detail: format!("security_overrides contains duplicate security: {security}"),
            });
        }
    }

    Ok(())
}

fn validate_reference_data(params: &RequestParams) -> Result<(), BlpAsyncError> {
    if !has_securities(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "securities is required for ReferenceDataRequest".to_string(),
        });
    }

    if !has_fields(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "fields is required for ReferenceDataRequest".to_string(),
        });
    }

    Ok(())
}

fn validate_historical_data(params: &RequestParams) -> Result<(), BlpAsyncError> {
    if !has_securities(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "securities is required for HistoricalDataRequest".to_string(),
        });
    }

    if !has_fields(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "fields is required for HistoricalDataRequest".to_string(),
        });
    }

    if !has_start_date(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "start_date is required for HistoricalDataRequest".to_string(),
        });
    }

    if !has_end_date(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "end_date is required for HistoricalDataRequest".to_string(),
        });
    }

    Ok(())
}

fn validate_intraday_bar(params: &RequestParams) -> Result<(), BlpAsyncError> {
    if !has_security(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "security is required for IntradayBarRequest".to_string(),
        });
    }

    if !has_event_type(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "event_type is required for IntradayBarRequest".to_string(),
        });
    }

    if params.interval.is_none() {
        return Err(BlpAsyncError::ConfigError {
            detail: "interval is required for IntradayBarRequest".to_string(),
        });
    }

    if !has_start_datetime(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "start_datetime is required for IntradayBarRequest".to_string(),
        });
    }

    if !has_end_datetime(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "end_datetime is required for IntradayBarRequest".to_string(),
        });
    }

    Ok(())
}

fn validate_intraday_tick(params: &RequestParams) -> Result<(), BlpAsyncError> {
    if !has_security(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "security is required for IntradayTickRequest".to_string(),
        });
    }

    if !has_start_datetime(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "start_datetime is required for IntradayTickRequest".to_string(),
        });
    }

    if !has_end_datetime(params) {
        return Err(BlpAsyncError::ConfigError {
            detail: "end_datetime is required for IntradayTickRequest".to_string(),
        });
    }

    Ok(())
}

fn validate_field_request(
    params: &RequestParams,
    operation: &Operation,
) -> Result<(), BlpAsyncError> {
    let has_fields = has_fields(params);

    match operation {
        Operation::FieldInfo => {
            let has_field_ids = params.field_ids.as_ref().is_some_and(|ids| !ids.is_empty());
            if !has_fields && !has_field_ids {
                return Err(BlpAsyncError::ConfigError {
                    detail: "fields is required for field metadata requests".to_string(),
                });
            }
        }
        Operation::FieldSearch => {
            let has_search_spec = params.search_spec.as_ref().is_some_and(|s| !s.is_empty());
            if !has_fields && !has_search_spec {
                return Err(BlpAsyncError::ConfigError {
                    detail: "fields is required for field metadata requests".to_string(),
                });
            }
        }
        _ => {}
    }

    Ok(())
}

fn has_securities(params: &RequestParams) -> bool {
    params
        .securities
        .as_ref()
        .is_some_and(|values| !values.is_empty())
}

fn has_security(params: &RequestParams) -> bool {
    params
        .security
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

fn has_fields(params: &RequestParams) -> bool {
    params
        .fields
        .as_ref()
        .is_some_and(|values| !values.is_empty())
}

fn has_start_date(params: &RequestParams) -> bool {
    params
        .start_date
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

fn has_end_date(params: &RequestParams) -> bool {
    params
        .end_date
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

fn has_start_datetime(params: &RequestParams) -> bool {
    params
        .start_datetime
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

fn has_end_datetime(params: &RequestParams) -> bool {
    params
        .end_datetime
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

fn has_event_type(params: &RequestParams) -> bool {
    params
        .event_type
        .as_ref()
        .is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::schema::SchemaCache;
    use crate::services::{Operation, Service};

    fn empty_schema() -> SchemaCache {
        SchemaCache::new()
    }

    fn refdata_params() -> RequestParams {
        RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            securities: Some(vec!["AAPL US Equity".to_string()]),
            fields: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        }
    }

    #[test]
    fn prepares_reference_data_defaults_and_preserves_flags() {
        let mut field_types = HashMap::new();
        field_types.insert("PX_LAST".to_string(), "Float64".to_string());
        let params = RequestParams {
            include_security_errors: true,
            field_types: Some(field_types.clone()),
            format: Some("long_typed".to_string()),
            validate_fields: Some(false),
            ..refdata_params()
        };

        let prepared = PreparedRequest::prepare(params, &empty_schema()).unwrap();

        assert!(matches!(
            prepared.shape(),
            PlannedRequestShape::RefData(PlannedOutput {
                format: OutputFormat::Long,
                long_mode: LongMode::Typed,
            })
        ));
        let params = prepared.params();
        assert_eq!(params.extractor, ExtractorType::RefData);
        assert!(params.include_security_errors);
        assert_eq!(params.field_types.as_ref(), Some(&field_types));
        assert_eq!(params.format.as_deref(), Some("long_typed"));
        assert_eq!(params.validate_fields, Some(false));
    }

    #[test]
    fn reference_data_requires_service_securities_and_fields() {
        let mut params = refdata_params();
        params.service.clear();
        assert!(validate_request_params(&params).is_err());

        let mut params = refdata_params();
        params.securities = None;
        assert!(validate_request_params(&params).is_err());

        let mut params = refdata_params();
        params.fields = Some(Vec::new());
        assert!(validate_request_params(&params).is_err());
    }

    #[test]
    fn prepares_reference_and_historical_formats_with_current_output_semantics() {
        for (format, expected) in [
            (None, (OutputFormat::Long, LongMode::String)),
            (Some(""), (OutputFormat::Long, LongMode::String)),
            (Some("long"), (OutputFormat::Long, LongMode::String)),
            (Some("long_typed"), (OutputFormat::Long, LongMode::Typed)),
            (Some("typed"), (OutputFormat::Long, LongMode::Typed)),
            (
                Some("long_metadata"),
                (OutputFormat::Long, LongMode::WithMetadata),
            ),
            (
                Some("metadata"),
                (OutputFormat::Long, LongMode::WithMetadata),
            ),
            (
                Some("with_metadata"),
                (OutputFormat::Long, LongMode::WithMetadata),
            ),
            (Some("wide"), (OutputFormat::Wide, LongMode::String)),
            (Some("semi_long"), (OutputFormat::Wide, LongMode::String)),
        ] {
            let reference = RequestParams {
                format: format.map(str::to_string),
                ..refdata_params()
            };
            let historical = RequestParams {
                operation: Operation::HistoricalData.to_string(),
                start_date: Some("20240101".to_string()),
                end_date: Some("20240131".to_string()),
                ..reference.clone()
            };
            let expected = PlannedOutput {
                format: expected.0,
                long_mode: expected.1,
            };

            for (params, expected_shape) in [
                (reference, PlannedRequestShape::RefData(expected)),
                (historical, PlannedRequestShape::HistData(expected)),
            ] {
                let prepared = PreparedRequest::prepare(params, &empty_schema()).unwrap();
                assert_eq!(prepared.shape(), expected_shape, "format: {format:?}");
            }
        }
    }

    #[test]
    fn output_format_errors_come_from_generated_parser() {
        for format in [
            "sideways",
            "LONG",
            " long",
            "long ",
            "long_with_metadata",
            "arrow",
            "json",
        ] {
            let params = RequestParams {
                format: Some(format.to_string()),
                ..refdata_params()
            };
            let error = PreparedRequest::prepare(params, &empty_schema()).unwrap_err();
            assert!(
                matches!(
                    error,
                    BlpAsyncError::ConfigError { detail }
                        if detail == Format::from_str(format).unwrap_err()
                ),
                "format: {format}"
            );
        }
    }

    #[test]
    fn prepares_intraday_bar_and_tick_inputs() {
        let bar = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::IntradayBar.to_string(),
            security: Some("AAPL US Equity".to_string()),
            event_type: Some("TRADE".to_string()),
            interval: Some(5),
            start_datetime: Some("2024-01-01T09:30:00".to_string()),
            end_datetime: Some("2024-01-01T10:00:00".to_string()),
            request_tz: Some("NY".to_string()),
            output_tz: Some("UTC".to_string()),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(bar, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::IntradayBar);
        assert_eq!(prepared.params().request_tz.as_deref(), Some("NY"));
        assert_eq!(prepared.params().output_tz.as_deref(), Some("UTC"));

        let tick = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::IntradayTick.to_string(),
            security: Some("AAPL US Equity".to_string()),
            event_types: Some(vec!["TRADE".to_string(), "BID".to_string()]),
            start_datetime: Some("2024-01-01T09:30:00".to_string()),
            end_datetime: Some("2024-01-01T10:00:00".to_string()),
            options: Some(vec![(
                "includeConditionCodes".to_string(),
                "true".to_string(),
            )]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(tick, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::IntradayTick);
        assert_eq!(
            prepared.params().event_types.as_ref().unwrap(),
            &["TRADE".to_string(), "BID".to_string()]
        );
        assert_eq!(
            prepared.params().options.as_ref().unwrap(),
            &[("includeConditionCodes".to_string(), "true".to_string())]
        );
    }

    #[test]
    fn prepares_field_info_and_search_alias_inputs() {
        let info = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldInfo.to_string(),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(info, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::FieldInfo);
        assert_eq!(
            prepared.params().field_ids.as_ref().unwrap(),
            &["PX_LAST".to_string()]
        );

        let search = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            search_spec: Some("price".to_string()),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(search, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::Generic);
        assert_eq!(prepared.params().search_spec.as_deref(), Some("price"));

        let normalized_search = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            search_spec: Some("price".to_string()),
            extractor: ExtractorType::FieldInfo,
            extractor_set: true,
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(normalized_search, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::FieldInfo);
        assert_eq!(prepared.params().search_spec.as_deref(), Some("price"));

        let invalid = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            ..Default::default()
        };
        assert!(PreparedRequest::prepare(invalid, &empty_schema()).is_err());
    }

    #[test]
    fn raw_request_requires_and_preserves_effective_operation() {
        let invalid = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::RawRequest.to_string(),
            ..Default::default()
        };
        assert!(PreparedRequest::prepare(invalid, &empty_schema()).is_err());

        let valid = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::RawRequest.to_string(),
            request_operation: Some(Operation::ReferenceData.to_string()),
            kwargs: Some(HashMap::from([("rawElement".to_string(), "1".to_string())])),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(valid, &empty_schema()).unwrap();
        assert_eq!(
            prepared.effective_operation(),
            Operation::ReferenceData.to_string()
        );
        assert!(matches!(prepared.shape(), PlannedRequestShape::RefData(_)));
        assert_eq!(
            prepared.params().elements.as_ref().unwrap(),
            &[("rawElement".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn prepares_bql_and_excel_grid_routing() {
        let bql = RequestParams {
            service: Service::BqlSvc.to_string(),
            operation: Operation::BqlSendQuery.to_string(),
            elements: Some(vec![("expression".to_string(), "get(px_last)".to_string())]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(bql, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::Bql);
        assert_eq!(
            prepared.params().elements.as_ref().unwrap(),
            &[("expression".to_string(), "get(px_last)".to_string())]
        );

        let grid = RequestParams {
            service: Service::ExrSvc.to_string(),
            operation: Operation::ExcelGetGrid.to_string(),
            elements: Some(vec![("Domain".to_string(), "FI".to_string())]),
            kwargs: Some(HashMap::from([("Ticker".to_string(), "IBM".to_string())])),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(grid, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::Bsrch);
        assert_eq!(
            prepared.params().elements.as_ref().unwrap(),
            &[("Domain".to_string(), "FI".to_string())]
        );
        assert_eq!(
            prepared.params().overrides.as_ref().unwrap(),
            &[("Ticker".to_string(), "IBM".to_string())]
        );
    }

    #[test]
    fn merge_raw_kwargs_into_elements_preserves_existing_elements_and_sorts_kwargs() {
        let mut params = RequestParams {
            elements: Some(vec![("alpha".to_string(), "1".to_string())]),
            ..Default::default()
        };

        merge_raw_kwargs_into_elements(
            &mut params,
            HashMap::from([
                ("zeta".to_string(), "9".to_string()),
                ("beta".to_string(), "2".to_string()),
            ]),
        );

        assert_eq!(
            params.elements,
            Some(vec![
                ("alpha".to_string(), "1".to_string()),
                ("beta".to_string(), "2".to_string()),
                ("zeta".to_string(), "9".to_string()),
            ])
        );
    }

    #[test]
    fn normalize_excel_grid_params_routes_domain_and_grid_overrides() {
        let mut params = RequestParams {
            operation: Operation::ExcelGetGrid.to_string(),
            elements: Some(vec![
                ("Domain".to_string(), "FI:OLD".to_string()),
                ("provider".to_string(), "wsi".to_string()),
            ]),
            overrides: Some(vec![
                ("location".to_string(), "nwe".to_string()),
                ("Domain".to_string(), "COMDTY:WEATHER".to_string()),
            ]),
            ..Default::default()
        };

        normalize_excel_grid_params(
            &mut params,
            HashMap::from([("model".to_string(), "ecmwf".to_string())]),
        );

        assert_eq!(
            params.elements,
            Some(vec![("Domain".to_string(), "COMDTY:WEATHER".to_string())])
        );
        assert_eq!(
            params.overrides,
            Some(vec![
                ("provider".to_string(), "wsi".to_string()),
                ("location".to_string(), "nwe".to_string()),
                ("model".to_string(), "ecmwf".to_string()),
            ])
        );
    }

    #[test]
    fn generic_field_aliases_are_normalized_in_request_plan() {
        let info = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldInfo.to_string(),
            fields: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(info, &empty_schema()).unwrap();
        assert_eq!(
            prepared.params().field_ids.as_deref(),
            Some(&["PX_LAST".to_string()][..])
        );
        assert!(prepared.params().fields.is_none());

        let search = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            fields: Some(vec!["price".to_string()]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(search, &empty_schema()).unwrap();
        assert_eq!(prepared.params().search_spec.as_deref(), Some("price"));
        assert!(prepared.params().fields.is_none());
    }

    #[test]
    fn validation_catches_metadata_alias_and_format_errors_before_routing() {
        let field_info_with_duplicate_aliases = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldInfo.to_string(),
            fields: Some(vec!["PX_LAST".to_string()]),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        };
        assert!(validate_request_params(&field_info_with_duplicate_aliases).is_err());

        let field_search_with_duplicate_aliases = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            fields: Some(vec!["price".to_string()]),
            search_spec: Some("price".to_string()),
            ..Default::default()
        };
        assert!(validate_request_params(&field_search_with_duplicate_aliases).is_err());

        let field_search_with_ambiguous_alias = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            fields: Some(vec!["price".to_string(), "last".to_string()]),
            ..Default::default()
        };
        assert!(validate_request_params(&field_search_with_ambiguous_alias).is_err());

        let refdata_wide = RequestParams {
            format: Some("wide".to_string()),
            ..refdata_params()
        };
        assert!(validate_request_params(&refdata_wide).is_ok());

        let unsupported_operation_format = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::Beqs.to_string(),
            format: Some("wide".to_string()),
            ..Default::default()
        };
        assert!(validate_request_params(&unsupported_operation_format).is_err());

        let unknown_format = RequestParams {
            format: Some("sideways".to_string()),
            ..refdata_params()
        };
        assert!(validate_request_params(&unknown_format).is_err());
    }

    #[test]
    fn explicit_extractor_must_match_planned_request_shape() {
        let bds = RequestParams {
            extractor: ExtractorType::BulkData,
            extractor_set: true,
            ..refdata_params()
        };
        let prepared = PreparedRequest::prepare(bds, &empty_schema()).unwrap();
        assert_eq!(prepared.shape(), PlannedRequestShape::BulkData);

        let invalid = RequestParams {
            extractor: ExtractorType::HistData,
            extractor_set: true,
            ..refdata_params()
        };
        let err = PreparedRequest::prepare(invalid, &empty_schema()).unwrap_err();
        assert!(err.to_string().contains("not compatible"));
    }

    #[test]
    fn empty_optional_collections_do_not_create_alias_conflicts() {
        let info = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldInfo.to_string(),
            fields: Some(Vec::new()),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(info, &empty_schema()).unwrap();
        assert_eq!(
            prepared.params().field_ids.as_deref(),
            Some(&["PX_LAST".to_string()][..])
        );
        assert!(prepared.params().fields.is_none());

        let search = RequestParams {
            service: Service::ApiFlds.to_string(),
            operation: Operation::FieldSearch.to_string(),
            fields: Some(Vec::new()),
            search_spec: Some("price".to_string()),
            ..Default::default()
        };
        let prepared = PreparedRequest::prepare(search, &empty_schema()).unwrap();
        assert_eq!(prepared.params().search_spec.as_deref(), Some("price"));
        assert!(prepared.params().fields.is_none());
    }
}
