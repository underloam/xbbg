use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Write as _};
use std::io;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use rmcp::ErrorData;
use serde::Serialize;
use serde_json::{json, Map, Number, Value};
use xbbg_core::EntitlementCheck;

use crate::request_adapter::MAX_ENTITLEMENT_EIDS;

const MAX_EID_METADATA_SECURITIES: usize = 1_000;
const MAX_EID_METADATA_SECURITY_BYTES: usize = 64 * 1024;
const OUTPUT_ENVELOPE_RESERVE: usize = 1_536;
pub(crate) const MIN_RESULT_BYTES: usize = 2_048;

const BLOCKING_CELL_THRESHOLD: usize = 4_096;
const BLOCKING_COLUMN_THRESHOLD: usize = 512;
const BLOCKING_METADATA_THRESHOLD: usize = 32 * 1024;
const BLOCKING_STRING_BYTES_THRESHOLD: usize = 256 * 1024;
const MAX_INLINE_METADATA_ENTRIES: usize = 16;

const EID_METADATA_KEY: &str = "xbbg.eid_data";
const PRIORITY_METADATA_KEYS: [&str; 3] = [
    "xbbg.security_errors",
    "xbbg.field_exceptions",
    EID_METADATA_KEY,
];

#[derive(Clone, Debug)]
pub(crate) struct ResultLimits {
    pub(crate) max_rows: usize,
    pub(crate) max_cells: usize,
    pub(crate) max_metadata_properties: usize,
    pub(crate) max_metadata_bytes: usize,
    pub(crate) max_string_chars: usize,
    pub(crate) max_string_bytes: usize,
    pub(crate) max_result_bytes: usize,
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            max_rows: 500,
            max_cells: 50_000,
            max_metadata_properties: 50_000,
            max_metadata_bytes: 64 * 1024,
            max_string_chars: 2_048,
            max_string_bytes: 8 * 1024,
            max_result_bytes: 1024 * 1024,
        }
    }
}

pub(crate) fn should_offload(batch: &RecordBatch, limits: &ResultLimits) -> bool {
    let rows = batch.num_rows().min(limits.max_rows);
    let columns = batch.num_columns().min(limits.max_cells);
    let cells = rows.saturating_mul(columns).min(limits.max_cells);
    if cells >= BLOCKING_CELL_THRESHOLD || batch.num_columns() >= BLOCKING_COLUMN_THRESHOLD {
        return true;
    }
    for column in batch.columns().iter().take(columns) {
        let string_bytes = match column.data_type() {
            DataType::Utf8 => column
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|array| array.value_data().len()),
            DataType::LargeUtf8 => column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|array| array.value_data().len()),
            DataType::Binary => column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(|array| array.value_data().len()),
            DataType::LargeBinary => column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(|array| array.value_data().len()),
            DataType::FixedSizeBinary(_) => column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .map(|array| array.value_data().len()),
            data_type if nested_data_type(data_type) => return true,
            _ => Some(0),
        };
        if string_bytes.is_none_or(|bytes| bytes >= BLOCKING_STRING_BYTES_THRESHOLD) {
            return true;
        }
    }

    let metadata = batch.schema();
    if metadata.metadata().len() > MAX_INLINE_METADATA_ENTRIES {
        return true;
    }
    let metadata_bytes = metadata
        .metadata()
        .iter()
        .fold(0usize, |total, (key, value)| {
            total.saturating_add(key.len()).saturating_add(value.len())
        });
    metadata_bytes >= BLOCKING_METADATA_THRESHOLD
}

pub(crate) fn should_offload_items(items: usize) -> bool {
    items >= BLOCKING_CELL_THRESHOLD
}

pub(crate) fn record_batch_to_json(
    batch: &RecordBatch,
    limits: &ResultLimits,
) -> Result<Value, ErrorData> {
    if limits.max_result_bytes < MIN_RESULT_BYTES {
        return Err(ErrorData::internal_error(
            format!(
                "MCP result byte budget must be at least {MIN_RESULT_BYTES}, got {}",
                limits.max_result_bytes
            ),
            None,
        ));
    }

    let schema = batch.schema();
    let total_rows = batch.num_rows();
    let total_columns = batch.num_columns();
    let mut component_budget = ComponentBudget::new(
        limits
            .max_result_bytes
            .saturating_sub(OUTPUT_ENVELOPE_RESERVE),
    );

    // Preserve diagnostics ahead of ordinary rows: bound raw input before parsing, then retain
    // complete diagnostic records while the property budget remains.
    let metadata_output = build_metadata(schema.metadata(), limits, &mut component_budget)?;

    let mut schema_json = Vec::new();
    let mut selected_columns = Vec::new();
    let mut selected_names = HashSet::new();
    let mut known_duplicate_columns = 0usize;
    let mut known_invalid_identity_columns = 0usize;
    let mut schema_column_count_complete = true;
    let mut schema_value_stats = ValueStats::default();
    let inspected_column_limit = total_columns.min(limits.max_cells);
    let mut schema_bytes_remaining = if total_rows == 0 {
        component_budget.remaining()
    } else {
        component_budget.remaining() / 2
    };
    for (column_index, field) in schema
        .fields()
        .iter()
        .take(inspected_column_limit)
        .enumerate()
    {
        // Field names are object keys and cannot be shortened without changing identity.
        if !identity_string_fits(field.name(), limits) {
            known_invalid_identity_columns += 1;
            continue;
        }
        if !selected_names.insert(field.name().clone()) {
            known_duplicate_columns += 1;
            continue;
        }

        let (data_type, type_stats) = bounded_display(field.data_type(), limits);
        let descriptor = json!({
            "name": field.name(),
            "data_type": data_type,
            "nullable": field.is_nullable(),
        });
        let descriptor_cost = serialized_len(&descriptor)?.saturating_add(1);
        if descriptor_cost > schema_bytes_remaining {
            component_budget.mark_truncated();
            schema_column_count_complete = false;
            break;
        }
        if !component_budget.try_take(descriptor_cost) {
            schema_column_count_complete = false;
            break;
        }
        schema_bytes_remaining -= descriptor_cost;
        schema_value_stats.merge(type_stats);
        schema_json.push(descriptor);
        selected_columns.push((column_index, serialized_len(field.name())?));
    }
    schema_column_count_complete &= inspected_column_limit == total_columns;

    let returned_columns = selected_columns.len();
    let row_limit = if total_columns == 0 {
        total_rows.min(limits.max_rows)
    } else {
        total_rows
            .min(limits.max_rows)
            .min(limits.max_cells.checked_div(returned_columns).unwrap_or(0))
    };

    // Keep Time64 and Timestamp on the reusable Arrow formatter path: it preserves
    // fractional seconds, leaves time-only values without a date or timezone, and
    // retains timestamp offsets (UTC as Z) without inventing one for naive values.
    let format_options = FormatOptions::default().with_display_error(true);
    let column_formatters = selected_columns
        .iter()
        .map(|(column_index, _)| {
            let column = &batch.columns()[*column_index];
            if row_limit == 0
                || directly_serialized_type(column.data_type())
                || nested_data_type(column.data_type())
            {
                Ok(None)
            } else {
                ArrayFormatter::try_new(column.as_ref(), &format_options)
                    .map(Some)
                    .map_err(|error| display_error(error, limits))
            }
        })
        .collect::<Result<Vec<_>, ErrorData>>()?;

    let mut rows = Vec::with_capacity(row_limit.min(256));
    let mut row_value_stats = ValueStats::default();
    for row_index in 0..row_limit {
        let mut row = Map::new();
        let mut row_bytes = 2usize; // opening and closing braces
        let mut candidate_stats = ValueStats::default();
        let mut fits = true;

        for ((column_index, field_name_bytes), formatter) in
            selected_columns.iter().zip(&column_formatters)
        {
            let field = &schema.fields()[*column_index];
            let value = array_cell_to_json(
                &batch.columns()[*column_index],
                field.data_type(),
                formatter.as_ref(),
                row_index,
                limits,
                &mut candidate_stats,
            )?;
            let separator = usize::from(!row.is_empty());
            let entry_bytes = field_name_bytes
                .saturating_add(1)
                .saturating_add(serialized_len(&value)?)
                .saturating_add(separator);
            if row_bytes.saturating_add(entry_bytes).saturating_add(1)
                > component_budget.remaining()
            {
                fits = false;
                break;
            }
            row_bytes = row_bytes.saturating_add(entry_bytes);
            row.insert(field.name().clone(), value);
        }

        if !fits || !component_budget.try_take(row_bytes.saturating_add(1)) {
            component_budget.mark_truncated();
            break;
        }
        row_value_stats.merge(candidate_stats);
        rows.push(Value::Object(row));
    }

    let returned_rows = rows.len();
    let returned_cells = returned_rows.saturating_mul(returned_columns);
    let total_cells = total_rows.saturating_mul(total_columns);
    let omitted_cells = total_cells.saturating_sub(returned_cells);
    let mut value_stats = metadata_output.value_stats;
    value_stats.merge(schema_value_stats);
    value_stats.merge(row_value_stats);
    let known_duplicate_cells = known_duplicate_columns.saturating_mul(total_rows);

    let metadata_truncated = metadata_output.returned_entries < metadata_output.total_entries
        || !metadata_output.input_count_complete
        || !metadata_output.property_count_complete
        || metadata_output.value_stats.truncated_values > 0
        || metadata_output.special_truncated;
    let omitted_priority_metadata = PRIORITY_METADATA_KEYS
        .iter()
        .copied()
        .filter(|key| {
            schema.metadata().contains_key(*key) && !metadata_output.metadata.contains_key(*key)
        })
        .collect::<Vec<_>>();
    let columns_truncated = returned_columns < total_columns;
    let rows_truncated = returned_rows < total_rows;

    let mut result = json!({
        "schema": schema_json,
        "row_count": total_rows,
        "column_count": total_columns,
        "returned_rows": returned_rows,
        "returned_columns": returned_columns,
        "returned_cells": returned_cells,
        "result_budget_bytes": limits.max_result_bytes,
        "truncated": {
            "rows": rows_truncated,
            "columns": columns_truncated,
            "cells": omitted_cells > 0,
            "values": value_stats.truncated_values > 0,
            "metadata": metadata_truncated,
            "output": component_budget.truncated(),
        },
        "truncation_counts": {
            "omitted_rows": total_rows.saturating_sub(returned_rows),
            "omitted_columns": total_columns.saturating_sub(returned_columns),
            "omitted_cells": omitted_cells,
            "known_duplicate_columns": known_duplicate_columns,
            "known_duplicate_cells": known_duplicate_cells,
            "known_invalid_identity_columns": known_invalid_identity_columns,
            "schema_column_count_complete": schema_column_count_complete,
            "truncated_values": value_stats.truncated_values,
            "known_omitted_value_bytes": value_stats.known_omitted_bytes,
            "omitted_complex_values": value_stats.omitted_complex_values,
            "omitted_metadata_entries": metadata_output.total_entries.saturating_sub(metadata_output.returned_entries),
            "inspected_metadata_entries": metadata_output.inspected_entries,
            "inspected_metadata_input_bytes": metadata_output.inspected_input_bytes,
            "omitted_metadata_input_bytes": if metadata_output.input_count_complete {
                json!(metadata_output.inspected_input_bytes.saturating_sub(metadata_output.returned_input_bytes))
            } else {
                Value::Null
            },
            "metadata_input_count_complete": metadata_output.input_count_complete,
            "known_omitted_metadata_properties": metadata_output.known_omitted_properties,
            "omitted_priority_metadata": omitted_priority_metadata,
            "metadata_property_count_complete": metadata_output.property_count_complete,
        },
        "rows": rows,
    });
    if !metadata_output.metadata.is_empty() {
        result["metadata"] = Value::Object(metadata_output.metadata);
    }
    if !metadata_output.counts.is_empty() {
        result["metadata_counts"] = Value::Object(metadata_output.counts);
    }

    let result_bytes = serialized_len(&result)?;
    if result_bytes > limits.max_result_bytes {
        return Err(ErrorData::internal_error(
            format!(
                "bounded MCP result exceeded its byte budget: {result_bytes} > {}",
                limits.max_result_bytes
            ),
            None,
        ));
    }
    Ok(result)
}

pub(crate) fn entitlement_check_to_json(
    service: String,
    eids: Vec<i32>,
    check: EntitlementCheck,
    limits: &ResultLimits,
) -> Result<Value, ErrorData> {
    const ENTITLEMENT_ENVELOPE_RESERVE: usize = 1_024;

    let mut service_limits = limits.clone();
    service_limits.max_string_bytes = service_limits
        .max_string_bytes
        .min(limits.max_result_bytes / 4)
        .max('…'.len_utf8());
    let mut service_stats = ValueStats::default();
    let service = bounded_json_string(
        &service,
        &service_limits,
        limits.max_result_bytes / 4,
        &mut service_stats,
    );
    let total_eids = eids.len();
    let total_failed_eids = check.failed_eids.len();
    let mut component_budget = ComponentBudget::new(
        limits
            .max_result_bytes
            .saturating_sub(ENTITLEMENT_ENVELOPE_RESERVE)
            .saturating_sub(serialized_len(&service)?),
    );
    let mut remaining_items = limits.max_cells;
    let mut failed_eids = Vec::new();
    for eid in check.failed_eids {
        if remaining_items == 0 {
            break;
        }
        let cost = serialized_len(&eid)?.saturating_add(1);
        if !component_budget.try_take(cost) {
            break;
        }
        failed_eids.push(eid);
        remaining_items -= 1;
    }
    let mut returned_eids = Vec::new();
    for eid in eids {
        if remaining_items == 0 {
            break;
        }
        let cost = serialized_len(&eid)?.saturating_add(1);
        if !component_budget.try_take(cost) {
            break;
        }
        returned_eids.push(eid);
        remaining_items -= 1;
    }

    let returned_eid_count = returned_eids.len();
    let returned_failed_eid_count = failed_eids.len();
    let output_truncated = component_budget.truncated()
        || returned_failed_eid_count < total_failed_eids
        || returned_eid_count < total_eids
        || service_stats.truncated_values > 0;
    let result = json!({
        "service": service,
        "eids": returned_eids,
        "entitled": check.entitled,
        "failed_eids": failed_eids,
        "total_eids": total_eids,
        "returned_eids": returned_eid_count,
        "total_failed_eids": total_failed_eids,
        "returned_failed_eids": returned_failed_eid_count,
        "omitted_eids": total_eids.saturating_sub(returned_eid_count),
        "omitted_failed_eids": total_failed_eids.saturating_sub(returned_failed_eid_count),
        "result_budget_bytes": limits.max_result_bytes,
        "truncated": {
            "eids": returned_eid_count < total_eids,
            "failed_eids": returned_failed_eid_count < total_failed_eids,
            "service": service_stats.truncated_values > 0,
            "output": output_truncated,
        },
    });
    let result_bytes = serialized_len(&result)?;
    if result_bytes > limits.max_result_bytes {
        return Err(ErrorData::internal_error(
            format!(
                "bounded MCP entitlement result exceeded its byte budget: {result_bytes} > {}",
                limits.max_result_bytes
            ),
            None,
        ));
    }
    Ok(result)
}

pub(crate) fn bounded_error_text(value: &str, limits: &ResultLimits) -> (String, bool) {
    bounded_json_text(value, limits, limits.max_result_bytes.saturating_sub(512))
}

pub(crate) fn bounded_json_text(
    value: &str,
    limits: &ResultLimits,
    max_serialized_bytes: usize,
) -> (String, bool) {
    let mut effective = limits.clone();
    effective.max_string_bytes = effective
        .max_string_bytes
        .min(max_serialized_bytes.max('…'.len_utf8()));
    let mut stats = ValueStats::default();
    let value = bounded_json_string(value, &effective, max_serialized_bytes, &mut stats);
    (value, stats.truncated_values > 0)
}

pub(crate) fn bounded_error_display(
    value: &impl fmt::Display,
    limits: &ResultLimits,
) -> (String, bool) {
    let mut effective = limits.clone();
    effective.max_string_bytes = effective
        .max_string_bytes
        .min(limits.max_result_bytes.saturating_sub(512))
        .max('…'.len_utf8());
    let mut writer =
        LimitedStringWriter::new(effective.max_string_chars, effective.max_string_bytes);
    let result = write!(&mut writer, "{value}");
    let writer_truncated = writer.truncated;
    if result.is_err() && !writer_truncated {
        return ("failed to format Bloomberg error".to_string(), true);
    }
    let mut writer_stats = ValueStats::default();
    let rendered = writer.finish(&mut writer_stats);
    let mut json_stats = ValueStats::default();
    let rendered = bounded_json_string(
        &rendered,
        &effective,
        limits.max_result_bytes.saturating_sub(512),
        &mut json_stats,
    );
    (
        rendered,
        writer_truncated || json_stats.truncated_values > 0,
    )
}

pub(crate) fn json_serialized_len(value: &(impl Serialize + ?Sized)) -> Result<usize, ErrorData> {
    serialized_len(value)
}
#[derive(Default)]
struct MetadataOutput {
    metadata: Map<String, Value>,
    counts: Map<String, Value>,
    total_entries: usize,
    inspected_entries: usize,
    returned_entries: usize,

    inspected_input_bytes: usize,
    returned_input_bytes: usize,
    known_omitted_properties: usize,
    input_count_complete: bool,
    property_count_complete: bool,
    value_stats: ValueStats,
    special_truncated: bool,
}

fn build_metadata(
    metadata: &HashMap<String, String>,
    limits: &ResultLimits,
    component_budget: &mut ComponentBudget,
) -> Result<MetadataOutput, ErrorData> {
    const MIN_INSPECTED_ENTRY_BYTES: usize = 8;
    const PRIORITY_COUNTS_RESERVE: usize = 192;

    let inspection_bytes = limits.max_metadata_bytes;
    let entry_limit = limits
        .max_metadata_properties
        .min(inspection_bytes / MIN_INSPECTED_ENTRY_BYTES);
    let priority_count = PRIORITY_METADATA_KEYS
        .iter()
        .filter(|key| metadata.contains_key(**key))
        .count();
    let inspect_generic = metadata.len() <= entry_limit;
    let mut ordered_keys = Vec::with_capacity(priority_count.saturating_add(if inspect_generic {
        metadata.len().saturating_sub(priority_count)
    } else {
        0
    }));
    for key in PRIORITY_METADATA_KEYS {
        if metadata.contains_key(key) {
            ordered_keys.push(key);
        }
    }
    if inspect_generic {
        let mut generic_keys = metadata
            .keys()
            .map(String::as_str)
            .filter(|key| !PRIORITY_METADATA_KEYS.contains(key))
            .collect::<Vec<_>>();
        generic_keys.sort_unstable();
        ordered_keys.extend(generic_keys);
    }

    let mut output = MetadataOutput {
        total_entries: metadata.len(),
        input_count_complete: inspect_generic,
        property_count_complete: inspect_generic,
        ..MetadataOutput::default()
    };
    let mut input_bytes_remaining = inspection_bytes;
    let mut output_bytes_remaining = limits.max_metadata_bytes.min(component_budget.remaining());
    let mut properties_remaining = limits.max_metadata_properties;

    for key in ordered_keys {
        let raw = &metadata[key];
        let input_bytes = key.len().saturating_add(raw.len());
        if output.inspected_entries == entry_limit || input_bytes > input_bytes_remaining {
            input_bytes_remaining = 0;
            output.input_count_complete = false;
            output.property_count_complete = false;
            continue;
        }

        // Charge inspection before parsing, even when identity, property, or output limits later
        // reject the entry.
        input_bytes_remaining -= input_bytes;
        output.inspected_entries += 1;
        output.inspected_input_bytes = output.inspected_input_bytes.saturating_add(input_bytes);

        if properties_remaining == 0 || !identity_string_fits(key, limits) {
            output.property_count_complete = false;
            continue;
        }

        let key_cost = serialized_len(key)?.saturating_add(2);
        let available_output = output_bytes_remaining.min(component_budget.remaining());
        let value_budget = available_output.saturating_sub(key_cost).saturating_sub(
            if PRIORITY_METADATA_KEYS.contains(&key) {
                PRIORITY_COUNTS_RESERVE
            } else {
                0
            },
        );
        if value_budget < 2 {
            component_budget.mark_truncated();
            output.property_count_complete = false;
            continue;
        }

        let mut entry = if key == EID_METADATA_KEY {
            bounded_eid_metadata(
                raw,
                limits,
                properties_remaining.saturating_sub(1),
                value_budget,
            )?
        } else {
            bounded_metadata_entry(
                raw,
                limits,
                properties_remaining.saturating_sub(1),
                value_budget,
            )?
        };
        if entry.counts.is_none() && PRIORITY_METADATA_KEYS.contains(&key) {
            entry.counts = Some(json!({
                "returned_properties": entry.returned_properties,
                "known_omitted_properties": entry.known_omitted_properties,
                "property_count_complete": entry.property_count_complete,
            }));
        }
        let metadata_cost = serialized_len(key)?
            .saturating_add(1)
            .saturating_add(serialized_len(&entry.value)?)
            .saturating_add(1);
        let counts_cost = match &entry.counts {
            Some(counts) => serialized_len(key)?
                .saturating_add(1)
                .saturating_add(serialized_len(counts)?)
                .saturating_add(1),
            None => 0,
        };
        let output_bytes = metadata_cost.saturating_add(counts_cost);
        if output_bytes > output_bytes_remaining || !component_budget.try_take(output_bytes) {
            output.property_count_complete = false;
            continue;
        }

        output_bytes_remaining -= output_bytes;
        properties_remaining =
            properties_remaining.saturating_sub(1usize.saturating_add(entry.returned_properties));
        output.returned_entries += 1;
        output.returned_input_bytes = output.returned_input_bytes.saturating_add(input_bytes);
        output.known_omitted_properties = output
            .known_omitted_properties
            .saturating_add(entry.known_omitted_properties);
        output.property_count_complete &= entry.property_count_complete;
        output.value_stats.merge(entry.value_stats);
        output.special_truncated |= entry.special_truncated;
        output.metadata.insert(key.to_string(), entry.value);
        if let Some(counts) = entry.counts {
            output.counts.insert(key.to_string(), counts);
        }
    }

    output.input_count_complete &= output.inspected_entries == metadata.len();
    output.property_count_complete &=
        output.input_count_complete && output.returned_entries == metadata.len();
    Ok(output)
}

struct MetadataEntry {
    value: Value,
    counts: Option<Value>,
    returned_properties: usize,
    known_omitted_properties: usize,
    property_count_complete: bool,
    value_stats: ValueStats,
    special_truncated: bool,
}

fn bounded_metadata_entry(
    raw: &str,
    limits: &ResultLimits,
    max_properties: usize,
    max_output_bytes: usize,
) -> Result<MetadataEntry, ErrorData> {
    let parsed =
        serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned()));
    let bounded = bound_metadata_value(&parsed, limits, max_properties, max_output_bytes)?;
    Ok(MetadataEntry {
        value: bounded.value,
        counts: None,
        returned_properties: bounded.returned_properties,
        known_omitted_properties: bounded.known_omitted_properties,
        property_count_complete: bounded.property_count_complete,
        value_stats: bounded.value_stats,
        special_truncated: false,
    })
}

fn bounded_eid_metadata(
    raw: &str,
    limits: &ResultLimits,
    max_properties: usize,
    max_output_bytes: usize,
) -> Result<MetadataEntry, ErrorData> {
    let invalid = || MetadataEntry {
        value: if max_properties > 0 && max_output_bytes >= 16 {
            json!({"invalid": true})
        } else {
            json!({})
        },
        counts: Some(json!({
            "total_eids": Value::Null,
            "returned_eids": 0,
            "total_securities": Value::Null,
            "returned_securities": 0,
            "valid": false,
            "counts_complete": false,
        })),
        returned_properties: usize::from(max_properties > 0 && max_output_bytes >= 16),
        known_omitted_properties: 0,
        property_count_complete: false,
        value_stats: ValueStats::default(),
        special_truncated: true,
    };

    let Ok(securities) = serde_json::from_str::<BTreeMap<String, Vec<i32>>>(raw) else {
        return Ok(invalid());
    };
    if securities.values().flatten().any(|eid| *eid <= 0) {
        return Ok(invalid());
    }

    let total_securities = securities.len();
    let total_eids = securities
        .values()
        .fold(0usize, |total, eids| total.saturating_add(eids.len()));
    let total_properties = total_securities.saturating_add(total_eids);
    let mut returned_securities = 0usize;
    let mut returned_security_bytes = 0usize;
    let mut returned_eids = 0usize;
    let mut returned_properties = 0usize;
    let mut value_bytes = 2usize;
    let mut bounded = Map::new();

    for (security, eids) in securities {
        if returned_properties == max_properties {
            break;
        }
        let security_bytes = security.len();
        if returned_securities == MAX_EID_METADATA_SECURITIES
            || !identity_string_fits(&security, limits)
            || returned_security_bytes.saturating_add(security_bytes)
                > MAX_EID_METADATA_SECURITY_BYTES
        {
            continue;
        }

        let key_bytes = serialized_len(&security)?;
        let separator = usize::from(!bounded.is_empty());
        let fixed_bytes = separator
            .saturating_add(key_bytes)
            .saturating_add(1)
            .saturating_add(2);
        if value_bytes.saturating_add(fixed_bytes) > max_output_bytes {
            continue;
        }

        let mut kept = Vec::new();
        let mut array_bytes = 2usize;
        let eid_limit = MAX_ENTITLEMENT_EIDS
            .saturating_sub(returned_eids)
            .min(max_properties.saturating_sub(returned_properties + 1));
        for eid in eids.iter().take(eid_limit) {
            let eid_bytes = serialized_len(eid)?;
            let eid_separator = usize::from(!kept.is_empty());
            let candidate_bytes = value_bytes
                .saturating_add(fixed_bytes)
                .saturating_add(array_bytes.saturating_sub(2))
                .saturating_add(eid_separator)
                .saturating_add(eid_bytes);
            if candidate_bytes > max_output_bytes {
                break;
            }
            array_bytes = array_bytes
                .saturating_add(eid_separator)
                .saturating_add(eid_bytes);
            kept.push(*eid);
        }

        value_bytes = value_bytes
            .saturating_add(separator)
            .saturating_add(key_bytes)
            .saturating_add(1)
            .saturating_add(array_bytes);
        returned_eids += kept.len();
        returned_properties += 1 + kept.len();
        returned_securities += 1;
        returned_security_bytes += security_bytes;
        bounded.insert(security, json!(kept));
    }

    let special_truncated = total_eids > returned_eids || total_securities > returned_securities;
    Ok(MetadataEntry {
        value: Value::Object(bounded),
        counts: Some(json!({
            "total_eids": total_eids,
            "returned_eids": returned_eids,
            "total_securities": total_securities,
            "returned_securities": returned_securities,
            "valid": true,
            "counts_complete": true,
        })),
        returned_properties,
        known_omitted_properties: total_properties.saturating_sub(returned_properties),
        property_count_complete: true,
        value_stats: ValueStats::default(),
        special_truncated,
    })
}

struct BoundedMetadataValue {
    value: Value,
    bytes: usize,
    returned_properties: usize,
    known_omitted_properties: usize,
    property_count_complete: bool,
    value_stats: ValueStats,
}

fn bound_metadata_value(
    value: &Value,
    limits: &ResultLimits,
    max_properties: usize,
    max_bytes: usize,
) -> Result<BoundedMetadataValue, ErrorData> {
    match value {
        Value::Object(object) => {
            let mut bounded = Map::new();
            let mut bytes = 2usize;
            let mut returned_properties = 0usize;
            let mut known_omitted_properties = 0usize;
            let mut property_count_complete = true;
            let mut value_stats = ValueStats::default();

            for (index, (key, child)) in object.iter().enumerate() {
                if returned_properties == max_properties {
                    known_omitted_properties =
                        known_omitted_properties.saturating_add(object.len().saturating_sub(index));
                    property_count_complete = false;
                    break;
                }
                if !identity_string_fits(key, limits) {
                    known_omitted_properties += 1;
                    property_count_complete = false;
                    continue;
                }
                let separator = usize::from(!bounded.is_empty());
                let key_bytes = serialized_len(key)?;
                let overhead = separator.saturating_add(key_bytes).saturating_add(1);
                let child_budget = max_bytes.saturating_sub(bytes.saturating_add(overhead));
                let child_value = bound_metadata_value(
                    child,
                    limits,
                    max_properties.saturating_sub(returned_properties + 1),
                    child_budget,
                )?;
                if !child_value.property_count_complete {
                    if let Some(atomic_properties) = atomic_object_properties(child) {
                        known_omitted_properties =
                            known_omitted_properties.saturating_add(1 + atomic_properties);
                        property_count_complete = false;
                        continue;
                    }
                }
                let candidate_bytes = bytes
                    .saturating_add(overhead)
                    .saturating_add(child_value.bytes);
                if candidate_bytes > max_bytes {
                    known_omitted_properties += 1;
                    property_count_complete = false;
                    continue;
                }
                bytes = candidate_bytes;
                returned_properties =
                    returned_properties.saturating_add(1 + child_value.returned_properties);
                known_omitted_properties =
                    known_omitted_properties.saturating_add(child_value.known_omitted_properties);
                property_count_complete &= child_value.property_count_complete;
                value_stats.merge(child_value.value_stats);
                bounded.insert(key.clone(), child_value.value);
            }
            Ok(BoundedMetadataValue {
                value: Value::Object(bounded),
                bytes,
                returned_properties,
                known_omitted_properties,
                property_count_complete,
                value_stats,
            })
        }
        Value::Array(array) => {
            let mut bounded = Vec::new();
            let mut bytes = 2usize;
            let mut returned_properties = 0usize;
            let mut known_omitted_properties = 0usize;
            let mut property_count_complete = true;
            let mut value_stats = ValueStats::default();

            for (index, child) in array.iter().enumerate() {
                if returned_properties == max_properties {
                    known_omitted_properties =
                        known_omitted_properties.saturating_add(array.len().saturating_sub(index));
                    property_count_complete = false;
                    break;
                }
                let separator = usize::from(!bounded.is_empty());
                let child_budget = max_bytes.saturating_sub(bytes.saturating_add(separator));
                let child_value = bound_metadata_value(
                    child,
                    limits,
                    max_properties.saturating_sub(returned_properties + 1),
                    child_budget,
                )?;
                if !child_value.property_count_complete {
                    if let Some(atomic_properties) = atomic_object_properties(child) {
                        known_omitted_properties =
                            known_omitted_properties.saturating_add(1 + atomic_properties);
                        property_count_complete = false;
                        continue;
                    }
                }
                let candidate_bytes = bytes
                    .saturating_add(separator)
                    .saturating_add(child_value.bytes);
                if candidate_bytes > max_bytes {
                    known_omitted_properties += 1;
                    property_count_complete = false;
                    continue;
                }
                bytes = candidate_bytes;
                returned_properties =
                    returned_properties.saturating_add(1 + child_value.returned_properties);
                known_omitted_properties =
                    known_omitted_properties.saturating_add(child_value.known_omitted_properties);
                property_count_complete &= child_value.property_count_complete;
                value_stats.merge(child_value.value_stats);
                bounded.push(child_value.value);
            }
            Ok(BoundedMetadataValue {
                value: Value::Array(bounded),
                bytes,
                returned_properties,
                known_omitted_properties,
                property_count_complete,
                value_stats,
            })
        }
        Value::String(value) => {
            let mut value_stats = ValueStats::default();
            let bounded = bounded_json_string(value, limits, max_bytes, &mut value_stats);
            let bounded_value = Value::String(bounded);
            let bytes = serialized_len(&bounded_value)?;
            Ok(BoundedMetadataValue {
                value: if bytes <= max_bytes {
                    bounded_value
                } else {
                    Value::Null
                },
                bytes: if bytes <= max_bytes { bytes } else { 4 },
                returned_properties: 0,
                known_omitted_properties: 0,
                property_count_complete: bytes <= max_bytes,
                value_stats,
            })
        }
        primitive => {
            let bytes = serialized_len(primitive)?;
            Ok(BoundedMetadataValue {
                value: primitive.clone(),
                bytes,
                returned_properties: 0,
                known_omitted_properties: 0,
                property_count_complete: bytes <= max_bytes,
                value_stats: ValueStats::default(),
            })
        }
    }
}

fn atomic_object_properties(value: &Value) -> Option<usize> {
    let Value::Object(object) = value else {
        return None;
    };
    object
        .values()
        .all(|value| !matches!(value, Value::Object(_) | Value::Array(_)))
        .then_some(object.len())
}

fn directly_serialized_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
    )
}

fn nested_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

#[derive(Clone, Copy, Default)]
struct ValueStats {
    truncated_values: usize,
    known_omitted_bytes: usize,
    omitted_complex_values: usize,
}

impl ValueStats {
    fn merge(&mut self, other: Self) {
        self.truncated_values = self.truncated_values.saturating_add(other.truncated_values);
        self.known_omitted_bytes = self
            .known_omitted_bytes
            .saturating_add(other.known_omitted_bytes);
        self.omitted_complex_values = self
            .omitted_complex_values
            .saturating_add(other.omitted_complex_values);
    }
}

fn array_cell_to_json(
    column: &ArrayRef,
    data_type: &DataType,
    formatter: Option<&ArrayFormatter<'_>>,
    row_index: usize,
    limits: &ResultLimits,
    value_stats: &mut ValueStats,
) -> Result<Value, ErrorData> {
    if column.is_null(row_index) {
        return Ok(Value::Null);
    }

    let value = match data_type {
        DataType::Utf8 => {
            let array = downcast::<StringArray>(column, "Utf8")?;
            Value::String(bounded_string(array.value(row_index), limits, value_stats))
        }
        DataType::LargeUtf8 => {
            let array = downcast::<LargeStringArray>(column, "LargeUtf8")?;
            Value::String(bounded_string(array.value(row_index), limits, value_stats))
        }
        DataType::Boolean => {
            Value::Bool(downcast::<BooleanArray>(column, "Boolean")?.value(row_index))
        }
        DataType::Int32 => Value::Number(Number::from(
            downcast::<Int32Array>(column, "Int32")?.value(row_index),
        )),
        DataType::Int64 => int64_to_json(downcast::<Int64Array>(column, "Int64")?.value(row_index)),
        DataType::UInt32 => Value::Number(Number::from(
            downcast::<UInt32Array>(column, "UInt32")?.value(row_index),
        )),
        DataType::UInt64 => {
            uint64_to_json(downcast::<UInt64Array>(column, "UInt64")?.value(row_index))
        }
        DataType::Float32 => {
            float_to_json(downcast::<Float32Array>(column, "Float32")?.value(row_index) as f64)
        }
        DataType::Float64 => {
            float_to_json(downcast::<Float64Array>(column, "Float64")?.value(row_index))
        }
        DataType::Date32 => {
            let array = downcast::<Date32Array>(column, "Date32")?;
            match array.value_as_date(row_index) {
                Some(date) => Value::String(bounded_string(&date.to_string(), limits, value_stats)),
                None => Value::String(bounded_string(
                    &array.value(row_index).to_string(),
                    limits,
                    value_stats,
                )),
            }
        }
        _ if nested_data_type(data_type) => {
            value_stats.truncated_values += 1;
            value_stats.omitted_complex_values += 1;
            Value::String(omitted_complex_value_marker(limits))
        }
        _ => Value::String(format_array_value_bounded(
            formatter.ok_or_else(|| {
                ErrorData::internal_error(
                    "missing reusable Arrow value formatter".to_string(),
                    None,
                )
            })?,
            row_index,
            limits,
            value_stats,
        )?),
    };

    Ok(value)
}

fn int64_to_json(value: i64) -> Value {
    const JS_SAFE_INTEGER_MIN: i64 = -9_007_199_254_740_991;
    const JS_SAFE_INTEGER_MAX: i64 = 9_007_199_254_740_991;

    if (JS_SAFE_INTEGER_MIN..=JS_SAFE_INTEGER_MAX).contains(&value) {
        Value::Number(Number::from(value))
    } else {
        Value::String(value.to_string())
    }
}

fn uint64_to_json(value: u64) -> Value {
    const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

    if value <= JS_SAFE_INTEGER_MAX {
        Value::Number(Number::from(value))
    } else {
        Value::String(value.to_string())
    }
}

fn float_to_json(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

fn identity_string_fits(value: &str, limits: &ResultLimits) -> bool {
    value.len() <= limits.max_string_bytes
        && value
            .chars()
            .take(limits.max_string_chars.saturating_add(1))
            .count()
            <= limits.max_string_chars
}

fn bounded_string(value: &str, limits: &ResultLimits, value_stats: &mut ValueStats) -> String {
    if identity_string_fits(value, limits) {
        return value.to_owned();
    }

    let content_char_limit = limits.max_string_chars.saturating_sub(1);
    let content_byte_limit = limits.max_string_bytes.saturating_sub('…'.len_utf8());
    let mut end = 0usize;
    for (chars, character) in value.chars().enumerate() {
        if chars == content_char_limit
            || end.saturating_add(character.len_utf8()) > content_byte_limit
        {
            break;
        }
        end += character.len_utf8();
    }

    value_stats.truncated_values += 1;
    value_stats.known_omitted_bytes = value_stats
        .known_omitted_bytes
        .saturating_add(value.len().saturating_sub(end));
    let mut bounded = String::with_capacity(end.saturating_add('…'.len_utf8()));
    bounded.push_str(&value[..end]);
    bounded.push('…');
    bounded
}

fn bounded_json_string(
    value: &str,
    limits: &ResultLimits,
    max_serialized_bytes: usize,
    value_stats: &mut ValueStats,
) -> String {
    if identity_string_fits(value, limits) {
        let serialized_bytes = value.chars().fold(2usize, |bytes, character| {
            bytes.saturating_add(json_escaped_char_bytes(character))
        });
        if serialized_bytes <= max_serialized_bytes {
            return value.to_owned();
        }
    }
    if max_serialized_bytes < 5 {
        value_stats.truncated_values += 1;
        value_stats.known_omitted_bytes =
            value_stats.known_omitted_bytes.saturating_add(value.len());
        return String::new();
    }

    let content_char_limit = limits.max_string_chars.saturating_sub(1);
    let content_byte_limit = limits.max_string_bytes.saturating_sub('…'.len_utf8());
    let content_json_limit = max_serialized_bytes
        .saturating_sub(2)
        .saturating_sub('…'.len_utf8());
    let mut raw_bytes = 0usize;
    let mut json_bytes = 0usize;
    for (chars, character) in value.chars().enumerate() {
        let character_bytes = character.len_utf8();
        let escaped_bytes = json_escaped_char_bytes(character);
        if chars == content_char_limit
            || raw_bytes.saturating_add(character_bytes) > content_byte_limit
            || json_bytes.saturating_add(escaped_bytes) > content_json_limit
        {
            break;
        }
        raw_bytes += character_bytes;
        json_bytes += escaped_bytes;
    }

    value_stats.truncated_values += 1;
    value_stats.known_omitted_bytes = value_stats
        .known_omitted_bytes
        .saturating_add(value.len().saturating_sub(raw_bytes));
    let mut bounded = String::with_capacity(raw_bytes.saturating_add('…'.len_utf8()));
    bounded.push_str(&value[..raw_bytes]);
    bounded.push('…');
    bounded
}

fn json_escaped_char_bytes(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{08}' | '\t' | '\n' | '\u{0C}' | '\r' => 2,
        '\u{00}'..='\u{1F}' => 6,
        _ => character.len_utf8(),
    }
}

fn bounded_display(value: &impl fmt::Display, limits: &ResultLimits) -> (String, ValueStats) {
    let mut writer = LimitedStringWriter::new(limits.max_string_chars, limits.max_string_bytes);
    let result = write!(&mut writer, "{value}");
    let intentionally_truncated = writer.truncated;
    if result.is_err() && !intentionally_truncated {
        return ("<format error>".to_string(), ValueStats::default());
    }
    let mut stats = ValueStats::default();
    (writer.finish(&mut stats), stats)
}

fn omitted_complex_value_marker(limits: &ResultLimits) -> String {
    let mut ignored = ValueStats::default();
    bounded_string("[nested Arrow value omitted]", limits, &mut ignored)
}

fn format_array_value_bounded(
    formatter: &ArrayFormatter<'_>,
    row_index: usize,
    limits: &ResultLimits,
    value_stats: &mut ValueStats,
) -> Result<String, ErrorData> {
    let mut writer = LimitedStringWriter::new(limits.max_string_chars, limits.max_string_bytes);
    let result = write!(&mut writer, "{}", formatter.value(row_index));
    if result.is_err() && !writer.truncated {
        return Err(ErrorData::internal_error(
            "failed to format Arrow value".to_string(),
            None,
        ));
    }
    Ok(writer.finish(value_stats))
}

struct LimitedStringWriter {
    value: String,
    max_chars: usize,
    max_bytes: usize,
    chars: usize,
    truncated: bool,
}

impl LimitedStringWriter {
    fn new(max_chars: usize, max_bytes: usize) -> Self {
        Self {
            value: String::with_capacity(max_bytes.min(256)),
            max_chars,
            max_bytes,
            chars: 0,
            truncated: false,
        }
    }

    fn finish(mut self, stats: &mut ValueStats) -> String {
        if self.truncated {
            while self.chars.saturating_add(1) > self.max_chars
                || self.value.len().saturating_add('…'.len_utf8()) > self.max_bytes
            {
                if self.value.pop().is_none() {
                    break;
                }
                self.chars = self.chars.saturating_sub(1);
            }
            self.value.push('…');
            stats.truncated_values += 1;
        }
        self.value
    }
}

impl fmt::Write for LimitedStringWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.truncated {
            return Err(fmt::Error);
        }
        for character in value.chars() {
            if self.chars == self.max_chars
                || self.value.len().saturating_add(character.len_utf8()) > self.max_bytes
            {
                self.truncated = true;
                return Err(fmt::Error);
            }
            self.value.push(character);
            self.chars += 1;
        }
        Ok(())
    }
}

fn downcast<'a, T: 'static>(column: &'a ArrayRef, label: &str) -> Result<&'a T, ErrorData> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        ErrorData::internal_error(format!("failed to downcast Arrow column as {label}"), None)
    })
}

fn display_error(error: arrow::error::ArrowError, limits: &ResultLimits) -> ErrorData {
    let (message, stats) = bounded_display(
        &format_args!("failed to format Arrow value: {error}"),
        limits,
    );
    ErrorData::internal_error(
        message,
        (stats.truncated_values > 0).then(|| json!({"message_truncated": true})),
    )
}

fn serialized_len(value: &(impl Serialize + ?Sized)) -> Result<usize, ErrorData> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        ErrorData::internal_error(format!("failed to size MCP JSON result: {error}"), None)
    })?;
    Ok(writer.bytes)
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ComponentBudget {
    remaining: usize,
    truncated: bool,
}

impl ComponentBudget {
    fn new(remaining: usize) -> Self {
        Self {
            remaining,
            truncated: false,
        }
    }

    fn remaining(&self) -> usize {
        self.remaining
    }

    fn try_take(&mut self, bytes: usize) -> bool {
        if bytes > self.remaining {
            self.truncated = true;
            return false;
        }
        self.remaining -= bytes;
        true
    }

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn truncated(&self) -> bool {
        self.truncated
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, Int32Array, ListArray, StringArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatchOptions;

    use super::*;

    fn limits() -> ResultLimits {
        ResultLimits::default()
    }

    fn batch_with_metadata(
        columns: Vec<(&str, ArrayRef)>,
        metadata: HashMap<String, String>,
    ) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
            .collect::<Vec<_>>();
        let row_count = columns.first().map_or(0, |(_, column)| column.len());
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new_with_metadata(fields, metadata)),
            columns.into_iter().map(|(_, column)| column).collect(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .unwrap()
    }

    #[test]
    fn small_results_keep_schema_rows_and_structured_metadata() {
        let batch = batch_with_metadata(
            vec![
                (
                    "security",
                    Arc::new(StringArray::from(vec!["IBM US Equity"])),
                ),
                ("PX_LAST", Arc::new(Int32Array::from(vec![123]))),
            ],
            HashMap::from([(
                "xbbg.security_errors".to_string(),
                r#"{"BAD Equity":{"category":"BAD_SEC","message":"unknown"}}"#.to_string(),
            )]),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(payload["row_count"], 1);
        assert_eq!(payload["returned_rows"], 1);
        assert_eq!(payload["returned_columns"], 2);
        assert_eq!(payload["rows"][0]["security"], "IBM US Equity");
        assert_eq!(payload["rows"][0]["PX_LAST"], 123);
        assert_eq!(payload["schema"][1]["name"], "PX_LAST");
        assert_eq!(
            payload["metadata"]["xbbg.security_errors"]["BAD Equity"]["message"],
            "unknown"
        );
        assert_eq!(
            payload["metadata_counts"]["xbbg.security_errors"],
            json!({
                "returned_properties": 3,
                "known_omitted_properties": 0,
                "property_count_complete": true,
            })
        );
        assert_eq!(payload["truncated"]["rows"], false);
        assert_eq!(payload["truncated"]["metadata"], false);
    }

    #[test]
    fn temporal_values_preserve_microseconds_nulls_and_epoch_boundaries() {
        let batch = batch_with_metadata(
            vec![
                (
                    "value_time",
                    Arc::new(Time64MicrosecondArray::from(vec![
                        Some(57_541_123_456),
                        None,
                        Some(0),
                        Some(86_399_999_999),
                    ])),
                ),
                (
                    "value_date",
                    Arc::new(Date32Array::from(vec![Some(1), None, Some(0), Some(-1)])),
                ),
                (
                    "value_ts",
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            Some(86_399_123_456),
                            None,
                            Some(0),
                            Some(-1),
                        ])
                        .with_timezone("UTC"),
                    ),
                ),
            ],
            HashMap::new(),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(
            payload["rows"],
            json!([
                {
                    "value_time": "15:59:01.123456",
                    "value_date": "1970-01-02",
                    "value_ts": "1970-01-01T23:59:59.123456Z",
                },
                {"value_time": null, "value_date": null, "value_ts": null},
                {
                    "value_time": "00:00:00",
                    "value_date": "1970-01-01",
                    "value_ts": "1970-01-01T00:00:00Z",
                },
                {
                    "value_time": "23:59:59.999999",
                    "value_date": "1969-12-31",
                    "value_ts": "1969-12-31T23:59:59.999999Z",
                },
            ])
        );
    }

    #[test]
    fn timestamp_timezones_do_not_invent_or_discard_offsets() {
        let batch = batch_with_metadata(
            vec![
                ("naive", Arc::new(TimestampMicrosecondArray::from(vec![-1]))),
                (
                    "east",
                    Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("+05:30")),
                ),
                (
                    "west",
                    Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("-05:00")),
                ),
            ],
            HashMap::new(),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(
            payload["rows"],
            json!([{
                "naive": "1969-12-31T23:59:59.999999",
                "east": "1970-01-01T05:29:59.999999+05:30",
                "west": "1969-12-31T18:59:59.999999-05:00",
            }])
        );
    }

    #[test]
    fn temporal_units_keep_their_subsecond_precision() {
        let batch = batch_with_metadata(
            vec![
                ("time_ns", Arc::new(Time64NanosecondArray::from(vec![1]))),
                (
                    "timestamp_s",
                    Arc::new(TimestampSecondArray::from(vec![-1]).with_timezone("UTC")),
                ),
                (
                    "timestamp_ms",
                    Arc::new(TimestampMillisecondArray::from(vec![-1]).with_timezone("UTC")),
                ),
                (
                    "timestamp_ns",
                    Arc::new(TimestampNanosecondArray::from(vec![-1]).with_timezone("UTC")),
                ),
            ],
            HashMap::new(),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(
            payload["rows"],
            json!([{
                "time_ns": "00:00:00.000000001",
                "timestamp_s": "1969-12-31T23:59:59Z",
                "timestamp_ms": "1969-12-31T23:59:59.999Z",
                "timestamp_ns": "1969-12-31T23:59:59.999999999Z",
            }])
        );
    }

    #[test]
    fn row_and_cell_limits_report_exact_omissions() {
        let batch = batch_with_metadata(
            vec![
                ("a", Arc::new(Int32Array::from(vec![1, 2, 3, 4]))),
                ("b", Arc::new(Int32Array::from(vec![5, 6, 7, 8]))),
                ("c", Arc::new(Int32Array::from(vec![9, 10, 11, 12]))),
            ],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_rows: 4,
            max_cells: 5,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(payload["returned_columns"], 3);
        assert_eq!(payload["returned_rows"], 1);
        assert_eq!(payload["returned_cells"], 3);
        assert_eq!(payload["truncation_counts"]["omitted_rows"], 3);
        assert_eq!(payload["truncation_counts"]["omitted_columns"], 0);
        assert_eq!(payload["truncation_counts"]["omitted_cells"], 9);
        assert_eq!(payload["truncated"]["cells"], true);
    }

    #[test]
    fn wide_results_bound_schema_and_row_properties_consistently() {
        let batch = batch_with_metadata(
            vec![
                ("a", Arc::new(Int32Array::from(vec![1, 2]))),
                ("b", Arc::new(Int32Array::from(vec![3, 4]))),
                ("c", Arc::new(Int32Array::from(vec![5, 6]))),
                ("d", Arc::new(Int32Array::from(vec![7, 8]))),
            ],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_rows: 2,
            max_cells: 2,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(payload["schema"].as_array().unwrap().len(), 2);
        assert_eq!(payload["returned_columns"], 2);
        assert_eq!(payload["returned_rows"], 1);
        assert_eq!(payload["truncation_counts"]["omitted_columns"], 2);
        assert_eq!(payload["truncation_counts"]["omitted_cells"], 6);
        assert!(payload["rows"][0].get("a").is_some());
        assert!(payload["rows"][0].get("b").is_some());
        assert!(payload["rows"][0].get("c").is_none());
        assert_eq!(payload["truncated"]["columns"], true);
    }

    #[test]
    fn result_budget_returns_valid_bounded_json_with_explicit_output_truncation() {
        let values = (0..200)
            .map(|index| format!("row-{index:03}-{}", "x".repeat(96)))
            .collect::<Vec<_>>();
        let batch = batch_with_metadata(
            vec![("value", Arc::new(StringArray::from(values)))],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_rows: 200,
            max_result_bytes: MIN_RESULT_BYTES,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();
        let encoded = serde_json::to_vec(&payload).unwrap();

        assert!(encoded.len() <= bounded.max_result_bytes);
        assert!(payload["returned_rows"].as_u64().unwrap() < 200);
        assert_eq!(payload["truncated"]["output"], true);
        assert_eq!(
            payload["truncation_counts"]["omitted_rows"]
                .as_u64()
                .unwrap()
                + payload["returned_rows"].as_u64().unwrap(),
            200
        );
        serde_json::from_slice::<Value>(&encoded).unwrap();
    }

    #[test]
    fn metadata_budget_skips_oversized_input_without_parsing_it() {
        let oversized = format!("{{\"diagnostic\":\"{}\"}}", "x".repeat(8_192));
        let batch = batch_with_metadata(
            vec![("value", Arc::new(Int32Array::from(vec![1])))],
            HashMap::from([
                ("xbbg.security_errors".to_string(), oversized.clone()),
                ("small".to_string(), r#"{"ok":true}"#.to_string()),
            ]),
        );
        let bounded = ResultLimits {
            max_metadata_bytes: 64,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert!(payload.get("metadata").is_none());
        assert_eq!(payload["truncation_counts"]["omitted_metadata_entries"], 2);
        assert_eq!(
            payload["truncation_counts"]["inspected_metadata_entries"],
            0
        );
        assert_eq!(
            payload["truncation_counts"]["omitted_metadata_input_bytes"],
            Value::Null
        );
        assert_eq!(
            payload["truncation_counts"]["metadata_input_count_complete"],
            false
        );
        assert_eq!(
            payload["truncation_counts"]["omitted_priority_metadata"],
            json!(["xbbg.security_errors"])
        );
        assert_eq!(payload["truncated"]["metadata"], true);
    }

    #[test]
    fn rejected_metadata_identity_still_consumes_the_inspection_budget() {
        let batch = batch_with_metadata(
            vec![("v", Arc::new(Int32Array::from(vec![1])))],
            HashMap::from([
                ("aa".to_string(), "1".to_string()),
                ("x".to_string(), "2".to_string()),
            ]),
        );
        let bounded = ResultLimits {
            max_string_chars: 1,
            max_string_bytes: 8,
            max_metadata_bytes: 64,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(payload["metadata"]["x"], 2);
        assert_eq!(
            payload["truncation_counts"]["inspected_metadata_entries"],
            2
        );
        assert_eq!(
            payload["truncation_counts"]["omitted_metadata_input_bytes"],
            3
        );
        assert_eq!(
            payload["truncation_counts"]["metadata_input_count_complete"],
            true
        );
    }

    #[test]
    fn metadata_property_limit_keeps_complete_error_fields_and_marks_count_incomplete() {
        let batch = batch_with_metadata(
            vec![("value", Arc::new(Int32Array::from(vec![1])))],
            HashMap::from([(
                "xbbg.security_errors".to_string(),
                r#"{"A":{"category":"BAD_SEC","message":"first"},"B":{"category":"BAD_SEC","message":"second"}}"#.to_string(),
            )]),
        );
        let bounded = ResultLimits {
            max_metadata_properties: 4,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(
            payload["metadata"]["xbbg.security_errors"]["A"],
            json!({"category": "BAD_SEC", "message": "first"})
        );
        assert!(payload["metadata"]["xbbg.security_errors"]
            .get("B")
            .is_none());
        assert_eq!(
            payload["truncation_counts"]["known_omitted_metadata_properties"],
            1
        );
        assert_eq!(
            payload["truncation_counts"]["metadata_property_count_complete"],
            false
        );
        assert_eq!(
            payload["metadata_counts"]["xbbg.security_errors"]["property_count_complete"],
            false
        );
        assert_eq!(payload["truncated"]["metadata"], true);
    }

    #[test]
    fn priority_diagnostics_keep_complete_records_at_the_minimum_result_budget() {
        let diagnostics = (0..5)
            .map(|index| {
                (
                    format!("S{index}"),
                    json!({"category": "BAD_SEC", "message": "x".repeat(300)}),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let batch = batch_with_metadata(
            vec![("value", Arc::new(Int32Array::from(vec![1])))],
            HashMap::from([(
                "xbbg.security_errors".to_string(),
                serde_json::to_string(&diagnostics).unwrap(),
            )]),
        );
        let bounded = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();
        let returned = payload["metadata"]["xbbg.security_errors"]
            .as_object()
            .unwrap();

        assert!(!returned.is_empty());
        assert!(returned.len() < diagnostics.len());
        assert!(returned
            .values()
            .all(|record| { record.get("category").is_some() && record.get("message").is_some() }));
        assert!(returned.values().next().unwrap()["message"]
            .as_str()
            .unwrap()
            .ends_with('…'));
        assert!(
            payload["truncation_counts"]["inspected_metadata_input_bytes"]
                .as_u64()
                .unwrap()
                > 512
        );
        assert_eq!(
            payload["metadata_counts"]["xbbg.security_errors"]["property_count_complete"],
            false
        );
        assert!(serde_json::to_vec(&payload).unwrap().len() <= MIN_RESULT_BYTES);
    }

    #[test]
    fn oversized_generic_metadata_collection_is_skipped_without_a_global_scan() {
        let metadata = (0..20)
            .map(|index| (format!("key-{index}"), r#"{"ok":true}"#.to_string()))
            .collect();
        let batch = batch_with_metadata(
            vec![("value", Arc::new(Int32Array::from(vec![1])))],
            metadata,
        );
        let bounded = ResultLimits {
            max_metadata_properties: 100,
            max_metadata_bytes: 64,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert!(payload.get("metadata").is_none());
        assert_eq!(payload["truncation_counts"]["omitted_metadata_entries"], 20);
        assert_eq!(
            payload["truncation_counts"]["inspected_metadata_entries"],
            0
        );
        assert_eq!(
            payload["truncation_counts"]["metadata_property_count_complete"],
            false
        );
        assert_eq!(payload["truncated"]["metadata"], true);
    }

    #[test]
    fn strings_are_utf8_safe_and_bounded_by_chars_and_bytes() {
        let batch = batch_with_metadata(
            vec![("value", Arc::new(StringArray::from(vec!["éééééé"])))],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_string_chars: 10,
            max_string_bytes: 8,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();
        let value = payload["rows"][0]["value"].as_str().unwrap();

        assert_eq!(value, "éé…");
        assert!(value.len() <= bounded.max_string_bytes);
        assert_eq!(payload["truncation_counts"]["truncated_values"], 1);
        assert_eq!(payload["truncation_counts"]["known_omitted_value_bytes"], 8);
    }

    #[test]
    fn ellipsis_respects_a_one_character_limit() {
        let batch = batch_with_metadata(
            vec![("v", Arc::new(StringArray::from(vec!["abcdef"])))],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_string_chars: 1,
            max_string_bytes: 8,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(payload["rows"][0]["v"], "…");
        assert_eq!(payload["rows"][0]["v"].as_str().unwrap().chars().count(), 1);
    }

    #[test]
    fn overlong_field_identity_is_omitted_instead_of_rewritten() {
        let batch = batch_with_metadata(
            vec![("toolong", Arc::new(Int32Array::from(vec![1])))],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_string_chars: 1,
            max_string_bytes: 8,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();

        assert_eq!(payload["returned_columns"], 0);
        assert_eq!(
            payload["truncation_counts"]["known_invalid_identity_columns"],
            1
        );
        assert_eq!(payload["rows"], json!([]));
    }

    #[test]
    fn duplicate_field_names_do_not_overwrite_row_properties() {
        let batch = batch_with_metadata(
            vec![
                ("dup", Arc::new(Int32Array::from(vec![1, 2]))),
                ("dup", Arc::new(Int32Array::from(vec![9, 10]))),
            ],
            HashMap::new(),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(payload["returned_columns"], 1);
        assert_eq!(payload["rows"][0]["dup"], 1);
        assert_eq!(payload["rows"][1]["dup"], 2);
        assert_eq!(payload["truncation_counts"]["known_duplicate_columns"], 1);
        assert_eq!(payload["truncation_counts"]["known_duplicate_cells"], 2);
    }

    #[test]
    fn nested_arrow_values_are_explicitly_omitted_and_offloaded() {
        let values =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1), Some(2)])]);
        let batch = batch_with_metadata(vec![("values", Arc::new(values))], HashMap::new());

        assert!(should_offload(&batch, &limits()));
        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(payload["rows"][0]["values"], "[nested Arrow value omitted]");
        assert_eq!(payload["truncation_counts"]["omitted_complex_values"], 1);
        assert_eq!(payload["truncated"]["values"], true);
    }

    #[test]
    fn fallback_arrow_formatting_stops_at_the_string_budget() {
        let bytes = vec![0xAB; 1_000_000];
        let batch = batch_with_metadata(
            vec![(
                "binary",
                Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()])),
            )],
            HashMap::new(),
        );
        let bounded = ResultLimits {
            max_string_chars: 64,
            max_string_bytes: 64,
            ..limits()
        };

        let payload = record_batch_to_json(&batch, &bounded).unwrap();
        let value = payload["rows"][0]["binary"].as_str().unwrap();

        assert!(value.len() <= bounded.max_string_bytes);
        assert!(value.ends_with('…'));
        assert_eq!(payload["truncation_counts"]["truncated_values"], 1);
    }

    #[test]
    fn offload_preflight_keeps_small_fallback_values_inline() {
        let small = [0xAB];
        let small_batch = batch_with_metadata(
            vec![(
                "binary",
                Arc::new(BinaryArray::from_vec(vec![small.as_slice()])),
            )],
            HashMap::new(),
        );
        let large = vec![0xCD; BLOCKING_STRING_BYTES_THRESHOLD];
        let large_batch = batch_with_metadata(
            vec![(
                "binary",
                Arc::new(BinaryArray::from_vec(vec![large.as_slice()])),
            )],
            HashMap::new(),
        );

        assert!(!should_offload(&small_batch, &limits()));
        assert!(should_offload(&large_batch, &limits()));
    }

    #[test]
    fn eid_metadata_remains_structured_and_has_accurate_counts() {
        let batch = batch_with_metadata(
            Vec::new(),
            HashMap::from([(
                EID_METADATA_KEY.to_string(),
                r#"{"IBM US Equity":[101,202]}"#.to_string(),
            )]),
        );

        let payload = record_batch_to_json(&batch, &limits()).unwrap();

        assert_eq!(
            payload["metadata"][EID_METADATA_KEY]["IBM US Equity"],
            json!([101, 202])
        );
        assert_eq!(payload["truncated"]["metadata"], false);
        assert_eq!(
            payload["metadata_counts"][EID_METADATA_KEY],
            json!({"total_eids": 2, "returned_eids": 2, "total_securities": 1, "returned_securities": 1, "valid": true, "counts_complete": true})
        );
    }

    #[test]
    fn eid_metadata_preserves_entitlement_cap_and_reports_truncation() {
        let raw = json!({
            "A US Equity": (1..=6_000).collect::<Vec<_>>(),
            "B US Equity": (1..=5_000).collect::<Vec<_>>(),
        })
        .to_string();
        let entry = bounded_eid_metadata(
            &raw,
            &limits(),
            limits().max_metadata_properties,
            limits().max_metadata_bytes,
        )
        .unwrap();
        let counts = entry.counts.unwrap();

        assert_eq!(entry.value["A US Equity"].as_array().unwrap().len(), 6_000);
        assert_eq!(entry.value["B US Equity"].as_array().unwrap().len(), 4_000);
        assert_eq!(counts["total_eids"], 11_000);
        assert_eq!(counts["returned_eids"], MAX_ENTITLEMENT_EIDS);
        assert!(entry.special_truncated);
    }

    #[test]
    fn invalid_eid_metadata_stays_structured_and_explicit() {
        for malformed in [
            "{not json",
            r#"{"IBM US Equity":{"nested":[101]}}"#,
            r#"{"IBM US Equity":[101,"202"]}"#,
            r#"{"IBM US Equity":[0]}"#,
            r#"[101,202]"#,
        ] {
            let entry = bounded_eid_metadata(
                malformed,
                &limits(),
                limits().max_metadata_properties,
                limits().max_metadata_bytes,
            )
            .unwrap();
            assert_eq!(entry.value, json!({"invalid": true}));
            let counts = entry.counts.unwrap();
            assert_eq!(counts["valid"], false);
            assert_eq!(counts["counts_complete"], false);
            assert_eq!(counts["total_eids"], Value::Null);
            assert_eq!(counts["total_securities"], Value::Null);
            assert!(entry.special_truncated);
        }
    }

    #[test]
    fn entitlement_result_prioritizes_failures_and_respects_total_budget() {
        let bounded = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            ..limits()
        };
        let payload = entitlement_check_to_json(
            "//blp/refdata".to_string(),
            (1..=1_000).collect(),
            EntitlementCheck {
                entitled: false,
                failed_eids: (2_001..=3_000).collect(),
            },
            &bounded,
        )
        .unwrap();
        let encoded = serde_json::to_vec(&payload).unwrap();

        assert!(encoded.len() <= bounded.max_result_bytes);
        assert_eq!(payload["total_eids"], 1_000);
        assert_eq!(payload["total_failed_eids"], 1_000);
        assert!(payload["returned_failed_eids"].as_u64().unwrap() > 0);
        assert_eq!(payload["truncated"]["output"], true);
        assert_eq!(
            payload["returned_failed_eids"].as_u64().unwrap()
                + payload["omitted_failed_eids"].as_u64().unwrap(),
            1_000
        );
    }
}
