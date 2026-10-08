//! Borrowed adapters for the two JSON trees and their shared Arrow materializer.
//!
//! Adapters only expose response structure. Ordering, diagnostics, row selection,
//! deduplication, and cell conversion live here, without copying the parsed cells.

use std::{borrow::Cow, collections::HashSet, sync::Arc};

use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value as JsonValue;
use xbbg_core::BlpError;

use super::{BqlCell, BqlException, BqlJsonColumn, BqlJsonField, BqlJsonResponse, BqlNumber};

pub(super) fn from_typed(response: &BqlJsonResponse<'_>) -> Result<RecordBatch, BlpError> {
    materialize(
        response
            .client_context
            .as_ref()
            .and_then(|context| context.client_request_id.as_deref()),
        response
            .response_exceptions
            .iter()
            .flatten()
            .map(ExceptionView::from),
        response
            .results
            .iter()
            .flat_map(|results| results.iter())
            .map(|(name, field)| (name.as_str(), field)),
    )
}

pub(super) fn from_value(response: &JsonValue) -> Result<RecordBatch, BlpError> {
    let results = match response.get("results") {
        Some(JsonValue::Object(results)) => Some(results),
        Some(JsonValue::Null) | None => None,
        Some(other) => {
            return Err(BlpError::Internal {
                detail: format!("BQL 'results' has unexpected type: {other}"),
            });
        }
    };

    materialize(
        response
            .get("clientContext")
            .and_then(|context| context.get("clientRequestId"))
            .and_then(JsonValue::as_str),
        json_exceptions(response),
        results
            .into_iter()
            .flat_map(|results| results.iter())
            .map(|(name, field)| (name.as_str(), field)),
    )
}

struct ExceptionView<'a> {
    message: Option<&'a str>,
    node_name: Option<&'a str>,
}

impl<'a> From<&'a BqlException<'_>> for ExceptionView<'a> {
    fn from(exception: &'a BqlException<'_>) -> Self {
        Self {
            message: exception.message.as_deref(),
            node_name: exception.node_name.as_deref(),
        }
    }
}

impl<'a> From<&'a JsonValue> for ExceptionView<'a> {
    fn from(exception: &'a JsonValue) -> Self {
        Self {
            message: exception.get("message").and_then(JsonValue::as_str),
            node_name: exception.get("nodeName").and_then(JsonValue::as_str),
        }
    }
}

fn json_exceptions(response: &JsonValue) -> impl Iterator<Item = ExceptionView<'_>> {
    response
        .get("responseExceptions")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .map(ExceptionView::from)
}

struct ColumnView<'a, C> {
    name: Option<&'a str>,
    type_hint: Option<&'a str>,
    values: &'a [C],
}

impl<C> Default for ColumnView<'_, C> {
    fn default() -> Self {
        Self {
            name: None,
            type_hint: None,
            values: &[],
        }
    }
}

impl<'a, 'json> From<&'a BqlJsonColumn<'json>> for ColumnView<'a, BqlCell<'json>> {
    fn from(column: &'a BqlJsonColumn<'json>) -> Self {
        Self {
            name: column.name.as_deref(),
            type_hint: column.data_type.as_deref(),
            values: &column.values,
        }
    }
}

impl<'a> From<&'a JsonValue> for ColumnView<'a, JsonValue> {
    fn from(column: &'a JsonValue) -> Self {
        Self {
            name: column.get("name").and_then(JsonValue::as_str),
            type_hint: column.get("type").and_then(JsonValue::as_str),
            values: column
                .get("values")
                .and_then(JsonValue::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        }
    }
}

trait ResultField {
    type Cell: CellValue;

    fn id_values(&self) -> &[Self::Cell];
    fn value_column(&self) -> ColumnView<'_, Self::Cell>;
    fn secondary_columns(&self) -> impl Iterator<Item = ColumnView<'_, Self::Cell>>;
    fn exceptions(&self) -> impl Iterator<Item = ExceptionView<'_>>;
}

impl<'json> ResultField for BqlJsonField<'json> {
    type Cell = BqlCell<'json>;

    fn id_values(&self) -> &[Self::Cell] {
        self.id_column
            .as_ref()
            .map(|column| column.values.as_slice())
            .unwrap_or_default()
    }

    fn value_column(&self) -> ColumnView<'_, Self::Cell> {
        self.values_column
            .as_ref()
            .map(ColumnView::from)
            .unwrap_or_default()
    }

    fn secondary_columns(&self) -> impl Iterator<Item = ColumnView<'_, Self::Cell>> {
        self.secondary_columns.iter().map(ColumnView::from)
    }

    fn exceptions(&self) -> impl Iterator<Item = ExceptionView<'_>> {
        self.response_exceptions
            .iter()
            .flatten()
            .map(ExceptionView::from)
    }
}

impl ResultField for JsonValue {
    type Cell = JsonValue;

    fn id_values(&self) -> &[Self::Cell] {
        self.get("idColumn")
            .and_then(|column| column.get("values"))
            .and_then(JsonValue::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn value_column(&self) -> ColumnView<'_, Self::Cell> {
        self.get("valuesColumn")
            .map(ColumnView::from)
            .unwrap_or_default()
    }

    fn secondary_columns(&self) -> impl Iterator<Item = ColumnView<'_, Self::Cell>> {
        self.get("secondaryColumns")
            .and_then(JsonValue::as_array)
            .into_iter()
            .flatten()
            .map(ColumnView::from)
    }

    fn exceptions(&self) -> impl Iterator<Item = ExceptionView<'_>> {
        json_exceptions(self)
    }
}

fn materialize<'a, F>(
    request_id: Option<&str>,
    exceptions: impl Iterator<Item = ExceptionView<'a>>,
    results: impl Iterator<Item = (&'a str, &'a F)> + Clone,
) -> Result<RecordBatch, BlpError>
where
    F: ResultField + 'a,
    F::Cell: 'a,
{
    let top_exceptions = exception_messages(exceptions);
    let mut results = results.peekable();
    if results.peek().is_none() {
        return match top_exceptions {
            Some(source) => Err(BlpError::RequestFailure {
                service: "//blp/bqlsvc".into(),
                operation: Some("sendQuery".into()),
                cid: None,
                label: None,
                request_id: request_id.map(str::to_owned),
                source: Some(source.into()),
            }),
            None => empty_batch(),
        };
    }

    if let Some(exceptions) = top_exceptions {
        xbbg_log::warn!(
            exceptions = exceptions.as_str(),
            "BQL response has partial exceptions but results are present"
        );
    }

    // The first nonempty ID column fixes the row count for every output column.
    // Cloning the iterator borrows the same tree; it does not clone field data.
    let id_values = results
        .clone()
        .map(|(_, field)| field.id_values())
        .find(|values| !values.is_empty())
        .unwrap_or_default();
    let row_count = id_values.len();
    let mut id_builder = string_builder(row_count);
    for value in id_values {
        value.view().append_as_id(&mut id_builder);
    }

    // "ticker" avoids conflicting with a user-requested primary "id" field.
    let mut fields = vec![Field::new("ticker", DataType::Utf8, true)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(id_builder.finish())];
    let mut column_names: HashSet<Cow<'a, str>> = HashSet::new();
    for (field_name, field) in results {
        // Both maps use sorted key order; serde_json's `preserve_order` feature
        // must stay disabled. Secondary columns precede their primary field,
        // and the first occurrence of each lowercase name wins.
        for column in field.secondary_columns() {
            let Some(name) = column.name else {
                continue;
            };
            let name = name.to_lowercase();
            if column_names.contains(name.as_str()) {
                continue;
            }
            append_column(&name, column, row_count, &mut fields, &mut arrays);
            column_names.insert(Cow::Owned(name));
        }

        if let Some(exceptions) = exception_messages(field.exceptions()) {
            xbbg_log::warn!(
                field = field_name,
                exceptions = exceptions.as_str(),
                "BQL field has partial errors"
            );
        }

        append_column(
            field_name,
            field.value_column(),
            row_count,
            &mut fields,
            &mut arrays,
        );
        column_names.insert(Cow::Borrowed(field_name));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| BlpError::Internal {
        detail: format!("Failed to create RecordBatch: {}", e),
    })
}

/// Preserve an empty message as an exception, but ignore entries without one.
fn exception_messages<'a>(exceptions: impl Iterator<Item = ExceptionView<'a>>) -> Option<String> {
    let mut messages: Option<String> = None;
    for exception in exceptions {
        let Some(message) = exception.message else {
            continue;
        };
        let output = if let Some(messages) = &mut messages {
            messages.push_str("; ");
            messages
        } else {
            messages.insert(String::new())
        };
        output.push_str(message);
        if let Some(node) = exception.node_name {
            output.push_str(" (in ");
            output.push_str(node);
            output.push(')');
        }
    }
    messages
}

enum CellView<'a> {
    String(&'a str),
    Number(BqlNumber),
    Bool(bool),
    Null,
    Other(&'a JsonValue),
}

trait CellValue {
    fn view(&self) -> CellView<'_>;
}

impl CellValue for BqlCell<'_> {
    fn view(&self) -> CellView<'_> {
        match self {
            Self::String(value) => CellView::String(value.as_ref()),
            Self::Number(value) => CellView::Number(*value),
            Self::Bool(value) => CellView::Bool(*value),
            Self::Null => CellView::Null,
            Self::Other(value) => CellView::Other(value.as_ref()),
        }
    }
}

impl CellValue for JsonValue {
    fn view(&self) -> CellView<'_> {
        match self {
            Self::String(value) => CellView::String(value),
            Self::Number(value) => CellView::Number(BqlNumber::from_json(value)),
            Self::Bool(value) => CellView::Bool(*value),
            Self::Null => CellView::Null,
            other => CellView::Other(other),
        }
    }
}

impl CellView<'_> {
    fn append_as_string(self, builder: &mut StringBuilder) {
        match self {
            Self::String(value) => builder.append_value(value),
            Self::Number(number) => number.append_as_string(builder),
            Self::Bool(value) => builder.append_value(if value { "true" } else { "false" }),
            Self::Null => builder.append_null(),
            Self::Other(value) => builder.append_value(value.to_string()),
        }
    }

    fn append_as_id(self, builder: &mut StringBuilder) {
        match self {
            Self::Null => builder.append_value(""),
            other => other.append_as_string(builder),
        }
    }
}

impl BqlNumber {
    fn from_json(value: &serde_json::Number) -> Self {
        if let Some(value) = value.as_i64() {
            Self::Signed(value)
        } else if let Some(value) = value.as_u64() {
            Self::Unsigned(value)
        } else {
            Self::Float(value.as_f64().unwrap_or(f64::NAN))
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Signed(value) => value as f64,
            Self::Unsigned(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    fn append_as_string(self, builder: &mut StringBuilder) {
        match self {
            Self::Signed(value) => {
                let mut buffer = itoa::Buffer::new();
                builder.append_value(buffer.format(value));
            }
            Self::Unsigned(value) => {
                let mut buffer = itoa::Buffer::new();
                builder.append_value(buffer.format(value));
            }
            Self::Float(value) => builder.append_value(value.to_string()),
        }
    }
}

enum ColumnKind {
    Numeric,
    String,
    Infer,
}

fn column_kind(type_hint: Option<&str>) -> ColumnKind {
    match type_hint {
        Some(t)
            if t.eq_ignore_ascii_case("DOUBLE")
                || t.eq_ignore_ascii_case("FLOAT")
                || t.eq_ignore_ascii_case("INT32")
                || t.eq_ignore_ascii_case("INT64")
                || t.eq_ignore_ascii_case("INTEGER") =>
        {
            ColumnKind::Numeric
        }
        Some(t)
            if t.eq_ignore_ascii_case("STRING")
                || t.eq_ignore_ascii_case("DATE")
                || t.eq_ignore_ascii_case("DATETIME") =>
        {
            ColumnKind::String
        }
        _ => ColumnKind::Infer,
    }
}

fn append_column<C: CellValue>(
    name: &str,
    column: ColumnView<'_, C>,
    row_count: usize,
    fields: &mut Vec<Field>,
    arrays: &mut Vec<ArrayRef>,
) {
    let numeric = match column_kind(column.type_hint) {
        ColumnKind::Numeric => true,
        ColumnKind::String => false,
        // Inspect only emitted rows and borrow them again when building Arrow.
        // This preserves integer text on promotion without a numeric-cell copy.
        ColumnKind::Infer => column
            .values
            .iter()
            .take(row_count)
            .all(|value| matches!(value.view(), CellView::Number(_) | CellView::Null)),
    };

    if numeric {
        let mut builder = float_builder(row_count);
        for row_idx in 0..row_count {
            let value = match column.values.get(row_idx).map(CellValue::view) {
                Some(CellView::Number(number)) => Some(number.as_f64()),
                Some(CellView::String(value)) => value.parse::<f64>().ok(),
                _ => None,
            };
            builder.append_option(value);
        }
        fields.push(Field::new(name, DataType::Float64, true));
        arrays.push(Arc::new(builder.finish()));
    } else {
        let mut builder = string_builder(row_count);
        for row_idx in 0..row_count {
            match column.values.get(row_idx) {
                Some(value) => value.view().append_as_string(&mut builder),
                None => builder.append_null(),
            }
        }
        fields.push(Field::new(name, DataType::Utf8, true));
        arrays.push(Arc::new(builder.finish()));
    }
}

fn string_builder(row_count: usize) -> StringBuilder {
    if row_count <= 1 {
        StringBuilder::new()
    } else {
        StringBuilder::with_capacity(row_count, row_count.saturating_mul(16).max(1))
    }
}

fn float_builder(row_count: usize) -> Float64Builder {
    if row_count <= 1 {
        Float64Builder::new()
    } else {
        Float64Builder::with_capacity(row_count)
    }
}

fn empty_batch() -> Result<RecordBatch, BlpError> {
    let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, true)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
    )
    .map_err(|e| BlpError::Internal {
        detail: format!("Failed to create empty batch: {}", e),
    })
}
