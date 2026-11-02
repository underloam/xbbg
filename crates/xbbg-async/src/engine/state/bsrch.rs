//! Bloomberg SRCH / Excel `GridResponse` state with Arrow builders.
//!
//! `ExcelGetGridRequest` responses from `//blp/exrsvc` are not BEQS-shaped.
//! Bloomberg returns a `GridResponse` message whose root elements are:
//!
//! ```text
//! GridResponse {
//!   NumOfFields (0 means Bloomberg did not report the field count)
//!   NumOfRecords
//!   ColumnTitles[]
//!   DataRecords[] {
//!     DataFields[] {
//!       StringValue | IntValue | LongValue | FloatValue | DoubleValue
//!       DateValue | TimeValue | DateTimeValue
//!     }
//!   }
//!   ReachMax
//!   Error
//!   SequenceNumber
//! }
//! ```

use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use tokio::sync::oneshot;
use xbbg_core::{BlpError, Element, Message, Name, Value};
use xbbg_log::debug;

use super::typed_builder::{ColumnSet, ColumnSlot};
use super::value_utils::{get_value_with_datatype, top_level_response_error};

struct GridNames {
    num_of_records: Name,
    num_of_fields: Name,
    reach_max: Name,
    sequence_number: Name,
    error: Name,
    column_titles: Name,
    data_records: Name,
    data_fields: Name,
}

static GRID_NAMES: LazyLock<GridNames> = LazyLock::new(|| GridNames {
    num_of_records: Name::get_or_intern("NumOfRecords"),
    num_of_fields: Name::get_or_intern("NumOfFields"),
    reach_max: Name::get_or_intern("ReachMax"),
    sequence_number: Name::get_or_intern("SequenceNumber"),
    error: Name::get_or_intern("Error"),
    column_titles: Name::get_or_intern("ColumnTitles"),
    data_records: Name::get_or_intern("DataRecords"),
    data_fields: Name::get_or_intern("DataFields"),
});

#[derive(Default)]
struct ProcessedGridPage {
    num_records: Option<usize>,
    num_fields: Option<usize>,
    reach_max: bool,
    sequence_number: Option<i32>,
    error: Option<String>,
}

/// State for a Bloomberg SRCH / Excel grid request.
pub struct BsrchState {
    /// Column set for building the output.
    columns: ColumnSet,
    /// Column names from `ColumnTitles[]`, or fallback `colN` names.
    column_names: Vec<String>,
    /// Column-set slots parallel to `column_names`.
    column_slots: Vec<ColumnSlot>,
    /// Bloomberg `Error` payload, if one was returned.
    error: Option<String>,
    /// Bloomberg reported `NumOfRecords` total across processed pages.
    expected_records: Option<usize>,
    /// Bloomberg reported positive `NumOfFields`, if present.
    expected_fields: Option<usize>,
    /// Reply channel.
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl BsrchState {
    /// Create a new BSRCH state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            columns: ColumnSet::new(),
            column_names: Vec::new(),
            column_slots: Vec::new(),
            error: None,
            expected_records: None,
            expected_fields: None,
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        let page = self.process_message(msg);
        self.merge_page(page);
    }

    /// Process a final RESPONSE message.
    pub fn on_response(&mut self, msg: &Message) {
        let page = self.process_message(msg);
        self.merge_page(page);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/exrsvc", "ExcelGetGridRequest") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.on_response(msg);
        self.finish_processed();
    }

    /// Send the accumulated result via reply channel.
    pub fn finish_processed(self) {
        let BsrchState {
            columns,
            column_names,
            error,
            expected_records,
            reply,
            ..
        } = self;
        let result = Self::into_record_batch(columns, column_names, error, expected_records);
        if let Ok(ref batch) = result {
            debug!(rows = batch.num_rows(), "bsrch finish");
        }
        let _ = reply.send(result);
    }

    fn into_record_batch(
        columns: ColumnSet,
        column_names: Vec<String>,
        error: Option<String>,
        expected_records: Option<usize>,
    ) -> Result<RecordBatch, BlpError> {
        if let Some(error) = error {
            return Err(BlpError::InvalidArgument {
                detail: format!("bsrch error: {error}"),
            });
        }

        if let Some(expected) = expected_records {
            let actual = columns.row_count();
            if actual != expected {
                return Err(BlpError::Internal {
                    detail: format!(
                        "bsrch parsed {actual} rows but Bloomberg reported NumOfRecords={expected}"
                    ),
                });
            }
        }

        if !column_names.is_empty() {
            let order: Vec<&str> = column_names.iter().map(String::as_str).collect();
            return columns.finish_with_order(&order);
        }

        if columns.column_count() == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        columns.finish()
    }

    /// Process a BSRCH `GridResponse` message using the Element API.
    fn process_message(&mut self, msg: &Message) -> ProcessedGridPage {
        let names = &*GRID_NAMES;
        let root = msg.elements();
        let mut page = ProcessedGridPage {
            num_records: element_i32_usize(&root, &names.num_of_records),
            num_fields: element_reported_num_fields(&root, &names.num_of_fields),
            reach_max: root
                .get(&names.reach_max)
                .and_then(|value| value.get_bool(0))
                .unwrap_or(false),
            sequence_number: root
                .get(&names.sequence_number)
                .and_then(|value| value.get_i32(0)),
            ..ProcessedGridPage::default()
        };

        if let Some(error) = child_by_name(&root, &names.error).and_then(element_string) {
            page.error = Some(error);
        }

        self.read_column_titles(&root);
        if let Some(num_fields) = page.num_fields {
            if self.column_names.len() > num_fields {
                page.error.get_or_insert_with(|| {
                    format!(
                        "GridResponse ColumnTitles has {} columns but NumOfFields={num_fields}",
                        self.column_names.len()
                    )
                });
            } else {
                self.ensure_column_width(num_fields);
            }
        }

        let Some(records) = root.get(&names.data_records) else {
            return page;
        };

        for record in records.values() {
            let expected_width = expected_record_width(
                page.num_fields,
                self.expected_fields,
                self.column_names.len(),
            );

            let Some(fields) = record.get(&names.data_fields) else {
                if let Some(width) = expected_width.filter(|width| *width > 0) {
                    self.append_null_record(width);
                } else {
                    page.error.get_or_insert_with(|| {
                        "GridResponse DataRecords entry missing DataFields".to_string()
                    });
                }
                continue;
            };

            self.append_record(&fields, expected_width, &mut page.error);
        }

        page
    }
    fn merge_page(&mut self, page: ProcessedGridPage) {
        let ProcessedGridPage {
            num_records,
            num_fields,
            reach_max,
            sequence_number,
            error,
        } = page;

        if let Some(error) = error {
            self.error.get_or_insert(error);
        }

        if let Some(num_fields) = num_fields {
            match self.expected_fields {
                Some(expected) if expected != num_fields => {
                    self.error.get_or_insert_with(|| {
                        format!("GridResponse NumOfFields changed from {expected} to {num_fields}")
                    });
                }
                None => self.expected_fields = Some(num_fields),
                Some(_) => {}
            }
        }

        if let Some(num_records) = num_records {
            let total = self
                .expected_records
                .unwrap_or(0)
                .saturating_add(num_records);
            self.expected_records = Some(total);
        }

        if reach_max && self.error.is_none() && self.columns.row_count() > 0 {
            debug!(
                sequence_number = ?sequence_number,
                rows = self.columns.row_count(),
                "bsrch GridResponse ReachMax returned; current page retained"
            );
        }
    }

    fn read_column_titles(&mut self, root: &Element<'_>) {
        if !self.column_names.is_empty() {
            return;
        }

        let Some(titles) = root.get(&GRID_NAMES.column_titles) else {
            return;
        };

        for idx in 0..titles.len() {
            let title = titles.get_str(idx);
            self.push_column_name(title, idx);
        }
    }

    fn append_record(
        &mut self,
        fields: &Element<'_>,
        expected_width: Option<usize>,
        error: &mut Option<String>,
    ) {
        if let Some(expected_width) = expected_width {
            let actual_width = fields.len();
            if actual_width > expected_width {
                error.get_or_insert_with(|| {
                    format!(
                        "GridResponse DataFields has {actual_width} fields but NumOfFields={expected_width}"
                    )
                });
            }
        }

        let width = expected_width
            .unwrap_or(0)
            .max(fields.len())
            .max(self.column_names.len());
        self.ensure_column_width(width);

        for (idx, &slot) in self.column_slots[..width].iter().enumerate() {
            if let Some(field) = fields.get_element(idx)
                && append_grid_field_value(&mut self.columns, slot, field)
            {
                continue;
            }

            self.columns.append_null_slot(slot);
        }

        self.columns.end_row();
    }

    fn append_null_record(&mut self, width: usize) {
        self.ensure_column_width(width);
        for &slot in &self.column_slots[..width] {
            self.columns.append_null_slot(slot);
        }
        self.columns.end_row();
    }

    fn ensure_column_width(&mut self, width: usize) {
        while self.column_names.len() < width {
            let idx = self.column_names.len();
            self.push_column_name(None, idx);
        }
    }

    fn push_column_name(&mut self, raw: Option<&str>, idx: usize) {
        let base = raw
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("col{idx}"));
        let name = self.unique_column_name(base);
        self.column_slots.push(self.columns.slot(&name));
        self.column_names.push(name);
    }

    fn unique_column_name(&self, base: String) -> String {
        if !self.column_names.iter().any(|name| name == &base) {
            return base;
        }

        let mut suffix = 2;
        loop {
            let candidate = format!("{base}_{suffix}");
            if !self.column_names.iter().any(|name| name == &candidate) {
                return candidate;
            }
            suffix += 1;
        }
    }
}

fn append_grid_field_value(columns: &mut ColumnSet, slot: ColumnSlot, field: Element<'_>) -> bool {
    for child in field.children() {
        if let Some(value) = get_value_with_datatype(&child, child.datatype()) {
            columns.append_slot(slot, value);
            return true;
        }
    }

    if let Some(value) = get_value_with_datatype(&field, field.datatype()) {
        columns.append_slot(slot, value);
        return true;
    }

    false
}

fn child_by_name<'a>(element: &'a Element<'a>, name: &Name) -> Option<Element<'a>> {
    if let Some(child) = element.get(name) {
        return Some(child);
    }

    let name = name.as_str();
    element
        .children()
        .find(|child| child.name_str().eq_ignore_ascii_case(name))
}

fn element_i32_usize(element: &Element<'_>, name: &Name) -> Option<usize> {
    let value = child_by_name(element, name)?.get_i32(0)?;
    usize::try_from(value).ok()
}

fn element_reported_num_fields(element: &Element<'_>, name: &Name) -> Option<usize> {
    reported_num_fields(element_i32_usize(element, name))
}

fn reported_num_fields(value: Option<usize>) -> Option<usize> {
    value.filter(|value| *value > 0)
}

fn expected_record_width(
    page_fields: Option<usize>,
    expected_fields: Option<usize>,
    column_name_count: usize,
) -> Option<usize> {
    page_fields
        .or(expected_fields)
        .or_else(|| (column_name_count > 0).then_some(column_name_count))
}

fn element_string(element: Element<'_>) -> Option<String> {
    let value = if let Some(value) = element.get_value(0) {
        match value {
            Value::String(value) | Value::Enum(value) => value,
            _ => element.get_str(0)?,
        }
    } else {
        element.get_str(0)?
    };

    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::{expected_record_width, reported_num_fields};

    #[test]
    fn reported_num_fields_treats_zero_as_unreported() {
        assert_eq!(reported_num_fields(Some(1)), Some(1));
        assert_eq!(reported_num_fields(Some(0)), None);
        assert_eq!(reported_num_fields(None), None);
    }

    #[test]
    fn expected_record_width_uses_column_titles_when_num_fields_is_zero() {
        assert_eq!(
            expected_record_width(reported_num_fields(Some(0)), None, 1),
            Some(1)
        );
    }

    #[test]
    fn expected_record_width_prefers_positive_num_fields() {
        assert_eq!(
            expected_record_width(reported_num_fields(Some(2)), None, 1),
            Some(2)
        );
    }
}
