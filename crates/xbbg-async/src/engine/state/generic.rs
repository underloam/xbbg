//! Generic flattener state for arbitrary Bloomberg responses.
//!
//! Flattens any Bloomberg response into a normalized table with columns:
//! - path: Element path (e.g., "securityData[0].fieldData.PX_LAST")
//! - type: Value type (string, number, boolean, null, array, object)
//! - value_str: String representation of value
//! - value_num: Numeric value (if applicable)
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use tokio::sync::oneshot;

use super::value_utils::{format_date32, format_timestamp_micros, top_level_response_error};
use xbbg_core::{BlpError, DataType, Element, Message, Value};

/// State for a generic request that flattens elements to tabular format.
pub struct GenericState {
    /// Element path builder
    path_builder: StringBuilder,
    /// Value type builder
    type_builder: StringBuilder,
    /// String value builder
    value_str_builder: StringBuilder,
    /// Numeric value builder
    value_num_builder: Float64Builder,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl GenericState {
    /// Create a new generic state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            path_builder: StringBuilder::new(),
            type_builder: StringBuilder::new(),
            value_str_builder: StringBuilder::new(),
            value_num_builder: Float64Builder::new(),
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "<generic>", "GenericRequest") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);

        let result = self.build_batch();
        if let Ok(ref batch) = result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "generic finish"
            );
        }
        let _ = self.reply.send(result);
    }

    /// Process a message by flattening its elements to rows.
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();
        self.flatten_element("", &root);
    }

    /// Recursively flatten an element into rows.
    fn flatten_element(&mut self, path: &str, elem: &Element<'_>) {
        match elem.datatype() {
            DataType::Sequence => {
                // Check if this sequence is an array (iterate by index)
                // vs a struct with named children (iterate by children)
                if elem.is_array() {
                    // Array of elements - iterate by index using len()
                    let n = elem.len();
                    for i in 0..n {
                        if let Some(item) = elem.get_element(i) {
                            let item_path = format!("{}[{}]", path, i);
                            self.flatten_element(&item_path, &item);
                        }
                    }
                } else {
                    // Sequence with named children - iterate over children
                    for child in elem.children() {
                        let child_name = child.name_str();
                        let child_path = if path.is_empty() {
                            child_name.to_string()
                        } else {
                            format!("{}.{}", path, child_name)
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            }
            DataType::Choice => {
                // Choice - single selected child
                for child in elem.children() {
                    let child_name = child.name_str();
                    let child_path = if path.is_empty() {
                        child_name.to_string()
                    } else {
                        format!("{}.{}", path, child_name)
                    };
                    self.flatten_element(&child_path, &child);
                }
            }
            _ => {
                // Leaf value - extract and record
                if elem.is_null() {
                    self.append_row(path, "null", None, None);
                    return;
                }

                // Try to get the value at index 0
                if let Some(value) = elem.get_value(0) {
                    match value {
                        Value::Null => {
                            self.append_row(path, "null", None, None);
                        }
                        Value::Bool(b) => {
                            self.append_row(path, "boolean", Some(&b.to_string()), None);
                        }
                        Value::Int32(i) => {
                            self.append_row(path, "number", Some(&i.to_string()), Some(i as f64));
                        }
                        Value::Int64(i) => {
                            self.append_row(path, "number", Some(&i.to_string()), Some(i as f64));
                        }
                        Value::Float64(f) => {
                            self.append_row(path, "number", Some(&f.to_string()), Some(f));
                        }
                        Value::String(s) => {
                            self.append_row(path, "string", Some(s), None);
                        }
                        Value::Enum(s) => {
                            self.append_row(path, "string", Some(s), None);
                        }
                        Value::Date32(days) => {
                            let date_str = format_date32(days);
                            self.append_row(path, "date", Some(&date_str), Some(days as f64));
                        }
                        Value::TimestampMicros(micros) => {
                            let dt_str = format_timestamp_micros(micros);
                            self.append_row(path, "datetime", Some(&dt_str), Some(micros as f64));
                        }
                        Value::Datetime(dt) => {
                            let micros = dt.to_micros();
                            let dt_str = format_timestamp_micros(micros);
                            self.append_row(path, "datetime", Some(&dt_str), Some(micros as f64));
                        }
                        Value::Byte(b) => {
                            self.append_row(path, "number", Some(&b.to_string()), Some(b as f64));
                        }
                        Value::Time64Micros(micros) => {
                            let t = micros / 1_000_000;
                            let time_str =
                                format!("{:02}:{:02}:{:02}", t / 3600, (t % 3600) / 60, t % 60);
                            self.append_row(path, "time", Some(&time_str), Some(micros as f64));
                        }
                    }
                } else {
                    // Could not extract value
                    self.append_row(path, "null", None, None);
                }
            }
        }
    }

    /// Append a single row to the builders.
    fn append_row(
        &mut self,
        path: &str,
        value_type: &str,
        value_str: Option<&str>,
        value_num: Option<f64>,
    ) {
        self.path_builder.append_value(path);
        self.type_builder.append_value(value_type);

        match value_str {
            Some(s) => self.value_str_builder.append_value(s),
            None => self.value_str_builder.append_null(),
        }

        match value_num {
            Some(n) => self.value_num_builder.append_value(n),
            None => self.value_num_builder.append_null(),
        }
    }

    /// Build the final RecordBatch.
    fn build_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let path_array = self.path_builder.finish();
        let type_array = self.type_builder.finish();
        let value_str_array = self.value_str_builder.finish();
        let value_num_array = self.value_num_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("value_str", ArrowDataType::Utf8, true),
            Field::new("value_num", ArrowDataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(path_array),
                Arc::new(type_array),
                Arc::new(value_str_array),
                Arc::new(value_num_array),
            ],
        )
        .map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }
}
