//! Bulk data (bds) state with Arrow builders.
//!
//! Extracts BulkDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use arrow_array::RecordBatch;
use std::collections::HashMap;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::typed_builder::ColumnSet;
use super::value_utils::{
    ResponseMetadata, arrow_type_for_element, should_emit_scalar_field, top_level_response_error,
};
use xbbg_core::{BlpError, Element, Message};

/// State for a bulk data request (bds).
pub struct BulkDataState {
    /// Field name as string (the bulk field to extract)
    field_name: String,
    /// Column set for building the output.
    columns: ColumnSet,
    /// Discovered scalar sub-field names, in first-seen order across all rows.
    subfield_names: Vec<String>,
    /// Interned-name key to `subfield_names` index for allocation-free row decode.
    subfield_index_by_key: HashMap<usize, usize>,
    /// Per-row scratch bitmap for fields seen while walking one bulk row.
    seen_subfields: Vec<bool>,
    /// Response-level diagnostics (eidData / securityError / fieldExceptions) attached to the
    /// result batch as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl BulkDataState {
    /// Create a new bulkdata state.
    pub fn new(field: String, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            field_name: field,
            columns: ColumnSet::new(),
            subfield_names: Vec::new(),
            subfield_index_by_key: HashMap::new(),
            seen_subfields: Vec::new(),
            response_meta: ResponseMetadata::default(),
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "ReferenceDataRequest")
        {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);
        let reply = self.reply;
        let response_meta = std::mem::take(&mut self.response_meta);
        // Include "field" column to identify which bulk field was queried
        let mut order = vec!["ticker", "field"];
        order.extend(self.subfield_names.iter().map(|s| s.as_str()));
        let result = self
            .columns
            .finish_with_order(&order)
            .map(|batch| response_meta.attach(batch));
        if let Ok(batch) = &result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "bulkdata finish"
            );
        }
        let _ = reply.send(result);
    }

    /// Process a BulkDataResponse message using Element API.
    ///
    /// Bloomberg structure (for bds - similar to refdata but with array fields):
    /// ```text
    /// ReferenceDataResponse {
    ///   securityData[] {
    ///     security: "AAPL US Equity"
    ///     fieldData {
    ///       DVD_HIST[] {           // <-- bulk field is an array
    ///         Declared Date: "2024-01-15"
    ///         Amount: 0.24
    ///         ...
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Get securityData array
        let Some(security_data) = root.get_by_str("securityData") else {
            trace!("No securityData in message");
            return;
        };

        // Iterate through each security
        let n = security_data.len();
        for i in 0..n {
            let Some(sec) = security_data.get_element(i) else {
                continue;
            };

            // Get ticker
            let ticker = sec
                .get_by_str("security")
                .and_then(|e| e.get_str(0))
                .unwrap_or("");

            let diagnostics = self.response_meta.record_security(ticker, &sec);
            if let Some(error) = diagnostics.security_error {
                xbbg_log::warn!(
                    ticker = ticker,
                    category = error.category,
                    code = error.code,
                    message = error.message,
                    "BulkData securityError; skipping security"
                );
                continue;
            }

            // Get fieldData
            let Some(field_data) = sec.get_by_str("fieldData") else {
                trace!(ticker = ticker, "No fieldData for security");
                continue;
            };

            // Get the bulk field (which is an array)
            let Some(bulk_field) = field_data.get_by_str(&self.field_name) else {
                trace!(ticker = ticker, field = %self.field_name, "Bulk field not found");
                continue;
            };

            // Iterate through the array of rows
            let row_count = bulk_field.len();
            for j in 0..row_count {
                let Some(row) = bulk_field.get_element(j) else {
                    continue;
                };

                self.columns.append_str("ticker", ticker);
                self.columns.append_str("field", &self.field_name);

                self.append_row_subfields(&row);

                self.columns.end_row();
            }
        }
    }

    fn append_row_subfields(&mut self, row: &Element<'_>) {
        self.seen_subfields.fill(false);

        for child in row.children() {
            if !should_emit_scalar_field(&child) {
                continue;
            }

            let idx = self.resolve_subfield_index(&child);
            if self.seen_subfields[idx] {
                continue;
            }
            self.seen_subfields[idx] = true;

            let name = self.subfield_names[idx].as_str();
            if let Some(value) = child.get_value(0) {
                self.columns.append(name, value);
            } else {
                self.columns.append_null(name);
            }
        }

        for (idx, name) in self.subfield_names.iter().enumerate() {
            if !self.seen_subfields[idx] {
                self.columns.append_null(name);
            }
        }
    }

    fn resolve_subfield_index(&mut self, child: &Element<'_>) -> usize {
        let key = child.name_key();
        if let Some(&idx) = self.subfield_index_by_key.get(&key) {
            return idx;
        }

        let name = child.name_str().to_string();
        let idx = self.subfield_names.len();
        self.columns
            .set_type_hint(&name, arrow_type_for_element(child));
        self.subfield_names.push(name);
        self.subfield_index_by_key.insert(key, idx);
        self.seen_subfields.push(false);
        idx
    }
}
