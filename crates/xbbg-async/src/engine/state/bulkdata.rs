//! Bulk data (bds) state with Arrow builders.
//!
//! Extracts BulkDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use arrow_array::RecordBatch;
use std::collections::HashMap;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::typed_builder::{ColumnSet, ColumnSlot};
use super::value_utils::{
    ResponseMetadata, SecurityElements, arrow_type_for_element, get_value_with_datatype,
    has_scalar_value, top_level_response_error,
};
use xbbg_core::{BlpError, Element, Message, Name};

/// One scalar sub-field discovered in the bulk rows.
struct BulkSubfield {
    name: String,
    slot: ColumnSlot,
    /// Column-set row this sub-field was last written in; a repeated name in
    /// one bulk row keeps its first value.
    last_row: Option<usize>,
}

/// State for a bulk data request (bds).
pub struct BulkDataState {
    /// Field name as string (the bulk field to extract)
    field_name: String,
    /// Interned bulk field name, looked up once per security.
    field_lookup_name: Name,
    /// Interned `securityData` name, looked up once per message.
    security_data_name: Name,
    /// Column set for building the output.
    columns: ColumnSet,
    ticker_slot: ColumnSlot,
    field_slot: ColumnSlot,
    /// Discovered scalar sub-fields, in first-seen order across all rows.
    subfields: Vec<BulkSubfield>,
    /// Interned-name key to `subfields` index for allocation-free row decode.
    subfield_index_by_key: HashMap<usize, usize>,
    /// Response-level diagnostics (eidData / securityError / fieldExceptions) attached to the
    /// result batch as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl BulkDataState {
    /// Create a new bulkdata state.
    pub fn new(field: String, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        let mut columns = ColumnSet::new();
        let ticker_slot = columns.slot("ticker");
        let field_slot = columns.slot("field");
        Self {
            field_lookup_name: Name::get_or_intern(&field),
            security_data_name: Name::get_or_intern("securityData"),
            field_name: field,
            columns,
            ticker_slot,
            field_slot,
            subfields: Vec::new(),
            subfield_index_by_key: HashMap::new(),
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
        order.extend(self.subfields.iter().map(|subfield| subfield.name.as_str()));
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
        let Some(security_data) = root.get(&self.security_data_name) else {
            trace!("No securityData in message");
            return;
        };

        // Iterate through each security
        let n = security_data.len();
        for i in 0..n {
            let Some(sec) = security_data.get_element(i) else {
                continue;
            };

            let security = SecurityElements::scan(&sec);
            let ticker = security.ticker();

            let diagnostics = self.response_meta.record_security(ticker, &security);
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
            let Some(field_data) = security.field_data() else {
                trace!(ticker = ticker, "No fieldData for security");
                continue;
            };

            // Get the bulk field (which is an array)
            let Some(bulk_field) = field_data.get(&self.field_lookup_name) else {
                trace!(ticker = ticker, field = %self.field_name, "Bulk field not found");
                continue;
            };

            // Iterate through the array of rows
            let row_count = bulk_field.len();
            for j in 0..row_count {
                let Some(row) = bulk_field.get_element(j) else {
                    continue;
                };

                self.columns.append_str_slot(self.ticker_slot, ticker);
                self.columns
                    .append_str_slot(self.field_slot, &self.field_name);

                self.append_row_subfields(&row);

                self.columns.end_row();
            }
        }
    }

    /// Append one bulk row's scalar sub-fields; sub-fields absent from the row
    /// read as null through the column set's padding.
    fn append_row_subfields(&mut self, row: &Element<'_>) {
        let row_number = self.columns.row_count();
        for child in row.children() {
            if child.is_array() {
                continue;
            }
            // Read once: it both filters the child and selects the value getter.
            let datatype = child.datatype();
            if !has_scalar_value(datatype) {
                continue;
            }

            let idx = self.resolve_subfield_index(&child);
            let subfield = &mut self.subfields[idx];
            if subfield.last_row == Some(row_number) {
                continue;
            }
            subfield.last_row = Some(row_number);

            let slot = subfield.slot;
            if let Some(value) = get_value_with_datatype(&child, datatype) {
                self.columns.append_slot(slot, value);
            } else {
                self.columns.append_null_slot(slot);
            }
        }
    }

    fn resolve_subfield_index(&mut self, child: &Element<'_>) -> usize {
        let key = child.name_key();
        if let Some(&idx) = self.subfield_index_by_key.get(&key) {
            return idx;
        }

        let name = child.name_str();
        let idx = self.subfields.len();
        self.columns
            .set_type_hint(name, arrow_type_for_element(child));
        self.subfields.push(BulkSubfield {
            name: name.to_owned(),
            slot: self.columns.slot(name),
            last_row: None,
        });
        self.subfield_index_by_key.insert(key, idx);
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::super::value_utils::METADATA_KEY_TYPE_MISSES;
    use super::*;
    use arrow_array::Int64Array;
    use xbbg_core::EventType;
    use xbbg_core::test_support::TestEvent;

    fn response(datatype: &str, event_type: EventType, rows: serde_json::Value) -> TestEvent {
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test.bulk.{datatype}" version="1.0.0.0">
            <service name="//xbbg/test/bulk/{datatype}" version="1.0.0.0">
                <event name="ReferenceDataResponse" eventType="BulkResponse{datatype}"/>
            </service>
            <schema>
                <sequenceType name="BulkResponse{datatype}">
                    <element name="securityData" type="BulkSecurity{datatype}" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="BulkSecurity{datatype}">
                    <element name="security" type="String"/>
                    <element name="fieldData" type="BulkFields{datatype}"/>
                </sequenceType>
                <sequenceType name="BulkFields{datatype}">
                    <element name="BULK" type="BulkRow{datatype}" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="BulkRow{datatype}">
                    <element name="VALUE" type="{datatype}" minOccurs="0"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#
        );
        TestEvent::with_schema(
            &schema,
            event_type,
            "ReferenceDataResponse",
            &[],
            |formatter| {
                formatter.json(
                    &serde_json::json!({"securityData": [{
                        "security": "SYNTHETIC_SECURITY", "fieldData": {"BULK": rows}
                    }]})
                    .to_string(),
                );
            },
        )
    }

    #[test]
    fn bulk_column_type_changes_report_non_null_losses_without_counting_null_rows() {
        let first = response(
            "Int64",
            EventType::PartialResponse,
            serde_json::json!([{"VALUE": 7}, {"VALUE": null}]),
        );
        let last = response(
            "String",
            EventType::Response,
            serde_json::json!([{"VALUE": "synthetic incompatible text"}, {}]),
        );
        let (reply, mut result) = oneshot::channel();
        let mut state = BulkDataState::new("BULK".into(), reply);
        state.on_partial(&first.event().messages().next().unwrap());
        state.finish(&last.event().messages().next().unwrap());
        let batch = result.try_recv().unwrap().unwrap();
        let values = batch
            .column_by_name("VALUE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            [Some(7), None, None, None]
        );
        let misses: serde_json::Value =
            serde_json::from_str(&batch.schema_ref().metadata()[METADATA_KEY_TYPE_MISSES]).unwrap();
        assert_eq!(misses, serde_json::json!({"VALUE": 1}));
    }

    #[test]
    fn bulk_subfields_absent_from_some_messages_read_as_null_in_first_seen_order() {
        use arrow_array::{Array, StringArray};

        // Each message's schema declares only its own sub-fields, so a field is
        // genuinely absent (not null) from the other message's rows.
        fn rows_response(
            suffix: &str,
            row_type: &str,
            event_type: EventType,
            rows: serde_json::Value,
        ) -> TestEvent {
            let schema = format!(
                r#"<ServiceDefinition name="xbbg.test.bulk_rows.{suffix}" version="1.0.0.0">
                <service name="//xbbg/test/bulk_rows/{suffix}" version="1.0.0.0">
                    <event name="ReferenceDataResponse" eventType="RowsResponse{suffix}"/>
                </service>
                <schema>
                    <sequenceType name="RowsResponse{suffix}">
                        <element name="securityData" type="RowsSecurity{suffix}" maxOccurs="unbounded"/>
                    </sequenceType>
                    <sequenceType name="RowsSecurity{suffix}">
                        <element name="security" type="String"/>
                        <element name="fieldData" type="RowsFields{suffix}"/>
                    </sequenceType>
                    <sequenceType name="RowsFields{suffix}">
                        <element name="BULK" type="Row{suffix}" maxOccurs="unbounded"/>
                    </sequenceType>
                    <sequenceType name="Row{suffix}">{row_type}</sequenceType>
                </schema>
            </ServiceDefinition>"#
            );
            TestEvent::with_schema(
                &schema,
                event_type,
                "ReferenceDataResponse",
                &[],
                |formatter| {
                    formatter.json(
                        &serde_json::json!({"securityData": [{
                            "security": "SYNTHETIC_SECURITY", "fieldData": {"BULK": rows}
                        }]})
                        .to_string(),
                    );
                },
            )
        }
        let first = rows_response(
            "Count",
            r#"<element name="COUNT" type="Int64"/>"#,
            EventType::PartialResponse,
            serde_json::json!([{"COUNT": 1}, {"COUNT": 2}]),
        );
        let last = rows_response(
            "Label",
            r#"<element name="LABEL" type="String"/>"#,
            EventType::Response,
            serde_json::json!([{"LABEL": "late"}]),
        );
        let (reply, mut result) = oneshot::channel();
        let mut state = BulkDataState::new("BULK".into(), reply);
        state.on_partial(&first.event().messages().next().unwrap());
        state.finish(&last.event().messages().next().unwrap());
        let batch = result.try_recv().unwrap().unwrap();

        let names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, ["ticker", "field", "COUNT", "LABEL"]);
        let counts = batch
            .column_by_name("COUNT")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.iter().collect::<Vec<_>>(), [Some(1), Some(2), None]);
        let labels = batch
            .column_by_name("LABEL")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            [None, None, Some("late")]
        );
        assert_eq!(batch.column_by_name("ticker").unwrap().null_count(), 0);
    }
}
