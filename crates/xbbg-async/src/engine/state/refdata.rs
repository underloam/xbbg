//! Reference data (bdp) state with Arrow builders.
//!
//! Extracts ReferenceDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use std::collections::{BTreeSet, HashMap};

use arrow_array::RecordBatch;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::typed_builder::{ArrowType, ColumnSet};
use super::value_utils::{
    append_long_value_row, common_value_type, get_value_cached_datatype, top_level_response_error,
    FieldExceptionMeta, LongStringColumns, ResponseMetadata, SecurityErrorMeta, TypedLongColumns,
    WideColumns,
};
use xbbg_core::{BlpError, DataType as BlpDataType, Element, Message, Name, Value};

/// Output format for reference data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Long format: ticker, field, value (one row per ticker-field pair)
    #[default]
    Long,
    /// Wide format: ticker, field1, field2, ... (one row per ticker)
    Wide,
}

/// Long format output mode variants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LongMode {
    /// All values as strings (default, backwards-compatible)
    #[default]
    String,
    /// String values with dtype column containing Arrow type name
    WithMetadata,
    /// Multi-value columns: value_f64, value_i64, value_str, value_bool,
    /// value_date, value_ts (full datetimes), value_time (time-only microseconds).
    Typed,
}

struct RefDataElementNames {
    security_data: Name,
    security: Name,
    security_error: Name,
    field_exceptions: Name,
    field_data: Name,
    category: Name,
    code: Name,
    message: Name,
    subcategory: Name,
    field_id: Name,
    error_info: Name,
    eid_data: Name,
}

impl RefDataElementNames {
    fn new() -> Self {
        Self {
            security_data: Name::get_or_intern("securityData"),
            security: Name::get_or_intern("security"),
            security_error: Name::get_or_intern("securityError"),
            field_exceptions: Name::get_or_intern("fieldExceptions"),
            field_data: Name::get_or_intern("fieldData"),
            category: Name::get_or_intern("category"),
            code: Name::get_or_intern("code"),
            message: Name::get_or_intern("message"),
            subcategory: Name::get_or_intern("subcategory"),
            field_id: Name::get_or_intern("fieldId"),
            error_info: Name::get_or_intern("errorInfo"),
            eid_data: Name::get_or_intern("eidData"),
        }
    }
}

/// State for a reference data request (bdp).
pub struct RefDataState {
    /// Field names as strings
    field_names: Vec<String>,
    /// Pre-interned Bloomberg field names for hot lookups
    field_lookup_names: Vec<Name>,
    /// Observed Bloomberg data types for requested fields, learned from returned Elements
    field_value_datatypes: Vec<Option<BlpDataType>>,
    /// Pre-interned structural names for response traversal
    names: RefDataElementNames,
    /// Field type hints (field name -> arrow type)
    field_types: HashMap<String, ArrowType>,
    /// Output format
    format: OutputFormat,
    /// Long format mode (only used when format == Long)
    long_mode: LongMode,
    /// Include security error rows in output.
    include_security_errors: bool,
    /// Security identifiers that returned securityError.
    failed_securities: Vec<String>,
    /// Security identifiers that returned fieldExceptions.
    field_exception_securities: BTreeSet<String>,
    /// Total number of fieldExceptions across all securities.
    field_exception_count: usize,
    /// Column set for building the output
    columns: ColumnSet,
    /// Fixed long-format builders for the common string-value output path
    long_columns: Option<LongStringColumns>,
    /// Fixed typed-long builders for direct multi-value output
    typed_long_columns: Option<TypedLongColumns>,
    /// Fixed wide-format builders for requested field columns
    wide_columns: Option<WideColumns>,
    /// Response-level diagnostics (eidData / securityError / fieldExceptions)
    /// attached to the result batch as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl RefDataState {
    /// Create a new refdata state with Long format (default).
    pub fn new(fields: Vec<String>, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self::with_format(
            fields,
            OutputFormat::Long,
            LongMode::String,
            None,
            false,
            reply,
        )
    }

    /// Create a new refdata state with specified format.
    pub fn with_format(
        fields: Vec<String>,
        format: OutputFormat,
        long_mode: LongMode,
        field_types: Option<HashMap<String, String>>,
        include_security_errors: bool,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        // Convert string types to ArrowType
        let arrow_types: HashMap<String, ArrowType> = field_types
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, ArrowType::parse(&v)))
            .collect();
        let field_lookup_names: Vec<Name> = fields
            .iter()
            .map(|field| Name::get_or_intern(field))
            .collect();
        let field_value_datatypes = vec![None; field_lookup_names.len()];

        // Fixed long output modes bypass ColumnSet entirely; keep ColumnSet hints only
        // for dynamic wide/metadata paths that actually append through ColumnSet.
        let long_value_type = (format == OutputFormat::Long && long_mode == LongMode::String)
            .then(|| common_value_type(&fields, &arrow_types));
        let wide_columns =
            (format == OutputFormat::Wide).then(|| WideColumns::refdata(&fields, &arrow_types));
        let mut columns = ColumnSet::new();
        if long_value_type.is_none() && wide_columns.is_none() && long_mode != LongMode::Typed {
            for (name, arrow_type) in &arrow_types {
                columns.set_type_hint(name, *arrow_type);
            }
        }

        Self {
            field_names: fields,
            field_lookup_names,
            field_value_datatypes,
            names: RefDataElementNames::new(),
            field_types: arrow_types,
            format,
            long_mode,
            include_security_errors,
            failed_securities: Vec::new(),
            field_exception_securities: BTreeSet::new(),
            field_exception_count: 0,
            columns,
            long_columns: long_value_type.map(LongStringColumns::refdata),
            typed_long_columns: (format == OutputFormat::Long && long_mode == LongMode::Typed)
                .then(TypedLongColumns::refdata),
            wide_columns,
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

        if !self.failed_securities.is_empty() {
            xbbg_log::warn!(
                count = self.failed_securities.len(),
                tickers = ?self.failed_securities,
                "ReferenceData completed with security failures"
            );
        }

        if self.field_exception_count > 0 {
            xbbg_log::warn!(
                count = self.field_exception_count,
                ticker_count = self.field_exception_securities.len(),
                tickers = ?self.field_exception_securities,
                "ReferenceData completed with field exceptions"
            );
        }

        let row_count = match self.format {
            OutputFormat::Long => self.long_columns.as_ref().map_or_else(
                || {
                    self.typed_long_columns
                        .as_ref()
                        .map_or_else(|| self.columns.row_count(), TypedLongColumns::row_count)
                },
                LongStringColumns::row_count,
            ),
            OutputFormat::Wide => self
                .wide_columns
                .as_ref()
                .map_or_else(|| self.columns.row_count(), WideColumns::row_count),
        };
        if row_count == 0 && !self.failed_securities.is_empty() {
            let detail = format!(
                "All securities failed: {}",
                self.failed_securities.join(", ")
            );
            let _ = self.reply.send(Err(BlpError::RequestFailure {
                service: "//blp/refdata".to_string(),
                operation: Some("ReferenceDataRequest".to_string()),
                cid: None,
                label: Some(detail),
                request_id: None,
                source: None,
            }));
            return;
        }

        let reply = self.reply;
        let response_meta = std::mem::take(&mut self.response_meta);
        let result = match self.format {
            OutputFormat::Long => match self.long_mode {
                LongMode::String => {
                    if let Some(long_columns) = self.long_columns.take() {
                        long_columns.finish_refdata()
                    } else {
                        self.columns
                            .finish_with_order(&["ticker", "field", "value"])
                    }
                }
                LongMode::WithMetadata => self
                    .columns
                    .finish_with_order(&["ticker", "field", "value", "dtype"]),
                LongMode::Typed => self
                    .typed_long_columns
                    .take()
                    .unwrap_or_else(TypedLongColumns::refdata)
                    .finish(),
            },
            OutputFormat::Wide => {
                if let Some(wide_columns) = self.wide_columns.take() {
                    wide_columns.finish_refdata()
                } else {
                    let mut order = vec!["ticker"];
                    order.extend(self.field_names.iter().map(|s| s.as_str()));
                    self.columns.finish_with_order(&order)
                }
            }
        };
        let result = result.map(|batch| response_meta.attach(batch));
        if let Ok(batch) = &result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "refdata finish"
            );
        }
        let _ = reply.send(result);
    }

    /// Process a ReferenceDataResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// ReferenceDataResponse {
    ///   securityData[] {
    ///     security: "AAPL US Equity"
    ///     fieldData {
    ///       PX_LAST: 150.0
    ///       NAME: "Apple Inc"
    ///       ...
    ///     }
    ///     fieldExceptions[]? { ... }
    ///     securityError? { ... }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Get securityData array
        let Some(security_data) = root.get(&self.names.security_data) else {
            trace!("No securityData in message");
            return;
        };
        if self.format == OutputFormat::Long && self.long_mode == LongMode::Typed {
            if let Some(columns) = self.typed_long_columns.as_mut() {
                columns
                    .reserve_if_empty(security_data.len().saturating_mul(self.field_names.len()));
            }
        }

        // Iterate through each security
        for sec in security_data.values() {
            // Get ticker
            let ticker = sec
                .get(&self.names.security)
                .and_then(|e| e.get_str(0))
                .unwrap_or("");

            // eidData rides alongside fieldData when returnEids was requested;
            // failed securities can still carry it, so record before the
            // securityError skip.
            if let Some(eids) = sec.get(&self.names.eid_data) {
                self.response_meta.record_eid_data(ticker, &eids);
            }

            // Check for security error
            if let Some(security_error) = sec.get(&self.names.security_error) {
                let category = security_error
                    .get(&self.names.category)
                    .and_then(|e| e.get_str(0))
                    .unwrap_or("");
                let code = security_error
                    .get(&self.names.code)
                    .and_then(|e| e.get_i32(0))
                    .unwrap_or_default();
                let message = security_error
                    .get(&self.names.message)
                    .and_then(|e| e.get_str(0))
                    .unwrap_or("");
                let subcategory = security_error
                    .get(&self.names.subcategory)
                    .and_then(|e| e.get_str(0))
                    .unwrap_or("");

                xbbg_log::warn!(
                    ticker = ticker,
                    category = category,
                    code = code,
                    message = message,
                    "ReferenceData securityError; skipping security"
                );

                self.failed_securities.push(ticker.to_string());
                self.response_meta.record_security_error(
                    ticker,
                    SecurityErrorMeta {
                        category: category.to_string(),
                        code,
                        subcategory: subcategory.to_string(),
                        message: message.to_string(),
                    },
                );

                if self.include_security_errors {
                    self.append_security_error_row(ticker, code, category, subcategory, message);
                }
                continue;
            }

            if let Some(field_exceptions) = sec.get(&self.names.field_exceptions) {
                let n = field_exceptions.len();
                if n > 0 {
                    // Collect field names and error messages for diagnostics
                    let mut details: Vec<String> = Vec::with_capacity(n);
                    for exc in field_exceptions.values() {
                        let field_id = exc
                            .get(&self.names.field_id)
                            .and_then(|e| e.get_str(0))
                            .unwrap_or("?");
                        let err_info = exc.get(&self.names.error_info);
                        let message = err_info
                            .as_ref()
                            .and_then(|e| e.get(&self.names.message))
                            .and_then(|e| e.get_str(0))
                            .unwrap_or("");
                        let category = err_info
                            .as_ref()
                            .and_then(|e| e.get(&self.names.category))
                            .and_then(|e| e.get_str(0))
                            .unwrap_or("");
                        let code = err_info
                            .as_ref()
                            .and_then(|e| e.get(&self.names.code))
                            .and_then(|e| e.get_i32(0))
                            .unwrap_or_default();
                        let subcategory = err_info
                            .as_ref()
                            .and_then(|e| e.get(&self.names.subcategory))
                            .and_then(|e| e.get_str(0))
                            .unwrap_or("");
                        self.response_meta.record_field_exception(
                            ticker,
                            FieldExceptionMeta {
                                field: field_id.to_string(),
                                category: category.to_string(),
                                code,
                                subcategory: subcategory.to_string(),
                                message: message.to_string(),
                            },
                        );
                        details.push(format!("{field_id}: {message}"));
                    }
                    self.field_exception_securities.insert(ticker.to_string());
                    self.field_exception_count += n;
                    xbbg_log::debug!(
                        ticker = ticker,
                        count = n,
                        fields = details.join(", ").as_str(),
                        "ReferenceData fieldExceptions"
                    );
                }
            }

            // Get fieldData
            let Some(field_data) = sec.get(&self.names.field_data) else {
                trace!(ticker = ticker, "No fieldData for security");
                continue;
            };

            match self.format {
                OutputFormat::Long => {
                    self.process_long_format(ticker, &field_data);
                }
                OutputFormat::Wide => {
                    self.process_wide_format(ticker, &field_data);
                }
            }
        }
    }

    fn append_security_error_row(
        &mut self,
        ticker: &str,
        code: i32,
        category: &str,
        subcategory: &str,
        message: &str,
    ) {
        if let Some(long_columns) = self.long_columns.as_mut() {
            let detail = format!(
                "code={code} category={category} subcategory={subcategory} message={message}"
            );
            long_columns.append_refdata_row(
                ticker,
                "__SECURITY_ERROR__",
                Some(Value::String(detail.as_str())),
            );
            return;
        }
        if let Some(typed_columns) = self.typed_long_columns.as_mut() {
            let detail = format!(
                "code={code} category={category} subcategory={subcategory} message={message}"
            );
            typed_columns.append_row(
                ticker,
                None,
                "__SECURITY_ERROR__",
                Some(Value::String(detail.as_str())),
            );
            return;
        }
        let detail =
            format!("code={code} category={category} subcategory={subcategory} message={message}");
        let value = Some(Value::String(detail.as_str()));
        append_long_value_row(
            &mut self.columns,
            self.long_mode,
            "__SECURITY_ERROR__",
            value,
            Some("string"),
            |columns| columns.append_str("ticker", ticker),
        );
    }

    /// Process security in long format (one row per field).
    fn process_long_format(&mut self, ticker: &str, field_data: &Element) {
        if let Some(long_columns) = self.long_columns.as_mut() {
            for ((field_name, field_lookup_name), field_datatype) in self
                .field_names
                .iter()
                .zip(&self.field_lookup_names)
                .zip(self.field_value_datatypes.iter_mut())
            {
                let value = field_data
                    .get(field_lookup_name)
                    .and_then(|element| get_value_cached_datatype(&element, field_datatype));
                long_columns.append_refdata_row(ticker, field_name, value);
            }
            return;
        }
        if let Some(typed_columns) = self.typed_long_columns.as_mut() {
            for (field_name, field_lookup_name) in
                self.field_names.iter().zip(&self.field_lookup_names)
            {
                let value = field_data
                    .get(field_lookup_name)
                    .and_then(|element| element.get_value(0));
                typed_columns.append_row(ticker, None, field_name, value);
            }
            return;
        }

        let long_mode = self.long_mode;
        let field_names = &self.field_names;
        let field_lookup_names = &self.field_lookup_names;
        let field_types = &self.field_types;
        let columns = &mut self.columns;

        for (field_name, field_lookup_name) in field_names.iter().zip(field_lookup_names) {
            // Get the field element
            let value = field_data
                .get(field_lookup_name)
                .and_then(|e| e.get_value(0));
            let dtype = value
                .as_ref()
                .map(|v| dtype_from_hints(field_types, field_name, v));

            append_long_value_row(columns, long_mode, field_name, value, dtype, |columns| {
                columns.append_str("ticker", ticker)
            });
        }
    }

    /// Process security in wide format (one row per ticker).
    fn process_wide_format(&mut self, ticker: &str, field_data: &Element) {
        if let Some(wide_columns) = self.wide_columns.as_mut() {
            wide_columns.append_refdata_row(
                ticker,
                &self.field_lookup_names,
                &mut self.field_value_datatypes,
                |field_lookup_name, field_datatype| {
                    field_data
                        .get(field_lookup_name)
                        .and_then(|element| get_value_cached_datatype(&element, field_datatype))
                },
            );
        }
    }
}

fn dtype_from_hints(
    field_types: &HashMap<String, ArrowType>,
    field_name: &str,
    value: &Value<'_>,
) -> &'static str {
    // Use type hint if available
    if let Some(hint) = field_types.get(field_name) {
        return hint.type_name();
    }
    // Otherwise infer from value
    ArrowType::from_value(value).type_name()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::HistDataState;
    use crate::field_cache::BlpFieldType;
    use arrow_array::{
        Array, Date32Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    use xbbg_core::test_support::TestEvent;
    use xbbg_core::EventType;

    fn temporal_response(historical: bool) -> TestEvent {
        let (message_type, securities_array, data_array) = if historical {
            ("HistoricalDataResponse", "", "maxOccurs=\"unbounded\"")
        } else {
            ("ReferenceDataResponse", "maxOccurs=\"unbounded\"", "")
        };
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test.{message_type}" version="1.0.0.0">
            <service name="//xbbg/test/{message_type}" version="1.0.0.0">
                <event name="{message_type}" eventType="Response{message_type}"/>
            </service>
            <schema>
                <sequenceType name="Response{message_type}">
                    <element name="securityData" type="SecurityData{message_type}" {securities_array}/>
                </sequenceType>
                <sequenceType name="SecurityData{message_type}">
                    <element name="security" type="String"/>
                    <element name="fieldData" type="FieldData{message_type}" {data_array}/>
                </sequenceType>
                <sequenceType name="FieldData{message_type}">
                    <element name="date" type="Date" minOccurs="0"/>
                    <element name="SYNTHETIC_TIME" type="Time"/>
                    <element name="SYNTHETIC_DATE" type="Date"/>
                    <element name="SYNTHETIC_STAMP" type="Datetime"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#
        );
        let data = serde_json::json!({
            "date": "2026-01-01",
            "SYNTHETIC_TIME": "15:59:01.123456",
            "SYNTHETIC_DATE": "2026-01-02",
            "SYNTHETIC_STAMP": "2026-01-02T15:59:01.123456Z"
        });
        let security = serde_json::json!({
            "security": "IBM US Equity",
            "fieldData": if historical { serde_json::json!([data]) } else { data }
        });
        let response = serde_json::json!({
            "securityData": if historical { security } else { serde_json::json!([security]) }
        });
        TestEvent::with_schema(
            &schema,
            EventType::Response,
            message_type,
            &[],
            |formatter| {
                formatter.json(&response.to_string());
            },
        )
    }

    fn temporal_batch(historical: bool, format: OutputFormat, mode: LongMode) -> RecordBatch {
        let event = temporal_response(historical);
        // Keep the SDK iterator alive while consuming its borrowed message.
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        let fields = [
            "SYNTHETIC_TIME",
            "SYNTHETIC_DATE",
            "SYNTHETIC_STAMP",
            "SYNTHETIC_MISSING_TIME",
        ]
        .map(str::to_string)
        .to_vec();
        let hints = fields
            .iter()
            .zip(["Time", "Date", "Datetime", "Time"])
            .map(|(field, ftype)| {
                (
                    field.clone(),
                    BlpFieldType::from_metadata(Some("Datetime"), Some(ftype))
                        .to_arrow_type_str()
                        .to_string(),
                )
            })
            .collect();
        let (sender, mut receiver) = oneshot::channel();
        if historical {
            HistDataState::with_format(fields, format, mode, Some(hints), sender).finish(&message);
        } else {
            RefDataState::with_format(fields, format, mode, Some(hints), false, sender)
                .finish(&message);
        }
        receiver.try_recv().unwrap().unwrap()
    }

    #[test]
    fn typed_long_refdata_and_history_separate_times_from_timestamps() {
        for historical in [false, true] {
            let batch = temporal_batch(historical, OutputFormat::Long, LongMode::Typed);
            let times = batch
                .column_by_name("value_time")
                .unwrap()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            let dates = batch
                .column_by_name("value_date")
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            let timestamps = batch
                .column_by_name("value_ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(
                times.iter().collect::<Vec<_>>(),
                [Some(57_541_123_456), None, None, None]
            );
            assert_eq!(
                dates.iter().collect::<Vec<_>>(),
                [None, Some(20_455), None, None]
            );
            assert_eq!(
                timestamps.iter().collect::<Vec<_>>(),
                [None, None, Some(1_767_369_541_123_456), None]
            );
            if historical {
                assert_eq!(
                    batch
                        .column_by_name("date")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(0),
                    20_454
                );
            }
        }
    }

    #[test]
    fn semi_long_refdata_and_history_keep_typed_temporal_fields_and_nulls() {
        for historical in [false, true] {
            let batch = temporal_batch(historical, OutputFormat::Wide, LongMode::String);
            assert_eq!(batch.num_rows(), 1);
            let times = batch
                .column_by_name("SYNTHETIC_TIME")
                .unwrap()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            assert_eq!(times.value(0), 57_541_123_456);
            let date = batch
                .column_by_name("SYNTHETIC_DATE")
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            assert_eq!(date.value(0), 20_455);
            let stamp = batch.column_by_name("SYNTHETIC_STAMP").unwrap();
            assert_eq!(
                stamp.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            );
            assert_eq!(
                stamp
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(0),
                1_767_369_541_123_456
            );
            let missing = batch.column_by_name("SYNTHETIC_MISSING_TIME").unwrap();
            assert_eq!(
                missing.data_type(),
                &DataType::Time64(TimeUnit::Microsecond)
            );
            assert!(missing.is_null(0));
        }
    }

    #[test]
    fn long_temporal_values_stay_text_and_metadata_reports_correct_kinds() {
        for historical in [false, true] {
            for mode in [LongMode::String, LongMode::WithMetadata] {
                let batch = temporal_batch(historical, OutputFormat::Long, mode);
                let values = batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    [
                        Some("15:59:01.123456"),
                        Some("2026-01-02"),
                        Some("2026-01-02T15:59:01.123456Z"),
                        None,
                    ]
                );
                if mode == LongMode::WithMetadata {
                    let types = batch
                        .column_by_name("dtype")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(
                        types.iter().collect::<Vec<_>>(),
                        [
                            Some("time64"),
                            Some("date32"),
                            Some("timestamp"),
                            Some("null")
                        ]
                    );
                }
            }
        }
    }
}
