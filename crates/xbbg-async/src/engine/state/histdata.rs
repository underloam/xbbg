//! Historical data (bdh) state with Arrow builders.
//!
//! Extracts HistoricalDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::refdata::{LongMode, OutputFormat};
use super::typed_builder::{ArrowType, ColumnSet};
use super::value_utils::{
    LongRowWriter, LongStringColumns, ResponseMetadata, SecurityElements, TypedLongColumns,
    WideColumns, common_value_type, field_dtype_hints, get_value_cached_datatype,
    top_level_response_error,
};
use xbbg_core::{BlpError, DataType as BlpDataType, Message, Name, Value};

struct HistDataElementNames {
    security_data: Name,
    date: Name,
}

impl HistDataElementNames {
    fn new() -> Self {
        Self {
            security_data: Name::get_or_intern("securityData"),
            date: Name::get_or_intern("date"),
        }
    }
}

/// Shared decoder and builders; delivery adapters decide when to finish a batch.
pub(super) struct HistDataResponse {
    /// Field names as strings
    field_names: Vec<String>,
    /// Pre-interned Bloomberg field names for hot lookups
    field_lookup_names: Vec<Name>,
    /// Observed Bloomberg data types for requested fields, learned from returned Elements
    field_value_datatypes: Vec<Option<BlpDataType>>,
    /// Observed Bloomberg data type for the structural date field
    date_datatype: Option<BlpDataType>,
    /// Pre-interned structural names for response traversal
    names: HistDataElementNames,
    /// Per requested field, the `dtype` its type hint fixes in metadata rows
    field_dtype_hints: Vec<Option<&'static str>>,
    /// Output format
    format: OutputFormat,
    /// Long format mode (only used when format == Long)
    long_mode: LongMode,
    /// Column set for building the output
    columns: ColumnSet,
    /// Slots for long rows appended through `columns`, resolved on first use
    long_rows: Option<LongRowWriter>,
    /// Fixed long-format builders for the common string-value output path
    long_columns: Option<LongStringColumns>,
    /// Fixed typed-long builders for direct multi-value output
    typed_long_columns: Option<TypedLongColumns>,
    /// Fixed wide-format builders for requested field columns
    wide_columns: Option<WideColumns>,
    /// Response-level diagnostics (eidData / securityError / fieldExceptions)
    /// attached to the result batch as schema metadata.
    response_meta: ResponseMetadata,
}

/// State for a historical data request (bdh).
pub struct HistDataState {
    response: HistDataResponse,
    failed_securities: Vec<String>,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl HistDataState {
    /// Create a new histdata state with Long format (default).
    pub fn new(fields: Vec<String>, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self::with_format(fields, OutputFormat::Long, LongMode::String, None, reply)
    }

    /// Create a new histdata state with optional field type overrides (defaults to Long format).
    pub fn with_types(
        fields: Vec<String>,
        field_types: Option<HashMap<String, ArrowType>>,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        Self::with_format(
            fields,
            OutputFormat::Long,
            LongMode::String,
            field_types,
            reply,
        )
    }

    /// Create a new histdata state with specified format.
    pub fn with_format(
        fields: Vec<String>,
        format: OutputFormat,
        long_mode: LongMode,
        field_types: Option<HashMap<String, ArrowType>>,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        Self {
            response: HistDataResponse::new(fields, format, long_mode, field_types),
            failed_securities: Vec::new(),
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        if let Some(ticker) = self.response.process_message(msg) {
            self.failed_securities.push(ticker.to_string());
        }
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "HistoricalDataRequest")
        {
            let _ = self.reply.send(Err(error));
            return;
        }
        self.on_partial(msg);
        if self.response.row_count() == 0 && !self.failed_securities.is_empty() {
            let _ = self.reply.send(Err(BlpError::RequestFailure {
                service: "//blp/refdata".to_string(),
                operation: Some("HistoricalDataRequest".to_string()),
                cid: None,
                label: Some(format!(
                    "All securities failed: {}",
                    self.failed_securities.join(", ")
                )),
                request_id: None,
                bloomberg_request_ids: Vec::new(),
                source: None,
            }));
            return;
        }
        let result = self.response.finish_batch();
        if let Ok(batch) = &result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "histdata finish"
            );
        }
        let _ = self.reply.send(result);
    }
}

impl HistDataResponse {
    pub(super) fn new(
        fields: Vec<String>,
        format: OutputFormat,
        long_mode: LongMode,
        field_types: Option<HashMap<String, ArrowType>>,
    ) -> Self {
        let arrow_types = field_types.unwrap_or_default();
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
            (format == OutputFormat::Wide).then(|| WideColumns::histdata(&fields, &arrow_types));
        let mut columns = ColumnSet::new();
        if long_value_type.is_none() && wide_columns.is_none() && long_mode != LongMode::Typed {
            for (name, arrow_type) in &arrow_types {
                columns.set_type_hint(name, *arrow_type);
            }
        }

        Self {
            field_dtype_hints: field_dtype_hints(&fields, &arrow_types),
            field_names: fields,
            field_lookup_names,
            field_value_datatypes,
            date_datatype: None,
            names: HistDataElementNames::new(),
            format,
            long_mode,
            columns,
            long_rows: None,
            long_columns: long_value_type.map(LongStringColumns::histdata),
            typed_long_columns: (format == OutputFormat::Long && long_mode == LongMode::Typed)
                .then(TypedLongColumns::histdata),
            wide_columns,
            response_meta: ResponseMetadata::default(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.row_count() == 0 && self.response_meta.is_empty()
    }

    fn row_count(&self) -> usize {
        match self.format {
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
        }
    }

    pub(super) fn finish_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let response_meta = std::mem::take(&mut self.response_meta);
        let result = match self.format {
            OutputFormat::Long => match self.long_mode {
                LongMode::String => {
                    if let Some(long_columns) = self.long_columns.take() {
                        long_columns.finish_histdata()
                    } else {
                        self.take_columns()
                            .finish_with_order(&["ticker", "date", "field", "value"])
                    }
                }
                LongMode::WithMetadata => self
                    .take_columns()
                    .finish_with_order(&["ticker", "date", "field", "value", "dtype"]),
                LongMode::Typed => self
                    .typed_long_columns
                    .take()
                    .unwrap_or_else(TypedLongColumns::histdata)
                    .finish(),
            },
            OutputFormat::Wide => self
                .wide_columns
                .as_mut()
                .expect("wide historical response has wide builders")
                .finish_histdata(),
        };
        result.map(|batch| response_meta.attach(batch))
    }

    /// Take the column set; slots belong to the set that resolved them, so
    /// its replacement resolves them again.
    fn take_columns(&mut self) -> ColumnSet {
        self.long_rows = None;
        std::mem::take(&mut self.columns)
    }

    /// Process a HistoricalDataResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// HistoricalDataResponse {
    ///   securityData {
    ///     security: "AAPL US Equity"
    ///     fieldData[] {
    ///       date: 2024-01-15
    ///       PX_LAST: 150.0
    ///       VOLUME: 1000000
    ///       ...
    ///     }
    ///     fieldExceptions[]? { ... }
    ///     securityError? { ... }
    ///   }
    /// }
    /// ```
    pub(super) fn process_message<'a>(&mut self, msg: &Message<'a>) -> Option<&'a str> {
        let root = msg.elements();

        // Get securityData (note: singular in HistoricalDataResponse)
        let Some(security_data) = root
            .get(&self.names.security_data)
            .filter(|data| !data.is_null())
        else {
            trace!("No securityData in message");
            return None;
        };

        let security = SecurityElements::scan(&security_data);
        let ticker = security.ticker();

        let diagnostics = self.response_meta.record_security(ticker, &security);
        if let Some(error) = diagnostics.security_error {
            xbbg_log::warn!(
                ticker = ticker,
                category = error.category,
                code = error.code,
                message = error.message,
                "HistoricalData securityError; skipping security"
            );
            return Some(ticker);
        }

        // Get fieldData array
        let Some(field_data) = security.field_data() else {
            trace!(ticker = ticker, "No fieldData for security");
            return None;
        };
        if self.format == OutputFormat::Long
            && self.long_mode == LongMode::Typed
            && let Some(columns) = self.typed_long_columns.as_mut()
        {
            columns.reserve_if_empty(field_data.len().saturating_mul(self.field_names.len()));
        }

        // Iterate through each row (each date)
        for row in field_data.values() {
            // Get date value for this row
            let date_value = row
                .get(&self.names.date)
                .and_then(|element| get_value_cached_datatype(&element, &mut self.date_datatype));

            match self.format {
                OutputFormat::Long => {
                    self.process_long_format(ticker, &date_value, &row);
                }
                OutputFormat::Wide => {
                    self.process_wide_format(ticker, &date_value, &row);
                }
            }
        }
        None
    }

    /// Process row in long format (one row per field).
    fn process_long_format(
        &mut self,
        ticker: &str,
        date_value: &Option<Value>,
        row: &xbbg_core::Element,
    ) {
        if let Some(long_columns) = self.long_columns.as_mut() {
            for ((field_name, field_lookup_name), field_datatype) in self
                .field_names
                .iter()
                .zip(&self.field_lookup_names)
                .zip(self.field_value_datatypes.iter_mut())
            {
                let value = row
                    .get(field_lookup_name)
                    .and_then(|element| get_value_cached_datatype(&element, field_datatype));
                long_columns.append_histdata_row(ticker, date_value.clone(), field_name, value);
            }
            return;
        }
        if let Some(typed_columns) = self.typed_long_columns.as_mut() {
            for ((field_name, field_lookup_name), field_datatype) in self
                .field_names
                .iter()
                .zip(&self.field_lookup_names)
                .zip(self.field_value_datatypes.iter_mut())
            {
                let value = row
                    .get(field_lookup_name)
                    .and_then(|element| get_value_cached_datatype(&element, field_datatype));
                typed_columns.append_row(ticker, date_value.as_ref(), field_name, value);
            }
            return;
        }

        let writer = *self
            .long_rows
            .get_or_insert_with(|| LongRowWriter::histdata(&mut self.columns, self.long_mode));
        for (((field_name, field_lookup_name), field_datatype), dtype_hint) in self
            .field_names
            .iter()
            .zip(&self.field_lookup_names)
            .zip(self.field_value_datatypes.iter_mut())
            .zip(&self.field_dtype_hints)
        {
            let value = row
                .get(field_lookup_name)
                .and_then(|element| get_value_cached_datatype(&element, field_datatype));
            writer.append_row(
                &mut self.columns,
                ticker,
                date_value.as_ref(),
                field_name,
                value,
                *dtype_hint,
            );
        }
    }

    /// Process row in wide format (one row per date with all fields as columns).
    fn process_wide_format(
        &mut self,
        ticker: &str,
        date_value: &Option<Value>,
        row: &xbbg_core::Element,
    ) {
        if let Some(wide_columns) = self.wide_columns.as_mut() {
            wide_columns.append_histdata_row(
                ticker,
                date_value.clone(),
                &self.field_lookup_names,
                &mut self.field_value_datatypes,
                |field_lookup_name, field_datatype| {
                    row.get(field_lookup_name)
                        .and_then(|element| get_value_cached_datatype(&element, field_datatype))
                },
            );
        }
    }
}
