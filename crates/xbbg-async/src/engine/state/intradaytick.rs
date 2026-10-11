//! Intraday tick (bdtick) state with specialized Arrow builders.
//!
//! Extracts IntradayTickResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::typed_builder::{ArrowType, TypedBuilder};
use super::value_utils::{
    ResponseMetadata, arrow_type_for_element, should_emit_scalar_field, top_level_response_error,
};
use xbbg_core::{BlpError, Element, Message, Name};

const TICKER_COLUMN: &str = "ticker";
const CORE_TICK_FIELDS: [(&str, ArrowType); 4] = [
    ("time", ArrowType::TimestampMicros),
    ("type", ArrowType::String),
    ("value", ArrowType::Float64),
    ("size", ArrowType::Int64),
];

struct TickField {
    output_name: String,
    lookup_name: Name,
    builder: TypedBuilder,
}

impl TickField {
    fn new(name: &str, arrow_type: ArrowType) -> Self {
        Self {
            output_name: name.to_string(),
            lookup_name: Name::get_or_intern(name),
            builder: TypedBuilder::new(arrow_type),
        }
    }
}

/// State for an intraday tick request (bdtick).
pub struct IntradayTickState {
    /// Ticker for this request.
    ticker: String,
    /// Field membership for O(1) duplicate checks while preserving output order.
    column_name_set: HashSet<String>,
    /// Synthetic ticker column builder.
    ticker_builder: TypedBuilder,
    /// Bloomberg tick fields in output order, excluding synthetic ticker.
    tick_fields: Vec<TickField>,
    /// Pre-interned lookup names parallel to `tick_fields` for one-shot name matching.
    lookup_names: Vec<Name>,
    /// Per-row scratch bitmap for fields seen while walking the tick children.
    seen_fields: Vec<bool>,
    /// Number of completed output rows.
    row_count: usize,
    /// Response-level entitlement IDs attached as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel.
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl IntradayTickState {
    /// Create a new intraday tick state.
    pub fn new(ticker: String, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        let tick_fields: Vec<_> = CORE_TICK_FIELDS
            .iter()
            .map(|(name, arrow_type)| TickField::new(name, *arrow_type))
            .collect();
        let lookup_names = tick_fields
            .iter()
            .map(|field| field.lookup_name.clone())
            .collect();

        Self {
            ticker,
            column_name_set: std::iter::once(TICKER_COLUMN.to_string())
                .chain(CORE_TICK_FIELDS.iter().map(|(name, _)| (*name).to_string()))
                .collect(),
            ticker_builder: TypedBuilder::new(ArrowType::String),
            tick_fields,
            lookup_names,
            seen_fields: vec![false; CORE_TICK_FIELDS.len()],
            row_count: 0,
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
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "IntradayTickRequest") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);
        let result = self.finish_batch();
        if let Ok(ref batch) = result {
            xbbg_log::debug!(rows = batch.num_rows(), "intradaytick finish");
        }
        let _ = self.reply.send(result);
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let mut fields = Vec::with_capacity(1 + self.tick_fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + self.tick_fields.len());

        fields.push(Field::new(
            TICKER_COLUMN,
            self.ticker_builder.data_type(),
            true,
        ));
        arrays.push(self.ticker_builder.finish());

        for field in &mut self.tick_fields {
            fields.push(Field::new(
                field.output_name.as_str(),
                field.builder.data_type(),
                true,
            ));
            arrays.push(field.builder.finish());
        }

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| {
            BlpError::Internal {
                detail: format!("build IntradayTick RecordBatch: {e}"),
            }
        })?;
        Ok(std::mem::take(&mut self.response_meta).attach(batch))
    }

    /// Process an IntradayTickResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// IntradayTickResponse {
    ///   tickData {
    ///     tickData[] {
    ///       time: 2024-01-15T09:30:00.123
    ///       type: "TRADE"
    ///       value: 150.0
    ///       size: 100
    ///       conditionCodes: "..."  # optional request-dependent fields
    ///     }
    ///     eidData[] { ... }          # response metadata, not per-tick data
    ///   }
    /// }
    /// ```
    ///
    /// Extra scalar children under `tickData.tickData[]` are appended as typed
    /// columns. Response metadata outside the tick array is attached to the
    /// resulting Arrow schema.
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        let Some(tick_data_outer) = root.get_by_str("tickData") else {
            trace!("No tickData in message");
            return;
        };

        if let Some(eids) = tick_data_outer.get_by_str("eidData") {
            self.response_meta.record_eid_data(&self.ticker, &eids);
        }

        let Some(tick_data) = tick_data_outer.get_by_str("tickData") else {
            trace!("No inner tickData array in message");
            return;
        };

        let n = tick_data.len();
        for i in 0..n {
            let Some(tick) = tick_data.get_element(i) else {
                continue;
            };

            self.ticker_builder.append_str(&self.ticker);
            self.seen_fields.clear();
            self.seen_fields.resize(self.tick_fields.len(), false);
            self.append_tick_fields(&tick);
            for (idx, field) in self.tick_fields.iter_mut().enumerate() {
                if !self.seen_fields[idx] {
                    field.builder.append_null();
                }
            }
            self.row_count += 1;
        }
    }

    fn append_tick_fields(&mut self, tick: &Element<'_>) {
        for child in tick.children() {
            let field_index = match self.find_tick_field(&child) {
                Some(idx) => idx,
                None => {
                    if !should_emit_scalar_field(&child) {
                        continue;
                    }
                    let Some(idx) = self.discover_tick_field(&child) else {
                        continue;
                    };
                    idx
                }
            };

            if field_index >= self.seen_fields.len() {
                self.seen_fields.resize(self.tick_fields.len(), false);
            }
            if self.seen_fields[field_index] {
                continue;
            }
            self.seen_fields[field_index] = true;

            let field = &mut self.tick_fields[field_index];
            Self::append_child_value(&mut field.builder, &child);
        }
    }

    fn append_child_value(builder: &mut TypedBuilder, child: &Element<'_>) {
        match builder {
            TypedBuilder::Float64(builder) => {
                if let Some(value) = child.get_f64(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::Int64(builder) => {
                if let Some(value) = child.get_i64(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::Int32(builder) => {
                if let Some(value) = child.get_i32(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::String(builder) => {
                if let Some(value) = child.get_str(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::Bool(builder) => {
                if let Some(value) = child.get_bool(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::Date32(builder) => {
                if let Some(value) = child.get_date32(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::TimestampMicros(builder) => {
                if let Some(value) = child.get_timestamp_us(0) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            TypedBuilder::Time64Micros(builder) => {
                if let Some(value) = child.get_datetime(0).map(|dt| dt.to_time_micros()) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
        }
    }

    fn find_tick_field(&self, child: &Element<'_>) -> Option<usize> {
        child.name_index(&self.lookup_names)
    }
    fn discover_tick_field(&mut self, child: &Element<'_>) -> Option<usize> {
        let name = child.name_str();
        if name == TICKER_COLUMN || self.column_name_set.contains(name) {
            return None;
        }

        let output_name = name.to_string();
        self.column_name_set.insert(output_name.clone());

        let mut builder = TypedBuilder::new(arrow_type_for_element(child));
        for _ in 0..self.row_count {
            builder.append_null();
        }

        let lookup_name = Name::get_or_intern(&output_name);
        self.tick_fields.push(TickField {
            output_name,
            lookup_name: lookup_name.clone(),
            builder,
        });
        self.lookup_names.push(lookup_name);
        Some(self.tick_fields.len() - 1)
    }
}
