//! Intraday bar (bdib) state with Arrow builders.
//!
//! Accumulation and streaming share the Element decoder, Arrow builders and schema.
//! Ticker is non-nullable: every row uses the request's supplied ticker string.

use std::sync::{Arc, LazyLock};

use arrow_array::builder::{
    Float64Builder, Int32Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::value_utils::{top_level_response_error, ResponseMetadata};
use xbbg_core::{BlpError, Element, Message, Name, Value};

struct IntradayBarNames {
    bar_data: Name,
    bar_tick_data: Name,
    eid_data: Name,
    time: Name,
    open: Name,
    high: Name,
    low: Name,
    close: Name,
    volume: Name,
    num_events: Name,
    value: Name,
}

impl IntradayBarNames {
    fn new() -> Self {
        Self {
            bar_data: Name::get_or_intern("barData"),
            bar_tick_data: Name::get_or_intern("barTickData"),
            eid_data: Name::get_or_intern("eidData"),
            time: Name::get_or_intern("time"),
            open: Name::get_or_intern("open"),
            high: Name::get_or_intern("high"),
            low: Name::get_or_intern("low"),
            close: Name::get_or_intern("close"),
            volume: Name::get_or_intern("volume"),
            num_events: Name::get_or_intern("numEvents"),
            value: Name::get_or_intern("value"),
        }
    }
}

struct IntradayBarBuilders {
    ticker: StringBuilder,
    time: TimestampMicrosecondBuilder,
    open: Float64Builder,
    high: Float64Builder,
    low: Float64Builder,
    close: Float64Builder,
    volume: Float64Builder,
    num_events: Int32Builder,
    value: Float64Builder,
}

impl IntradayBarBuilders {
    fn new(rows: usize, ticker_len: usize) -> Self {
        Self {
            ticker: StringBuilder::with_capacity(rows, ticker_len.saturating_mul(rows)),
            time: TimestampMicrosecondBuilder::with_capacity(rows),
            open: Float64Builder::with_capacity(rows),
            high: Float64Builder::with_capacity(rows),
            low: Float64Builder::with_capacity(rows),
            close: Float64Builder::with_capacity(rows),
            volume: Float64Builder::with_capacity(rows),
            num_events: Int32Builder::with_capacity(rows),
            value: Float64Builder::with_capacity(rows),
        }
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.ticker.finish()),
            Arc::new(self.time.finish().with_timezone("UTC")),
            Arc::new(self.open.finish()),
            Arc::new(self.high.finish()),
            Arc::new(self.low.finish()),
            Arc::new(self.close.finish()),
            Arc::new(self.volume.finish()),
            Arc::new(self.num_events.finish()),
            Arc::new(self.value.finish()),
        ]
    }
}

/// Shared bar decoding and Arrow materialization for both delivery modes.
pub(super) struct IntradayBarResponse {
    /// Ticker for this request
    ticker: String,
    /// Pre-interned Bloomberg field names used in hot-path lookups.
    names: IntradayBarNames,
    /// Fixed Arrow builders for the output schema.
    builders: Option<IntradayBarBuilders>,
    /// Response-level entitlement IDs attached as schema metadata.
    response_meta: ResponseMetadata,
}

/// State for an intraday bar request (bdib).
pub struct IntradayBarState {
    event_type: String,
    interval: u32,
    response: IntradayBarResponse,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl IntradayBarState {
    /// Create a new intraday bar state.
    pub fn new(
        ticker: String,
        event_type: String,
        interval: u32,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        Self {
            event_type,
            interval,
            response: IntradayBarResponse::new(ticker),
            reply,
        }
    }

    /// Get the event type.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Get the interval.
    pub fn interval(&self) -> u32 {
        self.interval
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.response.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "IntradayBarRequest") {
            let _ = self.reply.send(Err(error));
            return;
        }
        self.response.process_message(msg);
        let result = self.response.finish_batch();
        if let Ok(ref batch) = result {
            xbbg_log::debug!(rows = batch.num_rows(), "intradaybar finish");
        }
        let _ = self.reply.send(result);
    }
}

impl IntradayBarResponse {
    pub(super) fn new(ticker: String) -> Self {
        Self {
            ticker,
            names: IntradayBarNames::new(),
            builders: None,
            response_meta: ResponseMetadata::default(),
        }
    }

    /// Process an IntradayBarResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// IntradayBarResponse {
    ///   barData {
    ///     barTickData[] {
    ///       time: 2024-01-15T09:30:00
    ///       open: 150.0
    ///       high: 151.0
    ///       low: 149.5
    ///       close: 150.5
    ///       volume: 1000000
    ///       numEvents: 500
    ///       value: 150500000.0
    ///     }
    ///   }
    /// }
    /// ```
    pub(super) fn process_message(&mut self, msg: &Message) -> bool {
        let root = msg.elements();

        // Get barData
        let Some(bar_data) = root.get(&self.names.bar_data) else {
            trace!("No barData in message");
            return false;
        };

        if let Some(eids) = bar_data.get(&self.names.eid_data) {
            self.response_meta.record_eid_data(&self.ticker, &eids);
        }

        // Get barTickData array
        let Some(bar_tick_data) = bar_data.get(&self.names.bar_tick_data) else {
            trace!("No barTickData in message");
            return !self.response_meta.is_empty();
        };

        // Iterate through each bar
        let n = bar_tick_data.len();
        if n == 0 {
            return !self.response_meta.is_empty();
        }
        let builders = self
            .builders
            .get_or_insert_with(|| IntradayBarBuilders::new(n, self.ticker.len()));
        for i in 0..n {
            let Some(bar) = bar_tick_data.get_element(i) else {
                continue;
            };

            builders.ticker.append_value(&self.ticker);
            Self::append_time_field(&bar, &self.names.time, &mut builders.time);
            Self::append_f64_field(&bar, &self.names.open, &mut builders.open);
            Self::append_f64_field(&bar, &self.names.high, &mut builders.high);
            Self::append_f64_field(&bar, &self.names.low, &mut builders.low);
            Self::append_f64_field(&bar, &self.names.close, &mut builders.close);
            Self::append_f64_field(&bar, &self.names.volume, &mut builders.volume);
            Self::append_i32_field(&bar, &self.names.num_events, &mut builders.num_events);
            Self::append_f64_field(&bar, &self.names.value, &mut builders.value);
        }
        true
    }

    pub(super) fn finish_batch(&mut self) -> Result<RecordBatch, BlpError> {
        static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
            Arc::new(Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("open", DataType::Float64, true),
                Field::new("high", DataType::Float64, true),
                Field::new("low", DataType::Float64, true),
                Field::new("close", DataType::Float64, true),
                Field::new("volume", DataType::Float64, true),
                Field::new("numEvents", DataType::Int32, true),
                Field::new("value", DataType::Float64, true),
            ]))
        });
        let mut builders = self
            .builders
            .take()
            .unwrap_or_else(|| IntradayBarBuilders::new(0, self.ticker.len()));
        let batch = RecordBatch::try_new(Arc::clone(&SCHEMA), builders.finish()).map_err(|e| {
            BlpError::Internal {
                detail: format!("build IntradayBar RecordBatch: {e}"),
            }
        })?;
        Ok(std::mem::take(&mut self.response_meta).attach(batch))
    }

    fn append_time_field(
        element: &Element<'_>,
        field_name: &Name,
        builder: &mut TimestampMicrosecondBuilder,
    ) {
        if let Some(field_elem) = element.get(field_name) {
            if let Some(value) = field_elem.get_timestamp_us(0) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }

    fn append_f64_field(element: &Element<'_>, field_name: &Name, builder: &mut Float64Builder) {
        if let Some(field_elem) = element.get(field_name) {
            if let Some(value) = field_elem.get_value(0).and_then(|value| value.as_f64()) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }

    fn append_i32_field(element: &Element<'_>, field_name: &Name, builder: &mut Int32Builder) {
        if let Some(field_elem) = element.get(field_name) {
            match field_elem.get_value(0) {
                Some(Value::Int32(value)) => builder.append_value(value),
                Some(Value::Int64(value)) => builder.append_value(value as i32),
                Some(Value::Byte(value)) => builder.append_value(value as i32),
                Some(Value::Bool(value)) => builder.append_value(if value { 1 } else { 0 }),
                Some(Value::Float64(value))
                    if value.is_finite()
                        && value.fract() == 0.0
                        && value >= i32::MIN as f64
                        && value <= i32::MAX as f64 =>
                {
                    builder.append_value(value as i32);
                }
                _ => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }
}
