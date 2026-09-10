use std::{collections::HashMap, sync::Arc};

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use xbbg_core::BlpError;

use super::update::{FieldKind, FieldLayout, SubscriptionUpdate, UpdateValue};

const PRESENCE_COLUMN_NAME: &str = "__xbbg_present";
const PRESENCE_METADATA_KEY: &str = "xbbg.subscription_presence";
const PRESENCE_METADATA_VALUE: &str =
    "column=__xbbg_present;encoding=binary-lsb-first;mapping=bit-i-to-schema-field-(i+2)";

pub struct SubscriptionArrowBatcher {
    layout: Option<Arc<FieldLayout>>,
    schema: Option<SchemaRef>,
    builders: Vec<SubscriptionColumnBuilder>,
    scratch: Vec<Option<usize>>,
    presence_scratch: Vec<u8>,
    rows: usize,
    capacity: usize,
}

impl SubscriptionArrowBatcher {
    /// Create a batcher sized for the one-update adapter path.
    pub fn new() -> Self {
        Self::with_capacity(1)
    }

    /// Create a batcher with a bounded expected row count per flush.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            layout: None,
            schema: None,
            builders: Vec::new(),
            scratch: Vec::new(),
            presence_scratch: Vec::new(),
            rows: 0,
            capacity,
        }
    }

    /// Append one update, flushing pending rows first when the layout changes.
    pub fn append(&mut self, update: &SubscriptionUpdate) -> Option<RecordBatch> {
        if !self.matches_layout(&update.layout) {
            let batch = self.flush();
            self.rebuild_for_layout(update.layout.clone());
            self.append_current_layout(update);
            return batch;
        }

        self.append_current_layout(update);
        None
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Build a batch from pending rows.
    ///
    /// Finishing transfers the builders' buffers to the returned arrays. The
    /// schema and layout remain cached, while builders are recreated lazily
    /// with the configured capacity before the next append.
    pub fn flush(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }

        let schema = self
            .schema
            .as_ref()
            .expect("subscription arrow schema initialized")
            .clone();
        let columns: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(SubscriptionColumnBuilder::finish)
            .collect();
        self.builders.clear();
        self.rows = 0;

        Some(
            RecordBatch::try_new(schema, columns)
                .expect("subscription arrow builders must match cached schema"),
        )
    }

    fn matches_layout(&self, layout: &Arc<FieldLayout>) -> bool {
        self.layout.as_ref().is_some_and(|current| {
            Arc::ptr_eq(current, layout)
                || (current.version == layout.version
                    && current.fields.len() == layout.fields.len()
                    && current.fields.iter().zip(layout.fields.iter()).all(
                        |(current_field, next_field)| {
                            current_field.index == next_field.index
                                && current_field.kind == next_field.kind
                                && current_field.name == next_field.name
                        },
                    ))
        })
    }

    fn rebuild_for_layout(&mut self, layout: Arc<FieldLayout>) {
        let mut fields = Vec::with_capacity(layout.fields.len() + 3);
        fields.push(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        fields.push(Field::new("topic", DataType::Utf8, false));
        fields.extend(
            layout
                .fields
                .iter()
                .map(|meta| Field::new(meta.name.as_ref(), arrow_datatype(meta.kind), true)),
        );
        fields.push(Field::new(PRESENCE_COLUMN_NAME, DataType::Binary, false));

        let builders = subscription_builders(&layout, self.capacity);

        self.layout = Some(layout);
        self.schema = Some(Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([(
                PRESENCE_METADATA_KEY.to_string(),
                PRESENCE_METADATA_VALUE.to_string(),
            )]),
        )));
        self.builders = builders;
        self.scratch.clear();
        self.presence_scratch.clear();
    }

    fn append_current_layout(&mut self, update: &SubscriptionUpdate) {
        let layout = self
            .layout
            .as_ref()
            .expect("subscription arrow layout initialized")
            .clone();

        if self.builders.is_empty() {
            self.builders = subscription_builders(&layout, self.capacity);
        }

        self.scratch.clear();
        self.scratch.resize(layout.fields.len(), None);
        self.presence_scratch.clear();
        self.presence_scratch
            .resize(layout.fields.len().div_ceil(8), 0);
        for (position, field) in update.values.iter().enumerate() {
            if let Some(slot) = self.scratch.get_mut(field.index as usize) {
                *slot = Some(position);
            }
        }

        self.builders[0].append_timestamp(update.timestamp_us);
        self.builders[1].append_topic(update.topic.as_ref());

        for (position, meta) in layout.fields.iter().enumerate() {
            let value = self
                .scratch
                .get(meta.index as usize)
                .and_then(|slot| slot.map(|value_position| &update.values[value_position].value));
            if value.is_some() {
                self.presence_scratch[position / 8] |= 1u8 << (position % 8);
            }
            self.builders[position + 2].append_update(value);
        }
        self.builders[layout.fields.len() + 2].append_presence(&self.presence_scratch);

        self.rows += 1;
    }
}

fn subscription_builders(layout: &FieldLayout, capacity: usize) -> Vec<SubscriptionColumnBuilder> {
    let mut builders = Vec::with_capacity(layout.fields.len() + 3);
    builders.push(SubscriptionColumnBuilder::Timestamp(
        TimestampMicrosecondBuilder::with_capacity(capacity),
    ));
    builders.push(SubscriptionColumnBuilder::Topic(
        StringBuilder::with_capacity(capacity, capacity),
    ));
    builders.extend(
        layout
            .fields
            .iter()
            .map(|meta| SubscriptionColumnBuilder::for_kind(meta.kind, capacity)),
    );
    let bitmap_bytes = layout.fields.len().div_ceil(8);
    builders.push(SubscriptionColumnBuilder::Presence(
        BinaryBuilder::with_capacity(capacity, capacity.saturating_mul(bitmap_bytes)),
    ));
    builders
}

impl Default for SubscriptionArrowBatcher {
    fn default() -> Self {
        Self::new()
    }
}

pub fn subscription_update_to_record_batch(
    update: &SubscriptionUpdate,
) -> Result<RecordBatch, BlpError> {
    let mut batcher = SubscriptionArrowBatcher::new();
    batcher.append(update);
    Ok(batcher
        .flush()
        .expect("subscription update batcher should flush appended row"))
}

fn arrow_datatype(kind: FieldKind) -> DataType {
    match kind {
        FieldKind::Unknown | FieldKind::Str => DataType::Utf8,
        FieldKind::Bool => DataType::Boolean,
        FieldKind::I32 => DataType::Int32,
        FieldKind::I64 => DataType::Int64,
        FieldKind::F64 => DataType::Float64,
        FieldKind::Date32 => DataType::Date32,
        FieldKind::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
        FieldKind::TimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
    }
}

enum SubscriptionColumnBuilder {
    Timestamp(TimestampMicrosecondBuilder),
    Topic(StringBuilder),
    Presence(BinaryBuilder),
    Bool(BooleanBuilder),
    I32(Int32Builder),
    I64(Int64Builder),
    F64(Float64Builder),
    String(StringBuilder),
    Date32(Date32Builder),
    Time64Micros(Time64MicrosecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
}

impl SubscriptionColumnBuilder {
    fn for_kind(kind: FieldKind, capacity: usize) -> Self {
        match kind {
            FieldKind::Unknown | FieldKind::Str => {
                Self::String(StringBuilder::with_capacity(capacity, capacity))
            }
            FieldKind::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            FieldKind::I32 => Self::I32(Int32Builder::with_capacity(capacity)),
            FieldKind::I64 => Self::I64(Int64Builder::with_capacity(capacity)),
            FieldKind::F64 => Self::F64(Float64Builder::with_capacity(capacity)),
            FieldKind::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            FieldKind::Time64Micros => {
                Self::Time64Micros(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            FieldKind::TimestampMicros => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
        }
    }

    fn append_timestamp(&mut self, value: i64) {
        match self {
            Self::Timestamp(builder) => builder.append_value(value),
            _ => unreachable!("timestamp append used with non-timestamp builder"),
        }
    }

    fn append_topic(&mut self, value: &str) {
        match self {
            Self::Topic(builder) => builder.append_value(value),
            _ => unreachable!("topic append used with non-topic builder"),
        }
    }

    fn append_presence(&mut self, value: &[u8]) {
        match self {
            Self::Presence(builder) => builder.append_value(value),
            _ => unreachable!("presence append used with non-presence builder"),
        }
    }

    fn append_update(&mut self, value: Option<&UpdateValue>) {
        match self {
            Self::Timestamp(_) | Self::Topic(_) | Self::Presence(_) => {
                unreachable!("fixed subscription columns are appended separately")
            }
            Self::Bool(builder) => match value {
                Some(UpdateValue::Bool(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::I32(builder) => match value {
                Some(UpdateValue::I32(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::I64(builder) => match value {
                Some(UpdateValue::I64(value)) => builder.append_value(*value),
                Some(UpdateValue::I32(value)) => builder.append_value(*value as i64),
                _ => builder.append_null(),
            },
            Self::F64(builder) => match value {
                Some(UpdateValue::F64(value)) => builder.append_value(*value),
                Some(UpdateValue::I32(value)) => builder.append_value(*value as f64),
                Some(UpdateValue::I64(value)) => builder.append_value(*value as f64),
                _ => builder.append_null(),
            },
            Self::String(builder) => append_string_value(builder, value),
            Self::Date32(builder) => match value {
                Some(UpdateValue::Date32(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::Time64Micros(builder) => match value {
                Some(UpdateValue::Time64Micros(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::TimestampMicros(builder) => match value {
                Some(UpdateValue::TimestampMicros(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Timestamp(builder) => Arc::new(builder.finish().with_timezone("UTC")),
            Self::Topic(builder) => Arc::new(builder.finish()),
            Self::Presence(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::Time64Micros(builder) => Arc::new(builder.finish()),
            Self::TimestampMicros(builder) => Arc::new(builder.finish().with_timezone("UTC")),
        }
    }
}

fn append_string_value(builder: &mut StringBuilder, value: Option<&UpdateValue>) {
    match value {
        Some(UpdateValue::Bool(value)) => {
            builder.append_value(if *value { "true" } else { "false" })
        }
        Some(UpdateValue::I32(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::I64(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::F64(value)) if value.is_finite() => {
            let mut buffer = ryu::Buffer::new();
            builder.append_value(buffer.format_finite(*value));
        }
        Some(UpdateValue::F64(value)) => {
            let value = value.to_string();
            builder.append_value(&value);
        }
        Some(UpdateValue::Str(value)) => builder.append_value(value.as_ref()),
        Some(UpdateValue::Date32(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::Time64Micros(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::TimestampMicros(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::Null) | None => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::update::{FieldMeta, UpdateField};
    use arrow_array::{
        Array, BinaryArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };

    fn layout(version: u32, fields: Vec<FieldMeta>) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(version, fields))
    }

    fn update(
        timestamp_us: i64,
        topic: &str,
        layout: Arc<FieldLayout>,
        values: impl IntoIterator<Item = UpdateField>,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from(topic),
            layout,
            values: values.into_iter().collect(),
        }
    }

    fn update_field(index: u16, value: UpdateValue) -> UpdateField {
        UpdateField { index, value }
    }

    fn presence_bitmap(batch: &RecordBatch) -> &BinaryArray {
        batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("trailing presence bitmap")
    }

    #[test]
    fn schema_declares_trailing_non_null_presence_bitmap() {
        let layout = layout(
            1,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK_SIZE", 1, FieldKind::I32),
                FieldMeta::new("OPEN", 2, FieldKind::Date32),
                FieldMeta::new("ACTIVE", 3, FieldKind::Bool),
                FieldMeta::new("LAST_UPDATE", 4, FieldKind::TimestampMicros),
            ],
        );
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [
                update_field(0, UpdateValue::F64(1.25)),
                update_field(1, UpdateValue::I32(100)),
                update_field(2, UpdateValue::Date32(20_000)),
                update_field(3, UpdateValue::Bool(true)),
                update_field(4, UpdateValue::TimestampMicros(11)),
            ],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();
        let schema = batch.schema();
        let presence = schema.field(schema.fields().len() - 1);

        assert_eq!(presence.name(), PRESENCE_COLUMN_NAME);
        assert_eq!(presence.data_type(), &DataType::Binary);
        assert!(!presence.is_nullable());
        assert_eq!(
            schema
                .metadata()
                .get(PRESENCE_METADATA_KEY)
                .map(String::as_str),
            Some(PRESENCE_METADATA_VALUE)
        );
    }

    #[test]
    fn multi_append_flush_preserves_rows_and_sparse_nulls() {
        let layout = layout(
            1,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK", 1, FieldKind::F64),
                FieldMeta::new("STATUS", 2, FieldKind::Str),
            ],
        );
        let mut batcher = SubscriptionArrowBatcher::with_capacity(2);

        assert!(batcher
            .append(&update(
                10,
                "IBM US Equity",
                layout.clone(),
                [
                    update_field(0, UpdateValue::F64(1.25)),
                    update_field(2, UpdateValue::Str(Arc::from("OK"))),
                ],
            ))
            .is_none());
        assert!(batcher
            .append(&update(
                20,
                "MSFT US Equity",
                layout,
                [update_field(1, UpdateValue::F64(2.5))],
            ))
            .is_none());

        let batch = batcher.flush().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 10);
        assert_eq!(timestamps.value(1), 20);
        let topics = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(topics.value(0), "IBM US Equity");
        assert_eq!(topics.value(1), "MSFT US Equity");
        let bid = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(bid.value(0), 1.25);
        assert!(bid.is_null(1));
        let ask = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(ask.is_null(0));
        assert_eq!(ask.value(1), 2.5);
        let status = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "OK");
        assert!(status.is_null(1));
        let presence = presence_bitmap(&batch);
        assert_eq!(presence.value(0), &[0b0000_0101]);
        assert_eq!(presence.value(1), &[0b0000_0010]);
    }

    #[test]
    fn explicit_null_and_absence_have_distinct_presence_bits() {
        let layout = layout(
            1,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK", 1, FieldKind::F64),
            ],
        );
        let mut batcher = SubscriptionArrowBatcher::with_capacity(2);
        batcher.append(&update(
            10,
            "IBM US Equity",
            layout.clone(),
            [update_field(0, UpdateValue::Null)],
        ));
        batcher.append(&update(
            20,
            "IBM US Equity",
            layout,
            [update_field(1, UpdateValue::Null)],
        ));

        let batch = batcher.flush().unwrap();
        for column in 2..=3 {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(values.is_null(0));
            assert!(values.is_null(1));
        }
        let presence = presence_bitmap(&batch);
        assert_eq!(presence.null_count(), 0);
        assert_eq!(presence.value(0), &[0b0000_0001]);
        assert_eq!(presence.value(1), &[0b0000_0010]);
    }

    #[test]
    fn presence_bitmap_spans_bytes_by_layout_ordinal() {
        let fields = (0..10)
            .map(|ordinal| {
                FieldMeta::new(
                    format!("FIELD_{ordinal}"),
                    (9 - ordinal) as u16,
                    FieldKind::F64,
                )
            })
            .collect();
        let layout = layout(1, fields);
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [
                update_field(9, UpdateValue::Null),
                update_field(2, UpdateValue::Null),
                update_field(1, UpdateValue::Null),
                update_field(0, UpdateValue::Null),
            ],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();
        assert_eq!(
            presence_bitmap(&batch).value(0),
            &[0b1000_0001, 0b0000_0011]
        );
    }

    #[test]
    fn layout_change_flushes_old_rows_and_keeps_new_layout_pending() {
        let first_layout = layout(1, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let second_layout = layout(2, vec![FieldMeta::new("ASK_SIZE", 0, FieldKind::I32)]);
        let mut batcher = SubscriptionArrowBatcher::new();

        assert!(batcher
            .append(&update(
                10,
                "IBM US Equity",
                first_layout,
                [update_field(0, UpdateValue::F64(1.25))],
            ))
            .is_none());
        let old_batch = batcher
            .append(&update(
                20,
                "IBM US Equity",
                second_layout,
                [update_field(0, UpdateValue::I32(100))],
            ))
            .unwrap();

        assert_eq!(old_batch.num_rows(), 1);
        assert_eq!(old_batch.schema().field(2).name(), "BID");
        assert_eq!(
            old_batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.25
        );
        assert_eq!(batcher.rows(), 1);

        let new_batch = batcher.flush().unwrap();
        assert_eq!(new_batch.num_rows(), 1);
        assert_eq!(new_batch.schema().field(2).name(), "ASK_SIZE");
        assert_eq!(
            new_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            100
        );
    }

    #[test]
    fn presence_bitmap_resets_width_and_bits_after_layout_change() {
        let first_layout = layout(
            1,
            (0..9)
                .map(|index| FieldMeta::new(format!("OLD_{index}"), index as u16, FieldKind::F64))
                .collect(),
        );
        let second_layout = layout(
            2,
            vec![
                FieldMeta::new("NEW_0", 0, FieldKind::F64),
                FieldMeta::new("NEW_1", 1, FieldKind::F64),
            ],
        );
        let mut batcher = SubscriptionArrowBatcher::new();
        batcher.append(&update(
            10,
            "IBM US Equity",
            first_layout,
            [update_field(8, UpdateValue::Null)],
        ));

        let old_batch = batcher
            .append(&update(
                20,
                "IBM US Equity",
                second_layout,
                [update_field(1, UpdateValue::Null)],
            ))
            .expect("layout change flushes old bitmap width");
        assert_eq!(presence_bitmap(&old_batch).value(0), &[0, 1]);

        let new_batch = batcher.flush().unwrap();
        assert_eq!(presence_bitmap(&new_batch).value(0), &[0b0000_0010]);
    }

    #[test]
    fn same_version_layout_change_flushes_and_preserves_string_promotion() {
        let bid_layout = layout(2, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let text_bid_layout = layout(2, vec![FieldMeta::new("BID", 0, FieldKind::Str)]);
        let mut batcher = SubscriptionArrowBatcher::with_capacity(2);

        assert!(batcher
            .append(&update(
                10,
                "IBM US Equity",
                bid_layout,
                [update_field(0, UpdateValue::F64(1.25))],
            ))
            .is_none());
        let bid_batch = batcher
            .append(&update(
                20,
                "MSFT US Equity",
                text_bid_layout,
                [update_field(0, UpdateValue::Bool(true))],
            ))
            .expect("a structurally different layout must flush pending rows");

        assert_eq!(bid_batch.schema().field(2).name(), "BID");
        assert_eq!(
            bid_batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.25
        );

        let status_batch = batcher.flush().unwrap();
        assert_eq!(status_batch.schema().field(2).name(), "BID");
        assert_eq!(status_batch.schema().field(2).data_type(), &DataType::Utf8);
        assert_eq!(
            status_batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "true"
        );
    }

    #[test]
    fn i64_values_do_not_wrap_into_i32_columns() {
        let layout = layout(1, vec![FieldMeta::new("SIZE", 0, FieldKind::I32)]);
        let mut batcher = SubscriptionArrowBatcher::with_capacity(2);
        for value in [i64::from(i32::MAX) + 1, i64::from(i32::MIN) - 1] {
            assert!(batcher
                .append(&update(
                    value,
                    "IBM US Equity",
                    layout.clone(),
                    [update_field(0, UpdateValue::I64(value))],
                ))
                .is_none());
        }

        let batch = batcher.flush().unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.is_null(0));
        assert!(values.is_null(1));
    }

    #[test]
    fn appending_after_flush_recreates_capacity_bounded_builders() {
        let layout = layout(1, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let mut batcher = SubscriptionArrowBatcher::new();

        batcher.append(&update(
            10,
            "IBM US Equity",
            layout.clone(),
            [update_field(0, UpdateValue::F64(1.25))],
        ));
        let first = batcher.flush().unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.25
        );
        assert!(batcher.is_empty());

        batcher.append(&update(
            20,
            "IBM US Equity",
            layout,
            [update_field(0, UpdateValue::F64(2.5))],
        ));
        let second = batcher.flush().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert_eq!(
            second
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.5
        );
    }

    #[test]
    fn one_row_adapter_does_not_retain_bulk_builder_capacity() {
        let layout = layout(1, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [update_field(0, UpdateValue::F64(1.25))],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();

        assert!(batch.column(0).get_buffer_memory_size() < 1024);
        assert!(batch.column(2).get_buffer_memory_size() < 1024);
        assert!(
            batch
                .column(batch.num_columns() - 1)
                .get_buffer_memory_size()
                < 1024
        );
    }

    #[test]
    fn arrow_adapter_null_fills_sparse_layout() {
        let layout = layout(
            2,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK", 1, FieldKind::F64),
            ],
        );
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [update_field(0, UpdateValue::F64(1.25))],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(2).name(), "BID");
        assert_eq!(batch.schema().field(3).name(), "ASK");
        let ask = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(ask.is_null(0));
        assert_eq!(presence_bitmap(&batch).value(0), &[0b0000_0001]);
    }
}
