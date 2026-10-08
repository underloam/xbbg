use std::sync::Arc;

use arrow_array::RecordBatch;
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionArrowBatcher, SubscriptionUpdate, UpdateField,
    UpdateValue,
};

pub(super) const BATCH_SIZE: usize = 1_024;

pub(super) fn batch_updates(
    messages: usize,
    topic_count: usize,
    field_count: usize,
    mut on_batch: impl FnMut(RecordBatch),
) {
    let topics: Vec<Arc<str>> = (0..topic_count)
        .map(|topic| Arc::from(format!("SYN{topic:05} US Equity")))
        .collect();
    let layout = Arc::new(FieldLayout::new(
        1,
        (0..field_count)
            .map(|index| FieldMeta::new(format!("field_{index}"), index as _, FieldKind::F64))
            .collect(),
    ));
    let mut update = SubscriptionUpdate {
        timestamp_us: 0,
        topic_id: 0,
        topic: topics[0].clone(),
        layout,
        values: (0..field_count)
            .map(|index| UpdateField {
                index: index as _,
                value: UpdateValue::F64(0.0),
            })
            .collect(),
    };
    let mut batcher = SubscriptionArrowBatcher::with_capacity(BATCH_SIZE);
    for row in 0..messages {
        let topic = row % topic_count;
        update.timestamp_us = 1_700_000_000_000_000 + row as i64 * 250;
        update.topic_id = topic as _;
        update.topic = topics[topic].clone();
        for field in &mut update.values {
            field.value =
                UpdateValue::F64(((topic + field.index as usize + row) % 10_000) as f64 * 0.0001);
        }
        if let Some(batch) = batcher.append(&update) {
            on_batch(batch);
        }
        if batcher.rows() >= BATCH_SIZE {
            on_batch(
                batcher
                    .flush()
                    .expect("synthetic batch contains pending rows"),
            );
        }
    }
    if let Some(batch) = batcher.flush() {
        on_batch(batch);
    }
}
