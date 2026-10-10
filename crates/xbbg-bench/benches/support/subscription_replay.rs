use std::sync::Arc;

use arrow_array::RecordBatch;
use xbbg_async::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionArrowBatcher, SubscriptionUpdate, UpdateField,
    UpdateValue,
};

const TOPICS: [&str; 8] = [
    "IBM US Equity",
    "MSFT US Equity",
    "AAPL US Equity",
    "NVDA US Equity",
    "ES1 Index",
    "TY1 Comdty",
    "EURUSD Curncy",
    "SPX Index",
];
pub(super) const REQUESTED_FIELDS: [(&str, FieldKind); 8] = [
    ("LAST_PRICE", FieldKind::F64),
    ("BID", FieldKind::F64),
    ("ASK", FieldKind::F64),
    ("BID_SIZE", FieldKind::I32),
    ("ASK_SIZE", FieldKind::I64),
    ("IS_DELAYED_STREAM", FieldKind::Bool),
    ("TRADE_TIME", FieldKind::TimestampMicros),
    ("CONDITION_CODE", FieldKind::Str),
];
pub(super) const LATE_FIELDS: [(&str, FieldKind); 4] = [
    ("RT_PX_CHG_NET_1D", FieldKind::F64),
    ("VOLUME", FieldKind::I64),
    ("MKTDATA_EVENT_TYPE", FieldKind::Str),
    ("MKTDATA_EVENT_SUBTYPE", FieldKind::Str),
];

pub(super) fn replay(rows: usize, flush_threshold: usize, mut on_batch: impl FnMut(RecordBatch)) {
    let fields = REQUESTED_FIELDS
        .iter()
        .chain(LATE_FIELDS.iter())
        .enumerate()
        .map(|(index, &(name, kind))| FieldMeta::new(name, index as _, kind))
        .collect::<Vec<_>>();
    let requested_layout = Arc::new(FieldLayout::new(
        1,
        fields[..REQUESTED_FIELDS.len()].to_vec(),
    ));
    let expanded_layout = Arc::new(FieldLayout::new(2, fields));
    let topics = TOPICS.map(Arc::<str>::from);
    let strings = ["OPEN", "CLOSE", "REGULAR", "TRADE", "SUMMARY"].map(Arc::<str>::from);
    let mut update = SubscriptionUpdate {
        timestamp_us: 0,
        topic_id: 0,
        topic: topics[0].clone(),
        layout: requested_layout,
        values: Default::default(),
    };
    update
        .values
        .reserve(REQUESTED_FIELDS.len() + LATE_FIELDS.len());
    let mut batcher = SubscriptionArrowBatcher::with_capacity(flush_threshold);

    for row in 0..rows {
        // A changed layout flushes the accepted prefix in the production batcher.
        if row == flush_threshold / 2 {
            update.layout = expanded_layout.clone();
        }
        let topic = row % topics.len();
        update.timestamp_us = 1_700_000_000_000_000_i64 + row as i64 * 250;
        update.topic_id = topic as _;
        update.topic = topics[topic].clone();
        update.values.clear();
        for index in 0..update.layout.fields.len() {
            if let Some(value) = synthetic_value(index, row, &strings) {
                update.values.push(UpdateField {
                    index: index as _,
                    value,
                });
            }
        }
        if let Some(batch) = batcher.append(&update) {
            on_batch(batch);
        }
        if batcher.rows() >= flush_threshold {
            on_batch(batcher.flush().expect("replay batch contains pending rows"));
        }
    }
    if let Some(batch) = batcher.flush() {
        on_batch(batch);
    }
}

fn synthetic_value(index: usize, row: usize, strings: &[Arc<str>; 5]) -> Option<UpdateValue> {
    match index {
        0 => (!row.is_multiple_of(17))
            .then_some(UpdateValue::F64(100.0 + (row % 10_000) as f64 * 0.01)),
        1 => (!row.is_multiple_of(11))
            .then_some(UpdateValue::F64(99.95 + (row % 10_000) as f64 * 0.01)),
        2 => (!row.is_multiple_of(13))
            .then_some(UpdateValue::F64(100.05 + (row % 10_000) as f64 * 0.01)),
        3 => (!row.is_multiple_of(5)).then_some(UpdateValue::I32((row % 1_000) as i32 + 1)),
        4 => (!row.is_multiple_of(7)).then_some(UpdateValue::I64((row % 2_000) as i64 + 1)),
        5 => Some(UpdateValue::Bool(row.is_multiple_of(19))),
        6 => (!row.is_multiple_of(23)).then_some(UpdateValue::TimestampMicros(
            1_700_000_000_000_000_i64 + row as i64 * 250,
        )),
        7 => match row % 9 {
            0 => None,
            1 => Some(UpdateValue::Str(strings[0].clone())),
            2 => Some(UpdateValue::Str(strings[1].clone())),
            _ => Some(UpdateValue::Str(strings[2].clone())),
        },
        8 => (!row.is_multiple_of(3)).then_some(UpdateValue::F64((row as f64 % 200.0) - 100.0)),
        9 => (!row.is_multiple_of(4)).then_some(UpdateValue::I64(1_000_000 + row as i64 * 10)),
        10 => (!row.is_multiple_of(6)).then(|| UpdateValue::Str(strings[3].clone())),
        11 => (!row.is_multiple_of(10)).then(|| UpdateValue::Str(strings[4].clone())),
        _ => unreachable!("replay field index must belong to the fixture layout"),
    }
}
