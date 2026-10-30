use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, Time64MicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use serde_json::{Value, json};
use xbbg_async::engine::state::{SubscriptionUpdate, UpdateField, UpdateValue};
use xbbg_async::subscription_consumer::SubscriptionConsumer;
use xbbg_async::testing::SubscriptionFixture;
use xbbg_async::{EngineConfig, SubscribeRequest, SubscriptionDeliveryMode};

use super::delivery_common::{Config, FIELDS, Payloads, WAIT, distribution, ns};

fn request(topics: Vec<String>, mode: SubscriptionDeliveryMode, limit: usize) -> SubscribeRequest {
    // Capacity/overflow are engine defaults, not explicit state-mode options.
    SubscribeRequest {
        topics,
        fields: FIELDS.into_iter().map(String::from).collect(),
        delivery: mode,
        flush_threshold: Some(limit),
        ..SubscribeRequest::default()
    }
}

fn value(batch: &RecordBatch, field: &str, row: usize) -> UpdateValue {
    let array = batch
        .column_by_name(field)
        .unwrap_or_else(|| panic!("missing field {field}"));
    if array.is_null(row) {
        return UpdateValue::Null;
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        UpdateValue::I64(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        UpdateValue::I32(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        UpdateValue::F64(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        UpdateValue::Bool(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        UpdateValue::Str(Arc::from(array.value(row)))
    } else if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
        UpdateValue::Date32(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Time64MicrosecondArray>() {
        UpdateValue::Time64Micros(array.value(row))
    } else {
        panic!("unexpected Arrow type for {field}: {:?}", array.data_type());
    }
}

fn topic_index(batch: &RecordBatch, row: usize, payloads: &Payloads) -> usize {
    let topics = batch
        .column_by_name("topic")
        .expect("topic")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string topic");
    payloads
        .topics
        .iter()
        .position(|topic| topic.as_ref() == topics.value(row))
        .expect("known synthetic topic")
}

fn assert_image_values(batch: &RecordBatch, row: usize, expected: &[UpdateValue]) {
    for (field, expected) in FIELDS.into_iter().zip(expected) {
        assert_eq!(&value(batch, field, row), expected, "state field {field}");
    }
}

fn assert_state_delivery_schema(batch: &RecordBatch) {
    let schema = batch.schema();
    let metadata = [
        ("topic", DataType::Utf8, false),
        (
            "last_update",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        ("live", DataType::Boolean, false),
        ("delayed", DataType::Boolean, true),
        ("valid", DataType::Boolean, false),
    ];
    for (name, data_type, nullable) in metadata {
        let field = schema
            .field_with_name(name)
            .expect("mandatory state metadata field");
        assert_eq!(
            field.data_type(),
            &data_type,
            "state metadata type for {name}"
        );
        assert_eq!(
            field.is_nullable(),
            nullable,
            "state metadata nullability for {name}"
        );
        if !nullable {
            assert_eq!(
                batch
                    .column_by_name(name)
                    .expect("state metadata array")
                    .null_count(),
                0
            );
        }
    }
    let valid = batch
        .column_by_name("valid")
        .expect("mandatory state valid array")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool state valid");
    assert_eq!(valid.null_count(), 0);
    assert!(
        (0..batch.num_rows()).all(|row| valid.value(row)),
        "prefilled state images must be valid"
    );
}

async fn read(
    consumer: &SubscriptionConsumer,
    limit: usize,
) -> (xbbg_async::SubscriptionDelivery, u64) {
    let started = Instant::now();
    let delivery = tokio::time::timeout(WAIT, consumer.next_delivery(limit))
        .await
        .expect("native replay read deadline")
        .expect("native replay delivery")
        .commit()
        .expect("native replay success");
    let elapsed = ns(started);
    assert!(delivery.data.num_rows() > 0);
    assert!(
        delivery.gap_before.is_none(),
        "strict delta replay is sized to avoid overflow; state has no local queue gap"
    );
    if delivery.kind == SubscriptionDeliveryMode::State {
        assert_state_delivery_schema(&delivery.data);
    }
    (delivery, elapsed)
}

fn update_field<'a>(update: &'a SubscriptionUpdate, field: &str) -> Option<&'a UpdateValue> {
    update
        .values
        .iter()
        .find(|entry| update.layout.fields[entry.index as usize].name.as_ref() == field)
        .map(|entry| &entry.value)
}

// A real SDK TestUtil/CID-dispatch/decode preflight. No synthetic layout or
// replacement decoder is used here; unlike replay timing, this covers decode.
async fn sdk_preflight() -> Value {
    let fixture = SubscriptionFixture::new(EngineConfig::default());
    let topic = "SYNTH_SDK_SPARSE";
    let fields: Vec<String> = ["PRICE", "SIZE", "ACTIVE", "CONDITION"]
        .into_iter()
        .map(String::from)
        .collect();
    let request = SubscribeRequest {
        topics: vec![topic.into()],
        fields: fields.clone(),
        ..SubscribeRequest::default()
    };
    let mut deltas = fixture
        .subscribe(request.clone())
        .await
        .expect("SDK delta subscription");
    let state = fixture
        .subscribe(SubscribeRequest {
            delivery: SubscriptionDeliveryMode::State,
            ..request
        })
        .await
        .expect("SDK state subscription");
    let (rx, handle) = state.into_parts();
    let state = SubscriptionConsumer::new(rx, handle);
    fixture
        .publish_json(
            topic,
            r#"{"PRICE":0.125,"SIZE":2,"ACTIVE":true,"CONDITION":"SYNTHETIC"}"#,
        )
        .expect("SDK full data");
    let first = deltas
        .try_next()
        .expect("decoded full update")
        .expect("decoded data");
    assert_eq!(
        update_field(&first, "PRICE"),
        Some(&UpdateValue::F64(0.125))
    );
    assert_eq!(
        update_field(&first, "ACTIVE"),
        Some(&UpdateValue::Bool(true))
    );
    assert!(
        matches!(update_field(&first, "CONDITION"), Some(UpdateValue::Str(text)) if text.as_ref() == "SYNTHETIC")
    );
    let (initial, _) = read(&state, 1).await;
    assert_eq!(value(&initial.data, "PRICE", 0), UpdateValue::F64(0.125));
    fixture
        .publish_json(topic, r#"{"PRICE":null}"#)
        .expect("SDK explicit null");
    let cleared = deltas
        .try_next()
        .expect("decoded clear")
        .expect("decoded clear data");
    assert_eq!(update_field(&cleared, "PRICE"), Some(&UpdateValue::Null));
    assert!(
        update_field(&cleared, "SIZE").is_none(),
        "absent is not a clear"
    );
    fixture
        .publish_json(topic, r#"{"SIZE":9}"#)
        .expect("SDK sparse update");
    let sparse = deltas
        .try_next()
        .expect("decoded sparse")
        .expect("decoded sparse data");
    assert!(update_field(&sparse, "PRICE").is_none());
    assert_eq!(update_field(&sparse, "SIZE"), Some(&UpdateValue::F64(9.0)));
    let (image, _) = read(&state, 1).await;
    assert_eq!(value(&image.data, "PRICE", 0), UpdateValue::Null);
    assert_eq!(value(&image.data, "SIZE", 0), UpdateValue::F64(9.0));
    assert_eq!(value(&image.data, "ACTIVE", 0), UpdateValue::Bool(true));
    assert!(
        matches!(value(&image.data, "CONDITION", 0), UpdateValue::Str(text) if text.as_ref() == "SYNTHETIC")
    );
    state
        .handle()
        .expect("state handle")
        .unsubscribe()
        .await
        .expect("SDK state cleanup");
    deltas.unsubscribe(false).await.expect("SDK delta cleanup");
    drop(state);
    fixture.settle().await;
    json!({
        "passed": true,
        "scope": "existing SDK-backed TestSubscriptionSession.data/TestUtil actual CID dispatch and source decode; Float64/bool/string/null; sparse absence stays absent, explicit null remains cleared after a later omitted-field update; actual State receiver materializes retained values",
        "source_layout_versions": [first.layout.version, cleared.layout.version, sparse.layout.version],
        "source_layout_reused_for_sparse": Arc::ptr_eq(&cleared.layout, &sparse.layout),
        "timed": false,
    })
}

struct Plan {
    payloads: Payloads,
    initial: Vec<SubscriptionUpdate>,
    updates: Vec<SubscriptionUpdate>,
}

impl Plan {
    fn new(config: &Config) -> Self {
        let payloads = Payloads::new(config.topics);
        let initial: Vec<_> = (0..config.topics)
            .map(|topic| payloads.full(topic + 1, topic))
            .collect();
        let updates = (0..config.replay_updates)
            .map(|index| {
                let topic = if (index + 1) % config.quiet_every == 0 {
                    1 + (index / config.quiet_every) % (config.topics - 1)
                } else {
                    0
                };
                let sequence = index + config.topics + 1;
                let mut update = payloads.full(sequence, topic);
                // Keep the stable *full* layout, but only publish two fields. The
                // untimed full constructor also prebuilds the owned string needed
                // for the condition-changing case; none is allocated while timing.
                let changed = match index % 4 {
                    0 => UpdateField {
                        index: 1,
                        value: UpdateValue::F64(sequence as f64 * 0.001),
                    },
                    1 => UpdateField {
                        index: 1,
                        value: UpdateValue::Null,
                    },
                    2 => UpdateField {
                        index: 2,
                        value: UpdateValue::I32((sequence % 10_000) as i32),
                    },
                    3 => update.values[5].clone(),
                    _ => unreachable!(),
                };
                update.values.clear();
                update.values.push(UpdateField {
                    index: 0,
                    value: UpdateValue::I64(sequence as i64),
                });
                update.values.push(changed);
                update
            })
            .collect();
        Self {
            payloads,
            initial,
            updates,
        }
    }
}

struct ReplayResult {
    json: Value,
    producer_latencies: Vec<u64>,
    materialization_latencies: Vec<u64>,
    rows: usize,
    checksum: u64,
}

async fn iteration(
    mode: SubscriptionDeliveryMode,
    config: &Config,
    plan: &Plan,
    sample: usize,
) -> ReplayResult {
    let limit = config.read_every.max(config.topics);
    let fixture = SubscriptionFixture::new(EngineConfig {
        subscription_stream_capacity: config.read_every + config.topics,
        subscription_flush_threshold: limit,
        ..EngineConfig::default()
    });
    let stream = fixture
        .subscribe(request(
            plan.payloads
                .topics
                .iter()
                .map(|topic| topic.to_string())
                .collect(),
            mode,
            limit,
        ))
        .await
        .expect("replay subscription");
    let (rx, handle) = stream.into_parts();
    let consumer = SubscriptionConsumer::new(rx, handle);
    let feeds: Vec<_> = plan
        .payloads
        .topics
        .iter()
        .map(|topic| fixture.feed(topic).expect("actual subscribed feed"))
        .collect();
    // Setup images and drain subscription-time blank/initial dirty markers by
    // reading the actual receiver, never by clearing internals or replacing it.
    for update in &plan.initial {
        feeds[update.topic_id as usize].publish(update);
    }
    let mut initial_rows = 0;
    while initial_rows < config.topics {
        let (delivery, _) = read(&consumer, limit).await;
        initial_rows += delivery.data.num_rows();
    }
    assert_eq!(initial_rows, config.topics);
    // This is only a correctness oracle outside all reported timing, not a
    // substitute state implementation on the path being measured.
    let mut expected: Vec<Vec<UpdateValue>> = plan
        .initial
        .iter()
        .map(|update| {
            update
                .values
                .iter()
                .map(|field| field.value.clone())
                .collect()
        })
        .collect();
    let mut rows = 0;
    let mut batches = 0;
    let mut producer_ns = 0;
    let mut materialization_ns = 0;
    let mut output_drop_ns = 0;
    let mut producer_latencies =
        Vec::with_capacity(config.replay_updates.div_ceil(config.sample_every));
    let mut materialization_latencies =
        Vec::with_capacity(config.replay_updates.div_ceil(config.read_every));
    let mut topic_rows = vec![0usize; config.topics];
    let mut expected_state_rows = 0;
    let mut delta_checksum = 0u64;
    let mut published = 0;
    for chunk in plan.updates.chunks(config.read_every) {
        let started = Instant::now();
        for (index, update) in chunk.iter().enumerate() {
            let feed = &feeds[update.topic_id as usize];
            if (published + index + sample).is_multiple_of(config.sample_every) {
                let started = Instant::now();
                feed.publish(black_box(update));
                producer_latencies.push(ns(started));
            } else {
                feed.publish(black_box(update));
            }
        }
        producer_ns += ns(started);
        published += chunk.len();
        let mut dirty = vec![false; config.topics];
        for update in chunk {
            let topic = update.topic_id as usize;
            dirty[topic] = true;
            for field in &update.values {
                expected[topic][field.index as usize] = field.value.clone();
            }
        }
        let dirty_count = dirty.iter().filter(|&&dirty| dirty).count();
        expected_state_rows += dirty_count;
        let expected_rows = if mode == SubscriptionDeliveryMode::State {
            dirty_count
        } else {
            chunk.len()
        };
        let mut read_rows = 0;
        let mut emitted_topics = vec![false; config.topics];
        while read_rows < expected_rows {
            let (delivery, elapsed) = read(&consumer, expected_rows - read_rows).await;
            materialization_ns += elapsed;
            materialization_latencies.push(elapsed);
            assert_eq!(delivery.kind, mode);
            for row in 0..delivery.data.num_rows() {
                let topic = topic_index(&delivery.data, row, &plan.payloads);
                topic_rows[topic] += 1;
                match mode {
                    SubscriptionDeliveryMode::State => {
                        assert!(dirty[topic]);
                        assert!(
                            !emitted_topics[topic],
                            "one image per dirty topic at a slow-read boundary"
                        );
                        emitted_topics[topic] = true;
                        assert_image_values(&delivery.data, row, &expected[topic]);
                    }
                    SubscriptionDeliveryMode::Deltas => {
                        let expected_update = &chunk[read_rows + row];
                        assert_eq!(topic, expected_update.topic_id as usize);
                        let presence = delivery
                            .data
                            .column_by_name("__xbbg_present")
                            .expect("delta presence")
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .expect("binary presence");
                        let schema = delivery.data.schema();
                        for field in FIELDS {
                            let expected_value = update_field(expected_update, field);
                            let bit = schema.index_of(field).expect("delta field") - 2;
                            assert_eq!(
                                presence.value(row)[bit / 8] & (1 << (bit % 8)) != 0,
                                expected_value.is_some(),
                                "presence of {field}"
                            );
                            assert_eq!(
                                value(&delivery.data, field, row),
                                expected_value.cloned().unwrap_or(UpdateValue::Null)
                            );
                        }
                        delta_checksum += expected_update.timestamp_us as u64;
                    }
                }
            }
            read_rows += delivery.data.num_rows();
            rows += delivery.data.num_rows();
            batches += 1;
            let started = Instant::now();
            drop(black_box(delivery));
            output_drop_ns += ns(started);
        }
        assert_eq!(read_rows, expected_rows);
    }
    let final_image = consumer
        .handle()
        .expect("replay handle")
        .latest()
        .expect("actual shared final images");
    assert_eq!(final_image.num_rows(), config.topics);
    let mut final_checksum = 0u64;
    let mut seen = vec![false; config.topics];
    for row in 0..final_image.num_rows() {
        let topic = topic_index(&final_image, row, &plan.payloads);
        assert!(!seen[topic]);
        seen[topic] = true;
        assert_image_values(&final_image, row, &expected[topic]);
        if let UpdateValue::I64(sequence) = value(&final_image, "SEQ", row) {
            final_checksum += sequence as u64;
        } else {
            panic!("final sequence type");
        }
    }
    assert!(seen.into_iter().all(|seen| seen));
    if mode == SubscriptionDeliveryMode::State {
        assert_eq!(rows, expected_state_rows);
        assert!(
            rows < config.replay_updates,
            "slow reads must conflate output volume"
        );
    } else {
        assert_eq!(rows, config.replay_updates);
        let expected_checksum: u64 = plan
            .updates
            .iter()
            .map(|update| update.timestamp_us as u64)
            .sum();
        assert_eq!(delta_checksum, expected_checksum);
    }
    consumer
        .handle()
        .expect("replay handle")
        .unsubscribe()
        .await
        .expect("replay cleanup");
    drop(consumer);
    fixture.settle().await;
    ReplayResult {
        json: json!({
            "sample": sample, "input_updates": config.replay_updates,
            "producer_publish_ns": producer_ns,
            "producer_updates_per_second": config.replay_updates as f64 * 1e9 / producer_ns as f64,
            "native_read_materialization_ns": materialization_ns,
            "native_materialization_ns_per_output_row": materialization_ns as f64 / rows as f64,
            "output_drop_ns": output_drop_ns,
            "output_rows": rows, "output_batches": batches,
            "output_rows_per_input_update": rows as f64 / config.replay_updates as f64,
            "topic_output_rows": topic_rows,
            "final_image_sequence_checksum": final_checksum,
            "delta_sequence_checksum": (mode == SubscriptionDeliveryMode::Deltas).then_some(delta_checksum),
        }),
        producer_latencies,
        materialization_latencies,
        rows,
        checksum: final_checksum,
    }
}

pub(crate) async fn run(config: &Config) -> Value {
    let sdk = sdk_preflight().await;
    let plan = Plan::new(config);
    let modes = [
        SubscriptionDeliveryMode::Deltas,
        SubscriptionDeliveryMode::State,
    ];
    let mut results: [Vec<Value>; 2] = std::array::from_fn(|_| Vec::new());
    let mut producers: [Vec<u64>; 2] = std::array::from_fn(|_| Vec::new());
    let mut materializations: [Vec<u64>; 2] = std::array::from_fn(|_| Vec::new());
    let mut row_counts: [Vec<usize>; 2] = std::array::from_fn(|_| Vec::new());
    for sample in 0..config.warmup + config.samples {
        let mut checksums = [0u64; 2];
        for offset in 0..2 {
            let index = (offset + sample) % 2;
            let result = iteration(modes[index], config, &plan, sample).await;
            checksums[index] = result.checksum;
            if sample >= config.warmup {
                results[index].push(result.json);
                producers[index].extend(result.producer_latencies);
                materializations[index].extend(result.materialization_latencies);
                row_counts[index].push(result.rows);
            }
        }
        assert_eq!(
            checksums[0], checksums[1],
            "delta and state shared images end at identical latest values"
        );
    }
    let cases: Vec<_> = modes.into_iter().enumerate().map(|(index, mode)| json!({
    "mode": if mode == SubscriptionDeliveryMode::Deltas { "strict_deltas" } else { "conflated_state" },
    "sample_count": results[index].len(),
    "producer_publish_latency": distribution(&producers[index]),
    "native_read_materialization_latency": distribution(&materializations[index]),
    "samples": results[index],
})).collect();
    json!({
        "sdk_decode_preflight": sdk,
        "path": "SyntheticSubscriptionFeed.publish(&SubscriptionUpdate) -> actual shared Feed::on_update -> production projection/shared image/strict queue or state dirty receiver -> actual SubscriptionConsumer.next_delivery().commit()",
        "setup_scope": "separate actual fixture per mode/sample; production subscription construction and topic membership; full eight-field initial images and drain; immutable stable full layouts and prebuilt sparse two-field updates; excluded from reported timing",
        "producer_timing_scope": "decoded publish only, including actual feed/shared-image/projection/metrics/queue-or-dirty bookkeeping and Arc/value clones required by production; borrowed immutable input Arcs are retained by the plan, so final input-string deallocation is not measured; excludes SDK decoding/network, update generation, read/materialization and correctness oracle; no Bloomberg service is contacted",
        "read_timing_scope": "separately accumulated actual native next_delivery + transactional lease.commit, receiver gathering and Arrow image/delta materialization; ready data is read immediately with no added sleep; output buffer destruction separate; reader validation and expected-image oracle excluded",
        "cadence": "one hot topic; one quiet-topic update every quiet_every inputs rotating through other topics; consumer drains only after each read_every published inputs; deterministic operation-count slow-read cadence, not a wall-clock traffic model; both modes get identical input order and cadence in separate fixtures",
        "correctness": "every delta row checked against original sparse payload and presence bitmap; every state image checked against untimed expected retained values/explicit-null clears and valid=true; quiet fields retained; final actual shared latest() images checked for every topic and equal across modes; strict stream capacity exceeds one read burst, so no strict drops are hidden",
        "state_to_delta_output_row_ratio": row_counts[1].iter().sum::<usize>() as f64 / row_counts[0].iter().sum::<usize>() as f64,
        "cases": cases,
    })
}
