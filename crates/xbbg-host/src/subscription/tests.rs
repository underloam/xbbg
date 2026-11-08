//! Facade tests against the production shared feeds of `SubscriptionFixture`.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BooleanArray, Float64Array};
use xbbg_async::engine::state::UpdateValue;
use xbbg_async::testing::SubscriptionFixture;
use xbbg_async::{EngineConfig, OverflowPolicy, SubscribeRequest, SubscriptionDeliveryMode};

use super::*;
use crate::errors::HostErrorKind;

const TOPIC: &str = "SYNTHETIC Equity";

fn fixture(capacity: usize, overflow: OverflowPolicy) -> SubscriptionFixture {
    let config = EngineConfig {
        subscription_stream_capacity: capacity,
        overflow_policy: overflow,
        ..EngineConfig::default()
    };
    SubscriptionFixture::new(config)
}

fn request(fields: &[&str]) -> SubscribeRequest {
    SubscribeRequest {
        topics: vec![TOPIC.into()],
        fields: fields.iter().map(|field| (*field).to_string()).collect(),
        ..SubscribeRequest::default()
    }
}

async fn subscribe(
    source: &SubscriptionFixture,
    request: SubscribeRequest,
    batch_items: usize,
) -> HostSubscription {
    let stream = source.subscribe(request).await.expect("subscribe");
    HostSubscription::from_stream(stream, batch_items, None)
}

fn publish(source: &SubscriptionFixture, json: &str) {
    source.publish_json(TOPIC, json).expect("publish");
}

fn f64_at(batch: &RecordBatch, column: &str, row: usize) -> Option<f64> {
    let column = batch
        .column_by_name(column)
        .expect("column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64 column");
    (!column.is_null(row)).then(|| column.value(row))
}

fn bool_at(batch: &RecordBatch, column: &str, row: usize) -> bool {
    batch
        .column_by_name(column)
        .expect("column")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool column")
        .value(row)
}

fn bid(update: &SubscriptionUpdate) -> Option<&UpdateValue> {
    update.values.iter().find_map(|field| {
        (update.layout.fields[usize::from(field.index)].name.as_ref() == "BID")
            .then_some(&field.value)
    })
}

fn ready<T>(outcome: ReadOutcome<T>) -> T {
    match outcome {
        ReadOutcome::Ready(value) => value,
        ReadOutcome::Ended => panic!("read ended"),
        ReadOutcome::TimedOut => panic!("read timed out"),
    }
}

async fn delivery(subscription: &HostSubscription) -> DeliveryData {
    let read = ready(
        subscription
            .next_delivery(None, Some(Duration::from_secs(5)))
            .await
            .expect("delivery read"),
    );
    match read.prepare() {
        PreparedDelivery::Data(data) => data,
        PreparedDelivery::Closed => panic!("closed"),
        PreparedDelivery::Error(_) => panic!("error receipt"),
    }
}

#[tokio::test]
async fn strict_scalar_reads_send_each_layout_once_and_honour_limits() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID", "ASK"]), 16).await;
    for value in [1.0, 2.0, 3.0] {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    let first = ready(
        subscription
            .next_updates(Some(2), None)
            .await
            .expect("scalar read"),
    );
    let batch = first.value();
    assert_eq!(batch.updates.len(), 2);
    assert!(batch.layout.is_some());
    assert_eq!(batch.layout_version, batch.updates[0].layout.version);
    assert_eq!(bid(&batch.updates[1]), Some(&UpdateValue::F64(2.0)));
    drop(first);
    let second = ready(subscription.next_updates(None, None).await.expect("second"));
    assert!(
        second.value().layout.is_none(),
        "an unchanged layout is not resent"
    );
    assert_eq!(second.value().updates.len(), 1);
    drop(second);
    let error = subscription
        .next_updates(Some(0), None)
        .await
        .expect_err("zero limit");
    assert_eq!(error.kind, HostErrorKind::Argument);
    assert_eq!(error.message, "maxItems must be greater than zero");
}

#[tokio::test]
async fn deadlines_fill_batches_time_out_without_consuming_and_end_after_close() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = Arc::new(subscribe(&source, request(&["BID"]), 16).await);
    assert!(matches!(
        subscription
            .next_updates(None, Some(Duration::ZERO))
            .await
            .expect("empty poll"),
        ReadOutcome::TimedOut
    ));
    assert!(matches!(
        subscription
            .next_arrow_batch(None, Some(Duration::from_millis(5)))
            .await
            .expect("empty arrow poll"),
        ReadOutcome::TimedOut
    ));
    publish(&source, r#"{"BID":1}"#);
    publish(&source, r#"{"BID":2}"#);
    let filled = ready(
        subscription
            .next_updates(Some(3), Some(Duration::from_millis(50)))
            .await
            .expect("deadline read"),
    );
    assert_eq!(filled.value().updates.len(), 2, "waits until the deadline");
    drop(filled);

    let reader = Arc::clone(&subscription);
    let pending = tokio::spawn(async move { reader.next_updates(None, None).await });
    tokio::task::yield_now().await;
    subscription.signal_closing();
    assert!(matches!(
        pending.await.expect("join").expect("closed read"),
        ReadOutcome::Ended
    ));
    assert!(matches!(
        subscription
            .next_delivery(None, None)
            .await
            .expect("closed delivery"),
        ReadOutcome::Ended
    ));
}

#[tokio::test]
async fn image_only_subscriptions_refuse_every_read_but_serve_latest() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(
        &source,
        SubscribeRequest {
            deliver_rows: false,
            ..request(&["BID"])
        },
        16,
    )
    .await;
    publish(&source, r#"{"BID":4.5}"#);
    let message =
        "image-only subscription (rows: false) does not deliver rows; use latest() instead";
    for error in [
        subscription
            .next_updates(None, None)
            .await
            .expect_err("scalar"),
        subscription
            .next_arrow_batch(None, None)
            .await
            .expect_err("arrow"),
        subscription
            .next_delivery(None, None)
            .await
            .expect_err("delivery"),
    ] {
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(error.message, message);
        assert_eq!(error.context.element.as_deref(), Some("rows"));
    }
    assert!(!subscription.delivers_rows());
    let latest = subscription.latest().expect("latest");
    assert_eq!(latest.num_rows(), 1);
    assert_eq!(f64_at(&latest, "BID", 0), Some(4.5));
    assert!(!subscription.is_closed(), "guards never close");
}

#[tokio::test]
async fn drop_oldest_requires_deliveries_and_reports_the_exact_gap() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    for value in [1.25, 2.5, 3.75] {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    assert!(subscription.requires_deliveries());
    let error = subscription
        .next_updates(None, None)
        .await
        .expect_err("plain read");
    assert_eq!(error.kind, HostErrorKind::Validation);
    assert!(
        error
            .message
            .starts_with("state and drop_oldest subscriptions require .deliveries()")
    );
    let data = delivery(&subscription).await;
    assert_eq!(data.kind, SubscriptionDeliveryMode::Deltas);
    assert_eq!(data.gap_before.map(|gap| gap.dropped_updates), Some(2));
    assert_eq!(f64_at(&data.data, "BID", 0), Some(3.75));
    let receipt = data.publish(&subscription).expect("receipt");
    assert!(subscription.commit_delivery(receipt).expect("commit"));
    assert!(!subscription.commit_delivery(receipt).expect("settled once"));
}

#[tokio::test]
async fn unpublished_or_rolled_back_deliveries_are_redelivered_with_their_gap() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    for value in [1.0, 2.0, 3.0] {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    // Conversion failure: dropping the prepared data rolls it back.
    drop(delivery(&subscription).await);
    // Explicit rollback after publication.
    let receipt = delivery(&subscription)
        .await
        .publish(&subscription)
        .expect("receipt");
    subscription.rollback_delivery(receipt);
    let data = delivery(&subscription).await;
    assert_eq!(f64_at(&data.data, "BID", 0), Some(3.0));
    assert_eq!(data.gap_before.map(|gap| gap.dropped_updates), Some(2));
    let receipt = data.publish(&subscription).expect("receipt");
    assert!(subscription.commit_delivery(receipt).expect("commit"));
    assert!(matches!(
        subscription
            .next_delivery(Some(1), Some(Duration::ZERO))
            .await
            .expect("drained"),
        ReadOutcome::TimedOut
    ));
}

#[tokio::test]
async fn close_wins_before_and_after_conversion_and_drains_keep_the_data() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    for value in [1.0, 2.0, 3.0] {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    // Close after conversion: publish refuses and restores.
    let data = delivery(&subscription).await;
    subscription.signal_closing();
    assert!(data.publish(&subscription).is_none());

    let drained = subscription
        .unsubscribe_deliveries(true)
        .await
        .expect("drain");
    assert_eq!(drained.deliveries.len(), 1);
    assert_eq!(f64_at(&drained.deliveries[0].data, "BID", 0), Some(3.0));
    assert_eq!(
        drained.deliveries[0]
            .gap_before
            .map(|gap| gap.dropped_updates),
        Some(2)
    );
    let receipt = drained.publish(&subscription).expect("drain receipt");
    // A failed host conversion rolls the whole drain back for a retry.
    subscription.rollback_drain(receipt);
    let retried = subscription
        .unsubscribe_deliveries(true)
        .await
        .expect("retry");
    assert_eq!(retried.deliveries.len(), 1);
    let receipt = retried.publish(&subscription).expect("retry receipt");
    assert!(subscription.commit_drain(receipt));
    assert!(!subscription.commit_drain(receipt));
    let empty = subscription
        .unsubscribe_deliveries(true)
        .await
        .expect("empty");
    assert!(empty.deliveries.is_empty());
    assert!(empty.publish(&subscription).is_none());
}

#[tokio::test]
async fn close_before_conversion_restores_the_lease() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    publish(&source, r#"{"BID":9.5}"#);
    let read = ready(
        subscription
            .next_delivery(None, None)
            .await
            .expect("delivery"),
    );
    subscription.signal_closing();
    assert!(matches!(read.prepare(), PreparedDelivery::Closed));
    let drained = subscription
        .unsubscribe_deliveries(true)
        .await
        .expect("drain");
    assert_eq!(f64_at(&drained.deliveries[0].data, "BID", 0), Some(9.5));
}

#[tokio::test]
async fn outstanding_receipts_roll_back_when_a_close_starts() {
    let source = fixture(4, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    publish(&source, r#"{"BID":7}"#);
    let receipt = delivery(&subscription)
        .await
        .publish(&subscription)
        .expect("receipt");
    // The receipt holds the marked-read gate; close must not wait for it.
    let drained = tokio::time::timeout(
        Duration::from_secs(5),
        subscription.unsubscribe_deliveries(true),
    )
    .await
    .expect("close does not wedge")
    .expect("drain");
    assert_eq!(f64_at(&drained.deliveries[0].data, "BID", 0), Some(7.0));
    assert!(!subscription.commit_delivery(receipt).expect("close won"));
}

#[tokio::test]
async fn terminal_data_loss_is_published_once_after_committed_rows() {
    let source = fixture(1, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    publish(&source, r#"{"BID":1}"#);
    publish(&source, r#"{"BID":2}"#);
    let receipt = delivery(&subscription)
        .await
        .publish(&subscription)
        .expect("receipt");
    assert!(subscription.commit_delivery(receipt).expect("first row"));
    let read = ready(
        subscription
            .next_delivery(None, None)
            .await
            .expect("error read"),
    );
    let PreparedDelivery::Error(error) = read.prepare() else {
        panic!("expected the terminal error");
    };
    let receipt = error.publish(&subscription).expect("error receipt");
    // Rolling an error receipt back keeps it for the next reader.
    subscription.rollback_delivery(receipt);
    let read = ready(
        subscription
            .next_delivery(None, None)
            .await
            .expect("error again"),
    );
    let PreparedDelivery::Error(error) = read.prepare() else {
        panic!("expected the restored terminal error");
    };
    let receipt = error.publish(&subscription).expect("error receipt");
    let failure = subscription
        .commit_delivery(receipt)
        .expect_err("terminal error");
    assert_eq!(failure.kind, HostErrorKind::DataLoss);
    assert_eq!(failure.context.topic.as_deref(), Some(TOPIC));
    assert!(matches!(
        subscription
            .next_delivery(None, None)
            .await
            .expect("after error"),
        ReadOutcome::Ended
    ));
}

#[tokio::test]
async fn state_deliveries_conflate_sparse_updates_and_clears() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(
        &source,
        SubscribeRequest {
            delivery: SubscriptionDeliveryMode::State,
            flush_threshold: Some(2),
            ..request(&["BID", "ASK"])
        },
        2,
    )
    .await;
    assert!(subscription.requires_deliveries());
    assert_eq!(
        subscription.delivery_mode(),
        SubscriptionDeliveryMode::State
    );
    let initial = delivery(&subscription).await;
    assert!(!bool_at(&initial.data, "valid", 0), "unknown image first");
    let receipt = initial.publish(&subscription).expect("receipt");
    assert!(subscription.commit_delivery(receipt).expect("commit"));
    publish(&source, r#"{"BID":1.25,"ASK":2.25}"#);
    publish(&source, r#"{"BID":3.75}"#);
    publish(&source, r#"{"ASK":null}"#);
    let state = delivery(&subscription).await;
    assert_eq!(state.kind, SubscriptionDeliveryMode::State);
    assert!(state.gap_before.is_none());
    assert_eq!(state.data.num_rows(), 1);
    assert!(bool_at(&state.data, "valid", 0));
    assert_eq!(f64_at(&state.data, "BID", 0), Some(3.75));
    assert_eq!(f64_at(&state.data, "ASK", 0), None);
}

#[tokio::test]
async fn scalar_drains_split_by_batch_size_and_keep_every_row() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 2).await;
    for value in 1..=5 {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    let batches = subscription.unsubscribe_scalar(true).await.expect("drain");
    let sizes: Vec<usize> = batches.iter().map(|batch| batch.updates.len()).collect();
    assert_eq!(sizes, vec![2, 2, 1]);
    assert!(batches[0].layout.is_some());
    assert!(batches[1].layout.is_none());
    assert!(!subscription.is_active());
    let error = subscription.latest().expect_err("closed");
    assert_eq!(error.message, "subscription already closed");
    assert_eq!(error.kind, HostErrorKind::Validation);
    for error in [
        subscription
            .add(vec!["OTHER Equity".into()], Vec::new())
            .await
            .expect_err("add"),
        subscription
            .add_fields(vec!["ASK".into()])
            .await
            .expect_err("add fields"),
        subscription
            .remove(vec![TOPIC.into()])
            .await
            .expect_err("remove"),
    ] {
        assert_eq!(error.message, "subscription already closed");
    }
}

#[tokio::test]
async fn arrow_drains_return_queued_batches_then_rebatched_rows() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 2).await;
    publish(&source, r#"{"BID":1}"#);
    let first = ready(
        subscription
            .next_arrow_batch(None, None)
            .await
            .expect("arrow read"),
    );
    assert_eq!(f64_at(first.value(), "BID", 0), Some(1.0));
    assert!(first.value().column_by_name("__xbbg_present").is_some());
    drop(first);
    for value in 2..=4 {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    let batches = subscription.unsubscribe_arrow(true).await.expect("drain");
    let rows: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows, vec![2, 1]);
    assert_eq!(f64_at(&batches[1], "BID", 0), Some(4.0));
}

#[tokio::test]
async fn plain_drains_of_delivery_only_subscriptions_close_then_refuse() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    publish(&source, r#"{"BID":1}"#);
    let error = subscription
        .unsubscribe_scalar(true)
        .await
        .expect_err("plain drain");
    assert_eq!(error.kind, HostErrorKind::Validation);
    assert!(error.message.contains("require .deliveries()"));
    assert!(subscription.is_closed());
    // A source DATALOSS still outranks the plain-drain guard.
    let source = fixture(4, OverflowPolicy::DropNewest);
    let lossy = subscribe(&source, request(&["BID"]), 16).await;
    publish(&source, r#"{"BID":1}"#);
    source.data_loss(TOPIC).expect("data loss");
    let error = lossy.unsubscribe_scalar(true).await.expect_err("loss");
    assert_eq!(error.kind, HostErrorKind::DataLoss);
}

#[tokio::test]
async fn controls_grow_projection_and_track_labels() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(
        &source,
        SubscribeRequest {
            aliases: vec![(TOPIC.into(), "synthetic".into())],
            ..request(&["BID"])
        },
        16,
    )
    .await;
    assert_eq!(subscription.tickers(), vec!["synthetic"]);
    subscription
        .add_fields(vec!["ASK".into()])
        .await
        .expect("add fields");
    assert_eq!(subscription.fields(), vec!["BID", "ASK"]);
    subscription
        .add(
            vec!["SECOND SYNTHETIC Equity".into()],
            vec![("SECOND SYNTHETIC Equity".into(), "second".into())],
        )
        .await
        .expect("add");
    assert_eq!(subscription.tickers(), vec!["synthetic", "second"]);
    subscription
        .remove(vec!["synthetic".into()])
        .await
        .expect("remove");
    assert_eq!(subscription.tickers(), vec!["second"]);
    let snapshot = subscription.status_snapshot();
    assert!(status::topic_states(&snapshot).contains_key("second"));
    assert!(subscription.is_active());
}

#[tokio::test]
async fn delayed_streams_produce_warnings_once() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    publish(
        &source,
        r#"{"BID":1,"IS_DELAYED_STREAM":true,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let _ = subscription
        .next_updates(None, Some(Duration::from_secs(5)))
        .await
        .expect("read");
    let warnings = subscription.take_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].message_type, "DelayedStream");
    assert!(subscription.take_warnings().is_empty());
}

#[tokio::test]
async fn owners_live_until_the_subscription_drops() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let owner = Arc::new(());
    let subscription = subscribe(&source, request(&["BID"]), 16)
        .await
        .keep_alive(Box::new(Arc::clone(&owner)));
    assert_eq!(Arc::strong_count(&owner), 2);
    drop(subscription);
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[tokio::test]
async fn publication_pause_parks_a_leased_read_until_released() {
    let source = fixture(1, OverflowPolicy::DropOldest);
    let subscription = Arc::new(subscribe(&source, request(&["BID"]), 16).await);
    for value in [1.25, 2.5, 3.75] {
        publish(&source, &format!(r#"{{"BID":{value}}}"#));
    }
    let pause = Arc::new(PublicationPause::default());
    subscription
        .arm_publication_pause(Arc::clone(&pause))
        .expect("armed");
    let error = subscription
        .arm_publication_pause(Arc::new(PublicationPause::default()))
        .expect_err("already armed");
    assert_eq!(error.message, "a delivery pause is already armed");
    let reader = Arc::clone(&subscription);
    let pending = tokio::spawn(async move { reader.next_delivery(None, None).await });
    pause.ready().await;
    subscription.signal_closing();
    pause.release();
    let read = ready(pending.await.expect("join").expect("paused read"));
    assert!(matches!(read.prepare(), PreparedDelivery::Closed));
    let drained = subscription
        .unsubscribe_deliveries(true)
        .await
        .expect("drain");
    assert_eq!(f64_at(&drained.deliveries[0].data, "BID", 0), Some(3.75));
    assert_eq!(
        drained.deliveries[0]
            .gap_before
            .map(|gap| gap.dropped_updates),
        Some(2)
    );
}

#[tokio::test]
async fn session_termination_fails_reads_with_an_internal_error() {
    let source = fixture(8, OverflowPolicy::DropNewest);
    let subscription = subscribe(&source, request(&["BID"]), 16).await;
    source.terminate();
    let error = subscription
        .next_updates(None, Some(Duration::from_secs(5)))
        .await
        .expect_err("terminated");
    assert_eq!(error.kind, HostErrorKind::Internal);
}
