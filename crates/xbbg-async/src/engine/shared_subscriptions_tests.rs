use super::testing::{data, hub, hub_with_config, test_feed};
use super::*;
use arrow_array::{Array, Float64Array};

fn request(topics: &[&str], fields: &[&str]) -> SubscribeRequest {
    SubscribeRequest {
        topics: topics.iter().map(|value| value.to_string()).collect(),
        fields: fields.iter().map(|value| value.to_string()).collect(),
        ..SubscribeRequest::default()
    }
}

fn next(stream: &mut SubscriptionStream) -> SubscriptionUpdate {
    stream.try_next().expect("queued row").expect("data row")
}

fn value<'a>(row: &'a SubscriptionUpdate, name: &str) -> Option<&'a UpdateValue> {
    row.values
        .iter()
        .find(|value| row.layout.fields[value.index as usize].name.as_ref() == name)
        .map(|value| &value.value)
}

fn row_signature(row: &SubscriptionUpdate) -> Vec<(String, Option<String>)> {
    row.values
        .iter()
        .map(|value| {
            (
                row.layout.fields[value.index as usize].name.to_string(),
                value.value.as_string_lossy(),
            )
        })
        .collect()
}

async fn cleanup(hub: &Arc<SharedSubscriptions>) {
    hub.schedule_cleanup();
    // Acquiring the gate after an async yield observes the already-enqueued cleanup.
    tokio::task::yield_now().await;
    let _gate = hub.mutations.lock().await;
}

#[tokio::test]
async fn filtered_consumer_is_independent_of_union_and_suppresses_unrelated_rows() {
    let (shared, _) = hub();
    let (alone, _) = hub();
    let mut expected = alone
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut actual = shared
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut co = shared
        .subscribe(request(&["IBM US Equity"], &["ASK"]))
        .await
        .unwrap();
    let (a, ak) = test_feed(&alone, "IBM US Equity");
    let (b, bk) = test_feed(&shared, "IBM US Equity");
    for json in [
        r#"{"BID":10,"ASK":11,"MKTDATA_EVENT_TYPE":"QUOTE"}"#,
        r#"{"ASK":12,"MKTDATA_EVENT_TYPE":"QUOTE"}"#,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY"}"#,
        r#"{"BID":null,"MKTDATA_EVENT_TYPE":"QUOTE"}"#,
    ] {
        data(&a, ak, json);
        data(&b, bk, json);
        let left = expected.try_next();
        let right = actual.try_next();
        match (left, right) {
            (Some(Ok(left)), Some(Ok(right))) => {
                assert_eq!(row_signature(&left), row_signature(&right))
            }
            (None, None) => {}
            _ => panic!("consumer row presence depends on co-consumer"),
        }
    }
    assert_eq!(b.subscribes.load(Ordering::Relaxed), 1);
    assert!(matches!(
        value(&next(&mut co), "ASK"),
        Some(UpdateValue::F64(11.0))
    ));
}

#[tokio::test]
async fn late_join_emits_known_values_clears_and_synthetic_metadata() {
    let (hub, factory) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity"], &["BID", "ASK", "LAST_PRICE"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(
        &session,
        key,
        r#"{"BID":10,"ASK":null,"MKTDATA_EVENT_TYPE":"QUOTE"}"#,
    );
    next(&mut first);
    let mut joined = hub
        .subscribe(request(&["IBM US Equity"], &["BID", "ASK", "LAST_PRICE"]))
        .await
        .unwrap();
    let image = next(&mut joined);
    assert!(matches!(value(&image, "BID"), Some(UpdateValue::F64(10.0))));
    assert!(matches!(value(&image, "ASK"), Some(UpdateValue::Null)));
    assert!(value(&image, "LAST_PRICE").is_none());
    assert_eq!(text_field(&image, EVENT_TYPE), Some("SUMMARY"));
    assert_eq!(text_field(&image, EVENT_SUBTYPE), Some("INITPAINT"));
    assert!(image.timestamp_us > 0);
    let batch = super::super::state::subscription_update_to_record_batch(&image).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let presence = batch
        .column_by_name("__xbbg_present")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap()
        .value(0);
    assert_ne!(presence[0] & 0b001, 0, "known BID must be present");
    assert_ne!(presence[0] & 0b010, 0, "explicit ASK clear must be present");
    assert_eq!(presence[0] & 0b100, 0, "unknown LAST_PRICE must be absent");
    assert_eq!(
        factory.sessions.lock().len(),
        1,
        "attach-only handle acquired a session"
    );
}

#[tokio::test]
async fn growth_resubscribes_once_and_repaint_resyncs_only_changed_existing_values() {
    let (hub, _) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(
        &session,
        key,
        r#"{"BID":10,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    next(&mut first);
    let mut joined = hub
        .subscribe(request(&["IBM US Equity"], &["ASK"]))
        .await
        .unwrap();
    next(&mut joined); // Synthetic row: ASK is not known yet.
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 1);
    data(
        &session,
        key,
        r#"{"BID":11,"ASK":12,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert!(matches!(
        value(&next(&mut first), "BID"),
        Some(UpdateValue::F64(11.0))
    ));
    assert!(matches!(
        value(&next(&mut joined), "ASK"),
        Some(UpdateValue::F64(12.0))
    ));
    data(
        &session,
        key,
        r#"{"BID":11,"ASK":12,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert!(first.try_next().is_none());
    assert!(matches!(
        value(&next(&mut joined), "ASK"),
        Some(UpdateValue::F64(12.0))
    ));
    data(
        &session,
        key,
        r#"{"BID":13,"ASK":14,"MKTDATA_EVENT_SUBTYPE":"UPDATE"}"#,
    );
    assert!(matches!(
        value(&next(&mut first), "BID"),
        Some(UpdateValue::F64(13.0))
    ));
    next(&mut joined);
    first.add_fields(vec!["LAST_PRICE".into()]).await.unwrap();
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
    data(
        &session,
        key,
        r#"{"BID":15,"ASK":16,"LAST_PRICE":17,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert!(matches!(
        value(&next(&mut first), "LAST_PRICE"),
        Some(UpdateValue::F64(17.0))
    ));
    assert!(matches!(
        value(&next(&mut joined), "ASK"),
        Some(UpdateValue::F64(16.0))
    ));
    first.add_fields(vec!["LAST_PRICE".into()]).await.unwrap();
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn detach_is_refcounted_and_empty_session_is_released() {
    let (hub, factory) = hub();
    let first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let second = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, _) = test_feed(&hub, "IBM US Equity");
    first.unsubscribe(false).await.unwrap();
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 0);
    assert_eq!(hub.feeds()[0].consumers, 1);
    second.unsubscribe(false).await.unwrap();
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 1);
    assert!(hub.feeds().is_empty());
    tokio::task::yield_now().await;
    assert!(
        factory
            .owners
            .lock()
            .iter()
            .all(|owner| owner.upgrade().is_none())
    );
}

#[tokio::test]
async fn local_overflow_closes_only_its_consumer_and_dataloss_closes_all_carriers() {
    let (hub, _) = hub();
    let mut small_req = request(&["IBM US Equity"], &["BID"]);
    small_req.stream_capacity = Some(1);
    let mut small = hub.subscribe(small_req).await.unwrap();
    let mut healthy = hub
        .subscribe(request(&["IBM US Equity", "AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut other = hub
        .subscribe(request(&["AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1}"#);
    next(&mut healthy);
    data(&session, key, r#"{"BID":2}"#);
    next(&mut healthy);
    assert!(matches!(
        value(&next(&mut small), "BID"),
        Some(UpdateValue::F64(1.0))
    ));
    assert!(matches!(
        small.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    assert!(healthy.is_active());
    cleanup(&hub).await;
    assert_eq!(
        hub.feeds()
            .iter()
            .find(|feed| feed.topic == "IBM US Equity")
            .unwrap()
            .consumers,
        1
    );
    let mut carrier = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    next(&mut carrier);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    assert!(matches!(
        healthy.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    assert!(matches!(
        carrier.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    cleanup(&hub).await;
    assert!(hub.feeds().iter().all(|feed| feed.topic != "IBM US Equity"));
    let (aapl, aapl_key) = test_feed(&hub, "AAPL US Equity");
    data(&aapl, aapl_key, r#"{"BID":3}"#);
    assert!(matches!(
        value(&next(&mut other), "BID"),
        Some(UpdateValue::F64(3.0))
    ));
}

#[tokio::test]
async fn subscription_failure_is_per_topic_and_session_termination_is_per_session() {
    let (hub, _) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity", "AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut second = hub
        .subscribe(request(&["IBM US Equity", "MSFT US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.status(key, "SubscriptionFailure");
    assert_eq!(first.topics(), vec!["AAPL US Equity"]);
    assert_eq!(second.topics(), vec!["MSFT US Equity"]);
    assert_eq!(first.status().load().failures()[0].topic, "IBM US Equity");
    assert!(first.try_next().is_none());
    assert!(second.try_next().is_none());
    session.session_event("SessionTerminated");
    assert!(matches!(first.try_next(), Some(Err(_))));
    let (msft, msft_key) = test_feed(&hub, "MSFT US Equity");
    data(&msft, msft_key, r#"{"BID":4}"#);
    assert!(matches!(
        value(&next(&mut second), "BID"),
        Some(UpdateValue::F64(4.0))
    ));
}

#[tokio::test]
async fn delayed_policies_and_known_delayed_attach_are_consumer_local() {
    let (hub, _) = hub();
    let mut warn = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut raise_req = request(&["IBM US Equity", "AAPL US Equity"], &["BID"]);
    raise_req.delayed_policy = DelayedPolicy::Raise;
    let raise = hub.subscribe(raise_req).await.unwrap();
    let mut ignore_req = request(&["IBM US Equity"], &["BID"]);
    ignore_req.delayed_policy = DelayedPolicy::Ignore;
    let mut ignore = hub.subscribe(ignore_req).await.unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    for _ in 0..2 {
        data(&session, key, r#"{"BID":1,"IS_DELAYED_STREAM":true}"#);
        next(&mut warn);
        next(&mut ignore);
    }
    assert_eq!(
        warn.take_warnings()
            .iter()
            .filter(|event| event.message_type == "DelayedStream")
            .count(),
        1
    );
    assert!(warn.take_warnings().is_empty());
    assert!(ignore.take_warnings().is_empty());
    assert_eq!(
        ignore.status().load().topic_statuses()["IBM US Equity"].delayed,
        Some(true)
    );
    assert_eq!(raise.topics(), vec!["AAPL US Equity"]);
    assert_eq!(raise.status().load().failures()[0].topic, "IBM US Equity");
    assert!(
        raise.take_warnings().is_empty(),
        "raise rejects rather than issuing a delayed warning"
    );
    let late = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    assert_eq!(late.take_warnings().len(), 1);
    let mut rejected = request(&["IBM US Equity"], &["BID"]);
    rejected.delayed_policy = DelayedPolicy::Raise;
    let mut rejected = hub.subscribe(rejected).await.unwrap();
    assert!(matches!(
        rejected.try_next(),
        Some(Err(BlpError::SubscriptionFailure { .. }))
    ));
}

#[tokio::test]
async fn field_exceptions_are_attributed_to_explicit_consumers_once() {
    let (hub, _) = hub();
    let first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut req = request(&["IBM US Equity"], &["ASK"]);
    req.aliases = vec![("IBM US Equity".into(), "IBM".into())];
    let second = hub.subscribe(req).await.unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    let json = r#"{"exceptions":[{"fieldId":"ASK","reason":{"category":"BAD_FLD","description":"synthetic rejected field","subcategory":"INVALID_FIELD","errorCode":1,"source":"synthetic"}}]}"#;
    session.field_exceptions(key, json);
    session.field_exceptions(key, json);
    assert!(first.status().load().field_errors().is_empty());
    assert_eq!(
        second.status().load().field_errors()["IBM"]["ASK"],
        "BAD_FLD"
    );
    let warnings = second.take_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].message_type, "FieldException");
    assert_eq!(warnings[0].topic.as_deref(), Some("IBM"));
}

#[tokio::test]
async fn aliases_remove_and_latest_preserve_typed_values_and_explicit_clears() {
    let (hub, _) = hub();
    let mut req = request(&["IBM US Equity"], &["BID", "ASK"]);
    req.aliases = vec![("IBM US Equity".into(), "IBM".into())];
    let mut stream = hub.subscribe(req).await.unwrap();
    let empty = stream.latest().unwrap();
    assert_eq!(
        empty
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["topic", "last_update", "live", "delayed", "BID", "ASK"]
    );
    assert!(empty.column(1).is_null(0));
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(
        &session,
        key,
        r#"{"BID":1,"ASK":2,"IS_DELAYED_STREAM":false,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert_eq!(next(&mut stream).topic.as_ref(), "IBM");
    data(&session, key, r#"{"BID":3,"ASK":null}"#);
    next(&mut stream);
    let latest = stream.latest().unwrap();
    assert_eq!(
        latest.schema().field(1).data_type(),
        &ArrowType::TimestampMicros.to_arrow_datatype()
    );
    assert_eq!(
        latest
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "IBM"
    );
    assert_eq!(
        latest
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.0
    );
    assert!(latest.column(5).is_null(0));
    assert!(
        latest
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        stream.status().load().topic_statuses()["IBM"].feed_topic,
        "IBM US Equity"
    );
    stream.remove(vec!["IBM".into()]).await.unwrap();
    assert!(stream.topics().is_empty());
    assert_eq!(stream.latest().unwrap().num_rows(), 0);
    assert!(!stream.status().load().topic_statuses().contains_key("IBM"));
}

#[tokio::test]
async fn options_normalize_but_isolated_and_other_services_never_share() {
    let (hub, factory) = hub();
    let mut req = request(&[" IBM US Equity "], &["BID"]);
    req.options = vec![" interval=1 ".into(), "conflate".into(), "conflate".into()];
    let _first = hub.subscribe(req.clone()).await.unwrap();
    req.options = vec!["conflate".into(), "interval=1".into()];
    let _second = hub.subscribe(req.clone()).await.unwrap();
    assert_eq!(factory.sessions.lock().len(), 1);
    req.isolated = true;
    let _third = hub.subscribe(req.clone()).await.unwrap();
    let _fourth = hub.subscribe(req).await.unwrap();
    assert_eq!(factory.sessions.lock().len(), 3);
    let mut non_market = request(&["//blp/mktvwap/ticker/IBM US Equity"], &["BID"]);
    non_market.service = "//blp/mktvwap".into();
    let mut first = hub.subscribe(non_market.clone()).await.unwrap();
    let feed = hub
        .registry
        .lock()
        .values()
        .find(|feed| feed.identity.service == "//blp/mktvwap")
        .cloned()
        .unwrap();
    let session = feed.session.test.clone().unwrap();
    let key = feed.upstream_key.load(Ordering::Acquire);
    data(&session, key, r#"{"BID":2}"#);
    next(&mut first);
    let mut second = hub.subscribe(non_market).await.unwrap();
    assert!(
        second.try_next().is_none(),
        "non-mktdata must not synthesize an image"
    );
    assert_eq!(factory.sessions.lock().len(), 5);
    assert_eq!(hub.feeds().iter().filter(|feed| feed.isolated).count(), 4);
}

#[tokio::test]
async fn known_delayed_rejection_preserves_newly_added_siblings() {
    let (hub, _) = hub();
    let _owner = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"IS_DELAYED_STREAM":true}"#);
    let mut req = request(&["IBM US Equity", "AAPL US Equity"], &["BID"]);
    req.delayed_policy = DelayedPolicy::Raise;
    let mut mixed = hub.subscribe(req).await.unwrap();
    assert_eq!(mixed.topics(), vec!["AAPL US Equity"]);
    assert!(mixed.is_active());
    let (aapl, key) = test_feed(&hub, "AAPL US Equity");
    data(&aapl, key, r#"{"BID":2}"#);
    assert!(matches!(
        value(&next(&mut mixed), "BID"),
        Some(UpdateValue::F64(2.0))
    ));
}

#[tokio::test]
async fn adding_an_existing_union_field_uses_image_and_existing_field_errors() {
    let (hub, _) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let _second = hub
        .subscribe(request(&["IBM US Equity"], &["ASK", "LAST_PRICE"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"ASK":2}"#);
    next(&mut first);
    session.field_exceptions(key, r#"{"exceptions":[{"fieldId":"LAST_PRICE","reason":{"category":"NOT_APPLICABLE","description":"synthetic","subcategory":"SYNTHETIC","errorCode":1,"source":"synthetic"}}]}"#);
    let resubscribes = session.resubscribes.load(Ordering::Relaxed);
    first
        .add_fields(vec!["ASK".into(), "LAST_PRICE".into()])
        .await
        .unwrap();
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), resubscribes);
    assert!(matches!(
        value(&next(&mut first), "ASK"),
        Some(UpdateValue::F64(2.0))
    ));
    assert_eq!(
        first.status().load().field_errors()["IBM US Equity"]["LAST_PRICE"],
        "NOT_APPLICABLE"
    );
    assert_eq!(first.take_warnings()[0].message_type, "FieldException");
}

#[tokio::test]
async fn dynamic_aliases_detach_independently_and_drop_unsubscribes_the_last() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    stream
        .add(
            vec!["IBM US Equity".into()],
            vec![("IBM US Equity".into(), "IBM".into())],
        )
        .await
        .unwrap();
    assert_eq!(stream.topics(), vec!["IBM US Equity", "IBM"]);
    let (session, _) = test_feed(&hub, "IBM US Equity");
    assert_eq!(session.subscribes.load(Ordering::Relaxed), 1);
    stream.remove(vec!["IBM US Equity".into()]).await.unwrap();
    assert_eq!(stream.topics(), vec!["IBM"]);
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 0);
    drop(stream);
    cleanup(&hub).await;
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn shutdown_publishes_terminal_errors_even_for_empty_handles() {
    let (hub, _) = hub();
    let mut stream = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    stream.remove(vec!["IBM US Equity".into()]).await.unwrap();
    hub.shutdown();
    assert!(matches!(
        stream.try_next(),
        Some(Err(BlpError::Internal { .. }))
    ));
    assert!(
        hub.subscribe(request(&["IBM US Equity"], &["BID"]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn session_death_is_topic_local_but_last_topic_preserves_terminal_error() {
    for event in ["SessionTerminated", "AuthorizationRevoked"] {
        let (hub, _) = hub();
        let mut original = hub
            .subscribe(request(&["IBM US Equity", "AAPL US Equity"], &["BID"]))
            .await
            .unwrap();
        let mut mixed = hub
            .subscribe(request(&["IBM US Equity", "MSFT US Equity"], &["BID"]))
            .await
            .unwrap();
        let (session, _) = test_feed(&hub, "IBM US Equity");
        session.session_event(event);
        assert!(
            matches!(original.try_next(), Some(Err(BlpError::Internal { detail })) if detail.contains(if event == "AuthorizationRevoked" { "revoked" } else { "terminated" }))
        );
        assert_eq!(mixed.topics(), vec!["MSFT US Equity"]);
        assert!(mixed.try_next().is_none());
        let status = mixed.status();
        assert_eq!(
            status.load().failures()[0].kind,
            SubscriptionFailureKind::Terminated
        );
        assert!(status.load().failures()[0].reason.contains(event));
        let (healthy, key) = test_feed(&hub, "MSFT US Equity");
        data(&healthy, key, r#"{"BID":7}"#);
        assert!(matches!(
            value(&next(&mut mixed), "BID"),
            Some(UpdateValue::F64(7.0))
        ));
    }
}

#[tokio::test]
async fn late_join_and_repaint_keep_consumer_lifecycle_streaming() {
    let (hub, _) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.status(key, "SubscriptionStarted");
    session.status(key, "SubscriptionStreamsActivated");
    data(&session, key, r#"{"BID":1}"#);
    next(&mut first);
    let joined = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    assert_eq!(
        joined.status().load().topic_statuses()["IBM US Equity"].state,
        TopicLifecycleState::Streaming
    );
    assert!(joined.status().load().topic_statuses()["IBM US Equity"].streams_active);
    first.add_fields(vec!["ASK".into()]).await.unwrap();
    session.status(key, "SubscriptionStarted");
    data(
        &session,
        key,
        r#"{"BID":2,"ASK":3,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    next(&mut first);
    assert_eq!(
        first.status().load().topic_statuses()["IBM US Equity"].state,
        TopicLifecycleState::Streaming
    );
    assert_eq!(
        joined.status().load().topic_statuses()["IBM US Equity"].state,
        TopicLifecycleState::Streaming
    );
}

#[tokio::test]
async fn immediate_subscribe_after_failure_creates_a_fresh_feed() {
    let (hub, factory) = hub();
    let mut first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.status(key, "SubscriptionFailure");
    assert!(matches!(first.try_next(), Some(Err(_))));
    let mut replacement = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    assert_eq!(factory.sessions.lock().len(), 2);
    let (fresh, key) = test_feed(&hub, "IBM US Equity");
    assert!(!Arc::ptr_eq(&session, &fresh));
    data(&fresh, key, r#"{"BID":8}"#);
    assert!(matches!(
        value(&next(&mut replacement), "BID"),
        Some(UpdateValue::F64(8.0))
    ));
}

#[tokio::test]
async fn block_timeout_detaches_without_another_market_message() {
    let (hub, _) = hub();
    let mut req = request(&["IBM US Equity"], &["BID"]);
    req.overflow_policy = Some(OverflowPolicy::Block);
    req.stream_capacity = Some(1);
    let mut slow = hub.subscribe(req).await.unwrap();
    let mut healthy = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1}"#);
    slow.handle.drain_forwarder().await.unwrap();
    next(&mut healthy);
    data(&session, key, r#"{"BID":2}"#);
    next(&mut healthy);
    slow.handle.drain_forwarder().await.unwrap(); // queue is full: bounded wait expires
    cleanup(&hub).await;
    assert_eq!(hub.feeds()[0].consumers, 1);
    assert!(slow.topics().is_empty());
    assert!(matches!(
        value(&next(&mut slow), "BID"),
        Some(UpdateValue::F64(1.0))
    ));
    assert!(matches!(
        slow.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    healthy.unsubscribe(false).await.unwrap();
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn field_growth_finishes_healthy_topics_after_a_middle_resubscribe_failure() {
    let (hub, _) = hub();
    let mut stream = hub
        .subscribe(request(
            &["IBM US Equity", "AAPL US Equity", "MSFT US Equity"],
            &["BID"],
        ))
        .await
        .unwrap();
    let (session, bad_key) = test_feed(&hub, "AAPL US Equity");
    session.fail_resubscribe.lock().insert(bad_key);
    assert!(stream.add_fields(vec!["ASK".into()]).await.is_err());
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 3);
    assert_eq!(stream.topics(), vec!["IBM US Equity", "MSFT US Equity"]);
    for topic in ["IBM US Equity", "MSFT US Equity"] {
        let (session, key) = test_feed(&hub, topic);
        data(&session, key, r#"{"ASK":9}"#);
        assert!(matches!(
            value(&next(&mut stream), "ASK"),
            Some(UpdateValue::F64(9.0))
        ));
    }
}

#[tokio::test]
async fn union_shape_errors_fail_only_requesting_consumers_and_keep_error_type() {
    let (hub, _) = hub();
    let mut healthy = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut bad = hub
        .subscribe(request(&["IBM US Equity"], &["LEVELS"]))
        .await
        .unwrap();
    let mut all_request = request(&["IBM US Equity"], &["BID"]);
    all_request.all_fields = true;
    let mut all = hub.subscribe(all_request).await.unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.data(key, r#"<element name="BID" type="Float64"/><element name="LEVELS" type="Float64" maxOccurs="unbounded"/>"#,
        r#"{"BID":1,"LEVELS":[2,3]}"#);
    assert!(
        matches!(bad.try_next(), Some(Err(BlpError::SchemaUnsupported { element, .. })) if element == "LEVELS")
    );
    assert!(matches!(
        value(&next(&mut healthy), "BID"),
        Some(UpdateValue::F64(1.0))
    ));
    let all_row = next(&mut all);
    assert!(value(&all_row, "LEVELS").is_none());
    assert!(matches!(
        value(&all_row, "BID"),
        Some(UpdateValue::F64(1.0))
    ));
    assert!(healthy.is_active());
}

#[tokio::test]
async fn all_fields_attach_promotes_decoder_without_changing_filtered_projection() {
    let (hub, _) = hub();
    let mut filtered = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"ASK":2}"#);
    next(&mut filtered);
    let mut req = request(&["IBM US Equity"], &["BID"]);
    req.all_fields = true;
    let mut all = hub.subscribe(req).await.unwrap();
    next(&mut all);
    data(&session, key, r#"{"ASK":3}"#);
    assert!(filtered.try_next().is_none());
    assert!(matches!(
        value(&next(&mut all), "ASK"),
        Some(UpdateValue::F64(3.0))
    ));
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 0);
}

fn feed_for_label(stream: &SubscriptionStream, label: &str) -> Arc<Feed> {
    stream
        .handle
        .inner
        .memberships
        .lock()
        .iter()
        .find(|member| member.label == label)
        .unwrap()
        .feed
        .upgrade()
        .unwrap()
}

#[tokio::test]
async fn topic_status_is_constant_fanout_and_global_status_publishes_once_per_consumer() {
    let (hub, _) = hub();
    let topics: Vec<_> = (0..256)
        .map(|index| format!("SYNTH{index} US Equity"))
        .collect();
    let mut large = hub
        .subscribe(SubscribeRequest {
            topics,
            fields: vec!["BID".into()],
            ..SubscribeRequest::default()
        })
        .await
        .unwrap();
    assert!(
        large.status().publication_count() <= 3,
        "initial registration published once per topic"
    );
    let source = feed_for_label(&large, "SYNTH0 US Equity").session.clone();
    assert_eq!(
        source
            .status
            .load()
            .events()
            .iter()
            .filter(|event| event.message_type == "ServiceReady")
            .count(),
        1
    );
    assert!(
        source.status.publication_count() <= 2,
        "upstream registration was not batched"
    );
    let mut unrelated = hub
        .subscribe(request(&["SYNTH1 US Equity"], &["BID"]))
        .await
        .unwrap();
    let main_status = large.status();
    let unrelated_status = unrelated.status();
    let (session, key) = test_feed(&hub, "SYNTH0 US Equity");
    let before = main_status.publication_count();
    let other_before = unrelated_status.publication_count();
    session.status(key, "SubscriptionStarted");
    assert_eq!(main_status.publication_count() - before, 1);
    assert_eq!(unrelated_status.publication_count(), other_before);
    assert_eq!(
        main_status.load().topic_statuses()["SYNTH0 US Equity"].state,
        TopicLifecycleState::Started
    );
    assert_eq!(
        main_status.load().topic_statuses()["SYNTH1 US Equity"].state,
        TopicLifecycleState::Pending
    );

    for action in ["session", "service", "admin"] {
        let before = main_status.publication_count();
        let other_before = unrelated_status.publication_count();
        match action {
            "session" => session.session_event("SessionConnectionDown"),
            "service" => session.service_event("ServiceDown"),
            _ => session.admin_event("SlowConsumerWarning"),
        }
        assert_eq!(
            main_status.publication_count() - before,
            1,
            "{action} published more than once for one consumer"
        );
        assert_eq!(unrelated_status.publication_count() - other_before, 1);
    }
    let before = main_status.publication_count();
    let other_before = unrelated_status.publication_count();
    session.session_event("SessionTerminated");
    assert_eq!(
        main_status.publication_count() - before,
        1,
        "terminal status must finalize the consumer in one publication"
    );
    assert_eq!(unrelated_status.publication_count() - other_before, 1);
    assert_eq!(main_status.load().failures().len(), 256);
    assert!(matches!(
        large.try_next(),
        Some(Err(BlpError::Internal { .. }))
    ));
    assert!(matches!(
        unrelated.try_next(),
        Some(Err(BlpError::Internal { .. }))
    ));
}

#[tokio::test]
async fn isolated_and_non_market_aliases_create_independent_upstreams_on_the_same_handle() {
    for (service, topic, isolated) in [
        (MKTDATA, "IBM US Equity", true),
        ("//blp/mktvwap", "//blp/mktvwap/ticker/IBM US Equity", false),
    ] {
        let (hub, factory) = hub();
        let mut req = request(&[topic], &["BID"]);
        req.service = service.into();
        req.isolated = isolated;
        req.aliases = vec![(topic.into(), "first".into())];
        let mut stream = hub.subscribe(req).await.unwrap();
        let first = feed_for_label(&stream, "first");
        let session = first.session.test.clone().unwrap();
        let first_key = first.upstream_key.load(Ordering::Acquire);
        data(&session, first_key, r#"{"BID":1}"#);
        assert_eq!(next(&mut stream).topic.as_ref(), "first");
        stream
            .add(vec![topic.into()], vec![(topic.into(), "second".into())])
            .await
            .unwrap();
        let second = feed_for_label(&stream, "second");
        let second_key = second.upstream_key.load(Ordering::Acquire);
        assert_ne!(first_key, second_key);
        assert!(Arc::ptr_eq(&first.session, &second.session));
        assert_eq!(factory.sessions.lock().len(), 1);
        assert_eq!(session.subscribes.load(Ordering::Relaxed), 2);
        assert_eq!(hub.feeds().len(), 2);
        assert!(
            hub.feeds()
                .iter()
                .all(|feed| feed.isolated && feed.consumers == 1)
        );
        assert!(
            stream.try_next().is_none(),
            "isolated membership received a synthetic image"
        );

        session.status(second_key, "SubscriptionStarted");
        assert_eq!(
            stream.status().load().topic_statuses()["second"].state,
            TopicLifecycleState::Started
        );
        assert_eq!(
            stream.status().load().topic_statuses()["first"].state,
            TopicLifecycleState::Streaming
        );
        data(&session, second_key, r#"{"BID":2}"#);
        let row = next(&mut stream);
        assert_eq!(row.topic.as_ref(), "second");
        assert!(matches!(value(&row, "BID"), Some(UpdateValue::F64(2.0))));
        assert!(stream.try_next().is_none());
        session.status(second_key, "SubscriptionFailure");
        assert_eq!(stream.topics(), vec!["first"]);
        assert_eq!(stream.status().load().failures()[0].topic, "second");
        data(&session, first_key, r#"{"BID":3}"#);
        assert_eq!(next(&mut stream).topic.as_ref(), "first");
    }
}

fn assert_alias_error(error: BlpAsyncError, details: &[&str]) {
    let BlpAsyncError::ConfigError { detail } = error else {
        panic!("alias conflict must be ConfigError")
    };
    for expected in details {
        assert!(detail.contains(expected), "conflict omitted {expected}");
    }
}

#[tokio::test]
async fn initial_alias_conflicts_are_rejected_before_any_subscription_mutation() {
    for aliases in [
        vec![("IBM US Equity", "TECH"), ("AAPL US Equity", "TECH")],
        vec![("IBM US Equity", "first"), (" IBM US Equity ", "second")],
        vec![("IBM US Equity", "AAPL US Equity")],
    ] {
        let (hub, factory) = hub();
        let mut req = request(&["IBM US Equity", "AAPL US Equity"], &["BID"]);
        req.aliases = aliases
            .iter()
            .map(|(topic, label)| (topic.to_string(), label.to_string()))
            .collect();
        let error = hub
            .subscribe(req)
            .await
            .err()
            .expect("conflicting aliases must fail");
        let expected = if aliases.len() == 2 && aliases[0].0.trim() == aliases[1].0.trim() {
            vec!["IBM US Equity", "first", "second"]
        } else {
            vec!["IBM US Equity", "AAPL US Equity"]
        };
        assert_alias_error(error, &expected);
        assert!(hub.feeds().is_empty());
        assert!(factory.sessions.lock().is_empty());
        assert!(hub.consumers.lock().is_empty());
        assert_eq!(hub.next_consumer.load(Ordering::Relaxed), 1);
        assert_eq!(hub.next_feed.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn dynamic_alias_conflict_does_not_add_an_earlier_valid_topic() {
    let (hub, factory) = hub();
    let mut req = request(&["IBM US Equity"], &["BID"]);
    req.aliases = vec![("IBM US Equity".into(), "TECH".into())];
    let stream = hub.subscribe(req).await.unwrap();
    let status = stream.status();
    let before = status.publication_count();
    let next_feed = hub.next_feed.load(Ordering::Relaxed);
    let error = stream
        .add(
            vec!["MSFT US Equity".into(), "AAPL US Equity".into()],
            vec![("AAPL US Equity".into(), "TECH".into())],
        )
        .await
        .unwrap_err();
    assert_alias_error(error, &["TECH", "IBM US Equity", "AAPL US Equity"]);
    assert_eq!(stream.topics(), vec!["TECH"]);
    assert_eq!(status.publication_count(), before);
    assert_eq!(hub.next_feed.load(Ordering::Relaxed), next_feed);
    assert_eq!(hub.feeds().len(), 1);
    assert_eq!(
        factory.sessions.lock()[0]
            .subscribes
            .load(Ordering::Relaxed),
        1
    );
    let error = stream
        .add(
            vec!["AAPL US Equity".into()],
            vec![
                ("AAPL US Equity".into(), "one".into()),
                ("AAPL US Equity".into(), "two".into()),
            ],
        )
        .await
        .unwrap_err();
    assert_alias_error(error, &["AAPL US Equity", "one", "two"]);
    assert_eq!(status.publication_count(), before);
}

#[tokio::test]
async fn exact_duplicate_alias_pairs_deduplicate_without_extra_feeds() {
    let (hub, factory) = hub();
    let mut req = request(&["IBM US Equity", " IBM US Equity "], &["BID"]);
    req.aliases = vec![
        ("IBM US Equity".into(), "IBM".into()),
        (" IBM US Equity ".into(), "IBM".into()),
    ];
    let stream = hub.subscribe(req).await.unwrap();
    stream
        .add(
            vec!["IBM US Equity".into()],
            vec![("IBM US Equity".into(), "IBM".into())],
        )
        .await
        .unwrap();
    assert_eq!(stream.topics(), vec!["IBM"]);
    assert_eq!(hub.feeds().len(), 1);
    assert_eq!(
        factory.sessions.lock()[0]
            .subscribes
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn warning_drain_is_publication_free_when_empty_and_returns_pending_events_once() {
    let status = SubscriptionStatusHandle::default();
    let before = status.publication_count();
    for _ in 0..8 {
        assert!(status.take_warnings().is_empty());
    }
    assert_eq!(status.publication_count(), before);
    status.update(|state| {
        state.record_subscription_event(
            "DelayedStream",
            Some("IBM".into()),
            Some("synthetic delayed warning".into()),
            SubscriptionEventLevel::Warning,
        );
        state.record_subscription_event(
            "FieldException",
            Some("IBM".into()),
            Some("BID: BAD_FLD".into()),
            SubscriptionEventLevel::Warning,
        );
    });
    let before = status.publication_count();
    let warnings = status.take_warnings();
    assert_eq!(
        warnings
            .iter()
            .map(|event| event.message_type.as_str())
            .collect::<Vec<_>>(),
        vec!["DelayedStream", "FieldException"]
    );
    assert_eq!(status.publication_count() - before, 1);
    let before = status.publication_count();
    assert!(status.take_warnings().is_empty());
    assert_eq!(status.publication_count(), before);
    assert_eq!(
        status.load().events().len(),
        2,
        "draining warnings erased event history"
    );
}

#[tokio::test]
async fn warnings_survive_subscription_close_on_the_status_handle() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let status = stream.status();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"IS_DELAYED_STREAM":true}"#);
    stream.unsubscribe(false).await.unwrap();
    assert_eq!(status.take_warnings()[0].message_type, "DelayedStream");
    assert!(status.take_warnings().is_empty());
}

#[tokio::test]
async fn batched_topic_failures_keep_wire_order_kind_and_reason_with_linear_routing() {
    const COUNT: usize = 256;
    let (hub, _) = hub();
    let topics: Vec<_> = (0..COUNT)
        .map(|index| format!("BATCH{index} US Equity"))
        .collect();
    let mut stream = hub
        .subscribe(SubscribeRequest {
            topics: topics.clone(),
            fields: vec!["BID".into()],
            ..SubscribeRequest::default()
        })
        .await
        .unwrap();
    let source = feed_for_label(&stream, &topics[0]).session.clone();
    let session = source.test.clone().unwrap();
    let messages: Vec<_> = topics
        .iter()
        .enumerate()
        .rev()
        .map(|(index, topic)| {
            let key = feed_for_label(&stream, topic)
                .upstream_key
                .load(Ordering::Acquire);
            (
                key,
                if index % 2 == 0 {
                    "SubscriptionTerminated"
                } else {
                    "SubscriptionFailure"
                },
                format!("synthetic reason {index}"),
            )
        })
        .collect();
    let status = stream.status();
    let before = status.publication_count();
    let source_before = source.status.publication_count();
    let scans = source.status.load().index_scans;
    let visits = (
        source.routing.events.load(Ordering::Relaxed),
        source.routing.failures.load(Ordering::Relaxed),
        source.routing.feeds.load(Ordering::Relaxed),
    );
    session.status_batch(&messages);
    assert_eq!(status.publication_count() - before, 1);
    assert_eq!(source.status.publication_count() - source_before, 1);
    assert_eq!(source.status.load().index_scans - scans, 1);
    assert_eq!(
        source.routing.events.load(Ordering::Relaxed) - visits.0,
        COUNT
    );
    assert_eq!(
        source.routing.failures.load(Ordering::Relaxed) - visits.1,
        COUNT
    );
    assert_eq!(
        source.routing.feeds.load(Ordering::Relaxed) - visits.2,
        COUNT
    );
    let snapshot = status.load();
    assert_eq!(
        snapshot
            .failures()
            .iter()
            .map(|failure| failure.topic.as_str())
            .collect::<Vec<_>>(),
        topics.iter().rev().map(String::as_str).collect::<Vec<_>>()
    );
    for (failure, (_, message_type, reason)) in snapshot.failures().iter().zip(&messages) {
        assert_eq!(&failure.reason, reason);
        assert_eq!(
            failure.kind,
            if *message_type == "SubscriptionTerminated" {
                SubscriptionFailureKind::Terminated
            } else {
                SubscriptionFailureKind::Failure
            }
        );
    }
    let expected: Vec<_> = topics
        .iter()
        .rev()
        .zip(&messages)
        .skip(COUNT - super::super::subscription_status::SUBSCRIPTION_EVENT_HISTORY_LIMIT)
        .map(|(topic, (_, kind, reason))| {
            (kind.to_string(), Some(topic.clone()), Some(reason.clone()))
        })
        .collect();
    let events: Vec<_> = snapshot
        .events()
        .iter()
        .map(|event| {
            (
                event.message_type.clone(),
                event.topic.clone(),
                event.detail.clone(),
            )
        })
        .collect();
    assert_eq!(
        events, expected,
        "terminal events were duplicated or reordered"
    );
    drop(snapshot);
    assert!(
        matches!(stream.try_next(), Some(Err(BlpError::SubscriptionFailure { label: Some(reason), .. })) if reason == "synthetic reason 0")
    );
}

#[tokio::test]
async fn last_upstream_termination_preserves_terminal_kind_and_reason() {
    let (hub, _) = hub();
    let mut stream = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.status_batch(&[(
        key,
        "SubscriptionTerminated",
        "synthetic terminal reason".into(),
    )]);
    let status = stream.status();
    let snapshot = status.load();
    assert_eq!(
        snapshot.failures()[0].kind,
        SubscriptionFailureKind::Terminated
    );
    assert_eq!(snapshot.failures()[0].reason, "synthetic terminal reason");
    assert_eq!(
        snapshot
            .events()
            .iter()
            .filter(|event| event.message_type == "SubscriptionTerminated")
            .count(),
        1
    );
    assert!(
        matches!(stream.try_next(), Some(Err(BlpError::SubscriptionFailure { label: Some(reason), .. })) if reason == "synthetic terminal reason")
    );
}

#[tokio::test]
async fn mixed_session_metadata_merges_and_does_not_regress_when_one_session_dies() {
    let (hub, _) = hub();
    let _first = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let _second = hub
        .subscribe(request(&["AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut mixed = hub
        .subscribe(request(&["IBM US Equity", "AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    let (first, _) = test_feed(&hub, "IBM US Equity");
    let (second, second_key) = test_feed(&hub, "AAPL US Equity");
    first.service_event_for("ServiceUp", "//xbbg/synthetic");
    first.admin_event("SlowConsumerWarning");
    first.admin_event("SlowConsumerWarning");
    second.admin_event("SlowConsumerWarning");
    let status = mixed.status();
    assert_eq!(status.load().admin().slow_consumer_warning_count, 3);
    let warning_time = status.load().admin().last_warning_us;
    assert!(status.load().services().contains_key("//xbbg/synthetic"));
    second.admin_event("SlowConsumerWarningCleared");
    assert!(status.load().admin().slow_consumer_warning_active);
    first.session_event("SessionConnectionDown");
    first.session_event("SessionConnectionUp");
    first.session_event("SessionTerminated");
    {
        let snapshot = status.load();
        assert_eq!(snapshot.session().state, SessionLifecycleState::Up);
        assert_eq!(snapshot.session().disconnect_count, 1);
        assert_eq!(snapshot.session().reconnect_count, 1);
        assert_eq!(snapshot.admin().slow_consumer_warning_count, 3);
        assert_eq!(snapshot.admin().slow_consumer_cleared_count, 1);
        assert_eq!(snapshot.admin().last_warning_us, warning_time);
        assert!(!snapshot.admin().slow_consumer_warning_active);
        assert!(snapshot.services().contains_key("//xbbg/synthetic"));
        assert!(snapshot.services()[MKTDATA].up);
    }
    data(&second, second_key, r#"{"BID":10}"#);
    assert_eq!(next(&mut mixed).topic.as_ref(), "AAPL US Equity");
    second.admin_event("SlowConsumerWarning");
    assert_eq!(status.load().admin().slow_consumer_warning_count, 4);
    assert!(status.load().admin().last_warning_us >= warning_time);
}

fn image_request(topics: &[&str], fields: &[&str]) -> SubscribeRequest {
    SubscribeRequest {
        deliver_rows: false,
        stream_capacity: Some(1),
        ..request(topics, fields)
    }
}

#[tokio::test]
async fn image_only_updates_latest_without_rows_or_overflow_and_closes_its_receiver() {
    let (hub, _) = hub();
    let mut req = image_request(&["IBM US Equity"], &["BID"]);
    req.overflow_policy = Some(OverflowPolicy::Block);
    let mut stream = hub.subscribe(req).await.unwrap();
    assert!(!stream.handle.delivers_rows());
    let metric = stream
        .status()
        .load()
        .fields_metrics()
        .values()
        .next()
        .unwrap()
        .clone();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    for price in 0..128 {
        data(&session, key, &format!(r#"{{"BID":{price}}}"#));
    }
    assert!(stream.try_next().is_none());
    assert_eq!(metric.messages_received.load(Ordering::Relaxed), 128);
    assert_eq!(metric.batches_sent.load(Ordering::Relaxed), 0);
    assert_eq!(metric.dropped_batches.load(Ordering::Relaxed), 0);
    assert_eq!(
        stream
            .latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        127.0
    );
    let mut late = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    assert!(
        late.try_next().is_none(),
        "image-only late join enqueued a synthetic row"
    );
    assert_eq!(
        late.latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        127.0
    );
    let (mut receiver, handle) = stream.into_parts();
    handle.unsubscribe().await.unwrap();
    assert!(receiver.recv().await.is_none());
    assert!(matches!(handle.latest(), Err(BlpAsyncError::ChannelClosed)));
}

#[tokio::test]
async fn image_only_recovers_after_data_loss_while_row_consumers_fail_closed() {
    let (hub, _) = hub();
    let mut image = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID", "ASK"]))
        .await
        .unwrap();
    let mut rows = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    let metrics = image
        .status()
        .load()
        .fields_metrics()
        .values()
        .next()
        .unwrap()
        .clone();
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    next(&mut rows);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    assert!(matches!(
        rows.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    assert!(image.try_next().is_none());
    let latest = image.latest().unwrap();
    assert!(
        !latest
            .column_by_name("live")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        latest
            .column_by_name("ASK")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        11.0
    );
    assert_eq!(metrics.data_loss_events.load(Ordering::Relaxed), 1);
    let status = image.status();
    assert!(
        status
            .load()
            .events()
            .iter()
            .any(|event| event.message_type == "DataLoss"
                && event.category == super::super::SubscriptionEventCategory::Subscription
                && event.level == SubscriptionEventLevel::Warning)
    );
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 1);
    assert_eq!(hub.feeds()[0].consumers, 1);
    data(&session, key, r#"{"BID":99}"#);
    assert_eq!(
        image
            .latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        10.0,
        "recovery applied a delta before fresh paint"
    );
    data(
        &session,
        key,
        r#"{"BID":12,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let latest = image.latest().unwrap();
    assert_eq!(
        latest
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        12.0
    );
    assert!(
        latest.column_by_name("ASK").unwrap().is_null(0),
        "recovery retained a stale omitted field"
    );
    assert!(
        !latest
            .column_by_name("live")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        status
            .load()
            .events()
            .iter()
            .any(|event| event.message_type == "FeedRecovered"
                && event.level == SubscriptionEventLevel::Info)
    );
    data(&session, key, r#"{"ASK":13}"#);
    assert!(
        image
            .latest()
            .unwrap()
            .column_by_name("live")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(image.try_next().is_none());
}

#[tokio::test]
async fn repeated_loss_coalesces_to_one_recovery_restart_and_failures_stay_topic_local() {
    let (hub, _) = hub();
    let image = hub
        .subscribe(image_request(
            &["IBM US Equity", "AAPL US Equity"],
            &["BID"],
        ))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1}"#);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    for _ in 0..4 {
        data(
            &session,
            key,
            r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
        );
    }
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 1);
    data(
        &session,
        key,
        r#"{"BID":99,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
    session.status_batch(&[(
        key,
        "SubscriptionFailure",
        "synthetic recovery rejection".into(),
    )]);
    assert_eq!(image.topics(), vec!["AAPL US Equity"]);
    assert_eq!(
        image.status().load().failures()[0].reason,
        "synthetic recovery rejection"
    );
    let (other, other_key) = test_feed(&hub, "AAPL US Equity");
    data(&other, other_key, r#"{"BID":2}"#);
    assert_eq!(image.latest().unwrap().num_rows(), 1);
}

#[tokio::test]
async fn recovery_resubscribe_error_and_session_death_end_image_only_streams() {
    for session_death in [false, true] {
        let (hub, _) = hub();
        let mut image = hub
            .subscribe(image_request(&["IBM US Equity"], &["BID"]))
            .await
            .unwrap();
        let (session, key) = test_feed(&hub, "IBM US Equity");
        if session_death {
            session.session_event("SessionTerminated");
        } else {
            session.fail_resubscribe.lock().insert(key);
            data(
                &session,
                key,
                r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
            );
            cleanup(&hub).await;
        }
        assert!(matches!(image.try_next(), Some(Err(_))));
        assert!(matches!(image.latest(), Err(BlpAsyncError::ChannelClosed)));
        if session_death {
            assert_eq!(session.resubscribes.load(Ordering::Relaxed), 0);
        }
    }
}

#[tokio::test]
async fn repaint_resync_emits_only_changed_values_and_explicit_clears_with_metadata() {
    let (hub, _) = hub();
    let mut old = hub
        .subscribe(request(&["IBM US Equity"], &["BID", "ASK"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"ASK":2}"#);
    next(&mut old);
    let mut trigger = hub
        .subscribe(request(&["IBM US Equity"], &["LAST_PRICE"]))
        .await
        .unwrap();
    next(&mut trigger);
    data(
        &session,
        key,
        r#"{"BID":1,"ASK":null,"LAST_PRICE":3,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let row = next(&mut old);
    assert!(value(&row, "BID").is_none());
    assert!(matches!(value(&row, "ASK"), Some(UpdateValue::Null)));
    assert!(value(&row, "LAST_PRICE").is_none());
    assert_eq!(text_field(&row, EVENT_TYPE), Some("SUMMARY"));
    assert_eq!(text_field(&row, EVENT_SUBTYPE), Some("INITPAINT"));
    data(
        &session,
        key,
        r#"{"BID":1,"ASK":null,"LAST_PRICE":4,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert!(old.try_next().is_none());
    assert_eq!(
        old.latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );
}

#[tokio::test]
async fn field_error_policies_warn_ignore_or_reject_only_matching_topics() {
    for policy in [
        FieldErrorPolicy::Warn,
        FieldErrorPolicy::Ignore,
        FieldErrorPolicy::Raise,
    ] {
        let (hub, _) = hub();
        let mut req = image_request(&["IBM US Equity", "AAPL US Equity"], &["BID", "PX_BID"]);
        req.field_error_policy = policy;
        let stream = hub.subscribe(req).await.unwrap();
        let (session, key) = test_feed(&hub, "IBM US Equity");
        let json = r#"{"exceptions":[{"fieldId":"PX_BID","reason":{"category":"BAD_FLD","description":"synthetic","subcategory":"SYNTHETIC","errorCode":1,"source":"synthetic"}}]}"#;
        session.field_exceptions(key, json);
        assert_eq!(
            stream.status().load().field_errors()["IBM US Equity"]["PX_BID"],
            "BAD_FLD"
        );
        let warnings = stream.take_warnings();
        assert_eq!(
            warnings.len(),
            usize::from(policy == FieldErrorPolicy::Warn)
        );
        if policy == FieldErrorPolicy::Raise {
            assert_eq!(stream.topics(), vec!["AAPL US Equity"]);
            assert_eq!(
                stream.status().load().failures()[0].reason,
                "PX_BID: BAD_FLD"
            );
        } else {
            assert_eq!(stream.topics().len(), 2);
        }
        let mut late = image_request(&["IBM US Equity"], &["PX_BID"]);
        late.field_error_policy = FieldErrorPolicy::Raise;
        let mut late = hub.subscribe(late).await.unwrap();
        assert!(matches!(
            late.try_next(),
            Some(Err(BlpError::SubscriptionFailure { .. }))
        ));
    }
    assert_eq!(
        " RAISE ".parse::<FieldErrorPolicy>().unwrap(),
        FieldErrorPolicy::Raise
    );
    assert_eq!(
        "ignore".parse::<FieldErrorPolicy>().unwrap(),
        FieldErrorPolicy::Ignore
    );
    assert!(matches!(
        "invalid".parse::<FieldErrorPolicy>(),
        Err(BlpAsyncError::ConfigError { .. })
    ));
}

#[tokio::test]
async fn zero_as_null_changes_only_latest_materialization() {
    let (hub, _) = hub();
    let mut req = request(&["IBM US Equity"], &["BID", "ASK", "COUNT", "FLAG"]);
    req.zero_as_null = vec!["BID".into(), "COUNT".into(), "FLAG".into()];
    let mut masked = hub.subscribe(req).await.unwrap();
    let plain = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID", "COUNT"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.data(key, r#"<element name="BID" type="Float64"/><element name="ASK" type="Float64"/><element name="COUNT" type="Int64"/><element name="FLAG" type="Boolean"/>"#,
        r#"{"BID":-0.0,"ASK":0.0,"COUNT":0,"FLAG":false}"#);
    let row = next(&mut masked);
    assert!(matches!(value(&row, "BID"), Some(UpdateValue::F64(value)) if *value == 0.0));
    assert!(matches!(value(&row, "COUNT"), Some(UpdateValue::I64(0))));
    let latest = masked.latest().unwrap();
    assert!(latest.column_by_name("BID").unwrap().is_null(0));
    assert!(latest.column_by_name("COUNT").unwrap().is_null(0));
    assert!(latest.column_by_name("ASK").unwrap().is_valid(0));
    assert!(latest.column_by_name("FLAG").unwrap().is_valid(0));
    assert!(
        plain
            .latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .is_valid(0)
    );
    assert!(
        plain
            .latest()
            .unwrap()
            .column_by_name("COUNT")
            .unwrap()
            .is_valid(0)
    );
}

#[tokio::test]
async fn session_wait_timeout_preserves_shared_feed_after_creator_closes() {
    let (hub, factory) = hub_with_config(EngineConfig {
        max_subscription_sessions: 1,
        ..EngineConfig::default()
    });
    let owner = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut attached_request = image_request(&["IBM US Equity"], &["BID"]);
    attached_request.session_wait = Some(Duration::from_millis(20));
    let attached = hub.subscribe(attached_request).await.unwrap();
    owner.handle.unsubscribe().await.unwrap();
    let before = hub.feeds();
    let mut joining = image_request(&["IBM US Equity", "MSFT US Equity"], &["ASK"]);
    joining.session_wait = Some(Duration::from_millis(20));
    let result = tokio::time::timeout(Duration::from_millis(500), hub.subscribe(joining))
        .await
        .expect("session admission exceeded its bounded wait");
    assert!(
        matches!(result, Err(BlpAsyncError::ConfigError { detail }) if detail.starts_with("session_wait:"))
    );
    assert_eq!(hub.feeds(), before);
    assert_eq!(factory.sessions.lock().len(), 1);
    assert_eq!(attached.topics(), vec!["IBM US Equity"]);
    let before_status = attached.status().load().clone();
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        attached.add(vec!["MSFT US Equity".into()], vec![]),
    )
    .await
    .expect("add exceeded its bounded session wait");
    assert!(
        matches!(result, Err(BlpAsyncError::ConfigError { detail }) if detail.starts_with("session_wait:"))
    );
    assert_eq!(hub.feeds(), before);
    assert_eq!(attached.topics(), vec!["IBM US Equity"]);
    assert!(Arc::ptr_eq(&before_status, &attached.status().load()));
}

#[tokio::test]
async fn session_wait_claims_a_session_released_before_the_deadline() {
    let (hub, _) = hub_with_config(EngineConfig {
        max_subscription_sessions: 1,
        ..EngineConfig::default()
    });
    let owner = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut req = image_request(&["MSFT US Equity"], &["BID"]);
    req.session_wait = Some(Duration::from_secs(1));
    let waiting = hub.subscribe(req);
    tokio::pin!(waiting);
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    owner.handle.unsubscribe().await.unwrap();
    let joined = waiting.await.unwrap();
    assert_eq!(joined.topics(), vec!["MSFT US Equity"]);
    let (session, key) = test_feed(&hub, "MSFT US Equity");
    data(&session, key, r#"{"BID":42}"#);
    let latest = joined.latest().unwrap();
    assert_eq!(
        latest
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        42.0
    );
    assert_eq!(
        hub.feeds()
            .iter()
            .map(|feed| feed.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["MSFT US Equity"]
    );
}

#[tokio::test]
async fn session_wait_does_not_limit_startup_after_capacity_is_acquired() {
    for wait in [Duration::ZERO, Duration::from_millis(10)] {
        let (hub, factory) = hub_with_config(EngineConfig {
            max_subscription_sessions: 1,
            ..EngineConfig::default()
        });
        *factory.startup_delay.lock() = Duration::from_millis(40);
        let mut req = image_request(&["IBM US Equity"], &["BID"]);
        req.session_wait = Some(wait);
        let joined = tokio::time::timeout(Duration::from_secs(1), hub.subscribe(req))
            .await
            .expect("startup did not finish")
            .expect("available capacity was mistaken for an admission timeout");
        let (session, key) = test_feed(&hub, "IBM US Equity");
        data(&session, key, r#"{"BID":42}"#);
        let latest = joined.latest().unwrap();
        assert_eq!(
            latest
                .column_by_name("BID")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            42.0
        );
    }
}

#[tokio::test]
async fn session_wait_zero_still_attaches_existing_feeds_on_subscribe_and_add() {
    let (hub, factory) = hub_with_config(EngineConfig {
        max_subscription_sessions: 1,
        ..EngineConfig::default()
    });
    let _owner = hub
        .subscribe(image_request(
            &["IBM US Equity", "AAPL US Equity"],
            &["BID"],
        ))
        .await
        .unwrap();
    let mut req = image_request(&["IBM US Equity"], &["ASK"]);
    req.session_wait = Some(Duration::ZERO);
    let joined = hub.subscribe(req).await.unwrap();
    joined
        .add(vec!["AAPL US Equity".into()], vec![])
        .await
        .unwrap();
    assert_eq!(joined.topics(), vec!["IBM US Equity", "AAPL US Equity"]);
    assert_eq!(factory.sessions.lock().len(), 1);
    let before = hub.feeds();
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        joined.add(vec!["MSFT US Equity".into()], vec![]),
    )
    .await
    .expect("zero-wait add remained parked on session admission");
    assert!(
        matches!(result, Err(BlpAsyncError::ConfigError { detail }) if detail.starts_with("session_wait:"))
    );
    assert_eq!(hub.feeds(), before);
}

#[tokio::test]
async fn session_wait_deadline_is_not_reset_by_other_consumers_joining() {
    let (hub, _) = hub_with_config(EngineConfig {
        max_subscription_sessions: 1,
        ..EngineConfig::default()
    });
    let _owner = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut req = image_request(&["MSFT US Equity"], &["BID"]);
    req.session_wait = Some(Duration::from_millis(20));
    let waiting = hub.subscribe(req);
    tokio::pin!(waiting);
    let mut activity = tokio::time::interval(Duration::from_millis(1));
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            tokio::select! {
                biased;
                result = &mut waiting => break result,
                _ = activity.tick() => {
                    let temporary = hub.subscribe(image_request(&["IBM US Equity"], &["BID"])).await.unwrap();
                    temporary.handle.unsubscribe().await.unwrap();
                }
            }
        }
    }).await.expect("registry changes extended the admission deadline");
    assert!(
        matches!(result, Err(BlpAsyncError::ConfigError { detail }) if detail.starts_with("session_wait:"))
    );
    assert_eq!(
        hub.feeds()
            .iter()
            .map(|feed| feed.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["IBM US Equity"]
    );
}

#[tokio::test]
async fn session_wait_ends_when_the_waiting_consumer_closes_without_retiring_the_feed() {
    for drop_receiver in [false, true] {
        let (hub, _) = hub_with_config(EngineConfig {
            max_subscription_sessions: 1,
            ..EngineConfig::default()
        });
        let owner = hub
            .subscribe(request(&["IBM US Equity"], &["BID"]))
            .await
            .unwrap();
        let joined = hub
            .subscribe(request(&["IBM US Equity"], &["BID"]))
            .await
            .unwrap();
        let (receiver, handle) = joined.into_parts();
        let waiting = handle.add(vec!["MSFT US Equity".into()], vec![]);
        tokio::pin!(waiting);
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        if drop_receiver {
            drop(receiver);
        } else {
            handle.unsubscribe().await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_millis(500), waiting)
            .await
            .expect("closed consumer remained parked on session admission");
        assert!(matches!(result, Err(BlpAsyncError::ChannelClosed)));
        assert_eq!(owner.topics(), vec!["IBM US Equity"]);
        assert_eq!(
            hub.feeds()
                .iter()
                .map(|feed| feed.topic.as_str())
                .collect::<Vec<_>>(),
            vec!["IBM US Equity"]
        );
    }
}

#[tokio::test]
async fn session_wait_rejects_unrepresentable_deadline_without_mutation() {
    let (hub, _) = hub_with_config(EngineConfig {
        max_subscription_sessions: 1,
        ..EngineConfig::default()
    });
    let _owner = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let mut req = image_request(&["IBM US Equity"], &["BID"]);
    req.session_wait = Some(Duration::MAX);
    let joined = hub.subscribe(req).await.unwrap();
    let before = hub.feeds();
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        joined.add(vec!["MSFT US Equity".into()], vec![]),
    )
    .await
    .expect("unrepresentable bound silently became an unbounded wait");
    assert!(
        matches!(result, Err(BlpAsyncError::ConfigError { detail }) if detail.starts_with("session_wait:"))
    );
    assert_eq!(hub.feeds(), before);
    assert_eq!(joined.topics(), vec!["IBM US Equity"]);
}

#[tokio::test]
async fn latest_distinguishes_empty_open_handles_from_closed_and_failed_streams() {
    let (hub, _) = hub();
    let open = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    open.remove(vec!["IBM US Equity".into()]).await.unwrap();
    assert_eq!(open.latest().unwrap().num_rows(), 0);
    let (_, handle) = open.into_parts();
    assert!(
        matches!(handle.latest(), Err(BlpAsyncError::ChannelClosed)),
        "dropped receiver remained readable"
    );
    let stream = hub
        .subscribe(image_request(&["AAPL US Equity"], &["BID"]))
        .await
        .unwrap();
    stream.handle.unsubscribe().await.unwrap();
    assert!(matches!(stream.latest(), Err(BlpAsyncError::ChannelClosed)));
}

#[tokio::test]
async fn metadata_seeds_stream_and_image_types_before_values_and_after_field_growth() {
    use crate::engine::subscription_types::SubscriptionTypeResolver;
    use crate::field_cache::FieldTypeResolver;
    let directory = tempfile::tempdir().unwrap();
    let cache = Arc::new(FieldTypeResolver::with_cache_path(
        directory.path().join("fields.json"),
    ));
    let queries = Arc::new(AtomicUsize::new(0));
    let count = queries.clone();
    let resolver = Arc::new(SubscriptionTypeResolver::with_query(
        cache,
        tokio::runtime::Handle::current(),
        move |fields| {
            count.fetch_add(1, Ordering::Relaxed);
            let types: Vec<_> = fields
                .iter()
                .map(|field| match field.as_str() {
                    "BID" | "ASK" => Some("float64"),
                    "TEST_DATE" => Some("date32"),
                    "TEST_TIME" => Some("time64"),
                    "FLAG" => Some("bool"),
                    _ => None,
                })
                .collect();
            let batch = RecordBatch::try_from_iter([
                (
                    "field",
                    Arc::new(StringArray::from(fields.clone())) as ArrayRef,
                ),
                ("type", Arc::new(StringArray::from(types)) as ArrayRef),
            ])
            .unwrap();
            Box::pin(std::future::ready(Ok(batch)))
        },
    ));
    let (mut hub, _) = hub();
    Arc::get_mut(&mut hub).unwrap().type_resolver = Some(resolver);
    let stream = hub
        .subscribe(image_request(
            &["IBM US Equity"],
            &["BID", "TEST_DATE", "TEST_TIME", "FLAG", "MISSING"],
        ))
        .await
        .unwrap();
    let latest = stream.latest().unwrap();
    assert_eq!(
        latest.schema().field_with_name("BID").unwrap().data_type(),
        &arrow_schema::DataType::Float64
    );
    assert_eq!(
        latest
            .schema()
            .field_with_name("TEST_DATE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Date32
    );
    assert_eq!(
        latest
            .schema()
            .field_with_name("TEST_TIME")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond)
    );
    assert_eq!(
        latest.schema().field_with_name("FLAG").unwrap().data_type(),
        &arrow_schema::DataType::Boolean
    );
    assert_eq!(
        latest
            .schema()
            .field_with_name("MISSING")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Utf8
    );
    stream
        .add(vec!["AAPL US Equity".into()], vec![])
        .await
        .unwrap();
    stream.add_fields(vec!["ASK".into()]).await.unwrap();
    assert_eq!(
        stream
            .latest()
            .unwrap()
            .schema()
            .field_with_name("ASK")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Float64
    );
    let feed = feed_for_label(&stream, "IBM US Equity");
    assert_eq!(
        feed.state
            .lock()
            .layout
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name.as_ref() == "ASK")
            .unwrap()
            .kind,
        FieldKind::F64
    );
    assert!(queries.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn unattributable_admin_loss_starts_one_recovery_per_image_feed() {
    let (hub, _) = hub();
    let image = hub
        .subscribe(image_request(
            &["IBM US Equity", "AAPL US Equity"],
            &["BID"],
        ))
        .await
        .unwrap();
    let metrics: Vec<_> = image
        .status()
        .load()
        .fields_metrics()
        .values()
        .cloned()
        .collect();
    let (session, _) = test_feed(&hub, "IBM US Equity");
    session.admin_event("DataLoss");
    assert!(
        metrics
            .iter()
            .all(|metric| metric.data_loss_events.load(Ordering::Relaxed) == 1)
    );
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
    assert_eq!(
        image
            .status()
            .load()
            .events()
            .iter()
            .filter(|event| event.message_type == "DataLoss"
                && event.category == super::super::SubscriptionEventCategory::Subscription)
            .count(),
        2
    );
}

#[tokio::test]
async fn superseded_recovery_paint_is_not_published_or_retained_in_the_restart_image() {
    let (hub, _) = hub();
    let image = hub
        .subscribe(image_request(&["IBM US Equity"], &["BID", "ASK"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1,"ASK":2}"#);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    session.status(key, "SubscriptionStarted");
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    assert_eq!(
        session.resubscribes.load(Ordering::Relaxed),
        1,
        "two recovery requests were in flight"
    );
    data(
        &session,
        key,
        r#"{"BID":99,"ASK":100,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert_eq!(
        image
            .latest()
            .unwrap()
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );
    assert!(
        !image
            .status()
            .load()
            .events()
            .iter()
            .any(|event| event.message_type == "FeedRecovered")
    );
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
    data(
        &session,
        key,
        r#"{"ASK":101,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert_eq!(
        image
            .latest()
            .unwrap()
            .column_by_name("ASK")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0,
        "stale paint crossed the restart boundary"
    );
    session.status(key, "SubscriptionStarted");
    data(
        &session,
        key,
        r#"{"BID":3,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let latest = image.latest().unwrap();
    assert_eq!(
        latest
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.0
    );
    assert!(latest.column_by_name("ASK").unwrap().is_null(0));
    assert_eq!(
        image
            .status()
            .load()
            .events()
            .iter()
            .filter(|event| event.message_type == "FeedRecovered")
            .count(),
        1
    );
}

#[tokio::test]
async fn ordinary_update_before_union_paint_does_not_disarm_sparse_repaint_resync() {
    let (hub, _) = hub();
    let mut old = hub
        .subscribe(request(&["IBM US Equity"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "IBM US Equity");
    data(&session, key, r#"{"BID":1}"#);
    next(&mut old);
    let mut joined = hub
        .subscribe(request(&["IBM US Equity"], &["ASK"]))
        .await
        .unwrap();
    next(&mut joined);
    data(
        &session,
        key,
        r#"{"BID":2,"MKTDATA_EVENT_SUBTYPE":"UPDATE"}"#,
    );
    next(&mut old);
    data(
        &session,
        key,
        r#"{"BID":2,"ASK":3,"MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert!(
        old.try_next().is_none(),
        "unchanged old field was emitted after an in-flight delta"
    );
    assert!(matches!(
        value(&next(&mut joined), "ASK"),
        Some(UpdateValue::F64(3.0))
    ));
}

#[tokio::test]
async fn int32_stream_observations_replace_widened_metadata_hints_even_with_unseen_siblings() {
    use crate::engine::subscription_types::SubscriptionTypeResolver;
    use crate::field_cache::{FieldInfo, FieldTypeResolver};
    let directory = tempfile::tempdir().unwrap();
    let cache = Arc::new(FieldTypeResolver::with_cache_path(
        directory.path().join("fields.json"),
    ));
    cache.insert(FieldInfo {
        field_id: "ASK_SIZE".into(),
        arrow_type: "int64".into(),
        description: String::new(),
        category: String::new(),
    });
    let resolver = Arc::new(SubscriptionTypeResolver::with_query(
        cache,
        tokio::runtime::Handle::current(),
        |_| Box::pin(std::future::ready(Err(BlpAsyncError::ChannelClosed))),
    ));
    let (mut hub, _) = hub();
    Arc::get_mut(&mut hub).unwrap().type_resolver = Some(resolver);
    let mut stream = hub
        .subscribe(request(&["IBM US Equity", "AAPL US Equity"], &["ASK_SIZE"]))
        .await
        .unwrap();
    assert_eq!(
        stream
            .latest()
            .unwrap()
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int64
    );
    let (session, key) = test_feed(&hub, "IBM US Equity");
    session.data(
        key,
        r#"<element name="ASK_SIZE" type="Int32" minOccurs="0"/>"#,
        r#"{"ASK_SIZE":null}"#,
    );
    let clear = next(&mut stream);
    assert_eq!(clear.layout.fields[0].kind, FieldKind::I32);
    assert_eq!(
        stream
            .latest()
            .unwrap()
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int32
    );
    session.data(
        key,
        r#"<element name="ASK_SIZE" type="Int32"/>"#,
        r#"{"ASK_SIZE":5}"#,
    );
    assert_eq!(next(&mut stream).layout.fields[0].kind, FieldKind::I32);
    let late = hub
        .subscribe(image_request(&["IBM US Equity"], &["ASK_SIZE"]))
        .await
        .unwrap();
    let latest = late.latest().unwrap();
    assert_eq!(
        latest
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int32
    );
    assert_eq!(
        latest
            .column_by_name("ASK_SIZE")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(0),
        5
    );
}

fn state_request(topics: &[&str], fields: &[&str]) -> SubscribeRequest {
    SubscribeRequest {
        delivery: SubscriptionDeliveryMode::State,
        ..request(topics, fields)
    }
}

fn state_consumer(
    stream: SubscriptionStream,
) -> crate::subscription_consumer::SubscriptionConsumer {
    let (receiver, handle) = stream.into_parts();
    crate::subscription_consumer::SubscriptionConsumer::new(receiver, handle)
}

fn state_number(batch: &RecordBatch, name: &str, row: usize) -> f64 {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row)
}

fn state_flag(batch: &RecordBatch, name: &str, row: usize) -> bool {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(row)
}

fn state_label(batch: &RecordBatch, row: usize) -> &str {
    batch
        .column_by_name("topic")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}
struct ResumeSnapshot(Arc<std::sync::Barrier>);

impl Drop for ResumeSnapshot {
    fn drop(&mut self) {
        self.0.wait();
    }
}

fn assert_state_recovery_epochs(deliveries: &[SubscriptionDelivery], bid: f64) {
    assert_eq!(deliveries.len(), 2);
    assert!(!state_flag(&deliveries[0].data, "valid", 0));
    assert!(deliveries[0].data.column_by_name("BID").unwrap().is_null(0));
    assert!(deliveries[0].data.column_by_name("ASK").unwrap().is_null(0));
    assert!(state_flag(&deliveries[1].data, "valid", 0));
    assert_eq!(state_number(&deliveries[1].data, "BID", 0), bid);
    assert!(deliveries[1].data.column_by_name("ASK").unwrap().is_null(0));
}

#[tokio::test]
async fn state_delivery_conflates_sparse_images_without_starving_quiet_topics() {
    let (hub, factory) = hub();
    let stream = hub
        .subscribe(state_request(
            &["SYNTH HOT", "SYNTH QUIET"],
            &["BID", "ASK"],
        ))
        .await
        .unwrap();
    let handle = stream.handle.clone();
    let state = state_consumer(stream);
    let initial = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(!state_flag(&initial.data, "valid", 0));
    assert_eq!(state_label(&initial.data, 0), "SYNTH HOT");
    let mut strict_request = request(&["SYNTH HOT"], &["BID"]);
    strict_request.stream_capacity = Some(1);
    let mut strict = hub.subscribe(strict_request).await.unwrap();
    let (hot, hot_key) = test_feed(&hub, "SYNTH HOT");
    let (quiet, quiet_key) = test_feed(&hub, "SYNTH QUIET");
    data(&quiet, quiet_key, r#"{"BID":42,"ASK":43}"#);
    data(&hot, hot_key, r#"{"BID":1,"ASK":2}"#);
    data(&hot, hot_key, r#"{"ASK":null}"#);
    for bid in 3..1024 {
        data(&hot, hot_key, &format!(r#"{{"BID":{bid}}}"#));
    }
    assert_eq!(
        state.rx.lock().await.as_ref().unwrap().len(),
        2,
        "dirty storage must be bounded by active topics, not update count"
    );
    let first = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(first.kind, SubscriptionDeliveryMode::State);
    assert!(first.gap_before.is_none());
    assert_eq!(state_label(&first.data, 0), "SYNTH QUIET");
    assert!(state_flag(&first.data, "valid", 0));
    assert_eq!(state_number(&first.data, "BID", 0), 42.0);
    assert_eq!(state_number(&first.data, "ASK", 0), 43.0);
    let second = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(state_label(&second.data, 0), "SYNTH HOT");
    assert!(state_flag(&second.data, "valid", 0));
    assert_eq!(state_number(&second.data, "BID", 0), 1023.0);
    assert!(second.data.column_by_name("ASK").unwrap().is_null(0));
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
    assert!(matches!(
        value(&next(&mut strict), "BID"),
        Some(UpdateValue::F64(1.0))
    ));
    assert!(matches!(
        strict.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    assert!(
        handle.is_active(),
        "strict local overflow invalidated shared state"
    );
    assert_eq!(factory.sessions.lock().len(), 1);
    for metrics in handle.status().load().fields_metrics().values() {
        assert_eq!(metrics.batches_sent.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.dropped_batches.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn cancelled_state_receipt_re_reads_current_image_instead_of_replaying_a_batch() {
    let (hub, _) = hub();
    let state = state_consumer(
        hub.subscribe(state_request(&["SYNTH CANCEL"], &["BID", "ASK"]))
            .await
            .unwrap(),
    );
    state.next_delivery(1).await.unwrap().commit().unwrap();
    let (session, key) = test_feed(&hub, "SYNTH CANCEL");
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    let cancelled = state.next_delivery(1).await.unwrap();
    assert_eq!(
        state_number(&cancelled.as_ref().unwrap().data, "BID", 0),
        10.0
    );
    data(&session, key, r#"{"BID":12}"#);
    data(&session, key, r#"{"ASK":null}"#);
    drop(cancelled);
    let current = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(state_number(&current.data, "BID", 0), 12.0);
    assert!(current.data.column_by_name("ASK").unwrap().is_null(0));
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn state_membership_generation_survives_alias_readd_and_field_type_growth() {
    let (hub, _) = hub();
    let _poller = hub
        .subscribe(image_request(&["SYNTH ALIAS"], &["BID", "ASK"]))
        .await
        .unwrap();
    let mut req = state_request(&["SYNTH ALIAS"], &["BID"]);
    req.aliases = vec![("SYNTH ALIAS".into(), "board".into())];
    let stream = hub.subscribe(req).await.unwrap();
    let handle = stream.handle.clone();
    let state = state_consumer(stream);
    state.next_delivery(1).await.unwrap().commit().unwrap();
    let (session, key) = test_feed(&hub, "SYNTH ALIAS");
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    let removed = state.next_delivery(1).await.unwrap();
    handle.remove(vec!["board".into()]).await.unwrap();
    data(&session, key, r#"{"BID":20,"ASK":21}"#);
    handle
        .add(
            vec!["SYNTH ALIAS".into()],
            vec![("SYNTH ALIAS".into(), "board".into())],
        )
        .await
        .unwrap();
    drop(removed);
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 1);
    let readded = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(state_label(&readded.data, 0), "board");
    assert_eq!(state_number(&readded.data, "BID", 0), 20.0);
    handle.add_fields(vec!["ASK".into()]).await.unwrap();
    let grown = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(state_number(&grown.data, "ASK", 0), 21.0);
    data(&session, key, r#"{"BID":"changed"}"#);
    let changed = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert_eq!(
        changed
            .data
            .column_by_name("BID")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "changed"
    );
    assert_eq!(state_number(&changed.data, "ASK", 0), 21.0);
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn state_delivers_invalidation_before_recovered_image_even_when_unread() {
    let (hub, _) = hub();
    let state = state_consumer(
        hub.subscribe(state_request(&["SYNTH RECOVER"], &["BID", "ASK"]))
            .await
            .unwrap(),
    );
    let mut strict = hub
        .subscribe(request(&["SYNTH RECOVER"], &["BID"]))
        .await
        .unwrap();
    let mut lossy_request = request(&["SYNTH RECOVER"], &["BID"]);
    lossy_request.overflow_policy = Some(OverflowPolicy::DropOldest);
    lossy_request.stream_capacity = Some(1);
    let lossy = state_consumer(hub.subscribe(lossy_request).await.unwrap());
    let (session, key) = test_feed(&hub, "SYNTH RECOVER");
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    next(&mut strict);
    lossy.next_delivery(1).await.unwrap().commit().unwrap();
    state.next_delivery(1).await.unwrap().commit().unwrap();
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    assert!(matches!(
        strict.try_next(),
        Some(Err(BlpError::SubscriptionDataLoss { .. }))
    ));
    assert!(matches!(
        lossy.next_delivery(1).await.unwrap().commit(),
        Err(BlpError::SubscriptionDataLoss { .. })
    ));
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 1);
    data(&session, key, r#"{"BID":99}"#);
    data(
        &session,
        key,
        r#"{"BID":12,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let cancelled = state.next_delivery(1).await.unwrap();
    assert!(!state_flag(&cancelled.as_ref().unwrap().data, "valid", 0));
    assert!(
        cancelled
            .as_ref()
            .unwrap()
            .data
            .column_by_name("BID")
            .unwrap()
            .is_null(0)
    );
    drop(cancelled);
    let invalid = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(!state_flag(&invalid.data, "valid", 0));
    assert!(!state_flag(&invalid.data, "live", 0));
    assert!(invalid.data.column_by_name("ASK").unwrap().is_null(0));
    let recovered = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&recovered.data, "valid", 0));
    assert_eq!(state_number(&recovered.data, "BID", 0), 12.0);
    assert!(recovered.data.column_by_name("ASK").unwrap().is_null(0));
    assert!(!state_flag(&recovered.data, "live", 0));
    data(&session, key, r#"{"ASK":13}"#);
    let live = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&live.data, "valid", 0));
    assert!(state_flag(&live.data, "live", 0));
    assert_eq!(state_number(&live.data, "BID", 0), 12.0);
    assert_eq!(state_number(&live.data, "ASK", 0), 13.0);
}

#[tokio::test]
async fn state_terminal_error_wakes_reader_and_survives_cancelled_publication() {
    let (hub, _) = hub();
    let state = Arc::new(state_consumer(
        hub.subscribe(state_request(&["SYNTH END"], &["BID"]))
            .await
            .unwrap(),
    ));
    state.next_delivery(1).await.unwrap().commit().unwrap();
    let reader = state.clone();
    let waiting = tokio::spawn(async move { reader.next_delivery(1).await });
    tokio::task::yield_now().await;
    let (session, _) = test_feed(&hub, "SYNTH END");
    session.session_event("SessionTerminated");
    let terminal = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(terminal.as_ref().is_err());
    drop(terminal);
    let terminal = state.next_delivery(1).await.unwrap();
    assert!(terminal.commit().is_err());
    assert!(state.next_delivery(1).await.is_none());
}

#[tokio::test]
async fn state_delivery_drain_freezes_dirty_images_before_detaching_last_membership() {
    let (hub, _) = hub();
    let mut state = hub
        .subscribe(state_request(&["SYNTH DRAIN"], &["BID", "ASK"]))
        .await
        .unwrap()
        .deliveries();
    state.next().await.unwrap().unwrap();
    let (session, key) = test_feed(&hub, "SYNTH DRAIN");
    data(&session, key, r#"{"BID":1,"ASK":2}"#);
    data(&session, key, r#"{"BID":3}"#);
    data(&session, key, r#"{"ASK":null}"#);
    let drained = state.unsubscribe(true).await.unwrap();
    assert_eq!(drained.len(), 1);
    assert_eq!(state_number(&drained[0].data, "BID", 0), 3.0);
    assert!(drained[0].data.column_by_name("ASK").unwrap().is_null(0));
    assert!(state_flag(&drained[0].data, "valid", 0));
    assert!(hub.feeds().is_empty());
    assert_eq!(session.unsubscribes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prepared_state_drain_rolls_back_to_the_final_image_not_a_borrowed_snapshot() {
    let (hub, _) = hub();
    let _poller = hub
        .subscribe(image_request(&["SYNTH FREEZE"], &["BID"]))
        .await
        .unwrap();
    let stream = hub
        .subscribe(state_request(&["SYNTH FREEZE"], &["BID"]))
        .await
        .unwrap();
    let (mut receiver, handle) = stream.into_parts();
    receiver.recv_state(1).await.unwrap().commit().unwrap();
    let (session, key) = test_feed(&hub, "SYNTH FREEZE");
    data(&session, key, r#"{"BID":1}"#);
    let mut borrowed = receiver.recv_state(1).await.unwrap();
    let moved = borrowed.take_success().unwrap();
    assert_eq!(state_number(&moved.data, "BID", 0), 1.0);
    data(&session, key, r#"{"BID":3}"#);
    receiver.prepare_close();
    handle.unsubscribe().await.unwrap();
    data(&session, key, r#"{"BID":4}"#);
    drop(moved);
    drop(borrowed);
    let mut receipt = receiver.try_recv_state(1).unwrap();
    let final_image = receipt.take_success().unwrap();
    receipt.acknowledge();
    assert_eq!(state_number(&final_image.data, "BID", 0), 3.0);
    assert!(receiver.try_recv_state(1).is_none());
}

#[tokio::test]
async fn state_drain_reports_source_terminal_error_instead_of_successful_partial_state() {
    let (hub, _) = hub();
    let state = hub
        .subscribe(state_request(&["SYNTH DRAIN ERROR"], &["BID"]))
        .await
        .unwrap()
        .deliveries();
    let (session, key) = test_feed(&hub, "SYNTH DRAIN ERROR");
    data(&session, key, r#"{"BID":1}"#);
    session.session_event("SessionTerminated");
    assert!(matches!(
        state.unsubscribe(true).await,
        Err(BlpAsyncError::Blp(_))
    ));
}

#[tokio::test]
async fn state_options_reject_contradictions_before_claiming_or_mutating() {
    for contradictory in [
        SubscribeRequest {
            deliver_rows: false,
            ..state_request(&["SYNTH VALIDATE"], &["BID"])
        },
        SubscribeRequest {
            stream_capacity: Some(8),
            ..state_request(&["SYNTH VALIDATE"], &["BID"])
        },
        SubscribeRequest {
            overflow_policy: Some(OverflowPolicy::DropNewest),
            ..state_request(&["SYNTH VALIDATE"], &["BID"])
        },
        SubscribeRequest {
            overflow_policy: Some(OverflowPolicy::DropOldest),
            ..state_request(&["SYNTH VALIDATE"], &["BID"])
        },
        SubscribeRequest {
            overflow_policy: Some(OverflowPolicy::Block),
            ..state_request(&["SYNTH VALIDATE"], &["BID"])
        },
    ] {
        let (hub, factory) = hub();
        assert!(matches!(
            hub.subscribe(contradictory).await,
            Err(BlpAsyncError::ConfigError { .. })
        ));
        assert!(hub.feeds().is_empty());
        assert!(factory.sessions.lock().is_empty());
        assert!(hub.consumers.lock().is_empty());
    }
}

#[tokio::test]
async fn state_ignores_inherited_raw_overflow_and_retains_all_field_discoveries() {
    for overflow_policy in [OverflowPolicy::DropOldest, OverflowPolicy::Block] {
        let (hub, _) = hub_with_config(EngineConfig {
            overflow_policy,
            subscription_stream_capacity: 1,
            ..EngineConfig::default()
        });
        let mut req = state_request(&["SYNTH ALL"], &["BID"]);
        req.all_fields = true;
        req.flush_threshold = Some(4);
        req.zero_as_null = vec!["BID".into()];
        let stream = hub.subscribe(req).await.unwrap();
        let handle = stream.handle.clone();
        assert!(handle.inner.forwarder.is_none());
        assert_eq!(handle.delivery_batch_limit(), 4);
        let state = state_consumer(stream);
        let (session, key) = test_feed(&hub, "SYNTH ALL");
        for bid in 0..256 {
            data(
                &session,
                key,
                &format!(r#"{{"BID":{bid},"EXTRA":"retained"}}"#),
            );
        }
        data(&session, key, r#"{"BID":0}"#);
        let latest = state.next_delivery(4).await.unwrap().commit().unwrap();
        assert_eq!(latest.data.num_rows(), 1);
        assert!(state_flag(&latest.data, "valid", 0));
        assert!(latest.data.column_by_name("BID").unwrap().is_null(0));
        assert_eq!(
            latest
                .data
                .column_by_name("EXTRA")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "retained"
        );
        assert!(handle.is_active());
        let (_operation, result) = state.unsubscribe_deliveries(false, None).await;
        assert!(result.unwrap().is_empty());
    }
}

#[tokio::test]
async fn unattributable_admin_loss_invalidates_every_state_topic_and_wakes_a_waiter() {
    let (hub, _) = hub();
    let mut req = state_request(&["SYNTH ADMIN A", "SYNTH ADMIN B"], &["BID"]);
    req.flush_threshold = Some(2);
    let state = Arc::new(state_consumer(hub.subscribe(req).await.unwrap()));
    let (session, first_key) = test_feed(&hub, "SYNTH ADMIN A");
    let (_, second_key) = test_feed(&hub, "SYNTH ADMIN B");
    data(&session, first_key, r#"{"BID":1}"#);
    data(&session, second_key, r#"{"BID":2}"#);
    let initial = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(initial.data.num_rows(), 2);
    let reader = state.clone();
    let waiting = tokio::spawn(async move { reader.next_delivery(2).await });
    tokio::task::yield_now().await;
    session.admin_event("DataLoss");
    let invalid = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(invalid.data.num_rows(), 2);
    for row in 0..2 {
        assert!(!state_flag(&invalid.data, "valid", row));
        assert!(invalid.data.column_by_name("BID").unwrap().is_null(row));
    }
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn state_schema_retains_discoveries_and_promotions_when_only_other_topics_are_dirty() {
    let (hub, _) = hub();
    let mut req = state_request(&["SYNTH SCHEMA A", "SYNTH SCHEMA B"], &["BID"]);
    req.all_fields = true;
    let state = state_consumer(hub.subscribe(req).await.unwrap());
    let (session, first_key) = test_feed(&hub, "SYNTH SCHEMA A");
    let (_, second_key) = test_feed(&hub, "SYNTH SCHEMA B");
    data(&session, first_key, r#"{"BID":1,"EXTRA":42}"#);
    data(&session, second_key, r#"{"BID":2}"#);
    let both = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(both.data.num_rows(), 2);
    assert_eq!(state_number(&both.data, "EXTRA", 0), 42.0);
    assert!(both.data.column_by_name("EXTRA").unwrap().is_null(1));
    assert!(
        state_flag(&both.data, "valid", 1),
        "unknown fields do not invalidate continuity"
    );
    data(&session, second_key, r#"{"BID":3}"#);
    let only_second = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(only_second.data.num_rows(), 1);
    assert_eq!(state_label(&only_second.data, 0), "SYNTH SCHEMA B");
    assert_eq!(
        only_second
            .data
            .schema()
            .field_with_name("EXTRA")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Float64
    );
    assert!(only_second.data.column_by_name("EXTRA").unwrap().is_null(0));
    data(&session, second_key, r#"{"EXTRA":"promoted"}"#);
    let promoted = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(
        promoted
            .data
            .column_by_name("EXTRA")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "promoted"
    );
    data(&session, first_key, r#"{"BID":4}"#);
    let narrower = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(state_label(&narrower.data, 0), "SYNTH SCHEMA A");
    assert_eq!(
        narrower
            .data
            .schema()
            .field_with_name("EXTRA")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Utf8,
        "a narrower dirty-topic subset downgraded the observed union"
    );
    assert!(narrower.data.column_by_name("EXTRA").unwrap().is_valid(0));
}

#[tokio::test]
async fn state_schema_yields_metadata_hints_to_observed_types_and_ignores_quiet_hints() {
    use crate::engine::subscription_types::SubscriptionTypeResolver;
    use crate::field_cache::{FieldInfo, FieldTypeResolver};
    let directory = tempfile::tempdir().unwrap();
    let cache = Arc::new(FieldTypeResolver::with_cache_path(
        directory.path().join("fields.json"),
    ));
    cache.insert(FieldInfo {
        field_id: "ASK_SIZE".into(),
        arrow_type: "int64".into(),
        description: String::new(),
        category: String::new(),
    });
    let resolver = Arc::new(SubscriptionTypeResolver::with_query(
        cache,
        tokio::runtime::Handle::current(),
        |_| Box::pin(std::future::ready(Err(BlpAsyncError::ChannelClosed))),
    ));
    let (mut hub, _) = hub();
    Arc::get_mut(&mut hub).unwrap().type_resolver = Some(resolver);
    let state = state_consumer(
        hub.subscribe(state_request(
            &["SYNTH HINT A", "SYNTH HINT B"],
            &["ASK_SIZE"],
        ))
        .await
        .unwrap(),
    );
    let seeded = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(
        seeded
            .data
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int64
    );
    let (session, first_key) = test_feed(&hub, "SYNTH HINT A");
    let (_, second_key) = test_feed(&hub, "SYNTH HINT B");
    session.data(
        first_key,
        r#"<element name="ASK_SIZE" type="Int32" minOccurs="0"/>"#,
        r#"{"ASK_SIZE":null}"#,
    );
    let observed = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(
        observed
            .data
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int32
    );
    data(
        &session,
        second_key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let hint_only = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(state_label(&hint_only.data, 0), "SYNTH HINT B");
    assert_eq!(
        hint_only
            .data
            .schema()
            .field_with_name("ASK_SIZE")
            .unwrap()
            .data_type(),
        &arrow_schema::DataType::Int32,
        "an unseen sibling's provisional Int64 hint replaced the observed Int32"
    );
    assert!(
        hint_only
            .data
            .column_by_name("ASK_SIZE")
            .unwrap()
            .is_null(0)
    );
}

#[tokio::test]
async fn state_plain_reads_do_not_consume_images_and_plain_drain_still_cleans_up() {
    let (hub, _) = hub();
    let mut stream = hub
        .subscribe(state_request(&["SYNTH GUARDED"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "SYNTH GUARDED");
    data(&session, key, r#"{"BID":5}"#);
    assert!(matches!(stream.try_next(), Some(Err(_))));
    let mut deliveries = stream.deliveries();
    let state = deliveries.next().await.unwrap().unwrap();
    assert_eq!(state_number(&state.data, "BID", 0), 5.0);
    deliveries.unsubscribe(false).await.unwrap();
    assert!(hub.feeds().is_empty());
    let stream = hub
        .subscribe(state_request(&["SYNTH GUARDED DRAIN"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "SYNTH GUARDED DRAIN");
    data(&session, key, r#"{"BID":6}"#);
    assert!(stream.unsubscribe(true).await.is_err());
    assert!(hub.feeds().is_empty());
}

#[tokio::test]
async fn state_topic_failure_or_termination_invalidates_only_that_topic_after_cleanup() {
    for message_type in ["SubscriptionFailure", "SubscriptionTerminated"] {
        let (hub, _) = hub();
        let stream = hub
            .subscribe(state_request(
                &["SYNTH FAILED", "SYNTH SURVIVOR"],
                &["BID", "ASK"],
            ))
            .await
            .unwrap();
        let handle = stream.handle.clone();
        let state = state_consumer(stream);
        let (session, failed_key) = test_feed(&hub, "SYNTH FAILED");
        let (_, survivor_key) = test_feed(&hub, "SYNTH SURVIVOR");
        data(&session, failed_key, r#"{"BID":10,"ASK":11}"#);
        data(&session, survivor_key, r#"{"BID":20,"ASK":21}"#);
        let initial = state.next_delivery(2).await.unwrap().commit().unwrap();
        assert!(state_flag(&initial.data, "valid", 0));
        assert!(state_flag(&initial.data, "live", 0));
        session.status(failed_key, message_type);
        cleanup(&hub).await;
        assert_eq!(hub.feeds().len(), 1);
        assert!(
            handle.is_active(),
            "a topic-local failure closed the surviving topic"
        );
        let cancelled = state.next_delivery(2).await.unwrap();
        let invalid = cancelled.as_ref().unwrap();
        assert_eq!(invalid.data.num_rows(), 1);
        assert_eq!(state_label(&invalid.data, 0), "SYNTH FAILED");
        assert!(!state_flag(&invalid.data, "valid", 0));
        assert!(!state_flag(&invalid.data, "live", 0));
        assert!(invalid.data.column_by_name("BID").unwrap().is_null(0));
        assert!(invalid.data.column_by_name("ASK").unwrap().is_null(0));
        drop(cancelled);
        let invalid = state.next_delivery(2).await.unwrap().commit().unwrap();
        assert!(!state_flag(&invalid.data, "valid", 0));
        assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
        data(&session, survivor_key, r#"{"BID":22}"#);
        let survivor = state.next_delivery(2).await.unwrap().commit().unwrap();
        assert_eq!(survivor.data.num_rows(), 1);
        assert_eq!(state_label(&survivor.data, 0), "SYNTH SURVIVOR");
        assert!(state_flag(&survivor.data, "valid", 0));
        assert!(state_flag(&survivor.data, "live", 0));
        assert_eq!(state_number(&survivor.data, "BID", 0), 22.0);
        assert_eq!(state_number(&survivor.data, "ASK", 0), 21.0);
        handle
            .add(vec!["SYNTH FAILED".into()], vec![])
            .await
            .unwrap();
        let (session, failed_key) = test_feed(&hub, "SYNTH FAILED");
        data(&session, failed_key, r#"{"BID":30,"ASK":31}"#);
        session.status(failed_key, message_type);
        cleanup(&hub).await;
        let (_operation, drained) = state.unsubscribe_deliveries(true, None).await;
        let drained = drained.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(state_label(&drained[0].data, 0), "SYNTH FAILED");
        assert!(!state_flag(&drained[0].data, "valid", 0));
    }
}

#[tokio::test]
async fn state_stream_availability_wakes_only_the_changed_topic_without_changing_polling() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH AVAIL A", "SYNTH AVAIL B"], &["BID"]))
        .await
        .unwrap();
    let state = Arc::new(state_consumer(stream));
    let polling = hub
        .subscribe(image_request(&["SYNTH AVAIL A", "SYNTH AVAIL B"], &["BID"]))
        .await
        .unwrap();
    let (session, first_key) = test_feed(&hub, "SYNTH AVAIL A");
    let (_, second_key) = test_feed(&hub, "SYNTH AVAIL B");
    session.status(first_key, "SubscriptionStreamsActivated");
    data(&session, first_key, r#"{"BID":1}"#);
    data(&session, second_key, r#"{"BID":2}"#);
    let initial = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert!(state_flag(&initial.data, "live", 0));
    assert!(state_flag(&initial.data, "live", 1));
    let reader = state.clone();
    let waiting = tokio::spawn(async move { reader.next_delivery(2).await });
    tokio::task::yield_now().await;
    session.status(first_key, "SubscriptionStreamsDeactivated");
    let inactive = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(inactive.data.num_rows(), 1);
    assert_eq!(state_label(&inactive.data, 0), "SYNTH AVAIL A");
    assert!(state_flag(&inactive.data, "valid", 0));
    assert!(!state_flag(&inactive.data, "live", 0));
    assert_eq!(state_number(&inactive.data, "BID", 0), 1.0);
    assert!(state_flag(&polling.latest().unwrap(), "live", 0));
    data(&session, first_key, r#"{"BID":3}"#);
    let still_inactive = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert!(!state_flag(&still_inactive.data, "live", 0));
    assert_eq!(state_number(&still_inactive.data, "BID", 0), 3.0);
    session.status(first_key, "SubscriptionStreamsActivated");
    let active = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(active.data.num_rows(), 1);
    assert_eq!(state_label(&active.data, 0), "SYNTH AVAIL A");
    assert!(state_flag(&active.data, "valid", 0));
    assert!(state_flag(&active.data, "live", 0));
    assert_eq!(state_number(&active.data, "BID", 0), 3.0);
}

#[tokio::test]
async fn state_readd_replaces_cancelled_retired_generation_before_it_can_reappear() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH READD", "SYNTH KEEP"], &["BID"]))
        .await
        .unwrap();
    let handle = stream.handle.clone();
    let state = state_consumer(stream);
    let (session, failed_key) = test_feed(&hub, "SYNTH READD");
    let (_, kept_key) = test_feed(&hub, "SYNTH KEEP");
    data(&session, failed_key, r#"{"BID":1}"#);
    data(&session, kept_key, r#"{"BID":2}"#);
    state.next_delivery(2).await.unwrap().commit().unwrap();
    session.status(failed_key, "SubscriptionFailure");
    cleanup(&hub).await;
    let invalidation = state.next_delivery(1).await.unwrap();
    assert!(!state_flag(
        &invalidation.as_ref().unwrap().data,
        "valid",
        0
    ));
    handle
        .add(vec!["SYNTH READD".into()], vec![])
        .await
        .unwrap();
    let (session, new_key) = test_feed(&hub, "SYNTH READD");
    data(&session, new_key, r#"{"BID":9}"#);
    drop(invalidation);
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 1);
    let current = state.next_delivery(2).await.unwrap().commit().unwrap();
    assert_eq!(current.data.num_rows(), 1);
    assert_eq!(state_label(&current.data, 0), "SYNTH READD");
    assert!(state_flag(&current.data, "valid", 0));
    assert_eq!(state_number(&current.data, "BID", 0), 9.0);
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn state_loss_and_recovery_between_selection_and_snapshot_emits_invalid_then_quiet_current() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH SNAPSHOT RACE"], &["BID", "ASK"]))
        .await
        .unwrap();
    let handle = stream.handle.clone();
    let state = Arc::new(state_consumer(stream));
    let (session, key) = test_feed(&hub, "SYNTH SNAPSHOT RACE");
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    state.next_delivery(1).await.unwrap().commit().unwrap();
    data(&session, key, r#"{"BID":12}"#);

    let selected = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let selected_reader = selected.clone();
    let resume_reader = resume.clone();
    handle
        .inner
        .state_delivery
        .as_ref()
        .unwrap()
        .before_next_snapshot(Arc::new(move || {
            selected_reader.wait();
            resume_reader.wait();
        }));
    let reader = state.clone();
    let runtime = tokio::runtime::Handle::current();
    let waiting = tokio::task::spawn_blocking(move || runtime.block_on(reader.next_delivery(1)));
    selected.wait();
    let resume = ResumeSnapshot(resume);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    assert_eq!(session.resubscribes.load(Ordering::Relaxed), 1);
    data(
        &session,
        key,
        r#"{"BID":20,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    drop(resume);

    let invalid = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(!state_flag(&invalid.data, "valid", 0));
    assert!(invalid.data.column_by_name("BID").unwrap().is_null(0));
    assert!(invalid.data.column_by_name("ASK").unwrap().is_null(0));
    let recovered = tokio::time::timeout(Duration::from_secs(1), state.next_delivery(1))
        .await
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(state_flag(&recovered.data, "valid", 0));
    assert_eq!(state_number(&recovered.data, "BID", 0), 20.0);
    assert!(recovered.data.column_by_name("ASK").unwrap().is_null(0));
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn moved_state_terminal_error_is_restorable_until_publication_acknowledgement() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH ERROR RECEIPT"], &["BID"]))
        .await
        .unwrap();
    let (mut receiver, _handle) = stream.into_parts();
    receiver.recv_state(1).await.unwrap().commit().unwrap();
    let (session, _) = test_feed(&hub, "SYNTH ERROR RECEIPT");
    session.session_event("SessionTerminated");
    let mut cancelled = receiver.recv_state(1).await.unwrap();
    let error = cancelled.take_error().expect("source terminal error");
    cancelled.restore_error(error);
    drop(cancelled);
    let mut published = receiver.recv_state(1).await.unwrap();
    assert!(published.take_success().is_none());
    let _error = published
        .take_error()
        .expect("restored source terminal error");
    published.acknowledge();
    assert!(receiver.recv_state(1).await.is_none());
}

#[tokio::test]
async fn moved_state_drain_rollback_rebuilds_frozen_final_images_in_fifo_order() {
    let (hub, _) = hub();
    let _poller = hub
        .subscribe(image_request(
            &["SYNTH RETRY A", "SYNTH RETRY B"],
            &["BID", "ASK"],
        ))
        .await
        .unwrap();
    let mut req = state_request(&["SYNTH RETRY A", "SYNTH RETRY B"], &["BID", "ASK"]);
    req.flush_threshold = Some(1);
    let state = state_consumer(hub.subscribe(req).await.unwrap());
    let (session, first_key) = test_feed(&hub, "SYNTH RETRY A");
    let (_, second_key) = test_feed(&hub, "SYNTH RETRY B");
    data(&session, first_key, r#"{"BID":1,"ASK":2}"#);
    data(&session, second_key, r#"{"BID":3,"ASK":4}"#);
    state.next_delivery(2).await.unwrap().commit().unwrap();
    data(&session, first_key, r#"{"BID":5,"ASK":null}"#);
    data(&session, second_key, r#"{"BID":6,"ASK":7}"#);

    let drain = state.prepare_unsubscribe_deliveries(true, None).await;
    assert_eq!(drain.as_ref().unwrap().len(), 2);
    let (outputs, acknowledgement) = match drain.prepare_success() {
        Ok(prepared) => prepared,
        Err(_) => panic!("state drain unexpectedly failed"),
    };
    assert_eq!(state_label(&outputs[0].data, 0), "SYNTH RETRY A");
    assert_eq!(state_label(&outputs[1].data, 0), "SYNTH RETRY B");
    drop(outputs);
    data(&session, first_key, r#"{"BID":99,"ASK":100}"#);
    data(&session, second_key, r#"{"BID":101,"ASK":102}"#);
    drop(acknowledgement);

    let retried = state
        .prepare_unsubscribe_deliveries(true, None)
        .await
        .commit()
        .unwrap();
    assert_eq!(retried.len(), 2);
    assert_eq!(state_label(&retried[0].data, 0), "SYNTH RETRY A");
    assert_eq!(state_label(&retried[1].data, 0), "SYNTH RETRY B");
    assert_eq!(state_number(&retried[0].data, "BID", 0), 5.0);
    assert!(retried[0].data.column_by_name("ASK").unwrap().is_null(0));
    assert_eq!(state_number(&retried[1].data, "BID", 0), 6.0);
    assert_eq!(state_number(&retried[1].data, "ASK", 0), 7.0);
    assert!(
        state
            .prepare_unsubscribe_deliveries(true, None)
            .await
            .commit()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn state_wide_snapshot_late_join_and_layout_growth_retry_preserve_full_images() {
    let (hub, _) = hub();
    let fields: Vec<_> = (0..12).map(|index| format!("F{index}")).collect();
    let field_names: Vec<_> = fields.iter().map(String::as_str).collect();
    let _poller = hub
        .subscribe(image_request(&["SYNTH WIDE"], &field_names))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "SYNTH WIDE");
    let payload: serde_json::Map<String, serde_json::Value> = (0..12)
        .map(|index| (format!("F{index}"), serde_json::json!(index)))
        .collect();
    data(
        &session,
        key,
        &serde_json::Value::Object(payload).to_string(),
    );
    let mut request = state_request(&["SYNTH WIDE"], &["F0"]);
    request.all_fields = true;
    let stream = hub.subscribe(request).await.unwrap();
    let handle = stream.handle.clone();
    let state = Arc::new(state_consumer(stream));
    let joined = state.next_delivery(1).await.unwrap().commit().unwrap();
    for index in 0..12 {
        assert_eq!(
            state_number(&joined.data, &format!("F{index}"), 0),
            index as f64
        );
    }
    data(&session, key, r#"{"F0":100}"#);
    let prepared = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let prepared_reader = prepared.clone();
    let resume_reader = resume.clone();
    handle
        .inner
        .state_delivery
        .as_ref()
        .unwrap()
        .before_next_snapshot(Arc::new(move || {
            prepared_reader.wait();
            resume_reader.wait();
        }));
    let reader = state.clone();
    let runtime = tokio::runtime::Handle::current();
    let waiting = tokio::task::spawn_blocking(move || runtime.block_on(reader.next_delivery(1)));
    prepared.wait();
    let resume = ResumeSnapshot(resume);
    data(&session, key, r#"{"F3":null,"F12":"grown"}"#);
    drop(resume);
    let grown = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(state_number(&grown.data, "F0", 0), 100.0);
    assert_eq!(state_number(&grown.data, "F11", 0), 11.0);
    assert!(grown.data.column_by_name("F3").unwrap().is_null(0));
    assert_eq!(
        grown
            .data
            .column_by_name("F12")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "grown"
    );
    assert_eq!(state.rx.lock().await.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn state_reserved_metadata_fields_reject_configuration_before_mutation() {
    for field in ["topic", "last_update", "live", "delayed", "valid"] {
        let (hub, factory) = hub();
        assert!(matches!(
            hub.subscribe(state_request(&["SYNTH RESERVED"], &[field]))
                .await,
            Err(BlpAsyncError::ConfigError { .. })
        ));
        assert!(hub.feeds().is_empty());
        assert!(factory.sessions.lock().is_empty());
        assert!(hub.consumers.lock().is_empty());
    }
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH RESERVED ADD"], &["BID"]))
        .await
        .unwrap();
    assert!(matches!(
        stream.add_fields(vec!["valid".into()]).await,
        Err(BlpAsyncError::ConfigError { .. })
    ));
    assert_eq!(stream.fields(), vec!["BID"]);
}

#[tokio::test]
async fn state_sdk_reserved_valid_field_fails_without_shadowing_continuity_or_polling_data() {
    let (hub, _) = hub();
    let poller = hub
        .subscribe(image_request(&["SYNTH SDK RESERVED"], &["BID", "valid"]))
        .await
        .unwrap();
    let mut req = state_request(&["SYNTH SDK RESERVED"], &["BID"]);
    req.all_fields = true;
    let state = state_consumer(hub.subscribe(req).await.unwrap());
    let filtered = state_consumer(
        hub.subscribe(state_request(&["SYNTH SDK RESERVED"], &["BID"]))
            .await
            .unwrap(),
    );
    let (session, key) = test_feed(&hub, "SYNTH SDK RESERVED");
    data(&session, key, r#"{"BID":7}"#);
    let warmed = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&warmed.data, "valid", 0));
    data(&session, key, r#"{"BID":8,"valid":false}"#);
    let delivery = state.next_delivery(1).await.unwrap();
    assert!(matches!(
        delivery.as_ref(),
        Err(BlpError::InvalidArgument { .. })
    ));
    assert!(delivery.commit().is_err());
    let filtered = filtered.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(
        state_flag(&filtered.data, "valid", 0),
        "SDK data overwrote continuity"
    );
    assert_eq!(state_number(&filtered.data, "BID", 0), 8.0);
    let latest = poller.latest().unwrap();
    assert!(
        !state_flag(&latest, "valid", 0),
        "polling lost its legitimate SDK field"
    );
    assert_eq!(state_number(&latest, "BID", 0), 8.0);
}

#[tokio::test]
async fn frozen_state_drain_stages_invalidation_and_recovery_before_ack_and_restores_both() {
    for limit in [1, 16] {
        for rollback in [false, true] {
            let (hub, _) = hub();
            let poller = hub
                .subscribe(image_request(&["SYNTH DRAIN EPOCHS"], &["BID", "ASK"]))
                .await
                .unwrap();
            let mut req = state_request(&["SYNTH DRAIN EPOCHS"], &["BID", "ASK"]);
            req.flush_threshold = Some(limit);
            let state = state_consumer(hub.subscribe(req).await.unwrap());
            let (session, key) = test_feed(&hub, "SYNTH DRAIN EPOCHS");
            data(&session, key, r#"{"BID":10,"ASK":11}"#);
            state.next_delivery(limit).await.unwrap().commit().unwrap();
            data(
                &session,
                key,
                r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
            );
            cleanup(&hub).await;
            data(
                &session,
                key,
                r#"{"BID":20,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
            );
            let drain = state.prepare_unsubscribe_deliveries(true, None).await;
            let staged = drain.as_ref().unwrap();
            assert_eq!(staged.len(), 2);
            assert!(!state_flag(&staged[0].data, "valid", 0));
            assert!(staged[0].data.column_by_name("BID").unwrap().is_null(0));
            assert!(state_flag(&staged[1].data, "valid", 0));
            assert_eq!(state_number(&staged[1].data, "BID", 0), 20.0);
            let drained = if rollback {
                let (outputs, acknowledgement) = match drain.prepare_success() {
                    Ok(prepared) => prepared,
                    Err(_) => panic!("state epochs unexpectedly failed"),
                };
                drop(outputs);
                data(&session, key, r#"{"BID":99,"ASK":100}"#);
                drop(acknowledgement);
                assert_eq!(state_number(&poller.latest().unwrap(), "BID", 0), 99.0);
                state
                    .prepare_unsubscribe_deliveries(true, None)
                    .await
                    .commit()
                    .unwrap()
            } else {
                drain.commit().unwrap()
            };
            assert_eq!(drained.len(), 2);
            assert!(!state_flag(&drained[0].data, "valid", 0));
            assert!(state_flag(&drained[1].data, "valid", 0));
            assert_eq!(state_number(&drained[1].data, "BID", 0), 20.0);
            assert!(drained[1].data.column_by_name("ASK").unwrap().is_null(0));
            assert!(
                state
                    .prepare_unsubscribe_deliveries(true, None)
                    .await
                    .commit()
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
async fn state_base_schema_reuses_clean_fields_and_rebuilds_after_kind_growth() {
    let (hub, _) = hub();
    let state = state_consumer(
        hub.subscribe(state_request(&["SYNTH SCHEMA CACHE"], &["BID"]))
            .await
            .unwrap(),
    );
    let (session, key) = test_feed(&hub, "SYNTH SCHEMA CACHE");
    data(&session, key, r#"{"BID":1}"#);
    let first = state.next_delivery(1).await.unwrap().commit().unwrap();
    let base = first.data.schema();
    let mut diagnostics = super::super::state::ResponseMetadata::default();
    diagnostics.record_type_miss("BID");
    let decorated = diagnostics.attach(first.data);
    let metadata_key = super::super::state::METADATA_KEY_TYPE_MISSES;
    assert!(decorated.schema().metadata().contains_key(metadata_key));
    data(&session, key, r#"{"BID":2}"#);
    let second = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(Arc::ptr_eq(&base, &second.data.schema()));
    assert!(!second.data.schema().metadata().contains_key(metadata_key));
    data(&session, key, r#"{"BID":"changed"}"#);
    let changed = state.next_delivery(1).await.unwrap().commit().unwrap();
    let changed_schema = changed.data.schema();
    assert!(!Arc::ptr_eq(&base, &changed_schema));
    assert_eq!(
        changed_schema.field_with_name("BID").unwrap().data_type(),
        &arrow_schema::DataType::Utf8
    );
    data(&session, key, r#"{"BID":"again"}"#);
    let stable = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(Arc::ptr_eq(&changed_schema, &stable.data.schema()));
    assert!(!stable.data.schema().metadata().contains_key(metadata_key));
}

#[tokio::test]
async fn cached_image_schema_keeps_plain_and_marked_headers_separate() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH HEADER CACHE"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "SYNTH HEADER CACHE");
    data(&session, key, r#"{"BID":3}"#);
    let feed = feed_for_label(&stream, "SYNTH HEADER CACHE");
    let member = stream.handle.inner.memberships.lock()[0].key;
    let (_, row) = stream
        .handle
        .inner
        .snapshot_image(&feed, member, true)
        .unwrap();
    let rows = std::slice::from_ref(&row);
    let mut schema = ImageSchema::new(stream.handle.inner.image_fields());
    schema.observe_rows(rows.iter());
    let plain = materialize_images(rows.iter(), &mut schema, &HashSet::new(), false).unwrap();
    let marked = materialize_images(rows.iter(), &mut schema, &HashSet::new(), true).unwrap();
    let plain_again = materialize_images(rows.iter(), &mut schema, &HashSet::new(), false).unwrap();
    assert_eq!(plain.num_columns(), 5);
    assert_eq!(marked.num_columns(), 6);
    assert!(plain.column_by_name("valid").is_none());
    assert!(state_flag(&marked, "valid", 0));
    assert_eq!(state_number(&plain, "BID", 0), 3.0);
    assert_eq!(state_number(&marked, "BID", 0), 3.0);
    assert!(Arc::ptr_eq(&plain.schema(), &plain_again.schema()));
    assert!(!Arc::ptr_eq(&plain.schema(), &marked.schema()));
}

#[tokio::test]
async fn state_freeze_retains_loss_and_recovery_metadata_while_snapshot_is_preparing() {
    for rollback in [false, true] {
        let (hub, _) = hub();
        let _poller = hub
            .subscribe(image_request(&["SYNTH FREEZE LOSS"], &["BID", "ASK"]))
            .await
            .unwrap();
        let stream = hub
            .subscribe(state_request(&["SYNTH FREEZE LOSS"], &["BID", "ASK"]))
            .await
            .unwrap();
        let shared = stream.handle.inner.state_delivery.as_ref().unwrap().clone();
        let state = Arc::new(state_consumer(stream));
        let (session, key) = test_feed(&hub, "SYNTH FREEZE LOSS");
        data(&session, key, r#"{"BID":10,"ASK":11}"#);
        state.next_delivery(1).await.unwrap().commit().unwrap();
        data(&session, key, r#"{"BID":12}"#);
        let prepared = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let prepared_reader = prepared.clone();
        let resume_reader = resume.clone();
        shared.before_next_snapshot(Arc::new(move || {
            prepared_reader.wait();
            resume_reader.wait();
        }));
        let reader = state.clone();
        let runtime = tokio::runtime::Handle::current();
        let waiting = tokio::task::spawn_blocking(move || {
            runtime.block_on(reader.prepare_unsubscribe_deliveries(true, None))
        });
        prepared.wait();
        let resume = ResumeSnapshot(resume);
        data(
            &session,
            key,
            r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
        );
        cleanup(&hub).await;
        data(
            &session,
            key,
            r#"{"BID":20,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
        );
        drop(resume);
        let drain = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_state_recovery_epochs(drain.as_ref().unwrap(), 20.0);
        let delivered = if rollback {
            let (outputs, acknowledgement) = match drain.prepare_success() {
                Ok(prepared) => prepared,
                Err(_) => panic!("state freeze unexpectedly failed"),
            };
            drop(outputs);
            data(&session, key, r#"{"BID":99}"#);
            drop(acknowledgement);
            state
                .prepare_unsubscribe_deliveries(true, None)
                .await
                .commit()
                .unwrap()
        } else {
            drain.commit().unwrap()
        };
        assert_state_recovery_epochs(&delivered, 20.0);
    }
}

#[tokio::test]
async fn state_freeze_retains_topic_retirement_while_snapshot_is_preparing() {
    for message_type in ["SubscriptionFailure", "SubscriptionTerminated"] {
        let (hub, _) = hub();
        let stream = hub
            .subscribe(state_request(
                &["SYNTH FREEZE FAIL", "SYNTH FREEZE KEEP"],
                &["BID"],
            ))
            .await
            .unwrap();
        let shared = stream.handle.inner.state_delivery.as_ref().unwrap().clone();
        let state = Arc::new(state_consumer(stream));
        let (session, key) = test_feed(&hub, "SYNTH FREEZE FAIL");
        let (_, kept) = test_feed(&hub, "SYNTH FREEZE KEEP");
        data(&session, key, r#"{"BID":1}"#);
        data(&session, kept, r#"{"BID":2}"#);
        state.next_delivery(2).await.unwrap().commit().unwrap();
        data(&session, key, r#"{"BID":3}"#);
        let prepared = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let prepared_reader = prepared.clone();
        let resume_reader = resume.clone();
        shared.before_next_snapshot(Arc::new(move || {
            prepared_reader.wait();
            resume_reader.wait();
        }));
        let reader = state.clone();
        let runtime = tokio::runtime::Handle::current();
        let waiting = tokio::task::spawn_blocking(move || {
            runtime.block_on(reader.prepare_unsubscribe_deliveries(true, None))
        });
        prepared.wait();
        let resume = ResumeSnapshot(resume);
        session.status(key, message_type);
        cleanup(&hub).await;
        drop(resume);
        let drain = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        let (outputs, acknowledgement) = match drain.prepare_success() {
            Ok(prepared) => prepared,
            Err(_) => panic!("topic-local retirement closed the whole state subscription"),
        };
        assert_eq!(outputs.len(), 1);
        assert_eq!(state_label(&outputs[0].data, 0), "SYNTH FREEZE FAIL");
        assert!(!state_flag(&outputs[0].data, "valid", 0));
        assert!(outputs[0].data.column_by_name("BID").unwrap().is_null(0));
        drop(outputs);
        drop(acknowledgement);
        let retried = state
            .prepare_unsubscribe_deliveries(true, None)
            .await
            .commit()
            .unwrap();
        assert_eq!(retried.len(), 1);
        assert_eq!(state_label(&retried[0].data, 0), "SYNTH FREEZE FAIL");
        assert!(!state_flag(&retried[0].data, "valid", 0));
    }
}

#[tokio::test]
async fn state_freeze_preserves_first_loss_before_capture_and_excludes_later_recovery() {
    let (hub, _) = hub();
    let poller = hub
        .subscribe(image_request(&["SYNTH FREEZE CUTOFF"], &["BID", "ASK"]))
        .await
        .unwrap();
    let stream = hub
        .subscribe(state_request(&["SYNTH FREEZE CUTOFF"], &["BID", "ASK"]))
        .await
        .unwrap();
    let shared = stream.handle.inner.state_delivery.as_ref().unwrap().clone();
    let state = Arc::new(state_consumer(stream));
    let (session, key) = test_feed(&hub, "SYNTH FREEZE CUTOFF");
    data(&session, key, r#"{"BID":10,"ASK":11}"#);
    state.next_delivery(1).await.unwrap().commit().unwrap();
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    data(
        &session,
        key,
        r#"{"BID":20,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    let captured = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let captured_reader = captured.clone();
    let resume_reader = resume.clone();
    shared.after_next_snapshot(Arc::new(move || {
        captured_reader.wait();
        resume_reader.wait();
    }));
    let reader = state.clone();
    let runtime = tokio::runtime::Handle::current();
    let waiting = tokio::task::spawn_blocking(move || {
        runtime.block_on(reader.prepare_unsubscribe_deliveries(true, None))
    });
    captured.wait();
    let resume = ResumeSnapshot(resume);
    data(
        &session,
        key,
        r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
    );
    cleanup(&hub).await;
    data(
        &session,
        key,
        r#"{"BID":99,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
    );
    assert_eq!(state_number(&poller.latest().unwrap(), "BID", 0), 99.0);
    drop(resume);
    let drain = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_state_recovery_epochs(drain.as_ref().unwrap(), 20.0);
    let (outputs, acknowledgement) = match drain.prepare_success() {
        Ok(prepared) => prepared,
        Err(_) => panic!("captured state epochs unexpectedly failed"),
    };
    drop(outputs);
    drop(acknowledgement);
    let retried = state
        .prepare_unsubscribe_deliveries(true, None)
        .await
        .commit()
        .unwrap();
    assert_state_recovery_epochs(&retried, 20.0);
}

#[tokio::test]
async fn state_wide_scratch_pool_tracks_actual_live_batch_peak_not_topics_or_freeze_size() {
    let (hub, _) = hub();
    let topics = ["SYNTH POOL A", "SYNTH POOL B", "SYNTH POOL C"];
    let fields: Vec<_> = (0..12).map(|index| format!("F{index}")).collect();
    let field_names: Vec<_> = fields.iter().map(String::as_str).collect();
    let stream = hub
        .subscribe(state_request(&topics, &field_names))
        .await
        .unwrap();
    let shared = stream.handle.inner.state_delivery.as_ref().unwrap().clone();
    let state = state_consumer(stream);
    for _ in &topics {
        state.next_delivery(1).await.unwrap().commit().unwrap();
    }
    assert_eq!(shared.spare_buffer_stats(), (1, 0));
    for (topic_index, topic) in topics.iter().enumerate() {
        let (session, key) = test_feed(&hub, topic);
        let payload: serde_json::Map<String, serde_json::Value> = (0..12)
            .map(|index| {
                (
                    format!("F{index}"),
                    serde_json::json!(topic_index * 100 + index),
                )
            })
            .collect();
        data(
            &session,
            key,
            &serde_json::Value::Object(payload).to_string(),
        );
        let delivery = state.next_delivery(4096).await.unwrap().commit().unwrap();
        assert_eq!(delivery.data.num_rows(), 1);
        assert_eq!(
            state_number(&delivery.data, "F11", 0),
            (topic_index * 100 + 11) as f64
        );
        assert_eq!(shared.spare_buffer_stats(), (1, 1));
    }
    for topic in &topics {
        let (session, key) = test_feed(&hub, topic);
        data(&session, key, r#"{"F0":1000}"#);
    }
    let delivery = state.next_delivery(4096).await.unwrap().commit().unwrap();
    assert_eq!(delivery.data.num_rows(), topics.len());
    assert_eq!(shared.spare_buffer_stats(), (3, 3));
    let mut request = state_request(&topics, &field_names);
    request.flush_threshold = Some(4096);
    let freeze = hub.subscribe(request).await.unwrap();
    let frozen_shared = freeze.handle.inner.state_delivery.as_ref().unwrap().clone();
    let freeze = state_consumer(freeze);
    let drain = freeze.prepare_unsubscribe_deliveries(true, None).await;
    assert_eq!(drain.as_ref().unwrap()[0].data.num_rows(), topics.len());
    assert_eq!(frozen_shared.spare_buffer_stats(), (0, 0));
    drain.commit().unwrap();
}

#[tokio::test]
async fn first_sdk_deactivation_without_prior_activation_marks_state_not_polling() {
    let (hub, _) = hub();
    let stream = hub
        .subscribe(state_request(&["SYNTH FIRST INACTIVE"], &["BID"]))
        .await
        .unwrap();
    let handle = stream.handle.clone();
    let state = state_consumer(stream);
    let polling = hub
        .subscribe(image_request(&["SYNTH FIRST INACTIVE"], &["BID"]))
        .await
        .unwrap();
    let (session, key) = test_feed(&hub, "SYNTH FIRST INACTIVE");
    data(&session, key, r#"{"BID":1}"#);
    let active = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&active.data, "valid", 0));
    assert!(state_flag(&active.data, "live", 0));
    assert!(!handle.status().load().topic_statuses()["SYNTH FIRST INACTIVE"].streams_active);
    session.status(key, "SubscriptionStreamsDeactivated");
    let inactive = tokio::time::timeout(Duration::from_secs(1), state.next_delivery(1))
        .await
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(state_flag(&inactive.data, "valid", 0));
    assert!(!state_flag(&inactive.data, "live", 0));
    assert_eq!(state_number(&inactive.data, "BID", 0), 1.0);
    assert!(state_flag(&polling.latest().unwrap(), "live", 0));
    let late = state_consumer(
        hub.subscribe(state_request(&["SYNTH FIRST INACTIVE"], &["BID"]))
            .await
            .unwrap(),
    );
    let late_inactive = late.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&late_inactive.data, "valid", 0));
    assert!(!state_flag(&late_inactive.data, "live", 0));
    session.status(key, "SubscriptionStreamsActivated");
    let resumed = state.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&resumed.data, "live", 0));
    let late_resumed = late.next_delivery(1).await.unwrap().commit().unwrap();
    assert!(state_flag(&late_resumed.data, "live", 0));
}
