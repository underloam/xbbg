//! Live shared-subscription lifecycle checks against the configured Bloomberg endpoint.
//!
//! These tests require a working connection and are ignored by default.
//! They do not silently skip connection or entitlement failures.
//! Run: cargo test -p xbbg-async --test shared_subscriptions_live -- --ignored

use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use tokio::time::{Instant, sleep, timeout};
use xbbg_async::engine::state::{SubscriptionUpdate, UpdateValue};
use xbbg_async::engine::{Engine, EngineConfig, ServerAddr, SubscriptionStream, Transport};
use xbbg_async::{BlpAsyncError, FieldErrorPolicy, SubscribeRequest};

const TOPIC: &str = "IBM US Equity";
const WAIT: Duration = Duration::from_secs(15);

fn create_engine() -> Engine {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("BLP_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(8194);
    let auth = match std::env::var("XBBG_TEST_AUTH").ok().as_deref() {
        Some("user") => Some(xbbg_core::AuthConfig::User),
        Some(_) => panic!("unsupported live-test authentication configuration"),
        None => None,
    };
    Engine::start(EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
        auth,
        request_pool_size: 1,
        subscription_pool_size: 0,
        max_subscription_sessions: 2,
        subscription_stream_capacity: 4096,
        ..EngineConfig::default()
    })
    .expect("live test needs a Bloomberg connection")
}

fn request(fields: &[&str]) -> SubscribeRequest {
    SubscribeRequest {
        topics: vec![TOPIC.into()],
        fields: fields.iter().map(|field| (*field).into()).collect(),
        ..SubscribeRequest::default()
    }
}

fn known(batch: &RecordBatch, field: &str) -> bool {
    batch.num_rows() == 1
        && batch
            .column_by_name(field)
            .is_some_and(|column| column.is_valid(0))
}

fn observed_quote(batch: &RecordBatch, field: &str) -> bool {
    batch.num_rows() == 1
        && batch
            .schema()
            .field_with_name(field)
            .is_ok_and(|field| field.data_type() == &DataType::Float64)
}

fn text_field<'a>(update: &'a SubscriptionUpdate, field: &str) -> Option<&'a str> {
    update.values.iter().find_map(|entry| {
        if update.layout.fields[entry.index as usize].name.as_ref() != field {
            return None;
        }
        match &entry.value {
            UpdateValue::Str(value) => Some(value.as_ref()),
            _ => None,
        }
    })
}

async fn wait_until(
    stream: &mut SubscriptionStream,
    mut companion: Option<&mut SubscriptionStream>,
    mut ready: impl FnMut(&SubscriptionStream) -> bool,
    phase: &str,
) {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(other) = companion.as_deref_mut() {
            while let Some(item) = other.try_next() {
                assert!(
                    item.is_ok(),
                    "companion live stream failed while waiting for {phase}"
                );
            }
            assert!(
                other.is_active(),
                "companion live stream ended while waiting for {phase}"
            );
        }
        if ready(stream) {
            return;
        }
        assert!(
            stream.is_active(),
            "live subscription ended while waiting for {phase}"
        );
        assert!(Instant::now() < deadline, "timed out waiting for {phase}");
        tokio::select! {
            item = stream.next() => assert!(matches!(item, Some(Ok(_))), "live stream failed while waiting for {phase}"),
            _ = sleep(Duration::from_millis(20)) => {}
        }
    }
}

fn started_count(stream: &SubscriptionStream) -> usize {
    stream
        .status()
        .load()
        .events()
        .iter()
        .filter(|event| event.message_type == "SubscriptionStarted")
        .count()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn shared_feed_live_image_growth_warnings_isolation_and_detach() {
    let engine = create_engine();
    let mut first = engine
        .subscribe(request(&["LAST_PRICE"]))
        .await
        .expect("first live subscription");
    // Initial images and resubscribe paints, rather than new trades, make these
    // assertions meaningful outside market hours as well as during trading.
    wait_until(
        &mut first,
        None,
        |stream| {
            known(&stream.latest().expect("latest image"), "LAST_PRICE")
                && stream.status().load().topic_statuses()[TOPIC]
                    .delayed
                    .is_some()
        },
        "initial last-trade image and delayed flag",
    )
    .await;

    let mut second = engine
        .subscribe(request(&["LAST_PRICE", "BID"]))
        .await
        .expect("shared live subscription");
    let feeds = engine.subscription_feeds();
    assert_eq!(feeds.len(), 1);
    assert_eq!(feeds[0].consumers, 2);
    assert_eq!(feeds[0].fields, vec!["LAST_PRICE", "BID"]);
    let late = timeout(WAIT, second.next())
        .await
        .expect("late image timeout")
        .expect("late stream ended")
        .expect("late stream failed");
    assert_eq!(late.topic.as_ref(), TOPIC);
    assert_eq!(text_field(&late, "MKTDATA_EVENT_TYPE"), Some("SUMMARY"));
    assert_eq!(
        text_field(&late, "MKTDATA_EVENT_SUBTYPE"),
        Some("INITPAINT")
    );
    assert!(
        late.values.iter().any(
            |entry| late.layout.fields[entry.index as usize].name.as_ref() == "LAST_PRICE"
                && !matches!(entry.value, UpdateValue::Null)
        ),
        "late image omitted known last trade"
    );
    // Quotes may be explicitly cleared outside market hours. Their seeded
    // Float64 schema remains usable; SubscriptionStarted below confirms resubscription.
    wait_until(
        &mut second,
        Some(&mut first),
        |stream| observed_quote(&stream.latest().expect("latest BID image"), "BID"),
        "union BID paint",
    )
    .await;

    let starts = started_count(&second);
    second
        .add_fields(vec!["ASK".into()])
        .await
        .expect("grow live field union");
    wait_until(
        &mut second,
        Some(&mut first),
        |stream| {
            started_count(stream) > starts
                && observed_quote(&stream.latest().expect("latest expanded image"), "ASK")
        },
        "resubscribe status and ASK paint",
    )
    .await;
    let latest = second.latest().expect("materialized latest image");
    assert_eq!(
        latest
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "topic",
            "last_update",
            "live",
            "delayed",
            "LAST_PRICE",
            "BID",
            "ASK"
        ]
    );
    assert_eq!(latest.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        latest.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(latest.schema().field(2).data_type(), &DataType::Boolean);
    assert_eq!(latest.schema().field(3).data_type(), &DataType::Boolean);
    assert!(known(&latest, "last_update"));
    assert!(
        second.status().load().topic_statuses()[TOPIC]
            .delayed
            .is_some()
    );

    second
        .add_fields(vec!["PX_BID".into()])
        .await
        .expect("request rejected stream field");
    let mut field_warning = false;
    wait_until(
        &mut second,
        Some(&mut first),
        |stream| {
            field_warning |= stream.take_warnings().iter().any(|event| {
                event.message_type == "FieldException" && event.topic.as_deref() == Some(TOPIC)
            });
            field_warning
                && stream
                    .status()
                    .load()
                    .field_errors()
                    .get(TOPIC)
                    .is_some_and(|errors| errors.contains_key("PX_BID"))
        },
        "PX_BID field exception and warning",
    )
    .await;

    let mut isolated_request = request(&["BID"]);
    isolated_request.isolated = true;
    let pending_isolated = engine.subscribe(isolated_request);
    tokio::pin!(pending_isolated);
    let isolated = timeout(WAIT, async {
        loop {
            tokio::select! {
                result = &mut pending_isolated => break result.expect("isolated live subscription"),
                item = first.next() => assert!(matches!(item, Some(Ok(_))), "first live stream failed during isolated startup"),
                item = second.next() => assert!(matches!(item, Some(Ok(_))), "second live stream failed during isolated startup"),
            }
        }
    }).await.expect("isolated subscription startup timeout");
    let feeds = engine.subscription_feeds();
    assert_eq!(feeds.len(), 2);
    assert_eq!(
        feeds
            .iter()
            .filter(|feed| feed.isolated && feed.consumers == 1)
            .count(),
        1
    );
    assert_eq!(
        feeds
            .iter()
            .filter(|feed| !feed.isolated && feed.consumers == 2)
            .count(),
        1
    );

    second
        .unsubscribe(false)
        .await
        .expect("detach second consumer");
    assert_eq!(
        engine
            .subscription_feeds()
            .iter()
            .find(|feed| !feed.isolated)
            .unwrap()
            .consumers,
        1
    );
    first
        .unsubscribe(false)
        .await
        .expect("detach final shared consumer");
    let feeds = engine.subscription_feeds();
    assert_eq!(feeds.len(), 1);
    assert!(feeds[0].isolated);
    isolated
        .unsubscribe(false)
        .await
        .expect("detach isolated consumer");
    assert!(engine.subscription_feeds().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn image_only_live_latest_field_rejection_and_zero_materialization() {
    let engine = create_engine();
    let mut plain_request = request(&["LAST_PRICE", "THEO_PRICE"]);
    plain_request.deliver_rows = false;
    plain_request.stream_capacity = Some(1);
    let mut plain = engine
        .subscribe(plain_request.clone())
        .await
        .expect("image-only subscription");
    assert!(!plain.delivers_rows());
    wait_until(
        &mut plain,
        None,
        |stream| known(&stream.latest().expect("image-only latest"), "LAST_PRICE"),
        "image-only last-trade image",
    )
    .await;
    assert!(
        plain.try_next().is_none(),
        "image-only subscription emitted a data row"
    );
    let mut masked_request = plain_request;
    masked_request.zero_as_null = vec!["THEO_PRICE".into()];
    masked_request.session_wait = Some(Duration::ZERO);
    let mut masked = engine
        .subscribe(masked_request)
        .await
        .expect("zero-wait shared image view");
    assert!(masked.try_next().is_none());
    let raw = plain.latest().expect("unmasked image");
    let masked_latest = masked.latest().expect("zero-filtered image");
    let same_image = raw.column_by_name("last_update").unwrap()
        == masked_latest.column_by_name("last_update").unwrap();
    if same_image
        && let Some(values) = raw
            .column_by_name("THEO_PRICE")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
        && values.is_valid(0)
        && values.value(0) == 0.0
    {
        assert!(
            masked_latest
                .column_by_name("THEO_PRICE")
                .unwrap()
                .is_null(0)
        );
    }

    let mut invalid = request(&["PX_BID"]);
    invalid.deliver_rows = false;
    invalid.field_error_policy = FieldErrorPolicy::Raise;
    let mut rejected = engine
        .subscribe(invalid)
        .await
        .expect("field-error policy subscription");
    assert!(matches!(
        timeout(WAIT, rejected.next())
            .await
            .expect("field rejection timeout"),
        Some(Err(xbbg_core::BlpError::SubscriptionFailure { .. }))
    ));
    assert!(
        rejected
            .status()
            .load()
            .field_errors()
            .get(TOPIC)
            .is_some_and(|fields| fields.contains_key("PX_BID"))
    );
    assert!(matches!(
        rejected.latest(),
        Err(BlpAsyncError::ChannelClosed)
    ));
    rejected
        .unsubscribe(false)
        .await
        .expect("close rejected subscription");
    masked.unsubscribe(false).await.expect("close image view");
    plain
        .unsubscribe(false)
        .await
        .expect("close final image-only subscription");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: needs a Bloomberg session; run with `cargo test -- --ignored`"]
async fn live_repaint_rows_resynchronize_existing_values_when_changes_are_observable() {
    let engine = create_engine();
    let mut rows = engine
        .subscribe(request(&["LAST_PRICE"]))
        .await
        .expect("row subscription");
    let mut last = timeout(WAIT, async {
        loop {
            let update = rows
                .next()
                .await
                .expect("row stream ended")
                .expect("row stream failed");
            if let Some(value) = update.values.iter().find(|entry| {
                update.layout.fields[entry.index as usize].name.as_ref() == "LAST_PRICE"
            }) {
                break value.value.clone();
            }
        }
    })
    .await
    .expect("last-trade image timeout");
    let prior_starts = started_count(&rows);
    let mut image_request = request(&["LAST_PRICE", "BID"]);
    image_request.deliver_rows = false;
    let pending = engine.subscribe(image_request);
    tokio::pin!(pending);
    let image = timeout(WAIT, async {
        loop {
            tokio::select! {
                result = &mut pending => break result.expect("field-union image view"),
                update = rows.next() => {
                    let update = update.expect("row stream ended").expect("row stream failed");
                    if let Some(value) = update.values.iter().find(|entry|
                        update.layout.fields[entry.index as usize].name.as_ref() == "LAST_PRICE")
                    { last = value.value.clone(); }
                }
            }
        }
    })
    .await
    .expect("shared image subscription timeout");
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        tokio::select! {
            update = rows.next() => {
                let update = update.expect("row stream ended").expect("row stream failed");
                let repaint_start = rows.status().load().events().iter()
                    .filter(|event| event.message_type == "SubscriptionStarted").nth(prior_starts)
                    .map(|event| event.at_us);
                if let Some(value) = update.values.iter().find(|entry|
                    update.layout.fields[entry.index as usize].name.as_ref() == "LAST_PRICE")
                {
                    if text_field(&update, "MKTDATA_EVENT_SUBTYPE") == Some("INITPAINT")
                        && repaint_start.is_some_and(|at_us| update.timestamp_us >= at_us)
                    {
                        assert!(last != value.value, "unchanged repaint value was emitted to an existing consumer");
                    }
                    last = value.value.clone();
                }
            }
            _ = sleep(Duration::from_millis(20)) => {}
        }
    }
    assert!(known(
        &image.latest().expect("shared image latest"),
        "LAST_PRICE"
    ));
    image.unsubscribe(false).await.expect("close image view");
    rows.unsubscribe(false)
        .await
        .expect("close row subscription");
}
