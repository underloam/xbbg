mod arrow_zero_copy;
mod config;
mod engine;
mod errors;
mod ext;
mod recipes;
mod request;
mod sdk;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod subscription_fixture;
mod subscriptions;

pub use config::{
    AuthConfigInput, EngineConfigInput, EntitlementReport, FeedInfoOutput, FieldInfoOutput,
    RequestInput, RetryPolicyInput, SecurityOverridesInput, ServerAddressInput,
    SessionConnectionsOutput, Socks5ConfigInput, StringPair, TlsConfigInput, TlsMaterialInput,
};
pub use engine::JsEngine;
pub(crate) use engine::to_ipc_buffer;
pub(crate) use errors::host_error_to_napi;
pub use ext::*;
pub use request::plan_request;
pub use sdk::*;
pub use subscriptions::{
    AdminStatusOutput, JsSubscription, JsSubscriptionPublication, NativeLegacyRead,
    NativeSubscriptionDeliveryDrain, NativeSubscriptionDeliveryOutput,
    NativeSubscriptionDeliveryRead, NativeSubscriptionLayout, NativeSubscriptionUpdateBatch,
    ServiceStatusOutput, SessionStatusOutput, SubscriptionEventOutput, SubscriptionFailureOutput,
    SubscriptionStats, SubscriptionStatusOutput, TopicStateOutput,
};

use napi::bindgen_prelude::{Error, Status, create_custom_tokio_runtime};
use napi_derive::napi;

#[napi_derive::module_init]
fn init_async_runtime() {
    xbbg_log::init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create NAPI async bridge runtime");
    create_custom_tokio_runtime(runtime);
}

#[napi]
pub fn set_log_level(level: String) -> napi::Result<()> {
    let lvl = xbbg_log::parse_level(&level).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            format!("Invalid log level '{level}'. Expected: trace, debug, info, warn, error"),
        )
    })?;
    xbbg_log::set_level(lvl);
    Ok(())
}

#[napi]
pub fn get_log_level() -> String {
    match xbbg_log::current_level() {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::prepare_subscribe_args;
    use crate::errors::{
        blp_async_error_to_napi, blp_error_to_napi, recipe_error_to_napi,
        subscription_control_error_to_napi,
    };
    use crate::subscriptions::TickValue;
    use std::future::Future;
    use std::sync::Arc;
    use xbbg_async::BlpAsyncError;
    use xbbg_async::engine::state::{
        FieldKind, FieldLayout, FieldMeta, SubscriptionReceiver, SubscriptionUpdate, UpdateValue,
        subscription_channel, subscription_channel_with_policy,
    };
    use xbbg_async::engine::{
        EngineConfig, OverflowPolicy, RequestParams, ServerAddr, SharedSubscriptionStatus,
        SubscriptionDeliveryMode, TlsMaterial, Transport,
    };
    use xbbg_async::services::ExtractorType;
    use xbbg_async::subscription_consumer::{PendingUpdates, SubscriptionConsumer};
    use xbbg_async::{DelayedPolicy, FieldErrorPolicy};
    use xbbg_core::{AuthConfig, BlpError};
    use xbbg_host::errors::{HostError, HostErrorKind};
    use xbbg_host::subscription::{HostSubscription, PreparedDelivery};

    fn minimal_input() -> EngineConfigInput {
        EngineConfigInput {
            host: None,
            port: None,
            servers: None,
            zfp_remote: None,
            request_pool_size: None,
            subscription_pool_size: None,
            runtime_worker_threads: None,
            max_subscription_sessions: None,
            shard_requests: None,
            shard_threshold: None,
            shard_chunk_size: None,
            shard_max_concurrent: None,
            validation_mode: None,
            key_column: None,
            subscription_flush_threshold: None,
            max_event_queue_size: None,
            command_queue_size: None,
            subscription_stream_capacity: None,
            overflow_policy: None,
            warmup_services: None,
            field_cache_path: None,
            bql_app_name: None,
            auth: None,
            tls: None,
            num_start_attempts: None,
            auto_restart_on_disconnection: None,
            retry_policy: None,
            request_timeout_ms: None,
            streams_deactivated_warn_ms: None,
            reconnect_grace_ms: None,
            keep_alive_enabled: None,
            keep_alive_inactivity_ms: None,
            keep_alive_response_timeout_ms: None,
            slow_consumer_hi_water_mark: None,
            slow_consumer_lo_water_mark: None,
            sdk_log_level: None,
            socks5: None,
        }
    }

    fn direct_servers(config: &EngineConfig) -> &[ServerAddr] {
        match &config.transport {
            Transport::Direct(s) => s.as_slice(),
            other => panic!("expected Direct, got {other}"),
        }
    }
    fn pair(key: &str, value: &str) -> StringPair {
        StringPair {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn subscription_update(layout_version: u32, timestamp_us: i64) -> SubscriptionUpdate {
        subscription_update_with_layout(
            Arc::new(FieldLayout::new(layout_version, Vec::new())),
            timestamp_us,
        )
    }

    fn subscription_update_with_layout(
        layout: Arc<FieldLayout>,
        timestamp_us: i64,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from("IBM US Equity"),
            layout,
            values: Default::default(),
        }
    }

    fn subscription_with_receiver(rx: SubscriptionReceiver, deliver_rows: bool) -> JsSubscription {
        let consumer = SubscriptionConsumer::from_receiver(rx);
        JsSubscription {
            host: Arc::new(HostSubscription::from_consumer(
                consumer,
                SharedSubscriptionStatus::default(),
                deliver_rows,
                1,
                None,
            )),
        }
    }

    /// Split a coded napi error into its `[XBBG:<CODE>` prefix, context JSON and message.
    fn decode_coded(error: &Error) -> (String, serde_json::Value, String) {
        let (code, rest) = error.reason.split_once("] ").expect("code prefix");
        match rest
            .strip_prefix("[XBBG_CONTEXT]")
            .and_then(|rest| rest.split_once('\n'))
        {
            Some((context, message)) => (
                code.to_string(),
                serde_json::from_str(context).expect("context json"),
                message.to_string(),
            ),
            None => (code.to_string(), serde_json::Value::Null, rest.to_string()),
        }
    }

    fn subscribe_error(args: &str) -> Error {
        host_error_to_napi(
            prepare_subscribe_args(args, 64).expect_err("subscribe arguments must be rejected"),
        )
    }

    #[test]
    fn subscribe_arguments_default_to_a_market_data_request() {
        let prepared = prepare_subscribe_args(
            r#"{"tickers":["IBM US Equity"],"fields":["LAST_PRICE"]}"#,
            64,
        )
        .expect("defaults");
        assert_eq!(prepared.batch_items, 64);
        assert_eq!(prepared.request.service, "//blp/mktdata");
        assert_eq!(prepared.request.topics, ["IBM US Equity"]);
        assert_eq!(prepared.request.fields, ["LAST_PRICE"]);
        assert!(prepared.request.options.is_empty());
        assert!(prepared.request.deliver_rows);
        assert!(!prepared.request.isolated);
        assert_eq!(prepared.request.delayed_policy, DelayedPolicy::Warn);
        assert_eq!(prepared.request.field_error_policy, FieldErrorPolicy::Warn);
        assert_eq!(prepared.request.delivery, SubscriptionDeliveryMode::Deltas);
        assert_eq!(prepared.request.stream_capacity, None);
        assert_eq!(prepared.request.flush_threshold, None);
        assert_eq!(prepared.request.overflow_policy, None);
    }

    #[test]
    fn subscribe_arguments_carry_every_stream_option() {
        let prepared = prepare_subscribe_args(
            r#"{"service":"//blp/mktdata","tickers":["IBM US Equity"],"fields":["BID"],
                "options":{"options":[" & interval=5 ","  "],"flushThreshold":8,
                "overflowPolicy":"drop_newest","streamCapacity":32,"allFields":true,
                "aliases":{"IBM US Equity":"ibm"},"onDelayed":"raise","isolated":true,
                "rows":false,"onFieldError":"ignore","zeroAsNull":["BID"],
                "delivery":"deltas"}}"#,
            64,
        )
        .expect("all options");
        let request = prepared.request;
        assert_eq!(prepared.batch_items, 8);
        assert_eq!(request.options, ["interval=5"]);
        assert_eq!(request.flush_threshold, Some(8));
        assert_eq!(request.stream_capacity, Some(32));
        assert_eq!(request.overflow_policy, Some(OverflowPolicy::DropNewest));
        assert!(request.all_fields);
        assert_eq!(
            request.aliases,
            [("IBM US Equity".to_string(), "ibm".to_string())]
        );
        assert_eq!(request.delayed_policy, DelayedPolicy::Raise);
        assert!(request.isolated);
        assert!(!request.deliver_rows);
        assert_eq!(request.field_error_policy, FieldErrorPolicy::Ignore);
        assert_eq!(request.zero_as_null, ["BID"]);
    }

    #[test]
    fn subscribe_raw_options_pass_through_and_named_options_add_conflate() {
        let raw = prepare_subscribe_args(
            r#"{"tickers":["A"],"fields":["F"],"rawOptions":true,"options":{"options":[" &raw "]}}"#,
            64,
        )
        .expect("raw options");
        assert_eq!(raw.request.options, [" &raw "]);

        let named = prepare_subscribe_args(
            r#"{"tickers":["A"],"fields":["F"],"options":{"conflate":true}}"#,
            64,
        )
        .expect("conflate");
        assert_eq!(named.request.options, ["conflate"]);
    }

    #[test]
    fn subscribe_rejections_are_coded_with_the_offending_element() {
        let cases = [
            (
                r#"{"onDelayed":"WARN"}"#,
                "onDelayed",
                "onDelayed must be warn, raise, or ignore",
            ),
            (
                r#"{"onDelayed":" warn "}"#,
                "onDelayed",
                "onDelayed must be warn, raise, or ignore",
            ),
            (
                r#"{"onFieldError":"silent"}"#,
                "onFieldError",
                "onFieldError must be warn, raise, or ignore",
            ),
            (
                r#"{"delivery":"Raise"}"#,
                "delivery",
                "delivery must be deltas or state",
            ),
            (
                r#"{"isolated":"yes"}"#,
                "isolated",
                "isolated must be a boolean",
            ),
            (
                r#"{"aliases":["IBM"]}"#,
                "aliases",
                "aliases must map topic strings to label strings",
            ),
            (
                r#"{"delivery":"state","streamCapacity":4}"#,
                "streamCapacity",
                "delivery=state does not accept streamCapacity",
            ),
            (
                r#"{"conflate":true,"options":["interval=5"]}"#,
                "conflate",
                "conflate=true cannot be combined with interval options; intervalization overrides conflation",
            ),
            (
                r#"{"streamCapacity":0}"#,
                "streamCapacity",
                "streamCapacity must be greater than zero",
            ),
            (
                r#"{"flushThreshold":0}"#,
                "flushThreshold",
                "flushThreshold must be greater than zero",
            ),
        ];
        for (options, element, message) in cases {
            let args = format!(r#"{{"tickers":["A"],"fields":["F"],"options":{options}}}"#);
            let error = subscribe_error(&args);
            assert_eq!(error.status, Status::InvalidArg, "{options}");
            let (code, context, text) = decode_coded(&error);
            assert_eq!(code, "[XBBG:VALIDATION", "{options}");
            assert_eq!(context["element"], element, "{options}");
            assert_eq!(text, message, "{options}");
        }

        let error = subscribe_error(
            r#"{"service":"//blp/mktvwap","tickers":["A"],"fields":["F"],"options":{"conflate":true}}"#,
        );
        let (code, context, text) = decode_coded(&error);
        assert_eq!(code, "[XBBG:VALIDATION");
        assert_eq!(context["element"], "conflate");
        assert_eq!(
            text,
            "conflate=true is only supported for //blp/mktdata subscriptions"
        );

        let error = subscribe_error(
            r#"{"tickers":["A"],"fields":["F"],"options":{"overflowPolicy":"nope"}}"#,
        );
        let (code, context, text) = decode_coded(&error);
        assert_eq!(code, "[XBBG:VALIDATION");
        assert_eq!(context["element"], "overflowPolicy");
        assert!(text.contains("nope"), "{text}");

        let error =
            subscribe_error(r#"{"tickers":["A"],"fields":["F"],"options":{"backend":"x"}}"#);
        assert_eq!(error.status, Status::InvalidArg);
        assert_eq!(error.reason, "[XBBG:ARGUMENT] unknown field 'backend'");
    }

    #[test]
    fn image_only_reads_reject_without_receiving_or_discarding_terminal_errors() {
        use std::task::Poll;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(1);
            let subscription = subscription_with_receiver(rx, false);
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "SYNTHETIC Equity".to_string(),
                detail: "synthetic terminal failure".to_string(),
            });
            // A row read must fail before taking the receive lock or doing cleanup.
            let rx_guard = subscription.host.consumer().rx.lock().await;
            let mut scalar_read = Box::pin(subscription.next_updates(None, None));
            let mut arrow_read = Box::pin(subscription.next_arrow_batch(None, None));
            let mut delivery_read = Box::pin(subscription.next_delivery(None, None, None));
            std::future::poll_fn(|cx| {
                let scalar_error = match scalar_read.as_mut().poll(cx) {
                    Poll::Ready(Err(error)) => error,
                    _ => panic!("image-only scalar reads must reject immediately"),
                };
                let arrow_error = match arrow_read.as_mut().poll(cx) {
                    Poll::Ready(Err(error)) => error,
                    _ => panic!("image-only Arrow reads must reject immediately"),
                };
                let delivery_error = match delivery_read.as_mut().poll(cx) {
                    Poll::Ready(Err(error)) => error,
                    _ => panic!("image-only delivery reads must reject immediately"),
                };
                for error in [scalar_error, arrow_error, delivery_error] {
                    assert_eq!(error.status, Status::InvalidArg);
                    assert!(error.reason.starts_with("[XBBG:VALIDATION]"));
                    assert!(error.reason.contains("rows: false"));
                    assert!(error.reason.contains("latest()"));
                }
                Poll::Ready(())
            })
            .await;
            assert!(!subscription.delivers_rows());
            assert!(!subscription.host.is_closed());
            drop(rx_guard);

            let error = match subscription.unsubscribe(Some(true)).await {
                Err(error) => error,
                Ok(_) => panic!("drain must retain the unread terminal error"),
            };
            assert_eq!(error.status, Status::GenericFailure);
            assert!(error.reason.starts_with("[XBBG:DATALOSS]"));
            assert!(error.reason.contains("synthetic terminal failure"));
            assert!(!subscription.delivers_rows());
        });
    }

    #[test]
    fn row_subscription_reads_updates_and_retains_mode_after_unsubscribe() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(1);
            let subscription = subscription_with_receiver(rx, true);
            tx.send(Ok(subscription_update(1, 42)))
                .await
                .expect("synthetic update");
            let batch = subscription
                .next_updates(None, None)
                .await
                .expect("row read")
                .output
                .expect("row conversion")
                .expect("update batch");
            assert_eq!(batch.updates[0].timestamp_us, 42);
            assert!(subscription.delivers_rows());
            subscription.unsubscribe(Some(false)).await.expect("close");
            assert!(subscription.delivers_rows());
            assert!(
                subscription
                    .next_updates(None, None)
                    .await
                    .unwrap()
                    .output
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn legacy_native_outputs_hold_receiver_until_publication_when_first_marked_gate_is_absent() {
        use std::task::Poll;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            for arrow in [false, true] {
                let (tx, rx) = subscription_channel(2);
                let subscription = subscription_with_receiver(rx, true);
                for timestamp in [1, 2] {
                    tx.try_send(Ok(subscription_update(1, timestamp)))
                        .expect("synthetic update");
                }
                let guard = if arrow {
                    let output = subscription
                        .next_arrow_batch(None, None)
                        .await
                        .expect("legacy Arrow");
                    assert!(output.output.as_ref().expect("Arrow output").is_some());
                    output.guard.expect("read guard")
                } else {
                    let output = subscription
                        .next_updates(None, None)
                        .await
                        .expect("legacy scalar");
                    assert_eq!(
                        output
                            .output
                            .as_ref()
                            .expect("scalar output")
                            .as_ref()
                            .unwrap()
                            .updates[0]
                            .timestamp_us,
                        1
                    );
                    output.guard.expect("read guard")
                };
                assert!(guard.holds_receiver());
                assert!(!guard.holds_publication());
                assert!(!subscription.host.consumer().has_delivery_read_gate());
                let mut marked = Box::pin(subscription.next_delivery(None, None, None));
                std::future::poll_fn(|cx| {
                    assert!(matches!(marked.as_mut().poll(cx), Poll::Pending));
                    Poll::Ready(())
                })
                .await;
                assert!(subscription.host.consumer().has_delivery_read_gate());
                // This is where ToNapiValue releases the legacy publication
                // guard after constructing the complete JS result.
                drop(guard);
                let completion = marked.await.expect("marked read").expect("second update");
                let PreparedDelivery::Data(delivery) = completion.read.prepare() else {
                    panic!("expected delivery data");
                };
                let ts = delivery
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .expect("timestamp")
                    .value(0);
                let receipt = delivery.publish(&completion.host).expect("publish");
                assert!(completion.host.commit_delivery(receipt).expect("commit"));
                assert_eq!(ts, 2);
                subscription.unsubscribe(Some(false)).await.expect("close");
            }
        });
    }

    #[test]
    fn native_delivery_drops_restore_overflow_data_and_gap_until_commit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            let subscription = subscription_with_receiver(rx, true);
            for timestamp in 1..=3 {
                tx.try_send(Ok(subscription_update(1, timestamp)))
                    .expect("overwrite queue accepts updates");
            }
            assert_eq!(subscription.delivery(), "deltas");
            assert!(subscription.requires_deliveries());
            for error in [
                subscription
                    .next_updates(None, None)
                    .await
                    .err()
                    .expect("plain scalar guard"),
                subscription
                    .next_arrow_batch(None, None)
                    .await
                    .err()
                    .expect("plain Arrow guard"),
            ] {
                assert_eq!(error.status, Status::InvalidArg);
            }
            let completion = subscription
                .next_delivery(None, None, None)
                .await
                .expect("native read")
                .expect("retained delivery");
            let PreparedDelivery::Data(delivery) = completion.read.prepare() else {
                panic!("expected delivery data");
            };
            assert_eq!(delivery.kind, SubscriptionDeliveryMode::Deltas);
            assert_eq!(delivery.data.num_rows(), 1);
            assert_eq!(delivery.gap_before.map(|gap| gap.dropped_updates), Some(2));
            drop(delivery);

            let retried = subscription
                .next_delivery(None, None, None)
                .await
                .expect("native retry")
                .expect("restored delivery");
            let PreparedDelivery::Data(delivery) = retried.read.prepare() else {
                panic!("expected restored delivery data");
            };
            assert_eq!(delivery.gap_before.map(|gap| gap.dropped_updates), Some(2));
            let ts = delivery
                .data
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .expect("timestamp")
                .value(0);
            let receipt = delivery.publish(&retried.host).expect("publish retry");
            assert!(retried.host.commit_delivery(receipt).expect("commit retry"));
            assert_eq!(ts, 3);
            subscription
                .unsubscribe(Some(false))
                .await
                .expect("universal cleanup");
        });
    }

    #[test]
    fn native_delivery_drain_preserves_gap_and_plain_drain_guards_still_close() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            let subscription = subscription_with_receiver(rx, true);
            for timestamp in 1..=3 {
                tx.try_send(Ok(subscription_update(1, timestamp)))
                    .expect("synthetic update");
            }
            let drained = subscription
                .unsubscribe_deliveries(Some(true), None)
                .await
                .expect("delivery drain")
                .expect("drained delivery");
            assert_eq!(drained.prepared.deliveries.len(), 1);
            assert_eq!(
                drained.prepared.deliveries[0]
                    .gap_before
                    .map(|gap| gap.dropped_updates),
                Some(2)
            );
            assert_eq!(drained.prepared.deliveries[0].data.num_rows(), 1);
            if let Some(id) = drained.prepared.publish(&drained.host) {
                assert!(drained.host.commit_drain(id));
            }
            assert!(subscription.host.is_closed());

            for arrow in [false, true] {
                let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
                let subscription = subscription_with_receiver(rx, true);
                tx.try_send(Ok(subscription_update(1, 7)))
                    .expect("synthetic update");
                let error = if arrow {
                    subscription.unsubscribe_arrow(Some(true)).await.err()
                } else {
                    subscription.unsubscribe(Some(true)).await.err()
                }
                .expect("plain drains must reject");
                assert_eq!(error.status, Status::InvalidArg);
                assert!(subscription.host.is_closed());
                assert!(tx.is_closed());
            }
        });
    }

    #[test]
    fn native_delivery_terminal_errors_are_retryable_until_publication_then_end_once() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            let subscription = subscription_with_receiver(rx, true);
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "SYNTHETIC Equity".into(),
                detail: "synthetic source loss".into(),
            });
            let first = subscription
                .next_delivery(None, None, None)
                .await
                .expect("native read")
                .expect("terminal delivery");
            let PreparedDelivery::Error(err_delivery) = first.read.prepare() else {
                panic!("expected terminal error delivery");
            };
            drop(err_delivery);
            let terminal = subscription
                .next_delivery(None, None, None)
                .await
                .expect("native retry")
                .expect("restored terminal delivery");
            let PreparedDelivery::Error(err_delivery) = terminal.read.prepare() else {
                panic!("expected restored terminal error");
            };
            let receipt = err_delivery.publish(&terminal.host).expect("publish error");
            let error = terminal
                .host
                .commit_delivery(receipt)
                .expect_err("terminal error");
            assert_eq!(error.kind, HostErrorKind::DataLoss);
            assert!(
                subscription
                    .next_delivery(None, None, None)
                    .await
                    .expect("EOF")
                    .is_none()
            );
        });
    }

    #[test]
    fn native_delivery_deadlines_cancel_reads_without_closing_or_losing_future_gaps() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            let subscription = subscription_with_receiver(rx, true);
            assert!(
                subscription
                    .next_delivery(None, Some(1), None)
                    .await
                    .expect("deadline")
                    .is_none()
            );
            assert!(!subscription.host.is_closed());
            tx.try_send(Ok(subscription_update(1, 1)))
                .expect("first update");
            tx.try_send(Ok(subscription_update(1, 2)))
                .expect("second update");
            let read = subscription
                .next_delivery(Some(1), None, None)
                .await
                .expect("next read")
                .expect("new delivery");
            let PreparedDelivery::Data(delivery) = read.read.prepare() else {
                panic!("expected delivery");
            };
            assert_eq!(delivery.gap_before.map(|gap| gap.dropped_updates), Some(1));
            let receipt = delivery.publish(&read.host).expect("publish");
            assert!(read.host.commit_delivery(receipt).expect("commit"));
            let error = subscription
                .next_delivery(Some(0), None, None)
                .await
                .err()
                .expect("zero batch limit must reject");
            assert_eq!(error.status, Status::InvalidArg);
            subscription.unsubscribe(Some(false)).await.expect("close");
        });
    }

    #[test]
    fn request_error_transport_preserves_structured_sdk_identifiers() {
        for timeout in [false, true] {
            for count in [1, 2] {
                let ids: Vec<std::sync::Arc<str>> = ["sdk-synthetic-]\"\\\n", "sdk-synthetic-z"]
                    .into_iter()
                    .take(count)
                    .map(std::sync::Arc::from)
                    .collect();
                let core = if timeout {
                    BlpError::RequestTimeout {
                        request_id: Some("req-label".into()),
                        bloomberg_request_ids: ids.clone(),
                    }
                } else {
                    BlpError::RequestFailure {
                        service: "//synthetic/refdata".into(),
                        operation: Some("SyntheticRequest".into()),
                        cid: None,
                        label: Some("synthetic failure".into()),
                        request_id: Some("req-label".into()),
                        bloomberg_request_ids: ids.clone(),
                        source: None,
                    }
                };
                let error = blp_error_to_napi(core);
                let prefix = if timeout {
                    "[XBBG:TIMEOUT] [XBBG_CONTEXT]"
                } else {
                    "[XBBG:REQUEST] [XBBG_CONTEXT]"
                };
                let (json, message) = error
                    .reason
                    .strip_prefix(prefix)
                    .expect("context prefix")
                    .split_once('\n')
                    .expect("one JSON line");
                let context: serde_json::Value = serde_json::from_str(json).unwrap();
                assert_eq!(context["request_id"], "req-label");
                assert_eq!(
                    context["bloomberg_request_ids"],
                    serde_json::json!(ids.iter().map(AsRef::as_ref).collect::<Vec<&str>>())
                );
                assert_eq!(
                    context["bloomberg_request_id"].as_str(),
                    (count == 1).then(|| ids[0].as_ref())
                );
                assert!(message.contains("[request_id=req-label]"));
                assert_eq!(error.status, Status::GenericFailure);
            }
        }
    }

    #[test]
    fn core_error_adapters_preserve_napi_status_codes_and_context() {
        for wrapped in [false, true] {
            for (error, status, message) in [
                (
                    BlpError::SessionStart {
                        source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                        label: Some("unavailable".into()),
                    },
                    Status::GenericFailure,
                    "[XBBG:SESSION] Session start failed: unavailable - synthetic cause",
                ),
                (
                    BlpError::SubscriptionFailure {
                        cid: None,
                        label: None,
                    },
                    Status::GenericFailure,
                    "[XBBG:REQUEST] Subscription failed",
                ),
                (
                    BlpError::RequestFailure {
                        service: "//blp/refdata".into(),
                        operation: Some("ReferenceDataRequest".into()),
                        cid: Some(xbbg_core::errors::CorrelationContext::U64(42)),
                        label: Some("subcategory=DAILY_CAPACITY_REACHED".into()),
                        request_id: Some("synthetic-request".into()),
                        bloomberg_request_ids: Vec::new(),
                        source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                    },
                    Status::GenericFailure,
                    "[XBBG:LIMIT] Request failed on //blp/refdata::ReferenceDataRequest (cid=42) [request_id=synthetic-request] - subcategory=DAILY_CAPACITY_REACHED: synthetic cause",
                ),
                (
                    BlpError::SchemaTypeMismatch {
                        element: "fields".into(),
                        expected: "String".into(),
                        found: "Int32".into(),
                    },
                    Status::InvalidArg,
                    "[XBBG:VALIDATION] Schema type mismatch at fields: expected String, found Int32",
                ),
                (
                    BlpError::Validation {
                        message: "invalid request".into(),
                        errors: vec![xbbg_core::errors::ValidationError {
                            path: "fields[0]".into(),
                            message: "unknown field".into(),
                            suggestion: Some("PX_LAST".into()),
                        }],
                    },
                    Status::InvalidArg,
                    "[XBBG:VALIDATION] invalid request: fields[0]: unknown field (did you mean 'PX_LAST'?)",
                ),
            ] {
                let error = if wrapped {
                    blp_async_error_to_napi(error.into())
                } else {
                    blp_error_to_napi(error)
                };
                assert_eq!(error.status, status);
                assert_eq!(error.reason, message);
            }
        }
    }

    #[test]
    fn engine_and_recipe_errors_preserve_napi_status_codes() {
        for recipe in [false, true] {
            for (error, status, message) in [
                (
                    BlpAsyncError::AllWorkersDown { pool_size: 2 },
                    Status::GenericFailure,
                    "[XBBG:SESSION] All 2 request workers are down",
                ),
                (
                    BlpAsyncError::ChannelClosed,
                    Status::GenericFailure,
                    "[XBBG:INTERNAL] Channel closed unexpectedly",
                ),
                (
                    BlpAsyncError::Internal("synthetic failure".into()),
                    Status::GenericFailure,
                    "[XBBG:INTERNAL] synthetic failure",
                ),
                (
                    BlpAsyncError::ConfigError {
                        detail: "synthetic config failure".into(),
                    },
                    Status::InvalidArg,
                    "[XBBG:VALIDATION] Configuration error: synthetic config failure",
                ),
                (
                    BlpError::Timeout.into(),
                    Status::GenericFailure,
                    "[XBBG:TIMEOUT] Request timed out",
                ),
                (
                    BlpError::Internal {
                        detail: "session connection dropped (worker=2)".into(),
                    }
                    .into(),
                    Status::GenericFailure,
                    "[XBBG:INTERNAL] Internal error: session connection dropped (worker=2)",
                ),
                (
                    BlpError::SubscriptionDataLoss {
                        topic: "SYNTHETIC Equity".into(),
                        detail: "synthetic overflow".into(),
                    }
                    .into(),
                    Status::GenericFailure,
                    "[XBBG:DATALOSS] Subscription data loss [topic=SYNTHETIC Equity]: synthetic overflow",
                ),
            ] {
                let error = if recipe {
                    recipe_error_to_napi(xbbg_recipes::RecipeError::Engine(Box::new(error)))
                } else {
                    blp_async_error_to_napi(error)
                };
                assert_eq!(error.status, status);
                assert_eq!(error.reason, message);
            }
        }
    }

    #[test]
    fn contextual_recipe_errors_keep_codes_messages_and_sdk_ids() {
        use xbbg_recipes::RecipeError;

        let sources: [fn() -> RecipeError; 5] = [
            || RecipeError::Other("synthetic recipe rejection".into()),
            || RecipeError::InvalidArgument("synthetic recipe argument".into()),
            || {
                RecipeError::from(BlpAsyncError::ConfigError {
                    detail: "synthetic engine validation".into(),
                })
            },
            || RecipeError::from(BlpAsyncError::AllWorkersDown { pool_size: 2 }),
            || RecipeError::from(BlpAsyncError::from(BlpError::Timeout)),
        ];
        for count in [1, 2] {
            for source in sources {
                let plain = recipe_error_to_napi(source());
                let (code, message) = plain
                    .reason
                    .strip_prefix("[XBBG:")
                    .unwrap()
                    .split_once("] ")
                    .unwrap();
                let ids = ["synthetic_a", "synthetic_z"];
                let error = recipe_error_to_napi(source().with_bloomberg_request_ids(
                    ids.iter().take(count).map(|id| std::sync::Arc::from(*id)),
                ));
                assert_eq!(error.status, plain.status);
                let prefix = format!("[XBBG:{code}] [XBBG_CONTEXT]");
                let (context, actual_message) = error
                    .reason
                    .strip_prefix(&prefix)
                    .unwrap()
                    .split_once('\n')
                    .unwrap();
                let context: serde_json::Value = serde_json::from_str(context).unwrap();
                assert_eq!(actual_message, message);
                assert_eq!(
                    context["bloomberg_request_ids"],
                    serde_json::json!(&ids[..count])
                );
                assert_eq!(
                    context["bloomberg_request_id"].as_str(),
                    (count == 1).then_some(ids[0])
                );
                for attribute in ["service", "operation", "request_id"] {
                    assert!(context[attribute].is_null(), "no invented {attribute}");
                }
            }
        }
    }

    #[test]
    fn closed_subscription_controls_are_validation_errors_without_masking_other_errors() {
        let error = subscription_control_error_to_napi(BlpAsyncError::ChannelClosed);
        assert_eq!(error.status, Status::InvalidArg);
        assert!(error.reason.starts_with("[XBBG:VALIDATION]"));
        assert!(error.reason.contains("already closed"));

        let error = subscription_control_error_to_napi(BlpError::Timeout.into());
        assert_eq!(error.status, Status::GenericFailure);
        assert!(error.reason.starts_with("[XBBG:TIMEOUT]"));

        let error = subscription_control_error_to_napi(BlpAsyncError::Blp(
            BlpError::SubscriptionDataLoss {
                topic: "SYNTHETIC Equity".to_string(),
                detail: "synthetic terminal failure".to_string(),
            },
        ));
        assert_eq!(error.status, Status::GenericFailure);
        assert!(error.reason.starts_with("[XBBG:DATALOSS]"));
        assert!(error.reason.contains("synthetic terminal failure"));
    }

    #[test]
    fn tick_values_resolve_like_the_former_kind_selected_slots() {
        use crate::subscriptions::TickValue::{BigInt, Bool, F64, I32, Null, Str, Text};

        // JS used to read the typed slot selected by the layout kind: a payload in
        // another slot read as null, and promoted string layouts read every
        // concrete payload as its text rendering.
        let payloads = [
            UpdateValue::Null,
            UpdateValue::Bool(true),
            UpdateValue::I32(42),
            UpdateValue::I64(-9_007_199_254_740_993),
            UpdateValue::F64(1.25),
            UpdateValue::Str(Arc::from("text")),
            UpdateValue::Date32(7),
            UpdateValue::Time64Micros(3_600_000_000),
            UpdateValue::TimestampMicros(1_700_000_000_000_000),
        ];
        let resolve = |kind| {
            payloads
                .iter()
                .map(|payload| TickValue::resolve(kind, payload))
                .collect::<Vec<_>>()
        };
        let text = |value: &str| Text(value.to_string());

        assert_eq!(
            resolve(FieldKind::Bool),
            [Null, Bool(true), Null, Null, Null, Null, Null, Null, Null]
        );
        for kind in [FieldKind::I32, FieldKind::Date32] {
            assert_eq!(
                resolve(kind),
                [Null, Null, I32(42), Null, Null, Null, I32(7), Null, Null]
            );
        }
        assert_eq!(
            resolve(FieldKind::F64),
            [Null, Null, Null, Null, F64(1.25), Null, Null, Null, Null]
        );
        for kind in [
            FieldKind::I64,
            FieldKind::Time64Micros,
            FieldKind::TimestampMicros,
        ] {
            assert_eq!(
                resolve(kind),
                [
                    Null,
                    Null,
                    Null,
                    BigInt(-9_007_199_254_740_993),
                    Null,
                    Null,
                    Null,
                    BigInt(3_600_000_000),
                    BigInt(1_700_000_000_000_000),
                ]
            );
        }
        for kind in [FieldKind::Str, FieldKind::Unknown] {
            assert_eq!(
                resolve(kind),
                [
                    Null,
                    text("true"),
                    text("42"),
                    text("-9007199254740993"),
                    text("1.25"),
                    Str("text"),
                    text("7"),
                    text("3600000000"),
                    text("1700000000000000"),
                ]
            );
        }
    }

    #[test]
    fn subscription_data_loss_uses_the_structured_native_error_code() {
        let error = blp_error_to_napi(BlpError::SubscriptionDataLoss {
            topic: "IBM US Equity".to_string(),
            detail: "stream queue reached capacity".to_string(),
        });

        assert_eq!(
            error.reason,
            "[XBBG:DATALOSS] Subscription data loss [topic=IBM US Equity]: stream queue reached capacity"
        );
    }

    #[test]
    fn request_input_preserves_all_request_fields() {
        let params = RequestParams::try_from(RequestInput {
            service: "//blp/refdata".to_string(),
            operation: String::new(),
            request_operation: Some("ReferenceDataRequest".to_string()),
            request_id: Some("req-123".to_string()),
            extractor: Some("refdata".to_string()),
            securities: Some(vec!["IBM US Equity".to_string()]),
            security: Some("IBM US Equity".to_string()),
            fields: Some(vec!["PX_LAST".to_string()]),
            overrides: Some(vec![pair("EQY_FUND_CRNCY", "USD")]),
            security_overrides: Some(vec![SecurityOverridesInput {
                security: "IBM US Equity".to_string(),
                overrides: vec![pair("CRNCY", "EUR")],
            }]),
            elements: Some(vec![pair("returnEids", "true")]),
            kwargs: Some(vec![pair("Period", "D")]),
            json_elements: Some(r#"{"nested":{"flag":true},"count":3}"#.to_string()),
            start_date: Some("20240101".to_string()),
            end_date: Some("20240131".to_string()),
            start_datetime: Some("2024-01-01T09:30:00".to_string()),
            end_datetime: Some("2024-01-01T10:00:00".to_string()),
            request_tz: Some("NY".to_string()),
            output_tz: Some("UTC".to_string()),
            event_type: Some("TRADE".to_string()),
            event_types: Some(vec!["TRADE".to_string(), "BID".to_string()]),
            interval: Some(5),
            options: Some(vec![pair("includeConditionCodes", "true")]),
            field_types: Some(vec![pair("PX_LAST", "Float64")]),
            include_security_errors: Some(true),
            return_eids: Some(true),
            on_delayed: Some("warn".to_string()),
            validate_fields: Some(false),
            search_spec: Some("price".to_string()),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            format: Some("long_typed".to_string()),
        })
        .expect("request input");

        assert_eq!(params.service, "//blp/refdata");
        assert_eq!(params.operation, "");
        assert_eq!(
            params.request_operation.as_deref(),
            Some("ReferenceDataRequest")
        );
        assert_eq!(params.request_id.as_deref(), Some("req-123"));
        assert_eq!(params.extractor, ExtractorType::RefData);
        assert!(params.extractor_set);
        assert_eq!(
            params.securities.as_deref(),
            Some(&["IBM US Equity".to_string()][..])
        );
        assert_eq!(params.security.as_deref(), Some("IBM US Equity"));
        assert_eq!(params.fields.as_deref(), Some(&["PX_LAST".to_string()][..]));
        assert_eq!(
            params.overrides.as_deref(),
            Some(&[("EQY_FUND_CRNCY".to_string(), "USD".to_string())][..])
        );
        assert_eq!(
            params.security_overrides.as_deref(),
            Some(
                &[(
                    "IBM US Equity".to_string(),
                    vec![("CRNCY".to_string(), "EUR".to_string())]
                )][..]
            )
        );
        let elements = params.elements.as_ref().expect("elements");
        assert!(elements.contains(&("returnEids".to_string(), "true".to_string())));
        assert!(elements.contains(&("nested.flag".to_string(), "true".to_string())));
        assert!(elements.contains(&("count".to_string(), "3".to_string())));
        assert_eq!(
            params
                .kwargs
                .as_ref()
                .and_then(|values| values.get("Period")),
            Some(&"D".to_string())
        );
        assert_eq!(params.start_date.as_deref(), Some("20240101"));
        assert_eq!(params.end_date.as_deref(), Some("20240131"));
        assert_eq!(
            params.start_datetime.as_deref(),
            Some("2024-01-01T09:30:00")
        );
        assert_eq!(params.end_datetime.as_deref(), Some("2024-01-01T10:00:00"));
        assert_eq!(params.request_tz.as_deref(), Some("NY"));
        assert_eq!(params.output_tz.as_deref(), Some("UTC"));
        assert_eq!(params.event_type.as_deref(), Some("TRADE"));
        assert_eq!(
            params.event_types.as_deref(),
            Some(&["TRADE".to_string(), "BID".to_string()][..])
        );
        assert_eq!(params.interval, Some(5));
        assert_eq!(
            params.options.as_deref(),
            Some(&[("includeConditionCodes".to_string(), "true".to_string())][..])
        );
        assert_eq!(
            params
                .field_types
                .as_ref()
                .and_then(|values| values.get("PX_LAST")),
            Some(&"Float64".to_string())
        );
        assert!(params.include_security_errors);
        assert!(params.return_eids);
        assert_eq!(params.on_delayed, Some(DelayedPolicy::Warn));
        assert_eq!(params.validate_fields, Some(false));
        assert_eq!(params.search_spec.as_deref(), Some("price"));
        assert_eq!(
            params.field_ids.as_deref(),
            Some(&["PX_LAST".to_string()][..])
        );
        assert_eq!(params.format.as_deref(), Some("long_typed"));
    }

    fn bare_request(operation: &str) -> RequestInput {
        RequestInput {
            service: "//blp/refdata".to_string(),
            operation: operation.to_string(),
            request_operation: None,
            request_id: None,
            extractor: None,
            securities: None,
            security: None,
            fields: None,
            overrides: None,
            security_overrides: None,
            elements: None,
            kwargs: None,
            json_elements: None,
            start_date: None,
            end_date: None,
            start_datetime: None,
            end_datetime: None,
            request_tz: None,
            output_tz: None,
            event_type: None,
            event_types: None,
            interval: None,
            options: None,
            field_types: None,
            include_security_errors: None,
            return_eids: None,
            on_delayed: None,
            validate_fields: None,
            search_spec: None,
            field_ids: None,
            format: None,
        }
    }

    #[test]
    fn request_input_flattens_json_elements_in_key_order_after_elements() {
        let params = RequestParams::try_from(RequestInput {
            elements: Some(vec![pair("first", "1")]),
            json_elements: Some(
                r#"{"z":{"b":2,"a":[1,{"x":null}]},"list":["s",3,true,null,[1]],"flag":false}"#
                    .to_string(),
            ),
            ..bare_request("ReferenceDataRequest")
        })
        .expect("json elements");
        let expected: Vec<(String, String)> = [
            ("first", "1"),
            ("flag", "false"),
            ("list", "s"),
            ("list", "3"),
            ("list", "true"),
            ("list", "null"),
            ("list", "[1]"),
            ("z.a", r#"[1,{"x":null}]"#),
            ("z.b", "2"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        assert_eq!(params.elements, Some(expected));
    }

    #[test]
    fn request_input_errors_are_coded_validation_errors() {
        for (json, message) in [
            ("{", "invalid jsonElements payload: "),
            ("[1]", "jsonElements must be a JSON object at the top level"),
            ("3", "jsonElements must be a JSON object at the top level"),
        ] {
            let error = RequestParams::try_from(RequestInput {
                json_elements: Some(json.to_string()),
                ..bare_request("ReferenceDataRequest")
            })
            .expect_err("bad jsonElements");
            assert_eq!(error.status, Status::InvalidArg, "{json}");
            let (code, context, text) = decode_coded(&error);
            assert_eq!(code, "[XBBG:VALIDATION", "{json}");
            assert_eq!(context["element"], "jsonElements", "{json}");
            assert!(text.starts_with(message), "{json}: {text}");
        }

        let error = RequestParams::try_from(RequestInput {
            on_delayed: Some("WARN".to_string()),
            ..bare_request("ReferenceDataRequest")
        })
        .expect_err("unknown onDelayed policy");
        let (code, context, text) = decode_coded(&error);
        assert_eq!(code, "[XBBG:VALIDATION");
        assert_eq!(context["element"], "onDelayed");
        assert_eq!(text, "onDelayed must be warn, raise, or ignore");

        let error = RequestParams::try_from(RequestInput {
            extractor: Some("nope".to_string()),
            ..bare_request("ReferenceDataRequest")
        })
        .expect_err("unknown extractor");
        assert_eq!(decode_coded(&error).2, "invalid extractor type: nope");
    }

    #[test]
    fn plan_request_returns_the_planned_request_as_json() {
        let planned: serde_json::Value = serde_json::from_str(
            &request::plan_request(
                "bdp".to_string(),
                r#"{"tickers":["IBM US Equity"],"fields":["PX_LAST"],"kwargs":{"b":2,"a":true}}"#
                    .to_string(),
            )
            .expect("bdp plan"),
        )
        .expect("plan json");
        assert_eq!(planned["service"], "//blp/refdata");
        assert_eq!(planned["operation"], "ReferenceDataRequest");
        assert_eq!(planned["extractor"], "refdata");
        assert_eq!(planned["includeSecurityErrors"], false);
        assert_eq!(
            planned["kwargs"],
            serde_json::json!([{"key": "b", "value": "2"}, {"key": "a", "value": "true"}])
        );

        let unknown = request::plan_request("bogus".to_string(), "{}".to_string())
            .expect_err("unknown method");
        assert_eq!(unknown.status, Status::GenericFailure);
        assert_eq!(
            unknown.reason,
            "[XBBG:INTERNAL] unknown engine method 'bogus'"
        );

        let malformed = request::plan_request(
            "bdp".to_string(),
            r#"{"tickers":["A"],"fields":["F"],"extra":1}"#.to_string(),
        )
        .expect_err("unknown key");
        assert_eq!(malformed.status, Status::InvalidArg);
        assert_eq!(malformed.reason, "[XBBG:ARGUMENT] unknown field 'extra'");
    }

    #[test]
    fn engine_config_input_rejects_invalid_resource_limits() {
        let err = EngineConfig::try_from(EngineConfigInput {
            runtime_worker_threads: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero runtime worker count should fail");
        assert!(err.to_string().contains("runtimeWorkerThreads"));

        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_pool_size: Some(4),
            max_subscription_sessions: Some(3),
            ..minimal_input()
        })
        .err()
        .expect("subscription prewarm above session cap should fail");
        assert!(err.to_string().contains("maxSubscriptionSessions"));

        let err = EngineConfig::try_from(EngineConfigInput {
            request_pool_size: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero request pool should fail during conversion");
        assert!(err.to_string().contains("requestPoolSize"));

        let config = EngineConfig::try_from(EngineConfigInput {
            subscription_pool_size: Some(0),
            ..minimal_input()
        })
        .expect("subscription prewarm may be disabled");
        assert_eq!(config.subscription_pool_size, 0);
    }

    #[test]
    fn engine_config_input_validates_bql_and_auth_app_names_independently() {
        for (bql_app_name, auth_app_name, expected_field) in [
            (" \t", "example-auth-app", "bqlAppName"),
            ("example-bql-app", "", "auth.appName"),
        ] {
            let err = EngineConfig::try_from(EngineConfigInput {
                bql_app_name: Some(bql_app_name.to_string()),
                auth: Some(AuthConfigInput {
                    method: "app".to_string(),
                    app_name: Some(auth_app_name.to_string()),
                    dir_property: None,
                    user_id: None,
                    ip_address: None,
                    token: None,
                }),
                ..minimal_input()
            })
            .err()
            .expect("one valid application name must not mask the other invalid name");
            assert_eq!(err.status, Status::InvalidArg);
            assert!(err.to_string().contains(expected_field));
        }
    }

    #[test]
    fn engine_config_input_defaults_leave_auth_unset() {
        let config =
            EngineConfig::try_from(minimal_input()).expect("default config should convert");

        assert_eq!(config.auth, None);
        let servers = direct_servers(&config);
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].host, "localhost");
        assert_eq!(servers[0].port, 8194);
        assert!(servers[0].proxy.is_none());
        assert_eq!(config.runtime_worker_threads, 2);
        assert_eq!(config.max_subscription_sessions, 32);
    }

    #[test]
    fn engine_config_input_rejects_zero_subscription_stream_capacity() {
        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_stream_capacity: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero subscription stream capacity should fail");

        assert!(
            err.to_string().contains("subscriptionStreamCapacity"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zero_subscription_flush_threshold() {
        let err = EngineConfig::try_from(EngineConfigInput {
            subscription_flush_threshold: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero subscription flush threshold should fail");
        assert!(err.to_string().contains("subscriptionFlushThreshold"));
    }

    #[test]
    fn engine_config_input_maps_bpipe_servers_with_socks5() {
        let config = EngineConfig::try_from(EngineConfigInput {
            servers: Some(vec![
                ServerAddressInput {
                    host: "primary.example.com".to_string(),
                    port: 8194,
                },
                ServerAddressInput {
                    host: "secondary.example.com".to_string(),
                    port: 8196,
                },
            ]),
            request_pool_size: Some(4),
            subscription_pool_size: Some(2),
            shard_requests: Some(true),
            shard_threshold: Some(5),
            shard_chunk_size: Some(3),
            shard_max_concurrent: Some(2),
            validation_mode: Some("strict".to_string()),
            subscription_flush_threshold: Some(8),
            max_event_queue_size: Some(16_000),
            command_queue_size: Some(512),
            subscription_stream_capacity: Some(1024),
            overflow_policy: Some("block".to_string()),
            warmup_services: Some(vec!["//blp/refdata".to_string()]),
            field_cache_path: Some("/tmp/xbbg-field-cache.json".to_string()),
            auth: Some(AuthConfigInput {
                method: "manual".to_string(),
                app_name: Some("app-name".to_string()),
                dir_property: None,
                user_id: Some("123456".to_string()),
                ip_address: Some("10.0.0.1".to_string()),
                token: None,
            }),
            num_start_attempts: Some(5),
            auto_restart_on_disconnection: Some(false),
            retry_policy: Some(RetryPolicyInput {
                max_retries: Some(3),
                initial_delay_ms: Some(250),
                backoff_factor: Some(1.5),
                max_delay_ms: Some(5_000),
            }),
            request_timeout_ms: Some(12_000),
            streams_deactivated_warn_ms: Some(45_000),
            reconnect_grace_ms: Some(2_500),
            keep_alive_enabled: Some(false),
            keep_alive_inactivity_ms: Some(25_000),
            keep_alive_response_timeout_ms: Some(11_000),
            slow_consumer_hi_water_mark: Some(0.8),
            slow_consumer_lo_water_mark: Some(0.4),
            sdk_log_level: Some("warn".to_string()),
            socks5: Some(Socks5ConfigInput {
                host: "proxy.example.com".to_string(),
                port: 1080,
            }),
            ..minimal_input()
        })
        .expect("direct+SOCKS5 config should convert");

        let servers = direct_servers(&config);
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].host, "primary.example.com");
        assert_eq!(servers[0].port, 8194);
        assert_eq!(servers[1].host, "secondary.example.com");
        assert_eq!(servers[1].port, 8196);
        // SOCKS5 is broadcast across every server.
        for s in servers {
            let proxy = s.proxy.as_ref().expect("proxy should be set");
            assert_eq!(proxy.host, "proxy.example.com");
            assert_eq!(proxy.port, 1080);
        }
        assert!(config.tls.is_none());
        assert_eq!(
            config.auth,
            Some(AuthConfig::Manual {
                app_name: "app-name".to_string(),
                user_id: "123456".to_string(),
                ip_address: "10.0.0.1".to_string(),
            })
        );
        assert_eq!(
            config.field_cache_path,
            Some(std::path::PathBuf::from("/tmp/xbbg-field-cache.json"))
        );
        assert_eq!(config.num_start_attempts, 5);
        assert!(!config.auto_restart_on_disconnection);
        assert!(config.shard_requests);
        assert_eq!(config.shard_threshold, 5);
        assert_eq!(config.shard_chunk_size, 3);
        assert_eq!(config.shard_max_concurrent, 2);
        assert_eq!(config.retry_policy.max_retries, 3);
        assert_eq!(config.retry_policy.initial_delay_ms, 250);
        assert_eq!(config.retry_policy.backoff_factor, 1.5);
        assert_eq!(config.retry_policy.max_delay_ms, 5_000);
        assert_eq!(config.request_timeout_ms, 12_000);
        assert_eq!(config.streams_deactivated_warn_ms, 45_000);
        assert_eq!(config.reconnect_grace_ms, 2_500);
    }

    #[test]
    fn engine_config_input_zfp_with_tls_resolves_zfp_transport() {
        let config = EngineConfig::try_from(EngineConfigInput {
            zfp_remote: Some("8194".to_string()),
            tls: Some(TlsConfigInput {
                client_credentials: Some(TlsMaterialInput("/tmp/client.p12".into())),
                client_credentials_password: Some("secret".to_string()),
                trust_material: Some(TlsMaterialInput("/tmp/trust.p7".into())),
                handshake_timeout_ms: Some(2000),
                crl_fetch_timeout_ms: Some(3000),
            }),
            ..minimal_input()
        })
        .expect("ZFP+TLS config should convert");

        match &config.transport {
            Transport::Zfp(remote) => {
                assert_eq!(*remote, xbbg_core::zfp::ZfpRemote::Remote8194);
            }
            other => panic!("expected Zfp, got {other}"),
        }
        let tls = config.tls.as_ref().expect("tls should be set");
        assert_eq!(
            tls.client_credentials,
            TlsMaterial::Path("/tmp/client.p12".into())
        );
        assert_eq!(tls.client_credentials_password, "secret");
        assert_eq!(
            tls.trust_material,
            TlsMaterial::Path("/tmp/trust.p7".into())
        );
        assert_eq!(tls.handshake_timeout_ms, Some(2000));
        assert_eq!(tls.crl_fetch_timeout_ms, Some(3000));
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_host() {
        let err = EngineConfig::try_from(EngineConfigInput {
            host: Some("bpipe.firm.com".to_string()),
            zfp_remote: Some("8194".to_string()),
            ..minimal_input()
        })
        .err()
        .expect("zfp + host should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_servers() {
        let err = EngineConfig::try_from(EngineConfigInput {
            servers: Some(vec![ServerAddressInput {
                host: "x".to_string(),
                port: 8194,
            }]),
            zfp_remote: Some("8196".to_string()),
            ..minimal_input()
        })
        .err()
        .expect("zfp + servers should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_rejects_zfp_plus_socks5() {
        let err = EngineConfig::try_from(EngineConfigInput {
            zfp_remote: Some("8194".to_string()),
            socks5: Some(Socks5ConfigInput {
                host: "proxy".to_string(),
                port: 1080,
            }),
            ..minimal_input()
        })
        .err()
        .expect("zfp + socks5 should fail");
        assert!(
            err.to_string().contains("zfpRemote cannot be combined"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn engine_config_input_requires_auth_fields_for_selected_method() {
        let err = match EngineConfig::try_from(EngineConfigInput {
            auth: Some(AuthConfigInput {
                method: "app".to_string(),
                app_name: None,
                dir_property: None,
                user_id: None,
                ip_address: None,
                token: None,
            }),
            ..minimal_input()
        }) {
            Ok(_) => panic!("missing appName should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("auth.appName is required"));
    }

    #[test]
    fn tls_input_owns_offset_bytes_before_async_normalization() {
        let credentials = b"\x30\0\xffsynthetic-client";
        let trust = b"\x30\0\xfesynthetic-trust";
        let mut client_backing = [b"prefix".as_slice(), credentials, b"suffix"].concat();
        let mut trust_backing = [b"prefix".as_slice(), trust, b"suffix"].concat();
        let client_input = TlsMaterialInput::from_bytes(&client_backing[6..6 + credentials.len()]);
        let trust_input = TlsMaterialInput::from_bytes(&trust_backing[6..6 + trust.len()]);
        let TlsMaterial::Blob(owned_client) = &client_input.0 else {
            panic!("expected owned blob");
        };
        let owned_client = owned_client.clone();
        let input = EngineConfigInput {
            zfp_remote: Some("8194".into()),
            tls: Some(TlsConfigInput {
                client_credentials: Some(client_input),
                client_credentials_password: Some("synthetic-password".into()),
                trust_material: Some(trust_input),
                handshake_timeout_ms: Some(0),
                crl_fetch_timeout_ms: Some(25),
            }),
            ..minimal_input()
        };
        client_backing.fill(0);
        trust_backing.fill(0);
        drop(client_backing);
        drop(trust_backing);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let config = runtime
            .block_on(async move {
                tokio::spawn(async move { EngineConfig::try_from(input) })
                    .await
                    .unwrap()
            })
            .unwrap();
        let tls = config.tls.as_ref().unwrap();
        let TlsMaterial::Blob(actual_client) = &tls.client_credentials else {
            panic!("expected client blob");
        };
        assert_eq!(actual_client.as_ref(), credentials);
        assert!(Arc::ptr_eq(actual_client, &owned_client));
        assert_eq!(tls.trust_material, TlsMaterial::Blob(Arc::from(&trust[..])));
        assert_eq!(tls.handshake_timeout_ms, Some(0));
        assert_eq!(tls.crl_fetch_timeout_ms, Some(25));
        assert!(matches!(config.transport, Transport::Zfp(_)));
        assert!(!format!("{tls:?}").contains("synthetic"));
    }

    #[test]
    fn engine_config_errors_are_coded_with_the_offending_member() {
        let error = EngineConfig::try_from(EngineConfigInput {
            request_timeout_ms: Some(-1),
            ..minimal_input()
        })
        .err()
        .expect("negative timeout should fail");
        assert_eq!(error.status, Status::InvalidArg);
        assert!(error.reason.starts_with("[XBBG:VALIDATION] [XBBG_CONTEXT]"));
        let (context, message) = error
            .reason
            .strip_prefix("[XBBG:VALIDATION] [XBBG_CONTEXT]")
            .and_then(|rest| rest.split_once('\n'))
            .expect("context line");
        let context: serde_json::Value = serde_json::from_str(context).expect("context json");
        assert_eq!(context["element"], "requestTimeoutMs");
        assert_eq!(
            message,
            "requestTimeoutMs must be a non-negative integer number of milliseconds"
        );

        let error = EngineConfig::try_from(EngineConfigInput {
            request_pool_size: Some(0),
            ..minimal_input()
        })
        .err()
        .expect("zero pool should fail");
        assert!(error.reason.starts_with("[XBBG:VALIDATION] "));
        assert!(error.reason.contains("requestPoolSize"));
    }

    #[test]
    fn host_errors_keep_their_kind_status_and_context_on_the_wire() {
        let bare = host_error_to_napi(HostError::internal("synthetic failure"));
        assert_eq!(bare.status, Status::GenericFailure);
        assert_eq!(bare.reason, "[XBBG:INTERNAL] synthetic failure");

        let argument = host_error_to_napi(HostError::argument("unknown field 'synthetic'"));
        assert_eq!(argument.status, Status::InvalidArg);
        assert_eq!(argument.reason, "[XBBG:ARGUMENT] unknown field 'synthetic'");

        let contextual = host_error_to_napi(
            HostError::validation("synthetic rejection")
                .with_element("tickers")
                .with_suggestion("PX_LAST")
                .with_bloomberg_request_ids(["sdk-synthetic-a", "sdk-synthetic-z"]),
        );
        assert_eq!(contextual.status, Status::InvalidArg);
        let (code, rest) = contextual.reason.split_once("] ").expect("code prefix");
        assert_eq!(code, "[XBBG:VALIDATION");
        let (context, message) = rest
            .strip_prefix("[XBBG_CONTEXT]")
            .and_then(|rest| rest.split_once('\n'))
            .expect("context line");
        assert_eq!(message, "synthetic rejection");
        let context: serde_json::Value = serde_json::from_str(context).expect("context json");
        assert_eq!(
            context,
            serde_json::json!({
                "element": "tickers",
                "suggestion": "PX_LAST",
                "bloomberg_request_ids": ["sdk-synthetic-a", "sdk-synthetic-z"],
            })
        );

        let single = host_error_to_napi(
            HostError::new(HostErrorKind::Request, "synthetic request failure")
                .with_bloomberg_request_ids(["sdk-synthetic-a"]),
        );
        let context = single
            .reason
            .strip_prefix("[XBBG:REQUEST] [XBBG_CONTEXT]")
            .and_then(|rest| rest.split_once('\n'))
            .map(|(context, _)| context)
            .expect("context line");
        let context: serde_json::Value = serde_json::from_str(context).expect("context json");
        assert_eq!(context["bloomberg_request_id"], "sdk-synthetic-a");
    }

    #[test]
    fn engine_config_input_rejects_missing_and_mixed_blob_pairs() {
        let blob = || TlsMaterialInput::from_bytes(b"synthetic-secret-blob");
        let path = || TlsMaterialInput("fixtures/material.der".into());
        for (credentials, trust) in [
            (Some(blob()), None),
            (None, Some(blob())),
            (Some(blob()), Some(path())),
            (Some(path()), Some(blob())),
        ] {
            let error = EngineConfig::try_from(EngineConfigInput {
                tls: Some(TlsConfigInput {
                    client_credentials: credentials,
                    client_credentials_password: Some("synthetic-secret-password".into()),
                    trust_material: trust,
                    handshake_timeout_ms: None,
                    crl_fetch_timeout_ms: None,
                }),
                ..minimal_input()
            })
            .err()
            .expect("missing or mixed TLS material should fail");
            assert_eq!(error.status, Status::InvalidArg);
            assert!(error.reason.contains("tls.clientCredentials"));
            assert!(error.reason.contains("tls.trustMaterial"));
            assert!(!error.reason.contains("synthetic-secret"));
            assert!(!error.reason.contains("fixtures/material.der"));
        }
    }

    #[test]
    fn engine_config_input_rejects_negative_tls_timeouts_without_or_with_credentials() {
        for with_material in [false, true] {
            for (handshake, crl, field) in [
                (Some(-5), None, "tls.handshakeTimeoutMs"),
                (None, Some(-5), "tls.crlFetchTimeoutMs"),
            ] {
                let err = EngineConfig::try_from(EngineConfigInput {
                    tls: Some(TlsConfigInput {
                        client_credentials: with_material
                            .then(|| TlsMaterialInput("fixtures/client.p12".into())),
                        client_credentials_password: None,
                        trust_material: with_material
                            .then(|| TlsMaterialInput("fixtures/trust.p7".into())),
                        handshake_timeout_ms: handshake,
                        crl_fetch_timeout_ms: crl,
                    }),
                    ..minimal_input()
                })
                .err()
                .expect("negative TLS timeout should fail");
                assert_eq!(err.status, Status::InvalidArg);
                assert!(err.to_string().contains(&format!(
                    "{field} must be a non-negative integer number of milliseconds"
                )));
            }
        }
    }

    #[test]
    fn engine_config_input_preserves_tls_pair_error_labels() {
        for (credentials, trust, expected) in [
            (
                Some("fixtures/client.p12".to_string()),
                None,
                "tls.clientCredentials set without tls.trustMaterial",
            ),
            (
                None,
                Some("fixtures/trust.p7".to_string()),
                "tls.trustMaterial set without tls.clientCredentials",
            ),
        ] {
            let err = EngineConfig::try_from(EngineConfigInput {
                tls: Some(TlsConfigInput {
                    client_credentials: credentials.map(|path| TlsMaterialInput(path.into())),
                    client_credentials_password: None,
                    trust_material: trust.map(|path| TlsMaterialInput(path.into())),
                    handshake_timeout_ms: None,
                    crl_fetch_timeout_ms: None,
                }),
                ..minimal_input()
            })
            .err()
            .expect("unpaired TLS");
            assert_eq!(err.status, Status::InvalidArg);
            assert!(err.to_string().contains(expected));
        }
    }

    #[test]
    fn engine_config_input_rejects_out_of_range_watermarks_before_narrowing() {
        for (hi, lo, field) in [
            (Some(1.0 + f64::EPSILON), None, "slowConsumerHiWaterMark"),
            (Some(f64::NAN), None, "slowConsumerHiWaterMark"),
            (None, Some(1.0), "slowConsumerLoWaterMark"),
            (None, Some(-0.1), "slowConsumerLoWaterMark"),
        ] {
            let err = EngineConfig::try_from(EngineConfigInput {
                slow_consumer_hi_water_mark: hi,
                slow_consumer_lo_water_mark: lo,
                ..minimal_input()
            })
            .err()
            .expect("out-of-range watermark");
            assert_eq!(err.status, Status::InvalidArg);
            assert!(err.to_string().contains(field));
        }
        let config = EngineConfig::try_from(EngineConfigInput {
            slow_consumer_hi_water_mark: Some(1.0),
            slow_consumer_lo_water_mark: Some(0.0),
            ..minimal_input()
        })
        .expect("watermark endpoints");
        assert_eq!(config.slow_consumer_hi_water_mark, Some(1.0));
        assert_eq!(config.slow_consumer_lo_water_mark, Some(0.0));
    }

    #[test]
    fn engine_config_input_preserves_keep_alive_and_duration_error_labels() {
        for (inactivity, response, field) in [
            (Some(-1), None, "keepAliveInactivityMs"),
            (None, Some(-1), "keepAliveResponseTimeoutMs"),
        ] {
            let err = EngineConfig::try_from(EngineConfigInput {
                keep_alive_inactivity_ms: inactivity,
                keep_alive_response_timeout_ms: response,
                ..minimal_input()
            })
            .err()
            .expect("negative keep-alive duration");
            assert!(
                err.to_string()
                    .contains(&format!("{field} must be non-negative"))
            );
        }
        let err = EngineConfig::try_from(EngineConfigInput {
            request_timeout_ms: Some(-1),
            ..minimal_input()
        })
        .err()
        .expect("negative unsigned duration");
        assert!(err.to_string().contains("requestTimeoutMs"));
        let err = EngineConfig::try_from(EngineConfigInput {
            reconnect_grace_ms: Some(-1),
            ..minimal_input()
        })
        .err()
        .expect("negative reconnect grace");
        assert!(err.to_string().contains("reconnectGraceMs"));
    }

    #[test]
    fn engine_config_input_defaults_reconnect_grace() {
        let config = EngineConfig::try_from(minimal_input()).expect("minimal config");
        assert_eq!(config.reconnect_grace_ms, 10_000);
        let config = EngineConfig::try_from(EngineConfigInput {
            reconnect_grace_ms: Some(0),
            ..minimal_input()
        })
        .expect("zero grace restores fail-fast");
        assert_eq!(config.reconnect_grace_ms, 0);
    }

    #[test]
    fn engine_config_input_normalizes_auth_aliases() {
        let config = EngineConfig::try_from(EngineConfigInput {
            auth: Some(AuthConfigInput {
                method: " DIRECTORY ".to_string(),
                app_name: None,
                dir_property: Some("synthetic-property".to_string()),
                user_id: None,
                ip_address: None,
                token: None,
            }),
            ..minimal_input()
        })
        .expect("directory alias");
        assert_eq!(
            config.auth,
            Some(AuthConfig::Directory {
                property_name: "synthetic-property".to_string(),
            })
        );
    }

    #[test]
    fn default_subscription_reads_one_update() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update(1, 20)))
                .await
                .expect("second");
            let subscription = subscription_with_receiver(rx, true);

            let first = subscription
                .next_updates(Some(1), None)
                .await
                .expect("read")
                .output
                .expect("conversion")
                .expect("updates");

            assert_eq!(first.updates.len(), 1);
            assert_eq!(first.updates[0].timestamp_us, 10);

            let second = subscription
                .next_updates(Some(1), None)
                .await
                .expect("second read")
                .output
                .expect("conversion")
                .expect("second update");
            assert_eq!(second.updates[0].timestamp_us, 20);
        });
    }

    #[test]
    fn bounded_subscription_read_flushes_sparse_partial_batch() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("update");
            let subscription = subscription_with_receiver(rx, true);

            let batch = subscription
                .next_updates(Some(4), Some(5))
                .await
                .expect("read")
                .output
                .expect("conversion")
                .expect("partial batch");

            assert_eq!(batch.updates.len(), 1);
            assert_eq!(batch.updates[0].timestamp_us, 10);
        });
    }

    #[test]
    fn layout_change_is_deferred_without_losing_update() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update(2, 20)))
                .await
                .expect("second");
            let subscription = subscription_with_receiver(rx, true);

            let first = subscription
                .next_updates(Some(2), None)
                .await
                .expect("first read")
                .output
                .expect("conversion")
                .expect("first batch");
            let second = subscription
                .next_updates(Some(2), Some(0))
                .await
                .expect("second read")
                .output
                .expect("conversion")
                .expect("second batch");

            assert_eq!(first.layout_version, 1);
            assert_eq!(second.layout_version, 2);
        });
    }

    #[test]
    fn same_version_schema_change_is_split_and_republished() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let first_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("PX_LAST", 0, FieldKind::F64)],
            ));
            let second_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("BID", 0, FieldKind::F64)],
            ));
            let (tx, rx) = subscription_channel(4);
            tx.send(Ok(subscription_update_with_layout(first_layout, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update_with_layout(second_layout, 20)))
                .await
                .expect("second");
            let subscription = subscription_with_receiver(rx, true);

            let first = subscription
                .next_updates(Some(2), None)
                .await
                .expect("first read")
                .output
                .expect("conversion")
                .expect("first output");
            let second = subscription
                .next_updates(Some(2), Some(0))
                .await
                .expect("second read")
                .output
                .expect("conversion")
                .expect("second output");

            assert_eq!(first.updates[0].timestamp_us, 10);
            assert!(first.layout.is_some());
            assert_eq!(second.updates[0].timestamp_us, 20);
            assert!(second.layout.is_some());
        });
    }

    #[test]
    fn cancelled_partial_read_restores_consumed_updates() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(1);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("first update");
            let subscription = subscription_with_receiver(rx, true);
            let mut reader = Box::pin(subscription.next_updates(Some(2), Some(60_000)));
            std::future::poll_fn(|cx| {
                assert!(reader.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(reader);

            tx.send(Ok(subscription_update(1, 20)))
                .await
                .expect("second update");
            let restored = subscription
                .next_updates(Some(2), Some(0))
                .await
                .expect("resumed read")
                .output
                .expect("conversion")
                .expect("restored batch");
            assert_eq!(
                restored
                    .updates
                    .iter()
                    .map(|update| update.timestamp_us)
                    .collect::<Vec<_>>(),
                [10, 20],
            );
        });
    }

    #[test]
    fn close_before_read_is_observed_without_a_lost_wakeup() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (_tx, rx) = subscription_channel(1);
            let subscription = subscription_with_receiver(rx, true);
            subscription.signal_closing();

            let updates = subscription
                .next_updates(Some(1), None)
                .await
                .expect("read")
                .output
                .expect("conversion");
            assert!(updates.is_none());
        });
    }

    #[test]
    fn full_queue_terminal_error_is_delivered_after_buffered_data_and_before_end() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel(1);
            tx.send(Ok(subscription_update(1, 10)))
                .await
                .expect("update");
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "IBM US Equity".to_string(),
                detail: "stream queue reached capacity".to_string(),
            });
            let subscription = subscription_with_receiver(rx, true);

            let batch = subscription
                .next_updates(Some(2), None)
                .await
                .expect("buffered update")
                .output
                .expect("conversion")
                .expect("batch");
            assert_eq!(batch.updates[0].timestamp_us, 10);

            let error = subscription
                .next_updates(Some(1), None)
                .await
                .err()
                .expect("terminal data-loss error");
            assert!(error.reason.starts_with("[XBBG:DATALOSS]"));
            assert!(error.reason.contains("stream queue reached capacity"));

            let end = subscription
                .next_updates(Some(1), None)
                .await
                .expect("end")
                .output
                .expect("conversion");
            assert!(end.is_none());
        });
    }

    #[test]
    fn errors_only_drain_returns_the_terminal_failure() {
        let (tx, rx) = subscription_channel(1);
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "ES1 Index".to_string(),
            detail: "Bloomberg reported DATALOSS".to_string(),
        });
        let pending = PendingUpdates::default();

        let error = pending
            .collect_unread(Some(rx), true)
            .expect_err("errors-only drain must fail");

        assert!(matches!(
            *error,
            BlpError::SubscriptionDataLoss {
                ref topic,
                ref detail,
            } if topic == "ES1 Index" && detail == "Bloomberg reported DATALOSS"
        ));
    }
}
