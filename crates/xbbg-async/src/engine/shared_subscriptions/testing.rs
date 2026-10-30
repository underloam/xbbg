//! Optional SDK-backed subscription fixtures; never opens a Bloomberg session.

use super::*;

pub(super) struct TestSessionFactory {
    config: Arc<EngineConfig>,
    pub(super) sessions: Mutex<Vec<Arc<TestSession>>>,
    #[cfg(test)]
    pub(super) owners: Mutex<Vec<Weak<FeedSession>>>,
    #[cfg(test)]
    pub(super) startup_delay: Mutex<Duration>,
}

impl TestSessionFactory {
    pub(super) async fn claim(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Arc<FeedSession> {
        #[cfg(test)]
        {
            let delay = *self.startup_delay.lock();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        let test = TestSession::new(self.config.clone());
        self.sessions.lock().push(test.clone());
        let owner = Arc::new_cyclic(|weak: &Weak<FeedSession>| {
            let weak = weak.clone();
            let status = Arc::new(SubscriptionStatusHandle::with_observer(Arc::new(
                move |previous, status, scope, events| {
                    if let Some(session) = weak.upgrade() {
                        session.on_status(previous, status, scope, events);
                    }
                },
            )));
            FeedSession {
                claim: Mutex::new(None),
                status,
                feeds: Mutex::new(HashMap::new()),
                live_feeds: AtomicUsize::new(0),
                test: Some(test),
                #[cfg(test)]
                routing: StatusRoutingMetrics::default(),
                _test_admission: Some(permit),
            }
        });
        #[cfg(test)]
        self.owners.lock().push(Arc::downgrade(&owner));
        owner
    }
}

#[cfg(test)]
pub(super) fn hub() -> (Arc<SharedSubscriptions>, Arc<TestSessionFactory>) {
    hub_with_config(EngineConfig::default())
}

pub(super) fn hub_with_config(
    mut config: EngineConfig,
) -> (Arc<SharedSubscriptions>, Arc<TestSessionFactory>) {
    config.subscription_pool_size = 0;
    let config = Arc::new(config);
    let pool = Arc::new(
        SubscriptionSessionPool::new(0, config.clone())
            .expect("valid synthetic subscription config"),
    );
    let factory = Arc::new(TestSessionFactory {
        config: config.clone(),
        sessions: Mutex::new(Vec::new()),
        #[cfg(test)]
        owners: Mutex::new(Vec::new()),
        #[cfg(test)]
        startup_delay: Mutex::new(Duration::ZERO),
    });
    let mut hub = SharedSubscriptions::new(pool, config, tokio::runtime::Handle::current(), None);
    Arc::get_mut(&mut hub).unwrap().test_sessions = Some(factory.clone());
    (hub, factory)
}

#[cfg(test)]
pub(super) fn test_feed(hub: &SharedSubscriptions, topic: &str) -> (Arc<TestSession>, SlabKey) {
    let feed = hub
        .registry
        .lock()
        .values()
        .find(|feed| feed.identity.topic == topic)
        .cloned()
        .unwrap();
    (
        feed.session.test.clone().unwrap(),
        feed.upstream_key.load(Ordering::Acquire),
    )
}

#[cfg(test)]
pub(super) fn data(session: &TestSession, key: SlabKey, json: &str) {
    session
        .publish_json(key, json)
        .expect("valid synthetic scalar update");
}

/// An offline owner of real shared feeds, consumers, SDK dispatch and decoders.
///
/// Available only with `test-support`. Construction requires a current Tokio
/// runtime and a valid subscription-session capacity. No SDK session is started.
#[cfg(feature = "test-support")]
pub struct SubscriptionFixture {
    hub: Arc<SharedSubscriptions>,
    sessions: Arc<TestSessionFactory>,
}

#[cfg(feature = "test-support")]
impl SubscriptionFixture {
    pub fn new(config: EngineConfig) -> Self {
        let (hub, sessions) = hub_with_config(config);
        Self { hub, sessions }
    }

    /// Construct a production subscription, including its chosen delivery mode.
    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<SubscriptionStream, BlpAsyncError> {
        self.hub.subscribe(request).await
    }

    /// Dispatch a scalar JSON object through SDK TestUtil and the actual CID.
    ///
    /// Omitted fields are absent from the SDK message, while explicit JSON nulls
    /// clear their fields. Strings, booleans and numbers use the same fixture
    /// schema convention as the shared-feed unit tests. The production decoder
    /// retains the source layout and observed kinds across sparse messages.
    /// XML 1.0-forbidden characters in field names return `ConfigError` before
    /// schema construction. Other names are escaped and accepted or rejected by
    /// the SDK; construction/formatting failures return errors rather than panic.
    pub fn publish_json(&self, topic: &str, json: &str) -> Result<(), BlpAsyncError> {
        let feed = self.lookup_feed(topic)?;
        let session = feed.session.test.as_ref().expect("synthetic session");
        session.publish_json(feed.upstream_key.load(Ordering::Acquire), json)
    }

    /// Obtain a direct decoded-input handle for production shared-feed timings.
    pub fn feed(&self, topic: &str) -> Result<SyntheticSubscriptionFeed, BlpAsyncError> {
        self.lookup_feed(topic)
            .map(|feed| SyntheticSubscriptionFeed { feed })
    }

    /// Inject an SDK data-loss summary; recovery follows the production path.
    ///
    /// After the recovery task runs, publish an `INITPAINT` summary followed by
    /// a normal delta to exercise recovery and the return to a live image.
    pub fn data_loss(&self, topic: &str) -> Result<(), BlpAsyncError> {
        self.publish_json(
            topic,
            r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#,
        )
    }

    /// Settle production recovery and cleanup effects, including dropped consumers.
    ///
    /// This is a production-task barrier, not a timer or a simulated recovery.
    /// An `INITPAINT` published after this returns can satisfy the pending
    /// recovery resubscribe deterministically on single- and multi-thread runtimes.
    pub async fn settle(&self) {
        self.hub.schedule_cleanup();
        loop {
            let pending = {
                let _gate = self.hub.mutations.lock().await;
                self.hub.cleanup_pending.load(Ordering::Acquire)
                    || self
                        .hub
                        .registry
                        .lock()
                        .values()
                        .any(|feed| feed.state.lock().recovery_task_scheduled)
            };
            if !pending {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Terminate every synthetic session through actual SDK session-status dispatch.
    pub fn terminate(&self) {
        let sessions = self.sessions.sessions.lock().clone();
        for session in sessions {
            session.session_event("SessionTerminated");
        }
    }

    fn lookup_feed(&self, topic: &str) -> Result<Arc<Feed>, BlpAsyncError> {
        let registry = self.hub.registry.lock();
        let mut matching = registry
            .values()
            .filter(|feed| feed.identity.topic == topic);
        let feed = matching
            .next()
            .ok_or_else(|| config_error("synthetic topic has no active subscription"))?;
        if matching.next().is_some() {
            return Err(config_error(
                "synthetic topic identifies more than one upstream feed",
            ));
        }
        Ok(Arc::clone(feed))
    }
}

/// Cloneable handle to actual decoded shared-feed delivery, without SDK decode.
///
/// Timings using this handle exclude transport, SDK message creation and decode.
/// Callers may share a stable full `FieldLayout` across representative updates.
#[cfg(feature = "test-support")]
#[derive(Clone)]
pub struct SyntheticSubscriptionFeed {
    feed: Arc<Feed>,
}

#[cfg(feature = "test-support")]
impl SyntheticSubscriptionFeed {
    /// Borrow the original payload directly; only production fan-out may clone it.
    pub fn publish(&self, update: &SubscriptionUpdate) {
        self.feed.on_update(update);
    }
}

#[cfg(all(test, feature = "test-support"))]
mod fixture_tests {
    use super::*;
    use arrow_array::{Array, Float64Array};

    fn request(delivery: SubscriptionDeliveryMode) -> SubscribeRequest {
        SubscribeRequest {
            topics: vec!["SYNTH FIXTURE".into()],
            fields: vec!["NUMBER".into(), "LABEL".into(), "FLAG".into()],
            delivery,
            stream_capacity: (delivery == SubscriptionDeliveryMode::Deltas).then_some(1),
            flush_threshold: Some(1),
            ..SubscribeRequest::default()
        }
    }

    fn value<'a>(update: &'a SubscriptionUpdate, name: &str) -> Option<&'a UpdateValue> {
        update.values.iter().find_map(|field| {
            (update.layout.fields[field.index as usize].name.as_ref() == name)
                .then_some(&field.value)
        })
    }

    fn number(batch: &RecordBatch) -> f64 {
        batch
            .column_by_name("NUMBER")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    fn flag(batch: &RecordBatch, name: &str) -> bool {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn public_fixture_rejects_xml_invalid_field_names_without_dispatching() {
        let fixture = SubscriptionFixture::new(EngineConfig::default());
        let mut stream = fixture
            .subscribe(request(SubscriptionDeliveryMode::Deltas))
            .await
            .unwrap();
        for name in ["BAD\0KEY", "BAD\u{1}KEY", "BAD\u{fffe}KEY"] {
            let json = serde_json::json!({(name): 1}).to_string();
            let result = fixture.publish_json("SYNTH FIXTURE", &json);
            assert!(matches!(
                result,
                Err(BlpAsyncError::ConfigError { ref detail })
                    if detail.contains("field names")
            ));
            assert!(stream.try_next().is_none());
        }
        fixture
            .publish_json("SYNTH FIXTURE", r#"{"NUMBER":2.5}"#)
            .unwrap();
        let update = stream.try_next().unwrap().unwrap();
        assert!(matches!(
            value(&update, "NUMBER"),
            Some(UpdateValue::F64(2.5))
        ));
    }

    #[tokio::test]
    async fn public_fixture_field_names_follow_actual_sdk_acceptance() {
        let fixture = SubscriptionFixture::new(EngineConfig::default());
        let mut stream = fixture
            .subscribe(SubscribeRequest {
                topics: vec!["SYNTH NAMES".into()],
                all_fields: true,
                ..SubscribeRequest::default()
            })
            .await
            .unwrap();
        for (name, schema_name) in [
            ("9FIELD", "9FIELD"),
            ("BID.FIELD", "BID.FIELD"),
            ("PRIX_\u{e9}", "PRIX_&#xE9;"),
            ("FIELD&\"NAME", "FIELD&amp;&quot;NAME"),
            ("BAD FIELD", "BAD FIELD"),
            ("BAD<KEY", "BAD&lt;KEY"),
            ("", ""),
        ] {
            let json = serde_json::json!({(name): 2.5}).to_string();
            let schema = format!(
                r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
                <service name="//xbbg/test" version="1.0.0.0">
                    <event name="MarketDataEvents" eventType="Update"/>
                </service><schema><sequenceType name="Update">
                    <element name="{schema_name}" type="Float64" minOccurs="0"/>
                </sequenceType></schema></ServiceDefinition>"#
            );
            let reference = xbbg_core::test_support::TestEvent::try_with_schema(
                &schema,
                xbbg_core::EventType::SubscriptionData,
                "MarketDataEvents",
                &[],
                |formatter| formatter.try_json(&json),
            );
            let actual = fixture.publish_json("SYNTH NAMES", &json);
            assert_eq!(actual.is_ok(), reference.is_ok(), "field {name:?}");
            if actual.is_ok() {
                let update = stream.try_next().unwrap().unwrap();
                assert!(matches!(value(&update, name), Some(UpdateValue::F64(2.5))));
            } else {
                assert!(stream.try_next().is_none());
            }
        }
    }

    #[tokio::test]
    async fn public_fixture_preserves_decoder_layout_for_sparse_and_typed_null_updates() {
        let fixture = SubscriptionFixture::new(EngineConfig::default());
        let mut stream = fixture
            .subscribe(request(SubscriptionDeliveryMode::Deltas))
            .await
            .unwrap();
        fixture
            .publish_json(
                "SYNTH FIXTURE",
                r#"{"NUMBER":1.25,"LABEL":"synthetic","FLAG":true}"#,
            )
            .unwrap();
        let first = stream.try_next().unwrap().unwrap();
        assert!(matches!(value(&first, "LABEL"), Some(UpdateValue::Str(_))));
        assert!(matches!(
            value(&first, "FLAG"),
            Some(UpdateValue::Bool(true))
        ));

        fixture
            .publish_json("SYNTH FIXTURE", r#"{"NUMBER":2.5}"#)
            .unwrap();
        let sparse = stream.try_next().unwrap().unwrap();
        assert_eq!(sparse.values.len(), 1);
        assert!(Arc::ptr_eq(&first.layout, &sparse.layout));
        assert!(value(&sparse, "LABEL").is_none());

        fixture
            .publish_json("SYNTH FIXTURE", r#"{"LABEL":null,"FLAG":null}"#)
            .unwrap();
        let cleared = stream.try_next().unwrap().unwrap();
        assert!(Arc::ptr_eq(&first.layout, &cleared.layout));
        assert!(matches!(value(&cleared, "LABEL"), Some(UpdateValue::Null)));
        assert!(matches!(value(&cleared, "FLAG"), Some(UpdateValue::Null)));
        assert!(value(&cleared, "NUMBER").is_none());

        fixture
            .publish_json("SYNTH FIXTURE", r#"{"NUMBER":"changed kind"}"#)
            .unwrap();
        let changed = stream.try_next().unwrap().unwrap();
        assert!(matches!(
            value(&changed, "NUMBER"),
            Some(UpdateValue::Str(_))
        ));
        assert!(!Arc::ptr_eq(&first.layout, &changed.layout));
        fixture.terminate();
        assert!(matches!(
            stream.try_next(),
            Some(Err(BlpError::Internal { .. }))
        ));
    }

    #[tokio::test]
    async fn public_fixture_drives_real_modes_loss_recovery_and_termination() {
        let fixture = SubscriptionFixture::new(EngineConfig::default());
        let mut strict = fixture
            .subscribe(request(SubscriptionDeliveryMode::Deltas))
            .await
            .unwrap();
        let mut lossy_request = request(SubscriptionDeliveryMode::Deltas);
        lossy_request.overflow_policy = Some(OverflowPolicy::DropOldest);
        let mut lossy = fixture.subscribe(lossy_request).await.unwrap().deliveries();
        let mut state = fixture
            .subscribe(request(SubscriptionDeliveryMode::State))
            .await
            .unwrap()
            .deliveries();
        let initial = state.next().await.unwrap().unwrap();
        assert!(!flag(&initial.data, "valid"));
        fixture
            .publish_json(
                "SYNTH FIXTURE",
                r#"{"NUMBER":1,"LABEL":"synthetic","FLAG":true}"#,
            )
            .unwrap();
        let mut update = strict.try_next().unwrap().unwrap();
        let feed = fixture.feed("SYNTH FIXTURE").unwrap();
        let producer = feed.clone();
        for number in 2..=4 {
            update
                .values
                .iter_mut()
                .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "NUMBER")
                .unwrap()
                .value = UpdateValue::F64(f64::from(number));
            producer.publish(&update);
        }
        assert!(matches!(
            value(&strict.try_next().unwrap().unwrap(), "NUMBER"),
            Some(UpdateValue::F64(2.0))
        ));
        assert!(matches!(
            strict.try_next(),
            Some(Err(BlpError::SubscriptionDataLoss { .. }))
        ));
        let overwritten = lossy.next().await.unwrap().unwrap();
        assert_eq!(overwritten.kind, SubscriptionDeliveryMode::Deltas);
        assert_eq!(overwritten.gap_before.unwrap().dropped_updates, 3);
        assert_eq!(number(&overwritten.data), 4.0);
        let conflated = state.next().await.unwrap().unwrap();
        assert_eq!(conflated.kind, SubscriptionDeliveryMode::State);
        assert_eq!(number(&conflated.data), 4.0);

        fixture.data_loss("SYNTH FIXTURE").unwrap();
        fixture.settle().await;
        assert!(matches!(
            lossy.next().await,
            Some(Err(BlpError::SubscriptionDataLoss { .. }))
        ));
        fixture.publish_json(
            "SYNTH FIXTURE",
            r#"{"NUMBER":9,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#,
        ).unwrap();
        let invalid = state.next().await.unwrap().unwrap();
        assert!(!flag(&invalid.data, "valid"));
        assert!(invalid.data.column_by_name("NUMBER").unwrap().is_null(0));
        let recovered = state.next().await.unwrap().unwrap();
        assert!(flag(&recovered.data, "valid"));
        assert!(!flag(&recovered.data, "live"));
        assert_eq!(number(&recovered.data), 9.0);
        fixture
            .publish_json("SYNTH FIXTURE", r#"{"NUMBER":10}"#)
            .unwrap();
        let live = state.next().await.unwrap().unwrap();
        assert!(flag(&live.data, "live"));
        fixture.terminate();
        assert!(matches!(
            state.next().await,
            Some(Err(BlpError::Internal { .. }))
        ));
    }

    #[tokio::test]
    async fn settle_retires_a_dropped_consumer_even_when_its_receiver_is_retained() {
        let fixture = SubscriptionFixture::new(EngineConfig::default());
        let stream = fixture
            .subscribe(SubscribeRequest {
                topics: vec!["SYNTH DROP".into()],
                fields: vec!["NUMBER".into()],
                ..SubscribeRequest::default()
            })
            .await
            .unwrap();
        let (_receiver, handle) = stream.into_parts();
        drop(handle);
        fixture.settle().await;
        assert!(fixture.feed("SYNTH DROP").is_err());
    }
}
