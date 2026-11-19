//! SDK-backed worker adapter for deterministic shared-feed tests and benchmarks.
//! Exercises the real CID dispatch and extraction without starting a session.

use super::*;

pub(in crate::engine) struct TestSubscriptionSession {
    shared: SubscriptionWorkerShared,
    #[cfg(test)]
    pub(in crate::engine) subscribes: AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) resubscribes: AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) unsubscribes: AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) fail_resubscribe: Mutex<HashSet<SlabKey>>,
}

impl TestSubscriptionSession {
    pub(in crate::engine) fn new(config: Arc<EngineConfig>) -> Arc<Self> {
        Arc::new(Self {
            shared: SubscriptionWorkerShared::new(
                0,
                config,
                Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8)),
            ),
            #[cfg(test)]
            subscribes: AtomicUsize::new(0),
            #[cfg(test)]
            resubscribes: AtomicUsize::new(0),
            #[cfg(test)]
            unsubscribes: AtomicUsize::new(0),
            #[cfg(test)]
            fail_resubscribe: Mutex::new(HashSet::new()),
        })
    }

    pub(in crate::engine) fn subscribe(
        &self,
        service: &str,
        registrations: Vec<FeedRegistration>,
        fields: Vec<String>,
        field_kinds: HashMap<String, FieldKind>,
        all_fields: bool,
        status: SharedSubscriptionStatus,
    ) -> Result<Vec<SlabKey>, BlpAsyncError> {
        self.shared.set_status(status.clone());
        self.shared
            .record_service_ready_if_already_open(service, true);
        let (_, keys, metrics, topics) =
            self.shared
                .register_subscriptions(SubscriptionRegistrationRequest {
                    service: service.into(),
                    registrations,
                    fields,
                    field_kinds,
                    all_fields,
                    options: Vec::new(),
                    overflow_policy: Some(OverflowPolicy::DropNewest),
                    forwarder: None,
                })?;
        status.update_topics(&keys, |next| next.add_active(&topics, &keys, metrics));
        #[cfg(test)]
        self.subscribes.fetch_add(keys.len(), Ordering::Relaxed);
        Ok(keys)
    }

    pub(in crate::engine) fn resubscribe(
        &self,
        key: SlabKey,
        fields: &[String],
        kinds: &HashMap<String, FieldKind>,
    ) -> Result<(), BlpAsyncError> {
        #[cfg(test)]
        self.resubscribes.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if self.fail_resubscribe.lock().contains(&key) {
            return Err(BlpAsyncError::ConfigError {
                detail: "synthetic resubscribe failure".into(),
            });
        }
        let mut worker = self.shared.state.lock();
        let sub = worker.subs.get_mut(key).ok_or_else(Self::unregistered)?;
        sub.add_fields(fields);
        sub.seed_kinds(kinds);
        Ok(())
    }

    pub(in crate::engine) fn enable_all_fields(&self, key: SlabKey) -> Result<(), BlpAsyncError> {
        let mut worker = self.shared.state.lock();
        let sub = worker.subs.get_mut(key).ok_or_else(Self::unregistered)?;
        sub.enable_all_fields();
        Ok(())
    }

    pub(in crate::engine) fn seed_kinds(
        &self,
        key: SlabKey,
        kinds: &HashMap<String, FieldKind>,
    ) -> Result<(), BlpAsyncError> {
        let mut worker = self.shared.state.lock();
        let sub = worker.subs.get_mut(key).ok_or_else(Self::unregistered)?;
        sub.seed_kinds(kinds);
        Ok(())
    }

    pub(in crate::engine) fn unsubscribe(
        &self,
        key: SlabKey,
        _status: &SharedSubscriptionStatus,
    ) -> Result<(), BlpAsyncError> {
        let (_, count) = self.shared.build_unsubscribe_list(vec![key]);
        #[cfg(test)]
        self.unsubscribes.fetch_add(count, Ordering::Relaxed);
        if count != 0 {
            self.status(key, "SubscriptionTerminated");
        }
        Ok(())
    }

    fn cid(key: SlabKey) -> i64 {
        match DispatchKey::from_slab_key(key).to_correlation_id() {
            CorrelationId::Int(value) => value,
            _ => unreachable!("slab dispatch uses integer CIDs"),
        }
    }

    /// Dispatch a scalar JSON update to the worker slot `key`.
    ///
    /// Worker keys are recycled, so `registered` names the registration the
    /// caller meant. It is evaluated under the worker lock at dispatch, the lock
    /// under which a slot is freed and reused, so a slot that no longer belongs
    /// to the caller is rejected instead of delivered to its new owner.
    pub(in crate::engine) fn publish_json(
        &self,
        key: SlabKey,
        json: &str,
        registered: &dyn Fn() -> bool,
    ) -> Result<(), BlpAsyncError> {
        use std::fmt::Write;

        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|error| BlpAsyncError::ConfigError {
                detail: format!("invalid synthetic update JSON: {error}"),
            })?;
        let values = value
            .as_object()
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "synthetic update must be a JSON object".into(),
            })?;
        let layout = {
            let worker = self.shared.state.lock();
            if !registered() {
                return Err(Self::unregistered());
            }
            worker
                .subs
                .get(key)
                .map(SubscriptionState::layout)
                .ok_or_else(Self::unregistered)?
        };
        let mut fields = String::new();
        for (name, value) in values {
            if name.chars().any(|character| {
                !matches!(
                    character,
                    '\u{9}' | '\u{a}' | '\u{d}'
                        | '\u{20}'..='\u{d7ff}'
                        | '\u{e000}'..='\u{fffd}'
                        | '\u{10000}'..='\u{10ffff}'
                )
            }) {
                return Err(BlpAsyncError::ConfigError {
                    detail: "synthetic field names contain XML 1.0-forbidden characters".into(),
                });
            }
            let kind = match value {
                serde_json::Value::String(_) => "String",
                serde_json::Value::Bool(_) => "Boolean",
                serde_json::Value::Number(_) => "Float64",
                serde_json::Value::Null if name.starts_with("MKTDATA_") => "String",
                serde_json::Value::Null if name == "IS_DELAYED_STREAM" => "Boolean",
                serde_json::Value::Null => {
                    match layout
                        .fields
                        .iter()
                        .find(|field| field.name.as_ref() == name)
                    {
                        Some(field) => match field.kind {
                            FieldKind::Bool => "Boolean",
                            FieldKind::I32 => "Int32",
                            FieldKind::I64 => "Int64",
                            FieldKind::Str => "String",
                            FieldKind::Date32 => "Date",
                            FieldKind::Time64Micros => "Time",
                            FieldKind::TimestampMicros => "Datetime",
                            FieldKind::Unknown | FieldKind::F64 => "Float64",
                        },
                        None => "Float64",
                    }
                }
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                    return Err(BlpAsyncError::ConfigError {
                        detail: "synthetic update fields must be scalar values".into(),
                    });
                }
            };
            fields.push_str("<element name=\"");
            for character in name.chars() {
                match character {
                    '&' => fields.push_str("&amp;"),
                    '<' => fields.push_str("&lt;"),
                    '>' => fields.push_str("&gt;"),
                    '"' => fields.push_str("&quot;"),
                    '\'' => fields.push_str("&apos;"),
                    '\t' => fields.push_str("&#x9;"),
                    '\n' => fields.push_str("&#xA;"),
                    '\r' => fields.push_str("&#xD;"),
                    character if !character.is_ascii() => {
                        write!(&mut fields, "&#x{:X};", u32::from(character))
                            .expect("writing synthetic field name to a String");
                    }
                    character => fields.push(character),
                }
            }
            write!(&mut fields, r#"" type="{kind}" minOccurs="0"/>"#)
                .expect("writing synthetic schema to a String");
        }
        self.try_data(key, &fields, json, registered)
    }

    fn unregistered() -> BlpAsyncError {
        BlpAsyncError::ConfigError {
            detail: "synthetic subscription is no longer registered".into(),
        }
    }

    #[cfg(test)]
    pub(in crate::engine) fn data(&self, key: SlabKey, fields: &str, json: &str) {
        self.try_data(key, fields, json, &|| true)
            .expect("valid synthetic SDK data");
    }

    fn try_data(
        &self,
        key: SlabKey,
        fields: &str,
        json: &str,
        registered: &dyn Fn() -> bool,
    ) -> Result<(), BlpAsyncError> {
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
            <service name="//xbbg/test" version="1.0.0.0">
                <event name="MarketDataEvents" eventType="Update"/>
            </service><schema><sequenceType name="Update">{fields}</sequenceType></schema>
            </ServiceDefinition>"#
        );
        let event = xbbg_core::test_support::TestEvent::try_with_schema(
            &schema,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[Self::cid(key)],
            |formatter| formatter.try_json(json),
        )?;
        let mut worker = self.shared.state.lock();
        if !registered() {
            return Err(Self::unregistered());
        }
        worker.dispatch_event(event.event(), &self.shared);
        Ok(())
    }

    pub(in crate::engine) fn status(&self, key: SlabKey, message_type: &str) {
        let event = xbbg_core::test_support::TestEvent::admin(
            EventType::SubscriptionStatus,
            message_type,
            &[Self::cid(key)],
            |_| {},
        );
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }

    #[cfg(test)]
    pub(in crate::engine) fn status_batch(&self, messages: &[(SlabKey, &str, String)]) {
        let schema = r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
        <service name="//xbbg/test" version="1.0.0.0">
            <event name="SubscriptionFailure" eventType="Status"/>
            <event name="SubscriptionTerminated" eventType="Status"/>
        </service><schema>
            <sequenceType name="Status"><element name="reason" type="Reason"/></sequenceType>
            <sequenceType name="Reason"><element name="description" type="String"/></sequenceType>
        </schema></ServiceDefinition>"#;
        let (key, message_type, reason) = messages.first().expect("nonempty status batch");
        let json = serde_json::json!({"reason": {"description": reason}}).to_string();
        let mut event = xbbg_core::test_support::TestEvent::with_schema(
            schema,
            EventType::SubscriptionStatus,
            message_type,
            &[Self::cid(*key)],
            |formatter| formatter.json(&json),
        );
        for (key, message_type, reason) in &messages[1..] {
            let json = serde_json::json!({"reason": {"description": reason}}).to_string();
            event.append_message(message_type, &[Self::cid(*key)], |formatter| {
                formatter.json(&json)
            });
        }
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }

    #[cfg(test)]
    pub(in crate::engine) fn field_exceptions(&self, key: SlabKey, json: &str) {
        let schema = r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
        <service name="//xbbg/test" version="1.0.0.0">
            <event name="SubscriptionStarted" eventType="Started"/>
        </service><schema>
            <sequenceType name="Started"><element name="exceptions" type="Exception" maxOccurs="unbounded"/></sequenceType>
            <sequenceType name="Exception"><element name="fieldId" type="String"/><element name="reason" type="Reason"/></sequenceType>
            <sequenceType name="Reason">
                <element name="category" type="String"/><element name="description" type="String"/>
                <element name="subcategory" type="String"/><element name="errorCode" type="Int32"/>
                <element name="source" type="String"/>
            </sequenceType>
        </schema></ServiceDefinition>"#;
        let event = xbbg_core::test_support::TestEvent::with_schema(
            schema,
            EventType::SubscriptionStatus,
            "SubscriptionStarted",
            &[Self::cid(key)],
            |formatter| formatter.json(json),
        );
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }

    pub(in crate::engine) fn session_event(&self, message_type: &str) {
        self.session_event_from(message_type, None);
    }

    /// A session status message naming the `server` it concerns.
    pub(in crate::engine) fn session_event_from(&self, message_type: &str, server: Option<&str>) {
        let event = if message_type == "AuthorizationRevoked" {
            // TestUtil has no built-in authorization schema.
            let schema = r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
            <service name="//xbbg/test" version="1.0.0.0">
                <event name="AuthorizationRevoked" eventType="Revoked"/>
            </service><schema><sequenceType name="Revoked"/></schema></ServiceDefinition>"#;
            xbbg_core::test_support::TestEvent::with_schema(
                schema,
                EventType::AuthorizationStatus,
                message_type,
                &[],
                |_| {},
            )
        } else {
            xbbg_core::test_support::TestEvent::admin(
                EventType::SessionStatus,
                message_type,
                &[],
                |formatter| {
                    if let Some(server) = server {
                        formatter.json(&format!(r#"{{"server":"{server}"}}"#));
                    }
                },
            )
        };
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }

    #[cfg(test)]
    pub(in crate::engine) fn admin_event(&self, message_type: &str) {
        let event =
            xbbg_core::test_support::TestEvent::admin(EventType::Admin, message_type, &[], |_| {});
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }

    #[cfg(test)]
    pub(in crate::engine) fn service_event(&self, message_type: &str) {
        self.service_event_for(message_type, "//blp/mktdata");
    }

    #[cfg(test)]
    pub(in crate::engine) fn service_event_for(&self, message_type: &str, service: &str) {
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
        <service name="//xbbg/test" version="1.0.0.0"><event name="{message_type}" eventType="Status"/></service>
        <schema><sequenceType name="Status"><element name="serviceName" type="String"/></sequenceType></schema>
        </ServiceDefinition>"#
        );
        let event = xbbg_core::test_support::TestEvent::with_schema(
            &schema,
            EventType::ServiceStatus,
            message_type,
            &[],
            |formatter| formatter.json(&serde_json::json!({"serviceName": service}).to_string()),
        );
        self.shared
            .state
            .lock()
            .dispatch_event(event.event(), &self.shared);
    }
}
