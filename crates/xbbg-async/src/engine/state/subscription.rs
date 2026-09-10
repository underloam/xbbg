//! Update-first subscription state for real-time data.
//!
//! Extracts Bloomberg subscription messages into native `SubscriptionUpdate`s
//! without constructing Arrow on the hot path. Arrow conversion is an explicit
//! compatibility adapter in `update_arrow`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use smallvec::SmallVec;
use tokio::sync::{mpsc, oneshot};

use xbbg_core::{BlpError, DataType as BlpDataType, Message, Name};

use super::super::OverflowPolicy;
use super::update::{
    FieldIndex, FieldKind, FieldLayout, FieldMeta, StringValueCache, SubscriptionUpdate, TopicId,
    UpdateField, UpdateValue,
};
use super::SubscriptionSender;

pub struct SubscriptionMetrics {
    pub messages_received: Arc<AtomicU64>,
    pub dropped_batches: Arc<AtomicU64>,
    pub batches_sent: Arc<AtomicU64>,
    pub slow_consumer: Arc<AtomicBool>,
    pub data_loss_events: Arc<AtomicU64>,
    pub last_message_us: Arc<AtomicU64>,
    pub last_data_loss_us: Arc<AtomicU64>,
}

const BLOCK_FORWARD_TIMEOUT: Duration = Duration::from_millis(50);

struct ForwardedSubscriptionUpdate {
    stream: SubscriptionSender,
    update: SubscriptionUpdate,
    metrics: Arc<SubscriptionMetrics>,
    topic: Arc<str>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Keep the common update inline: boxing would allocate on every SDK callback; drain barriers are rare"
)]
enum ForwarderCommand {
    Update(ForwardedSubscriptionUpdate),
    Drain(oneshot::Sender<()>),
}

/// Bounded, non-blocking ingress from Bloomberg's callback into an async
/// forwarding task. One forwarder is shared by every topic on a session.
#[derive(Clone)]
pub(crate) struct SubscriptionForwarder {
    tx: mpsc::Sender<ForwarderCommand>,
}

pub(crate) fn subscription_forwarder_channel(
    capacity: usize,
) -> (
    SubscriptionForwarder,
    impl Future<Output = ()> + Send + 'static,
) {
    let (tx, mut rx) = mpsc::channel::<ForwarderCommand>(capacity);
    let future = async move {
        while let Some(command) = rx.recv().await {
            let item = match command {
                ForwarderCommand::Update(item) => item,
                ForwarderCommand::Drain(done) => {
                    let _ = done.send(());
                    continue;
                }
            };
            let ForwardedSubscriptionUpdate {
                stream,
                update,
                metrics,
                topic,
            } = item;
            match tokio::time::timeout(BLOCK_FORWARD_TIMEOUT, stream.send(Ok(update))).await {
                Ok(Ok(())) => {
                    metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) => {}
                Err(_) => {
                    let dropped = metrics.dropped_batches.fetch_add(1, Ordering::Relaxed) + 1;
                    metrics.slow_consumer.store(true, Ordering::Relaxed);
                    stream.fail(BlpError::SubscriptionDataLoss {
                        topic: topic.to_string(),
                        detail: "bounded forwarding timed out; resubscribe for a fresh image"
                            .into(),
                    });
                    if dropped == 1 || dropped.is_multiple_of(1024) {
                        xbbg_log::warn!(
                            topic = %topic,
                            dropped,
                            policy = "Block",
                            "bounded forwarding timed out - subscription closed after data loss"
                        );
                    }
                }
            }
        }
    };
    (SubscriptionForwarder { tx }, future)
}

impl SubscriptionForwarder {
    fn try_forward(
        &self,
        stream: SubscriptionSender,
        update: SubscriptionUpdate,
        metrics: Arc<SubscriptionMetrics>,
        topic: Arc<str>,
    ) -> Result<(), mpsc::error::TrySendError<()>> {
        self.tx
            .try_send(ForwarderCommand::Update(ForwardedSubscriptionUpdate {
                stream,
                update,
                metrics,
                topic,
            }))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
                mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
            })
    }

    pub(crate) async fn drain(&self) -> Result<(), ()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(ForwarderCommand::Drain(done_tx))
            .await
            .map_err(|_| ())?;
        done_rx.await.map_err(|_| ())
    }
}

#[derive(Clone, Copy)]
enum AllFieldSlot {
    Captured {
        key: usize,
        idx: FieldIndex,
        datatype: BlpDataType,
    },
    Skipped {
        key: usize,
        datatype: BlpDataType,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageOutcome {
    Normal { first_message: bool },
    DataLoss,
    Closed,
}

/// State for a single subscription, owned by PumpA.
pub struct SubscriptionState {
    /// Topic string (e.g., "IBM US Equity")
    pub topic: Arc<str>,
    topic_id: TopicId,
    /// Field names as strings for layout, logs, and compatibility schemas.
    pub field_strings: Vec<Arc<str>>,
    /// Pre-interned field names for requested-field hot-path lookup.
    field_names: Vec<Name>,
    /// Fast dynamic-field lookup keyed by Bloomberg's interned Name pointer.
    field_name_keys: HashMap<usize, FieldIndex>,
    /// Initial requested/metadata fields, excluding all-fields discoveries.
    requested_fields: usize,
    /// Per-position allFields cache for stable Bloomberg subscription schemas.
    all_field_slots: Vec<Option<AllFieldSlot>>,
    field_kinds: Vec<FieldKind>,
    layout_version: u32,
    layout: Arc<FieldLayout>,
    /// Stream to send native updates (or errors for subscription failures).
    pub stream: SubscriptionSender,
    /// Session-scoped off-callback forwarding for `OverflowPolicy::Block`.
    forwarder: Option<SubscriptionForwarder>,
    /// Retained for option/status compatibility. Updates are emitted immediately.
    pub flush_threshold: usize,
    /// Slow consumer flag (DATALOSS received)
    pub slow_consumer: bool,
    /// Overflow policy for slow consumers
    pub overflow_policy: OverflowPolicy,
    /// Dropped update count (keeps historical field name for stats compatibility)
    pub dropped_batches: u64,
    pub metrics: Arc<SubscriptionMetrics>,
    /// Whether at least one data message has been observed.
    has_received_data: bool,
    /// Suppress stream-closed warnings during expected shutdown paths.
    suppress_closed_warning: bool,
    /// Whether to append all top-level scalar fields Bloomberg exposes.
    capture_all_fields: bool,
    /// Optional projected field for Bloomberg mktbar message kind (MarketBarStart/Update/End).
    subscription_data_index: Option<FieldIndex>,
    event_type_index: FieldIndex,
    event_subtype_index: FieldIndex,
    string_value_cache: Vec<StringValueCache>,
    subscription_data_type_cache: [Option<(usize, Arc<str>)>; 4],
}

impl SubscriptionState {
    const EVENT_METADATA_FIELDS: [&'static str; 2] =
        ["MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE"];
    const SUBSCRIPTION_DATA_FIELD: &'static str = "SUBSCRIPTION_DATA";

    /// Create a new subscription state with default overflow policy.
    pub fn new(
        topic: String,
        fields: Vec<String>,
        stream: SubscriptionSender,
        flush_threshold: usize,
        capture_all_fields: bool,
    ) -> Self {
        Self::with_policy(
            topic,
            fields,
            stream,
            flush_threshold,
            OverflowPolicy::default(),
            capture_all_fields,
        )
    }

    /// Create a new subscription state with specified overflow policy.
    pub fn with_policy(
        topic: String,
        fields: Vec<String>,
        stream: SubscriptionSender,
        flush_threshold: usize,
        overflow_policy: OverflowPolicy,
        capture_all_fields: bool,
    ) -> Self {
        Self::with_policy_and_forwarder(
            topic,
            fields,
            stream,
            flush_threshold,
            overflow_policy,
            capture_all_fields,
            None,
        )
    }

    pub(crate) fn with_policy_and_forwarder(
        topic: String,
        fields: Vec<String>,
        stream: SubscriptionSender,
        flush_threshold: usize,
        overflow_policy: OverflowPolicy,
        capture_all_fields: bool,
        forwarder: Option<SubscriptionForwarder>,
    ) -> Self {
        let mut field_strings =
            Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_names = Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_name_keys =
            HashMap::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_kinds = Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());

        for field in fields {
            Self::push_field_if_new(
                &mut field_strings,
                &mut field_names,
                &mut field_name_keys,
                &mut field_kinds,
                field.into(),
            );
        }
        let event_type_index = Self::push_field_if_new(
            &mut field_strings,
            &mut field_names,
            &mut field_name_keys,
            &mut field_kinds,
            Arc::from("MKTDATA_EVENT_TYPE"),
        );
        let event_subtype_index = Self::push_field_if_new(
            &mut field_strings,
            &mut field_names,
            &mut field_name_keys,
            &mut field_kinds,
            Arc::from("MKTDATA_EVENT_SUBTYPE"),
        );
        let subscription_data_index = if topic.starts_with("//blp/mktbar/") {
            Some(Self::push_field_if_new(
                &mut field_strings,
                &mut field_names,
                &mut field_name_keys,
                &mut field_kinds,
                Arc::from(Self::SUBSCRIPTION_DATA_FIELD),
            ))
        } else {
            None
        };

        let metrics = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });
        let layout = Self::build_layout(1, &field_strings, &field_kinds);
        let string_value_cache = vec![None; field_strings.len()];
        let requested_fields = field_names.len();

        Self {
            topic: Arc::from(topic),
            topic_id: 0,
            field_strings,
            field_names,
            field_name_keys,
            requested_fields,
            all_field_slots: Vec::new(),
            field_kinds,
            layout_version: 1,
            layout,
            stream,
            forwarder,
            flush_threshold,
            slow_consumer: false,
            overflow_policy,
            dropped_batches: 0,
            metrics,
            has_received_data: false,
            suppress_closed_warning: false,
            capture_all_fields,
            subscription_data_index,
            event_type_index,
            event_subtype_index,
            string_value_cache,
            subscription_data_type_cache: std::array::from_fn(|_| None),
        }
    }

    pub fn set_topic_id(&mut self, topic_id: TopicId) {
        self.topic_id = topic_id;
    }

    fn push_field_if_new(
        field_strings: &mut Vec<Arc<str>>,
        field_names: &mut Vec<Name>,
        field_name_keys: &mut HashMap<usize, FieldIndex>,
        field_kinds: &mut Vec<FieldKind>,
        field: Arc<str>,
    ) -> FieldIndex {
        let name = Name::get_or_intern(&field);
        let key = name.as_ptr() as usize;
        if let Some(&idx) = field_name_keys.get(&key) {
            return idx;
        }
        let idx = field_strings.len() as FieldIndex;
        field_name_keys.insert(key, idx);
        field_kinds.push(FieldKind::Unknown);
        field_names.push(name);
        field_strings.push(field);
        idx
    }

    fn build_layout(version: u32, names: &[Arc<str>], kinds: &[FieldKind]) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(
            version,
            names
                .iter()
                .zip(kinds.iter())
                .enumerate()
                .map(|(idx, (name, kind))| FieldMeta::new(name.clone(), idx as FieldIndex, *kind))
                .collect(),
        ))
    }

    /// Process a SUBSCRIPTION_DATA message using Element API.
    ///
    /// Timestamps use Bloomberg SDK receive time when available (requires
    /// `setRecordSubscriptionDataReceiveTimes(true)`), falling back to
    /// `SystemTime::now()` if not enabled.
    pub fn on_message(&mut self, msg: &Message) -> MessageOutcome {
        if self.stream.is_closed() {
            return MessageOutcome::Closed;
        }
        let timestamp = msg.time_received_us().unwrap_or_else(Self::system_time_us);
        let subscription_data = self.subscription_data_arc(msg);
        let elem = msg.elements();
        let values = if self.capture_all_fields {
            self.extract_all_fields(&elem, subscription_data.as_ref())
        } else {
            self.extract_requested_fields(&elem, subscription_data.as_ref())
        };
        let values = match values {
            Ok(values) => values,
            Err(error) => {
                self.fail(error);
                return MessageOutcome::Closed;
            }
        };

        if self.is_dataloss_update(&values) {
            self.on_dataloss(msg.time_received_us());
            return MessageOutcome::DataLoss;
        }

        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_message_us
            .store(timestamp as u64, Ordering::Relaxed);

        let first_message = !self.has_received_data;
        self.has_received_data = true;

        let update = SubscriptionUpdate {
            timestamp_us: timestamp,
            topic_id: self.topic_id,
            topic: self.topic.clone(),
            layout: self.layout.clone(),
            values,
        };
        if self.send_update(update) {
            MessageOutcome::Normal { first_message }
        } else {
            MessageOutcome::Closed
        }
    }

    fn subscription_data_arc(&mut self, msg: &Message) -> Option<Arc<str>> {
        self.subscription_data_index?;
        let key = msg.name_key();
        for (cached_key, cached) in self.subscription_data_type_cache.iter().flatten() {
            if *cached_key == key {
                return Some(Arc::clone(cached));
            }
        }
        let value = Arc::<str>::from(msg.type_str());
        let slot = key % self.subscription_data_type_cache.len();
        self.subscription_data_type_cache[slot] = Some((key, Arc::clone(&value)));
        Some(value)
    }

    fn is_dataloss_update(&self, values: &[UpdateField]) -> bool {
        let mut is_summary = false;
        let mut is_dataloss = false;
        for field in values {
            if field.index == self.event_type_index {
                is_summary =
                    matches!(&field.value, UpdateValue::Str(value) if value.as_ref() == "SUMMARY");
            } else if field.index == self.event_subtype_index {
                is_dataloss =
                    matches!(&field.value, UpdateValue::Str(value) if value.as_ref() == "DATALOSS");
            }
        }
        is_summary && is_dataloss
    }

    fn system_time_us() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    fn extract_requested_fields(
        &mut self,
        elem: &xbbg_core::Element<'_>,
        subscription_data: Option<&Arc<str>>,
    ) -> Result<SmallVec<[UpdateField; 8]>, BlpError> {
        let mut values = SmallVec::new();
        for idx in 0..self.field_names.len() {
            let value = if Some(idx as FieldIndex) == self.subscription_data_index {
                let Some(value) = subscription_data else {
                    continue;
                };
                UpdateValue::Str(Arc::clone(value))
            } else {
                let Some(field) = elem.get(&self.field_names[idx]) else {
                    // Missing is not a clear: leave it out of this delta.
                    continue;
                };
                let datatype = field.datatype();
                if field.is_array() || !Self::should_capture_datatype(datatype) {
                    return Err(Self::unsupported_shape(&field));
                }
                self.update_value_for_field(idx, &field, datatype)?
            };
            self.observe_kind(idx as FieldIndex, &value);
            values.push(UpdateField {
                index: idx as FieldIndex,
                value,
            });
        }
        Ok(values)
    }

    fn extract_all_fields(
        &mut self,
        elem: &xbbg_core::Element<'_>,
        subscription_data: Option<&Arc<str>>,
    ) -> Result<SmallVec<[UpdateField; 8]>, BlpError> {
        let children = elem.num_children();
        let projected =
            usize::from(self.subscription_data_index.is_some() && subscription_data.is_some());
        let mut values = SmallVec::with_capacity(children + projected);
        self.push_subscription_data(&mut values, subscription_data);
        for child_idx in 0..children {
            let child = elem
                .get_at(child_idx)
                .ok_or_else(|| BlpError::SchemaUnsupported {
                    element: elem.name_str().to_owned(),
                    detail: format!(
                    "Bloomberg reported {children} children but child {child_idx} was inaccessible"
                ),
                })?;

            let key = child.name_key();
            if child.is_array() {
                if self.is_requested_field(key) {
                    return Err(Self::unsupported_shape(&child));
                }
                // all_fields promises scalars. Do not cache this shape: another
                // message can expose a scalar at the same name and ordinal.
                continue;
            }

            let datatype = child.datatype();
            if let Some(Some(slot)) = self.all_field_slots.get(child_idx).copied() {
                match slot {
                    AllFieldSlot::Captured {
                        key: cached_key,
                        idx,
                        datatype: cached_datatype,
                    } if cached_key == key && cached_datatype == datatype => {
                        let value = self.update_value_for_field(idx as usize, &child, datatype)?;
                        self.observe_kind(idx, &value);
                        values.push(UpdateField { index: idx, value });
                        continue;
                    }
                    AllFieldSlot::Skipped {
                        key: cached_key,
                        datatype: cached_datatype,
                    } if cached_key == key && cached_datatype == datatype => {
                        if self.is_requested_field(key) {
                            return Err(Self::unsupported_shape(&child));
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            if !Self::should_capture_datatype(datatype) {
                if self.is_requested_field(key) {
                    return Err(Self::unsupported_shape(&child));
                }
                self.cache_all_field_slot(child_idx, AllFieldSlot::Skipped { key, datatype });
                continue;
            }

            let idx = self.ensure_field_for_child(&child, key);
            self.cache_all_field_slot(child_idx, AllFieldSlot::Captured { key, idx, datatype });
            let value = self.update_value_for_field(idx as usize, &child, datatype)?;
            self.observe_kind(idx, &value);
            values.push(UpdateField { index: idx, value });
        }
        Ok(values)
    }

    fn push_subscription_data(
        &mut self,
        values: &mut SmallVec<[UpdateField; 8]>,
        subscription_data: Option<&Arc<str>>,
    ) {
        let Some(idx) = self.subscription_data_index else {
            return;
        };
        let Some(value) = subscription_data else {
            return;
        };
        let value = UpdateValue::Str(Arc::clone(value));
        self.observe_kind(idx, &value);
        values.push(UpdateField { index: idx, value });
    }

    fn unsupported_shape(field: &xbbg_core::Element<'_>) -> BlpError {
        BlpError::SchemaUnsupported {
            element: field.name_str().to_owned(),
            detail: "subscription fields must be top-level scalar values".into(),
        }
    }

    fn update_value_for_field(
        &mut self,
        idx: usize,
        element: &xbbg_core::Element<'_>,
        datatype: BlpDataType,
    ) -> Result<UpdateValue, BlpError> {
        let value = match element.get_value_fast_with_datatype(0, datatype) {
            Some(value) => value,
            None if element.is_null() => xbbg_core::Value::Null,
            None => {
                return Err(BlpError::SchemaUnsupported {
                    element: self.field_strings[idx].to_string(),
                    detail: format!("cannot decode non-null Bloomberg {datatype:?} scalar"),
                });
            }
        };
        let value =
            UpdateValue::from_blp_with_str_cache(value, self.string_value_cache.get_mut(idx));
        if matches!(value, UpdateValue::Null) {
            self.observe_field_kind(idx as FieldIndex, FieldKind::from_blp_datatype(datatype));
        }
        Ok(value)
    }

    fn observe_kind(&mut self, idx: FieldIndex, value: &UpdateValue) {
        self.observe_field_kind(idx, FieldKind::from_value(value));
    }

    fn observe_field_kind(&mut self, idx: FieldIndex, observed: FieldKind) {
        let idx = idx as usize;
        let merged = self.field_kinds[idx].merge_observed(observed);
        if merged != self.field_kinds[idx] {
            self.field_kinds[idx] = merged;
            self.layout_version = self.layout_version.wrapping_add(1).max(1);
            self.layout =
                Self::build_layout(self.layout_version, &self.field_strings, &self.field_kinds);
        }
    }

    fn is_requested_field(&self, key: usize) -> bool {
        self.field_name_keys
            .get(&key)
            .is_some_and(|idx| (*idx as usize) < self.requested_fields)
    }

    fn should_capture_datatype(datatype: BlpDataType) -> bool {
        !matches!(
            datatype,
            BlpDataType::Sequence
                | BlpDataType::Choice
                | BlpDataType::ByteArray
                | BlpDataType::CorrelationId
        )
    }

    fn cache_all_field_slot(&mut self, child_idx: usize, slot: AllFieldSlot) {
        if child_idx >= self.all_field_slots.len() {
            self.all_field_slots.resize(child_idx + 1, None);
        }
        self.all_field_slots[child_idx] = Some(slot);
    }

    fn ensure_field_for_child(
        &mut self,
        field: &xbbg_core::Element<'_>,
        field_key: usize,
    ) -> FieldIndex {
        if let Some(&idx) = self.field_name_keys.get(&field_key) {
            return idx;
        }

        let field_name = Arc::<str>::from(field.name_str());
        let idx = self.field_strings.len() as FieldIndex;
        let name = Name::get_or_intern(&field_name);
        self.field_strings.push(field_name.clone());
        self.field_names.push(name);
        self.field_name_keys.insert(field_key, idx);
        self.field_kinds.push(FieldKind::Unknown);
        self.string_value_cache.push(None);
        self.layout_version = self.layout_version.wrapping_add(1).max(1);
        self.layout =
            Self::build_layout(self.layout_version, &self.field_strings, &self.field_kinds);
        idx
    }

    /// Handle DATALOSS indicator.
    pub fn on_dataloss(&mut self, timestamp_us: Option<i64>) {
        self.slow_consumer = true;
        self.metrics.slow_consumer.store(true, Ordering::Relaxed);
        self.metrics
            .data_loss_events
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.last_data_loss_us.store(
            timestamp_us.unwrap_or_default().max(0) as u64,
            Ordering::Relaxed,
        );
        self.stream.fail(BlpError::SubscriptionDataLoss {
            topic: self.topic.to_string(),
            detail: "Bloomberg reported DATALOSS; resubscribe for a fresh image".into(),
        });
        xbbg_log::warn!(topic = %self.topic, "DATALOSS detected - subscription closed");
    }

    pub fn clear_slow_consumer(&mut self) {
        self.slow_consumer = false;
        self.metrics.slow_consumer.store(false, Ordering::Relaxed);
    }

    pub fn mark_closing(&mut self) {
        self.suppress_closed_warning = true;
    }

    /// Native updates are emitted immediately. This remains for existing worker
    /// shutdown/drop callsites that previously flushed Arrow builders.
    pub fn flush(&mut self) {}

    /// Deliver a terminal error independently of bounded data queue capacity.
    pub fn fail(&self, error: BlpError) {
        self.stream.fail(error);
    }

    fn send_update(&mut self, update: SubscriptionUpdate) -> bool {
        if self.stream.is_closed() {
            return false;
        }
        match self.overflow_policy {
            OverflowPolicy::Block => {
                let result = match &self.forwarder {
                    Some(forwarder) => forwarder.try_forward(
                        self.stream.clone(),
                        update,
                        Arc::clone(&self.metrics),
                        Arc::clone(&self.topic),
                    ),
                    None => self
                        .stream
                        .try_send(Ok(update))
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                mpsc::error::TrySendError::Full(())
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                mpsc::error::TrySendError::Closed(())
                            }
                        }),
                };
                match result {
                    Ok(()) if self.forwarder.is_none() => {
                        self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                        true
                    }
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(())) => {
                        self.dropped_batches += 1;
                        self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                        self.metrics.slow_consumer.store(true, Ordering::Relaxed);
                        self.stream.fail(BlpError::SubscriptionDataLoss {
                            topic: self.topic.to_string(),
                            detail: "bounded forwarding queue full; resubscribe for a fresh image"
                                .into(),
                        });
                        if self.dropped_batches == 1 || self.dropped_batches.is_multiple_of(1024) {
                            xbbg_log::warn!(
                                topic = %self.topic,
                                dropped = self.dropped_batches,
                                policy = "Block",
                                "bounded forwarding queue full - subscription closed after data loss"
                            );
                        }
                        false
                    }
                    Err(mpsc::error::TrySendError::Closed(())) => {
                        if !self.suppress_closed_warning {
                            xbbg_log::warn!(topic = %self.topic, "stream closed");
                        }
                        false
                    }
                }
            }
            OverflowPolicy::DropNewest => match self.stream.try_send(Ok(update)) {
                Ok(()) => {
                    self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                    true
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_batches += 1;
                    self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                    self.stream.fail(BlpError::SubscriptionDataLoss {
                        topic: self.topic.to_string(),
                        detail: "subscription queue full; resubscribe for a fresh image".into(),
                    });
                    xbbg_log::warn!(
                        topic = %self.topic,
                        dropped = self.dropped_batches,
                        policy = "DropNewest",
                        "stream full - subscription closed after data loss"
                    );
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    if !self.suppress_closed_warning {
                        xbbg_log::warn!(topic = %self.topic, "stream closed");
                    }
                    false
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::subscription_channel;
    use xbbg_core::test_support::{TestEvent, TestMessageFormatter};

    fn fixture(
        fields: &str,
        message_type: &str,
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> TestEvent {
        fixture_with_types(fields, "", message_type, format)
    }

    fn fixture_with_types(
        fields: &str,
        additional_types: &str,
        message_type: &str,
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> TestEvent {
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
            <service name="//xbbg/test" version="1.0.0.0">
                <event name="{message_type}" eventType="Update"/>
            </service>
            <schema>
                <sequenceType name="Update">{fields}</sequenceType>
                {additional_types}
            </schema>
            </ServiceDefinition>"#
        );
        TestEvent::subscription(&schema, message_type, format)
    }

    fn deliver(state: &mut SubscriptionState, event: &TestEvent) -> MessageOutcome {
        state.on_message(&event.event().messages().next().expect("fixture message"))
    }

    #[test]
    fn sparse_updates_preserve_prior_values_and_apply_explicit_clears() {
        let image = fixture(
            r#"<element name="BID" type="Float64"/><element name="ASK" type="Float64"/>
            <element name="LAST_PRICE" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"BID":10.0,"ASK":20.0,"LAST_PRICE":30.0}"#),
        );
        // TestUtil creates omitted declared fields as null. BID must be absent
        // from this schema to exercise an actual missing field.
        let delta = fixture(
            r#"<element name="ASK" type="Float64" minOccurs="0"/>
            <element name="LAST_PRICE" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"ASK":null,"LAST_PRICE":0.0}"#),
        );
        for all_fields in [false, true] {
            let (tx, mut rx) = subscription_channel(2);
            let mut state = SubscriptionState::new(
                "TEST".into(),
                vec!["BID".into(), "ASK".into(), "LAST_PRICE".into()],
                tx,
                1,
                all_fields,
            );
            let mut latest = HashMap::new();
            for event in [&image, &delta] {
                deliver(&mut state, event);
                let update = rx.try_recv().unwrap().unwrap();
                for field in &update.values {
                    let name = update.layout.fields[field.index as usize].name.to_string();
                    let value = match field.value {
                        UpdateValue::F64(value) => Some(value),
                        UpdateValue::Null => None,
                        ref other => panic!("unexpected price value: {other:?}"),
                    };
                    latest.insert(name, value);
                }
            }
            assert_eq!(latest.get("BID"), Some(&Some(10.0)));
            assert_eq!(latest.get("ASK"), Some(&None));
            assert_eq!(latest.get("LAST_PRICE"), Some(&Some(0.0)));
            assert!(!latest.contains_key("MKTDATA_EVENT_TYPE"));
        }
    }

    #[test]
    fn formerly_guarded_field_preserves_timestamp_and_time_only_values() {
        for all_fields in [false, true] {
            for (with_date, expected) in [
                (true, UpdateValue::TimestampMicros(1_789_046_055_000_000)),
                (false, UpdateValue::Time64Micros(47_655_000_000)),
            ] {
                let datetime = blpapi_sys::blpapi_HighPrecisionDatetime_t {
                    datetime: blpapi_sys::blpapi_Datetime_t {
                        parts: if with_date {
                            xbbg_core::ffi::BLPAPI_DATETIME_TIME_PART
                                | xbbg_core::ffi::BLPAPI_DATETIME_DATE_PART
                        } else {
                            // Live LAST_UPDATE_ALL_SESSIONS_RT has H/M/S but no millisecond bit.
                            112
                        },
                        hours: 13,
                        minutes: 14,
                        seconds: 15,
                        milliSeconds: 0,
                        month: if with_date { 9 } else { 0 },
                        day: if with_date { 10 } else { 0 },
                        year: if with_date { 2026 } else { 0 },
                        offset: 0,
                    },
                    picoseconds: 0,
                };
                let event = fixture(
                    r#"<element name="LAST_UPDATE_ALL_SESSIONS_RT" type="Datetime"/>"#,
                    "MarketDataEvents",
                    |formatter| formatter.datetime("LAST_UPDATE_ALL_SESSIONS_RT", &datetime),
                );
                let (tx, mut rx) = subscription_channel(1);
                let mut state = SubscriptionState::new(
                    "TEST".into(),
                    vec!["LAST_UPDATE_ALL_SESSIONS_RT".into()],
                    tx,
                    1,
                    all_fields,
                );
                deliver(&mut state, &event);
                let update = rx.try_recv().unwrap().unwrap();
                match (&update.values[0].value, &expected) {
                    (
                        UpdateValue::TimestampMicros(actual),
                        UpdateValue::TimestampMicros(expected),
                    )
                    | (UpdateValue::Time64Micros(actual), UpdateValue::Time64Micros(expected)) => {
                        assert_eq!(actual, expected);
                    }
                    (actual, expected) => panic!("{actual:?} replaced {expected:?}"),
                }
            }
        }
    }

    #[test]
    fn null_char_defers_kind_but_real_char_changes_promote_layout() {
        for all_fields in [false, true] {
            let (tx, mut rx) = subscription_channel(1);
            let mut state =
                SubscriptionState::new("TEST".into(), vec!["FLAG".into()], tx, 1, all_fields);
            for (value, expected_kind) in [
                (None, FieldKind::Unknown),
                (Some(b'Y'), FieldKind::Bool),
                (Some(b'A'), FieldKind::Str),
                (Some(b'Y'), FieldKind::Str),
            ] {
                let event = fixture(
                    r#"<element name="FLAG" type="Char" minOccurs="0"/>"#,
                    "MarketDataEvents",
                    |formatter| formatter.char("FLAG", value),
                );
                deliver(&mut state, &event);
                let update = rx.try_recv().unwrap().unwrap();
                assert_eq!(update.layout.fields[0].kind, expected_kind);
                match value {
                    None => assert!(matches!(update.values[0].value, UpdateValue::Null)),
                    Some(b'Y') => {
                        assert!(matches!(update.values[0].value, UpdateValue::Bool(true)))
                    }
                    Some(_) => assert!(matches!(update.values[0].value, UpdateValue::I32(65))),
                }
            }
        }
    }

    #[test]
    fn scalar_datatype_changes_use_current_getter_and_promote_layout() {
        let numeric = fixture(
            r#"<element name="VALUE" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"VALUE":1.25}"#),
        );
        let text = fixture(
            r#"<element name="VALUE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"VALUE":"changed"}"#),
        );

        for all_fields in [false, true] {
            let fields = if all_fields {
                Vec::new()
            } else {
                vec!["VALUE".into()]
            };
            let (tx, mut rx) = subscription_channel(1);
            let mut state = SubscriptionState::new("TEST".into(), fields, tx, 1, all_fields);

            assert_eq!(
                deliver(&mut state, &numeric),
                MessageOutcome::Normal {
                    first_message: true
                }
            );
            let update = rx.try_recv().unwrap().unwrap();
            let field = update
                .values
                .iter()
                .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "VALUE")
                .expect("numeric field");
            assert!(matches!(
                &field.value,
                UpdateValue::F64(value) if *value == 1.25
            ));

            assert_eq!(
                deliver(&mut state, &text),
                MessageOutcome::Normal {
                    first_message: false
                }
            );
            let update = rx.try_recv().unwrap().unwrap();
            let field = update
                .values
                .iter()
                .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "VALUE")
                .expect("text field");
            assert!(matches!(&field.value, UpdateValue::Str(value) if value.as_ref() == "changed"));
            assert_eq!(
                update.layout.fields[field.index as usize].kind,
                FieldKind::Str
            );
        }
    }

    #[test]
    fn all_fields_rechecks_unsupported_complex_before_scalar_discovery() {
        let complex = fixture_with_types(
            r#"<element name="VALUE" type="ComplexValue"/>"#,
            r#"<sequenceType name="ComplexValue">
                <element name="INNER" type="String"/>
            </sequenceType>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"VALUE":{"INNER":"nested"}}"#),
        );
        let scalar = fixture(
            r#"<element name="VALUE" type="Int32"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"VALUE":7}"#),
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), Vec::new(), tx, 1, true);

        assert!(matches!(
            deliver(&mut state, &complex),
            MessageOutcome::Normal { .. }
        ));
        assert!(rx.try_recv().unwrap().unwrap().values.is_empty());

        assert!(matches!(
            deliver(&mut state, &scalar),
            MessageOutcome::Normal { .. }
        ));
        let update = rx.try_recv().unwrap().unwrap();
        let field = update
            .values
            .iter()
            .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "VALUE")
            .expect("scalar field discovered after complex value");
        assert!(matches!(field.value, UpdateValue::I32(7)));
    }

    #[test]
    fn requested_arrays_fail_instead_of_returning_the_first_value() {
        let event = fixture(
            r#"<element name="LEVELS" type="Float64" maxOccurs="unbounded"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":[1.25,2.5]}"#),
        );
        for all_fields in [false, true] {
            let (tx, mut rx) = subscription_channel(1);
            let mut state =
                SubscriptionState::new("TEST".into(), vec!["LEVELS".into()], tx, 1, all_fields);
            assert_eq!(deliver(&mut state, &event), MessageOutcome::Closed);
            assert!(matches!(
                rx.try_recv().unwrap(),
                Err(BlpError::SchemaUnsupported { element, .. }) if element == "LEVELS"
            ));
        }
    }

    #[test]
    fn all_fields_omits_array_between_discovered_scalar_updates() {
        let first_scalar = fixture(
            r#"<element name="LEVELS" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":3.5}"#),
        );
        let array = fixture(
            r#"<element name="LEVELS" type="Float64" maxOccurs="unbounded"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":[1.25,2.5]}"#),
        );
        let following_scalar = fixture(
            r#"<element name="LEVELS" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":4.5}"#),
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), Vec::new(), tx, 1, true);

        deliver(&mut state, &first_scalar);
        let update = rx.try_recv().unwrap().unwrap();
        let field = update
            .values
            .iter()
            .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "LEVELS")
            .expect("initial scalar discovery");
        assert!(matches!(field.value, UpdateValue::F64(3.5)));

        deliver(&mut state, &array);
        let update = rx.try_recv().unwrap().unwrap();
        assert!(!update
            .values
            .iter()
            .any(|field| { update.layout.fields[field.index as usize].name.as_ref() == "LEVELS" }));

        deliver(&mut state, &following_scalar);
        let update = rx.try_recv().unwrap().unwrap();
        let field = update
            .values
            .iter()
            .find(|field| update.layout.fields[field.index as usize].name.as_ref() == "LEVELS")
            .expect("scalar field after array");
        assert!(matches!(field.value, UpdateValue::F64(4.5)));
    }

    #[test]
    fn mktbar_message_kind_is_delivered_as_present_metadata() {
        let event = fixture(
            r#"<element name="LAST_PRICE" type="Float64"/>"#,
            "MarketBarStart",
            |formatter| formatter.json(r#"{"LAST_PRICE":1.0}"#),
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "//blp/mktbar/ticker/TEST".into(),
            vec!["LAST_PRICE".into()],
            tx,
            1,
            false,
        );
        deliver(&mut state, &event);
        let update = rx.try_recv().unwrap().unwrap();
        let kind = update
            .values
            .iter()
            .find(|field| {
                update.layout.fields[field.index as usize].name.as_ref() == "SUBSCRIPTION_DATA"
            })
            .expect("mktbar message kind");
        assert!(
            matches!(&kind.value, UpdateValue::Str(value) if value.as_ref() == "MarketBarStart")
        );
    }

    #[test]
    fn dataloss_follows_buffered_data_and_prevents_later_deltas() {
        let event = fixture(
            r#"<element name="MKTDATA_EVENT_TYPE" type="String"/>
            <element name="MKTDATA_EVENT_SUBTYPE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter
                    .json(r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"DATALOSS"}"#)
            },
        );
        for all_fields in [false, true] {
            let (tx, mut rx) = subscription_channel(1);
            tx.try_send(Ok(test_update(1))).unwrap();
            let mut state = SubscriptionState::new("TEST".into(), Vec::new(), tx, 1, all_fields);
            assert_eq!(deliver(&mut state, &event), MessageOutcome::DataLoss);
            assert_eq!(rx.try_recv().unwrap().unwrap().topic_id, 1);
            assert!(matches!(
                rx.try_recv().unwrap(), Err(BlpError::SubscriptionDataLoss { topic, .. }) if topic == "TEST"
            ));
            assert_eq!(deliver(&mut state, &event), MessageOutcome::Closed);
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ));
        }
    }

    fn test_update(topic_id: TopicId) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us: topic_id as i64,
            topic_id,
            topic: Arc::from("TEST"),
            layout: Arc::new(FieldLayout::new(1, Vec::new())),
            values: SmallVec::new(),
        }
    }

    fn test_metrics() -> Arc<SubscriptionMetrics> {
        Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        })
    }

    #[test]
    fn block_ingress_overflow_reports_a_gap_without_waiting_for_the_forwarder() {
        let (consumer_tx, mut consumer_rx) = subscription_channel(1);
        let (forwarder, _task) = subscription_forwarder_channel(1);
        forwarder
            .try_forward(
                consumer_tx.clone(),
                test_update(1),
                test_metrics(),
                Arc::from("TEST"),
            )
            .expect("fill forwarding queue");
        let mut state = SubscriptionState::with_policy_and_forwarder(
            "TEST".to_string(),
            Vec::new(),
            consumer_tx,
            1,
            OverflowPolicy::Block,
            false,
            Some(forwarder),
        );

        state.send_update(test_update(2));

        assert!(matches!(
            consumer_rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        assert_eq!(state.metrics.dropped_batches.load(Ordering::Relaxed), 1);
        assert!(state.metrics.slow_consumer.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn block_forwarder_preserves_enqueue_order() {
        let (consumer_tx, mut consumer_rx) = subscription_channel(4);
        let (forwarder, task) = subscription_forwarder_channel(4);
        let handle = tokio::spawn(task);
        let metrics = test_metrics();
        for topic_id in [10, 11, 12] {
            forwarder
                .try_forward(
                    consumer_tx.clone(),
                    test_update(topic_id),
                    Arc::clone(&metrics),
                    Arc::from("TEST"),
                )
                .expect("enqueue update");
        }
        forwarder.drain().await.unwrap();
        for expected in [10, 11, 12] {
            let update = consumer_rx
                .try_recv()
                .expect("forwarded item")
                .expect("successful update");
            assert_eq!(update.topic_id, expected);
        }
        drop(forwarder);
        handle.await.expect("forwarder task");
        assert_eq!(metrics.batches_sent.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drop_newest_overflow_ends_the_stream_after_buffered_data() {
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::with_policy(
            "TEST".into(),
            Vec::new(),
            tx,
            1,
            OverflowPolicy::DropNewest,
            false,
        );
        state.send_update(test_update(1));
        state.send_update(test_update(2));
        state.send_update(test_update(3));
        assert_eq!(rx.try_recv().unwrap().unwrap().topic_id, 1);
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn block_forwarding_timeout_reports_a_gap_and_completes_its_barrier() {
        let (tx, mut rx) = subscription_channel(1);
        tx.try_send(Ok(test_update(1))).unwrap();
        let (forwarder, task) = subscription_forwarder_channel(1);
        let handle = tokio::spawn(task);
        forwarder
            .try_forward(tx, test_update(2), test_metrics(), Arc::from("TEST"))
            .unwrap();
        forwarder.drain().await.unwrap();
        assert_eq!(rx.try_recv().unwrap().unwrap().topic_id, 1);
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        drop(forwarder);
        handle.await.unwrap();
    }
}
