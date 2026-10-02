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

/// Handles from one sparse message, sorted by requested index. `None` is projected metadata.
type RequestedFieldSelection<'a> = SmallVec<[(FieldIndex, Option<xbbg_core::Element<'a>>); 8]>;

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
    /// Explicit requests and projected metadata, excluding all-fields discoveries.
    requested_fields: Vec<bool>,
    /// Per-position allFields cache for stable Bloomberg subscription schemas.
    all_field_slots: Vec<Option<AllFieldSlot>>,
    field_kinds: Vec<FieldKind>,
    provisional_kinds: Vec<bool>,
    layout_version: u32,
    layout: Arc<FieldLayout>,
    /// Cached source-index to consumer-index projection, invalidated by field growth.
    projection_source: Option<Arc<FieldLayout>>,
    projection_version: u32,
    projection_indices: Vec<Option<FieldIndex>>,
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
    /// Only mktdata has last-value semantics and suppresses filtered metadata-only rows.
    mktdata_service: bool,
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
        let provisional_kinds = vec![false; field_strings.len()];
        let layout = Self::build_layout(1, &field_strings, &field_kinds, &provisional_kinds);
        let string_value_cache = vec![None; field_strings.len()];
        let requested_fields = vec![true; field_names.len()];
        let mktdata_service = !topic.starts_with("//")
            || topic == "//blp/mktdata"
            || topic.starts_with("//blp/mktdata/");

        Self {
            topic: Arc::from(topic),
            topic_id: 0,
            field_strings,
            field_names,
            field_name_keys,
            requested_fields,
            all_field_slots: Vec::new(),
            field_kinds,
            provisional_kinds,
            layout_version: 1,
            layout,
            projection_source: None,
            projection_version: 0,
            projection_indices: Vec::new(),
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
            mktdata_service,
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
    /// Replace the consumer-facing topic without changing service-specific projections.
    pub(crate) fn set_label(&mut self, label: Arc<str>) {
        self.topic = label;
    }

    /// Set the service for topics that do not include a fully qualified service prefix.
    pub(crate) fn set_service(&mut self, service: &str) {
        self.mktdata_service = service.trim() == "//blp/mktdata";
    }

    pub(crate) fn enable_all_fields(&mut self) {
        self.capture_all_fields = true;
        self.projection_source = None;
    }

    /// Append explicit fields without changing existing field indices or observed kinds.
    pub(crate) fn add_fields(&mut self, fields: &[String]) {
        for field in fields {
            let name = Name::get_or_intern(field);
            let idx = match self.field_name_keys.get(&(name.as_ptr() as usize)) {
                Some(&idx) => idx,
                None => {
                    let idx = self.append_field(Arc::from(field.as_str()), name);
                    self.projection_source = None;
                    idx
                }
            };
            self.requested_fields[idx as usize] = true;
        }
        self.refresh_layout();
    }

    /// Seed unresolved fields from metadata without replacing observed kinds.
    ///
    /// Metadata never adds fields or changes their indices. The first observed
    /// kind replaces its provisional hint; later observations merge normally.
    pub(crate) fn seed_kinds(&mut self, kinds: &HashMap<String, FieldKind>) {
        for idx in 0..self.field_strings.len() {
            if self.field_kinds[idx] == FieldKind::Unknown {
                if let Some(&kind) = kinds.get(self.field_strings[idx].as_ref()) {
                    self.seed_kind(idx as FieldIndex, kind);
                }
            }
        }
        self.refresh_layout();
    }

    /// Current immutable layout, including scalar discoveries and observed types.
    pub(crate) fn layout(&self) -> Arc<FieldLayout> {
        Arc::clone(&self.layout)
    }

    /// Project one decoded feed delta and deliver it through this consumer's queue.
    ///
    /// Filtered mktdata callers suppress rows without requested data, but still
    /// observe message metrics and terminal DATALOSS metadata. Other services pass
    /// `false` to retain their original event semantics.
    pub(crate) fn project_update(
        &mut self,
        source: &SubscriptionUpdate,
        suppress_empty: bool,
    ) -> MessageOutcome {
        if self.stream.is_closed() {
            return MessageOutcome::Closed;
        }
        let update = self.project_image(source);
        if self.is_dataloss_update(&update.values) {
            self.on_dataloss(Some(source.timestamp_us));
            return MessageOutcome::DataLoss;
        }
        self.deliver_update(update, suppress_empty)
    }

    /// Observe an image-only consumer's delta without allocating projected values
    /// or using queue capacity. The feed sink handles DATALOSS before this call.
    pub(crate) fn observe_update(&mut self, source: &SubscriptionUpdate) -> MessageOutcome {
        if self.stream.is_closed() {
            return MessageOutcome::Closed;
        }
        self.visit_projected_fields(source, |_, _| {});
        let first_message = self.record_received(source.timestamp_us);
        MessageOutcome::Normal { first_message }
    }

    /// Project known image entries without emitting or changing delivery metrics.
    ///
    /// Missing entries remain absent and explicit clears remain present nulls.
    /// Source names and string values are shared; stable layouts reuse the index
    /// mapping and an inline sparse delta allocates nothing.
    pub(crate) fn project_image(&mut self, source: &SubscriptionUpdate) -> SubscriptionUpdate {
        let mut values: SmallVec<[UpdateField; 8]> = SmallVec::new();
        self.visit_projected_fields(source, |index, value| {
            values.push(UpdateField {
                index,
                value: value.clone(),
            });
        });
        if !self.capture_all_fields {
            // The shared decoder visits schema order; filtered consumers retain
            // the requested-field order used by their standalone decoder.
            values.sort_unstable_by_key(|field| field.index);
        }
        SubscriptionUpdate {
            timestamp_us: source.timestamp_us,
            topic_id: self.topic_id,
            topic: Arc::clone(&self.topic),
            layout: Arc::clone(&self.layout),
            values,
        }
    }

    fn visit_projected_fields(
        &mut self,
        source: &SubscriptionUpdate,
        mut visit: impl FnMut(FieldIndex, &UpdateValue),
    ) {
        self.cache_projection(&source.layout);
        for field in &source.values {
            let source_idx = field.index as usize;
            let Some(meta) = source.layout.fields.get(source_idx) else {
                continue;
            };
            let idx = match self.projection_indices[source_idx] {
                Some(idx) => idx,
                None if self.capture_all_fields => {
                    let name = Name::get_or_intern(&meta.name);
                    let idx = self.append_field(Arc::clone(&meta.name), name);
                    self.projection_indices[source_idx] = Some(idx);
                    idx
                }
                None => continue,
            };
            if meta.provisional {
                self.seed_kind(idx, meta.kind);
            } else {
                self.observe_field_kind(idx, meta.kind);
            }
            self.observe_kind(idx, &field.value);
            visit(idx, &field.value);
        }
        self.refresh_layout();
    }

    fn cache_projection(&mut self, source: &Arc<FieldLayout>) {
        if self.projection_version == source.version
            && self
                .projection_source
                .as_ref()
                .is_some_and(|cached| Arc::ptr_eq(cached, source))
        {
            return;
        }
        self.projection_indices.clear();
        self.projection_indices
            .extend(source.fields.iter().map(|meta| {
                self.field_strings
                    .iter()
                    .position(|name| name == &meta.name)
                    .map(|idx| idx as FieldIndex)
            }));
        self.projection_source = Some(Arc::clone(source));
        self.projection_version = source.version;
    }

    fn refresh_layout(&mut self) {
        if self.layout.version != self.layout_version {
            self.layout = Self::build_layout(
                self.layout_version,
                &self.field_strings,
                &self.field_kinds,
                &self.provisional_kinds,
            );
        }
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

    fn build_layout(
        version: u32,
        names: &[Arc<str>],
        kinds: &[FieldKind],
        provisional: &[bool],
    ) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(
            version,
            names
                .iter()
                .zip(kinds.iter())
                .enumerate()
                .map(|(idx, (name, kind))| {
                    let mut field = FieldMeta::new(name.clone(), idx as FieldIndex, *kind);
                    field.provisional = provisional[idx];
                    field
                })
                .collect(),
        ))
    }

    /// Process a SUBSCRIPTION_DATA message using Element API.
    ///
    /// Timestamps use Bloomberg SDK receive time when available (requires
    /// `setRecordSubscriptionDataReceiveTimes(true)`), falling back to
    /// `SystemTime::now()` if not enabled.
    /// Filtered mktdata metadata-only rows are intentionally not emitted.
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
        // Keep every historical version increment, but materialize only this
        // message's final schema, including partial progress before an error.
        self.refresh_layout();
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

        let update = SubscriptionUpdate {
            timestamp_us: timestamp,
            topic_id: self.topic_id,
            topic: self.topic.clone(),
            layout: self.layout.clone(),
            values,
        };
        self.deliver_update(update, self.mktdata_service && !self.stream.is_callback())
    }

    fn deliver_update(
        &mut self,
        update: SubscriptionUpdate,
        suppress_empty: bool,
    ) -> MessageOutcome {
        let first_message = self.record_received(update.timestamp_us);
        if suppress_empty
            && !self.capture_all_fields
            && !update.values.iter().any(|field| {
                field.index != self.event_type_index && field.index != self.event_subtype_index
            })
        {
            return MessageOutcome::Normal { first_message };
        }
        if self.send_update(update) {
            MessageOutcome::Normal { first_message }
        } else {
            MessageOutcome::Closed
        }
    }

    fn record_received(&mut self, timestamp_us: i64) -> bool {
        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_message_us
            .store(timestamp_us as u64, Ordering::Relaxed);
        let first_message = !self.has_received_data;
        self.has_received_data = true;
        first_message
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
        let selected = self.select_present_requested_fields(elem, subscription_data.is_some());
        let field_count = selected
            .as_ref()
            .map_or(self.field_names.len(), |fields| fields.len());
        let mut values = SmallVec::new();
        for position in 0..field_count {
            let (idx, selected_field) = selected.as_ref().map_or((position, None), |fields| {
                let (idx, field) = &fields[position];
                (usize::from(*idx), field.as_ref())
            });
            let value = if Some(idx as FieldIndex) == self.subscription_data_index {
                let Some(value) = subscription_data else {
                    continue;
                };
                UpdateValue::Str(Arc::clone(value))
            } else {
                let named_field;
                let field = match selected_field {
                    Some(field) => field,
                    None => {
                        let Some(field) = elem.get(&self.field_names[idx]) else {
                            // Missing is not a clear: leave it out of this delta.
                            continue;
                        };
                        named_field = field;
                        &named_field
                    }
                };
                let datatype = field.datatype();
                if field.is_array() || !Self::should_capture_datatype(datatype) {
                    let error = Self::unsupported_shape(field);
                    if self.stream.report_field_error(&error, false) {
                        continue;
                    }
                    return Err(error);
                }
                match self.update_value_for_field(idx, field, datatype) {
                    Ok(value) => value,
                    Err(error) if self.stream.report_field_error(&error, true) => continue,
                    Err(error) => return Err(error),
                }
            };
            self.observe_kind(idx as FieldIndex, &value);
            values.push(UpdateField {
                index: idx as FieldIndex,
                value,
            });
        }
        Ok(values)
    }

    fn select_present_requested_fields<'a>(
        &self,
        elem: &xbbg_core::Element<'a>,
        has_subscription_data: bool,
    ) -> Option<RequestedFieldSelection<'a>> {
        // Keep narrow and dense subscriptions on the existing name-lookup path.
        // The scan is bounded by inline capacity, including projected metadata.
        if self.field_names.len() <= 8 || self.field_names.len() > usize::from(FieldIndex::MAX) + 1
        {
            return None;
        }
        let projected = self
            .subscription_data_index
            .filter(|_| has_subscription_data);
        let children = elem.num_children();
        if children > 8 - usize::from(projected.is_some()) || children * 2 > self.field_names.len()
        {
            return None;
        }
        let mut selected = RequestedFieldSelection::new();
        if let Some(idx) = projected {
            selected.push((idx, None));
        }
        for child_idx in 0..children {
            // Fall back before changing state if indexed access is unavailable.
            let child = elem.get_at(child_idx)?;
            let Some(&idx) = self.field_name_keys.get(&child.name_key()) else {
                continue;
            };
            if Some(idx) != self.subscription_data_index {
                selected.push((idx, Some(child)));
            }
        }
        // Decode in requested order, not schema order: error precedence and
        // field-kind/version observation must match the name-lookup path.
        selected.sort_unstable_by_key(|(idx, _)| *idx);
        Some(selected)
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
                    let error = Self::unsupported_shape(&child);
                    if !self.stream.report_field_error(&error, false) {
                        return Err(error);
                    }
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
                        let value =
                            match self.update_value_for_field(idx as usize, &child, datatype) {
                                Ok(value) => value,
                                Err(error) if self.stream.report_field_error(&error, true) => {
                                    continue
                                }
                                Err(error) => return Err(error),
                            };
                        self.observe_kind(idx, &value);
                        values.push(UpdateField { index: idx, value });
                        continue;
                    }
                    AllFieldSlot::Skipped {
                        key: cached_key,
                        datatype: cached_datatype,
                    } if cached_key == key && cached_datatype == datatype => {
                        if self.is_requested_field(key) {
                            let error = Self::unsupported_shape(&child);
                            if !self.stream.report_field_error(&error, false) {
                                return Err(error);
                            }
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            if !Self::should_capture_datatype(datatype) {
                if self.is_requested_field(key) {
                    let error = Self::unsupported_shape(&child);
                    if !self.stream.report_field_error(&error, false) {
                        return Err(error);
                    }
                }
                self.cache_all_field_slot(child_idx, AllFieldSlot::Skipped { key, datatype });
                continue;
            }

            let idx = self.ensure_field_for_child(&child, key);
            self.cache_all_field_slot(child_idx, AllFieldSlot::Captured { key, idx, datatype });
            let value = match self.update_value_for_field(idx as usize, &child, datatype) {
                Ok(value) => value,
                Err(error) if self.stream.report_field_error(&error, true) => continue,
                Err(error) => return Err(error),
            };
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

    fn seed_kind(&mut self, idx: FieldIndex, kind: FieldKind) {
        let idx = idx as usize;
        if self.field_kinds[idx] == FieldKind::Unknown && kind != FieldKind::Unknown {
            self.field_kinds[idx] = kind;
            self.provisional_kinds[idx] = true;
            self.layout_version = self.layout_version.wrapping_add(1).max(1);
        }
    }

    fn observe_field_kind(&mut self, idx: FieldIndex, observed: FieldKind) {
        if observed == FieldKind::Unknown {
            return;
        }
        let idx = idx as usize;
        let provisional = self.provisional_kinds[idx];
        let merged = if provisional {
            observed
        } else {
            self.field_kinds[idx].merge_observed(observed)
        };
        if merged != self.field_kinds[idx] || provisional {
            self.field_kinds[idx] = merged;
            self.provisional_kinds[idx] = false;
            self.layout_version = self.layout_version.wrapping_add(1).max(1);
        }
    }

    fn is_requested_field(&self, key: usize) -> bool {
        self.field_name_keys
            .get(&key)
            .is_some_and(|idx| self.requested_fields[*idx as usize])
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
        let name = Name::get_or_intern(&field_name);
        self.projection_source = None;
        self.append_field(field_name, name)
    }

    fn append_field(&mut self, field_name: Arc<str>, name: Name) -> FieldIndex {
        let idx = self.field_strings.len() as FieldIndex;
        self.field_strings.push(field_name);
        self.field_name_keys.insert(name.as_ptr() as usize, idx);
        self.field_names.push(name);
        self.requested_fields.push(false);
        self.field_kinds.push(FieldKind::Unknown);
        self.provisional_kinds.push(false);
        self.string_value_cache.push(None);
        self.layout_version = self.layout_version.wrapping_add(1).max(1);
        idx
    }

    /// Record DATALOSS for a recovering consumer without failing its stream.
    pub(crate) fn record_dataloss(&mut self, timestamp_us: Option<i64>) {
        self.slow_consumer = true;
        self.metrics.slow_consumer.store(true, Ordering::Relaxed);
        self.metrics
            .data_loss_events
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.last_data_loss_us.store(
            timestamp_us.unwrap_or_default().max(0) as u64,
            Ordering::Relaxed,
        );
    }

    /// Handle DATALOSS, failing streams unless their callback sink owns recovery.
    pub fn on_dataloss(&mut self, timestamp_us: Option<i64>) {
        self.record_dataloss(timestamp_us);
        self.stream.fail(BlpError::SubscriptionDataLoss {
            topic: self.topic.to_string(),
            detail: "Bloomberg reported DATALOSS; resubscribe for a fresh image".into(),
        });
        xbbg_log::warn!(topic = %self.topic, "DATALOSS detected");
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

    fn decoded_layout(version: u32, fields: &[(&str, FieldKind)]) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(
            version,
            fields
                .iter()
                .enumerate()
                .map(|(idx, (name, kind))| FieldMeta::new(*name, idx as FieldIndex, *kind))
                .collect(),
        ))
    }

    fn decoded_update(
        layout: Arc<FieldLayout>,
        values: impl IntoIterator<Item = (FieldIndex, UpdateValue)>,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us: 42,
            topic_id: 100,
            topic: Arc::from("TEST"),
            layout,
            values: values
                .into_iter()
                .map(|(index, value)| UpdateField { index, value })
                .collect(),
        }
    }

    fn value_names(update: &SubscriptionUpdate) -> Vec<&str> {
        update
            .values
            .iter()
            .map(|field| update.layout.fields[field.index as usize].name.as_ref())
            .collect()
    }

    #[test]
    fn metadata_seeding_preserves_observed_kinds_promotions_and_field_order() {
        let (tx, _rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "TEST".into(),
            ["OBSERVED", "COUNT", "TIME", "UNRESOLVED"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            tx,
            1,
            false,
        );
        state.project_image(&decoded_update(
            decoded_layout(1, &[("OBSERVED", FieldKind::F64)]),
            [(0, UpdateValue::F64(1.5))],
        ));
        let unseeded = state.layout();
        let kinds = HashMap::from([
            ("OBSERVED".into(), FieldKind::I32),
            ("COUNT".into(), FieldKind::I32),
            ("TIME".into(), FieldKind::Time64Micros),
            ("UNRESOLVED".into(), FieldKind::Unknown),
            ("DATE".into(), FieldKind::Date32),
        ]);
        state.seed_kinds(&kinds);
        let seeded = state.layout();
        assert_eq!(
            seeded
                .fields
                .iter()
                .map(|field| (field.name.as_ref(), field.kind))
                .collect::<Vec<_>>(),
            vec![
                ("OBSERVED", FieldKind::F64),
                ("COUNT", FieldKind::I32),
                ("TIME", FieldKind::Time64Micros),
                ("UNRESOLVED", FieldKind::Unknown),
                ("MKTDATA_EVENT_TYPE", FieldKind::Unknown),
                ("MKTDATA_EVENT_SUBTYPE", FieldKind::Unknown),
            ]
        );
        assert_eq!(unseeded.fields[1].kind, FieldKind::Unknown);
        assert_eq!(unseeded.fields[2].kind, FieldKind::Unknown);
        state.seed_kinds(&kinds);
        assert!(Arc::ptr_eq(&seeded, &state.layout()));

        let promoted = state.project_image(&decoded_update(
            decoded_layout(2, &[("COUNT", FieldKind::I64)]),
            [(0, UpdateValue::I64(1_234_567_890_123))],
        ));
        assert_eq!(promoted.layout.fields[1].kind, FieldKind::I64);
        let changed = state.project_image(&decoded_update(
            decoded_layout(3, &[("COUNT", FieldKind::I32)]),
            [(0, UpdateValue::I32(7))],
        ));
        assert_eq!(changed.layout.fields[1].kind, FieldKind::Str);
        state.add_fields(&["DATE".into(), "TIME".into()]);
        state.seed_kinds(&kinds);
        let grown = state.layout();
        assert_eq!(
            grown
                .fields
                .iter()
                .map(|field| (field.name.as_ref(), field.index, field.kind))
                .collect::<Vec<_>>(),
            vec![
                ("OBSERVED", 0, FieldKind::F64),
                ("COUNT", 1, FieldKind::Str),
                ("TIME", 2, FieldKind::Time64Micros),
                ("UNRESOLVED", 3, FieldKind::Unknown),
                ("MKTDATA_EVENT_TYPE", 4, FieldKind::Unknown),
                ("MKTDATA_EVENT_SUBTYPE", 5, FieldKind::Unknown),
                ("DATE", 6, FieldKind::Date32),
            ]
        );
        assert_eq!(seeded.fields[1].kind, FieldKind::I32);
        assert_eq!(seeded.fields.len(), 6);
    }

    #[test]
    fn seeded_decoder_preserves_temporal_kinds_on_untyped_datetime_clears() {
        let event = fixture(
            r#"<element name="VALUE" type="Datetime" minOccurs="0"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"VALUE":null}"#),
        );
        for kind in [
            FieldKind::Date32,
            FieldKind::Time64Micros,
            FieldKind::TimestampMicros,
        ] {
            let (tx, mut rx) = subscription_channel(1);
            let mut state =
                SubscriptionState::new("TEST".into(), vec!["VALUE".into()], tx, 1, false);
            state.seed_kinds(&HashMap::from([("VALUE".into(), kind)]));
            let seeded = state.layout();
            assert_eq!(seeded.fields[0].kind, kind);
            assert_eq!(
                deliver(&mut state, &event),
                MessageOutcome::Normal {
                    first_message: true
                }
            );
            let update = rx.try_recv().unwrap().unwrap();
            assert_eq!(value_names(&update), vec!["VALUE"]);
            assert!(matches!(update.values[0].value, UpdateValue::Null));
            assert_eq!(update.layout.fields[0].kind, kind);
            assert!(Arc::ptr_eq(&seeded, &update.layout));
        }
    }

    #[test]
    fn image_only_observation_never_uses_queue_capacity_while_layouts_grow() {
        for all_fields in [false, true] {
            for policy in [OverflowPolicy::Block, OverflowPolicy::DropNewest] {
                let (tx, mut rx) = subscription_channel(1);
                let mut state = SubscriptionState::with_policy(
                    "TEST".into(),
                    vec!["FIELD_00".into(), "FIELD_19".into()],
                    tx,
                    1,
                    policy,
                    all_fields,
                );
                let mut received = 0_u64;
                for width in [4_u16, 12, 20] {
                    let mut fields: Vec<_> = (0..width)
                        .map(|index| {
                            FieldMeta::new(format!("FIELD_{index:02}"), index, FieldKind::F64)
                        })
                        .collect();
                    fields.push(FieldMeta::new("ABSENT", width, FieldKind::F64));
                    let mut source = decoded_update(
                        Arc::new(FieldLayout::new(u32::from(width), fields)),
                        (0..width).map(|index| {
                            let value = if index == 19 {
                                UpdateValue::Null
                            } else {
                                UpdateValue::F64(f64::from(index))
                            };
                            (index, value)
                        }),
                    );
                    for _ in 0..1024 {
                        let first_message = received == 0;
                        received += 1;
                        source.timestamp_us = received as i64;
                        assert_eq!(
                            state.observe_update(&source),
                            MessageOutcome::Normal { first_message }
                        );
                    }
                }

                let layout = state.layout();
                let mut expected_names = vec![
                    "FIELD_00".to_owned(),
                    "FIELD_19".to_owned(),
                    "MKTDATA_EVENT_TYPE".to_owned(),
                    "MKTDATA_EVENT_SUBTYPE".to_owned(),
                ];
                if all_fields {
                    expected_names.extend((1..19).map(|index| format!("FIELD_{index:02}")));
                }
                assert_eq!(
                    layout
                        .fields
                        .iter()
                        .map(|field| field.name.to_string())
                        .collect::<Vec<_>>(),
                    expected_names
                );
                assert_eq!(layout.fields[0].kind, FieldKind::F64);
                assert_eq!(layout.fields[1].kind, FieldKind::F64);
                assert_eq!(
                    state.metrics.messages_received.load(Ordering::Relaxed),
                    3072
                );
                assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 3072);
                assert_eq!(state.metrics.batches_sent.load(Ordering::Relaxed), 0);
                assert_eq!(state.metrics.dropped_batches.load(Ordering::Relaxed), 0);
                assert_eq!(state.metrics.data_loss_events.load(Ordering::Relaxed), 0);
                assert_eq!(state.dropped_batches, 0);
                assert!(!state.metrics.slow_consumer.load(Ordering::Relaxed));
                assert!(!state.stream.is_closed());
                assert!(matches!(
                    rx.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            }
        }
    }

    #[test]
    fn image_only_observation_remaps_added_fields_and_reordered_typed_clears() {
        let source = decoded_update(
            decoded_layout(
                1,
                &[
                    ("UNREQUESTED", FieldKind::Str),
                    ("TIME", FieldKind::Time64Micros),
                    ("DATE", FieldKind::Date32),
                    ("STAMP", FieldKind::TimestampMicros),
                ],
            ),
            [
                (0, UpdateValue::Str(Arc::from("ignored"))),
                (1, UpdateValue::Null),
                (2, UpdateValue::Null),
                (3, UpdateValue::Null),
            ],
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), vec!["TIME".into()], tx, 1, false);
        let unrequested = decoded_update(
            Arc::clone(&source.layout),
            [(0, UpdateValue::Str(Arc::from("ignored")))],
        );
        assert_eq!(
            state.observe_update(&unrequested),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        assert_eq!(state.layout().fields[0].kind, FieldKind::Unknown);
        assert_eq!(
            state.observe_update(&source),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let initial = state.layout();
        state.add_fields(&["STAMP".into(), "DATE".into()]);
        assert_eq!(
            state.observe_update(&source),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let reordered = decoded_update(
            decoded_layout(
                1,
                &[
                    ("STAMP", FieldKind::TimestampMicros),
                    ("DATE", FieldKind::Date32),
                    ("TIME", FieldKind::Time64Micros),
                ],
            ),
            [
                (0, UpdateValue::Null),
                (1, UpdateValue::Null),
                (2, UpdateValue::Null),
            ],
        );
        assert_eq!(
            state.observe_update(&reordered),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let layout = state.layout();
        assert_eq!(
            layout
                .fields
                .iter()
                .map(|field| (field.name.as_ref(), field.kind))
                .collect::<Vec<_>>(),
            vec![
                ("TIME", FieldKind::Time64Micros),
                ("MKTDATA_EVENT_TYPE", FieldKind::Unknown),
                ("MKTDATA_EVENT_SUBTYPE", FieldKind::Unknown),
                ("STAMP", FieldKind::TimestampMicros),
                ("DATE", FieldKind::Date32),
            ]
        );
        assert_eq!(initial.fields.len(), 3);
        assert_eq!(initial.fields[0].kind, FieldKind::Time64Micros);
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 4);
        assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 42);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn recorded_dataloss_keeps_observation_open_until_an_explicit_failure() {
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), vec!["BID".into()], tx, 1, false);
        for (index, (timestamp, expected)) in [(Some(17), 17), (None, 0), (Some(-1), 0)]
            .into_iter()
            .enumerate()
        {
            state.record_dataloss(timestamp);
            assert!(state.slow_consumer);
            assert!(state.metrics.slow_consumer.load(Ordering::Relaxed));
            assert_eq!(
                state.metrics.data_loss_events.load(Ordering::Relaxed),
                index as u64 + 1
            );
            assert_eq!(
                state.metrics.last_data_loss_us.load(Ordering::Relaxed),
                expected
            );
            assert!(!state.stream.is_closed());
        }
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 0);
        assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 0);
        let source = decoded_update(
            decoded_layout(1, &[("BID", FieldKind::F64)]),
            [(0, UpdateValue::F64(10.0))],
        );
        assert_eq!(
            state.observe_update(&source),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 1);
        assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 42);
        assert!(state.slow_consumer);
        state.clear_slow_consumer();
        assert!(!state.slow_consumer);
        assert!(!state.metrics.slow_consumer.load(Ordering::Relaxed));
        assert_eq!(state.metrics.data_loss_events.load(Ordering::Relaxed), 3);
        assert_eq!(state.metrics.batches_sent.load(Ordering::Relaxed), 0);
        assert_eq!(state.metrics.dropped_batches.load(Ordering::Relaxed), 0);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        state.on_dataloss(Some(99));
        assert_eq!(state.observe_update(&source), MessageOutcome::Closed);
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 1);
        assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 42);
        assert_eq!(state.metrics.data_loss_events.load(Ordering::Relaxed), 4);
        assert_eq!(state.metrics.last_data_loss_us.load(Ordering::Relaxed), 99);
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { topic, .. }) if topic == "TEST"
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn closed_image_only_consumers_do_not_observe_new_kinds_or_messages() {
        for terminal_error in [false, true] {
            let (tx, mut rx) = subscription_channel(1);
            let mut state = SubscriptionState::new("TEST".into(), vec!["BID".into()], tx, 1, true);
            let layout = state.layout();
            if terminal_error {
                state.fail(BlpError::Timeout);
            } else {
                rx.close();
            }
            let source = decoded_update(
                decoded_layout(1, &[("BID", FieldKind::F64), ("ASK", FieldKind::F64)]),
                [(0, UpdateValue::F64(10.0)), (1, UpdateValue::F64(12.0))],
            );
            assert_eq!(state.observe_update(&source), MessageOutcome::Closed);
            assert!(Arc::ptr_eq(&layout, &state.layout()));
            assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 0);
            assert_eq!(state.metrics.last_message_us.load(Ordering::Relaxed), 0);
            if terminal_error {
                assert!(matches!(rx.try_recv().unwrap(), Err(BlpError::Timeout)));
            }
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ));
        }
    }

    #[test]
    fn filtered_mktdata_suppresses_metadata_and_unrequested_fields_but_not_clears() {
        let metadata = fixture(
            r#"<element name="MKTDATA_EVENT_TYPE" type="String"/>
            <element name="MKTDATA_EVENT_SUBTYPE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter
                    .json(r#"{"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}"#)
            },
        );
        let unrequested = fixture(
            r#"<element name="ASK" type="Float64"/>
            <element name="MKTDATA_EVENT_TYPE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"ASK":12.0,"MKTDATA_EVENT_TYPE":"QUOTE"}"#),
        );
        let clear = fixture(
            r#"<element name="BID" type="Float64" minOccurs="0"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"BID":null}"#),
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), vec!["BID".into()], tx, 1, false);

        assert_eq!(
            deliver(&mut state, &metadata),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        deliver(&mut state, &unrequested);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            deliver(&mut state, &clear),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let update = rx.try_recv().unwrap().unwrap();
        assert_eq!(value_names(&update), vec!["BID"]);
        assert!(matches!(update.values[0].value, UpdateValue::Null));
        assert_eq!(update.layout.fields[0].kind, FieldKind::F64);
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 3);
        assert_eq!(state.metrics.batches_sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn non_mktdata_and_all_fields_preserve_metadata_only_messages() {
        let event = fixture(
            r#"<element name="MKTDATA_EVENT_TYPE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"MKTDATA_EVENT_TYPE":"SUMMARY"}"#),
        );
        for (topic, service, all_fields) in [
            ("TEST", "//blp/mktvwap", false),
            ("//blp/mktdepth/ticker/TEST", "//blp/mktdepth", false),
            ("TEST", "//blp/mktdata", true),
        ] {
            let (tx, mut rx) = subscription_channel(1);
            let mut state =
                SubscriptionState::new(topic.into(), vec!["BID".into()], tx, 1, all_fields);
            state.set_service(service);
            state.set_label(Arc::from("LABEL"));
            deliver(&mut state, &event);
            let update = rx.try_recv().unwrap().unwrap();
            assert_eq!(update.topic.as_ref(), "LABEL");
            assert_eq!(value_names(&update), vec!["MKTDATA_EVENT_TYPE"]);

            state.project_update(&update, false);
            assert_eq!(
                value_names(&rx.try_recv().unwrap().unwrap()),
                vec!["MKTDATA_EVENT_TYPE"]
            );
        }
    }

    #[test]
    fn decoded_projection_keeps_consumer_fields_labels_and_delivery_independent() {
        let layout = decoded_layout(
            1,
            &[
                ("ASK", FieldKind::F64),
                ("BID", FieldKind::F64),
                ("MKTDATA_EVENT_TYPE", FieldKind::Str),
            ],
        );
        let (bid_tx, mut bid_rx) = subscription_channel(1);
        let (ask_tx, mut ask_rx) = subscription_channel(1);
        let mut bid = SubscriptionState::new("TEST".into(), vec!["BID".into()], bid_tx, 1, false);
        let mut ask = SubscriptionState::new("TEST".into(), vec!["ASK".into()], ask_tx, 1, false);
        bid.set_label(Arc::from("BID_LABEL"));
        ask.set_label(Arc::from("ASK_LABEL"));
        bid.set_topic_id(11);
        ask.set_topic_id(22);

        let bid_delta = decoded_update(
            Arc::clone(&layout),
            [
                (2, UpdateValue::Str(Arc::from("QUOTE"))),
                (1, UpdateValue::F64(10.0)),
            ],
        );
        bid.project_update(&bid_delta, true);
        ask.project_update(&bid_delta, true);
        let update = bid_rx.try_recv().unwrap().unwrap();
        assert_eq!(value_names(&update), vec!["BID", "MKTDATA_EVENT_TYPE"]);
        assert_eq!(update.topic.as_ref(), "BID_LABEL");
        assert_eq!(update.topic_id, 11);
        assert_eq!(update.timestamp_us, 42);
        assert!(matches!(update.values[0].value, UpdateValue::F64(10.0)));
        assert!(matches!(
            ask_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let ask_delta = decoded_update(layout, [(0, UpdateValue::F64(12.0))]);
        bid.project_update(&ask_delta, true);
        ask.project_update(&ask_delta, true);
        assert!(matches!(
            bid_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        let update = ask_rx.try_recv().unwrap().unwrap();
        assert_eq!(value_names(&update), vec!["ASK"]);
        assert_eq!(update.topic.as_ref(), "ASK_LABEL");
        assert_eq!(update.topic_id, 22);
        assert!(matches!(update.values[0].value, UpdateValue::F64(12.0)));
        assert_eq!(bid.metrics.batches_sent.load(Ordering::Relaxed), 1);
        assert_eq!(ask.metrics.batches_sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn projection_growth_remaps_source_layouts_and_preserves_typed_clears() {
        let layout = decoded_layout(1, &[("ASK", FieldKind::F64), ("BID", FieldKind::F64)]);
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), vec!["BID".into()], tx, 1, false);
        let initial = state.project_image(&decoded_update(
            Arc::clone(&layout),
            [(1, UpdateValue::F64(10.0))],
        ));
        assert_eq!(value_names(&initial), vec!["BID"]);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(state.metrics.messages_received.load(Ordering::Relaxed), 0);

        state.add_fields(&["ASK".into(), "BID".into(), "ASK".into()]);
        let grown = state.layout();
        assert_eq!(
            grown
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            vec!["BID", "MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE", "ASK"]
        );
        assert_eq!(initial.layout.fields.len(), 3);
        let clear = state.project_image(&decoded_update(layout, [(0, UpdateValue::Null)]));
        assert_eq!(value_names(&clear), vec!["ASK"]);
        assert_eq!(clear.values[0].index, 3);
        assert!(matches!(clear.values[0].value, UpdateValue::Null));
        assert_eq!(clear.layout.fields[3].kind, FieldKind::F64);
        assert_eq!(grown.fields[3].kind, FieldKind::Unknown);

        // A new source with the same version must not reuse the old index mapping.
        let reordered = decoded_layout(1, &[("BID", FieldKind::F64), ("ASK", FieldKind::F64)]);
        let projected = state.project_image(&decoded_update(
            reordered,
            [(1, UpdateValue::F64(21.0)), (0, UpdateValue::F64(19.0))],
        ));
        assert_eq!(value_names(&projected), vec!["BID", "ASK"]);
        assert!(matches!(projected.values[0].value, UpdateValue::F64(19.0)));
        assert!(matches!(projected.values[1].value, UpdateValue::F64(21.0)));

        let promoted = decoded_layout(2, &[("BID", FieldKind::F64), ("ASK", FieldKind::Str)]);
        let clear = state.project_image(&decoded_update(promoted, [(1, UpdateValue::Null)]));
        assert_eq!(clear.layout.fields[3].kind, FieldKind::Str);
        assert!(matches!(clear.values[0].value, UpdateValue::Null));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn all_fields_projection_discovers_present_scalars_and_reuses_shared_strings() {
        let layout = decoded_layout(1, &[("TEXT", FieldKind::Str), ("FUTURE", FieldKind::F64)]);
        let text = Arc::<str>::from("synthetic");
        let source = decoded_update(
            Arc::clone(&layout),
            [(0, UpdateValue::Str(Arc::clone(&text)))],
        );
        let (tx, _rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), Vec::new(), tx, 1, true);
        let first = state.project_image(&source);
        assert_eq!(value_names(&first), vec!["TEXT"]);
        assert!(!first
            .layout
            .fields
            .iter()
            .any(|field| field.name.as_ref() == "FUTURE"));
        assert!(Arc::ptr_eq(
            &first.layout.fields[2].name,
            &layout.fields[0].name
        ));
        assert!(
            matches!(&first.values[0].value, UpdateValue::Str(value) if Arc::ptr_eq(value, &text))
        );

        let repeated = state.project_image(&source);
        assert!(Arc::ptr_eq(&first.layout, &repeated.layout));
        assert!(!repeated.values.spilled());
        let clear = state.project_image(&decoded_update(layout, [(1, UpdateValue::Null)]));
        assert_eq!(value_names(&clear), vec!["FUTURE"]);
        assert_eq!(clear.layout.fields[3].kind, FieldKind::F64);
        assert!(matches!(clear.values[0].value, UpdateValue::Null));
        assert_eq!(first.layout.fields.len(), 3);
    }

    #[test]
    fn projected_dataloss_is_terminal_even_without_requested_data() {
        let layout = decoded_layout(
            1,
            &[
                ("MKTDATA_EVENT_TYPE", FieldKind::Str),
                ("MKTDATA_EVENT_SUBTYPE", FieldKind::Str),
            ],
        );
        let source = decoded_update(
            layout,
            [
                (0, UpdateValue::Str(Arc::from("SUMMARY"))),
                (1, UpdateValue::Str(Arc::from("DATALOSS"))),
            ],
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), vec!["BID".into()], tx, 1, false);
        assert_eq!(
            state.project_update(&source, true),
            MessageOutcome::DataLoss
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        assert_eq!(state.project_update(&source, true), MessageOutcome::Closed);
        assert_eq!(state.metrics.data_loss_events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn projected_overflow_closes_only_the_full_consumer_queue() {
        let source = decoded_update(
            decoded_layout(1, &[("BID", FieldKind::F64)]),
            [(0, UpdateValue::F64(10.0))],
        );
        let (slow_tx, mut slow_rx) = subscription_channel(1);
        let (fast_tx, mut fast_rx) = subscription_channel(2);
        let mut slow = SubscriptionState::new("TEST".into(), vec!["BID".into()], slow_tx, 1, false);
        let mut fast = SubscriptionState::new("TEST".into(), vec!["BID".into()], fast_tx, 1, false);
        slow.project_update(&source, true);
        fast.project_update(&source, true);
        assert_eq!(slow.project_update(&source, true), MessageOutcome::Closed);
        assert_eq!(
            fast.project_update(&source, true),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        assert!(matches!(
            slow_rx.try_recv().unwrap().unwrap().values[0].value,
            UpdateValue::F64(10.0)
        ));
        assert!(matches!(
            slow_rx.try_recv().unwrap(),
            Err(BlpError::SubscriptionDataLoss { .. })
        ));
        for _ in 0..2 {
            assert!(matches!(
                fast_rx.try_recv().unwrap().unwrap().values[0].value,
                UpdateValue::F64(10.0)
            ));
        }
        assert!(!fast.stream.is_closed());
        assert_eq!(fast.metrics.dropped_batches.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn mktbar_synthetic_message_kind_counts_as_data_after_label_projection() {
        let event = fixture("", "MarketBarEnd", |formatter| formatter.json("{}"));
        let (source_tx, mut source_rx) = subscription_channel(1);
        let mut decoder = SubscriptionState::new(
            "//blp/mktbar/ticker/TEST".into(),
            Vec::new(),
            source_tx,
            1,
            false,
        );
        deliver(&mut decoder, &event);
        let source = source_rx.try_recv().unwrap().unwrap();
        let (tx, mut rx) = subscription_channel(1);
        let mut consumer =
            SubscriptionState::new("//blp/mktbar/ticker/TEST".into(), Vec::new(), tx, 1, false);
        consumer.set_label(Arc::from("LABEL"));
        consumer.project_update(&source, true);
        let update = rx.try_recv().unwrap().unwrap();
        assert_eq!(update.topic.as_ref(), "LABEL");
        assert_eq!(value_names(&update), vec!["SUBSCRIPTION_DATA"]);
        assert!(
            matches!(&update.values[0].value, UpdateValue::Str(value) if value.as_ref() == "MarketBarEnd")
        );
    }

    #[test]
    fn add_fields_promotes_prior_scalar_discoveries_to_explicit_requests() {
        let scalar = fixture(
            r#"<element name="LEVELS" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":3.5}"#),
        );
        let array = fixture(
            r#"<element name="LEVELS" type="Float64" maxOccurs="unbounded"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LEVELS":[1.25,2.5]}"#),
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new("TEST".into(), Vec::new(), tx, 1, true);
        deliver(&mut state, &scalar);
        let update = rx.try_recv().unwrap().unwrap();
        assert_eq!(value_names(&update), vec!["LEVELS"]);
        state.add_fields(&["LEVELS".into(), "NEW_FIELD".into()]);
        assert_eq!(deliver(&mut state, &array), MessageOutcome::Closed);
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(BlpError::SchemaUnsupported { element, .. }) if element == "LEVELS"
        ));
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
    #[test]
    fn wide_sparse_reverse_schema_preserves_i64_null_absence_and_requested_order() {
        let requested = [
            "FIELD_00",
            "EXACT_I64",
            "FIELD_02",
            "EXPLICIT_NULL",
            "FIELD_04",
            "ABSENT_AFTER_IMAGE",
            "FIELD_06",
            "TEXT",
            "FIELD_08",
            "FIELD_09",
            "FIELD_10",
            "FIELD_11",
            "FIELD_12",
            "FIELD_13",
            "FIELD_14",
            "FIELD_15",
        ];
        let image = fixture(
            r#"<element name="TEXT" type="String"/>
        <element name="ABSENT_AFTER_IMAGE" type="Int32"/>
        <element name="EXPLICIT_NULL" type="Int32"/>
        <element name="EXACT_I64" type="Int64"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter.json(
                r#"{"TEXT":"initial","ABSENT_AFTER_IMAGE":77,"EXPLICIT_NULL":88,"EXACT_I64":-9007199254740993}"#,
            )
            },
        );
        let delta = fixture(
            r#"<element name="TEXT" type="String"/>
        <element name="EXPLICIT_NULL" type="Int32" minOccurs="0"/>
        <element name="EXACT_I64" type="Int64"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter
                    .json(r#"{"TEXT":"changed","EXPLICIT_NULL":null,"EXACT_I64":9007199254740993}"#)
            },
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "TEST".into(),
            requested.iter().map(|field| (*field).to_owned()).collect(),
            tx,
            5,
            false,
        );

        assert_eq!(
            deliver(&mut state, &image),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        let image_update = rx
            .try_recv()
            .expect("image delivered immediately")
            .expect("image update");
        let image_names: Vec<_> = image_update
            .values
            .iter()
            .map(|field| {
                image_update.layout.fields[field.index as usize]
                    .name
                    .as_ref()
            })
            .collect();
        assert_eq!(
            image_names,
            vec!["EXACT_I64", "EXPLICIT_NULL", "ABSENT_AFTER_IMAGE", "TEXT"]
        );
        assert!(matches!(
            image_update.values[0].value,
            UpdateValue::I64(-9_007_199_254_740_993)
        ));
        assert!(matches!(image_update.values[1].value, UpdateValue::I32(88)));
        assert!(matches!(image_update.values[2].value, UpdateValue::I32(77)));
        assert!(matches!(
            &image_update.values[3].value,
            UpdateValue::Str(value) if value.as_ref() == "initial"
        ));

        assert_eq!(
            deliver(&mut state, &delta),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let delta_update = rx
            .try_recv()
            .expect("delta delivered immediately")
            .expect("delta update");
        let delta_names: Vec<_> = delta_update
            .values
            .iter()
            .map(|field| {
                delta_update.layout.fields[field.index as usize]
                    .name
                    .as_ref()
            })
            .collect();
        assert_eq!(delta_names, vec!["EXACT_I64", "EXPLICIT_NULL", "TEXT"]);
        assert!(matches!(
            delta_update.values[0].value,
            UpdateValue::I64(9_007_199_254_740_993)
        ));
        assert!(matches!(delta_update.values[1].value, UpdateValue::Null));
        assert!(matches!(
            &delta_update.values[2].value,
            UpdateValue::Str(value) if value.as_ref() == "changed"
        ));
    }

    #[test]
    fn wide_sparse_reverse_schema_reports_first_requested_unsupported_field() {
        let requested = [
            "SUPPORTED",
            "FIRST_UNSUPPORTED",
            "FIELD_02",
            "FIELD_03",
            "FIELD_04",
            "FIELD_05",
            "FIELD_06",
            "FIELD_07",
            "FIELD_08",
            "FIELD_09",
            "FIELD_10",
            "FIELD_11",
            "FIELD_12",
            "FIELD_13",
            "SECOND_UNSUPPORTED",
            "FIELD_15",
        ];
        let event = fixture_with_types(
            r#"<element name="SECOND_UNSUPPORTED" type="SecondValue"/>
        <element name="FIRST_UNSUPPORTED" type="FirstValue"/>
        <element name="SUPPORTED" type="Int32"/>"#,
            r#"<sequenceType name="FirstValue">
            <element name="INNER" type="String"/>
        </sequenceType>
        <sequenceType name="SecondValue">
            <element name="INNER" type="String"/>
        </sequenceType>"#,
            "MarketDataEvents",
            |formatter| {
                formatter.json(
                r#"{"SECOND_UNSUPPORTED":{"INNER":"second"},"FIRST_UNSUPPORTED":{"INNER":"first"},"SUPPORTED":7}"#,
            )
            },
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "TEST".into(),
            requested.iter().map(|field| (*field).to_owned()).collect(),
            tx,
            6,
            false,
        );

        assert_eq!(deliver(&mut state, &event), MessageOutcome::Closed);
        assert!(matches!(
            rx.try_recv().expect("terminal schema error"),
            Err(BlpError::SchemaUnsupported { element, .. }) if element == "FIRST_UNSUPPORTED"
        ));
    }

    #[test]
    fn one_message_keeps_exact_layout_version_and_retained_layouts_immutable() {
        let requested = [
            "ANCHOR",
            "PROMOTED_I64",
            "FIELD_02",
            "PROMOTED_TEXT",
            "FIELD_04",
            "FIELD_05",
            "FIELD_06",
            "FIELD_07",
            "FIELD_08",
            "FIELD_09",
            "FIELD_10",
            "FIELD_11",
            "FIELD_12",
            "FIELD_13",
            "FIELD_14",
            "FIELD_15",
        ];
        let first_event = fixture(
            r#"<element name="ANCHOR" type="Float64"/>
        <element name="PROMOTED_I64" type="Int32"/>
        <element name="PROMOTED_TEXT" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"ANCHOR":1.25,"PROMOTED_I64":7,"PROMOTED_TEXT":8.5}"#),
        );
        let multi_change = fixture(
            r#"<element name="ANCHOR" type="Float64"/>
        <element name="PROMOTED_I64" type="Int64"/>
        <element name="PROMOTED_TEXT" type="String"/>
        <element name="DISCOVERED_I32" type="Int32"/>
        <element name="DISCOVERED_F64" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter.json(
                r#"{"ANCHOR":2.5,"PROMOTED_I64":1234567890123,"PROMOTED_TEXT":"ready","DISCOVERED_I32":7,"DISCOVERED_F64":4.5}"#,
            )
            },
        );
        let initial_names: Vec<_> = requested
            .iter()
            .copied()
            .chain(["MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE"])
            .collect();
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "TEST".into(),
            requested.iter().map(|field| (*field).to_owned()).collect(),
            tx,
            7,
            true,
        );

        assert_eq!(
            deliver(&mut state, &first_event),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        let first = rx
            .try_recv()
            .expect("first update delivered immediately")
            .expect("first update");
        let first_version = first.layout.version;
        assert_eq!(first.values.len(), 3);
        assert!(matches!(first.values[0].value, UpdateValue::F64(1.25)));
        let retained_layout = first.layout.clone();

        assert_eq!(
            deliver(&mut state, &multi_change),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let second = rx
            .try_recv()
            .expect("multi-change update delivered immediately")
            .expect("multi-change update");
        // Two kind promotions, plus discovery and kind observation for each new field.
        assert_eq!(second.layout.version, first_version + 6);
        let mut expected_names = initial_names.clone();
        expected_names.extend(["DISCOVERED_I32", "DISCOVERED_F64"]);
        let second_names: Vec<_> = second
            .layout
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect();
        assert_eq!(second_names, expected_names);
        for (index, field) in second.layout.fields.iter().enumerate() {
            assert_eq!(field.index as usize, index);
        }
        assert_eq!(second.layout.fields[0].kind, FieldKind::F64);
        assert_eq!(second.layout.fields[1].kind, FieldKind::Str);
        assert_eq!(second.layout.fields[3].kind, FieldKind::Str);
        assert_eq!(second.layout.fields[18].kind, FieldKind::I32);
        assert_eq!(second.layout.fields[19].kind, FieldKind::F64);
        let second_value_names: Vec<_> = second
            .values
            .iter()
            .map(|field| second.layout.fields[field.index as usize].name.as_ref())
            .collect();
        assert_eq!(
            second_value_names,
            vec![
                "ANCHOR",
                "PROMOTED_I64",
                "PROMOTED_TEXT",
                "DISCOVERED_I32",
                "DISCOVERED_F64"
            ]
        );
        assert!(matches!(second.values[0].value, UpdateValue::F64(2.5)));
        assert!(matches!(
            second.values[1].value,
            UpdateValue::I64(1_234_567_890_123)
        ));
        assert!(matches!(
            &second.values[2].value,
            UpdateValue::Str(value) if value.as_ref() == "ready"
        ));
        assert!(matches!(second.values[3].value, UpdateValue::I32(7)));
        assert!(matches!(second.values[4].value, UpdateValue::F64(4.5)));

        assert_eq!(retained_layout.version, first_version);
        let retained_names: Vec<_> = retained_layout
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect();
        assert_eq!(retained_names, initial_names);
        assert_eq!(retained_layout.fields[0].kind, FieldKind::F64);
        assert_eq!(retained_layout.fields[1].kind, FieldKind::I32);
        assert_eq!(retained_layout.fields[3].kind, FieldKind::F64);

        let second_signature: Vec<_> = second
            .layout
            .fields
            .iter()
            .map(|field| (field.name.to_string(), field.index, field.kind))
            .collect();
        assert_eq!(
            deliver(&mut state, &multi_change),
            MessageOutcome::Normal {
                first_message: false
            }
        );
        let third = rx
            .try_recv()
            .expect("same-kind update delivered immediately")
            .expect("same-kind update");
        assert_eq!(third.layout.version, second.layout.version);
        let third_signature: Vec<_> = third
            .layout
            .fields
            .iter()
            .map(|field| (field.name.to_string(), field.index, field.kind))
            .collect();
        assert_eq!(third_signature, second_signature);
    }

    #[test]
    fn mktbar_projection_overrides_sdk_field_at_requested_position() {
        let requested = [
            "FIELD_00",
            "VOLUME",
            "FIELD_02",
            "SUBSCRIPTION_DATA",
            "FIELD_04",
            "LAST_PRICE",
            "FIELD_06",
            "FIELD_07",
            "FIELD_08",
            "FIELD_09",
            "FIELD_10",
            "FIELD_11",
            "FIELD_12",
            "FIELD_13",
            "FIELD_14",
            "FIELD_15",
        ];
        let event = fixture(
            r#"<element name="LAST_PRICE" type="Float64"/>
        <element name="SUBSCRIPTION_DATA" type="String"/>
        <element name="VOLUME" type="Int32"/>"#,
            "MarketBarUpdate",
            |formatter| {
                formatter.json(
                    r#"{"LAST_PRICE":101.5,"SUBSCRIPTION_DATA":"SDK_FIELD_VALUE","VOLUME":42}"#,
                )
            },
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "//blp/mktbar/ticker/TEST".into(),
            requested.iter().map(|field| (*field).to_owned()).collect(),
            tx,
            9,
            false,
        );

        assert_eq!(
            deliver(&mut state, &event),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        let update = rx
            .try_recv()
            .expect("mktbar update delivered immediately")
            .expect("mktbar update");
        let names: Vec<_> = update
            .values
            .iter()
            .map(|field| update.layout.fields[field.index as usize].name.as_ref())
            .collect();
        assert_eq!(names, vec!["VOLUME", "SUBSCRIPTION_DATA", "LAST_PRICE"]);
        assert_eq!(update.values[0].index, 1);
        assert_eq!(update.values[1].index, 3);
        assert_eq!(update.values[2].index, 5);
        assert!(matches!(update.values[0].value, UpdateValue::I32(42)));
        assert!(matches!(
            &update.values[1].value,
            UpdateValue::Str(value) if value.as_ref() == "MarketBarUpdate"
        ));
        assert!(matches!(update.values[2].value, UpdateValue::F64(101.5)));
    }

    #[test]
    fn wide_sparse_dataloss_is_terminal_after_an_accepted_update() {
        let requested = [
            "LAST_PRICE",
            "FIELD_01",
            "FIELD_02",
            "FIELD_03",
            "FIELD_04",
            "FIELD_05",
            "FIELD_06",
            "FIELD_07",
            "FIELD_08",
            "FIELD_09",
            "FIELD_10",
            "FIELD_11",
            "FIELD_12",
            "FIELD_13",
            "FIELD_14",
            "FIELD_15",
        ];
        let data = fixture(
            r#"<element name="LAST_PRICE" type="Float64"/>"#,
            "MarketDataEvents",
            |formatter| formatter.json(r#"{"LAST_PRICE":99.25}"#),
        );
        let dataloss = fixture(
            r#"<element name="MKTDATA_EVENT_SUBTYPE" type="String"/>
        <element name="MKTDATA_EVENT_TYPE" type="String"/>"#,
            "MarketDataEvents",
            |formatter| {
                formatter
                    .json(r#"{"MKTDATA_EVENT_SUBTYPE":"DATALOSS","MKTDATA_EVENT_TYPE":"SUMMARY"}"#)
            },
        );
        let (tx, mut rx) = subscription_channel(1);
        let mut state = SubscriptionState::new(
            "TEST".into(),
            requested.iter().map(|field| (*field).to_owned()).collect(),
            tx,
            11,
            false,
        );

        assert_eq!(
            deliver(&mut state, &data),
            MessageOutcome::Normal {
                first_message: true
            }
        );
        let accepted = rx
            .try_recv()
            .expect("data update delivered immediately")
            .expect("accepted data update");
        assert_eq!(accepted.values.len(), 1);
        assert_eq!(
            accepted.layout.fields[accepted.values[0].index as usize]
                .name
                .as_ref(),
            "LAST_PRICE"
        );
        assert!(matches!(accepted.values[0].value, UpdateValue::F64(99.25)));

        assert_eq!(deliver(&mut state, &dataloss), MessageOutcome::DataLoss);
        assert!(matches!(
            rx.try_recv().expect("terminal DATALOSS error"),
            Err(BlpError::SubscriptionDataLoss { topic, .. }) if topic == "TEST"
        ));
        assert_eq!(deliver(&mut state, &data), MessageOutcome::Closed);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
