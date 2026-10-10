//! Subscription lifecycle snapshots, topic indexes, and status publication.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use parking_lot::Mutex as ParkingMutex;

use super::{SlabKey, SubscriptionMetrics};

/// Why Bloomberg stopped a single subscribed topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionFailureKind {
    Failure,
    Terminated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicLifecycleState {
    Pending,
    Started,
    Streaming,
    Unsubscribing,
    Unsubscribed,
    Failed,
    Terminated,
}

impl TopicLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Started => "started",
            Self::Streaming => "streaming",
            Self::Unsubscribing => "unsubscribing",
            Self::Unsubscribed => "unsubscribed",
            Self::Failed => "failed",
            Self::Terminated => "terminated",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionLifecycleState {
    Starting,
    Up,
    Down,
    Terminated,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerHealth {
    #[default]
    Healthy,
    Degraded,
    Dead,
}

impl WorkerHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Dead => "dead",
        }
    }
}

impl SessionLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Up => "up",
            Self::Down => "down",
            Self::Terminated => "terminated",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionEventCategory {
    Session,
    Service,
    Admin,
    Subscription,
    Lifecycle,
}

impl SubscriptionEventCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Service => "service",
            Self::Admin => "admin",
            Self::Subscription => "subscription",
            Self::Lifecycle => "lifecycle",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionEventLevel {
    Info,
    Warning,
    Error,
}

impl SubscriptionEventLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicStatusInfo {
    pub topic: String,
    /// Bloomberg topic, before the consumer's alias is applied.
    pub feed_topic: String,
    pub delayed: Option<bool>,
    pub state: TopicLifecycleState,
    pub last_change_us: i64,
    /// Whether Bloomberg currently has active streams for this topic.
    /// Set by `SubscriptionStreamsActivated` / `SubscriptionStreamsDeactivated`.
    /// The SDK (v3.11.6+) auto-recovers streams across transient disconnections;
    /// callers use this to see "stream alive but temporarily silent" vs. "streaming".
    pub streams_active: bool,
    /// Microsecond timestamp of the most recent streams_active transition.
    pub streams_changed_us: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceStatusInfo {
    pub service: String,
    pub up: bool,
    pub last_change_us: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdminStatusInfo {
    pub slow_consumer_warning_active: bool,
    pub slow_consumer_warning_count: u64,
    pub slow_consumer_cleared_count: u64,
    pub data_loss_count: u64,
    pub last_warning_us: Option<i64>,
    pub last_cleared_us: Option<i64>,
    pub last_data_loss_us: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatusInfo {
    pub state: SessionLifecycleState,
    pub last_change_us: i64,
    pub disconnect_count: u64,
    pub reconnect_count: u64,
}

impl Default for SessionStatusInfo {
    fn default() -> Self {
        Self {
            state: SessionLifecycleState::Starting,
            last_change_us: timestamp_now_us(),
            disconnect_count: 0,
            reconnect_count: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionEventInfo {
    pub at_us: i64,
    pub category: SubscriptionEventCategory,
    pub level: SubscriptionEventLevel,
    pub message_type: String,
    pub topic: Option<String>,
    pub detail: Option<String>,
}

pub(super) const SUBSCRIPTION_EVENT_HISTORY_LIMIT: usize = 128;

pub(super) fn timestamp_now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}

impl SubscriptionFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Terminated => "terminated",
        }
    }
}

/// Recorded non-fatal failure for a single topic in a multi-topic subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionFailureInfo {
    pub topic: String,
    pub reason: String,
    pub kind: SubscriptionFailureKind,
    pub at_us: i64,
}

/// Shared subscription status visible to worker and consumer-facing handles.
#[derive(Clone, Default)]
pub struct SubscriptionStatusState {
    keys: Vec<SlabKey>,
    topics: Vec<String>,
    topic_to_key: HashMap<String, SlabKey>,
    key_to_topic: HashMap<SlabKey, String>,
    pub(super) pending_key_to_topic: HashMap<SlabKey, String>,
    metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>>,
    failures: Vec<SubscriptionFailureInfo>,
    topic_states: HashMap<String, TopicStatusInfo>,
    events: VecDeque<SubscriptionEventInfo>,
    event_sequence: u64,
    observer_events: Option<Vec<SubscriptionEventInfo>>,
    pub(super) defer_indices: bool,
    indices_dirty: bool,
    #[cfg(test)]
    pub(super) index_scans: usize,
    field_errors: HashMap<String, HashMap<String, String>>,
    warnings: VecDeque<SubscriptionEventInfo>,
    pub(super) session: SessionStatusInfo,
    pub(super) services: HashMap<String, ServiceStatusInfo>,
    pub(super) admin: AdminStatusInfo,
}

#[derive(Clone, Copy)]
pub(crate) enum SubscriptionStatusScope<'a> {
    Topics(&'a [SlabKey]),
    Global,
}

type SubscriptionStatusObserver = dyn Fn(
        &SubscriptionStatusState,
        &SubscriptionStatusState,
        SubscriptionStatusScope<'_>,
        &[SubscriptionEventInfo],
    ) + Send
    + Sync;

/// Shared status handle: readers get an ArcSwap snapshot, while writers take a
/// small mutation mutex so all changes for one dispatch path are published as a
/// single snapshot update instead of many independent whole-state RCU clones.
#[derive(Default)]
pub struct SubscriptionStatusHandle {
    snapshot: ArcSwap<SubscriptionStatusState>,
    mutation_lock: ParkingMutex<()>,
    observer: Option<Arc<SubscriptionStatusObserver>>,
    pending_warnings: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    publications: std::sync::atomic::AtomicUsize,
}

pub type SharedSubscriptionStatus = Arc<SubscriptionStatusHandle>;

impl SubscriptionStatusHandle {
    pub fn new(initial: SubscriptionStatusState) -> Self {
        let pending_warnings = initial.warnings.len();
        Self {
            snapshot: ArcSwap::from_pointee(initial),
            mutation_lock: ParkingMutex::new(()),
            observer: None,
            pending_warnings: std::sync::atomic::AtomicUsize::new(pending_warnings),
            #[cfg(test)]
            publications: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_observer(observer: Arc<SubscriptionStatusObserver>) -> Self {
        Self {
            observer: Some(observer),
            ..Self::default()
        }
    }

    fn notify_observer(
        &self,
        previous: &SubscriptionStatusState,
        next: &SubscriptionStatusState,
        scope: SubscriptionStatusScope<'_>,
        events: &[SubscriptionEventInfo],
    ) {
        #[cfg(test)]
        self.publications
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            observer(previous, next, scope, events);
        }
    }

    #[cfg(test)]
    pub(crate) fn publication_count(&self) -> usize {
        self.publications.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn load(&self) -> arc_swap::Guard<Arc<SubscriptionStatusState>> {
        self.snapshot.load()
    }

    pub fn store(&self, next: Arc<SubscriptionStatusState>) {
        let _guard = self.mutation_lock.lock();
        let previous = self.snapshot.swap(next.clone());
        self.pending_warnings
            .store(next.warnings.len(), std::sync::atomic::Ordering::Release);
        let count = next.event_sequence.wrapping_sub(previous.event_sequence) as usize;
        let events: Vec<_> = next
            .events
            .iter()
            .skip(next.events.len().saturating_sub(count))
            .cloned()
            .collect();
        self.notify_observer(&previous, &next, SubscriptionStatusScope::Global, &events);
        drop(previous);
    }

    pub fn update(&self, mutate: impl FnOnce(&mut SubscriptionStatusState)) {
        self.update_with(|status| {
            mutate(status);
        });
    }

    pub fn update_with<R>(&self, mutate: impl FnOnce(&mut SubscriptionStatusState) -> R) -> R {
        self.update_scoped(SubscriptionStatusScope::Global, mutate)
    }

    /// Drain new subscription warnings without publishing or cloning an empty snapshot.
    /// The status handle can outlive its subscription control, so this also works after close.
    pub fn take_warnings(&self) -> Vec<SubscriptionEventInfo> {
        if self
            .pending_warnings
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return Vec::new();
        }
        let _guard = self.mutation_lock.lock();
        if self
            .pending_warnings
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return Vec::new();
        }
        let current = self.snapshot.load_full();
        let mut next = (*current).clone();
        let warnings = next.take_warnings();
        let next = Arc::new(next);
        self.snapshot.store(next.clone());
        self.pending_warnings
            .store(0, std::sync::atomic::Ordering::Release);
        self.notify_observer(&current, &next, SubscriptionStatusScope::Global, &[]);
        warnings
    }

    pub(crate) fn update_topics(
        &self,
        keys: &[SlabKey],
        mutate: impl FnOnce(&mut SubscriptionStatusState),
    ) {
        self.update_scoped(SubscriptionStatusScope::Topics(keys), mutate);
    }

    fn update_scoped<R>(
        &self,
        scope: SubscriptionStatusScope<'_>,
        mutate: impl FnOnce(&mut SubscriptionStatusState) -> R,
    ) -> R {
        let _guard = self.mutation_lock.lock();
        let current = self.snapshot.load_full();
        let mut next = (*current).clone();
        next.observer_events = self.observer.as_ref().map(|_| Vec::new());
        next.defer_indices =
            matches!(scope, SubscriptionStatusScope::Topics(keys) if keys.len() > 1);
        let result = mutate(&mut next);
        next.finish_index_changes();
        let events = next.observer_events.take().unwrap_or_default();
        let next = Arc::new(next);
        self.snapshot.store(next.clone());
        self.pending_warnings
            .store(next.warnings.len(), std::sync::atomic::Ordering::Release);
        self.notify_observer(&current, &next, scope, &events);
        result
    }
}

impl SubscriptionStatusState {
    #[cfg(any(test, feature = "bench-internals"))]
    pub fn from_active(
        topics: Vec<String>,
        keys: Vec<SlabKey>,
        metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>>,
    ) -> Self {
        let mut status = Self {
            keys,
            topics,
            topic_to_key: HashMap::new(),
            key_to_topic: HashMap::new(),
            pending_key_to_topic: HashMap::new(),
            metrics,
            failures: Vec::new(),
            topic_states: HashMap::new(),
            events: VecDeque::with_capacity(SUBSCRIPTION_EVENT_HISTORY_LIMIT),
            event_sequence: 0,
            observer_events: None,
            defer_indices: false,
            indices_dirty: false,
            #[cfg(test)]
            index_scans: 0,
            field_errors: HashMap::new(),
            warnings: VecDeque::new(),
            session: SessionStatusInfo {
                state: SessionLifecycleState::Up,
                ..SessionStatusInfo::default()
            },
            services: HashMap::new(),
            admin: AdminStatusInfo::default(),
        };
        let now = timestamp_now_us();
        let topics = status.topics.clone();
        let keys = status.keys.clone();
        for (topic, key) in topics.into_iter().zip(keys) {
            status.topic_to_key.insert(topic.clone(), key);
            status.key_to_topic.insert(key, topic.clone());
            status.topic_states.insert(
                topic.clone(),
                TopicStatusInfo {
                    feed_topic: topic.clone(),
                    delayed: None,
                    topic,
                    state: TopicLifecycleState::Pending,
                    last_change_us: now,
                    streams_active: false,
                    streams_changed_us: now,
                },
            );
        }
        status
    }

    pub fn add_active(
        &mut self,
        topics: &[String],
        keys: &[SlabKey],
        metrics: Vec<Arc<SubscriptionMetrics>>,
    ) {
        let now = timestamp_now_us();
        if self.keys.is_empty() {
            self.session.state = SessionLifecycleState::Up;
            self.session.last_change_us = now;
        }
        for ((topic, key), metric) in topics.iter().zip(keys.iter()).zip(metrics) {
            self.topic_to_key.insert(topic.clone(), *key);
            self.pending_key_to_topic.remove(key);
            self.key_to_topic.insert(*key, topic.clone());
            self.topics.push(topic.clone());
            self.keys.push(*key);
            self.metrics.insert(*key, metric);
            self.topic_states.insert(
                topic.clone(),
                TopicStatusInfo {
                    feed_topic: topic.clone(),
                    delayed: None,
                    topic: topic.clone(),
                    state: TopicLifecycleState::Pending,
                    last_change_us: now,
                    streams_active: false,
                    streams_changed_us: now,
                },
            );
        }
    }

    pub fn remove_topic(&mut self, topic: &str) -> Option<SlabKey> {
        let key = self.topic_to_key.remove(topic)?;
        self.key_to_topic.remove(&key);
        self.remove_active_index(key, topic);
        self.metrics.remove(&key);
        Some(key)
    }

    /// Fully remove a topic at the user's request, including its status history.
    ///
    /// Unlike [`Self::remove_topic`] (which keeps the `topic_states` entry so the SDK
    /// terminal path can report a final lifecycle state), this also drops the
    /// `topic_states` entry so the topic disappears from [`Self::topic_statuses`].
    pub fn drop_topic(&mut self, topic: &str) -> Option<SlabKey> {
        let key = self.remove_topic(topic).or_else(|| {
            let key = self
                .pending_key_to_topic
                .iter()
                .find_map(|(key, pending)| (pending == topic).then_some(*key))?;
            self.pending_key_to_topic.remove(&key);
            Some(key)
        });
        self.topic_states.remove(topic);
        self.field_errors.remove(topic);
        key
    }

    pub fn topic_for_key(&self, key: SlabKey) -> Option<&str> {
        self.key_to_topic.get(&key).map(String::as_str)
    }

    pub fn topic_statuses(&self) -> &HashMap<String, TopicStatusInfo> {
        &self.topic_states
    }

    pub fn session(&self) -> &SessionStatusInfo {
        &self.session
    }

    pub fn services(&self) -> &HashMap<String, ServiceStatusInfo> {
        &self.services
    }

    pub fn admin(&self) -> &AdminStatusInfo {
        &self.admin
    }

    pub fn events(&self) -> &VecDeque<SubscriptionEventInfo> {
        &self.events
    }

    pub fn field_errors(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.field_errors
    }

    pub(crate) fn set_feed_topic(&mut self, label: &str, feed_topic: &str) {
        if let Some(info) = self.topic_states.get_mut(label) {
            info.feed_topic = feed_topic.to_string();
        }
    }

    pub(crate) fn set_delayed(&mut self, label: &str, delayed: Option<bool>) {
        if let Some(info) = self.topic_states.get_mut(label) {
            info.delayed = delayed;
        }
    }

    pub(crate) fn record_field_error(&mut self, label: &str, field: &str, category: &str) {
        self.field_errors
            .entry(label.to_string())
            .or_default()
            .insert(field.to_string(), category.to_string());
    }

    pub fn take_warnings(&mut self) -> Vec<SubscriptionEventInfo> {
        self.warnings.drain(..).collect()
    }

    fn finalize_key(&mut self, key: SlabKey) -> Option<String> {
        let topic = self
            .key_to_topic
            .remove(&key)
            .or_else(|| self.pending_key_to_topic.remove(&key))?;
        self.topic_to_key.remove(&topic);
        self.remove_active_index(key, &topic);
        self.metrics.remove(&key);
        Some(topic)
    }

    fn remove_active_index(&mut self, key: SlabKey, topic: &str) {
        if self.defer_indices {
            self.indices_dirty = true;
        } else {
            #[cfg(test)]
            {
                self.index_scans += 1;
            }
            self.keys.retain(|existing| *existing != key);
            self.topics.retain(|existing| existing != topic);
        }
    }

    fn finish_index_changes(&mut self) {
        if self.indices_dirty {
            #[cfg(test)]
            {
                self.index_scans += 1;
            }
            self.keys.retain(|key| self.key_to_topic.contains_key(key));
            self.topics
                .retain(|topic| self.topic_to_key.contains_key(topic));
        }
        self.defer_indices = false;
        self.indices_dirty = false;
    }

    pub fn push_event(
        &mut self,
        category: SubscriptionEventCategory,
        level: SubscriptionEventLevel,
        message_type: impl Into<String>,
        topic: Option<String>,
        detail: Option<String>,
    ) {
        let event = SubscriptionEventInfo {
            at_us: timestamp_now_us(),
            category,
            level,
            message_type: message_type.into(),
            topic,
            detail,
        };
        self.append_event(event);
    }

    pub(super) fn append_event(&mut self, event: SubscriptionEventInfo) {
        if let Some(events) = &mut self.observer_events {
            events.push(event.clone());
        }
        if self.events.len() >= SUBSCRIPTION_EVENT_HISTORY_LIMIT {
            self.events.pop_front();
        }
        if event.level == SubscriptionEventLevel::Warning
            && matches!(
                event.message_type.as_str(),
                "DelayedStream" | "FieldException"
            )
        {
            self.warnings.push_back(event.clone());
        }
        self.event_sequence = self.event_sequence.wrapping_add(1);
        self.events.push_back(event);
    }

    pub(super) fn update_topic_state(&mut self, topic: &str, state: TopicLifecycleState) {
        let now = timestamp_now_us();
        self.topic_states
            .entry(topic.to_string())
            .and_modify(|status| {
                status.state = state;
                status.last_change_us = now;
            })
            .or_insert_with(|| TopicStatusInfo {
                topic: topic.to_string(),
                feed_topic: topic.to_string(),
                delayed: None,
                state,
                last_change_us: now,
                streams_active: false,
                streams_changed_us: now,
            });
    }

    /// Flip `streams_active` for a topic (driven by SubscriptionStreams{Activated,Deactivated}).
    /// Returns the previous value if the topic existed, else None.
    pub fn set_topic_streams_active(&mut self, topic: &str, active: bool) -> Option<bool> {
        let now = timestamp_now_us();
        let entry = self.topic_states.get_mut(topic)?;
        let prev = entry.streams_active;
        if prev != active {
            entry.streams_active = active;
            entry.streams_changed_us = now;
        }
        Some(prev)
    }

    pub fn mark_topic_started(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.topic_for_key(key)?.to_string();
        self.update_topic_state(&topic, TopicLifecycleState::Started);
        Some(topic)
    }

    pub fn mark_topic_streaming(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.topic_for_key(key)?.to_string();
        self.update_topic_state(&topic, TopicLifecycleState::Streaming);
        Some(topic)
    }

    pub fn mark_topic_unsubscribing(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.key_to_topic.remove(&key)?;
        self.topic_to_key.remove(&topic);
        self.remove_active_index(key, &topic);
        self.metrics.remove(&key);
        self.pending_key_to_topic.insert(key, topic.clone());
        self.update_topic_state(&topic, TopicLifecycleState::Unsubscribing);
        Some(topic)
    }

    pub fn mark_topic_unsubscribed(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.finalize_key(key)?;
        self.update_topic_state(&topic, TopicLifecycleState::Unsubscribed);
        let _ = self.set_topic_streams_active(&topic, false);
        Some(topic)
    }

    pub fn record_failure(
        &mut self,
        key: SlabKey,
        reason: String,
        kind: SubscriptionFailureKind,
    ) -> Option<String> {
        let topic = self.finalize_key(key)?;
        let state = match kind {
            SubscriptionFailureKind::Failure => TopicLifecycleState::Failed,
            SubscriptionFailureKind::Terminated => TopicLifecycleState::Terminated,
        };
        self.update_topic_state(&topic, state);
        let _ = self.set_topic_streams_active(&topic, false);
        self.failures.push(SubscriptionFailureInfo {
            topic: topic.clone(),
            reason,
            kind,
            at_us: timestamp_now_us(),
        });
        Some(topic)
    }

    /// Finalize a session's failed consumer topics without repeatedly scanning
    /// the active vectors once for every topic.
    pub(super) fn record_failures(
        &mut self,
        failures: Vec<(SlabKey, String, SubscriptionFailureKind)>,
    ) {
        let keys: std::collections::HashSet<_> = failures.iter().map(|failure| failure.0).collect();
        for (key, reason, kind) in failures {
            let Some(topic) = self
                .key_to_topic
                .remove(&key)
                .or_else(|| self.pending_key_to_topic.remove(&key))
            else {
                continue;
            };
            self.topic_to_key.remove(&topic);
            self.metrics.remove(&key);
            let state = match kind {
                SubscriptionFailureKind::Failure => TopicLifecycleState::Failed,
                SubscriptionFailureKind::Terminated => TopicLifecycleState::Terminated,
            };
            self.update_topic_state(&topic, state);
            self.set_topic_streams_active(&topic, false);
            self.failures.push(SubscriptionFailureInfo {
                topic: topic.clone(),
                reason: reason.clone(),
                kind,
                at_us: timestamp_now_us(),
            });
        }
        #[cfg(test)]
        {
            self.index_scans += 1;
        }
        self.keys.retain(|key| !keys.contains(key));
        self.topics
            .retain(|topic| self.topic_to_key.contains_key(topic));
    }

    pub fn clear_active(&mut self) {
        let now = timestamp_now_us();
        for topic in self.topic_states.values_mut() {
            if topic.streams_active {
                topic.streams_active = false;
                topic.streams_changed_us = now;
            }
        }
        self.keys.clear();
        self.topics.clear();
        self.topic_to_key.clear();
        self.key_to_topic.clear();
        self.metrics.clear();
    }

    pub fn keys(&self) -> &[SlabKey] {
        &self.keys
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub fn fields_metrics(&self) -> &HashMap<SlabKey, Arc<SubscriptionMetrics>> {
        &self.metrics
    }

    pub fn topic_to_key(&self) -> &HashMap<String, SlabKey> {
        &self.topic_to_key
    }

    pub fn failures(&self) -> &[SubscriptionFailureInfo] {
        &self.failures
    }

    pub fn has_active_topics(&self) -> bool {
        !self.keys.is_empty()
    }

    pub fn record_subscription_event(
        &mut self,
        message_type: &str,
        topic: Option<String>,
        detail: Option<String>,
        level: SubscriptionEventLevel,
    ) {
        self.push_event(
            SubscriptionEventCategory::Subscription,
            level,
            message_type,
            topic,
            detail,
        );
    }

    pub fn record_session_state(
        &mut self,
        state: SessionLifecycleState,
        message_type: &str,
        detail: Option<String>,
    ) {
        let now = timestamp_now_us();
        if self.session.state == SessionLifecycleState::Down && state == SessionLifecycleState::Up {
            self.session.reconnect_count += 1;
        }
        if state == SessionLifecycleState::Down {
            self.session.disconnect_count += 1;
        }
        self.session.state = state;
        self.session.last_change_us = now;
        let level = match state {
            SessionLifecycleState::Down | SessionLifecycleState::Terminated => {
                SubscriptionEventLevel::Error
            }
            _ => SubscriptionEventLevel::Info,
        };
        self.push_event(
            SubscriptionEventCategory::Session,
            level,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_service_state(
        &mut self,
        service: String,
        up: bool,
        message_type: &str,
        detail: Option<String>,
    ) {
        let now = timestamp_now_us();
        self.services.insert(
            service.clone(),
            ServiceStatusInfo {
                service: service.clone(),
                up,
                last_change_us: now,
            },
        );
        self.push_event(
            SubscriptionEventCategory::Service,
            if up {
                SubscriptionEventLevel::Info
            } else {
                SubscriptionEventLevel::Warning
            },
            message_type,
            Some(service),
            detail,
        );
    }

    pub fn record_admin_warning(&mut self, message_type: &str, detail: Option<String>) {
        self.admin.slow_consumer_warning_active = true;
        self.admin.slow_consumer_warning_count += 1;
        self.admin.last_warning_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Warning,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_admin_warning_cleared(&mut self, message_type: &str, detail: Option<String>) {
        self.admin.slow_consumer_warning_active = false;
        self.admin.slow_consumer_cleared_count += 1;
        self.admin.last_cleared_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Info,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_admin_data_loss(&mut self, topic: Option<String>, detail: Option<String>) {
        self.admin.data_loss_count += 1;
        self.admin.last_data_loss_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Warning,
            "DataLoss",
            topic,
            detail,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    #[test]
    fn subscription_status_records_failure_and_removes_active_topic() {
        let metric = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });
        let mut status = SubscriptionStatusState::from_active(
            vec![
                "SPY US Equity".to_string(),
                "/isin/BMG8192H1557".to_string(),
            ],
            vec![10, 11],
            HashMap::from([(10, metric.clone()), (11, metric)]),
        );
        assert_eq!(
            status.set_topic_streams_active("/isin/BMG8192H1557", true),
            Some(false)
        );

        let topic = status.record_failure(
            11,
            "Security is not valid for subscription [EX336]".to_string(),
            SubscriptionFailureKind::Failure,
        );

        assert_eq!(topic.as_deref(), Some("/isin/BMG8192H1557"));
        assert_eq!(status.topics(), &["SPY US Equity".to_string()]);
        assert_eq!(status.keys(), &[10]);
        assert_eq!(status.failures().len(), 1);
        assert_eq!(status.failures()[0].kind, SubscriptionFailureKind::Failure);
        assert_eq!(status.failures()[0].topic, "/isin/BMG8192H1557");
        assert_eq!(
            status.topic_statuses()["/isin/BMG8192H1557"].state,
            TopicLifecycleState::Failed,
        );
        assert!(!status.topic_statuses()["/isin/BMG8192H1557"].streams_active);
    }

    #[test]
    fn subscription_status_tracks_session_and_admin_events() {
        let mut status = SubscriptionStatusState::default();

        status.record_session_state(
            SessionLifecycleState::Down,
            "SessionConnectionDown",
            Some("worker=0 active_subscriptions=2".to_string()),
        );
        status.record_session_state(
            SessionLifecycleState::Up,
            "SessionConnectionUp",
            Some("worker=0 active_subscriptions=2".to_string()),
        );
        status.record_admin_warning("SlowConsumerWarning", None);
        status.record_admin_warning_cleared("SlowConsumerWarningCleared", None);
        status.record_admin_data_loss(Some("SPY US Equity".to_string()), None);

        assert_eq!(status.session().state, SessionLifecycleState::Up);
        assert_eq!(status.session().disconnect_count, 1);
        assert_eq!(status.session().reconnect_count, 1);
        assert_eq!(status.admin().slow_consumer_warning_count, 1);
        assert_eq!(status.admin().slow_consumer_cleared_count, 1);
        assert_eq!(status.admin().data_loss_count, 1);
        assert_eq!(status.events().len(), 5);
        assert_eq!(
            status
                .events()
                .back()
                .map(|event| event.message_type.as_str()),
            Some("DataLoss"),
        );
    }

    #[test]
    fn subscription_status_drop_topic_removes_all_state_and_blocks_resurrection() {
        let metric = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });
        let mut status = SubscriptionStatusState::from_active(
            vec!["SPY US Equity".to_string(), "IBM US Equity".to_string()],
            vec![10, 11],
            HashMap::from([(10, metric.clone()), (11, metric)]),
        );

        let key = status.drop_topic("IBM US Equity");

        assert_eq!(key, Some(11));
        // topic_to_key invariant: gone from both directions.
        assert!(!status.topic_to_key().contains_key("IBM US Equity"));
        assert_eq!(status.topic_for_key(11), None);
        // Active lists, metrics, and status history no longer reference the topic/key.
        assert_eq!(status.topics(), &["SPY US Equity".to_string()]);
        assert_eq!(status.keys(), &[10]);
        assert!(!status.fields_metrics().contains_key(&11));
        assert!(!status.topic_statuses().contains_key("IBM US Equity"));
        // A late tick for the dropped key cannot resurrect the topic.
        assert_eq!(status.mark_topic_streaming(11), None);
        assert!(!status.topic_statuses().contains_key("IBM US Equity"));
        // The surviving topic is untouched.
        assert!(status.topic_statuses().contains_key("SPY US Equity"));
    }

    #[test]
    fn subscription_status_completes_pending_unsubscribe_with_topic() {
        let metric = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });
        let mut status = SubscriptionStatusState::from_active(
            vec!["IBM US Equity".to_string()],
            vec![11],
            HashMap::from([(11, metric)]),
        );

        assert_eq!(
            status.mark_topic_unsubscribing(11).as_deref(),
            Some("IBM US Equity")
        );
        assert!(status.keys().is_empty());
        assert_eq!(
            status.mark_topic_unsubscribed(11).as_deref(),
            Some("IBM US Equity")
        );
        assert_eq!(
            status.topic_statuses()["IBM US Equity"].state,
            TopicLifecycleState::Unsubscribed
        );
    }

    #[test]
    fn clearing_active_topics_marks_sdk_streams_inactive() {
        let mut status = SubscriptionStatusState::from_active(
            vec!["IBM US Equity".to_string()],
            vec![11],
            HashMap::new(),
        );
        assert_eq!(
            status.set_topic_streams_active("IBM US Equity", true),
            Some(false)
        );

        status.clear_active();

        assert!(status.keys().is_empty());
        assert!(!status.topic_statuses()["IBM US Equity"].streams_active);
    }
}
