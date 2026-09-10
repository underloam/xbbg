//! Worker Pool Engine for Bloomberg API.
//!
//! Architecture:
//! - RequestWorkerPool: Pre-warmed workers for all request types (bdp/bdh/bds/bdib/bdtick)
//! - SubscriptionSessionPool: Pre-warmed sessions for subscriptions (each gets dedicated session)
//!
//! Workers encode stable dispatch keys into Bloomberg correlation IDs for O(1) dispatch.
//! Pool sizes are configurable with sensible defaults.

mod dispatch;
mod exchange;
mod exchange_cache;
mod intraday_timezone;
mod request_plan;
mod request_pool;
pub mod state;
mod subscription_pool;
mod transport;
mod worker;

pub use transport::{ServerAddr, Socks5Proxy, TlsConfig, Transport};

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use futures_util::stream::{self, StreamExt};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::watch;

use xbbg_core::{apply_session_identity_options, AuthConfig, BlpError, SessionOptions};

use crate::errors::BlpAsyncError;
use crate::services::{Operation, Service};
use exchange_cache::ExchangeCache;

// ExtractorType is defined in services.rs (generated from defs/bloomberg.toml).
// Re-export here so existing `use xbbg_async::engine::ExtractorType` paths keep working.
pub use crate::services::ExtractorType;

pub(crate) use request_plan::{PlannedRequestShape, PreparedRequest, PreparedRequestBuilder};
pub use request_pool::{RequestStream, RequestWorkerPool};
use state::typed_builder::{ArrowType, TypedBuilder};
use state::{subscription_channel, SubscriptionMetrics, SubscriptionReceiver, SubscriptionSender};
pub use state::{
    BqlState, BulkDataState, HistDataState, IntradayTickState, LongMode, OutputFormat,
    RefDataState, SubscriptionState, SubscriptionUpdate,
};
pub use subscription_pool::{SessionClaim, SubscriptionCommandHandle, SubscriptionSessionPool};
pub use worker::UnifiedRequestState;

const SESSION_STARTUP_TIMEOUT_MS: u32 = 30_000;
pub type OverridePairs = Vec<(String, String)>;
pub type SecurityOverridePairs = Vec<(String, OverridePairs)>;

fn parse_operation_lossless(operation: &str) -> Operation {
    match Operation::from_str(operation) {
        Ok(operation) => operation,
        Err(never) => match never {},
    }
}

fn apply_direct_transport(
    options: &mut SessionOptions,
    servers: &[ServerAddr],
) -> Result<(), BlpError> {
    for (index, addr) in servers.iter().enumerate() {
        match &addr.proxy {
            Some(proxy) => {
                let socks5 = xbbg_core::socks5::Socks5Config::new(&proxy.host, proxy.port)?;
                options.set_server_address_with_proxy(&addr.host, addr.port, &socks5, index)?;
            }
            None => {
                options.set_server_address(&addr.host, addr.port, index)?;
            }
        }
    }
    Ok(())
}

/// Apply non-transport session behavior: pool sizes, keep-alive, slow-consumer
/// watermarks, identity auth, etc. Endpoint configuration (server addresses,
/// SOCKS5, TLS) is handled separately in `build_session_options` so ZFP
/// options from `ZfpUtil::getOptionsForLeasedLines` are never clobbered.
fn configure_session_behavior(
    options: &mut SessionOptions,
    config: &EngineConfig,
    record_subscription_receive_times: bool,
) -> Result<(), BlpError> {
    options.set_num_start_attempts(config.num_start_attempts)?;
    options.set_auto_restart_on_disconnection(config.auto_restart_on_disconnection);
    options.set_max_event_queue_size(config.max_event_queue_size);
    let _ = options.set_bandwidth_save_mode_disabled(true);

    options.set_keep_alive_enabled(config.keep_alive_enabled)?;
    if let Some(ms) = config.keep_alive_inactivity_ms {
        options.set_keep_alive_inactivity_time_ms(ms)?;
    }
    if let Some(ms) = config.keep_alive_response_timeout_ms {
        options.set_keep_alive_response_timeout_ms(ms)?;
    }
    if let Some(hi) = config.slow_consumer_hi_water_mark {
        options.set_slow_consumer_warning_hi_watermark(hi)?;
    }
    if let Some(lo) = config.slow_consumer_lo_water_mark {
        options.set_slow_consumer_warning_lo_watermark(lo)?;
    }

    if record_subscription_receive_times {
        options.set_record_subscription_receive_times(true);
    }

    if let Some(auth_config) = config.auth.as_ref() {
        let _ = apply_session_identity_options(options, auth_config)?;
    }

    Ok(())
}

/// Build fully-configured `SessionOptions` for this engine config: transport
/// endpoints (direct/ZFP, SOCKS5, TLS) plus behavioral knobs.
fn build_session_options(
    config: &EngineConfig,
    record_subscription_receive_times: bool,
) -> Result<SessionOptions, BlpError> {
    config.transport.validate()?;

    let mut options = SessionOptions::new()?;
    let tls = config.tls.as_ref().map(TlsConfig::build).transpose()?;

    match &config.transport {
        Transport::Direct(servers) => {
            apply_direct_transport(&mut options, servers)?;
            if let Some(tls) = &tls {
                options.set_tls_options(tls);
            }
        }
        Transport::Zfp(remote) => {
            // SDK contract (blpapi_zfputil.h): ZfpUtil::getOptionsForLeasedLines
            // returns SessionOptions "only valid for private leased line
            // connectivity". TLS is bundled into that call; re-applying TLS
            // afterwards is redundant and risks overwriting transport-level
            // flags the SDK may set from `getOptionsForLeasedLines`.
            let tls = tls.as_ref().ok_or_else(|| BlpError::InvalidArgument {
                detail: "zfp_remote requires TLS (tls_client_credentials + tls_trust_material)"
                    .into(),
            })?;
            xbbg_core::zfp::configure_zfp_options(&mut options, tls, *remote)?;
        }
    }

    configure_session_behavior(&mut options, config, record_subscription_receive_times)?;

    Ok(options)
}

fn attach_auth_context(error: BlpError, auth: Option<&AuthConfig>) -> BlpError {
    let Some(auth) = auth else {
        return error;
    };

    match error {
        BlpError::SessionStart { source, label } => {
            let label = match label {
                Some(existing) => {
                    Some(format!("auth_method={} - {}", auth.method_name(), existing))
                }
                None => Some(format!("auth_method={}", auth.method_name())),
            };
            BlpError::SessionStart { source, label }
        }
        other => other,
    }
}

/// Slab key for O(1) correlation dispatch.
pub type SlabKey = usize;

/// Overflow policy for slow consumers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Close with a data-loss error when the buffer is full (default, non-blocking).
    #[default]
    DropNewest,
    /// Wait briefly on a bounded forwarding task. Queue overflow or timeout
    /// closes with a data-loss error; the Bloomberg SDK callback never waits.
    Block,
}

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

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub backoff_factor: f64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            initial_delay_ms: 1000,
            backoff_factor: 2.0,
            max_delay_ms: 30_000,
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

const SUBSCRIPTION_EVENT_HISTORY_LIMIT: usize = 128;

fn timestamp_now_us() -> i64 {
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
    pending_key_to_topic: HashMap<SlabKey, String>,
    metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>>,
    failures: Vec<SubscriptionFailureInfo>,
    topic_states: HashMap<String, TopicStatusInfo>,
    events: VecDeque<SubscriptionEventInfo>,
    session: SessionStatusInfo,
    services: HashMap<String, ServiceStatusInfo>,
    admin: AdminStatusInfo,
}

/// Shared status handle: readers get an ArcSwap snapshot, while writers take a
/// small mutation mutex so all changes for one dispatch path are published as a
/// single snapshot update instead of many independent whole-state RCU clones.
#[derive(Default)]
pub struct SubscriptionStatusHandle {
    snapshot: ArcSwap<SubscriptionStatusState>,
    mutation_lock: ParkingMutex<()>,
}

pub type SharedSubscriptionStatus = Arc<SubscriptionStatusHandle>;

impl SubscriptionStatusHandle {
    pub fn new(initial: SubscriptionStatusState) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(initial),
            mutation_lock: ParkingMutex::new(()),
        }
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<SubscriptionStatusState>> {
        self.snapshot.load()
    }

    pub fn store(&self, next: Arc<SubscriptionStatusState>) {
        let previous = {
            let _guard = self.mutation_lock.lock();
            self.snapshot.swap(next)
        };
        drop(previous);
    }

    pub fn update(&self, mutate: impl FnOnce(&mut SubscriptionStatusState)) {
        self.update_with(|status| {
            mutate(status);
        });
    }

    pub fn update_with<R>(&self, mutate: impl FnOnce(&mut SubscriptionStatusState) -> R) -> R {
        let (result, previous) = {
            let _guard = self.mutation_lock.lock();
            let current = self.snapshot.load_full();
            let mut next = (*current).clone();
            let result = mutate(&mut next);
            let previous = self.snapshot.swap(Arc::new(next));
            (result, previous)
        };
        drop(previous);
        result
    }
}

impl SubscriptionStatusState {
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
        self.topics.retain(|existing| existing != topic);
        self.keys.retain(|existing| *existing != key);
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

    fn finalize_key(&mut self, key: SlabKey) -> Option<String> {
        let topic = self
            .key_to_topic
            .remove(&key)
            .or_else(|| self.pending_key_to_topic.remove(&key))?;
        self.topic_to_key.remove(&topic);
        self.topics.retain(|existing| existing != &topic);
        self.keys.retain(|existing| *existing != key);
        self.metrics.remove(&key);
        Some(topic)
    }

    pub fn push_event(
        &mut self,
        category: SubscriptionEventCategory,
        level: SubscriptionEventLevel,
        message_type: impl Into<String>,
        topic: Option<String>,
        detail: Option<String>,
    ) {
        if self.events.len() >= SUBSCRIPTION_EVENT_HISTORY_LIMIT {
            self.events.pop_front();
        }
        self.events.push_back(SubscriptionEventInfo {
            at_us: timestamp_now_us(),
            category,
            level,
            message_type: message_type.into(),
            topic,
            detail,
        });
    }

    fn update_topic_state(&mut self, topic: &str, state: TopicLifecycleState) {
        let now = timestamp_now_us();
        self.topic_states
            .entry(topic.to_string())
            .and_modify(|status| {
                status.state = state;
                status.last_change_us = now;
            })
            .or_insert_with(|| TopicStatusInfo {
                topic: topic.to_string(),
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
        self.topics.retain(|existing| existing != &topic);
        self.keys.retain(|existing| *existing != key);
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

impl std::str::FromStr for OverflowPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop_newest" | "dropnewest" => Ok(Self::DropNewest),
            "block" => Ok(Self::Block),
            _ => Err(format!(
                "unknown overflow policy '{}': expected 'drop_newest' or 'block'",
                s
            )),
        }
    }
}

impl std::fmt::Display for OverflowPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DropNewest => write!(f, "drop_newest"),
            Self::Block => write!(f, "block"),
        }
    }
}

/// Generic request parameters from Python.
///
/// This unified struct holds all possible Bloomberg request parameters.
/// Not all fields are used for all request types.
#[derive(Clone, Debug, Default)]
pub struct RequestParams {
    /// Bloomberg service URI (e.g., "//blp/refdata")
    pub service: String,
    /// Request operation name (e.g., "ReferenceDataRequest")
    pub operation: String,
    /// Actual Bloomberg operation name when using the RawRequest marker.
    pub request_operation: Option<String>,
    pub request_id: Option<String>,
    /// Extractor type hint for Arrow conversion
    pub extractor: ExtractorType,
    /// Whether extractor was explicitly provided by the caller.
    pub extractor_set: bool,
    /// Multiple securities (for bdp/bdh)
    pub securities: Option<Vec<String>>,
    /// Single security (for intraday)
    pub security: Option<String>,
    /// Fields to retrieve
    pub fields: Option<Vec<String>>,
    /// Global field overrides applied to every security.
    pub overrides: Option<OverridePairs>,
    /// Per-security field overrides. Each entry applies only to matching securities.
    pub security_overrides: Option<SecurityOverridePairs>,
    /// Generic request elements (for BQL expression, bsrch domain, etc.)
    pub elements: Option<Vec<(String, String)>>,
    /// Raw kwargs to route into elements/overrides using schema-driven logic.
    pub kwargs: Option<HashMap<String, String>>,
    /// Start date (YYYYMMDD for bdh)
    pub start_date: Option<String>,
    /// End date (YYYYMMDD for bdh)
    pub end_date: Option<String>,
    /// Start datetime (ISO for intraday)
    pub start_datetime: Option<String>,
    /// End datetime (ISO for intraday)
    pub end_datetime: Option<String>,
    /// How to interpret naive `start_datetime` / `end_datetime` before sending to Bloomberg:
    /// `UTC`, `local`, `exchange`, `NY`/`LN`/… aliases, another ticker (space), or an IANA name.
    pub request_tz: Option<String>,
    /// Relabel Arrow `time` from UTC to this zone (same instants): same tokens as `request_tz`.
    pub output_tz: Option<String>,
    /// Event type (TRADE, BID, ASK for intraday bars - singular)
    pub event_type: Option<String>,
    /// Event types (TRADE, BID, ASK for intraday ticks - array)
    pub event_types: Option<Vec<String>>,
    /// Bar interval in minutes (for bdib)
    pub interval: Option<u32>,
    /// Additional Bloomberg options
    pub options: Option<Vec<(String, String)>>,
    /// Manual field type overrides (for future type resolution)
    pub field_types: Option<HashMap<String, String>>,
    /// Include security error rows in RefData long output when present.
    pub include_security_errors: bool,
    /// Request entitlement IDs (`returnEids`) on reference, historical,
    /// intraday bar, and intraday tick requests; per-security EIDs surface in
    /// the batch metadata under `xbbg.eid_data`.
    pub return_eids: bool,
    /// Optional per-request field validation override.
    ///
    /// - Some(true): force strict field validation for this request
    /// - Some(false): disable field validation for this request
    /// - None: follow engine-level validation_mode
    pub validate_fields: Option<bool>,
    /// Search spec for FieldSearchRequest (//blp/apiflds)
    pub search_spec: Option<String>,
    /// Field IDs for FieldInfoRequest (//blp/apiflds)
    pub field_ids: Option<Vec<String>>,
    /// Output format (long, long_typed, long_metadata, wide)
    pub format: Option<String>,
}

impl RequestParams {
    pub(crate) fn is_raw_request(&self) -> bool {
        matches!(
            parse_operation_lossless(&self.operation),
            Operation::RawRequest
        )
    }

    pub(crate) fn effective_operation(&self) -> &str {
        if self.is_raw_request() {
            self.request_operation.as_deref().unwrap_or_default()
        } else {
            &self.operation
        }
    }

    pub(crate) fn is_excel_get_grid_request(&self) -> bool {
        matches!(
            parse_operation_lossless(self.effective_operation()),
            Operation::ExcelGetGrid
        )
    }

    /// Apply default values derived from operation semantics.
    pub fn with_defaults(mut self) -> Self {
        request_plan::normalize_request_params(&mut self);
        request_plan::apply_request_defaults(&mut self);
        self
    }

    /// Validate request parameters for known Bloomberg operations.
    pub fn validate(&self) -> Result<(), BlpAsyncError> {
        request_plan::validate_request_params(self).map(|_| ())
    }
}
#[derive(Clone, Debug, Default)]
pub struct RequestParamsInput {
    pub service: String,
    pub operation: Option<String>,
    pub request_operation: Option<String>,
    pub request_id: Option<String>,
    pub extractor: Option<String>,
    pub securities: Option<Vec<String>>,
    pub security: Option<String>,
    pub fields: Option<Vec<String>>,
    pub overrides: Option<OverridePairs>,
    pub security_overrides: Option<SecurityOverridePairs>,
    pub elements: Option<Vec<(String, String)>>,
    pub kwargs: Option<HashMap<String, String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_datetime: Option<String>,
    pub end_datetime: Option<String>,
    pub request_tz: Option<String>,
    pub output_tz: Option<String>,
    pub event_type: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub interval: Option<u32>,
    pub options: Option<Vec<(String, String)>>,
    pub field_types: Option<HashMap<String, String>>,
    pub include_security_errors: Option<bool>,
    pub return_eids: Option<bool>,
    pub validate_fields: Option<bool>,
    pub search_spec: Option<String>,
    pub field_ids: Option<Vec<String>>,
    pub format: Option<String>,
}

impl RequestParamsInput {
    pub fn into_request_params(self) -> Result<RequestParams, RequestParamsInputError> {
        let request_operation = normalize_input_string(self.request_operation);
        let operation = match self.operation {
            Some(operation) => operation,
            None if request_operation.is_some() => Operation::RawRequest.to_string(),
            None => {
                return Err(RequestParamsInputError::new(
                    "operation is required unless request_operation is used for RawRequest",
                ))
            }
        };

        let (extractor, extractor_set) = match normalize_input_string(self.extractor) {
            Some(name) => {
                let extractor = ExtractorType::parse(&name).ok_or_else(|| {
                    RequestParamsInputError::new(format!("invalid extractor type: {name}"))
                })?;
                (extractor, true)
            }
            None => (ExtractorType::default(), false),
        };

        let mut service = self.service;
        if service.is_empty() {
            let default_operation = if parse_operation_lossless(&operation) == Operation::RawRequest
            {
                request_operation.as_deref().unwrap_or_default()
            } else {
                operation.as_str()
            };
            if let Some(default_service) =
                parse_operation_lossless(default_operation).default_service()
            {
                service = default_service.to_string();
            }
        }

        let mut params = RequestParams {
            service,
            operation,
            request_operation,
            request_id: self.request_id,
            extractor,
            extractor_set,
            securities: self.securities,
            security: self.security,
            fields: self.fields,
            overrides: self.overrides,
            security_overrides: self.security_overrides,
            elements: self.elements,
            kwargs: self.kwargs,
            start_date: self.start_date,
            end_date: self.end_date,
            start_datetime: self.start_datetime,
            end_datetime: self.end_datetime,
            request_tz: self.request_tz,
            output_tz: self.output_tz,
            event_type: self.event_type,
            event_types: self.event_types,
            interval: self.interval,
            options: self.options,
            field_types: self.field_types,
            include_security_errors: self.include_security_errors.unwrap_or(false),
            return_eids: self.return_eids.unwrap_or(false),
            validate_fields: self.validate_fields,
            search_spec: self.search_spec,
            field_ids: self.field_ids,
            format: self.format,
        };
        request_plan::normalize_request_params(&mut params);
        request_plan::apply_request_defaults(&mut params);
        Ok(params)
    }
}

fn normalize_input_string(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestParamsInputError {
    detail: String,
}

impl RequestParamsInputError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for RequestParamsInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for RequestParamsInputError {}

/// Validation mode for request validation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValidationMode {
    /// Error on invalid fields/requests
    Strict,
    /// Warn but still send request
    Lenient,
    /// Skip validation entirely (default)
    #[default]
    Disabled,
}

impl std::str::FromStr for ValidationMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(Self::Strict),
            "lenient" => Ok(Self::Lenient),
            "disabled" | "off" | "none" => Ok(Self::Disabled),
            _ => Err(format!(
                "unknown validation mode '{}': expected strict, lenient, or disabled",
                s
            )),
        }
    }
}

impl std::fmt::Display for ValidationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict => write!(f, "strict"),
            Self::Lenient => write!(f, "lenient"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// Configuration for the Engine.
#[derive(Clone)]
pub struct EngineConfig {
    /// How sessions reach Bloomberg — direct TCP (optionally with per-server
    /// SOCKS5) or ZFP leased lines. See [`Transport`].
    pub transport: Transport,
    /// Max event queue size (Bloomberg SDK setting)
    pub max_event_queue_size: usize,
    /// Command channel capacity (backpressure)
    pub command_queue_size: usize,
    /// Subscription flush threshold (rows before auto-flush)
    pub subscription_flush_threshold: usize,
    /// Subscription stream capacity (backpressure)
    pub subscription_stream_capacity: usize,
    /// Overflow policy for slow consumers
    pub overflow_policy: OverflowPolicy,
    /// Number of request workers (default: 2)
    pub request_pool_size: usize,
    /// Number of Tokio runtime worker threads used by the engine (default: 2).
    pub runtime_worker_threads: usize,
    /// Number of pre-warmed subscription sessions (default: 1)
    pub subscription_pool_size: usize,
    /// Maximum live subscription sessions, including checked-out sessions
    /// (default: 32). `subscription_pool_size` is only the pre-warm count.
    pub max_subscription_sessions: usize,
    /// Enable request sharding for eligible multi-security reference/history requests.
    /// Default: false (opt-in).
    pub shard_requests: bool,
    /// Minimum number of securities before an eligible request is sharded.
    /// Default: 20.
    pub shard_threshold: usize,
    /// Maximum securities per shard when request sharding is enabled.
    /// Default: 16.
    pub shard_chunk_size: usize,
    /// Maximum in-flight shard requests per user request.
    /// Default: 4.
    pub shard_max_concurrent: usize,
    /// Services to pre-warm on request workers
    pub warmup_services: Vec<String>,
    /// Validation mode for requests (default: Strict)
    pub validation_mode: ValidationMode,
    /// Custom path for the field cache JSON file (default: ~/.xbbg/field_cache.json)
    pub field_cache_path: Option<std::path::PathBuf>,
    /// Structured Bloomberg session auth configuration.
    pub auth: Option<AuthConfig>,
    /// Optional TLS material. Required for `Transport::Zfp`; optional for
    /// `Transport::Direct` when connecting to B-PIPE over TLS.
    pub tls: Option<TlsConfig>,
    /// Number of times the SDK will attempt to connect before giving up.
    pub num_start_attempts: usize,
    /// Whether the SDK should auto-restart the session after disconnection.
    pub auto_restart_on_disconnection: bool,
    /// Retry policy for transient request failures (default: no retry).
    pub retry_policy: RetryPolicy,
    /// Hard per-request timeout in ms. Workers cancel the Bloomberg request and
    /// fail the oneshot if no response arrives in this window. Guarantees that
    /// a request cannot hang forever even if Bloomberg or the SDK misbehaves.
    /// Default: 0 (disabled) — callers must opt in by setting a non-zero value.
    /// Large historical requests (e.g. full-day `bdtick`) routinely exceed any
    /// fixed bound, so the library does not impose one by default.
    pub request_timeout_ms: u64,
    /// If a topic's subscription streams have been deactivated for more than
    /// this many ms without reactivation, emit a one-shot escalated Warning
    /// event. The SDK (v3.11.6+) is still trying to recover; this is a hint
    /// to callers who poll status that their data is quiet, not dead. Set to
    /// 0 to disable. Default: 30_000 (30s).
    pub streams_deactivated_warn_ms: u64,
    /// Bloomberg SDK internal log level. Bridges SDK logs into xbbg tracing.
    /// Must be set before first session starts. Default: Off.
    pub sdk_log_level: crate::sdk_logging::SdkLogLevel,
    /// Enable BLPAPI keep-alive pings. SDK default: true.
    pub keep_alive_enabled: bool,
    /// Milliseconds of inactivity before the keep-alive ping is sent. When
    /// `None`, the SDK default (20_000 = 20s) is left in place. Raise this
    /// for laggy VPN/WAN connections where the aggressive 30s total window
    /// (20s inactivity + 10s response) causes spurious `SessionConnectionDown`.
    pub keep_alive_inactivity_ms: Option<i32>,
    /// Milliseconds to wait for a keep-alive response before declaring the
    /// connection dead. When `None`, the SDK default (10_000 = 10s) is used.
    pub keep_alive_response_timeout_ms: Option<i32>,
    /// Hi water mark for the "slow consumer warning" event, as a fraction of
    /// `max_event_queue_size` (0.0..=1.0). SDK default 0.75. When `None`,
    /// the SDK default is kept.
    pub slow_consumer_hi_water_mark: Option<f32>,
    /// Lo water mark for the "slow consumer warning cleared" event, as a
    /// fraction of `max_event_queue_size` (0.0..1.0). SDK default 0.5. When
    /// `None`, the SDK default is kept. Must be strictly less than
    /// `slow_consumer_hi_water_mark`.
    pub slow_consumer_lo_water_mark: Option<f32>,
}

impl EngineConfig {
    pub fn validate(&self) -> Result<(), BlpAsyncError> {
        if self.request_pool_size == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "request_pool_size must be greater than zero".to_string(),
            });
        }
        if self.runtime_worker_threads == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "runtime_worker_threads must be greater than zero".to_string(),
            });
        }
        if self.max_subscription_sessions == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "max_subscription_sessions must be greater than zero".to_string(),
            });
        }
        if self.subscription_pool_size > self.max_subscription_sessions {
            return Err(BlpAsyncError::ConfigError {
                detail: "max_subscription_sessions must be greater than or equal to subscription_pool_size".to_string(),
            });
        }
        if self.command_queue_size == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "command_queue_size must be greater than zero".to_string(),
            });
        }
        if self.subscription_stream_capacity == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription_stream_capacity must be greater than zero".to_string(),
            });
        }
        if self.shard_threshold < 2 {
            return Err(BlpAsyncError::ConfigError {
                detail: "shard_threshold must be at least 2".to_string(),
            });
        }
        if self.shard_chunk_size == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "shard_chunk_size must be greater than zero".to_string(),
            });
        }
        if self.shard_max_concurrent == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "shard_max_concurrent must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            transport: Transport::default_direct(),
            tls: None,
            max_event_queue_size: 10_000,
            command_queue_size: 256,
            subscription_flush_threshold: 1,
            subscription_stream_capacity: 256,
            overflow_policy: OverflowPolicy::default(),
            request_pool_size: 2,
            runtime_worker_threads: 2,
            subscription_pool_size: 1,
            max_subscription_sessions: 32,
            shard_requests: false,
            shard_threshold: 20,
            shard_chunk_size: 16,
            shard_max_concurrent: 4,
            warmup_services: vec![
                crate::services::Service::RefData.to_string(),
                crate::services::Service::ApiFlds.to_string(),
            ],
            validation_mode: ValidationMode::default(),
            field_cache_path: None,
            auth: None,
            num_start_attempts: 3,
            auto_restart_on_disconnection: true,
            retry_policy: RetryPolicy::default(),
            request_timeout_ms: 0,
            streams_deactivated_warn_ms: 30_000,
            keep_alive_enabled: true,
            keep_alive_inactivity_ms: None,
            keep_alive_response_timeout_ms: None,
            slow_consumer_hi_water_mark: None,
            slow_consumer_lo_water_mark: None,
            sdk_log_level: crate::sdk_logging::SdkLogLevel::Off,
        }
    }
}
fn shard_security_chunks(securities: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    if chunk_size == 0 {
        return Vec::new();
    }
    securities
        .chunks(chunk_size)
        .map(<[String]>::to_vec)
        .collect()
}

fn merge_override_pairs(
    global: Option<&OverridePairs>,
    security: Option<&OverridePairs>,
) -> Option<OverridePairs> {
    let capacity = global.map_or(0, Vec::len) + security.map_or(0, Vec::len);
    if capacity == 0 {
        return None;
    }

    let mut merged = Vec::with_capacity(capacity);
    if let Some(global) = global {
        merged.extend(global.iter().cloned());
    }
    if let Some(security) = security {
        for (key, value) in security {
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
    }

    Some(merged)
}

fn push_security_override_shard(
    shards: &mut Vec<PreparedRequest>,
    prepared: &PreparedRequest,
    securities: &mut Vec<String>,
    security_overrides: Option<&OverridePairs>,
) {
    if securities.is_empty() {
        return;
    }
    let merged_overrides =
        merge_override_pairs(prepared.params().overrides.as_ref(), security_overrides);
    shards.push(prepared.for_security_shard(std::mem::take(securities), merged_overrides));
}

fn security_override_shards(
    config: &EngineConfig,
    prepared: &PreparedRequest,
) -> Option<Vec<PreparedRequest>> {
    let security_overrides = prepared
        .params()
        .security_overrides
        .as_ref()
        .filter(|entries| !entries.is_empty())?;
    let securities = prepared.params().securities.as_ref()?;
    if securities.is_empty() {
        return None;
    }

    let lookup: HashMap<&str, &OverridePairs> = security_overrides
        .iter()
        .map(|(security, overrides)| (security.as_str(), overrides))
        .collect();
    let max_chunk = if config.shard_requests {
        config.shard_chunk_size
    } else {
        usize::MAX
    };

    let mut shards = Vec::new();
    let mut current_securities = Vec::new();
    let mut current_overrides: Option<&OverridePairs> = None;
    let mut have_current = false;

    for security in securities {
        let next_overrides = lookup.get(security.as_str()).copied();
        if have_current
            && (current_overrides != next_overrides || current_securities.len() >= max_chunk)
        {
            push_security_override_shard(
                &mut shards,
                prepared,
                &mut current_securities,
                current_overrides,
            );
            have_current = false;
        }
        if !have_current {
            current_overrides = next_overrides;
            have_current = true;
        }
        current_securities.push(security.clone());
    }

    if have_current {
        push_security_override_shard(
            &mut shards,
            prepared,
            &mut current_securities,
            current_overrides,
        );
    }

    Some(shards)
}

fn sharded_requests(
    config: &EngineConfig,
    prepared: &PreparedRequest,
) -> Option<Vec<PreparedRequest>> {
    if prepared.is_raw() {
        return None;
    }
    if !matches!(
        prepared.operation(),
        Operation::ReferenceData | Operation::HistoricalData
    ) {
        return None;
    }
    if !matches!(
        prepared.shape(),
        PlannedRequestShape::RefData(_) | PlannedRequestShape::HistData(_)
    ) {
        return None;
    }
    if let Some(shards) = security_override_shards(config, prepared) {
        return Some(shards);
    }
    if !config.shard_requests {
        return None;
    }

    let securities = prepared.params().securities.as_ref()?;
    if securities.is_empty() || securities.len() < config.shard_threshold {
        return None;
    }

    let chunks = shard_security_chunks(securities, config.shard_chunk_size);
    if chunks.len() < 2 {
        return None;
    }
    Some(
        chunks
            .into_iter()
            .map(|securities| {
                prepared.for_security_shard(securities, prepared.params().overrides.clone())
            })
            .collect(),
    )
}

fn concat_sharded_batches(batches: Vec<RecordBatch>) -> Result<RecordBatch, BlpAsyncError> {
    let Some((first, rest)) = batches.split_first() else {
        return Err(BlpAsyncError::Internal(
            "cannot concatenate zero sharded batches".to_string(),
        ));
    };
    if rest.is_empty() {
        return Ok(first.clone());
    }

    // Concatenation keeps only `target_schema`'s metadata; union the
    // response diagnostics (eidData / securityError / fieldExceptions)
    // across shards so later shards' entries are not silently dropped.
    let merged_meta = state::ResponseMetadata::union_of(&batches);

    let target_schema = first.schema_ref().clone();
    let mut normalized = Vec::with_capacity(batches.len());
    normalized.push(first.clone());
    for batch in rest {
        normalized.push(normalize_batch_to_schema(batch.clone(), &target_schema)?);
    }
    arrow_select::concat::concat_batches(&target_schema, normalized.iter())
        .map(|batch| merged_meta.attach(batch))
        .map_err(|err| {
            BlpAsyncError::Internal(format!("concatenate sharded request batches: {err}"))
        })
}

fn normalize_batch_to_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, BlpAsyncError> {
    if batch.schema_ref().as_ref() == schema.as_ref() {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(BlpAsyncError::Internal(
            "sharded request produced incompatible column count".to_string(),
        ));
    }

    let batch_schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, target_field) in schema.fields().iter().enumerate() {
        let source_field = batch_schema.field(idx);
        let expected = target_field.name();
        let actual = source_field.name();
        if actual != expected {
            return Err(BlpAsyncError::Internal(format!(
                "sharded request column mismatch at index {idx}: expected {expected}, got {actual}"
            )));
        }

        let array = batch.column(idx);
        if array.data_type() == target_field.data_type() {
            columns.push(array.clone());
        } else if array.null_count() == array.len() {
            columns.push(null_array_for_datatype(
                target_field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            return Err(BlpAsyncError::Internal(format!(
                "sharded request column type mismatch for {expected}: expected {:?}, got {:?}",
                target_field.data_type(),
                array.data_type()
            )));
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(|err| {
        BlpAsyncError::Internal(format!("concatenate sharded request batches: {err}"))
    })
}

fn null_array_for_datatype(data_type: &DataType, len: usize) -> Result<ArrayRef, BlpAsyncError> {
    let arrow_type = match data_type {
        DataType::Utf8 => ArrowType::String,
        DataType::Float64 => ArrowType::Float64,
        DataType::Int64 => ArrowType::Int64,
        DataType::Int32 => ArrowType::Int32,
        DataType::Boolean => ArrowType::Bool,
        DataType::Date32 => ArrowType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            ArrowType::TimestampMicros
        }
        DataType::Time64(TimeUnit::Microsecond) => ArrowType::Time64Micros,
        _ => {
            return Err(BlpAsyncError::Internal(format!(
                "cannot build null shard column for Arrow type {data_type:?}"
            )));
        }
    };

    let mut builder = TypedBuilder::new(arrow_type);
    for _ in 0..len {
        builder.append_null();
    }
    Ok(builder.finish())
}

/// Worker Pool Bloomberg Engine.
///
/// Uses pre-warmed worker pools for efficient request handling:
/// - RequestWorkerPool: Handles all request types with round-robin dispatch
/// - SubscriptionSessionPool: Provides isolated sessions for subscriptions
pub struct Engine {
    /// Pool of request workers
    request_pool: RequestWorkerPool,
    /// Pool of subscription sessions
    subscription_pool: Arc<SubscriptionSessionPool>,
    /// Tokio runtime for async ops.
    ///
    /// `Some` for the entire public lifetime of the Engine; cleared only inside
    /// `Drop`, which needs to own the runtime to tear it down without blocking.
    rt: Option<Arc<tokio::runtime::Runtime>>,
    /// Configuration
    config: Arc<EngineConfig>,
    /// Schema cache (in-memory + disk)
    schema_cache: crate::schema::SchemaCache,
    /// Exchange metadata cache (in-memory + disk)
    exchange_cache: ExchangeCache,
    /// Broadcast shutdown signal for data-path consumers (e.g. PySubscription).
    shutdown_signal: watch::Sender<bool>,
}

impl Engine {
    /// Create and start a new Engine with worker pools.
    pub fn start(config: EngineConfig) -> Result<Self, BlpAsyncError> {
        crate::sdk_logging::register_sdk_logging(config.sdk_log_level);
        config.validate()?;

        let config = Arc::new(config);

        let field_resolver =
            crate::field_cache::init_global_resolver(config.field_cache_path.clone());
        field_resolver.preload();

        xbbg_log::info!(
            request_pool_size = config.request_pool_size,
            subscription_pool_size = config.subscription_pool_size,
            max_subscription_sessions = config.max_subscription_sessions,
            runtime_worker_threads = config.runtime_worker_threads,
            "starting Engine with worker pools"
        );

        // Create request worker pool
        let request_pool = RequestWorkerPool::new(config.request_pool_size, config.clone())?;

        // Create subscription session pool
        let subscription_pool = Arc::new(SubscriptionSessionPool::new(
            config.subscription_pool_size,
            config.clone(),
        )?);

        let total_sessions = config.request_pool_size + config.subscription_pool_size;
        xbbg_log::info!(
            request_workers = config.request_pool_size,
            subscription_workers = config.subscription_pool_size,
            total_bloomberg_sessions = total_sessions,
            transport = %config.transport,
            "Engine ready"
        );

        // Built last, after every fallible step above. A tokio Runtime cannot be
        // dropped from inside an async context, so creating it earlier meant any
        // `?` here dropped it on an async caller's thread and replaced the real
        // error with tokio's "Cannot drop a runtime in a context where blocking is
        // not allowed" panic — masking, for example, a refused Bloomberg
        // connection behind an unrelated runtime message.
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.runtime_worker_threads)
                .enable_all()
                .build()
                .map_err(|e| BlpAsyncError::Internal(format!("tokio runtime: {e}")))?,
        );
        subscription_pool.attach_runtime(rt.handle().clone());

        let (shutdown_signal, _) = watch::channel(false);

        let exchange_cache = ExchangeCache::new();
        if let Err(e) = exchange_cache.preload() {
            xbbg_log::warn!(error = %e, "failed to preload exchange cache");
        }

        Ok(Self {
            request_pool,
            subscription_pool,
            rt: Some(rt),
            config,
            schema_cache: crate::schema::SchemaCache::new(),
            exchange_cache,
            shutdown_signal,
        })
    }

    // ─── Generic Request API ─────────────────────────────────────────────────

    /// Generic Bloomberg request - dispatches to worker pool.
    ///
    /// All request types are handled by the same pool of workers.
    /// Dispatch a prepared request without intraday timezone transforms.
    ///
    /// Used for nested RefData calls (e.g. exchange metadata) so `request_tz=exchange` does not
    /// recurse into [`Engine::request`].
    pub(crate) async fn request_without_intraday_transform(
        &self,
        params: RequestParams,
    ) -> Result<RecordBatch, BlpAsyncError> {
        let prepared = self.prepare_request_builder(params)?.finalize()?;
        self.maybe_validate_request_fields(&prepared).await?;
        self.request_pool.request(prepared).await
    }
    async fn request_shards_ordered(
        &self,
        shards: Vec<PreparedRequest>,
    ) -> Result<RecordBatch, BlpAsyncError> {
        let operation = shards
            .first()
            .map(|request| request.operation().as_str())
            .unwrap_or("unknown");
        let security_counts: Vec<usize> = shards
            .iter()
            .map(|request| request.params().securities.as_ref().map_or(0, Vec::len))
            .collect();
        xbbg_log::debug!(
            operation = operation,
            shard_count = shards.len(),
            max_concurrent = self.config.shard_max_concurrent,
            security_counts = ?security_counts,
            "dispatching sharded request"
        );

        let results = stream::iter(shards)
            .map(|request| async move { self.request_pool.request(request).await })
            .buffered(self.config.shard_max_concurrent)
            .collect::<Vec<_>>()
            .await;

        let mut batches = Vec::with_capacity(results.len());
        for result in results {
            batches.push(result?);
        }
        concat_sharded_batches(batches)
    }

    pub async fn request(&self, params: RequestParams) -> Result<RecordBatch, BlpAsyncError> {
        let mut builder = self.prepare_request_builder(params)?;
        self.apply_intraday_request_timezone(&mut builder).await?;
        let prepared = builder.finalize()?;
        self.maybe_validate_request_fields(&prepared).await?;
        let output_params = prepared.params().clone();
        let batch = if let Some(shards) = sharded_requests(self.config.as_ref(), &prepared) {
            self.request_shards_ordered(shards).await?
        } else {
            self.request_pool.request(prepared).await?
        };
        intraday_timezone::apply_intraday_output_timezone(self, batch, &output_params).await
    }

    /// Streaming generic request - dispatches to worker pool.
    ///
    /// The returned [`RequestStream`] owns cancellation of the Bloomberg
    /// request. Dropping or closing it cancels unfinished work.
    pub async fn request_stream(
        &self,
        params: RequestParams,
    ) -> Result<RequestStream, BlpAsyncError> {
        let mut builder = self.prepare_request_builder(params)?;
        self.apply_intraday_request_timezone(&mut builder).await?;
        let out_iana = intraday_timezone::resolve_output_tz_iana(self, builder.params()).await?;
        let prepared = builder.finalize()?;
        self.maybe_validate_request_fields(&prepared).await?;
        let stream = self.request_pool.request_stream(prepared).await?;
        Ok(stream.with_output_timezone(out_iana))
    }

    /// Resolve defaults, validate, schema-route kwargs, and apply field-cache hints.
    fn prepare_request_builder(
        &self,
        params: RequestParams,
    ) -> Result<PreparedRequestBuilder, BlpAsyncError> {
        let mut builder = PreparedRequestBuilder::prepare(params, &self.schema_cache)?;
        self.apply_cached_field_types(&mut builder)?;
        Ok(builder)
    }

    fn apply_cached_field_types(
        &self,
        builder: &mut PreparedRequestBuilder,
    ) -> Result<(), BlpAsyncError> {
        if !matches!(
            builder.shape()?,
            PlannedRequestShape::RefData(_) | PlannedRequestShape::HistData(_)
        ) {
            return Ok(());
        }

        let params = builder.params();
        let Some(fields) = params.fields.as_ref().filter(|fields| !fields.is_empty()) else {
            return Ok(());
        };

        let resolved = crate::field_cache::global_resolver()
            .resolve_cached_types(fields, params.field_types.as_ref());
        if !resolved.is_empty() {
            let added = params
                .field_types
                .as_ref()
                .map_or(resolved.len(), |existing| {
                    resolved.len().saturating_sub(existing.len())
                });
            if added > 0 {
                xbbg_log::debug!(field_count = added, "using cached field type hints");
            }
            builder.set_field_types(resolved);
        }
        Ok(())
    }

    async fn apply_intraday_request_timezone(
        &self,
        builder: &mut PreparedRequestBuilder,
    ) -> Result<(), BlpAsyncError> {
        let Some((start_datetime, end_datetime)) =
            intraday_timezone::resolve_intraday_request_datetimes(self, builder.params()).await?
        else {
            return Ok(());
        };
        builder.set_intraday_datetimes(start_datetime, end_datetime);
        Ok(())
    }

    /// Validate request fields against Bloomberg field metadata when enabled.
    async fn maybe_validate_request_fields(
        &self,
        prepared: &PreparedRequest,
    ) -> Result<(), BlpAsyncError> {
        let params = prepared.params();
        let validation_mode = match params.validate_fields {
            Some(true) => ValidationMode::Strict,
            Some(false) => ValidationMode::Disabled,
            None => self.config.validation_mode,
        };

        if validation_mode == ValidationMode::Disabled {
            return Ok(());
        }

        if prepared.is_raw() {
            return Ok(());
        }

        if params.service != Service::RefData.to_string() {
            return Ok(());
        }

        let operation = prepared.operation();
        if !matches!(
            operation,
            Operation::ReferenceData | Operation::HistoricalData
        ) {
            return Ok(());
        }

        let Some(fields) = params.fields.as_ref() else {
            return Ok(());
        };
        if fields.is_empty() {
            return Ok(());
        }

        let invalid_fields = self.validate_fields(fields).await?;
        if invalid_fields.is_empty() {
            return Ok(());
        }

        let detail = format!("Unknown Bloomberg field(s): {}", invalid_fields.join(", "));
        if validation_mode == ValidationMode::Lenient {
            xbbg_log::warn!(
                service = %params.service,
                operation = %prepared.effective_operation(),
                invalid_fields = ?invalid_fields,
                "field validation warning"
            );
            return Ok(());
        }

        Err(BlpAsyncError::ConfigError { detail })
    }

    // ─── Subscriptions ───────────────────────────────────────────────────────

    /// Subscribe to real-time market data (//blp/mktdata).
    ///
    /// Claims a dedicated session from the pool for this subscription.
    /// Returns a `SubscriptionStream` that provides:
    /// - Async iteration over incoming data
    /// - Dynamic add/remove of tickers
    /// - Explicit unsubscribe with optional drain
    ///
    /// The session is returned to the pool when the stream is dropped.
    pub async fn subscribe(
        &self,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
    ) -> Result<SubscriptionStream, BlpAsyncError> {
        self.subscribe_with_options(
            crate::services::Service::MktData.to_string(),
            topics,
            fields,
            all_fields,
            vec![],
            None,
            None,
            None,
        )
        .await
    }

    /// Subscribe to real-time data with custom service and options.
    ///
    /// This is the generic subscription method that supports different services
    /// (e.g., //blp/mktdata, //blp/mktvwap) and subscription options.
    ///
    /// # Arguments
    /// * `service` - Bloomberg service (e.g., "//blp/mktdata", "//blp/mktvwap")
    /// * `topics` - Securities to subscribe to
    /// * `fields` - Fields to subscribe to
    /// * `options` - Subscription options (e.g., ["VWAP_START_TIME=09:30"])
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_with_options(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        stream_capacity: Option<usize>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
    ) -> Result<SubscriptionStream, BlpAsyncError> {
        let capacity = stream_capacity.unwrap_or(self.config.subscription_stream_capacity);
        if capacity == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription stream capacity must be greater than zero".to_string(),
            });
        }
        let (tx, rx) = subscription_channel(capacity);
        let status = Arc::new(SubscriptionStatusHandle::new(
            SubscriptionStatusState::default(),
        ));

        // Admission waits without consuming a runtime worker. Session startup
        // itself moves to the blocking pool after a bounded permit is acquired.
        let mut claim = self.subscription_pool.claim().await?;

        // Start the subscription
        claim
            .subscribe(
                service.clone(),
                topics,
                fields.clone(),
                all_fields,
                options.clone(),
                flush_threshold,
                overflow_policy,
                tx.clone(),
                status.clone(),
            )
            .await?;
        claim.set_cleanup_status(status.clone());

        let stream = SubscriptionStream {
            rx,
            tx,
            claim: Some(claim),
            fields,
            all_fields,
            service,
            options,
            status,
            flush_threshold,
            overflow_policy,
        };

        Ok(stream)
    }

    // ─── Field Type Resolution ──────────────────────────────────────────────

    /// Resolve field types for a list of fields.
    ///
    /// This queries //blp/apiflds for any fields not already in the cache,
    /// updates the cache, and returns a HashMap of field -> arrow_type_string.
    pub async fn resolve_field_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
        default_type: &str,
    ) -> Result<HashMap<String, String>, BlpAsyncError> {
        use crate::field_cache::global_resolver;

        let resolver = global_resolver();

        // Find fields not in cache (and not manually overridden)
        let uncached: Vec<String> = fields
            .iter()
            .filter(|f| {
                if let Some(overrides) = manual_overrides {
                    if overrides.contains_key(*f) || overrides.contains_key(&f.to_uppercase()) {
                        return false;
                    }
                }
                resolver.get(f).is_none()
            })
            .cloned()
            .collect();

        // Query //blp/apiflds for uncached fields
        if !uncached.is_empty() {
            xbbg_log::debug!(fields = ?uncached, "Querying //blp/apiflds for field types");

            let params = RequestParams {
                service: crate::services::Service::ApiFlds.to_string(),
                operation: "FieldInfoRequest".to_string(),
                extractor: ExtractorType::FieldInfo,
                field_ids: Some(uncached.clone()),
                ..Default::default()
            };

            match self.request(params).await {
                Ok(batch) => {
                    resolver.insert_from_response(&batch);

                    let (_, cache_path) = resolver.stats();
                    let resolver_clone = resolver.clone();
                    match self
                        .runtime()
                        .spawn_blocking(move || resolver_clone.save_to_disk())
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            xbbg_log::warn!(
                                path = %cache_path.display(),
                                error = %error,
                                "failed to persist field cache"
                            );
                        }
                        Err(error) => {
                            xbbg_log::warn!(
                                path = %cache_path.display(),
                                error = %error,
                                "field cache save task failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    xbbg_log::warn!(error = %e, "Failed to query field types, using defaults");
                }
            }
        }

        Ok(resolver.resolve_types(fields, manual_overrides, default_type))
    }

    /// Pre-populate the field type cache for a list of fields.
    pub async fn cache_field_types(&self, fields: &[String]) -> Result<(), BlpAsyncError> {
        let _ = self.resolve_field_types(fields, None, "string").await?;
        Ok(())
    }

    /// Get field info from cache (doesn't query API).
    pub fn get_field_info(&self, field: &str) -> Option<crate::field_cache::FieldInfo> {
        crate::field_cache::global_resolver().get(field)
    }

    /// Clear the field type cache.
    pub fn clear_field_cache(&self) -> Result<(), String> {
        crate::field_cache::global_resolver().clear()
    }

    /// Save the field type cache to disk.
    pub fn save_field_cache(&self) -> Result<(), String> {
        crate::field_cache::global_resolver().save_to_disk()
    }

    /// Get field cache statistics including the active cache file path.
    pub fn field_cache_stats(&self) -> (usize, std::path::PathBuf) {
        crate::field_cache::global_resolver().stats()
    }

    /// Validate Bloomberg field names.
    ///
    /// Queries `//blp/apiflds` for the given fields and returns a list of
    /// invalid field names (fields that Bloomberg doesn't recognize).
    ///
    /// # Example
    /// ```ignore
    /// let invalid = engine.validate_fields(&["PX_LAST", "INVALID_FIELD"]).await?;
    /// // invalid = ["INVALID_FIELD"]
    /// ```
    pub async fn validate_fields(&self, fields: &[String]) -> Result<Vec<String>, BlpAsyncError> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }

        // Query //blp/apiflds for the fields
        let params = RequestParams {
            service: crate::services::Service::ApiFlds.to_string(),
            operation: "FieldInfoRequest".to_string(),
            extractor: ExtractorType::FieldInfo,
            field_ids: Some(fields.to_vec()),
            ..Default::default()
        };

        let params = self.prepare_request_builder(params)?.finalize()?;
        let batch = self.request_pool.request(params).await?;

        // Get the field column from the response
        let field_col = batch
            .column_by_name("field")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());

        let valid_fields: std::collections::HashSet<String> = match field_col {
            Some(col) => (0..col.len())
                .filter_map(|i| {
                    if col.is_null(i) {
                        None
                    } else {
                        Some(col.value(i).to_uppercase())
                    }
                })
                .collect(),
            None => std::collections::HashSet::new(),
        };

        // Find fields that weren't returned (invalid)
        let invalid: Vec<String> = fields
            .iter()
            .filter(|f| !valid_fields.contains(&f.to_uppercase()))
            .cloned()
            .collect();

        Ok(invalid)
    }

    /// Check if field validation is enabled based on validation mode.
    pub fn is_field_validation_enabled(&self) -> bool {
        self.config.validation_mode != ValidationMode::Disabled
    }

    // ─── Schema Introspection ─────────────────────────────────────────────────

    /// Get the schema for a Bloomberg service.
    ///
    /// Checks the cache first; if not cached, introspects the service via a worker
    /// and caches the result both in memory and on disk.
    pub async fn get_schema(
        &self,
        service: &str,
    ) -> Result<Arc<crate::schema::ServiceSchema>, BlpAsyncError> {
        if let Some(schema) = self.schema_cache.get_memory(service) {
            return Ok(schema);
        }
        let _load_guard = self.schema_cache.lock_load().await;
        if let Some(schema) = self.schema_cache.get_memory(service) {
            return Ok(schema);
        }

        let cache_for_load = self.schema_cache.clone();
        let service_for_load = service.to_string();
        match self
            .runtime()
            .spawn_blocking(move || cache_for_load.get(&service_for_load))
            .await
        {
            Ok(Some(schema)) => return Ok(schema),
            Ok(None) => {}
            Err(error) => {
                xbbg_log::warn!(service, error = %error, "schema cache load task failed");
            }
        }

        let schema = self
            .request_pool
            .introspect_schema(service.to_string())
            .await?;

        let cache_dir = self.schema_cache.cache_dir();
        let cache_for_insert = self.schema_cache.clone();
        let service_for_insert = service.to_string();
        let schema_for_insert = schema.clone();
        match self
            .runtime()
            .spawn_blocking(move || cache_for_insert.insert(&service_for_insert, schema_for_insert))
            .await
        {
            Ok(Ok(schema)) => Ok(schema),
            Ok(Err(error)) => {
                xbbg_log::warn!(
                    service,
                    path = %cache_dir.display(),
                    error = %error,
                    "failed to persist schema cache"
                );
                Ok(self
                    .schema_cache
                    .get_memory(service)
                    .unwrap_or_else(|| Arc::new(schema)))
            }
            Err(error) => {
                xbbg_log::warn!(
                    service,
                    path = %cache_dir.display(),
                    error = %error,
                    "schema cache insert task failed"
                );
                Ok(self
                    .schema_cache
                    .get_memory(service)
                    .unwrap_or_else(|| self.schema_cache.insert_memory(service, schema)))
            }
        }
    }

    /// Get a specific operation's schema from a service.
    ///
    /// This is a convenience method that gets the full service schema and
    /// extracts the requested operation.
    pub async fn get_operation(
        &self,
        service: &str,
        operation: &str,
    ) -> Result<crate::schema::OperationSchema, BlpAsyncError> {
        let schema = self.get_schema(service).await?;

        schema
            .get_operation(operation)
            .cloned()
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: format!(
                    "Operation '{}' not found in service '{}'",
                    operation, service
                ),
            })
    }

    /// List all operations for a service.
    pub async fn list_operations(&self, service: &str) -> Result<Vec<String>, BlpAsyncError> {
        let schema = self.get_schema(service).await?;
        Ok(schema.operation_names())
    }

    /// Get a schema already loaded in memory without triggering introspection or disk I/O.
    ///
    /// Returns None if the schema has not been loaded into the in-memory cache.
    pub fn get_cached_schema(&self, service: &str) -> Option<Arc<crate::schema::ServiceSchema>> {
        self.schema_cache.get_memory(service)
    }

    /// Invalidate a cached schema (removes from memory and disk).
    pub fn invalidate_schema(&self, service: &str) -> Result<(), String> {
        self.schema_cache.invalidate(service)
    }

    /// Clear all cached schemas.
    pub fn clear_schema_cache(&self) -> Result<(), String> {
        self.schema_cache.clear()
    }

    /// List all cached service URIs.
    pub fn list_cached_schemas(&self) -> Vec<String> {
        self.schema_cache.list()
    }

    /// Get valid enum values for a request element.
    ///
    /// Returns None if the element is not an enum or doesn't exist.
    pub async fn get_enum_values(
        &self,
        service: &str,
        operation: &str,
        element: &str,
    ) -> Result<Option<Vec<String>>, BlpAsyncError> {
        let op_schema = self.get_operation(service, operation).await?;
        Ok(op_schema.find_request_enum_values(element))
    }

    /// List all valid element names for a request.
    ///
    /// Returns None if the operation doesn't exist.
    pub async fn list_valid_elements(
        &self,
        service: &str,
        operation: &str,
    ) -> Result<Option<Vec<String>>, BlpAsyncError> {
        let op_schema = self.get_operation(service, operation).await?;
        Ok(Some(op_schema.request_element_names()))
    }

    // ─── Pool Info ──────────────────────────────────────────────────────────

    /// Get the number of request workers.
    pub fn request_worker_count(&self) -> usize {
        self.request_pool.size()
    }

    /// Get the number of available subscription sessions.
    pub fn available_subscription_sessions(&self) -> usize {
        self.subscription_pool.available_count()
    }

    // ─── Admin ───────────────────────────────────────────────────────────────

    /// Signal shutdown to all workers (non-blocking).
    ///
    /// Workers will terminate when they see the shutdown signal.
    /// Used by Drop and Python atexit to avoid blocking.
    pub fn signal_shutdown(&self) {
        xbbg_log::info!("Engine signal_shutdown requested");
        let _ = self.shutdown_signal.send(true);
        self.request_pool.signal_shutdown();
        self.subscription_pool.signal_shutdown();
    }

    /// Graceful shutdown - waits for all workers to finish (blocking).
    ///
    /// Use this for clean shutdown when you can afford to wait.
    /// Consumes the Engine.
    pub fn shutdown_blocking(mut self) {
        xbbg_log::info!("Engine shutdown_blocking requested");
        let _ = self.shutdown_signal.send(true);
        self.request_pool.shutdown_blocking();
        self.subscription_pool.shutdown_blocking();
    }

    /// Get a receiver that fires when shutdown is signaled.
    ///
    /// This signals a request, not completed terminal publication. Subscription
    /// reads observe their channel's queued data, terminal error, and EOF instead.
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_signal.subscribe()
    }

    /// Get the tokio runtime (for spawning tasks).
    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        self.rt
            .as_ref()
            .expect("engine runtime is cleared only while dropping")
    }

    pub fn request_pool_health(&self) -> Vec<(usize, WorkerHealth)> {
        self.request_pool.worker_health()
    }

    /// Seat type of the session's authorized identity (`BPS` / `NONBPS` /
    /// `INVALID`).
    ///
    /// With [`AuthConfig`] set (SAPI/B-PIPE), the SDK session identity is
    /// authorized once and reused. Without it, each call runs the classic
    /// flow (`generateToken` for the OS logon user → `//blp/apiauth`
    /// authorization) — this succeeds on Desktop API terminals whose user is
    /// EMRS-enrolled; otherwise Bloomberg's precise reason (e.g. "User not
    /// in emrs userid=...") is surfaced with configuration guidance.
    pub async fn seat_type(&self) -> Result<xbbg_core::SeatType, BlpAsyncError> {
        let worker = self.request_pool.any_healthy_worker()?;
        worker.identity_seat_type().await.map_err(Into::into)
    }

    /// Check the authorized identity's entitlements for `service`,
    /// reporting exactly which EIDs failed (empty when fully entitled).
    /// Pair with `return_eids` request metadata (`xbbg.eid_data`) to gate
    /// redistribution per security.
    pub async fn check_entitlements(
        &self,
        service: &str,
        eids: &[i32],
    ) -> Result<xbbg_core::EntitlementCheck, BlpAsyncError> {
        let worker = self.request_pool.any_healthy_worker()?;
        worker
            .identity_check_entitlements(service, eids)
            .await
            .map_err(Into::into)
    }

    /// Whether the authorized identity is authorized for `service` at all.
    pub async fn identity_is_authorized(&self, service: &str) -> Result<bool, BlpAsyncError> {
        let worker = self.request_pool.any_healthy_worker()?;
        worker
            .identity_is_authorized(service)
            .await
            .map_err(Into::into)
    }
}

/// Release the engine's tokio runtime safely from any calling context.
///
/// Tokio panics with "Cannot drop a runtime in a context where blocking is not
/// allowed" when a `Runtime`'s last handle is released inside an async context,
/// because teardown blocks to join the worker threads. `Engine` is routinely
/// owned by async code — `xbbg-mcp` holds one across `#[tokio::main]`, and the
/// live tests hold one across `#[tokio::test]` — so inside a runtime we hand
/// teardown to tokio's non-blocking path instead.
///
/// Outside a runtime the `Arc` drops here, which is the correct blocking
/// shutdown. Taking ownership is what makes this race-free: the caller's field
/// is left `None`, so no second release path can reach a zero refcount on the
/// async thread. When another clone is still outstanding, `Arc::into_inner`
/// returns `None` and that remaining owner stays responsible.
fn release_runtime(rt: Option<Arc<tokio::runtime::Runtime>>) {
    let Some(rt) = rt else { return };

    if tokio::runtime::Handle::try_current().is_ok() {
        if let Some(rt) = Arc::into_inner(rt) {
            rt.shutdown_background();
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Non-blocking: signal all workers to shut down.
        // For blocking shutdown, call shutdown_blocking() explicitly before dropping.
        self.signal_shutdown();
        release_runtime(self.rt.take());
    }
}

#[cfg(test)]
mod release_runtime_tests {
    use super::release_runtime;
    use std::sync::Arc;

    fn new_runtime() -> Arc<tokio::runtime::Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime builds"),
        )
    }

    /// Regression guard for the panic that made every clean `xbbg-mcp` shutdown
    /// abort: releasing the runtime from inside an async context must not panic.
    #[tokio::test(flavor = "multi_thread")]
    async fn releases_inside_async_context() {
        release_runtime(Some(new_runtime()));
    }

    /// The synchronous path (Python/atexit, plain `main`) still drops in place.
    #[test]
    fn releases_outside_async_context() {
        release_runtime(Some(new_runtime()));
    }

    /// With a clone outstanding, `Arc::into_inner` yields `None` and the runtime
    /// survives for the remaining owner rather than panicking.
    #[tokio::test(flavor = "multi_thread")]
    async fn tolerates_outstanding_clone() {
        let rt = new_runtime();
        let survivor = Arc::clone(&rt);

        release_runtime(Some(rt));
        assert_eq!(Arc::strong_count(&survivor), 1);

        // Still inside the async context, so this last handle needs the same path.
        release_runtime(Some(survivor));
    }

    #[test]
    fn none_is_a_noop() {
        release_runtime(None);
    }
}

fn record_drained_subscription_item(
    item: Result<SubscriptionUpdate, BlpError>,
    remaining: &mut Vec<SubscriptionUpdate>,
    first_error: &mut Option<BlpError>,
) {
    match item {
        Ok(update) => remaining.push(update),
        Err(error) if first_error.is_none() => *first_error = Some(error),
        Err(_) => {}
    }
}

async fn collect_subscription_updates_until_drained(
    rx: &mut SubscriptionReceiver,
    barrier: impl Future<Output = Result<(), BlpAsyncError>>,
    remaining: &mut Vec<SubscriptionUpdate>,
    first_error: &mut Option<BlpError>,
) -> Result<(), BlpAsyncError> {
    tokio::pin!(barrier);
    loop {
        tokio::select! {
            biased;
            item = rx.recv() => {
                match item {
                    Some(item) => {
                        record_drained_subscription_item(item, remaining, first_error);
                    }
                    None => return barrier.await,
                }
            }
            result = &mut barrier => return result,
        }
    }
}

/// Stream for receiving real-time market data with dynamic subscription control.
///
/// Provides async iteration over incoming data and methods to dynamically
/// add/remove tickers while the subscription is active.
///
/// Data arrives as `Result<SubscriptionUpdate, BlpError>`:
/// - `Ok(update)` — normal data
/// - `Err(error)` — subscription failure, session death, etc.
///
/// The underlying session is released back to the pool on drop.
pub struct SubscriptionStream {
    /// Receiver for incoming data batches (or errors).
    rx: SubscriptionReceiver,
    /// Sender for adding new topics (shares channel with existing subs).
    tx: SubscriptionSender,
    /// Session claim (released on drop).
    claim: Option<SessionClaim>,
    /// Subscribed fields.
    fields: Vec<String>,
    /// Whether batches should expose all top-level Bloomberg fields.
    all_fields: bool,
    /// Bloomberg service (e.g., "//blp/mktdata", "//blp/mktvwap").
    service: String,
    /// Subscription options.
    options: Vec<String>,
    /// Shared active/failed topic status.
    status: SharedSubscriptionStatus,
    /// Optional flush threshold override.
    flush_threshold: Option<usize>,
    /// Optional overflow policy override.
    overflow_policy: Option<OverflowPolicy>,
}

impl SubscriptionStream {
    fn command_handle(&self) -> Result<SubscriptionCommandHandle, BlpAsyncError> {
        self.claim
            .as_ref()
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "subscription already closed".to_string(),
            })?
            .command_handle()
    }

    fn cleanup_without_reuse_if_active(&mut self) {
        let keys = self.status.load().keys().to_vec();
        if keys.is_empty() {
            return;
        }
        if let Some(claim) = self.claim.take() {
            claim.close_without_reuse(keys);
        }
        self.status.update(|next| next.clear_active());
    }

    /// Receive the next batch of data or an error.
    ///
    /// Returns:
    /// - `Some(Ok(update))` — normal data
    /// - `Some(Err(error))` — subscription failure, session death, etc.
    /// - `None` — subscription is closed
    pub async fn next(&mut self) -> Option<Result<SubscriptionUpdate, BlpError>> {
        let item = self.rx.recv().await;
        if matches!(&item, Some(Err(_))) {
            self.cleanup_without_reuse_if_active();
        }
        item
    }

    /// Try to receive data without blocking.
    pub fn try_next(&mut self) -> Option<Result<SubscriptionUpdate, BlpError>> {
        let item = self.rx.try_recv().ok();
        if matches!(&item, Some(Err(_))) {
            self.cleanup_without_reuse_if_active();
        }
        item
    }

    /// Add tickers to the subscription dynamically.
    ///
    /// New tickers will start receiving data on the same stream.
    pub async fn add(&mut self, topics: Vec<String>) -> Result<(), BlpAsyncError> {
        if self.tx.is_closed() {
            return Err(BlpAsyncError::ConfigError {
                detail: "cannot add topics to a terminal subscription stream".to_string(),
            });
        }
        let command = self.command_handle()?;
        let mut seen_topics = HashSet::new();

        // Filter out already subscribed topics
        let new_topics: Vec<String> = {
            let snapshot = self.status.load();
            topics
                .into_iter()
                .filter(|t| {
                    !snapshot.topic_to_key().contains_key(t) && seen_topics.insert(t.clone())
                })
                .collect()
        };

        if new_topics.is_empty() {
            return Ok(());
        }

        xbbg_log::debug!(topics = ?new_topics, "adding topics to subscription");

        // Add new topics using the same stream sender
        command
            .add_topics(
                self.service.clone(),
                new_topics,
                self.fields.clone(),
                self.all_fields,
                self.options.clone(),
                self.flush_threshold,
                self.overflow_policy,
                self.tx.clone(),
                self.status.clone(),
            )
            .await?;

        Ok(())
    }

    /// Remove tickers from the subscription dynamically.
    ///
    /// Removed tickers will stop receiving data.
    pub async fn remove(&mut self, topics: Vec<String>) -> Result<(), BlpAsyncError> {
        let command = self.command_handle()?;
        let mut seen_keys = HashSet::new();

        // Find keys for topics to remove
        let mut keys_to_remove = Vec::new();
        let mut topics_to_remove = Vec::new();
        {
            let snapshot = self.status.load();
            for topic in topics {
                if let Some(&key) = snapshot.topic_to_key().get(&topic) {
                    if seen_keys.insert(key) {
                        keys_to_remove.push(key);
                        topics_to_remove.push(topic);
                    }
                }
            }
        }

        if keys_to_remove.is_empty() {
            return Ok(());
        }

        xbbg_log::debug!(topics = ?topics_to_remove, keys = ?keys_to_remove, "removing topics from subscription");

        command.unsubscribe(keys_to_remove.clone()).await?;

        self.status.update(|next| {
            for topic in &topics_to_remove {
                next.drop_topic(topic);
            }
        });

        Ok(())
    }

    /// Get the currently subscribed topics.
    pub fn topics(&self) -> Vec<String> {
        self.status.load().topics().to_vec()
    }

    /// Get the subscribed fields.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Check if any topics are still subscribed.
    pub fn is_active(&self) -> bool {
        self.claim.is_some() && !self.tx.is_closed() && self.status.load().has_active_topics()
    }

    /// Unsubscribe from all topics and close the stream.
    ///
    /// If `drain` is true, returns remaining buffered updates before closing.
    /// An unread stream failure is returned after forwarding and cleanup complete.
    pub async fn unsubscribe(
        mut self,
        drain: bool,
    ) -> Result<Vec<SubscriptionUpdate>, BlpAsyncError> {
        let mut remaining = Vec::new();
        let mut first_stream_error = None;
        let mut cleanup_error = None;

        if let Some(claim) = self.claim.take() {
            let keys = self.status.load().keys().to_vec();
            if !keys.is_empty() {
                if let Err(error) = claim.unsubscribe(keys).await {
                    cleanup_error = Some(error);
                }
            }
            if drain {
                // Consume the receiver while the ordered forwarding barrier
                // advances. Otherwise a full receiver makes an already-
                // accepted Block-policy update wait for its timeout before the
                // barrier can run.
                if let Err(error) = collect_subscription_updates_until_drained(
                    &mut self.rx,
                    claim.drain_forwarder(),
                    &mut remaining,
                    &mut first_stream_error,
                )
                .await
                {
                    cleanup_error.get_or_insert(error);
                }
                // The forwarding barrier has accounted for all accepted work.
                // Closing now distinguishes deliberate cancellation from a
                // worker-shutdown failure while retaining buffered data and any
                // earlier terminal error.
                self.rx.close();
            }
            // SessionClaim::drop completes cleanup or quarantines a worker that
            // still has pending Bloomberg state before an error is reported.
            drop(claim);
        }

        if drain {
            while let Ok(item) = self.rx.try_recv() {
                record_drained_subscription_item(item, &mut remaining, &mut first_stream_error);
            }
        }
        self.status.update(|next| next.clear_active());

        if let Some(error) = first_stream_error {
            return Err(error.into());
        }
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        Ok(remaining)
    }

    /// Close the stream with best-effort cleanup.
    ///
    /// Drop cannot await Bloomberg termination confirmations. If active topics remain,
    /// cleanup sends an unsubscribe command and discards the worker instead of
    /// returning a potentially dirty session to the reusable pool.
    pub fn close(mut self) {
        self.cleanup_without_reuse_if_active();
    }

    /// Destructure the stream into its component parts.
    ///
    /// Used by PyO3 layer to separate rx (for iteration) from claim (for add/remove)
    /// so they can use independent locks and avoid contention.
    ///
    /// Consumes self without running Drop (since we're taking ownership of parts).
    ///
    /// Returns an error if the stream was already closed and no longer owns a session claim.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> Result<
        (
            SubscriptionReceiver,
            SubscriptionSender,
            SessionClaim,
            SharedSubscriptionStatus,
            Option<usize>,          // flush_threshold
            Option<OverflowPolicy>, // overflow_policy
            String,                 // service
            Vec<String>,            // options
            bool,                   // all_fields
        ),
        BlpError,
    > {
        use std::mem::ManuallyDrop;
        use std::ptr;

        // Prevent Drop from running — we're taking ownership of each field individually.
        let this = ManuallyDrop::new(self);

        // SAFETY: We read each field exactly once from the ManuallyDrop wrapper.
        // The wrapper prevents the destructor from running, so no double-free.
        unsafe {
            let rx = ptr::read(&this.rx);
            let tx = ptr::read(&this.tx);
            let claim = ptr::read(&this.claim);
            let status = ptr::read(&this.status);
            let flush_threshold = ptr::read(&this.flush_threshold);
            let overflow_policy = ptr::read(&this.overflow_policy);
            let service = ptr::read(&this.service);
            let options = ptr::read(&this.options);
            let all_fields = ptr::read(&this.all_fields);

            let Some(claim) = claim else {
                return Err(BlpError::Internal {
                    detail: "SubscriptionStream::into_parts called on already-closed stream"
                        .to_string(),
                });
            };

            Ok((
                rx,
                tx,
                claim,
                status,
                flush_threshold,
                overflow_policy,
                service,
                options,
                all_fields,
            ))
        }
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        self.cleanup_without_reuse_if_active();
        // If no active topics remain, SessionClaim drops normally and returns the worker to the pool.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;

    use crate::schema::SchemaCache;

    fn empty_schema() -> SchemaCache {
        SchemaCache::new()
    }

    fn prepare_refdata(securities: &[&str]) -> PreparedRequest {
        PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(
                    securities
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                ),
                fields: Some(vec!["PX_LAST".to_string()]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata")
    }

    fn prepare_histdata(securities: &[&str]) -> PreparedRequest {
        PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::HistoricalData.to_string(),
                securities: Some(
                    securities
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                ),
                fields: Some(vec!["PX_LAST".to_string()]),
                start_date: Some("20240101".to_string()),
                end_date: Some("20240131".to_string()),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared histdata")
    }

    fn shard_config() -> EngineConfig {
        EngineConfig {
            shard_requests: true,
            shard_threshold: 2,
            shard_chunk_size: 2,
            shard_max_concurrent: 2,
            ..Default::default()
        }
    }

    fn config_error_detail(err: BlpAsyncError) -> String {
        match err {
            BlpAsyncError::ConfigError { detail } => detail,
            other => panic!("expected config error, got {other}"),
        }
    }

    fn ticker_batch(values: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "ticker",
            Arc::new(StringArray::from_iter_values(values.iter().copied())) as ArrayRef,
        )])
        .expect("ticker batch")
    }

    fn px_last_batch(tickers: &[&str], px_last: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("PX_LAST", px_last.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(tickers.iter().copied())) as ArrayRef,
                px_last,
            ],
        )
        .expect("px_last batch")
    }

    #[test]
    fn raw_request_uses_request_operation_for_validation_and_dispatch() {
        let params = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::RawRequest.to_string(),
            request_operation: Some(Operation::ReferenceData.to_string()),
            ..Default::default()
        };

        assert!(params.is_raw_request());
        assert_eq!(params.effective_operation(), "ReferenceDataRequest");
        assert!(params.validate().is_ok());
    }

    #[test]
    fn raw_request_requires_request_operation() {
        let params = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::RawRequest.to_string(),
            ..Default::default()
        };

        let err = params.validate().unwrap_err().to_string();
        assert!(err.contains("request_operation is required for RawRequest"));
    }

    #[test]
    fn engine_config_defaults_include_auth_and_resource_defaults() {
        let config = EngineConfig::default();

        assert_eq!(config.auth, None);
        assert_eq!(config.num_start_attempts, 3);
        assert!(config.auto_restart_on_disconnection);
        assert_eq!(config.runtime_worker_threads, 2);
        assert_eq!(config.max_subscription_sessions, 32);
        assert!(config.max_subscription_sessions >= config.subscription_pool_size);
    }

    #[test]
    fn engine_config_rejects_zero_subscription_stream_capacity() {
        let config = EngineConfig {
            subscription_stream_capacity: 0,
            ..Default::default()
        };

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("subscription_stream_capacity must be greater than zero"));
    }

    #[test]
    fn engine_config_rejects_invalid_resource_bounds() {
        for (config, expected) in [
            (
                EngineConfig {
                    request_pool_size: 0,
                    ..Default::default()
                },
                "request_pool_size must be greater than zero",
            ),
            (
                EngineConfig {
                    runtime_worker_threads: 0,
                    ..Default::default()
                },
                "runtime_worker_threads must be greater than zero",
            ),
            (
                EngineConfig {
                    max_subscription_sessions: 0,
                    ..Default::default()
                },
                "max_subscription_sessions must be greater than zero",
            ),
            (
                EngineConfig {
                    subscription_pool_size: 3,
                    max_subscription_sessions: 2,
                    ..Default::default()
                },
                "max_subscription_sessions must be greater than or equal to subscription_pool_size",
            ),
            (
                EngineConfig {
                    command_queue_size: 0,
                    ..Default::default()
                },
                "command_queue_size must be greater than zero",
            ),
        ] {
            assert_eq!(
                config_error_detail(config.validate().unwrap_err()),
                expected
            );
        }
    }

    #[test]
    fn test_security_overrides_shard_by_contiguous_override_set_and_merge_globals() {
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec![
                    "A".to_string(),
                    "B".to_string(),
                    "C".to_string(),
                    "D".to_string(),
                ]),
                fields: Some(vec!["PX_LAST".to_string()]),
                overrides: Some(vec![("CRNCY".to_string(), "USD".to_string())]),
                security_overrides: Some(vec![
                    (
                        "A".to_string(),
                        vec![("CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "C".to_string(),
                        vec![("CRNCY".to_string(), "JPY".to_string())],
                    ),
                    (
                        "D".to_string(),
                        vec![("CRNCY".to_string(), "JPY".to_string())],
                    ),
                ]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with per-security overrides");

        let shards = sharded_requests(&EngineConfig::default(), &prepared).expect("shards");
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string()][..])
        );
        assert_eq!(
            shards[0].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "EUR".to_string())][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "USD".to_string())][..])
        );
        assert_eq!(
            shards[2].params().securities.as_deref(),
            Some(&["C".to_string(), "D".to_string()][..])
        );
        assert_eq!(
            shards[2].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "JPY".to_string())][..])
        );
        assert!(shards
            .iter()
            .all(|shard| shard.params().security_overrides.is_none()));
    }

    #[test]
    fn test_security_overrides_honor_enabled_shard_chunk_size() {
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                security_overrides: Some(vec![
                    (
                        "A".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "B".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "C".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                ]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with same per-security overrides");

        let shards = sharded_requests(
            &EngineConfig {
                shard_requests: true,
                shard_threshold: 2,
                shard_chunk_size: 2,
                shard_max_concurrent: 2,
                ..Default::default()
            },
            &prepared,
        )
        .expect("shards");

        assert_eq!(shards.len(), 2);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string(), "B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["C".to_string()][..])
        );
        for shard in shards {
            assert_eq!(
                shard.params().overrides.as_deref(),
                Some(&[("EQY_FUND_CRNCY".to_string(), "EUR".to_string())][..])
            );
        }
    }

    #[test]
    fn test_security_overrides_reject_unknown_security() {
        let err = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                security_overrides: Some(vec![(
                    "B".to_string(),
                    vec![("CRNCY".to_string(), "EUR".to_string())],
                )]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("security_overrides contains security not in request: B"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn test_engine_config_defaults_disable_sharding() {
        let config = EngineConfig::default();

        assert!(!config.shard_requests);
        assert_eq!(config.shard_threshold, 20);
        assert_eq!(config.shard_chunk_size, 16);
        assert_eq!(config.shard_max_concurrent, 4);
    }

    #[test]
    fn test_engine_config_rejects_invalid_sharding_knobs() {
        let mut config = EngineConfig {
            shard_threshold: 1,
            ..Default::default()
        };
        assert_eq!(
            config_error_detail(config.validate().unwrap_err()),
            "shard_threshold must be at least 2"
        );

        config = EngineConfig {
            shard_chunk_size: 0,
            ..Default::default()
        };
        assert_eq!(
            config_error_detail(config.validate().unwrap_err()),
            "shard_chunk_size must be greater than zero"
        );

        config = EngineConfig {
            shard_max_concurrent: 0,
            ..Default::default()
        };
        assert_eq!(
            config_error_detail(config.validate().unwrap_err()),
            "shard_max_concurrent must be greater than zero"
        );
    }

    #[test]
    fn test_sharded_requests_skip_when_disabled_or_below_threshold() {
        let prepared = prepare_refdata(&["A", "B", "C"]);

        assert!(sharded_requests(&EngineConfig::default(), &prepared).is_none());
        assert!(sharded_requests(
            &EngineConfig {
                shard_requests: true,
                shard_threshold: 4,
                shard_chunk_size: 2,
                shard_max_concurrent: 2,
                ..Default::default()
            },
            &prepared,
        )
        .is_none());
    }

    #[test]
    fn test_sharded_requests_skip_raw_and_non_ref_hist() {
        let raw = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::RawRequest.to_string(),
                request_operation: Some(Operation::ReferenceData.to_string()),
                extractor: ExtractorType::RefData,
                extractor_set: true,
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared raw refdata");
        assert!(sharded_requests(&shard_config(), &raw).is_none());

        let intraday = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::IntradayBar.to_string(),
                security: Some("A".to_string()),
                event_type: Some("TRADE".to_string()),
                interval: Some(1),
                start_datetime: Some("2024-01-01T09:30:00".to_string()),
                end_datetime: Some("2024-01-01T10:00:00".to_string()),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared intraday");
        assert!(sharded_requests(&shard_config(), &intraday).is_none());
    }

    #[test]
    fn test_sharded_requests_chunk_in_order() {
        let prepared = prepare_refdata(&["S0", "S1", "S2", "S3", "S4"]);
        let shards = sharded_requests(&shard_config(), &prepared).expect("shards");
        let securities: Vec<Vec<String>> = shards
            .iter()
            .map(|shard| shard.params().securities.clone().expect("securities"))
            .collect();

        assert_eq!(
            securities,
            vec![
                vec!["S0".to_string(), "S1".to_string()],
                vec!["S2".to_string(), "S3".to_string()],
                vec!["S4".to_string()],
            ]
        );

        let hist = prepare_histdata(&["H0", "H1", "H2"]);
        assert_eq!(
            sharded_requests(&shard_config(), &hist)
                .expect("hist shards")
                .len(),
            2
        );
    }

    #[test]
    fn test_sharded_requests_preserve_overrides_elements_and_field_types() {
        let field_types = HashMap::from([("PX_LAST".to_string(), "float64".to_string())]);
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                overrides: Some(vec![("EQY_FUND_CRNCY".to_string(), "USD".to_string())]),
                elements: Some(vec![("returnEids".to_string(), "true".to_string())]),
                field_types: Some(field_types.clone()),
                include_security_errors: true,
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with overrides");

        let shards = sharded_requests(&shard_config(), &prepared).expect("shards");
        assert_eq!(shards.len(), 2);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string(), "B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["C".to_string()][..])
        );
        for shard in shards {
            assert_eq!(shard.params().overrides, prepared.params().overrides);
            assert_eq!(shard.params().elements, prepared.params().elements);
            assert_eq!(shard.params().field_types, Some(field_types.clone()));
            assert!(shard.params().include_security_errors);
        }
    }

    #[test]
    fn test_concat_sharded_batches_preserves_order() {
        let batch =
            concat_sharded_batches(vec![ticker_batch(&["A", "B"]), ticker_batch(&["C", "D"])])
                .expect("concatenated batch");
        let ticker = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker column");

        assert_eq!(ticker.value(0), "A");
        assert_eq!(ticker.value(1), "B");
        assert_eq!(ticker.value(2), "C");
        assert_eq!(ticker.value(3), "D");
    }

    #[test]
    fn test_concat_sharded_batches_promotes_all_null_column_to_target_schema() {
        let batch = concat_sharded_batches(vec![
            px_last_batch(
                &["A"],
                Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef,
            ),
            px_last_batch(
                &["B"],
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ])
        .expect("concatenated batch");

        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
        let px_last = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("PX_LAST column");
        assert_eq!(px_last.value(0), 1.0);
        assert!(px_last.is_null(1));
    }

    #[test]
    fn test_concat_sharded_batches_rejects_non_null_type_mismatch() {
        let err = concat_sharded_batches(vec![
            px_last_batch(
                &["A"],
                Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef,
            ),
            px_last_batch(
                &["B"],
                Arc::new(StringArray::from(vec![Some("bad")])) as ArrayRef,
            ),
        ])
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("column type mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn excel_grid_detection_uses_raw_request_operation() {
        let params = RequestParams {
            operation: Operation::RawRequest.to_string(),
            request_operation: Some(Operation::ExcelGetGrid.to_string()),
            ..Default::default()
        };

        assert!(params.is_excel_get_grid_request());
    }

    #[test]
    fn raw_excel_grid_defaults_to_bsrch_extractor() {
        let params = RequestParams {
            operation: Operation::RawRequest.to_string(),
            request_operation: Some(Operation::ExcelGetGrid.to_string()),
            ..Default::default()
        }
        .with_defaults();

        assert_eq!(params.extractor, ExtractorType::Bsrch);
    }

    #[test]
    fn request_params_input_centralizes_extractor_and_raw_defaults() {
        let params = RequestParamsInput {
            service: String::new(),
            operation: None,
            request_operation: Some(Operation::ReferenceData.to_string()),
            extractor: Some("bulk".to_string()),
            securities: Some(vec!["INDU Index".to_string()]),
            fields: Some(vec!["INDX_MEMBERS".to_string()]),
            include_security_errors: None,
            ..Default::default()
        }
        .into_request_params()
        .unwrap();

        assert_eq!(params.service, Service::RefData.to_string());
        assert_eq!(params.operation, Operation::RawRequest.to_string());
        assert_eq!(
            params.request_operation.as_deref(),
            Some(Operation::ReferenceData.as_str())
        );
        assert_eq!(params.extractor, ExtractorType::BulkData);
        assert!(params.extractor_set);
        assert!(!params.include_security_errors);
    }

    #[test]
    fn request_params_input_normalizes_empty_optionals() {
        let params = RequestParamsInput {
            service: Service::RefData.to_string(),
            operation: Some(Operation::ReferenceData.to_string()),
            extractor: Some(String::new()),
            securities: Some(Vec::new()),
            fields: Some(vec!["PX_LAST".to_string()]),
            kwargs: Some(HashMap::new()),
            format: Some(String::new()),
            ..Default::default()
        }
        .into_request_params()
        .unwrap();

        assert_eq!(params.extractor, ExtractorType::RefData);
        assert!(!params.extractor_set);
        assert!(params.securities.is_none());
        assert!(params.kwargs.is_none());
        assert!(params.format.is_none());
    }

    #[test]
    fn request_params_input_maps_return_eids() {
        let base = RequestParamsInput {
            service: Service::RefData.to_string(),
            operation: Some(Operation::ReferenceData.to_string()),
            securities: Some(vec!["AAPL US Equity".to_string()]),
            fields: Some(vec!["PX_LAST".to_string()]),
            ..Default::default()
        };

        let defaulted = base.clone().into_request_params().unwrap();
        assert!(!defaulted.return_eids);

        let enabled = RequestParamsInput {
            return_eids: Some(true),
            ..base
        }
        .into_request_params()
        .unwrap();
        assert!(enabled.return_eids);
        enabled.validate().expect("returnEids valid for refdata");
    }

    #[test]
    fn return_eids_validation_matches_supported_operations() {
        let common = RequestParamsInput {
            service: Service::RefData.to_string(),
            security: Some("AAPL US Equity".to_string()),
            start_datetime: Some("2024-01-02T00:00:00".to_string()),
            end_datetime: Some("2024-01-03T00:00:00".to_string()),
            event_type: Some("TRADE".to_string()),
            return_eids: Some(true),
            ..Default::default()
        };

        for operation in [
            Operation::ReferenceData,
            Operation::HistoricalData,
            Operation::IntradayBar,
            Operation::IntradayTick,
        ] {
            let input = RequestParamsInput {
                operation: Some(operation.to_string()),
                securities: matches!(
                    operation,
                    Operation::ReferenceData | Operation::HistoricalData
                )
                .then(|| vec!["AAPL US Equity".to_string()]),
                fields: matches!(
                    operation,
                    Operation::ReferenceData | Operation::HistoricalData
                )
                .then(|| vec!["PX_LAST".to_string()]),
                start_date: matches!(operation, Operation::HistoricalData)
                    .then(|| "20240102".to_string()),
                end_date: matches!(operation, Operation::HistoricalData)
                    .then(|| "20240103".to_string()),
                interval: matches!(operation, Operation::IntradayBar).then_some(1),
                ..common.clone()
            };
            let params = input.into_request_params().unwrap();
            params
                .validate()
                .unwrap_or_else(|err| panic!("returnEids invalid for {operation}: {err}"));
        }

        let unsupported = RequestParamsInput {
            service: Service::ApiFlds.to_string(),
            operation: Some(Operation::FieldInfo.to_string()),
            field_ids: Some(vec!["PX_LAST".to_string()]),
            return_eids: Some(true),
            ..Default::default()
        }
        .into_request_params()
        .unwrap();
        let err = unsupported
            .validate()
            .expect_err("returnEids must be rejected for FieldInfo");
        assert!(
            err.to_string().contains("return_eids"),
            "unexpected error: {err}"
        );

        let raw = RequestParamsInput {
            service: Service::RefData.to_string(),
            operation: Some(Operation::RawRequest.to_string()),
            request_operation: Some(Operation::ReferenceData.to_string()),
            return_eids: Some(true),
            ..Default::default()
        }
        .into_request_params()
        .unwrap();
        raw.validate()
            .expect("raw ReferenceData target supports returnEids");

        let unsupported_raw = RequestParamsInput {
            service: Service::RefData.to_string(),
            operation: Some(Operation::RawRequest.to_string()),
            request_operation: Some(Operation::FieldInfo.to_string()),
            return_eids: Some(true),
            ..Default::default()
        }
        .into_request_params()
        .unwrap();
        let err = unsupported_raw
            .validate()
            .expect_err("first-class returnEids must validate the raw target");
        assert!(
            err.to_string().contains("return_eids"),
            "unexpected error: {err}"
        );

        let explicit_element = RequestParamsInput {
            service: Service::RefData.to_string(),
            operation: Some(Operation::RawRequest.to_string()),
            request_operation: Some(Operation::FieldInfo.to_string()),
            elements: Some(vec![("returnEids".to_string(), "true".to_string())]),
            ..Default::default()
        }
        .into_request_params()
        .unwrap();
        explicit_element
            .validate()
            .expect("generic explicit returnEids element remains an escape hatch");
    }

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

    #[tokio::test]
    async fn drain_barrier_consumes_receiver_while_forwarding_is_blocked() {
        let update = |topic_id| SubscriptionUpdate {
            timestamp_us: topic_id as i64,
            topic_id,
            topic: Arc::from("TEST"),
            layout: Arc::new(state::FieldLayout::new(1, Vec::new())),
            values: Default::default(),
        };
        let first = update(1);
        let second = update(2);
        let (tx, mut rx) = subscription_channel(1);
        let barrier = async move {
            tx.send(Ok(first))
                .await
                .map_err(|_| BlpAsyncError::ChannelClosed)?;
            tx.send(Ok(second))
                .await
                .map_err(|_| BlpAsyncError::ChannelClosed)?;
            Ok(())
        };
        let mut remaining = Vec::new();
        let mut first_error = None;

        collect_subscription_updates_until_drained(
            &mut rx,
            barrier,
            &mut remaining,
            &mut first_error,
        )
        .await
        .expect("forwarding barrier");
        while let Ok(item) = rx.try_recv() {
            record_drained_subscription_item(item, &mut remaining, &mut first_error);
        }

        assert_eq!(
            remaining
                .iter()
                .map(|update| update.topic_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn drain_waits_for_barrier_before_propagating_terminal_error() {
        let (tx, mut rx) = subscription_channel(1);
        tx.try_send(Ok(SubscriptionUpdate {
            timestamp_us: 1,
            topic_id: 1,
            topic: Arc::from("TEST"),
            layout: Arc::new(state::FieldLayout::new(1, Vec::new())),
            values: Default::default(),
        }))
        .expect("queued update");
        tx.fail(BlpError::Internal {
            detail: "terminal before cleanup".to_string(),
        });
        let (release_barrier, barrier) = tokio::sync::oneshot::channel();
        let (barrier_started, wait_for_barrier) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            let mut remaining = Vec::new();
            let mut first_error = None;
            collect_subscription_updates_until_drained(
                &mut rx,
                async move {
                    barrier_started
                        .send(())
                        .map_err(|_| BlpAsyncError::ChannelClosed)?;
                    barrier.await.map_err(|_| BlpAsyncError::ChannelClosed)?;
                    Ok(())
                },
                &mut remaining,
                &mut first_error,
            )
            .await
            .expect("forwarding barrier");
            (remaining, first_error)
        });
        wait_for_barrier.await.expect("barrier was polled");
        assert!(!task.is_finished());

        release_barrier.send(()).expect("release cleanup barrier");
        let (remaining, error) = task.await.expect("drain task");
        assert_eq!(remaining.len(), 1);
        assert!(error
            .expect("terminal error")
            .to_string()
            .contains("terminal before cleanup"));
    }
}
