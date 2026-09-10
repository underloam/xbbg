//! Subscription session pool with claim/release semantics.
//!
//! Subscription sessions use Bloomberg SDK asynchronous callback mode: idle
//! workers do not poll, and events are dispatched on the SDK dispatcher thread.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};
use slab::Slab;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use xbbg_core::{AsyncSession, BlpError, CorrelationId, EventType, SubscriptionList};

/// Max wall time for an async open_service reply before we give up.
const SERVICE_OPEN_TIMEOUT_MS: u64 = 10_000;

use super::dispatch::{DispatchKey, SERVICE_OPEN_CID_TAG};
use super::state::{
    subscription_forwarder_channel, MessageOutcome, SubscriptionForwarder, SubscriptionMetrics,
    SubscriptionSender, SubscriptionState, SubscriptionTerminator,
};
use super::{
    attach_auth_context, build_session_options, BlpAsyncError, EngineConfig, OverflowPolicy,
    SessionLifecycleState, SharedSubscriptionStatus, SlabKey, SubscriptionEventCategory,
    SubscriptionEventLevel, SubscriptionFailureKind, WorkerHealth, SESSION_STARTUP_TIMEOUT_MS,
};

type RegisteredSubscriptions = (
    SubscriptionList,
    Vec<SlabKey>,
    Vec<Arc<SubscriptionMetrics>>,
    Vec<String>,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleLogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriptionSessionEvent {
    ConnectionDown,
    Terminated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionEventLogContext {
    event: SubscriptionSessionEvent,
    level: LifecycleLogLevel,
    shutdown_requested: bool,
}

impl SubscriptionSessionEvent {
    const fn name(self) -> &'static str {
        match self {
            Self::ConnectionDown => "SessionConnectionDown",
            Self::Terminated => "SessionTerminated",
        }
    }

    const fn log_context(self, shutdown_requested: bool) -> SessionEventLogContext {
        let level = if shutdown_requested {
            LifecycleLogLevel::Info
        } else {
            match self {
                Self::ConnectionDown => LifecycleLogLevel::Warn,
                Self::Terminated => LifecycleLogLevel::Error,
            }
        };
        SessionEventLogContext {
            event: self,
            level,
            shutdown_requested,
        }
    }
}

impl SessionEventLogContext {
    const fn classification(self) -> &'static str {
        match (self.shutdown_requested, self.event) {
            (true, _) => "shutdown_in_progress",
            (false, SubscriptionSessionEvent::ConnectionDown) => "connection_down_without_shutdown",
            (false, SubscriptionSessionEvent::Terminated) => "termination_without_shutdown",
        }
    }
}

fn log_subscription_session_event(
    context: SessionEventLogContext,
    worker_id: usize,
    active_subs: usize,
    reason: Option<&str>,
) {
    match context.level {
        LifecycleLogLevel::Info => xbbg_log::info!(
            worker_id,
            active_subs,
            event = %context.event.name(),
            classification = %context.classification(),
            shutdown_requested = context.shutdown_requested,
            reason = %reason.unwrap_or(""),
            "Bloomberg subscription session status"
        ),
        LifecycleLogLevel::Warn => xbbg_log::warn!(
            worker_id,
            active_subs,
            event = %context.event.name(),
            classification = %context.classification(),
            shutdown_requested = context.shutdown_requested,
            reason = %reason.unwrap_or(""),
            "Bloomberg subscription session status"
        ),
        LifecycleLogLevel::Error => xbbg_log::error!(
            worker_id,
            active_subs,
            event = %context.event.name(),
            classification = %context.classification(),
            shutdown_requested = context.shutdown_requested,
            reason = %reason.unwrap_or(""),
            "Bloomberg subscription session status"
        ),
    }
}

struct SubscriptionRegistrationRequest {
    topics: Vec<String>,
    fields: Vec<String>,
    all_fields: bool,
    options: Vec<String>,
    flush_threshold: Option<usize>,
    overflow_policy: Option<OverflowPolicy>,
    stream: SubscriptionSender,
    forwarder: Option<SubscriptionForwarder>,
}

struct PendingServiceOpen {
    cid: i64,
    waiters: Vec<oneshot::Sender<Result<(), BlpError>>>,
}
enum StatusMutation {
    Started {
        key: SlabKey,
        reason: Option<String>,
    },
    Unsubscribed {
        key: SlabKey,
        message_type: &'static str,
        reason: Option<String>,
    },
    Failed {
        key: SlabKey,
        fallback_topic: String,
        reason: String,
        kind: SubscriptionFailureKind,
        message_type: &'static str,
    },
    StreamsActive {
        key: SlabKey,
        active: bool,
        reason: Option<String>,
    },
}

impl StatusMutation {
    fn apply(self, status: &mut super::SubscriptionStatusState) {
        match self {
            Self::Started { key, reason } => {
                let topic = status.mark_topic_started(key);
                status.record_subscription_event(
                    "SubscriptionStarted",
                    topic,
                    reason,
                    SubscriptionEventLevel::Info,
                );
            }
            Self::Unsubscribed {
                key,
                message_type,
                reason,
            } => {
                let topic = status.mark_topic_unsubscribed(key);
                status.record_subscription_event(
                    message_type,
                    topic,
                    reason,
                    SubscriptionEventLevel::Info,
                );
            }
            Self::Failed {
                key,
                fallback_topic,
                reason,
                kind,
                message_type,
            } => {
                let topic = status
                    .record_failure(key, reason.clone(), kind)
                    .unwrap_or(fallback_topic);
                status.record_subscription_event(
                    message_type,
                    Some(topic),
                    Some(reason),
                    SubscriptionEventLevel::Warning,
                );
            }
            Self::StreamsActive {
                key,
                active,
                reason,
            } => {
                let Some(topic) = status.topic_for_key(key).map(str::to_string) else {
                    return;
                };
                let previous = status
                    .topic_statuses()
                    .get(&topic)
                    .map(|info| info.streams_active);
                let _ = status.set_topic_streams_active(&topic, active);
                if (active && previous == Some(false)) || (!active && previous != Some(false)) {
                    status.record_subscription_event(
                        if active {
                            "SubscriptionStreamsActivated"
                        } else {
                            "SubscriptionStreamsDeactivated"
                        },
                        Some(topic),
                        reason,
                        if active {
                            SubscriptionEventLevel::Info
                        } else {
                            SubscriptionEventLevel::Warning
                        },
                    );
                }
            }
        }
    }
}

#[derive(Default)]
struct StartupLatch {
    resolved: bool,
    result: Option<Result<(), BlpError>>,
}

struct SubscriptionWorkerState {
    id: usize,
    subs: Slab<SubscriptionState>,
    /// Weak fail capability for the claim's channel, retained after its final
    /// per-topic state is removed and cleared before the worker is pooled.
    stream_terminator: Option<SubscriptionTerminator>,
    config: Arc<EngineConfig>,
    open_services: HashSet<String>,
    pending_cancel: HashSet<SlabKey>,
    status: Option<SharedSubscriptionStatus>,
    last_streams_warn_us: HashMap<SlabKey, i64>,
    pending_service_opens: HashMap<String, PendingServiceOpen>,
}

struct SubscriptionWorkerShared {
    id: usize,
    state: Mutex<SubscriptionWorkerState>,
    health: Arc<AtomicU8>,
    startup: Mutex<StartupLatch>,
    startup_cv: Condvar,
    next_service_open_id: AtomicI64,
    shutdown: AtomicBool,
    /// Commands take a shared guard; explicit shutdown takes the exclusive
    /// guard so no SDK operation starts after the lifecycle gate closes.
    lifecycle: RwLock<()>,
    runtime_handle: OnceLock<tokio::runtime::Handle>,
    forwarder: Mutex<Option<(SubscriptionForwarder, JoinHandle<()>)>>,
    forwarder_capacity: usize,
}

struct PendingSubscriptionServiceWaiter<'a> {
    shared: &'a SubscriptionWorkerShared,
    service: &'a str,
    attempt_cid: i64,
    receiver: Option<oneshot::Receiver<Result<(), BlpError>>>,
}

impl<'a> PendingSubscriptionServiceWaiter<'a> {
    fn new(
        shared: &'a SubscriptionWorkerShared,
        service: &'a str,
        attempt_cid: i64,
        receiver: oneshot::Receiver<Result<(), BlpError>>,
    ) -> Self {
        Self {
            shared,
            service,
            attempt_cid,
            receiver: Some(receiver),
        }
    }

    fn receiver(&mut self) -> &mut oneshot::Receiver<Result<(), BlpError>> {
        self.receiver.as_mut().expect("receiver is present")
    }
}

impl Drop for PendingSubscriptionServiceWaiter<'_> {
    fn drop(&mut self) {
        self.receiver.take();
        self.shared
            .prune_pending_service_waiters(self.service, self.attempt_cid);
    }
}

impl SubscriptionWorkerShared {
    fn new(id: usize, config: Arc<EngineConfig>, health: Arc<AtomicU8>) -> Self {
        let forwarder_capacity = config.command_queue_size;
        Self {
            id,
            state: Mutex::new(SubscriptionWorkerState::new(id, config)),
            health,
            startup: Mutex::new(StartupLatch::default()),
            startup_cv: Condvar::new(),
            next_service_open_id: AtomicI64::new(0),
            shutdown: AtomicBool::new(false),
            lifecycle: RwLock::new(()),
            runtime_handle: OnceLock::new(),
            forwarder: Mutex::new(None),
            forwarder_capacity,
        }
    }

    fn resolve_startup(&self, result: Result<(), BlpError>) {
        let mut startup = self.startup.lock();
        if !startup.resolved {
            startup.resolved = true;
            startup.result = Some(result);
            self.startup_cv.notify_all();
        }
    }

    fn wait_startup(&self, timeout: Duration) -> Result<(), BlpError> {
        let deadline = Instant::now() + timeout;
        let mut startup = self.startup.lock();
        while startup.result.is_none() {
            if self
                .startup_cv
                .wait_until(&mut startup, deadline)
                .timed_out()
            {
                return Err(BlpError::Timeout);
            }
        }
        startup.result.take().expect("checked above")
    }

    fn next_service_cid(&self) -> i64 {
        SERVICE_OPEN_CID_TAG
            | self
                .next_service_open_id
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1)
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn ensure_running(&self) -> Result<(), BlpAsyncError> {
        if self.shutdown_requested() {
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription session is shut down".to_string(),
            });
        }
        Ok(())
    }

    fn effective_overflow_policy(&self, policy: Option<OverflowPolicy>) -> OverflowPolicy {
        policy.unwrap_or_else(|| self.state.lock().config.overflow_policy)
    }

    fn drain_for_shutdown(&self, detail: &str) {
        self.health
            .store(WorkerHealth::Dead as u8, Ordering::Release);
        self.state.lock().drain_for_shutdown(detail);
    }

    fn attach_runtime(&self, handle: tokio::runtime::Handle) {
        let _ = self.runtime_handle.set(handle);
    }

    fn ensure_forwarder(&self) -> Result<SubscriptionForwarder, BlpAsyncError> {
        let mut forwarder = self.forwarder.lock();
        if self.shutdown_requested() {
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription session is shut down".to_string(),
            });
        }
        if let Some((sender, _)) = forwarder.as_ref() {
            return Ok(sender.clone());
        }
        let runtime = self
            .runtime_handle
            .get()
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "subscription session runtime is not attached".to_string(),
            })?;
        let (sender, task) = subscription_forwarder_channel(self.forwarder_capacity);
        let handle = runtime.spawn(task);
        *forwarder = Some((sender.clone(), handle));
        Ok(sender)
    }

    fn stop_forwarder(&self) {
        if let Some((sender, handle)) = self.forwarder.lock().take() {
            drop(sender);
            handle.abort();
        }
    }

    async fn drain_forwarder(&self) -> Result<(), BlpAsyncError> {
        let forwarder = self
            .forwarder
            .lock()
            .as_ref()
            .map(|(sender, _)| sender.clone());
        if let Some(forwarder) = forwarder {
            forwarder
                .drain()
                .await
                .map_err(|_| BlpAsyncError::ChannelClosed)?;
        }
        Ok(())
    }

    fn mark_shutdown_requested(&self) -> bool {
        !self.shutdown.swap(true, Ordering::AcqRel)
    }

    fn dispatch_event(self: &Arc<Self>, ev: xbbg_core::Event) {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut state = self.state.lock();
            state.dispatch_event(ev, self);
        }));
        if result.is_err() {
            self.health.store(
                if self.shutdown_requested() {
                    WorkerHealth::Dead as u8
                } else {
                    WorkerHealth::Degraded as u8
                },
                Ordering::Release,
            );
            xbbg_log::error!(
                worker_id = self.id,
                "panic in subscription SDK callback; event dropped"
            );
        }
    }

    fn check_streams_deactivated(&self) {
        self.state.lock().check_streams_deactivated();
    }

    fn set_status(&self, status: SharedSubscriptionStatus) {
        self.state.lock().status = Some(status);
    }

    fn service_is_open(&self, service: &str) -> bool {
        self.state.lock().open_services.contains(service)
    }

    fn record_service_ready_if_already_open(&self, service: &str, was_open: bool) {
        self.state
            .lock()
            .record_service_ready_if_already_open(service, was_open);
    }

    fn record_service_open_error(&self, service: &str, error: &BlpError) {
        self.state.lock().record_service_open_error(service, error);
    }

    fn register_service_waiter(
        &self,
        service: &str,
    ) -> (bool, i64, oneshot::Receiver<Result<(), BlpError>>) {
        let (tx, rx) = oneshot::channel();
        let mut state = self.state.lock();
        if state.open_services.contains(service) {
            let _ = tx.send(Ok(()));
            return (false, 0, rx);
        }
        if let Some(open) = state.pending_service_opens.get_mut(service) {
            open.waiters.retain(|waiter| !waiter.is_closed());
            open.waiters.push(tx);
            return (false, open.cid, rx);
        }
        let cid = self.next_service_cid();
        state.pending_service_opens.insert(
            service.to_string(),
            PendingServiceOpen {
                cid,
                waiters: vec![tx],
            },
        );
        (true, cid, rx)
    }

    fn remove_pending_service_open(
        &self,
        service: &str,
        attempt_cid: i64,
    ) -> Option<PendingServiceOpen> {
        let mut state = self.state.lock();
        if state
            .pending_service_opens
            .get(service)
            .is_some_and(|open| open.cid == attempt_cid)
        {
            state.pending_service_opens.remove(service)
        } else {
            None
        }
    }

    fn prune_pending_service_waiters(&self, service: &str, attempt_cid: i64) {
        let mut state = self.state.lock();
        let remove_attempt = if let Some(open) = state.pending_service_opens.get_mut(service) {
            if open.cid != attempt_cid {
                return;
            }
            open.waiters.retain(|waiter| !waiter.is_closed());
            open.waiters.is_empty()
        } else {
            false
        };
        if remove_attempt {
            state.pending_service_opens.remove(service);
        }
    }

    fn register_subscriptions(
        &self,
        request: SubscriptionRegistrationRequest,
    ) -> Result<RegisteredSubscriptions, BlpError> {
        self.state.lock().register_subscriptions(request)
    }

    fn cleanup_failed_subscribe(&self, keys: &[SlabKey]) {
        let mut state = self.state.lock();
        for &key in keys {
            if state.subs.contains(key) {
                state.subs.remove(key);
            }
            state.pending_cancel.remove(&key);
            state.last_streams_warn_us.remove(&key);
        }
    }

    fn build_unsubscribe_list(&self, keys: Vec<SlabKey>) -> (SubscriptionList, usize) {
        self.state.lock().build_unsubscribe_list(keys)
    }

    fn reusable(&self) -> bool {
        !self.shutdown_requested() && self.state.lock().is_clean()
    }

    fn prepare_for_reuse(&self) -> bool {
        self.state.lock().prepare_for_reuse()
    }
}

impl SubscriptionWorkerState {
    fn new(id: usize, config: Arc<EngineConfig>) -> Self {
        Self {
            id,
            subs: Slab::new(),
            stream_terminator: None,
            config,
            open_services: HashSet::new(),
            pending_cancel: HashSet::new(),
            status: None,
            last_streams_warn_us: HashMap::new(),
            pending_service_opens: HashMap::new(),
        }
    }

    fn is_clean(&self) -> bool {
        self.subs.is_empty()
            && self.pending_cancel.is_empty()
            && self.pending_service_opens.is_empty()
    }

    fn prepare_for_reuse(&mut self) -> bool {
        if !self.is_clean() {
            return false;
        }
        self.stream_terminator = None;
        true
    }

    fn drain_for_shutdown(&mut self, detail: &str) {
        if let Some(terminator) = self.stream_terminator.take() {
            terminator.fail(BlpError::Internal {
                detail: detail.to_string(),
            });
        }
        let keys: Vec<SlabKey> = self.subs.iter().map(|(key, _)| key).collect();
        for key in keys {
            let mut state = self.subs.remove(key);
            state.mark_closing();
            state.fail(BlpError::Internal {
                detail: detail.to_string(),
            });
        }
        self.pending_cancel.clear();
        self.last_streams_warn_us.clear();
        for (_, open) in self.pending_service_opens.drain() {
            for waiter in open.waiters {
                let _ = waiter.send(Err(BlpError::Internal {
                    detail: detail.to_string(),
                }));
            }
        }
        self.clear_active_status();
    }

    fn clear_active_status(&mut self) {
        if let Some(status) = &self.status {
            status.update(|next| next.clear_active());
        }
    }

    fn record_service_ready_if_already_open(&self, service: &str, was_open: bool) {
        if !was_open {
            return;
        }
        if let Some(status) = &self.status {
            status.update(|next| {
                next.record_service_state(
                    service.to_string(),
                    true,
                    "ServiceReady",
                    Some("service available for subscription".to_string()),
                );
            });
        }
    }

    fn record_service_open_error(&self, service: &str, error: &BlpError) {
        if let Some(status) = &self.status {
            let message_type = match error {
                BlpError::Timeout => "ServiceOpenTimeout",
                _ => "ServiceOpenFailure",
            };
            let detail = Some(error.to_string());
            status.update(|next| {
                let already_recorded = next.events().back().is_some_and(|event| {
                    event.category == SubscriptionEventCategory::Service
                        && event.topic.as_deref() == Some(service)
                        && event.message_type == message_type
                });
                if !already_recorded {
                    next.record_service_state(
                        service.to_string(),
                        false,
                        message_type,
                        detail.clone(),
                    );
                }
            });
        }
    }

    fn register_subscriptions(
        &mut self,
        request: SubscriptionRegistrationRequest,
    ) -> Result<RegisteredSubscriptions, BlpError> {
        let SubscriptionRegistrationRequest {
            topics,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            stream,
            forwarder,
        } = request;
        if stream.is_closed() {
            return Err(BlpError::SubscriptionFailure {
                cid: None,
                label: Some("cannot subscribe with a terminal subscription stream".to_string()),
            });
        }
        let stream_terminator = stream.terminator();
        let mut sub_list = SubscriptionList::new();
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        let options_str = options.join(",");
        let mut keys = Vec::with_capacity(topics.len());
        let mut metrics = Vec::with_capacity(topics.len());
        let mut registered_topics = Vec::with_capacity(topics.len());
        let ft = flush_threshold.unwrap_or(self.config.subscription_flush_threshold);
        let op = overflow_policy.unwrap_or(self.config.overflow_policy);

        for topic in &topics {
            let state = SubscriptionState::with_policy_and_forwarder(
                topic.clone(),
                fields.clone(),
                stream.clone(),
                ft,
                op,
                all_fields,
                forwarder.clone(),
            );
            let metrics_arc = state.metrics.clone();
            let key = self.subs.insert(state);
            if let Some(state) = self.subs.get_mut(key) {
                state.set_topic_id(key as u32);
            }
            let cid = DispatchKey::from_slab_key(key).to_correlation_id();
            if let Err(e) = sub_list.add(topic, &field_refs, &options_str, &cid) {
                xbbg_log::error!(worker_id = self.id, topic = %topic, error = %e, "failed to add topic");
                self.subs.remove(key);
                continue;
            }
            keys.push(key);
            metrics.push(metrics_arc);
            registered_topics.push(topic.clone());
            xbbg_log::debug!(worker_id = self.id, topic = %topic, key = key, "subscription added");
        }

        if keys.is_empty() {
            return Err(BlpError::SubscriptionFailure {
                cid: None,
                label: Some("failed to build any subscription entries".to_string()),
            });
        }
        self.stream_terminator = Some(stream_terminator);
        Ok((sub_list, keys, metrics, registered_topics))
    }

    fn build_unsubscribe_list(&mut self, keys: Vec<SlabKey>) -> (SubscriptionList, usize) {
        let mut unsub_list = SubscriptionList::new();
        let mut pending_keys = Vec::with_capacity(keys.len());
        for &key in &keys {
            if self.subs.contains(key) && !self.pending_cancel.contains(&key) {
                let state = &mut self.subs[key];
                state.mark_closing();
                let cid = DispatchKey::from_slab_key(key).to_correlation_id();
                if let Err(e) = unsub_list.add(&state.topic, &[], "", &cid) {
                    xbbg_log::error!(worker_id = self.id, key = key, error = %e, "failed to build unsub list entry");
                } else {
                    pending_keys.push(key);
                }
            }
        }

        for &key in &pending_keys {
            self.pending_cancel.insert(key);
            xbbg_log::debug!(
                worker_id = self.id,
                key = key,
                "subscription pending cancel"
            );
        }
        if let Some(status) = &self.status {
            if !pending_keys.is_empty() {
                status.update(|next| {
                    for &key in &pending_keys {
                        let topic = next.mark_topic_unsubscribing(key);
                        next.record_subscription_event(
                            "SubscriptionPendingCancel",
                            topic,
                            None,
                            SubscriptionEventLevel::Info,
                        );
                    }
                });
            }
        }

        (unsub_list, pending_keys.len())
    }

    fn dispatch_event(&mut self, ev: xbbg_core::Event, shared: &SubscriptionWorkerShared) {
        let et = ev.event_type();
        if et == EventType::SubscriptionStatus {
            let mut mutations = Vec::new();
            for msg in ev.iter() {
                self.collect_subscription_status(&msg, &mut mutations);
            }
            if let Some(status) = &self.status {
                if !mutations.is_empty() {
                    status.update(|next| {
                        for mutation in mutations {
                            mutation.apply(next);
                        }
                    });
                }
            }
            return;
        }
        if et == EventType::SubscriptionData {
            let mut data_loss_topics = Vec::new();
            let mut streaming_topics = Vec::new();
            let mut channel_closed = false;
            for msg in ev.iter() {
                self.collect_subscription_data(
                    &msg,
                    &mut data_loss_topics,
                    &mut streaming_topics,
                    &mut channel_closed,
                );
            }
            streaming_topics.retain(|(key, _)| {
                self.subs
                    .get(*key)
                    .is_some_and(|state| !state.stream.is_closed())
            });
            let inactive_topics = if channel_closed {
                self.subs
                    .iter()
                    .filter(|(_, state)| state.stream.is_closed())
                    .map(|(_, state)| state.topic.to_string())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if let Some(status) = &self.status {
                if !data_loss_topics.is_empty()
                    || !streaming_topics.is_empty()
                    || !inactive_topics.is_empty()
                {
                    status.update(|next| {
                        for topic in data_loss_topics {
                            next.record_admin_data_loss(
                                Some(topic),
                                Some("subscription data reported DATALOSS".to_string()),
                            );
                        }
                        for (key, topic) in streaming_topics {
                            next.mark_topic_streaming(key);
                            next.record_subscription_event(
                                "SubscriptionStreaming",
                                Some(topic),
                                None,
                                SubscriptionEventLevel::Info,
                            );
                        }
                        for topic in inactive_topics {
                            let _ = next.set_topic_streams_active(&topic, false);
                        }
                    });
                }
            }
            return;
        }
        for msg in ev.iter() {
            match et {
                EventType::SessionStatus | EventType::AuthorizationStatus => {
                    self.handle_session_status(&msg, shared);
                }
                EventType::ServiceStatus => self.handle_service_status(&msg),
                EventType::Admin => self.handle_admin_event(&msg),
                _ => {}
            }
        }
    }

    fn collect_subscription_data(
        &mut self,
        msg: &xbbg_core::Message<'_>,
        data_loss_topics: &mut Vec<String>,
        streaming_topics: &mut Vec<(SlabKey, String)>,
        channel_closed: &mut bool,
    ) {
        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            let key = dispatch_key.to_slab_key();
            if self.pending_cancel.contains(&key) {
                continue;
            }
            if let Some(state) = self.subs.get_mut(key) {
                match state.on_message(msg) {
                    MessageOutcome::DataLoss => {
                        data_loss_topics.push(state.topic.to_string());
                        *channel_closed = true;
                    }
                    MessageOutcome::Closed => {
                        *channel_closed = true;
                    }
                    MessageOutcome::Normal { first_message } => {
                        if first_message {
                            let topic = state.topic.to_string();
                            streaming_topics.push((key, topic.clone()));
                            xbbg_log::debug!(
                                worker_id = self.id,
                                key,
                                topic,
                                "subscription entered streaming state"
                            );
                        }
                    }
                }
            }
        }
    }

    fn collect_subscription_status(
        &mut self,
        msg: &xbbg_core::Message<'_>,
        mutations: &mut Vec<StatusMutation>,
    ) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        let reason = msg
            .elements()
            .get_by_str("reason")
            .and_then(|value| value.get_by_str("description"))
            .and_then(|value| value.get_str(0))
            .map(str::to_string);

        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            let key = dispatch_key.to_slab_key();
            match msg_type {
                "SubscriptionStarted" => {
                    if self
                        .subs
                        .get(key)
                        .is_some_and(|state| !state.stream.is_closed())
                    {
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key,
                            reason = %reason.as_deref().unwrap_or(""),
                            "subscription started"
                        );
                        mutations.push(StatusMutation::Started {
                            key,
                            reason: reason.clone(),
                        });
                    }
                }
                "SubscriptionFailure" => {
                    if self.pending_cancel.remove(&key) {
                        if self.subs.contains(key) {
                            let mut state = self.subs.remove(key);
                            state.mark_closing();
                            mutations.push(StatusMutation::Unsubscribed {
                                key,
                                message_type: "SubscriptionCancelled",
                                reason: reason.clone(),
                            });
                        }
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key,
                            "pending cancel confirmed via SubscriptionFailure"
                        );
                    } else {
                        let reason_text = reason
                            .clone()
                            .unwrap_or_else(|| "subscription failed".to_string());
                        if self.subs.contains(key) {
                            let mut state = self.subs.remove(key);
                            state.mark_closing();
                            let topic = state.topic.to_string();
                            mutations.push(StatusMutation::Failed {
                                key,
                                fallback_topic: topic.clone(),
                                reason: reason_text.clone(),
                                kind: SubscriptionFailureKind::Failure,
                                message_type: "SubscriptionFailure",
                            });
                            xbbg_log::warn!(
                                worker_id = self.id,
                                key,
                                topic,
                                reason = %reason_text,
                                "subscription failed for topic"
                            );
                            if self.subs.is_empty() && self.pending_cancel.is_empty() {
                                state.fail(BlpError::SubscriptionFailure {
                                    cid: None,
                                    label: Some(format!(
                                        "All subscriptions failed; last failure: {} ({})",
                                        state.topic, reason_text,
                                    )),
                                });
                            }
                        }
                    }
                }
                "SubscriptionTerminated" => {
                    if self.pending_cancel.remove(&key) {
                        if self.subs.contains(key) {
                            let mut state = self.subs.remove(key);
                            state.mark_closing();
                            mutations.push(StatusMutation::Unsubscribed {
                                key,
                                message_type: "SubscriptionTerminated",
                                reason: reason.clone(),
                            });
                        }
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key,
                            "pending cancel confirmed by Bloomberg"
                        );
                    } else {
                        let reason_text = reason
                            .clone()
                            .unwrap_or_else(|| "subscription terminated".to_string());
                        if self.subs.contains(key) {
                            let mut state = self.subs.remove(key);
                            state.mark_closing();
                            let topic = state.topic.to_string();
                            mutations.push(StatusMutation::Failed {
                                key,
                                fallback_topic: topic.clone(),
                                reason: reason_text.clone(),
                                kind: SubscriptionFailureKind::Terminated,
                                message_type: "SubscriptionTerminated",
                            });
                            xbbg_log::warn!(
                                worker_id = self.id,
                                key,
                                topic,
                                reason = %reason_text,
                                "subscription terminated for topic"
                            );
                            if self.subs.is_empty() && self.pending_cancel.is_empty() {
                                state.fail(BlpError::SubscriptionFailure {
                                    cid: None,
                                    label: Some(format!(
                                        "All subscriptions ended; last termination: {} ({})",
                                        state.topic, reason_text,
                                    )),
                                });
                            }
                        }
                    }
                }
                "SubscriptionStreamsActivated" => {
                    if self
                        .subs
                        .get(key)
                        .is_some_and(|state| !state.stream.is_closed())
                    {
                        self.last_streams_warn_us.remove(&key);
                        mutations.push(StatusMutation::StreamsActive {
                            key,
                            active: true,
                            reason: reason.clone(),
                        });
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key,
                            "subscription streams activated"
                        );
                    }
                }
                "SubscriptionStreamsDeactivated" => {
                    if self.subs.contains(key) {
                        mutations.push(StatusMutation::StreamsActive {
                            key,
                            active: false,
                            reason: reason.clone(),
                        });
                    }
                    xbbg_log::warn!(
                        worker_id = self.id,
                        key,
                        reason = %reason.as_deref().unwrap_or(""),
                        "subscription streams deactivated"
                    );
                }
                _ => {
                    xbbg_log::trace!(worker_id = self.id, key, msg_type, "subscription status");
                }
            }
        }
        for (key, state) in self.subs.iter() {
            if state.stream.is_closed() {
                mutations.push(StatusMutation::StreamsActive {
                    key,
                    active: false,
                    reason: None,
                });
            }
        }
    }

    fn handle_session_status(
        &mut self,
        msg: &xbbg_core::Message<'_>,
        shared: &SubscriptionWorkerShared,
    ) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        match msg_type {
            "SessionStarted" => {
                if !shared.shutdown_requested() {
                    shared
                        .health
                        .store(WorkerHealth::Healthy as u8, Ordering::Release);
                }
                shared.resolve_startup(Ok(()));
                xbbg_log::info!(worker_id = self.id, "session started");
                if let Some(status) = &self.status {
                    status.update(|next| {
                        next.record_session_state(
                            SessionLifecycleState::Up,
                            "SessionStarted",
                            None,
                        );
                    });
                }
            }
            "SessionConnectionDown" => {
                // Bloomberg SDK contract: SessionConnectionDown is informational.
                // The SDK's auto_restart_on_disconnection machinery attempts reconnection
                // and can recover active subscriptions after a successful reconnect
                // (see BLPAPI ChangeLog v3.11.6).
                // We just record state for diagnostics; do NOT drain subscriptions and
                // do NOT resubscribe on the subsequent Up.
                let reason = extract_reason_description(msg);
                let context = SubscriptionSessionEvent::ConnectionDown
                    .log_context(shared.shutdown_requested());
                log_subscription_session_event(
                    context,
                    self.id,
                    self.subs.len(),
                    reason.as_deref(),
                );
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| {
                        Some(format!(
                            "worker={} active_subscriptions={}",
                            self.id,
                            self.subs.len(),
                        ))
                    });
                    status.update(|next| {
                        next.record_session_state(
                            SessionLifecycleState::Down,
                            "SessionConnectionDown",
                            detail.clone(),
                        );
                    });
                }
            }
            "AuthorizationRevoked" => {
                // Session identity was revoked mid-session (e.g. token expired,
                // entitlement change). Any authorized request/subscribe will now
                // fail. Treat this as terminal for the worker: we have no
                // re-auth flow, so drain subs, mark Dead, and let the pool spawn
                // a fresh worker that re-auths during startup.
                let reason = extract_reason_description(msg);
                xbbg_log::error!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "AuthorizationRevoked — identity gone; closing subscriptions"
                );
                let detail = format!(
                    "Bloomberg session identity revoked (worker={}){}. \
                     Subscription closed. Please re-authenticate and resubscribe.",
                    self.id,
                    reason
                        .as_deref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default(),
                );
                self.drain_for_shutdown(&detail);
                shared.mark_shutdown_requested();
                shared.stop_forwarder();
                shared
                    .health
                    .store(WorkerHealth::Dead as u8, Ordering::Release);
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| Some(format!("worker={}", self.id)));
                    status.update(|next| {
                        next.record_session_state(
                            SessionLifecycleState::Terminated,
                            "AuthorizationRevoked",
                            detail.clone(),
                        );
                    });
                }
            }
            "SessionStartupFailure" => {
                let reason = extract_reason_description(msg);
                shared
                    .health
                    .store(WorkerHealth::Dead as u8, Ordering::Release);
                shared.resolve_startup(Err(session_start_error(
                    "subscription session startup failure",
                    reason.clone(),
                )));
                xbbg_log::error!(worker_id = self.id, reason = %reason.as_deref().unwrap_or(""), "subscription session startup failed");
            }
            "SessionTerminated" => {
                let reason = extract_reason_description(msg);
                let context =
                    SubscriptionSessionEvent::Terminated.log_context(shared.shutdown_requested());
                shared.resolve_startup(Err(session_start_error(
                    "subscription session terminated during startup",
                    reason.clone(),
                )));
                log_subscription_session_event(
                    context,
                    self.id,
                    self.subs.len(),
                    reason.as_deref(),
                );
                // Session is dead. Send error to all consumers and remove all subs.
                let detail = format!(
                    "Bloomberg session terminated (worker={}){}. \
                     Subscription closed. Please resubscribe.",
                    self.id,
                    reason
                        .as_deref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default(),
                );
                self.drain_for_shutdown(&detail);
                shared.mark_shutdown_requested();
                shared.stop_forwarder();
                // Mark the worker Dead so the pool refuses to hand it out to
                // new claims — the session ptr is terminated and can't be restarted.
                shared
                    .health
                    .store(WorkerHealth::Dead as u8, Ordering::Release);
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| Some(format!("worker={}", self.id)));
                    status.update(|next| {
                        next.record_session_state(
                            SessionLifecycleState::Terminated,
                            "SessionTerminated",
                            detail.clone(),
                        );
                    });
                }
            }
            "SessionConnectionUp" => {
                // Informational. The SDK has re-established the TCP connection.
                // SubscriptionStreams{Activated,Deactivated} events report whether
                // individual subscriptions become active again.
                let reason = extract_reason_description(msg);
                xbbg_log::info!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "SessionConnectionUp — informational; SDK re-established the connection"
                );
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| {
                        Some(format!(
                            "worker={} active_subscriptions={}",
                            self.id,
                            self.subs.len(),
                        ))
                    });
                    status.update(|next| {
                        next.record_session_state(
                            SessionLifecycleState::Up,
                            "SessionConnectionUp",
                            detail.clone(),
                        );
                    });
                }
            }
            _ => {
                xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "session status");
            }
        }
    }

    fn handle_service_status(&mut self, msg: &xbbg_core::Message<'_>) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();

        if matches!(msg_type, "ServiceOpened" | "ServiceOpenFailure") {
            if let Some(CorrelationId::Int(cid_int)) = msg.correlation_id(0) {
                let service = self
                    .pending_service_opens
                    .iter()
                    .find_map(|(service, open)| (open.cid == cid_int).then(|| service.clone()));
                if let Some(service_name) = service {
                    let open = self
                        .pending_service_opens
                        .remove(&service_name)
                        .expect("found pending open");
                    match msg_type {
                        "ServiceOpened" => {
                            self.open_services.insert(service_name.clone());
                            if let Some(status) = &self.status {
                                let service_for_status = service_name.clone();
                                status.update(|next| {
                                    next.record_service_state(
                                        service_for_status,
                                        true,
                                        "ServiceOpened",
                                        Some("service opened on demand".to_string()),
                                    );
                                });
                            }
                            for waiter in open.waiters {
                                let _ = waiter.send(Ok(()));
                            }
                        }
                        "ServiceOpenFailure" => {
                            let reason = extract_reason_description(msg);
                            if let Some(status) = &self.status {
                                let service_for_status = service_name.clone();
                                let reason_for_status = reason.clone();
                                status.update(|next| {
                                    next.record_service_state(
                                        service_for_status,
                                        false,
                                        "ServiceOpenFailure",
                                        reason_for_status,
                                    );
                                });
                            }
                            for waiter in open.waiters {
                                let _ = waiter.send(Err(BlpError::OpenService {
                                    service: service_name.clone(),
                                    source: None,
                                    label: reason.clone(),
                                }));
                            }
                        }
                        _ => {}
                    }
                    return;
                }
            }
        }

        let service = msg
            .elements()
            .get_by_str("serviceName")
            .and_then(|value| value.get_str(0))
            .map(str::to_string);
        if let Some(status) = &self.status {
            match msg_type {
                "ServiceDown" => {
                    let service_name = service.clone().unwrap_or_else(|| "unknown".to_string());
                    let active_subs = self.subs.len();
                    let has_active = !self.subs.is_empty();
                    status.update(|next| {
                        next.record_service_state(service_name.clone(), false, msg_type, None);
                        if has_active {
                            next.record_subscription_event(
                                "ServiceDownAffectsActiveSubscriptions",
                                None,
                                Some(format!(
                                    "service={} active_subscriptions={}",
                                    service_name, active_subs,
                                )),
                                SubscriptionEventLevel::Warning,
                            );
                        }
                    });
                    if has_active {
                        xbbg_log::warn!(
                            worker_id = self.id,
                            service = %service_name,
                            active_subs = active_subs,
                            "ServiceDown — active subscriptions may be silently quieted"
                        );
                    }
                }
                "ServiceUp" | "ServiceOpened" => {
                    let service_name = service.unwrap_or_else(|| "unknown".to_string());
                    status.update(|next| {
                        next.record_service_state(service_name.clone(), true, msg_type, None);
                    });
                }
                _ => {}
            }
        }
        xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "service status");
    }

    fn handle_admin_event(&mut self, msg: &xbbg_core::Message<'_>) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        match msg_type {
            "SlowConsumerWarning" => {
                if let Some(status) = &self.status {
                    status.update(|next| {
                        next.record_admin_warning(msg_type, None);
                    });
                }
                xbbg_log::warn!(worker_id = self.id, "slow consumer warning");
            }
            "SlowConsumerWarningCleared" => {
                for (_, state) in self.subs.iter_mut() {
                    state.clear_slow_consumer();
                }
                if let Some(status) = &self.status {
                    status.update(|next| {
                        next.record_admin_warning_cleared(msg_type, None);
                    });
                }
                xbbg_log::info!(worker_id = self.id, "slow consumer warning cleared");
            }
            "DataLoss" => {
                let correlation_count = msg.num_correlation_ids();
                let mut resolved_keys = Vec::with_capacity(correlation_count);
                let mut seen_keys = HashSet::with_capacity(correlation_count);
                let mut unattributable = correlation_count == 0;
                for index in 0..correlation_count {
                    let Some(correlation_id) = msg.correlation_id(index) else {
                        unattributable = true;
                        continue;
                    };
                    let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id)
                    else {
                        unattributable = true;
                        continue;
                    };
                    let key = dispatch_key.to_slab_key();
                    if !self.subs.contains(key) {
                        unattributable = true;
                        continue;
                    }
                    if seen_keys.insert(key) {
                        resolved_keys.push(key);
                    }
                }
                self.apply_admin_data_loss(msg.time_received_us(), resolved_keys, unattributable);
                xbbg_log::warn!(worker_id = self.id, "data loss event received");
            }
            _ => {
                if let Some(status) = &self.status {
                    status.update(|next| {
                        next.push_event(
                            super::SubscriptionEventCategory::Admin,
                            SubscriptionEventLevel::Info,
                            msg_type,
                            None,
                            None,
                        );
                    });
                }
                xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "admin event");
            }
        }
    }

    fn apply_admin_data_loss(
        &mut self,
        timestamp_us: Option<i64>,
        resolved_keys: Vec<SlabKey>,
        unattributable: bool,
    ) {
        let mut topics = Vec::new();
        if unattributable {
            if let Some(terminator) = &self.stream_terminator {
                terminator.fail(BlpError::SubscriptionDataLoss {
                    topic: "*".to_string(),
                    detail:
                        "Bloomberg reported unattributable DATALOSS; resubscribe for a fresh image"
                            .to_string(),
                });
            }
            topics.reserve(self.subs.len());
            for (_, state) in self.subs.iter_mut() {
                topics.push(state.topic.to_string());
                state.on_dataloss(timestamp_us);
            }
        } else {
            topics.reserve(resolved_keys.len());
            for key in resolved_keys {
                if let Some(state) = self.subs.get_mut(key) {
                    topics.push(state.topic.to_string());
                    state.on_dataloss(timestamp_us);
                }
            }
        }

        let inactive_topics = self
            .subs
            .iter()
            .filter(|(_, state)| state.stream.is_closed())
            .map(|(_, state)| state.topic.to_string())
            .collect::<Vec<_>>();
        if let Some(status) = &self.status {
            status.update(|next| {
                if unattributable || topics.is_empty() {
                    next.record_admin_data_loss(None, None);
                } else {
                    for topic in topics {
                        next.record_admin_data_loss(Some(topic), None);
                    }
                }
                for topic in inactive_topics {
                    let _ = next.set_topic_streams_active(&topic, false);
                }
            });
        }
    }

    /// Check if any topics are in streams-deactivated state longer than the configured
    /// warn threshold and emit a one-shot Warning event so callers polling status
    /// see "your data is quiet, not broken — SDK is still trying to recover".
    fn check_streams_deactivated(&mut self) {
        if self.subs.iter().any(|(_, state)| state.stream.is_closed()) {
            if let Some(status) = &self.status {
                let topics = {
                    let snapshot = status.load();
                    snapshot
                        .topic_statuses()
                        .iter()
                        .filter(|(_, info)| info.streams_active)
                        .map(|(topic, _)| topic.clone())
                        .collect::<Vec<_>>()
                };
                if !topics.is_empty() {
                    status.update(|next| {
                        for topic in topics {
                            let _ = next.set_topic_streams_active(&topic, false);
                        }
                    });
                }
            }
            return;
        }
        let warn_ms = self.config.streams_deactivated_warn_ms;
        if warn_ms == 0 {
            return;
        }
        let Some(status_arc) = self.status.clone() else {
            return;
        };

        let now = super::timestamp_now_us();
        let warn_us = (warn_ms as i64) * 1_000;

        // Collect keys to warn about without holding a snapshot across the warn emission.
        let mut to_warn: Vec<(SlabKey, String, i64)> = Vec::new();
        {
            let snapshot = status_arc.load();
            for (topic, info) in snapshot.topic_statuses().iter() {
                if info.streams_active {
                    continue;
                }
                // Only warn for topics that have actually been deactivated (not pre-streaming).
                if info.streams_changed_us == 0 {
                    continue;
                }
                let elapsed = now - info.streams_changed_us;
                if elapsed < warn_us {
                    continue;
                }
                // Map back to slab key for debouncing.
                if let Some(&key) = snapshot.topic_to_key().get(topic) {
                    let last_warn = self.last_streams_warn_us.get(&key).copied().unwrap_or(0);
                    if now - last_warn >= warn_us {
                        to_warn.push((key, topic.clone(), elapsed));
                    }
                }
            }
        }

        if to_warn.is_empty() {
            return;
        }

        for (key, topic, elapsed_us) in &to_warn {
            self.last_streams_warn_us.insert(*key, now);
            xbbg_log::warn!(
                worker_id = self.id,
                topic = %topic,
                elapsed_ms = elapsed_us / 1_000,
                "subscription streams still deactivated"
            );
        }
        status_arc.update(|next| {
            for (_, topic, elapsed_us) in &to_warn {
                let detail = format!(
                    "topic has been streams-inactive for {}ms; SDK is still trying to recover",
                    elapsed_us / 1_000
                );
                next.record_subscription_event(
                    "SubscriptionStreamsDeactivatedPersisting",
                    Some(topic.clone()),
                    Some(detail),
                    SubscriptionEventLevel::Warning,
                );
            }
        });
    }
}

fn session_start_error(context: &str, reason: Option<String>) -> BlpError {
    BlpError::SessionStart {
        source: None,
        label: Some(match reason {
            Some(reason) => format!("{context}: {reason}"),
            None => context.to_string(),
        }),
    }
}

fn extract_reason_description(msg: &xbbg_core::Message<'_>) -> Option<String> {
    let reason = msg.elements().get_by_str("reason")?;
    for key in ["description", "category", "message"] {
        if let Some(s) = reason.get_by_str(key).and_then(|e| e.get_str(0)) {
            return Some(s.to_string());
        }
    }
    None
}

struct SubscriptionWorkerHandleInner {
    id: usize,
    session: Arc<AsyncSession>,
    shared: Arc<SubscriptionWorkerShared>,
    health: Arc<AtomicU8>,
    monitor: Mutex<Option<JoinHandle<()>>>,
    stop_started: AtomicBool,
}

impl SubscriptionWorkerHandleInner {
    fn ensure_monitor_started(self: &Arc<Self>) -> Result<(), BlpAsyncError> {
        let mut monitor = self.monitor.lock();
        if monitor.is_some() || self.shared.shutdown_requested() {
            return Ok(());
        }
        let runtime =
            self.shared
                .runtime_handle
                .get()
                .ok_or_else(|| BlpAsyncError::ConfigError {
                    detail: "subscription session runtime is not attached".to_string(),
                })?;
        let shared = Arc::clone(&self.shared);
        *monitor = Some(runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if shared.shutdown_requested()
                    || shared.health.load(Ordering::Acquire) == WorkerHealth::Dead as u8
                {
                    return;
                }
                shared.check_streams_deactivated();
            }
        }));
        Ok(())
    }

    fn reusable(&self) -> bool {
        self.health.load(Ordering::Acquire) != WorkerHealth::Dead as u8 && self.shared.reusable()
    }

    fn signal_shutdown(&self) {
        let _lifecycle = self.shared.lifecycle.write();
        if self.stop_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let first_request = self.shared.mark_shutdown_requested();
        if first_request {
            xbbg_log::info!(
                worker_id = self.id,
                shutdown_requested = true,
                "subscription worker shutdown requested"
            );
        }
        if let Some(handle) = self.monitor.lock().take() {
            handle.abort();
        }
        self.shared.stop_forwarder();
        self.shared
            .drain_for_shutdown("Bloomberg subscription session is shutting down");
        self.session.stop_async();
    }

    fn shutdown_blocking(&self) {
        let _lifecycle = self.shared.lifecycle.write();
        self.stop_started.store(true, Ordering::Release);
        let first_request = self.shared.mark_shutdown_requested();
        if first_request {
            xbbg_log::info!(
                worker_id = self.id,
                shutdown_requested = true,
                "subscription worker blocking shutdown requested"
            );
        }
        if let Some(handle) = self.monitor.lock().take() {
            handle.abort();
        }
        self.shared.stop_forwarder();
        self.shared
            .drain_for_shutdown("Bloomberg subscription session is shutting down");
        self.session.stop();
    }
}

struct ClaimLease {
    valid: AtomicBool,
    command_count: AtomicUsize,
    _permit: OwnedSemaphorePermit,
}

impl ClaimLease {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            valid: AtomicBool::new(true),
            command_count: AtomicUsize::new(0),
            _permit: permit,
        }
    }

    fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
    }

    fn command_count(&self) -> usize {
        self.command_count.load(Ordering::Acquire)
    }
}

struct CommandLeaseRef {
    lease: Arc<ClaimLease>,
}

impl CommandLeaseRef {
    fn try_new(lease: Arc<ClaimLease>) -> Result<Self, BlpAsyncError> {
        if !lease.valid.load(Ordering::Acquire) {
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription claim lease has ended".to_string(),
            });
        }
        lease.command_count.fetch_add(1, Ordering::AcqRel);
        if !lease.valid.load(Ordering::Acquire) {
            lease.command_count.fetch_sub(1, Ordering::AcqRel);
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription claim lease has ended".to_string(),
            });
        }
        Ok(Self { lease })
    }

    fn validate(&self) -> Result<(), BlpAsyncError> {
        if self.lease.valid.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(BlpAsyncError::ConfigError {
                detail: "subscription claim lease has ended".to_string(),
            })
        }
    }
}

impl Clone for CommandLeaseRef {
    fn clone(&self) -> Self {
        self.lease.command_count.fetch_add(1, Ordering::AcqRel);
        Self {
            lease: Arc::clone(&self.lease),
        }
    }
}

impl Drop for CommandLeaseRef {
    fn drop(&mut self) {
        self.lease.command_count.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct SubscriptionCommandHandle {
    inner: Arc<SubscriptionWorkerHandleInner>,
    lease: CommandLeaseRef,
}

impl SubscriptionCommandHandle {
    fn validate_lease_blp(&self) -> Result<(), BlpError> {
        self.lease.validate().map_err(|error| BlpError::Internal {
            detail: error.to_string(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: SubscriptionSender,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        if stream.is_closed() {
            return Err(BlpAsyncError::ConfigError {
                detail: "cannot subscribe with a terminal subscription stream".to_string(),
            });
        }
        self.lease.validate()?;
        {
            let _lifecycle = self.inner.shared.lifecycle.read();
            self.lease.validate()?;
            self.inner.shared.ensure_running()?;
            self.inner.shared.set_status(status.clone());
        }

        let was_open = self.inner.shared.service_is_open(&service);
        if let Err(error) = self.ensure_service(&service).await {
            self.inner
                .shared
                .record_service_open_error(&service, &error);
            xbbg_log::error!(
                worker_id = self.worker_id(),
                service = %service,
                error = %error,
                "failed to open service"
            );
            return Err(BlpAsyncError::BlpError(error));
        }
        let stream_status = stream.clone();

        let (keys, metrics, registered_topics, subscribe_result) = {
            self.lease.validate()?;
            let _lifecycle = self.inner.shared.lifecycle.read();
            self.lease.validate()?;
            self.inner.shared.ensure_running()?;
            self.inner.ensure_monitor_started()?;
            let forwarder = if self.inner.shared.effective_overflow_policy(overflow_policy)
                == OverflowPolicy::Block
            {
                Some(self.inner.shared.ensure_forwarder()?)
            } else {
                None
            };
            self.inner
                .shared
                .record_service_ready_if_already_open(&service, was_open);
            let request = SubscriptionRegistrationRequest {
                topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                stream,
                forwarder,
            };
            let (sub_list, keys, metrics, registered_topics) = self
                .inner
                .shared
                .register_subscriptions(request)
                .map_err(BlpAsyncError::BlpError)?;
            status.update(|next| {
                next.add_active(&registered_topics, &keys, metrics.clone());
            });
            let result = self.inner.session.subscribe(&sub_list, None);
            (keys, metrics, registered_topics, result)
        };

        if let Err(error) = subscribe_result {
            self.inner.shared.cleanup_failed_subscribe(&keys);
            status.update(|next| {
                for topic in &registered_topics {
                    next.drop_topic(topic);
                }
            });
            xbbg_log::error!(
                worker_id = self.worker_id(),
                error = %error,
                "subscribe failed; quarantining subscription worker"
            );
            self.inner.signal_shutdown();
            return Err(BlpAsyncError::BlpError(error));
        }
        if stream_status.is_closed() {
            xbbg_log::warn!(
                worker_id = self.worker_id(),
                "subscription stream terminated during subscribe; quarantining worker"
            );
            self.inner.signal_shutdown();
            return Err(BlpAsyncError::ConfigError {
                detail: "subscription stream terminated during subscribe".to_string(),
            });
        }
        Ok((keys, metrics))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_topics(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: SubscriptionSender,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.subscribe(
            service,
            topics,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            stream,
            status,
        )
        .await
    }

    async fn ensure_service(&self, service: &str) -> Result<(), BlpError> {
        self.validate_lease_blp()?;
        if self.inner.shared.service_is_open(service) {
            return Ok(());
        }
        let (attempt_cid, rx) = {
            self.validate_lease_blp()?;
            let _lifecycle = self.inner.shared.lifecycle.read();
            self.validate_lease_blp()?;
            if self.inner.shared.shutdown_requested() {
                return Err(BlpError::Internal {
                    detail: "subscription session is shut down".to_string(),
                });
            }
            let (should_open, cid_int, rx) = self.inner.shared.register_service_waiter(service);
            if should_open {
                xbbg_log::info!(
                    worker_id = self.worker_id(),
                    service = service,
                    "opening service on demand (async)"
                );
                let cid = CorrelationId::Int(cid_int);
                if let Err(error) = self.inner.session.open_service_async(service, &cid) {
                    self.inner
                        .shared
                        .remove_pending_service_open(service, cid_int);
                    return Err(error);
                }
            }
            (cid_int, rx)
        };
        let mut waiter =
            PendingSubscriptionServiceWaiter::new(&self.inner.shared, service, attempt_cid, rx);
        match tokio::time::timeout(
            Duration::from_millis(SERVICE_OPEN_TIMEOUT_MS),
            waiter.receiver(),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BlpError::Internal {
                detail: format!("pending service open for {service} was cancelled"),
            }),
            Err(_) => {
                if let Some(open) = self
                    .inner
                    .shared
                    .remove_pending_service_open(service, attempt_cid)
                {
                    for waiter in open.waiters {
                        let _ = waiter.send(Err(BlpError::Timeout));
                    }
                }
                Err(BlpError::Timeout)
            }
        }
    }

    pub async fn unsubscribe(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        self.unsubscribe_now(keys)
    }

    pub(crate) async fn drain_forwarder(&self) -> Result<(), BlpAsyncError> {
        self.lease.validate()?;
        self.inner.shared.drain_forwarder().await
    }

    fn unsubscribe_now(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        self.lease.validate()?;
        let unsubscribe_result = {
            let _lifecycle = self.inner.shared.lifecycle.read();
            self.lease.validate()?;
            self.inner.shared.ensure_running()?;
            let (unsub_list, unsub_count) = self.inner.shared.build_unsubscribe_list(keys);
            if unsub_count == 0 {
                return Ok(());
            }
            self.inner.session.unsubscribe(&unsub_list)
        };
        if let Err(error) = unsubscribe_result {
            xbbg_log::error!(
                worker_id = self.worker_id(),
                error = %error,
                "session.unsubscribe failed; quarantining subscription worker"
            );
            self.inner.signal_shutdown();
            return Err(BlpAsyncError::BlpError(error));
        }
        Ok(())
    }

    pub fn worker_id(&self) -> usize {
        self.inner.id
    }
}

pub struct SubscriptionWorkerHandle {
    inner: Arc<SubscriptionWorkerHandleInner>,
}

impl SubscriptionWorkerHandle {
    fn spawn(
        id: usize,
        config: Arc<EngineConfig>,
        runtime_handle: Option<tokio::runtime::Handle>,
    ) -> Result<Self, BlpError> {
        let options = build_session_options(&config, true)?;
        let health = Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8));
        let shared = Arc::new(SubscriptionWorkerShared::new(
            id,
            Arc::clone(&config),
            Arc::clone(&health),
        ));
        if let Some(runtime_handle) = runtime_handle {
            shared.attach_runtime(runtime_handle);
        }
        let handler_shared = Arc::clone(&shared);
        let session = AsyncSession::new(&options, move |event| {
            handler_shared.dispatch_event(event);
        })?;
        session
            .start()
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
        shared
            .wait_startup(Duration::from_millis(u64::from(SESSION_STARTUP_TIMEOUT_MS)))
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
        session.open_service(crate::services::Service::MktData.as_str())?;
        shared
            .state
            .lock()
            .open_services
            .insert(crate::services::Service::MktData.to_string());
        xbbg_log::info!(worker_id = id, "subscription worker pre-warmed");
        let session = Arc::new(session);
        Ok(Self {
            inner: Arc::new(SubscriptionWorkerHandleInner {
                id,
                session,
                shared,
                health,
                monitor: Mutex::new(None),
                stop_started: AtomicBool::new(false),
            }),
        })
    }

    fn id(&self) -> usize {
        self.inner.id
    }

    fn command_handle(&self, lease: CommandLeaseRef) -> SubscriptionCommandHandle {
        SubscriptionCommandHandle {
            inner: Arc::clone(&self.inner),
            lease,
        }
    }

    fn unsubscribe_now(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        let unsubscribe_result = {
            let _lifecycle = self.inner.shared.lifecycle.read();
            self.inner.shared.ensure_running()?;
            let (unsub_list, unsub_count) = self.inner.shared.build_unsubscribe_list(keys);
            if unsub_count == 0 {
                return Ok(());
            }
            self.inner.session.unsubscribe(&unsub_list)
        };
        if let Err(error) = unsubscribe_result {
            self.signal_shutdown();
            return Err(BlpAsyncError::BlpError(error));
        }
        Ok(())
    }

    fn signal_shutdown(&self) {
        self.inner.signal_shutdown();
    }
}

impl Drop for SubscriptionWorkerHandle {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

pub struct SubscriptionSessionPool {
    available: Mutex<Vec<SubscriptionWorkerHandle>>,
    all_workers: Mutex<Vec<Weak<SubscriptionWorkerHandleInner>>>,
    admission: Arc<Semaphore>,
    shutdown: AtomicBool,
    next_id: AtomicUsize,
    config: Arc<EngineConfig>,
    initial_size: usize,
    runtime_handle: OnceLock<tokio::runtime::Handle>,
    creations: Mutex<usize>,
    creation_cv: Condvar,
}

struct CreationGuard {
    pool: Arc<SubscriptionSessionPool>,
}

impl Drop for CreationGuard {
    fn drop(&mut self) {
        let mut creations = self.pool.creations.lock();
        debug_assert!(*creations > 0);
        *creations -= 1;
        if *creations == 0 {
            self.pool.creation_cv.notify_all();
        }
    }
}

impl SubscriptionSessionPool {
    pub fn new(size: usize, config: Arc<EngineConfig>) -> Result<Self, BlpAsyncError> {
        if config.max_subscription_sessions == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "max_subscription_sessions must be greater than zero".to_string(),
            });
        }
        if size > config.max_subscription_sessions {
            return Err(BlpAsyncError::ConfigError {
                detail:
                    "max_subscription_sessions must be greater than or equal to subscription_pool_size"
                        .to_string(),
            });
        }
        xbbg_log::info!(
            pool_size = size,
            max_sessions = config.max_subscription_sessions,
            "creating subscription session pool"
        );
        let mut available = Vec::with_capacity(size);
        let mut all_workers = Vec::with_capacity(size);
        for id in 0..size {
            let handle = SubscriptionWorkerHandle::spawn(id, Arc::clone(&config), None).map_err(
                |error| {
                    BlpAsyncError::BlpError(BlpError::Internal {
                        detail: format!("failed to spawn subscription worker {id}: {error}"),
                    })
                },
            )?;
            all_workers.push(Arc::downgrade(&handle.inner));
            available.push(handle);
        }
        xbbg_log::info!(pool_size = size, "subscription session pool ready");
        Ok(Self {
            available: Mutex::new(available),
            all_workers: Mutex::new(all_workers),
            admission: Arc::new(Semaphore::new(config.max_subscription_sessions)),
            shutdown: AtomicBool::new(false),
            next_id: AtomicUsize::new(size),
            config,
            initial_size: size,
            runtime_handle: OnceLock::new(),
            creations: Mutex::new(0),
            creation_cv: Condvar::new(),
        })
    }

    pub(crate) fn attach_runtime(&self, handle: tokio::runtime::Handle) {
        let _ = self.runtime_handle.set(handle.clone());
        for worker in self.registered_workers() {
            worker.shared.attach_runtime(handle.clone());
        }
    }

    fn shutdown_error() -> BlpAsyncError {
        BlpAsyncError::ConfigError {
            detail: "subscription session pool is shut down".to_string(),
        }
    }

    fn spawn_registered_worker(
        &self,
        id: usize,
    ) -> Result<SubscriptionWorkerHandle, BlpAsyncError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Self::shutdown_error());
        }
        let runtime_handle =
            self.runtime_handle
                .get()
                .cloned()
                .ok_or_else(|| BlpAsyncError::ConfigError {
                    detail: "subscription pool runtime is not attached".to_string(),
                })?;
        let handle =
            SubscriptionWorkerHandle::spawn(id, Arc::clone(&self.config), Some(runtime_handle))
                .map_err(|error| {
                    BlpAsyncError::BlpError(BlpError::Internal {
                        detail: format!("failed to create dynamic subscription worker: {error}"),
                    })
                })?;
        let mut all_workers = self.all_workers.lock();
        if self.shutdown.load(Ordering::Acquire) {
            drop(all_workers);
            handle.signal_shutdown();
            return Err(Self::shutdown_error());
        }
        all_workers.retain(|worker| worker.strong_count() > 0);
        all_workers.push(Arc::downgrade(&handle.inner));
        Ok(handle)
    }
    fn claim_with_permit(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
    ) -> Result<SessionClaim, BlpAsyncError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Self::shutdown_error());
        }
        let handle = {
            let mut available = self.available.lock();
            let mut chosen = None;
            while let Some(candidate) = available.pop() {
                if !candidate.inner.reusable() {
                    xbbg_log::warn!(
                        worker_id = candidate.id(),
                        "discarding dirty or dead subscription worker"
                    );
                    drop(candidate);
                    continue;
                }
                chosen = Some(candidate);
                break;
            }
            if let Some(handle) = chosen {
                xbbg_log::debug!(
                    worker_id = handle.id(),
                    remaining = available.len(),
                    "claimed session from pool"
                );
                handle
            } else {
                drop(available);
                let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                xbbg_log::warn!(
                    worker_id = new_id,
                    initial_size = self.initial_size,
                    max_sessions = self.config.max_subscription_sessions,
                    "subscription pool exhausted or all workers dirty, creating bounded session"
                );
                self.spawn_registered_worker(new_id)?
            }
        };
        if self.shutdown.load(Ordering::Acquire) || !handle.inner.reusable() {
            handle.signal_shutdown();
            return Err(Self::shutdown_error());
        }
        Ok(SessionClaim {
            handle: Some(handle),
            pool: Arc::clone(self),
            cleanup_status: None,
            lease: Arc::new(ClaimLease::new(permit)),
        })
    }

    fn begin_creation(self: &Arc<Self>) -> Result<CreationGuard, BlpAsyncError> {
        let mut creations = self.creations.lock();
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Self::shutdown_error());
        }
        *creations += 1;
        Ok(CreationGuard {
            pool: Arc::clone(self),
        })
    }

    pub async fn claim(self: &Arc<Self>) -> Result<SessionClaim, BlpAsyncError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Self::shutdown_error());
        }
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .map_err(|_| Self::shutdown_error())?;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Self::shutdown_error());
        }
        let creation = self.begin_creation()?;
        let pool = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _creation = creation;
            pool.claim_with_permit(permit)
        })
        .await
        .map_err(|join_error| {
            BlpAsyncError::BlpError(BlpError::Internal {
                detail: format!("subscription pool claim task failed: {join_error}"),
            })
        })?
    }

    fn release(&self, handle: SubscriptionWorkerHandle) {
        if self.shutdown.load(Ordering::Acquire) || !handle.inner.reusable() {
            xbbg_log::warn!(
                worker_id = handle.id(),
                "discarding dirty, dead, or post-shutdown subscription worker"
            );
            handle.signal_shutdown();
            drop(handle);
            return;
        }
        let mut available = self.available.lock();
        if self.shutdown.load(Ordering::Acquire)
            || !handle.inner.reusable()
            || !handle.inner.shared.prepare_for_reuse()
        {
            drop(available);
            handle.signal_shutdown();
            drop(handle);
            return;
        }
        xbbg_log::debug!(
            worker_id = handle.id(),
            pool_size = available.len() + 1,
            "session returned to pool"
        );
        available.push(handle);
    }

    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    fn registered_workers(&self) -> Vec<Arc<SubscriptionWorkerHandleInner>> {
        let mut registry = self.all_workers.lock();
        let mut workers = Vec::with_capacity(registry.len());
        registry.retain(|worker| {
            if let Some(worker) = worker.upgrade() {
                workers.push(worker);
                true
            } else {
                false
            }
        });
        workers
    }

    fn drain_available(&self) {
        let available = {
            let mut locked = self.available.lock();
            std::mem::take(&mut *locked)
        };
        drop(available);
    }

    pub fn signal_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.admission.close();
        let workers = self.registered_workers();
        self.drain_available();
        xbbg_log::info!(
            count = workers.len(),
            "signaling all subscription workers shutdown"
        );
        for worker in workers {
            worker.signal_shutdown();
        }
    }

    pub fn shutdown_blocking(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.admission.close();
        {
            let mut creations = self.creations.lock();
            while *creations != 0 {
                self.creation_cv.wait(&mut creations);
            }
        }
        let workers = self.registered_workers();
        self.drain_available();
        xbbg_log::info!(
            count = workers.len(),
            "shutting down all subscription workers (blocking)"
        );
        for worker in workers {
            worker.shutdown_blocking();
        }
    }
}

impl Drop for SubscriptionSessionPool {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

pub struct SessionClaim {
    handle: Option<SubscriptionWorkerHandle>,
    pool: Arc<SubscriptionSessionPool>,
    cleanup_status: Option<SharedSubscriptionStatus>,
    lease: Arc<ClaimLease>,
}

impl SessionClaim {
    pub fn command_handle(&self) -> Result<SubscriptionCommandHandle, BlpAsyncError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "session already released".to_string(),
            })?;
        let lease = CommandLeaseRef::try_new(Arc::clone(&self.lease))?;
        Ok(handle.command_handle(lease))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: SubscriptionSender,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.command_handle()?
            .subscribe(
                service,
                topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                stream,
                status,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_topics(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: SubscriptionSender,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.command_handle()?
            .add_topics(
                service,
                topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                stream,
                status,
            )
            .await
    }

    pub async fn unsubscribe(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        self.command_handle()?.unsubscribe(keys).await
    }

    /// Wait for every Block-policy update accepted before this call to leave
    /// the session forwarder. Keep consuming the subscription receiver while
    /// awaiting this barrier so a full receiver cannot stall the forwarder.
    pub async fn drain_forwarder(&self) -> Result<(), BlpAsyncError> {
        self.command_handle()?.drain_forwarder().await
    }

    pub fn set_cleanup_status(&mut self, status: SharedSubscriptionStatus) {
        self.cleanup_status = Some(status);
    }

    pub fn close_without_reuse(mut self, keys: Vec<SlabKey>) {
        self.lease.invalidate();
        if let Some(handle) = self.handle.take() {
            if !keys.is_empty() {
                let _ = handle.unsubscribe_now(keys);
            }
            handle.signal_shutdown();
            drop(handle);
        }
    }

    pub fn worker_id(&self) -> Option<usize> {
        self.handle.as_ref().map(SubscriptionWorkerHandle::id)
    }
}

impl Drop for SessionClaim {
    fn drop(&mut self) {
        self.lease.invalidate();
        let Some(handle) = self.handle.take() else {
            return;
        };
        let outstanding_commands = self.lease.command_count() != 0;
        let active_keys = self
            .cleanup_status
            .as_ref()
            .map(|status| status.load().keys().to_vec())
            .unwrap_or_default();
        if active_keys.is_empty() && !outstanding_commands && handle.inner.reusable() {
            self.pool.release(handle);
            return;
        }
        if !active_keys.is_empty() {
            let _ = handle.unsubscribe_now(active_keys);
        }
        if let Some(status) = &self.cleanup_status {
            status.update(|next| next.clear_active());
        }
        handle.signal_shutdown();
        drop(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::subscription_channel;
    use xbbg_core::test_support::TestEvent;

    fn register_topics(
        worker: &mut SubscriptionWorkerState,
        stream: &SubscriptionSender,
        topics: &[&str],
    ) -> (Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>, Vec<String>) {
        let (_, keys, metrics, topics) = worker
            .register_subscriptions(SubscriptionRegistrationRequest {
                topics: topics.iter().map(|topic| (*topic).to_string()).collect(),
                fields: vec!["PX_LAST".to_string()],
                all_fields: false,
                options: Vec::new(),
                flush_threshold: None,
                overflow_policy: None,
                stream: stream.clone(),
                forwarder: None,
            })
            .expect("register test subscriptions");
        (keys, metrics, topics)
    }

    fn remove_registered_topics(worker: &mut SubscriptionWorkerState, keys: &[SlabKey]) {
        for &key in keys {
            let mut state = worker.subs.remove(key);
            state.mark_closing();
        }
    }

    fn attach_active_status(
        worker: &mut SubscriptionWorkerState,
        topics: &[String],
        keys: &[SlabKey],
        metrics: &[Arc<SubscriptionMetrics>],
    ) -> SharedSubscriptionStatus {
        let metrics_by_key = keys.iter().copied().zip(metrics.iter().cloned()).collect();
        let status = Arc::new(crate::engine::SubscriptionStatusHandle::new(
            crate::engine::SubscriptionStatusState::from_active(
                topics.to_vec(),
                keys.to_vec(),
                metrics_by_key,
            ),
        ));
        status.update(|next| {
            for topic in topics {
                let _ = next.set_topic_streams_active(topic, true);
            }
        });
        worker.status = Some(status.clone());
        status
    }

    fn correlation_id_for_key(key: SlabKey) -> i64 {
        match DispatchKey::from_slab_key(key).to_correlation_id() {
            CorrelationId::Int(value) => value,
            _ => panic!("slab dispatch key must use an integer correlation id"),
        }
    }
    #[test]
    fn shutdown_state_is_monotonic_and_callback_visible() {
        let health = Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8));
        let shared = SubscriptionWorkerShared::new(0, Arc::new(EngineConfig::default()), health);

        assert!(!shared.shutdown_requested());
        assert!(shared.mark_shutdown_requested());
        assert!(shared.shutdown_requested());
        assert!(!shared.mark_shutdown_requested());
    }

    #[test]
    fn session_event_context_classifies_all_shutdown_states() {
        let cases = [
            (
                SubscriptionSessionEvent::ConnectionDown,
                false,
                LifecycleLogLevel::Warn,
                "connection_down_without_shutdown",
            ),
            (
                SubscriptionSessionEvent::ConnectionDown,
                true,
                LifecycleLogLevel::Info,
                "shutdown_in_progress",
            ),
            (
                SubscriptionSessionEvent::Terminated,
                false,
                LifecycleLogLevel::Error,
                "termination_without_shutdown",
            ),
            (
                SubscriptionSessionEvent::Terminated,
                true,
                LifecycleLogLevel::Info,
                "shutdown_in_progress",
            ),
        ];

        for (event, shutdown_requested, expected_level, expected_classification) in cases {
            let context = event.log_context(shutdown_requested);
            assert_eq!(context.event, event);
            assert_eq!(context.level, expected_level);
            assert_eq!(context.shutdown_requested, shutdown_requested);
            assert_eq!(context.classification(), expected_classification);
        }
    }

    #[tokio::test]
    async fn shutdown_closes_bounded_admission_and_wakes_waiters() {
        let config = Arc::new(EngineConfig {
            subscription_pool_size: 0,
            max_subscription_sessions: 1,
            ..EngineConfig::default()
        });
        let pool = Arc::new(SubscriptionSessionPool::new(0, config).expect("empty pool"));
        let held = Arc::clone(&pool.admission)
            .acquire_owned()
            .await
            .expect("first admission permit");
        let waiter = {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move { pool.claim().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        pool.signal_shutdown();
        drop(held);
        let error = match waiter.await.expect("waiter task") {
            Ok(_) => panic!("post-shutdown claim must fail"),
            Err(error) => error,
        };

        assert!(matches!(error, BlpAsyncError::ConfigError { .. }));
        assert_eq!(pool.available_count(), 0);
        assert_eq!(pool.next_id.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn shutdown_drain_removes_dirty_subscription_state_and_closes_sender() {
        let config = Arc::new(EngineConfig::default());
        let mut worker = SubscriptionWorkerState::new(7, config);
        let (tx, mut rx) = subscription_channel(1);
        worker.subs.insert(SubscriptionState::new(
            "TEST US Equity".to_string(),
            vec!["PX_LAST".to_string()],
            tx,
            1,
            false,
        ));
        assert!(!worker.is_clean());

        worker.drain_for_shutdown("test shutdown");

        assert!(worker.is_clean());
        assert!(matches!(rx.try_recv(), Ok(Err(BlpError::Internal { .. }))));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn remove_all_then_shutdown_fails_the_retained_channel() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (tx, mut rx) = subscription_channel(1);
        let (keys, _, _) = register_topics(&mut worker, &tx, &["TEST US Equity"]);
        remove_registered_topics(&mut worker, &keys);
        assert!(worker.subs.is_empty());

        worker.drain_for_shutdown("shutdown");

        assert!(matches!(rx.try_recv(), Ok(Err(BlpError::Internal { .. }))));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn worker_reuse_replaces_the_previous_channel_terminator() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (old_tx, mut old_rx) = subscription_channel(1);
        let (old_keys, _, _) = register_topics(&mut worker, &old_tx, &["OLD US Equity"]);
        remove_registered_topics(&mut worker, &old_keys);
        assert!(worker.prepare_for_reuse());

        let (new_tx, mut new_rx) = subscription_channel(1);
        let (new_keys, _, _) = register_topics(&mut worker, &new_tx, &["NEW US Equity"]);
        remove_registered_topics(&mut worker, &new_keys);
        worker.drain_for_shutdown("shutdown");

        assert!(!old_tx.is_closed());
        assert!(matches!(
            old_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            new_rx.try_recv(),
            Ok(Err(BlpError::Internal { .. }))
        ));
        drop(old_tx);
        assert!(matches!(
            old_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn terminal_session_events_fail_an_empty_registered_channel() {
        for (event_type, message_type) in [
            (EventType::SessionStatus, "SessionTerminated"),
            (EventType::AuthorizationStatus, "AuthorizationRevoked"),
        ] {
            let shared = SubscriptionWorkerShared::new(
                7,
                Arc::new(EngineConfig::default()),
                Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8)),
            );
            let (tx, mut rx) = subscription_channel(1);
            let event = if message_type == "AuthorizationRevoked" {
                // TestUtil's built-in definitions omit authorization messages.
                let schema = r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
                    <service name="//xbbg/test" version="1.0.0.0">
                        <event name="AuthorizationRevoked" eventType="Revoked"/>
                    </service>
                    <schema><sequenceType name="Revoked"/></schema>
                </ServiceDefinition>"#;
                TestEvent::with_schema(schema, event_type, message_type, &[], |_| {})
            } else {
                TestEvent::admin(event_type, message_type, &[], |_| {})
            };
            {
                let mut worker = shared.state.lock();
                let (keys, _, _) = register_topics(&mut worker, &tx, &["TEST US Equity"]);
                remove_registered_topics(&mut worker, &keys);
                let message = event.event().messages().next().expect("terminal message");
                worker.handle_session_status(&message, &shared);
            }

            assert!(shared.shutdown_requested());
            assert_eq!(
                shared.health.load(Ordering::Acquire),
                WorkerHealth::Dead as u8
            );
            assert!(matches!(rx.try_recv(), Ok(Err(BlpError::Internal { .. }))));
        }
    }

    #[test]
    fn zero_cid_admin_data_loss_fails_an_empty_registered_channel() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (tx, mut rx) = subscription_channel(1);
        let (keys, _, _) = register_topics(&mut worker, &tx, &["TEST US Equity"]);
        remove_registered_topics(&mut worker, &keys);
        let event = TestEvent::admin(EventType::Admin, "DataLoss", &[], |formatter| {
            formatter.json("{}");
        });
        let message = event.event().messages().next().expect("data loss message");

        worker.handle_admin_event(&message);

        assert!(matches!(
            rx.try_recv(),
            Ok(Err(BlpError::SubscriptionDataLoss { topic, .. })) if topic == "*"
        ));
    }

    #[test]
    fn unresolved_admin_data_loss_fails_all_topics_and_preserves_cleanup() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (tx, mut rx) = subscription_channel(1);
        let (keys, metrics, topics) =
            register_topics(&mut worker, &tx, &["FIRST US Equity", "SECOND US Equity"]);
        let status = attach_active_status(&mut worker, &topics, &keys, &metrics);
        let unknown_cid = correlation_id_for_key(keys.iter().copied().max().unwrap() + 1);
        let event = TestEvent::admin(EventType::Admin, "DataLoss", &[unknown_cid], |formatter| {
            formatter.json("{}")
        });
        let message = event.event().messages().next().expect("data loss message");

        worker.handle_admin_event(&message);

        assert!(matches!(
            rx.try_recv(),
            Ok(Err(BlpError::SubscriptionDataLoss { topic, .. })) if topic == "*"
        ));
        assert_eq!(worker.subs.len(), keys.len());
        for metric in &metrics {
            assert_eq!(metric.data_loss_events.load(Ordering::Relaxed), 1);
            assert!(metric.slow_consumer.load(Ordering::Relaxed));
        }
        let snapshot = status.load();
        assert_eq!(snapshot.admin().data_loss_count, 1);
        assert!(topics
            .iter()
            .all(|topic| !snapshot.topic_statuses()[topic].streams_active));
    }

    #[test]
    fn correlated_admin_data_loss_deactivates_every_topic_on_the_channel() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (tx, mut rx) = subscription_channel(1);
        let (keys, metrics, topics) =
            register_topics(&mut worker, &tx, &["FIRST US Equity", "SECOND US Equity"]);
        let status = attach_active_status(&mut worker, &topics, &keys, &metrics);
        let cid = correlation_id_for_key(keys[0]);
        let event = TestEvent::admin(EventType::Admin, "DataLoss", &[cid], |formatter| {
            formatter.json("{}");
        });
        let message = event.event().messages().next().expect("data loss message");

        worker.handle_admin_event(&message);

        assert!(matches!(
            rx.try_recv(),
            Ok(Err(BlpError::SubscriptionDataLoss { topic, .. }))
                if topic == "FIRST US Equity"
        ));
        assert_eq!(metrics[0].data_loss_events.load(Ordering::Relaxed), 1);
        assert_eq!(metrics[1].data_loss_events.load(Ordering::Relaxed), 0);
        let snapshot = status.load();
        assert!(topics
            .iter()
            .all(|topic| !snapshot.topic_statuses()[topic].streams_active));
        assert_eq!(worker.subs.len(), keys.len());
    }

    #[test]
    fn partial_subscription_terminal_status_preserves_healthy_siblings() {
        for (message_type, expected_state) in [
            (
                "SubscriptionFailure",
                crate::engine::TopicLifecycleState::Failed,
            ),
            (
                "SubscriptionTerminated",
                crate::engine::TopicLifecycleState::Terminated,
            ),
        ] {
            let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
            let (tx, mut rx) = subscription_channel(1);
            let (keys, metrics, topics) =
                register_topics(&mut worker, &tx, &["FIRST US Equity", "SECOND US Equity"]);
            let status = attach_active_status(&mut worker, &topics, &keys, &metrics);

            let first_cid = correlation_id_for_key(keys[0]);
            let first_event = TestEvent::admin(
                EventType::SubscriptionStatus,
                message_type,
                &[first_cid],
                |formatter| formatter.json("{}"),
            );
            let first_message = first_event
                .event()
                .messages()
                .next()
                .expect("first status message");
            let mut mutations = Vec::new();
            worker.collect_subscription_status(&first_message, &mut mutations);
            status.update(|next| {
                for mutation in mutations {
                    mutation.apply(next);
                }
            });

            assert!(!tx.is_closed());
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            assert!(!worker.subs.contains(keys[0]));
            assert!(worker.subs.contains(keys[1]));
            {
                let snapshot = status.load();
                assert_eq!(snapshot.keys(), &[keys[1]]);
                assert_eq!(snapshot.topic_statuses()[&topics[0]].state, expected_state);
                assert!(!snapshot.topic_statuses()[&topics[0]].streams_active);
                assert!(snapshot.topic_statuses()[&topics[1]].streams_active);
            }

            let last_cid = correlation_id_for_key(keys[1]);
            let last_event = TestEvent::admin(
                EventType::SubscriptionStatus,
                message_type,
                &[last_cid],
                |formatter| formatter.json("{}"),
            );
            let last_message = last_event
                .event()
                .messages()
                .next()
                .expect("last status message");
            let mut mutations = Vec::new();
            worker.collect_subscription_status(&last_message, &mut mutations);
            status.update(|next| {
                for mutation in mutations {
                    mutation.apply(next);
                }
            });

            assert!(matches!(
                rx.try_recv(),
                Ok(Err(BlpError::SubscriptionFailure { .. }))
            ));
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ));
            assert!(worker.subs.is_empty());
            let snapshot = status.load();
            assert!(snapshot.keys().is_empty());
            assert_eq!(snapshot.topic_statuses()[&topics[1]].state, expected_state);
            assert!(!snapshot.topic_statuses()[&topics[1]].streams_active);
        }
    }

    #[test]
    fn late_positive_status_events_do_not_reactivate_a_terminal_channel() {
        let mut worker = SubscriptionWorkerState::new(7, Arc::new(EngineConfig::default()));
        let (tx, _rx) = subscription_channel(1);
        let (keys, metrics, topics) = register_topics(&mut worker, &tx, &["TEST US Equity"]);
        let status = Arc::new(crate::engine::SubscriptionStatusHandle::new(
            crate::engine::SubscriptionStatusState::from_active(
                topics.clone(),
                keys.clone(),
                keys.iter().copied().zip(metrics.iter().cloned()).collect(),
            ),
        ));
        worker.status = Some(status.clone());
        tx.fail(BlpError::Timeout);
        let cid = correlation_id_for_key(keys[0]);

        for message_type in ["SubscriptionStarted", "SubscriptionStreamsActivated"] {
            let event =
                TestEvent::admin(EventType::SubscriptionStatus, message_type, &[cid], |_| {});
            let message = event.event().messages().next().expect("status message");
            let mut mutations = Vec::new();
            worker.collect_subscription_status(&message, &mut mutations);
            status.update(|next| {
                for mutation in mutations {
                    mutation.apply(next);
                }
            });
        }

        let snapshot = status.load();
        let topic = &snapshot.topic_statuses()["TEST US Equity"];
        assert_eq!(topic.state, crate::engine::TopicLifecycleState::Pending);
        assert!(!topic.streams_active);
    }

    #[test]
    fn terminal_stream_rejects_add_without_removing_cleanup_keys() {
        let config = Arc::new(EngineConfig::default());
        let mut worker = SubscriptionWorkerState::new(7, config);
        let (tx, _rx) = subscription_channel(1);
        let existing_key = worker.subs.insert(SubscriptionState::new(
            "EXISTING US Equity".to_string(),
            vec!["PX_LAST".to_string()],
            tx.clone(),
            1,
            false,
        ));
        let status = Arc::new(crate::engine::SubscriptionStatusHandle::new(
            crate::engine::SubscriptionStatusState::from_active(
                vec!["EXISTING US Equity".to_string()],
                vec![existing_key],
                HashMap::new(),
            ),
        ));
        status.update(|next| {
            assert_eq!(
                next.set_topic_streams_active("EXISTING US Equity", true),
                Some(false)
            );
        });
        worker.status = Some(status.clone());
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "EXISTING US Equity".to_string(),
            detail: "test gap".to_string(),
        });
        worker.check_streams_deactivated();

        let error = match worker.register_subscriptions(SubscriptionRegistrationRequest {
            topics: vec!["NEW US Equity".to_string()],
            fields: vec!["PX_LAST".to_string()],
            all_fields: false,
            options: Vec::new(),
            flush_threshold: None,
            overflow_policy: None,
            stream: tx,
            forwarder: None,
        }) {
            Ok(_) => panic!("terminal stream must reject add"),
            Err(error) => error,
        };

        assert!(matches!(error, BlpError::SubscriptionFailure { .. }));
        assert!(worker.subs.contains(existing_key));
        assert_eq!(status.load().keys(), &[existing_key]);
        assert!(!status.load().topic_statuses()["EXISTING US Equity"].streams_active);
        assert!(!worker.is_clean());
    }

    #[tokio::test]
    async fn invalidated_claim_lease_blocks_commands_and_retains_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit");
        let lease = Arc::new(ClaimLease::new(permit));
        let command_lease =
            CommandLeaseRef::try_new(Arc::clone(&lease)).expect("valid command lease");
        assert_eq!(lease.command_count(), 1);
        assert_eq!(admission.available_permits(), 0);

        lease.invalidate();
        assert!(command_lease.validate().is_err());
        assert!(CommandLeaseRef::try_new(Arc::clone(&lease)).is_err());
        drop(lease);
        assert_eq!(admission.available_permits(), 0);

        drop(command_lease);
        assert_eq!(admission.available_permits(), 1);
    }

    #[test]
    fn service_waiters_prune_closed_receivers_and_preserve_new_attempts() {
        let shared = SubscriptionWorkerShared::new(
            0,
            Arc::new(EngineConfig::default()),
            Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8)),
        );
        let service = "//blp/mktdata";

        let (must_open, first_cid, first_rx) = shared.register_service_waiter(service);
        assert!(must_open);
        drop(first_rx);
        let (must_open, joined_cid, _joined_rx) = shared.register_service_waiter(service);
        assert!(!must_open);
        assert_eq!(joined_cid, first_cid);
        assert_eq!(
            shared
                .state
                .lock()
                .pending_service_opens
                .get(service)
                .expect("pending open")
                .waiters
                .len(),
            1
        );

        shared
            .remove_pending_service_open(service, first_cid)
            .expect("first generation");
        let (_, second_cid, _second_rx) = shared.register_service_waiter(service);
        assert_ne!(second_cid, first_cid);
        assert!(shared
            .remove_pending_service_open(service, first_cid)
            .is_none());
        assert_eq!(
            shared
                .state
                .lock()
                .pending_service_opens
                .get(service)
                .expect("new generation remains")
                .cid,
            second_cid
        );
    }

    #[test]
    fn cancelled_subscription_service_waiter_is_removed_immediately() {
        let shared = SubscriptionWorkerShared::new(
            0,
            Arc::new(EngineConfig::default()),
            Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8)),
        );
        let service = "//blp/mktdata";
        let (_, cid, rx) = shared.register_service_waiter(service);

        drop(PendingSubscriptionServiceWaiter::new(
            &shared, service, cid, rx,
        ));

        assert!(!shared
            .state
            .lock()
            .pending_service_opens
            .contains_key(service));
    }

    #[test]
    fn blocking_shutdown_waits_for_claim_creation_barrier() {
        let config = Arc::new(EngineConfig {
            subscription_pool_size: 0,
            max_subscription_sessions: 1,
            ..EngineConfig::default()
        });
        let pool = Arc::new(SubscriptionSessionPool::new(0, config).expect("empty pool"));
        let creation = pool.begin_creation().expect("creation guard");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
        let shutdown_pool = Arc::clone(&pool);
        let thread = std::thread::spawn(move || {
            started_tx.send(()).expect("announce shutdown");
            shutdown_pool.shutdown_blocking();
            done_tx.send(()).expect("announce completion");
        });
        started_rx.recv().expect("shutdown started");
        while !pool.shutdown.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        assert!(done_rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(creation);
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown completes after creation");
        thread.join().expect("shutdown thread");
    }

    #[tokio::test]
    async fn stopped_worker_cannot_recreate_forwarder() {
        let shared = SubscriptionWorkerShared::new(
            0,
            Arc::new(EngineConfig::default()),
            Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8)),
        );
        shared.attach_runtime(tokio::runtime::Handle::current());
        let _sender = shared.ensure_forwarder().expect("attached runtime");
        assert!(shared.mark_shutdown_requested());
        assert!(shared.ensure_forwarder().is_err());
        shared.stop_forwarder();
    }
}
