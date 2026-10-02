//! Engine-scoped market-data feeds and consumer-local delivery.
//!
//! The SDK worker still owns each decoder and correlation ID. Its synchronous
//! sink applies the image once, then projects Arc-backed values to consumers.
//! Registry mutations never hold a feed lock across an SDK command.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{Field, Schema};
use parking_lot::Mutex;
use smallvec::SmallVec;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use xbbg_core::{BlpError, Value};

use super::state::typed_builder::{ArrowType, TypedBuilder};
use super::state::{
    subscription_channel, subscription_forwarder_channel, FieldKind, FieldLayout, FieldMeta,
    MessageOutcome, SubscriptionForwarder, SubscriptionSender, SubscriptionState,
    SubscriptionUpdate, UpdateField, UpdateValue,
};
use super::subscription_pool::FeedRegistration;
use super::{
    timestamp_now_us, BlpAsyncError, EngineConfig, OverflowPolicy, SessionClaim,
    SessionLifecycleState, SharedSubscriptionStatus, SlabKey, SubscriptionCommandHandle,
    SubscriptionEventInfo, SubscriptionEventLevel, SubscriptionFailureKind,
    SubscriptionSessionPool, SubscriptionStatusHandle, SubscriptionStatusScope,
    SubscriptionStatusState, SubscriptionStream, TopicLifecycleState,
};

const MKTDATA: &str = "//blp/mktdata";
#[cfg(test)]
use super::subscription_pool::TestSubscriptionSession as TestSession;
#[cfg(test)]
#[path = "shared_subscriptions_tests.rs"]
mod tests;
#[cfg(test)]
use tests::TestSessionFactory;
const EVENT_TYPE: &str = "MKTDATA_EVENT_TYPE";
const EVENT_SUBTYPE: &str = "MKTDATA_EVENT_SUBTYPE";
const SESSION_WAIT_TIMED_OUT: &str = "session_wait: subscription session capacity wait timed out";

/// Consumer-local policy for a delayed Bloomberg stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DelayedPolicy {
    #[default]
    Warn,
    Raise,
    Ignore,
}

impl FromStr for DelayedPolicy {
    type Err = BlpAsyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(Self::Warn),
            "raise" => Ok(Self::Raise),
            "ignore" => Ok(Self::Ignore),
            _ => Err(config_error("on_delayed must be warn, raise, or ignore")),
        }
    }
}

/// Consumer-local treatment of fields rejected by Bloomberg.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FieldErrorPolicy {
    #[default]
    Warn,
    Raise,
    Ignore,
}

impl FromStr for FieldErrorPolicy {
    type Err = BlpAsyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(Self::Warn),
            "raise" => Ok(Self::Raise),
            "ignore" => Ok(Self::Ignore),
            _ => Err(config_error(
                "on_field_error must be warn, raise, or ignore",
            )),
        }
    }
}

/// All subscription configuration. Options affect feed identity; aliases do not.
#[derive(Clone, Debug)]
pub struct SubscribeRequest {
    pub service: String,
    pub topics: Vec<String>,
    pub fields: Vec<String>,
    pub all_fields: bool,
    pub options: Vec<String>,
    pub aliases: Vec<(String, String)>,
    pub delayed_policy: DelayedPolicy,
    pub field_error_policy: FieldErrorPolicy,
    pub deliver_rows: bool,
    pub zero_as_null: Vec<String>,
    /// Maximum wait for pool capacity. None waits indefinitely; excludes startup.
    /// Expiry returns ConfigError with a `session_wait:` detail before mutation.
    pub session_wait: Option<Duration>,
    pub isolated: bool,
    pub stream_capacity: Option<usize>,
    pub flush_threshold: Option<usize>,
    pub overflow_policy: Option<OverflowPolicy>,
}

impl Default for SubscribeRequest {
    fn default() -> Self {
        Self {
            service: MKTDATA.into(),
            topics: Vec::new(),
            fields: Vec::new(),
            all_fields: false,
            options: Vec::new(),
            aliases: Vec::new(),
            delayed_policy: DelayedPolicy::Warn,
            field_error_policy: FieldErrorPolicy::Warn,
            deliver_rows: true,
            zero_as_null: Vec::new(),
            session_wait: None,
            isolated: false,
            stream_capacity: None,
            flush_threshold: None,
            overflow_policy: None,
        }
    }
}

/// Operational feed diagnostics, deliberately excluding session credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedInfo {
    pub service: String,
    pub topic: String,
    pub options: Vec<String>,
    pub fields: Vec<String>,
    pub consumers: usize,
    pub delayed: Option<bool>,
    pub state: String,
    pub isolated: bool,
    pub field_errors: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FeedKey {
    service: String,
    topic: String,
    options: Vec<String>,
    /// Zero is shared; every isolated membership gets a unique nonce.
    isolation: usize,
}

struct ConsumerTopic {
    owner: Weak<Consumer>,
    key: SlabKey,
    session_id: usize,
    active: bool,
    projection: SubscriptionState,
    explicit_fields: HashSet<String>,
    awaiting_image: bool,
    awaiting_repaint: bool,
    delayed_warned: bool,
    warned_fields: HashSet<String>,
    pending_data_loss: usize,
    last_data_loss_us: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecoveryStage {
    QueuedInitial,
    WaitingInitialPaint,
    QueuedRestart,
    WaitingRestartStart,
    WaitingRestartPaint,
}

struct FeedRecovery {
    stage: RecoveryStage,
    restart_requested: bool,
}

struct FeedState {
    fields: Vec<String>,
    capture_all_fields: bool,
    consumers: Vec<ConsumerTopic>,
    layout: Option<Arc<FieldLayout>>,
    image: Vec<Option<UpdateValue>>,
    last_update: Option<i64>,
    live: bool,
    delayed: Option<bool>,
    repainting: bool,
    repaint_started: bool,
    recovery: Option<FeedRecovery>,
    recovery_task_scheduled: bool,
    kinds: HashMap<String, FieldKind>,
    lifecycle: &'static str,
    field_errors: HashMap<String, String>,
}

struct Feed {
    identity: FeedKey,
    status_topic: String,
    session: Arc<FeedSession>,
    upstream_key: AtomicUsize,
    state: Mutex<FeedState>,
    hub: Weak<SharedSubscriptions>,
}

impl Feed {
    fn new(
        identity: FeedKey,
        fields: Vec<String>,
        kinds: &HashMap<String, FieldKind>,
        session: Arc<FeedSession>,
        hub: Weak<SharedSubscriptions>,
        id: usize,
    ) -> Arc<Self> {
        let mut state = FeedState {
            fields,
            consumers: Vec::new(),
            layout: None,
            image: Vec::new(),
            capture_all_fields: false,
            last_update: None,
            live: false,
            delayed: None,
            repainting: false,
            repaint_started: false,
            recovery: None,
            recovery_task_scheduled: false,
            kinds: HashMap::new(),
            lifecycle: "pending",
            field_errors: HashMap::new(),
        };
        seed_image_kinds(&mut state, kinds);
        Arc::new(Self {
            identity,
            session,
            upstream_key: AtomicUsize::new(usize::MAX),
            hub,
            status_topic: format!("feed-{id}"),
            state: Mutex::new(state),
        })
    }

    fn sink(self: &Arc<Self>) -> SubscriptionSender {
        let feed = Arc::downgrade(self);
        let sink = SubscriptionSender::callback(Arc::new(move |item| {
            if let Some(feed) = feed.upgrade() {
                match item {
                    Ok(update) => feed.on_update(&update),
                    Err(error) => feed.on_error(error),
                }
            }
        }));
        let feed = Arc::downgrade(self);
        sink.on_field_error(Arc::new(move |error, scalar| {
            if let Some(feed) = feed.upgrade() {
                feed.on_field_error(error, scalar);
            }
        }));
        sink.keep_open_on_data_loss();
        sink
    }

    fn on_field_error(&self, error: &BlpError, scalar: bool) {
        let BlpError::SchemaUnsupported { element, detail } = error else {
            return;
        };
        let mut state = self.state.lock();
        for consumer in &mut state.consumers {
            let Some(owner) = consumer.owner.upgrade() else {
                continue;
            };
            if owner.stream.is_closed() {
                consumer.active = false;
                continue;
            }
            if consumer.active
                && (consumer.explicit_fields.contains(element)
                    || (scalar && owner.request.all_fields))
            {
                consumer.active = false;
                consumer.projection.fail(BlpError::SchemaUnsupported {
                    element: element.clone(),
                    detail: detail.clone(),
                });
                owner.status.update(|status| status.clear_active());
            }
        }
    }

    fn on_update(&self, update: &SubscriptionUpdate) {
        let mut state = self.state.lock();
        if matches!(state.lifecycle, "failed" | "closed") {
            return;
        }
        let initpaint = text_field(update, EVENT_SUBTYPE) == Some("INITPAINT");
        let delayed = update.values.iter().find_map(|value| {
            (update.layout.fields[value.index as usize].name.as_ref() == "IS_DELAYED_STREAM")
                .then_some(&value.value)
                .and_then(|value| match value {
                    UpdateValue::Bool(value) => Some(*value),
                    _ => None,
                })
        });
        if let Some(delayed) = delayed {
            state.delayed = Some(delayed);
        }
        let mut recovered = false;
        let mut frozen = false;
        let mut restart_ready = false;
        if let Some(recovery) = &mut state.recovery {
            frozen = true;
            match recovery.stage {
                RecoveryStage::WaitingInitialPaint if initpaint => {
                    if recovery.restart_requested {
                        recovery.stage = RecoveryStage::QueuedRestart;
                        restart_ready = true;
                    } else {
                        recovered = true;
                        frozen = false;
                    }
                }
                RecoveryStage::WaitingRestartPaint if initpaint => {
                    recovered = true;
                    frozen = false;
                }
                _ => {}
            }
        }
        let schedule_restart = restart_ready && !state.recovery_task_scheduled;
        state.recovery_task_scheduled |= schedule_restart;
        if recovered {
            state.recovery = None;
        }
        if recovered {
            state.image.fill(None);
        }
        let repainting = state.repainting;
        let changes =
            (repainting && initpaint && !frozen).then(|| repaint_changes(update, &state.image));
        if repainting && initpaint && !frozen {
            state.repaint_started = true;
        }
        let finish_paint = !initpaint && !frozen && (!repainting || state.repaint_started);
        if !frozen {
            state.layout = Some(update.layout.clone());
            state.image.resize(update.layout.fields.len(), None);
            for value in &update.values {
                state.image[value.index as usize] = Some(value.value.clone());
            }
            state.last_update = Some(update.timestamp_us);
            state.live |= !initpaint && state.recovery.is_none();
            state.lifecycle = "active";
            if finish_paint {
                state.repaint_started = false;
                state.repainting = false;
            }
        }
        let delayed = state.delayed;
        let suppress_empty = self.identity.service == MKTDATA;
        let mut cleanup = false;
        let mut notices: HashMap<usize, (Arc<Consumer>, Vec<String>)> = HashMap::new();
        for consumer in &mut state.consumers {
            let Some(owner) = consumer.owner.upgrade() else {
                cleanup = true;
                continue;
            };
            if !consumer.active
                || owner.closed.load(Ordering::Acquire)
                || consumer.projection.stream.is_closed()
            {
                consumer.active = false;
                cleanup = true;
                continue;
            }
            if !apply_delayed(&owner, consumer, delayed) {
                cleanup = true;
                continue;
            }
            if frozen {
                continue;
            }
            if finish_paint {
                consumer.awaiting_image = false;
                consumer.awaiting_repaint = false;
            }
            let outcome = if !owner.request.deliver_rows {
                consumer.projection.observe_update(update)
            } else if repainting && initpaint && !consumer.awaiting_image {
                let (changed, has_data) = changes.as_ref().expect("repaint changes were prepared");
                if !has_data {
                    continue;
                }
                consumer.projection.project_update(changed, true)
            } else {
                consumer.projection.project_update(update, suppress_empty)
            };
            if matches!(outcome, MessageOutcome::Closed | MessageOutcome::DataLoss) {
                consumer.active = false;
                owner.status.update(|status| status.clear_active());
                cleanup = true;
            } else if recovered && !owner.request.deliver_rows {
                consumer.projection.clear_slow_consumer();
                notices
                    .entry(owner.id)
                    .or_insert_with(|| (owner, Vec::new()))
                    .1
                    .push(consumer.projection.topic.to_string());
            }
        }
        drop(state);
        if schedule_restart {
            if let Some(hub) = self.hub.upgrade() {
                hub.schedule_recovery(&self.identity);
            }
        }
        for (owner, labels) in notices.into_values() {
            owner.status.update(|status| {
                for label in labels {
                    status.record_subscription_event(
                        "FeedRecovered",
                        Some(label),
                        Some("a fresh Bloomberg image restored the feed".into()),
                        SubscriptionEventLevel::Info,
                    );
                }
            });
        }
        if cleanup {
            self.schedule_cleanup();
        }
    }

    fn on_error(&self, error: BlpError) {
        if matches!(error, BlpError::SubscriptionDataLoss { .. }) {
            self.on_data_loss();
            return;
        }
        let mut state = self.state.lock();
        if matches!(state.lifecycle, "failed" | "closed") {
            return;
        }
        state.lifecycle = "failed";
        let detail = match &error {
            BlpError::Internal { detail }
            | BlpError::SubscriptionFailure {
                label: Some(detail),
                ..
            } => detail.clone(),
            _ => error.to_string(),
        };
        let upstream = self.session.status.load();
        let session_ended = upstream.session().state == SessionLifecycleState::Terminated;
        let session_event = upstream
            .events()
            .iter()
            .rev()
            .find(|event| {
                matches!(
                    event.message_type.as_str(),
                    "SessionTerminated" | "AuthorizationRevoked"
                )
            })
            .map(|event| event.message_type.as_str())
            .unwrap_or("SessionTerminated");
        for consumer in &mut state.consumers {
            let Some(owner) = consumer.owner.upgrade() else {
                continue;
            };
            if owner.stream.is_closed() {
                consumer.active = false;
                continue;
            }
            match &error {
                BlpError::SubscriptionFailure { .. } => fail_topic(
                    &owner,
                    consumer,
                    &detail,
                    SubscriptionFailureKind::Failure,
                    "SubscriptionFailure",
                ),
                _ => {
                    let kind = if session_ended {
                        SubscriptionFailureKind::Terminated
                    } else {
                        SubscriptionFailureKind::Failure
                    };
                    let reason = if session_ended {
                        format!("{session_event}: {detail}")
                    } else {
                        detail.clone()
                    };
                    if record_topic_failure(
                        &owner,
                        consumer,
                        &reason,
                        kind,
                        if session_ended {
                            "SubscriptionTerminated"
                        } else {
                            "SubscriptionFailure"
                        },
                    ) {
                        consumer.projection.fail(BlpError::Internal {
                            detail: detail.clone(),
                        });
                    }
                }
            }
        }
        drop(state);
        self.schedule_cleanup();
    }

    fn on_data_loss(&self) {
        let mut state = self.state.lock();
        if matches!(state.lifecycle, "failed" | "closed") {
            return;
        }
        let at_us = timestamp_now_us();
        let mut image_consumer = false;
        for consumer in &mut state.consumers {
            let Some(owner) = consumer.owner.upgrade() else {
                continue;
            };
            if !consumer.active || owner.closed.load(Ordering::Acquire) || owner.stream.is_closed()
            {
                consumer.active = false;
                continue;
            }
            if owner.request.deliver_rows {
                consumer.active = false;
                consumer.projection.on_dataloss(Some(at_us));
                owner.status.update(|status| {
                    status.record_admin_data_loss(
                        Some(consumer.projection.topic.to_string()),
                        Some("subscription data continuity was lost".into()),
                    );
                    status.clear_active();
                });
            } else {
                image_consumer = true;
                consumer.projection.record_dataloss(Some(at_us));
                consumer.pending_data_loss += 1;
                consumer.last_data_loss_us = at_us;
            }
        }
        state.live = false;
        let schedule = if image_consumer {
            state.lifecycle = "pending";
            state.repainting = true;
            state.repaint_started = false;
            if let Some(recovery) = &mut state.recovery {
                if matches!(
                    recovery.stage,
                    RecoveryStage::QueuedInitial | RecoveryStage::WaitingInitialPaint
                ) {
                    recovery.restart_requested = true;
                }
            } else {
                state.recovery = Some(FeedRecovery {
                    stage: RecoveryStage::QueuedInitial,
                    restart_requested: false,
                });
            }
            let pending = state.recovery.as_ref().is_some_and(|recovery| {
                matches!(
                    recovery.stage,
                    RecoveryStage::QueuedInitial | RecoveryStage::QueuedRestart
                )
            });
            let schedule = pending && !state.recovery_task_scheduled;
            state.recovery_task_scheduled |= schedule;
            schedule
        } else {
            state.lifecycle = "failed";
            false
        };
        drop(state);
        if let Some(hub) = self.hub.upgrade() {
            if schedule {
                hub.schedule_recovery(&self.identity);
            }
            hub.schedule_cleanup();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_status(
        &self,
        upstream: &SubscriptionStatusState,
        events: &[(usize, &SubscriptionEventInfo)],
        failure_info: Option<(usize, &super::SubscriptionFailureInfo)>,
        topic_changed: bool,
        session_end: Option<&SessionEnd>,
        target_consumer: Option<usize>,
        patches: &mut HashMap<usize, ConsumerStatusPatch>,
    ) {
        let mut state = self.state.lock();
        if events
            .iter()
            .any(|(_, event)| event.message_type == "SubscriptionStarted")
        {
            if state.repainting {
                state.repaint_started = false;
            }
            if let Some(recovery) = &mut state.recovery {
                if recovery.stage == RecoveryStage::WaitingRestartStart {
                    recovery.stage = RecoveryStage::WaitingRestartPaint;
                }
            }
        }
        let topic = upstream.topic_statuses().get(&self.status_topic);
        let next_state = topic.map(|topic| topic.state);
        let terminal = matches!(
            next_state,
            Some(TopicLifecycleState::Failed | TopicLifecycleState::Terminated)
        );
        if topic_changed {
            if let Some(errors) = upstream.field_errors().get(&self.status_topic) {
                if &state.field_errors != errors {
                    state.field_errors.clone_from(errors);
                }
            }
        }
        if terminal || session_end.is_some() {
            state.lifecycle = "failed";
        }
        let FeedState {
            consumers,
            field_errors,
            ..
        } = &mut *state;
        for consumer in consumers {
            if !consumer.active {
                continue;
            }
            let Some(owner) = consumer.owner.upgrade() else {
                continue;
            };
            if owner.stream.is_closed() {
                consumer.active = false;
                continue;
            }
            if target_consumer.is_some_and(|id| id != owner.id) {
                continue;
            }
            let patch = patches
                .entry(owner.id)
                .or_insert_with(|| ConsumerStatusPatch {
                    owner,
                    topics: Vec::new(),
                    events: Vec::new(),
                });
            for _ in 0..std::mem::take(&mut consumer.pending_data_loss) {
                patch.events.push((
                    (0, consumer.key),
                    SubscriptionEventInfo {
                        at_us: consumer.last_data_loss_us,
                        category: super::SubscriptionEventCategory::Subscription,
                        level: SubscriptionEventLevel::Warning,
                        message_type: "DataLoss".into(),
                        topic: Some(consumer.projection.topic.to_string()),
                        detail: Some(
                            "Bloomberg reported DATALOSS; requesting a fresh image".into(),
                        ),
                    },
                ));
            }
            if !topic_changed && session_end.is_none() {
                continue;
            }
            let mut lifecycle = next_state;
            if matches!(
                lifecycle,
                Some(TopicLifecycleState::Pending | TopicLifecycleState::Started)
            ) && consumer
                .projection
                .metrics
                .messages_received
                .load(Ordering::Relaxed)
                != 0
            {
                lifecycle = Some(TopicLifecycleState::Streaming);
            }
            let mut failure = if let Some(end) = session_end {
                Some((
                    format!("{}: {}", end.message_type, end.detail),
                    SubscriptionFailureKind::Terminated,
                ))
            } else if terminal {
                let kind = if next_state == Some(TopicLifecycleState::Terminated) {
                    SubscriptionFailureKind::Terminated
                } else {
                    SubscriptionFailureKind::Failure
                };
                Some((
                    failure_info
                        .map(|(_, failure)| failure.reason.clone())
                        .unwrap_or_else(|| "subscription ended".into()),
                    kind,
                ))
            } else {
                None
            };
            if failure.is_some() {
                consumer.active = false;
            }
            let snapshot = patch.owner.status.load();
            let previous_errors = snapshot
                .field_errors()
                .get(consumer.projection.topic.as_ref());
            let errors = field_errors
                .iter()
                .filter_map(|(field, category)| {
                    if !consumer.explicit_fields.contains(field) {
                        return None;
                    }
                    let first = !consumer.warned_fields.contains(field);
                    if first {
                        consumer.warned_fields.insert(field.clone());
                    }
                    (first
                        || previous_errors.and_then(|errors| errors.get(field)) != Some(category))
                    .then(|| {
                        (
                            field.clone(),
                            category.clone(),
                            first
                                && patch.owner.request.field_error_policy == FieldErrorPolicy::Warn,
                        )
                    })
                })
                .collect();
            drop(snapshot);
            if failure.is_none()
                && patch.owner.request.field_error_policy == FieldErrorPolicy::Raise
            {
                if let Some((field, category)) = field_errors
                    .iter()
                    .filter(|(field, _)| consumer.explicit_fields.contains(*field))
                    .min_by(|(left, _), (right, _)| left.cmp(right))
                {
                    let reason = format!("{field}: {category}");
                    failure = Some((reason.clone(), SubscriptionFailureKind::Failure));
                    consumer.active = false;
                    patch.events.push((
                        (usize::MAX, consumer.key),
                        SubscriptionEventInfo {
                            at_us: timestamp_now_us(),
                            category: super::SubscriptionEventCategory::Subscription,
                            level: SubscriptionEventLevel::Warning,
                            message_type: "SubscriptionFailure".into(),
                            topic: Some(consumer.projection.topic.to_string()),
                            detail: Some(reason),
                        },
                    ));
                }
            }
            let order = if session_end.is_some() {
                usize::MAX
            } else {
                events
                    .last()
                    .map(|(index, _)| *index)
                    .or_else(|| failure_info.map(|(index, _)| index))
                    .unwrap_or(usize::MAX)
            };
            patch.topics.push(ConsumerTopicStatus {
                key: consumer.key,
                label: consumer.projection.topic.clone(),
                lifecycle,
                order: (order, consumer.key),
                streams_active: topic.map(|topic| topic.streams_active),
                errors,
                failure,
            });
            for (index, event) in events {
                if !patch.owner.request.deliver_rows && event.message_type == "DataLoss" {
                    continue;
                }
                let mut event = (*event).clone();
                event.topic = Some(consumer.projection.topic.to_string());
                patch.events.push(((*index, consumer.key), event));
            }
            if let Some(end) = session_end {
                patch.events.push((
                    (usize::MAX, consumer.key),
                    SubscriptionEventInfo {
                        at_us: end.at_us,
                        category: super::SubscriptionEventCategory::Subscription,
                        level: SubscriptionEventLevel::Warning,
                        message_type: "SubscriptionTerminated".into(),
                        topic: Some(consumer.projection.topic.to_string()),
                        detail: Some(format!("{}: {}", end.message_type, end.detail)),
                    },
                ));
            }
        }
    }

    fn sync_consumer(&self, consumer_id: usize) {
        let upstream = self.session.status.load();
        let mut patches = HashMap::new();
        self.collect_status(
            &upstream,
            &[],
            None,
            true,
            None,
            Some(consumer_id),
            &mut patches,
        );
        for patch in patches.into_values() {
            patch.apply(&self.session, &upstream, GlobalStatusChanges::all(), None);
        }
    }

    fn schedule_cleanup(&self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.schedule_cleanup();
        }
    }
}

fn text_field<'a>(update: &'a SubscriptionUpdate, name: &str) -> Option<&'a str> {
    update.values.iter().find_map(|field| {
        if update.layout.fields[field.index as usize].name.as_ref() != name {
            return None;
        }
        match &field.value {
            UpdateValue::Str(value) => Some(value.as_ref()),
            _ => None,
        }
    })
}

fn apply_delayed(owner: &Consumer, consumer: &mut ConsumerTopic, delayed: Option<bool>) -> bool {
    let previous = owner
        .status
        .load()
        .topic_statuses()
        .get(consumer.projection.topic.as_ref())
        .and_then(|info| info.delayed);
    if previous != delayed {
        owner
            .status
            .update(|status| status.set_delayed(&consumer.projection.topic, delayed));
    }
    if delayed != Some(true) {
        return true;
    }
    match owner.request.delayed_policy {
        DelayedPolicy::Ignore => true,
        DelayedPolicy::Warn => {
            if !consumer.delayed_warned {
                consumer.delayed_warned = true;
                owner.status.update(|status| {
                    status.record_subscription_event(
                        "DelayedStream",
                        Some(consumer.projection.topic.to_string()),
                        Some("Bloomberg is delivering delayed data for this topic".into()),
                        SubscriptionEventLevel::Warning,
                    )
                });
                xbbg_log::warn!(topic = %consumer.projection.topic, "Bloomberg is delivering delayed data");
            }
            true
        }
        DelayedPolicy::Raise => {
            fail_topic(
                owner,
                consumer,
                "Bloomberg is delivering delayed data; on_delayed=raise",
                SubscriptionFailureKind::Failure,
                "SubscriptionFailure",
            );
            false
        }
    }
}

fn apply_field_errors(
    owner: &Consumer,
    consumer: &mut ConsumerTopic,
    errors: &HashMap<String, String>,
) -> bool {
    let rejected = errors
        .iter()
        .filter(|(field, _)| consumer.explicit_fields.contains(*field))
        .min_by(|(left, _), (right, _)| left.cmp(right));
    let Some((field, category)) = rejected else {
        return consumer.active;
    };
    owner.status.update(|status| {
        for (field, category) in errors {
            if !consumer.explicit_fields.contains(field) {
                continue;
            }
            let first = consumer.warned_fields.insert(field.clone());
            status.record_field_error(&consumer.projection.topic, field, category);
            if first && owner.request.field_error_policy == FieldErrorPolicy::Warn {
                status.record_subscription_event(
                    "FieldException",
                    Some(consumer.projection.topic.to_string()),
                    Some(format!("{field}: {category}")),
                    SubscriptionEventLevel::Warning,
                );
            }
        }
    });
    if owner.request.field_error_policy == FieldErrorPolicy::Raise && consumer.active {
        fail_topic(
            owner,
            consumer,
            &format!("{field}: {category}"),
            SubscriptionFailureKind::Failure,
            "SubscriptionFailure",
        );
    }
    consumer.active
}

fn record_topic_failure(
    owner: &Consumer,
    consumer: &mut ConsumerTopic,
    reason: &str,
    kind: SubscriptionFailureKind,
    message_type: &str,
) -> bool {
    consumer.active = false;
    let mut sessions = owner.sessions.lock();
    sessions.detach(consumer.session_id, consumer.key);
    let aggregate = sessions.snapshot(&owner.status.load(), false);
    owner.status.update_with(|status| {
        if !owner.stream.is_closed() {
            aggregate.publish(status);
        }
        if let Some(label) = status.record_failure(consumer.key, reason.to_string(), kind) {
            status.record_subscription_event(
                message_type,
                Some(label),
                Some(reason.to_string()),
                SubscriptionEventLevel::Warning,
            );
        }
        !status.has_active_topics()
    })
}

fn fail_topic(
    owner: &Consumer,
    consumer: &mut ConsumerTopic,
    reason: &str,
    kind: SubscriptionFailureKind,
    message_type: &str,
) {
    if record_topic_failure(owner, consumer, reason, kind, message_type) {
        consumer.projection.fail(BlpError::SubscriptionFailure {
            cid: None,
            label: Some(reason.to_string()),
        });
    }
}

#[derive(Clone, PartialEq, Eq)]
struct GlobalStatusSnapshot {
    session: super::SessionStatusInfo,
    services: HashMap<String, super::ServiceStatusInfo>,
    admin: super::AdminStatusInfo,
}

impl Default for GlobalStatusSnapshot {
    fn default() -> Self {
        Self {
            session: super::SessionStatusInfo {
                state: SessionLifecycleState::Starting,
                last_change_us: 0,
                disconnect_count: 0,
                reconnect_count: 0,
            },
            services: HashMap::new(),
            admin: super::AdminStatusInfo::default(),
        }
    }
}

impl GlobalStatusSnapshot {
    fn from_status(status: &SubscriptionStatusState) -> Self {
        Self {
            session: status.session().clone(),
            services: status.services().clone(),
            admin: status.admin().clone(),
        }
    }

    fn matches(&self, status: &SubscriptionStatusState) -> bool {
        &self.session == status.session()
            && &self.services == status.services()
            && &self.admin == status.admin()
    }

    fn merge_history(&mut self, other: &Self) {
        self.session.disconnect_count += other.session.disconnect_count;
        self.session.reconnect_count += other.session.reconnect_count;
        self.session.last_change_us = self
            .session
            .last_change_us
            .max(other.session.last_change_us);
        self.admin.slow_consumer_warning_count += other.admin.slow_consumer_warning_count;
        self.admin.slow_consumer_cleared_count += other.admin.slow_consumer_cleared_count;
        self.admin.data_loss_count += other.admin.data_loss_count;
        self.admin.last_warning_us = self.admin.last_warning_us.max(other.admin.last_warning_us);
        self.admin.last_cleared_us = self.admin.last_cleared_us.max(other.admin.last_cleared_us);
        self.admin.last_data_loss_us = self
            .admin
            .last_data_loss_us
            .max(other.admin.last_data_loss_us);
        for (name, service) in &other.services {
            let mut service = service.clone();
            if other.session.state == SessionLifecycleState::Terminated {
                service.up = false;
                service.last_change_us = service.last_change_us.max(other.session.last_change_us);
            }
            self.services
                .entry(name.clone())
                .and_modify(|current| {
                    if service.last_change_us > current.last_change_us {
                        *current = service.clone();
                    } else if service.last_change_us == current.last_change_us {
                        current.up &= service.up;
                    }
                })
                .or_insert(service);
        }
    }

    fn publish(self, status: &mut SubscriptionStatusState) {
        status.session = self.session;
        status.services = self.services;
        status.admin = self.admin;
    }
}

struct SessionContribution {
    source: Weak<FeedSession>,
    keys: HashSet<SlabKey>,
    snapshot: GlobalStatusSnapshot,
}

#[derive(Default)]
struct ConsumerSessions {
    sessions: HashMap<usize, SessionContribution>,
    retired: GlobalStatusSnapshot,
}

impl ConsumerSessions {
    fn prune(&mut self) {
        let retired = &mut self.retired;
        self.sessions.retain(|_, contribution| {
            if contribution.source.strong_count() != 0 {
                return true;
            }
            retired.merge_history(&contribution.snapshot);
            false
        });
    }

    fn attach(&mut self, source: &Arc<FeedSession>, key: SlabKey) {
        self.prune();
        let upstream = source.status.load();
        let entry = self
            .sessions
            .entry(source.id())
            .or_insert_with(|| SessionContribution {
                source: Arc::downgrade(source),
                keys: HashSet::new(),
                snapshot: GlobalStatusSnapshot::from_status(&upstream),
            });
        if !entry.snapshot.matches(&upstream) {
            entry.snapshot = GlobalStatusSnapshot::from_status(&upstream);
        }
        entry.keys.insert(key);
    }

    fn update(&mut self, source: usize, upstream: &SubscriptionStatusState) {
        if let Some(entry) = self.sessions.get_mut(&source) {
            if !entry.snapshot.matches(upstream) {
                entry.snapshot = GlobalStatusSnapshot::from_status(upstream);
            }
        }
    }

    fn detach(&mut self, source: usize, key: SlabKey) {
        if let Some(entry) = self.sessions.get_mut(&source) {
            entry.keys.remove(&key);
        }
    }

    fn snapshot(
        &mut self,
        current: &SubscriptionStatusState,
        terminal: bool,
    ) -> GlobalStatusSnapshot {
        self.prune();
        let mut result = self.retired.clone();
        let mut up = false;
        let mut down = false;
        let mut starting = false;
        let mut active_services: HashMap<String, super::ServiceStatusInfo> = HashMap::new();
        result.admin.slow_consumer_warning_active = false;
        for entry in self.sessions.values() {
            result.merge_history(&entry.snapshot);
            if entry.keys.is_empty() {
                continue;
            }
            match entry.snapshot.session.state {
                SessionLifecycleState::Up => up = true,
                SessionLifecycleState::Down => down = true,
                SessionLifecycleState::Starting => starting = true,
                SessionLifecycleState::Terminated => continue,
            }
            result.admin.slow_consumer_warning_active |=
                entry.snapshot.admin.slow_consumer_warning_active;
            for (name, service) in &entry.snapshot.services {
                active_services
                    .entry(name.clone())
                    .and_modify(|current| {
                        current.up &= service.up;
                        current.last_change_us = current.last_change_us.max(service.last_change_us);
                    })
                    .or_insert_with(|| service.clone());
            }
        }
        result.session.state = if down {
            SessionLifecycleState::Down
        } else if starting {
            SessionLifecycleState::Starting
        } else if up {
            SessionLifecycleState::Up
        } else if terminal {
            SessionLifecycleState::Terminated
        } else {
            current.session().state
        };
        result.session.last_change_us = result
            .session
            .last_change_us
            .max(current.session().last_change_us);
        for (name, mut service) in active_services {
            if let Some(previous) = result.services.get(&name) {
                service.last_change_us = service.last_change_us.max(previous.last_change_us);
            }
            result.services.insert(name, service);
        }
        result
    }
}

#[derive(Clone, Copy, Default)]
struct GlobalStatusChanges {
    session: bool,
    services: bool,
    admin: bool,
}

impl GlobalStatusChanges {
    fn all() -> Self {
        Self {
            session: true,
            services: true,
            admin: true,
        }
    }
    fn any(self) -> bool {
        self.session || self.services || self.admin
    }
}

struct SessionEnd {
    message_type: String,
    detail: String,
    at_us: i64,
}

struct ConsumerTopicStatus {
    key: SlabKey,
    order: (usize, SlabKey),
    label: Arc<str>,
    lifecycle: Option<TopicLifecycleState>,
    streams_active: Option<bool>,
    errors: Vec<(String, String, bool)>,
    failure: Option<(String, SubscriptionFailureKind)>,
}

struct ConsumerStatusPatch {
    owner: Arc<Consumer>,
    topics: Vec<ConsumerTopicStatus>,
    events: Vec<((usize, SlabKey), SubscriptionEventInfo)>,
}

impl ConsumerStatusPatch {
    fn apply(
        mut self,
        source: &FeedSession,
        upstream: &SubscriptionStatusState,
        _globals: GlobalStatusChanges,
        session_end: Option<&SessionEnd>,
    ) {
        self.events.sort_unstable_by_key(|(order, _)| *order);
        self.topics.sort_unstable_by_key(|topic| topic.order);
        let failures: Vec<_> = self
            .topics
            .iter()
            .filter_map(|topic| {
                topic
                    .failure
                    .as_ref()
                    .map(|(reason, kind)| (topic.key, reason.clone(), *kind))
            })
            .collect();
        let last_failure = failures.last().map(|(_, reason, _)| reason.clone());
        let mut sessions = self.owner.sessions.lock();
        sessions.update(source.id(), upstream);
        for (key, _, _) in &failures {
            sessions.detach(source.id(), *key);
        }
        let current = self.owner.status.load();
        let aggregate = sessions.snapshot(&current, session_end.is_some());
        let changed = !aggregate.matches(&current)
            || !self.events.is_empty()
            || !failures.is_empty()
            || self.topics.iter().any(|topic| {
                !topic.errors.is_empty()
                    || current
                        .topic_statuses()
                        .get(topic.label.as_ref())
                        .is_some_and(|info| {
                            topic.lifecycle.is_some_and(|state| state != info.state)
                                || topic
                                    .streams_active
                                    .is_some_and(|active| active != info.streams_active)
                        })
            });
        drop(current);
        if !changed || self.owner.stream.is_closed() {
            return;
        }
        let empty = self.owner.status.update_with(|status| {
            if self.owner.stream.is_closed() {
                return !status.has_active_topics();
            }
            aggregate.publish(status);
            for (_, event) in self.events {
                status.append_event(event);
            }
            for topic in self.topics {
                if topic.failure.is_none() && status.topic_for_key(topic.key).is_some() {
                    if let Some(next) = topic.lifecycle {
                        if status
                            .topic_statuses()
                            .get(topic.label.as_ref())
                            .is_some_and(|info| info.state != next)
                        {
                            status.update_topic_state(&topic.label, next);
                        }
                    }
                    if let Some(active) = topic.streams_active {
                        status.set_topic_streams_active(&topic.label, active);
                    }
                }
                for (field, category, first) in topic.errors {
                    status.record_field_error(&topic.label, &field, &category);
                    if first {
                        status.record_subscription_event(
                            "FieldException",
                            Some(topic.label.to_string()),
                            Some(format!("{field}: {category}")),
                            SubscriptionEventLevel::Warning,
                        );
                    }
                }
            }
            if !failures.is_empty() {
                status.record_failures(failures);
            }
            !status.has_active_topics()
        });
        drop(sessions);
        if let Some(reason) = last_failure {
            if empty {
                let error = if let Some(end) = session_end {
                    BlpError::Internal {
                        detail: end.detail.clone(),
                    }
                } else {
                    BlpError::SubscriptionFailure {
                        cid: None,
                        label: Some(reason),
                    }
                };
                self.owner.stream.fail(error);
            }
            self.owner.hub.schedule_cleanup();
        }
    }
}

fn is_global_status_event(event: &SubscriptionEventInfo) -> bool {
    matches!(
        event.category,
        super::SubscriptionEventCategory::Session
            | super::SubscriptionEventCategory::Service
            | super::SubscriptionEventCategory::Lifecycle
    ) || event.topic.is_none()
}

#[cfg(test)]
#[derive(Default)]
struct StatusRoutingMetrics {
    events: AtomicUsize,
    failures: AtomicUsize,
    feeds: AtomicUsize,
}

/// A session is owned by its feeds, not by the handle which happened to create it.
struct FeedSession {
    claim: Mutex<Option<SessionClaim>>,
    status: SharedSubscriptionStatus,
    feeds: Mutex<HashMap<String, Weak<Feed>>>,
    live_feeds: AtomicUsize,
    #[cfg(test)]
    test: Option<Arc<TestSession>>,
    #[cfg(test)]
    routing: StatusRoutingMetrics,
    #[cfg(test)]
    _test_admission: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl FeedSession {
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn new(mut claim: SessionClaim) -> Arc<Self> {
        Arc::new_cyclic(|session: &Weak<Self>| {
            let weak = session.clone();
            let status = Arc::new(SubscriptionStatusHandle::with_observer(Arc::new(
                move |previous, status, scope, events| {
                    if let Some(session) = weak.upgrade() {
                        session.on_status(previous, status, scope, events);
                    }
                },
            )));
            claim.set_cleanup_status(status.clone());
            Self {
                claim: Mutex::new(Some(claim)),
                status,
                feeds: Mutex::new(HashMap::new()),
                live_feeds: AtomicUsize::new(0),
                #[cfg(test)]
                test: None,
                #[cfg(test)]
                routing: StatusRoutingMetrics::default(),
                #[cfg(test)]
                _test_admission: None,
            }
        })
    }

    fn on_status(
        &self,
        previous: &SubscriptionStatusState,
        status: &SubscriptionStatusState,
        scope: SubscriptionStatusScope<'_>,
        events: &[SubscriptionEventInfo],
    ) {
        let failures = &status.failures()[previous.failures().len().min(status.failures().len())..];
        let failure_index: HashMap<_, _> = failures
            .iter()
            .enumerate()
            .map(|(index, failure)| (failure.topic.as_str(), (index, failure)))
            .collect();
        let mut topic_events: HashMap<&str, Vec<(usize, &SubscriptionEventInfo)>> = HashMap::new();
        let mut global_events = Vec::new();
        for (index, event) in events.iter().enumerate() {
            if is_global_status_event(event) {
                global_events.push((index, event));
            } else if let Some(topic) = event.topic.as_deref() {
                topic_events.entry(topic).or_default().push((index, event));
            }
        }
        #[cfg(test)]
        {
            self.routing
                .events
                .fetch_add(events.len(), Ordering::Relaxed);
            self.routing
                .failures
                .fetch_add(failures.len(), Ordering::Relaxed);
        }
        let globals = GlobalStatusChanges {
            session: previous.session() != status.session(),
            services: previous.services() != status.services(),
            admin: previous.admin() != status.admin(),
        };
        let session_end = (globals.session
            && status.session().state == SessionLifecycleState::Terminated)
            .then(|| {
                events.iter().rev().find(|event| {
                    matches!(
                        event.message_type.as_str(),
                        "SessionTerminated" | "AuthorizationRevoked"
                    )
                })
            })
            .flatten()
            .map(|event| SessionEnd {
                message_type: event.message_type.clone(),
                detail: event
                    .detail
                    .clone()
                    .unwrap_or_else(|| "subscription session ended".into()),
                at_us: event.at_us,
            });
        let mut labels: HashSet<&str> = topic_events
            .keys()
            .copied()
            .chain(failure_index.keys().copied())
            .collect();
        if let SubscriptionStatusScope::Topics(keys) = scope {
            for key in keys {
                if let Some(topic) = status
                    .topic_for_key(*key)
                    .or_else(|| previous.topic_for_key(*key))
                    .or_else(|| previous.pending_key_to_topic.get(key).map(String::as_str))
                {
                    labels.insert(topic);
                }
            }
        }
        let all_feeds = matches!(scope, SubscriptionStatusScope::Global)
            && (globals.any() || !global_events.is_empty());
        let feeds: Vec<_> = {
            let mut feeds = self.feeds.lock();
            if all_feeds {
                let mut live = Vec::with_capacity(feeds.len());
                feeds.retain(|_, weak| {
                    if let Some(feed) = weak.upgrade() {
                        live.push(feed);
                        true
                    } else {
                        false
                    }
                });
                live
            } else {
                labels
                    .iter()
                    .filter_map(|label| feeds.get(*label).and_then(Weak::upgrade))
                    .collect()
            }
        };
        #[cfg(test)]
        self.routing.feeds.fetch_add(feeds.len(), Ordering::Relaxed);
        let mut patches = HashMap::new();
        for feed in feeds {
            let topic = feed.status_topic.as_str();
            feed.collect_status(
                status,
                topic_events.get(topic).map(Vec::as_slice).unwrap_or(&[]),
                failure_index.get(topic).copied(),
                labels.contains(topic),
                session_end.as_ref(),
                None,
                &mut patches,
            );
        }
        for mut patch in patches.into_values() {
            let deliver_rows = patch.owner.request.deliver_rows;
            patch.events.extend(
                global_events
                    .iter()
                    .filter(|(_, event)| deliver_rows || event.message_type != "DataLoss")
                    .map(|(index, event)| ((*index, 0), (*event).clone())),
            );
            patch.apply(self, status, globals, session_end.as_ref());
        }
    }

    fn command(&self) -> Result<SubscriptionCommandHandle, BlpAsyncError> {
        self.claim
            .lock()
            .as_ref()
            .ok_or_else(|| config_error("subscription session is closed"))?
            .command_handle()
    }

    async fn subscribe(
        &self,
        feeds: &[Arc<Feed>],
        fields: Vec<String>,
        all_fields: bool,
    ) -> Result<Vec<SlabKey>, BlpAsyncError> {
        let first = feeds.first().expect("nonempty new feed batch");
        let kinds = first.state.lock().kinds.clone();
        let registrations = feeds
            .iter()
            .map(|feed| FeedRegistration {
                topic: feed.identity.topic.clone(),
                status_topic: feed.status_topic.clone(),
                stream: feed.sink(),
            })
            .collect();
        #[cfg(test)]
        if let Some(test) = &self.test {
            return test.subscribe(
                &first.identity.service,
                registrations,
                fields,
                kinds,
                all_fields,
                self.status.clone(),
            );
        }
        let (keys, _) = self
            .command()?
            .subscribe(
                first.identity.service.clone(),
                registrations,
                fields,
                kinds,
                all_fields,
                first.identity.options.clone(),
                Some(1),
                Some(OverflowPolicy::DropNewest),
                self.status.clone(),
            )
            .await?;
        Ok(keys)
    }

    fn resubscribe(
        &self,
        key: SlabKey,
        fields: &[String],
        kinds: &HashMap<String, FieldKind>,
        options: &[String],
    ) -> Result<(), BlpAsyncError> {
        #[cfg(test)]
        if let Some(test) = &self.test {
            return test.resubscribe(key, fields, kinds);
        }
        self.command()?.resubscribe(key, fields, kinds, options)
    }

    fn enable_all_fields(&self, key: SlabKey) -> Result<(), BlpAsyncError> {
        #[cfg(test)]
        if let Some(test) = &self.test {
            test.enable_all_fields(key);
            return Ok(());
        }
        self.command()?.enable_all_fields(key)
    }

    fn seed_kinds(
        &self,
        key: SlabKey,
        kinds: &HashMap<String, FieldKind>,
    ) -> Result<(), BlpAsyncError> {
        #[cfg(test)]
        if let Some(test) = &self.test {
            test.seed_kinds(key, kinds);
            return Ok(());
        }
        self.command()?.seed_kinds(key, kinds)
    }

    fn unsubscribe(&self, key: SlabKey) -> Result<(), BlpAsyncError> {
        #[cfg(test)]
        if let Some(test) = &self.test {
            return test.unsubscribe(key, &self.status);
        }
        self.command()?.unsubscribe_now(vec![key])
    }

    async fn wait_clean(&self) {
        #[cfg(test)]
        if self.test.is_some() {
            return;
        }
        if let Ok(command) = self.command() {
            command.wait_clean().await;
        }
    }
}

#[derive(Clone)]
struct Membership {
    key: SlabKey,
    label: String,
    topic: String,
    session_id: usize,
    feed: Weak<Feed>,
}

struct Consumer {
    id: usize,
    hub: Arc<SharedSubscriptions>,
    request: SubscribeRequest,
    fields: Mutex<Vec<String>>,
    field_kinds: Mutex<HashMap<String, FieldKind>>,
    zero_as_null: HashSet<String>,
    memberships: Mutex<Vec<Membership>>,
    own_session: Mutex<Weak<FeedSession>>,
    sessions: Mutex<ConsumerSessions>,
    status: SharedSubscriptionStatus,
    stream: SubscriptionSender,
    forwarder: Option<SubscriptionForwarder>,
    closed: AtomicBool,
    next_key: AtomicUsize,
}

impl Consumer {
    fn after_detach(
        &self,
        members: &[Membership],
        mutate: impl FnOnce(&mut SubscriptionStatusState),
    ) {
        let mut sessions = self.sessions.lock();
        for member in members {
            sessions.detach(member.session_id, member.key);
        }
        let aggregate = sessions.snapshot(&self.status.load(), false);
        self.status.update(|status| {
            status.defer_indices = true;
            mutate(status);
            if !self.stream.is_closed() {
                aggregate.publish(status);
            }
        });
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let memberships = std::mem::take(self.memberships.get_mut());
        let hub = self.hub.clone();
        let id = self.id;
        self.hub.runtime.spawn(async move {
            let _gate = hub.mutations.lock().await;
            for member in memberships {
                let _ = hub.detach(id, &member);
            }
        });
    }
}

/// Cancelling add while a service opens must not leave attachable phantom feeds.
struct AddRollback {
    consumer: Arc<Consumer>,
    labels: HashSet<String>,
    committed: bool,
}

impl Drop for AddRollback {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let removed = {
            let mut members = self.consumer.memberships.lock();
            let removed: Vec<_> = members
                .iter()
                .filter(|member| self.labels.contains(&member.label))
                .cloned()
                .collect();
            members.retain(|member| !self.labels.contains(&member.label));
            removed
        };
        for member in &removed {
            let _ = self.consumer.hub.detach(self.consumer.id, member);
        }
        self.consumer.after_detach(&removed, |status| {
            for label in &self.labels {
                status.drop_topic(label);
            }
        });
    }
}

/// Cloneable control for one consumer. The last clone detaches all of its topics.
#[derive(Clone)]
pub struct SubscriptionHandle {
    inner: Arc<Consumer>,
}

impl SubscriptionHandle {
    pub fn status(&self) -> SharedSubscriptionStatus {
        self.inner.status.clone()
    }
    pub fn fields(&self) -> Vec<String> {
        self.inner.fields.lock().clone()
    }
    pub fn all_fields(&self) -> bool {
        self.inner.request.all_fields
    }
    pub fn delivers_rows(&self) -> bool {
        self.inner.request.deliver_rows
    }
    pub fn overflow_policy(&self) -> OverflowPolicy {
        self.inner
            .request
            .overflow_policy
            .unwrap_or(self.inner.hub.config.overflow_policy)
    }
    pub fn topics(&self) -> Vec<String> {
        self.inner.status.load().topics().to_vec()
    }
    pub fn is_active(&self) -> bool {
        !self.inner.closed.load(Ordering::Acquire)
            && !self.inner.stream.is_closed()
            && self.inner.status.load().has_active_topics()
    }

    pub async fn add(
        &self,
        topics: Vec<String>,
        aliases: Vec<(String, String)>,
    ) -> Result<(), BlpAsyncError> {
        self.inner.hub.add(&self.inner, topics, aliases).await
    }

    pub async fn remove(&self, labels: Vec<String>) -> Result<(), BlpAsyncError> {
        let _gate = self.inner.hub.mutations.lock().await;
        let labels: HashSet<_> = labels.into_iter().collect();
        let removed = {
            let mut members = self.inner.memberships.lock();
            let removed: Vec<_> = members
                .iter()
                .filter(|member| labels.contains(&member.label))
                .cloned()
                .collect();
            members.retain(|member| !labels.contains(&member.label));
            removed
        };
        let mut error = None;
        for member in &removed {
            if let Err(next) = self.inner.hub.detach(self.inner.id, member) {
                error.get_or_insert(next);
            }
        }
        self.inner.after_detach(&removed, |status| {
            for label in &labels {
                status.drop_topic(label);
            }
        });
        error.map_or(Ok(()), Err)
    }

    pub async fn add_fields(&self, fields: Vec<String>) -> Result<(), BlpAsyncError> {
        self.ensure_open()?;
        let fields = normalized_fields(fields);
        let resolved = self
            .inner
            .hub
            .resolve_kinds(&self.inner.request.service, &fields)
            .await;
        let _gate = self.inner.hub.mutations.lock().await;
        self.ensure_open()?;
        let new_fields = {
            let mut existing = self.inner.fields.lock();
            let mut added = Vec::new();
            for field in fields {
                if !existing.contains(&field) {
                    existing.push(field.clone());
                    added.push(field);
                }
            }
            added
        };
        let kinds_changed = merge_kind_hints(&mut self.inner.field_kinds.lock(), &resolved);
        let kinds = self.inner.field_kinds.lock().clone();
        if new_fields.is_empty() && !kinds_changed {
            return Ok(());
        }
        let members = self.inner.memberships.lock().clone();
        let mut visited = HashSet::new();
        let mut resubscribe = Vec::new();
        let mut first_error = None;
        for member in members {
            let Some(feed) = member.feed.upgrade() else {
                continue;
            };
            if !visited.insert(Arc::as_ptr(&feed) as usize) {
                continue;
            }
            let changed = {
                let mut state = feed.state.lock();
                let changed = grow_union(&mut state, &new_fields);
                seed_image_kinds(&mut state, &kinds);
                let image = (!new_fields.is_empty()
                    && self.inner.request.deliver_rows
                    && !changed
                    && feed.identity.service == MKTDATA)
                    .then(|| image_update(&feed, &state, true))
                    .flatten();
                let errors = state.field_errors.clone();
                for consumer in &mut state.consumers {
                    if consumer.active && consumer.owner.ptr_eq(&Arc::downgrade(&self.inner)) {
                        consumer.projection.add_fields(&new_fields);
                        consumer.projection.seed_kinds(&kinds);
                        consumer.explicit_fields.extend(new_fields.iter().cloned());
                        consumer.awaiting_image |= changed;
                        consumer.awaiting_repaint |= changed;
                        if !apply_field_errors(&self.inner, consumer, &errors) {
                            continue;
                        }
                        if let Some(image) = &image {
                            consumer.projection.project_update(image, false);
                        }
                    }
                }
                changed
            };
            if changed {
                resubscribe.push(feed);
            } else if !kinds.is_empty() {
                if let Err(error) = feed
                    .session
                    .seed_kinds(feed.upstream_key.load(Ordering::Acquire), &kinds)
                {
                    feed.on_error(BlpError::Internal {
                        detail: error.to_string(),
                    });
                    first_error.get_or_insert(error);
                }
            }
        }
        for feed in resubscribe {
            if let Err(error) = self.inner.hub.resubscribe(&feed) {
                first_error.get_or_insert(error);
            }
        }
        self.inner.hub.schedule_cleanup();
        first_error.map_or(Ok(()), Err)
    }

    pub async fn unsubscribe(&self) -> Result<(), BlpAsyncError> {
        let _gate = self.inner.hub.mutations.lock().await;
        self.inner.closed.store(true, Ordering::Release);
        self.inner.hub.changed.notify_waiters();
        let members = std::mem::take(&mut *self.inner.memberships.lock());
        let mut error = None;
        for member in &members {
            if let Err(next) = self.inner.hub.detach(self.inner.id, member) {
                error.get_or_insert(next);
            }
        }
        self.inner
            .after_detach(&members, |status| status.clear_active());
        if !self.inner.request.deliver_rows {
            self.inner.stream.close();
        }
        error.map_or(Ok(()), Err)
    }

    pub async fn drain_forwarder(&self) -> Result<(), BlpAsyncError> {
        if let Some(forwarder) = &self.inner.forwarder {
            forwarder
                .drain()
                .await
                .map_err(|_| BlpAsyncError::ChannelClosed)?;
        }
        Ok(())
    }

    pub fn take_warnings(&self) -> Vec<SubscriptionEventInfo> {
        self.inner.status.take_warnings()
    }

    pub fn latest(&self) -> Result<RecordBatch, BlpAsyncError> {
        self.ensure_open()?;
        let active: HashSet<_> = self.inner.status.load().keys().iter().copied().collect();
        let members = self.inner.memberships.lock().clone();
        let mut rows = Vec::new();
        let seeded = self.inner.field_kinds.lock().clone();
        let mut field_order: Vec<(Arc<str>, FieldKind, bool)> = self
            .fields()
            .into_iter()
            .filter(|field| !is_metadata(field))
            .map(|field| {
                let kind = seeded.get(&field).copied().unwrap_or(FieldKind::Unknown);
                (Arc::from(field), kind, true)
            })
            .collect();
        for member in members {
            if !active.contains(&member.key) {
                continue;
            }
            let Some(feed) = member.feed.upgrade() else {
                continue;
            };
            let mut state = feed.state.lock();
            let source = image_update(&feed, &state, false);
            let last_update = state.last_update;
            let live = state.live;
            let delayed = state.delayed;
            let Some(consumer) = state.consumers.iter_mut().find(|consumer| {
                consumer.key == member.key && consumer.owner.ptr_eq(&Arc::downgrade(&self.inner))
            }) else {
                continue;
            };
            let projected = source
                .as_ref()
                .map(|source| consumer.projection.project_image(source));
            let layout = consumer.projection.layout();
            for field in layout
                .fields
                .iter()
                .filter(|field| !is_metadata(&field.name))
            {
                if field.kind == FieldKind::Unknown {
                    continue;
                }
                if let Some((_, kind, provisional)) = field_order
                    .iter_mut()
                    .find(|(name, _, _)| name == &field.name)
                {
                    if *kind == FieldKind::Unknown || (*provisional && !field.provisional) {
                        *kind = field.kind;
                        *provisional = field.provisional;
                    } else if *provisional == field.provisional {
                        *kind = kind.merge_observed(field.kind);
                    }
                } else {
                    field_order.push((field.name.clone(), field.kind, field.provisional));
                }
            }
            rows.push((member.label, last_update, live, delayed, projected));
        }
        let mut schema = vec![
            Field::new("topic", arrow_schema::DataType::Utf8, false),
            Field::new(
                "last_update",
                ArrowType::TimestampMicros.to_arrow_datatype(),
                true,
            ),
            Field::new("live", arrow_schema::DataType::Boolean, false),
            Field::new("delayed", arrow_schema::DataType::Boolean, true),
        ];
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
        ];
        for (name, kind, _) in field_order {
            let arrow_type = arrow_kind(kind);
            schema.push(Field::new(
                name.as_ref(),
                arrow_type.to_arrow_datatype(),
                true,
            ));
            let mut builder = TypedBuilder::new(arrow_type);
            let zero_as_null = self.inner.zero_as_null.contains(name.as_ref());
            for row in &rows {
                let value =
                    row.4
                        .as_ref()
                        .and_then(|update| {
                            update.values.iter().find(|value| {
                                update.layout.fields[value.index as usize].name == name
                            })
                        })
                        .map(|value| &value.value);
                let value = value.filter(|value| !zero_as_null || !is_numeric_zero(value));
                builder.append_value(value.map(core_value));
            }
            arrays.push(builder.finish());
        }
        RecordBatch::try_new(Arc::new(Schema::new(schema)), arrays)
            .map_err(|error| BlpAsyncError::Internal(error.to_string()))
    }

    fn ensure_open(&self) -> Result<(), BlpAsyncError> {
        if self.inner.closed.load(Ordering::Acquire) || self.inner.stream.is_closed() {
            Err(BlpAsyncError::ChannelClosed)
        } else {
            Ok(())
        }
    }
}

pub(super) struct SharedSubscriptions {
    pool: Arc<SubscriptionSessionPool>,
    config: Arc<EngineConfig>,
    runtime: tokio::runtime::Handle,
    type_resolver: Option<Arc<super::subscription_types::SubscriptionTypeResolver>>,
    registry: Mutex<HashMap<FeedKey, Arc<Feed>>>,
    consumers: Mutex<Vec<Weak<Consumer>>>,
    shutdown: AtomicBool,
    mutations: AsyncMutex<()>,
    changed: Notify,
    next_consumer: AtomicUsize,
    next_feed: AtomicUsize,
    cleanup_pending: AtomicBool,
    #[cfg(test)]
    test_sessions: Option<Arc<TestSessionFactory>>,
}

impl SharedSubscriptions {
    pub(super) fn new(
        pool: Arc<SubscriptionSessionPool>,
        config: Arc<EngineConfig>,
        runtime: tokio::runtime::Handle,
        type_resolver: Option<Arc<super::subscription_types::SubscriptionTypeResolver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            config,
            runtime,
            type_resolver,
            registry: Mutex::new(HashMap::new()),
            consumers: Mutex::new(Vec::new()),
            shutdown: AtomicBool::new(false),
            mutations: AsyncMutex::new(()),
            changed: Notify::new(),
            next_consumer: AtomicUsize::new(1),
            next_feed: AtomicUsize::new(1),
            cleanup_pending: AtomicBool::new(false),
            #[cfg(test)]
            test_sessions: None,
        })
    }

    pub(super) fn shutdown(self: &Arc<Self>) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        let consumers: Vec<_> = self
            .consumers
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for consumer in consumers {
            consumer.stream.fail(BlpError::Internal {
                detail: "subscription engine shut down".into(),
            });
            consumer.status.update(|status| {
                status.record_session_state(
                    SessionLifecycleState::Terminated,
                    "SessionTerminated",
                    Some("subscription engine shut down".into()),
                );
                status.clear_active();
            });
        }
        self.changed.notify_waiters();
        self.schedule_cleanup();
    }

    pub(super) async fn subscribe(
        self: &Arc<Self>,
        mut request: SubscribeRequest,
    ) -> Result<SubscriptionStream, BlpAsyncError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(config_error("subscription engine is shut down"));
        }
        request.service = request.service.trim().to_string();
        request.fields = normalized_fields(request.fields);
        request.options = normalize_options(request.options);
        let capacity = request
            .stream_capacity
            .unwrap_or(self.config.subscription_stream_capacity);
        if capacity == 0 {
            return Err(config_error(
                "subscription stream capacity must be greater than zero",
            ));
        }
        let topics = validate_topic_aliases(
            std::mem::take(&mut request.topics),
            std::mem::take(&mut request.aliases),
        )?;
        let (stream, rx) = subscription_channel(if request.deliver_rows { capacity } else { 1 });
        let hub = Arc::downgrade(self);
        stream.on_closed(Arc::new(move || {
            if let Some(hub) = hub.upgrade() {
                hub.changed.notify_waiters();
                hub.schedule_cleanup();
            }
        }));
        let forwarder = if request.deliver_rows
            && request
                .overflow_policy
                .unwrap_or(self.config.overflow_policy)
                == OverflowPolicy::Block
        {
            let (sender, future) = subscription_forwarder_channel(self.config.command_queue_size);
            self.runtime.spawn(future);
            Some(sender)
        } else {
            None
        };
        let consumer = Arc::new(Consumer {
            id: self.next_consumer.fetch_add(1, Ordering::Relaxed),
            hub: self.clone(),
            fields: Mutex::new(request.fields.clone()),
            field_kinds: Mutex::new(HashMap::new()),
            zero_as_null: normalized_fields(request.zero_as_null.clone())
                .into_iter()
                .collect(),
            request,
            memberships: Mutex::new(Vec::new()),
            own_session: Mutex::new(Weak::new()),
            status: Arc::new(SubscriptionStatusHandle::default()),
            sessions: Mutex::new(ConsumerSessions::default()),
            stream,
            forwarder,
            closed: AtomicBool::new(false),
            next_key: AtomicUsize::new(0),
        });
        {
            let mut consumers = self.consumers.lock();
            consumers.retain(|consumer| consumer.strong_count() != 0);
            consumers.push(Arc::downgrade(&consumer));
        }
        self.add_validated(&consumer, topics).await?;
        Ok(SubscriptionStream {
            rx,
            handle: SubscriptionHandle { inner: consumer },
        })
    }

    async fn claim(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Arc<FeedSession>, BlpAsyncError> {
        #[cfg(test)]
        if let Some(factory) = &self.test_sessions {
            return Ok(factory.claim(permit).await);
        }
        Ok(FeedSession::new(self.pool.claim(permit).await?))
    }

    async fn add(
        self: &Arc<Self>,
        consumer: &Arc<Consumer>,
        topics: Vec<String>,
        aliases: Vec<(String, String)>,
    ) -> Result<(), BlpAsyncError> {
        self.add_validated(consumer, validate_topic_aliases(topics, aliases)?)
            .await
    }

    async fn resolve_kinds(&self, service: &str, fields: &[String]) -> HashMap<String, FieldKind> {
        if service != MKTDATA || fields.is_empty() {
            return HashMap::new();
        }
        match &self.type_resolver {
            Some(resolver) => resolver.resolve(fields).await,
            None => HashMap::new(),
        }
    }

    async fn add_validated(
        self: &Arc<Self>,
        consumer: &Arc<Consumer>,
        requested: Vec<(String, String)>,
    ) -> Result<(), BlpAsyncError> {
        if consumer.closed.load(Ordering::Acquire) || consumer.stream.is_closed() {
            return Err(BlpAsyncError::ChannelClosed);
        }
        validate_existing_labels(consumer, &requested)?;
        let fields = consumer.fields.lock().clone();
        let resolved = self.resolve_kinds(&consumer.request.service, &fields).await;
        let mut acquired = None;
        let mut admission_started = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shutdown.load(Ordering::Acquire) {
                return Err(config_error("subscription engine is shut down"));
            }
            let gate = self.mutations.lock().await;
            if consumer.closed.load(Ordering::Acquire) || consumer.stream.is_closed() {
                return Err(BlpAsyncError::ChannelClosed);
            }
            validate_existing_labels(consumer, &requested)?;
            let snapshot = consumer.status.load();
            let requested: Vec<_> = requested
                .iter()
                .filter(|(_, label)| !snapshot.topic_to_key().contains_key(label))
                .cloned()
                .collect();
            drop(snapshot);
            if requested.is_empty() {
                merge_kind_hints(&mut consumer.field_kinds.lock(), &resolved);
                return Ok(());
            }
            let keys: Vec<_> = requested
                .iter()
                .map(|(topic, _)| FeedKey {
                    service: consumer.request.service.clone(),
                    topic: topic.clone(),
                    options: consumer.request.options.clone(),
                    isolation: if consumer.request.isolated || consumer.request.service != MKTDATA {
                        self.next_feed.fetch_add(1, Ordering::Relaxed)
                    } else {
                        0
                    },
                })
                .collect();
            let mut retired: Vec<_> = {
                let registry = self.registry.lock();
                keys.iter()
                    .filter_map(|key| registry.get(key).cloned())
                    .collect()
            };
            retired.retain(|feed| matches!(feed.state.lock().lifecycle, "failed" | "closed"));
            let missing = {
                let registry = self.registry.lock();
                !retired.is_empty() || keys.iter().any(|key| !registry.contains_key(key))
            };
            let existing_session = consumer.own_session.lock().upgrade().filter(|session| {
                session.status.load().session().state != SessionLifecycleState::Terminated
            });
            if missing && existing_session.is_none() && acquired.is_none() {
                drop(retired);
                drop(gate);
                let deadline = match consumer.request.session_wait {
                    Some(wait) => Some(
                        admission_started
                            .get_or_insert_with(tokio::time::Instant::now)
                            .checked_add(wait)
                            .ok_or_else(|| {
                                config_error("session_wait: duration exceeds supported clock range")
                            })?,
                    ),
                    None => None,
                };
                let admission = async {
                    match deadline {
                        Some(deadline) => {
                            tokio::time::timeout_at(deadline, self.pool.acquire_capacity())
                                .await
                                .map_err(|_| config_error(SESSION_WAIT_TIMED_OUT))?
                        }
                        None => self.pool.acquire_capacity().await,
                    }
                };
                let permit = tokio::select! {
                    biased;
                    result = admission => Some(result?),
                    _ = &mut changed => None,
                };
                if let Some(permit) = permit {
                    // Once capacity is reserved, startup keeps its normal failure semantics.
                    acquired = Some(self.claim(permit).await?);
                }
                continue;
            }
            // Admission is complete before any existing feed or consumer state changes.
            for feed in retired {
                let _ = self.retire(feed);
            }
            let session = existing_session.or_else(|| acquired.take());
            merge_kind_hints(&mut consumer.field_kinds.lock(), &resolved);
            let fields = consumer.fields.lock().clone();
            let kinds = consumer.field_kinds.lock().clone();
            let planned: Vec<_> = requested
                .into_iter()
                .zip(keys)
                .map(|((_, label), identity)| {
                    let key = consumer.next_key.fetch_add(1, Ordering::Relaxed);
                    let mut projection = SubscriptionState::with_policy_and_forwarder(
                        identity.topic.clone(),
                        fields.clone(),
                        consumer.stream.clone(),
                        consumer
                            .request
                            .flush_threshold
                            .unwrap_or(self.config.subscription_flush_threshold),
                        consumer
                            .request
                            .overflow_policy
                            .unwrap_or(self.config.overflow_policy),
                        consumer.request.all_fields,
                        consumer.forwarder.clone(),
                    );
                    projection.set_service(&identity.service);
                    projection.seed_kinds(&kinds);
                    projection.set_label(Arc::from(label.as_str()));
                    projection.set_topic_id(key as u32);
                    (label, identity, key, projection)
                })
                .collect();
            let mut rollback = AddRollback {
                consumer: consumer.clone(),
                labels: planned
                    .iter()
                    .map(|(label, _, _, _)| label.clone())
                    .collect(),
                committed: false,
            };
            // Reserve every sibling before the SDK can reject the first topic.
            consumer.status.update(|status| {
                for (label, identity, key, projection) in &planned {
                    status.add_active(
                        std::slice::from_ref(label),
                        &[*key],
                        vec![projection.metrics.clone()],
                    );
                    status.set_feed_topic(label, &identity.topic);
                }
            });
            let mut creations = Vec::new();
            for (label, identity, key, projection) in planned {
                let existing = self.registry.lock().get(&identity).cloned();
                let (feed, created) = match existing {
                    Some(feed) => (feed, false),
                    None => {
                        let session = session
                            .as_ref()
                            .expect("missing feed acquired a session")
                            .clone();
                        let feed = Feed::new(
                            identity.clone(),
                            fields.clone(),
                            &kinds,
                            session.clone(),
                            Arc::downgrade(self),
                            self.next_feed.fetch_add(1, Ordering::Relaxed),
                        );
                        session
                            .feeds
                            .lock()
                            .insert(feed.status_topic.clone(), Arc::downgrade(&feed));
                        session.live_feeds.fetch_add(1, Ordering::Relaxed);
                        self.registry.lock().insert(identity, feed.clone());
                        *consumer.own_session.lock() = Arc::downgrade(&session);
                        (feed, true)
                    }
                };
                if !created && !kinds.is_empty() {
                    feed.session
                        .seed_kinds(feed.upstream_key.load(Ordering::Acquire), &kinds)?;
                }
                if consumer.request.all_fields && !feed.state.lock().capture_all_fields {
                    if !created {
                        feed.session
                            .enable_all_fields(feed.upstream_key.load(Ordering::Acquire))?;
                    }
                    feed.state.lock().capture_all_fields = true;
                }
                consumer.sessions.lock().attach(&feed.session, key);
                consumer.memberships.lock().push(Membership {
                    key,
                    label,
                    topic: feed.identity.topic.clone(),
                    session_id: feed.session.id(),
                    feed: Arc::downgrade(&feed),
                });
                let growth = {
                    let mut state = feed.state.lock();
                    if matches!(state.lifecycle, "failed" | "closed") {
                        return Err(BlpAsyncError::BlpError(BlpError::SubscriptionFailure {
                            cid: None,
                            label: Some("upstream feed ended while attaching".into()),
                        }));
                    }
                    let growth = grow_union(&mut state, &fields);
                    seed_image_kinds(&mut state, &kinds);
                    let mut entry = ConsumerTopic {
                        owner: Arc::downgrade(consumer),
                        key,
                        projection,
                        session_id: feed.session.id(),
                        explicit_fields: fields.iter().cloned().collect(),
                        awaiting_image: created
                            || growth
                            || state.last_update.is_none()
                            || state.recovery.is_some(),
                        awaiting_repaint: growth,
                        active: true,
                        delayed_warned: false,
                        warned_fields: HashSet::new(),
                        pending_data_loss: 0,
                        last_data_loss_us: 0,
                    };
                    let allowed = apply_delayed(consumer, &mut entry, state.delayed)
                        && apply_field_errors(consumer, &mut entry, &state.field_errors);
                    if allowed
                        && consumer.request.deliver_rows
                        && !created
                        && feed.identity.service == MKTDATA
                    {
                        if let Some(image) = image_update(&feed, &state, true) {
                            // A synthetic image is always one row, including an
                            // entirely unknown projection. Only known values have presence bits.
                            entry.projection.project_update(&image, false);
                        }
                    }
                    state.consumers.push(entry);
                    if !allowed {
                        self.schedule_cleanup();
                    }
                    growth
                };
                if created {
                    creations.push(feed);
                } else {
                    if growth {
                        self.resubscribe(&feed)?;
                    }
                    feed.sync_consumer(consumer.id);
                }
            }
            if let Some(first) = creations.first() {
                match first
                    .session
                    .subscribe(&creations, fields, consumer.request.all_fields)
                    .await
                {
                    Ok(keys) => {
                        for (feed, key) in creations.iter().zip(keys) {
                            feed.upstream_key.store(key, Ordering::Release);
                        }
                    }
                    Err(error) => {
                        for feed in &creations {
                            feed.on_error(BlpError::Internal {
                                detail: error.to_string(),
                            });
                        }
                        self.changed.notify_waiters();
                        return Err(error);
                    }
                }
            }
            rollback.committed = true;
            self.changed.notify_waiters();
            return Ok(());
        }
    }

    fn resubscribe(&self, feed: &Arc<Feed>) -> Result<(), BlpAsyncError> {
        let (fields, kinds) = {
            let mut state = feed.state.lock();
            state.repainting = feed.identity.service == MKTDATA || state.recovery.is_some();
            state.repaint_started = false;
            (state.fields.clone(), state.kinds.clone())
        };
        let result = feed.session.resubscribe(
            feed.upstream_key.load(Ordering::Acquire),
            &fields,
            &kinds,
            &feed.identity.options,
        );
        if let Err(error) = &result {
            feed.on_error(BlpError::Internal {
                detail: error.to_string(),
            });
        }
        result
    }

    fn detach(&self, consumer_id: usize, membership: &Membership) -> Result<(), BlpAsyncError> {
        let Some(feed) = membership.feed.upgrade() else {
            return Ok(());
        };
        let empty = {
            let mut state = feed.state.lock();
            state.consumers.retain(|consumer| {
                !(consumer.key == membership.key
                    && consumer
                        .owner
                        .upgrade()
                        .is_none_or(|owner| owner.id == consumer_id))
            });
            state.consumers.is_empty()
        };
        if empty {
            self.retire(feed)
        } else {
            Ok(())
        }
    }

    fn retire(&self, feed: Arc<Feed>) -> Result<(), BlpAsyncError> {
        let removed = {
            let mut registry = self.registry.lock();
            if registry
                .get(&feed.identity)
                .is_some_and(|current| Arc::ptr_eq(current, &feed))
            {
                registry.remove(&feed.identity);
                true
            } else {
                false
            }
        };
        if !removed {
            return Ok(());
        }
        feed.state.lock().lifecycle = "closed";
        feed.session.feeds.lock().remove(&feed.status_topic);
        let key = feed.upstream_key.load(Ordering::Acquire);
        let result = if key == usize::MAX {
            Ok(())
        } else {
            feed.session.unsubscribe(key)
        };
        let session = feed.session.clone();
        drop(feed);
        if session.live_feeds.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.runtime.spawn(async move {
                session.wait_clean().await;
            });
        }
        self.changed.notify_waiters();
        result
    }

    fn schedule_recovery(self: &Arc<Self>, identity: &FeedKey) {
        let Some(feed) = self.registry.lock().get(identity).cloned() else {
            return;
        };
        let hub = self.clone();
        self.runtime.spawn(async move {
            let _gate = hub.mutations.lock().await;
            loop {
                let send = {
                    let mut state = feed.state.lock();
                    if hub.shutdown.load(Ordering::Acquire)
                        || matches!(state.lifecycle, "failed" | "closed")
                    {
                        state.recovery = None;
                        state.recovery_task_scheduled = false;
                        false
                    } else if let Some(recovery) = &mut state.recovery {
                        match recovery.stage {
                            RecoveryStage::QueuedInitial => {
                                recovery.stage = RecoveryStage::WaitingInitialPaint;
                                true
                            }
                            RecoveryStage::QueuedRestart => {
                                recovery.stage = RecoveryStage::WaitingRestartStart;
                                true
                            }
                            _ => {
                                state.recovery_task_scheduled = false;
                                false
                            }
                        }
                    } else {
                        state.recovery_task_scheduled = false;
                        false
                    }
                };
                if !send {
                    break;
                }
                if hub.resubscribe(&feed).is_err() {
                    let mut state = feed.state.lock();
                    state.recovery = None;
                    state.recovery_task_scheduled = false;
                    break;
                }
            }
        });
    }

    fn schedule_cleanup(self: &Arc<Self>) {
        if self.cleanup_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let hub = self.clone();
        self.runtime.spawn(async move {
            let _gate = hub.mutations.lock().await;
            hub.cleanup_pending.store(false, Ordering::Release);
            let feeds: Vec<_> = hub.registry.lock().values().cloned().collect();
            for feed in feeds {
                let empty = {
                    let mut state = feed.state.lock();
                    let failed = state.lifecycle == "failed";
                    state.consumers.retain(|consumer| {
                        consumer.owner.upgrade().is_some_and(|owner| {
                            let active = !failed
                                && consumer.active
                                && !owner.closed.load(Ordering::Acquire)
                                && !consumer.projection.stream.is_closed();
                            if !active && consumer.projection.stream.is_closed() {
                                owner.status.update(|status| status.clear_active());
                            }
                            active
                        })
                    });
                    state.consumers.is_empty()
                };
                if empty {
                    let _ = hub.retire(feed);
                }
            }
        });
    }

    pub(super) fn feeds(&self) -> Vec<FeedInfo> {
        let feeds: Vec<_> = self.registry.lock().values().cloned().collect();
        let mut infos: Vec<_> = feeds
            .into_iter()
            .map(|feed| {
                let state = feed.state.lock();
                FeedInfo {
                    service: feed.identity.service.clone(),
                    topic: feed.identity.topic.clone(),
                    options: feed.identity.options.clone(),
                    fields: state.fields.clone(),
                    consumers: state.consumers.len(),
                    delayed: state.delayed,
                    state: state.lifecycle.into(),
                    isolated: feed.identity.isolation != 0,
                    field_errors: state.field_errors.clone(),
                }
            })
            .collect();
        infos.sort_by(|a, b| {
            (&a.service, &a.topic, &a.options).cmp(&(&b.service, &b.topic, &b.options))
        });
        infos
    }
}

fn grow_union(state: &mut FeedState, fields: &[String]) -> bool {
    let mut changed = false;
    for field in fields {
        if !state.fields.contains(field) {
            state.fields.push(field.clone());
            changed = true;
        }
    }
    if changed && state.last_update.is_some() {
        for consumer in &mut state.consumers {
            if !consumer.awaiting_repaint {
                consumer.awaiting_image = false;
            }
        }
    }
    changed
}

fn validate_existing_labels(
    consumer: &Consumer,
    requested: &[(String, String)],
) -> Result<(), BlpAsyncError> {
    let memberships = consumer.memberships.lock();
    let labels: HashMap<_, _> = memberships
        .iter()
        .map(|member| (member.label.as_str(), member.topic.as_str()))
        .collect();
    for (topic, label) in requested {
        if let Some(existing) = labels.get(label.as_str()) {
            if *existing != topic {
                return Err(alias_label_conflict(label, existing, topic));
            }
        }
    }
    Ok(())
}

fn merge_kind_hints(
    target: &mut HashMap<String, FieldKind>,
    kinds: &HashMap<String, FieldKind>,
) -> bool {
    let mut changed = false;
    for (field, kind) in kinds {
        if *kind != FieldKind::Unknown && !target.contains_key(field) {
            target.insert(field.clone(), *kind);
            changed = true;
        }
    }
    changed
}

fn seed_image_kinds(state: &mut FeedState, kinds: &HashMap<String, FieldKind>) {
    for (field, kind) in kinds {
        if *kind != FieldKind::Unknown {
            state.kinds.entry(field.clone()).or_insert(*kind);
        }
    }
    let mut fields = state
        .layout
        .as_ref()
        .map(|layout| layout.fields.to_vec())
        .unwrap_or_default();
    let mut indices: HashMap<Arc<str>, usize> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name.clone(), index))
        .collect();
    let mut changed = state.layout.is_none();
    for name in state
        .fields
        .iter()
        .map(String::as_str)
        .chain([EVENT_TYPE, EVENT_SUBTYPE])
    {
        if !indices.contains_key(name) {
            let index = fields.len();
            let name = Arc::<str>::from(name);
            fields.push(FieldMeta::new(
                name.clone(),
                index as u16,
                FieldKind::Unknown,
            ));
            indices.insert(name, index);
            changed = true;
        }
    }
    for field in &mut fields {
        if field.kind == FieldKind::Unknown {
            if let Some(&kind) = state.kinds.get(field.name.as_ref()) {
                field.kind = kind;
                field.provisional = true;
                changed = true;
            }
        }
    }
    if changed {
        let version = state
            .layout
            .as_ref()
            .map_or(1, |layout| layout.version.wrapping_add(1).max(1));
        state.image.resize(fields.len(), None);
        state.layout = Some(Arc::new(FieldLayout::new(version, fields)));
    }
}

fn repaint_changes(
    update: &SubscriptionUpdate,
    image: &[Option<UpdateValue>],
) -> (SubscriptionUpdate, bool) {
    let mut values = SmallVec::new();
    let mut has_data = false;
    for field in &update.values {
        let metadata = is_metadata(&update.layout.fields[field.index as usize].name);
        let same = image.get(field.index as usize).and_then(Option::as_ref).is_some_and(|previous| {
            previous == &field.value || matches!((previous, &field.value),
                (UpdateValue::F64(left), UpdateValue::F64(right)) if left.is_nan() && right.is_nan())
        });
        if metadata || !same {
            has_data |= !metadata;
            values.push(field.clone());
        }
    }
    (
        SubscriptionUpdate {
            timestamp_us: update.timestamp_us,
            topic_id: update.topic_id,
            topic: update.topic.clone(),
            layout: update.layout.clone(),
            values,
        },
        has_data,
    )
}

fn image_update(feed: &Feed, state: &FeedState, synthetic: bool) -> Option<SubscriptionUpdate> {
    if synthetic && (state.last_update.is_none() || state.recovery.is_some()) {
        return None;
    }
    let layout = state.layout.as_ref()?.clone();
    let mut values = SmallVec::new();
    for field in layout.fields.iter() {
        let value = if synthetic && field.name.as_ref() == EVENT_TYPE {
            Some(UpdateValue::Str(Arc::from("SUMMARY")))
        } else if synthetic && field.name.as_ref() == EVENT_SUBTYPE {
            Some(UpdateValue::Str(Arc::from("INITPAINT")))
        } else {
            state.image[field.index as usize].clone()
        };
        if let Some(value) = value {
            values.push(UpdateField {
                index: field.index,
                value,
            });
        }
    }
    Some(SubscriptionUpdate {
        timestamp_us: if synthetic {
            timestamp_now_us()
        } else {
            state.last_update.unwrap_or(0)
        },
        topic_id: 0,
        topic: Arc::from(feed.identity.topic.as_str()),
        layout,
        values,
    })
}

fn is_numeric_zero(value: &UpdateValue) -> bool {
    matches!(value, UpdateValue::I32(0) | UpdateValue::I64(0))
        || matches!(value, UpdateValue::F64(value) if *value == 0.0)
}

fn is_metadata(field: &str) -> bool {
    matches!(field, EVENT_TYPE | EVENT_SUBTYPE)
}

fn alias_label_conflict(label: &str, first: &str, second: &str) -> BlpAsyncError {
    config_error(&format!(
        "subscription label '{label}' maps to conflicting topics '{first}' and '{second}'"
    ))
}

fn validate_topic_aliases(
    topics: Vec<String>,
    aliases: Vec<(String, String)>,
) -> Result<Vec<(String, String)>, BlpAsyncError> {
    let mut by_topic = HashMap::<String, String>::new();
    let mut by_label = HashMap::<String, String>::new();
    for (topic, label) in aliases {
        let topic = topic.trim().to_string();
        if let Some(previous) = by_topic.get(&topic) {
            if previous != &label {
                return Err(config_error(&format!("subscription topic '{topic}' maps to conflicting labels '{previous}' and '{label}'")));
            }
            continue;
        }
        if let Some(previous) = by_label.get(&label) {
            if previous != &topic {
                return Err(alias_label_conflict(&label, previous, &topic));
            }
        }
        by_label.insert(label.clone(), topic.clone());
        by_topic.insert(topic, label);
    }
    let mut seen = HashSet::new();
    let mut requested = Vec::with_capacity(topics.len());
    for topic in topics {
        let topic = topic.trim().to_string();
        let label = by_topic
            .get(&topic)
            .cloned()
            .unwrap_or_else(|| topic.clone());
        if let Some(previous) = by_label.get(&label) {
            if previous != &topic {
                return Err(alias_label_conflict(&label, previous, &topic));
            }
        }
        by_label.insert(label.clone(), topic.clone());
        if seen.insert(label.clone()) {
            requested.push((topic, label));
        }
    }
    Ok(requested)
}

fn normalized_fields(fields: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    fields
        .into_iter()
        .map(|field| field.trim().to_string())
        .filter(|field| seen.insert(field.clone()))
        .collect()
}

fn normalize_options(options: Vec<String>) -> Vec<String> {
    let mut options: Vec<_> = options
        .into_iter()
        .map(|option| option.trim().to_string())
        .filter(|option| !option.is_empty())
        .collect();
    options.sort();
    options.dedup();
    options
}

fn config_error(detail: &str) -> BlpAsyncError {
    BlpAsyncError::ConfigError {
        detail: detail.into(),
    }
}

fn arrow_kind(kind: FieldKind) -> ArrowType {
    match kind {
        FieldKind::Unknown | FieldKind::Str => ArrowType::String,
        FieldKind::Bool => ArrowType::Bool,
        FieldKind::I32 => ArrowType::Int32,
        FieldKind::I64 => ArrowType::Int64,
        FieldKind::F64 => ArrowType::Float64,
        FieldKind::Date32 => ArrowType::Date32,
        FieldKind::Time64Micros => ArrowType::Time64Micros,
        FieldKind::TimestampMicros => ArrowType::TimestampMicros,
    }
}

fn core_value(value: &UpdateValue) -> Value<'_> {
    match value {
        UpdateValue::Null => Value::Null,
        UpdateValue::Bool(value) => Value::Bool(*value),
        UpdateValue::I32(value) => Value::Int32(*value),
        UpdateValue::I64(value) => Value::Int64(*value),
        UpdateValue::F64(value) => Value::Float64(*value),
        UpdateValue::Str(value) => Value::String(value),
        UpdateValue::Date32(value) => Value::Date32(*value),
        UpdateValue::Time64Micros(value) => Value::Time64Micros(*value),
        UpdateValue::TimestampMicros(value) => Value::TimestampMicros(*value),
    }
}
