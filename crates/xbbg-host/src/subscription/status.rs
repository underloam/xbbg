//! Subscription status snapshots as camelCase JSON objects.
//!
//! The shapes mirror the napi output structs (`SubscriptionStats`,
//! `SubscriptionEventOutput`, `SubscriptionStatusOutput`, ...). Unsigned
//! counters saturate to `i64`; maps are keyed by consumer label (topic states,
//! field errors) or service name and serialize in sorted key order. Optional
//! members are omitted when absent.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use serde::Serialize;
use xbbg_async::engine::{
    AdminStatusInfo, ServiceStatusInfo, SessionStatusInfo, SubscriptionEventInfo,
    SubscriptionFailureInfo, SubscriptionStatusState, TopicStatusInfo,
};

fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// `stats`: totals over every topic's metrics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsOutput {
    pub messages_received: i64,
    pub dropped_batches: i64,
    pub batches_sent: i64,
    pub slow_consumer: bool,
}

/// One status history event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventOutput {
    pub at_us: i64,
    pub category: &'static str,
    pub level: &'static str,
    pub message_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// One failed or terminated topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureOutput {
    pub topic: String,
    pub reason: String,
    pub kind: &'static str,
    pub at_us: i64,
}

/// Lifecycle of one consumer label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStateOutput {
    pub topic: String,
    pub feed_topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delayed: Option<bool>,
    pub state: &'static str,
    pub last_change_us: i64,
    pub streams_active: bool,
    pub streams_changed_us: i64,
}

/// Subscription session state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionOutput {
    pub state: &'static str,
    pub last_change_us: i64,
    pub disconnect_count: i64,
    pub reconnect_count: i64,
}

/// Availability of one Bloomberg service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceOutput {
    pub service: String,
    pub up: bool,
    pub last_change_us: i64,
}

/// Slow-consumer and data-loss admin counters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminOutput {
    pub slow_consumer_warning_active: bool,
    pub slow_consumer_warning_count: i64,
    pub slow_consumer_cleared_count: i64,
    pub data_loss_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warning_us: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_cleared_us: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_data_loss_us: Option<i64>,
}

/// `status`: one consistent snapshot of every diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusOutput {
    pub events: Vec<EventOutput>,
    pub failures: Vec<FailureOutput>,
    pub failed_tickers: Vec<String>,
    pub topic_states: BTreeMap<String, TopicStateOutput>,
    pub field_errors: BTreeMap<String, BTreeMap<String, String>>,
    pub session: SessionOutput,
    pub services: BTreeMap<String, ServiceOutput>,
    pub admin: AdminOutput,
}

impl From<&SubscriptionEventInfo> for EventOutput {
    fn from(event: &SubscriptionEventInfo) -> Self {
        Self {
            at_us: event.at_us,
            category: event.category.as_str(),
            level: event.level.as_str(),
            message_type: event.message_type.clone(),
            topic: event.topic.clone(),
            detail: event.detail.clone(),
        }
    }
}

impl From<&SubscriptionFailureInfo> for FailureOutput {
    fn from(failure: &SubscriptionFailureInfo) -> Self {
        Self {
            topic: failure.topic.clone(),
            reason: failure.reason.clone(),
            kind: failure.kind.as_str(),
            at_us: failure.at_us,
        }
    }
}

impl From<&TopicStatusInfo> for TopicStateOutput {
    fn from(topic: &TopicStatusInfo) -> Self {
        Self {
            topic: topic.topic.clone(),
            feed_topic: topic.feed_topic.clone(),
            delayed: topic.delayed,
            state: topic.state.as_str(),
            last_change_us: topic.last_change_us,
            streams_active: topic.streams_active,
            streams_changed_us: topic.streams_changed_us,
        }
    }
}

impl From<&SessionStatusInfo> for SessionOutput {
    fn from(session: &SessionStatusInfo) -> Self {
        Self {
            state: session.state.as_str(),
            last_change_us: session.last_change_us,
            disconnect_count: saturating(session.disconnect_count),
            reconnect_count: saturating(session.reconnect_count),
        }
    }
}

impl From<&ServiceStatusInfo> for ServiceOutput {
    fn from(service: &ServiceStatusInfo) -> Self {
        Self {
            service: service.service.clone(),
            up: service.up,
            last_change_us: service.last_change_us,
        }
    }
}

impl From<&AdminStatusInfo> for AdminOutput {
    fn from(admin: &AdminStatusInfo) -> Self {
        Self {
            slow_consumer_warning_active: admin.slow_consumer_warning_active,
            slow_consumer_warning_count: saturating(admin.slow_consumer_warning_count),
            slow_consumer_cleared_count: saturating(admin.slow_consumer_cleared_count),
            data_loss_count: saturating(admin.data_loss_count),
            last_warning_us: admin.last_warning_us,
            last_cleared_us: admin.last_cleared_us,
            last_data_loss_us: admin.last_data_loss_us,
        }
    }
}

/// Events in history order.
pub fn events(status: &SubscriptionStatusState) -> Vec<EventOutput> {
    status.events().iter().map(Into::into).collect()
}

/// Failures in occurrence order.
pub fn failures(status: &SubscriptionStatusState) -> Vec<FailureOutput> {
    status.failures().iter().map(Into::into).collect()
}

/// Failed topics, in failure order.
pub fn failed_tickers(status: &SubscriptionStatusState) -> Vec<String> {
    status
        .failures()
        .iter()
        .map(|failure| failure.topic.clone())
        .collect()
}

/// Topic lifecycle by consumer label.
pub fn topic_states(status: &SubscriptionStatusState) -> BTreeMap<String, TopicStateOutput> {
    status
        .topic_statuses()
        .iter()
        .map(|(label, topic)| (label.clone(), topic.into()))
        .collect()
}

/// Rejected fields by consumer label, then field name.
pub fn field_errors(
    status: &SubscriptionStatusState,
) -> BTreeMap<String, BTreeMap<String, String>> {
    status
        .field_errors()
        .iter()
        .map(|(label, fields)| {
            (
                label.clone(),
                fields
                    .iter()
                    .map(|(field, category)| (field.clone(), category.clone()))
                    .collect(),
            )
        })
        .collect()
}

/// Service availability by service name.
pub fn services(status: &SubscriptionStatusState) -> BTreeMap<String, ServiceOutput> {
    status
        .services()
        .iter()
        .map(|(name, service)| (name.clone(), service.into()))
        .collect()
}

/// Totals over every topic's metrics (napi `stats`).
pub fn stats(status: &SubscriptionStatusState) -> StatsOutput {
    let metrics = status.fields_metrics();
    let sum = |field: fn(&xbbg_async::engine::state::SubscriptionMetrics) -> u64| {
        saturating(
            metrics
                .values()
                .fold(0_u64, |total, metric| total.saturating_add(field(metric))),
        )
    };
    StatsOutput {
        messages_received: sum(|metric| metric.messages_received.load(Ordering::Relaxed)),
        dropped_batches: sum(|metric| metric.dropped_batches.load(Ordering::Relaxed)),
        batches_sent: sum(|metric| metric.batches_sent.load(Ordering::Relaxed)),
        slow_consumer: metrics
            .values()
            .any(|metric| metric.slow_consumer.load(Ordering::Relaxed)),
    }
}

/// The combined snapshot.
pub fn status(status: &SubscriptionStatusState) -> StatusOutput {
    StatusOutput {
        events: events(status),
        failures: failures(status),
        failed_tickers: failed_tickers(status),
        topic_states: topic_states(status),
        field_errors: field_errors(status),
        session: status.session().into(),
        services: services(status),
        admin: status.admin().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_saturate_and_absent_options_are_omitted() {
        assert_eq!(saturating(u64::MAX), i64::MAX);
        assert_eq!(saturating(7), 7);
        let admin = AdminOutput::from(&AdminStatusInfo {
            slow_consumer_warning_count: u64::MAX,
            last_cleared_us: Some(5),
            ..AdminStatusInfo::default()
        });
        let json = serde_json::to_value(&admin).expect("admin json");
        assert_eq!(
            json,
            serde_json::json!({
                "slowConsumerWarningActive": false,
                "slowConsumerWarningCount": i64::MAX,
                "slowConsumerClearedCount": 0,
                "dataLossCount": 0,
                "lastClearedUs": 5
            })
        );
    }

    #[test]
    fn an_empty_snapshot_has_every_member() {
        let json =
            serde_json::to_value(status(&SubscriptionStatusState::default())).expect("status json");
        let object = json.as_object().expect("object");
        for key in [
            "events",
            "failures",
            "failedTickers",
            "topicStates",
            "fieldErrors",
            "session",
            "services",
            "admin",
        ] {
            assert!(object.contains_key(key), "missing {key}");
        }
        assert_eq!(json["session"]["state"], "starting");
        assert_eq!(
            stats(&SubscriptionStatusState::default()),
            StatsOutput {
                messages_received: 0,
                dropped_batches: 0,
                batches_sent: 0,
                slow_consumer: false,
            }
        );
    }
}
