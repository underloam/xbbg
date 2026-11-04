//! Test-support subscription fixture (`fixture.<name>` test calls).
//!
//! Synthetic events enter the production shared feed, decoder and consumer
//! queues through `xbbg_async::testing::SubscriptionFixture`; no Bloomberg
//! session is opened. Fixture subscriptions are ordinary subscription handles
//! (every production call works on them) that keep their fixture alive.
//!
//! | name | sync/async | args | result |
//! |---|---|---|---|
//! | `create` | async | `{streamCapacity?, overflowPolicy?}` | HANDLE fixture (engine-wide defaults) |
//! | `engine` | sync | `{fixture}` | HANDLE engine posing over the fixture (below) |
//! | `subscribe` | async | `{fixture, tickers, fields, options?}` | HANDLE subscription |
//! | `publishJson` | sync | `{fixture, topic, json}` | VOID; a flat JSON object of scalars |
//! | `dataLoss` | sync | `{fixture, topic}` | VOID |
//! | `pauseNextDelivery` | sync | `{fixture, subscription}` | HANDLE pause |
//! | `pauseReady` | async | `{pause}` | VOID once a read is parked |
//! | `pauseRelease` | sync | `{pause}` | VOID; idempotent |
//! | `settle` | async | `{fixture}` | VOID after production cleanup and recovery tasks |
//! | `terminate` | sync | `{fixture}` | VOID; releases every pause, then terminates the sessions |
//! | `release` | sync | `{fixture}` | VOID; drops the handle (subscriptions keep the fixture) |
//!
//! `engine` registers an ordinary engine handle backed by the fixture, so the
//! Java engine lifecycle can be tested without Bloomberg: `keyColumn` and
//! `signalShutdown` work, every other engine method fails INTERNAL. Each later
//! `subscribe` on the fixture holds that engine until the subscription is
//! released, like a Bloomberg engine's subscriptions do; `engineClose`
//! terminates the fixture (as `terminate` does) and `engineRelease` drops the
//! handle only. A fixture has at most one live engine (VALIDATION
//! `subscription fixture already has an engine`).
//!
//! `subscribe` options (napi `SubscriptionFixtureOptions`): `delivery`,
//! `overflowPolicy`, `streamCapacity`, `flushThreshold`, `allFields`, `rows`,
//! `aliases`, `zeroAsNull`. Validation follows napi: `streamCapacity must be
//! positive` (create), `flushThreshold must be positive` (subscribe) and
//! `a delivery pause is already armed`.

use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use serde::Deserialize;
use serde_json::Value;
use xbbg_async::testing::SubscriptionFixture;
use xbbg_async::{
    EngineConfig, KeyColumn, OverflowPolicy, SubscribeRequest, SubscriptionDeliveryMode,
};
use xbbg_host::errors::{HostError, parse_args};
use xbbg_host::subscribe::validate_aliases;
use xbbg_host::subscription::{HostSubscription, PublicationPause};

use crate::engine::{ENGINES, EngineEntry, Engines};
use crate::handles::HandleTable;
use crate::payload::{Payload, ResultKind};
use crate::subscriptions;

struct FixtureEntry {
    fixture: SubscriptionFixture,
    batch_items: usize,
    key_column: KeyColumn,
    /// Pause handles armed through this fixture.
    pauses: Mutex<Vec<u64>>,
    /// The engine posing over this fixture (`engine`); later subscriptions
    /// hold it.
    engine: Mutex<Weak<EngineEntry>>,
}

impl FixtureEntry {
    /// A fixture with `config`'s subscription defaults. Needs a Tokio runtime,
    /// which the fixture's feeds use.
    fn new(config: EngineConfig) -> Self {
        Self {
            batch_items: config.subscription_flush_threshold,
            key_column: config.key_column,
            fixture: SubscriptionFixture::new(config),
            pauses: Mutex::new(Vec::new()),
            engine: Mutex::new(Weak::new()),
        }
    }

    /// Release and forget every pause armed through this fixture, so no read
    /// stays parked.
    fn release_pauses(&self) {
        let pauses =
            std::mem::take(&mut *self.pauses.lock().unwrap_or_else(PoisonError::into_inner));
        for id in pauses {
            if let Some(pause) = PAUSES.remove(id) {
                pause.release();
            }
        }
    }

    /// Release every pause, then terminate every synthetic session.
    fn terminate(&self) {
        self.release_pauses();
        self.fixture.terminate();
    }
}

/// A subscription fixture posing as an engine (`fixture.engine`).
pub(crate) struct FixtureEngine(Arc<FixtureEntry>);

impl FixtureEngine {
    /// The key column of the fixture's configuration.
    pub(crate) fn key_column(&self) -> KeyColumn {
        self.0.key_column
    }

    /// The fixture's counterpart of an engine shutdown: terminating its
    /// sessions ends every subscription on it.
    pub(crate) fn signal_shutdown(&self) {
        self.0.terminate();
    }
}

static FIXTURES: HandleTable<FixtureEntry> = HandleTable::new();
static PAUSES: HandleTable<PublicationPause> = HandleTable::new();

fn fixture(id: u64) -> Result<Arc<FixtureEntry>, HostError> {
    FIXTURES
        .get(id)
        .ok_or_else(|| HostError::state("subscription fixture is closed"))
}

fn pause(id: u64) -> Result<Arc<PublicationPause>, HostError> {
    PAUSES
        .get(id)
        .ok_or_else(|| HostError::state("delivery pause is closed"))
}

fn overflow_policy(value: &str) -> Result<OverflowPolicy, HostError> {
    OverflowPolicy::from_str(value)
        .map_err(|message| HostError::validation(message).with_element("overflowPolicy"))
}

/// Register an engine posing over `entry` in `engines` and return its handle.
fn pose_as_engine(engines: &'static Engines, entry: &Arc<FixtureEntry>) -> Result<u64, HostError> {
    let engine = {
        let mut slot = entry.engine.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.upgrade().is_some() {
            return Err(HostError::validation(
                "subscription fixture already has an engine",
            ));
        }
        let engine = engines.fixture_entry(FixtureEngine(Arc::clone(entry)), entry.batch_items);
        *slot = Arc::downgrade(&engine);
        engine
    };
    engines.admit(engine)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateArgs {
    stream_capacity: Option<u64>,
    overflow_policy: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureOptions {
    delivery: Option<String>,
    overflow_policy: Option<String>,
    stream_capacity: Option<u64>,
    flush_threshold: Option<u64>,
    all_fields: Option<bool>,
    rows: Option<bool>,
    aliases: Option<Value>,
    zero_as_null: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscribeArgs {
    fixture: u64,
    tickers: Vec<String>,
    fields: Vec<String>,
    #[serde(default)]
    options: FixtureOptions,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishArgs {
    fixture: u64,
    topic: String,
    json: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopicArgs {
    fixture: u64,
    topic: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureArgs {
    fixture: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PauseArmArgs {
    fixture: u64,
    subscription: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PauseArgs {
    pause: u64,
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

/// A production subscription on `entry`. It keeps the fixture alive, and the
/// engine posing over it when there is one.
async fn open(
    entry: Arc<FixtureEntry>,
    tickers: Vec<String>,
    fields: Vec<String>,
    options: FixtureOptions,
) -> Result<HostSubscription, HostError> {
    if options.flush_threshold == Some(0) {
        return Err(
            HostError::validation("flushThreshold must be positive").with_element("flushThreshold")
        );
    }
    let delivery = options
        .delivery
        .as_deref()
        .map(SubscriptionDeliveryMode::from_str)
        .transpose()
        .map_err(HostError::from_async)?
        .unwrap_or_default();
    let overflow = options
        .overflow_policy
        .as_deref()
        .map(overflow_policy)
        .transpose()?;
    let batch_items = options.flush_threshold.map_or(entry.batch_items, to_usize);
    let stream = entry
        .fixture
        .subscribe(SubscribeRequest {
            topics: tickers,
            fields,
            delivery,
            all_fields: options.all_fields.unwrap_or(false),
            deliver_rows: options.rows.unwrap_or(true),
            aliases: validate_aliases(options.aliases.as_ref())?,
            zero_as_null: options.zero_as_null.unwrap_or_default(),
            stream_capacity: options.stream_capacity.map(to_usize),
            flush_threshold: options.flush_threshold.map(to_usize),
            overflow_policy: overflow,
            ..SubscribeRequest::default()
        })
        .await
        .map_err(HostError::from_async)?;
    let engine = entry
        .engine
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .upgrade();
    Ok(HostSubscription::from_stream(stream, batch_items, None)
        .keep_alive(Box::new((entry, engine))))
}

/// Sync `fixture.<name>` call (the prefix is stripped).
pub(crate) fn call(name: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    match name {
        "engine" => {
            kind.expect(ResultKind::Handle, name)?;
            let args = parse_args::<FixtureArgs>(args)?;
            let entry = fixture(args.fixture)?;
            Ok(Payload::Handle(pose_as_engine(&ENGINES, &entry)?))
        }
        "publishJson" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<PublishArgs>(args)?;
            fixture(args.fixture)?
                .fixture
                .publish_json(&args.topic, &args.json)
                .map_err(HostError::from_async)?;
            Ok(Payload::Void)
        }
        "dataLoss" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<TopicArgs>(args)?;
            fixture(args.fixture)?
                .fixture
                .data_loss(&args.topic)
                .map_err(HostError::from_async)?;
            Ok(Payload::Void)
        }
        "pauseNextDelivery" => {
            kind.expect(ResultKind::Handle, name)?;
            let args = parse_args::<PauseArmArgs>(args)?;
            let entry = fixture(args.fixture)?;
            let subscription = subscriptions::subscription(args.subscription)?;
            let pause = Arc::new(PublicationPause::default());
            subscription.arm_publication_pause(Arc::clone(&pause))?;
            let id = PAUSES.insert(pause).map_err(|pause| {
                pause.release();
                HostError::state("delivery pause is closed")
            })?;
            entry
                .pauses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(id);
            Ok(Payload::Handle(id))
        }
        "pauseRelease" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<PauseArgs>(args)?;
            if let Some(pause) = PAUSES.remove(args.pause) {
                pause.release();
            }
            Ok(Payload::Void)
        }
        "terminate" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<FixtureArgs>(args)?;
            fixture(args.fixture)?.terminate();
            Ok(Payload::Void)
        }
        "release" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<FixtureArgs>(args)?;
            if let Some(entry) = FIXTURES.remove(args.fixture) {
                entry.release_pauses();
            }
            Ok(Payload::Void)
        }
        _ => Err(HostError::unknown_method("test-support", name)),
    }
}

/// Async `fixture.<name>` call (the prefix is stripped).
pub(crate) async fn call_async(
    name: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    match name {
        "create" => {
            kind.expect(ResultKind::Handle, name)?;
            let args = parse_args::<CreateArgs>(args)?;
            let mut config = EngineConfig::default();
            if let Some(capacity) = args.stream_capacity {
                if capacity == 0 {
                    return Err(HostError::validation("streamCapacity must be positive")
                        .with_element("streamCapacity"));
                }
                config.subscription_stream_capacity = to_usize(capacity);
            }
            if let Some(policy) = args.overflow_policy.as_deref() {
                config.overflow_policy = overflow_policy(policy)?;
            }
            config.validate().map_err(HostError::from_async)?;
            // Runs on the bridge runtime, which the fixture's feeds use.
            let entry = Arc::new(FixtureEntry::new(config));
            let id = FIXTURES
                .insert(entry)
                .map_err(|_| HostError::state("subscription fixture is closed"))?;
            Ok(Payload::Handle(id))
        }
        "subscribe" => {
            kind.expect(ResultKind::Handle, name)?;
            let args = parse_args::<SubscribeArgs>(args)?;
            let entry = fixture(args.fixture)?;
            let subscription = open(entry, args.tickers, args.fields, args.options).await?;
            Ok(Payload::Handle(subscriptions::register(subscription)?))
        }
        "pauseReady" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<PauseArgs>(args)?;
            pause(args.pause)?.ready().await;
            Ok(Payload::Void)
        }
        "settle" => {
            kind.expect(ResultKind::Void, name)?;
            let args = parse_args::<FixtureArgs>(args)?;
            fixture(args.fixture)?.fixture.settle().await;
            Ok(Payload::Void)
        }
        _ => Err(HostError::unknown_method("test-support", name)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use xbbg_host::errors::HostErrorKind;
    use xbbg_host::subscription::ReadOutcome;

    use super::*;
    use crate::engine::dispatch_sync;
    use crate::handles::java_handle;

    const TOPIC: &str = "SYNTHETIC Equity";
    const WAIT: Duration = Duration::from_secs(10);

    fn fixture_entry() -> Arc<FixtureEntry> {
        Arc::new(FixtureEntry::new(EngineConfig::default()))
    }

    async fn subscribe(entry: &Arc<FixtureEntry>) -> Arc<HostSubscription> {
        let subscription = open(
            Arc::clone(entry),
            vec![TOPIC.to_owned()],
            vec!["BID".to_owned()],
            FixtureOptions::default(),
        )
        .await
        .expect("subscribe");
        Arc::new(subscription)
    }

    /// Whether `done` holds within [`WAIT`].
    async fn eventually(done: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + WAIT;
        while !done() {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_released_engine_keeps_its_subscriptions_reading_until_the_last_holder_drops() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let entry = fixture_entry();
        let handle = java_handle(pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect("engine"));
        let engine = ENGINES_UNDER_TEST.entry(handle).expect("registered");
        assert!(matches!(
            dispatch_sync(&engine, "keyColumn", "{}", ResultKind::Str),
            Ok(Payload::Str(column)) if column == "ticker"
        ));
        let Err(unsupported) = dispatch_sync(&engine, "isAvailable", "{}", ResultKind::Bool) else {
            panic!("a fixture engine has no request pool");
        };
        assert_eq!(unsupported.kind, HostErrorKind::Internal);
        drop(engine);
        let subscription = subscribe(&entry).await;

        ENGINES_UNDER_TEST.release(handle);
        let Err(closed) = ENGINES_UNDER_TEST.entry(handle) else {
            panic!("the handle is gone");
        };
        assert_eq!(closed.message, "engine is closed");
        assert_eq!(
            ENGINES_UNDER_TEST.live(),
            1,
            "the subscription holds the engine"
        );
        entry
            .fixture
            .publish_json(TOPIC, r#"{"BID":1.5}"#)
            .expect("publish");
        let read = subscription
            .next_updates(Some(1), Some(WAIT))
            .await
            .expect("read");
        let ReadOutcome::Ready(read) = read else {
            panic!("a released engine keeps its subscriptions running");
        };
        assert_eq!(read.value().updates.len(), 1);
        drop(read);

        drop(subscription);
        drop(entry);
        assert!(
            eventually(|| ENGINES_UNDER_TEST.live() == 0).await,
            "the last holder's drop tears the engine down"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closing_an_engine_ends_its_subscriptions() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let entry = fixture_entry();
        let handle = java_handle(pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect("engine"));
        let subscription = subscribe(&entry).await;
        let reader = Arc::clone(&subscription);
        let pending = tokio::spawn(async move { reader.next_updates(None, None).await });
        tokio::task::yield_now().await;

        ENGINES_UNDER_TEST.close(handle);
        let read = tokio::time::timeout(WAIT, pending)
            .await
            .expect("the pending read ends")
            .expect("join");
        assert!(
            !matches!(read, Ok(ReadOutcome::Ready(_))),
            "a closed engine's subscription delivers nothing more"
        );
        drop(subscription);
        drop(entry);
        assert!(eventually(|| ENGINES_UNDER_TEST.live() == 0).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_engine_registered_after_shutdown_is_shut_down_at_once() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let entry = fixture_entry();
        let subscription = subscribe(&entry).await;
        let reader = Arc::clone(&subscription);
        let pending = tokio::spawn(async move { reader.next_updates(None, None).await });
        tokio::task::yield_now().await;

        ENGINES_UNDER_TEST.close_all();
        let refused = pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect_err("closed table");
        assert_eq!(refused.kind, HostErrorKind::State);
        assert_eq!(refused.message, "engine is closed");
        let read = tokio::time::timeout(WAIT, pending)
            .await
            .expect("the refused engine was shut down")
            .expect("join");
        assert!(!matches!(read, Ok(ReadOutcome::Ready(_))));
        assert!(eventually(|| ENGINES_UNDER_TEST.live() == 0).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_fixture_has_one_live_engine() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let entry = fixture_entry();
        let first = java_handle(pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect("engine"));
        let error = pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect_err("second engine");
        assert_eq!(error.kind, HostErrorKind::Validation);
        assert_eq!(error.message, "subscription fixture already has an engine");
        ENGINES_UNDER_TEST.release(first);
        assert!(eventually(|| ENGINES_UNDER_TEST.live() == 0).await);
        let second = pose_as_engine(&ENGINES_UNDER_TEST, &entry).expect("the first engine is gone");
        ENGINES_UNDER_TEST.release(java_handle(second));
        assert!(eventually(|| ENGINES_UNDER_TEST.live() == 0).await);
    }
}
