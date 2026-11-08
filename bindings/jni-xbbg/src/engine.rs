//! Engines, their handles, lifecycle and the engine method router.
//!
//! `connect` parses the host `EngineConfig` JSON ([`EngineConfigSpec`]),
//! starts the engine on the bridge runtime's blocking pool and completes with
//! its handle. `engineClose` signals shutdown and releases the table's
//! reference; `engineRelease` (the Java cleaner's path) only releases it, so
//! the engine keeps serving its subscriptions and in-flight calls. Either way
//! the engine is torn down on an `xbbg-teardown` thread once its last holder
//! (the table, an in-flight call or a subscription) lets go.
//! `shutdown(timeout)` closes every engine and subscription, refuses new ones
//! and waits for teardown.
//!
//! `engineCall` / `engineCallSync` dispatch by JS method name: the core
//! methods below, then the area modules (`requests`, `recipes`,
//! `subscriptions`), each owning its `ASYNC` / `SYNC` name lists. Arguments are
//! one JSON object; every core method rejects unknown members.
//!
//! Core methods (`args` → result kind):
//!
//! | method | args | result |
//! |---|---|---|
//! | `request` | the `RequestInput` object | TABLE / IPC_STREAM / IPC_FILE |
//! | `seatType` | `{}` | STRING `BPS` / `NONBPS` / `INVALID` |
//! | `checkEntitlements` | `{service, eids: int[]}` | JSON `{"entitled": bool, "failedEids": int[]}` |
//! | `identityIsAuthorized` | `{service}` | BOOL |
//! | `resolveFieldTypes` | `{fields, overrides?: {field: type}, defaultType?}` (default `"string"`) | JSON `{field: arrowType}`, keys sorted |
//! | `validateFields` | `{fields}` | JSON string array (invalid names) |
//! | `getSchema` | `{service}` | JSON (service schema) |
//! | `getOperation` | `{service, operation}` | JSON (operation schema) |
//! | `listOperations` | `{service}` | JSON string array |
//! | `getEnumValues` | `{service, operation, element}` | JSON string array, or null |
//! | `listValidElements` | `{service, operation}` | JSON string array, or null |
//! | `getFieldInfo` (sync) | `{field}` | JSON `{"fieldId", "arrowType", "description", "category"}`, or null |
//! | `clearFieldCache` (sync) | `{}` | VOID |
//! | `saveFieldCache` (sync) | `{}` | VOID |
//! | `isFieldValidationEnabled` (sync) | `{}` | BOOL |
//! | `getCachedSchema` (sync) | `{service}` | JSON, or null |
//! | `invalidateSchema` (sync) | `{service}` | VOID |
//! | `clearSchemaCache` (sync) | `{}` | VOID |
//! | `listCachedSchemas` (sync) | `{}` | JSON string array |
//! | `subscriptionFeeds` (sync) | `{}` | JSON array of `{service, topic, options, fields, consumers, delayed?, state, isolated, fieldErrors}` |
//! | `signalShutdown` (sync) | `{}` | VOID |
//! | `isAvailable` (sync) | `{}` | BOOL |
//! | `keyColumn` (sync) | `{}` | STRING `ticker` / `security` |
//!
//! Field-cache and schema-cache failures are INTERNAL; engine failures keep
//! their engine classification.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use xbbg_async::KeyColumn;
use xbbg_async::engine::{Engine, RequestParams, WorkerHealth};
use xbbg_host::config::EngineConfigSpec;
use xbbg_host::errors::{HostError, parse_args};
use xbbg_host::request::RequestInput;
use xbbg_host::schema::{self, SchemaJsonCache};
use xbbg_host::teardown::{DeferredDrop, wait_idle_timeout};

use crate::handles::{HandleTable, handle_id};
use crate::payload::{Payload, ResultKind};
#[cfg(feature = "test-support")]
use crate::subscription_fixture::FixtureEngine;
use crate::{recipes, requests, subscriptions};

/// An engine and its per-engine host state.
pub(crate) struct EngineEntry {
    backend: Backend,
    schema: SchemaJsonCache,
    subscription_batch_items: usize,
}

/// What a handle drives. Dropping the last `Arc<EngineEntry>` moves the
/// backend to an `xbbg-teardown` thread; it counts as live until torn down.
#[allow(
    clippy::large_enum_variant,
    reason = "one per engine, behind an Arc; the small variant exists in test-support builds only"
)]
enum Backend {
    Bloomberg(DeferredDrop<Live<Engine>>),
    /// A subscription fixture posing as an engine (`fixture.engine`).
    #[cfg(feature = "test-support")]
    Fixture(DeferredDrop<Live<FixtureEngine>>),
}

impl EngineEntry {
    fn new(backend: Backend, subscription_batch_items: usize) -> Self {
        Self {
            backend,
            schema: SchemaJsonCache::new(),
            subscription_batch_items,
        }
    }

    /// The Bloomberg engine; hold the `Arc<EngineEntry>` to keep it alive.
    /// A fixture engine has none (INTERNAL).
    pub(crate) fn engine(&self) -> Result<&Engine, HostError> {
        match &self.backend {
            Backend::Bloomberg(engine) => Ok(&engine.value),
            #[cfg(feature = "test-support")]
            Backend::Fixture(_) => Err(HostError::internal(
                "a subscription fixture engine only supports keyColumn and signalShutdown",
            )),
        }
    }

    /// Name of the result column holding each row's security.
    fn key_column(&self) -> KeyColumn {
        match &self.backend {
            Backend::Bloomberg(engine) => engine.value.key_column(),
            #[cfg(feature = "test-support")]
            Backend::Fixture(fixture) => fixture.value.key_column(),
        }
    }

    /// End the engine's subscriptions and refuse new work; the engine itself
    /// stays until its last holder lets go.
    fn signal_shutdown(&self) {
        match &self.backend {
            Backend::Bloomberg(engine) => engine.value.signal_shutdown(),
            #[cfg(feature = "test-support")]
            Backend::Fixture(fixture) => fixture.value.signal_shutdown(),
        }
    }

    /// Default rows per subscription read (`subscriptionFlushThreshold` at connect).
    pub(crate) fn subscription_batch_items(&self) -> usize {
        self.subscription_batch_items
    }
}

/// A backend counted live until it has been dropped. Fields drop in
/// declaration order, so the count falls only once `value` is gone (on its
/// teardown thread): `shutdown` never misses a teardown that has yet to start.
struct Live<T> {
    value: T,
    _count: LiveCount,
}

/// One unit of an [`Engines`] live count, given back on drop.
struct LiveCount(&'static AtomicUsize);

impl Drop for LiveCount {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Engine handles plus what `shutdown` needs: the number of engines not yet
/// torn down and, in the table, the latch that refuses engines once it began.
pub(crate) struct Engines {
    handles: HandleTable<EngineEntry>,
    live: AtomicUsize,
}

/// Every engine of this process.
pub(crate) static ENGINES: Engines = Engines::new();

impl Engines {
    pub(crate) const fn new() -> Self {
        Self {
            handles: HandleTable::new(),
            live: AtomicUsize::new(0),
        }
    }

    /// Engines not yet torn down: registered, held by a call or a
    /// subscription, or still stopping on a teardown thread.
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// `value`, counted live until it has been dropped.
    fn counted<T>(&'static self, value: T) -> Live<T> {
        self.live.fetch_add(1, Ordering::SeqCst);
        Live {
            value,
            _count: LiveCount(&self.live),
        }
    }

    /// The live engine for a Java handle; STATE `engine is closed` otherwise.
    pub(crate) fn entry(&self, handle: i64) -> Result<Arc<EngineEntry>, HostError> {
        self.handles
            .get(handle_id(handle))
            .ok_or_else(HostError::engine_closed)
    }

    /// Register an engine and return its handle. Once shutdown has begun the
    /// table refuses it: the engine is shut down at once and the caller gets
    /// STATE `engine is closed`.
    pub(crate) fn admit(&self, entry: Arc<EngineEntry>) -> Result<u64, HostError> {
        self.handles.insert(entry).map_err(|refused| {
            refused.signal_shutdown();
            HostError::engine_closed()
        })
    }

    /// Start an engine from the host `EngineConfig` JSON; completes with its handle.
    pub(crate) async fn connect(&'static self, config_json: String) -> Result<Payload, HostError> {
        let config = EngineConfigSpec::from_json(&config_json)?.into_engine_config()?;
        if self.handles.is_closed() {
            return Err(HostError::engine_closed());
        }
        let batch_items = config.subscription_flush_threshold;
        // Engine::start blocks for the session connect; a dropped (cancelled)
        // result still tears down off the blocking thread. The error is
        // classified there too, so only a small result crosses back.
        let engine = tokio::task::spawn_blocking(move || {
            Engine::start(config)
                .map(|engine| DeferredDrop::new(self.counted(engine)))
                .map_err(HostError::from_async)
        })
        .await
        .map_err(|error| {
            if error.is_panic() {
                HostError::native_panic(&*error.into_panic())
            } else {
                HostError::internal(format!("engine startup task failed: {error}"))
            }
        })??;
        let entry = EngineEntry::new(Backend::Bloomberg(engine), batch_items);
        self.admit(Arc::new(entry)).map(Payload::Handle)
    }

    /// `engineClose`: signal shutdown and drop the table's reference. Idempotent.
    pub(crate) fn close(&self, handle: i64) {
        if let Some(entry) = self.handles.remove(handle_id(handle)) {
            entry.signal_shutdown();
        }
    }

    /// `engineRelease`: drop the table's reference only; subscriptions and
    /// in-flight calls keep the engine running. Idempotent.
    pub(crate) fn release(&self, handle: i64) {
        drop(self.handles.remove(handle_id(handle)));
    }

    /// Shut every engine down and refuse new ones.
    pub(crate) fn close_all(&self) {
        for entry in self.handles.close() {
            entry.signal_shutdown();
        }
    }

    /// Wait until every engine has been torn down and no deferred teardown is
    /// pending, or until `deadline`. Returns whether everything finished.
    fn wait_torn_down(&self, deadline: Instant) -> bool {
        while self.live() > 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        wait_idle_timeout(deadline.saturating_duration_since(Instant::now()))
    }
}

#[cfg(feature = "test-support")]
impl Engines {
    /// An engine entry over a subscription fixture, counted and torn down like
    /// a Bloomberg engine (`fixture.engine`); register it with [`Engines::admit`].
    pub(crate) fn fixture_entry(
        &'static self,
        fixture: FixtureEngine,
        subscription_batch_items: usize,
    ) -> Arc<EngineEntry> {
        let backend = Backend::Fixture(DeferredDrop::new(self.counted(fixture)));
        Arc::new(EngineEntry::new(backend, subscription_batch_items))
    }
}

/// The live engine for a Java handle; STATE `engine is closed` otherwise.
pub(crate) fn engine_entry(handle: i64) -> Result<Arc<EngineEntry>, HostError> {
    ENGINES.entry(handle)
}

/// `connect(configJson)`.
pub(crate) async fn connect(config_json: String) -> Result<Payload, HostError> {
    ENGINES.connect(config_json).await
}

/// `engineClose(engine)`.
pub(crate) fn close(handle: i64) {
    ENGINES.close(handle);
}

/// `engineRelease(engine)`.
pub(crate) fn release(handle: i64) {
    ENGINES.release(handle);
}

/// Close every engine and subscription, refuse new ones and wait up to
/// `timeout` for engine teardown. Returns whether everything finished in time.
pub(crate) fn shutdown(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    ENGINES.close_all();
    subscriptions::release_all();
    ENGINES.wait_torn_down(deadline)
}

const CORE_ASYNC: &[&str] = &[
    "request",
    "seatType",
    "checkEntitlements",
    "identityIsAuthorized",
    "resolveFieldTypes",
    "validateFields",
    "getSchema",
    "getOperation",
    "listOperations",
    "getEnumValues",
    "listValidElements",
];

const CORE_SYNC: &[&str] = &[
    "getFieldInfo",
    "clearFieldCache",
    "saveFieldCache",
    "isFieldValidationEnabled",
    "getCachedSchema",
    "invalidateSchema",
    "clearSchemaCache",
    "listCachedSchemas",
    "subscriptionFeeds",
    "signalShutdown",
    "isAvailable",
    "keyColumn",
];

/// Where a method name dispatches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Area {
    Core,
    Requests,
    Recipes,
    Subscriptions,
}

fn async_area(method: &str) -> Option<Area> {
    [
        (CORE_ASYNC, Area::Core),
        (requests::ASYNC, Area::Requests),
        (recipes::ASYNC, Area::Recipes),
        (subscriptions::ASYNC, Area::Subscriptions),
    ]
    .into_iter()
    .find_map(|(names, area)| names.contains(&method).then_some(area))
}

fn sync_area(method: &str) -> Option<Area> {
    [
        (CORE_SYNC, Area::Core),
        (requests::SYNC, Area::Requests),
        (recipes::SYNC, Area::Recipes),
        (subscriptions::SYNC, Area::Subscriptions),
    ]
    .into_iter()
    .find_map(|(names, area)| names.contains(&method).then_some(area))
}

fn unknown(method: &str, other_side: Option<Area>, use_instead: &str) -> HostError {
    match other_side {
        Some(_) => HostError::internal(format!(
            "engine method '{method}' must be called with {use_instead}"
        )),
        None => HostError::unknown_method("engine", method),
    }
}

/// Route an async engine call.
pub(crate) async fn dispatch(
    entry: Arc<EngineEntry>,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    match async_area(method) {
        Some(Area::Core) => core_call(&entry, method, args, kind).await,
        Some(Area::Requests) => requests::call(entry, method, args, kind).await,
        Some(Area::Recipes) => recipes::call(entry, method, args, kind).await,
        Some(Area::Subscriptions) => subscriptions::call(entry, method, args, kind).await,
        None => Err(unknown(method, sync_area(method), "engineCallSync")),
    }
}

/// Route a sync engine call.
pub(crate) fn dispatch_sync(
    entry: &EngineEntry,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    match sync_area(method) {
        Some(Area::Core) => core_call_sync(entry, method, args, kind),
        Some(Area::Requests) => requests::call_sync(entry, method, args, kind),
        Some(Area::Recipes) => recipes::call_sync(entry, method, args, kind),
        Some(Area::Subscriptions) => subscriptions::call_sync(entry, method, args, kind),
        None => Err(unknown(method, async_area(method), "engineCall")),
    }
}

/// Run a planned request and shape the result for `kind` (shared with the
/// typed request wrappers so they behave exactly like `request`).
pub(crate) async fn run_request(
    entry: &Arc<EngineEntry>,
    params: RequestParams,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let batch = entry
        .engine()?
        .request(params)
        .await
        .map_err(HostError::from_async)?;
    Payload::table(batch, kind)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceArgs {
    service: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OperationArgs {
    service: String,
    operation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ElementArgs {
    service: String,
    operation: String,
    element: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntitlementArgs {
    service: String,
    eids: Vec<i32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FieldTypesArgs {
    fields: Vec<String>,
    overrides: Option<HashMap<String, String>>,
    default_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FieldsArgs {
    fields: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FieldArgs {
    field: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EntitlementReport {
    entitled: bool,
    failed_eids: Vec<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FieldInfoOutput<'a> {
    field_id: &'a str,
    arrow_type: &'a str,
    description: &'a str,
    category: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeedInfoOutput<'a> {
    service: &'a str,
    topic: &'a str,
    options: &'a [String],
    fields: &'a [String],
    consumers: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    delayed: Option<bool>,
    state: &'a str,
    isolated: bool,
    field_errors: BTreeMap<&'a str, &'a str>,
}

/// A JSON text result, or `null` when absent.
fn json_text(text: Option<Arc<str>>) -> Payload {
    text.map_or(Payload::Void, |text| Payload::Json(text.to_string()))
}

async fn core_call(
    entry: &Arc<EngineEntry>,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let engine = entry.engine()?;
    match method {
        "request" => {
            kind.expect_table(method)?;
            let params = RequestInput::from_json(args)?.into_request_params()?;
            run_request(entry, params, kind).await
        }
        "seatType" => {
            kind.expect(ResultKind::Str, method)?;
            parse_args::<NoArgs>(args)?;
            let seat = engine.seat_type().await.map_err(HostError::from_async)?;
            Ok(Payload::Str(seat.as_str().to_owned()))
        }
        "checkEntitlements" => {
            kind.expect(ResultKind::Json, method)?;
            let args: EntitlementArgs = parse_args(args)?;
            let report = engine
                .check_entitlements(&args.service, &args.eids)
                .await
                .map_err(HostError::from_async)?;
            Payload::json(&EntitlementReport {
                entitled: report.entitled,
                failed_eids: report.failed_eids,
            })
        }
        "identityIsAuthorized" => {
            kind.expect(ResultKind::Bool, method)?;
            let args: ServiceArgs = parse_args(args)?;
            let authorized = engine
                .identity_is_authorized(&args.service)
                .await
                .map_err(HostError::from_async)?;
            Ok(Payload::Bool(authorized))
        }
        "resolveFieldTypes" => {
            kind.expect(ResultKind::Json, method)?;
            let args: FieldTypesArgs = parse_args(args)?;
            let types = engine
                .resolve_field_types(
                    &args.fields,
                    args.overrides.as_ref(),
                    args.default_type.as_deref().unwrap_or("string"),
                )
                .await
                .map_err(HostError::from_async)?;
            Payload::json(&types.into_iter().collect::<BTreeMap<_, _>>())
        }
        "validateFields" => {
            kind.expect(ResultKind::Json, method)?;
            let args: FieldsArgs = parse_args(args)?;
            let invalid = engine
                .validate_fields(&args.fields)
                .await
                .map_err(HostError::from_async)?;
            Payload::json(&invalid)
        }
        "getSchema" => {
            kind.expect(ResultKind::Json, method)?;
            let args: ServiceArgs = parse_args(args)?;
            let json = entry.schema.schema(engine, &args.service).await?;
            Ok(json_text(Some(json)))
        }
        "getOperation" => {
            kind.expect(ResultKind::Json, method)?;
            let args: OperationArgs = parse_args(args)?;
            let json = entry
                .schema
                .operation(engine, &args.service, &args.operation)
                .await?;
            Ok(json_text(Some(json)))
        }
        "listOperations" => {
            kind.expect(ResultKind::Json, method)?;
            let args: ServiceArgs = parse_args(args)?;
            Payload::json(&schema::list_operations(engine, &args.service).await?)
        }
        "getEnumValues" => {
            kind.expect(ResultKind::Json, method)?;
            let args: ElementArgs = parse_args(args)?;
            let values =
                schema::enum_values(engine, &args.service, &args.operation, &args.element).await?;
            values.map_or(Ok(Payload::Void), |values| Payload::json(&values))
        }
        "listValidElements" => {
            kind.expect(ResultKind::Json, method)?;
            let args: OperationArgs = parse_args(args)?;
            let elements = schema::valid_elements(engine, &args.service, &args.operation).await?;
            elements.map_or(Ok(Payload::Void), |elements| Payload::json(&elements))
        }
        other => Err(HostError::unknown_method("engine", other)),
    }
}

fn core_call_sync(
    entry: &EngineEntry,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    // The lifecycle methods also work on a test fixture engine.
    match method {
        "signalShutdown" => {
            kind.expect(ResultKind::Void, method)?;
            parse_args::<NoArgs>(args)?;
            entry.signal_shutdown();
            return Ok(Payload::Void);
        }
        "keyColumn" => {
            kind.expect(ResultKind::Str, method)?;
            parse_args::<NoArgs>(args)?;
            return Ok(Payload::Str(entry.key_column().as_str().to_owned()));
        }
        _ => {}
    }
    let engine = entry.engine()?;
    match method {
        "getFieldInfo" => {
            kind.expect(ResultKind::Json, method)?;
            let args: FieldArgs = parse_args(args)?;
            match engine.get_field_info(&args.field) {
                Some(info) => Payload::json(&FieldInfoOutput {
                    field_id: &info.field_id,
                    arrow_type: &info.arrow_type,
                    description: &info.description,
                    category: &info.category,
                }),
                None => Ok(Payload::Void),
            }
        }
        "clearFieldCache" => {
            kind.expect(ResultKind::Void, method)?;
            parse_args::<NoArgs>(args)?;
            engine.clear_field_cache().map_err(HostError::internal)?;
            Ok(Payload::Void)
        }
        "saveFieldCache" => {
            kind.expect(ResultKind::Void, method)?;
            parse_args::<NoArgs>(args)?;
            engine.save_field_cache().map_err(HostError::internal)?;
            Ok(Payload::Void)
        }
        "isFieldValidationEnabled" => {
            kind.expect(ResultKind::Bool, method)?;
            parse_args::<NoArgs>(args)?;
            Ok(Payload::Bool(engine.is_field_validation_enabled()))
        }
        "getCachedSchema" => {
            kind.expect(ResultKind::Json, method)?;
            let args: ServiceArgs = parse_args(args)?;
            Ok(json_text(entry.schema.cached_schema(engine, &args.service)))
        }
        "invalidateSchema" => {
            kind.expect(ResultKind::Void, method)?;
            let args: ServiceArgs = parse_args(args)?;
            entry.schema.invalidate(engine, &args.service)?;
            Ok(Payload::Void)
        }
        "clearSchemaCache" => {
            kind.expect(ResultKind::Void, method)?;
            parse_args::<NoArgs>(args)?;
            entry.schema.clear(engine)?;
            Ok(Payload::Void)
        }
        "listCachedSchemas" => {
            kind.expect(ResultKind::Json, method)?;
            parse_args::<NoArgs>(args)?;
            Payload::json(&schema::list_cached_schemas(engine))
        }
        "subscriptionFeeds" => {
            kind.expect(ResultKind::Json, method)?;
            parse_args::<NoArgs>(args)?;
            let feeds = engine.subscription_feeds();
            let output: Vec<FeedInfoOutput<'_>> = feeds
                .iter()
                .map(|feed| FeedInfoOutput {
                    service: &feed.service,
                    topic: &feed.topic,
                    options: &feed.options,
                    fields: &feed.fields,
                    consumers: feed.consumers,
                    delayed: feed.delayed,
                    state: &feed.state,
                    isolated: feed.isolated,
                    field_errors: feed
                        .field_errors
                        .iter()
                        .map(|(field, category)| (field.as_str(), category.as_str()))
                        .collect(),
                })
                .collect();
            Payload::json(&output)
        }
        "isAvailable" => {
            kind.expect(ResultKind::Bool, method)?;
            parse_args::<NoArgs>(args)?;
            Ok(Payload::Bool(
                engine
                    .request_pool_health()
                    .iter()
                    .any(|(_, health)| *health == WorkerHealth::Healthy),
            ))
        }
        other => Err(HostError::unknown_method("engine", other)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    /// Port 1 on loopback refuses at once; no Bloomberg process is involved.
    const UNREACHABLE: &str = r#"{"host":"127.0.0.1","port":1,"numStartAttempts":1,
        "autoRestartOnDisconnection":false,"requestPoolSize":1,"subscriptionPoolSize":0}"#;

    #[test]
    fn connecting_to_an_unreachable_server_fails_with_session() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let started = Instant::now();
        let error = crate::bridge::runtime()
            .block_on(ENGINES_UNDER_TEST.connect(UNREACHABLE.to_owned()))
            .expect_err("nothing listens on port 1");
        assert_eq!(
            error.kind,
            xbbg_host::errors::HostErrorKind::Session,
            "{error:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(60));
        assert_eq!(ENGINES_UNDER_TEST.live(), 0);
    }

    #[test]
    fn connecting_after_shutdown_is_refused_without_a_session() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        ENGINES_UNDER_TEST.close_all();
        let error = crate::bridge::runtime()
            .block_on(ENGINES_UNDER_TEST.connect(UNREACHABLE.to_owned()))
            .expect_err("closed");
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::State);
        assert_eq!(error.message, "engine is closed");
        assert_eq!(ENGINES_UNDER_TEST.live(), 0, "no engine was started");
    }

    /// Blocks its drop until released, reporting when the drop began.
    struct Gate {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Drop for Gate {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    #[test]
    fn shutdown_waits_for_a_teardown_that_runs_after_the_last_holder_let_go() {
        static ENGINES_UNDER_TEST: Engines = Engines::new();
        let (entered_tx, entered) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let backend = DeferredDrop::new(ENGINES_UNDER_TEST.counted(Gate {
            entered: entered_tx,
            release: release_rx,
        }));
        assert_eq!(ENGINES_UNDER_TEST.live(), 1);

        // The last holder lets go: teardown moves to an xbbg-teardown thread.
        drop(backend);
        entered
            .recv_timeout(Duration::from_secs(10))
            .expect("teardown started");
        assert_eq!(
            ENGINES_UNDER_TEST.live(),
            1,
            "an engine counts as live until its teardown has finished"
        );
        assert!(
            !ENGINES_UNDER_TEST.wait_torn_down(Instant::now() + Duration::from_millis(50)),
            "shutdown keeps waiting while the teardown runs"
        );

        release.send(()).expect("teardown thread waits");
        assert!(ENGINES_UNDER_TEST.wait_torn_down(Instant::now() + Duration::from_secs(10)));
        assert_eq!(ENGINES_UNDER_TEST.live(), 0);
    }

    #[test]
    fn invalid_config_fails_before_any_session_starts() {
        let runtime = crate::bridge::runtime();
        let error = runtime
            .block_on(connect(r#"{"requestPoolSize":0}"#.to_owned()))
            .expect_err("rejected");
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::Validation);
        assert_eq!(error.message, "requestPoolSize must be greater than zero");
        let error = runtime
            .block_on(connect(r#"{"hots":"x"}"#.to_owned()))
            .expect_err("rejected");
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::Argument);
    }

    #[test]
    fn method_lists_are_disjoint() {
        let lists = [
            CORE_ASYNC,
            CORE_SYNC,
            requests::ASYNC,
            requests::SYNC,
            recipes::ASYNC,
            recipes::SYNC,
            subscriptions::ASYNC,
            subscriptions::SYNC,
        ];
        let mut seen = std::collections::HashSet::new();
        for name in lists.into_iter().flatten() {
            assert!(seen.insert(*name), "'{name}' is dispatched twice");
        }
    }

    #[test]
    fn routing_distinguishes_unknown_and_wrong_side_calls() {
        assert_eq!(async_area("request"), Some(Area::Core));
        assert_eq!(sync_area("keyColumn"), Some(Area::Core));
        assert_eq!(async_area("keyColumn"), None);

        let wrong = unknown("keyColumn", sync_area("keyColumn"), "engineCallSync");
        assert_eq!(
            wrong.message,
            "engine method 'keyColumn' must be called with engineCallSync"
        );
        let missing = unknown("nope", sync_area("nope"), "engineCallSync");
        assert_eq!(missing.message, "unknown engine method 'nope'");
        assert_eq!(missing.kind, xbbg_host::errors::HostErrorKind::Internal);
    }

    #[test]
    fn closed_and_unknown_handles_are_state_errors() {
        for handle in [0, -5, 987_654] {
            let Err(error) = engine_entry(handle) else {
                panic!("no engine expected for {handle}");
            };
            assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::State);
            assert_eq!(error.message, "engine is closed");
        }
        close(987_654);
    }

    #[test]
    fn core_argument_objects_reject_unknown_members() {
        let error = parse_args::<NoArgs>(r#"{"x":1}"#).err().unwrap();
        assert_eq!(error.message, "unknown field 'x'");
        let error = parse_args::<EntitlementArgs>(r#"{"service":"//blp/mktdata","eids":[1.5]}"#)
            .err()
            .unwrap();
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::Argument);
        let args: FieldTypesArgs =
            parse_args(r#"{"fields":["PX_LAST"],"overrides":{"PX_LAST":"float64"}}"#).unwrap();
        assert_eq!(args.overrides.unwrap()["PX_LAST"], "float64");
        assert_eq!(args.default_type, None);
    }

    #[test]
    fn feed_output_omits_unknown_delay_and_sorts_field_errors() {
        let field_errors = BTreeMap::from([("B", "x"), ("A", "y")]);
        let options = ["interval=1".to_string()];
        let fields = ["LAST_PRICE".to_string()];
        let json = serde_json::to_string(&FeedInfoOutput {
            service: "//blp/mktdata",
            topic: "IBM US Equity",
            options: &options,
            fields: &fields,
            consumers: 2,
            delayed: None,
            state: "streaming",
            isolated: false,
            field_errors,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"service":"//blp/mktdata","topic":"IBM US Equity","options":["interval=1"],"fields":["LAST_PRICE"],"consumers":2,"state":"streaming","isolated":false,"fieldErrors":{"A":"y","B":"x"}}"#
        );
    }
}
