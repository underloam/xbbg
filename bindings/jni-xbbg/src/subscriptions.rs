//! Subscriptions: the engine methods that open them and the calls on a
//! subscription handle (`NativeXbbg.subscriptionCall` / `subscriptionCallSync`).
//!
//! A subscription handle owns one [`HostSubscription`], which keeps its engine
//! (or test fixture) alive until the handle is released. Releasing the handle
//! signals close, so reads still pending on it end.
//!
//! # Engine methods (async, HANDLE)
//!
//! | method | args |
//! |---|---|
//! | `subscribe` | [`xbbg_host::subscribe::SubscribeArgs`] |
//! | `subscribeAuction` | [`xbbg_host::preflight::AuctionSubscribeArgs`] |
//! | `subscribeEtfInav` | [`xbbg_host::preflight::EtfInavSubscribeArgs`] |
//!
//! # Async subscription calls
//!
//! | method | args | kind and result |
//! |---|---|---|
//! | `nextTicks` | `{maxItems?, maxWaitMs?}` | TICKS: a `TickBatch`, or `null` (ended, or the wait passed) |
//! | `nextArrow` | `{maxRows?, maxWaitMs?}` | TABLE / IPC_*: one batch, or `null` |
//! | `nextDelivery` | `{maxRows?, maxWaitMs?, format?}` | DELIVERY: a `NativeDelivery` (data with a receipt, an error receipt, or a retryable conversion failure), or `null` |
//! | `unsubscribeTicks` | `{drain?}` | TICKS_LIST: `Object[]` of `TickBatch` |
//! | `unsubscribeArrow` | `{drain?}` | TABLE_LIST |
//! | `unsubscribeDeliveries` | `{drain?, format?}` | DELIVERY_LIST: a `NativeDrain` (deliveries with one receipt, or a retryable conversion failure) |
//! | `add` | `{tickers, aliases?}` | VOID |
//! | `addFields` | `{fields}` | VOID |
//! | `remove` | `{tickers}` | VOID (labels) |
//!
//! `format` is `table` (default), `ipcStream` or `ipcFile` and selects the
//! delivery data representation. A delivery receipt is settled with
//! `commitDelivery` / `rollbackDelivery`, a drain receipt with `commitDrain` /
//! `rollbackDrain`; until then the source stays restorable.
//!
//! # Sync subscription calls
//!
//! | method | args | kind and result |
//! |---|---|---|
//! | `info` | `{}` | JSON `{deliversRows, requiresDeliveries, delivery, batchItems}` |
//! | `signalClosing` | `{}` | VOID |
//! | `commitDelivery` | `{receipt}` | BOOL (`false`: close won); an error receipt throws its terminal error once |
//! | `rollbackDelivery` | `{receipt}` | VOID |
//! | `commitDrain` | `{receipt}` | BOOL (`false`: already settled) |
//! | `rollbackDrain` | `{receipt}` | VOID |
//! | `latest` | `{}` | TABLE / IPC_* |
//! | `takeWarnings` | `{}` | JSON array of events |
//! | `tickers`, `fields`, `failedTickers` | `{}` | JSON string array |
//! | `isActive` | `{}` | BOOL |
//! | `stats`, `status`, `events`, `failures`, `topicStates`, `fieldErrors`, `sessionStatus`, `serviceStatus`, `adminStatus` | `{}` | JSON (see `xbbg_host::subscription::status`) |
//!
//! A released or unknown handle is STATE `subscription is closed`.

use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use jni::errors::Error as JniError;
use jni::ids::JStaticMethodID;
use jni::objects::{
    Global, JByteArray, JCharArray, JClass, JIntArray, JLongArray, JObject, JObjectArray, JString,
};
use jni::refs::{IntoAuto, LoaderContext, Reference};
use jni::signature::JavaType;
use jni::sys::jvalue;
use jni::{Env, jni_sig, jni_str};
use serde::Deserialize;
use serde_json::Value;
use xbbg_async::SubscriptionDeliveryMode;
use xbbg_async::engine::state::{FieldKind, UpdateValue};
use xbbg_host::errors::{HostError, parse_args};
use xbbg_host::preflight::{
    AuctionSubscribeArgs, EtfInavSubscribeArgs, prepare_auction_subscribe,
    prepare_etf_inav_subscribe,
};
use xbbg_host::subscribe::{PreparedSubscribe, SubscribeArgs, prepare_subscribe, validate_aliases};
use xbbg_host::subscription::{
    DeliveryRead, GuardedRead, HostSubscription, PreparedDelivery, PreparedDrain, ReadOutcome,
    ScalarBatch, status,
};

use crate::bridge::{NativeError, java, throwable};
use crate::engine::EngineEntry;
use crate::handles::HandleTable;
use crate::payload::{JavaPayload, Payload, ResultKind, ipc_file, ipc_stream, jsize};
use crate::table;

/// Async engine methods dispatched here.
pub(crate) const ASYNC: &[&str] = &["subscribe", "subscribeAuction", "subscribeEtfInav"];
/// Sync engine methods dispatched here.
pub(crate) const SYNC: &[&str] = &[];

/// Live subscriptions by handle.
static SUBSCRIPTIONS: HandleTable<HostSubscription> = HandleTable::new();

/// Register a new subscription and return its handle. Once shutdown has
/// begun the table refuses it: it is closed at once and the caller gets STATE
/// `subscription is closed`.
pub(crate) fn register(subscription: HostSubscription) -> Result<u64, HostError> {
    admit(&SUBSCRIPTIONS, subscription)
}

fn admit(
    table: &HandleTable<HostSubscription>,
    subscription: HostSubscription,
) -> Result<u64, HostError> {
    table.insert(Arc::new(subscription)).map_err(|refused| {
        refused.signal_closing();
        HostError::state("subscription is closed")
    })
}

/// The live subscription for a handle; STATE `subscription is closed` otherwise.
pub(crate) fn subscription(id: u64) -> Result<Arc<HostSubscription>, HostError> {
    SUBSCRIPTIONS
        .get(id)
        .ok_or_else(|| HostError::state("subscription is closed"))
}

pub(crate) async fn call(
    engine: Arc<EngineEntry>,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let batch_items = engine.subscription_batch_items();
    let bloomberg = engine.engine()?;
    let prepared: PreparedSubscribe = match method {
        "subscribe" => {
            kind.expect(ResultKind::Handle, method)?;
            prepare_subscribe(parse_args::<SubscribeArgs>(args)?, batch_items)?
        }
        "subscribeAuction" => {
            kind.expect(ResultKind::Handle, method)?;
            let args = parse_args::<AuctionSubscribeArgs>(args)?;
            prepare_auction_subscribe(bloomberg, args, batch_items).await?
        }
        "subscribeEtfInav" => {
            kind.expect(ResultKind::Handle, method)?;
            let args = parse_args::<EtfInavSubscribeArgs>(args)?;
            prepare_etf_inav_subscribe(bloomberg, args, batch_items).await?
        }
        _ => return Err(HostError::unknown_method("engine", method)),
    };
    let stream = bloomberg
        .subscribe(prepared.request)
        .await
        .map_err(HostError::from_async)?;
    let shutdown = bloomberg.shutdown_receiver();
    let subscription = HostSubscription::from_stream(stream, prepared.batch_items, Some(shutdown))
        .keep_alive(Box::new(engine));
    Ok(Payload::Handle(register(subscription)?))
}

pub(crate) fn call_sync(
    _engine: &EngineEntry,
    method: &str,
    _args: &str,
    _kind: ResultKind,
) -> Result<Payload, HostError> {
    Err(HostError::unknown_method("engine", method))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

/// Delivery data representation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
enum DataFormat {
    #[default]
    Table,
    IpcStream,
    IpcFile,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TickReadArgs {
    max_items: Option<u64>,
    max_wait_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RowReadArgs {
    max_rows: Option<u64>,
    max_wait_ms: Option<u64>,
    #[serde(default)]
    format: DataFormat,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DrainArgs {
    #[serde(default)]
    drain: bool,
    #[serde(default)]
    format: DataFormat,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AddArgs {
    tickers: Vec<String>,
    aliases: Option<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FieldsArgs {
    fields: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TickersArgs {
    tickers: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReceiptArgs {
    receipt: u64,
}

fn limit(value: Option<u64>) -> Option<usize> {
    value.map(|value| usize::try_from(value).unwrap_or(usize::MAX))
}

fn wait(value: Option<u64>) -> Option<Duration> {
    value.map(Duration::from_millis)
}

/// `subscriptionCall(sub, method, argsJson, kind, token)`.
pub(crate) async fn subscription_call(
    id: u64,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let subscription = subscription(id)?;
    match method {
        "nextTicks" => {
            kind.expect(ResultKind::Ticks, method)?;
            let args = parse_args::<TickReadArgs>(args)?;
            let outcome = subscription
                .next_updates(limit(args.max_items), wait(args.max_wait_ms))
                .await?;
            Ok(match outcome {
                ReadOutcome::Ready(read) => Payload::Java(Box::new(TicksRead(read))),
                ReadOutcome::Ended | ReadOutcome::TimedOut => Payload::Void,
            })
        }
        "nextArrow" => {
            kind.expect_table(method)?;
            let args = parse_args::<RowReadArgs>(args)?;
            let outcome = subscription
                .next_arrow_batch(limit(args.max_rows), wait(args.max_wait_ms))
                .await?;
            match outcome {
                ReadOutcome::Ready(read) => match kind {
                    // Keep the read guards until the Java table exists.
                    ResultKind::Table => Ok(Payload::Java(Box::new(ArrowRead(read)))),
                    ResultKind::IpcStream => Ok(Payload::Bytes(ipc_stream(read.value())?)),
                    _ => Ok(Payload::Bytes(ipc_file(read.value())?)),
                },
                ReadOutcome::Ended | ReadOutcome::TimedOut => Ok(Payload::Void),
            }
        }
        "nextDelivery" => {
            kind.expect(ResultKind::Delivery, method)?;
            let args = parse_args::<RowReadArgs>(args)?;
            let outcome = subscription
                .next_delivery(limit(args.max_rows), wait(args.max_wait_ms))
                .await?;
            Ok(match outcome {
                ReadOutcome::Ready(read) => Payload::Java(Box::new(DeliveryPublication {
                    subscription,
                    read,
                    format: args.format,
                })),
                ReadOutcome::Ended | ReadOutcome::TimedOut => Payload::Void,
            })
        }
        "unsubscribeTicks" => {
            kind.expect(ResultKind::TicksList, method)?;
            let args = parse_args::<DrainArgs>(args)?;
            let batches = subscription.unsubscribe_scalar(args.drain).await?;
            Ok(Payload::Java(Box::new(TicksList(batches))))
        }
        "unsubscribeArrow" => {
            kind.expect(ResultKind::TableList, method)?;
            let args = parse_args::<DrainArgs>(args)?;
            Ok(Payload::TableList(
                subscription.unsubscribe_arrow(args.drain).await?,
            ))
        }
        "unsubscribeDeliveries" => {
            kind.expect(ResultKind::DeliveryList, method)?;
            let args = parse_args::<DrainArgs>(args)?;
            let drain = subscription.unsubscribe_deliveries(args.drain).await?;
            Ok(Payload::Java(Box::new(DrainPublication {
                subscription,
                drain,
                format: args.format,
            })))
        }
        "add" => {
            kind.expect(ResultKind::Void, method)?;
            let args = parse_args::<AddArgs>(args)?;
            let aliases = validate_aliases(args.aliases.as_ref())?;
            subscription.add(args.tickers, aliases).await?;
            Ok(Payload::Void)
        }
        "addFields" => {
            kind.expect(ResultKind::Void, method)?;
            let args = parse_args::<FieldsArgs>(args)?;
            subscription.add_fields(args.fields).await?;
            Ok(Payload::Void)
        }
        "remove" => {
            kind.expect(ResultKind::Void, method)?;
            let args = parse_args::<TickersArgs>(args)?;
            subscription.remove(args.tickers).await?;
            Ok(Payload::Void)
        }
        _ => Err(HostError::unknown_method("subscription", method)),
    }
}

/// `subscriptionCallSync(sub, method, argsJson, kind)`.
pub(crate) fn subscription_call_sync(
    id: u64,
    method: &str,
    args: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let subscription = subscription(id)?;
    match method {
        "commitDelivery" => {
            kind.expect(ResultKind::Bool, method)?;
            let args = parse_args::<ReceiptArgs>(args)?;
            Ok(Payload::Bool(subscription.commit_delivery(args.receipt)?))
        }
        "rollbackDelivery" => {
            kind.expect(ResultKind::Void, method)?;
            let args = parse_args::<ReceiptArgs>(args)?;
            subscription.rollback_delivery(args.receipt);
            Ok(Payload::Void)
        }
        "commitDrain" => {
            kind.expect(ResultKind::Bool, method)?;
            let args = parse_args::<ReceiptArgs>(args)?;
            Ok(Payload::Bool(subscription.commit_drain(args.receipt)))
        }
        "rollbackDrain" => {
            kind.expect(ResultKind::Void, method)?;
            let args = parse_args::<ReceiptArgs>(args)?;
            subscription.rollback_drain(args.receipt);
            Ok(Payload::Void)
        }
        "latest" => {
            kind.expect_table(method)?;
            parse_args::<NoArgs>(args)?;
            Payload::table(subscription.latest()?, kind)
        }
        _ => {
            parse_args::<NoArgs>(args)?;
            snapshot_call(&subscription, method, kind)
        }
    }
}

/// Calls without arguments: lifecycle flags, warnings and status snapshots.
fn snapshot_call(
    subscription: &HostSubscription,
    method: &str,
    kind: ResultKind,
) -> Result<Payload, HostError> {
    let json = |value: &dyn erased::Json| -> Result<Payload, HostError> {
        kind.expect(ResultKind::Json, method)?;
        value.payload()
    };
    match method {
        "info" => json(&serde_json::json!({
            "deliversRows": subscription.delivers_rows(),
            "requiresDeliveries": subscription.requires_deliveries(),
            "delivery": subscription.delivery_mode().as_str(),
            "batchItems": subscription.batch_items(),
        })),
        "signalClosing" => {
            kind.expect(ResultKind::Void, method)?;
            subscription.signal_closing();
            Ok(Payload::Void)
        }
        "isActive" => {
            kind.expect(ResultKind::Bool, method)?;
            Ok(Payload::Bool(subscription.is_active()))
        }
        "takeWarnings" => {
            let warnings: Vec<status::EventOutput> = subscription
                .take_warnings()
                .iter()
                .map(Into::into)
                .collect();
            json(&warnings)
        }
        "tickers" => json(&subscription.tickers()),
        "fields" => json(&subscription.fields()),
        _ => {
            let snapshot = subscription.status_snapshot();
            match method {
                "stats" => json(&status::stats(&snapshot)),
                "status" => json(&status::status(&snapshot)),
                "events" => json(&status::events(&snapshot)),
                "failures" => json(&status::failures(&snapshot)),
                "failedTickers" => json(&status::failed_tickers(&snapshot)),
                "topicStates" => json(&status::topic_states(&snapshot)),
                "fieldErrors" => json(&status::field_errors(&snapshot)),
                "sessionStatus" => json(&status::SessionOutput::from(snapshot.session())),
                "serviceStatus" => json(&status::services(&snapshot)),
                "adminStatus" => json(&status::AdminOutput::from(snapshot.admin())),
                _ => Err(HostError::unknown_method("subscription", method)),
            }
        }
    }
}

/// Object-safe JSON serialization for [`snapshot_call`].
mod erased {
    use xbbg_host::errors::HostError;

    use crate::payload::Payload;

    pub(super) trait Json {
        fn payload(&self) -> Result<Payload, HostError>;
    }

    impl<T: serde::Serialize> Json for T {
        fn payload(&self) -> Result<Payload, HostError> {
            Payload::json(self)
        }
    }
}

/// `subscriptionRelease(sub)`: drop the handle and signal close, so reads
/// still pending on it end. Idempotent.
pub(crate) fn subscription_release(id: u64) {
    if let Some(subscription) = SUBSCRIPTIONS.remove(id) {
        subscription.signal_closing();
    }
}

/// Release every subscription and refuse new ones (JVM shutdown, after the
/// engines were shut down); pending reads end.
pub(crate) fn release_all() {
    close_all(&SUBSCRIPTIONS);
}

fn close_all(table: &HandleTable<HostSubscription>) {
    for subscription in table.close() {
        subscription.signal_closing();
    }
}

// ---------------------------------------------------------------- Java conversion

/// `SubscriptionBridge` and the classes its factories take, resolved once
/// through the class loader of `NativeBridge` (bridge threads cannot rely on
/// `FindClass` for application classes).
struct SubscriptionRefs {
    bridge: Global<JClass<'static>>,
    ticks: JStaticMethodID,
    delivery: JStaticMethodID,
    delivery_error: JStaticMethodID,
    delivery_failure: JStaticMethodID,
    drain: JStaticMethodID,
    drain_failure: JStaticMethodID,
}

static REFS: OnceLock<SubscriptionRefs> = OnceLock::new();

fn refs(env: &mut Env<'_>) -> Result<&'static SubscriptionRefs, NativeError> {
    if let Some(refs) = REFS.get() {
        return Ok(refs);
    }
    let resolved = env.with_local_frame(16, |env| -> Result<SubscriptionRefs, NativeError> {
        let loader = java()?.bridge.get_class_loader(env)?;
        let bridge = LoaderContext::Loader(&loader).load_class(
            env,
            jni_str!("io.github.underloam.xbbg.internal.SubscriptionBridge"),
            true,
        )?;
        let ticks = env.get_static_method_id(
            &bridge,
            jni_str!("ticks"),
            jni_sig!(
                "(I[Ljava/lang/String;[B[Ljava/lang/String;[J[I[C[B[J[Ljava/lang/String;)Lio/github/underloam/xbbg/internal/TickBatch;"
            ),
        )?;
        let delivery = env.get_static_method_id(
            &bridge,
            jni_str!("delivery"),
            jni_sig!("(ILjava/lang/Object;ZJJ)Lio/github/underloam/xbbg/internal/NativeDelivery;"),
        )?;
        let delivery_error = env.get_static_method_id(
            &bridge,
            jni_str!("deliveryError"),
            jni_sig!("(J)Lio/github/underloam/xbbg/internal/NativeDelivery;"),
        )?;
        let delivery_failure = env.get_static_method_id(
            &bridge,
            jni_str!("deliveryConversionFailure"),
            jni_sig!("(Ljava/lang/Throwable;)Lio/github/underloam/xbbg/internal/NativeDelivery;"),
        )?;
        let drain = env.get_static_method_id(
            &bridge,
            jni_str!("drain"),
            jni_sig!("([Ljava/lang/Object;J)Lio/github/underloam/xbbg/internal/NativeDrain;"),
        )?;
        let drain_failure = env.get_static_method_id(
            &bridge,
            jni_str!("drainConversionFailure"),
            jni_sig!("(Ljava/lang/Throwable;)Lio/github/underloam/xbbg/internal/NativeDrain;"),
        )?;
        Ok(SubscriptionRefs {
            bridge: env.new_global_ref(bridge)?,
            ticks,
            delivery,
            delivery_error,
            delivery_failure,
            drain,
            drain_failure,
        })
    })?;
    // A concurrent first use keeps whichever set won; both are equivalent.
    let _ = REFS.set(resolved);
    Ok(REFS
        .get()
        .expect("subscription bridge references were just set"))
}

impl SubscriptionRefs {
    /// Call a static factory returning an object.
    fn call<'local>(
        &self,
        env: &mut Env<'local>,
        method: JStaticMethodID,
        args: &[jvalue],
    ) -> Result<JObject<'local>, JniError> {
        // SAFETY: every factory was resolved with the signature its callers
        // build `args` for, and each returns an object.
        unsafe { env.call_static_method_unchecked(&self.bridge, method, JavaType::Object, args) }?
            .l()
    }
}

/// Layout kind codes (`TickBatch.KIND_*`).
fn kind_code(kind: FieldKind) -> i8 {
    match kind {
        FieldKind::Unknown => 0,
        FieldKind::Bool => 1,
        FieldKind::I32 => 2,
        FieldKind::I64 => 3,
        FieldKind::F64 => 4,
        FieldKind::Str => 5,
        FieldKind::Date32 => 6,
        FieldKind::Time64Micros => 7,
        FieldKind::TimestampMicros => 8,
    }
}

const VALUE_NULL: i8 = 0;
const VALUE_BOOL: i8 = 1;
const VALUE_I32: i8 = 2;
const VALUE_F64: i8 = 3;
const VALUE_STR: i8 = 4;
const VALUE_I64: i8 = 5;

/// One present value resolved by its field's layout kind (napi `TickValue::resolve`):
/// a payload that does not match the kind reads as null; a non-string payload
/// of a string (or unknown) field becomes its text.
fn resolve<'a>(kind: FieldKind, value: &'a UpdateValue) -> (i8, i64, Option<Cow<'a, str>>) {
    match (kind, value) {
        (_, UpdateValue::Null) => (VALUE_NULL, 0, None),
        (FieldKind::Unknown | FieldKind::Str, UpdateValue::Str(text)) => {
            (VALUE_STR, 0, Some(Cow::Borrowed(text.as_ref())))
        }
        (FieldKind::Unknown | FieldKind::Str, other) => match other.as_string_lossy() {
            Some(text) => (VALUE_STR, 0, Some(Cow::Owned(text))),
            None => (VALUE_NULL, 0, None),
        },
        (FieldKind::Bool, UpdateValue::Bool(flag)) => (VALUE_BOOL, i64::from(*flag), None),
        (
            FieldKind::I32 | FieldKind::Date32,
            UpdateValue::I32(number) | UpdateValue::Date32(number),
        ) => (VALUE_I32, i64::from(*number), None),
        (FieldKind::F64, UpdateValue::F64(number)) => (
            VALUE_F64,
            i64::from_ne_bytes(number.to_bits().to_ne_bytes()),
            None,
        ),
        (
            FieldKind::I64 | FieldKind::Time64Micros | FieldKind::TimestampMicros,
            UpdateValue::I64(number)
            | UpdateValue::Time64Micros(number)
            | UpdateValue::TimestampMicros(number),
        ) => (VALUE_I64, *number, None),
        _ => (VALUE_NULL, 0, None),
    }
}

/// A `String[]` of `items`.
fn string_array<'local, 'a>(
    env: &mut Env<'local>,
    items: impl ExactSizeIterator<Item = &'a str>,
) -> Result<JObjectArray<'local, JString<'local>>, NativeError> {
    let array = JObjectArray::<JString>::new(env, items.len(), JString::null())?;
    for (index, item) in items.enumerate() {
        let text = JString::new(env, item)?.auto();
        array.set_element(env, index, &*text)?;
    }
    Ok(array)
}

/// The columnar `TickBatch` of one scalar batch (contract §7).
fn tick_batch<'local>(
    env: &mut Env<'local>,
    refs: &SubscriptionRefs,
    batch: &ScalarBatch,
) -> Result<JObject<'local>, NativeError> {
    let rows = batch.updates.len();
    let cells: usize = batch.updates.iter().map(|update| update.values.len()).sum();
    jsize(rows, "subscription batch rows")?;
    jsize(cells, "subscription batch values")?;
    let mut timestamps = Vec::with_capacity(rows);
    let mut row_offsets = Vec::with_capacity(rows + 1);
    let mut field_indices = Vec::with_capacity(cells);
    let mut value_kinds = Vec::with_capacity(cells);
    let mut value_bits = Vec::with_capacity(cells);
    let mut strings: Vec<Cow<'_, str>> = Vec::new();
    row_offsets.push(0_i32);
    for update in &batch.updates {
        timestamps.push(update.timestamp_us);
        for field in &update.values {
            let kind = update
                .layout
                .fields
                .get(usize::from(field.index))
                .map_or(FieldKind::Unknown, |meta| meta.kind);
            let (value_kind, mut bits, text) = resolve(kind, &field.value);
            if let Some(text) = text {
                bits = i64::try_from(strings.len()).unwrap_or(i64::MAX);
                strings.push(text);
            }
            field_indices.push(field.index);
            value_kinds.push(value_kind);
            value_bits.push(bits);
        }
        row_offsets.push(jsize(field_indices.len(), "subscription batch values")?);
    }
    env.with_local_frame_returning_local::<_, JObject, NativeError>(32, |env| {
        let (layout_fields, layout_kinds) = match &batch.layout {
            Some(layout) => {
                let names = string_array(env, layout.fields.iter().map(|meta| meta.name.as_ref()))?;
                let codes: Vec<i8> = layout
                    .fields
                    .iter()
                    .map(|meta| kind_code(meta.kind))
                    .collect();
                let kinds = JByteArray::new(env, codes.len())?;
                kinds.set_region(env, 0, &codes)?;
                (JObject::from(names), JObject::from(kinds))
            }
            None => (JObject::null(), JObject::null()),
        };
        let topics = JObjectArray::<JString>::new(env, rows, JString::null())?;
        let mut previous: Option<(&str, jni::refs::Auto<'_, JString<'_>>)> = None;
        for (row, update) in batch.updates.iter().enumerate() {
            // Bursts usually repeat one topic: share its Java string.
            if previous
                .as_ref()
                .is_none_or(|(topic, _)| *topic != update.topic.as_ref())
            {
                let text = JString::new(env, update.topic.as_ref())?.auto();
                previous = Some((update.topic.as_ref(), text));
            }
            let (_, text) = previous.as_ref().expect("topic string was just set");
            topics.set_element(env, row, &**text)?;
        }
        drop(previous);
        let timestamps_array = JLongArray::new(env, rows)?;
        timestamps_array.set_region(env, 0, &timestamps)?;
        let offsets_array = JIntArray::new(env, row_offsets.len())?;
        offsets_array.set_region(env, 0, &row_offsets)?;
        let indices_array = JCharArray::new(env, cells)?;
        indices_array.set_region(env, 0, &field_indices)?;
        let kinds_array = JByteArray::new(env, cells)?;
        kinds_array.set_region(env, 0, &value_kinds)?;
        let bits_array = JLongArray::new(env, cells)?;
        bits_array.set_region(env, 0, &value_bits)?;
        let strings_array = string_array(env, strings.iter().map(AsRef::as_ref))?;
        let layout_version = i32::try_from(batch.layout_version).unwrap_or(i32::MAX);
        let args = [
            jvalue { i: layout_version },
            jvalue {
                l: layout_fields.as_raw(),
            },
            jvalue {
                l: layout_kinds.as_raw(),
            },
            jvalue { l: topics.as_raw() },
            jvalue {
                l: timestamps_array.as_raw(),
            },
            jvalue {
                l: offsets_array.as_raw(),
            },
            jvalue {
                l: indices_array.as_raw(),
            },
            jvalue {
                l: kinds_array.as_raw(),
            },
            jvalue {
                l: bits_array.as_raw(),
            },
            jvalue {
                l: strings_array.as_raw(),
            },
        ];
        Ok(refs.call(env, refs.ticks, &args)?)
    })
}

/// A scalar read: the batch is converted while its read guards are held.
struct TicksRead(GuardedRead<ScalarBatch>);

impl JavaPayload for TicksRead {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let refs = refs(env)?;
        let converted = tick_batch(env, refs, self.0.value());
        drop(self);
        converted
    }
}

/// An Arrow read: the table is built while its read guards are held.
struct ArrowRead(GuardedRead<RecordBatch>);

impl JavaPayload for ArrowRead {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let converted = table::to_java(env, self.0.value());
        drop(self);
        converted
    }
}

/// Drained scalar batches as `Object[]` of `TickBatch`.
struct TicksList(Vec<ScalarBatch>);

impl JavaPayload for TicksList {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let refs = refs(env)?;
        let array = JObjectArray::<JObject>::new(env, self.0.len(), JObject::null())?;
        for (index, batch) in self.0.iter().enumerate() {
            let element = tick_batch(env, refs, batch)?.auto();
            array.set_element(env, index, &*element)?;
        }
        Ok(array.into())
    }
}

/// Delivery data in its requested representation.
fn delivery_data<'local>(
    env: &mut Env<'local>,
    data: &RecordBatch,
    format: DataFormat,
) -> Result<JObject<'local>, NativeError> {
    match format {
        DataFormat::Table => table::to_java(env, data),
        DataFormat::IpcStream => Ok(env.byte_array_from_slice(&ipc_stream(data)?)?.into()),
        DataFormat::IpcFile => Ok(env.byte_array_from_slice(&ipc_file(data)?)?.into()),
    }
}

fn delivery_kind(kind: SubscriptionDeliveryMode) -> i32 {
    match kind {
        SubscriptionDeliveryMode::State => 1,
        _ => 0,
    }
}

/// `SubscriptionBridge.delivery(kind, data, hasGap, droppedUpdates, receipt)`.
fn delivery_object<'local>(
    env: &mut Env<'local>,
    refs: &SubscriptionRefs,
    kind: SubscriptionDeliveryMode,
    data: &JObject<'_>,
    dropped_updates: Option<u64>,
    receipt: u64,
) -> Result<JObject<'local>, NativeError> {
    let args = [
        jvalue {
            i: delivery_kind(kind),
        },
        jvalue { l: data.as_raw() },
        jvalue {
            z: dropped_updates.is_some(),
        },
        jvalue {
            // The unsigned count crosses as its bit pattern.
            j: i64::from_ne_bytes(dropped_updates.unwrap_or(0).to_ne_bytes()),
        },
        jvalue {
            j: i64::try_from(receipt).unwrap_or(i64::MAX),
        },
    ];
    Ok(refs.call(env, refs.delivery, &args)?)
}

/// A conversion failure as a retryable marker carrying the Java exception.
fn conversion_failure<'local>(
    env: &mut Env<'local>,
    refs: &SubscriptionRefs,
    factory: JStaticMethodID,
    error: NativeError,
) -> Result<JObject<'local>, NativeError> {
    let cause = throwable(env, error)?.auto();
    Ok(refs.call(env, factory, &[jvalue { l: cause.as_raw() }])?)
}

/// A marked delivery read, published in two phases: convert, then register
/// the receipt (re-checking close); any failure rolls the source back.
struct DeliveryPublication {
    subscription: Arc<HostSubscription>,
    read: DeliveryRead,
    format: DataFormat,
}

impl JavaPayload for DeliveryPublication {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let Self {
            subscription,
            read,
            format,
        } = *self;
        let refs = refs(env)?;
        match read.prepare() {
            PreparedDelivery::Closed => Ok(JObject::null()),
            PreparedDelivery::Error(error) => match error.publish(&subscription) {
                None => Ok(JObject::null()),
                Some(receipt) => {
                    let args = [jvalue {
                        j: i64::try_from(receipt).unwrap_or(i64::MAX),
                    }];
                    refs.call(env, refs.delivery_error, &args)
                        .or_else(|failure| {
                            subscription.rollback_delivery(receipt);
                            conversion_failure(env, refs, refs.delivery_failure, failure.into())
                        })
                }
            },
            PreparedDelivery::Data(data) => {
                let converted = env
                    .with_local_frame_returning_local::<_, JObject, NativeError>(8, |env| {
                        delivery_data(env, &data.data, format)
                    });
                let java_data = match converted {
                    Ok(java_data) => java_data.auto(),
                    // Dropping `data` rolls the delivery back for a retry.
                    Err(failure) => {
                        drop(data);
                        return conversion_failure(env, refs, refs.delivery_failure, failure);
                    }
                };
                let kind = data.kind;
                let dropped = data.gap_before.map(|gap| gap.dropped_updates);
                let Some(receipt) = data.publish(&subscription) else {
                    return Ok(JObject::null());
                };
                delivery_object(env, refs, kind, &java_data, dropped, receipt).or_else(|failure| {
                    subscription.rollback_delivery(receipt);
                    conversion_failure(env, refs, refs.delivery_failure, failure)
                })
            }
        }
    }
}

/// A frozen delivery drain: every delivery is converted before one receipt
/// is registered; any failure rolls the whole drain back for a retry.
struct DrainPublication {
    subscription: Arc<HostSubscription>,
    drain: PreparedDrain,
    format: DataFormat,
}

impl DrainPublication {
    fn deliveries<'local>(
        &self,
        env: &mut Env<'local>,
        refs: &SubscriptionRefs,
    ) -> Result<JObjectArray<'local, JObject<'local>>, NativeError> {
        let deliveries = &self.drain.deliveries;
        let array = JObjectArray::<JObject>::new(env, deliveries.len(), JObject::null())?;
        for (index, delivery) in deliveries.iter().enumerate() {
            env.with_local_frame(8, |env| -> Result<(), NativeError> {
                let data = delivery_data(env, &delivery.data, self.format)?;
                let dropped = delivery.gap_before.map(|gap| gap.dropped_updates);
                let element = delivery_object(env, refs, delivery.kind, &data, dropped, 0)?;
                array.set_element(env, index, &element)?;
                Ok(())
            })?;
        }
        Ok(array)
    }
}

impl JavaPayload for DrainPublication {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let refs = refs(env)?;
        let deliveries = match self.deliveries(env, refs) {
            Ok(deliveries) => deliveries.auto(),
            Err(failure) => {
                // Dropping the drain rolls every delivery back for a retry.
                drop(self);
                return conversion_failure(env, refs, refs.drain_failure, failure);
            }
        };
        let Self {
            subscription,
            drain,
            ..
        } = *self;
        let receipt = drain.publish(&subscription);
        let args = [
            jvalue {
                l: deliveries.as_raw(),
            },
            jvalue {
                j: receipt.map_or(0, |receipt| i64::try_from(receipt).unwrap_or(i64::MAX)),
            },
        ];
        refs.call(env, refs.drain, &args).or_else(|failure| {
            if let Some(receipt) = receipt {
                subscription.rollback_drain(receipt);
            }
            conversion_failure(env, refs, refs.drain_failure, failure.into())
        })
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use xbbg_async::testing::SubscriptionFixture;
    use xbbg_async::{EngineConfig, SubscribeRequest};

    use super::*;

    const TOPIC: &str = "SYNTHETIC Equity";
    const WAIT: Duration = Duration::from_secs(10);

    async fn subscribe(source: &SubscriptionFixture) -> HostSubscription {
        let stream = source
            .subscribe(SubscribeRequest {
                topics: vec![TOPIC.into()],
                fields: vec!["BID".into()],
                ..SubscribeRequest::default()
            })
            .await
            .expect("subscribe");
        HostSubscription::from_stream(stream, 16, None)
    }

    async fn ended(
        read: tokio::task::JoinHandle<Result<ReadOutcome<GuardedRead<ScalarBatch>>, HostError>>,
    ) -> bool {
        let outcome = tokio::time::timeout(WAIT, read)
            .await
            .expect("the pending read ends")
            .expect("join")
            .expect("read");
        matches!(outcome, ReadOutcome::Ended)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn releasing_a_handle_ends_its_pending_read() {
        let source = SubscriptionFixture::new(EngineConfig::default());
        let id = register(subscribe(&source).await).expect("open table");
        let held = subscription(id).expect("registered");
        let pending = tokio::spawn(async move { held.next_updates(None, None).await });
        tokio::task::yield_now().await;

        subscription_release(id);
        assert!(ended(pending).await);
        let Err(closed) = subscription(id) else {
            panic!("released");
        };
        assert_eq!(closed.message, "subscription is closed");
        subscription_release(id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_ends_pending_reads_and_refuses_later_subscriptions() {
        static TABLE: HandleTable<HostSubscription> = HandleTable::new();
        let source = SubscriptionFixture::new(EngineConfig::default());
        let id = admit(&TABLE, subscribe(&source).await).expect("open table");
        let held = TABLE.get(id).expect("registered");
        let pending = tokio::spawn(async move { held.next_updates(None, None).await });
        tokio::task::yield_now().await;

        close_all(&TABLE);
        assert!(ended(pending).await);

        // A subscription that finishes opening after shutdown must not keep
        // its engine alive: it is refused and dropped with what it holds.
        let engine = Arc::new(());
        let late = subscribe(&source)
            .await
            .keep_alive(Box::new(Arc::clone(&engine)));
        let error = admit(&TABLE, late).expect_err("closed table");
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::State);
        assert_eq!(error.message, "subscription is closed");
        assert_eq!(Arc::strong_count(&engine), 1);
        assert_eq!(TABLE.len(), 0);
    }
}
