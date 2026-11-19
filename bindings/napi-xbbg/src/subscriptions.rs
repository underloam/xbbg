//! N-API subscription handle, delivery publication, and scalar/Arrow batch marshalling.
//!
//! All subscription state, two-phase delivery settlement, drains, row guards, and
//! membership controls live in [`xbbg_host::subscription::HostSubscription`].

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use napi::bindgen_prelude::{
    Env, Error, FromNapiValue, FunctionRef, JavaScriptClassExt, JsObjectValue, JsValue, Object,
    Status, ToNapiValue, Unknown,
};
use napi_derive::napi;
use xbbg_async::engine::state::{FieldKind, FieldLayout, SubscriptionUpdate, UpdateValue};
use xbbg_async::engine::{
    AdminStatusInfo, ServiceStatusInfo, SessionStatusInfo, SubscriptionDelivery,
    SubscriptionDeliveryMode, SubscriptionEventInfo, SubscriptionFailureInfo, SubscriptionGap,
    SubscriptionStatusState, SubscriptionStream, TopicStatusInfo,
};
use xbbg_host::subscription::{
    DeliveryRead, HostSubscription, PreparedDelivery, PreparedDrain, ReadGuard, ReadOutcome,
    ScalarBatch,
};

use crate::arrow_zero_copy::{NativeArrowBatch, NativeArrowIpcFile};
use crate::config::to_i64_saturating;
use crate::engine::to_native_record_batch;
use crate::errors::{coded, host_error_to_napi};

#[napi(object)]
pub struct SubscriptionStats {
    pub messages_received: i64,
    pub dropped_batches: i64,
    pub batches_sent: i64,
    pub slow_consumer: bool,
}

#[napi(object)]
pub struct SubscriptionEventOutput {
    pub at_us: i64,
    pub category: String,
    pub level: String,
    pub message_type: String,
    pub topic: Option<String>,
    pub detail: Option<String>,
}

#[napi(object)]
pub struct SubscriptionFailureOutput {
    pub topic: String,
    pub reason: String,
    pub kind: String,
    pub at_us: i64,
}

#[napi(object)]
pub struct TopicStateOutput {
    pub topic: String,
    pub feed_topic: String,
    pub delayed: Option<bool>,
    pub state: String,
    pub last_change_us: i64,
    pub streams_active: bool,
    pub streams_changed_us: i64,
}

#[napi(object)]
pub struct SessionStatusOutput {
    pub state: String,
    pub last_change_us: i64,
    pub disconnect_count: i64,
    pub reconnect_count: i64,
}

#[napi(object)]
pub struct ServiceStatusOutput {
    pub service: String,
    pub up: bool,
    pub last_change_us: i64,
}

#[napi(object)]
pub struct AdminStatusOutput {
    pub slow_consumer_warning_active: bool,
    pub slow_consumer_warning_count: i64,
    pub slow_consumer_cleared_count: i64,
    pub data_loss_count: i64,
    pub last_warning_us: Option<i64>,
    pub last_cleared_us: Option<i64>,
    pub last_data_loss_us: Option<i64>,
}

#[napi(object)]
pub struct SubscriptionStatusOutput {
    pub events: Vec<SubscriptionEventOutput>,
    pub failures: Vec<SubscriptionFailureOutput>,
    pub failed_tickers: Vec<String>,
    pub topic_states: HashMap<String, TopicStateOutput>,
    pub field_errors: HashMap<String, HashMap<String, String>>,
    pub session: SessionStatusOutput,
    pub services: HashMap<String, ServiceStatusOutput>,
    pub admin: AdminStatusOutput,
}

#[napi(object)]
pub struct NativeSubscriptionLayout {
    pub version: u32,
    pub fields: Vec<String>,
    pub kinds: Vec<String>,
}

/// One layout-homogeneous batch of scalar rows. `ToNapiValue` writes it to JS
/// as columns straight from the engine updates: no per-row Rust allocation, no
/// padding for absent value kinds, and no decimal round trip for 64-bit values.
pub struct NativeSubscriptionUpdateBatch {
    pub(crate) layout: Option<NativeSubscriptionLayout>,
    pub(crate) layout_version: u32,
    pub(crate) updates: Vec<SubscriptionUpdate>,
}

impl From<SubscriptionEventInfo> for SubscriptionEventOutput {
    fn from(event: SubscriptionEventInfo) -> Self {
        Self {
            at_us: event.at_us,
            category: event.category.as_str().to_string(),
            level: event.level.as_str().to_string(),
            message_type: event.message_type,
            topic: event.topic,
            detail: event.detail,
        }
    }
}

impl From<&SubscriptionFailureInfo> for SubscriptionFailureOutput {
    fn from(failure: &SubscriptionFailureInfo) -> Self {
        Self {
            topic: failure.topic.clone(),
            reason: failure.reason.clone(),
            kind: failure.kind.as_str().to_string(),
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
            state: topic.state.as_str().to_string(),
            last_change_us: topic.last_change_us,
            streams_active: topic.streams_active,
            streams_changed_us: topic.streams_changed_us,
        }
    }
}

impl From<&SessionStatusInfo> for SessionStatusOutput {
    fn from(session: &SessionStatusInfo) -> Self {
        Self {
            state: session.state.as_str().to_string(),
            last_change_us: session.last_change_us,
            disconnect_count: to_i64_saturating(session.disconnect_count),
            reconnect_count: to_i64_saturating(session.reconnect_count),
        }
    }
}

impl From<&ServiceStatusInfo> for ServiceStatusOutput {
    fn from(service: &ServiceStatusInfo) -> Self {
        Self {
            service: service.service.clone(),
            up: service.up,
            last_change_us: service.last_change_us,
        }
    }
}

impl From<&AdminStatusInfo> for AdminStatusOutput {
    fn from(admin: &AdminStatusInfo) -> Self {
        Self {
            slow_consumer_warning_active: admin.slow_consumer_warning_active,
            slow_consumer_warning_count: to_i64_saturating(admin.slow_consumer_warning_count),
            slow_consumer_cleared_count: to_i64_saturating(admin.slow_consumer_cleared_count),
            data_loss_count: to_i64_saturating(admin.data_loss_count),
            last_warning_us: admin.last_warning_us,
            last_cleared_us: admin.last_cleared_us,
            last_data_loss_us: admin.last_data_loss_us,
        }
    }
}

impl From<&SubscriptionStatusState> for SubscriptionStatusOutput {
    fn from(status: &SubscriptionStatusState) -> Self {
        Self {
            events: status.events().iter().cloned().map(Into::into).collect(),
            failures: status.failures().iter().map(Into::into).collect(),
            failed_tickers: status
                .failures()
                .iter()
                .map(|failure| failure.topic.clone())
                .collect(),
            topic_states: status
                .topic_statuses()
                .iter()
                .map(|(label, topic)| (label.clone(), topic.into()))
                .collect(),
            field_errors: status.field_errors().clone(),
            session: status.session().into(),
            services: status
                .services()
                .iter()
                .map(|(name, service)| (name.clone(), service.into()))
                .collect(),
            admin: status.admin().into(),
        }
    }
}

fn field_kind_label(kind: FieldKind) -> &'static str {
    match kind {
        FieldKind::Unknown => "unknown",
        FieldKind::Bool => "bool",
        FieldKind::I32 => "i32",
        FieldKind::I64 => "i64",
        FieldKind::F64 => "f64",
        FieldKind::Str => "str",
        FieldKind::Date32 => "date32",
        FieldKind::Time64Micros => "time64_us",
        FieldKind::TimestampMicros => "timestamp_us",
    }
}

fn to_native_layout(layout: &FieldLayout) -> NativeSubscriptionLayout {
    let fields = layout
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect();
    let kinds = layout
        .fields
        .iter()
        .map(|field| field_kind_label(field.kind).to_string())
        .collect();
    NativeSubscriptionLayout {
        version: layout.version,
        fields,
        kinds,
    }
}

fn scalar_batch_to_native(batch: ScalarBatch) -> NativeSubscriptionUpdateBatch {
    NativeSubscriptionUpdateBatch {
        layout: batch.layout.as_deref().map(to_native_layout),
        layout_version: batch.layout_version,
        updates: batch.updates,
    }
}

/// One present scalar value as JavaScript receives it under its layout kind,
/// resolved once in Rust. Date32 stays a day count (JS builds the `Date`
/// lazily) and 64-bit kinds become BigInt without a decimal round trip.
#[derive(Debug, PartialEq)]
pub(crate) enum TickValue<'a> {
    Null,
    Bool(bool),
    I32(i32),
    F64(f64),
    Str(&'a str),
    /// Compatibility rendering of a non-string payload in a promoted string field.
    Text(String),
    BigInt(i64),
}

impl<'a> TickValue<'a> {
    /// A payload that does not match its layout kind reads as an explicit null,
    /// exactly as the former per-kind value slots did.
    pub(crate) fn resolve(kind: FieldKind, value: &'a UpdateValue) -> Self {
        match (kind, value) {
            (_, UpdateValue::Null) => Self::Null,
            (FieldKind::Unknown | FieldKind::Str, UpdateValue::Str(text)) => Self::Str(text),
            (FieldKind::Unknown | FieldKind::Str, other) => {
                other.as_string_lossy().map_or(Self::Null, Self::Text)
            }
            (FieldKind::Bool, UpdateValue::Bool(flag)) => Self::Bool(*flag),
            (
                FieldKind::I32 | FieldKind::Date32,
                UpdateValue::I32(number) | UpdateValue::Date32(number),
            ) => Self::I32(*number),
            (FieldKind::F64, UpdateValue::F64(number)) => Self::F64(*number),
            (
                FieldKind::I64 | FieldKind::Time64Micros | FieldKind::TimestampMicros,
                UpdateValue::I64(number)
                | UpdateValue::Time64Micros(number)
                | UpdateValue::TimestampMicros(number),
            ) => Self::BigInt(*number),
            _ => Self::Null,
        }
    }
}

impl ToNapiValue for TickValue<'_> {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        use napi::bindgen_prelude::{Null as JsNull, i64n};

        // SAFETY: every variant forwards the caller's ToNapiValue contract.
        unsafe {
            match value {
                Self::Null => JsNull::to_napi_value(env, JsNull),
                Self::Bool(flag) => <bool as ToNapiValue>::to_napi_value(env, flag),
                Self::I32(number) => <i32 as ToNapiValue>::to_napi_value(env, number),
                Self::F64(number) => <f64 as ToNapiValue>::to_napi_value(env, number),
                Self::Str(text) => <&str as ToNapiValue>::to_napi_value(env, text),
                Self::Text(text) => <String as ToNapiValue>::to_napi_value(env, text),
                Self::BigInt(number) => i64n::to_napi_value(env, i64n(number)),
            }
        }
    }
}

trait TypedArrayElement: Copy {
    const KIND: napi::sys::napi_typedarray_type;
}

impl TypedArrayElement for u16 {
    const KIND: napi::sys::napi_typedarray_type = napi::sys::TypedarrayType::uint16_array;
}

impl TypedArrayElement for u32 {
    const KIND: napi::sys::napi_typedarray_type = napi::sys::TypedarrayType::uint32_array;
}

impl TypedArrayElement for f64 {
    const KIND: napi::sys::napi_typedarray_type = napi::sys::TypedarrayType::float64_array;
}

fn napi_created(status: napi::sys::napi_status, what: &str) -> napi::Result<()> {
    if status == napi::sys::Status::napi_ok {
        Ok(())
    } else {
        Err(Error::new(
            Status::from(status),
            format!("failed to create {what}"),
        ))
    }
}

fn copied_typed_array<T: TypedArrayElement>(
    env: napi::sys::napi_env,
    elements: &[T],
) -> napi::Result<napi::sys::napi_value> {
    let byte_length = std::mem::size_of_val(elements);
    let mut data = std::ptr::null_mut();
    let mut buffer = std::ptr::null_mut();
    // SAFETY: `env` is the live environment of the calling ToNapiValue.
    let status =
        unsafe { napi::sys::napi_create_arraybuffer(env, byte_length, &mut data, &mut buffer) };
    napi_created(status, "subscription batch ArrayBuffer")?;
    if byte_length != 0 {
        // SAFETY: V8 just allocated `byte_length` writable bytes at `data`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                elements.as_ptr().cast::<u8>(),
                data.cast::<u8>(),
                byte_length,
            );
        }
    }
    let mut array = std::ptr::null_mut();
    // SAFETY: `buffer` holds exactly `elements.len()` values of `T::KIND`.
    let status = unsafe {
        napi::sys::napi_create_typedarray(env, T::KIND, elements.len(), buffer, 0, &mut array)
    };
    napi_created(status, "subscription batch typed array")?;
    Ok(array)
}

fn js_array_length(label: &str, length: usize) -> napi::Result<u32> {
    u32::try_from(length).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("subscription batch {label} {length} exceeds the JavaScript array limit"),
        )
    })
}

impl ToNapiValue for NativeSubscriptionUpdateBatch {
    unsafe fn to_napi_value(
        raw_env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        let Self {
            layout,
            layout_version,
            updates,
        } = value;
        let env = Env::from_raw(raw_env);
        let cell_count = updates
            .iter()
            .map(|update| update.values.len())
            .sum::<usize>();
        let mut topics = env.create_array(js_array_length("rows", updates.len())?)?;
        let mut values = env.create_array(js_array_length("values", cell_count)?)?;
        let mut timestamps_us = Vec::with_capacity(updates.len());
        let mut row_offsets = Vec::with_capacity(updates.len() + 1);
        let mut field_indices = Vec::with_capacity(cell_count);
        row_offsets.push(0_u32);
        let mut cell = 0_u32;
        let mut previous_topic: Option<(&str, napi::sys::napi_value)> = None;
        for (row, update) in (0_u32..).zip(&updates) {
            let topic = match previous_topic {
                Some((name, handle)) if *name == *update.topic => handle,
                // SAFETY: this ToNapiValue runs on the owning JS thread.
                _ => unsafe { <&str as ToNapiValue>::to_napi_value(raw_env, &update.topic)? },
            };
            previous_topic = Some((update.topic.as_ref(), topic));
            topics.set(row, topic)?;
            timestamps_us.push(update.timestamp_us as f64);
            for field in &update.values {
                let kind = update
                    .layout
                    .fields
                    .get(usize::from(field.index))
                    .map_or(FieldKind::Unknown, |meta| meta.kind);
                values.set(cell, TickValue::resolve(kind, &field.value))?;
                field_indices.push(field.index);
                cell += 1;
            }
            row_offsets.push(cell);
        }

        let mut batch = Object::new(&env)?;
        batch.set_c_named_property(c"kind", "batch")?;
        if let Some(layout) = layout {
            batch.set_c_named_property(c"layout", layout)?;
        }
        batch.set_c_named_property(c"layoutVersion", layout_version)?;
        batch.set_c_named_property(c"topics", topics)?;
        batch.set_c_named_property(
            c"timestampsUs",
            copied_typed_array(raw_env, &timestamps_us)?,
        )?;
        batch.set_c_named_property(c"rowOffsets", copied_typed_array(raw_env, &row_offsets)?)?;
        batch.set_c_named_property(
            c"fieldIndices",
            copied_typed_array(raw_env, &field_indices)?,
        )?;
        batch.set_c_named_property(c"values", values)?;
        Ok(batch.raw())
    }
}

/// Private settlement state retains no Arrow arrays or JavaScript buffers.
#[napi]
pub struct JsSubscriptionPublication {
    host: Arc<HostSubscription>,
    receipt: StdMutex<Option<u64>>,
}

impl Drop for JsSubscriptionPublication {
    fn drop(&mut self) {
        if let Some(id) = self.receipt.get_mut().ok().and_then(Option::take) {
            self.host.rollback_delivery(id);
        }
    }
}

#[napi]
impl JsSubscriptionPublication {
    #[napi]
    pub fn commit(&self) -> napi::Result<bool> {
        let id = self
            .receipt
            .lock()
            .expect("subscription publication poisoned")
            .take()
            .ok_or_else(|| {
                coded(
                    Status::InvalidArg,
                    "VALIDATION",
                    "delivery publication already settled",
                )
            })?;
        self.host.commit_delivery(id).map_err(host_error_to_napi)
    }

    #[napi]
    pub fn rollback(&self) {
        if let Some(id) = self
            .receipt
            .lock()
            .expect("subscription publication poisoned")
            .take()
        {
            self.host.rollback_delivery(id);
        }
    }
}

/// Rust-only completion state. Arrow buffers are exported and the optional backend
/// projector is invoked only by `ToNapiValue` on the owning JS thread.
pub struct NativeSubscriptionDeliveryRead {
    pub(crate) host: Arc<HostSubscription>,
    pub(crate) read: DeliveryRead,
    project: Option<SubscriptionDeliveryProjector>,
    format: DeliveryDataFormat,
}

type SubscriptionDeliveryProjector =
    FunctionRef<NativeSubscriptionDeliveryOutput, Unknown<'static>>;

#[derive(Clone, Copy)]
enum DeliveryDataFormat {
    ZeroCopy,
    IpcFile,
}

enum NativeDeliveryData {
    ZeroCopy(NativeArrowBatch),
    IpcFile(NativeArrowIpcFile),
}

impl ToNapiValue for NativeDeliveryData {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        // SAFETY: both variants forward the caller's ToNapiValue contract.
        unsafe {
            match value {
                Self::ZeroCopy(batch) => NativeArrowBatch::to_napi_value(env, batch),
                Self::IpcFile(file) => NativeArrowIpcFile::to_napi_value(env, file),
            }
        }
    }
}

pub struct NativeSubscriptionDeliveryOutput {
    kind: SubscriptionDeliveryMode,
    data: NativeDeliveryData,
    gap_before: Option<SubscriptionGap>,
}

impl NativeSubscriptionDeliveryOutput {
    fn from_parts(
        kind: SubscriptionDeliveryMode,
        batch: RecordBatch,
        gap_before: Option<SubscriptionGap>,
        format: DeliveryDataFormat,
    ) -> napi::Result<Self> {
        let data = match format {
            DeliveryDataFormat::ZeroCopy => {
                NativeDeliveryData::ZeroCopy(to_native_record_batch(batch)?)
            }
            DeliveryDataFormat::IpcFile => {
                NativeDeliveryData::IpcFile(NativeArrowIpcFile::from_record_batch(&batch)?)
            }
        };
        Ok(Self {
            kind,
            data,
            gap_before,
        })
    }

    fn from_delivery(
        delivery: SubscriptionDelivery,
        format: DeliveryDataFormat,
    ) -> napi::Result<Self> {
        Self::from_parts(delivery.kind, delivery.data, delivery.gap_before, format)
    }
}

impl ToNapiValue for NativeSubscriptionDeliveryOutput {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut output = Object::new(&env)?;
        output.set_named_property("kind", value.kind.as_str())?;
        output.set_named_property("data", value.data)?;
        if let Some(gap) = value.gap_before {
            let mut gap_before = Object::new(&env)?;
            gap_before.set_named_property("cause", gap.cause())?;
            gap_before.set_named_property("droppedUpdates", gap.dropped_updates)?;
            output.set_named_property("gapBefore", gap_before)?;
        }
        Ok(output.raw())
    }
}

fn project_subscription_delivery(
    env: &Env,
    output: NativeSubscriptionDeliveryOutput,
    project: Option<&SubscriptionDeliveryProjector>,
) -> napi::Result<napi::sys::napi_value> {
    if let Some(project) = project {
        let value = project.borrow_back(env)?.call(output)?;
        let value_type = value.get_type()?;
        let thenable = matches!(
            value_type,
            napi::ValueType::Object | napi::ValueType::Function
        ) && value
            .coerce_to_object()?
            .get_named_property::<Unknown>("then")?
            .get_type()?
            == napi::ValueType::Function;
        if thenable {
            return Err(Error::new(
                Status::InvalidArg,
                "delivery backend projector must return synchronously, not a Promise or thenable",
            ));
        }
        return Ok(value.raw());
    }
    // SAFETY: called only from ToNapiValue on the owning JS thread.
    unsafe { NativeSubscriptionDeliveryOutput::to_napi_value(env.raw(), output) }
}

fn delivery_conversion_error(env: napi::sys::napi_env, mut error: Error) -> Error {
    let mut pending = false;
    let status = unsafe { napi::sys::napi_is_exception_pending(env, &mut pending) };
    if status != napi::sys::Status::napi_ok {
        let mut failure = coded(
            Status::from(status),
            "DELIVERY_CONVERSION",
            "failed to inspect a pending JavaScript delivery-conversion exception",
        );
        failure.set_cause(error);
        return failure;
    }
    if pending {
        let mut exception = std::ptr::null_mut();
        let status = unsafe { napi::sys::napi_get_and_clear_last_exception(env, &mut exception) };
        if status != napi::sys::Status::napi_ok {
            let mut failure = coded(
                Status::from(status),
                "DELIVERY_CONVERSION",
                "failed to capture a pending JavaScript delivery-conversion exception",
            );
            failure.set_cause(error);
            return failure;
        }
        // SAFETY: napi_get_and_clear_last_exception returned this live value.
        error = Error::from(unsafe { Unknown::from_raw_unchecked(env, exception) });
    }
    let status = if error.status == Status::PendingException {
        Status::GenericFailure
    } else {
        error.status
    };
    let mut failure = coded(status, "DELIVERY_CONVERSION", &error.reason);
    failure.set_cause(error);
    failure
}

impl ToNapiValue for NativeSubscriptionDeliveryRead {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        let Self {
            host,
            read,
            project,
            format,
        } = value;
        match read.prepare() {
            PreparedDelivery::Closed => unsafe { Option::<()>::to_napi_value(env, None) },
            PreparedDelivery::Error(error) => {
                let env_wrapper = Env::from_raw(env);
                let publication = JsSubscriptionPublication {
                    host: Arc::clone(&host),
                    receipt: StdMutex::new(None),
                }
                .into_instance(&env_wrapper)
                .map_err(|err| delivery_conversion_error(env, err))?;
                let mut output = publication.as_object(&env_wrapper);
                output
                    .set_named_property("kind", "error")
                    .map_err(|err| delivery_conversion_error(env, err))?;
                let Some(id) = error.publish(&host) else {
                    return unsafe { Option::<()>::to_napi_value(env, None) };
                };
                *publication
                    .receipt
                    .lock()
                    .expect("subscription publication poisoned") = Some(id);
                Ok(output.raw())
            }
            PreparedDelivery::Data(data) => {
                let env_wrapper = Env::from_raw(env);
                let projected = NativeSubscriptionDeliveryOutput::from_parts(
                    data.kind,
                    data.data.clone(),
                    data.gap_before,
                    format,
                )
                .and_then(|out| project_subscription_delivery(&env_wrapper, out, project.as_ref()));
                if host.is_closed() {
                    return unsafe { Option::<()>::to_napi_value(env, None) };
                }
                let raw_value = projected.map_err(|err| delivery_conversion_error(env, err))?;
                let publication = JsSubscriptionPublication {
                    host: Arc::clone(&host),
                    receipt: StdMutex::new(None),
                }
                .into_instance(&env_wrapper)
                .map_err(|err| delivery_conversion_error(env, err))?;
                let mut output = publication.as_object(&env_wrapper);
                output
                    .set_named_property("kind", "delivery")
                    .map_err(|err| delivery_conversion_error(env, err))?;
                // SAFETY: created in this active JS completion handle scope.
                output
                    .set_named_property("value", unsafe {
                        Unknown::from_napi_value(env, raw_value)
                            .map_err(|err| delivery_conversion_error(env, err))?
                    })
                    .map_err(|err| delivery_conversion_error(env, err))?;
                let Some(id) = data.publish(&host) else {
                    return unsafe { Option::<()>::to_napi_value(env, None) };
                };
                *publication
                    .receipt
                    .lock()
                    .expect("subscription publication poisoned") = Some(id);
                Ok(output.raw())
            }
        }
    }
}

pub struct NativeSubscriptionDeliveryDrain {
    pub(crate) host: Arc<HostSubscription>,
    pub(crate) prepared: PreparedDrain,
    project: Option<SubscriptionDeliveryProjector>,
    format: DeliveryDataFormat,
}

impl ToNapiValue for NativeSubscriptionDeliveryDrain {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        mut value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        let env_wrapper = Env::from_raw(env);
        let deliveries = std::mem::take(&mut value.prepared.deliveries);
        let mut output = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            let item = NativeSubscriptionDeliveryOutput::from_delivery(delivery, value.format)
                .map_err(|error| delivery_conversion_error(env, error))?;
            let raw = project_subscription_delivery(&env_wrapper, item, value.project.as_ref())
                .map_err(|error| delivery_conversion_error(env, error))?;
            // SAFETY: this value belongs to the active ToNapiValue handle scope.
            output.push(unsafe { Unknown::from_napi_value(env, raw)? });
        }
        // Build the complete JS result before acknowledging any drained batch.
        // SAFETY: all values belong to this JS environment and handle scope.
        let js_output = if output.is_empty() {
            unsafe { Option::<()>::to_napi_value(env, None) }
        } else {
            unsafe { Vec::<Unknown<'_>>::to_napi_value(env, output) }
        }
        .map_err(|error| delivery_conversion_error(env, error))?;
        if let Some(id) = value.prepared.publish(&value.host) {
            value.host.commit_drain(id);
        }
        Ok(js_output)
    }
}

/// Legacy rows retain their existing publication point, keeping the host's
/// `ReadGuard` until JS conversion finishes.
pub struct NativeLegacyRead<T> {
    pub(crate) output: napi::Result<Option<T>>,
    pub(crate) guard: Option<ReadGuard>,
}

impl<T> NativeLegacyRead<T> {
    fn empty() -> Self {
        Self {
            output: Ok(None),
            guard: None,
        }
    }

    fn ready(output: napi::Result<Option<T>>, guard: ReadGuard) -> Self {
        Self {
            output,
            guard: Some(guard),
        }
    }
}

impl<T: ToNapiValue> ToNapiValue for NativeLegacyRead<T> {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        let Self { output, guard } = value;
        let result = match output {
            Ok(output) => unsafe { Option::<T>::to_napi_value(env, output) },
            Err(error) => Err(error),
        };
        drop(guard);
        result
    }
}

#[napi]
pub struct JsSubscription {
    pub(crate) host: Arc<HostSubscription>,
}

#[napi]
impl JsSubscription {
    pub(crate) fn from_stream(stream: SubscriptionStream, batch_items: usize) -> Self {
        Self {
            host: Arc::new(HostSubscription::from_stream(stream, batch_items, None)),
        }
    }

    /// Stop publication before a pending native completion reaches JavaScript.
    /// The asynchronous unsubscribe operation still owns detach/drain/cleanup.
    #[napi]
    pub fn signal_closing(&self) {
        self.host.signal_closing();
    }

    #[napi]
    pub async fn next_updates(
        &self,
        max_items: Option<u32>,
        max_wait_ms: Option<u32>,
    ) -> napi::Result<NativeLegacyRead<NativeSubscriptionUpdateBatch>> {
        let outcome = self
            .host
            .next_updates(
                max_items.map(|v| v as usize),
                max_wait_ms.map(|ms| Duration::from_millis(u64::from(ms))),
            )
            .await
            .map_err(host_error_to_napi)?;
        Ok(match outcome {
            ReadOutcome::Ready(guarded) => {
                let (batch, guard) = guarded.into_parts();
                NativeLegacyRead::ready(Ok(Some(scalar_batch_to_native(batch))), guard)
            }
            ReadOutcome::Ended | ReadOutcome::TimedOut => NativeLegacyRead::empty(),
        })
    }

    #[napi]
    pub async fn next_arrow_batch(
        &self,
        max_rows: Option<u32>,
        max_wait_ms: Option<u32>,
    ) -> napi::Result<NativeLegacyRead<NativeArrowBatch>> {
        let outcome = self
            .host
            .next_arrow_batch(
                max_rows.map(|v| v as usize),
                max_wait_ms.map(|ms| Duration::from_millis(u64::from(ms))),
            )
            .await
            .map_err(host_error_to_napi)?;
        Ok(match outcome {
            ReadOutcome::Ready(guarded) => {
                let (batch, guard) = guarded.into_parts();
                NativeLegacyRead::ready(to_native_record_batch(batch).map(Some), guard)
            }
            ReadOutcome::Ended | ReadOutcome::TimedOut => NativeLegacyRead::empty(),
        })
    }

    /// Retain the delivery until main-thread Arrow/backend conversion succeeds.
    #[napi]
    pub async fn next_delivery(
        &self,
        max_rows: Option<u32>,
        max_wait_ms: Option<u32>,
        project: Option<SubscriptionDeliveryProjector>,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryRead>> {
        self.read_delivery(max_rows, max_wait_ms, project, DeliveryDataFormat::ZeroCopy)
            .await
    }

    /// `next_delivery` whose `data` is one Arrow IPC file instead of zero-copy
    /// descriptors, so nodejs-polars reads it without JS decoding or re-encoding.
    #[napi]
    pub async fn next_ipc_file_delivery(
        &self,
        max_rows: Option<u32>,
        max_wait_ms: Option<u32>,
        project: Option<SubscriptionDeliveryProjector>,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryRead>> {
        self.read_delivery(max_rows, max_wait_ms, project, DeliveryDataFormat::IpcFile)
            .await
    }

    #[napi]
    pub async fn add(
        &self,
        tickers: Vec<String>,
        aliases: Option<HashMap<String, String>>,
    ) -> napi::Result<()> {
        self.host
            .add(tickers, aliases.unwrap_or_default().into_iter().collect())
            .await
            .map_err(host_error_to_napi)
    }

    /// Grow this consumer's field projection and the shared feed's field union.
    #[napi]
    pub async fn add_fields(&self, fields: Vec<String>) -> napi::Result<()> {
        self.host
            .add_fields(fields)
            .await
            .map_err(host_error_to_napi)
    }

    /// Materialized latest values for active consumer labels.
    #[napi]
    pub fn latest(&self) -> napi::Result<NativeArrowBatch> {
        let batch = self.host.latest().map_err(host_error_to_napi)?;
        to_native_record_batch(batch)
    }

    /// Drain new delayed-data and rejected-field warnings without clearing history.
    #[napi]
    pub fn take_warnings(&self) -> Vec<SubscriptionEventOutput> {
        self.host
            .take_warnings()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    #[napi]
    pub async fn remove(&self, tickers: Vec<String>) -> napi::Result<()> {
        self.host.remove(tickers).await.map_err(host_error_to_napi)
    }

    #[napi(getter)]
    pub fn tickers(&self) -> Vec<String> {
        self.host.tickers()
    }

    #[napi(getter)]
    pub fn fields(&self) -> Vec<String> {
        self.host.fields()
    }

    /// Whether this consumer delivers rows; unchanged after close.
    #[napi(getter)]
    pub fn delivers_rows(&self) -> bool {
        self.host.delivers_rows()
    }

    #[napi(getter)]
    pub fn delivery(&self) -> &'static str {
        self.host.delivery_mode().as_str()
    }

    #[napi(getter)]
    pub fn requires_deliveries(&self) -> bool {
        self.host.requires_deliveries()
    }

    #[napi(getter)]
    pub fn is_active(&self) -> bool {
        self.host.is_active()
    }

    #[napi(getter)]
    pub fn status(&self) -> SubscriptionStatusOutput {
        self.host.status_snapshot().as_ref().into()
    }

    #[napi(getter)]
    pub fn events(&self) -> Vec<SubscriptionEventOutput> {
        self.host
            .status_snapshot()
            .events()
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    #[napi(getter)]
    pub fn failures(&self) -> Vec<SubscriptionFailureOutput> {
        self.host
            .status_snapshot()
            .failures()
            .iter()
            .map(Into::into)
            .collect()
    }

    #[napi(getter)]
    pub fn failed_tickers(&self) -> Vec<String> {
        self.host
            .status_snapshot()
            .failures()
            .iter()
            .map(|failure| failure.topic.clone())
            .collect()
    }

    #[napi(getter)]
    pub fn topic_states(&self) -> HashMap<String, TopicStateOutput> {
        self.host
            .status_snapshot()
            .topic_statuses()
            .iter()
            .map(|(label, topic)| (label.clone(), topic.into()))
            .collect()
    }

    #[napi(getter)]
    pub fn field_errors(&self) -> HashMap<String, HashMap<String, String>> {
        self.host.status_snapshot().field_errors().clone()
    }

    #[napi(getter)]
    pub fn session_status(&self) -> SessionStatusOutput {
        self.host.status_snapshot().session().into()
    }

    #[napi(getter)]
    pub fn service_status(&self) -> HashMap<String, ServiceStatusOutput> {
        self.host
            .status_snapshot()
            .services()
            .iter()
            .map(|(name, service)| (name.clone(), service.into()))
            .collect()
    }

    #[napi(getter)]
    pub fn admin_status(&self) -> AdminStatusOutput {
        self.host.status_snapshot().admin().into()
    }

    #[napi(getter)]
    pub fn stats(&self) -> SubscriptionStats {
        let snapshot = self.host.status_snapshot();
        let metrics: Vec<_> = snapshot.fields_metrics().values().cloned().collect();
        SubscriptionStats {
            messages_received: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.messages_received.load(Ordering::Relaxed))
                    .sum(),
            ),
            dropped_batches: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.dropped_batches.load(Ordering::Relaxed))
                    .sum(),
            ),
            batches_sent: to_i64_saturating(
                metrics
                    .iter()
                    .map(|metric| metric.batches_sent.load(Ordering::Relaxed))
                    .sum(),
            ),
            slow_consumer: metrics
                .iter()
                .any(|metric| metric.slow_consumer.load(Ordering::Relaxed)),
        }
    }

    #[napi]
    pub async fn unsubscribe(
        &self,
        drain: Option<bool>,
    ) -> napi::Result<Option<Vec<NativeSubscriptionUpdateBatch>>> {
        let batches = self
            .host
            .unsubscribe_scalar(drain.unwrap_or(false))
            .await
            .map_err(host_error_to_napi)?;
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                batches.into_iter().map(scalar_batch_to_native).collect(),
            ))
        }
    }

    #[napi]
    pub async fn unsubscribe_arrow(
        &self,
        drain: Option<bool>,
    ) -> napi::Result<Option<Vec<NativeArrowBatch>>> {
        let batches = self
            .host
            .unsubscribe_arrow(drain.unwrap_or(false))
            .await
            .map_err(host_error_to_napi)?;
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                batches
                    .into_iter()
                    .map(to_native_record_batch)
                    .collect::<napi::Result<Vec<_>>>()?,
            ))
        }
    }

    #[napi]
    pub async fn unsubscribe_deliveries(
        &self,
        drain: Option<bool>,
        project: Option<SubscriptionDeliveryProjector>,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryDrain>> {
        self.drain_deliveries(drain, project, DeliveryDataFormat::ZeroCopy)
            .await
    }

    /// `unsubscribe_deliveries` whose drained `data` values are Arrow IPC files.
    #[napi]
    pub async fn unsubscribe_ipc_file_deliveries(
        &self,
        drain: Option<bool>,
        project: Option<SubscriptionDeliveryProjector>,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryDrain>> {
        self.drain_deliveries(drain, project, DeliveryDataFormat::IpcFile)
            .await
    }
}

impl JsSubscription {
    async fn read_delivery(
        &self,
        max_rows: Option<u32>,
        max_wait_ms: Option<u32>,
        project: Option<SubscriptionDeliveryProjector>,
        format: DeliveryDataFormat,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryRead>> {
        let outcome = self
            .host
            .next_delivery(
                max_rows.map(|v| v as usize),
                max_wait_ms.map(|ms| Duration::from_millis(u64::from(ms))),
            )
            .await
            .map_err(host_error_to_napi)?;
        Ok(match outcome {
            ReadOutcome::Ready(read) => Some(NativeSubscriptionDeliveryRead {
                host: Arc::clone(&self.host),
                read,
                project,
                format,
            }),
            ReadOutcome::Ended | ReadOutcome::TimedOut => None,
        })
    }

    async fn drain_deliveries(
        &self,
        drain: Option<bool>,
        project: Option<SubscriptionDeliveryProjector>,
        format: DeliveryDataFormat,
    ) -> napi::Result<Option<NativeSubscriptionDeliveryDrain>> {
        let prepared = self
            .host
            .unsubscribe_deliveries(drain.unwrap_or(false))
            .await
            .map_err(host_error_to_napi)?;
        Ok(Some(NativeSubscriptionDeliveryDrain {
            host: Arc::clone(&self.host),
            prepared,
            project,
            format,
        }))
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn arm_publication_pause(
        &self,
        pause: Arc<xbbg_host::subscription::PublicationPause>,
    ) -> Result<(), xbbg_host::errors::HostError> {
        self.host.arm_publication_pause(pause)
    }
}
