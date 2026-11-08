//! Test-only natives (`--features test-support` builds only).
//!
//! `call(method, argsJson, kind)` and `callAsync(method, argsJson, kind, token)`
//! route by prefix, which is stripped before dispatch: `core.*` below,
//! `requests.*` and `recipes.*` to those area modules' test hooks, and
//! `fixture.*` to the subscription fixture. No core method opens a session.
//!
//! Core methods:
//!
//! | method | args | result |
//! |---|---|---|
//! | `core.sampleTable` | `{offset?, length?}` (defaults 3 and the rest) | TABLE / IPC_STREAM / IPC_FILE window of the sample batch; TABLE_LIST `[whole batch, window]` |
//! | `core.sampleTableExpected` | `{offset?, length?}` | JSON: the expected window, rendered from Arrow directly (below) |
//! | `core.raise` | `{kind, message?, context?}` | fails with that error kind (`SESSION` … `SDK_NOT_FOUND`) and context |
//! | `core.panic` | `{message?}` | panics; reported as INTERNAL `native panic: <message>` |
//! | `core.echo` | `{value?}` | `value` as the requested VOID / JSON / STRING / BOOL / HANDLE, or the sample for table kinds |
//! | `core.sleep` (async only) | `{millis}` | VOID after the delay; cancellable |
//! | `core.callsInFlight` | `{}` | JSON number of async calls still running |
//! | `core.liveEngines` | `{}` | JSON number of engines not yet torn down (registered, held by a call or a subscription, or still stopping) |
//!
//! The sample batch has 11 rows and one column per transport type, nulls,
//! extreme values and timestamp zones, plus schema metadata with every decoded
//! `xbbg.*` key (a version-1 field-exception envelope included). The default
//! window starts at row 3, so every column is sliced: bitmaps start at a bit
//! offset and variable-width offsets need rebasing.
//!
//! `core.sampleTableExpected` returns
//! `{"numRows", "columns": [{"name", "type", "nullable", "timezone", "values"}], "metadata": {..}, "decodedMetadata": {..}}`
//! where `values` holds one string (or `null`) per row: booleans
//! `true`/`false`; integers in decimal (unsigned as unsigned); floats as the
//! lowercase hex of their IEEE bits (8 or 16 digits); UTF-8 text verbatim;
//! binary as lowercase hex; dates, times and timestamps as their raw integer.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, NullArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong};
use jni::{Env, EnvUnowned};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use xbbg_host::errors::{HostError, HostErrorKind, parse_args};

use crate::bridge::{NativeError, ThrowNative};
use crate::payload::{Payload, ResultKind};
use crate::table::TypeCode;
use crate::{recipes, requests, subscription_fixture};

/// Rows in the sample batch.
const SAMPLE_ROWS: usize = 11;
/// Default first row of the sample window.
const SAMPLE_OFFSET: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Area {
    Core,
    Requests,
    Recipes,
    Fixture,
}

fn route(method: &str) -> Result<(Area, &str), HostError> {
    [
        ("core.", Area::Core),
        ("requests.", Area::Requests),
        ("recipes.", Area::Recipes),
        ("fixture.", Area::Fixture),
    ]
    .into_iter()
    .find_map(|(prefix, area)| method.strip_prefix(prefix).map(|name| (area, name)))
    .ok_or_else(|| HostError::unknown_method("test-support", method))
}

fn dispatch(method: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    let (area, name) = route(method)?;
    match area {
        Area::Core => core_call(name, args, kind),
        Area::Requests => requests::test_call(name, args, kind),
        Area::Recipes => recipes::test_call(name, args, kind),
        Area::Fixture => subscription_fixture::call(name, args, kind),
    }
}

async fn dispatch_async(method: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    let (area, name) = route(method)?;
    match area {
        Area::Core if name == "sleep" => sleep(args).await,
        Area::Core => core_call(name, args, kind),
        Area::Requests => requests::test_call_async(name, args, kind).await,
        Area::Recipes => recipes::test_call_async(name, args, kind).await,
        Area::Fixture => subscription_fixture::call_async(name, args, kind).await,
    }
}

#[unsafe(no_mangle)]
extern "system" fn Java_io_github_underloam_xbbg_internal_NativeTestSupport_call<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
) -> JObject<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let method = crate::string_arg(env, &method, "method")?;
        let args = crate::args_arg(env, &args)?;
        let kind = ResultKind::from_code(result_kind)?;
        let payload = dispatch(&method, &args, kind)?;
        payload.into_java(env, kind)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
extern "system" fn Java_io_github_underloam_xbbg_internal_NativeTestSupport_callAsync<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
    token: jlong,
) {
    env.with_env(|env: &mut Env<'local>| -> Result<_, NativeError> {
        let Some((method, args)) = crate::read_or_fail(env, token, |env| {
            Ok((
                crate::string_arg(env, &method, "method")?,
                crate::args_arg(env, &args)?,
            ))
        })?
        else {
            return Ok(());
        };
        crate::spawn_checked(token, result_kind, move |kind| async move {
            dispatch_async(&method, &args, kind).await
        });
        Ok(())
    })
    .resolve::<ThrowNative>()
}

fn core_call(name: &str, args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    match name {
        "sampleTable" => {
            let window: Window = parse_args(args)?;
            let batch = window.apply(&sample_batch())?;
            if kind == ResultKind::TableList {
                return Ok(Payload::TableList(vec![sample_batch(), batch]));
            }
            Payload::table(batch, kind)
        }
        "sampleTableExpected" => {
            kind.expect(ResultKind::Json, "core.sampleTableExpected")?;
            let window: Window = parse_args(args)?;
            Payload::json(&expected(&window.apply(&sample_batch())?))
        }
        "raise" => Err(raise(args)?),
        "panic" => {
            let args: PanicArgs = parse_args(args)?;
            panic!(
                "{}",
                args.message
                    .as_deref()
                    .unwrap_or("synthetic test-support panic")
            );
        }
        "echo" => echo(args, kind),
        "callsInFlight" => {
            kind.expect(ResultKind::Json, "core.callsInFlight")?;
            parse_args::<NoArgs>(args)?;
            Payload::json(&crate::bridge::calls_in_flight())
        }
        "liveEngines" => {
            kind.expect(ResultKind::Json, "core.liveEngines")?;
            parse_args::<NoArgs>(args)?;
            Payload::json(&crate::engine::ENGINES.live())
        }
        "sleep" => Err(HostError::internal(
            "core.sleep is async only; call it with callAsync",
        )),
        other => Err(HostError::unknown_method(
            "test-support",
            &format!("core.{other}"),
        )),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Window {
    offset: Option<usize>,
    length: Option<usize>,
}

impl Window {
    fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, HostError> {
        let offset = self.offset.unwrap_or(SAMPLE_OFFSET);
        let rows = batch.num_rows();
        let length = self.length.unwrap_or(rows.saturating_sub(offset));
        if offset.checked_add(length).is_none_or(|end| end > rows) {
            return Err(HostError::argument(format!(
                "window {offset}+{length} exceeds the {rows}-row sample"
            )));
        }
        Ok(batch.slice(offset, length))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PanicArgs {
    message: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RaiseArgs {
    kind: String,
    message: Option<String>,
    context: Option<RaiseContext>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RaiseContext {
    service: Option<String>,
    operation: Option<String>,
    request_id: Option<String>,
    #[serde(default)]
    bloomberg_request_ids: Vec<String>,
    topic: Option<String>,
    detail: Option<String>,
    element: Option<String>,
    suggestion: Option<String>,
    code: Option<String>,
}

/// The error `core.raise` reports.
fn raise(args: &str) -> Result<HostError, HostError> {
    let args: RaiseArgs = parse_args(args)?;
    let kind = HostErrorKind::from_name(&args.kind).ok_or_else(|| {
        HostError::argument(format!("unknown error kind '{}'", args.kind)).with_element("kind")
    })?;
    let message = args
        .message
        .unwrap_or_else(|| format!("synthetic {} error", kind.name()));
    let context = args.context.unwrap_or_default();
    let mut error = HostError::new(kind, message);
    error.context.service = context.service;
    error.context.operation = context.operation;
    error.context.request_id = context.request_id;
    error.context.bloomberg_request_ids = context.bloomberg_request_ids;
    error.context.topic = context.topic;
    error.context.detail = context.detail;
    error.context.element = context.element;
    error.context.suggestion = context.suggestion;
    error.context.code = context.code;
    Ok(error)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EchoArgs {
    #[serde(default)]
    value: Value,
}

fn echo(args: &str, kind: ResultKind) -> Result<Payload, HostError> {
    let args: EchoArgs = parse_args(args)?;
    let wrong = |expected: &str| {
        HostError::argument(format!(
            "core.echo {} needs a {expected} value",
            kind.name()
        ))
    };
    match kind {
        ResultKind::Void => Ok(Payload::Void),
        ResultKind::Json => Ok(Payload::Json(args.value.to_string())),
        ResultKind::Str => args
            .value
            .as_str()
            .map(|text| Payload::Str(text.to_owned()))
            .ok_or_else(|| wrong("string")),
        ResultKind::Bool => args
            .value
            .as_bool()
            .map(Payload::Bool)
            .ok_or_else(|| wrong("boolean")),
        ResultKind::Handle => args
            .value
            .as_u64()
            .filter(|id| i64::try_from(*id).is_ok())
            .map(Payload::Handle)
            .ok_or_else(|| wrong("non-negative long")),
        ResultKind::Table | ResultKind::IpcStream | ResultKind::IpcFile => {
            Payload::table(sample_batch(), kind)
        }
        ResultKind::TableList => Ok(Payload::TableList(vec![
            sample_batch(),
            Window {
                offset: None,
                length: None,
            }
            .apply(&sample_batch())?,
        ])),
        other => Err(HostError::internal(format!(
            "core.echo cannot produce {}",
            other.name()
        ))),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SleepArgs {
    millis: u64,
}

async fn sleep(args: &str) -> Result<Payload, HostError> {
    let args: SleepArgs = parse_args(args)?;
    tokio::time::sleep(Duration::from_millis(args.millis)).await;
    Ok(Payload::Void)
}

/// Row `r` is null for this column when `pattern` says so.
fn null_at(row: usize, every: usize, phase: usize) -> bool {
    row % every == phase
}

fn optional<T>(row: usize, every: usize, phase: usize, value: T) -> Option<T> {
    (!null_at(row, every, phase)).then_some(value)
}

/// The 11-row sample batch: one column per transport type.
pub(crate) fn sample_batch() -> RecordBatch {
    let rows = 0..SAMPLE_ROWS;
    let i8s = [i8::MIN, -1, 0, 1, i8::MAX];
    let i16s = [i16::MIN, -300, 0, 300, i16::MAX];
    let i32s = [i32::MIN, -70_000, 0, 70_000, i32::MAX];
    let i64s = [i64::MIN, -5_000_000_000, 0, 5_000_000_000, i64::MAX];
    let f32s = [
        f32::from_bits(0x7fc0_0001),
        -0.0,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::MIN_POSITIVE,
        1.5,
        -3.25e30,
    ];
    let f64s = [
        f64::from_bits(0x7ff8_0000_0000_0001),
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        0.1,
        -1.0e300,
    ];
    let texts = [
        "",
        "IBM US Equity",
        "π €",
        "emoji 😀",
        "nul\u{0}inside",
        "tab\tline\n",
    ];
    let blobs: [&[u8]; 4] = [b"", b"\x00", b"\xff\x00\x7f", b"xbbg"];
    let dates = [-719_162, -1, 0, 19_000, 2_932_896];
    let pick = |row: usize, len: usize| row % len;

    let columns: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("ticker", DataType::Utf8, false),
            Arc::new(StringArray::from_iter_values(
                rows.clone().map(|row| format!("SEC{row} Equity")),
            )),
        ),
        (
            Field::new("null", DataType::Null, true),
            Arc::new(NullArray::new(SAMPLE_ROWS)),
        ),
        (
            Field::new("bool", DataType::Boolean, true),
            Arc::new(BooleanArray::from_iter(
                rows.clone().map(|row| optional(row, 5, 4, row % 3 == 0)),
            )),
        ),
        (
            Field::new("int8", DataType::Int8, true),
            Arc::new(Int8Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 2, i8s[pick(row, 5)])),
            )),
        ),
        (
            Field::new("int16", DataType::Int16, true),
            Arc::new(Int16Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 1, i16s[pick(row, 5)])),
            )),
        ),
        (
            Field::new("int32", DataType::Int32, true),
            Arc::new(Int32Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 0, i32s[pick(row, 5)])),
            )),
        ),
        (
            Field::new("int64", DataType::Int64, true),
            Arc::new(Int64Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 3, i64s[pick(row, 5)])),
            )),
        ),
        (
            Field::new("uint8", DataType::UInt8, true),
            Arc::new(UInt8Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 6, 5, [0, 1, u8::MAX][pick(row, 3)])),
            )),
        ),
        (
            Field::new("uint16", DataType::UInt16, true),
            Arc::new(UInt16Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 6, 4, [0, 1, u16::MAX][pick(row, 3)])),
            )),
        ),
        (
            Field::new("uint32", DataType::UInt32, true),
            Arc::new(UInt32Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 6, 3, [0, 1, u32::MAX][pick(row, 3)])),
            )),
        ),
        (
            Field::new("uint64", DataType::UInt64, true),
            Arc::new(UInt64Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 6, 2, [0, 1, u64::MAX][pick(row, 3)])),
            )),
        ),
        (
            Field::new("float32", DataType::Float32, true),
            Arc::new(Float32Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 8, 7, f32s[pick(row, 7)])),
            )),
        ),
        (
            Field::new("float64", DataType::Float64, true),
            Arc::new(Float64Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 8, 6, f64s[pick(row, 7)])),
            )),
        ),
        (
            Field::new("utf8", DataType::Utf8, true),
            Arc::new(StringArray::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 2, texts[pick(row, 6)])),
            )),
        ),
        (
            Field::new("large_utf8", DataType::LargeUtf8, true),
            Arc::new(LargeStringArray::from_iter(
                rows.clone()
                    .map(|row| optional(row, 5, 0, texts[pick(row + 1, 6)])),
            )),
        ),
        (
            Field::new("binary", DataType::Binary, true),
            Arc::new(BinaryArray::from_iter(
                rows.clone()
                    .map(|row| optional(row, 3, 1, blobs[pick(row, 4)])),
            )),
        ),
        (
            Field::new("large_binary", DataType::LargeBinary, true),
            Arc::new(LargeBinaryArray::from_iter(
                rows.clone()
                    .map(|row| optional(row, 3, 2, blobs[pick(row + 2, 4)])),
            )),
        ),
        (
            Field::new("date32", DataType::Date32, true),
            Arc::new(Date32Array::from_iter(
                rows.clone()
                    .map(|row| optional(row, 4, 3, dates[pick(row, 5)])),
            )),
        ),
        (
            Field::new("date64", DataType::Date64, true),
            Arc::new(Date64Array::from_iter(rows.clone().map(|row| {
                optional(row, 4, 0, i64::from(dates[pick(row, 5)]) * 86_400_000)
            }))),
        ),
        (
            Field::new("time32_s", DataType::Time32(TimeUnit::Second), true),
            Arc::new(Time32SecondArray::from_iter(rows.clone().map(|row| {
                optional(row, 5, 1, [0, 1, 43_200, 86_399][pick(row, 4)])
            }))),
        ),
        (
            Field::new("time32_ms", DataType::Time32(TimeUnit::Millisecond), true),
            Arc::new(Time32MillisecondArray::from_iter(rows.clone().map(|row| {
                optional(row, 5, 2, [0, 1, 43_200_000, 86_399_999][pick(row, 4)])
            }))),
        ),
        (
            Field::new("time64_us", DataType::Time64(TimeUnit::Microsecond), true),
            Arc::new(Time64MicrosecondArray::from_iter(rows.clone().map(|row| {
                optional(
                    row,
                    5,
                    3,
                    [0, 1, 43_200_000_000, 86_399_999_999][pick(row, 4)],
                )
            }))),
        ),
        (
            Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), true),
            Arc::new(Time64NanosecondArray::from_iter(rows.clone().map(|row| {
                optional(
                    row,
                    5,
                    4,
                    [0, 1, 43_200_000_000_000, 86_399_999_999_999][pick(row, 4)],
                )
            }))),
        ),
        (
            Field::new(
                "timestamp_s",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            Arc::new(
                TimestampSecondArray::from_iter(rows.clone().map(|row| {
                    optional(row, 6, 0, [-62_135_596_800, 0, 1_700_000_000][pick(row, 3)])
                }))
                .with_timezone("UTC"),
            ),
        ),
        (
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
                true,
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter(
                    rows.clone()
                        .map(|row| optional(row, 6, 1, [-1, 0, 1_700_000_000_123][pick(row, 3)])),
                )
                .with_timezone("America/New_York"),
            ),
        ),
        (
            Field::new(
                "timestamp_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Tokyo".into())),
                true,
            ),
            Arc::new(
                TimestampMicrosecondArray::from_iter(
                    rows.clone().map(|row| {
                        optional(row, 6, 2, [-1, 0, 1_700_000_000_123_456][pick(row, 3)])
                    }),
                )
                .with_timezone("Asia/Tokyo"),
            ),
        ),
        (
            Field::new(
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Arc::new(TimestampNanosecondArray::from_iter(rows.map(|row| {
                optional(row, 6, 3, [i64::MIN, 0, i64::MAX][pick(row, 3)])
            }))),
        ),
    ];
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
    let schema = Schema::new(fields).with_metadata(sample_metadata());
    RecordBatch::try_new(Arc::new(schema), arrays).expect("the sample batch is consistent")
}

fn sample_metadata() -> HashMap<String, String> {
    let envelope = json!({
        "version": 1,
        "entries": [
            {"field": "BAD_FLD", "category": "BAD_FLD", "code": 9, "subcategory": "INVALID_FIELD", "message": "Field not valid"},
            {"field": "PX_LAST", "category": "NOT_APPLICABLE", "code": 4, "subcategory": "NA", "message": "Not applicable"}
        ],
        "securities": {"SEC3 Equity": [0, 1, 0], "SEC4 Equity": [1]}
    });
    HashMap::from([
        (
            "xbbg.bloomberg_request_ids".to_string(),
            r#"["sdk-a","sdk-b"]"#.to_string(),
        ),
        ("xbbg.field_exceptions".to_string(), envelope.to_string()),
        (
            "xbbg.eid_data".to_string(),
            r#"{"SEC3 Equity":[14005,35009]}"#.to_string(),
        ),
        (
            "xbbg.delayed".to_string(),
            r#"{"SEC3 Equity":{"delayed":true,"delay_minutes":15}}"#.to_string(),
        ),
        (
            "xbbg.security_errors".to_string(),
            r#"{"BAD Equity":{"category":"BAD_SEC","code":43,"subcategory":"INVALID_SECURITY","message":"Unknown security"}}"#
                .to_string(),
        ),
        (
            "xbbg.type_misses".to_string(),
            r#"{"PX_LAST":2}"#.to_string(),
        ),
        (
            "xbbg.field_errors".to_string(),
            r#"{"BAD_FLD":{"category":"BAD_FLD","code":9,"subcategory":"NOT_FOUND","message":"Unknown field","source":"apiflds"}}"#
                .to_string(),
        ),
        ("sample.note".to_string(), "kept verbatim".to_string()),
    ])
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedColumn {
    name: String,
    #[serde(rename = "type")]
    type_code: i8,
    nullable: bool,
    timezone: Option<String>,
    values: Vec<Option<String>>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Render every cell of `array` with Arrow's typed accessors (independent of
/// the transport canonicalization it is compared against).
fn render(array: &dyn Array) -> Vec<Option<String>> {
    macro_rules! primitive {
        ($type:ty, $format:expr) => {{
            let typed = array.as_primitive::<$type>();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| $format(typed.value(row))))
                .collect()
        }};
    }
    match array.data_type() {
        DataType::Null => vec![None; array.len()],
        DataType::Boolean => {
            let typed = array.as_boolean();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| typed.value(row).to_string()))
                .collect()
        }
        DataType::Int8 => primitive!(Int8Type, |value: i8| value.to_string()),
        DataType::Int16 => primitive!(Int16Type, |value: i16| value.to_string()),
        DataType::Int32 => primitive!(Int32Type, |value: i32| value.to_string()),
        DataType::Int64 => primitive!(Int64Type, |value: i64| value.to_string()),
        DataType::UInt8 => primitive!(UInt8Type, |value: u8| value.to_string()),
        DataType::UInt16 => primitive!(UInt16Type, |value: u16| value.to_string()),
        DataType::UInt32 => primitive!(UInt32Type, |value: u32| value.to_string()),
        DataType::UInt64 => primitive!(UInt64Type, |value: u64| value.to_string()),
        DataType::Float32 => {
            primitive!(Float32Type, |value: f32| format!("{:08x}", value.to_bits()))
        }
        DataType::Float64 => {
            primitive!(Float64Type, |value: f64| format!(
                "{:016x}",
                value.to_bits()
            ))
        }
        DataType::Date32 => primitive!(Date32Type, |value: i32| value.to_string()),
        DataType::Date64 => primitive!(Date64Type, |value: i64| value.to_string()),
        DataType::Time32(TimeUnit::Second) => {
            primitive!(Time32SecondType, |value: i32| value.to_string())
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive!(Time32MillisecondType, |value: i32| value.to_string())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive!(Time64MicrosecondType, |value: i64| value.to_string())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            primitive!(Time64NanosecondType, |value: i64| value.to_string())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            primitive!(TimestampSecondType, |value: i64| value.to_string())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondType, |value: i64| value.to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondType, |value: i64| value.to_string())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondType, |value: i64| value.to_string())
        }
        DataType::Utf8 => {
            let typed = array.as_string::<i32>();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| typed.value(row).to_owned()))
                .collect()
        }
        DataType::LargeUtf8 => {
            let typed = array.as_string::<i64>();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| typed.value(row).to_owned()))
                .collect()
        }
        DataType::Binary => {
            let typed = array.as_binary::<i32>();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| hex(typed.value(row))))
                .collect()
        }
        DataType::LargeBinary => {
            let typed = array.as_binary::<i64>();
            (0..typed.len())
                .map(|row| typed.is_valid(row).then(|| hex(typed.value(row))))
                .collect()
        }
        other => unreachable!("the sample has no {other:?} column"),
    }
}

/// The expected Java view of `batch` (see the module docs).
fn expected(batch: &RecordBatch) -> Value {
    let schema = batch.schema_ref();
    let columns: Vec<ExpectedColumn> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            let (code, timezone) =
                TypeCode::of(field.data_type()).expect("the sample uses transport types only");
            ExpectedColumn {
                name: field.name().clone(),
                type_code: code as i8,
                nullable: field.is_nullable(),
                timezone,
                values: render(array.as_ref()),
            }
        })
        .collect();
    let metadata: BTreeMap<&String, &String> = schema.metadata().iter().collect();
    let decoded: Value =
        serde_json::from_str(&xbbg_host::metadata::decode(schema.metadata()).to_json())
            .expect("decoded metadata is JSON");
    json!({
        "numRows": batch.num_rows(),
        "columns": columns,
        "metadata": metadata,
        "decodedMetadata": decoded,
    })
}

#[cfg(test)]
mod tests {
    use crate::table::{Bytes, TableTransport};

    use super::*;

    fn window(offset: Option<usize>, length: Option<usize>) -> Result<RecordBatch, HostError> {
        Window { offset, length }.apply(&sample_batch())
    }

    #[test]
    fn routing_strips_known_prefixes() {
        assert_eq!(route("core.raise").unwrap(), (Area::Core, "raise"));
        assert_eq!(route("requests.plan").unwrap(), (Area::Requests, "plan"));
        assert_eq!(route("recipes.bqr").unwrap(), (Area::Recipes, "bqr"));
        assert_eq!(
            route("fixture.publish").unwrap(),
            (Area::Fixture, "publish")
        );
        let error = route("other.thing").unwrap_err();
        assert_eq!(error.message, "unknown test-support method 'other.thing'");
    }

    #[test]
    fn the_sample_covers_every_transport_type() {
        let batch = sample_batch();
        let mut codes: Vec<i8> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| TypeCode::of(field.data_type()).unwrap().0 as i8)
            .collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes, (0..26).collect::<Vec<i8>>());
        for array in batch.columns() {
            if array.data_type() != &DataType::Null && array.data_type() != &DataType::Utf8 {
                assert!(array.null_count() > 0, "{:?} has nulls", array.data_type());
            }
        }
    }

    #[test]
    fn the_default_window_is_sliced_and_canonicalized() {
        let sliced = window(None, None).unwrap();
        assert_eq!(sliced.num_rows(), SAMPLE_ROWS - SAMPLE_OFFSET);
        let transport = TableTransport::new(&sliced).unwrap();
        let bool_column = sliced.schema().index_of("bool").unwrap();
        let expected_bits: Vec<Option<String>> = render(sliced.column(bool_column).as_ref());
        let packed = transport.values[bool_column].as_ref().unwrap().as_slice();
        let validity = transport.validity[bool_column].as_ref().unwrap().as_slice();
        for (row, value) in expected_bits.iter().enumerate() {
            let valid = validity[row / 8] >> (row % 8) & 1 == 1;
            assert_eq!(valid, value.is_some(), "row {row}");
            if let Some(value) = value {
                let bit = packed[row / 8] >> (row % 8) & 1 == 1;
                assert_eq!(bit.to_string(), *value, "row {row}");
            }
        }
        let utf8 = sliced.schema().index_of("utf8").unwrap();
        assert!(matches!(transport.offsets[utf8], Some(Bytes::Owned(_))));
    }

    #[test]
    fn windows_outside_the_sample_are_argument_errors() {
        assert_eq!(window(Some(0), None).unwrap().num_rows(), SAMPLE_ROWS);
        assert_eq!(window(Some(SAMPLE_ROWS), None).unwrap().num_rows(), 0);
        let error = window(Some(5), Some(7)).unwrap_err();
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert_eq!(error.message, "window 5+7 exceeds the 11-row sample");
        assert!(window(Some(usize::MAX), Some(2)).is_err());
    }

    #[test]
    fn expected_rendering_uses_exact_bits_and_raw_integers() {
        let expected = expected(&window(Some(0), Some(6)).unwrap());
        let column = |name: &str| {
            expected["columns"]
                .as_array()
                .unwrap()
                .iter()
                .find(|column| column["name"] == name)
                .unwrap()
                .clone()
        };
        assert_eq!(expected["numRows"], 6);
        assert_eq!(column("float32")["values"][0], "7fc00001");
        assert_eq!(column("float64")["values"][1], "8000000000000000");
        assert_eq!(column("uint64")["values"][5], u64::MAX.to_string());
        assert_eq!(column("uint64")["values"][2], Value::Null);
        assert_eq!(column("int8")["values"][0], "-128");
        assert_eq!(column("int8")["values"][2], Value::Null);
        assert_eq!(column("binary")["values"][0], "");
        assert_eq!(column("binary")["values"][1], Value::Null);
        assert_eq!(column("binary")["values"][2], "ff007f");
        assert_eq!(
            column("null")["values"],
            json!([null, null, null, null, null, null])
        );
        assert_eq!(column("timestamp_ms")["timezone"], "America/New_York");
        assert_eq!(column("timestamp_ns")["timezone"], Value::Null);
        assert_eq!(column("ticker")["nullable"], false);
        assert_eq!(column("date32")["type"], 16);
        assert_eq!(
            expected["decodedMetadata"]["fieldExceptions"]["SEC3 Equity"][1]["field"],
            "PX_LAST"
        );
        assert_eq!(expected["metadata"]["sample.note"], "kept verbatim");
    }

    #[test]
    fn raise_builds_each_kind_with_context() {
        for kind in HostErrorKind::ALL {
            let error = raise(&format!(
                r#"{{"kind":"{}","context":{{"service":"//blp/refdata","bloombergRequestIds":["a"],"topic":"T"}}}}"#,
                kind.name()
            ))
            .unwrap();
            assert_eq!(error.kind, kind);
            assert_eq!(error.message, format!("synthetic {} error", kind.name()));
            assert_eq!(
                error.context_json().as_deref(),
                Some(r#"{"service":"//blp/refdata","bloombergRequestIds":["a"],"topic":"T"}"#)
            );
        }
        let error = raise(r#"{"kind":"NOPE"}"#).unwrap_err();
        assert_eq!(error.kind, HostErrorKind::Argument);
        assert_eq!(error.context.element.as_deref(), Some("kind"));
        let plain = raise(r#"{"kind":"STATE","message":"engine is closed"}"#).unwrap();
        assert_eq!(plain.context_json(), None);
    }

    #[test]
    fn echo_produces_each_scalar_kind() {
        assert!(matches!(
            echo(r#"{"value":"x"}"#, ResultKind::Str),
            Ok(Payload::Str(text)) if text == "x"
        ));
        assert!(matches!(
            echo(r#"{"value":true}"#, ResultKind::Bool),
            Ok(Payload::Bool(true))
        ));
        assert!(matches!(
            echo(r#"{"value":7}"#, ResultKind::Handle),
            Ok(Payload::Handle(7))
        ));
        assert!(matches!(
            echo(r#"{"value":{"a":[1]}}"#, ResultKind::Json),
            Ok(Payload::Json(text)) if text == r#"{"a":[1]}"#
        ));
        assert!(matches!(echo("{}", ResultKind::Void), Ok(Payload::Void)));
        assert!(matches!(
            echo("{}", ResultKind::TableList),
            Ok(Payload::TableList(batches)) if batches.len() == 2
        ));
        let error = echo(r#"{"value":-1}"#, ResultKind::Handle).unwrap_err();
        assert_eq!(
            error.message,
            "core.echo HANDLE needs a non-negative long value"
        );
        assert!(echo("{}", ResultKind::Ticks).is_err());
    }

    #[test]
    fn core_sleep_requires_the_async_entry_point() {
        let error = core_call("sleep", r#"{"millis":1}"#, ResultKind::Void).unwrap_err();
        assert_eq!(error.kind, HostErrorKind::Internal);
        assert_eq!(
            core_call("nope", "{}", ResultKind::Void)
                .unwrap_err()
                .message,
            "unknown test-support method 'core.nope'"
        );
    }
}
