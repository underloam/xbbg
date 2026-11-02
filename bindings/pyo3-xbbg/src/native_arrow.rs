use std::cell::OnceCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, Float16Array, Float32Array, Float64Array, GenericStringArray, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, OffsetSizeTrait,
    RecordBatch, RecordBatchOptions, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Offset, Timelike};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{
    PyImportError, PyIndexError, PyKeyError, PyOverflowError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::intern;
use pyo3::marker::Ungil;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyCapsule, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt,
    PyIterator, PyList, PyMapping, PyString, PyTime, PyTuple, PyType, PyTzInfo,
};
use pyo3_arrow::ffi::{
    ArrayIterator, to_array_pycapsules, to_schema_pycapsule, to_stream_pycapsule,
};
use pyo3_arrow::{PyChunkedArray, PyRecordBatch, PyTable};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_arrow::scalar::{date_from_cell, date_from_days};
use xbbg_arrow::{
    ArrowCoreError, CellValue, ColumnData, SortDirection, TableData, build_array, cell_from_array,
};
use xbbg_async::diagnostics::{
    FieldExceptions, METADATA_KEY_FIELD_ERRORS, METADATA_KEY_FIELD_EXCEPTIONS,
};
use xbbg_async::engine::state::{
    METADATA_KEY_BLOOMBERG_REQUEST_ID, METADATA_KEY_BLOOMBERG_REQUEST_IDS, METADATA_KEY_DELAYED,
    METADATA_KEY_EID_DATA, METADATA_KEY_SECURITY_ERRORS, METADATA_KEY_TYPE_MISSES,
};

use crate::shaping::reshape_bqr_generic_table;

pub(crate) fn core_err_to_py(err: ArrowCoreError) -> PyErr {
    match err {
        ArrowCoreError::UnknownColumn(name) => {
            PyKeyError::new_err(format!("unknown column: {name}"))
        }
        ArrowCoreError::ColumnIndexOutOfRange => PyIndexError::new_err("column index out of range"),
        ArrowCoreError::RowIndexOutOfRange => PyIndexError::new_err("row index out of range"),
        other => PyValueError::new_err(other.to_string()),
    }
}

fn map_core<T>(result: xbbg_arrow::Result<T>) -> PyResult<T> {
    result.map_err(core_err_to_py)
}

fn serde_json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(value) => {
            Ok(PyBool::new(py, *value).to_owned().into_any().unbind())
        }
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = value.as_u64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = value.as_f64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyValueError::new_err("unsupported JSON number"))
            }
        }
        serde_json::Value::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(serde_json_value_to_py(py, value)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, serde_json_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Schema-metadata JSON values at least this long are parsed detached.
///
/// Typical diagnostics metadata is well under a kilobyte and parses in
/// microseconds, less than re-acquiring a contended interpreter can cost (up
/// to one switch interval, 5 ms by default). Metadata has no size cap, though:
/// diagnostics grow with securities x failing fields. At serde_json's parse
/// rate, 64 KiB takes on the order of 100 us, the point where parsing starts
/// to outweigh the reattach cost.
const DETACHED_METADATA_PARSE_MIN_BYTES: usize = 64 * 1024;

fn parse_metadata_json(
    py: Python<'_>,
    metadata: &HashMap<String, String>,
    key: &str,
) -> PyResult<Py<PyAny>> {
    let Some(raw) = metadata.get(key) else {
        return Ok(py.None());
    };
    let parse = || serde_json::from_str::<serde_json::Value>(raw);
    let value = if raw.len() >= DETACHED_METADATA_PARSE_MIN_BYTES {
        py.detach(parse)
    } else {
        parse()
    }
    .map_err(|err| PyValueError::new_err(format!("invalid JSON in {key}: {err}")))?;
    serde_json_value_to_py(py, &value)
}

fn metadata_sizes(metadata: &HashMap<String, String>) -> HashMap<String, usize> {
    metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.len()))
        .collect()
}

fn metadata_value<'a>(
    metadata: &'a HashMap<String, String>,
    key: &str,
    max_bytes: Option<usize>,
) -> Option<&'a str> {
    let value = metadata.get(key)?.as_str();
    let Some(max_bytes) = max_bytes.filter(|max_bytes| *max_bytes < value.len()) else {
        return Some(value);
    };
    let end = (0..=max_bytes)
        .rev()
        .find(|end| value.is_char_boundary(*end))
        .unwrap_or(0);
    Some(&value[..end])
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _decode_field_exceptions(py: Python<'_>, raw: &str) -> PyResult<Py<PyAny>> {
    // Decoding (including large historical maps) is pure Rust; only the
    // expanded Python objects below need the GIL.
    let exceptions = py
        .detach(|| FieldExceptions::from_json(raw))
        .map_err(|err| {
            PyValueError::new_err(format!(
                "invalid {METADATA_KEY_FIELD_EXCEPTIONS} metadata: {err}"
            ))
        })?;
    let securities = PyDict::new(py);
    for ticker in exceptions.securities() {
        let values = PyList::empty(py);
        for exception in exceptions.for_security(ticker) {
            let value = PyDict::new(py);
            value.set_item("field", &exception.field)?;
            value.set_item("category", &exception.category)?;
            value.set_item("code", exception.code)?;
            value.set_item("subcategory", &exception.subcategory)?;
            value.set_item("message", &exception.message)?;
            values.append(value)?;
        }
        securities.set_item(ticker, values)?;
    }
    Ok(securities.into_any().unbind())
}

fn parse_field_exceptions_metadata(
    py: Python<'_>,
    metadata: &HashMap<String, String>,
) -> PyResult<Py<PyAny>> {
    match metadata.get(METADATA_KEY_FIELD_EXCEPTIONS) {
        Some(raw) => _decode_field_exceptions(py, raw),
        None => Ok(py.None()),
    }
}

fn ensure_optional_module(
    py: Python<'_>,
    module: &str,
    install: &str,
    context: &str,
) -> PyResult<()> {
    py.import(module).map(|_| ()).map_err(|err| {
        PyImportError::new_err(format!(
            "{context} requires the '{module}' package. Install it with `{install}`. Original import error: {err}"
        ))
    })
}

fn int64_overflow_error() -> PyErr {
    PyOverflowError::new_err("Python integer is outside the supported signed 64-bit Arrow range")
}

fn py_value_to_cell(value: &Bound<'_, PyAny>) -> PyResult<CellValue> {
    if value.is_none() {
        return Ok(CellValue::Null);
    }
    // Exact builtin scalars map straight to what the probing chain below
    // yields for them, without its failed extractions (each raises and fetches
    // a Python exception). Subclasses and every other type still probe.
    if let Ok(flag) = value.cast_exact::<PyBool>() {
        return Ok(CellValue::Bool(flag.is_true()));
    }
    if value.is_exact_instance_of::<PyInt>() {
        return value
            .extract::<i64>()
            .map(CellValue::Int)
            .map_err(|_| int64_overflow_error());
    }
    if let Ok(number) = value.cast_exact::<PyFloat>() {
        return Ok(CellValue::Float(number.value()));
    }
    if let Ok(text) = value.cast_exact::<PyString>()
        && let Ok(text) = text.to_str()
    {
        return Ok(CellValue::Text(text.to_owned()));
    }
    if let Ok(v) = value.extract::<bool>() {
        return Ok(CellValue::Bool(v));
    }
    if let Ok(v) = value.extract::<i64>() {
        return Ok(CellValue::Int(v));
    }
    if value.is_instance_of::<PyInt>() {
        return Err(int64_overflow_error());
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(CellValue::Float(v));
    }
    if let Ok(v) = value.extract::<String>() {
        return Ok(CellValue::Text(v));
    }
    if value.is_instance_of::<PyDateTime>() {
        let text = value.call_method0("isoformat")?.extract::<String>()?;
        if let Ok(datetime) = DateTime::parse_from_rfc3339(&text) {
            return Ok(CellValue::Timestamp(datetime.timestamp_micros()));
        }
        if let Ok(datetime) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.f") {
            return Ok(CellValue::Timestamp(datetime.and_utc().timestamp_micros()));
        }
        return Err(PyValueError::new_err(format!(
            "unsupported Python datetime representation: {text}"
        )));
    }
    if let Ok(isoformat) = value.getattr("isoformat")
        && let Ok(text) = isoformat.call0()?.extract::<String>()
    {
        if text.len() == 10
            && let Ok(date) = NaiveDate::parse_from_str(&text, "%Y-%m-%d")
        {
            return Ok(CellValue::Date(date));
        }
        return Ok(CellValue::Text(text));
    }
    Ok(CellValue::Text(value.str()?.to_string()))
}
fn filter_value_supported(data_type: &DataType, needle: &CellValue) -> bool {
    matches!(
        (data_type, needle),
        (_, CellValue::Null)
            | (DataType::Boolean, CellValue::Bool(_))
            | (
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64,
                CellValue::Int(_) | CellValue::Float(_)
            )
            | (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                CellValue::Text(_)
            )
            | (DataType::Date32 | DataType::Date64, CellValue::Date(_))
            | (
                DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
                    | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond),
                CellValue::Int(_)
            )
            | (
                DataType::Timestamp(_, _),
                CellValue::Timestamp(_) | CellValue::Int(_)
            )
    )
}

fn rows_to_batch(rows: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let (columns, row_count) = rows_to_columns(rows)?;
    rows.py()
        .detach(move || columns_to_batch(columns, row_count))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Named cell columns collected from Python dict rows.
type CellColumns = Vec<(String, Vec<CellValue>)>;

/// Collect dict rows into cell columns named in first-seen key order.
fn rows_to_columns(rows: &Bound<'_, PyAny>) -> PyResult<(CellColumns, usize)> {
    let iterator = PyIterator::from_object(rows)?;
    let mut columns = CellColumns::new();
    let mut column_indices: HashMap<String, usize> = HashMap::new();
    // Rows built alike repeat the same key objects in the same order, so the
    // key last seen at a dict position usually resolves its column by
    // identity. Holding the key keeps its address from being reused.
    let mut position_keys: Vec<(Bound<'_, PyAny>, usize)> = Vec::new();
    let mut row_count = 0usize;

    for item in iterator {
        let item = item?;
        let dict = item.cast::<PyDict>().map_err(|_| {
            PyTypeError::new_err("ArrowTable.from_pylist expects an iterable of dict rows")
        })?;
        let row_idx = row_count;

        for (position, (key, value)) in dict.iter().enumerate() {
            let column_idx = match position_keys.get(position) {
                Some((cached, idx)) if cached.is(&key) => *idx,
                _ => {
                    let name = key.extract::<String>()?;
                    let idx = if let Some(&idx) = column_indices.get(&name) {
                        idx
                    } else {
                        let idx = columns.len();
                        column_indices.insert(name.clone(), idx);
                        columns.push((name, vec![CellValue::Null; row_idx]));
                        idx
                    };
                    if let Some(slot) = position_keys.get_mut(position) {
                        *slot = (key, idx);
                    } else {
                        position_keys.push((key, idx));
                    }
                    idx
                }
            };
            columns[column_idx].1.push(py_value_to_cell(&value)?);
        }

        for (_, cells) in &mut columns {
            if cells.len() == row_idx {
                cells.push(CellValue::Null);
            }
        }
        row_count += 1;
    }
    Ok((columns, row_count))
}

/// Build the record batch for collected cell columns; pure Rust work.
fn columns_to_batch(columns: CellColumns, row_count: usize) -> Result<RecordBatch, ArrowError> {
    if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        return RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options);
    }
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns
        .iter()
        .map(|(name, cells)| build_array(name, cells))
        .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

fn py_sequence_to_cells(values: &Bound<'_, PyAny>) -> PyResult<Vec<CellValue>> {
    let iterator = PyIterator::from_object(values)?;
    iterator
        .map(|item| item.and_then(|value| py_value_to_cell(&value)))
        .collect()
}

/// Collect a column mapping into cell columns, in mapping order.
fn pydict_to_columns(columns: &Bound<'_, PyAny>) -> PyResult<(CellColumns, usize)> {
    let mapping = columns.cast::<PyMapping>().map_err(|_| {
        PyTypeError::new_err(
            "ArrowTable.from_pydict expects a mapping of column names to value sequences",
        )
    })?;
    let mut out = CellColumns::new();
    let mut row_count = None;
    for item in mapping.items()?.iter() {
        let (key, values) = item.extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
        let name = key.extract::<String>()?;
        if values.is_instance_of::<PyString>() || values.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err(format!(
                "ArrowTable.from_pydict column {name:?} must be a sequence of values"
            )));
        }
        let cells = py_sequence_to_cells(&values)?;
        match row_count {
            None => row_count = Some(cells.len()),
            Some(expected) if expected != cells.len() => {
                return Err(PyValueError::new_err(format!(
                    "ArrowTable.from_pydict column {name:?} has {} values; expected {expected}",
                    cells.len()
                )));
            }
            Some(_) => {}
        }
        out.push((name, cells));
    }
    Ok((out, row_count.unwrap_or(0)))
}

fn normalize_index(index: isize, len: usize, axis: &str) -> PyResult<usize> {
    let len = len as isize;
    let idx = if index < 0 { len + index } else { index };
    if !(0..len).contains(&idx) {
        return Err(PyIndexError::new_err(format!("{axis} index out of range")));
    }
    Ok(idx as usize)
}

fn column_index_arg(value: &Bound<'_, PyAny>, schema: &SchemaRef) -> PyResult<usize> {
    if let Ok(index) = value.extract::<isize>() {
        return normalize_index(index, schema.fields().len(), "column");
    }
    if let Ok(name) = value.extract::<String>() {
        return schema
            .index_of(&name)
            .map_err(|_| PyKeyError::new_err(format!("unknown column: {name}")));
    }
    Err(PyTypeError::new_err(
        "column selector must be an int or str",
    ))
}

fn column_indices_arg(value: &Bound<'_, PyAny>, schema: &SchemaRef) -> PyResult<Vec<usize>> {
    if value.extract::<String>().is_ok() || value.extract::<isize>().is_ok() {
        return Ok(vec![column_index_arg(value, schema)?]);
    }
    let iterator = PyIterator::from_object(value).map_err(|_| {
        PyTypeError::new_err("columns must be a sequence of column names or indices")
    })?;
    iterator
        .map(|item| item.and_then(|column| column_index_arg(&column, schema)))
        .collect()
}

fn date32_to_py(py: Python<'_>, days: i32) -> PyResult<Py<PyAny>> {
    let date = date_from_days(days).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Arrow date32 value {days} is outside Python's date range"
        ))
    })?;
    Ok(
        PyDate::new(py, date.year(), date.month() as u8, date.day() as u8)?
            .into_any()
            .unbind(),
    )
}

fn date64_to_py(py: Python<'_>, millis: i64) -> PyResult<Py<PyAny>> {
    let days = millis.div_euclid(86_400_000);
    let days = i32::try_from(days).map_err(|_| {
        PyValueError::new_err(format!(
            "Arrow date64 value {millis} is outside Python's date range"
        ))
    })?;
    date32_to_py(py, days)
}

/// Python types imported once per process rather than once per value.
static DATETIME_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static TIMEDELTA_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static TIMEZONE_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Largest `datetime.timedelta.days` magnitude Python accepts.
const MAX_TIMEDELTA_DAYS: i64 = 999_999_999;

/// Target zone of one timestamp column, looked up at most once per column.
enum TimestampZone<'a, 'py> {
    Utc,
    Named {
        name: &'a str,
        /// `zoneinfo.ZoneInfo(name)`, or `None` when zoneinfo cannot load it.
        zoneinfo: OnceCell<Option<Bound<'py, PyTzInfo>>>,
        /// chrono-tz zone for values zoneinfo does not convert.
        fallback: OnceCell<Option<chrono_tz::Tz>>,
    },
}

impl<'a> TimestampZone<'a, '_> {
    fn new(timezone: Option<&'a str>) -> Self {
        match timezone.filter(|tz| !tz.eq_ignore_ascii_case("UTC")) {
            None => Self::Utc,
            Some(name) => Self::Named {
                name,
                zoneinfo: OnceCell::new(),
                fallback: OnceCell::new(),
            },
        }
    }
}

fn timestamp_to_py<'py>(
    py: Python<'py>,
    micros: i64,
    zone: &TimestampZone<'_, 'py>,
) -> PyResult<Py<PyAny>> {
    let dt = DateTime::from_timestamp_micros(micros).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Arrow timestamp value {micros}us is outside Python's datetime range"
        ))
    })?;

    let utc = PyTzInfo::utc(py)?;
    let utc_datetime = PyDateTime::new(
        py,
        dt.year(),
        dt.month() as u8,
        dt.day() as u8,
        dt.hour() as u8,
        dt.minute() as u8,
        dt.second() as u8,
        dt.timestamp_subsec_micros(),
        Some(&utc),
    )?;

    let TimestampZone::Named {
        name,
        zoneinfo,
        fallback,
    } = zone
    else {
        return Ok(utc_datetime.into_any().unbind());
    };

    if let Some(zoneinfo) = zoneinfo.get_or_init(|| PyTzInfo::timezone(py, *name).ok())
        && let Ok(converted) = utc_datetime.call_method1(intern!(py, "astimezone"), (zoneinfo,))
    {
        return Ok(converted.unbind());
    }

    let tz = (*fallback.get_or_init(|| name.parse::<chrono_tz::Tz>().ok()))
        .ok_or_else(|| PyValueError::new_err(format!("unknown timestamp timezone: {name}")))?;
    let local = dt.with_timezone(&tz);
    let offset_seconds = local.offset().fix().local_minus_utc();
    let timedelta = TIMEDELTA_TYPE
        .import(py, "datetime", "timedelta")?
        .call1((0, offset_seconds))?;
    let fixed_tz = TIMEZONE_TYPE
        .import(py, "datetime", "timezone")?
        .call1((timedelta, *name))?;

    Ok(DATETIME_TYPE
        .import(py, "datetime", "datetime")?
        .call1((
            local.year(),
            local.month(),
            local.day(),
            local.hour(),
            local.minute(),
            local.second(),
            local.timestamp_subsec_micros(),
            fixed_tz,
        ))?
        .unbind())
}

fn timestamp_unit_to_py<'py>(
    py: Python<'py>,
    value: i64,
    unit: &TimeUnit,
    zone: &TimestampZone<'_, 'py>,
) -> PyResult<Py<PyAny>> {
    let micros = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value.div_euclid(1_000)),
    }
    .ok_or_else(|| {
        PyValueError::new_err(format!(
            "Arrow timestamp value {value} {unit:?} overflows microsecond conversion"
        ))
    })?;
    timestamp_to_py(py, micros, zone)
}

fn time_unit_to_py(py: Python<'_>, value: i64, unit: &TimeUnit) -> PyResult<Py<PyAny>> {
    let micros = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value.div_euclid(1_000)),
    }
    .ok_or_else(|| {
        PyValueError::new_err(format!(
            "Arrow time value {value} {unit:?} overflows microsecond conversion"
        ))
    })?;
    if !(0..86_400_000_000).contains(&micros) {
        return Err(PyValueError::new_err(format!(
            "Arrow time value {value} {unit:?} is outside Python's time range"
        )));
    }
    let seconds = micros / 1_000_000;
    let microsecond = (micros % 1_000_000) as u32;
    let hour = (seconds / 3_600) as u8;
    let minute = ((seconds % 3_600) / 60) as u8;
    let second = (seconds % 60) as u8;
    Ok(PyTime::new(py, hour, minute, second, microsecond, None)?
        .into_any()
        .unbind())
}

fn duration_to_py(py: Python<'_>, value: i64, unit: &TimeUnit) -> PyResult<Py<PyAny>> {
    let (seconds, micros) = match unit {
        TimeUnit::Second => (value, 0),
        TimeUnit::Millisecond => (value.div_euclid(1_000), value.rem_euclid(1_000) * 1_000),
        TimeUnit::Microsecond => (value.div_euclid(1_000_000), value.rem_euclid(1_000_000)),
        TimeUnit::Nanosecond => {
            let total_micros = value / 1_000;
            (
                total_micros.div_euclid(1_000_000),
                total_micros.rem_euclid(1_000_000),
            )
        }
    };
    let days = seconds.div_euclid(86_400);
    if (-MAX_TIMEDELTA_DAYS..=MAX_TIMEDELTA_DAYS).contains(&days) {
        // `timedelta(0, seconds, micros)` normalizes to exactly these parts.
        let delta = PyDelta::new(
            py,
            days as i32,
            seconds.rem_euclid(86_400) as i32,
            micros as i32,
            true,
        )?;
        return Ok(delta.into_any().unbind());
    }
    // Out of timedelta's range: Python raises its usual error.
    Ok(TIMEDELTA_TYPE
        .import(py, "datetime", "timedelta")?
        .call1((0, seconds, micros))?
        .unbind())
}

fn decimal_to_py(
    py: Python<'_>,
    unscaled: i128,
    scale: i8,
    scratch: &mut String,
) -> PyResult<Py<PyAny>> {
    scratch.clear();
    write!(scratch, "{unscaled}E{}", -i32::from(scale)).expect("writing to a String cannot fail");
    Ok(DECIMAL_TYPE
        .import(py, "decimal", "Decimal")?
        .call1((scratch.as_str(),))?
        .unbind())
}

/// The concrete array behind `array`, whose data type the caller matched.
fn typed<T: Array + 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("Arrow array matches its data type")
}

/// Values of one Arrow column, downcast once for Python materialization.
enum PyScalarValues<'a, 'py> {
    Null,
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float16(&'a Float16Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    BinaryView(&'a BinaryViewArray),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    Time32(&'a [i32], TimeUnit),
    Time64(&'a [i64], TimeUnit),
    Timestamp(&'a [i64], TimeUnit, TimestampZone<'a, 'py>),
    Duration(&'a [i64], TimeUnit),
    Decimal128(&'a Decimal128Array, i8),
    Unsupported,
}

/// One Arrow column resolved for Python materialization: the concrete array
/// and any timezone are looked up once, so each value only reads its slot.
struct PyScalarColumn<'a, 'py> {
    array: &'a dyn Array,
    values: PyScalarValues<'a, 'py>,
}

impl<'a, 'py> PyScalarColumn<'a, 'py> {
    fn new(array: &'a dyn Array) -> Self {
        use PyScalarValues as V;
        let values = match array.data_type() {
            DataType::Null => V::Null,
            DataType::Boolean => V::Boolean(typed(array)),
            DataType::Int8 => V::Int8(typed(array)),
            DataType::Int16 => V::Int16(typed(array)),
            DataType::Int32 => V::Int32(typed(array)),
            DataType::Int64 => V::Int64(typed(array)),
            DataType::UInt8 => V::UInt8(typed(array)),
            DataType::UInt16 => V::UInt16(typed(array)),
            DataType::UInt32 => V::UInt32(typed(array)),
            DataType::UInt64 => V::UInt64(typed(array)),
            DataType::Float16 => V::Float16(typed(array)),
            DataType::Float32 => V::Float32(typed(array)),
            DataType::Float64 => V::Float64(typed(array)),
            DataType::Utf8 => V::Utf8(typed(array)),
            DataType::LargeUtf8 => V::LargeUtf8(typed(array)),
            DataType::Utf8View => V::Utf8View(typed(array)),
            DataType::Binary => V::Binary(typed(array)),
            DataType::LargeBinary => V::LargeBinary(typed(array)),
            DataType::BinaryView => V::BinaryView(typed(array)),
            DataType::Date32 => V::Date32(typed(array)),
            DataType::Date64 => V::Date64(typed(array)),
            DataType::Time32(TimeUnit::Second) => V::Time32(
                &typed::<Time32SecondArray>(array).values()[..],
                TimeUnit::Second,
            ),
            DataType::Time32(TimeUnit::Millisecond) => V::Time32(
                &typed::<Time32MillisecondArray>(array).values()[..],
                TimeUnit::Millisecond,
            ),
            DataType::Time64(TimeUnit::Microsecond) => V::Time64(
                &typed::<Time64MicrosecondArray>(array).values()[..],
                TimeUnit::Microsecond,
            ),
            DataType::Time64(TimeUnit::Nanosecond) => V::Time64(
                &typed::<Time64NanosecondArray>(array).values()[..],
                TimeUnit::Nanosecond,
            ),
            DataType::Timestamp(unit, timezone) => {
                let values = match unit {
                    TimeUnit::Second => &typed::<TimestampSecondArray>(array).values()[..],
                    TimeUnit::Millisecond => {
                        &typed::<TimestampMillisecondArray>(array).values()[..]
                    }
                    TimeUnit::Microsecond => {
                        &typed::<TimestampMicrosecondArray>(array).values()[..]
                    }
                    TimeUnit::Nanosecond => &typed::<TimestampNanosecondArray>(array).values()[..],
                };
                V::Timestamp(values, *unit, TimestampZone::new(timezone.as_deref()))
            }
            DataType::Duration(unit) => {
                let values = match unit {
                    TimeUnit::Second => &typed::<DurationSecondArray>(array).values()[..],
                    TimeUnit::Millisecond => &typed::<DurationMillisecondArray>(array).values()[..],
                    TimeUnit::Microsecond => &typed::<DurationMicrosecondArray>(array).values()[..],
                    TimeUnit::Nanosecond => &typed::<DurationNanosecondArray>(array).values()[..],
                };
                V::Duration(values, *unit)
            }
            DataType::Decimal128(_, scale) if *scale >= 0 => V::Decimal128(typed(array), *scale),
            _ => V::Unsupported,
        };
        Self { array, values }
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    /// The Python scalar at `row`; `scratch` is reusable formatting space.
    fn value(&self, py: Python<'py>, row: usize, scratch: &mut String) -> PyResult<Py<PyAny>> {
        use PyScalarValues as V;
        if self.array.is_null(row) {
            return Ok(py.None());
        }
        match &self.values {
            V::Null => Ok(py.None()),
            V::Boolean(values) => values.value(row).into_py_any(py),
            V::Int8(values) => values.value(row).into_py_any(py),
            V::Int16(values) => values.value(row).into_py_any(py),
            V::Int32(values) => values.value(row).into_py_any(py),
            V::Int64(values) => values.value(row).into_py_any(py),
            V::UInt8(values) => values.value(row).into_py_any(py),
            V::UInt16(values) => values.value(row).into_py_any(py),
            V::UInt32(values) => values.value(row).into_py_any(py),
            V::UInt64(values) => values.value(row).into_py_any(py),
            V::Float16(values) => f64::from(values.value(row).to_f32()).into_py_any(py),
            V::Float32(values) => f64::from(values.value(row)).into_py_any(py),
            V::Float64(values) => values.value(row).into_py_any(py),
            V::Utf8(values) => values.value(row).into_py_any(py),
            V::LargeUtf8(values) => values.value(row).into_py_any(py),
            V::Utf8View(values) => values.value(row).into_py_any(py),
            V::Binary(values) => Ok(PyBytes::new(py, values.value(row)).into_any().unbind()),
            V::LargeBinary(values) => Ok(PyBytes::new(py, values.value(row)).into_any().unbind()),
            V::BinaryView(values) => Ok(PyBytes::new(py, values.value(row)).into_any().unbind()),
            V::Date32(values) => date32_to_py(py, values.value(row)),
            V::Date64(values) => date64_to_py(py, values.value(row)),
            V::Time32(values, unit) => time_unit_to_py(py, i64::from(values[row]), unit),
            V::Time64(values, unit) => time_unit_to_py(py, values[row], unit),
            V::Timestamp(values, unit, zone) => timestamp_unit_to_py(py, values[row], unit, zone),
            V::Duration(values, unit) => duration_to_py(py, values[row], unit),
            V::Decimal128(values, scale) => decimal_to_py(py, values.value(row), *scale, scratch),
            V::Unsupported => Err(PyTypeError::new_err(format!(
                "Arrow dtype {} cannot be materialized to a Python scalar without PyArrow",
                self.array.data_type()
            ))),
        }
    }
}

fn scalar_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    PyScalarColumn::new(array).value(py, row, &mut String::new())
}

/// Python dict keys for a schema's field names, created once per table.
fn field_name_keys<'py>(py: Python<'py>, schema: &Schema) -> Vec<Bound<'py, PyString>> {
    schema
        .fields()
        .iter()
        .map(|field| PyString::new(py, field.name()))
        .collect()
}

/// Append one dict per row of `batch`, keyed by the schema's `keys`.
fn append_batch_rows<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    keys: &[Bound<'py, PyString>],
    rows: &mut Vec<Py<PyAny>>,
) -> PyResult<()> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| PyScalarColumn::new(column.as_ref()))
        .collect::<Vec<_>>();
    let mut scratch = String::new();
    for row in 0..batch.num_rows() {
        let dict = PyDict::new(py);
        for (key, column) in keys.iter().zip(&columns) {
            dict.set_item(key, column.value(py, row, &mut scratch)?)?;
        }
        rows.push(dict.into_any().unbind());
    }
    Ok(())
}

fn batch_to_pylist(py: Python<'_>, batch: &RecordBatch) -> PyResult<Vec<Py<PyAny>>> {
    let keys = field_name_keys(py, batch.schema_ref());
    let mut rows = Vec::with_capacity(batch.num_rows());
    append_batch_rows(py, batch, &keys, &mut rows)?;
    Ok(rows)
}

fn table_to_pylist(py: Python<'_>, table: &TableData) -> PyResult<Vec<Py<PyAny>>> {
    // Every batch of a table has the table's field names.
    let keys = field_name_keys(py, &table.schema);
    let mut rows = Vec::with_capacity(table.num_rows());
    for batch in &table.batches {
        append_batch_rows(py, batch, &keys, &mut rows)?;
    }
    Ok(rows)
}

fn column_to_pylist(py: Python<'_>, column: &ColumnData) -> PyResult<Vec<Py<PyAny>>> {
    let mut values = Vec::with_capacity(column.len());
    let mut scratch = String::new();
    for chunk in &column.chunks {
        let chunk = PyScalarColumn::new(chunk.as_ref());
        for row in 0..chunk.len() {
            values.push(chunk.value(py, row, &mut scratch)?);
        }
    }
    Ok(values)
}

/// Whether any row holds a value, exactly as
/// `cell_has_value(&cell_from_array(array, row))` decides per row: nulls, empty
/// text, temporal values the carrier cannot represent and dtypes it does not
/// support are absent.
fn column_has_value(array: &dyn Array) -> bool {
    if array.null_count() == array.len() {
        return false;
    }
    match array.data_type() {
        // Every valid slot of these is a present carrier scalar.
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => true,
        DataType::Utf8 => text_has_value(typed::<StringArray>(array)),
        DataType::LargeUtf8 => text_has_value(typed::<LargeStringArray>(array)),
        DataType::Utf8View => {
            let array = typed::<StringViewArray>(array);
            match array.nulls() {
                None => array.lengths().any(|len| len > 0),
                // A view's low 32 bits are its byte length.
                Some(nulls) => nulls
                    .valid_indices()
                    .any(|row| array.views()[row] as u32 > 0),
            }
        }
        DataType::Date32 => typed::<Date32Array>(array)
            .iter()
            .flatten()
            .any(|days| date_from_days(days).is_some()),
        DataType::Date64 => typed::<Date64Array>(array).iter().flatten().any(|millis| {
            i32::try_from(millis.div_euclid(86_400_000))
                .ok()
                .and_then(date_from_days)
                .is_some()
        }),
        DataType::Timestamp(TimeUnit::Second, _) => typed::<TimestampSecondArray>(array)
            .iter()
            .flatten()
            .any(|seconds| seconds.checked_mul(1_000_000).is_some()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => typed::<TimestampMillisecondArray>(array)
            .iter()
            .flatten()
            .any(|millis| millis.checked_mul(1_000).is_some()),
        _ => false,
    }
}

/// Whether a valid UTF-8 slot is non-empty, read from the offsets alone.
fn text_has_value<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> bool {
    let offsets = array.value_offsets();
    match array.nulls() {
        None => offsets.first() != offsets.last(),
        Some(nulls) => nulls
            .valid_indices()
            .any(|row| offsets[row + 1] > offsets[row]),
    }
}

/// Append the text `cell_to_string` renders for `cell`; nothing for null.
fn write_cell_text(cell: &CellValue, out: &mut String) {
    let written = match cell {
        CellValue::Null => Ok(()),
        CellValue::Bool(value) => write!(out, "{value}"),
        CellValue::Int(value) => write!(out, "{value}"),
        CellValue::Float(value) => write!(out, "{value}"),
        CellValue::Date(value) => write!(out, "{value}"),
        CellValue::Timestamp(value) => match DateTime::from_timestamp_micros(*value) {
            Some(timestamp) => {
                out.push_str(&timestamp.to_rfc3339());
                Ok(())
            }
            None => write!(out, "{value}"),
        },
        CellValue::Text(value) => {
            out.push_str(value);
            Ok(())
        }
    };
    written.expect("writing to a String cannot fail");
}

/// `date_from_cell` for borrowed text: an ISO date prefix or `YYYYMMDD`.
fn date_from_text(text: &str) -> Option<NaiveDate> {
    if text.len() >= 10 {
        text.get(..10)
            .and_then(|prefix| NaiveDate::parse_from_str(prefix, "%Y-%m-%d").ok())
    } else if text.len() == 8 {
        NaiveDate::parse_from_str(text, "%Y%m%d").ok()
    } else {
        None
    }
}

/// Bloomberg periodicity of a historical presentation, parsed once per call.
#[derive(Clone, Copy)]
enum PeriodLabel {
    Year,
    Half,
    Quarter,
    Month,
    Week,
    Day,
}

impl PeriodLabel {
    fn parse(periodicity: Option<&str>) -> Self {
        let periodicity = periodicity.unwrap_or("DAILY");
        let is_any = |names: &[&str]| {
            names
                .iter()
                .any(|name| periodicity.eq_ignore_ascii_case(name))
        };
        if is_any(&["YEARLY", "Y"]) {
            Self::Year
        } else if is_any(&["SEMI_ANNUALLY", "SEMIANNUALLY", "S"]) {
            Self::Half
        } else if is_any(&["QUARTERLY", "Q"]) {
            Self::Quarter
        } else if is_any(&["MONTHLY", "M"]) {
            Self::Month
        } else if is_any(&["WEEKLY", "W"]) {
            Self::Week
        } else {
            Self::Day
        }
    }

    fn write(self, date: NaiveDate, out: &mut String) {
        let written = match self {
            Self::Year => write!(out, "{:04}", date.year()),
            Self::Half => {
                let half = if date.month() <= 6 { 1 } else { 2 };
                write!(out, "{:04}H{half}", date.year())
            }
            Self::Quarter => write!(out, "{:04}Q{}", date.year(), (date.month() - 1) / 3 + 1),
            Self::Month => write!(out, "{:04}-{:02}", date.year(), date.month()),
            Self::Week => {
                let week = date.iso_week();
                write!(out, "{:04}-W{:02}", week.year(), week.week())
            }
            Self::Day => write!(out, "{date}"),
        };
        written.expect("writing to a String cannot fail");
    }
}

/// Builder for one batch's `period` column.
struct PeriodLabels {
    builder: StringBuilder,
    scratch: String,
    label: PeriodLabel,
}

impl PeriodLabels {
    fn date(&mut self, date: NaiveDate) {
        self.scratch.clear();
        self.label.write(date, &mut self.scratch);
        self.builder.append_value(&self.scratch);
    }

    fn cell(&mut self, cell: &CellValue) {
        self.scratch.clear();
        write_cell_text(cell, &mut self.scratch);
        self.builder.append_value(&self.scratch);
    }

    fn texts<'a>(&mut self, values: impl IntoIterator<Item = Option<&'a str>>) {
        for value in values {
            match value {
                Some(text) => match date_from_text(text) {
                    Some(date) => self.date(date),
                    None => self.builder.append_value(text),
                },
                None => self.builder.append_value(""),
            }
        }
    }
}

/// The `period` label of every date row: dates `date_from_array` parses get
/// their period, other values keep their carrier text, and nulls become "".
fn period_labels(dates: &dyn Array, label: PeriodLabel) -> ArrayRef {
    let mut labels = PeriodLabels {
        builder: StringBuilder::with_capacity(dates.len(), dates.len() * 10),
        scratch: String::new(),
        label,
    };
    match dates.data_type() {
        DataType::Date32 => {
            for days in typed::<Date32Array>(dates) {
                match days.and_then(date_from_days) {
                    Some(date) => labels.date(date),
                    None => labels.builder.append_value(""),
                }
            }
        }
        DataType::Utf8 => labels.texts(typed::<StringArray>(dates)),
        DataType::LargeUtf8 => labels.texts(typed::<LargeStringArray>(dates)),
        DataType::Utf8View => labels.texts(typed::<StringViewArray>(dates)),
        _ => {
            for row in 0..dates.len() {
                let cell = cell_from_array(dates, row);
                match date_from_cell(&cell) {
                    Some(date) => labels.date(date),
                    None => labels.cell(&cell),
                }
            }
        }
    }
    Arc::new(labels.builder.finish())
}

/// Insert (or replace) a column given as one array per physical batch, as
/// `TableData::with_cells_column` does for scalar cells.
fn with_array_column(
    data: &TableData,
    index: usize,
    field: FieldRef,
    arrays: Vec<ArrayRef>,
    replace: bool,
) -> xbbg_arrow::Result<TableData> {
    let width = data.num_columns();
    if (replace && index >= width) || index > width {
        return Err(ArrowCoreError::ColumnIndexOutOfRange);
    }
    let batches = data
        .batches
        .iter()
        .zip(arrays)
        .map(|(batch, array)| {
            let schema = batch.schema();
            let mut fields = schema.fields().to_vec();
            let mut columns = batch.columns().to_vec();
            if replace {
                fields[index] = Arc::clone(&field);
                columns[index] = array;
            } else {
                fields.insert(index, Arc::clone(&field));
                columns.insert(index, array);
            }
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
                columns,
            )
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    TableData::try_new(batches)
}

/// Excel-style BDH presentation (`sort`, `date_format`, `show_date`) applied
/// as pure table work; sorting keys rows by `key_column`.
pub(crate) fn historical_presentation(
    data: &TableData,
    show_date: Option<bool>,
    date_format: Option<&str>,
    sort: Option<&str>,
    periodicity: Option<&str>,
    key_column: &str,
) -> xbbg_arrow::Result<TableData> {
    let mut result = data.clone();

    if let Some(sort) = sort
        && result.schema.index_of("date").is_ok()
    {
        let date_order = if sort.eq_ignore_ascii_case("DESCENDING") {
            SortDirection::Descending
        } else {
            SortDirection::Ascending
        };
        let mut sort_keys = Vec::with_capacity(3);
        if result.schema.index_of(key_column).is_ok() {
            sort_keys.push((key_column.to_string(), SortDirection::Ascending));
        }
        sort_keys.push(("date".to_string(), date_order));
        if result.schema.index_of("field").is_ok() {
            sort_keys.push(("field".to_string(), SortDirection::Ascending));
        }
        result = result.sort_by(&sort_keys, false)?;
    }

    if let Some(date_format) = date_format {
        let periodic = date_format.eq_ignore_ascii_case("PERIODIC");
        if (periodic || date_format.eq_ignore_ascii_case("BOTH"))
            && let Ok(date_idx) = result.schema.index_of("date")
        {
            let label = PeriodLabel::parse(periodicity);
            let periods = result
                .batches
                .iter()
                .map(|batch| period_labels(batch.column(date_idx).as_ref(), label))
                .collect();
            let period = Arc::new(Field::new("period", DataType::Utf8, true));
            result = if periodic {
                with_array_column(&result, date_idx, period, periods, true)?
            } else {
                with_array_column(&result, date_idx + 1, period, periods, false)?
            };
        }
    }

    if show_date == Some(false) {
        result = result.drop_columns(&["date".to_string(), "period".to_string()])?;
    }
    Ok(result)
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ArrowField {
    field: FieldRef,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl ArrowField {
    #[getter]
    fn name(&self) -> String {
        self.field.name().clone()
    }

    #[getter]
    fn data_type(&self) -> String {
        self.field.data_type().to_string()
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.field.is_nullable()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowField(name={:?}, data_type={:?}, nullable={})",
            self.field.name(),
            self.field.data_type(),
            self.field.is_nullable()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ArrowSchema {
    schema: SchemaRef,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl ArrowSchema {
    #[getter]
    fn names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[getter]
    fn fields(&self) -> Vec<ArrowField> {
        self.schema
            .fields()
            .iter()
            .map(|field| ArrowField {
                field: field.clone(),
            })
            .collect()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_schema_pycapsule(py, self.schema.as_ref())
            .map(Bound::into_any)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn __repr__(&self) -> String {
        format!("xbbg.ArrowSchema(names={:?})", self.names())
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ArrowColumn {
    data: ColumnData,
}

impl ArrowColumn {
    fn new(data: ColumnData) -> Self {
        Self { data }
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl ArrowColumn {
    #[getter]
    fn name(&self) -> String {
        self.data.name.clone()
    }

    #[getter]
    fn field(&self) -> ArrowField {
        ArrowField {
            field: self.data.field.clone(),
        }
    }

    #[getter]
    fn data_type(&self) -> String {
        self.data.field.data_type().to_string()
    }

    #[getter]
    fn null_count(&self) -> usize {
        self.data.null_count()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.data.nbytes()
    }

    /// Bytes a right-sized copy of this column's logical slice would occupy.
    ///
    /// Computed without copying, unlike `compact().nbytes`, and unlike
    /// `nbytes`, which counts the full buffers a slice still references.
    /// Counts value bytes, offsets or 16-byte views, and validity bitmaps:
    /// variable-width types count only the sliced value span, and
    /// `Utf8View`/`BinaryView` count the out-of-line bytes of values longer
    /// than 12 bytes. List children and dictionary values are counted in full.
    #[getter]
    fn logical_nbytes(&self) -> PyResult<usize> {
        map_core(self.data.logical_nbytes())
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        Self::new(self.data.slice(offset, length))
    }

    /// Copy logical values into independent, right-sized Arrow buffers.
    fn compact(&self, py: Python<'_>) -> PyResult<Self> {
        map_core(py.detach(|| self.data.compact())).map(Self::new)
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, column_to_pylist(py, &self.data)?)?
            .into_any()
            .unbind())
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowColumn.to_pyarrow()",
        )?;
        PyChunkedArray::try_new(self.data.chunks.clone(), self.data.field.clone())?.into_pyarrow(py)
    }

    fn __len__(&self) -> usize {
        self.data.len()
    }

    fn __getitem__(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyAny>> {
        let idx = normalize_index(index, self.data.len(), "row")?;
        let (chunk, row) = map_core(self.data.chunk_for_index(idx))?;
        scalar_to_py(py, chunk.as_ref(), row)
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self.to_pylist(py)?;
        Ok(values.bind(py).call_method0("__iter__")?.unbind())
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let values = self.to_pylist(py)?;
        values
            .bind(py)
            .rich_compare(other, pyo3::basic::CompareOp::Eq)?
            .is_truthy()
    }

    fn __ne__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let values = self.to_pylist(py)?;
        values
            .bind(py)
            .rich_compare(other, pyo3::basic::CompareOp::Ne)?
            .is_truthy()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowColumn(name={:?}, len={}, data_type={:?})",
            self.data.name,
            self.data.len(),
            self.data.field.data_type()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ArrowRecordBatch {
    batch: RecordBatch,
}

impl ArrowRecordBatch {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    fn table_data(&self) -> PyResult<TableData> {
        map_core(TableData::try_new(vec![self.batch.clone()]))
    }

    fn to_array_pycapsules<'py>(
        py: Python<'py>,
        record_batch: RecordBatch,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let field = Field::new_struct("", record_batch.schema_ref().fields().clone(), false)
            .with_metadata(record_batch.schema_ref().metadata().clone());
        let array: ArrayRef = Arc::new(StructArray::from(record_batch));
        to_array_pycapsules(py, field.into(), array.as_ref(), requested_schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow array export failed: {e}")))
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl ArrowRecordBatch {
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        (self.batch.num_rows(), self.batch.num_columns())
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.batch
            .columns()
            .iter()
            .map(|array| array.get_buffer_memory_size())
            .sum()
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<ArrowColumn>> {
        let table = self.table_data()?;
        (0..table.num_columns())
            .map(|idx| map_core(table.column_by_index(idx)).map(ArrowColumn::new))
            .collect()
    }

    #[getter]
    fn schema(&self) -> ArrowSchema {
        ArrowSchema {
            schema: self.batch.schema(),
        }
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.batch.schema_ref().metadata().clone()
    }

    /// UTF-8 byte length of each schema-metadata value, without copying values.
    fn metadata_sizes(&self) -> HashMap<String, usize> {
        metadata_sizes(self.batch.schema_ref().metadata())
    }

    /// One schema-metadata value, or its first `max_bytes` UTF-8 bytes cut back
    /// to a character boundary; `None` when the key is absent.
    #[pyo3(signature = (key, max_bytes=None))]
    fn metadata_value(&self, key: &str, max_bytes: Option<usize>) -> Option<&str> {
        metadata_value(self.batch.schema_ref().metadata(), key, max_bytes)
    }

    #[getter]
    fn field_exceptions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_field_exceptions_metadata(py, self.batch.schema_ref().metadata())
    }

    #[getter]
    fn field_errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(
            py,
            self.batch.schema_ref().metadata(),
            METADATA_KEY_FIELD_ERRORS,
        )
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let requested_schema = requested_schema
            .as_ref()
            .map(|value| value.cast::<PyCapsule>().cloned())
            .transpose()?;
        Self::to_array_pycapsules(py, self.batch.clone(), requested_schema)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_schema_pycapsule(py, self.batch.schema_ref().as_ref())
            .map(Bound::into_any)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        batch_to_pylist(py, &self.batch)
    }

    fn to_table(&self) -> PyResult<ArrowTable> {
        ArrowTable::try_new(vec![self.batch.clone()])
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowRecordBatch.to_pyarrow()",
        )?;
        PyRecordBatch::new(self.batch.clone()).into_pyarrow(py)
    }

    fn column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        let idx = column_index_arg(column, self.batch.schema_ref())?;
        map_core(self.table_data()?.column_by_index(idx)).map(ArrowColumn::new)
    }

    fn get_column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn __getitem__(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn select(&self, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let indices = column_indices_arg(columns, self.batch.schema_ref())?;
        let table = map_core(self.table_data()?.select_indices(&indices))?;
        let batch = table
            .batches
            .into_iter()
            .next()
            .unwrap_or_else(|| RecordBatch::new_empty(self.batch.schema()));
        Ok(Self::new(batch))
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        if offset >= self.batch.num_rows() || length == Some(0) {
            return Self::new(RecordBatch::new_empty(self.batch.schema()));
        }
        let take = length
            .unwrap_or(self.batch.num_rows() - offset)
            .min(self.batch.num_rows() - offset);
        Self::new(self.batch.slice(offset, take))
    }

    /// Copy logical values into independent, right-sized Arrow buffers.
    fn compact(&self, py: Python<'_>) -> PyResult<Self> {
        let batch = &self.batch;
        let table = map_core(py.detach(|| TableData::try_new(vec![batch.clone()])?.compact()))?;
        let batch = table
            .batches
            .into_iter()
            .next()
            .expect("compacting one record batch preserves its physical batch");
        Ok(Self::new(batch))
    }

    fn __len__(&self) -> usize {
        self.batch.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowRecordBatch(num_rows={}, num_columns={}, columns={:?})",
            self.batch.num_rows(),
            self.batch.num_columns(),
            self.column_names()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ArrowTable {
    data: TableData,
}

impl ArrowTable {
    fn try_new(batches: Vec<RecordBatch>) -> PyResult<Self> {
        map_core(TableData::try_new(batches)).map(Self::from_data)
    }

    pub(crate) fn from_data(data: TableData) -> Self {
        Self { data }
    }

    pub(crate) fn table_data(&self) -> &TableData {
        &self.data
    }

    /// Run native table work with this thread detached from the interpreter.
    fn detached(
        py: Python<'_>,
        work: impl Ungil + FnOnce() -> xbbg_arrow::Result<TableData>,
    ) -> PyResult<Self> {
        map_core(py.detach(work)).map(Self::from_data)
    }

    fn to_stream_pycapsule<'py>(
        py: Python<'py>,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let fields = schema.fields();
        let array_reader = batches.into_iter().map(|batch| {
            Ok::<ArrayRef, arrow_schema::ArrowError>(Arc::new(StructArray::from(batch)) as ArrayRef)
        });
        let field =
            Field::new_struct("", fields.clone(), false).with_metadata(schema.metadata().clone());
        let array_reader = Box::new(ArrayIterator::new(array_reader, field.into()));
        to_stream_pycapsule(py, array_reader, requested_schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow stream export failed: {e}")))
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl ArrowTable {
    #[classmethod]
    #[pyo3(signature = (rows, schema=None))]
    fn from_pylist(
        _cls: &Bound<'_, PyType>,
        rows: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if schema.is_some() {
            return Err(PyValueError::new_err(
                "ArrowTable.from_pylist schema override is not implemented; pass dict rows with desired keys",
            ));
        }
        Self::try_new(vec![rows_to_batch(rows)?])
    }

    /// Build a table from a mapping of column name to values, in mapping order.
    ///
    /// Cells are typed like `from_pylist`; every column must have the same
    /// length.
    #[classmethod]
    fn from_pydict(_cls: &Bound<'_, PyType>, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let (columns_data, row_count) = pydict_to_columns(columns)?;
        let batch = columns
            .py()
            .detach(move || columns_to_batch(columns_data, row_count))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Self::try_new(vec![batch])
    }

    #[classmethod]
    fn empty(_cls: &Bound<'_, PyType>, schema_or_columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(names) = schema_or_columns.extract::<Vec<String>>() {
            return map_core(TableData::empty_from_columns(names)).map(Self::from_data);
        }
        if let Ok(schema) = schema_or_columns.extract::<PyRef<'_, ArrowSchema>>() {
            return Self::try_new(vec![RecordBatch::new_empty(schema.schema.clone())]);
        }
        Err(PyTypeError::new_err(
            "ArrowTable.empty expects a sequence of column names or ArrowSchema",
        ))
    }

    #[classmethod]
    fn from_batches(_cls: &Bound<'_, PyType>, batches: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_batches = Vec::new();
        for item in PyIterator::from_object(batches)? {
            let item = item?;
            let batch = item.extract::<PyRef<'_, ArrowRecordBatch>>()?;
            rust_batches.push(batch.batch.clone());
        }
        Self::try_new(rust_batches)
    }

    #[classmethod]
    fn concat_tables(_cls: &Bound<'_, PyType>, tables: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_tables = Vec::new();
        for item in PyIterator::from_object(tables)? {
            let item = item?;
            let table = item.extract::<PyRef<'_, ArrowTable>>()?;
            rust_tables.push(table.data.clone());
        }
        map_core(TableData::concat_tables(&rust_tables)).map(Self::from_data)
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.data.column_names()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.data.num_columns()
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        self.data.shape()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.data.nbytes()
    }

    #[getter]
    fn chunk_lengths(&self) -> Vec<usize> {
        self.data.chunk_lengths()
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<ArrowColumn>> {
        (0..self.data.num_columns())
            .map(|idx| map_core(self.data.column_by_index(idx)).map(ArrowColumn::new))
            .collect()
    }

    #[getter]
    fn schema(&self) -> ArrowSchema {
        ArrowSchema {
            schema: self.data.schema.clone(),
        }
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.data.schema.metadata().clone()
    }

    /// UTF-8 byte length of each schema-metadata value, without copying values.
    fn metadata_sizes(&self) -> HashMap<String, usize> {
        metadata_sizes(self.data.schema.metadata())
    }

    /// One schema-metadata value, or its first `max_bytes` UTF-8 bytes cut back
    /// to a character boundary; `None` when the key is absent.
    #[pyo3(signature = (key, max_bytes=None))]
    fn metadata_value(&self, key: &str, max_bytes: Option<usize>) -> Option<&str> {
        metadata_value(self.data.schema.metadata(), key, max_bytes)
    }

    #[getter]
    fn bloomberg_request_id(&self) -> Option<&str> {
        self.data
            .schema
            .metadata()
            .get(METADATA_KEY_BLOOMBERG_REQUEST_ID)
            .map(String::as_str)
    }

    #[getter]
    fn bloomberg_request_ids(&self) -> PyResult<Vec<String>> {
        if let Some(id) = self.bloomberg_request_id() {
            return Ok(vec![id.to_owned()]);
        }
        self.data
            .schema
            .metadata()
            .get(METADATA_KEY_BLOOMBERG_REQUEST_IDS)
            .map(|raw| {
                serde_json::from_str(raw).map_err(|err| {
                    PyValueError::new_err(format!(
                        "invalid JSON in {METADATA_KEY_BLOOMBERG_REQUEST_IDS}: {err}"
                    ))
                })
            })
            .transpose()
            .map(Option::unwrap_or_default)
    }

    #[getter]
    fn eid_data(&self) -> PyResult<Option<HashMap<String, Vec<i32>>>> {
        self.data
            .schema
            .metadata()
            .get(METADATA_KEY_EID_DATA)
            .map(|raw| {
                serde_json::from_str(raw).map_err(|err| {
                    PyValueError::new_err(format!("invalid JSON in {METADATA_KEY_EID_DATA}: {err}"))
                })
            })
            .transpose()
    }

    #[getter]
    fn security_errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(
            py,
            self.data.schema.metadata(),
            METADATA_KEY_SECURITY_ERRORS,
        )
    }

    #[getter]
    fn field_exceptions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_field_exceptions_metadata(py, self.data.schema.metadata())
    }

    #[getter]
    fn field_errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_FIELD_ERRORS)
    }

    #[getter]
    fn delayed(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_DELAYED)
    }

    #[getter]
    fn type_misses(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_TYPE_MISSES)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requested_schema = requested_schema
            .as_ref()
            .map(|value| value.cast::<PyCapsule>().cloned())
            .transpose()?;
        Self::to_stream_pycapsule(
            py,
            self.data.batches.clone(),
            self.data.schema.clone(),
            requested_schema,
        )
        .map(Bound::into_any)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_schema_pycapsule(py, self.data.schema.as_ref())
            .map(Bound::into_any)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn to_batches(&self, py: Python<'_>) -> PyResult<Vec<Py<ArrowRecordBatch>>> {
        self.data
            .batches
            .iter()
            .cloned()
            .map(|batch| Py::new(py, ArrowRecordBatch::new(batch)))
            .collect()
    }

    fn to_record_batch(&self, py: Python<'_>) -> PyResult<ArrowRecordBatch> {
        // One batch is an Arc clone; only concatenating several copies rows.
        let batch = if self.data.batches.len() > 1 {
            py.detach(|| self.data.combined_batch())
        } else {
            self.data.combined_batch()
        };
        map_core(batch).map(ArrowRecordBatch::new)
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        table_to_pylist(py, &self.data)
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowTable.to_pyarrow()",
        )?;
        PyTable::try_new(self.data.batches.clone(), self.data.schema.clone())?.into_pyarrow(py)
    }

    fn to_pandas<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowTable.to_pandas()",
        )?;
        ensure_optional_module(
            py,
            "pandas",
            "pip install xbbg[pandas]",
            "ArrowTable.to_pandas()",
        )?;
        let table = slf.to_pyarrow(py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("split_blocks", true)?;
        let frame = table.call_method("to_pandas", (), Some(&kwargs))?;
        py.import("xbbg.backend")?
            .call_method1("_attach_xbbg_metadata_attrs", (frame, slf))
    }

    /// Convert to a Polars DataFrame through `xbbg.backend`, keeping native chunks.
    fn to_polars<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        py.import("xbbg.backend")?
            .call_method1("_to_polars_frame", (slf,))
    }

    fn column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        let idx = column_index_arg(column, &self.data.schema)?;
        map_core(self.data.column_by_index(idx)).map(ArrowColumn::new)
    }

    fn get_column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if key.cast::<PyList>().is_ok() || key.cast::<PyTuple>().is_ok() {
            return Py::new(py, self.select(key)?).map(|obj| obj.into_any());
        }
        Py::new(py, self.column(key)?).map(|obj| obj.into_any())
    }

    fn __len__(&self) -> usize {
        self.num_rows()
    }

    fn select(&self, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let indices = column_indices_arg(columns, &self.data.schema)?;
        map_core(self.data.select_indices(&indices)).map(Self::from_data)
    }

    fn select_columns(&self, names: Vec<String>) -> PyResult<Self> {
        map_core(self.data.select_names(&names)).map(Self::from_data)
    }

    fn drop_columns(&self, names: Vec<String>) -> PyResult<Self> {
        map_core(self.data.drop_columns(&names)).map(Self::from_data)
    }

    fn rename_columns(&self, mapping: HashMap<String, String>) -> PyResult<Self> {
        map_core(self.data.rename_columns(&mapping)).map(Self::from_data)
    }

    fn rename(&self, mapping: HashMap<String, String>) -> PyResult<Self> {
        self.rename_columns(mapping)
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> PyResult<Self> {
        map_core(self.data.slice(offset, length)).map(Self::from_data)
    }

    /// Copy logical values into independent, right-sized Arrow buffers.
    fn compact(&self, py: Python<'_>) -> PyResult<Self> {
        Self::detached(py, || self.data.compact())
    }

    fn tail(&self, n: usize) -> PyResult<Self> {
        map_core(self.data.tail(n)).map(Self::from_data)
    }

    #[pyo3(signature = (sort_keys, *, nulls_last=true))]
    fn sort_by(
        &self,
        py: Python<'_>,
        sort_keys: Vec<(String, String)>,
        nulls_last: bool,
    ) -> PyResult<Self> {
        let keys = sort_keys
            .into_iter()
            .map(|(name, direction)| {
                SortDirection::from_str(&direction)
                    .map(|direction| (name.clone(), direction))
                    .map_err(|_| {
                        PyValueError::new_err(format!(
                            "unsupported sort direction for {name}: {direction}"
                        ))
                    })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Self::detached(py, || self.data.sort_by(&keys, !nulls_last))
    }

    fn filter_eq(&self, column: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let needle = py_value_to_cell(value)?;
        let column_index = map_core(self.data.column_index(column))?;
        let data_type = self.data.schema.field(column_index).data_type();
        if !filter_value_supported(data_type, &needle) {
            return Err(PyTypeError::new_err(format!(
                "cannot compare Python value to Arrow column {column:?} with dtype {data_type}"
            )));
        }
        Self::detached(value.py(), || self.data.filter_eq(column, &needle))
    }

    fn add_column(&self, index: usize, name: &str, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        Self::detached(values.py(), || self.data.add_column(index, name, &cells))
    }

    fn set_column(&self, index: usize, name: &str, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        Self::detached(values.py(), || self.data.set_column(index, name, &cells))
    }

    fn head(&self, n: usize) -> PyResult<Self> {
        map_core(self.data.head(n)).map(Self::from_data)
    }

    fn has_any_value(&self, py: Python<'_>, names: Vec<String>) -> bool {
        let data = &self.data;
        py.detach(|| {
            names
                .iter()
                .filter_map(|name| data.schema.index_of(name).ok())
                .any(|idx| {
                    data.batches
                        .iter()
                        .any(|batch| column_has_value(batch.column(idx).as_ref()))
                })
        })
    }

    /// Excel-style BDH presentation; sorting keys rows by `key_column`.
    fn apply_historical_presentation(
        &self,
        py: Python<'_>,
        show_date: Option<bool>,
        date_format: Option<String>,
        sort: Option<String>,
        periodicity: Option<String>,
        key_column: &str,
    ) -> PyResult<Self> {
        Self::detached(py, || {
            historical_presentation(
                &self.data,
                show_date,
                date_format.as_deref(),
                sort.as_deref(),
                periodicity.as_deref(),
                key_column,
            )
        })
    }

    /// Quote rows from generic tick output, with the security in `key_column`.
    ///
    /// The BQR shaper's reshape, shared with `ashape_bqr`, `xbbg.ext.abqr`
    /// and Node `bqr`; schema metadata is kept.
    fn reshape_bqr_generic(
        &self,
        py: Python<'_>,
        ticker: &str,
        key_column: &str,
    ) -> PyResult<Self> {
        py.detach(|| reshape_bqr_generic_table(&self.data, ticker, key_column))
            .map(Self::from_data)
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowTable(num_rows={}, num_columns={}, columns={:?})",
            self.num_rows(),
            self.num_columns(),
            self.column_names()
        )
    }
}

pub(crate) fn record_batch_to_arrow_record_batch(
    py: Python<'_>,
    batch: RecordBatch,
) -> PyResult<Py<PyAny>> {
    Py::new(py, ArrowRecordBatch::new(batch)).map(|obj| obj.into_any())
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowField>()?;
    m.add_class::<ArrowSchema>()?;
    m.add_class::<ArrowColumn>()?;
    m.add_class::<ArrowRecordBatch>()?;
    m.add_class::<ArrowTable>()?;
    m.add_function(wrap_pyfunction!(_decode_field_exceptions, m)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    use arrow_array::builder::NullBufferBuilder;
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, NullArray};
    use xbbg_arrow::{cell_has_value, cell_to_string};

    /// The row-wise rule `column_has_value` replaced.
    fn reference_has_value(array: &dyn Array) -> bool {
        (0..array.len()).any(|row| cell_has_value(&cell_from_array(array, row)))
    }

    /// The per-row labelling `period_labels` replaced.
    fn reference_period_label(array: &dyn Array, row: usize, periodicity: Option<&str>) -> String {
        let Some(parsed) = xbbg_arrow::scalar::date_from_array(array, row) else {
            return cell_to_string(&cell_from_array(array, row)).unwrap_or_default();
        };
        match periodicity.unwrap_or("DAILY").to_ascii_uppercase().as_str() {
            "YEARLY" | "Y" => format!("{:04}", parsed.year()),
            "SEMI_ANNUALLY" | "SEMIANNUALLY" | "S" => {
                let half = if parsed.month() <= 6 { 1 } else { 2 };
                format!("{:04}H{}", parsed.year(), half)
            }
            "QUARTERLY" | "Q" => {
                let quarter = ((parsed.month() - 1) / 3) + 1;
                format!("{:04}Q{}", parsed.year(), quarter)
            }
            "MONTHLY" | "M" => format!("{:04}-{:02}", parsed.year(), parsed.month()),
            "WEEKLY" | "W" => {
                let iso = parsed.iso_week();
                format!("{:04}-W{:02}", iso.year(), iso.week())
            }
            _ => parsed.to_string(),
        }
    }

    /// The path parser of the cell-map BQR reshape.
    fn reference_parse_bqr_path(path: &str) -> Option<(usize, String)> {
        let start = path.find("tickData[")? + "tickData[".len();
        let rest = &path[start..];
        let end = rest.find(']')?;
        let idx = rest[..end].parse::<usize>().ok()?;
        let after = rest.get(end + 1..)?;
        let field_start = after.strip_prefix('.')?;
        let field = field_start
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect::<String>();
        (!field.is_empty()).then_some((idx, field))
    }

    /// Tick columns of the cell-map BQR reshape, in output order after the key.
    const REFERENCE_BQR_TICK_COLUMNS: [&str; 4] = ["time", "type", "value", "size"];

    /// The cell-map BQR reshape `ArrowTable.reshape_bqr_generic` ran before
    /// the shared recipe reshape.
    fn reference_reshape_bqr_generic(
        data: &TableData,
        ticker: &str,
        key_column: &str,
    ) -> TableData {
        let empty = || {
            TableData::empty_from_columns(
                std::iter::once(key_column)
                    .chain(REFERENCE_BQR_TICK_COLUMNS)
                    .map(ToString::to_string)
                    .collect(),
            )
            .unwrap()
        };
        let Ok(path_idx) = data.schema.index_of("path") else {
            return empty();
        };
        let value_str_idx = data.schema.index_of("value_str").ok();
        let value_num_idx = data.schema.index_of("value_num").ok();
        let batch = data.combined_batch().unwrap();
        let mut fields = BTreeSet::new();
        let mut records: BTreeMap<usize, HashMap<String, CellValue>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let Some(path) = cell_to_string(&cell_from_array(batch.column(path_idx).as_ref(), row))
            else {
                continue;
            };
            let Some((idx, field)) = reference_parse_bqr_path(&path) else {
                continue;
            };
            fields.insert(field.clone());
            let value_str = value_str_idx
                .map(|idx| cell_from_array(batch.column(idx).as_ref(), row))
                .unwrap_or(CellValue::Null);
            let value_num = value_num_idx
                .map(|idx| cell_from_array(batch.column(idx).as_ref(), row))
                .unwrap_or(CellValue::Null);
            let value = if cell_has_value(&value_str) {
                value_str
            } else {
                value_num
            };
            records.entry(idx).or_default().insert(field, value);
        }
        if records.is_empty() {
            return empty();
        }
        let mut columns = vec![key_column.to_string()];
        for name in REFERENCE_BQR_TICK_COLUMNS {
            if fields.contains(name) {
                columns.push(name.to_string());
            }
        }
        for name in fields {
            if name != key_column && !REFERENCE_BQR_TICK_COLUMNS.contains(&name.as_str()) {
                columns.push(name);
            }
        }
        let mut out_fields = Vec::new();
        let mut out_arrays = Vec::new();
        for column in &columns {
            let cells = records
                .values()
                .map(|record| {
                    if column == key_column {
                        CellValue::Text(ticker.to_string())
                    } else {
                        record.get(column).cloned().unwrap_or(CellValue::Null)
                    }
                })
                .collect::<Vec<_>>();
            let (field, array) = build_array(column, &cells);
            out_fields.push(field);
            out_arrays.push(array);
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_arrays).unwrap();
        TableData::try_new(vec![batch]).unwrap()
    }

    fn validity(valid: &[bool]) -> NullBufferBuilder {
        let mut nulls = NullBufferBuilder::new(valid.len());
        for &is_valid in valid {
            nulls.append(is_valid);
        }
        nulls
    }

    fn epoch_days(year: i32, month: u32, day: u32) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32
    }

    fn single_batch_table(fields: Vec<Field>, columns: Vec<ArrayRef>) -> TableData {
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        TableData::try_new(vec![batch]).unwrap()
    }

    #[test]
    fn native_batch_and_table_preserve_temporal_types_precision_and_nulls() {
        Python::initialize();
        Python::attach(|py| {
            let days = NaiveDate::from_ymd_opt(2026, 1, 2)
                .unwrap()
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32;
            let schema = Schema::new(vec![
                Field::new("trade_date", DataType::Date32, true),
                Field::new(
                    "auction_time",
                    DataType::Time64(TimeUnit::Microsecond),
                    true,
                ),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Date32Array::from(vec![Some(days), None])),
                    Arc::new(Time64MicrosecondArray::from(vec![
                        Some(57_541_123_456),
                        None,
                    ])),
                ],
            )
            .unwrap();
            let batch = record_batch_to_arrow_record_batch(py, batch).unwrap();
            let table = batch.bind(py).call_method0("to_table").unwrap();
            for carrier in [batch.bind(py), &table] {
                let rows = carrier.call_method0("to_pylist").unwrap();
                let values = rows.get_item(0).unwrap();
                let date = values.get_item("trade_date").unwrap();
                assert!(date.is_instance_of::<PyDate>());
                assert!(date.eq(PyDate::new(py, 2026, 1, 2).unwrap()).unwrap());

                let time = values.get_item("auction_time").unwrap();
                assert!(time.is_instance_of::<PyTime>());
                assert!(
                    time.eq(PyTime::new(py, 15, 59, 1, 123_456, None).unwrap())
                        .unwrap()
                );
                assert!(time.getattr("tzinfo").unwrap().is_none());

                let nulls = rows.get_item(1).unwrap();
                assert!(nulls.get_item("trade_date").unwrap().is_none());
                assert!(nulls.get_item("auction_time").unwrap().is_none());
            }
        });
    }

    #[test]
    fn scalar_to_py_preserves_timestamp_timezone_metadata() {
        Python::initialize();
        Python::attach(|py| {
            let array = TimestampMicrosecondArray::from(vec![0]).with_timezone("Asia/Hong_Kong");
            let value = scalar_to_py(py, &array, 0).expect("timestamp");
            let iso: String = value
                .bind(py)
                .call_method0("isoformat")
                .expect("isoformat")
                .extract()
                .expect("iso string");

            assert_eq!(iso, "1970-01-01T08:00:00+08:00");
        });
    }

    #[test]
    fn scalar_to_py_preserves_utc_timestamp_default() {
        Python::initialize();
        Python::attach(|py| {
            let array = TimestampMicrosecondArray::from(vec![0]);
            let value = scalar_to_py(py, &array, 0).expect("timestamp");
            let iso: String = value
                .bind(py)
                .call_method0("isoformat")
                .expect("isoformat")
                .extract()
                .expect("iso string");

            assert_eq!(iso, "1970-01-01T00:00:00+00:00");
        });
    }

    #[test]
    fn scalar_to_py_materializes_the_full_legacy_dtype_surface() {
        Python::initialize();
        Python::attach(|py| {
            let int8 = Int8Array::from(vec![-7_i8]);
            assert_eq!(
                scalar_to_py(py, &int8, 0)
                    .unwrap()
                    .extract::<i64>(py)
                    .unwrap(),
                -7
            );

            let large_text = LargeStringArray::from(vec!["wide"]);
            assert_eq!(
                scalar_to_py(py, &large_text, 0)
                    .unwrap()
                    .extract::<String>(py)
                    .unwrap(),
                "wide"
            );

            let binary = BinaryArray::from(vec![b"\x00\xff".as_slice()]);
            let value = scalar_to_py(py, &binary, 0).unwrap();
            assert_eq!(
                value.bind(py).cast::<PyBytes>().unwrap().as_bytes(),
                b"\x00\xff"
            );

            let date64 = Date64Array::from(vec![86_400_000_i64]);
            let date_iso: String = scalar_to_py(py, &date64, 0)
                .unwrap()
                .bind(py)
                .call_method0("isoformat")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(date_iso, "1970-01-02");

            let time = Time32SecondArray::from(vec![3_661_i32]);
            let time_iso: String = scalar_to_py(py, &time, 0)
                .unwrap()
                .bind(py)
                .call_method0("isoformat")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(time_iso, "01:01:01");

            let timestamp = TimestampNanosecondArray::from(vec![1_234_567_000_i64]);
            let timestamp_iso: String = scalar_to_py(py, &timestamp, 0)
                .unwrap()
                .bind(py)
                .call_method0("isoformat")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(timestamp_iso, "1970-01-01T00:00:01.234567+00:00");

            let duration = DurationMillisecondArray::from(vec![1_500_i64]);
            let seconds: f64 = scalar_to_py(py, &duration, 0)
                .unwrap()
                .bind(py)
                .call_method0("total_seconds")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(seconds, 1.5);

            let decimal = Decimal128Array::from(vec![
                12_345_678_901_234_567_890_123_456_789_012_345_678_i128,
            ])
            .with_precision_and_scale(38, 2)
            .unwrap();
            assert_eq!(
                scalar_to_py(py, &decimal, 0)
                    .unwrap()
                    .bind(py)
                    .str()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "123456789012345678901234567890123456.78"
            );
        });
    }

    #[test]
    fn empty_rows_and_python_datetimes_keep_valid_native_types() {
        Python::initialize();
        Python::attach(|py| {
            let rows = PyList::empty(py).into_any();
            let empty = rows_to_batch(&rows).unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.num_columns(), 0);

            let rows = PyList::empty(py);
            rows.append(PyDict::new(py)).unwrap();
            rows.append(PyDict::new(py)).unwrap();
            let empty_columns = rows_to_batch(&rows.into_any()).unwrap();
            assert_eq!(empty_columns.num_rows(), 2);
            assert_eq!(empty_columns.num_columns(), 0);

            let datetime = py
                .import("datetime")
                .unwrap()
                .getattr("datetime")
                .unwrap()
                .call1((2024, 1, 2, 3, 4, 5, 123_456))
                .unwrap();
            let expected = NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_micro_opt(3, 4, 5, 123_456)
                .unwrap()
                .and_utc()
                .timestamp_micros();
            let cell = py_value_to_cell(&datetime).unwrap();
            assert_eq!(cell, CellValue::Timestamp(expected));
            let (field, array) = build_array("timestamp", std::slice::from_ref(&cell));
            assert_eq!(
                field.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            );
            assert!(xbbg_arrow::cell_matches(array.as_ref(), 0, &cell));
        });
    }

    #[test]
    fn python_integer_overflow_is_not_rounded_to_float() {
        Python::initialize();
        Python::attach(|py| {
            let huge = py
                .import("builtins")
                .unwrap()
                .getattr("pow")
                .unwrap()
                .call1((2, 63))
                .unwrap();
            let err = py_value_to_cell(&huge).unwrap_err();
            assert!(err.is_instance_of::<PyOverflowError>(py));
        });
    }

    #[test]
    fn scalar_to_py_fails_closed_for_non_null_unsupported_types() {
        Python::initialize();
        Python::attach(|py| {
            let unsupported = StructArray::new_empty_fields(1, None);
            let err = scalar_to_py(py, &unsupported, 0).unwrap_err();
            assert!(err.is_instance_of::<PyTypeError>(py));
        });
    }

    #[test]
    fn arrow_table_sdk_provenance_survives_native_composition_and_export() {
        Python::initialize();
        Python::attach(|py| {
            let batch = |id: &str| {
                let schema = Arc::new(
                    Schema::new(vec![Field::new("row", DataType::Int64, false)]).with_metadata(
                        HashMap::from([(METADATA_KEY_BLOOMBERG_REQUEST_ID.into(), id.into())]),
                    ),
                );
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap()
            };
            let first = ArrowTable::try_new(vec![batch("sdk-synthetic-z")]).unwrap();
            assert_eq!(first.bloomberg_request_id(), Some("sdk-synthetic-z"));
            assert_eq!(first.bloomberg_request_ids().unwrap(), ["sdk-synthetic-z"]);
            let table =
                ArrowTable::try_new(vec![batch("sdk-synthetic-z"), batch("sdk-synthetic-a")])
                    .unwrap();
            assert!(table.bloomberg_request_id().is_none());
            assert_eq!(
                table.bloomberg_request_ids().unwrap(),
                ["sdk-synthetic-a", "sdk-synthetic-z"]
            );
            let native = Py::new(py, table).unwrap();
            let ids = native
                .bind(py)
                .getattr("bloomberg_request_ids")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap();
            assert_eq!(ids, ["sdk-synthetic-a", "sdk-synthetic-z"]);
            if let Ok(pa) = py.import("pyarrow") {
                let arrow = pa.getattr("table").unwrap().call1((native,)).unwrap();
                let metadata = arrow
                    .getattr("schema")
                    .unwrap()
                    .getattr("metadata")
                    .unwrap();
                let raw = metadata
                    .get_item(PyBytes::new(
                        py,
                        METADATA_KEY_BLOOMBERG_REQUEST_IDS.as_bytes(),
                    ))
                    .unwrap();
                assert_eq!(
                    raw.extract::<Vec<u8>>().unwrap(),
                    br#"["sdk-synthetic-a","sdk-synthetic-z"]"#
                );
            }
        });
    }

    #[test]
    fn native_field_exceptions_expand_compact_and_persisted_legacy_metadata() {
        Python::initialize();
        Python::attach(|py| {
            let first = serde_json::json!({
                "field": "OPEN_INT", "category": "BAD_FLD", "code": 9,
                "subcategory": "NOT_APPLICABLE_TO_REF_DATA", "message": "not applicable"
            });
            let second = serde_json::json!({
                "field": "PX_BAD", "category": "BAD_FLD", "code": 2,
                "subcategory": "INVALID", "message": "bad field"
            });
            let expanded = serde_json::json!({
                "IBM US Equity": [first, second, first],
                "MSFT US Equity": [second]
            });
            let compact = serde_json::json!({
                "version": 1,
                "entries": [first, second],
                "securities": {"IBM US Equity": [0, 1, 0], "MSFT US Equity": [1]}
            });
            let expected = serde_json_value_to_py(py, &expanded).unwrap();
            for payload in [expanded, compact] {
                let raw = payload.to_string();
                let schema =
                    Schema::new(vec![Field::new("ticker", DataType::Utf8, false)]).with_metadata(
                        HashMap::from([(METADATA_KEY_FIELD_EXCEPTIONS.into(), raw.clone())]),
                    );
                let batch = RecordBatch::try_new(
                    Arc::new(schema),
                    vec![Arc::new(StringArray::from(vec!["IBM US Equity"]))],
                )
                .unwrap();
                let batch = record_batch_to_arrow_record_batch(py, batch).unwrap();
                let table = batch.bind(py).call_method0("to_table").unwrap();
                for carrier in [batch.bind(py), &table] {
                    let actual = carrier.getattr("field_exceptions").unwrap();
                    assert!(actual.eq(expected.bind(py)).unwrap());
                    assert_eq!(
                        carrier
                            .getattr("metadata")
                            .unwrap()
                            .get_item(METADATA_KEY_FIELD_EXCEPTIONS)
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        raw
                    );
                }
            }
        });
    }

    #[test]
    fn native_field_exception_getters_reject_malformed_compact_indices() {
        Python::initialize();
        Python::attach(|py| {
            for index in [
                serde_json::json!(-1),
                serde_json::json!(1),
                serde_json::json!(0.5),
                serde_json::json!("0"),
                serde_json::json!(true),
                serde_json::Value::Null,
            ] {
                let raw = serde_json::json!({
                    "version": 1,
                    "entries": [{
                        "field": "PX_BAD", "category": "BAD_FLD", "code": 2,
                        "subcategory": "INVALID", "message": "bad field"
                    }],
                    "securities": {"IBM US Equity": [index]}
                })
                .to_string();
                let schema = Schema::empty()
                    .with_metadata(HashMap::from([(METADATA_KEY_FIELD_EXCEPTIONS.into(), raw)]));
                let batch = record_batch_to_arrow_record_batch(
                    py,
                    RecordBatch::new_empty(Arc::new(schema)),
                )
                .unwrap();
                let table = batch.bind(py).call_method0("to_table").unwrap();
                for carrier in [batch.bind(py), &table] {
                    let error = carrier.getattr("field_exceptions").unwrap_err();
                    assert!(error.is_instance_of::<PyValueError>(py));
                    assert!(error.to_string().contains(METADATA_KEY_FIELD_EXCEPTIONS));
                }
            }
        });
    }

    #[test]
    fn native_field_errors_account_for_refused_ids_on_empty_typed_results() {
        Python::initialize();
        Python::attach(|py| {
            let errors = serde_json::json!({
                "NOT_A_FIELD": {
                    "category": "BAD_FLD", "code": -103, "subcategory": "",
                    "message": "Unknown Field Id/Mnemonic", "source": "apiflds@test"
                },
                "REFUSED": {
                    "category": "NOT_AUTHORIZED", "code": -1, "subcategory": "DENIED",
                    "message": "Not authorized"
                }
            });
            let raw = errors.to_string();
            let schema = Schema::new(
                ["field", "type", "description", "category", "id"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Utf8, false))
                    .collect::<Vec<_>>(),
            )
            .with_metadata(HashMap::from([(
                METADATA_KEY_FIELD_ERRORS.into(),
                raw.clone(),
            )]));
            let batch =
                record_batch_to_arrow_record_batch(py, RecordBatch::new_empty(Arc::new(schema)))
                    .unwrap();
            let table = batch.bind(py).call_method0("to_table").unwrap();
            let expected = serde_json_value_to_py(py, &errors).unwrap();
            for carrier in [batch.bind(py), &table] {
                assert_eq!(
                    carrier
                        .getattr("num_rows")
                        .unwrap()
                        .extract::<usize>()
                        .unwrap(),
                    0
                );
                assert_eq!(
                    carrier
                        .getattr("column_names")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap(),
                    ["field", "type", "description", "category", "id"]
                );
                assert!(
                    carrier
                        .getattr("field_errors")
                        .unwrap()
                        .eq(expected.bind(py))
                        .unwrap()
                );
                assert!(carrier.getattr("field_exceptions").unwrap().is_none());
                assert_eq!(
                    carrier
                        .getattr("metadata")
                        .unwrap()
                        .get_item(METADATA_KEY_FIELD_ERRORS)
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    raw
                );
            }
        });
    }

    #[test]
    fn arrow_table_metadata_getters_parse_xbbg_json() {
        Python::initialize();
        Python::attach(|py| {
            let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, false)])
                .with_metadata(HashMap::from([
                    (
                        METADATA_KEY_EID_DATA.to_string(),
                        r#"{"IBM US Equity":[101,202]}"#.to_string(),
                    ),
                    (
                        METADATA_KEY_SECURITY_ERRORS.to_string(),
                        r#"{"BAD Ticker":{"category":"BAD_SEC","code":1,"subcategory":"INVALID","message":"bad security"}}"#.to_string(),
                    ),
                    (
                        METADATA_KEY_FIELD_EXCEPTIONS.to_string(),
                        r#"{"IBM US Equity":[{"field":"PX_BAD","category":"BAD_FLD","code":2,"subcategory":"INVALID","message":"bad field"}]}"#.to_string(),
                    ),
                    (
                        METADATA_KEY_DELAYED.to_string(),
                        r#"{"IBM US Equity":{"delayed":true,"delay_minutes":15},"MSFT US Equity":{"delayed":false,"delay_minutes":null}}"#.to_string(),
                    ),
                ]));
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(StringArray::from(vec!["IBM US Equity"]))],
            )
            .expect("record batch");
            let table = ArrowTable::try_new(vec![batch]).expect("table");

            assert!(table.metadata().contains_key(METADATA_KEY_EID_DATA));
            let eid_data = table.eid_data().expect("valid eid data").expect("eid data");
            assert_eq!(eid_data["IBM US Equity"], vec![101, 202]);

            let security_errors = table.security_errors(py).expect("security errors");
            assert!(
                security_errors
                    .bind(py)
                    .get_item("BAD Ticker")
                    .expect("security error")
                    .is_instance_of::<PyDict>()
            );

            let field_exceptions = table.field_exceptions(py).expect("field exceptions");
            assert!(
                field_exceptions
                    .bind(py)
                    .get_item("IBM US Equity")
                    .expect("field exception")
                    .is_instance_of::<PyList>()
            );

            let delayed = table.delayed(py).expect("delayed");
            let delayed = delayed.bind(py);
            let ibm = delayed.get_item("IBM US Equity").expect("delayed ticker");
            assert!(
                ibm.get_item("delayed")
                    .expect("delayed flag")
                    .extract::<bool>()
                    .expect("bool flag")
            );
            assert_eq!(
                ibm.get_item("delay_minutes")
                    .expect("delay minutes")
                    .extract::<u32>()
                    .expect("integer minutes"),
                15
            );
            assert!(
                delayed
                    .get_item("MSFT US Equity")
                    .expect("real-time ticker")
                    .get_item("delay_minutes")
                    .expect("delay minutes")
                    .is_none()
            );
        });
    }

    #[test]
    fn column_has_value_matches_the_row_wise_cell_rule() {
        const LONG: &str = "a value longer than twelve bytes";
        let text = |values: Vec<Option<&str>>| Arc::new(StringArray::from(values)) as ArrayRef;
        // Null slots that still span value bytes.
        let (offsets, values, _) = StringArray::from(vec!["abc", ""]).into_parts();
        let masked_utf8 = StringArray::new(offsets, values, validity(&[false, true]).finish());
        let (offsets, values, _) = LargeStringArray::from(vec!["abc", "d"]).into_parts();
        let masked_large =
            LargeStringArray::new(offsets, values, validity(&[false, true]).finish());
        let (views, buffers, _) = StringViewArray::from(vec![LONG, ""]).into_parts();
        let masked_view = StringViewArray::new(views, buffers, validity(&[false, true]).finish());
        let cases: Vec<(ArrayRef, bool)> = vec![
            (text(vec![]), false),
            (text(vec![None, Some("")]), false),
            (text(vec![None, Some(""), Some("x")]), true),
            (Arc::new(masked_utf8), false),
            (text(vec![Some("x"), Some(""), None]).slice(1, 2), false),
            (Arc::new(masked_large), true),
            (Arc::new(LargeStringArray::from(vec![Some("")])), false),
            (Arc::new(StringViewArray::from(vec![Some(""), None])), false),
            (
                Arc::new(StringViewArray::from(vec![None, Some(LONG)])),
                true,
            ),
            (Arc::new(masked_view), false),
            (Arc::new(Int64Array::from(vec![None, None])), false),
            (Arc::new(Int64Array::from(vec![None, Some(0)])), true),
            (Arc::new(Float64Array::from(vec![f64::NAN])), true),
            (Arc::new(BooleanArray::from(vec![Some(false)])), true),
            (Arc::new(UInt64Array::from(vec![u64::MAX])), true),
            (Arc::new(Time32SecondArray::from(vec![1])), true),
            (Arc::new(Date32Array::from(vec![i32::MAX])), false),
            (
                Arc::new(Date32Array::from(vec![Some(i32::MIN), None, Some(0)])),
                true,
            ),
            (Arc::new(Date64Array::from(vec![i64::MAX])), false),
            (Arc::new(Date64Array::from(vec![86_400_000])), true),
            (Arc::new(TimestampSecondArray::from(vec![i64::MAX])), false),
            (Arc::new(TimestampSecondArray::from(vec![1])), true),
            (
                Arc::new(TimestampMillisecondArray::from(vec![i64::MIN])),
                false,
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![i64::MIN])),
                true,
            ),
            (Arc::new(BinaryArray::from(vec![b"x".as_slice()])), false),
            (Arc::new(Decimal128Array::from(vec![1_i128])), false),
            (Arc::new(DurationSecondArray::from(vec![1_i64])), false),
            (Arc::new(NullArray::new(3)), false),
            (
                Arc::new(["x"].into_iter().collect::<DictionaryArray<Int32Type>>()),
                false,
            ),
        ];
        for (array, expected) in cases {
            let label = format!("{} {array:?}", array.data_type());
            assert_eq!(
                reference_has_value(array.as_ref()),
                expected,
                "row-wise: {label}"
            );
            assert_eq!(
                column_has_value(array.as_ref()),
                expected,
                "column: {label}"
            );
        }
    }

    #[test]
    fn has_any_value_checks_named_columns_across_batches() {
        Python::initialize();
        Python::attach(|py| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("brokerBuyCode", DataType::Utf8, true),
                Field::new("size", DataType::Float64, true),
            ]));
            let batch = |codes: Vec<Option<&str>>| {
                let sizes = Float64Array::from(vec![None::<f64>; codes.len()]);
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(codes)), Arc::new(sizes)],
                )
                .unwrap()
            };
            let table = ArrowTable::try_new(vec![
                batch(vec![Some(""), None]),
                batch(vec![None, Some("DLR1")]),
            ])
            .unwrap();
            assert!(table.has_any_value(py, vec!["missing".into(), "brokerBuyCode".into()]));
            assert!(!table.has_any_value(py, vec!["size".into(), "missing".into()]));
            assert!(!table.has_any_value(py, Vec::new()));
        });
    }

    #[test]
    fn period_labels_match_row_wise_labels_for_every_periodicity() {
        let texts = vec![
            Some("2024-03-15"),
            Some("20240315"),
            Some("2024-12-30T10:00:00Z"),
            Some("not-a-date"),
            Some(""),
            None,
            // Byte 10 falls inside 'é', so there is no ISO date prefix.
            Some("2024-03-1é"),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![
                Some(epoch_days(2024, 3, 15)),
                None,
                Some(i32::MAX),
                Some(epoch_days(2024, 12, 30)),
                Some(epoch_days(7, 7, 4)),
                Some(epoch_days(-1, 12, 31)),
                Some(epoch_days(10_000, 1, 1)),
            ])),
            Arc::new(StringArray::from(texts.clone())),
            Arc::new(LargeStringArray::from(texts.clone())),
            Arc::new(StringViewArray::from(texts)),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                None,
                Some(i64::MAX),
            ])),
            Arc::new(Date64Array::from(vec![Some(86_400_000), None])),
            Arc::new(Int64Array::from(vec![Some(20_240_315), None])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ];
        let periodicities = [
            None,
            Some("daily"),
            Some("Y"),
            Some("yearly"),
            Some("S"),
            Some("semi_annually"),
            Some("SemiAnnually"),
            Some("q"),
            Some("Quarterly"),
            Some("M"),
            Some("monthly"),
            Some("w"),
            Some("WEEKLY"),
            Some("bogus"),
        ];
        for array in &arrays {
            for periodicity in periodicities {
                let labels = period_labels(array.as_ref(), PeriodLabel::parse(periodicity));
                let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(labels.null_count(), 0);
                let expected = (0..array.len())
                    .map(|row| reference_period_label(array.as_ref(), row, periodicity))
                    .collect::<Vec<_>>();
                let actual = labels
                    .iter()
                    .map(|label| label.unwrap().to_string())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{} {periodicity:?}", array.data_type());
            }
        }
    }

    #[test]
    fn historical_presentation_matches_cell_built_period_columns() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new("date", DataType::Date32, true),
                Field::new("field", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ])
            .with_metadata(HashMap::from([(
                METADATA_KEY_BLOOMBERG_REQUEST_ID.to_string(),
                "sdk-synthetic".to_string(),
            )])),
        );
        let batch = |tickers: Vec<&str>, days: Vec<Option<i32>>| {
            let fields = vec!["PX_LAST"; tickers.len()];
            let values = (0..tickers.len())
                .map(|value| Some(value as f64))
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(tickers)),
                    Arc::new(Date32Array::from(days)),
                    Arc::new(StringArray::from(fields)),
                    Arc::new(Float64Array::from(values)),
                ],
            )
            .unwrap()
        };
        let data = TableData::try_new(vec![
            batch(
                vec!["IBM US Equity", "AAPL US Equity"],
                vec![Some(epoch_days(2024, 6, 28)), None],
            ),
            batch(vec!["AAPL US Equity"], vec![Some(epoch_days(2024, 7, 1))]),
        ])
        .unwrap();
        let period_cells = |table: &TableData, periodicity: Option<&str>| {
            let date_idx = table.schema.index_of("date").unwrap();
            table
                .batches
                .iter()
                .flat_map(|batch| {
                    let dates = batch.column(date_idx).clone();
                    (0..dates.len()).map(move |row| {
                        CellValue::Text(reference_period_label(dates.as_ref(), row, periodicity))
                    })
                })
                .collect::<Vec<_>>()
        };

        // PERIODIC replaces the date column with its labels.
        let periodic =
            historical_presentation(&data, None, Some("periodic"), None, Some("q"), "ticker")
                .unwrap();
        let expected = data
            .with_cells_column(1, "period", &period_cells(&data, Some("q")), true, true)
            .unwrap();
        assert_eq!(periodic.schema, expected.schema);
        assert_eq!(periodic.batches, expected.batches);

        // BOTH sorts first, then inserts the labels after the date column.
        let both = historical_presentation(
            &data,
            None,
            Some("Both"),
            Some("descending"),
            Some("W"),
            "ticker",
        )
        .unwrap();
        let sorted = data
            .sort_by(
                &[
                    ("ticker".to_string(), SortDirection::Ascending),
                    ("date".to_string(), SortDirection::Descending),
                    ("field".to_string(), SortDirection::Ascending),
                ],
                false,
            )
            .unwrap();
        let expected = sorted
            .with_cells_column(2, "period", &period_cells(&sorted, Some("W")), false, true)
            .unwrap();
        assert_eq!(both.schema, expected.schema);
        assert_eq!(both.batches, expected.batches);

        let hidden =
            historical_presentation(&data, Some(false), Some("BOTH"), None, None, "ticker")
                .unwrap();
        assert_eq!(hidden.column_names(), ["ticker", "field", "value"]);
    }

    #[test]
    fn bqr_reshape_matches_the_cell_map_reshape() {
        let generic = Arc::new(
            Schema::new(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("value_str", DataType::Utf8, true),
                Field::new("value_num", DataType::Float64, true),
            ])
            .with_metadata(HashMap::from([(
                METADATA_KEY_BLOOMBERG_REQUEST_ID.to_string(),
                "sdk-synthetic".to_string(),
            )])),
        );
        let generic_batch =
            |paths: Vec<Option<&str>>, text: Vec<Option<&str>>, numbers: Vec<Option<f64>>| {
                RecordBatch::try_new(
                    generic.clone(),
                    vec![
                        Arc::new(StringArray::from(paths)),
                        Arc::new(StringArray::from(text)),
                        Arc::new(Float64Array::from(numbers)),
                    ],
                )
                .unwrap()
            };
        let quotes = TableData::try_new(vec![
            generic_batch(
                vec![
                    Some("tickData[1].time"),
                    Some("tickData[1].value"),
                    Some("tickData[0].type"),
                    Some("tickData[0].value"),
                    Some("tickData[1].brokerBuyCode"),
                    Some("garbage"),
                    None,
                ],
                vec![
                    Some("2024-01-02T10:00:00"),
                    Some(""),
                    Some("BID"),
                    None,
                    Some("DLR1"),
                    Some("x"),
                    Some("y"),
                ],
                vec![
                    None,
                    Some(101.5),
                    None,
                    Some(99.0),
                    None,
                    Some(1.0),
                    Some(2.0),
                ],
            ),
            generic_batch(
                vec![
                    // Overwrites tick 0's numeric value with text.
                    Some("tickData[0].value"),
                    Some("tickData[2].ticker"),
                    Some("tickData[2].conditionCodes"),
                    Some("tickData[x].time"),
                    Some("tickData[3]time"),
                    Some("tickData[4]."),
                    Some("tickData[5].size"),
                    Some("tickData[5].é"),
                ],
                vec![
                    Some("100.25"),
                    Some("ignored"),
                    Some("A"),
                    Some("bad"),
                    Some("bad"),
                    Some("bad"),
                    None,
                    Some("bad"),
                ],
                vec![Some(1.0), None, None, None, None, None, Some(3.0), None],
            ),
        ])
        .unwrap();
        let typed_values = single_batch_table(
            vec![
                Field::new("path", DataType::LargeUtf8, false),
                Field::new("value_str", DataType::Utf8View, true),
                Field::new("value_num", DataType::Int64, true),
            ],
            vec![
                Arc::new(LargeStringArray::from(vec![
                    "tickData[0].size",
                    "tickData[1].size",
                    "tickData[0].flag",
                    "tickData[1].flag",
                ])),
                Arc::new(StringViewArray::from(vec![
                    None,
                    None,
                    Some(""),
                    Some("a value longer than twelve bytes"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(5),
                    Some(i64::MAX),
                    None,
                    Some(7),
                ])),
            ],
        );
        let unsigned_values = single_batch_table(
            vec![
                Field::new("path", DataType::Utf8View, false),
                Field::new("value_str", DataType::Utf8, true),
                Field::new("value_num", DataType::UInt64, true),
            ],
            vec![
                Arc::new(StringViewArray::from(vec![
                    "tickData[0].count",
                    "tickData[1].count",
                    "tickData[0].note",
                ])),
                Arc::new(StringArray::from(vec![None, Some("n/a"), None])),
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(3), Some(4)])),
            ],
        );
        let date_values = single_batch_table(
            vec![
                Field::new("path", DataType::Utf8, false),
                Field::new("value_str", DataType::Utf8, true),
                Field::new("value_num", DataType::Date32, true),
            ],
            vec![
                Arc::new(StringArray::from(vec![
                    "tickData[0].settle",
                    "tickData[1].settle",
                    "tickData[1].due",
                ])),
                Arc::new(StringArray::from(vec![None, Some("TBD"), None])),
                Arc::new(Date32Array::from(vec![Some(19_000), None, Some(19_001)])),
            ],
        );
        let paths_only = single_batch_table(
            vec![Field::new("path", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec!["tickData[0].time"]))],
        );
        let numeric_paths = single_batch_table(
            vec![Field::new("path", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let no_paths = single_batch_table(
            vec![Field::new("value_str", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![Some("x")]))],
        );
        let no_rows = TableData::try_new(vec![RecordBatch::new_empty(generic.clone())]).unwrap();

        let reshape = |data: &TableData, key_column| {
            reshape_bqr_generic_table(data, "TEST Corp", key_column).unwrap()
        };
        assert_eq!(
            reshape(&quotes, "ticker").column_names(),
            [
                "ticker",
                "time",
                "type",
                "value",
                "size",
                "brokerBuyCode",
                "conditionCodes"
            ]
        );
        for (label, data) in [
            ("quotes", &quotes),
            ("typed values", &typed_values),
            ("unsigned values", &unsigned_values),
            ("date values", &date_values),
            ("paths only", &paths_only),
            ("numeric paths", &numeric_paths),
            ("no paths", &no_paths),
            ("no rows", &no_rows),
        ] {
            for key_column in ["ticker", "security"] {
                let actual = reshape(data, key_column);
                let expected = reference_reshape_bqr_generic(data, "TEST Corp", key_column);
                // The cell-map reshape dropped schema metadata.
                assert_eq!(
                    actual.schema.metadata(),
                    data.schema.metadata(),
                    "{label} {key_column}"
                );
                let actual = actual.combined_batch().unwrap();
                let expected = expected.combined_batch().unwrap();
                assert_eq!(
                    (actual.num_columns(), actual.num_rows()),
                    (expected.num_columns(), expected.num_rows()),
                    "{label} {key_column}"
                );
                for (index, field) in expected.schema().fields().iter().enumerate() {
                    let context = format!("{label} {key_column} {}", field.name());
                    let (column, reference) = (actual.column(index), expected.column(index));
                    if column.data_type() == field.data_type() {
                        assert_eq!(actual.schema().field(index), field.as_ref(), "{context}");
                        assert_eq!(column, reference, "{context}");
                        continue;
                    }
                    // The cell-map reshape widened integers to Int64; the
                    // shared reshape keeps the `value_num` type and its cells.
                    assert_eq!(actual.schema().field(index).name(), field.name());
                    assert_eq!(
                        column.data_type(),
                        data.schema
                            .field_with_name("value_num")
                            .unwrap()
                            .data_type(),
                        "{context}"
                    );
                    for row in 0..column.len() {
                        assert_eq!(
                            cell_from_array(column.as_ref(), row),
                            cell_from_array(reference.as_ref(), row),
                            "{context} row {row}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn pylist_resolves_zones_durations_and_decimals_per_column() {
        Python::initialize();
        Python::attach(|py| {
            let schema = Schema::new(vec![
                Field::new(
                    "hk",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Hong_Kong".into())),
                    true,
                ),
                Field::new(
                    "utc",
                    DataType::Timestamp(TimeUnit::Second, Some("utc".into())),
                    true,
                ),
                Field::new(
                    "naive",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new("elapsed", DataType::Duration(TimeUnit::Nanosecond), true),
                Field::new("amount", DataType::Decimal128(10, 2), true),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            Some(0),
                            None,
                            Some(1_700_000_000_123_456),
                        ])
                        .with_timezone("Asia/Hong_Kong"),
                    ),
                    Arc::new(
                        TimestampSecondArray::from(vec![Some(0), Some(86_400), None])
                            .with_timezone("utc"),
                    ),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        Some(-1),
                        None,
                        Some(1_234_567_000),
                    ])),
                    Arc::new(DurationNanosecondArray::from(vec![
                        Some(-1_500),
                        Some(90_061_000_001_000),
                        None,
                    ])),
                    Arc::new(
                        Decimal128Array::from(vec![Some(12_345), Some(-5), None])
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap();
            let rows = batch_to_pylist(py, &batch).unwrap();
            let cell = |row: usize, name: &str| rows[row].bind(py).get_item(name).unwrap();
            let iso = |row: usize, name: &str| -> Option<String> {
                let value = cell(row, name);
                (!value.is_none())
                    .then(|| value.call_method0("isoformat").unwrap().extract().unwrap())
            };

            assert_eq!(iso(0, "hk").as_deref(), Some("1970-01-01T08:00:00+08:00"));
            assert_eq!(iso(1, "hk"), None);
            assert_eq!(
                iso(2, "hk").as_deref(),
                Some("2023-11-15T06:13:20.123456+08:00")
            );
            let zone = cell(2, "hk").getattr("tzinfo").unwrap();
            assert_eq!(zone.str().unwrap().to_str().unwrap(), "Asia/Hong_Kong");
            assert_eq!(iso(0, "utc").as_deref(), Some("1970-01-01T00:00:00+00:00"));
            assert_eq!(iso(1, "utc").as_deref(), Some("1970-01-02T00:00:00+00:00"));
            assert_eq!(
                iso(0, "naive").as_deref(),
                Some("1969-12-31T23:59:59.999999+00:00")
            );
            assert_eq!(
                iso(2, "naive").as_deref(),
                Some("1970-01-01T00:00:01.234567+00:00")
            );

            let timedelta = py.import("datetime").unwrap().getattr("timedelta").unwrap();
            assert!(
                cell(0, "elapsed")
                    .eq(timedelta.call1((0, 0, -1)).unwrap())
                    .unwrap()
            );
            assert!(
                cell(1, "elapsed")
                    .eq(timedelta.call1((1, 3_661, 1)).unwrap())
                    .unwrap()
            );
            assert!(cell(2, "elapsed").is_none());

            let decimal = py.import("decimal").unwrap().getattr("Decimal").unwrap();
            assert!(cell(0, "amount").is_instance(&decimal).unwrap());
            assert_eq!(cell(0, "amount").str().unwrap().to_str().unwrap(), "123.45");
            assert_eq!(cell(1, "amount").str().unwrap().to_str().unwrap(), "-0.05");
            assert!(cell(2, "amount").is_none());

            let keys = rows[0]
                .bind(py)
                .cast::<PyDict>()
                .unwrap()
                .keys()
                .extract::<Vec<String>>()
                .unwrap();
            assert_eq!(keys, ["hk", "utc", "naive", "elapsed", "amount"]);
        });
    }

    #[test]
    fn python_scalars_raise_only_for_non_null_unrepresentable_values() {
        Python::initialize();
        Python::attach(|py| {
            let mut scratch = String::new();
            let max_days = 999_999_999_i64;
            let durations = DurationSecondArray::from(vec![
                Some(max_days * 86_400),
                Some((max_days + 1) * 86_400),
                Some(i64::MAX),
                None,
            ]);
            let column = PyScalarColumn::new(&durations);
            let days = column
                .value(py, 0, &mut scratch)
                .unwrap()
                .bind(py)
                .getattr("days")
                .unwrap()
                .extract::<i64>()
                .unwrap();
            assert_eq!(days, max_days);
            for row in [1, 2] {
                let err = column.value(py, row, &mut scratch).unwrap_err();
                assert!(err.is_instance_of::<PyOverflowError>(py), "{err}");
            }
            assert!(column.value(py, 3, &mut scratch).unwrap().is_none(py));

            let unknown =
                TimestampMicrosecondArray::from(vec![None, Some(0)]).with_timezone("Not/A_Zone");
            let column = PyScalarColumn::new(&unknown);
            assert!(column.value(py, 0, &mut scratch).unwrap().is_none(py));
            let err = column.value(py, 1, &mut scratch).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(
                err.to_string()
                    .contains("unknown timestamp timezone: Not/A_Zone")
            );
        });
    }

    #[test]
    fn from_pylist_resolves_reordered_and_repeated_keys_by_name() {
        Python::initialize();
        Python::attach(|py| {
            let rows = py
                .eval(
                    c"[{'alpha': 1, 'beta': 'x'}, {'beta': 'y', 'alpha': 2}, {'alpha': 3, 'gamma': 1.5}, {}, {'alpha': True}]",
                    None,
                    None,
                )
                .unwrap();
            let text = |value: &str| CellValue::Text(value.to_string());
            let expected = columns_to_batch(
                vec![
                    (
                        "alpha".to_string(),
                        vec![
                            CellValue::Int(1),
                            CellValue::Int(2),
                            CellValue::Int(3),
                            CellValue::Null,
                            CellValue::Bool(true),
                        ],
                    ),
                    (
                        "beta".to_string(),
                        vec![
                            text("x"),
                            text("y"),
                            CellValue::Null,
                            CellValue::Null,
                            CellValue::Null,
                        ],
                    ),
                    (
                        "gamma".to_string(),
                        vec![
                            CellValue::Null,
                            CellValue::Null,
                            CellValue::Float(1.5),
                            CellValue::Null,
                            CellValue::Null,
                        ],
                    ),
                ],
                5,
            )
            .unwrap();
            assert_eq!(rows_to_batch(&rows).unwrap(), expected);

            // A str the fast path cannot borrow falls back to the probing chain.
            let lone_surrogate = py.eval(c"'\\ud800'", None, None).unwrap();
            assert_eq!(
                py_value_to_cell(&lone_surrogate).unwrap(),
                CellValue::Text(lone_surrogate.to_string())
            );
        });
    }
}
