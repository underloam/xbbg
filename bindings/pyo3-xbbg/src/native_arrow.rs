use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchOptions,
    StringArray, StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Offset, Timelike};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{
    PyImportError, PyIndexError, PyKeyError, PyOverflowError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::marker::Ungil;
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyCapsule, PyDate, PyDateTime, PyDict, PyInt, PyIterator, PyList,
    PyMapping, PyString, PyTime, PyTuple, PyType, PyTzInfo,
};
use pyo3_arrow::ffi::{
    ArrayIterator, to_array_pycapsules, to_schema_pycapsule, to_stream_pycapsule,
};
use pyo3_arrow::{PyChunkedArray, PyRecordBatch, PyTable};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_arrow::{
    ArrowCoreError, CellValue, ColumnData, SortDirection, TableData, build_array, cell_from_array,
    cell_to_string,
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

/// Run a data-sized Arrow kernel with the interpreter detached.
///
/// Callers extract Python inputs before and publish Python results after;
/// the kernel touches only Rust data. Errors are mapped once reattached.
fn detach_core<T, F>(py: Python<'_>, kernel: F) -> PyResult<T>
where
    F: Ungil + FnOnce() -> xbbg_arrow::Result<T>,
    xbbg_arrow::Result<T>: Ungil,
{
    py.detach(kernel).map_err(core_err_to_py)
}

/// Schema-metadata JSON values at least this long are parsed detached.
///
/// Typical diagnostics metadata is well under a kilobyte and parses in
/// microseconds, less than re-acquiring a contended interpreter can cost (up
/// to one switch interval, 5 ms by default). Metadata has no size cap, though:
/// field exceptions grow with securities x failing fields. At serde_json's
/// parse rate, 64 KiB takes on the order of 100 us, the point where parsing
/// starts to outweigh the reattach cost.
const DETACHED_METADATA_PARSE_MIN_BYTES: usize = 64 * 1024;

/// Parse `len` bytes of metadata, detached once it reaches the threshold.
fn parse_metadata_value<T, F>(py: Python<'_>, len: usize, parse: F) -> T
where
    F: Ungil + FnOnce() -> T,
    T: Ungil,
{
    if len >= DETACHED_METADATA_PARSE_MIN_BYTES {
        py.detach(parse)
    } else {
        parse()
    }
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

fn parse_metadata_json(
    py: Python<'_>,
    metadata: &HashMap<String, String>,
    key: &str,
) -> PyResult<Py<PyAny>> {
    let Some(raw) = metadata.get(key) else {
        return Ok(py.None());
    };
    let value = parse_metadata_value(py, raw.len(), || {
        serde_json::from_str::<serde_json::Value>(raw)
    })
    .map_err(|err| PyValueError::new_err(format!("invalid JSON in {key}: {err}")))?;
    serde_json_value_to_py(py, &value)
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _decode_field_exceptions(py: Python<'_>, raw: String) -> PyResult<Py<PyAny>> {
    field_exceptions_to_py(py, &raw)
}

fn field_exceptions_to_py(py: Python<'_>, raw: &str) -> PyResult<Py<PyAny>> {
    let exceptions = parse_metadata_value(py, raw.len(), || FieldExceptions::from_json(raw))
        .map_err(|err| {
            PyValueError::new_err(format!(
                "invalid JSON in {METADATA_KEY_FIELD_EXCEPTIONS}: {err}"
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
        Some(raw) => field_exceptions_to_py(py, raw),
        None => Ok(py.None()),
    }
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

fn py_value_to_cell(value: &Bound<'_, PyAny>) -> PyResult<CellValue> {
    if value.is_none() {
        return Ok(CellValue::Null);
    }
    if let Ok(v) = value.extract::<bool>() {
        return Ok(CellValue::Bool(v));
    }
    if let Ok(v) = value.extract::<i64>() {
        return Ok(CellValue::Int(v));
    }
    if value.is_instance_of::<PyInt>() {
        return Err(PyOverflowError::new_err(
            "Python integer is outside the supported signed 64-bit Arrow range",
        ));
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

/// Owned per-column cells extracted from Python input, in column order.
type CellColumns = Vec<(String, Vec<CellValue>)>;

/// Iterate dict rows into owned cells: first-seen column order, with nulls for
/// keys a row lacks.
fn rows_to_cell_columns(rows: &Bound<'_, PyAny>) -> PyResult<(CellColumns, usize)> {
    let iterator = PyIterator::from_object(rows)?;
    let mut columns: CellColumns = Vec::new();
    let mut column_indices: HashMap<String, usize> = HashMap::new();
    let mut row_count = 0usize;

    for item in iterator {
        let item = item?;
        let dict = item.cast::<PyDict>().map_err(|_| {
            PyTypeError::new_err("ArrowTable.from_pylist expects an iterable of dict rows")
        })?;
        let row_idx = row_count;

        for (key, value) in dict.iter() {
            let name = key.extract::<String>()?;
            let column_idx = if let Some(&idx) = column_indices.get(&name) {
                idx
            } else {
                let idx = columns.len();
                column_indices.insert(name.clone(), idx);
                columns.push((name, vec![CellValue::Null; row_idx]));
                idx
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

/// Iterate a column mapping into owned cells, in mapping order.
fn pydict_to_cell_columns(columns: &Bound<'_, PyAny>) -> PyResult<(CellColumns, usize)> {
    let mapping = columns.cast::<PyMapping>().map_err(|_| {
        PyTypeError::new_err(
            "ArrowTable.from_pydict expects a mapping of column names to value sequences",
        )
    })?;
    let mut out: CellColumns = Vec::new();
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

/// Build typed arrays and one record batch from owned cells.
fn cell_columns_to_batch(
    columns: CellColumns,
    row_count: usize,
) -> xbbg_arrow::Result<RecordBatch> {
    if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        return Ok(RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &options,
        )?);
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (name, cells) in columns {
        let (field, array) = build_array(&name, &cells);
        fields.push(field);
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn py_sequence_to_cells(values: &Bound<'_, PyAny>) -> PyResult<Vec<CellValue>> {
    let iterator = PyIterator::from_object(values)?;
    iterator
        .map(|item| item.and_then(|value| py_value_to_cell(&value)))
        .collect()
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
    let date = xbbg_arrow::scalar::date_from_days(days).ok_or_else(|| {
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

fn timestamp_to_py(py: Python<'_>, micros: i64, timezone: Option<&str>) -> PyResult<Py<PyAny>> {
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

    let Some(tz_name) = timezone.filter(|tz| !tz.eq_ignore_ascii_case("UTC")) else {
        return Ok(utc_datetime.into_any().unbind());
    };

    if let Ok(zoneinfo) = py
        .import("zoneinfo")
        .and_then(|module| module.getattr("ZoneInfo"))
        .and_then(|zoneinfo| zoneinfo.call1((tz_name,)))
        && let Ok(converted) = utc_datetime.call_method1("astimezone", (zoneinfo,))
    {
        return Ok(converted.unbind());
    }

    let tz = tz_name
        .parse::<chrono_tz::Tz>()
        .map_err(|_| PyValueError::new_err(format!("unknown timestamp timezone: {tz_name}")))?;
    let local = dt.with_timezone(&tz);
    let offset_seconds = local.offset().fix().local_minus_utc();
    let datetime = py.import("datetime")?;
    let timedelta = datetime.getattr("timedelta")?.call1((0, offset_seconds))?;
    let fixed_tz = datetime.getattr("timezone")?.call1((timedelta, tz_name))?;

    Ok(datetime
        .getattr("datetime")?
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

fn timestamp_unit_to_py(
    py: Python<'_>,
    value: i64,
    unit: &TimeUnit,
    timezone: Option<&str>,
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
    timestamp_to_py(py, micros, timezone)
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
    Ok(py
        .import("datetime")?
        .getattr("timedelta")?
        .call1((0, seconds, micros))?
        .unbind())
}

fn decimal_to_py(py: Python<'_>, unscaled: String, scale: i8) -> PyResult<Py<PyAny>> {
    let exact = format!("{unscaled}E{}", -i32::from(scale));
    Ok(py
        .import("decimal")?
        .getattr("Decimal")?
        .call1((exact,))?
        .unbind())
}

fn scalar_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }

    macro_rules! native_scalar {
        ($array_ty:ty, $expect:literal) => {
            array
                .as_any()
                .downcast_ref::<$array_ty>()
                .expect($expect)
                .value(row)
                .into_py_any(py)
        };
    }

    match array.data_type() {
        DataType::Null => Ok(py.None()),
        DataType::Boolean => native_scalar!(BooleanArray, "BooleanArray"),
        DataType::Int8 => native_scalar!(Int8Array, "Int8Array"),
        DataType::Int16 => native_scalar!(Int16Array, "Int16Array"),
        DataType::Int32 => native_scalar!(Int32Array, "Int32Array"),
        DataType::Int64 => native_scalar!(Int64Array, "Int64Array"),
        DataType::UInt8 => native_scalar!(UInt8Array, "UInt8Array"),
        DataType::UInt16 => native_scalar!(UInt16Array, "UInt16Array"),
        DataType::UInt32 => native_scalar!(UInt32Array, "UInt32Array"),
        DataType::UInt64 => native_scalar!(UInt64Array, "UInt64Array"),
        DataType::Float16 => (array
            .as_any()
            .downcast_ref::<Float16Array>()
            .expect("Float16Array")
            .value(row)
            .to_f32() as f64)
            .into_py_any(py),
        DataType::Float32 => (array
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array")
            .value(row) as f64)
            .into_py_any(py),
        DataType::Float64 => native_scalar!(Float64Array, "Float64Array"),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray")
            .value(row)
            .into_py_any(py),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("LargeStringArray")
            .value(row)
            .into_py_any(py),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray")
            .value(row)
            .into_py_any(py),
        DataType::Binary => Ok(PyBytes::new(
            py,
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("BinaryArray")
                .value(row),
        )
        .into_any()
        .unbind()),
        DataType::LargeBinary => Ok(PyBytes::new(
            py,
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("LargeBinaryArray")
                .value(row),
        )
        .into_any()
        .unbind()),
        DataType::BinaryView => Ok(PyBytes::new(
            py,
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .expect("BinaryViewArray")
                .value(row),
        )
        .into_any()
        .unbind()),
        DataType::Date32 => date32_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        ),
        DataType::Date64 => date64_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("Date64Array")
                .value(row),
        ),
        DataType::Time32(TimeUnit::Second) => time_unit_to_py(
            py,
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .expect("Time32SecondArray")
                    .value(row),
            ),
            &TimeUnit::Second,
        ),
        DataType::Time32(TimeUnit::Millisecond) => time_unit_to_py(
            py,
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .expect("Time32MillisecondArray")
                    .value(row),
            ),
            &TimeUnit::Millisecond,
        ),
        DataType::Time64(TimeUnit::Microsecond) => time_unit_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("Time64MicrosecondArray")
                .value(row),
            &TimeUnit::Microsecond,
        ),
        DataType::Time64(TimeUnit::Nanosecond) => time_unit_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("Time64NanosecondArray")
                .value(row),
            &TimeUnit::Nanosecond,
        ),
        DataType::Timestamp(unit, timezone) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .expect("TimestampSecondArray")
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("TimestampMillisecondArray")
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("TimestampMicrosecondArray")
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("TimestampNanosecondArray")
                    .value(row),
            };
            timestamp_unit_to_py(py, value, unit, timezone.as_deref())
        }
        DataType::Duration(unit) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<DurationSecondArray>()
                    .expect("DurationSecondArray")
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .expect("DurationMillisecondArray")
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .expect("DurationMicrosecondArray")
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .expect("DurationNanosecondArray")
                    .value(row),
            };
            duration_to_py(py, value, unit)
        }
        DataType::Decimal128(_, scale) if *scale >= 0 => decimal_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128Array")
                .value(row)
                .to_string(),
            *scale,
        ),
        data_type => Err(PyTypeError::new_err(format!(
            "Arrow dtype {data_type} cannot be materialized to a Python scalar without PyArrow"
        ))),
    }
}

fn batch_to_pylist(py: Python<'_>, batch: &RecordBatch) -> PyResult<Vec<Py<PyAny>>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let dict = PyDict::new(py);
        for (column_idx, field) in batch.schema().fields().iter().enumerate() {
            dict.set_item(
                field.name(),
                scalar_to_py(py, batch.column(column_idx).as_ref(), row_idx)?,
            )?;
        }
        rows.push(dict.into_any().unbind());
    }
    Ok(rows)
}

fn table_to_pylist(py: Python<'_>, table: &TableData) -> PyResult<Vec<Py<PyAny>>> {
    let mut rows = Vec::new();
    for batch in &table.batches {
        rows.extend(batch_to_pylist(py, batch)?);
    }
    Ok(rows)
}

fn column_to_pylist(py: Python<'_>, column: &ColumnData) -> PyResult<Vec<Py<PyAny>>> {
    let mut values = Vec::with_capacity(column.len());
    for chunk in &column.chunks {
        for row in 0..chunk.len() {
            values.push(scalar_to_py(py, chunk.as_ref(), row)?);
        }
    }
    Ok(values)
}

/// Period granularity for BDH `PERIODIC`/`BOTH` date labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeriodLabel {
    Yearly,
    SemiAnnual,
    Quarterly,
    Monthly,
    Weekly,
    Daily,
}

impl PeriodLabel {
    /// Normalize a periodicity once per table; unknown values label by day.
    fn from_periodicity(periodicity: Option<&str>) -> Self {
        match periodicity.unwrap_or("DAILY").to_ascii_uppercase().as_str() {
            "YEARLY" | "Y" => Self::Yearly,
            "SEMI_ANNUALLY" | "SEMIANNUALLY" | "S" => Self::SemiAnnual,
            "QUARTERLY" | "Q" => Self::Quarterly,
            "MONTHLY" | "M" => Self::Monthly,
            "WEEKLY" | "W" => Self::Weekly,
            _ => Self::Daily,
        }
    }

    fn label(self, array: &dyn Array, row: usize) -> String {
        let Some(parsed) = xbbg_arrow::scalar::date_from_array(array, row) else {
            return cell_to_string(&cell_from_array(array, row)).unwrap_or_default();
        };
        match self {
            Self::Yearly => format!("{:04}", parsed.year()),
            Self::SemiAnnual => {
                let half = if parsed.month() <= 6 { 1 } else { 2 };
                format!("{:04}H{}", parsed.year(), half)
            }
            Self::Quarterly => {
                let quarter = ((parsed.month() - 1) / 3) + 1;
                format!("{:04}Q{}", parsed.year(), quarter)
            }
            Self::Monthly => format!("{:04}-{:02}", parsed.year(), parsed.month()),
            Self::Weekly => {
                let iso = parsed.iso_week();
                format!("{:04}-W{:02}", iso.year(), iso.week())
            }
            Self::Daily => parsed.to_string(),
        }
    }
}

/// Apply BDH presentation options: sort, period labels, and date visibility.
///
/// - `sort` orders by `ticker` ascending, `date` (descending only for
///   `"DESCENDING"`), then `field` ascending, each when present, nulls last.
/// - `date_format` `PERIODIC` replaces `date` with a UTF-8 `period` column;
///   `BOTH` inserts `period` after `date`.
/// - `show_date=Some(false)` drops `date` and `period`.
///
/// Pure Arrow work with no Python state: `apply_historical_presentation`
/// runs it detached and `apresent_historical` on the blocking pool.
pub(crate) fn historical_presentation(
    data: &TableData,
    show_date: Option<bool>,
    date_format: Option<&str>,
    sort: Option<&str>,
    periodicity: Option<&str>,
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
        if result.schema.index_of("ticker").is_ok() {
            sort_keys.push(("ticker".to_string(), SortDirection::Ascending));
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
            let label = PeriodLabel::from_periodicity(periodicity);
            let mut period_cells = Vec::with_capacity(result.num_rows());
            for batch in &result.batches {
                let date_array = batch.column(date_idx).as_ref();
                period_cells.extend(
                    (0..date_array.len()).map(|row| CellValue::Text(label.label(date_array, row))),
                );
            }
            result = if periodic {
                result.with_cells_column(date_idx, "period", &period_cells, true, true)?
            } else {
                result.with_cells_column(date_idx + 1, "period", &period_cells, false, true)?
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
        detach_core(py, || self.data.compact()).map(Self::new)
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = PyList::empty(py);
        for value in column_to_pylist(py, &self.data)? {
            values.append(value.bind(py))?;
        }
        Ok(values.into_any().unbind())
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
        let table = detach_core(py, || {
            TableData::try_new(vec![self.batch.clone()])?.compact()
        })?;
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

    /// Build arrays and the table from extracted cells with the interpreter detached.
    fn from_cell_columns(py: Python<'_>, columns: CellColumns, row_count: usize) -> PyResult<Self> {
        detach_core(py, move || {
            TableData::try_new(vec![cell_columns_to_batch(columns, row_count)?])
        })
        .map(Self::from_data)
    }

    pub(crate) fn table_data(&self) -> &TableData {
        &self.data
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
        cls: &Bound<'_, PyType>,
        rows: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if schema.is_some() {
            return Err(PyValueError::new_err(
                "ArrowTable.from_pylist schema override is not implemented; pass dict rows with desired keys",
            ));
        }
        let (columns, row_count) = rows_to_cell_columns(rows)?;
        Self::from_cell_columns(cls.py(), columns, row_count)
    }

    /// Build a table from a mapping of column name to values, in mapping order.
    ///
    /// Cells are typed like `from_pylist`; every column must have the same
    /// length.
    #[classmethod]
    fn from_pydict(cls: &Bound<'_, PyType>, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let (columns, row_count) = pydict_to_cell_columns(columns)?;
        Self::from_cell_columns(cls.py(), columns, row_count)
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
    fn from_batches(cls: &Bound<'_, PyType>, batches: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_batches = Vec::new();
        for item in PyIterator::from_object(batches)? {
            let item = item?;
            let batch = item.extract::<PyRef<'_, ArrowRecordBatch>>()?;
            rust_batches.push(batch.batch.clone());
        }
        detach_core(cls.py(), move || TableData::try_new(rust_batches)).map(Self::from_data)
    }

    #[classmethod]
    fn concat_tables(cls: &Bound<'_, PyType>, tables: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_tables = Vec::new();
        for item in PyIterator::from_object(tables)? {
            let item = item?;
            let table = item.extract::<PyRef<'_, ArrowTable>>()?;
            rust_tables.push(table.data.clone());
        }
        detach_core(cls.py(), move || TableData::concat_tables(&rust_tables)).map(Self::from_data)
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
        detach_core(py, || self.data.combined_batch()).map(ArrowRecordBatch::new)
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
        detach_core(py, || self.data.compact()).map(Self::from_data)
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
        detach_core(py, || self.data.sort_by(&keys, !nulls_last)).map(Self::from_data)
    }

    fn filter_eq(&self, py: Python<'_>, column: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let needle = py_value_to_cell(value)?;
        let column_index = map_core(self.data.column_index(column))?;
        let data_type = self.data.schema.field(column_index).data_type();
        if !filter_value_supported(data_type, &needle) {
            return Err(PyTypeError::new_err(format!(
                "cannot compare Python value to Arrow column {column:?} with dtype {data_type}"
            )));
        }
        detach_core(py, || self.data.filter_eq(column, &needle)).map(Self::from_data)
    }

    fn add_column(
        &self,
        py: Python<'_>,
        index: usize,
        name: &str,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        detach_core(py, || self.data.add_column(index, name, &cells)).map(Self::from_data)
    }

    fn set_column(
        &self,
        py: Python<'_>,
        index: usize,
        name: &str,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        detach_core(py, || self.data.set_column(index, name, &cells)).map(Self::from_data)
    }

    fn head(&self, n: usize) -> PyResult<Self> {
        map_core(self.data.head(n)).map(Self::from_data)
    }

    /// Whether any named column holds a value other than null or empty text.
    ///
    /// Unknown column names are ignored.
    fn has_any_value(&self, py: Python<'_>, names: Vec<String>) -> bool {
        py.detach(|| self.data.has_any_value(&names))
    }

    fn apply_historical_presentation(
        &self,
        py: Python<'_>,
        show_date: Option<bool>,
        date_format: Option<String>,
        sort: Option<String>,
        periodicity: Option<String>,
    ) -> PyResult<Self> {
        detach_core(py, || {
            historical_presentation(
                &self.data,
                show_date,
                date_format.as_deref(),
                sort.as_deref(),
                periodicity.as_deref(),
            )
        })
        .map(Self::from_data)
    }

    /// Pivot generic BQR `path`/`value_str`/`value_num` rows into one row per tick.
    ///
    /// Uses the BQR quote shaper's reshape, as `ashape_bqr` does: fields holding
    /// only numbers are Float64, other fields UTF-8, and schema metadata is
    /// kept. Without tick rows the result is the empty
    /// `ticker`/`time`/`type`/`value`/`size` table.
    fn reshape_bqr_generic(&self, py: Python<'_>, ticker: &str) -> PyResult<Self> {
        py.detach(|| reshape_bqr_generic_table(&self.data, ticker))
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
    use super::*;

    #[test]
    fn detach_core_runs_kernels_detached_and_maps_errors_after_reattaching() {
        Python::initialize();
        Python::attach(|py| {
            assert_ne!(unsafe { pyo3::ffi::PyGILState_Check() }, 0);
            let attached_inside = detach_core(py, || Ok(unsafe { pyo3::ffi::PyGILState_Check() }));
            assert_eq!(attached_inside.unwrap(), 0);
            assert_ne!(unsafe { pyo3::ffi::PyGILState_Check() }, 0);

            let error = |err: ArrowCoreError| detach_core::<(), _>(py, || Err(err)).unwrap_err();
            let unknown = error(ArrowCoreError::UnknownColumn("missing".into()));
            assert!(unknown.is_instance_of::<PyKeyError>(py));
            assert!(unknown.to_string().contains("unknown column: missing"));
            for index_error in [
                ArrowCoreError::ColumnIndexOutOfRange,
                ArrowCoreError::RowIndexOutOfRange,
            ] {
                assert!(error(index_error).is_instance_of::<PyIndexError>(py));
            }
            for value_error in [
                ArrowCoreError::IncompatibleSchemas,
                ArrowCoreError::ColumnLengthMismatch {
                    actual: 1,
                    expected: 2,
                },
            ] {
                assert!(error(value_error).is_instance_of::<PyValueError>(py));
            }
        });
    }

    #[test]
    fn field_exceptions_above_the_detach_threshold_decode_like_small_ones() {
        Python::initialize();
        Python::attach(|py| {
            let entry = serde_json::json!({
                "field": "PX_BAD", "category": "BAD_FLD", "code": 2,
                "subcategory": "INVALID", "message": "bad field"
            });
            let securities = (0..4_000)
                .map(|idx| (format!("SYNTH{idx:05} Equity"), serde_json::json!([0])))
                .collect::<serde_json::Map<_, _>>();
            let raw =
                serde_json::json!({"version": 1, "entries": [entry], "securities": securities})
                    .to_string();
            assert!(raw.len() >= DETACHED_METADATA_PARSE_MIN_BYTES);

            let decoded = _decode_field_exceptions(py, raw.clone()).unwrap();
            let decoded = decoded.bind(py).cast::<PyDict>().unwrap().clone();
            assert_eq!(decoded.len(), 4_000);
            let first = decoded.get_item("SYNTH00000 Equity").unwrap().unwrap();
            assert!(
                first
                    .get_item(0)
                    .unwrap()
                    .get_item("field")
                    .unwrap()
                    .eq("PX_BAD")
                    .unwrap()
            );

            let malformed = format!("{}x", &raw[..raw.len() - 1]);
            let err = _decode_field_exceptions(py, malformed).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains(METADATA_KEY_FIELD_EXCEPTIONS));
        });
    }

    #[test]
    fn logical_nbytes_counts_one_cell_of_large_variable_width_columns() {
        Python::initialize();
        Python::attach(|py| {
            let cells = (0..1_000)
                .map(|row| format!("{row:04}").repeat(25))
                .collect::<Vec<_>>();
            let text = cells.iter().map(String::as_str).collect::<Vec<_>>();
            let bytes = cells.iter().map(String::as_bytes).collect::<Vec<_>>();
            let columns: Vec<(ArrayRef, usize)> = vec![
                (Arc::new(StringArray::from(text.clone())), 2 * 4 + 100),
                (Arc::new(LargeStringArray::from(text.clone())), 2 * 8 + 100),
                (Arc::new(BinaryArray::from(bytes)), 2 * 4 + 100),
                (
                    Arc::new(StringViewArray::from_iter_values(text.iter().copied())),
                    16 + 100,
                ),
            ];
            for (array, expected) in columns {
                let data_type = array.data_type().clone();
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "payload",
                    data_type.clone(),
                    false,
                )]));
                let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                let batch = record_batch_to_arrow_record_batch(py, batch).unwrap();
                let cell = batch
                    .bind(py)
                    .call_method1("column", ("payload",))
                    .unwrap()
                    .call_method1("slice", (500, 1))
                    .unwrap();
                let logical = cell
                    .getattr("logical_nbytes")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap();
                let physical = cell.getattr("nbytes").unwrap().extract::<usize>().unwrap();
                assert_eq!(logical, expected, "{data_type}");
                assert!(physical >= 100_000, "{data_type}");
            }
        });
    }

    #[test]
    fn metadata_sizes_and_bounded_values_match_the_metadata_mapping() {
        Python::initialize();
        Python::attach(|py| {
            let metadata = HashMap::from([
                ("xbbg.synthetic".to_string(), "\u{e9}".repeat(10)),
                (
                    METADATA_KEY_DELAYED.to_string(),
                    r#"{"SYN1 Equity":{"delayed":false,"delay_minutes":null}}"#.to_string(),
                ),
            ]);
            let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, false)])
                .with_metadata(metadata.clone());
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(StringArray::from(vec!["SYN1 Equity"]))],
            )
            .unwrap();
            let batch = record_batch_to_arrow_record_batch(py, batch).unwrap();
            let table = batch.bind(py).call_method0("to_table").unwrap();
            for carrier in [batch.bind(py), &table] {
                let full = carrier
                    .getattr("metadata")
                    .unwrap()
                    .extract::<HashMap<String, String>>()
                    .unwrap();
                assert_eq!(full, metadata);
                let sizes = carrier
                    .call_method0("metadata_sizes")
                    .unwrap()
                    .extract::<HashMap<String, usize>>()
                    .unwrap();
                assert_eq!(
                    sizes,
                    full.iter()
                        .map(|(key, value)| (key.clone(), value.len()))
                        .collect::<HashMap<_, _>>()
                );
                for (key, value) in &full {
                    let fetched = carrier
                        .call_method1("metadata_value", (key,))
                        .unwrap()
                        .extract::<String>()
                        .unwrap();
                    assert_eq!(&fetched, value);
                }
                assert!(
                    carrier
                        .call_method1("metadata_value", ("absent",))
                        .unwrap()
                        .is_none()
                );
                let prefix = |max_bytes: usize| {
                    carrier
                        .call_method1("metadata_value", ("xbbg.synthetic", max_bytes))
                        .unwrap()
                        .extract::<String>()
                        .unwrap()
                };
                // Each character is two UTF-8 bytes: odd bounds cut back a byte.
                assert_eq!(prefix(5), "\u{e9}".repeat(2));
                assert_eq!(prefix(4), "\u{e9}".repeat(2));
                assert_eq!(prefix(1), "");
                assert_eq!(prefix(0), "");
                assert_eq!(prefix(20), "\u{e9}".repeat(10));
                assert_eq!(prefix(1_000), "\u{e9}".repeat(10));
            }
        });
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
            let (columns, row_count) = rows_to_cell_columns(&rows).unwrap();
            let empty = cell_columns_to_batch(columns, row_count).unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.num_columns(), 0);

            let rows = PyList::empty(py);
            rows.append(PyDict::new(py)).unwrap();
            rows.append(PyDict::new(py)).unwrap();
            let (columns, row_count) = rows_to_cell_columns(&rows.into_any()).unwrap();
            let empty_columns = cell_columns_to_batch(columns, row_count).unwrap();
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
}
