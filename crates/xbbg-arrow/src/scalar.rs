//! Scalar conversion helpers for xbbg Arrow carrier data.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType, TimestampSecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, PrimitiveArray,
    Scalar, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::BooleanBuffer;
use arrow_ord::cmp::eq;
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};
use chrono::{Datelike, NaiveDate};

/// Scalar values used by xbbg's native carrier operations.
#[derive(Clone, Debug, PartialEq)]
pub enum CellValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Date(NaiveDate),
    Timestamp(i64),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InferredKind {
    Bool,
    Int,
    Float,
    Date,
    Timestamp,
    Text,
}

fn merge_kind(current: Option<InferredKind>, value: &CellValue) -> Option<InferredKind> {
    let next = match value {
        CellValue::Null => return current,
        CellValue::Bool(_) => InferredKind::Bool,
        CellValue::Int(_) => InferredKind::Int,
        CellValue::Float(_) => InferredKind::Float,
        CellValue::Date(_) => InferredKind::Date,
        CellValue::Timestamp(_) => InferredKind::Timestamp,
        CellValue::Text(_) => InferredKind::Text,
    };
    Some(match (current, next) {
        (None, kind) => kind,
        (Some(InferredKind::Text), _) | (_, InferredKind::Text) => InferredKind::Text,
        (Some(InferredKind::Date), InferredKind::Date) => InferredKind::Date,
        (Some(InferredKind::Timestamp), InferredKind::Timestamp) => InferredKind::Timestamp,
        (Some(InferredKind::Date | InferredKind::Timestamp), _)
        | (_, InferredKind::Date | InferredKind::Timestamp) => InferredKind::Text,
        (
            Some(InferredKind::Float),
            InferredKind::Int | InferredKind::Bool | InferredKind::Float,
        )
        | (Some(InferredKind::Int | InferredKind::Bool), InferredKind::Float) => {
            InferredKind::Float
        }
        (Some(InferredKind::Int), InferredKind::Bool | InferredKind::Int)
        | (Some(InferredKind::Bool), InferredKind::Int) => InferredKind::Int,
        (Some(InferredKind::Bool), InferredKind::Bool) => InferredKind::Bool,
    })
}
fn integer_exactly_representable_as_f64(value: i64) -> bool {
    i128::from(value) == (value as f64) as i128
}

pub(crate) fn infer_kind(cells: &[CellValue]) -> InferredKind {
    let mut kind = None;
    let mut has_integer = false;
    for cell in cells {
        has_integer |= matches!(cell, CellValue::Int(_));
        kind = merge_kind(kind, cell);
        if kind == Some(InferredKind::Text) {
            return InferredKind::Text;
        }
    }

    let kind = kind.unwrap_or(InferredKind::Text);
    if kind == InferredKind::Float
        && has_integer
        && cells.iter().any(|cell| {
            matches!(cell, CellValue::Int(value) if !integer_exactly_representable_as_f64(*value))
        })
    {
        InferredKind::Text
    } else {
        kind
    }
}

/// Convert a carrier scalar to a string representation, preserving nulls.
pub fn cell_to_string(value: &CellValue) -> Option<String> {
    match value {
        CellValue::Null => None,
        CellValue::Bool(v) => Some(v.to_string()),
        CellValue::Int(v) => Some(v.to_string()),
        CellValue::Float(v) => Some(v.to_string()),
        CellValue::Date(v) => Some(v.to_string()),
        CellValue::Timestamp(v) => chrono::DateTime::from_timestamp_micros(*v)
            .map(|value| value.to_rfc3339())
            .or_else(|| Some(v.to_string())),
        CellValue::Text(v) => Some(v.clone()),
    }
}

/// Build an Arrow array from xbbg carrier scalar values, inferring a narrow type.
pub fn build_array(name: &str, cells: &[CellValue]) -> (Field, ArrayRef) {
    build_array_for_kind(name, cells, infer_kind(cells))
}

pub(crate) fn build_array_for_kind(
    name: &str,
    cells: &[CellValue],
    kind: InferredKind,
) -> (Field, ArrayRef) {
    match kind {
        InferredKind::Bool => {
            let mut builder = BooleanBuilder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    CellValue::Null => builder.append_null(),
                    CellValue::Bool(value) => builder.append_value(*value),
                    CellValue::Int(value) => builder.append_value(*value != 0),
                    CellValue::Float(value) => builder.append_value(*value != 0.0),
                    CellValue::Date(_) | CellValue::Timestamp(_) | CellValue::Text(_) => {
                        builder.append_null()
                    }
                }
            }
            (
                Field::new(name, DataType::Boolean, true),
                Arc::new(builder.finish()),
            )
        }
        InferredKind::Int => {
            let mut builder = Int64Builder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    CellValue::Null => builder.append_null(),
                    CellValue::Bool(value) => builder.append_value(i64::from(*value)),
                    CellValue::Int(value) => builder.append_value(*value),
                    CellValue::Float(value) => builder.append_value(*value as i64),
                    CellValue::Date(_) | CellValue::Timestamp(_) | CellValue::Text(_) => {
                        builder.append_null()
                    }
                }
            }
            (
                Field::new(name, DataType::Int64, true),
                Arc::new(builder.finish()),
            )
        }
        InferredKind::Float => {
            let mut builder = Float64Builder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    CellValue::Null => builder.append_null(),
                    CellValue::Bool(value) => builder.append_value(if *value { 1.0 } else { 0.0 }),
                    CellValue::Int(value) => builder.append_value(*value as f64),
                    CellValue::Float(value) => builder.append_value(*value),
                    CellValue::Date(_) | CellValue::Timestamp(_) | CellValue::Text(_) => {
                        builder.append_null()
                    }
                }
            }
            (
                Field::new(name, DataType::Float64, true),
                Arc::new(builder.finish()),
            )
        }
        InferredKind::Date => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date");
            let mut builder = Date32Builder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    CellValue::Date(value) => {
                        builder.append_value(value.signed_duration_since(epoch).num_days() as i32)
                    }
                    _ => builder.append_null(),
                }
            }
            (
                Field::new(name, DataType::Date32, true),
                Arc::new(builder.finish()),
            )
        }
        InferredKind::Timestamp => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    CellValue::Timestamp(value) => builder.append_value(*value),
                    _ => builder.append_null(),
                }
            }
            (
                Field::new(
                    name,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Arc::new(builder.finish().with_timezone("UTC")),
            )
        }
        InferredKind::Text => build_text_array(name, cells),
    }
}

pub(crate) fn build_text_array(name: &str, cells: &[CellValue]) -> (Field, ArrayRef) {
    let data_capacity = cells.iter().fold(0usize, |capacity, cell| {
        let bytes = match cell {
            CellValue::Null => 0,
            CellValue::Bool(_) => 5,
            CellValue::Int(_) => 20,
            CellValue::Float(_) => 24,
            CellValue::Date(_) => 10,
            CellValue::Timestamp(_) => 32,
            CellValue::Text(value) => value.len(),
        };
        capacity.saturating_add(bytes)
    });
    let mut builder = StringBuilder::with_capacity(cells.len(), data_capacity);
    let mut scratch = String::new();
    for cell in cells {
        match cell {
            CellValue::Null => builder.append_null(),
            CellValue::Bool(value) => builder.append_value(if *value { "true" } else { "false" }),
            CellValue::Text(value) => builder.append_value(value.as_str()),
            CellValue::Int(value) => {
                scratch.clear();
                write!(scratch, "{value}").expect("writing to a String cannot fail");
                builder.append_value(scratch.as_str());
            }
            CellValue::Float(value) => {
                scratch.clear();
                write!(scratch, "{value}").expect("writing to a String cannot fail");
                builder.append_value(scratch.as_str());
            }
            CellValue::Date(value) => {
                scratch.clear();
                write!(scratch, "{value}").expect("writing to a String cannot fail");
                builder.append_value(scratch.as_str());
            }
            CellValue::Timestamp(value) => {
                scratch.clear();
                if let Some(value) = chrono::DateTime::from_timestamp_micros(*value) {
                    write!(scratch, "{}", value.to_rfc3339())
                        .expect("writing to a String cannot fail");
                } else {
                    write!(scratch, "{value}").expect("writing to a String cannot fail");
                }
                builder.append_value(scratch.as_str());
            }
        }
    }
    (
        Field::new(name, DataType::Utf8, true),
        Arc::new(builder.finish()),
    )
}

/// Convert date32 days from Unix epoch to a [`NaiveDate`].
pub fn date_from_days(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::days(days as i64))
}

/// Convert an Arrow scalar at `row` to xbbg's carrier scalar representation.
///
/// Out-of-range rows, nulls, and unsupported data types all yield
/// [`CellValue::Null`] (absent) rather than a fabricated value.
pub fn cell_from_array(array: &dyn Array, row: usize) -> CellValue {
    if row >= array.len() || array.is_null(row) {
        return CellValue::Null;
    }
    match array.data_type() {
        DataType::Boolean => CellValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray")
                .value(row),
        ),
        DataType::Int8 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8Array")
                .value(row),
        )),
        DataType::Int16 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16Array")
                .value(row),
        )),
        DataType::Int32 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array")
                .value(row),
        )),
        DataType::Int64 => CellValue::Int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array")
                .value(row),
        ),
        DataType::UInt8 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("UInt8Array")
                .value(row),
        )),
        DataType::UInt16 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("UInt16Array")
                .value(row),
        )),
        DataType::UInt32 => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("UInt32Array")
                .value(row),
        )),
        DataType::UInt64 => {
            let value = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64Array")
                .value(row);
            i64::try_from(value)
                .map(CellValue::Int)
                .unwrap_or_else(|_| CellValue::Text(value.to_string()))
        }
        DataType::Float16 => CellValue::Float(
            array
                .as_any()
                .downcast_ref::<Float16Array>()
                .expect("Float16Array")
                .value(row)
                .to_f32() as f64,
        ),
        DataType::Float32 => CellValue::Float(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32Array")
                .value(row) as f64,
        ),
        DataType::Float64 => CellValue::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array")
                .value(row),
        ),
        DataType::Utf8 => CellValue::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray")
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => CellValue::Text(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeStringArray")
                .value(row)
                .to_string(),
        ),
        DataType::Utf8View => CellValue::Text(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("StringViewArray")
                .value(row)
                .to_string(),
        ),
        DataType::Date32 => date_from_days(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        )
        .map(CellValue::Date)
        .unwrap_or(CellValue::Null),
        DataType::Date64 => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("Date64Array")
                .value(row);
            i32::try_from(millis.div_euclid(86_400_000))
                .ok()
                .and_then(date_from_days)
                .map(CellValue::Date)
                .unwrap_or(CellValue::Null)
        }
        DataType::Time32(TimeUnit::Second) => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .expect("Time32SecondArray")
                .value(row),
        )),
        DataType::Time32(TimeUnit::Millisecond) => CellValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .expect("Time32MillisecondArray")
                .value(row),
        )),
        DataType::Time64(TimeUnit::Microsecond) => CellValue::Int(
            array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("Time64MicrosecondArray")
                .value(row),
        ),
        DataType::Time64(TimeUnit::Nanosecond) => CellValue::Int(
            array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("Time64NanosecondArray")
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("TimestampSecondArray")
            .value(row)
            .checked_mul(1_000_000)
            .map(CellValue::Timestamp)
            .unwrap_or(CellValue::Null),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("TimestampMillisecondArray")
            .value(row)
            .checked_mul(1_000)
            .map(CellValue::Timestamp)
            .unwrap_or(CellValue::Null),
        DataType::Timestamp(TimeUnit::Microsecond, _) => CellValue::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimestampMicrosecondArray")
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => CellValue::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("TimestampNanosecondArray")
                .value(row)
                .div_euclid(1_000),
        ),
        // Unsupported dtype: report absent instead of fabricating a value
        // (the old behavior Debug-dumped the entire array into every cell).
        _ => CellValue::Null,
    }
}

/// Whether a carrier scalar should count as a present value.
pub fn cell_has_value(cell: &CellValue) -> bool {
    match cell {
        CellValue::Null => false,
        CellValue::Text(text) => !text.is_empty(),
        _ => true,
    }
}

/// Parse a carrier scalar as a date when possible.
pub fn date_from_cell(cell: &CellValue) -> Option<NaiveDate> {
    match cell {
        CellValue::Date(value) => Some(*value),
        CellValue::Timestamp(value) => {
            chrono::DateTime::from_timestamp_micros(*value).map(|value| value.date_naive())
        }
        CellValue::Text(value) if value.len() >= 10 => value
            .get(..10)
            .and_then(|prefix| NaiveDate::parse_from_str(prefix, "%Y-%m-%d").ok()),
        CellValue::Text(value) if value.len() == 8 => {
            NaiveDate::parse_from_str(value, "%Y%m%d").ok()
        }
        _ => None,
    }
}

/// Parse an Arrow scalar at `row` as a date when possible.
pub fn date_from_array(array: &dyn Array, row: usize) -> Option<NaiveDate> {
    if row >= array.len() || array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Date32 => date_from_days(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        ),
        _ => date_from_cell(&cell_from_array(array, row)),
    }
}

#[derive(Clone, Copy)]
enum Number {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

fn number_from_array(array: &dyn Array, row: usize) -> Option<Number> {
    Some(match array.data_type() {
        DataType::Int8 => Number::Signed(i64::from(
            array.as_any().downcast_ref::<Int8Array>()?.value(row),
        )),
        DataType::Int16 => Number::Signed(i64::from(
            array.as_any().downcast_ref::<Int16Array>()?.value(row),
        )),
        DataType::Int32 => Number::Signed(i64::from(
            array.as_any().downcast_ref::<Int32Array>()?.value(row),
        )),
        DataType::Int64 => Number::Signed(array.as_any().downcast_ref::<Int64Array>()?.value(row)),
        DataType::UInt8 => Number::Unsigned(u64::from(
            array.as_any().downcast_ref::<UInt8Array>()?.value(row),
        )),
        DataType::UInt16 => Number::Unsigned(u64::from(
            array.as_any().downcast_ref::<UInt16Array>()?.value(row),
        )),
        DataType::UInt32 => Number::Unsigned(u64::from(
            array.as_any().downcast_ref::<UInt32Array>()?.value(row),
        )),
        DataType::UInt64 => {
            Number::Unsigned(array.as_any().downcast_ref::<UInt64Array>()?.value(row))
        }
        DataType::Float16 => Number::Float(
            array
                .as_any()
                .downcast_ref::<Float16Array>()?
                .value(row)
                .to_f32() as f64,
        ),
        DataType::Float32 => {
            Number::Float(array.as_any().downcast_ref::<Float32Array>()?.value(row) as f64)
        }
        DataType::Float64 => {
            Number::Float(array.as_any().downcast_ref::<Float64Array>()?.value(row))
        }
        _ => return None,
    })
}

fn exact_signed_from_float(value: f64) -> Option<i64> {
    const I64_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < I64_UPPER_EXCLUSIVE
    {
        let converted = value as i64;
        ((converted as f64) == value).then_some(converted)
    } else {
        None
    }
}

fn exact_unsigned_from_float(value: f64) -> Option<u64> {
    const U64_UPPER_EXCLUSIVE: f64 = 18_446_744_073_709_551_616.0;
    if value.is_finite() && value.fract() == 0.0 && (0.0..U64_UPPER_EXCLUSIVE).contains(&value) {
        let converted = value as u64;
        ((converted as f64) == value).then_some(converted)
    } else {
        None
    }
}

fn number_matches(actual: Number, needle: &CellValue) -> bool {
    match (actual, needle) {
        (Number::Signed(actual), CellValue::Int(expected)) => actual == *expected,
        (Number::Signed(actual), CellValue::Float(expected)) => {
            exact_signed_from_float(*expected) == Some(actual)
        }
        (Number::Unsigned(actual), CellValue::Int(expected)) => {
            u64::try_from(*expected).ok() == Some(actual)
        }
        (Number::Unsigned(actual), CellValue::Float(expected)) => {
            exact_unsigned_from_float(*expected) == Some(actual)
        }
        (Number::Float(actual), CellValue::Int(expected)) => {
            exact_signed_from_float(actual) == Some(*expected)
        }
        (Number::Float(actual), CellValue::Float(expected)) => actual == *expected,
        _ => false,
    }
}

fn timestamp_micros(array: &dyn Array, row: usize) -> Option<i64> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()?
            .value(row)
            .checked_mul(1_000_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()?
            .value(row)
            .checked_mul(1_000),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()?
                .value(row)
                .div_euclid(1_000),
        ),
        _ => None,
    }
}

/// Compare an Arrow scalar at `row` with a carrier scalar.
pub fn cell_matches(array: &dyn Array, row: usize, needle: &CellValue) -> bool {
    if row >= array.len() {
        return false;
    }
    if array.is_null(row) {
        return matches!(needle, CellValue::Null);
    }
    if let Some(number) = number_from_array(array, row) {
        return number_matches(number, needle);
    }
    match (array.data_type(), needle) {
        (DataType::Boolean, CellValue::Bool(expected)) => {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray")
                .value(row)
                == *expected
        }
        (DataType::Time32(TimeUnit::Second), CellValue::Int(expected)) => {
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .expect("Time32SecondArray")
                    .value(row),
            ) == *expected
        }
        (DataType::Time32(TimeUnit::Millisecond), CellValue::Int(expected)) => {
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .expect("Time32MillisecondArray")
                    .value(row),
            ) == *expected
        }
        (DataType::Time64(TimeUnit::Microsecond), CellValue::Int(expected)) => {
            array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("Time64MicrosecondArray")
                .value(row)
                == *expected
        }
        (DataType::Time64(TimeUnit::Nanosecond), CellValue::Int(expected)) => {
            array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("Time64NanosecondArray")
                .value(row)
                == *expected
        }
        (DataType::Date32, CellValue::Date(expected)) => date_from_days(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        )
        .is_some_and(|value| value == *expected),
        (DataType::Date64, CellValue::Date(expected)) => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("Date64Array")
                .value(row);
            i32::try_from(millis.div_euclid(86_400_000))
                .ok()
                .and_then(date_from_days)
                .is_some_and(|value| value == *expected)
        }
        (DataType::Utf8, CellValue::Text(expected)) => {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray")
                .value(row)
                == expected
        }
        (DataType::LargeUtf8, CellValue::Text(expected)) => {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeStringArray")
                .value(row)
                == expected
        }
        (DataType::Utf8View, CellValue::Text(expected)) => {
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("StringViewArray")
                .value(row)
                == expected
        }
        (DataType::Timestamp(_, _), CellValue::Timestamp(expected))
        | (DataType::Timestamp(_, _), CellValue::Int(expected)) => {
            timestamp_micros(array, row) == Some(*expected)
        }
        _ => false,
    }
}

/// Days from 0001-01-01 (CE) to the Unix epoch, as used by Arrow `Date32`.
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Filter-mask builder selecting the rows where [`cell_matches`] holds.
///
/// The needle is resolved once per column data type: combinations whose
/// [`cell_matches`] semantics provably coincide with Arrow equality become a
/// typed scalar compared with one vectorized kernel, and every other
/// combination falls back to the row-wise comparison. Non-matching rows are
/// `false` or null in the mask, which `arrow_select::filter` treats
/// identically.
pub(crate) struct CellMatchMask<'a> {
    needle: &'a CellValue,
    target: Option<EqTarget>,
}

impl<'a> CellMatchMask<'a> {
    /// Resolve `needle` for arrays of `data_type`.
    pub(crate) fn new(data_type: &DataType, needle: &'a CellValue) -> Self {
        Self {
            needle,
            target: EqTarget::resolve(data_type, needle),
        }
    }

    /// Mask `array`, which must have the data type this mask was resolved for.
    pub(crate) fn mask(&self, array: &dyn Array) -> Result<BooleanArray, ArrowError> {
        match &self.target {
            Some(target) => target.mask(array),
            None => Ok(self.row_wise_mask(array)),
        }
    }

    fn row_wise_mask(&self, array: &dyn Array) -> BooleanArray {
        BooleanArray::new(
            BooleanBuffer::collect_bool(array.len(), |row| cell_matches(array, row, self.needle)),
            None,
        )
    }
}

/// A needle resolved against one column type for vectorized comparison.
enum EqTarget {
    /// No non-null row can match.
    NoMatch,
    /// Exactly the rows [`Array::is_null`] reports as null.
    NullRows,
    /// Rows equal to a one-element array of the column's exact data type.
    Equal(Scalar<ArrayRef>),
    /// Float rows equal to either signed zero (IEEE `0.0 == -0.0`, unlike
    /// Arrow's total-order float equality).
    SignedZero(Scalar<ArrayRef>, Scalar<ArrayRef>),
}

impl EqTarget {
    /// Resolve `needle` for a column of `data_type`, or `None` when only the
    /// row-wise comparison reproduces [`cell_matches`].
    fn resolve(data_type: &DataType, needle: &CellValue) -> Option<Self> {
        match needle {
            CellValue::Null => Some(Self::NullRows),
            CellValue::Bool(expected) => match data_type {
                DataType::Boolean => Some(Self::equal(BooleanArray::from(vec![*expected]))),
                _ => None,
            },
            CellValue::Text(expected) => {
                let expected = expected.as_str();
                Some(match data_type {
                    DataType::Utf8 => Self::equal(StringArray::from(vec![expected])),
                    DataType::LargeUtf8 => Self::equal(LargeStringArray::from(vec![expected])),
                    DataType::Utf8View => Self::equal(StringViewArray::from(vec![expected])),
                    _ => return None,
                })
            }
            CellValue::Int(expected) => Self::int_needle(data_type, *expected),
            CellValue::Float(expected) => Self::float_needle(data_type, *expected),
            CellValue::Date(expected) => match data_type {
                DataType::Date32 => {
                    let days = expected.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE;
                    Some(Self::primitive::<Date32Type>(data_type, days))
                }
                _ => None,
            },
            CellValue::Timestamp(expected) => Self::timestamp(data_type, *expected),
        }
    }

    fn int_needle(data_type: &DataType, expected: i64) -> Option<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Self::signed(data_type, expected)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                match u64::try_from(expected) {
                    Ok(expected) => Self::unsigned(data_type, expected),
                    Err(_) => Some(Self::NoMatch),
                }
            }
            // A float row matches an integer needle only when it is exactly
            // that integer, which requires the integer to be exact in f64.
            DataType::Float32 | DataType::Float64 => {
                if integer_exactly_representable_as_f64(expected) {
                    Self::float(data_type, expected as f64)
                } else {
                    Some(Self::NoMatch)
                }
            }
            DataType::Time32(TimeUnit::Second) => Some(match i32::try_from(expected) {
                Ok(expected) => Self::primitive::<Time32SecondType>(data_type, expected),
                Err(_) => Self::NoMatch,
            }),
            DataType::Time32(TimeUnit::Millisecond) => Some(match i32::try_from(expected) {
                Ok(expected) => Self::primitive::<Time32MillisecondType>(data_type, expected),
                Err(_) => Self::NoMatch,
            }),
            DataType::Time64(TimeUnit::Microsecond) => Some(
                Self::primitive::<Time64MicrosecondType>(data_type, expected),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Some(Self::primitive::<Time64NanosecondType>(data_type, expected))
            }
            DataType::Timestamp(_, _) => Self::timestamp(data_type, expected),
            _ => None,
        }
    }

    fn float_needle(data_type: &DataType, expected: f64) -> Option<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                match exact_signed_from_float(expected) {
                    Some(expected) => Self::signed(data_type, expected),
                    None => Some(Self::NoMatch),
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                match exact_unsigned_from_float(expected) {
                    Some(expected) => Self::unsigned(data_type, expected),
                    None => Some(Self::NoMatch),
                }
            }
            DataType::Float32 | DataType::Float64 => Self::float(data_type, expected),
            _ => None,
        }
    }

    fn signed(data_type: &DataType, expected: i64) -> Option<Self> {
        let target = match data_type {
            DataType::Int8 => i8::try_from(expected)
                .map(|expected| Self::primitive::<Int8Type>(data_type, expected)),
            DataType::Int16 => i16::try_from(expected)
                .map(|expected| Self::primitive::<Int16Type>(data_type, expected)),
            DataType::Int32 => i32::try_from(expected)
                .map(|expected| Self::primitive::<Int32Type>(data_type, expected)),
            DataType::Int64 => Ok(Self::primitive::<Int64Type>(data_type, expected)),
            _ => return None,
        };
        Some(target.unwrap_or(Self::NoMatch))
    }

    fn unsigned(data_type: &DataType, expected: u64) -> Option<Self> {
        let target = match data_type {
            DataType::UInt8 => u8::try_from(expected)
                .map(|expected| Self::primitive::<UInt8Type>(data_type, expected)),
            DataType::UInt16 => u16::try_from(expected)
                .map(|expected| Self::primitive::<UInt16Type>(data_type, expected)),
            DataType::UInt32 => u32::try_from(expected)
                .map(|expected| Self::primitive::<UInt32Type>(data_type, expected)),
            DataType::UInt64 => Ok(Self::primitive::<UInt64Type>(data_type, expected)),
            _ => return None,
        };
        Some(target.unwrap_or(Self::NoMatch))
    }

    /// IEEE equality against `expected` for Float32/Float64 columns.
    ///
    /// NaN never matches and both zeros match zero, so neither may reach
    /// Arrow's total-order equality as a plain scalar.
    fn float(data_type: &DataType, expected: f64) -> Option<Self> {
        if !matches!(data_type, DataType::Float32 | DataType::Float64) {
            return None;
        }
        if expected.is_nan() {
            return Some(Self::NoMatch);
        }
        Some(match data_type {
            DataType::Float32 => {
                // Float32 rows are widened losslessly before comparison, so
                // only a needle exact in f32 can match.
                let narrowed = expected as f32;
                if f64::from(narrowed) != expected {
                    Self::NoMatch
                } else if narrowed == 0.0 {
                    Self::SignedZero(
                        Self::scalar::<Float32Type>(data_type, 0.0),
                        Self::scalar::<Float32Type>(data_type, -0.0),
                    )
                } else {
                    Self::primitive::<Float32Type>(data_type, narrowed)
                }
            }
            _ if expected == 0.0 => Self::SignedZero(
                Self::scalar::<Float64Type>(data_type, 0.0),
                Self::scalar::<Float64Type>(data_type, -0.0),
            ),
            _ => Self::primitive::<Float64Type>(data_type, expected),
        })
    }

    /// Timestamp rows match microseconds after exact unit scaling; nanosecond
    /// rows floor to microseconds, so they stay on the row-wise path.
    fn timestamp(data_type: &DataType, micros: i64) -> Option<Self> {
        let DataType::Timestamp(unit, _) = data_type else {
            return None;
        };
        let (target, scale) = match unit {
            TimeUnit::Second => (micros / 1_000_000, 1_000_000),
            TimeUnit::Millisecond => (micros / 1_000, 1_000),
            TimeUnit::Microsecond => (micros, 1),
            TimeUnit::Nanosecond => return None,
        };
        if micros % scale != 0 {
            return Some(Self::NoMatch);
        }
        Some(match unit {
            TimeUnit::Second => Self::primitive::<TimestampSecondType>(data_type, target),
            TimeUnit::Millisecond => Self::primitive::<TimestampMillisecondType>(data_type, target),
            _ => Self::primitive::<TimestampMicrosecondType>(data_type, target),
        })
    }

    fn equal(array: impl Array + 'static) -> Self {
        Self::Equal(Scalar::new(Arc::new(array) as ArrayRef))
    }

    fn primitive<T: ArrowPrimitiveType>(data_type: &DataType, value: T::Native) -> Self {
        Self::Equal(Self::scalar::<T>(data_type, value))
    }

    /// One-element scalar carrying the column's exact data type (including
    /// timestamp timezone), as Arrow comparison kernels require.
    fn scalar<T: ArrowPrimitiveType>(data_type: &DataType, value: T::Native) -> Scalar<ArrayRef> {
        let array = PrimitiveArray::<T>::from_value(value, 1).with_data_type(data_type.clone());
        Scalar::new(Arc::new(array) as ArrayRef)
    }

    fn mask(&self, array: &dyn Array) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::NoMatch => Ok(BooleanArray::new(
                BooleanBuffer::new_unset(array.len()),
                None,
            )),
            // Mirrors `Array::is_null`, which reads the physical null buffer.
            Self::NullRows => Ok(BooleanArray::new(
                match array.nulls() {
                    Some(nulls) => !nulls.inner(),
                    None => BooleanBuffer::new_unset(array.len()),
                },
                None,
            )),
            Self::Equal(value) => eq(&array, value),
            Self::SignedZero(positive, negative) => {
                let (positive, nulls) = eq(&array, positive)?.into_parts();
                let negative = eq(&array, negative)?;
                Ok(BooleanArray::new(&positive | negative.values(), nulls))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{DictionaryArray, NullArray};

    use super::CellValue::{Bool, Date, Float, Int, Null, Text, Timestamp};
    use super::*;

    fn selected(mask: &BooleanArray) -> Vec<bool> {
        (0..mask.len())
            .map(|row| mask.is_valid(row) && mask.value(row))
            .collect()
    }

    /// Assert each needle takes the expected path and that its mask selects
    /// exactly the row-wise rows, for the array and an offset slice of it.
    fn assert_parity(array: ArrayRef, vectorized: &[CellValue], row_wise: &[CellValue]) {
        let sliced = array.slice(1, array.len() - 1);
        for (needles, expect_vectorized) in [(vectorized, true), (row_wise, false)] {
            for needle in needles {
                let matcher = CellMatchMask::new(array.data_type(), needle);
                assert_eq!(
                    matcher.target.is_some(),
                    expect_vectorized,
                    "{:?} {needle:?}",
                    array.data_type(),
                );
                for input in [array.as_ref(), sliced.as_ref()] {
                    assert_eq!(
                        selected(&matcher.mask(input).unwrap()),
                        selected(&matcher.row_wise_mask(input)),
                        "{:?} {needle:?}",
                        array.data_type(),
                    );
                }
            }
        }
    }

    #[test]
    fn text_masks_match_row_wise() {
        let values = vec![
            Some("AAPL US Equity"),
            None,
            Some(""),
            Some("IBM"),
            Some("AAPL US Equity"),
            Some("IBM"),
        ];
        let needles = [
            Text("AAPL US Equity".to_string()),
            Text("IBM".to_string()),
            Text(String::new()),
            Text("MSFT".to_string()),
            Null,
        ];
        let row_wise = [Int(1), Bool(true)];
        assert_parity(
            Arc::new(StringArray::from(values.clone())),
            &needles,
            &row_wise,
        );
        assert_parity(
            Arc::new(LargeStringArray::from(values.clone())),
            &needles,
            &row_wise,
        );
        assert_parity(Arc::new(StringViewArray::from(values)), &needles, &row_wise);
    }

    #[test]
    fn integer_masks_match_row_wise_across_widths() {
        let row_wise = [Text("7".to_string()), Bool(true)];
        assert_parity(
            Arc::new(Int64Array::from(vec![
                Some(0),
                None,
                Some(-1),
                Some(i64::MAX),
                Some(i64::MIN),
                Some(7),
            ])),
            &[
                Int(0),
                Int(7),
                Int(i64::MIN),
                Int(i64::MAX),
                Float(7.0),
                Float(7.5),
                Float(-0.0),
                Float(f64::NAN),
                Float(9.3e18),
                Float(-9_223_372_036_854_775_808.0),
                Null,
            ],
            &row_wise,
        );
        assert_parity(
            Arc::new(Int8Array::from(vec![Some(-128), Some(127), None, Some(0)])),
            &[
                Int(127),
                Int(128),
                Int(-129),
                Float(-128.0),
                Float(128.0),
                Null,
            ],
            &row_wise,
        );
        assert_parity(
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(5),
                Some(5),
            ])),
            &[
                Int(i64::from(i32::MIN)),
                Int(i64::from(i32::MIN) - 1),
                Int(5),
                Float(5.0),
            ],
            &row_wise,
        );
        assert_parity(
            Arc::new(UInt8Array::from(vec![Some(0), Some(255), None, Some(0)])),
            &[
                Int(-1),
                Int(0),
                Int(255),
                Int(256),
                Float(255.0),
                Float(-0.0),
                Float(0.0),
                Null,
            ],
            &row_wise,
        );
        assert_parity(
            Arc::new(UInt64Array::from(vec![
                Some(u64::MAX),
                Some(0),
                None,
                Some(1 << 63),
            ])),
            &[
                Int(-1),
                Int(0),
                Int(i64::MAX),
                Float(18_446_744_073_709_551_616.0),
                Float(9_223_372_036_854_775_808.0),
                Null,
            ],
            &row_wise,
        );
    }

    #[test]
    fn float_masks_keep_ieee_equality() {
        let row_wise = [Text("0".to_string())];
        assert_parity(
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                None,
                Some(1.5),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(9_007_199_254_740_992.0),
                Some(-f64::NAN),
            ])),
            &[
                Float(0.0),
                Float(-0.0),
                Float(f64::NAN),
                Float(1.5),
                Float(f64::INFINITY),
                Int(0),
                Int(1),
                Int(9_007_199_254_740_992),
                Int(9_007_199_254_740_993),
                Int(i64::MAX),
                Null,
            ],
            &row_wise,
        );
        assert_parity(
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f32::NAN),
                None,
                Some(0.1),
                Some(1.5),
                Some(16_777_216.0),
                Some(f32::INFINITY),
            ])),
            &[
                Float(0.1),
                Float(f64::from(0.1_f32)),
                Float(1.5),
                Float(-0.0),
                Float(f64::MAX),
                Float(f64::INFINITY),
                Float(f64::NAN),
                Int(0),
                Int(16_777_216),
                Int(16_777_217),
                Null,
            ],
            &row_wise,
        );
    }

    #[test]
    fn boolean_date_and_time_masks_match_row_wise() {
        assert_parity(
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            &[Bool(true), Bool(false), Null],
            &[Int(1), Text("true".to_string())],
        );
        let date = |year, month, day| Date(NaiveDate::from_ymd_opt(year, month, day).unwrap());
        assert_parity(
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(19_723),
                None,
                Some(-1),
                Some(i32::MAX),
                Some(19_723),
            ])),
            &[
                date(1970, 1, 1),
                date(2024, 1, 1),
                date(1969, 12, 31),
                date(2024, 1, 2),
                Null,
            ],
            &[Timestamp(0), Int(19_723)],
        );
        assert_parity(
            Arc::new(Time32SecondArray::from(vec![Some(0), Some(86_399), None])),
            &[Int(86_399), Int(i64::MAX), Null],
            &[Float(0.0)],
        );
        assert_parity(
            Arc::new(Time64NanosecondArray::from(vec![Some(5), None, Some(5)])),
            &[Int(5), Int(-5), Null],
            &[Timestamp(5)],
        );
    }

    #[test]
    fn timestamp_masks_scale_units_exactly() {
        let seconds = TimestampSecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(-1),
            Some(i64::MAX),
            Some(1),
        ])
        .with_timezone("UTC");
        assert_parity(
            Arc::new(seconds),
            &[
                Timestamp(1_000_000),
                Timestamp(1_000_001),
                Timestamp(-1_000_000),
                Timestamp(i64::MAX),
                Int(0),
                Int(1_000_000),
                Null,
            ],
            &[Float(1.0)],
        );
        let millis =
            TimestampMillisecondArray::from(vec![Some(0), Some(1), None, Some(-1), Some(i64::MIN)]);
        assert_parity(
            Arc::new(millis),
            &[
                Timestamp(1_000),
                Timestamp(1_500),
                Timestamp(-1_000),
                Timestamp(i64::MIN),
                Int(1_000),
                Null,
            ],
            &[],
        );
        let micros = TimestampMicrosecondArray::from(vec![Some(0), Some(1), None, Some(-1)])
            .with_timezone("America/New_York");
        assert_parity(
            Arc::new(micros),
            &[Timestamp(1), Timestamp(-1), Int(0), Null],
            &[],
        );
        let nanos = TimestampNanosecondArray::from(vec![Some(1_999), None, Some(-1), Some(2_000)]);
        assert_parity(
            Arc::new(nanos),
            &[Null],
            &[Timestamp(1), Timestamp(-1), Int(2)],
        );
    }

    #[test]
    fn dictionary_and_null_type_masks_vectorize_only_null_needles() {
        let dictionary: DictionaryArray<Int32Type> = vec![Some("a"), None, Some("b"), Some("a")]
            .into_iter()
            .collect();
        assert_parity(
            Arc::new(dictionary),
            &[Null],
            &[Text("a".to_string()), Int(0)],
        );
        // `NullArray` has no physical null buffer, so `cell_matches` never
        // matches even a null needle there.
        assert_parity(Arc::new(NullArray::new(3)), &[Null], &[Int(0)]);
    }
}
