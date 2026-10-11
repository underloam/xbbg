//! Scalar conversion helpers for xbbg Arrow carrier data.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::NaiveDate;

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
