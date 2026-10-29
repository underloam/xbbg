//! Dynamic value type for Bloomberg elements.
//!
//! The `Value` enum provides type-safe extraction of Bloomberg element values
//! when the type is not known at compile time. This replaces JSON serialization
//! with direct typed extraction.
//!
//! # Example
//!
//! ```ignore
//! use xbbg_core::{Element, Value};
//!
//! fn process_field(elem: &Element) {
//!     match elem.get_value(0) {
//!         Some(Value::Float64(v)) => println!("float: {}", v),
//!         Some(Value::Int64(v)) => println!("int: {}", v),
//!         Some(Value::String(s)) => println!("string: {}", s),
//!         Some(Value::Bool(b)) => println!("bool: {}", b),
//!         Some(Value::TimestampMicros(ts)) => println!("timestamp: {}", ts),
//!         Some(Value::Date32(days)) => println!("date (days): {}", days),
//!         Some(Value::Null) => println!("null"),
//!         None => println!("no value at index"),
//!     }
//! }
//! ```

use crate::{DataType, HighPrecisionDatetime};

/// A dynamically-typed Bloomberg value (borrows strings from Element).
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(&'a str),
    Date32(i32),
    TimestampMicros(i64),
    /// Time-of-day as microseconds from midnight (0..86_400_000_000).
    Time64Micros(i64),
    Datetime(HighPrecisionDatetime),
    Enum(&'a str),
    Byte(u8),
}

/// Owned version of Value (owns strings, can outlive Element).
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Date32(i32),
    TimestampMicros(i64),
    /// Time-of-day as microseconds from midnight (0..86_400_000_000).
    Time64Micros(i64),
    Datetime(HighPrecisionDatetime),
    Enum(String),
    Byte(u8),
}

impl<'a> Value<'a> {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(v) => Some(*v),
            Self::Int64(v) => Some(*v as f64),
            Self::Int32(v) => Some(*v as f64),
            Self::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            Self::Byte(v) => Some(*v as f64),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            Self::Int32(v) => Some(*v as i64),
            Self::Bool(v) => Some(if *v { 1 } else { 0 }),
            Self::Byte(v) => Some(*v as i64),
            Self::Date32(v) => Some(*v as i64),
            Self::TimestampMicros(v) => Some(*v),
            Self::Time64Micros(v) => Some(*v),
            // Bloomberg often sends integer-typed fields (e.g. PX_VOLUME, OPEN_INT)
            // as Float64 in HistoricalDataResponse. Accept only whole numbers within
            // i64 range; anything with a fractional part stays None.
            Self::Float64(v)
                if v.is_finite()
                    && v.fract() == 0.0
                    && *v >= i64::MIN as f64
                    && *v < -(i64::MIN as f64) =>
            {
                Some(*v as i64)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&'a str> {
        match self {
            Self::String(s) | Self::Enum(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            Self::Int32(v) => Some(*v != 0),
            Self::Int64(v) => Some(*v != 0),
            _ => None,
        }
    }

    #[inline]
    pub fn to_owned(&self) -> OwnedValue {
        match self {
            Self::Null => OwnedValue::Null,
            Self::Bool(v) => OwnedValue::Bool(*v),
            Self::Int32(v) => OwnedValue::Int32(*v),
            Self::Int64(v) => OwnedValue::Int64(*v),
            Self::Float64(v) => OwnedValue::Float64(*v),
            Self::String(s) => OwnedValue::String((*s).to_string()),
            Self::Date32(v) => OwnedValue::Date32(*v),
            Self::TimestampMicros(v) => OwnedValue::TimestampMicros(*v),
            Self::Time64Micros(v) => OwnedValue::Time64Micros(*v),
            Self::Datetime(dt) => OwnedValue::Datetime(*dt),
            Self::Enum(s) => OwnedValue::Enum((*s).to_string()),
            Self::Byte(v) => OwnedValue::Byte(*v),
        }
    }

    #[inline]
    pub fn datatype(&self) -> DataType {
        match self {
            Self::Null => DataType::String,
            Self::Bool(_) => DataType::Bool,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::String(_) => DataType::String,
            Self::Date32(_) => DataType::Date,
            Self::TimestampMicros(_) | Self::Datetime(_) => DataType::Datetime,
            Self::Time64Micros(_) => DataType::Time,
            Self::Enum(_) => DataType::Enumeration,
            Self::Byte(_) => DataType::Byte,
        }
    }
}

impl OwnedValue {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(v) => Some(*v),
            Self::Int64(v) => Some(*v as f64),
            Self::Int32(v) => Some(*v as f64),
            Self::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            Self::Byte(v) => Some(*v as f64),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            Self::Int32(v) => Some(*v as i64),
            Self::Bool(v) => Some(if *v { 1 } else { 0 }),
            Self::Byte(v) => Some(*v as i64),
            Self::Date32(v) => Some(*v as i64),
            Self::TimestampMicros(v) => Some(*v),
            Self::Time64Micros(v) => Some(*v),
            Self::Float64(v)
                if v.is_finite()
                    && v.fract() == 0.0
                    && *v >= i64::MIN as f64
                    && *v < -(i64::MIN as f64) =>
            {
                Some(*v as i64)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) | Self::Enum(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            Self::Int32(v) => Some(*v != 0),
            Self::Int64(v) => Some(*v != 0),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::Float64(12.5).as_f64(), Some(12.5));
        assert_eq!(Value::Int64(42).as_i64(), Some(42));
        assert_eq!(Value::String("hello").as_str(), Some("hello"));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert!(Value::Null.is_null());
    }

    #[test]
    fn floating_integer_conversion_rejects_the_rounded_i64_upper_bound() {
        let upper = -(i64::MIN as f64);
        let below_upper = f64::from_bits(upper.to_bits() - 1);
        let below_lower = f64::from_bits((i64::MIN as f64).to_bits() + 1);
        for (input, expected) in [
            (upper, None),
            (below_upper, Some(i64::MAX - 1023)),
            (i64::MIN as f64, Some(i64::MIN)),
            (below_lower, None),
            (42.5, None),
            (f64::INFINITY, None),
            (f64::NEG_INFINITY, None),
            (f64::NAN, None),
        ] {
            assert_eq!(Value::Float64(input).as_i64(), expected);
            assert_eq!(OwnedValue::Float64(input).as_i64(), expected);
        }
    }

    #[test]
    fn test_owned_value() {
        let owned = Value::String("hello").to_owned();
        assert_eq!(owned.as_str(), Some("hello"));
    }

    #[test]
    fn test_datatype() {
        assert_eq!(Value::Float64(0.0).datatype(), DataType::Float64);
        assert_eq!(Value::Bool(false).datatype(), DataType::Bool);
        assert_eq!(Value::Date32(0).datatype(), DataType::Date);
    }
}
