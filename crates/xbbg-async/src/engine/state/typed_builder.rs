//! Typed Arrow array builders for dynamic schema support.
//!
//! This module provides:
//! - `TypedBuilder`: A builder that can hold different Arrow array builder types
//! - `ColumnSet`: A collection of named columns for building RecordBatches
//!
//! These work directly with `xbbg_core::Value` - no JSON intermediate.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use indexmap::IndexMap;
use xbbg_core::{BlpError, Value};

use super::value_utils::{format_date32, format_time64_micros, format_timestamp_micros};

/// Arrow type identifier (subset of Arrow types we support).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArrowType {
    Float64,
    Int64,
    Int32,
    String,
    Bool,
    Date32,
    TimestampMicros,
    Time64Micros,
}

impl ArrowType {
    /// Parse from type string (e.g., "float64", "int64", "string").
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "float64" | "float" | "double" | "f64" => ArrowType::Float64,
            "int64" | "int" | "integer" | "i64" => ArrowType::Int64,
            "int32" | "i32" => ArrowType::Int32,
            "bool" | "boolean" => ArrowType::Bool,
            "date32" | "date" => ArrowType::Date32,
            "timestamp" | "datetime" | "timestamp_us" => ArrowType::TimestampMicros,
            "time64" | "time" | "time64_us" => ArrowType::Time64Micros,
            _ => ArrowType::String, // Default to string
        }
    }

    /// Infer ArrowType from a xbbg_core::Value.
    pub fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Null => ArrowType::String, // Default null to string
            Value::Bool(_) => ArrowType::Bool,
            Value::Int32(_) => ArrowType::Int32,
            Value::Int64(_) => ArrowType::Int64,
            Value::Float64(_) => ArrowType::Float64,
            Value::String(_) | Value::Enum(_) => ArrowType::String,
            Value::Date32(_) => ArrowType::Date32,
            Value::TimestampMicros(_) | Value::Datetime(_) => ArrowType::TimestampMicros,
            Value::Time64Micros(_) => ArrowType::Time64Micros,
            Value::Byte(_) => ArrowType::Int32, // Promote byte to int32
        }
    }

    /// Get the Arrow DataType for this type.
    pub fn to_arrow_datatype(&self) -> DataType {
        match self {
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::String => DataType::Utf8,
            ArrowType::Bool => DataType::Boolean,
            ArrowType::Date32 => DataType::Date32,
            ArrowType::TimestampMicros => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            ArrowType::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
        }
    }

    /// Get type name string.
    pub fn type_name(&self) -> &'static str {
        match self {
            ArrowType::Float64 => "float64",
            ArrowType::Int64 => "int64",
            ArrowType::Int32 => "int32",
            ArrowType::String => "string",
            ArrowType::Bool => "bool",
            ArrowType::Date32 => "date32",
            ArrowType::TimestampMicros => "timestamp",
            ArrowType::Time64Micros => "time64",
        }
    }
}

/// A builder that can hold different Arrow array builder types.
pub enum TypedBuilder {
    Float64(Float64Builder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    String(StringBuilder),
    Bool(BooleanBuilder),
    Date32(Date32Builder),
    TimestampMicros(TimestampMicrosecondBuilder),
    Time64Micros(Time64MicrosecondBuilder),
}

impl TypedBuilder {
    /// Create a new builder from an ArrowType.
    pub fn new(arrow_type: ArrowType) -> Self {
        match arrow_type {
            ArrowType::Float64 => TypedBuilder::Float64(Float64Builder::new()),
            ArrowType::Int64 => TypedBuilder::Int64(Int64Builder::new()),
            ArrowType::Int32 => TypedBuilder::Int32(Int32Builder::new()),
            ArrowType::String => TypedBuilder::String(StringBuilder::new()),
            ArrowType::Bool => TypedBuilder::Bool(BooleanBuilder::new()),
            ArrowType::Date32 => TypedBuilder::Date32(Date32Builder::new()),
            ArrowType::TimestampMicros => {
                TypedBuilder::TimestampMicros(TimestampMicrosecondBuilder::new())
            }
            ArrowType::Time64Micros => TypedBuilder::Time64Micros(Time64MicrosecondBuilder::new()),
        }
    }

    /// Create a new builder from a type string.
    pub fn from_type_str(type_str: &str) -> Self {
        Self::new(ArrowType::parse(type_str))
    }

    /// Append a value from xbbg_core::Value, converting as needed.
    pub fn append_value(&mut self, value: Option<Value<'_>>) {
        let _ = self.append_value_report_miss(value);
    }

    /// Append a value and report whether a non-null value could not be converted.
    pub fn append_value_report_miss(&mut self, value: Option<Value<'_>>) -> bool {
        match self {
            TypedBuilder::Float64(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(v) = v.as_f64() {
                        b.append_value(v);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::Int64(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(v) = v.as_i64() {
                        b.append_value(v);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::Int32(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(v) = match v {
                        Value::Int32(i) => Some(i),
                        Value::Int64(i) => Some(i as i32),
                        Value::Byte(i) => Some(i as i32),
                        Value::Bool(b) => Some(if b { 1 } else { 0 }),
                        Value::Float64(f)
                            if f.is_finite()
                                && f.fract() == 0.0
                                && f >= i32::MIN as f64
                                && f <= i32::MAX as f64 =>
                        {
                            Some(f as i32)
                        }
                        _ => None,
                    } {
                        b.append_value(v);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::String(b) => {
                match value {
                    Some(Value::String(s)) | Some(Value::Enum(s)) => b.append_value(s),
                    Some(Value::Float64(f)) if f.is_finite() => {
                        let mut buffer = ryu::Buffer::new();
                        b.append_value(buffer.format_finite(f));
                    }
                    Some(Value::Float64(f)) => {
                        let s = f.to_string();
                        b.append_value(&s);
                    }
                    Some(Value::Int64(i)) => {
                        let mut buffer = itoa::Buffer::new();
                        b.append_value(buffer.format(i));
                    }
                    Some(Value::Int32(i)) => {
                        let mut buffer = itoa::Buffer::new();
                        b.append_value(buffer.format(i));
                    }
                    Some(Value::Bool(v)) => b.append_value(if v { "true" } else { "false" }),
                    Some(Value::Date32(d)) => {
                        let s = format_date32(d);
                        b.append_value(&s);
                    }
                    Some(Value::TimestampMicros(ts)) => {
                        let s = format_timestamp_micros(ts);
                        b.append_value(&s);
                    }
                    Some(Value::Datetime(dt)) => {
                        let s = format_timestamp_micros(dt.to_micros());
                        b.append_value(&s);
                    }
                    Some(Value::Time64Micros(t)) => {
                        let s = format_time64_micros(t);
                        b.append_value(&s);
                    }
                    Some(Value::Byte(v)) => {
                        let mut buffer = itoa::Buffer::new();
                        b.append_value(buffer.format(v));
                    }
                    Some(Value::Null) | None => b.append_null(),
                }
                false
            }
            TypedBuilder::Bool(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(v) = v.as_bool() {
                        b.append_value(v);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::Date32(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(days) = match v {
                        Value::Date32(d) => Some(d),
                        Value::TimestampMicros(ts) => Some((ts / 86_400_000_000) as i32),
                        _ => None,
                    } {
                        b.append_value(days);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::TimestampMicros(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(micros) = match v {
                        Value::TimestampMicros(ts) => Some(ts),
                        Value::Datetime(dt) => Some(dt.to_micros()),
                        Value::Date32(d) => Some(d as i64 * 86_400_000_000),
                        _ => None,
                    } {
                        b.append_value(micros);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
            TypedBuilder::Time64Micros(b) => match value {
                Some(Value::Null) | None => {
                    b.append_null();
                    false
                }
                Some(v) => {
                    if let Some(micros) = match v {
                        Value::Time64Micros(ts) => Some(ts),
                        Value::TimestampMicros(ts) => {
                            // Extract time-of-day from full timestamp
                            Some(ts.rem_euclid(86_400_000_000))
                        }
                        _ => None,
                    } {
                        b.append_value(micros);
                        false
                    } else {
                        b.append_null();
                        true
                    }
                }
            },
        }
    }

    /// Append a string value directly.
    pub fn append_str(&mut self, s: &str) {
        match self {
            TypedBuilder::String(b) => b.append_value(s),
            _ => self.append_value(Some(Value::String(s))),
        }
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        match self {
            TypedBuilder::Float64(b) => b.append_null(),
            TypedBuilder::Int64(b) => b.append_null(),
            TypedBuilder::Int32(b) => b.append_null(),
            TypedBuilder::String(b) => b.append_null(),
            TypedBuilder::Bool(b) => b.append_null(),
            TypedBuilder::Date32(b) => b.append_null(),
            TypedBuilder::TimestampMicros(b) => b.append_null(),
            TypedBuilder::Time64Micros(b) => b.append_null(),
        }
    }

    /// Get the number of values appended.
    pub fn len(&self) -> usize {
        match self {
            TypedBuilder::Float64(b) => b.len(),
            TypedBuilder::Int64(b) => b.len(),
            TypedBuilder::Int32(b) => b.len(),
            TypedBuilder::String(b) => b.len(),
            TypedBuilder::Bool(b) => b.len(),
            TypedBuilder::Date32(b) => b.len(),
            TypedBuilder::TimestampMicros(b) => b.len(),
            TypedBuilder::Time64Micros(b) => b.len(),
        }
    }

    /// Check if builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish building and return the array.
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            TypedBuilder::Float64(b) => Arc::new(b.finish()),
            TypedBuilder::Int64(b) => Arc::new(b.finish()),
            TypedBuilder::Int32(b) => Arc::new(b.finish()),
            TypedBuilder::String(b) => Arc::new(b.finish()),
            TypedBuilder::Bool(b) => Arc::new(b.finish()),
            TypedBuilder::Date32(b) => Arc::new(b.finish()),
            TypedBuilder::TimestampMicros(b) => Arc::new(b.finish().with_timezone("UTC")),
            TypedBuilder::Time64Micros(b) => Arc::new(b.finish()),
        }
    }

    /// Get the Arrow DataType for this builder.
    pub fn data_type(&self) -> DataType {
        match self {
            TypedBuilder::Float64(_) => DataType::Float64,
            TypedBuilder::Int64(_) => DataType::Int64,
            TypedBuilder::Int32(_) => DataType::Int32,
            TypedBuilder::String(_) => DataType::Utf8,
            TypedBuilder::Bool(_) => DataType::Boolean,
            TypedBuilder::Date32(_) => DataType::Date32,
            TypedBuilder::TimestampMicros(_) => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            TypedBuilder::Time64Micros(_) => DataType::Time64(TimeUnit::Microsecond),
        }
    }

    /// Get the ArrowType for this builder.
    pub fn arrow_type(&self) -> ArrowType {
        match self {
            TypedBuilder::Float64(_) => ArrowType::Float64,
            TypedBuilder::Int64(_) => ArrowType::Int64,
            TypedBuilder::Int32(_) => ArrowType::Int32,
            TypedBuilder::String(_) => ArrowType::String,
            TypedBuilder::Bool(_) => ArrowType::Bool,
            TypedBuilder::Date32(_) => ArrowType::Date32,
            TypedBuilder::TimestampMicros(_) => ArrowType::TimestampMicros,
            TypedBuilder::Time64Micros(_) => ArrowType::Time64Micros,
        }
    }
}

/// A collection of named columns for building RecordBatches.
///
/// Handles dynamic column creation and ensures all columns have the same length.
///
/// # Example
///
/// ```ignore
/// let mut cols = ColumnSet::new();
/// cols.append("ticker", Value::String("AAPL US Equity"));
/// cols.append("price", Value::Float64(150.0));
/// cols.end_row(); // Ensures all columns have same length
///
/// let batch = cols.finish()?;
/// ```
pub struct ColumnSet {
    /// Columns in insertion order (preserves field order)
    columns: IndexMap<String, TypedBuilder>,
    /// Type hints for columns (optional, from field_types config)
    type_hints: IndexMap<String, ArrowType>,
    /// Current row count
    row_count: usize,
}

impl ColumnSet {
    fn build_empty_with_order(&self, order: &[&str]) -> Result<RecordBatch, BlpError> {
        if order.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let mut fields = Vec::with_capacity(order.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(order.len());

        for &name in order {
            let arrow_type = self
                .type_hints
                .get(name)
                .copied()
                .unwrap_or(ArrowType::String);
            fields.push(Field::new(name, arrow_type.to_arrow_datatype(), true));
            arrays.push(TypedBuilder::new(arrow_type).finish());
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build empty RecordBatch from order: {e}"),
        })
    }

    /// Create a new empty ColumnSet.
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
            type_hints: IndexMap::new(),
            row_count: 0,
        }
    }

    /// Create with type hints for specific columns.
    pub fn with_type_hints(hints: impl IntoIterator<Item = (String, ArrowType)>) -> Self {
        Self {
            columns: IndexMap::new(),
            type_hints: hints.into_iter().collect(),
            row_count: 0,
        }
    }

    /// Set a type hint for a column.
    pub fn set_type_hint(&mut self, name: &str, arrow_type: ArrowType) {
        self.type_hints.insert(name.to_string(), arrow_type);
    }

    /// Append a value to a column.
    ///
    /// Creates the column if it doesn't exist, inferring type from the value
    /// or using type hints if available.
    pub fn append(&mut self, name: &str, value: Value<'_>) {
        if let Some(builder) = self.columns.get_mut(name) {
            builder.append_value(Some(value));
            return;
        }

        // Use type hint if available, otherwise infer from value. When a column
        // first appears after earlier rows, pre-pad those completed rows so the
        // first value remains aligned with the current row.
        let arrow_type = self
            .type_hints
            .get(name)
            .copied()
            .unwrap_or_else(|| ArrowType::from_value(&value));
        let mut builder = TypedBuilder::new(arrow_type);
        for _ in 0..self.row_count {
            builder.append_null();
        }
        builder.append_value(Some(value));
        self.columns.insert(name.to_string(), builder);
    }

    /// Append a string value to a column (convenience method).
    pub fn append_str(&mut self, name: &str, value: &str) {
        self.append(name, Value::String(value));
    }

    /// Append a null to a column.
    pub fn append_null(&mut self, name: &str) {
        if let Some(builder) = self.columns.get_mut(name) {
            builder.append_null();
        } else {
            let arrow_type = self
                .type_hints
                .get(name)
                .copied()
                .unwrap_or(ArrowType::String);
            let mut builder = TypedBuilder::new(arrow_type);
            for _ in 0..=self.row_count {
                builder.append_null();
            }
            self.columns.insert(name.to_string(), builder);
        }
    }

    /// End the current row, ensuring all columns have the same length.
    ///
    /// Call this after appending all values for a row. Any columns that
    /// weren't updated will get a null appended.
    pub fn end_row(&mut self) {
        self.row_count += 1;
        for builder in self.columns.values_mut() {
            while builder.len() < self.row_count {
                builder.append_null();
            }
        }
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    /// Get column names in order.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|s| s.as_str())
    }

    /// Finish building and return a RecordBatch.
    pub fn finish(self) -> Result<RecordBatch, BlpError> {
        if self.columns.is_empty() {
            // Return empty batch with no columns
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        // Build schema and arrays
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());

        for (name, mut builder) in self.columns {
            fields.push(Field::new(&name, builder.data_type(), true));
            arrays.push(builder.finish());
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }

    /// Build with a specific column order.
    ///
    /// Columns not in `order` are appended at the end.
    /// Columns in `order` but not in the set are skipped.
    pub fn finish_with_order(mut self, order: &[&str]) -> Result<RecordBatch, BlpError> {
        if self.columns.is_empty() {
            return self.build_empty_with_order(order);
        }

        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        let mut used = std::collections::HashSet::new();

        // First, add columns in specified order
        for &name in order {
            if let Some(mut builder) = self.columns.swap_remove(name) {
                fields.push(Field::new(name, builder.data_type(), true));
                arrays.push(builder.finish());
                used.insert(name.to_string());
            }
        }

        // Then, add remaining columns in their original order
        for (name, mut builder) in self.columns {
            if !used.contains(&name) {
                fields.push(Field::new(&name, builder.data_type(), true));
                arrays.push(builder.finish());
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }
}

impl Default for ColumnSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Create an Arrow Field from a field name and ArrowType.
pub fn create_field(name: &str, arrow_type: ArrowType, nullable: bool) -> Field {
    Field::new(name, arrow_type.to_arrow_datatype(), nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_type_parse() {
        assert_eq!(ArrowType::parse("float64"), ArrowType::Float64);
        assert_eq!(ArrowType::parse("INT64"), ArrowType::Int64);
        assert_eq!(ArrowType::parse("string"), ArrowType::String);
        assert_eq!(ArrowType::parse("unknown"), ArrowType::String);
    }

    #[test]
    fn test_arrow_type_from_value() {
        assert_eq!(
            ArrowType::from_value(&Value::Float64(1.0)),
            ArrowType::Float64
        );
        assert_eq!(ArrowType::from_value(&Value::Int64(1)), ArrowType::Int64);
        assert_eq!(
            ArrowType::from_value(&Value::String("x")),
            ArrowType::String
        );
        assert_eq!(ArrowType::from_value(&Value::Bool(true)), ArrowType::Bool);
    }

    #[test]
    fn test_column_set_basic() {
        let mut cols = ColumnSet::new();

        cols.append("ticker", Value::String("AAPL"));
        cols.append("price", Value::Float64(150.0));
        cols.end_row();

        cols.append("ticker", Value::String("MSFT"));
        cols.append("price", Value::Float64(300.0));
        cols.end_row();

        assert_eq!(cols.row_count(), 2);
        assert_eq!(cols.column_count(), 2);

        let batch = cols.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_column_set_with_nulls() {
        let mut cols = ColumnSet::new();

        cols.append("a", Value::Int64(1));
        cols.append("b", Value::Int64(2));
        cols.end_row();

        cols.append("a", Value::Int64(3));
        // Don't append "b" - should get null
        cols.end_row();

        let batch = cols.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_column_set_late_value_pads_prior_rows() {
        use arrow_array::{Array, StringArray};

        let mut cols = ColumnSet::new();
        cols.append("a", Value::Int64(1));
        cols.end_row();

        cols.append("a", Value::Int64(2));
        cols.append("late", Value::String("x"));
        cols.end_row();

        let batch = cols.finish_with_order(&["a", "late"]).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let late = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(late.is_null(0));
        assert_eq!(late.value(1), "x");
    }

    #[test]
    fn test_column_set_late_null_pads_current_and_prior_rows() {
        use arrow_array::{Array, StringArray};

        let mut cols = ColumnSet::new();
        cols.append("a", Value::Int64(1));
        cols.end_row();

        cols.append("a", Value::Int64(2));
        cols.append_null("late");
        cols.end_row();

        let batch = cols.finish_with_order(&["a", "late"]).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let late = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(late.is_null(0));
        assert!(late.is_null(1));
    }

    #[test]
    fn test_column_set_type_hints() {
        let mut cols = ColumnSet::with_type_hints([("price".to_string(), ArrowType::Float64)]);

        // First value is null, but we want float64 column
        cols.append_null("price");
        cols.end_row();

        cols.append("price", Value::Float64(100.0));
        cols.end_row();

        let batch = cols.finish().unwrap();
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_finish_with_order_empty_columns_non_matching_hints() {
        let cols = ColumnSet::with_type_hints([("PX_LAST".to_string(), ArrowType::Float64)]);
        let batch = cols
            .finish_with_order(&["ticker", "field", "value"])
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "field");
        assert_eq!(batch.schema().field(2).name(), "value");
    }

    #[test]
    fn typed_builder_string_formats_scalar_values() {
        use arrow_array::StringArray;

        let mut builder = TypedBuilder::new(ArrowType::String);
        builder.append_value(Some(Value::Float64(123.45)));
        builder.append_value(Some(Value::Int64(-42)));
        builder.append_value(Some(Value::Int32(7)));
        builder.append_value(Some(Value::Bool(true)));
        builder.append_value(Some(Value::Bool(false)));
        builder.append_value(Some(Value::Byte(255)));

        let array = builder.finish();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "123.45");
        assert_eq!(strings.value(1), "-42");
        assert_eq!(strings.value(2), "7");
        assert_eq!(strings.value(3), "true");
        assert_eq!(strings.value(4), "false");
        assert_eq!(strings.value(5), "255");
    }

    #[test]
    fn test_finish_with_order_empty_columns_no_hints() {
        let cols = ColumnSet::new();
        let batch = cols
            .finish_with_order(&["ticker", "field", "value"])
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "field");
        assert_eq!(batch.schema().field(2).name(), "value");
    }
}
