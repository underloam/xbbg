use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, NullArray,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::buffer::{Buffer as ArrowBuffer, MutableBuffer};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use napi::bindgen_prelude::{
    Buffer, BufferSlice, Env, JsObjectValue, JsValue, Object, ToNapiValue,
};
use napi::{Error, Result, Status};

use crate::StringPair;

pub struct NativeArrowBatch {
    num_rows: usize,
    columns: Vec<NativeArrowColumn>,
    metadata: Vec<StringPair>,
}

struct NativeArrowColumn {
    name: String,
    arrow_type: NativeArrowType,
    nullable: bool,
    length: usize,
    null_count: usize,
    data: Option<MutableBuffer>,
    offsets: Option<MutableBuffer>,
    null_bitmap: Option<MutableBuffer>,
}

struct PendingArrowColumn {
    name: String,
    arrow_type: NativeArrowType,
    nullable: bool,
    length: usize,
    null_count: usize,
    data: Option<PendingData>,
    offsets: Option<PendingOffsets>,
    null_bitmap: Option<PendingBitmap>,
}

enum PendingData {
    Bytes(ArrowBuffer),
    Bitmap(PendingBitmap),
}

enum PendingOffsets {
    I32(Vec<i32>),
    I64(Vec<i64>),
}

struct PendingBitmap {
    buffer: ArrowBuffer,
    bit_offset: usize,
    bit_len: usize,
}

#[derive(Clone)]
enum NativeArrowType {
    Bool,
    Binary,
    Date32,
    Date64,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    LargeBinary,
    LargeUtf8,
    Null,
    Time32Millisecond,
    Time32Second,
    Time64Microsecond,
    Time64Nanosecond,
    TimestampMicrosecond { timezone: Option<String> },
    TimestampMillisecond { timezone: Option<String> },
    TimestampNanosecond { timezone: Option<String> },
    TimestampSecond { timezone: Option<String> },
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
}

struct ExternalBufferOwner {
    _buffer: MutableBuffer,
}

fn checked_js_u32_len(label: &str, value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{label} {value} exceeds the JavaScript native Arrow length limit"),
        )
    })
}

impl NativeArrowBatch {
    pub fn from_record_batch(batch: RecordBatch) -> Result<Self> {
        let unsupported = unsupported_columns(&batch);
        if !unsupported.is_empty() {
            return Err(Error::new(
                Status::GenericFailure,
                format!(
                    "native subscription transfer does not support this Arrow schema. \
                     Unsupported columns: {}. Supported subscription column types are: \
                     bool, binary, date32, date64, float32, float64, int8, int16, int32, int64, \
                     large_binary, large_utf8, null, time32[s], time32[ms], time64[us], time64[ns], \
                     timestamp[s], timestamp[ms], timestamp[us], timestamp[ns], uint8, uint16, uint32, uint64, utf8.",
                    unsupported.join("; ")
                ),
            ));
        }

        let metadata = batch
            .schema_ref()
            .metadata()
            .iter()
            .map(|(key, value)| StringPair {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let (schema, arrays, num_rows) = batch.into_parts();
        let pending = arrays
            .iter()
            .enumerate()
            .map(|(idx, array)| {
                let field = schema.field(idx);
                PendingArrowColumn::from_array(
                    field.name().clone(),
                    field.is_nullable(),
                    array.as_ref(),
                )
            })
            .collect::<Vec<_>>();

        // Dropping every Arrow array before claiming mutable ownership ensures
        // `Buffer::into_mutable` cannot leave a Rust reader alias behind.
        drop(arrays);
        let columns = pending
            .into_iter()
            .map(PendingArrowColumn::into_native)
            .collect();

        Ok(Self {
            num_rows,
            columns,
            metadata,
        })
    }
}

impl PendingArrowColumn {
    fn from_array(name: String, nullable: bool, array: &dyn Array) -> Self {
        let null_bitmap = array.nulls().map(|nulls| {
            let bits = nulls.inner();
            PendingBitmap {
                buffer: bits.inner().clone(),
                bit_offset: bits.offset(),
                bit_len: bits.len(),
            }
        });
        let null_count = array.null_count();
        let length = array.len();

        macro_rules! primitive_column {
            ($array_ty:ty, $arrow_type:expr, $expect:literal) => {{
                let values = array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .expect($expect)
                    .values()
                    .inner()
                    .clone();
                Self {
                    name,
                    arrow_type: $arrow_type,
                    nullable,
                    length,
                    null_count,
                    data: Some(PendingData::Bytes(values)),
                    offsets: None,
                    null_bitmap,
                }
            }};
        }

        macro_rules! variable_width_column {
            ($array_ty:ty, $arrow_type:expr, $offset_kind:ident, $expect:literal) => {{
                let values = array.as_any().downcast_ref::<$array_ty>().expect($expect);
                let source_offsets = values.value_offsets();
                let first = source_offsets[0];
                let last = source_offsets[source_offsets.len() - 1];
                let data_start =
                    usize::try_from(first).expect("validated Arrow offset must be non-negative");
                let data_end =
                    usize::try_from(last).expect("validated Arrow offset must be non-negative");
                let offsets = source_offsets
                    .iter()
                    .map(|offset| *offset - first)
                    .collect::<Vec<_>>();
                Self {
                    name,
                    arrow_type: $arrow_type,
                    nullable,
                    length,
                    null_count,
                    data: Some(PendingData::Bytes(
                        values
                            .values()
                            .slice_with_length(data_start, data_end - data_start),
                    )),
                    offsets: Some(PendingOffsets::$offset_kind(offsets)),
                    null_bitmap,
                }
            }};
        }

        match array.data_type() {
            DataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("supported boolean array")
                    .values();
                Self {
                    name,
                    arrow_type: NativeArrowType::Bool,
                    nullable,
                    length,
                    null_count,
                    data: Some(PendingData::Bitmap(PendingBitmap {
                        buffer: values.inner().clone(),
                        bit_offset: values.offset(),
                        bit_len: values.len(),
                    })),
                    offsets: None,
                    null_bitmap,
                }
            }
            DataType::Binary => variable_width_column!(
                BinaryArray,
                NativeArrowType::Binary,
                I32,
                "supported binary array"
            ),
            DataType::Date32 => primitive_column!(
                Date32Array,
                NativeArrowType::Date32,
                "supported date32 array"
            ),
            DataType::Date64 => primitive_column!(
                Date64Array,
                NativeArrowType::Date64,
                "supported date64 array"
            ),
            DataType::Float32 => primitive_column!(
                Float32Array,
                NativeArrowType::Float32,
                "supported float32 array"
            ),
            DataType::Float64 => primitive_column!(
                Float64Array,
                NativeArrowType::Float64,
                "supported float64 array"
            ),
            DataType::Int8 => {
                primitive_column!(Int8Array, NativeArrowType::Int8, "supported int8 array")
            }
            DataType::Int16 => {
                primitive_column!(Int16Array, NativeArrowType::Int16, "supported int16 array")
            }
            DataType::Int32 => {
                primitive_column!(Int32Array, NativeArrowType::Int32, "supported int32 array")
            }
            DataType::Int64 => {
                primitive_column!(Int64Array, NativeArrowType::Int64, "supported int64 array")
            }
            DataType::LargeBinary => variable_width_column!(
                LargeBinaryArray,
                NativeArrowType::LargeBinary,
                I64,
                "supported large_binary array"
            ),
            DataType::LargeUtf8 => variable_width_column!(
                LargeStringArray,
                NativeArrowType::LargeUtf8,
                I64,
                "supported large_utf8 array"
            ),
            DataType::Null => {
                let _ = array
                    .as_any()
                    .downcast_ref::<NullArray>()
                    .expect("supported null array");
                Self {
                    name,
                    arrow_type: NativeArrowType::Null,
                    nullable,
                    length,
                    null_count,
                    data: None,
                    offsets: None,
                    null_bitmap,
                }
            }
            DataType::Time32(TimeUnit::Second) => primitive_column!(
                Time32SecondArray,
                NativeArrowType::Time32Second,
                "supported time32[s] array"
            ),
            DataType::Time32(TimeUnit::Millisecond) => primitive_column!(
                Time32MillisecondArray,
                NativeArrowType::Time32Millisecond,
                "supported time32[ms] array"
            ),
            DataType::Time64(TimeUnit::Microsecond) => primitive_column!(
                Time64MicrosecondArray,
                NativeArrowType::Time64Microsecond,
                "supported time64[us] array"
            ),
            DataType::Time64(TimeUnit::Nanosecond) => primitive_column!(
                Time64NanosecondArray,
                NativeArrowType::Time64Nanosecond,
                "supported time64[ns] array"
            ),
            DataType::Timestamp(TimeUnit::Second, timezone) => primitive_column!(
                TimestampSecondArray,
                NativeArrowType::TimestampSecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[s] array"
            ),
            DataType::Timestamp(TimeUnit::Millisecond, timezone) => primitive_column!(
                TimestampMillisecondArray,
                NativeArrowType::TimestampMillisecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[ms] array"
            ),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => primitive_column!(
                TimestampMicrosecondArray,
                NativeArrowType::TimestampMicrosecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[us] array"
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => primitive_column!(
                TimestampNanosecondArray,
                NativeArrowType::TimestampNanosecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[ns] array"
            ),
            DataType::UInt8 => {
                primitive_column!(UInt8Array, NativeArrowType::UInt8, "supported uint8 array")
            }
            DataType::UInt16 => primitive_column!(
                UInt16Array,
                NativeArrowType::UInt16,
                "supported uint16 array"
            ),
            DataType::UInt32 => primitive_column!(
                UInt32Array,
                NativeArrowType::UInt32,
                "supported uint32 array"
            ),
            DataType::UInt64 => primitive_column!(
                UInt64Array,
                NativeArrowType::UInt64,
                "supported uint64 array"
            ),
            DataType::Utf8 => variable_width_column!(
                StringArray,
                NativeArrowType::Utf8,
                I32,
                "supported utf8 array"
            ),
            _ => unreachable!("unsupported array checked before conversion"),
        }
    }

    fn into_native(self) -> NativeArrowColumn {
        NativeArrowColumn {
            name: self.name,
            arrow_type: self.arrow_type,
            nullable: self.nullable,
            length: self.length,
            null_count: self.null_count,
            data: self.data.map(PendingData::into_mutable),
            offsets: self.offsets.map(PendingOffsets::into_mutable),
            null_bitmap: self.null_bitmap.map(PendingBitmap::into_mutable),
        }
    }
}

impl PendingData {
    fn into_mutable(self) -> MutableBuffer {
        match self {
            Self::Bytes(buffer) => into_exclusive_mutable(buffer),
            Self::Bitmap(bitmap) => bitmap.into_mutable(),
        }
    }
}

impl PendingOffsets {
    fn into_mutable(self) -> MutableBuffer {
        match self {
            Self::I32(offsets) => MutableBuffer::from(offsets),
            Self::I64(offsets) => MutableBuffer::from(offsets),
        }
    }
}

impl PendingBitmap {
    fn into_mutable(self) -> MutableBuffer {
        let canonical = self.buffer.bit_slice(self.bit_offset, self.bit_len);
        let mut buffer = into_exclusive_mutable(canonical);
        let trailing_bits = self.bit_len % 8;
        if trailing_bits != 0 {
            let last = buffer.len() - 1;
            buffer.as_slice_mut()[last] &= (1_u8 << trailing_bits) - 1;
        }
        buffer
    }
}

fn into_exclusive_mutable(buffer: ArrowBuffer) -> MutableBuffer {
    let bounded_capacity = buffer
        .len()
        .checked_add(63)
        .map(|len| len & !63)
        .unwrap_or(usize::MAX);
    if buffer.ptr_offset() == 0 && buffer.capacity() <= bounded_capacity {
        match buffer.into_mutable() {
            Ok(buffer) => return buffer,
            Err(buffer) => return MutableBuffer::from(buffer.as_slice().to_vec()),
        }
    }
    MutableBuffer::from(buffer.as_slice().to_vec())
}

impl NativeArrowType {
    fn label(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Binary => "binary",
            Self::Date32 => "date32",
            Self::Date64 => "date64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::LargeBinary => "large_binary",
            Self::LargeUtf8 => "large_utf8",
            Self::Null => "null",
            Self::Time32Millisecond => "time32_ms",
            Self::Time32Second => "time32_s",
            Self::Time64Microsecond => "time64_us",
            Self::Time64Nanosecond => "time64_ns",
            Self::TimestampMicrosecond { .. } => "timestamp_us",
            Self::TimestampMillisecond { .. } => "timestamp_ms",
            Self::TimestampNanosecond { .. } => "timestamp_ns",
            Self::TimestampSecond { .. } => "timestamp_s",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Utf8 => "utf8",
        }
    }

    fn timezone(&self) -> Option<&str> {
        match self {
            Self::TimestampMicrosecond { timezone }
            | Self::TimestampMillisecond { timezone }
            | Self::TimestampNanosecond { timezone }
            | Self::TimestampSecond { timezone } => timezone.as_deref(),
            _ => None,
        }
    }
}

fn unsupported_columns(batch: &RecordBatch) -> Vec<String> {
    let schema = batch.schema();
    batch
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, array)| {
            unsupported_array_reason(array.as_ref()).map(|reason| {
                let field = schema.field(idx);
                format!("#{idx} '{}' ({reason})", field.name())
            })
        })
        .collect()
}

fn unsupported_array_reason(array: &dyn Array) -> Option<String> {
    match array.data_type() {
        DataType::Boolean
        | DataType::Binary
        | DataType::Date32
        | DataType::Date64
        | DataType::Float32
        | DataType::Float64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::LargeBinary
        | DataType::LargeUtf8
        | DataType::Null
        | DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8 => None,
        data_type => Some(format!("unsupported type={data_type:?}")),
    }
}

/// Transfer an exclusively owned mutable allocation to a V8 `Buffer`.
///
/// Canonicalization and any required copy happen before this boundary. V8 is
/// therefore the only remaining reader or writer of the exposed allocation.
fn external_buffer(env: &Env, mut buffer: MutableBuffer) -> Result<Option<Buffer>> {
    let len = buffer.len();
    if len == 0 {
        return Ok(None);
    }

    let data = buffer.as_mut_ptr();
    let owner = ExternalBufferOwner { _buffer: buffer };
    // SAFETY: `data` and `len` describe the uniquely owned mutable allocation
    // transferred into `owner`. No Arrow array or other exported Buffer aliases
    // it, and the finalizer retains the allocation for V8's full lifetime.
    let slice = unsafe {
        BufferSlice::from_external(env, data, len, owner, |_env, _owner| {
            // Dropping the owner releases the allocation once V8 is done.
        })?
    };
    slice.into_buffer(env).map(Some)
}

impl ToNapiValue for NativeArrowBatch {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut obj = Object::new(&env)?;
        let mut metadata = Object::new(&env)?;
        for pair in value.metadata {
            metadata.set_named_property(&pair.key, pair.value)?;
        }
        obj.set_named_property("kind", "zeroCopy")?;
        obj.set_named_property("numRows", checked_js_u32_len("numRows", value.num_rows)?)?;
        obj.set_named_property("columns", value.columns)?;
        obj.set_named_property("metadata", metadata)?;
        Ok(obj.raw())
    }
}

impl ToNapiValue for NativeArrowColumn {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut obj = Object::new(&env)?;
        obj.set_named_property("name", value.name)?;
        obj.set_named_property("type", value.arrow_type.label())?;
        obj.set_named_property("nullable", value.nullable)?;
        obj.set_named_property("length", checked_js_u32_len("length", value.length)?)?;
        obj.set_named_property(
            "nullCount",
            checked_js_u32_len("nullCount", value.null_count)?,
        )?;
        if let Some(timezone) = value.arrow_type.timezone() {
            obj.set_named_property("timezone", timezone)?;
        }
        if let Some(buffer) = value.data {
            if let Some(buffer) = external_buffer(&env, buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow data buffer: {e}"),
                )
            })? {
                obj.set_named_property("data", buffer)?;
            }
        }
        if let Some(buffer) = value.offsets {
            if let Some(buffer) = external_buffer(&env, buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow offsets buffer: {e}"),
                )
            })? {
                obj.set_named_property("offsets", buffer)?;
            }
        }
        if let Some(buffer) = value.null_bitmap {
            if let Some(buffer) = external_buffer(&env, buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow null bitmap: {e}"),
                )
            })? {
                obj.set_named_property("nullBitmap", buffer)?;
            }
        }
        Ok(obj.raw())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn one_column_batch(name: &str, array: ArrayRef) -> RecordBatch {
        let field = Field::new(name, array.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
    }

    fn read_i32_offsets(buffer: &MutableBuffer) -> Vec<i32> {
        buffer
            .as_slice()
            .as_chunks::<{ std::mem::size_of::<i32>() }>()
            .0
            .iter()
            .map(|bytes| i32::from_ne_bytes(*bytes))
            .collect()
    }

    #[test]
    fn checked_js_u32_len_accepts_u32_max() {
        assert_eq!(
            checked_js_u32_len("length", u32::MAX as usize).unwrap(),
            u32::MAX
        );
    }

    #[test]
    fn checked_js_u32_len_rejects_overflow() {
        let err = checked_js_u32_len("length", (u32::MAX as usize) + 1).unwrap_err();
        assert_eq!(err.status, Status::InvalidArg);
        assert!(err
            .reason
            .contains("exceeds the JavaScript native Arrow length limit"));
    }

    #[test]
    fn uniquely_owned_canonical_values_keep_their_allocation() {
        let values = Int64Array::from(vec![7_i64, 11]);
        let original = values.values().inner().as_ptr();
        let batch = one_column_batch("value", Arc::new(values));

        let native = NativeArrowBatch::from_record_batch(batch).unwrap();

        assert_eq!(native.columns[0].data.as_ref().unwrap().as_ptr(), original);
    }

    #[test]
    fn shared_values_are_copied_before_mutable_export() {
        let values = Arc::new(Int64Array::from(vec![7_i64, 11]));
        let original = values.values().inner().as_ptr();
        let batch = one_column_batch("value", values.clone());

        let mut native = NativeArrowBatch::from_record_batch(batch).unwrap();
        let exported = native.columns[0].data.as_mut().unwrap();
        assert_ne!(exported.as_ptr(), original);
        exported.as_slice_mut()[..8].copy_from_slice(&99_i64.to_ne_bytes());

        assert_eq!(values.value(0), 7);
    }

    #[test]
    fn duplicate_columns_never_export_aliasing_mutable_buffers() {
        let values = Arc::new(Int64Array::from(vec![7_i64, 11]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int64, false),
            Field::new("right", DataType::Int64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![values.clone() as ArrayRef, values as ArrayRef])
                .unwrap();

        let mut native = NativeArrowBatch::from_record_batch(batch).unwrap();
        let left = native.columns[0].data.as_ref().unwrap().as_ptr();
        let right = native.columns[1].data.as_ref().unwrap().as_ptr();
        assert_ne!(left, right);
        native.columns[0].data.as_mut().unwrap().as_slice_mut()[..8]
            .copy_from_slice(&99_i64.to_ne_bytes());
        let right = native.columns[1].data.as_ref().unwrap().as_slice();
        assert_eq!(i64::from_ne_bytes(right[..8].try_into().unwrap()), 7);
    }

    #[test]
    fn prefix_boolean_slice_masks_trailing_value_and_validity_bits() {
        let source = BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let sliced = source.slice(0, 1);
        drop(source);
        let native =
            NativeArrowBatch::from_record_batch(one_column_batch("flag", Arc::new(sliced)))
                .unwrap();
        let column = &native.columns[0];

        assert_eq!(column.data.as_ref().unwrap().as_slice(), &[0b0000_0001]);
        assert_eq!(
            column.null_bitmap.as_ref().unwrap().as_slice(),
            &[0b0000_0001]
        );
    }

    #[test]
    fn utf8_prefix_slice_rebases_offsets_and_does_not_retain_suffix_bytes() {
        let secret = "secret".repeat(65_536);
        let source = StringArray::from(vec![Some("visible"), Some(secret.as_str())]);
        let sliced = source.slice(0, 1);
        drop(source);
        let native =
            NativeArrowBatch::from_record_batch(one_column_batch("text", Arc::new(sliced)))
                .unwrap();
        let column = &native.columns[0];

        assert_eq!(column.data.as_ref().unwrap().as_slice(), b"visible");
        assert_eq!(read_i32_offsets(column.offsets.as_ref().unwrap()), [0, 7]);
        assert!(column.data.as_ref().unwrap().capacity() <= 64);
    }

    #[test]
    fn binary_slice_rebases_offsets_and_exports_only_logical_bytes() {
        let source = BinaryArray::from(vec![
            Some(b"hidden".as_slice()),
            Some(b"abc".as_slice()),
            Some(b"suffix".as_slice()),
        ]);
        let sliced = source.slice(1, 1);
        drop(source);
        let native =
            NativeArrowBatch::from_record_batch(one_column_batch("bytes", Arc::new(sliced)))
                .unwrap();
        let column = &native.columns[0];

        assert_eq!(column.data.as_ref().unwrap().as_slice(), b"abc");
        assert_eq!(read_i32_offsets(column.offsets.as_ref().unwrap()), [0, 3]);
    }

    #[test]
    fn native_column_buffers_outlive_arrays_without_retaining_the_batch() {
        let values = Arc::new(Int64Array::from(vec![7_i64, 11]));
        let weak_values = Arc::downgrade(&values);
        let batch = one_column_batch("value", values.clone());

        let native = NativeArrowBatch::from_record_batch(batch).unwrap();
        drop(values);

        assert!(weak_values.upgrade().is_none());
        let data = native.columns[0].data.as_ref().unwrap().as_slice();
        assert_eq!(i64::from_ne_bytes(data[..8].try_into().unwrap()), 7);
        assert_eq!(i64::from_ne_bytes(data[8..16].try_into().unwrap()), 11);
    }
}
