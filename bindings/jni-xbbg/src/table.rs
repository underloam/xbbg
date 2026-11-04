//! Arrow record batches as Java `table.Table` objects.
//!
//! A batch is first canonicalized exactly like the napi zero-copy exporter
//! (`bindings/napi-xbbg/src/arrow_zero_copy.rs`): every buffer starts at offset
//! zero, bitmaps are re-sliced to bit 0 with the bits past the last row
//! cleared, variable-width offsets are rebased to start at zero and value
//! bytes are trimmed to the used range. Canonical buffers borrow the Arrow
//! allocation whenever it already has that shape and are copied otherwise;
//! each then crosses into Java with one bulk copy. The batch arrives at
//! `NativeBridge.table(...)` as parallel per-column arrays:
//!
//! - `types[i]`: the type code below; `timezones[i]`: the timestamp zone or null;
//! - `values[i]`: fixed width → `numRows × width` little-endian bytes; BOOL →
//!   `(numRows + 7) / 8` bytes, LSB first; UTF8/BINARY (and LARGE_*) → the value
//!   bytes; NULL → null;
//! - `offsets[i]`: UTF8/BINARY → `numRows + 1` int32; LARGE_* → int64; else null;
//! - `validity[i]`: LSB-first bitmap, or null when `nullCounts[i] == 0`. A NULL
//!   column reports every row null with an all-zero bitmap.
//!
//! Schema metadata crosses as sorted parallel key/value arrays plus
//! [`xbbg_host::metadata::decode`] JSON. Field metadata is not carried (the
//! engine emits none).

use arrow::array::{Array, AsArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use jni::Env;
use jni::objects::{JBooleanArray, JByteArray, JIntArray, JObject, JObjectArray, JString};
use jni::refs::{IntoAuto, Reference};
use jni::signature::JavaType;
use jni::sys::jvalue;
use xbbg_host::errors::HostError;

use crate::bridge::{NativeError, java};
use crate::payload::jsize;

/// Java `table.DataType` codes (napi `NativeArrowColumnType` order).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i8)]
pub(crate) enum TypeCode {
    Null = 0,
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    UInt8 = 6,
    UInt16 = 7,
    UInt32 = 8,
    UInt64 = 9,
    Float32 = 10,
    Float64 = 11,
    Utf8 = 12,
    LargeUtf8 = 13,
    Binary = 14,
    LargeBinary = 15,
    Date32 = 16,
    Date64 = 17,
    Time32S = 18,
    Time32Ms = 19,
    Time64Us = 20,
    Time64Ns = 21,
    TimestampS = 22,
    TimestampMs = 23,
    TimestampUs = 24,
    TimestampNs = 25,
}

impl TypeCode {
    /// The transport code of a supported Arrow type and its timestamp zone.
    pub(crate) fn of(data_type: &DataType) -> Option<(Self, Option<String>)> {
        let zone = |tz: &Option<std::sync::Arc<str>>| tz.as_ref().map(|tz| tz.to_string());
        Some(match data_type {
            DataType::Null => (Self::Null, None),
            DataType::Boolean => (Self::Bool, None),
            DataType::Int8 => (Self::Int8, None),
            DataType::Int16 => (Self::Int16, None),
            DataType::Int32 => (Self::Int32, None),
            DataType::Int64 => (Self::Int64, None),
            DataType::UInt8 => (Self::UInt8, None),
            DataType::UInt16 => (Self::UInt16, None),
            DataType::UInt32 => (Self::UInt32, None),
            DataType::UInt64 => (Self::UInt64, None),
            DataType::Float32 => (Self::Float32, None),
            DataType::Float64 => (Self::Float64, None),
            DataType::Utf8 => (Self::Utf8, None),
            DataType::LargeUtf8 => (Self::LargeUtf8, None),
            DataType::Binary => (Self::Binary, None),
            DataType::LargeBinary => (Self::LargeBinary, None),
            DataType::Date32 => (Self::Date32, None),
            DataType::Date64 => (Self::Date64, None),
            DataType::Time32(TimeUnit::Second) => (Self::Time32S, None),
            DataType::Time32(TimeUnit::Millisecond) => (Self::Time32Ms, None),
            DataType::Time64(TimeUnit::Microsecond) => (Self::Time64Us, None),
            DataType::Time64(TimeUnit::Nanosecond) => (Self::Time64Ns, None),
            DataType::Timestamp(TimeUnit::Second, tz) => (Self::TimestampS, zone(tz)),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => (Self::TimestampMs, zone(tz)),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => (Self::TimestampUs, zone(tz)),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => (Self::TimestampNs, zone(tz)),
            _ => return None,
        })
    }
}

/// Canonical bytes: an Arrow allocation that already has the exact shape, or a copy.
#[derive(Clone, Debug)]
pub(crate) enum Bytes {
    Shared(Buffer),
    Owned(Vec<u8>),
}

impl Bytes {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Shared(buffer) => buffer.as_slice(),
            Self::Owned(bytes) => bytes,
        }
    }
}

/// One batch, canonicalized and ready to cross into Java.
#[derive(Clone, Debug)]
pub(crate) struct TableTransport {
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<TypeCode>,
    pub(crate) timezones: Vec<Option<String>>,
    pub(crate) nullable: Vec<bool>,
    pub(crate) num_rows: i32,
    pub(crate) null_counts: Vec<i32>,
    pub(crate) values: Vec<Option<Bytes>>,
    pub(crate) offsets: Vec<Option<Bytes>>,
    pub(crate) validity: Vec<Option<Bytes>>,
    pub(crate) metadata: Vec<(String, String)>,
    pub(crate) decoded_metadata: String,
}

/// Unsupported columns fail listing every offender (napi's text, without its
/// subscription wording: requests and subscriptions share this transport).
fn unsupported_columns(batch: &RecordBatch) -> Option<String> {
    let schema = batch.schema_ref();
    let unsupported: Vec<String> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| TypeCode::of(field.data_type()).is_none())
        .map(|(index, field)| {
            format!(
                "#{index} '{}' (unsupported type={:?})",
                field.name(),
                field.data_type()
            )
        })
        .collect();
    (!unsupported.is_empty()).then(|| {
        format!(
            "native table transfer does not support this Arrow schema. \
             Unsupported columns: {}. Supported column types are: \
             bool, binary, date32, date64, float32, float64, int8, int16, int32, int64, \
             large_binary, large_utf8, null, time32[s], time32[ms], time64[us], time64[ns], \
             timestamp[s], timestamp[ms], timestamp[us], timestamp[ns], uint8, uint16, uint32, uint64, utf8.",
            unsupported.join("; ")
        )
    })
}

/// A bitmap re-sliced to bit 0 with the bits past `len` cleared.
fn bitmap(buffer: &Buffer, offset: usize, len: usize) -> Bytes {
    let sliced = buffer.bit_slice(offset, len);
    let trailing = len % 8;
    match sliced.as_slice().last() {
        Some(&last) if trailing != 0 && last >> trailing != 0 => {
            let mut owned = sliced.as_slice().to_vec();
            if let Some(last) = owned.last_mut() {
                *last &= (1_u8 << trailing) - 1;
            }
            Bytes::Owned(owned)
        }
        _ => Bytes::Shared(sliced),
    }
}

/// Offsets rebased to zero and the value bytes they cover.
fn variable_width<O: Copy + Into<i64> + arrow::datatypes::ArrowNativeType>(
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &Buffer,
) -> (Bytes, Bytes) {
    let first: i64 = offsets[0].into();
    let last: i64 = offsets[offsets.len() - 1].into();
    let start = usize::try_from(first).expect("validated Arrow offsets are non-negative");
    let end = usize::try_from(last).expect("validated Arrow offsets are non-negative");
    let data = Bytes::Shared(values.slice_with_length(start, end - start));
    let offsets = if first == 0 {
        Bytes::Shared(offsets.inner().inner().clone())
    } else {
        let width = std::mem::size_of::<O>();
        let mut rebased = Vec::with_capacity(offsets.len() * width);
        for offset in offsets.iter() {
            let value: i64 = (*offset).into() - first;
            if width == 4 {
                let value = i32::try_from(value).expect("rebased i32 offsets stay in range");
                rebased.extend_from_slice(&value.to_le_bytes());
            } else {
                rebased.extend_from_slice(&value.to_le_bytes());
            }
        }
        Bytes::Owned(rebased)
    };
    (data, offsets)
}

struct Column {
    values: Option<Bytes>,
    offsets: Option<Bytes>,
    validity: Option<Bytes>,
    null_count: usize,
}

fn column(array: &dyn Array, code: TypeCode) -> Column {
    let len = array.len();
    let (values, offsets) = match code {
        TypeCode::Null => (None, None),
        TypeCode::Bool => {
            let values = array.as_boolean().values();
            (
                Some(bitmap(values.inner(), values.offset(), values.len())),
                None,
            )
        }
        TypeCode::Utf8 => {
            let array = array.as_string::<i32>();
            let (data, offsets) = variable_width(array.offsets(), array.values());
            (Some(data), Some(offsets))
        }
        TypeCode::LargeUtf8 => {
            let array = array.as_string::<i64>();
            let (data, offsets) = variable_width(array.offsets(), array.values());
            (Some(data), Some(offsets))
        }
        TypeCode::Binary => {
            let array = array.as_binary::<i32>();
            let (data, offsets) = variable_width(array.offsets(), array.values());
            (Some(data), Some(offsets))
        }
        TypeCode::LargeBinary => {
            let array = array.as_binary::<i64>();
            let (data, offsets) = variable_width(array.offsets(), array.values());
            (Some(data), Some(offsets))
        }
        _ => {
            let data = array.to_data();
            let width = data
                .data_type()
                .primitive_width()
                .expect("every remaining supported type is fixed width");
            let values = data.buffers()[0].slice_with_length(data.offset() * width, len * width);
            (Some(Bytes::Shared(values)), None)
        }
    };
    let (null_count, validity) = if code == TypeCode::Null {
        let zeros = (len > 0).then(|| Bytes::Owned(vec![0; len.div_ceil(8)]));
        (len, zeros)
    } else {
        let null_count = array.null_count();
        let validity = array.nulls().filter(|_| null_count > 0).map(|nulls| {
            let bits = nulls.inner();
            bitmap(bits.inner(), bits.offset(), bits.len())
        });
        (null_count, validity)
    };
    Column {
        values,
        offsets,
        validity,
        null_count,
    }
}

impl TableTransport {
    /// Canonicalize `batch`; unsupported types and buffers too large for a
    /// Java array are INTERNAL errors.
    pub(crate) fn new(batch: &RecordBatch) -> Result<Self, HostError> {
        if let Some(message) = unsupported_columns(batch) {
            return Err(HostError::internal(message));
        }
        let schema = batch.schema_ref();
        let columns = batch.num_columns();
        let mut transport = Self {
            names: Vec::with_capacity(columns),
            types: Vec::with_capacity(columns),
            timezones: Vec::with_capacity(columns),
            nullable: Vec::with_capacity(columns),
            num_rows: jsize(batch.num_rows(), "table row count")?,
            null_counts: Vec::with_capacity(columns),
            values: Vec::with_capacity(columns),
            offsets: Vec::with_capacity(columns),
            validity: Vec::with_capacity(columns),
            metadata: Vec::new(),
            decoded_metadata: xbbg_host::metadata::decode(schema.metadata()).to_json(),
        };
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            let (code, zone) =
                TypeCode::of(field.data_type()).expect("unsupported columns rejected above");
            let column = column(array.as_ref(), code);
            for (bytes, part) in [
                (&column.values, "values"),
                (&column.offsets, "offsets"),
                (&column.validity, "validity"),
            ] {
                if let Some(bytes) = bytes {
                    jsize(
                        bytes.as_slice().len(),
                        &format!("column '{}' {part}", field.name()),
                    )?;
                }
            }
            transport.names.push(field.name().clone());
            transport.types.push(code);
            transport.timezones.push(zone);
            transport.nullable.push(field.is_nullable());
            transport
                .null_counts
                .push(jsize(column.null_count, "null count")?);
            transport.values.push(column.values);
            transport.offsets.push(column.offsets);
            transport.validity.push(column.validity);
        }
        transport.metadata = schema
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        transport.metadata.sort_unstable();
        Ok(transport)
    }

    /// Build the Java `Table` through `NativeBridge.table(...)`.
    pub(crate) fn to_java<'local>(
        &self,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError> {
        let java = java()?;
        env.with_local_frame_returning_local::<_, JObject, NativeError>(32, |env| {
            let columns = self.names.len();
            let names = string_array(env, self.names.iter().map(|name| Some(name.as_str())))?;
            let types = JByteArray::new(env, columns)?;
            let codes: Vec<i8> = self.types.iter().map(|code| *code as i8).collect();
            types.set_region(env, 0, &codes)?;
            let timezones = string_array(env, self.timezones.iter().map(Option::as_deref))?;
            let nullable = JBooleanArray::new(env, columns)?;
            nullable.set_region(env, 0, &self.nullable)?;
            let null_counts = JIntArray::new(env, columns)?;
            null_counts.set_region(env, 0, &self.null_counts)?;
            let values = bytes_array(env, &self.values)?;
            let offsets = bytes_array(env, &self.offsets)?;
            let validity = bytes_array(env, &self.validity)?;
            let keys = string_array(env, self.metadata.iter().map(|(key, _)| Some(key.as_str())))?;
            let metadata_values = string_array(
                env,
                self.metadata.iter().map(|(_, value)| Some(value.as_str())),
            )?;
            let decoded = JString::new(env, &self.decoded_metadata)?;
            let args = [
                jvalue { l: names.as_raw() },
                jvalue { l: types.as_raw() },
                jvalue {
                    l: timezones.as_raw(),
                },
                jvalue {
                    l: nullable.as_raw(),
                },
                jvalue { i: self.num_rows },
                jvalue {
                    l: null_counts.as_raw(),
                },
                jvalue { l: values.as_raw() },
                jvalue {
                    l: offsets.as_raw(),
                },
                jvalue {
                    l: validity.as_raw(),
                },
                jvalue { l: keys.as_raw() },
                jvalue {
                    l: metadata_values.as_raw(),
                },
                jvalue {
                    l: decoded.as_raw(),
                },
            ];
            // SAFETY: `table` was resolved in JNI_OnLoad with exactly this
            // signature and every argument has the declared Java type.
            let table = unsafe {
                env.call_static_method_unchecked(&java.bridge, java.table, JavaType::Object, &args)
            }?;
            Ok(table.l()?)
        })
    }
}

/// Convert one batch to a Java `Table`.
pub(crate) fn to_java<'local>(
    env: &mut Env<'local>,
    batch: &RecordBatch,
) -> Result<JObject<'local>, NativeError> {
    TableTransport::new(batch)?.to_java(env)
}

/// A `String[]` with `null` for absent entries.
fn string_array<'local, 'a>(
    env: &mut Env<'local>,
    items: impl ExactSizeIterator<Item = Option<&'a str>>,
) -> Result<JObjectArray<'local, JString<'local>>, NativeError> {
    let array = JObjectArray::<JString>::new(env, items.len(), JString::null())?;
    for (index, item) in items.enumerate() {
        if let Some(text) = item {
            let text = JString::new(env, text)?.auto();
            array.set_element(env, index, &*text)?;
        }
    }
    Ok(array)
}

/// A `byte[][]` with `null` for absent buffers; one bulk copy per buffer.
fn bytes_array<'local>(
    env: &mut Env<'local>,
    items: &[Option<Bytes>],
) -> Result<JObjectArray<'local, JByteArray<'local>>, NativeError> {
    let array = JObjectArray::<JByteArray>::new(env, items.len(), JByteArray::null())?;
    for (index, item) in items.iter().enumerate() {
        if let Some(bytes) = item {
            let bytes = env.byte_array_from_slice(bytes.as_slice())?.auto();
            array.set_element(env, index, &*bytes)?;
        }
    }
    Ok(array)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int8Array, Int32Array,
        LargeStringArray, NullArray, StringArray, TimestampMicrosecondArray, UInt64Array,
    };
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap()
    }

    fn bytes(value: &Option<Bytes>) -> Option<Vec<u8>> {
        value.as_ref().map(|bytes| bytes.as_slice().to_vec())
    }

    fn i32s(value: &Option<Bytes>) -> Vec<i32> {
        value
            .as_ref()
            .unwrap()
            .as_slice()
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| i32::from_le_bytes(*chunk))
            .collect()
    }

    fn i64s(value: &Option<Bytes>) -> Vec<i64> {
        value
            .as_ref()
            .unwrap()
            .as_slice()
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| i64::from_le_bytes(*chunk))
            .collect()
    }

    #[test]
    fn primitive_values_are_sliced_to_the_array_window() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let transport = TableTransport::new(&batch(vec![("v", values)]).slice(1, 3)).unwrap();
        assert_eq!(transport.num_rows, 3);
        assert_eq!(transport.types, [TypeCode::Int32]);
        // The null slot keeps the builder's zero; only the window is copied.
        assert_eq!(i32s(&transport.values[0]), [2, 0, 4]);
        assert_eq!(transport.null_counts, [1]);
        // rows 1..4 of [valid, valid, null, valid, valid] -> valid, null, valid
        assert_eq!(bytes(&transport.validity[0]), Some(vec![0b101]));
        assert!(transport.offsets[0].is_none());
    }

    #[test]
    fn booleans_are_resliced_to_bit_zero_with_trailing_bits_cleared() {
        let values: Vec<bool> = (0..20).map(|index| index % 3 == 0).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(values.clone()));
        for (offset, len) in [(0, 20), (3, 10), (8, 9), (5, 0), (19, 1)] {
            let transport =
                TableTransport::new(&batch(vec![("b", Arc::clone(&array))]).slice(offset, len))
                    .unwrap();
            let packed = bytes(&transport.values[0]).unwrap();
            assert_eq!(packed.len(), len.div_ceil(8), "offset {offset} len {len}");
            for row in 0..len {
                let bit = packed[row / 8] >> (row % 8) & 1 == 1;
                assert_eq!(bit, values[offset + row], "offset {offset} row {row}");
            }
            if len % 8 != 0 {
                assert_eq!(packed[len / 8] >> (len % 8), 0, "trailing bits cleared");
            }
            assert!(transport.validity[0].is_none());
        }
    }

    #[test]
    fn dirty_trailing_bits_of_an_aligned_bitmap_are_masked() {
        // An unsliced 4-row bitmap whose spare bits are set, as foreign
        // producers may leave them.
        let raw = Buffer::from(vec![0b1111_0101_u8]);
        let array: ArrayRef = Arc::new(BooleanArray::new(BooleanBuffer::new(raw, 0, 4), None));
        let transport = TableTransport::new(&batch(vec![("b", array)])).unwrap();
        assert_eq!(bytes(&transport.values[0]), Some(vec![0b0101]));
        assert!(matches!(transport.values[0], Some(Bytes::Owned(_))));

        let clean: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true, false]));
        let transport = TableTransport::new(&batch(vec![("b", clean)])).unwrap();
        assert_eq!(bytes(&transport.values[0]), Some(vec![0b0101]));
        assert!(matches!(transport.values[0], Some(Bytes::Shared(_))));
    }

    #[test]
    fn validity_uses_the_same_canonical_bitmap_and_is_omitted_without_nulls() {
        let validity = NullBuffer::new(BooleanBuffer::new(
            Buffer::from(vec![0xFF_u8, 0b1101]),
            0,
            12,
        ));
        let array: ArrayRef = Arc::new(Int8Array::new((0..12).collect(), Some(validity)));
        let sliced = batch(vec![("n", array)]).slice(6, 6);
        let transport = TableTransport::new(&sliced).unwrap();
        // bits 6..12 of 0xFF, 0b1101 -> 1, 1, 1, 0, 1, 1
        assert_eq!(bytes(&transport.validity[0]), Some(vec![0b11_0111]));
        assert_eq!(transport.null_counts, [1]);

        let dense: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let transport = TableTransport::new(&batch(vec![("d", dense)])).unwrap();
        assert_eq!(transport.null_counts, [0]);
        assert!(transport.validity[0].is_none());

        let all_valid_buffer =
            NullBuffer::new(BooleanBuffer::new(Buffer::from(vec![0b111_u8]), 0, 3));
        let explicit: ArrayRef =
            Arc::new(Int8Array::new(vec![1, 2, 3].into(), Some(all_valid_buffer)));
        let transport = TableTransport::new(&batch(vec![("e", explicit)])).unwrap();
        assert!(
            transport.validity[0].is_none(),
            "nullCount 0 sends no bitmap"
        );
    }

    #[test]
    fn variable_width_offsets_are_rebased_and_data_trimmed() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("be"),
            None,
            Some(""),
            Some("gamma"),
        ]));
        let unsliced = TableTransport::new(&batch(vec![("s", Arc::clone(&strings))])).unwrap();
        assert_eq!(i32s(&unsliced.offsets[0]), [0, 5, 7, 7, 7, 12]);
        assert!(matches!(unsliced.offsets[0], Some(Bytes::Shared(_))));
        assert_eq!(bytes(&unsliced.values[0]), Some(b"alphabegamma".to_vec()));

        let sliced = TableTransport::new(&batch(vec![("s", strings)]).slice(1, 3)).unwrap();
        assert_eq!(i32s(&sliced.offsets[0]), [0, 2, 2, 2]);
        assert!(matches!(sliced.offsets[0], Some(Bytes::Owned(_))));
        assert_eq!(bytes(&sliced.values[0]), Some(b"be".to_vec()));
        assert_eq!(sliced.null_counts, [1]);
        assert_eq!(bytes(&sliced.validity[0]), Some(vec![0b101]));

        let large: ArrayRef = Arc::new(LargeStringArray::from(vec!["x", "yz", "w"]));
        let sliced = TableTransport::new(&batch(vec![("l", large)]).slice(1, 2)).unwrap();
        assert_eq!(sliced.types, [TypeCode::LargeUtf8]);
        assert_eq!(i64s(&sliced.offsets[0]), [0, 2, 3]);
        assert_eq!(bytes(&sliced.values[0]), Some(b"yzw".to_vec()));

        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![&b"\x00\x01"[..], b"", b"\xff"]));
        let transport = TableTransport::new(&batch(vec![("b", binary)]).slice(2, 1)).unwrap();
        assert_eq!(i32s(&transport.offsets[0]), [0, 1]);
        assert_eq!(bytes(&transport.values[0]), Some(vec![0xff]));
    }

    #[test]
    fn empty_columns_keep_their_shape() {
        let transport = TableTransport::new(
            &batch(vec![
                ("s", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
                ("i", Arc::new(Int32Array::from(vec![1]))),
                ("n", Arc::new(NullArray::new(1))),
            ])
            .slice(1, 0),
        )
        .unwrap();
        assert_eq!(transport.num_rows, 0);
        assert_eq!(i32s(&transport.offsets[0]), [0]);
        assert_eq!(bytes(&transport.values[0]), Some(Vec::new()));
        assert_eq!(bytes(&transport.values[1]), Some(Vec::new()));
        assert_eq!(transport.null_counts, [0, 0, 0]);
        assert!(transport.validity.iter().all(Option::is_none));
    }

    #[test]
    fn null_columns_report_every_row_null() {
        let transport = TableTransport::new(&batch(vec![(
            "n",
            Arc::new(NullArray::new(10)) as ArrayRef,
        )]))
        .unwrap();
        assert_eq!(transport.types, [TypeCode::Null]);
        assert!(transport.values[0].is_none() && transport.offsets[0].is_none());
        assert_eq!(transport.null_counts, [10]);
        assert_eq!(bytes(&transport.validity[0]), Some(vec![0, 0]));
    }

    #[test]
    fn timestamps_keep_their_zone_and_metadata_is_sorted_and_decoded() {
        let ts: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000), None])
                .with_timezone("America/New_York"),
        );
        let naive: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(1), Some(2)]));
        let date: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_000), Some(-1)]));
        let base = batch(vec![("ts", ts), ("naive", naive), ("date", date)]);
        let schema = base.schema().as_ref().clone().with_metadata(HashMap::from([
            ("z.key".to_string(), "last".to_string()),
            ("xbbg.bloomberg_request_id".to_string(), "sdk-1".to_string()),
        ]));
        let batch = base.with_schema(Arc::new(schema)).unwrap();
        let transport = TableTransport::new(&batch).unwrap();
        assert_eq!(
            transport.timezones,
            [Some("America/New_York".to_string()), None, None]
        );
        assert_eq!(
            transport.types,
            [
                TypeCode::TimestampUs,
                TypeCode::TimestampUs,
                TypeCode::Date32
            ]
        );
        assert_eq!(i64s(&transport.values[0])[0], 1_700_000_000_000_000);
        assert_eq!(i32s(&transport.values[2]), [19_000, -1]);
        assert_eq!(
            transport.metadata,
            [
                ("xbbg.bloomberg_request_id".to_string(), "sdk-1".to_string()),
                ("z.key".to_string(), "last".to_string())
            ]
        );
        assert_eq!(
            transport.decoded_metadata,
            r#"{"bloombergRequestId":"sdk-1","bloombergRequestIds":["sdk-1"]}"#
        );
    }

    #[test]
    fn floats_and_unsigned_values_keep_their_exact_bits() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::from_bits(0x7ff8_0000_0000_0001),
            -0.0,
            f64::INFINITY,
        ]));
        let unsigned: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX, 0, 1]));
        let transport = TableTransport::new(&batch(vec![("f", floats), ("u", unsigned)])).unwrap();
        let float_bits: Vec<u64> = bytes(&transport.values[0])
            .unwrap()
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| u64::from_le_bytes(*chunk))
            .collect();
        assert_eq!(
            float_bits,
            [
                0x7ff8_0000_0000_0001,
                (-0.0_f64).to_bits(),
                f64::INFINITY.to_bits()
            ]
        );
        assert_eq!(
            bytes(&transport.values[1]).unwrap()[..8],
            u64::MAX.to_le_bytes()
        );
    }

    #[test]
    fn unsupported_types_list_every_offending_column() {
        let list: ArrayRef = Arc::new(arrow::array::ListArray::from_iter_primitive::<
            arrow::datatypes::Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(1)])]));
        let duration: ArrayRef = Arc::new(arrow::array::DurationSecondArray::from(vec![1]));
        let ok: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let error = TableTransport::new(&batch(vec![("ok", ok), ("l", list), ("d", duration)]))
            .unwrap_err();
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::Internal);
        assert!(
            error.message.starts_with(
                "native table transfer does not support this Arrow schema. Unsupported columns: #1 'l' (unsupported type=List("
            ),
            "{}",
            error.message
        );
        assert!(
            error.message.contains(
                "; #2 'd' (unsupported type=Duration(Second)). Supported column types are: bool,"
            ),
            "{}",
            error.message
        );
    }

    #[test]
    fn every_supported_type_has_its_contract_code() {
        let expected = [
            (DataType::Null, 0),
            (DataType::Boolean, 1),
            (DataType::Int8, 2),
            (DataType::Int16, 3),
            (DataType::Int32, 4),
            (DataType::Int64, 5),
            (DataType::UInt8, 6),
            (DataType::UInt16, 7),
            (DataType::UInt32, 8),
            (DataType::UInt64, 9),
            (DataType::Float32, 10),
            (DataType::Float64, 11),
            (DataType::Utf8, 12),
            (DataType::LargeUtf8, 13),
            (DataType::Binary, 14),
            (DataType::LargeBinary, 15),
            (DataType::Date32, 16),
            (DataType::Date64, 17),
            (DataType::Time32(TimeUnit::Second), 18),
            (DataType::Time32(TimeUnit::Millisecond), 19),
            (DataType::Time64(TimeUnit::Microsecond), 20),
            (DataType::Time64(TimeUnit::Nanosecond), 21),
            (DataType::Timestamp(TimeUnit::Second, None), 22),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                23,
            ),
            (DataType::Timestamp(TimeUnit::Microsecond, None), 24),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Tokyo".into())),
                25,
            ),
        ];
        for (data_type, code) in expected {
            let (type_code, _) = TypeCode::of(&data_type).unwrap();
            assert_eq!(type_code as i8, code, "{data_type:?}");
        }
        assert!(TypeCode::of(&DataType::Float16).is_none());
        assert!(TypeCode::of(&DataType::Time32(TimeUnit::Microsecond)).is_none());
        assert!(TypeCode::of(&DataType::Utf8View).is_none());
    }
}
