//! Arrow-backed column carrier data.

use std::sync::Arc;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ByteViewType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, DictionaryArray, GenericByteViewArray, StringViewArray,
    make_array,
};
use arrow_buffer::ArrowNativeType;
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType, FieldRef};
use arrow_select::interleave::interleave;

use crate::error::{ArrowCoreError, Result};

/// A logical Arrow column made of one or more chunks.
#[derive(Clone, Debug)]
pub struct ColumnData {
    /// Column name as exposed by the table schema.
    pub name: String,
    /// Arrow field metadata for this column.
    pub field: FieldRef,
    /// Physical array chunks for this logical column.
    pub chunks: Vec<ArrayRef>,
}

impl ColumnData {
    /// Create column data from field and chunk arrays.
    pub fn new(name: String, field: FieldRef, chunks: Vec<ArrayRef>) -> Result<Self> {
        for chunk in &chunks {
            if chunk.data_type() != field.data_type() {
                return Err(ArrowCoreError::Arrow(
                    arrow_schema::ArrowError::SchemaError(format!(
                        "column {name} chunk type {} does not match field type {}",
                        chunk.data_type(),
                        field.data_type()
                    )),
                ));
            }
        }
        Ok(Self {
            name,
            field,
            chunks,
        })
    }

    /// Number of logical values across all chunks.
    pub fn len(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.len()).sum()
    }

    /// Whether the column contains no logical values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total physical null count across all chunks.
    pub fn null_count(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.null_count()).sum()
    }

    /// Approximate bytes referenced by this column's Arrow buffers.
    pub fn nbytes(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| chunk.get_buffer_memory_size())
            .sum()
    }

    /// Bytes a right-sized copy of this column's logical slice would occupy,
    /// computed without copying.
    ///
    /// Unlike [`Self::nbytes`], which reports the full backing buffers a
    /// zero-copy slice still references, this counts only the sliced values:
    ///
    /// - fixed-width types: element width x length;
    /// - `Utf8`/`Binary` and their `Large` forms: the offsets for the slice plus
    ///   the value bytes between its first and last offset;
    /// - `Utf8View`/`BinaryView`: 16 bytes per view plus the out-of-line bytes
    ///   of each non-null value longer than the 12 bytes a view stores inline;
    /// - validity bitmaps are included (one bit per row, rounded up to bytes).
    ///
    /// Nested and dictionary types follow arrow-data's
    /// `ArrayData::get_slice_memory_size`, which counts list children and
    /// dictionary values in full, so their figure is an upper bound.
    pub fn logical_nbytes(&self) -> Result<usize> {
        self.chunks.iter().try_fold(0usize, |total, chunk| {
            Ok(total + array_logical_nbytes(chunk.as_ref())?)
        })
    }

    /// Return the chunk and local index for a logical row index.
    pub fn chunk_for_index(&self, index: usize) -> Result<(&ArrayRef, usize)> {
        let mut offset = 0;
        for chunk in &self.chunks {
            let end = offset + chunk.len();
            if index < end {
                return Ok((chunk, index - offset));
            }
            offset = end;
        }
        Err(ArrowCoreError::RowIndexOutOfRange)
    }

    /// Return a zero-copy slice of this logical column.
    pub fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        let total = self.len();
        if offset >= total {
            return Self {
                name: self.name.clone(),
                field: self.field.clone(),
                chunks: Vec::new(),
            };
        }

        let mut remaining = length.unwrap_or(total - offset).min(total - offset);
        let mut skipped = 0;
        let mut chunks = Vec::new();
        for chunk in &self.chunks {
            if remaining == 0 {
                break;
            }
            let chunk_len = chunk.len();
            if skipped + chunk_len <= offset {
                skipped += chunk_len;
                continue;
            }
            let local_offset = offset.saturating_sub(skipped);
            let take = remaining.min(chunk_len - local_offset);
            chunks.push(chunk.slice(local_offset, take));
            remaining -= take;
            skipped += chunk_len;
        }

        Self {
            name: self.name.clone(),
            field: self.field.clone(),
            chunks,
        }
    }

    /// Copy logical values into independent, right-sized Arrow buffers.
    ///
    /// Physical chunk boundaries are preserved.
    pub fn compact(&self) -> Result<Self> {
        let chunks = self
            .chunks
            .iter()
            .map(|chunk| compact_array(chunk.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Self::new(self.name.clone(), self.field.clone(), chunks)
    }
}

fn array_logical_nbytes(array: &dyn Array) -> Result<usize> {
    match array.data_type() {
        DataType::Utf8View => Ok(view_logical_nbytes(array.as_string_view())),
        DataType::BinaryView => Ok(view_logical_nbytes(array.as_binary_view())),
        _ => Ok(array.to_data().get_slice_memory_size()?),
    }
}

/// Longest value a byte view stores inline, without a data buffer reference.
const MAX_INLINE_VIEW_LEN: usize = 12;

fn view_logical_nbytes<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> usize {
    let views = array.views();
    let validity = array.nulls().map_or(0, |nulls| nulls.len().div_ceil(8));
    let out_of_line = views
        .iter()
        .enumerate()
        .filter(|(row, _)| array.is_valid(*row))
        // The low 32 bits of a view hold the value length.
        .map(|(_, view)| *view as u32 as usize)
        .filter(|len| *len > MAX_INLINE_VIEW_LEN)
        .sum::<usize>();
    views.len() * std::mem::size_of::<u128>() + validity + out_of_line
}

pub(crate) fn compact_array(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int8Type>>()
                    .expect("Int8 dictionary"),
            ),
            DataType::Int16 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int16Type>>()
                    .expect("Int16 dictionary"),
            ),
            DataType::Int32 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Int32 dictionary"),
            ),
            DataType::Int64 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int64Type>>()
                    .expect("Int64 dictionary"),
            ),
            DataType::UInt8 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt8Type>>()
                    .expect("UInt8 dictionary"),
            ),
            DataType::UInt16 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                    .expect("UInt16 dictionary"),
            ),
            DataType::UInt32 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .expect("UInt32 dictionary"),
            ),
            DataType::UInt64 => compact_dictionary(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt64Type>>()
                    .expect("UInt64 dictionary"),
            ),
            _ => unreachable!("Arrow dictionary key types are integer primitives"),
        },
        _ => {
            let indices = (0..array.len()).map(|row| (0, row)).collect::<Vec<_>>();
            let copied = interleave(&[array], &indices)?;
            compact_owned_array(copied)
        }
    }
}

fn compact_dictionary<K: ArrowDictionaryKeyType>(array: &DictionaryArray<K>) -> Result<ArrayRef> {
    let mut remapped = vec![None; array.values().len()];
    let mut selected = Vec::new();
    let mut keys = PrimitiveBuilder::<K>::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            keys.append_null();
            continue;
        }
        let old_key = array.keys().value(row).as_usize();
        let new_key = match remapped[old_key] {
            Some(key) => key,
            None => {
                let key = K::Native::from_usize(selected.len())
                    .expect("selected dictionary values must fit the original key type");
                remapped[old_key] = Some(key);
                selected.push((0, old_key));
                key
            }
        };
        keys.append_value(new_key);
    }
    let selected_values = interleave(&[array.values().as_ref()], &selected)?;
    let values = compact_array(selected_values.as_ref())?;
    let mut compacted: ArrayRef = Arc::new(DictionaryArray::try_new(keys.finish(), values)?);
    Arc::get_mut(&mut compacted)
        .expect("freshly rebuilt dictionary must be uniquely owned")
        .shrink_to_fit();
    Ok(compacted)
}
fn compact_owned_array(array: ArrayRef) -> Result<ArrayRef> {
    let array: ArrayRef = match array.data_type() {
        DataType::Utf8View => Arc::new(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("StringViewArray")
                .gc(),
        ),
        DataType::BinaryView => Arc::new(
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .expect("BinaryViewArray")
                .gc(),
        ),
        _ => array,
    };
    let data = array.to_data();
    drop(array);
    let (data_type, len, nulls, offset, buffers, child_data) = data.into_parts();
    let child_data = child_data
        .into_iter()
        .map(|child| {
            let child = make_array(child);
            let compacted = compact_array(child.as_ref())?;
            Ok(compacted.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let data = ArrayDataBuilder::new(data_type)
        .len(len)
        .nulls(nulls)
        .offset(offset)
        .buffers(buffers)
        .child_data(child_data)
        .build()?;
    let mut array = make_array(data);
    Arc::get_mut(&mut array)
        .expect("freshly rebuilt compact array must be uniquely owned")
        .shrink_to_fit();
    Ok(array)
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, StringArray};
    use arrow_schema::Field;

    use super::*;

    const ROWS: usize = 1_000;
    const CELL_LEN: usize = 100;

    fn column(array: ArrayRef) -> ColumnData {
        let field = Arc::new(Field::new("payload", array.data_type().clone(), true));
        ColumnData::new("payload".into(), field, vec![array]).unwrap()
    }

    fn cells() -> Vec<String> {
        (0..ROWS)
            .map(|row| format!("{row:04}").repeat(CELL_LEN / 4))
            .collect()
    }

    fn one_cell_slice(array: ArrayRef) -> (ColumnData, ColumnData) {
        let parent = column(array);
        let slice = parent.slice(ROWS / 2, Some(1));
        (parent, slice)
    }

    #[test]
    fn offset_types_count_only_the_sliced_offsets_and_value_bytes() {
        let values = cells();
        let refs = values.iter().map(String::as_str).collect::<Vec<_>>();
        let bytes = values.iter().map(String::as_bytes).collect::<Vec<_>>();
        for (array, offset_width) in [
            (Arc::new(StringArray::from(refs.clone())) as ArrayRef, 4),
            (
                Arc::new(LargeStringArray::from(refs.clone())) as ArrayRef,
                8,
            ),
            (Arc::new(BinaryArray::from(bytes.clone())) as ArrayRef, 4),
            (
                Arc::new(LargeBinaryArray::from(bytes.clone())) as ArrayRef,
                8,
            ),
        ] {
            let data_type = array.data_type().clone();
            let (parent, slice) = one_cell_slice(array);
            assert_eq!(
                slice.logical_nbytes().unwrap(),
                2 * offset_width + CELL_LEN,
                "{data_type}"
            );
            assert!(slice.nbytes() >= ROWS * CELL_LEN, "{data_type}");
            assert_eq!(
                parent.logical_nbytes().unwrap(),
                (ROWS + 1) * offset_width + ROWS * CELL_LEN,
                "{data_type}"
            );
        }
    }

    #[test]
    fn view_types_sum_out_of_line_value_lengths() {
        let values = cells();
        let utf8_view: ArrayRef = Arc::new(StringViewArray::from_iter_values(values.iter()));
        let binary_view: ArrayRef = Arc::new(BinaryViewArray::from_iter_values(
            values.iter().map(String::as_bytes),
        ));
        for array in [utf8_view, binary_view] {
            let data_type = array.data_type().clone();
            let (parent, slice) = one_cell_slice(array);
            assert_eq!(
                slice.logical_nbytes().unwrap(),
                16 + CELL_LEN,
                "{data_type}"
            );
            assert!(slice.nbytes() >= ROWS * CELL_LEN, "{data_type}");
            assert_eq!(
                parent.logical_nbytes().unwrap(),
                ROWS * (16 + CELL_LEN),
                "{data_type}"
            );
        }

        let short: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("inline"),
            None,
            Some("exactly12byt"),
            Some("thirteen byte"),
        ]));
        let short = column(short);
        // Four views, a one-byte validity bitmap, and only the 13-byte value out of line.
        assert_eq!(short.logical_nbytes().unwrap(), 4 * 16 + 1 + 13);
        assert_eq!(short.slice(1, Some(1)).logical_nbytes().unwrap(), 16 + 1);
    }

    #[test]
    fn fixed_width_types_count_element_width_times_length_across_chunks() {
        let field = Arc::new(Field::new("value", DataType::Int64, true));
        let first: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
        let second: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let column = ColumnData::new("value".into(), field, vec![first, second]).unwrap();
        assert_eq!(column.logical_nbytes().unwrap(), ROWS * 8 + 3 * 8 + 1);
        assert_eq!(column.slice(10, Some(5)).logical_nbytes().unwrap(), 5 * 8);
        assert_eq!(
            column.slice(ROWS + 1, None).logical_nbytes().unwrap(),
            2 * 8 + 1
        );
    }
}
