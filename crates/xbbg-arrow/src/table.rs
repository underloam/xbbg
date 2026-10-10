//! Arrow table carrier data and pure table operations.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, StringArray};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow_select::concat::{concat, concat_batches};
use arrow_select::filter::filter_record_batch;
use arrow_select::interleave::interleave;
use arrow_select::take::take_record_batch;

use crate::column::{ColumnData, compact_array};
use crate::error::{ArrowCoreError, Result};
use crate::scalar::{CellValue, InferredKind, build_array_for_kind, cell_matches, infer_kind};

/// Sort direction for Arrow table sorting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl SortDirection {
    /// Whether this direction is descending for arrow-rs sort options.
    pub fn is_descending(self) -> bool {
        matches!(self, Self::Descending)
    }
}

impl FromStr for SortDirection {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "asc" | "ascending" => Ok(Self::Ascending),
            "desc" | "descending" => Ok(Self::Descending),
            _ => Err(()),
        }
    }
}

/// A logical Arrow table made of schema-compatible record batches.
#[derive(Clone, Debug)]
pub struct TableData {
    /// Physical Arrow record batches.
    pub batches: Vec<RecordBatch>,
    /// Shared logical table schema.
    pub schema: SchemaRef,
}

impl TableData {
    /// Create a table from record batches that all share one schema.
    pub fn try_new(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = table_schema(&batches);
        for batch in &batches {
            ensure_compatible_schema(&schema, batch)?;
        }
        Ok(Self { batches, schema })
    }

    /// Create an empty table with UTF-8 columns by name.
    pub fn empty_from_columns(columns: Vec<String>) -> Result<Self> {
        let fields = columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|_| Arc::new(StringArray::from(Vec::<Option<String>>::new())) as ArrayRef)
            .collect::<Vec<_>>();
        let options = RecordBatchOptions::new().with_row_count(Some(0));
        let batch =
            RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)?;
        Self::try_new(vec![batch])
    }

    /// Concatenate logical tables by appending their batches.
    pub fn concat_tables(tables: &[Self]) -> Result<Self> {
        let mut batches = Vec::new();
        let mut expected_schema: Option<SchemaRef> = None;
        for table in tables {
            if let Some(schema) = &expected_schema {
                if table.schema != *schema {
                    return Err(ArrowCoreError::IncompatibleTableSchemas);
                }
            } else {
                expected_schema = Some(table.schema.clone());
            }
            batches.extend(table.batches.iter().cloned());
        }
        Self::try_new(batches)
    }

    /// Total number of rows.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Total number of columns.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// `(num_rows, num_columns)`.
    pub fn shape(&self) -> (usize, usize) {
        (self.num_rows(), self.num_columns())
    }

    /// Approximate bytes referenced by all Arrow column buffers.
    pub fn nbytes(&self) -> usize {
        self.batches
            .iter()
            .flat_map(|batch| batch.columns())
            .map(|array| array.get_buffer_memory_size())
            .sum()
    }

    /// Row count for each physical chunk/batch.
    pub fn chunk_lengths(&self) -> Vec<usize> {
        self.batches.iter().map(RecordBatch::num_rows).collect()
    }

    /// Column names in schema order.
    pub fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    /// Find a column index by name.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.schema
            .index_of(name)
            .map_err(|_| ArrowCoreError::UnknownColumn(name.to_string()))
    }

    /// Field for a column index.
    pub fn field(&self, index: usize) -> Result<FieldRef> {
        self.schema
            .fields()
            .get(index)
            .cloned()
            .ok_or(ArrowCoreError::ColumnIndexOutOfRange)
    }

    /// Extract a logical column by index.
    pub fn column_by_index(&self, index: usize) -> Result<ColumnData> {
        let field = self.field(index)?;
        let name = field.name().clone();
        let chunks = self
            .batches
            .iter()
            .map(|batch| batch.column(index).clone())
            .collect::<Vec<_>>();
        ColumnData::new(name, field, chunks)
    }

    /// Select columns by name.
    pub fn select_names(&self, names: &[String]) -> Result<Self> {
        let indices = names
            .iter()
            .map(|name| self.column_index(name))
            .collect::<Result<Vec<_>>>()?;
        self.select_indices(&indices)
    }

    /// Select columns by index.
    pub fn select_indices(&self, indices: &[usize]) -> Result<Self> {
        let projected = self
            .batches
            .iter()
            .map(|batch| select_batch(batch, indices))
            .collect::<Result<Vec<_>>>()?;
        Self::try_new(projected)
    }

    /// Drop named columns; unknown names are ignored to preserve existing xbbg behavior.
    pub fn drop_columns(&self, names: &[String]) -> Result<Self> {
        let drop = names.iter().collect::<HashSet<_>>();
        let keep = self
            .column_names()
            .into_iter()
            .filter(|name| !drop.contains(name))
            .collect::<Vec<_>>();
        self.select_names(&keep)
    }

    /// Rename columns by mapping existing names to new names.
    pub fn rename_columns(&self, mapping: &HashMap<String, String>) -> Result<Self> {
        let renamed = self
            .batches
            .iter()
            .map(|batch| rename_batch(batch, mapping))
            .collect::<Result<Vec<_>>>()?;
        Self::try_new(renamed)
    }

    /// Return a zero-copy row slice across physical batches.
    pub fn slice(&self, offset: usize, length: Option<usize>) -> Result<Self> {
        let total = self.num_rows();
        if offset >= total || length == Some(0) {
            return Self::try_new(vec![RecordBatch::new_empty(self.schema.clone())]);
        }

        let mut remaining = length.unwrap_or(total - offset).min(total - offset);
        let mut skipped = 0;
        let mut out = Vec::new();
        for batch in &self.batches {
            if remaining == 0 {
                break;
            }
            let batch_rows = batch.num_rows();
            if skipped + batch_rows <= offset {
                skipped += batch_rows;
                continue;
            }
            let local_offset = offset.saturating_sub(skipped);
            let take = remaining.min(batch_rows - local_offset);
            out.push(batch.slice(local_offset, take));
            remaining -= take;
            skipped += batch_rows;
        }
        Self::try_new(out)
    }

    /// Return the first `n` rows.
    pub fn head(&self, n: usize) -> Result<Self> {
        self.slice(0, Some(n))
    }

    /// Return the last `n` rows.
    pub fn tail(&self, n: usize) -> Result<Self> {
        let rows = self.num_rows();
        if n >= rows {
            return Ok(self.clone());
        }
        self.slice(rows - n, Some(n))
    }
    /// Copy logical values into independent, right-sized Arrow buffers.
    ///
    /// Unlike [`Self::slice`], this deliberately does not retain the source
    /// arrays' backing allocations. Physical batch boundaries are preserved.
    pub fn compact(&self) -> Result<Self> {
        let mut batches = Vec::with_capacity(self.batches.len());
        for batch in &self.batches {
            let columns = batch
                .columns()
                .iter()
                .map(|column| compact_array(column.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            batches.push(RecordBatch::try_new_with_options(
                batch.schema(),
                columns,
                &options,
            )?);
        }
        Self::try_new(batches)
    }

    /// Materialize batches as one batch when multiple chunks are present.
    pub fn combined_batch(&self) -> Result<RecordBatch> {
        if self.batches.len() == 1 {
            return Ok(self.batches[0].clone());
        }
        concat_batches(&self.schema, self.batches.iter()).map_err(Into::into)
    }

    /// Sort rows by named columns with explicit null placement.
    pub fn sort_by(
        &self,
        sort_keys: &[(String, SortDirection)],
        nulls_first: bool,
    ) -> Result<Self> {
        if sort_keys.is_empty() || self.num_rows() == 0 {
            return Ok(self.clone());
        }

        if self.batches.len() == 1 {
            let batch = &self.batches[0];
            let columns = sort_columns_for_batch(batch, sort_keys, nulls_first)?;
            let indices = lexsort_to_indices(&columns, None)?;
            let sorted = take_record_batch(batch, &indices)?;
            return Self::try_new(vec![sorted]);
        }

        let columns = sort_keys
            .iter()
            .map(|(name, direction)| {
                let idx = self
                    .schema
                    .index_of(name)
                    .map_err(|_| ArrowCoreError::UnknownColumn(name.clone()))?;
                let chunks = self
                    .batches
                    .iter()
                    .map(|batch| batch.column(idx).as_ref())
                    .collect::<Vec<&dyn Array>>();
                Ok(SortColumn {
                    values: concat(&chunks)?,
                    options: Some(SortOptions {
                        descending: direction.is_descending(),
                        nulls_first,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = lexsort_to_indices(&columns, None)?;

        let batch_ends = self
            .batches
            .iter()
            .scan(0usize, |end, batch| {
                *end += batch.num_rows();
                Some(*end)
            })
            .collect::<Vec<_>>();
        let gather_indices = indices
            .values()
            .iter()
            .map(|global| {
                let global = usize::try_from(*global).expect("UInt32 sort index must fit in usize");
                let batch_idx = batch_ends.partition_point(|end| *end <= global);
                let batch_start = batch_idx
                    .checked_sub(1)
                    .map(|previous| batch_ends[previous])
                    .unwrap_or(0);
                (batch_idx, global - batch_start)
            })
            .collect::<Vec<_>>();
        let sorted =
            interleave_record_batches(&self.batches, self.schema.clone(), &gather_indices)?;
        Self::try_new(vec![sorted])
    }

    /// Filter rows where a column equals a scalar value.
    pub fn filter_eq(&self, column: &str, value: &CellValue) -> Result<Self> {
        let idx = self.column_index(column)?;
        let filtered = self
            .batches
            .iter()
            .map(|batch| {
                let mask = BooleanArray::from(
                    (0..batch.num_rows())
                        .map(|row| cell_matches(batch.column(idx).as_ref(), row, value))
                        .collect::<Vec<_>>(),
                );
                filter_record_batch(batch, &mask).map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::try_new(filtered)
    }

    /// Insert a scalar-built column at `index`.
    pub fn add_column(&self, index: usize, name: &str, cells: &[CellValue]) -> Result<Self> {
        self.with_cells_column(index, name, cells, false, false)
    }

    /// Replace a scalar-built column at `index`.
    pub fn set_column(&self, index: usize, name: &str, cells: &[CellValue]) -> Result<Self> {
        self.with_cells_column(index, name, cells, true, false)
    }

    /// Insert or replace a column, optionally forcing UTF-8 output.
    pub fn with_cells_column(
        &self,
        index: usize,
        name: &str,
        cells: &[CellValue],
        replace: bool,
        force_text: bool,
    ) -> Result<Self> {
        let max_index = self.num_columns();
        if replace {
            if index >= max_index {
                return Err(ArrowCoreError::ColumnIndexOutOfRange);
            }
        } else if index > max_index {
            return Err(ArrowCoreError::ColumnIndexOutOfRange);
        }

        let chunks = split_cells_for_batches(cells, &self.batches)?;
        let kind = if force_text {
            InferredKind::Text
        } else {
            infer_kind(cells)
        };
        let mut out = Vec::with_capacity(self.batches.len());
        for (batch, chunk) in self.batches.iter().zip(chunks) {
            let (field, array) = build_array_for_kind(name, chunk, kind);
            let mut fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            let mut columns = batch.columns().to_vec();
            if replace {
                fields[index] = field;
                columns[index] = array;
            } else {
                fields.insert(index, field);
                columns.insert(index, array);
            }
            out.push(RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    fields,
                    batch.schema().metadata().clone(),
                )),
                columns,
            )?);
        }
        Self::try_new(out)
    }
}

fn sort_columns_for_batch(
    batch: &RecordBatch,
    sort_keys: &[(String, SortDirection)],
    nulls_first: bool,
) -> Result<Vec<SortColumn>> {
    let schema = batch.schema();
    sort_keys
        .iter()
        .map(|(name, direction)| {
            let idx = schema
                .index_of(name)
                .map_err(|_| ArrowCoreError::UnknownColumn(name.clone()))?;
            Ok(SortColumn {
                values: batch.column(idx).clone(),
                options: Some(SortOptions {
                    descending: direction.is_descending(),
                    nulls_first,
                }),
            })
        })
        .collect()
}

fn interleave_record_batches(
    batches: &[RecordBatch],
    schema: SchemaRef,
    indices: &[(usize, usize)],
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let columns = (0..schema.fields().len())
        .map(|column_idx| {
            let chunks = batches
                .iter()
                .map(|batch| batch.column(column_idx).as_ref())
                .collect::<Vec<&dyn Array>>();
            interleave(&chunks, indices).map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(Into::into)
}

fn table_schema(batches: &[RecordBatch]) -> SchemaRef {
    batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()))
}

fn ensure_compatible_schema(expected: &SchemaRef, batch: &RecordBatch) -> Result<()> {
    if batch.schema_ref() != expected {
        return Err(ArrowCoreError::IncompatibleSchemas);
    }
    Ok(())
}

fn select_batch(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(indices.len());
    let mut columns = Vec::with_capacity(indices.len());
    for &idx in indices {
        let field = schema
            .fields()
            .get(idx)
            .cloned()
            .ok_or(ArrowCoreError::ColumnIndexOutOfRange)?;
        fields.push(field.as_ref().clone());
        columns.push(batch.column(idx).clone());
    }
    let projected_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(projected_schema, columns, &options).map_err(Into::into)
}

fn rename_batch(batch: &RecordBatch, mapping: &HashMap<String, String>) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            mapping
                .get(field.name().as_str())
                .map(|new_name| field.as_ref().clone().with_name(new_name))
                .unwrap_or_else(|| field.as_ref().clone())
        })
        .collect::<Vec<_>>();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batch.columns().to_vec(),
        &options,
    )
    .map_err(Into::into)
}

/// Split one logical column's cells across the table's physical batches.
pub fn split_cells_for_batches<'a>(
    cells: &'a [CellValue],
    batches: &[RecordBatch],
) -> Result<Vec<&'a [CellValue]>> {
    let expected = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    if cells.len() != expected {
        return Err(ArrowCoreError::ColumnLengthMismatch {
            actual: cells.len(),
            expected,
        });
    }
    let mut offset = 0;
    Ok(batches
        .iter()
        .map(|batch| {
            let end = offset + batch.num_rows();
            let chunk = &cells[offset..end];
            offset = end;
            chunk
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray};

    use super::*;

    fn sample_table() -> TableData {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("px_last", DataType::Float64, true),
            Field::new("volume", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["MSFT", "AAPL", "IBM"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![380.0, 150.0, 190.0])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2_i64, 3, 1])) as ArrayRef,
            ],
        )
        .unwrap();
        TableData::try_new(vec![batch]).unwrap()
    }

    #[test]
    fn selects_drops_and_renames_columns() {
        let table = sample_table();
        let selected = table
            .select_names(&["ticker".to_string(), "volume".to_string()])
            .unwrap();
        assert_eq!(selected.column_names(), ["ticker", "volume"]);

        let dropped = table.drop_columns(&["volume".to_string()]).unwrap();
        assert_eq!(dropped.column_names(), ["ticker", "px_last"]);

        let renamed = table
            .rename_columns(&HashMap::from([(
                "px_last".to_string(),
                "last".to_string(),
            )]))
            .unwrap();
        assert_eq!(renamed.column_names(), ["ticker", "last", "volume"]);
    }

    #[test]
    fn slices_head_and_tail_across_batches() {
        let table = sample_table();
        assert_eq!(table.slice(1, Some(1)).unwrap().num_rows(), 1);
        assert_eq!(table.head(2).unwrap().num_rows(), 2);
        assert_eq!(table.tail(2).unwrap().num_rows(), 2);
        assert_eq!(table.slice(99, None).unwrap().chunk_lengths(), [0]);
    }

    #[test]
    fn extracts_column_chunks_without_materializing_python_values() {
        let table = sample_table();
        let column = table
            .column_by_index(table.column_index("ticker").unwrap())
            .unwrap();
        assert_eq!(column.name, "ticker");
        assert_eq!(column.len(), 3);
        assert_eq!(column.null_count(), 0);
        assert_eq!(column.chunk_for_index(2).unwrap().1, 2);
    }

    #[test]
    fn filters_and_sorts_rows() {
        let table = sample_table();
        let filtered = table
            .filter_eq("ticker", &CellValue::Text("AAPL".to_string()))
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(
            table
                .filter_eq("px_last", &CellValue::Int(190))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            table
                .filter_eq("volume", &CellValue::Float(2.0))
                .unwrap()
                .num_rows(),
            1
        );

        let sorted = table
            .sort_by(&[("volume".to_string(), SortDirection::Ascending)], false)
            .unwrap();
        let column = sorted
            .column_by_index(sorted.column_index("ticker").unwrap())
            .unwrap();
        let (chunk, _) = column.chunk_for_index(0).unwrap();
        let values = chunk.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "IBM");
    }

    #[test]
    fn float_filter_does_not_round_large_integer_needles() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![9_007_199_254_740_992.0])) as ArrayRef],
        )
        .unwrap();
        let table = TableData::try_new(vec![batch]).unwrap();

        assert_eq!(
            table
                .filter_eq("value", &CellValue::Int(9_007_199_254_740_992))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            table
                .filter_eq("value", &CellValue::Int(9_007_199_254_740_993))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn adds_and_sets_cell_columns() {
        let table = sample_table();
        let added = table
            .add_column(
                1,
                "side",
                &[
                    CellValue::Text("B".to_string()),
                    CellValue::Text("A".to_string()),
                    CellValue::Text("I".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            added.column_names(),
            ["ticker", "side", "px_last", "volume"]
        );

        let replaced = added
            .set_column(
                1,
                "side2",
                &[
                    CellValue::Text("buy".to_string()),
                    CellValue::Text("ask".to_string()),
                    CellValue::Text("indic".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            replaced.column_names(),
            ["ticker", "side2", "px_last", "volume"]
        );
    }
    #[test]
    fn fragmented_sort_preserves_schema_nulls_and_row_order() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("key", DataType::Int64, true)
                    .with_metadata(HashMap::from([("unit".to_string(), "rank".to_string())])),
                Field::new("payload", DataType::Utf8, true),
            ],
            HashMap::from([("source".to_string(), "test".to_string())]),
        ));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(2_i64), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("second"), None])) as ArrayRef,
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1_i64), Some(3)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("first"), Some("third")])) as ArrayRef,
            ],
        )
        .unwrap();
        let empty = RecordBatch::new_empty(schema.clone());

        let table = TableData::try_new(vec![empty.clone(), first, empty, second]).unwrap();
        let sorted = table
            .sort_by(&[("key".to_string(), SortDirection::Ascending)], false)
            .unwrap();

        assert_eq!(sorted.schema.as_ref(), schema.as_ref());
        assert_eq!(sorted.chunk_lengths(), [4]);
        let keys = sorted.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            keys.iter().collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3), None]
        );
        let payload = sorted.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            payload.iter().collect::<Vec<_>>(),
            [Some("first"), Some("second"), Some("third"), None]
        );

        let nulls_first = table
            .sort_by(&[("key".to_string(), SortDirection::Ascending)], true)
            .unwrap();
        let keys = nulls_first.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            keys.iter().collect::<Vec<_>>(),
            [None, Some(1), Some(2), Some(3)]
        );
        let payload = nulls_first.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            payload.iter().collect::<Vec<_>>(),
            [None, Some("first"), Some("second"), Some("third")]
        );
    }

    #[test]
    fn fragmented_cell_columns_infer_one_logical_dtype() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("row", DataType::Int64, false)],
            HashMap::from([("source".to_string(), "fragmented".to_string())]),
        ));
        let batches = [0_i64, 1]
            .into_iter()
            .map(|row| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![row])) as ArrayRef],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let table = TableData::try_new(batches).unwrap();

        let integers = table
            .add_column(1, "value", &[CellValue::Int(7), CellValue::Null])
            .unwrap();
        assert_eq!(integers.schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(
            integers.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(7)]
        );
        assert!(integers.batches[1].column(1).is_null(0));
        assert_eq!(
            integers.schema.metadata().get("source").map(String::as_str),
            Some("fragmented")
        );

        let text = table
            .add_column(
                1,
                "mixed",
                &[CellValue::Int(7), CellValue::Text("eight".to_string())],
            )
            .unwrap();
        assert_eq!(text.schema.field(1).data_type(), &DataType::Utf8);
        let values = text
            .batches
            .iter()
            .map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, ["7", "eight"]);
    }

    #[test]
    fn mixed_numeric_inference_preserves_integer_exactness() {
        let table = sample_table().slice(0, Some(2)).unwrap();
        let exact = table
            .add_column(
                1,
                "mixed",
                &[CellValue::Int(9_007_199_254_740_992), CellValue::Float(0.5)],
            )
            .unwrap();
        assert_eq!(exact.schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(
            exact.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            9_007_199_254_740_992.0
        );

        for inexact in [9_007_199_254_740_993, i64::MAX] {
            let lossless = table
                .add_column(
                    1,
                    "mixed",
                    &[CellValue::Int(inexact), CellValue::Float(0.5)],
                )
                .unwrap();
            assert_eq!(lossless.schema.field(1).data_type(), &DataType::Utf8);
            let values = lossless.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), inexact.to_string());
            assert_eq!(values.value(1), "0.5");
        }
    }

    #[test]
    fn zero_column_construction_and_rename_preserve_row_counts() {
        let empty = TableData::empty_from_columns(vec![]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), 0);

        let options = RecordBatchOptions::new().with_row_count(Some(2));
        let batch =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
        let table = TableData::try_new(vec![batch]).unwrap();
        let renamed = table.rename_columns(&HashMap::new()).unwrap();
        assert_eq!(renamed.num_rows(), 2);
        assert_eq!(renamed.num_columns(), 0);
    }

    #[test]
    fn compact_rebuilds_dictionary_values_referenced_by_a_slice() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for index in 0..256 {
            builder.append_value(format!("{index:04}-{}", "x".repeat(512)));
        }
        let array = builder.finish();
        let original_dictionary_values = Arc::downgrade(array.values());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap();
        let retained = TableData::try_new(vec![batch])
            .unwrap()
            .slice(128, Some(2))
            .unwrap();

        let retained_bytes = retained.nbytes();
        let compact = retained.compact().unwrap();
        drop(retained);
        assert!(original_dictionary_values.upgrade().is_none());
        assert!(compact.nbytes() < retained_bytes);
        let dictionary = compact.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.values().len(), 2);
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(values.value(0).starts_with("0128-"));
        assert!(values.value(1).starts_with("0129-"));
    }

    #[test]
    fn compact_copies_slices_and_preserves_schema_and_nulls() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("payload", DataType::Utf8, true)
                    .with_metadata(HashMap::from([("kind".to_string(), "text".to_string())])),
            ],
            HashMap::from([("source".to_string(), "test".to_string())]),
        ));
        let mut values = (0..256)
            .map(|index| Some(format!("{index:04}-{}", "x".repeat(512))))
            .collect::<Vec<_>>();
        values[129] = None;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();
        let retained = TableData::try_new(vec![batch])
            .unwrap()
            .slice(128, Some(2))
            .unwrap();

        let compact = retained.compact().unwrap();
        assert_eq!(compact.schema.as_ref(), schema.as_ref());
        assert!(compact.nbytes() < retained.nbytes());
        let payload = compact.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(payload.value(0).starts_with("0128-"));
        assert!(payload.is_null(1));

        let retained_column = retained
            .column_by_index(retained.column_index("payload").unwrap())
            .unwrap();
        let compact_column = retained_column.compact().unwrap();
        assert_eq!(compact_column.field, retained_column.field);
        assert!(compact_column.nbytes() < retained_column.nbytes());
        let payload = compact_column.chunks[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(payload.value(0).starts_with("0128-"));
        assert!(payload.is_null(1));
    }

    #[test]
    fn forced_text_column_preserves_scalar_formatting_and_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("row", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3, 4, 5])) as ArrayRef],
        )
        .unwrap();
        let table = TableData::try_new(vec![batch]).unwrap();
        let cells = [
            CellValue::Bool(true),
            CellValue::Int(-7),
            CellValue::Float(1.25),
            CellValue::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
            CellValue::Text("borrowed".to_string()),
            CellValue::Null,
        ];

        let output = table
            .with_cells_column(1, "text", &cells, false, true)
            .unwrap();
        let text = output.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            text.iter().collect::<Vec<_>>(),
            [
                Some("true"),
                Some("-7"),
                Some("1.25"),
                Some("2024-01-02"),
                Some("borrowed"),
                None,
            ]
        );
    }
}
