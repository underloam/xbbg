//! DataFrame pivot operations using Arrow.
//!
//! Provides efficient long-to-wide pivoting for Bloomberg data.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;

use crate::error::{ExtError, Result};

/// Names xbbg gives the column holding each row's security.
const KEY_COLUMNS: [&str; 2] = ["security", "ticker"];

/// Marks a (key, field) cell with no source row.
const MISSING_ROW: u32 = u32::MAX;

/// Pivot a long-format DataFrame to wide format.
///
/// Transforms data from:
/// ```text
/// | ticker        | field   | value  |
/// |---------------|---------|--------|
/// | AAPL US Equity| PX_LAST | 150.0  |
/// | AAPL US Equity| VOLUME  | 1000000|
/// | MSFT US Equity| PX_LAST | 300.0  |
/// ```
///
/// To:
/// ```text
/// | ticker        | PX_LAST | VOLUME  |
/// |---------------|---------|---------|
/// | AAPL US Equity| 150.0   | 1000000 |
/// | MSFT US Equity| 300.0   | null    |
/// ```
///
/// # Arguments
///
/// * `batch` - Input RecordBatch with a key column (`security` or `ticker`),
///   `field`, and `value`
///
/// # Returns
///
/// A new RecordBatch with the input's non-nullable key column (unique keys in
/// first-appearance order) and one nullable column per unique field in
/// first-appearance order. Field columns keep the `value` column's data type;
/// when a (key, field) pair repeats, the last row wins, including a null
/// value. Rows with a null key or field contribute no cell, but their
/// non-null key still gets a row or column.
///
/// # Errors
///
/// Returns error if the key column or `field` is not a string column, or if the
/// batch has more than `u32::MAX` rows.
pub fn pivot_to_wide(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    // Already wide format or a different structure: return as-is.
    let Some(key) = long_format_key(&schema) else {
        return Ok(batch.clone());
    };

    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Get column indices
    let ticker_idx = schema
        .index_of(key)
        .map_err(|_| ExtError::MissingColumn(key.into()))?;
    let field_idx = schema
        .index_of("field")
        .map_err(|_| ExtError::MissingColumn("field".into()))?;
    let value_idx = schema
        .index_of("value")
        .map_err(|_| ExtError::MissingColumn("value".into()))?;

    let ticker_col = batch
        .column(ticker_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ExtError::MissingColumn(format!("{key} must be string")))?;

    let field_col = batch
        .column(field_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ExtError::MissingColumn("field must be string".into()))?;

    let value_col = batch.column(value_idx);
    if u32::try_from(batch.num_rows()).is_err() {
        return Err(ExtError::InvalidInput(format!(
            "pivot_to_wide supports at most {} rows",
            u32::MAX
        )));
    }

    // One pass: code tickers and fields in first-appearance order and record
    // every row that names both, in row order so later rows overwrite.
    let mut ticker_codes: HashMap<&str, u32> = HashMap::new();
    let mut tickers: Vec<&str> = Vec::new();
    let mut field_codes: HashMap<&str, u32> = HashMap::new();
    let mut fields: Vec<&str> = Vec::new();
    let mut cells: Vec<(u32, u32, u32)> = Vec::with_capacity(batch.num_rows());
    for (row, (ticker, field)) in ticker_col.iter().zip(field_col.iter()).enumerate() {
        let ticker = ticker.map(|ticker| intern(&mut ticker_codes, &mut tickers, ticker));
        let field = field.map(|field| intern(&mut field_codes, &mut fields, field));
        if let (Some(ticker), Some(field)) = (ticker, field) {
            // Lossless: the row count was checked against `u32::MAX` above.
            cells.push((ticker, field, row as u32));
        }
    }

    // Field-major matrix of the last source row for each (ticker, field).
    let n_tickers = tickers.len();
    let mut last_rows = vec![MISSING_ROW; n_tickers * fields.len()];
    for (ticker, field, row) in cells {
        last_rows[field as usize * n_tickers + ticker as usize] = row;
    }

    let mut out_fields = Vec::with_capacity(1 + fields.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + fields.len());
    out_fields.push(Field::new(key, DataType::Utf8, false));
    columns.push(Arc::new(StringArray::from(tickers)));
    for (code, name) in fields.iter().enumerate() {
        let indices = last_rows[code * n_tickers..(code + 1) * n_tickers]
            .iter()
            .map(|&row| (row != MISSING_ROW).then_some(row))
            .collect::<UInt32Array>();
        out_fields.push(Field::new(*name, value_col.data_type().clone(), true));
        columns.push(take(value_col.as_ref(), &indices, None)?);
    }

    RecordBatch::try_new(Arc::new(Schema::new(out_fields)), columns).map_err(ExtError::Arrow)
}

/// Code `key` in first-appearance order.
fn intern<'a>(codes: &mut HashMap<&'a str, u32>, keys: &mut Vec<&'a str>, key: &'a str) -> u32 {
    *codes.entry(key).or_insert_with(|| {
        keys.push(key);
        // Lossless: there are at most as many keys as rows (<= `u32::MAX`).
        (keys.len() - 1) as u32
    })
}

/// Check if a RecordBatch is in long format (`security` or `ticker`, `field`,
/// `value`).
pub fn is_long_format(batch: &RecordBatch) -> bool {
    long_format_key(&batch.schema()).is_some()
}

/// The key column of a long `(key, field, value)` schema.
fn long_format_key(schema: &Schema) -> Option<&'static str> {
    let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if columns.len() != 3 || !columns.contains(&"field") || !columns.contains(&"value") {
        return None;
    }
    KEY_COLUMNS.into_iter().find(|key| columns.contains(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::Schema;

    fn make_long_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let ticker = StringArray::from(vec![
            "AAPL US Equity",
            "AAPL US Equity",
            "MSFT US Equity",
            "MSFT US Equity",
        ]);
        let field = StringArray::from(vec!["PX_LAST", "VOLUME", "PX_LAST", "VOLUME"]);
        let value = StringArray::from(vec!["150.0", "1000000", "300.0", "2000000"]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ticker), Arc::new(field), Arc::new(value)],
        )
        .unwrap()
    }

    #[test]
    fn test_pivot_basic() {
        let input = make_long_batch();
        let output = pivot_to_wide(&input).unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.num_columns(), 3); // ticker, PX_LAST, VOLUME

        let schema = output.schema();
        assert!(schema.field_with_name("ticker").is_ok());
        assert!(schema.field_with_name("PX_LAST").is_ok());
        assert!(schema.field_with_name("VOLUME").is_ok());
    }

    #[test]
    fn security_keyed_batch_pivots_under_its_own_key() {
        let input = make_long_batch();
        let schema = Schema::new(vec![
            Field::new("security", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let input = RecordBatch::try_new(Arc::new(schema), input.columns().to_vec()).unwrap();

        assert!(is_long_format(&input));
        let output = pivot_to_wide(&input).unwrap();
        let names: Vec<_> = output
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["security", "PX_LAST", "VOLUME"]);
    }

    #[test]
    fn test_pivot_preserves_values() {
        let input = make_long_batch();
        let output = pivot_to_wide(&input).unwrap();

        let ticker_col = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tickers: Vec<&str> = (0..ticker_col.len()).map(|i| ticker_col.value(i)).collect();

        assert!(tickers.contains(&"AAPL US Equity"));
        assert!(tickers.contains(&"MSFT US Equity"));
    }

    #[test]
    fn test_is_long_format() {
        let long_batch = make_long_batch();
        assert!(is_long_format(&long_batch));

        // Create a wide batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("PX_LAST", DataType::Utf8, true),
        ]));
        let ticker = StringArray::from(vec!["AAPL US Equity"]);
        let px = StringArray::from(vec!["150.0"]);
        let wide_batch =
            RecordBatch::try_new(schema, vec![Arc::new(ticker), Arc::new(px)]).unwrap();

        assert!(!is_long_format(&wide_batch));
    }

    #[test]
    fn test_pivot_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::new_empty(schema);
        let result = pivot_to_wide(&empty).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_pivot_already_wide() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("PX_LAST", DataType::Utf8, true),
            Field::new("VOLUME", DataType::Utf8, true),
        ]));
        let ticker = StringArray::from(vec!["AAPL US Equity"]);
        let px = StringArray::from(vec!["150.0"]);
        let vol = StringArray::from(vec!["1000000"]);
        let wide =
            RecordBatch::try_new(schema, vec![Arc::new(ticker), Arc::new(px), Arc::new(vol)])
                .unwrap();

        // Should return unchanged
        let result = pivot_to_wide(&wide).unwrap();
        assert_eq!(result.num_columns(), 3);
    }

    fn long_batch(
        tickers: Vec<Option<&str>>,
        fields: Vec<Option<&str>>,
        values: ArrayRef,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("field", DataType::Utf8, true),
            Field::new("value", values.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tickers)),
                Arc::new(StringArray::from(fields)),
                values,
            ],
        )
        .unwrap()
    }

    fn wide_batch(
        tickers: Vec<&str>,
        value_type: &DataType,
        columns: Vec<(&str, ArrayRef)>,
    ) -> RecordBatch {
        let mut fields = vec![Field::new("ticker", DataType::Utf8, false)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(tickers))];
        for (name, array) in columns {
            fields.push(Field::new(name, value_type.clone(), true));
            arrays.push(array);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn test_pivot_utf8_last_write_wins_including_null_overwrite() {
        let input = long_batch(
            vec![
                Some("AAPL US Equity"),
                Some("AAPL US Equity"),
                Some("MSFT US Equity"),
                Some("AAPL US Equity"),
                Some("MSFT US Equity"),
                Some("MSFT US Equity"),
            ],
            vec![
                Some("PX_LAST"),
                Some("VOLUME"),
                Some("PX_LAST"),
                Some("PX_LAST"),
                Some("VOLUME"),
                Some("VOLUME"),
            ],
            Arc::new(StringArray::from(vec![
                Some("150.0"),
                Some("1000000"),
                Some("300.0"),
                None,
                None,
                Some("2000000"),
            ])),
        );
        let expected = wide_batch(
            vec!["AAPL US Equity", "MSFT US Equity"],
            &DataType::Utf8,
            vec![
                (
                    "PX_LAST",
                    Arc::new(StringArray::from(vec![None, Some("300.0")])),
                ),
                (
                    "VOLUME",
                    Arc::new(StringArray::from(vec![Some("1000000"), Some("2000000")])),
                ),
            ],
        );
        assert_eq!(pivot_to_wide(&input).unwrap(), expected);
    }

    #[test]
    fn test_pivot_null_keys_skip_cells_but_keep_their_other_key() {
        let input = long_batch(
            vec![None, Some("IBM US Equity"), Some("AAPL US Equity")],
            vec![Some("CUR_MKT_CAP"), None, Some("PX_LAST")],
            Arc::new(StringArray::from(vec!["1", "2", "3"])),
        );
        let expected = wide_batch(
            vec!["IBM US Equity", "AAPL US Equity"],
            &DataType::Utf8,
            vec![
                (
                    "CUR_MKT_CAP",
                    Arc::new(StringArray::from(vec![None::<&str>, None])),
                ),
                (
                    "PX_LAST",
                    Arc::new(StringArray::from(vec![None, Some("3")])),
                ),
            ],
        );
        assert_eq!(pivot_to_wide(&input).unwrap(), expected);
    }

    #[test]
    fn test_pivot_typed_values_keep_their_type() {
        let input = long_batch(
            vec![
                Some("AAPL US Equity"),
                Some("MSFT US Equity"),
                Some("AAPL US Equity"),
            ],
            vec![Some("PX_LAST"), Some("PX_LAST"), Some("VOLUME")],
            Arc::new(Float64Array::from(vec![Some(150.5), None, Some(1e6)])),
        );
        let expected = wide_batch(
            vec!["AAPL US Equity", "MSFT US Equity"],
            &DataType::Float64,
            vec![
                (
                    "PX_LAST",
                    Arc::new(Float64Array::from(vec![Some(150.5), None])),
                ),
                (
                    "VOLUME",
                    Arc::new(Float64Array::from(vec![Some(1e6), None])),
                ),
            ],
        );
        assert_eq!(pivot_to_wide(&input).unwrap(), expected);

        let input = long_batch(
            vec![Some("AAPL US Equity"), Some("AAPL US Equity")],
            vec![Some("VOLUME"), Some("VOLUME")],
            Arc::new(Int64Array::from(vec![1, 2])),
        );
        let expected = wide_batch(
            vec!["AAPL US Equity"],
            &DataType::Int64,
            vec![("VOLUME", Arc::new(Int64Array::from(vec![2])))],
        );
        assert_eq!(pivot_to_wide(&input).unwrap(), expected);
    }
}
