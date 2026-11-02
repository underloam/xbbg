//! DataFrame pivot operations using Arrow.
//!
//! Provides efficient long-to-wide pivoting for Bloomberg data.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::error::{ExtError, Result};

/// Names xbbg gives the column holding each row's security.
const KEY_COLUMNS: [&str; 2] = ["security", "ticker"];

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
/// A new RecordBatch with the input's key column and one column per unique
/// field value.
///
/// # Errors
///
/// Returns error if required columns are missing or if the batch is already wide format.
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

    let value_col = batch
        .column(value_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ExtError::MissingColumn("value must be string".into()))?;

    // First pass: collect unique tickers and fields
    let mut unique_tickers: Vec<String> = Vec::new();
    let mut ticker_to_idx: HashMap<String, usize> = HashMap::new();
    let mut unique_fields: Vec<String> = Vec::new();
    let mut field_to_idx: HashMap<String, usize> = HashMap::new();

    for i in 0..batch.num_rows() {
        if !ticker_col.is_null(i) {
            let ticker = ticker_col.value(i);
            if !ticker_to_idx.contains_key(ticker) {
                ticker_to_idx.insert(ticker.to_string(), unique_tickers.len());
                unique_tickers.push(ticker.to_string());
            }
        }
        if !field_col.is_null(i) {
            let field = field_col.value(i);
            if !field_to_idx.contains_key(field) {
                field_to_idx.insert(field.to_string(), unique_fields.len());
                unique_fields.push(field.to_string());
            }
        }
    }

    let n_tickers = unique_tickers.len();
    let n_fields = unique_fields.len();

    // Create value matrix: values[ticker_idx][field_idx] = value
    let mut values: Vec<Vec<Option<String>>> = vec![vec![None; n_fields]; n_tickers];

    // Second pass: fill in values
    for i in 0..batch.num_rows() {
        if ticker_col.is_null(i) || field_col.is_null(i) {
            continue;
        }
        let ticker = ticker_col.value(i);
        let field = field_col.value(i);
        let value = if value_col.is_null(i) {
            None
        } else {
            Some(value_col.value(i).to_string())
        };

        if let (Some(&t_idx), Some(&f_idx)) = (ticker_to_idx.get(ticker), field_to_idx.get(field)) {
            values[t_idx][f_idx] = value;
        }
    }

    // Build output schema: key + each field
    let mut fields = vec![Field::new(key, DataType::Utf8, false)];
    for field_name in &unique_fields {
        fields.push(Field::new(field_name, DataType::Utf8, true));
    }
    let out_schema = Arc::new(Schema::new(fields));

    // Build output columns
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + n_fields);

    // Ticker column
    let ticker_array = StringArray::from(unique_tickers.clone());
    columns.push(Arc::new(ticker_array));

    // Field columns
    for f_idx in 0..n_fields {
        let field_values: Vec<Option<&str>> =
            values.iter().map(|row| row[f_idx].as_deref()).collect();
        let array = StringArray::from(field_values);
        columns.push(Arc::new(array));
    }

    RecordBatch::try_new(out_schema, columns).map_err(ExtError::Arrow)
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
}
