//! Historical data transformation utilities.
//!
//! Column renaming, percentage calculations for dividend and earnings data.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};

use crate::constants::{DVD_COLS, ETF_COLS};
use crate::error::{ExtError, Result};

/// Rename dividend columns from Bloomberg names to clean names.
///
/// Uses the `DVD_COLS` mapping to transform column names like
/// "Declared Date" to "dec_date".
///
/// # Arguments
///
/// * `columns` - Slice of column names to rename
///
/// # Returns
///
/// A vector of (old_name, new_name) pairs for columns that need renaming.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::historical::rename_dividend_columns;
///
/// let columns = vec!["ticker", "Declared Date", "Ex-Date", "Dividend Amount"];
/// let renames = rename_dividend_columns(&columns);
///
/// assert_eq!(renames.len(), 3);
/// assert!(renames.iter().any(|(old, new)| old == "Declared Date" && new == "dec_date"));
/// ```
pub fn rename_dividend_columns(columns: &[&str]) -> Vec<(String, String)> {
    columns
        .iter()
        .filter_map(|col| {
            DVD_COLS
                .get(*col)
                .map(|new_name| (col.to_string(), new_name.to_string()))
        })
        .collect()
}

/// Rename ETF holdings columns from Bloomberg names to clean names.
///
/// Uses the `ETF_COLS` mapping.
pub fn rename_etf_columns(columns: &[&str]) -> Vec<(String, String)> {
    columns
        .iter()
        .filter_map(|col| {
            ETF_COLS
                .get(*col)
                .map(|new_name| (col.to_string(), new_name.to_string()))
        })
        .collect()
}

/// Build column rename mapping from earnings header values.
///
/// Maps data column names (e.g., "Period X Value") to human-readable names
/// derived from the header row (e.g., "fy2023").
///
/// # Arguments
///
/// * `header_row` - Pairs of (column_name, header_value) from the header DataFrame
/// * `data_columns` - Column names from the data DataFrame
///
/// # Returns
///
/// A vector of (old_name, new_name) pairs for columns that need renaming.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::historical::build_earning_header_rename;
///
/// let header = vec![
///     ("Period 1 Header".to_string(), "FY 2023".to_string()),
///     ("Period 2 Header".to_string(), "FY 2022".to_string()),
/// ];
/// let data_cols = vec!["ticker", "Period 1 Value", "Period 2 Value", "level"];
///
/// let renames = build_earning_header_rename(&header, &data_cols);
/// assert!(renames.iter().any(|(old, new)| old == "Period 1 Value" && new == "fy2023"));
/// assert!(renames.iter().any(|(old, new)| old == "Period 2 Value" && new == "fy2022"));
/// ```
pub fn build_earning_header_rename(
    header_row: &[(String, String)],
    data_columns: &[&str],
) -> Vec<(String, String)> {
    let header_map: std::collections::HashMap<&str, &str> = header_row
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    data_columns
        .iter()
        .filter_map(|data_col| {
            if *data_col == "ticker" {
                return None;
            }

            // Determine the corresponding header column name
            let header_col = if data_col.ends_with(" Value") {
                data_col.replace(" Value", " Header")
            } else {
                format!("{} Header", data_col)
            };

            // Get the header value and clean it
            header_map.get(header_col.as_str()).map(|header_val| {
                let new_name = header_val
                    .to_lowercase()
                    .replace(' ', "_")
                    .replace("_20", "20");
                (data_col.to_string(), new_name)
            })
        })
        .collect()
}

/// Apply column renaming to a RecordBatch.
///
/// Creates a new RecordBatch with renamed columns based on the provided mapping.
pub fn apply_column_renames(
    batch: &RecordBatch,
    renames: &[(String, String)],
) -> Result<RecordBatch> {
    let schema = batch.schema();

    // Build new schema with renamed fields
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let new_name = renames
                .iter()
                .find(|(old, _)| old == field.name())
                .map(|(_, new)| new.as_str())
                .unwrap_or(field.name());

            Field::new(new_name, field.data_type().clone(), field.is_nullable())
        })
        .collect();

    let new_schema = Arc::new(Schema::new(new_fields));

    // Columns stay the same, just schema changes
    let columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| batch.column(i).clone())
        .collect();

    RecordBatch::try_new(new_schema, columns).map_err(ExtError::Arrow)
}

/// Calculate percentage values within groups.
///
/// For earnings data, calculates what percentage each row represents
/// of its group total (e.g., geographic breakdown as % of total).
///
/// # Arguments
///
/// * `values` - The numeric values
/// * `levels` - The hierarchy levels (1 = top level, 2 = sub-level, etc.)
///
/// # Returns
///
/// A vector of percentages (0-100) for each row.
pub fn calculate_level_percentages(
    values: &[Option<f64>],
    levels: &[Option<i64>],
) -> Vec<Option<f64>> {
    if values.len() != levels.len() {
        return vec![None; values.len()];
    }

    let mut percentages = vec![None; values.len()];

    // Calculate level 1 percentages (% of total level 1)
    let level_1_indices: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter_map(|(i, lvl)| if *lvl == Some(1) { Some(i) } else { None })
        .collect();

    if !level_1_indices.is_empty() {
        let level_1_sum: f64 = level_1_indices.iter().filter_map(|&i| values[i]).sum();

        if level_1_sum != 0.0 {
            for &i in &level_1_indices {
                if let Some(val) = values[i] {
                    percentages[i] = Some(100.0 * val / level_1_sum);
                }
            }
        }
    }

    // Calculate level 2 percentages (% of parent level 1 group).
    //
    // Each level-2 row's share is computed relative to the sum of its group.
    fn flush_group(group: &[usize], values: &[Option<f64>], out: &mut [Option<f64>]) {
        if group.is_empty() {
            return;
        }
        let group_sum: f64 = group.iter().filter_map(|&j| values[j]).sum();
        if group_sum != 0.0 {
            for &j in group {
                if let Some(val) = values[j] {
                    out[j] = Some(100.0 * val / group_sum);
                }
            }
        }
    }

    // Iterate backwards to group level 2 rows by their level 1 parent.
    let mut level_2_group: Vec<usize> = Vec::new();

    for i in (0..levels.len()).rev() {
        match levels[i] {
            Some(2) => {
                level_2_group.push(i);
            }
            Some(1) => {
                flush_group(&level_2_group, values, &mut percentages);
                level_2_group.clear();
            }
            _ => {}
        }
    }

    // Flush any leading level-2 rows that precede the first level-1 parent.
    flush_group(&level_2_group, values, &mut percentages);

    percentages
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rename_dividend_columns() {
        let columns = vec!["ticker", "Declared Date", "Ex-Date", "Unknown"];
        let renames = rename_dividend_columns(&columns);

        assert_eq!(renames.len(), 2);
        assert!(renames.contains(&("Declared Date".to_string(), "dec_date".to_string())));
        assert!(renames.contains(&("Ex-Date".to_string(), "ex_date".to_string())));
    }

    #[test]
    fn test_rename_etf_columns() {
        let columns = vec!["Holding Name", "Weight", "Unknown"];
        let renames = rename_etf_columns(&columns);

        assert_eq!(renames.len(), 2);
        assert!(renames.contains(&("Holding Name".to_string(), "name".to_string())));
        assert!(renames.contains(&("Weight".to_string(), "weight".to_string())));
    }

    #[test]
    fn test_calculate_level_percentages() {
        let values = vec![Some(100.0), Some(200.0), Some(50.0), Some(50.0)];
        let levels = vec![Some(1), Some(1), Some(2), Some(2)];

        let pcts = calculate_level_percentages(&values, &levels);

        // Level 1: 100/(100+200) = 33.33%, 200/(100+200) = 66.67%
        assert!((pcts[0].unwrap() - 33.333).abs() < 0.01);
        assert!((pcts[1].unwrap() - 66.667).abs() < 0.01);

        // Level 2: 50/(50+50) = 50%, 50/(50+50) = 50%
        assert!((pcts[2].unwrap() - 50.0).abs() < 0.01);
        assert!((pcts[3].unwrap() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_calculate_level_percentages_with_nones() {
        let values = vec![Some(100.0), None, Some(200.0)];
        let levels = vec![Some(1), Some(1), Some(1)];

        let pcts = calculate_level_percentages(&values, &levels);

        // Sum is 300 (ignoring None)
        assert!((pcts[0].unwrap() - 33.333).abs() < 0.01);
        assert!(pcts[1].is_none());
        assert!((pcts[2].unwrap() - 66.667).abs() < 0.01);
    }

    #[test]
    fn test_calculate_level_percentages_leading_level_2_group() {
        // Level-2 rows that precede the first level-1 parent must still be
        // computed relative to their group total (regression for dropped group).
        let values = vec![Some(30.0), Some(70.0), Some(100.0)];
        let levels = vec![Some(2), Some(2), Some(1)];

        let pcts = calculate_level_percentages(&values, &levels);

        assert!((pcts[0].unwrap() - 30.0).abs() < 0.01);
        assert!((pcts[1].unwrap() - 70.0).abs() < 0.01);
        // The lone level-1 row is 100% of the level-1 total.
        assert!((pcts[2].unwrap() - 100.0).abs() < 0.01);
    }
}
