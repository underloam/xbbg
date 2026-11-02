//! Index constituent recipes.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use xbbg_async::engine::{Engine, ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};

use crate::error::{RecipeError, Result};
use crate::utils::{array_value_as_string, canonical_name};

const SUPPORTED_INDEX_MEMBER_FIELDS: &[&str] = &["INDX_MWEIGHT", "INDX_MEMBERS", "INDX_MEMBERS3"];

/// Fetch and normalize Bloomberg index constituent bulk data.
pub async fn recipe_index_members(
    engine: &Engine,
    index: String,
    field: Option<String>,
    asof: Option<String>,
) -> Result<RecordBatch> {
    let field = field.unwrap_or_else(|| "INDX_MWEIGHT".to_string());
    let normalized_field = field.trim().to_ascii_uppercase();
    if !SUPPORTED_INDEX_MEMBER_FIELDS.contains(&normalized_field.as_str()) {
        return Err(RecipeError::InvalidArgument(format!(
            "unsupported index member field '{field}', expected one of {SUPPORTED_INDEX_MEMBER_FIELDS:?}"
        )));
    }

    let overrides = asof.map(|date| vec![("END_DATE_OVERRIDE".to_string(), date)]);
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(vec![index.clone()]),
        fields: Some(vec![normalized_field.clone()]),
        overrides,
        ..Default::default()
    };

    let batch = engine.request(params).await?;
    normalize_index_members_batch(
        batch,
        engine.key_column().as_str(),
        &index,
        &normalized_field,
    )
}

/// Rename the member identifier column of bulk output keyed by the engine's
/// `key` column to `member`.
pub(crate) fn normalize_index_members_batch(
    batch: RecordBatch,
    key: &str,
    index: &str,
    field: &str,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Err(RecipeError::Other(format!(
            "Bloomberg returned no rows for index '{index}' field '{field}'"
        )));
    }

    let member_col_idx = find_member_column_index(&batch, key).ok_or_else(|| {
        RecipeError::Other(format!(
            "Bloomberg returned no recognizable member identifier column for index '{index}' field '{field}'"
        ))
    })?;

    let member_source = batch.column(member_col_idx);
    let mut member_builder = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 16);
    for row in 0..batch.num_rows() {
        match array_value_as_string(member_source, row) {
            Some(value) if !value.trim().is_empty() => member_builder.append_value(value.trim()),
            _ => member_builder.append_null(),
        }
    }

    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, original_field) in batch.schema().fields().iter().enumerate() {
        if idx == member_col_idx {
            fields.push(Field::new("member", DataType::Utf8, true));
            columns.push(Arc::new(member_builder.finish()));
            continue;
        }
        fields.push(original_field.as_ref().clone());
        columns.push(batch.column(idx).clone());
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn find_member_column_index(batch: &RecordBatch, key: &str) -> Option<usize> {
    let preferred = [
        "member",
        "member ticker and exchange code",
        "index member",
        "index member ticker",
        "security description",
        "futures ticker",
        "ticker and exchange code",
        "ticker",
    ];
    let preferred = preferred
        .iter()
        .map(|candidate| canonical_name(candidate))
        .collect::<Vec<_>>();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        if name == key || name == "field" {
            continue;
        }
        let key = canonical_name(name);
        if preferred.iter().any(|candidate| candidate == &key) {
            return Some(idx);
        }
    }

    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find_map(|(idx, field)| {
            let name = field.name();
            (name != key && name != "field").then_some(idx)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, StringArray};

    #[test]
    fn normalize_members_renames_first_identifier_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Member Ticker and Exchange Code", DataType::Utf8, true),
            Field::new("Percent Weight", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["SPX Index"])),
                Arc::new(StringArray::from(vec!["INDX_MWEIGHT"])),
                Arc::new(StringArray::from(vec!["AAPL US"])),
                Arc::new(Float64Array::from(vec![Some(7.0)])),
            ],
        )
        .unwrap();

        let normalized =
            normalize_index_members_batch(batch, "ticker", "SPX Index", "INDX_MWEIGHT").unwrap();
        assert!(normalized.column_by_name("member").is_some());
        assert!(normalized.column_by_name("Percent Weight").is_some());
    }

    #[test]
    fn unrecognized_member_column_is_never_the_key_column() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "security",
                Arc::new(StringArray::from(vec!["SPX Index"])) as ArrayRef,
            ),
            ("field", Arc::new(StringArray::from(vec!["INDX_MEMBERS"]))),
            ("Constituent", Arc::new(StringArray::from(vec!["AAPL US"]))),
        ])
        .unwrap();

        let normalized =
            normalize_index_members_batch(batch, "security", "SPX Index", "INDX_MEMBERS").unwrap();
        let names: Vec<_> = normalized
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["security", "field", "member"]);
    }

    #[test]
    fn normalize_members_rejects_empty_response() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ticker",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let err = normalize_index_members_batch(batch, "ticker", "BAD Index", "INDX_MWEIGHT")
            .unwrap_err();
        assert!(err.to_string().contains("no rows"));
    }
}
