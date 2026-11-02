//! The result column that holds each row's security.
//!
//! Extractors always name it `ticker`. [`Engine::request`] and
//! [`RequestStream`] rename it per [`EngineConfig::key_column`] as results leave
//! the engine, so engine-internal requests and recipe inputs share one name per
//! engine.
//!
//! [`Engine::request`]: super::Engine::request
//! [`RequestStream`]: super::RequestStream
//! [`EngineConfig::key_column`]: super::EngineConfig::key_column

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};

/// Name extractors give the leading security column of keyed results.
const EXTRACTED_NAME: &str = "ticker";

/// Name of the result column holding each row's security, as requested.
///
/// `Ticker` is the historical name and still the default, but it is
/// deprecated: once a consumer case-folds identifiers, it collides with
/// Bloomberg's `TICKER` field. `Security` follows Bloomberg's
/// `securityData.security` element. The next major release makes `Security`
/// the default and removes `Ticker`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyColumn {
    /// `ticker`: the default, deprecated.
    #[default]
    Ticker,
    /// `security`.
    Security,
}

impl KeyColumn {
    /// Column name results use.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ticker => "ticker",
            Self::Security => "security",
        }
    }
}

impl std::str::FromStr for KeyColumn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "ticker" => Ok(Self::Ticker),
            "security" => Ok(Self::Security),
            _ => Err(format!(
                "unknown key column '{s}': expected security or ticker"
            )),
        }
    }
}

impl std::fmt::Display for KeyColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Name the leading security column of a keyed result per `key`.
pub(crate) fn name_key_column(batch: RecordBatch, key: KeyColumn) -> RecordBatch {
    match renamed_schema(batch.schema_ref(), key) {
        Some(schema) => with_renamed_schema(batch, schema),
        None => batch,
    }
}

/// `schema` with its leading `ticker` field named per `key`; `None` when
/// nothing changes.
pub(crate) fn renamed_schema(schema: &SchemaRef, key: KeyColumn) -> Option<SchemaRef> {
    let first = schema.fields().first()?;
    if key.as_str() == EXTRACTED_NAME || first.name() != EXTRACTED_NAME {
        return None;
    }
    let mut fields = schema.fields().to_vec();
    fields[0] = Arc::new(first.as_ref().clone().with_name(key.as_str()));
    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Swap in a schema from [`renamed_schema`] for the batch's own.
pub(crate) fn with_renamed_schema(batch: RecordBatch, schema: SchemaRef) -> RecordBatch {
    let (_, columns, row_count) = batch.into_parts();
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .expect("renaming a field keeps every column's type")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use arrow_array::{ArrayRef, Float64Array, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;

    fn keyed_batch(first: &str) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(first, DataType::Utf8, false),
            Field::new("TICKER", DataType::Utf8, true),
            Field::new("PX_LAST", DataType::Float64, true),
        ])
        .with_metadata(HashMap::from([(
            "xbbg.bloomberg_request_id".to_string(),
            "sdk-synthetic".to_string(),
        )]));
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["EURUSD BGN Curncy"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["EURUSD"])),
                Arc::new(Float64Array::from(vec![1.08])),
            ],
        )
        .unwrap()
    }

    fn names(batch: &RecordBatch) -> Vec<&str> {
        batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    #[test]
    fn parses_every_spelling_case_insensitively() {
        assert_eq!(KeyColumn::from_str("Security"), Ok(KeyColumn::Security));
        assert_eq!(KeyColumn::from_str("TICKER"), Ok(KeyColumn::Ticker));
        let error = KeyColumn::from_str("security_id").unwrap_err();
        assert!(error.contains("security_id"), "{error}");
    }

    #[test]
    fn security_renames_only_the_leading_key_column() {
        let source = keyed_batch("ticker");
        let renamed = name_key_column(source.clone(), KeyColumn::Security);

        assert_eq!(names(&renamed), ["security", "TICKER", "PX_LAST"]);
        assert_eq!(
            renamed.schema_ref().metadata(),
            source.schema_ref().metadata()
        );
        assert!(!renamed.schema_ref().field(0).is_nullable());
        for (renamed, source) in renamed.columns().iter().zip(source.columns()) {
            assert!(Arc::ptr_eq(renamed, source), "columns are not copied");
        }
    }

    #[test]
    fn leaves_ticker_engines_and_unkeyed_batches_unchanged() {
        let ticker = keyed_batch("ticker");
        let kept = name_key_column(ticker.clone(), KeyColumn::Ticker);
        assert!(Arc::ptr_eq(kept.schema_ref(), ticker.schema_ref()));

        let unkeyed = keyed_batch("field");
        let kept = name_key_column(unkeyed.clone(), KeyColumn::Security);
        assert!(Arc::ptr_eq(kept.schema_ref(), unkeyed.schema_ref()));
    }
}
