//! Request shard planning, ordered execution, and Arrow schema reconciliation.

use std::collections::HashMap;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use futures_util::stream::{self, StreamExt};

use super::state::typed_builder::{ArrowType, TypedBuilder};
use super::{
    state, BlpAsyncError, Engine, EngineConfig, Operation, OverridePairs, PlannedRequestShape,
    PreparedRequest,
};

fn shard_security_chunks(securities: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    if chunk_size == 0 {
        return Vec::new();
    }
    securities
        .chunks(chunk_size)
        .map(<[String]>::to_vec)
        .collect()
}

fn merge_override_pairs(
    global: Option<&OverridePairs>,
    security: Option<&OverridePairs>,
) -> Option<OverridePairs> {
    let capacity = global.map_or(0, Vec::len) + security.map_or(0, Vec::len);
    if capacity == 0 {
        return None;
    }

    let mut merged = Vec::with_capacity(capacity);
    if let Some(global) = global {
        merged.extend(global.iter().cloned());
    }
    if let Some(security) = security {
        for (key, value) in security {
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
    }

    Some(merged)
}

fn push_security_override_shard(
    shards: &mut Vec<PreparedRequest>,
    prepared: &PreparedRequest,
    securities: &mut Vec<String>,
    security_overrides: Option<&OverridePairs>,
) {
    if securities.is_empty() {
        return;
    }
    let merged_overrides =
        merge_override_pairs(prepared.params().overrides.as_ref(), security_overrides);
    shards.push(prepared.for_security_shard(std::mem::take(securities), merged_overrides));
}

fn security_override_shards(
    config: &EngineConfig,
    prepared: &PreparedRequest,
) -> Option<Vec<PreparedRequest>> {
    let security_overrides = prepared
        .params()
        .security_overrides
        .as_ref()
        .filter(|entries| !entries.is_empty())?;
    let securities = prepared.params().securities.as_ref()?;
    if securities.is_empty() {
        return None;
    }

    let lookup: HashMap<&str, &OverridePairs> = security_overrides
        .iter()
        .map(|(security, overrides)| (security.as_str(), overrides))
        .collect();
    let max_chunk = if config.shard_requests {
        config.shard_chunk_size
    } else {
        usize::MAX
    };

    let mut shards = Vec::new();
    let mut current_securities = Vec::new();
    let mut current_overrides: Option<&OverridePairs> = None;
    let mut have_current = false;

    for security in securities {
        let next_overrides = lookup.get(security.as_str()).copied();
        if have_current
            && (current_overrides != next_overrides || current_securities.len() >= max_chunk)
        {
            push_security_override_shard(
                &mut shards,
                prepared,
                &mut current_securities,
                current_overrides,
            );
            have_current = false;
        }
        if !have_current {
            current_overrides = next_overrides;
            have_current = true;
        }
        current_securities.push(security.clone());
    }

    if have_current {
        push_security_override_shard(
            &mut shards,
            prepared,
            &mut current_securities,
            current_overrides,
        );
    }

    Some(shards)
}

pub(super) fn sharded_requests(
    config: &EngineConfig,
    prepared: &PreparedRequest,
) -> Option<Vec<PreparedRequest>> {
    if prepared.is_raw() {
        return None;
    }
    if !matches!(
        prepared.operation(),
        Operation::ReferenceData | Operation::HistoricalData
    ) {
        return None;
    }
    if !matches!(
        prepared.shape(),
        PlannedRequestShape::RefData(_) | PlannedRequestShape::HistData(_)
    ) {
        return None;
    }
    if let Some(shards) = security_override_shards(config, prepared) {
        return Some(shards);
    }
    if !config.shard_requests {
        return None;
    }

    let securities = prepared.params().securities.as_ref()?;
    if securities.is_empty() || securities.len() < config.shard_threshold {
        return None;
    }

    let chunks = shard_security_chunks(securities, config.shard_chunk_size);
    if chunks.len() < 2 {
        return None;
    }
    Some(
        chunks
            .into_iter()
            .map(|securities| {
                prepared.for_security_shard(securities, prepared.params().overrides.clone())
            })
            .collect(),
    )
}

fn concat_sharded_batches(batches: Vec<RecordBatch>) -> Result<RecordBatch, BlpAsyncError> {
    let Some((first, rest)) = batches.split_first() else {
        return Err(BlpAsyncError::Internal(
            "cannot concatenate zero sharded batches".to_string(),
        ));
    };
    if rest.is_empty() {
        return Ok(first.clone());
    }

    // Concatenation keeps only `target_schema`'s metadata; union the
    // response diagnostics (eidData / securityError / fieldExceptions)
    // across shards so later shards' entries are not silently dropped.
    let merged_meta = state::ResponseMetadata::union_of(&batches);

    let target_schema = first.schema_ref().clone();
    let mut normalized = Vec::with_capacity(batches.len());
    normalized.push(first.clone());
    for batch in rest {
        normalized.push(normalize_batch_to_schema(batch.clone(), &target_schema)?);
    }
    arrow_select::concat::concat_batches(&target_schema, normalized.iter())
        .map(|batch| merged_meta.attach(batch))
        .map_err(|err| {
            BlpAsyncError::Internal(format!("concatenate sharded request batches: {err}"))
        })
}

fn normalize_batch_to_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, BlpAsyncError> {
    if batch.schema_ref().as_ref() == schema.as_ref() {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(BlpAsyncError::Internal(
            "sharded request produced incompatible column count".to_string(),
        ));
    }

    let batch_schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, target_field) in schema.fields().iter().enumerate() {
        let source_field = batch_schema.field(idx);
        let expected = target_field.name();
        let actual = source_field.name();
        if actual != expected {
            return Err(BlpAsyncError::Internal(format!(
                "sharded request column mismatch at index {idx}: expected {expected}, got {actual}"
            )));
        }

        let array = batch.column(idx);
        if array.data_type() == target_field.data_type() {
            columns.push(array.clone());
        } else if array.null_count() == array.len() {
            columns.push(null_array_for_datatype(
                target_field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            return Err(BlpAsyncError::Internal(format!(
                "sharded request column type mismatch for {expected}: expected {:?}, got {:?}",
                target_field.data_type(),
                array.data_type()
            )));
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(|err| {
        BlpAsyncError::Internal(format!("concatenate sharded request batches: {err}"))
    })
}

fn null_array_for_datatype(data_type: &DataType, len: usize) -> Result<ArrayRef, BlpAsyncError> {
    let arrow_type = match data_type {
        DataType::Utf8 => ArrowType::String,
        DataType::Float64 => ArrowType::Float64,
        DataType::Int64 => ArrowType::Int64,
        DataType::Int32 => ArrowType::Int32,
        DataType::Boolean => ArrowType::Bool,
        DataType::Date32 => ArrowType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            ArrowType::TimestampMicros
        }
        DataType::Time64(TimeUnit::Microsecond) => ArrowType::Time64Micros,
        _ => {
            return Err(BlpAsyncError::Internal(format!(
                "cannot build null shard column for Arrow type {data_type:?}"
            )));
        }
    };

    let mut builder = TypedBuilder::new(arrow_type);
    for _ in 0..len {
        builder.append_null();
    }
    Ok(builder.finish())
}

impl Engine {
    pub(super) async fn request_shards_ordered(
        &self,
        shards: Vec<PreparedRequest>,
    ) -> Result<RecordBatch, BlpAsyncError> {
        let operation = shards
            .first()
            .map(|request| request.operation().as_str())
            .unwrap_or("unknown");
        let security_counts: Vec<usize> = shards
            .iter()
            .map(|request| request.params().securities.as_ref().map_or(0, Vec::len))
            .collect();
        xbbg_log::debug!(
            operation = operation,
            shard_count = shards.len(),
            max_concurrent = self.config.shard_max_concurrent,
            security_counts = ?security_counts,
            "dispatching sharded request"
        );

        let results = stream::iter(shards)
            .map(|request| async move { self.request_pool.request(request).await })
            .buffered(self.config.shard_max_concurrent)
            .collect::<Vec<_>>()
            .await;

        let mut batches = Vec::with_capacity(results.len());
        for result in results {
            batches.push(result?);
        }
        concat_sharded_batches(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ExtractorType, RequestParams, Service};
    use super::*;
    use crate::schema::SchemaCache;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;
    fn empty_schema() -> SchemaCache {
        SchemaCache::new()
    }

    fn prepare_refdata(securities: &[&str]) -> PreparedRequest {
        PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(
                    securities
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                ),
                fields: Some(vec!["PX_LAST".to_string()]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata")
    }

    fn prepare_histdata(securities: &[&str]) -> PreparedRequest {
        PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::HistoricalData.to_string(),
                securities: Some(
                    securities
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                ),
                fields: Some(vec!["PX_LAST".to_string()]),
                start_date: Some("20240101".to_string()),
                end_date: Some("20240131".to_string()),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared histdata")
    }

    fn shard_config() -> EngineConfig {
        EngineConfig {
            shard_requests: true,
            shard_threshold: 2,
            shard_chunk_size: 2,
            shard_max_concurrent: 2,
            ..Default::default()
        }
    }

    fn ticker_batch(values: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "ticker",
            Arc::new(StringArray::from_iter_values(values.iter().copied())) as ArrayRef,
        )])
        .expect("ticker batch")
    }

    fn px_last_batch(tickers: &[&str], px_last: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, true),
            Field::new("PX_LAST", px_last.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(tickers.iter().copied())) as ArrayRef,
                px_last,
            ],
        )
        .expect("px_last batch")
    }

    #[test]
    fn test_security_overrides_shard_by_contiguous_override_set_and_merge_globals() {
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec![
                    "A".to_string(),
                    "B".to_string(),
                    "C".to_string(),
                    "D".to_string(),
                ]),
                fields: Some(vec!["PX_LAST".to_string()]),
                overrides: Some(vec![("CRNCY".to_string(), "USD".to_string())]),
                security_overrides: Some(vec![
                    (
                        "A".to_string(),
                        vec![("CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "C".to_string(),
                        vec![("CRNCY".to_string(), "JPY".to_string())],
                    ),
                    (
                        "D".to_string(),
                        vec![("CRNCY".to_string(), "JPY".to_string())],
                    ),
                ]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with per-security overrides");

        let shards = sharded_requests(&EngineConfig::default(), &prepared).expect("shards");
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string()][..])
        );
        assert_eq!(
            shards[0].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "EUR".to_string())][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "USD".to_string())][..])
        );
        assert_eq!(
            shards[2].params().securities.as_deref(),
            Some(&["C".to_string(), "D".to_string()][..])
        );
        assert_eq!(
            shards[2].params().overrides.as_deref(),
            Some(&[("CRNCY".to_string(), "JPY".to_string())][..])
        );
        assert!(shards
            .iter()
            .all(|shard| shard.params().security_overrides.is_none()));
    }

    #[test]
    fn test_security_overrides_honor_enabled_shard_chunk_size() {
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                security_overrides: Some(vec![
                    (
                        "A".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "B".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                    (
                        "C".to_string(),
                        vec![("EQY_FUND_CRNCY".to_string(), "EUR".to_string())],
                    ),
                ]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with same per-security overrides");

        let shards = sharded_requests(
            &EngineConfig {
                shard_requests: true,
                shard_threshold: 2,
                shard_chunk_size: 2,
                shard_max_concurrent: 2,
                ..Default::default()
            },
            &prepared,
        )
        .expect("shards");

        assert_eq!(shards.len(), 2);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string(), "B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["C".to_string()][..])
        );
        for shard in shards {
            assert_eq!(
                shard.params().overrides.as_deref(),
                Some(&[("EQY_FUND_CRNCY".to_string(), "EUR".to_string())][..])
            );
        }
    }

    #[test]
    fn test_security_overrides_reject_unknown_security() {
        let err = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                security_overrides: Some(vec![(
                    "B".to_string(),
                    vec![("CRNCY".to_string(), "EUR".to_string())],
                )]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("security_overrides contains security not in request: B"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn test_sharded_requests_skip_when_disabled_or_below_threshold() {
        let prepared = prepare_refdata(&["A", "B", "C"]);

        assert!(sharded_requests(&EngineConfig::default(), &prepared).is_none());
        assert!(sharded_requests(
            &EngineConfig {
                shard_requests: true,
                shard_threshold: 4,
                shard_chunk_size: 2,
                shard_max_concurrent: 2,
                ..Default::default()
            },
            &prepared,
        )
        .is_none());
    }

    #[test]
    fn test_sharded_requests_skip_raw_and_non_ref_hist() {
        let raw = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::RawRequest.to_string(),
                request_operation: Some(Operation::ReferenceData.to_string()),
                extractor: ExtractorType::RefData,
                extractor_set: true,
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared raw refdata");
        assert!(sharded_requests(&shard_config(), &raw).is_none());

        let intraday = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::IntradayBar.to_string(),
                security: Some("A".to_string()),
                event_type: Some("TRADE".to_string()),
                interval: Some(1),
                start_datetime: Some("2024-01-01T09:30:00".to_string()),
                end_datetime: Some("2024-01-01T10:00:00".to_string()),
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared intraday");
        assert!(sharded_requests(&shard_config(), &intraday).is_none());
    }

    #[test]
    fn test_sharded_requests_chunk_in_order() {
        let prepared = prepare_refdata(&["S0", "S1", "S2", "S3", "S4"]);
        let shards = sharded_requests(&shard_config(), &prepared).expect("shards");
        let securities: Vec<Vec<String>> = shards
            .iter()
            .map(|shard| shard.params().securities.clone().expect("securities"))
            .collect();

        assert_eq!(
            securities,
            vec![
                vec!["S0".to_string(), "S1".to_string()],
                vec!["S2".to_string(), "S3".to_string()],
                vec!["S4".to_string()],
            ]
        );

        let hist = prepare_histdata(&["H0", "H1", "H2"]);
        assert_eq!(
            sharded_requests(&shard_config(), &hist)
                .expect("hist shards")
                .len(),
            2
        );
    }

    #[test]
    fn test_sharded_requests_preserve_overrides_elements_and_field_types() {
        let field_types = HashMap::from([("PX_LAST".to_string(), "float64".to_string())]);
        let prepared = PreparedRequest::prepare(
            RequestParams {
                service: Service::RefData.to_string(),
                operation: Operation::ReferenceData.to_string(),
                securities: Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                fields: Some(vec!["PX_LAST".to_string()]),
                overrides: Some(vec![("EQY_FUND_CRNCY".to_string(), "USD".to_string())]),
                elements: Some(vec![("returnEids".to_string(), "true".to_string())]),
                field_types: Some(field_types.clone()),
                include_security_errors: true,
                ..Default::default()
            },
            &empty_schema(),
        )
        .expect("prepared refdata with overrides");

        let shards = sharded_requests(&shard_config(), &prepared).expect("shards");
        assert_eq!(shards.len(), 2);
        assert_eq!(
            shards[0].params().securities.as_deref(),
            Some(&["A".to_string(), "B".to_string()][..])
        );
        assert_eq!(
            shards[1].params().securities.as_deref(),
            Some(&["C".to_string()][..])
        );
        for shard in shards {
            assert_eq!(shard.params().overrides, prepared.params().overrides);
            assert_eq!(shard.params().elements, prepared.params().elements);
            assert_eq!(shard.params().field_types, Some(field_types.clone()));
            assert!(shard.params().include_security_errors);
        }
    }

    #[test]
    fn test_concat_sharded_batches_preserves_order() {
        let batch =
            concat_sharded_batches(vec![ticker_batch(&["A", "B"]), ticker_batch(&["C", "D"])])
                .expect("concatenated batch");
        let ticker = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker column");

        assert_eq!(ticker.value(0), "A");
        assert_eq!(ticker.value(1), "B");
        assert_eq!(ticker.value(2), "C");
        assert_eq!(ticker.value(3), "D");
    }

    #[test]
    fn test_concat_sharded_batches_promotes_all_null_column_to_target_schema() {
        let batch = concat_sharded_batches(vec![
            px_last_batch(
                &["A"],
                Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef,
            ),
            px_last_batch(
                &["B"],
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ])
        .expect("concatenated batch");

        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
        let px_last = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("PX_LAST column");
        assert_eq!(px_last.value(0), 1.0);
        assert!(px_last.is_null(1));
    }

    #[test]
    fn test_concat_sharded_batches_rejects_non_null_type_mismatch() {
        let err = concat_sharded_batches(vec![
            px_last_batch(
                &["A"],
                Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef,
            ),
            px_last_batch(
                &["B"],
                Arc::new(StringArray::from(vec![Some("bad")])) as ArrayRef,
            ),
        ])
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("column type mismatch"),
            "unexpected error: {err}"
        );
    }
}
