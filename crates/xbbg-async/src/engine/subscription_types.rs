//! Shared field-metadata resolution for requests and subscription layouts.
//!
//! Both paths use the engine's field cache and FieldInfo extractor. Subscription
//! seeding is best-effort: one deadline covers metadata lookup and persistence,
//! and missing or unsupported metadata never establishes a string field kind.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
#[cfg(test)]
use futures_util::future::BoxFuture;
use tokio::runtime::Handle;

use crate::errors::BlpAsyncError;
use crate::field_cache::{global_resolver, FieldTypeResolver};
use crate::schema::SchemaCache;
use crate::services::{ExtractorType, Operation, Service};

use super::request_pool::RequestWorkerPool;
use super::state::typed_builder::ArrowType;
use super::state::FieldKind;
use super::{PreparedRequestBuilder, RequestParams};

const RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
type TestFieldInfoQuery =
    dyn Fn(Vec<String>) -> BoxFuture<'static, Result<RecordBatch, BlpAsyncError>> + Send + Sync;

/// Dispatches FieldInfo requests without boxing the production future.
/// Tests replace only the metadata transport, retaining real cache semantics.
enum FieldInfoQuery {
    Worker {
        pool: Arc<RequestWorkerPool>,
        schema_cache: SchemaCache,
    },
    #[cfg(test)]
    Injected(Box<TestFieldInfoQuery>),
}

impl FieldInfoQuery {
    async fn request(&self, fields: Vec<String>) -> Result<RecordBatch, BlpAsyncError> {
        match self {
            Self::Worker { pool, schema_cache } => {
                let params = RequestParams {
                    service: Service::ApiFlds.to_string(),
                    operation: Operation::FieldInfo.to_string(),
                    extractor: ExtractorType::FieldInfo,
                    field_ids: Some(fields),
                    ..Default::default()
                };
                // Cached-field hints and intraday timezone transforms do not
                // apply to the FieldInfo response shape.
                let request = PreparedRequestBuilder::prepare(params, schema_cache)?.finalize()?;
                pool.request(request).await
            }
            #[cfg(test)]
            Self::Injected(query) => query(fields).await,
        }
    }
}

pub(crate) struct SubscriptionTypeResolver {
    query: FieldInfoQuery,
    cache: Arc<FieldTypeResolver>,
    runtime: Handle,
}

impl SubscriptionTypeResolver {
    pub(crate) fn new(
        pool: Arc<RequestWorkerPool>,
        schema_cache: SchemaCache,
        runtime: Handle,
    ) -> Self {
        let cache = global_resolver();
        // Engine startup normally preloads this. Keep disk loading outside the
        // bounded async subscription path even when constructed independently.
        cache.preload();
        Self {
            query: FieldInfoQuery::Worker { pool, schema_cache },
            cache,
            runtime,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_query(
        cache: Arc<FieldTypeResolver>,
        runtime: Handle,
        query: impl Fn(Vec<String>) -> BoxFuture<'static, Result<RecordBatch, BlpAsyncError>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        cache.preload();
        Self {
            query: FieldInfoQuery::Injected(Box::new(query)),
            cache,
            runtime,
        }
    }

    /// Resolve with the existing manual override -> cache/API -> default order.
    /// Query and persistence failures retain the cached/default result, matching
    /// the public engine API rather than failing the requesting operation.
    pub(crate) async fn resolve_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
        default_type: &str,
    ) -> Result<HashMap<String, String>, BlpAsyncError> {
        let mut uncached = self.cache.get_uncached_fields(fields);
        if let Some(overrides) = manual_overrides {
            uncached.retain(|field| {
                !overrides.contains_key(field) && !overrides.contains_key(&field.to_uppercase())
            });
        }

        if !uncached.is_empty() {
            xbbg_log::debug!(
                field_count = uncached.len(),
                "Querying //blp/apiflds for field types"
            );
            match self.query.request(uncached).await {
                Ok(batch) => {
                    self.cache.insert_from_response(&batch);
                    let cache = Arc::clone(&self.cache);
                    match self
                        .runtime
                        .spawn_blocking(move || cache.save_to_disk())
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => {
                            xbbg_log::warn!("failed to persist field cache");
                        }
                        Err(_) => {
                            xbbg_log::warn!("field cache save task failed");
                        }
                    }
                }
                Err(_) => {
                    xbbg_log::warn!("Failed to query field types, using cached types and defaults");
                }
            }
        }

        Ok(self
            .cache
            .resolve_types(fields, manual_overrides, default_type))
    }

    /// Seed explicit subscription fields without failing or waiting over five
    /// seconds for metadata. Cached types survive failed or timed-out lookups.
    pub(crate) async fn resolve(&self, fields: &[String]) -> HashMap<String, FieldKind> {
        // An empty default distinguishes missing metadata from a known string.
        // Timeout drops the pool request future and its cancellation guard.
        let types =
            match tokio::time::timeout(RESOLUTION_TIMEOUT, self.resolve_types(fields, None, ""))
                .await
            {
                Ok(Ok(types)) => types,
                _ => self.cache.resolve_types(fields, None, ""),
            };
        types
            .into_iter()
            .map(|(field, arrow_type)| (field, field_kind(&arrow_type)))
            .collect()
    }
}

fn field_kind(arrow_type: &str) -> FieldKind {
    match ArrowType::parse(arrow_type) {
        ArrowType::Float64 => FieldKind::F64,
        ArrowType::Int64 => FieldKind::I64,
        ArrowType::Int32 => FieldKind::I32,
        ArrowType::Bool => FieldKind::Bool,
        ArrowType::Date32 => FieldKind::Date32,
        ArrowType::Time64Micros => FieldKind::Time64Micros,
        ArrowType::TimestampMicros => FieldKind::TimestampMicros,
        ArrowType::String if arrow_type.eq_ignore_ascii_case("string") => FieldKind::Str,
        // ArrowType::parse deliberately defaults unsupported types to String;
        // a subscription must instead wait for an observed value to type them.
        ArrowType::String => FieldKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow_array::{ArrayRef, StringArray};

    use super::*;
    use crate::field_cache::FieldInfo;

    fn local_cache() -> (tempfile::TempDir, Arc<FieldTypeResolver>) {
        let directory = tempfile::tempdir().expect("temporary field cache directory");
        let cache = Arc::new(FieldTypeResolver::with_cache_path(
            directory.path().join("fields.json"),
        ));
        (directory, cache)
    }

    fn cache_field(cache: &FieldTypeResolver, field: &str, arrow_type: &str) {
        cache.insert(FieldInfo {
            field_id: field.to_string(),
            arrow_type: arrow_type.to_string(),
            description: String::new(),
            category: String::new(),
        });
    }

    fn metadata(rows: &[(&str, Option<&str>)]) -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "field",
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(field, _)| *field),
                )) as ArrayRef,
            ),
            (
                "type",
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|(_, arrow_type)| *arrow_type),
                )) as ArrayRef,
            ),
        ])
        .expect("synthetic field metadata")
    }

    #[tokio::test]
    async fn metadata_seeds_all_kinds_without_string_defaults_for_unknown_fields() {
        let (_directory, cache) = local_cache();
        let cases = [
            ("TEST_FLOAT", Some("DOUBLE"), FieldKind::F64),
            ("TEST_INT64", Some("integer"), FieldKind::I64),
            ("TEST_INT32", Some("i32"), FieldKind::I32),
            ("TEST_BOOL", Some("Boolean"), FieldKind::Bool),
            ("TEST_STRING", Some("STRING"), FieldKind::Str),
            ("TEST_DATE", Some("date"), FieldKind::Date32),
            ("TEST_TIME", Some("time64_us"), FieldKind::Time64Micros),
            (
                "TEST_TIMESTAMP",
                Some("timestamp_us"),
                FieldKind::TimestampMicros,
            ),
            ("TEST_UNSUPPORTED", Some("unsupported"), FieldKind::Unknown),
            ("TEST_REJECTED", None, FieldKind::Unknown),
        ];
        let batch = metadata(
            &cases
                .iter()
                .map(|(field, arrow_type, _)| (*field, *arrow_type))
                .collect::<Vec<_>>(),
        );
        let resolver = SubscriptionTypeResolver::with_query(cache, Handle::current(), move |_| {
            Box::pin(std::future::ready(Ok(batch.clone())))
        });
        let mut fields: Vec<String> = cases
            .iter()
            .map(|(field, _, _)| (*field).to_string())
            .collect();
        fields.push("TEST_MISSING".to_string());
        let mut expected: HashMap<String, FieldKind> = cases
            .iter()
            .map(|(field, _, kind)| ((*field).to_string(), *kind))
            .collect();
        expected.insert("TEST_MISSING".to_string(), FieldKind::Unknown);

        assert_eq!(resolver.resolve(&fields).await, expected);
    }

    #[tokio::test]
    async fn public_resolution_preserves_override_cache_metadata_and_default_precedence() {
        let (_directory, cache) = local_cache();
        cache_field(&cache, "TEST_CACHED", "date32");
        cache_field(&cache, "TEST_MANUAL", "string");
        let batch = metadata(&[("TEST_FETCHED", Some("time64"))]);
        let resolver = SubscriptionTypeResolver::with_query(cache, Handle::current(), move |_| {
            Box::pin(std::future::ready(Ok(batch.clone())))
        });
        let fields = [
            "test_cached",
            "test_manual",
            "test_override_only",
            "test_fetched",
            "test_absent",
        ]
        .map(str::to_string);
        let overrides = HashMap::from([
            ("test_manual".to_string(), "int32".to_string()),
            ("TEST_MANUAL".to_string(), "bool".to_string()),
            ("TEST_OVERRIDE_ONLY".to_string(), "int64".to_string()),
        ]);

        let types = resolver
            .resolve_types(&fields, Some(&overrides), "float64")
            .await
            .expect("best-effort type resolution");

        assert_eq!(
            types,
            HashMap::from([
                ("test_cached".to_string(), "date32".to_string()),
                ("test_manual".to_string(), "int32".to_string()),
                ("test_override_only".to_string(), "int64".to_string()),
                ("test_fetched".to_string(), "time64".to_string()),
                ("test_absent".to_string(), "float64".to_string()),
            ])
        );
    }

    #[tokio::test]
    async fn fetched_metadata_is_persisted_and_resolves_without_another_query() {
        let (directory, cache) = local_cache();
        let batch = metadata(&[("TEST_DATE", Some("date32")), ("TEST_TIME", Some("time64"))]);
        let resolver = SubscriptionTypeResolver::with_query(cache, Handle::current(), move |_| {
            Box::pin(std::future::ready(Ok(batch.clone())))
        });
        let fields = ["test_date".to_string(), "test_time".to_string()];
        resolver
            .resolve_types(&fields, None, "string")
            .await
            .expect("metadata query and persistence");

        let reloaded = Arc::new(FieldTypeResolver::with_cache_path(
            directory.path().join("fields.json"),
        ));
        let cached = SubscriptionTypeResolver::with_query(reloaded, Handle::current(), |_| {
            panic!("persisted metadata must not require a query")
        });
        assert_eq!(
            cached.resolve(&fields).await,
            HashMap::from([
                ("test_date".to_string(), FieldKind::Date32),
                ("test_time".to_string(), FieldKind::Time64Micros),
            ])
        );
    }

    #[tokio::test]
    async fn failed_lookup_preserves_cached_kinds_and_public_defaults() {
        let (_directory, cache) = local_cache();
        cache_field(&cache, "TEST_KNOWN", "time64");
        let resolver = SubscriptionTypeResolver::with_query(cache, Handle::current(), |_| {
            Box::pin(std::future::ready(Err(BlpAsyncError::ChannelClosed)))
        });
        let fields = ["test_known".to_string(), "TEST_UNKNOWN".to_string()];

        assert_eq!(
            resolver.resolve(&fields).await,
            HashMap::from([
                ("test_known".to_string(), FieldKind::Time64Micros),
                ("TEST_UNKNOWN".to_string(), FieldKind::Unknown),
            ])
        );
        assert_eq!(
            resolver
                .resolve_types(&fields, None, "float64")
                .await
                .expect("query failures keep public defaults"),
            HashMap::from([
                ("test_known".to_string(), "time64".to_string()),
                ("TEST_UNKNOWN".to_string(), "float64".to_string()),
            ])
        );
    }

    struct QueryCancelled(Arc<AtomicBool>);

    impl Drop for QueryCancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn lookup_deadline_preserves_cached_types_and_drops_unfinished_query() {
        let (_directory, cache) = local_cache();
        cache_field(&cache, "TEST_KNOWN", "date32");
        let cancelled = Arc::new(AtomicBool::new(false));
        let query_cancelled = Arc::clone(&cancelled);
        let resolver = SubscriptionTypeResolver::with_query(cache, Handle::current(), move |_| {
            let guard = QueryCancelled(Arc::clone(&query_cancelled));
            Box::pin(async move {
                let _guard = guard;
                std::future::pending::<Result<RecordBatch, BlpAsyncError>>().await
            })
        });
        let fields = ["TEST_KNOWN".to_string(), "TEST_UNKNOWN".to_string()];

        // One second of scheduling slack around the production five-second
        // deadline keeps this real-timer test independent of test-util features.
        let kinds = tokio::time::timeout(Duration::from_secs(6), resolver.resolve(&fields))
            .await
            .expect("metadata resolution must finish within its bounded wait");

        assert_eq!(
            kinds,
            HashMap::from([
                ("TEST_KNOWN".to_string(), FieldKind::Date32),
                ("TEST_UNKNOWN".to_string(), FieldKind::Unknown),
            ])
        );
        assert!(cancelled.load(Ordering::Acquire));
    }
}
