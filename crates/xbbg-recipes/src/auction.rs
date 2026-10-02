//! Primary-venue resolution and validated, typed auction snapshots.
//!
//! Routing lookups are cached process-wide for 12 hours, but every workflow still
//! validates its venue topics. Snapshot validation is part of the data request.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arrow_array::builder::{Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, NaiveDateTime, NaiveTime, Timelike};
use xbbg_async::engine::state::typed_builder::{ArrowType, TypedBuilder};
use xbbg_async::engine::{Engine, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::auction::{
    normalize_security_input, pfd_pricing_source, route_venue, validate_venue, VenueDecision,
    VenueReference, VenueStatus, DEFAULT,
};

use crate::error::Result;
use crate::identifiers::refdata_value_map;
use crate::utils::{naive_to_date32, parse_any_date, parse_f64_like};

type ReferenceValues = HashMap<String, HashMap<String, String>>;

const VENUE_CACHE_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const MAX_VENUE_CACHE_ENTRIES: usize = 4_096;

// Keep the canonical contents rather than a lossy hash: distinct override sets
// must never share routes, even when their hashes collide.
type PcsOverridesFingerprint = Arc<[(String, Option<String>)]>;

#[derive(Debug, PartialEq, Eq, Hash)]
struct VenueCacheKey {
    lookup: String,
    pcs_overrides: PcsOverridesFingerprint,
}

struct CachedVenue {
    decision: VenueDecision,
    expires_at: Instant,
}

struct VenueCache {
    entries: HashMap<Arc<VenueCacheKey>, CachedVenue>,
    insertion_order: VecDeque<Arc<VenueCacheKey>>,
    max_entries: usize,
    generation: u64,
}

impl VenueCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries,
            generation: 0,
        }
    }

    fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.entries.clear();
        self.insertion_order.clear();
    }

    fn expire(&mut self, now: Instant) {
        while self.insertion_order.front().is_some_and(|key| {
            self.entries
                .get(key)
                .is_none_or(|entry| now >= entry.expires_at)
        }) {
            if let Some(key) = self.insertion_order.pop_front() {
                self.entries.remove(&key);
            }
        }
    }

    fn record_validation(&mut self, rows: &[VenueRow], now: Instant) {
        self.expire(now);
        let planned_generation = self.generation;
        let mut failed_keys = HashSet::new();
        let mut invalidated = false;
        let mut validation_failed = false;
        for row in rows {
            if row.decision.status != VenueStatus::Resolved {
                failed_keys.insert(row.cache_key.as_ref());
                validation_failed |= row.decision.venue_topic.is_some();
                invalidated |= self.entries.remove(&row.cache_key).is_some();
            }
        }
        if validation_failed {
            // Invalidate outstanding cache writes even when this failed route
            // has no entry yet. A later success must not resurrect an older miss.
            self.generation = self.generation.wrapping_add(1);
        }
        if invalidated {
            // Remove invalidated keys as well as entries so the FIFO stays bounded.
            self.insertion_order
                .retain(|key| self.entries.contains_key(key));
        }
        if self.max_entries == 0 {
            return;
        }
        for row in rows {
            if row.decision.status != VenueStatus::Resolved
                || row.cache_hit
                || row.cache_generation != planned_generation
                || failed_keys.contains(row.cache_key.as_ref())
                || self.entries.contains_key(&row.cache_key)
            {
                continue;
            }
            if self.entries.len() == self.max_entries {
                if let Some(key) = self.insertion_order.pop_front() {
                    self.entries.remove(&key);
                }
            }
            self.insertion_order.push_back(Arc::clone(&row.cache_key));
            self.entries.insert(
                Arc::clone(&row.cache_key),
                CachedVenue {
                    decision: row.decision.clone(),
                    expires_at: now + VENUE_CACHE_TTL,
                },
            );
        }
    }
}

static VENUE_CACHE: LazyLock<Mutex<VenueCache>> =
    LazyLock::new(|| Mutex::new(VenueCache::new(MAX_VENUE_CACHE_ENTRIES)));

fn venue_cache() -> MutexGuard<'static, VenueCache> {
    VENUE_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clear the process-wide routing cache used by both auction recipes.
///
/// Only successfully validated routes are cached, for 12 hours without extending
/// their lifetime on hits. The cache keeps at most 4,096 routes, evicting the oldest
/// insertion first. Keys include the normalised lookup and effective PCS overrides.
/// Venue validation and snapshot data are always requested again, even on hits.
/// Clearing also prevents in-flight routing requests from repopulating the cache.
/// Their validated results are still returned normally, just without a cache write.
pub fn clear_venue_cache() {
    venue_cache().clear();
}

fn pcs_overrides_fingerprint(overrides: &HashMap<String, String>) -> PcsOverridesFingerprint {
    let mut fingerprint: Vec<_> = overrides
        .keys()
        .map(|name| {
            let mut exchange = String::with_capacity(name.len());
            for word in name.split_whitespace() {
                if !exchange.is_empty() {
                    exchange.push(' ');
                }
                exchange.extend(word.chars().flat_map(char::to_uppercase));
            }
            // Reuse the routing helper's value normalization and duplicate-key
            // precedence rather than changing what an override means for caching.
            (exchange, pfd_pricing_source(name, overrides))
        })
        .collect();
    fingerprint.sort_unstable();
    fingerprint.dedup();
    fingerprint.into()
}

struct VenueLookup {
    key: Arc<VenueCacheKey>,
    cached: Option<VenueDecision>,
    generation: u64,
}

fn venue_cache_keys(
    securities: &[String],
    pcs_overrides: &HashMap<String, String>,
) -> Vec<VenueCacheKey> {
    let fingerprint = pcs_overrides_fingerprint(pcs_overrides);
    securities
        .iter()
        .map(|security| VenueCacheKey {
            lookup: normalize_security_input(security),
            pcs_overrides: Arc::clone(&fingerprint),
        })
        .collect()
}

fn plan_venue_lookups(
    keys: Vec<VenueCacheKey>,
    cache: &mut VenueCache,
    now: Instant,
) -> Vec<VenueLookup> {
    cache.expire(now);
    keys.into_iter()
        .map(|key| match cache.entries.get_key_value(&key) {
            Some((key, entry)) => VenueLookup {
                key: Arc::clone(key),
                cached: Some(entry.decision.clone()),
                generation: cache.generation,
            },
            None => VenueLookup {
                key: Arc::new(key),
                cached: None,
                generation: cache.generation,
            },
        })
        .collect()
}

fn routing_lookups(lookups: &[VenueLookup]) -> Vec<String> {
    unique_strings(
        lookups
            .iter()
            .filter(|lookup| lookup.cached.is_none())
            .map(|lookup| lookup.key.lookup.as_str()),
    )
}

const ROUTING_FIELDS: &[&str] = &[
    "MARKET_SECTOR_DES",
    "TICKER",
    "EXCH_CODE",
    "COMPOSITE_EXCH_CODE",
    "EQY_PRIM_EXCH_SHRT",
    "ID_MIC_PRIM_EXCH",
    "PRICING_SOURCE",
    "ID_ISIN",
    "PARSEKYABLE_DES",
    "ID_BB_GLOBAL",
];

#[derive(Debug)]
struct VenueRow {
    input_order: i32,
    security: String,
    cache_key: Arc<VenueCacheKey>,
    cache_hit: bool,
    cache_generation: u64,
    decision: VenueDecision,
    venue_figi: Option<String>,
    exch_code: Option<String>,
    pricing_source: Option<String>,
}

/// Resolve securities to validated primary-venue topics, preserving input order and duplicates.
///
/// Unsupported or unresolved inputs remain rows with diagnostic status/error columns.
/// An empty input makes no requests; no venue request is sent if routing finds no candidates.
/// Successful routing lookups share a 12-hour process cache; venue validation is
/// never cached. Use [`clear_venue_cache`] to force fresh routing lookups.
pub async fn recipe_resolve_venues(
    engine: &Engine,
    securities: Vec<String>,
    pcs_overrides: HashMap<String, String>,
) -> Result<RecordBatch> {
    let mut rows = lookup_venues(engine, &securities, &pcs_overrides).await?;
    let topics = venue_topics(&rows);
    if !topics.is_empty() {
        let values = request_reference(
            engine,
            topics,
            ["PRICING_SOURCE", "EXCH_CODE", "ID_BB_GLOBAL"]
                .map(str::to_string)
                .to_vec(),
            None,
        )
        .await?;
        validate_rows(&mut rows, &values);
        venue_cache().record_validation(&rows, Instant::now());
    }
    build_resolve_venues_batch(&rows)
}

/// Fetch auction fields only from validated venues, keeping failed rows as null data.
///
/// Empty `fields` selects [`DEFAULT`]. Field types use the engine's cached
/// `//blp/apiflds` resolution. Time-only values retain Time64 microseconds; full
/// datetimes use UTC microsecond timestamps. Mixed/unparseable temporal values
/// retain their text rather than losing values or inventing a calendar date.
/// Routing lookups share the cache with [`recipe_resolve_venues`], while validation
/// and data are always fetched from the venue again.
pub async fn recipe_auction_snapshot(
    engine: &Engine,
    securities: Vec<String>,
    fields: Vec<String>,
    pcs_overrides: HashMap<String, String>,
) -> Result<RecordBatch> {
    let fields = snapshot_fields(fields);
    let mut rows = lookup_venues(engine, &securities, &pcs_overrides).await?;
    let field_types = engine.resolve_field_types(&fields, None, "string").await?;
    let topics = venue_topics(&rows);
    let values = if topics.is_empty() {
        ReferenceValues::new()
    } else {
        let mut request_fields = unique_strings(fields.iter().map(String::as_str));
        for field in ["PRICING_SOURCE", "EXCH_CODE"] {
            if !request_fields
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(field))
            {
                request_fields.push(field.to_string());
            }
        }
        let mut request_types = field_types.clone();
        // The validation strings also force the refdata long `value` column to
        // remain text, preserving Y/N and time-only values for final coercion.
        for field in &request_fields {
            if field.eq_ignore_ascii_case("PRICING_SOURCE")
                || field.eq_ignore_ascii_case("EXCH_CODE")
            {
                request_types.insert(field.clone(), "string".to_string());
            }
        }
        let values = request_reference(engine, topics, request_fields, Some(request_types)).await?;
        validate_rows(&mut rows, &values);
        venue_cache().record_validation(&rows, Instant::now());
        values
    };
    build_snapshot_batch(&rows, &fields, &field_types, &values)
}

fn snapshot_fields(fields: Vec<String>) -> Vec<String> {
    if fields.is_empty() {
        DEFAULT.iter().map(|field| (*field).to_string()).collect()
    } else {
        fields
    }
}

async fn request_reference(
    engine: &Engine,
    securities: Vec<String>,
    fields: Vec<String>,
    field_types: Option<HashMap<String, String>>,
) -> Result<ReferenceValues> {
    let batch = engine
        .request(RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            securities: Some(securities),
            fields: Some(fields),
            field_types,
            format: Some("long".to_string()),
            // This keeps all-rejected requests representable as unresolved rows.
            include_security_errors: true,
            ..Default::default()
        })
        .await?;
    refdata_value_map(&batch)
}

async fn lookup_venues(
    engine: &Engine,
    securities: &[String],
    pcs_overrides: &HashMap<String, String>,
) -> Result<Vec<VenueRow>> {
    // Input and override normalization can be expensive; do it before taking
    // the process-wide mutex, which protects only expiry/probes and cache updates.
    let keys = venue_cache_keys(securities, pcs_overrides);
    let lookups = plan_venue_lookups(keys, &mut venue_cache(), Instant::now());
    let missing = routing_lookups(&lookups);
    let values = if missing.is_empty() {
        ReferenceValues::new()
    } else {
        request_reference(
            engine,
            missing,
            ROUTING_FIELDS
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            None,
        )
        .await?
    };
    Ok(build_venue_rows(
        securities,
        lookups,
        &values,
        pcs_overrides,
    ))
}

fn unique_strings<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .filter(|value| seen.insert(*value))
        .map(str::to_string)
        .collect()
}

fn venue_topics(rows: &[VenueRow]) -> Vec<String> {
    unique_strings(
        rows.iter()
            .filter_map(|row| row.decision.venue_topic.as_deref()),
    )
}

fn reference(fields: Option<&HashMap<String, String>>) -> VenueReference<'_> {
    let get = |field| fields.and_then(|values| values.get(field).map(String::as_str));
    VenueReference {
        market_sector_des: get("MARKET_SECTOR_DES"),
        ticker: get("TICKER"),
        exch_code: get("EXCH_CODE"),
        composite_exch_code: get("COMPOSITE_EXCH_CODE"),
        eqy_prim_exch_shrt: get("EQY_PRIM_EXCH_SHRT"),
        id_mic_prim_exch: get("ID_MIC_PRIM_EXCH"),
        pricing_source: get("PRICING_SOURCE"),
        id_isin: get("ID_ISIN"),
        parsekyable_des: get("PARSEKYABLE_DES"),
    }
}

fn build_venue_rows(
    securities: &[String],
    lookups: Vec<VenueLookup>,
    values: &ReferenceValues,
    pcs_overrides: &HashMap<String, String>,
) -> Vec<VenueRow> {
    securities
        .iter()
        .zip(lookups)
        .enumerate()
        .map(|(index, (security, lookup))| {
            let cache_hit = lookup.cached.is_some();
            let decision = lookup.cached.unwrap_or_else(|| {
                route_venue(
                    &lookup.key.lookup,
                    &reference(values.get(&lookup.key.lookup)),
                    pcs_overrides,
                )
            });
            VenueRow {
                input_order: index as i32,
                security: security.clone(),
                cache_key: lookup.key,
                cache_hit,
                cache_generation: lookup.generation,
                decision,
                venue_figi: None,
                exch_code: None,
                pricing_source: None,
            }
        })
        .collect()
}

fn validate_rows(rows: &mut [VenueRow], values: &ReferenceValues) {
    for row in rows {
        let fields = row
            .decision
            .venue_topic
            .as_ref()
            .and_then(|topic| values.get(topic));
        validate_venue(&mut row.decision, &reference(fields));
        if row.decision.status == VenueStatus::Resolved {
            row.venue_figi = fields
                .and_then(|fields| fields.get("ID_BB_GLOBAL"))
                .cloned();
            row.exch_code = fields.and_then(|fields| fields.get("EXCH_CODE")).cloned();
            row.pricing_source = fields
                .and_then(|fields| fields.get("PRICING_SOURCE"))
                .cloned();
        }
    }
}

fn build_resolve_venues_batch(rows: &[VenueRow]) -> Result<RecordBatch> {
    let mut order = Int32Builder::with_capacity(rows.len());
    let mut security = StringBuilder::new();
    let mut lookup = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut composite = StringBuilder::new();
    let mut venue_topic = StringBuilder::new();
    let mut venue_figi = StringBuilder::new();
    let mut method = StringBuilder::new();
    let mut exch_code = StringBuilder::new();
    let mut mic = StringBuilder::new();
    let mut pricing_source = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut error = StringBuilder::new();
    for row in rows {
        order.append_value(row.input_order);
        security.append_value(&row.security);
        lookup.append_value(&row.cache_key.lookup);
        kind.append_value(&row.decision.kind);
        composite.append_option(row.decision.composite.as_deref());
        venue_topic.append_option(row.decision.venue_topic.as_deref());
        venue_figi.append_option(row.venue_figi.as_deref());
        method.append_option(row.decision.method.map(|method| method.as_str()));
        exch_code.append_option(row.exch_code.as_deref());
        mic.append_option(row.decision.mic.as_deref());
        pricing_source.append_option(row.pricing_source.as_deref());
        status.append_value(row.decision.status.as_str());
        error.append_option(row.decision.error.as_deref());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("security", DataType::Utf8, false),
        Field::new("lookup", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("composite", DataType::Utf8, true),
        Field::new("venue_topic", DataType::Utf8, true),
        Field::new("venue_figi", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("exch_code", DataType::Utf8, true),
        Field::new("mic", DataType::Utf8, true),
        Field::new("pricing_source", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(order.finish()),
            Arc::new(security.finish()),
            Arc::new(lookup.finish()),
            Arc::new(kind.finish()),
            Arc::new(composite.finish()),
            Arc::new(venue_topic.finish()),
            Arc::new(venue_figi.finish()),
            Arc::new(method.finish()),
            Arc::new(exch_code.finish()),
            Arc::new(mic.finish()),
            Arc::new(pricing_source.finish()),
            Arc::new(status.finish()),
            Arc::new(error.finish()),
        ],
    )
    .map_err(Into::into)
}

fn build_snapshot_batch(
    rows: &[VenueRow],
    fields: &[String],
    field_types: &HashMap<String, String>,
    values: &ReferenceValues,
) -> Result<RecordBatch> {
    let mut order = Int32Builder::with_capacity(rows.len());
    let mut security = StringBuilder::new();
    let mut venue_topic = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut error = StringBuilder::new();
    let data: Vec<_> = rows
        .iter()
        .map(|row| {
            (row.decision.status == VenueStatus::Resolved)
                .then(|| {
                    row.decision
                        .venue_topic
                        .as_ref()
                        .and_then(|topic| values.get(topic))
                })
                .flatten()
        })
        .collect();
    for row in rows {
        order.append_value(row.input_order);
        security.append_value(&row.security);
        venue_topic.append_option(row.decision.venue_topic.as_deref());
        status.append_value(row.decision.status.as_str());
        error.append_option(row.decision.error.as_deref());
    }
    let mut schema = vec![
        Field::new("input_order", DataType::Int32, false),
        Field::new("security", DataType::Utf8, false),
        Field::new("venue_topic", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(order.finish()),
        Arc::new(security.finish()),
        Arc::new(venue_topic.finish()),
        Arc::new(status.finish()),
        Arc::new(error.finish()),
    ];
    for field in fields {
        let key = field.to_ascii_uppercase();
        let hint = field_types
            .get(field)
            .or_else(|| field_types.get(&key))
            .map(String::as_str)
            .unwrap_or("string");
        let mut arrow_type = ArrowType::parse(hint);
        if arrow_type == ArrowType::Int32 {
            arrow_type = ArrowType::Int64;
        }
        if matches!(
            arrow_type,
            ArrowType::TimestampMicros | ArrowType::Time64Micros
        ) {
            arrow_type = temporal_column_type(
                arrow_type,
                data.iter().filter_map(|fields| {
                    fields.and_then(|fields| fields.get(&key).map(String::as_str))
                }),
            );
        }
        let mut builder = TypedBuilder::new(arrow_type);
        for fields in &data {
            append_field_value(
                &mut builder,
                fields.and_then(|fields| fields.get(&key).map(String::as_str)),
            );
        }
        schema.push(Field::new(field, builder.data_type(), true));
        arrays.push(builder.finish());
    }
    RecordBatch::try_new(Arc::new(Schema::new(schema)), arrays).map_err(Into::into)
}

fn temporal_column_type<'a>(hint: ArrowType, values: impl Iterator<Item = &'a str>) -> ArrowType {
    let mut observed = None;
    for value in values {
        let next = if parse_time_micros(value).is_some() {
            ArrowType::Time64Micros
        } else if parse_timestamp_micros(value).is_some() {
            ArrowType::TimestampMicros
        } else {
            return ArrowType::String;
        };
        if observed.is_some_and(|previous| previous != next) {
            return ArrowType::String;
        }
        observed = Some(next);
    }
    observed.unwrap_or(hint)
}

fn append_field_value(builder: &mut TypedBuilder, value: Option<&str>) {
    let Some(value) = value else {
        builder.append_null();
        return;
    };
    match builder {
        TypedBuilder::Float64(builder) => builder.append_option(parse_f64_like(value)),
        TypedBuilder::Int64(builder) => builder.append_option(value.parse::<i64>().ok()),
        TypedBuilder::Int32(builder) => builder.append_option(value.parse::<i32>().ok()),
        TypedBuilder::String(builder) => builder.append_value(value),
        TypedBuilder::Bool(builder) => builder.append_option(parse_bool(value)),
        TypedBuilder::Date32(builder) => {
            builder.append_option(parse_any_date(value).map(naive_to_date32))
        }
        TypedBuilder::TimestampMicros(builder) => {
            builder.append_option(parse_timestamp_micros(value))
        }
        TypedBuilder::Time64Micros(builder) => builder.append_option(parse_time_micros(value)),
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("Y") || value.eq_ignore_ascii_case("true") {
        Some(true)
    } else if value.eq_ignore_ascii_case("N") || value.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn parse_time_micros(value: &str) -> Option<i64> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M"))
        .ok()?;
    Some(
        i64::from(time.num_seconds_from_midnight()) * 1_000_000
            + i64::from(time.nanosecond() / 1_000),
    )
}

fn parse_timestamp_micros(value: &str) -> Option<i64> {
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Some(datetime.timestamp_micros());
    }
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(value, format).ok())
        .map(|datetime| datetime.and_utc().timestamp_micros())
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
        Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::TimeUnit;
    use xbbg_async::field_cache::BlpFieldType;

    use super::*;
    use crate::utils::as_string_col;

    fn fields(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn metadata_types(metadata: &[(&str, Option<&str>, Option<&str>)]) -> HashMap<String, String> {
        metadata
            .iter()
            .map(|(name, datatype, ftype)| {
                (
                    (*name).to_string(),
                    BlpFieldType::from_metadata(*datatype, *ftype)
                        .to_arrow_type_str()
                        .to_string(),
                )
            })
            .collect()
    }

    fn uncached_rows(
        securities: &[String],
        values: &ReferenceValues,
        overrides: &HashMap<String, String>,
    ) -> Vec<VenueRow> {
        let lookups = plan_venue_lookups(
            venue_cache_keys(securities, overrides),
            &mut VenueCache::new(0),
            Instant::now(),
        );
        build_venue_rows(securities, lookups, values, overrides)
    }

    fn synthetic_isin() -> String {
        (0..10)
            .map(|digit| format!("ZZ000000001{digit}"))
            .find(|isin| xbbg_ext::is_valid_isin(isin))
            .unwrap()
    }

    fn equity_fields(ticker: &str, primary: &str) -> HashMap<String, String> {
        fields(&[
            ("MARKET_SECTOR_DES", "Equity"),
            ("TICKER", ticker),
            ("EXCH_CODE", "ZZ"),
            ("COMPOSITE_EXCH_CODE", "ZZ"),
            ("EQY_PRIM_EXCH_SHRT", primary),
        ])
    }

    fn seed_equity(cache: &mut VenueCache, ticker: &str, primary: &str, now: Instant) {
        let securities = [format!("{ticker} ZZ Equity")];
        let overrides = HashMap::new();
        let lookups = plan_venue_lookups(venue_cache_keys(&securities, &overrides), cache, now);
        let routing = HashMap::from([(securities[0].clone(), equity_fields(ticker, primary))]);
        let mut rows = build_venue_rows(&securities, lookups, &routing, &overrides);
        validate_rows(
            &mut rows,
            &HashMap::from([(
                format!("{ticker} {primary} Equity"),
                fields(&[("EXCH_CODE", primary), ("ID_BB_GLOBAL", "SYNTH_OLD")]),
            )]),
        );
        cache.record_validation(&rows, now);
    }

    #[test]
    fn venue_cache_mixed_hits_keep_order_duplicates_and_fresh_data() {
        let now = Instant::now();
        let mut cache = VenueCache::new(8);
        seed_equity(&mut cache, "SYNTH_A", "Z1", now);
        let securities = [
            " SYNTH_A ZZ Equity ",
            "SYNTH_B ZZ Equity",
            "SYNTH_A ZZ Equity",
        ]
        .map(str::to_string);
        let overrides = HashMap::new();
        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert_eq!(routing_lookups(&lookups), ["SYNTH_B ZZ Equity"]);
        let routing = HashMap::from([(
            "SYNTH_B ZZ Equity".to_string(),
            equity_fields("SYNTH_B", "Z2"),
        )]);
        let mut rows = build_venue_rows(&securities, lookups, &routing, &overrides);
        assert_eq!(
            venue_topics(&rows),
            ["SYNTH_A Z1 Equity", "SYNTH_B Z2 Equity"]
        );
        let data = HashMap::from([
            (
                "SYNTH_A Z1 Equity".to_string(),
                fields(&[
                    ("EXCH_CODE", "Z1"),
                    ("ID_BB_GLOBAL", "SYNTH_NEW"),
                    ("REFERENCE_PRICE_RT", "101.25"),
                ]),
            ),
            (
                "SYNTH_B Z2 Equity".to_string(),
                fields(&[
                    ("EXCH_CODE", "Z2"),
                    ("ID_BB_GLOBAL", "SYNTH_B_NEW"),
                    ("REFERENCE_PRICE_RT", "202.5"),
                ]),
            ),
        ]);
        validate_rows(&mut rows, &data);
        cache.record_validation(&rows, now);
        let venues = build_resolve_venues_batch(&rows).unwrap();
        assert_eq!(
            as_string_col(&venues, "security")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            securities
                .iter()
                .map(|value| Some(value.as_str()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            venues
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1, 2]
        );
        assert_eq!(
            as_string_col(&venues, "venue_figi")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("SYNTH_NEW"), Some("SYNTH_B_NEW"), Some("SYNTH_NEW")]
        );
        let batch = build_snapshot_batch(
            &rows,
            &["REFERENCE_PRICE_RT".to_string()],
            &metadata_types(&[("REFERENCE_PRICE_RT", Some("Double"), None)]),
            &data,
        )
        .unwrap();
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(101.25), Some(202.5), Some(101.25)]
        );

        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert!(routing_lookups(&lookups).is_empty());
        let rows = build_venue_rows(&securities, lookups, &HashMap::new(), &overrides);
        assert_eq!(
            venue_topics(&rows),
            ["SYNTH_A Z1 Equity", "SYNTH_B Z2 Equity"]
        );
    }

    #[test]
    fn venue_cache_expires_at_twelve_hours_without_renewing_hits() {
        let now = Instant::now();
        let mut cache = VenueCache::new(2);
        seed_equity(&mut cache, "SYNTH_A", "Z1", now);
        let securities = ["SYNTH_A ZZ Equity".to_string()];
        let overrides = HashMap::new();
        let before_expiry = now + VENUE_CACHE_TTL - Duration::from_nanos(1);
        let lookups = plan_venue_lookups(
            venue_cache_keys(&securities, &overrides),
            &mut cache,
            before_expiry,
        );
        assert!(routing_lookups(&lookups).is_empty());
        let mut rows = build_venue_rows(&securities, lookups, &HashMap::new(), &overrides);
        let validation = HashMap::from([(
            "SYNTH_A Z1 Equity".to_string(),
            fields(&[("EXCH_CODE", "Z1")]),
        )]);
        validate_rows(&mut rows, &validation);
        cache.record_validation(&rows, before_expiry);
        let expiry = now + VENUE_CACHE_TTL;
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                expiry,
            )),
            securities
        );
        // A hit whose validation finishes after expiry must not resurrect the old lookup.
        cache.record_validation(&rows, expiry);
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                expiry + Duration::from_nanos(1),
            )),
            securities
        );
    }

    #[test]
    fn venue_cache_normalizes_lookups_and_effective_pcs_overrides() {
        let now = Instant::now();
        let mut cache = VenueCache::new(8);
        let isin = synthetic_isin();
        let lookup = normalize_security_input(&isin);
        let securities = [format!(" {isin} ")];
        let overrides = fields(&[
            (" synth\t venue ", " pcs_one "),
            ("OTHER VENUE", "PCS_UNUSED"),
        ]);
        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert_eq!(routing_lookups(&lookups), std::slice::from_ref(&lookup));
        let routing = HashMap::from([(
            lookup.clone(),
            fields(&[
                ("MARKET_SECTOR_DES", "Pfd"),
                ("EXCH_CODE", "SYNTH VENUE"),
                ("PRICING_SOURCE", "EXCH"),
                ("ID_ISIN", &isin),
            ]),
        )]);
        let mut rows = build_venue_rows(&securities, lookups, &routing, &overrides);
        let topic = format!("/isin/{isin}@PCS_ONE");
        validate_rows(
            &mut rows,
            &HashMap::from([(topic.clone(), fields(&[("PRICING_SOURCE", "PCS_ONE")]))]),
        );
        cache.record_validation(&rows, now);
        let canonical = fields(&[("other  venue", "pcs_unused"), ("SYNTH VENUE", "PCS_ONE")]);
        let mut aliases = canonical.clone();
        aliases.insert(" synth\t venue ".to_string(), "PCS_OTHER".to_string());
        for equivalent in [&canonical, &aliases] {
            let lookups = plan_venue_lookups(
                venue_cache_keys(std::slice::from_ref(&lookup), equivalent),
                &mut cache,
                now,
            );
            assert!(routing_lookups(&lookups).is_empty());
            let rows = build_venue_rows(
                std::slice::from_ref(&lookup),
                lookups,
                &HashMap::new(),
                equivalent,
            );
            assert_eq!(venue_topics(&rows), std::slice::from_ref(&topic));
        }
        let changed = fields(&[("SYNTH VENUE", "PCS_TWO"), ("OTHER VENUE", "PCS_UNUSED")]);
        let lookups = plan_venue_lookups(venue_cache_keys(&securities, &changed), &mut cache, now);
        assert_eq!(routing_lookups(&lookups), std::slice::from_ref(&lookup));
        let rows = build_venue_rows(&securities, lookups, &routing, &changed);
        assert_eq!(venue_topics(&rows), [format!("/isin/{isin}@PCS_TWO")]);
        for isolated in [
            HashMap::new(),
            fields(&[("SYNTH VENUE", "PCS_ONE")]),
            fields(&[("SYNTH VENUE", "N/A"), ("OTHER VENUE", "PCS_UNUSED")]),
        ] {
            assert_eq!(
                routing_lookups(&plan_venue_lookups(
                    venue_cache_keys(&securities, &isolated),
                    &mut cache,
                    now,
                )),
                std::slice::from_ref(&lookup)
            );
        }
    }

    #[test]
    fn venue_cache_does_not_store_unvalidated_unresolved_or_unsupported_routes() {
        let now = Instant::now();
        let mut cache = VenueCache::new(8);
        let isin = synthetic_isin();
        let preferred = normalize_security_input(&isin);
        let securities = vec![
            "SYNTH_MISSING".to_string(),
            "SYNTH_INCOMPLETE ZZ Equity".to_string(),
            "SYNTH_UNSUPPORTED Corp".to_string(),
            preferred.clone(),
            "SYNTH_READY ZZ Equity".to_string(),
        ];
        let overrides = HashMap::new();
        let routing = HashMap::from([
            (
                securities[1].clone(),
                fields(&[
                    ("MARKET_SECTOR_DES", "Equity"),
                    ("TICKER", "SYNTH_INCOMPLETE"),
                ]),
            ),
            (
                securities[2].clone(),
                fields(&[("MARKET_SECTOR_DES", "Corp")]),
            ),
            (
                preferred,
                fields(&[
                    ("MARKET_SECTOR_DES", "Pfd"),
                    ("EXCH_CODE", "SYNTH_UNMAPPED"),
                    ("PRICING_SOURCE", "EXCH"),
                    ("ID_ISIN", &isin),
                ]),
            ),
            (securities[4].clone(), equity_fields("SYNTH_READY", "Z1")),
        ]);
        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        let mut rows = build_venue_rows(&securities, lookups, &routing, &overrides);
        assert_eq!(venue_topics(&rows), ["SYNTH_READY Z1 Equity"]);
        cache.record_validation(&rows, now);
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                now,
            )),
            securities
        );
        validate_rows(&mut rows, &HashMap::new());
        assert_eq!(
            rows.iter()
                .map(|row| row.decision.status)
                .collect::<Vec<_>>(),
            [
                VenueStatus::Unresolved,
                VenueStatus::Unresolved,
                VenueStatus::Unsupported,
                VenueStatus::Unsupported,
                VenueStatus::Unresolved,
            ]
        );
        cache.record_validation(&rows, now);
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                now,
            )),
            securities
        );
    }

    #[test]
    fn venue_cache_evicts_failed_validation_without_leaking_data_or_other_overrides() {
        let now = Instant::now();
        let securities = ["SYNTH_A ZZ Equity".to_string()];
        let overrides = HashMap::new();
        let other_overrides = fields(&[("SYNTH VENUE", "PCS_OTHER")]);
        for (exchange, expected) in [
            (Some("ZZ"), VenueStatus::Mismatch),
            (None, VenueStatus::Unresolved),
        ] {
            let mut cache = VenueCache::new(8);
            seed_equity(&mut cache, "SYNTH_A", "Z1", now);
            let lookups = plan_venue_lookups(
                venue_cache_keys(&securities, &other_overrides),
                &mut cache,
                now,
            );
            let mut other_rows = build_venue_rows(
                &securities,
                lookups,
                &HashMap::from([(securities[0].clone(), equity_fields("SYNTH_A", "Z1"))]),
                &other_overrides,
            );
            validate_rows(
                &mut other_rows,
                &HashMap::from([(
                    "SYNTH_A Z1 Equity".to_string(),
                    fields(&[("EXCH_CODE", "Z1")]),
                )]),
            );
            cache.record_validation(&other_rows, now);
            let lookups =
                plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
            assert!(routing_lookups(&lookups).is_empty());
            let mut rows = build_venue_rows(&securities, lookups, &HashMap::new(), &overrides);
            let mut values = fields(&[("REFERENCE_PRICE_RT", "999")]);
            if let Some(exchange) = exchange {
                values.insert("EXCH_CODE".to_string(), exchange.to_string());
            }
            let data = HashMap::from([("SYNTH_A Z1 Equity".to_string(), values)]);
            validate_rows(&mut rows, &data);
            cache.record_validation(&rows, now);
            assert_eq!(rows[0].decision.status, expected);
            let snapshot = build_snapshot_batch(
                &rows,
                &["REFERENCE_PRICE_RT".to_string()],
                &metadata_types(&[("REFERENCE_PRICE_RT", Some("Double"), None)]),
                &data,
            )
            .unwrap();
            assert!(snapshot.column(5).is_null(0));
            let venues = build_resolve_venues_batch(&rows).unwrap();
            assert!(venues.column_by_name("venue_figi").unwrap().is_null(0));
            assert_eq!(
                routing_lookups(&plan_venue_lookups(
                    venue_cache_keys(&securities, &overrides),
                    &mut cache,
                    now,
                )),
                securities
            );
            assert!(routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &other_overrides),
                &mut cache,
                now,
            ))
            .is_empty());
        }
    }

    #[test]
    fn venue_cache_is_bounded_and_explicit_clear_forces_fresh_lookups() {
        let now = Instant::now();
        let mut cache = VenueCache::new(2);
        seed_equity(&mut cache, "SYNTH_A", "Z1", now);
        seed_equity(&mut cache, "SYNTH_B", "Z2", now + Duration::from_secs(1));
        seed_equity(&mut cache, "SYNTH_C", "Z3", now + Duration::from_secs(2));
        let securities = [
            "SYNTH_A ZZ Equity",
            "SYNTH_B ZZ Equity",
            "SYNTH_C ZZ Equity",
        ]
        .map(str::to_string);
        let overrides = HashMap::new();
        let later = now + Duration::from_secs(3);
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                later,
            )),
            ["SYNTH_A ZZ Equity"]
        );
        cache.clear();
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                later,
            )),
            securities
        );
    }

    #[test]
    fn venue_cache_clear_prevents_an_older_miss_from_repopulating() {
        let now = Instant::now();
        let mut cache = VenueCache::new(2);
        let securities = ["SYNTH_A ZZ Equity".to_string()];
        let overrides = HashMap::new();
        let pending =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert_eq!(routing_lookups(&pending), securities);
        let routing = HashMap::from([(securities[0].clone(), equity_fields("SYNTH_A", "Z1"))]);
        let mut rows = build_venue_rows(&securities, pending, &routing, &overrides);

        // The request has missed and is in flight when clear completes.
        cache.clear();
        let later = now + Duration::from_secs(1);
        let data = HashMap::from([(
            "SYNTH_A Z1 Equity".to_string(),
            fields(&[("EXCH_CODE", "Z1"), ("REFERENCE_PRICE_RT", "101.25")]),
        )]);
        validate_rows(&mut rows, &data);
        cache.record_validation(&rows, later);
        let snapshot = build_snapshot_batch(
            &rows,
            &["REFERENCE_PRICE_RT".to_string()],
            &metadata_types(&[("REFERENCE_PRICE_RT", Some("Double"), None)]),
            &data,
        )
        .unwrap();
        assert_eq!(
            as_string_col(&snapshot, "status").unwrap().value(0),
            "resolved"
        );
        assert_eq!(
            snapshot
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(101.25)]
        );
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                later,
            )),
            securities
        );
        seed_equity(&mut cache, "SYNTH_A", "Z1", later);
        assert!(routing_lookups(&plan_venue_lookups(
            venue_cache_keys(&securities, &overrides),
            &mut cache,
            later,
        ))
        .is_empty());
    }

    #[test]
    fn venue_cache_failed_validation_prevents_an_older_miss_from_repopulating() {
        let now = Instant::now();
        let mut cache = VenueCache::new(2);
        let securities = ["SYNTH_A ZZ Equity".to_string()];
        let overrides = HashMap::new();
        let pending =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        let newer = plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert_eq!(routing_lookups(&pending), securities);
        assert_eq!(routing_lookups(&newer), securities);
        let routing = HashMap::from([(securities[0].clone(), equity_fields("SYNTH_A", "Z1"))]);
        let mut newer_rows = build_venue_rows(&securities, newer, &routing, &overrides);
        validate_rows(
            &mut newer_rows,
            &HashMap::from([(
                "SYNTH_A Z1 Equity".to_string(),
                fields(&[("EXCH_CODE", "ZZ")]),
            )]),
        );
        assert_eq!(newer_rows[0].decision.status, VenueStatus::Mismatch);
        cache.record_validation(&newer_rows, now);

        // No cache entry existed to remove, but the newer mismatch must still
        // invalidate the outstanding older lookup's eventual cache write.
        let mut older_rows = build_venue_rows(&securities, pending, &routing, &overrides);
        let data = HashMap::from([(
            "SYNTH_A Z1 Equity".to_string(),
            fields(&[("EXCH_CODE", "Z1"), ("REFERENCE_PRICE_RT", "101.25")]),
        )]);
        validate_rows(&mut older_rows, &data);
        let later = now + Duration::from_secs(1);
        cache.record_validation(&older_rows, later);
        let snapshot = build_snapshot_batch(
            &older_rows,
            &["REFERENCE_PRICE_RT".to_string()],
            &metadata_types(&[("REFERENCE_PRICE_RT", Some("Double"), None)]),
            &data,
        )
        .unwrap();
        assert_eq!(
            as_string_col(&snapshot, "status").unwrap().value(0),
            "resolved"
        );
        assert_eq!(
            snapshot
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(101.25)]
        );
        assert_eq!(
            routing_lookups(&plan_venue_lookups(
                venue_cache_keys(&securities, &overrides),
                &mut cache,
                later,
            )),
            securities
        );
    }

    #[test]
    fn venue_cache_keeps_successes_when_another_venue_in_the_batch_mismatches() {
        let now = Instant::now();
        let mut cache = VenueCache::new(2);
        let securities = ["SYNTH_A ZZ Equity", "SYNTH_B ZZ Equity"].map(str::to_string);
        let overrides = HashMap::new();
        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        let routing = HashMap::from([
            (securities[0].clone(), equity_fields("SYNTH_A", "Z1")),
            (securities[1].clone(), equity_fields("SYNTH_B", "Z2")),
        ]);
        let mut rows = build_venue_rows(&securities, lookups, &routing, &overrides);
        validate_rows(
            &mut rows,
            &HashMap::from([
                (
                    "SYNTH_A Z1 Equity".to_string(),
                    fields(&[("EXCH_CODE", "Z1")]),
                ),
                (
                    "SYNTH_B Z2 Equity".to_string(),
                    fields(&[("EXCH_CODE", "ZZ")]),
                ),
            ]),
        );
        assert_eq!(rows[0].decision.status, VenueStatus::Resolved);
        assert_eq!(rows[1].decision.status, VenueStatus::Mismatch);
        cache.record_validation(&rows, now);
        let lookups =
            plan_venue_lookups(venue_cache_keys(&securities, &overrides), &mut cache, now);
        assert_eq!(routing_lookups(&lookups), ["SYNTH_B ZZ Equity"]);
        let cached = build_venue_rows(&securities, lookups, &HashMap::new(), &overrides);
        assert_eq!(
            cached[0].decision.venue_topic.as_deref(),
            Some("SYNTH_A Z1 Equity")
        );
        assert!(cached[1].decision.venue_topic.is_none());
    }

    fn fixture() -> (Vec<VenueRow>, ReferenceValues) {
        let securities = vec![
            " IBM US Equity ",
            "UNKNOWN",
            "AAPL US Equity",
            "IBM US Equity",
            "MSFT US Equity",
            "SYNTHETIC Corp",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let values = HashMap::from([
            (
                "IBM US Equity".to_string(),
                fields(&[
                    ("MARKET_SECTOR_DES", "Equity"),
                    ("TICKER", "IBM"),
                    ("EXCH_CODE", "US"),
                    ("COMPOSITE_EXCH_CODE", "US"),
                    ("EQY_PRIM_EXCH_SHRT", "UN"),
                    ("ID_MIC_PRIM_EXCH", "XNYS"),
                ]),
            ),
            (
                "AAPL US Equity".to_string(),
                fields(&[
                    ("MARKET_SECTOR_DES", "Equity"),
                    ("TICKER", "AAPL"),
                    ("EXCH_CODE", "US"),
                    ("COMPOSITE_EXCH_CODE", "US"),
                    ("EQY_PRIM_EXCH_SHRT", "UW"),
                ]),
            ),
            (
                "MSFT US Equity".to_string(),
                fields(&[
                    ("MARKET_SECTOR_DES", "Equity"),
                    ("TICKER", "MSFT"),
                    ("EXCH_CODE", "US"),
                    ("COMPOSITE_EXCH_CODE", "US"),
                    ("EQY_PRIM_EXCH_SHRT", "UW"),
                ]),
            ),
            (
                "SYNTHETIC Corp".to_string(),
                fields(&[("MARKET_SECTOR_DES", "Corp")]),
            ),
        ]);
        let mut rows = uncached_rows(&securities, &values, &HashMap::new());
        let venue_values = HashMap::from([
            (
                "IBM UN Equity".to_string(),
                fields(&[
                    ("EXCH_CODE", "UN"),
                    ("PRICING_SOURCE", "UN"),
                    ("ID_BB_GLOBAL", "BBG000TEST001"),
                    ("ORDER_IMB_BUY_VOLUME", "42"),
                    ("IN_AUCTION_RT", "Y"),
                    ("REFERENCE_PRICE_RT", "101.25"),
                    ("CLOSING_AUCTION_VOLUME_DATE_RT", "2026-01-02"),
                    ("IMBALANCE_INDIC_RT", "BUY"),
                    ("IMBALANCE_TIMESTAMP_RT", "15:59:01.123456"),
                ]),
            ),
            (
                "AAPL UW Equity".to_string(),
                fields(&[
                    ("EXCH_CODE", "US"),
                    ("PRICING_SOURCE", "US"),
                    ("ID_BB_GLOBAL", "BBG000TEST002"),
                    ("ORDER_IMB_BUY_VOLUME", "900"),
                    ("IN_AUCTION_RT", "N"),
                    ("REFERENCE_PRICE_RT", "999.0"),
                ]),
            ),
            (
                "MSFT UW Equity".to_string(),
                fields(&[("ORDER_IMB_BUY_VOLUME", "800"), ("IN_AUCTION_RT", "Y")]),
            ),
        ]);
        validate_rows(&mut rows, &venue_values);
        (rows, venue_values)
    }

    #[test]
    fn venue_batch_preserves_order_duplicates_and_failure_nulls() {
        let (rows, _) = fixture();
        let batch = build_resolve_venues_batch(&rows).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "input_order",
                "security",
                "lookup",
                "kind",
                "composite",
                "venue_topic",
                "venue_figi",
                "method",
                "exch_code",
                "mic",
                "pricing_source",
                "status",
                "error"
            ]
        );
        let order = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(order.values().as_ref(), &[0, 1, 2, 3, 4, 5]);
        assert_eq!(
            as_string_col(&batch, "security").unwrap().value(0),
            " IBM US Equity "
        );
        assert_eq!(
            as_string_col(&batch, "lookup").unwrap().value(0),
            "IBM US Equity"
        );
        assert_eq!(
            as_string_col(&batch, "status")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("resolved"),
                Some("unresolved"),
                Some("mismatch"),
                Some("resolved"),
                Some("unresolved"),
                Some("unsupported")
            ]
        );
        let topics = as_string_col(&batch, "venue_topic").unwrap();
        assert_eq!(topics.value(0), "IBM UN Equity");
        assert_eq!(topics.value(3), "IBM UN Equity");
        assert!(topics.is_null(1));
        assert_eq!(
            as_string_col(&batch, "venue_figi").unwrap().value(0),
            "BBG000TEST001"
        );
        for field in ["venue_figi", "exch_code", "pricing_source"] {
            assert!(batch.column_by_name(field).unwrap().is_null(2));
            assert!(batch.column_by_name(field).unwrap().is_null(4));
        }
        assert_eq!(as_string_col(&batch, "kind").unwrap().value(1), "unknown");
    }

    #[test]
    fn snapshot_uses_metadata_types_and_masks_invalid_venues() {
        let (rows, values) = fixture();
        let names = [
            "IN_AUCTION_RT",
            "ORDER_IMB_BUY_VOLUME",
            "REFERENCE_PRICE_RT",
            "CLOSING_AUCTION_VOLUME_DATE_RT",
            "IMBALANCE_INDIC_RT",
            "MISSING_FIELD",
        ]
        .map(str::to_string)
        .to_vec();
        let types = fields(&[
            ("IN_AUCTION_RT", "bool"),
            ("ORDER_IMB_BUY_VOLUME", "float64"),
            ("REFERENCE_PRICE_RT", "float64"),
            ("CLOSING_AUCTION_VOLUME_DATE_RT", "date32"),
            ("IMBALANCE_INDIC_RT", "string"),
            ("MISSING_FIELD", "int64"),
        ]);
        let batch = build_snapshot_batch(&rows, &names, &types, &values).unwrap();
        let expected_names: Vec<_> = ["input_order", "security", "venue_topic", "status", "error"]
            .iter()
            .copied()
            .chain(names.iter().map(String::as_str))
            .collect();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            expected_names
        );
        assert!(batch
            .column(5)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            42.0
        );
        assert_eq!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            101.25
        );
        assert_eq!(
            batch
                .column(8)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            naive_to_date32(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap())
        );
        assert_eq!(
            batch
                .column(9)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "BUY"
        );
        assert_eq!(batch.column(10).data_type(), &DataType::Int64);
        assert_eq!(batch.column(10).null_count(), rows.len());
        for column in batch.columns().iter().skip(5) {
            for index in [1, 2, 4, 5] {
                assert!(column.is_null(index));
            }
        }
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(3),
            42.0
        );
    }

    #[test]
    fn snapshot_coerces_boolean_and_promotes_integer_columns() {
        let (mut rows, mut values) = fixture();
        rows.retain(|row| row.decision.status == VenueStatus::Resolved);
        for (raw, expected) in [
            ("Y", Some(true)),
            ("N", Some(false)),
            ("true", Some(true)),
            ("FALSE", Some(false)),
            ("unknown", None),
        ] {
            values
                .get_mut("IBM UN Equity")
                .unwrap()
                .insert("IN_AUCTION_RT".to_string(), raw.to_string());
            let batch = build_snapshot_batch(
                &rows,
                &[
                    "IN_AUCTION_RT".to_string(),
                    "ORDER_IMB_BUY_VOLUME".to_string(),
                ],
                &fields(&[("IN_AUCTION_RT", "bool"), ("ORDER_IMB_BUY_VOLUME", "int32")]),
                &values,
            )
            .unwrap();
            let booleans = batch
                .column(5)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert_eq!(booleans.iter().next().unwrap(), expected);
            let integers = batch
                .column(6)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(integers.value(0), 42);
        }
    }

    #[test]
    fn snapshot_uses_real_apiflds_temporal_metadata() {
        let (rows, mut values) = fixture();
        values.get_mut("IBM UN Equity").unwrap().extend(fields(&[
            ("SYNTH_DATETIME", "2026-01-02T15:59:01.123456+01:00"),
            ("SYNTH_DATE_OR_TIME", "15:59:01.123456"),
        ]));
        let metadata = [
            ("IMBALANCE_TIMESTAMP_RT", Some("Datetime"), Some("Time")),
            (
                "CLOSING_AUCTION_VOLUME_DATE_RT",
                Some("Datetime"),
                Some("Date"),
            ),
            ("SYNTH_DATETIME", Some("Datetime"), Some("Datetime")),
            ("SYNTH_DATE_OR_TIME", Some("Datetime"), Some("DateOrTime")),
            ("MISSING_TIME", Some("Datetime"), Some("Time")),
            ("MISSING_DATE", Some("Datetime"), Some("Date")),
            ("MISSING_DATETIME", Some("Datetime"), None),
        ];
        let names = metadata
            .iter()
            .map(|(name, _, _)| name.to_string())
            .collect::<Vec<_>>();
        let batch =
            build_snapshot_batch(&rows, &names, &metadata_types(&metadata), &values).unwrap();
        let expected_types = [
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ];
        for (index, expected) in expected_types.iter().enumerate() {
            assert_eq!(batch.column(index + 5).data_type(), expected);
            for invalid_row in [1, 2, 4, 5] {
                assert!(batch.column(index + 5).is_null(invalid_row));
            }
        }
        let times = batch
            .column(5)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 57_541_123_456);
        let dates = batch
            .column(6)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(
            dates.value(0),
            naive_to_date32(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap())
        );
        let timestamps = batch
            .column(7)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            timestamps.value(0),
            DateTime::parse_from_rfc3339("2026-01-02T14:59:01.123456Z")
                .unwrap()
                .timestamp_micros()
        );
        assert_eq!(
            as_string_col(&batch, "SYNTH_DATE_OR_TIME")
                .unwrap()
                .value(0),
            "15:59:01.123456"
        );
        for column in batch.columns().iter().skip(9) {
            assert_eq!(column.null_count(), rows.len());
        }
    }

    #[test]
    fn snapshot_retains_mixed_or_unparseable_temporal_text() {
        let (mut rows, mut values) = fixture();
        values
            .get_mut("AAPL UW Equity")
            .unwrap()
            .insert("EXCH_CODE".to_string(), "UW".to_string());
        for text in ["2026-01-02T15:59:01.123456Z", "UNPARSEABLE"] {
            values
                .get_mut("AAPL UW Equity")
                .unwrap()
                .insert("IMBALANCE_TIMESTAMP_RT".to_string(), text.to_string());
            validate_rows(&mut rows, &values);
            let batch = build_snapshot_batch(
                &rows,
                &["IMBALANCE_TIMESTAMP_RT".to_string()],
                &metadata_types(&[("IMBALANCE_TIMESTAMP_RT", Some("Datetime"), Some("Time"))]),
                &values,
            )
            .unwrap();
            let times = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(times.value(0), "15:59:01.123456");
            assert_eq!(times.value(2), text);
        }
    }

    #[test]
    fn snapshot_full_datetimes_use_utc_microseconds() {
        let (rows, mut values) = fixture();
        values.get_mut("IBM UN Equity").unwrap().insert(
            "IMBALANCE_TIMESTAMP_RT".to_string(),
            "2026-01-02T15:59:01.123456+01:00".to_string(),
        );
        let batch = build_snapshot_batch(
            &rows,
            &["IMBALANCE_TIMESTAMP_RT".to_string()],
            &metadata_types(&[("IMBALANCE_TIMESTAMP_RT", Some("Datetime"), Some("Time"))]),
            &values,
        )
        .unwrap();
        assert_eq!(
            batch.column(5).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let timestamps = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            timestamps.value(0),
            DateTime::parse_from_rfc3339("2026-01-02T14:59:01.123456Z")
                .unwrap()
                .timestamp_micros()
        );
    }

    #[test]
    fn empty_batches_keep_schema_and_default_fields() {
        let fields = snapshot_fields(Vec::new());
        let batch = build_snapshot_batch(&[], &fields, &HashMap::new(), &HashMap::new()).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .skip(5)
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            DEFAULT
        );
        let venues = build_resolve_venues_batch(&[]).unwrap();
        assert_eq!(venues.num_rows(), 0);
        assert_eq!(venues.schema().field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn preferred_validation_prevents_snapshot_fallback_data() {
        let isin = synthetic_isin();
        let lookup = normalize_security_input(&isin);
        let values = HashMap::from([(
            lookup.clone(),
            fields(&[
                ("MARKET_SECTOR_DES", "Pfd"),
                ("PRICING_SOURCE", "EXCH"),
                ("EXCH_CODE", "NEW YORK"),
                ("ID_ISIN", &isin),
            ]),
        )]);
        let mut rows = uncached_rows(std::slice::from_ref(&isin), &values, &HashMap::new());
        let topic = format!("/isin/{isin}@SNY2");
        let mut data = HashMap::from([(
            topic.clone(),
            fields(&[
                ("PRICING_SOURCE", "EXCH"),
                ("EXCH_CODE", "NEW YORK"),
                ("IN_AUCTION_RT", "N"),
            ]),
        )]);
        validate_rows(&mut rows, &data);
        let names = vec!["IN_AUCTION_RT".to_string()];
        let types = fields(&[("IN_AUCTION_RT", "bool")]);
        let batch = build_snapshot_batch(&rows, &names, &types, &data).unwrap();
        assert_eq!(
            as_string_col(&batch, "status").unwrap().value(0),
            "mismatch"
        );
        assert!(batch.column(5).is_null(0));
        data.get_mut(&topic)
            .unwrap()
            .insert("PRICING_SOURCE".to_string(), "SNY2".to_string());
        validate_rows(&mut rows, &data);
        let batch = build_snapshot_batch(&rows, &names, &types, &data).unwrap();
        assert_eq!(
            as_string_col(&batch, "status").unwrap().value(0),
            "resolved"
        );
        assert!(!batch
            .column(5)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn routing_from_refdata_keeps_literal_sentinel_shaped_tickers() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new("field", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["SYNTHETIC"; 5])),
                Arc::new(StringArray::from(vec![
                    "MARKET_SECTOR_DES",
                    "TICKER",
                    "EXCH_CODE",
                    "COMPOSITE_EXCH_CODE",
                    "EQY_PRIM_EXCH_SHRT",
                ])),
                Arc::new(StringArray::from(vec!["Equity", "null", "US", "US", "UN"])),
            ],
        )
        .unwrap();
        let values = refdata_value_map(&batch).unwrap();
        let mut rows = uncached_rows(&["SYNTHETIC".to_string()], &values, &HashMap::new());
        validate_rows(
            &mut rows,
            &HashMap::from([("null UN Equity".to_string(), fields(&[("EXCH_CODE", "UN")]))]),
        );
        let output = build_resolve_venues_batch(&rows).unwrap();
        assert_eq!(
            as_string_col(&output, "venue_topic").unwrap().value(0),
            "null UN Equity"
        );
        assert_eq!(
            as_string_col(&output, "status").unwrap().value(0),
            "resolved"
        );
    }
}
