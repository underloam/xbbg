//! Field type cache and resolution.
//!
//! Provides automatic field type resolution using a hierarchy:
//! 1. Manual Override (from Python)
//! 2. Physical Cache (default: `~/.xbbg/field_cache.json`, configurable via `EngineConfig`)
//! 3. API Query (//blp/apiflds service)
//! 4. Defaults (bdp=String, bdh=Float64)
//!
//! In-memory storage uses `ArcSwap` — lock-free reads via atomic pointer load.
//! Writers mutate a mutex-protected source-of-truth map and publish snapshots.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, TimeUnit};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use xbbg_log::{debug, info, warn};

use crate::cache_io::{read_json_array_bounded, AtomicJsonPublisher, PublicationOutcome};

/// Bloomberg field type as returned by //blp/apiflds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlpFieldType {
    Boolean,
    Character,
    Date,
    DateOrTime,
    Datetime,
    Double,
    Float,
    Int32,
    Int64,
    String,
    Time,
    // Bulk types (arrays)
    BulkFormat,
    // Unknown/other
    Unknown(String),
}

impl BlpFieldType {
    /// Parse from Bloomberg field type string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "boolean" | "bool" => BlpFieldType::Boolean,
            "character" | "char" => BlpFieldType::Character,
            "date" => BlpFieldType::Date,
            "dateortime" | "date_or_time" => BlpFieldType::DateOrTime,
            "datetime" | "timestamp" => BlpFieldType::Datetime,
            "double" | "real" | "price" => BlpFieldType::Double,
            "float" => BlpFieldType::Float,
            "int32" | "integer" => BlpFieldType::Int32,
            "int64" | "long" => BlpFieldType::Int64,
            "string" | "longcharacter" | "stringorreal" => BlpFieldType::String,
            "time" => BlpFieldType::Time,
            "bulkformat" | "bulk" => BlpFieldType::BulkFormat,
            other => BlpFieldType::Unknown(other.to_string()),
        }
    }

    /// Resolve apiflds metadata, whose generic Datetime datatype needs the ftype.
    pub fn from_metadata(datatype: Option<&str>, ftype: Option<&str>) -> Self {
        let datatype = datatype.map(str::trim).filter(|value| !value.is_empty());
        if let Some(datatype) = datatype.filter(|value| !value.eq_ignore_ascii_case("Datetime")) {
            return Self::parse(datatype);
        }
        match Self::parse(ftype.unwrap_or("")) {
            Self::Date => Self::Date,
            Self::Time => Self::Time,
            Self::DateOrTime => Self::DateOrTime,
            _ => Self::Datetime,
        }
    }

    /// Convert to Arrow DataType.
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            BlpFieldType::Boolean => DataType::Boolean,
            BlpFieldType::Character => DataType::Utf8,
            BlpFieldType::Date => DataType::Date32,
            BlpFieldType::DateOrTime => DataType::Utf8, // Could be either, use string
            BlpFieldType::Datetime => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            BlpFieldType::Double | BlpFieldType::Float => DataType::Float64,
            BlpFieldType::Int32 => DataType::Int32,
            BlpFieldType::Int64 => DataType::Int64,
            BlpFieldType::String => DataType::Utf8,
            BlpFieldType::Time => DataType::Time64(TimeUnit::Microsecond),
            BlpFieldType::BulkFormat => DataType::Utf8, // Bulk data as JSON string
            BlpFieldType::Unknown(_) => DataType::Utf8,
        }
    }

    /// Convert to Arrow type string (for serialization).
    ///
    /// Int32 is normalised to Int64; time-only values have no date or timezone.
    pub fn to_arrow_type_str(&self) -> &'static str {
        match self {
            BlpFieldType::Boolean => "bool", // Python uses "bool" not "boolean"
            BlpFieldType::Character => "string",
            BlpFieldType::Date => "date32",
            BlpFieldType::DateOrTime => "string",
            BlpFieldType::Datetime => "timestamp",
            BlpFieldType::Double | BlpFieldType::Float => "float64",
            BlpFieldType::Int32 => "int64", // Python normalizes Int32 → int64
            BlpFieldType::Int64 => "int64",
            BlpFieldType::String => "string",
            BlpFieldType::Time => "time64",
            BlpFieldType::BulkFormat => "string",
            BlpFieldType::Unknown(_) => "string",
        }
    }
}

/// Cached field information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldInfo {
    pub field_id: String,
    pub arrow_type: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub category: String,
}

const DEFAULT_MAX_FIELD_CACHE_ENTRIES: usize = 65_536;
const MAX_FIELD_CACHE_FILE_BYTES: u64 = 64 * 1024 * 1024;
// Bump when metadata interpretation changes. Legacy unversioned snapshots can
// contain Datetime -> string / Time -> timestamp mappings; any non-current
// entry discards the whole snapshot. The next metadata query atomically saves
// a current-version snapshot at the same configured path.
const FIELD_CACHE_SCHEMA_VERSION: u32 = 2;

/// Keep the public FieldInfo unchanged while versioning every persisted entry.
#[derive(Serialize, Deserialize)]
struct PersistedFieldInfo {
    #[serde(default)]
    schema_version: u32,
    #[serde(flatten)]
    info: FieldInfo,
}

#[derive(Clone, Debug)]
struct FieldCacheEntry {
    info: FieldInfo,
    inserted_at: u64,
}

#[derive(Debug, Default)]
struct FieldCacheState {
    entries: HashMap<String, FieldCacheEntry>,
    next_insert_epoch: u64,
}

/// Field type resolver with caching.
pub struct FieldTypeResolver {
    /// In-memory cache (uppercased field_id -> FieldInfo). Lock-free reads via ArcSwap.
    cache: ArcSwap<HashMap<String, FieldInfo>>,
    /// Write-side source of truth. Writers hold this briefly, then publish one snapshot.
    write_cache: Mutex<FieldCacheState>,
    /// Path to cache file
    cache_path: PathBuf,
    /// Disk load happens at most once (lazy).
    loaded: OnceLock<()>,
    max_entries: usize,
    publisher: AtomicJsonPublisher,
}

impl FieldTypeResolver {
    /// Create a new resolver with default cache path (~/.xbbg/field_cache.json).
    pub fn new() -> Self {
        Self::with_cache_path(Self::default_cache_path())
    }

    /// Create a resolver with a custom cache path.
    pub fn with_cache_path(path: PathBuf) -> Self {
        Self::with_cache_path_and_max_entries(path, DEFAULT_MAX_FIELD_CACHE_ENTRIES)
    }

    /// Create a resolver with a custom cache path and entry bound.
    pub fn with_cache_path_and_max_entries(path: PathBuf, max_entries: usize) -> Self {
        Self {
            cache: ArcSwap::from_pointee(HashMap::new()),
            write_cache: Mutex::new(FieldCacheState::default()),
            cache_path: path,
            loaded: OnceLock::new(),
            max_entries,
            publisher: AtomicJsonPublisher::default(),
        }
    }

    /// Get the default cache path.
    fn default_cache_path() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        match home {
            Some(h) => h.join(".xbbg").join("field_cache.json"),
            None => {
                warn!(
                    "Home directory not found (USERPROFILE/HOME not set). \
                     Field cache will use current directory. Set field_cache_path in \
                     EngineConfig to specify a persistent location."
                );
                PathBuf::from(".").join(".xbbg").join("field_cache.json")
            }
        }
    }

    /// Ensure cache is loaded from disk (lazy, runs at most once).
    fn ensure_loaded(&self) {
        self.loaded.get_or_init(|| self.load_from_disk());
    }

    fn swap_snapshot(&self, state: &FieldCacheState) -> Arc<HashMap<String, FieldInfo>> {
        let snapshot = state
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.info.clone()))
            .collect();
        self.cache.swap(Arc::new(snapshot))
    }

    fn insert_keyed_entries<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (String, FieldInfo)>,
    {
        let (previous_snapshot, replaced, evicted) = {
            let mut state = self.write_cache.lock();
            let mut replaced = Vec::new();
            let mut changed = false;

            for (key, info) in entries {
                let key = key.to_uppercase();
                if key.is_empty() {
                    continue;
                }

                let inserted_at = match state.entries.get(&key) {
                    Some(existing) => existing.inserted_at,
                    None => {
                        let epoch = state.next_insert_epoch;
                        state.next_insert_epoch = state.next_insert_epoch.saturating_add(1);
                        epoch
                    }
                };

                if let Some(previous) = state
                    .entries
                    .insert(key, FieldCacheEntry { info, inserted_at })
                {
                    replaced.push(previous);
                }
                changed = true;
            }

            if !changed {
                return;
            }

            let evicted = Self::evict_oldest(&mut state, self.max_entries);
            let previous_snapshot = self.swap_snapshot(&state);
            (previous_snapshot, replaced, evicted)
        };

        // A retired ArcSwap snapshot and evicted metadata can both be large.
        // Release them after the write-side state lock is available again.
        drop(previous_snapshot);
        drop(evicted);
        drop(replaced);
    }

    fn evict_oldest(
        state: &mut FieldCacheState,
        max_entries: usize,
    ) -> Vec<(String, FieldCacheEntry)> {
        let excess = state.entries.len().saturating_sub(max_entries);
        if excess == 0 {
            return Vec::new();
        }

        let mut oldest: Vec<(&str, u64)> = state
            .entries
            .iter()
            .map(|(key, entry)| (key.as_str(), entry.inserted_at))
            .collect();
        if excess < oldest.len() {
            oldest.select_nth_unstable_by(excess - 1, |left, right| {
                left.1.cmp(&right.1).then_with(|| left.0.cmp(right.0))
            });
        }
        let keys: Vec<String> = oldest[..excess]
            .iter()
            .map(|(key, _)| (*key).to_string())
            .collect();

        keys.into_iter()
            .filter_map(|key| state.entries.remove_entry(&key))
            .collect()
    }

    /// Load cache from JSON file.
    fn load_from_disk(&self) {
        if !self.cache_path.exists() {
            info!(
                path = %self.cache_path.display(),
                "No field cache file found, will build cache from API queries"
            );
            return;
        }

        let entries: Vec<PersistedFieldInfo> = match read_json_array_bounded(
            &self.cache_path,
            MAX_FIELD_CACHE_FILE_BYTES,
            self.max_entries,
        ) {
            Ok(entries) => entries,
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.cache_path.display(),
                    "Field cache file is unreadable or invalid, ignoring. Will rebuild from API queries."
                );
                return;
            }
        };
        if entries
            .iter()
            .any(|entry| entry.schema_version != FIELD_CACHE_SCHEMA_VERSION)
        {
            info!("Ignoring stale field-type cache; metadata will be refreshed");
            return;
        }

        let pairs: Vec<(String, FieldInfo)> = entries
            .into_iter()
            .map(|entry| (entry.info.field_id.to_uppercase(), entry.info))
            .collect();

        if !pairs.is_empty() {
            self.insert_keyed_entries(pairs);
        }

        info!(count = self.cache.load().len(), path = %self.cache_path.display(), "Loaded field cache");
    }

    /// Save cache to JSON file.
    pub fn save_to_disk(&self) -> Result<(), String> {
        self.ensure_loaded();

        // Reserve publication order and capture the matching snapshot while
        // holding the write-state lock. Clear uses the same ordering point.
        let (publication, snapshot) = {
            let _state = self.write_cache.lock();
            (self.publisher.begin(), self.cache.load_full())
        };

        let entries: Vec<PersistedFieldInfo> = snapshot
            .values()
            .map(|info| PersistedFieldInfo {
                schema_version: FIELD_CACHE_SCHEMA_VERSION,
                info: info.clone(),
            })
            .collect();
        match publication.publish(&self.cache_path, &entries)? {
            PublicationOutcome::Published => {
                info!(count = entries.len(), path = %self.cache_path.display(), "Saved field cache");
            }
            PublicationOutcome::Superseded => {
                debug!(path = %self.cache_path.display(), "Skipped superseded field cache snapshot");
            }
        }

        Ok(())
    }

    /// Preload the cache from disk now instead of on first async resolution.
    pub fn preload(&self) {
        self.ensure_loaded();
    }

    /// Get field info from cache.
    pub fn get(&self, field_id: &str) -> Option<FieldInfo> {
        self.ensure_loaded();
        self.cache.load().get(&field_id.to_uppercase()).cloned()
    }

    /// Get Arrow type string for a field.
    pub fn get_arrow_type(&self, field_id: &str) -> Option<String> {
        self.get(field_id).map(|info| info.arrow_type)
    }

    /// Insert field info into cache.
    pub fn insert(&self, info: FieldInfo) {
        self.insert_many(std::iter::once(info));
    }

    /// Insert multiple field infos and publish a single snapshot.
    pub fn insert_many<I>(&self, infos: I)
    where
        I: IntoIterator<Item = FieldInfo>,
    {
        self.ensure_loaded();
        self.insert_keyed_entries(
            infos
                .into_iter()
                .map(|info| (info.field_id.to_uppercase(), info)),
        );
    }

    /// Extend the cache with multiple (uppercase_key, FieldInfo) pairs in one snapshot.
    ///
    /// Exposed only for benchmarks so callers can measure the batched-publish cost directly.
    #[cfg(feature = "bench-internals")]
    pub fn cache_rcu_extend(&self, entries: Vec<(String, FieldInfo)>) {
        self.ensure_loaded();
        self.insert_keyed_entries(entries);
    }

    /// Insert multiple field infos from a FieldInfoRequest response.
    ///
    /// Expects columns from the FieldInfo extractor:
    /// - field: Field mnemonic (e.g., "PX_LAST")
    /// - type: Arrow type string (e.g., "float64")
    /// - description: Field description
    /// - category: Category name
    pub fn insert_from_response(&self, batch: &RecordBatch) {
        self.ensure_loaded();

        let field_col = batch
            .column_by_name("field")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let type_col = batch
            .column_by_name("type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let desc_col = batch
            .column_by_name("description")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let cat_col = batch
            .column_by_name("category")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let (Some(fields), Some(types)) = (field_col, type_col) else {
            warn!("FieldInfo batch missing required columns (field, type)");
            return;
        };

        // Collect all entries first, then publish a single snapshot — avoids O(n²) clone cost.
        let mut entries: Vec<FieldInfo> = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if fields.is_null(i) || types.is_null(i) {
                continue;
            }
            let field_id = fields.value(i).to_uppercase();
            let arrow_type = types.value(i).to_string();
            let description = desc_col
                .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                .unwrap_or("")
                .to_string();
            let category = cat_col
                .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                .unwrap_or("")
                .to_string();

            debug!(field = %field_id, arrow_type = %arrow_type, "Cached field type");
            entries.push(FieldInfo {
                field_id,
                arrow_type,
                description,
                category,
            });
        }

        if !entries.is_empty() {
            self.insert_many(entries);
        }
    }

    /// Resolve field types for a list of fields.
    ///
    /// Returns a HashMap of field_id -> arrow_type_string.
    /// Uses the hierarchy: manual_overrides -> cache -> default.
    pub fn resolve_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
        default_type: &str,
    ) -> HashMap<String, String> {
        self.ensure_loaded();

        let snapshot = self.cache.load();
        let mut result = HashMap::new();

        for field in fields {
            let field_upper = field.to_uppercase();

            // 1. Check manual overrides
            if let Some(overrides) = manual_overrides {
                if let Some(t) = overrides.get(field).or_else(|| overrides.get(&field_upper)) {
                    result.insert(field.clone(), t.clone());
                    continue;
                }
            }

            // 2. Check cache
            if let Some(info) = snapshot.get(&field_upper) {
                result.insert(field.clone(), info.arrow_type.clone());
                continue;
            }

            // 3. Use default
            result.insert(field.clone(), default_type.to_string());
        }

        result
    }

    /// Resolve only manual overrides and already-cached field types.
    ///
    /// Unlike [`Self::resolve_types`], this does not apply defaults. It lets
    /// request paths opportunistically use metadata that has already been
    /// resolved without changing behavior for unknown fields or issuing extra
    /// Bloomberg metadata requests.
    pub fn resolve_cached_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        self.ensure_loaded();
        let mut result = manual_overrides.cloned().unwrap_or_default();

        let snapshot = self.cache.load();
        for field in fields {
            let field_upper = field.to_uppercase();
            if result.contains_key(field) || result.contains_key(&field_upper) {
                continue;
            }

            if let Some(info) = snapshot.get(&field_upper) {
                result.insert(field.clone(), info.arrow_type.clone());
            }
        }

        result
    }

    /// Get list of fields that are not in cache.
    pub fn get_uncached_fields(&self, fields: &[String]) -> Vec<String> {
        self.ensure_loaded();

        let snapshot = self.cache.load();
        fields
            .iter()
            .filter(|f| !snapshot.contains_key(&f.to_uppercase()))
            .cloned()
            .collect()
    }

    /// Clear all cached field info and its persisted snapshot.
    pub fn clear(&self) -> Result<(), String> {
        self.ensure_loaded();
        let (previous_snapshot, removed) = {
            let mut state = self.write_cache.lock();
            let removal = self.publisher.begin();
            let outcome = removal.remove(&self.cache_path)?;
            debug_assert_eq!(outcome, PublicationOutcome::Published);

            let removed: Vec<(String, FieldCacheEntry)> = state.entries.drain().collect();
            state.next_insert_epoch = 0;
            let previous_snapshot = self.cache.swap(Arc::new(HashMap::new()));
            (previous_snapshot, removed)
        };
        drop(previous_snapshot);
        drop(removed);
        info!("Cleared field cache");
        Ok(())
    }

    /// Get cache statistics.
    pub fn stats(&self) -> (usize, PathBuf) {
        self.ensure_loaded();
        (self.cache.load().len(), self.cache_path.clone())
    }
}

impl Default for FieldTypeResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Global field type resolver (initialized on first access or via `init_global_resolver`).
static GLOBAL_RESOLVER: std::sync::OnceLock<Arc<FieldTypeResolver>> = std::sync::OnceLock::new();

/// Initialize the global field type resolver with an optional custom cache path.
///
/// If already initialized (e.g., from a previous `Engine::start()` call), this is a no-op
/// and the existing resolver is returned. The cache path cannot be changed after initialization.
pub fn init_global_resolver(cache_path: Option<PathBuf>) -> Arc<FieldTypeResolver> {
    GLOBAL_RESOLVER
        .get_or_init(|| {
            let resolver = match cache_path {
                Some(ref path) => {
                    info!(path = %path.display(), "Using custom field cache path");
                    FieldTypeResolver::with_cache_path(path.clone())
                }
                None => FieldTypeResolver::new(),
            };
            Arc::new(resolver)
        })
        .clone()
}

/// Get the global field type resolver.
///
/// If not yet initialized, creates one with the default cache path.
pub fn global_resolver() -> Arc<FieldTypeResolver> {
    init_global_resolver(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn field_info(field_id: &str, arrow_type: &str) -> FieldInfo {
        FieldInfo {
            field_id: field_id.to_string(),
            arrow_type: arrow_type.to_string(),
            description: String::new(),
            category: String::new(),
        }
    }

    #[test]
    fn test_blp_field_type_parsing() {
        assert_eq!(BlpFieldType::parse("Double"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("REAL"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("Price"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("String"), BlpFieldType::String);
        assert_eq!(BlpFieldType::parse("Boolean"), BlpFieldType::Boolean);
        assert_eq!(BlpFieldType::parse("Date"), BlpFieldType::Date);
        assert_eq!(BlpFieldType::parse("Int64"), BlpFieldType::Int64);
    }

    #[test]
    fn test_arrow_type_conversion() {
        assert_eq!(BlpFieldType::Double.to_arrow_type(), DataType::Float64);
        assert_eq!(BlpFieldType::String.to_arrow_type(), DataType::Utf8);
        assert_eq!(BlpFieldType::Boolean.to_arrow_type(), DataType::Boolean);
        assert_eq!(BlpFieldType::Date.to_arrow_type(), DataType::Date32);
        assert_eq!(BlpFieldType::Int64.to_arrow_type(), DataType::Int64);
    }

    #[test]
    fn test_resolve_with_overrides() {
        let resolver = FieldTypeResolver::new();

        let fields = vec!["PX_LAST".to_string(), "VOLUME".to_string()];
        let mut overrides = HashMap::new();
        overrides.insert("VOLUME".to_string(), "int64".to_string());

        let resolved = resolver.resolve_types(&fields, Some(&overrides), "float64");

        assert_eq!(resolved.get("PX_LAST"), Some(&"float64".to_string()));
        assert_eq!(resolved.get("VOLUME"), Some(&"int64".to_string()));
    }

    #[test]
    fn resolve_cached_types_preserves_overrides_and_skips_unknowns() {
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path(dir.path().join("field_cache.json"));
        resolver.insert(FieldInfo {
            field_id: "PX_LAST".to_string(),
            arrow_type: "float64".to_string(),
            description: String::new(),
            category: String::new(),
        });

        let fields = vec![
            "PX_LAST".to_string(),
            "VOLUME".to_string(),
            "NAME".to_string(),
        ];
        let mut overrides = HashMap::new();
        overrides.insert("VOLUME".to_string(), "int64".to_string());

        let resolved = resolver.resolve_cached_types(&fields, Some(&overrides));

        assert_eq!(resolved.get("PX_LAST"), Some(&"float64".to_string()));
        assert_eq!(resolved.get("VOLUME"), Some(&"int64".to_string()));
        assert!(!resolved.contains_key("NAME"));
    }

    #[test]
    fn insert_many_publishes_one_snapshot_and_reads_reflect_writes() {
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path(dir.path().join("field_cache.json"));
        let before = resolver.cache.load_full();

        resolver.insert_many([
            field_info("PX_LAST", "float64"),
            field_info("VOLUME", "int64"),
        ]);

        let after = resolver.cache.load_full();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.len(), 2);
        assert_eq!(
            resolver.get_arrow_type("PX_LAST").as_deref(),
            Some("float64")
        );
        assert_eq!(resolver.get_arrow_type("VOLUME").as_deref(), Some("int64"));

        let after_reads = resolver.cache.load_full();
        let _ = resolver.get("PX_LAST");
        let _ = resolver.get("VOLUME");
        assert!(Arc::ptr_eq(&after_reads, &resolver.cache.load_full()));
    }

    #[test]
    fn evicts_oldest_inserted_field_when_bound_is_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path_and_max_entries(
            dir.path().join("field_cache.json"),
            2,
        );

        resolver.insert_many([
            field_info("FIRST", "float64"),
            field_info("SECOND", "int64"),
            field_info("THIRD", "string"),
        ]);

        assert!(resolver.get("FIRST").is_none());
        assert_eq!(resolver.get_arrow_type("SECOND").as_deref(), Some("int64"));
        assert_eq!(resolver.get_arrow_type("THIRD").as_deref(), Some("string"));
        assert_eq!(resolver.cache.load().len(), 2);
    }
    #[test]
    fn saved_snapshot_round_trips_through_atomic_publication() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        let resolver = FieldTypeResolver::with_cache_path(path.clone());
        resolver.insert_many([
            field_info("PX_LAST", "float64"),
            field_info("VOLUME", "int64"),
        ]);

        resolver.save_to_disk().unwrap();

        let reloaded = FieldTypeResolver::with_cache_path(path);
        assert_eq!(
            reloaded.get_arrow_type("PX_LAST").as_deref(),
            Some("float64")
        );
        assert_eq!(reloaded.get_arrow_type("VOLUME").as_deref(), Some("int64"));
    }

    #[test]
    fn replacement_failure_surfaces_from_save() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        fs::create_dir(&path).unwrap();
        let resolver = FieldTypeResolver::with_cache_path(path);
        resolver.insert(field_info("PX_LAST", "float64"));

        let error = resolver.save_to_disk().unwrap_err();

        assert!(error.contains("cannot replace cache file"));
    }

    #[test]
    fn clear_removes_persisted_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        let resolver = FieldTypeResolver::with_cache_path(path.clone());
        resolver.insert(field_info("PX_LAST", "float64"));
        resolver.save_to_disk().unwrap();

        resolver.clear().unwrap();

        assert!(!path.exists());
        let reloaded = FieldTypeResolver::with_cache_path(path);
        assert!(reloaded.get("PX_LAST").is_none());
    }

    #[test]
    fn failed_clear_keeps_memory_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        fs::create_dir(&path).unwrap();
        let resolver = FieldTypeResolver::with_cache_path(path);
        resolver.insert(field_info("PX_LAST", "float64"));

        let error = resolver.clear().unwrap_err();

        assert!(error.contains("cannot remove cache file"));
        assert!(resolver.get("PX_LAST").is_some());
    }

    #[test]
    fn cold_load_rejects_more_than_configured_entry_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        let entries = [
            field_info("FIRST", "float64"),
            field_info("SECOND", "int64"),
            field_info("THIRD", "string"),
        ]
        .map(|info| PersistedFieldInfo {
            schema_version: FIELD_CACHE_SCHEMA_VERSION,
            info,
        });
        fs::write(&path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let resolver = FieldTypeResolver::with_cache_path_and_max_entries(path, 2);

        resolver.preload();

        assert_eq!(resolver.stats().0, 0);
        assert!(resolver.get("FIRST").is_none());
    }

    #[test]
    fn metadata_disambiguates_datetime_without_overriding_concrete_types() {
        for datatype in [Some("Datetime"), Some(" DATETIME "), None, Some("")] {
            for (ftype, expected) in [
                (Some("Date"), BlpFieldType::Date),
                (Some("time"), BlpFieldType::Time),
                (Some("DateOrTime"), BlpFieldType::DateOrTime),
                (Some("Datetime"), BlpFieldType::Datetime),
                (Some("Other"), BlpFieldType::Datetime),
                (None, BlpFieldType::Datetime),
            ] {
                assert_eq!(BlpFieldType::from_metadata(datatype, ftype), expected);
            }
        }
        assert_eq!(
            BlpFieldType::from_metadata(Some("Double"), Some("Time")),
            BlpFieldType::Double
        );
        assert_eq!(
            BlpFieldType::Time.to_arrow_type(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(BlpFieldType::Time.to_arrow_type_str(), "time64");
        assert_eq!(BlpFieldType::Date.to_arrow_type_str(), "date32");
        assert_eq!(
            BlpFieldType::Datetime.to_arrow_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn stale_field_cache_entries_are_refetched_instead_of_reused_as_strings() {
        for schema_version in [None, Some(1), Some(FIELD_CACHE_SCHEMA_VERSION + 1)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("field_cache.json");
            let mut entry = serde_json::to_value(field_info("SYNTHETIC_TIME", "string")).unwrap();
            if let Some(version) = schema_version {
                entry["schema_version"] = serde_json::json!(version);
            }
            fs::write(&path, serde_json::to_vec(&vec![entry]).unwrap()).unwrap();
            let resolver = FieldTypeResolver::with_cache_path(path.clone());
            let fields = vec!["SYNTHETIC_TIME".to_string()];
            assert_eq!(resolver.get_uncached_fields(&fields), fields);
            assert!(resolver.resolve_cached_types(&fields, None).is_empty());

            resolver.insert(field_info("SYNTHETIC_TIME", "time64"));
            resolver.save_to_disk().unwrap();
            let current = FieldTypeResolver::with_cache_path(path);
            assert_eq!(
                current.get_arrow_type("SYNTHETIC_TIME").as_deref(),
                Some("time64")
            );
        }
    }
}
