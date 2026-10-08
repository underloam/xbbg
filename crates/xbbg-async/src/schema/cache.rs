//! Schema cache with in-memory and disk persistence.
//!
//! Caches introspected service schemas to avoid repeated API calls.
//! The cache persists to disk at ~/.xbbg/schema_cache/ for cross-session reuse.
//!
//! In-memory reads are lock-free via `ArcSwap`; synchronous writers serialize
//! disk and memory state transitions, then atomically publish cloned snapshots.
//! Retired snapshots are destroyed after the ordering lock is released.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use xbbg_log::{debug, info, warn};

use super::types::ServiceSchema;
use crate::cache_io::{read_json_bounded, remove_cache_file, write_json_atomic};

type SchemaMap = HashMap<String, Arc<ServiceSchema>>;

const MAX_SCHEMA_CACHE_FILE_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
struct SchemaBarrierHook {
    reached: std::sync::Barrier,
    resume: std::sync::Barrier,
}

/// Schema cache with in-memory and disk persistence.
///
/// Thread-safe cache for service schemas. Schemas are loaded lazily from disk
/// on first access and persisted automatically when updated.
#[derive(Clone)]
pub struct SchemaCache {
    /// In-memory cache (service_uri -> schema). Lock-free reads.
    cache: Arc<ArcSwap<SchemaMap>>,
    /// Directory for cached schema files
    cache_dir: PathBuf,
    /// Coalesces Engine cold loads without an unbounded per-service lock map.
    load_lock: Arc<tokio::sync::Mutex<()>>,
    /// Linearizes synchronous disk loads, writes, and invalidations.
    order_lock: Arc<parking_lot::Mutex<()>>,
    #[cfg(test)]
    disk_read_hook: Arc<parking_lot::Mutex<Option<Arc<SchemaBarrierHook>>>>,
    #[cfg(test)]
    invalidate_hook: Arc<parking_lot::Mutex<Option<Arc<SchemaBarrierHook>>>>,
}

impl SchemaCache {
    /// Create a new cache with default directory (~/.xbbg/schema_cache/).
    pub fn new() -> Self {
        Self::with_cache_dir(Self::default_cache_dir())
    }

    /// Create a cache with a custom directory.
    pub fn with_cache_dir(cache_dir: PathBuf) -> Self {
        Self {
            cache: Arc::new(ArcSwap::from_pointee(SchemaMap::new())),
            cache_dir,
            load_lock: Arc::new(tokio::sync::Mutex::new(())),
            order_lock: Arc::new(parking_lot::Mutex::new(())),
            #[cfg(test)]
            disk_read_hook: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(test)]
            invalidate_hook: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Get the default cache directory.
    fn default_cache_dir() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        home.unwrap_or_else(|| PathBuf::from("."))
            .join(".xbbg")
            .join("schema_cache")
    }

    /// Convert a service URI into one safe, injective path component.
    ///
    /// Encoding every UTF-8 byte avoids path prefixes, separators, traversal,
    /// and collisions between otherwise similar service names.
    fn service_to_filename(service: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let bytes = service.as_bytes();
        let mut filename = String::with_capacity(3 + bytes.len() * 2 + 5);
        filename.push_str("v1-");
        for &byte in bytes {
            filename.push(char::from(HEX[usize::from(byte >> 4)]));
            filename.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        filename.push_str(".json");
        filename
    }

    /// Get the file path for a service's cached schema.
    fn cache_path(&self, service: &str) -> PathBuf {
        self.cache_dir.join(Self::service_to_filename(service))
    }

    /// Load a bounded schema document from disk.
    fn load_from_disk(&self, service: &str) -> Option<ServiceSchema> {
        let path = self.cache_path(service);
        if !path.exists() {
            debug!(service, path = %path.display(), "Schema cache file not found");
            return None;
        }

        match read_json_bounded::<ServiceSchema>(&path, MAX_SCHEMA_CACHE_FILE_BYTES) {
            Ok(schema) if schema.service == service => {
                #[cfg(test)]
                self.pause_after_disk_read();
                info!(service, path = %path.display(), "Loaded schema from cache");
                Some(schema)
            }
            Ok(schema) => {
                warn!(
                    service,
                    embedded_service = %schema.service,
                    path = %path.display(),
                    "Ignoring schema cache file whose embedded service does not match its key"
                );
                None
            }
            Err(error) => {
                warn!(service, error = %error, "Failed to load schema cache file");
                None
            }
        }
    }

    /// Read a regular cache document whose filename agrees with its embedded
    /// service. This keeps directory scans bounded and rejects forged indexes.
    fn read_indexed_schema(path: &std::path::Path) -> Option<ServiceSchema> {
        let metadata = fs::symlink_metadata(path).ok()?;
        if !metadata.file_type().is_file() {
            return None;
        }
        let schema = read_json_bounded::<ServiceSchema>(path, MAX_SCHEMA_CACHE_FILE_BYTES).ok()?;
        let filename = Self::service_to_filename(&schema.service);
        (path.file_name() == Some(std::ffi::OsStr::new(&filename))).then_some(schema)
    }

    #[cfg(test)]
    fn pause_after_disk_read(&self) {
        Self::pause_at_hook(&self.disk_read_hook);
    }

    #[cfg(test)]
    fn pause_before_invalidate(&self) {
        Self::pause_at_hook(&self.invalidate_hook);
    }

    #[cfg(test)]
    fn pause_at_hook(slot: &parking_lot::Mutex<Option<Arc<SchemaBarrierHook>>>) {
        let hook = slot.lock().clone();
        if let Some(hook) = hook {
            hook.reached.wait();
            hook.resume.wait();
        }
    }

    fn write_schema(&self, service: &str, schema: &ServiceSchema) -> Result<(), String> {
        let path = self.cache_path(service);
        write_json_atomic(&path, schema)?;

        info!(service, path = %path.display(), "Saved schema to cache");
        Ok(())
    }

    /// Swap a new memory snapshot. The caller must hold `order_lock`.
    fn upsert_locked(&self, service: &str, schema: Arc<ServiceSchema>) -> Arc<SchemaMap> {
        let mut next = self.cache.load_full().as_ref().clone();
        next.insert(service.to_string(), schema);
        self.cache.swap(Arc::new(next))
    }

    /// Swap a memory snapshot without `service`. The caller must hold `order_lock`.
    fn evict_locked(&self, service: &str) -> Arc<SchemaMap> {
        let mut next = self.cache.load_full().as_ref().clone();
        next.remove(service);
        self.cache.swap(Arc::new(next))
    }

    /// Get a cached schema from memory only.
    ///
    /// This is safe for request hot paths because it never performs disk I/O.
    pub fn get_memory(&self, service: &str) -> Option<Arc<ServiceSchema>> {
        self.cache.load().get(service).map(Arc::clone)
    }

    /// Serialize cache misses so concurrent Engine callers can share one load.
    pub(crate) fn lock_load(&self) -> impl Future<Output = tokio::sync::MutexGuard<'_, ()>> {
        self.load_lock.lock()
    }

    /// Get a cached schema.
    ///
    /// First checks in-memory cache (lock-free), then disk cache.
    /// Returns None if not cached anywhere.
    pub fn get(&self, service: &str) -> Option<Arc<ServiceSchema>> {
        if let Some(schema) = self.get_memory(service) {
            return Some(schema);
        }

        let (schema, previous_snapshot) = {
            let _order = self.order_lock.lock();
            if let Some(schema) = self.get_memory(service) {
                return Some(schema);
            }

            let schema = Arc::new(self.load_from_disk(service)?);
            let previous_snapshot = self.upsert_locked(service, Arc::clone(&schema));
            (schema, previous_snapshot)
        };
        drop(previous_snapshot);
        Some(schema)
    }

    /// Insert a schema into the in-memory cache without disk persistence.
    pub fn insert_memory(&self, service: &str, schema: ServiceSchema) -> Arc<ServiceSchema> {
        let schema = Arc::new(schema);
        let previous_snapshot = {
            let _order = self.order_lock.lock();
            self.upsert_locked(service, Arc::clone(&schema))
        };
        drop(previous_snapshot);
        schema
    }

    /// Return the cache directory used for disk-backed operations.
    pub fn cache_dir(&self) -> PathBuf {
        self.cache_dir.clone()
    }

    /// Insert a schema into memory and attempt to persist it.
    ///
    /// Memory publication still completes when persistence fails so automatic
    /// cache population can remain best-effort; the caller receives the disk
    /// error and decides whether to log or propagate it.
    pub fn insert(
        &self,
        service: &str,
        schema: ServiceSchema,
    ) -> Result<Arc<ServiceSchema>, String> {
        let schema = Arc::new(schema);
        let (previous_snapshot, persist_result) = {
            let _order = self.order_lock.lock();
            let persist_result = self.write_schema(service, &schema);
            let previous_snapshot = self.upsert_locked(service, Arc::clone(&schema));
            (previous_snapshot, persist_result)
        };
        drop(previous_snapshot);
        persist_result.map(|()| schema)
    }

    /// Invalidate a cached schema (removes from memory and disk).
    pub fn invalidate(&self, service: &str) -> Result<(), String> {
        let previous_snapshot = {
            let _order = self.order_lock.lock();
            #[cfg(test)]
            self.pause_before_invalidate();
            remove_cache_file(&self.cache_path(service))?;
            self.evict_locked(service)
        };
        drop(previous_snapshot);
        info!(service, "Invalidated schema cache");
        Ok(())
    }

    /// Clear all cached schemas.
    pub fn clear(&self) -> Result<(), String> {
        let previous_snapshot = {
            let _order = self.order_lock.lock();
            self.clear_disk_locked()?;
            self.cache.swap(Arc::new(SchemaMap::new()))
        };
        drop(previous_snapshot);
        info!("Cleared schema cache");
        Ok(())
    }

    /// Remove all schema JSON files. The caller must hold `order_lock`.
    fn clear_disk_locked(&self) -> Result<(), String> {
        let entries = match fs::read_dir(&self.cache_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "cannot read schema cache directory '{}': {error}",
                    self.cache_dir.display()
                ));
            }
        };

        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "cannot read entry in schema cache directory '{}': {error}",
                    self.cache_dir.display()
                )
            })?;
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                remove_cache_file(&path)?;
            }
        }
        Ok(())
    }

    /// List all cached service URIs.
    ///
    /// Returns URIs from both memory and disk.
    pub fn list(&self) -> Vec<String> {
        let snapshot = self.cache.load();
        let mut services: Vec<String> = snapshot.keys().cloned().collect();

        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    continue;
                }
                let Some(schema) = Self::read_indexed_schema(&path) else {
                    continue;
                };
                if !services.contains(&schema.service) {
                    services.push(schema.service);
                }
            }
        }

        services.sort();
        services
    }

    /// Check if a schema is cached (memory or disk).
    pub fn contains(&self, service: &str) -> bool {
        if self.cache.load().contains_key(service) {
            return true;
        }
        Self::read_indexed_schema(&self.cache_path(service))
            .is_some_and(|schema| schema.service == service)
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let memory_count = self.cache.load().len();
        let disk_count = fs::read_dir(&self.cache_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| Self::read_indexed_schema(&entry.path()).is_some())
                    .count()
            })
            .unwrap_or(0);

        CacheStats {
            memory_count,
            disk_count,
            cache_dir: self.cache_dir.clone(),
        }
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of schemas in memory
    pub memory_count: usize,
    /// Number of schemas on disk
    pub disk_count: usize,
    /// Cache directory path
    pub cache_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::OperationSchema;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use tempfile::TempDir;

    fn create_test_schema(service: &str) -> ServiceSchema {
        ServiceSchema::new(
            service.to_string(),
            "Test Service".to_string(),
            vec![OperationSchema {
                name: "TestRequest".to_string(),
                description: "A test operation".to_string(),
                request: crate::schema::types::ElementInfo::empty(),
                responses: vec![],
            }],
        )
    }

    fn assert_thread_is_blocked<T>(thread: &std::thread::JoinHandle<T>) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !thread.is_finished(),
            "ordered cache mutation completed while another transition held the lock"
        );
    }

    #[test]
    fn service_filenames_are_safe_and_injective() {
        assert_eq!(
            SchemaCache::service_to_filename("//blp/refdata"),
            "v1-2f2f626c702f72656664617461.json"
        );
        assert_ne!(
            SchemaCache::service_to_filename("//blp/a_b"),
            SchemaCache::service_to_filename("//blp/a/b")
        );

        let cache = SchemaCache::with_cache_dir(PathBuf::from("cache-root"));
        for service in ["C:foo", r"..\outside", "../outside", "/absolute"] {
            let path = cache.cache_path(service);
            assert_eq!(path.parent(), Some(cache.cache_dir.as_path()));
            assert_eq!(
                path.components().count(),
                cache.cache_dir.components().count() + 1
            );
        }
    }

    #[test]
    fn distinct_service_uris_round_trip_and_list_without_collisions() {
        let temp_dir = TempDir::new().unwrap();
        let first = "//blp/a_b";
        let second = "//blp/a/b";
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            cache.insert(first, create_test_schema(first)).unwrap();
            cache.insert(second, create_test_schema(second)).unwrap();
        }

        let reloaded = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        assert_eq!(reloaded.list(), vec![second.to_string(), first.to_string()]);
        assert_eq!(reloaded.get(first).unwrap().service, first);
        assert_eq!(reloaded.get(second).unwrap().service, second);
    }

    #[test]
    fn mismatched_embedded_service_is_not_loaded_or_listed() {
        let temp_dir = TempDir::new().unwrap();
        let expected = "//blp/refdata";
        let other = "//blp/mktdata";
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let path = cache.cache_path(expected);
        crate::cache_io::write_json_atomic(&path, &create_test_schema(other)).unwrap();

        assert!(cache.get(expected).is_none());
        assert!(!cache.contains(expected));
        assert!(cache.list().is_empty());
    }

    #[test]
    fn test_memory_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        // Initially empty
        assert!(cache.get("//blp/refdata").is_none());
        assert!(!cache.contains("//blp/refdata"));

        // Insert and retrieve
        let schema = create_test_schema("//blp/refdata");
        cache.insert("//blp/refdata", schema).unwrap();

        assert!(cache.contains("//blp/refdata"));
        let retrieved = cache.get("//blp/refdata").unwrap();
        assert_eq!(retrieved.service, "//blp/refdata");
    }

    #[test]
    fn test_disk_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Insert with one cache instance
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            cache
                .insert("//blp/refdata", create_test_schema("//blp/refdata"))
                .unwrap();
        }

        // Retrieve with a new cache instance (should load from disk)
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            let schema = cache.get("//blp/refdata").unwrap();
            assert_eq!(schema.service, "//blp/refdata");
        }
    }

    #[test]
    fn test_get_memory_does_not_load_disk() {
        let temp_dir = TempDir::new().unwrap();
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            cache
                .insert("//blp/refdata", create_test_schema("//blp/refdata"))
                .unwrap();
        }

        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        assert!(cache.get_memory("//blp/refdata").is_none());

        let schema = cache.get("//blp/refdata").unwrap();
        assert_eq!(schema.service, "//blp/refdata");
        assert!(cache.get_memory("//blp/refdata").is_some());
    }

    #[test]
    fn test_invalidate() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache
            .insert("//blp/refdata", create_test_schema("//blp/refdata"))
            .unwrap();
        assert!(cache.contains("//blp/refdata"));

        cache.invalidate("//blp/refdata").unwrap();
        assert!(!cache.contains("//blp/refdata"));
    }

    #[test]
    fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache
            .insert("//blp/refdata", create_test_schema("//blp/refdata"))
            .unwrap();
        cache
            .insert("//blp/mktdata", create_test_schema("//blp/mktdata"))
            .unwrap();

        let list = cache.list();
        assert_eq!(list.len(), 2);

        cache.clear().unwrap();
        assert!(cache.list().is_empty());
    }

    #[test]
    fn test_list() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache
            .insert("//blp/refdata", create_test_schema("//blp/refdata"))
            .unwrap();
        cache
            .insert("//blp/mktdata", create_test_schema("//blp/mktdata"))
            .unwrap();

        let list = cache.list();
        assert!(list.contains(&"//blp/refdata".to_string()));
        assert!(list.contains(&"//blp/mktdata".to_string()));
    }

    #[test]
    fn test_stats() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        let stats = cache.stats();
        assert_eq!(stats.memory_count, 0);
        assert_eq!(stats.disk_count, 0);

        cache
            .insert("//blp/refdata", create_test_schema("//blp/refdata"))
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.memory_count, 1);
        assert_eq!(stats.disk_count, 1);
    }
    #[test]
    fn disk_load_cannot_overwrite_a_concurrent_insert() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let writer = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let mut old_schema = create_test_schema(service);
        old_schema.description = "old".to_string();
        writer.insert(service, old_schema).unwrap();
        drop(writer);

        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let hook = Arc::new(SchemaBarrierHook {
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        });
        *cache.disk_read_hook.lock() = Some(Arc::clone(&hook));

        let getter_cache = cache.clone();
        let getter = std::thread::spawn(move || getter_cache.get(service).unwrap());
        hook.reached.wait();

        let mut new_schema = create_test_schema(service);
        new_schema.description = "new".to_string();
        let (started_tx, started_rx) = mpsc::channel();
        let inserter_cache = cache.clone();
        let inserter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            inserter_cache.insert(service, new_schema)
        });
        started_rx.recv().unwrap();
        assert_thread_is_blocked(&inserter);

        hook.resume.wait();
        assert_eq!(getter.join().unwrap().description, "old");
        assert_eq!(inserter.join().unwrap().unwrap().description, "new");
        *cache.disk_read_hook.lock() = None;

        assert_eq!(cache.get_memory(service).unwrap().description, "new");
        let reloaded = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        assert_eq!(reloaded.get(service).unwrap().description, "new");
    }

    #[test]
    fn in_flight_disk_load_cannot_resurrect_after_invalidation() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let writer = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        writer.insert(service, create_test_schema(service)).unwrap();
        drop(writer);

        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let hook = Arc::new(SchemaBarrierHook {
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        });
        *cache.disk_read_hook.lock() = Some(Arc::clone(&hook));

        let getter_cache = cache.clone();
        let getter = std::thread::spawn(move || getter_cache.get(service));
        hook.reached.wait();

        let (started_tx, started_rx) = mpsc::channel();
        let invalidator_cache = cache.clone();
        let invalidator = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            invalidator_cache.invalidate(service)
        });
        started_rx.recv().unwrap();
        assert_thread_is_blocked(&invalidator);

        hook.resume.wait();
        assert!(getter.join().unwrap().is_some());
        invalidator.join().unwrap().unwrap();
        *cache.disk_read_hook.lock() = None;

        assert!(cache.get_memory(service).is_none());
        assert!(cache.get(service).is_none());
    }

    #[test]
    fn invalidation_linearizes_before_a_waiting_insert() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let mut old_schema = create_test_schema(service);
        old_schema.description = "old".to_string();
        cache.insert(service, old_schema).unwrap();

        let hook = Arc::new(SchemaBarrierHook {
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        });
        *cache.invalidate_hook.lock() = Some(Arc::clone(&hook));

        let invalidator_cache = cache.clone();
        let invalidator = std::thread::spawn(move || invalidator_cache.invalidate(service));
        hook.reached.wait();
        assert_eq!(cache.get_memory(service).unwrap().description, "old");

        let mut new_schema = create_test_schema(service);
        new_schema.description = "new".to_string();
        let (started_tx, started_rx) = mpsc::channel();
        let inserter_cache = cache.clone();
        let inserter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            inserter_cache.insert(service, new_schema)
        });
        started_rx.recv().unwrap();
        assert_thread_is_blocked(&inserter);

        hook.resume.wait();
        invalidator.join().unwrap().unwrap();
        assert_eq!(inserter.join().unwrap().unwrap().description, "new");
        *cache.invalidate_hook.lock() = None;

        assert_eq!(cache.get_memory(service).unwrap().description, "new");
        let reloaded = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        assert_eq!(reloaded.get(service).unwrap().description, "new");
    }

    #[test]
    fn failed_invalidation_leaves_memory_state_intact() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        fs::create_dir(cache.cache_path(service)).unwrap();
        cache.insert_memory(service, create_test_schema(service));

        let error = cache.invalidate(service).unwrap_err();

        assert!(error.contains("cannot remove cache file"));
        assert!(cache.get_memory(service).is_some());
    }

    #[test]
    fn insert_failure_surfaces_while_published_memory_stays_available() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        fs::create_dir(cache.cache_path(service)).unwrap();

        let error = cache
            .insert(service, create_test_schema(service))
            .unwrap_err();

        assert!(error.contains("cannot replace cache file"));
        assert!(cache.get_memory(service).is_some());
    }

    #[test]
    fn cold_load_rejects_oversized_schema_file() {
        let temp_dir = TempDir::new().unwrap();
        let service = "//blp/refdata";
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        let path = cache.cache_path(service);
        let file = fs::File::create(path).unwrap();
        file.set_len(MAX_SCHEMA_CACHE_FILE_BYTES + 1).unwrap();
        drop(file);

        assert!(cache.get(service).is_none());
        assert!(cache.get_memory(service).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cold_load_lock_coalesces_concurrent_cache_fills() {
        const CALLERS: usize = 8;
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(SchemaCache::with_cache_dir(temp_dir.path().to_path_buf()));
        let arrived = Arc::new(tokio::sync::Barrier::new(CALLERS));
        let load_count = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::with_capacity(CALLERS);

        for _ in 0..CALLERS {
            let cache = Arc::clone(&cache);
            let arrived = Arc::clone(&arrived);
            let load_count = Arc::clone(&load_count);
            callers.push(tokio::spawn(async move {
                assert!(cache.get_memory("//blp/refdata").is_none());
                arrived.wait().await;

                let _load = cache.lock_load().await;
                if let Some(schema) = cache.get_memory("//blp/refdata") {
                    return schema;
                }

                load_count.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
                cache.insert_memory("//blp/refdata", create_test_schema("//blp/refdata"))
            }));
        }

        for caller in callers {
            assert_eq!(caller.await.unwrap().service, "//blp/refdata",);
        }
        assert_eq!(load_count.load(Ordering::Relaxed), 1);
    }
}
