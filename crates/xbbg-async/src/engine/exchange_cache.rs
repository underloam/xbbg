use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use xbbg_ext::{ExchangeInfo, ExchangeInfoSource};

use crate::cache_io::{AtomicJsonPublisher, PublicationOutcome, read_json_array_bounded};

/// Days an exchange cache entry stays valid. Exchange metadata (timezones,
/// session hours) drifts rarely; a month bounds staleness without forcing
/// per-run Bloomberg lookups.
pub(crate) const EXCHANGE_CACHE_TTL_DAYS: i64 = 30;

fn is_fresh(info: &ExchangeInfo) -> bool {
    info.cached_at.is_some_and(|cached_at| {
        Utc::now().signed_duration_since(cached_at)
            <= chrono::Duration::days(EXCHANGE_CACHE_TTL_DAYS)
    })
}

fn eviction_timestamp(info: &ExchangeInfo) -> DateTime<Utc> {
    info.cached_at.unwrap_or(DateTime::<Utc>::MIN_UTC)
}

const DEFAULT_MAX_EXCHANGE_CACHE_ENTRIES: usize = 16_384;
const MAX_EXCHANGE_CACHE_FILE_BYTES: u64 = 32 * 1024 * 1024;

/// In-memory + disk cache for exchange metadata.
///
/// In-memory reads are lock-free (atomic pointer load) via `ArcSwap`; writers
/// mutate a mutex-protected source-of-truth map and publish cloned snapshots.
/// Disk is loaded lazily at most once via `OnceLock`.
///
/// Entries carry a 30-day TTL ([`EXCHANGE_CACHE_TTL_DAYS`]): expired entries
/// (and legacy entries without a `cached_at` stamp) are served as misses and
/// replaced by the caller's next resolution `put`; expired disk entries are
/// skipped at load. Use [`ExchangeCache::invalidate`] for manual eviction.
pub(crate) struct ExchangeCache {
    cache: ArcSwap<HashMap<String, ExchangeInfo>>,
    write_cache: Mutex<HashMap<String, ExchangeInfo>>,
    cache_path: PathBuf,
    loaded: OnceLock<Result<(), String>>,
    max_entries: usize,
    publisher: AtomicJsonPublisher,
}

impl ExchangeCache {
    pub(crate) fn new() -> Self {
        Self::with_cache_path(Self::default_cache_path())
    }

    pub(crate) fn with_cache_path(path: PathBuf) -> Self {
        Self::with_cache_path_and_max_entries(path, DEFAULT_MAX_EXCHANGE_CACHE_ENTRIES)
    }

    pub(crate) fn with_cache_path_and_max_entries(path: PathBuf, max_entries: usize) -> Self {
        Self {
            cache: ArcSwap::from_pointee(HashMap::new()),
            write_cache: Mutex::new(HashMap::new()),
            cache_path: path,
            loaded: OnceLock::new(),
            max_entries,
            publisher: AtomicJsonPublisher::default(),
        }
    }

    pub(crate) fn get(&self, ticker: &str) -> Option<ExchangeInfo> {
        let _ = self.ensure_loaded();
        let key = ticker.trim();
        if key.is_empty() {
            return None;
        }
        self.cache
            .load()
            .get(key)
            .filter(|info| is_fresh(info))
            .cloned()
            .map(ExchangeInfo::as_cache_hit)
    }

    pub(crate) fn put(&self, ticker: &str, info: ExchangeInfo) {
        self.put_many(std::iter::once((ticker, info)));
    }

    pub(crate) fn put_many<I, S>(&self, entries: I)
    where
        I: IntoIterator<Item = (S, ExchangeInfo)>,
        S: AsRef<str>,
    {
        let _ = self.ensure_loaded();

        let (previous_snapshot, replaced, evicted) = {
            let mut write_cache = self.write_cache.lock();
            let mut replaced = Vec::new();
            let mut changed = false;
            for (ticker, mut info) in entries {
                let key = ticker.as_ref().trim();
                if key.is_empty() {
                    continue;
                }
                info.cached_at = Some(Utc::now());
                if info.source == ExchangeInfoSource::Fallback {
                    info.source = ExchangeInfoSource::Bloomberg;
                }
                if let Some(previous) = write_cache.insert(key.to_string(), info) {
                    replaced.push(previous);
                }
                changed = true;
            }

            if !changed {
                return;
            }

            let evicted = Self::evict_oldest(&mut write_cache, self.max_entries);
            let previous_snapshot = self.swap_snapshot(&write_cache);
            (previous_snapshot, replaced, evicted)
        };

        drop(previous_snapshot);
        drop(evicted);
        drop(replaced);
    }

    pub(crate) fn invalidate(&self, ticker: Option<&str>) -> Result<(), String> {
        let load_result = self.ensure_loaded();
        if ticker.is_some_and(|ticker| !ticker.trim().is_empty()) {
            load_result?;
        }
        let (previous_snapshot, removed) = {
            let mut write_cache = self.write_cache.lock();
            match ticker {
                Some(ticker) if !ticker.trim().is_empty() => {
                    let key = ticker.trim();
                    let mut next = write_cache.clone();
                    let Some(removed) = next.remove_entry(key) else {
                        return Ok(());
                    };

                    let publication = self.publisher.begin();
                    let entries: Vec<ExchangeInfo> = next.values().cloned().collect();
                    let outcome = publication.publish(&self.cache_path, &entries)?;
                    debug_assert_eq!(outcome, PublicationOutcome::Published);

                    let next_snapshot = Arc::new(next.clone());
                    *write_cache = next;
                    (self.cache.swap(next_snapshot), vec![removed])
                }
                _ => {
                    let publication = self.publisher.begin();
                    let outcome = publication.remove(&self.cache_path)?;
                    debug_assert_eq!(outcome, PublicationOutcome::Published);

                    let removed: Vec<(String, ExchangeInfo)> = write_cache.drain().collect();
                    let previous_snapshot = self.cache.swap(Arc::new(HashMap::new()));
                    (previous_snapshot, removed)
                }
            }
        };

        drop(previous_snapshot);
        drop(removed);
        Ok(())
    }

    pub(crate) fn save_to_disk(&self) -> Result<(), String> {
        let _ = self.ensure_loaded();

        let (publication, snapshot) = {
            let _write_cache = self.write_cache.lock();
            (self.publisher.begin(), self.cache.load_full())
        };
        let entries: Vec<ExchangeInfo> = snapshot.values().cloned().collect();
        match publication.publish(&self.cache_path, &entries)? {
            PublicationOutcome::Published => Ok(()),
            PublicationOutcome::Superseded => {
                xbbg_log::debug!(path = %self.cache_path.display(), "skipped superseded exchange cache snapshot");
                Ok(())
            }
        }
    }

    /// Eagerly load the on-disk cache (idempotent).
    pub(crate) fn preload(&self) -> Result<(), String> {
        self.ensure_loaded()
    }

    fn swap_snapshot(
        &self,
        write_cache: &HashMap<String, ExchangeInfo>,
    ) -> Arc<HashMap<String, ExchangeInfo>> {
        self.cache.swap(Arc::new(write_cache.clone()))
    }

    fn evict_oldest(
        cache: &mut HashMap<String, ExchangeInfo>,
        max_entries: usize,
    ) -> Vec<(String, ExchangeInfo)> {
        let excess = cache.len().saturating_sub(max_entries);
        if excess == 0 {
            return Vec::new();
        }

        let mut oldest: Vec<(&str, DateTime<Utc>)> = cache
            .iter()
            .map(|(key, info)| (key.as_str(), eviction_timestamp(info)))
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
            .filter_map(|key| cache.remove_entry(&key))
            .collect()
    }

    fn ensure_loaded(&self) -> Result<(), String> {
        self.loaded.get_or_init(|| self.load_from_disk()).clone()
    }

    fn load_from_disk(&self) -> Result<(), String> {
        if !self.cache_path.exists() {
            return Ok(());
        }
        let entries: Vec<ExchangeInfo> = match read_json_array_bounded(
            &self.cache_path,
            MAX_EXCHANGE_CACHE_FILE_BYTES,
            self.max_entries,
        ) {
            Ok(entries) => entries,
            Err(error) => {
                xbbg_log::warn!(error = %error, path = %self.cache_path.display(), "failed to load exchange cache");
                return Err(error);
            }
        };

        let pairs: Vec<(String, ExchangeInfo)> = entries
            .into_iter()
            // Drop entries that already exceeded the TTL (or predate the
            // cached_at stamp) so stale disk state never reaches the map.
            .filter(is_fresh)
            .map(|mut entry| {
                entry.source = ExchangeInfoSource::Cache;
                (entry.ticker.clone(), entry)
            })
            .collect();

        if !pairs.is_empty() {
            let (previous_snapshot, evicted) = {
                let mut write_cache = self.write_cache.lock();
                write_cache.extend(pairs);
                let evicted = Self::evict_oldest(&mut write_cache, self.max_entries);
                let previous_snapshot = self.swap_snapshot(&write_cache);
                (previous_snapshot, evicted)
            };
            drop(previous_snapshot);
            drop(evicted);
        }

        Ok(())
    }

    fn default_cache_path() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        home.unwrap_or_else(|| PathBuf::from("."))
            .join(".xbbg")
            .join("cache")
            .join("exchanges.json")
    }
}

impl Default for ExchangeCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::fs;

    fn info(ticker: &str, cached_at: Option<chrono::DateTime<Utc>>) -> ExchangeInfo {
        ExchangeInfo {
            cached_at,
            source: ExchangeInfoSource::Bloomberg,
            ..ExchangeInfo::fallback(ticker)
        }
    }

    fn temp_cache_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "xbbg-exchange-cache-test-{name}-{}.json",
            std::process::id()
        ))
    }

    #[test]
    fn fresh_put_round_trips() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("fresh"));
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        let hit = cache.get("AAPL US Equity").expect("fresh entry should hit");
        assert_eq!(hit.source, ExchangeInfoSource::Cache);
        assert!(hit.cached_at.is_some(), "put must stamp cached_at");
    }

    #[test]
    fn put_many_publishes_one_snapshot_and_reads_reflect_writes() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("batch"));
        let before = cache.cache.load_full();

        cache.put_many([
            ("AAPL US Equity", info("AAPL US Equity", None)),
            ("MSFT US Equity", info("MSFT US Equity", None)),
        ]);

        let after = cache.cache.load_full();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.len(), 2);
        assert!(cache.get("AAPL US Equity").is_some());
        assert!(cache.get("MSFT US Equity").is_some());

        let after_reads = cache.cache.load_full();
        let _ = cache.get("AAPL US Equity");
        let _ = cache.get("MSFT US Equity");
        assert!(Arc::ptr_eq(&after_reads, &cache.cache.load_full()));
    }

    #[test]
    fn evicts_oldest_entry_when_bound_is_exceeded() {
        let cache = ExchangeCache::with_cache_path_and_max_entries(temp_cache_path("evict"), 1);
        cache.put("OLD US Equity", info("OLD US Equity", None));
        std::thread::sleep(std::time::Duration::from_millis(1));
        cache.put("NEW US Equity", info("NEW US Equity", None));

        assert!(cache.get("OLD US Equity").is_none());
        assert!(cache.get("NEW US Equity").is_some());
        assert_eq!(cache.cache.load().len(), 1);
    }

    #[test]
    fn expired_entry_is_a_miss() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("expired"));
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        // Overwrite the stamp with one beyond the TTL in the read snapshot.
        let mut next = (*cache.cache.load_full()).clone();
        if let Some(entry) = next.get_mut("AAPL US Equity") {
            entry.cached_at = Some(Utc::now() - Duration::days(EXCHANGE_CACHE_TTL_DAYS + 1));
        }
        cache.cache.store(Arc::new(next));
        assert!(
            cache.get("AAPL US Equity").is_none(),
            "expired entry must miss"
        );
    }

    #[test]
    fn legacy_entry_without_stamp_is_a_miss() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("legacy"));
        let mut next = HashMap::new();
        next.insert("IBM US Equity".to_string(), info("IBM US Equity", None));
        cache.cache.store(Arc::new(next));
        assert!(cache.get("IBM US Equity").is_none());
    }

    #[test]
    fn load_from_disk_skips_expired_entries() {
        let path = temp_cache_path("disk");
        let fresh = info("FRESH US Equity", Some(Utc::now()));
        let stale = info(
            "STALE US Equity",
            Some(Utc::now() - Duration::days(EXCHANGE_CACHE_TTL_DAYS + 1)),
        );
        let payload = serde_json::to_string(&vec![&fresh, &stale]).unwrap();
        std::fs::write(&path, payload).unwrap();

        let cache = ExchangeCache::with_cache_path(path.clone());
        assert!(cache.get("FRESH US Equity").is_some());
        assert!(cache.get("STALE US Equity").is_none());
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn saved_snapshot_round_trips_through_atomic_publication() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        let cache = ExchangeCache::with_cache_path(path.clone());
        cache.put("AAPL US Equity", info("AAPL US Equity", None));

        cache.save_to_disk().unwrap();

        let reloaded = ExchangeCache::with_cache_path(path);
        assert!(reloaded.get("AAPL US Equity").is_some());
    }

    #[test]
    fn replacement_failure_surfaces_from_save() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        fs::create_dir(&path).unwrap();
        let cache = ExchangeCache::with_cache_path(path);
        cache.put("AAPL US Equity", info("AAPL US Equity", None));

        let error = cache.save_to_disk().unwrap_err();

        assert!(error.contains("cannot replace cache file"));
    }

    #[test]
    fn invalidation_updates_the_persisted_cache() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        let cache = ExchangeCache::with_cache_path(path.clone());
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        cache.save_to_disk().unwrap();

        cache.invalidate(Some("AAPL US Equity")).unwrap();

        let reloaded = ExchangeCache::with_cache_path(path);
        assert!(reloaded.get("AAPL US Equity").is_none());
    }

    #[test]
    fn failed_invalidation_keeps_memory_state_intact() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        let cache = ExchangeCache::with_cache_path(path.clone());
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        fs::create_dir(&path).unwrap();

        let error = cache.invalidate(Some("AAPL US Equity")).unwrap_err();

        assert!(error.contains("cannot replace cache file"));
        assert!(cache.get("AAPL US Equity").is_some());
    }

    #[test]
    fn targeted_invalidation_propagates_a_cold_load_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        let invalid = b"not json";
        fs::write(&path, invalid).unwrap();
        let cache = ExchangeCache::with_cache_path(path.clone());

        let error = cache.invalidate(Some("AAPL US Equity")).unwrap_err();

        assert!(error.contains("cannot parse cache file"));
        assert_eq!(fs::read(path).unwrap(), invalid);
    }

    #[test]
    fn preload_reports_corrupt_cache() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        fs::write(&path, b"not json").unwrap();
        let cache = ExchangeCache::with_cache_path(path);

        let error = cache.preload().unwrap_err();

        assert!(error.contains("cannot parse cache file"));
        assert!(cache.get("AAPL US Equity").is_none());
    }

    #[test]
    fn preload_rejects_more_than_configured_entry_bound() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("exchanges.json");
        let entries = vec![
            info("A US Equity", Some(Utc::now())),
            info("B US Equity", Some(Utc::now())),
            info("C US Equity", Some(Utc::now())),
        ];
        fs::write(&path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let cache = ExchangeCache::with_cache_path_and_max_entries(path, 2);

        let error = cache.preload().unwrap_err();

        assert!(error.contains("2-entry limit"));
        assert!(cache.get("A US Equity").is_none());
    }
}
