//! Bloomberg schema introspection with a per-engine JSON cache.
//!
//! A host keeps one [`SchemaJsonCache`] per engine. Service and operation
//! schemas are serialized once and shared as `Arc<str>` (napi
//! `SchemaJsonCache` semantics): a hit never touches the engine, invalidating
//! a service drops it and every cached operation of that service, and clearing
//! resets the whole cache. Locks are poison-tolerant and never held across an
//! await. The pass-through calls (`list_operations`, `list_cached_schemas`,
//! `enum_values`, `valid_elements`) only classify errors.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use xbbg_async::engine::Engine;

use crate::errors::HostError;

#[derive(Default)]
struct Entries {
    services: HashMap<String, Arc<str>>,
    operations: HashMap<(String, String), Arc<str>>,
}

/// Serialized schema JSON for one engine.
#[derive(Default)]
pub struct SchemaJsonCache {
    entries: Mutex<Entries>,
}

impl SchemaJsonCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The service schema as JSON, introspecting it on a miss.
    pub async fn schema(&self, engine: &Engine, service: &str) -> Result<Arc<str>, HostError> {
        if let Some(hit) = self.lock().services.get(service).cloned() {
            return Ok(hit);
        }
        let schema = engine
            .get_schema(service)
            .await
            .map_err(HostError::from_async)?;
        let json: Arc<str> = serde_json::to_string(&*schema)
            .map_err(|error| HostError::internal(format!("serialize schema: {error}")))?
            .into();
        self.lock()
            .services
            .insert(service.to_owned(), Arc::clone(&json));
        Ok(json)
    }

    /// One operation's schema as JSON; a missing operation is VALIDATION.
    pub async fn operation(
        &self,
        engine: &Engine,
        service: &str,
        operation: &str,
    ) -> Result<Arc<str>, HostError> {
        let key = (service.to_owned(), operation.to_owned());
        if let Some(hit) = self.lock().operations.get(&key).cloned() {
            return Ok(hit);
        }
        let schema = engine
            .get_operation(service, operation)
            .await
            .map_err(HostError::from_async)?;
        let json: Arc<str> = serde_json::to_string(&schema)
            .map_err(|error| HostError::internal(format!("serialize operation: {error}")))?
            .into();
        self.lock().operations.insert(key, Arc::clone(&json));
        Ok(json)
    }

    /// A schema already in memory, without introspection or disk I/O.
    pub fn cached_schema(&self, engine: &Engine, service: &str) -> Option<Arc<str>> {
        if let Some(hit) = self.lock().services.get(service).cloned() {
            return Some(hit);
        }
        let json: Arc<str> = engine
            .get_cached_schema(service)
            .and_then(|schema| serde_json::to_string(&*schema).ok())?
            .into();
        self.lock()
            .services
            .insert(service.to_owned(), Arc::clone(&json));
        Some(json)
    }

    /// Drop `service` from the engine caches (memory and disk) and from here.
    pub fn invalidate(&self, engine: &Engine, service: &str) -> Result<(), HostError> {
        engine
            .invalidate_schema(service)
            .map_err(HostError::internal)?;
        self.forget(service);
        Ok(())
    }

    /// Clear every cached schema in the engine and here.
    pub fn clear(&self, engine: &Engine) -> Result<(), HostError> {
        engine.clear_schema_cache().map_err(HostError::internal)?;
        *self.lock() = Entries::default();
        Ok(())
    }

    /// Forget the service document and every operation document of `service`.
    fn forget(&self, service: &str) {
        let mut entries = self.lock();
        entries.services.remove(service);
        entries
            .operations
            .retain(|(cached_service, _), _| cached_service != service);
    }
}

/// Operation names of `service`.
pub async fn list_operations(engine: &Engine, service: &str) -> Result<Vec<String>, HostError> {
    engine
        .list_operations(service)
        .await
        .map_err(HostError::from_async)
}

/// Services whose schema is cached in memory or on disk.
pub fn list_cached_schemas(engine: &Engine) -> Vec<String> {
    engine.list_cached_schemas()
}

/// Valid values of an enumerated request element, `None` when not an enum.
pub async fn enum_values(
    engine: &Engine,
    service: &str,
    operation: &str,
    element: &str,
) -> Result<Option<Vec<String>>, HostError> {
    engine
        .get_enum_values(service, operation, element)
        .await
        .map_err(HostError::from_async)
}

/// Request element names of an operation.
pub async fn valid_elements(
    engine: &Engine,
    service: &str,
    operation: &str,
) -> Result<Option<Vec<String>>, HostError> {
    engine
        .list_valid_elements(service, operation)
        .await
        .map_err(HostError::from_async)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeded() -> SchemaJsonCache {
        let cache = SchemaJsonCache::new();
        {
            let mut entries = cache.lock();
            entries.services.insert("//blp/refdata".into(), "{}".into());
            entries.services.insert("//blp/apiflds".into(), "{}".into());
            for (service, operation) in [
                ("//blp/refdata", "ReferenceDataRequest"),
                ("//blp/refdata", "HistoricalDataRequest"),
                ("//blp/apiflds", "FieldInfoRequest"),
            ] {
                entries
                    .operations
                    .insert((service.into(), operation.into()), "{}".into());
            }
        }
        cache
    }

    fn keys(cache: &SchemaJsonCache) -> (Vec<String>, Vec<(String, String)>) {
        let entries = cache.lock();
        let mut services: Vec<_> = entries.services.keys().cloned().collect();
        let mut operations: Vec<_> = entries.operations.keys().cloned().collect();
        services.sort();
        operations.sort();
        (services, operations)
    }

    #[test]
    fn forgetting_a_service_drops_only_its_documents() {
        let cache = seeded();
        cache.forget("//blp/refdata");
        assert_eq!(
            keys(&cache),
            (
                vec!["//blp/apiflds".to_string()],
                vec![("//blp/apiflds".to_string(), "FieldInfoRequest".to_string())]
            )
        );
        // A service that was never cached is a no-op.
        cache.forget("//blp/mktdata");
        assert_eq!(keys(&cache).1.len(), 1);
    }

    #[test]
    fn a_poisoned_lock_keeps_the_cache_usable() {
        let cache = Arc::new(seeded());
        let poisoner = Arc::clone(&cache);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock();
            panic!("poison the cache lock");
        })
        .join();
        assert!(cache.entries.is_poisoned());
        cache.forget("//blp/apiflds");
        assert_eq!(keys(&cache).0, ["//blp/refdata"]);
    }
}
