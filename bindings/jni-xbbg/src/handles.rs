//! Native objects addressed from Java by `long` handles.
//!
//! Ids start at 1, increase monotonically and are never reused, so a stale
//! Java handle can only ever miss (the caller reports its STATE error, such as
//! `engine is closed`); it can never reach a newer object. JVM shutdown closes
//! a table: closing drains it and refuses every later insert, so an object
//! registered while shutdown runs is handed back instead of being missed.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// `u64 -> Arc<T>` map with monotonic, never-reused ids.
pub(crate) struct HandleTable<T> {
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    next: u64,
    entries: BTreeMap<u64, Arc<T>>,
    closed: bool,
}

impl<T> HandleTable<T> {
    pub(crate) const fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next: 1,
                entries: BTreeMap::new(),
                closed: false,
            }),
        }
    }

    /// Store `value` and return its new id; a closed table hands `value` back.
    pub(crate) fn insert(&self, value: Arc<T>) -> Result<u64, Arc<T>> {
        let mut inner = self.inner.lock();
        if inner.closed {
            return Err(value);
        }
        let id = inner.next;
        inner.next = id.checked_add(1).expect("handle ids are never exhausted");
        inner.entries.insert(id, value);
        Ok(id)
    }

    /// The live object for `id`, if any.
    pub(crate) fn get(&self, id: u64) -> Option<Arc<T>> {
        self.inner.lock().entries.get(&id).cloned()
    }

    /// Remove `id`, returning its object if it was live.
    pub(crate) fn remove(&self, id: u64) -> Option<Arc<T>> {
        self.inner.lock().entries.remove(&id)
    }

    /// Refuse every later insert and remove every live object.
    pub(crate) fn close(&self) -> Vec<Arc<T>> {
        let mut inner = self.inner.lock();
        inner.closed = true;
        std::mem::take(&mut inner.entries).into_values().collect()
    }

    /// Whether [`HandleTable::close`] ran.
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    /// Number of live objects.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }
}

/// A Java `long` handle as a table id; negative values never match.
pub(crate) fn handle_id(handle: i64) -> u64 {
    u64::try_from(handle).unwrap_or(0)
}

/// A table id as a Java `long`.
pub(crate) fn java_handle(id: u64) -> i64 {
    i64::try_from(id).expect("handle ids stay below i64::MAX")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn ids_start_at_one_and_are_never_reused() {
        let table = HandleTable::new();
        let first = table.insert(Arc::new("a")).unwrap();
        let second = table.insert(Arc::new("b")).unwrap();
        assert_eq!((first, second), (1, 2));
        assert_eq!(table.remove(first).as_deref(), Some(&"a"));
        assert_eq!(table.remove(first), None);
        assert_eq!(table.get(first), None);
        let third = table.insert(Arc::new("c")).unwrap();
        assert_eq!(third, 3);
        assert_eq!(table.get(second).as_deref(), Some(&"b"));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn removal_keeps_objects_alive_for_existing_holders() {
        let table = HandleTable::new();
        let id = table.insert(Arc::new(String::from("engine"))).unwrap();
        let held = table.get(id).expect("live");
        let removed = table.remove(id).expect("removed once");
        assert_eq!(Arc::strong_count(&held), 2);
        drop(removed);
        assert_eq!(*held, "engine");
        assert_eq!(table.get(id), None);
    }

    #[test]
    fn close_empties_the_table_and_hands_later_inserts_back() {
        let table = HandleTable::new();
        for value in 0..3 {
            table.insert(Arc::new(value)).unwrap();
        }
        assert!(!table.is_closed());
        let drained: Vec<i32> = table.close().into_iter().map(|value| *value).collect();
        assert_eq!(drained, [0, 1, 2]);
        assert!(table.is_closed());
        assert_eq!(table.len(), 0);
        let refused = table.insert(Arc::new(9)).expect_err("closed");
        assert_eq!(*refused, 9);
        assert_eq!(table.len(), 0);
        assert!(table.close().is_empty());
    }

    #[test]
    fn inserts_racing_close_are_either_drained_or_handed_back() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;
        static TABLE: HandleTable<usize> = HandleTable::new();
        let start = Arc::new(Barrier::new(THREADS + 1));
        let workers: Vec<_> = (0..THREADS)
            .map(|worker| {
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    let (mut kept, mut refused) = (Vec::new(), Vec::new());
                    for index in 0..PER_THREAD {
                        let value = worker * PER_THREAD + index;
                        match TABLE.insert(Arc::new(value)) {
                            Ok(_) => kept.push(value),
                            Err(back) => refused.push(*back),
                        }
                    }
                    (kept, refused)
                })
            })
            .collect();
        start.wait();
        thread::yield_now();
        let drained: BTreeSet<usize> = TABLE.close().into_iter().map(|value| *value).collect();
        let mut kept = BTreeSet::new();
        let mut refused = BTreeSet::new();
        for worker in workers {
            let (worker_kept, worker_refused) = worker.join().expect("worker");
            kept.extend(worker_kept);
            refused.extend(worker_refused);
        }
        assert_eq!(drained, kept, "every accepted insert was drained");
        assert!(kept.is_disjoint(&refused));
        assert_eq!(kept.len() + refused.len(), THREADS * PER_THREAD);
        assert_eq!(TABLE.len(), 0, "nothing stays registered after close");
    }

    #[test]
    fn concurrent_inserts_get_distinct_ids() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 100;
        static TABLE: HandleTable<usize> = HandleTable::new();
        let start = Arc::new(Barrier::new(THREADS));
        let workers: Vec<_> = (0..THREADS)
            .map(|worker| {
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    (0..PER_THREAD)
                        .map(|index| {
                            TABLE
                                .insert(Arc::new(worker * PER_THREAD + index))
                                .expect("open table")
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut ids: Vec<u64> = workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("worker"))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), THREADS * PER_THREAD);
        assert_eq!(ids.first(), Some(&1));
        assert_eq!(ids.last(), Some(&((THREADS * PER_THREAD) as u64)));
    }

    #[test]
    fn java_handles_round_trip_and_reject_negatives() {
        assert_eq!(handle_id(java_handle(42)), 42);
        assert_eq!(handle_id(-1), 0);
        assert_eq!(handle_id(0), 0);
        let table: HandleTable<()> = HandleTable::new();
        assert_eq!(table.get(handle_id(-7)), None);
    }
}
