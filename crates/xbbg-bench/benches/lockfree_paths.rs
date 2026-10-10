//! Microbenches for the lock-free cache and subscription-status paths.
//!
//! Measures absolute latency for:
//! - FieldTypeResolver: get (read), insert (per-key write), bulk insert, resolve_types
//! - SubscriptionStatusState: read path, rcu writes of various kinds, burst writes
//!
//! exchange_cache: skipped — `exchange_cache` is a private module (`mod exchange_cache;`)
//! inside `engine/mod.rs` and `ExchangeCache` is not re-exported publicly. Adding a
//! bench-internals gate for it is out of scope; the FieldTypeResolver benches already
//! cover the identical ArcSwap/RCU pattern.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

use xbbg_async::engine::state::SubscriptionMetrics;
use xbbg_async::engine::{
    SharedSubscriptionStatus, SlabKey, SubscriptionEventLevel, SubscriptionStatusHandle,
    SubscriptionStatusState,
};
use xbbg_async::field_cache::{FieldInfo, FieldTypeResolver};
fn print_provenance_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        println!(
            "XBBG_BENCH_PROVENANCE {}",
            xbbg_bench::benchmark_provenance_json(
                "offline lock-free field-cache and subscription-status microbenchmarks; synthetic inputs; no Bloomberg SDK or network"
            )
        );
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_field_info(i: usize) -> FieldInfo {
    FieldInfo {
        field_id: format!("F{i}"),
        arrow_type: "float64".into(),
        description: String::new(),
        category: String::new(),
    }
}

fn make_resolver(n: usize) -> FieldTypeResolver {
    let resolver = FieldTypeResolver::new();
    for i in 0..n {
        resolver.insert(make_field_info(i));
    }
    resolver
}

fn make_metrics() -> Arc<SubscriptionMetrics> {
    Arc::new(SubscriptionMetrics {
        messages_received: Arc::new(AtomicU64::new(0)),
        dropped_batches: Arc::new(AtomicU64::new(0)),
        batches_sent: Arc::new(AtomicU64::new(0)),
        slow_consumer: Arc::new(AtomicBool::new(false)),
        data_loss_events: Arc::new(AtomicU64::new(0)),
        last_message_us: Arc::new(AtomicU64::new(0)),
        last_data_loss_us: Arc::new(AtomicU64::new(0)),
    })
}

fn make_status_state(n_topics: usize) -> SubscriptionStatusState {
    let topics: Vec<String> = (0..n_topics)
        .map(|i| format!("TOPIC{i} US Equity"))
        .collect();
    let keys: Vec<SlabKey> = (0..n_topics).collect();
    let metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>> =
        keys.iter().map(|&k| (k, make_metrics())).collect();
    SubscriptionStatusState::from_active(topics, keys, metrics)
}

fn make_shared_status(n_topics: usize) -> SharedSubscriptionStatus {
    Arc::new(SubscriptionStatusHandle::new(make_status_state(n_topics)))
}

// ---------------------------------------------------------------------------
// Field cache benches
// ---------------------------------------------------------------------------

fn bench_field_cache_get(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("field_cache_get");
    for size in [100, 5000] {
        let resolver = make_resolver(size);
        let key = format!("F{}", size / 2);
        group.bench_with_input(BenchmarkId::new("entries", size), &size, |b, _| {
            b.iter(|| {
                // Look up a prebuilt key near the middle — definitely present.
                black_box(resolver.get(black_box(key.as_str())))
            });
        });
    }
    group.finish();
}

fn bench_field_cache_insert(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("field_cache_insert");
    // Pre-populate 1000 entries; then measure per-insert RCU cost.
    group.bench_function("single_key_on_1000_entry_cache", |b| {
        let resolver = make_resolver(1000);
        let mut counter = 1000usize;
        b.iter_batched(
            || {
                counter += 1;
                make_field_info(counter)
            },
            |info| resolver.insert(black_box(info)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_field_cache_bulk_insert(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("field_cache_bulk_insert");
    // Simulate insert_from_response: 100 entries in a single RCU via repeated insert()
    // calls (worst-case unbatched). Also bench a manual single-RCU equivalent.
    group.bench_function("100x_insert_on_1000_entry_cache", |b| {
        b.iter_batched(
            || make_resolver(1000),
            |resolver| {
                for i in 2000..2100 {
                    resolver.insert(black_box(make_field_info(i)));
                }
                black_box(resolver)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Single-RCU bulk path: build all entries, extend in one shot.
    // Mirrors what insert_from_response() does internally.
    group.bench_function("single_rcu_bulk_100_on_1000_entry_cache", |b| {
        b.iter_batched(
            || {
                let resolver = make_resolver(1000);
                let entries: Vec<(String, FieldInfo)> = (2000..2100)
                    .map(|i| {
                        let info = make_field_info(i);
                        (info.field_id.to_uppercase(), info)
                    })
                    .collect();
                (resolver, entries)
            },
            |(resolver, entries)| {
                // Replicate the single-RCU pattern from insert_from_response.
                resolver.cache_rcu_extend(black_box(entries));
                black_box(resolver)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_field_cache_resolve_types(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("field_cache_resolve_types");
    let resolver = make_resolver(1000);
    // 50 fields, all present in the 1000-entry cache.
    let fields: Vec<String> = (0..50).map(|i| format!("F{}", i * 10)).collect();
    group.bench_function("50_fields_on_1000_entry_cache", |b| {
        b.iter(|| black_box(resolver.resolve_types(black_box(&fields), None, "float64")));
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// SubscriptionStatusState benches
// ---------------------------------------------------------------------------

fn bench_status_load_topic_for_key(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("subscription_status_read");
    for n_topics in [10, 50, 100] {
        let shared = make_shared_status(n_topics);
        let key: SlabKey = n_topics / 2;
        group.bench_with_input(
            BenchmarkId::new("load_topic_for_key_borrowed", n_topics),
            &n_topics,
            |b, _| {
                b.iter(|| {
                    let snap = shared.load();
                    if let Some(topic) = snap.topic_for_key(black_box(key)) {
                        black_box(topic);
                    }
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("load_topic_for_key_owned_string", n_topics),
            &n_topics,
            |b, _| {
                b.iter(|| {
                    let snap = shared.load();
                    black_box(snap.topic_for_key(black_box(key)).map(str::to_string))
                });
            },
        );
    }
    group.finish();
}

fn bench_status_rcu_record_subscription_event(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("subscription_status_rcu_subscription_event");
    for n_topics in [10, 100] {
        let shared = make_shared_status(n_topics);
        let topic = "TOPIC0 US Equity".to_string();
        group.bench_with_input(BenchmarkId::new("topics", n_topics), &n_topics, |b, _| {
            b.iter_batched(
                || topic.clone(),
                |topic| {
                    shared.update(|next| {
                        next.record_subscription_event(
                            black_box("BenchEvent"),
                            black_box(Some(topic)),
                            black_box(None),
                            black_box(SubscriptionEventLevel::Info),
                        );
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_status_rcu_record_service_state(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("subscription_status_rcu_service_state");
    for n_topics in [10, 100] {
        let shared = make_shared_status(n_topics);
        let service = "//blp/mktdata".to_string();
        group.bench_with_input(BenchmarkId::new("topics", n_topics), &n_topics, |b, _| {
            b.iter_batched(
                || service.clone(),
                |service| {
                    shared.update(|next| {
                        next.record_service_state(
                            black_box(service),
                            black_box(true),
                            black_box("ServiceOpened"),
                            black_box(None),
                        );
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_status_rcu_mark_topic_streaming(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("subscription_status_rcu_mark_streaming");
    for n_topics in [10, 100] {
        let shared = make_shared_status(n_topics);
        let key: SlabKey = 0;
        group.bench_with_input(BenchmarkId::new("topics", n_topics), &n_topics, |b, _| {
            b.iter(|| {
                shared.update(|next| {
                    next.mark_topic_streaming(black_box(key));
                });
            });
        });
    }
    group.finish();
}

fn bench_status_rcu_burst(c: &mut Criterion) {
    print_provenance_once();
    let mut group = c.benchmark_group("subscription_status_burst_100_writes");
    let shared = make_shared_status(50);
    let topics: Vec<String> = (0..100u64)
        .map(|i| format!("TOPIC{} US Equity", i % 50))
        .collect();
    group.bench_function("50_topic_state_100_sequential_rcus", |b| {
        b.iter_batched(
            || topics.clone(),
            |topics| {
                for topic in topics {
                    shared.update(|next| {
                        next.record_subscription_event(
                            black_box("BenchBurst"),
                            black_box(Some(topic)),
                            black_box(None),
                            black_box(SubscriptionEventLevel::Info),
                        );
                    });
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    field_cache,
    bench_field_cache_get,
    bench_field_cache_insert,
    bench_field_cache_bulk_insert,
    bench_field_cache_resolve_types,
);

criterion_group!(
    subscription_status,
    bench_status_load_topic_for_key,
    bench_status_rcu_record_subscription_event,
    bench_status_rcu_record_service_state,
    bench_status_rcu_mark_topic_streaming,
    bench_status_rcu_burst,
);

criterion_main!(field_cache, subscription_status);
