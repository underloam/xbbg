# xbbg-async

Async worker-pool engine over `xbbg-core` for Bloomberg API requests and subscriptions.

## Architecture

```
Engine
├── RequestWorkerPool        (round-robin dispatch, default 2 workers)
│   └── Worker threads       each owns a Session + Slab<UnifiedRequestState>
│       └── 12 state machines: RefData, HistData, BulkData, IntradayBar,
│           IntradayTick, HistDataStream, IntradayBarStream,
│           IntradayTickStream, Generic, Bql, Bsrch, FieldInfo
├── SubscriptionSessionPool  (1 pre-warmed session, 32-session cap by default)
│   └── Sub-worker threads   each owns a Session + Slab<SubscriptionState>
├── SchemaCache              (in-memory + disk-persisted service schemas)
├── FieldCache               (global, disk-persisted field type resolution)
└── Tokio Runtime
```

Each worker thread owns its own Bloomberg `Session` — no `Arc<Session>`, no shared
state, no contention.  Requests are dispatched round-robin across the pool;
subscriptions are claimed from a separate session pool.

## Key modules

| Module | Purpose |
|--------|---------|
| `engine/` | Engine startup, shutdown, command dispatch |
| `engine/worker.rs` | Per-worker event loop and request lifecycle |
| `engine/subscription_pool.rs` | Per-session subscription lifecycle and callbacks |
| `engine/session_lifecycle.rs` | Shared startup latch and coalesced service-open attempts |
| `engine/subscription_status.rs` | Topic indexes, lifecycle snapshots, events, and warning publication |
| `engine/request_sharding.rs` | Shard planning, ordered execution, and schema reconciliation |
| `engine/state/` | 12 state machines for different Bloomberg operations |
| `schema/` | Service schema introspection + disk cache |
| `field_cache.rs` | Global field-type resolver with disk persistence |
| `errors.rs` | `BlpAsyncError` — async-layer error type |

## Design decisions

- **One Session per thread** — Bloomberg's `Session` is not `Sync`.  Rather than
  wrapping it in a `Mutex`, each worker owns its session outright, eliminating
  contention entirely.
- **Slab-based correlation** — In-flight requests are tracked with a `Slab`,
  giving O(1) insert/remove and compact memory layout.
- **Schema + field caching** — Service schemas and field metadata are cached to
  disk, avoiding repeated introspection on startup.
- **Shared response decoders** — Historical and intraday-bar requests use the same
  extraction and Arrow builders for accumulated responses and streamed chunks.
- **Canonical format vocabulary** — Request planning uses the generated `Format`
  parser, including aliases declared in `defs/bloomberg.toml`.

## Request response decoding

Historical streaming honors field-type hints and includes `eidData`,
`securityError`, and `fieldExceptions` metadata just like accumulated responses.
Intraday-bar responses use a non-nullable `ticker` column in both delivery modes:
each row carries the request ticker, including an empty request string.

Response diagnostics use `xbbg_async::diagnostics` for compact field exceptions and
per-id field metadata errors. `xbbg.field_exceptions` writes a version-1 dictionary
of unique exception payloads with ordered per-security indices; the shared codec
also reads historical expanded maps and validates every index before merging.
FieldInfo tables retain the enclosing echoed `id` alongside the mnemonic and
preserve refused ids under `xbbg.field_errors`, including metadata-only failures.
See the [response diagnostic contract](../../README.md#response-diagnostics).

Historical and intraday-bar streams never silently drop a chunk when their
bounded channel fills. They terminate with an error after previously accepted
chunks; the SDK callback does not wait for the consumer to drain the channel.

BQL keeps separate small-payload (at most 32 KiB) and large-payload JSON parsers,
but both use the same Arrow materializer. Secondary columns with omitted values
are null-padded in either route, and their first occurrence wins when names
repeat; large payloads no longer skip those columns.

## Subscription deltas and termination

With default delta delivery, `SubscriptionStream::next()` yields sparse `SubscriptionUpdate` values. Missing
`UpdateField` entries mean unchanged; `UpdateValue::Null` is an explicit SDK
clear. Requested arrays/complex values and non-null decode failures are errors.
All-fields mode omits unrequested nonscalars without suppressing a later scalar.

Requested-field extraction uses a bounded present-field scan for wide, sparse
messages and retains name lookups for narrow or dense messages. Values remain in
requested order. Changed immutable layouts are built once per message while
preserving every discovery/type-change version increment. These optimizations
neither combine messages nor wait for another event before delivering an update.

`SubscriptionState` emits updates immediately; it has no flush threshold or
`flush()` operation. `SubscriptionArrowBatcher` owns Arrow batching. The public
`EngineConfig::subscription_flush_threshold` and `SubscribeRequest::flush_threshold`
options remain consumer-side hints used by bindings, not engine buffering settings.

Arrow conversion appends non-null binary `__xbbg_present`. Bit `i`, LSB-first,
maps to schema field `i + 2` after `timestamp` and `topic`. A present-null field
sets its bit; absence does not. Interpret against each batch's current schema;
`xbbg.subscription_presence` metadata records the encoding and mapping.

The bounded subscription channel reserves terminal errors outside data capacity.
It yields committed data, one terminal error, then EOF even when sender handles
remain. Bloomberg `DATALOSS`, `DropNewest` overflow, and `Block` forwarding
overflow/timeout terminate with `BlpError::SubscriptionDataLoss`; resubscribe for
a fresh image. Unattributed/session-wide loss uses `topic="*"`. The SDK callback
never waits for consumer space.

Draining unsubscribe propagates unread failures after cleanup rather than
returning a successful partial result. Removing every topic does not detach the
handle from engine/session termination. Partial-topic failures and ordinary
connection-down notifications retain their nonterminal behavior.

## Gap-aware delivery and state images

Set `SubscribeRequest::overflow_policy` to `Some(OverflowPolicy::DropOldest)`
for newest-pending deltas, or `SubscribeRequest::delivery` to
`SubscriptionDeliveryMode::State` for conflated current images. Consume either
through `Engine::subscribe(request).await?.deliveries()`, which takes ownership
of the ordinary stream and returns `SubscriptionDeliveries`.

`SubscriptionDeliveries::next()` returns `SubscriptionDelivery` values containing
`kind`, an Arrow `RecordBatch` in `data`, and optional `gap_before`. Local
drop-oldest overflow reports `SubscriptionGap::dropped_updates` with cause
`local_overflow`; the count is evicted source updates, not Arrow batches.
Upstream SDK data loss is still terminal for delta delivery.

State delivery reads the existing shared image for dirty topics in fair FIFO
order, without allocating Arrow on ingestion or scanning quiet topics per read.
Metadata columns are `topic`, `last_update`, `live`, `delayed`, and `valid`;
these names are reserved. Unknown/invalid images carry null data. Continuity
invalidation precedes recovery, including a drain with both epochs pending.
`valid` does not promise every selected field has a value. Observed schemas
persist across topic subsets. `zero_as_null` applies to state and polling images.

State rejects disabled rows and explicit raw-queue controls. The flush threshold
is a read-side batch maximum, not a wait-for-full-buffer setting. Plain reads
reject advanced modes; non-draining close remains universal. Marked drains retain
frozen native state until publication and can be retried after conversion failure.
Bindings use data-free acknowledgments so rollback never relies on mutable
exported Arrow buffers.

Offline performance checks use synthetic data and the SDK TestUtil decoder:

```sh
cargo bench -p xbbg-bench --bench subscription_queue_default
cargo bench -p xbbg-bench --bench subscription_delivery
```

The standalone default-queue target also runs unchanged against v1.7.0. Queue
reports distinguish complete production semantics from bare async-channel and
Crossbeam primitives; decoded feed replay excludes SDK/network time. Reports
omit local paths and raw environment/compiler strings.
