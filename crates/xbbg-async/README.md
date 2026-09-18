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
| `engine/worker/` | Per-worker event loop and request lifecycle |
| `engine/subscription_pool.rs` | Per-session subscription lifecycle and callbacks |
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

## Subscription deltas and termination

`SubscriptionStream::next()` yields sparse `SubscriptionUpdate` values. Missing
`UpdateField` entries mean unchanged; `UpdateValue::Null` is an explicit SDK
clear. Requested arrays/complex values and non-null decode failures are errors.
All-fields mode omits unrequested nonscalars without suppressing a later scalar.

Requested-field extraction uses a bounded present-field scan for wide, sparse
messages and retains name lookups for narrow or dense messages. Values remain in
requested order. Changed immutable layouts are built once per message while
preserving every discovery/type-change version increment. These optimizations
neither combine messages nor wait for another event before delivering an update.

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
