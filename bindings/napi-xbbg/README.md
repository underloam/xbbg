# napi-xbbg

Node.js bindings for the xbbg Bloomberg engine via [napi-rs](https://napi.rs).

## Native API

The Rust addon exposes `JsEngine` and `JsSubscription` to the public `js-xbbg`
TypeScript package. Requests, recipes, and subscription Arrow batches use the
existing zero-copy column-buffer representation; generated typings are produced
by the package build.

### Shared subscriptions

`subscribe(tickers, fields, allFields?, aliases?, onDelayed?, isolated?, rows?,
onFieldError?, zeroAsNull?)` and
`subscribeWithOptions(service, tickers, fields, options?, flushThreshold?,
overflowPolicy?, streamCapacity?, allFields?, aliases?, onDelayed?, isolated?,
rows?, onFieldError?, zeroAsNull?)` construct the engine's shared-subscription
request and wait for subscription-session capacity like any other subscription.

- `aliases` maps Bloomberg topics to consumer labels. Rows, status keys, `tickers`,
  and `remove()` use the labels.
- `onDelayed` accepts `warn` (default), `raise`, or `ignore`, ignoring case and
  surrounding whitespace. Other values produce a validation error.
- `onFieldError` also accepts `warn` (default), `raise`, or `ignore`, trimmed and
  case-insensitive; invalid values produce a validation error. `warn` records
  rejected fields and emits warnings, `ignore` records them without warnings, and
  `raise` fails only affected consumer topics while healthy siblings continue.
- `rows` defaults to `true`. With `false`, no data rows are queued, so the consumer
  cannot overflow; `latest()`, status, warnings, and dynamic controls still work.
  The immutable `deliversRows` getter reports the handle's delivery mode.
  `nextUpdates()` and `nextArrowBatch()` reject image-only reads immediately with
  a validation error directing callers to `latest()`, without consuming or closing
  the subscription.
- Matching `//blp/mktdata` feeds share by default within an engine.
  `isolated: true` and non-market-data services keep independent feeds.
- `add(tickers, aliases?)` adds memberships; `addFields(fields)` asynchronously
  grows the field projection. `fields` reads the current projection.
- `latest()` synchronously returns an Arrow snapshot with `topic`, `last_update`,
  `live`, `delayed`, and the projected data fields.
  After termination or close, it reports `subscription already closed` when the
  engine returns `ChannelClosed`; other engine errors retain their original codes.
- `zeroAsNull` defaults to an empty field list. In `latest()` only, numeric zero
  (including floating-point negative zero) becomes null for the listed fields.
  Tick rows, stored images, and statistics are unchanged.
- Image-only consumers recover from feed data loss while row consumers fail
  closed. Existing status events report `DataLoss` and `FeedRecovered`; recovery
  clears and rebuilds the image on the recovery paint and keeps `live` false until
  the next non-paint update. Session termination does not recover automatically.
- `status` is a consistent snapshot of `events`, `failures`, `failedTickers`,
  `topicStates`, `fieldErrors`, `session`, `services`, and `admin`. Separate getters
  expose the same data, including `sessionStatus`, `serviceStatus`, and
  `adminStatus`. Native object properties are camelCase; topic states include
  `feedTopic` and nullable `delayed`.
- `takeWarnings()` returns and clears pending delayed-data/field-exception events,
  independently of event history. The TypeScript layer emits Node warnings.
  This drain remains usable after close. Unsubscribe/drain retains unread
  terminal errors even for image-only consumers.
- `subscriptionFeeds()` returns engine diagnostics for currently retained feeds:
  service, topic, options, field union, consumer count, delay/lifecycle state,
  isolation, and field errors. It exposes no identity or transport configuration.

### Auction utilities and recipes

`extAuctionFieldGroup(name)`, `extAuctionZeroPriceFields()`, and
`extImbalanceSide(code)` are pure utilities
backed by `xbbg-ext` and require no engine. Unknown field groups or imbalance sides
return `null`. `extAuctionZeroPriceFields()` returns the native `ZERO_PRICE_FIELDS`
price-sentinel list (not a field group); callers can intersect it with their
requested fields when choosing `zeroAsNull`.

`recipeResolveVenues(securities, pcsOverrides?)` resolves and validates each input's
primary venue. `recipeAuctionSnapshot(securities, fields?, pcsOverrides?)` requests
auction fields only from validated venues; omitted or empty fields select the
default auction group. Both return the same Arrow representation as other native
recipes, retaining Date32, Time64 microseconds, and timestamp column types.
Time-only values carry no invented date or timezone. Preferred pricing-source
overrides are per-call string maps.

## Architecture

```
xbbg-core + xbbg-async  (pure Rust engine)
         ↓
    napi-xbbg            (this crate — thin N-API wrapper)
         ↓
     js-xbbg             (npm package + TS types)
```

Both language bindings use `xbbg-async` for subscription ordering and close
barriers, engine configuration validation, and domain error presentation. Node
retains deadline-aware reads, cancelled-read rollback, and typed error encoding.
TLS timeouts must be non-negative even without credentials. The raw addon's unused
`version()` and `extAuctionFieldGroupNames()` exports are removed; the package-level
`version()` and `AuctionFields` remain available.
