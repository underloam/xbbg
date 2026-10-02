# @xbbg/core reference

Detailed behavior, options, and examples for `@xbbg/core`. Start with the [README](README.md) for installation, runtime requirements, and a quickstart.

## Native addon and runtime discovery

`@xbbg/core` loads a packaged native `napi_xbbg.node` addon from an optional dependency for supported targets:

- `@xbbg/core-darwin-arm64` — macOS arm64
- `@xbbg/core-linux-x64` — Linux x64 (glibc 2.28+, the same floor as official Node.js binaries)
- `@xbbg/core-win32-x64` — Windows x64

If no packaged addon is available for your platform, build from source locally instead.

On Windows, `@xbbg/core` prepends the first detected Bloomberg runtime DLL directory before loading the native addon. Explicit `BLPAPI_LIB_DIR` / `BLPAPI_ROOT` values win; otherwise it probes local SDK roots plus standard Terminal DAPI installs including `C:\blp\DAPI` and `C:\Program Files (x86)\Bloomberg\Blp\DAPI`.

## Local Development

```bash
# Preferred: build and copy js-xbbg/napi_xbbg.node into the package directory
npm --prefix js-xbbg run build

# Lower-level Rust build (useful while hacking on napi-xbbg itself)
cargo build -p napi-xbbg

# Stage the current platform package template with the built addon
npm --prefix js-xbbg run stage:native-package

# Run package smoke tests from js-xbbg/
npm run test:smoke
```

`npm test` runs the offline unit suite, excluding the live and session smoke suites. `npm run test:live` explicitly connects to Bloomberg. Its auction/shared-feed cases cover initial-paint behavior without requiring an active auction or new trades; the optional preferred-routing case requires `XBBG_LIVE_PFD_ISIN`. Live tests are intended for an entitled environment and always close their new subscriptions.

The JS package automatically loads a local `js-xbbg/napi_xbbg.node` addon first, then falls back to packaged optional native dependencies for supported platforms.

## Request and subscription behavior

`bdp()` / `bds()` / `bdh()` forward `validateFields` for per-request field validation. `bdib()` and `bdtick()` forward `requestTz` / `outputTz`; `bdtick()` also exposes common include-code request flags such as `includeConditionCodes` and `includeExchangeCodes` as typed options while still accepting raw Bloomberg request kwargs.

### Date and datetime input (#317)

Every API surface that takes a date or datetime accepts a wide input set:

- `Date` (JavaScript)
- ISO 8601 `string` (`"2023-01-17"`, `"2023-01-17T10:30:00"`, `"2023-01-17T10:30:00-05:00"`)
- Bloomberg-native `string` (`"20230117"`)
- Epoch milliseconds `number`
- Duck-typed Luxon `DateTime` — anything implementing `toJSDate(): Date`

Ambiguous formats like `"01/17/2023"` are rejected with a clear `TypeError`. Naive ISO datetime strings without a tz suffix are passed through to the Rust engine so `requestTz` semantics still apply; tz-aware strings are preserved end-to-end. The helpers `formatDate` and `formatDateTime` plus the `DateLike` / `DateTimeLike` types are re-exported from `@xbbg/core` if you want to apply them yourself.

```ts
import { bdh, bdtick } from '@xbbg/core';

await bdh('AAPL US Equity', 'PX_LAST', { start: new Date('2024-01-01'), end: '20240630' });
await bdtick('AAPL US Equity', new Date('2024-12-01T09:30Z'), Date.UTC(2024, 11, 1, 16, 0));
```

### Typed date and time results

Arrow-backed requests and auction snapshots preserve date-only fields as `Date32`, time-only fields as `Time64(Microsecond)`, and full datetimes as timestamps. Time-only values have no invented calendar date or timezone. Apache Arrow JS and the core JSON-row backend expose Time64 values as microsecond `bigint`s and Date32 values as UTC-midnight epoch-millisecond numbers; nulls stay null. Convert `bigint`s explicitly before calling `JSON.stringify`.

`Format.LONG_TYPED` appends nullable `value_time` immediately after `value_ts`: reference-data columns are `ticker, field, value_f64, value_i64, value_str, value_bool, value_date, value_ts, value_time`; historical results also have `date` after `ticker`. `value_ts` holds full datetimes, not time-only values. `LONG_WITH_METADATA` retains its string `value` and reports `dtype: 'time64'` or `'date32'`; default `LONG` remains textual. The engine discards legacy field-cache snapshots and refreshes their metadata instead of retaining stale temporal types.

Subscriptions yield scalar `Tick` objects by default. Choose `sub.arrow()` to construct Apache Arrow JS tables directly from native descriptors without Arrow IPC for supported primitive, binary/string, date/time, timestamp, and null columns. The exposed mutable buffers are JS-owned snapshots: exclusive, bounded native allocations can transfer ownership, while shared, sliced, or oversized storage is copied or canonicalized before exposure. JS mutation therefore cannot change a retained Rust Arrow source. No-IPC construction does **not** imply a universal zero-copy Rust/JS boundary. Unsupported types fail with column-level diagnostics rather than silently changing transport.
Pass `{ allFields: true }` to `stream()` / `subscribe()` / service stream helpers to expose every top-level scalar field Bloomberg sends, matching Python's `all_fields=True`. Unrequested arrays and complex fields are omitted; explicitly requested unsupported shapes fail instead of being truncated. The default remains filtered mode: requested fields plus `MKTDATA_EVENT_TYPE` and `MKTDATA_EVENT_SUBTYPE`. Filtered market-data consumers omit rows containing none of their requested data fields, including metadata-only rows.

Ticks are sparse deltas: `tick.has(field)` distinguishes a present value/clear from an absent field. `get()` and typed accessors preserve `undefined` for absence versus `null` for an explicit clear; `toObject()` omits absent keys. Values in a promoted string layout are strings, not fabricated nulls.

Arrow tables append non-null binary `__xbbg_present`. Bit `i`, least-significant bit first, marks schema field `i + 2` after `timestamp` and `topic`: set bit plus null means clear; unset bit means unchanged. Interpret each row against its current table schema, not cached ordinals from a previous layout. Metadata key `xbbg.subscription_presence` records the encoding and mapping.

### Subscription lifecycle

- Choose scalar iteration (`sub.next()` / `for await`) or Arrow iteration (`sub.arrow()`) once per native subscription. The first read or draining close fixes the format; mixing formats raises `TypeError`. Reads are serialized across views, not independent consumers.
- `unsubscribe()` discards unread values. `unsubscribe(true)` returns buffered values in the selected format, including pending JS values, an in-flight batch, and the native drain; use the Arrow view's `unsubscribe(true)` after Arrow reads. Draining through the wrong view closes without draining and reports the format error.
- Close is shared across scalar/Arrow views and wakes pending reads. Concurrent closes share cleanup rather than unsubscribing twice; repeated closes do not replay drained values. Consumed and discarded pending values are released.
- `return()` and an early `for await` exit await non-draining cleanup. For manual reads, close in `finally`. `next({ signal })` accepts an `AbortSignal`; abort closes the whole subscription, not just that read, and waits for cleanup before rejecting.
- Native read/conversion failures trigger cleanup. If cleanup also fails, an `AggregateError` preserves both failures instead of silently hiding the cleanup error. Rejecting an image-only row read happens before native work and does not close the board.
- Row subscriptions fail closed on Bloomberg `DATALOSS`, `drop_newest` overflow, or bounded `block` forwarding overflow/timeout with `BlpSubscriptionDataLossError`. Committed queue data precedes the error, then EOF; resubscribe for a fresh image. Image-only subscriptions recover Bloomberg feed data loss as described below. `topic="*"` identifies unattributed/session-wide loss. The SDK callback never waits for consumers.
- A draining close propagates unread failures after cleanup instead of returning a successful partial result. Terminal session errors wake pending reads even after remove-all; ordinary connection-down events and failures of only some topics remain nonterminal.

### Shared market-data feeds and diagnostics

Within one `Engine`, market-data consumers share a feed when service, trimmed topic and normalized subscription options match. Aliases do not affect sharing. The upstream field union grows when consumers join or call `addFields`; it does not shrink until the feed closes. A late row consumer gets a synthetic `SUMMARY/INITPAINT` row from known values. Consumers awaiting an image get the full union-growth repaint; existing readers receive only projected values or explicit clears that changed, suppressing unchanged repeats.

- `aliases?: Record<string, string>` maps upstream topics to consumer labels. Labels appear in ticks, `tickers`, status keys and `remove()`. `add(tickers, aliases?)` accepts upstream topics plus optional labels.
- `onDelayed?: 'warn' | 'raise' | 'ignore'` defaults to `'warn'`. `'raise'` rejects only the delayed topic, leaving healthy sibling topics active. `'ignore'` still records the delayed flag.
- `isolated?: boolean` defaults to `false`; `true` gives the consumer its own feeds/session. Non-`//blp/mktdata` services remain isolated and do not receive synthetic late-join images.
- `rows?: boolean` defaults to `true`. Set `false` for an image-only board: no data rows are queued and there is no output-queue overflow. Use `latest()` without reading the iterator. Scalar and Arrow `next()` attempts reject with a clear error and leave the board available for polling.
- `onFieldError?: 'warn' | 'raise' | 'ignore'` defaults to `'warn'`. `'raise'` fails only topics with a rejected explicit field, recording its field/category in the failure reason; healthy sibling topics continue. `'ignore'` records `fieldErrors` without emitting a field warning.
- `zeroAsNull?: readonly string[]` defaults to `[]`. In `latest()` only, numeric zero (including `-0`) in the listed fields becomes null. Tick rows, underlying images, booleans, dates/times and stats are unchanged.

These options work with `engine.subscribe`, `engine.stream`, service stream helpers and module-level `subscribe`/`asubscribe`. `allFields` intentionally sees all scalar fields received on the shared feed, including fields induced by other consumers. Local queue overflow fails only that row consumer. Detaching one consumer does not unsubscribe its siblings.

```ts
const sub = await engine.subscribe(['IBM UN Equity'], ['BID'], {
  aliases: { 'IBM UN Equity': 'IBM US Equity' },
  onDelayed: 'warn',
});
try {
  await sub.addFields(['ASK']); // sub.fields now reflects the current projection
  const image = sub.latest({ backend: 'json' });
  console.log(image, sub.topicStates, engine.subscriptionFeeds());
  await sub.remove(['IBM US Equity']); // remove by consumer label
} finally {
  await sub.unsubscribe();
}
```

`latest({ backend? })` is synchronous and does not consume deltas or choose an iterator format. It returns an Arrow table by default, or JSON rows with `backend: 'json'`. Columns are `topic` (label), nullable UTC `last_update`, `live`, nullable `delayed`, then consumer data fields. Metadata fields are excluded; unknown values and explicit clears are null. Rows contain the last known image, not a new Bloomberg request.

Explicit field kinds are seeded from field metadata when available before the first value; metadata lookup is best effort and observed kinds still update the layout. A failed, closed or unsubscribed handle rejects `latest()` with a clear closed-subscription error; removing all topics from an otherwise open handle still produces an empty table.

For a polling auction board, use `await engine.subscribeAuction(['IBM US Equity'], { rows: false, onFieldError: 'raise' })`, call `latest()` at the application's chosen cadence, and always `unsubscribe()` in `finally`. `streamAuction` does not accept `rows`; it always delivers rows.

On Bloomberg `DATALOSS`, row consumers fail closed. If image-only consumers remain, the shared feed resubscribes automatically. The `live` column in `latest()` becomes false immediately; the old image remains until recovery paint arrives, then is cleared and rebuilt so missing recovery fields are null. The first ordinary update makes it live again. `sub.events` records `DataLoss` and `FeedRecovered`; these recovery notices are not emitted as field/delayed warnings. Session termination still ends subscriptions without automatic recovery.

Read `status` for a combined snapshot (`events`, `failures`, `failedTickers`, `topicStates`, `fieldErrors`, `session`, `services`, `admin`), or use the individual getters `events`, `failures`, `failedTickers`, `topicStates`, `fieldErrors`, `sessionStatus`, `serviceStatus` and `adminStatus`. Topic states include `feedTopic` (upstream identity) and `delayed`. Field errors map consumer label → field → category. `engine.subscriptionFeeds()` returns current `FeedInfo[]`: service, topic, options, union fields, consumer count, delayed flag, state, isolation flag and field errors. It contains no authentication or connection identity information.

Warnings are drained once from the shared scalar/Arrow lifecycle, including initial attach, reads, close and calls to `add`, `remove`, `addFields` and `latest`. They use Node's standard `process.emitWarning(message, { type, code, detail })`:

| Event                                            | Warning type            | Code                   |
| ------------------------------------------------ | ----------------------- | ---------------------- |
| Delayed topic (`onDelayed: 'warn'`)              | `BlpDelayedDataWarning` | `XBBG_DELAYED_STREAM`  |
| Rejected explicit field (`onFieldError: 'warn'`) | `BlpFieldWarning`       | `XBBG_FIELD_EXCEPTION` |

`detail` carries the native diagnostic; the message identifies the consumer topic. Emitting a warning does not erase the bounded `events` history or persistent `fieldErrors`. Use `process.on('warning', handler)` for application handling.

### Subscription replay benchmark

`npm run bench:subscription-replay` measures offline synthetic/JSONL replay, not Bloomberg end-to-end latency. Replay batches default to **64 rows**; use `--batch-rows 1` for one-update-at-a-time comparisons. Timing scopes differ:

- `--path legacy`: row-array construction, IPC encoding, IPC decoding, and the selected consumer.
- `--path arrow-decode-only`: decoding prebuilt IPC plus the consumer; IPC encoding is outside the timed loop.
- `--path subscription-wrapper`: an autonomous bounded **synthetic** native-descriptor queue, `ArrowSubscription.next()`, descriptor-to-Arrow construction, and the consumer. It does not measure Rust descriptor production, the native ownership boundary, SDK events, or network latency.

`--consume rows|vector|schema|none` selects consumption (`rows` is the default; `rows` and `vector` touch every cell). Setup and `--warmup-iterations N` are separate from measured replay. Per-batch service timings include the selected read/conversion/consumer path; `--consumer-delay-ms` is excluded from those samples but included in elapsed throughput. The wrapper reports queue counts, drops, and high-water marks in **batches**; `--queue-capacity`, `--producer-burst`, `--cancel-after-rows`, and `--drain-on-cancel` exercise bounded queue/cancellation behavior.

Duration summaries report their sample count: **p95 requires at least 20 samples**, **p99 at least 100**, otherwise the value is `null`. These are per-batch samples, not a claim of independent live-event latency observations. Memory is sampled before/after one **separate untimed replay** using heap, external, ArrayBuffer, and RSS counters; it is not peak memory or an allocation total.

Keep the result's provenance with any comparison: input/fixture hash, Node/V8, OS/CPU, package/Arrow versions, and the resolved native artifact path/hash. When available, `.native-build-info.json` records the producing Rust compiler, build profile/optimization, target CPU/features, flags, allocator, commit, Bloomberg SDK header version and library hash, and artifact hash. Check the reported attestation/hash-match status; missing or unattested build information is not verified compiler/SDK provenance.

Live capture requires an available, authorized Bloomberg endpoint, writes ticks to JSONL, and reports existing `sub.stats` telemetry. Its elapsed time starts after connection/subscription setup and includes native waits, the JS wrapper, `toObject()`, JSON serialization, file backpressure, and cleanup—not just Bloomberg latency. Replaying a capture remains offline work. No live latency gain is established by these benchmarks.

```bash
# Synthetic 64-row batches, no Bloomberg connection needed; rows consumer is the default
npm run bench:subscription-replay -- --rows 100000 --iterations 3

# Time JS Arrow decode only, with IPC buffers precomputed outside the timed loop
npm run bench:subscription-replay -- --path arrow-decode-only --rows 100000 --iterations 3

# Time the ArrowSubscription wrapper around a synthetic bounded descriptor queue
npm run build:ts
npm run bench:subscription-replay -- --path subscription-wrapper --rows 100000 --iterations 3

# Capture real XBTUSD ticks to JSONL, printing existing sub.stats telemetry
npm run bench:subscription-replay -- --capture-live "XBTUSD Curncy" --capture-ms 10000 --out tmp/xbtusd-ticks.jsonl

# Replay captured ticks one update at a time with schema-only consumption after one warmup iteration
npm run bench:subscription-replay -- --fixture tmp/xbtusd-ticks.jsonl --batch-rows 1 --iterations 10 --warmup-iterations 1 --consume schema
```

## Usage

```typescript
import * as xbbg from '@xbbg/core';
import { bdp, ovr } from '@xbbg/core';

xbbg.configure({
  host: 'localhost',
  port: 8194,
});

// Direct B-PIPE / leased-line hosts with ordered failover
const bpipeEngine = await xbbg.connect({
  servers: [
    { host: 'bpipe-primary.example.com', port: 8194 },
    { host: 'bpipe-secondary.example.com', port: 8196 },
  ],
  auth: { method: 'userapp', appName: 'my-bpipe-app' },
  tls: {
    clientCredentials: '/secure/client.p12',
    clientCredentialsPassword: process.env.BPIPE_TLS_PASSWORD,
    trustMaterial: '/secure/trust.p7',
  },
});

// Authorized ZFP over leased lines: use Bloomberg-provisioned endpoints via zfpRemote
const zfpEngine = await xbbg.connect({
  zfpRemote: '8194',
  tls: {
    clientCredentials: '/secure/client.p12',
    clientCredentialsPassword: process.env.BPIPE_TLS_PASSWORD,
    trustMaterial: '/secure/trust.p7',
  },
});

// Python-style blp namespace
const hist = await xbbg.blp.abdh(['AAPL US Equity'], ['PX_LAST'], '2024-01-01', '2024-12-31');
const ref = await xbbg.blp.abdp(['AAPL US Equity'], ['PX_LAST', 'SECURITY_NAME']);
const bulk = await xbbg.blp.abds(['ES1 Index'], ['FUT_CHAIN_LAST_TRADE_DATES']);

// Composable override helper
await bdp(['AAPL US Equity'], ['CRNCY_ADJ_PX_LAST'], {
  overrides: ovr({ EQY_FUND_CRNCY: 'EUR' }),
});
await bdp(['AAPL US Equity', 'MSFT US Equity'], ['CRNCY_ADJ_PX_LAST'], {
  overrides: ovr({
    EQY_FUND_CRNCY: 'USD',
    'AAPL US Equity': ovr({ EQY_FUND_CRNCY: 'EUR' }),
    'MSFT US Equity': ovr({ EQY_FUND_CRNCY: 'JPY' }),
  }),
});

const bars = await xbbg.blp.abdib('AAPL US Equity', '2024-12-01', 5);
const ticks = await xbbg.blp.abdtick(
  'AAPL US Equity',
  '2024-12-01T09:30:00',
  '2024-12-01T10:00:00',
);

// Live streaming
const sub = await xbbg.blp.asubscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK']);
for await (const tick of sub) {
  console.log(tick);
}

// Conflated market data: quote updates are conflated by Bloomberg; trades still stream as received.
const conflated = await xbbg.blp.asubscribe(
  ['ES1 Index'],
  ['BID', 'ASK', 'LAST_PRICE', 'MKTDATA_EVENT_TYPE', 'MKTDATA_EVENT_SUBTYPE'],
  { conflate: true },
);
for await (const tick of conflated) {
  console.log(tick);
}

// CDX analytics
const cdxInfo = await xbbg.ext.cdx.acdx_info('CDX IG CDSI GEN 5Y Corp');
const cdxPricing = await xbbg.ext.cdx.acdx_pricing('CDX IG CDSI GEN 5Y Corp');
const cdxRisk = await xbbg.ext.cdx.acdx_risk('CDX IG CDSI GEN 5Y Corp');
```

## Entitlement IDs

Set `returnEids: true` on `bdp`, `bds`, `bdh`, `bdib`, or `bdtick`. Bloomberg supports this option only for `ReferenceDataRequest` (including BDS), `HistoricalDataRequest`, `IntradayBarRequest`, and `IntradayTickRequest`.

```ts
import { Backend, connect, type ResultMetadata } from '@xbbg/core';

const engine = await connect({ host: 'localhost', port: 8194 });
const ticks = (await engine.bdtick('AAPL US Equity', {
  start: '2024-01-15T09:30:00',
  end: '2024-01-15T10:00:00',
  eventTypes: ['TRADE'],
  returnEids: true,
  backend: Backend.JSON,
})) as Array<Record<string, unknown>> & ResultMetadata;

const eids = Object.values(ticks.eidData ?? {}).flat();
if (eids.length > 0) {
  const report = await engine.checkEntitlements('//blp/refdata', eids);
  console.log(report);
}
```

`eidData` belongs to the outer result container, not to individual rows. The outer result also exposes the raw schema metadata through `metadata`.

## Recipes

High-level workflows wrap common Bloomberg request patterns. Recipes return an Arrow `Table` by default or JSON rows with `backend: 'json'`, and errors use the standard `BlpError` hierarchy. Polars is available on the general request IPC path, not the native recipe table path.

```javascript
import * as xbbg from '@xbbg/core';

const engine = await xbbg.connect({ host: 'localhost', port: 8194 });

// Fixed income
const yas = await engine.yas(['US912810TM69 Govt'], ['YAS_BOND_YLD'], {
  settleDt: '20240115',
  yieldType: 1, // 1=YTM, 2=YTC, 3=YTW, 4=YTB, 5=YTP, 6=YTN, 7=OAS, 8=YTS, 9=YTAL
  price: 99.5,
});
const bqr = await engine.bqr('US912810TM69 Govt', {
  startDatetime: '2024-06-03T14:30:00',
  endDatetime: '2024-06-03T15:00:00',
  eventTypes: ['BID', 'ASK'],
});
const preferreds = await engine.preferreds('BAC US Equity');
const corpBonds = await engine.corporateBonds('AAPL', { ccy: 'USD' });

// Futures and CDX resolution
const front = await engine.futTicker('ES1 Index', '20240301');
const active = await engine.activeFutures('CL1 Comdty', '20240301', { freq: 'M' });
const curve = await engine.futuresCurve('ES1 Index', { maxContracts: 6 });
// cdxTicker resolves the series whose first accrual date is on or before the
// date, so historical dates return the series that was on the run then; the
// resolved Vn is included by default (versionless: true gives a legacy alias).
const cdx = await engine.cdxTicker('CDX IG CDSI GEN 5Y Corp', '20240301');
// activeCdx additionally waits for the new series' first print after a roll.
const activeCdx = await engine.activeCdx('CDX IG CDSI GEN 5Y Corp', '20240301', {
  lookbackDays: 10,
});

// Historical helpers
const dvd = await engine.dividend(['AAPL US Equity'], '20230101', '20231231');
const turn = await engine.turnover(['AAPL US Equity'], '20240101', '20240131', {
  ccy: 'USD',
});
const holdings = await engine.etfHoldings('SPY US Equity');
const realizedYield = await engine.dividendYield('AAPL US Equity', '20230101', '20231231', {
  dividendTypes: ['Regular Cash'],
});
const members = await engine.indexMembers('SPX Index', { field: 'INDX_MWEIGHT', asof: '20240102' });
const surface = await engine.volSurface('SPX Index', '20240102', '20240105', {
  preset: 'MONEYNESS_30D',
  includeDerived: true,
  riskFreeRate: 0.05,
});
const resolved = await engine.resolveIsins(['US0378331005', 'INVALIDISIN000']);
const issuers = await engine.issuerIsins(['US037833FB15', 'INVALIDISIN000']);

// ETF NAV / iNAV toolkit — resolves Bloomberg's authoritative
// ETF_NAV_TICKER / ETF_INAV_TICKER relationships (no suffix guessing):
// QQQ US Equity -> QQQNV Index / QXV Index; AT1 LN Equity -> null / AT1IN Index
const navRel = await engine.etfNavRelationships(['QQQ US Equity', 'AT1 LN Equity']);

// Daily history: mapped Index targets price with PX_LAST; AT1's missing
// daily NAV falls back to the fund's FUND_NET_ASSET_VAL — see the
// nav_source_ticker / nav_source_field columns on every row
const navHist = await engine.etfNavHistory(
  ['QQQ US Equity', 'AT1 LN Equity'],
  '20260601',
  '20260701',
);

// Real-time iNAV: validates every mapping first, then subscribes to the
// resolved iNAV topics (here QXV Index) with LAST_PRICE by default
const inavSub = await engine.subscribeEtfInav('QQQ US Equity');
for await (const tick of inavSub) {
  console.log(tick.topic, tick.get('LAST_PRICE'));
  break;
}
await inavSub.unsubscribe();

// Currency-converted prices
const px = await engine.currencyConversion('700 HK Equity', 'USD', '20240101', '20240131');
```

### Exchange auctions and imbalance

`resolveVenues(securities, { pcsOverrides?, backend? })` resolves composite equities to their primary listing and preferreds to a validated pricing-source topic. Explicit venues are respected. `auctionSnapshot(securities, { fields?, pcsOverrides?, backend? })` resolves and validates the venue in the data request; it never returns composite data as a fallback for a mismatched venue.

Both recipes preserve input order and duplicates, with `input_order`, `security`, `status` (`resolved`, `unresolved`, `unsupported`, `mismatch`) and `error` columns. Resolution also reports `lookup`, `kind`, `composite`, `venue_topic`, `venue_figi`, `method`, `exch_code`, `mic` and `pricing_source`. Snapshots contain `venue_topic` followed by the requested fields, typed using Bloomberg field metadata; unresolved and mismatched rows have null data fields.

```ts
import { AuctionFields, imbalanceSide } from '@xbbg/core';

const venues = await engine.resolveVenues(['IBM US Equity', 'AAPL US Equity']);
const snapshot = await engine.auctionSnapshot('IBM US Equity', {
  fields: [...AuctionFields.imbalance, ...AuctionFields.quotes],
});
const auction = await engine.subscribeAuction(['IBM US Equity', 'AAPL US Equity'], {
  fields: AuctionFields.default,
  onDelayed: 'raise',
});
try {
  for await (const tick of auction) {
    console.log(tick.topic, imbalanceSide(tick.str('IMBALANCE_INDIC_RT') ?? ''));
    break;
  }
} finally {
  await auction.unsubscribe();
}
```

`subscribeAuction` and `streamAuction` perform atomic preflight: every distinct input must resolve before any subscription opens. Identical input strings collapse; distinct inputs resolving to the same venue raise `BlpValidationError` naming the conflicting inputs. Any unresolved, unsupported or mismatched input also fails preflight. Venue topics are automatically aliased to the original security strings; caller-supplied aliases are rejected before resolution. Dynamic `add()` takes already-resolved venue topics with aliases; `remove()` takes original security labels.

`AuctionFields` exposes readonly `imbalance`, `indicative`, `state`, `halts`, `results`, `composite`, `quotes` and `default` arrays sourced from Rust. `default` concatenates the first five groups (39 fields), excluding composite prices and quotes. Missing or empty snapshot/auction-stream fields select that default. These are stream-valid names (`THEO_PRICE`, `BID`, `ASK`), not static aliases such as `PX_THEO` or `PX_BID`.

`AuctionFields.zeroPriceFields` is a separate readonly sentinel list, not a field group: `THEO_PRICE`, `INDICATIVE_NEAR`, `INDICATIVE_FAR`, `IMBALANCE_BUY`, `IMBALANCE_SELL`, `REFERENCE_PRICE_RT`. Auction subscriptions default `zeroAsNull` to this list intersected with the requested fields, so an unavailable auction price becomes null in `latest()`. Pass `zeroAsNull: []` to retain zero sentinels. Both auction helpers accept `onFieldError`; `subscribeAuction` also accepts `rows: false`.

`imbalanceSide(code)` trims and ignores case: BUY/MBUY/RBUY → `'buy'`, SELL/MSEL/RSEL → `'sell'`, NOIM/NIMB → `'none'`; unknown, undisclosed and insufficient-order codes return `null`. Time-of-day stream values are terminal-local, not exchange-local.

Preferred `pcsOverrides` map exchange names to pricing sources, for example `{ 'NEW YORK': 'SNY2' }`; keys are normalized and per-call overrides take precedence. Only the packaged NEW YORK → SNY2 mapping is live-verified; every venue still undergoes reference-data validation before use.

## Engine configuration

`connect()` and `configure()` accept a structured `EngineConfig` object. The most important connection controls are:

- `host` / `port` for a local Terminal session or an already-provisioned direct endpoint
- `servers` for ordered failover across already-provisioned direct hosts
- `auth` for Bloomberg session identity auth: `user`, `app`, `userapp`, `dir`, `manual`, or `token`
- `tls` for encrypted B-PIPE/direct sessions and as a required input for ZFP
- `zfpRemote` (`'8194'` or `'8196'`) for Bloomberg-assigned ZFP endpoints in an entitled environment; do not combine it with `host`/`port`/`servers`/`socks5`
- `socks5` for proxied access to already-provisioned direct Bloomberg endpoints
- `retryPolicy`, `numStartAttempts`, and recovery settings for reconnect behavior
- `shardRequests`, `shardThreshold`, `shardChunkSize`, and `shardMaxConcurrent` for opt-in sharding of wide multi-security `bdp`/`bdh` requests
- `runtimeWorkerThreads`: shared Tokio runtime threads, default **2**, minimum **1**; this is not the total process thread count
- `subscriptionPoolSize`: pre-warmed subscription sessions, default **1**, minimum **0**
- `maxSubscriptionSessions`: concurrent subscription-session cap, default **32**, minimum **1**, and at least `subscriptionPoolSize`; native admission waits for capacity instead of allocating unbounded sessions

The JS binding forwards these fields directly to the Rust engine, so Node can configure the same auth and transport features already available in the core runtime. Invalid transport combinations such as `zfpRemote` plus direct hosts fail during configuration instead of silently connecting to `localhost:8194`.

Engine shutdown closes admission and signals idle and checked-out subscription sessions so blocked operations can terminate. Await subscription cleanup while the engine is still available.

Field-cache publication on Windows uses `FileRenameInfoEx` with POSIX rename semantics (Windows 10 1607+ and a supporting filesystem). Existing readers retain the old snapshot while new opens see the complete replacement. Unsupported filesystems report persistence errors and retain the prior snapshot, with no unsafe replacement fallback.
