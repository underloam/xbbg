<!-- markdownlint-disable MD033 MD041 -->
<div align="center">

<a href="https://xbbg.org/javascript/">
  <img src="https://raw.githubusercontent.com/underloam/xbbg/main/.github/assets/readme-hero-flat.svg" alt="xbbg banner" width="100%">
</a>

[![npm version](https://img.shields.io/npm/v/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core)
[![Node.js versions](https://img.shields.io/node/v/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core)
[![npm downloads per month](https://img.shields.io/npm/dm/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core)
[![CI](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml/badge.svg)](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml)
[![Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/P34uMwgCjC)

**Links:** [Documentation](https://xbbg.org/javascript/) · [API reference](https://github.com/underloam/xbbg/blob/main/js-xbbg/REFERENCE.md) · [GitHub](https://github.com/underloam/xbbg) · [Changelog](https://github.com/underloam/xbbg/blob/main/CHANGELOG.md) · [LangGraph tools](https://www.npmjs.com/package/@xbbg/langgraph)

</div>

---

# @xbbg/core

Bloomberg data for Node.js, backed by the same Rust engine as the [`xbbg`](https://pypi.org/project/xbbg/) Python package. Requests and subscriptions run through a native N-API addon, with no HTTP hop, and return Apache Arrow tables or JSON rows.

> **Important:** `@xbbg/core` is an independent open-source project. It is not affiliated with, endorsed by, sponsored by, or approved by Bloomberg Finance L.P. or its affiliates. It does not provide Bloomberg access, credentials, entitlements, data rights, or SDK licenses; use your own under your Bloomberg agreements and policies.

## Why @xbbg/core?

- **One engine for Python and Node.** Both packages share the Rust request engine, response parsing, and typed errors, so requests behave the same in both.
- **Broad request coverage.** BDP, BDS, BDH, intraday bars and ticks, BQL, BEQS, BSRCH, BQR, and YAS, plus recipes for futures, CDX, ETFs, dividends, indices, and fixed income.
- **Real-time built for many consumers.** Subscriptions to the same security share one Bloomberg feed, `latest()` gives a current-value board, and delayed data or rejected fields raise warnings instead of failing silently.
- **Arrow-native results.** Requests return Apache Arrow tables or JSON rows, and typed formats keep dates, times, and timestamps as typed columns.
- **Every Bloomberg connection mode.** Desktop API (DAPI), B-PIPE/SAPI, ZFP, TLS, failover hosts, and SOCKS5.
- **TypeScript-first and prebuilt.** Full type definitions and native addons for macOS arm64, Linux x64 (glibc 2.28+), and Windows x64.

## Install

```bash
npm install @xbbg/core
# or
bun add @xbbg/core
```

`@xbbg/core` runs on Node.js 24+ and Bun 1.4+ servers, not in browsers. npm and Bun install the matching prebuilt addon automatically. You also need Bloomberg access (Terminal/DAPI, B-PIPE, SAPI, or ZFP) and Bloomberg's SDK runtime library on the same machine; on Windows, standard Terminal installs such as `C:\blp\DAPI` are found automatically.

## Quickstart

```ts
import * as xbbg from '@xbbg/core';

xbbg.configure({ host: 'localhost', port: 8194 });

// Reference and historical data
const ref = await xbbg.blp.abdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST', 'SECURITY_NAME']);
const hist = await xbbg.blp.abdh(['SPX Index'], ['PX_LAST'], '2024-01-01', '2024-12-31');

// Intraday bars and ticks
const bars = await xbbg.blp.abdib('AAPL US Equity', '2024-12-02', 5);
const ticks = await xbbg.blp.abdtick(
  'AAPL US Equity',
  '2024-12-02T09:30:00',
  '2024-12-02T10:00:00',
);

// BQL and other requests on an engine
const engine = await xbbg.connect({ host: 'localhost', port: 8194 });
const bql = await engine.bql("get(px_last) for('AAPL US Equity')");

// Live data
const sub = await xbbg.blp.asubscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK']);
try {
  for await (const tick of sub) {
    console.log(tick.topic, tick.get('LAST_PRICE'));
    break;
  }
} finally {
  await sub.unsubscribe();
}
```

## Connections

`configure()` and `connect()` take the same config: `host`/`port` for a local Terminal, `servers` for ordered failover, `auth` (`user`, `app`, `userapp`, `dir`, `manual`, or `token`), `tls`, `zfpRemote` for ZFP leased lines, and `socks5`.

```ts
const bpipe = await xbbg.connect({
  servers: [
    { host: 'bpipe-primary.example.com', port: 8194 },
    { host: 'bpipe-secondary.example.com', port: 8196 },
  ],
  auth: { method: 'userapp', appName: 'my-bpipe-app' },
  tls: { clientCredentials: '/secure/client.p12', trustMaterial: '/secure/trust.p7' },
});
```

## Subscriptions

```ts
// A board you poll: rows: false keeps current values without queuing ticks
const board = await engine.subscribe(
  ['IBM US Equity', 'MSFT US Equity'],
  ['BID', 'ASK', 'LAST_PRICE'],
  {
    rows: false,
  },
);
try {
  console.log(board.latest({ backend: 'json' })); // one row per security
} finally {
  await board.unsubscribe();
}
```

- Subscriptions to the same security within one engine share a Bloomberg feed; `addFields()` grows it with a single re-subscribe.
- `onDelayed` and `onFieldError` choose whether delayed data and fields Bloomberg rejects warn (through `process.emitWarning`), raise, or are only recorded.
- Iterate scalar ticks or call `sub.arrow()` for Apache Arrow tables. Row subscriptions end with `BlpSubscriptionDataLossError` on data loss; `rows: false` boards re-subscribe automatically.

## Exchange auctions and imbalance

Bloomberg publishes imbalance and auction data only on the listing where the auction runs. These helpers route each ticker or ISIN to that venue and check Bloomberg's answer before returning data.

```ts
import { AuctionFields } from '@xbbg/core';

// SPY US Equity -> SPY UP Equity, IBM US Equity -> IBM UN Equity
const venues = await engine.resolveVenues(['SPY US Equity', 'IBM US Equity']);
const snapshot = await engine.auctionSnapshot(['SPY US Equity'], {
  fields: AuctionFields.imbalance,
});
const auction = await engine.subscribeAuction(['SPY US Equity', 'IBM US Equity'], { rows: false });
```

## Recipes

Recipes wrap common Bloomberg workflows and return an Arrow table, or JSON rows with `backend: 'json'`: futures (`futTicker`, `activeFutures`, `futuresCurve`), CDX (`cdxTicker`, `activeCdx`), fixed income (`yas`, `bqr`, `preferreds`, `corporateBonds`), equities and ETFs (`indexMembers`, `etfHoldings`, `etfNavHistory`, `dividend`, `turnover`), identifiers (`resolveIsins`, `issuerIsins`), FX (`currencyConversion`), and volatility surfaces (`volSurface`).

```ts
const curve = await engine.futuresCurve('ES1 Index', { maxContracts: 6 });
const members = await engine.indexMembers('SPX Index', { asof: '20240102' });
```

Pass `returnEids: true` to `bdp`, `bds`, `bdh`, `bdib`, or `bdtick` to get Bloomberg entitlement IDs, then check them with `engine.checkEntitlements('//blp/refdata', eids)`.

## More

- [API reference](https://github.com/underloam/xbbg/blob/main/js-xbbg/REFERENCE.md): date inputs, typed results, subscription lifecycle and options, warnings, every recipe, engine configuration, and local development.
- [`@xbbg/langgraph`](https://www.npmjs.com/package/@xbbg/langgraph): LangChain and LangGraph tools built on `@xbbg/core`.
- [`xbbg` for Python](https://pypi.org/project/xbbg/) and the [Rust crates](https://crates.io/crates/xbbg_core) use the same engine.
- Releases are published from GitHub Actions with npm trusted publishing and provenance.

## License

[Apache-2.0](https://github.com/underloam/xbbg/blob/main/LICENSE)
