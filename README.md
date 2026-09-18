<!-- markdownlint-disable MD013 MD031 MD032 MD033 MD041 MD051 -->
<div align="center">

<p>
  <a href="https://xbbg.org/python/quickstart">
    <img src=".github/assets/readme-hero-flat.svg" alt="xbbg banner" width="100%">
  </a>
</p>

[![CI](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml/badge.svg)](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml)
[![Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/P34uMwgCjC)<br>
**Python** [![PyPI version](https://img.shields.io/pypi/v/xbbg.svg)](https://pypi.org/project/xbbg/) [![conda-forge version](https://img.shields.io/conda/vn/conda-forge/xbbg.svg)](https://anaconda.org/conda-forge/xbbg) [![Python versions](https://img.shields.io/pypi/pyversions/xbbg.svg)](https://pypi.org/project/xbbg/) [![PyPI downloads per month](https://static.pepy.tech/badge/xbbg/month)](https://pepy.tech/projects/xbbg) [![PyPI downloads total](https://static.pepy.tech/badge/xbbg)](https://pepy.tech/projects/xbbg)<br>
**JavaScript** [![npm version](https://img.shields.io/npm/v/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core) [![Node.js versions](https://img.shields.io/node/v/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core) [![npm downloads per month](https://img.shields.io/npm/dm/%40xbbg%2Fcore.svg)](https://www.npmjs.com/package/@xbbg/core)<br>
**Rust** [![crates.io version](https://img.shields.io/crates/v/xbbg_core.svg)](https://crates.io/crates/xbbg_core) [![MSRV](https://img.shields.io/crates/msrv/xbbg_core.svg)](https://crates.io/crates/xbbg_core) [![crates.io downloads](https://img.shields.io/crates/d/xbbg_core.svg)](https://crates.io/crates/xbbg_core)

**Links:** [Documentation](https://xbbg.org/) · [Quickstart](#quickstart) · [Configuration](#configuration-and-engines) · [Examples notebook](py-xbbg/examples/xbbg_jupyter_examples.ipynb) · [Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md)

</div>

---

<!-- xbbg:latest-release-start -->
Latest release: xbbg==1.4.12 (release: [notes](https://github.com/underloam/xbbg/releases/tag/v1.4.12))
<!-- xbbg:latest-release-end -->

> This `main` branch is the Rust-powered v1 release. For the legacy pure-Python line, use [`release/0.x`](https://github.com/underloam/xbbg/tree/release/0.x).

> **Important:** xbbg is an independent open-source project. It is not affiliated with, endorsed by, sponsored by, or approved by Bloomberg Finance L.P. or its affiliates. Bloomberg, Bloomberg Terminal, B-PIPE, BQL, and related names are trademarks or service marks of their respective owners. xbbg does not grant access to Bloomberg services, data, software, licenses, credentials, or entitlements; users must obtain and use those separately under their own Bloomberg agreements and applicable policies.

## Contents

- [What is xbbg?](#what-is-xbbg)
- [Why xbbg?](#why-xbbg)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [JavaScript and Node](#javascript-and-node)
- [Configuration and engines](#configuration-and-engines)
- [Common API surface](#common-api-surface)
- [Entitlement IDs](#entitlement-ids)
- [Output backends](#output-backends)
- [Async usage](#async-usage)
- [Subscriptions: raw, tick mode, and all fields](#subscriptions-raw-tick-mode-and-all-fields)
- [MCP server](#mcp-server)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Project links](#project-links)

## What is xbbg?

xbbg is a Bloomberg client with Python as the primary surface and companion JavaScript/Node bindings, all backed by a shared Rust engine for request execution, response parsing, Arrow-shaped data movement, async workers, typed errors, and diagnostics.

Use xbbg when you already have Bloomberg access and want higher-level helpers for common request patterns, plus an escape hatch for lower-level Bloomberg service requests.

Core scope:

- request helpers for BDP, BDS, BDH, intraday bars, ticks, BQL, BEQS, BSRCH, BQR, BTA, YAS, and related analytics
- local Bloomberg Desktop API / DAPI by default
- configuration for managed Bloomberg environments, including B-PIPE/SAPI, ZFP leased lines, TLS, failover hosts, SOCKS5, and SDK logging
- sync and async Python APIs backed by the same engine
- output as Narwhals, native xbbg Arrow carriers, PyArrow, pandas, Polars, DuckDB, and other optional Narwhals-backed libraries
- JavaScript/Node bindings in [`js-xbbg`](js-xbbg/README.md)

## Why xbbg?

xbbg's project goal is direct: be the most complete, technically advanced, and performance-focused open-source Bloomberg client for Python workflows, while staying independent of Bloomberg and requiring users to bring their own authorized Bloomberg access.

The short version: if all you need is a tiny one-off `bdp()` wrapper, several packages can work. xbbg is built for the path where that notebook later grows into intraday data, BQL, streaming, B-PIPE/SAPI, ZFP, async services, typed errors, diagnostics, and non-pandas data pipelines.

| Capability | xbbg | raw `blpapi` | pdblp / blp | bbg-fetch | polars-bloomberg |
| --- | --- | --- | --- | --- | --- |
| BDP/BDS/BDH helpers | yes | manual SDK code | yes | yes | partial |
| Intraday bars and ticks | yes | manual SDK code | limited / no | no | partial |
| Streaming subscriptions | yes | manual SDK code | no | no | no |
| BQL, BEQS, BSRCH, BQR, YAS, BTA | broad helper coverage | manual SDK code | limited | limited | partial |
| DAPI, SAPI/B-PIPE, ZFP, TLS, failover, SOCKS5 | configurable engine support | manual SDK code | limited | limited | limited |
| Async worker pools and isolated subscription sessions | yes | application-owned | no | no | no |
| Rust request/parsing engine with Arrow-shaped output | yes | no | no | no | no |
| Output backends beyond pandas | Narwhals, native, PyArrow, pandas, Polars, DuckDB | application-owned | pandas-first | pandas-first | Polars-first |
| Typed errors, diagnostics, field cache, testing helpers | yes | application-owned | limited | limited | limited |
| Usable install footprint (Windows x64, Python 3.14) | xbbg 1.4.12 + narwhals 2.26.0, no `blpapi` = 18.245 MiB | blpapi 3.26.8.1 = 14.702 MiB | pdblp 0.1.8 + pandas 3.0.5 + numpy 2.5.3 + blpapi 3.26.8.1 = 130.816 MiB / blp 0.0.4 + pandas 3.0.5 + numpy 2.5.3 + blpapi 3.26.8.1 = 131.002 MiB | bbg-fetch 3.2.0 + pandas 3.0.5 + numpy 2.5.3 + blpapi 3.26.8.1 = 130.863 MiB | polars-bloomberg 0.6.0 + polars 1.44.2 + blpapi 3.26.8.1 = 191.118 MiB |

## Installation

```cmd
pip install xbbg
```

Conda users can install the conda-forge build:

```cmd
conda install -c conda-forge xbbg
```

`blpapi` is **not** required as a Python dependency. xbbg only needs Bloomberg's shared runtime library
(`blpapi3_64.dll` on Windows, `libblpapi3_64.so` on macOS/Linux), which can come from Bloomberg
Terminal/DAPI, a managed Bloomberg C++ SDK install, or Bloomberg's official `blpapi` wheel. Installing
the wheel is just the easiest discovery path for many users:

```cmd
pip install blpapi --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/
```

Supported Python versions: **3.10 through 3.14**.

Requirements and notes:

- You need an authorized Bloomberg environment: local Terminal/DAPI, SAPI/B-PIPE, or ZFP, depending on your setup.
- If you build from source, stage the Bloomberg C++ SDK with `bash ./scripts/sdktool.sh` on macOS/Linux or `.\\scripts\\sdktool.ps1` on Windows PowerShell.
- If you manage the SDK yourself, set `BLPAPI_ROOT` or use `xbbg.set_sdk_path(...)`.
- On Windows Terminal installs, xbbg automatically probes DAPI runtime roots such as `C:\blp\DAPI` and `C:\Program Files (x86)\Bloomberg\Blp\DAPI` before requiring manual configuration.
- Linux wheels are `manylinux_2_28` (x86_64): any distro with glibc ≥ 2.28 works — RHEL/Alma/Rocky 8+, Debian 10+, Ubuntu 20.04+, Amazon Linux 2023.
- Optional dataframe conversions are installed separately: `xbbg[pyarrow]`, `xbbg[pandas]`, `xbbg[polars]`, or `xbbg[duckdb]`.

Verify the install:

```python
import xbbg

print(xbbg.__version__)
print(xbbg.get_sdk_info())
```

## Quickstart

```python
from xbbg import blp

# Reference data
prices = blp.bdp(["AAPL US Equity", "MSFT US Equity"], "PX_LAST")

# Historical data
hist = blp.bdh("SPX Index", "PX_LAST", "2024-01-01", "2024-12-31")

# Intraday bars
bars = blp.bdib("TSLA US Equity", dt="2024-01-15", interval=5)
```

Common request patterns:

```python
from xbbg import blp, ovr

# Multiple fields
info = blp.bdp("NVDA US Equity", ["Security_Name", "GICS_Sector_Name", "PX_LAST"])

# Bloomberg-style overrides
vwap = blp.bdp("AAPL US Equity", "Eqy_Weighted_Avg_Px", VWAP_Dt="20240115")
adj = blp.bdp("AAPL US Equity", "CRNCY_ADJ_PX_LAST", overrides=ovr(EQY_FUND_CRNCY="EUR"))
per_sec = blp.bdp(
    ["AAPL US Equity", "MSFT US Equity"],
    "CRNCY_ADJ_PX_LAST",
    overrides=ovr(
        {
            "EQY_FUND_CRNCY": "USD",
            "AAPL US Equity": ovr(EQY_FUND_CRNCY="EUR"),
            "MSFT US Equity": ovr(EQY_FUND_CRNCY="JPY"),
        }
    ),
)

# Bulk data
holders = blp.bds("AAPL US Equity", "DVD_Hist_All", DVD_Start_Dt="20240101")

# BQL
result = blp.bql("get(px_last) for('AAPL US Equity')")

# Field lookup
fields = blp.bflds(search_spec="vwap")

# Equity screening and constituents
screen = blp.beqs(screen="MyScreen", asof="2024-01-01")
members = blp.index_members("SPX Index", asof="2024-01-02")

# Workflow helpers
active = blp.active_futures("ESA Index", "2024-01-15")
surface = blp.vol_surface("SPX Index", start_date="2024-01-02", end_date="2024-01-05")
resolved = blp.resolve_isins(["US0378331005", "INVALIDISIN000"])
```

ETF NAV / iNAV workflows live in `xbbg.ext` and resolve Bloomberg's authoritative
`ETF_NAV_TICKER` / `ETF_INAV_TICKER` relationships instead of guessing ticker suffixes:

```python
from xbbg import ext

# Relationship discovery: QQQ US Equity -> QQQNV Index / QXV Index,
# AT1 LN Equity -> null daily NAV / AT1IN Index (independently nullable)
rel = ext.etf_nav_relationships(["QQQ US Equity", "AT1 LN Equity"])

# Daily NAV/iNAV history: mapped Index targets price with PX_LAST; AT1's
# missing daily NAV falls back to the fund's FUND_NET_ASSET_VAL — see the
# nav_source_ticker / nav_source_field columns on every row
hist = ext.etf_nav_history(
    ["QQQ US Equity", "AT1 LN Equity"],
    start_date="2026-06-01",
    end_date="2026-07-01",
)

# Real-time iNAV: validates every mapping first, then subscribes to the
# resolved iNAV topics (here QXV Index) with LAST_PRICE by default
sub = await ext.asubscribe_etf_inav("QQQ US Equity")
async for table in sub:
    print(table.to_pylist())
    break
await sub.unsubscribe()
```

For longer walkthroughs and example output shapes, use the [examples notebook](py-xbbg/examples/xbbg_jupyter_examples.ipynb) or [xbbg.org](https://xbbg.org/).

## Python LangChain and LangGraph

[`py-xbbg-langgraph`](py-xbbg-langgraph/) provides the separate `xbbg-langgraph` distribution, imported as `xbbg_langgraph`. It exposes the same 32 tool names as the JavaScript adapter: 21 Bloomberg request/recipe/snapshot tools and 11 extension helpers, including inline Vega-Lite chart specifications. Python arguments and factory names use `snake_case`.

Install from this checkout (the new distribution has not yet been published):

```bash
pip install ./py-xbbg-langgraph
# For the agent example below:
pip install langchain langchain-openai
# For custom graphs without the LangChain agent package:
pip install "langgraph>=1.2,<2"
```

Requires Python 3.10–3.14, xbbg 1.4.12+, and `langchain-core` 1.4+. Graph workflows target LangGraph 1.2+. The adapter installs `langchain-core` and Pydantic; ordinary xbbg installs remain unchanged. Live requests still require authorized Bloomberg connectivity and SDK runtime libraries. Tool creation and schema inspection do not import the native extension or start a session.

```python
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from xbbg_langgraph import BLOOMBERG_TOOL_INSTRUCTIONS, create_all_bloomberg_tools

tools = create_all_bloomberg_tools(
    max_securities=10,
    max_fields=25,
    max_rows=500,          # Application artifact
    max_content_rows=50,  # Model-facing preview
    disabled_tools={"xbbg_bql", "xbbg_bsrch"},
)

# Configure OPENAI_API_KEY and Bloomberg access before invoking.
agent = create_agent(
    model=ChatOpenAI(model="gpt-4.1"),
    tools=tools,
    system_prompt=BLOOMBERG_TOOL_INSTRUCTIONS,
)
result = agent.invoke({
    "messages": [{"role": "user", "content": "Get PX_LAST for IBM US Equity."}],
})
```

For custom LangGraph workflows, pass these tools to `langgraph.prebuilt.ToolNode` and bind the same list to your model. `create_bloomberg_tools()` returns only the 21 core tools; `create_bloomberg_ext_tools()` returns the 11 helpers. Individual factories such as `create_bdp_tool()` are also exported. Factories accept either keyword options or one `BloombergToolsOptions` instance, not both.

All tools support `.invoke()` and `.ainvoke()`. Use `.ainvoke()` inside a running event loop. A full LangChain tool call produces a `ToolMessage` with independently bounded content and artifact:

```python
from xbbg_langgraph import create_bdp_tool

tool = create_bdp_tool(max_securities=1, max_fields=1)
message = tool.invoke({
    "type": "tool_call",
    "id": "reference-price",
    "name": "xbbg_bdp",
    "args": {"securities": ["IBM US Equity"], "fields": ["PX_LAST"]},
})
print(message.content)
print(message.artifact)
```

Calling with bare arguments instead returns the content string, following LangChain's normal contract. Envelopes retain the JavaScript keys `tool`, `data`, `rowCount`, `truncated`, and optional `truncation`/`hasErrors`. Defaults cap artifacts at 1 MiB and content at 64 KiB, with additional node, depth, and string limits. Native Arrow rows are sliced before conversion; snapshot tables share one materialization allowance. Errors and entitlement metadata take priority over ordinary data. Binary values, including sparse-update presence bitmaps, use tagged base64 rather than losing null-versus-absent information.

Snapshots require `max_updates`; `timeout_ms` cannot exceed `max_stream_wait_ms` (15 seconds by default). They close subscriptions on completion, timeout, error, or cancellation. Successful collection with a cleanup failure retains the updates and reports `unsubscribeError`. Depth snapshots without explicit fields request all available scalar fields; `all_fields=False` requires a nonempty field list.

Pass `engine=your_xbbg_engine` to route tools through an application-owned `xbbg.blp.Engine`; otherwise they use xbbg's existing global/scoped engine. The adapter never reconfigures or shuts down that engine. `request_timeout` is a coroutine deadline in seconds (default 60), not a way to preempt synchronous SDK startup or subscription cleanup. Configure the engine's connection/request timeouts for those boundaries.

Output caps do not limit Bloomberg's upstream response size. Native metadata getters and an individual native cell can allocate before projection. Request bounded universes and date ranges even when the returned preview is small.

Python-specific boundaries:

- Recipe tickers must be fully qualified; builders never guess a `US Equity` suffix. Recipe fields accept identifiers and argument-free BQL field access, not arbitrary query fragments. Use `xbbg_bql` for parameterized expressions.
- `active_only` is not exposed because the current native corporate-bond builder does not implement that filter. Unsupported BQL/BFLDS keyword options are also rejected rather than ignored.
- CDX `recovery_rate` uses Python's percentage convention: `40` means 40%, not `0.40`.
- Chart tools do not fetch data or render it. Render only when `data.spec` is present and `data.renderable` is not false. Explicit `max_points` truncation preserves a valid inline spec; structural result clipping disables rendering.

Adapter checks: `pip install "./py-xbbg-langgraph[test]"`, then `pytest py-xbbg-langgraph/tests`. The existing CI Python matrix and dependency-floor job run this suite.

## JavaScript and Node

xbbg also ships supported Node bindings in [`@xbbg/core`](js-xbbg/README.md). The JS layer uses the same Rust engine through a native N-API addon, so Node can use the same Bloomberg connection modes and request surfaces as Python.

```bash
npm install @xbbg/core
# or
bun add @xbbg/core
```

The packages target Node.js 24+ server runtimes. Packaged native addons are provided for macOS arm64, Linux x64 (glibc 2.28+), and Windows x64. You still need Bloomberg access plus Bloomberg SDK runtime libraries on the target system.

```ts
import * as xbbg from '@xbbg/core';

xbbg.configure({ host: 'localhost', port: 8194 });

const hist = await xbbg.blp.abdh(['AAPL US Equity'], ['PX_LAST'], '2024-01-01', '2024-12-31');
const ref = await xbbg.blp.abdp(['AAPL US Equity'], ['PX_LAST', 'SECURITY_NAME']);
```

See [`js-xbbg/README.md`](js-xbbg/README.md) for platform packaging, runtime prerequisites, and the supported JavaScript API surface.

For LangChain and LangGraph agents, use the supported [`@xbbg/langgraph`](js-xbbg-langgraph/README.md) adapter. It exposes reusable server-side Bloomberg tools backed by `@xbbg/core` without making MCP, a chat app, or a browser integration the core path:

```bash
npm install @xbbg/langgraph @xbbg/core @langchain/core
```

```ts
import { createAllBloombergTools, BLOOMBERG_TOOL_INSTRUCTIONS } from '@xbbg/langgraph';

const tools = createAllBloombergTools({ maxSecurities: 10, maxFields: 10 });
```

Use the existing [`apps/xbbg-mcp`](apps/xbbg-mcp/README.md) package only when you specifically need MCP.

## Configuration and engines

By default, xbbg starts a Rust-backed engine and connects to local Bloomberg Desktop API / DAPI on `localhost:8194`. Configure the engine before the first request when you need a different transport, authentication mode, worker count, timeout policy, field cache, or logging behavior.

```python
from xbbg import blp, configure

# Equivalent to the default local Terminal / DAPI path
configure(host="localhost", port=8194)

print(blp.bdp("AAPL US Equity", "PX_LAST"))
```

Common environments:

| Environment | Use when | Configuration shape |
| --- | --- | --- |
| Desktop API / DAPI | Local Bloomberg Terminal session | no config, or `configure(host="localhost", port=8194)` |
| Direct server / SAPI | Firm-managed Bloomberg server | `configure(host="bpipe-host", port=8194, auth_method="app", app_name="...")` |
| B-PIPE | Enterprise Bloomberg feed infrastructure | direct host/failover config plus the auth/TLS settings your Bloomberg setup requires |
| ZFP leased line | Bloomberg zero-footprint leased-line path | `configure(zfp_remote="8194", tls_client_credentials="...", tls_trust_material="...")` |

Example B-PIPE/SAPI-style configuration:

```python
from xbbg import configure

configure(
    host="bpipe-host",
    port=8194,
    auth_method="app",
    app_name="my-app",
    request_pool_size=4,
    # Opt-in sharding for wide multi-security BDP/BDH requests:
    # shard_requests=True,
    # shard_threshold=20,
    # shard_chunk_size=16,
    # shard_max_concurrent=4,
    subscription_pool_size=2,
    num_start_attempts=5,
)
```

Example ZFP leased-line configuration:

```python
from xbbg import configure

configure(
    zfp_remote="8194",
    tls_client_credentials="/path/to/client.p12",
    tls_client_credentials_password="<load from your secret store>",
    tls_trust_material="/path/to/trust.pem",
)
```

The engine uses separate worker pools for request/response calls and subscriptions:

- request workers hold independent Bloomberg sessions and dispatch BDP/BDH/BDS/BQL-style calls across the pool
- subscription sessions are isolated from request workers, so live streams do not share a single blocking session with batch requests
- field validation, field-type caching, SDK logging, retry policy, keep-alive, slow-consumer thresholds, TLS, SOCKS5, and failover servers are configuration options rather than per-call ad hoc code

`runtime_worker_threads` defaults to **2** (minimum **1**) and controls the engine's shared Tokio runtime, not the total process thread count. `subscription_pool_size` is the pre-warm count (default **1**, minimum **0**); `max_subscription_sessions` caps concurrent subscription sessions (default **32**, minimum **1**, and at least `subscription_pool_size`). Native subscription admission waits for capacity instead of allocating unbounded sessions. Node uses the corresponding `runtimeWorkerThreads`, `subscriptionPoolSize`, and `maxSubscriptionSessions` fields.

Use `Engine(...)` when an application needs a scoped engine with its own connection settings instead of mutating global configuration.

Engine shutdown closes subscription admission and signals both idle and checked-out sessions, waking pending operations so termination and errors can reach callers. Close subscriptions explicitly before releasing their engine; do not rely on interpreter teardown for application cleanup.

Field-cache snapshots are published atomically. On Windows this uses `FileRenameInfoEx` with POSIX rename semantics, requiring Windows 10 1607+ and a supporting filesystem. Existing readers can finish with the old snapshot while new opens see the complete replacement. Unsupported filesystems report persistence errors and retain the prior snapshot; there is no unsafe replacement fallback.

## Common API surface

| Area | Functions |
| --- | --- |
| Reference and bulk data | `bdp`, `bds`, `bflds`, `fieldInfo`, `fieldSearch`, `blkp`, `bport` |
| Historical data | `bdh`, `dividend`, `earnings`, `turnover`, `dividend_yield` |
| Intraday data | `bdib`, `bdtick` |
| Query and screening | `bql`, `beqs`, `bsrch`, `bqr`, `bcurves`, `bgovts`, `etf_holdings`, `index_members` |
| Analytics and utilities | `yas`, `bta`, `ta_studies`, `ta_study_params`, `convert_ccy`, `fut_ticker`, `active_futures`, `futures_curve`, `vol_surface`, `resolve_isins`, `issuer_isins`, `cdx_ticker`, `active_cdx` |
| Real-time data | `subscribe`, `stream`, `vwap`, `mktbar`, `depth`, `chains` |
| Generic requests | `request`, `Service`, `Operation`, `RequestParams`, `OutputMode` |
| Schema and diagnostics | `bops`, `bschema`, `get_sdk_info`, `enable_sdk_logging`, `print_backend_status` |
| Testing helpers | `xbbg.testing.create_mock_response`, `xbbg.testing.mock_engine` |

Most sync helpers have async counterparts with an `a` prefix: `bdp` → `abdp`, `bdh` → `abdh`, `bdib` → `abdib`, `request` → `arequest`.

## Entitlement IDs

Bloomberg can return entitlement IDs only for these four request operations. Opt in with `return_eids=True`:

| Bloomberg operation | Python routes |
| --- | --- |
| `ReferenceDataRequest` | `blp.bdp`, `blp.bds` (BDS uses the reference-data operation) |
| `HistoricalDataRequest` | `blp.bdh` |
| `IntradayBarRequest` | `blp.bdib` |
| `IntradayTickRequest` | `blp.bdtick` |

For example, request EIDs with intraday ticks and check them against the default `//blp/refdata` service:

```python
from xbbg import blp

ticks = blp.bdtick(
    "AAPL US Equity",
    "2024-01-15T09:30:00",
    "2024-01-15T10:00:00",
    return_eids=True,
    backend="native",
)

eid_data = ticks.eid_data or {}
eids = sorted({eid for security_eids in eid_data.values() for eid in security_eids})
if eids:
    print(blp.check_entitlements(eids))
```

EID metadata remains available through the native `ArrowTable.eid_data` property, pandas `attrs["xbbg_eid_data"]`, or PyArrow schema metadata under `xbbg.eid_data`. Polars and DuckDB do not provide a stable entitlement-metadata side channel; use the native, PyArrow, or pandas backend when EIDs are required.

This opt-in request metadata is separate from a subscription message's top-level `EID` field.

## Output backends

xbbg defaults to a Narwhals DataFrame. When PyArrow is installed, the Narwhals frame is backed by a real `pyarrow.Table`; otherwise xbbg falls back through available dataframe libraries and finally to its native Arrow carrier.

```python
from xbbg import Backend, blp

# Default Narwhals output
frame = blp.bdh("SPX Index", "PX_LAST", "2024-01-01", "2024-12-31")

# Explicit native xbbg Arrow carrier
table = blp.bdp("AAPL US Equity", "PX_LAST", backend="native")

# Optional conversions
as_pyarrow = blp.bdp("IBM US Equity", "PX_LAST", backend=Backend.PYARROW)
as_pandas = blp.bdp("MSFT US Equity", "PX_LAST", backend=Backend.PANDAS)
as_polars = blp.bdp("AAPL US Equity", "PX_LAST", backend=Backend.POLARS)
as_duckdb = blp.bdh("SPX Index", "PX_LAST", "2024-01-01", "2024-12-31", backend=Backend.DUCKDB)
```

Output shape is controlled with `format=`, including `long`, `long_typed`, `long_metadata`, and `semi_long`.

- Native `ArrowTable`, `ArrowRecordBatch`, and `ArrowColumn` slices can retain their source allocations. Use `.compact()` when retaining a small result should release that backing storage: it returns an independent copy of the logical values with right-sized buffers, preserving schema, nulls, and physical batch/chunk boundaries. Native zero-column tables retain their row count.
- Polars conversion preserves Arrow chunks rather than implicitly rechunking. The supported floor is **Polars >=0.20.4**; older supported versions use PyArrow when available or schema-aware per-batch materialization, while capsule-capable versions consume the native Arrow stream. Install `xbbg[polars]` to include the timezone data required on Windows.
- `backend="polars_lazy"` returns a Polars `LazyFrame`; `backend="narwhals_lazy"` returns a genuine Narwhals lazy frame backed by Polars and requires Polars. Bloomberg retrieval and Arrow-to-Polars conversion have already happened: only subsequent local dataframe operations are deferred, with no Bloomberg query pushdown.
- DuckDB results use isolated per-relation connections to one shared process-local in-memory database, not a new database per conversion. A retained relation keeps its connection and registered Arrow input alive; releasing one relation does not invalidate another. The shared database anchor is closed at process exit. If this backend was initialized before a fork, xbbg refuses inherited backend use or explicit close in the child; use multiprocessing `spawn` or fork before initializing the DuckDB backend.

## Async usage

Use async helpers directly in async applications:

```python
import asyncio
from xbbg import blp

async def main():
    aapl, msft = await asyncio.gather(
        blp.abdp("AAPL US Equity", "PX_LAST"),
        blp.abdp("MSFT US Equity", "PX_LAST"),
    )
    return aapl, msft

result = asyncio.run(main())
```

In Jupyter, VS Code Interactive, and marimo, one-shot sync calls such as `blp.bdp(...)` and `blp.bdh(...)` use a notebook-only bridge when the notebook event loop is already running. Generic async applications such as FastAPI or ASGI services should still use the async APIs directly.

## Subscriptions: raw, tick mode, and all fields

Use `asubscribe()` when you need dynamic add/remove, explicit unsubscribe, raw Arrow batches, or subscription health diagnostics. Use `astream()` for the simple async iterator, or `stream()` for synchronous iteration.

```python
from xbbg import asubscribe

sub = await asubscribe(
    ["AAPL US Equity"],
    ["LAST_PRICE", "BID", "ASK"],
    tick_mode=True,
    all_fields=True,
    conflate=True,
)

async for tick in sub:
    print(tick)       # dict ticks in tick_mode
    print(sub.stats)  # messages_received, dropped_batches, data_loss_events, ...
    break

await sub.unsubscribe()
```

```python
raw_sub = await asubscribe(["AAPL US Equity"], ["LAST_PRICE"], raw=True)

async for batch in raw_sub:
    print(batch.to_table())  # raw xbbg ArrowRecordBatch -> ArrowTable
    break

await raw_sub.unsubscribe()
```

Key behaviors:

- `output` accepts exactly `record_batch`, `backend`, `dict`, or `tick` (case-insensitive); an explicit selector overrides both `raw` and `tick_mode`
- `raw=True` or `output="record_batch"` yields raw xbbg `ArrowRecordBatch` values for max-performance consumers
- `tick_mode=True`, `output="dict"`, or `output="tick"` returns native dict ticks and implies raw subscription mode
- `output="backend"` returns the configured backend output, the same as default iteration without `raw=True`
- `all_fields=True` exposes all top-level scalar Bloomberg subscription fields; unrequested arrays/complex fields are omitted, while explicitly requested unsupported shapes fail rather than being truncated
- filtered mode keeps requested fields plus `MKTDATA_EVENT_TYPE` and `MKTDATA_EVENT_SUBTYPE`
- `conflate=True` requests Bloomberg-conflated quote updates on `//blp/mktdata`; trades are still delivered as received
- `sub.add(...)`, `sub.remove(...)`, `sub.status`, `sub.events`, `sub.failed_tickers`, and `sub.stats` expose runtime control and diagnostics
- use `async with` on an acquired subscription or `try`/`finally` with `await sub.unsubscribe()` for deterministic cleanup; `unsubscribe(drain=True)` returns a list in the same dict/raw/backend representation as iteration (an empty drain is `[]`). An unread stream failure is raised after cleanup, never hidden by a successful partial drain.

Subscription rows are **deltas, not complete images**. In dict ticks, a missing key means unchanged; a present `None` means an explicit Bloomberg clear. Arrow batches retain nullable data columns and append non-null binary `__xbbg_present`: bit `i` (least-significant bit first) marks whether schema column `i + 2` is present, after `timestamp` and `topic`. A set bit plus a null cell is a clear; an unset bit is unchanged. Use each batch's current schema because all-fields layouts can grow or promote types. Schema metadata key `xbbg.subscription_presence` documents the encoding and mapping.

Native queues fail closed on any continuity gap: Bloomberg `DATALOSS`, a full `drop_newest` buffer, or overflow/timeout of the bounded `block` forwarder. Already committed queue data is followed by one `BlpSubscriptionDataLossError`, then EOF; no post-gap deltas are delivered. Its `topic` and `detail` identify the gap (`topic="*"` denotes unattributed/session-wide loss). **Resubscribe for a fresh image** before applying further deltas. Terminal session errors also wake pending reads, including handles whose topics were all removed. Ordinary connection-down notifications remain nonterminal, and a single rejected topic does not terminate healthy siblings.

Python `stream()` producers share one managed background event-loop thread, not a thread per stream. Each sync bridge has a bounded queue (`stream_capacity`, default **256**, minimum **1**) and asynchronously waits for consumer space; native overflow policy remains separate. Active sync producers are admitted per global/scoped engine up to `max_subscription_sessions`; excess producers raise `RuntimeError` rather than creating more tasks. Callbacks run on the consuming thread. Close the generator explicitly when stopping early: close cancels and waits for producer cleanup, and a cleanup timeout is reported while the producer remains tracked against its admission limit. In async applications use `astream()` directly and close it explicitly when retaining the generator after an early exit.

In Node, pass `{ allFields: true }` to `stream()` / `subscribe()` helpers for the same top-level field expansion. Default iteration yields scalar `Tick` objects; `sub.arrow()` constructs Arrow JS tables without IPC for supported schemas. Exposed mutable buffers are JS-owned snapshots: exclusive bounded allocations can be transferred, while shared/sliced/oversized storage is copied or canonicalized first. This is not a universal zero-copy Rust/JS boundary. Choose scalar or Arrow reads once per subscription; see the [Node lifecycle and benchmark contracts](js-xbbg/README.md).

## MCP server

The repository also includes a local MCP server for coding-agent workflows. It wraps selected xbbg request/response operations and returns bounded JSON results with schema metadata.

See [`apps/xbbg-mcp/README.md`](apps/xbbg-mcp/README.md) for installation, supported environment variables, raw GitHub Release tar/zip assets, and the `xbbg-mcp-v<VERSION>.mcpb` local connector artifact. Official MCP Registry publication uses the generated `server.json` metadata after the matching GitHub Release contains the `.mcpb`; no MCP release asset includes Bloomberg SDK files or runtime components.

## Troubleshooting

Empty results usually mean one of the inputs or entitlements is wrong rather than that the Python call failed:

```python
from xbbg import blp

# Check security lookup and field discovery
print(blp.blkp("Apple", yellowkey="eqty"))
print(blp.fieldSearch("vwap"))
```

Connection failures:

- confirm Bloomberg Terminal is running and logged in for local DAPI usage
- confirm the host, port, auth method, TLS files, and entitlements for SAPI/B-PIPE/ZFP environments
- run `print(xbbg.get_sdk_info())` to see how the SDK/runtime was detected
- enable SDK logging before the first session when debugging low-level connection problems

Timeouts and large responses:

- increase per-request timeout where appropriate
- split large historical/tick requests into smaller date ranges
- enable opt-in sharding for wide multi-security `bdp`/`bdh` requests with `shard_requests=True`
- tune `request_pool_size`, `subscription_pool_size`, queue sizes, and keep-alive settings for managed infrastructure

When reporting issues, include:

1. xbbg version: `import xbbg; print(xbbg.__version__)`
2. Python version and operating system
3. Bloomberg connection mode: DAPI, SAPI/B-PIPE, ZFP, or other
4. minimal code to reproduce
5. full traceback or error message

## Development

Set up the development environment with [pixi](https://pixi.sh/):

```bash
# Stage an authorized Bloomberg SDK locally under vendor/blpapi-sdk/
bash ./scripts/sdktool.sh               # macOS/Linux
# .\scripts\sdktool.ps1                # Windows PowerShell

# Install the environment and compile the Rust extension
pixi install
pixi run install
```

Common checks:

```bash
pixi run test
pixi run lint
pixi run ci
```

For non-live tests, use `xbbg.testing`:

```python
from xbbg import blp
from xbbg.testing import create_mock_response, mock_engine

response = create_mock_response(
    service="//blp/refdata",
    operation="ReferenceDataRequest",
    data={"AAPL US Equity": {"PX_LAST": 101.23}},
)

with mock_engine([response]):
    df = blp.bdp("AAPL US Equity", "PX_LAST")
```

Publishing is handled through GitHub Actions and PyPI Trusted Publishing.

## Project links

- Documentation: [xbbg.org](https://xbbg.org/)
- JavaScript/Node bindings: [js-xbbg/README.md](js-xbbg/README.md)
- Python LangChain/LangGraph tools: [usage](#python-langchain-and-langgraph)
- JavaScript LangChain/LangGraph tools: [js-xbbg-langgraph/README.md](js-xbbg-langgraph/README.md)
- PyPI: [pypi.org/project/xbbg](https://pypi.org/project/xbbg/)
- npm: [npmjs.com/package/@xbbg/core](https://www.npmjs.com/package/@xbbg/core)
- crates.io: [crates.io/crates/xbbg_core](https://crates.io/crates/xbbg_core)
- Source: [github.com/underloam/xbbg](https://github.com/underloam/xbbg)
- Issues: [GitHub Issues](https://github.com/underloam/xbbg/issues)
- Discord: [Join the community](https://discord.gg/P34uMwgCjC)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Security: [SECURITY.md](SECURITY.md)
- License: [LICENSE](LICENSE)

## Citation

If you use xbbg in research or published work, please cite:

```bibtex
@software{xbbg,
  author = {{xbbg contributors}},
  title = {{xbbg}: Independent client for Bloomberg-connected data workflows},
  year = {2026},
  publisher = {GitHub},
  url = {https://github.com/underloam/xbbg},
  version = {1.3.0}
}
```
