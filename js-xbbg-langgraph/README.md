# @xbbg/langgraph

Last updated: 2026-09-30.

LangChain/LangGraph-compatible Bloomberg tools backed by [`@xbbg/core`](../js-xbbg/README.md).

This package is a reusable tool adapter. It is not a chat app, HTTP server, MCP server, browser package, or agent framework.

## Status

Supported server-side LangChain/LangGraph adapter for Bloomberg-connected Node.js 24+ environments. `@xbbg/langgraph` is published alongside `@xbbg/core`, uses the same semver release cadence, and keeps Bloomberg connectivity in the core Node binding while exposing bounded tools, structured artifacts, and model-facing guidance for agent runtimes.

## Prerequisites

Bloomberg connectivity is still provided by `@xbbg/core`: an installed Bloomberg Terminal/Desktop API, B-PIPE, SAPI, or ZFP setup plus Bloomberg SDK runtime libraries must be available on the server running the tools.

The package targets Node.js 24+ and runs on the server side. It is not a browser package; frontends should call an application backend that owns Bloomberg connectivity.

## Install

Tool package only:

```bash
npm install @xbbg/langgraph @xbbg/core @langchain/core
```

LangGraph app:

```bash
npm install @xbbg/langgraph @xbbg/core @langchain/core @langchain/langgraph @langchain/openai
```

Current LangChain agent app:

```bash
npm install @xbbg/langgraph @xbbg/core @langchain/core langchain @langchain/openai
```

## Agent guidance

Append the exported instructions to your system prompt:

```ts
import { BLOOMBERG_TOOL_INSTRUCTIONS } from "@xbbg/langgraph";
```

The instructions tell the model to ask clarifying questions for ambiguous tickers, fields, date ranges, currencies, periodicity, overrides, or universes; request `/isin/{isin}` for ISIN identifiers and `/cusip/{cusip}` for CUSIPs (the auction recipes also accept valid bare ISINs); use `xbbg_bflds` for unknown fields; prefer finite recipe tools for BEQS/YAS/universe and validated auction workflows; use bounded snapshot tools instead of open subscriptions; keep requests bounded; and report empty, truncated, or errored responses directly.

## LangChain `createAgent` example

```ts
import { createAgent } from "langchain";
import { ChatOpenAI } from "@langchain/openai";
import { createAllBloombergTools, BLOOMBERG_TOOL_INSTRUCTIONS } from "@xbbg/langgraph";

const tools = createAllBloombergTools({
  maxSecurities: 10,
  maxFields: 10,
});

const agent = createAgent({
  model: new ChatOpenAI({ model: "gpt-4.1" }),
  tools,
  systemPrompt: BLOOMBERG_TOOL_INSTRUCTIONS,
});

const result = await agent.invoke({
  messages: [{ role: "user", content: "Get <FIELD> for <TICKER> <MARKET_SECTOR>." }],
});
```

## LangGraph example

`createReactAgent` from `@langchain/langgraph/prebuilt` is deprecated upstream in favor of `createAgent` from `langchain`, but it is still common in LangGraph examples and accepts these tools because they are normal LangChain tools.

```ts
import { createReactAgent } from "@langchain/langgraph/prebuilt";
import { ChatOpenAI } from "@langchain/openai";
import { createBloombergTools, BLOOMBERG_TOOL_INSTRUCTIONS } from "@xbbg/langgraph";

const agent = createReactAgent({
  llm: new ChatOpenAI({ model: "gpt-4.1" }),
  tools: createBloombergTools({ maxSecurities: 5, maxFields: 5 }),
  prompt: BLOOMBERG_TOOL_INSTRUCTIONS,
});
```

For custom graphs, bind the tools to your model and route tool calls through LangGraph's `ToolNode`:

```ts
import { AIMessage } from "@langchain/core/messages";
import { END, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { ToolNode } from "@langchain/langgraph/prebuilt";
import { ChatOpenAI } from "@langchain/openai";
import { createBloombergTools, BLOOMBERG_TOOL_INSTRUCTIONS } from "@xbbg/langgraph";

const tools = createBloombergTools({ maxSecurities: 5, maxFields: 5 });
const model = new ChatOpenAI({ model: "gpt-4.1" }).bindTools(tools);
const toolNode = new ToolNode(tools);

const callModel = async (state: typeof MessagesAnnotation.State) => ({
  messages: [
    await model.invoke([
      { role: "system", content: BLOOMBERG_TOOL_INSTRUCTIONS },
      ...state.messages,
    ]),
  ],
});

const route = (state: typeof MessagesAnnotation.State) => {
  const last = state.messages.at(-1);
  return last instanceof AIMessage && last.tool_calls?.length ? "tools" : END;
};

const graph = new StateGraph(MessagesAnnotation)
  .addNode("model", callModel)
  .addNode("tools", toolNode)
  .addEdge(START, "model")
  .addConditionalEdges("model", route)
  .addEdge("tools", "model")
  .compile();
```

All tools use LangChain `responseFormat: "content_and_artifact"`. In `ToolNode`, the tool message content is a separately bounded model-facing preview; `artifact` is the structured, independently bounded projection for application code. Content normally starts with a compact summary followed by JSON, but very small byte budgets can reduce it to a compact summary alone.

## Tool factories

`createBloombergTools()` includes all 23 core tools by default. `createBloombergExtTools()` adds 11 extension helpers; `createAllBloombergTools()` includes all 34 tools. `BLOOMBERG_TOOL_NAMES` lists the complete inventory, and `disabledTools` removes named tools from these factories.

Core Bloomberg request tools:

- `xbbg_bdp` - reference/current fields for a bounded securities list and explicit fields list.
- `xbbg_bdh` - historical time series; requires explicit `start` and `end`.
- `xbbg_bds` - one Bloomberg bulk/table field.
- `xbbg_bdib` - intraday bars; requires explicit `start`, `end`, and `interval`.
- `xbbg_bdtick` - intraday ticks; requires explicit `start`, `end`, and event types when the default stream is not intended.
- `xbbg_check_entitlements` - checks a nonempty EID list against `//blp/refdata` or an explicitly supplied Bloomberg service.
- `xbbg_bql` - BQL expressions only.
- `xbbg_bsrch` - Bloomberg search/grid requests, not normal security lookup.
- `xbbg_bqr` - Bloomberg Quote Request / fixed-income dealer quotes; prefer identifiers such as `/isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>`.
- `xbbg_bflds` - field metadata/search; use first for uncertain mnemonics.
- `xbbg_beqs` - named Bloomberg BEQS equity screens.
- `xbbg_yas` - fixed-income YAS recipe fields for yield, duration, spread, benchmark, or price analytics.
- `xbbg_preferreds` - preferred stock discovery for one equity ticker.
- `xbbg_corporate_bonds` - corporate bond universe query for one issuer/company ticker.
- `xbbg_index_members` - index constituents through the core index recipe.
- `xbbg_resolve_isins` - raw ISIN-to-security resolution; pass raw ISIN strings without `/isin/` prefixes.
- `xbbg_resolve_venues` - validated primary venue routing for equities and preferreds, with per-call pricing-source overrides and per-input status/error rows.
- `xbbg_auction_snapshot` - finite reference-data auction/imbalance fields on validated venues; unresolved or mismatched rows retain diagnostics and null field values.
- `xbbg_issuer_isins` - issuer/bond ISIN workflow starting from known bond ISIN strings.
- `xbbg_etf_holdings` - ETF holdings for one ETF ticker.
- `xbbg_stream_snapshot` - bounded `//blp/mktdata` live observation that always unsubscribes.
- `xbbg_mktbar_snapshot` - bounded `//blp/mktbar` live bar observation for one ticker.
- `xbbg_depth_snapshot` - bounded `//blp/mktdepthdata` market-depth observation for one ticker.

### Auction recipes

Both auction tools take a bounded `securities` array of Bloomberg tickers, valid bare ISINs, `/isin/<ISIN>`, or `/bbgid/<FIGI>` inputs. The core routes composite equities to their primary exchange listing and preferreds to a pricing-source-qualified venue, validates the returned `EXCH_CODE` or `PRICING_SOURCE`, and respects explicitly supplied venues. Never guess a venue ticker or accept a silent composite fallback.

The recipes keep one row per input in input order, including duplicates. `input_order`, `security`, `status`, and `error` remain available alongside the venue; `xbbg_resolve_venues` also returns routing metadata such as `lookup`, `method`, and `venue_figi`. Inspect every row's `status` (`resolved`, `unresolved`, `unsupported`, or `mismatch`) rather than treating the whole response as a success. Auction field values are null on unresolved, unsupported, or mismatched rows. Normal artifact/content limits still apply, so inspect truncation diagnostics before assuming every row is present.

Optional `pcsOverrides` is a per-call string record mapping preferred exchange names to pricing sources, for example `{ "NEW YORK": "SNY2" }`. Overrides take precedence over the packaged mappings, exchange names are normalized by the core, and overridden venues are still validated. `xbbg_auction_snapshot` accepts optional `fields` in requested column order; omitting them or passing `[]` selects `AuctionFields.default` from `@xbbg/core`. The `maxFields` limit applies to explicitly supplied fields, not the core's default bundle.

The tools use the core's JSON row backend. Booleans and finite floating-point values remain JSON primitives; Arrow Int64 and Time64 microsecond values become decimal strings in the adapter to avoid loss of integer precision. Date32 values are UTC-midnight epoch-millisecond numbers; time-only values have no invented date or timezone. Duplicate security rows are retained. JSON objects cannot represent repeated column names; use the core Arrow backend when repeated field columns are required.

```ts
import { createResolveVenuesTool, createAuctionSnapshotTool } from "@xbbg/langgraph";

const venues = createResolveVenuesTool();
const auction = createAuctionSnapshotTool();

await venues.invoke({ securities: ["IBM US Equity", "AAPL US Equity"] });
await auction.invoke({
  securities: ["IBM US Equity"],
  fields: ["IMBALANCE_INDIC_RT", "ORDER_IMB_BUY_VOLUME", "ORDER_IMB_SELL_VOLUME"],
  pcsOverrides: { "NEW YORK": "SNY2" },
});
```

These tools use finite recipe requests with `backend: "json"`; they do not open streams or require `maxUpdates`.

### Entitlement IDs

The `xbbg_bdp`, `xbbg_bds`, `xbbg_bdh`, `xbbg_bdib`, and `xbbg_bdtick` inputs accept `returnEids: true`. These map only to Bloomberg's EID-capable `ReferenceDataRequest` (including BDS), `HistoricalDataRequest`, `IntradayBarRequest`, and `IntradayTickRequest` operations.

When EIDs are requested, bounded tool artifacts can retain result metadata alongside the bounded rows. For example, an untruncated zero-row artifact can include:

```json
{
  "tool": "xbbg_bdp",
  "data": {
    "rows": [],
    "eidData": { "<TICKER> <MARKET_SECTOR>": [101, 202] },
    "metadata": { "xbbg.eid_data": "{\"<TICKER> <MARKET_SECTOR>\":[101,202]}" }
  },
  "rowCount": 0,
  "truncated": false
}
```

The row limit alone does not discard attached `eidData`, `metadata`, `securityErrors`, or `fieldExceptions`; byte, node, string, and entitlement limits still apply. Diagnostics and EID metadata are prioritized over ordinary properties. When present, `eidDataTruncation` reports retained IDs and securities, omissions, and whether original totals are complete; an unknown original count is not zero. Metadata may itself be omitted under tighter budgets, so inspect the truncation diagnostics before treating an EID map as complete. Pass the collected IDs to `xbbg_check_entitlements`:

```json
{ "eids": [101, 202], "service": "//blp/refdata" }
```

Securities are passed through in the form the user supplied them: Bloomberg tickers as `<TICKER> <MARKET_SECTOR>` (for example `<TICKER> <EXCHANGE> Equity`, `<INDEX_TICKER> Index`, `<CCY_PAIR> Curncy`), raw ISINs as `/isin/<ISIN>`, raw CUSIPs as `/cusip/<CUSIP>`. The auction recipes also accept valid bare ISINs and normalize them in the core. The market sector ending is Bloomberg's yellow key — `Equity`, `Index`, `Curncy`, `Comdty`, `Corp`, `Govt`, `Muni`, `Mtge`, `M-Mkt`, or `Pfd` (preferred securities) — and general request tools pass it through to Bloomberg unvalidated; auction recipes validate venue routing separately. The agent guidance and securities/ticker field descriptions instruct the model that the ticker format is a template, not authorization to construct one — identifiers are never converted into guessed tickers; `xbbg_resolve_isins` exists for explicit security resolution and `xbbg_resolve_venues` for venue resolution. Note `xbbg_ext_ticker`'s `parse_ticker` is narrower than the request tools: it parses generic futures-style tickers only (`Index`/`Curncy`/`Comdty`/`Corp`, or `<ROOT><N> <EXCHANGE> Equity`) and rejects other sectors.
BQL is passed as one complete expression string. Use placeholder shapes such as `get(<FIELD>) for('<TICKER> <MARKET_SECTOR>')`, `get(<FIELD_1>, <FIELD_2>) for(['<TICKER_1> <MARKET_SECTOR>', '<TICKER_2> <MARKET_SECTOR>'])`, `get(<FIELD>, <WEIGHT_FIELD>) for(holdings('<ETF_TICKER> <MARKET_SECTOR>'))`, or `get(<FIELD>) for(members('<INDEX_TICKER> <MARKET_SECTOR>')) with(...)`. Prefer `xbbg_bdp`/`xbbg_bdh` for simple reference or historical requests.

Dealer quote / BQR workflows in xbbg use fixed-income identifiers with a quote source, for example `/isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>`; use `xbbg_bqr` for that workflow and `xbbg_bdtick` for raw intraday ticks.

Streaming surfaces are intentionally exposed only as bounded snapshot tools (`xbbg_stream_snapshot`, `xbbg_mktbar_snapshot`, and `xbbg_depth_snapshot`). Each streaming snapshot requires `maxUpdates`, applies the configured `maxStreamUpdates`/`maxStreamWaitMs` caps, stops on count, timeout, or stream completion, and calls `unsubscribe(false)` unless `drain: true` is explicitly provided. The package does not expose open-ended async subscription iterators as agent tools. If collection succeeds but releasing the subscription fails, the snapshot result still returns the collected updates and reports the failure in an `unsubscribeError` field instead of discarding data.

Arrow snapshot rows share one materialization allowance across all updates, not a fresh allowance for each table: at most `min(max(maxRows, maxContentRows), floor(maxResultNodes / 3))` rows are read before projection. Materialized rows are charged to the same aggregate node budget used for result bounding. An update cut short at this stage carries `rowCount`, `rows`, `truncated`, and a `truncation` diagnostic with `reason: "max_rows"` and `omittedRowsAtLeast`. A collection failure remains the primary thrown error even if unsubscribe also fails.

```ts
import { createBloombergTools, createBdpTool } from "@xbbg/langgraph";

const tools = createBloombergTools();
const bdpOnly = createBdpTool({ maxSecurities: 3 });
```

Extension helper tools:

- `xbbg_ext_ticker` - ticker hygiene before live requests: parse, normalize lists, filter equity tickers, check specific contracts, and validate generic futures tickers.
- `xbbg_ext_futures` - futures construction and selection: month-code lookup, build a specific contract, generate candidates from a generic, rank contracts, filter by cycle, and filter valid contracts for a date.
- `xbbg_ext_cdx` - CDX workflows: parse CDX tickers, roll to previous series, resolve generic to specific series, and run predefined CDX info/pricing/risk field bundles.
- `xbbg_ext_currency` - currency planning: build FX pair metadata, test same-currency requests, and identify currencies needing conversion to a target.
- `xbbg_ext_bql_builder` - BQL query builders for preferred stocks, corporate bonds, and ETF holdings; prefer these over hand-writing those query shapes.
- `xbbg_ext_market_session` - exchange sessions and timezones: derive sessions, infer timezone, convert local session times to UTC, fetch market rules, compute turnover/BQR default ranges, and inspect exchange overrides.
- `xbbg_ext_yas_overrides` - build flat YAS override maps for lower-level fixed-income BDP workflows. Prefer `xbbg_yas` when you want the actual YAS recipe result.
- `xbbg_ext_constants` - static constants and formatting helpers for dates, futures months, dividend types, and ETF/dividend columns.
- `xbbg_ext_columns` - rename helpers for dividend, ETF, and earnings-shaped Bloomberg responses.
- `xbbg_ext_calculate` - small numeric helper for level percentage calculations.
- `xbbg_ext_chart_spec` - renderer-neutral chart specs for frontend generative UI; converts bounded rows from `xbbg_bdh`, `xbbg_bdib`, holdings, depth, or already-shaped row data into Vega-Lite JSON.

```ts
import { createBloombergExtTools, createAllBloombergTools } from "@xbbg/langgraph";

const helperTools = createBloombergExtTools();
const allTools = createAllBloombergTools({
  disabledTools: ["xbbg_bql", "xbbg_bsrch"],
});
```

### Chart specs and generative UI frontends

`xbbg_ext_chart_spec` does not fetch Bloomberg data and does not render images. Use a data tool first, then pass its bounded rows into the chart-spec helper. The result is a portable visualization payload:

```json
{
  "kind": "xbbg.visualization",
  "component": "xbbg_chart",
  "renderer": "vega-lite",
  "spec": { "data": { "values": [] }, "mark": { "type": "line" } }
}
```

Frontend runtimes should register their own `xbbg_chart` component and render `data.spec` with their preferred Vega-Lite React wrapper. This keeps `@xbbg/langgraph` dependency-free and works with CopilotKit `useRenderTool`, assistant-ui data/generative UI renderers, Vercel AI SDK tool parts, or LangGraph `typedUi()` adapters.

Example tool input after a historical request:

```json
{
  "source": "bdh",
  "rows": [{ "ticker": "AAPL US Equity", "date": "20240102", "value": 190.1 }],
  "xField": "date",
  "yFields": ["value"],
  "seriesField": "ticker",
  "title": "AAPL PX_LAST"
}
```

## Engine handling

By default the first tool invocation lazily imports `@xbbg/core`, calls `connect(engineConfig)`, and reuses the resulting engine across the tool set. Parallel LangGraph tool calls share the same in-flight initialization promise.

Lazily connected engines get a hard per-request timeout (`DEFAULT_ENGINE_REQUEST_TIMEOUT_MS`, 60s) because `@xbbg/core` disables request timeouts by default, which would let a wedged Terminal session hang tool calls forever. Pass `engineConfig: { requestTimeoutMs: ... }` to change it, or `0` to disable. A user-supplied `engine` is used as-is — its configuration and lifecycle (including disconnect) stay with the caller.

```ts
import * as xbbg from "@xbbg/core";
import { createBloombergTools } from "@xbbg/langgraph";

const engine = await xbbg.connect({ host: "localhost", port: 8194 });
const tools = createBloombergTools({ engine });
```

### Cancellation

Tools honor the LangChain/LangGraph `AbortSignal` (`graph.invoke(input, { signal })`): an aborted call rejects immediately, already-cancelled calls never start Bloomberg work, and snapshot tools stop collecting and unsubscribe right away (skipping `drain`) instead of running out their timeout. In-flight Bloomberg request/response calls cannot be cancelled mid-flight; they are bounded by the engine request timeout above.

## Limits and outputs

Numeric options must be positive safe integers. Defaults and minima:

| Option               |   Default | Minimum | Scope                                                                                         |
| -------------------- | --------: | ------: | --------------------------------------------------------------------------------------------- |
| `maxSecurities`      |        25 |       1 | Securities per request                                                                        |
| `maxFields`          |        25 |       1 | Fields per request                                                                            |
| `maxRows`            |       500 |       1 | Aggregate primary rows in the application artifact                                            |
| `maxResultBytes`     | 1,048,576 |     256 | UTF-8 bytes of the serialized artifact, including its envelope                                |
| `maxContentRows`     |        50 |       1 | Aggregate primary rows in model-facing content                                                |
| `maxContentBytes`    |    65,536 |     256 | UTF-8 bytes of the complete model-facing content, including summary and JSON                  |
| `maxResultNodes`     |    50,000 |      10 | Aggregate inspection work across materialization, canonical preparation, and both projections |
| `maxStringChars`     |     2,000 |       1 | String bounding before the two projections                                                    |
| `maxBqlQueryChars`   |     4,000 |       1 | BQL input length                                                                              |
| `maxSearchSpecChars` |     1,000 |       1 | Search specification input length                                                             |
| `maxStreamUpdates`   |        10 |       1 | Snapshot updates                                                                              |
| `maxStreamWaitMs`    |    15,000 |       1 | Snapshot wait in milliseconds                                                                 |

Artifact and content limits are independent, not successive cuts of the artifact. A smaller model preview does not shrink the application artifact; conversely, `maxRows: 1` and `maxContentRows: 3` can retain one artifact row and three model rows when the other budgets permit. Both projections come from a shared bounded preparation of the original result, so exhausted shared node or string limits can affect both. These are ceilings, not promises to fill every row or byte allowance.

The node budget is aggregate across the whole result, including nested properties and both projections; it does not reset for each object or array. Primary row collections likewise share the relevant projection's row allowance. Depth is capped at 32. Cycles, accessors, binary data, unsupported values, upstream truncation, and exhausted row/string/node/byte budgets produce explicit truncation reasons rather than unbounded traversal or raw binary output.

Date inputs accept `YYYY-MM-DD` or `YYYYMMDD` strings, integer `YYYYMMDD` values (parsed as calendar dates), and epoch milliseconds; ambiguous numbers between those ranges and ambiguous `MM/DD/YYYY` strings are rejected with actionable schema errors. `Date` instances are deliberately not part of the wire contract: JSON tool calls cannot carry them and `z.date()` breaks JSON Schema conversion in zod v4.

Schemas only advertise parameters the engine accepts: `format` exists on `xbbg_bdp`/`xbbg_bdh` only, because the engine rejects it for BulkData (`xbbg_bds`), BQL, search, field-info, and BEQS output; a model-sent `format` on those tools is stripped rather than forwarded. The exported `toolParameterJsonSchema(tool)` returns the provider-ready `$ref`-free JSON Schema used in each tool's embedded provider definition.

Empty results are called out in the model-facing summary (`empty result; verify identifiers, fields, and date range before concluding no data exists`) so agents distinguish "no rows" from silent failure instead of inventing data.

Finite request results use `backend: "json"`. For a small untruncated result, model-facing content can look like:

```text
xbbg_bdp: 1 row; artifactTruncated=false; contentTruncated=false; artifactReasons=none; contentReasons=none
{"tool":"xbbg_bdp","rowCount":1,"truncated":false,"contentTruncated":false,"data":[{"security":"AAPL US Equity","field":"PX_LAST","value":190.1}]}
```

The corresponding application artifact has its own envelope and budget:

```json
{
  "tool": "xbbg_bdp",
  "rowCount": 1,
  "truncated": false,
  "data": [{ "security": "AAPL US Equity", "field": "PX_LAST", "value": 190.1 }]
}
```

These examples illustrate the current shape, not an exhaustive field list. `data` varies by tool and can include a metadata-bearing object with `rows` rather than a bare array. `rowCount` describes the source count when known (or is `null`), not necessarily the number of rows retained. `truncated` in the artifact and normal content payload refers to the artifact; `contentTruncated` and the summary's separate reason lists describe model-preview limits.

When available, `truncation` includes `reasons`, `inspectedNodes`, `retainedNodes`, `omittedRows`, and `omittedPropertiesAtLeast`. Lower-bound counts deliberately do not claim an exhaustive scan. Small budgets can omit counters or data while retaining a compact truncation indication. Check diagnostics rather than inferring completeness from a short result.

Bloomberg errors are prioritized ahead of ordinary properties, and model content can expose them separately under `diagnostics` even when the relevant data rows are omitted. `hasErrors: true` marks detected errors; tight content budgets use a compact summary to retain that signal. Bounded primary diagnostics are retained in small-budget previews where they fit, rather than replaced by an apparent success or empty result. This does not guarantee every error record or its full text survives every budget. Thrown `Error` instances keep the original error as `cause` and preserve its name when wrapped with tool context; snapshot cleanup failures do not replace a primary collection error.

When invoking tools outside an agent graph and you need the artifact, invoke with a tool-call id (or use LangGraph `ToolNode`) so LangChain returns a `ToolMessage` with `artifact`.

Use smaller factories or `disabledTools` when broad BQL/search helpers are not appropriate for a deployment.
