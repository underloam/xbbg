<!-- markdownlint-disable MD033 MD041 -->
<div align="center">

<a href="https://xbbg.org/javascript/">
  <img src="https://raw.githubusercontent.com/underloam/xbbg/main/.github/assets/readme-hero-flat.svg" alt="xbbg banner" width="100%">
</a>

[![npm version](https://img.shields.io/npm/v/%40xbbg%2Flanggraph.svg)](https://www.npmjs.com/package/@xbbg/langgraph)
[![Node.js versions](https://img.shields.io/node/v/%40xbbg%2Flanggraph.svg)](https://www.npmjs.com/package/@xbbg/langgraph)
[![npm downloads per month](https://img.shields.io/npm/dm/%40xbbg%2Flanggraph.svg)](https://www.npmjs.com/package/@xbbg/langgraph)
[![CI](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml/badge.svg)](https://github.com/underloam/xbbg/actions/workflows/ci-rust.yml)
[![Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/P34uMwgCjC)

**Links:** [Documentation](https://xbbg.org/javascript/) · [Tool reference](https://github.com/underloam/xbbg/blob/main/js-xbbg-langgraph/REFERENCE.md) · [`@xbbg/core`](https://www.npmjs.com/package/@xbbg/core) · [GitHub](https://github.com/underloam/xbbg) · [Changelog](https://github.com/underloam/xbbg/blob/main/CHANGELOG.md)

</div>

---

# @xbbg/langgraph

LangChain and LangGraph tools for Bloomberg data, backed by [`@xbbg/core`](https://www.npmjs.com/package/@xbbg/core). It gives agents bounded Bloomberg request tools, finite live-data snapshots, and model guidance; it is not a chat app, MCP server, or browser package.

> **Important:** `@xbbg/langgraph` is an independent open-source project. It is not affiliated with, endorsed by, sponsored by, or approved by Bloomberg Finance L.P. or its affiliates. It does not provide Bloomberg access, credentials, entitlements, data rights, or SDK licenses; use your own under your Bloomberg agreements and policies.

## Why @xbbg/langgraph?

- **Ordinary LangChain tools.** They work with `createAgent`, LangGraph's `ToolNode`, and anything else that accepts LangChain tools.
- **Bounded by default.** Tools cap securities, fields, rows, bytes, and wait time, and return a short model-facing preview plus a separately bounded artifact for your application.
- **No open-ended streams.** Live data comes from snapshot tools that stop after a set number of updates or a timeout and always unsubscribe.
- **Guidance for the model.** `BLOOMBERG_TOOL_INSTRUCTIONS` tells the model to ask before guessing tickers or fields, look up unknown fields first, and report empty or truncated results honestly.
- **Built on `@xbbg/core`.** Released in lockstep with it and uses its engine, typed errors, and connection modes.

## Install

```bash
npm install @xbbg/langgraph @xbbg/core @langchain/core
# For a LangChain agent
npm install langchain @langchain/openai
```

Requires Node.js 24+ on a server, plus Bloomberg access (Terminal/DAPI, B-PIPE, SAPI, or ZFP) and Bloomberg's SDK runtime library on the machine running the tools.

## Quickstart

```ts
import { createAgent } from "langchain";
import { ChatOpenAI } from "@langchain/openai";
import { createAllBloombergTools, BLOOMBERG_TOOL_INSTRUCTIONS } from "@xbbg/langgraph";

const agent = createAgent({
  model: new ChatOpenAI({ model: "gpt-4.1" }),
  tools: createAllBloombergTools({ maxSecurities: 10, maxFields: 10 }),
  systemPrompt: BLOOMBERG_TOOL_INSTRUCTIONS,
});

const result = await agent.invoke({
  messages: [{ role: "user", content: "Get PX_LAST for IBM US Equity." }],
});
```

For a custom LangGraph graph, bind the same tools to your model and route tool calls through `ToolNode`; see the [tool reference](https://github.com/underloam/xbbg/blob/main/js-xbbg-langgraph/REFERENCE.md#langgraph-example).

## Tools

`createBloombergTools()` returns the 23 Bloomberg tools, `createBloombergExtTools()` the 11 helpers, and `createAllBloombergTools()` all 34. Remove any with `disabledTools`, or build one with its factory, such as `createBdpTool()`.

| Group          | Tools                                                                                                                                                                                                                                          |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Requests       | `xbbg_bdp`, `xbbg_bdh`, `xbbg_bds`, `xbbg_bdib`, `xbbg_bdtick`, `xbbg_bql`, `xbbg_bsrch`, `xbbg_bqr`, `xbbg_bflds`, `xbbg_beqs`, `xbbg_check_entitlements`                                                                                     |
| Recipes        | `xbbg_yas`, `xbbg_preferreds`, `xbbg_corporate_bonds`, `xbbg_index_members`, `xbbg_resolve_isins`, `xbbg_issuer_isins`, `xbbg_etf_holdings`                                                                                                    |
| Auctions       | `xbbg_resolve_venues`, `xbbg_auction_snapshot`                                                                                                                                                                                                 |
| Live snapshots | `xbbg_stream_snapshot`, `xbbg_mktbar_snapshot`, `xbbg_depth_snapshot`                                                                                                                                                                          |
| Helpers        | `xbbg_ext_ticker`, `xbbg_ext_futures`, `xbbg_ext_cdx`, `xbbg_ext_currency`, `xbbg_ext_bql_builder`, `xbbg_ext_market_session`, `xbbg_ext_yas_overrides`, `xbbg_ext_constants`, `xbbg_ext_columns`, `xbbg_ext_calculate`, `xbbg_ext_chart_spec` |

`xbbg_ext_chart_spec` turns rows from a data tool into a Vega-Lite spec your frontend can render.

## Engine and limits

The first tool call connects `@xbbg/core` once and shares the engine across the tool set, with a 60-second request timeout. Pass `engine` to use your own connection instead; its configuration and lifecycle stay yours.

```ts
import * as xbbg from "@xbbg/core";
import { createBloombergTools } from "@xbbg/langgraph";

const engine = await xbbg.connect({ host: "localhost", port: 8194 });
const tools = createBloombergTools({ engine, maxRows: 200 });
```

Defaults include 25 securities and 25 fields per request, 500 artifact rows, 50 model-facing rows, and 10 updates or 15 seconds per live snapshot. Tools honor LangGraph's `AbortSignal`. The [tool reference](https://github.com/underloam/xbbg/blob/main/js-xbbg-langgraph/REFERENCE.md) lists every limit, the result format, and each tool's inputs.

## License

[Apache-2.0](https://github.com/underloam/xbbg/blob/main/LICENSE)
