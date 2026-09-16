# xbbg-mcp

Last updated: 2026-09-04.

Stdio MCP server for Bloomberg request/response workflows backed by `xbbg-async`.

This binary is intended for coding agents such as Claude Code and OpenCode that can launch a local MCP server process and call tools over stdio.

## What it exposes

The current server exposes request/response tools only:

- `bdp` - reference data
- `bdh` - historical data
- `bds` - bulk data
- `bdib` - intraday bars
- `bql` - Bloomberg Query Language
- `bsrch` - Bloomberg search
- `bflds` - field metadata lookup
- `check_entitlements` - entitlement-ID check for a Bloomberg service
- `request` - generic raw/custom request path

Responses are returned as bounded structured JSON with Arrow schema metadata so an agent can inspect the shape without receiving an unbounded payload. Limits cover rows, cells, strings, metadata inspection/retention, and the final serialized payload; a row limit alone is not the output bound.

## Entitlement IDs

Set `return_eids: true` only on routes backed by Bloomberg operations that support `returnEids`:

| MCP route | Bloomberg operation | EID option |
| --- | --- | --- |
| `bdp`, `bds` | `ReferenceDataRequest` (including BDS/bulk) | `return_eids: true` |
| `bdh` | `HistoricalDataRequest` | `return_eids: true` |
| `bdib` | `IntradayBarRequest` | `return_eids: true` |
| `request` | `IntradayTickRequest` | `return_eids: true` |

The dedicated `bdp`, `bds`, `bdh`, and `bdib` tools advertise `return_eids` directly. Use the generic `request` tool for intraday ticks:

```json
{
  "service": "//blp/refdata",
  "operation": "IntradayTickRequest",
  "extractor": "intraday_tick",
  "security": "AAPL US Equity",
  "start_datetime": "2024-01-15T09:30:00",
  "end_datetime": "2024-01-15T10:00:00",
  "event_types": ["TRADE"],
  "return_eids": true
}
```

When retained within the metadata and output budgets, results expose the EID map as structured JSON at `metadata["xbbg.eid_data"]`, alongside `schema`, `row_count`, `returned_rows`, `truncated`, and `rows`. `truncated` is an object of flags, not a single boolean. Consult `metadata_counts["xbbg.eid_data"]` when present for total/returned EID and security counts, `valid`, and `counts_complete`; an unknown total is `null`, not zero. `truncation_counts.omitted_priority_metadata` identifies priority metadata that could not be retained. Pass the collected IDs to the read-only `check_entitlements` tool; `service` defaults to `//blp/refdata`:

```json
{ "eids": [101, 202], "service": "//blp/refdata" }
```

The check result includes `service`, `eids`, `entitled`, and `failed_eids`, plus total, returned, and omitted counts for both ID lists and per-component `truncated` flags. Failed IDs receive the shared cell/byte allowance before the echoed input IDs. A shortened list must not be interpreted as the full entitlement result.

## Install from GitHub Releases

For macOS arm64 and Linux amd64, install the latest wrapper + binary pair with:

```bash
curl -fsSL https://raw.githubusercontent.com/underloam/xbbg/main/scripts/install-xbbg-mcp.sh | sh
```

To install a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/underloam/xbbg/main/scripts/install-xbbg-mcp.sh | sh -s -- 1.0.0
```

The installer places two files in `~/.local/bin/` by default:

- `xbbg-mcp` - launcher wrapper
- `xbbg-mcp-real` - compiled binary

GitHub release assets include only the launcher wrapper and compiled xbbg binary. They do **not** include Bloomberg SDK files or the Bloomberg runtime. You must provide those locally from a source you are authorized to use under your Bloomberg agreements and entitlements.

GitHub Release tar/zip assets remain the raw binary channel. The MCPB asset is the directory and one-click local connector channel for MCPB-aware clients and registries.

The wrapper locates the Bloomberg runtime in this order:

1. `XBBG_MCP_LIB_DIR`
2. `BLPAPI_LIB_DIR`
3. `BLPAPI_ROOT`
4. locally staged authorized SDK under `vendor/blpapi-sdk/`
5. the official Python `blpapi` package

If you install Bloomberg's Python package, the wrapper can usually run without any extra shell configuration:

```bash
pip install blpapi --index-url https://blpapi.bloomberg.com/repository/releases/python/simple/
```

Windows release assets are attached as `.zip` files, but the convenience installer currently targets macOS/Linux only.

## Install MCPB

Claude Desktop and MCPB-aware registries can use the `xbbg-mcp-v<VERSION>.mcpb` asset attached to GitHub Releases. The MCPB currently supports macOS arm64, Linux amd64, and Windows amd64. The raw platform tar/zip assets remain available for manual installs and troubleshooting.

The MCPB does not include Bloomberg SDK files, Bloomberg runtime libraries, credentials, or market data. Configure `XBBG_MCP_LIB_DIR`, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`, or install Bloomberg's official Python `blpapi` package so the launcher can find the authorized Bloomberg runtime locally.

On Windows the MCPB PowerShell launcher follows the same order, then also scans `PATH` (a Bloomberg Terminal install puts `C:\blp\DAPI` there) before falling back to the Python package. `xbbg-mcp.exe` is 64-bit and needs `blpapi3_64.dll`; a runtime whose version cannot be read, or which is too old to export the entry points the binary imports, is skipped with a diagnostic. The minimum, currently Bloomberg API 3.20.0, is derived from imported symbol versions when the bundle is packaged; packaging fails rather than silently disabling the gate if derivation is impossible. The launcher makes the validated directory both the child's working directory and the first `PATH` entry, and refuses a higher-priority DLL beside the executable or in a Windows system directory that would shadow it. It starts `xbbg-mcp.exe` with inherited standard handles, so the MCP host talks to the binary directly rather than through PowerShell. Loader-status diagnostics name the selected Bloomberg runtime without claiming that it caused every direct or transitive loader failure.

## Troubleshooting

The server writes only to stderr, which MCP hosts such as Claude Desktop capture in their server logs. When stdin is closed by the host it prints `xbbg-mcp: stdin closed; shutting down`; a failed stdin read, reader-thread failure, or abnormal service stop returns a nonzero process status. Diagnostic writes are best-effort, so a closed stderr handle cannot terminate the stdin reader or turn a failure into clean EOF. Set `RUST_LOG=info` (or `debug`) to also see the engine's and the MCP library's own log lines.

## Build from source

```bash
bash ./scripts/sdktool.sh
cargo build --release -p xbbg-mcp --locked
./scripts/xbbg-mcp
```

## Claude Code

```bash
claude mcp add --transport stdio xbbg -- ~/.local/bin/xbbg-mcp
```

## OpenCode

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "xbbg": {
      "type": "local",
      "command": ["/Users/you/.local/bin/xbbg-mcp"],
      "enabled": true
    }
  }
}
```

## Directory and registry publishing

Official MCP Registry: publish the generated `server.json` after the release contains the `.mcpb` asset.

GitHub MCP Registry: verify the server appears after official registry ingestion.

Claude Connectors Directory: submit the MCPB as a desktop extension; remote connector submission is not the default for Bloomberg-local workflows.

Smithery: publish the MCPB bundle.

Glama: verify indexing after official registry publication; if automated sandbox build cannot run without Bloomberg runtime, keep the listing metadata-focused.

PulseMCP: wait for official registry ingestion or use the Pulse submit page.

MCP.so: submit the GitHub repo/release after canonical registry metadata is live.

## Runtime environment

`xbbg-mcp` accepts the same engine-oriented connection settings as the Rust core, with MCP-prefixed names taking precedence where available.

Common settings:

- `XBBG_MCP_HOST` / `XBBG_HOST`
- `XBBG_MCP_PORT` / `XBBG_PORT`
- `XBBG_MCP_AUTH_METHOD` / `XBBG_AUTH_METHOD`
- `XBBG_MCP_APP_NAME`
- `XBBG_MCP_DIR_PROPERTY`
- `XBBG_MCP_USER_ID`
- `XBBG_MCP_IP_ADDRESS`
- `XBBG_MCP_TOKEN`
- `XBBG_MCP_REQUEST_POOL_SIZE`
- `XBBG_MCP_VALIDATION_MODE` / `XBBG_VALIDATION_MODE` – `disabled` (default), `lenient`, `strict`
- `XBBG_MCP_SDK_LOG_LEVEL` / `XBBG_SDK_LOG_LEVEL` – `off` (default), `fatal`, `error`, `warn`, `info`, `debug`, `trace`
- `XBBG_MCP_OVERFLOW_POLICY` / `XBBG_OVERFLOW_POLICY` – `drop_newest` (default), `block`

Supported auth methods:

- `none`
- `user`
- `app`
- `userapp`
- `dir`
- `directory`
- `manual`
- `token`

### Result budgets

These MCP-only environment settings use integer values; unset settings use the defaults below. Values below the minimum or invalid integers cause configuration to fail rather than silently disabling a limit.

| Setting | Default | Minimum | Scope |
| --- | ---: | ---: | --- |
| `XBBG_MCP_MAX_ROWS` | 500 | 1 | Returned rows |
| `XBBG_MCP_MAX_CELLS` | 50,000 | 1 | Returned cells and inspected schema columns; also shared entitlement-list items and validation-error record count |
| `XBBG_MCP_MAX_METADATA_PROPERTIES` | 50,000 | 1 | Aggregate retained metadata properties/array items and metadata-entry inspection |
| `XBBG_MCP_MAX_METADATA_BYTES` | 65,536 | 1 | Aggregate raw metadata inspection bytes before parsing, and retained metadata plus its count summaries |
| `XBBG_MCP_MAX_STRING_CHARS` | 2,048 | 1 | Unicode characters per string |
| `XBBG_MCP_MAX_STRING_BYTES` | 8,192 | 3 | UTF-8 bytes per string, including a truncation marker when needed |
| `XBBG_MCP_MAX_RESULT_BYTES` | 1,048,576 | 2,048 | Final serialized structured JSON payload, including schema, metadata, and diagnostics; also bounds serialized error results |

The final-byte limit includes JSON escaping and envelope overhead; it is not just a sum of raw string lengths and does not describe the surrounding MCP transport framing. The limits are ceilings: metadata and schema consume space before rows, so a payload may return fewer rows or columns than their individual limits allow. The cell allowance applies across returned rows and columns, not separately to every row. Even zero-row batches have bounded schema output.

String values are shortened on Unicode boundaries. Column names and metadata keys are identities: oversized names are omitted rather than shortened, and duplicate column names are not silently overwritten in JSON rows. Nested Arrow values are represented by a bounded omission marker instead of recursively formatting an unbounded cell; omission counts distinguish this from an ordinary scalar value. Integer values outside JavaScript's safe range are returned as strings rather than rounded numbers.

Metadata inspection is bounded before JSON parsing. Oversized entries and generic metadata that cannot be inspected within the aggregate allowance may be omitted without a full scan. `xbbg.security_errors`, `xbbg.field_exceptions`, and `xbbg.eid_data` are prioritized before generic metadata and rows. Diagnostic records are retained with their structure rather than arbitrary property fragments, but string contents and the number of records remain bounded; even priority metadata can be omitted when it cannot fit.

### Output diagnostics and errors

Result envelopes are extensible. For example, a one-row result can include the following excerpt (additional counters are omitted here):

```json
{
  "schema": [{ "name": "PX_LAST", "data_type": "Float64", "nullable": true }],
  "row_count": 1,
  "column_count": 1,
  "returned_rows": 1,
  "returned_columns": 1,
  "returned_cells": 1,
  "result_budget_bytes": 1048576,
  "truncated": {
    "rows": false,
    "columns": false,
    "cells": false,
    "values": false,
    "metadata": false,
    "output": false
  },
  "rows": [{ "PX_LAST": 190.1 }]
}
```

`row_count` and `column_count` describe the source batch; the `returned_*` fields describe what was emitted. `truncation_counts` reports omissions and inspection completeness, including `omitted_rows`, `omitted_columns`, `omitted_cells`, `known_omitted_value_bytes`, `omitted_complex_values`, and `known_omitted_metadata_properties`. Treat `known_*` counts as lower bounds unless completeness is established, not as evidence of an exhaustive scan. In particular, `schema_column_count_complete`, `metadata_input_count_complete`, and `metadata_property_count_complete` qualify the related diagnostics; `omitted_metadata_input_bytes` is `null` when the input count is incomplete. Optional `metadata_counts` supplies additional per-key diagnostics.

Errors are bounded too: request/validation/internal failures remain errors, not success-shaped empty results. Error messages respect string and escaped-JSON byte limits; `message_truncated` indicates shortening. Existing adapter error codes are preserved, while omitted auxiliary error data is marked with `error_data_omitted`. Validation errors return bounded structured `errors` records with `path`, `message`, and a per-record `truncated` flag, plus `total_errors`, `returned_errors`, and `omitted_errors`. Space is reserved for the primary validation diagnostic even at the 2,048-byte minimum result budget: its path/message take priority over a long summary or optional `suggestion`. The path and message can be shortened and suggestions or later records omitted, but a later error does not displace the primary diagnostic.

### Serialization concurrency

The server uses two async runtime workers. Small result conversions stay inline; larger or expensive conversions are offloaded to blocking workers. A shared permit limit allows at most two offloaded serializations at once, with the permit held until conversion finishes. This bounds serialization concurrency, not the total process thread count: Bloomberg engine work and stdio handling have their own lifecycle.

## Privacy Policy

`xbbg-mcp` runs locally as a stdio MCP server. It does not send telemetry or usage data to underloam, GitHub, Anthropic, Smithery, Glama, or any other third party.

Tool calls are handled on the user's machine and forwarded only to the Bloomberg runtime/API endpoint configured by the user (`XBBG_MCP_HOST` / `XBBG_HOST`, default `localhost:8194`) under that user's Bloomberg agreements and entitlements. Results are returned only to the MCP client process that launched the server.

Bloomberg validates entitlements at request time, including per-security and per-field access. A valid local session can still return empty or partial results when the user's DAPI, SAPI/B-PIPE, or ZFP entitlement set does not cover the requested data.

The server reads configuration from environment variables and does not persist request data, credentials, or Bloomberg responses to disk.

## Smoke test

After building locally, verify the stdio handshake and a few live requests with:

```bash
uv run python -X utf8 scripts/xbbg_mcp_smoke.py
```

That script expects a live Bloomberg connection and a locally built `target/debug/xbbg-mcp` binary, or `target/release/xbbg-mcp` if no debug build is present.
