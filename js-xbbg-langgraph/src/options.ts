import type * as xbbg from "@xbbg/core";

import type { XbbgCoreLike, XbbgEngineLike } from "./core-loader";

export const BLOOMBERG_TOOL_NAMES = [
  "xbbg_bdp",
  "xbbg_bdh",
  "xbbg_bds",
  "xbbg_bdib",
  "xbbg_bdtick",
  "xbbg_check_entitlements",
  "xbbg_bql",
  "xbbg_bsrch",
  "xbbg_bqr",
  "xbbg_bflds",
  "xbbg_beqs",
  "xbbg_yas",
  "xbbg_preferreds",
  "xbbg_corporate_bonds",
  "xbbg_index_members",
  "xbbg_resolve_isins",
  "xbbg_resolve_venues",
  "xbbg_auction_snapshot",
  "xbbg_issuer_isins",
  "xbbg_etf_holdings",
  "xbbg_stream_snapshot",
  "xbbg_mktbar_snapshot",
  "xbbg_depth_snapshot",
  "xbbg_ext_ticker",
  "xbbg_ext_futures",
  "xbbg_ext_cdx",
  "xbbg_ext_currency",
  "xbbg_ext_bql_builder",
  "xbbg_ext_chart_spec",
  "xbbg_ext_market_session",
  "xbbg_ext_yas_overrides",
  "xbbg_ext_constants",
  "xbbg_ext_columns",
  "xbbg_ext_calculate",
] as const;

export type BloombergToolName = (typeof BLOOMBERG_TOOL_NAMES)[number];

export interface BloombergToolsOptions {
  readonly engine?: XbbgEngineLike;
  readonly engineConfig?: xbbg.EngineConfig;
  readonly core?: XbbgCoreLike;
  readonly maxSecurities?: number;
  readonly maxFields?: number;
  /** Maximum aggregate primary rows retained in the structured artifact. */
  readonly maxRows?: number;
  readonly maxStringChars?: number;
  /** Maximum UTF-8 bytes retained in the structured tool artifact. Minimum 256. */
  readonly maxResultBytes?: number;
  /** Maximum aggregate values/properties inspected while bounding one result. Minimum 10. */
  readonly maxResultNodes?: number;
  /** Maximum UTF-8 bytes sent back to the model as tool content. Minimum 256. */
  readonly maxContentBytes?: number;
  /** Maximum aggregate primary rows in model content; independent of artifact maxRows. */
  readonly maxContentRows?: number;
  readonly maxBqlQueryChars?: number;
  readonly maxSearchSpecChars?: number;
  readonly maxStreamUpdates?: number;
  readonly maxStreamWaitMs?: number;
  readonly validateFields?: boolean;
  readonly disabledTools?: readonly BloombergToolName[];
}

export interface NormalizedBloombergToolsOptions {
  readonly engine?: XbbgEngineLike;
  readonly engineConfig: xbbg.EngineConfig;
  readonly core?: XbbgCoreLike;
  readonly maxSecurities: number;
  readonly maxFields: number;
  readonly maxRows: number;
  readonly maxStringChars: number;
  readonly maxResultBytes: number;
  readonly maxResultNodes: number;
  readonly maxContentBytes: number;
  readonly maxContentRows: number;
  readonly maxBqlQueryChars: number;
  readonly maxSearchSpecChars: number;
  readonly maxStreamUpdates: number;
  readonly maxStreamWaitMs: number;
  readonly validateFields: boolean | undefined;
  readonly disabledTools: ReadonlySet<BloombergToolName>;
}

const DEFAULT_MAX_SECURITIES = 25;
const DEFAULT_MAX_FIELDS = 25;
const DEFAULT_MAX_ROWS = 500;
const DEFAULT_MAX_STRING_CHARS = 2000;
const DEFAULT_MAX_RESULT_BYTES = 1_048_576;
const DEFAULT_MAX_RESULT_NODES = 50_000;
const DEFAULT_MAX_CONTENT_BYTES = 65_536;
const DEFAULT_MAX_CONTENT_ROWS = 50;
const MIN_RESULT_BYTE_BUDGET = 256;
const MIN_RESULT_NODE_BUDGET = 10;
const DEFAULT_MAX_BQL_QUERY_CHARS = 4000;
const DEFAULT_MAX_SEARCH_SPEC_CHARS = 1000;
const DEFAULT_MAX_STREAM_UPDATES = 10;
const DEFAULT_MAX_STREAM_WAIT_MS = 15_000;

/**
 * Default hard per-request timeout applied to lazily connected engines.
 * @xbbg/core disables request timeouts by default (`requestTimeoutMs: 0`),
 * which would let a wedged Terminal session hang tool calls forever. An
 * explicit `engineConfig.requestTimeoutMs` (including 0) always wins.
 */
export const DEFAULT_ENGINE_REQUEST_TIMEOUT_MS = 60_000;

function engineConfigWithDefaults(config: xbbg.EngineConfig | undefined): xbbg.EngineConfig {
  if (config?.requestTimeoutMs !== undefined) {
    return config;
  }
  return { ...config, requestTimeoutMs: DEFAULT_ENGINE_REQUEST_TIMEOUT_MS };
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(
      `${name} must be a positive integer no greater than Number.MAX_SAFE_INTEGER; got ${String(value)}`,
    );
  }
  return value;
}

function integerAtLeast(
  value: number | undefined,
  fallback: number,
  minimum: number,
  name: string,
): number {
  const normalized = positiveInteger(value, fallback, name);
  if (normalized < minimum) {
    throw new RangeError(`${name} must be at least ${String(minimum)}; got ${String(normalized)}`);
  }
  return normalized;
}

function disabledToolSet(
  tools: readonly BloombergToolName[] | undefined,
): ReadonlySet<BloombergToolName> {
  return new Set(tools ?? []);
}

export function normalizeBloombergToolsOptions(
  options: BloombergToolsOptions = {},
): NormalizedBloombergToolsOptions {
  return {
    core: options.core,
    disabledTools: disabledToolSet(options.disabledTools),
    engine: options.engine,
    engineConfig: engineConfigWithDefaults(options.engineConfig),
    maxBqlQueryChars: positiveInteger(
      options.maxBqlQueryChars,
      DEFAULT_MAX_BQL_QUERY_CHARS,
      "maxBqlQueryChars",
    ),
    maxFields: positiveInteger(options.maxFields, DEFAULT_MAX_FIELDS, "maxFields"),
    maxContentBytes: integerAtLeast(
      options.maxContentBytes,
      DEFAULT_MAX_CONTENT_BYTES,
      MIN_RESULT_BYTE_BUDGET,
      "maxContentBytes",
    ),
    maxContentRows: positiveInteger(
      options.maxContentRows,
      DEFAULT_MAX_CONTENT_ROWS,
      "maxContentRows",
    ),
    maxRows: positiveInteger(options.maxRows, DEFAULT_MAX_ROWS, "maxRows"),
    maxResultBytes: integerAtLeast(
      options.maxResultBytes,
      DEFAULT_MAX_RESULT_BYTES,
      MIN_RESULT_BYTE_BUDGET,
      "maxResultBytes",
    ),
    maxResultNodes: integerAtLeast(
      options.maxResultNodes,
      DEFAULT_MAX_RESULT_NODES,
      MIN_RESULT_NODE_BUDGET,
      "maxResultNodes",
    ),
    maxSearchSpecChars: positiveInteger(
      options.maxSearchSpecChars,
      DEFAULT_MAX_SEARCH_SPEC_CHARS,
      "maxSearchSpecChars",
    ),
    maxStreamUpdates: positiveInteger(
      options.maxStreamUpdates,
      DEFAULT_MAX_STREAM_UPDATES,
      "maxStreamUpdates",
    ),
    maxStreamWaitMs: positiveInteger(
      options.maxStreamWaitMs,
      DEFAULT_MAX_STREAM_WAIT_MS,
      "maxStreamWaitMs",
    ),
    maxSecurities: positiveInteger(options.maxSecurities, DEFAULT_MAX_SECURITIES, "maxSecurities"),
    maxStringChars: positiveInteger(
      options.maxStringChars,
      DEFAULT_MAX_STRING_CHARS,
      "maxStringChars",
    ),
    validateFields: options.validateFields,
  };
}

export function isToolDisabled(
  options: NormalizedBloombergToolsOptions,
  name: BloombergToolName,
): boolean {
  return options.disabledTools.has(name);
}
