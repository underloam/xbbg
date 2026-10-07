import type * as xbbg from "@xbbg/core";
import {
  DEFAULT_MAX_SECURITIES,
  DEFAULT_MAX_FIELDS,
  DEFAULT_MAX_ROWS,
  DEFAULT_MAX_STRING_CHARS,
  DEFAULT_MAX_RESULT_BYTES,
  DEFAULT_MAX_RESULT_NODES,
  DEFAULT_MAX_CONTENT_BYTES,
  DEFAULT_MAX_CONTENT_ROWS,
  DEFAULT_MAX_BQL_QUERY_CHARS,
  DEFAULT_MAX_SEARCH_SPEC_CHARS,
  DEFAULT_MAX_STREAM_UPDATES,
  DEFAULT_MAX_STREAM_WAIT_MS,
  MIN_TOOL_RESULT_BYTES as MIN_RESULT_BYTE_BUDGET,
  MIN_TOOL_RESULT_NODES as MIN_RESULT_NODE_BUDGET,
  type BloombergToolName,
} from "./_defs_gen";

import type { XbbgCoreLike, XbbgEngineLike } from "./core-loader";

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
