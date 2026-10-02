import type { StructuredToolInterface } from "@langchain/core/tools";
import type { OverflowPolicy } from "./_defs_gen";

import { createCoreResolver, type CoreResolver } from "./core-loader";
import {
  AUCTION_SNAPSHOT_DESCRIPTION,
  BDP_DESCRIPTION,
  BDH_DESCRIPTION,
  BDS_DESCRIPTION,
  BDIB_DESCRIPTION,
  BDTICK_DESCRIPTION,
  CHECK_ENTITLEMENTS_DESCRIPTION,
  BFLDS_DESCRIPTION,
  BQL_DESCRIPTION,
  BSRCH_DESCRIPTION,
  BQR_DESCRIPTION,
  BEQS_DESCRIPTION,
  CORPORATE_BONDS_DESCRIPTION,
  ETF_HOLDINGS_DESCRIPTION,
  DEPTH_SNAPSHOT_DESCRIPTION,
  INDEX_MEMBERS_DESCRIPTION,
  ISSUER_ISINS_DESCRIPTION,
  MKTBAR_SNAPSHOT_DESCRIPTION,
  PREFERREDS_DESCRIPTION,
  RESOLVE_ISINS_DESCRIPTION,
  RESOLVE_VENUES_DESCRIPTION,
  STREAM_SNAPSHOT_DESCRIPTION,
  YAS_DESCRIPTION,
} from "./descriptions";
import { createBloombergStructuredTool, type ToolInvocationConfig } from "./langchain-tool";
import type { BloombergToolsOptions, BloombergToolName } from "./options";
import { isToolDisabled } from "./options";
import {
  createToolResult,
  MAX_ENTITLEMENT_EIDS,
  throwWithToolContext,
  type ToolContentAndArtifact,
} from "./result-limits";
import {
  createAuctionSnapshotSchema,
  createBeqsSchema,
  createBdhSchema,
  createBdibSchema,
  createBdtickSchema,
  createBdpSchema,
  createCheckEntitlementsSchema,
  createBdsSchema,
  createBfldsSchema,
  createBqlSchema,
  createBsrchSchema,
  createBqrSchema,
  createCorporateBondsSchema,
  createDepthSnapshotSchema,
  createEtfHoldingsSchema,
  createIndexMembersSchema,
  createIssuerIsinsSchema,
  createMktbarSnapshotSchema,
  createPreferredsSchema,
  createResolveIsinsSchema,
  createResolveVenuesSchema,
  createStreamSnapshotSchema,
  createYasSchema,
  type AuctionSnapshotInput,
  type BdhInput,
  type BeqsInput,
  type BdibInput,
  type BdtickInput,
  type BdpInput,
  type BdsInput,
  type CheckEntitlementsInput,
  type BfldsInput,
  type BqlInput,
  type BsrchInput,
  type BqrInput,
  type CorporateBondsInput,
  type DepthSnapshotInput,
  type EtfHoldingsInput,
  type IndexMembersInput,
  type IssuerIsinsInput,
  type MktbarSnapshotInput,
  type PreferredsInput,
  type ResolveIsinsInput,
  type ResolveVenuesInput,
  type StreamSnapshotInput,
  type YasInput,
} from "./schemas";

export type BloombergTool = StructuredToolInterface;

type ToolCreator = (resolver: CoreResolver) => BloombergTool;

function resultString(
  resolver: CoreResolver,
  name: BloombergToolName,
  value: unknown,
): ToolContentAndArtifact {
  return createToolResult(name, value, resolver.options);
}

type SnapshotReason = "max_updates" | "timeout" | "done";

interface SnapshotControlInput {
  readonly maxUpdates: number;
  readonly timeoutMs: number;
  readonly drain?: boolean;
}

interface SnapshotSubscriptionLike {
  next(): Promise<IteratorResult<unknown>>;
  unsubscribe(drain?: boolean): Promise<unknown>;
}

interface SnapshotResult {
  readonly updateCount: number;
  readonly maxUpdates: number;
  readonly timeoutMs: number;
  readonly reason: SnapshotReason;
  readonly updates: readonly unknown[];
  /** Set when collection succeeded but releasing the subscription failed. */
  readonly unsubscribeError?: string;
}

interface CollectedSnapshot {
  readonly materializedRows: number;
  readonly result: SnapshotResult;
}

interface StreamOptionsInput {
  readonly options?: readonly string[];
  readonly conflate?: boolean;
  readonly flushThreshold?: number;
  readonly overflowPolicy?: OverflowPolicy;
  readonly streamCapacity?: number;
  readonly allFields?: boolean;
  readonly fields?: readonly string[];
}

const STREAM_TIMEOUT = Symbol("stream_timeout");
const STREAM_ABORTED = Symbol("stream_aborted");

function abortError(signal: AbortSignal | undefined): Error {
  const reason: unknown = signal?.reason;
  return reason instanceof Error ? reason : new Error("Tool call aborted");
}

function streamOptions(input: StreamOptionsInput): StreamOptionsInput {
  return {
    allFields: input.allFields,
    conflate: input.conflate,
    flushThreshold: input.flushThreshold,
    options: input.options,
    overflowPolicy: input.overflowPolicy,
    streamCapacity: input.streamCapacity,
  };
}

/** mktbar/depth take fields through options; stream() takes them positionally. */
function singleTickerStreamOptions(input: StreamOptionsInput): StreamOptionsInput {
  return { ...streamOptions(input), fields: input.fields };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

interface TruncatedArrowRows {
  readonly rows: readonly unknown[];
  readonly rowCount: number;
  readonly truncated: true;
  readonly truncation: {
    readonly omittedRowsAtLeast: number;
    readonly reason: "max_rows";
  };
}

interface NormalizedStreamUpdate {
  readonly materializedRows: number;
  readonly value: unknown;
}

function truncatedArrowRows(rows: readonly unknown[], rowCount: number): TruncatedArrowRows {
  return {
    rows,
    rowCount,
    truncated: true,
    truncation: {
      omittedRowsAtLeast: rowCount - rows.length,
      reason: "max_rows",
    },
  };
}

function rowsFromArrowTable(value: unknown, maxRows: number): NormalizedStreamUpdate | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const numRows = value.numRows;
  const get = value.get;
  if (
    typeof numRows === "number" &&
    Number.isSafeInteger(numRows) &&
    numRows >= 0 &&
    typeof get === "function"
  ) {
    const retainedRows = Math.min(numRows, maxRows);
    const rows: unknown[] = [];
    for (let index = 0; index < retainedRows; index += 1) {
      rows.push(get.call(value, index));
    }
    return {
      materializedRows: retainedRows,
      value: retainedRows === numRows ? rows : truncatedArrowRows(rows, numRows),
    };
  }
  return undefined;
}

function normalizeStreamUpdate(value: unknown, maxRows: number): NormalizedStreamUpdate {
  const arrowRows = rowsFromArrowTable(value, maxRows);
  if (arrowRows !== undefined) {
    return arrowRows;
  }
  if (isRecord(value)) {
    const toObject = value.toObject;
    if (typeof toObject === "function") {
      return { materializedRows: 0, value: toObject.call(value) };
    }
  }
  return { materializedRows: 0, value };
}

async function nextWithinTimeout(
  iterator: SnapshotSubscriptionLike,
  deadlineMs: number,
  signal: AbortSignal | undefined,
): Promise<IteratorResult<unknown> | typeof STREAM_TIMEOUT | typeof STREAM_ABORTED> {
  if (signal?.aborted === true) {
    return STREAM_ABORTED;
  }
  const remainingMs = deadlineMs - Date.now();
  if (remainingMs <= 0) {
    return STREAM_TIMEOUT;
  }
  const nextPromise = iterator.next();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const racers: Promise<IteratorResult<unknown> | typeof STREAM_TIMEOUT | typeof STREAM_ABORTED>[] =
    [
      nextPromise,
      new Promise<typeof STREAM_TIMEOUT>((resolve) => {
        timer = setTimeout(() => resolve(STREAM_TIMEOUT), remainingMs);
      }),
    ];
  if (signal !== undefined) {
    racers.push(
      new Promise<typeof STREAM_ABORTED>((resolve) => {
        onAbort = () => resolve(STREAM_ABORTED);
        signal.addEventListener("abort", onAbort, { once: true });
      }),
    );
  }
  const result = await Promise.race(racers);
  if (timer !== undefined) {
    clearTimeout(timer);
  }
  if (signal !== undefined && onAbort !== undefined) {
    signal.removeEventListener("abort", onAbort);
  }
  if (result === STREAM_TIMEOUT || result === STREAM_ABORTED) {
    // The abandoned next() settles later (unsubscribe wakes it); swallow it.
    void nextPromise.catch(() => undefined);
  }
  return result;
}

async function collectSnapshot(
  subscription: SnapshotSubscriptionLike,
  input: SnapshotControlInput,
  signal: AbortSignal | undefined,
  maxRows: number,
  maxResultNodes: number,
): Promise<CollectedSnapshot> {
  const updates: unknown[] = [];
  const deadlineMs = Date.now() + input.timeoutMs;
  // Each retained Arrow row expands into at least one container and one
  // serialized value, so reserve two thirds of the node budget downstream.
  const arrowRowAllowance = Math.min(maxRows, Math.floor(maxResultNodes / 3));
  let remainingArrowRows = arrowRowAllowance;
  let reason: SnapshotReason = "max_updates";
  let failed = false;
  let caught: unknown;
  try {
    while (updates.length < input.maxUpdates) {
      const next = await nextWithinTimeout(subscription, deadlineMs, signal);
      if (next === STREAM_ABORTED) {
        throw abortError(signal);
      }
      if (next === STREAM_TIMEOUT) {
        reason = "timeout";
        break;
      }
      if (next.done === true) {
        reason = "done";
        break;
      }
      const normalized = normalizeStreamUpdate(next.value, remainingArrowRows);
      updates.push(normalized.value);
      remainingArrowRows -= normalized.materializedRows;
    }
  } catch (error) {
    failed = true;
    caught = error;
  }
  // Never drain on abort: release the subscription as fast as possible.
  const drain = input.drain === true && signal?.aborted !== true;
  let unsubscribeError: string | undefined;
  try {
    await subscription.unsubscribe(drain);
  } catch (error) {
    // A collection error outranks a cleanup error; a cleanup-only failure is
    // reported in the result instead of discarding the collected updates.
    if (!failed) {
      unsubscribeError = error instanceof Error ? error.message : String(error);
    }
  }
  if (failed) {
    throw caught;
  }
  return {
    materializedRows: arrowRowAllowance - remainingArrowRows,
    result: {
      maxUpdates: input.maxUpdates,
      reason,
      timeoutMs: input.timeoutMs,
      updateCount: updates.length,
      updates,
      ...(unsubscribeError === undefined ? {} : { unsubscribeError }),
    },
  };
}

function validationSetting(
  resolver: CoreResolver,
  value: boolean | undefined,
): boolean | undefined {
  return value ?? resolver.options.validateFields;
}

function bdpWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bdp" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BdpInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const options = {
          backend: "json" as const,
          format: input.format,
          includeSecurityErrors: input.includeSecurityErrors,
          kwargs: input.kwargs,
          overrides: input.overrides as never,
          validateFields: validationSetting(resolver, input.validateFields),
          returnEids: input.returnEids,
        };
        const result = await engine.bdp(input.securities, input.fields, options);
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BDP_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBdpSchema(resolver.options),
    },
  );
}

function bdhWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bdh" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BdhInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const options = {
          backend: "json" as const,
          end: input.end,
          format: input.format,
          kwargs: input.kwargs,
          overrides: input.overrides as never,
          start: input.start,
          returnEids: input.returnEids,
          validateFields: validationSetting(resolver, input.validateFields),
        };
        const result = await engine.bdh(input.securities, input.fields, options);
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BDH_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBdhSchema(resolver.options),
    },
  );
}

function bdsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bds" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BdsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const options = {
          backend: "json" as const,
          kwargs: input.kwargs,
          overrides: input.overrides as never,
          returnEids: input.returnEids,
          validateFields: validationSetting(resolver, input.validateFields),
        };
        const result = await engine.bds(input.securities, [input.field], options);
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BDS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBdsSchema(resolver.options),
    },
  );
}

function bdibWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bdib" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BdibInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bdib(input.ticker, {
          backend: "json",
          end: input.end,
          eventType: input.eventType,
          interval: input.interval,
          kwargs: input.kwargs,
          outputTz: input.outputTz,
          requestTz: input.requestTz,
          returnEids: input.returnEids,
          start: input.start,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BDIB_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBdibSchema(resolver.options),
    },
  );
}

function bdtickWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bdtick" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BdtickInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bdtick(input.ticker, {
          backend: "json",
          end: input.end,
          eventTypes: input.eventTypes,
          includeBicMicCodes: input.includeBicMicCodes,
          includeBloombergStandardConditionCodes: input.includeBloombergStandardConditionCodes,
          includeBrokerCodes: input.includeBrokerCodes,
          includeConditionCodes: input.includeConditionCodes,
          includeExchangeCodes: input.includeExchangeCodes,
          includeNonPlottableEvents: input.includeNonPlottableEvents,
          includeRpsCodes: input.includeRpsCodes,
          kwargs: input.kwargs,
          outputTz: input.outputTz,
          requestTz: input.requestTz,
          returnEids: input.returnEids,
          start: input.start,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BDTICK_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBdtickSchema(resolver.options),
    },
  );
}

function checkEntitlementsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_check_entitlements" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CheckEntitlementsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.checkEntitlements(input.service ?? "//blp/refdata", input.eids);
        return createToolResult(name, result, {
          ...resolver.options,
          maxRows: MAX_ENTITLEMENT_EIDS,
        });
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: CHECK_ENTITLEMENTS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createCheckEntitlementsSchema(resolver.options),
    },
  );
}

function bqlWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bql" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BqlInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bql(input.query, {
          backend: "json",
          kwargs: input.kwargs,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BQL_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBqlSchema(resolver.options),
    },
  );
}

function bsrchWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bsrch" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BsrchInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bsrch(input.searchSpec, {
          backend: "json",
          kwargs: input.kwargs,
          overrides: input.overrides,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BSRCH_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBsrchSchema(resolver.options),
    },
  );
}

function bqrWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bqr" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BqrInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bqr(input.ticker, {
          backend: "json",
          endDatetime: input.end,
          eventTypes: input.eventTypes,
          includeBrokerCodes: input.includeBrokerCodes,
          startDatetime: input.start,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BQR_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBqrSchema(resolver.options),
    },
  );
}

function bfldsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_bflds" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BfldsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.bflds({
          backend: "json",
          fields: input.fields,
          kwargs: input.kwargs,
          searchSpec: input.searchSpec,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BFLDS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBfldsSchema(resolver.options),
    },
  );
}

function beqsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_beqs" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BeqsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.beqs(input.screen, {
          asof: input.asof,
          backend: "json",
          group: input.group,
          kwargs: input.kwargs,
          overrides: input.overrides,
          screenType: input.screenType,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: BEQS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createBeqsSchema(resolver.options),
    },
  );
}

function yasWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_yas" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: YasInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.yas(input.tickers, input.fields, {
          backend: "json",
          benchmark: input.benchmark,
          price: input.price,
          settleDt: input.settleDt,
          spread: input.spread,
          yieldType: input.yieldType,
          yieldVal: input.yieldVal,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: YAS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createYasSchema(resolver.options),
    },
  );
}

function preferredsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_preferreds" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: PreferredsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.preferreds(input.equityTicker, {
          backend: "json",
          fields: input.fields,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: PREFERREDS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createPreferredsSchema(resolver.options),
    },
  );
}

function corporateBondsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_corporate_bonds" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CorporateBondsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.corporateBonds(input.ticker, {
          activeOnly: input.activeOnly,
          backend: "json",
          ccy: input.ccy,
          fields: input.fields,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: CORPORATE_BONDS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createCorporateBondsSchema(resolver.options),
    },
  );
}

function indexMembersWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_index_members" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: IndexMembersInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.indexMembers(input.index, {
          asof: input.asof,
          backend: "json",
          field: input.field,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: INDEX_MEMBERS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createIndexMembersSchema(resolver.options),
    },
  );
}

function resolveIsinsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_resolve_isins" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ResolveIsinsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.resolveIsins(input.isins, { backend: "json" });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: RESOLVE_ISINS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createResolveIsinsSchema(resolver.options),
    },
  );
}

function resolveVenuesWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_resolve_venues" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ResolveVenuesInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.resolveVenues(input.securities, {
          backend: "json",
          pcsOverrides: input.pcsOverrides,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: RESOLVE_VENUES_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createResolveVenuesSchema(resolver.options),
    },
  );
}

function auctionSnapshotWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_auction_snapshot" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: AuctionSnapshotInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.auctionSnapshot(input.securities, {
          backend: "json",
          fields: input.fields,
          pcsOverrides: input.pcsOverrides,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: AUCTION_SNAPSHOT_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createAuctionSnapshotSchema(resolver.options),
    },
  );
}

function issuerIsinsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_issuer_isins" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: IssuerIsinsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.issuerIsins(input.bondIsins, { backend: "json" });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: ISSUER_ISINS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createIssuerIsinsSchema(resolver.options),
    },
  );
}

function etfHoldingsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_etf_holdings" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: EtfHoldingsInput): Promise<ToolContentAndArtifact> => {
      try {
        const engine = await resolver.getEngine();
        const result = await engine.etfHoldings(input.etfTicker, {
          backend: "json",
          fields: input.fields,
        });
        return resultString(resolver, name, result);
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: ETF_HOLDINGS_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createEtfHoldingsSchema(resolver.options),
    },
  );
}

function streamSnapshotWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_stream_snapshot" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (
      input: StreamSnapshotInput,
      config?: ToolInvocationConfig,
    ): Promise<ToolContentAndArtifact> => {
      const signal = config?.signal;
      try {
        const engine = await resolver.getEngine();
        // Connecting may have outlived the caller; never open a doomed subscription.
        signal?.throwIfAborted();
        const subscription = await engine.stream(input.tickers, input.fields, streamOptions(input));
        const snapshot = await collectSnapshot(
          subscription,
          input,
          signal,
          Math.max(resolver.options.maxRows, resolver.options.maxContentRows),
          resolver.options.maxResultNodes,
        );
        return createToolResult(name, snapshot.result, {
          ...resolver.options,
          materializedNodes: snapshot.materializedRows,
        });
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: STREAM_SNAPSHOT_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createStreamSnapshotSchema(resolver.options),
    },
  );
}

function mktbarSnapshotWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_mktbar_snapshot" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (
      input: MktbarSnapshotInput,
      config?: ToolInvocationConfig,
    ): Promise<ToolContentAndArtifact> => {
      const signal = config?.signal;
      try {
        const engine = await resolver.getEngine();
        signal?.throwIfAborted();
        const subscription = await engine.mktbar(input.ticker, singleTickerStreamOptions(input));
        const snapshot = await collectSnapshot(
          subscription,
          input,
          signal,
          Math.max(resolver.options.maxRows, resolver.options.maxContentRows),
          resolver.options.maxResultNodes,
        );
        return createToolResult(name, snapshot.result, {
          ...resolver.options,
          materializedNodes: snapshot.materializedRows,
        });
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: MKTBAR_SNAPSHOT_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createMktbarSnapshotSchema(resolver.options),
    },
  );
}

function depthSnapshotWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_depth_snapshot" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (
      input: DepthSnapshotInput,
      config?: ToolInvocationConfig,
    ): Promise<ToolContentAndArtifact> => {
      const signal = config?.signal;
      try {
        const engine = await resolver.getEngine();
        signal?.throwIfAborted();
        const subscription = await engine.depth(input.ticker, singleTickerStreamOptions(input));
        const snapshot = await collectSnapshot(
          subscription,
          input,
          signal,
          Math.max(resolver.options.maxRows, resolver.options.maxContentRows),
          resolver.options.maxResultNodes,
        );
        return createToolResult(name, snapshot.result, {
          ...resolver.options,
          materializedNodes: snapshot.materializedRows,
        });
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      description: DEPTH_SNAPSHOT_DESCRIPTION,
      name,
      responseFormat: "content_and_artifact",
      schema: createDepthSnapshotSchema(resolver.options),
    },
  );
}

export function createBdpTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bdpWithResolver(createCoreResolver(options));
}

export function createBdhTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bdhWithResolver(createCoreResolver(options));
}

export function createBdsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bdsWithResolver(createCoreResolver(options));
}

export function createBdibTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bdibWithResolver(createCoreResolver(options));
}

export function createBdtickTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bdtickWithResolver(createCoreResolver(options));
}

export function createCheckEntitlementsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return checkEntitlementsWithResolver(createCoreResolver(options));
}

export function createBqlTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bqlWithResolver(createCoreResolver(options));
}

export function createBsrchTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bsrchWithResolver(createCoreResolver(options));
}

export function createBqrTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bqrWithResolver(createCoreResolver(options));
}

export function createBfldsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return bfldsWithResolver(createCoreResolver(options));
}

export function createBeqsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return beqsWithResolver(createCoreResolver(options));
}

export function createYasTool(options: BloombergToolsOptions = {}): BloombergTool {
  return yasWithResolver(createCoreResolver(options));
}

export function createPreferredsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return preferredsWithResolver(createCoreResolver(options));
}

export function createCorporateBondsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return corporateBondsWithResolver(createCoreResolver(options));
}

export function createIndexMembersTool(options: BloombergToolsOptions = {}): BloombergTool {
  return indexMembersWithResolver(createCoreResolver(options));
}

export function createResolveIsinsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return resolveIsinsWithResolver(createCoreResolver(options));
}

export function createResolveVenuesTool(options: BloombergToolsOptions = {}): BloombergTool {
  return resolveVenuesWithResolver(createCoreResolver(options));
}

export function createAuctionSnapshotTool(options: BloombergToolsOptions = {}): BloombergTool {
  return auctionSnapshotWithResolver(createCoreResolver(options));
}

export function createIssuerIsinsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return issuerIsinsWithResolver(createCoreResolver(options));
}

export function createEtfHoldingsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return etfHoldingsWithResolver(createCoreResolver(options));
}

export function createStreamSnapshotTool(options: BloombergToolsOptions = {}): BloombergTool {
  return streamSnapshotWithResolver(createCoreResolver(options));
}

export function createMktbarSnapshotTool(options: BloombergToolsOptions = {}): BloombergTool {
  return mktbarSnapshotWithResolver(createCoreResolver(options));
}

export function createDepthSnapshotTool(options: BloombergToolsOptions = {}): BloombergTool {
  return depthSnapshotWithResolver(createCoreResolver(options));
}

interface CoreToolDefinition {
  readonly create: ToolCreator;
  readonly name: BloombergToolName;
}

const CORE_TOOL_DEFINITIONS: readonly CoreToolDefinition[] = Object.freeze([
  { create: bdpWithResolver, name: "xbbg_bdp" },
  { create: bdhWithResolver, name: "xbbg_bdh" },
  { create: bdsWithResolver, name: "xbbg_bds" },
  { create: bdibWithResolver, name: "xbbg_bdib" },
  { create: bdtickWithResolver, name: "xbbg_bdtick" },
  { create: checkEntitlementsWithResolver, name: "xbbg_check_entitlements" },
  { create: bqlWithResolver, name: "xbbg_bql" },
  { create: bsrchWithResolver, name: "xbbg_bsrch" },
  { create: bqrWithResolver, name: "xbbg_bqr" },
  { create: bfldsWithResolver, name: "xbbg_bflds" },
  { create: beqsWithResolver, name: "xbbg_beqs" },
  { create: yasWithResolver, name: "xbbg_yas" },
  { create: preferredsWithResolver, name: "xbbg_preferreds" },
  { create: corporateBondsWithResolver, name: "xbbg_corporate_bonds" },
  { create: indexMembersWithResolver, name: "xbbg_index_members" },
  { create: resolveIsinsWithResolver, name: "xbbg_resolve_isins" },
  { create: resolveVenuesWithResolver, name: "xbbg_resolve_venues" },
  { create: auctionSnapshotWithResolver, name: "xbbg_auction_snapshot" },
  { create: issuerIsinsWithResolver, name: "xbbg_issuer_isins" },
  { create: etfHoldingsWithResolver, name: "xbbg_etf_holdings" },
  { create: streamSnapshotWithResolver, name: "xbbg_stream_snapshot" },
  { create: mktbarSnapshotWithResolver, name: "xbbg_mktbar_snapshot" },
  { create: depthSnapshotWithResolver, name: "xbbg_depth_snapshot" },
]);

export function createBloombergToolsForResolver(resolver: CoreResolver): BloombergTool[] {
  return CORE_TOOL_DEFINITIONS.filter(
    (definition) => !isToolDisabled(resolver.options, definition.name),
  ).map((definition) => definition.create(resolver));
}

export function createBloombergTools(options: BloombergToolsOptions = {}): BloombergTool[] {
  return createBloombergToolsForResolver(createCoreResolver(options));
}
