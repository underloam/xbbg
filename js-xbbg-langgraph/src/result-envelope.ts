import {
  CONTENT_ENVELOPE_RESERVE_BYTES,
  MIN_TOOL_RESULT_BYTES,
  MIN_TOOL_RESULT_NODES,
  RESULT_ENVELOPE_RESERVE_BYTES,
  TRUNCATION_REASON_ORDER,
  type BloombergToolName,
  type ResultTruncationReason,
} from "./_defs_gen";
import { projectResult, rowCountOf, validateResultLimits } from "./result-limits";
import {
  ownEnumerableDescriptor,
  type LimitResult,
  type ResultLimitOptions,
  type ResultTruncationSummary,
} from "./result-types";

export interface ToolResultLimitOptions extends ResultLimitOptions {
  readonly maxContentBytes: number;
  readonly maxContentRows: number;
}

interface ToolResultWorkBudgetOptions {
  readonly materializedNodes?: number;
}

type ToolResultBuildOptions = ToolResultLimitOptions & ToolResultWorkBudgetOptions;

export interface ToolEnvelope {
  readonly tool: BloombergToolName;
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly truncation?: ResultTruncationSummary;
  readonly hasErrors?: true;
  readonly data: unknown;
}

export type ToolContentAndArtifact = [string, ToolEnvelope];

const UTF8_ENCODER = new TextEncoder();

function projectionBudget(totalBytes: number, preferredReserveBytes: number): number {
  const reserve = Math.min(preferredReserveBytes, Math.floor(totalBytes / 2));
  return Math.max(4, totalBytes - reserve);
}

function artifactEnvelope(
  tool: BloombergToolName,
  limited: LimitResult,
  truncation: ResultTruncationSummary | undefined,
): ToolEnvelope {
  return {
    tool,
    rowCount: limited.rowCount,
    truncated: limited.truncated,
    ...(truncation === undefined ? {} : { truncation }),
    ...(limited.hasErrors ? { hasErrors: true as const } : {}),
    data: limited.value,
  };
}

function boundedJsonByteLength(value: unknown): number {
  return UTF8_ENCODER.encode(JSON.stringify(value)).byteLength;
}

function mergeLimited(first: LimitResult, second: LimitResult): LimitResult {
  const reasons = new Set<ResultTruncationReason>(first.truncation?.reasons ?? []);
  for (const reason of second.truncation?.reasons ?? []) {
    reasons.add(reason);
  }
  const retainedNodes = second.truncation?.retainedNodes ?? first.truncation?.retainedNodes;
  const omittedPropertiesAtLeast =
    (first.truncation?.omittedPropertiesAtLeast ?? 0) +
    (second.truncation?.omittedPropertiesAtLeast ?? 0);
  const omittedRows = (first.truncation?.omittedRows ?? 0) + (second.truncation?.omittedRows ?? 0);
  const truncation: ResultTruncationSummary | undefined =
    reasons.size === 0
      ? undefined
      : {
          reasons: TRUNCATION_REASON_ORDER.filter((reason) => reasons.has(reason)),
          inspectedNodes: first.inspectedNodes + second.inspectedNodes,
          ...(retainedNodes === undefined ? {} : { retainedNodes }),
          ...(omittedPropertiesAtLeast === 0 ? {} : { omittedPropertiesAtLeast }),
          ...(omittedRows === 0 ? {} : { omittedRows }),
        };
  return {
    byteLength: second.byteLength,
    maximumArrayRows: second.maximumArrayRows,
    retainedRows: second.retainedRows,
    errorDiagnostics:
      second.errorDiagnostics.length === 0 ? first.errorDiagnostics : second.errorDiagnostics,
    hasErrors: first.hasErrors || second.hasErrors,
    inspectedNodes: first.inspectedNodes + second.inspectedNodes,
    rowCount: first.rowCount,
    truncated: truncation !== undefined,
    ...(truncation === undefined ? {} : { truncation }),
    value: second.value,
  };
}

function fitArtifact(
  tool: BloombergToolName,
  limited: LimitResult,
  maxResultBytes: number,
): ToolEnvelope {
  let envelope = artifactEnvelope(tool, limited, limited.truncation);
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  const fitLimited = limited.truncated ? limited : { ...limited, truncated: true };
  const reasons = limited.truncation?.reasons ?? ["max_result_bytes"];
  envelope = artifactEnvelope(tool, fitLimited, { reasons });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  const primaryReason =
    reasons.find((reason) => reason !== "max_result_bytes") ?? "max_result_bytes";
  const compactReasons: ResultTruncationReason[] =
    primaryReason === "max_result_bytes" ? [primaryReason] : [primaryReason, "max_result_bytes"];
  envelope = artifactEnvelope(tool, fitLimited, { reasons: compactReasons });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  envelope = artifactEnvelope(tool, fitLimited, {
    reasons: ["max_result_bytes"],
  });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  return {
    tool,
    rowCount: limited.rowCount,
    truncated: true,
    truncation: { reasons: ["max_result_bytes"] },
    ...(limited.hasErrors ? { hasErrors: true as const } : {}),
    data: null,
  };
}

function rowText(rowCount: number | null): string {
  return rowCount === null
    ? "row count unknown"
    : `${String(rowCount)} row${rowCount === 1 ? "" : "s"}`;
}

function summarizeEnvelope(
  envelope: ToolEnvelope,
  contentTruncation: ResultTruncationSummary | undefined,
): string {
  const notes: string[] = [];
  if (
    envelope.rowCount === 0 ||
    (!envelope.truncated && (envelope.data === null || envelope.data === undefined))
  ) {
    notes.push(
      "empty result; verify identifiers, fields, and date range before concluding no data exists",
    );
  }
  if (envelope.hasErrors === true) {
    notes.push("Bloomberg error diagnostics included in preview");
  }
  const artifactReasons = envelope.truncation?.reasons.join(",") ?? "none";
  const contentReasons = contentTruncation?.reasons.join(",") ?? "none";
  const noteText = notes.length === 0 ? "" : `; ${notes.join("; ")}`;
  return `${envelope.tool}: ${rowText(envelope.rowCount)}; artifactTruncated=${String(envelope.truncated)}; contentTruncated=${String(contentTruncation !== undefined)}; artifactReasons=${artifactReasons}; contentReasons=${contentReasons}${noteText}`;
}

function contentPayload(envelope: ToolEnvelope, limited: LimitResult): Record<string, unknown> {
  const projected = limited.value as Record<string, unknown> | null;
  return {
    tool: envelope.tool,
    rowCount: envelope.rowCount,
    truncated: envelope.truncated,
    contentTruncated: limited.truncated,
    ...(projected ?? { data: null }),
  };
}

function formatToolContent(
  envelope: ToolEnvelope,
  preview: LimitResult,
  maxContentBytes: number,
): string {
  const firstPreviewValue = preview.value;
  const summary = summarizeEnvelope(envelope, preview.truncation);
  const payload = contentPayload(envelope, preview);
  const content = `${summary}\n${JSON.stringify(payload)}`;
  if (UTF8_ENCODER.encode(content).byteLength <= maxContentBytes) {
    return content;
  }

  const compactSummary = `${envelope.tool}: ${rowText(envelope.rowCount)}; artifactTruncated=${String(envelope.truncated)}; contentTruncated=${String(preview.truncated)}; hasErrors=${String(envelope.hasErrors === true)}`;
  const compactContent = `${compactSummary}\n${JSON.stringify(firstPreviewValue)}`;
  if (UTF8_ENCODER.encode(compactContent).byteLength <= maxContentBytes) {
    return compactContent;
  }
  return compactSummary;
}

function withAggregateInspection(limited: LimitResult, inspectedNodes: number): LimitResult {
  return {
    ...limited,
    inspectedNodes,
    ...(limited.truncation === undefined
      ? {}
      : {
          truncation: {
            ...limited.truncation,
            inspectedNodes,
          },
        }),
  };
}

function reusedProjection(value: unknown, canonical: LimitResult): LimitResult {
  return {
    byteLength: boundedJsonByteLength(value),
    errorDiagnostics: [],
    hasErrors: false,
    inspectedNodes: 0,
    maximumArrayRows: canonical.maximumArrayRows,
    retainedRows: canonical.retainedRows,
    rowCount: rowCountOf(value),
    truncated: false,
    value,
  };
}

function canReuseProjection(
  canonical: LimitResult,
  byteLength: number,
  maxBytes: number,
  maxRows: number,
): boolean {
  return byteLength <= maxBytes && canonical.maximumArrayRows <= maxRows;
}

export function createToolResult(
  tool: BloombergToolName,
  value: unknown,
  limits: ToolResultBuildOptions,
): ToolContentAndArtifact {
  const initialInspectedNodes = limits.materializedNodes ?? 0;
  if (limits.maxResultBytes < MIN_TOOL_RESULT_BYTES) {
    throw new RangeError(
      `maxResultBytes must be at least ${String(MIN_TOOL_RESULT_BYTES)}; got ${String(limits.maxResultBytes)}`,
    );
  }
  if (limits.maxContentBytes < MIN_TOOL_RESULT_BYTES) {
    throw new RangeError(
      `maxContentBytes must be at least ${String(MIN_TOOL_RESULT_BYTES)}; got ${String(limits.maxContentBytes)}`,
    );
  }
  if (limits.maxResultNodes < MIN_TOOL_RESULT_NODES) {
    throw new RangeError(
      `maxResultNodes must be at least ${String(MIN_TOOL_RESULT_NODES)}; got ${String(limits.maxResultNodes)}`,
    );
  }

  validateResultLimits(limits);
  if (
    !Number.isSafeInteger(initialInspectedNodes) ||
    initialInspectedNodes < 0 ||
    initialInspectedNodes > limits.maxResultNodes
  ) {
    throw new RangeError(
      `materializedNodes must be between 0 and maxResultNodes; got ${String(initialInspectedNodes)}`,
    );
  }
  const availableNodeBudget = limits.maxResultNodes - initialInspectedNodes;
  const hasEidMetadata =
    typeof value === "object" &&
    value !== null &&
    ownEnumerableDescriptor(value, "eidData") !== undefined;
  const canonicalNodeBudget =
    availableNodeBudget === 0
      ? 0
      : Math.max(
          1,
          hasEidMetadata
            ? availableNodeBudget - Math.ceil(availableNodeBudget / 5)
            : Math.floor(availableNodeBudget / 3),
        );
  const canonical = projectResult(
    value,
    {
      maxResultBytes: Math.max(limits.maxResultBytes, limits.maxContentBytes),
      maxResultNodes: canonicalNodeBudget,
      maxRows: Math.max(limits.maxRows, limits.maxContentRows),
      maxStringChars: limits.maxStringChars,
    },
    false,
  );

  const artifactDataBudget = projectionBudget(limits.maxResultBytes, RESULT_ENVELOPE_RESERVE_BYTES);
  const contentDataBudget = projectionBudget(
    limits.maxContentBytes,
    CONTENT_ENVELOPE_RESERVE_BYTES,
  );
  const contentSource = Object.create(null) as Record<string, unknown>;
  if (canonical.errorDiagnostics.length > 0) {
    contentSource.diagnostics = canonical.errorDiagnostics;
  }
  contentSource.data = canonical.value;
  const contentSourceBytes = boundedJsonByteLength(contentSource);
  const reuseArtifact = canReuseProjection(
    canonical,
    canonical.byteLength,
    artifactDataBudget,
    limits.maxRows,
  );
  const reuseContent = canReuseProjection(
    canonical,
    contentSourceBytes,
    contentDataBudget,
    limits.maxContentRows,
  );
  const remainingNodeBudget = Math.max(0, availableNodeBudget - canonical.inspectedNodes);
  const artifactNeedsNodes = !reuseArtifact;
  const contentNeedsNodes = !reuseContent;
  const artifactNodeBudget = artifactNeedsNodes
    ? contentNeedsNodes
      ? Math.floor(remainingNodeBudget / 2)
      : remainingNodeBudget
    : 0;
  const contentNodeBudget = contentNeedsNodes ? remainingNodeBudget - artifactNodeBudget : 0;
  const artifactProjection = reuseArtifact
    ? reusedProjection(canonical.value, canonical)
    : projectResult(
        canonical.value,
        {
          maxResultBytes: artifactDataBudget,
          maxResultNodes: artifactNodeBudget,
          maxRows: limits.maxRows,
          maxStringChars: Number.MAX_SAFE_INTEGER,
        },
        false,
      );
  const contentProjection = reuseContent
    ? reusedProjection(contentSource, canonical)
    : projectResult(
        contentSource,
        {
          maxResultBytes: contentDataBudget,
          maxResultNodes: contentNodeBudget,
          maxRows: limits.maxContentRows,
          maxStringChars: Number.MAX_SAFE_INTEGER,
        },
        true,
      );

  const inspectedNodes =
    initialInspectedNodes +
    canonical.inspectedNodes +
    artifactProjection.inspectedNodes +
    contentProjection.inspectedNodes;
  const artifactResult = withAggregateInspection(
    mergeLimited(canonical, artifactProjection),
    inspectedNodes,
  );
  const contentResult = withAggregateInspection(
    mergeLimited(canonical, contentProjection),
    inspectedNodes,
  );
  const envelope = fitArtifact(tool, artifactResult, limits.maxResultBytes);
  return [formatToolContent(envelope, contentResult, limits.maxContentBytes), envelope];
}

export function throwWithToolContext(tool: BloombergToolName, error: unknown): never {
  const prefix = `${tool} failed`;
  if (error instanceof Error) {
    if (error.message.startsWith(prefix)) {
      throw error;
    }
    // Never mutate the original: a memoized connect rejection delivers the
    // same Error instance to every concurrently pending tool call.
    const wrapped = new Error(`${prefix}: ${error.message}`, { cause: error });
    wrapped.name = error.name;
    throw wrapped;
  }
  throw new Error(`${prefix}: ${String(error)}`);
}
