import {
  MAX_BLOOMBERG_EID,
  MAX_EID_SECURITIES,
  MAX_EID_SECURITY_NAME_BYTES,
  MAX_ENTITLEMENT_EIDS,
} from "./_defs_gen";
import {
  addReason,
  consumeVisit,
  defineJsonProperty,
  isPlainObject,
  OMIT,
  ownEnumerableDescriptor,
  type BuiltValue,
  type LimitState,
  type ObjectAccumulator,
} from "./result-types";

interface EntitlementTraversal {
  buildValue(
    value: unknown,
    maximumJsonBytes: number,
    state: LimitState,
    depth: number,
    rowLimit: number,
    useSharedRows: boolean,
  ): BuiltValue | typeof OMIT;
  appendBuiltProperty(
    accumulator: ObjectAccumulator,
    key: string,
    built: BuiltValue,
    maximumJsonBytes: number,
    state: LimitState,
  ): boolean;
}

const EID_SUMMARY_KEY_BYTES = '"eidDataTruncation"'.length;

interface PreparedEidData {
  readonly data: Record<string, unknown>;
  readonly invalidSecurityCount: number;
  readonly scannedSecurityCount: number;
  readonly securityCounts: readonly { originalCount: number; retainedCount: number }[];
  readonly totalEidCount: number | null;
  readonly totalSecurityCount: number | null;
  readonly truncation?: EidDataTruncation;
}

interface EidDataTruncation {
  readonly totalSecurityCount: number | null;
  readonly retainedSecurityCount: number;
  readonly omittedSecurityCount: number | null;
  readonly invalidSecurityCount: number;
  readonly scannedSecurityCount: number;
  readonly totalEidCount: number | null;
  readonly retainedEidCount: number;
  /** Counts align by index with Object.keys(eidData), avoiding duplicate security-name bytes. */
  readonly securityCounts: readonly { originalCount: number | null; retainedCount: number }[];
}

function consumeVisitBefore(state: LimitState, limit: number): boolean {
  if (state.visitedNodes >= limit) {
    addReason(state, "max_result_nodes");
    return false;
  }
  return consumeVisit(state);
}

function utf8ByteLengthAtMost(value: string, maximum: number): number | null {
  let byteLength = 0;
  for (let index = 0; index < value.length;) {
    const code = value.charCodeAt(index);
    let width = 1;
    let bytes: number;
    if (code <= 0x7f) {
      bytes = 1;
    } else if (code <= 0x7ff) {
      bytes = 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes = 4;
        width = 2;
      } else {
        bytes = 3;
      }
    } else {
      bytes = 3;
    }
    byteLength += bytes;
    if (byteLength > maximum) {
      return null;
    }
    index += width;
  }
  return byteLength;
}

function prepareEidData(value: unknown, state: LimitState): PreparedEidData {
  const data = Object.create(null) as Record<string, unknown>;
  const securityCounts: { originalCount: number; retainedCount: number }[] = [];
  let validContainer = false;
  if (typeof value === "object" && value !== null) {
    try {
      validContainer = isPlainObject(value);
    } catch {
      validContainer = false;
    }
  }
  if (!validContainer) {
    addReason(state, "invalid_entitlement_data");
    return {
      data,
      invalidSecurityCount: 1,
      scannedSecurityCount: 1,
      securityCounts,
      totalEidCount: 0,
      totalSecurityCount: 1,
      truncation: {
        invalidSecurityCount: 1,
        omittedSecurityCount: 0,
        retainedEidCount: 0,
        retainedSecurityCount: 0,
        scannedSecurityCount: 1,
        securityCounts,
        totalEidCount: 0,
        totalSecurityCount: 1,
      },
    };
  }
  const eidRecord = value as Record<string, unknown>;

  let complete = true;
  let invalidSecurityCount = 0;
  let retainedEidCount = 0;
  let retainedSecurityCount = 0;
  let retainedSecurityNameBytes = 0;
  let scannedSecurityCount = 0;
  let totalEidCount = 0;
  const remainingNodeBudget = state.limits.maxResultNodes - state.visitedNodes;
  const reservedSummaryNodes = Math.min(16, Math.max(0, remainingNodeBudget - 1));
  const eidVisitLimit =
    state.visitedNodes + Math.max(1, Math.floor((remainingNodeBudget - reservedSummaryNodes) / 2));

  securityLoop: for (const security in eidRecord) {
    if (!Object.hasOwn(eidRecord, security)) {
      continue;
    }
    if (!consumeVisitBefore(state, eidVisitLimit)) {
      complete = false;
      break;
    }
    scannedSecurityCount += 1;
    const descriptor = ownEnumerableDescriptor(eidRecord, security);
    if (descriptor === undefined) {
      continue;
    }
    if (!("value" in descriptor) || !Array.isArray(descriptor.value)) {
      invalidSecurityCount += 1;
      addReason(state, "invalid_entitlement_data");
      continue;
    }
    const eids = descriptor.value as readonly unknown[];
    const remainingNameBytes = MAX_EID_SECURITY_NAME_BYTES - retainedSecurityNameBytes;
    const securityNameBytes = utf8ByteLengthAtMost(security, remainingNameBytes);
    const canRetainSecurity =
      retainedSecurityCount < MAX_EID_SECURITIES && securityNameBytes !== null;
    const remainingEidCapacity = Math.max(0, MAX_ENTITLEMENT_EIDS - retainedEidCount);
    const retained: number[] = [];
    let incomplete = false;
    for (let index = 0; index < eids.length; index += 1) {
      if (!consumeVisitBefore(state, eidVisitLimit)) {
        complete = false;
        incomplete = true;
        break;
      }
      const eidDescriptor = Object.getOwnPropertyDescriptor(eids, String(index));
      const eid: unknown =
        eidDescriptor !== undefined && "value" in eidDescriptor ? eidDescriptor.value : undefined;
      if (
        eidDescriptor === undefined ||
        !("value" in eidDescriptor) ||
        typeof eid !== "number" ||
        !Number.isInteger(eid) ||
        eid <= 0 ||
        eid > MAX_BLOOMBERG_EID
      ) {
        invalidSecurityCount += 1;
        addReason(state, "invalid_entitlement_data");
        continue securityLoop;
      }
      if (canRetainSecurity && retained.length < remainingEidCapacity) {
        retained.push(eid);
      }
    }
    totalEidCount = Math.min(Number.MAX_SAFE_INTEGER, totalEidCount + eids.length);

    if (!canRetainSecurity) {
      addReason(state, "entitlement_limit");
      if (incomplete) {
        break;
      }
      continue;
    }

    retainedEidCount += retained.length;
    retainedSecurityCount += 1;
    retainedSecurityNameBytes += securityNameBytes;
    defineJsonProperty(data, security, retained);
    securityCounts.push({ originalCount: eids.length, retainedCount: retained.length });
    if (!incomplete && retained.length !== eids.length) {
      addReason(state, "entitlement_limit");
    }
    if (incomplete) {
      break;
    }
  }

  const omittedSecurityCount = complete
    ? scannedSecurityCount - retainedSecurityCount - invalidSecurityCount
    : null;
  const wasTruncated =
    !complete ||
    invalidSecurityCount > 0 ||
    omittedSecurityCount !== 0 ||
    retainedEidCount !== totalEidCount;
  if (!wasTruncated) {
    return {
      data,
      invalidSecurityCount,
      scannedSecurityCount,
      securityCounts,
      totalEidCount,
      totalSecurityCount: scannedSecurityCount,
    };
  }
  return {
    data,
    invalidSecurityCount,
    scannedSecurityCount,
    securityCounts,
    totalEidCount: complete ? totalEidCount : null,
    totalSecurityCount: complete ? scannedSecurityCount : null,
    truncation: {
      invalidSecurityCount,
      omittedSecurityCount,
      retainedEidCount,
      retainedSecurityCount,
      scannedSecurityCount,
      securityCounts,
      totalEidCount: complete ? totalEidCount : null,
      totalSecurityCount: complete ? scannedSecurityCount : null,
    },
  };
}

function eidSummaryRecord(source: object): Record<string, unknown> | undefined {
  const descriptor = ownEnumerableDescriptor(source, "eidDataTruncation");
  if (
    descriptor === undefined ||
    !("value" in descriptor) ||
    typeof descriptor.value !== "object" ||
    descriptor.value === null
  ) {
    return undefined;
  }
  return descriptor.value as Record<string, unknown>;
}

function eidSummaryCount(
  summary: Record<string, unknown> | undefined,
  key: string,
): number | null | undefined {
  if (summary === undefined) {
    return undefined;
  }
  const descriptor = ownEnumerableDescriptor(summary, key);
  if (descriptor === undefined || !("value" in descriptor)) {
    return undefined;
  }
  const value: unknown = descriptor.value;
  return value === null || (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    ? value
    : undefined;
}

function eidSummarySecurityCounts(
  summary: Record<string, unknown> | undefined,
): readonly { originalCount: number | null; retainedCount: number }[] {
  if (summary === undefined) {
    return [];
  }
  const descriptor = ownEnumerableDescriptor(summary, "securityCounts");
  if (descriptor === undefined || !("value" in descriptor) || !Array.isArray(descriptor.value)) {
    return [];
  }
  const counts: { originalCount: number | null; retainedCount: number }[] = [];
  for (let index = 0; index < descriptor.value.length; index += 1) {
    const entryDescriptor = Object.getOwnPropertyDescriptor(descriptor.value, String(index));
    if (
      entryDescriptor === undefined ||
      !("value" in entryDescriptor) ||
      typeof entryDescriptor.value !== "object" ||
      entryDescriptor.value === null
    ) {
      break;
    }
    const originalCount = eidSummaryCount(
      entryDescriptor.value as Record<string, unknown>,
      "originalCount",
    );
    const retainedCount = eidSummaryCount(
      entryDescriptor.value as Record<string, unknown>,
      "retainedCount",
    );
    if (
      originalCount === undefined ||
      (originalCount !== null && typeof originalCount !== "number") ||
      typeof retainedCount !== "number"
    ) {
      break;
    }
    counts.push({ originalCount, retainedCount });
  }
  return counts;
}

function emittedEidTruncation(
  source: object,
  emittedValue: unknown,
  prepared: PreparedEidData,
): EidDataTruncation | undefined {
  const emitted =
    typeof emittedValue === "object" && emittedValue !== null
      ? (emittedValue as Record<string, unknown>)
      : (Object.create(null) as Record<string, unknown>);
  const prior = eidSummaryRecord(source);
  const priorSecurityCounts = eidSummarySecurityCounts(prior);
  const securityCounts: { originalCount: number | null; retainedCount: number }[] = [];
  let retainedEidCount = 0;
  let retainedSecurityCount = 0;
  for (const security in emitted) {
    if (!Object.hasOwn(emitted, security)) {
      continue;
    }
    const descriptor = ownEnumerableDescriptor(emitted, security);
    if (descriptor === undefined || !("value" in descriptor) || !Array.isArray(descriptor.value)) {
      continue;
    }
    const retainedCount = descriptor.value.length;
    const preparedCount = prepared.securityCounts[retainedSecurityCount];
    const priorCount = priorSecurityCounts[retainedSecurityCount];
    securityCounts.push({
      originalCount:
        prior === undefined
          ? (preparedCount?.originalCount ?? retainedCount)
          : (priorCount?.originalCount ?? null),
      retainedCount,
    });
    retainedEidCount = Math.min(Number.MAX_SAFE_INTEGER, retainedEidCount + retainedCount);
    retainedSecurityCount += 1;
  }

  const priorTotalEidCount = eidSummaryCount(prior, "totalEidCount");
  const priorTotalSecurityCount = eidSummaryCount(prior, "totalSecurityCount");
  const totalEidCount =
    prior === undefined
      ? prepared.totalEidCount
      : priorTotalEidCount === undefined
        ? null
        : priorTotalEidCount;
  const totalSecurityCount =
    prior === undefined
      ? prepared.totalSecurityCount
      : priorTotalSecurityCount === undefined
        ? null
        : priorTotalSecurityCount;
  const priorInvalidSecurityCount = eidSummaryCount(prior, "invalidSecurityCount");
  const invalidSecurityCount =
    typeof priorInvalidSecurityCount === "number"
      ? priorInvalidSecurityCount
      : prepared.invalidSecurityCount;
  const priorScannedSecurityCount = eidSummaryCount(prior, "scannedSecurityCount");
  const scannedSecurityCount =
    typeof priorScannedSecurityCount === "number"
      ? priorScannedSecurityCount
      : prepared.scannedSecurityCount;
  const omittedSecurityCount =
    totalSecurityCount === null
      ? null
      : Math.max(0, totalSecurityCount - retainedSecurityCount - invalidSecurityCount);
  const truncated =
    prior !== undefined ||
    prepared.truncation !== undefined ||
    totalEidCount === null ||
    totalSecurityCount === null ||
    retainedEidCount !== totalEidCount ||
    retainedSecurityCount + invalidSecurityCount !== totalSecurityCount;
  if (!truncated) {
    return undefined;
  }
  return {
    retainedEidCount,
    totalEidCount,
    retainedSecurityCount,
    totalSecurityCount,
    securityCounts,
    invalidSecurityCount,
    omittedSecurityCount,
    scannedSecurityCount,
  };
}

// Entitlement validation and emission consume the caller's traversal state;
// neither the EID prefix nor its summary gets a separate inspection budget.
export function buildEntitlementProperty(
  source: object,
  rawValue: unknown,
  accumulator: ObjectAccumulator,
  maximumJsonBytes: number,
  childBudget: number,
  state: LimitState,
  depth: number,
  traversal: EntitlementTraversal,
): boolean {
  const prepared = prepareEidData(rawValue, state);
  const built = traversal.buildValue(
    prepared.data,
    Math.max(2, Math.floor(childBudget / 3)),
    state,
    depth + 1,
    MAX_ENTITLEMENT_EIDS,
    false,
  );
  if (
    built === OMIT ||
    !traversal.appendBuiltProperty(accumulator, "eidData", built, maximumJsonBytes, state)
  ) {
    return false;
  }
  const emittedTruncation = emittedEidTruncation(source, built.value, prepared);
  if (emittedTruncation !== undefined) {
    const summaryCommaBytes = accumulator.propertyCount === 0 ? 0 : 1;
    const summaryBudget =
      maximumJsonBytes - accumulator.byteLength - summaryCommaBytes - EID_SUMMARY_KEY_BYTES - 1;
    const summary = traversal.buildValue(
      emittedTruncation,
      summaryBudget,
      state,
      depth + 1,
      MAX_EID_SECURITIES,
      false,
    );
    if (
      summary === OMIT ||
      !traversal.appendBuiltProperty(
        accumulator,
        "eidDataTruncation",
        summary,
        maximumJsonBytes,
        state,
      )
    ) {
      state.omittedPropertiesAtLeast += 1;
    }
  }
  return true;
}
