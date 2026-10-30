import { MAX_RESULT_DEPTH } from "./_defs_gen";
import {
  addReason,
  consumeVisit,
  isPlainObject,
  OMIT,
  ownEnumerableDescriptor,
  type BuiltValue,
  type LimitState,
  type ObjectAccumulator,
} from "./result-types";

const FIELD_EXCEPTIONS_KEY = "xbbg.field_exceptions";
const DIAGNOSTIC_ROW_LIMIT = Number.MAX_SAFE_INTEGER;

interface DiagnosticTraversal {
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
  jsonStringByteLength(value: string, stopAfter?: number): number;
}

function invalidMetadata(detail: string): TypeError {
  return new TypeError(`Invalid ${FIELD_EXCEPTIONS_KEY} metadata: ${detail}`);
}

/** Read only data descriptors: a core convenience getter may expand millions of occurrences. */
export function rawFieldExceptionsMetadata(source: object): string | undefined {
  const metadata = ownEnumerableDescriptor(source, "metadata");
  if (metadata === undefined || !("value" in metadata) || !isPlainObject(metadata.value)) {
    return undefined;
  }
  const raw = ownEnumerableDescriptor(metadata.value, FIELD_EXCEPTIONS_KEY);
  if (raw === undefined) {
    return undefined;
  }
  if (!("value" in raw) || typeof raw.value !== "string") {
    throw invalidMetadata("expected a JSON string");
  }
  return raw.value;
}

interface DiagnosticException {
  field?: string;
  category?: string;
  code?: number | string;
  subcategory?: string;
  message?: string;
}

function isException(value: unknown, compact: boolean): value is DiagnosticException {
  if (!isPlainObject(value)) {
    return false;
  }
  const { field, category, code, subcategory, message } = value;
  if (compact) {
    return (
      typeof field === "string" &&
      typeof category === "string" &&
      typeof code === "number" &&
      Number.isInteger(code) &&
      code >= -2_147_483_648 &&
      code <= 2_147_483_647 &&
      typeof subcategory === "string" &&
      typeof message === "string"
    );
  }
  return (
    (field === undefined || typeof field === "string") &&
    (category === undefined || typeof category === "string") &&
    (code === undefined || typeof code === "number" || typeof code === "string") &&
    (subcategory === undefined || typeof subcategory === "string") &&
    (message === undefined || typeof message === "string")
  );
}

function exceptionAt(
  occurrence: unknown,
  entries: readonly unknown[] | undefined,
): DiagnosticException {
  let value = occurrence;
  if (entries !== undefined) {
    if (
      typeof occurrence !== "number" ||
      !Number.isSafeInteger(occurrence) ||
      occurrence < 0 ||
      occurrence >= entries.length
    ) {
      throw invalidMetadata("invalid dictionary index");
    }
    value = entries[occurrence];
  }
  if (!isException(value, entries !== undefined)) {
    throw invalidMetadata("malformed exception entry");
  }
  return value;
}

function admitRawMetadata(raw: string, state: LimitState, traversal: DiagnosticTraversal): boolean {
  const byteLimit = state.limits.maxResultBytes;
  const bytesFit =
    raw.length <= byteLimit && traversal.jsonStringByteLength(raw, byteLimit) <= byteLimit;
  const nodesFit = raw.length <= state.limits.maxResultNodes - state.visitedNodes;
  if (!bytesFit) {
    addReason(state, "max_result_bytes");
  }
  if (!nodesFit) {
    addReason(state, "max_result_nodes");
  }
  if (!bytesFit || !nodesFit) {
    // The uninspected value may carry errors; do not silently claim success.
    state.hasErrors = true;
    state.omittedPropertiesAtLeast += 1;
    return false;
  }
  // Input characters conservatively bound parsed nodes and full validation work.
  state.visitedNodes += raw.length;
  return true;
}

function validateOccurrences(
  securities: Record<string, unknown>,
  entries: readonly unknown[] | undefined,
): boolean {
  if (entries !== undefined && !entries.every((entry) => isException(entry, true))) {
    throw invalidMetadata("malformed exception entry");
  }
  let hasOccurrences = false;
  for (const security in securities) {
    if (!Object.hasOwn(securities, security)) {
      continue;
    }
    const occurrences = securities[security];
    if (!Array.isArray(occurrences)) {
      throw invalidMetadata("expected per-security diagnostic arrays");
    }
    for (const occurrence of occurrences as readonly unknown[]) {
      exceptionAt(occurrence, entries);
    }
    hasOccurrences ||= occurrences.length > 0;
  }
  return hasOccurrences;
}

function buildExceptionArray(
  occurrences: readonly unknown[],
  entries: readonly unknown[] | undefined,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  traversal: DiagnosticTraversal,
): BuiltValue | typeof OMIT {
  if (!consumeVisit(state)) {
    return OMIT;
  }
  if (depth > MAX_RESULT_DEPTH) {
    addReason(state, "max_result_depth");
    return OMIT;
  }
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.retainedNodes += 1;
  const value: unknown[] = [];
  let byteLength = 2;
  for (const occurrence of occurrences) {
    if (state.visitedNodes >= state.limits.maxResultNodes) {
      addReason(state, "max_result_nodes");
      break;
    }
    if (depth + 2 > MAX_RESULT_DEPTH) {
      addReason(state, "max_result_depth");
      break;
    }
    const commaBytes = value.length === 0 ? 0 : 1;
    const childBudget = maximumJsonBytes - byteLength - commaBytes;
    if (childBudget < 2) {
      addReason(state, "max_result_bytes");
      break;
    }
    const entry = exceptionAt(occurrence, entries);
    const retainedNodesBefore = state.retainedNodes;
    const omittedPropertiesBefore = state.omittedPropertiesAtLeast;
    const built = traversal.buildValue(
      entry,
      childBudget,
      state,
      depth + 1,
      DIAGNOSTIC_ROW_LIMIT,
      false,
    );
    if (
      built === OMIT ||
      !isPlainObject(built.value) ||
      state.omittedPropertiesAtLeast > omittedPropertiesBefore
    ) {
      // Do not turn a partial object into an apparently complete diagnostic occurrence.
      state.retainedNodes = retainedNodesBefore;
      break;
    }
    value.push(built.value);
    byteLength += commaBytes + built.byteLength;
  }
  return { byteLength, value };
}

/** Project expanded records directly from raw indices, never constructing an expanded input map. */
export function buildRawFieldExceptions(
  raw: string,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  traversal: DiagnosticTraversal,
): BuiltValue | typeof OMIT {
  if (!consumeVisit(state)) {
    state.hasErrors = true;
    state.omittedPropertiesAtLeast += 1;
    return OMIT;
  }
  if (depth > MAX_RESULT_DEPTH) {
    addReason(state, "max_result_depth");
    state.hasErrors = true;
    state.omittedPropertiesAtLeast += 1;
    return OMIT;
  }
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    state.hasErrors = true;
    state.omittedPropertiesAtLeast += 1;
    return OMIT;
  }
  if (!admitRawMetadata(raw, state, traversal)) {
    return OMIT;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new TypeError(`Invalid ${FIELD_EXCEPTIONS_KEY} metadata: expected valid JSON`, { cause });
  }
  if (!isPlainObject(parsed)) {
    throw invalidMetadata("expected an object");
  }
  let entries: readonly unknown[] | undefined;
  let securities = parsed;
  if (Object.hasOwn(parsed, "entries") && Object.hasOwn(parsed, "securities")) {
    if (
      Object.keys(parsed).length !== 3 ||
      parsed.version !== 1 ||
      !Array.isArray(parsed.entries) ||
      !isPlainObject(parsed.securities)
    ) {
      throw invalidMetadata("malformed or unsupported compact envelope");
    }
    entries = parsed.entries as readonly unknown[];
    securities = parsed.securities;
  }
  const hasOccurrences = validateOccurrences(securities, entries);
  state.hasErrors ||= hasOccurrences;
  state.retainedNodes += 1;
  const accumulator: ObjectAccumulator = {
    byteLength: 2,
    propertyCount: 0,
    value: Object.create(null) as Record<string, unknown>,
  };
  for (const security in securities) {
    if (!Object.hasOwn(securities, security)) {
      continue;
    }
    const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
    const maximumKeyBytes = maximumJsonBytes - accumulator.byteLength - commaBytes - 3;
    if (maximumKeyBytes < 2) {
      addReason(state, "max_result_bytes");
      state.omittedPropertiesAtLeast += 1;
      break;
    }
    const keyBytes = traversal.jsonStringByteLength(security, maximumKeyBytes);
    if (keyBytes > maximumKeyBytes) {
      addReason(state, "max_result_bytes");
      state.omittedPropertiesAtLeast += 1;
      break;
    }
    const occurrences = securities[security];
    if (!Array.isArray(occurrences)) {
      throw invalidMetadata("expected per-security diagnostic arrays");
    }
    const childBudget = maximumJsonBytes - accumulator.byteLength - commaBytes - keyBytes - 1;
    const built = buildExceptionArray(
      occurrences as readonly unknown[],
      entries,
      childBudget,
      state,
      depth + 1,
      traversal,
    );
    if (
      built === OMIT ||
      !traversal.appendBuiltProperty(accumulator, security, built, maximumJsonBytes, state)
    ) {
      state.omittedPropertiesAtLeast += 1;
      break;
    }
  }
  return { byteLength: accumulator.byteLength, value: accumulator.value };
}
