import {
  ERROR_SHAPE_KEYS,
  MAX_ERROR_DIAGNOSTICS,
  MAX_RESULT_DEPTH,
  PRIORITY_KEYS,
  TRUNCATION_REASON_ORDER,
  type ResultTruncationReason,
} from "./_defs_gen";
import { buildEntitlementProperty } from "./result-entitlements";
import {
  addReason,
  consumeVisit,
  defineJsonProperty,
  isPlainObject,
  OMIT,
  ownEnumerableDescriptor,
  type BuiltValue,
  type LimitResult,
  type LimitState,
  type ObjectAccumulator,
  type ResultLimitOptions,
  type ResultTruncationSummary,
} from "./result-types";

const ERROR_KEYS = new Set<string>(ERROR_SHAPE_KEYS);
const ENTITLEMENT_TRAVERSAL = { buildValue, appendBuiltProperty };

function jsonStringUnit(
  value: string,
  index: number,
): { readonly bytes: number; readonly width: number } {
  const code = value.charCodeAt(index);
  if (
    code === 0x22 ||
    code === 0x5c ||
    code === 0x08 ||
    code === 0x0c ||
    code === 0x0a ||
    code === 0x0d ||
    code === 0x09
  ) {
    return { bytes: 2, width: 1 };
  }
  if (code <= 0x1f) {
    return { bytes: 6, width: 1 };
  }
  if (code <= 0x7f) {
    return { bytes: 1, width: 1 };
  }
  if (code <= 0x7ff) {
    return { bytes: 2, width: 1 };
  }
  if (code >= 0xd800 && code <= 0xdbff) {
    const next = value.charCodeAt(index + 1);
    if (next >= 0xdc00 && next <= 0xdfff) {
      return { bytes: 4, width: 2 };
    }
    return { bytes: 6, width: 1 };
  }
  if (code >= 0xdc00 && code <= 0xdfff) {
    return { bytes: 6, width: 1 };
  }
  return { bytes: 3, width: 1 };
}

function jsonStringByteLength(value: string, stopAfter = Number.MAX_SAFE_INTEGER): number {
  let byteLength = 2;
  for (let index = 0; index < value.length;) {
    const unit = jsonStringUnit(value, index);
    byteLength += unit.bytes;
    if (byteLength > stopAfter) {
      return stopAfter + 1;
    }
    index += unit.width;
  }
  return byteLength;
}

function safePrefixEnd(value: string, requestedEnd: number): number {
  if (
    requestedEnd > 0 &&
    requestedEnd < value.length &&
    value.charCodeAt(requestedEnd - 1) >= 0xd800 &&
    value.charCodeAt(requestedEnd - 1) <= 0xdbff &&
    value.charCodeAt(requestedEnd) >= 0xdc00 &&
    value.charCodeAt(requestedEnd) <= 0xdfff
  ) {
    return requestedEnd - 1;
  }
  return requestedEnd;
}

function fitString(
  value: string,
  maximumJsonBytes: number,
  state: LimitState,
): BuiltValue | typeof OMIT {
  const charLimit = safePrefixEnd(value, Math.min(value.length, state.limits.maxStringChars));
  if (charLimit < value.length) {
    addReason(state, "max_string_chars");
  }
  const suffix = `…[truncated ${value.length - charLimit} chars]`;
  const characterLimited =
    charLimit === value.length ? value : `${value.slice(0, charLimit)}${suffix}`;
  const characterLimitedBytes = jsonStringByteLength(characterLimited, maximumJsonBytes);
  if (characterLimitedBytes <= maximumJsonBytes) {
    state.retainedNodes += 1;
    return { byteLength: characterLimitedBytes, value: characterLimited };
  }

  addReason(state, "max_result_bytes");
  const markerOnly = `…[truncated ${value.length} chars]`;
  const markerOnlyBytes = jsonStringByteLength(markerOnly, maximumJsonBytes);
  if (markerOnlyBytes > maximumJsonBytes) {
    if (maximumJsonBytes < 2) {
      return OMIT;
    }
    state.retainedNodes += 1;
    return { byteLength: 2, value: "" };
  }

  // Sixty-four bytes safely covers the fixed marker plus every possible JS
  // string-length digit. The scan therefore stops at the configured byte
  // budget instead of walking a multi-megabyte string just to cut it later.
  const prefixBudget = Math.max(0, maximumJsonBytes - 2 - 64);
  let prefixBytes = 0;
  let prefixEnd = 0;
  while (prefixEnd < charLimit) {
    const unit = jsonStringUnit(value, prefixEnd);
    if (prefixBytes + unit.bytes > prefixBudget) {
      break;
    }
    prefixBytes += unit.bytes;
    prefixEnd += unit.width;
  }
  const fittedSuffix = `…[truncated ${value.length - prefixEnd} chars]`;
  const fitted = `${value.slice(0, prefixEnd)}${fittedSuffix}`;
  const fittedBytes = jsonStringByteLength(fitted, maximumJsonBytes);
  if (fittedBytes > maximumJsonBytes) {
    state.retainedNodes += 1;
    return { byteLength: markerOnlyBytes, value: markerOnly };
  }
  state.retainedNodes += 1;
  return { byteLength: fittedBytes, value: fitted };
}

function rememberErrorDiagnostic(state: LimitState, key: string, value: unknown): void {
  state.hasErrors = true;
  if (state.diagnostics.length >= MAX_ERROR_DIAGNOSTICS) {
    return;
  }
  const diagnostic = Object.create(null) as Record<string, unknown>;
  defineJsonProperty(diagnostic, key, value);
  state.diagnostics.push(Object.freeze(diagnostic));
}

function hasReportedError(value: unknown): boolean {
  if (value === undefined || value === null || value === false || value === "") {
    return false;
  }
  return !Array.isArray(value) || value.length > 0;
}

function primitiveBuilt(
  value: null | boolean | number,
  maximumJsonBytes: number,
  state: LimitState,
): BuiltValue | typeof OMIT {
  const json = value === null ? "null" : String(value);
  const byteLength = json.length;
  if (byteLength > maximumJsonBytes) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.retainedNodes += 1;
  return { byteLength, value };
}

function buildUnsupported(
  label: string,
  maximumJsonBytes: number,
  state: LimitState,
  reason: ResultTruncationReason = "unsupported_value",
): BuiltValue | typeof OMIT {
  addReason(state, reason);
  return fitString(label, maximumJsonBytes, state);
}

function appendBuiltProperty(
  accumulator: ObjectAccumulator,
  key: string,
  built: BuiltValue,
  maximumJsonBytes: number,
  state: LimitState,
): boolean {
  if (Object.hasOwn(accumulator.value, key)) {
    return true;
  }
  const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
  const availableForKey =
    maximumJsonBytes - accumulator.byteLength - commaBytes - 1 - built.byteLength;
  if (availableForKey < 2) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const keyBytes = jsonStringByteLength(key, availableForKey);
  if (keyBytes > availableForKey) {
    addReason(state, "max_result_bytes");
    return false;
  }
  defineJsonProperty(accumulator.value, key, built.value);
  accumulator.byteLength += commaBytes + keyBytes + 1 + built.byteLength;
  accumulator.propertyCount += 1;
  return true;
}

function buildProperty(
  source: object,
  key: string,
  accumulator: ObjectAccumulator,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
): boolean {
  const descriptor = ownEnumerableDescriptor(source, key);
  if (descriptor === undefined) {
    return true;
  }
  if (key === "eidDataTruncation" && ownEnumerableDescriptor(source, "eidData") !== undefined) {
    return true;
  }
  const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
  const availableForKeyAndChild = maximumJsonBytes - accumulator.byteLength - commaBytes - 1;
  if (availableForKeyAndChild < 4) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const maximumKeyBytes = availableForKeyAndChild - 2;
  const keyBytes = jsonStringByteLength(key, maximumKeyBytes);
  if (keyBytes > maximumKeyBytes) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const childBudget = availableForKeyAndChild - keyBytes;

  let rawValue: unknown;
  if ("value" in descriptor) {
    rawValue = descriptor.value;
  } else {
    addReason(state, "accessor_omitted");
    rawValue = "[Accessor omitted]";
  }
  const errorKey = ERROR_KEYS.has(key.toLowerCase()) && hasReportedError(rawValue);
  if (errorKey) {
    state.hasErrors = true;
  }
  const useSharedRows = key === "rows" || (key === "data" && Array.isArray(rawValue));

  if (key === "eidData") {
    return buildEntitlementProperty(
      source,
      rawValue,
      accumulator,
      maximumJsonBytes,
      childBudget,
      state,
      depth,
      ENTITLEMENT_TRAVERSAL,
    );
  }

  const built = buildValue(rawValue, childBudget, state, depth + 1, rowLimit, useSharedRows);
  if (built === OMIT || !appendBuiltProperty(accumulator, key, built, maximumJsonBytes, state)) {
    return false;
  }
  if (errorKey) {
    rememberErrorDiagnostic(state, key, built.value);
  }
  if ((key === "truncated" || key === "truncatedInput") && rawValue === true) {
    addReason(state, "upstream_truncation");
  }
  return true;
}

function hasArrayMetadata(value: readonly unknown[]): boolean {
  for (const key of PRIORITY_KEYS) {
    if (ownEnumerableDescriptor(value, key) !== undefined) {
      return true;
    }
  }
  return false;
}

function buildArrayRows(
  value: readonly unknown[],
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.retainedNodes += 1;
  const output: unknown[] = [];
  let byteLength = 2;
  let processedRows = 0;
  const retainedLength = Math.min(
    value.length,
    rowLimit,
    useSharedRows ? state.remainingRows : Number.MAX_SAFE_INTEGER,
  );
  for (let index = 0; index < retainedLength; index += 1) {
    const commaBytes = index === 0 ? 0 : 1;
    const childBudget = maximumJsonBytes - byteLength - commaBytes;
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    let entry: unknown = null;
    if (descriptor !== undefined && "value" in descriptor) {
      entry = descriptor.value;
    } else if (descriptor !== undefined) {
      addReason(state, "accessor_omitted");
      entry = "[Accessor omitted]";
    }
    const built = buildValue(entry, childBudget, state, depth + 1, rowLimit, false);
    if (built === OMIT) {
      break;
    }
    output.push(built.value);
    byteLength += commaBytes + built.byteLength;
    processedRows = index + 1;
  }
  if (useSharedRows) {
    state.maximumArrayRows = Math.max(state.maximumArrayRows, processedRows);
  }
  if (useSharedRows) {
    state.remainingRows -= processedRows;
    state.retainedRows += processedRows;
  }
  if (retainedLength < value.length) {
    addReason(state, "max_rows");
  }
  if (processedRows < value.length) {
    state.omittedRows += value.length - processedRows;
  }
  return { byteLength, value: output };
}

function buildArray(
  value: readonly unknown[],
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  state.ancestors.add(value);
  try {
    if (!hasArrayMetadata(value)) {
      return buildArrayRows(value, maximumJsonBytes, state, depth, rowLimit, useSharedRows);
    }
    if (maximumJsonBytes < 2) {
      addReason(state, "max_result_bytes");
      return OMIT;
    }
    state.retainedNodes += 1;
    const accumulator: ObjectAccumulator = {
      byteLength: 2,
      propertyCount: 0,
      value: Object.create(null) as Record<string, unknown>,
    };
    for (const key of PRIORITY_KEYS) {
      const isErrorMetadata = key === "diagnostics" || ERROR_KEYS.has(key.toLowerCase());
      if (state.rowsBeforeMetadata && !isErrorMetadata) {
        continue;
      }
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
      }
    }

    const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
    const rowsKeyBytes = jsonStringByteLength("rows");
    const rowsBudget = maximumJsonBytes - accumulator.byteLength - commaBytes - rowsKeyBytes - 1;
    const rows = consumeVisit(state)
      ? buildArrayRows(value, rowsBudget, state, depth + 1, rowLimit, useSharedRows)
      : OMIT;
    if (rows === OMIT) {
      state.omittedRows += value.length;
      state.omittedPropertiesAtLeast += 1;
    } else if (!appendBuiltProperty(accumulator, "rows", rows, maximumJsonBytes, state)) {
      state.omittedRows += Math.min(value.length, rowLimit);
      state.omittedPropertiesAtLeast += 1;
    }

    if (state.rowsBeforeMetadata) {
      for (const key of PRIORITY_KEYS) {
        if (key === "diagnostics" || ERROR_KEYS.has(key.toLowerCase())) {
          continue;
        }
        if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
          state.omittedPropertiesAtLeast += 1;
        }
      }
    }

    return { byteLength: accumulator.byteLength, value: accumulator.value };
  } finally {
    state.ancestors.delete(value);
  }
}

function buildObject(
  value: Record<string, unknown>,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
): BuiltValue | typeof OMIT {
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.ancestors.add(value);
  state.retainedNodes += 1;
  try {
    const accumulator: ObjectAccumulator = {
      byteLength: 2,
      propertyCount: 0,
      value: Object.create(null) as Record<string, unknown>,
    };
    const processed = new Set<string>();
    // Preserve diagnostics and entitlement metadata before a wide ordinary
    // branch can exhaust the shared budget; other keys keep enumeration order.
    for (const key of PRIORITY_KEYS) {
      processed.add(key);
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
      }
    }

    for (const key in value) {
      if (!Object.hasOwn(value, key) || processed.has(key)) {
        continue;
      }
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
        break;
      }
    }
    return { byteLength: accumulator.byteLength, value: accumulator.value };
  } finally {
    state.ancestors.delete(value);
  }
}

function errorRecord(error: Error): Record<string, unknown> {
  const record: Record<string, unknown> = {
    message: error.message,
    name: error.name,
  };
  if (error.cause !== undefined) {
    record.cause = error.cause;
  }
  return record;
}

const ACCESSOR_METHOD = Symbol("accessor_method");

function isCallable(value: unknown): value is (...args: readonly unknown[]) => unknown {
  return typeof value === "function";
}

function dataMethod(
  value: object,
  name: string,
): ((...args: readonly unknown[]) => unknown) | typeof ACCESSOR_METHOD | undefined {
  let current: object | null = value;
  try {
    for (let depth = 0; current !== null && depth <= MAX_RESULT_DEPTH; depth += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(current, name);
      if (descriptor !== undefined) {
        if (!("value" in descriptor)) {
          return ACCESSOR_METHOD;
        }
        const method: unknown = descriptor.value;
        return isCallable(method) ? method : undefined;
      }
      current = Object.getPrototypeOf(current) as object | null;
    }
  } catch {
    return ACCESSOR_METHOD;
  }
  return undefined;
}

function buildValue(
  value: unknown,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (!consumeVisit(state)) {
    return OMIT;
  }
  if (depth > MAX_RESULT_DEPTH) {
    return buildUnsupported(
      "[Max result depth exceeded]",
      maximumJsonBytes,
      state,
      "max_result_depth",
    );
  }
  if (typeof value === "string") {
    return fitString(value, maximumJsonBytes, state);
  }
  if (typeof value === "bigint") {
    return fitString(value.toString(), maximumJsonBytes, state);
  }
  if (value === null || typeof value === "boolean") {
    return primitiveBuilt(value, maximumJsonBytes, state);
  }
  if (typeof value === "number") {
    const jsonValue = Number.isFinite(value) ? value : null;
    if (jsonValue === null) {
      addReason(state, "unsupported_value");
    }
    return primitiveBuilt(jsonValue, maximumJsonBytes, state);
  }
  if (typeof value === "undefined" || typeof value === "function" || typeof value === "symbol") {
    addReason(state, "unsupported_value");
    return primitiveBuilt(null, maximumJsonBytes, state);
  }

  if (value instanceof Date) {
    const milliseconds = value.getTime();
    if (!Number.isFinite(milliseconds)) {
      addReason(state, "unsupported_value");
      return fitString("[Invalid Date]", maximumJsonBytes, state);
    }
    return fitString(value.toISOString(), maximumJsonBytes, state);
  }
  if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
    return buildUnsupported(
      `[binary data: ${String(value.byteLength)} bytes]`,
      maximumJsonBytes,
      state,
      "binary_data",
    );
  }
  if (Array.isArray(value)) {
    return buildArray(value, maximumJsonBytes, state, depth, rowLimit, useSharedRows);
  }
  if (value instanceof Error) {
    state.hasErrors = true;
    if (state.ancestors.has(value)) {
      return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
    }
    state.ancestors.add(value);
    try {
      const built = buildObject(errorRecord(value), maximumJsonBytes, state, depth, rowLimit);
      if (built !== OMIT) {
        rememberErrorDiagnostic(state, "error", built.value);
      }
      return built;
    } finally {
      state.ancestors.delete(value);
    }
  }

  let plainObject: Record<string, unknown> | undefined;
  try {
    plainObject = isPlainObject(value) ? value : undefined;
  } catch {
    return buildUnsupported("[Uninspectable object]", maximumJsonBytes, state);
  }
  if (plainObject !== undefined) {
    return buildObject(plainObject, maximumJsonBytes, state, depth, rowLimit);
  }

  const toJSON = dataMethod(value, "toJSON");
  if (toJSON === ACCESSOR_METHOD) {
    return buildUnsupported("[Accessor omitted]", maximumJsonBytes, state, "accessor_omitted");
  }
  if (toJSON === undefined) {
    return buildUnsupported("[Unsupported object]", maximumJsonBytes, state);
  }
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  state.ancestors.add(value);
  try {
    let converted: unknown;
    try {
      converted = toJSON.call(value);
    } catch (error) {
      state.hasErrors = true;
      addReason(state, "unsupported_value");
      converted = {
        error: {
          message: error instanceof Error ? error.message : String(error),
          name: "toJSON",
        },
      };
    }
    return buildValue(converted, maximumJsonBytes, state, depth + 1, rowLimit, useSharedRows);
  } finally {
    state.ancestors.delete(value);
  }
}

export function rowCountOf(value: unknown): number | null {
  if (Array.isArray(value)) {
    return value.length;
  }
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const record = value as Record<string, unknown>;
  for (const key of ["rowCount", "updateCount"] as const) {
    const descriptor = ownEnumerableDescriptor(record, key);
    if (
      descriptor !== undefined &&
      "value" in descriptor &&
      typeof descriptor.value === "number" &&
      Number.isSafeInteger(descriptor.value) &&
      descriptor.value >= 0
    ) {
      return descriptor.value;
    }
  }
  return null;
}

function truncationSummary(state: LimitState): ResultTruncationSummary | undefined {
  if (state.reasons.size === 0) {
    return undefined;
  }
  return {
    reasons: TRUNCATION_REASON_ORDER.filter((reason) => state.reasons.has(reason)),
    inspectedNodes: state.visitedNodes,
    retainedNodes: state.retainedNodes,
    ...(state.omittedPropertiesAtLeast === 0
      ? {}
      : { omittedPropertiesAtLeast: state.omittedPropertiesAtLeast }),
    ...(state.omittedRows === 0 ? {} : { omittedRows: state.omittedRows }),
  };
}

export function validateResultLimits(limits: ResultLimitOptions): void {
  for (const name of ["maxResultBytes", "maxResultNodes", "maxRows", "maxStringChars"] as const) {
    const value = limits[name];
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new RangeError(`${name} must be a positive safe integer; got ${String(value)}`);
    }
  }
  if (limits.maxResultBytes < 4) {
    throw new RangeError(`maxResultBytes must be at least 4; got ${String(limits.maxResultBytes)}`);
  }
}

export function limitResultWithRowPriority(
  value: unknown,
  limits: ResultLimitOptions,
  rowsBeforeMetadata: boolean,
): LimitResult {
  validateResultLimits(limits);
  const state: LimitState = {
    ancestors: new WeakSet<object>(),
    diagnostics: [],
    hasErrors: false,
    maximumArrayRows: 0,
    limits,
    omittedPropertiesAtLeast: 0,
    omittedRows: 0,
    remainingRows: limits.maxRows,
    reasons: new Set<ResultTruncationReason>(),
    retainedNodes: 0,
    retainedRows: 0,
    rowsBeforeMetadata,
    visitedNodes: 0,
  };
  const rowCount = rowCountOf(value);
  const built = buildValue(value, limits.maxResultBytes, state, 0, limits.maxRows, true);
  const result = built === OMIT ? { byteLength: 4, value: null } : built;
  if (built === OMIT) {
    addReason(state, "max_result_bytes");
  }
  const truncation = truncationSummary(state);
  return {
    byteLength: result.byteLength,
    errorDiagnostics: state.diagnostics,
    inspectedNodes: state.visitedNodes,
    maximumArrayRows: state.maximumArrayRows,
    retainedRows: state.retainedRows,
    hasErrors: state.hasErrors,
    rowCount,
    truncated: truncation !== undefined,
    ...(truncation === undefined ? {} : { truncation }),
    value: result.value,
  };
}

function exhaustedProjection(rowCount: number | null): LimitResult {
  return {
    byteLength: 4,
    maximumArrayRows: 0,
    retainedRows: 0,
    errorDiagnostics: [],
    hasErrors: false,
    inspectedNodes: 0,
    rowCount,
    truncated: true,
    truncation: {
      inspectedNodes: 0,
      reasons: ["max_result_nodes"],
      retainedNodes: 0,
    },
    value: null,
  };
}

export function projectResult(
  value: unknown,
  limits: ResultLimitOptions,
  rowsBeforeMetadata: boolean,
): LimitResult {
  return limits.maxResultNodes === 0
    ? exhaustedProjection(rowCountOf(value))
    : limitResultWithRowPriority(value, limits, rowsBeforeMetadata);
}
