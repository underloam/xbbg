import type { ResultTruncationReason } from "./_defs_gen";

export interface ResultLimitOptions {
  readonly maxResultBytes: number;
  readonly maxResultNodes: number;
  readonly maxRows: number;
  readonly maxStringChars: number;
}

export interface ResultTruncationSummary {
  readonly reasons: readonly ResultTruncationReason[];
  readonly retainedNodes?: number;
  readonly inspectedNodes?: number;
  readonly omittedPropertiesAtLeast?: number;
  readonly omittedRows?: number;
}

export interface LimitResult {
  readonly byteLength: number;
  readonly inspectedNodes: number;
  readonly maximumArrayRows: number;
  readonly retainedRows: number;
  readonly errorDiagnostics: readonly Readonly<Record<string, unknown>>[];
  readonly hasErrors: boolean;
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly truncation?: ResultTruncationSummary;
  readonly value: unknown;
}

export interface LimitState {
  readonly ancestors: WeakSet<object>;
  readonly diagnostics: Readonly<Record<string, unknown>>[];
  readonly limits: ResultLimitOptions;
  readonly reasons: Set<ResultTruncationReason>;
  readonly rowsBeforeMetadata: boolean;
  maximumArrayRows: number;
  hasErrors: boolean;
  omittedPropertiesAtLeast: number;
  remainingRows: number;
  omittedRows: number;
  retainedNodes: number;
  retainedRows: number;
  visitedNodes: number;
}

export interface BuiltValue {
  readonly byteLength: number;
  readonly value: unknown;
}

export interface ObjectAccumulator {
  byteLength: number;
  propertyCount: number;
  readonly value: Record<string, unknown>;
}

export const OMIT = Symbol("omit_result_value");

export function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

export function addReason(state: LimitState, reason: ResultTruncationReason): void {
  state.reasons.add(reason);
}

export function consumeVisit(state: LimitState): boolean {
  if (state.visitedNodes >= state.limits.maxResultNodes) {
    addReason(state, "max_result_nodes");
    return false;
  }
  state.visitedNodes += 1;
  return true;
}

export function defineJsonProperty(
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

export function ownEnumerableDescriptor(
  value: object,
  key: string,
): PropertyDescriptor | undefined {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor?.enumerable === true ? descriptor : undefined;
  } catch {
    return undefined;
  }
}
