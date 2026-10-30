import type { Table } from 'apache-arrow';

import { RecordBatchReader, tableFromIPC, tableToIPC } from 'apache-arrow';
import { createRequire } from 'node:module';

import { FORMAT_BY_NAME } from './_defs_gen';
import { tableFromNativeArrowBatch } from './arrow-zero-copy';
import { wrapError } from './errors';
import type { NativeArrowZeroCopyBatch } from './napi';
import { isPlainObject } from './objects';
import type {
  BackendKind,
  BloombergFieldError,
  BloombergFieldException,
  BloombergMetadataError,
  DelayedDataInfo,
  FormatKind,
  ResultMetadata,
} from './types';

const nodeRequire = createRequire(__filename);

export const Backend = Object.freeze({
  ARROW: 'arrow',
  JSON: 'json',
  POLARS: 'polars',
}) satisfies Readonly<Record<string, BackendKind>>;

/**
 * Canonical output formats. Names and wire values both come from
 * `defs/bloomberg.toml` via `_defs_gen.ts`.
 *
 * Retyping them here is what shipped `LONG_WITH_METADATA` as
 * `'long_with_metadata'`, a value the Rust engine rejects; its real wire value
 * is `'long_metadata'`.
 */
export const Format = Object.freeze(FORMAT_BY_NAME) satisfies Readonly<Record<string, FormatKind>>;
interface PolarsModule {
  readIPC(buffer: Buffer): object;
}

function isPolarsModule(value: unknown): value is PolarsModule {
  return isPlainObject(value) && typeof value.readIPC === 'function';
}

function requirePolarsModule(): PolarsModule {
  const loaded: unknown = nodeRequire('nodejs-polars');
  if (isPolarsModule(loaded)) {
    return loaded;
  }
  throw new TypeError('nodejs-polars did not expose readIPC(buffer)');
}

const METADATA_KEY_EID_DATA = 'xbbg.eid_data';
const METADATA_KEY_SECURITY_ERRORS = 'xbbg.security_errors';
const METADATA_KEY_FIELD_EXCEPTIONS = 'xbbg.field_exceptions';
const METADATA_KEY_FIELD_ERRORS = 'xbbg.field_errors';
const METADATA_KEY_DELAYED = 'xbbg.delayed';
const METADATA_KEY_BLOOMBERG_REQUEST_ID = 'xbbg.bloomberg_request_id';
const METADATA_KEY_BLOOMBERG_REQUEST_IDS = 'xbbg.bloomberg_request_ids';

function metadataRecordFromMap(metadata: ReadonlyMap<string, string>): Record<string, string> {
  return Object.fromEntries(metadata.entries());
}

function parseJsonMetadata<T>(
  metadata: Record<string, string>,
  key: string,
  guard: (value: unknown) => value is T,
): T | undefined {
  const raw = metadata[key];
  if (raw === undefined) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    return guard(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function isNumberArrayRecord(value: unknown): value is Record<string, number[]> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(
    (entry) => Array.isArray(entry) && entry.every((eid) => typeof eid === 'number'),
  );
}

function isMetadataError(value: unknown): value is BloombergMetadataError {
  if (!isPlainObject(value)) {
    return false;
  }
  const { category, code, message, subcategory } = value;
  return (
    (category === undefined || typeof category === 'string') &&
    (code === undefined || typeof code === 'string' || typeof code === 'number') &&
    (message === undefined || typeof message === 'string') &&
    (subcategory === undefined || typeof subcategory === 'string')
  );
}

function isMetadataErrorRecord(value: unknown): value is Record<string, BloombergMetadataError> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(isMetadataError);
}

function isFieldException(value: unknown): value is BloombergFieldException {
  if (!isMetadataError(value)) {
    return false;
  }
  if (!('field' in value)) {
    return true;
  }
  return typeof value.field === 'string';
}

function isFieldExceptionRecord(
  value: unknown,
): value is Record<string, BloombergFieldException[]> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(
    (entry) => Array.isArray(entry) && entry.every(isFieldException),
  );
}

function parseDiagnosticMetadata(metadata: Record<string, string>, key: string): unknown {
  const raw = metadata[key];
  if (raw === undefined) {
    return undefined;
  }
  try {
    return JSON.parse(raw) as unknown;
  } catch (cause) {
    throw new TypeError(`Invalid ${key} metadata: expected valid JSON`, { cause });
  }
}

function isInt32(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isInteger(value) &&
    value >= -2_147_483_648 &&
    value <= 2_147_483_647
  );
}

function isCompactFieldException(value: unknown): value is BloombergFieldException {
  return (
    isPlainObject(value) &&
    typeof value.field === 'string' &&
    typeof value.category === 'string' &&
    isInt32(value.code) &&
    typeof value.subcategory === 'string' &&
    typeof value.message === 'string'
  );
}

type FieldExceptionsProperty =
  | { value: Record<string, BloombergFieldException[]> | undefined }
  | { get: () => Record<string, BloombergFieldException[]> };

function validateFieldExceptionIndices(
  securities: Record<string, unknown>,
  dictionarySize: number,
): asserts securities is Record<string, number[]> {
  for (const [security, indices] of Object.entries(securities)) {
    if (!Array.isArray(indices)) {
      throw new TypeError(
        `Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: expected index array for ${JSON.stringify(security)}`,
      );
    }
    for (const index of indices as readonly unknown[]) {
      if (
        typeof index !== 'number' ||
        !Number.isSafeInteger(index) ||
        index < 0 ||
        index >= dictionarySize
      ) {
        throw new TypeError(
          `Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: invalid dictionary index for ${JSON.stringify(security)}`,
        );
      }
    }
  }
}

function fieldExceptionsFromMetadata(metadata: Record<string, string>): FieldExceptionsProperty {
  const parsed = parseDiagnosticMetadata(metadata, METADATA_KEY_FIELD_EXCEPTIONS);
  if (parsed === undefined) {
    return { value: undefined };
  }
  if (!isPlainObject(parsed)) {
    throw new TypeError(`Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: expected an object`);
  }
  // The reserved pair identifies envelopes even when their member types are malformed.
  const envelope = Object.hasOwn(parsed, 'entries') && Object.hasOwn(parsed, 'securities');
  if (!envelope && isFieldExceptionRecord(parsed)) {
    return { value: parsed };
  }
  if (parsed.version !== 1) {
    throw new TypeError(
      `Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: unsupported or missing version`,
    );
  }
  const { entries, securities } = parsed;
  if (
    Object.keys(parsed).length !== 3 ||
    !Object.hasOwn(parsed, 'entries') ||
    !Object.hasOwn(parsed, 'securities') ||
    !Array.isArray(entries) ||
    !entries.every(isCompactFieldException) ||
    !isPlainObject(securities)
  ) {
    throw new TypeError(
      `Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: malformed version 1 envelope`,
    );
  }
  // Validate every occurrence now, without allocating expanded payload objects.
  validateFieldExceptionIndices(securities, entries.length);
  let expanded: Record<string, BloombergFieldException[]> | undefined;
  return {
    get: () => {
      expanded ??= Object.fromEntries(
        Object.entries(securities).map(
          ([security, indices]): [string, BloombergFieldException[]] => [
            security,
            indices.map((index) => {
              const exception = entries[index];
              if (exception === undefined) {
                throw new TypeError(
                  `Invalid ${METADATA_KEY_FIELD_EXCEPTIONS} metadata: missing dictionary index`,
                );
              }
              // Each expanded occurrence has its own mutable payload.
              return { ...exception };
            }),
          ],
        ),
      );
      return expanded;
    },
  };
}

function isFieldError(value: unknown): value is BloombergFieldError {
  return (
    isPlainObject(value) &&
    typeof value.category === 'string' &&
    isInt32(value.code) &&
    typeof value.subcategory === 'string' &&
    typeof value.message === 'string' &&
    (value.source === undefined || typeof value.source === 'string')
  );
}

function isFieldErrorRecord(value: unknown): value is Record<string, BloombergFieldError> {
  return isPlainObject(value) && Object.values(value).every(isFieldError);
}

function fieldErrorsFromMetadata(
  metadata: Record<string, string>,
): Record<string, BloombergFieldError> | undefined {
  const parsed = parseDiagnosticMetadata(metadata, METADATA_KEY_FIELD_ERRORS);
  if (parsed === undefined) {
    return undefined;
  }
  if (!isFieldErrorRecord(parsed)) {
    throw new TypeError(`Invalid ${METADATA_KEY_FIELD_ERRORS} metadata: malformed field errors`);
  }
  return parsed;
}

function isDelayedDataInfo(value: unknown): value is DelayedDataInfo {
  if (!isPlainObject(value)) {
    return false;
  }
  const { delayed, delay_minutes: delayMinutes } = value;
  return (
    typeof delayed === 'boolean' && (delayMinutes === null || typeof delayMinutes === 'number')
  );
}

function isDelayedDataRecord(value: unknown): value is Record<string, DelayedDataInfo> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(isDelayedDataInfo);
}

/**
 * Emit one `BlpDelayedDataWarning` per delayed ticker in `xbbg.delayed`.
 *
 * Callers pass native result metadata before backend conversion, so every
 * backend warns.
 */
export function emitDelayedDataWarnings(metadata: Record<string, string>): void {
  const delayed = parseJsonMetadata(metadata, METADATA_KEY_DELAYED, isDelayedDataRecord);
  if (delayed === undefined) {
    return;
  }
  for (const [ticker, info] of Object.entries(delayed)) {
    if (!info.delayed) {
      continue;
    }
    const lag =
      info.delay_minutes === null ? '' : ` (${info.delay_minutes} minutes behind real time)`;
    process.emitWarning(`${ticker}: Bloomberg is delivering delayed data${lag}`, {
      type: 'BlpDelayedDataWarning',
      code: 'XBBG_DELAYED_DATA',
    });
  }
}

/** Read the schema metadata of an Arrow IPC buffer without decoding its record batches. */
export function ipcSchemaMetadata(buffer: Buffer): Record<string, string> {
  return metadataRecordFromMap(RecordBatchReader.from(buffer).open().schema.metadata);
}

function attachResultMetadata<T extends object>(
  result: T,
  metadata: Record<string, string>,
  configurable = false,
): T & ResultMetadata {
  const bloombergRequestId = metadata[METADATA_KEY_BLOOMBERG_REQUEST_ID];
  const bloombergRequestIds =
    bloombergRequestId === undefined
      ? parseJsonMetadata(
          metadata,
          METADATA_KEY_BLOOMBERG_REQUEST_IDS,
          (value): value is string[] =>
            Array.isArray(value) && value.every((id) => typeof id === 'string'),
        )
      : [bloombergRequestId];
  const eidData = parseJsonMetadata(metadata, METADATA_KEY_EID_DATA, isNumberArrayRecord);
  const delayed = parseJsonMetadata(metadata, METADATA_KEY_DELAYED, isDelayedDataRecord);
  const securityErrors = parseJsonMetadata(
    metadata,
    METADATA_KEY_SECURITY_ERRORS,
    isMetadataErrorRecord,
  );
  const fieldExceptions = fieldExceptionsFromMetadata(metadata);
  const fieldErrors = fieldErrorsFromMetadata(metadata);
  Object.defineProperties(result, {
    bloombergRequestId: { enumerable: true, configurable, value: bloombergRequestId },
    bloombergRequestIds: { enumerable: true, configurable, value: bloombergRequestIds },
    delayed: { enumerable: true, configurable, value: delayed },
    eidData: { enumerable: true, configurable, value: eidData },
    fieldExceptions: { enumerable: true, configurable, ...fieldExceptions },
    fieldErrors: { enumerable: true, configurable, value: fieldErrors },
    metadata: { enumerable: true, configurable, value: { ...metadata } },
    securityErrors: { enumerable: true, configurable, value: securityErrors },
  });
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Object.defineProperties attaches the ResultMetadata fields above.
  return result as T & ResultMetadata;
}

export function toArrowTableFromNative(batch: NativeArrowZeroCopyBatch): Table & ResultMetadata {
  return attachResultMetadata(tableFromNativeArrowBatch(batch), batch.metadata);
}

let polarsModule: PolarsModule | undefined;
let polarsLoadError: Error | undefined;

function cachePolarsLoadError(err: unknown): Error {
  const error = new Error(
    'nodejs-polars is required for Polars backend. Install: npm install nodejs-polars',
  );
  Object.defineProperty(error, 'cause', { configurable: true, value: err });
  polarsLoadError = error;
  return error;
}

function loadPolars(): PolarsModule {
  if (polarsModule !== undefined) {
    return polarsModule;
  }
  if (polarsLoadError !== undefined) {
    throw polarsLoadError;
  }
  try {
    polarsModule = requirePolarsModule();
    return polarsModule;
  } catch (error) {
    throw cachePolarsLoadError(error);
  }
}

export function normalizeBackend(backend: BackendKind | undefined): BackendKind {
  const selected: unknown = backend ?? Backend.ARROW;
  if (selected === Backend.ARROW || selected === Backend.JSON || selected === Backend.POLARS) {
    return selected;
  }
  throw new TypeError(
    `Unsupported @xbbg/core backend "${String(selected)}". Expected one of: ${Object.values(
      Backend,
    ).join(', ')}`,
  );
}

export function nativeArrowToBackend(
  batch: NativeArrowZeroCopyBatch,
  backend: BackendKind | undefined,
): unknown {
  const selected = normalizeNativeBackend(backend);
  const table = tableFromNativeArrowBatch(batch);
  const metadata = metadataRecordFromMap(table.schema.metadata);
  if (selected === Backend.JSON) {
    return attachResultMetadata([...table], metadata);
  }
  return attachResultMetadata(table, metadata);
}

export function nativeArrowToPolars(batch: NativeArrowZeroCopyBatch): unknown {
  const polars = loadPolars();
  // The supported nodejs-polars 0.10 API reads file IPC, not streams.
  const ipc = tableToIPC(tableFromNativeArrowBatch(batch), 'file');
  const frame = polars.readIPC(Buffer.from(ipc.buffer, ipc.byteOffset, ipc.byteLength));
  // Configurable metadata respects Polars' column-only ownKeys Proxy.
  return attachResultMetadata(frame, batch.metadata, true);
}

export function ipcToPolars(buffer: Buffer): unknown {
  const reader = RecordBatchReader.from(buffer).open();
  const metadata = metadataRecordFromMap(reader.schema.metadata);
  // The supported Polars reader needs file IPC; keep file inputs zero-copy.
  const ipc = reader.isFile() ? buffer : tableToIPC(tableFromIPC(reader), 'file');
  const frame = loadPolars().readIPC(Buffer.from(ipc.buffer, ipc.byteOffset, ipc.byteLength));
  return attachResultMetadata(frame, metadata, true);
}

function normalizeNativeBackend(backend: BackendKind | undefined): 'arrow' | 'json' {
  const selected = normalizeBackend(backend);
  if (selected === Backend.POLARS) {
    throw new TypeError('Polars backend requires the IPC requestRaw path');
  }
  return selected;
}

export async function nativeRecipeResult(
  backend: BackendKind | undefined,
  request: () => Promise<NativeArrowZeroCopyBatch>,
): Promise<unknown> {
  const selected = normalizeNativeBackend(backend);
  try {
    return nativeArrowToBackend(await request(), selected);
  } catch (error) {
    throw wrapError(error);
  }
}
