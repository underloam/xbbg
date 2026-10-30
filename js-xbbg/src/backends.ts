import type { Table } from 'apache-arrow';

import { RecordBatchReader, tableToIPC } from 'apache-arrow';
import { createRequire } from 'node:module';

import { FORMAT_BY_NAME } from './_defs_gen';
import { tableFromNativeArrowBatch } from './arrow-zero-copy';
import { wrapError } from './errors';
import type { NativeArrowZeroCopyBatch } from './napi';
import { isPlainObject } from './objects';
import type {
  BackendKind,
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
  const fieldExceptions = parseJsonMetadata(
    metadata,
    METADATA_KEY_FIELD_EXCEPTIONS,
    isFieldExceptionRecord,
  );
  Object.defineProperties(result, {
    bloombergRequestId: { enumerable: true, configurable, value: bloombergRequestId },
    bloombergRequestIds: { enumerable: true, configurable, value: bloombergRequestIds },
    delayed: { enumerable: true, configurable, value: delayed },
    eidData: { enumerable: true, configurable, value: eidData },
    fieldExceptions: { enumerable: true, configurable, value: fieldExceptions },
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
  return loadPolars().readIPC(buffer);
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
