#!/usr/bin/env node

import type { Table } from 'apache-arrow';

import { tableFromArrays, tableFromIPC, tableToIPC } from 'apache-arrow';
import * as fs from 'node:fs';
import { createRequire } from 'node:module';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';
import { setImmediate as yieldToEventLoop, setTimeout as sleep } from 'node:timers/promises';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import type { NativeArrowColumn, NativeArrowZeroCopyBatch, NativeSubscription } from '../src/napi';
import type { SubscriptionReadOptions, SubscriptionStats } from '../src/types';
import {
  collectProvenance,
  fileSha256,
  memoryMb,
  round,
  summarizeDurations,
} from './benchmark-contracts';

const requireDist = createRequire(__filename);
const DEFAULT_FIELDS = ['LAST_PRICE', 'BID', 'ASK'];
const DEFAULT_TOPIC = 'XBTUSD Curncy';
const DEFAULT_ROWS = 100_000;
const DEFAULT_ITERATIONS = 1;
const DEFAULT_CAPTURE_MS = 10_000;
const DEFAULT_STATS_INTERVAL_MS = 1000;
const DEFAULT_BATCH_ROWS = 64;
const DEFAULT_QUEUE_CAPACITY = 16;
const DEFAULT_PRODUCER_BURST = 4;
const REPLAY_PATH_VALUES = ['legacy', 'arrow-decode-only', 'subscription-wrapper'] as const;
const CONSUME_MODE_VALUES = ['rows', 'vector', 'schema', 'none'] as const;
// Arrow bitmap bits; each row's bit is set once, so addition equals OR.
const ARROW_BITMAP_BITS = [1, 2, 4, 8, 16, 32, 64, 128] as const;

type ReplayPath = (typeof REPLAY_PATH_VALUES)[number];
type ConsumeMode = (typeof CONSUME_MODE_VALUES)[number];
type ReplayRow = Record<string, unknown>;
type BenchmarkResult = Record<string, unknown>;

interface CliArgs {
  batchRows: number;
  cancelAfterRows: number;
  captureLive?: string;
  captureMs: number;
  consume: ConsumeMode;
  consumerDelayMs: number;
  drainOnCancel: boolean;
  fields: string[];
  fixture?: string;
  help?: boolean;
  iterations: number;
  json: boolean;
  out?: string;
  path: ReplayPath;
  producerBurst: number;
  queueCapacity: number;
  rows: number;
  statsIntervalMs: number;
  topic: string;
  warmupIterations: number;
}

interface SyntheticQueueMetrics {
  readonly cancelled: boolean;
  readonly deliveredBatches: number;
  readonly deliveredAfterDrop: number;
  readonly deliveredRows: number;
  readonly discardedBatches: number;
  readonly discardedRows: number;
  readonly drainedBatches: number;
  readonly drainedRows: number;
  readonly droppedBatches: number;
  readonly droppedRows: number;
  readonly maxQueueDepth: number;
  readonly producedBatches: number;
  readonly producedRows: number;
  readonly queueCapacityBatches: number;
}

interface ReplayIteration {
  readonly batchDurationsMs: readonly number[];
  readonly checksum: number;
  readonly elapsedMs: number;
  readonly queue?: SyntheticQueueMetrics;
  readonly rowsConsumed: number;
}

interface JsonSerializableRow {
  toJSON(): unknown;
}

interface ArrowReplaySubscription {
  next(options?: SubscriptionReadOptions): Promise<IteratorResult<Table>>;
  return?(): Promise<IteratorResult<Table>>;
}

type NativeArrowReader = Pick<
  NativeSubscription,
  'nextArrowBatch' | 'unsubscribeArrow' | 'takeWarnings' | 'deliversRows'
>;
type ArrowSubscriptionConstructor = new (inner: NativeArrowReader) => ArrowReplaySubscription;

interface CaptureTick {
  toObject(): ReplayRow;
}

interface CaptureSubscription {
  readonly stats: SubscriptionStats;
  next(options?: SubscriptionReadOptions): Promise<IteratorResult<CaptureTick>>;
  unsubscribe(drain: boolean): Promise<CaptureTick[]>;
}

interface CaptureEngine {
  signalShutdown(): void;
  stream(tickers: readonly string[], fields: readonly string[]): Promise<CaptureSubscription>;
}

interface DistCore {
  connect(config: { readonly host: string; readonly port: number }): Promise<CaptureEngine>;
}

interface DistReplayCore extends DistCore {
  readonly ArrowSubscription: ArrowSubscriptionConstructor;
}

class AutonomousNativeSubscription implements NativeArrowReader {
  readonly fields: string[];
  readonly tickers: string[];
  readonly deliversRows = true;

  private readonly batches: readonly NativeArrowZeroCopyBatch[];
  private readonly capacity: number;
  private readonly producerBurst: number;
  private readonly queue: NativeArrowZeroCopyBatch[] = [];
  private readonly waiters: ((batch: NativeArrowZeroCopyBatch | null) => void)[] = [];
  private readonly producer: Promise<void>;
  private active = true;
  private cancelled = false;
  private deliveredBatches = 0;
  private deliveredRows = 0;
  private deliveredAfterDrop = 0;
  private sawDrop = false;
  private discardedBatches = 0;
  private drainedBatches = 0;
  private drainedRows = 0;
  private droppedBatches = 0;
  private droppedRows = 0;
  private discardedRows = 0;
  private maxQueueDepth = 0;
  private producedBatches = 0;
  private producedRows = 0;

  constructor(
    batches: readonly NativeArrowZeroCopyBatch[],
    args: Pick<CliArgs, 'fields' | 'producerBurst' | 'queueCapacity' | 'topic'>,
  ) {
    this.batches = batches;
    this.capacity = args.queueCapacity;
    this.fields = [...args.fields];
    this.producerBurst = args.producerBurst;
    this.tickers = [args.topic];
    this.producer = this.produce();
  }

  get isActive(): boolean {
    return this.active || this.queue.length > 0;
  }

  get stats(): SubscriptionStats {
    return {
      batchesSent: this.producedBatches - this.droppedBatches,
      droppedBatches: this.droppedBatches,
      messagesReceived: this.producedRows,
      slowConsumer: this.droppedBatches > 0,
    };
  }

  takeWarnings(): [] {
    return [];
  }

  async nextArrowBatch(
    _maxRows?: number,
    _maxWaitMs?: number,
  ): Promise<NativeArrowZeroCopyBatch | null> {
    const queued = this.queue.shift();
    if (queued !== undefined) {
      this.recordDelivery(queued);
      return queued;
    }
    if (!this.active) {
      return null;
    }
    return new Promise<NativeArrowZeroCopyBatch | null>((resolve) => {
      this.waiters.push((batch) => {
        if (batch !== null) {
          this.recordDelivery(batch);
        }
        resolve(batch);
      });
    });
  }

  async unsubscribeArrow(drain: boolean): Promise<NativeArrowZeroCopyBatch[] | null> {
    return this.stop(drain);
  }

  async settled(): Promise<void> {
    await this.producer;
  }

  metrics(): SyntheticQueueMetrics {
    return {
      cancelled: this.cancelled,
      deliveredBatches: this.deliveredBatches,
      deliveredAfterDrop: this.deliveredAfterDrop,
      deliveredRows: this.deliveredRows,
      discardedBatches: this.discardedBatches,
      discardedRows: this.discardedRows,
      drainedBatches: this.drainedBatches,
      drainedRows: this.drainedRows,
      droppedBatches: this.droppedBatches,
      droppedRows: this.droppedRows,
      maxQueueDepth: this.maxQueueDepth,
      producedBatches: this.producedBatches,
      producedRows: this.producedRows,
      queueCapacityBatches: this.capacity,
    };
  }

  private async produce(): Promise<void> {
    for (let index = 0; index < this.batches.length && this.active; index += 1) {
      const batch = this.batches[index];
      if (batch === undefined) {
        throw new Error(`Missing synthetic native batch at ${index}`);
      }
      this.producedBatches += 1;
      this.producedRows += batch.numRows;
      const waiter = this.waiters.shift();
      if (waiter !== undefined) {
        waiter(batch);
      } else if (this.queue.length < this.capacity) {
        this.queue.push(batch);
        this.maxQueueDepth = Math.max(this.maxQueueDepth, this.queue.length);
      } else {
        this.sawDrop = true;
        this.droppedBatches += 1;
        this.droppedRows += batch.numRows;
      }
      if ((index + 1) % this.producerBurst === 0) {
        await yieldToEventLoop();
      }
    }
    this.active = false;
    this.resolveWaiters();
  }

  private recordDelivery(batch: NativeArrowZeroCopyBatch): void {
    if (this.sawDrop) {
      this.deliveredAfterDrop += 1;
    }
    this.deliveredBatches += 1;
    this.deliveredRows += batch.numRows;
  }

  private async stop(drain: boolean): Promise<NativeArrowZeroCopyBatch[] | null> {
    if (this.active) {
      this.cancelled = true;
      this.active = false;
    }
    await this.producer;
    const queued = this.queue.splice(0);
    let queuedRows = 0;
    for (const batch of queued) {
      queuedRows += batch.numRows;
    }
    if (drain) {
      this.drainedBatches += queued.length;
      this.drainedRows += queuedRows;
    } else {
      this.discardedBatches += queued.length;
      this.discardedRows += queuedRows;
    }
    this.resolveWaiters();
    return drain && queued.length > 0 ? queued : null;
  }

  private resolveWaiters(): void {
    for (const waiter of this.waiters.splice(0)) {
      waiter(null);
    }
  }
}

function usage(): string {
  return `Usage:
  tsx benchmarks/bench-subscription-replay.ts [--rows N] [--iterations N]
  tsx benchmarks/bench-subscription-replay.ts --fixture tmp/ticks.jsonl
  tsx benchmarks/bench-subscription-replay.ts --capture-live "XBTUSD Curncy" --out tmp/ticks.jsonl

Replay options:
  --path legacy|arrow-decode-only|subscription-wrapper
  --consume rows|vector|schema|none
  --batch-rows N             Rows per replay batch. Default ${DEFAULT_BATCH_ROWS}
  --queue-capacity N         Synthetic wrapper queue capacity in batches. Default ${DEFAULT_QUEUE_CAPACITY}
  --producer-burst N         Batches produced before yielding. Default ${DEFAULT_PRODUCER_BURST}
  --consumer-delay-ms N      Delay after each batch; included in throughput, excluded from service latency
  --cancel-after-rows N      Cancel wrapper after at least N consumed rows; 0 disables
  --drain-on-cancel          Convert queued batches after cancellation instead of discarding them
  --warmup-iterations N      Untimed replay iterations
  --iterations N             Measured replay iterations. Default ${DEFAULT_ITERATIONS}
  --fixture PATH             JSONL fixture; otherwise generate --rows synthetic rows

Capture options:
  --capture-live TICKER      Live Engine.stream() capture
  --capture-ms N             Capture duration. Default ${DEFAULT_CAPTURE_MS}
  --out PATH                 Required JSONL destination
  --stats-interval-ms N      Stats reporting interval. Default ${DEFAULT_STATS_INTERVAL_MS}

Common:
  --fields A,B,C             Default ${DEFAULT_FIELDS.join(',')}
  --topic TICKER             Default ${DEFAULT_TOPIC}
  --json
  --help
`;
}

function parseArgs(argv: readonly string[]): CliArgs {
  const args: CliArgs = {
    batchRows: DEFAULT_BATCH_ROWS,
    cancelAfterRows: 0,
    captureMs: DEFAULT_CAPTURE_MS,
    consume: 'rows',
    consumerDelayMs: 0,
    drainOnCancel: false,
    fields: [...DEFAULT_FIELDS],
    iterations: DEFAULT_ITERATIONS,
    json: false,
    path: 'legacy',
    producerBurst: DEFAULT_PRODUCER_BURST,
    queueCapacity: DEFAULT_QUEUE_CAPACITY,
    rows: DEFAULT_ROWS,
    statsIntervalMs: DEFAULT_STATS_INTERVAL_MS,
    topic: DEFAULT_TOPIC,
    warmupIterations: 0,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = readArg(argv, index, 'argument');
    const next = (): string => readArg(argv, (index += 1), arg);
    switch (arg) {
      case '--rows':
        args.rows = parsePositiveInteger(next(), '--rows');
        break;
      case '--iterations':
        args.iterations = parsePositiveInteger(next(), '--iterations');
        break;
      case '--warmup-iterations':
        args.warmupIterations = parseNonNegativeInteger(next(), '--warmup-iterations');
        break;
      case '--fixture':
        args.fixture = next();
        break;
      case '--path':
        args.path = parseReplayPath(next());
        break;
      case '--consume':
        args.consume = parseConsumeMode(next());
        break;
      case '--batch-rows':
        args.batchRows = parsePositiveInteger(next(), '--batch-rows');
        break;
      case '--queue-capacity':
        args.queueCapacity = parsePositiveInteger(next(), '--queue-capacity');
        break;
      case '--producer-burst':
        args.producerBurst = parsePositiveInteger(next(), '--producer-burst');
        break;
      case '--cancel-after-rows':
        args.cancelAfterRows = parseNonNegativeInteger(next(), '--cancel-after-rows');
        break;
      case '--drain-on-cancel':
        args.drainOnCancel = true;
        break;
      case '--capture-live':
        args.captureLive = next();
        break;
      case '--capture-ms':
        args.captureMs = parsePositiveInteger(next(), '--capture-ms');
        break;
      case '--out':
        args.out = next();
        break;
      case '--fields':
        args.fields = parseCsv(next());
        break;
      case '--topic':
        args.topic = next();
        break;
      case '--consumer-delay-ms':
        args.consumerDelayMs = parseNonNegativeInteger(next(), '--consumer-delay-ms');
        break;
      case '--stats-interval-ms':
        args.statsIntervalMs = parsePositiveInteger(next(), '--stats-interval-ms');
        break;
      case '--json':
        args.json = true;
        break;
      case '--help':
      case '-h':
        args.help = true;
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }
  return args;
}

function readArg(argv: readonly string[], index: number, name: string): string {
  const value = argv[index];
  if (value === undefined) {
    throw new Error(`${name} requires a value`);
  }
  return value;
}

function parsePositiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}
function parseReplayPath(value: string): ReplayPath {
  switch (value) {
    case 'legacy':
    case 'arrow-decode-only':
    case 'subscription-wrapper':
      return value;
    default:
      throw new Error(`--path must be one of: ${REPLAY_PATH_VALUES.join(', ')}`);
  }
}

function parseConsumeMode(value: string): ConsumeMode {
  switch (value) {
    case 'rows':
    case 'vector':
    case 'schema':
    case 'none':
      return value;
    default:
      throw new Error(`--consume must be one of: ${CONSUME_MODE_VALUES.join(', ')}`);
  }
}

function parseCsv(value: string): string[] {
  const items = value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (items.length === 0) {
    throw new Error('expected at least one field');
  }
  return items;
}

function assertReplayRow(value: unknown, message: string): asserts value is ReplayRow {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError(message);
  }
}

function syntheticTick(index: number, topic: string, fields: readonly string[]): ReplayRow {
  const price = 50_000 + Math.sin(index / 17) * 250 + (index % 97) * 0.25;
  const row: ReplayRow = {
    MKTDATA_EVENT_SUBTYPE: '',
    MKTDATA_EVENT_TYPE: 'TRADE',
    timestamp: new Date(1_735_689_600_000 + index * 100).toISOString(),
    topic,
  };
  for (const field of fields) {
    row[field] =
      field === 'BID' ? round(price - 1.25) : field === 'ASK' ? round(price + 1.25) : round(price);
  }
  return row;
}

function readJsonl(filePath: string): ReplayRow[] {
  const rows: ReplayRow[] = [];
  let lineNumber = 0;
  for (const line of fs.readFileSync(filePath, 'utf8').split(/\r?\n/u)) {
    lineNumber += 1;
    if (line.trim() === '') {
      continue;
    }
    try {
      const parsed: unknown = JSON.parse(line);
      assertReplayRow(parsed, 'expected a JSON object row');
      rows.push(parsed);
    } catch (error) {
      throw new Error(`invalid JSONL at ${filePath}:${lineNumber}: ${errorMessage(error)}`, {
        cause: error,
      });
    }
  }
  if (rows.length === 0) {
    throw new Error(`fixture contains no rows: ${filePath}`);
  }
  return rows;
}

function chunkRows(rows: readonly ReplayRow[], batchRows: number): ReplayRow[][] {
  const chunks: ReplayRow[][] = [];
  for (let index = 0; index < rows.length; index += batchRows) {
    chunks.push(rows.slice(index, index + batchRows));
  }
  return chunks;
}

function replayValueKind(name: string, value: unknown): string {
  const kind = value instanceof Date ? 'Date' : typeof value;
  if (!['number', 'bigint', 'boolean', 'string', 'Date'].includes(kind)) {
    throw new TypeError(`replay column ${name} contains unsupported ${kind} values`);
  }
  return kind;
}

function assertReplayRows(rows: readonly ReplayRow[]): void {
  const columnKinds = new Map<string, string>();
  for (const row of rows) {
    for (const [name, value] of Object.entries(row)) {
      if (value === null || value === undefined) {
        continue;
      }
      const kind = replayValueKind(name, value);
      const expectedKind = columnKinds.get(name);
      if (expectedKind === undefined) {
        columnKinds.set(name, kind);
      } else if (kind !== expectedKind) {
        throw new TypeError(`replay column ${name} mixes ${expectedKind} and ${kind} values`);
      }
    }
  }
}

function replayArrays(rows: readonly ReplayRow[]): Record<string, unknown[]> {
  if (rows.length === 0) {
    throw new Error('cannot encode an empty replay batch');
  }
  const names: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    for (const name of Object.keys(row)) {
      if (!seen.has(name)) {
        seen.add(name);
        names.push(name);
      }
    }
  }
  const arrays: Record<string, unknown[]> = {};
  for (const name of names) {
    const values = rows.map((row) => row[name] ?? null);
    assertHomogeneousColumn(name, values);
    arrays[name] = values;
  }
  return arrays;
}

function assertHomogeneousColumn(name: string, values: readonly unknown[]): void {
  let expectedKind: string | undefined;
  for (const value of values) {
    if (value === null || value === undefined) {
      continue;
    }
    const kind = replayValueKind(name, value);
    if (expectedKind === undefined) {
      expectedKind = kind;
    } else if (kind !== expectedKind) {
      throw new TypeError(`replay column ${name} mixes ${expectedKind} and ${kind} values`);
    }
  }
}

function rowsToIpc(rows: readonly ReplayRow[]): Uint8Array {
  return tableToIPC(tableFromArrays(replayArrays(rows)));
}

function rowsToNativeArrowBatch(rows: readonly ReplayRow[]): NativeArrowZeroCopyBatch {
  const arrays = replayArrays(rows);
  return {
    columns: Object.entries(arrays).map(([name, values]) => nativeArrowColumn(name, values)),
    kind: 'zeroCopy',
    metadata: {
      'xbbg.benchmark': 'subscription-replay',
      'xbbg.synthetic': 'true',
    },
    numRows: rows.length,
  };
}

function setArrowBitmapBit(bitmap: Buffer, index: number): void {
  const byteIndex = Math.floor(index / ARROW_BITMAP_BITS.length);
  bitmap[byteIndex] =
    (bitmap[byteIndex] ?? 0) + (ARROW_BITMAP_BITS[index % ARROW_BITMAP_BITS.length] ?? 0);
}

function nativeArrowColumn(name: string, values: readonly unknown[]): NativeArrowColumn {
  const first = values.find((value) => value !== null && value !== undefined);
  if (first === undefined) {
    return { length: values.length, name, nullCount: values.length, nullable: true, type: 'null' };
  }
  const validity = validityBitmap(values);
  if (typeof first === 'number') {
    const data = new Float64Array(values.length);
    for (const [index, value] of values.entries()) {
      if (value === null || value === undefined) {
        continue;
      }
      if (typeof value !== 'number') {
        throw new TypeError(`replay column ${name} contains a non-number value`);
      }
      data[index] = value;
    }
    return bufferedColumn(name, 'float64', values.length, typedBuffer(data), validity);
  }
  if (typeof first === 'bigint') {
    const data = new BigInt64Array(values.length);
    for (const [index, value] of values.entries()) {
      if (value === null || value === undefined) {
        continue;
      }
      if (typeof value !== 'bigint') {
        throw new TypeError(`replay column ${name} contains a non-bigint value`);
      }
      data[index] = value;
    }
    return bufferedColumn(name, 'int64', values.length, typedBuffer(data), validity);
  }
  if (first instanceof Date) {
    const data = new BigInt64Array(values.length);
    for (const [index, value] of values.entries()) {
      if (value === null || value === undefined) {
        continue;
      }
      if (!(value instanceof Date)) {
        throw new TypeError(`replay column ${name} contains a non-Date value`);
      }
      data[index] = BigInt(value.getTime()) * 1000n;
    }
    return bufferedColumn(name, 'timestamp_us', values.length, typedBuffer(data), validity);
  }
  if (typeof first === 'boolean') {
    const data = Buffer.alloc(Math.ceil(values.length / ARROW_BITMAP_BITS.length));
    for (const [index, value] of values.entries()) {
      if (value === null || value === undefined) {
        continue;
      }
      if (typeof value !== 'boolean') {
        throw new TypeError(`replay column ${name} contains a non-boolean value`);
      }
      if (value) {
        setArrowBitmapBit(data, index);
      }
    }
    return bufferedColumn(name, 'bool', values.length, data, validity);
  }
  if (typeof first !== 'string') {
    throw new TypeError(`replay column ${name} contains an unsupported value`);
  }
  const offsets = new Int32Array(values.length + 1);
  const chunks: Buffer[] = [];
  let byteLength = 0;
  for (const [index, value] of values.entries()) {
    if (value !== null && value !== undefined) {
      if (typeof value !== 'string') {
        throw new TypeError(`replay column ${name} contains a non-string value`);
      }
      const chunk = Buffer.from(value);
      chunks.push(chunk);
      byteLength += chunk.byteLength;
    }
    offsets[index + 1] = byteLength;
  }
  return {
    data: Buffer.concat(chunks, byteLength),
    length: values.length,
    name,
    nullBitmap: validity.nullCount === 0 ? undefined : validity.bitmap,
    nullCount: validity.nullCount,
    nullable: validity.nullCount > 0,
    offsets: typedBuffer(offsets),
    type: 'utf8',
  };
}

function validityBitmap(values: readonly unknown[]): { bitmap: Buffer; nullCount: number } {
  const bitmap = Buffer.alloc(Math.ceil(values.length / ARROW_BITMAP_BITS.length));
  let nullCount = 0;
  for (const [index, value] of values.entries()) {
    if (value === null || value === undefined) {
      nullCount += 1;
    } else {
      setArrowBitmapBit(bitmap, index);
    }
  }
  return { bitmap, nullCount };
}

function bufferedColumn(
  name: string,
  type: NativeArrowColumn['type'],
  length: number,
  data: Buffer,
  validity: { bitmap: Buffer; nullCount: number },
): NativeArrowColumn {
  return {
    data,
    length,
    name,
    nullBitmap: validity.nullCount === 0 ? undefined : validity.bitmap,
    nullCount: validity.nullCount,
    nullable: validity.nullCount > 0,
    type,
  };
}

function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

function isJsonSerializableRow(value: unknown): value is JsonSerializableRow {
  // Arrow row proxies expose methods through get, but their has trap only sees columns.
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'toJSON') === 'function'
  );
}

function consumeDecodedTable(table: Table, mode: ConsumeMode): number {
  if (mode === 'none') {
    return table.numRows;
  }
  if (mode === 'schema') {
    return table.schema.fields.length;
  }
  let checksum = 0;
  if (mode === 'vector') {
    for (const field of table.schema.fields) {
      const vector = table.getChild(field.name);
      for (let row = 0; row < table.numRows; row += 1) {
        checksum += valueChecksum(vector?.get(row));
      }
    }
    return checksum;
  }
  for (const row of table) {
    const rowValue: unknown = row;
    if (!isJsonSerializableRow(rowValue)) {
      throw new TypeError('Arrow table yielded a row without toJSON');
    }
    const materialized: unknown = rowValue.toJSON();
    assertReplayRow(materialized, 'Arrow row toJSON must return an object');
    for (const value of Object.values(materialized)) {
      checksum += valueChecksum(value);
    }
  }
  return checksum;
}

function valueChecksum(value: unknown): number {
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value % 997 : 0;
  }
  if (typeof value === 'bigint') {
    return Number(value % 997n);
  }
  if (typeof value === 'string') {
    return value.length;
  }
  if (value instanceof Date) {
    return value.getTime() % 997;
  }
  return JSON.stringify(value)?.length ?? 1;
}

function loadDistModule(): unknown {
  const moduleValue: unknown = requireDist('../dist/index.js');
  return moduleValue;
}

function loadCore(): DistCore {
  const core = loadDistModule();
  if (!isDistCore(core)) {
    throw new Error('built dist/index.js does not expose the expected capture API');
  }
  return core;
}

function loadSubscriptionClass(): ArrowSubscriptionConstructor {
  const core = loadDistModule();
  if (!isDistReplayCore(core)) {
    throw new Error('subscription-wrapper requires built dist/index.js with ArrowSubscription');
  }
  return core.ArrowSubscription;
}

function timingBoundary(pathName: ReplayPath, consume: ConsumeMode): string {
  const consumer =
    consume === 'rows' || consume === 'vector'
      ? ` plus every cell through ${consume}`
      : ` plus ${consume} consumer`;

  if (pathName === 'legacy') {
    return `inclusive row arrays to IPC encode, IPC decode${consumer}`;
  }
  if (pathName === 'arrow-decode-only') {
    return `isolated prebuilt IPC decode${consumer}; IPC encoding excluded`;
  }
  return `autonomous bounded native descriptor queue, ArrowSubscription.next, descriptor conversion${consumer}`;
}

function assertQueueConservation(metrics: SyntheticQueueMetrics): void {
  const accountedBatches =
    metrics.deliveredBatches +
    metrics.drainedBatches +
    metrics.droppedBatches +
    metrics.discardedBatches;
  const accountedRows =
    metrics.deliveredRows + metrics.drainedRows + metrics.droppedRows + metrics.discardedRows;
  if (accountedBatches !== metrics.producedBatches || accountedRows !== metrics.producedRows) {
    throw new Error(
      `synthetic queue lost accounting: produced=${metrics.producedBatches}/${metrics.producedRows} accounted=${accountedBatches}/${accountedRows}`,
    );
  }
  if (metrics.maxQueueDepth > metrics.queueCapacityBatches) {
    throw new Error('synthetic queue exceeded its configured capacity');
  }
}

async function replayIteration(
  args: CliArgs,
  rowChunks: readonly ReplayRow[][],
  replayBuffers: readonly Buffer[] | undefined,
  replayNativeBatches: readonly NativeArrowZeroCopyBatch[] | undefined,
  ArrowSubscription: ArrowSubscriptionConstructor | undefined,
): Promise<ReplayIteration> {
  const durations: number[] = [];
  let checksum = 0;
  let rowsConsumed = 0;
  const started = performance.now();
  let queue: SyntheticQueueMetrics | undefined;

  if (args.path === 'subscription-wrapper') {
    if (replayNativeBatches === undefined || ArrowSubscription === undefined) {
      throw new Error('missing prebuilt native descriptor batches or preloaded wrapper');
    }
    const native = new AutonomousNativeSubscription(replayNativeBatches, args);
    const subscription = new ArrowSubscription(native);
    let drainedAfterCancel: NativeArrowZeroCopyBatch[] | null | undefined;
    while (true) {
      const before = performance.now();
      const result = await subscription.next();
      if (result.done === true) {
        break;
      }
      checksum += consumeDecodedTable(result.value, args.consume);
      durations.push(performance.now() - before);
      rowsConsumed += result.value.numRows;
      if (args.cancelAfterRows > 0 && rowsConsumed >= args.cancelAfterRows) {
        drainedAfterCancel = await native.unsubscribeArrow(args.drainOnCancel);
        break;
      }
      if (args.consumerDelayMs > 0) {
        await sleep(args.consumerDelayMs);
      }
    }
    if (drainedAfterCancel !== undefined && drainedAfterCancel !== null) {
      for (const batch of drainedAfterCancel) {
        const table = tableFromNativeArrowBatch(batch);
        checksum += consumeDecodedTable(table, args.consume);
        rowsConsumed += table.numRows;
      }
    }
    await native.settled();
    if (subscription.return !== undefined) {
      await subscription.return();
    }
    queue = native.metrics();
    assertQueueConservation(queue);
  } else {
    const inputs = args.path === 'arrow-decode-only' ? replayBuffers : rowChunks;
    if (inputs === undefined) {
      throw new Error('missing replay inputs');
    }
    for (let index = 0; index < inputs.length; index += 1) {
      const before = performance.now();
      let table: Table;
      if (args.path === 'legacy') {
        const rows = rowChunks[index];
        if (rows === undefined) {
          throw new Error(`missing legacy replay batch ${index}`);
        }
        table = tableFromIPC(rowsToIpc(rows));
      } else {
        const buffer = replayBuffers?.[index];
        if (buffer === undefined) {
          throw new Error(`missing prebuilt Arrow replay batch ${index}`);
        }
        table = tableFromIPC(buffer);
      }
      checksum += consumeDecodedTable(table, args.consume);
      durations.push(performance.now() - before);
      rowsConsumed += table.numRows;
      if (args.cancelAfterRows > 0 && rowsConsumed >= args.cancelAfterRows) {
        break;
      }
      if (args.consumerDelayMs > 0) {
        await sleep(args.consumerDelayMs);
      }
    }
  }

  return {
    batchDurationsMs: durations,
    checksum,
    elapsedMs: performance.now() - started,
    queue,
    rowsConsumed,
  };
}

function isLosslessReplay(observation: ReplayIteration, expectedRows: number): boolean {
  if (observation.rowsConsumed !== expectedRows) {
    return false;
  }
  const queue = observation.queue;
  return (
    queue === undefined ||
    (queue.producedRows === expectedRows &&
      queue.droppedBatches === 0 &&
      queue.discardedBatches === 0)
  );
}

async function runReplay(args: CliArgs): Promise<BenchmarkResult> {
  const fixturePath = args.fixture === undefined ? undefined : path.resolve(args.fixture);
  const rows =
    fixturePath === undefined
      ? Array.from({ length: args.rows }, (_, index) =>
          syntheticTick(index, args.topic, args.fields),
        )
      : readJsonl(fixturePath);
  assertReplayRows(rows);
  const rowChunks = chunkRows(rows, args.batchRows);
  const ArrowSubscription =
    args.path === 'subscription-wrapper' ? loadSubscriptionClass() : undefined;
  const setupStarted = performance.now();
  const replayBuffers =
    args.path === 'arrow-decode-only'
      ? rowChunks.map((chunk) => Buffer.from(rowsToIpc(chunk)))
      : undefined;
  const replayNativeBatches =
    args.path === 'subscription-wrapper'
      ? rowChunks.map((chunk) => rowsToNativeArrowBatch(chunk))
      : undefined;
  const setupMs = performance.now() - setupStarted;

  for (let iteration = 0; iteration < args.warmupIterations; iteration += 1) {
    await replayIteration(args, rowChunks, replayBuffers, replayNativeBatches, ArrowSubscription);
  }
  const measured: ReplayIteration[] = [];
  for (let iteration = 0; iteration < args.iterations; iteration += 1) {
    measured.push(
      await replayIteration(args, rowChunks, replayBuffers, replayNativeBatches, ArrowSubscription),
    );
  }
  const checksum = measured[0]?.checksum ?? 0;
  const measuredChecksumsComparable = measured.every((observation) =>
    isLosslessReplay(observation, rows.length),
  );
  if (
    measuredChecksumsComparable &&
    measured.some((observation) => observation.checksum !== checksum)
  ) {
    throw new Error('lossless replay checksum changed between measured samples');
  }

  const memoryBefore = memoryMb();
  const memoryObservation = await replayIteration(
    args,
    rowChunks,
    replayBuffers,
    replayNativeBatches,
    ArrowSubscription,
  );
  const memoryAfter = memoryMb();
  const memoryChecksumCompared =
    measuredChecksumsComparable && isLosslessReplay(memoryObservation, rows.length);
  if (memoryChecksumCompared && memoryObservation.checksum !== checksum) {
    throw new Error('separate lossless memory observation changed the replay checksum');
  }
  const batchDurations = measured.flatMap((observation) => observation.batchDurationsMs);
  let elapsedMs = 0;
  let rowsConsumed = 0;
  for (const observation of measured) {
    elapsedMs += observation.elapsedMs;
    rowsConsumed += observation.rowsConsumed;
  }
  const provenanceInput =
    fixturePath === undefined
      ? {
          batchRows: args.batchRows,
          fields: args.fields,
          rows: args.rows,
          source: 'deterministic synthetic generator',
          topic: args.topic,
        }
      : {
          bytes: fs.statSync(fixturePath).size,
          sha256: fileSha256(fixturePath),
          path: fixturePath,
          source: 'JSONL fixture',
        };
  return {
    batchRows: args.batchRows,
    checksum,
    checksumContract: measuredChecksumsComparable
      ? 'compared across lossless measured runs'
      : 'not compared because at least one measured run was lossy or partial',
    consume: args.consume,
    consumerDelayMs: args.consumerDelayMs,
    coverage:
      'offline synthetic/fixture replay; subscription-wrapper uses a synthetic autonomous bounded NativeSubscription, not SDK/network events',
    elapsedMs,
    fixture: fixturePath,
    iterations: args.iterations,
    measuredChecksums: measured.map((observation) => observation.checksum),
    memoryMb: { after: memoryAfter, before: memoryBefore },
    memoryObservationChecksum: memoryObservation.checksum,
    memoryChecksumCompared,
    memorySampleCount: 1,
    memoryScope:
      'V8 process counters before/after one separate untimed replay; includes heap/external/array buffers/RSS but is not a peak',
    mode: fixturePath === undefined ? 'synthetic-replay' : 'fixture-replay',
    path: args.path,
    perBatchMs: summarizeDurations(batchDurations),
    provenance: collectProvenance(
      provenanceInput,
      path.basename(__filename),
      args.path === 'subscription-wrapper',
    ),
    overflowPolicy: args.path === 'subscription-wrapper' ? 'drop-newest' : undefined,
    queue:
      args.path === 'subscription-wrapper'
        ? measured.map((observation) => observation.queue)
        : undefined,
    queueCapacityUnits: args.path === 'subscription-wrapper' ? 'Arrow batches' : undefined,
    rowsConsumed,
    rowsPerIteration: rows.length,
    schemaVersion: 2,
    setupMs: round(setupMs),
    throughputRowsPerSecond: rowsConsumed / (elapsedMs / 1000),
    timingBoundary: timingBoundary(args.path, args.consume),
    warmupIterations: args.warmupIterations,
  };
}

async function writeLine(writer: fs.WriteStream, line: string): Promise<void> {
  if (writer.write(line)) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const onDrain = (): void => {
      writer.off('error', onError);
      resolve();
    };
    const onError = (error: Error): void => {
      writer.off('drain', onDrain);
      reject(error);
    };
    writer.once('drain', onDrain);
    writer.once('error', onError);
  });
}

async function closeWriter(writer: fs.WriteStream): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onFinish = (): void => {
      writer.off('error', onError);
      resolve();
    };
    const onError = (error: Error): void => {
      writer.off('finish', onFinish);
      reject(error);
    };
    writer.once('finish', onFinish);
    writer.once('error', onError);
    writer.end();
  });
}

async function runCapture(args: CliArgs): Promise<BenchmarkResult> {
  if (args.out === undefined || args.out.length === 0 || args.captureLive === undefined) {
    throw new Error('--capture-live requires a ticker and --out');
  }
  const core = loadCore();
  const outputPath = path.resolve(args.out);
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const engine = await core.connect({
    host: process.env.XBBG_HOST ?? 'localhost',
    port: Number(process.env.XBBG_PORT ?? 8194),
  });
  const subscription = await engine.stream([args.captureLive], args.fields);
  const writer = fs.createWriteStream(outputPath, { encoding: 'utf8' });
  const controller = new AbortController();
  const abortTimer = setTimeout(() => {
    controller.abort();
  }, args.captureMs);
  let rows = 0;
  let iteratorItems = 0;
  let finalStats = subscription.stats;
  const started = performance.now();
  const statsTimer = setInterval(() => {
    finalStats = subscription.stats;
    if (!args.json) {
      console.error(
        `stats ${JSON.stringify(finalStats)} rows=${rows} iteratorItems=${iteratorItems}`,
      );
    }
  }, args.statsIntervalMs);

  try {
    while (true) {
      let result: IteratorResult<CaptureTick>;
      try {
        result = await subscription.next({ signal: controller.signal });
      } catch (error) {
        if (controller.signal.aborted && isAbortError(error)) {
          break;
        }
        throw error;
      }
      if (result.done === true) {
        break;
      }
      iteratorItems += 1;
      await writeLine(writer, `${JSON.stringify(result.value.toObject())}\n`);
      rows += 1;
    }
  } finally {
    clearTimeout(abortTimer);
    clearInterval(statsTimer);
    controller.abort();
    finalStats = subscription.stats;
    try {
      await subscription.unsubscribe(false).catch((): CaptureTick[] => []);
      await closeWriter(writer);
    } finally {
      engine.signalShutdown();
    }
  }

  const elapsedMs = performance.now() - started;
  return {
    iteratorItems,
    coverage: 'live SDK/network Engine.stream capture',
    elapsedMs,
    fields: args.fields,
    mode: 'capture-live',
    output: outputPath,
    provenance: collectProvenance(
      {
        captureMs: args.captureMs,
        fields: args.fields,
        output: outputPath,
        ticker: args.captureLive,
        outputSha256: fileSha256(outputPath),
      },
      path.basename(__filename),
      true,
    ),
    rows,
    schemaVersion: 2,
    stats: finalStats,
    ticker: args.captureLive,
    timingBoundary:
      'warm connected stream capture including native wait, JS wrapper, toObject, JSON serialization, and file backpressure',
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError';
}

function isDistCore(value: unknown): value is DistCore {
  return (
    typeof value === 'object' &&
    value !== null &&
    'connect' in value &&
    typeof value.connect === 'function'
  );
}

function isDistReplayCore(value: unknown): value is DistReplayCore {
  return (
    isDistCore(value) &&
    'ArrowSubscription' in value &&
    typeof value.ArrowSubscription === 'function'
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help === true) {
    process.stdout.write(usage());
    return;
  }
  if (args.captureLive !== undefined && args.fixture !== undefined) {
    throw new Error('--capture-live and --fixture are mutually exclusive');
  }
  const result = args.captureLive === undefined ? await runReplay(args) : await runCapture(args);
  console.log(JSON.stringify(result, null, 2));
}

main().catch((error: unknown) => {
  console.error(errorMessage(error));
  process.exit(1);
});
