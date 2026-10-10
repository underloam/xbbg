import type { Table } from 'apache-arrow';

import { nativeArrowToBackend, toArrowTableFromNative } from './backends';
import { BlpValidationError, wrapError } from './errors';
import type {
  NativeArrowZeroCopyBatch,
  NativeEngine,
  NativeSubscription,
  NativeSubscriptionFieldKind,
  NativeSubscriptionLayout,
  NativeSubscriptionRow,
  NativeSubscriptionUpdateBatch,
} from './napi';
import { isPlainObject } from './objects';
import type {
  AdminStatus,
  RecipeBackendOptions,
  ServiceStatus,
  SessionStatus,
  StreamOptions,
  SubscriptionEvent,
  SubscriptionFailure,
  SubscriptionReadOptions,
  SubscriptionStats,
  SubscriptionStatus,
  TopicState,
} from './types';

export const MKTDATA_SERVICE = '//blp/mktdata';

// The positional subscribeWithOptions API also accepts native overflow policy strings.
type SubscriptionOptions = Omit<StreamOptions, 'overflowPolicy'> & { overflowPolicy?: string };

function subscriptionOptionKey(option: string): string {
  return normalizeSubscriptionOption(option).split('=')[0]?.trim().toLowerCase() ?? '';
}

function normalizeSubscriptionOption(option: string): string {
  let clean = option.trim();
  while (clean.startsWith('&')) {
    clean = clean.slice(1).trim();
  }
  return clean;
}

function validateAliases(aliases: unknown): void {
  if (aliases === undefined) {
    return;
  }
  if (isPlainObject(aliases)) {
    const prototype: unknown = Object.getPrototypeOf(aliases);
    if (
      (prototype === Object.prototype || prototype === null) &&
      Object.values(aliases).every((label) => typeof label === 'string')
    ) {
      return;
    }
  }
  throw new BlpValidationError('aliases must map topic strings to label strings', {
    element: 'aliases',
  });
}

export function validateStreamControls(options: SubscriptionOptions): void {
  validateAliases(options.aliases);
  const policy: unknown = options.onDelayed;
  if (policy !== undefined && policy !== 'warn' && policy !== 'raise' && policy !== 'ignore') {
    throw new BlpValidationError('onDelayed must be warn, raise, or ignore', {
      element: 'onDelayed',
    });
  }
  const isolated: unknown = options.isolated;
  if (isolated !== undefined && typeof isolated !== 'boolean') {
    throw new BlpValidationError('isolated must be a boolean', { element: 'isolated' });
  }
  const rows: unknown = options.rows;
  if (rows !== undefined && typeof rows !== 'boolean') {
    throw new BlpValidationError('rows must be a boolean', { element: 'rows' });
  }
  const fieldPolicy: unknown = options.onFieldError;
  if (
    fieldPolicy !== undefined &&
    fieldPolicy !== 'warn' &&
    fieldPolicy !== 'raise' &&
    fieldPolicy !== 'ignore'
  ) {
    throw new BlpValidationError('onFieldError must be warn, raise, or ignore', {
      element: 'onFieldError',
    });
  }
  const zeroAsNull: unknown = options.zeroAsNull;
  if (
    zeroAsNull !== undefined &&
    (!Array.isArray(zeroAsNull) || zeroAsNull.some((field: unknown) => typeof field !== 'string'))
  ) {
    throw new BlpValidationError('zeroAsNull must be an array of field names', {
      element: 'zeroAsNull',
    });
  }
}

function buildStreamSubscriptionOptions(
  service: string,
  options: SubscriptionOptions,
): readonly string[] | undefined {
  const rawOptions = options.options;
  const { conflate } = options;

  if (rawOptions === undefined && conflate !== true) {
    return undefined;
  }

  const subscriptionOptions = (rawOptions ?? [])
    .map((option) => normalizeSubscriptionOption(option))
    .filter((option) => option.length > 0);

  if (conflate === true) {
    if (service !== MKTDATA_SERVICE) {
      throw new BlpValidationError(
        'conflate=true is only supported for //blp/mktdata subscriptions',
        { element: 'conflate' },
      );
    }
    if (subscriptionOptions.some((option) => subscriptionOptionKey(option) === 'interval')) {
      throw new BlpValidationError(
        'conflate=true cannot be combined with interval options; intervalization overrides conflation',
        { element: 'conflate' },
      );
    }
    if (!subscriptionOptions.some((option) => subscriptionOptionKey(option) === 'conflate')) {
      subscriptionOptions.push('conflate');
    }
  }

  return subscriptionOptions.length > 0 || rawOptions !== undefined
    ? subscriptionOptions
    : undefined;
}

export async function createSubscription(
  engine: Pick<NativeEngine, 'subscribe' | 'subscribeWithOptions'>,
  service: string,
  tickers: readonly string[],
  fields: readonly string[],
  options: SubscriptionOptions,
  mode: 'default' | 'stream' | 'raw' = 'stream',
): Promise<Subscription> {
  try {
    // Raw positional calls preserve the caller's wire options; named options normalize them.
    validateStreamControls(options);
    const subscriptionOptions =
      mode === 'raw' ? options.options : buildStreamSubscriptionOptions(service, options);
    const controls = [
      options.allFields,
      options.aliases,
      options.onDelayed,
      options.isolated,
      options.rows,
      options.onFieldError,
      options.zeroAsNull,
    ] as const;
    const useDefaults =
      mode === 'default' &&
      subscriptionOptions === undefined &&
      options.flushThreshold === undefined &&
      options.overflowPolicy === undefined &&
      options.streamCapacity === undefined;
    const stream = useDefaults
      ? await engine.subscribe(tickers, fields, ...controls)
      : await engine.subscribeWithOptions(
          service,
          tickers,
          fields,
          subscriptionOptions,
          options.flushThreshold,
          options.overflowPolicy,
          options.streamCapacity,
          ...controls,
        );
    return new Subscription(stream);
  } catch (error) {
    throw wrapError(error);
  }
}

// ── Subscription class ──────────────────────────────────────────────────

export type TickValue = null | boolean | number | bigint | string | Date;

export class FieldHandle {
  public constructor(public readonly name: string) {}
}

interface TickLayout {
  readonly version: number;
  readonly fields: readonly string[];
  readonly kinds: readonly NativeSubscriptionFieldKind[];
  readonly positions: Map<string, number>;
}

function createTickLayout(layout: NativeSubscriptionLayout): TickLayout {
  return {
    fields: layout.fields,
    kinds: layout.kinds,
    positions: new Map(layout.fields.map((field, index) => [field, index])),
    version: layout.version,
  };
}

export class Tick {
  private readonly decodedSet: boolean[] = [];
  private readonly decodedValues: (TickValue | undefined)[] = [];
  private rowPositions: number[] | undefined;

  public constructor(
    private readonly update: NativeSubscriptionRow,
    private readonly layout: TickLayout,
  ) {}

  public get topic(): string {
    return this.update.topic;
  }

  public get timestampUs(): number {
    return this.update.timestampUs;
  }

  public get layoutVersion(): number {
    return this.update.layoutVersion;
  }

  public has(field: string | FieldHandle): boolean {
    const name = typeof field === 'string' ? field : field.name;
    const fieldIndex = this.layout.positions.get(name);
    return fieldIndex !== undefined && this.valuePosition(fieldIndex) !== undefined;
  }

  public get(field: string | FieldHandle): TickValue | undefined {
    const name = typeof field === 'string' ? field : field.name;
    const fieldIndex = this.layout.positions.get(name);
    return fieldIndex === undefined ? undefined : this.getByFieldIndex(fieldIndex);
  }

  private getByFieldIndex(fieldIndex: number): TickValue | undefined {
    if (this.decodedSet[fieldIndex] === true) {
      return this.decodedValues[fieldIndex];
    }
    const position = this.valuePosition(fieldIndex);
    if (position === undefined) {
      this.decodedSet[fieldIndex] = true;
      this.decodedValues[fieldIndex] = undefined;
      return undefined;
    }

    const kind = this.layout.kinds[fieldIndex] ?? 'unknown';
    let value: TickValue;
    if (kind === 'bool') {
      value = this.update.boolValues[position] ?? null;
    } else if (kind === 'i32') {
      value = this.update.i32Values[position] ?? null;
    } else if (kind === 'f64') {
      value = this.update.f64Values[position] ?? null;
    } else if (kind === 'str' || kind === 'unknown') {
      value = this.update.stringValues[position] ?? null;
    } else if (kind === 'date32') {
      const days = this.update.i32Values[position];
      value = days === null || days === undefined ? null : new Date(Date.UTC(1970, 0, 1 + days));
    } else {
      const raw = this.update.i64Values[position];
      if (raw === null || raw === undefined) {
        value = null;
      } else {
        try {
          value = BigInt(raw);
        } catch {
          value = null;
        }
      }
    }
    this.decodedSet[fieldIndex] = true;
    this.decodedValues[fieldIndex] = value;
    return value;
  }

  private valuePosition(fieldIndex: number): number | undefined {
    let positions = this.rowPositions;
    if (positions === undefined) {
      const built: number[] = [];
      for (const [position, index] of this.update.fieldIndices.entries()) {
        built[index] = position;
      }
      positions = built;
      this.rowPositions = positions;
    }
    return positions[fieldIndex];
  }

  public f64(field: string | FieldHandle): number | null | undefined {
    const value = this.get(field);
    if (value === null || value === undefined) {
      return value;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  public i64(field: string | FieldHandle): bigint | null | undefined {
    const value = this.get(field);
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof value === 'bigint') {
      return value;
    }
    try {
      return BigInt(typeof value === 'string' ? value : String(value));
    } catch {
      return null;
    }
  }

  public str(field: string | FieldHandle): string | null | undefined {
    const value = this.get(field);
    return value === null || value === undefined ? value : String(value);
  }

  public toObject(): Record<string, unknown> {
    const out: Record<string, unknown> = { timestampUs: this.timestampUs, topic: this.topic };
    for (const fieldIndex of this.update.fieldIndices) {
      const field = this.layout.fields[fieldIndex];
      if (field !== undefined) {
        out[field] = this.getByFieldIndex(fieldIndex);
      }
    }
    return out;
  }
}

class SubscriptionReadQueue {
  private tail: Promise<void> = Promise.resolve();

  public enqueue<T>(read: () => Promise<T>): Promise<T> {
    const result = this.tail.then(read);
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  public barrier(): Promise<void> {
    return this.tail;
  }
}

type SubscriptionIteratorPhase = 'open' | 'closing' | 'closed';
type SubscriptionReadMode = 'scalar' | 'arrow';

type SubscriptionBatchProjection<TBatch, TValue> =
  | {
      readonly cardinality: 'many';
      readonly project: (batch: TBatch) => TValue[];
    }
  | {
      readonly cardinality: 'one';
      readonly project: (batch: TBatch) => TValue;
    };

class SubscriptionCoordinator {
  public readonly reads = new SubscriptionReadQueue();
  private phase: SubscriptionIteratorPhase = 'open';
  private readMode: SubscriptionReadMode | undefined;
  private readonly closeObservers = new Set<(owner: object | undefined) => void>();
  private readonly closed: Promise<void>;
  private readonly resolveClosed: () => void;
  private closeError: Error | undefined;
  private lateReadError: Error | undefined;
  private readonly deliversRows: boolean;

  public constructor(private readonly inner: NativeSubscription) {
    this.deliversRows = inner.deliversRows;
    let resolveClosed!: () => void;
    this.closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    this.resolveClosed = resolveClosed;
  }

  public emitWarnings(): void {
    for (const warning of this.inner.takeWarnings()) {
      const delayed = warning.messageType === 'DelayedStream';
      const message = `${warning.messageType}${warning.topic === undefined || warning.topic === null ? '' : `: ${warning.topic}`}`;
      process.emitWarning(message, {
        type: delayed ? 'BlpDelayedDataWarning' : 'BlpFieldWarning',
        code: delayed ? 'XBBG_DELAYED_STREAM' : 'XBBG_FIELD_EXCEPTION',
        ...(warning.detail === undefined || warning.detail === null
          ? {}
          : { detail: warning.detail }),
      });
    }
  }

  public ensureRowDelivery(): void {
    if (!this.deliversRows) {
      throw new BlpValidationError(
        'Subscription has rows=false; use latest() instead of reading rows',
        {
          element: 'rows',
        },
      );
    }
  }

  public get isOpen(): boolean {
    return this.phase === 'open';
  }

  public get closeReadError(): Error | undefined {
    return this.lateReadError;
  }

  public recordCloseReadError(error: unknown): void {
    this.lateReadError ??= wrapError(error);
  }

  public claimReadMode(mode: SubscriptionReadMode): void {
    const mismatch = this.readModeMismatch(mode);
    if (mismatch !== undefined) {
      throw mismatch;
    }
    this.readMode = mode;
  }

  public readModeMismatch(mode: SubscriptionReadMode): TypeError | undefined {
    if (this.readMode !== undefined && this.readMode !== mode) {
      return new TypeError(
        `subscription is already being read as ${this.readMode}; cannot also read as ${mode}`,
      );
    }
    return undefined;
  }

  public observeClose(observer: (owner: object | undefined) => void): void {
    if (this.phase === 'open') {
      this.closeObservers.add(observer);
    } else {
      observer(undefined);
    }
  }

  public beginClose(owner: object): { readonly barrier: Promise<void>; readonly started: boolean } {
    if (this.phase !== 'open') {
      return { barrier: this.closed, started: false };
    }
    this.phase = 'closing';
    for (const observer of this.closeObservers) {
      observer(owner);
    }
    this.closeObservers.clear();
    return { barrier: this.reads.barrier(), started: true };
  }

  public finishNaturalClose(owner: object): void {
    if (this.phase !== 'open') {
      return;
    }
    this.phase = 'closed';
    for (const observer of this.closeObservers) {
      observer(owner);
    }
    this.closeObservers.clear();
    this.resolveClosed();
  }

  public finishClose(error: Error | undefined): void {
    if (this.phase === 'closed') {
      return;
    }
    this.phase = 'closed';
    this.closeError = error;
    this.lateReadError = undefined;
    this.closeObservers.clear();
    this.resolveClosed();
  }

  public async whenClosed(): Promise<void> {
    await this.closed;
    if (this.closeError !== undefined) {
      throw this.closeError;
    }
  }
}

const subscriptionCoordinators = new WeakMap<NativeSubscription, SubscriptionCoordinator>();

function subscriptionCoordinatorFor(inner: NativeSubscription): SubscriptionCoordinator {
  const existing = subscriptionCoordinators.get(inner);
  if (existing !== undefined) {
    return existing;
  }
  const created = new SubscriptionCoordinator(inner);
  subscriptionCoordinators.set(inner, created);
  created.emitWarnings();
  return created;
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new DOMException('The operation was aborted', 'AbortError');
}

function throwIfSubscriptionReadAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw abortReason(signal);
  }
}

async function rejectAfterSubscriptionCleanup(
  primary: Error,
  cleanup: Promise<unknown>,
): Promise<never> {
  try {
    await cleanup;
  } catch (cleanupError) {
    if (cleanupError === primary) {
      throw primary;
    }
    throw new AggregateError(
      [primary, cleanupError],
      `${primary.message}; subscription cleanup failed`,
      { cause: cleanupError },
    );
  }
  throw primary;
}

class SubscriptionIterator<TBatch, TValue> {
  private pending: (TValue | undefined)[] = [];
  private pendingCursor = 0;
  private phase: SubscriptionIteratorPhase = 'open';
  private closeDrain = false;
  private readonly closingPending: TValue[] = [];
  private closeInFlight: Promise<TValue[]> | undefined;
  private readonly owner = {};
  private readonly nextSerializedWithoutSignal = this.nextSerialized.bind(this, undefined);

  public constructor(
    private readonly readBatch: () => Promise<TBatch | null>,
    private readonly closeNative: (drain: boolean) => Promise<readonly TBatch[] | null>,
    private readonly projection: SubscriptionBatchProjection<TBatch, TValue>,
    private readonly coordinator: SubscriptionCoordinator,
    private readonly readMode: SubscriptionReadMode,
  ) {
    this.coordinator.observeClose((owner) => {
      if (owner === this.owner) {
        return;
      }
      this.phase = 'closing';
      this.closeDrain = false;
      this.clearPending();
      this.closingPending.length = 0;
    });
  }

  private isOpen(): boolean {
    return this.phase === 'open';
  }

  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<TValue, undefined>> {
    const signal = options?.signal;
    return signal === undefined ? this.nextWithoutSignal() : this.nextWithSignal(signal);
  }

  private async nextWithoutSignal(): Promise<IteratorResult<TValue, undefined>> {
    this.coordinator.ensureRowDelivery();
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    this.coordinator.claimReadMode(this.readMode);
    const queued = this.coordinator.reads.enqueue(this.nextSerializedWithoutSignal);
    try {
      return await queued;
    } catch (error) {
      return await rejectAfterSubscriptionCleanup(wrapError(error), this.startClose(false));
    }
  }

  private async nextWithSignal(signal: AbortSignal): Promise<IteratorResult<TValue, undefined>> {
    this.coordinator.ensureRowDelivery();
    if (signal.aborted) {
      return await rejectAfterSubscriptionCleanup(abortReason(signal), this.startClose(false));
    }
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    this.coordinator.claimReadMode(this.readMode);

    let abortError: Error | undefined;
    const onAbort = (): void => {
      abortError = abortReason(signal);
      void this.startClose(false).catch(() => undefined);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    const queued = this.coordinator.reads.enqueue(this.nextSerialized.bind(this, signal));
    try {
      return await queued;
    } catch (error) {
      const primary = abortError ?? wrapError(error);
      return await rejectAfterSubscriptionCleanup(primary, this.startClose(false));
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  private async nextSerialized(
    signal: AbortSignal | undefined,
  ): Promise<IteratorResult<TValue, undefined>> {
    throwIfSubscriptionReadAborted(signal);
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    const pending = this.takePending();
    if (pending !== undefined) {
      return { done: false, value: pending };
    }

    while (this.isOpen()) {
      let batch: TBatch | null;
      try {
        batch = await this.readBatch();
      } catch (error) {
        if (!this.isOpen()) {
          this.coordinator.recordCloseReadError(error);
          throwIfSubscriptionReadAborted(signal);
          return { done: true, value: undefined };
        }
        throwIfSubscriptionReadAborted(signal);
        throw error;
      } finally {
        this.coordinator.emitWarnings();
      }

      if (!this.isOpen()) {
        if (batch !== null && this.closeDrain) {
          try {
            this.appendBatch(this.closingPending, batch);
          } catch (error) {
            this.coordinator.recordCloseReadError(error);
          }
        }
        throwIfSubscriptionReadAborted(signal);
        return { done: true, value: undefined };
      }
      throwIfSubscriptionReadAborted(signal);
      if (batch === null) {
        this.phase = 'closed';
        this.clearPending();
        this.coordinator.finishNaturalClose(this.owner);
        return { done: true, value: undefined };
      }

      if (this.projection.cardinality === 'one') {
        return { done: false, value: this.projection.project(batch) };
      }
      const values = this.projection.project(batch);
      if (values.length === 0) {
        continue;
      }

      this.pending = values;
      this.pendingCursor = 0;
      const value = this.takePending();
      if (value !== undefined) {
        return { done: false, value };
      }
    }

    return { done: true, value: undefined };
  }

  private appendBatch(target: TValue[], batch: TBatch): void {
    if (this.projection.cardinality === 'one') {
      target.push(this.projection.project(batch));
      return;
    }
    for (const value of this.projection.project(batch)) {
      target.push(value);
    }
  }

  private takePending(): TValue | undefined {
    const value = this.pending[this.pendingCursor];
    if (value === undefined) {
      this.clearPending();
      return undefined;
    }
    this.pending[this.pendingCursor] = undefined;
    this.pendingCursor += 1;
    if (this.pendingCursor === this.pending.length) {
      this.pending = [];
      this.pendingCursor = 0;
    }
    return value;
  }

  private drainPendingToClosing(): void {
    for (let index = this.pendingCursor; index < this.pending.length; index += 1) {
      const value = this.pending[index];
      this.pending[index] = undefined;
      if (value !== undefined) {
        this.closingPending.push(value);
      }
    }
    this.pending = [];
    this.pendingCursor = 0;
  }

  private clearPending(): void {
    this.pending.fill(undefined);
    this.pending = [];
    this.pendingCursor = 0;
  }

  public unsubscribe(drain = false): Promise<TValue[]> {
    return this.startClose(drain);
  }

  private async startClose(drain: boolean): Promise<TValue[]> {
    const formatError = drain ? this.coordinator.readModeMismatch(this.readMode) : undefined;
    if (formatError !== undefined) {
      const cleanup = this.coordinator.isOpen ? this.startClose(false) : this.waitForSharedClose();
      return await rejectAfterSubscriptionCleanup(formatError, cleanup);
    }
    if (drain) {
      this.coordinator.claimReadMode(this.readMode);
    }
    if (this.closeInFlight !== undefined) {
      return await this.closeInFlight;
    }
    if (!this.coordinator.isOpen) {
      return await this.waitForSharedClose();
    }

    this.phase = 'closing';
    this.closeDrain = drain;
    if (drain) {
      this.drainPendingToClosing();
    } else {
      this.clearPending();
    }

    const { barrier: readBarrier, started } = this.coordinator.beginClose(this.owner);
    if (!started) {
      return await this.waitForSharedClose();
    }

    let nativeClose: Promise<readonly TBatch[] | null>;
    try {
      nativeClose = this.closeNative(drain);
    } catch (error) {
      nativeClose = Promise.reject(wrapError(error));
    }
    const closePromise = (async (): Promise<TValue[]> => {
      let nativeBatches: readonly TBatch[] | null = null;
      let nativeError: Error | undefined;
      let closeError: Error | undefined;
      try {
        try {
          nativeBatches = await nativeClose;
        } catch (error) {
          nativeError = wrapError(error);
        }
        await readBarrier;

        const readError = this.coordinator.closeReadError;
        if (nativeError !== undefined && readError !== undefined && nativeError !== readError) {
          const cleanupError = wrapError(nativeError);
          closeError = new AggregateError(
            [wrapError(readError), cleanupError],
            `${readError.message}; subscription cleanup failed`,
            { cause: cleanupError },
          );
          throw closeError;
        }
        if (nativeError !== undefined) {
          throw nativeError;
        }
        if (readError !== undefined) {
          throw readError;
        }
        if (!drain) {
          return [];
        }
        const drained = [...this.closingPending];
        for (const batch of nativeBatches ?? []) {
          this.appendBatch(drained, batch);
        }
        return drained;
      } catch (error) {
        closeError ??= wrapError(error);
        throw closeError;
      } finally {
        this.phase = 'closed';
        this.closeDrain = false;
        this.clearPending();
        this.closingPending.length = 0;
        this.coordinator.finishClose(closeError);
        this.coordinator.emitWarnings();
      }
    })();
    this.closeInFlight = closePromise;
    const releaseClose = (): void => {
      if (this.closeInFlight === closePromise) {
        this.closeInFlight = undefined;
      }
    };
    void closePromise.then(releaseClose, releaseClose);
    return await closePromise;
  }

  private async waitForSharedClose(): Promise<TValue[]> {
    try {
      await this.coordinator.whenClosed();
      return [];
    } finally {
      this.phase = 'closed';
      this.closeDrain = false;
      this.clearPending();
      this.closingPending.length = 0;
    }
  }
}
export class ArrowSubscription
  implements
    AsyncIterator<Table, undefined, SubscriptionReadOptions | undefined>,
    AsyncIterable<Table>
{
  private readonly iterator: SubscriptionIterator<NativeArrowZeroCopyBatch, Table>;

  public constructor(inner: NativeSubscription) {
    const coordinator = subscriptionCoordinatorFor(inner);
    this.iterator = new SubscriptionIterator(
      inner.nextArrowBatch.bind(inner),
      inner.unsubscribeArrow.bind(inner),
      { cardinality: 'one', project: toArrowTableFromNative },
      coordinator,
      'arrow',
    );
  }

  /** Return the next zero-copy Arrow table; one native crossing may contain many rows. */
  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<Table, undefined>> {
    return this.iterator.next(options);
  }

  public unsubscribe(drain = false): Promise<Table[]> {
    return this.iterator.unsubscribe(drain);
  }

  public async return(): Promise<IteratorResult<Table, undefined>> {
    await this.unsubscribe(false);
    return { done: true, value: undefined };
  }

  public [Symbol.asyncIterator](): this {
    return this;
  }
}

export class Subscription
  implements
    AsyncIterator<Tick, undefined, SubscriptionReadOptions | undefined>,
    AsyncIterable<Tick>
{
  private readonly layouts = new Map<number, TickLayout>();
  private readonly coordinator: SubscriptionCoordinator;
  private readonly iterator: SubscriptionIterator<NativeSubscriptionUpdateBatch, Tick>;
  private arrowView: ArrowSubscription | undefined;

  public constructor(private readonly inner: NativeSubscription) {
    this.coordinator = subscriptionCoordinatorFor(this.inner);
    this.iterator = new SubscriptionIterator(
      this.inner.nextUpdates.bind(this.inner),
      this.inner.unsubscribe.bind(this.inner),
      { cardinality: 'many', project: (batch) => this.ticksFromBatch(batch) },
      this.coordinator,
      'scalar',
    );
  }

  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<Tick, undefined>> {
    return this.iterator.next(options);
  }

  private ticksFromBatch(batch: NativeSubscriptionUpdateBatch): Tick[] {
    if (batch.layout !== undefined) {
      this.layouts.set(batch.layout.version, createTickLayout(batch.layout));
    }
    return batch.updates.map((update) => {
      const layout = this.layouts.get(update.layoutVersion);
      if (layout === undefined) {
        throw new Error(`subscription layout ${update.layoutVersion} was not supplied by native`);
      }
      return new Tick(update, layout);
    });
  }

  public async add(tickers: readonly string[], aliases?: Record<string, string>): Promise<void> {
    validateAliases(aliases);
    try {
      await this.inner.add(tickers, aliases);
    } catch (error) {
      throw wrapError(error);
    } finally {
      this.coordinator.emitWarnings();
    }
  }

  /** Grow this consumer's projection and the shared feed's field union. */
  public async addFields(fields: readonly string[]): Promise<void> {
    try {
      await this.inner.addFields(fields);
    } catch (error) {
      throw wrapError(error);
    } finally {
      this.coordinator.emitWarnings();
    }
  }

  /** Materialized last values, independent of the scalar/Arrow iterator read mode. */
  public latest(options: RecipeBackendOptions = {}): unknown {
    try {
      return nativeArrowToBackend(this.inner.latest(), options.backend);
    } catch (error) {
      throw wrapError(error);
    } finally {
      this.coordinator.emitWarnings();
    }
  }

  public async remove(tickers: readonly string[]): Promise<void> {
    try {
      await this.inner.remove(tickers);
    } catch (error) {
      throw wrapError(error);
    } finally {
      this.coordinator.emitWarnings();
    }
  }

  public unsubscribe(drain = false): Promise<Tick[]> {
    return this.iterator.unsubscribe(drain);
  }

  public async return(): Promise<IteratorResult<Tick, undefined>> {
    await this.unsubscribe(false);
    return { done: true, value: undefined };
  }

  public field(name: string): FieldHandle {
    return new FieldHandle(name);
  }

  public arrow(): ArrowSubscription {
    this.arrowView ??= new ArrowSubscription(this.inner);
    return this.arrowView;
  }

  public get tickers(): string[] {
    return this.inner.tickers;
  }

  public get fields(): string[] {
    return this.inner.fields;
  }

  public get isActive(): boolean {
    return this.inner.isActive;
  }

  public get stats(): SubscriptionStats {
    return this.inner.stats;
  }

  public get status(): SubscriptionStatus {
    return this.inner.status;
  }

  public get events(): SubscriptionEvent[] {
    return this.inner.events;
  }

  public get failures(): SubscriptionFailure[] {
    return this.inner.failures;
  }

  public get failedTickers(): string[] {
    return this.inner.failedTickers;
  }

  public get topicStates(): Record<string, TopicState> {
    return this.inner.topicStates;
  }

  public get fieldErrors(): Record<string, Record<string, string>> {
    return this.inner.fieldErrors;
  }

  public get sessionStatus(): SessionStatus {
    return this.inner.sessionStatus;
  }

  public get adminStatus(): AdminStatus {
    return this.inner.adminStatus;
  }

  public get serviceStatus(): Record<string, ServiceStatus> {
    return this.inner.serviceStatus;
  }

  public [Symbol.asyncIterator](): this {
    return this;
  }
}
