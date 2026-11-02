import { tableToIPC } from 'apache-arrow';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import type {
  NativeArrowColumn,
  NativeArrowIpcFile,
  NativeArrowZeroCopyBatch,
  NativeSubscription,
  NativeSubscriptionDeliveryReceipt,
  NativeSubscriptionLayout,
  NativeSubscriptionUpdateBatch,
  NativeSubscriptionValue,
} from '../src/napi';
import type { SubscriptionStatus } from '../src/types';

export function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

export function jsonValue(value: unknown): unknown {
  const serialized = JSON.stringify(value);
  return serialized === undefined ? undefined : JSON.parse(serialized);
}

/** One scalar row per value: field 0 (`answer`, i32) carries the value. */
export function scalarBatch(
  values: readonly number[],
  includeLayout = true,
): NativeSubscriptionUpdateBatch {
  return {
    kind: 'batch',
    ...(includeLayout
      ? { layout: { fields: ['answer'], kinds: ['i32'] as const, version: 1 } }
      : {}),
    layoutVersion: 1,
    topics: values.map((value) => `topic-${value}`),
    timestampsUs: Float64Array.from(values),
    rowOffsets: Uint32Array.from({ length: values.length + 1 }, (_, row) => row),
    fieldIndices: new Uint16Array(values.length),
    values,
  };
}

/** One row at timestamp 123 on `topic-1`, with present fields in update order. */
export function scalarRowBatch(
  layout: NativeSubscriptionLayout,
  fieldIndices: readonly number[],
  values: readonly NativeSubscriptionValue[],
): NativeSubscriptionUpdateBatch {
  return {
    kind: 'batch',
    layout,
    layoutVersion: layout.version,
    topics: ['topic-1'],
    timestampsUs: Float64Array.of(123),
    rowOffsets: Uint32Array.of(0, fieldIndices.length),
    fieldIndices: Uint16Array.from(fieldIndices),
    values,
  };
}

/** The file IPC native code produces for the Polars path, encoded here in JS. */
export function ipcFileFromBatch(batch: NativeArrowZeroCopyBatch): NativeArrowIpcFile {
  return {
    kind: 'ipcFile',
    ipc: Buffer.from(tableToIPC(tableFromNativeArrowBatch(batch), 'file')),
    metadata: batch.metadata,
  };
}

export function int32ArrowBatch(
  value: number | null,
  metadata: Record<string, string> = {},
  name = 'answer',
): NativeArrowZeroCopyBatch {
  return {
    columns: [
      {
        name,
        type: 'int32',
        nullable: value === null,
        length: 1,
        nullCount: value === null ? 1 : 0,
        data: typedBuffer(new Int32Array([value ?? 0])),
        ...(value === null ? { nullBitmap: typedBuffer(new Uint8Array([0])) } : {}),
      },
    ],
    kind: 'zeroCopy',
    metadata,
    numRows: 1,
  };
}

export function emptyFieldInfoArrowBatch(
  metadata: Record<string, string>,
): NativeArrowZeroCopyBatch {
  return {
    columns: ['field', 'type', 'description', 'category', 'id'].map((name): NativeArrowColumn => ({
      name,
      type: 'utf8',
      nullable: name === 'description' || name === 'category',
      length: 0,
      nullCount: 0,
      data: Buffer.alloc(0),
      offsets: typedBuffer(new Int32Array([0])),
    })),
    kind: 'zeroCopy',
    metadata,
    numRows: 0,
  };
}

export function fakeNativeSubscription(
  overrides: Partial<NativeSubscription> = {},
): NativeSubscription {
  const status: SubscriptionStatus = {
    events: [],
    failures: [],
    failedTickers: [],
    topicStates: {},
    fieldErrors: {},
    session: { state: 'up', lastChangeUs: 0, disconnectCount: 0, reconnectCount: 0 },
    services: {},
    admin: {
      slowConsumerWarningActive: false,
      slowConsumerWarningCount: 0,
      slowConsumerClearedCount: 0,
      dataLossCount: 0,
    },
  };
  return {
    add: async () => undefined,
    addFields: async () => undefined,
    latest: () => int32ArrowBatch(7),
    takeWarnings: () => [],
    signalClosing: () => undefined,
    status,
    events: status.events,
    failures: status.failures,
    failedTickers: status.failedTickers,
    topicStates: status.topicStates,
    fieldErrors: status.fieldErrors,
    sessionStatus: status.session,
    serviceStatus: status.services,
    adminStatus: status.admin,
    fields: ['answer'],
    isActive: true,
    deliversRows: true,
    delivery: 'deltas',
    requiresDeliveries: false,
    nextArrowBatch: async () => null,
    nextUpdates: async () => null,
    nextDelivery: async () => null,
    nextIpcFileDelivery: async () => null,
    remove: async () => undefined,
    stats: {
      batchesSent: 0,
      droppedBatches: 0,
      messagesReceived: 0,
      slowConsumer: false,
    },
    tickers: ['topic-1'],
    unsubscribe: async () => null,
    unsubscribeArrow: async () => null,
    unsubscribeDeliveries: async () => null,
    unsubscribeIpcFileDeliveries: async () => null,
    ...overrides,
  };
}

export function deliveryReceipt<T>(
  value: T,
  commit: () => void = () => undefined,
  rollback: () => void = () => undefined,
): NativeSubscriptionDeliveryReceipt<T> {
  let settled = false;
  return {
    kind: 'delivery',
    value,
    commit: () => {
      if (settled) throw new Error('delivery receipt already settled');
      settled = true;
      commit();
      return true;
    },
    rollback: () => {
      if (settled) return;
      settled = true;
      rollback();
    },
  };
}
