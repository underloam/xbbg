import type {
  NativeArrowColumn,
  NativeArrowZeroCopyBatch,
  NativeSubscription,
  NativeSubscriptionDeliveryReceipt,
  NativeSubscriptionUpdateBatch,
} from '../src/napi';
import type { SubscriptionStatus } from '../src/types';

export function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

export function jsonValue(value: unknown): unknown {
  const serialized = JSON.stringify(value);
  return serialized === undefined ? undefined : JSON.parse(serialized);
}

export function scalarBatch(
  values: readonly number[],
  includeLayout = true,
): NativeSubscriptionUpdateBatch {
  return {
    kind: 'batch',
    ...(includeLayout
      ? { layout: { fields: ['answer'], kinds: ['i32'] as const, version: 1 } }
      : {}),
    updates: values.map((value) => ({
      boolValues: [null],
      f64Values: [null],
      fieldIndices: [0],
      i32Values: [value],
      i64Values: [null],
      layoutVersion: 1,
      stringValues: [null],
      timestampUs: value,
      topic: `topic-${value}`,
      topicId: value,
    })),
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
