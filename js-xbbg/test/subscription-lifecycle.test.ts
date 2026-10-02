import type { Table } from 'apache-arrow';

import { DataType, DateUnit, TimeUnit } from 'apache-arrow';

import {
  NATIVE_ARROW_LAYOUT_CACHE_LIMIT,
  nativeArrowLayoutCacheSize,
  tableFromNativeArrowBatch,
} from '../src/arrow-zero-copy';
import * as api from '../src/index';
import type {
  NativeArrowColumn,
  NativeEngine,
  NativeArrowZeroCopyBatch,
  NativeSubscription,
  NativeSubscriptionUpdateBatch,
} from '../src/napi';

// Exercise the public wrappers while replacing only their eager native-addon load.
// The resolver mock covers native-free installs; the require mock also isolates local builds.
vi.mock(import('node:module'), async (importOriginal) => {
  const actual = await importOriginal();
  const nativeAddon = {
    JsEngine: vi.fn<() => never>(() => {
      throw new Error('subscription lifecycle tests must not instantiate the native engine');
    }),
    extAuctionFieldGroup: (name: string) =>
      name === 'default'
        ? ['THEO_PRICE', 'INDICATIVE_NEAR', 'ORDER_IMB_BUY_VOLUME']
        : [`${name.toUpperCase()}_FIELD`],
    extAuctionZeroPriceFields: () => ['THEO_PRICE', 'INDICATIVE_NEAR', 'REFERENCE_PRICE_RT'],
    extImbalanceSide: () => null,
    getLogLevel: () => 'off',
    setLogLevel: () => undefined,
  };

  return {
    ...actual,
    createRequire: (...args: Parameters<typeof actual.createRequire>) => {
      const nodeRequire = actual.createRequire(...args);
      return new Proxy(nodeRequire, {
        apply(target, thisArgument, argumentsList) {
          const moduleId: unknown = argumentsList[0];
          if (typeof moduleId === 'string' && /(?:^|[/\\])napi[-_]xbbg\.node$/u.test(moduleId)) {
            return nativeAddon;
          }
          return Reflect.apply(target, thisArgument, argumentsList);
        },
      });
    },
  };
});

vi.mock(import('../src/native/resolve-native.js'), () => ({
  resolveNativeAddon: () => ({
    binaryPath: 'napi_xbbg.node',
    key: 'test',
    packageName: '@xbbg/core-test',
  }),
}));

function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

function scalarBatch(
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

function int32ArrowBatch(
  value: number,
  metadata: Record<string, string> = {},
  name = 'answer',
): NativeArrowZeroCopyBatch {
  return {
    columns: [
      {
        name,
        type: 'int32',
        nullable: false,
        length: 1,
        nullCount: 0,
        data: typedBuffer(new Int32Array([value])),
      },
    ],
    kind: 'zeroCopy',
    metadata,
    numRows: 1,
  };
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let settle!: (value: T) => void;
  const promise = new Promise<T>((resolve) => {
    settle = resolve;
  });
  return { promise, resolve: settle };
}

function deferredSignal(): {
  readonly promise: Promise<undefined>;
  readonly resolve: () => void;
} {
  let settle!: () => void;
  const promise = new Promise<undefined>((resolve) => {
    settle = () => {
      resolve(undefined);
    };
  });
  return { promise, resolve: settle };
}

function fakeNativeSubscription(overrides: Partial<NativeSubscription> = {}): NativeSubscription {
  const status: api.SubscriptionStatus = {
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
    nextArrowBatch: async () => null,
    nextUpdates: async () => null,
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
    ...overrides,
  };
}

describe('subscription iterator lifecycle', () => {
  it('closes and discards buffered scalar ticks when for-await exits early', async () => {
    const closeCalls: boolean[] = [];
    const native = fakeNativeSubscription({
      nextUpdates: async () => scalarBatch([1, 2]),
      unsubscribe: async (drain) => {
        closeCalls.push(drain);
        return null;
      },
    });
    const subscription = new api.Subscription(native);
    const received: number[] = [];

    for await (const tick of subscription) {
      const value = tick.f64('answer') ?? -1;
      received.push(value);
      if (value === 1) {
        break;
      }
    }

    expect(received).toStrictEqual([1]);
    expect(closeCalls).toStrictEqual([false]);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('closes the Arrow subscription when for-await exits early', async () => {
    const closeCalls: boolean[] = [];
    const native = fakeNativeSubscription({
      nextArrowBatch: async () => int32ArrowBatch(7),
      unsubscribeArrow: async (drain) => {
        closeCalls.push(drain);
        return null;
      },
    });
    const subscription = new api.ArrowSubscription(native);
    const received: number[] = [];

    for await (const table of subscription) {
      const value = table.getChild('answer')?.get(0) ?? -1;
      received.push(value);
      if (value === 7) {
        break;
      }
    }

    expect(received).toStrictEqual([7]);
    expect(closeCalls).toStrictEqual([false]);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('drains buffered JS ticks before later native ticks', async () => {
    const native = fakeNativeSubscription({
      nextUpdates: async () => scalarBatch([1, 2]),
      unsubscribe: async (drain) => (drain ? [scalarBatch([3], false)] : null),
    });
    const subscription = new api.Subscription(native);

    const first = await subscription.next();
    const drained = await subscription.unsubscribe(true);

    expect(first.value?.f64('answer')).toBe(1);
    expect(drained.map((tick) => tick.f64('answer'))).toStrictEqual([2, 3]);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('preserves absent fields separately from explicit clears in canonical and typed accessors', async () => {
    const native = fakeNativeSubscription({
      nextUpdates: async () => ({
        kind: 'batch',
        layout: {
          fields: ['present', 'cleared', 'absent'],
          kinds: ['i32', 'i32', 'i32'],
          version: 1,
        },
        updates: [
          {
            boolValues: [null, null],
            f64Values: [null, null],
            fieldIndices: [0, 1],
            i32Values: [7, null],
            i64Values: [null, null],
            layoutVersion: 1,
            stringValues: [null, null],
            timestampUs: 123,
            topic: 'topic-1',
            topicId: 1,
          },
        ],
      }),
    });
    const subscription = new api.Subscription(native);
    const tick = (await subscription.next()).value;
    if (tick === undefined) {
      throw new Error('expected tick');
    }

    expect(tick.has('present')).toBeTruthy();
    expect(tick.has('cleared')).toBeTruthy();
    expect(tick.has('absent')).toBeFalsy();
    expect(tick.has('unknown')).toBeFalsy();
    expect(tick.has(new api.FieldHandle('present'))).toBeTruthy();
    expect(tick.get('present')).toBe(7);
    expect(tick.get('cleared')).toBeNull();
    expect(tick.get('absent')).toBeUndefined();
    expect(tick.get('unknown')).toBeUndefined();
    expect(tick.f64('cleared')).toBeNull();
    expect(tick.f64('absent')).toBeUndefined();
    expect(tick.i64('cleared')).toBeNull();
    expect(tick.i64('absent')).toBeUndefined();
    expect(tick.str('cleared')).toBeNull();
    expect(tick.str('absent')).toBeUndefined();
    expect(tick.toObject()).toStrictEqual({
      cleared: null,
      present: 7,
      timestampUs: 123,
      topic: 'topic-1',
    });
    await subscription.unsubscribe(false);
  });

  it('decodes promoted boolean and numeric values from native string slots', async () => {
    const native = fakeNativeSubscription({
      nextUpdates: async () => ({
        kind: 'batch',
        layout: {
          fields: ['flag', 'count', 'price'],
          kinds: ['str', 'str', 'str'],
          version: 1,
        },
        updates: [
          {
            boolValues: [null, null, null],
            f64Values: [null, null, null],
            fieldIndices: [0, 1, 2],
            i32Values: [null, null, null],
            i64Values: [null, null, null],
            layoutVersion: 1,
            stringValues: ['true', '42', '1.25'],
            timestampUs: 123,
            topic: 'topic-1',
            topicId: 1,
          },
        ],
      }),
    });
    const subscription = new api.Subscription(native);
    const tick = (await subscription.next()).value;
    if (tick === undefined) {
      throw new Error('expected tick');
    }

    expect(tick.get('flag')).toBe('true');
    expect(tick.get('count')).toBe('42');
    expect(tick.get('price')).toBe('1.25');
    expect(tick.f64('price')).toBe(1.25);
    expect(tick.i64('count')).toBe(42n);
    await subscription.unsubscribe(false);
  });

  it('includes an in-flight batch in drain without delivering it after close', async () => {
    const started = deferredSignal();
    const pendingRead = deferred<NativeSubscriptionUpdateBatch | null>();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        return await pendingRead.promise;
      },
      unsubscribe: async (drain) => {
        pendingRead.resolve(scalarBatch([2]));
        return drain ? [scalarBatch([3], false)] : null;
      },
    });
    const subscription = new api.Subscription(native);

    const next = subscription.next();
    await started.promise;
    const close = subscription.unsubscribe(true);

    await expect(next).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(
      close.then((ticks) => ticks.map((tick) => tick.f64('answer'))),
    ).resolves.toStrictEqual([2, 3]);
  });

  it('aborting a scalar read closes native work and leaves no busy read behind', async () => {
    const started = deferredSignal();
    const pendingRead = deferred<NativeSubscriptionUpdateBatch | null>();
    const closeCalls: boolean[] = [];
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        return await pendingRead.promise;
      },
      unsubscribe: async (drain) => {
        closeCalls.push(drain);
        pendingRead.resolve(scalarBatch([9]));
        return null;
      },
    });
    const subscription = new api.Subscription(native);
    const controller = new AbortController();

    const next = subscription.next({ signal: controller.signal });
    await started.promise;
    controller.abort();

    await expect(next).rejects.toMatchObject({ name: 'AbortError' });
    await expect(subscription.unsubscribe(false)).resolves.toStrictEqual([]);
    expect(closeCalls).toStrictEqual([false]);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('aborting an Arrow read closes the Arrow native path', async () => {
    const started = deferredSignal();
    const pendingRead = deferred<NativeArrowZeroCopyBatch | null>();
    const closeCalls: boolean[] = [];
    const native = fakeNativeSubscription({
      nextArrowBatch: async () => {
        started.resolve();
        return await pendingRead.promise;
      },
      unsubscribeArrow: async (drain) => {
        closeCalls.push(drain);
        pendingRead.resolve(int32ArrowBatch(9));
        return null;
      },
    });
    const subscription = new api.ArrowSubscription(native);
    const controller = new AbortController();

    const next = subscription.next({ signal: controller.signal });
    await started.promise;
    controller.abort();

    await expect(next).rejects.toMatchObject({ name: 'AbortError' });
    await expect(subscription.unsubscribe(false)).resolves.toStrictEqual([]);
    expect(closeCalls).toStrictEqual([false]);
  });

  it('serializes concurrent scalar reads without changing tick order', async () => {
    const gates = [
      deferred<NativeSubscriptionUpdateBatch | null>(),
      deferred<NativeSubscriptionUpdateBatch | null>(),
    ] as const;
    const starts = [deferredSignal(), deferredSignal()] as const;
    let active = false;
    let readIndex = 0;
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        if (active) {
          throw new Error('subscription receiver busy');
        }
        active = true;
        const index = readIndex;
        readIndex += 1;
        starts[index]?.resolve();
        try {
          const gate = gates[index];
          if (gate === undefined) {
            throw new Error('unexpected native read');
          }
          return await gate.promise;
        } finally {
          active = false;
        }
      },
    });
    const subscription = new api.Subscription(native);

    const first = subscription.next();
    const second = subscription.next();
    await starts[0].promise;
    expect(readIndex).toBe(1);
    gates[0].resolve(scalarBatch([1]));
    await expect(first.then((result) => result.value?.f64('answer'))).resolves.toBe(1);
    await starts[1].promise;
    gates[1].resolve(scalarBatch([2], false));
    await expect(second.then((result) => result.value?.f64('answer'))).resolves.toBe(2);
    await subscription.unsubscribe(false);
  });

  it('rejects switching read formats before a second native receive can steal ordering', async () => {
    let arrowReads = 0;
    const native = fakeNativeSubscription({
      nextArrowBatch: async () => {
        arrowReads += 1;
        return int32ArrowBatch(2);
      },
      nextUpdates: async () => scalarBatch([1]),
    });
    const subscription = new api.Subscription(native);

    await expect(subscription.next().then((result) => result.value?.f64('answer'))).resolves.toBe(
      1,
    );
    await expect(subscription.arrow().next()).rejects.toThrow(
      /already being read as scalar; cannot also read as arrow/u,
    );
    expect(arrowReads).toBe(0);
    await subscription.unsubscribe(false);
  });

  it('rejects a cross-format drain but still closes and discards buffered rows', async () => {
    const arrowCloseCalls: boolean[] = [];
    const native = fakeNativeSubscription({
      nextUpdates: async () => scalarBatch([1, 2]),
      unsubscribeArrow: async (drain) => {
        arrowCloseCalls.push(drain);
        return null;
      },
    });
    const subscription = new api.Subscription(native);

    await subscription.next();
    await expect(subscription.arrow().unsubscribe(true)).rejects.toThrow(
      /already being read as scalar; cannot also read as arrow/u,
    );

    expect(arrowCloseCalls).toStrictEqual([false]);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('keeps an owned batch in the first drain when that read is then aborted', async () => {
    const started = deferredSignal();
    const pendingRead = deferred<NativeSubscriptionUpdateBatch | null>();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        return await pendingRead.promise;
      },
      unsubscribe: async (drain) => (drain ? [scalarBatch([3], false)] : null),
    });
    const subscription = new api.Subscription(native);
    const controller = new AbortController();

    const next = subscription.next({ signal: controller.signal });
    await started.promise;
    const close = subscription.unsubscribe(true);
    controller.abort();
    pendingRead.resolve(scalarBatch([2]));

    await expect(next).rejects.toMatchObject({ name: 'AbortError' });
    await expect(
      close.then((ticks) => ticks.map((tick) => tick.f64('answer'))),
    ).resolves.toStrictEqual([2, 3]);
  });

  it('closes native work after a scalar batch decoding failure', async () => {
    let closeCalls = 0;
    const native = fakeNativeSubscription({
      nextUpdates: async () => scalarBatch([1], false),
      unsubscribe: async () => {
        closeCalls += 1;
        return null;
      },
    });
    const subscription = new api.Subscription(native);

    await expect(subscription.next()).rejects.toThrow(/layout 1 was not supplied/u);
    expect(closeCalls).toBe(1);
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('normalizes one undefined close rejection across scalar and Arrow views', async () => {
    let arrowCloseCalls = 0;
    const rejectClose = vi.fn<NativeSubscription['unsubscribe']>().mockRejectedValue(undefined);
    const native = fakeNativeSubscription({
      unsubscribe: rejectClose,
      unsubscribeArrow: async () => {
        arrowCloseCalls += 1;
        return null;
      },
    });
    const subscription = new api.Subscription(native);
    const arrow = subscription.arrow();

    const scalarClose = subscription.unsubscribe(false);
    const arrowClose = arrow.unsubscribe(false);
    const [scalarError, arrowError] = await Promise.all([
      scalarClose.then(
        () => null,
        (error: unknown) => error,
      ),
      arrowClose.then(
        () => null,
        (error: unknown) => error,
      ),
    ]);

    expect(scalarError).toBeInstanceOf(api.BlpError);
    expect(arrowError).toBe(scalarError);
    expect(rejectClose).toHaveBeenCalledTimes(1);
    expect(arrowCloseCalls).toBe(0);
  });

  it('keeps read format validation sticky while close is in flight', async () => {
    const closeStarted = deferredSignal();
    const allowClose = deferredSignal();
    let arrowCloseCalls = 0;
    const native = fakeNativeSubscription({
      nextUpdates: async () => scalarBatch([1]),
      unsubscribe: async () => {
        closeStarted.resolve();
        await allowClose.promise;
        return null;
      },
      unsubscribeArrow: async () => {
        arrowCloseCalls += 1;
        return null;
      },
    });
    const subscription = new api.Subscription(native);
    await subscription.next();

    const scalarClose = subscription.unsubscribe(false);
    await closeStarted.promise;
    const arrowDrain = subscription.arrow().unsubscribe(true);
    allowClose.resolve();

    await expect(scalarClose).resolves.toStrictEqual([]);
    await expect(arrowDrain).rejects.toThrow(
      /already being read as scalar; cannot also read as arrow/u,
    );
    expect(arrowCloseCalls).toBe(0);
  });

  it('reports abort and cleanup failure together after cleanup settles', async () => {
    const started = deferredSignal();
    const pendingRead = deferred<NativeSubscriptionUpdateBatch | null>();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        return await pendingRead.promise;
      },
      unsubscribe: async () => {
        pendingRead.resolve(null);
        throw new Error('abort cleanup failed');
      },
    });
    const subscription = new api.Subscription(native);
    const controller = new AbortController();

    const next = subscription.next({ signal: controller.signal });
    await started.promise;
    controller.abort();
    const error = await next.then(
      () => null,
      (raised: unknown) => raised,
    );

    expect(error).toBeInstanceOf(AggregateError);
    const aggregate = error as AggregateError;
    expect(aggregate.errors[0]).toMatchObject({ name: 'AbortError' });
    expect(aggregate.errors[1]).toMatchObject({ message: 'abort cleanup failed' });
    expect(aggregate.cause).toBe(aggregate.errors[1]);
    await expect(subscription.unsubscribe(false)).rejects.toBe(aggregate.errors[1]);
  });

  it('reports cleanup failure for a pre-aborted read', async () => {
    const native = fakeNativeSubscription({
      unsubscribe: async () => {
        throw new Error('pre-abort cleanup failed');
      },
    });
    const subscription = new api.Subscription(native);
    const controller = new AbortController();
    controller.abort();

    const error = await subscription.next({ signal: controller.signal }).then(
      () => null,
      (raised: unknown) => raised,
    );

    expect(error).toBeInstanceOf(AggregateError);
    const aggregate = error as AggregateError;
    expect(aggregate.errors[0]).toMatchObject({ name: 'AbortError' });
    expect(aggregate.errors[1]).toMatchObject({ message: 'pre-abort cleanup failed' });
  });

  it('propagates an in-flight read failure to an earlier drain close', async () => {
    const started = deferredSignal();
    const failRead = deferredSignal();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        await failRead.promise;
        throw new Error('in-flight read failed');
      },
      unsubscribe: async () => null,
    });
    const subscription = new api.Subscription(native);

    const next = subscription.next();
    await started.promise;
    const close = subscription.unsubscribe(true);
    failRead.resolve();

    await expect(next).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(close).rejects.toThrow('in-flight read failed');
  });

  it('shares a late read failure with an opposite-view close', async () => {
    const started = deferredSignal();
    const failRead = deferredSignal();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        await failRead.promise;
        throw new Error('late scalar read failed');
      },
    });
    const subscription = new api.Subscription(native);
    const arrow = subscription.arrow();

    const next = subscription.next();
    await started.promise;
    const close = arrow.unsubscribe(false);
    failRead.resolve();
    const error = await close.then(
      () => null,
      (raised: unknown) => raised,
    );

    expect(error).toMatchObject({ message: 'late scalar read failed' });
    await expect(subscription.unsubscribe(false)).rejects.toBe(error);
    await expect(next).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('preserves both a late read failure and native close failure across views', async () => {
    const started = deferredSignal();
    const failRead = deferredSignal();
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        await failRead.promise;
        throw new Error('late read failed');
      },
      unsubscribe: async () => {
        throw new Error('native close failed');
      },
    });
    const subscription = new api.Subscription(native);
    const arrow = subscription.arrow();

    const next = subscription.next();
    await started.promise;
    const close = subscription.unsubscribe(false);
    const sharedClose = arrow.unsubscribe(false);
    failRead.resolve();
    const [error, sharedError] = await Promise.all([
      close.then(
        () => null,
        (raised: unknown) => raised,
      ),
      sharedClose.then(
        () => null,
        (raised: unknown) => raised,
      ),
    ]);

    expect(error).toBeInstanceOf(AggregateError);
    const aggregate = error as AggregateError;
    expect(aggregate.errors).toMatchObject([
      { message: 'late read failed' },
      { message: 'native close failed' },
    ]);
    expect(aggregate.cause).toBe(aggregate.errors[1]);
    expect(sharedError).toBe(error);
    await expect(next).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('reports one shared Error instance only once during close', async () => {
    const started = deferredSignal();
    const failRead = deferredSignal();
    const failure = new Error('shared native failure');
    const native = fakeNativeSubscription({
      nextUpdates: async () => {
        started.resolve();
        await failRead.promise;
        throw failure;
      },
      unsubscribe: async () => {
        throw failure;
      },
    });
    const subscription = new api.Subscription(native);

    const next = subscription.next();
    await started.promise;
    const close = subscription.unsubscribe(false);
    failRead.resolve();
    const error = await close.then(
      () => null,
      (raised: unknown) => raised,
    );

    expect(error).toBeInstanceOf(api.BlpError);
    expect(error).toMatchObject({ message: 'shared native failure' });
    await expect(next).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('rejects an errors-only scalar drain with structured subscription data-loss details', async () => {
    const native = fakeNativeSubscription({
      unsubscribe: async () => {
        throw new Error(
          '[XBBG:DATALOSS] Subscription data loss [topic=IBM US Equity]: stream queue reached capacity',
        );
      },
    });
    const subscription = new api.Subscription(native);

    const error = await subscription.unsubscribe(true).then(
      () => null,
      (raised: unknown) => raised,
    );

    expect(error).toBeInstanceOf(api.BlpSubscriptionDataLossError);
    expect(error).toMatchObject({
      code: 'DATALOSS',
      detail: 'stream queue reached capacity',
      topic: 'IBM US Equity',
    });
  });

  it('uses the Arrow native close path when an unread Arrow drain fails', async () => {
    const scalarClose = vi.fn<NativeSubscription['unsubscribe']>();
    const arrowClose = vi
      .fn<NativeSubscription['unsubscribeArrow']>()
      .mockRejectedValue(
        new Error(
          '[XBBG:DATALOSS] Subscription data loss [topic=ES1 Index]: Bloomberg reported DATALOSS',
        ),
      );
    const native = fakeNativeSubscription({
      unsubscribe: scalarClose,
      unsubscribeArrow: arrowClose,
    });
    const subscription = new api.Subscription(native);

    const error = await subscription
      .arrow()
      .unsubscribe(true)
      .then(
        () => null,
        (raised: unknown) => raised,
      );

    expect(error).toBeInstanceOf(api.BlpSubscriptionDataLossError);
    expect(error).toMatchObject({
      code: 'DATALOSS',
      detail: 'Bloomberg reported DATALOSS',
      topic: 'ES1 Index',
    });
    expect(arrowClose).toHaveBeenCalledTimes(1);
    expect(arrowClose).toHaveBeenCalledWith(true);
    expect(scalarClose).not.toHaveBeenCalled();
  });
});

describe('native Arrow layout caching', () => {
  it('retains per-result metadata for equal physical layouts', () => {
    const first = tableFromNativeArrowBatch(int32ArrowBatch(1, { request: 'first' }));
    const second = tableFromNativeArrowBatch(int32ArrowBatch(2, { request: 'second' }));

    expect(first.schema.metadata.get('request')).toBe('first');
    expect(second.schema.metadata.get('request')).toBe('second');
    expect(first.schema.metadata.get('request')).toBe('first');
  });

  it('bounds distinct cached physical layouts', () => {
    for (let index = 0; index < NATIVE_ARROW_LAYOUT_CACHE_LIMIT + 32; index += 1) {
      tableFromNativeArrowBatch(int32ArrowBatch(index, {}, `field-${index}`));
    }

    expect(nativeArrowLayoutCacheSize()).toBeLessThanOrEqual(NATIVE_ARROW_LAYOUT_CACHE_LIMIT);
  });
});

function jsonRoundTrip(value: unknown): unknown {
  const encoded = JSON.stringify(value);
  return JSON.parse(encoded) as unknown;
}

function engineWithNative(inner: Partial<NativeEngine>): api.Engine {
  return Object.assign(Object.create(api.Engine.prototype) as api.Engine, { inner });
}

describe('shared subscription controls', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each(['subscribe', 'stream'] as const)(
    'passes aliases and feed policies through %s',
    async (method) => {
      const subscribe = vi
        .fn<NativeEngine['subscribe']>()
        .mockResolvedValue(fakeNativeSubscription());
      const subscribeWithOptions = vi
        .fn<NativeEngine['subscribeWithOptions']>()
        .mockResolvedValue(fakeNativeSubscription());
      const engine = engineWithNative({ subscribe, subscribeWithOptions });
      const aliases = { 'SYN UN Equity': 'source' };
      await engine[method](['SYN UN Equity'], ['BID'], {
        aliases,
        onDelayed: 'raise',
        isolated: true,
        rows: false,
        onFieldError: 'raise',
        zeroAsNull: ['BID'],
      });
      const call =
        method === 'subscribe' ? subscribe.mock.calls[0] : subscribeWithOptions.mock.calls[0];
      expect(call?.slice(-6)).toStrictEqual([aliases, 'raise', true, false, 'raise', ['BID']]);
    },
  );

  it('passes policies through module-level subscriptions and option-based subscribe', async () => {
    const subscribeWithOptions = vi
      .fn<NativeEngine['subscribeWithOptions']>()
      .mockResolvedValue(fakeNativeSubscription());
    const engine = engineWithNative({ subscribeWithOptions, signalShutdown: () => undefined });
    vi.spyOn(api.Engine, 'connect').mockResolvedValue(engine);
    api.configure({});
    const options = {
      aliases: { SYN: 'label' },
      onDelayed: 'ignore',
      isolated: false,
      conflate: true,
      rows: false,
      onFieldError: 'ignore',
      zeroAsNull: ['BID'],
    } as const;
    try {
      await api.asubscribe('SYN', 'BID', options);
      await api.subscribe('SYN', 'BID', options);
      expect(subscribeWithOptions.mock.calls).toStrictEqual([
        [
          '//blp/mktdata',
          ['SYN'],
          ['BID'],
          ['conflate'],
          undefined,
          undefined,
          undefined,
          undefined,
          options.aliases,
          'ignore',
          false,
          false,
          'ignore',
          ['BID'],
        ],
        [
          '//blp/mktdata',
          ['SYN'],
          ['BID'],
          ['conflate'],
          undefined,
          undefined,
          undefined,
          undefined,
          options.aliases,
          'ignore',
          false,
          false,
          'ignore',
          ['BID'],
        ],
      ]);
    } finally {
      api.configure();
    }
  });

  it.each([
    { onDelayed: 'unknown' },
    { isolated: 'true' },
    { aliases: ['label'] },
    { aliases: { SYN: 42 } },
    { aliases: new Date(0) },
    { aliases: new Map([['SYN', 'label']]) },
    { aliases: /SYN/u },
    { aliases: Object.create({ inheritedLabel: 'SYN' }) as unknown },
    { rows: 'false' },
    { onFieldError: 'unknown' },
    { zeroAsNull: 'THEO_PRICE' },
    { zeroAsNull: ['THEO_PRICE', 7] },
  ])('rejects invalid controls before opening a feed: %j', async (options) => {
    const subscribe = vi.fn<NativeEngine['subscribe']>();
    const subscribeWithOptions = vi.fn<NativeEngine['subscribeWithOptions']>();
    const engine = engineWithNative({ subscribe, subscribeWithOptions });
    await expect(
      engine.subscribe(['SYN'], ['BID'], options as api.StreamOptions),
    ).rejects.toBeInstanceOf(api.BlpValidationError);
    await expect(
      engine.stream(['SYN'], ['BID'], options as api.StreamOptions),
    ).rejects.toBeInstanceOf(api.BlpValidationError);
    expect(subscribe).not.toHaveBeenCalled();
    expect(subscribeWithOptions).not.toHaveBeenCalled();
  });

  it('exposes a live field projection after addFields and keeps labels on add', async () => {
    let fields = ['BID'];
    const add = vi.fn<NativeSubscription['add']>().mockResolvedValue(undefined);
    const native = fakeNativeSubscription({
      add,
      fields,
      addFields: async (newFields) => {
        fields = [...fields, ...newFields];
      },
    });
    Object.defineProperty(native, 'fields', { get: () => [...fields] });
    const subscription = new api.Subscription(native);
    await subscription.add(['SYN UN Equity'], { 'SYN UN Equity': 'source' });
    await subscription.addFields(['ASK']);
    expect(subscription.fields).toStrictEqual(['BID', 'ASK']);
    expect(add).toHaveBeenCalledWith(['SYN UN Equity'], { 'SYN UN Equity': 'source' });
    await expect(
      subscription.add(['SYN'], { SYN: 0 } as unknown as Record<string, string>),
    ).rejects.toBeInstanceOf(api.BlpValidationError);
    expect(add).toHaveBeenCalledTimes(1);
  });

  it('provides materialized snapshots without claiming the stream read mode', async () => {
    const native = fakeNativeSubscription({ nextArrowBatch: async () => int32ArrowBatch(9) });
    const subscription = new api.Subscription(native);
    const image = subscription.latest({ backend: api.Backend.JSON });
    expect(jsonRoundTrip(image)).toStrictEqual([{ answer: 7 }]);
    const result = await subscription.arrow().next();
    expect(result.value?.getChild('answer')?.get(0)).toBe(9);
    expect((subscription.latest() as Table).getChild('answer')?.get(0)).toBe(7);
    await subscription.unsubscribe();
  });

  it('keeps topic aliases, upstream identity and diagnostic errors in every status view', () => {
    const native = fakeNativeSubscription();
    const event: api.SubscriptionEvent = {
      atUs: 10,
      category: 'subscription',
      level: 'warning',
      messageType: 'FieldException',
      topic: 'source',
      detail: 'BAD_FIELD: BAD_FLD',
    };
    native.status.events.push(event);
    native.status.failures.push({ topic: 'source', reason: 'rejected', kind: 'failure', atUs: 11 });
    native.status.failedTickers.push('source');
    native.status.topicStates.source = {
      topic: 'source',
      feedTopic: 'SYN UN Equity',
      delayed: true,
      state: 'failed',
      lastChangeUs: 11,
      streamsActive: false,
      streamsChangedUs: 10,
    };
    native.status.fieldErrors.source = { BAD_FIELD: 'BAD_FLD' };
    const subscription = new api.Subscription(native);
    expect(subscription.status).toMatchObject({
      events: [event],
      failedTickers: ['source'],
      fieldErrors: { source: { BAD_FIELD: 'BAD_FLD' } },
      topicStates: { source: { feedTopic: 'SYN UN Equity', delayed: true } },
    });
    expect(subscription.events).toStrictEqual([event]);
    expect(subscription.failures[0]?.reason).toBe('rejected');
    expect(subscription.failedTickers).toStrictEqual(['source']);
    expect(subscription.topicStates.source?.topic).toBe('source');
    expect(subscription.fieldErrors.source).toStrictEqual({ BAD_FIELD: 'BAD_FLD' });
    expect(subscription.sessionStatus.state).toBe('up');
    expect(subscription.adminStatus.dataLossCount).toBe(0);
    expect(subscription.serviceStatus).toStrictEqual({});
    const feeds: api.FeedInfo[] = [
      {
        service: '//blp/mktdata',
        topic: 'SYN UN Equity',
        options: [],
        fields: ['BID', 'ASK'],
        consumers: 2,
        delayed: true,
        state: 'active',
        isolated: false,
        fieldErrors: { BAD_FIELD: 'BAD_FLD' },
      },
    ];
    expect(engineWithNative({ subscriptionFeeds: () => feeds }).subscriptionFeeds()).toStrictEqual(
      feeds,
    );
  });

  it('keeps image-only subscriptions usable after scalar and Arrow read attempts', async () => {
    const nextUpdates = vi.fn<NativeSubscription['nextUpdates']>();
    const nextArrowBatch = vi.fn<NativeSubscription['nextArrowBatch']>();
    const unsubscribe = vi.fn<NativeSubscription['unsubscribe']>().mockResolvedValue(null);
    const subscription = new api.Subscription(
      fakeNativeSubscription({
        deliversRows: false,
        nextUpdates,
        nextArrowBatch,
        unsubscribe,
      }),
    );
    await expect(subscription.next()).rejects.toMatchObject({
      name: 'BlpValidationError',
      element: 'rows',
      message: expect.stringMatching(/rows=false.*latest/u),
    });
    await expect(subscription.arrow().next()).rejects.toThrow(/rows=false.*latest/u);
    await expect(subscription.next({ signal: new AbortController().signal })).rejects.toThrow(
      /rows=false.*latest/u,
    );
    expect((subscription.latest() as Table).getChild('answer')?.get(0)).toBe(7);
    expect(nextUpdates).not.toHaveBeenCalled();
    expect(nextArrowBatch).not.toHaveBeenCalled();
    expect(unsubscribe).not.toHaveBeenCalled();
    await subscription.unsubscribe();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it('surfaces closed latest errors instead of returning a successful empty image', () => {
    const subscription = new api.Subscription(
      fakeNativeSubscription({
        latest: () => {
          throw new Error('[XBBG:VALIDATION] subscription already closed');
        },
      }),
    );
    expect(() => subscription.latest()).toThrow(api.BlpValidationError);
    expect(() => subscription.latest()).toThrow(/subscription already closed/u);
  });

  it('keeps recovery history separate from consumable delayed and field warnings', () => {
    const emitted = vi.spyOn(process, 'emitWarning').mockReturnValue(undefined);
    const warnings: api.SubscriptionEvent[] = [
      {
        atUs: 3,
        category: 'subscription',
        level: 'warning',
        messageType: 'DelayedStream',
        topic: 'source',
      },
    ];
    const native = fakeNativeSubscription({
      deliversRows: false,
      takeWarnings: () => warnings.splice(0),
    });
    native.status.events.push(
      {
        atUs: 1,
        category: 'subscription',
        level: 'warning',
        messageType: 'DataLoss',
        topic: 'source',
      },
      {
        atUs: 2,
        category: 'subscription',
        level: 'info',
        messageType: 'FeedRecovered',
        topic: 'source',
      },
    );
    const subscription = new api.Subscription(native);
    subscription.latest();
    subscription.latest();
    expect(emitted.mock.calls).toStrictEqual([
      ['DelayedStream: source', { type: 'BlpDelayedDataWarning', code: 'XBBG_DELAYED_STREAM' }],
    ]);
  });
});

describe('subscription warning lifecycle', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });
  const delayed: api.SubscriptionEvent = {
    atUs: 1,
    category: 'subscription',
    level: 'warning',
    messageType: 'DelayedStream',
    topic: 'source',
    detail: 'Feed is delayed',
  };
  const field: api.SubscriptionEvent = {
    atUs: 2,
    category: 'subscription',
    level: 'warning',
    messageType: 'FieldException',
    topic: 'source',
    detail: 'BAD_FIELD: BAD_FLD',
  };

  it('emits queued late-attach warnings before returning a subscription without replaying them', async () => {
    const emitted = vi.spyOn(process, 'emitWarning').mockReturnValue(undefined);
    const warnings = [delayed];
    const native = fakeNativeSubscription({
      takeWarnings: () => warnings.splice(0),
      remove: async () => {
        warnings.push(field);
      },
    });
    const engine = engineWithNative({ subscribe: async () => native });
    const subscription = await engine.subscribe(['SYN'], ['BID']);
    expect(emitted).toHaveBeenCalledTimes(1);
    await subscription.remove(['source']);
    subscription.latest();
    await subscription.unsubscribe();
    expect(emitted.mock.calls.map((call) => call[1])).toMatchObject([
      { type: 'BlpDelayedDataWarning', code: 'XBBG_DELAYED_STREAM' },
      { type: 'BlpFieldWarning', code: 'XBBG_FIELD_EXCEPTION' },
    ]);
  });

  it.each(['scalar', 'arrow'] as const)(
    'emits warnings once from the %s read lifecycle',
    async (view) => {
      const emitted = vi.spyOn(process, 'emitWarning').mockReturnValue(undefined);
      const warnings: api.SubscriptionEvent[] = [];
      const takeWarnings = vi.fn<NativeSubscription['takeWarnings']>(() => warnings.splice(0));
      const native = fakeNativeSubscription({
        takeWarnings,
        nextUpdates: async () => {
          warnings.push(delayed, field);
          return scalarBatch([1, 2]);
        },
        nextArrowBatch: async () => {
          warnings.push(delayed, field);
          return int32ArrowBatch(1);
        },
      });
      const subscription = new api.Subscription(native);
      if (view === 'scalar') {
        await subscription.next();
        await subscription.next(); // JS-buffered rows must not replay native warnings.
      } else {
        await subscription.arrow().next();
      }
      // One attach drain and one native batch; buffered ticks must not cross into native.
      expect(takeWarnings).toHaveBeenCalledTimes(2);
      subscription.latest();
      await subscription.unsubscribe();
      expect(emitted.mock.calls).toStrictEqual([
        [
          'DelayedStream: source',
          { type: 'BlpDelayedDataWarning', code: 'XBBG_DELAYED_STREAM', detail: 'Feed is delayed' },
        ],
        [
          'FieldException: source',
          { type: 'BlpFieldWarning', code: 'XBBG_FIELD_EXCEPTION', detail: 'BAD_FIELD: BAD_FLD' },
        ],
      ]);
    },
  );

  it('drains warnings after control operations and terminal reads, even when they fail', async () => {
    const emitted = vi.spyOn(process, 'emitWarning').mockReturnValue(undefined);
    const warnings: api.SubscriptionEvent[] = [];
    const subscription = new api.Subscription(
      fakeNativeSubscription({
        takeWarnings: () => warnings.splice(0),
        add: async () => {
          warnings.push(delayed);
        },
        addFields: async () => {
          warnings.push(field);
          throw new Error('[XBBG:VALIDATION] subscription already closed');
        },
        latest: () => {
          warnings.push(delayed);
          return int32ArrowBatch(7);
        },
        nextUpdates: async () => {
          warnings.push(field);
          return null;
        },
      }),
    );
    await subscription.add(['SYN']);
    await expect(subscription.addFields(['BAD_FIELD'])).rejects.toBeInstanceOf(
      api.BlpValidationError,
    );
    subscription.latest();
    await subscription.next();
    await subscription.unsubscribe();
    expect(emitted.mock.calls.map((call) => call[1])).toMatchObject([
      { code: 'XBBG_DELAYED_STREAM' },
      { code: 'XBBG_FIELD_EXCEPTION' },
      { code: 'XBBG_DELAYED_STREAM' },
      { code: 'XBBG_FIELD_EXCEPTION' },
    ]);
  });
});

interface VenueFixture {
  inputOrder: number;
  security: string;
  topic: string | null;
  status?: string;
  error?: string | null;
}

function venueBatch(rows: readonly VenueFixture[]): NativeArrowZeroCopyBatch {
  function utf8(name: string, values: readonly (string | null)[]): NativeArrowColumn {
    const offsets = new Int32Array(values.length + 1);
    const validity = new Uint8Array(Math.ceil(values.length / 8));
    let text = '';
    let nullCount = 0;
    for (const [index, value] of values.entries()) {
      if (value === null) {
        nullCount += 1;
      } else {
        text += value;
        const byte = Math.floor(index / 8);
        validity[byte] = (validity[byte] ?? 0) + 2 ** (index % 8);
      }
      offsets[index + 1] = Buffer.byteLength(text);
    }
    return {
      name,
      type: 'utf8',
      nullable: true,
      length: values.length,
      nullCount,
      offsets: typedBuffer(offsets),
      data: Buffer.from(text),
      nullBitmap: Buffer.from(validity),
    };
  }
  return {
    kind: 'zeroCopy',
    numRows: rows.length,
    metadata: {},
    columns: [
      {
        name: 'input_order',
        type: 'int32',
        nullable: false,
        length: rows.length,
        nullCount: 0,
        data: typedBuffer(new Int32Array(rows.map((row) => row.inputOrder))),
      },
      utf8(
        'security',
        rows.map((row) => row.security),
      ),
      utf8(
        'venue_topic',
        rows.map((row) => row.topic),
      ),
      utf8(
        'status',
        rows.map((row) => row.status ?? 'resolved'),
      ),
      utf8(
        'error',
        rows.map((row) => row.error ?? null),
      ),
    ],
  };
}

describe('auction preflight and results', () => {
  it('rejects caller aliases before resolving or opening auction feeds', async () => {
    const recipeResolveVenues = vi.fn<NativeEngine['recipeResolveVenues']>();
    const subscribe = vi.fn<NativeEngine['subscribe']>();
    const engine = engineWithNative({ recipeResolveVenues, subscribe });
    const options = { aliases: { VENUE_A: 'custom' } } as api.AuctionStreamOptions;
    await expect(engine.subscribeAuction(['SYN_A'], options)).rejects.toMatchObject({
      name: 'BlpValidationError',
      element: 'aliases',
    });
    expect(recipeResolveVenues).not.toHaveBeenCalled();
    expect(subscribe).not.toHaveBeenCalled();
  });

  it.each(['subscribeAuction', 'streamAuction'] as const)(
    'atomically resolves unique inputs for %s and preserves their labels',
    async (method) => {
      const recipe = vi.fn<NativeEngine['recipeResolveVenues']>().mockResolvedValue(
        venueBatch([
          { inputOrder: 1, security: 'SYN_B US Equity', topic: 'SYN_B UW Equity' },
          { inputOrder: 0, security: 'SYN_A US Equity', topic: 'SYN_A UN Equity' },
        ]),
      );
      const subscribe = vi
        .fn<NativeEngine['subscribe']>()
        .mockResolvedValue(fakeNativeSubscription());
      const engine = engineWithNative({ recipeResolveVenues: recipe, subscribe });
      const pcsOverrides = { 'SYNTHETIC EXCHANGE': 'PCS' };
      await engine[method](['SYN_A US Equity', 'SYN_A US Equity', 'SYN_B US Equity'], {
        fields: ['BID'],
        pcsOverrides,
        isolated: true,
        onDelayed: 'raise',
      });
      expect(recipe).toHaveBeenCalledWith(['SYN_A US Equity', 'SYN_B US Equity'], pcsOverrides);
      expect(subscribe).toHaveBeenCalledWith(
        ['SYN_A UN Equity', 'SYN_B UW Equity'],
        ['BID'],
        undefined,
        { 'SYN_A UN Equity': 'SYN_A US Equity', 'SYN_B UW Equity': 'SYN_B US Equity' },
        'raise',
        true,
        undefined,
        undefined,
        [],
      );
    },
  );

  it.each(['unresolved', 'unsupported', 'mismatch'])(
    'opens no partial stream when any input is %s',
    async (status) => {
      const subscribe = vi.fn<NativeEngine['subscribe']>();
      const engine = engineWithNative({
        subscribe,
        recipeResolveVenues: async () =>
          venueBatch([
            { inputOrder: 0, security: 'SYN_A', topic: 'VENUE_A' },
            {
              inputOrder: 1,
              security: 'SYN_B',
              topic: 'VENUE_B',
              status,
              error: 'venue not validated',
            },
          ]),
      });
      await expect(engine.subscribeAuction(['SYN_A', 'SYN_B'])).rejects.toMatchObject({
        name: 'BlpValidationError',
        element: 'securities',
        message: expect.stringContaining('SYN_B'),
      });
      expect(subscribe).not.toHaveBeenCalled();
    },
  );

  it('rejects distinct inputs sharing a venue instead of silently merging their labels', async () => {
    const subscribe = vi.fn<NativeEngine['subscribe']>();
    const engine = engineWithNative({
      subscribe,
      recipeResolveVenues: async () =>
        venueBatch([
          { inputOrder: 0, security: 'SYN_A', topic: 'VENUE_A' },
          { inputOrder: 1, security: 'SYN_B', topic: 'VENUE_A' },
        ]),
    });
    await expect(engine.subscribeAuction(['SYN_A', 'SYN_B'])).rejects.toThrow(/SYN_A, SYN_B/u);
    expect(subscribe).not.toHaveBeenCalled();
  });

  it.each([
    { rows: [{ inputOrder: 0, security: 'WRONG', topic: 'VENUE_A' }] },
    { rows: [{ inputOrder: 1, security: 'SYN_A', topic: 'VENUE_A' }] },
    { rows: [{ inputOrder: 0, security: 'SYN_A', topic: null }] },
    { rows: [] },
  ])('rejects incomplete or misidentified recipe rows before subscribing', async ({ rows }) => {
    const subscribe = vi.fn<NativeEngine['subscribe']>();
    const engine = engineWithNative({
      subscribe,
      recipeResolveVenues: async () => venueBatch(rows),
    });
    await expect(engine.subscribeAuction(['SYN_A'])).rejects.toBeInstanceOf(api.BlpValidationError);
    expect(subscribe).not.toHaveBeenCalled();
  });

  it('rejects empty inputs before resolution and retains default fields for an empty field list', async () => {
    const recipe = vi
      .fn<NativeEngine['recipeResolveVenues']>()
      .mockResolvedValue(venueBatch([{ inputOrder: 0, security: 'SYN_A', topic: 'VENUE_A' }]));
    const subscribe = vi
      .fn<NativeEngine['subscribe']>()
      .mockResolvedValue(fakeNativeSubscription());
    const engine = engineWithNative({ recipeResolveVenues: recipe, subscribe });
    await expect(engine.subscribeAuction([])).rejects.toBeInstanceOf(api.BlpValidationError);
    expect(recipe).not.toHaveBeenCalled();
    await engine.subscribeAuction('SYN_A', { fields: [] });
    expect(subscribe.mock.calls[0]?.[1]).toStrictEqual(api.AuctionFields.default);
  });

  it('converts resolve and snapshot recipe tables using the requested backend', async () => {
    const pcsOverrides = { 'SYNTHETIC EXCHANGE': 'PCS' };
    const recipeResolveVenues = vi
      .fn<NativeEngine['recipeResolveVenues']>()
      .mockResolvedValue(venueBatch([{ inputOrder: 0, security: 'SYN_A', topic: 'VENUE_A' }]));
    const recipeAuctionSnapshot = vi
      .fn<NativeEngine['recipeAuctionSnapshot']>()
      .mockResolvedValue(int32ArrowBatch(42, {}, 'BID'));
    const engine = engineWithNative({ recipeResolveVenues, recipeAuctionSnapshot });
    const venues = await engine.resolveVenues('SYN_A', { pcsOverrides, backend: 'json' });
    expect(jsonRoundTrip(venues)).toStrictEqual([
      {
        input_order: 0,
        security: 'SYN_A',
        venue_topic: 'VENUE_A',
        status: 'resolved',
        error: null,
      },
    ]);
    const snapshot = await engine.auctionSnapshot('SYN_A', {
      fields: ['BID'],
      pcsOverrides,
      backend: 'json',
    });
    expect(jsonRoundTrip(snapshot)).toStrictEqual([{ BID: 42 }]);
    expect(recipeResolveVenues).toHaveBeenCalledWith(['SYN_A'], pcsOverrides);
    expect(recipeAuctionSnapshot).toHaveBeenCalledWith(['SYN_A'], ['BID'], pcsOverrides);
  });

  it.each([
    { fields: undefined, zeroAsNull: undefined, expected: ['THEO_PRICE', 'INDICATIVE_NEAR'] },
    { fields: [], zeroAsNull: undefined, expected: ['THEO_PRICE', 'INDICATIVE_NEAR'] },
    {
      fields: ['BID', 'REFERENCE_PRICE_RT'],
      zeroAsNull: undefined,
      expected: ['REFERENCE_PRICE_RT'],
    },
    { fields: ['THEO_PRICE'], zeroAsNull: [], expected: [] },
    { fields: ['BID'], zeroAsNull: ['BID'], expected: ['BID'] },
  ])(
    'applies auction zero sentinels only to selected fields: %j',
    async ({ fields, zeroAsNull, expected }) => {
      const subscribe = vi
        .fn<NativeEngine['subscribe']>()
        .mockResolvedValue(fakeNativeSubscription());
      const engine = engineWithNative({
        subscribe,
        recipeResolveVenues: async () =>
          venueBatch([{ inputOrder: 0, security: 'SYN', topic: 'VENUE' }]),
      });
      await engine.subscribeAuction('SYN', {
        fields,
        zeroAsNull,
        rows: false,
        onFieldError: 'raise',
      });
      expect(subscribe.mock.calls[0]?.slice(-3)).toStrictEqual([false, 'raise', expected]);
    },
  );

  it('rejects a row-delivery option on streamAuction before venue resolution', async () => {
    const recipeResolveVenues = vi.fn<NativeEngine['recipeResolveVenues']>();
    const engine = engineWithNative({ recipeResolveVenues });
    const options = { fields: ['BID'], rows: false };
    await expect(engine.streamAuction('SYN', options)).rejects.toMatchObject({
      name: 'BlpValidationError',
      element: 'rows',
      message: expect.stringContaining('subscribeAuction'),
    });
    expect(recipeResolveVenues).not.toHaveBeenCalled();
  });
});

describe('temporal native table results', () => {
  it('preserves Time64 microseconds, Date32 days and nulls in Arrow and JSON row backends', async () => {
    const dateMs = Date.UTC(2026, 0, 2);
    const batch: NativeArrowZeroCopyBatch = {
      kind: 'zeroCopy',
      numRows: 3,
      metadata: {},
      columns: [
        {
          name: 'value_time',
          type: 'time64_us',
          nullable: true,
          length: 3,
          nullCount: 1,
          data: typedBuffer(new BigInt64Array([57_541_123_456n, 0n, 0n])),
          nullBitmap: Buffer.from([3]),
        },
        {
          name: 'value_date',
          type: 'date32',
          nullable: true,
          length: 3,
          nullCount: 1,
          data: typedBuffer(new Int32Array([dateMs / 86_400_000, 0, 0])),
          nullBitmap: Buffer.from([3]),
        },
      ],
    };
    const engine = engineWithNative({
      request: async () => batch,
      recipeAuctionSnapshot: async () => batch,
    });
    const request = {
      service: '//blp/refdata',
      operation: 'ReferenceDataRequest',
      format: 'long_typed',
    } as const;
    const table = (await engine.request(request)) as Table;
    const time = table.getChild('value_time');
    const date = table.getChild('value_date');
    expect(DataType.isTime(time!.type)).toBeTruthy();
    expect(time!.type).toMatchObject({ unit: TimeUnit.MICROSECOND, bitWidth: 64 });
    expect(DataType.isDate(date!.type)).toBeTruthy();
    expect(date!.type).toMatchObject({ unit: DateUnit.DAY });
    expect([time?.get(0), time?.get(1), time?.get(2)]).toStrictEqual([57_541_123_456n, 0n, null]);
    expect([date?.get(0), date?.get(1), date?.get(2)]).toStrictEqual([dateMs, 0, null]);
    const rows = (await engine.auctionSnapshot('SYN', { backend: 'json' })) as Record<
      string,
      unknown
    >[];
    expect(rows.map((row) => [row.value_time, row.value_date])).toStrictEqual([
      [57_541_123_456n, dateMs],
      [0n, 0],
      [null, null],
    ]);
  });
});
