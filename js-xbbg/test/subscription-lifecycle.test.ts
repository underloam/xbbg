import {
  NATIVE_ARROW_LAYOUT_CACHE_LIMIT,
  nativeArrowLayoutCacheSize,
  tableFromNativeArrowBatch,
} from '../src/arrow-zero-copy';
import * as api from '../src/index';
import type {
  NativeArrowZeroCopyBatch,
  NativeSubscription,
  NativeSubscriptionUpdateBatch,
} from '../src/napi';

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
  return {
    add: async () => undefined,
    fields: ['answer'],
    isActive: true,
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
