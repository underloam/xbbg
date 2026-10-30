import { Table } from 'apache-arrow';

import { nativeArrowToBackend, toArrowTableFromNative } from '../src/backends';
import { BlpSubscriptionDataLossError, BlpValidationError } from '../src/errors';
import type { NativeSubscription, NativeSubscriptionDelivery } from '../src/napi';
import { DeliveryStream, Subscription } from '../src/subscriptions';
import type { SubscriptionDelivery, SubscriptionGap } from '../src/types';
import {
  deliveryReceipt,
  fakeNativeSubscription,
  int32ArrowBatch,
  jsonValue,
  scalarBatch,
  typedBuffer,
} from './subscription-fixtures';

// EOF/rejection doubles have concrete results valid for every native projector
// Type; asking Vitest to mock the generic signature would erase its return type.
type EmptyDeliveryClose = (
  ...args: Parameters<NativeSubscription['unsubscribeDeliveries']>
) => Promise<null>;
type RejectedDeliveryRead = (
  ...args: Parameters<NativeSubscription['nextDelivery']>
) => Promise<never>;

function queuedDeliveries(deliveries: readonly NativeSubscriptionDelivery[]): {
  readonly native: NativeSubscription;
  readonly queue: NativeSubscriptionDelivery[];
} {
  const queue = [...deliveries];
  const native = fakeNativeSubscription({
    nextDelivery: async (_maxRows, _maxWaitMs, project) => {
      const delivery = queue[0];
      if (delivery === undefined) {
        return null;
      }
      if (project === undefined) {
        throw new Error('public delivery reads must supply their transactional projector');
      }
      const output = project(delivery);
      return deliveryReceipt(output, () => {
        queue.shift();
      });
    },
    unsubscribe: async () => {
      queue.length = 0;
      return null;
    },
    unsubscribeDeliveries: async (drain, project) => {
      if (!drain) {
        queue.length = 0;
        return null;
      }
      if (project === undefined) {
        throw new Error('public delivery drains must supply their backend projector');
      }
      const output = queue.map(project);
      queue.length = 0;
      return output;
    },
  });
  return { native, queue };
}

function delta(value: number, gapBefore?: SubscriptionGap): NativeSubscriptionDelivery {
  return {
    kind: 'deltas',
    data: int32ArrowBatch(value, { 'synthetic.layout': 'retained' }),
    ...(gapBefore === undefined ? {} : { gapBefore }),
  };
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  return Promise.withResolvers<T>();
}

describe('subscription delivery views', () => {
  it('returns Arrow envelopes without rounding gaps or hiding them in dataframe metadata', async () => {
    const gap = { cause: 'local_overflow', droppedUpdates: 9_007_199_254_740_995n } as const;
    const { native } = queuedDeliveries([delta(7, gap), delta(8)]);
    const subscription = new Subscription(native);
    const view = subscription.deliveries();
    expect(subscription.deliveries()).toBe(view);
    expect(subscription.delivery).toBe('deltas');
    expectTypeOf(view).toEqualTypeOf<DeliveryStream>();

    const first = await view.next();
    expectTypeOf(first.value).toEqualTypeOf<SubscriptionDelivery | undefined>();
    expect(first.done).toBeFalsy();
    expect(first.value?.kind).toBe('deltas');
    expect(first.value?.data).toBeInstanceOf(Table);
    expect(first.value?.data.getChild('answer')?.get(0)).toBe(7);
    expect(first.value?.data.schema.metadata.get('synthetic.layout')).toBe('retained');
    expect(first.value?.gapBefore).toStrictEqual(gap);
    expect(first.value?.data.schema.metadata.has('gapBefore')).toBeFalsy();
    expect((await view.next()).value?.gapBefore).toBeUndefined();
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  it('keeps gap controls separate from JSON data and dataframe metadata', async () => {
    const gap = { cause: 'local_overflow', droppedUpdates: 5n } as const;
    const subscription = new Subscription(queuedDeliveries([delta(7, gap)]).native);
    const output = (await subscription.deliveries({ backend: 'json' }).next()).value;
    expect(jsonValue(output?.data)).toStrictEqual([{ answer: 7 }]);
    expect(output?.gapBefore).toStrictEqual(gap);
    await subscription.unsubscribe(false);
  });

  it('keeps state validity and explicit nulls through JSON backend conversion', async () => {
    const data = int32ArrowBatch(null);
    const { native } = queuedDeliveries([
      {
        kind: 'state',
        data: {
          ...data,
          columns: [
            ...data.columns,
            {
              name: 'valid',
              type: 'bool',
              nullable: false,
              length: 1,
              nullCount: 0,
              data: typedBuffer(new Uint8Array([0])),
            },
          ],
        },
      },
    ]);
    const subscription = new Subscription({
      ...native,
      delivery: 'state',
      requiresDeliveries: true,
    });
    const output = (await subscription.deliveries({ backend: 'json' }).next()).value;
    expect(output?.kind).toBe('state');
    expect(jsonValue(output?.data)).toStrictEqual([{ answer: null, valid: false }]);
    expect(output?.gapBefore).toBeUndefined();
    await subscription.unsubscribe(false);
  });

  it.each(['state', 'deltas'] as const)(
    'rejects unmarked reads and drains for advanced %s consumers before receiving',
    async (mode) => {
      const nextUpdates = vi.fn<NativeSubscription['nextUpdates']>();
      const nextArrowBatch = vi.fn<NativeSubscription['nextArrowBatch']>();
      const unsubscribe = vi.fn<NativeSubscription['unsubscribe']>().mockResolvedValue(null);
      const { native } = queuedDeliveries([delta(7)]);
      const drainCalls: boolean[] = [];
      const subscription = new Subscription({
        ...native,
        delivery: mode,
        requiresDeliveries: true,
        nextUpdates,
        nextArrowBatch,
        unsubscribe,
        unsubscribeDeliveries: async (drain, project) => {
          drainCalls.push(drain);
          return await native.unsubscribeDeliveries(drain, project);
        },
      });
      await expect(subscription.next()).rejects.toBeInstanceOf(BlpValidationError);
      await expect(subscription.arrow().next()).rejects.toBeInstanceOf(BlpValidationError);
      expect(nextUpdates).not.toHaveBeenCalled();
      expect(nextArrowBatch).not.toHaveBeenCalled();
      expect((await subscription.deliveries().next()).value?.data.numRows).toBe(1);
      await expect(subscription.unsubscribe(true)).rejects.toBeInstanceOf(BlpValidationError);
      expect(unsubscribe).not.toHaveBeenCalled();
      expect(drainCalls).toStrictEqual([true]);
      await expect(subscription.deliveries().next()).resolves.toStrictEqual({
        done: true,
        value: undefined,
      });
    },
  );

  it('rejects mixed scalar, Arrow, and envelope reads without stealing their batches', async () => {
    const { native, queue } = queuedDeliveries([delta(7)]);
    const subscription = new Subscription({ ...native, nextUpdates: async () => scalarBatch([1]) });
    await subscription.next();
    await expect(subscription.deliveries().next()).rejects.toBeInstanceOf(TypeError);
    expect(queue).toHaveLength(1);
    await subscription.unsubscribe(false);

    const { native: secondNative, queue: secondQueue } = queuedDeliveries([delta(8), delta(9)]);
    const second = new Subscription(secondNative);
    await second.deliveries().next();
    await expect(second.next()).rejects.toBeInstanceOf(TypeError);
    await expect(second.arrow().next()).rejects.toBeInstanceOf(TypeError);
    expect(secondQueue).toHaveLength(1);
    await second.unsubscribe(false);
  });

  it('leaves a failed backend projection retryable, preserving the data and gap', async () => {
    const gap = { cause: 'local_overflow', droppedUpdates: 17n } as const;
    const { native, queue } = queuedDeliveries([delta(7, gap)]);
    const close = vi.fn<EmptyDeliveryClose>().mockResolvedValue(null);
    const inner = { ...native, unsubscribeDeliveries: close };
    const failing = new DeliveryStream(inner, () => {
      throw new Error('synthetic backend conversion failure');
    });
    await expect(failing.next()).rejects.toThrow('synthetic backend conversion failure');
    expect(close).not.toHaveBeenCalled();
    expect(queue).toHaveLength(1);

    const recovered = (await new Subscription(inner).deliveries().next()).value;
    expect(recovered?.data.getChild('answer')?.get(0)).toBe(7);
    expect(recovered?.gapBefore).toStrictEqual(gap);
    expect(queue).toHaveLength(0);
    await failing.unsubscribe(false);
  });

  it('keeps native export failures retryable before a JS projector can run', async () => {
    const gap = { cause: 'local_overflow', droppedUpdates: 3n } as const;
    const { native, queue } = queuedDeliveries([delta(7, gap)]);
    let first = true;
    const close = vi.fn<EmptyDeliveryClose>().mockResolvedValue(null);
    const subscription = new Subscription({
      ...native,
      nextDelivery: async (maxRows, maxWaitMs, project) => {
        if (first) {
          first = false;
          throw new Error('[XBBG:DELIVERY_CONVERSION] synthetic native export failure');
        }
        return await native.nextDelivery(maxRows, maxWaitMs, project);
      },
      unsubscribeDeliveries: close,
    });
    const view = subscription.deliveries();
    await expect(view.next()).rejects.toThrow('synthetic native export failure');
    expect(close).not.toHaveBeenCalled();
    expect(queue).toHaveLength(1);
    const recovered = (await view.next()).value;
    expect(recovered?.data.getChild('answer')?.get(0)).toBe(7);
    expect(recovered?.gapBefore).toStrictEqual(gap);
    await view.unsubscribe(false);
  });

  it.each(['arrow', 'json'] as const)(
    'rolls a late unpublished read into the %s owner drain with every gap intact',
    async (backend) => {
      const started = deferred<undefined>();
      const release = deferred<undefined>();
      const readFinished = deferred<undefined>();
      const frames = [
        delta(2, { cause: 'local_overflow', droppedUpdates: 1n }),
        delta(3, { cause: 'local_overflow', droppedUpdates: 2n }),
      ];
      let closing = false;
      let readProjections = 0;
      const native = fakeNativeSubscription({
        signalClosing: () => {
          closing = true;
        },
        nextDelivery: async (_maxRows, _maxWaitMs, project) => {
          started.resolve(undefined);
          try {
            await release.promise;
            if (closing) return null;
            const delivery = frames[0];
            if (delivery === undefined) return null;
            if (project === undefined) throw new Error('missing projector');
            readProjections += 1;
            const output = project(delivery);
            return deliveryReceipt(output, () => {
              frames.shift();
            });
          } finally {
            readFinished.resolve(undefined);
          }
        },
        unsubscribeDeliveries: async (drain, project) => {
          await readFinished.promise;
          if (!drain) return null;
          if (project === undefined) throw new Error('missing projector');
          const output = frames.map(project);
          frames.length = 0;
          return output;
        },
      });
      const subscription = new Subscription(native);
      const reader = subscription.deliveries();
      const pending = reader.next();
      await started.promise;
      const closingRead =
        backend === 'arrow'
          ? reader.unsubscribe(true)
          : subscription.deliveries({ backend: 'json' }).unsubscribe(true);
      release.resolve(undefined);
      await expect(pending).resolves.toStrictEqual({ done: true, value: undefined });
      const results = await closingRead;
      expect(readProjections).toBe(0);
      expect(
        results.map((delivery) => {
          const rows = delivery.data instanceof Table ? [...delivery.data] : delivery.data;
          return JSON.stringify(rows);
        }),
      ).toStrictEqual(['[{"answer":2}]', '[{"answer":3}]']);
      expect(results.map((delivery) => delivery.gapBefore?.droppedUpdates)).toStrictEqual([1n, 2n]);
    },
  );

  it.each([0, 1])(
    'allows backend retry after drain projection failure at batch %i',
    async (failAt) => {
      const gap = { cause: 'local_overflow', droppedUpdates: 17n } as const;
      const { native, queue } = queuedDeliveries([delta(1, gap), delta(2), delta(3)]);
      let index = 0;
      const failing = new DeliveryStream(native, (delivery) => {
        const current = index;
        index += 1;
        if (current === failAt) throw new Error('synthetic drain projection failure');
        return delivery;
      });
      await expect(failing.unsubscribe(true)).rejects.toThrow('synthetic drain projection failure');
      expect(queue).toHaveLength(3);
      await expect(failing.next()).resolves.toStrictEqual({ done: true, value: undefined });
      const drained = await new Subscription(native)
        .deliveries({ backend: 'json' })
        .unsubscribe(true);
      expect(drained.map((delivery) => jsonValue(delivery.data))).toStrictEqual([
        [{ answer: 1 }],
        [{ answer: 2 }],
        [{ answer: 3 }],
      ]);
      expect(drained.map((delivery) => delivery.gapBefore?.droppedUpdates)).toStrictEqual([
        17n,
        undefined,
        undefined,
      ]);
      expect(queue).toHaveLength(0);
    },
  );

  it.each([
    { backend: 'arrow', throwInitial: false },
    { backend: 'json', throwInitial: false },
    { backend: 'arrow', throwInitial: true },
    { backend: 'json', throwInitial: true },
  ] as const)(
    'keeps a resolved-but-unpublished read across $backend microtask close (throws=$throwInitial)',
    async ({ backend, throwInitial }) => {
      const gap = { cause: 'local_overflow', droppedUpdates: 9n } as const;
      const { native, queue } = queuedDeliveries([delta(1, gap), delta(2), delta(3)]);
      const subscription = new Subscription(native);
      const publication: { closing?: Promise<SubscriptionDelivery<unknown>[]> } = {};
      let initial = true;
      let drainIndex = 0;
      const reader: DeliveryStream = new DeliveryStream(
        native,
        (delivery): SubscriptionDelivery => {
          if (initial) {
            initial = false;
            queueMicrotask(() => {
              const closing =
                backend === 'arrow' ? reader.unsubscribe(true) : jsonOwner.unsubscribe(true);
              publication.closing = closing;
              void closing.catch(() => undefined);
            });
            if (throwInitial) throw new Error('synthetic unpublished read conversion failure');
          } else {
            const current = drainIndex;
            drainIndex += 1;
            if (current === 1) throw new Error('synthetic later drain failure');
          }
          return { ...delivery, data: toArrowTableFromNative(delivery.data) };
        },
      );
      const jsonOwner: DeliveryStream<unknown> = new DeliveryStream<unknown>(native, (delivery) => {
        const current = drainIndex;
        drainIndex += 1;
        if (current === 1) throw new Error('synthetic later drain failure');
        return { ...delivery, data: nativeArrowToBackend(delivery.data, 'json') };
      });
      await expect(reader.next()).resolves.toStrictEqual({ done: true, value: undefined });
      if (publication.closing === undefined) throw new Error('microtask did not start close');
      await expect(publication.closing).rejects.toThrow('synthetic later drain failure');
      expect(queue).toHaveLength(3);
      const drained = await (backend === 'arrow'
        ? subscription.deliveries({ backend: 'json' }).unsubscribe(true)
        : subscription.deliveries().unsubscribe(true));
      expect(
        drained.map((delivery) => {
          const rows = delivery.data instanceof Table ? [...delivery.data] : delivery.data;
          return JSON.stringify(rows);
        }),
      ).toStrictEqual(['[{"answer":1}]', '[{"answer":2}]', '[{"answer":3}]']);
      expect(drained.map((delivery) => delivery.gapBefore?.droppedUpdates)).toStrictEqual([
        9n,
        undefined,
        undefined,
      ]);
    },
  );

  it('gives unread SDK terminal failure precedence over the advanced plain-drain guard', async () => {
    const unsubscribe = vi.fn<NativeSubscription['unsubscribe']>().mockResolvedValue(null);
    const subscription = new Subscription(
      fakeNativeSubscription({
        requiresDeliveries: true,
        unsubscribe,
        unsubscribeDeliveries: async () => {
          throw new Error(
            '[XBBG:DATALOSS] Subscription data loss [topic=SYN]: synthetic source loss',
          );
        },
      }),
    );
    await expect(subscription.unsubscribe(true)).rejects.toBeInstanceOf(
      BlpSubscriptionDataLossError,
    );
    expect(unsubscribe).not.toHaveBeenCalled();
  });

  it('aborts an in-flight delivery read through the shared delivery cleanup path', async () => {
    const started = deferred<undefined>();
    const pending = deferred<NativeSubscriptionDelivery | null>();
    const closeCalls: boolean[] = [];
    let closing = false;
    let readProjections = 0;
    const native = fakeNativeSubscription({
      signalClosing: () => {
        closing = true;
      },
      nextDelivery: async (_maxRows, _maxWaitMs, project) => {
        started.resolve(undefined);
        const delivery = await pending.promise;
        if (closing || delivery === null) return null;
        if (project === undefined) throw new Error('missing projector');
        readProjections += 1;
        return deliveryReceipt(project(delivery));
      },
      unsubscribeDeliveries: async (drain) => {
        closeCalls.push(drain);
        pending.resolve(delta(9));
        return null;
      },
    });
    const view = new Subscription(native).deliveries();
    const controller = new AbortController();
    const next = view.next({ signal: controller.signal });
    await started.promise;
    controller.abort();
    await expect(next).rejects.toMatchObject({ name: 'AbortError' });
    expect(closeCalls).toStrictEqual([false]);
    expect(readProjections).toBe(0);
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(view.return()).resolves.toStrictEqual({ done: true, value: undefined });
    expect(closeCalls).toStrictEqual([false]);
  });

  it.each(['rejection', 'receipt'] as const)(
    'preserves a source %s when diagnostic reporting also fails',
    async (mode) => {
      let armed = false;
      let reported = false;
      let unread = true;
      const sourceFailure = new Error(
        '[XBBG:DATALOSS] Subscription data loss [topic=SYN]: synthetic source loss',
      );
      const subscription = new Subscription(
        fakeNativeSubscription({
          nextDelivery: async () => {
            if (!unread) return null;
            if (mode === 'rejection') throw sourceFailure;
            return {
              kind: 'error' as const,
              commit: () => {
                unread = false;
                throw sourceFailure;
              },
              rollback: () => {
                unread = true;
              },
            };
          },
          takeWarnings: () => {
            if (armed && !reported) {
              reported = true;
              throw new Error('synthetic diagnostic failure');
            }
            return [];
          },
        }),
      );
      const view = subscription.deliveries();
      armed = true;
      await expect(view.next()).rejects.toBeInstanceOf(BlpSubscriptionDataLossError);
      await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    },
  );

  it('reports a terminal source failure once and ends all views', async () => {
    const sourceFailure = new Error(
      '[XBBG:DATALOSS] Subscription data loss [topic=SYN]: synthetic source loss',
    );
    const nextDelivery = vi.fn<RejectedDeliveryRead>().mockRejectedValue(sourceFailure);
    const close = vi.fn<EmptyDeliveryClose>().mockResolvedValue(null);
    const subscription = new Subscription(
      fakeNativeSubscription({ nextDelivery, unsubscribeDeliveries: close }),
    );
    const view = subscription.deliveries();
    await expect(view.next()).rejects.toBeInstanceOf(BlpSubscriptionDataLossError);
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
    expect(nextDelivery).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledExactlyOnceWith(false, expect.any(Function));
  });

  it('closes once when a delivery for-await loop exits early', async () => {
    const close = vi.fn<EmptyDeliveryClose>().mockResolvedValue(null);
    const { native } = queuedDeliveries([delta(7), delta(8)]);
    const view = new Subscription({ ...native, unsubscribeDeliveries: close }).deliveries();
    let price: unknown;
    for await (const delivery of view) {
      price = delivery.data.getChild('answer')?.get(0);
      if (price === 7) break;
    }
    expect(price).toBe(7);
    expect(close).toHaveBeenCalledExactlyOnceWith(false, expect.any(Function));
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
  });
});
