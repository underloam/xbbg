import { Table } from 'apache-arrow';
import { createRequire } from 'node:module';
import path from 'node:path';

import { nativeArrowToBackend, nativeArrowToPolars, toArrowTableFromNative } from '../src/backends';
import { wrapError } from '../src/errors';
import type * as Core from '../src/index';
import type { NativeSubscription, NativeSubscriptionDelivery } from '../src/napi';
import type { ResultMetadata, StreamOptions, SubscriptionDelivery } from '../src/types';
import { int32ArrowBatch, jsonValue } from './subscription-fixtures';

type NativeSubscriptionFixtureOptions = Pick<
  StreamOptions,
  | 'delivery'
  | 'overflowPolicy'
  | 'streamCapacity'
  | 'flushThreshold'
  | 'allFields'
  | 'rows'
  | 'aliases'
  | 'zeroAsNull'
>;

interface PolarsDeliveryFrame extends ResultMetadata {
  readonly columns: readonly string[];
  getColumn(name: string): { toArray(): unknown[] };
}

interface NativePublicationPause {
  ready(): Promise<void>;
  release(): void;
}

function corruptDeliveryBuffers(delivery: NativeSubscriptionDelivery): void {
  for (const column of delivery.data.columns) {
    column.data?.fill(0x7f);
    column.offsets?.fill(0x7f);
    column.nullBitmap?.fill(0x7f);
  }
  if (delivery.gapBefore !== undefined) {
    Object.defineProperty(delivery.gapBefore, 'droppedUpdates', {
      value: 999_999_999_999_999_999n,
    });
  }
}

interface NativeSubscriptionFixture {
  subscribe(
    tickers: readonly string[],
    fields: readonly string[],
    options?: NativeSubscriptionFixtureOptions,
  ): Promise<NativeSubscription>;
  publishJson(topic: string, json: string): void;
  dataLoss(topic: string): void;
  pauseNextDelivery(subscription: NativeSubscription): NativePublicationPause;
  settle(): Promise<void>;
  terminate(): void;
}

interface SubscriptionFixtureAddon {
  createSubscriptionFixture(
    capacity?: number,
    overflow?: StreamOptions['overflowPolicy'],
  ): Promise<NativeSubscriptionFixture>;
}

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');
const enabled = process.env.XBBG_TEST_SUBSCRIPTIONS === '1';
const hasPolars = (() => {
  try {
    createRequire(__filename).resolve('nodejs-polars');
    return true;
  } catch {
    return false;
  }
})();

// Requires an explicit test-support addon build. All events pass through the real
// SDK JSON decoder/shared feed/queue/consumer and NAPI completion, without a session.
describe.skipIf(!enabled)('native subscription deliveries', () => {
  let api: typeof Core;
  let addon: SubscriptionFixtureAddon;
  const fixtures: NativeSubscriptionFixture[] = [];
  const publicationPauses: NativePublicationPause[] = [];

  beforeAll(async () => {
    // Static package import would load the addon even in native-free test runs.
    api = await import('../src/index.js');
    addon = createRequire(__filename)(nativePath) as SubscriptionFixtureAddon;
  });

  afterEach(() => {
    for (const pause of publicationPauses.splice(0)) pause.release();
    for (const source of fixtures.splice(0)) source.terminate();
  });

  async function fixture(
    capacity = 1,
    overflow: StreamOptions['overflowPolicy'] = 'drop_oldest',
  ): Promise<NativeSubscriptionFixture> {
    const created = await addon.createSubscriptionFixture(capacity, overflow);
    fixtures.push(created);
    return created;
  }
  function pauseNextDelivery(
    source: NativeSubscriptionFixture,
    subscription: NativeSubscription,
  ): NativePublicationPause {
    const pause = source.pauseNextDelivery(subscription);
    publicationPauses.push(pause);
    return pause;
  }

  test('preserves default scalar and Arrow iteration on strict consumers', async () => {
    const source = await fixture(4, 'drop_newest');
    const scalar = new api.Subscription(
      await source.subscribe(['SCALAR SYNTHETIC Equity'], ['BID']),
    );
    source.publishJson('SCALAR SYNTHETIC Equity', '{"BID":4.5}');
    expect((await scalar.next()).value?.f64('BID')).toBe(4.5);
    await scalar.unsubscribe(false);

    const arrow = new api.Subscription(await source.subscribe(['ARROW SYNTHETIC Equity'], ['BID']));
    source.publishJson('ARROW SYNTHETIC Equity', '{"BID":5.5}');
    expect(Number((await arrow.arrow().next()).value?.getChild('BID')?.get(0))).toBe(5.5);
    await arrow.unsubscribe(false);
  });

  test('guards inherited drop-oldest reads and exports its exact gap with retained Arrow data', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    const subscription = new api.Subscription(native);
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    expect(native.requiresDeliveries).toBeTruthy();
    await expect(subscription.next()).rejects.toBeInstanceOf(api.BlpValidationError);
    await expect(subscription.arrow().next()).rejects.toBeInstanceOf(api.BlpValidationError);

    const delivery = (await subscription.deliveries().next()).value;
    expect(delivery?.kind).toBe('deltas');
    expect(Number(delivery?.data.getChild('BID')?.get(0))).toBe(3.75);
    expect(delivery?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    expect(typeof delivery?.gapBefore?.droppedUpdates).toBe('bigint');
    expect(delivery?.data.schema.metadata.has('gapBefore')).toBeFalsy();
    await subscription.unsubscribe(false);
  });

  test('rolls back through V8 projector exceptions without aliasing mutated JS buffers', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    const failing = new api.DeliveryStream(native, (delivery) => {
      corruptDeliveryBuffers(delivery);
      throw new Error('synthetic projector failure');
    });
    await expect(failing.next()).rejects.toThrow('synthetic projector failure');
    const recovered = (await new api.Subscription(native).deliveries().next()).value;
    expect(Number(recovered?.data.getChild('BID')?.get(0))).toBe(3.75);
    expect(recovered?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    await failing.unsubscribe(false);
  });

  test('keeps retained data and gaps through Polars conversion', async (context) => {
    if (!hasPolars) context.skip();
    const source = await fixture();
    const subscription = new api.Subscription(
      await source.subscribe(['SYNTHETIC Equity'], ['BID']),
    );
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    const delivery = (await subscription.deliveries({ backend: 'polars' }).next()).value;
    const frame = delivery?.data as PolarsDeliveryFrame;
    expect(frame.getColumn('BID').toArray()).toStrictEqual([3.75]);
    expect(delivery?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    expect(frame.columns).toContain('__xbbg_present');
    expect(frame.metadata['xbbg.subscription_presence']).toBe(
      'column=__xbbg_present;encoding=binary-lsb-first;mapping=bit-i-to-schema-field-(i+2)',
    );
    expect(frame.columns).not.toContain('gapBefore');
    expect(frame.metadata).not.toHaveProperty('gapBefore');
    expect(Object.keys(frame)).toContain('BID');
    await subscription.unsubscribe(false);
  });

  test('preserves typed null diagnostics and ordinary metadata on a real Polars frame', (context) => {
    if (!hasPolars) context.skip();
    const metadata = {
      'xbbg.type_misses': '{"answer":1}',
      'xbbg.bloomberg_request_id': 'synthetic-support-id',
      'xbbg.eid_data': '{"SYN":[101]}',
    };
    const frame = nativeArrowToPolars(int32ArrowBatch(null, metadata)) as PolarsDeliveryFrame;
    expect(frame.getColumn('answer').toArray()).toStrictEqual([null]);
    expect(frame.metadata).toStrictEqual(metadata);
    expect(frame.bloombergRequestId).toBe('synthetic-support-id');
    expect(frame.bloombergRequestIds).toStrictEqual(['synthetic-support-id']);
    expect(frame.eidData).toStrictEqual({ SYN: [101] });
    expect(Object.keys(frame)).toContain('answer');
  });

  test('projects state validity, sparse updates, and explicit clears into Polars', async (context) => {
    if (!hasPolars) context.skip();
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID', 'ASK'], {
      delivery: 'state',
    });
    const subscription = new api.Subscription(native);
    source.publishJson('SYNTHETIC Equity', '{"BID":1.25,"ASK":2.5}');
    source.publishJson('SYNTHETIC Equity', '{"BID":3.75}');
    source.publishJson('SYNTHETIC Equity', '{"ASK":null}');
    const view = subscription.deliveries({ backend: 'polars' });
    const delivery = (await view.next()).value;
    const frame = delivery?.data as PolarsDeliveryFrame;
    expect(delivery?.kind).toBe('state');
    expect(frame.getColumn('BID').toArray()).toStrictEqual([3.75]);
    expect(frame.getColumn('ASK').toArray()).toStrictEqual([null]);
    expect(frame.getColumn('valid').toArray()).toStrictEqual([true]);
    expect(delivery?.gapBefore).toBeUndefined();
    expect(frame.columns).not.toContain('gapBefore');
    expect(frame.metadata).not.toHaveProperty('gapBefore');
    source.dataLoss('SYNTHETIC Equity');
    const invalid = (await view.next()).value;
    const invalidFrame = invalid?.data as PolarsDeliveryFrame;
    expect(invalidFrame.getColumn('valid').toArray()).toStrictEqual([false]);
    expect(invalidFrame.getColumn('BID').toArray()).toStrictEqual([null]);
    expect(invalidFrame.getColumn('ASK').toArray()).toStrictEqual([null]);
    await subscription.unsubscribe(false);
  });

  test('retries real Polars conversion after a mutable exported-buffer failure', async (context) => {
    if (!hasPolars) context.skip();
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    const failing = new api.DeliveryStream(native, (delivery) => {
      const frame = nativeArrowToPolars(delivery.data) as PolarsDeliveryFrame;
      expect(frame.getColumn('BID').toArray()).toStrictEqual([3.75]);
      corruptDeliveryBuffers(delivery);
      throw new Error('synthetic failure after real Polars conversion');
    });
    await expect(failing.next()).rejects.toThrow('synthetic failure after real Polars conversion');
    const subscription = new api.Subscription(native);
    const recovered = (await subscription.deliveries({ backend: 'polars' }).next()).value;
    const frame = recovered?.data as PolarsDeliveryFrame;
    expect(frame.getColumn('BID').toArray()).toStrictEqual([3.75]);
    expect(recovered?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    expect(frame.metadata['xbbg.subscription_presence']).toBe(
      'column=__xbbg_present;encoding=binary-lsb-first;mapping=bit-i-to-schema-field-(i+2)',
    );
    await subscription.unsubscribe(false);
  });

  test('retries retained data when optional Polars is unavailable', async (context) => {
    if (hasPolars) context.skip();
    const source = await fixture();
    const subscription = new api.Subscription(
      await source.subscribe(['SYNTHETIC Equity'], ['BID']),
    );
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    await expect(subscription.deliveries({ backend: 'polars' }).next()).rejects.toBeInstanceOf(
      Error,
    );
    const recovered = (await subscription.deliveries().next()).value;
    expect(Number(recovered?.data.getChild('BID')?.get(0))).toBe(3.75);
    expect(recovered?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    await subscription.unsubscribe(false);
  });

  test('conflates real sparse updates and explicit clears into valid current-state rows', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID', 'ASK'], {
      delivery: 'state',
      flushThreshold: 2,
    });
    const subscription = new api.Subscription(native);
    source.publishJson('SYNTHETIC Equity', '{"BID":1.25,"ASK":2.25}');
    source.publishJson('SYNTHETIC Equity', '{"BID":3.75}');
    source.publishJson('SYNTHETIC Equity', '{"ASK":null}');
    expect(native.delivery).toBe('state');
    await expect(subscription.next()).rejects.toBeInstanceOf(api.BlpValidationError);
    const delivery = (await subscription.deliveries({ backend: 'json' }).next()).value;
    expect(delivery?.kind).toBe('state');
    expect(jsonValue(delivery?.data)).toStrictEqual([
      expect.objectContaining({ topic: 'SYNTHETIC Equity', BID: 3.75, ASK: null, valid: true }),
    ]);
    expect(delivery?.gapBefore).toBeUndefined();
    await subscription.unsubscribe(false);
  });

  test('re-dirties failed state projections and retries the newest source revision', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID'], { delivery: 'state' });
    source.publishJson('SYNTHETIC Equity', '{"BID":1.25}');
    const failing = new api.DeliveryStream(native, () => {
      source.publishJson('SYNTHETIC Equity', '{"BID":2.5}');
      throw new Error('synthetic state projection failure');
    });
    await expect(failing.next()).rejects.toThrow('synthetic state projection failure');
    const recovered = (await new api.Subscription(native).deliveries().next()).value;
    expect(recovered?.kind).toBe('state');
    expect(Number(recovered?.data.getChild('BID')?.get(0))).toBe(2.5);
    expect(recovered?.data.getChild('valid')?.get(0)).toBeTruthy();
    await expect(native.nextDelivery(1, 0)).resolves.toBeNull();
    await failing.unsubscribe(false);
  });

  test.each([false, true])(
    'drains pending invalidation and recovered final state, including retry: %s',
    async (retry) => {
      const source = await fixture();
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID'], {
        delivery: 'state',
        flushThreshold: 1,
      });
      const subscription = new api.Subscription(native);
      source.publishJson('SYNTHETIC Equity', '{"BID":1.25}');
      await subscription.deliveries().next();
      source.dataLoss('SYNTHETIC Equity');
      await source.settle();
      source.publishJson(
        'SYNTHETIC Equity',
        '{"BID":2.5,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}',
      );
      source.publishJson('SYNTHETIC Equity', '{"BID":3.75}');
      let index = 0;
      const failing = new api.DeliveryStream(native, (delivery) => {
        corruptDeliveryBuffers(delivery);
        const current = index;
        index += 1;
        if (current === 1) throw new Error('synthetic recovered-state drain failure');
        return delivery;
      });
      const attempt = retry ? failing.unsubscribe(true) : Promise.resolve(undefined);
      const failure = await attempt.then(
        () => undefined,
        (error: unknown) => error,
      );
      const expectedFailure = expect.any(Error);
      expect(failure).toStrictEqual(retry ? expectedFailure : undefined);
      const drained = await subscription.deliveries().unsubscribe(true);
      expect(drained).toHaveLength(2);
      expect(drained.map((delivery) => delivery.data.getChild('valid')?.get(0))).toStrictEqual([
        false,
        true,
      ]);
      expect(drained.map((delivery) => delivery.data.getChild('BID')?.get(0))).toStrictEqual([
        null,
        3.75,
      ]);
      expect(drained.map((delivery) => delivery.gapBefore)).toStrictEqual([undefined, undefined]);
    },
  );

  test('keeps SDK source DATALOSS terminal for deltas while invalidating state rows', async () => {
    const source = await fixture();
    const raw = new api.Subscription(await source.subscribe(['SYNTHETIC Equity'], ['BID']));
    const state = new api.Subscription(
      await source.subscribe(['SYNTHETIC Equity'], ['BID'], { delivery: 'state' }),
    );
    source.publishJson('SYNTHETIC Equity', '{"BID":7}');
    await raw.deliveries().next();
    expect((await state.deliveries().next()).value?.data.getChild('valid')?.get(0)).toBeTruthy();
    source.dataLoss('SYNTHETIC Equity');
    await expect(raw.deliveries().next()).rejects.toBeInstanceOf(api.BlpSubscriptionDataLossError);
    await expect(raw.deliveries().next()).resolves.toStrictEqual({ done: true, value: undefined });
    const invalid = (await state.deliveries().next()).value;
    expect(invalid?.kind).toBe('state');
    expect(invalid?.data.getChild('valid')?.get(0)).toBeFalsy();
    expect(invalid?.data.getChild('BID')?.get(0)).toBeNull();
    await source.settle();
    source.publishJson(
      'SYNTHETIC Equity',
      '{"BID":13.25,"MKTDATA_EVENT_TYPE":"SUMMARY","MKTDATA_EVENT_SUBTYPE":"INITPAINT"}',
    );
    const repaint = (await state.deliveries().next()).value;
    expect(repaint?.data.getChild('valid')?.get(0)).toBeTruthy();
    expect(Number(repaint?.data.getChild('BID')?.get(0))).toBe(13.25);
    expect(repaint?.data.getChild('live')?.get(0)).toBeFalsy();
    source.publishJson('SYNTHETIC Equity', '{"BID":14.25}');
    const live = (await state.deliveries().next()).value;
    expect(live?.data.getChild('valid')?.get(0)).toBeTruthy();
    expect(live?.data.getChild('live')?.get(0)).toBeTruthy();
    await state.unsubscribe(false);
  });

  test('drains gap envelopes through backend conversion and closes plain drains deterministically', async () => {
    const source = await fixture();
    const subscription = new api.Subscription(
      await source.subscribe(['SYNTHETIC Equity'], ['BID']),
    );
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    const drained = await subscription.deliveries({ backend: 'json' }).unsubscribe(true);
    expect(drained).toHaveLength(1);
    expect(jsonValue(drained[0]?.data)).toStrictEqual([
      expect.objectContaining({ BID: 3.75, topic: 'SYNTHETIC Equity' }),
    ]);
    expect(drained[0]?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });

    const native = await source.subscribe(['SECOND SYNTHETIC Equity'], ['BID']);
    const plain = new api.Subscription(native);
    source.publishJson('SECOND SYNTHETIC Equity', '{"BID":9}');
    await expect(plain.unsubscribe(true)).rejects.toBeInstanceOf(api.BlpValidationError);
    expect(native.isActive).toBeFalsy();
  });

  test('freezes final dirty state before unsubscribe removes aliases and memberships', async () => {
    const source = await fixture();
    const native = await source.subscribe(
      ['FIRST SYNTHETIC Equity', 'SECOND SYNTHETIC Equity'],
      ['BID'],
      {
        delivery: 'state',
        flushThreshold: 1,
        aliases: { 'FIRST SYNTHETIC Equity': 'first', 'SECOND SYNTHETIC Equity': 'second' },
      },
    );
    const subscription = new api.Subscription(native);
    source.publishJson('FIRST SYNTHETIC Equity', '{"BID":1.25}');
    source.publishJson('SECOND SYNTHETIC Equity', '{"BID":2.5}');
    const drained = await subscription.deliveries({ backend: 'json' }).unsubscribe(true);
    expect(drained).toHaveLength(2);
    expect(drained.map((delivery) => delivery.kind)).toStrictEqual(['state', 'state']);
    expect(drained.map((delivery) => delivery.gapBefore)).toStrictEqual([undefined, undefined]);
    expect(drained.flatMap((delivery) => jsonValue(delivery.data))).toStrictEqual([
      expect.objectContaining({ topic: 'first', BID: 1.25, valid: true }),
      expect.objectContaining({ topic: 'second', BID: 2.5, valid: true }),
    ]);
    expect(native.isActive).toBeFalsy();
  });

  test.each(['arrow', 'json'] as const)(
    'hands a leased late completion to its %s draining owner before projection',
    async (backend) => {
      const source = await fixture();
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
      const subscription = new api.Subscription(native);
      for (const value of [1.25, 2.5, 3.75])
        source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
      let projects = 0;
      const reader = new api.DeliveryStream(native, (delivery) => {
        projects += 1;
        return { ...delivery, data: toArrowTableFromNative(delivery.data) };
      });
      const pause = pauseNextDelivery(source, native);
      const pending = reader.next();
      await pause.ready();
      const closing =
        backend === 'arrow'
          ? reader.unsubscribe(true)
          : subscription.deliveries({ backend: 'json' }).unsubscribe(true);
      pause.release();
      await expect(pending).resolves.toStrictEqual({ done: true, value: undefined });
      const drained = await closing;
      expect(drained).toHaveLength(1);
      expect(projects).toBe(backend === 'arrow' ? 1 : 0);
      const data = drained[0]?.data;
      const rows = data instanceof Table ? [...data] : data;
      expect(jsonValue(rows)).toStrictEqual([
        expect.objectContaining({ BID: 3.75, topic: 'SYNTHETIC Equity' }),
      ]);
      expect(drained[0]?.gapBefore).toStrictEqual({ cause: 'local_overflow', droppedUpdates: 2n });
    },
  );

  test.each([false, true])(
    'rolls back reentrant close even when its projector then throws: %s',
    async (throwAfterClose) => {
      const source = await fixture();
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
      const subscription = new api.Subscription(native);
      for (const value of [1.25, 2.5, 3.75])
        source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
      const publication: { closing?: Promise<SubscriptionDelivery<unknown>[]> } = {};
      const reader = new api.DeliveryStream(native, (delivery) => {
        publication.closing = subscription.deliveries({ backend: 'json' }).unsubscribe(true);
        if (throwAfterClose)
          throw new Error('expected close handoff, not a late terminal read error');
        return { ...delivery, data: toArrowTableFromNative(delivery.data) };
      });
      await expect(reader.next()).resolves.toStrictEqual({ done: true, value: undefined });
      if (publication.closing === undefined) throw new Error('projector did not start close');
      const drained = await publication.closing;
      expect(drained).toHaveLength(1);
      expect(drained[0]?.gapBefore?.droppedUpdates).toBe(2n);
      expect(jsonValue(drained[0]?.data)).toStrictEqual([expect.objectContaining({ BID: 3.75 })]);
    },
  );

  test.each([
    { backend: 'arrow', throwInitial: false },
    { backend: 'json', throwInitial: false },
    { backend: 'arrow', throwInitial: true },
    { backend: 'json', throwInitial: true },
  ] as const)(
    'retains $backend microtask-close read and failed drain (throws=$throwInitial)',
    async ({ backend, throwInitial }) => {
      const source = await fixture(3);
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID'], { flushThreshold: 1 });
      const subscription = new api.Subscription(native);
      for (const value of [1.25, 2.5, 3.75, 4.5])
        source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
      const publication: { closing?: Promise<SubscriptionDelivery<unknown>[]> } = {};
      let initial = true;
      let drainIndex = 0;
      const reader: Core.DeliveryStream = new api.DeliveryStream(
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
            if (current === 1)
              throw new Error('synthetic later drain failure after read resolution');
          }
          return { ...delivery, data: toArrowTableFromNative(delivery.data) };
        },
      );
      const jsonOwner: Core.DeliveryStream<unknown> = new api.DeliveryStream<unknown>(
        native,
        (delivery) => {
          const current = drainIndex;
          drainIndex += 1;
          if (current === 1) throw new Error('synthetic later drain failure after read resolution');
          return { ...delivery, data: nativeArrowToBackend(delivery.data, 'json') };
        },
      );
      await expect(reader.next()).resolves.toStrictEqual({ done: true, value: undefined });
      if (publication.closing === undefined)
        throw new Error('microtask did not start draining close');
      await expect(publication.closing).rejects.toThrow(
        'synthetic later drain failure after read resolution',
      );
      const drained = await (backend === 'arrow'
        ? subscription.deliveries({ backend: 'json' }).unsubscribe(true)
        : subscription.deliveries().unsubscribe(true));
      expect(drained).toHaveLength(3);
      expect(drained.map((delivery) => delivery.gapBefore?.droppedUpdates)).toStrictEqual([
        1n,
        undefined,
        undefined,
      ]);
      for (const [index, delivery] of drained.entries()) {
        const rows = delivery.data instanceof Table ? [...delivery.data] : delivery.data;
        expect(jsonValue(rows)).toStrictEqual([
          expect.objectContaining({ BID: [2.5, 3.75, 4.5][index] }),
        ]);
      }
    },
  );

  test('skips a cancelled native projector and retains its unacknowledged data and gap for drain', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    for (const value of [1.25, 2.5, 3.75])
      source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
    let projects = 0;
    const pause = pauseNextDelivery(source, native);
    const pending = native.nextDelivery(undefined, undefined, (delivery) => {
      projects += 1;
      return delivery;
    });
    await pause.ready();
    native.signalClosing();
    pause.release();
    await expect(pending).resolves.toBeNull();
    expect(projects).toBe(0);
    const drained = await native.unsubscribeDeliveries(true, (delivery) => ({
      ...delivery,
      data: toArrowTableFromNative(delivery.data),
    }));
    expect(Number(drained?.[0]?.data.getChild('BID')?.get(0))).toBe(3.75);
    expect(drained?.[0]?.gapBefore?.droppedUpdates).toBe(2n);
  });

  test('aborts a leased native delivery without invoking its backend and closes all views', async () => {
    const source = await fixture();
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    const subscription = new api.Subscription(native);
    source.publishJson('SYNTHETIC Equity', '{"BID":9.5}');
    let projects = 0;
    const view = new api.DeliveryStream(native, (delivery) => {
      projects += 1;
      return { ...delivery, data: toArrowTableFromNative(delivery.data) };
    });
    const pause = pauseNextDelivery(source, native);
    const controller = new AbortController();
    const pending = view.next({ signal: controller.signal });
    await pause.ready();
    controller.abort();
    pause.release();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(projects).toBe(0);
    expect(native.isActive).toBeFalsy();
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(subscription.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(view.return()).resolves.toStrictEqual({ done: true, value: undefined });
  });

  test.each([0, 1])(
    'retries the entire raw drain after mutated export failure at batch %i',
    async (failAt) => {
      const source = await fixture(2);
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID'], { flushThreshold: 1 });
      for (const value of [1.25, 2.5, 3.75, 4.5])
        source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
      let index = 0;
      const failing = new api.DeliveryStream(native, (delivery) => {
        corruptDeliveryBuffers(delivery);
        const current = index;
        index += 1;
        if (current === failAt) throw new Error('synthetic raw drain conversion failure');
        return delivery;
      });
      await expect(failing.unsubscribe(true)).rejects.toThrow(
        'synthetic raw drain conversion failure',
      );
      expect(native.isActive).toBeFalsy();
      await expect(failing.next()).resolves.toStrictEqual({ done: true, value: undefined });
      const drained = await new api.Subscription(native).deliveries().unsubscribe(true);
      expect(
        drained.map((delivery) => Number(delivery.data.getChild('BID')?.get(0))),
      ).toStrictEqual([3.75, 4.5]);
      expect(drained.map((delivery) => delivery.gapBefore?.droppedUpdates)).toStrictEqual([
        2n,
        undefined,
      ]);
    },
  );

  test('retries frozen state snapshots after later mutated drain export failure and detach', async () => {
    const source = await fixture();
    const native = await source.subscribe(
      ['FIRST SYNTHETIC Equity', 'SECOND SYNTHETIC Equity'],
      ['BID'],
      {
        delivery: 'state',
        flushThreshold: 1,
        aliases: { 'FIRST SYNTHETIC Equity': 'first', 'SECOND SYNTHETIC Equity': 'second' },
      },
    );
    source.publishJson('FIRST SYNTHETIC Equity', '{"BID":1.25}');
    source.publishJson('SECOND SYNTHETIC Equity', '{"BID":2.5}');
    let index = 0;
    const failing = new api.DeliveryStream(native, (delivery) => {
      corruptDeliveryBuffers(delivery);
      const current = index;
      index += 1;
      if (current === 1) throw new Error('synthetic state drain conversion failure');
      return delivery;
    });
    await expect(failing.unsubscribe(true)).rejects.toThrow(
      'synthetic state drain conversion failure',
    );
    expect(native.isActive).toBeFalsy();
    const drained = await new api.Subscription(native)
      .deliveries({ backend: 'json' })
      .unsubscribe(true);
    expect(drained).toHaveLength(2);
    expect(drained.flatMap((delivery) => jsonValue(delivery.data))).toStrictEqual([
      expect.objectContaining({ topic: 'first', BID: 1.25, valid: true }),
      expect.objectContaining({ topic: 'second', BID: 2.5, valid: true }),
    ]);
  });

  test.each(['scalar', 'arrow', 'nativeScalar', 'nativeArrow'] as const)(
    'keeps SDK terminal failure ahead of the advanced %s plain-drain guard',
    async (mode) => {
      const source = await fixture(4);
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
      source.publishJson('SYNTHETIC Equity', '{"BID":1.25}');
      source.dataLoss('SYNTHETIC Equity');
      const subscription = new api.Subscription(native);
      const actions = {
        scalar: async () => subscription.unsubscribe(true),
        arrow: async () => subscription.arrow().unsubscribe(true),
        nativeScalar: async () => native.unsubscribe(true),
        nativeArrow: async () => native.unsubscribeArrow(true),
      };
      const failure = await actions[mode]().then(
        () => undefined,
        (error: unknown) => wrapError(error),
      );
      expect(failure).toBeInstanceOf(api.BlpSubscriptionDataLossError);
      expect(native.isActive).toBeFalsy();
    },
  );

  test('publishes a drain terminal failure before invoking any converter and consumes it once', async () => {
    const source = await fixture(4);
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    source.publishJson('SYNTHETIC Equity', '{"BID":1.25}');
    source.dataLoss('SYNTHETIC Equity');
    let projects = 0;
    const view = new api.DeliveryStream(native, () => {
      projects += 1;
      throw new Error('must not hide SDK failure');
    });
    await expect(view.unsubscribe(true)).rejects.toBeInstanceOf(api.BlpSubscriptionDataLossError);
    expect(projects).toBe(0);
    await expect(view.next()).resolves.toStrictEqual({ done: true, value: undefined });
    await expect(native.unsubscribeDeliveries(true)).resolves.toBeNull();
  });

  test.each(['value', 'kind'] as const)(
    'keeps data or SDK failure retryable after a generic prototype %s setter rejects receipt construction',
    async (field) => {
      const source = await fixture();
      const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
      const view = new api.DeliveryStream(native, (delivery) => delivery);
      source.publishJson('SYNTHETIC Equity', '{"BID":0}');
      const probe = await native.nextDelivery();
      if (probe?.kind !== 'delivery') throw new Error('missing publication probe');
      const prototype: unknown = Object.getPrototypeOf(probe);
      if (prototype === null || typeof prototype !== 'object')
        throw new Error('missing publication prototype');
      probe.commit();
      if (field === 'value') {
        for (const value of [1.25, 2.5, 3.75])
          source.publishJson('SYNTHETIC Equity', JSON.stringify({ BID: value }));
      } else {
        source.publishJson('SYNTHETIC Equity', '{"BID":1.25}');
        await view.next();
        source.dataLoss('SYNTHETIC Equity');
      }
      const previous = Object.getOwnPropertyDescriptor(prototype, field);
      let armed = true;
      let failure: unknown;
      Object.defineProperty(prototype, field, {
        configurable: true,
        get: () => undefined,
        set(this: object, value: unknown) {
          if (armed) {
            armed = false;
            throw new Error(`synthetic receipt field failure: ${field}`);
          }
          Object.defineProperty(this, field, {
            value,
            configurable: true,
            enumerable: true,
            writable: true,
          });
        },
      });
      try {
        await view.next();
      } catch (error) {
        failure = error;
      } finally {
        if (previous === undefined) Reflect.deleteProperty(prototype, field);
        else Object.defineProperty(prototype, field, previous);
      }
      expect(armed).toBeFalsy();
      expect(failure).toMatchObject({
        message: expect.stringContaining(`synthetic receipt field failure: ${field}`),
      });
      let recovered: unknown;
      if (field === 'value') {
        const subscription = new api.Subscription(native);
        const delivery = (await subscription.deliveries().next()).value;
        recovered = {
          BID: Number(delivery?.data.getChild('BID')?.get(0)),
          gap: delivery?.gapBefore?.droppedUpdates,
        };
        await subscription.unsubscribe(false);
      } else {
        const error = await view.next().then(
          () => undefined,
          (terminalFailure: unknown) => terminalFailure,
        );
        recovered = { error, ended: await view.next() };
      }
      const expectedError = expect.any(api.BlpSubscriptionDataLossError);
      expect(recovered).toMatchObject(
        field === 'value'
          ? { BID: 3.75, gap: 2n }
          : {
              error: expectedError,
              ended: { done: true, value: undefined },
            },
      );
    },
  );

  test('rejects invalid fixture field keys without dispatching or poisoning subsequent updates', async () => {
    const source = await fixture(4, 'drop_newest');
    const native = await source.subscribe(['SYNTHETIC Equity'], ['BID']);
    for (const control of [0, 1]) {
      const json = JSON.stringify({ [`BAD${String.fromCodePoint(control)}KEY`]: 1 });
      expect(() => {
        source.publishJson('SYNTHETIC Equity', json);
      }).toThrow(Error);
    }
    source.publishJson('SYNTHETIC Equity', '{"BID":2.5}');
    const subscription = new api.Subscription(native);
    const delivery = (await subscription.deliveries().next()).value;
    expect(Number(delivery?.data.getChild('BID')?.get(0))).toBe(2.5);
    expect(delivery?.gapBefore).toBeUndefined();
    await expect(native.nextDelivery(1, 0)).resolves.toBeNull();
    await subscription.unsubscribe(false);
  });
});
