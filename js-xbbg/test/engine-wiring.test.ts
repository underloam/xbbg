import type { Table } from 'apache-arrow';

import { DataType, DateUnit, TimeUnit } from 'apache-arrow';

import * as api from '../src/index';
import type {
  NativeArrowColumn,
  NativeEngine,
  NativeArrowZeroCopyBatch,
  NativeSubscription,
} from '../src/napi';
import * as subscriptions from '../src/subscriptions';
import {
  fakeNativeSubscription,
  int32ArrowBatch,
  scalarBatch,
  typedBuffer,
} from './subscription-fixtures';

// Exercise the public wrappers while replacing only their eager native-addon load.
// The resolver mock covers native-free installs; the require mock also isolates local builds.
vi.mock(import('node:module'), async (importOriginal) => {
  const actual = await importOriginal();
  const nativeAddon = {
    JsEngine: vi.fn<() => never>(() => {
      throw new Error('engine wiring tests must not instantiate the native engine');
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
function venueBatch(rows: readonly VenueFixture[]): NativeArrowZeroCopyBatch {
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

interface RecipeCase {
  name: string;
  invoke: (engine: api.Engine, options: api.RecipeBackendOptions) => Promise<unknown>;
}

const recipes: readonly RecipeCase[] = [
  { name: 'bqr', invoke: async (engine, options) => await engine.bqr('SYN', options) },
  {
    name: 'yas',
    invoke: async (engine, options) => await engine.yas('SYN', 'YAS_BOND_YLD', options),
  },
  {
    name: 'preferreds',
    invoke: async (engine, options) => await engine.preferreds('SYN', options),
  },
  {
    name: 'corporateBonds',
    invoke: async (engine, options) => await engine.corporateBonds('SYN', options),
  },
  {
    name: 'futTicker',
    invoke: async (engine, options) => await engine.futTicker('SYN', '20240102', options),
  },
  {
    name: 'activeFutures',
    invoke: async (engine, options) => await engine.activeFutures('SYN', '20240102', options),
  },
  {
    name: 'futuresCurve',
    invoke: async (engine, options) => await engine.futuresCurve('SYN', options),
  },
  {
    name: 'cdxTicker',
    invoke: async (engine, options) => await engine.cdxTicker('SYN', '20240102', options),
  },
  {
    name: 'activeCdx',
    invoke: async (engine, options) => await engine.activeCdx('SYN', '20240102', options),
  },
  {
    name: 'dividend',
    invoke: async (engine, options) =>
      await engine.dividend('SYN', '20240102', '20240103', options),
  },
  {
    name: 'dividendYield',
    invoke: async (engine, options) =>
      await engine.dividendYield('SYN', '20240102', '20240103', options),
  },
  {
    name: 'turnover',
    invoke: async (engine, options) =>
      await engine.turnover('SYN', '20240102', '20240103', options),
  },
  {
    name: 'etfHoldings',
    invoke: async (engine, options) => await engine.etfHoldings('SYN', options),
  },
  {
    name: 'volSurface',
    invoke: async (engine, options) =>
      await engine.volSurface('SYN', '20240102', '20240103', options),
  },
  {
    name: 'indexMembers',
    invoke: async (engine, options) => await engine.indexMembers('SYN', options),
  },
  {
    name: 'resolveIsins',
    invoke: async (engine, options) => await engine.resolveIsins('SYN', options),
  },
  {
    name: 'resolveVenues',
    invoke: async (engine, options) => await engine.resolveVenues('SYN', options),
  },
  {
    name: 'auctionSnapshot',
    invoke: async (engine, options) => await engine.auctionSnapshot('SYN', options),
  },
  {
    name: 'issuerIsins',
    invoke: async (engine, options) => await engine.issuerIsins('SYN', options),
  },
  {
    name: 'etfNavRelationships',
    invoke: async (engine, options) => await engine.etfNavRelationships('SYN', options),
  },
  {
    name: 'etfNavSnapshot',
    invoke: async (engine, options) => await engine.etfNavSnapshot('SYN', options),
  },
  {
    name: 'etfNavHistory',
    invoke: async (engine, options) =>
      await engine.etfNavHistory('SYN', '20240102', '20240103', options),
  },
  {
    name: 'currencyConversion',
    invoke: async (engine, options) =>
      await engine.currencyConversion('SYN', 'USD', '20240102', '20240103', options),
  },
];

describe('shared native recipe result adapter', () => {
  it.each(recipes)(
    'rejects unsupported backends before $name touches native code',
    async ({ invoke }) => {
      const nativeCall = vi.fn<() => Promise<NativeArrowZeroCopyBatch>>();
      const get = vi.fn<() => typeof nativeCall>(() => nativeCall);
      const engine = engineWithNative(new Proxy({} as NativeEngine, { get }));

      await expect(invoke(engine, { backend: 'polars' })).rejects.toThrow(
        'Polars backend requires the IPC requestRaw path',
      );
      await expect(invoke(engine, { backend: 'unsupported' as api.BackendKind })).rejects.toThrow(
        'Unsupported @xbbg/core backend',
      );
      expect(get).not.toHaveBeenCalled();
      expect(nativeCall).not.toHaveBeenCalled();
    },
  );

  it.each(recipes)('converts $name results and preserves metadata', async ({ invoke }) => {
    const nativeCall = vi.fn<() => Promise<NativeArrowZeroCopyBatch>>().mockResolvedValue(
      int32ArrowBatch(42, {
        'xbbg.eid_data': '{"SYN":[101]}',
        'xbbg.security_errors': '{"SYN":{"code":7,"message":"synthetic"}}',
        'xbbg.field_exceptions': '{"SYN":[{"field":"BID","category":"synthetic"}]}',
      }),
    );
    const engine = engineWithNative(new Proxy({} as NativeEngine, { get: () => nativeCall }));

    const rows = await invoke(engine, { backend: 'json' });

    expect(jsonRoundTrip(rows)).toStrictEqual([{ answer: 42 }]);
    expect(rows).toMatchObject({
      eidData: { SYN: [101] },
      securityErrors: { SYN: { code: 7, message: 'synthetic' } },
      fieldExceptions: { SYN: [{ field: 'BID', category: 'synthetic' }] },
    });
    expect(nativeCall).toHaveBeenCalledTimes(1);
  });

  it('wraps native recipe failures using the public error hierarchy', async () => {
    const engine = engineWithNative({
      recipeResolveIsins: async () => {
        throw new Error('[XBBG:VALIDATION] synthetic recipe failure');
      },
    });

    await expect(engine.resolveIsins('SYN')).rejects.toBeInstanceOf(api.BlpValidationError);
  });

  it('forwards only the supported corporate bond arguments', async () => {
    const recipeCorporateBonds = vi
      .fn<NativeEngine['recipeCorporateBonds']>()
      .mockResolvedValue(int32ArrowBatch(1));
    const engine = engineWithNative({ recipeCorporateBonds });

    await engine.corporateBonds('SYN', { ccy: 'USD', fields: ['BID'] });

    expect(recipeCorporateBonds).toHaveBeenCalledWith('SYN', 'USD', ['BID']);
  });

  it('preserves the subscription class identities at the package root', () => {
    expect(api.Subscription).toBe(subscriptions.Subscription);
    expect(api.ArrowSubscription).toBe(subscriptions.ArrowSubscription);
    expect(api.FieldHandle).toBe(subscriptions.FieldHandle);
    expect(api.Tick).toBe(subscriptions.Tick);
  });
});

describe('subscription service option forwarding', () => {
  it.each([
    { method: 'stream', service: '//blp/mktdata' },
    { method: 'vwap', service: '//blp/mktvwap' },
    { method: 'mktbar', service: '//blp/mktbar' },
    { method: 'depth', service: '//blp/mktdepthdata' },
    { method: 'chains', service: '//blp/mktlist' },
  ] as const)('forwards every control through $method', async ({ method, service }) => {
    const subscribeWithOptions = vi
      .fn<NativeEngine['subscribeWithOptions']>()
      .mockResolvedValue(fakeNativeSubscription());
    const engine = engineWithNative({ subscribeWithOptions });
    const options: api.StreamOptions = {
      options: [' &interval=1 ', ' '],
      flushThreshold: 5,
      overflowPolicy: 'drop_newest',
      streamCapacity: 8,
      allFields: true,
      aliases: { SYN: 'label' },
      onDelayed: 'ignore',
      isolated: true,
      rows: false,
      onFieldError: 'raise',
      zeroAsNull: ['BID'],
    };
    await (method === 'stream' || method === 'vwap'
      ? engine[method](['SYN'], ['BID'], options)
      : engine[method]('SYN', { ...options, fields: ['BID'] }));

    expect(subscribeWithOptions).toHaveBeenCalledWith(
      service,
      ['SYN'],
      ['BID'],
      ['interval=1'],
      5,
      'drop_newest',
      8,
      true,
      { SYN: 'label' },
      'ignore',
      true,
      false,
      'raise',
      ['BID'],
    );
  });

  it('preserves raw positional wire options and native policy strings', async () => {
    const subscribeWithOptions = vi
      .fn<NativeEngine['subscribeWithOptions']>()
      .mockResolvedValue(fakeNativeSubscription());
    const engine = engineWithNative({ subscribeWithOptions });

    await engine.subscribeWithOptions(
      '//blp/mktdata',
      ['SYN'],
      ['BID'],
      [' &raw=1 '],
      5,
      'native-policy',
      8,
      true,
      { SYN: 'label' },
      'ignore',
      true,
      false,
      'raise',
      ['BID'],
    );

    expect(subscribeWithOptions).toHaveBeenCalledWith(
      '//blp/mktdata',
      ['SYN'],
      ['BID'],
      [' &raw=1 '],
      5,
      'native-policy',
      8,
      true,
      { SYN: 'label' },
      'ignore',
      true,
      false,
      'raise',
      ['BID'],
    );
  });

  it('rejects conflicting or unsupported conflation before calling native', async () => {
    const subscribeWithOptions = vi.fn<NativeEngine['subscribeWithOptions']>();
    const engine = engineWithNative({ subscribeWithOptions });

    await expect(
      engine.stream(['SYN'], ['BID'], {
        conflate: true,
        options: [' &INTERVAL=1'],
      }),
    ).rejects.toThrow('conflate=true cannot be combined with interval');
    await expect(engine.vwap(['SYN'], ['BID'], { conflate: true })).rejects.toThrow(
      'conflate=true is only supported for //blp/mktdata',
    );
    expect(subscribeWithOptions).not.toHaveBeenCalled();
  });
});
