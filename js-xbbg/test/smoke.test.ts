import { expectTypeOf } from 'vitest';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import * as api from '../src/index';
import type {
  BackendKind,
  EntitlementReport,
  FormatKind,
  OverridesInput,
  RequestOptions,
  ResultMetadata,
  SeatType,
  StreamOptions,
} from '../src/index';
import type { NativeArrowColumn, NativeArrowZeroCopyBatch } from '../src/napi';
import type { RequestInput } from '../src/types';

const SESSION_HOST = process.env.XBBG_HOST ?? 'localhost';
const SESSION_PORT = Number(process.env.XBBG_PORT ?? 8194);

function nativeUnavailable(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err);
  return (
    message.includes('Unable to load native napi-xbbg module') ||
    message.toLowerCase().includes('session start failed') ||
    message.toLowerCase().includes('failed to spawn worker')
  );
}

function fakeNativeSubscription(): any {
  return {
    add: async () => undefined,
    fields: ['BID', 'ASK'],
    isActive: true,
    nextArrowBatch: async () => null,
    nextUpdates: async () => null,
    remove: async () => undefined,
    stats: {
      batchesSent: 0,
      dataLossEvents: 0,
      droppedBatches: 0,
      effectiveOverflowPolicy: 'drop_newest',
      lastDataLossUs: 0,
      lastMessageUs: 0,
      messagesReceived: 0,
      slowConsumer: false,
    },
    tickers: ['ES1 Index'],
    unsubscribe: async () => [],
    unsubscribeArrow: async () => [],
  };
}

function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

function metadataBatch(metadata: Record<string, string>): NativeArrowZeroCopyBatch {
  return {
    columns: [],
    kind: 'zeroCopy',
    metadata,
    numRows: 0,
  };
}

async function expectValidationError(promise: Promise<unknown>, message: string): Promise<void> {
  const error = await promise.then(
    () => null,
    (raised: unknown) => raised,
  );
  expect(error).toBeInstanceOf(api.BlpValidationError);
  const validation = error as InstanceType<typeof api.BlpValidationError>;
  expect(validation.message).toBe(message);
  expect(validation.element).toBe('tickers');
}

function captureRequests(): api.Engine & { readonly calls: RequestInput[] } {
  const calls: RequestInput[] = [];
  const engine = Object.create(api.Engine.prototype) as api.Engine & {
    calls: RequestInput[];
    request(params: RequestInput): Promise<unknown>;
  };
  engine.calls = calls;
  engine.request = async (params: RequestInput): Promise<unknown> => {
    calls.push(params);
    return Promise.resolve(params);
  };
  return engine;
}

describe('@xbbg/core surface', () => {
  it('exposes all public exports', () => {
    const required = [
      'Engine',
      'Subscription',
      'ArrowSubscription',
      'connect',
      'configure',
      'blp',
      'ext',
      'Backend',
      'Format',
      'OverrideSpec',
      'bdp',
      'bdh',
      'bds',
      'bdib',
      'bdtick',
      'subscribe',
      'abdp',
      'abdh',
      'abds',
      'abdib',
      'abdtick',
      'asubscribe',
      'BlpError',
      'BlpSessionError',
      'BlpRequestError',
      'BlpSubscriptionDataLossError',
      'BlpValidationError',
      'BlpTimeoutError',
      'BlpInternalError',
      'wrapError',
      'version',
      'setLogLevel',
      'getLogLevel',
      'formatDate',
      'formatDateTime',
      'ovr',
    ] as const;
    for (const key of required) {
      expect(api).toHaveProperty(key);
    }
  });
  it('keeps representative public type exports available', () => {
    const seatType: SeatType = 'BPS';
    const entitlementReport: EntitlementReport = { entitled: false, failedEids: [101] };
    const resultMetadata: ResultMetadata = {
      eidData: { 'IBM US Equity': [101] },
      metadata: { 'xbbg.eid_data': '{"IBM US Equity":[101]}' },
    };

    expectTypeOf(seatType).toExtend<SeatType>();
    expectTypeOf(entitlementReport).toEqualTypeOf<EntitlementReport>();
    expectTypeOf(resultMetadata).toEqualTypeOf<ResultMetadata>();
    const backend: BackendKind = api.Backend.ARROW;
    const format: FormatKind = api.Format.LONG_TYPED;
    const requestOptions: RequestOptions = { backend, format };
    const streamOptions: StreamOptions = { allFields: true };
    const overrides: OverridesInput = api.ovr({
      EQY_FUND_CRNCY: 'USD',
      'IBM US Equity': api.ovr({ EQY_FUND_CRNCY: 'EUR' }),
    });
    const requestWithOverrides: RequestOptions = { backend, format, overrides };

    expectTypeOf(backend).toExtend<BackendKind>();
    expectTypeOf(format).toExtend<FormatKind>();
    expectTypeOf(requestOptions).toEqualTypeOf<RequestOptions>();
    expectTypeOf(streamOptions).toEqualTypeOf<StreamOptions>();
    expectTypeOf(overrides).toExtend<OverridesInput>();
    expectTypeOf(requestWithOverrides).toEqualTypeOf<RequestOptions>();
  });

  it('normalizes JavaScript override specs', () => {
    const spec = api
      .ovr({
        EQY_FUND_CRNCY: 'EUR',
        USER_LOCAL_TRADE_DATE: new Date(Date.UTC(2023, 0, 17)),
      })
      .merge({ EQY_FUND_CRNCY: 'USD' });

    expect(spec.toPairs()).toStrictEqual([
      { key: 'EQY_FUND_CRNCY', value: 'USD' },
      { key: 'USER_LOCAL_TRADE_DATE', value: '20230117' },
    ]);
    expect(spec.toObject()).toStrictEqual({
      EQY_FUND_CRNCY: 'USD',
      USER_LOCAL_TRADE_DATE: '20230117',
    });
    expect(spec.toSecurityOverrides()).toStrictEqual([]);
    expect(
      spec
        .forSecurity('IBM US Equity', { EQY_FUND_CRNCY: 'EUR' })
        .merge({ 'MSFT US Equity': { USER_LOCAL_TRADE_DATE: new Date(Date.UTC(2024, 0, 2)) } })
        .toSecurityOverrides(),
    ).toStrictEqual([
      {
        overrides: [{ key: 'EQY_FUND_CRNCY', value: 'EUR' }],
        security: 'IBM US Equity',
      },
      {
        overrides: [{ key: 'USER_LOCAL_TRADE_DATE', value: '20240102' }],
        security: 'MSFT US Equity',
      },
    ]);
    expect(() => api.ovr('BAD' as never)).toThrow(
      'ovr() expects objects, OverrideSpec, or arrays of override entries',
    );
  });

  it('backend enum is frozen with correct values', () => {
    expect(Object.isFrozen(api.Backend)).toBeTruthy();
    expect(api.Backend.ARROW).toBe('arrow');
    expect(api.Backend.JSON).toBe('json');
    expect(api.Backend.POLARS).toBe('polars');
    expect(Object.keys(api.Backend)).toHaveLength(3);
  });

  it('format enum is frozen with correct values', () => {
    expect(Object.isFrozen(api.Format)).toBeTruthy();
    expect(api.Format.LONG).toBe('long');
    expect(api.Format.LONG_TYPED).toBe('long_typed');
    // 'long_with_metadata' is rejected by the engine; the wire value is 'long_metadata'.
    expect(api.Format.LONG_WITH_METADATA).toBe('long_metadata');
    expect(api.Format.SEMI_LONG).toBe('semi_long');
    expect(Object.keys(api.Format)).toHaveLength(4);
  });

  it('has a correct error class hierarchy', () => {
    expect(api.BlpError.prototype).toBeInstanceOf(Error);
    expect(api.BlpSessionError.prototype).toBeInstanceOf(api.BlpError);
    expect(api.BlpRequestError.prototype).toBeInstanceOf(api.BlpError);
    expect(api.BlpSubscriptionDataLossError.prototype).toBeInstanceOf(api.BlpError);
    expect(api.BlpValidationError.prototype).toBeInstanceOf(api.BlpError);
    expect(api.BlpTimeoutError.prototype).toBeInstanceOf(api.BlpError);
    expect(api.BlpInternalError.prototype).toBeInstanceOf(api.BlpError);
  });

  it('sets the .name property on error instances', () => {
    expect(new api.BlpError('test').name).toBe('BlpError');
    expect(new api.BlpSessionError('test').name).toBe('BlpSessionError');
    expect(new api.BlpRequestError('test').name).toBe('BlpRequestError');
    expect(new api.BlpSubscriptionDataLossError('test').name).toBe('BlpSubscriptionDataLossError');
    expect(new api.BlpValidationError('test').name).toBe('BlpValidationError');
    expect(new api.BlpTimeoutError('test').name).toBe('BlpTimeoutError');
    expect(new api.BlpInternalError('test').name).toBe('BlpInternalError');
  });

  it('blpRequestError carries optional properties', () => {
    const err = new api.BlpRequestError('test', {
      code: 123,
      operation: 'BDP',
      service: '//blp/refdata',
    });
    expect(err.service).toBe('//blp/refdata');
    expect(err.operation).toBe('BDP');
    expect(err.code).toBe(123);
  });

  it('blpValidationError carries optional properties', () => {
    const err = new api.BlpValidationError('test', {
      element: 'field1',
      suggestion: 'PX_LAST',
    });
    expect(err.element).toBe('field1');
    expect(err.suggestion).toBe('PX_LAST');
  });

  it('wrapError maps NAPI error prefixes to correct classes', () => {
    const wrapCases: [string, new (...args: any[]) => Error][] = [
      ['Session start failed: x', api.BlpSessionError],
      ['Failed to open service: x', api.BlpSessionError],
      ['Request failed: x', api.BlpRequestError],
      ['Subscription failed: x', api.BlpRequestError],
      ['Invalid argument: x', api.BlpValidationError],
      ['Request timed out', api.BlpTimeoutError],
      ['Internal error: x', api.BlpInternalError],
      ['some unknown error', api.BlpError],
    ];
    for (const [msg, Cls] of wrapCases) {
      expect(api.wrapError(new Error(msg))).toBeInstanceOf(Cls);
    }
  });

  it('wrapError exposes structured subscription data-loss details', () => {
    const dataLoss = api.wrapError(
      new Error(
        '[XBBG:DATALOSS] Subscription data loss [topic=IBM US Equity]: stream queue reached capacity',
      ),
    );
    expect(dataLoss).toBeInstanceOf(api.BlpSubscriptionDataLossError);
    expect(dataLoss).toMatchObject({
      code: 'DATALOSS',
      detail: 'stream queue reached capacity',
      topic: 'IBM US Equity',
    });
  });

  it('wrapError preserves typed errors', () => {
    const err = new api.BlpValidationError('typed validation');
    expect(api.wrapError(err)).toBe(err);
  });

  it('exposes version, connect, setLogLevel, getLogLevel as functions', () => {
    expectTypeOf(api.connect).toBeFunction();
    expectTypeOf(api.version).toBeFunction();
    expectTypeOf(api.setLogLevel).toBeFunction();
    expectTypeOf(api.getLogLevel).toBeFunction();
    expectTypeOf(api.version()).toBeString();
    expect(api.version().length).toBeGreaterThan(0);
  });

  it('configure accepts both flat and nested config shapes', () => {
    expect(api.configure({ host: SESSION_HOST, port: SESSION_PORT })).toStrictEqual({
      host: SESSION_HOST,
      port: SESSION_PORT,
    });
    const advanced = {
      auth: { appName: 'my-bpipe-app', method: 'userapp' as const },
      retryPolicy: {
        backoffFactor: 1.5,
        initialDelayMs: 100,
        maxDelayMs: 1000,
        maxRetries: 2,
      },
      servers: [
        { host: 'primary.example.com', port: 8194 },
        { host: 'secondary.example.com', port: 8196 },
      ],
      socks5: { host: 'proxy.example.com', port: 1080 },
      tls: {
        clientCredentials: '/secure/client.p12',
        trustMaterial: '/secure/trust.p7',
      },
      shardChunkSize: 3,
      shardMaxConcurrent: 2,
      shardRequests: true,
      shardThreshold: 5,
      zfpRemote: '8194' as const,
    };
    expect(api.configure(advanced)).toStrictEqual(advanced);
    expect(api.configure(SESSION_HOST, SESSION_PORT)).toStrictEqual({
      host: SESSION_HOST,
      port: SESSION_PORT,
    });
  });

  it('blp namespace exposes Python-style helpers', () => {
    const methods = [
      'bdp',
      'bdh',
      'bds',
      'bdib',
      'bdtick',
      'subscribe',
      'abdp',
      'abdh',
      'abds',
      'abdib',
      'abdtick',
      'asubscribe',
    ] as const;
    for (const m of methods) {
      expectTypeOf(api.blp[m]).toBeFunction();
    }
  });

  it('ext.cdx namespace exposes acdx_* helpers', () => {
    for (const m of ['acdx_info', 'acdx_pricing', 'acdx_risk'] as const) {
      expect(typeof (api.ext.cdx as any)[m]).toBe('function');
    }
  });
});

describe('conflated market data options', () => {
  function fakeEngine(captured: Record<string, unknown>): api.Engine {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    (engine as unknown as { inner: unknown }).inner = {
      subscribe: async (
        tickers: readonly string[],
        fields: readonly string[],
        allFields?: boolean,
      ) => {
        captured.subscribe = { allFields, fields, tickers };
        return fakeNativeSubscription();
      },
      subscribeWithOptions: async (
        service: string,
        tickers: readonly string[],
        fields: readonly string[],
        options?: readonly string[],
        flushThreshold?: number,
        overflowPolicy?: string,
        streamCapacity?: number,
        allFields?: boolean,
      ) => {
        captured.subscribeWithOptions = {
          allFields,
          fields,
          flushThreshold,
          options,
          overflowPolicy,
          service,
          streamCapacity,
          tickers,
        };
        return fakeNativeSubscription();
      },
    };
    return engine;
  }

  it('adds the conflate Bloomberg option for mktdata subscriptions', async () => {
    const captured: Record<string, unknown> = {};
    await fakeEngine(captured).subscribe(['ES1 Index'], ['BID', 'ASK'], { conflate: true });

    expect(captured.subscribe).toBeUndefined();
    expect(captured.subscribeWithOptions).toMatchObject({
      fields: ['BID', 'ASK'],
      options: ['conflate'],
      service: '//blp/mktdata',
      tickers: ['ES1 Index'],
    });
  });

  it('normalizes ampersand conflate and avoids duplicates', async () => {
    const captured: Record<string, unknown> = {};
    await fakeEngine(captured).subscribe(['ES1 Index'], ['BID', 'ASK'], {
      conflate: true,
      options: ['&conflate'],
    });

    expect(captured.subscribeWithOptions).toMatchObject({
      options: ['conflate'],
    });
  });

  it('rejects conflate for non-mktdata helpers', async () => {
    await expect(
      fakeEngine({}).vwap(['IBM US Equity'], ['VWAP'], { conflate: true }),
    ).rejects.toBeInstanceOf(api.BlpValidationError);
  });

  it('rejects conflate with interval options', async () => {
    await expect(
      fakeEngine({}).subscribe(['ES1 Index'], ['BID', 'ASK'], {
        conflate: true,
        options: ['interval=5'],
      }),
    ).rejects.toBeInstanceOf(api.BlpValidationError);
  });
});

describe('native Arrow zero-copy table construction', () => {
  it('constructs an Arrow table from native buffer descriptors', () => {
    const prices = new Float64Array([50_000.5, 0]);
    const sizes = new Int32Array([10, 20]);
    const offsets = new Int32Array([0, 13, 26]);
    const text = Buffer.from('XBTUSD CurncyIBM US Equity');
    const updateTime = new BigInt64Array([45_000_000_000n, 45_000_001_000n]);
    const quantities = new Uint32Array([1, 4_000_000_000]);
    const yields = new Float32Array([1.25, 2.5]);
    const tradeDates = new BigInt64Array([1_700_000_000_000n, 1_700_086_400_000n]);
    const binaryOffsets = new Int32Array([0, 2, 5]);
    const binaryValues = Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x01]);
    const batch: NativeArrowZeroCopyBatch = {
      columns: [
        {
          name: 'topic',
          type: 'utf8',
          nullable: false,
          length: 2,
          nullCount: 0,
          offsets: typedBuffer(offsets),
          data: text,
        },
        {
          name: 'LAST_PRICE',
          type: 'float64',
          nullable: true,
          length: 2,
          nullCount: 1,
          nullBitmap: Buffer.from([0b00000001]),
          data: typedBuffer(prices),
        },
        {
          name: 'SIZE',
          type: 'int32',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: typedBuffer(sizes),
        },
        {
          name: 'UPDATE_TIME',
          type: 'time64_us',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: typedBuffer(updateTime),
        },
        {
          name: 'QUANTITY',
          type: 'uint32',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: typedBuffer(quantities),
        },
        {
          name: 'YIELD',
          type: 'float32',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: typedBuffer(yields),
        },
        {
          name: 'TRADE_DATE',
          type: 'date64',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: typedBuffer(tradeDates),
        },
        {
          name: 'PAYLOAD',
          type: 'binary',
          nullable: false,
          length: 2,
          nullCount: 0,
          offsets: typedBuffer(binaryOffsets),
          data: binaryValues,
        },
      ],
      kind: 'zeroCopy',
      numRows: 2,
      metadata: { 'xbbg.eid_data': '{"IBM US Equity":[101]}' },
    };

    const table = tableFromNativeArrowBatch(batch);

    expect(table.numRows).toBe(2);
    expect(table.getChild('topic')?.get(0)).toBe('XBTUSD Curncy');
    expect(table.getChild('topic')?.get(1)).toBe('IBM US Equity');
    expect(table.getChild('LAST_PRICE')?.get(0)).toBe(50_000.5);
    expect(table.getChild('LAST_PRICE')?.get(1)).toBeNull();
    expect(table.getChild('SIZE')?.get(1)).toBe(20);
    expect(table.getChild('UPDATE_TIME')?.get(0)).toBe(45_000_000_000n);
    expect(table.getChild('QUANTITY')?.get(1)).toBe(4_000_000_000);
    expect(table.getChild('YIELD')?.get(0)).toBeCloseTo(1.25);
    expect(table.getChild('TRADE_DATE')?.get(0)).toBe(1_700_000_000_000);
    expect([...(table.getChild('PAYLOAD')?.get(1) ?? [])]).toStrictEqual([0xbe, 0xef, 0x01]);
    expect(table.schema.metadata.get('xbbg.eid_data')).toBe('{"IBM US Equity":[101]}');
  });

  it('rejects native descriptors whose primitive data buffer is too small', () => {
    const batch: NativeArrowZeroCopyBatch = {
      kind: 'zeroCopy',
      numRows: 2,
      columns: [
        {
          name: 'LAST_PRICE',
          type: 'float64',
          nullable: false,
          length: 2,
          nullCount: 0,
          data: Buffer.alloc(Float64Array.BYTES_PER_ELEMENT),
        },
      ],
      metadata: {},
    };

    expect(() => tableFromNativeArrowBatch(batch)).toThrow(/LAST_PRICE data buffer is too small/u);
  });

  it('subscription.next uses native updates', async () => {
    const sub = new api.Subscription({
      add: async () => {},
      fields: [],
      isActive: true,
      nextArrowBatch: async () => Promise.resolve(null),
      nextUpdates: async () =>
        Promise.resolve({
          kind: 'batch',
          layout: { fields: ['answer'], kinds: ['i32'], version: 1 },
          updates: [
            {
              boolValues: [null],
              f64Values: [null],
              fieldIndices: [0],
              i32Values: [42],
              i64Values: [null],
              layoutVersion: 1,
              stringValues: [null],
              timestampUs: 123,
              topic: 'XBTUSD Curncy',
              topicId: 1,
            },
          ],
        }),
      remove: async () => {},
      stats: { batchesSent: 0, droppedBatches: 0, messagesReceived: 0, slowConsumer: false },
      tickers: [],
      unsubscribe: async () => Promise.resolve(null),
      unsubscribeArrow: async () => Promise.resolve(null),
    });

    const result = await sub.next();

    expect(result.done).toBeFalsy();
    expect(result.value?.topic).toBe('XBTUSD Curncy');
    expect(result.value?.f64('answer')).toBe(42);
  });

  it('subscription.arrow drains native zero-copy batches', async () => {
    const values = new Int32Array([7]);
    const batch: NativeArrowZeroCopyBatch = {
      columns: [
        {
          name: 'answer',
          type: 'int32',
          nullable: false,
          length: 1,
          nullCount: 0,
          data: typedBuffer(values),
        },
      ],
      kind: 'zeroCopy',
      numRows: 1,
      metadata: { 'xbbg.eid_data': '{"IBM US Equity":[101]}' },
    };
    const sub = new api.Subscription({
      add: async () => {},
      fields: [],
      isActive: true,
      nextArrowBatch: async () => Promise.resolve(null),
      nextUpdates: async () => Promise.resolve(null),
      remove: async () => {},
      stats: { batchesSent: 0, droppedBatches: 0, messagesReceived: 0, slowConsumer: false },
      tickers: [],
      unsubscribe: async () => Promise.resolve(null),
      unsubscribeArrow: async (drain) => Promise.resolve(drain ? [batch] : null),
    });

    const drained = await sub.arrow().unsubscribe(true);

    expect(drained).toHaveLength(1);
    expect(drained[0]?.getChild('answer')?.get(0)).toBe(7);
    const drainedTable = drained[0];
    if (drainedTable === undefined) {
      throw new Error('expected drained table');
    }
    const metadataResult = drainedTable as unknown as ResultMetadata;
    expect(metadataResult.metadata).toStrictEqual({ 'xbbg.eid_data': '{"IBM US Equity":[101]}' });
    expect(metadataResult.eidData).toStrictEqual({ 'IBM US Equity': [101] });
    expect(metadataResult.securityErrors).toBeUndefined();
    expect(metadataResult.fieldExceptions).toBeUndefined();
  });

  it('parses JSON result metadata and ignores absent or malformed convenience payloads', async () => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    const batches = [
      metadataBatch({
        'xbbg.eid_data': '{"IBM US Equity":[101,202]}',
        'xbbg.field_exceptions':
          '{"IBM US Equity":[{"field":"PX_BAD","category":"BAD_FLD","code":9,"subcategory":"INVALID_FIELD","message":"bad field"}]}',
        'xbbg.security_errors':
          '{"MSFT US Equity":{"category":"BAD_SEC","code":"10","subcategory":"INVALID_SECURITY","message":"bad security"}}',
      }),
      metadataBatch({}),
      metadataBatch({ 'xbbg.eid_data': '{not-json' }),
    ];
    Reflect.set(engine, 'inner', {
      request: async () => {
        const next = batches.shift();
        if (next === undefined) {
          throw new Error('unexpected request');
        }
        return next;
      },
    });

    const valid = await engine.request({
      backend: api.Backend.JSON,
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    });
    const validMetadata = valid as ResultMetadata;
    expect(Array.isArray(valid)).toBeTruthy();
    expect(validMetadata.eidData).toStrictEqual({ 'IBM US Equity': [101, 202] });
    expect(validMetadata.securityErrors).toStrictEqual({
      'MSFT US Equity': {
        category: 'BAD_SEC',
        code: '10',
        message: 'bad security',
        subcategory: 'INVALID_SECURITY',
      },
    });
    expect(validMetadata.fieldExceptions).toStrictEqual({
      'IBM US Equity': [
        {
          category: 'BAD_FLD',
          code: 9,
          field: 'PX_BAD',
          message: 'bad field',
          subcategory: 'INVALID_FIELD',
        },
      ],
    });

    const absent = await engine.request({
      backend: api.Backend.JSON,
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    });
    const absentMetadata = absent as ResultMetadata;
    expect(absentMetadata.metadata).toStrictEqual({});
    expect(absentMetadata.eidData).toBeUndefined();

    const malformed = await engine.request({
      backend: api.Backend.JSON,
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    });
    const malformedMetadata = malformed as ResultMetadata;
    expect(malformedMetadata.metadata).toStrictEqual({ 'xbbg.eid_data': '{not-json' });
    expect(malformedMetadata.eidData).toBeUndefined();
  });
  it.each([
    ['ReferenceDataRequest', 'IBM US Equity'],
    ['HistoricalDataRequest', 'MSFT US Equity'],
    ['IntradayBarRequest', 'AAPL US Equity'],
    ['IntradayTickRequest', 'NVDA US Equity'],
  ])('exposes %s EIDs on empty JSON results', async (operation, security) => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    const eidData = { [security]: [101, 202] };
    Reflect.set(engine, 'inner', {
      request: async () =>
        metadataBatch({
          'xbbg.eid_data': JSON.stringify(eidData),
        }),
    });

    const result = await engine.request({
      backend: api.Backend.JSON,
      operation,
      service: '//blp/refdata',
    });
    const resultMetadata = result as ResultMetadata;

    expect(result).toHaveLength(0);
    expect(resultMetadata.eidData).toStrictEqual(eidData);
    expect(resultMetadata.metadata).toStrictEqual({
      'xbbg.eid_data': JSON.stringify(eidData),
    });
  });

  it('keeps generic result metadata on the result container rather than JSON rows', async () => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    Reflect.set(engine, 'inner', {
      request: async (): Promise<NativeArrowZeroCopyBatch> => ({
        columns: [
          {
            data: typedBuffer(new Int32Array([42])),
            length: 1,
            name: 'value',
            nullCount: 0,
            nullable: false,
            type: 'int32',
          },
        ],
        kind: 'zeroCopy',
        metadata: { 'xbbg.eid_data': '{"IBM US Equity":[101]}' },
        numRows: 1,
      }),
    });

    const result = (await engine.request({
      backend: api.Backend.JSON,
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    })) as unknown[] & ResultMetadata;

    expect(result).toHaveLength(1);
    expect(result.eidData).toStrictEqual({ 'IBM US Equity': [101] });
    expect(result[0]).toMatchObject({ value: 42 });
    expect(result[0]).not.toHaveProperty('eidData');
    expect(result[0]).not.toHaveProperty('metadata');
  });

  it('passes EIDs flattened from result metadata to entitlement checks', async () => {
    const entitlementCalls: { eids: readonly number[]; service: string }[] = [];
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    Reflect.set(engine, 'inner', {
      checkEntitlements: async (service: string, eids: readonly number[]) => {
        entitlementCalls.push({ eids, service });
        return { entitled: false, failedEids: [202] };
      },
      request: async () =>
        metadataBatch({
          'xbbg.eid_data': '{"IBM US Equity":[101,202],"MSFT US Equity":[303]}',
        }),
    });

    const result = (await engine.request({
      backend: api.Backend.JSON,
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    })) as ResultMetadata;
    const eids = Object.values(result.eidData ?? {}).flat();
    const report = await engine.checkEntitlements('//blp/refdata', eids);

    expect(entitlementCalls).toStrictEqual([{ eids: [101, 202, 303], service: '//blp/refdata' }]);
    expect(report).toStrictEqual({ entitled: false, failedEids: [202] });
  });
});

describe('engine wrapper request plumbing', () => {
  it('forwards per-request validation toggles for reference and history wrappers', async () => {
    const engine = captureRequests();

    await engine.bdp(['IBM US Equity'], ['PX_LAST'], { validateFields: true });
    await engine.bds(['IBM US Equity'], ['DVD_HIST'], { validateFields: false });
    await engine.bdh(['IBM US Equity'], ['PX_LAST'], {
      end: '2024-01-02',
      start: '2024-01-01',
      validateFields: true,
    });

    expect(engine.calls[0]?.validateFields).toBeTruthy();
    expect(engine.calls[1]?.validateFields).toBeFalsy();
    expect(engine.calls[2]?.validateFields).toBeTruthy();
  });

  it('forwards JavaScript override specs through bdp and bdh', async () => {
    const engine = captureRequests();

    await engine.bdp(['IBM US Equity'], ['PX_LAST'], {
      overrides: api.ovr({ EQY_FUND_CRNCY: 'EUR' }),
    });
    await engine.bdh(['IBM US Equity'], ['PX_LAST'], {
      end: '2024-01-02',
      overrides: api.ovr([['EQY_FUND_CRNCY', 'USD']]),
      start: '2024-01-01',
    });

    const bdpOverrides = engine.calls[0]?.overrides;
    const bdhOverrides = engine.calls[1]?.overrides;

    expect(bdpOverrides).toBeInstanceOf(api.OverrideSpec);
    expect((bdpOverrides as api.OverrideSpec).toPairs()).toStrictEqual([
      { key: 'EQY_FUND_CRNCY', value: 'EUR' },
    ]);
    expect(bdhOverrides).toBeInstanceOf(api.OverrideSpec);
    expect((bdhOverrides as api.OverrideSpec).toPairs()).toStrictEqual([
      { key: 'EQY_FUND_CRNCY', value: 'USD' },
    ]);
  });

  it('carries per-security overrides inside JavaScript override specs', async () => {
    const engine = captureRequests();

    await engine.bdp(['IBM US Equity', 'MSFT US Equity'], ['PX_LAST'], {
      overrides: api.ovr({
        EQY_FUND_CRNCY: 'USD',
        'IBM US Equity': api.ovr({ EQY_FUND_CRNCY: 'EUR' }),
        'MSFT US Equity': { USER_LOCAL_TRADE_DATE: new Date(Date.UTC(2024, 0, 2)) },
      }),
    });
    await engine.bds(['IBM US Equity'], ['DVD_HIST_ALL'], {
      overrides: api.ovr({
        'IBM US Equity': { DVD_Start_Dt: new Date(Date.UTC(2024, 0, 1)) },
      }),
    });
    await engine.bdh(['IBM US Equity'], ['PX_LAST'], {
      end: '2024-01-02',
      overrides: api.ovr().forSecurity('IBM US Equity', { CRNCY: 'EUR' }),
      start: '2024-01-01',
    });

    const refOverrides = engine.calls[0]?.overrides;
    const bulkOverrides = engine.calls[1]?.overrides;
    const histOverrides = engine.calls[2]?.overrides;

    expect(refOverrides).toBeInstanceOf(api.OverrideSpec);
    expect((refOverrides as api.OverrideSpec).toPairs()).toStrictEqual([
      { key: 'EQY_FUND_CRNCY', value: 'USD' },
    ]);
    expect((refOverrides as api.OverrideSpec).toSecurityOverrides()).toStrictEqual([
      {
        overrides: [{ key: 'EQY_FUND_CRNCY', value: 'EUR' }],
        security: 'IBM US Equity',
      },
      {
        overrides: [{ key: 'USER_LOCAL_TRADE_DATE', value: '20240102' }],
        security: 'MSFT US Equity',
      },
    ]);
    expect(bulkOverrides).toBeInstanceOf(api.OverrideSpec);
    expect((bulkOverrides as api.OverrideSpec).toSecurityOverrides()).toStrictEqual([
      {
        overrides: [{ key: 'DVD_Start_Dt', value: '20240101' }],
        security: 'IBM US Equity',
      },
    ]);
    expect(histOverrides).toBeInstanceOf(api.OverrideSpec);
    expect((histOverrides as api.OverrideSpec).toSecurityOverrides()).toStrictEqual([
      {
        overrides: [{ key: 'CRNCY', value: 'EUR' }],
        security: 'IBM US Equity',
      },
    ]);
  });

  it('splits OverrideSpec security overrides at the native request boundary', async () => {
    const captured: unknown[] = [];
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    Reflect.set(engine, 'inner', {
      requestRaw: async (params: unknown) => {
        captured.push(params);
        return Buffer.from([]);
      },
    });

    await engine.requestRaw({
      fields: ['PX_LAST'],
      operation: 'ReferenceDataRequest',
      overrides: api.ovr({
        EQY_FUND_CRNCY: 'USD',
        'IBM US Equity': { EQY_FUND_CRNCY: 'EUR' },
      }),
      securities: ['IBM US Equity'],
      service: '//blp/refdata',
    });

    expect(captured[0]).toMatchObject({
      overrides: [{ key: 'EQY_FUND_CRNCY', value: 'USD' }],
      securityOverrides: [
        {
          overrides: [{ key: 'EQY_FUND_CRNCY', value: 'EUR' }],
          security: 'IBM US Equity',
        },
      ],
    });
  });

  it('forwards returnEids on every typed request helper', async () => {
    const engine = captureRequests();

    await engine.bdp(['IBM US Equity'], ['PX_LAST'], { returnEids: true });
    await engine.bds(['IBM US Equity'], ['DVD_HIST_ALL'], { returnEids: true });
    await engine.bdh(['IBM US Equity'], ['PX_LAST'], {
      end: '2024-01-31',
      returnEids: true,
      start: '2024-01-01',
    });
    await engine.bdib('IBM US Equity', { returnEids: true });
    await engine.bdtick('IBM US Equity', { returnEids: true });

    expect(engine.calls.map((call) => call.returnEids)).toStrictEqual([
      true,
      true,
      true,
      true,
      true,
    ]);
  });

  it('wraps native errors from entitlement methods', async () => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    Reflect.set(engine, 'inner', {
      checkEntitlements: async () => {
        throw new Error('[XBBG:VALIDATION] invalid eids');
      },
      identityIsAuthorized: async () => {
        throw new Error('[XBBG:SESSION] not authorized');
      },
      seatType: async () => {
        throw new Error('[XBBG:TIMEOUT] authorization timed out');
      },
    });

    await expect(engine.seatType()).rejects.toBeInstanceOf(api.BlpTimeoutError);
    await expect(engine.checkEntitlements('//blp/refdata', [101])).rejects.toBeInstanceOf(
      api.BlpValidationError,
    );
    await expect(engine.identityIsAuthorized('//blp/refdata')).rejects.toBeInstanceOf(
      api.BlpSessionError,
    );
  });

  it('forwards intraday timezone controls and typed tick include options', async () => {
    const engine = captureRequests();

    await engine.bdib('IBM US Equity', {
      end: '2024-01-02T10:00:00',
      outputTz: 'exchange',
      requestTz: 'NY',
      start: '2024-01-02T09:30:00',
    });
    await engine.bdtick('IBM US Equity', {
      end: '2024-01-02T10:00:00',
      includeConditionCodes: true,
      includeExchangeCodes: true,
      kwargs: { customOption: 'customValue' },
      outputTz: 'NY',
      requestTz: 'NY',
      start: '2024-01-02T09:30:00',
    });

    expect(engine.calls[0]?.requestTz).toBe('NY');
    expect(engine.calls[0]?.outputTz).toBe('exchange');
    expect(engine.calls[1]?.requestTz).toBe('NY');
    expect(engine.calls[1]?.outputTz).toBe('NY');
    expect(engine.calls[1]?.kwargs).toStrictEqual(
      expect.arrayContaining([
        { key: 'customOption', value: 'customValue' },
        { key: 'includeConditionCodes', value: 'true' },
        { key: 'includeExchangeCodes', value: 'true' },
      ]),
    );

    await engine.bdtick('IBM US Equity', {
      end: '2024-01-02T10:00:00',
      includeConditionCodes: false,
      kwargs: { includeConditionCodes: true },
      start: '2024-01-02T09:30:00',
    });
    expect(engine.calls[2]?.kwargs).toContainEqual({
      key: 'includeConditionCodes',
      value: 'false',
    });
  });

  it('rejects unknown backend strings instead of silently returning Arrow', async () => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    (engine as unknown as { inner: unknown }).inner = {
      request: async () => Promise.resolve(Buffer.alloc(0)),
    };

    const invalidRequest = {
      backend: 'bogus',
      operation: 'ReferenceDataRequest',
      service: '//blp/refdata',
    };
    const request = engine.request.bind(engine) as (params: unknown) => Promise<unknown>;
    await expect(request(invalidRequest)).rejects.toThrow('Unsupported @xbbg/core backend');
  });
});

describe('recipe wrapper forwarding', () => {
  it('forwards new workflow options to native recipe methods', async () => {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    const calls: Record<string, unknown[]> = {};
    (engine as unknown as { inner: unknown }).inner = {
      recipeFuturesCurve: async (...args: unknown[]) => {
        calls.futuresCurve = args;
        throw new Error('stop');
      },
      recipeVolSurface: async (...args: unknown[]) => {
        calls.volSurface = args;
        throw new Error('stop');
      },
      recipeDividendYield: async (...args: unknown[]) => {
        calls.dividendYield = args;
        throw new Error('stop');
      },
      recipeIndexMembers: async (...args: unknown[]) => {
        calls.indexMembers = args;
        throw new Error('stop');
      },
      recipeResolveIsins: async (...args: unknown[]) => {
        calls.resolveIsins = args;
        throw new Error('stop');
      },
      recipeIssuerIsins: async (...args: unknown[]) => {
        calls.issuerIsins = args;
        throw new Error('stop');
      },
      recipeEtfNavRelationships: async (...args: unknown[]) => {
        calls.etfNavRelationships = args;
        throw new Error('stop');
      },
      recipeEtfNavSnapshot: async (...args: unknown[]) => {
        calls.etfNavSnapshot = args;
        throw new Error('stop');
      },
      recipeEtfNavHistory: async (...args: unknown[]) => {
        calls.etfNavHistory = args;
        throw new Error('stop');
      },
    };

    await expect(
      engine.futuresCurve('ES1 Index', {
        asof: '2024-01-02',
        chainField: 'FUT_CHAIN_LAST_TRADE_DATES',
        fields: ['PX_BID'],
        maxContracts: 4,
      }),
    ).rejects.toThrow('stop');
    await expect(
      engine.volSurface('SPX Index', '2024-01-02', '2024-01-03', {
        preset: ['MONEYNESS_30D'],
        fields: {
          CUSTOM_VOL: { metric: 'implied_volatility', tenor: '1M', pointType: 'custom', point: 1 },
        },
        includeDerived: true,
        riskFreeRate: 0.05,
      }),
    ).rejects.toThrow('stop');
    await expect(
      engine.dividendYield('AAPL US Equity', '2024-01-01', '2024-12-31', {
        dividendTypes: ['Regular Cash'],
        windowDays: 365,
      }),
    ).rejects.toThrow('stop');
    await expect(
      engine.indexMembers('SPX Index', { field: 'INDX_MWEIGHT', asof: '2024-01-02' }),
    ).rejects.toThrow('stop');
    await expect(engine.resolveIsins(['US0378331005', 'BAD'])).rejects.toThrow('stop');
    await expect(engine.issuerIsins('US037833FB15')).rejects.toThrow('stop');
    await expect(engine.etfNavRelationships([' QQQ US Equity ', 'AT1 LN Equity'])).rejects.toThrow(
      'stop',
    );
    await expect(engine.etfNavSnapshot('QQQ US Equity')).rejects.toThrow('stop');
    await expect(
      engine.etfNavHistory([' QQQ US Equity ', 'AT1 LN Equity'], '2026-06-01', '2026-07-01'),
    ).rejects.toThrow('stop');

    expect(calls.futuresCurve).toStrictEqual([
      'ES1 Index',
      '20240102',
      'FUT_CHAIN_LAST_TRADE_DATES',
      ['PX_BID'],
      4,
    ]);
    expect(calls.volSurface).toStrictEqual([
      ['SPX Index'],
      '20240102',
      '20240103',
      ['MONEYNESS_30D'],
      ['CUSTOM_VOL|implied_volatility|1M|custom|1'],
      true,
      true,
      0.05,
      undefined,
    ]);
    expect(calls.dividendYield).toStrictEqual([
      ['AAPL US Equity'],
      '20240101',
      '20241231',
      ['Regular Cash'],
      365,
    ]);
    expect(calls.indexMembers).toStrictEqual(['SPX Index', 'INDX_MWEIGHT', '20240102']);
    expect(calls.resolveIsins).toStrictEqual([['US0378331005', 'BAD']]);
    expect(calls.issuerIsins).toStrictEqual([['US037833FB15']]);
    expect(calls.etfNavRelationships).toStrictEqual([['QQQ US Equity', 'AT1 LN Equity']]);
    expect(calls.etfNavSnapshot).toStrictEqual([['QQQ US Equity']]);
    expect(calls.etfNavHistory).toStrictEqual([
      ['QQQ US Equity', 'AT1 LN Equity'],
      '20260601',
      '20260701',
    ]);
  });
});

describe('subscribeEtfInav preflight', () => {
  const QQQ = 'QQQ US Equity';
  const AT1 = 'AT1 LN Equity';
  const QQQ_NAV = 'QQQNV Index';
  const QQQ_INAV = 'QXV Index';
  const AT1_INAV = 'AT1IN Index';

  function int32Column(name: string, values: readonly number[]): NativeArrowColumn {
    return {
      name,
      type: 'int32',
      nullable: false,
      length: values.length,
      nullCount: 0,
      data: typedBuffer(new Int32Array(values)),
    };
  }

  // Arrow validity bits; each row's bit is set once, so addition equals OR.
  const VALIDITY_BITS = [1, 2, 4, 8, 16, 32, 64, 128] as const;

  function utf8Column(name: string, values: readonly (string | null)[]): NativeArrowColumn {
    const offsets = new Int32Array(values.length + 1);
    const bitmap = new Uint8Array(Math.max(1, Math.ceil(values.length / 8)));
    let text = '';
    let nullCount = 0;
    for (const [index, value] of values.entries()) {
      if (value === null) {
        nullCount += 1;
      } else {
        text += value;
        const byteIndex = Math.floor(index / 8);
        bitmap[byteIndex] = (bitmap[byteIndex] ?? 0) + (VALIDITY_BITS[index % 8] ?? 0);
      }
      offsets[index + 1] = Buffer.byteLength(text);
    }
    return {
      name,
      type: 'utf8',
      nullable: true,
      length: values.length,
      nullCount,
      ...(nullCount > 0 ? { nullBitmap: Buffer.from(bitmap) } : {}),
      offsets: typedBuffer(offsets),
      data: Buffer.from(text),
    };
  }

  interface RelationshipFixtureRow {
    inputOrder: number;
    etfTicker: string;
    navTicker?: string | null;
    inavTicker: string | null;
    inavValidationError?: string | null;
  }

  function relationshipBatch(rows: readonly RelationshipFixtureRow[]): NativeArrowZeroCopyBatch {
    const navTickers = rows.map((row) => row.navTicker ?? null);
    const inavTickers = rows.map((row) => row.inavTicker);
    return {
      columns: [
        int32Column(
          'input_order',
          rows.map((row) => row.inputOrder),
        ),
        utf8Column(
          'etf_ticker',
          rows.map((row) => row.etfTicker),
        ),
        utf8Column('nav_ticker', navTickers),
        utf8Column(
          'nav_market_sector_des',
          navTickers.map((ticker) => (ticker === null ? null : 'Index')),
        ),
        utf8Column(
          'nav_name',
          navTickers.map((ticker) => (ticker === null ? null : `${ticker} name`)),
        ),
        utf8Column(
          'nav_validation_error',
          rows.map(() => null),
        ),
        utf8Column('inav_ticker', inavTickers),
        utf8Column(
          'inav_market_sector_des',
          inavTickers.map((ticker) => (ticker === null ? null : 'Index')),
        ),
        utf8Column(
          'inav_name',
          inavTickers.map((ticker) => (ticker === null ? null : `${ticker} name`)),
        ),
        utf8Column(
          'inav_validation_error',
          rows.map((row) => row.inavValidationError ?? null),
        ),
      ],
      kind: 'zeroCopy',
      metadata: {},
      numRows: rows.length,
    };
  }

  function qqqAt1Batch(): NativeArrowZeroCopyBatch {
    return relationshipBatch([
      { inputOrder: 0, etfTicker: QQQ, navTicker: QQQ_NAV, inavTicker: QQQ_INAV },
      { inputOrder: 1, etfTicker: AT1, inavTicker: AT1_INAV },
    ]);
  }

  interface SubscribeCall {
    tickers: readonly string[];
    fields: readonly string[];
    options: Record<string, unknown>;
  }

  function preflightHarness(batchFactory: () => NativeArrowZeroCopyBatch): {
    engine: api.Engine;
    recipeCalls: unknown[][];
    subscribeCalls: SubscribeCall[];
    subscription: object;
  } {
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    const recipeCalls: unknown[][] = [];
    const subscribeCalls: SubscribeCall[] = [];
    const subscription = {};
    Reflect.set(engine, 'inner', {
      recipeEtfNavRelationships: async (...args: unknown[]) => {
        recipeCalls.push(args);
        return batchFactory();
      },
    });
    Reflect.set(
      engine,
      'subscribe',
      async (
        tickers: readonly string[],
        fields: readonly string[],
        options: Record<string, unknown>,
      ) => {
        subscribeCalls.push({ tickers, fields, options });
        return subscription;
      },
    );
    return { engine, recipeCalls, subscribeCalls, subscription };
  }

  it('delegates validated iNAV topics with default LAST_PRICE', async () => {
    const { engine, recipeCalls, subscribeCalls, subscription } = preflightHarness(qqqAt1Batch);

    const result = await engine.subscribeEtfInav([` ${QQQ} `, AT1]);

    expect(result).toBe(subscription);
    expect(recipeCalls).toStrictEqual([[[QQQ, AT1]]]);
    expect(subscribeCalls).toStrictEqual([
      { tickers: [QQQ_INAV, AT1_INAV], fields: ['LAST_PRICE'], options: {} },
    ]);
  });

  it('honors a custom fields override and forwards remaining stream options', async () => {
    const { engine, subscribeCalls } = preflightHarness(() =>
      relationshipBatch([
        { inputOrder: 0, etfTicker: QQQ, navTicker: QQQ_NAV, inavTicker: QQQ_INAV },
      ]),
    );

    await engine.subscribeEtfInav(QQQ, {
      fields: ['BID', 'ASK'],
      conflate: true,
      flushThreshold: 5,
      overflowPolicy: 'block',
      streamCapacity: 128,
      allFields: true,
      options: ['interval=2'],
    });

    expect(subscribeCalls).toStrictEqual([
      {
        tickers: [QQQ_INAV],
        fields: ['BID', 'ASK'],
        options: {
          conflate: true,
          flushThreshold: 5,
          overflowPolicy: 'block',
          streamCapacity: 128,
          allFields: true,
          options: ['interval=2'],
        },
      },
    ]);
  });

  it('rejects empty input before native resolution', async () => {
    const { engine, recipeCalls, subscribeCalls } = preflightHarness(qqqAt1Batch);

    await expectValidationError(engine.subscribeEtfInav([]), 'etfs must not be empty');

    expect(recipeCalls).toStrictEqual([]);
    expect(subscribeCalls).toStrictEqual([]);
  });

  it('rejects duplicate trimmed inputs before native resolution', async () => {
    const { engine, recipeCalls, subscribeCalls } = preflightHarness(qqqAt1Batch);

    await expectValidationError(
      engine.subscribeEtfInav([QQQ, AT1, ` ${QQQ} `, AT1]),
      `Duplicate ETF inputs are not allowed: ${QQQ}, ${AT1}`,
    );

    expect(recipeCalls).toStrictEqual([]);
    expect(subscribeCalls).toStrictEqual([]);
  });

  it('rejects an invalid iNAV relationship before opening a stream', async () => {
    const { engine, recipeCalls, subscribeCalls } = preflightHarness(() =>
      relationshipBatch([
        { inputOrder: 0, etfTicker: QQQ, navTicker: QQQ_NAV, inavTicker: QQQ_INAV },
        {
          inputOrder: 1,
          etfTicker: AT1,
          inavTicker: AT1_INAV,
          inavValidationError: 'NAME is missing',
        },
      ]),
    );

    await expectValidationError(
      engine.subscribeEtfInav([QQQ, AT1]),
      `Invalid iNAV relationship for ETF ${AT1}: NAME is missing`,
    );

    expect(recipeCalls).toHaveLength(1);
    expect(subscribeCalls).toStrictEqual([]);
  });

  it('collects every missing iNAV relationship in request order', async () => {
    const { engine, subscribeCalls } = preflightHarness(() =>
      relationshipBatch([
        { inputOrder: 0, etfTicker: QQQ, navTicker: QQQ_NAV, inavTicker: null },
        { inputOrder: 1, etfTicker: AT1, inavTicker: '   ' },
        { inputOrder: 2, etfTicker: 'SPY US Equity', inavTicker: 'SPYIV Index' },
      ]),
    );

    await expectValidationError(
      engine.subscribeEtfInav([QQQ, AT1, 'SPY US Equity']),
      `Missing valid iNAV relationship for ETFs: ${QQQ}, ${AT1}`,
    );

    expect(subscribeCalls).toStrictEqual([]);
  });

  it('rejects an ambiguous iNAV reverse mapping', async () => {
    const { engine, subscribeCalls } = preflightHarness(() =>
      relationshipBatch([
        { inputOrder: 0, etfTicker: QQQ, navTicker: QQQ_NAV, inavTicker: QQQ_INAV },
        { inputOrder: 1, etfTicker: 'QQQM US Equity', inavTicker: QQQ_INAV },
      ]),
    );

    await expectValidationError(
      engine.subscribeEtfInav([QQQ, 'QQQM US Equity']),
      `Ambiguous iNAV reverse mapping for ${QQQ_INAV}: ${QQQ}, QQQM US Equity`,
    );

    expect(subscribeCalls).toStrictEqual([]);
  });

  it('rejects malformed relationship results atomically', async () => {
    const malformedBatches: (() => NativeArrowZeroCopyBatch)[] = [
      // Too few rows.
      () => relationshipBatch([{ inputOrder: 0, etfTicker: QQQ, inavTicker: QQQ_INAV }]),
      // Duplicate input_order.
      () =>
        relationshipBatch([
          { inputOrder: 0, etfTicker: QQQ, inavTicker: QQQ_INAV },
          { inputOrder: 0, etfTicker: AT1, inavTicker: AT1_INAV },
        ]),
      // Mismatched etf_ticker identity.
      () =>
        relationshipBatch([
          { inputOrder: 0, etfTicker: QQQ, inavTicker: QQQ_INAV },
          { inputOrder: 1, etfTicker: 'WRONG US Equity', inavTicker: AT1_INAV },
        ]),
      // Missing required vector.
      () => ({
        columns: [
          int32Column('input_order', [0, 1]),
          utf8Column('etf_ticker', [QQQ, AT1]),
          utf8Column('inav_ticker', [QQQ_INAV, AT1_INAV]),
        ],
        kind: 'zeroCopy',
        metadata: {},
        numRows: 2,
      }),
    ];

    for (const batchFactory of malformedBatches) {
      const { engine, subscribeCalls } = preflightHarness(batchFactory);
      await expectValidationError(
        engine.subscribeEtfInav([QQQ, AT1]),
        'ETF NAV relationship result is not one-to-one with requested ETFs',
      );
      expect(subscribeCalls).toStrictEqual([]);
    }
  });
});

describe('cdx recipe options', () => {
  it('forwards canonical defaults and versionless presentation', async () => {
    const cdxCalls: unknown[][] = [];
    const activeCalls: unknown[][] = [];
    const inner = {
      recipeCdxTicker: async (...args: unknown[]) => {
        cdxCalls.push(args);
        return metadataBatch({});
      },
      recipeActiveCdx: async (...args: unknown[]) => {
        activeCalls.push(args);
        return metadataBatch({});
      },
    };
    const engine = Object.create(api.Engine.prototype) as api.Engine;
    Object.defineProperty(engine, 'inner', { value: inner });
    const generic = 'CDX HY CDSI GEN 5Y Corp';

    await engine.cdxTicker(generic, '20260717', { backend: api.Backend.JSON });
    await engine.cdxTicker(generic, '20260717', {
      backend: api.Backend.JSON,
      versionless: true,
    });
    await engine.activeCdx(generic, '20260717', {
      backend: api.Backend.JSON,
      lookbackDays: 7,
    });
    await engine.activeCdx(generic, '20260717', {
      backend: api.Backend.JSON,
      versionless: true,
    });

    expect(cdxCalls).toStrictEqual([
      [generic, '20260717', false],
      [generic, '20260717', true],
    ]);
    expect(activeCalls).toStrictEqual([
      [generic, '20260717', 7, false],
      [generic, '20260717', undefined, true],
    ]);
  });
});

describe('engine instantiation', () => {
  it('new Engine(host, port) exposes expected methods', () => {
    try {
      const engine: any = new api.Engine(SESSION_HOST, SESSION_PORT);
      expect(engine).toBeInstanceOf(api.Engine);
      const asyncMethods = [
        'bdp',
        'bds',
        'bdh',
        'bdib',
        'bdtick',
        'bql',
        'beqs',
        'bsrch',
        'bta',
        'bflds',
        'blkp',
        'bport',
        'bcurves',
        'bgovts',
        'stream',
        'vwap',
        'mktbar',
        'depth',
        'chains',
        'bops',
        'bschema',
        'fieldInfo',
        'fieldSearch',
        'bqr',
        'yas',
        'preferreds',
        'corporateBonds',
        'futTicker',
        'activeFutures',
        'futuresCurve',
        'cdxTicker',
        'activeCdx',
        'dividend',
        'dividendYield',
        'turnover',
        'etfHoldings',
        'volSurface',
        'indexMembers',
        'resolveIsins',
        'issuerIsins',
        'etfNavRelationships',
        'etfNavSnapshot',
        'etfNavHistory',
        'subscribeEtfInav',
        'currencyConversion',
        'subscribe',
        'subscribeWithOptions',
        'request',
        'requestRaw',
        'resolveFieldTypes',
      ];
      for (const method of asyncMethods) {
        expect(typeof engine[method]).toBe('function');
      }
      const syncMethods = [
        'getFieldInfo',
        'clearFieldCache',
        'saveFieldCache',
        'validateFields',
        'isFieldValidationEnabled',
        'getSchema',
        'getOperation',
        'listOperations',
        'getCachedSchema',
        'invalidateSchema',
        'clearSchemaCache',
        'listCachedSchemas',
        'getEnumValues',
        'listValidElements',
        'signalShutdown',
        'isAvailable',
      ];
      for (const method of syncMethods) {
        expect(typeof engine[method]).toBe('function');
      }
    } catch (error) {
      if (nativeUnavailable(error)) {
        console.warn('Engine instantiation skipped: native module or session unavailable');
        return;
      }
      throw error;
    }
  });

  it('engine.withConfig returns an Engine', () => {
    try {
      const engine = api.Engine.withConfig({ host: SESSION_HOST, port: SESSION_PORT });
      expect(engine).toBeInstanceOf(api.Engine);
    } catch (error) {
      if (nativeUnavailable(error)) {
        console.warn('Engine.withConfig skipped: native module or session unavailable');
        return;
      }
      throw error;
    }
  });

  it('subscription prototype exposes async iterator + methods', () => {
    const subProto = api.Subscription.prototype as any;
    expect(typeof subProto.next).toBe('function');
    expect(typeof subProto.add).toBe('function');
    expect(typeof subProto.remove).toBe('function');
    expect(typeof subProto.unsubscribe).toBe('function');
    expect(typeof subProto[Symbol.asyncIterator]).toBe('function');
  });
});
