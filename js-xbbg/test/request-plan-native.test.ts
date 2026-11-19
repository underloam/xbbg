import { existsSync } from 'node:fs';
import path from 'node:path';

import { TA_DEFAULTS } from '../src/_defs_gen';
import type * as Core from '../src/index';
import type { NativeEngine } from '../src/napi';
import { int32ArrowBatch } from './subscription-fixtures';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');

// Typed requests are planned by the host (xbbg-host `request::plan`) inside the real addon.
// No engine/session is constructed: the engine's `request` is a spy that receives the plan.
describe.skipIf(!existsSync(nativePath))('typed requests planned by the host', () => {
  let api: typeof Core;
  let request: ReturnType<typeof vi.fn<NativeEngine['request']>>;
  let engine: Core.Engine;

  beforeAll(async () => {
    // Not a static import: the package loads the addon on import and throws without one.
    api = await import('../src/index.js');
  });

  beforeEach(() => {
    request = vi.fn<NativeEngine['request']>().mockResolvedValue(int32ArrowBatch(1));
    engine = Object.assign(Object.create(api.Engine.prototype) as Core.Engine, {
      inner: { request },
    });
  });

  /** The plan the wrapper handed to the engine. */
  function planned(): unknown {
    expect(request).toHaveBeenCalledTimes(1);
    return request.mock.calls[0]?.[0];
  }

  test('plans bdp with merged overrides, per-security overrides and defaults', async () => {
    await engine.bdp(['SYN_A Equity'], ['PX_LAST'], {
      backend: 'json',
      kwargs: { Period: 'D', Count: 2 },
      overrides: api.ovr({ EQY_FUND_CRNCY: 'EUR' }).forSecurity('SYN_A Equity', {
        EQY_FUND_CRNCY: 'USD',
      }),
      returnEids: true,
      onDelayed: 'ignore',
    });
    expect(planned()).toStrictEqual({
      service: '//blp/refdata',
      operation: 'ReferenceDataRequest',
      extractor: 'refdata',
      securities: ['SYN_A Equity'],
      fields: ['PX_LAST'],
      overrides: [{ key: 'EQY_FUND_CRNCY', value: 'EUR' }],
      securityOverrides: [
        { security: 'SYN_A Equity', overrides: [{ key: 'EQY_FUND_CRNCY', value: 'USD' }] },
      ],
      kwargs: [
        { key: 'Period', value: 'D' },
        { key: 'Count', value: '2' },
      ],
      includeSecurityErrors: false,
      returnEids: true,
      onDelayed: 'ignore',
    });
  });

  test('plans bds and bdh with host-formatted dates', async () => {
    await engine.bds(['SYN_A Equity'], ['DVD_HIST'], { backend: 'json', validateFields: false });
    expect(planned()).toMatchObject({
      operation: 'ReferenceDataRequest',
      extractor: 'bulk',
      validateFields: false,
    });
    request.mockClear();

    await engine.bdh(['SYN_A Equity'], ['PX_LAST'], {
      backend: 'json',
      start: '2024-01-02',
      end: new Date(Date.UTC(2024, 0, 31)),
    });
    expect(planned()).toMatchObject({
      operation: 'HistoricalDataRequest',
      extractor: 'histdata',
      startDate: '20240102',
      endDate: '20240131',
    });
  });

  test('plans bdib and bdtick with their defaults and typed flags', async () => {
    await engine.bdib('SYN_A Equity', { backend: 'json' });
    expect(planned()).toStrictEqual({
      service: '//blp/refdata',
      operation: 'IntradayBarRequest',
      extractor: 'intraday_bar',
      security: 'SYN_A Equity',
      eventType: 'TRADE',
      interval: 1,
    });
    request.mockClear();

    await engine.bdtick('SYN_A Equity', {
      backend: 'json',
      includeBrokerCodes: true,
      includeRpsCodes: false,
      kwargs: { includeRpsCodes: 'true', other: 'x' },
    });
    expect(planned()).toMatchObject({
      operation: 'IntradayTickRequest',
      extractor: 'intraday_tick',
      eventTypes: ['TRADE'],
      kwargs: [
        { key: 'includeRpsCodes', value: 'false' },
        { key: 'other', value: 'x' },
        { key: 'includeBrokerCodes', value: 'true' },
      ],
    });
  });

  test('plans the search and listing requests', async () => {
    await engine.bql('get(px_last) for(["SYN Equity"])', { backend: 'json', appName: 'syn' });
    expect(planned()).toMatchObject({
      service: '//blp/bqlsvc',
      operation: 'sendQuery',
      elements: [{ key: 'expression', value: 'get(px_last) for(["SYN Equity"])' }],
      kwargs: [{ key: 'clientContext.appName', value: 'syn' }],
    });
    request.mockClear();

    await engine.beqs('SYN_SCREEN', { backend: 'json', asof: '2024-03-04' });
    expect(planned()).toMatchObject({
      operation: 'BeqsRequest',
      elements: [
        { key: 'screenName', value: 'SYN_SCREEN' },
        { key: 'screenType', value: 'PRIVATE' },
        { key: 'Group', value: 'General' },
        { key: 'asOfDate', value: '20240304' },
      ],
    });
    request.mockClear();

    await engine.bsrch('domain:SYN', { backend: 'json', overrides: { LIMIT: 5 } });
    expect(planned()).toMatchObject({
      service: '//blp/exrsvc',
      elements: [
        { key: 'Domain', value: 'domain:SYN' },
        { key: 'LIMIT', value: '5' },
      ],
    });
    request.mockClear();

    await engine.bflds({ backend: 'json', fields: 'PX_LAST' });
    expect(planned()).toMatchObject({ operation: 'FieldInfoRequest', fieldIds: ['PX_LAST'] });
    request.mockClear();

    await engine.bflds({ backend: 'json', searchSpec: 'last price' });
    expect(planned()).toMatchObject({ operation: 'FieldSearchRequest', searchSpec: 'last price' });
    request.mockClear();

    await engine.blkp('SYN', { backend: 'json' });
    expect(planned()).toMatchObject({
      operation: 'instrumentListRequest',
      elements: [{ key: 'query', value: 'SYN' }],
    });
    request.mockClear();

    await engine.bport('SYN_PORT', 'PORTFOLIO_MWEIGHT', { backend: 'json' });
    expect(planned()).toMatchObject({
      operation: 'PortfolioDataRequest',
      security: 'SYN_PORT',
      fields: ['PORTFOLIO_MWEIGHT'],
    });
  });

  test('plans bcurves and bgovts and keeps ignoring overrides', async () => {
    await engine.bcurves('SYN', { backend: 'json', overrides: { IGNORED: 'x' } });
    expect(planned()).toMatchObject({
      operation: 'curveListRequest',
      elements: [{ key: 'query', value: 'SYN' }],
    });
    expect(planned()).not.toHaveProperty('overrides');
    request.mockClear();

    await engine.bgovts('SYN', { backend: 'json', overrides: { IGNORED: 'x' } });
    expect(planned()).toMatchObject({ operation: 'govtListRequest' });
    expect(planned()).not.toHaveProperty('overrides');
  });

  test.each(['beqs', 'bsrch'] as const)('rejects per-security overrides for %s', async (method) => {
    const overrides = api.ovr().forSecurity('SYN_A Equity', { X: 'y' });
    await expect(engine[method]('SYN', { backend: 'json', overrides })).rejects.toMatchObject({
      name: 'BlpValidationError',
      message: 'Per-security overrides are only supported by bdp(), bdh(), and bds()',
    });
    expect(request).not.toHaveBeenCalled();
  });

  test('merges shared study defaults with caller parameters', async () => {
    await engine.bta('SYNTHETIC Equity', 'rsi', { backend: 'json', studyParams: { period: 7 } });
    expect(planned()).toStrictEqual({
      service: '//blp/tasvc',
      operation: 'studyRequest',
      extractor: 'generic',
      elements: [
        { key: 'priceSource.securityName', value: 'SYNTHETIC Equity' },
        { key: 'priceSource.dataRange.historical.periodicitySelection', value: 'DAILY' },
        ...Object.entries({ ...TA_DEFAULTS.rsiStudyAttributes, period: 7 }).map(([key, value]) => ({
          key: `studyAttributes.rsiStudyAttributes.${key}`,
          value: String(value),
        })),
      ],
    });
  });

  test.each([
    ['SMA', 'smavgStudyAttributes'],
    ['Fear Greed', 'fgStudyAttributes'],
    ['FEAR-GREED', 'fgStudyAttributes'],
    ['custom', 'customStudyAttributes'],
    ['customStudyAttributes', 'customstudyattributes'],
  ])('preserves normalization and custom study fallback for %s', async (study, attr) => {
    await engine.bta('SYNTHETIC Equity', study, { backend: 'json', studyParams: { period: 7 } });
    expect(planned()).toMatchObject({
      elements: expect.arrayContaining([{ key: `studyAttributes.${attr}.period`, value: '7' }]),
    });
  });

  test('formats bta dates from kwargs and options and lets undefined params unset defaults', async () => {
    await engine.bta(
      'SYNTHETIC Equity',
      { study: 'rsi', length: 9, priceSourceClose: undefined },
      {
        backend: 'json',
        startDate: '2024-01-02',
        kwargs: { endDate: '2024-02-03', extra: 'x' },
      },
    );
    const { elements } = planned() as { elements: { key: string; value: string }[] };
    expect(elements).toStrictEqual([
      { key: 'priceSource.securityName', value: 'SYNTHETIC Equity' },
      { key: 'priceSource.dataRange.historical.startDate', value: '20240102' },
      { key: 'priceSource.dataRange.historical.endDate', value: '20240203' },
      { key: 'priceSource.dataRange.historical.periodicitySelection', value: 'DAILY' },
      { key: 'studyAttributes.rsiStudyAttributes.period', value: '9' },
      { key: 'extra', value: 'x' },
    ]);
  });

  test('reports a bad planner argument as a validation error', async () => {
    await expect(
      engine.bdp(['SYN_A Equity'], ['PX_LAST'], { backend: 'json', kwargs: { bad: [1] as never } }),
    ).rejects.toMatchObject({ name: 'BlpValidationError' });
    expect(request).not.toHaveBeenCalled();
  });
});
