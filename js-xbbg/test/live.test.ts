/*
Live Bloomberg tests for js-xbbg.
Run with:
  npm run test:live
*/

import type { Table } from 'apache-arrow';

import { DataType, DateUnit, TimeUnit } from 'apache-arrow';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { Backend, connect } from '../src/index';
import type { Engine, FeedInfo, Subscription } from '../src/index';

const CONFIG = Object.freeze({
  bond_ticker: 'GT10 Govt',
  equity_multi: ['AAPL US Equity', 'MSFT US Equity'],
  equity_single: 'IBM US Equity',
  etf_ticker: 'SPY US Equity',
  futures_generic: 'ES1 Index',
  index_ticker: 'INDU Index',
  name_field: 'NAME',
  price_field: 'PX_LAST',
  streaming_ticker: process.env.XBBG_STREAMING_TICKER ?? 'XBTUSD Curncy',
  volume_field: 'VOLUME',
  portfolio_security: process.env.XBBG_LIVE_PORTFOLIO_SECURITY,
} as const);

const SESSION_CONFIG = Object.freeze({
  host: process.env.XBBG_HOST ?? 'localhost',
  port: Number(process.env.XBBG_PORT ?? 8194),
});

let enginePromise: Promise<Engine> | undefined;
let engine: Engine | undefined;
let sessionUnavailableReason: string | null = null;

function formatLocalDate(day: Date): string {
  const year = day.getFullYear();
  const month = String(day.getMonth() + 1).padStart(2, '0');
  const date = String(day.getDate()).padStart(2, '0');
  return `${year}-${month}-${date}`;
}

function getRecentTradingDay(): string {
  const now = new Date();
  for (let daysBack = 1; daysBack <= 5; daysBack += 1) {
    const candidate = new Date(now);
    candidate.setDate(now.getDate() - daysBack);
    const dow = candidate.getDay();
    if (dow !== 0 && dow !== 6) {
      return formatLocalDate(candidate);
    }
  }
  const fallback = new Date(now);
  fallback.setDate(now.getDate() - 1);
  return formatLocalDate(fallback);
}

function getDateRange(days: number): { start: string; end: string } {
  const end = new Date();
  const start = new Date(end);
  start.setDate(end.getDate() - days);
  return {
    end: end.toISOString().slice(0, 10).replace(/-/gu, ''),
    start: start.toISOString().slice(0, 10).replace(/-/gu, ''),
  };
}

async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Timed out waiting for ${label}`));
    }, ms);
    void (async (): Promise<void> => {
      try {
        const val = await promise;
        clearTimeout(timer);
        resolve(val);
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    })();
  });
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function columnsOf(table: any): string[] {
  return table.schema.fields.map((f: any) => f.name);
}

function tableSummary(table: any): string {
  return `rows=${table.numRows}, cols=${table.numCols}, fields=[${columnsOf(table).join(', ')}]`;
}

function cellValue(table: Table, column: string, row: number): unknown {
  return table.getChild(column)?.get(row);
}

function stringCell(table: Table, column: string, row: number): string | null {
  const value: unknown = cellValue(table, column, row);
  return typeof value === 'string' ? value : null;
}

function numberCell(table: Table, column: string, row: number): number | null {
  const value: unknown = cellValue(table, column, row);
  return typeof value === 'number' ? value : null;
}

async function nextWithTimeout(sub: any, ms: number): Promise<any> {
  return Promise.race([sub.next(), sleep(ms).then(() => null)]);
}

async function collectStreamBatches(
  sub: any,
  minBatches: number,
  maxWaitMs: number,
): Promise<any[]> {
  const batches: any[] = [];
  const started = Date.now();
  while (batches.length < minBatches) {
    const remainingMs = maxWaitMs - (Date.now() - started);
    if (remainingMs <= 0) {
      break;
    }
    const next = await nextWithTimeout(sub, remainingMs);
    if (!next || next.done) {
      break;
    }
    batches.push(next.value);
  }
  return batches;
}

function columnSet(table: any): Set<string> {
  return new Set(columnsOf(table));
}

function assertArrowTable(table: any, requiredColumns: string[], minRows = 1): void {
  assert.ok(table, 'Expected table');
  assert.ok(Number.isInteger(table.numRows), 'Expected table.numRows');
  assert.ok(Number.isInteger(table.numCols), 'Expected table.numCols');
  assert.ok(table.numRows >= minRows, `Expected at least ${minRows} rows, got ${table.numRows}`);
  assert.ok(
    table.numCols >= requiredColumns.length,
    `Expected at least ${requiredColumns.length} columns`,
  );
  const fields = columnsOf(table);
  for (const col of requiredColumns) {
    assert.ok(fields.includes(col), `Missing expected column: ${col}`);
  }
}

function toNumber(value: any): number {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'bigint') {
    return Number(value);
  }
  if (typeof value === 'string') {
    const n = Number(value);
    return Number.isFinite(n) ? n : Number.NaN;
  }
  return Number.NaN;
}

function toMillis(value: any): number {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'bigint') {
    if (value > 1e15) {
      return Number(value / 1000n);
    }
    return Number(value);
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function maybeSkipEntitlement(t: any, err: any): boolean {
  const message = String(err?.message ?? err);
  const markers = [
    'not authorized',
    'not permissioned',
    'entitlement',
    'NO_AUTH',
    'BAD_SEC',
    'Unknown/Invalid security',
    'Unable to resolve',
    'service not available',
    'Operation not found',
    'failed to encode',
    'Channel closed',
    'Timed out',
    'Problem accessing the saved search',
  ];
  if (markers.some((marker) => message.includes(marker))) {
    t.skip(`Entitlement/unavailable: ${message}`);
    return true;
  }
  return false;
}

function isSessionUnavailable(err: any): boolean {
  const message = String(err?.message ?? err).toLowerCase();
  return (
    message.includes('session start failed') ||
    message.includes('failed to spawn worker') ||
    message.includes('connect event failed')
  );
}

async function runCase(t: any, name: string, fn: () => Promise<void>): Promise<void> {
  if (sessionUnavailableReason !== null) {
    t.skip(sessionUnavailableReason);
    return;
  }
  const started = performance.now();
  try {
    await fn();
    const elapsed = (performance.now() - started).toFixed(1);
    console.log(`[PASS] ${name} (${elapsed}ms)`);
  } catch (error: any) {
    const elapsed = (performance.now() - started).toFixed(1);
    if (error?.code === 'ERR_TEST_SKIP') {
      console.log(`[SKIP] ${name} (${elapsed}ms) ${error.message ?? ''}`);
      throw error;
    }
    console.log(`[FAIL] ${name} (${elapsed}ms): ${error?.message ?? error}`);
    throw error;
  }
}

describe('js-xbbg live Bloomberg API', () => {
  beforeAll(async () => {
    try {
      enginePromise ??= connect(SESSION_CONFIG);
      engine = await enginePromise;
    } catch (error: any) {
      if (isSessionUnavailable(error)) {
        sessionUnavailableReason = `Bloomberg session is not available in this environment: ${error.message ?? error}`;
        return;
      }
      throw error;
    }
    assert.ok(engine, 'Engine should be created via connect()');
  });

  afterAll(() => {
    if (engine) {
      engine.signalShutdown();
    }
  });

  describe('connectivity', () => {
    it('engine is available', async (t) =>
      runCase(t, 'engine is available', async () => {
        assert.equal(typeof engine!.isAvailable, 'function');
        const available = engine!.isAvailable();
        assert.equal(typeof available, 'boolean');
        console.log(`  engine.isAvailable(): ${available}`);
      }));

    it('bdp baseline request works', async (t) =>
      runCase(t, 'bdp baseline request works', async () => {
        const table: any = await engine!.bdp([CONFIG.equity_single], [CONFIG.price_field]);
        assertArrowTable(table, ['ticker', 'field', 'value']);
        console.log(`  BDP baseline -> ${tableSummary(table)}`);
      }));
  });

  describe('bDP reference data', () => {
    it('single ticker single field', async (t) =>
      runCase(t, 'bdp single ticker single field', async () => {
        const table: any = await engine!.bdp([CONFIG.equity_single], [CONFIG.price_field]);
        assertArrowTable(table, ['ticker', 'field', 'value'], 1);
        const ticker = table.getChild('ticker')?.get(0);
        const field = table.getChild('field')?.get(0);
        const value = table.getChild('value')?.get(0);
        assert.ok(String(ticker).includes('IBM US Equity'));
        assert.equal(String(field), CONFIG.price_field);
        assert.notEqual(value, null);
        console.log(`  ${ticker} ${field}=${value}`);
      }));

    it('single ticker multiple fields', async (t) =>
      runCase(t, 'bdp single ticker multiple fields', async () => {
        const fields = [CONFIG.price_field, CONFIG.name_field, CONFIG.volume_field];
        const table: any = await engine!.bdp([CONFIG.equity_single], fields);
        assertArrowTable(table, ['ticker', 'field', 'value'], fields.length);
        const gotFields = new Set<string>(
          table
            .getChild('field')
            .toArray()
            .map((v: any) => String(v)),
        );
        for (const f of fields) {
          assert.ok(gotFields.has(f), `Missing ${f}`);
        }
        console.log(`  ${CONFIG.equity_single} fields=${[...gotFields].join(', ')}`);
      }));

    it('multiple tickers single field', async (t) =>
      runCase(t, 'bdp multiple tickers single field', async () => {
        const table: any = await engine!.bdp(CONFIG.equity_multi, [CONFIG.price_field]);
        assertArrowTable(table, ['ticker', 'field', 'value'], CONFIG.equity_multi.length);
        const tickers = new Set<string>(
          table
            .getChild('ticker')
            .toArray()
            .map((v: any) => String(v)),
        );
        for (const expected of CONFIG.equity_multi) {
          assert.ok(
            [...tickers].some((tkr) => tkr.includes(expected)),
            `Missing ticker ${expected}`,
          );
        }
        console.log(`  tickers=${[...tickers].join(', ')}`);
      }));

    it('multiple tickers multiple fields', async (t) =>
      runCase(t, 'bdp multiple tickers multiple fields', async () => {
        const fields = [CONFIG.price_field, CONFIG.volume_field];
        const table: any = await engine!.bdp(CONFIG.equity_multi, fields);
        const expectedRows = CONFIG.equity_multi.length * fields.length;
        assertArrowTable(table, ['ticker', 'field', 'value'], expectedRows);
        assert.equal(table.numRows, expectedRows);
        console.log(`  expectedRows=${expectedRows}, actualRows=${table.numRows}`);
      }));

    it('with EUR override', async (t) =>
      runCase(t, 'bdp with override', async () => {
        const table: any = await engine!.bdp([CONFIG.equity_single], ['CRNCY_ADJ_PX_LAST'], {
          overrides: { EQY_FUND_CRNCY: 'EUR' },
        });
        assertArrowTable(table, ['ticker', 'field', 'value'], 1);
        const value = table.getChild('value')?.get(0);
        assert.ok(Number.isFinite(toNumber(value)), 'Override value should be numeric');
        console.log(`  EUR adjusted price=${value}`);
      }));

    it('price is positive number', async (t) =>
      runCase(t, 'bdp price positive', async () => {
        const table: any = await engine!.bdp([CONFIG.equity_single], [CONFIG.price_field]);
        assertArrowTable(table, ['ticker', 'field', 'value'], 1);
        const price = toNumber(table.getChild('value')?.get(0));
        assert.ok(Number.isFinite(price));
        assert.ok(price > 0, `Expected positive price, got ${price}`);
        console.log(`  price=${price}`);
      }));

    it('name is non-empty string', async (t) =>
      runCase(t, 'bdp name string', async () => {
        const table: any = await engine!.bdp([CONFIG.equity_single], [CONFIG.name_field]);
        assertArrowTable(table, ['ticker', 'field', 'value'], 1);
        const name = String(table.getChild('value')?.get(0) ?? '');
        assert.ok(name.trim().length > 0, 'Name should be non-empty');
        console.log(`  name=${name}`);
      }));
  });

  describe('bDH historical data', () => {
    it('single ticker date range', async (t) =>
      runCase(t, 'bdh single ticker range', async () => {
        const range = getDateRange(7);
        const table: any = await engine!.bdh([CONFIG.equity_single], [CONFIG.price_field], range);
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], 1);
        console.log(`  ${CONFIG.equity_single} ${range.start}->${range.end} rows=${table.numRows}`);
      }));

    it('multiple tickers single field', async (t) =>
      runCase(t, 'bdh multi ticker', async () => {
        const range = getDateRange(5);
        const table: any = await engine!.bdh(CONFIG.equity_multi, [CONFIG.price_field], range);
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], CONFIG.equity_multi.length);
        console.log(`  bdh multi ticker rows=${table.numRows}`);
      }));

    it('single ticker multiple fields', async (t) =>
      runCase(t, 'bdh multi field', async () => {
        const range = getDateRange(5);
        const fields = [CONFIG.price_field, CONFIG.volume_field];
        const table: any = await engine!.bdh([CONFIG.equity_single], fields, range);
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], 2);
        const gotFields = new Set<string>(
          table
            .getChild('field')
            .toArray()
            .map((v: any) => String(v)),
        );
        assert.ok(gotFields.has(CONFIG.price_field));
        assert.ok(gotFields.has(CONFIG.volume_field));
        console.log(`  got fields=${[...gotFields].join(', ')}`);
      }));

    it('date order is ascending', async (t) =>
      runCase(t, 'bdh dates ordered', async () => {
        const range = getDateRange(14);
        const table: any = await engine!.bdh([CONFIG.equity_single], [CONFIG.price_field], range);
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], 1);
        const dates = table.getChild('date').toArray().map(toMillis).filter(Number.isFinite);
        for (let i = 1; i < dates.length; i += 1) {
          assert.ok(dates[i] >= dates[i - 1], 'Dates should be ascending');
        }
        console.log(`  ordered dates=${dates.length}`);
      }));

    it('supports periodicitySelection kwargs', async (t) =>
      runCase(t, 'bdh kwargs periodicity', async () => {
        const range = getDateRange(30);
        const table: any = await engine!.bdh([CONFIG.equity_single], [CONFIG.price_field], {
          ...range,
          kwargs: { periodicitySelection: 'DAILY' },
        });
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], 1);
        console.log(`  periodicity DAILY rows=${table.numRows}`);
      }));

    it('contains at least one positive price', async (t) =>
      runCase(t, 'bdh positive values', async () => {
        const range = getDateRange(10);
        const table: any = await engine!.bdh([CONFIG.equity_single], [CONFIG.price_field], range);
        assertArrowTable(table, ['ticker', 'date', 'field', 'value'], 1);
        const values = table.getChild('value').toArray().map(toNumber).filter(Number.isFinite);
        assert.ok(
          values.some((v: number) => v > 0),
          'Expected at least one positive value',
        );
        console.log(`  positive observations=${values.filter((v: number) => v > 0).length}`);
      }));
  });

  describe('bDS bulk data', () => {
    it('index members return 30 rows', async (t) =>
      runCase(t, 'bds index members', async () => {
        const table: any = await engine!.bds([CONFIG.index_ticker], ['INDX_MEMBERS']);
        assertArrowTable(table, ['ticker'], 30);
        assert.equal(table.numRows, 30, `Expected 30 DJIA members, got ${table.numRows}`);
        console.log(`  INDU members rows=${table.numRows}`);
      }));

    it('index members include non-empty member identifiers', async (t) =>
      runCase(t, 'bds member identifiers', async () => {
        const table: any = await engine!.bds([CONFIG.index_ticker], ['INDX_MEMBERS']);
        assertArrowTable(table, ['ticker'], 30);
        const rows = table.toArray();
        assert.ok(rows.length === 30);
        const hasNonEmpty = rows.some((row: any) =>
          Object.values(row).some((v: any) => String(v ?? '').trim().length > 0),
        );
        assert.ok(hasNonEmpty, 'Expected non-empty member values');
        console.log(`  sample member row=${JSON.stringify(rows[0])}`);
      }));

    it('dividend history returns rows', async (t) =>
      runCase(t, 'bds dividend history', async () => {
        try {
          const table: any = await engine!.bds([CONFIG.equity_single], ['DVD_HIST']);
          assertArrowTable(table, ['ticker'], 1);
          console.log(`  dividend history rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('dividend history has structured columns', async (t) =>
      runCase(t, 'bds dividend columns', async () => {
        try {
          const table: any = await engine!.bds([CONFIG.equity_single], ['DVD_HIST']);
          assert.ok(table.numCols >= 2, `Expected >=2 columns, got ${table.numCols}`);
          console.log(`  dividend columns=${columnsOf(table).join(', ')}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));
  });

  describe('bDIB intraday bars', () => {
    it('single day 5-min bars', async (t) =>
      runCase(t, 'bdib single day', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdib(CONFIG.equity_single, {
          end: `${day}T20:00:00`,
          interval: 5,
          start: `${day}T14:30:00`,
        });
        assertArrowTable(
          table,
          ['time', 'open', 'high', 'low', 'close', 'volume', 'numEvents', 'value'],
          1,
        );
        console.log(`  day=${day}, rows=${table.numRows}`);
      }));

    it('datetime range 14:30-15:30 UTC', async (t) =>
      runCase(t, 'bdib datetime range', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdib(CONFIG.equity_single, {
          end: `${day}T15:30:00`,
          interval: 5,
          start: `${day}T14:30:00`,
        });
        assertArrowTable(
          table,
          ['time', 'open', 'high', 'low', 'close', 'volume', 'numEvents', 'value'],
          1,
        );
        console.log(`  ${day} 14:30-15:30 UTC rows=${table.numRows}`);
      }));

    it('bar OHLC values are numeric', async (t) =>
      runCase(t, 'bdib numeric ohlc', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdib(CONFIG.equity_single, {
          end: `${day}T15:30:00`,
          interval: 5,
          start: `${day}T14:30:00`,
        });
        assertArrowTable(table, ['open', 'high', 'low', 'close'], 1);
        const firstOpen = toNumber(table.getChild('open')?.get(0));
        const firstClose = toNumber(table.getChild('close')?.get(0));
        assert.ok(Number.isFinite(firstOpen));
        assert.ok(Number.isFinite(firstClose));
        console.log(`  first open=${firstOpen}, close=${firstClose}`);
      }));

    it('bar times are ordered', async (t) =>
      runCase(t, 'bdib times ordered', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdib(CONFIG.equity_single, {
          end: `${day}T16:00:00`,
          interval: 5,
          start: `${day}T14:30:00`,
        });
        assertArrowTable(table, ['time'], 1);
        const times = [...table.getChild('time')].map(toMillis).filter(Number.isFinite);
        for (let i = 1; i < times.length; i += 1) {
          assert.ok(times[i]! >= times[i - 1]!, 'Bar times should be ascending');
        }
        console.log(`  ordered bars=${times.length}`);
      }));
  });

  describe('bDTICK intraday ticks', () => {
    it('one-hour market open window', async (t) =>
      runCase(t, 'bdtick one-hour window', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdtick(CONFIG.equity_single, {
          end: `${day}T15:30:00`,
          eventTypes: ['TRADE'],
          start: `${day}T14:30:00`,
        });
        assertArrowTable(table, ['time'], 1);
        console.log(`  ticks rows=${table.numRows}, day=${day}`);
      }));

    it('tick columns include time/type/value', async (t) =>
      runCase(t, 'bdtick expected columns', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdtick(CONFIG.equity_single, {
          end: `${day}T15:30:00`,
          start: `${day}T14:30:00`,
        });
        assertArrowTable(table, ['time'], 1);
        const cols = columnsOf(table);
        assert.ok(
          cols.some((c) => c.toLowerCase().includes('value') || c.toLowerCase().includes('price')),
        );
        console.log(`  tick columns=${cols.join(', ')}`);
      }));

    it('supports multiple event types', async (t) =>
      runCase(t, 'bdtick multi event types', async () => {
        const day = getRecentTradingDay();
        const table: any = await engine!.bdtick(CONFIG.equity_single, {
          end: `${day}T15:30:00`,
          eventTypes: ['TRADE', 'BID'],
          start: `${day}T14:30:00`,
        });
        assertArrowTable(table, ['time'], 1);
        console.log(`  multi event rows=${table.numRows}`);
      }));

    it('tick times are ordered', async (t) =>
      runCase(t, 'bdtick times ordered', async () => {
        const day = getRecentTradingDay();
        const start = `${day}T14:30:00`;
        const end = `${day}T15:30:00`;
        const table: any = await engine!.bdtick(CONFIG.equity_single, {
          end,
          eventTypes: ['TRADE'],
          start,
        });
        assertArrowTable(table, ['time'], 1);
        const times = [...table.getChild('time')].map(toMillis).filter(Number.isFinite);
        for (let i = 1; i < times.length; i += 1) {
          assert.ok(times[i]! >= times[i - 1]!, 'Tick times should be ascending');
        }
        console.log(`  ordered ticks=${times.length}`);
      }));
  });

  describe('bQL query', () => {
    it('basic query returns rows', async (t) =>
      runCase(t, 'bql basic', async () => {
        try {
          const table: any = await engine!.bql("get(px_last) for('IBM US Equity')");
          assertArrowTable(table, columnsOf(table), 1);
          console.log(`  bql rows=${table.numRows}, cols=${table.numCols}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('query output has non-empty schema fields', async (t) =>
      runCase(t, 'bql schema fields', async () => {
        try {
          const table: any = await engine!.bql("get(px_last) for('IBM US Equity')");
          const fields = columnsOf(table);
          assert.ok(fields.length > 0, 'Expected BQL output fields');
          console.log(`  bql fields=${fields.join(', ')}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));
  });

  describe('bEQS screening', () => {
    it('core capital goods makers screen', async (t) =>
      runCase(t, 'beqs basic screen', async () => {
        try {
          const table: any = await withTimeout(
            engine!.beqs('Core Capital Goods Makers'),
            30_000,
            'BEQS',
          );
          assertArrowTable(table, columnsOf(table), 1);
          console.log(`  beqs rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('screen result columns are present', async (t) =>
      runCase(t, 'beqs columns', async () => {
        try {
          const table: any = await withTimeout(
            engine!.beqs('Core Capital Goods Makers'),
            30_000,
            'BEQS',
          );
          assert.ok(table.numCols >= 1);
          console.log(`  beqs columns=${columnsOf(table).join(', ')}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));
  });

  describe('bFLDS field metadata', () => {
    it('single field info PX_LAST', async (t) =>
      runCase(t, 'bflds single field', async () => {
        const table: any = await engine!.bflds({ fields: CONFIG.price_field });
        assertArrowTable(table, columnsOf(table), 1);
        console.log(`  bflds PX_LAST -> ${tableSummary(table)}`);
      }));

    it('multiple field info', async (t) =>
      runCase(t, 'bflds multi field', async () => {
        const table: any = await engine!.bflds({
          fields: [CONFIG.price_field, CONFIG.volume_field, CONFIG.name_field],
        });
        assertArrowTable(table, columnsOf(table), 3);
        console.log(`  bflds multi rows=${table.numRows}`);
      }));

    it('fieldSearch finds PX_LAST', async (t) =>
      runCase(t, 'fieldSearch PX_LAST', async () => {
        const table: any = await engine!.fieldSearch('PX_LAST');
        assertArrowTable(table, columnsOf(table), 1);
        console.log(`  fieldSearch rows=${table.numRows}`);
      }));

    it('fieldInfo alias works', async (t) =>
      runCase(t, 'fieldInfo alias', async () => {
        const table: any = await engine!.fieldInfo([CONFIG.price_field, CONFIG.volume_field]);
        assertArrowTable(table, columnsOf(table), 2);
        console.log(`  fieldInfo rows=${table.numRows}`);
      }));
  });

  describe('bLKP instrument lookup', () => {
    it('lookup IBM returns rows', async (t) =>
      runCase(t, 'blkp IBM lookup', async () => {
        try {
          const table: any = await engine!.blkp('IBM');
          assertArrowTable(table, columnsOf(table), 0);
          console.log(`  blkp rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('lookup result has text values', async (t) =>
      runCase(t, 'blkp textual result', async () => {
        try {
          const table: any = await engine!.blkp('IBM');
          if (table.numRows === 0) {
            t.skip('blkp returned 0 rows (service may be unavailable)');
            return;
          }
          assertArrowTable(table, columnsOf(table), 1);
          const firstRow = table.toArray()[0];
          const hasText = Object.values(firstRow).some(
            (v) => typeof v === 'string' && v.trim().length > 0,
          );
          assert.ok(hasText, 'Expected at least one textual value in lookup row');
          console.log(`  sample row=${JSON.stringify(firstRow)}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));
  });

  describe(`Streaming ${CONFIG.streaming_ticker}`, () => {
    it('subscribe receives 2-3 ticks and unsubscribe', async (t) =>
      runCase(t, 'stream subscribe/unsubscribe', async () => {
        const sub = (await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE'])).arrow();
        const rows = await collectStreamBatches(sub, 3, 15_000);
        for (const [index, table] of rows.entries()) {
          console.log(`  tick batch ${index + 1}: ${tableSummary(table)}`);
        }

        const drained = await sub.unsubscribe(true);
        assert.ok(rows.length > 0, `Expected at least 1 tick batch, got ${rows.length}`);
        assert.ok(Array.isArray(drained), 'unsubscribe(true) should return drained table array');
        console.log(`  received=${rows.length}, drained=${drained.length}`);
      }));

    it('streamed ticks contain price-like column', async (t) =>
      runCase(t, 'stream includes price-like field', async () => {
        const sub = (
          await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE', 'BID', 'ASK'])
        ).arrow();
        const result: any = await nextWithTimeout(sub, 12_000);
        await sub.unsubscribe(true);
        assert.ok(result && !result.done, 'Expected at least one streamed tick batch');
        const cols = columnsOf(result.value);
        assert.ok(
          cols.some(
            (c) =>
              c.toLowerCase().includes('price') ||
              c.toLowerCase().includes('last') ||
              c.toLowerCase().includes('bid'),
          ),
        );
        console.log(`  stream columns=${cols.join(', ')}`);
      }));

    it('stream supports conflated market data option', async (t) =>
      runCase(t, 'stream conflate option', async () => {
        const sub = (
          await engine!.stream(
            [CONFIG.streaming_ticker],
            ['LAST_PRICE', 'BID', 'ASK', 'MKTDATA_EVENT_TYPE', 'MKTDATA_EVENT_SUBTYPE'],
            { conflate: true },
          )
        ).arrow();
        const result: any = await nextWithTimeout(sub, 20_000);
        await sub.unsubscribe(true);
        assert.ok(result && !result.done, 'Expected at least one conflated tick batch');
        const cols = columnsOf(result.value);
        assert.ok(cols.includes('MKTDATA_EVENT_TYPE'), `columns=${cols.join(', ')}`);
        console.log(`  conflate stream columns=${cols.join(', ')}`);
      }));

    it('stream continues while BDP and BDH requests run concurrently', async (t) =>
      runCase(t, 'stream concurrent with reference requests', async () => {
        const sub = await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE']);
        let batches: any[];
        let bdpTable: any;
        let bdhTable: any;
        try {
          const range = getDateRange(5);
          const requests = Promise.all([
            withTimeout(
              engine!.bdp([CONFIG.equity_single], [CONFIG.price_field]),
              20_000,
              'concurrent BDP',
            ),
            withTimeout(
              engine!.bdh([CONFIG.equity_single], [CONFIG.price_field], range),
              25_000,
              'concurrent BDH',
            ),
          ]);

          batches = await collectStreamBatches(sub, 1, 20_000);
          [bdpTable, bdhTable] = (await requests) as [any, any];
        } finally {
          await sub.unsubscribe(true).catch(() => []);
        }

        assertArrowTable(bdpTable, ['ticker', 'field', 'value']);
        assertArrowTable(bdhTable, ['ticker', 'date', 'field', 'value']);
        assert.ok(batches.length > 0, 'Expected stream to remain alive during requests');
        console.log(
          `  concurrent stream batches=${batches.length}, bdp=${bdpTable.numRows}, bdh=${bdhTable.numRows}`,
        );
      }));

    it('allFields is a strict superset of filtered fields when full payload is present', async (t) =>
      runCase(t, 'stream allFields vs filtered payload', async () => {
        let filtered: any;
        let allFields: any;
        let filteredResult: any;
        let allFieldsResult: any;
        try {
          filtered = (await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE'])).arrow();
          allFields = (
            await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE'], {
              allFields: true,
            })
          ).arrow();

          [filteredResult, allFieldsResult] = await Promise.all([
            nextWithTimeout(filtered, 15_000),
            nextWithTimeout(allFields, 15_000),
          ]);

          assert.ok(filteredResult && !filteredResult.done, 'Expected filtered tick batch');
          assert.ok(allFieldsResult && !allFieldsResult.done, 'Expected allFields tick batch');
          const filteredCols = columnSet(filteredResult.value);
          const allCols = columnSet(allFieldsResult.value);
          for (const col of filteredCols) {
            assert.ok(allCols.has(col), `allFields payload missing filtered column: ${col}`);
          }

          const extraCols = [...allCols].filter((col) => !filteredCols.has(col));
          if (allCols.size <= filteredCols.size) {
            t.skip(
              `Bloomberg did not provide a wider allFields payload in this window: filtered=${[...filteredCols].join(', ')} all=${[...allCols].join(', ')}`,
            );
            return;
          }
          assert.ok(
            extraCols.length > 0,
            'Expected allFields to contain columns beyond filtered mode',
          );

          const row = allFieldsResult.value.toArray()[0] as Record<string, unknown>;
          assert.notEqual(row.MKTDATA_EVENT_TYPE, null, 'MKTDATA_EVENT_TYPE should be non-null');
          assert.notEqual(
            row.MKTDATA_EVENT_TYPE,
            undefined,
            'MKTDATA_EVENT_TYPE should be defined',
          );
          assert.notEqual(
            row.MKTDATA_EVENT_SUBTYPE,
            null,
            'MKTDATA_EVENT_SUBTYPE should be non-null',
          );
          assert.notEqual(
            row.MKTDATA_EVENT_SUBTYPE,
            undefined,
            'MKTDATA_EVENT_SUBTYPE should be defined',
          );
          console.log(
            `  filtered cols=${filteredCols.size}, allFields cols=${allCols.size}, extra=${extraCols.slice(0, 8).join(', ')}`,
          );
        } finally {
          await filtered?.unsubscribe(false).catch(() => []);
          await allFields?.unsubscribe(false).catch(() => []);
        }
      }));

    it('subscription add/remove updates metadata without killing stream', async (t) =>
      runCase(t, 'stream add/remove tickers', async () => {
        const addedTicker = 'IBM US Equity';
        const sub = await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE']);
        const arrowSub = sub.arrow();
        try {
          assert.deepEqual(sub.tickers, [CONFIG.streaming_ticker]);
          await sub.add([addedTicker]);
          assert.ok(
            sub.tickers.includes(addedTicker),
            `Expected metadata to include ${addedTicker}`,
          );
          await sub.remove([addedTicker]);
          assert.ok(
            !sub.tickers.includes(addedTicker),
            `Expected metadata to remove ${addedTicker}: ${sub.tickers.join(', ')}`,
          );

          const next: any = await nextWithTimeout(arrowSub, 12_000);
          assert.ok(next && !next.done, 'Expected primary stream data after add/remove');
          const topics = new Set(
            next.value
              .toArray()
              .map((row: Record<string, unknown>) => {
                const topic = row.topic ?? row.TICKER;
                return typeof topic === 'string' ? topic : '';
              })
              .filter((topic: string) => topic.length > 0),
          );
          if (topics.size > 0) {
            assert.ok(
              topics.has(CONFIG.streaming_ticker),
              `Expected post-remove batch to include primary ticker; got ${[...topics].join(', ')}`,
            );
          }
          console.log(`  tickers=${sub.tickers.join(', ')}, stats=${JSON.stringify(sub.stats)}`);
        } finally {
          await sub.unsubscribe(false).catch(() => []);
        }
      }));

    it('subscription metadata is populated', async (t) =>
      runCase(t, 'stream metadata', async () => {
        const sub = await engine!.stream([CONFIG.streaming_ticker], ['LAST_PRICE']);
        assert.ok(Array.isArray(sub.tickers));
        assert.ok(Array.isArray(sub.fields));
        assert.equal(sub.isActive, true);
        const first: any = await Promise.race([sub.next(), sleep(8000).then(() => null)]);
        await sub.unsubscribe(false);
        assert.ok(first && !first.done, 'Expected one streamed table');
        console.log(`  tickers=${sub.tickers.join(', ')}, fields=${sub.fields.join(', ')}`);
      }));
  });

  describe('schema and operations', () => {
    it('bops lists operations', async (t) =>
      runCase(t, 'bops list operations', async () => {
        const ops = await engine!.bops('//blp/refdata');
        assert.ok(Array.isArray(ops));
        assert.ok(ops.length > 0);
        console.log(`  bops count=${ops.length}`);
      }));

    it('bschema returns service schema', async (t) =>
      runCase(t, 'bschema service', async () => {
        const schema: any = await engine!.bschema('//blp/refdata');
        assert.ok(schema && typeof schema === 'object');
        assert.ok(Array.isArray(schema.operations));
        console.log(`  schema operations=${schema.operations.length}`);
      }));

    it('bschema returns operation schema', async (t) =>
      runCase(t, 'bschema operation', async () => {
        const opSchema: any = await engine!.bschema('//blp/refdata', 'ReferenceDataRequest');
        assert.ok(opSchema && typeof opSchema === 'object');
        console.log(`  op schema keys=${Object.keys(opSchema).slice(0, 6).join(', ')}`);
      }));

    it('listOperations mirrors bops', async (t) =>
      runCase(t, 'listOperations', async () => {
        const [opsA, opsB] = await Promise.all([
          engine!.bops('//blp/refdata'),
          engine!.listOperations('//blp/refdata'),
        ]);
        assert.ok(Array.isArray(opsA));
        assert.ok(Array.isArray(opsB));
        assert.equal(opsA.length, opsB.length);
        console.log(`  bops/listOperations count=${opsA.length}`);
      }));

    it('getEnumValues returns periodicity selection', async (t) =>
      runCase(t, 'getEnumValues', async () => {
        const vals = await engine!.getEnumValues(
          '//blp/refdata',
          'HistoricalDataRequest',
          'periodicitySelection',
        );
        assert.ok(vals === null || Array.isArray(vals));
        if (Array.isArray(vals)) {
          assert.ok(vals.length > 0);
        }
        console.log(`  enum values=${Array.isArray(vals) ? vals.join(', ') : 'null'}`);
      }));

    it('listValidElements returns request elements', async (t) =>
      runCase(t, 'listValidElements', async () => {
        const elems = await engine!.listValidElements('//blp/refdata', 'ReferenceDataRequest');
        assert.ok(elems === null || Array.isArray(elems));
        if (Array.isArray(elems)) {
          assert.ok(elems.length > 0);
        }
        console.log(`  valid elements count=${Array.isArray(elems) ? elems.length : 0}`);
      }));
  });

  describe('backend conversion', () => {
    it('bDP JSON backend returns array', async (t) =>
      runCase(t, 'backend JSON bdp', async () => {
        const rows: any = await engine!.request({
          backend: Backend.JSON,
          extractor: 'refdata',
          fields: [CONFIG.price_field],
          operation: 'ReferenceDataRequest',
          securities: [CONFIG.equity_single],
          service: '//blp/refdata',
        });
        assert.ok(Array.isArray(rows), 'Expected JSON backend to return array');
        assert.ok(rows.length > 0);
        assert.ok(Object.hasOwn(rows[0], 'ticker'));
        assert.ok(Object.hasOwn(rows[0], 'field'));
        assert.ok(Object.hasOwn(rows[0], 'value'));
        console.log(`  json rows=${rows.length}, first=${JSON.stringify(rows[0])}`);
      }));

    it('bDH JSON backend returns array', async (t) =>
      runCase(t, 'backend JSON bdh', async () => {
        const range = getDateRange(5);
        const rows: any = await engine!.request({
          backend: Backend.JSON,
          endDate: range.end,
          extractor: 'histdata',
          fields: [CONFIG.price_field],
          operation: 'HistoricalDataRequest',
          securities: [CONFIG.equity_single],
          service: '//blp/refdata',
          startDate: range.start,
        });
        assert.ok(Array.isArray(rows), 'Expected JSON backend to return array');
        assert.ok(rows.length > 0);
        assert.ok(Object.hasOwn(rows[0], 'date'));
        console.log(`  json hist rows=${rows.length}`);
      }));

    it('generic request with JSON backend', async (t) =>
      runCase(t, 'backend JSON request()', async () => {
        const rows: any = await engine!.request({
          backend: Backend.JSON,
          extractor: 'refdata',
          fields: [CONFIG.price_field],
          operation: 'ReferenceDataRequest',
          securities: [CONFIG.equity_single],
          service: '//blp/refdata',
        });
        assert.ok(Array.isArray(rows));
        assert.ok(rows.length > 0);
        console.log(`  request(json) rows=${rows.length}`);
      }));
  });

  describe('additional API coverage', () => {
    it('bcurves query is callable', async (t) =>
      runCase(t, 'bcurves callable', async () => {
        try {
          const table: any = await engine!.bcurves('YCSW0023 Index');
          assert.ok(table.numRows >= 0);
          console.log(`  bcurves rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('bgovts query is callable', async (t) =>
      runCase(t, 'bgovts callable', async () => {
        try {
          const table: any = await engine!.bgovts('USD');
          assert.ok(table.numRows >= 0);
          console.log(`  bgovts rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('bport API is callable', async (t) =>
      runCase(t, 'bport callable', async () => {
        if (!CONFIG.portfolio_security) {
          t.skip('Set XBBG_LIVE_PORTFOLIO_SECURITY to run portfolio live tests');
          return;
        }

        try {
          const table: any = await engine!.bport(CONFIG.portfolio_security, ['PORTFOLIO_DATA']);
          assert.ok(table.numRows >= 0);
          console.log(`  bport rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('bsrch returns search results', async (t) =>
      runCase(t, 'bsrch basic', async () => {
        try {
          const table: any = await withTimeout(engine!.bsrch('FI:SOVR'), 30_000, 'bsrch');
          assert.ok(table.numRows >= 0);
          console.log(`  bsrch rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('bta technical analysis request', async (t) =>
      runCase(t, 'bta basic', async () => {
        try {
          const range = getDateRange(30);
          const table: any = await withTimeout(
            engine!.bta(CONFIG.futures_generic, 'sma', {
              kwargs: { endDate: range.end, startDate: range.start },
              studyParams: { calcInterval: 'DAILY', length: 20 },
            }),
            30_000,
            'bta',
          );
          assert.ok(table.numRows >= 0);
          console.log(`  bta rows=${table.numRows}`);
        } catch (error) {
          if (!maybeSkipEntitlement(t, error)) throw error;
        }
      }));

    it('resolveFieldTypes works', async (t) =>
      runCase(t, 'resolveFieldTypes', async () => {
        const mapping = await engine!.resolveFieldTypes([
          CONFIG.price_field,
          CONFIG.volume_field,
          CONFIG.name_field,
        ]);
        assert.ok(typeof mapping === 'object');
        assert.ok(Object.keys(mapping).length >= 3);
        console.log(`  field types=${JSON.stringify(mapping)}`);
      }));

    it('validateFields returns validation result', async (t) =>
      runCase(t, 'validateFields', async () => {
        const result = await engine!.validateFields([CONFIG.price_field, CONFIG.name_field]);
        assert.ok(Array.isArray(result));
        console.log(`  validateFields=${JSON.stringify(result)}`);
      }));

    it('schema cache lifecycle operations callable', async (t) =>
      runCase(t, 'schema cache lifecycle', async () => {
        engine!.clearSchemaCache();
        const cached0 = engine!.listCachedSchemas();
        assert.ok(Array.isArray(cached0));
        await engine!.getSchema('//blp/refdata');
        const cached1 = engine!.listCachedSchemas();
        assert.ok(Array.isArray(cached1));
        engine!.invalidateSchema('//blp/refdata');
        const cached2 = engine!.listCachedSchemas();
        assert.ok(Array.isArray(cached2));
        console.log(
          `  cache sizes before=${cached0.length}, afterLoad=${cached1.length}, afterInvalidate=${cached2.length}`,
        );
      }));

    it('field cache lifecycle operations callable', async (t) =>
      runCase(t, 'field cache lifecycle', async () => {
        engine!.clearFieldCache();
        const enabled = engine!.isFieldValidationEnabled();
        assert.ok(typeof enabled === 'boolean' || enabled === undefined);
        engine!.saveFieldCache();
        assert.ok(true, 'saveFieldCache should be callable');
        console.log(`  validationEnabled=${enabled}, saveFieldCache=called`);
      }));

    it('getFieldInfo callable for PX_LAST', async (t) =>
      runCase(t, 'getFieldInfo', async () => {
        const info = engine!.getFieldInfo(CONFIG.price_field);
        assert.ok(info === null || typeof info === 'object');
        console.log(`  getFieldInfo type=${info === null ? 'null' : typeof info}`);
      }));
  });

  describe('eTF NAV recipes', () => {
    const QQQ = 'QQQ US Equity';
    const AT1 = 'AT1 LN Equity';
    const QQQ_NAV = 'QQQNV Index';
    const QQQ_INAV = 'QXV Index';
    const AT1_INAV = 'AT1IN Index';

    const RELATIONSHIP_COLUMNS = [
      'input_order',
      'etf_ticker',
      'nav_ticker',
      'nav_market_sector_des',
      'nav_name',
      'nav_validation_error',
      'inav_ticker',
      'inav_market_sector_des',
      'inav_name',
      'inav_validation_error',
    ];
    const SNAPSHOT_COLUMNS = [
      'input_order',
      'etf_ticker',
      'nav_ticker',
      'nav_value',
      'nav_source_ticker',
      'nav_source_field',
      'inav_ticker',
      'inav_value',
    ];
    const HISTORY_COLUMNS = [
      'input_order',
      'etf_ticker',
      'date',
      'nav_ticker',
      'nav_value',
      'nav_source_ticker',
      'nav_source_field',
      'inav_ticker',
      'inav_value',
    ];

    it('resolves ETF NAV relationships for QQQ and AT1 exactly', async (t) =>
      runCase(t, 'ETF NAV relationships', async () => {
        // Default recipe backend returns an apache-arrow Table.
        const table = (await engine!.etfNavRelationships([QQQ, AT1])) as Table;
        assert.deepEqual(columnsOf(table), RELATIONSHIP_COLUMNS);
        assert.equal(table.numRows, 2);

        assert.equal(stringCell(table, 'etf_ticker', 0), QQQ);
        assert.equal(stringCell(table, 'nav_ticker', 0), QQQ_NAV);
        assert.equal(stringCell(table, 'inav_ticker', 0), QQQ_INAV);
        assert.equal(stringCell(table, 'nav_market_sector_des', 0)?.trim().toLowerCase(), 'index');
        assert.equal(stringCell(table, 'inav_market_sector_des', 0)?.trim().toLowerCase(), 'index');
        assert.ok((stringCell(table, 'nav_name', 0) ?? '').trim().length > 0);
        assert.ok((stringCell(table, 'inav_name', 0) ?? '').trim().length > 0);
        assert.equal(cellValue(table, 'nav_validation_error', 0), null);
        assert.equal(cellValue(table, 'inav_validation_error', 0), null);

        assert.equal(stringCell(table, 'etf_ticker', 1), AT1);
        assert.equal(cellValue(table, 'nav_ticker', 1), null);
        assert.equal(cellValue(table, 'nav_market_sector_des', 1), null);
        assert.equal(cellValue(table, 'nav_name', 1), null);
        assert.equal(cellValue(table, 'nav_validation_error', 1), null);
        assert.equal(stringCell(table, 'inav_ticker', 1), AT1_INAV);
        assert.equal(stringCell(table, 'inav_market_sector_des', 1)?.trim().toLowerCase(), 'index');
        assert.ok((stringCell(table, 'inav_name', 1) ?? '').trim().length > 0);
        assert.equal(cellValue(table, 'inav_validation_error', 1), null);
        console.log(`  relationships -> ${tableSummary(table)}`);
      }));

    it('prices ETF NAV snapshots from PX_LAST and FUND_NET_ASSET_VAL sources', async (t) =>
      runCase(t, 'ETF NAV snapshot', async () => {
        const table = (await engine!.etfNavSnapshot([QQQ, AT1])) as Table;
        assert.deepEqual(columnsOf(table), SNAPSHOT_COLUMNS);
        assert.equal(table.numRows, 2);

        assert.equal(stringCell(table, 'nav_ticker', 0), QQQ_NAV);
        assert.equal(stringCell(table, 'nav_source_ticker', 0), QQQ_NAV);
        assert.equal(stringCell(table, 'nav_source_field', 0), 'PX_LAST');
        assert.equal(stringCell(table, 'inav_ticker', 0), QQQ_INAV);
        assert.ok((numberCell(table, 'nav_value', 0) ?? 0) > 0);
        assert.ok((numberCell(table, 'inav_value', 0) ?? 0) > 0);

        assert.equal(cellValue(table, 'nav_ticker', 1), null);
        assert.equal(stringCell(table, 'nav_source_ticker', 1), AT1);
        assert.equal(stringCell(table, 'nav_source_field', 1), 'FUND_NET_ASSET_VAL');
        assert.equal(stringCell(table, 'inav_ticker', 1), AT1_INAV);
        assert.ok((numberCell(table, 'nav_value', 1) ?? 0) > 0);
        assert.ok((numberCell(table, 'inav_value', 1) ?? 0) > 0);
        console.log(`  snapshot -> ${tableSummary(table)}`);
      }));

    it('unions daily ETF NAV and iNAV history observations', async (t) =>
      runCase(t, 'ETF NAV history', async () => {
        const range = getDateRange(14);
        const table = (await engine!.etfNavHistory([QQQ, AT1], range.start, range.end)) as Table;
        assert.deepEqual(columnsOf(table), HISTORY_COLUMNS);
        assert.ok(table.numRows > 0, 'expected at least one daily observation');

        let qqqRows = 0;
        let qqqNavPoints = 0;
        let at1Rows = 0;
        let at1NavPoints = 0;
        let previousQqqDate = Number.NEGATIVE_INFINITY;
        for (let row = 0; row < table.numRows; row += 1) {
          const etf = stringCell(table, 'etf_ticker', row);
          const dateMillis = toMillis(cellValue(table, 'date', row));
          assert.ok(Number.isFinite(dateMillis), 'expected a real observation date');
          if (etf === QQQ) {
            qqqRows += 1;
            assert.equal(stringCell(table, 'nav_ticker', row), QQQ_NAV);
            assert.equal(stringCell(table, 'nav_source_ticker', row), QQQ_NAV);
            assert.equal(stringCell(table, 'nav_source_field', row), 'PX_LAST');
            assert.equal(stringCell(table, 'inav_ticker', row), QQQ_INAV);
            assert.ok(dateMillis > previousQqqDate, 'QQQ dates must be strictly increasing');
            previousQqqDate = dateMillis;
            if (numberCell(table, 'nav_value', row) !== null) {
              qqqNavPoints += 1;
            }
          } else {
            assert.equal(etf, AT1);
            at1Rows += 1;
            assert.equal(cellValue(table, 'nav_ticker', row), null);
            assert.equal(stringCell(table, 'nav_source_ticker', row), AT1);
            assert.equal(stringCell(table, 'nav_source_field', row), 'FUND_NET_ASSET_VAL');
            assert.equal(stringCell(table, 'inav_ticker', row), AT1_INAV);
            if (numberCell(table, 'nav_value', row) !== null) {
              at1NavPoints += 1;
            }
          }
        }
        assert.ok(qqqRows > 0, 'expected QQQ history rows');
        assert.ok(at1Rows > 0, 'expected AT1 history rows');
        assert.ok(qqqNavPoints > 0, 'expected QQQ PX_LAST NAV points');
        assert.ok(at1NavPoints > 0, 'expected AT1 FUND_NET_ASSET_VAL points');
        console.log(
          `  history -> rows=${table.numRows}, qqq=${qqqRows}/${qqqNavPoints}, at1=${at1Rows}/${at1NavPoints}`,
        );
      }));

    it('opens the ETF NAV iNAV subscription on QXV Index and unsubscribes', async (t) =>
      runCase(t, 'ETF NAV iNAV subscription', async () => {
        const subscription = await engine!.subscribeEtfInav(QQQ);
        try {
          assert.deepEqual(subscription.tickers, [QQQ_INAV]);

          const deadline = Date.now() + 60_000;
          let lastPrice: number | null = null;
          while (lastPrice === null) {
            const remaining = deadline - Date.now();
            assert.ok(remaining > 0, 'no QXV Index LAST_PRICE update within 60s');
            const result = await withTimeout(subscription.next(), remaining, 'iNAV tick');
            if (result.done) {
              assert.fail('subscription ended before a LAST_PRICE update');
            }
            const tick = result.value;
            if (tick.topic === QQQ_INAV) {
              const value: unknown = tick.get('LAST_PRICE');
              if (typeof value === 'number' && Number.isFinite(value)) {
                lastPrice = value;
              }
            }
          }
          assert.ok(lastPrice > 0, 'expected a positive QXV Index LAST_PRICE');
          console.log(`  iNAV subscription -> topic=${QQQ_INAV}, LAST_PRICE=${lastPrice}`);
        } finally {
          await subscription.unsubscribe();
        }
      }));
  });

  describe('auction and shared subscription contracts', () => {
    const topic = 'IBM US Equity';
    const readTimeoutMs = 10_000;

    async function waitForState(predicate: () => boolean, label: string): Promise<void> {
      const deadline = Date.now() + readTimeoutMs;
      while (!predicate()) {
        assert.ok(Date.now() < deadline, `No ${label} within 10s`);
        await sleep(50);
      }
    }

    it('resolves composite and ISIN inputs and respects an explicit venue', async (t) =>
      runCase(t, 'auction venue resolution', async () => {
        const table = (await withTimeout(
          engine!.resolveVenues(['IBM US Equity', 'US0378331005', 'SPY UP Equity']),
          15_000,
          'venue resolution',
        )) as Table;
        assert.equal(table.numRows, 3);
        assert.deepEqual(
          [0, 1, 2].map((row) => stringCell(table, 'venue_topic', row)),
          ['IBM UN Equity', 'AAPL UW Equity', 'SPY UP Equity'],
        );
        assert.deepEqual(
          [0, 1, 2].map((row) => stringCell(table, 'status', row)),
          ['resolved', 'resolved', 'resolved'],
        );
        assert.equal(stringCell(table, 'method', 2), 'as_is');
      }));

    it('returns validated auction columns without requiring an active auction', async (t) =>
      runCase(t, 'auction snapshot', async () => {
        const table = (await withTimeout(
          engine!.auctionSnapshot([topic], { fields: ['IN_AUCTION_RT', 'ORDER_IMB_BUY_VOLUME'] }),
          15_000,
          'auction snapshot',
        )) as Table;
        assertArrowTable(table, [
          'input_order',
          'security',
          'venue_topic',
          'status',
          'error',
          'IN_AUCTION_RT',
          'ORDER_IMB_BUY_VOLUME',
        ]);
        assert.equal(table.numRows, 1);
        assert.equal(stringCell(table, 'status', 0), 'resolved');
        assert.equal(stringCell(table, 'venue_topic', 0), 'IBM UN Equity');
      }));

    it('keeps Time64 microsecond and Date32 auction columns without requiring non-null values', async (t) =>
      runCase(t, 'auction snapshot temporal types', async () => {
        const timeField = 'IMBALANCE_TIMESTAMP_RT';
        const dateField = 'CLOSING_AUCTION_VOLUME_DATE_RT';
        const table = (await withTimeout(
          engine!.auctionSnapshot([topic], { fields: [timeField, dateField] }),
          15_000,
          'auction temporal snapshot',
        )) as Table;
        assertArrowTable(table, ['security', 'venue_topic', 'status', timeField, dateField]);
        assert.equal(table.numRows, 1);
        assert.equal(stringCell(table, 'status', 0), 'resolved');
        assert.equal(stringCell(table, 'venue_topic', 0), 'IBM UN Equity');

        // Schema types must not depend on there being an active auction or a non-null value.
        const timeType = table.getChild(timeField)?.type;
        assert.ok(DataType.isTime(timeType), 'Expected an Arrow time-of-day column');
        assert.equal(timeType.bitWidth, 64);
        assert.equal(timeType.unit, TimeUnit.MICROSECOND);
        const dateType = table.getChild(dateField)?.type;
        assert.ok(DataType.isDate(dateType), 'Expected an Arrow date column');
        assert.equal(dateType.unit, DateUnit.DAY);
      }));

    it('shares a field union and late image, but isolates an opted-out consumer', async (t) =>
      runCase(t, 'shared feed lifecycle', async () => {
        const first = await engine!.subscribe([topic], ['LAST_PRICE', 'BID']);
        let second: Subscription | undefined;
        let isolated: Subscription | undefined;
        const failures: unknown[] = [];
        const feeds = (): FeedInfo[] =>
          engine!
            .subscriptionFeeds()
            .filter((feed) => feed.service === '//blp/mktdata' && feed.topic === topic);
        try {
          const paint = await first.next({ signal: AbortSignal.timeout(readTimeoutMs) });
          assert.equal(paint.done, false, 'Initial paint should be available outside market hours');
          second = await engine!.subscribe([topic], ['LAST_PRICE', 'ASK']);
          const late = await second.next({ signal: AbortSignal.timeout(readTimeoutMs) });
          assert.equal(late.done, false);
          assert.equal(late.value?.str('MKTDATA_EVENT_TYPE'), 'SUMMARY');
          assert.equal(late.value?.str('MKTDATA_EVENT_SUBTYPE'), 'INITPAINT');
          const shared = feeds();
          assert.equal(shared.length, 1);
          assert.equal(shared[0]?.consumers, 2);
          assert.ok(shared[0]?.fields.includes('BID'));
          assert.ok(shared[0]?.fields.includes('ASK'));

          isolated = await engine!.subscribe([topic], ['BID'], { isolated: true });
          await waitForState(() => feeds().length === 2, 'isolated feed');
          assert.equal(feeds().find((feed) => feed.isolated)?.consumers, 1);
          assert.equal(feeds().find((feed) => !feed.isolated)?.consumers, 2);
        } catch (error) {
          failures.push(error);
        } finally {
          const cleanup = await Promise.allSettled(
            [first, second, isolated].map(async (sub) => sub?.unsubscribe()),
          );
          for (const result of cleanup) {
            if (result.status === 'rejected') {
              failures.push(result.reason);
            }
          }
        }
        try {
          await waitForState(() => feeds().length === 0, 'feed retirement after the last consumer');
        } catch (error) {
          failures.push(error);
        }
        if (failures.length > 0) {
          throw new AggregateError(failures, 'Shared-feed lifecycle failed');
        }
      }));

    it('grows fields and exposes image metadata after initial paint', async (t) =>
      runCase(t, 'subscription addFields and latest', async () => {
        const sub = await engine!.subscribe([topic], ['LAST_PRICE', 'BID']);
        try {
          const paint = await sub.next({ signal: AbortSignal.timeout(readTimeoutMs) });
          assert.equal(paint.done, false);
          await sub.addFields(['ASK']);
          assert.ok(sub.fields.includes('ASK'));
          await waitForState(
            () => typeof sub.topicStates[topic]?.delayed === 'boolean',
            'delayed flag from initial paint',
          );
          const image = sub.latest() as Table;
          assertArrowTable(image, ['topic', 'last_update', 'live', 'delayed', 'BID', 'ASK']);
          assert.equal(image.numRows, 1);
          assert.equal(stringCell(image, 'topic', 0), topic);
          assert.equal(typeof cellValue(image, 'live', 0), 'boolean');
          assert.equal(typeof cellValue(image, 'delayed', 0), 'boolean');
          assert.notEqual(cellValue(image, 'last_update', 0), null);
        } finally {
          await sub.unsubscribe();
        }
      }));

    it('updates an image-only board through latest without reading tick rows', async (t) =>
      runCase(t, 'image-only subscription board', async () => {
        const sub = await engine!.subscribe([topic], ['LAST_PRICE', 'BID'], { rows: false });
        try {
          let image = sub.latest() as Table;
          await waitForState(() => {
            image = sub.latest() as Table;
            const lastUpdate = cellValue(image, 'last_update', 0);
            const lastPrice = numberCell(image, 'LAST_PRICE', 0);
            return (
              lastUpdate !== null &&
              lastUpdate !== undefined &&
              lastPrice !== null &&
              Number.isFinite(lastPrice)
            );
          }, 'image-only initial paint with a known LAST_PRICE');
          assertArrowTable(image, ['topic', 'last_update', 'live', 'delayed', 'LAST_PRICE', 'BID']);
          assert.equal(image.numRows, 1);
          assert.equal(stringCell(image, 'topic', 0), topic);
          assert.equal(typeof cellValue(image, 'live', 0), 'boolean');
          assert.ok(sub.isActive, 'Reading the image must leave the subscription active');
        } finally {
          await sub.unsubscribe();
        }
      }));

    it('surfaces a rejected static field through Node warnings and fieldErrors', async (t) =>
      runCase(t, 'subscription field warning', async () => {
        const warnings: Error[] = [];
        const listener = (warning: Error): void => {
          warnings.push(warning);
        };
        process.on('warning', listener);
        let sub: Subscription | undefined;
        try {
          sub = await engine!.subscribe([topic], ['LAST_PRICE', 'BID', 'PX_BID']);
          const paint = await sub.next({ signal: AbortSignal.timeout(readTimeoutMs) });
          assert.equal(paint.done, false);
          await waitForState(() => {
            sub!.latest(); // Drains warnings without depending on another market tick.
            return (
              sub!.fieldErrors[topic]?.PX_BID === 'BAD_FLD' &&
              warnings.some(
                (warning) =>
                  warning.name === 'BlpFieldWarning' &&
                  'code' in warning &&
                  warning.code === 'XBBG_FIELD_EXCEPTION',
              )
            );
          }, 'field warning and BAD_FLD status');
        } finally {
          try {
            await sub?.unsubscribe();
          } finally {
            process.removeListener('warning', listener);
          }
        }
      }));

    it('fails a rejected field with onFieldError raise without waiting for trades', async (t) =>
      runCase(t, 'subscription field-error raise', async () => {
        const sub = await engine!.subscribe([topic], ['LAST_PRICE', 'PX_BID'], {
          rows: false,
          onFieldError: 'raise',
        });
        try {
          await waitForState(
            () =>
              sub.fieldErrors[topic]?.PX_BID === 'BAD_FLD' &&
              sub.topicStates[topic]?.state === 'failed' &&
              !sub.isActive,
            'rejected field category and failed topic',
          );
          assert.ok(sub.failedTickers.includes(topic));
          const failure = sub.failures.find((entry) => entry.topic === topic);
          assert.ok(failure, 'Expected a topic failure for the rejected field');
          assert.match(failure.reason, /PX_BID/u);
          assert.match(failure.reason, /BAD_FLD/u);
          assert.throws(() => sub.latest(), /subscription already closed/iu);
        } finally {
          await sub.unsubscribe();
        }
      }));

    it('labels auction ticks with the input and keeps the upstream venue in status', async (t) =>
      runCase(t, 'auction subscription alias', async () => {
        const sub = await engine!.subscribeAuction([topic], { fields: ['LAST_PRICE', 'BID'] });
        try {
          const paint = await sub.next({ signal: AbortSignal.timeout(readTimeoutMs) });
          assert.equal(paint.done, false);
          assert.equal(paint.value?.topic, topic);
          assert.equal(sub.topicStates[topic]?.feedTopic, 'IBM UN Equity');
        } finally {
          await sub.unsubscribe();
        }
      }));

    it('masks observed zero auction-price sentinels by default while an explicit empty list preserves them', async (t) =>
      runCase(t, 'auction default zero-price masking', async () => {
        const securities = [topic, 'AAPL US Equity'];
        const fields = ['THEO_PRICE', 'INDICATIVE_NEAR', 'INDICATIVE_FAR'];
        const subscriptions: Subscription[] = [];
        const failures: unknown[] = [];
        let observedZero = false;
        try {
          const raw = await engine!.subscribeAuction(securities, {
            fields,
            rows: false,
            zeroAsNull: [],
          });
          subscriptions.push(raw);
          const masked = await engine!.subscribeAuction(securities, { fields, rows: false });
          subscriptions.push(masked);

          const deadline = Date.now() + readTimeoutMs;
          while (Date.now() < deadline) {
            const rawImage = raw.latest() as Table;
            const maskedImage = masked.latest() as Table;
            for (const image of [rawImage, maskedImage]) {
              assertArrowTable(image, ['topic', 'last_update', ...fields], securities.length);
              assert.equal(image.numRows, securities.length);
            }
            for (let row = 0; row < securities.length; row += 1) {
              assert.equal(stringCell(rawImage, 'topic', row), securities[row]);
              assert.equal(stringCell(maskedImage, 'topic', row), securities[row]);
              const updatedAt = toMillis(cellValue(rawImage, 'last_update', row));
              // Compare the same shared-feed image, not values separated by a market update.
              if (
                !Number.isFinite(updatedAt) ||
                updatedAt !== toMillis(cellValue(maskedImage, 'last_update', row))
              ) {
                continue;
              }
              const zeroFields = fields.filter((field) => {
                const value = cellValue(rawImage, field, row);
                return value === 0 || value === 0n;
              });
              observedZero ||= zeroFields.length > 0;
              for (const field of zeroFields) {
                assert.equal(
                  cellValue(maskedImage, field, row),
                  null,
                  'Default auction board must mask a zero price sentinel',
                );
              }
            }
            if (observedZero) {
              break;
            }
            await sleep(Math.min(50, Math.max(0, deadline - Date.now())));
          }
        } catch (error) {
          failures.push(error);
        } finally {
          const cleanup = await Promise.allSettled(
            subscriptions.map(async (sub) => sub.unsubscribe()),
          );
          for (const result of cleanup) {
            if (result.status === 'rejected') {
              failures.push(result.reason);
            }
          }
        }
        if (failures.length > 0) {
          throw new AggregateError(failures, 'Auction zero-price masking failed');
        }
        if (!observedZero) {
          t.skip('No comparable zero auction-price sentinel observed within the 10s live window');
        }
      }));

    it('optionally validates an environment-supplied preferred route', async (t) =>
      runCase(t, 'optional preferred venue routing', async () => {
        const isin = process.env.XBBG_LIVE_PFD_ISIN;
        if (isin === undefined || isin.trim().length === 0) {
          t.skip('Set XBBG_LIVE_PFD_ISIN to test preferred venue routing');
          return;
        }
        let table: Table;
        try {
          table = (await withTimeout(
            engine!.resolveVenues([isin]),
            15_000,
            'preferred venue',
          )) as Table;
        } catch {
          // Native errors may contain the private input; do not retain their message or cause.
          throw new Error('Environment-supplied preferred venue request failed');
        }
        // Do not print the environment-supplied security or venue.
        assert.ok(stringCell(table, 'kind', 0) === 'pfd', 'Expected a preferred security');
        assert.ok(stringCell(table, 'status', 0) === 'resolved', 'Preferred venue must validate');
        assert.ok(stringCell(table, 'venue_topic', 0) !== null, 'Expected a venue topic');
      }));
  });
});
