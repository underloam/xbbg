import type { Table } from 'apache-arrow';

import { DataType, tableFromIPC, tableToIPC } from 'apache-arrow';
import { expectTypeOf } from 'vitest';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import {
  ipcToPolars,
  nativeArrowToBackend,
  nativeArrowToPolars,
  toArrowTableFromNative,
} from '../src/backends';
import type { BloombergFieldError, ResultMetadata } from '../src/index';
import type { NativeArrowZeroCopyBatch } from '../src/napi';
import { emptyFieldInfoArrowBatch, int32ArrowBatch } from './subscription-fixtures';

// Metadata decoding must also work in wrapper-only installations without an addon.
// Only the optional Polars peer is replaced; Arrow and JSON conversion are real.
vi.mock(import('node:module'), async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    createRequire: (...args: Parameters<typeof actual.createRequire>) => {
      const nodeRequire = actual.createRequire(...args);
      return new Proxy(nodeRequire, {
        apply(target, thisArgument, argumentsList) {
          if (argumentsList[0] === 'nodejs-polars') {
            return {
              readIPC: (buffer: Buffer) => {
                if (
                  buffer.subarray(0, 6).toString('ascii') !== 'ARROW1' ||
                  buffer.subarray(-6).toString('ascii') !== 'ARROW1'
                ) {
                  throw new TypeError('Polars peer requires Arrow file IPC');
                }
                const table = tableFromIPC(buffer);
                const names = table.schema.fields.map((field) => field.name);
                const frame = Object.fromEntries(names.map((name) => [name, table.getChild(name)]));
                Object.defineProperties(frame, {
                  ipcMetadata: {
                    configurable: true,
                    value: Object.fromEntries(table.schema.metadata),
                  },
                  ipcTable: { configurable: true, value: table },
                });
                return new Proxy(frame, { ownKeys: () => names });
              },
            };
          }
          return Reflect.apply(target, thisArgument, argumentsList);
        },
      });
    },
  };
});

const FIRST_EXCEPTION = {
  field: 'OPEN_INT',
  category: 'BAD_FLD',
  code: 9,
  subcategory: 'NOT_APPLICABLE_TO_REF_DATA',
  message: 'Field not applicable to security',
};
const SECOND_EXCEPTION = {
  ...FIRST_EXCEPTION,
  field: 'FUND_EXPENSE_RATIO',
};
const FIELD_ERROR: BloombergFieldError = {
  category: 'BAD_FLD',
  code: -103,
  subcategory: 'INVALID_FIELD',
  message: 'Unknown Field Id/Mnemonic',
  source: 'synthetic-apiflds',
};

const arrowConversions = [
  {
    name: 'native Arrow backend',
    convert: (batch: NativeArrowZeroCopyBatch) => nativeArrowToBackend(batch, 'arrow'),
  },
  {
    name: 'native table transfer',
    convert: (batch: NativeArrowZeroCopyBatch) => toArrowTableFromNative(batch),
  },
];
const jsonConversions = [
  {
    name: 'native JSON backend',
    convert: (batch: NativeArrowZeroCopyBatch) => nativeArrowToBackend(batch, 'json'),
  },
];
const polarsConversions = [
  {
    name: 'native Polars transfer',
    convert: (batch: NativeArrowZeroCopyBatch) => nativeArrowToPolars(batch),
  },
  {
    name: 'file IPC Polars backend',
    convert: (batch: NativeArrowZeroCopyBatch) =>
      ipcToPolars(Buffer.from(tableToIPC(tableFromNativeArrowBatch(batch), 'file'))),
  },
  {
    name: 'stream IPC Polars backend',
    convert: (batch: NativeArrowZeroCopyBatch) =>
      ipcToPolars(Buffer.from(tableToIPC(tableFromNativeArrowBatch(batch), 'stream'))),
  },
];
const conversions = [...arrowConversions, ...jsonConversions, ...polarsConversions];

function expectDiagnosticTypeError(action: () => unknown, context: RegExp): void {
  expect(action).toThrow(TypeError);
  expect(action).toThrow(context);
}

function compactExceptions(indices: unknown = [1, 0, 1]): string {
  return JSON.stringify({
    version: 1,
    entries: [FIRST_EXCEPTION, SECOND_EXCEPTION],
    securities: Object.fromEntries([
      ['__proto__', indices],
      ['constructor', [0]],
      ['SPY US Equity', [1]],
    ]),
  });
}

describe.each(conversions)('result metadata through $name', ({ convert }) => {
  it('expands compact exceptions losslessly and preserves field refusals and raw metadata', () => {
    const fieldErrors = Object.fromEntries([
      ['__proto__', FIELD_ERROR],
      ['constructor', { ...FIELD_ERROR, source: undefined }],
    ]);
    const metadata = {
      'xbbg.field_exceptions': compactExceptions(),
      'xbbg.field_errors': JSON.stringify(fieldErrors),
      'synthetic.metadata': 'retained',
    };
    const result = convert(int32ArrowBatch(42, metadata)) as ResultMetadata;

    expect(result.metadata).toStrictEqual(metadata);
    expect(new Map(Object.entries(result.fieldExceptions ?? {}))).toStrictEqual(
      new Map([
        ['__proto__', [SECOND_EXCEPTION, FIRST_EXCEPTION, SECOND_EXCEPTION]],
        ['constructor', [FIRST_EXCEPTION]],
        ['SPY US Equity', [SECOND_EXCEPTION]],
      ]),
    );
    expect(new Map(Object.entries(result.fieldErrors ?? {}))).toStrictEqual(
      new Map([
        ['__proto__', FIELD_ERROR],
        [
          'constructor',
          {
            category: FIELD_ERROR.category,
            code: FIELD_ERROR.code,
            subcategory: FIELD_ERROR.subcategory,
            message: FIELD_ERROR.message,
          },
        ],
      ]),
    );
    expect(Object.getPrototypeOf(result.fieldExceptions)).toBe(Object.prototype);
    expect(Object.hasOwn(result.fieldExceptions ?? {}, '__proto__')).toBeTruthy();
    expect(Object.hasOwn(result.fieldErrors ?? {}, '__proto__')).toBeTruthy();
    const prototypeKey = '__proto__';
    expect(result.fieldExceptions?.[prototypeKey]?.[0]).not.toBe(
      result.fieldExceptions?.[prototypeKey]?.[2],
    );
    expectTypeOf(result.fieldErrors).toEqualTypeOf<
      Record<string, BloombergFieldError> | undefined
    >();
  });

  it.each(['version', 'entries', 'securities'])(
    'keeps persisted legacy diagnostics with the standalone reserved ticker %s',
    (ticker) => {
      const expanded = Object.fromEntries([
        [ticker, [SECOND_EXCEPTION, FIRST_EXCEPTION]],
        ['__proto__', [SECOND_EXCEPTION]],
      ]);
      const result = convert(
        int32ArrowBatch(42, {
          'xbbg.field_exceptions': JSON.stringify(expanded),
        }),
      ) as ResultMetadata;

      expect(result.fieldExceptions).toStrictEqual(expanded);
      expect(result.fieldErrors).toBeUndefined();
    },
  );

  it('defers compact expansion until explicit access and caches the expanded map', () => {
    const occurrences = 65_537;
    const result = convert(
      int32ArrowBatch(42, {
        'xbbg.field_exceptions': compactExceptions(Array.from({ length: occurrences }, () => 0)),
      }),
    ) as ResultMetadata;
    const descriptor = Object.getOwnPropertyDescriptor(result, 'fieldExceptions');

    expect(descriptor).toHaveProperty('get', expect.any(Function));
    expect(descriptor).not.toHaveProperty('value');
    expect(result.metadata?.['xbbg.field_exceptions']).toContain('"version":1');

    const expanded = result.fieldExceptions;
    const prototypeKey = '__proto__';
    const repeated = expanded?.[prototypeKey];

    expect(repeated).toHaveLength(occurrences);
    expect(repeated?.[0]).toStrictEqual(FIRST_EXCEPTION);
    expect(repeated?.[occurrences - 1]).toStrictEqual(FIRST_EXCEPTION);
    expect(repeated?.[0]).not.toBe(repeated?.[occurrences - 1]);
    expect(result.fieldExceptions).toBe(expanded);
    expect(result.fieldExceptions?.[prototypeKey]).toBe(repeated);
  });

  it('validates the final compact index before any accessor or small-prefix consumer can run', () => {
    const indices = Array.from({ length: 65_536 }, () => 0);
    indices.push(2);
    const metadata = { 'xbbg.field_exceptions': compactExceptions(indices) };

    // Merely converting must fail: this callback never reads fieldExceptions.
    expectDiagnosticTypeError(
      () => convert(int32ArrowBatch(42, metadata)),
      /xbbg\.field_exceptions.*index/iu,
    );
  });

  it('retains refusal metadata on an all-rejected empty native result', () => {
    const metadata = { 'xbbg.field_errors': JSON.stringify({ NOT_A_FIELD: FIELD_ERROR }) };
    const result = convert(emptyFieldInfoArrowBatch(metadata)) as ResultMetadata;

    expect(result.metadata).toStrictEqual(metadata);
    expect(result.fieldErrors).toStrictEqual({ NOT_A_FIELD: FIELD_ERROR });
    expect(result.fieldExceptions).toBeUndefined();
  });

  it.each([
    { name: 'negative', index: -1 },
    { name: 'fractional', index: 0.5 },
    { name: 'out of range', index: 2 },
    { name: 'unsafe integer', index: 9_007_199_254_740_992 },
    { name: 'string', index: '0' },
    { name: 'null', index: null },
    { name: 'boolean', index: false },
  ])('rejects $name compact dictionary indices explicitly', ({ index }) => {
    expectDiagnosticTypeError(
      () =>
        convert(
          int32ArrowBatch(42, {
            'xbbg.field_exceptions': compactExceptions([index]),
          }),
        ),
      /xbbg\.field_exceptions.*index/iu,
    );
  });

  it.each([
    { name: 'future version', value: { version: 2, entries: [], securities: {} } },
    { name: 'array version and securities', value: { version: [], entries: [], securities: [] } },
    {
      name: 'unknown top-level member',
      value: { version: 1, entries: [], securities: {}, extra: [] },
    },
    { name: 'string version', value: { version: '1', entries: [], securities: {} } },
    { name: 'missing version', value: { entries: [], securities: {} } },
    {
      name: 'ambiguous legacy entries and securities ticker pair',
      value: { entries: [FIRST_EXCEPTION], securities: [SECOND_EXCEPTION] },
    },
    { name: 'missing entries', value: { version: 1, securities: {} } },
    { name: 'array securities', value: { version: 1, entries: [], securities: [] } },
    { name: 'non-array indices', value: { version: 1, entries: [], securities: { SYN: 0 } } },
    {
      name: 'incomplete dictionary entry',
      value: { version: 1, entries: [{ field: 'OPEN_INT' }], securities: { SYN: [0] } },
    },
    {
      name: 'unused incomplete dictionary entry',
      value: {
        version: 1,
        entries: [FIRST_EXCEPTION, { field: 'OPEN_INT' }],
        securities: { SYN: [0] },
      },
    },
    {
      name: 'fractional exception code',
      value: {
        version: 1,
        entries: [{ ...FIRST_EXCEPTION, code: 0.5 }],
        securities: { SYN: [0] },
      },
    },
    {
      name: 'out-of-range exception code',
      value: {
        version: 1,
        entries: [{ ...FIRST_EXCEPTION, code: 2_147_483_648 }],
        securities: { SYN: [0] },
      },
    },
  ])('rejects a $name compact envelope instead of dropping diagnostics', ({ value }) => {
    expectDiagnosticTypeError(
      () =>
        convert(
          int32ArrowBatch(42, {
            'xbbg.field_exceptions': JSON.stringify(value),
          }),
        ),
      /xbbg\.field_exceptions/iu,
    );
  });

  it('rejects invalid diagnostic JSON and malformed field errors', () => {
    for (const key of ['xbbg.field_exceptions', 'xbbg.field_errors']) {
      expect(() => convert(int32ArrowBatch(42, { [key]: '{not-json' }))).toThrow(
        expect.objectContaining({
          name: 'TypeError',
          message: expect.stringContaining(key),
        }),
      );
    }
    expect(() =>
      convert(
        int32ArrowBatch(42, {
          'xbbg.field_errors': JSON.stringify({ SYN: { ...FIELD_ERROR, source: 7 } }),
        }),
      ),
    ).toThrow(
      expect.objectContaining({
        name: 'TypeError',
        message: expect.stringMatching(/xbbg\.field_errors/iu),
      }),
    );
  });

  it('preserves the existing omission behavior for unrelated malformed metadata', () => {
    const metadata = {
      'xbbg.eid_data': '{not-json',
      'xbbg.security_errors': '[]',
      'xbbg.delayed': 'null',
      'xbbg.bloomberg_request_ids': '{}',
    };
    const result = convert(int32ArrowBatch(42, metadata)) as ResultMetadata;

    expect(result.metadata).toStrictEqual(metadata);
    expect(result.eidData).toBeUndefined();
    expect(result.securityErrors).toBeUndefined();
    expect(result.delayed).toBeUndefined();
    expect(result.bloombergRequestIds).toBeUndefined();
    expect(result.fieldExceptions).toBeUndefined();
    expect(result.fieldErrors).toBeUndefined();
  });
});

describe.each(polarsConversions)('polars peer payload through $name', ({ convert }) => {
  it('passes file IPC with preserved payload and schema metadata to the peer', () => {
    const metadata = {
      'xbbg.field_exceptions': compactExceptions(),
      'synthetic.metadata': 'retained',
    };
    const frame = convert(int32ArrowBatch(42, metadata)) as ResultMetadata & {
      answer: { get(index: number): unknown };
      ipcMetadata: Record<string, string>;
    };

    expect(frame.answer.get(0)).toBe(42);
    expect(frame.ipcMetadata).toStrictEqual(metadata);
    // Like the peer, the double exposes only columns through ownKeys.
    // Metadata descriptors must remain configurable for this Proxy invariant.
    expect(Object.keys(frame)).toStrictEqual(['answer']);
  });

  it('preserves the typed empty FieldInfo schema in file IPC', () => {
    const frame = convert(emptyFieldInfoArrowBatch({})) as { ipcTable: Table };

    expectEmptyFieldInfoSchema(frame.ipcTable);
  });
});

describe.each(arrowConversions)('empty FieldInfo schema through $name', ({ convert }) => {
  it('keeps all typed columns when no fields are accepted', () => {
    expectEmptyFieldInfoSchema(convert(emptyFieldInfoArrowBatch({})) as Table);
  });
});

describe.each(jsonConversions)('empty FieldInfo rows through $name', ({ convert }) => {
  it('has no fabricated success rows', () => {
    expect(convert(emptyFieldInfoArrowBatch({}))).toHaveLength(0);
  });
});

function expectEmptyFieldInfoSchema(table: Table): void {
  expect(table.numRows).toBe(0);
  expect(
    table.schema.fields.map((field) => [field.name, DataType.isUtf8(field.type), field.nullable]),
  ).toStrictEqual([
    ['field', true, false],
    ['type', true, false],
    ['description', true, true],
    ['category', true, true],
    ['id', true, false],
  ]);
}

describe('native Arrow schema metadata', () => {
  it('retains compact diagnostics verbatim in the native Arrow schema', () => {
    const metadata = { 'xbbg.field_exceptions': compactExceptions() };
    const table = toArrowTableFromNative(int32ArrowBatch(42, metadata));

    expect(table.schema.metadata.get('xbbg.field_exceptions')).toBe(
      metadata['xbbg.field_exceptions'],
    );
    expect(table.getChild('answer')?.get(0)).toBe(42);
  });
});
