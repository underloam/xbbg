import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import type * as Core from '../src/index';
import type { NativeAddon } from '../src/napi';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');

// Pure native helpers only: no engine/session is constructed.
// Installs without the addon skip this suite and still run the mocked preflight suite.
describe.skipIf(!existsSync(nativePath))('native auction definitions', () => {
  let api: typeof Core;
  let native: NativeAddon;

  beforeAll(async () => {
    // Not a static import: the package loads the addon on import and throws without one.
    // Import the package first: on Windows it adds Bloomberg's DLL directory to the search path.
    api = await import('../src/index.js');
    native = createRequire(__filename)(nativePath) as NativeAddon;
  });

  test('exports immutable native field groups with the default auction projection', () => {
    expect(Object.isFrozen(api.AuctionFields)).toBeTruthy();
    const { zeroPriceFields, ...groups } = api.AuctionFields;
    for (const [name, fields] of Object.entries(groups)) {
      expect(fields).toStrictEqual(native.extAuctionFieldGroup(name));
      expect(Object.isFrozen(fields)).toBeTruthy();
    }
    expect(api.AuctionFields.default).toStrictEqual([
      ...api.AuctionFields.imbalance,
      ...api.AuctionFields.indicative,
      ...api.AuctionFields.state,
      ...api.AuctionFields.halts,
      ...api.AuctionFields.results,
    ]);
    expect(api.AuctionFields.default).toHaveLength(39);
    expect(api.AuctionFields.quotes).toStrictEqual(['BID', 'ASK', 'BID_SIZE', 'ASK_SIZE']);
    expect(native.extAuctionFieldGroup('unknown')).toBeNull();
    expect(zeroPriceFields).toStrictEqual(native.extAuctionZeroPriceFields());
    expect(zeroPriceFields).toStrictEqual([
      'THEO_PRICE',
      'INDICATIVE_NEAR',
      'INDICATIVE_FAR',
      'IMBALANCE_BUY',
      'IMBALANCE_SELL',
      'REFERENCE_PRICE_RT',
    ]);
    expect(Object.isFrozen(zeroPriceFields)).toBeTruthy();
    expect(native.extAuctionFieldGroup('zeroPriceFields')).toBeNull();
  });

  test.each([
    ['BUY', 'buy'],
    [' mbuy ', 'buy'],
    ['RBUY', 'buy'],
    ['SELL', 'sell'],
    [' msel ', 'sell'],
    ['RSEL', 'sell'],
    ['NOIM', 'none'],
    ['nimb', 'none'],
    ['INOR', null],
    ['NODS', null],
    ['', null],
    ['N.A.', null],
    ['unknown', null],
  ] as const)('interprets %j consistently across native and TypeScript', (code, expected) => {
    expect(native.extImbalanceSide(code)).toBe(expected);
    expect(api.imbalanceSide(code)).toBe(expected);
  });
});
