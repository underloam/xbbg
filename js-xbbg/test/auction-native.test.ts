import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import type * as Core from '../src/index';
import type { NativeAddon } from '../src/napi';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');

// Pure native helpers only: no engine/session is constructed. Native-free installs
// Still run the mocked subscription/preflight suite; js-build enables this parity suite.
describe.skipIf(!existsSync(nativePath))('native auction definitions', () => {
  let api: typeof Core;
  let native: NativeAddon;

  beforeAll(async () => {
    native = createRequire(__filename)(nativePath) as NativeAddon;
    // Delay package loading: the native addon may not exist in native-free test installs.
    api = await import('../src/index.js');
  });

  test('exports immutable native field groups with the default auction projection', () => {
    expect(native.extAuctionFieldGroupNames()).toStrictEqual([
      'imbalance',
      'indicative',
      'state',
      'halts',
      'results',
      'composite',
      'quotes',
      'default',
    ]);
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
