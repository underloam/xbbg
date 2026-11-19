import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';

import type { NativeAddon } from '../src/napi';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');
const sdkRoot = process.env.BLPAPI_ROOT ?? '';

// Every test file runs in its own process, so nothing has loaded a Bloomberg runtime yet.
// The addon is loaded directly, not through the package entry, which would load the runtime itself.
describe.skipIf(!fs.existsSync(nativePath) || sdkRoot.length === 0)(
  'native Bloomberg runtime loading',
  () => {
    let native: NativeAddon;
    let scratch: string;

    beforeAll(() => {
      native = createRequire(__filename)(nativePath) as NativeAddon;
      scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-sdk-native-'));
    });

    afterAll(() => {
      fs.rmSync(scratch, { force: true, recursive: true });
    });

    test('reports nothing loaded, and never reads the function table, before a load', () => {
      expect(native.loadedSdk()).toBeNull();
      expect(native.sdkVersion()).toBeNull();
    });

    test('lists the candidates BLPAPI_ROOT yields without loading anything', () => {
      const located = native.locateSdk({ env: { BLPAPI_ROOT: sdkRoot } });

      expect(located.candidates.length).toBeGreaterThan(0);
      expect(located.candidates[0]).toMatchObject({ detail: 'BLPAPI_ROOT', source: 'blpapi_root' });
      expect(native.sdkVersion()).toBeNull();
    });

    test('refuses an unknown request field', () => {
      expect(() => native.loadSdk({ bogus: true } as never)).toThrow(/bogus/u);
    });

    test('loads from BLPAPI_ROOT in the request, skipping a directory that holds no runtime', () => {
      const emptyDir = path.join(scratch, 'empty');
      fs.mkdirSync(emptyDir);

      const loaded = native.loadSdk({ env: { BLPAPI_LIB_DIR: emptyDir, BLPAPI_ROOT: sdkRoot } });

      expect(loaded.source).toBe('blpapi_root');
      expect(loaded.detail).toBe('BLPAPI_ROOT');
      expect(path.isAbsolute(loaded.path)).toBeTruthy();
      expect(loaded.version).toMatch(/^\d+\.\d+\.\d+\.\d+$/u);
      expect(native.sdkVersion()).toBe(loaded.version);
    });

    test('returns the loaded runtime on every later call, whatever the request', () => {
      const first = native.loadedSdk();
      expect(first).not.toBeNull();
      expect(native.loadSdk()).toStrictEqual(first);
      expect(
        native.loadSdk({ env: { BLPAPI_LIB_DIR: path.join(scratch, 'elsewhere') } }),
      ).toStrictEqual(first);
    });
  },
);
