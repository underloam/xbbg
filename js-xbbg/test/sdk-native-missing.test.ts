import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';

import type { NativeAddon } from '../src/napi';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');

// Runs in its own process, where no runtime was ever loaded, so a failed load leaves it unloaded.
describe.skipIf(!fs.existsSync(nativePath))(
  'native load without a usable Bloomberg runtime',
  () => {
    let native: NativeAddon;
    let scratch: string;

    beforeAll(() => {
      native = createRequire(__filename)(nativePath) as NativeAddon;
      scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-sdk-missing-'));
    });

    afterAll(() => {
      fs.rmSync(scratch, { force: true, recursive: true });
    });

    test('throws an error naming the library it refused, and stays unloaded', () => {
      const libDir = path.join(scratch, 'synthetic-lib');
      fs.mkdirSync(libDir);
      for (const name of [
        'blpapi3_64.dll',
        'libblpapi3_64.so',
        'libblpapi3_64.dylib',
        'libblpapi3.so',
        'libblpapi3.dylib',
      ]) {
        fs.writeFileSync(path.join(libDir, name), 'not a shared library');
      }
      const cacheDir = path.join(scratch, 'cache');
      const homeDir = path.join(scratch, 'home');
      fs.mkdirSync(cacheDir);
      fs.mkdirSync(homeDir);

      let thrown: unknown;
      try {
        native.loadSdk({
          env: {
            BLPAPI_LIB_DIR: libDir,
            HOME: homeDir,
            USERPROFILE: homeDir,
            XBBG_CACHE_DIR: cacheDir,
          },
        });
      } catch (error) {
        thrown = error;
      }

      expect(thrown).toBeInstanceOf(Error);
      const { message } = thrown as Error;
      expect(message).toContain(path.basename(scratch));
      expect(message).toContain('BLPAPI_LIB_DIR');
      expect(native.loadedSdk()).toBeNull();
      expect(native.sdkVersion()).toBeNull();
    });
  },
);
