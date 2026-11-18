import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';

import type { RuntimeAddon } from '../src/native/runtime-addon';

const addonPath = path.join(__dirname, '..', 'napi_xbbg_runtime.node');

const PLATFORM = 'linux-x86_64';

function sha256(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

// A synthetic install: placeholder bytes, never a real Bloomberg library.
function writeInstall(
  cacheRoot: string,
  version: string,
  libBytes: Buffer,
  recordedBytes = libBytes,
): string {
  const installDir = path.join(cacheRoot, 'sdk', version, PLATFORM, 'artifact-0123456789ab');
  fs.mkdirSync(path.join(installDir, 'lib'), { recursive: true });
  fs.writeFileSync(path.join(installDir, 'lib', 'libblpapi3_64.so'), libBytes);
  fs.writeFileSync(
    path.join(installDir, 'provenance.json'),
    JSON.stringify({
      artifactSha256: '0'.repeat(64),
      artifactUrl: 'file:fixture',
      lib: 'lib/libblpapi3_64.so',
      libSha256: sha256(recordedBytes),
      licenses: [],
      platform: PLATFORM,
      source: 'artifact:fixture',
      version,
    }),
  );
  return installDir;
}

// Resolver addon only: it links no Bloomberg library, so it loads on any machine and offline.
describe.skipIf(!fs.existsSync(addonPath))('native runtime resolver addon', () => {
  let addon: RuntimeAddon;
  let cacheRoot = '';

  beforeAll(() => {
    addon = createRequire(__filename)(addonPath) as RuntimeAddon;
  });

  beforeEach(() => {
    cacheRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-runtime-cache-'));
  });

  afterEach(() => {
    fs.rmSync(cacheRoot, { force: true, recursive: true });
  });

  test('exports the resolver surface', () => {
    expect(Object.keys(addon).toSorted()).toStrictEqual([
      'defaultCacheRoot',
      'preloadLibrary',
      'scanCache',
    ]);
  });

  test('reports the cache root override', () => {
    const previous = process.env.XBBG_CACHE_DIR;
    process.env.XBBG_CACHE_DIR = cacheRoot;
    try {
      expect(addon.defaultCacheRoot()).toBe(cacheRoot);
    } finally {
      if (previous === undefined) {
        delete process.env.XBBG_CACHE_DIR;
      } else {
        process.env.XBBG_CACHE_DIR = previous;
      }
    }
  });

  test('lists verified runtimes best first and refused ones with their reason', () => {
    const good = writeInstall(cacheRoot, '3.26.9.1', Buffer.from('synthetic runtime 1'));
    const newer = writeInstall(cacheRoot, '3.26.10.1', Buffer.from('synthetic runtime 2'));
    const tampered = path.join(cacheRoot, 'sdk', '3.27.0.1', PLATFORM, 'artifact-0123456789ab');
    writeInstall(cacheRoot, '3.27.0.1', Buffer.from('synthetic runtime 3'));
    fs.writeFileSync(path.join(tampered, 'lib', 'libblpapi3_64.so'), Buffer.from('changed bytes'));

    const scan = addon.scanCache(cacheRoot, PLATFORM);

    expect(scan.runtimes.map((entry) => entry.version)).toStrictEqual(['3.26.10.1', '3.26.9.1']);
    expect(scan.runtimes[0]).toStrictEqual({
      installDir: newer,
      libPath: path.join(newer, 'lib', 'libblpapi3_64.so'),
      libSha256: sha256(Buffer.from('synthetic runtime 2')),
      platform: PLATFORM,
      source: 'artifact:fixture',
      version: '3.26.10.1',
    });
    expect(scan.runtimes[1]?.installDir).toBe(good);
    expect(scan.rejected).toHaveLength(1);
    expect(scan.rejected[0]?.installDir).toBe(tampered);
    expect(scan.rejected[0]?.reason).toContain('hash mismatch');
  });

  test('scans a missing cache as empty and rejects an unknown platform', () => {
    expect(addon.scanCache(path.join(cacheRoot, 'absent'), PLATFORM)).toStrictEqual({
      rejected: [],
      runtimes: [],
    });
    expect(() => addon.scanCache(cacheRoot, 'plan9-mips')).toThrow(/invalid platform/u);
  });

  test('throws the operating system error when a library cannot be preloaded', () => {
    const missing = path.join(cacheRoot, 'absent-runtime');
    expect(() => {
      addon.preloadLibrary(missing);
    }).toThrow(`cannot preload ${missing}: `);

    const notALibrary = path.join(cacheRoot, 'fake-runtime');
    fs.writeFileSync(notALibrary, 'synthetic bytes, not an executable image');
    expect(() => {
      addon.preloadLibrary(notALibrary);
    }).toThrow(`cannot preload ${notALibrary}: `);
    expect(() => {
      addon.preloadLibrary('relative/lib');
    }).toThrow(/must be absolute/u);
  });
});
