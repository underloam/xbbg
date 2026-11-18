import path from 'node:path';

import type { CacheScan, RuntimeAddon } from '../src/native/runtime-addon';
import {
  applyCacheRule,
  configureRuntimeSearchPath,
  loadWithCacheFallback,
} from '../src/runtime-search-path';

interface MockAddon extends RuntimeAddon {
  readonly preloaded: string[];
}

function runtime(installDir: string, libName = 'lib.bin'): CacheScan['runtimes'][number] {
  return {
    installDir,
    libPath: path.join(installDir, 'lib', libName),
    libSha256: 'a'.repeat(64),
    platform: 'test-platform',
    source: 'artifact:fixture',
    version: '1.2.3.4',
  };
}

function mockAddon(
  scan: CacheScan | Error,
  failPreload: ReadonlySet<string> = new Set(),
): MockAddon {
  const preloaded: string[] = [];
  return {
    defaultCacheRoot: () => null,
    preloadLibrary(libPath: string) {
      if (failPreload.has(libPath)) {
        throw new Error(`dlopen failed for ${libPath}`);
      }
      preloaded.push(libPath);
    },
    preloaded,
    scanCache() {
      if (scan instanceof Error) {
        throw scan;
      }
      return scan;
    },
  };
}

const LOADER_ERROR = 'dlopen: libblpapi3_64 is not found';

// A load that fails until a library has been preloaded, like an engine addon without Bloomberg.
function engineLoad(addon: MockAddon): () => string {
  return () => {
    if (addon.preloaded.length === 0) {
      throw new Error(LOADER_ERROR);
    }
    return 'engine';
  };
}

describe('xbbg cache rule', () => {
  it('is not consulted when the engine loads without it', () => {
    let addonLoads = 0;
    const result = loadWithCacheFallback(() => 'engine', {
      loadRuntimeAddon: () => {
        addonLoads += 1;
        return null;
      },
    });

    expect(result).toBe('engine');
    expect(addonLoads).toBe(0);
  });

  it('keeps every earlier rule ahead of the cache on Windows', () => {
    const dapi = String.raw`C:\blp\DAPI`;
    const env: NodeJS.ProcessEnv = { PATH: String.raw`C:\existing`, SystemDrive: 'C:' };
    const selected = configureRuntimeSearchPath({
      env,
      exists: (target) =>
        path.win32.normalize(target).toLowerCase() === String.raw`c:\blp\dapi\blpapi3_64.dll`,
      platform: 'win32',
      readDir: () => [],
      repoRoot: String.raw`C:\repo`,
    });
    let addonLoads = 0;
    // The engine finds the runtime through the PATH entry the existing rules added.
    const result = loadWithCacheFallback(
      () => {
        if (env.PATH?.split(';')[0] !== dapi) {
          throw new Error(LOADER_ERROR);
        }
        return 'engine';
      },
      {
        env,
        loadRuntimeAddon: () => {
          addonLoads += 1;
          return mockAddon({ rejected: [], runtimes: [runtime('/cache/a')] });
        },
        platform: 'win32',
      },
    );

    expect(selected).toBe(dapi);
    expect(result).toBe('engine');
    expect(addonLoads).toBe(0);
  });

  it('preloads the best verified runtime and loads the engine again', () => {
    const addon = mockAddon({
      rejected: [{ installDir: '/cache/bad', reason: 'hash mismatch' }],
      runtimes: [runtime('/cache/best'), runtime('/cache/older')],
    });

    const result = loadWithCacheFallback(engineLoad(addon), { loadRuntimeAddon: () => addon });

    expect(result).toBe('engine');
    expect(addon.preloaded).toStrictEqual([path.join('/cache/best', 'lib', 'lib.bin')]);
  });

  it('falls through to the next verified runtime when one cannot be loaded', () => {
    const best = runtime('/cache/best');
    const older = runtime('/cache/older');
    const addon = mockAddon({ rejected: [], runtimes: [best, older] }, new Set([best.libPath]));

    const outcome = applyCacheRule({ loadRuntimeAddon: () => addon });

    expect(outcome.libPath).toBe(older.libPath);
    expect(outcome.problems).toStrictEqual([
      `cached Bloomberg runtime /cache/best could not be loaded: dlopen failed for ${best.libPath}`,
    ]);
  });

  it('names the rejection when the cache only holds refused runtimes', () => {
    const addon = mockAddon({
      rejected: [
        {
          installDir: '/cache/sdk/1.2.3.4/test-platform/artifact-abc',
          reason: 'library `lib/lib.bin` does not match its recorded hash (hash mismatch)',
        },
      ],
      runtimes: [],
    });

    let thrown: unknown;
    try {
      loadWithCacheFallback(engineLoad(addon), { loadRuntimeAddon: () => addon });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(Error);
    const { cause, message } = thrown as Error;
    expect(message).toContain(
      'cached Bloomberg runtime /cache/sdk/1.2.3.4/test-platform/artifact-abc rejected: library `lib/lib.bin` does not match its recorded hash (hash mismatch)',
    );
    expect(message).toContain(LOADER_ERROR);
    expect((cause as Error).message).toBe(LOADER_ERROR);
    expect(addon.preloaded).toStrictEqual([]);
  });

  it('rethrows the loader error when the cache holds nothing', () => {
    const addon = mockAddon({ rejected: [], runtimes: [] });

    expect(() =>
      loadWithCacheFallback(engineLoad(addon), { loadRuntimeAddon: () => addon }),
    ).toThrow(new Error(LOADER_ERROR));
  });

  it('skips the rule silently without a resolver addon or when scanning fails', () => {
    expect(() =>
      loadWithCacheFallback(engineLoad(mockAddon({ rejected: [], runtimes: [] })), {
        loadRuntimeAddon: () => null,
      }),
    ).toThrow(new Error(LOADER_ERROR));

    const failing = mockAddon(new Error('cannot scan the xbbg cache'));
    expect(() =>
      loadWithCacheFallback(engineLoad(failing), { loadRuntimeAddon: () => failing }),
    ).toThrow(new Error(LOADER_ERROR));
  });

  it('prepends the library directory to PATH on Windows only', () => {
    const libDir = String.raw`C:\cache\sdk\1.2.3.4\windows-x86_64\artifact-abc\lib`;
    const libPath = `${libDir}\\blpapi3_64.dll`;
    const scan: CacheScan = {
      rejected: [],
      runtimes: [{ ...runtime('/unused'), libPath }],
    };

    const windowsEnv: NodeJS.ProcessEnv = { PATH: String.raw`C:\existing` };
    const windowsAddon = mockAddon(scan);
    expect(
      applyCacheRule({ env: windowsEnv, loadRuntimeAddon: () => windowsAddon, platform: 'win32' })
        .libPath,
    ).toBe(libPath);
    expect(windowsEnv.PATH).toBe(`${libDir};C:\\existing`);
    expect(windowsAddon.preloaded).toStrictEqual([libPath]);

    const unixEnv: NodeJS.ProcessEnv = { PATH: '/usr/bin' };
    const unixAddon = mockAddon(scan);
    applyCacheRule({ env: unixEnv, loadRuntimeAddon: () => unixAddon, platform: 'linux' });
    expect(unixEnv.PATH).toBe('/usr/bin');
    expect(unixAddon.preloaded).toStrictEqual([libPath]);
  });
});
