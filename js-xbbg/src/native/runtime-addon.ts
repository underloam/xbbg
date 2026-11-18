import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import { runtimeBinaryName } from './platform-map';
import { resolveNativeAddon } from './resolve-native';

const nodeRequire = createRequire(__filename);

/** A Bloomberg runtime in the xbbg cache whose library matched its recorded SHA-256. */
export interface CachedRuntime {
  readonly version: string;
  readonly platform: string;
  /** Absolute path of the Bloomberg library. */
  readonly libPath: string;
  readonly libSha256: string;
  readonly source: string;
  readonly installDir: string;
}

/** A cache install that was refused, and why. */
export interface RejectedRuntime {
  readonly installDir: string;
  readonly reason: string;
}

export interface CacheScan {
  /** Verified runtimes, best first. */
  readonly runtimes: readonly CachedRuntime[];
  readonly rejected: readonly RejectedRuntime[];
}

/**
 * The resolver addon (`napi_xbbg_runtime.node`). It never links Bloomberg, so it loads before
 * the engine addon, which cannot even load until the runtime is found.
 */
export interface RuntimeAddon {
  defaultCacheRoot(): string | null;
  scanCache(cacheRoot?: string, platform?: string): CacheScan;
  preloadLibrary(path: string): void;
}

function isRuntimeAddon(value: unknown): value is RuntimeAddon {
  return (
    typeof value === 'object' &&
    value !== null &&
    'defaultCacheRoot' in value &&
    typeof value.defaultCacheRoot === 'function' &&
    'scanCache' in value &&
    typeof value.scanCache === 'function' &&
    'preloadLibrary' in value &&
    typeof value.preloadLibrary === 'function'
  );
}

/**
 * Load the resolver addon: a local `napi_xbbg_runtime.node` beside `baseDir` (the directory of
 * the entry module) first, as for the engine addon, then the one shipped in the platform
 * package. Returns null when there is none or it cannot be loaded; the cache rule is then
 * skipped.
 */
export function loadRuntimeAddon(baseDir: string): RuntimeAddon | null {
  try {
    const local = [
      path.join(baseDir, runtimeBinaryName),
      path.join(baseDir, '..', runtimeBinaryName),
    ].find((candidate) => fs.existsSync(candidate));
    const binaryPath =
      local ?? resolveNativeAddon(path.resolve(baseDir, '..', '..')).runtimeBinaryPath;
    if (binaryPath === null) {
      return null;
    }
    const loaded: unknown = nodeRequire(binaryPath);
    return isRuntimeAddon(loaded) ? loaded : null;
  } catch {
    return null;
  }
}
