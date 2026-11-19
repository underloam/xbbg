/* oxlint-disable node/no-sync -- native addon resolution runs synchronously on module load. */
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import type { NativeAddon } from '../napi';
import { isPlainObject } from '../objects';
import { resolveNativeAddon } from './resolve-native';
import { loadBloombergRuntime } from './sdk-load';

const nodeRequire = createRequire(__filename);

let cachedAddon: NativeAddon | null = null;
let runtimeLoaded = false;

function isNativeAddon(value: unknown): value is NativeAddon {
  return (
    isPlainObject(value) &&
    typeof value.JsEngine === 'function' &&
    typeof value.extAuctionFieldGroup === 'function' &&
    typeof value.extAuctionZeroPriceFields === 'function' &&
    typeof value.extImbalanceSide === 'function' &&
    typeof value.getLogLevel === 'function' &&
    typeof value.setLogLevel === 'function' &&
    typeof value.loadSdk === 'function' &&
    typeof value.planRequest === 'function'
  );
}

/**
 * Resolve the monorepo root when running out of a source checkout (enables `<root>/vendor/blpapi-sdk`).
 * Works both from `js-xbbg/src/native` (tsx/vitest) and bundled `js-xbbg/dist`.
 */
function checkoutRepoRoot(): string | undefined {
  let current = __dirname;
  for (let depth = 0; depth < 4; depth += 1) {
    if (fs.existsSync(path.join(current, 'vendor', 'blpapi-sdk'))) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }
  return undefined;
}

function packageRoot(): string {
  let current = __dirname;
  for (let depth = 0; depth < 4; depth += 1) {
    if (fs.existsSync(path.join(current, 'package.json'))) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }
  return path.resolve(__dirname, '..');
}

function resolveAddonBinaryPath(): string {
  const pkgRoot = packageRoot();
  const candidates = [
    path.join(pkgRoot, 'napi_xbbg.node'),
    path.join(pkgRoot, 'dist', 'napi_xbbg.node'),
    path.join(__dirname, 'napi_xbbg.node'),
    path.join(__dirname, '..', 'napi_xbbg.node'),
    path.join(__dirname, 'napi-xbbg.node'),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  const { key, packageName, binaryPath } = resolveNativeAddon(path.dirname(pkgRoot));
  if (binaryPath !== null) {
    return binaryPath;
  }
  if (packageName === null) {
    throw new Error(
      `No packaged @xbbg/core native addon is available for ${key}. Build it locally with "npm run build" from js-xbbg.`,
    );
  }
  throw new Error(
    `Unable to load native napi-xbbg module for ${key}. Install ${packageName} via Bun/npm, or build it locally with "npm run build" from js-xbbg.`,
  );
}

/**
 * Load the N-API addon without loading any Bloomberg runtime library.
 *
 * The addon links no Bloomberg code, so this succeeds on clean machines before
 * `provisionSdk` populates the shared xbbg cache.
 */
export function loadNativeAddonWithoutRuntime(): NativeAddon {
  if (cachedAddon !== null) {
    return cachedAddon;
  }
  const modulePath = resolveAddonBinaryPath();
  const loaded: unknown = nodeRequire(modulePath);
  if (!isNativeAddon(loaded)) {
    throw new TypeError(
      `Native addon ${modulePath} does not expose the expected @xbbg/core surface`,
    );
  }
  cachedAddon = loaded;
  return loaded;
}

/**
 * Load the N-API addon and locate/provision/load the Bloomberg runtime into this process.
 */
export function loadNativeAddon(): NativeAddon {
  const addon = loadNativeAddonWithoutRuntime();
  if (!runtimeLoaded) {
    const repoRoot = checkoutRepoRoot();
    loadBloombergRuntime(addon, repoRoot === undefined ? {} : { repoRoot });
    runtimeLoaded = true;
  }
  return addon;
}
