import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import { nativePackageForKey, platformKey } from './platform-map';
import type { NativePackageDescriptor } from './platform-map';

const nodeRequire = createRequire(__filename);

interface NativePackageResolution {
  readonly binaryPath: string;
}

export interface NativeAddonResolution {
  readonly key: string;
  readonly packageName: string | null;
  readonly binaryPath: string | null;
}

interface NativeResolverBaseOptions {
  readonly repoRoot: string;
  readonly requirePackage: (id: string) => unknown;
  readonly exists: (target: string) => boolean;
}

interface SupportedNativeResolverOptions extends NativeResolverBaseOptions {
  readonly nativePackage: NativePackageDescriptor;
}

interface UnsupportedNativeResolverOptions extends NativeResolverBaseOptions {
  readonly key: string;
  readonly nativePackage: null;
}

export type NativeResolverOptions =
  | SupportedNativeResolverOptions
  | UnsupportedNativeResolverOptions;

function exists(target: string): boolean {
  try {
    fs.accessSync(target);
    return true;
  } catch {
    return false;
  }
}

function isResolutionObject(value: unknown): value is NativePackageResolution {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function validateNativePackage(packageName: string, resolved: unknown): NativePackageResolution {
  if (!isResolutionObject(resolved)) {
    throw new Error(`Invalid native package ${packageName}: expected an object with binaryPath`);
  }
  if (!('binaryPath' in resolved)) {
    throw new Error(`Invalid native package ${packageName}: missing binaryPath`);
  }
  if (typeof resolved.binaryPath !== 'string') {
    throw new TypeError(`Invalid native package ${packageName}: binaryPath must be a string`);
  }
  return resolved;
}

function localPackageIndex(repoRoot: string, packageDir: string): string {
  return path.join(repoRoot, packageDir, 'index.js');
}

function isOptionalPackageMissing(err: unknown, packageName: string): boolean {
  if (!(err instanceof Error)) {
    return false;
  }
  const code = 'code' in err ? (err as { readonly code?: unknown }).code : undefined;
  if (code !== 'MODULE_NOT_FOUND') {
    return false;
  }
  return (
    err.message.includes(`Cannot find module '${packageName}'`) ||
    err.message.includes(`Cannot find module "${packageName}"`) ||
    err.message.includes(`Cannot find package '${packageName}'`) ||
    err.message.includes(`Cannot find package "${packageName}"`)
  );
}

function resolveInstalledPackage(
  packageName: string,
  requirePackage: (id: string) => unknown,
  existsFile: (target: string) => boolean,
): NativePackageResolution | null {
  try {
    const resolved = validateNativePackage(packageName, requirePackage(packageName));
    if (!existsFile(resolved.binaryPath)) {
      throw new Error(
        `Invalid native package ${packageName}: binaryPath does not exist: ${resolved.binaryPath}`,
      );
    }
    return resolved;
  } catch (error) {
    if (isOptionalPackageMissing(error, packageName)) {
      return null;
    }
    throw error;
  }
}

function resolveLocalPackage(
  repoRoot: string,
  nativePackage: NativePackageDescriptor,
  requirePackage: (id: string) => unknown,
  existsFile: (target: string) => boolean,
): NativePackageResolution | null {
  const localIndex = localPackageIndex(repoRoot, nativePackage.packageDir);
  if (!existsFile(localIndex)) {
    return null;
  }

  const resolved = validateNativePackage(nativePackage.packageName, requirePackage(localIndex));
  if (!existsFile(resolved.binaryPath)) {
    throw new Error(
      `Invalid native package ${nativePackage.packageName}: binaryPath does not exist: ${resolved.binaryPath}`,
    );
  }
  return resolved;
}

export function resolveNativeAddonCore(options: NativeResolverOptions): NativeAddonResolution {
  const { nativePackage, repoRoot, requirePackage, exists: existsFile } = options;
  if (nativePackage === null) {
    return { binaryPath: null, key: options.key, packageName: null };
  }

  const { key, packageName } = nativePackage;
  const installed = resolveInstalledPackage(packageName, requirePackage, existsFile);
  if (installed !== null) {
    return { binaryPath: installed.binaryPath, key, packageName };
  }

  const local = resolveLocalPackage(repoRoot, nativePackage, requirePackage, existsFile);
  if (local !== null) {
    return { binaryPath: local.binaryPath, key, packageName };
  }

  return { binaryPath: null, key, packageName };
}

export function resolveNativeAddon(repoRoot: string): NativeAddonResolution {
  const key = platformKey();
  const nativePackage = nativePackageForKey(key);
  if (nativePackage === null) {
    return resolveNativeAddonCore({
      exists,
      key,
      nativePackage,
      repoRoot,
      requirePackage: nodeRequire,
    });
  }
  return resolveNativeAddonCore({ exists, nativePackage, repoRoot, requirePackage: nodeRequire });
}
