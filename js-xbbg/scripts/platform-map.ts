import path from 'node:path';

import { nativePackageForKey, platformPackages } from '../src/native/platform-map';
import type { NativePackageDescriptor } from '../src/native/platform-map';

export { nativeBinaryName, platformKey, platformPackages } from '../src/native/platform-map';

export type NativePackageSpec = NativePackageDescriptor & {
  readonly dirName: string;
  readonly expectedFiles: readonly string[];
};

function specForKey(key: string): NativePackageSpec {
  const descriptor = nativePackageForKey(key);
  if (descriptor === null) {
    throw new Error(`missing native package descriptor for ${key}`);
  }
  return Object.freeze({
    ...descriptor,
    dirName: path.basename(descriptor.packageDir),
    expectedFiles: descriptor.files,
  });
}

export const nativePackageSpecs = Object.freeze(
  Object.keys(platformPackages).map((key) => specForKey(key)),
);

export function nativePackageSpecForKey(key: string): NativePackageSpec | null {
  return nativePackageSpecs.find((spec) => spec.key === key) ?? null;
}
