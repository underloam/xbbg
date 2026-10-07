export const nativeBinaryName = 'napi_xbbg.node';

const nativePackageFiles = Object.freeze([
  'index.js',
  'index.d.ts',
  'README.md',
  'LICENSE',
  'package.json',
  nativeBinaryName,
] as const);

const nativePackageDescriptors = Object.freeze({
  'darwin-arm64': Object.freeze({
    binaryName: nativeBinaryName,
    cpu: 'arm64',
    files: nativePackageFiles,
    key: 'darwin-arm64',
    os: 'darwin',
    packageDir: 'packages/xbbg-core-darwin-arm64',
    packageName: '@xbbg/core-darwin-arm64',
  }),
  'linux-x64': Object.freeze({
    binaryName: nativeBinaryName,
    cpu: 'x64',
    files: nativePackageFiles,
    key: 'linux-x64',
    os: 'linux',
    packageDir: 'packages/xbbg-core-linux-x64',
    packageName: '@xbbg/core-linux-x64',
  }),
  'win32-x64': Object.freeze({
    binaryName: nativeBinaryName,
    cpu: 'x64',
    files: nativePackageFiles,
    key: 'win32-x64',
    os: 'win32',
    packageDir: 'packages/xbbg-core-win32-x64',
    packageName: '@xbbg/core-win32-x64',
  }),
} as const);

export type PlatformKey = keyof typeof nativePackageDescriptors;
export type NativePackageDescriptor = (typeof nativePackageDescriptors)[PlatformKey];

export const platformPackages = Object.freeze({
  'darwin-arm64': nativePackageDescriptors['darwin-arm64'].packageName,
  'linux-x64': nativePackageDescriptors['linux-x64'].packageName,
  'win32-x64': nativePackageDescriptors['win32-x64'].packageName,
}) satisfies Readonly<{ [K in PlatformKey]: (typeof nativePackageDescriptors)[K]['packageName'] }>;

export function nativePackageForKey(key: string): NativePackageDescriptor | null {
  return (
    (nativePackageDescriptors as Readonly<Record<string, NativePackageDescriptor | undefined>>)[
      key
    ] ?? null
  );
}

export function platformKey(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): string {
  return `${platform}-${arch}`;
}
