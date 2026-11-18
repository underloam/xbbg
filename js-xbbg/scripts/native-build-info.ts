import { createHash } from 'node:crypto';
import fs from 'node:fs';

import type { PlatformKey } from '../src/native/platform-map';

export const nativeBuildTargets: Readonly<Record<PlatformKey, string>> = Object.freeze({
  'darwin-arm64': 'aarch64-apple-darwin',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'win32-x64': 'x86_64-pc-windows-msvc',
});

export interface NativeBuildInfo {
  allocator: string;
  artifactSha256: string;
  gitCommit: string;
  optLevel: string;
  portable: boolean;
  profile: 'debug' | 'release';
  rustcVersion: string;
  rustFlags: readonly string[];
  /** The resolver addon built alongside: it never links Bloomberg. */
  runtimeAddon: { file: string; sha256: string };
  schemaVersion: 2;
  sdkLibrary: { file: string; sha256: string };
  sdkVersion: string;
  target: string;
  targetCpu: string;
  targetFeatures: readonly string[];
}

export function sha256File(file: string): string {
  const hash = createHash('sha256');
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const descriptor = fs.openSync(file, 'r');
  try {
    for (;;) {
      const length = fs.readSync(descriptor, buffer, 0, buffer.length, null);
      if (length === 0) {
        break;
      }
      hash.update(length === buffer.length ? buffer : buffer.subarray(0, length));
    }
  } finally {
    fs.closeSync(descriptor);
  }
  return hash.digest('hex');
}

export function inspectRustCodegen(flags: readonly string[]): {
  portable: boolean;
  targetCpu: string;
} {
  let targetCpu = 'default';
  let customFeatures = false;
  for (let index = 0; index < flags.length; index += 1) {
    const flag = flags[index];
    if (flag === undefined) {
      continue;
    }
    let option: string | undefined;
    if (flag === '-C' || flag === '--codegen') {
      index += 1;
      option = flags[index];
    } else if (flag.startsWith('-C')) {
      option = flag.slice(2).trimStart();
    } else if (flag.startsWith('--codegen=')) {
      option = flag.slice('--codegen='.length).trimStart();
    }
    if (option === undefined) {
      continue;
    }
    if (option.startsWith('target-cpu=')) {
      targetCpu = option.slice('target-cpu='.length);
    } else if (option.startsWith('target-feature=') || option.startsWith('llvm-args=')) {
      // Conservative packaging policy: explicit feature/LLVM tuning is a local build.
      customFeatures = true;
    }
  }
  return {
    portable: !customFeatures && ['default', 'generic', 'x86-64'].includes(targetCpu),
    targetCpu,
  };
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isNativeBuildInfo(info: unknown): info is NativeBuildInfo {
  return (
    typeof info === 'object' &&
    info !== null &&
    'schemaVersion' in info &&
    info.schemaVersion === 2 &&
    'profile' in info &&
    (info.profile === 'debug' || info.profile === 'release') &&
    'portable' in info &&
    typeof info.portable === 'boolean' &&
    'rustFlags' in info &&
    Array.isArray(info.rustFlags) &&
    info.rustFlags.every((flag: unknown) => typeof flag === 'string') &&
    'targetFeatures' in info &&
    Array.isArray(info.targetFeatures) &&
    info.targetFeatures.every((feature: unknown) => typeof feature === 'string') &&
    'allocator' in info &&
    isNonEmptyString(info.allocator) &&
    'artifactSha256' in info &&
    isNonEmptyString(info.artifactSha256) &&
    'gitCommit' in info &&
    isNonEmptyString(info.gitCommit) &&
    'optLevel' in info &&
    isNonEmptyString(info.optLevel) &&
    'rustcVersion' in info &&
    isNonEmptyString(info.rustcVersion) &&
    'sdkVersion' in info &&
    isNonEmptyString(info.sdkVersion) &&
    'target' in info &&
    isNonEmptyString(info.target) &&
    'targetCpu' in info &&
    isNonEmptyString(info.targetCpu) &&
    'runtimeAddon' in info &&
    typeof info.runtimeAddon === 'object' &&
    info.runtimeAddon !== null &&
    'file' in info.runtimeAddon &&
    typeof info.runtimeAddon.file === 'string' &&
    'sha256' in info.runtimeAddon &&
    typeof info.runtimeAddon.sha256 === 'string' &&
    'sdkLibrary' in info &&
    typeof info.sdkLibrary === 'object' &&
    info.sdkLibrary !== null &&
    'file' in info.sdkLibrary &&
    typeof info.sdkLibrary.file === 'string' &&
    'sha256' in info.sdkLibrary &&
    typeof info.sdkLibrary.sha256 === 'string'
  );
}

export function readNativeBuildInfo(file: string): NativeBuildInfo {
  const info: unknown = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (typeof info !== 'object' || info === null || Array.isArray(info)) {
    throw new Error('native build provenance must be an object');
  }
  if (!isNativeBuildInfo(info)) {
    throw new Error('native build provenance is incomplete; rebuild the native addon');
  }
  if (
    !/^[a-f0-9]{64}$/u.test(info.artifactSha256) ||
    info.runtimeAddon.file.length === 0 ||
    !/^[a-f0-9]{64}$/u.test(info.runtimeAddon.sha256) ||
    info.sdkLibrary.file.length === 0 ||
    !/^[a-f0-9]{64}$/u.test(info.sdkLibrary.sha256)
  ) {
    throw new Error('native build provenance contains an invalid artifact identity');
  }
  const codegen = inspectRustCodegen(info.rustFlags);
  if (info.targetCpu !== codegen.targetCpu || info.portable !== codegen.portable) {
    throw new Error('native build codegen provenance is inconsistent');
  }
  return info;
}

export function assertNativeBuildArtifact(
  info: NativeBuildInfo,
  preparedFile: string,
  preparedRuntimeFile: string,
  key: PlatformKey,
  release: boolean,
): void {
  if (info.target !== nativeBuildTargets[key]) {
    throw new Error(`native target ${info.target} does not match package ${key}`);
  }
  if (release && (info.profile !== 'release' || !info.portable)) {
    throw new Error(
      'release staging requires a portable release build; use --debug only for local staging',
    );
  }
  if (sha256File(preparedFile) !== info.artifactSha256) {
    throw new Error('native addon does not match its build provenance; rebuild before staging');
  }
  if (sha256File(preparedRuntimeFile) !== info.runtimeAddon.sha256) {
    throw new Error(
      'runtime resolver addon does not match its build provenance; rebuild before staging',
    );
  }
}
