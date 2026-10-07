#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

import { assertNativeBuildArtifact, readNativeBuildInfo } from './native-build-info';
import { runNpm } from './npm-process';
import { nativeBinaryName, nativePackageSpecForKey, platformKey } from './platform-map';
import type { NativePackageSpec } from './platform-map';

const repoRoot = path.resolve(__dirname, '..', '..');
const packageDir = path.resolve(repoRoot, 'js-xbbg');
const sourceBinary = path.join(packageDir, nativeBinaryName);

interface StageOptions {
  build: boolean;
  release: boolean;
  version: string | null;
}

interface StagePackageResult {
  readonly destBinary: string;
  readonly key: string;
  readonly localPackageDir: string;
  readonly packageName: string;
}

function fail(message: string): never {
  throw new Error(message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseArgs(argv: readonly string[]): StageOptions {
  // Release by default: staged packages must not silently carry debug addons.
  const parsed: StageOptions = { build: false, release: true, version: null };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === undefined) {
      fail('encountered missing CLI argument while parsing stage options');
    }
    switch (value) {
      case '--build': {
        parsed.build = true;
        break;
      }
      case '--release': {
        parsed.release = true;
        break;
      }
      case '--debug': {
        parsed.release = false;
        break;
      }
      case '--version': {
        index += 1;
        parsed.version = argv[index] ?? null;
        break;
      }
      default: {
        fail(`unknown argument: ${value}`);
      }
    }
  }
  return parsed;
}

function ensurePlatformLoader(localPackageDir: string): void {
  const loaderPath = path.join(localPackageDir, 'index.js');
  if (fs.existsSync(loaderPath)) {
    return;
  }
  runNpm(['--prefix', packageDir, 'run', 'build:ts'], { cwd: repoRoot });
  if (!fs.existsSync(loaderPath)) {
    fail(`expected generated platform loader at ${loaderPath}; build:ts did not produce it`);
  }
}

function tempPathFor(destPath: string, suffix: string): string {
  return path.join(
    path.dirname(destPath),
    `.${path.basename(destPath)}.${process.pid}.${Date.now()}.${suffix}.tmp`,
  );
}

function copyBinarySafely(
  sourcePath: string,
  destPath: string,
  key: NativePackageSpec['key'],
  release: boolean,
): void {
  const tempPath = tempPathFor(destPath, 'binary');
  try {
    const provenance = readNativeBuildInfo(path.join(packageDir, '.native-build-info.json'));
    fs.copyFileSync(sourcePath, tempPath);
    fs.chmodSync(tempPath, 0o755);
    const stat = fs.statSync(tempPath);
    if (!stat.isFile() || stat.size === 0) {
      fail(`prepared native addon is empty: ${tempPath}`);
    }
    assertNativeBuildArtifact(provenance, tempPath, key, release);
    fs.renameSync(tempPath, destPath);
  } finally {
    fs.rmSync(tempPath, { force: true });
  }
}

function writePackageJsonSafely(
  packageJsonPath: string,
  packageJson: Record<string, unknown>,
): void {
  const tempPath = tempPathFor(packageJsonPath, 'json');
  try {
    const serialized = `${JSON.stringify(packageJson, null, 2)}\n`;
    fs.writeFileSync(tempPath, serialized);
    JSON.parse(fs.readFileSync(tempPath, 'utf8'));
    fs.renameSync(tempPath, packageJsonPath);
  } finally {
    fs.rmSync(tempPath, { force: true });
  }
}

function stagePackage(release: boolean, version: string | null = null): StagePackageResult {
  const key = platformKey();
  const spec: NativePackageSpec | null = nativePackageSpecForKey(key);
  if (spec === null) {
    fail(`unsupported platform for packaged @xbbg/core native addon: ${key}`);
  }

  const localPackageDir = path.join(packageDir, spec.packageDir);
  if (!fs.existsSync(localPackageDir)) {
    fail(`local platform package directory not found: ${localPackageDir}`);
  }
  ensurePlatformLoader(localPackageDir);

  if (!fs.existsSync(sourceBinary)) {
    fail(
      `expected built native addon at ${sourceBinary}; run npm --prefix js-xbbg run build:native first or pass --build`,
    );
  }

  const destBinary = path.join(localPackageDir, spec.binaryName);
  copyBinarySafely(sourceBinary, destBinary, spec.key, release);

  if (version !== null) {
    const normalizedVersion = version.replace(/^v/u, '');
    const packageJsonPath = path.join(localPackageDir, 'package.json');
    const packageJson: unknown = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    if (!isRecord(packageJson)) {
      fail(`expected package.json object at ${packageJsonPath}`);
    }
    writePackageJsonSafely(packageJsonPath, { ...packageJson, version: normalizedVersion });
  }

  console.log(`Staged ${sourceBinary} -> ${destBinary}`);
  return { destBinary, key, localPackageDir, packageName: spec.packageName };
}

function maybeBuild({ build, release }: StageOptions): void {
  if (!build && fs.existsSync(sourceBinary)) {
    return;
  }
  runNpm(
    release
      ? ['--prefix', packageDir, 'run', 'build:native', '--', '--release']
      : ['--prefix', packageDir, 'run', 'build:native', '--', '--debug'],
    { cwd: repoRoot },
  );
}

try {
  const options = parseArgs(process.argv.slice(2));
  maybeBuild(options);
  stagePackage(options.release, options.version);
} catch (error) {
  console.error(`js-xbbg stage failed: ${String(error)}`);
  process.exitCode = 1;
}
