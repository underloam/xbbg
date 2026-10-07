#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

import { runNpm } from './npm-process';
import { nativeBinaryName, nativePackageSpecs } from './platform-map';
import type { NativePackageSpec } from './platform-map';

const repoRoot = path.resolve(__dirname, '..', '..');
const packageDir = path.resolve(repoRoot, 'js-xbbg');

type ValidationMode = 'attw' | 'dry-run' | 'publint';

interface BasePackageSpec {
  readonly dir: string;
  readonly expectedFiles: readonly string[];
  readonly forbiddenFiles?: readonly string[];
  readonly name: string;
}

type NativeValidationSpec = BasePackageSpec & {
  readonly native: NativePackageSpec;
};

type PackageSpec = BasePackageSpec | NativeValidationSpec;

interface PackFile {
  readonly path?: unknown;
}

interface PackResult {
  readonly files?: unknown;
}

interface PackageManifest {
  readonly cpu?: readonly string[];
  readonly files?: readonly string[];
  readonly name: string;
  readonly os?: readonly string[];
  readonly version: string;
}

interface AttwProblem {
  readonly entrypoint?: string;
  readonly kind: string;
  readonly resolutionKind?: string;
}

interface ValidateOptions {
  readonly allPlatforms: boolean;
  readonly mode: ValidationMode;
}

function fail(message: string): never {
  console.error(`js-xbbg package validation failed: ${message}`);
  process.exit(1);
}

function parseArgs(argv: readonly string[]): ValidateOptions {
  let mode: ValidationMode | null = null;
  let allPlatforms = false;

  for (const arg of argv) {
    switch (arg) {
      case '--all-platforms': {
        allPlatforms = true;
        break;
      }
      case '--attw': {
        mode = setMode(mode, 'attw');
        break;
      }
      case '--dry-run': {
        mode = setMode(mode, 'dry-run');
        break;
      }
      case '--publint': {
        mode = setMode(mode, 'publint');
        break;
      }
      default: {
        fail(`unknown argument: ${arg}`);
      }
    }
  }

  if (mode === null) {
    fail('usage: validate-packages.ts (--dry-run | --publint | --attw) [--all-platforms]');
  }

  return { allPlatforms, mode };
}

function setMode(current: ValidationMode | null, next: ValidationMode): ValidationMode {
  if (current !== null && current !== next) {
    fail('choose exactly one package validation mode');
  }
  return next;
}

function platformSpec(native: NativePackageSpec): NativeValidationSpec {
  return {
    dir: path.join(packageDir, native.packageDir),
    expectedFiles: native.expectedFiles,
    name: native.packageName,
    native,
  };
}

function packageSpecs(allPlatforms: boolean): PackageSpec[] {
  const specs: PackageSpec[] = [
    {
      dir: packageDir,
      expectedFiles: ['dist/index.js', 'dist/index.d.ts', 'README.md', 'LICENSE', 'package.json'],
      forbiddenFiles: [nativeBinaryName],
      name: '@xbbg/core',
    },
  ];

  const allPlatformSpecs = nativePackageSpecs.map(platformSpec);
  const selectedPlatformSpecs = allPlatforms
    ? allPlatformSpecs
    : allPlatformSpecs.filter((spec) => fs.existsSync(path.join(spec.dir, spec.native.binaryName)));

  if (selectedPlatformSpecs.length === 0) {
    fail('no staged platform package found; run npm run stage:native-package first');
  }

  specs.push(...selectedPlatformSpecs);
  return specs;
}

function parsePackJson(raw: string, spec: PackageSpec): PackResult {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed) || parsed.length !== 1 || !isRecord(parsed[0])) {
    fail(`${spec.name}: npm pack --json returned an unexpected shape`);
  }
  return parsed[0];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === 'string');
}

function isNativeValidationSpec(spec: PackageSpec): spec is NativeValidationSpec {
  return 'native' in spec;
}

function packFiles(result: PackResult, spec: PackageSpec): Set<string> {
  if (!Array.isArray(result.files)) {
    fail(`${spec.name}: npm pack --json result did not include a files array`);
  }
  const files = new Set<string>();
  for (const file of result.files) {
    if (!isRecord(file)) {
      fail(`${spec.name}: npm pack file entry was not an object`);
    }
    const filePath = (file as PackFile).path;
    if (typeof filePath !== 'string') {
      fail(`${spec.name}: npm pack file entry did not include a string path`);
    }
    files.add(filePath);
  }
  return files;
}

function dryRunFiles(spec: PackageSpec): readonly string[] {
  const raw = runNpm(['pack', spec.dir, '--dry-run', '--json', '--ignore-scripts'], {
    capture: true,
    cwd: repoRoot,
  });
  return Object.freeze([...packFiles(parsePackJson(raw, spec), spec)].toSorted());
}

function validateDryRun(spec: PackageSpec): void {
  const files = dryRunFiles(spec);

  for (const expectedFile of spec.expectedFiles) {
    if (!files.includes(expectedFile)) {
      fail(`${spec.name}: npm pack dry-run omitted ${expectedFile}`);
    }
  }

  for (const forbiddenFile of spec.forbiddenFiles ?? []) {
    if (files.includes(forbiddenFile)) {
      fail(`${spec.name}: npm pack dry-run unexpectedly included ${forbiddenFile}`);
    }
  }

  console.log(`${spec.name}: dry-run package contents OK`);
}

function validatePublint(spec: PackageSpec): void {
  runNpm(['exec', '--', 'publint', 'run', spec.dir, '--pack', 'npm', '--strict'], {
    cwd: packageDir,
  });
}

function readPackageManifest(spec: PackageSpec): PackageManifest {
  const packageJsonPath = path.join(spec.dir, 'package.json');
  const parsed: unknown = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
  if (!isRecord(parsed) || typeof parsed.name !== 'string' || typeof parsed.version !== 'string') {
    fail(`${spec.name}: package.json must contain string name and version fields`);
  }
  const files = parsed.files;
  const os = parsed.os;
  const cpu = parsed.cpu;
  if (
    (files !== undefined && !isStringArray(files)) ||
    (os !== undefined && !isStringArray(os)) ||
    (cpu !== undefined && !isStringArray(cpu))
  ) {
    fail(`${spec.name}: package.json files, os, and cpu must be string arrays when present`);
  }
  return { cpu, files, name: parsed.name, os, version: parsed.version };
}

function assertArrayEqual(
  spec: PackageSpec,
  field: 'cpu' | 'files' | 'os',
  actual: readonly string[] | undefined,
  expected: readonly string[],
): void {
  if (actual === undefined || actual.length !== expected.length) {
    fail(`${spec.name}: package.json ${field} must be ${JSON.stringify(expected)}`);
  }
  for (const [index, expectedValue] of expected.entries()) {
    if (actual[index] !== expectedValue) {
      fail(`${spec.name}: package.json ${field} must be ${JSON.stringify(expected)}`);
    }
  }
}

function validatePackageManifest(spec: PackageSpec): PackageManifest {
  const manifest = readPackageManifest(spec);
  if (manifest.name !== spec.name) {
    fail(`${spec.name}: package.json name is ${manifest.name}`);
  }
  if (isNativeValidationSpec(spec)) {
    assertArrayEqual(spec, 'os', manifest.os, [spec.native.os]);
    assertArrayEqual(spec, 'cpu', manifest.cpu, [spec.native.cpu]);
    assertArrayEqual(spec, 'files', manifest.files, spec.native.files);
  }
  return manifest;
}

function readSourcePackageFileMap(
  spec: PackageSpec,
  files: readonly string[],
  packageName: string,
): Record<string, Uint8Array> {
  const packageFiles: Record<string, Uint8Array> = {};
  for (const file of files) {
    const sourcePath = path.join(spec.dir, ...file.split('/'));
    packageFiles[`/node_modules/${packageName}/${file}`] = fs.readFileSync(sourcePath);
  }
  return packageFiles;
}

function isNode10OnlyProblem(problem: AttwProblem): boolean {
  return problem.resolutionKind === 'node10';
}

function describeAttwProblem(problem: AttwProblem): string {
  const resolution = problem.resolutionKind === undefined ? '' : ` ${problem.resolutionKind}`;
  const entrypoint = problem.entrypoint === undefined ? '' : ` ${problem.entrypoint}`;
  return `${problem.kind}${resolution}${entrypoint}`;
}

async function validateAttw(spec: PackageSpec): Promise<void> {
  const { Package, checkPackage } = await import('@arethetypeswrong/core');
  const manifest = validatePackageManifest(spec);
  const files = dryRunFiles(spec);
  const pkg = new Package(
    readSourcePackageFileMap(spec, files, manifest.name),
    manifest.name,
    manifest.version,
  );
  const analysis = await checkPackage(pkg);
  if (analysis.types === false) {
    fail(`${spec.name}: package does not expose TypeScript types`);
  }
  const failures = analysis.problems.filter((problem) => !isNode10OnlyProblem(problem));
  if (failures.length > 0) {
    fail(
      `${spec.name}: attw found ${failures.length} problem(s): ${failures.map(describeAttwProblem).join(', ')}`,
    );
  }
  console.log(`${spec.name}: attw type resolution OK`);
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  for (const spec of packageSpecs(options.allPlatforms)) {
    validatePackageManifest(spec);
    switch (options.mode) {
      case 'attw': {
        await validateAttw(spec);
        break;
      }
      case 'dry-run': {
        validateDryRun(spec);
        break;
      }
      case 'publint': {
        validatePublint(spec);
        break;
      }
    }
  }
}

main().catch((error: unknown) => {
  fail(error instanceof Error ? error.message : String(error));
});
