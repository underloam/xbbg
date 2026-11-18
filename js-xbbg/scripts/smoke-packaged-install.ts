#!/usr/bin/env node

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { run, runNpm } from './npm-process';
import { nativePackageSpecForKey, platformKey } from './platform-map';

const repoRoot = path.resolve(__dirname, '..', '..');
const packedMode = process.argv.includes('--packed');
const bunMode = process.argv.includes('--bun');

// Without a loadable runtime the import throws, and the message lists the library it refused.
const MISSING_RUNTIME_SCRIPT =
  "let message; try { require('@xbbg/core'); } catch (error) { message = String(error instanceof Error ? error.message : error); } if (message === undefined) throw new Error('expected the import to fail without a Bloomberg runtime'); if (!message.includes(process.env.BLPAPI_LIB_DIR)) throw new Error(`the error does not list the rejected runtime: ${message}`); console.log('missing-runtime error ok');";

const LOAD_PACKAGE_SCRIPT =
  "const resolved = require.resolve('@xbbg/core'); if (!resolved.includes('dist')) throw new Error(`expected packaged dist entrypoint, got ${resolved}`); const core = require('@xbbg/core'); console.log(resolved, typeof core.connect, typeof core.version, core.version());";

function fail(message: string): never {
  console.error(`js-xbbg packaged-install smoke failed: ${message}`);
  process.exit(1);
}

function lastOutputLine(output: string): string {
  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.at(-1) ?? '';
}

function newestVendoredSdkRoot(): string | null {
  const vendorDir = path.join(repoRoot, 'vendor', 'blpapi-sdk');
  if (!fs.existsSync(vendorDir)) {
    return null;
  }
  const versions = fs
    .readdirSync(vendorDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && /^\d+(?:\.\d+)*$/u.test(entry.name))
    .map((entry) => entry.name)
    .toSorted((left, right) => right.localeCompare(left, 'en', { numeric: true }));
  const newest = versions[0];
  return newest === undefined ? null : path.join(vendorDir, newest);
}

function smokeRuntimeEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  if (
    env.BLPAPI_ROOT !== undefined &&
    env.BLPAPI_ROOT.length > 0 &&
    !path.isAbsolute(env.BLPAPI_ROOT)
  ) {
    env.BLPAPI_ROOT = path.resolve(repoRoot, env.BLPAPI_ROOT);
  }
  if (
    env.XBBG_DEV_SDK_ROOT !== undefined &&
    env.XBBG_DEV_SDK_ROOT.length > 0 &&
    !path.isAbsolute(env.XBBG_DEV_SDK_ROOT)
  ) {
    env.XBBG_DEV_SDK_ROOT = path.resolve(repoRoot, env.XBBG_DEV_SDK_ROOT);
  }
  if (
    (env.BLPAPI_ROOT === undefined || env.BLPAPI_ROOT.length === 0) &&
    (env.XBBG_DEV_SDK_ROOT === undefined || env.XBBG_DEV_SDK_ROOT.length === 0)
  ) {
    const sdkRoot = newestVendoredSdkRoot();
    if (sdkRoot !== null) {
      env.BLPAPI_ROOT = sdkRoot;
    }
  }
  return env;
}

// A packaged install with no usable Bloomberg runtime must fail at import.
// The error names what was tried: `BLPAPI_LIB_DIR` points at synthetic library files.
// The locator finds a candidate, fails to open it, and has a rejection to list.
function smokeMissingRuntime(appDir: string, runtime: string): void {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-no-runtime-'));
  try {
    const libDir = path.join(scratch, 'not-a-runtime');
    const cacheDir = path.join(scratch, 'cache');
    const homeDir = path.join(scratch, 'home');
    for (const dir of [libDir, cacheDir, homeDir]) {
      fs.mkdirSync(dir);
    }
    const runtimeNames = [
      'blpapi3_64.dll',
      'libblpapi3_64.so',
      'libblpapi3_64.dylib',
      'libblpapi3.so',
      'libblpapi3.dylib',
    ];
    for (const name of runtimeNames) {
      fs.writeFileSync(path.join(libDir, name), 'not a shared library');
    }
    const withheld = new Set([
      'BLPAPI_ROOT',
      'XBBG_DEV_SDK_ROOT',
      'LD_LIBRARY_PATH',
      'DYLD_LIBRARY_PATH',
      'DYLD_FALLBACK_LIBRARY_PATH',
    ]);
    const env: NodeJS.ProcessEnv = {
      ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !withheld.has(key))),
      BLPAPI_LIB_DIR: libDir,
      HOME: homeDir,
      USERPROFILE: homeDir,
      XBBG_CACHE_DIR: cacheDir,
    };
    // The search also scans PATH on Windows, where CI puts the SDK's runtime directory.
    // Drop every PATH entry that holds a runtime so nothing else can satisfy the import.
    const pathKey = Object.keys(env).find((key) => key.toUpperCase() === 'PATH');
    if (pathKey !== undefined) {
      env[pathKey] = (env[pathKey] ?? '')
        .split(path.delimiter)
        .filter(
          (dir) =>
            dir.length > 0 && !runtimeNames.some((name) => fs.existsSync(path.join(dir, name))),
        )
        .join(path.delimiter);
    }
    run(runtime, ['-e', MISSING_RUNTIME_SCRIPT], { cwd: appDir, env });
  } finally {
    fs.rmSync(scratch, { force: true, recursive: true });
  }
}

function smokeRequire(appDir: string, runtime: string = process.execPath): void {
  run(runtime, ['-e', LOAD_PACKAGE_SCRIPT], { cwd: appDir, env: smokeRuntimeEnv() });
  smokeMissingRuntime(appDir, runtime);
}

function smokeSourceInstall(jsPackageDir: string): void {
  const appDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-source-install-'));
  runNpm(['init', '-y'], { cwd: appDir, env: process.env });
  runNpm(['install', jsPackageDir], { cwd: appDir, env: process.env });
  smokeRequire(appDir);
}

function packTarballs(
  jsPackageDir: string,
  platformPackageDir: string,
): { core: string; platform: string } {
  const packDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-packaged-install-'));
  const coreTarball = lastOutputLine(
    runNpm(['pack', jsPackageDir, '--pack-destination', packDir], {
      capture: true,
      cwd: repoRoot,
      env: process.env,
    }),
  );
  const platformTarball = lastOutputLine(
    runNpm(['pack', platformPackageDir, '--pack-destination', packDir], {
      capture: true,
      cwd: repoRoot,
      env: process.env,
    }),
  );
  return { core: path.join(packDir, coreTarball), platform: path.join(packDir, platformTarball) };
}

function smokePackedInstall(jsPackageDir: string, platformPackageDir: string): void {
  const tarballs = packTarballs(jsPackageDir, platformPackageDir);
  const appDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-packed-install-'));
  runNpm(['init', '-y'], { cwd: appDir, env: process.env });
  runNpm(['install', tarballs.platform, tarballs.core], { cwd: appDir, env: process.env });
  smokeRequire(appDir);
}

// Bun's installer and node_modules layout differ most from npm's; the native loader
// (`src/native/resolve-native.ts`) must still find the platform package Bun installed.
function smokeBunPackedInstall(jsPackageDir: string, platformPackageDir: string): void {
  const tarballs = packTarballs(jsPackageDir, platformPackageDir);
  const appDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-bun-packed-install-'));
  fs.writeFileSync(
    path.join(appDir, 'package.json'),
    `${JSON.stringify({ name: 'xbbg-bun-smoke', private: true }, null, 2)}\n`,
  );
  run('bun', ['add', tarballs.platform, tarballs.core], { cwd: appDir, env: process.env });
  smokeRequire(appDir, 'bun');
}

function main(): void {
  const currentKey = platformKey();
  const currentSpec = nativePackageSpecForKey(currentKey);
  if (currentSpec === null) {
    fail(`unsupported platform for smoke test: ${currentKey}`);
  }

  const jsPackageDir = path.join(repoRoot, 'js-xbbg');
  const platformPackageDir = path.join(jsPackageDir, currentSpec.packageDir);
  const stagedBinary = path.join(platformPackageDir, currentSpec.binaryName);

  if (!fs.existsSync(stagedBinary)) {
    fail(
      `expected staged native package binary at ${stagedBinary}; run stage:native-package first`,
    );
  }

  if (bunMode) {
    smokeBunPackedInstall(jsPackageDir, platformPackageDir);
    return;
  }

  if (packedMode) {
    smokePackedInstall(jsPackageDir, platformPackageDir);
    return;
  }

  smokeSourceInstall(jsPackageDir);
}

main();
