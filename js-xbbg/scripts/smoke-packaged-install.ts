#!/usr/bin/env node

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { resolveVendorSdkRoot } from '../src/runtime-search-path';
import { run, runNpm } from './npm-process';
import { nativePackageSpecForKey, platformKey } from './platform-map';

const repoRoot = path.resolve(__dirname, '..', '..');
const packedMode = process.argv.includes('--packed');
const bunMode = process.argv.includes('--bun');

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
    const sdkRoot = resolveVendorSdkRoot(repoRoot);
    if (sdkRoot !== null) {
      env.BLPAPI_ROOT = sdkRoot;
    }
  }
  return env;
}

function smokeRequire(appDir: string, runtime: string = process.execPath): void {
  run(runtime, ['-e', LOAD_PACKAGE_SCRIPT], { cwd: appDir, env: smokeRuntimeEnv() });
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
