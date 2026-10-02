#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { nativePackageSpecForKey, platformKey } from './platform-map';

interface RunOptions {
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
}

interface NpmInvocation {
  readonly args: string[];
  readonly command: string;
}

const repoRoot = path.resolve(__dirname, '..', '..');
const npmExecPath = process.env.npm_execpath;
const packedMode = process.argv.includes('--packed');
const bunMode = process.argv.includes('--bun');

const LOAD_PACKAGE_SCRIPT =
  "const resolved = require.resolve('@xbbg/core'); if (!resolved.includes('dist')) throw new Error(`expected packaged dist entrypoint, got ${resolved}`); const core = require('@xbbg/core'); console.log(resolved, typeof core.connect, typeof core.version, core.version());";

function fail(message: string): never {
  console.error(`js-xbbg packaged-install smoke failed: ${message}`);
  process.exit(1);
}

function outputText(output: string | Buffer | null | undefined): string {
  return typeof output === 'string' ? output : (output?.toString('utf8') ?? '');
}

function run(command: string, args: readonly string[], options: RunOptions = {}): void {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: 'inherit',
    windowsHide: true,
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function runCapture(command: string, args: readonly string[], options: RunOptions = {}): string {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: 'utf8',
    env: options.env,
    windowsHide: true,
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.stderr.write(outputText(result.stderr));
    process.exit(result.status ?? 1);
  }
  const lines = outputText(result.stdout)
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.at(-1) ?? '';
}

function npmCommand(args: readonly string[]): NpmInvocation {
  if (npmExecPath !== undefined && npmExecPath.length > 0) {
    return { args: [npmExecPath, ...args], command: process.execPath };
  }
  if (process.platform === 'win32') {
    return {
      args: ['/d', '/s', '/c', 'npm.cmd', ...args],
      command: process.env.ComSpec ?? 'cmd.exe',
    };
  }
  return { args: [...args], command: 'npm' };
}

function runNpm(args: readonly string[], options: RunOptions = {}): void {
  const invocation = npmCommand(args);
  run(invocation.command, invocation.args, options);
}

function runNpmCapture(args: readonly string[], options: RunOptions = {}): string {
  const invocation = npmCommand(args);
  return runCapture(invocation.command, invocation.args, options);
}

function resolveVendoredSdkRoot(): string | null {
  const vendorDir = path.join(repoRoot, 'vendor', 'blpapi-sdk');
  if (!fs.existsSync(vendorDir)) {
    return null;
  }
  const candidates = fs
    .readdirSync(vendorDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(vendorDir, entry.name))
    .filter((dir) => fs.existsSync(path.join(dir, 'bin')) || fs.existsSync(path.join(dir, 'lib')))
    .toSorted();
  return candidates.at(-1) ?? null;
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
    const sdkRoot = resolveVendoredSdkRoot();
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
  const coreTarball = runNpmCapture(['pack', jsPackageDir, '--pack-destination', packDir], {
    cwd: repoRoot,
    env: process.env,
  });
  const platformTarball = runNpmCapture(
    ['pack', platformPackageDir, '--pack-destination', packDir],
    { cwd: repoRoot, env: process.env },
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
