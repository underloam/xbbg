#!/usr/bin/env node
import type { SpawnSyncOptions } from 'node:child_process';

/*
 * Build an offline bundle zip for one @xbbg/core platform.
 *
 * Assumes the repo is checked out, `js-xbbg/` has been stamped (version
 * matches `js-xbbg/packages/xbbg-core-<label>/package.json`), and the platform
 * package has been staged with its prebuilt `napi_xbbg.node`.
 *
 * Output: <out-dir>/xbbg-offline-<label>-<version>.zip containing:
 *   bundle/            preinstalled node_modules tree + smoke test
 *   tarballs/          packed @xbbg/core and @xbbg/core-<label>
 *   README.txt         usage + Bloomberg runtime requirement
 *
 * Requires: node, npm, zip (on PATH). Intended to run on Linux in CI.
 */
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { nativePackageSpecForKey } from './platform-map';

interface OfflineBundleArgs {
  readonly label: string;
  readonly 'out-dir': string;
}

interface VersionedPackageJson {
  readonly version: string;
}

function usageError(): never {
  console.error('Usage: tsx ./scripts/build-offline-bundle.ts --label <platform> --out-dir <dir>');
  process.exit(2);
}

function parseArgs(argv: readonly string[]): OfflineBundleArgs {
  const args: Partial<Record<keyof OfflineBundleArgs, string>> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (key === '--label' || key === '--out-dir') {
      const value = argv[i + 1];
      if (value === undefined) {
        usageError();
      }
      if (key === '--label') {
        args.label = value;
      } else {
        args['out-dir'] = value;
      }
      i += 1;
    }
  }
  if (args.label === undefined || args['out-dir'] === undefined) {
    usageError();
  }
  return { label: args.label, 'out-dir': args['out-dir'] };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readVersionedPackageJson(packageJsonPath: string): VersionedPackageJson {
  const packageJson: unknown = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
  if (!isRecord(packageJson) || typeof packageJson.version !== 'string') {
    throw new Error(`Expected package.json with string version: ${packageJsonPath}`);
  }
  return { version: packageJson.version };
}

function run(cmd: string, cmdArgs: readonly string[], opts: SpawnSyncOptions = {}): void {
  const result = spawnSync(cmd, cmdArgs, { stdio: 'inherit', ...opts });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${cmd} ${cmdArgs.join(' ')} exited with status ${result.status}`);
  }
}

function main(): void {
  const args = parseArgs(process.argv.slice(2));
  const { label } = args;
  const spec = nativePackageSpecForKey(label);
  if (spec === null) {
    throw new Error(`Unsupported platform label: ${label}`);
  }
  const jsPackageDir = path.resolve(__dirname, '..');
  const outDir = path.resolve(args['out-dir']);
  const corePkgDir = path.join(jsPackageDir, spec.packageDir);

  const corePkg = readVersionedPackageJson(path.join(jsPackageDir, 'package.json'));
  const platPkg = readVersionedPackageJson(path.join(corePkgDir, 'package.json'));
  if (corePkg.version !== platPkg.version) {
    throw new Error(
      `Version mismatch: @xbbg/core is ${corePkg.version} but ${spec.packageName} is ${platPkg.version}`,
    );
  }
  const { version } = corePkg;

  fs.mkdirSync(outDir, { recursive: true });
  const work = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-offline-'));
  const tarballs = path.join(work, 'tarballs');
  const bundle = path.join(work, 'bundle');
  fs.mkdirSync(tarballs, { recursive: true });
  fs.mkdirSync(bundle, { recursive: true });

  run('npm', ['pack', jsPackageDir, '--pack-destination', tarballs]);
  run('npm', ['pack', corePkgDir, '--pack-destination', tarballs]);

  const coreTgz = `xbbg-core-${version}.tgz`;
  const platTgz = `${spec.dirName}-${version}.tgz`;
  for (const name of [coreTgz, platTgz]) {
    if (!fs.existsSync(path.join(tarballs, name))) {
      throw new Error(`Expected tarball not produced: ${name}`);
    }
  }

  fs.writeFileSync(
    path.join(bundle, 'package.json'),
    `${JSON.stringify(
      {
        dependencies: {
          '@xbbg/core': `file:../tarballs/${coreTgz}`,
          [spec.packageName]: `file:../tarballs/${platTgz}`,
        },
        name: `xbbg-offline-${label}-bundle`,
        private: true,
        type: 'commonjs',
      },
      null,
      2,
    )}\n`,
  );

  // --force bypasses npm's os/cpu check for @xbbg/core-<label> when
  // Bundling cross-platform (e.g. assembling a win32-x64 bundle on a
  // Linux runner). The bundle is never executed on the install host.
  run(
    'npm',
    [
      'install',
      '--omit=optional',
      '--omit=dev',
      '--omit=peer',
      '--no-audit',
      '--no-fund',
      '--install-strategy=hoisted',
      '--force',
    ],
    { cwd: bundle },
  );

  fs.writeFileSync(
    path.join(bundle, 'test-load.js'),
    [
      'const xbbg = require("@xbbg/core");',
      'const version = typeof xbbg.version === "function" ? xbbg.version() : "unknown";',
      'console.log("@xbbg/core loaded; version:", version);',
      '',
    ].join('\n'),
  );

  const readme = [
    `xbbg offline ${label} bundle (version ${version})`,
    '='.repeat(60),
    '',
    'Contents',
    '--------',
    '- bundle/node_modules            preinstalled Node module tree',
    '- bundle/package.json            bundle manifest',
    '- bundle/test-load.js            minimal load smoke test',
    `- tarballs/${coreTgz}   @xbbg/core JS package`,
    `- tarballs/${platTgz}   prebuilt native N-API addons for ${label} (engine and runtime resolver)`,
    '',
    'Usage',
    '-----',
    'Option A - copy node_modules into your project:',
    '  cp -r bundle/node_modules /path/to/your/project/',
    '',
    'Option B - install tarballs directly:',
    '  cd /path/to/your/project',
    `  npm install <path>/tarballs/${coreTgz} <path>/tarballs/${platTgz}`,
    '',
    'Bloomberg runtime requirement',
    '-----------------------------',
    "This bundle does NOT include Bloomberg's blpapi runtime library.",
    'On the target machine, install Bloomberg Terminal or the blpapi SDK',
    'and ensure blpapi3_64.dll / libblpapi3_64.so / libblpapi3.dylib is',
    'discoverable via one of:',
    '  - BLPAPI_LIB_DIR pointing to the folder containing the runtime',
    '  - BLPAPI_ROOT pointing to the SDK root',
    '  - PATH (Windows) / LD_LIBRARY_PATH (Linux) / DYLD_LIBRARY_PATH (macOS)',
    '  - the xbbg cache (XBBG_CACHE_DIR), checked last, when a verified runtime was',
    '    provisioned into it',
    '',
    'Smoke test',
    '----------',
    '  cd bundle',
    '  node test-load.js',
    '',
  ].join('\n');
  fs.writeFileSync(path.join(work, 'README.txt'), readme);

  const zipName = `xbbg-offline-${label}-${version}.zip`;
  const zipPath = path.join(outDir, zipName);
  if (fs.existsSync(zipPath)) {
    fs.unlinkSync(zipPath);
  }
  run('zip', ['-r', '-q', zipPath, 'bundle', 'tarballs', 'README.txt'], { cwd: work });

  console.log(`Wrote ${zipPath}`);
}

main();
