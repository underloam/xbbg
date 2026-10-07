import type { SpawnSyncReturns } from 'node:child_process';

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  inspectRustCodegen,
  nativeBuildTargets,
  readNativeBuildInfo,
} from '../scripts/native-build-info';
import type { NativeBuildInfo } from '../scripts/native-build-info';
import { nativeBinaryName, nativePackageForKey, platformKey } from '../src/native/platform-map';

interface NativeFixture {
  root: string;
  packageDir: string;
  stagedArtifact: string;
  target: string;
}

function withNativeRepo(check: (fixture: NativeFixture) => void): void {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-build-provenance-'));
  try {
    const packageDir = path.join(root, 'js-xbbg');
    for (const relative of [
      'scripts/build-native.ts',
      'scripts/stage-native-package.ts',
      'scripts/native-build-info.ts',
      'scripts/npm-process.ts',
      'scripts/platform-map.ts',
      'src/native/platform-map.ts',
    ]) {
      const destination = path.join(packageDir, relative);
      fs.mkdirSync(path.dirname(destination), { recursive: true });
      fs.copyFileSync(path.resolve(__dirname, '..', relative), destination);
    }
    const descriptor = nativePackageForKey(platformKey());
    if (descriptor === null) {
      throw new Error('native staging fixture requires a supported package platform');
    }
    const platformDir = path.join(packageDir, descriptor.packageDir);
    fs.mkdirSync(platformDir, { recursive: true });
    fs.writeFileSync(path.join(platformDir, 'index.js'), 'module.exports = {};\n');
    const stagedArtifact = path.join(platformDir, nativeBinaryName);
    fs.writeFileSync(stagedArtifact, 'previous-artifact');
    check({ root, packageDir, stagedArtifact, target: nativeBuildTargets[descriptor.key] });
  } finally {
    fs.rmSync(root, { force: true, recursive: true });
  }
}

function prepareArtifact(
  fixture: NativeFixture,
  artifact: string,
  overrides: Partial<NativeBuildInfo> = {},
): void {
  const rustFlags = overrides.rustFlags ?? [];
  const info: NativeBuildInfo = {
    allocator: 'system',
    artifactSha256: createHash('sha256').update(artifact).digest('hex'),
    gitCommit: 'compiler-commit',
    optLevel: '3',
    profile: 'release',
    rustcVersion: 'rustc 1.90.0',
    rustFlags,
    schemaVersion: 1,
    sdkLibrary: { file: 'sdk-link-input', sha256: '0'.repeat(64) },
    sdkVersion: '3.26.7.1',
    target: fixture.target,
    targetFeatures: [],
    ...inspectRustCodegen(rustFlags),
    ...overrides,
  };
  fs.writeFileSync(path.join(fixture.packageDir, nativeBinaryName), artifact);
  fs.writeFileSync(path.join(fixture.packageDir, '.native-build-info.json'), JSON.stringify(info));
}

function stage(fixture: NativeFixture, args: readonly string[] = []): SpawnSyncReturns<string> {
  return spawnSync(
    process.execPath,
    [
      require.resolve('tsx/cli'),
      path.join(fixture.packageDir, 'scripts/stage-native-package.ts'),
      ...args,
    ],
    { cwd: fixture.root, encoding: 'utf8', timeout: 20_000 },
  );
}

function fixtureTool(directory: string, name: string, code: string): void {
  const script = path.join(directory, `${name}.cjs`);
  fs.writeFileSync(script, code);
  if (process.platform === 'win32') {
    fs.writeFileSync(
      path.join(directory, `${name}.cmd`),
      `@echo off\r\n"${process.execPath}" "${script}" %*\r\n`,
    );
  } else {
    const quoted = [process.execPath, script]
      .map((value) => `'${value.replaceAll("'", String.raw`'\''`)}'`)
      .join(' ');
    fs.writeFileSync(path.join(directory, name), `#!/bin/sh\nexec ${quoted} "$@"\n`, {
      mode: 0o755,
    });
  }
}

// These fixtures drive the native build/staging tooling, which only ever runs under Node.
describe.skipIf(process.versions.bun !== undefined)('native artifact provenance', () => {
  test('publishes exactly the attested portable release over an existing artifact', () => {
    withNativeRepo((fixture) => {
      prepareArtifact(fixture, 'portable-release');
      const result = stage(fixture);
      expect(result).toMatchObject({ status: 0 });
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('portable-release');
    });
  });

  test('does not promote a host-tuned artifact even when its hash matches', () => {
    withNativeRepo((fixture) => {
      prepareArtifact(fixture, 'host-tuned-release', { rustFlags: ['-C', 'target-cpu=native'] });
      const result = stage(fixture);
      expect(result.status).toBe(1);
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('previous-artifact');
    });
  });

  test('recognizes rustc long codegen spellings when enforcing release portability', () => {
    withNativeRepo((fixture) => {
      for (const rustFlags of [
        ['--codegen', 'target-cpu=native'],
        ['--codegen=target-cpu=native'],
      ]) {
        prepareArtifact(fixture, 'host-tuned-release', { rustFlags });
        expect(stage(fixture).status).toBe(1);
        expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('previous-artifact');
      }
    });
  });

  test('requires debug opt-in and still checks the bytes on every staging attempt', () => {
    withNativeRepo((fixture) => {
      prepareArtifact(fixture, 'debug-artifact', { optLevel: '0', profile: 'debug' });
      expect(stage(fixture).status).toBe(1);
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('previous-artifact');
      const accepted = stage(fixture, ['--debug']);
      expect(accepted).toMatchObject({ status: 0 });
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('debug-artifact');
      fs.writeFileSync(path.join(fixture.packageDir, nativeBinaryName), 'replaced-without-rebuild');
      expect(stage(fixture, ['--debug']).status).toBe(1);
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('debug-artifact');
    });
  });

  test('refuses a matching artifact compiled for a different package target', () => {
    withNativeRepo((fixture) => {
      prepareArtifact(fixture, 'foreign-artifact', { target: 'aarch64-unknown-linux-gnu' });
      expect(stage(fixture).status).toBe(1);
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('previous-artifact');
    });
  });

  test('keeps the packaged artifact when debug provenance is missing', () => {
    withNativeRepo((fixture) => {
      fs.writeFileSync(path.join(fixture.packageDir, nativeBinaryName), 'unattested-artifact');
      expect(stage(fixture, ['--debug']).status).toBe(1);
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('previous-artifact');
    });
  });

  test('selects Cargo-reported target artifacts and SDK link inputs instead of stale conventional paths', () => {
    withNativeRepo((fixture) => {
      const toolDir = path.join(fixture.root, 'tools');
      const sdkRoot = path.join(fixture.root, 'sdk');
      const includeDir = path.join(sdkRoot, 'include');
      const guessedLibDir = path.join(sdkRoot, 'lib');
      const actualLibDir = path.join(fixture.root, 'actual-link-input');
      const customTarget = path.join(fixture.root, 'custom-cargo-target');
      const extension =
        process.platform === 'win32' ? 'dll' : process.platform === 'darwin' ? 'dylib' : 'so';
      const artifactPath = path.join(
        customTarget,
        fixture.target,
        'release',
        `napi_xbbg.${extension}`,
      );
      const staleArtifact = path.join(fixture.root, 'target', 'release', `napi_xbbg.${extension}`);
      const sdkLibrary =
        process.platform === 'win32' ? 'blpapi3_64.lib' : `libblpapi3_64.${extension}`;
      for (const directory of [
        toolDir,
        includeDir,
        guessedLibDir,
        actualLibDir,
        path.dirname(artifactPath),
        path.dirname(staleArtifact),
      ]) {
        fs.mkdirSync(directory, { recursive: true });
      }
      fs.writeFileSync(artifactPath, 'current-cargo-artifact');
      fs.writeFileSync(staleArtifact, 'stale-conventional-artifact');
      fs.writeFileSync(path.join(guessedLibDir, sdkLibrary), 'unlinked-sdk');
      fs.writeFileSync(path.join(actualLibDir, sdkLibrary), 'actual-sdk-link-input');
      fs.writeFileSync(
        path.join(includeDir, 'blpapi_versionmacros.h'),
        [
          '#define BLPAPI_VERSION_MAJOR 3',
          '#define BLPAPI_VERSION_MINOR 26',
          '#define BLPAPI_VERSION_PATCH 7',
          '#define BLPAPI_VERSION_BUILD 1',
        ].join('\n'),
      );
      fixtureTool(toolDir, 'cargo', 'process.stdout.write(process.env.XBBG_CARGO_TRANSCRIPT);\n');
      // This fixture exercises Cargo selection, not Mach-O rewriting.
      fixtureTool(toolDir, 'otool', 'process.stdout.write("");\n');
      fixtureTool(toolDir, 'install_name_tool', 'process.exit(0);\n');
      const buildValues = {
        ALLOCATOR: 'system',
        GIT_COMMIT: 'compiler-commit',
        OPT_LEVEL: '3',
        PROFILE: 'release',
        RUSTC_VERSION: 'rustc 1.90.0',
        RUSTFLAGS: '',
        TARGET: fixture.target,
        TARGET_FEATURES: 'sse2',
      };
      const transcript = [
        {
          reason: 'build-script-executed',
          package_id: 'napi-xbbg',
          env: Object.entries(buildValues).map(([key, value]) => [`XBBG_BUILD_${key}`, value]),
        },
        {
          reason: 'build-script-executed',
          package_id: 'xbbg-blpapi-sys',
          env: [['XBBG_SDK_INCLUDE_DIR', includeDir]],
          linked_libs: ['dylib=blpapi3_64'],
          linked_paths: [`native=${actualLibDir}`],
        },
        {
          reason: 'compiler-artifact',
          package_id: 'napi-xbbg',
          target: { name: 'napi_xbbg', crate_types: ['cdylib'] },
          filenames: [artifactPath],
        },
      ]
        .map((event) => JSON.stringify(event))
        .join('\n');
      const result = spawnSync(
        process.execPath,
        [require.resolve('tsx/cli'), path.join(fixture.packageDir, 'scripts/build-native.ts')],
        {
          cwd: fixture.root,
          encoding: 'utf8',
          timeout: 20_000,
          env: {
            ...process.env,
            BLPAPI_ROOT: path.join(fixture.root, 'missing-sdk-root'),
            BLPAPI_INCLUDE_DIR: includeDir,
            BLPAPI_LIB_DIR: actualLibDir,
            CARGO_BUILD_TARGET: fixture.target,
            CARGO_TARGET_DIR: customTarget,
            // Cargo's encoded flags win; launcher RUSTFLAGS are not compiler facts.
            CARGO_ENCODED_RUSTFLAGS: '',
            RUSTFLAGS: '-C target-cpu=native',
            PATH: `${toolDir}${path.delimiter}${process.env.PATH ?? ''}`,
            XBBG_CARGO_TRANSCRIPT: transcript,
          },
        },
      );
      expect(result).toMatchObject({ status: 0 });
      expect(fs.readFileSync(path.join(fixture.packageDir, nativeBinaryName), 'utf8')).toBe(
        'current-cargo-artifact',
      );
      const info = readNativeBuildInfo(path.join(fixture.packageDir, '.native-build-info.json'));
      expect(info.sdkLibrary.sha256).toBe(
        createHash('sha256').update('actual-sdk-link-input').digest('hex'),
      );
      const staged = stage(fixture);
      expect(staged).toMatchObject({ status: 0 });
      expect(fs.readFileSync(fixture.stagedArtifact, 'utf8')).toBe('current-cargo-artifact');
    });
  });
});
