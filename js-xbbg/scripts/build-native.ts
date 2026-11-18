import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import { inspectRustCodegen, sha256File } from './native-build-info';
import type { NativeBuildInfo } from './native-build-info';
import { nativeBinaryName, runtimeBinaryName } from './platform-map';

const packageDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(packageDir, '..');

interface CargoEvent {
  reason: string;
  package_id?: string;
  target?: { name: string; crate_types: readonly string[] };
  filenames?: readonly string[];
  env?: readonly (readonly [string, string])[];
  linked_libs?: readonly string[];
  linked_paths?: readonly string[];
  message?: { rendered?: string | null };
}

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((item: unknown) => typeof item === 'string');
}

function isCargoEvent(value: unknown): value is CargoEvent {
  return (
    typeof value === 'object' &&
    value !== null &&
    'reason' in value &&
    typeof value.reason === 'string' &&
    (!('package_id' in value) || typeof value.package_id === 'string') &&
    (!('target' in value) ||
      (typeof value.target === 'object' &&
        value.target !== null &&
        'name' in value.target &&
        typeof value.target.name === 'string' &&
        'crate_types' in value.target &&
        isStringArray(value.target.crate_types))) &&
    (!('filenames' in value) || isStringArray(value.filenames)) &&
    (!('env' in value) ||
      (Array.isArray(value.env) &&
        value.env.every(
          (entry: unknown) =>
            Array.isArray(entry) &&
            entry.length === 2 &&
            typeof entry[0] === 'string' &&
            typeof entry[1] === 'string',
        ))) &&
    (!('linked_libs' in value) || isStringArray(value.linked_libs)) &&
    (!('linked_paths' in value) || isStringArray(value.linked_paths)) &&
    (!('message' in value) ||
      (typeof value.message === 'object' &&
        value.message !== null &&
        (!('rendered' in value.message) ||
          value.message.rendered === null ||
          typeof value.message.rendered === 'string')))
  );
}

function fail(message: string): never {
  throw new Error(message);
}

function runTool(command: string, args: readonly string[], context: string): string {
  const result = spawnSync(command, args, { encoding: 'utf8' });
  if (result.error) {
    fail(`${context}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const output = [result.stdout, result.stderr]
      .filter((value): value is string => value !== undefined && value !== null && value.length > 0)
      .join('\n')
      .trim();
    fail(`${context}: ${output || `${command} exited with status ${result.status ?? 'unknown'}`}`);
  }
  return `${result.stdout ?? ''}${result.stderr ?? ''}`;
}

function stripOtoolPathSuffix(value: string): string {
  return value.replace(/\s+\(offset \d+\)$/u, '');
}

function parseDarwinRpaths(loadCommands: string): Set<string> {
  const rpaths = new Set<string>();
  let inRpath = false;

  for (const line of loadCommands.split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (trimmed === 'cmd LC_RPATH') {
      inRpath = true;
      continue;
    }
    if (inRpath && trimmed.startsWith('path ')) {
      rpaths.add(stripOtoolPathSuffix(trimmed.slice('path '.length)));
      inRpath = false;
      continue;
    }
    if (inRpath && trimmed.startsWith('cmd ')) {
      inRpath = false;
    }
  }

  return rpaths;
}

function parseDarwinLinkedLibraries(output: string): string[] {
  return output
    .split(/\r?\n/u)
    .slice(1)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/\s+\([^)]*\).*$/u, ''));
}

function readDarwinLoadCommands(binaryPath: string): string {
  return runTool(
    'otool',
    ['-l', binaryPath],
    `Failed to inspect Mach-O load commands for ${binaryPath}`,
  );
}

function readDarwinLinkedLibraries(binaryPath: string): string {
  return runTool(
    'otool',
    ['-L', binaryPath],
    `Failed to inspect Mach-O linked libraries for ${binaryPath}`,
  );
}

function installNameTool(args: readonly string[], context: string): void {
  runTool('install_name_tool', args, context);
}

function startsWithPath(value: string, parent: string): boolean {
  const normalizedValue = path.resolve(value);
  const normalizedParent = path.resolve(parent);
  return (
    normalizedValue === normalizedParent ||
    normalizedValue.startsWith(`${normalizedParent}${path.sep}`)
  );
}

function isSdkBlpapiLibrary(value: string, sdkLibDir: string): boolean {
  return (
    path.isAbsolute(value) &&
    startsWithPath(value, sdkLibDir) &&
    /^libblpapi3(_64|_32)?\.(so|dylib)$/u.test(path.basename(value))
  );
}

function isForbiddenDarwinPath(value: string): boolean {
  if (!path.isAbsolute(value)) {
    return false;
  }
  if (value.startsWith('/usr/lib/') || value.startsWith('/System/Library/')) {
    return false;
  }
  return true;
}

function verifyDarwinPortableBinary(binaryPath: string): void {
  const loadCommands = readDarwinLoadCommands(binaryPath);
  const linkedLibraries = parseDarwinLinkedLibraries(readDarwinLinkedLibraries(binaryPath));
  const values = [...parseDarwinRpaths(loadCommands), ...linkedLibraries];
  const forbidden = [...new Set(values.filter((value) => isForbiddenDarwinPath(value)))];
  if (forbidden.length > 0) {
    fail(
      `Mach-O load commands for ${binaryPath} contain non-portable build paths: ${forbidden.join(', ')}`,
    );
  }
}

// Keep the published macOS addon relocatable; Bloomberg's runtime remains user-provided.
function patchDarwinNativeAddon(binaryPath: string, sdkLibDir: string): void {
  if (process.platform !== 'darwin') {
    return;
  }

  installNameTool(
    ['-id', `@rpath/${nativeBinaryName}`, binaryPath],
    `Failed to set portable install name for ${binaryPath}`,
  );

  const linkedLibraries = parseDarwinLinkedLibraries(readDarwinLinkedLibraries(binaryPath));
  for (const linkedLibrary of linkedLibraries) {
    if (!isSdkBlpapiLibrary(linkedLibrary, sdkLibDir)) {
      continue;
    }
    installNameTool(
      ['-change', linkedLibrary, `@rpath/${path.basename(linkedLibrary)}`, binaryPath],
      `Failed to rewrite Bloomberg SDK dependency for ${binaryPath}`,
    );
  }

  let rpaths = parseDarwinRpaths(readDarwinLoadCommands(binaryPath));
  for (const rpath of rpaths) {
    if (!isForbiddenDarwinPath(rpath)) {
      continue;
    }
    installNameTool(
      ['-delete_rpath', rpath, binaryPath],
      `Failed to delete non-portable rpath ${rpath} from ${binaryPath}`,
    );
  }

  rpaths = parseDarwinRpaths(readDarwinLoadCommands(binaryPath));
  for (const rpath of ['@loader_path', '@loader_path/lib']) {
    if (rpaths.has(rpath)) {
      continue;
    }
    installNameTool(
      ['-add_rpath', rpath, binaryPath],
      `Failed to add portable rpath ${rpath} to ${binaryPath}`,
    );
  }

  verifyDarwinPortableBinary(binaryPath);
}

// The resolver addon has no Bloomberg dependency: it only needs a portable install name.
function patchDarwinRuntimeAddon(binaryPath: string): void {
  if (process.platform !== 'darwin') {
    return;
  }

  installNameTool(
    ['-id', `@rpath/${runtimeBinaryName}`, binaryPath],
    `Failed to set portable install name for ${binaryPath}`,
  );
  verifyDarwinPortableBinary(binaryPath);
}

function sdkVersion(includeDir: string): string | null {
  const headerPath = path.join(includeDir, 'blpapi_versionmacros.h');
  if (!fs.existsSync(headerPath)) {
    return null;
  }
  const header = fs.readFileSync(headerPath, 'utf8');
  const parts = ['MAJOR', 'MINOR', 'PATCH', 'BUILD'].map(
    (part) =>
      new RegExp(`^\\s*#\\s*define\\s+BLPAPI_VERSION_${part}\\s+(\\d+)`, 'mu').exec(header)?.[1],
  );
  return parts.some((part) => part === undefined) ? null : parts.join('.');
}

function main(): void {
  // Portable release by default; host tuning is deliberately local-only.
  const profile = process.argv.includes('--debug') ? 'debug' : 'release';
  const outputPath = path.join(packageDir, nativeBinaryName);
  const runtimeOutputPath = path.join(packageDir, runtimeBinaryName);
  const env: NodeJS.ProcessEnv = { ...process.env };
  if ((env.BLPAPI_ROOT ?? '').length === 0 && (env.XBBG_DEV_SDK_ROOT ?? '').length > 0) {
    env.BLPAPI_ROOT = path.resolve(repoRoot, env.XBBG_DEV_SDK_ROOT ?? '');
  }
  if (process.argv.includes('--target-cpu-native')) {
    const inherited =
      env.CARGO_ENCODED_RUSTFLAGS !== undefined
        ? env.CARGO_ENCODED_RUSTFLAGS.split('\u001F').filter(Boolean)
        : (env.RUSTFLAGS ?? '').split(/\s+/u).filter(Boolean);
    env.CARGO_ENCODED_RUSTFLAGS = [...inherited, '-C', 'target-cpu=native'].join('\u001F');
    delete env.RUSTFLAGS;
  }
  // Otherwise leave flag resolution to Cargo, including .cargo/config.toml.
  // Its build-script event reports the effective values, not this launcher's guesses.
  const cargoArgs = [
    'build',
    '-p',
    'napi-xbbg',
    '-p',
    'napi-xbbg-runtime',
    '--message-format=json-render-diagnostics',
  ];
  if (profile === 'release') {
    cargoArgs.push('--release');
  }
  const result = spawnSync(
    process.platform === 'win32' ? (env.ComSpec ?? 'cmd.exe') : 'cargo',
    process.platform === 'win32' ? ['/d', '/s', '/c', 'cargo', ...cargoArgs] : cargoArgs,
    {
      cwd: repoRoot,
      env,
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
      stdio: ['inherit', 'pipe', 'inherit'],
    },
  );
  if (result.error !== undefined) {
    fail(`Cargo invocation failed: ${result.error.message}`);
  }
  const events: CargoEvent[] = [];
  for (const line of (result.stdout ?? '').split(/\r?\n/u)) {
    if (line.length === 0) {
      continue;
    }
    const event: unknown = JSON.parse(line);
    if (!isCargoEvent(event)) {
      fail('Cargo emitted an invalid build event');
    }
    if (typeof event.message?.rendered === 'string') {
      process.stderr.write(event.message.rendered);
    }
    events.push(event);
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    return;
  }
  const artifacts = events.filter(
    (event) =>
      event.reason === 'compiler-artifact' &&
      event.target?.name === 'napi_xbbg' &&
      event.target.crate_types.includes('cdylib'),
  );
  if (artifacts.length !== 1 || artifacts[0] === undefined) {
    fail('Cargo did not identify exactly one current napi-xbbg cdylib artifact');
  }
  const artifact = artifacts[0];
  const files = artifact.filenames?.filter((file) => /\.(?:dll|dylib|so)$/u.test(file)) ?? [];
  if (files.length !== 1 || files[0] === undefined) {
    fail('Cargo did not identify exactly one loadable native library');
  }
  const artifactPath = files[0];
  const runtimeArtifacts = events.filter(
    (event) =>
      event.reason === 'compiler-artifact' &&
      event.target?.name === 'napi_xbbg_runtime' &&
      event.target.crate_types.includes('cdylib'),
  );
  const runtimeFiles =
    runtimeArtifacts[0]?.filenames?.filter((file) => /\.(?:dll|dylib|so)$/u.test(file)) ?? [];
  if (runtimeArtifacts.length !== 1 || runtimeFiles.length !== 1 || runtimeFiles[0] === undefined) {
    fail('Cargo did not identify exactly one napi-xbbg-runtime loadable native library');
  }
  const runtimeArtifactPath = runtimeFiles[0];
  const producer = events.filter(
    (event) => event.reason === 'build-script-executed' && event.package_id === artifact.package_id,
  );
  if (producer.length !== 1 || producer[0] === undefined) {
    fail('Cargo did not report native compiler provenance');
  }
  const buildValues = Object.fromEntries(producer[0].env ?? []);
  function buildValue(key: string): string {
    const value = buildValues[`XBBG_BUILD_${key}`];
    if (value === undefined) {
      fail(`Compiler provenance is missing ${key}`);
    }
    return value;
  }
  const actualProfile = buildValue('PROFILE');
  if (actualProfile !== profile) {
    fail(`Cargo built profile ${actualProfile}, not requested profile ${profile}`);
  }
  const rustFlags = buildValue('RUSTFLAGS').split('\u001F').filter(Boolean);
  const codegen = inspectRustCodegen(rustFlags);
  const libraryPattern = /^(dylib|static)=(blpapi3(?:_(?:32|64))?)$/u;
  const sdkEvent = events.find(
    (event) =>
      event.reason === 'build-script-executed' &&
      event.linked_libs?.some((library) => libraryPattern.test(library)) === true,
  );
  const linkedLibrary = sdkEvent?.linked_libs?.find((library) => libraryPattern.test(library));
  const libraryMatch = linkedLibrary === undefined ? null : libraryPattern.exec(linkedLibrary);
  const kind = libraryMatch?.[1];
  const stem = libraryMatch?.[2];
  if (sdkEvent === undefined || kind === undefined || stem === undefined) {
    fail('Cargo did not report the Bloomberg SDK link input');
  }
  const target = buildValue('TARGET');
  const libraryNames = target.includes('windows')
    ? [`${stem}.lib`]
    : kind === 'static'
      ? [`lib${stem}.a`]
      : target.includes('darwin')
        ? [`lib${stem}.dylib`, `lib${stem}.so`, `lib${stem}.a`]
        : [`lib${stem}.so`, `lib${stem}.a`];
  const libraryFile = (sdkEvent.linked_paths ?? [])
    .filter((directory) => directory.startsWith('native='))
    .flatMap((directory) => libraryNames.map((name) => path.join(directory.slice(7), name)))
    .find((file) => fs.existsSync(file));
  if (libraryFile === undefined) {
    fail(`Cannot fingerprint the SDK library Cargo linked: ${linkedLibrary}`);
  }
  const sdkValues = Object.fromEntries(sdkEvent.env ?? []);
  const sdkIncludeDir = sdkValues.XBBG_SDK_INCLUDE_DIR;
  if (sdkIncludeDir === undefined || sdkIncludeDir.length === 0) {
    fail('Cargo did not report the Bloomberg SDK headers used by the build');
  }
  const headerVersion = sdkVersion(sdkIncludeDir);
  if (headerVersion === null) {
    fail('The compiled Bloomberg SDK headers do not provide version macros');
  }
  const sdkLibDir = path.dirname(libraryFile);
  const preparedOutput = path.join(packageDir, `.${nativeBinaryName}.${process.pid}.build.tmp`);
  const preparedRuntimeOutput = path.join(
    packageDir,
    `.${runtimeBinaryName}.${process.pid}.build.tmp`,
  );
  const infoPath = path.join(packageDir, '.native-build-info.json');
  const preparedInfo = `${infoPath}.${process.pid}.tmp`;
  try {
    fs.copyFileSync(artifactPath, preparedOutput);
    fs.chmodSync(preparedOutput, 0o755);
    patchDarwinNativeAddon(preparedOutput, sdkLibDir);
    fs.copyFileSync(runtimeArtifactPath, preparedRuntimeOutput);
    fs.chmodSync(preparedRuntimeOutput, 0o755);
    patchDarwinRuntimeAddon(preparedRuntimeOutput);
    const buildInfo: NativeBuildInfo = {
      allocator: buildValue('ALLOCATOR'),
      artifactSha256: sha256File(preparedOutput),
      gitCommit: buildValue('GIT_COMMIT'),
      optLevel: buildValue('OPT_LEVEL'),
      portable: codegen.portable,
      profile,
      rustcVersion: buildValue('RUSTC_VERSION'),
      rustFlags,
      runtimeAddon: {
        file: runtimeBinaryName,
        sha256: sha256File(preparedRuntimeOutput),
      },
      schemaVersion: 2,
      sdkLibrary: { file: path.basename(libraryFile), sha256: sha256File(libraryFile) },
      sdkVersion: headerVersion,
      target,
      targetCpu: codegen.targetCpu,
      targetFeatures: buildValue('TARGET_FEATURES').split(',').filter(Boolean),
    };
    fs.writeFileSync(preparedInfo, `${JSON.stringify(buildInfo, null, 2)}\n`);
    fs.renameSync(preparedOutput, outputPath);
    fs.renameSync(preparedRuntimeOutput, runtimeOutputPath);
    fs.renameSync(preparedInfo, infoPath);
  } finally {
    fs.rmSync(preparedOutput, { force: true });
    fs.rmSync(preparedRuntimeOutput, { force: true });
    fs.rmSync(preparedInfo, { force: true });
  }
  console.log(
    `Copied ${path.relative(repoRoot, artifactPath)} -> ${path.relative(repoRoot, outputPath)}`,
  );
  console.log(
    `Copied ${path.relative(repoRoot, runtimeArtifactPath)} -> ${path.relative(repoRoot, runtimeOutputPath)}`,
  );
}

try {
  main();
} catch (error) {
  console.error(`js-xbbg build failed: ${String(error)}`);
  process.exitCode = 1;
}
