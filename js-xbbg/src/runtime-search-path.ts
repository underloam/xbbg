import fs from 'node:fs';
import path from 'node:path';

const BLPAPI_RUNTIME_NAMES = Object.freeze([
  'blpapi3_64.dll',
  'blpapi3_32.dll',
  'libblpapi3.dylib',
  'libblpapi3_64.so',
  'libblpapi3.so',
]);

const WINDOWS_PATH_SEPARATOR = ';';

type Exists = (target: string) => boolean;
type ReadDir = (target: string) => fs.Dirent[];

export interface RuntimeSearchPathOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly exists?: Exists;
  readonly platform?: NodeJS.Platform;
  readonly readDir?: ReadDir;
  readonly repoRoot?: string;
}

const readDirectory: ReadDir = (target) => fs.readdirSync(target, { withFileTypes: true });

function windowsPathKey(value: string): string {
  return path.win32.normalize(value).toLowerCase();
}

function candidateKey(value: string, platform: NodeJS.Platform): string {
  return platform === 'win32' ? windowsPathKey(value) : path.resolve(value);
}

function pushUnique(
  candidates: string[],
  seen: Set<string>,
  candidate: string | undefined,
  platform: NodeJS.Platform,
): void {
  if (candidate === undefined || candidate.length === 0) {
    return;
  }
  const key = candidateKey(candidate, platform);
  if (seen.has(key)) {
    return;
  }
  seen.add(key);
  candidates.push(candidate);
}

function windowsDrive(value: string | undefined): string {
  if (value !== undefined && /^[A-Za-z]:/u.test(value)) {
    return value.slice(0, 2);
  }
  return 'C:';
}

export function collectWindowsDapiRootCandidates(env: NodeJS.ProcessEnv = process.env): string[] {
  const candidates: string[] = [];
  const seen = new Set<string>();
  const add = (candidate: string | undefined): void => {
    pushUnique(candidates, seen, candidate, 'win32');
  };

  const systemDrive = windowsDrive(env.SystemDrive);
  const systemDriveRoot = `${systemDrive}\\`;
  add(path.win32.join(systemDriveRoot, 'blp', 'DAPI'));
  if (systemDrive.toLowerCase() !== 'c:') {
    add(path.win32.join('C:\\', 'blp', 'DAPI'));
  }

  const programFilesRoots = [
    env.ProgramFiles,
    path.win32.join(systemDriveRoot, 'Program Files'),
    env['ProgramFiles(x86)'],
    path.win32.join(systemDriveRoot, 'Program Files (x86)'),
  ];
  for (const root of programFilesRoots) {
    add(root === undefined ? undefined : path.win32.join(root, 'Bloomberg', 'Blp', 'DAPI'));
  }

  const localAppData =
    env.LOCALAPPDATA ??
    (env.USERPROFILE === undefined
      ? undefined
      : path.win32.join(env.USERPROFILE, 'AppData', 'Local'));
  if (localAppData !== undefined) {
    add(path.win32.join(localAppData, 'Bloomberg', 'DAPI'));
    add(path.win32.join(localAppData, 'Bloomberg', 'Blp', 'DAPI'));
  }

  return candidates;
}

function containsBlpapiRuntime(dir: string, exists: Exists = fs.existsSync): boolean {
  if (dir.length === 0) {
    return false;
  }
  return BLPAPI_RUNTIME_NAMES.some((name) => exists(path.join(dir, name)));
}

function parseVersionParts(name: string): number[] | null {
  const parts = name.split('.').map((part) => Number(part));
  return parts.every((part) => Number.isInteger(part) && part >= 0) ? parts : null;
}

function compareSdkRoots(left: string, right: string): number {
  const leftParts = parseVersionParts(path.basename(left));
  const rightParts = parseVersionParts(path.basename(right));
  if (leftParts !== null && rightParts !== null) {
    const length = Math.max(leftParts.length, rightParts.length);
    for (let index = 0; index < length; index += 1) {
      const leftPart = leftParts[index] ?? 0;
      const rightPart = rightParts[index] ?? 0;
      if (leftPart !== rightPart) {
        return rightPart - leftPart;
      }
    }
  }
  if (leftParts !== null) {
    return -1;
  }
  if (rightParts !== null) {
    return 1;
  }
  return right.localeCompare(left);
}

function pushSdkRuntimeCandidates(candidates: string[], sdkRoot: string): void {
  const resolved = path.resolve(sdkRoot);
  candidates.push(
    resolved,
    path.join(resolved, 'bin'),
    path.join(resolved, 'lib'),
    path.join(resolved, 'Lib'),
    path.join(resolved, 'lib', 'win64'),
    path.join(resolved, 'lib', 'win32'),
  );
}

function addSdkRuntimeRoot(add: (candidate: string | undefined) => void, sdkRoot: string): void {
  const sdkCandidates: string[] = [];
  pushSdkRuntimeCandidates(sdkCandidates, sdkRoot);
  for (const candidate of sdkCandidates) {
    add(candidate);
  }
}

export function resolveVendorSdkRoot(
  repoRoot: string,
  exists: Exists = fs.existsSync,
  readDir: ReadDir = readDirectory,
): string | null {
  const vendorDir = path.join(repoRoot, 'vendor', 'blpapi-sdk');
  if (!exists(vendorDir)) {
    return null;
  }
  const candidates = [vendorDir];
  for (const entry of readDir(vendorDir)) {
    if (entry.isDirectory()) {
      candidates.push(path.join(vendorDir, entry.name));
    }
  }
  candidates.sort(compareSdkRoots);
  return (
    candidates.find((candidate) => {
      const dirs = [
        candidate,
        path.join(candidate, 'bin'),
        path.join(candidate, 'lib'),
        path.join(candidate, 'Lib'),
      ];
      return dirs.some((dir) => containsBlpapiRuntime(dir, exists));
    }) ?? null
  );
}

function collectRuntimeSearchCandidates(options: RuntimeSearchPathOptions = {}): string[] {
  const env = options.env ?? process.env;
  const exists = options.exists ?? fs.existsSync;
  const platform = options.platform ?? process.platform;
  const readDir = options.readDir ?? readDirectory;
  const repoRoot = options.repoRoot ?? path.resolve(__dirname, '..', '..');
  const candidates: string[] = [];
  const seen = new Set<string>();
  const add = (candidate: string | undefined): void => {
    pushUnique(candidates, seen, candidate, platform);
  };

  const libDir = env.BLPAPI_LIB_DIR;
  if (libDir !== undefined && libDir.length > 0) {
    add(path.resolve(libDir));
  }

  const root = env.BLPAPI_ROOT;
  if (root !== undefined && root.length > 0) {
    addSdkRuntimeRoot(add, root);
  }

  const devRoot = env.XBBG_DEV_SDK_ROOT;
  if (devRoot !== undefined && devRoot.length > 0) {
    const resolved = path.isAbsolute(devRoot) ? devRoot : path.resolve(repoRoot, devRoot);
    addSdkRuntimeRoot(add, resolved);
  }

  const vendorRoot = resolveVendorSdkRoot(repoRoot, exists, readDir);
  if (vendorRoot !== null) {
    addSdkRuntimeRoot(add, vendorRoot);
  }

  if (platform === 'win32') {
    for (const dapiRoot of collectWindowsDapiRootCandidates(env)) {
      add(dapiRoot);
    }
  }

  return candidates;
}

function prependWindowsPath(env: NodeJS.ProcessEnv, candidate: string): void {
  const key = Object.keys(env).find((envKey) => envKey.toUpperCase() === 'PATH') ?? 'PATH';
  const currentPath = env[key] ?? '';
  const currentParts = currentPath.split(WINDOWS_PATH_SEPARATOR).filter((part) => part.length > 0);
  const candidatePathKey = windowsPathKey(candidate);
  if (currentParts.some((part) => windowsPathKey(part) === candidatePathKey)) {
    return;
  }
  env[key] =
    currentPath.length > 0 ? `${candidate}${WINDOWS_PATH_SEPARATOR}${currentPath}` : candidate;
}

export function configureRuntimeSearchPath(options: RuntimeSearchPathOptions = {}): string | null {
  const env = options.env ?? process.env;
  const exists = options.exists ?? fs.existsSync;
  const platform = options.platform ?? process.platform;
  if (platform !== 'win32') {
    return null;
  }

  for (const candidate of collectRuntimeSearchCandidates(options)) {
    if (!containsBlpapiRuntime(candidate, exists)) {
      continue;
    }
    prependWindowsPath(env, candidate);
    return candidate;
  }

  return null;
}
