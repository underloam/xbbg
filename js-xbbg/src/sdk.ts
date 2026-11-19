import { wrapError } from './errors';
import type { ProvisionSdkResult, SdkCliOutput } from './napi';
import { loadNativeAddonWithoutRuntime } from './native/load-addon';

export type { ProvisionSdkResult, SdkCliOutput } from './napi';

/** Bloomberg's public PyPI-compatible simple index URL. */
export const BLOOMBERG_PYPI_INDEX =
  'https://blpapi.bloomberg.com/repository/releases/python/simple/';

/** Supported Bloomberg runtime target platforms. */
export type SdkPlatform = 'windows-x86_64' | 'linux-x86_64' | 'macos-aarch64';

/** Structured provisioning source specification (alternative to a string spec). */
export type SdkSourceInput =
  | string
  | { readonly kind: 'pypi'; readonly url: string; readonly project?: string }
  | { readonly kind: 'conda'; readonly channel: string; readonly package?: string }
  | { readonly kind: 'artifact'; readonly location: string; readonly sha256?: string };

/** Proxy configuration for provisioning downloads (`"system"`, `"direct"`, a proxy URL, or structured config). */
export type SdkProxyInput =
  | string
  | { readonly mode: 'system' }
  | { readonly mode: 'direct' }
  | { readonly mode: 'explicit'; readonly url: string; readonly noProxy?: readonly string[] };

/** Per-host credentials for private PyPI/conda mirrors. */
export type SdkCredentialInput =
  | { readonly host: string; readonly username: string; readonly password: string }
  | { readonly host: string; readonly token: string };

/** Options for {@link provisionSdk}. */
export interface ProvisionSdkOptions {
  /**
   * Source specification: a string (`pypi:...`, `conda:...`, `artifact:...`, or bare URL/path)
   * or a structured object. Defaults to `process.env.XBBG_SDK_SOURCE` when omitted.
   */
  readonly source?: SdkSourceInput;
  /** Exact version (e.g. `"3.26.9.1"`) or `"latest"`. Defaults to `XBBG_SDK_VERSION` or latest. */
  readonly version?: string;
  /** Target platform; defaults to the host platform. */
  readonly platform?: SdkPlatform;
  /** Cache root directory; defaults to `XBBG_CACHE_DIR` or the shared per-user xbbg cache. */
  readonly cacheDir?: string;
  /** Disable network access and resolve from the local cache only. */
  readonly offline?: boolean;
  /** Explicit proxy setting; defaults to system proxy settings. */
  readonly proxy?: SdkProxyInput;
  /** Per-host basic or bearer credentials. */
  readonly credentials?: readonly SdkCredentialInput[];
  /** Path to a netrc file for per-host credentials. */
  readonly netrc?: string;
  /** Path to a PEM bundle appended to the OS trust store. */
  readonly extraCaPem?: string;
  /** Connection timeout in milliseconds. */
  readonly connectTimeoutMillis?: number;
  /** Read timeout in milliseconds. */
  readonly readTimeoutMillis?: number;
  /** Number of HTTP retries for transient failures. */
  readonly retries?: number;
  /** Require an expected SHA-256 for remote artifact URLs (default `true`). */
  readonly requireArtifactHash?: boolean;
}

/** Options for {@link installedSdks}. */
export interface InstalledSdksOptions {
  /** Cache root directory; defaults to the shared xbbg cache. */
  readonly cacheDir?: string;
  /** Optional platform filter. */
  readonly platform?: SdkPlatform;
}

interface ProvisionCapableAddon {
  readonly provisionSdk: (optionsJson: string) => Promise<ProvisionSdkResult>;
  readonly installedSdks: (cacheDir?: string, platform?: string) => ProvisionSdkResult[];
  readonly defaultSdkCacheDir: () => string | null;
  readonly runSdkCli: (prog: string, args: string[], env?: Record<string, string>) => SdkCliOutput;
}

function requireProvisionAddon(): ProvisionCapableAddon {
  const addon = loadNativeAddonWithoutRuntime();
  if (
    typeof addon.provisionSdk !== 'function' ||
    typeof addon.installedSdks !== 'function' ||
    typeof addon.defaultSdkCacheDir !== 'function' ||
    typeof addon.runSdkCli !== 'function'
  ) {
    throw new TypeError(
      'This @xbbg/core native addon was built without SDK provisioning support. Rebuild or reinstall @xbbg/core.',
    );
  }
  return {
    defaultSdkCacheDir: addon.defaultSdkCacheDir,
    installedSdks: addon.installedSdks,
    provisionSdk: addon.provisionSdk,
    runSdkCli: addon.runSdkCli,
  };
}

function buildProvisionPayload(options: ProvisionSdkOptions): string {
  const source = options.source ?? process.env.XBBG_SDK_SOURCE?.trim();
  if (source === undefined || (typeof source === 'string' && source.length === 0)) {
    throw new Error(
      'provisionSdk requires `source` (or set XBBG_SDK_SOURCE to a PyPI index, conda channel, or artifact path/URL)',
    );
  }
  const payload: Record<string, unknown> = { source };
  const version = options.version ?? process.env.XBBG_SDK_VERSION?.trim();
  if (version !== undefined && version.length > 0) {
    payload.version = version;
  }
  if (options.platform !== undefined) {
    payload.platform = options.platform;
  }
  const cacheDir = options.cacheDir ?? process.env.XBBG_CACHE_DIR?.trim();
  if (cacheDir !== undefined && cacheDir.length > 0) {
    payload.cacheDir = cacheDir;
  }
  if (options.offline !== undefined) {
    payload.offline = options.offline;
  }
  if (options.proxy !== undefined) {
    payload.proxy = options.proxy;
  }
  if (options.credentials !== undefined) {
    payload.credentials = options.credentials;
  }
  if (options.netrc !== undefined) {
    payload.netrc = options.netrc;
  }
  if (options.extraCaPem !== undefined) {
    payload.extraCaPem = options.extraCaPem;
  }
  if (options.connectTimeoutMillis !== undefined) {
    payload.connectTimeoutMillis = options.connectTimeoutMillis;
  }
  if (options.readTimeoutMillis !== undefined) {
    payload.readTimeoutMillis = options.readTimeoutMillis;
  }
  if (options.retries !== undefined) {
    payload.retries = options.retries;
  }
  if (options.requireArtifactHash !== undefined) {
    payload.requireArtifactHash = options.requireArtifactHash;
  }
  return JSON.stringify(payload);
}

/**
 * Download, verify, and atomically install a Bloomberg BLPAPI runtime into the shared xbbg cache.
 *
 * Works before any Bloomberg runtime exists on the machine (import from `@xbbg/core/sdk`).
 */
export async function provisionSdk(options: ProvisionSdkOptions = {}): Promise<ProvisionSdkResult> {
  const addon = requireProvisionAddon();
  const payload = buildProvisionPayload(options);
  try {
    return await addon.provisionSdk(payload);
  } catch (error) {
    throw wrapError(error);
  }
}

/**
 * List verified BLPAPI runtime installs in the shared xbbg cache, newest version first.
 *
 * Works before any Bloomberg runtime exists on the machine (import from `@xbbg/core/sdk`).
 */
export function installedSdks(options: InstalledSdksOptions = {}): ProvisionSdkResult[] {
  const addon = requireProvisionAddon();
  const cacheDir = options.cacheDir ?? process.env.XBBG_CACHE_DIR?.trim();
  try {
    return addon.installedSdks(
      cacheDir !== undefined && cacheDir.length > 0 ? cacheDir : undefined,
      options.platform,
    );
  } catch (error) {
    throw wrapError(error);
  }
}

/**
 * Default shared xbbg cache root directory, or `null` when no home directory is known.
 */
export function defaultSdkCacheDir(): string | null {
  const addon = requireProvisionAddon();
  return addon.defaultSdkCacheDir();
}

/**
 * Run the shared `xbbg-sdk` CLI parser and command runner, returning exit code and output.
 */
export function runSdkCli(
  args: readonly string[],
  options: { readonly prog?: string; readonly env?: NodeJS.ProcessEnv } = {},
): SdkCliOutput {
  const addon = requireProvisionAddon();
  const sourceEnv = options.env ?? process.env;
  const envMap: Record<string, string> = {};
  for (const [key, value] of Object.entries(sourceEnv)) {
    if (value !== undefined) {
      envMap[key] = value;
    }
  }
  return addon.runSdkCli(options.prog ?? 'xbbg-sdk', [...args], envMap);
}
