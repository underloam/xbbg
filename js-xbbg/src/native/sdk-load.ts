import type { LoadedSdk, NativeAddon, SdkLocateRequest } from '../napi';

/**
 * The environment variables the shared Bloomberg runtime search reads (crates/xbbg-sdk). Names are
 * uppercase: Windows looks them up ignoring case and passes them on in this spelling.
 */
const SDK_ENV_KEYS = Object.freeze([
  'BLPAPI_LIB_DIR',
  'BLPAPI_ROOT',
  'XBBG_DEV_SDK_ROOT',
  'XBBG_CACHE_DIR',
  'XDG_CACHE_HOME',
  'HOME',
  'USERPROFILE',
  'LOCALAPPDATA',
  'SYSTEMDRIVE',
  'PROGRAMFILES',
  'PROGRAMFILES(X86)',
  'PATH',
  'LD_LIBRARY_PATH',
  'DYLD_LIBRARY_PATH',
  'DYLD_FALLBACK_LIBRARY_PATH',
]);

export interface SdkRequestOptions {
  /** The environment to read; defaults to `process.env`. */
  readonly env?: NodeJS.ProcessEnv;
  readonly platform?: NodeJS.Platform;
  /** The development checkout, which enables the vendored SDK rule. */
  readonly repoRoot: string;
}

function readEnv(env: NodeJS.ProcessEnv, key: string, ignoreCase: boolean): string | undefined {
  if (!ignoreCase) {
    return env[key];
  }
  const actual = Object.keys(env).find((candidate) => candidate.toUpperCase() === key);
  return actual === undefined ? undefined : env[actual];
}

/**
 * Build the Node side of the Bloomberg runtime search: the environment this process sees and the
 * checkout the package was built in. The search order itself lives in Rust.
 *
 * The environment is read here and handed over, never left for Rust to read: under Bun and in
 * worker threads a `process.env` assignment never reaches the operating-system environment the
 * addon would otherwise see.
 */
export function sdkLocateRequest(options: SdkRequestOptions): SdkLocateRequest {
  const env = options.env ?? process.env;
  const ignoreCase = (options.platform ?? process.platform) === 'win32';
  const view: Record<string, string> = {};
  for (const key of SDK_ENV_KEYS) {
    const value = readEnv(env, key, ignoreCase);
    // An empty value counts as unset, as it does for every other host.
    if (value !== undefined && value.length > 0) {
      view[key] = value;
    }
  }
  return { env: view, repoRoot: options.repoRoot };
}

/** The message of the import-time failure: what the locator reports, then how to fix it. */
export function sdkLoadErrorMessage(error: unknown): string {
  const reason = error instanceof Error ? error.message : String(error);
  return [
    '@xbbg/core could not load the Bloomberg runtime.',
    reason,
    "Set BLPAPI_LIB_DIR to the folder that holds Bloomberg's blpapi runtime (BLPAPI_ROOT also works).",
  ].join('\n');
}

/**
 * Find the Bloomberg runtime and load it into this process; call it once, right after the engine
 * addon loads. Throws an error that lists every location the search refused, in order.
 */
export function loadBloombergRuntime(
  addon: Pick<NativeAddon, 'loadSdk'>,
  options: SdkRequestOptions,
): LoadedSdk {
  try {
    return addon.loadSdk(sdkLocateRequest(options));
  } catch (error) {
    throw new Error(sdkLoadErrorMessage(error), { cause: error });
  }
}
