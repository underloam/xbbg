import type * as xbbg from "@xbbg/core";

import type { BloombergToolsOptions, NormalizedBloombergToolsOptions } from "./options";
import { normalizeBloombergToolsOptions } from "./options";

type XbbgCoreModule = typeof xbbg;
interface EntitlementReport {
  readonly entitled: boolean;
  readonly failedEids: readonly number[];
}

type CoreEngineMethods = Pick<
  Awaited<ReturnType<XbbgCoreModule["connect"]>>,
  | "bdp"
  | "bdh"
  | "bds"
  | "bql"
  | "bsrch"
  | "bqr"
  | "bflds"
  | "beqs"
  | "yas"
  | "preferreds"
  | "corporateBonds"
  | "indexMembers"
  | "resolveIsins"
  | "resolveVenues"
  | "auctionSnapshot"
  | "issuerIsins"
  | "etfHoldings"
  | "stream"
  | "mktbar"
  | "depth"
>;

export type XbbgEngineLike = {
  readonly [Method in keyof CoreEngineMethods]: OmitThisParameter<CoreEngineMethods[Method]>;
} & {
  readonly bdib: (
    ticker: string,
    options: xbbg.BdibOptions & { readonly returnEids?: boolean },
  ) => Promise<unknown>;
  readonly bdtick: (
    ticker: string,
    options: xbbg.BdtickOptions & { readonly returnEids?: boolean },
  ) => Promise<unknown>;
  readonly checkEntitlements: (
    service: string,
    eids: readonly number[],
  ) => Promise<EntitlementReport>;
};
export interface XbbgCoreLike {
  readonly ext: XbbgCoreModule["ext"];
  readonly connect: (config?: xbbg.EngineConfig) => Promise<XbbgEngineLike>;
}

export interface CoreResolver {
  readonly options: NormalizedBloombergToolsOptions;
  getCore(): Promise<XbbgCoreLike>;
  getEngine(): Promise<XbbgEngineLike>;
}

async function importCore(): Promise<XbbgCoreLike> {
  // @xbbg/core is loaded lazily so constructing tools never loads the native addon.
  // Keep the adapter's engine contract derived from the current core package.
  return await import("@xbbg/core");
}

export function createCoreResolver(options: BloombergToolsOptions = {}): CoreResolver {
  const normalized = normalizeBloombergToolsOptions(options);
  let corePromise: Promise<XbbgCoreLike> | undefined;
  let enginePromise: Promise<XbbgEngineLike> | undefined;

  async function cacheCoreImport(): Promise<XbbgCoreLike> {
    const promise = importCore();
    corePromise = promise;
    promise.catch(() => {
      if (corePromise === promise) {
        corePromise = undefined;
      }
    });
    return await promise;
  }

  async function cacheEngineConnect(): Promise<XbbgEngineLike> {
    const promise = (async (): Promise<XbbgEngineLike> => {
      const core = await getCore();
      return await core.connect(normalized.engineConfig);
    })();
    enginePromise = promise;
    promise.catch(() => {
      if (enginePromise === promise) {
        enginePromise = undefined;
      }
    });
    return await promise;
  }

  async function getCore(): Promise<XbbgCoreLike> {
    if (normalized.core !== undefined) {
      return normalized.core;
    }
    return await (corePromise ?? cacheCoreImport());
  }

  async function getEngine(): Promise<XbbgEngineLike> {
    if (normalized.engine !== undefined) {
      return normalized.engine;
    }
    return await (enginePromise ?? cacheEngineConnect());
  }

  return {
    getCore,
    getEngine,
    options: normalized,
  };
}
