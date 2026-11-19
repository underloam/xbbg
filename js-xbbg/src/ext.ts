import type { NativeAddon } from './napi';
import type { BdpOptions, CdxOptions } from './types';

export interface CdxHelpers {
  readonly acdx_info: (ticker: string, options?: BdpOptions) => Promise<unknown>;
  readonly acdx_pricing: (ticker: string, options?: CdxOptions) => Promise<unknown>;
  readonly acdx_risk: (ticker: string, options?: CdxOptions) => Promise<unknown>;
}

export interface ExtNamespace {
  readonly buildCorporateBondsQuery: NativeAddon['extBuildCorporateBondsQuery'];
  readonly buildEarningHeaderRename: NativeAddon['extBuildEarningHeaderRename'];
  readonly buildEtfHoldingsQuery: NativeAddon['extBuildEtfHoldingsQuery'];
  readonly buildFuturesTicker: NativeAddon['extBuildFuturesTicker'];
  readonly buildFxPair: NativeAddon['extBuildFxPair'];
  readonly buildPreferredsQuery: NativeAddon['extBuildPreferredsQuery'];
  readonly buildYasOverrides: NativeAddon['extBuildYasOverrides'];
  readonly calculateLevelPercentages: NativeAddon['extCalculateLevelPercentages'];
  readonly cdx: Readonly<CdxHelpers>;
  readonly cdxGenToSpecific: NativeAddon['extCdxGenToSpecific'];
  readonly clearExchangeOverride: NativeAddon['extClearExchangeOverride'];
  readonly contractIndex: NativeAddon['extContractIndex'];
  readonly currenciesNeedingConversion: NativeAddon['extCurrenciesNeedingConversion'];
  readonly defaultBqrDatetimes: NativeAddon['extDefaultBqrDatetimes'];
  readonly defaultTurnoverDates: NativeAddon['extDefaultTurnoverDates'];
  readonly deriveSessions: NativeAddon['extDeriveSessions'];
  readonly filterCandidatesByCycle: NativeAddon['extFilterCandidatesByCycle'];
  readonly filterEquityTickers: NativeAddon['extFilterEquityTickers'];
  readonly filterValidContracts: NativeAddon['extFilterValidContracts'];
  readonly fmtDate: NativeAddon['extFmtDate'];
  readonly generateFuturesCandidates: NativeAddon['extGenerateFuturesCandidates'];
  readonly getDvdCols: NativeAddon['extGetDvdCols'];
  readonly getDvdType: NativeAddon['extGetDvdType'];
  readonly getDvdTypes: NativeAddon['extGetDvdTypes'];
  readonly getEtfCols: NativeAddon['extGetEtfCols'];
  readonly getExchangeOverride: NativeAddon['extGetExchangeOverride'];
  readonly getFuturesMonths: NativeAddon['extGetFuturesMonths'];
  readonly getMarketRule: NativeAddon['extGetMarketRule'];
  readonly getMonthCode: NativeAddon['extGetMonthCode'];
  readonly getMonthName: NativeAddon['extGetMonthName'];
  readonly inferTimezone: NativeAddon['extInferTimezone'];
  readonly isLongFormat: NativeAddon['extIsLongFormat'];
  readonly isSpecificContract: NativeAddon['extIsSpecificContract'];
  readonly listExchangeOverrides: NativeAddon['extListExchangeOverrides'];
  readonly normalizeTickers: NativeAddon['extNormalizeTickers'];
  readonly parseCdxTicker: NativeAddon['extParseCdxTicker'];
  readonly parseDate: NativeAddon['extParseDate'];
  readonly parseTicker: NativeAddon['extParseTicker'];
  readonly pivotToWide: NativeAddon['extPivotToWide'];
  readonly previousCdxSeries: NativeAddon['extPreviousCdxSeries'];
  readonly renameDividendColumns: NativeAddon['extRenameDividendColumns'];
  readonly renameEtfColumns: NativeAddon['extRenameEtfColumns'];
  readonly sameCurrency: NativeAddon['extSameCurrency'];
  readonly sessionTimesToUtc: NativeAddon['extSessionTimesToUtc'];
  readonly setExchangeOverride: NativeAddon['extSetExchangeOverride'];
  readonly validateGenericTicker: NativeAddon['extValidateGenericTicker'];
}

/** Build the frozen `ext` utility namespace backed by the native addon. */
export function createExt(native: NativeAddon, cdx: CdxHelpers): ExtNamespace {
  return Object.freeze({
    buildCorporateBondsQuery: native.extBuildCorporateBondsQuery,
    buildEarningHeaderRename: native.extBuildEarningHeaderRename,
    buildEtfHoldingsQuery: native.extBuildEtfHoldingsQuery,
    buildFuturesTicker: native.extBuildFuturesTicker,
    buildFxPair: native.extBuildFxPair,
    buildPreferredsQuery: native.extBuildPreferredsQuery,
    buildYasOverrides: native.extBuildYasOverrides,
    calculateLevelPercentages: native.extCalculateLevelPercentages,
    cdx: Object.freeze(cdx),
    cdxGenToSpecific: native.extCdxGenToSpecific,
    clearExchangeOverride: native.extClearExchangeOverride,
    contractIndex: native.extContractIndex,
    currenciesNeedingConversion: native.extCurrenciesNeedingConversion,
    defaultBqrDatetimes: native.extDefaultBqrDatetimes,
    defaultTurnoverDates: native.extDefaultTurnoverDates,
    deriveSessions: native.extDeriveSessions,
    filterCandidatesByCycle: native.extFilterCandidatesByCycle,
    filterEquityTickers: native.extFilterEquityTickers,
    filterValidContracts: native.extFilterValidContracts,
    fmtDate: native.extFmtDate,
    generateFuturesCandidates: native.extGenerateFuturesCandidates,
    getDvdCols: native.extGetDvdCols,
    getDvdType: native.extGetDvdType,
    getDvdTypes: native.extGetDvdTypes,
    getEtfCols: native.extGetEtfCols,
    getExchangeOverride: native.extGetExchangeOverride,
    getFuturesMonths: native.extGetFuturesMonths,
    getMarketRule: native.extGetMarketRule,
    getMonthCode: native.extGetMonthCode,
    getMonthName: native.extGetMonthName,
    inferTimezone: native.extInferTimezone,
    isLongFormat: native.extIsLongFormat,
    isSpecificContract: native.extIsSpecificContract,
    listExchangeOverrides: native.extListExchangeOverrides,
    normalizeTickers: native.extNormalizeTickers,
    parseCdxTicker: native.extParseCdxTicker,
    parseDate: native.extParseDate,
    parseTicker: native.extParseTicker,
    pivotToWide: native.extPivotToWide,
    previousCdxSeries: native.extPreviousCdxSeries,
    renameDividendColumns: native.extRenameDividendColumns,
    renameEtfColumns: native.extRenameEtfColumns,
    sameCurrency: native.extSameCurrency,
    sessionTimesToUtc: native.extSessionTimesToUtc,
    setExchangeOverride: native.extSetExchangeOverride,
    validateGenericTicker: native.extValidateGenericTicker,
  });
}
