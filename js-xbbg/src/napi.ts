import type {
  CdxTickerInfo,
  EngineConfig,
  EntitlementReport,
  ExchangeInfoResult,
  ExchangeOverrideInput,
  FieldInfo,
  FuturesCandidate,
  FxPairInfo,
  MarketRule,
  RequestInput,
  SeatType,
  SessionWindowsInfo,
  StringPair,
  SubscriptionStats,
  TickerParts,
  TimeRange,
} from './types';

export type NativeArrowColumnType =
  | 'bool'
  | 'binary'
  | 'date32'
  | 'date64'
  | 'float32'
  | 'float64'
  | 'int8'
  | 'int16'
  | 'int32'
  | 'int64'
  | 'large_binary'
  | 'large_utf8'
  | 'null'
  | 'time32_ms'
  | 'time32_s'
  | 'time64_us'
  | 'time64_ns'
  | 'timestamp_ms'
  | 'timestamp_ns'
  | 'timestamp_s'
  | 'timestamp_us'
  | 'uint8'
  | 'uint16'
  | 'uint32'
  | 'uint64'
  | 'utf8';

export interface NativeArrowColumn {
  readonly name: string;
  readonly type: NativeArrowColumnType;
  readonly nullable: boolean;
  readonly length: number;
  readonly nullCount: number;
  readonly timezone?: string;
  readonly data?: Buffer;
  readonly offsets?: Buffer;
  readonly nullBitmap?: Buffer;
}

export interface NativeArrowZeroCopyBatch {
  readonly kind: 'zeroCopy';
  readonly numRows: number;
  readonly columns: NativeArrowColumn[];
  readonly metadata: Record<string, string>;
}

// Scalar subscription rows use columnar, per-row typed option arrays instead of
// Serde_json values. Each fieldIndices entry marks a field as present. Exactly
// One typed array carries a concrete payload at that position; all-null slots
// Mean an explicit clear, while absent fields have no position. Int64/time64/
// Timestamp values are decimal strings so JS can materialize BigInt lazily.
export type NativeSubscriptionFieldKind =
  | 'unknown'
  | 'bool'
  | 'i32'
  | 'i64'
  | 'f64'
  | 'str'
  | 'date32'
  | 'time64_us'
  | 'timestamp_us';

export interface NativeSubscriptionLayout {
  readonly version: number;
  readonly fields: readonly string[];
  readonly kinds: readonly NativeSubscriptionFieldKind[];
}

export interface NativeSubscriptionRow {
  readonly topic: string;
  readonly topicId: number;
  readonly timestampUs: number;
  readonly layoutVersion: number;
  readonly fieldIndices: readonly number[];
  readonly boolValues: readonly (boolean | null)[];
  readonly i32Values: readonly (number | null)[];
  readonly f64Values: readonly (number | null)[];
  readonly stringValues: readonly (string | null)[];
  readonly i64Values: readonly (string | null)[];
}

export interface NativeSubscriptionUpdateBatch {
  readonly kind: 'batch';
  readonly layout?: NativeSubscriptionLayout;
  readonly updates: readonly NativeSubscriptionRow[];
}

export interface NativeSubscription {
  nextUpdates(maxItems?: number, maxWaitMs?: number): Promise<NativeSubscriptionUpdateBatch | null>;
  nextArrowBatch(maxRows?: number, maxWaitMs?: number): Promise<NativeArrowZeroCopyBatch | null>;
  add(tickers: readonly string[]): Promise<void>;
  remove(tickers: readonly string[]): Promise<void>;
  unsubscribe(drain: boolean): Promise<NativeSubscriptionUpdateBatch[] | null>;
  unsubscribeArrow(drain: boolean): Promise<NativeArrowZeroCopyBatch[] | null>;
  readonly tickers: string[];
  readonly fields: string[];
  readonly isActive: boolean;
  readonly stats: SubscriptionStats;
}

export interface NativeEngine {
  request(params: RequestInput): Promise<NativeArrowZeroCopyBatch>;
  requestRaw(params: RequestInput): Promise<Buffer>;
  seatType(): Promise<SeatType>;
  checkEntitlements(service: string, eids: readonly number[]): Promise<EntitlementReport>;
  identityIsAuthorized(service: string): Promise<boolean>;
  resolveFieldTypes(
    fields: readonly string[],
    overrides: readonly StringPair[] | undefined,
    defaultType: string,
  ): Promise<StringPair[]>;
  getFieldInfo(field: string): FieldInfo | null;
  clearFieldCache(): void;
  saveFieldCache(): void;
  validateFields(fields: readonly string[]): Promise<string[]>;
  isFieldValidationEnabled(): boolean;
  getSchema(service: string): Promise<string>;
  getOperation(service: string, operation: string): Promise<string>;
  listOperations(service: string): Promise<string[]>;
  getCachedSchema(service: string): string | null;
  invalidateSchema(service: string): void;
  clearSchemaCache(): void;
  listCachedSchemas(): string[];
  getEnumValues(service: string, operation: string, element: string): Promise<string[] | null>;
  listValidElements(service: string, operation: string): Promise<string[] | null>;
  subscribe(
    tickers: readonly string[],
    fields: readonly string[],
    allFields: boolean | undefined,
  ): Promise<NativeSubscription>;
  subscribeWithOptions(
    service: string,
    tickers: readonly string[],
    fields: readonly string[],
    options: readonly string[] | undefined,
    flushThreshold: number | undefined,
    overflowPolicy: string | undefined,
    streamCapacity: number | undefined,
    allFields: boolean | undefined,
  ): Promise<NativeSubscription>;
  signalShutdown(): void;
  isAvailable(): boolean;

  // Recipes
  recipeBqr(
    ticker: string,
    startDatetime: string | undefined,
    endDatetime: string | undefined,
    eventTypes: readonly string[] | null,
    includeBrokerCodes: boolean,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeYas(
    tickers: readonly string[],
    fields: readonly string[],
    settleDt: string | undefined,
    yieldType: number | undefined,
    spread: number | undefined,
    yieldVal: number | undefined,
    price: number | undefined,
    benchmark: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipePreferreds(
    equityTicker: string,
    fields: readonly string[] | null,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeCorporateBonds(
    ticker: string,
    ccy: string | undefined,
    fields: readonly string[] | null,
    activeOnly: boolean,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeFutTicker(
    genTicker: string,
    dt: string,
    freq: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeActiveFutures(
    genTicker: string,
    dt: string,
    freq: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeFuturesCurve(
    genTicker: string,
    asof: string | undefined,
    chainField: string | undefined,
    fields: readonly string[] | null,
    maxContracts: number | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeCdxTicker(
    genTicker: string,
    dt: string,
    versionless?: boolean,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeActiveCdx(
    genTicker: string,
    dt: string,
    lookbackDays: number | undefined,
    versionless?: boolean,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeDividend(
    tickers: readonly string[],
    startDate: string,
    endDate: string,
    dvdType: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeDividendYield(
    tickers: readonly string[],
    startDate: string,
    endDate: string,
    dividendTypes: readonly string[] | null,
    windowDays: number | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeTurnover(
    tickers: readonly string[],
    startDate: string,
    endDate: string,
    ccy: string | undefined,
    factor: number | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeEtfHoldings(
    etfTicker: string,
    fields: readonly string[] | null,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeVolSurface(
    tickers: readonly string[],
    startDate: string,
    endDate: string,
    presets: readonly string[] | null,
    fieldSpecs: readonly string[] | null,
    asDecimal: boolean | undefined,
    includeDerived: boolean | undefined,
    riskFreeRate: number | undefined,
    dividendYieldField: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeIndexMembers(
    index: string,
    field: string | undefined,
    asof: string | undefined,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeResolveIsins(isins: readonly string[]): Promise<NativeArrowZeroCopyBatch>;
  recipeIssuerIsins(bondIsins: readonly string[]): Promise<NativeArrowZeroCopyBatch>;
  recipeEtfNavRelationships(tickers: readonly string[]): Promise<NativeArrowZeroCopyBatch>;
  recipeEtfNavSnapshot(tickers: readonly string[]): Promise<NativeArrowZeroCopyBatch>;
  recipeEtfNavHistory(
    tickers: readonly string[],
    startDate: string,
    endDate: string,
  ): Promise<NativeArrowZeroCopyBatch>;
  recipeCurrencyConversion(
    ticker: string,
    targetCcy: string,
    startDate: string,
    endDate: string,
  ): Promise<NativeArrowZeroCopyBatch>;
}

export interface NativeEngineConstructor {
  new (host: string, port: number): NativeEngine;
  withConfig(config: EngineConfig): NativeEngine;
  /** Async factory: engine startup runs off the JS thread. */
  connect(host?: string, port?: number): Promise<NativeEngine>;
  /** Async factory from a full configuration. */
  connectWithConfig(config: EngineConfig): Promise<NativeEngine>;
}

export interface NativeAddon {
  JsEngine: NativeEngineConstructor;
  setLogLevel: (level: string) => void;
  getLogLevel: () => string;

  // Date utilities
  extParseDate: (dateStr: string) => number[];
  extFmtDate: (year: number, month: number, day: number, fmt?: string) => string;

  // Pivot utilities
  extPivotToWide: (ipcBuffer: Buffer) => Buffer;
  extIsLongFormat: (ipcBuffer: Buffer) => boolean;

  // Ticker utilities
  extParseTicker: (ticker: string) => TickerParts;
  extIsSpecificContract: (ticker: string) => boolean;
  extBuildFuturesTicker: (prefix: string, monthCode: string, year: string, asset: string) => string;
  extNormalizeTickers: (tickers: readonly string[]) => string[];
  extFilterEquityTickers: (tickers: readonly string[]) => string[];

  // Futures resolution
  extGenerateFuturesCandidates: (
    genTicker: string,
    year: number,
    month: number,
    day: number,
    freq?: string,
    count?: number,
  ) => FuturesCandidate[];
  extValidateGenericTicker: (ticker: string) => void;
  extContractIndex: (genTicker: string) => number;
  extFilterCandidatesByCycle: (
    candidates: readonly FuturesCandidate[],
    cycle: string,
  ) => FuturesCandidate[];
  extFilterValidContracts: (
    contracts: readonly StringPair[],
    year: number,
    month: number,
    day: number,
  ) => string[];

  // CDX resolution
  extParseCdxTicker: (ticker: string) => CdxTickerInfo;
  extPreviousCdxSeries: (ticker: string) => string | null;
  extCdxGenToSpecific: (genTicker: string, series: number) => string;

  // Currency utilities
  extBuildFxPair: (fromCcy: string, toCcy: string) => FxPairInfo;
  extSameCurrency: (ccy1: string, ccy2: string) => boolean;
  extCurrenciesNeedingConversion: (currencies: readonly string[], target: string) => string[];

  // Column renaming
  extRenameDividendColumns: (columns: readonly string[]) => StringPair[];
  extRenameEtfColumns: (columns: readonly string[]) => StringPair[];

  // Constants
  extGetMonthCode: (monthName: string) => string | null;
  extGetMonthName: (code: string) => string | null;
  extGetFuturesMonths: () => StringPair[];
  extGetDvdType: (typ: string) => string | null;
  extGetDvdTypes: () => StringPair[];
  extGetDvdCols: () => StringPair[];
  extGetEtfCols: () => StringPair[];

  // Fixed income / YAS
  extBuildYasOverrides: (
    settleDt?: string,
    yieldType?: number,
    spread?: number,
    yieldVal?: number,
    price?: number,
    benchmark?: string,
  ) => StringPair[];

  // Earnings utilities
  extBuildEarningHeaderRename: (
    headerRow: readonly StringPair[],
    dataColumns: readonly string[],
  ) => StringPair[];
  extCalculateLevelPercentages: (
    values: readonly (number | null)[],
    levels: readonly (number | null)[],
  ) => (number | null)[];

  // BQL query builders
  extBuildPreferredsQuery: (equityTicker: string, extraFields?: readonly string[]) => string;
  extBuildCorporateBondsQuery: (
    ticker: string,
    ccy?: string,
    extraFields?: readonly string[],
    activeOnly?: boolean,
  ) => string;
  extBuildEtfHoldingsQuery: (etfTicker: string, extraFields?: readonly string[]) => string;

  // DateTime defaults
  extDefaultTurnoverDates: (startDate?: string, endDate?: string) => TimeRange;
  extDefaultBqrDatetimes: (startDatetime?: string, endDatetime?: string) => TimeRange;

  // Markets — sessions & timezone
  extDeriveSessions: (
    dayStart: string,
    dayEnd: string,
    mic?: string,
    exchCode?: string,
  ) => SessionWindowsInfo;
  extGetMarketRule: (mic?: string, exchCode?: string) => MarketRule | null;
  extInferTimezone: (countryIso: string) => string | null;
  extSetExchangeOverride: (ticker: string, input: ExchangeOverrideInput) => void;
  extGetExchangeOverride: (ticker: string) => ExchangeInfoResult | null;
  extClearExchangeOverride: (ticker?: string) => void;
  extListExchangeOverrides: () => ExchangeInfoResult[];
  extSessionTimesToUtc: (
    startTime: string,
    endTime: string,
    exchangeTz: string,
    date: string,
  ) => TimeRange;
}
