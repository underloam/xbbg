import type {
  IndexMemberField,
  OverflowPolicy,
  SdkLogLevel,
  ValidationMode,
  VolSurfacePreset,
} from './_defs_gen';

/**
 * Date-like input accepted by xbbg JS surfaces (issue #317).
 *
 * Mirrors the Python `DateLike` alias and the JS half of the date-acceptance
 * matrix described in the issue:
 *
 * - `Date` — JavaScript Date (treated as a calendar date in UTC; the time
 *   portion is ignored when formatting to `YYYYMMDD`).
 * - `string` — ISO 8601 (`"2023-01-17"`, `"2023-01-17T10:30:00"`,
 *   `"2023-01-17T10:30:00-05:00"`) or Bloomberg-native (`"20230117"`).
 *   Ambiguous formats like `"01/17/2023"` are rejected.
 * - `number` — epoch milliseconds.
 * - duck-typed Luxon `DateTime` — anything implementing `toJSDate()`.
 */
export type DateLike = Date | string | number | { toJSDate: () => Date };

/**
 * Datetime-like input. Same shape as `DateLike` today; named separately so the
 * call sites (`startDatetime`, `endDatetime`, `dt` on intraday surfaces) can
 * read clearly.
 */
export type DateTimeLike = DateLike;

export interface StringPair {
  key: string;
  value: string;
}

export type SeatType = 'BPS' | 'NONBPS' | 'INVALID';

export interface EntitlementReport {
  entitled: boolean;
  failedEids: number[];
}

export interface BloombergMetadataError {
  category?: string;
  code?: string | number;
  subcategory?: string;
  message?: string;
}

export interface BloombergFieldException extends BloombergMetadataError {
  field?: string;
}

export interface ResultMetadata {
  metadata: Record<string, string>;
  eidData?: Record<string, number[]>;
  securityErrors?: Record<string, BloombergMetadataError>;
  fieldExceptions?: Record<string, BloombergFieldException[]>;
}

export interface ServerAddress {
  host: string;
  port: number;
}

export type AuthConfig =
  | { method: 'user' }
  | { method: 'app'; appName: string }
  | { method: 'userapp'; appName: string }
  | { method: 'dir' | 'directory'; dirProperty: string }
  | { method: 'manual'; appName: string; userId: string; ipAddress: string }
  | { method: 'token'; token: string };

export interface TlsConfig {
  clientCredentials?: string;
  clientCredentialsPassword?: string;
  trustMaterial?: string;
  handshakeTimeoutMs?: number;
  crlFetchTimeoutMs?: number;
}

export interface RetryPolicy {
  maxRetries?: number;
  initialDelayMs?: number;
  backoffFactor?: number;
  maxDelayMs?: number;
}

export interface Socks5Config {
  host: string;
  port: number;
}

export interface EngineConfig {
  host?: string;
  port?: number;
  servers?: ServerAddress[];
  zfpRemote?: '8194' | '8196';
  requestPoolSize?: number;
  subscriptionPoolSize?: number;
  /** Tokio runtime worker threads shared by the engine. Default 2; must be nonzero. */
  runtimeWorkerThreads?: number;
  /** Maximum live subscription sessions. Default 32; must be at least subscriptionPoolSize. */
  maxSubscriptionSessions?: number;
  /** Enable request sharding for eligible multi-security bdp/bdh requests. Default false. */
  shardRequests?: boolean;
  /** Minimum securities before request sharding applies. Default 20. */
  shardThreshold?: number;
  /** Maximum securities per sharded request. Default 16. */
  shardChunkSize?: number;
  /** Maximum concurrent shard requests per user request. Default 4. */
  shardMaxConcurrent?: number;
  /** Field validation before a request is sent. Default `'disabled'`. */
  validationMode?: ValidationMode;
  subscriptionFlushThreshold?: number;
  maxEventQueueSize?: number;
  commandQueueSize?: number;
  subscriptionStreamCapacity?: number;
  /** What a subscription does when a consumer stalls. Default `'drop_newest'`. */
  overflowPolicy?: OverflowPolicy;
  warmupServices?: string[];
  fieldCachePath?: string;
  auth?: AuthConfig;
  tls?: TlsConfig;
  numStartAttempts?: number;
  autoRestartOnDisconnection?: boolean;
  retryPolicy?: RetryPolicy;
  /** Hard per-request timeout in ms; 0 disables. Default 0. */
  requestTimeoutMs?: number;
  /** Warn threshold for streams staying deactivated, in ms. 0 disables. Default 30000. */
  streamsDeactivatedWarnMs?: number;
  /** Enable BLPAPI keep-alive pings. SDK default: true. */
  keepAliveEnabled?: boolean;
  /** Milliseconds of inactivity before keep-alive ping. SDK default: 20000. */
  keepAliveInactivityMs?: number;
  /** Milliseconds to wait for a keep-alive response. SDK default: 10000. */
  keepAliveResponseTimeoutMs?: number;
  /** Slow-consumer hi water mark as fraction of maxEventQueueSize. SDK default: 0.75. */
  slowConsumerHiWaterMark?: number;
  /** Slow-consumer lo water mark as fraction of maxEventQueueSize. SDK default: 0.5. */
  slowConsumerLoWaterMark?: number;
  /** Verbosity of the Bloomberg C SDK's own logging. Default `'off'`. */
  sdkLogLevel?: SdkLogLevel;
  socks5?: Socks5Config;
}

export interface SubscriptionReadOptions {
  /**
   * Cancelling closes the subscription, then rejects with the abort reason.
   * Cleanup failure produces an AggregateError containing both errors.
   */
  readonly signal?: AbortSignal;
}

export interface RequestInput {
  service: string;
  operation: string;
  requestOperation?: string;
  requestId?: string;
  extractor?: string;
  securities?: readonly string[];
  security?: string;
  fields?: readonly string[];
  overrides?: OverridesInput;
  elements?: readonly StringPair[];
  kwargs?: readonly StringPair[];
  jsonElements?: string;
  startDate?: string;
  endDate?: string;
  startDatetime?: string;
  endDatetime?: string;
  requestTz?: string;
  outputTz?: string;
  eventType?: string;
  eventTypes?: readonly string[];
  interval?: number;
  options?: readonly StringPair[];
  fieldTypes?: readonly StringPair[];
  includeSecurityErrors?: boolean;
  returnEids?: boolean;
  validateFields?: boolean;
  searchSpec?: string;
  fieldIds?: readonly string[];
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface SubscriptionStats {
  messagesReceived: number;
  droppedBatches: number;
  batchesSent: number;
  slowConsumer: boolean;
}

export interface FieldInfo {
  fieldId: string;
  arrowType: string;
  description: string;
  category: string;
}

export type PrimitiveValue = string | number | boolean;
export type OverridesMap = Record<string, PrimitiveValue>;
export type OverrideValue = PrimitiveValue | Date | { toJSDate: () => Date };
export interface OverrideObject {
  readonly [key: string]: OverrideValue | OverrideNestedSource;
}
export type OverrideNestedSource = OverrideObject | OverrideSpecLike | readonly OverrideEntry[];
export interface SecurityOverrideSpec {
  readonly security: string;
  readonly overrides: readonly StringPair[];
}
export interface OverrideSpecLike {
  readonly pairs: readonly StringPair[];
  readonly securityOverrides: readonly SecurityOverrideSpec[];
  toPairs(): StringPair[];
  toObject(): OverridesMap;
  toSecurityOverrides(): SecurityOverrideSpec[];
  merge(...sources: OverrideSource[]): OverrideSpecLike;
  forSecurity(security: string, ...sources: OverrideSource[]): OverrideSpecLike;
}
export type OverrideEntry =
  | { readonly key: string; readonly value: OverrideValue | OverrideNestedSource }
  | readonly [string, OverrideValue | OverrideNestedSource];
export type OverrideSource = OverrideObject | OverrideSpecLike | readonly OverrideEntry[];
export type OverridesInput = OverrideSource;

export interface BdpOptions {
  overrides?: OverridesInput;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
  includeSecurityErrors?: boolean;
  returnEids?: boolean;
  validateFields?: boolean;
}

export interface BdhOptions {
  start?: DateLike;
  end?: DateLike;
  overrides?: OverridesInput;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
  returnEids?: boolean;
  validateFields?: boolean;
}

export interface BdibOptions {
  start?: DateTimeLike;
  end?: DateTimeLike;
  requestTz?: string;
  outputTz?: string;
  eventType?: string;
  interval?: number;
  kwargs?: OverridesMap;
  backend?: BackendKind;
  returnEids?: boolean;
}

export interface BdtickOptions {
  start?: DateTimeLike;
  end?: DateTimeLike;
  requestTz?: string;
  outputTz?: string;
  eventTypes?: readonly string[];
  includeConditionCodes?: boolean;
  includeExchangeCodes?: boolean;
  includeBrokerCodes?: boolean;
  includeRpsCodes?: boolean;
  includeBicMicCodes?: boolean;
  includeNonPlottableEvents?: boolean;
  includeBloombergStandardConditionCodes?: boolean;
  kwargs?: OverridesMap;
  backend?: BackendKind;
  returnEids?: boolean;
}

export interface CdxOptions extends BdpOptions {
  /** Recovery rate in percent (40 means 40%). */
  recoveryRate?: number;
  /** Recovery rate in percent (40 means 40%). */
  recovery_rate?: number;
}

export interface BqlOptions {
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface BeqsOptions {
  asof?: DateLike;
  screenType?: string;
  group?: string;
  overrides?: OverridesInput;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface BsrchOptions {
  overrides?: OverridesInput;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface BtaOptions {
  studyParams?: OverridesMap;
  kwargs?: OverridesMap;
  startDate?: DateLike;
  endDate?: DateLike;
  start_date?: DateLike;
  end_date?: DateLike;
  periodicity?: string;
  interval?: number;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface BfldsOptions {
  fields?: string | readonly string[];
  searchSpec?: string;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface BlkpOptions {
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface RequestOptions {
  overrides?: OverridesInput;
  kwargs?: OverridesMap;
  format?: RequestFormat;
  backend?: BackendKind;
}

export interface StreamOptions {
  options?: readonly string[];
  conflate?: boolean;
  flushThreshold?: number;
  overflowPolicy?: OverflowPolicy;
  streamCapacity?: number;
  allFields?: boolean;
  fields?: readonly string[];
  /** Upstream topic -> consumer label (used by rows, status, tickers and remove). */
  aliases?: Record<string, string>;
  /** Warn once per topic (default), reject delayed topics, or only record the flag. */
  onDelayed?: 'warn' | 'raise' | 'ignore';
  /** Opt out of engine-local market-data feed sharing. Default false. */
  isolated?: boolean;
  /** Keep a materialized image without queuing rows. Default true. */
  rows?: boolean;
  /** Rejected explicit fields: warn (default), fail the topic, or only record errors. */
  onFieldError?: 'warn' | 'raise' | 'ignore';
  /** Numeric zero sentinels to materialize as null in latest() only. */
  zeroAsNull?: readonly string[];
}

export interface SubscriptionEvent {
  atUs: number;
  category: string;
  level: string;
  messageType: string;
  topic?: string | null;
  detail?: string | null;
}

export interface SubscriptionFailure {
  topic: string;
  reason: string;
  kind: string;
  atUs: number;
}

export interface TopicState {
  topic: string;
  feedTopic: string;
  delayed?: boolean | null;
  state: string;
  lastChangeUs: number;
  streamsActive: boolean;
  streamsChangedUs: number;
}

export interface SessionStatus {
  state: string;
  lastChangeUs: number;
  disconnectCount: number;
  reconnectCount: number;
}

export interface ServiceStatus {
  service: string;
  up: boolean;
  lastChangeUs: number;
}

export interface AdminStatus {
  slowConsumerWarningActive: boolean;
  slowConsumerWarningCount: number;
  slowConsumerClearedCount: number;
  dataLossCount: number;
  lastWarningUs?: number | null;
  lastClearedUs?: number | null;
  lastDataLossUs?: number | null;
}

export interface SubscriptionStatus {
  events: SubscriptionEvent[];
  failures: SubscriptionFailure[];
  failedTickers: string[];
  topicStates: Record<string, TopicState>;
  fieldErrors: Record<string, Record<string, string>>;
  session: SessionStatus;
  services: Record<string, ServiceStatus>;
  admin: AdminStatus;
}

export interface FeedInfo {
  service: string;
  topic: string;
  options: string[];
  fields: string[];
  consumers: number;
  delayed?: boolean | null;
  state: string;
  isolated: boolean;
  fieldErrors: Record<string, string>;
}

export interface ResolveVenuesOptions extends RecipeBackendOptions {
  pcsOverrides?: Record<string, string>;
}

export interface AuctionSnapshotOptions extends ResolveVenuesOptions {
  fields?: readonly string[];
}

export interface AuctionStreamOptions extends Omit<StreamOptions, 'aliases'> {
  pcsOverrides?: Record<string, string>;
}

export interface BqrOptions {
  startDatetime?: DateTimeLike;
  endDatetime?: DateTimeLike;
  eventTypes?: readonly string[];
  includeBrokerCodes?: boolean;
  backend?: BackendKind;
}

export interface YasOptions {
  settleDt?: DateLike;
  yieldType?: number;
  spread?: number;
  yieldVal?: number;
  price?: number;
  benchmark?: string;
  backend?: BackendKind;
}

export interface PreferredsOptions {
  fields?: readonly string[];
  backend?: BackendKind;
}

export interface CorporateBondsOptions {
  ccy?: string;
  fields?: readonly string[];
  backend?: BackendKind;
}

export interface FuturesResolveOptions {
  freq?: string;
  backend?: BackendKind;
}

export interface FuturesCurveOptions {
  asof?: DateLike;
  chainField?: string;
  fields?: readonly string[];
  maxContracts?: number;
  backend?: BackendKind;
}

export interface CdxResolveOptions extends RecipeBackendOptions {
  versionless?: boolean;
}

export interface ActiveCdxOptions extends CdxResolveOptions {
  lookbackDays?: number;
}

export interface DividendOptions {
  dvdType?: string;
  backend?: BackendKind;
}

export interface DividendYieldOptions {
  dividendTypes?: readonly string[];
  windowDays?: number;
  backend?: BackendKind;
}

export interface TurnoverOptions {
  ccy?: string;
  factor?: number;
  backend?: BackendKind;
}

export interface EtfHoldingsOptions {
  fields?: readonly string[];
  backend?: BackendKind;
}

export interface VolFieldSpec {
  metric?: string;
  tenor?: string;
  pointType?: string;
  point?: number;
}

export interface VolSurfaceOptions {
  preset?: VolSurfacePreset | readonly VolSurfacePreset[] | null;
  fields?: readonly string[] | Record<string, VolFieldSpec>;
  asDecimal?: boolean;
  includeDerived?: boolean;
  riskFreeRate?: number;
  dividendYieldField?: string;
  backend?: BackendKind;
}

export interface IndexMembersOptions {
  field?: IndexMemberField;
  asof?: DateLike;
  backend?: BackendKind;
}

export interface RecipeBackendOptions {
  backend?: BackendKind;
}

export interface TimeRange {
  start: string;
  end: string;
}

export interface TickerParts {
  prefix: string;
  index: number;
  asset: string;
  exchange?: string;
}

export interface FuturesCandidate {
  ticker: string;
  year: number;
  month: number;
}

export interface CdxTickerInfo {
  index: string;
  series: string;
  version?: number;
  tenor: string;
  asset: string;
  isGeneric: boolean;
  seriesNum?: number;
}

export interface FxPairInfo {
  fxPair: string;
  factor: number;
  fromCcy: string;
  toCcy: string;
}

export interface SessionWindowsInfo {
  day?: TimeRange;
  allday?: TimeRange;
  pre?: TimeRange;
  post?: TimeRange;
  am?: TimeRange;
  pm?: TimeRange;
}

export interface MarketRule {
  preMinutes: number;
  postMinutes: number;
  lunchStartMin?: number;
  lunchEndMin?: number;
  isContinuous: boolean;
}

export interface ExchangeInfoResult {
  ticker: string;
  mic?: string;
  exchCode?: string;
  timezone: string;
  utcOffset?: number;
  source: string;
  day?: TimeRange;
  allday?: TimeRange;
  pre?: TimeRange;
  post?: TimeRange;
  am?: TimeRange;
  pm?: TimeRange;
}

export interface ExchangeOverrideInput {
  timezone?: string;
  mic?: string;
  exchCode?: string;
  day?: TimeRange;
  allday?: TimeRange;
  pre?: TimeRange;
  post?: TimeRange;
  am?: TimeRange;
  pm?: TimeRange;
}

export type BackendKind = 'arrow' | 'json' | 'polars';

// Shared Python/JS vocabulary is generated from defs/bloomberg.toml.
export type {
  FormatKind,
  OverflowPolicy,
  SdkLogLevel,
  ValidationMode,
  VolSurfacePreset,
} from './_defs_gen';

/**
 * Output format for a request.
 *
 * Canonical values are `long` (default), `long_typed`, `long_metadata` and
 * `semi_long`; the engine also accepts the legacy aliases `typed`, `metadata`,
 * `with_metadata` and `wide`. Deliberately `string` rather than a union so
 * callers threading a computed value keep compiling — {@link FormatKind} is the
 * canonical closed set, and `FORMAT_VALUES` in `_defs_gen.ts` is the generated
 * list to quote in messages.
 */
export type RequestFormat = string;
