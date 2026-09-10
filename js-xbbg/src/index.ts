/* oxlint-disable import/max-dependencies -- public entry point intentionally consolidates native and helper modules. */
import type { Table } from 'apache-arrow';

import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import { tableFromNativeArrowBatch } from './arrow-zero-copy';
import { Backend, Format } from './backends';
// Date / datetime helpers (#317): isolated module so they can be tested
// Without loading the native NAPI addon. Re-exported as public API below.
import { formatDate, formatDateTime, hasToJSDate } from './dates';
import {
  BlpError,
  BlpLimitError,
  BlpInternalError,
  BlpRequestError,
  BlpSubscriptionDataLossError,
  BlpSessionError,
  BlpTimeoutError,
  BlpValidationError,
  wrapError,
} from './errors';
import type {
  NativeAddon,
  NativeArrowZeroCopyBatch,
  NativeEngine,
  NativeSubscription,
  NativeSubscriptionFieldKind,
  NativeSubscriptionLayout,
  NativeSubscriptionRow,
  NativeSubscriptionUpdateBatch,
} from './napi';
import { resolveNativeAddon } from './native/resolve-native';
import { configureRuntimeSearchPath } from './runtime-search-path';
import type {
  ActiveCdxOptions,
  AuthConfig,
  BackendKind,
  BdhOptions,
  BdibOptions,
  BdpOptions,
  BdtickOptions,
  BeqsOptions,
  BfldsOptions,
  BlkpOptions,
  BloombergFieldException,
  BloombergMetadataError,
  BqlOptions,
  BqrOptions,
  BsrchOptions,
  BtaOptions,
  CdxOptions,
  CdxResolveOptions,
  CdxTickerInfo,
  CorporateBondsOptions,
  DateLike,
  DateTimeLike,
  DividendOptions,
  DividendYieldOptions,
  EngineConfig,
  EntitlementReport,
  EtfHoldingsOptions,
  ExchangeInfoResult,
  ExchangeOverrideInput,
  FieldInfo,
  FormatKind,
  FuturesCandidate,
  FuturesResolveOptions,
  FuturesCurveOptions,
  FxPairInfo,
  MarketRule,
  OverridesMap,
  OverrideEntry,
  OverrideNestedSource,
  OverrideObject,
  OverrideSource,
  OverrideSpecLike,
  OverrideValue,
  OverridesInput,
  IndexMembersOptions,
  PreferredsOptions,
  PrimitiveValue,
  RecipeBackendOptions,
  RequestInput,
  RequestOptions,
  ResultMetadata,
  SeatType,
  ServerAddress,
  SecurityOverrideSpec,
  SessionWindowsInfo,
  Socks5Config,
  StreamOptions,
  StringPair,
  SubscriptionReadOptions,
  SubscriptionStats,
  TickerParts,
  TimeRange,
  TlsConfig,
  TurnoverOptions,
  VolFieldSpec,
  VolSurfaceOptions,
  VolSurfacePreset,
  YasOptions,
} from './types';

const nodeRequire = createRequire(__filename);

interface PackageJsonShape {
  readonly version: string;
}

interface PolarsModule {
  readIPC(buffer: Buffer): unknown;
}

function parsePackageJsonShape(value: unknown): PackageJsonShape {
  if (isPlainObject(value) && typeof value.version === 'string') {
    return { version: value.version };
  }
  throw new TypeError('@xbbg/core package.json is missing a string version field');
}

function isNativeAddon(value: unknown): value is NativeAddon {
  return (
    isPlainObject(value) &&
    typeof value.JsEngine === 'function' &&
    typeof value.getLogLevel === 'function' &&
    typeof value.setLogLevel === 'function'
  );
}

function requireNativeAddon(modulePath: string): NativeAddon {
  const loaded: unknown = nodeRequire(modulePath);
  if (isNativeAddon(loaded)) {
    return loaded;
  }
  throw new TypeError(`Native addon ${modulePath} does not expose the expected @xbbg/core surface`);
}

function isPolarsModule(value: unknown): value is PolarsModule {
  return isPlainObject(value) && typeof value.readIPC === 'function';
}

function requirePolarsModule(): PolarsModule {
  const loaded: unknown = nodeRequire('nodejs-polars');
  if (isPolarsModule(loaded)) {
    return loaded;
  }
  throw new TypeError('nodejs-polars did not expose readIPC(buffer)');
}

function isBdhOptionsInput(value: DateLike | BdhOptions | undefined): value is BdhOptions {
  return isPlainObject(value) && !(value instanceof Date) && !hasToJSDate(value);
}

function isBdibOptionsInput(
  value: DateTimeLike | BdibOptions | number | undefined,
): value is BdibOptions {
  return isPlainObject(value) && !(value instanceof Date) && !hasToJSDate(value);
}

const packageJson = parsePackageJsonShape(nodeRequire('../package.json'));

configureRuntimeSearchPath();

function loadNative(): NativeAddon {
  const root = path.resolve(__dirname, '..', '..');

  const candidates = [
    path.join(__dirname, 'napi_xbbg.node'),
    path.join(__dirname, '..', 'napi_xbbg.node'),
    path.join(__dirname, 'napi-xbbg.node'),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return requireNativeAddon(candidate);
    }
  }

  const { key, packageName, binaryPath } = resolveNativeAddon(root);
  if (binaryPath !== null) {
    return requireNativeAddon(binaryPath);
  }
  if (packageName === null) {
    throw new Error(
      `No packaged @xbbg/core native addon is available for ${key}. Build it locally with "npm run build" from js-xbbg.`,
    );
  }

  throw new Error(
    `Unable to load native napi-xbbg module for ${key}. Install ${packageName} via Bun/npm, or build it locally with "npm run build" from js-xbbg.`,
  );
}

const native = loadNative();

// ── Constants ───────────────────────────────────────────────────────────
export { Backend, Format };

export const CDX_INFO_FIELDS = Object.freeze([
  'ROLLING_SERIES',
  'VERSION',
  'ON_THE_RUN_CURRENT_BD_INDICATOR',
  'CDS_FIRST_ACCRUAL_START_DATE',
  'NAME',
  'NUM_CURRENT_COMPANIES_CCY_TKR',
  'NUM_ORIG_COMPANIES_CRNCY_TKR',
  'PX_LAST',
]);

export const CDX_PRICING_FIELDS = Object.freeze([
  'PX_LAST',
  'PX_BID',
  'PX_ASK',
  'UPFRONT_LAST',
  'UPFRONT_BID',
  'UPFRONT_ASK',
  'CDS_FLAT_SPREAD',
  'UPFRONT_FEE',
  'PV_CDS_PREMIUM_LEG',
  'PV_CDS_DEFAULT_LEG',
]);

export const CDX_RISK_FIELDS = Object.freeze([
  'SW_CNV_BPV',
  'SW_EQV_BPV',
  'CDS_SPREAD_MID_MODIFIED_DURATION',
  'CDS_SPREAD_MID_CONVEXITY',
  'RECOVERY_RATE_SEN',
  'CDS_RECOVERY_RT',
]);

const TA_STUDIES: Readonly<Record<string, string>> = Object.freeze({
  ado: 'adoStudyAttributes',
  adx: 'dmiStudyAttributes',
  al: 'alStudyAttributes',
  atr: 'atrStudyAttributes',
  bb: 'bollStudyAttributes',
  boll: 'bollStudyAttributes',
  bs: 'bsStudyAttributes',
  chko: 'chkoStudyAttributes',
  cmci: 'cmciStudyAttributes',
  dmi: 'dmiStudyAttributes',
  ema: 'emavgStudyAttributes',
  emavg: 'emavgStudyAttributes',
  etd: 'etdStudyAttributes',
  fear_greed: 'fgStudyAttributes',
  fg: 'fgStudyAttributes',
  goc: 'gocStudyAttributes',
  hurst: 'hurstStudyAttributes',
  ichimoku: 'gocStudyAttributes',
  ipmavg: 'ipmavgStudyAttributes',
  keltner: 'kltnStudyAttributes',
  kltn: 'kltnStudyAttributes',
  macd: 'macdStudyAttributes',
  mae: 'maeStudyAttributes',
  mao: 'maoStudyAttributes',
  maxmin: 'maxminStudyAttributes',
  mom: 'momentumStudyAttributes',
  momentum: 'momentumStudyAttributes',
  or: 'orStudyAttributes',
  parabolic: 'ptpsStudyAttributes',
  pcr: 'pcrStudyAttributes',
  pd: 'pdStudyAttributes',
  pivot: 'pivotStudyAttributes',
  ptps: 'ptpsStudyAttributes',
  rex: 'rexStudyAttributes',
  roc: 'rocStudyAttributes',
  rsi: 'rsiStudyAttributes',
  rv: 'rvStudyAttributes',
  sar: 'ptpsStudyAttributes',
  sma: 'smavgStudyAttributes',
  smavg: 'smavgStudyAttributes',
  stoch: 'tasStudyAttributes',
  tas: 'tasStudyAttributes',
  te: 'teStudyAttributes',
  tma: 'tmavgStudyAttributes',
  tmavg: 'tmavgStudyAttributes',
  trender: 'trenderStudyAttributes',
  tvat: 'tvatStudyAttributes',
  vat: 'vatStudyAttributes',
  vma: 'vmavgStudyAttributes',
  vmavg: 'vmavgStudyAttributes',
  williams: 'wlprStudyAttributes',
  wlpr: 'wlprStudyAttributes',
  wma: 'wmavgStudyAttributes',
  wmavg: 'wmavgStudyAttributes',
});

type StudyParams = Record<string, PrimitiveValue | undefined>;

const TA_DEFAULTS: Readonly<Record<string, Readonly<StudyParams>>> = Object.freeze({
  atrStudyAttributes: Object.freeze({
    maType: 'Simple',
    period: 14,
    priceSourceHigh: 'PX_HIGH',
    priceSourceLow: 'PX_LOW',
    priceSourceClose: 'PX_LAST',
  }),
  bollStudyAttributes: Object.freeze({
    period: 20,
    upperBand: 2.0,
    lowerBand: 2.0,
    priceSourceClose: 'PX_LAST',
  }),
  dmiStudyAttributes: Object.freeze({
    period: 14,
    priceSourceHigh: 'PX_HIGH',
    priceSourceLow: 'PX_LOW',
    priceSourceClose: 'PX_LAST',
  }),
  emavgStudyAttributes: Object.freeze({ period: 20, priceSourceClose: 'PX_LAST' }),
  macdStudyAttributes: Object.freeze({
    maPeriod1: 12,
    maPeriod2: 26,
    sigPeriod: 9,
    priceSourceClose: 'PX_LAST',
  }),
  rsiStudyAttributes: Object.freeze({ period: 14, priceSourceClose: 'PX_LAST' }),
  smavgStudyAttributes: Object.freeze({ period: 20, priceSourceClose: 'PX_LAST' }),
  tasStudyAttributes: Object.freeze({
    periodK: 14,
    periodD: 3,
    periodDS: 3,
    periodDSS: 3,
    priceSourceHigh: 'PX_HIGH',
    priceSourceLow: 'PX_LOW',
    priceSourceClose: 'PX_LAST',
  }),
  tmavgStudyAttributes: Object.freeze({ period: 20, priceSourceClose: 'PX_LAST' }),
  vmavgStudyAttributes: Object.freeze({ period: 20, priceSourceClose: 'PX_LAST' }),
  wmavgStudyAttributes: Object.freeze({ period: 20, priceSourceClose: 'PX_LAST' }),
});

const MKTDATA_SERVICE = '//blp/mktdata';

// ── Helpers ─────────────────────────────────────────────────────────────

function toArrowTableFromNative(batch: NativeArrowZeroCopyBatch): Table & ResultMetadata {
  return attachResultMetadata(tableFromNativeArrowBatch(batch), batch.metadata);
}

const ETF_NAV_RELATIONSHIP_NOT_ONE_TO_ONE =
  'ETF NAV relationship result is not one-to-one with requested ETFs';

function firstSeenDuplicates(values: readonly string[]): string[] {
  const seen = new Set<string>();
  const duplicates: string[] = [];
  for (const value of values) {
    if (seen.has(value)) {
      if (!duplicates.includes(value)) {
        duplicates.push(value);
      }
    } else {
      seen.add(value);
    }
  }
  return duplicates;
}

/**
 * Validate an ETF NAV relationship table and return one iNAV ticker per ETF.
 *
 * Enforces one row per `input_order` with exact ordered `etf_ticker`
 * identity, no iNAV validation errors, no missing iNAV relationships, and an
 * unambiguous iNAV reverse mapping — all before any subscription is opened.
 */
function validatedInavTickers(etfList: readonly string[], table: Table): string[] {
  const malformed = (): BlpValidationError =>
    new BlpValidationError(ETF_NAV_RELATIONSHIP_NOT_ONE_TO_ONE, { element: 'tickers' });

  const inputOrder = table.getChild('input_order');
  const etfTicker = table.getChild('etf_ticker');
  const inavTicker = table.getChild('inav_ticker');
  const inavValidationError = table.getChild('inav_validation_error');
  if (
    inputOrder === null ||
    etfTicker === null ||
    inavTicker === null ||
    inavValidationError === null
  ) {
    throw malformed();
  }
  if (table.numRows !== etfList.length) {
    throw malformed();
  }

  const rowByOrder = new Map<number, number>();
  for (let rowIndex = 0; rowIndex < table.numRows; rowIndex += 1) {
    const order: unknown = inputOrder.get(rowIndex);
    if (typeof order !== 'number' || rowByOrder.has(order)) {
      throw malformed();
    }
    rowByOrder.set(order, rowIndex);
  }

  const rows: { etf: string; rowIndex: number }[] = [];
  for (const [index, etf] of etfList.entries()) {
    const rowIndex = rowByOrder.get(index);
    if (rowIndex === undefined || etfTicker.get(rowIndex) !== etf) {
      throw malformed();
    }
    rows.push({ etf, rowIndex });
  }

  for (const { etf, rowIndex } of rows) {
    const validationError: unknown = inavValidationError.get(rowIndex);
    if (typeof validationError === 'string' && validationError.trim().length > 0) {
      throw new BlpValidationError(`Invalid iNAV relationship for ETF ${etf}: ${validationError}`, {
        element: 'tickers',
      });
    }
  }

  const pairs = rows.map(({ etf, rowIndex }) => {
    const raw: unknown = inavTicker.get(rowIndex);
    const trimmed = typeof raw === 'string' ? raw.trim() : '';
    return { etf, inav: trimmed.length > 0 ? trimmed : null };
  });

  const missing = pairs.filter(({ inav }) => inav === null).map(({ etf }) => etf);
  if (missing.length > 0) {
    throw new BlpValidationError(
      `Missing valid iNAV relationship for ETFs: ${missing.join(', ')}`,
      { element: 'tickers' },
    );
  }

  const resolved = pairs.flatMap(({ etf, inav }) => (inav === null ? [] : [{ etf, inav }]));
  const owners = new Map<string, string[]>();
  for (const { etf, inav } of resolved) {
    const list = owners.get(inav);
    if (list === undefined) {
      owners.set(inav, [etf]);
    } else {
      list.push(etf);
    }
  }
  for (const { inav } of resolved) {
    const etfs = owners.get(inav);
    if (etfs !== undefined && etfs.length > 1) {
      throw new BlpValidationError(
        `Ambiguous iNAV reverse mapping for ${inav}: ${etfs.join(', ')}`,
        {
          element: 'tickers',
        },
      );
    }
  }

  return resolved.map(({ inav }) => inav);
}

const METADATA_KEY_EID_DATA = 'xbbg.eid_data';
const METADATA_KEY_SECURITY_ERRORS = 'xbbg.security_errors';
const METADATA_KEY_FIELD_EXCEPTIONS = 'xbbg.field_exceptions';

function metadataRecordFromMap(metadata: ReadonlyMap<string, string>): Record<string, string> {
  return Object.fromEntries(metadata.entries());
}

function parseJsonMetadata<T>(
  metadata: Record<string, string>,
  key: string,
  guard: (value: unknown) => value is T,
): T | undefined {
  const raw = metadata[key];
  if (raw === undefined) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    return guard(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function isNumberArrayRecord(value: unknown): value is Record<string, number[]> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(
    (entry) => Array.isArray(entry) && entry.every((eid) => typeof eid === 'number'),
  );
}

function isMetadataError(value: unknown): value is BloombergMetadataError {
  if (!isPlainObject(value)) {
    return false;
  }
  const { category, code, message, subcategory } = value;
  return (
    (category === undefined || typeof category === 'string') &&
    (code === undefined || typeof code === 'string' || typeof code === 'number') &&
    (message === undefined || typeof message === 'string') &&
    (subcategory === undefined || typeof subcategory === 'string')
  );
}

function isMetadataErrorRecord(value: unknown): value is Record<string, BloombergMetadataError> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(isMetadataError);
}

function isFieldException(value: unknown): value is BloombergFieldException {
  if (!isMetadataError(value)) {
    return false;
  }
  if (!('field' in value)) {
    return true;
  }
  return typeof value.field === 'string';
}

function isFieldExceptionRecord(
  value: unknown,
): value is Record<string, BloombergFieldException[]> {
  if (!isPlainObject(value)) {
    return false;
  }
  return Object.values(value).every(
    (entry) => Array.isArray(entry) && entry.every(isFieldException),
  );
}

function attachResultMetadata<T extends object>(
  result: T,
  metadata: Record<string, string>,
): T & ResultMetadata {
  const eidData = parseJsonMetadata(metadata, METADATA_KEY_EID_DATA, isNumberArrayRecord);
  const securityErrors = parseJsonMetadata(
    metadata,
    METADATA_KEY_SECURITY_ERRORS,
    isMetadataErrorRecord,
  );
  const fieldExceptions = parseJsonMetadata(
    metadata,
    METADATA_KEY_FIELD_EXCEPTIONS,
    isFieldExceptionRecord,
  );
  Object.defineProperties(result, {
    eidData: { enumerable: true, value: eidData },
    fieldExceptions: { enumerable: true, value: fieldExceptions },
    metadata: { enumerable: true, value: { ...metadata } },
    securityErrors: { enumerable: true, value: securityErrors },
  });
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Object.defineProperties attaches the ResultMetadata fields above.
  return result as T & ResultMetadata;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toRequestString(value: unknown): string {
  return String(value);
}

function getLegacySecurityOverrides(params: RequestInput): unknown {
  if ('securityOverrides' in params) {
    return params.securityOverrides;
  }
  return undefined;
}

function mapObjectToPairs(obj: OverridesMap | undefined): StringPair[] | undefined {
  if (obj === undefined) {
    return undefined;
  }
  return Object.entries(obj).map(([key, value]) => ({
    key: toRequestString(key),
    value: toRequestString(value),
  }));
}

const OVR_SOURCE_TYPE_ERROR = 'ovr() expects objects, OverrideSpec, or arrays of override entries';

function normalizeOverrideValue(value: unknown): string {
  if (value instanceof Date || hasToJSDate(value)) {
    return formatDate(value) ?? '';
  }
  return String(value);
}

function isOverrideSpecLike(value: unknown): value is OverrideSpecLike {
  return (
    isPlainObject(value) &&
    Array.isArray((value as { pairs?: unknown }).pairs) &&
    typeof (value as { toPairs?: unknown }).toPairs === 'function' &&
    typeof (value as { toObject?: unknown }).toObject === 'function' &&
    typeof (value as { merge?: unknown }).merge === 'function'
  );
}

function isOverrideObject(value: unknown): value is OverrideObject {
  return (
    isPlainObject(value) &&
    !(value instanceof Date) &&
    !hasToJSDate(value) &&
    !isOverrideSpecLike(value) &&
    !ArrayBuffer.isView(value)
  );
}

function normalizeOverrideEntry(entry: unknown): readonly [string, unknown] {
  if (Array.isArray(entry)) {
    if (entry.length !== 2) {
      throw new TypeError(OVR_SOURCE_TYPE_ERROR);
    }
    return [String(entry[0]), entry[1]];
  }
  if (isPlainObject(entry) && 'key' in entry && 'value' in entry) {
    return [String(entry.key), entry.value];
  }
  throw new TypeError(OVR_SOURCE_TYPE_ERROR);
}

interface OverrideState {
  readonly merged: Map<string, string>;
  readonly securityMerged: Map<string, Map<string, string>>;
  readonly securityOrder: string[];
}

function createOverrideState(): OverrideState {
  return {
    merged: new Map<string, string>(),
    securityMerged: new Map<string, Map<string, string>>(),
    securityOrder: [],
  };
}

function isPerSecurityOverrideValue(value: unknown): value is OverrideSource {
  return isOverrideSpecLike(value) || Array.isArray(value) || isOverrideObject(value);
}

function addSecurityOverrideSource(
  security: string,
  source: OverrideSource,
  state: OverrideState,
): void {
  const spec = ovr(source);
  const pairs = spec.toPairs();
  if (pairs.length === 0) {
    return;
  }
  let merged = state.securityMerged.get(security);
  if (merged === undefined) {
    merged = new Map<string, string>();
    state.securityMerged.set(security, merged);
    state.securityOrder.push(security);
  }
  for (const pair of pairs) {
    merged.set(pair.key, pair.value);
  }
}

function addOverridePair(key: string, value: unknown, state: OverrideState): void {
  if (isPerSecurityOverrideValue(value)) {
    addSecurityOverrideSource(key, value, state);
    return;
  }
  state.merged.set(key, normalizeOverrideValue(value));
}

function addOverrideSource(source: OverrideSource, state: OverrideState): void {
  if (typeof source === 'string' || ArrayBuffer.isView(source)) {
    throw new TypeError(OVR_SOURCE_TYPE_ERROR);
  }
  if (isOverrideSpecLike(source)) {
    for (const pair of source.toPairs()) {
      state.merged.set(pair.key, normalizeOverrideValue(pair.value));
    }
    const securityOverrides =
      typeof source.toSecurityOverrides === 'function'
        ? source.toSecurityOverrides()
        : ((source as { securityOverrides?: readonly SecurityOverrideSpec[] }).securityOverrides ??
          []);
    for (const entry of securityOverrides) {
      addSecurityOverrideSource(entry.security, entry.overrides, state);
    }
    return;
  }
  if (Array.isArray(source)) {
    for (const entry of source) {
      const [key, value] = normalizeOverrideEntry(entry);
      addOverridePair(key, value, state);
    }
    return;
  }
  if (isOverrideObject(source)) {
    for (const [key, value] of Object.entries(source)) {
      addOverridePair(key, value, state);
    }
    return;
  }
  throw new TypeError(OVR_SOURCE_TYPE_ERROR);
}

function securityOverridesFromState(state: OverrideState): SecurityOverrideSpec[] {
  return state.securityOrder.flatMap((security) => {
    const pairs = state.securityMerged.get(security);
    if (pairs === undefined || pairs.size === 0) {
      return [];
    }
    return [
      {
        overrides: [...pairs].map(([key, value]) => ({ key, value })),
        security,
      },
    ];
  });
}

export class OverrideSpec implements OverrideSpecLike {
  public readonly pairs: readonly StringPair[];

  public readonly securityOverrides: readonly SecurityOverrideSpec[];

  public constructor(
    pairs: readonly StringPair[],
    securityOverrides: readonly SecurityOverrideSpec[] = [],
  ) {
    this.pairs = Object.freeze(
      pairs.map((pair) =>
        Object.freeze({
          key: pair.key,
          value: pair.value,
        }),
      ),
    );
    this.securityOverrides = Object.freeze(
      securityOverrides.map((entry) =>
        Object.freeze({
          overrides: Object.freeze(
            entry.overrides.map((pair) =>
              Object.freeze({
                key: pair.key,
                value: pair.value,
              }),
            ),
          ),
          security: entry.security,
        }),
      ),
    );
  }

  public [Symbol.iterator](): Iterator<StringPair> {
    return this.toPairs()[Symbol.iterator]();
  }

  public toPairs(): StringPair[] {
    return this.pairs.map((pair) => ({ key: pair.key, value: pair.value }));
  }

  public toObject(): OverridesMap {
    return Object.fromEntries(this.pairs.map((pair) => [pair.key, pair.value]));
  }

  public toSecurityOverrides(): SecurityOverrideSpec[] {
    return this.securityOverrides.map((entry) => ({
      overrides: entry.overrides.map((pair) => ({ key: pair.key, value: pair.value })),
      security: entry.security,
    }));
  }

  public merge(...sources: OverrideSource[]): OverrideSpec {
    return ovr(this, ...sources);
  }

  public forSecurity(security: string, ...sources: OverrideSource[]): OverrideSpec {
    return ovr(this, { [security]: ovr(...sources) });
  }
}

export function ovr(...sources: OverrideSource[]): OverrideSpec {
  const state = createOverrideState();
  for (const source of sources) {
    addOverrideSource(source, state);
  }
  return new OverrideSpec(
    [...state.merged].map(([key, value]) => ({ key, value })),
    securityOverridesFromState(state),
  );
}

function mapOverridesToPairs(input: OverridesInput | undefined): StringPair[] | undefined {
  if (input === undefined) {
    return undefined;
  }
  const spec = ovr(input);
  if (spec.toSecurityOverrides().length > 0) {
    throw new TypeError('Per-security overrides are only supported by bdp(), bdh(), and bds()');
  }
  return spec.toPairs();
}

interface RequestOverrideParts {
  readonly overrides?: StringPair[];
  readonly securityOverrides?: SecurityOverrideSpec[];
}

function mapOverridesToRequestParts(input: OverridesInput | undefined): RequestOverrideParts {
  if (input === undefined) {
    return {};
  }
  const spec = ovr(input);
  const overrides = spec.toPairs();
  const securityOverrides = spec.toSecurityOverrides();
  return {
    ...(overrides.length === 0 ? {} : { overrides }),
    ...(securityOverrides.length === 0 ? {} : { securityOverrides }),
  };
}

type BdtickBooleanOption =
  | 'includeConditionCodes'
  | 'includeExchangeCodes'
  | 'includeBrokerCodes'
  | 'includeRpsCodes'
  | 'includeBicMicCodes'
  | 'includeNonPlottableEvents'
  | 'includeBloombergStandardConditionCodes';

const BDTICK_BOOLEAN_KWARGS: readonly [BdtickBooleanOption, string][] = Object.freeze([
  ['includeConditionCodes', 'includeConditionCodes'],
  ['includeExchangeCodes', 'includeExchangeCodes'],
  ['includeBrokerCodes', 'includeBrokerCodes'],
  ['includeRpsCodes', 'includeRpsCodes'],
  ['includeBicMicCodes', 'includeBicMicCodes'],
  ['includeNonPlottableEvents', 'includeNonPlottableEvents'],
  ['includeBloombergStandardConditionCodes', 'includeBloombergStandardConditionCodes'],
]);

function upsertStringPair(pairs: StringPair[], key: string, value: string): void {
  const existing = pairs.find((pair) => pair.key === key);
  if (existing === undefined) {
    pairs.push({ key, value });
    return;
  }
  existing.value = value;
}

function buildBdtickKwargs(options: BdtickOptions): StringPair[] | undefined {
  const pairs = mapObjectToPairs(options.kwargs) ?? [];
  for (const [optionName, requestName] of BDTICK_BOOLEAN_KWARGS) {
    const typedValue = options[optionName];
    if (typedValue !== undefined) {
      upsertStringPair(pairs, requestName, typedValue ? 'true' : 'false');
    }
  }
  return pairs.length > 0 ? pairs : undefined;
}

function toStringArray(value: string | readonly string[] | null | undefined): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => toRequestString(item));
  }
  if (value === null || value === undefined) {
    return [];
  }
  return [toRequestString(value)];
}

function encodeVolFieldSpec(field: string, spec: VolFieldSpec | undefined): string {
  if (spec === undefined) {
    return field;
  }
  return [
    field,
    spec.metric ?? '',
    spec.tenor ?? '',
    spec.pointType ?? '',
    spec.point === undefined ? '' : String(spec.point),
  ].join('|');
}

function isVolFieldSpecMap(
  fields: VolSurfaceOptions['fields'],
): fields is Record<string, VolFieldSpec> {
  return fields !== undefined && !Array.isArray(fields);
}

function normalizeVolFieldSpecs(fields: VolSurfaceOptions['fields'] | undefined): string[] | null {
  if (fields === undefined) {
    return null;
  }
  if (!isVolFieldSpecMap(fields)) {
    return fields.map((field) => toRequestString(field));
  }
  return Object.entries(fields).map(([field, spec]) => encodeVolFieldSpec(field, spec));
}

function isVolSurfacePresetArray(
  preset: VolSurfaceOptions['preset'],
): preset is readonly VolSurfacePreset[] {
  return Array.isArray(preset);
}
function normalizeVolPresets(preset: VolSurfaceOptions['preset'] | undefined): string[] | null {
  if (preset === undefined || preset === null) {
    return null;
  }
  return isVolSurfacePresetArray(preset) ? [...preset] : [preset];
}

function subscriptionOptionKey(option: string): string {
  return normalizeSubscriptionOption(option).split('=')[0]?.trim().toLowerCase() ?? '';
}

function normalizeSubscriptionOption(option: string): string {
  let clean = option.trim();
  while (clean.startsWith('&')) {
    clean = clean.slice(1).trim();
  }
  return clean;
}

function buildStreamSubscriptionOptions(
  service: string,
  options: StreamOptions,
): readonly string[] | undefined {
  const rawOptions = options.options;
  const { conflate } = options;

  if (rawOptions === undefined && conflate !== true) {
    return undefined;
  }

  const subscriptionOptions = (rawOptions ?? [])
    .map((option) => normalizeSubscriptionOption(option))
    .filter((option) => option.length > 0);

  if (conflate === true) {
    if (service !== MKTDATA_SERVICE) {
      throw new BlpValidationError(
        'conflate=true is only supported for //blp/mktdata subscriptions',
        { element: 'conflate' },
      );
    }
    if (subscriptionOptions.some((option) => subscriptionOptionKey(option) === 'interval')) {
      throw new BlpValidationError(
        'conflate=true cannot be combined with interval options; intervalization overrides conflation',
        { element: 'conflate' },
      );
    }
    if (!subscriptionOptions.some((option) => subscriptionOptionKey(option) === 'conflate')) {
      subscriptionOptions.push('conflate');
    }
  }

  return subscriptionOptions.length > 0 || rawOptions !== undefined
    ? subscriptionOptions
    : undefined;
}

function normalizeConfigureArgs(
  configOrHost?: EngineConfig | string,
  port?: number,
): EngineConfig | undefined {
  if (configOrHost === undefined) {
    return undefined;
  }
  if (typeof configOrHost === 'string' || port !== undefined) {
    const config: EngineConfig = {};
    if (typeof configOrHost === 'string') {
      config.host = configOrHost;
    }
    if (port !== undefined) {
      config.port = port;
    }
    return config;
  }
  if (isPlainObject(configOrHost)) {
    return { ...(configOrHost as EngineConfig) };
  }
  throw new TypeError('configure expects either a config object or host/port arguments');
}

function normalizeRecoveryOptions(options: CdxOptions = {}): BdpOptions {
  const normalized: CdxOptions = { ...options };
  const recoveryRate = normalized.recoveryRate ?? normalized.recovery_rate;
  delete normalized.recoveryRate;
  delete normalized.recovery_rate;
  if (recoveryRate !== undefined) {
    normalized.overrides = ovr(normalized.overrides ?? {}, {
      CDS_RR: toRequestString(recoveryRate),
    });
  }
  return normalized;
}

function fullDayRange(dt: DateTimeLike): TimeRange {
  const formatted = formatDate(dt);
  if (formatted === undefined) {
    throw new TypeError('dt must be a non-empty date-like value');
  }
  const day = `${formatted.slice(0, 4)}-${formatted.slice(4, 6)}-${formatted.slice(6, 8)}`;
  return {
    end: `${day}T23:59:59`,
    start: `${day}T00:00:00`,
  };
}

function normalizeDate(value: DateLike | undefined): string | undefined {
  return formatDate(value);
}

function getStudyAttrName(study: string): string {
  const normalized = study.toLowerCase().replaceAll(/-/gu, '_').replaceAll(/ /gu, '_');
  const mapped = TA_STUDIES[normalized];
  if (mapped !== undefined) {
    return mapped;
  }
  if (normalized.endsWith('studyattributes')) {
    return normalized;
  }
  return `${normalized}StudyAttributes`;
}

interface RawStudy {
  studyType?: string;
  study?: string;
  calcInterval?: string;
  interval?: number | string;
  length?: number;
  period?: number;
  [key: string]: PrimitiveValue | undefined;
}

function buildTaRequest(
  ticker: string,
  study: string | RawStudy,
  options: BtaOptions = {},
): StringPair[] {
  const rawStudy: RawStudy = typeof study === 'string' ? { studyType: study } : { ...study };
  const studyType =
    rawStudy.studyType ?? rawStudy.study ?? (typeof study === 'string' ? study : '');
  const attrName = getStudyAttrName(toRequestString(studyType));

  const kwargs: Record<string, PrimitiveValue> = { ...options.kwargs };
  const startDate = normalizeDate(
    stringOrUndef(kwargs.startDate) ??
      stringOrUndef(kwargs.start_date) ??
      options.startDate ??
      options.start_date,
  );
  const endDate = normalizeDate(
    stringOrUndef(kwargs.endDate) ??
      stringOrUndef(kwargs.end_date) ??
      options.endDate ??
      options.end_date,
  );
  const periodicity = toRequestString(
    stringOrUndef(kwargs.periodicitySelection) ??
      stringOrUndef(kwargs.periodicity) ??
      rawStudy.calcInterval ??
      options.periodicity ??
      'DAILY',
  ).toUpperCase();
  const interval = kwargs.interval ?? rawStudy.interval ?? options.interval;

  delete kwargs.startDate;
  delete kwargs.start_date;
  delete kwargs.endDate;
  delete kwargs.end_date;
  delete kwargs.periodicitySelection;
  delete kwargs.periodicity;
  delete rawStudy.studyType;
  delete rawStudy.study;
  delete rawStudy.calcInterval;

  if (rawStudy.length !== undefined && rawStudy.period === undefined) {
    rawStudy.period = rawStudy.length;
  }
  delete rawStudy.length;

  const params: StudyParams = {
    ...TA_DEFAULTS[attrName],
    ...options.studyParams,
    ...(rawStudy as StudyParams),
  };

  if (params.length !== undefined && params.period === undefined) {
    params.period = params.length;
  }
  delete params.length;
  delete params.calcInterval;

  const elements: StringPair[] = [
    { key: 'priceSource.securityName', value: toRequestString(ticker) },
  ];

  if (periodicity === 'INTRADAY') {
    const prefix = 'priceSource.dataRange.intraday';
    if (startDate !== undefined) {
      elements.push({ key: `${prefix}.startDate`, value: startDate });
    }
    if (endDate !== undefined) {
      elements.push({ key: `${prefix}.endDate`, value: endDate });
    }
    elements.push({ key: `${prefix}.eventType`, value: 'TRADE' });
    if (interval !== undefined) {
      elements.push({ key: `${prefix}.interval`, value: toRequestString(interval) });
    }
  } else {
    const prefix = 'priceSource.dataRange.historical';
    if (startDate !== undefined) {
      elements.push({ key: `${prefix}.startDate`, value: startDate });
    }
    if (endDate !== undefined) {
      elements.push({ key: `${prefix}.endDate`, value: endDate });
    }
    elements.push({ key: `${prefix}.periodicitySelection`, value: periodicity });
  }

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined) {
      continue;
    }
    elements.push({
      key: `studyAttributes.${attrName}.${key}`,
      value: toRequestString(value),
    });
  }

  for (const [key, value] of Object.entries(kwargs)) {
    elements.push({ key: toRequestString(key), value: toRequestString(value) });
  }

  return elements;
}

function stringOrUndef(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

let polarsModule: PolarsModule | undefined;
let polarsLoadError: Error | undefined;

function cachePolarsLoadError(err: unknown): Error {
  const error = new Error(
    'nodejs-polars is required for Polars backend. Install: npm install nodejs-polars',
  );
  Object.defineProperty(error, 'cause', { configurable: true, value: err });
  polarsLoadError = error;
  return error;
}

function loadPolars(): PolarsModule {
  if (polarsModule !== undefined) {
    return polarsModule;
  }
  if (polarsLoadError !== undefined) {
    throw polarsLoadError;
  }
  try {
    polarsModule = requirePolarsModule();
    return polarsModule;
  } catch (error) {
    throw cachePolarsLoadError(error);
  }
}

function normalizeBackend(backend: BackendKind | undefined): BackendKind {
  const selected: unknown = backend ?? Backend.ARROW;
  if (selected === Backend.ARROW || selected === Backend.JSON || selected === Backend.POLARS) {
    return selected;
  }
  throw new TypeError(
    `Unsupported @xbbg/core backend "${toRequestString(selected)}". Expected one of: ${Object.values(
      Backend,
    ).join(', ')}`,
  );
}

function nativeArrowToBackend(
  batch: NativeArrowZeroCopyBatch,
  backend: BackendKind | undefined,
): unknown {
  const selected = normalizeBackend(backend);
  const table = tableFromNativeArrowBatch(batch);
  const metadata = metadataRecordFromMap(table.schema.metadata);
  if (selected === Backend.JSON) {
    return attachResultMetadata([...table], metadata);
  }
  if (selected === Backend.POLARS) {
    throw new TypeError('Polars backend requires the IPC requestRaw path');
  }
  return attachResultMetadata(table, metadata);
}

function ipcToPolars(buffer: Buffer): unknown {
  return loadPolars().readIPC(buffer);
}

// ── Configured engine state ─────────────────────────────────────────────

let configuredEngineConfig: EngineConfig | undefined;
let configuredEnginePromise: Promise<Engine> | undefined;

function clearConfiguredEngine(): void {
  const existing = configuredEnginePromise;
  configuredEnginePromise = undefined;
  if (existing !== undefined) {
    void (async (): Promise<void> => {
      try {
        const engine = await existing;
        engine.signalShutdown();
      } catch {
        /* Ignore shutdown errors */
      }
    })();
  }
}

async function getConfiguredEngine(): Promise<Engine> {
  if (configuredEnginePromise === undefined) {
    const pending = connect(configuredEngineConfig);
    pending.catch(() => {
      if (configuredEnginePromise === pending) {
        configuredEnginePromise = undefined;
      }
    });
    configuredEnginePromise = pending;
  }
  return await configuredEnginePromise;
}

// ── Subscription class ──────────────────────────────────────────────────

export type TickValue = null | boolean | number | bigint | string | Date;

export class FieldHandle {
  public constructor(public readonly name: string) {}
}

interface TickLayout {
  readonly version: number;
  readonly fields: readonly string[];
  readonly kinds: readonly NativeSubscriptionFieldKind[];
  readonly positions: Map<string, number>;
}

function createTickLayout(layout: NativeSubscriptionLayout): TickLayout {
  return {
    fields: layout.fields,
    kinds: layout.kinds,
    positions: new Map(layout.fields.map((field, index) => [field, index])),
    version: layout.version,
  };
}

export class Tick {
  private readonly decodedSet: boolean[] = [];
  private readonly decodedValues: (TickValue | undefined)[] = [];
  private rowPositions: number[] | undefined;

  public constructor(
    private readonly update: NativeSubscriptionRow,
    private readonly layout: TickLayout,
  ) {}

  public get topic(): string {
    return this.update.topic;
  }

  public get timestampUs(): number {
    return this.update.timestampUs;
  }

  public get layoutVersion(): number {
    return this.update.layoutVersion;
  }

  public has(field: string | FieldHandle): boolean {
    const name = typeof field === 'string' ? field : field.name;
    const fieldIndex = this.layout.positions.get(name);
    return fieldIndex !== undefined && this.valuePosition(fieldIndex) !== undefined;
  }

  public get(field: string | FieldHandle): TickValue | undefined {
    const name = typeof field === 'string' ? field : field.name;
    const fieldIndex = this.layout.positions.get(name);
    return fieldIndex === undefined ? undefined : this.getByFieldIndex(fieldIndex);
  }

  private getByFieldIndex(fieldIndex: number): TickValue | undefined {
    if (this.decodedSet[fieldIndex] === true) {
      return this.decodedValues[fieldIndex];
    }
    const position = this.valuePosition(fieldIndex);
    if (position === undefined) {
      this.decodedSet[fieldIndex] = true;
      this.decodedValues[fieldIndex] = undefined;
      return undefined;
    }

    const kind = this.layout.kinds[fieldIndex] ?? 'unknown';
    let value: TickValue;
    if (kind === 'bool') {
      value = this.update.boolValues[position] ?? null;
    } else if (kind === 'i32') {
      value = this.update.i32Values[position] ?? null;
    } else if (kind === 'f64') {
      value = this.update.f64Values[position] ?? null;
    } else if (kind === 'str' || kind === 'unknown') {
      value = this.update.stringValues[position] ?? null;
    } else if (kind === 'date32') {
      const days = this.update.i32Values[position];
      value = days === null || days === undefined ? null : new Date(Date.UTC(1970, 0, 1 + days));
    } else {
      const raw = this.update.i64Values[position];
      if (raw === null || raw === undefined) {
        value = null;
      } else {
        try {
          value = BigInt(raw);
        } catch {
          value = null;
        }
      }
    }
    this.decodedSet[fieldIndex] = true;
    this.decodedValues[fieldIndex] = value;
    return value;
  }

  private valuePosition(fieldIndex: number): number | undefined {
    let positions = this.rowPositions;
    if (positions === undefined) {
      const built: number[] = [];
      for (const [position, index] of this.update.fieldIndices.entries()) {
        built[index] = position;
      }
      positions = built;
      this.rowPositions = positions;
    }
    return positions[fieldIndex];
  }

  public f64(field: string | FieldHandle): number | null | undefined {
    const value = this.get(field);
    if (value === null || value === undefined) {
      return value;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  public i64(field: string | FieldHandle): bigint | null | undefined {
    const value = this.get(field);
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof value === 'bigint') {
      return value;
    }
    try {
      return BigInt(typeof value === 'string' ? value : String(value));
    } catch {
      return null;
    }
  }

  public str(field: string | FieldHandle): string | null | undefined {
    const value = this.get(field);
    return value === null || value === undefined ? value : String(value);
  }

  public toObject(): Record<string, unknown> {
    const out: Record<string, unknown> = { timestampUs: this.timestampUs, topic: this.topic };
    for (const fieldIndex of this.update.fieldIndices) {
      const field = this.layout.fields[fieldIndex];
      if (field !== undefined) {
        out[field] = this.getByFieldIndex(fieldIndex);
      }
    }
    return out;
  }
}

class SubscriptionReadQueue {
  private tail: Promise<void> = Promise.resolve();

  public enqueue<T>(read: () => Promise<T>): Promise<T> {
    const result = this.tail.then(read);
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  public barrier(): Promise<void> {
    return this.tail;
  }
}

type SubscriptionIteratorPhase = 'open' | 'closing' | 'closed';
type SubscriptionReadMode = 'scalar' | 'arrow';

type SubscriptionBatchProjection<TBatch, TValue> =
  | {
      readonly cardinality: 'many';
      readonly project: (batch: TBatch) => TValue[];
    }
  | {
      readonly cardinality: 'one';
      readonly project: (batch: TBatch) => TValue;
    };

class SubscriptionCoordinator {
  public readonly reads = new SubscriptionReadQueue();
  private phase: SubscriptionIteratorPhase = 'open';
  private readMode: SubscriptionReadMode | undefined;
  private readonly closeObservers = new Set<(owner: object | undefined) => void>();
  private readonly closed: Promise<void>;
  private readonly resolveClosed: () => void;
  private closeError: Error | undefined;
  private lateReadError: Error | undefined;

  public constructor() {
    let resolveClosed!: () => void;
    this.closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    this.resolveClosed = resolveClosed;
  }

  public get isOpen(): boolean {
    return this.phase === 'open';
  }

  public get closeReadError(): Error | undefined {
    return this.lateReadError;
  }

  public recordCloseReadError(error: unknown): void {
    this.lateReadError ??= wrapError(error);
  }

  public claimReadMode(mode: SubscriptionReadMode): void {
    const mismatch = this.readModeMismatch(mode);
    if (mismatch !== undefined) {
      throw mismatch;
    }
    this.readMode = mode;
  }

  public readModeMismatch(mode: SubscriptionReadMode): TypeError | undefined {
    if (this.readMode !== undefined && this.readMode !== mode) {
      return new TypeError(
        `subscription is already being read as ${this.readMode}; cannot also read as ${mode}`,
      );
    }
    return undefined;
  }

  public observeClose(observer: (owner: object | undefined) => void): void {
    if (this.phase === 'open') {
      this.closeObservers.add(observer);
    } else {
      observer(undefined);
    }
  }

  public beginClose(owner: object): { readonly barrier: Promise<void>; readonly started: boolean } {
    if (this.phase !== 'open') {
      return { barrier: this.closed, started: false };
    }
    this.phase = 'closing';
    for (const observer of this.closeObservers) {
      observer(owner);
    }
    this.closeObservers.clear();
    return { barrier: this.reads.barrier(), started: true };
  }

  public finishNaturalClose(owner: object): void {
    if (this.phase !== 'open') {
      return;
    }
    this.phase = 'closed';
    for (const observer of this.closeObservers) {
      observer(owner);
    }
    this.closeObservers.clear();
    this.resolveClosed();
  }

  public finishClose(error: Error | undefined): void {
    if (this.phase === 'closed') {
      return;
    }
    this.phase = 'closed';
    this.closeError = error;
    this.lateReadError = undefined;
    this.closeObservers.clear();
    this.resolveClosed();
  }

  public async whenClosed(): Promise<void> {
    await this.closed;
    if (this.closeError !== undefined) {
      throw this.closeError;
    }
  }
}

const subscriptionCoordinators = new WeakMap<NativeSubscription, SubscriptionCoordinator>();

function subscriptionCoordinatorFor(inner: NativeSubscription): SubscriptionCoordinator {
  const existing = subscriptionCoordinators.get(inner);
  if (existing !== undefined) {
    return existing;
  }
  const created = new SubscriptionCoordinator();
  subscriptionCoordinators.set(inner, created);
  return created;
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new DOMException('The operation was aborted', 'AbortError');
}

function throwIfSubscriptionReadAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw abortReason(signal);
  }
}

async function rejectAfterSubscriptionCleanup(
  primary: Error,
  cleanup: Promise<unknown>,
): Promise<never> {
  try {
    await cleanup;
  } catch (cleanupError) {
    if (cleanupError === primary) {
      throw primary;
    }
    throw new AggregateError(
      [primary, cleanupError],
      `${primary.message}; subscription cleanup failed`,
      { cause: cleanupError },
    );
  }
  throw primary;
}

class SubscriptionIterator<TBatch, TValue> {
  private pending: (TValue | undefined)[] = [];
  private pendingCursor = 0;
  private phase: SubscriptionIteratorPhase = 'open';
  private closeDrain = false;
  private readonly closingPending: TValue[] = [];
  private closeInFlight: Promise<TValue[]> | undefined;
  private readonly owner = {};
  private readonly nextSerializedWithoutSignal = this.nextSerialized.bind(this, undefined);

  public constructor(
    private readonly readBatch: () => Promise<TBatch | null>,
    private readonly closeNative: (drain: boolean) => Promise<readonly TBatch[] | null>,
    private readonly projection: SubscriptionBatchProjection<TBatch, TValue>,
    private readonly coordinator: SubscriptionCoordinator,
    private readonly readMode: SubscriptionReadMode,
  ) {
    this.coordinator.observeClose((owner) => {
      if (owner === this.owner) {
        return;
      }
      this.phase = 'closing';
      this.closeDrain = false;
      this.clearPending();
      this.closingPending.length = 0;
    });
  }

  private isOpen(): boolean {
    return this.phase === 'open';
  }

  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<TValue, undefined>> {
    const signal = options?.signal;
    return signal === undefined ? this.nextWithoutSignal() : this.nextWithSignal(signal);
  }

  private async nextWithoutSignal(): Promise<IteratorResult<TValue, undefined>> {
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    this.coordinator.claimReadMode(this.readMode);
    const queued = this.coordinator.reads.enqueue(this.nextSerializedWithoutSignal);
    try {
      return await queued;
    } catch (error) {
      return await rejectAfterSubscriptionCleanup(wrapError(error), this.startClose(false));
    }
  }

  private async nextWithSignal(signal: AbortSignal): Promise<IteratorResult<TValue, undefined>> {
    if (signal.aborted) {
      return await rejectAfterSubscriptionCleanup(abortReason(signal), this.startClose(false));
    }
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    this.coordinator.claimReadMode(this.readMode);

    let abortError: Error | undefined;
    const onAbort = (): void => {
      abortError = abortReason(signal);
      void this.startClose(false).catch(() => undefined);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    const queued = this.coordinator.reads.enqueue(this.nextSerialized.bind(this, signal));
    try {
      return await queued;
    } catch (error) {
      const primary = abortError ?? wrapError(error);
      return await rejectAfterSubscriptionCleanup(primary, this.startClose(false));
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  private async nextSerialized(
    signal: AbortSignal | undefined,
  ): Promise<IteratorResult<TValue, undefined>> {
    throwIfSubscriptionReadAborted(signal);
    if (!this.isOpen()) {
      return { done: true, value: undefined };
    }

    const pending = this.takePending();
    if (pending !== undefined) {
      return { done: false, value: pending };
    }

    while (this.isOpen()) {
      let batch: TBatch | null;
      try {
        batch = await this.readBatch();
      } catch (error) {
        if (!this.isOpen()) {
          this.coordinator.recordCloseReadError(error);
          throwIfSubscriptionReadAborted(signal);
          return { done: true, value: undefined };
        }
        throwIfSubscriptionReadAborted(signal);
        throw error;
      }

      if (!this.isOpen()) {
        if (batch !== null && this.closeDrain) {
          try {
            this.appendBatch(this.closingPending, batch);
          } catch (error) {
            this.coordinator.recordCloseReadError(error);
          }
        }
        throwIfSubscriptionReadAborted(signal);
        return { done: true, value: undefined };
      }
      throwIfSubscriptionReadAborted(signal);
      if (batch === null) {
        this.phase = 'closed';
        this.clearPending();
        this.coordinator.finishNaturalClose(this.owner);
        return { done: true, value: undefined };
      }

      if (this.projection.cardinality === 'one') {
        return { done: false, value: this.projection.project(batch) };
      }
      const values = this.projection.project(batch);
      if (values.length === 0) {
        continue;
      }

      this.pending = values;
      this.pendingCursor = 0;
      const value = this.takePending();
      if (value !== undefined) {
        return { done: false, value };
      }
    }

    return { done: true, value: undefined };
  }

  private appendBatch(target: TValue[], batch: TBatch): void {
    if (this.projection.cardinality === 'one') {
      target.push(this.projection.project(batch));
      return;
    }
    for (const value of this.projection.project(batch)) {
      target.push(value);
    }
  }

  private takePending(): TValue | undefined {
    const value = this.pending[this.pendingCursor];
    if (value === undefined) {
      this.clearPending();
      return undefined;
    }
    this.pending[this.pendingCursor] = undefined;
    this.pendingCursor += 1;
    if (this.pendingCursor === this.pending.length) {
      this.pending = [];
      this.pendingCursor = 0;
    }
    return value;
  }

  private drainPendingToClosing(): void {
    for (let index = this.pendingCursor; index < this.pending.length; index += 1) {
      const value = this.pending[index];
      this.pending[index] = undefined;
      if (value !== undefined) {
        this.closingPending.push(value);
      }
    }
    this.pending = [];
    this.pendingCursor = 0;
  }

  private clearPending(): void {
    this.pending.fill(undefined);
    this.pending = [];
    this.pendingCursor = 0;
  }

  public unsubscribe(drain = false): Promise<TValue[]> {
    return this.startClose(drain);
  }

  private async startClose(drain: boolean): Promise<TValue[]> {
    const formatError = drain ? this.coordinator.readModeMismatch(this.readMode) : undefined;
    if (formatError !== undefined) {
      const cleanup = this.coordinator.isOpen ? this.startClose(false) : this.waitForSharedClose();
      return await rejectAfterSubscriptionCleanup(formatError, cleanup);
    }
    if (drain) {
      this.coordinator.claimReadMode(this.readMode);
    }
    if (this.closeInFlight !== undefined) {
      return await this.closeInFlight;
    }
    if (!this.coordinator.isOpen) {
      return await this.waitForSharedClose();
    }

    this.phase = 'closing';
    this.closeDrain = drain;
    if (drain) {
      this.drainPendingToClosing();
    } else {
      this.clearPending();
    }

    const { barrier: readBarrier, started } = this.coordinator.beginClose(this.owner);
    if (!started) {
      return await this.waitForSharedClose();
    }

    let nativeClose: Promise<readonly TBatch[] | null>;
    try {
      nativeClose = this.closeNative(drain);
    } catch (error) {
      nativeClose = Promise.reject(wrapError(error));
    }
    const closePromise = (async (): Promise<TValue[]> => {
      let nativeBatches: readonly TBatch[] | null = null;
      let nativeError: Error | undefined;
      let closeError: Error | undefined;
      try {
        try {
          nativeBatches = await nativeClose;
        } catch (error) {
          nativeError = wrapError(error);
        }
        await readBarrier;

        const readError = this.coordinator.closeReadError;
        if (nativeError !== undefined && readError !== undefined && nativeError !== readError) {
          const cleanupError = wrapError(nativeError);
          closeError = new AggregateError(
            [wrapError(readError), cleanupError],
            `${readError.message}; subscription cleanup failed`,
            { cause: cleanupError },
          );
          throw closeError;
        }
        if (nativeError !== undefined) {
          throw nativeError;
        }
        if (readError !== undefined) {
          throw readError;
        }
        if (!drain) {
          return [];
        }
        const drained = [...this.closingPending];
        for (const batch of nativeBatches ?? []) {
          this.appendBatch(drained, batch);
        }
        return drained;
      } catch (error) {
        closeError ??= wrapError(error);
        throw closeError;
      } finally {
        this.phase = 'closed';
        this.closeDrain = false;
        this.clearPending();
        this.closingPending.length = 0;
        this.coordinator.finishClose(closeError);
      }
    })();
    this.closeInFlight = closePromise;
    const releaseClose = (): void => {
      if (this.closeInFlight === closePromise) {
        this.closeInFlight = undefined;
      }
    };
    void closePromise.then(releaseClose, releaseClose);
    return await closePromise;
  }

  private async waitForSharedClose(): Promise<TValue[]> {
    try {
      await this.coordinator.whenClosed();
      return [];
    } finally {
      this.phase = 'closed';
      this.closeDrain = false;
      this.clearPending();
      this.closingPending.length = 0;
    }
  }
}
export class ArrowSubscription
  implements
    AsyncIterator<Table, undefined, SubscriptionReadOptions | undefined>,
    AsyncIterable<Table>
{
  private readonly iterator: SubscriptionIterator<NativeArrowZeroCopyBatch, Table>;

  public constructor(inner: NativeSubscription) {
    const coordinator = subscriptionCoordinatorFor(inner);
    this.iterator = new SubscriptionIterator(
      inner.nextArrowBatch.bind(inner),
      inner.unsubscribeArrow.bind(inner),
      { cardinality: 'one', project: toArrowTableFromNative },
      coordinator,
      'arrow',
    );
  }

  /** Return the next zero-copy Arrow table; one native crossing may contain many rows. */
  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<Table, undefined>> {
    return this.iterator.next(options);
  }

  public unsubscribe(drain = false): Promise<Table[]> {
    return this.iterator.unsubscribe(drain);
  }

  public async return(): Promise<IteratorResult<Table, undefined>> {
    await this.unsubscribe(false);
    return { done: true, value: undefined };
  }

  public [Symbol.asyncIterator](): this {
    return this;
  }
}

export class Subscription
  implements
    AsyncIterator<Tick, undefined, SubscriptionReadOptions | undefined>,
    AsyncIterable<Tick>
{
  private readonly layouts = new Map<number, TickLayout>();
  private readonly coordinator: SubscriptionCoordinator;
  private readonly iterator: SubscriptionIterator<NativeSubscriptionUpdateBatch, Tick>;
  private arrowView: ArrowSubscription | undefined;

  public constructor(private readonly inner: NativeSubscription) {
    this.coordinator = subscriptionCoordinatorFor(this.inner);
    this.iterator = new SubscriptionIterator(
      this.inner.nextUpdates.bind(this.inner),
      this.inner.unsubscribe.bind(this.inner),
      { cardinality: 'many', project: (batch) => this.ticksFromBatch(batch) },
      this.coordinator,
      'scalar',
    );
  }

  public next(options?: SubscriptionReadOptions): Promise<IteratorResult<Tick, undefined>> {
    return this.iterator.next(options);
  }

  private ticksFromBatch(batch: NativeSubscriptionUpdateBatch): Tick[] {
    if (batch.layout !== undefined) {
      this.layouts.set(batch.layout.version, createTickLayout(batch.layout));
    }
    return batch.updates.map((update) => {
      const layout = this.layouts.get(update.layoutVersion);
      if (layout === undefined) {
        throw new Error(`subscription layout ${update.layoutVersion} was not supplied by native`);
      }
      return new Tick(update, layout);
    });
  }

  public async add(tickers: readonly string[]): Promise<void> {
    try {
      await this.inner.add(tickers);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async remove(tickers: readonly string[]): Promise<void> {
    try {
      await this.inner.remove(tickers);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public unsubscribe(drain = false): Promise<Tick[]> {
    return this.iterator.unsubscribe(drain);
  }

  public async return(): Promise<IteratorResult<Tick, undefined>> {
    await this.unsubscribe(false);
    return { done: true, value: undefined };
  }

  public field(name: string): FieldHandle {
    return new FieldHandle(name);
  }

  public arrow(): ArrowSubscription {
    this.arrowView ??= new ArrowSubscription(this.inner);
    return this.arrowView;
  }

  public get tickers(): string[] {
    return this.inner.tickers;
  }

  public get fields(): string[] {
    return this.inner.fields;
  }

  public get isActive(): boolean {
    return this.inner.isActive;
  }

  public get stats(): SubscriptionStats {
    return this.inner.stats;
  }

  public [Symbol.asyncIterator](): this {
    return this;
  }
}

// ── Engine class ────────────────────────────────────────────────────────

export class Engine {
  // Set via constructor or via `withConfig` (which instantiates via Object.create).
  private inner!: NativeEngine;
  private parsedSchemas = new Map<string, unknown>();
  private parsedOperations = new Map<string, unknown>();

  public constructor(host = 'localhost', port = 8194) {
    try {
      this.inner = new native.JsEngine(host, port);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /** Allocate an Engine around an already-constructed native engine. */
  private static fromInner(inner: NativeEngine): Engine {
    const maybeEngine: unknown = Object.create(Engine.prototype);
    if (!(maybeEngine instanceof Engine)) {
      throw new TypeError('Failed to allocate Engine instance');
    }
    maybeEngine.inner = inner;
    maybeEngine.parsedSchemas = new Map<string, unknown>();
    maybeEngine.parsedOperations = new Map<string, unknown>();
    return maybeEngine;
  }

  public static withConfig(config: EngineConfig = {}): Engine {
    try {
      return Engine.fromInner(native.JsEngine.withConfig(config));
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Connect asynchronously: the Bloomberg session connect and service warmup
   * run off the JS thread. The sync constructor and `withConfig` block the
   * Node event loop for the duration of the connect (seconds, up to the 30s
   * session timeout) — prefer this factory in servers.
   */
  public static async connect(config?: EngineConfig): Promise<Engine> {
    try {
      const inner =
        config === undefined
          ? await native.JsEngine.connect()
          : await native.JsEngine.connectWithConfig(config);
      return Engine.fromInner(inner);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Return the Bloomberg identity seat type: "BPS", "NONBPS", or "INVALID".
   *
   * Identity operations authorize lazily using the engine auth config when
   * configured, otherwise the Desktop terminal OS-logon user. The first call
   * may block for a few seconds and transient failures are retryable.
   */
  public async seatType(): Promise<SeatType> {
    try {
      return await this.inner.seatType();
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Check whether the authorized identity is entitled to all supplied EIDs.
   *
   * Identity operations authorize lazily using the engine auth config when
   * configured, otherwise the Desktop terminal OS-logon user. The first call
   * may block for a few seconds and transient failures are retryable.
   */
  public async checkEntitlements(
    service: string,
    eids: readonly number[],
  ): Promise<EntitlementReport> {
    try {
      return await this.inner.checkEntitlements(service, eids);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Return whether the authorized identity may use the Bloomberg service.
   *
   * Identity operations authorize lazily using the engine auth config when
   * configured, otherwise the Desktop terminal OS-logon user. The first call
   * may block for a few seconds and transient failures are retryable.
   */
  public async identityIsAuthorized(service: string): Promise<boolean> {
    try {
      return await this.inner.identityIsAuthorized(service);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async request(params: RequestInput): Promise<unknown> {
    const backend = normalizeBackend(params.backend);
    const { backend: _discarded, overrides, ...rest } = params;
    const legacySecurityOverrides = getLegacySecurityOverrides(params);
    if (legacySecurityOverrides !== undefined) {
      throw new TypeError(
        'Use overrides: ovr({ "<SECURITY>": { ... } }) for per-security overrides',
      );
    }
    const nativeParams = { ...rest, ...mapOverridesToRequestParts(overrides) };
    try {
      if (backend === Backend.POLARS) {
        return ipcToPolars(await this.inner.requestRaw(nativeParams));
      }
      const batch = await this.inner.request(nativeParams);
      return nativeArrowToBackend(batch, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async requestRaw(params: RequestInput): Promise<Buffer> {
    const { overrides, ...rest } = params;
    const legacySecurityOverrides = getLegacySecurityOverrides(params);
    if (legacySecurityOverrides !== undefined) {
      throw new TypeError(
        'Use overrides: ovr({ "<SECURITY>": { ... } }) for per-security overrides',
      );
    }
    try {
      return await this.inner.requestRaw({ ...rest, ...mapOverridesToRequestParts(overrides) });
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async bdp(
    tickers: readonly string[],
    fields: readonly string[],
    options: BdpOptions = {},
  ): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      extractor: 'refdata',
      fields,
      format: options.format,
      includeSecurityErrors: Boolean(options.includeSecurityErrors),
      returnEids: options.returnEids,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'ReferenceDataRequest',
      overrides: options.overrides,
      securities: tickers,
      service: '//blp/refdata',
      validateFields: options.validateFields,
    });
  }

  public async bds(
    tickers: readonly string[],
    fields: readonly string[],
    options: BdpOptions = {},
  ): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      extractor: 'bulk',
      fields,
      format: options.format,
      returnEids: options.returnEids,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'ReferenceDataRequest',
      overrides: options.overrides,
      securities: tickers,
      service: '//blp/refdata',
      validateFields: options.validateFields,
    });
  }

  public async bdh(
    tickers: readonly string[],
    fields: readonly string[],
    options: BdhOptions = {},
  ): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      endDate: formatDate(options.end),
      extractor: 'histdata',
      fields,
      format: options.format,
      returnEids: options.returnEids,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'HistoricalDataRequest',
      overrides: options.overrides,
      securities: tickers,
      service: '//blp/refdata',
      startDate: formatDate(options.start),
      validateFields: options.validateFields,
    });
  }

  public async bdib(ticker: string, options: BdibOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      endDatetime: formatDateTime(options.end),
      eventType: options.eventType ?? 'TRADE',
      extractor: 'intraday_bar',
      interval: options.interval ?? 1,
      returnEids: options.returnEids,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'IntradayBarRequest',
      outputTz: options.outputTz,
      requestTz: options.requestTz,
      security: ticker,
      service: '//blp/refdata',
      startDatetime: formatDateTime(options.start),
    });
  }

  public async bdtick(ticker: string, options: BdtickOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      endDatetime: formatDateTime(options.end),
      eventTypes: options.eventTypes ?? ['TRADE'],
      extractor: 'intraday_tick',
      kwargs: buildBdtickKwargs(options),
      returnEids: options.returnEids,
      operation: 'IntradayTickRequest',
      outputTz: options.outputTz,
      requestTz: options.requestTz,
      security: ticker,
      service: '//blp/refdata',
      startDatetime: formatDateTime(options.start),
    });
  }

  public async bql(query: string, options: BqlOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      elements: [{ key: 'expression', value: toRequestString(query) }],
      extractor: 'bql',
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'sendQuery',
      service: '//blp/bqlsvc',
    });
  }

  public async beqs(screen: string, options: BeqsOptions = {}): Promise<unknown> {
    const elements: StringPair[] = [
      { key: 'screenName', value: toRequestString(screen) },
      { key: 'screenType', value: toRequestString(options.screenType ?? 'PRIVATE') },
      { key: 'Group', value: toRequestString(options.group ?? 'General') },
    ];
    if (options.asof !== undefined) {
      const asofFormatted = formatDate(options.asof);
      if (asofFormatted !== undefined) {
        elements.push({ key: 'asOfDate', value: asofFormatted });
      }
    }
    return await this.request({
      backend: options.backend,
      elements,
      extractor: 'generic',
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'BeqsRequest',
      overrides: mapOverridesToPairs(options.overrides),
      service: '//blp/refdata',
    });
  }

  public async bsrch(searchSpec: string, options: BsrchOptions = {}): Promise<unknown> {
    const elements: StringPair[] = [
      { key: 'Domain', value: toRequestString(searchSpec) },
      ...(mapOverridesToPairs(options.overrides) ?? []),
      ...(mapObjectToPairs(options.kwargs) ?? []),
    ];
    return await this.request({
      backend: options.backend,
      elements,
      extractor: 'bsrch',
      format: options.format,
      operation: 'ExcelGetGridRequest',
      service: '//blp/exrsvc',
    });
  }

  public async bta(
    ticker: string,
    study: string | RawStudy,
    options: BtaOptions = {},
  ): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      elements: buildTaRequest(ticker, study, options),
      extractor: 'generic',
      format: options.format,
      operation: 'studyRequest',
      service: '//blp/tasvc',
    });
  }

  public async bflds(options: BfldsOptions = {}): Promise<unknown> {
    if (options.searchSpec !== undefined) {
      return await this.request({
        backend: options.backend,
        format: options.format,
        kwargs: mapObjectToPairs(options.kwargs),
        operation: 'FieldSearchRequest',
        searchSpec: toRequestString(options.searchSpec),
        service: '//blp/apiflds',
      });
    }
    const fields = toStringArray(options.fields);
    return await this.request({
      backend: options.backend,
      fieldIds: fields,
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'FieldInfoRequest',
      service: '//blp/apiflds',
    });
  }

  public async blkp(query: string, options: BlkpOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      elements: [{ key: 'query', value: toRequestString(query) }],
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'instrumentListRequest',
      service: '//blp/instruments',
    });
  }

  public async bport(
    portfolio: string,
    fields: string | readonly string[],
    options: RequestOptions = {},
  ): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      fields: Array.isArray(fields) ? fields : [toRequestString(fields)],
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'PortfolioDataRequest',
      overrides: mapOverridesToPairs(options.overrides),
      security: toRequestString(portfolio),
      service: '//blp/refdata',
    });
  }

  public async bcurves(ticker: string, options: RequestOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      elements: [{ key: 'query', value: toRequestString(ticker) }],
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'curveListRequest',
      service: '//blp/instruments',
    });
  }

  public async bgovts(ticker: string, options: RequestOptions = {}): Promise<unknown> {
    return await this.request({
      backend: options.backend,
      elements: [{ key: 'query', value: toRequestString(ticker) }],
      format: options.format,
      kwargs: mapObjectToPairs(options.kwargs),
      operation: 'govtListRequest',
      service: '//blp/instruments',
    });
  }

  public async resolveFieldTypes(
    fields: readonly string[],
    overrides?: OverridesMap,
    defaultType = 'string',
  ): Promise<Record<string, string>> {
    const items = await this.inner.resolveFieldTypes(
      fields,
      mapObjectToPairs(overrides),
      defaultType,
    );
    return Object.fromEntries(items.map((item) => [item.key, item.value]));
  }

  public getFieldInfo(field: string): FieldInfo | null {
    return this.inner.getFieldInfo(field);
  }

  public clearFieldCache(): void {
    this.inner.clearFieldCache();
  }

  public saveFieldCache(): void {
    this.inner.saveFieldCache();
  }

  public async validateFields(fields: readonly string[]): Promise<string[]> {
    return await this.inner.validateFields(fields);
  }

  public isFieldValidationEnabled(): boolean {
    return this.inner.isFieldValidationEnabled();
  }

  public async getSchema(service: string): Promise<unknown> {
    const cached = this.parsedSchemas.get(service);
    if (cached !== undefined) {
      return cached;
    }
    const parsed = JSON.parse(await this.inner.getSchema(service)) as unknown;
    this.parsedSchemas.set(service, parsed);
    return parsed;
  }

  public async getOperation(service: string, operation: string): Promise<unknown> {
    const key = `${service}\u0000${operation}`;
    const cached = this.parsedOperations.get(key);
    if (cached !== undefined) {
      return cached;
    }
    const parsed = JSON.parse(await this.inner.getOperation(service, operation)) as unknown;
    this.parsedOperations.set(key, parsed);
    return parsed;
  }

  public async listOperations(service: string): Promise<string[]> {
    return await this.inner.listOperations(service);
  }

  public getCachedSchema(service: string): unknown {
    const cached = this.parsedSchemas.get(service);
    if (cached !== undefined) {
      return cached;
    }
    const json = this.inner.getCachedSchema(service);
    if (json === null) {
      return null;
    }
    const parsed = JSON.parse(json) as unknown;
    this.parsedSchemas.set(service, parsed);
    return parsed;
  }

  public invalidateSchema(service: string): void {
    this.inner.invalidateSchema(service);
    this.parsedSchemas.delete(service);
    for (const key of this.parsedOperations.keys()) {
      if (key.startsWith(`${service}\u0000`)) {
        this.parsedOperations.delete(key);
      }
    }
  }

  public clearSchemaCache(): void {
    this.inner.clearSchemaCache();
    this.parsedSchemas.clear();
    this.parsedOperations.clear();
  }

  public listCachedSchemas(): string[] {
    return this.inner.listCachedSchemas();
  }

  public async getEnumValues(
    service: string,
    operation: string,
    element: string,
  ): Promise<string[] | null> {
    return await this.inner.getEnumValues(service, operation, element);
  }

  public async listValidElements(service: string, operation: string): Promise<string[] | null> {
    return await this.inner.listValidElements(service, operation);
  }

  public async subscribe(
    tickers: readonly string[],
    fields: readonly string[],
    options: StreamOptions = {},
  ): Promise<Subscription> {
    try {
      const subscriptionOptions = buildStreamSubscriptionOptions(MKTDATA_SERVICE, options);
      const useOptions =
        subscriptionOptions !== undefined ||
        options.flushThreshold !== undefined ||
        options.overflowPolicy !== undefined ||
        options.streamCapacity !== undefined;
      const stream = useOptions
        ? await this.inner.subscribeWithOptions(
            MKTDATA_SERVICE,
            tickers,
            fields,
            subscriptionOptions,
            options.flushThreshold,
            options.overflowPolicy,
            options.streamCapacity,
            options.allFields,
          )
        : await this.inner.subscribe(tickers, fields, options.allFields);
      return new Subscription(stream);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async subscribeWithOptions(
    service: string,
    tickers: readonly string[],
    fields: readonly string[],
    options?: readonly string[],
    flushThreshold?: number,
    overflowPolicy?: string,
    streamCapacity?: number,
    allFields?: boolean,
  ): Promise<Subscription> {
    try {
      const stream = await this.inner.subscribeWithOptions(
        service,
        tickers,
        fields,
        options,
        flushThreshold,
        overflowPolicy,
        streamCapacity,
        allFields,
      );
      return new Subscription(stream);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public signalShutdown(): void {
    this.inner.signalShutdown();
  }

  public isAvailable(): boolean {
    return this.inner.isAvailable();
  }

  public async stream(
    tickers: readonly string[],
    fields: readonly string[],
    options: StreamOptions = {},
  ): Promise<Subscription> {
    return await this.subscribeWithOptions(
      MKTDATA_SERVICE,
      tickers,
      fields,
      buildStreamSubscriptionOptions(MKTDATA_SERVICE, options),
      options.flushThreshold,
      options.overflowPolicy,
      options.streamCapacity,
      options.allFields,
    );
  }

  public async vwap(
    tickers: readonly string[],
    fields: readonly string[],
    options: StreamOptions = {},
  ): Promise<Subscription> {
    return await this.subscribeWithOptions(
      '//blp/mktvwap',
      tickers,
      fields,
      buildStreamSubscriptionOptions('//blp/mktvwap', options),
      options.flushThreshold,
      options.overflowPolicy,
      options.streamCapacity,
      options.allFields,
    );
  }

  public async mktbar(ticker: string, options: StreamOptions = {}): Promise<Subscription> {
    return await this.subscribeWithOptions(
      '//blp/mktbar',
      [ticker],
      options.fields ?? [],
      buildStreamSubscriptionOptions('//blp/mktbar', options),
      options.flushThreshold,
      options.overflowPolicy,
      options.streamCapacity,
      options.allFields,
    );
  }

  public async depth(ticker: string, options: StreamOptions = {}): Promise<Subscription> {
    return await this.subscribeWithOptions(
      '//blp/mktdepthdata',
      [ticker],
      options.fields ?? [],
      buildStreamSubscriptionOptions('//blp/mktdepthdata', options),
      options.flushThreshold,
      options.overflowPolicy,
      options.streamCapacity,
      options.allFields,
    );
  }

  public async chains(ticker: string, options: StreamOptions = {}): Promise<Subscription> {
    return await this.subscribeWithOptions(
      '//blp/mktlist',
      [ticker],
      options.fields ?? [],
      buildStreamSubscriptionOptions('//blp/mktlist', options),
      options.flushThreshold,
      options.overflowPolicy,
      options.streamCapacity,
      options.allFields,
    );
  }

  public async bops(service: string): Promise<string[]> {
    return await this.inner.listOperations(service);
  }

  public async bschema(service: string, operation?: string): Promise<unknown> {
    return operation === undefined
      ? await this.getSchema(service)
      : await this.getOperation(service, operation);
  }

  public async fieldInfo(
    fields: string | readonly string[],
    options: BfldsOptions = {},
  ): Promise<unknown> {
    return await this.bflds({
      fields: toStringArray(fields),
      ...options,
    });
  }

  public async fieldSearch(searchSpec: string, options: BfldsOptions = {}): Promise<unknown> {
    return await this.bflds({ searchSpec: toRequestString(searchSpec), ...options });
  }

  // ── Recipes ─────────────────────────────────────────────────────────

  public async bqr(ticker: string, options: BqrOptions = {}): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeBqr(
        toRequestString(ticker),
        formatDateTime(options.startDatetime),
        formatDateTime(options.endDatetime),
        options.eventTypes ?? null,
        options.includeBrokerCodes !== false,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async yas(
    tickers: string | readonly string[],
    fields: string | readonly string[],
    options: YasOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeYas(
        toStringArray(tickers),
        toStringArray(fields),
        formatDate(options.settleDt),
        options.yieldType ?? undefined,
        options.spread ?? undefined,
        options.yieldVal ?? undefined,
        options.price ?? undefined,
        options.benchmark ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async preferreds(equityTicker: string, options: PreferredsOptions = {}): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipePreferreds(
        toRequestString(equityTicker),
        options.fields !== undefined ? toStringArray(options.fields) : null,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async corporateBonds(
    ticker: string,
    options: CorporateBondsOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeCorporateBonds(
        toRequestString(ticker),
        options.ccy ?? undefined,
        options.fields !== undefined ? toStringArray(options.fields) : null,
        options.activeOnly !== false,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async futTicker(
    genTicker: string,
    dt: DateLike,
    options: FuturesResolveOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeFutTicker(
        toRequestString(genTicker),
        formatDate(dt) ?? '',
        options.freq ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async activeFutures(
    genTicker: string,
    dt: DateLike,
    options: FuturesResolveOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeActiveFutures(
        toRequestString(genTicker),
        formatDate(dt) ?? '',
        options.freq ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async futuresCurve(
    genTicker: string,
    options: FuturesCurveOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeFuturesCurve(
        toRequestString(genTicker),
        options.asof === undefined ? undefined : (formatDate(options.asof) ?? ''),
        options.chainField ?? undefined,
        options.fields !== undefined ? toStringArray(options.fields) : null,
        options.maxContracts ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Resolve a generic CDX ticker to the series that applies on `dt`.
   *
   * The series is the highest one whose Bloomberg
   * `CDS_FIRST_ACCRUAL_START_DATE` is on or before `dt`, so the result never
   * moves backwards as `dt` advances. Roll dates come from Bloomberg rather
   * than the nominal semi-annual cadence, because they are business-day
   * adjusted: CDX.NA.IG.45 starts 2025-09-22, so 2025-09-21 is still S44.
   *
   * The `Vn` token is the latest version Bloomberg reports for the resolved
   * series. That is also the only version carrying the series' price history,
   * so it is the identity to price against for any date in the series.
   */
  public async cdxTicker(
    genTicker: string,
    dt: DateLike,
    options: CdxResolveOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeCdxTicker(
        toRequestString(genTicker),
        formatDate(dt) ?? '',
        options.versionless ?? false,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Resolve the latest CDX series that had started *and* traded by `dt`.
   *
   * Matches {@link Engine.cdxTicker} except between a roll and the new
   * series' first print, when the preceding series is still the traded one --
   * CDX.NA.HY.46 started 2026-03-20 but first printed 2026-03-27.
   *
   * `lookbackDays` sets a minimum activity window; the window always reaches
   * back to the resolved series' first accrual date, so "this series has
   * traded" can only ever flip false to true.
   */
  public async activeCdx(
    genTicker: string,
    dt: DateLike,
    options: ActiveCdxOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeActiveCdx(
        toRequestString(genTicker),
        formatDate(dt) ?? '',
        options.lookbackDays ?? undefined,
        options.versionless ?? false,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async dividend(
    tickers: string | readonly string[],
    startDate: DateLike,
    endDate: DateLike,
    options: DividendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeDividend(
        toStringArray(tickers),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
        options.dvdType ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async dividendYield(
    tickers: string | readonly string[],
    startDate: DateLike,
    endDate: DateLike,
    options: DividendYieldOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeDividendYield(
        toStringArray(tickers),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
        options.dividendTypes !== undefined ? toStringArray(options.dividendTypes) : null,
        options.windowDays ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async turnover(
    tickers: string | readonly string[],
    startDate: DateLike,
    endDate: DateLike,
    options: TurnoverOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeTurnover(
        toStringArray(tickers),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
        options.ccy ?? undefined,
        options.factor ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async etfHoldings(etfTicker: string, options: EtfHoldingsOptions = {}): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeEtfHoldings(
        toRequestString(etfTicker),
        options.fields !== undefined ? toStringArray(options.fields) : null,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async volSurface(
    tickers: string | readonly string[],
    startDate: DateLike,
    endDate: DateLike,
    options: VolSurfaceOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeVolSurface(
        toStringArray(tickers),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
        normalizeVolPresets(options.preset ?? 'MONEYNESS_30D'),
        normalizeVolFieldSpecs(options.fields),
        options.asDecimal ?? true,
        options.includeDerived ?? false,
        options.riskFreeRate ?? undefined,
        options.dividendYieldField ?? undefined,
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async indexMembers(index: string, options: IndexMembersOptions = {}): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeIndexMembers(
        toRequestString(index),
        options.field ?? undefined,
        options.asof === undefined ? undefined : (formatDate(options.asof) ?? ''),
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async resolveIsins(
    isins: string | readonly string[],
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeResolveIsins(toStringArray(isins));
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async issuerIsins(
    bondIsins: string | readonly string[],
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeIssuerIsins(toStringArray(bondIsins));
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async etfNavRelationships(
    tickers: string | readonly string[],
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeEtfNavRelationships(
        toStringArray(tickers).map((ticker) => ticker.trim()),
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async etfNavSnapshot(
    tickers: string | readonly string[],
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeEtfNavSnapshot(
        toStringArray(tickers).map((ticker) => ticker.trim()),
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  public async etfNavHistory(
    tickers: string | readonly string[],
    startDate: DateLike,
    endDate: DateLike,
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeEtfNavHistory(
        toStringArray(tickers).map((ticker) => ticker.trim()),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }

  /**
   * Subscribe to real-time iNAV updates for ETFs after atomic preflight.
   *
   * Resolves every source ETF's validated iNAV Index target first and only
   * then opens one stream over the resolved iNAV tickers. Stream topics are
   * the normalized iNAV tickers, so dynamic `add`/`remove` on the returned
   * subscription expect already-resolved iNAV tickers, not source ETFs.
   */
  public async subscribeEtfInav(
    tickers: string | readonly string[],
    options: StreamOptions = {},
  ): Promise<Subscription> {
    const etfList = toStringArray(tickers).map((ticker) => ticker.trim());
    if (etfList.length === 0) {
      throw new BlpValidationError('etfs must not be empty', { element: 'tickers' });
    }
    const duplicates = firstSeenDuplicates(etfList);
    if (duplicates.length > 0) {
      throw new BlpValidationError(
        `Duplicate ETF inputs are not allowed: ${duplicates.join(', ')}`,
        { element: 'tickers' },
      );
    }

    let relationships: Table;
    try {
      relationships = toArrowTableFromNative(await this.inner.recipeEtfNavRelationships(etfList));
    } catch (error) {
      throw wrapError(error);
    }
    const inavTickers = validatedInavTickers(etfList, relationships);

    const { fields = ['LAST_PRICE'], ...streamOptions } = options;
    return await this.subscribe(inavTickers, fields, streamOptions);
  }

  public async currencyConversion(
    ticker: string,
    targetCcy: string,
    startDate: DateLike,
    endDate: DateLike,
    options: RecipeBackendOptions = {},
  ): Promise<unknown> {
    const backend = normalizeBackend(options.backend);
    try {
      const buffer = await this.inner.recipeCurrencyConversion(
        toRequestString(ticker),
        toRequestString(targetCcy),
        formatDate(startDate) ?? '',
        formatDate(endDate) ?? '',
      );
      return nativeArrowToBackend(buffer, backend);
    } catch (error) {
      throw wrapError(error);
    }
  }
}

// ── Top-level wrappers ──────────────────────────────────────────────────

export async function connect(config?: EngineConfig): Promise<Engine> {
  return await Engine.connect(config);
}

export function configure(config?: EngineConfig): EngineConfig | undefined;
export function configure(host?: string, port?: number): EngineConfig | undefined;
export function configure(
  configOrHost?: EngineConfig | string,
  port?: number,
): EngineConfig | undefined {
  configuredEngineConfig = normalizeConfigureArgs(configOrHost, port);
  clearConfiguredEngine();
  return configuredEngineConfig;
}

export async function abdp(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: BdpOptions = {},
): Promise<unknown> {
  const engine = await getConfiguredEngine();
  return await engine.bdp(toStringArray(tickers), toStringArray(fields), options);
}

export async function bdp(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: BdpOptions = {},
): Promise<unknown> {
  return await abdp(tickers, fields, options);
}

export async function abdh(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  start?: DateLike | BdhOptions,
  end?: DateLike,
  options: BdhOptions = {},
): Promise<unknown> {
  const engine = await getConfiguredEngine();
  // ``BdhOptions`` is a plain object literal; Dates / Luxon DateTimes are
  // Typed objects so they fall through to the date-typed branch.
  if (isBdhOptionsInput(start)) {
    if (end !== undefined) {
      throw new TypeError('abdh options object cannot be combined with a positional end date');
    }
    return await engine.bdh(toStringArray(tickers), toStringArray(fields), start);
  }
  return await engine.bdh(toStringArray(tickers), toStringArray(fields), {
    ...options,
    end,
    start,
  });
}

export async function bdh(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: BdhOptions = {},
): Promise<unknown> {
  return await abdh(tickers, fields, options);
}

export async function abds(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  overrides?: OverridesInput,
  options: BdpOptions = {},
): Promise<unknown> {
  const engine = await getConfiguredEngine();
  const normalizedOptions: BdpOptions =
    overrides === undefined
      ? options
      : { ...options, overrides: ovr(options.overrides ?? {}, overrides) };
  return await engine.bds(toStringArray(tickers), toStringArray(fields), normalizedOptions);
}

export async function bds(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: BdpOptions = {},
): Promise<unknown> {
  return await abds(tickers, fields, undefined, options);
}

export async function abdib(
  ticker: string,
  dt?: DateTimeLike | BdibOptions,
  interval: number | BdibOptions = 1,
  options: BdibOptions = {},
): Promise<unknown> {
  const engine = await getConfiguredEngine();
  // Distinguish a BdibOptions plain object from a Date / Luxon DateTime, both
  // Of which would also pass an ``isPlainObject`` check on bare typeof checks.
  if (isBdibOptionsInput(dt) && interval === 1 && Object.keys(options).length === 0) {
    return await engine.bdib(toRequestString(ticker), dt);
  }
  const normalizedOptions: BdibOptions = isBdibOptionsInput(interval)
    ? { ...interval }
    : { ...options, interval: typeof interval === 'number' ? interval : 1 };
  if (normalizedOptions.start === undefined && normalizedOptions.end === undefined) {
    if (dt === undefined || isBdibOptionsInput(dt)) {
      throw new TypeError('abdib requires dt or explicit start/end options');
    }
    const range = fullDayRange(dt);
    normalizedOptions.start = range.start;
    normalizedOptions.end = range.end;
  }
  return await engine.bdib(toRequestString(ticker), normalizedOptions);
}

export async function bdib(ticker: string, options: BdibOptions = {}): Promise<unknown> {
  return await abdib(ticker, options);
}

export async function abdtick(
  ticker: string,
  start: DateTimeLike | null | undefined,
  end: DateTimeLike | null | undefined,
  options: BdtickOptions = {},
): Promise<unknown> {
  if (start === undefined || start === null || end === undefined || end === null) {
    throw new TypeError('abdtick requires both start and end datetimes');
  }
  const engine = await getConfiguredEngine();
  return await engine.bdtick(toRequestString(ticker), { ...options, end, start });
}

export async function bdtick(ticker: string, options: BdtickOptions = {}): Promise<unknown> {
  const engine = await getConfiguredEngine();
  return await engine.bdtick(toRequestString(ticker), options);
}

export async function asubscribe(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: StreamOptions = {},
): Promise<Subscription> {
  const engine = await getConfiguredEngine();
  return await engine.subscribe(toStringArray(tickers), toStringArray(fields), options);
}

export async function subscribe(
  tickers: string | readonly string[],
  fields: string | readonly string[],
  options: StreamOptions = {},
): Promise<Subscription> {
  return await asubscribe(tickers, fields, options);
}

async function acdxInfo(ticker: string, options: BdpOptions = {}): Promise<unknown> {
  const engine = await getConfiguredEngine();
  return await engine.bdp([toRequestString(ticker)], [...CDX_INFO_FIELDS], options);
}

async function acdxPricing(ticker: string, options: CdxOptions = {}): Promise<unknown> {
  const engine = await getConfiguredEngine();
  return await engine.bdp(
    [toRequestString(ticker)],
    [...CDX_PRICING_FIELDS],
    normalizeRecoveryOptions(options),
  );
}

async function acdxRisk(ticker: string, options: CdxOptions = {}): Promise<unknown> {
  const engine = await getConfiguredEngine();
  return await engine.bdp(
    [toRequestString(ticker)],
    [...CDX_RISK_FIELDS],
    normalizeRecoveryOptions(options),
  );
}

export const blp = Object.freeze({
  abdh,
  abdib,
  abdp,
  abds,
  abdtick,
  asubscribe,
  bdh,
  bdib,
  bdp,
  bds,
  bdtick,
  subscribe,
});

export const ext = Object.freeze({
  buildCorporateBondsQuery: native.extBuildCorporateBondsQuery,

  buildEarningHeaderRename: native.extBuildEarningHeaderRename,
  buildEtfHoldingsQuery: native.extBuildEtfHoldingsQuery,

  buildFuturesTicker: native.extBuildFuturesTicker,
  buildFxPair: native.extBuildFxPair,

  buildPreferredsQuery: native.extBuildPreferredsQuery,
  buildYasOverrides: native.extBuildYasOverrides,
  calculateLevelPercentages: native.extCalculateLevelPercentages,
  cdx: Object.freeze({
    acdx_info: acdxInfo,
    acdx_pricing: acdxPricing,
    acdx_risk: acdxRisk,
  }),
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

export function version(): string {
  return packageJson.version;
}

export const { setLogLevel } = native;
export const { getLogLevel } = native;

// Issue #317: native datetime/date acceptance helpers, re-exported.
export { formatDate, formatDateTime } from './dates';

export {
  BlpError,
  BlpSessionError,
  BlpLimitError,
  BlpRequestError,
  BlpSubscriptionDataLossError,
  BlpValidationError,
  BlpTimeoutError,
  BlpInternalError,
  wrapError,
};

export type {
  ActiveCdxOptions,
  AuthConfig,
  BackendKind,
  BdhOptions,
  BdibOptions,
  BdpOptions,
  BdtickOptions,
  BeqsOptions,
  BfldsOptions,
  BlkpOptions,
  BloombergFieldException,
  BloombergMetadataError,
  BqlOptions,
  BqrOptions,
  BsrchOptions,
  BtaOptions,
  CdxOptions,
  CdxResolveOptions,
  CdxTickerInfo,
  CorporateBondsOptions,
  DateLike,
  DateTimeLike,
  DividendOptions,
  DividendYieldOptions,
  EngineConfig,
  EntitlementReport,
  EtfHoldingsOptions,
  ExchangeInfoResult,
  ExchangeOverrideInput,
  FieldInfo,
  FormatKind,
  FuturesCandidate,
  FuturesResolveOptions,
  FuturesCurveOptions,
  FxPairInfo,
  MarketRule,
  OverridesMap,
  OverrideEntry,
  OverrideNestedSource,
  OverrideObject,
  OverrideSource,
  OverrideSpecLike,
  OverrideValue,
  OverridesInput,
  IndexMembersOptions,
  PreferredsOptions,
  PrimitiveValue,
  RecipeBackendOptions,
  RequestInput,
  RequestOptions,
  ResultMetadata,
  SeatType,
  ServerAddress,
  SecurityOverrideSpec,
  SessionWindowsInfo,
  Socks5Config,
  StreamOptions,
  StringPair,
  SubscriptionReadOptions,
  SubscriptionStats,
  TickerParts,
  TimeRange,
  TlsConfig,
  TurnoverOptions,
  VolFieldSpec,
  VolSurfaceOptions,
  VolSurfacePreset,
  YasOptions,
};

// Closed string sets generated from defs/bloomberg.toml. These appear in
// EngineConfig and StreamOptions; FormatKind is re-exported above.
export type { OverflowPolicy, RequestFormat, SdkLogLevel, ValidationMode } from './types';
