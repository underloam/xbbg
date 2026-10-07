import * as z from "zod/v3";

import type { OverflowPolicy } from "./_defs_gen";
import {
  HISTORICAL_FORMATS,
  MAX_BLOOMBERG_EID,
  MAX_ENTITLEMENT_EIDS,
  OVERFLOW_POLICIES,
  OVERFLOW_POLICY_DEFAULT,
  OVERFLOW_POLICY_DOCS,
  OVERFLOW_POLICY_VALUES,
  REFERENCE_FORMATS,
} from "./_defs_gen";
import {
  boundedArray,
  nonEmptyString,
  overridesMap,
  primitiveMap,
  stringArray,
  type OverrideMap,
  type PrimitiveMap,
  type ZodOutput,
} from "./bounded-schemas";

import type { NormalizedBloombergToolsOptions } from "./options";

type ReferenceFormat = (typeof REFERENCE_FORMATS)[number];
type HistoricalFormat = (typeof HISTORICAL_FORMATS)[number];
interface ReferenceCallOptions {
  readonly securities: readonly string[];
  readonly overrides?: OverrideMap;
  readonly kwargs?: PrimitiveMap;
  readonly returnEids?: boolean;
  readonly validateFields?: boolean;
}

export interface BdpInput extends ReferenceCallOptions {
  readonly fields: readonly string[];
  readonly format?: ReferenceFormat;
  readonly includeSecurityErrors?: boolean;
}

export interface BdsInput extends ReferenceCallOptions {
  readonly field: string;
}

export interface BdhInput extends ReferenceCallOptions {
  readonly fields: readonly string[];
  readonly start: string;
  readonly end: string;
  readonly format?: HistoricalFormat;
}

export interface BdibInput {
  readonly ticker: string;
  readonly start: string;
  readonly end: string;
  readonly interval: number;
  readonly eventType?: string;
  readonly requestTz?: string;
  readonly outputTz?: string;
  readonly kwargs?: PrimitiveMap;
  readonly returnEids?: boolean;
}

export interface BdtickInput {
  readonly ticker: string;
  readonly start: string;
  readonly end: string;
  readonly eventTypes?: readonly string[];
  readonly includeConditionCodes?: boolean;
  readonly includeExchangeCodes?: boolean;
  readonly includeBrokerCodes?: boolean;
  readonly includeRpsCodes?: boolean;
  readonly includeBicMicCodes?: boolean;
  readonly includeNonPlottableEvents?: boolean;
  readonly includeBloombergStandardConditionCodes?: boolean;
  readonly requestTz?: string;
  readonly outputTz?: string;
  readonly kwargs?: PrimitiveMap;
  readonly returnEids?: boolean;
}

export interface CheckEntitlementsInput {
  readonly eids: readonly number[];
  readonly service?: string;
}

export interface BqlInput {
  readonly query: string;
  readonly kwargs?: PrimitiveMap;
}

export interface BsrchInput {
  readonly searchSpec: string;
  readonly overrides?: PrimitiveMap;
  readonly kwargs?: PrimitiveMap;
}

export interface BqrInput {
  readonly ticker: string;
  readonly start: string;
  readonly end: string;
  readonly eventTypes?: readonly string[];
  readonly includeBrokerCodes?: boolean;
}

export interface BfldsInput {
  readonly fields?: readonly string[];
  readonly searchSpec?: string;
  readonly kwargs?: PrimitiveMap;
}

export interface BeqsInput {
  readonly screen: string;
  readonly asof?: string;
  readonly screenType?: string;
  readonly group?: string;
  readonly overrides?: PrimitiveMap;
  readonly kwargs?: PrimitiveMap;
}

export interface YasInput {
  readonly tickers: readonly string[];
  readonly fields: readonly string[];
  readonly settleDt?: string;
  readonly yieldType?: number;
  readonly spread?: number;
  readonly yieldVal?: number;
  readonly price?: number;
  readonly benchmark?: string;
}

export interface PreferredsInput {
  readonly equityTicker: string;
  readonly fields?: readonly string[];
}

export interface CorporateBondsInput {
  readonly ticker: string;
  readonly ccy?: string;
  readonly fields?: readonly string[];
}

export interface IndexMembersInput {
  readonly index: string;
  readonly field?: "INDX_MWEIGHT" | "INDX_MEMBERS" | "INDX_MEMBERS3";
  readonly asof?: string;
}

export interface ResolveIsinsInput {
  readonly isins: readonly string[];
}

export interface ResolveVenuesInput {
  readonly securities: readonly string[];
  readonly pcsOverrides?: Record<string, string>;
}

export interface AuctionSnapshotInput extends ResolveVenuesInput {
  readonly fields?: readonly string[];
}

export interface IssuerIsinsInput {
  readonly bondIsins: readonly string[];
}

export interface EtfHoldingsInput {
  readonly etfTicker: string;
  readonly fields?: readonly string[];
}

export interface StreamSnapshotInput {
  readonly tickers: readonly string[];
  readonly fields: readonly string[];
  readonly maxUpdates: number;
  readonly timeoutMs: number;
  readonly drain?: boolean;
  readonly options?: readonly string[];
  readonly conflate?: boolean;
  readonly flushThreshold?: number;
  readonly overflowPolicy?: OverflowPolicy;
  readonly streamCapacity?: number;
  readonly allFields?: boolean;
}

export interface MktbarSnapshotInput {
  readonly ticker: string;
  readonly fields?: readonly string[];
  readonly maxUpdates: number;
  readonly timeoutMs: number;
  readonly drain?: boolean;
  readonly options?: readonly string[];
  readonly conflate?: boolean;
  readonly flushThreshold?: number;
  readonly overflowPolicy?: OverflowPolicy;
  readonly streamCapacity?: number;
  readonly allFields?: boolean;
}

export interface DepthSnapshotInput {
  readonly ticker: string;
  readonly fields?: readonly string[];
  readonly maxUpdates: number;
  readonly timeoutMs: number;
  readonly drain?: boolean;
  readonly options?: readonly string[];
  readonly conflate?: boolean;
  readonly flushThreshold?: number;
  readonly overflowPolicy?: OverflowPolicy;
  readonly streamCapacity?: number;
  readonly allFields?: boolean;
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/u;
const BBG_DATE_RE = /^\d{8}$/u;
const AMBIGUOUS_DATE_RE = /^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}([T \D]|$)/u;
const ISO_DATE_TIME_RE =
  /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$/u;

/** Integers in this range are YYYYMMDD calendar dates, never epoch milliseconds. */
const MIN_NUMERIC_BBG_DATE = 19_000_101;
const MAX_NUMERIC_BBG_DATE = 29_991_231;
/** Smallest numeric value interpreted as epoch milliseconds (~1973-03-03). */
const MIN_EPOCH_MS = 100_000_000_000;

function dateFromParts(year: string, month: string, day: string): string {
  const formatted = `${year}${month}${day}`;
  const parsed = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getUTCFullYear() !== Number(year) ||
    parsed.getUTCMonth() + 1 !== Number(month) ||
    parsed.getUTCDate() !== Number(day)
  ) {
    throw new TypeError(`Invalid date ${formatted}; expected a real calendar date like 2024-01-31`);
  }
  return formatted;
}

function dateToBbg(value: Date | number): string {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new TypeError("Invalid date value; expected YYYY-MM-DD, YYYYMMDD, Date, or epoch ms");
  }
  const year = String(date.getUTCFullYear()).padStart(4, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

/**
 * Disambiguates numeric date inputs: models routinely send YYYYMMDD as an
 * integer, which `new Date(number)` would silently read as epoch milliseconds
 * near 1970-01-01. Integers in the YYYYMMDD range parse as calendar dates;
 * large values parse as epoch milliseconds; everything between is rejected.
 */
function numericDateToBbg(value: number, unit: "date" | "datetime"): Date {
  if (Number.isFinite(value) && value >= MIN_EPOCH_MS) {
    return new Date(value);
  }
  throw new TypeError(
    `Ambiguous numeric ${unit} ${String(value)}; use "YYYY-MM-DD" text or epoch milliseconds`,
  );
}

/**
 * JSON tool calls can only carry strings and numbers, and `z.date()` is
 * unrepresentable in JSON Schema (zod v4's toJSONSchema throws on it), so the
 * wire contract is string | number only.
 */
function normalizeDate(value: string | number): string {
  if (typeof value === "number") {
    if (Number.isInteger(value) && value >= MIN_NUMERIC_BBG_DATE && value <= MAX_NUMERIC_BBG_DATE) {
      const text = String(value);
      return dateFromParts(text.slice(0, 4), text.slice(4, 6), text.slice(6, 8));
    }
    return dateToBbg(numericDateToBbg(value, "date"));
  }
  const text = value.trim();
  if (text.length === 0) {
    throw new TypeError("Date must be non-empty; use YYYY-MM-DD or YYYYMMDD");
  }
  if (AMBIGUOUS_DATE_RE.test(text)) {
    throw new TypeError(`Ambiguous date ${JSON.stringify(text)}; use YYYY-MM-DD or YYYYMMDD`);
  }
  if (BBG_DATE_RE.test(text)) {
    return dateFromParts(text.slice(0, 4), text.slice(4, 6), text.slice(6, 8));
  }
  if (ISO_DATE_RE.test(text)) {
    return dateFromParts(text.slice(0, 4), text.slice(5, 7), text.slice(8, 10));
  }
  throw new TypeError(`Invalid date ${JSON.stringify(text)}; use YYYY-MM-DD or YYYYMMDD`);
}

function normalizeDateTime(value: string | number): string {
  if (typeof value === "number") {
    if (Number.isInteger(value) && value >= MIN_NUMERIC_BBG_DATE && value <= MAX_NUMERIC_BBG_DATE) {
      throw new TypeError(
        `Invalid numeric datetime ${String(value)}; include an explicit time component such as "2024-01-02T09:30:00"`,
      );
    }
    return numericDateToBbg(value, "datetime").toISOString();
  }
  const text = value.trim();
  if (text.length === 0) {
    throw new TypeError("Datetime must be non-empty; use an ISO 8601 datetime");
  }
  if (AMBIGUOUS_DATE_RE.test(text)) {
    throw new TypeError(`Ambiguous datetime ${JSON.stringify(text)}; use ISO 8601`);
  }
  if (BBG_DATE_RE.test(text) || ISO_DATE_RE.test(text)) {
    throw new TypeError(
      `Invalid datetime ${JSON.stringify(text)}; include an explicit time component such as YYYY-MM-DDT09:30:00`,
    );
  }
  if (!ISO_DATE_TIME_RE.test(text)) {
    throw new TypeError(`Invalid datetime ${JSON.stringify(text)}; use ISO 8601`);
  }
  return text.replace(" ", "T");
}

/**
 * Reports a normalization failure as a zod issue. Errors *thrown* from
 * transforms reach LangChain as bare exceptions, which it masks with a
 * generic "did not match expected schema" message; zod issues keep the
 * actionable text visible to the model.
 */
function normalizationIssue(
  context: z.RefinementCtx,
  tool: string,
  field: string,
  error: unknown,
): typeof z.NEVER {
  context.addIssue({
    code: "custom",
    message: `${tool}: ${field}: ${error instanceof Error ? error.message : String(error)}`,
  });
  return z.NEVER;
}

function dateField(tool: string, field: string): ZodOutput<string> {
  return z
    .union([z.string(), z.number()])
    .transform((value, context) => {
      try {
        return normalizeDate(value);
      } catch (error) {
        return normalizationIssue(context, tool, field, error);
      }
    })
    .describe(
      `${field} date. Use YYYY-MM-DD or Bloomberg-native YYYYMMDD, never ambiguous MM/DD/YYYY.`,
    );
}

function dateTimeField(tool: string, field: string): ZodOutput<string> {
  return z
    .union([z.string(), z.number()])
    .superRefine((value, context) => {
      if (typeof value !== "string") {
        return;
      }
      const text = value.trim();
      if (BBG_DATE_RE.test(text) || ISO_DATE_RE.test(text)) {
        context.addIssue({
          code: "custom",
          message: `${tool}: ${field} datetime requires an explicit time component; use ISO 8601 such as YYYY-MM-DDT09:30:00`,
        });
      }
    })
    .transform((value, context) => {
      try {
        return normalizeDateTime(value);
      } catch (error) {
        return normalizationIssue(context, tool, field, error);
      }
    })
    .describe(`${field} datetime. Use ISO 8601 with an explicit time component.`);
}

function referenceFormat(tool: string): ZodOutput<ReferenceFormat | undefined> {
  return z
    .enum(REFERENCE_FORMATS, {
      errorMap: () => ({
        message: `${tool}: format must be one of ${REFERENCE_FORMATS.join(", ")}`,
      }),
    })
    .optional();
}

function historicalFormat(tool: string): ZodOutput<HistoricalFormat | undefined> {
  return z
    .enum(HISTORICAL_FORMATS, {
      errorMap: () => ({
        message: `${tool}: format must be one of ${HISTORICAL_FORMATS.join(", ")}`,
      }),
    })
    .optional();
}

/**
 * Closed enum for the subscription overflow policy.
 *
 * The value set, the default and the per-value semantics all come from the
 * generated vocabulary. This field previously accepted a free-form string and
 * documented no values at all, so models guessed `drop_oldest` — a policy that
 * was removed in 1.2.0 and that the engine rejects outright.
 */
function overflowPolicy(tool: string): ZodOutput<OverflowPolicy | undefined> {
  const semantics = OVERFLOW_POLICIES.map(
    (policy) => `${policy} - ${OVERFLOW_POLICY_DOCS[policy]}`,
  ).join(" ");
  return z
    .enum(OVERFLOW_POLICIES, {
      errorMap: () => ({
        message: `${tool}: overflowPolicy must be one of ${OVERFLOW_POLICY_VALUES}`,
      }),
    })
    .optional()
    .describe(
      `What the stream does when updates arrive faster than they are collected. ${semantics} Omit to use ${OVERFLOW_POLICY_DEFAULT}.`,
    );
}

interface ReferenceRequestShape {
  readonly kwargs: ZodOutput<PrimitiveMap | undefined>;
  readonly overrides: ZodOutput<OverrideMap | undefined>;
  readonly returnEids: ZodOutput<boolean | undefined>;
  readonly securities: ZodOutput<string[]>;
  readonly validateFields: ZodOutput<boolean | undefined>;
}

function referenceRequestFields(
  tool: string,
  options: NormalizedBloombergToolsOptions,
): ReferenceRequestShape {
  return {
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Advanced Bloomberg request kwargs as flat string/number/boolean values only.",
    ),
    overrides: overridesMap(tool, "overrides", options).describe(
      "Bloomberg field overrides. Use primitive values for global overrides and nested primitive maps keyed by exact security for per-security overrides.",
    ),
    returnEids: z
      .boolean()
      .optional()
      .describe("Request Bloomberg entitlement IDs and retain them in result metadata."),
    securities: stringArray(
      tool,
      "securities",
      options.maxSecurities,
      options.maxStringChars,
      '["<TICKER> <MARKET_SECTOR>"]',
    ).describe(
      "Securities exactly as the user supplied them: '<TICKER> <MARKET_SECTOR>' for Bloomberg tickers, '/isin/<ISIN>' for raw ISINs, '/cusip/<CUSIP>' for raw CUSIPs. Never invent, guess, or convert identifiers into tickers.",
    ),
    validateFields: z.boolean().optional().describe("Override field validation for this request."),
  };
}

export function createBdpSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BdpInput> {
  const tool = "xbbg_bdp";
  return z.object({
    ...referenceRequestFields(tool, options),
    fields: stringArray(
      tool,
      "fields",
      options.maxFields,
      options.maxStringChars,
      '["<FIELD>"]',
    ).describe("Bloomberg field mnemonics to retrieve. Use xbbg_bflds first if uncertain."),
    format: referenceFormat(tool).describe(
      "JSON output shape. Usually omit; use long_typed if downstream needs Bloomberg value types.",
    ),
    includeSecurityErrors: z
      .boolean()
      .optional()
      .describe("Include Bloomberg security errors in the response when supported."),
  });
}

export function createBdhSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BdhInput> {
  const tool = "xbbg_bdh";
  return z
    .object({
      ...referenceRequestFields(tool, options),
      end: dateField(tool, "end").describe("Required end date. Use YYYY-MM-DD or YYYYMMDD."),
      fields: stringArray(
        tool,
        "fields",
        options.maxFields,
        options.maxStringChars,
        '["<FIELD>"]',
      ).describe("Bloomberg historical field mnemonics supplied by the user."),
      format: historicalFormat(tool).describe(
        "Historical JSON output shape. Use semi_long only when the user asks for a table by date.",
      ),
      start: dateField(tool, "start").describe("Required start date. Use YYYY-MM-DD or YYYYMMDD."),
    })
    .superRefine((value, ctx) => {
      if (value.start > value.end) {
        ctx.addIssue({
          code: "custom",
          message: `${tool}: start must be on or before end. Use an explicit start/end date range.`,
          path: ["start"],
        });
      }
    });
}

export function createBdsSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BdsInput> {
  const tool = "xbbg_bds";
  return z.object({
    ...referenceRequestFields(tool, options),
    field: nonEmptyString(tool, "field", options.maxStringChars, "<BULK_FIELD>").describe(
      "Exactly one Bloomberg bulk/table field supplied by the user.",
    ),
  });
}

export function createBdibSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BdibInput> {
  const tool = "xbbg_bdib";
  return z.object({
    end: dateTimeField(tool, "end").describe(
      "Required intraday end datetime. Use ISO 8601 with timezone when possible.",
    ),
    eventType: nonEmptyString(tool, "eventType", options.maxStringChars, "<EVENT_TYPE>")
      .optional()
      .describe("Bloomberg event type supplied by the user."),
    interval: z
      .number()
      .int(`${tool}: interval must be a positive integer number of minutes. Example: 5`)
      .positive(`${tool}: interval must be greater than zero. Example: 5`)
      .describe("Bar interval in minutes. Must be a positive integer."),
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Advanced Bloomberg request kwargs as flat string/number/boolean values only.",
    ),
    outputTz: nonEmptyString(tool, "outputTz", options.maxStringChars, "<TIMEZONE>")
      .optional()
      .describe("Optional output timezone."),
    requestTz: nonEmptyString(tool, "requestTz", options.maxStringChars, "<TIMEZONE>")
      .optional()
      .describe("Timezone for naive start/end datetimes."),
    returnEids: z
      .boolean()
      .optional()
      .describe("Request Bloomberg entitlement IDs and retain them in result metadata."),
    start: dateTimeField(tool, "start").describe(
      "Required intraday start datetime. Use ISO 8601 with timezone when possible.",
    ),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "<TICKER> <MARKET_SECTOR>",
    ).describe(
      "One security exactly as the user supplied it: '<TICKER> <MARKET_SECTOR>', '/isin/<ISIN>', or '/cusip/<CUSIP>'. Never invent, guess, or convert identifiers into tickers.",
    ),
  });
}

export function createBdtickSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<BdtickInput> {
  const tool = "xbbg_bdtick";
  const includeFlag = z.boolean().optional().describe("Optional IntradayTickRequest include flag.");
  return z.object({
    end: dateTimeField(tool, "end").describe(
      "Required intraday tick end datetime. Use ISO 8601 with timezone when possible.",
    ),
    eventTypes: stringArray(
      tool,
      "eventTypes",
      options.maxFields,
      options.maxStringChars,
      '["<EVENT_TYPE>"]',
    )
      .optional()
      .describe('Bloomberg tick event types, for example ["<EVENT_TYPE>"].'),
    includeBicMicCodes: includeFlag,
    includeBloombergStandardConditionCodes: includeFlag,
    includeBrokerCodes: includeFlag,
    includeConditionCodes: includeFlag,
    includeExchangeCodes: includeFlag,
    includeNonPlottableEvents: includeFlag,
    includeRpsCodes: includeFlag,
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Advanced IntradayTickRequest kwargs as flat string/number/boolean values only.",
    ),
    outputTz: nonEmptyString(tool, "outputTz", options.maxStringChars, "<TIMEZONE>")
      .optional()
      .describe("Optional output timezone."),
    requestTz: nonEmptyString(tool, "requestTz", options.maxStringChars, "<TIMEZONE>")
      .optional()
      .describe("Timezone for naive start/end datetimes."),
    returnEids: z
      .boolean()
      .optional()
      .describe("Request Bloomberg entitlement IDs and retain them in result metadata."),
    start: dateTimeField(tool, "start").describe(
      "Required intraday tick start datetime. Use ISO 8601 with timezone when possible.",
    ),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "<TICKER> <MARKET_SECTOR>",
    ).describe(
      "One security exactly as the user supplied it: '<TICKER> <MARKET_SECTOR>', '/isin/<ISIN>', or '/cusip/<CUSIP>'. Never invent, guess, or convert identifiers into tickers.",
    ),
  });
}

export function createCheckEntitlementsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<CheckEntitlementsInput> {
  const tool = "xbbg_check_entitlements";
  return z.object({
    eids: z
      .array(
        z
          .number()
          .int(`${tool}: eids must contain integers only.`)
          .positive(`${tool}: eids must contain positive integers only.`)
          .max(MAX_BLOOMBERG_EID, `${tool}: eids must be signed 32-bit integers.`),
      )
      .nonempty(`${tool}: eids must contain at least one entitlement ID.`)
      .max(
        MAX_ENTITLEMENT_EIDS,
        `${tool}: eids must contain at most ${String(MAX_ENTITLEMENT_EIDS)} values.`,
      )
      .describe("Nonempty list of positive signed 32-bit Bloomberg entitlement IDs."),
    service: nonEmptyString(tool, "service", options.maxStringChars, "//blp/refdata")
      .optional()
      .default("//blp/refdata")
      .describe("Bloomberg service to check. Defaults to //blp/refdata."),
  });
}

export function createBqlSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BqlInput> {
  const tool = "xbbg_bql";
  return z.object({
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Advanced Bloomberg request kwargs as flat string/number/boolean values only.",
    ),
    query: nonEmptyString(tool, "query", options.maxBqlQueryChars, "<BQL_QUERY>").describe(
      "Complete BQL expression string with an explicit bounded universe; prefer BDP/BDH for simple reference or historical requests.",
    ),
  });
}

export function createBqrSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BqrInput> {
  const tool = "xbbg_bqr";
  return z.object({
    end: dateTimeField(tool, "end").describe(
      "Required BQR end datetime. Use ISO 8601 with timezone when possible.",
    ),
    eventTypes: stringArray(
      tool,
      "eventTypes",
      options.maxFields,
      options.maxStringChars,
      '["<EVENT_TYPE>"]',
    )
      .optional()
      .describe('BQR event types, for example ["<EVENT_TYPE>"].'),
    includeBrokerCodes: z
      .boolean()
      .optional()
      .describe("Include broker/dealer attribution columns. Defaults to true in @xbbg/core."),
    start: dateTimeField(tool, "start").describe(
      "Required BQR start datetime. Use ISO 8601 with timezone when possible.",
    ),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "/isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>",
    ).describe(
      "Fixed-income ticker or identifier with dealer quote source, for example /isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>.",
    ),
  });
}

export function createBsrchSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BsrchInput> {
  const tool = "xbbg_bsrch";
  return z.object({
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Search-grid kwargs as flat string/number/boolean values only.",
    ),
    overrides: primitiveMap(tool, "overrides", options).describe(
      "Search-grid overrides as flat string/number/boolean values only.",
    ),
    searchSpec: nonEmptyString(
      tool,
      "searchSpec",
      options.maxSearchSpecChars,
      "<SEARCH_SPEC>",
    ).describe(
      "Bloomberg search/grid domain or saved-search spec. Not for normal security lookup.",
    ),
  });
}

export function createBfldsSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BfldsInput> {
  const tool = "xbbg_bflds";
  return z
    .object({
      fields: stringArray(tool, "fields", options.maxFields, options.maxStringChars, '["<FIELD>"]')
        .optional()
        .describe(
          "Specific field mnemonics to inspect. Provide either fields or searchSpec, not both.",
        ),
      kwargs: primitiveMap(tool, "kwargs", options).describe(
        "Advanced Bloomberg request kwargs as flat string/number/boolean values only.",
      ),
      searchSpec: nonEmptyString(
        tool,
        "searchSpec",
        options.maxSearchSpecChars,
        "<FIELD_SEARCH_TEXT>",
      )
        .optional()
        .describe(
          "Field search text when the field mnemonic is unknown. Provide either searchSpec or fields, not both.",
        ),
    })
    .superRefine((value, ctx) => {
      const hasFields = value.fields !== undefined;
      const hasSearchSpec = value.searchSpec !== undefined;
      if (hasFields === hasSearchSpec) {
        ctx.addIssue({
          code: "custom",
          message: `${tool}: provide exactly one of fields or searchSpec. Example: {"fields":["<FIELD>"]}`,
          path: ["fields"],
        });
      }
    });
}

export function createBeqsSchema(options: NormalizedBloombergToolsOptions): ZodOutput<BeqsInput> {
  const tool = "xbbg_beqs";
  return z.object({
    asof: dateField(tool, "asof").optional().describe("Optional as-of date for the screen."),
    group: nonEmptyString(tool, "group", options.maxStringChars, "<BEQS_GROUP>")
      .optional()
      .describe("Bloomberg BEQS group when required by the screen."),
    kwargs: primitiveMap(tool, "kwargs", options).describe(
      "Advanced BEQS request kwargs as flat string/number/boolean values only.",
    ),
    overrides: primitiveMap(tool, "overrides", options).describe(
      "BEQS overrides as flat string/number/boolean values only.",
    ),
    screen: nonEmptyString(tool, "screen", options.maxStringChars, "<BEQS_SCREEN>").describe(
      "Existing Bloomberg BEQS screen name supplied by the user.",
    ),
    screenType: nonEmptyString(tool, "screenType", options.maxStringChars, "<SCREEN_TYPE>")
      .optional()
      .describe("Bloomberg BEQS screen type when required by the screen."),
  });
}

export function createYasSchema(options: NormalizedBloombergToolsOptions): ZodOutput<YasInput> {
  const tool = "xbbg_yas";
  return z.object({
    benchmark: nonEmptyString(tool, "benchmark", options.maxStringChars, "<BENCHMARK_TICKER>")
      .optional()
      .describe("Optional YAS benchmark supplied by the user."),
    fields: stringArray(
      tool,
      "fields",
      options.maxFields,
      options.maxStringChars,
      '["<YAS_FIELD>"]',
    ).describe("YAS field mnemonics supplied by the user."),
    price: z.number().optional().describe("Optional YAS price input."),
    settleDt: dateField(tool, "settleDt").optional().describe("Optional YAS settlement date."),
    spread: z.number().optional().describe("Optional YAS spread input."),
    tickers: stringArray(
      tool,
      "tickers",
      options.maxSecurities,
      options.maxStringChars,
      '["/isin/<ISIN> <MARKET_SECTOR>"]',
    ).describe(
      "Fixed-income securities exactly as the user supplied them: '<TICKER> <MARKET_SECTOR>' or identifier syntax such as '/isin/<ISIN> <MARKET_SECTOR>'. Never invent or guess tickers.",
    ),
    yieldType: z.number().int().optional().describe("Optional YAS yield type."),
    yieldVal: z.number().optional().describe("Optional YAS yield value input."),
  });
}

export function createPreferredsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<PreferredsInput> {
  const tool = "xbbg_preferreds";
  return z.object({
    equityTicker: nonEmptyString(
      tool,
      "equityTicker",
      options.maxStringChars,
      "<ISSUER_TICKER> <MARKET_SECTOR>",
    ).describe(
      "The issuer's common equity ticker as '<TICKER> <MARKET_SECTOR>', never a preferred ('Pfd') ticker and never a guessed one. Resolve a supplied ISIN/CUSIP with xbbg_resolve_isins first.",
    ),
    fields: boundedArray(
      nonEmptyString(tool, "fields", options.maxStringChars, '["<FIELD>"]'),
      options.maxFields,
      {
        minimum: 0,
        maximumMessage: `${tool}: fields can contain at most ${options.maxFields} values`,
      },
    )
      .transform((fields) => (fields.length === 0 ? undefined : fields))
      .optional()
      .describe("Optional fields to include in the preferreds recipe result."),
  });
}

export function createCorporateBondsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<CorporateBondsInput> {
  const tool = "xbbg_corporate_bonds";
  return z.object({
    ccy: nonEmptyString(tool, "ccy", options.maxStringChars, "<CCY>")
      .optional()
      .describe("Optional currency filter supplied by the user."),
    fields: stringArray(tool, "fields", options.maxFields, options.maxStringChars, '["<FIELD>"]')
      .optional()
      .describe("Optional fields to include in the corporate bond result."),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "<ISSUER_TICKER> <MARKET_SECTOR>",
    ).describe(
      "The issuer/company equity ticker as '<TICKER> <MARKET_SECTOR>' supplied by the user. Resolve a supplied ISIN/CUSIP with xbbg_resolve_isins first; never guess tickers.",
    ),
  });
}

export function createIndexMembersSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<IndexMembersInput> {
  const tool = "xbbg_index_members";
  return z.object({
    asof: dateField(tool, "asof").optional().describe("Optional index membership as-of date."),
    field: z
      .enum(["INDX_MWEIGHT", "INDX_MEMBERS", "INDX_MEMBERS3"])
      .optional()
      .describe("Bloomberg index members field. Omit for @xbbg/core default."),
    index: nonEmptyString(
      tool,
      "index",
      options.maxStringChars,
      "<INDEX_TICKER> <MARKET_SECTOR>",
    ).describe(
      "One Bloomberg index ticker as '<INDEX_TICKER> <MARKET_SECTOR>' supplied by the user; never guess index tickers.",
    ),
  });
}

export function createResolveIsinsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ResolveIsinsInput> {
  const tool = "xbbg_resolve_isins";
  return z.object({
    isins: stringArray(
      tool,
      "isins",
      options.maxSecurities,
      options.maxStringChars,
      '["<ISIN>"]',
    ).describe("Raw ISIN strings to resolve. Do not add /isin/ prefixes for this recipe."),
  });
}

interface VenueResolutionShape {
  readonly securities: ZodOutput<string[]>;
  readonly pcsOverrides: ZodOutput<Record<string, string> | undefined>;
}

function venueResolutionFields(
  tool: string,
  options: NormalizedBloombergToolsOptions,
): VenueResolutionShape {
  return {
    pcsOverrides: z
      .record(
        nonEmptyString(tool, "pcsOverrides exchange", options.maxStringChars, "NEW YORK"),
        nonEmptyString(tool, "pcsOverrides source", options.maxStringChars, "SNY2"),
      )
      .optional()
      .describe(
        'Per-call preferred exchange-name to pricing-source overrides, for example {"NEW YORK":"SNY2"}. Overrides win over packaged mappings; exchange names are normalized by the core and the returned venue pricing source is validated.',
      ),
    securities: stringArray(
      tool,
      "securities",
      options.maxSecurities,
      options.maxStringChars,
      '["IBM US Equity", "AAPL US Equity"]',
    ).describe(
      "Securities supplied by the user: Bloomberg tickers, valid bare ISINs, /isin/<ISIN>, or /bbgid/<FIGI>. The core routes composite equities and preferreds to validated primary venues and respects explicit venue inputs; never guess venue tickers. Results preserve input order, duplicates, and each row's status/error, subject to output limits.",
    ),
  };
}

export function createResolveVenuesSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ResolveVenuesInput> {
  return z.object({ ...venueResolutionFields("xbbg_resolve_venues", options) });
}

export function createAuctionSnapshotSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<AuctionSnapshotInput> {
  const tool = "xbbg_auction_snapshot";
  return z.object({
    ...venueResolutionFields(tool, options),
    fields: boundedArray(
      nonEmptyString(tool, "fields", options.maxStringChars, '["IMBALANCE_INDIC_RT"]'),
      options.maxFields,
      {
        minimum: 0,
        maximumMessage: `${tool}: fields can contain at most ${options.maxFields} values`,
      },
    )
      .optional()
      .describe(
        "Optional auction fields in requested column order. Omit or pass [] for AuctionFields.default. Values are returned only for validated venues; unresolved, unsupported, or mismatched rows retain status/error and have null field values.",
      ),
  });
}

export function createIssuerIsinsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<IssuerIsinsInput> {
  const tool = "xbbg_issuer_isins";
  return z.object({
    bondIsins: stringArray(
      tool,
      "bondIsins",
      options.maxSecurities,
      options.maxStringChars,
      '["<BOND_ISIN>"]',
    ).describe("Raw bond ISIN strings for issuer-level ISIN discovery."),
  });
}

export function createEtfHoldingsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<EtfHoldingsInput> {
  const tool = "xbbg_etf_holdings";
  return z.object({
    etfTicker: nonEmptyString(
      tool,
      "etfTicker",
      options.maxStringChars,
      "<ETF_TICKER> <MARKET_SECTOR>",
    ).describe(
      "One Bloomberg ETF ticker as '<ETF_TICKER> <MARKET_SECTOR>' supplied by the user. Resolve a supplied ISIN/CUSIP with xbbg_resolve_isins first; never guess tickers.",
    ),
    fields: stringArray(tool, "fields", options.maxFields, options.maxStringChars, '["<FIELD>"]')
      .optional()
      .describe("Optional fields to include in the ETF holdings recipe result."),
  });
}

interface SnapshotControlShape {
  readonly allFields: ZodOutput<boolean | undefined>;
  readonly conflate: ZodOutput<boolean | undefined>;
  readonly drain: ZodOutput<boolean | undefined>;
  readonly flushThreshold: ZodOutput<number | undefined>;
  readonly maxUpdates: ZodOutput<number>;
  readonly options: ZodOutput<string[] | undefined>;
  readonly overflowPolicy: ZodOutput<OverflowPolicy | undefined>;
  readonly streamCapacity: ZodOutput<number | undefined>;
  readonly timeoutMs: ZodOutput<number>;
}

function snapshotControlFields(
  tool: string,
  options: NormalizedBloombergToolsOptions,
): SnapshotControlShape {
  return {
    allFields: z.boolean().optional().describe("Request all Bloomberg fields when supported."),
    conflate: z
      .boolean()
      .optional()
      .describe("Enable Bloomberg conflated streaming when supported."),
    drain: z
      .boolean()
      .optional()
      .describe(
        "Flush buffered backlog while closing the subscription. The subscription always closes; collected output stays bounded either way. Defaults to false.",
      ),
    flushThreshold: z
      .number()
      .int()
      .positive()
      .optional()
      .describe("Optional stream flush threshold."),
    maxUpdates: z
      .number()
      .int(`${tool}: maxUpdates must be a positive integer.`)
      .positive(`${tool}: maxUpdates must be greater than zero.`)
      .max(
        options.maxStreamUpdates,
        `${tool}: maxUpdates can be at most ${options.maxStreamUpdates}.`,
      )
      .describe("Required maximum number of updates to collect before unsubscribing."),
    options: stringArray(
      tool,
      "options",
      options.maxFields,
      options.maxStringChars,
      '["interval=5"]',
    )
      .optional()
      .describe("Advanced Bloomberg subscription options."),
    overflowPolicy: overflowPolicy(tool),
    streamCapacity: z.number().int().positive().optional().describe("Optional stream capacity."),
    timeoutMs: z
      .number()
      .int()
      .positive()
      .max(options.maxStreamWaitMs, `${tool}: timeoutMs can be at most ${options.maxStreamWaitMs}.`)
      .optional()
      .default(options.maxStreamWaitMs)
      .describe("Maximum total wait in milliseconds before unsubscribing."),
  };
}

export function createStreamSnapshotSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<StreamSnapshotInput> {
  const tool = "xbbg_stream_snapshot";
  return z.object({
    fields: stringArray(
      tool,
      "fields",
      options.maxFields,
      options.maxStringChars,
      '["<FIELD>"]',
    ).describe("Bloomberg market-data fields to observe."),
    tickers: stringArray(
      tool,
      "tickers",
      options.maxSecurities,
      options.maxStringChars,
      '["<TICKER> <MARKET_SECTOR>"]',
    ).describe(
      "Securities to observe, exactly as the user supplied them: '<TICKER> <MARKET_SECTOR>', '/isin/<ISIN>', or '/cusip/<CUSIP>'. Never invent or guess tickers.",
    ),
    ...snapshotControlFields(tool, options),
  });
}

export function createMktbarSnapshotSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<MktbarSnapshotInput> {
  const tool = "xbbg_mktbar_snapshot";
  return z.object({
    fields: stringArray(tool, "fields", options.maxFields, options.maxStringChars, '["<FIELD>"]')
      .optional()
      .describe("Optional market-bar fields. Omit for Bloomberg defaults."),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "<TICKER> <MARKET_SECTOR>",
    ).describe(
      "One security to observe, exactly as the user supplied it: '<TICKER> <MARKET_SECTOR>', '/isin/<ISIN>', or '/cusip/<CUSIP>'. Never invent or guess tickers.",
    ),
    ...snapshotControlFields(tool, options),
  });
}

export function createDepthSnapshotSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<DepthSnapshotInput> {
  const tool = "xbbg_depth_snapshot";
  return z.object({
    fields: stringArray(tool, "fields", options.maxFields, options.maxStringChars, '["<FIELD>"]')
      .optional()
      .describe("Optional market-depth fields. Omit for Bloomberg defaults."),
    ticker: nonEmptyString(
      tool,
      "ticker",
      options.maxStringChars,
      "<TICKER> <MARKET_SECTOR>",
    ).describe(
      "One security to observe, exactly as the user supplied it: '<TICKER> <MARKET_SECTOR>', '/isin/<ISIN>', or '/cusip/<CUSIP>'. Never invent or guess tickers.",
    ),
    ...snapshotControlFields(tool, options),
  });
}
