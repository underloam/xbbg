import * as z from "zod/v3";

import { CHART_KINDS, CHART_SOURCES } from "./_defs_gen";
import { boundedArray, boundedString, type ZodOutput } from "./bounded-schemas";
import type { NormalizedBloombergToolsOptions } from "./options";

interface StringPair {
  readonly key: string;
  readonly value: string;
}

interface FuturesCandidate {
  readonly ticker: string;
  readonly year: number;
  readonly month: number;
}

type TickerOperation =
  | "parse_ticker"
  | "normalize_tickers"
  | "filter_equity_tickers"
  | "is_specific_contract"
  | "validate_generic_ticker";

type SingleTickerOperation = Exclude<
  TickerOperation,
  "normalize_tickers" | "filter_equity_tickers"
>;

interface SingleTickerInput {
  readonly operation: SingleTickerOperation;
  readonly ticker: string;
}

interface TickerListInput {
  readonly operation: "normalize_tickers" | "filter_equity_tickers";
  readonly tickers: readonly string[];
}

export type TickerInput = SingleTickerInput | TickerListInput;

interface FuturesBuildTickerInput {
  readonly operation: "build_futures_ticker";
  readonly prefix: string;
  readonly monthCode: string;
  readonly year: string;
  readonly asset: string;
}

interface FuturesGenerateCandidatesInput {
  readonly operation: "generate_candidates";
  readonly genTicker: string;
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly freq?: string;
  readonly count?: number;
}

interface FuturesContractIndexInput {
  readonly operation: "contract_index";
  readonly genTicker: string;
}

interface FuturesFilterCandidatesByCycleInput {
  readonly operation: "filter_candidates_by_cycle";
  readonly candidates: readonly FuturesCandidate[];
  readonly cycle: string;
}

interface FuturesFilterValidContractsInput {
  readonly operation: "filter_valid_contracts";
  readonly contracts: readonly StringPair[];
  readonly year: number;
  readonly month: number;
  readonly day: number;
}

interface FuturesMonthsInput {
  readonly operation: "get_futures_months";
}

export type FuturesInput =
  | FuturesBuildTickerInput
  | FuturesGenerateCandidatesInput
  | FuturesContractIndexInput
  | FuturesFilterCandidatesByCycleInput
  | FuturesFilterValidContractsInput
  | FuturesMonthsInput;

interface CdxTickerInput {
  readonly operation: "parse_cdx_ticker" | "previous_cdx_series" | "cdx_info";
  readonly ticker: string;
}

interface CdxMarketDataInput {
  readonly operation: "cdx_pricing" | "cdx_risk";
  readonly ticker: string;
  readonly recoveryRate?: number;
}

interface CdxGenToSpecificInput {
  readonly operation: "cdx_gen_to_specific";
  readonly genTicker: string;
  readonly series: number;
}

export type CdxInput = CdxTickerInput | CdxMarketDataInput | CdxGenToSpecificInput;

interface FxPairInput {
  readonly operation: "build_fx_pair";
  readonly fromCcy: string;
  readonly toCcy: string;
}

interface SameCurrencyInput {
  readonly operation: "same_currency";
  readonly ccy1: string;
  readonly ccy2: string;
}

interface CurrencyConversionInput {
  readonly operation: "currencies_needing_conversion";
  readonly currencies: readonly string[];
  readonly target: string;
}

export type CurrencyInput = FxPairInput | SameCurrencyInput | CurrencyConversionInput;

interface PreferredsQueryInput {
  readonly operation: "build_preferreds_query";
  readonly equityTicker: string;
  readonly extraFields?: readonly string[];
}

interface CorporateBondsQueryInput {
  readonly operation: "build_corporate_bonds_query";
  readonly ticker: string;
  readonly ccy?: string;
  readonly extraFields?: readonly string[];
}

interface EtfHoldingsQueryInput {
  readonly operation: "build_etf_holdings_query";
  readonly etfTicker: string;
  readonly extraFields?: readonly string[];
}

export type BqlBuilderInput =
  | PreferredsQueryInput
  | CorporateBondsQueryInput
  | EtfHoldingsQueryInput;

interface DeriveSessionsInput {
  readonly operation: "derive_sessions";
  readonly dayStart: string;
  readonly dayEnd: string;
  readonly mic?: string;
  readonly exchCode?: string;
}

interface MarketRuleInput {
  readonly operation: "get_market_rule";
  readonly mic?: string;
  readonly exchCode?: string;
}

interface InferTimezoneInput {
  readonly operation: "infer_timezone";
  readonly countryIso: string;
}

interface SessionTimesToUtcInput {
  readonly operation: "session_times_to_utc";
  readonly startTime: string;
  readonly endTime: string;
  readonly exchangeTz: string;
  readonly date: string;
}

interface TurnoverDatesInput {
  readonly operation: "default_turnover_dates";
  readonly startDate?: string;
  readonly endDate?: string;
}

interface BqrDatetimesInput {
  readonly operation: "default_bqr_datetimes";
  readonly startDatetime?: string;
  readonly endDatetime?: string;
}

interface ExchangeOverrideLookupInput {
  readonly operation: "get_exchange_override";
  readonly ticker: string;
}

interface ListExchangeOverridesInput {
  readonly operation: "list_exchange_overrides";
}

export type MarketSessionInput =
  | DeriveSessionsInput
  | MarketRuleInput
  | InferTimezoneInput
  | SessionTimesToUtcInput
  | TurnoverDatesInput
  | BqrDatetimesInput
  | ExchangeOverrideLookupInput
  | ListExchangeOverridesInput;

export interface YasOverridesInput {
  readonly settleDt?: string;
  readonly yieldType?: number;
  readonly spread?: number;
  readonly yieldVal?: number;
  readonly price?: number;
  readonly benchmark?: string;
}

interface ParseDateInput {
  readonly operation: "parse_date";
  readonly dateStr: string;
}

interface FmtDateInput {
  readonly operation: "fmt_date";
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly fmt?: string;
}

interface MonthCodeInput {
  readonly operation: "get_month_code";
  readonly monthName: string;
}

interface MonthNameInput {
  readonly operation: "get_month_name";
  readonly code: string;
}

interface DvdTypeInput {
  readonly operation: "get_dvd_type";
  readonly dvdType: string;
}

interface ConstantsLookupInput {
  readonly operation: "get_futures_months" | "get_dvd_types" | "get_dvd_cols" | "get_etf_cols";
}

export type ConstantsInput =
  | ParseDateInput
  | FmtDateInput
  | MonthCodeInput
  | MonthNameInput
  | DvdTypeInput
  | ConstantsLookupInput;

interface RenameColumnsInput {
  readonly operation: "rename_dividend_columns" | "rename_etf_columns";
  readonly columns: readonly string[];
}

interface EarningHeaderRenameInput {
  readonly operation: "build_earning_header_rename";
  readonly headerRow: readonly StringPair[];
  readonly dataColumns: readonly string[];
}

export type ColumnsInput = RenameColumnsInput | EarningHeaderRenameInput;

export interface CalculateInput {
  readonly operation: "calculate_level_percentages";
  readonly values: readonly (number | null)[];
  readonly levels: readonly (number | null)[];
}

export type BloombergChartSource = (typeof CHART_SOURCES)[number];

export type ChartKind = (typeof CHART_KINDS)[number];

export type ChartRenderer = "vega-lite";

export type ChartScalar = string | number | boolean | null;

export type ChartRow = Readonly<Record<string, ChartScalar>>;

export interface ChartSpecInput {
  readonly source: BloombergChartSource;
  readonly rows: readonly ChartRow[];
  readonly renderer?: ChartRenderer;
  readonly chart?: ChartKind;
  readonly title?: string;
  readonly xField?: string;
  readonly yFields?: readonly string[];
  readonly seriesField?: string;
  readonly labelField?: string;
  readonly valueField?: string;
  readonly openField?: string;
  readonly highField?: string;
  readonly lowField?: string;
  readonly closeField?: string;
  readonly sideField?: string;
  readonly priceField?: string;
  readonly sizeField?: string;
  readonly maxPoints?: number;
}

const stringPairSchema = z.object({
  key: z.string().trim().min(1).describe("String pair key."),
  value: z.string().trim().min(1).describe("String pair value."),
});

const futuresCandidateSchema = z.object({
  month: z.number().int().min(1).max(12).describe("Contract month number, 1-12."),
  ticker: z.string().trim().min(1).describe("Specific Bloomberg futures ticker."),
  year: z.number().int().min(1900).describe("Contract year."),
});

export function tickerSchema(options: NormalizedBloombergToolsOptions): ZodOutput<TickerInput> {
  const ticker = boundedString(options.maxStringChars).describe(
    "One generic futures-style Bloomberg ticker: <ROOT><N> ending in Index, Curncy, Comdty, or Corp, or <ROOT><N> <EXCHANGE> Equity. parse_ticker rejects other market sectors (Pfd, Govt, Muni, Mtge, M-Mkt) and non-futures securities.",
  );
  const tickers = boundedArray(
    boundedString(options.maxStringChars),
    options.maxSecurities,
  ).describe("Bloomberg tickers to normalize or filter.");
  return z.discriminatedUnion("operation", [
    z.object({ operation: z.literal("parse_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("is_specific_contract"), ticker }).strict(),
    z.object({ operation: z.literal("validate_generic_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("normalize_tickers"), tickers }).strict(),
    z.object({ operation: z.literal("filter_equity_tickers"), tickers }).strict(),
  ]);
}

export function futuresSchema(options: NormalizedBloombergToolsOptions): ZodOutput<FuturesInput> {
  const genTicker = boundedString(options.maxStringChars).describe(
    "Generic Bloomberg futures ticker, for example ES1 Index.",
  );
  const year = z.number().int().describe("Contract year, for example 2024.");
  const month = z.number().int().min(1).max(12).describe("Month number, 1-12.");
  const day = z.number().int().min(1).max(31).describe("Day number, 1-31.");
  return z.discriminatedUnion("operation", [
    z
      .object({
        asset: boundedString(options.maxStringChars).describe(
          "Bloomberg asset class suffix, for example Index or Comdty.",
        ),
        monthCode: boundedString(options.maxStringChars).describe(
          "Bloomberg futures month code, for example H.",
        ),
        operation: z.literal("build_futures_ticker"),
        prefix: boundedString(options.maxStringChars).describe(
          "Futures ticker root prefix, for example ES.",
        ),
        year: z
          .union([z.string().trim().min(1), z.number().int().transform(String)])
          .describe("Contract year, full or abbreviated, as a string or integer."),
      })
      .strict(),
    z
      .object({
        count: z
          .number()
          .int()
          .positive()
          .optional()
          .describe("Maximum number of futures candidates to generate."),
        day,
        freq: boundedString(options.maxStringChars)
          .optional()
          .describe("Futures frequency/cycle hint."),
        genTicker,
        month,
        operation: z.literal("generate_candidates"),
        year,
      })
      .strict(),
    z.object({ genTicker, operation: z.literal("contract_index") }).strict(),
    z
      .object({
        candidates: boundedArray(futuresCandidateSchema, options.maxFields).describe(
          "Candidate futures contracts.",
        ),
        cycle: boundedString(options.maxStringChars).describe(
          "Futures cycle code to filter candidates by.",
        ),
        operation: z.literal("filter_candidates_by_cycle"),
      })
      .strict(),
    z
      .object({
        contracts: boundedArray(stringPairSchema, options.maxFields).describe(
          "Contract pairs for validity filtering.",
        ),
        day,
        month,
        operation: z.literal("filter_valid_contracts"),
        year,
      })
      .strict(),
    z.object({ operation: z.literal("get_futures_months") }).strict(),
  ]);
}

export function cdxSchema(options: NormalizedBloombergToolsOptions): ZodOutput<CdxInput> {
  const ticker = boundedString(options.maxStringChars).describe("CDX ticker, generic or specific.");
  const recoveryRate = z
    .number()
    .min(0)
    .max(100)
    .optional()
    .describe("Recovery percentage override, e.g. 40 for 40%; sent as the CDS_RR override.");
  return z.discriminatedUnion("operation", [
    z.object({ operation: z.literal("parse_cdx_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("previous_cdx_series"), ticker }).strict(),
    z.object({ operation: z.literal("cdx_info"), ticker }).strict(),
    z.object({ operation: z.literal("cdx_pricing"), recoveryRate, ticker }).strict(),
    z.object({ operation: z.literal("cdx_risk"), recoveryRate, ticker }).strict(),
    z
      .object({
        genTicker: boundedString(options.maxStringChars).describe(
          "Generic CDX ticker, for example CDX IG CDSI GEN 5Y Corp.",
        ),
        operation: z.literal("cdx_gen_to_specific"),
        series: z.number().int().positive().describe("Specific CDX series number."),
      })
      .strict(),
  ]);
}

export function currencySchema(options: NormalizedBloombergToolsOptions): ZodOutput<CurrencyInput> {
  return z.discriminatedUnion("operation", [
    z
      .object({
        fromCcy: boundedString(options.maxStringChars).describe("Source ISO currency code."),
        operation: z.literal("build_fx_pair"),
        toCcy: boundedString(options.maxStringChars).describe("Destination ISO currency code."),
      })
      .strict(),
    z
      .object({
        ccy1: boundedString(options.maxStringChars).describe("First ISO currency code."),
        ccy2: boundedString(options.maxStringChars).describe("Second ISO currency code."),
        operation: z.literal("same_currency"),
      })
      .strict(),
    z
      .object({
        currencies: boundedArray(boundedString(options.maxStringChars), options.maxFields).describe(
          "ISO currency codes to check.",
        ),
        operation: z.literal("currencies_needing_conversion"),
        target: boundedString(options.maxStringChars).describe("Target ISO currency code."),
      })
      .strict(),
  ]);
}

export function bqlBuilderSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<BqlBuilderInput> {
  const extraFields = boundedArray(boundedString(options.maxStringChars), options.maxFields)
    .describe("Extra BQL fields to include.")
    .optional();
  return z.discriminatedUnion("operation", [
    z
      .object({
        equityTicker: boundedString(options.maxStringChars).describe(
          "Equity ticker for preferreds query.",
        ),
        extraFields,
        operation: z.literal("build_preferreds_query"),
      })
      .strict(),
    z
      .object({
        ccy: boundedString(options.maxStringChars)
          .optional()
          .describe("Currency filter for corporate bond query."),
        extraFields,
        operation: z.literal("build_corporate_bonds_query"),
        ticker: boundedString(options.maxStringChars).describe("Ticker for corporate bond query."),
      })
      .strict(),
    z
      .object({
        etfTicker: boundedString(options.maxStringChars).describe("ETF ticker for holdings query."),
        extraFields,
        operation: z.literal("build_etf_holdings_query"),
      })
      .strict(),
  ]);
}

export function marketSessionSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<MarketSessionInput> {
  const mic = boundedString(options.maxStringChars)
    .optional()
    .describe("Market Identifier Code, for example XNYS.");
  const exchCode = boundedString(options.maxStringChars)
    .optional()
    .describe("Bloomberg exchange code.");
  return z.discriminatedUnion("operation", [
    z
      .object({
        dayEnd: boundedString(options.maxStringChars).describe(
          "Exchange day end time, for example 16:00.",
        ),
        dayStart: boundedString(options.maxStringChars).describe(
          "Exchange day start time, for example 09:30.",
        ),
        exchCode,
        mic,
        operation: z.literal("derive_sessions"),
      })
      .strict(),
    z.object({ exchCode, mic, operation: z.literal("get_market_rule") }).strict(),
    z
      .object({
        countryIso: boundedString(options.maxStringChars).describe(
          "ISO country code for timezone inference.",
        ),
        operation: z.literal("infer_timezone"),
      })
      .strict(),
    z
      .object({
        date: boundedString(options.maxStringChars).describe(
          "Date for UTC session conversion, YYYY-MM-DD or YYYYMMDD.",
        ),
        endTime: boundedString(options.maxStringChars).describe(
          "Session end time, for example 16:00.",
        ),
        exchangeTz: boundedString(options.maxStringChars).describe(
          "IANA exchange timezone, for example America/New_York.",
        ),
        operation: z.literal("session_times_to_utc"),
        startTime: boundedString(options.maxStringChars).describe(
          "Session start time, for example 09:30.",
        ),
      })
      .strict(),
    z
      .object({
        endDate: boundedString(options.maxStringChars).optional().describe("Optional end date."),
        operation: z.literal("default_turnover_dates"),
        startDate: boundedString(options.maxStringChars)
          .optional()
          .describe("Optional start date."),
      })
      .strict(),
    z
      .object({
        endDatetime: boundedString(options.maxStringChars)
          .optional()
          .describe("Optional end datetime."),
        operation: z.literal("default_bqr_datetimes"),
        startDatetime: boundedString(options.maxStringChars)
          .optional()
          .describe("Optional start datetime."),
      })
      .strict(),
    z
      .object({
        operation: z.literal("get_exchange_override"),
        ticker: boundedString(options.maxStringChars).describe(
          "Ticker for exchange override lookup.",
        ),
      })
      .strict(),
    z.object({ operation: z.literal("list_exchange_overrides") }).strict(),
  ]);
}

export function yasOverridesSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<YasOverridesInput> {
  return z
    .object({
      benchmark: boundedString(options.maxStringChars)
        .optional()
        .describe("Optional YAS benchmark."),
      price: z.number().optional().describe("YAS price override."),
      settleDt: boundedString(options.maxStringChars).optional().describe("YAS settlement date."),
      spread: z.number().optional().describe("YAS spread override."),
      yieldType: z.number().int().optional().describe("YAS yield type override."),
      yieldVal: z.number().optional().describe("YAS yield value override."),
    })
    .strict();
}

export function constantsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ConstantsInput> {
  return z.discriminatedUnion("operation", [
    z
      .object({
        dateStr: boundedString(options.maxStringChars).describe("Date string to parse."),
        operation: z.literal("parse_date"),
      })
      .strict(),
    z
      .object({
        day: z.number().int().min(1).max(31).describe("Day number, 1-31."),
        fmt: boundedString(options.maxStringChars).optional().describe("Date output format."),
        month: z.number().int().min(1).max(12).describe("Month number, 1-12."),
        operation: z.literal("fmt_date"),
        year: z.number().int().min(1).describe("Year number."),
      })
      .strict(),
    z
      .object({
        monthName: boundedString(options.maxStringChars).describe("Month name, for example March."),
        operation: z.literal("get_month_code"),
      })
      .strict(),
    z
      .object({
        code: boundedString(options.maxStringChars).describe("Month code, for example H."),
        operation: z.literal("get_month_name"),
      })
      .strict(),
    z
      .object({
        dvdType: boundedString(options.maxStringChars).describe("Dividend type code or label."),
        operation: z.literal("get_dvd_type"),
      })
      .strict(),
    z.object({ operation: z.literal("get_futures_months") }).strict(),
    z.object({ operation: z.literal("get_dvd_types") }).strict(),
    z.object({ operation: z.literal("get_dvd_cols") }).strict(),
    z.object({ operation: z.literal("get_etf_cols") }).strict(),
  ]);
}

export function columnsSchema(options: NormalizedBloombergToolsOptions): ZodOutput<ColumnsInput> {
  const columns = boundedArray(boundedString(options.maxStringChars), options.maxFields).describe(
    "Column names to rename.",
  );
  return z.discriminatedUnion("operation", [
    z.object({ columns, operation: z.literal("rename_dividend_columns") }).strict(),
    z.object({ columns, operation: z.literal("rename_etf_columns") }).strict(),
    z
      .object({
        dataColumns: boundedArray(
          boundedString(options.maxStringChars),
          options.maxFields,
        ).describe("Earnings data column names."),
        headerRow: boundedArray(stringPairSchema, options.maxFields).describe(
          "Earnings header row key/value pairs.",
        ),
        operation: z.literal("build_earning_header_rename"),
      })
      .strict(),
  ]);
}

export function calculateSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<CalculateInput> {
  return z
    .object({
      levels: boundedArray(z.number().nullable(), options.maxFields).describe(
        "Hierarchy level values; supported levels are 1, 2, or null.",
      ),
      operation: z
        .literal("calculate_level_percentages")
        .describe("Numeric helper operation to run."),
      values: boundedArray(z.number().nullable(), options.maxFields).describe("Observed values."),
    })
    .strict()
    .superRefine((input, ctx) => {
      input.levels.forEach((level, index) => {
        if (level !== null && level !== 1 && level !== 2) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: "levels must contain only 1, 2, or null",
            path: ["levels", index],
          });
        }
      });
      if (input.values.length !== input.levels.length) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "values and levels must have the same length",
        });
      }
    });
}

export function chartSpecSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ChartSpecInput> {
  const chartScalar = z.union([
    boundedString(options.maxStringChars, { minimum: 0 }),
    z.number(),
    z.boolean(),
    z.null(),
  ]);
  const fieldName = boundedString(options.maxStringChars).describe("Input row field name.");
  return z
    .object({
      chart: z
        .enum(CHART_KINDS)
        .optional()
        .describe("Chart shape to generate. Defaults from source."),
      closeField: fieldName.optional().describe("Candlestick close-value field."),
      highField: fieldName.optional().describe("Candlestick high-value field."),
      labelField: fieldName.optional().describe("Categorical label field for bar charts."),
      lowField: fieldName.optional().describe("Candlestick low-value field."),
      maxPoints: z
        .number()
        .int()
        .positive()
        .max(options.maxRows)
        .optional()
        .describe("Maximum rows to include in the frontend spec; defaults to all provided rows."),
      openField: fieldName.optional().describe("Candlestick open-value field."),
      priceField: fieldName.optional().describe("Market-depth price field."),
      renderer: z
        .literal("vega-lite")
        .optional()
        .describe("Visualization spec renderer. Currently only vega-lite is generated."),
      rows: boundedArray(z.record(chartScalar), options.maxRows).describe(
        "Chart data rows copied from a bounded Bloomberg tool result.",
      ),
      seriesField: fieldName.optional().describe("Optional series/color field."),
      sideField: fieldName.optional().describe("Market-depth bid/ask side field."),
      sizeField: fieldName.optional().describe("Market-depth size field."),
      source: z.enum(CHART_SOURCES).describe("Bloomberg result shape that produced rows."),
      title: boundedString(options.maxStringChars).describe("Chart title.").optional(),
      valueField: fieldName.optional().describe("Primary numeric value field."),
      xField: fieldName.optional().describe("X-axis field."),
      yFields: boundedArray(boundedString(options.maxStringChars), options.maxFields)
        .describe("Numeric value fields to plot.")
        .optional(),
    })
    .strict();
}
