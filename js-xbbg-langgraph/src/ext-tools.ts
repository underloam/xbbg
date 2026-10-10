import { createBloombergStructuredTool, type BloombergToolResult } from "./langchain-tool";
import { createChartSpec } from "./chart-spec";

import {
  CDX_INFO_FIELDS,
  CDX_PRICING_FIELDS,
  CDX_RISK_FIELDS,
  type BloombergToolName,
} from "./_defs_gen";
import type { PrimitiveMap } from "./bounded-schemas";
import { createCoreResolver, type CoreResolver } from "./core-loader";
import {
  EXT_BQL_BUILDER_DESCRIPTION,
  EXT_CALCULATE_DESCRIPTION,
  EXT_CDX_DESCRIPTION,
  EXT_CHART_SPEC_DESCRIPTION,
  EXT_COLUMNS_DESCRIPTION,
  EXT_CONSTANTS_DESCRIPTION,
  EXT_CURRENCY_DESCRIPTION,
  EXT_FUTURES_DESCRIPTION,
  EXT_MARKET_SESSION_DESCRIPTION,
  EXT_TICKER_DESCRIPTION,
  EXT_YAS_OVERRIDES_DESCRIPTION,
} from "./descriptions";
import type { BloombergToolsOptions } from "./options";
import { isToolDisabled } from "./options";
import type { BloombergTool } from "./tools";
import {
  bqlBuilderSchema,
  calculateSchema,
  cdxSchema,
  chartSpecSchema,
  columnsSchema,
  constantsSchema,
  currencySchema,
  futuresSchema,
  marketSessionSchema,
  tickerSchema,
  yasOverridesSchema,
  type BqlBuilderInput,
  type CalculateInput,
  type CdxInput,
  type ChartSpecInput,
  type ColumnsInput,
  type ConstantsInput,
  type CurrencyInput,
  type FuturesInput,
  type MarketSessionInput,
  type TickerInput,
  type YasOverridesInput,
} from "./ext-schemas";

function recoveryOverrides(recoveryRate: number | undefined): PrimitiveMap | undefined {
  return recoveryRate === undefined ? undefined : { CDS_RR: recoveryRate };
}

interface ExtToolDefinition {
  readonly create: (resolver: CoreResolver) => BloombergTool;
  readonly name: BloombergToolName;
}

const EXT_TOOL_DEFINITIONS: readonly ExtToolDefinition[] = Object.freeze([
  { create: extTickerWithResolver, name: "xbbg_ext_ticker" },
  { create: extFuturesWithResolver, name: "xbbg_ext_futures" },
  { create: extCdxWithResolver, name: "xbbg_ext_cdx" },
  { create: extCurrencyWithResolver, name: "xbbg_ext_currency" },
  { create: extBqlBuilderWithResolver, name: "xbbg_ext_bql_builder" },
  { create: extChartSpecWithResolver, name: "xbbg_ext_chart_spec" },
  { create: extMarketSessionWithResolver, name: "xbbg_ext_market_session" },
  { create: extYasOverridesWithResolver, name: "xbbg_ext_yas_overrides" },
  { create: extConstantsWithResolver, name: "xbbg_ext_constants" },
  { create: extColumnsWithResolver, name: "xbbg_ext_columns" },
  { create: extCalculateWithResolver, name: "xbbg_ext_calculate" },
]);

export const BLOOMBERG_EXT_TOOL_NAMES = Object.freeze(
  EXT_TOOL_DEFINITIONS.map((definition) => definition.name),
);

function extChartSpecWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_chart_spec" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    (input: ChartSpecInput): BloombergToolResult => ({ value: createChartSpec(input) }),
    {
      limits: resolver.options,
      description: EXT_CHART_SPEC_DESCRIPTION,
      name,
      schema: chartSpecSchema(resolver.options),
    },
  );
}

function extTickerWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_ticker" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: TickerInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "parse_ticker":
          return { value: core.ext.parseTicker(input.ticker) };
        case "normalize_tickers":
          return { value: core.ext.normalizeTickers(input.tickers) };
        case "filter_equity_tickers":
          return { value: core.ext.filterEquityTickers(input.tickers) };
        case "is_specific_contract":
          return { value: core.ext.isSpecificContract(input.ticker) };
        case "validate_generic_ticker":
          core.ext.validateGenericTicker(input.ticker);
          return { value: { ticker: input.ticker, valid: true } };
      }
    },
    {
      limits: resolver.options,
      description: EXT_TICKER_DESCRIPTION,
      name,
      schema: tickerSchema(resolver.options),
    },
  );
}

function extFuturesWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_futures" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: FuturesInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "build_futures_ticker":
          return {
            value: core.ext.buildFuturesTicker(
              input.prefix,
              input.monthCode,
              input.year,
              input.asset,
            ),
          };
        case "generate_candidates":
          return {
            value: core.ext.generateFuturesCandidates(
              input.genTicker,
              input.year,
              input.month,
              input.day,
              input.freq,
              input.count,
            ),
          };
        case "contract_index":
          return { value: core.ext.contractIndex(input.genTicker) };
        case "filter_candidates_by_cycle":
          return { value: core.ext.filterCandidatesByCycle(input.candidates, input.cycle) };
        case "filter_valid_contracts":
          return {
            value: core.ext.filterValidContracts(
              input.contracts,
              input.year,
              input.month,
              input.day,
            ),
          };
        case "get_futures_months":
          return { value: core.ext.getFuturesMonths() };
      }
    },
    {
      limits: resolver.options,
      description: EXT_FUTURES_DESCRIPTION,
      name,
      schema: futuresSchema(resolver.options),
    },
  );
}

function extCdxWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_cdx" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CdxInput): Promise<BloombergToolResult> => {
      if (
        input.operation === "cdx_info" ||
        input.operation === "cdx_pricing" ||
        input.operation === "cdx_risk"
      ) {
        const fields =
          input.operation === "cdx_info"
            ? CDX_INFO_FIELDS
            : input.operation === "cdx_pricing"
              ? CDX_PRICING_FIELDS
              : CDX_RISK_FIELDS;
        if (fields.length > resolver.options.maxFields) {
          throw new RangeError(
            `${input.operation} requires ${fields.length} fields, exceeding maxFields=${resolver.options.maxFields}`,
          );
        }
        const engine = await resolver.getEngine();
        const result = await engine.bdp([input.ticker], fields, {
          backend: "json",
          validateFields: resolver.options.validateFields,
          overrides: recoveryOverrides(
            input.operation === "cdx_pricing" || input.operation === "cdx_risk"
              ? input.recoveryRate
              : undefined,
          ),
        });
        return { value: result };
      }
      const core = await resolver.getCore();
      switch (input.operation) {
        case "parse_cdx_ticker":
          return { value: core.ext.parseCdxTicker(input.ticker) };
        case "previous_cdx_series":
          return { value: core.ext.previousCdxSeries(input.ticker) };
        case "cdx_gen_to_specific":
          return { value: core.ext.cdxGenToSpecific(input.genTicker, input.series) };
      }
    },
    {
      limits: resolver.options,
      description: EXT_CDX_DESCRIPTION,
      name,
      schema: cdxSchema(resolver.options),
    },
  );
}

function extCurrencyWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_currency" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CurrencyInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "build_fx_pair":
          return { value: core.ext.buildFxPair(input.fromCcy, input.toCcy) };
        case "same_currency":
          return { value: core.ext.sameCurrency(input.ccy1, input.ccy2) };
        case "currencies_needing_conversion":
          return { value: core.ext.currenciesNeedingConversion(input.currencies, input.target) };
      }
    },
    {
      limits: resolver.options,
      description: EXT_CURRENCY_DESCRIPTION,
      name,
      schema: currencySchema(resolver.options),
    },
  );
}

function extBqlBuilderWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_bql_builder" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BqlBuilderInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "build_preferreds_query":
          return { value: core.ext.buildPreferredsQuery(input.equityTicker, input.extraFields) };
        case "build_corporate_bonds_query":
          return {
            value: core.ext.buildCorporateBondsQuery(input.ticker, input.ccy, input.extraFields),
          };
        case "build_etf_holdings_query":
          return { value: core.ext.buildEtfHoldingsQuery(input.etfTicker, input.extraFields) };
      }
    },
    {
      limits: resolver.options,
      description: EXT_BQL_BUILDER_DESCRIPTION,
      name,
      schema: bqlBuilderSchema(resolver.options),
    },
  );
}

function extMarketSessionWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_market_session" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: MarketSessionInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "derive_sessions":
          return {
            value: core.ext.deriveSessions(input.dayStart, input.dayEnd, input.mic, input.exchCode),
          };
        case "get_market_rule":
          return { value: core.ext.getMarketRule(input.mic, input.exchCode) };
        case "infer_timezone":
          return { value: core.ext.inferTimezone(input.countryIso) };
        case "session_times_to_utc":
          return {
            value: core.ext.sessionTimesToUtc(
              input.startTime,
              input.endTime,
              input.exchangeTz,
              input.date,
            ),
          };
        case "default_turnover_dates":
          return { value: core.ext.defaultTurnoverDates(input.startDate, input.endDate) };
        case "default_bqr_datetimes":
          return { value: core.ext.defaultBqrDatetimes(input.startDatetime, input.endDatetime) };
        case "get_exchange_override":
          return { value: core.ext.getExchangeOverride(input.ticker) };
        case "list_exchange_overrides":
          return { value: core.ext.listExchangeOverrides() };
      }
    },
    {
      limits: resolver.options,
      description: EXT_MARKET_SESSION_DESCRIPTION,
      name,
      schema: marketSessionSchema(resolver.options),
    },
  );
}

function extYasOverridesWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_yas_overrides" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: YasOverridesInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      return {
        value: core.ext.buildYasOverrides(
          input.settleDt,
          input.yieldType,
          input.spread,
          input.yieldVal,
          input.price,
          input.benchmark,
        ),
      };
    },
    {
      limits: resolver.options,
      description: EXT_YAS_OVERRIDES_DESCRIPTION,
      name,
      schema: yasOverridesSchema(resolver.options),
    },
  );
}

function extConstantsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_constants" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ConstantsInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "parse_date":
          return { value: core.ext.parseDate(input.dateStr) };
        case "fmt_date":
          return { value: core.ext.fmtDate(input.year, input.month, input.day, input.fmt) };
        case "get_month_code":
          return { value: core.ext.getMonthCode(input.monthName) };
        case "get_month_name":
          return { value: core.ext.getMonthName(input.code) };
        case "get_futures_months":
          return { value: core.ext.getFuturesMonths() };
        case "get_dvd_type":
          return { value: core.ext.getDvdType(input.dvdType) };
        case "get_dvd_types":
          return { value: core.ext.getDvdTypes() };
        case "get_dvd_cols":
          return { value: core.ext.getDvdCols() };
        case "get_etf_cols":
          return { value: core.ext.getEtfCols() };
      }
    },
    {
      limits: resolver.options,
      description: EXT_CONSTANTS_DESCRIPTION,
      name,
      schema: constantsSchema(resolver.options),
    },
  );
}

function extColumnsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_columns" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ColumnsInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      switch (input.operation) {
        case "rename_dividend_columns":
          return { value: core.ext.renameDividendColumns(input.columns) };
        case "rename_etf_columns":
          return { value: core.ext.renameEtfColumns(input.columns) };
        case "build_earning_header_rename":
          return { value: core.ext.buildEarningHeaderRename(input.headerRow, input.dataColumns) };
      }
    },
    {
      limits: resolver.options,
      description: EXT_COLUMNS_DESCRIPTION,
      name,
      schema: columnsSchema(resolver.options),
    },
  );
}

function extCalculateWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_calculate" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CalculateInput): Promise<BloombergToolResult> => {
      const core = await resolver.getCore();
      return { value: core.ext.calculateLevelPercentages(input.values, input.levels) };
    },
    {
      limits: resolver.options,
      description: EXT_CALCULATE_DESCRIPTION,
      name,
      schema: calculateSchema(resolver.options),
    },
  );
}

export function createExtTickerTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extTickerWithResolver(createCoreResolver(options));
}

export function createExtFuturesTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extFuturesWithResolver(createCoreResolver(options));
}

export function createExtCdxTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCdxWithResolver(createCoreResolver(options));
}

export function createExtCurrencyTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCurrencyWithResolver(createCoreResolver(options));
}

export function createExtBqlBuilderTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extBqlBuilderWithResolver(createCoreResolver(options));
}

export function createExtMarketSessionTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extMarketSessionWithResolver(createCoreResolver(options));
}

export function createExtYasOverridesTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extYasOverridesWithResolver(createCoreResolver(options));
}

export function createExtConstantsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extConstantsWithResolver(createCoreResolver(options));
}

export function createExtColumnsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extColumnsWithResolver(createCoreResolver(options));
}

export function createExtCalculateTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCalculateWithResolver(createCoreResolver(options));
}

export function createExtChartSpecTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extChartSpecWithResolver(createCoreResolver(options));
}

export function createBloombergExtToolsForResolver(resolver: CoreResolver): BloombergTool[] {
  return EXT_TOOL_DEFINITIONS.filter(
    (definition) => !isToolDisabled(resolver.options, definition.name),
  ).map((definition) => definition.create(resolver));
}

export function createBloombergExtTools(options: BloombergToolsOptions = {}): BloombergTool[] {
  return createBloombergExtToolsForResolver(createCoreResolver(options));
}
