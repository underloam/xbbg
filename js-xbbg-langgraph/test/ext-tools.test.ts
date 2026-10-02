import type { StructuredToolInterface } from "@langchain/core/tools";
import { ToolMessage } from "@langchain/core/messages";

import { createBloombergExtTools, createExtChartSpecTool } from "../src";
import type { XbbgCoreLike, XbbgEngineLike } from "../src/core-loader";

type CoreSubscription = Awaited<ReturnType<XbbgEngineLike["stream"]>>;

function emptySubscription(): CoreSubscription {
  return {
    next: vi.fn(async () => ({ done: true, value: undefined }) as IteratorResult<unknown>),
    unsubscribe: vi.fn(async () => []),
  } as unknown as CoreSubscription;
}

function engine(): XbbgEngineLike {
  return {
    bdp: vi.fn(async () => [{ ticker: "CDX IG CDSI GEN 5Y Corp", value: 1 }]),
    bdh: vi.fn(async () => []),
    bdib: vi.fn(async () => []),
    bdtick: vi.fn(async () => []),
    bds: vi.fn(async () => []),
    checkEntitlements: vi.fn(async () => ({
      entitled: true,
      failedEids: [],
    })),
    bflds: vi.fn(async () => []),
    bql: vi.fn(async () => []),
    bsrch: vi.fn(async () => []),
    bqr: vi.fn(async () => []),
    beqs: vi.fn(async () => []),
    yas: vi.fn(async () => []),
    preferreds: vi.fn(async () => []),
    corporateBonds: vi.fn(async () => []),
    indexMembers: vi.fn(async () => []),
    resolveIsins: vi.fn(async () => []),
    resolveVenues: vi.fn(async () => []),
    auctionSnapshot: vi.fn(async () => []),
    issuerIsins: vi.fn(async () => []),
    etfHoldings: vi.fn(async () => []),
    stream: vi.fn(async () => emptySubscription()),
    mktbar: vi.fn(async () => emptySubscription()),
    depth: vi.fn(async () => emptySubscription()),
  };
}

function methodResult(method: string) {
  return (...args: unknown[]) => ({ args, method });
}

function core(fakeEngine: XbbgEngineLike): XbbgCoreLike {
  return {
    connect: vi.fn(async () => fakeEngine),
    ext: {
      buildCorporateBondsQuery: vi.fn(() => "corp"),
      buildEarningHeaderRename: vi.fn(methodResult("buildEarningHeaderRename")),
      buildEtfHoldingsQuery: vi.fn(() => "etf"),
      buildFuturesTicker: vi.fn(() => "ESH4 Index"),
      buildFxPair: vi.fn(methodResult("buildFxPair")),
      buildPreferredsQuery: vi.fn(() => "prefs"),
      buildYasOverrides: vi.fn(methodResult("buildYasOverrides")),
      calculateLevelPercentages: vi.fn(() => [0.1, null]),
      cdx: {
        acdx_info: vi.fn(async () => []),
        acdx_pricing: vi.fn(async () => []),
        acdx_risk: vi.fn(async () => []),
      },
      cdxGenToSpecific: vi.fn(() => "CDX IG S40 5Y Corp"),
      clearExchangeOverride: vi.fn(),
      contractIndex: vi.fn(() => 2),
      currenciesNeedingConversion: vi.fn(() => ["EUR"]),
      defaultBqrDatetimes: vi.fn(methodResult("defaultBqrDatetimes")),
      defaultTurnoverDates: vi.fn(methodResult("defaultTurnoverDates")),
      deriveSessions: vi.fn(methodResult("deriveSessions")),
      filterCandidatesByCycle: vi.fn((candidates) => candidates),
      filterEquityTickers: vi.fn((tickers) => tickers),
      filterValidContracts: vi.fn(() => ["ESH4 Index"]),
      fmtDate: vi.fn(() => "20240102"),
      generateFuturesCandidates: vi.fn(() => [{ month: 3, ticker: "ESH4 Index", year: 2024 }]),
      getDvdCols: vi.fn(() => [{ key: "DVD", value: "Dividend" }]),
      getDvdType: vi.fn(() => "Cash"),
      getDvdTypes: vi.fn(() => [{ key: "C", value: "Cash" }]),
      getEtfCols: vi.fn(() => [{ key: "ETF", value: "ETF" }]),
      getExchangeOverride: vi.fn(() => null),
      getFuturesMonths: vi.fn(() => [{ key: "H", value: "March" }]),
      getMarketRule: vi.fn(methodResult("getMarketRule")),
      getMonthCode: vi.fn(() => "H"),
      getMonthName: vi.fn(() => "March"),
      inferTimezone: vi.fn(() => "America/New_York"),
      isLongFormat: vi.fn(() => false),
      isSpecificContract: vi.fn(() => true),
      listExchangeOverrides: vi.fn(() => []),
      normalizeTickers: vi.fn((tickers) => tickers),
      parseCdxTicker: vi.fn(methodResult("parseCdxTicker")),
      parseDate: vi.fn(() => [2024, 1, 2]),
      parseTicker: vi.fn(methodResult("parseTicker")),
      pivotToWide: vi.fn((buffer: Buffer) => buffer),
      previousCdxSeries: vi.fn(() => "CDX IG S39 5Y Corp"),
      renameDividendColumns: vi.fn(methodResult("renameDividendColumns")),
      renameEtfColumns: vi.fn(methodResult("renameEtfColumns")),
      sameCurrency: vi.fn(() => false),
      sessionTimesToUtc: vi.fn(methodResult("sessionTimesToUtc")),
      setExchangeOverride: vi.fn(),
      validateGenericTicker: vi.fn(),
    },
  } as unknown as XbbgCoreLike;
}

function byName(tools: readonly StructuredToolInterface[], name: string): StructuredToolInterface {
  const found = tools.find((entry) => entry.name === name);
  if (found === undefined) {
    throw new Error(`Missing tool ${name}`);
  }
  return found;
}

async function invokeArtifact(tool: StructuredToolInterface, input: unknown) {
  const result = await tool.invoke(input, {
    toolCall: { args: {}, id: `call_${tool.name}`, name: tool.name },
  });
  if (!ToolMessage.isInstance(result)) {
    throw new TypeError(`Expected ToolMessage from ${tool.name}`);
  }
  if (typeof result.content !== "string") {
    throw new TypeError(`Expected string content from ${tool.name}`);
  }
  return [result.content, result.artifact] as const;
}

async function invokeJson(tool: StructuredToolInterface, input: unknown) {
  const [, artifact] = await invokeArtifact(tool, input);
  return artifact as Record<string, any>;
}

describe("Bloomberg extension tools", () => {
  it("invokes every exposed extension operation", async () => {
    const fakeEngine = engine();
    const fakeCore = core(fakeEngine);
    const tools = createBloombergExtTools({ core: fakeCore });

    const ticker = byName(tools, "xbbg_ext_ticker");
    await invokeJson(ticker, { operation: "parse_ticker", ticker: "AAPL US Equity" });
    await invokeJson(ticker, { operation: "normalize_tickers", tickers: ["aapl us equity"] });
    await invokeJson(ticker, { operation: "filter_equity_tickers", tickers: ["AAPL US Equity"] });
    await invokeJson(ticker, { operation: "is_specific_contract", ticker: "ESH4 Index" });
    await invokeJson(ticker, { operation: "validate_generic_ticker", ticker: "ES1 Index" });
    expect(fakeCore.ext.parseTicker).toHaveBeenCalledWith("AAPL US Equity");
    expect(fakeCore.ext.validateGenericTicker).toHaveBeenCalledWith("ES1 Index");

    const futures = byName(tools, "xbbg_ext_futures");
    await invokeJson(futures, {
      asset: "Index",
      monthCode: "H",
      operation: "build_futures_ticker",
      prefix: "ES",
      year: "4",
    });
    await invokeJson(futures, {
      day: 2,
      genTicker: "ES1 Index",
      month: 1,
      operation: "generate_candidates",
      year: 2024,
    });
    await invokeJson(futures, { genTicker: "ES1 Index", operation: "contract_index" });
    await invokeJson(futures, {
      candidates: [{ month: 3, ticker: "ESH4 Index", year: 2024 }],
      cycle: "HMUZ",
      operation: "filter_candidates_by_cycle",
    });
    await invokeJson(futures, {
      contracts: [{ key: "ESH4 Index", value: "202403" }],
      day: 1,
      month: 3,
      operation: "filter_valid_contracts",
      year: 2024,
    });
    await invokeJson(futures, { operation: "get_futures_months" });
    expect(fakeCore.ext.buildFuturesTicker).toHaveBeenCalledWith("ES", "H", "4", "Index");
    expect(fakeCore.ext.getFuturesMonths).toHaveBeenCalled();

    const cdx = byName(tools, "xbbg_ext_cdx");
    await invokeJson(cdx, { operation: "parse_cdx_ticker", ticker: "CDX IG CDSI GEN 5Y Corp" });
    await invokeJson(cdx, { operation: "previous_cdx_series", ticker: "CDX IG S40 5Y Corp" });
    await invokeJson(cdx, {
      genTicker: "CDX IG CDSI GEN 5Y Corp",
      operation: "cdx_gen_to_specific",
      series: 40,
    });
    await invokeJson(cdx, { operation: "cdx_info", ticker: "CDX IG CDSI GEN 5Y Corp" });
    await invokeJson(cdx, {
      operation: "cdx_pricing",
      recoveryRate: 0.4,
      ticker: "CDX IG CDSI GEN 5Y Corp",
    });
    await invokeJson(cdx, { operation: "cdx_risk", ticker: "CDX IG CDSI GEN 5Y Corp" });
    expect(fakeEngine.bdp).toHaveBeenCalledTimes(3);
    expect(fakeEngine.bdp).toHaveBeenCalledWith(
      ["CDX IG CDSI GEN 5Y Corp"],
      expect.arrayContaining(["PX_LAST"]),
      expect.objectContaining({ backend: "json" }),
    );

    const currency = byName(tools, "xbbg_ext_currency");
    await invokeJson(currency, { fromCcy: "USD", operation: "build_fx_pair", toCcy: "EUR" });
    await invokeJson(currency, { ccy1: "USD", ccy2: "EUR", operation: "same_currency" });
    await invokeJson(currency, {
      currencies: ["USD", "EUR"],
      operation: "currencies_needing_conversion",
      target: "USD",
    });
    expect(fakeCore.ext.currenciesNeedingConversion).toHaveBeenCalledWith(["USD", "EUR"], "USD");

    const bql = byName(tools, "xbbg_ext_bql_builder");

    const chartSpec = await invokeJson(byName(tools, "xbbg_ext_chart_spec"), {
      rows: [
        { date: "20240102", ticker: "AAPL US Equity", value: 190.1 },
        { date: "20240103", ticker: "AAPL US Equity", value: 191.2 },
      ],
      seriesField: "ticker",
      source: "bdh",
      title: "AAPL PX_LAST",
      xField: "date",
      yFields: ["value"],
    });
    expect(chartSpec).toMatchObject({
      data: {
        component: "xbbg_chart",
        kind: "xbbg.visualization",
        renderer: "vega-lite",
        spec: {
          data: {
            values: [
              { date: "2024-01-02", ticker: "AAPL US Equity", value: 190.1 },
              { date: "2024-01-03", ticker: "AAPL US Equity", value: 191.2 },
            ],
          },
          title: "AAPL PX_LAST",
        },
        summary: {
          chart: "line",
          seriesField: "ticker",
          xField: "date",
          yFields: ["value"],
        },
      },
      rowCount: 2,
      tool: "xbbg_ext_chart_spec",
    });
    await invokeJson(bql, { equityTicker: "AAPL US Equity", operation: "build_preferreds_query" });
    await invokeJson(bql, {
      activeOnly: true,
      ccy: "USD",
      operation: "build_corporate_bonds_query",
      ticker: "AAPL US Equity",
    });
    await invokeJson(bql, { etfTicker: "SPY US Equity", operation: "build_etf_holdings_query" });
    expect(fakeCore.ext.buildCorporateBondsQuery).toHaveBeenCalledWith(
      "AAPL US Equity",
      "USD",
      undefined,
      true,
    );

    const session = byName(tools, "xbbg_ext_market_session");
    await invokeJson(session, {
      dayEnd: "16:00",
      dayStart: "09:30",
      mic: "XNYS",
      operation: "derive_sessions",
    });
    await invokeJson(session, { mic: "XNYS", operation: "get_market_rule" });
    await invokeJson(session, { countryIso: "US", operation: "infer_timezone" });
    await invokeJson(session, {
      date: "2024-01-02",
      endTime: "16:00",
      exchangeTz: "America/New_York",
      operation: "session_times_to_utc",
      startTime: "09:30",
    });
    await invokeJson(session, { operation: "default_turnover_dates" });
    await invokeJson(session, { operation: "default_bqr_datetimes" });
    await invokeJson(session, { operation: "get_exchange_override", ticker: "AAPL US Equity" });
    await invokeJson(session, { operation: "list_exchange_overrides" });
    expect(fakeCore.ext.setExchangeOverride).not.toHaveBeenCalled();
    expect(fakeCore.ext.clearExchangeOverride).not.toHaveBeenCalled();

    await invokeJson(byName(tools, "xbbg_ext_yas_overrides"), { price: 99, settleDt: "20240102" });

    const constants = byName(tools, "xbbg_ext_constants");
    await invokeJson(constants, { dateStr: "2024-01-02", operation: "parse_date" });
    await invokeJson(constants, { day: 2, month: 1, operation: "fmt_date", year: 2024 });
    await invokeJson(constants, { monthName: "March", operation: "get_month_code" });
    await invokeJson(constants, { code: "H", operation: "get_month_name" });
    await invokeJson(constants, { operation: "get_futures_months" });
    await invokeJson(constants, { dvdType: "C", operation: "get_dvd_type" });
    await invokeJson(constants, { operation: "get_dvd_types" });
    await invokeJson(constants, { operation: "get_dvd_cols" });
    await invokeJson(constants, { operation: "get_etf_cols" });

    const columns = byName(tools, "xbbg_ext_columns");
    await invokeJson(columns, { columns: ["DVD_TYP"], operation: "rename_dividend_columns" });
    await invokeJson(columns, { columns: ["Ticker"], operation: "rename_etf_columns" });
    await invokeJson(columns, {
      dataColumns: ["A"],
      headerRow: [{ key: "A", value: "Name" }],
      operation: "build_earning_header_rename",
    });

    const calculate = await invokeJson(byName(tools, "xbbg_ext_calculate"), {
      levels: [1, null],
      operation: "calculate_level_percentages",
      values: [11, null],
    });
    expect(calculate).toMatchObject({ data: [0.1, null], tool: "xbbg_ext_calculate" });
  });

  it("requires ticker inputs by ticker operation branch", async () => {
    const fakeCore = core(engine());
    const tools = createBloombergExtTools({ core: fakeCore });
    const ticker = byName(tools, "xbbg_ext_ticker");

    await expect(ticker.invoke({ operation: "parse_ticker" })).rejects.toThrow();
    await expect(
      ticker.invoke({ operation: "parse_ticker", tickers: ["ZZZ1 Test"] }),
    ).rejects.toThrow();
    await expect(ticker.invoke({ operation: "normalize_tickers" })).rejects.toThrow();
    await expect(
      ticker.invoke({ operation: "normalize_tickers", ticker: "ZZZ1 Test" }),
    ).rejects.toThrow();

    await invokeJson(ticker, { operation: "parse_ticker", ticker: "ZZZ1 Test" });
    await invokeJson(ticker, { operation: "normalize_tickers", tickers: ["ZZZ1 Test"] });

    expect(fakeCore.ext.parseTicker).toHaveBeenCalledWith("ZZZ1 Test");
    expect(fakeCore.ext.normalizeTickers).toHaveBeenCalledWith(["ZZZ1 Test"]);
  });

  it("rejects stray keys and missing required fields per operation branch", async () => {
    const tools = createBloombergExtTools({ core: core(engine()) });

    const constants = byName(tools, "xbbg_ext_constants");
    await expect(
      constants.invoke({ bogus: 1, dateStr: "2024-01-02", operation: "parse_date" }),
    ).rejects.toThrow();

    const cdx = byName(tools, "xbbg_ext_cdx");
    await expect(
      cdx.invoke({ genTicker: "CDX IG CDSI GEN 5Y Corp", operation: "cdx_gen_to_specific" }),
    ).rejects.toThrow();
    await expect(
      cdx.invoke({
        operation: "cdx_pricing",
        recoveryRate: 1.5,
        ticker: "CDX IG CDSI GEN 5Y Corp",
      }),
    ).rejects.toThrow();

    const chart = byName(tools, "xbbg_ext_chart_spec");
    await expect(
      chart.invoke({ bogus: true, rows: [{ date: "20240102", value: 1 }], source: "bdh" }),
    ).rejects.toThrow();
    await expect(
      chart.invoke({ rows: [{ date: "20240102", value: "not numeric" }], source: "bdh" }),
    ).rejects.toThrow(/finite numeric/);
  });

  it("creates chart specs without loading Bloomberg core", async () => {
    const chart = createExtChartSpecTool({ maxRows: 3 });

    const output = await invokeJson(chart, {
      chart: "bar",
      labelField: "ticker",
      rows: [
        { ticker: "AAPL US Equity", weight: 0.07 },
        { ticker: "MSFT US Equity", weight: 0.06 },
      ],
      source: "holdings",
      valueField: "weight",
    });

    expect(output).toMatchObject({
      data: {
        chart: "bar",
        component: "xbbg_chart",
        rowCount: 2,
        spec: {
          mark: { type: "bar" },
          title: "holdings bar",
        },
      },
      rowCount: 2,
    });
  });

  it("coerces an integer year to a string for build_futures_ticker", async () => {
    const fakeCore = core(engine());
    const tools = createBloombergExtTools({ core: fakeCore });

    await invokeJson(byName(tools, "xbbg_ext_futures"), {
      asset: "Index",
      monthCode: "H",
      operation: "build_futures_ticker",
      prefix: "ES",
      year: 2024,
    });

    expect(fakeCore.ext.buildFuturesTicker).toHaveBeenCalledWith("ES", "H", "2024", "Index");
  });

  it("rejects mismatched values and levels lengths", async () => {
    const tools = createBloombergExtTools({ core: core(engine()) });

    await expect(
      byName(tools, "xbbg_ext_calculate").invoke({
        levels: [1],
        operation: "calculate_level_percentages",
        values: [11, 12],
      }),
    ).rejects.toThrow(/same length/);
  });

  it("rejects unknown calculate operations before invoking the numeric helper", async () => {
    const fakeCore = core(engine());
    const tools = createBloombergExtTools({ core: fakeCore });

    await expect(
      byName(tools, "xbbg_ext_calculate").invoke({
        levels: [1],
        operation: "unknown_operation",
        values: [11],
      }),
    ).rejects.toThrow();
    expect(fakeCore.ext.calculateLevelPercentages).not.toHaveBeenCalled();
  });

  it("rejects unsupported calculate hierarchy levels", async () => {
    const fakeCore = core(engine());
    const tools = createBloombergExtTools({ core: fakeCore });

    await expect(
      byName(tools, "xbbg_ext_calculate").invoke({
        levels: [100, 102],
        operation: "calculate_level_percentages",
        values: [11, 12],
      }),
    ).rejects.toThrow();
    expect(fakeCore.ext.calculateLevelPercentages).not.toHaveBeenCalled();
  });

  it("honors disabledTools and rejects mutating operation names", async () => {
    const tools = createBloombergExtTools({
      core: core(engine()),
      disabledTools: ["xbbg_ext_cdx", "xbbg_ext_bql_builder", "xbbg_ext_chart_spec"],
    });

    expect(tools.map((entry) => entry.name)).not.toContain("xbbg_ext_cdx");
    expect(tools.map((entry) => entry.name)).not.toContain("xbbg_ext_chart_spec");
    await expect(
      byName(tools, "xbbg_ext_market_session").invoke({
        operation: "set_exchange_override",
        ticker: "AAPL US Equity",
      }),
    ).rejects.toThrow();
  });

  it("applies configured input limits to extension schemas", async () => {
    const tools = createBloombergExtTools({
      core: core(engine()),
      maxFields: 1,
      maxRows: 1,
      maxSecurities: 1,
      maxStringChars: 4,
    });
    const ticker = byName(tools, "xbbg_ext_ticker");
    await expect(
      ticker.invoke({ operation: "normalize_tickers", tickers: ["A", "B"] }),
    ).rejects.toThrow();
    await expect(ticker.invoke({ operation: "parse_ticker", ticker: "ABCDE" })).rejects.toThrow();

    const columns = byName(tools, "xbbg_ext_columns");
    await expect(
      columns.invoke({ columns: ["A", "B"], operation: "rename_dividend_columns" }),
    ).rejects.toThrow();

    const chart = byName(tools, "xbbg_ext_chart_spec");
    await expect(
      chart.invoke({
        rows: [
          { v: 1, x: "A" },
          { v: 2, x: "B" },
        ],
        source: "rows",
        xField: "x",
        yFields: ["v"],
      }),
    ).rejects.toThrow();
    await expect(
      chart.invoke({
        rows: [{ a: 1, b: 2, x: "A" }],
        source: "rows",
        xField: "x",
        yFields: ["a", "b"],
      }),
    ).rejects.toThrow();
  });
});
