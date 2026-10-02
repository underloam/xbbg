import type { BloombergToolsOptions } from "./options";
const REQUIRED_TOOL_INSTRUCTIONS = [
  "# Bloomberg tool usage",
  "- Use these tools only for server-side Bloomberg data access through @xbbg/core. Never imply Bloomberg data was retrieved unless a tool call actually returned it.",
  "- Ask a clarifying question before calling a tool when any security identity, field mnemonic, date range, currency, periodicity, intraday interval, timezone, override, or universe is ambiguous.",
  "- Do not invent Bloomberg tickers, field mnemonics, overrides, or BQL functions. If the user gives a field description rather than a confident mnemonic, call xbbg_bflds first.",
  "- Issue one tool call per dataset and read any error before retrying; never probe parameter variants in parallel. Omit optional output-shape parameters such as format unless the user asked for a specific shape.",
  "",
  "## Security identifiers",
  "- Pass each security in the form the user supplied it; never translate between identifier kinds on your own.",
  "- User supplied a Bloomberg ticker: pass it through fully qualified as <TICKER> <MARKET_SECTOR>, for example <TICKER> <EXCHANGE> Equity, <INDEX_TICKER> Index, or <CCY_PAIR> Curncy.",
  "- The market sector ending (Bloomberg yellow key) is part of the security string. The sectors are: Equity, Index, Curncy, Comdty, Corp, Govt, Muni, Mtge, M-Mkt, and Pfd. Equity securities carry an exchange or composite code before the sector (<TICKER> <EXCHANGE> Equity); preferred securities use the Pfd sector; corporate and government bonds use Corp and Govt. Request tools pass the security through to Bloomberg without validating the sector, so copy it exactly as the user supplied it.",
  "- User supplied a raw ISIN or CUSIP: pass Bloomberg identifier syntax directly: /isin/<ISIN> or /cusip/<CUSIP>. xbbg_resolve_isins and xbbg_issuer_isins take raw ISIN strings; xbbg_resolve_venues and xbbg_auction_snapshot also accept valid bare ISINs, which the core normalizes. Otherwise never pass a bare identifier without its prefix.",
  "- <TICKER> <MARKET_SECTOR> is a format template, not authorization to construct a ticker. Never invent, recall from memory, or guess the Bloomberg ticker behind an identifier the user gave; identifier syntax is already a complete, valid security input. Use xbbg_resolve_isins only when the user wants the resolved Bloomberg security itself.",
  "- Recipe tools that take tickers (xbbg_preferreds, xbbg_corporate_bonds, xbbg_index_members, xbbg_etf_holdings) do not accept identifier syntax. When the user supplied an ISIN or CUSIP for those workflows, resolve it with xbbg_resolve_isins first and use the returned Bloomberg security; never guess the ticker.",
  "- Do not use xbbg_bsrch as a replacement for a known ticker, ISIN, or CUSIP.",
  "- For dealer quote / BQR workflows, use xbbg_bqr with a fixed-income identifier plus a dealer quote source such as /isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>. For raw intraday ticks, use xbbg_bdtick.",
  "",
  "## Core request tools",
  "- xbbg_bdp: current or reference point-in-time fields. Use a small explicit securities list and a small explicit fields list. Use includeSecurityErrors only when the caller wants Bloomberg security errors in the response.",
  "- xbbg_bdh: historical daily or periodic time series. Always provide explicit start and end dates in YYYY-MM-DD or YYYYMMDD form. Ask before choosing periodicity, currency, fill behavior, adjustment overrides, or a wide output table.",
  "- xbbg_bds: Bloomberg bulk/table fields. Provide exactly one bulk field; do not use bds for ordinary multi-field reference data.",
  "- xbbg_bdib: intraday bars only. Provide one ticker, explicit ISO start/end datetimes with time components, a positive interval in minutes, and timezone context when datetimes are naive.",
  "- xbbg_bdtick: intraday tick data. Provide one ticker, explicit ISO start/end datetimes with time components, and explicit eventTypes unless the default event stream is intended. Use includeBrokerCodes or includeConditionCodes only when those columns are needed.",
  "- xbbg_check_entitlements: checks whether the current Bloomberg identity is entitled to a nonempty list of EIDs returned by an EID-capable request. Usually use the default //blp/refdata service.",
  "- xbbg_bql: BQL expressions only when the user asks for BQL or the request is naturally expressed as a bounded BQL query. Keep queries short, explicit, and scoped to the requested universe.",
  "- xbbg_bsrch: Bloomberg search-grid or saved-search workflows only. Do not use it for ordinary security lookup.",
  "- xbbg_bqr: Bloomberg Quote Request / dealer quotes. Prefer fixed-income identifier inputs with a dealer quote source such as /isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>, explicit start/end datetimes with time components, and explicit event types.",
  "- xbbg_bflds: Bloomberg field metadata/search. Provide exactly one of fields or searchSpec; use searchSpec for natural-language field names and fields for known mnemonics.",
  "- xbbg_beqs: Bloomberg equity screening by named BEQS screen. Prefer this over hand-written BQL when the user names an existing Bloomberg screen.",
  "- xbbg_yas: fixed-income YAS recipe fields. Prefer this over manual YAS-style BDP requests when the user asks for yield, duration, spread, or price analytics.",
  "- xbbg_preferreds: preferred stock discovery from the issuer's common equity ticker, never a guessed preferred ('Pfd') ticker. Resolve a supplied ISIN/CUSIP with xbbg_resolve_isins first. Prefer this over xbbg_ext_bql_builder plus xbbg_bql when the user wants the actual preferreds result.",
  "- xbbg_corporate_bonds: bounded corporate bond universe query for a company ticker. Prefer this over generic BQL for company debt discovery.",
  "- xbbg_index_members: index constituents through the core index recipe. Prefer this over generic BDS/BQL members when the user asks for constituents.",
  "- xbbg_resolve_isins: resolves supplied ISIN strings to Bloomberg securities. Pass raw ISIN strings to this recipe, not /isin/<ISIN> syntax.",
  "- xbbg_resolve_venues: resolves composite equities and preferreds to validated primary venues, respecting explicit venue inputs. Optional pcsOverrides maps preferred exchange names to pricing sources for this call. Inspect each row's status/error; input order and duplicates are retained within output limits.",
  "- xbbg_auction_snapshot: finite reference-data auction/imbalance snapshot on validated venues, not a stream. Pass optional fields (omit or [] for AuctionFields.default) and per-call pcsOverrides. Input order, duplicates, and status/error are retained; unresolved, unsupported, or mismatched rows have null field values. Never substitute composite data for a failed venue.",
  "- xbbg_issuer_isins: issuer/bond ISIN workflow for supplied bond ISIN strings.",
  "- xbbg_etf_holdings: ETF holdings recipe for a single ETF ticker. Prefer this over generic BQL holdings when the user asks for ETF constituents.",
  "- xbbg_stream_snapshot: bounded live market-data observation from //blp/mktdata. Requires explicit maxUpdates and always terminates/unsubscribes.",
  "- xbbg_mktbar_snapshot: bounded live market-bar observation from //blp/mktbar for one ticker. Requires explicit maxUpdates and always terminates/unsubscribes.",
  "- xbbg_depth_snapshot: bounded market-depth observation from //blp/mktdepthdata for one ticker. Requires explicit maxUpdates and always terminates/unsubscribes.",
  "",
  "## BQL guidance",
  "- BQL is a complete Bloomberg Query Language expression sent as one query string; the tool does not assemble get/for/with clauses for you.",
  "- Basic shape: get(<FIELD_1>, <FIELD_2>) for(<UNIVERSE>). Use placeholders such as '<TICKER> <MARKET_SECTOR>', holdings('<ETF_TICKER> <MARKET_SECTOR>'), or members('<INDEX_TICKER> <MARKET_SECTOR>') until the user supplies real inputs.",
  "- Use BQL for universe-oriented analytics and screens only when the user provides a bounded universe, filters, and date range.",
  "- Prefer xbbg_ext_bql_builder instead of hand-writing BQL for supported workflows: preferred stocks, corporate bonds, and ETF holdings.",
  "- Do not use BQL just because the user asks for normal reference data; xbbg_bdp is simpler for current fields and xbbg_bdh is simpler for historical time series.",
  "",
  "## Output handling",
  "- Tool results use LangChain content_and_artifact output: content starts with a compact summary and then includes bounded model-readable JSON; artifact is the structured bounded envelope with tool, rowCount, truncated, and data for application code.",
  "- If a response is empty, truncated, or contains Bloomberg/security errors, say that directly. Do not fill gaps from memory or assumptions.",
] as const;

const OPTIONAL_EXTENSION_INSTRUCTIONS = [
  "",
  "## Extension helper tools",
  "- xbbg_ext_ticker: ticker hygiene before live calls. parse_ticker splits generic futures-style tickers only — asset endings Index, Curncy, Comdty, or Corp as <ROOT><N> <ASSET>, or <ROOT><N> <EXCHANGE> Equity — and rejects other market sectors (Pfd, Govt, Muni, Mtge, M-Mkt) and non-futures securities. normalize_tickers trims/canonicalizes lists, filter_equity_tickers keeps equity-like tickers, is_specific_contract checks futures specificity, and validate_generic_ticker rejects malformed generic futures tickers.",
  "- xbbg_ext_futures: futures contract construction and selection. Use build_futures_ticker for root/month/year/asset assembly, get_futures_months for month-code lookup, generate_candidates for generic-to-specific candidates, contract_index for generic contract rank, filter_candidates_by_cycle for HMUZ/quarterly cycles, and filter_valid_contracts to keep contracts valid for a date.",
  "- xbbg_ext_cdx: CDX ticker workflow support. Use parse_cdx_ticker to understand a CDX ticker, previous_cdx_series to roll back a series, cdx_gen_to_specific to resolve a generic CDX to a target series, and cdx_info/cdx_pricing/cdx_risk for predefined BDP field bundles. cdx_pricing and cdx_risk accept recoveryRate, which becomes the CDS_RR override.",
  "- xbbg_ext_currency: currency-planning helpers. build_fx_pair constructs the Bloomberg FX pair and conversion factor, same_currency avoids unnecessary conversion, and currencies_needing_conversion identifies which currencies differ from a target before requesting converted values.",
  "- xbbg_ext_bql_builder: safe BQL generators for common xbbg workflows. Use build_preferreds_query for preferred-stock discovery from an equity, build_corporate_bonds_query for company bond universes with optional currency/active filters, and build_etf_holdings_query for ETF constituents. Prefer these builders over hand-writing those BQL shapes.",
  "- xbbg_ext_chart_spec: renderer-neutral chart spec helper. Convert bounded rows from xbbg_bdh, xbbg_bdib, holdings, depth, or already-shaped row data into a Vega-Lite JSON spec for frontend rendering; do not use it as proof that Bloomberg data was fetched.",
  "- xbbg_ext_market_session: exchange calendar/timezone support. derive_sessions turns day session times into session blocks, infer_timezone maps country codes to timezones, session_times_to_utc converts local sessions to UTC, get_market_rule gets MIC/exchange rules, default_turnover_dates and default_bqr_datetimes provide bounded defaults, and get/list_exchange_override inspect configured exchange metadata.",
  "- xbbg_ext_yas_overrides: builds flat YAS override maps for fixed-income BDP requests when the lower-level BDP workflow is required. Prefer xbbg_yas for actual YAS recipe fields.",
  "- xbbg_ext_constants: static lookup/format helpers for date parsing/formatting, futures month code/name mappings, dividend type mappings, and known dividend/ETF output columns.",
  "- xbbg_ext_columns: post-processing helpers for Bloomberg-shaped tables. Use rename_dividend_columns, rename_etf_columns, or build_earning_header_rename when explaining or normalizing response column names after a request.",
  "- xbbg_ext_calculate: small numeric helper for Bloomberg workflows. calculate_level_percentages pairs observed values with levels; values and levels must have the same length.",
] as const;

const OPTIONAL_LIMIT_INSTRUCTIONS = [
  "",
  "## Request limits and inputs",
  "- Keep Bloomberg requests bounded: explicit securities, explicit fields, explicit dates, limited rows, and no broad exploratory pulls unless the user narrows the universe.",
  "- Respect configured tool limits for securities, fields, rows, string size, BQL length, and search spec length. Ask the user to narrow the request rather than exceeding them.",
  "- Use primitive kwargs only: string, number, or boolean values. For overrides on xbbg_bdp/xbbg_bdh/xbbg_bds, use primitive values for global overrides and nested primitive maps keyed by exact security for per-security overrides. Do not send other nested objects, arrays, or inferred defaults.",
] as const;

export const BLOOMBERG_TOOL_INSTRUCTIONS = [
  ...REQUIRED_TOOL_INSTRUCTIONS,
  ...OPTIONAL_EXTENSION_INSTRUCTIONS,
  ...OPTIONAL_LIMIT_INSTRUCTIONS,
].join("\n");

export interface BloombergToolInstructionsOptions {
  readonly includeExtensionGuidance?: boolean;
  readonly includeLimitReminder?: boolean;
}

export function getBloombergToolInstructions(
  options: BloombergToolInstructionsOptions = {},
): string {
  const includeExtensionGuidance = options.includeExtensionGuidance ?? true;
  const includeLimitReminder = options.includeLimitReminder ?? true;
  const lines: string[] = [...REQUIRED_TOOL_INSTRUCTIONS];
  if (includeExtensionGuidance) {
    lines.push(...OPTIONAL_EXTENSION_INSTRUCTIONS);
  }
  if (includeLimitReminder) {
    lines.push(...OPTIONAL_LIMIT_INSTRUCTIONS);
  }
  return lines.join("\n");
}

export const BDP_DESCRIPTION =
  'Bloomberg reference data for current or point-in-time fields. Use for a small bounded list of fully qualified securities. Use /isin/<ISIN> for ISINs and /cusip/<CUSIP> for CUSIPs. Example: securities ["<TICKER> <MARKET_SECTOR>"], fields ["<FIELD>"].';

export const BDH_DESCRIPTION =
  'Bloomberg historical time series. Requires explicit start and end dates; ask before using if the date range or periodicity is ambiguous. Use /isin/<ISIN> for ISINs and /cusip/<CUSIP> for CUSIPs. Example: securities ["<TICKER> <MARKET_SECTOR>"], fields ["<FIELD>"], start "<START_DATE>", end "<END_DATE>".';

export const BDS_DESCRIPTION =
  'Bloomberg bulk/table reference data. Requires exactly one bulk field, not a field list. Use /isin/<ISIN> for ISINs and /cusip/<CUSIP> for CUSIPs. Example: securities ["<INDEX_TICKER> <MARKET_SECTOR>"], field "<BULK_FIELD>".';

export const BDIB_DESCRIPTION =
  'Bloomberg intraday bars. Requires one ticker plus explicit ISO start/end datetimes with time components and a positive interval in minutes. Use /isin/<ISIN> for ISINs and /cusip/<CUSIP> for CUSIPs. Example: ticker "<TICKER> <MARKET_SECTOR>", start "<START_DATETIME>", end "<END_DATETIME>", interval <MINUTES>.';

export const BDTICK_DESCRIPTION =
  'Bloomberg intraday tick data. Requires one ticker plus explicit ISO start/end datetimes with time components. Set eventTypes explicitly, for example ["<EVENT_TYPE>"], and includeBrokerCodes/includeConditionCodes only when needed.';

export const CHECK_ENTITLEMENTS_DESCRIPTION =
  "Check the current Bloomberg identity against entitlement IDs returned by a request with returnEids enabled. Accepts a nonempty integer EID list and defaults the service to //blp/refdata. This tool is read-only.";

export const BQL_DESCRIPTION =
  "Bloomberg Query Language expression sent as one complete query string. Use for bounded universe analytics with placeholder-shaped syntax such as get(<FIELD>) for('<TICKER> <MARKET_SECTOR>'), holdings('<ETF_TICKER> <MARKET_SECTOR>'), members('<INDEX_TICKER> <MARKET_SECTOR>'), filters with with(...), or dates=range(...). Prefer xbbg_bdp/xbbg_bdh for simple reference or historical requests.";

export const BSRCH_DESCRIPTION =
  'Bloomberg search/grid request. Use for saved-search or ExcelGetGrid-style Bloomberg searches, not ordinary security lookup. Example searchSpec "<SEARCH_SPEC>".';

export const BQR_DESCRIPTION =
  'Bloomberg Quote Request / dealer quotes. Use for fixed-income dealer quote ticks, preferably with an ISIN plus dealer source such as "/isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>"; requires explicit ISO start/end datetimes with time components. Set eventTypes explicitly, for example ["<EVENT_TYPE>"].';

export const BFLDS_DESCRIPTION =
  'Bloomberg field metadata and field search. Use first when a field mnemonic is uncertain. Provide exactly one of fields or searchSpec. Example: fields ["<FIELD>"] or searchSpec "<FIELD_SEARCH_TEXT>".';

export const BEQS_DESCRIPTION =
  "Bloomberg equity screening by named BEQS screen. Use when the user names an existing Bloomberg screen and wants its bounded result set. Prefer this over hand-written BQL for saved Bloomberg screens.";

export const YAS_DESCRIPTION =
  "Bloomberg fixed-income YAS recipe fields for one or more bonds. Use for yield, duration, spread, benchmark, or price analytics; provide explicit fields and optional settlement/yield/price inputs. Pass securities as supplied: '<TICKER> <MARKET_SECTOR>' or identifier syntax such as '/isin/<ISIN> <MARKET_SECTOR>'.";

export const PREFERREDS_DESCRIPTION =
  "Preferred stock discovery for one issuer. Takes the issuer's common equity ticker such as '<TICKER> US Equity', never a preferred ('Pfd') ticker and never a guessed one. If the user supplied an ISIN or CUSIP, resolve it with xbbg_resolve_isins first.";

export const CORPORATE_BONDS_DESCRIPTION =
  "Corporate bond universe query for one issuer/company equity ticker, with optional currency, active-only filter, and result fields. Prefer this over generic BQL for company debt discovery. If the user supplied an ISIN or CUSIP, resolve it with xbbg_resolve_isins first; never guess the ticker.";

export const INDEX_MEMBERS_DESCRIPTION =
  "Index constituent recipe for one Bloomberg index ticker such as '<INDEX_TICKER> Index'. Use for bounded member lists and optional historical/as-of constituent membership; never guess index tickers.";

export const RESOLVE_ISINS_DESCRIPTION =
  "Resolve raw ISIN strings to Bloomberg securities through the core ISIN recipe. Do not add /isin/ prefixes in this tool; pass the exact ISIN strings supplied by the user.";

export const RESOLVE_VENUES_DESCRIPTION =
  "Resolve supplied securities to validated primary venues for auction/imbalance data, respecting explicit venue inputs. Accepts Bloomberg tickers, valid bare ISINs, /isin/<ISIN>, or /bbgid/<FIGI>; never guess venue tickers. Optional pcsOverrides maps preferred exchange names to pricing sources for this call. Returns input_order, security, routing metadata, status, and error; preserves input order and duplicates within output limits. Check resolved/unresolved/unsupported/mismatch status before using a venue.";

export const AUCTION_SNAPSHOT_DESCRIPTION =
  "Finite reference-data auction/imbalance snapshot on validated primary venues, not a subscription. Accepts supplied securities, optional fields (omit or [] for AuctionFields.default), and per-call pcsOverrides for preferred exchange pricing sources. Explicit venues are respected. Returns input_order, security, venue_topic, status, error, then requested field columns in order. Input order and duplicate securities are retained within output limits; unresolved, unsupported, or mismatched rows retain status/error and have null field values, never composite fallback data. Arrow Int64 and Time64 microsecond values become decimal strings in JSON to preserve precision; Date32 values are UTC-midnight epoch milliseconds. Time-only values have no invented date or timezone.";

export const ISSUER_ISINS_DESCRIPTION =
  "Issuer/bond ISIN workflow for supplied bond ISIN strings. Use for issuer-level ISIN discovery starting from known bond ISINs.";

export const ETF_HOLDINGS_DESCRIPTION =
  "ETF holdings recipe for one ETF ticker such as '<ETF_TICKER> <MARKET_SECTOR>'. Use when the user asks for ETF constituents or holdings and wants the bounded holdings result. Resolve a supplied ISIN/CUSIP with xbbg_resolve_isins first; never guess the ticker.";

export const STREAM_SNAPSHOT_DESCRIPTION =
  "Bounded live market-data snapshot from //blp/mktdata. Collects at most maxUpdates updates until timeout/done, then always unsubscribes; use for finite observations, not open subscriptions.";

export const MKTBAR_SNAPSHOT_DESCRIPTION =
  "Bounded live market-bar snapshot from //blp/mktbar for one ticker. Collects at most maxUpdates updates until timeout/done, then always unsubscribes.";

export const DEPTH_SNAPSHOT_DESCRIPTION =
  "Bounded live market-depth snapshot from //blp/mktdepthdata for one ticker. Collects at most maxUpdates updates until timeout/done, then always unsubscribes.";

export const EXT_TICKER_DESCRIPTION =
  "Ticker hygiene helpers: parse_ticker (generic futures-style tickers ending in Index, Curncy, Comdty, or Corp, or <ROOT><N> <EXCHANGE> Equity; other market sectors are rejected), normalize_tickers, filter_equity_tickers, is_specific_contract, and validate_generic_ticker.";

export const EXT_FUTURES_DESCRIPTION =
  "Futures helpers for contract construction and selection: build_futures_ticker, generate_candidates, contract_index, filter_candidates_by_cycle, filter_valid_contracts, and get_futures_months.";

export const EXT_CDX_DESCRIPTION =
  "CDX helpers for parsing, series rolling/resolution, and predefined info/pricing/risk BDP field bundles.";

export const EXT_CURRENCY_DESCRIPTION =
  "Currency planning helpers: build FX pairs, test same-currency requests, and find currencies needing conversion.";

export const EXT_BQL_BUILDER_DESCRIPTION =
  "BQL builders for preferred stocks, corporate bonds, and ETF holdings. Prefer to construct those bounded BQL shapes before xbbg_bql.";
export const EXT_CHART_SPEC_DESCRIPTION =
  "Renderer-neutral chart spec helper for frontend generative UI. Converts bounded Bloomberg rows from bdh, bdib, holdings, depth, or already-shaped row data into a Vega-Lite JSON spec; it does not fetch Bloomberg data or render images.";

export const EXT_MARKET_SESSION_DESCRIPTION =
  "Market session and timezone helpers for deriving sessions, UTC windows, market rules, exchange metadata, turnover defaults, and BQR datetime defaults.";

export const EXT_YAS_OVERRIDES_DESCRIPTION =
  "Build flat Bloomberg YAS override maps for fixed-income analytics fields.";

export const EXT_CONSTANTS_DESCRIPTION =
  "Static Bloomberg helper constants for date parsing/formatting, futures months, dividend types, and ETF/dividend columns.";

export const EXT_COLUMNS_DESCRIPTION =
  "Column rename helpers for dividend, ETF, and earnings-shaped Bloomberg responses.";

export const EXT_CALCULATE_DESCRIPTION =
  "Small numeric helper operations for Bloomberg workflows, including level percentage calculations.";

export function describeConfiguredLimits(options: BloombergToolsOptions): string {
  const parts: string[] = [];
  if (options.maxSecurities !== undefined) {
    parts.push(`maxSecurities=${options.maxSecurities}`);
  }
  if (options.maxFields !== undefined) {
    parts.push(`maxFields=${options.maxFields}`);
  }
  if (options.maxRows !== undefined) {
    parts.push(`maxRows=${options.maxRows}`);
  }
  return parts.length === 0 ? "default request limits" : parts.join(", ");
}
