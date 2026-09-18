"""LangChain tools backed by xbbg's native helpers and Python extension recipes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from . import _ext_schemas as schemas
from ._bql import build_query
from ._runtime import make_tool
from .chart_spec import create_chart_spec
from .options import BloombergToolsOptions, resolve_options

__all__ = [
    "BLOOMBERG_EXT_TOOL_NAMES",
    "create_bloomberg_ext_tools",
    "create_ext_ticker_tool",
    "create_ext_futures_tool",
    "create_ext_cdx_tool",
    "create_ext_currency_tool",
    "create_ext_bql_builder_tool",
    "create_ext_chart_spec_tool",
    "create_ext_market_session_tool",
    "create_ext_yas_overrides_tool",
    "create_ext_constants_tool",
    "create_ext_columns_tool",
    "create_ext_calculate_tool",
]

# Python exposes native rename lookups, but not the native get_*_cols enumerators.
# Keep this Bloomberg input vocabulary aligned with xbbg-ext/src/constants.rs;
# the Rust helpers remain the sole source of the corresponding renamed values.
_DIVIDEND_COLUMNS = (
    "Declared Date",
    "Ex-Date",
    "Record Date",
    "Payable Date",
    "Dividend Amount",
    "Dividend Frequency",
    "Dividend Type",
    "Amount Status",
    "Adjustment Date",
    "Adjustment Factor",
    "Adjustment Factor Operator Type",
    "Adjustment Factor Flag",
    "Amount Per Share",
    "Projected/Confirmed",
)
_ETF_COLUMNS = (
    "Holding Name",
    "Holding Ticker",
    "Holding ISIN",
    "Holding Sedol",
    "Holding CUSIP",
    "Shares Held",
    "Market Value",
    "Weight",
    "% Weight",
    "Sector",
    "Country",
    "Asset Class",
    "Currency",
    "Coupon",
    "Maturity",
)
_SESSION_NAMES = ("day", "allday", "pre", "post", "am", "pm")
_NO_GUESS = (
    " Use only securities supplied by the user or returned by Bloomberg; never invent or guess tickers. "
    "For data requests use /isin/<ISIN> or /cusip/<CUSIP> directly. Ticker-only recipes and BQL builders "
    "require resolved Bloomberg securities; use identifier-resolution tools first instead of guessing."
)


def _arguments(input: BaseModel) -> tuple[str, dict[str, Any]]:
    arguments = input.model_dump(exclude_unset=True)
    operation = arguments.pop("operation")
    return operation, arguments


def _pairs(values: Iterable[tuple[str, str]]) -> list[dict[str, str]]:
    return [{"key": key, "value": value} for key, value in values]


def _candidates(values: Iterable[tuple[str, int, int]]) -> list[dict[str, Any]]:
    return [{"ticker": ticker, "year": year, "month": month} for ticker, year, month in values]


def _time_range(value: tuple[str, str] | None) -> dict[str, str] | None:
    if value is None:
        return None
    start, end = value
    return {"start": start, "end": end}


def _exchange_info(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": value["ticker"],
        "mic": value["mic"],
        "exchCode": value["exch_code"],
        "timezone": value["timezone"],
        "utcOffset": value["utc_offset"],
        "source": value["source"],
        **{name: _time_range(value[name]) for name in _SESSION_NAMES},
    }


async def _ticker(input: BaseModel, options: BloombergToolsOptions) -> Any:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "parse_ticker":
        prefix, index, asset, exchange = _core.ext_parse_ticker(**arguments)
        return {"prefix": prefix, "index": index, "asset": asset, "exchange": exchange}
    if operation == "normalize_tickers":
        return _core.ext_normalize_tickers(**arguments)
    if operation == "filter_equity_tickers":
        return _core.ext_filter_equity_tickers(**arguments)
    if operation == "is_specific_contract":
        return _core.ext_is_specific_contract(**arguments)
    if operation == "validate_generic_ticker":
        _core.ext_validate_generic_ticker(**arguments)
        return {"ticker": arguments["ticker"], "valid": True}
    raise ValueError(f"Unsupported ticker operation: {operation}")


async def _futures(input: BaseModel, options: BloombergToolsOptions) -> Any:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "build_futures_ticker":
        arguments["year"] = str(arguments["year"])
        return _core.ext_build_futures_ticker(**arguments)
    if operation == "generate_candidates":
        return _candidates(_core.ext_generate_futures_candidates(**arguments))
    if operation == "contract_index":
        return _core.ext_contract_index(**arguments)
    if operation == "filter_candidates_by_cycle":
        candidates = [(item["ticker"], item["year"], item["month"]) for item in arguments["candidates"]]
        return _candidates(_core.ext_filter_candidates_by_cycle(candidates, arguments["cycle"]))
    if operation == "filter_valid_contracts":
        arguments["contracts"] = [(pair["key"], pair["value"]) for pair in arguments["contracts"]]
        return _core.ext_filter_valid_contracts(**arguments)
    if operation == "get_futures_months":
        return _pairs(_core.ext_get_futures_months().items())
    raise ValueError(f"Unsupported futures operation: {operation}")


async def _cdx(input: BaseModel, options: BloombergToolsOptions) -> Any:
    operation, arguments = _arguments(input)
    if operation in ("cdx_info", "cdx_pricing", "cdx_risk"):
        from xbbg.ext import cdx

        recipes = {
            "cdx_info": (cdx.acdx_info, cdx._CDX_INFO_FIELDS),
            "cdx_pricing": (cdx.acdx_pricing, cdx._CDX_PRICING_FIELDS),
            "cdx_risk": (cdx.acdx_risk, cdx._CDX_RISK_FIELDS),
        }
        recipe, fields = recipes[operation]
        if len(fields) > options.max_fields:
            raise ValueError(f"{operation} requires {len(fields)} fields, exceeding max_fields={options.max_fields}")
        return await recipe(**arguments, backend="native", validate_fields=options.validate_fields)

    from xbbg import _core

    if operation == "parse_cdx_ticker":
        index, series, tenor, asset, is_generic, series_num = _core.ext_parse_cdx_ticker(**arguments)
        return {
            "index": index,
            "series": series,
            "version": _core.ext_parse_cdx_version(**arguments),
            "tenor": tenor,
            "asset": asset,
            "isGeneric": is_generic,
            "seriesNum": series_num,
        }
    if operation == "previous_cdx_series":
        return _core.ext_previous_cdx_series(**arguments)
    if operation == "cdx_gen_to_specific":
        return _core.ext_cdx_gen_to_specific(**arguments)
    raise ValueError(f"Unsupported CDX operation: {operation}")


async def _currency(input: BaseModel, options: BloombergToolsOptions) -> Any:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "build_fx_pair":
        pair, factor, source, target = _core.ext_build_fx_pair(**arguments)
        return {"fxPair": pair, "factor": factor, "fromCcy": source, "toCcy": target}
    if operation == "same_currency":
        return _core.ext_same_currency(**arguments)
    if operation == "currencies_needing_conversion":
        return _core.ext_currencies_needing_conversion(**arguments)
    raise ValueError(f"Unsupported currency operation: {operation}")


async def _bql_builder(input: BaseModel, options: BloombergToolsOptions) -> str:
    operation, arguments = _arguments(input)
    return build_query(operation, arguments, options.max_bql_query_chars)


async def _chart_spec(input: BaseModel, options: BloombergToolsOptions) -> dict[str, Any]:
    return create_chart_spec(input)


async def _market_session(input: BaseModel, options: BloombergToolsOptions) -> Any:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "derive_sessions":
        return {name: _time_range(value) for name, value in _core.ext_derive_sessions(**arguments).items()}
    if operation == "get_market_rule":
        rule = _core.ext_get_market_rule(**arguments)
        if rule is None:
            return None
        return {
            "preMinutes": rule["pre_minutes"],
            "postMinutes": rule["post_minutes"],
            "lunchStartMin": rule["lunch_start_min"],
            "lunchEndMin": rule["lunch_end_min"],
            "isContinuous": rule["is_continuous"],
        }
    if operation == "infer_timezone":
        return _core.ext_infer_timezone(**arguments)
    if operation == "session_times_to_utc":
        # The Python binding requires ISO input; native parsing also accepts the
        # compact Bloomberg dates supported by the JavaScript counterpart.
        year, month, day = _core.ext_parse_date(arguments["date"])
        arguments["date"] = _core.ext_fmt_date(year, month, day, "%Y-%m-%d")
        start, end = _core.ext_session_times_to_utc(**arguments)
        if end < start:
            # An overnight end belongs to the next local date, whose UTC offset
            # can differ at a daylight-saving boundary. Let Rust resolve it.
            arguments["date"] = (date(year, month, day) + timedelta(days=1)).isoformat()
            _, end = _core.ext_session_times_to_utc(**arguments)
        return {
            key: datetime.fromisoformat(value).replace(tzinfo=timezone.utc).isoformat()
            for key, value in (("start", start), ("end", end))
        }
    if operation == "default_turnover_dates":
        return _time_range(_core.ext_default_turnover_dates(**arguments))
    if operation == "default_bqr_datetimes":
        return _time_range(_core.ext_default_bqr_datetimes(**arguments))
    if operation == "get_exchange_override":
        info = _core.ext_get_exchange_override(**arguments)
        return None if info is None else _exchange_info(info)
    if operation == "list_exchange_overrides":
        return [_exchange_info(info) for info in _core.ext_list_exchange_overrides().values()]
    raise ValueError(f"Unsupported market-session operation: {operation}")


async def _yas_overrides(input: BaseModel, options: BloombergToolsOptions) -> list[dict[str, str]]:
    from xbbg import _core

    return _pairs(_core.ext_build_yas_overrides(**input.model_dump()))


async def _constants(input: BaseModel, options: BloombergToolsOptions) -> Any:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "parse_date":
        return list(_core.ext_parse_date(**arguments))
    if operation == "fmt_date":
        return _core.ext_fmt_date(**arguments)
    if operation == "get_month_code":
        return _core.ext_get_month_code(arguments["month_name"])
    if operation == "get_month_name":
        return _core.ext_get_month_name(**arguments)
    if operation == "get_futures_months":
        return _pairs(_core.ext_get_futures_months().items())
    if operation == "get_dvd_type":
        return _core.ext_get_dvd_type(arguments["dvd_type"])
    if operation == "get_dvd_types":
        return _pairs(_core.ext_get_dvd_types().items())
    if operation == "get_dvd_cols":
        return _pairs(_core.ext_rename_dividend_columns(_DIVIDEND_COLUMNS))
    if operation == "get_etf_cols":
        return _pairs(_core.ext_rename_etf_columns(_ETF_COLUMNS))
    raise ValueError(f"Unsupported constants operation: {operation}")


async def _columns(input: BaseModel, options: BloombergToolsOptions) -> list[dict[str, str]]:
    from xbbg import _core

    operation, arguments = _arguments(input)
    if operation == "rename_dividend_columns":
        return _pairs(_core.ext_rename_dividend_columns(**arguments))
    if operation == "rename_etf_columns":
        return _pairs(_core.ext_rename_etf_columns(**arguments))
    if operation == "build_earning_header_rename":
        arguments["header_row"] = [(pair["key"], pair["value"]) for pair in arguments["header_row"]]
        return _pairs(_core.ext_build_earning_header_rename(**arguments))
    raise ValueError(f"Unsupported columns operation: {operation}")


async def _calculate(input: BaseModel, options: BloombergToolsOptions) -> list[float | None]:
    from xbbg import _core

    _, arguments = _arguments(input)
    return _core.ext_calculate_level_percentages(**arguments)


Handler = Callable[[BaseModel, BloombergToolsOptions], Awaitable[Any]]
SchemaFactory = Callable[[BloombergToolsOptions], type[BaseModel]]
_DEFINITIONS: dict[str, tuple[str, SchemaFactory, Handler]] = {
    "xbbg_ext_ticker": (
        "Ticker hygiene: parse_ticker, normalize_tickers, filter_equity_tickers, is_specific_contract, "
        "validate_generic_ticker. parse_ticker accepts generic futures-style tickers ending in Index, "
        "Curncy, Comdty or Corp, or <ROOT><N> <EXCHANGE> Equity; it is not a general security resolver." + _NO_GUESS,
        schemas.ticker_schema,
        _ticker,
    ),
    "xbbg_ext_futures": (
        "Native futures contract construction/selection: build_futures_ticker, generate_candidates, "
        "contract_index, filter_candidates_by_cycle, filter_valid_contracts, get_futures_months. "
        "Candidate generation is bounded by max_rows; constructed candidates are not Bloomberg-validated securities."
        + _NO_GUESS,
        schemas.futures_schema,
        _futures,
    ),
    "xbbg_ext_cdx": (
        "CDX parsing and series helpers plus live cdx_info, cdx_pricing and cdx_risk bundles. "
        "recovery_rate uses Python xbbg percentage units: 0-100, so 40 means 40%. Pure parsing does not connect; "
        "the predefined data bundles require Bloomberg and must fit max_fields." + _NO_GUESS,
        schemas.cdx_schema,
        _cdx,
    ),
    "xbbg_ext_currency": (
        "Native currency planning: build_fx_pair, same_currency, currencies_needing_conversion. "
        "Returns conversion metadata, not live FX quotes.",
        schemas.currency_schema,
        _currency,
    ),
    "xbbg_ext_bql_builder": (
        "Build, but do not execute, BQL queries for preferred stocks, corporate bonds and ETF holdings. "
        "Use the issuer's common equity for preferreds, never a guessed preferred ticker. Corporate bonds "
        "returns the native debt-universe query; no active-only filtering guarantee is made. "
        "Prefer the corresponding core recipe tool when actual data is requested." + _NO_GUESS,
        schemas.bql_builder_schema,
        _bql_builder,
    ),
    "xbbg_ext_chart_spec": (
        "Build an inline, renderer-neutral Vega-Lite artifact from bounded Bloomberg rows. Supports line, "
        "area, bar, scatter, candlestick and depth charts from bdh, bdib, holdings, depth or rows. "
        "No Bloomberg fetch, remote data URL, renderer dependency or image generation. Field references "
        "must exist in the retained rows; use max_points to cap plotted rows. Render only when spec is present "
        "and data.renderable is not false; explicit max_points truncation retains a valid specification.",
        schemas.chart_spec_schema,
        _chart_spec,
    ),
    "xbbg_ext_market_session": (
        "Read-only native session and timezone helpers: derive_sessions, get_market_rule, infer_timezone, "
        "session_times_to_utc, default_turnover_dates, default_bqr_datetimes, get_exchange_override, "
        "list_exchange_overrides. Never writes or clears exchange overrides." + _NO_GUESS,
        schemas.market_session_schema,
        _market_session,
    ),
    "xbbg_ext_yas_overrides": (
        "Build native YAS override key/value pairs without requesting market data. Supports settle_dt, "
        "yield_type (1-9), spread, yield_val, price and benchmark." + _NO_GUESS,
        schemas.yas_overrides_schema,
        _yas_overrides,
    ),
    "xbbg_ext_constants": (
        "Native constant lookups: parse_date, fmt_date, get_month_code, get_month_name, get_futures_months, "
        "get_dvd_type, get_dvd_types, get_dvd_cols, get_etf_cols. Date parsing returns [year, month, day]; "
        "mapping lookups return key/value pairs.",
        schemas.constants_schema,
        _constants,
    ),
    "xbbg_ext_columns": (
        "Native rename mappings for rename_dividend_columns, rename_etf_columns and build_earning_header_rename. "
        "Returns key/value pairs without mutating source data.",
        schemas.columns_schema,
        _columns,
    ),
    "xbbg_ext_calculate": (
        "Native calculate_level_percentages for earnings hierarchies. values and levels must have equal "
        "length; levels may contain only integer 1, integer 2 or null. Results are percentages (0-100).",
        schemas.calculate_schema,
        _calculate,
    ),
}
BLOOMBERG_EXT_TOOL_NAMES = tuple(_DEFINITIONS)


def _create(name: str, options: BloombergToolsOptions | None, kwargs: dict[str, Any]) -> StructuredTool:
    resolved = resolve_options(options, kwargs)
    if name in resolved.disabled_tools:
        raise ValueError(f"Tool {name} is disabled")
    description, schema_factory, handler = _DEFINITIONS[name]

    async def invoke(input: BaseModel) -> Any:
        return await handler(input, resolved)

    return make_tool(name, description, schema_factory(resolved), invoke, resolved)


def create_ext_ticker_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_ticker", options, kwargs)


def create_ext_futures_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_futures", options, kwargs)


def create_ext_cdx_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_cdx", options, kwargs)


def create_ext_currency_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_currency", options, kwargs)


def create_ext_bql_builder_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_bql_builder", options, kwargs)


def create_ext_chart_spec_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_chart_spec", options, kwargs)


def create_ext_market_session_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_market_session", options, kwargs)


def create_ext_yas_overrides_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_yas_overrides", options, kwargs)


def create_ext_constants_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_constants", options, kwargs)


def create_ext_columns_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_columns", options, kwargs)


def create_ext_calculate_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _create("xbbg_ext_calculate", options, kwargs)


def create_bloomberg_ext_tools(options: BloombergToolsOptions | None = None, **kwargs: Any) -> list[StructuredTool]:
    """Create all eleven helpers, filtering explicitly disabled tool names."""
    resolved = resolve_options(options, kwargs)
    return [_create(name, resolved, {}) for name in BLOOMBERG_EXT_TOOL_NAMES if name not in resolved.disabled_tools]
