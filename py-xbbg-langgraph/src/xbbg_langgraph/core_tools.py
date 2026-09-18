"""LangChain tools backed by the existing asynchronous xbbg APIs."""

from __future__ import annotations

import asyncio
from typing import Any

from langchain_core.tools import StructuredTool

from ._bql import build_query
from ._runtime import ToolInput, make_tool
from ._schemas import create_core_schema
from .options import BloombergToolsOptions, resolve_options

_DESCRIPTIONS = {
    "xbbg_bdp": "Bloomberg reference data for current or point-in-time fields. Supply bounded securities and fields lists. Preserve exact tickers and /isin/ or /cusip/ identifiers; never guess securities. Use xbbg_bflds when a mnemonic is uncertain.",
    "xbbg_bdh": "Bloomberg historical time series. Requires explicit start and end calendar dates. Ask when the date range or periodicity is ambiguous. Preserve user-supplied security identifiers.",
    "xbbg_bds": "Bloomberg bulk/table reference data. Requires exactly one bulk field, not a field list. Preserve user-supplied securities and identifier syntax.",
    "xbbg_bdib": "Bloomberg intraday bars for one security. Requires explicit ISO start/end datetimes with time components and a positive interval in minutes. Ask when timezone or interval is ambiguous.",
    "xbbg_bdtick": "Bloomberg intraday ticks for one security. Requires explicit ISO start/end datetimes. Specify event_types when the default stream is not intended; request broker/condition codes only when needed.",
    "xbbg_check_entitlements": "Read-only check of the current Bloomberg identity against positive entitlement IDs returned by requests with return_eids enabled. The default service is //blp/refdata.",
    "xbbg_bql": "Execute one complete Bloomberg Query Language expression with an explicit bounded universe. Prefer xbbg_bdp/xbbg_bdh for simple reference/historical requests. Never invent fields or BQL functions.",
    "xbbg_bsrch": "Bloomberg search/grid request for an existing saved search or ExcelGetGrid-style search_spec. Not an ordinary security lookup tool.",
    "xbbg_bqr": "Bloomberg dealer quote ticks for fixed income. Preserve the supplied ticker or /isin/<ISIN>@<QUOTE_SOURCE> Corp identifier. Requires explicit ISO start/end datetimes; specify event_types when needed.",
    "xbbg_bflds": "Bloomberg field metadata and search. Use first when a field mnemonic is uncertain. Provide exactly one of fields or search_spec.",
    "xbbg_beqs": "Run an existing named Bloomberg equity screen. Prefer this to hand-written BQL when the user supplies a saved Bloomberg screen name.",
    "xbbg_yas": "Bloomberg fixed-income yield/spread analysis for supplied bonds and explicit fields, with optional settlement, yield, spread, price, and benchmark inputs. Preserve identifiers and never guess tickers.",
    "xbbg_preferreds": "Discover preferred stocks from the issuer's exact common Equity ticker, never a Pfd ticker. Resolve supplied identifiers with xbbg_resolve_isins first; never invent an equity ticker.",
    "xbbg_corporate_bonds": "Query the native corporate debt universe for an exact company Equity ticker, with optional currency and fields. There is no active-only filtering guarantee. Resolve identifiers first; never guess tickers.",
    "xbbg_index_members": "Native Bloomberg index constituent recipe with optional as-of date. Supply the exact qualified Index ticker, never a guessed ticker or unresolved identifier.",
    "xbbg_resolve_isins": "Resolve user-supplied raw ISIN strings through Bloomberg's identifier recipe. Do not add /isin/ prefixes; never infer an ISIN or ticker.",
    "xbbg_issuer_isins": "Resolve supplied raw bond ISIN strings to issuer equity ISINs through Bloomberg's native recipe.",
    "xbbg_etf_holdings": "Fetch bounded ETF holdings for the exact qualified Equity ticker. Resolve supplied identifiers first; never guess the ETF ticker.",
    "xbbg_stream_snapshot": "Bounded live //blp/mktdata snapshot. Requires max_updates; collects until that count, timeout, or stream completion, then always unsubscribes. Returns finite updates, not an open subscription.",
    "xbbg_mktbar_snapshot": "Bounded live //blp/mktbar snapshot for one security. Requires max_updates. Native market bars require LAST_PRICE and a bar_size subscription option (default 1 minute). Always unsubscribes.",
    "xbbg_depth_snapshot": "Bounded live //blp/mktdepthdata snapshot for one security. Requires max_updates and a Bloomberg B-PIPE environment with applicable entitlements. Always unsubscribes.",
}

BLOOMBERG_CORE_TOOL_NAMES: tuple[str, ...] = tuple(_DESCRIPTIONS)

_TICK_FLAGS = {
    "include_bic_mic_codes": "includeBicMicCodes",
    "include_bloomberg_standard_condition_codes": "includeBloombergStandardConditionCodes",
    "include_broker_codes": "includeBrokerCodes",
    "include_condition_codes": "includeConditionCodes",
    "include_exchange_codes": "includeExchangeCodes",
    "include_non_plottable_events": "includeNonPlottableEvents",
    "include_rps_codes": "includeRpsCodes",
}


async def _release_subscription(subscription: Any, pending: asyncio.Task[Any] | None, drain: bool) -> None:
    try:
        await subscription.unsubscribe(drain=drain)
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def _collect_snapshot(
    subscription: Any, *, max_updates: int, timeout_ms: int, drain: bool = False
) -> dict[str, Any]:
    """Collect native batches and finish cleanup even under caller cancellation."""
    updates: list[Any] = []
    pending: asyncio.Task[Any] | None = None
    primary_error: BaseException | None = None
    reason = "max_updates"
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
    try:
        while len(updates) < max_updates:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                reason = "timeout"
                break
            pending = asyncio.create_task(anext(subscription))
            completed, _ = await asyncio.wait((pending,), timeout=remaining)
            if not completed:
                reason = "timeout"
                break
            try:
                updates.append(pending.result())
            except StopAsyncIteration:
                reason = "done"
                break
            pending = None
    except BaseException as exc:
        primary_error = exc

    if pending is not None:
        pending.cancel()
    cleanup = asyncio.create_task(
        _release_subscription(subscription, pending, drain and not isinstance(primary_error, asyncio.CancelledError))
    )
    # Shield alone is insufficient: cancellation would return while cleanup was
    # still running and a synchronous invoke could close its event loop early.
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as exc:
            if primary_error is None:
                primary_error = exc
        except BaseException:
            break  # Retrieve the cleanup exception below, preserving precedence.

    cleanup_error: str | None = None
    try:
        cleanup.result()
    except asyncio.CancelledError as exc:
        if primary_error is None:
            primary_error = exc
    except Exception as exc:
        if primary_error is None:
            cleanup_error = str(exc)
    if primary_error is not None:
        raise primary_error

    result: dict[str, Any] = {
        "updateCount": len(updates),
        "maxUpdates": max_updates,
        "timeoutMs": timeout_ms,
        "reason": reason,
        "updates": updates,
    }
    if cleanup_error is not None:
        result["unsubscribeError"] = cleanup_error
    return result


async def _snapshot(name: str, data: dict[str, Any]) -> dict[str, Any]:
    from xbbg import blp

    max_updates = data.pop("max_updates")
    timeout_ms = data.pop("timeout_ms")
    drain = data.pop("drain", False)
    if name == "xbbg_stream_snapshot":
        tickers = data.pop("tickers")
        fields = data.pop("fields")
        service = "//blp/mktdata"
    elif name == "xbbg_mktbar_snapshot":
        tickers = [data.pop("ticker")]
        fields = data.pop("fields", ["LAST_PRICE"])
        service = "//blp/mktbar"
        subscription_options = data.pop("options", [])
        if not any(
            option.strip().lstrip("&").partition("=")[0].strip().lower() == "bar_size"
            for option in subscription_options
        ):
            subscription_options = [*subscription_options, "bar_size=1"]
        data["options"] = subscription_options
        data.setdefault("all_fields", True)
    else:
        tickers = [data.pop("ticker")]
        fields = data.pop("fields", [])
        service = "//blp/mktdepthdata"
        data.setdefault("all_fields", not fields)
    # asubscribe is the public API exposing all native subscription controls;
    # astream is an async generator and cannot supply an owned cleanup handle.
    subscription = await blp.asubscribe(tickers, fields, service=service, backend="native", **data)
    return await _collect_snapshot(subscription, max_updates=max_updates, timeout_ms=timeout_ms, drain=drain)


async def _execute(name: str, input: ToolInput, options: BloombergToolsOptions) -> Any:
    # Factories and their JSON schemas stay usable without loading the native
    # extension or connecting. Engine scope is exclusively owned by _runtime.
    from xbbg import blp

    data = input.model_dump(exclude_none=True)
    kwargs = data.pop("kwargs", {})
    if name in {"xbbg_bdp", "xbbg_bdh", "xbbg_bds"}:
        data.setdefault("validate_fields", options.validate_fields)
        securities = data.pop("securities")
        if name == "xbbg_bdp":
            return await blp.abdp(securities, data.pop("fields"), backend="native", **data, **kwargs)
        if name == "xbbg_bdh":
            fields = data.pop("fields")
            start, end = data.pop("start"), data.pop("end")
            return await blp.abdh(
                securities, fields, start_date=start, end_date=end, backend="native", **data, **kwargs
            )
        return await blp.abds(securities, data.pop("field"), backend="native", **data, **kwargs)
    if name in {"xbbg_bdib", "xbbg_bdtick"}:
        ticker, start, end = data.pop("ticker"), data.pop("start"), data.pop("end")
        if name == "xbbg_bdib":
            if "event_type" in data:
                data["typ"] = data.pop("event_type")
            return await blp.abdib(ticker, start_datetime=start, end_datetime=end, backend="native", **data, **kwargs)
        for input_name, bloomberg_name in _TICK_FLAGS.items():
            if input_name in data:
                kwargs[bloomberg_name] = data.pop(input_name)
        return await blp.abdtick(ticker, start, end, backend="native", **data, **kwargs)
    if name == "xbbg_check_entitlements":
        report = await blp.acheck_entitlements(**data)
        return {"entitled": report.entitled, "failedEids": report.failed_eids}
    if name == "xbbg_bql":
        return await blp.abql(data["query"], backend="native")
    if name == "xbbg_bsrch":
        return await blp.absrch(data.pop("search_spec"), backend="native", **data, **kwargs)
    if name == "xbbg_bflds":
        return await blp.abflds(backend="native", **data)
    if name == "xbbg_beqs":
        return await blp.abeqs(backend="native", **data, **kwargs)
    if name == "xbbg_bqr":
        from xbbg.ext.fixed_income import abqr

        return await abqr(
            data.pop("ticker"), start_datetime=data.pop("start"), end_datetime=data.pop("end"), backend="native", **data
        )
    if name == "xbbg_yas":
        from xbbg.ext.fixed_income import ayas

        if "yield_val" in data:
            data["yield_"] = data.pop("yield_val")
        return await ayas(data.pop("tickers"), data.pop("fields"), backend="native", **data)
    if name == "xbbg_preferreds":
        from xbbg.ext.fixed_income import apreferreds

        build_query(
            "build_preferreds_query",
            {"equity_ticker": data["equity_ticker"], "extra_fields": data.get("fields", [])},
            options.max_bql_query_chars,
        )
        return await apreferreds(backend="native", **data)
    if name == "xbbg_corporate_bonds":
        from xbbg.ext.fixed_income import acorporate_bonds

        build_query(
            "build_corporate_bonds_query",
            {"ticker": data["ticker"], "ccy": data.get("ccy"), "extra_fields": data.get("fields", [])},
            options.max_bql_query_chars,
        )
        # Unlike the Python recipe's USD default, omitted JS currency means no
        # currency filter. Pass None explicitly to preserve that contract.
        return await acorporate_bonds(ccy=data.pop("ccy", None), backend="native", **data)
    if name == "xbbg_index_members":
        from xbbg.ext.indices import aindex_members

        return await aindex_members(backend="native", **data)
    if name == "xbbg_resolve_isins":
        from xbbg.ext.identifiers import aresolve_isins

        return await aresolve_isins(backend="native", **data)
    if name == "xbbg_issuer_isins":
        from xbbg.ext.identifiers import aissuer_isins

        return await aissuer_isins(backend="native", **data)
    if name == "xbbg_etf_holdings":
        from xbbg.ext.historical import aetf_holdings

        build_query(
            "build_etf_holdings_query",
            {"etf_ticker": data["etf_ticker"], "extra_fields": data.get("fields", [])},
            options.max_bql_query_chars,
        )
        return await aetf_holdings(backend="native", **data)
    if name in {"xbbg_stream_snapshot", "xbbg_mktbar_snapshot", "xbbg_depth_snapshot"}:
        return await _snapshot(name, data)
    raise ValueError(f"Unknown Bloomberg core tool: {name}")


def _build_tool(name: str, options: BloombergToolsOptions) -> StructuredTool:
    if name in options.disabled_tools:
        raise ValueError(f"Bloomberg tool {name!r} is disabled")

    async def handler(input: ToolInput) -> Any:
        return await _execute(name, input, options)

    return make_tool(name, _DESCRIPTIONS[name], create_core_schema(name, options), handler, options)


def create_bdp_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bdp", resolve_options(options, kwargs))


def create_bdh_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bdh", resolve_options(options, kwargs))


def create_bds_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bds", resolve_options(options, kwargs))


def create_bdib_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bdib", resolve_options(options, kwargs))


def create_bdtick_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bdtick", resolve_options(options, kwargs))


def create_check_entitlements_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_check_entitlements", resolve_options(options, kwargs))


def create_bql_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bql", resolve_options(options, kwargs))


def create_bsrch_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bsrch", resolve_options(options, kwargs))


def create_bqr_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bqr", resolve_options(options, kwargs))


def create_bflds_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_bflds", resolve_options(options, kwargs))


def create_beqs_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_beqs", resolve_options(options, kwargs))


def create_yas_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_yas", resolve_options(options, kwargs))


def create_preferreds_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_preferreds", resolve_options(options, kwargs))


def create_corporate_bonds_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_corporate_bonds", resolve_options(options, kwargs))


def create_index_members_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_index_members", resolve_options(options, kwargs))


def create_resolve_isins_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_resolve_isins", resolve_options(options, kwargs))


def create_issuer_isins_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_issuer_isins", resolve_options(options, kwargs))


def create_etf_holdings_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_etf_holdings", resolve_options(options, kwargs))


def create_stream_snapshot_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_stream_snapshot", resolve_options(options, kwargs))


def create_mktbar_snapshot_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_mktbar_snapshot", resolve_options(options, kwargs))


def create_depth_snapshot_tool(options: BloombergToolsOptions | None = None, **kwargs: Any) -> StructuredTool:
    return _build_tool("xbbg_depth_snapshot", resolve_options(options, kwargs))


def create_bloomberg_tools(options: BloombergToolsOptions | None = None, **kwargs: Any) -> list[StructuredTool]:
    resolved = resolve_options(options, kwargs)
    return [_build_tool(name, resolved) for name in BLOOMBERG_CORE_TOOL_NAMES if name not in resolved.disabled_tools]


__all__ = [
    "BLOOMBERG_CORE_TOOL_NAMES",
    "create_bloomberg_tools",
    "create_bdp_tool",
    "create_bdh_tool",
    "create_bds_tool",
    "create_bdib_tool",
    "create_bdtick_tool",
    "create_check_entitlements_tool",
    "create_bql_tool",
    "create_bsrch_tool",
    "create_bqr_tool",
    "create_bflds_tool",
    "create_beqs_tool",
    "create_yas_tool",
    "create_preferreds_tool",
    "create_corporate_bonds_tool",
    "create_index_members_tool",
    "create_resolve_isins_tool",
    "create_issuer_isins_tool",
    "create_etf_holdings_tool",
    "create_stream_snapshot_tool",
    "create_mktbar_snapshot_tool",
    "create_depth_snapshot_tool",
]
