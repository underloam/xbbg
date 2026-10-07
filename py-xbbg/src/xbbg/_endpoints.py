"""Typed endpoint signatures and their Bloomberg request plans.

The original async functions are the public implementations, not templates for
exec-generated copies. The facade supplies one executor callback for completed
plans and installs sync wrappers; no endpoint imports the facade, even lazily.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
from typing import Any, TypeAlias
import warnings

from . import _engine, _request_options, backend as _backend
from ._dates import DateLike, _fmt_date, _fmt_datetime
from ._sync import _build_sync_wrapper
from .backend import Backend, ensure_arrow_table
from .services import ExtractorHint, Format, Operation, Service

DataFrameResult: TypeAlias = Any
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _EndpointPlan:
    request_kwargs: dict[str, Any]
    backend: Backend | str | None
    postprocess: Callable[[Any], DataFrameResult] | None = None
    service: Service | None = None
    operation: Operation | None = None
    extractor: ExtractorHint | None = None


@dataclass(frozen=True)
class _GeneratedEndpointSpec:
    async_name: str
    sync_name: str
    service: Service
    operation: Operation
    builder: Callable[[dict[str, Any]], Awaitable[_EndpointPlan] | _EndpointPlan]
    extractor: ExtractorHint | None = None


_GENERATED_ENDPOINT_SPECS: dict[str, _GeneratedEndpointSpec] = {}
_execute_generated_endpoint: Callable[[_GeneratedEndpointSpec, dict[str, Any]], Awaitable[DataFrameResult]]


def _install_generated_endpoints(
    execute: Callable[[_GeneratedEndpointSpec, dict[str, Any]], Awaitable[DataFrameResult]],
    namespace: dict[str, Any],
) -> None:
    """Bind the facade's plan executor and generate only synchronous wrappers."""
    global _execute_generated_endpoint
    _execute_generated_endpoint = execute
    for spec in _GENERATED_ENDPOINT_SPECS.values():
        namespace[spec.sync_name] = _build_sync_wrapper(spec.sync_name, globals()[spec.async_name])


# =============================================================================
# Async API - Typed Convenience Functions
# =============================================================================


async def abdp(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    include_security_errors: bool = False,
    return_eids: bool = False,
    validate_fields: bool | None = None,
    **kwargs,
):
    """Async Bloomberg reference data (BDP).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields to query.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, field, value (strings)
            - Format.LONG_TYPED: ticker, field, value_f64, value_i64,
              value_str, value_bool, value_date, value_ts, value_time.
              Dates use Date32, full datetimes use UTC timestamps, and
              time-only values use Time64 microseconds in value_time.
            - Format.LONG_WITH_METADATA: ticker, field, value, dtype
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        include_security_errors: Include ``__SECURITY_ERROR__`` rows for
            securities that Bloomberg rejected.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame in long format with columns: ticker, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdp("AAPL US Equity", ["PX_LAST", "VOLUME"])

        # Concurrent requests
        dfs = await asyncio.gather(
            abdp("AAPL US Equity", "PX_LAST"),
            abdp("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdp"], locals())


async def abdh(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    start_date: DateLike = None,
    end_date: DateLike = "today",
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    validate_fields: bool | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg historical data (BDH).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields. Defaults to ['PX_LAST'].
        start_date: Start date. Defaults to 8 weeks before end_date.
        end_date: End date. Defaults to 'today'.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, date, field, value (strings)
            - Format.LONG_TYPED: ticker, date, field, value_f64, value_i64,
              value_str, value_bool, value_date, value_ts, value_time.
              Dates use Date32, full datetimes use UTC timestamps, and
              time-only values use Time64 microseconds in value_time.
            - Format.LONG_WITH_METADATA: ticker, date, field, value, dtype
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional overrides and infrastructure options.
            adjust: Adjustment type ('all', 'dvd', 'split', '-', None).

    Returns:
        DataFrame in long format with columns: ticker, date, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdh("AAPL US Equity", "PX_LAST", start_date="2024-01-01")

        # Concurrent requests
        dfs = await asyncio.gather(
            abdh("AAPL US Equity", "PX_LAST"),
            abdh("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdh"], locals())


async def abds(
    tickers: str | Sequence[str],
    flds: str,
    *,
    backend: Backend | str | None = None,
    validate_fields: bool | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg bulk data (BDS).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field name (bulk fields return multiple rows).
        backend: DataFrame backend to return. If None, uses global default.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame with one row per Bloomberg bulk row. The only xbbg-added
        columns are ``ticker`` and ``field``; bulk subfield columns preserve
        Bloomberg's labels exactly as emitted, including spaces, punctuation,
        and case. Higher-level helpers must rename their own semantic outputs.

    Example::

        df = await abds("AAPL US Equity", "DVD_Hist_All")
        df = await abds("SPX Index", "INDX_MEMBERS", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abds"], locals())


async def abdib(
    ticker: str,
    dt: DateLike = None,
    session: str = "allday",
    typ: str = "TRADE",
    *,
    start_datetime: DateLike = None,
    end_datetime: DateLike = None,
    interval: int = 1,
    backend: Backend | str | None = None,
    request_tz: str | None = None,
    output_tz: str | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg intraday bar data (BDIB).

    Args:
        ticker: Ticker name.
        dt: Date to download (for single-day requests).
        session: Trading session name. Ignored when start_datetime/end_datetime provided.
        typ: Event type (TRADE, BID, ASK, etc.).
        start_datetime: Explicit start datetime for multi-day requests.
        end_datetime: Explicit end datetime for multi-day requests.
        interval: Bar interval in minutes (default: 1), or seconds if intervalHasSeconds=True.
        backend: DataFrame backend to return. If None, uses global default.
        request_tz: How naive ``start_datetime`` / ``end_datetime`` (and full-day ``dt`` window)
            are interpreted before Bloomberg: ``UTC`` (default when omitted), ``local``,
            ``exchange`` (uses this ticker), ``NY``/``LN``/``TK``/``HK``, another ticker string,
            or an IANA zone. Conversion to UTC is done in the Rust engine.
        output_tz: Relabel the ``time`` column to this zone (same instants; Rust engine).
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional Bloomberg options (e.g., intervalHasSeconds,
            gapFillInitialBar, or 0.x request-element aliases such as ``Points=1``).
            Pass true Bloomberg field overrides via ``overrides={...}``.

    Returns:
        DataFrame with intraday bar data.

    Example::

        # 1-minute bars (default)
        df = await abdib("AAPL US Equity", dt="2024-12-01")

        # 5-minute bars with explicit datetime range
        df = await abdib(
            "AAPL US Equity",
            start_datetime="2024-12-01 09:30",
            end_datetime="2024-12-01 16:00",
            interval=5,
        )

        # 10-second bars
        df = await abdib("AAPL US Equity", dt="2024-12-01", interval=10, intervalHasSeconds=True)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdib"], locals())


async def abdtick(
    ticker: str,
    start_datetime: DateLike,
    end_datetime: DateLike,
    *,
    event_types: Sequence[str] | None = None,
    backend: Backend | str | None = None,
    request_tz: str | None = None,
    output_tz: str | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg tick data (BDTICK).

    Args:
        ticker: Ticker name.
        start_datetime: Start datetime.
        end_datetime: End datetime.
        event_types: Event types to retrieve. Defaults to ["TRADE"].
            Options: TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK.
        backend: DataFrame backend to return. If None, uses global default.
        request_tz: How naive datetimes are interpreted before Bloomberg (see ``abdib``).
        output_tz: Relabel ``time`` column (same instants; Rust engine).
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional Bloomberg options. Schema-recognized request elements
            and 0.x request-element aliases such as ``Points=1`` may be passed as
            individual keyword arguments. Pass true Bloomberg field overrides via
            ``overrides={...}``.

    Returns:
        DataFrame with tick data.

    Example::

        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00")
        df = await abdtick(
            "AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", event_types=["TRADE", "BID", "ASK"]
        )
        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdtick"], locals())


# =============================================================================
# BQL API - Bloomberg Query Language
# =============================================================================


async def abql(
    expression: str,
    *,
    backend: Backend | str | None = None,
) -> DataFrameResult:
    """Async Bloomberg Query Language (BQL) request.

    BQL is Bloomberg's powerful query language for financial analytics.
    It allows you to query data across universes of securities with
    complex filters, calculations, and time series operations.

    Args:
        expression: BQL expression string.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        DataFrame with columns: id, <field1>, <field2>, ...
        Where 'id' is the security identifier from the BQL universe.

    Example::

        # Get price for a single security
        df = await abql("get(px_last) for('AAPL US Equity')")

        # Get multiple fields
        df = await abql("get(px_last, volume) for('AAPL US Equity')")

        # Holdings of an ETF
        df = await abql("get(id_isin, weights) for(holdings('SPY US Equity'))")

        # Index members
        df = await abql("get(px_last) for(members('SPX Index'))")

        # With filters
        df = await abql("get(px_last, pe_ratio) for(members('SPX Index')) with(pe_ratio > 20)")

        # Time series
        df = await abql("get(px_last) for('AAPL US Equity') with(dates=range(-5d, 0d))")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abql"], locals())


# =============================================================================
# BSRCH API - Bloomberg Search
# =============================================================================


async def absrch(
    domain: str,
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Search (BSRCH) request.

    BSRCH executes saved Bloomberg searches and returns matching securities.

    Args:
        domain: The saved search domain/name (e.g., "FI:SOVR", "COMDTY:PRECIOUS").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Search parameters. Direct keyword arguments and explicit
            ``overrides={...}`` mappings or ``(name, value)`` pairs are sent
            as ExcelGetGrid overrides. ``Domain`` is sent as the request's
            top-level domain element.

    Returns:
        DataFrame with columns from the saved search results.

    Example::

        # Sovereign bonds
        df = await absrch("FI:SOVR")

        # With additional parameters
        df = await absrch("COMDTY:WEATHER", LOCATION="NYC", MODEL="GFS")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["absrch"], locals())


# =============================================================================
# BQR API - Bloomberg Quote Request
# =============================================================================


def _parse_date_offset(offset: str, reference: datetime) -> datetime:
    """Parse date offset string like '-2d', '-1w', '-1m', '-3h'."""
    import re

    offset = offset.strip().lower()
    match = re.match(r"^(-?\d+)([dwmh])$", offset)
    if not match:
        raise ValueError(f"Invalid date offset format: {offset}. Use format like '-2d', '-1w', '-1m', '-3h'")

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "d":
        return reference + timedelta(days=value)
    if unit == "w":
        return reference + timedelta(weeks=value)
    if unit == "m":
        return reference + timedelta(days=value * 30)
    if unit == "h":
        return reference + timedelta(hours=value)
    raise ValueError(f"Unknown time unit: {unit}")


def _reshape_bqr_generic(table: Any, ticker: str) -> Any:
    """Reshape generic extractor output into structured BQR rows via native Arrow."""
    return table.reshape_bqr_generic(ticker)


_BQR_RENAME_MAP: dict[str, str] = {
    "type": "event_type",
    "value": "price",
    "brokerBuyCode": "broker_buy",
    "brokerSellCode": "broker_sell",
    "spreadPrice": "spread_price",
    "conditionCodes": "condition_codes",
    "exchangeCode": "exchange",
}
_BQR_BROKER_COLUMNS = ("brokerBuyCode", "brokerSellCode", "broker_buy", "broker_sell")
_BQR_DEALER_INPUT_EXAMPLE = "/isin/US037833FB15@MSG1 Corp"


def _looks_like_bqr_dealer_input(ticker: str) -> bool:
    normalized = " ".join(ticker.strip().casefold().split())
    return normalized.startswith("/isin/") and "@msg1 corp" in normalized


def _warn_bqr_dealer_input(ticker: str, *, stacklevel: int = 3) -> None:
    if _looks_like_bqr_dealer_input(ticker):
        return
    warnings.warn(
        "BQR broker attribution is intended for fixed-income ISIN inputs with an @MSG1 Corp "
        f"dealer quote source, for example '{_BQR_DEALER_INPUT_EXAMPLE}'. Other inputs may "
        "return quote rows without broker_buy/broker_sell and will raise unless "
        "include_broker_codes=False is passed explicitly.",
        UserWarning,
        stacklevel=stacklevel,
    )


def _bqr_has_broker_code_value(table: Any) -> bool:
    return table.has_any_value(list(_BQR_BROKER_COLUMNS))


def _postprocess_bqr_result(
    result: Any,
    *,
    ticker: str,
    backend: Backend | str | None,
    enforce_broker_codes: bool,
) -> DataFrameResult:
    table = ensure_arrow_table(result)

    if "path" in table.column_names:
        table = _reshape_bqr_generic(table, ticker)

    if enforce_broker_codes and table.num_rows > 0 and not _bqr_has_broker_code_value(table):
        raise RuntimeError(
            "BQR returned quote rows without broker attribution. "
            "Use a fixed-income ticker with a dealer quote pricing source such as '@MSG1 Corp', "
            "or pass include_broker_codes=False if raw quote ticks without dealer codes are intentional."
        )

    if table.num_rows > 0 and "time" in table.column_names:
        table = table.sort_by([("time", "ascending")])

    rename_map = {column: _BQR_RENAME_MAP[column] for column in table.column_names if column in _BQR_RENAME_MAP}
    table = table.rename_columns(rename_map)
    return _backend._convert_result_backend(table, backend)


async def abqr(
    ticker: str,
    date_offset: str | None = None,
    start_date: DateLike = None,
    end_date: DateLike = None,
    *,
    event_types: Sequence[str] | None = None,
    include_broker_codes: bool = True,
    include_spread_price: bool = False,
    include_yield: bool = False,
    include_condition_codes: bool = False,
    include_exchange_codes: bool = False,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Quote Request (BQR).

    Retrieves dealer quote data using IntradayTickRequest with BID/ASK events.
    Emulates the Excel =BQR() function.

    Args:
        ticker: Security identifier. Supports Bloomberg tickers with pricing
            source qualifiers (e.g., 'IBM US Equity@MSG1', '/isin/US037833FB15@MSG1').
        date_offset: Date offset from now (e.g., '-2d', '-1w', '-3h').
            Mutually exclusive with start_date/end_date.
        start_date: Start date (e.g., '2024-01-15'). Defaults to 2 days ago.
        end_date: End date (e.g., '2024-01-17'). Defaults to today.
        event_types: Event types to retrieve. Defaults to ['BID', 'ASK'].
        include_broker_codes: Include broker/dealer codes (default True).
        include_spread_price: Include spread price for bonds (default False).
        include_yield: Include yield data for bonds (default False).
        include_condition_codes: Include trade condition codes (default False).
        include_exchange_codes: Include exchange codes (default False).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional options.

    Returns:
        DataFrame with columns: ticker, time, event_type, price, size,
        plus optional broker_buy, broker_sell, spread_price, etc.

    Example::

        # With date offset (like Excel BQR)
        df = await abqr("IBM US Equity@MSG1", date_offset="-2d")

        # Bond with broker codes and spread
        df = await abqr(
            "US037833FB15@MSG1 Corp",
            date_offset="-2d",
            include_broker_codes=True,
            include_spread_price=True,
        )

        # With explicit date range
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            start_date="2024-01-15",
            end_date="2024-01-17",
        )

        # Trade events only
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            date_offset="-1d",
            event_types=["TRADE"],
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abqr"], locals())


async def abflds(
    fields: str | list[str] | None = None,
    *,
    search_spec: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg field metadata lookup (BFLDS).

    Unified field function: get metadata for specific fields, or search by keyword.

    Args:
        fields: Single field or list of fields to get metadata for.
            Mutually exclusive with search_spec.
        search_spec: Search term to find fields by name/description.
            Mutually exclusive with fields.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options (e.g., port, server).

    Returns:
        DataFrame with field information or search results.

    Raises:
        ValueError: If neither fields nor search_spec is provided, or both are provided.

    Example::

        # Get info for specific fields
        df = await abflds(fields=["PX_LAST", "VOLUME"])

        # Search for fields by keyword
        df = await abflds(search_spec="vwap")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abflds"], locals())


# =============================================================================
# BEQS API - Bloomberg Equity Screening
# =============================================================================


async def abeqs(
    screen: str,
    *,
    asof: str | None = None,
    screen_type: str = "PRIVATE",
    group: str = "General",
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Equity Screening (BEQS) request.

    Execute a saved Bloomberg equity screen and return matching securities.

    Args:
        screen: Screen name as saved in Bloomberg.
        asof: As-of date for the screen. Accepts ISO 8601 / ``YYYYMMDD``
            string, ``datetime.date``, ``datetime.datetime``, or
            ``pd.Timestamp``.
        screen_type: Screen type - "PRIVATE" (custom) or "GLOBAL" (Bloomberg).
        group: Group name if screen is organized into groups.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns from the screen results (security, fieldData, etc.).

    Example::

        # Run a private screen
        df = await abeqs("MyScreen")

        # Run with as-of date
        df = await abeqs("MyScreen", asof="20240101")

        # Run a Bloomberg global screen
        df = await abeqs("TOP_DECL_DVD", screen_type="GLOBAL")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abeqs"], locals())


# =============================================================================
# BLKP API - Bloomberg Security Lookup
# =============================================================================


async def ablkp(
    query: str,
    *,
    yellowkey: str = "YK_FILTER_NONE",
    language: str = "LANG_OVERRIDE_NONE",
    max_results: int = 20,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg security lookup (BLKP) request.

    Search for securities by company name or partial ticker.

    Args:
        query: Search query (company name or partial ticker).
        yellowkey: Asset class filter. Common values:
            - "YK_FILTER_NONE" (default, all asset classes)
            - "YK_FILTER_EQTY" (equities only)
            - "YK_FILTER_CORP" (corporate bonds)
            - "YK_FILTER_GOVT" (government bonds)
            - "YK_FILTER_INDX" (indices)
            - "YK_FILTER_CURR" (currencies)
            - "YK_FILTER_CMDT" (commodities)
        language: Language override for results.
        max_results: Maximum number of results (default: 20, max: 1000).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns: security, description, and other result fields.

    Example::

        # Search for Apple
        df = await ablkp("Apple")

        # Search for equities only
        df = await ablkp("NVDA", yellowkey="YK_FILTER_EQTY")

        # Get more results
        df = await ablkp("Microsoft", max_results=50)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["ablkp"], locals())


# =============================================================================
# BPORT API - Bloomberg Portfolio Data
# =============================================================================


async def abport(
    portfolio: str,
    fields: str | Sequence[str],
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg portfolio data (BPORT) request.

    Get portfolio holdings and related data using PortfolioDataRequest.

    Args:
        portfolio: Bloomberg PortfolioDataRequest security/portfolio ID string, not the PORT display name
            (for example, "UXXXXXXX-X Client" from PRTU/PORT).
        fields: Field name or list of fields (e.g., "PORTFOLIO_MWEIGHT").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters/overrides.

    Returns:
        DataFrame with portfolio data.

    Example::

        # Get portfolio overview data. Use the Bloomberg portfolio ID/security
        # string, not the human-readable PORT display name.
        df = await abport("UXXXXXXX-X Client", "PORTFOLIO_DATA")

        # Get portfolio weights
        df = await abport("UXXXXXXX-X Client", "PORTFOLIO_MWEIGHT")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abport"], locals())


# =============================================================================
# BCURVES API - Bloomberg Yield Curve List
# =============================================================================


async def abcurves(
    *,
    country: str | None = None,
    currency: str | None = None,
    curve_type: str | None = None,
    subtype: str | None = None,
    curveid: str | None = None,
    bbgid: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg yield curve list (BCURVES) request.

    Search for yield curves by country, currency, type, or other filters.

    Args:
        country: Country code filter (e.g., "US", "GB", "DE").
        currency: Currency code filter (e.g., "USD", "EUR", "GBP").
        curve_type: Curve type filter (e.g., "GOVERNMENT", "CORPORATE").
        subtype: Curve subtype filter.
        curveid: Specific curve ID to look up.
        bbgid: Bloomberg Global ID filter.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with yield curve information.

    Example::

        # List US yield curves
        df = await abcurves(country="US")

        # List USD government curves
        df = await abcurves(currency="USD", curve_type="GOVERNMENT")

        # Look up specific curve
        df = await abcurves(curveid="YCSW0023 Index")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abcurves"], locals())


# =============================================================================
# BGOVTS API - Bloomberg Government Securities List
# =============================================================================


async def abgovts(
    query: str | None = None,
    *,
    partial_match: bool = True,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg government securities list (BGOVTS) request.

    Search for government securities by ticker or name.

    Args:
        query: Search query (ticker or partial name).
        partial_match: If True, match partial ticker names (default: True).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with government securities information.

    Example::

        # Search for US Treasury securities
        df = await abgovts("T")

        # Search for German government bonds
        df = await abgovts("DBR")

        # Exact match only
        df = await abgovts("T 2.5 05/15/24", partial_match=False)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abgovts"], locals())


async def _build_abdp_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _request_options._normalize_tickers(args["tickers"])
    field_list = _request_options._normalize_fields(args.get("flds"))
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _request_options._normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs

    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)
    fmt = Format(args["format"]) if isinstance(args.get("format"), str) else args.get("format")

    resolved_types = await _engine._resolve_field_types_cached(
        field_list,
        args.get("field_types"),
        "string",
    )

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "overrides": overrides if overrides else None,
            "security_overrides": security_overrides,
            "elements": elements if elements else None,
            "field_types": resolved_types,
            "format": fmt,
            "include_security_errors": args.get("include_security_errors", False),
            "return_eids": args.get("return_eids", False),
            "validate_fields": args.get("validate_fields"),
        },
        backend=args.get("backend"),
        postprocess=None,
    )


async def _build_abdh_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _request_options._normalize_tickers(args["tickers"])
    field_list = _request_options._normalize_fields(args.get("flds"))
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _request_options._normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs
    presentation = _request_options._pop_presentation_aliases(kwargs)

    fmt = Format(args["format"]) if isinstance(args.get("format"), str) else args.get("format")
    fmt = _request_options._presentation_format(fmt, presentation)

    end_value = args.get("end_date", "today")
    start_value = args.get("start_date")

    # ``end_date`` defaults to "today" via the public signature, but callers may
    # explicitly pass ``end_date=None``; preserve the legacy "today" fallback in
    # that case so default ``bdh()`` calls remain unchanged.
    e_dt = _fmt_date(end_value, "%Y%m%d", default_today_on_none=True)
    if start_value is None:
        end_dt_parsed = datetime.strptime(e_dt, "%Y%m%d")
        s_dt = (end_dt_parsed - timedelta(weeks=8)).strftime("%Y%m%d")
    else:
        s_dt = _fmt_date(start_value, "%Y%m%d")

    options: list[tuple[str, str]] = []
    adjust = kwargs.pop("adjust", None)
    if adjust == "all":
        options.extend(
            [
                ("adjustmentSplit", "true"),
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "dvd":
        options.extend(
            [
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "split":
        options.append(("adjustmentSplit", "true"))

    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.HISTORICAL_DATA, kwargs)
    presentation_periodicity = _request_options._periodicity_selection(elements)

    resolved_types = await _engine._resolve_field_types_cached(
        field_list,
        args.get("field_types"),
        "float64",
    )

    backend = args.get("backend")
    needs_presentation_postprocess = (
        presentation.show_date is not None
        or presentation.sort is not None
        or presentation.date_format in {"PERIODIC", "BOTH"}
    )

    def postprocess(raw: Any) -> DataFrameResult:
        shaped = _request_options._apply_historical_presentation(
            raw,
            presentation,
            periodicity=presentation_periodicity,
        )
        return _backend._convert_result_backend(shaped, backend)

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "start_date": s_dt,
            "end_date": e_dt,
            "overrides": overrides if overrides else None,
            "security_overrides": security_overrides,
            "elements": elements if elements else None,
            "options": options if options else None,
            "field_types": resolved_types,
            "format": fmt,
            "validate_fields": args.get("validate_fields"),
            "return_eids": args.get("return_eids", False),
        },
        backend=backend,
        postprocess=postprocess if needs_presentation_postprocess else None,
    )


async def _build_abds_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _request_options._normalize_tickers(args["tickers"])
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _request_options._normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs
    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)

    req: dict[str, Any] = {
        "securities": ticker_list,
        "fields": [args["flds"]],
        "overrides": overrides if overrides else None,
        "security_overrides": security_overrides,
        "elements": elements if elements else None,
        "validate_fields": args.get("validate_fields"),
    }
    if args.get("return_eids"):
        req["return_eids"] = True

    return _EndpointPlan(request_kwargs=req, backend=args.get("backend"))


async def _build_abdib_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))

    start_dt = args.get("start_datetime")
    end_dt = args.get("end_datetime")
    dt_value = args.get("dt")

    if start_dt is not None and end_dt is not None:
        # Preserve any tz info the caller supplied; let the Rust engine
        # handle naive strings according to ``request_tz``.
        s_dt = _fmt_datetime(start_dt, default_tz=None)
        e_dt = _fmt_datetime(end_dt, default_tz=None)
    elif dt_value is not None:
        cur_dt = _fmt_date(dt_value, "%Y-%m-%d")
        s_dt = f"{cur_dt}T00:00:00"
        e_dt = f"{cur_dt}T23:59:59"
    else:
        raise ValueError("Either dt or both start_datetime and end_datetime must be provided")

    interval = args["interval"]
    alias_interval = _request_options._pop_element_alias(kwargs, "interval")
    if alias_interval is not None:
        interval = int(alias_interval)

    event_type = args["typ"]
    alias_event_type = _request_options._pop_element_alias(kwargs, "eventType")
    if alias_event_type is not None:
        event_type = str(alias_event_type)

    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.INTRADAY_BAR, kwargs)

    req: dict[str, Any] = {
        "security": args["ticker"],
        "event_type": event_type,
        "interval": interval,
        "start_datetime": s_dt,
        "end_datetime": e_dt,
        "elements": elements if elements else None,
        "overrides": overrides if overrides else None,
    }
    if args.get("return_eids"):
        req["return_eids"] = True
    if args.get("request_tz") is not None:
        req["request_tz"] = args["request_tz"]
    if args.get("output_tz") is not None:
        req["output_tz"] = args["output_tz"]

    return _EndpointPlan(
        request_kwargs=req,
        backend=args.get("backend"),
    )


async def _build_abdtick_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))

    # Accept native datetime/date plus duck-typed pd.Timestamp; preserve any
    # tz info the caller supplied so naive strings keep being interpreted by
    # the Rust engine according to ``request_tz``.
    s_dt = _fmt_datetime(args["start_datetime"], default_tz=None)
    e_dt = _fmt_datetime(args["end_datetime"], default_tz=None)

    alias_event_type = _request_options._pop_element_alias(kwargs, "eventType")
    event_types = args.get("event_types")
    if event_types is None:
        event_types = [str(alias_event_type)] if alias_event_type is not None else ["TRADE"]

    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.INTRADAY_TICK, kwargs)

    req: dict[str, Any] = {
        "security": args["ticker"],
        "start_datetime": s_dt,
        "end_datetime": e_dt,
        "event_types": list(event_types),
        "elements": elements if elements else None,
        "overrides": overrides if overrides else None,
    }
    if args.get("return_eids"):
        req["return_eids"] = True
    if args.get("request_tz") is not None:
        req["request_tz"] = args["request_tz"]
    if args.get("output_tz") is not None:
        req["output_tz"] = args["output_tz"]

    return _EndpointPlan(
        request_kwargs=req,
        backend=args.get("backend"),
    )


def _build_abql_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _EndpointPlan(
        request_kwargs={"overrides": {"expression": args["expression"]}},
        backend=args.get("backend"),
    )


async def _build_abqr_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    event_types = args.get("event_types")
    if event_types is None:
        event_types = ["BID", "ASK"]

    now = datetime.now()
    time_fmt = "%Y-%m-%dT%H:%M:%S"

    def fmt_bqr_datetime(value: Any, default_time: str) -> str:
        # Native types (datetime / date / pd.Timestamp).
        if not isinstance(value, str):
            if isinstance(value, datetime):
                return value.strftime(time_fmt)
            if isinstance(value, date):
                return _fmt_date(value, "%Y-%m-%d") + default_time
            if hasattr(value, "to_pydatetime"):
                coerced = value.to_pydatetime()
                if isinstance(coerced, datetime):
                    return coerced.strftime(time_fmt)
                if isinstance(coerced, date):
                    return _fmt_date(coerced, "%Y-%m-%d") + default_time
        text = str(value).replace(" ", "T")
        if "T" in text:
            return datetime.fromisoformat(text).strftime(time_fmt)
        return _fmt_date(value, "%Y-%m-%d") + default_time

    date_offset = args.get("date_offset")
    start_datetime = kwargs.pop("start_datetime", None)
    end_datetime = kwargs.pop("end_datetime", None)
    start_date = args.get("start_date")
    end_date = args.get("end_date")

    if date_offset:
        end_dt = now
        start_dt = _parse_date_offset(date_offset, now)
        s_dt = start_dt.strftime(time_fmt)
        e_dt = end_dt.strftime(time_fmt)
    elif start_datetime is not None:
        s_dt = fmt_bqr_datetime(start_datetime, "T00:00:00")
        e_dt = fmt_bqr_datetime(end_datetime, "T23:59:59") if end_datetime is not None else now.strftime(time_fmt)
    elif start_date is not None:
        s_dt = fmt_bqr_datetime(start_date, "T00:00:00")
        e_dt = fmt_bqr_datetime(end_date, "T23:59:59") if end_date is not None else now.strftime(time_fmt)
    else:
        start_dt = now - timedelta(days=2)
        s_dt = start_dt.strftime(time_fmt)
        e_dt = now.strftime(time_fmt)

    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.INTRADAY_TICK, kwargs)

    def upsert_element(name: str, value: Any) -> None:
        for idx, (existing_name, _) in enumerate(elements):
            if existing_name == name:
                elements[idx] = (name, value)
                return
        elements.append((name, value))

    include_broker_codes = bool(args.get("include_broker_codes"))
    if include_broker_codes:
        upsert_element("includeBrokerCodes", "true")
    if args.get("include_spread_price"):
        upsert_element("includeSpreadPrice", "true")
    if args.get("include_yield"):
        upsert_element("includeYield", "true")
    if args.get("include_condition_codes"):
        upsert_element("includeConditionCodes", "true")
    if args.get("include_exchange_codes"):
        upsert_element("includeExchangeCodes", "true")

    ticker = args["ticker"]
    backend = args.get("backend")
    if include_broker_codes:
        _warn_bqr_dealer_input(ticker, stacklevel=4)

    logger.debug(
        "abqr: ticker=%s start=%s end=%s events=%s",
        ticker,
        s_dt,
        e_dt,
        event_types,
    )

    def postprocess(nw_df: Any) -> DataFrameResult:
        logger.debug("abqr: received %d rows", ensure_arrow_table(nw_df).num_rows)
        return _postprocess_bqr_result(
            nw_df,
            ticker=ticker,
            backend=backend,
            enforce_broker_codes=include_broker_codes,
        )

    return _EndpointPlan(
        request_kwargs={
            "security": ticker,
            "start_datetime": s_dt,
            "end_datetime": e_dt,
            "event_types": list(event_types),
            "elements": elements if elements else None,
            "overrides": overrides if overrides else None,
        },
        backend=backend,
        postprocess=postprocess,
    )


def _normalize_grid_override_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _iter_grid_override_pairs(value: Any) -> Iterable[tuple[Any, Any]]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return value.items()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        pairs: list[tuple[Any, Any]] = []
        for item in value:
            if isinstance(item, (str, bytes, bytearray)) or not isinstance(item, Sequence) or len(item) != 2:
                raise TypeError("bsrch overrides must be a mapping or a sequence of (name, value) pairs")
            pairs.append((item[0], item[1]))
        return pairs
    raise TypeError("bsrch overrides must be a mapping or a sequence of (name, value) pairs")


def _build_absrch_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    domain = str(args["domain"])
    grid_overrides: list[tuple[str, str]] = []

    def add_grid_override(key: Any, value: Any) -> None:
        nonlocal domain
        name = str(key)
        if name.lower() == "domain":
            domain = str(value)
            return
        grid_overrides.append((name, _normalize_grid_override_value(value)))

    for key, value in _iter_grid_override_pairs(kwargs.pop("overrides", None)):
        add_grid_override(key, value)
    for key, value in kwargs.items():
        add_grid_override(key, value)

    request_kwargs: dict[str, Any] = {"elements": [("Domain", domain)]}
    if grid_overrides:
        request_kwargs["overrides"] = grid_overrides

    return _EndpointPlan(
        request_kwargs=request_kwargs,
        backend=args.get("backend"),
    )


async def _build_abeqs_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.BEQS, kwargs)

    elements: list[tuple[str, Any]] = [
        ("screenName", args["screen"]),
        ("screenType", args["screen_type"]),
        ("Group", args["group"]),
    ]
    if args.get("asof"):
        elements.append(("asOfDate", _fmt_date(args["asof"])))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={
            "elements": elements,
            "overrides": overrides if overrides else None,
        },
        backend=args.get("backend"),
    )


async def _build_ablkp_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _request_options._aroute_kwargs(Service.INSTRUMENTS, Operation.INSTRUMENT_LIST, kwargs)

    elements: list[tuple[str, Any]] = [
        ("query", args["query"]),
        ("yellowKeyFilter", args["yellowkey"]),
        ("languageOverride", args["language"]),
        ("maxResults", args["max_results"]),
    ]
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements},
        backend=args.get("backend"),
    )


async def _build_abport_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    field_list = _request_options._normalize_fields(args["fields"])
    elements, overrides = await _request_options._aroute_kwargs(Service.REFDATA, Operation.PORTFOLIO_DATA, kwargs)

    return _EndpointPlan(
        request_kwargs={
            "securities": [args["portfolio"]],
            "fields": field_list,
            "elements": elements if elements else None,
            "overrides": overrides if overrides else None,
        },
        backend=args.get("backend"),
    )


async def _build_abcurves_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _request_options._aroute_kwargs(Service.INSTRUMENTS, Operation.CURVE_LIST, kwargs)

    elements: list[tuple[str, Any]] = []
    if args.get("country") is not None:
        elements.append(("countryCode", args["country"]))
    if args.get("currency") is not None:
        elements.append(("currencyCode", args["currency"]))
    if args.get("curve_type") is not None:
        elements.append(("type", args["curve_type"]))
    if args.get("subtype") is not None:
        elements.append(("subtype", args["subtype"]))
    if args.get("curveid") is not None:
        elements.append(("curveid", args["curveid"]))
    if args.get("bbgid") is not None:
        elements.append(("bbgid", args["bbgid"]))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements if elements else None},
        backend=args.get("backend"),
    )


async def _build_abgovts_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _request_options._aroute_kwargs(Service.INSTRUMENTS, Operation.GOVT_LIST, kwargs)

    elements: list[tuple[str, Any]] = []
    if args.get("query") is not None:
        elements.append(("ticker", args["query"]))
    elements.append(("partialMatch", args["partial_match"]))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements if elements else None},
        backend=args.get("backend"),
    )


def _build_abflds_plan(args: dict[str, Any]) -> _EndpointPlan:
    fields = args.get("fields")
    search_spec = args.get("search_spec")

    if fields is not None and search_spec is not None:
        raise ValueError("Cannot specify both 'fields' and 'search_spec'")
    if fields is None and search_spec is None:
        raise ValueError("Must specify either 'fields' or 'search_spec'")

    if fields is not None:
        field_list = _request_options._normalize_fields(fields)
        return _EndpointPlan(
            request_kwargs={"fields": field_list},
            backend=args.get("backend"),
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
        )

    return _EndpointPlan(
        request_kwargs={"fields": [search_spec]},
        backend=args.get("backend"),
        service=Service.APIFLDS,
        operation=Operation.FIELD_SEARCH,
        extractor=ExtractorHint.FIELD_INFO,
    )


_GENERATED_ENDPOINT_SPECS.update(
    {
        "abdp": _GeneratedEndpointSpec(
            async_name="abdp",
            sync_name="bdp",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abdp_plan,
        ),
        "abdh": _GeneratedEndpointSpec(
            async_name="abdh",
            sync_name="bdh",
            service=Service.REFDATA,
            operation=Operation.HISTORICAL_DATA,
            builder=_build_abdh_plan,
        ),
        "abds": _GeneratedEndpointSpec(
            async_name="abds",
            sync_name="bds",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abds_plan,
            extractor=ExtractorHint.BULK,
        ),
        "abdib": _GeneratedEndpointSpec(
            async_name="abdib",
            sync_name="bdib",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_BAR,
            builder=_build_abdib_plan,
        ),
        "abdtick": _GeneratedEndpointSpec(
            async_name="abdtick",
            sync_name="bdtick",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abdtick_plan,
        ),
        "abql": _GeneratedEndpointSpec(
            async_name="abql",
            sync_name="bql",
            service=Service.BQLSVC,
            operation=Operation.BQL_SEND_QUERY,
            builder=_build_abql_plan,
            extractor=ExtractorHint.BQL,
        ),
        "abqr": _GeneratedEndpointSpec(
            async_name="abqr",
            sync_name="bqr",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abqr_plan,
        ),
        "absrch": _GeneratedEndpointSpec(
            async_name="absrch",
            sync_name="bsrch",
            service=Service.EXRSVC,
            operation=Operation.EXCEL_GET_GRID,
            builder=_build_absrch_plan,
            extractor=ExtractorHint.BSRCH,
        ),
        "abeqs": _GeneratedEndpointSpec(
            async_name="abeqs",
            sync_name="beqs",
            service=Service.REFDATA,
            operation=Operation.BEQS,
            builder=_build_abeqs_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "ablkp": _GeneratedEndpointSpec(
            async_name="ablkp",
            sync_name="blkp",
            service=Service.INSTRUMENTS,
            operation=Operation.INSTRUMENT_LIST,
            builder=_build_ablkp_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abport": _GeneratedEndpointSpec(
            async_name="abport",
            sync_name="bport",
            service=Service.REFDATA,
            operation=Operation.PORTFOLIO_DATA,
            builder=_build_abport_plan,
        ),
        "abcurves": _GeneratedEndpointSpec(
            async_name="abcurves",
            sync_name="bcurves",
            service=Service.INSTRUMENTS,
            operation=Operation.CURVE_LIST,
            builder=_build_abcurves_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abgovts": _GeneratedEndpointSpec(
            async_name="abgovts",
            sync_name="bgovts",
            service=Service.INSTRUMENTS,
            operation=Operation.GOVT_LIST,
            builder=_build_abgovts_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abflds": _GeneratedEndpointSpec(
            async_name="abflds",
            sync_name="bflds",
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
            builder=_build_abflds_plan,
        ),
    }
)
