"""Fixed income extension functions.

Convenience adapters for Rust fixed income and bond analysis recipes.
Request construction and quote shaping run in Rust; results use the configured
DataFrame backend.

Sync functions (wrap async with asyncio.run):
    - yas(): Yield & Spread Analysis
    - preferreds(): Find preferred stocks for a company
    - corporate_bonds(): Find corporate bonds for a company
    - bqr(): Bloomberg Quote Request (dealer quotes)

Async functions (native recipe adapters):
    - ayas(): Async yield & spread analysis
    - apreferreds(): Async find preferred stocks
    - acorporate_bonds(): Async find corporate bonds
    - abqr(): Async Bloomberg Quote Request
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

from xbbg._dates import DateLike, _fmt_date, _fmt_datetime
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


class YieldType(IntEnum):
    """Bloomberg YAS yield type flags for YAS_YLD_FLAG override.

    These values control which yield calculation method Bloomberg uses
    in Yield & Spread Analysis (YAS) calculations.

    Standard Bloomberg YAS_YLD_FLAG values:
        1 = Yield to Maturity (YTM)
        2 = Yield to Call (YTC)
        3 = Yield to Refunding (YTR)
        4 = Yield to Next Put (YTP)
        5 = Yield to Worst (YTW)
        6 = Yield to Worst Refunding (YTWR)
        7 = Euro Yield to Worst (EYTW)
        8 = Euro Yield to Worst Refunding (EYTWR)
        9 = Yield to Average Life (YTAL)
    """

    YTM = 1
    YTC = 2
    YTR = 3
    YTP = 4
    YTW = 5
    YTWR = 6
    EYTW = 7
    EYTWR = 8
    YTAL = 9


# =============================================================================
# Async native recipe adapters
# =============================================================================


async def ayas(
    tickers: str | list[str],
    flds: str | list[str] = "YAS_BOND_YLD",
    *,
    settle_dt: DateLike = None,
    yield_type: YieldType | int | None = None,
    spread: float | None = None,
    yield_: float | None = None,
    price: float | None = None,
    benchmark: str | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async yield and spread analysis for fixed income securities.

    Executes the native YAS recipe and maps named parameters to Bloomberg
    overrides. Explicit overrides in ``kwargs`` take precedence.

    Args:
        tickers: Single ticker or list of bond tickers.
        flds: Field(s) to retrieve. Common YAS fields:
            - YAS_BOND_YLD: Calculated yield
            - YAS_YLD_SPREAD: Spread to benchmark
            - YAS_BOND_PX: Calculated price
            - YAS_ASSET_SWP_SPD: Asset swap spread
            - YAS_MOD_DUR: Modified duration
            - YAS_ISPREAD: I-spread
            - YAS_ZSPREAD: Z-spread
            - YAS_OAS: Option-adjusted spread
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        yield_type: Type of yield calculation. Use YieldType enum or int:
            - YieldType.YTM (1): Yield to Maturity
            - YieldType.YTC (2): Yield to Call
            - YieldType.YTR (3): Yield to Refunding
            - YieldType.YTP (4): Yield to Next Put
            - YieldType.YTW (5): Yield to Worst (next put, call, or maturity)
            - YieldType.YTWR (6): Yield to Worst Refunding
            - YieldType.EYTW (7): Euro Yield to Worst
            - YieldType.EYTWR (8): Euro Yield to Worst Refunding
            - YieldType.YTAL (9): Yield to Average Life
        spread: Input spread value (for reverse calculation from spread to price/yield).
        yield_: Input yield value (for reverse calculation from yield to price).
        price: Input price value (for reverse calculation from price to yield/spread).
        benchmark: Benchmark security for spread calculation (e.g., "T 4.5 05/15/38 Govt").
        **kwargs: Request controls, including additional overrides, per-security
            overrides, field types, validation, format, and output backend.

    Returns:
        Native reference-data result in the configured backend. The default is
        canonical long format; ``format`` selects supported native layouts.

    Example::

        import asyncio
        from xbbg.ext.fixed_income import ayas, YieldType


        async def main():
            # Get yield to maturity for a bond
            df = await ayas("US912810TM69 Govt", "YAS_BOND_YLD")

            # Get multiple YAS fields
            df = await ayas(
                "US912810TM69 Govt",
                ["YAS_BOND_YLD", "YAS_MOD_DUR", "YAS_ZSPREAD"],
            )

            # Calculate price from yield
            df = await ayas(
                "US912810TM69 Govt",
                "YAS_BOND_PX",
                yield_=4.5,
                yield_type=YieldType.YTM,
            )


        asyncio.run(main())
    """
    return await _call_native_recipe(
        "recipe_yas",
        [tickers] if isinstance(tickers, str) else list(tickers),
        [flds] if isinstance(flds, str) else list(flds),
        settle_dt=_fmt_date(settle_dt),
        yield_type=int(yield_type) if yield_type is not None else None,
        spread=spread,
        yield_val=yield_,
        price=price,
        benchmark=benchmark,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


async def apreferreds(
    equity_ticker: str,
    *,
    fields: list[str] | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async find preferred stocks for a company using BQL.

    Uses Bloomberg's debt filter to find preferred stock issues
    associated with a given equity ticker.

    Args:
        equity_ticker: Company equity ticker (e.g., "BAC US Equity" or "BAC").
            If no suffix is provided, " US Equity" will be appended.
        fields: Optional list of additional fields to retrieve.
            Default fields are: id, name.
        **kwargs: Native BQL request controls and output backend.

    Returns:
        DataFrame with preferred stock information (type depends on configured backend).
        Columns include the security ID, name, and any additional requested fields.

    Example::

        import asyncio
        from xbbg.ext.fixed_income import apreferreds


        async def main():
            # Get preferred stocks for Bank of America
            df = await apreferreds("BAC US Equity")

            # Get preferreds with additional fields
            df = await apreferreds("BAC", fields=["px_last", "dvd_yld"])


        asyncio.run(main())
    """
    return await _call_native_recipe(
        "recipe_preferreds",
        equity_ticker,
        fields=fields,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


async def acorporate_bonds(
    ticker: str,
    *,
    ccy: str | None = "USD",
    fields: list[str] | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async find corporate bonds for a company using BQL.

    Uses Bloomberg's debt() universe to find corporate bond issues
    for a given company via its equity ticker. Works across all markets.

    Args:
        ticker: Company equity ticker (e.g., "AAPL", "9984 JT Equity").
            If no suffix is provided, " US Equity" is appended.
        ccy: Currency filter (default: "USD"). Set to None for all currencies.
        fields: Optional list of additional fields to retrieve.
            Default field is: id.
        **kwargs: Native BQL request controls and output backend.

    Returns:
        DataFrame with corporate bond information (type depends on configured backend).
        Columns include the security ID and any additional requested fields.

    Example::

        import asyncio
        from xbbg.ext.fixed_income import acorporate_bonds


        async def main():
            # Get USD corporate bonds for Apple
            df = await acorporate_bonds("AAPL")

            # Get all currency bonds with additional fields
            df = await acorporate_bonds("MSFT", ccy=None, fields=["name", "cpn", "maturity"])


        asyncio.run(main())
    """
    return await _call_native_recipe(
        "recipe_corporate_bonds",
        ticker,
        ccy=ccy,
        fields=fields,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


async def abqr(
    ticker: str,
    *,
    start_datetime: DateLike = None,
    end_datetime: DateLike = None,
    event_types: list[str] | None = None,
    include_broker_codes: bool = True,
    **kwargs,
) -> IntoDataFrame:
    """Async Bloomberg Quote Request (dealer quotes).

    Retrieves intraday tick data with broker/dealer codes for a security.
    This is useful for analyzing dealer activity and market making.

    Note: For broker attribution, prefer fixed-income ISIN inputs with a dealer
    quote source, e.g. ``/isin/US037833FB15@MSG1 Corp``.

    Args:
        ticker: Security ticker (e.g., "US912810TM69 Govt").
        start_datetime: Start datetime (ISO format, date, or datetime object).
            Default: one hour before ``end_datetime``.
        end_datetime: End datetime. Default: the current UTC instant.
        event_types: List of event types to retrieve (default: ["BID", "ASK"]).
            Options: "TRADE", "BID", "ASK", "BID_BEST", "ASK_BEST", etc.
        include_broker_codes: Request and require broker attribution on nonempty
            results (default: True).
        **kwargs: Native tick request controls, including extra include flags,
            ``request_tz`` for naive inputs, ``output_tz`` for returned timestamps,
            and output backend. Aware datetime inputs retain their offsets.

    Returns:
        Time-sorted native dealer quote data in the configured backend. Columns
        typically include ticker, time, event_type, price, size, broker_buy, and
        broker_sell. Extra include fields and timezone metadata are preserved.

    Raises:
        RuntimeError: Nonempty quotes have no broker attribution when
            ``include_broker_codes=True``.

    Example::

        import asyncio
        from xbbg.ext.fixed_income import abqr


        async def main():
            # Get dealer quotes for a bond using an ISIN and MSG1 dealer source
            df = await abqr("/isin/US037833FB15@MSG1 Corp")

            # Get quotes for specific time range
            df = await abqr(
                "/isin/US037833FB15@MSG1 Corp",
                start_datetime="2024-01-15 09:00",
                end_datetime="2024-01-15 10:00",
            )


        asyncio.run(main())
    """
    from xbbg._endpoints import _warn_bqr_dealer_input

    if include_broker_codes:
        _warn_bqr_dealer_input(ticker, stacklevel=3)

    return await _call_native_recipe(
        "recipe_bqr",
        ticker,
        start_datetime=_fmt_datetime(start_datetime, default_tz=None),
        end_datetime=_fmt_datetime(end_datetime, default_tz=None),
        event_types=event_types,
        include_broker_codes=include_broker_codes,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


yas = _syncify(ayas)
preferreds = _syncify(apreferreds)
corporate_bonds = _syncify(acorporate_bonds)
bqr = _syncify(abqr)
