"""Fixed income extension functions.

Convenience wrappers for fixed income and bond analysis queries.
Returns DataFrame in the configured backend format.
Uses high-performance Rust utilities from xbbg._core.

Sync functions (wrap async with asyncio.run):
    - yas(): Yield & Spread Analysis
    - preferreds(): Find preferred stocks for a company
    - corporate_bonds(): Find corporate bonds for a company
    - bqr(): Bloomberg Quote Request (dealer quotes)

Async functions (primary implementation):
    - ayas(): Async yield & spread analysis
    - apreferreds(): Async find preferred stocks
    - acorporate_bonds(): Async find corporate bonds
    - abqr(): Async Bloomberg Quote Request
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

# Import Rust ext utilities for max performance
from xbbg._core import (
    ext_build_corporate_bonds_query,
    ext_build_preferreds_query,
    ext_build_yas_overrides,
    ext_normalize_tickers,
)
from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _syncify

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


def _normalize_tickers(tickers: str | list[str]) -> list[str]:
    """Normalize tickers to a list using Rust."""
    # ext_normalize_tickers expects a list, so wrap single string
    if isinstance(tickers, str):
        return ext_normalize_tickers([tickers])
    return ext_normalize_tickers(tickers)


# =============================================================================
# Async implementations (primary)
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

    Convenience wrapper around abdp() for Bloomberg's YAS (Yield & Spread Analysis).
    Maps named parameters to Bloomberg YAS override fields.

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
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with yield analysis data (type depends on configured backend).

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
    from xbbg import abdp

    tickers_list = _normalize_tickers(tickers)

    # Normalize fields
    if isinstance(flds, str):
        fields_list = [flds]
    else:
        fields_list = list(flds)

    # Build overrides using Rust (high performance)
    formatted_dt = _fmt_date(settle_dt) if settle_dt is not None else None
    yt_flag = int(yield_type) if yield_type is not None else None

    yas_pairs = ext_build_yas_overrides(
        settle_dt=formatted_dt,
        yield_type=yt_flag,
        spread=spread,
        yield_val=yield_,
        price=price,
        benchmark=benchmark,
    )
    overrides: dict[str, str] = dict(yas_pairs)

    # Merge with any additional overrides from kwargs
    if "overrides" in kwargs:
        existing = kwargs.pop("overrides")
        if isinstance(existing, dict):
            overrides.update(existing)
        elif isinstance(existing, list):
            for k, v in existing:
                overrides[k] = str(v)

    # Call abdp with the overrides
    return await abdp(tickers=tickers_list, flds=fields_list, overrides=overrides, **kwargs)


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
        **kwargs: Additional options passed to the underlying BQL query.

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
    from xbbg import abql

    # Build BQL query using Rust (handles ticker normalization, field dedup)
    extra = list(fields) if fields else []
    bql_query = ext_build_preferreds_query(equity_ticker, extra)

    return await abql(bql_query, **kwargs)


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
        **kwargs: Additional options passed to the underlying BQL query.

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
    from xbbg import abql

    # Build BQL query using Rust (handles field dedup, filter construction)
    extra = list(fields) if fields else []
    bql_query = ext_build_corporate_bonds_query(ticker, ccy, extra)

    return await abql(bql_query, **kwargs)


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
        start_datetime: Start datetime (ISO format or "YYYY-MM-DD HH:MM").
            Default: 1 hour ago.
        end_datetime: End datetime (ISO format or "YYYY-MM-DD HH:MM").
            Default: now.
        event_types: List of event types to retrieve (default: ["BID", "ASK"]).
            Options: "TRADE", "BID", "ASK", "BID_BEST", "ASK_BEST", etc.
        include_broker_codes: Whether to include broker/dealer codes (default: True).
        **kwargs: Additional options passed to abdtick.

    Returns:
        DataFrame with fixed-income dealer quote data. Columns typically include
        ticker, time, event_type, price, size, broker_buy, and broker_sell.

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
    from xbbg import abdtick
    from xbbg._core import ext_default_bqr_datetimes
    from xbbg._endpoints import _postprocess_bqr_result, _warn_bqr_dealer_input

    backend = kwargs.pop("backend", None)

    # Compute default datetime range using Rust (handles normalization + defaults)
    start_datetime, end_datetime = ext_default_bqr_datetimes(start_datetime, end_datetime)

    # Default event types
    if event_types is None:
        event_types = ["BID", "ASK"]

    # Add broker code request to kwargs if desired
    if include_broker_codes:
        kwargs["includeBrokerCodes"] = True
        _warn_bqr_dealer_input(ticker, stacklevel=3)

    # Use abdtick to get the data. Keep BID/ASK as the default BQR contract;
    # otherwise abdtick defaults to TRADE ticks and returns transaction rows.
    table = await abdtick(
        ticker=ticker,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        event_types=event_types,
        backend="native",
        **kwargs,
    )
    return _postprocess_bqr_result(
        table,
        ticker=ticker,
        backend=backend,
        enforce_broker_codes=include_broker_codes,
    )


yas = _syncify(ayas)
preferreds = _syncify(apreferreds)
corporate_bonds = _syncify(acorporate_bonds)
bqr = _syncify(abqr)
