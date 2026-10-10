"""Futures and CDX resolver extension functions.

Functions for resolving generic futures/CDX tickers to specific contracts.
Delegates Bloomberg requests, validation, and contract selection to Rust recipes.

Sync functions (wrap async with asyncio.run):
    - fut_ticker(): Resolve generic futures ticker to specific contract
    - active_futures(): Get most active futures contract for a date
    - futures_curve(): Build futures chain table with metadata and carry
    - cdx_ticker(): Resolve generic CDX ticker to specific series
    - active_cdx(): Get most active CDX contract for a date

Async functions (primary implementation):
    - afut_ticker(): Async resolve generic futures ticker
    - aactive_futures(): Async get most active futures contract
    - afutures_curve(): Async futures chain table
    - acdx_ticker(): Async resolve generic CDX ticker
    - aactive_cdx(): Async get most active CDX contract
"""

from __future__ import annotations

from datetime import datetime

import narwhals.stable.v1 as nw

from xbbg._core import ext_parse_date
from xbbg._dates import DateLike, _fmt_date, _normalize_to_datetime
from xbbg.ext._utils import _call_native_recipe, _syncify


def _parse_date(dt: DateLike) -> datetime:
    """Parse a date-like value (str / date / datetime / pd.Timestamp) to datetime."""
    if isinstance(dt, str):
        year, month, day = ext_parse_date(dt)
        return datetime(year, month, day)
    if dt is None:
        raise ValueError("Cannot parse date: None")
    return _normalize_to_datetime(dt)


async def afut_ticker(
    gen_ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async resolve generic futures ticker to specific contract.

    Rust resolves generated contract candidates by ``LAST_TRADEABLE_DT`` and
    falls back to Bloomberg's historical ``FUT_CHAIN_LAST_TRADE_DATES`` chain
    with ``CHAIN_DATE`` when the candidate window is too short. Contracts
    expiring on or before the reference date are excluded.

    Args:
        gen_ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
        dt: Reference date for contract resolution.
        **kwargs: Bloomberg request options forwarded to every native request.
            ``freq="Q"`` or ``"QE"`` selects quarterly candidates; the default
            is monthly. ``backend`` controls intermediate result conversion;
            the public result is always a string. Internal data uses long
            format regardless of the requested output ``format``.

    Returns:
        Specific contract ticker (e.g., 'ESH24 Index').

    Raises:
        ValueError: The generic ticker or reference date is invalid.
        RuntimeError: Bloomberg cannot supply the requested contract.

    Example::

        import asyncio
        from xbbg.ext.futures import afut_ticker


        async def main():
            # Get March 2024 E-mini S&P contract
            ticker = await afut_ticker("ES1 Index", "2024-01-15")
            # Returns: 'ESH24 Index'


        asyncio.run(main())
    """
    recipe = "recipe_fut_ticker"
    table = await _call_native_recipe(
        recipe,
        gen_ticker,
        _fmt_date(_parse_date(dt)),
        kwargs.pop("freq", None),
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )
    return _ticker_result(recipe, table)


async def aactive_futures(
    ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async get the most active futures contract for a date.

    Uses Bloomberg's latest ``FUT_CUR_GEN_TICKER`` mapping when available.
    Otherwise Rust resolves the front two contracts, retaining the front before
    its maturity month and comparing their latest non-null volumes over the
    preceding 10 calendar days during the roll month. Ties keep the front.

    Args:
        ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
            Must be a generic contract (e.g., 'ES1'), not specific (e.g., 'ESH24').
        dt: Reference date.
        **kwargs: Bloomberg request options forwarded to every native request.
            ``freq="Q"`` or ``"QE"`` selects quarterly candidates; the default
            is monthly. ``backend`` controls intermediate result conversion;
            the public result is always a string. Internal data uses long
            format regardless of the requested output ``format``.

    Returns:
        Bloomberg's mapped contract, or the contract selected by recent volume.

    Raises:
        ValueError: The ticker or date is invalid, including specific contracts.
        RuntimeError: Bloomberg cannot resolve a front contract.

    Example::

        import asyncio
        from xbbg.ext.futures import aactive_futures


        async def main():
            # Get most active E-mini S&P contract
            ticker = await aactive_futures("ES1 Index", "2024-01-15")


        asyncio.run(main())
    """
    recipe = "recipe_active_futures"
    table = await _call_native_recipe(
        recipe,
        ticker,
        _fmt_date(_parse_date(dt)),
        kwargs.pop("freq", None),
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )
    return _ticker_result(recipe, table)


def _ticker_result(recipe: str, table) -> str:
    """Unwrap a native recipe's single ticker across supported result backends."""
    if hasattr(table, "to_pylist"):
        rows = table.to_pylist()
    else:
        frame = nw.from_native(table).lazy().collect()
        rows = list(frame.iter_rows(named=True))
    if len(rows) != 1:
        raise ValueError(f"{recipe} returned {len(rows)} rows, expected exactly 1")
    ticker = rows[0].get("ticker")
    if not ticker:
        raise ValueError(f"{recipe} returned a row without a ticker")
    return str(ticker)


async def _resolve_cdx_recipe(recipe: str, *args) -> str:
    """Run a native CDX recipe and unwrap its single-ticker result."""
    table = await _call_native_recipe(recipe, *args, backend="native")
    return _ticker_result(recipe, table)


async def acdx_ticker(
    gen_ticker: str,
    dt: DateLike,
    versionless: bool = False,
) -> str:
    """Async resolve a generic CDX ticker to the series that applies on a date.

    The answer is the highest series whose Bloomberg
    ``CDS_FIRST_ACCRUAL_START_DATE`` falls on or before ``dt``, so it can never
    move backwards as ``dt`` advances. Roll dates are read from Bloomberg rather
    than assumed from the semi-annual cadence, because they are business-day
    adjusted: CDX.NA.IG.45 first accrues 2025-09-22, so 2025-09-21 still
    resolves to S44.

    The ``V{n}`` token is the latest version Bloomberg reports for the resolved
    series. Bloomberg publishes no as-of version, and superseded version
    tickers carry no price history, so an older ``V{n}`` would name a security
    that cannot be priced.

    Args:
        gen_ticker: Generic CDX ticker (e.g., 'CDX IG CDSI GEN 5Y Corp').
        dt: Reference date.
        versionless: Drop the ``V{n}`` token from the returned ticker.

    Returns:
        Specific series ticker (e.g., ``CDX IG CDSI S34 V1 5Y Corp``).

    Raises:
        ValueError: ``gen_ticker`` is not generic, or ``dt`` precedes the first
            series of the index.
        RuntimeError: Bloomberg did not report the series metadata the ladder
            needs, or reported an inconsistent series ladder.

    Example::

        import asyncio
        from xbbg.ext.futures import acdx_ticker


        async def main():
            # 'CDX IG CDSI S34 V1 5Y Corp' -- the series on the run in mid-2020
            return await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "2020-06-01")


        asyncio.run(main())
    """
    return await _resolve_cdx_recipe(
        "recipe_cdx_ticker",
        gen_ticker,
        _fmt_date(_parse_date(dt)),
        versionless,
    )


async def aactive_cdx(
    gen_ticker: str,
    dt: DateLike,
    lookback_days: int = 10,
    versionless: bool = False,
) -> str:
    """Async resolve the latest CDX series that had started and traded by a date.

    Matches :func:`acdx_ticker` except between a roll and the new series' first
    print, when the preceding series is still the traded one -- CDX.NA.HY.46
    started 2026-03-20 but first printed 2026-03-27, so those five business days
    resolve to S45.

    The activity window always reaches back to the resolved series' first
    accrual date, so "this series has traded" can only ever flip false to true
    and the result never moves backwards as ``dt`` advances.

    Args:
        gen_ticker: Generic CDX ticker (e.g., 'CDX HY CDSI GEN 5Y Corp').
        dt: Reference date.
        lookback_days: Minimum activity window, in days, before ``dt``.
        versionless: Drop the ``V{n}`` token from the returned ticker.

    Returns:
        Specific series ticker (e.g., ``CDX HY CDSI S45 V3 5Y Corp``).

    Raises:
        ValueError: ``gen_ticker`` is not generic, or ``dt`` precedes the first
            series of the index.
        RuntimeError: Neither the resolved series nor its predecessor reported
            ``PX_LAST`` in the window.
    """
    return await _resolve_cdx_recipe(
        "recipe_active_cdx",
        gen_ticker,
        _fmt_date(_parse_date(dt)),
        lookback_days,
        versionless,
    )


async def afutures_curve(
    gen_ticker: str,
    *,
    asof: DateLike = None,
    chain_field: str | None = None,
    fields: list[str] | None = None,
    max_contracts: int | None = None,
    backend=None,
    **_kwargs,
):
    """Async futures chain table with contract metadata, mid, and annualized carry."""
    asof_fmt = _fmt_date(asof) if asof is not None else None
    return await _call_native_recipe(
        "recipe_futures_curve",
        gen_ticker,
        asof_fmt,
        chain_field,
        list(fields) if fields is not None else None,
        max_contracts,
        backend=backend,
    )


fut_ticker = _syncify(afut_ticker)
active_futures = _syncify(aactive_futures)
cdx_ticker = _syncify(acdx_ticker)
active_cdx = _syncify(aactive_cdx)
futures_curve = _syncify(afutures_curve)
