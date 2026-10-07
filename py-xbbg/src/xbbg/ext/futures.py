"""Futures and CDX resolver extension functions.

Functions for resolving generic futures/CDX tickers to specific contracts.
Uses high-performance Rust utilities from xbbg._core for parsing and resolution.

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

import contextlib
from datetime import date, datetime, timedelta
import logging
import re

import narwhals.stable.v1 as nw

# Import Rust date parser (shared with other ext modules)
from xbbg._core import ext_get_futures_months, ext_parse_date
from xbbg._dates import DateLike, _fmt_date, _normalize_to_datetime
from xbbg.ext._utils import (
    _call_native_recipe,
    _canonical_column_name,
    _syncify,
)

logger = logging.getLogger(__name__)


def _parse_date(dt: DateLike) -> datetime:
    """Parse a date-like value (str / date / datetime / pd.Timestamp) to datetime."""
    if isinstance(dt, str):
        year, month, day = ext_parse_date(dt)
        return datetime(year, month, day)
    if dt is None:
        raise ValueError("Cannot parse date: None")
    return _normalize_to_datetime(dt)


_FUTURES_MONTH_CODES = "".join(ext_get_futures_months().values())


def _parse_generic_ticker(gen_ticker: str) -> tuple[str, int, str]:
    """Parse generic futures ticker into ``(root, n, asset_type)``."""
    parts = gen_ticker.split()
    if len(parts) < 2:
        raise ValueError(f"Unknown asset type for generic ticker: {gen_ticker}")

    asset = parts[-1]

    if asset in ["Index", "Curncy", "Comdty"]:
        ticker = " ".join(parts[:-1])
        root = ticker[:-1]
        n = int(ticker[-1])
        return root, n, asset

    if asset == "Equity":
        ticker = parts[0]
        root = ticker[:-1]
        n = int(ticker[-1])
        return root, n, " ".join(parts[1:])

    raise ValueError(f"Unknown asset type for generic ticker: {gen_ticker}")


def _find_col(columns: list[str], candidates: list[str]) -> str | None:
    """Find a column by raw label or wrapper-internal canonical alias."""
    by_exact_lower = {col.casefold(): col for col in columns}
    by_canonical = {_canonical_column_name(col): col for col in columns}

    for candidate in candidates:
        match = by_exact_lower.get(candidate.casefold())
        if match is not None:
            return match

        match = by_canonical.get(_canonical_column_name(candidate))
        if match is not None:
            return match

    return None


def _coerce_datetime(value) -> datetime | None:
    """Convert Bloomberg date-like values to ``datetime``."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    text = str(value).strip()
    if not text:
        return None

    candidates = [text]
    if "T" in text:
        candidates.append(text.split("T", 1)[0])
    if " " in text:
        candidates.append(text.split(" ", 1)[0])

    for candidate in candidates:
        try:
            return _parse_date(candidate)
        except (ValueError, TypeError):
            continue

    return None


async def _resolve_chain(gen_ticker: str, dt: datetime, **kwargs) -> list[tuple[str, datetime]]:
    """Resolve futures chain via ``FUT_CHAIN_LAST_TRADE_DATES`` at ``CHAIN_DATE``."""
    from xbbg import abds

    chain_date = dt.strftime("%Y%m%d")
    overrides = {"CHAIN_DATE": chain_date}

    try:
        chain = await abds(
            tickers=gen_ticker,
            flds="FUT_CHAIN_LAST_TRADE_DATES",
            overrides=overrides,
            **kwargs,
        )
    except (ValueError, TypeError, KeyError):
        logger.warning("Failed to get futures chain for %s", gen_ticker)
        return []

    nw_chain = nw.from_native(chain)
    if len(nw_chain) == 0:
        logger.warning("Empty futures chain for %s at %s", gen_ticker, chain_date)
        return []

    ticker_col = _find_col(
        list(nw_chain.columns),
        ["future's_ticker", "futures_ticker", "security_description", "ticker"],
    )
    date_col = _find_col(
        list(nw_chain.columns),
        ["last_trade_date", "last_tradeable_dt", "date"],
    )

    if ticker_col is None or date_col is None:
        logger.warning("Unexpected FUT_CHAIN_LAST_TRADE_DATES columns: %s", list(nw_chain.columns))
        return []

    contracts: list[tuple[str, datetime]] = []
    for row in nw_chain.iter_rows(named=True):
        ticker = row.get(ticker_col)
        expiry_raw = row.get(date_col)
        expiry = _coerce_datetime(expiry_raw)
        if ticker is None or expiry is None:
            continue
        if expiry > dt:
            contracts.append((str(ticker).strip(), expiry))

    contracts.sort(key=lambda item: item[1])
    return contracts


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def afut_ticker(
    gen_ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async resolve generic futures ticker to specific contract.

    Maps a generic futures ticker (e.g., 'ES1 Index') to the specific
    contract for a given date using Bloomberg's futures chain bulk field
    (``FUT_CHAIN_LAST_TRADE_DATES``) with ``CHAIN_DATE``.

    Args:
        gen_ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
        dt: Reference date for contract resolution.
        **kwargs: Additional arguments passed to abds.

    Returns:
        Specific contract ticker (e.g., 'ESH24 Index').

    Example::

        import asyncio
        from xbbg.ext.futures import afut_ticker


        async def main():
            # Get March 2024 E-mini S&P contract
            ticker = await afut_ticker("ES1 Index", "2024-01-15")
            # Returns: 'ESH24 Index'


        asyncio.run(main())
    """
    dt_parsed = _parse_date(dt)

    try:
        _root, n, _asset_type = _parse_generic_ticker(gen_ticker)
    except ValueError as exc:
        logger.error(str(exc))
        return ""

    contracts = await _resolve_chain(gen_ticker, dt_parsed, **kwargs)

    if len(contracts) < n:
        logger.warning(
            "Not enough contracts expiring after %s for %s (need %d, found %d)",
            dt_parsed.date(),
            gen_ticker,
            n,
            len(contracts),
        )
        return ""

    result = contracts[n - 1][0]
    logger.debug("Resolved %s @ %s -> %s", gen_ticker, dt_parsed.date(), result)
    return result


async def aactive_futures(
    ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async get the most active futures contract for a date.

    Selects the most active contract based on volume, typically choosing
    between the front month and second month contract.

    Args:
        ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
            Must be a generic contract (e.g., 'ES1'), not specific (e.g., 'ESH24').
        dt: Reference date.
        **kwargs: Additional arguments passed to abdp/abdh.

    Returns:
        Most active contract ticker based on recent volume.

    Raises:
        ValueError: If ticker appears to be a specific contract instead of generic.

    Example::

        import asyncio
        from xbbg.ext.futures import aactive_futures


        async def main():
            # Get most active E-mini S&P contract
            ticker = await aactive_futures("ES1 Index", "2024-01-15")


        asyncio.run(main())
    """
    from xbbg import abdh

    dt_parsed = _parse_date(dt)

    # Reject specific contracts (e.g., UXZ24 Index)
    ticker_base = ticker.rsplit(" ", 1)[0]
    month_code_pattern = rf"[{re.escape(_FUTURES_MONTH_CODES)}]"
    match = re.search(rf"(.+)({month_code_pattern})(\d{{1,2}})$", ticker_base)
    if match:
        _prefix, _month_char, digits = match.groups()
        if len(digits) == 2:
            msg = (
                f"'{ticker}' appears to be a specific contract "
                f"(ends with month code + 2-digit year), not a generic one. "
                f"Use a generic ticker like 'UX1 Index' instead of 'UXZ24 Index'."
            )
            raise ValueError(msg)
        if len(digits) == 1 and len(ticker_base) > 3:
            msg = (
                f"'{ticker}' appears to be a specific contract, "
                f"not a generic one. Use a generic ticker like "
                f"'UX1 Index' instead of 'UXZ5 Index'."
            )
            raise ValueError(msg)

    # Parse ticker components
    t_info = ticker.split()
    prefix, asset = " ".join(t_info[:-1]), t_info[-1]

    gen_1 = f"{prefix[:-1]}1 {asset}"
    contracts = await _resolve_chain(gen_1, dt_parsed, **kwargs)

    if not contracts:
        logger.error("Failed to resolve chain for %s", gen_1)
        return ""

    fut_1, fut_1_expiry = contracts[0]

    if len(contracts) < 2:
        return fut_1

    fut_2 = contracts[1][0]

    # If date is well before first expiry, keep front month
    if dt_parsed.month < fut_1_expiry.month and dt_parsed.year == fut_1_expiry.year:
        return fut_1

    # Compare latest volume over recent window
    start_date = dt_parsed - timedelta(days=15)
    volume = await abdh(
        tickers=[fut_1, fut_2],
        flds="volume",
        start_date=start_date,
        end_date=dt_parsed,
        **kwargs,
    )
    nw_vol = nw.from_native(volume)

    if len(nw_vol) == 0:
        return fut_1

    latest_volumes: dict[str, float] = {}

    # LONG format
    if "field" in nw_vol.columns and "value" in nw_vol.columns:
        vol_rows = nw_vol.filter(nw.col("field").str.to_lowercase() == "volume")
        if "date" in vol_rows.columns:
            vol_rows = vol_rows.sort("date", descending=True)

        for tk in [fut_1, fut_2]:
            tk_rows = vol_rows.filter(nw.col("ticker") == tk)
            if len(tk_rows) > 0:
                with contextlib.suppress(ValueError, TypeError):
                    latest_volumes[tk] = float(tk_rows["value"][0])

    # Wide fallback
    else:
        vol_col = "volume" if "volume" in nw_vol.columns else "VOLUME" if "VOLUME" in nw_vol.columns else None
        if vol_col is not None and "date" in nw_vol.columns:
            for tk in [fut_1, fut_2]:
                tk_rows = nw_vol.filter(nw.col("ticker") == tk).sort("date", descending=True)
                if len(tk_rows) > 0:
                    with contextlib.suppress(ValueError, TypeError):
                        latest_volumes[tk] = float(tk_rows[vol_col][0])

    if not latest_volumes:
        return fut_1

    return max(latest_volumes, key=lambda key: latest_volumes.get(key, 0.0))


async def _resolve_cdx_recipe(recipe: str, *args) -> str:
    """Run a native CDX recipe and unwrap its single-ticker result."""
    table = await _call_native_recipe(recipe, *args, backend="native")
    rows = table.to_pylist()
    if len(rows) != 1:
        raise ValueError(f"{recipe} returned {len(rows)} rows, expected exactly 1")
    ticker = rows[0].get("ticker")
    if not ticker:
        raise ValueError(f"{recipe} returned a row without a ticker")
    return str(ticker)


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
