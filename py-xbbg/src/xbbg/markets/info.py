"""Python presentation adapters for native market metadata and timing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from xbbg import _core, _engine, _sync
from xbbg.markets.bloomberg import fetch_exchange_info

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "exch_info",
    "exch_info_bloomberg",
    "market_info",
    "market_timing",
    "ccy_pair",
    "convert_session_times_to_utc",
    "CurrencyPair",
]


def _require_pandas(feature: str) -> Any:
    from xbbg.backend import Backend, _import_backend_module

    return _import_backend_module(Backend.PANDAS, feature=feature)


@dataclass(frozen=True)
class CurrencyPair:
    """FX conversion metadata."""

    ticker: str
    factor: float
    power: float


def exch_info_bloomberg(ticker: str, **kwargs) -> pd.Series:
    """Present resolved exchange metadata as a Series of session lists.

    Despite the historical name, resolution includes native overrides and the
    exchange cache before Bloomberg. ``ref=`` selects another ticker and
    ``engine=`` selects an explicit engine. Native fallback metadata remains an
    empty Series; engine startup errors propagate instead of being hidden.
    """
    pd = _require_pandas("xbbg.markets.exch_info_bloomberg()")
    ticker = kwargs.pop("ref", None) or ticker
    kwargs.pop("original", None)
    info = fetch_exchange_info(ticker, **kwargs)
    if info.source == "fallback":
        return pd.Series(dtype=object)

    values: dict[str, object] = {"tz": info.timezone}
    values.update({name: list(window) for name, window in info.sessions.items()})
    return pd.Series(values, name=info.mic or info.exch_code or "Bloomberg")


def exch_info(ticker: str, **kwargs) -> pd.Series:
    """Resolve exchange information; see :func:`exch_info_bloomberg`."""
    _require_pandas("xbbg.markets.exch_info()")
    return exch_info_bloomberg(ticker, **kwargs)


async def _afetch_market_info(ticker: str) -> dict[str, Any]:
    return await _engine._get_engine().fetch_market_info(ticker)


def market_info(ticker: str) -> pd.Series:
    """Present native market metadata as a Series, omitting absent fields.

    Rust decides which securities need futures-cycle metadata and can mark
    ``is_fut`` true even when ``freq`` is unavailable. All tickers, including
    CDX, use the native query rather than Python asset filters or hard-coded
    exchange metadata. Query errors propagate instead of returning empty data.
    """
    pd = _require_pandas("xbbg.markets.market_info()")
    values = _sync._run_sync("market_info", _afetch_market_info, (ticker,), {})
    return pd.Series({name: value for name, value in values.items() if value is not None})


def ccy_pair(local: str, base: str = "USD") -> CurrencyPair:
    """Currency pair info using Rust FX helpers."""
    if _core.ext_same_currency(base, local):
        factor = 1.0
        if base and base[-1].islower():
            factor /= 100.0
        if local and local[-1].islower():
            factor *= 100.0
        return CurrencyPair(ticker="", factor=factor, power=1.0)

    fx_pair, factor, _from_ccy, _to_ccy = _core.ext_build_fx_pair(local, base)
    return CurrencyPair(ticker=fx_pair, factor=float(factor), power=1.0)


def convert_session_times_to_utc(
    start_time: str,
    end_time: str,
    exchange_tz: str,
    time_fmt: str = "%Y-%m-%dT%H:%M:%S",
) -> tuple[str, str]:
    """Convert dated, timezone-naive session timestamps using native tz rules.

    Each endpoint retains its own input date, including overnight sessions.
    Pandas parses and formats the public timestamps; Rust performs timezone
    conversion and raises ``ValueError`` for ambiguous/nonexistent local times.
    The existing exact ``UTC`` passthrough is retained, including its formatting.
    """
    if exchange_tz == "UTC":
        return start_time, end_time

    pd = _require_pandas("xbbg.markets.convert_session_times_to_utc()")
    start = pd.Timestamp(start_time)
    end = pd.Timestamp(end_time)
    if start.tzinfo is not None or end.tzinfo is not None:
        raise TypeError("session timestamps must be timezone-naive")

    start_clock = start.strftime("%H:%M:%S")
    end_clock = end.strftime("%H:%M:%S")
    if start.date() == end.date():
        start_utc, end_utc = _core.ext_session_times_to_utc(
            start_clock, end_clock, exchange_tz, start.date().isoformat()
        )
    else:
        start_utc, _ = _core.ext_session_times_to_utc(start_clock, start_clock, exchange_tz, start.date().isoformat())
        _, end_utc = _core.ext_session_times_to_utc(end_clock, end_clock, exchange_tz, end.date().isoformat())

    # The native binding has second precision; retain caller-supplied fractions.
    start_result = pd.Timestamp(start_utc, tz="UTC") + pd.Timedelta(
        microseconds=start.microsecond, nanoseconds=start.nanosecond
    )
    end_result = pd.Timestamp(end_utc, tz="UTC") + pd.Timedelta(
        microseconds=end.microsecond, nanoseconds=end.nanosecond
    )
    return start_result.strftime(time_fmt), end_result.strftime(time_fmt)


async def _amarket_timing(ticker, date, timing, tz, **kwargs) -> str:
    ticker = kwargs.pop("ref", None) or ticker
    kwargs.pop("original", None)
    engine = _engine._get_engine(**kwargs)
    if tz is not None:
        tz = str(tz)
        country = {"NY": "US", "LN": "GB", "TK": "JP", "HK": "HK"}.get(tz.upper())
        if country is not None:
            tz = _core.ext_infer_timezone(country)
        elif " " in tz:
            tz = (await engine.resolve_exchange(tz))["timezone"]
    return await engine.market_timing(ticker, date, timing, tz)


def market_timing(ticker, dt, timing="EOD", tz="local", **kwargs) -> str:
    """Resolve BOD/EOD/FINISHED through native exchange and timezone rules.

    ``ref=`` and ``engine=`` select the reference ticker and engine. Target
    timezones accept IANA names, ``local``, NY/LN/TK/HK aliases or another ticker.
    Native timing names are case-insensitive; invalid names and missing day
    sessions raise instead of silently using EOD or returning an empty string.
    FINISHED uses the day close when an override has no ``allday`` window.
    """
    pd = _require_pandas("xbbg.markets.market_timing()")
    date = pd.Timestamp(str(dt)).date().isoformat()
    return _sync._run_sync("market_timing", _amarket_timing, (ticker, date, timing, tz), kwargs)
