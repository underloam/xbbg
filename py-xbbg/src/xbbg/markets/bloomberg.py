"""Bloomberg exchange metadata query module."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import importlib
import logging
import re
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

import narwhals.stable.v1 as nw

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def _require_pandas(feature: str) -> Any:
    from xbbg.backend import Backend, _import_backend_module

    return _import_backend_module(Backend.PANDAS, feature=feature)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        return True
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


EXCHANGE_FIELDS = [
    "IANA_TIME_ZONE",
    "TIME_ZONE_NUM",
    "ID_MIC_PRIM_EXCH",
    "EXCH_CODE",
    "COUNTRY_ISO",
    "TRADING_DAY_START_TIME_EOD",
    "TRADING_DAY_END_TIME_EOD",
    "FUT_TRADING_HRS",
]

# Kept for backward compatibility; timezone inference now comes from Rust.
COUNTRY_TIMEZONE_MAP: dict[str, str] = {}


@dataclass
class ExchangeInfo:
    """Exchange metadata from Bloomberg."""

    ticker: str
    mic: str | None = None
    exch_code: str | None = None
    timezone: str = "UTC"
    utc_offset: float | None = None
    sessions: dict[str, tuple[str, str]] = field(default_factory=dict)
    source: str = "fallback"
    cached_at: datetime | None = None


def _parse_hhmm(time_str: str | None) -> str | None:
    """Parse Bloomberg time format (HHMM or HH:MM) to HH:MM."""
    if not time_str or _is_missing(time_str):
        return None

    time_str = str(time_str).strip()
    if not time_str:
        return None

    if ":" in time_str:
        parts = time_str.split(":")
        if len(parts) >= 2:
            try:
                hh = int(parts[0])
                mm = int(parts[1])
                return f"{hh:02d}:{mm:02d}"
            except ValueError:
                return None

    if time_str.isdigit() and len(time_str) == 4:
        try:
            hh = int(time_str[:2])
            mm = int(time_str[2:])
            return f"{hh:02d}:{mm:02d}"
        except ValueError:
            return None

    return None


def _parse_futures_hours(fut_hours: str | None) -> dict[str, tuple[str, str]]:
    """Parse Bloomberg FUT_TRADING_HRS to a session dict."""
    if not fut_hours or _is_missing(fut_hours):
        return {}

    fut_hours = str(fut_hours).strip()
    if not fut_hours:
        return {}

    pattern = r"(\d{1,2}:\d{2}|\d{4})\s*-\s*(\d{1,2}:\d{2}|\d{4})"
    match = re.match(pattern, fut_hours)
    if not match:
        return {}

    start = _parse_hhmm(match.group(1))
    end = _parse_hhmm(match.group(2))
    if start and end:
        return {"futures": (start, end)}
    return {}


def _convert_est_to_local(time_str: str, local_tz: str) -> str:
    """Convert a time string from EST to local timezone."""
    if local_tz in ("America/New_York", "US/Eastern", "EST", "EDT"):
        return time_str

    try:
        hour_text, minute_text = time_str.split(":", 1)
        ref_date = datetime.now().date()
        est_ts = datetime(
            ref_date.year,
            ref_date.month,
            ref_date.day,
            int(hour_text),
            int(minute_text),
            tzinfo=ZoneInfo("America/New_York"),
        )
        local_ts = est_ts.astimezone(ZoneInfo(local_tz))
        return f"{local_ts.hour:02d}:{local_ts.minute:02d}"
    except Exception as e:
        logger.debug("Failed to convert time %s from EST to %s: %s", time_str, local_tz, e)
        return time_str


def _parse_trading_hours(
    start_time: str | None,
    end_time: str | None,
    fut_hours: str | None,
    local_tz: str = "America/New_York",
) -> dict[str, tuple[str, str]]:
    """Parse Bloomberg trading hours fields into sessions dict."""
    sessions: dict[str, tuple[str, str]] = {}

    start = _parse_hhmm(start_time)
    end = _parse_hhmm(end_time)
    if start and end:
        local_start = _convert_est_to_local(start, local_tz)
        local_end = _convert_est_to_local(end, local_tz)
        sessions["regular"] = (local_start, local_end)

    sessions.update(_parse_futures_hours(fut_hours))
    return sessions


def _infer_timezone_from_country(country_iso: str | None) -> str | None:
    """Infer IANA timezone from country ISO using Rust fallback map."""
    if not country_iso or _is_missing(country_iso):
        return None
    core = importlib.import_module("xbbg._core")
    infer = cast("Any", getattr(core, "ext_infer_timezone", None))
    if infer is None:
        return None
    return cast("str | None", infer(str(country_iso).strip().upper()))


def _extract_value(df: pd.DataFrame, field: str) -> str | float | None:
    """Extract a single value from Bloomberg response DataFrame."""
    if df.empty:
        return None

    col_map = {c.lower(): c for c in df.columns}
    actual_col = col_map.get(field.lower())
    if actual_col is None:
        return None

    val = df.iloc[0][actual_col]
    if _is_missing(val):
        return None
    return val


def _build_exchange_info_from_response(ticker: str, df: pd.DataFrame) -> ExchangeInfo:
    """Build ExchangeInfo from Bloomberg response DataFrame."""
    if df.empty:
        logger.warning("Empty response from Bloomberg for ticker %s, using fallback", ticker)
        return ExchangeInfo(ticker=ticker, source="fallback")

    iana_tz = _extract_value(df, "IANA_TIME_ZONE")
    tz_num = _extract_value(df, "TIME_ZONE_NUM")
    mic = _extract_value(df, "ID_MIC_PRIM_EXCH")
    exch_code = _extract_value(df, "EXCH_CODE")
    country_iso = _extract_value(df, "COUNTRY_ISO")
    start_time = _extract_value(df, "TRADING_DAY_START_TIME_EOD")
    end_time = _extract_value(df, "TRADING_DAY_END_TIME_EOD")
    fut_hours = _extract_value(df, "FUT_TRADING_HRS")

    timezone = "UTC"
    source = "bloomberg"
    if iana_tz and isinstance(iana_tz, str):
        timezone = iana_tz
        source = "bloomberg"
    elif country_iso:
        inferred_tz = _infer_timezone_from_country(str(country_iso))
        if inferred_tz:
            timezone = inferred_tz
            source = "inferred"
        else:
            source = "fallback"
    else:
        source = "fallback"

    sessions = _parse_trading_hours(
        str(start_time) if start_time else None,
        str(end_time) if end_time else None,
        str(fut_hours) if fut_hours else None,
        local_tz=timezone,
    )

    return ExchangeInfo(
        ticker=ticker,
        mic=str(mic) if mic else None,
        exch_code=str(exch_code) if exch_code else None,
        timezone=timezone,
        utc_offset=float(tz_num) if tz_num is not None else None,
        sessions=sessions,
        source=source,
        cached_at=None,
    )


def _to_pandas_wide(data: Any) -> pd.DataFrame:
    """Convert current backend response into a wide pandas DataFrame."""
    _require_pandas("xbbg.markets Bloomberg metadata helpers")
    utils = importlib.import_module("xbbg.ext._utils")
    pivot_fn = cast("Any", utils._pivot_bdp_to_wide)
    nw_df = nw.from_native(data)
    nw_df = pivot_fn(nw_df)
    return nw_df.to_pandas()


async def afetch_exchange_info(ticker: str, **kwargs) -> ExchangeInfo:
    """Async fetch exchange metadata from Bloomberg."""
    _require_pandas("xbbg.markets.fetch_exchange_info()")
    from xbbg.blp import abdp

    try:
        data = await abdp(tickers=ticker, flds=EXCHANGE_FIELDS, **kwargs)
        return _build_exchange_info_from_response(ticker, _to_pandas_wide(data))
    except Exception as e:
        logger.warning("Failed to fetch exchange info from Bloomberg for %s: %s", ticker, e)
        return ExchangeInfo(ticker=ticker, source="fallback")


def fetch_exchange_info(ticker: str, **kwargs) -> ExchangeInfo:
    """Sync fetch exchange metadata from Bloomberg."""
    from xbbg.blp import _run_sync

    return _run_sync("fetch_exchange_info", afetch_exchange_info, (ticker,), kwargs)
