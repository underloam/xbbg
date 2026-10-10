"""Python models for the native exchange-resolution waterfall."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from xbbg import _engine, _sync

_SESSION_NAMES = ("day", "allday", "pre", "post", "am", "pm")


@dataclass
class ExchangeInfo:
    """Resolved exchange metadata.

    ``sessions`` contains native ``day``, ``allday``, ``pre``, ``post``,
    ``am`` and ``pm`` windows, omitting unavailable sessions. Raw Bloomberg
    ``regular``/``futures`` keys are no longer returned. Times are interpreted
    by the native resolver; Python does not shift them from US Eastern time.
    ``cached_at`` remains available for caller-owned metadata, but is ``None``
    for native results because the binding does not expose cache timestamps.
    """

    ticker: str
    mic: str | None = None
    exch_code: str | None = None
    timezone: str = "UTC"
    utc_offset: float | None = None
    sessions: dict[str, tuple[str, str]] = field(default_factory=dict)
    source: str = "fallback"
    cached_at: datetime | None = None


def _exchange_info_from_native(values: Mapping[str, Any]) -> ExchangeInfo:
    """Nest the binding's flat session fields in the public Python model."""
    return ExchangeInfo(
        ticker=values["ticker"],
        mic=values["mic"],
        exch_code=values["exch_code"],
        timezone=values["timezone"],
        utc_offset=values["utc_offset"],
        sessions={name: values[name] for name in _SESSION_NAMES if values.get(name) is not None},
        source=values["source"],
    )


async def afetch_exchange_info(ticker: str, **kwargs) -> ExchangeInfo:
    """Resolve exchange metadata through the active native engine.

    Runtime overrides take precedence over the native cache and Bloomberg.
    Bloomberg lookup failures produce the native UTC fallback. Engine startup
    errors propagate. ``engine=`` may select an explicit scoped engine; BDP
    request/presentation options are not accepted. Pandas is not required.
    """
    values = await _engine._get_engine(**kwargs).resolve_exchange(ticker)
    return _exchange_info_from_native(values)


def fetch_exchange_info(ticker: str, **kwargs) -> ExchangeInfo:
    """Synchronously resolve metadata; see :func:`afetch_exchange_info`."""
    return _sync._run_sync("fetch_exchange_info", afetch_exchange_info, (ticker,), kwargs)
