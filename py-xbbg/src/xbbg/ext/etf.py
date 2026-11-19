"""ETF NAV / iNAV extension functions backed by native xbbg recipes.

Resolves Bloomberg's authoritative ``ETF_NAV_TICKER`` / ``ETF_INAV_TICKER``
relationship fields instead of guessing suffix conventions. NAV and iNAV are
independently nullable, and relationship values are normalized to exactly one
trailing ``Index`` token.

Sync functions (wrap async with asyncio.run):
    - etf_nav_relationships(): Resolve validated NAV / iNAV Index targets
    - etf_nav_snapshot(): Current NAV / iNAV levels with fund NAV fallback
    - etf_nav_history(): Daily NAV / iNAV history over a date range
    - subscribe_etf_inav(): Subscribe to real-time iNAV updates

Async functions (primary implementation):
    - aetf_nav_relationships(): Async relationship resolution
    - aetf_nav_snapshot(): Async NAV / iNAV snapshot
    - aetf_nav_history(): Async daily NAV / iNAV history
    - asubscribe_etf_inav(): Async iNAV subscription with atomic preflight
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from xbbg._core import ext_plan_etf_inav, ext_validated_inav_tickers
from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame

    from xbbg.backend import Backend
    from xbbg.blp import Subscription


def _normalize_etfs(values: str | Sequence[str]) -> list[str]:
    """Normalize ETF input to a trimmed ordered list, preserving duplicates."""
    if isinstance(values, str):
        return [values.strip()]
    return [str(value).strip() for value in values]


async def aetf_nav_relationships(
    etfs: str | Sequence[str],
    *,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async ETF NAV / iNAV relationship discovery.

    Resolves ``ETF_NAV_TICKER`` / ``ETF_INAV_TICKER`` per input ETF and
    validates each returned target as a genuine Index security. Missing
    relationships stay null without affecting the other leg.
    """
    return await _call_native_recipe(
        "recipe_etf_nav_relationships",
        _normalize_etfs(etfs),
        backend=backend,
    )


async def aetf_nav_snapshot(
    etfs: str | Sequence[str],
    *,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async current NAV / iNAV levels.

    Mapped Index targets are priced with ``PX_LAST``; ETFs without a daily
    NAV relationship fall back to the source fund's ``FUND_NET_ASSET_VAL``.
    """
    return await _call_native_recipe(
        "recipe_etf_nav_snapshot",
        _normalize_etfs(etfs),
        backend=backend,
    )


async def aetf_nav_history(
    etfs: str | Sequence[str],
    *,
    start_date: DateLike,
    end_date: DateLike,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async daily NAV / iNAV history between two dates (inclusive)."""
    start = _fmt_date(start_date)
    end = _fmt_date(end_date)
    if start is None or end is None:
        raise ValueError("start_date and end_date are required")
    return await _call_native_recipe(
        "recipe_etf_nav_history",
        _normalize_etfs(etfs),
        start,
        end,
        backend=backend,
    )


async def asubscribe_etf_inav(
    etfs: str | Sequence[str],
    fields: str | list[str] = "LAST_PRICE",
    *,
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
    options: list[str] | None = None,
    conflate: bool = False,
    tick_mode: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
    delivery: str = "deltas",
    output: str | None = None,
) -> Subscription:
    """Subscribe to real-time iNAV updates for ETFs after atomic preflight.

    Resolves every source ETF's validated iNAV Index target first and only
    then opens one stream over the resolved iNAV tickers. Any missing,
    invalid, or ambiguous relationship fails before a subscription is
    opened. Stream topics are the normalized iNAV tickers, so dynamic
    ``add``/``remove`` on the returned handle expect already-resolved iNAV
    tickers, not source ETFs.

    Args:
        overflow_policy: ``"drop_newest"`` (default), ``"block"``, or gap-aware
            ``"drop_oldest"``. Consume the latter through ``sub.deliveries()``.
        delivery: ``"deltas"`` (default) or conflated ``"state"``. State is read
            through ``sub.deliveries()`` and includes each row's ``valid`` flag.
        output: Output selector: ``"record_batch"``, ``"backend"``, ``"dict"``,
            or ``"tick"`` (case-insensitive). ``None`` (default) retains the
            behavior selected by ``raw`` and ``tick_mode``.
    """
    # Trimming, emptiness and duplicate checks run natively on the raw inputs.
    raw_etfs = [etfs] if isinstance(etfs, str) else [str(value) for value in etfs]
    etf_list = ext_plan_etf_inav(raw_etfs)

    table = await _call_native_recipe(
        "recipe_etf_nav_relationships",
        etf_list,
        backend="native",
    )
    inav_tickers = ext_validated_inav_tickers(etf_list, table)

    from xbbg import blp

    return await blp.asubscribe(
        inav_tickers,
        fields,
        raw=raw,
        all_fields=all_fields,
        backend=backend,
        options=options,
        conflate=conflate,
        tick_mode=tick_mode,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
        delivery=delivery,
        output=output,
    )


etf_nav_relationships = _syncify(aetf_nav_relationships)
etf_nav_snapshot = _syncify(aetf_nav_snapshot)
etf_nav_history = _syncify(aetf_nav_history)
subscribe_etf_inav = _syncify(asubscribe_etf_inav)
