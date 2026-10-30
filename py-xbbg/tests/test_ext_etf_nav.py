"""Offline tests for ETF NAV / iNAV extension wrappers and subscription preflight."""

from __future__ import annotations

from datetime import date
import inspect

import pytest

from xbbg._core import ArrowTable
import xbbg.ext as ext
from xbbg.ext import etf

QQQ = "QQQ US Equity"
AT1 = "AT1 LN Equity"
QQQ_NAV = "QQQNV Index"
QQQ_INAV = "QXV Index"
AT1_INAV = "AT1IN Index"


def _relationship_row(
    input_order: int,
    etf_ticker: str,
    *,
    nav_ticker: str | None,
    inav_ticker: str | None,
    inav_validation_error: str | None = None,
) -> dict:
    return {
        "input_order": input_order,
        "etf_ticker": etf_ticker,
        "nav_ticker": nav_ticker,
        "nav_market_sector_des": "Index" if nav_ticker else None,
        "nav_name": f"{nav_ticker} name" if nav_ticker else None,
        "nav_validation_error": None,
        "inav_ticker": inav_ticker,
        "inav_market_sector_des": "Index" if inav_ticker else None,
        "inav_name": f"{inav_ticker} name" if inav_ticker else None,
        "inav_validation_error": inav_validation_error,
    }


def _qqq_at1_rows() -> list[dict]:
    return [
        _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
        _relationship_row(1, AT1, nav_ticker=None, inav_ticker=AT1_INAV),
    ]


def _patch_recipe(monkeypatch, result_factory):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return result_factory()

    monkeypatch.setattr(etf, "_call_native_recipe", fake_call)
    return calls


def _patch_asubscribe(monkeypatch, sentinel):
    calls = []

    async def fake_asubscribe(tickers, fields, **kwargs):
        calls.append((tickers, fields, kwargs))
        return sentinel

    import xbbg.blp as blp

    monkeypatch.setattr(blp, "asubscribe", fake_asubscribe)
    return calls


# --- Table wrappers ---------------------------------------------------------


@pytest.mark.asyncio
async def test_etf_nav_relationships_forwards_trimmed_inputs(monkeypatch):
    calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))

    result = await etf.aetf_nav_relationships([f"  {QQQ}  ", AT1], backend="pandas")

    assert calls == [
        ("recipe_etf_nav_relationships", ([QQQ, AT1],), "pandas", {}),
    ]
    assert isinstance(result, ArrowTable)
    rows = result.to_pylist()
    assert rows[0]["inav_ticker"] == QQQ_INAV
    assert rows[1]["nav_ticker"] is None
    assert rows[1]["inav_ticker"] == AT1_INAV


@pytest.mark.asyncio
async def test_etf_nav_snapshot_normalizes_str_input(monkeypatch):
    calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))

    await etf.aetf_nav_snapshot(f" {QQQ} ")

    assert calls == [("recipe_etf_nav_snapshot", ([QQQ],), None, {})]


@pytest.mark.asyncio
async def test_etf_nav_history_formats_required_dates(monkeypatch):
    calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))

    await etf.aetf_nav_history(
        [QQQ, AT1],
        start_date="2026-06-01",
        end_date=date(2026, 7, 1),
        backend="narwhals",
    )

    assert calls == [
        ("recipe_etf_nav_history", ([QQQ, AT1], "20260601", "20260701"), "narwhals", {}),
    ]


@pytest.mark.asyncio
async def test_etf_nav_history_requires_both_dates(monkeypatch):
    calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))

    with pytest.raises(ValueError, match="start_date and end_date are required"):
        await etf.aetf_nav_history([QQQ], start_date=None, end_date="2026-07-01")
    with pytest.raises(ValueError, match="start_date and end_date are required"):
        await etf.aetf_nav_history([QQQ], start_date="2026-06-01", end_date=None)
    assert calls == []


@pytest.mark.asyncio
async def test_table_wrappers_pass_duplicates_and_empty_lists(monkeypatch):
    calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))

    await etf.aetf_nav_relationships([])
    await etf.aetf_nav_snapshot([QQQ, QQQ])

    assert calls[0][1] == ([],)
    assert calls[1][1] == ([QQQ, QQQ],)


# --- Subscription preflight ---------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_etf_inav_defaults_to_last_price(monkeypatch):
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))
    sentinel = object()
    subscribe_calls = _patch_asubscribe(monkeypatch, sentinel)

    subscription = await etf.asubscribe_etf_inav([QQQ, AT1])

    assert subscription is sentinel
    assert recipe_calls == [
        ("recipe_etf_nav_relationships", ([QQQ, AT1],), "native", {}),
    ]
    assert len(subscribe_calls) == 1
    tickers, fields, kwargs = subscribe_calls[0]
    assert tickers == [QQQ_INAV, AT1_INAV]
    assert fields == "LAST_PRICE"
    assert kwargs == {
        "raw": False,
        "all_fields": False,
        "backend": None,
        "options": None,
        "conflate": False,
        "tick_mode": False,
        "flush_threshold": None,
        "stream_capacity": None,
        "overflow_policy": None,
        "delivery": "deltas",
        "output": None,
    }


@pytest.mark.asyncio
async def test_subscribe_etf_inav_forwards_stream_options(monkeypatch):
    _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(_qqq_at1_rows()))
    sentinel = object()
    subscribe_calls = _patch_asubscribe(monkeypatch, sentinel)

    await etf.asubscribe_etf_inav(
        [QQQ, AT1],
        ["BID", "ASK"],
        raw=True,
        all_fields=True,
        backend="pandas",
        options=["interval=2"],
        conflate=True,
        tick_mode=True,
        flush_threshold=5,
        stream_capacity=128,
        overflow_policy="block",
        delivery="deltas",
        output="dict",
    )

    tickers, fields, kwargs = subscribe_calls[0]
    assert tickers == [QQQ_INAV, AT1_INAV]
    assert fields == ["BID", "ASK"]
    assert kwargs == {
        "raw": True,
        "all_fields": True,
        "backend": "pandas",
        "options": ["interval=2"],
        "conflate": True,
        "tick_mode": True,
        "flush_threshold": 5,
        "stream_capacity": 128,
        "overflow_policy": "block",
        "delivery": "deltas",
        "output": "dict",
    }


@pytest.mark.asyncio
async def test_subscribe_etf_inav_rejects_empty_input(monkeypatch):
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist([]))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(ValueError, match="etfs must not be empty"):
        await etf.asubscribe_etf_inav([])

    assert recipe_calls == []
    assert subscribe_calls == []


@pytest.mark.asyncio
async def test_subscribe_etf_inav_rejects_trimmed_duplicates(monkeypatch):
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist([]))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(
        ValueError,
        match=rf"Duplicate ETF inputs are not allowed: {QQQ}, {AT1}",
    ):
        await etf.asubscribe_etf_inav([QQQ, AT1, f"  {QQQ} ", AT1, QQQ])

    assert recipe_calls == []
    assert subscribe_calls == []


@pytest.mark.asyncio
async def test_subscribe_etf_inav_rejects_invalid_relationship(monkeypatch):
    rows = [
        _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
        _relationship_row(
            1,
            AT1,
            nav_ticker=None,
            inav_ticker=AT1_INAV,
            inav_validation_error="NAME is missing",
        ),
    ]
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(rows))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(
        ValueError,
        match=f"Invalid iNAV relationship for ETF {AT1}: NAME is missing",
    ):
        await etf.asubscribe_etf_inav([QQQ, AT1])

    assert len(recipe_calls) == 1
    assert subscribe_calls == []


@pytest.mark.asyncio
async def test_subscribe_etf_inav_collects_all_missing_inavs(monkeypatch):
    rows = [
        _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=None),
        _relationship_row(1, AT1, nav_ticker=None, inav_ticker="   "),
        _relationship_row(2, "SPY US Equity", nav_ticker=None, inav_ticker="SPYIV Index"),
    ]
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(rows))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(
        ValueError,
        match=f"Missing valid iNAV relationship for ETFs: {QQQ}, {AT1}",
    ):
        await etf.asubscribe_etf_inav([QQQ, AT1, "SPY US Equity"])

    assert len(recipe_calls) == 1
    assert subscribe_calls == []


@pytest.mark.asyncio
async def test_subscribe_etf_inav_rejects_ambiguous_reverse_mapping(monkeypatch):
    rows = [
        _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
        _relationship_row(1, "QQQM US Equity", nav_ticker=None, inav_ticker=QQQ_INAV),
    ]
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(rows))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(
        ValueError,
        match=f"Ambiguous iNAV reverse mapping for {QQQ_INAV}: {QQQ}, QQQM US Equity",
    ):
        await etf.asubscribe_etf_inav([QQQ, "QQQM US Equity"])

    assert len(recipe_calls) == 1
    assert subscribe_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        # Too few rows.
        [_relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV)],
        # Duplicate input_order.
        [
            _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
            _relationship_row(0, AT1, nav_ticker=None, inav_ticker=AT1_INAV),
        ],
        # Mismatched etf_ticker identity.
        [
            _relationship_row(0, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
            _relationship_row(1, "WRONG US Equity", nav_ticker=None, inav_ticker=AT1_INAV),
        ],
        # Reordered tickers do not match input positions.
        [
            _relationship_row(0, AT1, nav_ticker=None, inav_ticker=AT1_INAV),
            _relationship_row(1, QQQ, nav_ticker=QQQ_NAV, inav_ticker=QQQ_INAV),
        ],
    ],
)
async def test_subscribe_etf_inav_rejects_malformed_results(monkeypatch, rows):
    recipe_calls = _patch_recipe(monkeypatch, lambda: ArrowTable.from_pylist(rows))
    subscribe_calls = _patch_asubscribe(monkeypatch, object())

    with pytest.raises(
        ValueError,
        match="ETF NAV relationship result is not one-to-one with requested ETFs",
    ):
        await etf.asubscribe_etf_inav([QQQ, AT1])

    assert len(recipe_calls) == 1
    assert subscribe_calls == []


# --- Module surface -----------------------------------------------------------


def test_sync_aliases_mirror_async_signatures():
    pairs = [
        (ext.etf_nav_relationships, etf.aetf_nav_relationships),
        (ext.etf_nav_snapshot, etf.aetf_nav_snapshot),
        (ext.etf_nav_history, etf.aetf_nav_history),
        (ext.subscribe_etf_inav, etf.asubscribe_etf_inav),
    ]
    for sync_func, async_func in pairs:
        assert not inspect.iscoroutinefunction(sync_func)
        assert inspect.iscoroutinefunction(async_func)
        assert getattr(sync_func, "__name__", "") == getattr(async_func, "__name__", "")[1:]
        assert inspect.signature(sync_func) == inspect.signature(async_func)


def test_ext_all_exports_etf_nav_names():
    expected = {
        "etf_nav_relationships",
        "etf_nav_snapshot",
        "etf_nav_history",
        "subscribe_etf_inav",
        "aetf_nav_relationships",
        "aetf_nav_snapshot",
        "aetf_nav_history",
        "asubscribe_etf_inav",
    }
    assert expected <= set(ext.__all__)
    for name in expected:
        assert callable(getattr(ext, name))
