"""Offline tests for native-backed extension workflow wrappers."""

from __future__ import annotations

import pandas as pd
import pytest

import xbbg.ext as ext
from xbbg.ext import futures, historical, identifiers, indices, volatility


@pytest.mark.asyncio
async def test_futures_curve_forwards_native_recipe(monkeypatch):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return pd.DataFrame({"contract_ticker": ["ESH24 Index"]})

    monkeypatch.setattr(futures, "_call_native_recipe", fake_call)

    result = await futures.afutures_curve(
        "ES1 Index",
        asof="2024-01-02",
        fields=["PX_BID"],
        max_contracts=3,
        backend="pandas",
    )

    assert result["contract_ticker"].tolist() == ["ESH24 Index"]
    assert calls == [
        (
            "recipe_futures_curve",
            ("ES1 Index", "20240102", None, ["PX_BID"], 3),
            "pandas",
            {},
        )
    ]


@pytest.mark.asyncio
async def test_vol_surface_accepts_mapping_metadata(monkeypatch):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return pd.DataFrame({"metric": ["implied_volatility"]})

    monkeypatch.setattr(volatility, "_call_native_recipe", fake_call)

    await volatility.avol_surface(
        "SPX Index",
        start_date="2024-01-02",
        end_date="2024-01-03",
        preset=[volatility.VolSurfacePreset.MONEYNESS_30D],
        fields={"CUSTOM_VOL": {"metric": "implied_volatility", "tenor": "1M", "point_type": "custom", "point": 1}},
        include_derived=True,
        risk_free_rate=0.05,
    )

    assert calls[0][0] == "recipe_vol_surface"
    assert calls[0][1] == (
        ["SPX Index"],
        "20240102",
        "20240103",
        ["MONEYNESS_30D"],
        ["CUSTOM_VOL|implied_volatility|1M|custom|1"],
        True,
        True,
        0.05,
        None,
    )


@pytest.mark.asyncio
async def test_dividend_yield_forwards_filters(monkeypatch):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return pd.DataFrame({"dividend_yield": [0.01]})

    monkeypatch.setattr(historical, "_call_native_recipe", fake_call)

    await historical.adividend_yield(
        "AAPL US Equity",
        start_date="2024-01-01",
        end_date="2024-12-31",
        dividend_types=["Regular Cash"],
        window_days=365,
    )

    assert calls == [
        (
            "recipe_dividend_yield",
            (["AAPL US Equity"], "20240101", "20241231", ["Regular Cash"], 365),
            None,
            {},
        )
    ]


@pytest.mark.asyncio
async def test_index_members_validates_field_and_asof(monkeypatch):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return pd.DataFrame({"member": ["AAPL US"]})

    monkeypatch.setattr(indices, "_call_native_recipe", fake_call)

    await indices.aindex_members("SPX Index", field="INDX_MWEIGHT", asof="2024-01-02")
    assert calls[0][1] == ("SPX Index", "INDX_MWEIGHT", "20240102")

    with pytest.raises(ValueError, match="field must be one of"):
        await indices.aindex_members("SPX Index", field="BAD_FIELD")


@pytest.mark.asyncio
async def test_identifier_wrappers_preserve_input_order(monkeypatch):
    calls = []

    async def fake_call(recipe_name, *args, backend=None, **kwargs):
        calls.append((recipe_name, args, backend, kwargs))
        return pd.DataFrame({"input_order": [0, 1]})

    monkeypatch.setattr(identifiers, "_call_native_recipe", fake_call)

    await identifiers.aresolve_isins(["US0378331005", "BAD"])
    await identifiers.aissuer_isins("US037833FB15")

    assert calls[0][0] == "recipe_resolve_isins"
    assert calls[0][1] == (["US0378331005", "BAD"],)
    assert calls[1][0] == "recipe_issuer_isins"
    assert calls[1][1] == (["US037833FB15"],)


def test_ext_exports_new_workflows():
    for name in (
        "futures_curve",
        "afutures_curve",
        "vol_surface",
        "avol_surface",
        "dividend_yield",
        "adividend_yield",
        "index_members",
        "aindex_members",
        "resolve_isins",
        "aresolve_isins",
        "issuer_isins",
        "aissuer_isins",
        "VolSurfacePreset",
    ):
        assert hasattr(ext, name)
        assert name in ext.__all__


@pytest.mark.asyncio
async def test_corporate_bonds_uses_native_recipe_without_active_only(monkeypatch):
    import inspect

    from xbbg.ext import fixed_income

    calls = []
    native_result = object()

    async def call_recipe(name, ticker, **kwargs):
        calls.append((name, ticker, kwargs))
        return native_result

    monkeypatch.setattr(fixed_income, "_call_native_recipe", call_recipe)
    result = await fixed_income.acorporate_bonds(
        "ACME US Equity", ccy=None, fields=["name"], backend="native", mode="cached"
    )

    assert calls == [
        (
            "recipe_corporate_bonds",
            "ACME US Equity",
            {"ccy": None, "fields": ["name"], "backend": "native", "request_options": {"mode": "cached"}},
        )
    ]
    assert result is native_result
    assert "active_only" not in inspect.signature(fixed_income.acorporate_bonds).parameters
    assert "active_only" not in inspect.signature(fixed_income.corporate_bonds).parameters
