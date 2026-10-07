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


def test_pivot_bdp_to_wide_uses_native_pyo3_pivot_for_xbbg_narwhals(monkeypatch):
    import narwhals.stable.v1 as nw

    import xbbg._core as core_module
    from xbbg._core import ArrowTable
    from xbbg.ext._utils import _pivot_bdp_to_wide

    long_table = ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "field": "PX_LAST", "value": 123.45},
            {"ticker": "IBM US Equity", "field": "VOLUME", "value": 1000.0},
        ]
    )
    wide_batch = ArrowTable.from_pylist(
        [{"ticker": "IBM US Equity", "PX_LAST": 123.45, "VOLUME": 1000.0}]
    ).to_batches()[0]
    calls = []

    def fake_ext_pivot_to_wide(batch):
        calls.append(batch.to_pylist())
        return wide_batch

    monkeypatch.setattr(core_module, "ext_pivot_to_wide", fake_ext_pivot_to_wide)

    result = _pivot_bdp_to_wide(nw.from_native(long_table))

    assert calls == [long_table.to_pylist()]
    native = result.to_native()
    assert native.column_names == ["ticker", "PX_LAST", "VOLUME"]
    assert native.to_pylist() == [{"ticker": "IBM US Equity", "PX_LAST": 123.45, "VOLUME": 1000.0}]


def test_pivot_bdp_to_wide_keeps_foreign_frames_on_pure_narwhals_path(monkeypatch):
    import narwhals.stable.v1 as nw

    import xbbg._core as core_module
    from xbbg.ext._utils import _pivot_bdp_to_wide

    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        [
            {"ticker": "IBM US Equity", "field": "PX_LAST", "value": 123.45},
            {"ticker": "IBM US Equity", "field": "VOLUME", "value": 1000.0},
        ]
    )

    def fail_native_pivot(_batch):
        raise AssertionError("foreign Narwhals frames must not call the native xbbg pivot")

    monkeypatch.setattr(core_module, "ext_pivot_to_wide", fail_native_pivot)

    result = _pivot_bdp_to_wide(nw.from_native(frame)).to_native()

    assert set(result.columns) == {"ticker", "PX_LAST", "VOLUME"}
    row = result.set_index("ticker").loc["IBM US Equity"]
    assert row["PX_LAST"] == 123.45
    assert row["VOLUME"] == 1000.0


@pytest.mark.asyncio
async def test_corporate_bonds_uses_native_query_without_active_only(monkeypatch):
    import inspect

    import xbbg
    from xbbg.ext import fixed_income

    calls = []

    def build_query(ticker, ccy, fields):
        calls.append((ticker, ccy, fields))
        return "get(id) for(debt('ACME US Equity'))"

    async def query(expression, **kwargs):
        return expression, kwargs

    monkeypatch.setattr(fixed_income, "ext_build_corporate_bonds_query", build_query)
    monkeypatch.setattr(xbbg, "abql", query)
    result = await fixed_income.acorporate_bonds("ACME US Equity", ccy=None, fields=["name"], backend="native")

    assert calls == [("ACME US Equity", None, ["name"])]
    assert result == ("get(id) for(debt('ACME US Equity'))", {"backend": "native"})
    assert "active_only" not in inspect.signature(fixed_income.acorporate_bonds).parameters
    assert "active_only" not in inspect.signature(fixed_income.corporate_bonds).parameters
