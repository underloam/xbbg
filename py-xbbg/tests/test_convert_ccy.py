"""Offline coverage for the native currency recipe adapter."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import narwhals.stable.v1 as nw
import pandas as pd
import pytest

from xbbg.ext import currency


@pytest.mark.asyncio
async def test_convert_ccy_forwards_arrow_input_options_and_backend(monkeypatch):
    calls = []
    expected = pd.DataFrame({"value": ["0.5"]})

    async def recipe(name, *args, **kwargs):
        calls.append((name, args, kwargs))
        return expected

    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    data = pd.DataFrame({"ticker": ["ABC LN Equity"], "date": [date(2024, 1, 2)], "value": ["100"]})
    result = await currency.aconvert_ccy(data, ccy="USD", backend="pandas", Per="W", overrides={"CUSTOM": 1})

    assert result is expected
    name, args, kwargs = calls[0]
    assert name == "recipe_adjust_ccy"
    assert args[1] == "USD"
    assert list(nw.from_native(args[0]).iter_rows(named=True)) == data.to_dict("records")
    assert kwargs == {"backend": "pandas", "request_options": {"Per": "W", "overrides": {"CUSTOM": 1}}}


@pytest.mark.asyncio
async def test_convert_ccy_does_not_copy_native_input(monkeypatch):
    from xbbg._core import ArrowTable

    data = ArrowTable.from_pylist([{"ticker": "ABC LN Equity", "date": "2024-01-02", "value": "100"}])
    recipe = AsyncMock(return_value=data)
    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    assert await currency.aconvert_ccy(data, ccy="local", backend="native") is data
    recipe.assert_awaited_once_with("recipe_adjust_ccy", data, "local", backend="native", request_options={})


@pytest.mark.asyncio
async def test_convert_ccy_accepts_pandas_without_pyarrow(monkeypatch):
    monkeypatch.setattr(currency, "is_backend_available", lambda _backend: False)
    calls = []

    async def recipe(_name, batch, *_args, **_kwargs):
        calls.append(batch)
        return batch

    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    data = pd.DataFrame({"ticker": ["ABC LN Equity"], "date": [date(2024, 1, 2)], "value": [1.25]})
    result = await currency.aconvert_ccy(data)
    assert result is calls[0]
    assert result.to_pylist() == data.to_dict("records")


@pytest.mark.asyncio
async def test_convert_ccy_keeps_empty_column_names_without_pyarrow(monkeypatch):
    monkeypatch.setattr(currency, "is_backend_available", lambda _backend: False)

    async def recipe(_name, batch, *_args, **_kwargs):
        return batch

    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    result = await currency.aconvert_ccy(pd.DataFrame(columns=["ticker", "date", "value"]))
    assert result.column_names == ["ticker", "date", "value"]
    assert result.num_rows == 0


@pytest.mark.asyncio
async def test_convert_ccy_propagates_native_failures(monkeypatch):
    from xbbg._core import ArrowTable

    recipe = AsyncMock(side_effect=ValueError("invalid FX request"))
    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    with pytest.raises(ValueError, match="invalid FX request"):
        await currency.aconvert_ccy(ArrowTable.empty(["ticker", "date", "value"]))


def test_convert_ccy_sync_wrapper_uses_native_dispatch(monkeypatch):
    from xbbg._core import ArrowTable

    result = object()
    recipe = AsyncMock(return_value=result)
    monkeypatch.setattr(currency, "_call_native_recipe", recipe)
    assert currency.convert_ccy(ArrowTable.empty([])) is result
    recipe.assert_awaited_once()
