"""The configurable security key column and the deprecated ``ticker`` name."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
import warnings

import pytest

from xbbg import _core, _engine, blp
from xbbg._core import ArrowTable
from xbbg.ext import futures
from xbbg.services import Operation, Service
from xbbg.testing import create_mock_response


def _keyed(key: str) -> ArrowTable:
    return ArrowTable.from_pylist([{key: "EURUSD BGN Curncy", "TICKER": "EURUSD", "NAME": "EUR-USD X-RATE"}])


def _configure(monkeypatch, key_column: str | None) -> None:
    config = None if key_column is None else _core.PyEngineConfig(key_column=key_column)
    monkeypatch.setattr(_engine, "_config", config)
    monkeypatch.setattr(_engine, "_ticker_key_column_warned", False)


def _request(monkeypatch, table: ArrowTable):
    class FakeEngine:
        async def request(self, _params):
            return table.to_batches()[0]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    return blp.request(
        Service.REFDATA,
        Operation.REFERENCE_DATA,
        securities=["EURUSD BGN Curncy"],
        fields=["TICKER", "NAME"],
        backend="native",
    )


def test_ticker_keyed_results_warn_once_per_process_at_the_callers_line(monkeypatch):
    _configure(monkeypatch, None)
    with pytest.warns(FutureWarning, match="key_column='security'") as caught:
        _request(monkeypatch, _keyed("ticker"))
        _request(monkeypatch, _keyed("ticker"))
    assert len(caught) == 1
    assert caught[0].filename == __file__

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        _request(monkeypatch, _keyed("ticker"))


def test_security_engines_never_warn_even_for_bloomberg_ticker_columns(monkeypatch):
    _configure(monkeypatch, "SECURITY")
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        result = _request(monkeypatch, _keyed("ticker"))
    assert result.num_rows == 1


def test_mock_rows_follow_the_engine_the_context_routes_to(monkeypatch):
    _configure(monkeypatch, "security")
    reference = create_mock_response(
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        data={"IBM US Equity": {"PX_LAST": 1}},
    )
    assert reference.table.column_names == ["security", "field", "value"]

    scoped = cast("_engine.Engine", SimpleNamespace(_config_snapshot=_core.PyEngineConfig(key_column="ticker")))
    token = _engine._active_engine.set(scoped)
    try:
        history = create_mock_response(
            service="//blp/refdata",
            operation="HistoricalDataRequest",
            data={"IBM US Equity": [{"date": "2024-01-02", "PX_LAST": 1}]},
        )
    finally:
        _engine._active_engine.reset(token)
    assert history.table.to_pylist() == [
        {"ticker": "IBM US Equity", "date": "2024-01-02", "field": "PX_LAST", "value": "1"}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("key_column", ["ticker", "security"])
async def test_resolvers_unwrap_the_key_column_without_warning(monkeypatch, key_column):
    _configure(monkeypatch, key_column)

    async def resolve(_engine, *_args, **_kwargs):
        return ArrowTable.from_pylist([{key_column: "ESH24 Index"}]).to_batches()[0]

    monkeypatch.setattr(_engine, "_get_engine", lambda: object())
    monkeypatch.setattr(_core, "recipe_fut_ticker", resolve)
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        assert await futures.afut_ticker("ES1 Index", "2024-01-15") == "ESH24 Index"


def test_historical_presentation_sorts_by_the_security_key():
    table = ArrowTable.from_pylist(
        [
            {"security": "XYZ US Equity", "date": "2024-01-02", "field": "PX_LAST", "value": "1"},
            {"security": "ABC US Equity", "date": "2024-01-02", "field": "PX_LAST", "value": "2"},
            {"security": "ABC US Equity", "date": "2024-01-03", "field": "PX_LAST", "value": "3"},
        ]
    )
    shaped = table.apply_historical_presentation(None, None, "DESCENDING", None, "security")
    assert [(row["security"], row["date"]) for row in shaped.to_pylist()] == [
        ("ABC US Equity", "2024-01-03"),
        ("ABC US Equity", "2024-01-02"),
        ("XYZ US Equity", "2024-01-02"),
    ]
