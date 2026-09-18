from __future__ import annotations

from typing import Any

import pytest

import xbbg
from xbbg import blp
from xbbg._core import ArrowTable
from xbbg.ext import fixed_income
from xbbg.services import Operation, Service


class FakeEngine:
    async def list_valid_elements(self, _service, _operation):
        return [
            "includeBrokerCodes",
            "includeConditionCodes",
            "includeExchangeCodes",
            "includeSpreadPrice",
            "includeYield",
            "maxDataPoints",
        ]


@pytest.fixture
def stub_engine(monkeypatch):
    blp._VALID_ELEMENTS_CACHE.clear()
    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())


def _raw_bqr_table(*, broker: bool = True, condition_codes: bool = False) -> ArrowTable:
    row: dict[str, Any] = {
        "ticker": "/isin/US037833FB15@MSG1 Corp",
        "time": "2026-04-24T19:55:58",
        "type": "ASK",
        "value": 101.972,
        "size": 75000,
    }
    if broker:
        row["brokerSellCode"] = "CTSD"
    if condition_codes:
        row["conditionCodes"] = "R"
    return ArrowTable.from_pylist([row])


def _generic_bqr_table() -> ArrowTable:
    return ArrowTable.from_pylist(
        [
            {"path": "tickData[0].time", "value_str": "2026-03-03T09:30:00", "value_num": None},
            {"path": "tickData[0].type", "value_str": "BID", "value_num": None},
            {"path": "tickData[0].value", "value_str": None, "value_num": 123.45},
            {"path": "tickData[0].size", "value_str": None, "value_num": 1000.0},
            {"path": "tickData[0].brokerBuyCode", "value_str": "DLRA", "value_num": None},
            {"path": "tickData[0].spreadPrice", "value_str": None, "value_num": 29.0},
            {"path": "tickData.eidData[0]", "value_str": None, "value_num": None},
        ]
    )


@pytest.mark.asyncio
async def test_abqr_generated_routes_intraday_tick_defaults(monkeypatch, stub_engine):
    captured: dict[str, Any] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return _raw_bqr_table()

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abqr("/isin/US037833FB15@MSG1 Corp", date_offset="-1d")

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_TICK
    assert captured["backend"] is None

    kwargs = captured["kwargs"]
    assert kwargs["security"] == "/isin/US037833FB15@MSG1 Corp"
    assert kwargs["event_types"] == ["BID", "ASK"]
    assert kwargs["elements"] == [("includeBrokerCodes", "true")]
    assert "T" in kwargs["start_datetime"]
    assert "T" in kwargs["end_datetime"]
    assert result.column_names == ["ticker", "time", "event_type", "price", "size", "broker_sell"]
    assert result.to_pylist()[0]["broker_sell"] == "CTSD"


@pytest.mark.asyncio
async def test_abqr_generated_warns_for_non_isin_msg1_source(monkeypatch, stub_engine):
    async def fake_arequest(**_kwargs):
        return _raw_bqr_table()

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    with pytest.warns(UserWarning, match="@MSG1 Corp"):
        await blp.abqr("US037833FB15@MSG1 Corp", date_offset="-1d")


@pytest.mark.asyncio
async def test_abqr_generated_reshapes_generic_path_output(monkeypatch, stub_engine):
    captured: dict[str, Any] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return _generic_bqr_table()

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abqr(
        "/isin/US037833FB15@MSG1 Corp",
        date_offset="-2d",
        include_spread_price=True,
    )

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_TICK
    assert captured["backend"] is None

    kwargs = captured["kwargs"]
    assert kwargs["event_types"] == ["BID", "ASK"]
    assert kwargs["elements"] == [
        ("includeBrokerCodes", "true"),
        ("includeSpreadPrice", "true"),
    ]

    rows = result.to_pylist()
    assert len(rows) == 1
    assert rows[0]["ticker"] == "/isin/US037833FB15@MSG1 Corp"
    assert rows[0]["event_type"] == "BID"
    assert rows[0]["price"] == 123.45
    assert rows[0]["size"] == 1000.0
    assert rows[0]["broker_buy"] == "DLRA"
    assert rows[0]["spread_price"] == 29.0


@pytest.mark.asyncio
async def test_abqr_generated_keeps_typed_include_output(monkeypatch, stub_engine):
    captured: dict[str, Any] = {}
    typed_table = _raw_bqr_table(condition_codes=True)

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return typed_table

    def fail_reshape(_pdf, _ticker):
        raise AssertionError("typed bqr output must not use generic reshaper")

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "_reshape_bqr_generic", fail_reshape)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abqr(
        "/isin/US037833FB15@MSG1 Corp",
        date_offset="-2d",
        include_condition_codes=True,
    )

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_TICK
    assert captured["backend"] is None
    assert captured["kwargs"]["elements"] == [("includeBrokerCodes", "true"), ("includeConditionCodes", "true")]
    assert "condition_codes" in result.column_names
    assert result.to_pylist()[0]["broker_sell"] == "CTSD"


@pytest.mark.asyncio
async def test_abqr_generated_uses_explicit_datetime_range_and_event_types(monkeypatch, stub_engine):
    captured: dict[str, Any] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return _raw_bqr_table(broker=False)

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abqr(
        "XYZ 4.5 01/15/30 Corp",
        start_date="2024-01-15T10:30:00",
        end_date="2024-01-15T10:45:00",
        event_types=["TRADE"],
        include_broker_codes=False,
    )

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_TICK
    assert captured["backend"] is None

    kwargs = captured["kwargs"]
    assert kwargs["security"] == "XYZ 4.5 01/15/30 Corp"
    assert kwargs["start_datetime"] == "2024-01-15T10:30:00"
    assert kwargs["end_datetime"] == "2024-01-15T10:45:00"
    assert kwargs["event_types"] == ["TRADE"]
    assert kwargs["elements"] is None
    assert result.column_names == ["ticker", "time", "event_type", "price", "size"]


def test_bqr_postprocess_requires_broker_codes_for_attributed_quotes():
    with pytest.raises(RuntimeError, match="without broker attribution"):
        blp._postprocess_bqr_result(
            _raw_bqr_table(broker=False),
            ticker="/isin/US037833FB15",
            backend="pyarrow",
            enforce_broker_codes=True,
        )


def test_reshape_bqr_generic_uses_arrow_table_without_pandas():
    result = blp._reshape_bqr_generic(_generic_bqr_table(), "AAPL US Equity")
    rows = result.to_pylist()

    assert len(rows) == 1
    assert rows[0]["ticker"] == "AAPL US Equity"
    assert rows[0]["time"] == "2026-03-03T09:30:00"
    assert rows[0]["type"] == "BID"
    assert rows[0]["value"] == 123.45
    assert rows[0]["size"] == 1000.0
    assert rows[0]["brokerBuyCode"] == "DLRA"


@pytest.mark.asyncio
async def test_ext_abqr_defaults_to_bid_ask_and_broker_codes(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_abdtick(**kwargs):
        captured.update(kwargs)
        return _raw_bqr_table()

    monkeypatch.setattr(xbbg, "abdtick", fake_abdtick)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await fixed_income.abqr(
        "/isin/US037833FB15@MSG1 Corp",
        start_datetime="2026-04-24T00:00:00",
        end_datetime="2026-04-24T23:59:59",
        maxDataPoints=5,
    )

    assert captured["ticker"] == "/isin/US037833FB15@MSG1 Corp"
    assert captured["event_types"] == ["BID", "ASK"]
    assert captured["includeBrokerCodes"] is True
    assert captured["maxDataPoints"] == 5
    assert captured["backend"] == "native"
    assert result.column_names == ["ticker", "time", "event_type", "price", "size", "broker_sell"]
    assert result.to_pylist()[0]["broker_sell"] == "CTSD"


@pytest.mark.asyncio
async def test_ext_abqr_warns_for_non_isin_msg1_source(monkeypatch):
    async def fake_abdtick(**_kwargs):
        return _raw_bqr_table()

    monkeypatch.setattr(xbbg, "abdtick", fake_abdtick)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    with pytest.warns(UserWarning, match="/isin/US037833FB15@MSG1 Corp"):
        await fixed_income.abqr(
            "US037833FB15@MSG1 Corp",
            start_datetime="2026-04-24T00:00:00",
            end_datetime="2026-04-24T23:59:59",
            maxDataPoints=5,
        )


@pytest.mark.asyncio
async def test_ext_abqr_preserves_explicit_event_types(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_abdtick(**kwargs):
        captured.update(kwargs)
        return _raw_bqr_table(broker=False)

    monkeypatch.setattr(xbbg, "abdtick", fake_abdtick)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await fixed_income.abqr(
        "IBM US Equity",
        start_datetime="2026-04-24T00:00:00",
        end_datetime="2026-04-24T23:59:59",
        event_types=["TRADE"],
        include_broker_codes=False,
    )

    assert captured["event_types"] == ["TRADE"]
    assert captured["backend"] == "native"
    assert "includeBrokerCodes" not in captured
    assert result.column_names == ["ticker", "time", "event_type", "price", "size"]
