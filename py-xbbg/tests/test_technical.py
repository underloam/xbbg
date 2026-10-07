"""Offline contracts for the collocated technical-analysis implementation."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from xbbg import _engine, _technical, backend as backend_module, blp, schema
from xbbg._core import ArrowTable


def test_public_helpers_reexport_the_technical_implementation():
    for name in ("abta", "ta_studies", "ta_study_params", "generate_ta_stubs"):
        assert getattr(blp, name) is getattr(_technical, name)
    assert blp.bta.__wrapped__ is _technical.abta
    assert blp.ta_study_params("sma") == {"period": 20, "priceSourceClose": "PX_LAST"}
    assert "sma" in blp.ta_studies()


def test_study_request_uses_shared_aliases_defaults_and_explicit_parameters():
    elements = dict(
        _technical._build_study_request(
            "SYNTHETIC Equity",
            "sma",
            start_date="2024-01-02",
            end_date="2024-01-03",
            period=5,
        )
    )
    assert elements == {
        "priceSource.securityName": "SYNTHETIC Equity",
        "priceSource.dataRange.historical.startDate": "20240102",
        "priceSource.dataRange.historical.endDate": "20240103",
        "priceSource.dataRange.historical.periodicitySelection": "DAILY",
        "studyAttributes.smavgStudyAttributes.period": "5",
        "studyAttributes.smavgStudyAttributes.priceSourceClose": "PX_LAST",
    }
    assert blp.ta_study_params("sma")["period"] == 20


def test_abta_uses_shared_engine_and_backend_seams(monkeypatch):
    calls = []
    batch = ArrowTable.from_pylist([{"value": 7.0}]).to_batches()[0]

    class Engine:
        async def request(self, params):
            calls.append(params)
            return batch

    def convert(table, backend):
        assert backend is None
        return table.to_pylist()

    monkeypatch.setattr(_engine, "_get_engine", lambda: Engine())
    monkeypatch.setattr(backend_module, "_convert_result_backend", convert)

    assert asyncio.run(blp.abta(["SYNTHETIC1 Equity", "SYNTHETIC2 Equity"], "rsi")) == [
        {"value": 7.0},
        {"value": 7.0},
    ]
    assert [dict(call["elements"])["priceSource.securityName"] for call in calls] == [
        "SYNTHETIC1 Equity",
        "SYNTHETIC2 Equity",
    ]
    assert all(call["service"] == "//blp/tasvc" for call in calls)
    assert all(call["operation"] == "studyRequest" for call in calls)


def test_abta_preserves_partial_success_warnings(monkeypatch):
    batch = ArrowTable.from_pylist([{"value": 7.0}]).to_batches()[0]

    class Engine:
        async def request(self, params):
            if dict(params["elements"])["priceSource.securityName"] == "FAILED Equity":
                raise RuntimeError("synthetic failure")
            return batch

    monkeypatch.setattr(_engine, "_get_engine", lambda: Engine())
    monkeypatch.setattr(backend_module, "_convert_result_backend", lambda table, _backend: table)

    with pytest.warns(UserWarning, match="FAILED Equity: synthetic failure"):
        result = asyncio.run(blp.abta(["SYNTHETIC Equity", "FAILED Equity"], "sma"))
    assert result.to_pylist() == [{"value": 7.0}]


def test_stub_generation_uses_the_same_study_vocabulary(monkeypatch, tmp_path, capsys):
    parameter = SimpleNamespace(name="period", enum_values=[], data_type="Int32")
    study = SimpleNamespace(name="rsiStudyAttributes", children=[parameter])
    attributes = SimpleNamespace(name="studyAttributes", children=[study])
    operation = SimpleNamespace(request=SimpleNamespace(children=[attributes]))

    def get_schema(service):
        assert service == "//blp/tasvc"
        return SimpleNamespace(get_operation=lambda name: operation if name == "studyRequest" else None)

    monkeypatch.setattr(schema, "get_schema", get_schema)
    monkeypatch.setattr(schema, "configure_ide_stubs", lambda _path: "synthetic IDE configuration")

    stub = Path(blp.generate_ta_stubs(str(tmp_path)))
    source = stub.read_text()
    assert stub.name == "ta_studies.pyi"
    assert "class RSIParams(TypedDict, total=False):" in source
    assert "period: NotRequired[int]  # default: 14" in source
    assert repr("rsi") in source
    assert stub.with_suffix(".py").read_text() == source
    assert capsys.readouterr().out == "synthetic IDE configuration\n"
