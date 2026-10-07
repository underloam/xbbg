from __future__ import annotations

import pytest

from xbbg import _endpoints, _engine, backend as backend_module, blp
from xbbg._core import ArrowTable
from xbbg.services import ExtractorHint, Operation, Service


@pytest.mark.asyncio
async def test_absrch_generated_endpoint_preserves_excel_grid_overrides(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda frame, _backend: frame)

    result = await blp.absrch("COMDTY:WEATHER", provider="wsi", location_time=True)

    assert result == []
    assert captured["service"] == Service.EXRSVC
    assert captured["operation"] == Operation.EXCEL_GET_GRID
    assert captured["backend"] is None
    assert captured["kwargs"] == {
        "elements": [("Domain", "COMDTY:WEATHER")],
        "overrides": [("provider", "wsi"), ("location_time", "true")],
        "extractor": ExtractorHint.BSRCH,
        "_raw": True,
    }


@pytest.mark.asyncio
async def test_arequest_preserves_exrsvc_overrides_as_overrides(monkeypatch):
    captured: dict[str, object] = {}
    raw = ArrowTable.from_pylist([{"ok": "1"}])

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return raw

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.EXRSVC,
        operation=Operation.EXCEL_GET_GRID,
        elements=[("Domain", "COMDTY:WEATHER")],
        overrides=[("provider", "wsi")],
        _raw=True,
    )

    assert result.num_rows == 1
    assert captured["service"] == Service.EXRSVC.value
    assert captured["operation"] == Operation.EXCEL_GET_GRID.value
    assert captured["elements"] == [("Domain", "COMDTY:WEATHER")]
    assert captured["overrides"] == [("provider", "wsi")]


def test_absrch_plan_routes_search_kwargs_to_excel_grid_overrides():
    plan = _endpoints._build_absrch_plan(
        {
            "domain": "COMDTY:WEATHER",
            "backend": None,
            "kwargs": {
                "overrides": {"provider": "wsi", "Domain": "COMDTY:OVERRIDE"},
                "location_time": True,
                "fields": "HDD_18C",
            },
        }
    )

    assert plan.request_kwargs == {
        "elements": [("Domain", "COMDTY:OVERRIDE")],
        "overrides": [
            ("provider", "wsi"),
            ("location_time", "true"),
            ("fields", "HDD_18C"),
        ],
    }
    assert plan.backend is None


def test_absrch_plan_accepts_explicit_override_pairs():
    plan = _endpoints._build_absrch_plan(
        {
            "domain": "COMDTY:WEATHER",
            "backend": "native",
            "kwargs": {"overrides": [("location", "nwe"), ("model", "ecmwf")]},
        }
    )

    assert plan.request_kwargs == {
        "elements": [("Domain", "COMDTY:WEATHER")],
        "overrides": [("location", "nwe"), ("model", "ecmwf")],
    }
    assert plan.backend == "native"


def test_absrch_plan_rejects_scalar_overrides():
    with pytest.raises(TypeError, match="bsrch overrides"):
        _endpoints._build_absrch_plan(
            {"domain": "COMDTY:WEATHER", "backend": None, "kwargs": {"overrides": "provider=wsi"}}
        )


@pytest.mark.parametrize(
    "overrides",
    [
        ["ab"],
        [("a",)],
        [("a", "b", "c")],
    ],
)
def test_absrch_plan_rejects_malformed_override_pairs(overrides):
    with pytest.raises(TypeError, match="bsrch overrides"):
        _endpoints._build_absrch_plan({"domain": "COMDTY:WEATHER", "backend": None, "kwargs": {"overrides": overrides}})
