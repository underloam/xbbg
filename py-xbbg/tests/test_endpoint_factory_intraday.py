from __future__ import annotations

import pytest

from xbbg import blp
from xbbg.services import Operation, Service


class FakeEngine:
    async def list_valid_elements(self, _service, _operation):
        return ["eventType", "interval", "maxDataPoints"]


@pytest.fixture(autouse=True)
def stub_engine(monkeypatch):
    blp._VALID_ELEMENTS_CACHE.clear()
    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())


@pytest.mark.asyncio
async def test_abdtick_forwards_overrides(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    await blp.abdtick(
        "ESM6 Index",
        "2026-04-17T08:00:00",
        "2026-04-17T18:23:33",
        event_types=["BID", "ASK"],
        overrides={"Points": 1},
    )

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_TICK
    assert captured["kwargs"].get("elements") == [("maxDataPoints", 1)]
    assert captured["kwargs"].get("overrides") is None


@pytest.mark.asyncio
async def test_abdib_forwards_overrides(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    await blp.abdib(
        "ESM6 Index",
        dt="2026-04-17",
        typ="TRADE",
        overrides={"Points": 1},
    )

    assert captured["service"] == Service.REFDATA
    assert captured["operation"] == Operation.INTRADAY_BAR
    assert captured["kwargs"].get("elements") == [("maxDataPoints", 1)]
    assert captured["kwargs"].get("overrides") is None
