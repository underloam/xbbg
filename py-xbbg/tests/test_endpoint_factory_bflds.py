from __future__ import annotations

import pytest

from xbbg import backend as backend_module, blp
from xbbg.services import ExtractorHint, Operation, Service


@pytest.mark.asyncio
async def test_abflds_routes_field_info_for_fields(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return [{"field": "PX_LAST"}]

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abflds(fields="PX_LAST")

    assert captured["service"] == Service.APIFLDS
    assert captured["operation"] == Operation.FIELD_INFO
    assert captured["backend"] is None
    assert captured["kwargs"] == {"fields": ["PX_LAST"], "_raw": True}
    assert result == [{"field": "PX_LAST"}]


@pytest.mark.asyncio
async def test_abflds_routes_field_search_for_search_spec(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return [{"field": "VWAP"}]

    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abflds(search_spec="vwap")

    assert captured["service"] == Service.APIFLDS
    assert captured["operation"] == Operation.FIELD_SEARCH
    assert captured["backend"] is None
    assert captured["kwargs"] == {"fields": ["vwap"], "extractor": ExtractorHint.FIELD_INFO, "_raw": True}
    assert result == [{"field": "VWAP"}]


@pytest.mark.asyncio
async def test_abflds_validates_mutually_exclusive_inputs():
    with pytest.raises(ValueError, match="Cannot specify both 'fields' and 'search_spec'"):
        await blp.abflds(fields="PX_LAST", search_spec="vwap")

    with pytest.raises(ValueError, match="Must specify either 'fields' or 'search_spec'"):
        await blp.abflds()
