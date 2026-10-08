"""Native recipe dispatch and request-option marshaling contracts."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from xbbg.ext import _utils
from xbbg.services import Format


@pytest.mark.asyncio
async def test_recipe_dispatch_normalizes_options_before_native_call(monkeypatch):
    from xbbg import _core, _engine

    engine = object()
    batch = object()
    native = AsyncMock(return_value=batch)
    monkeypatch.setattr(_core, "recipe_yas", native)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    monkeypatch.setattr(_utils, "_convert_result_backend", lambda value, backend: (value, backend))

    result = await _utils._call_native_recipe(
        "recipe_yas",
        ["ABC Govt"],
        ["YAS_BOND_YLD"],
        backend="native",
        request_options={
            "overrides": {"SETTLE_DT": date(2024, 1, 2)},
            "security_overrides": [("ABC Govt", [("CUSTOM", 2)])],
            "elements": {"Per": "W"},
            "options": [("includeConditionCodes", True)],
            "format": Format.LONG,
            "validate_fields": False,
            "return_eids": True,
            "field_types": {"YAS_BOND_YLD": "float64"},
            "request_tz": "UTC",
            "output_tz": "UTC",
            "CUSTOM_OVERRIDE": date(2024, 1, 3),
        },
    )

    assert result == (batch, "native")
    native.assert_awaited_once_with(
        engine,
        ["ABC Govt"],
        ["YAS_BOND_YLD"],
        request_options={
            "overrides": [("SETTLE_DT", "20240102")],
            "security_overrides": [("ABC Govt", [("CUSTOM", "2")])],
            "elements": [("periodicitySelection", "WEEKLY")],
            "options": [("includeConditionCodes", "True")],
            "format": "long",
            "validate_fields": False,
            "return_eids": True,
            "field_types": {"YAS_BOND_YLD": "float64"},
            "request_tz": "UTC",
            "output_tz": "UTC",
            "kwargs": {"CUSTOM_OVERRIDE": "20240103"},
        },
    )


@pytest.mark.asyncio
async def test_existing_recipes_do_not_receive_new_options_unless_requested(monkeypatch):
    from xbbg import _core, _engine

    native = AsyncMock(return_value=object())
    engine = object()
    monkeypatch.setattr(_core, "recipe_index_members", native)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    monkeypatch.setattr(_utils, "_convert_result_backend", lambda value, _backend: value)
    await _utils._call_native_recipe("recipe_index_members", "SPX Index", None, None)
    native.assert_awaited_once_with(engine, "SPX Index", None, None)


def test_recipe_request_options_accepts_security_override_mappings():
    assert _utils._recipe_request_options({"security_overrides": {"ABC Govt": {"SETTLE_DT": date(2024, 1, 2)}}}) == {
        "security_overrides": [("ABC Govt", [("SETTLE_DT", "20240102")])]
    }
