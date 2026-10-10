from __future__ import annotations

import sys
import types
from typing import Any

import pytest

import xbbg
from xbbg import _engine, blp
from xbbg.testing import (
    append_message_dict,
    create_mock_event,
    create_mock_response,
    deserialize_service,
    mock_engine,
)


@pytest.fixture(autouse=True)
def reset_blp_state():
    old_config = _engine._config
    old_engine = _engine._engine
    old_middleware = blp.get_middleware()
    blp.clear_middleware()
    _engine._config = None
    _engine._engine = None
    try:
        yield
    finally:
        blp.clear_middleware()
        blp.set_middleware(old_middleware)
        _engine._config = old_config
        _engine._engine = old_engine


def test_create_mock_response_builds_reference_rows_without_blpapi():
    response = create_mock_response(
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        data={"AAPL US Equity": {"PX_LAST": 254.23, "VOLUME": 100}},
    )

    assert response.service == "//blp/refdata"
    assert response.operation == "ReferenceDataRequest"
    assert response.event is None
    assert response.table.to_pylist() == [
        {"ticker": "AAPL US Equity", "field": "PX_LAST", "value": "254.23"},
        {"ticker": "AAPL US Equity", "field": "VOLUME", "value": "100"},
    ]


def test_mock_engine_intercepts_bdp_requests(monkeypatch):
    class UnexpectedEngine:
        async def resolve_field_types(self, fields, overrides, default_type):
            return {field: (overrides or {}).get(field, default_type) for field in fields}

        async def request(self, _params_dict):
            raise AssertionError("live engine should not be called")

    monkeypatch.setattr(_engine, "_get_engine", lambda *args, **kwargs: UnexpectedEngine())

    response = create_mock_response(
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        data={"AAPL US Equity": {"PX_LAST": 254.23}},
    )

    with mock_engine([response]):
        result = blp.bdp("AAPL US Equity", "PX_LAST")

    native = result.to_native()
    assert native.to_pylist() == [
        {"ticker": "AAPL US Equity", "field": "PX_LAST", "value": "254.23"},
    ]


def test_mock_engine_restores_middleware_after_exit():
    before = blp.get_middleware()
    response = create_mock_response(
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        data={"AAPL US Equity": {"PX_LAST": 254.23}},
    )

    with mock_engine([response]):
        assert len(blp.get_middleware()) == len(before) + 1

    assert blp.get_middleware() == before


def test_mock_engine_raises_for_unmatched_request(monkeypatch):
    class UnexpectedEngine:
        async def resolve_field_types(self, fields, overrides, default_type):
            return {field: (overrides or {}).get(field, default_type) for field in fields}

        async def request(self, _params_dict):
            raise AssertionError("live engine should not be called")

    monkeypatch.setattr(_engine, "_get_engine", lambda *args, **kwargs: UnexpectedEngine())

    response = create_mock_response(
        service="//blp/refdata",
        operation="HistoricalDataRequest",
        data={"AAPL US Equity": {"2024-01-01": {"PX_LAST": 254.23}}},
    )

    with mock_engine([response]), pytest.raises(LookupError, match="No mock response matched"):
        blp.bdp("AAPL US Equity", "PX_LAST")


def test_mock_engine_intercepts_generic_request(monkeypatch):
    class UnexpectedEngine:
        async def request(self, _params_dict):
            raise AssertionError("live engine should not be called")

    monkeypatch.setattr(_engine, "_get_engine", lambda *args, **kwargs: UnexpectedEngine())

    response = create_mock_response(
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        data={"IBM US Equity": {"PX_LAST": 123.45}},
    )

    with mock_engine([response]):
        result = blp.request(
            service="//blp/refdata",
            operation="ReferenceDataRequest",
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
        )

    native = result.to_native()
    assert native.to_pylist() == [
        {"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"},
    ]


def test_testutil_wrappers_use_blpapi_when_available(monkeypatch):
    calls: list[tuple[Any, ...]] = []

    class FakeFormatter:
        def formatMessageDict(self, payload):
            calls.append(("format", dict(payload)))

    fake_test = types.SimpleNamespace(
        createEvent=lambda event_type: calls.append(("createEvent", event_type)) or {"event_type": event_type},
        deserializeService=lambda xml: calls.append(("deserializeService", xml)) or {"xml": xml},
        appendMessage=lambda event, element_def, properties=None: (
            calls.append(("appendMessage", event, element_def, properties)) or FakeFormatter()
        ),
        getAdminMessageDefinition=lambda name: calls.append(("adminDef", name)) or {"name": name},
    )
    fake_blpapi = types.SimpleNamespace(
        test=fake_test, Name=lambda value: ("Name", value), Event=types.SimpleNamespace(RESPONSE=5)
    )
    monkeypatch.setitem(sys.modules, "blpapi", fake_blpapi)

    event = create_mock_event(5)
    service = deserialize_service("<xml />")
    element_def = ("element", "def")
    formatter = append_message_dict(event, element_def, {"value": 1})

    assert service == {"xml": "<xml />"}
    assert formatter is not None
    assert calls == [
        ("createEvent", 5),
        ("deserializeService", "<xml />"),
        ("appendMessage", {"event_type": 5}, element_def, None),
        ("format", {"value": 1}),
    ]


def test_testing_module_is_importable_from_package():
    assert "testing" not in xbbg.__all__ or hasattr(xbbg, "__all__")
    module = __import__("xbbg.testing", fromlist=["mock_engine"])
    assert hasattr(module, "mock_engine")
