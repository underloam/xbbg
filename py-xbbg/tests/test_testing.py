from __future__ import annotations

import asyncio
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

pytestmark = pytest.mark.thread_unsafe(reason="mock_engine installs process-wide middleware and engine state")


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


# ---------------------------------------------------------------------------
# Diagnostics as values, served requests and offline engines
# ---------------------------------------------------------------------------

REFDATA = "//blp/refdata"
DIAGNOSTICS: dict[str, Any] = {
    "security_errors": {
        "NOTASECURITY1 Curncy": {
            "category": "BAD_SEC",
            "code": 15,
            "subcategory": "INVALID_SECURITY",
            "message": "Synthetic unknown security",
        }
    },
    "field_exceptions": {
        "TEST Equity": [
            {
                "field": "FIELD_B",
                "category": "BAD_FLD",
                "code": 9,
                "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                "message": "Synthetic field not applicable",
            },
            {
                "field": "FIELD_A",
                "category": "BAD_FLD",
                "code": 9,
                "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                "message": "Synthetic field not applicable",
            },
            {
                "field": "FIELD_B",
                "category": "BAD_FLD",
                "code": 9,
                "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                "message": "Synthetic field not applicable",
            },
        ]
    },
    "delayed": {
        "TEST Equity": {"delayed": True, "delay_minutes": 15},
        "TEST2 Equity": {"delayed": False, "delay_minutes": None},
    },
    "type_misses": {"FIELD_A": 2},
    "field_errors": {
        "REFUSED_ALIAS": {
            "category": "NO_AUTH",
            "code": -4,
            "subcategory": "NOT_ENTITLED",
            "message": "Synthetic refusal",
            "source": "apiflds@synthetic",
        }
    },
    "eid_data": {"TEST Equity": [14001, 14002]},
}
REQUEST_ID = "sdk-synthetic-1"
ACCESSORS = {
    "security_errors": "security_errors",
    "field_exceptions": "field_exceptions",
    "delayed": "delayed",
    "type_misses": "type_misses",
    "field_errors": "field_errors",
    "eid_data": "eid_data",
}
ATTRS = {name: f"xbbg_{name}" for name in ACCESSORS}


def _refdata_response(**diagnostics: Any):
    return create_mock_response(
        service=REFDATA,
        operation="ReferenceDataRequest",
        data={"TEST Equity": {"PX_LAST": 1.5}},
        **diagnostics,
    )


class _RefusesToConnect:
    @classmethod
    def with_config(cls, _config):
        raise AssertionError("an Engine under mock_engine must not connect")


def test_mock_response_metadata_is_the_native_encoders_output():
    from xbbg import _core

    response = _refdata_response(**DIAGNOSTICS, request_id=REQUEST_ID)

    expected = _core._encode_result_diagnostics(**DIAGNOSTICS, bloomberg_request_id=REQUEST_ID)
    assert response.table.metadata == expected
    assert set(expected) == {
        "xbbg.eid_data",
        "xbbg.security_errors",
        "xbbg.field_exceptions",
        "xbbg.delayed",
        "xbbg.type_misses",
        "xbbg.field_errors",
        "xbbg.bloomberg_request_id",
    }
    assert expected["xbbg.bloomberg_request_id"] == REQUEST_ID
    # Rows are untouched by the metadata.
    assert response.table.to_pylist() == [{"ticker": "TEST Equity", "field": "PX_LAST", "value": "1.5"}]


@pytest.mark.parametrize("diagnostic", sorted(DIAGNOSTICS))
def test_each_diagnostic_alone_writes_only_its_own_key(diagnostic):
    response = _refdata_response(**{diagnostic: DIAGNOSTICS[diagnostic]})

    assert list(response.table.metadata) == [f"xbbg.{diagnostic}"]
    assert getattr(response.table, ACCESSORS[diagnostic]) == DIAGNOSTICS[diagnostic]


def test_mock_response_without_diagnostics_has_no_metadata_and_a_request_id_has_one_key():
    assert _refdata_response().table.metadata == {}
    assert _refdata_response(request_id=REQUEST_ID).table.metadata == {"xbbg.bloomberg_request_id": REQUEST_ID}
    # Empty values leave their keys absent, as in engine results.
    assert _refdata_response(security_errors={}, type_misses=None, field_errors={}).table.metadata == {}


def test_mock_response_rejects_a_malformed_diagnostic_at_creation():
    with pytest.raises(ValueError, match="invalid security_errors"):
        _refdata_response(security_errors={"TEST Equity": {"category": "BAD_SEC"}})
    with pytest.raises(ValueError, match="invalid type_misses"):
        _refdata_response(type_misses={"FIELD_A": "many"})


@pytest.mark.parametrize("backend", ["native", "pyarrow", "pandas"])
def test_diagnostics_round_trip_through_mock_engine_unchanged(backend):
    response = _refdata_response(**DIAGNOSTICS, request_id=REQUEST_ID)
    written = dict(response.table.metadata)

    with mock_engine([response]):
        result = blp.bdp("TEST Equity", "PX_LAST", backend=backend)

    if backend == "native":
        for name, accessor in ACCESSORS.items():
            assert getattr(result, accessor) == DIAGNOSTICS[name], name
        assert result.bloomberg_request_id == REQUEST_ID
        assert result.bloomberg_request_ids == [REQUEST_ID]
        assert result.metadata == written
    elif backend == "pyarrow":
        metadata = {key.decode(): value.decode() for key, value in result.schema.metadata.items()}
        assert metadata == written
    else:
        for name, attr in ATTRS.items():
            assert result.attrs[attr] == DIAGNOSTICS[name], name
        assert result.attrs["xbbg_bloomberg_request_id"] == REQUEST_ID


def test_field_exception_occurrences_keep_their_order_and_duplicates():
    response = _refdata_response(field_exceptions=DIAGNOSTICS["field_exceptions"])

    occurrences = response.table.field_exceptions["TEST Equity"]
    assert [exception["field"] for exception in occurrences] == ["FIELD_B", "FIELD_A", "FIELD_B"]


def test_mock_engine_records_served_requests_in_order():
    refdata = _refdata_response()
    history = create_mock_response(
        service=REFDATA,
        operation="HistoricalDataRequest",
        data={"TEST Equity": {"2024-01-02": {"PX_LAST": 1.5}}},
    )

    with mock_engine([history, refdata]) as mock:
        assert mock.requests == []
        blp.bdp("TEST Equity", "PX_LAST")
        blp.bdh("TEST Equity", "PX_LAST", "2024-01-01", "2024-01-05")

    assert [(request.service, request.operation, request.served) for request in mock.requests] == [
        (REFDATA, "ReferenceDataRequest", True),
        (REFDATA, "HistoricalDataRequest", True),
    ]
    first, second = mock.requests
    assert first.params["securities"] == ["TEST Equity"]
    assert first.params["fields"] == ["PX_LAST"]
    assert second.params["start_date"] == "20240101"
    assert second.params["end_date"] == "20240105"
    assert first.params["request_id"] != second.params["request_id"]
    with pytest.raises(TypeError):
        first.params["securities"] = []  # type: ignore[index]


def test_mock_engine_records_requests_it_passes_through_when_not_strict(monkeypatch):
    class StubEngine:
        async def resolve_field_types(self, fields, overrides, default_type):
            return {field: (overrides or {}).get(field, default_type) for field in fields}

        async def request(self, _params_dict):
            return xbbg._core.ArrowTable.from_pylist([{"ticker": "TEST Equity", "field": "PX_LAST", "value": "9.0"}])

    monkeypatch.setattr(_engine, "_get_engine", lambda *args, **kwargs: StubEngine())

    with mock_engine([_refdata_response()], strict=False) as mock:
        blp.bdp("TEST Equity", "PX_LAST")
        blp.bdp("TEST2 Equity", "PX_OPEN")

    assert [(request.served, request.params["securities"]) for request in mock.requests] == [
        (True, ["TEST Equity"]),
        (False, ["TEST2 Equity"]),
    ]


def test_mock_engine_does_not_record_a_strict_mismatch():
    with mock_engine([_refdata_response()]) as mock, pytest.raises(LookupError):
        blp.bdh("TEST Equity", "PX_LAST", "2024-01-01", "2024-01-05")

    assert mock.requests == []


def test_engine_built_under_mock_engine_never_connects(monkeypatch):
    monkeypatch.setattr(xbbg._core, "PyEngine", _RefusesToConnect)
    response = _refdata_response(**DIAGNOSTICS, request_id=REQUEST_ID)

    with mock_engine([response]) as mock:
        engine = blp.Engine(host="a.example.com", port=8194)
        with engine:
            result = blp.bdp("TEST Equity", "PX_LAST", backend="native")
        engine.shutdown()

    assert result.to_pylist() == [{"ticker": "TEST Equity", "field": "PX_LAST", "value": "1.5"}]
    assert result.security_errors == DIAGNOSTICS["security_errors"]
    assert [request.operation for request in mock.requests] == ["ReferenceDataRequest"]


def test_offline_engine_scopes_async_and_reports_what_needs_bloomberg(monkeypatch):
    monkeypatch.setattr(xbbg._core, "PyEngine", _RefusesToConnect)

    with mock_engine([_refdata_response()]):
        engine = blp.Engine(host="a.example.com", port=8194)

        async def scoped_request():
            async with engine:
                return await blp.abdp("TEST Equity", "PX_LAST", backend="native")

        result = asyncio.run(scoped_request())
        with engine, pytest.raises(RuntimeError, match="mock_engine"):
            blp.subscription_feeds()

    assert result.to_pylist() == [{"ticker": "TEST Equity", "field": "PX_LAST", "value": "1.5"}]
    for call in (engine.worker_health, engine.subscription_feeds, engine.connections):
        with pytest.raises(RuntimeError, match="mock_engine"):
            call()
    # No-op, before or after the mock exits.
    engine.shutdown()


def test_unmatched_request_on_an_offline_engine_names_mock_engine(monkeypatch):
    monkeypatch.setattr(xbbg._core, "PyEngine", _RefusesToConnect)

    with mock_engine([], strict=False) as mock:
        engine = blp.Engine(host="a.example.com", port=8194)
        with engine, pytest.raises(RuntimeError, match="mock_engine") as raised:
            blp.bdp("TEST Equity", "PX_LAST")

    assert not isinstance(raised.value, AttributeError)
    assert [request.served for request in mock.requests] == [False]


def test_mock_engine_nesting_keeps_engines_offline_until_the_outermost_exits(monkeypatch):
    monkeypatch.setattr(xbbg._core, "PyEngine", _RefusesToConnect)

    with mock_engine([]):
        with mock_engine([]):
            inner = blp.Engine(host="a.example.com", port=8194)
        outer = blp.Engine(host="b.example.com", port=8194)
        assert _engine._mock_engine_active()
    assert not _engine._mock_engine_active()

    for engine in (inner, outer):
        with pytest.raises(RuntimeError, match="mock_engine"):
            engine.worker_health()
    with pytest.raises(AssertionError, match="must not connect"):
        blp.Engine(host="a.example.com", port=8194)


def test_mock_engine_releases_the_offline_flag_when_its_body_raises(monkeypatch):
    monkeypatch.setattr(xbbg._core, "PyEngine", _RefusesToConnect)

    with pytest.raises(KeyError), mock_engine([]):
        raise KeyError("body failed")

    assert not _engine._mock_engine_active()
    with pytest.raises(AssertionError, match="must not connect"):
        blp.Engine(host="a.example.com", port=8194)
