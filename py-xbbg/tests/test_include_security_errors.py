"""Tests for request flag plumbing."""

from __future__ import annotations

from collections.abc import Sized

import pytest

from xbbg import _engine, _request_options, backend as backend_module
from xbbg._core import ArrowRecordBatch, ArrowTable
from xbbg.exceptions import BlpValidationError
from xbbg.services import Operation, RequestParams, Service


def _sample_batch() -> ArrowRecordBatch:
    return ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"},
        ]
    ).to_batches()[0]


def test_request_params_to_dict_omits_include_security_errors_when_false():
    """RequestParams.to_dict() should omit the flag when False."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    result = params.to_dict()

    assert "include_security_errors" not in result


def test_request_params_to_dict_includes_include_security_errors_when_true():
    """RequestParams.to_dict() should include the flag when True."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        include_security_errors=True,
    )

    result = params.to_dict()

    assert result["include_security_errors"] is True


def test_request_params_to_dict_omits_return_eids_when_false():
    """RequestParams.to_dict() should omit return_eids when False."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    result = params.to_dict()

    assert "return_eids" not in result


def test_request_params_to_dict_includes_return_eids_when_true():
    """RequestParams.to_dict() should include return_eids when True."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        return_eids=True,
    )

    result = params.to_dict()

    assert result["return_eids"] is True


def test_request_params_reports_missing_operation_before_eid_support():
    """Missing operations should retain their canonical validation error."""
    params = RequestParams(
        service=Service.REFDATA,
        operation="",
        return_eids=True,
    )

    with pytest.raises(BlpValidationError, match=r"^operation is required$"):
        params.validate()


@pytest.mark.parametrize(
    ("operation", "request_operation"),
    [
        (Operation.FIELD_INFO, None),
        ("CustomTypedRequest", None),
        (Operation.RAW_REQUEST, Operation.FIELD_INFO),
    ],
)
def test_request_params_rejects_return_eids_for_unsupported_effective_operation(operation, request_operation):
    """Typed and raw requests should reject EIDs outside the four supported operations."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=operation,
        request_operation=request_operation,
        return_eids=True,
    )

    with pytest.raises(BlpValidationError, match="return_eids is only supported"):
        params.validate()


def test_raw_reference_request_accepts_return_eids_escape_hatch():
    """RawRequest should validate return_eids against its effective Bloomberg operation."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.RAW_REQUEST,
        request_operation=Operation.REFERENCE_DATA,
        return_eids=True,
    )

    params.validate()

    assert params.to_dict()["return_eids"] is True


@pytest.mark.asyncio
async def test_arequest_passes_include_security_errors_to_engine(monkeypatch):
    """arequest() should pass include_security_errors=True to engine.request()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        include_security_errors=True,
    )

    assert captured["include_security_errors"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_omits_include_security_errors_when_false(monkeypatch):
    """arequest() should not include include_security_errors when False."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    assert "include_security_errors" not in captured
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_passes_return_eids_to_engine(monkeypatch):
    """arequest() should pass return_eids=True to engine.request()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        return_eids=True,
    )

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_omits_return_eids_when_false(monkeypatch):
    """arequest() should not include return_eids when False."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    assert "return_eids" not in captured
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdp_forwards_include_security_errors(monkeypatch):
    """abdp() should forward include_security_errors to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdp("IBM US Equity", "PX_LAST", include_security_errors=True)

    assert captured["include_security_errors"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdp_forwards_return_eids(monkeypatch):
    """abdp() should forward return_eids to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdp("IBM US Equity", "PX_LAST", return_eids=True)

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdh_forwards_return_eids(monkeypatch):
    """abdh() should forward return_eids to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "date": "2024-01-01", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdh("IBM US Equity", "PX_LAST", start_date="2024-01-01", return_eids=True)

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abds_forwards_return_eids(monkeypatch):
    """abds() should forward return_eids to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "DVD_HIST_ALL"}]

    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    await blp.abds("IBM US Equity", "DVD_HIST_ALL", return_eids=True)

    assert captured["return_eids"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("return_eids", [False, True])
@pytest.mark.parametrize(
    ("call", "expected_operation"),
    [
        (lambda blp, flag: blp.abds("IBM US Equity", "DVD_HIST_ALL", return_eids=flag), Operation.REFERENCE_DATA),
        (
            lambda blp, flag: blp.abdib("IBM US Equity", dt="2024-01-02", return_eids=flag),
            Operation.INTRADAY_BAR,
        ),
        (
            lambda blp, flag: blp.abdtick(
                "IBM US Equity",
                "2024-01-02T09:30:00",
                "2024-01-02T10:00:00",
                return_eids=flag,
            ),
            Operation.INTRADAY_TICK,
        ),
    ],
)
async def test_eid_capable_async_builders_forward_only_true(monkeypatch, call, expected_operation, return_eids):
    """EID-capable endpoint builders should forward true and omit false."""
    from xbbg import blp

    captured: dict[str, object] = {}

    async def fake_route_kwargs(_service, operation, _kwargs):
        assert operation == expected_operation
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(backend_module, "_convert_result_backend", lambda frame, _backend: frame)
    monkeypatch.setattr(blp, "arequest", fake_arequest)

    await call(blp, return_eids)

    if return_eids:
        assert captured["return_eids"] is True
    else:
        assert "return_eids" not in captured


def test_sync_eid_capable_signatures_expose_return_eids():
    """Sync BDS, BDIB, and BDTICK wrappers should expose the request flag."""
    import inspect

    from xbbg import blp

    for endpoint in (blp.bds, blp.bdib, blp.bdtick):
        parameter = inspect.signature(endpoint).parameters["return_eids"]
        assert parameter.default is False
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "request_kwargs"),
    [
        (
            Operation.INTRADAY_BAR,
            {
                "security": "IBM US Equity",
                "start_datetime": "2024-01-02T09:30:00",
                "end_datetime": "2024-01-02T10:00:00",
                "event_type": "TRADE",
                "interval": 1,
            },
        ),
        (
            Operation.INTRADAY_TICK,
            {
                "security": "IBM US Equity",
                "start_datetime": "2024-01-02T09:30:00",
                "end_datetime": "2024-01-02T10:00:00",
                "event_types": ["TRADE"],
            },
        ),
    ],
)
async def test_generic_intraday_requests_forward_return_eids(monkeypatch, operation, request_kwargs):
    """Generic intraday requests should carry return_eids to the engine."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    await blp.arequest(Service.REFDATA, operation, return_eids=True, **request_kwargs)

    assert captured["return_eids"] is True


@pytest.mark.asyncio
async def test_acheck_entitlements_forwards_extracted_eids_and_service(monkeypatch):
    """Entitlement checks should preserve extracted integer EIDs and service."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def check_entitlements(self, service, eids):
            captured["service"] = service
            captured["eids"] = eids
            return object()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    extracted = {"IBM US Equity": [101, 202]}

    await blp.acheck_entitlements(extracted["IBM US Equity"], "//blp/mktdata")

    assert captured == {"service": "//blp/mktdata", "eids": [101, 202]}


def test_bdp_forwards_include_security_errors(monkeypatch):
    """bdp() sync wrapper should forward include_security_errors to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(_request_options, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda df, _backend: df)

    assert callable(blp.bdp)
    result = blp.bdp("IBM US Equity", "PX_LAST", include_security_errors=True)

    assert captured["include_security_errors"] is True
    assert len(result) == 1
