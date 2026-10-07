"""Tests for validate_fields request plumbing."""

from __future__ import annotations

import pytest

from xbbg import _engine, _request_options, backend as backend_module
from xbbg._core import ArrowTable
from xbbg.services import Operation, RequestParams, Service


def test_request_params_to_dict_omits_validate_fields_when_none():
    """RequestParams.to_dict() should omit validate_fields when None."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    result = params.to_dict()

    assert "validate_fields" not in result


def test_request_params_to_dict_includes_validate_fields_when_set():
    """RequestParams.to_dict() should include validate_fields when explicitly set."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        validate_fields=False,
    )

    result = params.to_dict()

    assert result["validate_fields"] is False


@pytest.mark.asyncio
async def test_arequest_passes_validate_fields_to_engine(monkeypatch):
    """arequest() should pass validate_fields through to engine.request()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return ArrowTable.from_pylist(
                [
                    {"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"},
                ]
            ).to_batches()[0]

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        validate_fields=True,
    )

    assert captured["validate_fields"] is True
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_maps_native_validation_error_to_public_exception(monkeypatch):
    """Native validation errors should be catchable via xbbg.exceptions."""
    from xbbg import _core, blp
    from xbbg.exceptions import BlpValidationError

    class FakeEngine:
        async def request(self, _params_dict):
            raise _core.BlpValidationError("Configuration error: Unknown Bloomberg field(s): BAD_FIELD")

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    with pytest.raises(BlpValidationError, match="Unknown Bloomberg field"):
        await blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["BAD_FIELD"],
            validate_fields=True,
        )


@pytest.mark.asyncio
async def test_arequest_maps_native_request_error_to_public_exception(monkeypatch):
    """Native request errors should be catchable via the top-level public export."""
    from xbbg import BlpRequestError, _core, blp

    class FakeEngine:
        async def request(self, _params_dict):
            raise _core.BlpRequestError("Request failed: All securities failed")

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    with pytest.raises(BlpRequestError, match="All securities failed") as exc_info:
        await blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["BADSECURITY"],
            fields=["PX_LAST"],
        )

    assert type(exc_info.value) is BlpRequestError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("native_name", "public_name"),
    [
        ("BlpLimitError", "BlpLimitError"),
        ("BlpSecurityError", "BlpSecurityError"),
        ("BlpFieldError", "BlpFieldError"),
    ],
)
async def test_arequest_preserves_native_request_subclasses(monkeypatch, native_name, public_name):
    """Native request subclasses should normalize to matching public subclasses."""
    from xbbg import _core, blp
    import xbbg.exceptions as public_exceptions

    native_cls = getattr(_core, native_name)
    public_cls = getattr(public_exceptions, public_name)

    class FakeEngine:
        async def request(self, _params_dict):
            raise native_cls(f"{native_name}: specific request failure")

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    with pytest.raises(public_cls, match="specific request failure") as exc_info:
        await blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["BADSECURITY"],
            fields=["PX_LAST"],
        )

    assert type(exc_info.value) is public_cls
    assert isinstance(exc_info.value, public_exceptions.BlpRequestError)


@pytest.mark.asyncio
async def test_abdp_forwards_validate_fields(monkeypatch):
    """abdp() should forward validate_fields to arequest()."""
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

    result = await blp.abdp("IBM US Equity", "PX_LAST", validate_fields=True)

    assert captured["validate_fields"] is True
    assert len(result) == 1


def test_bdp_forwards_validate_fields(monkeypatch):
    """bdp() sync wrapper should forward validate_fields to arequest()."""
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

    result = blp.bdp("IBM US Equity", "PX_LAST", validate_fields=True)

    assert captured["validate_fields"] is True
    assert len(result) == 1
