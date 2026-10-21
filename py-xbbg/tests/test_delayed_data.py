"""Request-side delayed-data checks: policy validation, warnings, and delay_status."""

from __future__ import annotations

import json
from typing import Any
import warnings

import pytest

from xbbg import _core, _engine, _request_options, blp
from xbbg.exceptions import BlpDelayedDataWarning, BlpValidationError
from xbbg.services import ExtractorHint, Operation, RequestParams, Service

DELAYED = {
    "VOD LN Equity": {"delayed": True, "delay_minutes": 15},
    "SYNTHETIC XV Equity": {"delayed": True, "delay_minutes": None},
    "AAPL US Equity": {"delayed": False, "delay_minutes": None},
}
DELAYED_MESSAGES = [
    "VOD LN Equity: Bloomberg is delivering delayed data (15 minutes behind real time)",
    "SYNTHETIC XV Equity: Bloomberg is delivering delayed data",
]


def _delayed_result() -> Any:
    class ArrowTable:
        """Native result stand-in; ensure_arrow_table recognizes xbbg carriers by class name."""

        def __init__(self) -> None:
            self._table = _core.ArrowTable.from_pylist([{"ticker": ticker} for ticker in DELAYED])
            self.num_rows = self._table.num_rows
            self.metadata = {"xbbg.delayed": json.dumps(DELAYED)}
            self.delayed = DELAYED

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self._table.__arrow_c_stream__(requested_schema)

    return ArrowTable()


@pytest.fixture
def dispatched(monkeypatch) -> list[dict[str, Any]]:
    """Run bdp through the real request path against an engine reporting delayed tickers."""
    requests: list[dict[str, Any]] = []

    class FakeEngine:
        async def resolve_field_types(self, fields, overrides, default_type):
            return overrides or dict.fromkeys(fields, default_type)

        async def request(self, params):
            requests.append(params)
            return _delayed_result()

    async def route_kwargs(_service, _operation, _kwargs):
        return [], []

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(_request_options, "_aroute_kwargs", route_kwargs)
    return requests


def _delayed_warnings(caught: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [item for item in caught if issubclass(item.category, BlpDelayedDataWarning)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call",
    [
        lambda policy: blp.abdp(list(DELAYED), "PX_LAST", backend="native", on_delayed=policy),
        lambda policy: blp.abdh(list(DELAYED), "PX_LAST", backend="native", on_delayed=policy),
        lambda policy: blp.abdib("VOD LN Equity", dt="2024-01-02", backend="native", on_delayed=policy),
        lambda policy: blp.abdtick(
            "VOD LN Equity", "2024-01-02T09:30:00", "2024-01-02T10:00:00", backend="native", on_delayed=policy
        ),
    ],
    ids=["bdp", "bdh", "bdib", "bdtick"],
)
async def test_warn_policy_emits_one_warning_per_delayed_ticker_at_the_caller(dispatched, call):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await call("Warn")

    emitted = _delayed_warnings(caught)
    assert dispatched[0]["on_delayed"] == "warn"
    assert [str(item.message) for item in emitted] == DELAYED_MESSAGES
    assert all(item.category is BlpDelayedDataWarning for item in emitted)
    assert all(item.filename == __file__ for item in emitted)


def test_sync_warnings_point_at_the_sync_caller(dispatched):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        blp.bdp(list(DELAYED), "PX_LAST", backend="native", on_delayed="warn")

    emitted = _delayed_warnings(caught)
    assert [str(item.message) for item in emitted] == DELAYED_MESSAGES
    assert all(item.filename == __file__ for item in emitted)


@pytest.mark.asyncio
async def test_ignore_policy_emits_no_delayed_warnings(dispatched):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await blp.abdp(list(DELAYED), "PX_LAST", backend="native", on_delayed="ignore")

    assert dispatched[0]["on_delayed"] == "ignore"
    assert _delayed_warnings(caught) == []


@pytest.mark.parametrize("on_delayed", ["always", "warning", ""])
def test_request_params_rejects_unknown_on_delayed_policies(on_delayed):
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["VOD LN Equity"],
        fields=["PX_LAST"],
        on_delayed=on_delayed,
    )

    with pytest.raises(BlpValidationError, match="on_delayed must be warn, raise, or ignore"):
        params.validate()


@pytest.mark.parametrize(
    ("operation", "request_operation", "extractor"),
    [
        (Operation.REFERENCE_DATA, None, ExtractorHint.BULK),
        (Operation.BQL_SEND_QUERY, None, None),
        (Operation.RAW_REQUEST, Operation.FIELD_INFO, None),
    ],
)
def test_request_params_rejects_on_delayed_outside_bdp_bdh_bdib_bdtick(operation, request_operation, extractor):
    params = RequestParams(
        service=Service.REFDATA,
        operation=operation,
        request_operation=request_operation,
        extractor=extractor,
        on_delayed="warn",
    )

    with pytest.raises(BlpValidationError, match="on_delayed is only supported"):
        params.validate()


def test_delay_status_returns_the_recipe_result_in_the_requested_backend(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    engine = object()
    calls: list[tuple[Any, ...]] = []
    row = {"ticker": "VOD LN Equity", "delayed": True, "delay_minutes": 15}

    async def recipe_delay_status(*args):
        calls.append(args)
        return _core.ArrowTable.from_pylist([row]).to_record_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    monkeypatch.setattr(_core, "recipe_delay_status", recipe_delay_status)

    result = blp.delay_status("VOD LN Equity", check_identity=True, backend="pyarrow")

    assert calls == [(engine, ["VOD LN Equity"], True)]
    assert isinstance(result, pa.Table)
    assert result.to_pylist() == [row]
