"""Per-request state must not leak between requests, engines, or schema refreshes."""

from __future__ import annotations

import asyncio
import warnings

import pytest

from xbbg import _engine, _request_options, request_middleware, schema
from xbbg.request_middleware import RequestContext, RequestEnvironment
from xbbg.services import Operation, RequestParams, Service


@pytest.fixture(autouse=True)
def isolated_middleware():
    saved = request_middleware.get_middleware()
    request_middleware.clear_middleware()
    try:
        yield
    finally:
        request_middleware.set_middleware(saved)


def _context(request_id: str) -> RequestContext:
    securities = ["SYNTHETIC1 Equity"]
    fields = ["PX_LAST"]
    return RequestContext(
        request_id=request_id,
        params=RequestParams(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=securities,
            fields=fields,
        ),
        backend=None,
        raw=False,
        securities=securities,
        fields=fields,
        environment=RequestEnvironment(source="test"),
    )


def test_middleware_removed_mid_request_keeps_the_request_chain():
    trace: list[str] = []

    async def scenario():
        paused = asyncio.Event()
        resume = asyncio.Event()

        async def middleware_a(context, call_next):
            trace.append("A")
            paused.set()
            await resume.wait()
            return await call_next(context)

        async def middleware_b(context, call_next):
            trace.append("B")
            return await call_next(context)

        async def terminal(context):
            trace.append("terminal")
            return context.request_id

        request_middleware.add_middleware(middleware_a)
        request_middleware.add_middleware(middleware_b)
        request = asyncio.create_task(request_middleware.run_request_middleware(_context("first"), terminal))
        await paused.wait()
        request_middleware.remove_middleware(middleware_a)
        resume.set()
        return await request

    assert asyncio.run(scenario()) == "first"
    assert trace == ["A", "B", "terminal"]


def test_middleware_added_mid_request_applies_to_the_next_request():
    trace: list[tuple[str, str]] = []

    async def scenario():
        paused = asyncio.Event()
        resume = asyncio.Event()

        async def middleware_a(context, call_next):
            trace.append((context.request_id, "A"))
            if context.request_id == "first":
                paused.set()
                await resume.wait()
            return await call_next(context)

        async def middleware_c(context, call_next):
            trace.append((context.request_id, "C"))
            return await call_next(context)

        async def terminal(context):
            trace.append((context.request_id, "terminal"))
            return context.request_id

        request_middleware.add_middleware(middleware_a)
        first = asyncio.create_task(request_middleware.run_request_middleware(_context("first"), terminal))
        await paused.wait()
        request_middleware.add_middleware(middleware_c)
        resume.set()
        await first
        await request_middleware.run_request_middleware(_context("second"), terminal)

    asyncio.run(scenario())
    assert trace == [
        ("first", "A"),
        ("first", "terminal"),
        ("second", "A"),
        ("second", "C"),
        ("second", "terminal"),
    ]


class SchemaEngine:
    """Fake engine whose schema gains an element once its cache is refreshed."""

    def __init__(self, elements: list[str], refreshed: list[str] | None = None):
        self.elements = list(elements)
        self.refreshed = list(refreshed if refreshed is not None else elements)

    async def list_valid_elements(self, _service, _operation):
        return list(self.elements)

    def invalidate_schema(self, _service):
        self.elements = list(self.refreshed)

    def clear_schema_cache(self):
        self.elements = list(self.refreshed)


def _warns_unknown_parameter() -> bool:
    """Route a lower-case element and report whether it was treated as unknown."""
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        elements, overrides = asyncio.run(
            _request_options._aroute_kwargs(
                Service.REFDATA,
                Operation.HISTORICAL_DATA,
                {"syntheticOption": "on"},
            )
        )
    assert elements == [("syntheticOption", "on")]
    assert overrides == []
    return any("Unknown parameter 'syntheticOption'" in str(entry.message) for entry in recorded)


@pytest.mark.parametrize("refresh", [lambda: schema.invalidate_schema("//blp/refdata"), schema.clear_schema_cache])
def test_schema_refresh_changes_element_routing(monkeypatch, refresh):
    engine = SchemaEngine(["otherOption"], refreshed=["otherOption", "syntheticOption"])
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)

    assert _warns_unknown_parameter()
    refresh()
    assert not _warns_unknown_parameter()


def test_each_engine_routes_with_its_own_schema(monkeypatch):
    engine_a = SchemaEngine(["otherOption"])
    engine_b = SchemaEngine(["otherOption", "syntheticOption"])
    active = [engine_a]
    monkeypatch.setattr(_engine, "_get_engine", lambda: active[0])

    assert _warns_unknown_parameter()
    active[0] = engine_b
    assert not _warns_unknown_parameter()
    active[0] = engine_a
    assert _warns_unknown_parameter()
