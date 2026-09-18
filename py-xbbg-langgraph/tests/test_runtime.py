"""Observable LangChain invocation boundaries independent of a Bloomberg session."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

from pydantic import Field
import pytest

from xbbg_langgraph import BloombergToolsOptions, create_bdp_tool
from xbbg_langgraph._runtime import ToolInput, make_tool


class Input(ToolInput):
    value: int = Field(gt=0)


@pytest.mark.asyncio
async def test_timeout_restores_engine_scope_and_finishes_handler_cleanup():
    active = ContextVar("active", default="application")
    cleaned = asyncio.Event()

    class ScopedEngine:
        def __enter__(self):
            self.token = active.set("tool")
            return self

        def __exit__(self, *_args):
            active.reset(self.token)

    async def blocked(_input):
        assert active.get() == "tool"
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    tool = make_tool(
        "xbbg_bdp",
        "A bounded request",
        Input,
        blocked,
        BloombergToolsOptions(engine=ScopedEngine(), request_timeout=0.01),
    )
    with pytest.raises(asyncio.TimeoutError):
        await tool.ainvoke({"value": 1})
    assert cleaned.is_set()
    assert active.get() == "application"


@pytest.mark.asyncio
async def test_sync_invocation_inside_loop_does_not_start_a_request():
    # No Bloomberg import or connection is needed to reject the wrong entry point.
    with pytest.raises(RuntimeError, match="ainvoke"):
        create_bdp_tool().invoke({"securities": ["IBM US Equity"], "fields": ["PX_LAST"]})
