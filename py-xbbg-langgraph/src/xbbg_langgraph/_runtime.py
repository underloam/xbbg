"""LangChain invocation and xbbg engine scoping, without eager native imports."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict

from .options import BloombergToolsOptions
from .results import create_tool_result


class ToolInput(BaseModel):
    """Reject parameters outside the advertised tool contract."""

    model_config = ConfigDict(extra="forbid")


def make_tool(
    name: str,
    description: str,
    schema: type[BaseModel],
    handler: Callable[[Any], Awaitable[Any]],
    options: BloombergToolsOptions,
) -> StructuredTool:
    """Build synchronous and asynchronous entry points sharing one implementation."""
    if name in options.disabled_tools:
        raise ValueError(f"Tool {name!r} is disabled")

    async def invoke_async(**kwargs: Any) -> tuple[str, dict[str, Any]]:
        # Revalidate the selected operation and populate its typed defaults;
        # action schemas serialize only fields belonging to that operation.
        parsed = schema.model_validate(kwargs)
        with options.engine if options.engine is not None else nullcontext():
            value = await asyncio.wait_for(handler(parsed), timeout=options.request_timeout)
        return create_tool_result(name, value, options)

    def invoke_sync(**kwargs: Any) -> tuple[str, dict[str, Any]]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Use await tool.ainvoke(...) inside a running event loop")
        return asyncio.run(invoke_async(**kwargs))

    return StructuredTool.from_function(
        func=invoke_sync,
        coroutine=invoke_async,
        name=name,
        description=description,
        args_schema=schema,
        infer_schema=False,
        response_format="content_and_artifact",
    )
