"""Request middleware primitives used by the public blp facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
import inspect
import time
from typing import Any, TypeAlias, cast

from xbbg.services import RequestParams

from .backend import Backend

DataFrameResult: TypeAlias = Any
RequestHandler: TypeAlias = Callable[["RequestContext"], Awaitable[DataFrameResult]]
RequestMiddleware: TypeAlias = Callable[
    ["RequestContext", RequestHandler],
    DataFrameResult | Awaitable[DataFrameResult],
]


@dataclass(frozen=True, slots=True)
class RequestEnvironment:
    """Read-only engine and auth snapshot available to request middleware."""

    source: str
    host: str | None = None
    port: int | None = None
    servers: tuple[tuple[str, int], ...] = ()
    zfp_remote: str | None = None
    auth_method: str | None = None
    app_name: str | None = None
    user_id: str | None = None
    validation_mode: str | None = None


@dataclass(slots=True)
class RequestContext:
    """Mutable context object passed through the request middleware chain."""

    request_id: str
    params: RequestParams
    backend: Backend | str | None
    raw: bool
    securities: list[str]
    fields: list[str]
    environment: RequestEnvironment
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.perf_counter)
    elapsed_ms: float | None = None
    batch: Any | None = None
    table: Any | None = None
    frame: DataFrameResult | None = None
    error: Exception | None = None

    def to_dispatch_dict(self) -> dict[str, Any]:
        """Materialize the current request state immediately before engine dispatch."""
        params_dict = self.params.to_dict()
        params_dict["request_id"] = self.request_id
        return params_dict


_request_middleware: list[RequestMiddleware] = []


async def _await_request_value(value: DataFrameResult | Awaitable[DataFrameResult]) -> DataFrameResult:
    if inspect.isawaitable(value):
        return cast("DataFrameResult", await value)
    return value


def add_middleware(middleware: RequestMiddleware) -> RequestMiddleware:
    """Register a request middleware callable.

    Middleware is called as ``middleware(context, call_next)`` and may be sync or async.
    Returning the middleware makes this usable as a decorator.
    """
    _request_middleware.append(middleware)
    return middleware


def remove_middleware(middleware: RequestMiddleware) -> None:
    """Remove a previously registered middleware callable."""
    _request_middleware.remove(middleware)


def clear_middleware() -> None:
    """Remove all registered middleware."""
    _request_middleware.clear()


def get_middleware() -> tuple[RequestMiddleware, ...]:
    """Return the currently registered middleware chain."""
    return tuple(_request_middleware)


def set_middleware(middleware: Sequence[RequestMiddleware]) -> None:
    """Replace the current middleware chain."""
    _request_middleware[:] = list(middleware)


async def run_request_middleware(
    context: RequestContext,
    terminal: RequestHandler,
) -> DataFrameResult:
    # One snapshot per request: middleware added or removed while this request
    # runs only affects later requests.
    chain = tuple(_request_middleware)

    async def invoke(index: int, current_context: RequestContext) -> DataFrameResult:
        if index >= len(chain):
            return await terminal(current_context)

        middleware = chain[index]

        async def call_next(next_context: RequestContext) -> DataFrameResult:
            return await invoke(index + 1, next_context)

        try:
            return await _await_request_value(middleware(current_context, call_next))
        except Exception as exc:
            current_context.error = exc
            raise

    return await invoke(0, context)
