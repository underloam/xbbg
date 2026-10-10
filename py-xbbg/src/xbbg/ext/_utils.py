"""Shared utility functions for ext modules.

Request date normalization lives in ``xbbg._dates``; this module owns recipe
adapters and shared Bloomberg field-query boilerplate.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping, Sequence
import functools
from typing import Any, ParamSpec, TypeVar

from xbbg._dates import _fmt_date
from xbbg._sync import _run_sync
from xbbg.backend import _convert_result_backend

_P = ParamSpec("_P")
_T = TypeVar("_T")


def _syncify(async_func: Callable[_P, Coroutine[Any, Any, _T]]) -> Callable[_P, _T]:
    """Create a synchronous wrapper for an async ext helper.

    Ext sync helpers share the core ``xbbg.bdp`` boundary: run normally from
    synchronous code, use the notebook bridge inside running notebook loops,
    and fail clearly in other running event loops before creating an
    unawaited coroutine.
    """
    sync_name = async_func.__name__[1:] if async_func.__name__.startswith("a") else async_func.__name__

    @functools.wraps(async_func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        return _run_sync(sync_name, async_func, args, kwargs)

    wrapper.__name__ = sync_name
    wrapper.__qualname__ = sync_name
    return wrapper


async def _call_native_recipe(
    recipe_name: str, *args: Any, backend: Any = None, request_options: Mapping[str, Any] | None = None, **kwargs: Any
) -> Any:
    """Call a native recipe through the active engine and convert its Arrow output."""
    from xbbg import _core, _engine

    recipe = getattr(_core, recipe_name)
    if request_options is not None:
        kwargs["request_options"] = _recipe_request_options(request_options)
    engine = _engine._get_engine()
    batch = await recipe(engine, *args, **kwargs)
    return _convert_result_backend(batch, backend)


def _recipe_request_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Marshal request controls using the same vocabulary as the core endpoints."""
    from xbbg._request_options import (
        _normalize_element_alias,
        _normalize_override_value,
        _normalize_request_overrides,
    )

    pending = dict(options)
    result: dict[str, Any] = {}
    overrides, security_overrides = _normalize_request_overrides(pending.pop("overrides", None))
    if overrides:
        result["overrides"] = overrides
    if security_overrides:
        result["security_overrides"] = security_overrides
    explicit_security = pending.pop("security_overrides", None)
    if explicit_security:
        securities = explicit_security.items() if isinstance(explicit_security, Mapping) else explicit_security
        result.setdefault("security_overrides", []).extend(
            (
                str(ticker),
                [
                    (str(key), _normalize_override_value(value))
                    for key, value in (pairs.items() if isinstance(pairs, Mapping) else pairs)
                ],
            )
            for ticker, pairs in securities
        )
    for name in ("elements", "options"):
        values = pending.pop(name, None)
        if values:
            pairs = values.items() if isinstance(values, Mapping) else values
            result[name] = [
                (key, _normalize_override_value(value))
                for key, value in (_normalize_element_alias(str(key), value) for key, value in pairs)
            ]
    for name in (
        "field_types",
        "include_security_errors",
        "return_eids",
        "validate_fields",
        "request_tz",
        "output_tz",
        "request_id",
        "format",
    ):
        value = pending.pop(name, None)
        if value is not None:
            result[name] = value.value if hasattr(value, "value") else value
    if pending:
        result["kwargs"] = {
            key: _normalize_override_value(value)
            for key, value in (_normalize_element_alias(str(key), value) for key, value in pending.items())
            if value is not None
        }
    return result


async def _abdp_fields(
    tickers: str | Sequence[str],
    fields: str | Sequence[str],
    **kwargs,
) -> Any:
    """Run abdp with shared field-query boilerplate."""
    from xbbg.blp import abdp

    return await abdp(tickers=tickers, flds=fields, **kwargs)


async def _abds_field(
    tickers: str | Sequence[str],
    field: str,
    **kwargs,
) -> Any:
    """Run abds with shared field-query boilerplate."""
    from xbbg.blp import abds

    return await abds(tickers=tickers, flds=field, **kwargs)


def _apply_settle_override(overrides: dict, settle_dt) -> None:
    """Apply a settle date override to the overrides dict in place.

    If settle_dt is not None and can be formatted, sets overrides["SETTLE_DT"].

    Args:
        overrides: Mutable dict of Bloomberg overrides to update.
        settle_dt: Settlement date as string, date object, or None.
    """
    if settle_dt is not None:
        formatted_settle = _fmt_date(settle_dt)
        if formatted_settle is not None:
            overrides["SETTLE_DT"] = formatted_settle
