"""Shared utility functions for ext modules.

Request date normalization lives in ``xbbg._dates``; this module owns recipe
adapters, native-result pivoting, and shared Bloomberg field-query boilerplate.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
import functools
import re
from typing import Any, ParamSpec, TypeVar

import narwhals.stable.v1 as nw

from xbbg._arrow import is_arrow_record_batch, is_arrow_table
from xbbg._dates import _fmt_date
from xbbg._sync import _run_sync
from xbbg.backend import _convert_result_backend

_P = ParamSpec("_P")
_T = TypeVar("_T")


_NON_WORD_RE = re.compile(r"[^0-9a-zA-Z]+")


def _canonical_column_name(name: str) -> str:
    """Return a wrapper-internal key for matching raw Bloomberg labels."""
    return _NON_WORD_RE.sub("_", name.strip().casefold()).strip("_")


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


async def _call_native_recipe(recipe_name: str, *args: Any, backend: Any = None, **kwargs: Any) -> Any:
    """Call a native recipe through the active engine and convert its Arrow output."""
    from xbbg import _core, _engine

    recipe = getattr(_core, recipe_name)
    engine = _engine._get_engine()
    batch = await recipe(engine, *args, **kwargs)
    return _convert_result_backend(batch, backend)


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


def _native_pivot_bdp_to_wide(nw_df):
    try:
        native = nw_df.to_native()
    except AttributeError:
        return None

    if is_arrow_record_batch(native):
        batch = native
    elif is_arrow_table(native):
        batch = native.to_record_batch()
    else:
        return None

    from xbbg._core import ext_pivot_to_wide

    return nw.from_native(ext_pivot_to_wide(batch).to_table())


def _pivot_bdp_to_wide(nw_df):
    """Pivot bdp result from long format (ticker, field, value) to wide format.

    If the dataframe already has the expected columns (not in long format),
    returns it unchanged.
    """
    # Check if already in wide format (has columns other than ticker/field/value)
    if set(nw_df.columns) != {"ticker", "field", "value"}:
        return nw_df

    if len(nw_df) == 0:
        return nw_df

    native_result = _native_pivot_bdp_to_wide(nw_df)
    if native_result is not None:
        return native_result

    # Pivot from long to wide: each unique field becomes a column
    # Group by ticker and create dict of field -> value
    rows_by_ticker: dict[str, dict[str, str]] = {}
    for row in nw_df.iter_rows(named=True):
        ticker = row["ticker"]
        field = row["field"]
        value = row["value"]
        if ticker not in rows_by_ticker:
            rows_by_ticker[ticker] = {"ticker": ticker}
        rows_by_ticker[ticker][field] = value

    # Build wide dataframe
    if not rows_by_ticker:
        return nw_df

    # Get all unique fields for column names
    all_fields = set()
    for row_data in rows_by_ticker.values():
        all_fields.update(k for k in row_data if k != "ticker")

    # Create lists for each column
    columns: dict[str, list[Any]] = {"ticker": []}
    for field in all_fields:
        columns[field] = []

    for ticker, row_data in rows_by_ticker.items():
        columns["ticker"].append(ticker)
        for field in all_fields:
            columns[field].append(row_data.get(field))

    # Create new dataframe using native namespace
    native_ns = nw.get_native_namespace(nw_df)
    result_cols = {k: nw.new_series(k, v, native_namespace=native_ns) for k, v in columns.items()}

    # Build dataframe from series
    first_series = next(iter(result_cols.values()))
    result_df = first_series.to_frame()
    for _name, series in list(result_cols.items())[1:]:
        result_df = result_df.with_columns(series)

    return result_df


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
