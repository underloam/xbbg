"""Shared utility functions for ext modules.

This module contains utility functions extracted from multiple ext modules
to eliminate duplication and improve maintainability.

Functions:
    - _pivot_bdp_to_wide(): Pivot bdp result from long to wide format
    - _fmt_date(): Format date to string (accepts str, date, datetime, duck-typed pd.Timestamp)
    - _fmt_datetime(): Format datetime to RFC 3339 string with tz handling
    - _apply_settle_override(): Apply settle date override to overrides dict
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from datetime import date, datetime, timezone
import functools
import re
from typing import Any, ParamSpec, TypeAlias, TypeVar

import narwhals.stable.v1 as nw

_P = ParamSpec("_P")
_T = TypeVar("_T")


_NON_WORD_RE = re.compile(r"[^0-9a-zA-Z]+")

# Public type alias used throughout the binding for date/datetime input.
# Accepts: ``str`` (ISO 8601, ``YYYYMMDD``, "today"), ``datetime.date``,
# ``datetime.datetime`` (naive or tz-aware), and duck-typed ``pd.Timestamp``
# (anything implementing ``to_pydatetime()``). ``None`` means "not provided".
DateLike: TypeAlias = "str | date | datetime | None"


# ISO 8601 date pattern: YYYY-MM-DD or YYYY/MM/DD (year-first only).
_ISO_DATE_RE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}$")
# Bloomberg compact form YYYYMMDD.
_BBG_DATE_RE = re.compile(r"^\d{8}$")
# US/EU-ambiguous patterns we explicitly reject (e.g. 01/17/2023, 17-01-2023, 1/17/23).
_AMBIGUOUS_DATE_RE = re.compile(r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$")
# Same pattern but allowed at the start of a datetime string (followed by 'T' or whitespace).
_AMBIGUOUS_DATETIME_PREFIX_RE = re.compile(r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}[T ]")


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
        from xbbg import blp

        return blp._run_sync(sync_name, async_func, args, kwargs)

    wrapper.__name__ = sync_name
    wrapper.__qualname__ = sync_name
    return wrapper


async def _call_native_recipe(recipe_name: str, *args: Any, backend: Any = None, **kwargs: Any) -> Any:
    """Call a native recipe through the active engine and convert its Arrow output."""
    from xbbg import _core, blp

    recipe = getattr(_core, recipe_name)
    engine = blp._get_engine()
    batch = await recipe(engine, *args, **kwargs)
    return blp._convert_result_backend(batch, backend)


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

    if native.__class__.__name__ == "ArrowRecordBatch" and hasattr(native, "__arrow_c_array__"):
        batch = native
    elif native.__class__.__name__ == "ArrowTable" and hasattr(native, "__arrow_c_stream__"):
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


def _normalize_to_date(value: Any) -> date:
    """Coerce a date-like value to a ``datetime.date``.

    Accepts:
        - ``datetime.datetime`` (returns its ``.date()``)
        - ``datetime.date``
        - Duck-typed ``pd.Timestamp`` (anything with ``to_pydatetime``)

    Raises ``TypeError`` for anything else.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        coerced = value.to_pydatetime()
        if isinstance(coerced, datetime):
            return coerced.date()
        if isinstance(coerced, date):
            return coerced
    raise TypeError(
        f"Cannot convert {type(value).__name__!r} value {value!r} to a date. "
        "Expected str (ISO 8601 / YYYYMMDD / 'today'), datetime.date, datetime.datetime, "
        "or a pandas Timestamp."
    )


def _normalize_to_datetime(value: Any) -> datetime:
    """Coerce a datetime-like value to a ``datetime.datetime``.

    Accepts:
        - ``datetime.datetime`` (returned as-is)
        - ``datetime.date`` (interpreted as midnight on that day)
        - Duck-typed ``pd.Timestamp`` (anything with ``to_pydatetime``)

    Raises ``TypeError`` for anything else.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if hasattr(value, "to_pydatetime"):
        coerced = value.to_pydatetime()
        if isinstance(coerced, datetime):
            return coerced
        if isinstance(coerced, date):
            return datetime(coerced.year, coerced.month, coerced.day)
    raise TypeError(
        f"Cannot convert {type(value).__name__!r} value {value!r} to a datetime. "
        "Expected str (ISO 8601), datetime.date, datetime.datetime, or a pandas Timestamp."
    )


def _parse_date_string(value: str) -> date:
    """Parse a date string accepting ISO 8601, ``YYYYMMDD``, or "today".

    Rejects ambiguous month/day ordering (e.g. ``"01/17/2023"``).
    """
    text = value.strip()
    if text.lower() == "today":
        return date.today()

    # Reject ambiguous formats where the year does not lead.
    # ISO date or Bloomberg-native must lead with a 4-digit year.
    if _AMBIGUOUS_DATE_RE.match(text) and not _ISO_DATE_RE.match(text):
        raise ValueError(
            f"Ambiguous date format {value!r}: month/day order cannot be inferred. "
            "Use ISO 8601 (YYYY-MM-DD), Bloomberg-native (YYYYMMDD), or pass a "
            "datetime.date / datetime.datetime object."
        )

    if _BBG_DATE_RE.match(text):
        try:
            return datetime.strptime(text, "%Y%m%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid Bloomberg-native date {value!r}: {exc}") from exc

    if _ISO_DATE_RE.match(text):
        normalized = text.replace("/", "-")
        try:
            return datetime.strptime(normalized, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid ISO date {value!r}: {exc}") from exc

    # Fall back to fromisoformat (handles datetime strings like 2023-01-17T10:30:00).
    try:
        return datetime.fromisoformat(text.replace(" ", "T")).date()
    except ValueError as exc:
        raise ValueError(
            f"Cannot parse {value!r} as a date. "
            "Expected ISO 8601 (YYYY-MM-DD), Bloomberg-native (YYYYMMDD), or 'today'."
        ) from exc


def _fmt_date(
    dt: Any,
    fmt: str = "%Y%m%d",
    *,
    default_today_on_none: bool = False,
) -> str | None:
    """Format a date-like value to a string.

    Accepts:
        - ``str``: ISO 8601 (``2023-01-17``), Bloomberg-native (``20230117``),
          ISO datetime (``2023-01-17T10:30:00``), or "today" (case-insensitive).
        - ``datetime.date``
        - ``datetime.datetime`` (date portion is used)
        - Duck-typed ``pd.Timestamp`` (via ``to_pydatetime``)

    Rejects ambiguous formats like ``"01/17/2023"`` (month/day order is unclear).

    Args:
        dt: The value to format.
        fmt: ``strftime`` format string (default ``"%Y%m%d"``).
        default_today_on_none: When True, ``None`` returns today's date in ``fmt``.
            When False (default), ``None`` returns ``None``.

    Returns:
        Formatted date string, or ``None`` if ``dt`` is ``None`` and
        ``default_today_on_none`` is False.
    """
    if dt is None:
        if default_today_on_none:
            return date.today().strftime(fmt)
        return None

    if isinstance(dt, str):
        parsed = _parse_date_string(dt)
        return parsed.strftime(fmt)

    return _normalize_to_date(dt).strftime(fmt)


def _fmt_datetime(
    value: Any,
    *,
    default_tz: str | None = "UTC",
) -> str | None:
    """Format a datetime-like value to an RFC 3339 string.

    Accepts the same input set as :func:`_fmt_date`, plus full ISO 8601
    datetime strings (with or without timezone). ``None`` returns ``None``.

    Timezone semantics:
        - ``datetime.datetime`` that is tz-naive: ``default_tz`` is applied.
        - ``datetime.datetime`` that is tz-aware: its tz is preserved.
        - ``str`` already carrying a tz suffix (``Z`` / ``+HH:MM``): tz preserved.
        - ``str`` without a tz suffix: ``default_tz`` is applied.
        - ``date`` (no time component): treated as midnight in ``default_tz``.
        - ``default_tz=None`` keeps tz-naive output for naive inputs.

    Args:
        value: The value to format.
        default_tz: Default timezone applied to naive inputs. Use ``None`` to
            leave naive inputs without a tz. Common values: ``"UTC"``,
            ``"local"`` (uses ``datetime.astimezone()`` to attach local tz).

    Returns:
        RFC 3339-formatted datetime string, or ``None`` if ``value`` is ``None``.
    """
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        if text.lower() == "today":
            base = datetime.combine(date.today(), datetime.min.time())
        else:
            normalized = text.replace(" ", "T", 1)
            ambiguous = (
                _AMBIGUOUS_DATE_RE.match(text) and not _ISO_DATE_RE.match(text)
            ) or _AMBIGUOUS_DATETIME_PREFIX_RE.match(text + "T")
            # Bloomberg-native YYYYMMDD: parse as midnight.
            if _BBG_DATE_RE.match(normalized):
                base = datetime.strptime(normalized, "%Y%m%d")
            elif ambiguous:
                raise ValueError(
                    f"Ambiguous datetime format {value!r}: month/day order cannot be inferred. "
                    "Use ISO 8601 (YYYY-MM-DDTHH:MM:SS) or pass a datetime.datetime object."
                )
            else:
                try:
                    base = datetime.fromisoformat(normalized)
                except ValueError as exc:
                    raise ValueError(
                        f"Cannot parse {value!r} as a datetime. "
                        "Expected ISO 8601 (e.g. '2023-01-17T10:30:00' or "
                        "'2023-01-17T10:30:00-05:00')."
                    ) from exc
    else:
        base = _normalize_to_datetime(value)

    # Apply default_tz only when the resulting datetime is naive.
    if base.tzinfo is None and default_tz is not None:
        if default_tz.upper() == "UTC":
            base = base.replace(tzinfo=timezone.utc)
        elif default_tz.lower() == "local":
            base = base.astimezone()
        else:
            try:
                from zoneinfo import ZoneInfo

                base = base.replace(tzinfo=ZoneInfo(default_tz))
            except Exception as exc:
                raise ValueError(
                    f"Unknown default_tz {default_tz!r}: {exc}. Use 'UTC', 'local', or an IANA zone name."
                ) from exc

    return base.isoformat()
