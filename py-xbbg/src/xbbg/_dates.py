"""Date and datetime normalization shared by requests and extension recipes."""

from __future__ import annotations

from datetime import date, datetime, timezone
import re
from typing import Any, TypeAlias

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
