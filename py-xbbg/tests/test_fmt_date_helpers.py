"""Unit tests for ``xbbg._dates._fmt_date`` and ``_fmt_datetime`` (#317).

These helpers are the single source of truth for normalizing user-supplied
date / datetime values across both the public ``blp.py`` API and the ``ext``
extension modules. They accept native Python types, duck-typed pandas
Timestamps, and strict ISO 8601 / Bloomberg-native strings, while rejecting
ambiguous month/day orderings.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from xbbg._dates import (
    _fmt_date,
    _fmt_datetime,
    _normalize_to_date,
    _normalize_to_datetime,
)


class _DuckTimestamp:
    """Minimal duck-typed pandas Timestamp without depending on pandas."""

    def __init__(self, dt: datetime) -> None:
        self._dt = dt

    def to_pydatetime(self) -> datetime:
        return self._dt


# ---------------------------------------------------------------------------
# _fmt_date
# ---------------------------------------------------------------------------


class TestFmtDateStrings:
    def test_iso_date_string(self) -> None:
        assert _fmt_date("2023-01-17") == "20230117"

    def test_bloomberg_native_string(self) -> None:
        assert _fmt_date("20230117") == "20230117"

    def test_iso_datetime_string_uses_date_portion(self) -> None:
        assert _fmt_date("2023-01-17T10:30:00") == "20230117"

    def test_today_lowercase(self) -> None:
        assert _fmt_date("today") == date.today().strftime("%Y%m%d")

    def test_today_uppercase(self) -> None:
        assert _fmt_date("TODAY") == date.today().strftime("%Y%m%d")

    def test_today_mixed_case_and_whitespace(self) -> None:
        assert _fmt_date("  Today  ") == date.today().strftime("%Y%m%d")

    def test_custom_format(self) -> None:
        assert _fmt_date("2023-01-17", fmt="%Y-%m-%d") == "2023-01-17"


class TestFmtDateNative:
    def test_date_object(self) -> None:
        assert _fmt_date(date(2023, 1, 17)) == "20230117"

    def test_datetime_naive(self) -> None:
        assert _fmt_date(datetime(2023, 1, 17, 10, 30)) == "20230117"

    def test_datetime_aware(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30, tzinfo=UTC)
        assert _fmt_date(dt) == "20230117"

    def test_duck_typed_timestamp(self) -> None:
        ts = _DuckTimestamp(datetime(2023, 1, 17, 10, 30))
        assert _fmt_date(ts) == "20230117"


class TestFmtDateNone:
    def test_none_returns_none_by_default(self) -> None:
        assert _fmt_date(None) is None

    def test_none_returns_today_when_default_today_on_none(self) -> None:
        assert _fmt_date(None, default_today_on_none=True) == date.today().strftime("%Y%m%d")

    def test_none_with_custom_format(self) -> None:
        assert _fmt_date(None, fmt="%Y-%m-%d", default_today_on_none=True) == date.today().strftime("%Y-%m-%d")


class TestFmtDateRejection:
    @pytest.mark.parametrize(
        "value",
        [
            "01/17/2023",
            "1/17/2023",
            "17-01-2023",
            "01-17-2023",
            "1/17/23",
        ],
    )
    def test_rejects_ambiguous(self, value: str) -> None:
        with pytest.raises(ValueError, match="Ambiguous"):
            _fmt_date(value)

    def test_rejects_garbage_string(self) -> None:
        with pytest.raises(ValueError):
            _fmt_date("not a date")

    def test_rejects_unsupported_type(self) -> None:
        with pytest.raises(TypeError):
            _fmt_date(12345)


class TestNormalizeToDate:
    def test_datetime(self) -> None:
        assert _normalize_to_date(datetime(2023, 1, 17, 10, 30)) == date(2023, 1, 17)

    def test_date(self) -> None:
        d = date(2023, 1, 17)
        assert _normalize_to_date(d) is d

    def test_duck(self) -> None:
        ts = _DuckTimestamp(datetime(2023, 1, 17))
        assert _normalize_to_date(ts) == date(2023, 1, 17)

    def test_unknown_raises(self) -> None:
        with pytest.raises(TypeError):
            _normalize_to_date(object())


# ---------------------------------------------------------------------------
# _fmt_datetime
# ---------------------------------------------------------------------------


class TestFmtDatetimeStrings:
    def test_iso_aware_string(self) -> None:
        assert _fmt_datetime("2023-01-17T10:30:00-05:00") == "2023-01-17T10:30:00-05:00"

    def test_iso_naive_string_default_utc(self) -> None:
        assert _fmt_datetime("2023-01-17T10:30:00") == "2023-01-17T10:30:00+00:00"

    def test_iso_naive_with_space(self) -> None:
        assert _fmt_datetime("2023-01-17 10:30:00") == "2023-01-17T10:30:00+00:00"

    def test_iso_naive_no_default_tz(self) -> None:
        assert _fmt_datetime("2023-01-17T10:30:00", default_tz=None) == "2023-01-17T10:30:00"

    def test_today_string(self) -> None:
        result = _fmt_datetime("today")
        assert result is not None
        assert result.startswith(date.today().isoformat())

    def test_bloomberg_native_string(self) -> None:
        assert _fmt_datetime("20230117") == "2023-01-17T00:00:00+00:00"


class TestFmtDatetimeNative:
    def test_naive_datetime_default_utc(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30)
        assert _fmt_datetime(dt) == "2023-01-17T10:30:00+00:00"

    def test_naive_datetime_no_tz(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30)
        assert _fmt_datetime(dt, default_tz=None) == "2023-01-17T10:30:00"

    def test_aware_datetime_preserves_tz(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30, tzinfo=timezone(timedelta(hours=-5)))
        assert _fmt_datetime(dt) == "2023-01-17T10:30:00-05:00"

    def test_aware_datetime_ignores_default_tz(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30, tzinfo=timezone(timedelta(hours=-5)))
        assert _fmt_datetime(dt, default_tz="UTC") == "2023-01-17T10:30:00-05:00"

    def test_date_object_treated_as_midnight(self) -> None:
        assert _fmt_datetime(date(2023, 1, 17)) == "2023-01-17T00:00:00+00:00"

    def test_duck_typed_timestamp(self) -> None:
        ts = _DuckTimestamp(datetime(2023, 1, 17, 10, 30, tzinfo=UTC))
        assert _fmt_datetime(ts) == "2023-01-17T10:30:00+00:00"


class TestFmtDatetimeIanaTz:
    def test_iana_zone(self) -> None:
        # Using New_York is fine because we don't depend on a tzdb on Windows;
        # zoneinfo will load it from the OS or tzdata package if available.
        result = _fmt_datetime(datetime(2023, 6, 15, 12, 0), default_tz="America/New_York")
        # We don't pin the offset (DST), but it must end with ±HH:MM and not 'Z'.
        assert result is not None
        assert result.startswith("2023-06-15T12:00:00")
        assert result[-6] in {"+", "-"}


class TestFmtDatetimeNone:
    def test_none(self) -> None:
        assert _fmt_datetime(None) is None


class TestFmtDatetimeRejection:
    @pytest.mark.parametrize(
        "value",
        [
            "01/17/2023 10:30:00",
            "01/17/2023T10:30:00",
            "17-01-2023 10:30:00",
        ],
    )
    def test_rejects_ambiguous(self, value: str) -> None:
        with pytest.raises(ValueError, match="Ambiguous"):
            _fmt_datetime(value)


class TestNormalizeToDatetime:
    def test_datetime(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30)
        assert _normalize_to_datetime(dt) is dt

    def test_date_at_midnight(self) -> None:
        assert _normalize_to_datetime(date(2023, 1, 17)) == datetime(2023, 1, 17)

    def test_duck(self) -> None:
        ts = _DuckTimestamp(datetime(2023, 1, 17, 10, 30))
        assert _normalize_to_datetime(ts) == datetime(2023, 1, 17, 10, 30)

    def test_unknown_raises(self) -> None:
        with pytest.raises(TypeError):
            _normalize_to_datetime(object())
