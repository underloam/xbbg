"""Live verification for #317 native datetime/date acceptance.

Existing live tests already cover string-shaped date inputs across all surfaces
(bdh, bdib, bdtick, bdp overrides, bonds.settle_dt, options.expiry_dt, etc.).
This file ONLY exercises the new native-type paths added in #317:

- ``date`` / ``datetime`` objects as request params
- ``date`` / ``datetime`` values inside override kwargs
- tz-aware datetime in bdtick

Each test issues a small bounded Bloomberg request to keep traffic light.

Run with:

    pytest py-xbbg/tests/live/test_issue_317_native_dates.py -s -v
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from xbbg import blp


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    day = date(year, month, 1)
    while day.weekday() != weekday:
        day += timedelta(days=1)
    return day + timedelta(days=7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    day = date(year, month + 1, 1) - timedelta(days=1)
    while day.weekday() != weekday:
        day -= timedelta(days=1)
    return day


def _easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_offset = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_offset) // 451
    month = (h + weekday_offset - 7 * m + 114) // 31
    day = ((h + weekday_offset - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _is_us_equity_market_holiday(day: date) -> bool:
    year = day.year
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_date(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }
    return day in holidays


def _recent_trading_days(max_days: int = 14):
    for days_back in range(1, max_days + 1):
        candidate = date.today() - timedelta(days=days_back)
        if candidate.weekday() < 5 and not _is_us_equity_market_holiday(candidate):
            yield candidate


def _recent_weekday() -> date:
    """Pick the most recent weekday that is not a known US equity market holiday."""
    for day in _recent_trading_days():
        return day
    raise RuntimeError("No recent US equity trading day found in the last 14 calendar days")


def _recent_non_empty_bdib_frame():
    checked_dates: list[str] = []
    for day in _recent_trading_days():
        checked_dates.append(day.isoformat())
        probe = blp.bdib(
            "AAPL US Equity",
            dt=day.isoformat(),
            Points=1,
        )
        if len(probe) < 1:
            continue
        return day, blp.bdib(
            "AAPL US Equity",
            dt=day,
            Points=1,
        )
    pytest.skip("No non-empty AAPL BDIB rows found for recent trading days: " + ", ".join(checked_dates))


def _recent_non_empty_bdtick_frame(*, tz_aware: bool):
    checked_dates: list[str] = []
    for day in _recent_trading_days():
        checked_dates.append(day.isoformat())
        probe_start = f"{day.isoformat()}T14:30:00"
        probe_end = f"{day.isoformat()}T14:32:00"
        probe = blp.bdtick(
            "ES1 Index",
            start_datetime=probe_start,
            end_datetime=probe_end,
            event_types=["TRADE"],
            maxDataPoints=5,
        )
        if len(probe) < 1:
            continue
        if tz_aware:
            start = datetime.combine(day, datetime.min.time()).replace(hour=14, minute=30, tzinfo=UTC)
        else:
            start = datetime.combine(day, datetime.min.time()).replace(hour=14, minute=30)
        end = start + timedelta(minutes=2)
        return day, blp.bdtick(
            "ES1 Index",
            start_datetime=start,
            end_datetime=end,
            event_types=["TRADE"],
            maxDataPoints=5,
        )
    pytest.skip("No non-empty ES1 BDTICK rows found for recent trading days: " + ", ".join(checked_dates))


@pytest.mark.live
def test_bdh_accepts_date_objects():
    """bdh start_date/end_date accept datetime.date instead of strings."""
    end = _recent_weekday()
    start = end - timedelta(days=4)

    df = blp.bdh(
        "AAPL US Equity",
        "PX_LAST",
        start_date=start,
        end_date=end,
    )

    assert len(df) >= 1, f"bdh with date objects returned no rows: {df}"


@pytest.mark.live
def test_bdib_accepts_date_for_dt():
    """bdib dt= accepts a date object (date-only single-day shortcut)."""
    day, df = _recent_non_empty_bdib_frame()

    assert len(df) >= 1, f"bdib with date object returned no rows for {day}: {df}"


@pytest.mark.live
def test_bdtick_accepts_naive_datetime():
    """bdtick start/end accept naive datetime objects (tz-naive → UTC default)."""
    day, df = _recent_non_empty_bdtick_frame(tz_aware=False)

    assert len(df) >= 1, f"bdtick with naive datetimes returned no rows for {day}: {df}"


@pytest.mark.live
def test_bdtick_accepts_tz_aware_datetime():
    """bdtick start/end accept tz-aware datetime objects (preserves their tz)."""
    day, df = _recent_non_empty_bdtick_frame(tz_aware=True)

    assert len(df) >= 1, f"bdtick with tz-aware datetimes returned no rows for {day}: {df}"


@pytest.mark.live
def test_bdp_override_accepts_date_object():
    """Override kwarg values accept date objects (auto-normalized to YYYYMMDD)."""
    settle = _recent_weekday()

    df = blp.bdp(
        "IT0005045270 Corp",
        "SETTLE_DT",
        USER_LOCAL_TRADE_DATE=settle,
    )

    assert len(df) >= 1, (
        f"bdp with date-typed override returned no rows: {df}. "
        "If failing, the override-path normalization hook may not be converting "
        "datetime.date values to YYYYMMDD before forwarding to Bloomberg."
    )
