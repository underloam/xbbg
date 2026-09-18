"""Regression boundaries for calendar validation and owned subscriptions."""

from __future__ import annotations

import asyncio

from pydantic import ValidationError
import pytest

from xbbg_langgraph import create_bdh_tool, create_bdtick_tool
from xbbg_langgraph.core_tools import _collect_snapshot


def test_calendar_validation_rejects_impossible_days():
    schema = create_bdh_tool().args_schema
    with pytest.raises(ValidationError):
        schema.model_validate(
            {
                "securities": ["IBM US Equity"],
                "fields": ["PX_LAST"],
                "start": "2023-02-29",
                "end": "2023-03-01",
            }
        )
    validated = schema.model_validate(
        {
            "securities": ["IBM US Equity"],
            "fields": ["PX_LAST"],
            "start": 20240229,
            "end": "2024-03-01",
        }
    )
    assert validated.start == "20240229"


def test_intraday_ordering_compares_instants_not_offset_text():
    schema = create_bdtick_tool().args_schema
    with pytest.raises(ValidationError):
        schema.model_validate(
            {
                "ticker": "/isin/US4592001014",
                "start": "2024-01-02T09:00:00-05:00",
                "end": "2024-01-02T10:00:00+00:00",
            }
        )
    with pytest.raises(ValidationError):
        schema.model_validate(
            {
                "ticker": "/isin/US4592001014",
                "start": "2024-01-02T09:00:00+00:60",
                "end": "2024-01-02T10:00:00+00:00",
            }
        )


@pytest.mark.asyncio
async def test_snapshot_cancellation_waits_for_cleanup_despite_second_cancellation():
    reading, closing, release, closed = (asyncio.Event() for _ in range(4))

    class Subscription:
        async def __anext__(self):
            reading.set()
            await asyncio.Event().wait()

        async def unsubscribe(self, drain=False):
            assert drain is False
            closing.set()
            await release.wait()
            closed.set()

    task = asyncio.create_task(_collect_snapshot(Subscription(), max_updates=2, timeout_ms=10_000, drain=True))
    try:
        await asyncio.wait_for(reading.wait(), timeout=1)
        task.cancel()
        await asyncio.wait_for(closing.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.TimeoutError])
async def test_collection_errors_outrank_cleanup_errors(error_type):
    closed = False

    class Subscription:
        async def __anext__(self):
            raise error_type("Bloomberg collection failed")

        async def unsubscribe(self, drain=False):
            nonlocal closed
            closed = True
            raise ValueError("Subscription cleanup failed")

    with pytest.raises(error_type, match="Bloomberg collection failed"):
        await _collect_snapshot(Subscription(), max_updates=1, timeout_ms=1000)
    assert closed


@pytest.mark.asyncio
async def test_successful_collection_reports_cleanup_failure_without_losing_updates():
    class Subscription:
        async def __anext__(self):
            return {"price": 123.5}

        async def unsubscribe(self, drain=False):
            raise RuntimeError("Unable to close subscription")

    result = await _collect_snapshot(Subscription(), max_updates=1, timeout_ms=1000)
    assert result["reason"] == "max_updates"
    assert result["updates"] == [{"price": 123.5}]
    assert result["unsubscribeError"] == "Unable to close subscription"


@pytest.mark.asyncio
async def test_elapsed_snapshot_deadline_closes_subscription_and_pending_read():
    read_closed, subscription_closed = asyncio.Event(), asyncio.Event()

    class Subscription:
        async def __anext__(self):
            try:
                await asyncio.Event().wait()
            finally:
                read_closed.set()

        async def unsubscribe(self, drain=False):
            subscription_closed.set()

    result = await asyncio.wait_for(_collect_snapshot(Subscription(), max_updates=1, timeout_ms=10), timeout=1)
    assert result["reason"] == "timeout"
    assert read_closed.is_set()
    assert subscription_closed.is_set()
