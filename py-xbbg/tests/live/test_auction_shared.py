"""Small live auction/shared-feed checks; never require auction trading activity.

Run explicitly with an authorized Bloomberg connection. CI skips this module.
No test asserts a non-null auction value or waits for an exchange auction.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
import os
import warnings

import pytest

import xbbg
from xbbg import ext

pytestmark = pytest.mark.live

IBM = "IBM US Equity"
IBM_VENUE = "IBM UN Equity"
TIMEOUT = 20.0


@pytest.fixture
def engine() -> Iterator[xbbg.Engine]:
    """Keep exact feed counts independent from other live tests."""
    instance = xbbg.Engine(request_timeout_ms=15_000, num_start_attempts=1)
    try:
        yield instance
    finally:
        instance.shutdown()


async def _wait_until(predicate: Callable[[], bool]) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.05)

    await asyncio.wait_for(poll(), timeout=TIMEOUT)


def _ibm_feeds() -> list[dict]:
    return [row for row in xbbg.subscription_feeds() if row["service"] == "//blp/mktdata" and row["topic"] == IBM]


def test_resolve_primary_and_explicit_venues(engine):
    with engine:
        rows = ext.resolve_venues([IBM, "US0378331005", "SPY UP Equity"], backend="native").to_pylist()
    assert [(row["venue_topic"], row["status"], row["method"]) for row in rows] == [
        (IBM_VENUE, "resolved", "exchange_ticker"),
        ("AAPL UW Equity", "resolved", "exchange_ticker"),
        ("SPY UP Equity", "resolved", "as_is"),
    ]


def test_auction_snapshot_keeps_nullable_typed_columns(engine):
    pa = pytest.importorskip("pyarrow")
    fields = [
        "ORDER_IMB_BUY_VOLUME",
        "IN_AUCTION_RT",
        "IMBALANCE_INDIC_RT",
        "IMBALANCE_TIMESTAMP_RT",
        "THEORETICAL_TIME_TODAY_RT",
        "CLOSING_AUCTION_VOLUME_DATE_RT",
    ]
    with engine:
        result = ext.auction_snapshot(IBM, fields, backend="pyarrow")
    row = result.to_pylist()[0]
    assert row["security"] == IBM
    assert row["venue_topic"] == IBM_VENUE
    assert row["status"] == "resolved"
    assert result.column_names == ["input_order", "security", "venue_topic", "status", "error", *fields]
    assert result.schema.field("input_order").type == pa.int32()
    assert result.schema.field("ORDER_IMB_BUY_VOLUME").type == pa.float64()
    assert result.schema.field("IN_AUCTION_RT").type == pa.bool_()
    assert result.schema.field("IMBALANCE_INDIC_RT").type == pa.string()
    assert result.schema.field("IMBALANCE_TIMESTAMP_RT").type == pa.time64("us")
    assert result.schema.field("THEORETICAL_TIME_TODAY_RT").type == pa.time64("us")
    assert result.schema.field("CLOSING_AUCTION_VOLUME_DATE_RT").type == pa.date32()


@pytest.mark.asyncio
async def test_image_only_board_updates_latest_without_iteration(engine):
    securities = [IBM, "AAPL US Equity", "SPY US Equity"]
    with engine:
        sub = await asyncio.wait_for(
            xbbg.asubscribe(
                securities, ["LAST_PRICE", "BID", "ASK"], rows=False, stream_capacity=1, on_delayed="ignore"
            ),
            TIMEOUT,
        )
        try:
            await _wait_until(
                lambda: all(row["LAST_PRICE"] is not None for row in sub.latest(backend="native").to_pylist())
            )
            rows = sub.latest(backend="native").to_pylist()
            assert [row["topic"] for row in rows] == securities
            assert all(row["last_update"] is not None for row in rows)
            assert sub.stats["batches_sent"] == 0
            assert sub.stats["dropped_batches"] == 0
            assert sub.is_active
        finally:
            await asyncio.wait_for(sub.unsubscribe(), TIMEOUT)


@pytest.mark.asyncio
async def test_shared_feed_late_join_union_and_last_detach(engine):
    with engine:
        first = await asyncio.wait_for(xbbg.asubscribe(IBM, ["BID", "ASK"], on_delayed="ignore"), TIMEOUT)
        second = None
        try:
            await asyncio.wait_for(anext(first), TIMEOUT)
            second = await asyncio.wait_for(xbbg.asubscribe(IBM, ["ASK", "LAST_PRICE"], on_delayed="ignore"), TIMEOUT)
            image = await asyncio.wait_for(anext(second), TIMEOUT)
            row = image.to_pylist()[0]
            assert row["topic"] == IBM
            assert row["MKTDATA_EVENT_TYPE"] == "SUMMARY"
            assert row["MKTDATA_EVENT_SUBTYPE"] == "INITPAINT"
            feeds = _ibm_feeds()
            assert len(feeds) == 1
            assert feeds[0]["consumers"] == 2
            assert {"BID", "ASK", "LAST_PRICE"} <= set(feeds[0]["fields"])
        finally:
            try:
                if second is not None:
                    await asyncio.wait_for(second.unsubscribe(), TIMEOUT)
            finally:
                await asyncio.wait_for(first.unsubscribe(), TIMEOUT)
        await _wait_until(lambda: not _ibm_feeds())


@pytest.mark.asyncio
async def test_add_fields_latest_image_and_delayed_flag(engine):
    with engine:
        sub = await asyncio.wait_for(xbbg.asubscribe([IBM, "AAPL US Equity"], "BID", on_delayed="ignore"), TIMEOUT)
        try:
            await _wait_until(lambda: all(isinstance(state["delayed"], bool) for state in sub.topic_states.values()))
            await asyncio.wait_for(sub.add_fields("ASK"), TIMEOUT)
            assert {"BID", "ASK"} <= set(sub.fields)
            latest = sub.latest(backend="native")
            assert {"topic", "last_update", "live", "delayed", "BID", "ASK"} <= set(latest.column_names)
            rows = latest.to_pylist()
            assert [row["topic"] for row in rows] == [IBM, "AAPL US Equity"]
            assert all(isinstance(row["delayed"], bool) for row in rows)
        finally:
            await asyncio.wait_for(sub.unsubscribe(), TIMEOUT)


@pytest.mark.asyncio
async def test_rejected_static_field_emits_warning_and_retains_error(engine):
    with engine:
        sub = await asyncio.wait_for(xbbg.asubscribe(IBM, ["BID", "PX_BID"], on_delayed="ignore"), TIMEOUT)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", xbbg.BlpFieldWarning)
                await _wait_until(lambda: "PX_BID" in sub.field_errors.get(IBM, {}))
                sub.latest(backend="native")
            assert "PX_BID" in sub.field_errors[IBM]
            field_warnings = [item for item in caught if issubclass(item.category, xbbg.BlpFieldWarning)]
            assert len(field_warnings) == 1
            assert "PX_BID" in str(field_warnings[0].message)
        finally:
            await asyncio.wait_for(sub.unsubscribe(), TIMEOUT)


@pytest.mark.asyncio
async def test_rejected_static_field_raise_fails_the_topic(engine):
    with engine:
        sub = await asyncio.wait_for(
            xbbg.asubscribe(IBM, ["LAST_PRICE", "PX_BID"], on_field_error="raise", on_delayed="ignore"), TIMEOUT
        )
        try:
            await _wait_until(lambda: IBM in sub.failed_tickers)
            assert sub.field_errors[IBM]["PX_BID"] == "BAD_FLD"
            assert any(
                failure["ticker"] == IBM and "PX_BID" in failure["reason"] and "BAD_FLD" in failure["reason"]
                for failure in sub.failures
            )
            assert sub.all_failed
        finally:
            await asyncio.wait_for(sub.unsubscribe(), TIMEOUT)


@pytest.mark.asyncio
async def test_isolated_consumer_uses_separate_feed(engine):
    with engine:
        shared = await asyncio.wait_for(xbbg.asubscribe(IBM, "BID", on_delayed="ignore"), TIMEOUT)
        isolated = None
        try:
            isolated = await asyncio.wait_for(xbbg.asubscribe(IBM, "ASK", isolated=True, on_delayed="ignore"), TIMEOUT)
            feeds = _ibm_feeds()
            assert len(feeds) == 2
            assert sorted(row["isolated"] for row in feeds) == [False, True]
            assert all(row["consumers"] == 1 for row in feeds)
        finally:
            try:
                if isolated is not None:
                    await asyncio.wait_for(isolated.unsubscribe(), TIMEOUT)
            finally:
                await asyncio.wait_for(shared.unsubscribe(), TIMEOUT)
        await _wait_until(lambda: not _ibm_feeds())


@pytest.mark.asyncio
async def test_auction_subscription_preserves_source_label(engine):
    with engine:
        sub = await asyncio.wait_for(ext.asubscribe_auction(IBM, fields="BID", on_delayed="ignore"), TIMEOUT)
        try:
            first = await asyncio.wait_for(anext(sub), TIMEOUT)
            assert first.to_pylist()[0]["topic"] == IBM
            assert sub.topic_states[IBM]["feed_topic"] == IBM_VENUE
        finally:
            await asyncio.wait_for(sub.unsubscribe(), TIMEOUT)


@pytest.mark.asyncio
async def test_auction_latest_masks_observed_zero_price_sentinels(engine):
    fields = ["LAST_PRICE", *ext.AUCTION.ZERO_PRICE_FIELDS]
    with engine:
        unmasked = await asyncio.wait_for(
            ext.asubscribe_auction(
                IBM, fields, rows=False, zero_as_null=(), on_delayed="ignore", on_field_error="ignore"
            ),
            TIMEOUT,
        )
        masked = None
        try:
            masked = await asyncio.wait_for(
                ext.asubscribe_auction(IBM, fields, rows=False, on_delayed="ignore", on_field_error="ignore"),
                TIMEOUT,
            )
            deadline = asyncio.get_running_loop().time() + TIMEOUT
            while asyncio.get_running_loop().time() < deadline:
                before = unmasked.latest(backend="native").to_pylist()[0]
                image = masked.latest(backend="native").to_pylist()[0]
                after = unmasked.latest(backend="native").to_pylist()[0]
                if (
                    before["last_update"] is not None
                    and before["last_update"] == image["last_update"] == after["last_update"]
                ):
                    zeros = [field for field in ext.AUCTION.ZERO_PRICE_FIELDS if before[field] == 0]
                    if zeros:
                        assert all(after[field] == 0 for field in zeros)
                        assert all(image[field] is None for field in zeros)
                        return
                await asyncio.sleep(0.05)
            pytest.skip("No stable zero price sentinel observed within the bounded wait")
        finally:
            try:
                if masked is not None:
                    await asyncio.wait_for(masked.unsubscribe(), TIMEOUT)
            finally:
                await asyncio.wait_for(unmasked.unsubscribe(), TIMEOUT)


@pytest.mark.skipif(not os.environ.get("XBBG_LIVE_PFD_ISIN"), reason="optional preferred test input is not configured")
def test_preferred_venue_from_explicit_environment_input(engine):
    with engine:
        row = ext.resolve_venues(os.environ["XBBG_LIVE_PFD_ISIN"], backend="native").to_pylist()[0]
    assert row["kind"] == "pfd"
    assert row["status"] == "resolved"
    assert row["method"] in {"pcs_suffix", "as_is"}
    assert row["pricing_source"] != "EXCH"
