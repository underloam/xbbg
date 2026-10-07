"""Synthetic offline coverage for venue preflight and auction recipes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from xbbg import _core, _engine, blp
from xbbg._core import ArrowTable
from xbbg.exceptions import BlpValidationError
from xbbg.ext import AUCTION, auction, imbalance_side

FIRST = "SYNTHETIC1 US Equity"
SECOND = "SYNTHETIC2 US Equity"
FIRST_VENUE = "SYNTHETIC1 XV Equity"
SECOND_VENUE = "SYNTHETIC2 XW Equity"


def _row(order, security, topic, status="resolved", error=None):
    return {"input_order": order, "security": security, "venue_topic": topic, "status": status, "error": error}


def _patch_recipe(monkeypatch, rows):
    calls = []
    engine = object()

    async def recipe(native_engine, securities, pcs_overrides):
        assert native_engine is engine
        calls.append((securities, pcs_overrides))
        return ArrowTable.from_pylist(rows).to_record_batch()

    monkeypatch.setattr(_core, "recipe_resolve_venues", recipe)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    return calls


@pytest.mark.parametrize("entrypoint", ["asubscribe_auction", "subscribe_auction", "astream_auction", "stream_auction"])
@pytest.mark.parametrize(
    "fields,zero_as_null,expected",
    [
        (
            ["BID", "REFERENCE_PRICE_RT", "IMBALANCE_INDIC_RT", "THEO_PRICE"],
            None,
            ("THEO_PRICE", "REFERENCE_PRICE_RT"),
        ),
        (["BID", "IN_AUCTION_RT", "ORDER_IMB_BUY_VOLUME"], None, ()),
        (["THEO_PRICE", "REFERENCE_PRICE_RT"], (), ()),
        (["THEO_PRICE", "BID"], ("BID",), ("BID",)),
        (["THEO_PRICE", "BID"], "THEO_PRICE", ("THEO_PRICE",)),
    ],
)
def test_auction_public_policies_select_requested_sentinels_and_honor_overrides(
    monkeypatch, entrypoint, fields, zero_as_null, expected
):
    _patch_recipe(monkeypatch, [_row(0, FIRST, FIRST_VENUE)])
    calls = []

    async def subscribe(topics, requested, **kwargs):
        calls.append((topics, requested, kwargs))
        return object()

    async def astream(topics, requested, **kwargs):
        calls.append((topics, requested, kwargs))
        yield None

    def stream(topics, requested, **kwargs):
        calls.append((topics, requested, kwargs))
        yield None

    monkeypatch.setattr(blp, "asubscribe", subscribe)
    monkeypatch.setattr(blp, "astream", astream)
    monkeypatch.setattr(blp, "stream", stream)
    kwargs = {"on_field_error": "raise"}
    if zero_as_null is not None:
        kwargs["zero_as_null"] = zero_as_null
    if entrypoint in {"asubscribe_auction", "subscribe_auction"}:
        kwargs["rows"] = False

    async def run_async():
        if entrypoint == "asubscribe_auction":
            await auction.asubscribe_auction(FIRST, fields, **kwargs)
        else:
            source = auction.astream_auction(FIRST, fields, **kwargs)
            try:
                await anext(source)
            finally:
                await source.aclose()

    if entrypoint in {"asubscribe_auction", "astream_auction"}:
        asyncio.run(run_async())
    elif entrypoint == "subscribe_auction":
        auction.subscribe_auction(FIRST, fields, **kwargs)
    else:
        source = auction.stream_auction(FIRST, fields, **kwargs)
        try:
            next(source)
        finally:
            source.close()

    topics, requested, options = calls[0]
    assert topics == [FIRST_VENUE]
    assert requested == fields
    assert options["aliases"] == {FIRST_VENUE: FIRST}
    assert tuple(options["zero_as_null"]) == expected
    assert options["on_field_error"] == "raise"
    if entrypoint in {"asubscribe_auction", "subscribe_auction"}:
        assert options["rows"] is False
    else:
        assert "rows" not in options


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (" buy ", "buy"),
        ("MBUY", "buy"),
        ("RSEL", "sell"),
        ("NOIM", "none"),
        ("NIMB", "none"),
        ("INOR", None),
        ("NODS", None),
        ("", None),
    ],
)
def test_imbalance_side_does_not_conflate_unknown_with_no_imbalance(code, expected):
    assert imbalance_side(code) == expected


@pytest.mark.asyncio
async def test_subscribe_preflight_collapses_identical_inputs_and_labels_validated_venues(monkeypatch):
    recipe_calls = _patch_recipe(monkeypatch, [_row(0, FIRST, FIRST_VENUE), _row(1, SECOND, SECOND_VENUE)])
    calls = []
    native = object()

    async def subscribe(tickers, fields, **kwargs):
        calls.append((tickers, fields, kwargs))
        return native

    monkeypatch.setattr(blp, "asubscribe", subscribe)
    result = await auction.asubscribe_auction(
        [FIRST, SECOND, FIRST],
        AUCTION.QUOTES,
        pcs_overrides={"SYNTHETIC EXCHANGE": "XV"},
        on_delayed="raise",
        isolated=True,
    )
    assert result is native
    assert recipe_calls == [([FIRST, SECOND], {"SYNTHETIC EXCHANGE": "XV"})]
    topics, fields, options = calls[0]
    assert topics == [FIRST_VENUE, SECOND_VENUE]
    assert fields == list(AUCTION.QUOTES)
    assert options["aliases"] == {FIRST_VENUE: FIRST, SECOND_VENUE: SECOND}
    assert options["rows"] is True
    assert options["on_field_error"] == "warn"
    assert tuple(options["zero_as_null"]) == ()


@pytest.mark.parametrize("status", ["unresolved", "unsupported", "mismatch"])
@pytest.mark.parametrize("entrypoint", ["asubscribe_auction", "subscribe_auction", "astream_auction", "stream_auction"])
def test_any_failed_input_prevents_every_subscription(monkeypatch, status, entrypoint):
    _patch_recipe(
        monkeypatch, [_row(0, FIRST, FIRST_VENUE), _row(1, SECOND, SECOND_VENUE, status, "synthetic rejection")]
    )

    async def forbidden(*_args, **_kwargs):
        pytest.fail("a stream opened before the entire universe passed preflight")

    monkeypatch.setattr(blp, "asubscribe", forbidden)

    async def run_async():
        if entrypoint == "asubscribe_auction":
            await auction.asubscribe_auction([FIRST, SECOND])
        else:
            await anext(auction.astream_auction([FIRST, SECOND]))

    with pytest.raises(BlpValidationError, match=f"{SECOND}: synthetic rejection"):
        if entrypoint in ("asubscribe_auction", "astream_auction"):
            asyncio.run(run_async())
        elif entrypoint == "subscribe_auction":
            auction.subscribe_auction([FIRST, SECOND])
        else:
            next(auction.stream_auction([FIRST, SECOND]))


@pytest.mark.asyncio
async def test_distinct_inputs_resolving_to_same_venue_are_ambiguous(monkeypatch):
    _patch_recipe(monkeypatch, [_row(0, FIRST, FIRST_VENUE), _row(1, SECOND, FIRST_VENUE)])

    async def forbidden(*_args, **_kwargs):
        pytest.fail("ambiguous aliases must fail before subscription")

    monkeypatch.setattr(blp, "asubscribe", forbidden)
    with pytest.raises(BlpValidationError) as raised:
        await auction.asubscribe_auction([FIRST, SECOND])
    assert FIRST in str(raised.value)
    assert SECOND in str(raised.value)
    assert FIRST_VENUE in str(raised.value)


@pytest.mark.asyncio
async def test_snapshot_and_resolution_preserve_rows_and_backend(monkeypatch):
    rows = [_row(0, FIRST, FIRST_VENUE), _row(1, FIRST, FIRST_VENUE)]
    _patch_recipe(monkeypatch, rows)
    table = await auction.aresolve_venues([FIRST, FIRST], backend="pyarrow")
    assert table.to_pylist() == rows

    async def snapshot(_engine, securities, fields, pcs_overrides):
        assert securities == [FIRST, SECOND]
        assert fields == ["BID", "IN_AUCTION_RT"]
        assert pcs_overrides == {"SYNTHETIC EXCHANGE": "XV"}
        return ArrowTable.from_pylist(
            [
                {**_row(0, FIRST, FIRST_VENUE), "BID": 1.25, "IN_AUCTION_RT": True},
                {**_row(1, SECOND, SECOND_VENUE, "mismatch", "wrong venue"), "BID": None, "IN_AUCTION_RT": None},
            ]
        ).to_record_batch()

    monkeypatch.setattr(_core, "recipe_auction_snapshot", snapshot)
    result = await auction.aauction_snapshot(
        [FIRST, SECOND], ["BID", "IN_AUCTION_RT"], pcs_overrides={"SYNTHETIC EXCHANGE": "XV"}, backend="pyarrow"
    )
    assert result.to_pydict()["BID"] == [1.25, None]
    assert result.to_pydict()["IN_AUCTION_RT"] == [True, None]
    assert result.to_pydict()["status"] == ["resolved", "mismatch"]


@pytest.mark.parametrize("sync", [False, True])
def test_auction_stream_early_close_unsubscribes_after_atomic_routing(monkeypatch, sync):
    captured = []

    class Native:
        remaining = 1
        delivers_rows = True
        closed = False

        def take_warnings(self):
            return []

        async def __anext__(self):
            if not self.remaining:
                await asyncio.Event().wait()
            self.remaining -= 1
            return ArrowTable.from_pylist([{"ticker": FIRST, "BID": 1.25}]).to_record_batch()

        async def unsubscribe(self, drain, tick_mode):
            self.closed = True

    native = Native()

    async def subscribe(topics, fields, **kwargs):
        captured.append((topics, fields, kwargs))
        return native

    engine = SimpleNamespace(subscribe=subscribe)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)

    async def resolve(_engine, securities, pcs_overrides):
        return ArrowTable.from_pylist([_row(0, FIRST, FIRST_VENUE)]).to_record_batch()

    monkeypatch.setattr(_core, "recipe_resolve_venues", resolve)

    async def consume():
        source = auction.astream_auction(FIRST, "BID", on_delayed="ignore")
        await anext(source)
        await source.aclose()

    if sync:
        source = auction.stream_auction(FIRST, "BID", on_delayed="ignore")
        next(source)
        source.close()
    else:
        asyncio.run(consume())
    assert captured[0][0] == [FIRST_VENUE]
    assert captured[0][2]["aliases"] == {FIRST_VENUE: FIRST}
    assert native.closed
