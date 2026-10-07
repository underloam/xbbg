"""Offline shared-subscription facade behavior, including warning delivery."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any
import warnings

import pytest

import xbbg
from xbbg import _engine, _streaming, blp
from xbbg._core import ArrowTable
from xbbg.exceptions import BlpDelayedDataWarning, BlpFieldWarning, BlpSubscriptionWarning

TOPIC = "SYNTHETIC XV Equity"
LABEL = "synthetic source"


def _events():
    return [
        {"message_type": "DelayedStream", "topic": LABEL, "detail": "feed is delayed"},
        {"message_type": "FieldException", "topic": LABEL, "detail": "BAD_FIELD: BAD_FLD"},
    ]


class NativeSubscription:
    def __init__(self, *, pending=None, ticks=2, rows=True):
        self.pending = list(pending or [])
        self.remaining = ticks
        self.fields = ["BID"]
        self.tickers = [LABEL]
        self.added = []
        self.removed = []
        self.closed = False
        self.field_errors = {LABEL: {"BAD_FIELD": "BAD_FLD"}}
        self.topic_states = [(LABEL, "streaming", 123, True, TOPIC)]
        self.delivers_rows = rows

    def take_warnings(self):
        pending, self.pending = self.pending, []
        return pending

    def latest(self):
        return ArrowTable.from_pylist(
            [{"topic": LABEL, "last_update": None, "live": False, "delayed": True, "BID": 17.5}]
        ).to_record_batch()

    async def __anext__(self):
        if not self.remaining:
            raise StopAsyncIteration
        self.remaining -= 1
        return self.latest()

    async def __anext_tick_dict__(self):
        await self.__anext__()
        return {"ticker": LABEL, "BID": 17.5}

    async def add(self, tickers, aliases=None):
        self.added.append((tickers, aliases))

    async def add_fields(self, fields):
        self.fields.extend(field for field in fields if field not in self.fields)

    async def remove(self, tickers):
        self.removed.extend(tickers)

    async def unsubscribe(self, drain, tick_mode):
        self.closed = True
        return []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["batch", "tick", "add", "add_fields", "latest", "unsubscribe"])
async def test_warning_queue_emits_categories_and_messages_once_at_caller(operation):
    native = NativeSubscription(pending=_events())
    sub = blp.Subscription(native, raw=True, backend=None, tick_mode=operation == "tick")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(2):
            if operation in ("batch", "tick"):
                await anext(sub)
            elif operation == "add":
                await sub.add(TOPIC, aliases={TOPIC: LABEL})
            elif operation == "add_fields":
                await sub.add_fields("ASK")
            elif operation == "unsubscribe":
                await sub.unsubscribe()
            else:
                sub.latest(backend="native")
    assert [item.category for item in caught] == [BlpDelayedDataWarning, BlpFieldWarning]
    assert [str(item.message) for item in caught] == [f"{LABEL}: feed is delayed", f"{LABEL}: BAD_FIELD: BAD_FLD"]
    assert all(item.filename == __file__ for item in caught)
    assert all(isinstance(item.message, xbbg.BlpSubscriptionWarning) for item in caught)
    assert issubclass(BlpSubscriptionWarning, UserWarning)


@pytest.mark.parametrize("ticks", [0, 1])
def test_sync_stream_warns_on_consumer_thread_even_when_no_batch_arrives(monkeypatch, ticks):
    native = NativeSubscription(pending=_events(), ticks=ticks)

    async def subscribe(*_args, **_kwargs):
        return native

    monkeypatch.setattr(_engine, "_get_engine", lambda: SimpleNamespace(subscribe=subscribe))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        batches = list(blp.stream(TOPIC, "BID", raw=True))
    assert len(batches) == ticks
    assert [item.category for item in caught] == [BlpDelayedDataWarning, BlpFieldWarning]
    assert all(item.filename == __file__ for item in caught)
    assert native.closed


@pytest.mark.parametrize("close_mode", ["close", "throw"])
def test_sync_stream_delivers_warnings_generated_during_cleanup(monkeypatch, close_mode):
    waiting = threading.Event()

    class Native(NativeSubscription):
        async def __anext__(self):
            if self.remaining:
                return await super().__anext__()
            waiting.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.pending.extend(_events())

    native = Native(ticks=1)

    async def subscribe(*_args, **_kwargs):
        return native

    monkeypatch.setattr(_engine, "_get_engine", lambda: SimpleNamespace(subscribe=subscribe))
    source = blp.stream(TOPIC, "BID", raw=True)
    next(source)
    assert waiting.wait(timeout=2)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if close_mode == "close":
            source.close()
        else:
            error = RuntimeError("consumer failed")
            with pytest.raises(RuntimeError) as raised:
                source.throw(error)
            assert raised.value is error
    assert [item.category for item in caught] == [BlpDelayedDataWarning, BlpFieldWarning]
    assert all(item.filename == __file__ for item in caught)
    assert native.closed
    assert native.take_warnings() == []


@pytest.mark.asyncio
async def test_projection_growth_latest_conversion_and_aliased_remove():
    native = NativeSubscription()
    sub = blp.Subscription(native, raw=True, backend="pandas", topic_normalizer=_streaming._normalize_mktbar_topics)
    await sub.add("SYNTHETIC2 Equity", aliases={"SYNTHETIC2 Equity": "second source"})
    assert native.added == [
        (["//blp/mktbar/ticker/SYNTHETIC2 Equity"], {"//blp/mktbar/ticker/SYNTHETIC2 Equity": "second source"})
    ]
    await sub.add_fields(["ASK", "BID"])
    assert sub.fields == ["BID", "ASK"]
    await sub.remove(LABEL)
    assert native.removed == [LABEL]
    latest = sub.latest(backend="pyarrow")
    assert latest.to_pylist() == [{"topic": LABEL, "last_update": None, "live": False, "delayed": True, "BID": 17.5}]
    assert sub.field_errors == {LABEL: {"BAD_FIELD": "BAD_FLD"}}
    assert sub.topic_states[LABEL] == {
        "state": "streaming",
        "last_change_us": 123,
        "delayed": True,
        "feed_topic": TOPIC,
    }


@pytest.mark.parametrize("zero_as_null", [("BID",), "BID"])
@pytest.mark.parametrize(
    ("entrypoint", "with_options"),
    [
        ("asubscribe", False),
        ("asubscribe", True),
        ("subscribe", False),
        ("subscribe", True),
        ("astream", False),
        ("stream", False),
    ],
)
def test_shared_subscription_options_reach_native(monkeypatch, entrypoint, with_options, zero_as_null):
    calls = []
    native = NativeSubscription(ticks=1)

    async def subscribe(*args, **kwargs):
        calls.append((args, kwargs))
        native.delivers_rows = kwargs["rows"]
        return native

    engine = SimpleNamespace(subscribe=subscribe, subscribe_with_options=subscribe)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    kwargs: dict[str, Any] = {
        "aliases": {TOPIC: LABEL},
        "on_delayed": "ignore",
        "isolated": True,
        "on_field_error": "raise",
        "zero_as_null": zero_as_null,
    }
    if entrypoint in ("asubscribe", "subscribe"):
        kwargs["rows"] = False
    if with_options:
        kwargs["options"] = ["interval=1"]

    async def run_async():
        if entrypoint == "asubscribe":
            return await blp.asubscribe(TOPIC, "BID", **kwargs)
        return [batch async for batch in blp.astream(TOPIC, "BID", **kwargs)]

    if entrypoint in ("asubscribe", "astream"):
        asyncio.run(run_async())
    elif entrypoint == "subscribe":
        xbbg.subscribe(TOPIC, "BID", **kwargs)
    else:
        list(blp.stream(TOPIC, "BID", **kwargs))
    args, options = calls[0]
    expected_args = ("//blp/mktdata", [TOPIC], ["BID"], ["interval=1"]) if with_options else ([TOPIC], ["BID"])
    assert args == expected_args
    assert options["aliases"] == {TOPIC: LABEL}
    assert options["on_delayed"] == "ignore"
    assert options["isolated"] is True
    assert options["on_field_error"] == "raise"
    assert options["zero_as_null"] == ["BID"]
    assert options["rows"] is (entrypoint not in ("asubscribe", "subscribe"))
    assert options["session_wait_ms"] == (5000 if entrypoint == "stream" else None)


def test_feed_diagnostics_keep_nested_python_values_and_engine_scope(monkeypatch):
    row = {
        "service": "//blp/mktdata",
        "topic": TOPIC,
        "options": ["interval=1"],
        "fields": ["BID", "ASK"],
        "consumers": 2,
        "delayed": None,
        "state": "pending",
        "isolated": False,
        "field_errors": {"BAD_FIELD": "BAD_FLD"},
    }
    global_engine = SimpleNamespace(subscription_feeds=list)
    scoped = object.__new__(blp.Engine)
    monkeypatch.setattr(scoped, "_py_engine", SimpleNamespace(subscription_feeds=lambda: [row]), raising=False)
    monkeypatch.setattr(_engine, "_engine", global_engine)
    assert xbbg.subscription_feeds() == []
    token = _engine._active_engine.set(scoped)
    try:
        assert xbbg.subscription_feeds() == [row]
        assert scoped.subscription_feeds() == [row]
    finally:
        _engine._active_engine.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("tick_mode", [False, True])
async def test_image_only_handles_reject_iteration_but_keep_latest_available(tick_mode):
    native = NativeSubscription(rows=False)
    sub = blp.Subscription(native, raw=True, backend=None, tick_mode=tick_mode)
    with pytest.raises(RuntimeError, match=r"rows=False.*latest\(\)"):
        aiter(sub)
    with pytest.raises(RuntimeError, match=r"rows=False.*latest\(\)"):
        await anext(sub)
    assert native.remaining == 2
    assert sub.latest(backend="native").to_pylist()[0]["BID"] == 17.5
    await sub.add_fields(["ASK"])
    assert sub.fields == ["BID", "ASK"]
    await sub.unsubscribe()
    assert native.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_unsubscribe_drains_warnings_after_native_cleanup(cleanup_fails):
    class Native(NativeSubscription):
        def take_warnings(self):
            assert self.closed, "warning drain must follow native cleanup"
            return super().take_warnings()

        async def unsubscribe(self, drain, tick_mode):
            self.closed = True
            if cleanup_fails:
                raise RuntimeError("synthetic cleanup failure")
            return []

    native = Native(pending=_events())
    sub = blp.Subscription(native, raw=True, backend=None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if cleanup_fails:
            with pytest.raises(RuntimeError, match="synthetic cleanup failure"):
                await sub.unsubscribe()
        else:
            await sub.unsubscribe()
    assert [item.category for item in caught] == [BlpDelayedDataWarning, BlpFieldWarning]
    assert all(item.filename == __file__ for item in caught)
    assert native.take_warnings() == []
