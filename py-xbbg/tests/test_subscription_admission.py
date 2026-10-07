"""Sync streams bound native session claims without counting Python producers."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from xbbg import _core, _engine, blp

FIRST = "SYNTHETIC1 Equity"
SECOND = "SYNTHETIC2 Equity"
THIRD = "SYNTHETIC3 Equity"


class _Feed:
    def __init__(self, topic):
        self.topic = topic
        self.consumers = 0


class _Rows:
    delivers_rows = True

    def __init__(self, engine, feed):
        self.engine = engine
        self.feed = feed
        self.sent = False
        self.closed = False
        feed.consumers += 1

    def take_warnings(self):
        return []

    async def __anext__(self):
        if self.sent:
            await asyncio.Event().wait()
        self.sent = True
        return {"topic": self.feed.topic, "BID": 1.0}

    async def unsubscribe(self, drain, tick_mode):
        if not self.closed:
            self.closed = True
            self.feed.consumers -= 1
            if not self.feed.consumers:
                self.engine.feeds.remove(self.feed)
                if self.engine.shared.get(self.feed.topic) is self.feed:
                    del self.engine.shared[self.feed.topic]
        return []


class _Engine:
    """Model only the native feed/session ownership boundary, without a clock."""

    def __init__(self, limit=1):
        self.limit = limit
        self.shared = {}
        self.feeds = set()
        self.attempts = []
        self.handles = []
        self.error = None

    async def subscribe(self, tickers, fields, *, session_wait_ms=None, isolated=False, **kwargs):
        self.attempts.append((tuple(tickers), session_wait_ms))
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        topic = tickers[0]
        feed = None if isolated else self.shared.get(topic)
        if feed is None:
            if len(self.feeds) >= self.limit:
                assert session_wait_ms is not None, "a sync stream must never make an unbounded native claim"
                raise _core.BlpValidationError("Configuration error: session_wait: synthetic claim deadline expired")
            feed = _Feed(topic)
            self.feeds.add(feed)
            if not isolated:
                self.shared[topic] = feed
        native = _Rows(self, feed)
        self.handles.append(native)
        return native


class _Scope:
    def __init__(self, engine):
        self._config_snapshot = SimpleNamespace(max_subscription_sessions=engine.limit)
        self._py_engine = engine


def _start(source, scope):
    token = _engine._active_engine.set(scope)
    try:
        return next(source)
    finally:
        _engine._active_engine.reset(token)


def test_shared_consumers_are_admitted_when_new_and_isolated_feeds_hit_capacity():
    engine = _Engine()
    scope = _Scope(engine)
    creator = blp.stream(FIRST, "BID", raw=True)
    attached = blp.stream(FIRST, "ASK", raw=True)
    new_feed = blp.stream(SECOND, "BID", raw=True)
    isolated = blp.stream(FIRST, "BID", raw=True, isolated=True)
    try:
        assert _start(creator, scope)["topic"] == FIRST
        assert _start(attached, scope)["topic"] == FIRST
        with pytest.raises(RuntimeError, match=r"^sync stream producer limit reached \(1\)$"):
            _start(new_feed, scope)
        with pytest.raises(RuntimeError, match=r"^sync stream producer limit reached \(1\)$"):
            _start(isolated, scope)
        assert engine.attempts == [((FIRST,), 5000), ((FIRST,), 5000), ((SECOND,), 5000), ((FIRST,), 5000)]
        assert len(engine.feeds) == 1
        assert engine.shared[FIRST].consumers == 2
    finally:
        creator.close()
        attached.close()
        new_feed.close()
        isolated.close()
    assert not engine.feeds
    assert all(handle.closed for handle in engine.handles)


def test_creator_close_cannot_leave_new_feed_claim_waiting_behind_retained_feed():
    engine = _Engine()
    scope = _Scope(engine)
    creator = blp.stream(FIRST, "BID", raw=True)
    attached = blp.stream(FIRST, "ASK", raw=True)
    blocked = blp.stream(SECOND, "BID", raw=True)
    after_detach = blp.stream(SECOND, "BID", raw=True)
    try:
        _start(creator, scope)
        _start(attached, scope)
        creator.close()
        assert engine.shared[FIRST].consumers == 1
        with pytest.raises(RuntimeError, match=r"^sync stream producer limit reached \(1\)$") as raised:
            _start(blocked, scope)
        assert isinstance(raised.value.__cause__, _core.BlpValidationError)
        attached.close()
        assert _start(after_detach, scope)["topic"] == SECOND
        assert all(wait == 5000 for _, wait in engine.attempts)
    finally:
        creator.close()
        attached.close()
        blocked.close()
        after_detach.close()
    assert not engine.feeds


def test_claim_timeout_message_uses_the_active_engine_limit():
    engine = _Engine(limit=2)
    scope = _Scope(engine)
    first = blp.stream(FIRST, "BID", raw=True)
    second = blp.stream(SECOND, "BID", raw=True)
    rejected = blp.stream(THIRD, "BID", raw=True)
    try:
        _start(first, scope)
        _start(second, scope)
        with pytest.raises(RuntimeError, match=r"^sync stream producer limit reached \(2\)$"):
            _start(rejected, scope)
        assert len(engine.feeds) == 2
    finally:
        first.close()
        second.close()
        rejected.close()


@pytest.mark.parametrize(
    "error",
    [
        _core.BlpValidationError("Configuration error: conflicting synthetic alias"),
        RuntimeError("Configuration error: session_wait: unrelated runtime failure"),
    ],
)
def test_only_dedicated_native_claim_timeouts_get_the_producer_limit_message(error):
    engine = _Engine()
    scope = _Scope(engine)
    engine.error = error
    source = blp.stream(FIRST, "BID", raw=True)
    try:
        with pytest.raises(type(error)) as raised:
            _start(source, scope)
        assert raised.value is error
    finally:
        source.close()
    assert not engine.feeds


@pytest.mark.asyncio
async def test_direct_async_subscription_keeps_native_unbounded_claim_default(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    sub = await blp.asubscribe(FIRST, "BID", raw=True)
    try:
        assert engine.attempts == [((FIRST,), None)]
    finally:
        await sub.unsubscribe()
    assert not engine.feeds


def test_claim_timeout_message_uses_global_configuration(monkeypatch):
    engine = _Engine(limit=3)
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)
    monkeypatch.setattr(_engine, "_config", SimpleNamespace(max_subscription_sessions=3))
    sources = [blp.stream(topic, "BID", raw=True) for topic in (FIRST, SECOND, THIRD, "SYNTHETIC4 Equity")]
    try:
        for source in sources[:3]:
            _start(source, None)
        with pytest.raises(RuntimeError, match=r"^sync stream producer limit reached \(3\)$"):
            _start(sources[3], None)
        assert len(engine.feeds) == 3
    finally:
        for source in sources:
            source.close()
    assert not engine.feeds
