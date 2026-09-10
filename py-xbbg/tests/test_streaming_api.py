"""Offline behavioral coverage for the Python streaming APIs."""

from __future__ import annotations

import asyncio
import inspect
import logging
import queue
import threading

import pytest


class TestAsubscribeSignature:
    """Verify asubscribe() has all new streaming params with correct defaults."""

    def test_asubscribe_signature(self):
        """All new params exist with correct defaults."""
        from xbbg.blp import asubscribe

        sig = inspect.signature(asubscribe)
        params = sig.parameters

        assert "service" in params
        assert params["service"].default is None

        assert "options" in params
        assert params["options"].default is None

        assert "conflate" in params
        assert params["conflate"].default is False

        assert "tick_mode" in params
        assert params["tick_mode"].default is False

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False


class TestAstreamSignature:
    """Verify astream() has callback and config params."""

    def test_astream_signature(self):
        """callback param exists with default None; config params present."""
        from xbbg.blp import astream

        sig = inspect.signature(astream)
        params = sig.parameters

        assert "callback" in params
        assert params["callback"].default is None

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False

        assert "conflate" in params
        assert params["conflate"].default is False


class TestStreamSignature:
    """Verify stream() also has the new params."""

    def test_stream_signature(self):
        """stream() has callback, flush_threshold, stream_capacity, overflow_policy."""
        from xbbg.blp import stream

        sig = inspect.signature(stream)
        params = sig.parameters

        assert "callback" in params
        assert params["callback"].default is None

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False

        assert "conflate" in params
        assert params["conflate"].default is False


class TestSyncStreamLifecycle:
    """Exercise the bounded sync bridge through the public generator."""

    def test_capacity_backpressures_and_close_unsubscribes(self, monkeypatch):
        from xbbg import blp as blp_module

        produced: list[int] = []
        reached_full_bridge = threading.Event()
        producer_completed = threading.Event()
        unsubscribed = threading.Event()

        async def autonomous_stream(*_args, **_kwargs):
            try:
                for item in range(10_000):
                    produced.append(item)
                    if item == 5:
                        reached_full_bridge.set()
                    yield item
                producer_completed.set()
            finally:
                unsubscribed.set()

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        capacity = 4
        batches = blp_module.stream("IBM US Equity", "LAST_PRICE", stream_capacity=capacity)

        assert next(batches) == 0
        assert reached_full_bridge.wait(timeout=1)
        assert produced == list(range(len(produced)))
        assert len(produced) <= capacity + 2  # one consumed batch and one producer-local batch
        assert not producer_completed.is_set()

        batches.close()

        assert unsubscribed.wait(timeout=1)

    def test_streams_share_one_managed_producer_thread_and_capture_context(self, monkeypatch):
        from xbbg import blp as blp_module

        unsubscribed: list[threading.Event] = []

        async def autonomous_stream(*_args, **_kwargs):
            closed = threading.Event()
            unsubscribed.append(closed)
            try:
                yield threading.current_thread().ident, blp_module._get_engine()
                await asyncio.Event().wait()
            finally:
                closed.set()

        class ScopedEngine:
            def __init__(self):
                self._py_engine = object()

        first_scope = ScopedEngine()
        second_scope = ScopedEngine()
        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        first = blp_module.stream("IBM US Equity", "LAST_PRICE")
        second = blp_module.stream("MSFT US Equity", "LAST_PRICE")

        first_token = blp_module._active_engine.set(first_scope)
        try:
            first_thread, first_engine = next(first)
        finally:
            blp_module._active_engine.reset(first_token)

        second_token = blp_module._active_engine.set(second_scope)
        try:
            second_thread, second_engine = next(second)
        finally:
            blp_module._active_engine.reset(second_token)

        try:
            assert first_thread == second_thread
            assert first_engine is first_scope._py_engine
            assert second_engine is second_scope._py_engine
        finally:
            first.close()
            second.close()

        assert len(unsubscribed) == 2
        assert all(closed.is_set() for closed in unsubscribed)

    def test_stream_producer_admission_is_scoped_per_engine(self, monkeypatch):
        from xbbg import blp as blp_module

        async def autonomous_stream(*_args, **_kwargs):
            yield "first"
            await asyncio.Event().wait()

        class Config:
            def __init__(self, limit):
                self.max_subscription_sessions = limit

        class ScopedEngine:
            def __init__(self, limit):
                self._config_snapshot = Config(limit)
                self._py_engine = object()

        def start_in_scope(generator, scope):
            token = blp_module._active_engine.set(scope)
            try:
                return next(generator)
            finally:
                blp_module._active_engine.reset(token)

        first_scope = ScopedEngine(2)
        second_scope = ScopedEngine(1)
        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        first_a = blp_module.stream("IBM US Equity", "LAST_PRICE")
        first_b = blp_module.stream("MSFT US Equity", "LAST_PRICE")
        second_a = blp_module.stream("NVDA US Equity", "LAST_PRICE")

        try:
            assert start_in_scope(first_a, first_scope) == "first"
            assert start_in_scope(first_b, second_scope) == "first"

            rejected_b = blp_module.stream("AMZN US Equity", "LAST_PRICE")
            with pytest.raises(RuntimeError, match="producer limit"):
                start_in_scope(rejected_b, second_scope)

            assert start_in_scope(second_a, first_scope) == "first"
            rejected_a = blp_module.stream("META US Equity", "LAST_PRICE")
            with pytest.raises(RuntimeError, match="producer limit"):
                start_in_scope(rejected_a, first_scope)
        finally:
            first_a.close()
            first_b.close()
            second_a.close()

    def test_consumer_exception_cancels_producer_waiting_for_tick(self, monkeypatch):
        from xbbg import blp as blp_module

        waiting_for_tick = threading.Event()
        unsubscribed = threading.Event()

        async def autonomous_stream(*_args, **_kwargs):
            try:
                yield "first"
                waiting_for_tick.set()
                await asyncio.Event().wait()
            finally:
                unsubscribed.set()

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        batches = blp_module.stream("IBM US Equity", "LAST_PRICE")

        assert next(batches) == "first"
        assert waiting_for_tick.wait(timeout=1)

        consumer_error = RuntimeError("consumer failed")
        with pytest.raises(RuntimeError) as raised:
            batches.throw(consumer_error)

        assert raised.value is consumer_error
        assert unsubscribed.wait(timeout=1)

    def test_cleanup_error_is_reported_without_masking_consumer_error(self, monkeypatch, caplog):
        from xbbg import blp as blp_module

        reached_full_bridge = threading.Event()
        cleanup_started = threading.Event()

        class CleanupError(RuntimeError):
            pass

        cleanup_error = CleanupError("unsubscribe failed")

        async def autonomous_stream(*_args, **_kwargs):
            try:
                for item in range(10_000):
                    if item == 5:
                        reached_full_bridge.set()
                    yield item
            finally:
                cleanup_started.set()
                raise cleanup_error

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        batches = blp_module.stream("IBM US Equity", "LAST_PRICE", stream_capacity=4)

        assert next(batches) == 0
        assert reached_full_bridge.wait(timeout=1)

        consumer_error = RuntimeError("consumer failed")
        with caplog.at_level(logging.ERROR, logger="xbbg.blp"), pytest.raises(RuntimeError) as raised:
            batches.throw(consumer_error)

        assert raised.value is consumer_error
        assert cleanup_started.wait(timeout=1)
        assert any(record.exc_info and record.exc_info[1] is cleanup_error for record in caplog.records)

    def test_close_timeout_fails_and_retains_bounded_producer_slot(self, monkeypatch):
        from xbbg import blp as blp_module

        cleanup_started = threading.Event()
        cleanup_finished = threading.Event()
        producer_released = threading.Event()
        cleanup_gate: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []
        producer_calls = []

        original_start = blp_module._notebook_sync_bridge.start

        def capture_start(*args, **kwargs):
            call = original_start(*args, **kwargs)
            producer_calls.append(call)
            return call

        async def cancellation_resistant_stream(*_args, **_kwargs):
            loop = asyncio.get_running_loop()
            gate = loop.create_future()
            cleanup_gate.append((loop, gate))
            try:
                yield "first"
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await gate
                cleanup_finished.set()

        monkeypatch.setattr(blp_module, "astream", cancellation_resistant_stream)
        monkeypatch.setattr(blp_module._notebook_sync_bridge, "start", capture_start)
        monkeypatch.setattr(blp_module, "_DEFAULT_MAX_SYNC_STREAM_PRODUCERS", 1)
        monkeypatch.setattr(blp_module, "_SYNC_STREAM_CLOSE_TIMEOUT_SECONDS", 0.01)
        monkeypatch.setattr(blp_module, "_config", None)
        batches = blp_module.stream("IBM US Equity", "LAST_PRICE")

        assert next(batches) == "first"
        try:
            with pytest.raises(RuntimeError, match="close timeout"):
                batches.close()
            assert cleanup_started.wait(timeout=1)

            blocked = blp_module.stream("MSFT US Equity", "LAST_PRICE")
            with pytest.raises(RuntimeError, match="producer limit"):
                next(blocked)
        finally:
            if cleanup_gate:
                assert producer_calls
                producer_calls[0].result.add_done_callback(lambda _result: producer_released.set())
                loop, gate = cleanup_gate[0]
                loop.call_soon_threadsafe(gate.set_result, None)

        assert cleanup_finished.wait(timeout=1)
        assert producer_calls
        assert producer_released.wait(timeout=1)

        async def completed_stream(*_args, **_kwargs):
            yield "accepted"

        monkeypatch.setattr(blp_module, "astream", completed_stream)
        assert list(blp_module.stream("MSFT US Equity", "LAST_PRICE")) == ["accepted"]

    def test_completion_drains_all_buffered_data(self, monkeypatch):
        from xbbg import blp as blp_module

        async def autonomous_stream(*_args, **_kwargs):
            for item in range(20):
                yield item

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)

        assert list(blp_module.stream("IBM US Equity", "LAST_PRICE", stream_capacity=4)) == list(range(20))

    def test_sync_callback_runs_on_consuming_thread(self, monkeypatch):
        from xbbg import blp as blp_module

        callback_threads = []

        async def autonomous_stream(*_args, **_kwargs):
            for item in range(3):
                yield item

        def callback(_batch):
            callback_threads.append(threading.current_thread().ident)

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        consuming_thread = threading.current_thread().ident

        assert list(blp_module.stream("IBM US Equity", "LAST_PRICE", callback=callback)) == [0, 1, 2]
        assert callback_threads == [consuming_thread, consuming_thread, consuming_thread]

    def test_completion_race_rechecks_queue_after_producer_stops(self, monkeypatch):
        from xbbg import blp as blp_module

        source_waiting = threading.Event()
        source_finished = threading.Event()
        source_gate: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []
        producer_calls = []

        original_start = blp_module._notebook_sync_bridge.start

        def capture_start(*args, **kwargs):
            call = original_start(*args, **kwargs)
            producer_calls.append(call)
            return call

        async def autonomous_stream(*_args, **_kwargs):
            loop = asyncio.get_running_loop()
            gate = loop.create_future()
            source_gate.append((loop, gate))
            source_waiting.set()
            await gate
            yield "last"
            source_finished.set()

        boundary_forced = False

        class CompletionBoundaryQueue(queue.Queue):
            def get_nowait(self):
                nonlocal boundary_forced
                try:
                    return super().get_nowait()
                except queue.Empty:
                    if boundary_forced:
                        raise
                    boundary_forced = True
                    assert source_waiting.wait(timeout=1)
                    assert producer_calls
                    completion_acknowledged = threading.Event()
                    producer_calls[0].result.add_done_callback(lambda _result: completion_acknowledged.set())
                    loop, gate = source_gate[0]
                    loop.call_soon_threadsafe(gate.set_result, None)
                    assert source_finished.wait(timeout=1)
                    assert completion_acknowledged.wait(timeout=1)
                    raise

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        monkeypatch.setattr(blp_module._notebook_sync_bridge, "start", capture_start)
        monkeypatch.setattr(queue, "Queue", CompletionBoundaryQueue)

        assert list(blp_module.stream("IBM US Equity", "LAST_PRICE", stream_capacity=4)) == ["last"]

    def test_producer_error_follows_all_buffered_data(self, monkeypatch):
        from xbbg import blp as blp_module

        class ProducerError(RuntimeError):
            pass

        error = ProducerError("producer failed")

        async def autonomous_stream(*_args, **_kwargs):
            for item in range(20):
                yield item
            raise error

        monkeypatch.setattr(blp_module, "astream", autonomous_stream)
        batches = blp_module.stream("IBM US Equity", "LAST_PRICE", stream_capacity=4)
        received = []

        with pytest.raises(ProducerError) as raised:
            while True:
                received.append(next(batches))

        assert raised.value is error
        assert received == list(range(20))


class TestStreamingServiceHelpersSignature:
    """avwap / amktbar / adepth / achains forward all_fields."""

    def test_all_fields_kwarg_defaults(self):
        from xbbg.blp import achains, adepth, amktbar, avwap

        for fn in (adepth, achains):
            sig = inspect.signature(fn)
            assert "all_fields" in sig.parameters
            assert sig.parameters["all_fields"].default is False

        for fn in (avwap, amktbar):
            sig = inspect.signature(fn)
            assert "all_fields" in sig.parameters
            assert sig.parameters["all_fields"].default is True


class TestVwapContract:
    """Verify Market VWAP helpers use Bloomberg's required subscription shape."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["//blp/mktvwap/ticker/IBM US Equity"]
            failed_tickers = []
            failures = []
            topic_states = [("//blp/mktvwap/ticker/IBM US Equity", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["VWAP"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

            def __init__(self):
                self.added: list[list[str]] = []

            async def add(self, tickers):
                self.added.append(tickers)

        fake_sub = FakePySubscription()

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return fake_sub

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module, fake_sub

    def test_avwap_signature_uses_vwap_only_contract(self):
        from xbbg.blp import avwap

        sig = inspect.signature(avwap)
        assert "fields" not in sig.parameters
        assert sig.parameters["all_fields"].default is True

    def test_avwap_builds_explicit_market_vwap_subscription(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.avwap(
                ["IBM US Equity", "//blp/mktvwap/ticker/MSFT US Equity"],
                start_time="10:00",
                end_time="16:00",
            )
        )

        assert captured["service"] == Service.MKTVWAP.value
        assert captured["tickers"] == [
            "//blp/mktvwap/ticker/IBM US Equity",
            "//blp/mktvwap/ticker/MSFT US Equity",
        ]
        assert captured["fields"] == ["VWAP"]
        assert captured["options"] == ["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("AAPL US Equity"))
        assert fake_sub.added == [["//blp/mktvwap/ticker/AAPL US Equity"]]

    def test_asubscribe_mktvwap_normalizes_topics_and_validates_contract(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "IBM US Equity",
                "VWAP",
                service=Service.MKTVWAP,
                options=["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"],
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTVWAP.value
        assert captured["tickers"] == ["//blp/mktvwap/ticker/IBM US Equity"]
        assert captured["fields"] == ["VWAP"]
        assert captured["options"] == ["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("MSFT US Equity"))
        assert fake_sub.added == [["//blp/mktvwap/ticker/MSFT US Equity"]]

    def test_mktvwap_rejects_invalid_contract_inputs(self):
        from xbbg.blp import asubscribe, avwap
        from xbbg.services import Service

        with pytest.raises(ValueError, match="market-VWAP topic"):
            asyncio.run(avwap("//blp/mktdata/ticker/IBM US Equity"))

        with pytest.raises(ValueError, match="VWAP"):
            asyncio.run(
                asubscribe(
                    "IBM US Equity",
                    ["RT_PX_VWAP"],
                    service=Service.MKTVWAP,
                )
            )


class TestMktbarContract:
    """Verify market-bar helpers use Bloomberg's required subscription shape."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["//blp/mktbar/ticker/ES1 Index"]
            failed_tickers = []
            failures = []
            topic_states = [("//blp/mktbar/ticker/ES1 Index", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

            def __init__(self):
                self.added: list[list[str]] = []
                self.removed: list[list[str]] = []

            async def add(self, tickers):
                self.added.append(tickers)

            async def remove(self, tickers):
                self.removed.append(tickers)

        fake_sub = FakePySubscription()

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return fake_sub

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module, fake_sub

    def test_amktbar_signature_uses_bar_size(self):
        from xbbg.blp import amktbar

        sig = inspect.signature(amktbar)
        assert "bar_size" in sig.parameters
        assert sig.parameters["bar_size"].default == 1
        assert "interval" not in sig.parameters

    def test_amktbar_builds_explicit_market_bar_subscription(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.amktbar(
                ["ES1 Index", "/figi/BBG000JB5HR2", "isin/GB00B16GWD56 LN"],
                bar_size=5,
                start_time="13:30",
                end_time="20:00",
            )
        )

        assert captured["service"] == Service.MKTBAR.value
        assert captured["tickers"] == [
            "//blp/mktbar/ticker/ES1 Index",
            "//blp/mktbar/figi/BBG000JB5HR2",
            "//blp/mktbar/isin/GB00B16GWD56 LN",
        ]
        assert captured["fields"] == ["LAST_PRICE"]
        assert captured["options"] == ["bar_size=5", "start_time=13:30", "end_time=20:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("EURUSD Curncy"))
        asyncio.run(sub.remove("/figi/BBG000JB5HR2"))
        assert fake_sub.added == [["//blp/mktbar/ticker/EURUSD Curncy"]]
        assert fake_sub.removed == [["//blp/mktbar/figi/BBG000JB5HR2"]]

    def test_asubscribe_mktbar_normalizes_topics_and_validates_contract(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                "LAST_PRICE",
                service=Service.MKTBAR,
                options=["bar_size=1"],
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTBAR.value
        assert captured["tickers"] == ["//blp/mktbar/ticker/ES1 Index"]
        assert captured["fields"] == ["LAST_PRICE"]
        assert captured["options"] == ["bar_size=1"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("ticker/EURUSD Curncy"))
        assert fake_sub.added == [["//blp/mktbar/ticker/EURUSD Curncy"]]

    def test_mktbar_rejects_invalid_contract_inputs(self):
        from xbbg.blp import amktbar, asubscribe
        from xbbg.services import Service

        with pytest.raises(ValueError, match="bar_size"):
            asyncio.run(amktbar("ES1 Index", bar_size=0))

        with pytest.raises(ValueError, match="market-bar topic"):
            asyncio.run(amktbar("//blp/mktdata/ticker/ES1 Index"))

        with pytest.raises(ValueError, match="LAST_PRICE"):
            asyncio.run(
                asubscribe(
                    "ES1 Index",
                    ["OPEN"],
                    service=Service.MKTBAR,
                    options=["bar_size=1"],
                )
            )

        with pytest.raises(ValueError, match="bar_size"):
            asyncio.run(asubscribe("ES1 Index", "LAST_PRICE", service=Service.MKTBAR))

        for invalid_option in (["bar_size"], ["bar_size="], ["bar_size=0"], ["bar_size=1441"], ["bar_size=abc"]):
            with pytest.raises(ValueError, match="bar_size"):
                asyncio.run(
                    asubscribe(
                        "ES1 Index",
                        "LAST_PRICE",
                        service=Service.MKTBAR,
                        options=invalid_option,
                    )
                )


class TestConflatedMarketDataContract:
    """Verify mktdata conflation is exposed as a typed subscription option."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["ES1 Index"]
            failed_tickers = []
            failures = []
            topic_states = [("ES1 Index", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["BID", "ASK"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return FakePySubscription()

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module

    def test_asubscribe_conflate_adds_mktdata_option(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                ["BID", "ASK"],
                conflate=True,
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTDATA.value
        assert captured["tickers"] == ["ES1 Index"]
        assert captured["fields"] == ["BID", "ASK"]
        assert captured["options"] == ["conflate"]
        assert captured["all_fields"] is True
        assert sub.tickers == ["ES1 Index"]

    def test_conflate_normalizes_ampersand_and_avoids_duplicates(self, monkeypatch):
        captured: dict[str, object] = {}
        blp_module = self._install_fake_engine(monkeypatch, captured)

        asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                ["BID", "ASK"],
                options=["&conflate", "delayed"],
                conflate=True,
            )
        )

        assert captured["options"] == ["conflate", "delayed"]

    def test_conflate_rejects_non_mktdata_service(self):
        from xbbg.blp import asubscribe
        from xbbg.services import Service

        with pytest.raises(ValueError, match="//blp/mktdata"):
            asyncio.run(asubscribe("IBM US Equity", "VWAP", service=Service.MKTVWAP, conflate=True))

    def test_conflate_rejects_interval_option(self):
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="interval"):
            asyncio.run(asubscribe("ES1 Index", ["BID", "ASK"], options=["interval=5"], conflate=True))


class TestConfigValidation:
    """Verify ValueError raised for invalid config params.

    Validation happens BEFORE the engine call, so these work offline.
    """

    def test_config_validation_flush_threshold(self):
        """flush_threshold=0 raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="flush_threshold"):
            asyncio.run(asubscribe(["AAPL US Equity"], ["LAST_PRICE"], flush_threshold=0))

    def test_config_validation_stream_capacity(self):
        """stream_capacity=0 raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="stream_capacity"):
            asyncio.run(asubscribe(["AAPL US Equity"], ["LAST_PRICE"], stream_capacity=0))

    def test_config_validation_overflow_policy(self):
        """Invalid overflow_policy raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="overflow_policy"):
            asyncio.run(
                asubscribe(
                    ["AAPL US Equity"],
                    ["LAST_PRICE"],
                    overflow_policy="invalid_policy",
                )
            )


class TestTickModeWarning:
    """Verify warning when tick_mode=True conflicts with flush_threshold."""

    def test_tick_mode_flush_threshold_warning(self):
        """tick_mode=True with flush_threshold>1 emits UserWarning before engine call."""
        from xbbg.blp import asubscribe

        captured: dict[str, object] = {}

        class FakePySubscription:
            tickers = ["AAPL US Equity"]
            failed_tickers = []
            failures = []
            topic_states = [("AAPL US Equity", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return FakePySubscription()

        import xbbg.blp as blp_module

        original_get_engine = blp_module._get_engine
        blp_module._get_engine = lambda: FakeEngine()

        try:
            with pytest.warns(UserWarning, match="tick_mode"):
                sub = asyncio.run(
                    asubscribe(
                        ["AAPL US Equity"],
                        ["LAST_PRICE"],
                        tick_mode=True,
                        flush_threshold=50,
                    )
                )
        finally:
            blp_module._get_engine = original_get_engine

        assert sub._tick_mode is True
        assert captured["flush_threshold"] == 1


class TestExplicitOutputSelector:
    """Explicit output modes determine both iteration and drain values."""

    @staticmethod
    def _install_fake_engine(
        monkeypatch,
        *,
        live_batch,
        drained_batches,
        live_tick,
        drained_ticks,
    ):
        from xbbg import blp as blp_module

        class FakePySubscription:
            def __init__(self):
                self.batch_reads = 0
                self.tick_reads = 0

            async def __anext__(self):
                self.batch_reads += 1
                return live_batch

            async def __anext_tick_dict__(self):
                self.tick_reads += 1
                return live_tick

            async def unsubscribe(self, drain, tick_mode):
                assert drain is True
                return list(drained_ticks if tick_mode else drained_batches)

        native_sub = FakePySubscription()

        class FakeEngine:
            async def subscribe(self, _tickers, _fields, *, all_fields):
                assert all_fields is False
                return native_sub

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module, native_sub

    def test_record_batch_overrides_legacy_tick_mode_for_iteration_and_drain(self, monkeypatch):
        from xbbg.blp import Backend

        live_batch = object()
        drained_batch = object()
        blp_module, native_sub = self._install_fake_engine(
            monkeypatch,
            live_batch=live_batch,
            drained_batches=[drained_batch],
            live_tick={"representation": "tick"},
            drained_ticks=[{"representation": "drained tick"}],
        )

        async def exercise():
            sub = await blp_module.asubscribe(
                "IBM US Equity",
                "LAST_PRICE",
                raw=False,
                backend=Backend.NATIVE,
                tick_mode=True,
                output="record_batch",
            )
            return await anext(sub), await sub.unsubscribe(drain=True)

        current, drained = asyncio.run(exercise())

        assert current is live_batch
        assert drained == [drained_batch]
        assert native_sub.batch_reads == 1
        assert native_sub.tick_reads == 0

    def test_backend_overrides_legacy_raw_and_tick_modes_for_iteration_and_drain(self, monkeypatch):
        from xbbg.blp import Backend

        live_table = object()
        drained_table = object()

        class FakeBatch:
            def __init__(self, table):
                self.table = table

            def to_table(self):
                return self.table

        live_batch = FakeBatch(live_table)
        drained_batch = FakeBatch(drained_table)
        blp_module, native_sub = self._install_fake_engine(
            monkeypatch,
            live_batch=live_batch,
            drained_batches=[drained_batch],
            live_tick={"representation": "tick"},
            drained_ticks=[{"representation": "drained tick"}],
        )
        monkeypatch.setattr(
            blp_module,
            "convert_backend_frame",
            lambda table, backend: ("backend", table, backend),
        )

        async def exercise():
            sub = await blp_module.asubscribe(
                "IBM US Equity",
                "LAST_PRICE",
                raw=True,
                backend=Backend.NATIVE,
                tick_mode=True,
                output="backend",
            )
            return await anext(sub), await sub.unsubscribe(drain=True)

        current, drained = asyncio.run(exercise())

        assert current == ("backend", live_table, Backend.NATIVE)
        assert drained == [("backend", drained_table, Backend.NATIVE)]
        assert native_sub.batch_reads == 1
        assert native_sub.tick_reads == 0

    @pytest.mark.parametrize("output", ["dict", "tick"])
    def test_dict_outputs_override_legacy_batch_modes_for_iteration_and_drain(self, monkeypatch, output):
        from xbbg.blp import Backend

        live_tick = {"topic": "IBM US Equity", "LAST_PRICE": 123.45}
        drained_tick = {"topic": "IBM US Equity", "LAST_PRICE": None}
        blp_module, native_sub = self._install_fake_engine(
            monkeypatch,
            live_batch=object(),
            drained_batches=[object()],
            live_tick=live_tick,
            drained_ticks=[drained_tick],
        )
        monkeypatch.setattr(
            blp_module,
            "convert_backend_frame",
            lambda *_args: (_ for _ in ()).throw(AssertionError("dict output must not use batch conversion")),
        )

        async def exercise():
            sub = await blp_module.asubscribe(
                "IBM US Equity",
                "LAST_PRICE",
                raw=False,
                backend=Backend.NATIVE,
                tick_mode=False,
                output=output,
            )
            return await anext(sub), await sub.unsubscribe(drain=True)

        current, drained = asyncio.run(exercise())

        assert current is live_tick
        assert drained == [drained_tick]
        assert native_sub.batch_reads == 0
        assert native_sub.tick_reads == 1


class TestPublicStreamingTyping:
    """The package stub exposes the same streaming and exception surface as runtime."""

    def test_top_level_stub_declares_streaming_and_exception_exports(self):
        from pathlib import Path

        import xbbg
        from xbbg import blp as blp_module, exceptions as exception_module
        from xbbg._exports import EXCEPTION_EXPORTS, PACKAGE_STREAMING_EXPORTS

        stub = Path(xbbg.__file__).with_name("__init__.pyi").read_text()
        for name in PACKAGE_STREAMING_EXPORTS:
            assert f"{name} as {name}" in stub
            assert f'    "{name}",' in stub
            assert getattr(xbbg, name) is getattr(blp_module, name)
        for name in EXCEPTION_EXPORTS:
            assert f"{name} as {name}" in stub
            assert f'    "{name}",' in stub
            assert getattr(xbbg, name) is getattr(exception_module, name)

    def test_subscription_and_asubscribe_annotations_survive_root_reexport(self):
        from typing import Any, get_type_hints

        import xbbg

        assert get_type_hints(xbbg.asubscribe)["return"] is xbbg.Subscription
        assert get_type_hints(xbbg.Subscription.unsubscribe)["return"] == list[Any] | None


class TestSubscriptionConversion:
    """Subscription iteration converts batches through the constructor-bound path."""

    def test_backend_none_yields_native_table_from_batch(self, monkeypatch):
        from xbbg import blp as blp_module
        from xbbg._core import ArrowTable
        from xbbg.blp import Subscription

        table = ArrowTable.from_pylist([{"ticker": "IBM US Equity", "LAST_PRICE": 123.45}])

        class FakeBatch:
            def __init__(self):
                self.to_table_calls = 0

            def to_table(self):
                self.to_table_calls += 1
                return table

        class FakePySubscription:
            async def __anext__(self):
                return batch

        def fail_facade_conversion(_frame, _backend):
            raise AssertionError("Subscription.__anext__ must not call facade backend conversion")

        batch = FakeBatch()
        monkeypatch.setattr(blp_module, "_convert_result_backend", fail_facade_conversion)
        sub = Subscription(FakePySubscription(), raw=False, backend=None)

        result = asyncio.run(sub.__anext__())

        assert result is table
        assert batch.to_table_calls == 1

    def test_explicit_backend_uses_bound_converter_without_facade_conversion(self, monkeypatch):
        from xbbg import blp as blp_module
        from xbbg._core import ArrowTable
        from xbbg.blp import Backend, Subscription

        table = ArrowTable.from_pylist([{"ticker": "IBM US Equity", "LAST_PRICE": 123.45}])
        converted = object()
        calls = []

        class FakeBatch:
            def to_table(self):
                calls.append("to_table")
                return table

        class FakePySubscription:
            async def __anext__(self):
                return FakeBatch()

        def bound_converter(frame, backend):
            calls.append((frame, backend))
            return converted

        def fail_facade_conversion(_frame, _backend):
            raise AssertionError("Subscription.__anext__ must not call facade backend conversion")

        monkeypatch.setattr(blp_module, "convert_backend_frame", bound_converter)
        monkeypatch.setattr(blp_module, "_convert_result_backend", fail_facade_conversion)
        sub = Subscription(FakePySubscription(), raw=False, backend=Backend.NATIVE)

        result = asyncio.run(sub.__anext__())

        assert result is converted
        assert calls == ["to_table", (table, Backend.NATIVE)]

    def test_raw_drain_returns_native_batches_unchanged(self):
        from xbbg.blp import Subscription

        batches = [object(), object()]
        calls = []

        class FakePySubscription:
            async def unsubscribe(self, drain, tick_mode):
                calls.append((drain, tick_mode))
                return batches

        sub = Subscription(FakePySubscription(), raw=True, backend=None)

        result = asyncio.run(sub.unsubscribe(drain=True))

        assert result == batches
        assert calls == [(True, False)]

    def test_table_drain_uses_the_iteration_conversion(self):
        from xbbg.blp import Subscription

        tables = [object(), object()]
        calls = []

        class FakeBatch:
            def __init__(self, table):
                self.table = table

            def to_table(self):
                calls.append(self.table)
                return self.table

        class FakePySubscription:
            async def unsubscribe(self, drain, tick_mode):
                assert (drain, tick_mode) == (True, False)
                return [FakeBatch(table) for table in tables]

        sub = Subscription(FakePySubscription(), raw=False, backend=None)

        result = asyncio.run(sub.unsubscribe(drain=True))

        assert result == tables
        assert calls == tables

    def test_backend_drain_uses_the_bound_iteration_converter(self, monkeypatch):
        from xbbg import blp as blp_module
        from xbbg.blp import Backend, Subscription

        table = object()
        converted = object()
        calls = []

        class FakeBatch:
            def to_table(self):
                calls.append("to_table")
                return table

        class FakePySubscription:
            async def unsubscribe(self, drain, tick_mode):
                assert (drain, tick_mode) == (True, False)
                return [FakeBatch()]

        def bound_converter(frame, backend):
            calls.append((frame, backend))
            return converted

        monkeypatch.setattr(blp_module, "convert_backend_frame", bound_converter)
        sub = Subscription(FakePySubscription(), raw=False, backend=Backend.NATIVE)

        result = asyncio.run(sub.unsubscribe(drain=True))

        assert result == [converted]
        assert calls == ["to_table", (table, Backend.NATIVE)]

    def test_tick_drain_returns_native_sparse_dicts_without_arrow_conversion(self):
        from xbbg.blp import Subscription

        ticks = [{"topic": "IBM US Equity", "PX_LAST": None}, {"topic": "IBM US Equity"}]
        calls = []

        class FakePySubscription:
            async def unsubscribe(self, drain, tick_mode):
                calls.append((drain, tick_mode))
                return ticks

        sub = Subscription(FakePySubscription(), raw=True, backend=None, tick_mode=True)
        sub._convert_batch = lambda _value: (_ for _ in ()).throw(
            AssertionError("dict drain must not convert through Arrow")
        )

        result = asyncio.run(sub.unsubscribe(drain=True))

        assert result == ticks
        assert "PX_LAST" in result[0]
        assert "PX_LAST" not in result[1]
        assert calls == [(True, True)]

    def test_drain_normalizes_unread_gap_error_with_context(self):
        from xbbg import _core
        from xbbg.blp import Subscription
        from xbbg.exceptions import BlpSubscriptionDataLossError

        native_error = _core.BlpSubscriptionDataLossError("subscription gap")
        native_error.topic = "IBM US Equity"
        native_error.detail = "consumer queue overflow"

        class FakePySubscription:
            async def unsubscribe(self, drain, tick_mode):
                assert (drain, tick_mode) == (True, False)
                raise native_error

        sub = Subscription(FakePySubscription(), raw=True, backend=None)

        with pytest.raises(BlpSubscriptionDataLossError) as raised:
            asyncio.run(sub.unsubscribe(drain=True))

        assert raised.value.topic == "IBM US Equity"
        assert raised.value.detail == "consumer queue overflow"


class TestSubscriptionStats:
    """Verify Subscription class has a stats property."""

    def test_subscription_stats_property_exists(self):
        """Subscription.stats is a property descriptor."""
        from xbbg.blp import Subscription

        assert hasattr(Subscription, "stats")
        assert isinstance(inspect.getattr_static(Subscription, "stats"), property)


class TestSubscriptionFailureMetadata:
    """Verify Subscription exposes non-fatal failure metadata."""

    def test_failure_properties_exist(self):
        from xbbg.blp import Subscription

        assert isinstance(inspect.getattr_static(Subscription, "failed_tickers"), property)
        assert isinstance(inspect.getattr_static(Subscription, "failures"), property)
        assert isinstance(inspect.getattr_static(Subscription, "status"), property)
        assert isinstance(inspect.getattr_static(Subscription, "events"), property)
        assert isinstance(inspect.getattr_static(Subscription, "topic_states"), property)

    def test_failure_properties_proxy_underlying_subscription(self):
        from xbbg.blp import Subscription

        class FakePySubscription:
            tickers = ["SPY US Equity"]
            failed_tickers = ["/isin/BMG8192H1557"]
            failures = [
                (
                    "/isin/BMG8192H1557",
                    "Security is not valid for subscription [EX336]",
                    "failure",
                )
            ]
            topic_states = [
                ("SPY US Equity", "streaming", 123),
                ("/isin/BMG8192H1557", "failed", 456),
            ]
            session_status = {
                "state": "up",
                "last_change_us": 789,
                "disconnect_count": 1,
                "reconnect_count": 1,
            }
            admin_status = {
                "slow_consumer_warning_active": True,
                "slow_consumer_warning_count": 2,
                "slow_consumer_cleared_count": 1,
                "data_loss_count": 3,
                "last_warning_us": 10,
                "last_cleared_us": 11,
                "last_data_loss_us": 12,
            }
            service_status = [("//blp/mktdata", True, 99)]
            events = [
                (1, "session", "info", "SessionConnectionUp", None, "worker=0 active_subscriptions=1"),
                (
                    2,
                    "subscription",
                    "warning",
                    "SubscriptionFailure",
                    "/isin/BMG8192H1557",
                    "Security is not valid for subscription [EX336]",
                ),
            ]
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 3,
                "last_message_us": 100,
                "last_data_loss_us": 12,
                "effective_overflow_policy": "drop_newest",
            }

        sub = Subscription(FakePySubscription(), raw=True, backend=None)

        assert sub.tickers == ["SPY US Equity"]
        assert sub.failed_tickers == ["/isin/BMG8192H1557"]
        assert sub.failures == [
            {
                "ticker": "/isin/BMG8192H1557",
                "reason": "Security is not valid for subscription [EX336]",
                "kind": "failure",
            }
        ]
        assert sub.topic_states["SPY US Equity"]["state"] == "streaming"
        assert sub.session_status["state"] == "up"
        assert sub.admin_status["data_loss_count"] == 3
        assert sub.service_status["//blp/mktdata"]["up"] is True
        assert sub.events[1]["message_type"] == "SubscriptionFailure"
        assert sub.status["session"]["reconnect_count"] == 1


class TestBackwardCompatibility:
    """Verify all new params are optional (backward compat)."""

    def test_backward_compat_signature(self):
        """asubscribe can be called with just tickers and fields — all new params have defaults."""
        from xbbg.blp import asubscribe

        sig = inspect.signature(asubscribe)
        params = sig.parameters

        assert "tickers" in params
        assert "fields" in params

        # Every new param must have a default (i.e. is optional)
        new_params = [
            "service",
            "options",
            "conflate",
            "tick_mode",
            "flush_threshold",
            "stream_capacity",
            "overflow_policy",
        ]
        for param_name in new_params:
            assert param_name in params, f"{param_name} missing from signature"
            assert params[param_name].default is not inspect.Parameter.empty, f"{param_name} should have a default"
