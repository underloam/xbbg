from __future__ import annotations

import asyncio
import logging
import threading
from typing import cast

import pytest

import xbbg
from xbbg import _engine, blp
from xbbg._core import ArrowRecordBatch, ArrowTable
from xbbg.services import Operation, Service


class DummyConfig:
    def __init__(self, **kwargs):
        self.host = "localhost"
        self.port = 8194
        self.request_pool_size = 2
        self.subscription_pool_size = 1
        self.runtime_worker_threads = 2
        self.max_subscription_sessions = 32
        self.shard_requests = False
        self.shard_threshold = 20
        self.shard_chunk_size = 16
        self.shard_max_concurrent = 4
        self.validation_mode = "disabled"
        self.subscription_flush_threshold = 1
        self.max_event_queue_size = 10_000
        self.command_queue_size = 256
        self.subscription_stream_capacity = 256
        self.overflow_policy = "drop_newest"
        self.warmup_services = ["//blp/refdata", "//blp/apiflds"]
        self.field_cache_path = None
        self.auth_method = None
        self.app_name = None
        self.dir_property = None
        self.user_id = None
        self.ip_address = None
        self.token = None
        self.num_start_attempts = 3
        self.auto_restart_on_disconnection = True
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture(autouse=True)
def reset_blp_state():
    old_config = _engine._config
    old_engine = _engine._engine
    old_middleware = blp.get_middleware()
    blp.clear_middleware()
    _engine._config = None
    _engine._engine = None
    try:
        yield
    finally:
        blp.clear_middleware()
        blp.set_middleware(old_middleware)
        _engine._config = old_config
        _engine._engine = old_engine


def _sample_batch() -> ArrowRecordBatch:
    return ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"},
        ]
    ).to_batches()[0]


def test_arequest_runs_sync_and_async_middleware_in_order(monkeypatch):
    events: list[tuple[str, object]] = []
    contexts: list[blp.RequestContext] = []

    class FakeEngine:
        async def request(self, params_dict):
            events.append(("engine", params_dict["operation"]))
            return _sample_batch()

    async def outer(context: blp.RequestContext, call_next):
        events.append(("outer_pre", context.params.operation))
        context.metadata["trace"] = "outer"
        contexts.append(context)
        result = await call_next(context)
        events.append(("outer_post", context.batch.num_rows if context.batch else 0))
        return result

    def inner(context: blp.RequestContext, call_next):
        events.append(("inner_pre", context.metadata["trace"]))
        return call_next(context)

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    blp.add_middleware(outer)
    blp.add_middleware(inner)

    result = asyncio.run(
        blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
        )
    )

    assert result.num_rows == 1
    assert events == [
        ("outer_pre", Operation.REFERENCE_DATA),
        ("inner_pre", "outer"),
        ("engine", Operation.REFERENCE_DATA.value),
        ("outer_post", 1),
    ]
    assert contexts[0].elapsed_ms is not None
    assert contexts[0].frame is result


def test_request_middleware_mutates_canonical_params_before_dispatch(monkeypatch):
    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    async def rewrite_fields(context: blp.RequestContext, call_next):
        context.params.fields = ["PX_OPEN"]
        return await call_next(context)

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    blp.add_middleware(rewrite_fields)

    asyncio.run(
        blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
        )
    )

    assert captured["fields"] == ["PX_OPEN"]
    assert str(captured["request_id"]).startswith("req-")


def test_request_context_exposes_environment_snapshot(monkeypatch):
    config = DummyConfig(
        host="bpipe-host",
        port=8195,
        auth_method="manual",
        app_name="my-app",
        user_id="123456",
        validation_mode="strict",
    )
    blp.configure(config)

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            return _sample_batch()

    async def recorder(context: blp.RequestContext, call_next):
        captured.update(
            {
                "request_id": context.request_id,
                "environment": context.environment,
                "params_request_id": context.to_dispatch_dict()["request_id"],
            }
        )
        return await call_next(context)

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    blp.add_middleware(recorder)

    asyncio.run(
        blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
        )
    )

    environment = cast("blp.RequestEnvironment", captured["environment"])
    assert captured["params_request_id"] == captured["request_id"]
    assert environment.source == "global_config"
    assert environment.host == "bpipe-host"
    assert environment.port == 8195
    assert environment.auth_method == "manual"
    assert environment.app_name == "my-app"
    assert environment.user_id == "123456"
    assert environment.validation_mode == "strict"


def test_arequest_middleware_can_short_circuit(monkeypatch):
    called = False
    cached_result = [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    class FakeEngine:
        async def request(self, params_dict):
            nonlocal called
            called = True
            return _sample_batch()

    async def cache_middleware(context: blp.RequestContext, _call_next):
        context.metadata["cache_hit"] = True
        context.frame = cached_result
        return cached_result

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    blp.add_middleware(cache_middleware)

    result = asyncio.run(
        blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
        )
    )

    assert result is cached_result
    assert called is False


def test_middleware_record_batch_result_receives_backend_conversion(monkeypatch):
    called = False

    class FakeEngine:
        async def request(self, params_dict):
            nonlocal called
            called = True
            return _sample_batch()

    async def record_batch_middleware(context: blp.RequestContext, _call_next):
        return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    blp.add_middleware(record_batch_middleware)

    result = asyncio.run(
        blp.arequest(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["IBM US Equity"],
            fields=["PX_LAST"],
            backend=blp.Backend.NATIVE,
        )
    )

    assert called is False
    assert isinstance(result, ArrowTable)
    assert result.num_rows == 1


def test_configure_applies_auth_kwargs():
    config = DummyConfig()

    blp.configure(
        config,
        host="bpipe-host",
        port=8195,
        num_start_attempts=5,
        auto_restart_on_disconnection=False,
        auth_method="manual",
        app_name="my-app",
        user_id="123456",
        ip_address="10.0.0.1",
    )

    assert _engine._config is config
    assert isinstance(_engine._config, DummyConfig)
    assert _engine._config.host == "bpipe-host"
    assert _engine._config.port == 8195
    assert _engine._config.auth_method == "manual"
    assert _engine._config.app_name == "my-app"
    assert _engine._config.user_id == "123456"
    assert _engine._config.ip_address == "10.0.0.1"
    assert _engine._config.num_start_attempts == 5
    assert _engine._config.auto_restart_on_disconnection is False
    assert _engine._engine is None


def test_configure_accepts_sharding_kwargs():
    config = DummyConfig()

    blp.configure(
        config,
        shard_requests=True,
        shard_threshold=3,
        shard_chunk_size=2,
        shard_max_concurrent=2,
    )

    assert _engine._config is config
    assert isinstance(_engine._config, DummyConfig)
    assert _engine._config.shard_requests is True
    assert _engine._config.shard_threshold == 3
    assert _engine._config.shard_chunk_size == 2
    assert _engine._config.shard_max_concurrent == 2


def test_configure_accepts_runtime_resource_limits():
    config = DummyConfig()

    blp.configure(
        config,
        runtime_worker_threads=6,
        max_subscription_sessions=48,
    )

    assert _engine._config is config
    assert _engine._config.runtime_worker_threads == 6
    assert _engine._config.max_subscription_sessions == 48


def test_configure_rejects_unknown_kwargs():
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        blp.configure(hots="bpipe-host")


def test_configure_rejects_invalid_num_start_attempts():
    with pytest.raises(ValueError, match="num_start_attempts"):
        blp.configure(num_start_attempts=0)


def test_configure_warns_and_restarts_after_engine_start():
    """configure() after engine start shuts down old engine with a warning."""

    class MockEngine:
        def __init__(self):
            self.shutdown_called = False

        def signal_shutdown(self):
            self.shutdown_called = True

    mock = MockEngine()
    _engine._engine = mock

    with pytest.warns(RuntimeWarning, match="already started"):
        blp.configure(host="bpipe-host")

    assert mock.shutdown_called, "signal_shutdown should have been called"
    assert _engine._engine is None, "engine should be cleared for recreation"
    assert _engine._config is not None, "new config should be stored"


def test_configure_atomically_replaces_engine_created_during_config_build(monkeypatch):
    from xbbg import _core

    config_started = threading.Event()
    release_config = threading.Event()
    engine_stopped = threading.Event()
    configured = []
    errors: list[BaseException] = []

    class FakeConfig:
        def __init__(self, **kwargs):
            config_started.set()
            assert release_config.wait(timeout=1)
            self.__dict__.update(kwargs)
            configured.append(self)

    class FakeEngine:
        def signal_shutdown(self):
            engine_stopped.set()

    class FakePyEngine:
        def __new__(cls):
            return FakeEngine()

        @staticmethod
        def with_config(_config):
            return FakeEngine()

    monkeypatch.setattr(_core, "PyEngineConfig", FakeConfig)
    monkeypatch.setattr(_core, "PyEngine", FakePyEngine)

    def configure_engine():
        try:
            blp.configure(host="configured-host")
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=configure_engine)
    worker.start()
    assert config_started.wait(timeout=1)
    concurrent_engine = _engine._get_engine()
    release_config.set()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert errors == []
    assert configured
    assert _engine._config is configured[0]
    assert _engine._engine is None
    assert engine_stopped.is_set()
    assert concurrent_engine is not None


@pytest.mark.parametrize("operation", ["shutdown", "reset"])
def test_global_lifecycle_waits_for_inflight_engine_construction(monkeypatch, operation):
    from xbbg import _core

    construction_started = threading.Event()
    release_construction = threading.Event()
    lifecycle_waiting = threading.Event()
    engine_stopped = threading.Event()
    lifecycle_started = threading.Event()
    lifecycle_errors: list[BaseException] = []

    class ObservedLock:
        def __init__(self):
            self._lock = threading.Lock()

        def __enter__(self):
            if not self._lock.acquire(blocking=False):
                lifecycle_waiting.set()
                self._lock.acquire()
            return self

        def __exit__(self, *_exc):
            self._lock.release()

    class FakeEngine:
        def signal_shutdown(self):
            engine_stopped.set()

    def construct_engine():
        construction_started.set()
        assert release_construction.wait(timeout=1)
        return FakeEngine()

    class BlockingPyEngine:
        def __new__(cls):
            return construct_engine()

        @staticmethod
        def with_config(_config):
            return construct_engine()

    monkeypatch.setattr(_engine, "_engine_lock", ObservedLock())
    monkeypatch.setattr(_core, "PyEngine", BlockingPyEngine)
    _engine._config = object()

    getter = threading.Thread(target=_engine._get_engine)
    getter.start()
    assert construction_started.wait(timeout=1)

    def run_lifecycle():
        lifecycle_started.set()
        try:
            getattr(blp, operation)()
        except BaseException as error:
            lifecycle_errors.append(error)

    lifecycle = threading.Thread(target=run_lifecycle)
    lifecycle.start()
    assert lifecycle_started.wait(timeout=1)
    try:
        assert lifecycle_waiting.wait(timeout=1)
    finally:
        release_construction.set()

    getter.join(timeout=1)
    lifecycle.join(timeout=1)

    assert not getter.is_alive()
    assert not lifecycle.is_alive()
    assert lifecycle_errors == []
    assert _engine._engine is None
    assert engine_stopped.is_set()
    if operation == "reset":
        assert _engine._config is None


def test_engine_shutdown_runs_outside_global_state_lock():
    lock_was_available = False

    class ReentrantEngine:
        def signal_shutdown(self):
            nonlocal lock_was_available
            lock_was_available = _engine._engine_lock.acquire(blocking=False)
            if lock_was_available:
                _engine._engine_lock.release()

    _engine._engine = ReentrantEngine()

    blp.shutdown()

    assert lock_was_available


def test_request_environment_getters_run_outside_global_state_lock():
    lock_was_available = False

    class ReentrantConfig:
        port = 8194

        @property
        def host(self):
            nonlocal lock_was_available
            lock_was_available = _engine._engine_lock.acquire(blocking=False)
            if lock_was_available:
                _engine._engine_lock.release()
            return "localhost"

    _engine._config = ReentrantConfig()

    environment = blp._snapshot_request_environment()

    assert lock_was_available
    assert environment.host == "localhost"


def test_public_exports_include_configure_and_middleware_helpers():
    assert "configure" in xbbg.__all__
    assert "reset" in xbbg.__all__
    assert "add_middleware" in xbbg.__all__
    assert "RequestContext" in xbbg.__all__
    assert "connect" not in xbbg.__all__
    assert "disconnect" not in xbbg.__all__
    assert not hasattr(xbbg, "connect")
    assert not hasattr(xbbg, "disconnect")
    assert callable(xbbg.configure)
    assert callable(xbbg.add_middleware)


def test_arequest_preserves_centralized_request_logging(monkeypatch, caplog):
    class FakeEngine:
        async def request(self, params_dict):
            return _sample_batch()

    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())

    with caplog.at_level(logging.INFO, logger="xbbg.blp"):
        asyncio.run(
            blp.arequest(
                service=Service.REFDATA,
                operation=Operation.REFERENCE_DATA,
                securities=["IBM US Equity"],
                fields=["PX_LAST"],
            )
        )

    messages = [record.message for record in caplog.records]
    assert any("bloomberg" in message and "ReferenceDataRequest" in message for message in messages)
    assert any("request_id=req-" in message for message in messages)
