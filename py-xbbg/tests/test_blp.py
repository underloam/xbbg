"""Tests for offline xbbg.blp behavior.

These tests verify selected Python API behavior without requiring a Bloomberg connection.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import inspect
from pathlib import Path
import sys
import threading
from types import ModuleType
from unittest import TestCase

import pytest

_CASE = TestCase()


class TestNotebookSyncBridge:
    """Tests for sync wrappers called from running event loops."""

    def test_generic_async_context_still_raises(self, monkeypatch):
        """Non-notebook async callers should be directed to the async API."""
        from xbbg import blp

        async def fake_request():
            return "ok"

        wrapper = blp._build_sync_wrapper("bdp", fake_request)
        monkeypatch.setattr(blp, "_is_notebook_context", lambda: False)

        async def call_wrapper():
            wrapper()

        with pytest.raises(RuntimeError, match="await abdp"):
            asyncio.run(call_wrapper())

    def test_marimo_context_uses_notebook_bridge(self, monkeypatch):
        """Marimo callers should use the notebook bridge without requiring IPykernel."""
        from xbbg import blp

        marimo = ModuleType("marimo")
        marimo.running_in_notebook = lambda: True
        monkeypatch.setitem(sys.modules, "marimo", marimo)
        monkeypatch.setitem(sys.modules, "IPython", None)
        monkeypatch.setattr(
            blp,
            "_run_in_notebook_sync_bridge",
            lambda async_func, args, kwargs: async_func.__name__,
        )

        async def call_bdp():
            return blp.bdp("AAPL US Equity", "PX_LAST")

        _CASE.assertEqual(asyncio.run(call_bdp()), "abdp")

    def test_imported_marimo_outside_notebook_still_raises(self, monkeypatch):
        """Importing marimo alone should not make a generic event loop a notebook."""
        from xbbg import blp

        marimo = ModuleType("marimo")
        marimo.running_in_notebook = lambda: False
        monkeypatch.setitem(sys.modules, "marimo", marimo)
        monkeypatch.setitem(sys.modules, "IPython", None)

        async def call_bdp():
            return blp.bdp("AAPL US Equity", "PX_LAST")

        with pytest.raises(RuntimeError, match="await abdp"):
            asyncio.run(call_bdp())

    def test_notebook_context_uses_background_loop_and_preserves_contextvars(self, monkeypatch):
        """Notebook callers should block on a background loop without losing context."""
        from xbbg import blp

        scoped_value = contextvars.ContextVar("scoped_value", default="missing")

        async def fake_request():
            return scoped_value.get(), threading.current_thread().name

        wrapper = blp._build_sync_wrapper("bdp", fake_request)
        monkeypatch.setattr(blp, "_is_notebook_context", lambda: True)
        token = scoped_value.set("active-engine")

        async def call_wrapper():
            return wrapper()

        try:
            value, thread_name = asyncio.run(call_wrapper())
        finally:
            scoped_value.reset(token)
            blp._stop_notebook_sync_loop()

        _CASE.assertEqual(value, "active-engine")
        _CASE.assertNotEqual(thread_name, threading.current_thread().name)

    def test_notebook_context_propagates_async_exceptions(self, monkeypatch):
        """Async failures should surface unchanged to the sync caller."""
        from xbbg import blp

        class ExpectedError(Exception):
            pass

        async def fake_request():
            raise ExpectedError("boom")

        wrapper = blp._build_sync_wrapper("bdh", fake_request)
        monkeypatch.setattr(blp, "_is_notebook_context", lambda: True)

        async def call_wrapper():
            wrapper()

        try:
            with pytest.raises(ExpectedError, match="boom"):
                asyncio.run(call_wrapper())
        finally:
            blp._stop_notebook_sync_loop()

    def test_notebook_context_propagates_base_exceptions_without_hanging(self):
        from xbbg import blp

        class BridgeAbort(BaseException):
            pass

        expected = BridgeAbort("abort")
        received: list[BaseException] = []

        async def fake_request():
            raise expected

        def call_bridge():
            try:
                blp._run_in_notebook_sync_bridge(fake_request, (), {})
            except BaseException as error:
                received.append(error)

        caller = threading.Thread(target=call_bridge, daemon=True)
        caller.start()
        caller.join(timeout=1)
        try:
            assert not caller.is_alive()
            assert received == [expected]
        finally:
            blp._stop_notebook_sync_loop()

    def test_stopping_notebook_bridge_cancels_active_call_and_releases_thread(self):
        from xbbg import blp

        started = threading.Event()
        received: list[BaseException] = []

        async def fake_request():
            started.set()
            await asyncio.Event().wait()

        def call_bridge():
            try:
                blp._run_in_notebook_sync_bridge(fake_request, (), {})
            except BaseException as error:
                received.append(error)

        caller = threading.Thread(target=call_bridge, daemon=True)
        caller.start()
        assert started.wait(timeout=1)

        blp._stop_notebook_sync_loop()
        caller.join(timeout=1)

        assert not caller.is_alive()
        assert len(received) == 1
        assert isinstance(received[0], concurrent.futures.CancelledError)
        assert not any(
            thread.name == "xbbg-notebook-sync-bridge" and thread.is_alive() for thread in threading.enumerate()
        )

    def test_stop_cancels_call_accepted_before_loop_dispatch(self, monkeypatch):
        from xbbg import blp

        loop = blp._ensure_notebook_sync_loop()
        loop_blocked = threading.Event()
        release_loop = threading.Event()
        call_accepted = threading.Event()
        received: list[BaseException] = []

        def block_loop():
            loop_blocked.set()
            release_loop.wait(timeout=1)

        loop.call_soon_threadsafe(block_loop)
        assert loop_blocked.wait(timeout=1)

        original_start = blp._notebook_sync_bridge.start

        def mark_accepted(*args, **kwargs):
            call = original_start(*args, **kwargs)
            call_accepted.set()
            return call

        async def fake_request():
            return "too late"

        def call_bridge():
            try:
                blp._run_in_notebook_sync_bridge(fake_request, (), {})
            except BaseException as error:
                received.append(error)

        monkeypatch.setattr(blp._notebook_sync_bridge, "start", mark_accepted)
        caller = threading.Thread(target=call_bridge, daemon=True)
        caller.start()
        assert call_accepted.wait(timeout=1)

        stopper = threading.Thread(target=blp._stop_notebook_sync_loop, daemon=True)
        stopper.start()
        try:
            caller.join(timeout=1)
            assert not caller.is_alive()
            assert len(received) == 1
            assert isinstance(received[0], concurrent.futures.CancelledError)
        finally:
            release_loop.set()
            stopper.join(timeout=1)
            assert not stopper.is_alive()

    def test_public_sync_wrappers_use_notebook_bridge(self, monkeypatch):
        """Generated and manual sync wrappers, including BQL (#353) and streams, bridge in notebooks."""
        from xbbg import blp

        def fake_bridge(async_func, args, kwargs):
            return async_func.__name__, args, kwargs

        monkeypatch.setattr(blp, "_is_notebook_context", lambda: True)
        monkeypatch.setattr(blp, "_run_in_notebook_sync_bridge", fake_bridge)

        query = "get(px_last) for(['AAPL US Equity'])"

        async def call_bql():
            return blp.bql(query, backend="pandas")

        async def call_subscribe():
            return blp.subscribe(["AAPL US Equity"], ["LAST_PRICE"])

        _CASE.assertEqual(asyncio.run(call_bql()), ("abql", (query,), {"backend": "pandas"}))
        _CASE.assertEqual(
            asyncio.run(call_subscribe()),
            ("asubscribe", (["AAPL US Equity"], ["LAST_PRICE"]), {}),
        )


class TestEngineContextScopes:
    """Scoped engines restore routing across nesting and concurrent tasks."""

    @staticmethod
    def _make_engine(monkeypatch):
        from xbbg import _core, blp

        native_engine = object()

        class FakeConfig:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakePyEngine:
            @staticmethod
            def with_config(_config):
                return native_engine

        monkeypatch.setattr(_core, "PyEngineConfig", FakeConfig)
        monkeypatch.setattr(_core, "PyEngine", FakePyEngine)
        return blp, blp.Engine(), native_engine

    def test_nested_same_engine_scope_restores_previous_routing(self, monkeypatch):
        blp, engine, native_engine = self._make_engine(monkeypatch)
        previous = object()
        previous_token = blp._active_engine.set(previous)
        try:
            with engine:
                assert blp._get_engine() is native_engine
                with engine:
                    assert blp._get_engine() is native_engine
                assert blp._get_engine() is native_engine
            assert blp._active_engine.get() is previous
        finally:
            blp._active_engine.reset(previous_token)

    def test_concurrent_same_engine_scopes_restore_each_task_context(self, monkeypatch):
        blp, engine, native_engine = self._make_engine(monkeypatch)

        async def exercise_scopes():
            entered = 0
            both_entered = asyncio.Event()

            async def worker(previous):
                nonlocal entered
                previous_token = blp._active_engine.set(previous)
                try:
                    async with engine:
                        entered += 1
                        if entered == 2:
                            both_entered.set()
                        await both_entered.wait()
                        assert blp._get_engine() is native_engine
                    assert blp._active_engine.get() is previous
                finally:
                    blp._active_engine.reset(previous_token)

            await asyncio.gather(worker(object()), worker(object()))

        asyncio.run(exercise_scopes())


class TestExtSyncify:
    """Tests for xbbg.ext sync wrappers sharing the core async boundary."""

    def test_sync_context_runs_helper(self):
        from xbbg.ext._utils import _syncify

        async def afake_ext(value):
            return f"ok:{value}"

        wrapper = _syncify(afake_ext)

        _CASE.assertEqual(wrapper("value"), "ok:value")
        _CASE.assertEqual(wrapper.__name__, "fake_ext")

    def test_generic_async_context_rejects_before_creating_coroutine(self):
        from xbbg.ext._utils import _syncify

        created = False

        def afake_ext():
            nonlocal created
            created = True

            async def inner():
                return "ok"

            return inner()

        wrapper = _syncify(afake_ext)

        async def call_wrapper():
            wrapper()

        with pytest.raises(RuntimeError, match="await afake_ext"):
            asyncio.run(call_wrapper())

        _CASE.assertFalse(created)

    def test_notebook_context_uses_core_bridge(self, monkeypatch):
        from xbbg import blp
        from xbbg.ext._utils import _syncify

        async def afake_ext(*args, **kwargs):
            return args, kwargs

        wrapper = _syncify(afake_ext)

        def fake_bridge(async_func, args, kwargs):
            return async_func.__name__, args, kwargs

        monkeypatch.setattr(blp, "_is_notebook_context", lambda: True)
        monkeypatch.setattr(blp, "_run_in_notebook_sync_bridge", fake_bridge)

        async def call_wrapper():
            return wrapper("abc", flag=True)

        name, args, kwargs = asyncio.run(call_wrapper())

        _CASE.assertEqual(name, "afake_ext")
        _CASE.assertEqual(args, ("abc",))
        _CASE.assertEqual(kwargs, {"flag": True})


class TestBdtick:
    """Tests for bdtick (tick data) function."""

    def test_bdtick_sync_signature_matches_async(self):
        """bdtick exposes the generated sync signature for IDE/runtime help."""
        import xbbg
        from xbbg import blp

        sync_sig = inspect.signature(blp.bdtick)
        async_sig = inspect.signature(blp.abdtick)

        _CASE.assertEqual(sync_sig, async_sig)
        _CASE.assertEqual(inspect.signature(xbbg.bdtick), async_sig)
        _CASE.assertIn("request_tz", sync_sig.parameters)
        _CASE.assertIn("output_tz", sync_sig.parameters)
        _CASE.assertIn("event_types", sync_sig.parameters)

    def test_top_level_stub_reexports_bdtick(self):
        """Generated top-level stub preserves IDE help for xbbg.bdtick."""
        import xbbg

        stub = Path(xbbg.__file__).with_name("__init__.pyi")
        text = stub.read_text()

        _CASE.assertIn("bdtick as bdtick", text)
        _CASE.assertIn('"bdtick"', text)
        _CASE.assertNotIn("__all__ = []", text)


class TestGeneratedEndpointFieldTypeCache:
    """Generated endpoint plans reuse resolved field metadata without leaking overrides."""

    def test_invalidation_during_resolution_does_not_republish_stale_entry(self, monkeypatch):
        from xbbg import blp

        async def exercise_invalidation():
            started = asyncio.Event()
            release = asyncio.Event()

            class FakeEngine:
                async def resolve_field_types(self, fields, overrides, default_type):
                    started.set()
                    await release.wait()
                    return dict.fromkeys(fields, default_type)

            monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
            blp._clear_field_type_resolution_cache()
            resolution = asyncio.create_task(blp._resolve_field_types_cached(["PX_LAST"], None, "float64"))
            await started.wait()
            blp._clear_field_type_resolution_cache()
            release.set()

            assert await resolution == {"PX_LAST": "float64"}
            with blp._FIELD_TYPE_RESOLUTION_CACHE_LOCK:
                assert blp._FIELD_TYPE_RESOLUTION_CACHE == {}

        try:
            asyncio.run(exercise_invalidation())
        finally:
            blp._clear_field_type_resolution_cache()

    @pytest.mark.parametrize(
        ("builder_name", "default_type", "extra_args"),
        [
            ("_build_abdp_plan", "string", {}),
            (
                "_build_abdh_plan",
                "float64",
                {"start_date": "20240101", "end_date": "20240102"},
            ),
        ],
    )
    def test_repeated_fields_resolve_types_once(self, monkeypatch, builder_name, default_type, extra_args):
        from xbbg import blp

        class FakeEngine:
            def __init__(self):
                self.calls = []

            async def resolve_field_types(self, fields, overrides, resolved_default_type):
                self.calls.append((list(fields), overrides, resolved_default_type))
                return {field: f"{resolved_default_type}:{field}" for field in fields}

        async def route_kwargs(_service, _operation, _kwargs):
            return [], []

        fake_engine = FakeEngine()
        monkeypatch.setattr(blp, "_get_engine", lambda: fake_engine)
        monkeypatch.setattr(blp, "_aroute_kwargs", route_kwargs)
        blp._clear_field_type_resolution_cache()
        args = {
            "tickers": ["IBM US Equity"],
            "flds": ["PX_LAST", "VOLUME"],
            "backend": None,
            "format": None,
            "field_types": None,
            "kwargs": {},
            "include_security_errors": False,
            "return_eids": False,
            "validate_fields": None,
            **extra_args,
        }

        try:
            first = asyncio.run(getattr(blp, builder_name)(args))
            second = asyncio.run(getattr(blp, builder_name)(args))
        finally:
            blp._clear_field_type_resolution_cache()

        expected_types = {
            "PX_LAST": f"{default_type}:PX_LAST",
            "VOLUME": f"{default_type}:VOLUME",
        }
        _CASE.assertEqual(first.request_kwargs["field_types"], expected_types)
        _CASE.assertEqual(second.request_kwargs["field_types"], expected_types)
        _CASE.assertEqual(fake_engine.calls, [(["PX_LAST", "VOLUME"], None, default_type)])

    @pytest.mark.parametrize(
        ("builder_name", "default_type", "extra_args"),
        [
            ("_build_abdp_plan", "string", {}),
            (
                "_build_abdh_plan",
                "float64",
                {"start_date": "20240101", "end_date": "20240102"},
            ),
        ],
    )
    def test_per_call_field_type_overrides_do_not_poison_cached_resolution(
        self, monkeypatch, builder_name, default_type, extra_args
    ):
        from xbbg import blp

        class FakeEngine:
            def __init__(self):
                self.calls = []

            async def resolve_field_types(self, fields, overrides, resolved_default_type):
                self.calls.append((list(fields), overrides, resolved_default_type))
                return {field: f"{resolved_default_type}:{field}" for field in fields}

        async def route_kwargs(_service, _operation, _kwargs):
            return [], []

        fake_engine = FakeEngine()
        monkeypatch.setattr(blp, "_get_engine", lambda: fake_engine)
        monkeypatch.setattr(blp, "_aroute_kwargs", route_kwargs)
        blp._clear_field_type_resolution_cache()

        def plan_args(field_types):
            return {
                "tickers": ["IBM US Equity"],
                "flds": ["PX_LAST", "VOLUME"],
                "backend": None,
                "format": None,
                "field_types": field_types,
                "kwargs": {},
                "include_security_errors": False,
                "return_eids": False,
                "validate_fields": None,
                **extra_args,
            }

        try:
            override_plan = asyncio.run(
                getattr(blp, builder_name)(plan_args({"PX_LAST": "decimal128", "VOLUME": "int64"}))
            )
            base_plan = asyncio.run(getattr(blp, builder_name)(plan_args(None)))
        finally:
            blp._clear_field_type_resolution_cache()

        _CASE.assertEqual(
            override_plan.request_kwargs["field_types"],
            {"PX_LAST": "decimal128", "VOLUME": "int64"},
        )
        _CASE.assertEqual(
            base_plan.request_kwargs["field_types"],
            {
                "PX_LAST": f"{default_type}:PX_LAST",
                "VOLUME": f"{default_type}:VOLUME",
            },
        )
        _CASE.assertEqual(fake_engine.calls, [(["PX_LAST", "VOLUME"], None, default_type)])
