"""Native engine teardown runs off interpreter threads, and interpreter exit waits for it.

The exit check needs the private subscription driver, which exists only in native
extensions built with `test-support`. Set XBBG_REQUIRE_TEST_SUPPORT=1 in its dedicated
run to fail rather than skip when the wrong extension is installed.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import textwrap
import threading
import weakref

import pytest

import xbbg
from xbbg import _core, _engine

WAIT_TIMEOUT_S = 10

EXIT_WITH_PENDING_READ = textwrap.dedent(
    """
    import asyncio

    # Importing _engine registers xbbg's interpreter-exit cleanup.
    from xbbg import _core, _engine

    TOPIC = "SYNTHETIC EXIT Equity"


    async def hold_pending_read():
        owner = await _core._test_subscription_fixture()
        sub = await owner.subscribe([TOPIC], ["BID"])
        read = asyncio.ensure_future(sub.__anext__())
        await owner.wait_legacy_read_started()
        return owner, sub, read


    loop = asyncio.new_event_loop()
    held = loop.run_until_complete(hold_pending_read())
    if held[2].done():
        raise SystemExit("the synthetic read completed before exit")
    print("exiting with a pending read", flush=True)
    """
)


def test_wait_native_teardown_returns_when_nothing_is_pending():
    returned = threading.Event()

    def wait():
        _core._wait_native_teardown()
        returned.set()

    threading.Thread(target=wait, daemon=True).start()

    assert returned.wait(WAIT_TIMEOUT_S), "_wait_native_teardown() blocked with no teardown pending"


def test_atexit_cleanup_waits_for_native_teardown_after_releasing_the_engine(monkeypatch):
    events = []

    class FakeEngine:
        def signal_shutdown(self):
            events.append("signal_shutdown")

    engine = FakeEngine()
    released = weakref.ref(engine)

    class FakeCore:
        @staticmethod
        def _signal_interpreter_shutdown():
            events.append("interpreter_shutdown")

        @staticmethod
        def _wait_native_teardown():
            events.append(("wait_native_teardown", released() is None))

    monkeypatch.setitem(sys.modules, "xbbg._core", FakeCore)
    monkeypatch.setattr(_engine, "_stop_notebook_sync_loop", lambda: events.append("stop_sync_loop"))
    monkeypatch.setattr(_engine, "_engine", engine)
    del engine

    _engine._atexit_cleanup()

    assert events == [
        "interpreter_shutdown",
        "signal_shutdown",
        "stop_sync_loop",
        ("wait_native_teardown", True),
    ]
    assert _engine._engine is None


def test_interpreter_exit_with_a_pending_subscription_read_is_clean():
    if getattr(_core, "_test_subscription_fixture", None) is None:
        if os.environ.get("XBBG_REQUIRE_TEST_SUPPORT") == "1":
            pytest.fail("native extension was not built with pyo3-xbbg/test-support")
        pytest.skip("requires a native extension built with pyo3-xbbg/test-support")
    env = {**os.environ, "PYTHONPATH": str(Path(xbbg.__file__).resolve().parents[1])}

    result = subprocess.run(
        [sys.executable, "-c", EXIT_WITH_PENDING_READ],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "exiting with a pending read" in result.stdout
    for failure in ("panicked at", "Fatal Python error", "Failed to wait for native engine teardown"):
        assert failure not in result.stderr, result.stderr
