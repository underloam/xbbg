"""Pytest configuration for xbbg tests."""

from __future__ import annotations

import inspect
import os
import sys
import sysconfig
from unittest import mock

import pytest

# Ensure the py-xbbg/src package is in path
pkg_root = os.path.dirname(os.path.dirname(__file__))
python_src = os.path.join(pkg_root, "src")
if python_src not in sys.path:
    sys.path.insert(0, python_src)


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (requires Bloomberg connection)",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow running",
    )
    config.addinivalue_line(
        "markers",
        "live: mark test as requiring a live Bloomberg Terminal or B-PIPE connection",
    )
    # pytest-run-parallel's marker, registered here too so the suite runs without it.
    config.addinivalue_line(
        "markers",
        "thread_unsafe(reason=None): run this test in one thread under pytest-run-parallel",
    )


_ASYNC_THREAD_UNSAFE = pytest.mark.thread_unsafe(
    reason="pytest-asyncio drives async tests on one event loop; pytest-run-parallel cannot run them in threads"
)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Run async tests single-threaded under pytest-run-parallel; skip live tests in CI."""
    for item in items:
        if inspect.iscoroutinefunction(getattr(item, "obj", None)):
            item.add_marker(_ASYNC_THREAD_UNSAFE)

    if not os.environ.get("CI"):
        return

    skip_live = pytest.mark.skip(reason="Bloomberg Terminal not available in CI")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


def pytest_sessionfinish(session):
    """Fail a free-threaded run whose imports re-enabled the GIL (unless the run forced it on)."""
    if not sysconfig.get_config_var("Py_GIL_DISABLED") or getattr(sys.flags, "gil", None) == 1:
        return
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if gil_enabled is not None and gil_enabled():
        if session.exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
        sys.stderr.write(
            "An import re-enabled the GIL on this free-threaded interpreter; CPython's RuntimeWarning "
            "about enabling the GIL names the extension module.\n"
        )


def _fail_engine_construction(*_args: object, **_kwargs: object) -> None:
    pytest.fail(
        "unit test constructed a real Bloomberg engine (xbbg._core.PyEngine). Without a "
        "terminal the SDK blocks for its whole start-attempt cycle and has hung Windows CI "
        "runners until the job timeout. Patch xbbg._engine._get_engine with a fake engine, or mark "
        "the test live/integration."
    )


class _ForbiddenPyEngine:
    """Stand-in for ``xbbg._core.PyEngine``; every constructor path fails the test."""

    def __new__(cls, *args: object, **kwargs: object):
        _fail_engine_construction()

    @staticmethod
    def with_config(*args: object, **kwargs: object) -> None:
        _fail_engine_construction()


@pytest.fixture(autouse=True)
def _no_real_engine(request):
    """Fail fast when a non-live test starts a real Bloomberg session.

    ``pytest.fail`` raises a ``BaseException`` subclass, so it is not swallowed by the
    ``except Exception`` fallbacks around schema lookups. The patch is installed during
    setup, before pytest-run-parallel starts a test's threads; ``monkeypatch`` would
    mark every test thread-unsafe instead.
    """
    if "live" in request.keywords or "integration" in request.keywords:
        yield
        return
    from xbbg import _core

    with mock.patch.object(_core, "PyEngine", _ForbiddenPyEngine):
        yield
