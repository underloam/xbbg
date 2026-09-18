"""Pytest configuration for xbbg tests."""

from __future__ import annotations

import os
import sys

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


def pytest_collection_modifyitems(config, items):
    """Auto-skip live tests when running in CI (no Bloomberg Terminal)."""
    if not os.environ.get("CI"):
        return

    skip_live = pytest.mark.skip(reason="Bloomberg Terminal not available in CI")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


def _fail_engine_construction(*_args: object, **_kwargs: object) -> None:
    pytest.fail(
        "unit test constructed a real Bloomberg engine (xbbg._core.PyEngine). Without a "
        "terminal the SDK blocks for its whole start-attempt cycle and has hung Windows CI "
        "runners until the job timeout. Patch blp._get_engine with a fake engine, or mark "
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
def _no_real_engine(request, monkeypatch):
    """Fail fast when a non-live test starts a real Bloomberg session.

    ``pytest.fail`` raises a ``BaseException`` subclass, so it is not swallowed by the
    ``except Exception`` fallbacks around schema lookups.
    """
    if "live" in request.keywords or "integration" in request.keywords:
        return
    from xbbg import _core

    monkeypatch.setattr(_core, "PyEngine", _ForbiddenPyEngine)


@pytest.fixture
def sample_tickers():
    """Fixture providing sample ticker symbols."""
    return ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]


@pytest.fixture
def sample_fields():
    """Fixture providing sample field names."""
    return ["PX_LAST", "VOLUME", "NAME"]
