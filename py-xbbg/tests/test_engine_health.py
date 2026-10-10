"""Public Engine diagnostics delegate to the native engine without a session."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import xbbg
from xbbg import _core


@pytest.mark.parametrize("health", [[], [(0, "healthy"), (1, "reconnecting")]])
def test_worker_health_returns_native_diagnostics(monkeypatch, health):
    class FakeEngine:
        def __init__(self):
            self.calls = 0
            self.health = health

        def worker_health(self):
            self.calls += 1
            return self.health

    native = FakeEngine()
    monkeypatch.setattr(_core, "PyEngine", SimpleNamespace(with_config=lambda config: native))
    engine = xbbg.Engine()

    assert engine.worker_health() is health
    native.health = [(2, "healthy")]
    assert engine.worker_health() is native.health
    assert native.calls == 2


def test_worker_health_propagates_native_errors(monkeypatch):
    class FakeEngine:
        def worker_health(self):
            raise RuntimeError("health unavailable")

    monkeypatch.setattr(_core, "PyEngine", SimpleNamespace(with_config=lambda config: FakeEngine()))
    engine = xbbg.Engine()

    with pytest.raises(RuntimeError, match="health unavailable"):
        engine.worker_health()
