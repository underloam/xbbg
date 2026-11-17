"""Engine reconnect settings and connection diagnostics, without a Bloomberg session."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import xbbg
from xbbg import _core


def test_reconnect_grace_defaults_to_ten_seconds():
    assert _core.PyEngineConfig().reconnect_grace_ms == 10_000


@pytest.mark.parametrize("grace", [0, 2500])
def test_engine_config_accepts_reconnect_grace(grace):
    assert _core.PyEngineConfig(reconnect_grace_ms=grace).reconnect_grace_ms == grace


def test_engine_kwargs_accept_reconnect_grace(monkeypatch):
    seen = []

    def with_config(config):
        seen.append(config)
        return SimpleNamespace()

    monkeypatch.setattr(_core, "PyEngine", SimpleNamespace(with_config=with_config))

    engine = xbbg.Engine(reconnect_grace_ms=0)

    assert seen[0].reconnect_grace_ms == 0
    assert engine._config_snapshot.reconnect_grace_ms == 0


def test_reconnect_grace_is_a_valid_configure_key():
    from xbbg._engine import _normalize_config_kwargs

    assert _normalize_config_kwargs({"reconnect_grace_ms": 5000}) == {"reconnect_grace_ms": 5000}
