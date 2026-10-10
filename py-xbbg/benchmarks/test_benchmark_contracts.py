"""Contracts for benchmark measurements and supported package lanes."""

from __future__ import annotations

from importlib import import_module

from benchmark_contracts import empirical_percentile
from config import PACKAGES
import pytest


def test_p95_requires_twenty_observations() -> None:
    assert empirical_percentile(range(19), 95) is None
    assert empirical_percentile(range(20), 95) is not None


def test_p99_requires_one_hundred_observations() -> None:
    assert empirical_percentile(range(99), 99) is None
    assert empirical_percentile(range(100), 99) is not None


def test_other_percentiles_are_rejected() -> None:
    with pytest.raises(ValueError):
        empirical_percentile([1.0] * 100, 90)


def test_package_configuration_uses_real_imports() -> None:
    assert {name: package["import"] for name, package in PACKAGES.items()} == {
        "xbbg-rust": "xbbg",
        "pdblp": "pdblp",
    }


@pytest.mark.parametrize(
    ("operation", "scenario_count", "packages"),
    [
        ("bdp", 2, ["xbbg-rust", "pdblp"]),
        ("bdh", 2, ["xbbg-rust", "pdblp"]),
        ("bdib", 1, ["xbbg-rust", "pdblp"]),
        ("bdtick", 3, ["xbbg-rust", "pdblp"]),
        ("bql", 2, ["xbbg-rust"]),
    ],
)
def test_entrypoints_dispatch_only_supported_lanes(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    scenario_count: int,
    packages: list[str],
) -> None:
    module = import_module(f"bench_{operation}")
    calls = []

    def capture_lane(package_name, adapter, *args):
        calls.append((package_name, adapter.__name__))

    monkeypatch.setattr(module, f"benchmark_{operation}", capture_lane)

    assert module.main() == []
    adapters = {"xbbg-rust": "run_xbbg_rust", "pdblp": "run_pdblp"}
    assert calls == [(package, adapters[package]) for package in packages] * scenario_count
    assert {name for name in vars(module) if name.startswith("run_")} == {adapters[package] for package in packages}
