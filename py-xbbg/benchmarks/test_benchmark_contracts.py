"""Contracts for benchmark measurements and supported package lanes."""

from __future__ import annotations

from importlib import import_module
import json
import os
from pathlib import Path
import platform
import sys

from benchmark_contracts import collect_provenance, empirical_percentile
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


def test_provenance_identifies_neither_machine_nor_user() -> None:
    """Reports are committed: provenance may carry versions and build profile, nothing host-specific."""
    provenance = collect_provenance(inputs={"tickers": ["IBM US Equity"]}, benchmark_file="run_all.py")

    assert set(provenance) == {"schema_version", "benchmark_file", "runtime", "build", "dependencies", "inputs_sha256"}
    assert set(provenance["runtime"]) == {"python", "implementation", "free_threaded", "gil_enabled"}
    assert set(provenance["build"]) <= {"profile", "optLevel", "allocator", "gitCommit"}
    text = json.dumps(provenance)
    identifying = [
        sys.executable,
        str(Path.home()),
        Path.home().name,
        platform.node(),
        platform.processor(),
        platform.platform(),
        os.environ.get("USERNAME", ""),
        os.environ.get("USER", ""),
    ]
    for value in filter(None, identifying):
        assert value not in text and json.dumps(value)[1:-1] not in text, value
    assert "\\" not in text and "/" not in text


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
