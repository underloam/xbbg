"""Measurement contracts shared by benchmark entry points."""

from __future__ import annotations

import atexit
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
from importlib import import_module, metadata
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import sysconfig
import time
import tracemalloc
from typing import Any
from collections.abc import Callable, Sequence


PYTHON_MEMORY_SCOPE = (
    "CPython tracemalloc peak during a separate untimed call; excludes Rust, "
    "Arrow native pools, allocator arenas, and process RSS"
)
WARM_TIMING_SCOPE = (
    "uninstrumented calls after discarded warmup on the package's reused session; "
    "session setup is excluded for every package"
)
FRESH_PROCESS_SCOPE = (
    "parent-observed fresh Python process spawn, helper and benchmark import, session setup, "
    "request, result construction, shape inspection, and flushed result marker; session teardown "
    "and child exit are excluded"
)
_FRESH_RESULT_PREFIX = "XBBG_BENCH_FRESH_RESULT="

_PDBLP_CONNECTION: Any = None


def reused_pdblp_connection() -> Any:
    """Return one pdblp session shared by all warm-session benchmark lanes."""
    global _PDBLP_CONNECTION
    if _PDBLP_CONNECTION is None:
        pdblp = import_module("pdblp")
        connection = pdblp.BCon(debug=False, timeout=5000)
        connection.start()
        _PDBLP_CONNECTION = connection
    return _PDBLP_CONNECTION


def close_shared_sessions() -> None:
    """Close benchmark-owned reusable competitor sessions."""
    global _PDBLP_CONNECTION
    connection, _PDBLP_CONNECTION = _PDBLP_CONNECTION, None
    if connection is not None:
        connection.stop()


atexit.register(close_shared_sessions)


@dataclass
class LiveMeasurement:
    """Separated fresh-process, warm-session, and Python-allocation observations."""

    fresh_process_first_result_ms: float
    fresh_process_sample_count: int
    fresh_process_scope: str
    warm_first_ms: float
    warm_mean_ms: float
    warm_median_ms: float
    warm_p95_ms: float | None
    warm_p99_ms: float | None
    warm_std_ms: float
    warm_max_ms: float
    warm_sample_count: int
    warmup_iterations: int
    timing_scope: str
    python_tracemalloc_peak_mb: float
    memory_sample_count: int
    memory_scope: str
    data_shape: tuple[int, ...]


def result_shape(result: Any) -> tuple[int, ...]:
    """Return a stable shape without materializing a lazy result."""
    shape = getattr(result, "shape", None)
    if isinstance(shape, tuple):
        return tuple(int(value) for value in shape)
    if hasattr(result, "__len__"):
        return (len(result),)
    return (1,)


def result_available(result: Any) -> bool:
    """Reject missing and zero-row responses so empty requests are not benchmark evidence."""
    if result is None:
        return False
    shape = result_shape(result)
    return len(shape) == 0 or shape[0] > 0


def empirical_percentile(values: Sequence[float], percentile: int) -> float | None:
    """Return a nearest-rank percentile only when its tail has at least one sample."""
    if percentile not in {95, 99}:
        raise ValueError("only p95 and p99 measurement contracts are supported")
    minimum = 20 if percentile == 95 else 100
    if len(values) < minimum:
        return None
    ordered = sorted(values)
    rank = (percentile * len(ordered) + 99) // 100
    return ordered[min(len(ordered) - 1, max(0, rank - 1))]


def measure_live_call(
    call: Callable[..., Any],
    args: Sequence[Any],
    *,
    iterations: int,
    warmup_iterations: int,
) -> LiveMeasurement | None:
    """Measure lifecycle, steady timing, and allocations in non-overlapping runs."""
    fresh_ms, fresh_available = _fresh_process_first_result(call, args)
    if not fresh_available:
        return None

    result: Any = None
    for _ in range(warmup_iterations):
        result = call(*args)
        if not result_available(result):
            return None

    timings: list[float] = []
    for _ in range(iterations):
        started = time.perf_counter()
        result = call(*args)
        timings.append((time.perf_counter() - started) * 1000)
        if not result_available(result):
            return None

    tracemalloc.start()
    try:
        allocation_result = call(*args)
        _current, python_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if not result_available(allocation_result):
        return None

    mean = statistics.mean(timings)
    return LiveMeasurement(
        fresh_process_first_result_ms=round(fresh_ms, 6),
        fresh_process_sample_count=1,
        fresh_process_scope=FRESH_PROCESS_SCOPE,
        warm_first_ms=timings[0],
        warm_mean_ms=mean,
        warm_median_ms=statistics.median(timings),
        warm_p95_ms=empirical_percentile(timings, 95),
        warm_p99_ms=empirical_percentile(timings, 99),
        warm_std_ms=statistics.stdev(timings) if len(timings) > 1 else 0.0,
        warm_max_ms=max(timings),
        warm_sample_count=len(timings),
        warmup_iterations=warmup_iterations,
        timing_scope=WARM_TIMING_SCOPE,
        python_tracemalloc_peak_mb=python_peak / 1024 / 1024,
        memory_sample_count=1,
        memory_scope=PYTHON_MEMORY_SCOPE,
        data_shape=result_shape(result),
    )


def _fresh_process_first_result(call: Callable[..., Any], args: Sequence[Any]) -> tuple[float, bool]:
    source = Path(call.__code__.co_filename).resolve()
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--fresh-probe",
        source.stem,
        call.__name__,
        json.dumps(list(args), separators=(",", ":"), default=str),
    ]
    env = os.environ.copy()
    env["XBBG_BENCH_FRESH_CHILD"] = "1"
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        cwd=source.parent,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output_lines: list[str] = []
    record: dict[str, Any] | None = None
    elapsed_ms: float | None = None
    assert process.stdout is not None
    for line in process.stdout:
        output_lines.append(line.rstrip())
        if line.startswith(_FRESH_RESULT_PREFIX) and record is None:
            elapsed_ms = (time.perf_counter() - started) * 1000
            record = json.loads(line.removeprefix(_FRESH_RESULT_PREFIX))
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(
            f"fresh-process benchmark failed for {source.stem}.{call.__name__}: " + "\n".join(output_lines).strip()
        )
    if record is None or elapsed_ms is None:
        raise RuntimeError("fresh-process benchmark returned no contract record: " + "\n".join(output_lines).strip())
    return elapsed_ms, bool(record.get("available"))


def _run_fresh_probe(module_name: str, function_name: str, encoded_args: str) -> int:
    module = import_module(module_name)
    call = getattr(module, function_name)
    args = json.loads(encoded_args)
    result = call(*args)
    record = {"available": result_available(result), "shape": result_shape(result)}
    print(f"{_FRESH_RESULT_PREFIX}{json.dumps(record)}", flush=True)
    close = getattr(module, "close_benchmark_sessions", None)
    if callable(close):
        close()
    close_shared_sessions()
    return 0


# Only these build facts are reported: rustFlags and targetFeatures describe the
# host CPU of a native build, and rustcVersion/target add nothing a comparison needs.
_REPORTED_BUILD_INFO = ("profile", "optLevel", "allocator", "gitCommit")


def collect_provenance(*, inputs: Any, benchmark_file: str) -> dict[str, Any]:
    """Describe what a report measured, without anything that identifies the machine or user.

    Reports are committed and shared, so provenance carries only what is needed to
    compare runs: package versions, the xbbg commit and build profile, and the Python
    implementation. Paths, host name, OS build, CPU model and count, compiler flags,
    environment variables, binary hashes and local times are deliberately left out.
    """
    build_info: dict[str, Any] = {}
    try:
        raw_build_info = getattr(import_module("xbbg._core"), "__build_info__", None)
    except ImportError:
        raw_build_info = None
    if isinstance(raw_build_info, dict):
        build_info = {key: raw_build_info[key] for key in _REPORTED_BUILD_INFO if key in raw_build_info}

    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    packages = {name: _package_version(name) for name in ("xbbg", "pyarrow", "pandas", "polars", "narwhals", "blpapi")}
    return {
        "schema_version": 2,
        "benchmark_file": benchmark_file,
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "free_threaded": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
            "gil_enabled": is_gil_enabled() if callable(is_gil_enabled) else True,
        },
        "build": build_info,
        "dependencies": packages,
        "inputs_sha256": hashlib.sha256(
            json.dumps(inputs, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest(),
    }


def report_date() -> str:
    """UTC calendar date of a run; a local time and zone offset would reveal where it ran."""
    return datetime.now(UTC).date().isoformat()


def archive_path(directory: Path, stem: str, suffix: str) -> Path:
    """First unused ``{stem}_{YYYYMMDD}_{n}{suffix}`` for today's UTC date.

    Archive names number runs within a day instead of recording the time of day.
    """
    day = datetime.now(UTC).strftime("%Y%m%d")
    run = 1
    while (path := directory / f"{stem}_{day}_{run}{suffix}").exists():
        run += 1
    return path


def _package_version(name: str) -> str:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "not-installed"


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--fresh-probe":
        raise SystemExit(_run_fresh_probe(sys.argv[2], sys.argv[3], sys.argv[4]))
    raise SystemExit("benchmark_contracts.py is an internal benchmark helper")
