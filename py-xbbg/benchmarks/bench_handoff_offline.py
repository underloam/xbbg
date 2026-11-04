"""Offline benchmarks for native xbbg binding handoff conversions.

The fixture setup intentionally builds native ``xbbg.ArrowTable`` objects once and
keeps that setup outside the measured sections.  Timed lanes therefore isolate
Rust-native Arrow carrier handoff into Python dataframe/Arrow objects, plus the
explicit materialising ``to_pylist()`` slow path for contrast.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import importlib
import importlib.util
import ctypes
import json
import logging
import os
import shutil
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
from typing import Any

sys.stdout.reconfigure(encoding="utf-8")

if "BLPAPI_ROOT" not in os.environ:
    repo_root = Path(__file__).resolve().parents[2]
    for bundled_sdk in (
        repo_root / "vendor" / "blpapi-sdk" / "3.26.5.1",
        repo_root / "vendor" / "blpapi-sdk" / "3.26.5.1" / "Darwin",
        repo_root / "js-xbbg" / "lib",
    ):
        if (bundled_sdk / "libblpapi3_64.so").is_file() or (bundled_sdk / "lib" / "libblpapi3_64.so").is_file():
            os.environ["BLPAPI_ROOT"] = str(bundled_sdk)
            break

known_sdk_lib = (
    Path(__file__).resolve().parents[2] / "vendor" / "blpapi-sdk" / "3.26.5.1" / "Darwin" / "libblpapi3_64.so"
)
known_runtime_lib = Path(__file__).resolve().parents[2] / ".pixi" / "envs" / "default" / "lib" / "libblpapi3_64.so"
if known_sdk_lib.is_file() and known_runtime_lib.parent.is_dir() and not known_runtime_lib.exists():
    try:
        known_runtime_lib.symlink_to(known_sdk_lib)
    except OSError:
        try:
            shutil.copy2(known_sdk_lib, known_runtime_lib)
        except OSError:
            pass

sdk_root = os.environ.get("BLPAPI_ROOT")
if sdk_root:
    sdk_path = Path(sdk_root)
    sdk_lib = sdk_path / "libblpapi3_64.so"
    if not sdk_lib.is_file():
        sdk_lib = sdk_path / "lib" / "libblpapi3_64.so"
    if not sdk_lib.is_file():
        sdk_lib = sdk_path / "Darwin" / "libblpapi3_64.so"
    if sdk_lib.is_file():
        runtime_libs = (
            Path(sys.executable).parent.parent / "lib" / "libblpapi3_64.so",
            Path(__file__).resolve().parents[2] / ".pixi" / "envs" / "default" / "lib" / "libblpapi3_64.so",
        )
        for runtime_lib in runtime_libs:
            if runtime_lib.exists() or not runtime_lib.parent.is_dir():
                continue
            try:
                runtime_lib.symlink_to(sdk_lib)
            except OSError:
                try:
                    shutil.copy2(sdk_lib, runtime_lib)
                except OSError:
                    pass
        try:
            ctypes.CDLL(str(sdk_lib), mode=ctypes.RTLD_GLOBAL)
        except OSError:
            pass

if sys.platform == "darwin" and os.environ.get("XBBG_HANDOFF_DYLD_REEXEC") != "1":
    sdk_root = os.environ.get("BLPAPI_ROOT")
    if sdk_root:
        sdk_path = Path(sdk_root)
        sdk_lib_dir = sdk_path
        if not (sdk_lib_dir / "libblpapi3_64.so").is_file():
            sdk_lib_dir = sdk_path / "lib"
        if not (sdk_lib_dir / "libblpapi3_64.so").is_file() and (sdk_path / "Darwin" / "libblpapi3_64.so").is_file():
            sdk_lib_dir = sdk_path / "Darwin"
        if (sdk_lib_dir / "libblpapi3_64.so").is_file():
            existing_dirs = [item for item in os.environ.get("DYLD_LIBRARY_PATH", "").split(":") if item]
            if str(sdk_lib_dir) not in existing_dirs:
                env = os.environ.copy()
                env["DYLD_LIBRARY_PATH"] = ":".join([str(sdk_lib_dir), *existing_dirs])
                env["XBBG_HANDOFF_DYLD_REEXEC"] = "1"
                os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

import xbbg
from xbbg.backend import Backend, convert_backend_frame

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).resolve().parent / "results"
DEFAULT_WARMUP = 2
DEFAULT_ITERATIONS = 30
QUICK_WARMUP = 1
QUICK_ITERATIONS = 3


@dataclass(frozen=True)
class ShapeCase:
    name: str
    rows: int
    columns: int
    batch_rows: int
    null_every: int
    string_bytes: int
    string_cardinality: int


DEFAULT_CASES: tuple[ShapeCase, ...] = (
    ShapeCase("small_dense", 100, 5, 100, 0, 12, 100),
    ShapeCase("chunked_nullable", 4_096, 8, 64, 10, 64, 256),
    ShapeCase("wide_string_heavy", 1_024, 25, 128, 2, 256, 1_024),
    ShapeCase("large_mixed", 100_000, 10, 1_024, 0, 16, 10_000),
)


@dataclass(frozen=True)
class FixtureTable:
    case: ShapeCase
    native_batches: tuple[Any, ...]
    setup_ms: float
    input_checksum: int


@dataclass(frozen=True)
class ConversionObservation:
    shape: tuple[int, int]
    checksum: int
    retained: Any


@dataclass(frozen=True)
class BenchmarkResult:
    package: str
    operation: str
    scenario: str
    consumer_scope: str
    rows: int
    columns: int
    native_batches: int
    batch_rows: int
    null_every: int
    string_bytes: int
    string_cardinality: int
    timing_scope: str
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    p95_ms: float | None
    p99_ms: float | None
    timing_sample_count: int
    warmup_iterations: int
    setup_ms: float
    python_tracemalloc_peak_mb: float
    memory_sample_count: int
    memory_scope: str
    pyarrow_pool_before_bytes: int | None
    pyarrow_pool_after_bytes: int | None
    pyarrow_pool_max_bytes: int | None
    pyarrow_pool_scope: str
    data_shape: tuple[int, int]
    checksum: int
    input_checksum: int
    offline: bool


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline xbbg native handoff benchmark")
    parser.add_argument("--quick", action="store_true", help="Run fewer iterations and only the first two cases")
    parser.add_argument("--iterations", type=positive_int, default=None, help="Measured iterations per lane")
    parser.add_argument("--warmup", type=non_negative_int, default=None, help="Warmup iterations per lane")
    parser.add_argument(
        "--shape",
        action="append",
        default=None,
        metavar="ROWSxCOLS",
        help="Custom dense single-batch shape; may be repeated",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=RESULTS_DIR,
        help="Directory for JSON results (default: py-xbbg/benchmarks/results)",
    )
    parser.add_argument("--json", action="store_true", help="Print only the JSON result document")
    return parser.parse_args(argv)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_shape(value: str) -> ShapeCase:
    normalized = value.lower().replace("_", "")
    try:
        rows_text, columns_text = normalized.split("x", 1)
        rows = int(rows_text)
        columns = int(columns_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid shape {value!r}; expected ROWSxCOLS") from exc
    if rows <= 0 or columns <= 0:
        raise argparse.ArgumentTypeError("shape dimensions must be positive")
    return ShapeCase(f"custom_{rows}x{columns}", rows, columns, rows, 0, 16, min(rows, 10_000))


def selected_cases(shape_args: Sequence[str] | None, quick: bool) -> tuple[ShapeCase, ...]:
    if shape_args:
        return tuple(parse_shape(shape) for shape in shape_args)
    return DEFAULT_CASES[:2] if quick else DEFAULT_CASES


def column_name(index: int) -> str:
    prefix = ("float", "int", "string", "timestamp")[index % 4]
    return f"{prefix}_{index:02d}"


def fixture_value(case: ShapeCase, row_index: int, column_index: int) -> Any:
    if case.null_every and (row_index + column_index) % case.null_every == 0:
        return None
    kind = column_index % 4
    if kind == 0:
        return row_index * 0.25 + column_index / 10
    if kind == 1:
        return row_index * 10_000 + column_index
    if kind == 2:
        identity = row_index % case.string_cardinality
        prefix = f"S{identity}:{column_index}:"
        return (prefix + "x" * case.string_bytes)[: case.string_bytes]
    base = datetime(2024, 1, 2, 9, 30)
    return base + timedelta(microseconds=row_index * 1_000 + column_index)


def value_checksum(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value % 997
    if isinstance(value, float):
        return int(value * 1000) % 997
    if isinstance(value, str):
        return len(value)
    if isinstance(value, datetime):
        return value.microsecond % 997
    return len(str(value))


def build_fixture(case: ShapeCase) -> FixtureTable:
    started = time.perf_counter()
    names = [column_name(index) for index in range(case.columns)]
    native_batches: list[Any] = []
    checksum = 0
    for batch_start in range(0, case.rows, case.batch_rows):
        rows: list[dict[str, Any]] = []
        for row_index in range(batch_start, min(case.rows, batch_start + case.batch_rows)):
            row: dict[str, Any] = {}
            for column_index, name in enumerate(names):
                value = fixture_value(case, row_index, column_index)
                row[name] = value
                checksum += value_checksum(value)
            rows.append(row)
        native_batches.append(xbbg.ArrowTable.from_pylist(rows))
    return FixtureTable(
        case=case,
        native_batches=tuple(native_batches),
        setup_ms=(time.perf_counter() - started) * 1000,
        input_checksum=checksum,
    )


def boundary_converter(converter: Callable[[Any], Any]) -> Callable[[FixtureTable], ConversionObservation]:
    def convert(fixture: FixtureTable) -> ConversionObservation:
        outputs = tuple(converter(batch) for batch in fixture.native_batches)
        return ConversionObservation(
            shape=(fixture.case.rows, fixture.case.columns),
            checksum=sum(_shape_checksum(output) for output in outputs),
            retained=outputs,
        )

    return convert


def pyarrow_checked_cells(fixture: FixtureTable) -> ConversionObservation:
    pa = importlib.import_module("pyarrow")
    outputs = tuple(pa.table(batch) for batch in fixture.native_batches)
    checksum = 0
    for table in outputs:
        for row in table.to_pylist():
            for value in row.values():
                checksum += value_checksum(value)
    return ConversionObservation(
        shape=(fixture.case.rows, fixture.case.columns),
        checksum=checksum,
        retained=outputs,
    )


def native_checked_cells(fixture: FixtureTable) -> ConversionObservation:
    outputs = tuple(batch.to_pylist() for batch in fixture.native_batches)
    checksum = sum(value_checksum(value) for rows in outputs for row in rows for value in row.values())
    return ConversionObservation(
        shape=(fixture.case.rows, fixture.case.columns),
        checksum=checksum,
        retained=outputs,
    )


def _shape_checksum(result: Any) -> int:
    shape = getattr(result, "shape", None)
    if isinstance(shape, tuple) and len(shape) >= 2:
        return int(shape[0]) * 31 + int(shape[1])
    rows = getattr(result, "num_rows", 0)
    columns = getattr(result, "num_columns", 0)
    return int(rows) * 31 + int(columns)


def available_converters() -> Iterable[tuple[str, str, Callable[[FixtureTable], ConversionObservation]]]:
    pa = importlib.import_module("pyarrow")
    yield "native_to_pyarrow_c_stream", "boundary_only_no_cell_reads", boundary_converter(pa.table)
    yield "native_to_pyarrow_checked_cells", "inclusive_all_cells_to_python_objects", pyarrow_checked_cells
    yield (
        "native_to_pandas_xbbg_backend",
        "boundary_and_backend_conversion_without_additional_consumer_cell_reads",
        boundary_converter(lambda table: convert_backend_frame(table, Backend.PANDAS)),
    )
    if importlib.util.find_spec("polars") is not None:
        yield (
            "native_to_polars",
            "boundary_and_backend_conversion_without_additional_consumer_cell_reads",
            boundary_converter(lambda table: convert_backend_frame(table, Backend.POLARS)),
        )
    yield "native_to_pylist_checked_cells", "inclusive_native_all_cell_materialization", native_checked_cells


def _pyarrow_pool_snapshot() -> tuple[int | None, int | None]:
    try:
        pool = importlib.import_module("pyarrow").default_memory_pool()
        current = int(pool.bytes_allocated())
        maximum = int(pool.max_memory())
        return current, maximum
    except (AttributeError, ImportError):
        return None, None


def benchmark_conversion(
    *,
    fixture: FixtureTable,
    scenario: str,
    consumer_scope: str,
    converter: Callable[[FixtureTable], ConversionObservation],
    iterations: int,
    warmup: int,
    package_label: str,
) -> BenchmarkResult:
    for _ in range(warmup):
        converter(fixture)

    durations: list[float] = []
    observed_checksum: int | None = None
    observation: ConversionObservation | None = None
    for _ in range(iterations):
        started = time.perf_counter()
        observation = converter(fixture)
        durations.append((time.perf_counter() - started) * 1000)
        if observed_checksum is None:
            observed_checksum = observation.checksum
        elif observation.checksum != observed_checksum:
            raise RuntimeError(f"{scenario} produced an unstable checksum")

    pool_before, _ = _pyarrow_pool_snapshot()
    tracemalloc.start()
    try:
        memory_observation = converter(fixture)
        _current, python_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    pool_after, pool_max = _pyarrow_pool_snapshot()
    if observation is None or memory_observation.checksum != observed_checksum:
        raise RuntimeError(f"{scenario} memory run changed the observable result")

    from benchmark_contracts import PYTHON_MEMORY_SCOPE, empirical_percentile

    return BenchmarkResult(
        package=package_label,
        operation=f"handoff_offline:{scenario}:{fixture.case.name}",
        scenario=scenario,
        consumer_scope=consumer_scope,
        rows=fixture.case.rows,
        columns=fixture.case.columns,
        native_batches=len(fixture.native_batches),
        batch_rows=fixture.case.batch_rows,
        null_every=fixture.case.null_every,
        string_bytes=fixture.case.string_bytes,
        string_cardinality=fixture.case.string_cardinality,
        timing_scope="uninstrumented warm conversion; fixture construction excluded",
        mean_ms=round(statistics.mean(durations), 6),
        median_ms=round(statistics.median(durations), 6),
        min_ms=round(min(durations), 6),
        max_ms=round(max(durations), 6),
        p95_ms=_round_optional(empirical_percentile(durations, 95)),
        p99_ms=_round_optional(empirical_percentile(durations, 99)),
        timing_sample_count=len(durations),
        warmup_iterations=warmup,
        setup_ms=round(fixture.setup_ms, 6),
        python_tracemalloc_peak_mb=round(python_peak / 1024 / 1024, 6),
        memory_sample_count=1,
        memory_scope=PYTHON_MEMORY_SCOPE,
        pyarrow_pool_before_bytes=pool_before,
        pyarrow_pool_after_bytes=pool_after,
        pyarrow_pool_max_bytes=pool_max,
        pyarrow_pool_scope=(
            "PyArrow default pool bytes before/after the separate untimed run and process-lifetime "
            "max_memory; the max is not attributable to this scenario"
        ),
        data_shape=observation.shape,
        checksum=observation.checksum,
        input_checksum=fixture.input_checksum,
        offline=True,
    )


def _round_optional(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def result_document(
    results: Sequence[BenchmarkResult],
    cases: Sequence[ShapeCase],
) -> dict[str, Any]:
    from benchmark_contracts import collect_provenance, report_date

    inputs = [asdict(case) for case in cases]
    return {
        "schema_version": 3,
        "version": getattr(xbbg, "__version__", "unknown"),
        "date": report_date(),
        "offline": True,
        "coverage": "synthetic native Arrow carriers only; no Bloomberg SDK event or network coverage",
        "benchmark_file": Path(__file__).name,
        "provenance": collect_provenance(inputs=inputs, benchmark_file=Path(__file__).name),
        "benchmarks": {"Binding Handoff - Offline": [asdict(result) for result in results]},
    }


def write_results(document: dict[str, Any], results_dir: Path) -> Path:
    from benchmark_contracts import archive_path

    results_dir.mkdir(parents=True, exist_ok=True)
    output_path = archive_path(results_dir, "handoff_offline", ".json")
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(document, file, indent=2, default=str)
    with (results_dir / "handoff_offline_latest.json").open("w", encoding="utf-8") as file:
        json.dump(document, file, indent=2, default=str)
    return output_path


def run(args: argparse.Namespace) -> tuple[list[BenchmarkResult], dict[str, Any], Path]:
    iterations = (
        args.iterations if args.iterations is not None else (QUICK_ITERATIONS if args.quick else DEFAULT_ITERATIONS)
    )
    warmup = args.warmup if args.warmup is not None else (QUICK_WARMUP if args.quick else DEFAULT_WARMUP)
    cases = selected_cases(args.shape, args.quick)
    from benchmark_contracts import collect_provenance

    provenance = collect_provenance(inputs=[asdict(case) for case in cases], benchmark_file=Path(__file__).name)
    profile = provenance["build"].get("profile", "unknown")
    package_label = f"xbbg-python-extension ({profile})"

    logger.info("Offline Native Handoff Benchmark")
    logger.info("Iterations: %s; warmup: %s; build profile: %s", iterations, warmup, profile)
    converters = tuple(available_converters())
    results: list[BenchmarkResult] = []
    for case in cases:
        logger.info(
            "Building %s: %sx%s, batch_rows=%s, null_every=%s, string_bytes=%s",
            case.name,
            case.rows,
            case.columns,
            case.batch_rows,
            case.null_every,
            case.string_bytes,
        )
        fixture = build_fixture(case)
        for scenario, consumer_scope, converter in converters:
            result = benchmark_conversion(
                fixture=fixture,
                scenario=scenario,
                consumer_scope=consumer_scope,
                converter=converter,
                iterations=iterations,
                warmup=warmup,
                package_label=package_label,
            )
            results.append(result)
            logger.info(
                "  %s: median %.4fms, max %.4fms (%s timing samples); Python peak %.2fMB",
                scenario,
                result.median_ms,
                result.max_ms,
                result.timing_sample_count,
                result.python_tracemalloc_peak_mb,
            )

    document = result_document(results, cases)
    output_path = write_results(document, args.results_dir)
    logger.info("Wrote JSON results: %s", output_path)
    return results, document, output_path


def main(argv: Sequence[str] | None = None) -> list[BenchmarkResult]:
    args = parse_args(argv)
    results, document, _output_path = run(args)
    if args.json:
        print(json.dumps(document, indent=2, default=str))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
