"""Run all benchmarks and generate comprehensive report.

This runs the complete benchmark suite comparing xbbg Rust against all
competing packages.

Data usage: ~200-350 Bloomberg data points total per run.

Results are saved per version:
- benchmark_v{version}.json (overwrites for same version)
- benchmark_v{version}_{timestamp}.json (timestamped archive)
- latest.json (symlink/copy to latest version)
"""

from __future__ import annotations
import argparse
import inspect
from dataclasses import asdict, is_dataclass
import json
import logging
from pathlib import Path
import sys

from benchmark_contracts import archive_path, close_shared_sessions, collect_provenance, report_date
import config as benchmark_config

logger = logging.getLogger(__name__)

# Create results directory
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def get_xbbg_version():
    """Get the current xbbg version.

    Returns:
        Version string (e.g., "1.0.0") or "unknown"
    """
    try:
        import xbbg

        if hasattr(xbbg, "__version__"):
            version_str = str(xbbg.__version__)
            return version_str
        else:
            # Try to get version from package metadata
            try:
                from importlib.metadata import version

                version_str = str(version("xbbg"))
                return version_str
            except Exception:
                return "unknown"
    except ImportError:
        return "unknown"


def run_benchmark_module(module_name: str):
    """Run a benchmark module and return results."""
    logger.info(f"\n{'=' * 70}")
    logger.info(f"Running {module_name}")
    logger.info(f"{'=' * 70}\n")

    try:
        module = __import__(module_name)
        if hasattr(module, "main"):
            main_func = module.main
            if inspect.signature(main_func).parameters:
                return main_func([])
            return main_func()
        else:
            logger.warning(f"{module_name} has no main() function")
            return []
    except Exception as e:
        logger.error(f"Error running {module_name}: {e}")
        import traceback

        traceback.print_exc()
        return []


def generate_markdown_report(all_results: dict, output_path: Path, version: str, date: str):
    """Generate a compact report whose labels match each measurement boundary."""
    with output_path.open("w", encoding="utf-8") as file:
        file.write("# xbbg Benchmark Results\n\n")
        file.write(f"**Version:** {version}\n")
        file.write(f"**Date (UTC):** {date}\n\n")
        for operation, results in all_results.items():
            file.write(f"## {operation}\n\n")
            if not results:
                file.write("*No results*\n\n")
                continue
            first = results[0]
            if hasattr(first, "warm_mean_ms"):
                file.write(
                    "| Package | Fresh-process first result (ms, n=1) | "
                    "Warm-session mean (ms) | Warm max (ms) | Warm n | "
                    "CPython tracemalloc peak (MB, separate run) | Shape |\n"
                )
                file.write("|---|---:|---:|---:|---:|---:|---|\n")
                for result in results:
                    file.write(
                        f"| {result.package} | {result.fresh_process_first_result_ms:.2f} | "
                        f"{result.warm_mean_ms:.2f} | {result.warm_max_ms:.2f} | "
                        f"{result.warm_sample_count} | {result.python_tracemalloc_peak_mb:.2f} | "
                        f"{result.data_shape} |\n"
                    )
            else:
                file.write(
                    "| Scenario | Case | Consumer boundary | Median (ms) | Max (ms) | n | "
                    "CPython tracemalloc peak (MB, separate run) |\n"
                )
                file.write("|---|---|---|---:|---:|---:|---:|\n")
                for result in results:
                    file.write(
                        f"| {result.scenario} | {result.operation.rsplit(':', 1)[-1]} | "
                        f"{result.consumer_scope} | {result.median_ms:.4f} | "
                        f"{result.max_ms:.4f} | {result.timing_sample_count} | "
                        f"{result.python_tracemalloc_peak_mb:.2f} |\n"
                    )
            file.write("\n")


def generate_json_report(
    all_results: dict,
    output_path: Path,
    version: str,
    date: str,
    provenance: dict,
):
    """Generate JSON without reintroducing legacy ambiguous metric aliases."""
    json_data = {
        "schema_version": 2,
        "version": version,
        "date": date,
        "provenance": provenance,
        "benchmarks": {},
    }
    for operation, results in all_results.items():
        entries = []
        for result in results:
            if not is_dataclass(result):
                raise TypeError(f"{operation} returned a non-dataclass benchmark record")
            entries.append(asdict(result))
        json_data["benchmarks"][operation] = entries
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=2, default=str)


def parse_args(argv: list[str] | None = None):
    """Parse benchmark runner arguments."""
    parser = argparse.ArgumentParser(description="Run xbbg benchmark suite")
    parser.add_argument(
        "--offline",
        dest="include_offline",
        action="store_true",
        default=True,
        help="Include offline binding handoff benchmarks (default)",
    )
    parser.add_argument(
        "--no-offline",
        dest="include_offline",
        action="store_false",
        help="Skip offline binding handoff benchmarks",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    """Run all benchmarks and generate reports."""
    logger.info("=" * 70)
    logger.info("xbbg Comprehensive Benchmark Suite")
    logger.info("=" * 70)
    args = parse_args(argv)

    # Get version
    version = get_xbbg_version()
    logger.info(f"\nxbbg version: {version}")
    logger.info("\nRunning benchmarks with live Bloomberg data...")
    logger.info("Estimated live data usage: ~200-350 data points")
    logger.info("Offline binding handoff benchmark included: %s\n", args.include_offline)

    all_results = {}

    # Run each benchmark module
    benchmarks = [
        ("bench_bdp", "BDP - Reference Data"),
        ("bench_bdh", "BDH - Historical Data"),
        ("bench_bdib", "BDIB - Intraday Bars"),
        ("bench_bdtick", "BDTICK - Tick Data"),
        ("bench_bql", "BQL - Query Language"),
    ]
    if args.include_offline:
        benchmarks.append(("bench_handoff_offline", "Binding Handoff - Offline"))
    provenance = collect_provenance(
        inputs={
            "benchmark_modules": [module_name for module_name, _ in benchmarks],
            "include_offline": args.include_offline,
            "tickers_single": benchmark_config.TICKERS_SINGLE,
            "tickers_multi": benchmark_config.TICKERS_MULTI,
            "fields_single": benchmark_config.FIELDS_SINGLE,
            "fields_multi": benchmark_config.FIELDS_MULTI,
            "bdh_range": [benchmark_config.BDH_START, benchmark_config.BDH_END],
            "bdib": [
                benchmark_config.BDIB_DATE,
                benchmark_config.BDIB_START_TIME,
                benchmark_config.BDIB_END_TIME,
                benchmark_config.BDIB_INTERVAL,
            ],
            "bdtick": [
                benchmark_config.BDTICK_DATE,
                benchmark_config.BDTICK_START_TIME,
                benchmark_config.BDTICK_END_TIME,
            ],
            "bql": [benchmark_config.BQL_SIMPLE, benchmark_config.BQL_MULTI],
            "iterations": benchmark_config.ITERATIONS,
            "warmup_iterations": benchmark_config.WARMUP_ITERATIONS,
        },
        benchmark_file=Path(__file__).name,
    )

    for module_name, description in benchmarks:
        try:
            results = run_benchmark_module(module_name)
            all_results[description] = results
        except Exception as e:
            logger.error(f"Failed to run {module_name}: {e}")
            all_results[description] = []
    close_shared_sessions()

    # Generate reports with version-based naming
    date = report_date()

    # Version-specific files (OVERWRITES for same version)
    version_json = RESULTS_DIR / f"benchmark_v{version}.json"
    version_md = RESULTS_DIR / f"benchmark_v{version}.md"

    # Archive copy numbered within the UTC day (NEVER overwritten)
    archive_json = archive_path(RESULTS_DIR, f"benchmark_v{version}", ".json")
    archive_md = archive_json.with_suffix(".md")

    logger.info(f"\n\n{'=' * 70}")
    logger.info("Generating Reports")
    logger.info(f"{'=' * 70}\n")

    # Generate version-specific files (overwrites)
    generate_json_report(all_results, version_json, version, date, provenance)
    logger.info(f"✓ Version JSON: {version_json}")

    generate_markdown_report(all_results, version_md, version, date)
    logger.info(f"✓ Version MD:   {version_md}")

    # Generate timestamped archives
    import shutil

    shutil.copy(version_json, archive_json)
    shutil.copy(version_md, archive_md)
    logger.info(f"✓ Archive JSON: {archive_json}")
    logger.info(f"✓ Archive MD:   {archive_md}")

    # Update latest symlinks/copies
    latest_json = RESULTS_DIR / "latest.json"
    latest_md = RESULTS_DIR / "latest.md"

    if latest_json.exists():
        latest_json.unlink()
    if latest_md.exists():
        latest_md.unlink()

    try:
        latest_json.symlink_to(version_json.name)
        latest_md.symlink_to(version_md.name)
        logger.info("✓ Latest symlinks updated")
    except OSError:
        # Windows may not support symlinks, just copy
        shutil.copy(version_json, latest_json)
        shutil.copy(version_md, latest_md)
        logger.info("✓ Latest files copied (symlinks not supported)")

    logger.info(f"\n{'=' * 70}")
    logger.info("Benchmarks Complete!")
    logger.info(f"{'=' * 70}\n")
    logger.info("Results saved:")
    logger.info(f"  - Version-specific (overwrites): {version_md}")
    logger.info(f"  - Timestamped archive (keeps):   {archive_md}")
    logger.info("  - Latest:                        latest.md")
    logger.info("\nCommit these files to git for version tracking.")

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
