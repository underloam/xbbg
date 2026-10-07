"""Benchmark BDH (Historical Data) across packages.

Data usage: ~15-30 data points per run (3-4 trading days)
"""

from __future__ import annotations

from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

from config import (
    BDH_END,
    BDH_START,
    FIELDS_MULTI,
    FIELDS_SINGLE,
    ITERATIONS,
    TICKERS_MULTI,
    TICKERS_SINGLE,
    WARMUP_ITERATIONS,
)
from benchmark_contracts import LiveMeasurement, measure_live_call, reused_pdblp_connection


@dataclass
class BenchmarkResult(LiveMeasurement):
    package: str
    operation: str
    iterations: int


def benchmark_bdh(package_name: str, bdh_func, tickers, fields, start_date, end_date) -> BenchmarkResult | None:
    measurement = measure_live_call(
        bdh_func,
        (tickers, fields, start_date, end_date),
        iterations=ITERATIONS,
        warmup_iterations=WARMUP_ITERATIONS,
    )
    if measurement is None:
        return None
    ticker_count = len(tickers) if isinstance(tickers, list) else 1
    field_count = len(fields) if isinstance(fields, list) else 1
    return BenchmarkResult(
        **vars(measurement),
        package=package_name,
        operation=f"bdh({ticker_count}t, {field_count}f, {start_date} to {end_date})",
        iterations=ITERATIONS,
    )


def run_xbbg_rust(tickers, fields, start_date, end_date):
    """Benchmark xbbg Rust version."""
    import xbbg

    return xbbg.bdh(tickers, fields, start_date, end_date)


def run_pdblp(tickers, fields, start_date, end_date):
    try:
        con = reused_pdblp_connection()
        ticker_list = tickers if isinstance(tickers, list) else [tickers]
        field_list = fields if isinstance(fields, list) else [fields]
        return con.bdh(ticker_list, field_list, start_date, end_date)
    except ImportError:
        logger.warning("pdblp not installed")
        return None


def main():
    """Run all BDH benchmarks."""
    logger.info("=" * 70)
    logger.info("BDH (Historical Data) Benchmark")
    logger.info("=" * 70)
    logger.info(f"\nIterations: {ITERATIONS}")
    logger.info(f"Warmup: {WARMUP_ITERATIONS}")
    logger.info(f"Date range: {BDH_START} to {BDH_END}")

    results = []

    # Test 1: Single ticker, single field
    logger.info("\n\nTest 1: Single ticker, single field")
    logger.info("-" * 70)

    if True:  # xbbg Rust
        logger.info("Running xbbg (Rust)...")
        try:
            result = benchmark_bdh("xbbg-rust", run_xbbg_rust, TICKERS_SINGLE[0], FIELDS_SINGLE[0], BDH_START, BDH_END)
            if result:
                results.append(result)
                logger.info(
                    f"  ✓ {result.warm_mean_ms:.2f}ms (mean), {result.python_tracemalloc_peak_mb:.2f}MB, shape={result.data_shape}"
                )
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")

    if True:  # pdblp
        logger.info("Running pdblp...")
        try:
            result = benchmark_bdh("pdblp", run_pdblp, TICKERS_SINGLE[0], FIELDS_SINGLE[0], BDH_START, BDH_END)
            if result:
                results.append(result)
                logger.info(
                    f"  ✓ {result.warm_mean_ms:.2f}ms (mean), {result.python_tracemalloc_peak_mb:.2f}MB, shape={result.data_shape}"
                )
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")

    # Test 2: Multiple tickers, multiple fields
    logger.info("\n\nTest 2: Multiple tickers, multiple fields")
    logger.info("-" * 70)

    if True:  # xbbg Rust
        logger.info("Running xbbg (Rust)...")
        try:
            result = benchmark_bdh("xbbg-rust", run_xbbg_rust, TICKERS_MULTI, FIELDS_MULTI, BDH_START, BDH_END)
            if result:
                results.append(result)
                logger.info(
                    f"  ✓ {result.warm_mean_ms:.2f}ms (mean), {result.python_tracemalloc_peak_mb:.2f}MB, shape={result.data_shape}"
                )
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")

    if True:  # pdblp
        logger.info("Running pdblp...")
        try:
            result = benchmark_bdh("pdblp", run_pdblp, TICKERS_MULTI, FIELDS_MULTI, BDH_START, BDH_END)
            if result:
                results.append(result)
                logger.info(
                    f"  ✓ {result.warm_mean_ms:.2f}ms (mean), {result.python_tracemalloc_peak_mb:.2f}MB, shape={result.data_shape}"
                )
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")

    # Print summary
    logger.info("\n\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    for result in results:
        logger.info(f"\n{result.package} - {result.operation}")
        logger.info(
            f"  Fresh-process first result: {result.fresh_process_first_result_ms:.2f}ms "
            f"({result.fresh_process_sample_count} sample)"
        )
        logger.info(f"  Warm mean:  {result.warm_mean_ms:.2f}ms ± {result.warm_std_ms:.2f}ms")
        logger.info(f"  Warm max:   {result.warm_max_ms:.2f}ms ({result.warm_sample_count} samples)")
        logger.info(f"  CPython tracemalloc peak (untimed call): {result.python_tracemalloc_peak_mb:.2f}MB")
        logger.info(f"  Shape:      {result.data_shape}")

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
