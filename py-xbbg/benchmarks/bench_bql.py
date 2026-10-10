"""Benchmark BQL (Bloomberg Query Language) with the current xbbg build.

Data usage: ~10-20 data points per run
"""

from __future__ import annotations

from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

from config import BQL_MULTI, BQL_SIMPLE, ITERATIONS, WARMUP_ITERATIONS
from benchmark_contracts import LiveMeasurement, measure_live_call


@dataclass
class BenchmarkResult(LiveMeasurement):
    package: str
    operation: str
    iterations: int


def benchmark_bql(package_name: str, bql_func, query: str) -> BenchmarkResult | None:
    measurement = measure_live_call(
        bql_func,
        (query,),
        iterations=ITERATIONS,
        warmup_iterations=WARMUP_ITERATIONS,
    )
    if measurement is None:
        return None
    query_display = query if len(query) <= 40 else query[:37] + "..."
    return BenchmarkResult(
        **vars(measurement),
        package=package_name,
        operation=f"bql({query_display})",
        iterations=ITERATIONS,
    )


def run_xbbg_rust(query: str):
    """Benchmark xbbg Rust version."""
    import xbbg

    return xbbg.bql(query)


def main():
    """Run all BQL benchmarks."""
    logger.info("=" * 70)
    logger.info("BQL (Bloomberg Query Language) Benchmark")
    logger.info("=" * 70)
    logger.info(f"\nIterations: {ITERATIONS}")
    logger.info(f"Warmup: {WARMUP_ITERATIONS}")

    results = []

    # Test 1: Simple query
    logger.info("\n\nTest 1: Simple BQL query")
    logger.info("-" * 70)
    logger.info(f"Query: {BQL_SIMPLE}")

    if True:  # xbbg Rust
        logger.info("\nRunning xbbg (Rust)...")
        try:
            result = benchmark_bql("xbbg-rust", run_xbbg_rust, BQL_SIMPLE)
            if result:
                results.append(result)
                logger.info(
                    f"  ✓ {result.warm_mean_ms:.2f}ms (mean), {result.python_tracemalloc_peak_mb:.2f}MB, shape={result.data_shape}"
                )
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")

    # Test 2: Multi-security query
    logger.info("\n\nTest 2: Multi-security BQL query")
    logger.info("-" * 70)
    logger.info(f"Query: {BQL_MULTI}")

    if True:  # xbbg Rust
        logger.info("\nRunning xbbg (Rust)...")
        try:
            result = benchmark_bql("xbbg-rust", run_xbbg_rust, BQL_MULTI)
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
