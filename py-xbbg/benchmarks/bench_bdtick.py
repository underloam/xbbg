"""Benchmark BDTICK (Tick Data) across packages.

Data usage: ~100-200 data points per run (5 minutes of tick data)
"""

from __future__ import annotations

from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

from config import (
    BDTICK_DATE,
    BENCH_TZ,
    BDTICK_END_TIME,
    BDTICK_START_TIME,
    ITERATIONS,
    TICKERS_SINGLE,
    WARMUP_ITERATIONS,
    intraday_window_utc,
)
from benchmark_contracts import LiveMeasurement, measure_live_call, reused_pdblp_connection


@dataclass
class BenchmarkResult(LiveMeasurement):
    package: str
    operation: str
    iterations: int


def benchmark_bdtick(
    package_name: str, bdtick_func, ticker, event_types, date, start_time, end_time
) -> BenchmarkResult | None:
    measurement = measure_live_call(
        bdtick_func,
        (ticker, event_types, date, start_time, end_time),
        iterations=ITERATIONS,
        warmup_iterations=WARMUP_ITERATIONS,
    )
    if measurement is None:
        return None
    event_label = ",".join(event_types) if isinstance(event_types, list) else event_types
    return BenchmarkResult(
        **vars(measurement),
        package=package_name,
        operation=f"bdtick({ticker}, [{event_label}])",
        iterations=ITERATIONS,
    )


def run_xbbg_rust(ticker, event_types, date, start_time, end_time):
    """Benchmark xbbg Rust version."""
    import xbbg

    return xbbg.bdtick(
        ticker,
        f"{date} {start_time}",
        f"{date} {end_time}",
        event_types=event_types,
        request_tz=BENCH_TZ,
    )


def run_pdblp(ticker, event_types, date, start_time, end_time):
    try:
        con = reused_pdblp_connection()
        start_datetime, end_datetime = intraday_window_utc(date, start_time, end_time)
        if not hasattr(con, "bdtick"):
            logger.warning("pdblp does not support bdtick")
            return None
        return con.bdtick(ticker, start_datetime, end_datetime, event_types)
    except ImportError:
        logger.warning("pdblp not installed")
        return None
    except Exception as exc:
        logger.warning("pdblp error: %s", exc)
        return None


def main():
    """Run all BDTICK benchmarks."""
    logger.info("=" * 70)
    logger.info("BDTICK (Tick Data) Benchmark")
    logger.info("=" * 70)
    logger.info(f"\nIterations: {ITERATIONS}")
    logger.info(f"Warmup: {WARMUP_ITERATIONS}")
    logger.info(f"Date: {BDTICK_DATE}")
    logger.info(f"Time range: {BDTICK_START_TIME} to {BDTICK_END_TIME}")

    results = []

    # Test different event type combinations
    test_cases = [
        (["TRADE"], "TRADE only"),
        (["BID", "ASK"], "BID + ASK"),
        (["TRADE", "BID", "ASK"], "All events"),
    ]

    for event_types, description in test_cases:
        logger.info(f"\n\nTest: {description}")
        logger.info("-" * 70)

        if True:  # xbbg Rust
            logger.info("Running xbbg (Rust)...")
            try:
                result = benchmark_bdtick(
                    "xbbg-rust",
                    run_xbbg_rust,
                    TICKERS_SINGLE[0],
                    event_types,
                    BDTICK_DATE,
                    BDTICK_START_TIME,
                    BDTICK_END_TIME,
                )
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
                result = benchmark_bdtick(
                    "pdblp",
                    run_pdblp,
                    TICKERS_SINGLE[0],
                    event_types,
                    BDTICK_DATE,
                    BDTICK_START_TIME,
                    BDTICK_END_TIME,
                )
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
