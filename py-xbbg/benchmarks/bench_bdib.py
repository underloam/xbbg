"""Benchmark BDIB (Intraday Bars) across packages.

Data usage: ~50-100 data points per run (30 minutes of 5-min bars)
"""

from __future__ import annotations

from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

from config import (
    BDIB_DATE,
    BENCH_TZ,
    BDIB_END_TIME,
    BDIB_INTERVAL,
    BDIB_START_TIME,
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


def benchmark_bdib(
    package_name: str, bdib_func, ticker, event_type, date, start_time, end_time, interval
) -> BenchmarkResult | None:
    measurement = measure_live_call(
        bdib_func,
        (ticker, event_type, date, start_time, end_time, interval),
        iterations=ITERATIONS,
        warmup_iterations=WARMUP_ITERATIONS,
    )
    if measurement is None:
        return None
    return BenchmarkResult(
        **vars(measurement),
        package=package_name,
        operation=f"bdib({ticker}, {event_type}, {interval}m bars)",
        iterations=ITERATIONS,
    )


def run_xbbg_rust(ticker, event_type, date, start_time, end_time, interval):
    """Benchmark xbbg Rust version."""
    import xbbg

    return xbbg.bdib(
        ticker,
        typ=event_type,
        start_datetime=f"{date} {start_time}",
        end_datetime=f"{date} {end_time}",
        interval=interval,
        request_tz=BENCH_TZ,
    )


def run_pdblp(ticker, event_type, date, start_time, end_time, interval):
    try:
        con = reused_pdblp_connection()
        start_datetime, end_datetime = intraday_window_utc(date, start_time, end_time)
        return con.bdib(ticker, start_datetime, end_datetime, event_type, interval)
    except ImportError:
        logger.warning("pdblp not installed")
        return None
    except Exception as exc:
        logger.warning("pdblp error: %s", exc)
        return None


def main():
    """Run all BDIB benchmarks."""
    logger.info("=" * 70)
    logger.info("BDIB (Intraday Bars) Benchmark")
    logger.info("=" * 70)
    logger.info(f"\nIterations: {ITERATIONS}")
    logger.info(f"Warmup: {WARMUP_ITERATIONS}")
    logger.info(f"Date: {BDIB_DATE}")
    logger.info(f"Time range: {BDIB_START_TIME} to {BDIB_END_TIME}")
    logger.info(f"Interval: {BDIB_INTERVAL} minutes")

    results = []

    event_types = ["TRADE"]  # Could also test BID, ASK, BEST_BID, BEST_ASK

    for event_type in event_types:
        logger.info(f"\n\nTest: {event_type} events")
        logger.info("-" * 70)

        if True:  # xbbg Rust
            logger.info("Running xbbg (Rust)...")
            try:
                result = benchmark_bdib(
                    "xbbg-rust",
                    run_xbbg_rust,
                    TICKERS_SINGLE[0],
                    event_type,
                    BDIB_DATE,
                    BDIB_START_TIME,
                    BDIB_END_TIME,
                    BDIB_INTERVAL,
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
                result = benchmark_bdib(
                    "pdblp",
                    run_pdblp,
                    TICKERS_SINGLE[0],
                    event_type,
                    BDIB_DATE,
                    BDIB_START_TIME,
                    BDIB_END_TIME,
                    BDIB_INTERVAL,
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
