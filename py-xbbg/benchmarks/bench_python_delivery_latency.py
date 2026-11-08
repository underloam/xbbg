"""Python-consumer delivery latency through the offline subscription test driver.

A producer thread publishes one synthetic update at a time with
``_SubscriptionFixture.publish_json`` while an asyncio consumer already awaits
``next_delivery()`` on the native subscription. The script reports three
distributions:

* publish -> delivery: from just before ``publish_json`` until the consumer has
  published the awaited delivery receipt to Python;
* publish_json call: how long that call blocks the producer thread;
* event-loop lag: how late ``loop.call_later`` callbacks run on the consumer loop
  while deliveries flow. It measures asyncio responsiveness, not GIL release, and
  is reported separately for that reason.

It needs a release build of ``xbbg._core`` compiled with the ``test-support``
feature for the interpreter that runs it, installed in place of the production
extension the same way CI's test-support step does::

    export PYO3_PYTHON="$(python -c 'import sys; print(sys.executable)')"
    export XBBG_DIST_VERSION="$(python -c 'from importlib.metadata import version; print(version("xbbg"))')"
    cargo build --release -p pyo3-xbbg --features test-support

then copy ``target/release/lib_core.so`` (``lib_core.dylib`` on macOS,
``_core.dll`` on Windows) over the installed ``xbbg/_core*`` extension. No
Bloomberg connection is used. Results are printed only; nothing is written.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import statistics
import sys
import threading
import time
from typing import Any

from benchmark_contracts import collect_provenance, empirical_percentile

TOPIC = "SYNTHETIC LATENCY Equity"
FIELD = "BID"
DEFAULT_SAMPLES = 2_000
DEFAULT_WARMUP = 200
DEFAULT_LAG_INTERVAL_MS = 1.0


@dataclass(frozen=True)
class Distribution:
    name: str
    samples: int
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: float | None

    @classmethod
    def of(cls, name: str, values_ms: Sequence[float]) -> Distribution:
        return cls(
            name=name,
            samples=len(values_ms),
            p50_ms=statistics.median(values_ms) if values_ms else None,
            p95_ms=empirical_percentile(values_ms, 95),
            p99_ms=empirical_percentile(values_ms, 99),
            max_ms=max(values_ms) if values_ms else None,
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline xbbg Python-consumer delivery latency benchmark")
    parser.add_argument("--samples", type=positive_int, default=DEFAULT_SAMPLES, help="Measured deliveries")
    parser.add_argument("--warmup", type=non_negative_int, default=DEFAULT_WARMUP, help="Discarded deliveries")
    parser.add_argument(
        "--lag-interval-ms",
        type=positive_float,
        default=DEFAULT_LAG_INTERVAL_MS,
        help="Delay of each event-loop lag probe callback",
    )
    parser.add_argument("--json", action="store_true", help="Print one JSON document instead of the text report")
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


def positive_float(value: str) -> float:
    parsed = float(value)
    if not parsed > 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def release_fixture_factory() -> Any:
    """Return the test driver factory, refusing builds whose timings are not representative."""
    from xbbg import _core

    build_info = getattr(_core, "__build_info__", None)
    profile = build_info.get("profile") if isinstance(build_info, dict) else None
    if profile != "release":
        raise SystemExit(
            f"xbbg._core is a {profile or 'unknown'} build; this benchmark only measures release builds "
            "(cargo build --release -p pyo3-xbbg --features test-support)."
        )
    factory = getattr(_core, "_test_subscription_fixture", None)
    if factory is None:
        raise SystemExit("xbbg._core was built without the test-support feature, which provides the offline driver.")
    return factory


class LoopLagProbe:
    """Chain ``call_later`` callbacks and record how late each one runs."""

    def __init__(self, loop: asyncio.AbstractEventLoop, interval_s: float) -> None:
        self._loop = loop
        self._interval_s = interval_s
        self._due = 0.0
        self._handle: asyncio.TimerHandle | None = None
        self.lateness_ms: list[float] = []

    def start(self) -> None:
        self._due = self._loop.time() + self._interval_s
        self._handle = self._loop.call_at(self._due, self._fire)

    def _fire(self) -> None:
        self.lateness_ms.append((self._loop.time() - self._due) * 1000)
        self.start()

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None


async def measure(factory: Any, *, samples: int, warmup: int, lag_interval_s: float) -> list[Distribution]:
    total = warmup + samples
    payloads = [json.dumps({FIELD: 100 + index / 100}) for index in range(total)]
    published_at: list[int] = []
    publish_call_ms: list[float] = []
    ready = threading.Event()
    stop = threading.Event()
    producer_failure: list[BaseException] = []
    loop = asyncio.get_running_loop()
    consumer = asyncio.current_task()
    owner = await factory(None)

    def produce() -> None:
        try:
            for payload in payloads:
                ready.wait()
                ready.clear()
                if stop.is_set():
                    return
                started = time.perf_counter_ns()
                published_at.append(started)
                owner.publish_json(TOPIC, payload)
                publish_call_ms.append((time.perf_counter_ns() - started) / 1e6)
        except BaseException as exc:
            producer_failure.append(exc)
            if consumer is not None:
                loop.call_soon_threadsafe(consumer.cancel)

    delivery_ms: list[float] = []
    lag = LoopLagProbe(loop, lag_interval_s)
    producer = threading.Thread(target=produce, name="synthetic-producer", daemon=True)
    subscription = None
    try:
        subscription = await owner.subscribe([TOPIC], [FIELD], flush_threshold=1)
        producer.start()
        async with asyncio.timeout(60 + total * 0.05):
            for index in range(total):
                if index == warmup:
                    lag.start()
                pending = subscription.next_delivery()
                ready.set()
                receipt = await pending
                _kind, batch, _dropped = receipt.value
                receipt.publish()
                received = time.perf_counter_ns()
                if index >= warmup:
                    delivery_ms.append((received - published_at[index]) / 1e6)
                if batch.num_rows != 1:
                    raise RuntimeError(f"expected one synthetic update per delivery, got {batch.num_rows}")
        lag.stop()
        last = batch.to_pylist()[0][FIELD]
        if last != json.loads(payloads[-1])[FIELD]:
            raise RuntimeError("the last delivery does not carry the last published value")
    except asyncio.CancelledError:
        if producer_failure:
            raise RuntimeError("the synthetic producer failed") from producer_failure[0]
        raise
    finally:
        lag.stop()
        stop.set()
        ready.set()
        if producer.is_alive():
            producer.join(timeout=10)
        if subscription is not None:
            await subscription.unsubscribe()
        owner.terminate()
        await owner.settle()

    return [
        Distribution.of("publish -> delivery", delivery_ms),
        Distribution.of("publish_json call", publish_call_ms[warmup:]),
        Distribution.of(f"event-loop lag ({lag_interval_s * 1000:g} ms timer)", lag.lateness_ms),
    ]


def format_ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f} ms"


def print_report(results: Sequence[Distribution], provenance: dict[str, Any]) -> None:
    runtime = provenance["runtime"]
    build_info = provenance["build"]
    print("xbbg Python-consumer delivery latency (offline test-support driver)")
    print(
        f"interpreter: {runtime['implementation']} {runtime['python']}, "
        f"free-threaded build: {runtime['free_threaded']}, "
        f"GIL enabled: {runtime['gil_enabled']}"
    )
    print(
        f"extension: {build_info.get('profile')}, opt-level {build_info.get('optLevel')}, "
        f"allocator {build_info.get('allocator')}"
    )
    width = max(len(result.name) for result in results)
    for result in results:
        print(
            f"{result.name:<{width}}  n={result.samples:<6} p50 {format_ms(result.p50_ms)}  "
            f"p95 {format_ms(result.p95_ms)}  p99 {format_ms(result.p99_ms)}  max {format_ms(result.max_ms)}"
        )
    print("p95/p99 need at least 20/100 samples. Event-loop lag measures asyncio responsiveness, not GIL release.")


def main(argv: Sequence[str] | None = None) -> list[Distribution]:
    args = parse_args(argv)
    factory = release_fixture_factory()
    results = asyncio.run(
        measure(factory, samples=args.samples, warmup=args.warmup, lag_interval_s=args.lag_interval_ms / 1000)
    )
    inputs = {
        "samples": args.samples,
        "warmup": args.warmup,
        "lag_interval_ms": args.lag_interval_ms,
        "topic": TOPIC,
        "field": FIELD,
    }
    provenance = collect_provenance(inputs=inputs, benchmark_file=Path(__file__).name)
    if args.json:
        document = {
            "benchmark_file": Path(__file__).name,
            "offline": True,
            "results": [asdict(result) for result in results],
            "provenance": provenance,
        }
        print(json.dumps(document, indent=2, default=str))
    else:
        print_report(results, provenance)
    return results


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
