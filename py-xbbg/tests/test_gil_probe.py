"""The GIL probe tells detached native work apart from Python that keeps the GIL."""

from __future__ import annotations

import sys
import time

from _gil_probe import releases_gil
import pytest

from xbbg import _core

pytestmark = pytest.mark.skipif(
    getattr(sys, "_is_gil_enabled", lambda: True)() is False,
    reason="GIL release is only observable on a GIL build",
)

# Long enough for the operating system to schedule the probe's helper thread.
MIN_CALL_SECONDS = 0.1
MAX_PIVOT_COPIES = 64


def _long_format_batch(min_seconds: float):
    """Return a synthetic long-format batch whose native pivot runs for at least ``min_seconds``."""
    base = _core.ArrowTable.from_pylist(
        [
            {"ticker": f"SYNTH{ticker:05d} Equity", "field": f"FIELD_{field}", "value": f"{ticker}.{field}"}
            for ticker in range(10_000)
            for field in range(5)
        ]
    )
    copies = 1
    while True:
        batch = _core.ArrowTable.concat_tables([base] * copies).to_record_batch()
        started = time.perf_counter()
        _core.ext_pivot_to_wide(batch)
        if time.perf_counter() - started >= min_seconds or copies >= MAX_PIVOT_COPIES:
            return batch
        copies *= 2


def test_detached_native_kernel_lets_another_thread_run():
    batch = _long_format_batch(MIN_CALL_SECONDS)
    pivoted = []

    assert releases_gil(lambda: pivoted.append(_core.ext_pivot_to_wide(batch)))
    assert pivoted[0].num_rows == 10_000


def test_python_loop_keeps_other_threads_waiting_and_restores_switch_interval():
    interval = sys.getswitchinterval()

    def spin():
        deadline = time.perf_counter() + MIN_CALL_SECONDS
        while time.perf_counter() < deadline:
            pass

    assert not releases_gil(spin)
    assert sys.getswitchinterval() == interval
