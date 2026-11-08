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


def _long_format_batch():
    """Return a synthetic long-format batch of 10,000 securities with five fields each."""
    return _core.ArrowTable.from_pylist(
        [
            {"ticker": f"SYNTH{ticker:05d} Equity", "field": f"FIELD_{field}", "value": f"{ticker}.{field}"}
            for ticker in range(10_000)
            for field in range(5)
        ]
    ).to_record_batch()


def test_detached_native_kernel_lets_another_thread_run():
    batch = _long_format_batch()
    pivoted = []

    assert releases_gil(lambda: pivoted.append(_core.ext_pivot_to_wide(batch)))
    assert pivoted[0].num_rows == 10_000


def test_release_shorter_than_a_thread_wakeup_is_detected():
    # time.sleep(0) releases the GIL only for the length of a system call, too
    # briefly for a single call to give the operating system time to wake the helper.
    assert releases_gil(lambda: time.sleep(0))


def test_python_loop_keeps_other_threads_waiting_and_restores_switch_interval():
    interval = sys.getswitchinterval()

    def spin():
        deadline = time.perf_counter() + 0.05
        while time.perf_counter() < deadline:
            pass

    assert not releases_gil(spin)
    assert sys.getswitchinterval() == interval
