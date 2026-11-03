"""Detect whether a call lets other Python threads run on a GIL-enabled CPython.

On a GIL build another Python thread can only run while the calling thread does
not hold the GIL. ``releases_gil`` raises the switch interval far beyond the
probe's run time, so the interpreter never forces a switch, wakes a helper
thread, and reports whether that helper ran during the call. Only a call that
released the GIL lets it run.

A call can release the GIL and return before the operating system schedules the
helper, so the probe repeats it until the helper has run or ``_RETRY_SECONDS``
have passed. Callers must pass a call that is safe to repeat, and skip on
free-threaded builds, where threads run regardless.
"""

from __future__ import annotations

from collections.abc import Callable
import sys
import threading
import time

# A waiting thread only forces a GIL switch after this long, so the whole probe
# must finish well within it.
_SWITCH_INTERVAL_SECONDS = 10.0
# How long to keep repeating the call while the helper has not run.
_RETRY_SECONDS = 1.0


def releases_gil(call: Callable[[], object]) -> bool:
    """Run ``call()`` on this thread and return whether another thread ran meanwhile."""
    if getattr(sys, "_is_gil_enabled", lambda: True)() is False:
        raise RuntimeError("releases_gil needs the GIL; skip this check on free-threaded builds")
    waiting = threading.Event()
    wake = threading.Event()
    ran: list[bool] = []

    def helper() -> None:
        waiting.set()
        wake.wait()
        ran.append(True)

    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(_SWITCH_INTERVAL_SECONDS)
    helper_thread = threading.Thread(target=helper, name="gil-probe-helper", daemon=True)
    try:
        helper_thread.start()
        # The helper keeps the GIL from waiting.set() until it blocks inside
        # wake.wait(), so this returns only once it is parked there.
        waiting.wait()
        wake.set()
        deadline = time.monotonic() + _RETRY_SECONDS
        call()
        # The loop itself never releases the GIL, so the helper can only run
        # inside one of the calls.
        while not ran and time.monotonic() < deadline:
            call()
        released = bool(ran)
    finally:
        wake.set()
        sys.setswitchinterval(previous_interval)
        helper_thread.join()
    return released
