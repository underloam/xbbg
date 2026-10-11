#!/usr/bin/env python
"""Live test: subscribe to prices, dynamically add and remove securities.

Demonstrates the full subscription lifecycle:
  1. Subscribe to initial tickers
  2. Stream data for a few seconds
  3. Add a new ticker mid-stream
  4. Stream more data (should see new ticker)
  5. Remove a ticker mid-stream
  6. Stream more data (removed ticker should stop)
  7. Clean up

Usage:
    uv run python py-xbbg/tests/live/test_subscription_add_remove.py
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


# -- Configuration -----------------------------------------------------------

INITIAL_TICKERS = ["ES1 Index", "NQ1 Index"]
ADD_TICKER = "UX1 Index"
REMOVE_TICKER = "ES1 Index"

FIELDS = ["LAST_PRICE", "BID", "ASK", "VOLUME"]

# How many batches to collect in each phase
# (with markets closed we'll mostly just see INITPAINT snapshots)
PHASE_1_BATCHES = 5
PHASE_2_BATCHES = 5
PHASE_3_BATCHES = 5

PHASE_TIMEOUT = 15  # seconds to wait per phase before moving on


# -- Helpers -----------------------------------------------------------------


def print_header(msg: str) -> None:
    print(f"\n{'=' * 64}")
    print(f"  {msg}")
    print(f"{'=' * 64}\n")


def print_sub_state(sub) -> None:
    print(f"  is_active : {sub.is_active}")
    print(f"  tickers   : {sub.tickers}")
    print(f"  fields    : {sub.fields}")
    print()


def print_batch(batch, idx: int) -> None:
    topic = batch.column("topic")[0].as_py()
    ts = batch.column("timestamp")[0].as_py()

    vals = []
    for field in batch.schema:
        if field.name in ("timestamp", "topic"):
            continue
        val = batch.column(field.name)[0].as_py()
        if val is not None:
            vals.append(f"{field.name}={val}")

    vals_str = "  ".join(vals) if vals else "(no data yet)"
    print(f"  [{idx:3d}] {topic:30s} {vals_str}")


# -- Main --------------------------------------------------------------------


async def main() -> None:
    from xbbg._core import PyEngine, set_log_level

    set_log_level("info")

    engine = PyEngine()

    print_header("PHASE 1: Subscribe to initial tickers")
    print(f"  Tickers: {INITIAL_TICKERS}")
    print(f"  Fields:  {FIELDS}")
    print()

    sub = await engine.subscribe(INITIAL_TICKERS, FIELDS)
    print("Subscription created.")
    print_sub_state(sub)

    batch_num = 0

    async def collect_batches(target: int, label: str) -> int:
        """Collect up to `target` batches with a timeout. Returns count collected."""
        nonlocal batch_num
        count = 0
        deadline = asyncio.get_event_loop().time() + PHASE_TIMEOUT
        try:
            while count < target:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    print(f"\n  (timeout after {PHASE_TIMEOUT}s — got {count}/{target} batches)")
                    break
                batch = await asyncio.wait_for(sub.__anext__(), timeout=remaining)
                batch_num += 1
                count += 1
                print_batch(batch, batch_num)
        except TimeoutError:
            print(f"\n  (timeout after {PHASE_TIMEOUT}s — got {count}/{target} batches)")
        except StopAsyncIteration:
            print(f"\n  (stream ended — got {count}/{target} batches)")
        return count

    # -- Phase 1: stream initial tickers ------------------------------------
    print("Streaming...\n")
    await collect_batches(PHASE_1_BATCHES, "Phase 1")

    # -- Phase 2: add a ticker ---------------------------------------------
    print_header(f"PHASE 2: Adding '{ADD_TICKER}'")
    await sub.add([ADD_TICKER])
    print("Added. New state:")
    print_sub_state(sub)

    print("Streaming...\n")
    await collect_batches(PHASE_2_BATCHES, "Phase 2")

    # -- Phase 3: remove a ticker ------------------------------------------
    print_header(f"PHASE 3: Removing '{REMOVE_TICKER}'")
    await sub.remove([REMOVE_TICKER])
    print("Removed. New state:")
    print_sub_state(sub)

    print("Streaming...\n")
    await collect_batches(PHASE_3_BATCHES, "Phase 3")

    # -- Cleanup -----------------------------------------------------------
    print_header("CLEANUP: Unsubscribing")
    await sub.unsubscribe()
    print(f"Done. Total batches: {batch_num}")
    print(f"  Phase 1 (initial):  {PHASE_1_BATCHES}")
    print(f"  Phase 2 (after add): {PHASE_2_BATCHES}")
    print(f"  Phase 3 (after remove): {PHASE_3_BATCHES}")
    print()


if __name__ == "__main__":
    print(f"xbbg subscription add/remove test  [{datetime.now()}]")
    asyncio.run(main())
