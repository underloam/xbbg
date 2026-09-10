"""Live regression for ordinary engine shutdown versus interpreter finalization.

Run with an available Bloomberg connection:
    python py-xbbg/tests/repro_270_shutdown_panic.py

Each scenario runs in its own process because the interpreter-finalization signal
is irreversible. Non-global engines ensure the exit hook cannot depend on blp's
global engine. Timeouts, missing data, native panics, and child failures fail the
runner instead of silently counting an unavailable Bloomberg session as success.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

TICKER = "XBTUSD Curncy"
SCENARIOS = ("engine-shutdown", "interpreter-finalization")


async def run_scenario(scenario: str) -> None:
    from xbbg import blp

    engine = blp.Engine(
        request_pool_size=1,
        subscription_pool_size=1,
        max_subscription_sessions=1,
        runtime_worker_threads=2,
    )
    try:
        with engine:
            sub = await blp.asubscribe(TICKER, ["LAST_PRICE"], tick_mode=True, stream_capacity=4)
        first = await asyncio.wait_for(anext(sub), timeout=30)
        assert first.get("LAST_PRICE") is not None, first
        assert blp._engine is None, "scenario must exercise a non-global engine"

        # An empty removal completes without waiting on SDK acknowledgements.
        await asyncio.wait_for(sub.remove([]), timeout=2)
        if scenario == "engine-shutdown":
            from xbbg.exceptions import BlpError

            engine.shutdown()
            try:
                await asyncio.wait_for(sub.unsubscribe(drain=True), timeout=5)
            except BlpError as error:
                terminal_error = type(error).__name__
            else:
                raise AssertionError("ordinary engine shutdown must surface its terminal stream error")
            try:
                await asyncio.wait_for(anext(sub), timeout=2)
            except StopAsyncIteration:
                pass
            else:
                raise AssertionError("closed subscription yielded after engine shutdown")
            print(json.dumps({"scenario": scenario, "terminal_error": terminal_error, "closed": True}))
        else:
            # Exercise the actual atexit entry point while Python is still alive
            # so suppression is observable, rather than relying on a timing race.
            blp._atexit_cleanup()
            try:
                await asyncio.wait_for(sub.remove([]), timeout=0.25)
            except asyncio.TimeoutError:
                print(json.dumps({"scenario": scenario, "native_completion_suppressed": True}))
            else:
                raise AssertionError("native future completed after interpreter shutdown was signalled")
    finally:
        engine.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS)
    args = parser.parse_args()
    if args.scenario is not None:
        asyncio.run(run_scenario(args.scenario))
        return

    for scenario in SCENARIOS:
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--scenario", scenario],
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
        print(completed.stdout, end="")
        print(completed.stderr, end="", file=sys.stderr)
        completed.check_returncode()
        if "panicked at" in completed.stderr or "Fatal Python error" in completed.stderr:
            raise AssertionError(f"native shutdown failure in {scenario}")


if __name__ == "__main__":
    main()
