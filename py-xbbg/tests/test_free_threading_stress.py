"""Many threads drive shared native state at once, each on its own event loop.

The subscription and request drivers exist only in native extensions built with
`test-support`; tests that need them skip otherwise. Set XBBG_REQUIRE_TEST_SUPPORT=1
in the dedicated run to fail rather than skip when the wrong extension is installed.
Every test also passes on GIL builds, where native work still runs in parallel while
the threads take turns in the interpreter.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import xbbg
from xbbg import _core, _engine, blp

THREADS = 8
BARRIER_TIMEOUT_S = 30
READ_TIMEOUT_S = 10
JOIN_TIMEOUT_S = 60
FIELDS = ["SEQ", "LABEL"]


def _test_support(name):
    driver = getattr(_core, name, None)
    if driver is None:
        if os.environ.get("XBBG_REQUIRE_TEST_SUPPORT") == "1":
            pytest.fail("native extension was not built with pyo3-xbbg/test-support")
        pytest.skip("requires a native extension built with pyo3-xbbg/test-support")
    return driver


def _run_on_new_loop(make_awaitable):
    """Create and await a native awaitable on a fresh event loop owned by the calling thread."""

    async def run():
        return await asyncio.wait_for(make_awaitable(), READ_TIMEOUT_S)

    return asyncio.run(run())


class _Workers:
    """Threads whose first failure fails the test and whose hang cannot outlive a bounded join.

    A failure or hang aborts the shared barriers and sets ``stop`` so the remaining
    threads give up promptly instead of waiting for partners that will never arrive.
    """

    def __init__(self, *barriers, stop=None):
        self._barriers = barriers
        self._stop = stop
        self._threads = []
        self._errors = []

    def start(self, target, *args):
        thread = threading.Thread(
            target=self._run,
            args=(target, args),
            name=f"{target.__name__}-{len(self._threads)}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()
        return thread

    def _run(self, target, args):
        try:
            target(*args)
        except BaseException as exc:
            self._errors.append(exc)
            self._release()

    def _release(self):
        for barrier in self._barriers:
            barrier.abort()
        if self._stop is not None:
            self._stop.set()

    def join(self, threads=None):
        threads = self._threads if threads is None else threads
        deadline = time.monotonic() + JOIN_TIMEOUT_S
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        hung = [thread.name for thread in threads if thread.is_alive()]
        if hung:
            self._release()
        if self._errors:
            raise self._errors[0]
        assert not hung, f"threads still running after {JOIN_TIMEOUT_S} s: {hung}"


@pytest.fixture
def subscription_driver(monkeypatch):
    """One synthetic subscription engine, shared by every thread through ``xbbg.asubscribe``."""
    driver = _run_on_new_loop(_test_support("_test_subscription_fixture"))
    monkeypatch.setattr(_engine, "_get_engine", lambda: driver)
    yield driver
    driver.terminate()
    _run_on_new_loop(driver.settle)


@pytest.fixture
def request_driver():
    return _test_support("_test_request_fixture")()


@pytest.fixture(autouse=True)
def isolated_engine_state():
    """Start without a global engine or configuration, and restore both afterwards."""
    saved = (_engine._config, _engine._engine)
    _engine._config = _engine._engine = None
    try:
        yield
    finally:
        _engine._config, _engine._engine = saved


# =============================================================================
# One subscription driver, one topic per thread
# =============================================================================

TOPIC_UPDATES = 400
PRODUCERS = 4
READ_MODES = ("batches", "ticks", "deliveries")


def _consume_own_topic(topic, mode, start, subscribed, read_all, results):
    async def consume():
        start.wait(BARRIER_TIMEOUT_S)
        # drop_newest fails the stream on overflow, and the capacity holds every
        # update, so reading them all proves none was dropped or reordered.
        subscription = await asyncio.wait_for(
            xbbg.asubscribe(
                topic,
                FIELDS,
                raw=True,
                tick_mode=mode == "ticks",
                stream_capacity=TOPIC_UPDATES,
                overflow_policy="drop_newest",
            ),
            READ_TIMEOUT_S,
        )
        subscribed.wait(BARRIER_TIMEOUT_S)
        rows = []
        if mode == "deliveries":
            view = subscription.deliveries(raw=True)
            while len(rows) < TOPIC_UPDATES:
                delivery = await asyncio.wait_for(view.read(), READ_TIMEOUT_S)
                assert delivery.gap_before is None, f"{topic} reported lost updates: {delivery.gap_before}"
                rows.extend(delivery.data.to_pylist())
            read_all.wait(BARRIER_TIMEOUT_S)
            leftover = await asyncio.wait_for(view.unsubscribe(drain=True), READ_TIMEOUT_S)
        else:
            while len(rows) < TOPIC_UPDATES:
                item = await asyncio.wait_for(anext(subscription), READ_TIMEOUT_S)
                rows.extend([item] if mode == "ticks" else item.to_pylist())
            read_all.wait(BARRIER_TIMEOUT_S)
            leftover = await asyncio.wait_for(subscription.unsubscribe(drain=True), READ_TIMEOUT_S)
        return rows, leftover, subscription.is_active

    results[topic] = asyncio.run(consume())


def _publish_in_order(driver, topics, subscribed):
    subscribed.wait(BARRIER_TIMEOUT_S)
    for sequence in range(TOPIC_UPDATES):
        for topic in topics:
            driver.publish_json(topic, json.dumps({"SEQ": sequence, "LABEL": topic}))


@pytest.mark.thread_unsafe(reason="starts its own threads and routes xbbg.asubscribe to a shared driver")
def test_threads_sharing_one_subscription_driver_read_only_their_own_topic_in_order(subscription_driver):
    topics = [f"SYNTHETIC STRESS {index} Equity" for index in range(THREADS)]
    start = threading.Barrier(THREADS)
    subscribed = threading.Barrier(THREADS + PRODUCERS)
    read_all = threading.Barrier(THREADS)
    results = {}
    workers = _Workers(start, subscribed, read_all)
    for index, topic in enumerate(topics):
        mode = READ_MODES[index % len(READ_MODES)]
        workers.start(_consume_own_topic, topic, mode, start, subscribed, read_all, results)
    for producer in range(PRODUCERS):
        workers.start(_publish_in_order, subscription_driver, topics[producer::PRODUCERS], subscribed)
    workers.join()

    assert sorted(results) == sorted(topics)
    for topic, (rows, leftover, active) in results.items():
        assert [(row["topic"], row["LABEL"]) for row in rows] == [(topic, topic)] * TOPIC_UPDATES
        assert [row["SEQ"] for row in rows] == [float(sequence) for sequence in range(TOPIC_UPDATES)]
        assert leftover == []
        assert active is False


# =============================================================================
# One request driver, one event loop per thread
# =============================================================================

ERROR_CLASSES = {
    "session": _core.BlpSessionError,
    "request": _core.BlpRequestError,
    "limit": _core.BlpLimitError,
    "data_loss": _core.BlpSubscriptionDataLossError,
    "validation": _core.BlpValidationError,
    "timeout": _core.BlpTimeoutError,
    "internal": _core.BlpInternalError,
}
# None asks for a successful result; each caller rotates the plan every round.
REQUEST_PLAN = (None, *ERROR_CLASSES, None)
REQUEST_ROUNDS = 20
REQUEST_SERVICE = "//synthetic/refdata"


def _result_rows(caller, position):
    """A row count no other caller or position uses, so a misrouted result cannot pass."""
    return 1 + caller * len(REQUEST_PLAN) + position


def _request_label(caller, round_, position):
    return f"synthetic-{caller}-{round_}-{position}"


def _gated_request(driver, caller, round_, position, kind):
    if kind is None:
        return driver.request(_result_rows(caller, position), gated=True)
    label = _request_label(caller, round_, position)
    return driver.request(gated=True, error=kind, message=f"{kind} {label}", service=REQUEST_SERVICE, request_id=label)


def _request_rounds(driver, caller, released, outcomes):
    async def rounds():
        completed = []
        for round_ in range(REQUEST_ROUNDS):
            shift = (caller + round_) % len(REQUEST_PLAN)
            plan = REQUEST_PLAN[shift:] + REQUEST_PLAN[:shift]
            gated = [_gated_request(driver, caller, round_, position, kind) for position, kind in enumerate(plan)]
            # An ungated request completes on this loop while the gated ones stay pending.
            ungated = await asyncio.wait_for(driver.request(caller + 1), READ_TIMEOUT_S)
            assert ungated.num_rows == caller + 1
            assert not any(future.done() for future in gated), "a gated request completed before its release"
            # The last caller to arrive releases every gated request, all in flight at once.
            released.wait(BARRIER_TIMEOUT_S)
            results = await asyncio.wait_for(asyncio.gather(*gated, return_exceptions=True), READ_TIMEOUT_S)
            completed.append((plan, results))
        return completed

    outcomes[caller] = asyncio.run(rounds())


@pytest.mark.thread_unsafe(reason="starts its own threads, each with its own event loop")
def test_gated_requests_from_many_event_loops_resolve_to_their_own_results_and_errors(request_driver):
    released = threading.Barrier(THREADS, action=request_driver.release_requests)
    outcomes = {}
    workers = _Workers(released)
    for caller in range(THREADS):
        workers.start(_request_rounds, request_driver, caller, released, outcomes)
    workers.join()

    assert sorted(outcomes) == list(range(THREADS))
    for caller, completed in outcomes.items():
        assert len(completed) == REQUEST_ROUNDS
        for round_, (plan, results) in enumerate(completed):
            for position, (kind, result) in enumerate(zip(plan, results, strict=True)):
                if kind is None:
                    rows = _result_rows(caller, position)
                    assert isinstance(result, xbbg.ArrowRecordBatch), repr(result)
                    assert result.to_pylist() == [
                        {"ticker": f"SYNTHETIC {row} Equity", "row": row} for row in range(rows)
                    ]
                    continue
                label = _request_label(caller, round_, position)
                assert type(result) is ERROR_CLASSES[kind], repr(result)
                assert str(result) == f"{kind} {label}"
                assert (result.service, result.request_id) == (REQUEST_SERVICE, label)


# =============================================================================
# Subscribe/unsubscribe churn on shared feeds
# =============================================================================

CHURN_TOPICS = tuple(f"SYNTHETIC CHURN {index} Equity" for index in range(3))
CHURN_ROUNDS = 24
CHURN_READS = 3
CHURN_CAPACITY = 4
NO_SUBSCRIBER = frozenset(
    {
        "Configuration error: synthetic topic has no active subscription",
        "Configuration error: synthetic subscription is no longer registered",
    }
)


class _Ledger:
    """One subscription's updates for its topic: consecutive rows and exactly accounted drops."""

    def __init__(self, topic):
        self.topic = topic
        self.last = None

    def record(self, delivery):
        assert delivery.kind == "deltas"
        rows = delivery.data.to_pylist()
        assert rows, f"a {self.topic} delivery carried no rows"
        assert {(row["topic"], row["LABEL"]) for row in rows} == {(self.topic, self.topic)}
        sequence = [int(row["SEQ"]) for row in rows]
        assert sequence == list(range(sequence[0], sequence[0] + len(sequence))), (
            f"{self.topic} skipped updates inside one delivery: {sequence}"
        )
        dropped = 0 if delivery.gap_before is None else delivery.gap_before.dropped_updates
        # Drops before the first delivery are unknowable (the feed was already live).
        if self.last is not None:
            assert sequence[0] - self.last - 1 == dropped, (
                f"{self.topic} jumped from {self.last} to {sequence[0]} but reported {dropped} dropped updates"
            )
        self.last = sequence[-1]


def _churn(index, start, completed):
    async def churn():
        start.wait(BARRIER_TIMEOUT_S)
        for round_ in range(CHURN_ROUNDS):
            topic = CHURN_TOPICS[(index + round_) % len(CHURN_TOPICS)]
            subscription = await asyncio.wait_for(
                xbbg.asubscribe(
                    topic,
                    FIELDS,
                    raw=True,
                    flush_threshold=2,
                    stream_capacity=CHURN_CAPACITY,
                    overflow_policy="drop_oldest",
                ),
                READ_TIMEOUT_S,
            )
            view = subscription.deliveries(raw=True)
            ledger = _Ledger(topic)
            for _ in range(CHURN_READS):
                ledger.record(await asyncio.wait_for(view.read(), READ_TIMEOUT_S))
            close = round_ % 3
            if close == 0:
                for delivery in await asyncio.wait_for(view.unsubscribe(drain=True), READ_TIMEOUT_S):
                    ledger.record(delivery)
            elif close == 1:
                await asyncio.wait_for(subscription.unsubscribe(), READ_TIMEOUT_S)
            else:
                await asyncio.wait_for(view.aclose(), READ_TIMEOUT_S)
            assert not subscription.is_active
            completed[index] = round_ + 1

    asyncio.run(churn())


def _publish_until_stopped(driver, topic, start, stop):
    start.wait(BARRIER_TIMEOUT_S)
    sequence = 0
    while not stop.is_set():
        try:
            driver.publish_json(topic, json.dumps({"SEQ": sequence, "LABEL": topic}))
        except _core.BlpValidationError as exc:
            if str(exc) not in NO_SUBSCRIBER:
                raise
            # Nobody is subscribed, so this sequence number was never published.
            continue
        sequence += 1


@pytest.mark.thread_unsafe(reason="starts its own threads and routes xbbg.asubscribe to a shared driver")
def test_subscription_churn_with_concurrent_producers_keeps_exact_gap_accounting(subscription_driver):
    start = threading.Barrier(THREADS + len(CHURN_TOPICS))
    stop = threading.Event()
    completed = {}
    workers = _Workers(start, stop=stop)
    churners = [workers.start(_churn, index, start, completed) for index in range(THREADS)]
    for topic in CHURN_TOPICS:
        workers.start(_publish_until_stopped, subscription_driver, topic, start, stop)
    workers.join(churners)
    stop.set()
    workers.join()

    assert completed == dict.fromkeys(range(THREADS), CHURN_ROUNDS)

    # Every consumer left: cleanup retires each shared feed.
    _run_on_new_loop(subscription_driver.settle)
    for topic in CHURN_TOPICS:
        with pytest.raises(_core.BlpValidationError, match="synthetic topic has no active subscription"):
            subscription_driver.publish_json(topic, json.dumps({"SEQ": -1, "LABEL": topic}))

    returned = threading.Event()

    def wait_for_teardown():
        _core._wait_native_teardown()
        returned.set()

    threading.Thread(target=wait_for_teardown, daemon=True).start()
    assert returned.wait(READ_TIMEOUT_S), "_wait_native_teardown() blocked after the churn"


# =============================================================================
# Concurrent first import of the native extension
# =============================================================================

CONCURRENT_FIRST_IMPORT = textwrap.dedent(
    """
    import importlib
    import sys
    import sysconfig
    import threading
    import time

    import xbbg

    THREADS = int(sys.argv[1])
    if "xbbg._core" in sys.modules:
        raise SystemExit("xbbg._core was imported by `import xbbg`")
    start = threading.Barrier(THREADS)
    results = {}


    def import_core(route):
        if route == 0:
            return xbbg._import_core()
        if route == 1:
            from xbbg import _core

            return _core
        if route == 2:
            return importlib.import_module("xbbg._core")
        from xbbg._core import ArrowTable  # bypasses xbbg.__getattr__

        core = sys.modules["xbbg._core"]
        if ArrowTable is not core.ArrowTable:
            raise RuntimeError("from-import returned a class from another module object")
        return core


    def first_import(index):
        try:
            start.wait(60)
            core = import_core(index % 4)
            results[index] = (core, core.ArrowTable.from_pylist([{"thread": index}]).to_pylist())
        except BaseException as exc:
            results[index] = exc


    threads = [threading.Thread(target=first_import, args=(index,), daemon=True) for index in range(THREADS)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 60
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    if any(thread.is_alive() for thread in threads):
        raise SystemExit("a thread never finished importing xbbg._core")
    failures = {index: repr(value) for index, value in results.items() if isinstance(value, BaseException)}
    if failures:
        raise SystemExit(f"concurrent first import failed: {failures}")
    core = sys.modules["xbbg._core"]
    if len(results) != THREADS or {id(module) for module, _ in results.values()} != {id(core)}:
        raise SystemExit(f"threads saw different xbbg._core modules: {results}")
    if xbbg._import_core() is not core:
        raise SystemExit("xbbg cached a different xbbg._core")
    if any(rows != [{"thread": index}] for index, (_, rows) in results.items()):
        raise SystemExit(f"xbbg._core misbehaved after a concurrent import: {results}")
    if sysconfig.get_config_var("Py_GIL_DISABLED") and sys._is_gil_enabled():
        raise SystemExit("importing xbbg._core concurrently re-enabled the GIL")
    print("imported once")
    """
)


@pytest.mark.thread_unsafe(reason="starts a subprocess whose threads import xbbg._core together")
def test_concurrent_first_import_yields_one_working_native_module():
    # PYTHON_GIL would keep the GIL off even if an import re-enabled it.
    env = {name: value for name, value in os.environ.items() if name != "PYTHON_GIL"}
    env["PYTHONPATH"] = str(Path(xbbg.__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", CONCURRENT_FIRST_IMPORT, str(THREADS)],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith("imported once"), result.stdout + result.stderr
    for failure in ("panicked at", "Fatal Python error"):
        assert failure not in result.stderr, result.stderr


# =============================================================================
# Global configuration under concurrent writers and readers
# =============================================================================

CONFIG_WRITERS = 4
CONFIG_READERS = 4
CONFIG_ROUNDS = 40
CONFIG_PORT_BASE = 20000


def _written(writer, round_):
    """Settings whose host, port and key column identify the writer and round together."""
    return {
        "host": f"synthetic-{writer}-{round_}",
        "port": CONFIG_PORT_BASE + 100 * writer + round_,
        "key_column": ("ticker", "security")[round_ % 2],
    }


def _write_configurations(writer, start):
    start.wait(BARRIER_TIMEOUT_S)
    for round_ in range(CONFIG_ROUNDS):
        if round_ % 2:
            xbbg.configure(xbbg.EngineConfig(), **_written(writer, round_))
            xbbg.reset()
        else:
            xbbg.configure(**_written(writer, round_))
            xbbg.shutdown()
    # Every writer ends with configure, so the last write overall is a configure.
    xbbg.configure(**_written(writer, CONFIG_ROUNDS))


def _read_configurations(start, writers_done, reads):
    start.wait(BARRIER_TIMEOUT_S)
    count = 0
    while True:
        environment = blp._snapshot_request_environment()
        if environment.source == "default_engine":
            assert (environment.host, environment.port, environment.servers) == ("localhost", 8194, ())
        else:
            assert environment.source == "global_config", environment
            writer, round_ = divmod(environment.port - CONFIG_PORT_BASE, 100)
            assert environment.host == _written(writer, round_)["host"], environment
            assert environment.servers == ((environment.host, environment.port),), environment
        assert _engine._active_key_column() in {"ticker", "security"}
        assert not xbbg.is_connected()
        count += 1
        if writers_done.is_set():
            break
    reads.append(count)


@pytest.mark.thread_unsafe(reason="starts its own threads and rewrites the process-wide engine configuration")
def test_configure_shutdown_and_reset_race_with_configuration_readers():
    start = threading.Barrier(CONFIG_WRITERS + CONFIG_READERS)
    writers_done = threading.Event()
    reads = []
    workers = _Workers(start, stop=writers_done)
    writers = [workers.start(_write_configurations, writer, start) for writer in range(CONFIG_WRITERS)]
    for _ in range(CONFIG_READERS):
        workers.start(_read_configurations, start, writers_done, reads)
    workers.join(writers)
    writers_done.set()
    workers.join()

    assert len(reads) == CONFIG_READERS
    final = _engine._config
    assert final is not None
    assert {"host": final.host, "port": final.port, "key_column": final.key_column} in [
        _written(writer, CONFIG_ROUNDS) for writer in range(CONFIG_WRITERS)
    ]
    assert _engine._engine is None
