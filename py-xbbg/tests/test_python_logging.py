"""Opt-in forwarding of native log events to Python ``logging`` (offline).

Native events come from ``_core._test_log_fixture``, which emits through the same
filter and destination as the engine's own events, so no Bloomberg session is needed.
The probe exists only in native extensions built with `test-support`; tests that need
it skip otherwise. Set XBBG_REQUIRE_TEST_SUPPORT=1 in the dedicated run to fail rather
than skip when the wrong extension is installed.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import xbbg
from xbbg import _core, _python_logging

pytestmark = pytest.mark.thread_unsafe(reason="the log bridge is process-global state")

PROBE_LOGGER = "_core.log_bridge"
WAIT_S = 10


def _native_probe():
    probe = getattr(_core, "_test_log_fixture", None)
    if probe is None:
        if os.environ.get("XBBG_REQUIRE_TEST_SUPPORT") == "1":
            pytest.fail("native extension was not built with pyo3-xbbg/test-support")
        pytest.skip("requires a native extension built with pyo3-xbbg/test-support")
    return probe


@pytest.fixture
def emit():
    """Emit one native log event: ``emit(level, message)``."""
    return _native_probe()


def wait_for(predicate, timeout: float = WAIT_S) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def forwarder_threads() -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == "xbbg-python-logging"]


class Collector(logging.Handler):
    """Keeps records; optionally blocks the forwarder inside ``emit``."""

    def __init__(self, *, block_first: threading.Event | None = None, raise_first: bool = False) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []
        self.delivered_at: list[float] = []
        self.entered = threading.Event()
        self._block = block_first
        self._raise_first = raise_first
        self._first = True

    def emit(self, record: logging.LogRecord) -> None:
        first, self._first = self._first, False
        if first:
            self.entered.set()
            if self._block is not None:
                self._block.wait(WAIT_S)
            if self._raise_first:
                raise RuntimeError("handler failure")
        self.records.append(record)
        self.delivered_at.append(time.time())

    def messages(self) -> list[str]:
        return [record.getMessage() for record in list(self.records)]


@pytest.fixture(autouse=True)
def clean_bridge():
    xbbg.disable_python_logging(timeout=2)
    previous_level = xbbg.get_log_level()
    xbbg.set_log_level("trace")
    yield
    xbbg.disable_python_logging(timeout=2)
    xbbg.set_log_level(previous_level)
    assert wait_for(lambda: not forwarder_threads()), "forwarder thread did not exit"


@pytest.fixture
def probe():
    """Route the probe logger to a collector alone, at ``level``; restores it afterwards."""
    logger = logging.getLogger(PROBE_LOGGER)
    previous = (logger.level, logger.propagate)
    attached: list[Collector] = []

    def configure(collector: Collector, *, level: int = logging.INFO) -> Collector:
        logger.addHandler(collector)
        attached.append(collector)
        logger.propagate = False
        logger.setLevel(level)
        return collector

    yield configure
    for collector in attached:
        logger.removeHandler(collector)
    logger.setLevel(previous[0])
    logger.propagate = previous[1]


def test_exports_are_lazy_package_attributes():
    for name in ("enable_python_logging", "disable_python_logging", "python_logging_dropped"):
        assert name in xbbg.__all__
        assert getattr(xbbg, name) is getattr(_python_logging, name)


def test_native_event_arrives_as_log_record(caplog, emit):
    caplog.set_level(logging.INFO)
    xbbg.enable_python_logging()

    before = time.time()
    emit("info", "synthetic probe event")
    after = time.time()

    assert wait_for(lambda: "synthetic probe event" in caplog.text)
    (record,) = [r for r in caplog.records if r.name == PROBE_LOGGER]
    assert record.levelno == logging.INFO
    assert record.getMessage() == "synthetic probe event"
    assert record.pathname.endswith("log_bridge.rs")
    assert record.lineno > 0
    assert before <= record.created <= after
    assert record.threadName
    assert record.threadName != "xbbg-python-logging"


def test_records_keep_emission_time_and_thread_while_delivery_is_late(probe, emit):
    release = threading.Event()
    collector = probe(Collector(block_first=release))
    xbbg.enable_python_logging()

    emit("info", "first")
    assert collector.entered.wait(WAIT_S), "forwarder never reached the handler"
    emitted = time.time()
    emit("info", "second")
    time.sleep(0.3)
    release.set()

    assert wait_for(lambda: collector.messages() == ["first", "second"])
    second = collector.records[1]
    assert abs(second.created - emitted) < 0.1
    assert collector.delivered_at[1] - second.created >= 0.25
    assert abs(second.msecs - (second.created % 1) * 1000) < 1.0
    fresh = logging.makeLogRecord({})
    assert abs((fresh.relativeCreated - second.relativeCreated) - (fresh.created - second.created) * 1000) < 50
    assert second.threadName != "xbbg-python-logging"


def test_python_logger_levels_filter_after_capture(probe, emit):
    collector = probe(Collector(), level=logging.WARNING)
    xbbg.enable_python_logging()

    emit("debug", "debug event")
    emit("info", "info event")
    emit("warn", "warn event")
    emit("error", "error event")

    assert wait_for(lambda: len(collector.records) == 2)
    assert collector.messages() == ["warn event", "error event"]
    assert [record.levelno for record in collector.records] == [logging.WARNING, logging.ERROR]


def test_native_level_decides_what_is_captured(probe, emit):
    collector = probe(Collector(), level=logging.DEBUG)
    xbbg.set_log_level("warn")
    xbbg.enable_python_logging()

    emit("info", "below the native threshold")
    emit("warn", "at the native threshold")

    assert wait_for(lambda: collector.messages() == ["at the native threshold"])


def test_trace_level_is_named_and_forwarded(probe, emit):
    collector = probe(Collector(), level=5)
    xbbg.enable_python_logging()

    assert logging.getLevelName(5) != "Level 5"
    emit("trace", "trace event")

    assert wait_for(lambda: collector.messages() == ["trace event"])
    assert collector.records[0].levelno == 5
    assert collector.records[0].levelname == logging.getLevelName(5)


def test_enabled_bridge_replaces_stderr_and_disable_restores_it(capfd, caplog, emit):
    caplog.set_level(logging.INFO)
    xbbg.set_log_level("info")
    capfd.readouterr()

    xbbg.enable_python_logging()
    emit("info", "while forwarding")
    assert wait_for(lambda: "while forwarding" in caplog.text)
    assert "while forwarding" not in capfd.readouterr().err

    xbbg.disable_python_logging()
    assert not forwarder_threads()
    caplog.clear()
    emit("info", "after disable")
    assert "after disable" in capfd.readouterr().err
    time.sleep(0.2)
    assert "after disable" not in caplog.text


def test_disabled_by_default_leaves_stderr_untouched(capfd, caplog, emit):
    caplog.set_level(logging.INFO)
    xbbg.set_log_level("info")
    capfd.readouterr()

    emit("info", "plain stderr")

    err = capfd.readouterr().err
    assert "plain stderr" in err
    assert "_core::log_bridge" in err
    time.sleep(0.2)
    assert "plain stderr" not in caplog.text


def test_enable_is_idempotent_and_queue_size_change_is_rejected():
    xbbg.enable_python_logging(queue_size=64)
    (thread,) = forwarder_threads()
    xbbg.enable_python_logging(queue_size=64)
    assert forwarder_threads() == [thread]
    assert thread.daemon

    with pytest.raises(RuntimeError, match="queue_size=64"):
        xbbg.enable_python_logging(queue_size=128)
    assert forwarder_threads() == [thread]

    xbbg.disable_python_logging()
    xbbg.enable_python_logging(queue_size=128)
    assert len(forwarder_threads()) == 1


def test_argument_validation():
    with pytest.raises(ValueError, match="at least 1"):
        xbbg.enable_python_logging(queue_size=0)
    with pytest.raises(TypeError, match="int"):
        xbbg.enable_python_logging(queue_size="10")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-negative"):
        xbbg.disable_python_logging(timeout=-1)
    with pytest.raises(ValueError, match="non-negative"):
        xbbg.disable_python_logging(timeout=float("nan"))
    assert not forwarder_threads()
    xbbg.disable_python_logging()  # not enabled: nothing to do


def test_native_api_reports_drops_without_a_later_record():
    dropped_before = xbbg.python_logging_dropped()
    assert _core._log_bridge_enable(2)
    try:
        assert not _core._log_bridge_enable(8)
        _core._log_bridge_count_dropped(5)
        assert xbbg.python_logging_dropped() - dropped_before == 5
        assert _core._log_bridge_next_batch(0.0, 8) == ([], 5, False)
        assert _core._log_bridge_next_batch(0.0, 8) == ([], 0, False)
    finally:
        _core._log_bridge_disable()
    assert _core._log_bridge_next_batch(0.0, 8) == ([], 0, True)
    assert _core._log_bridge_discard() == 0
    with pytest.raises(ValueError, match="timeout"):
        _core._log_bridge_next_batch(-1.0, 8)


def test_full_queue_drops_are_counted_and_warned(probe, caplog, emit):
    release = threading.Event()
    collector = probe(Collector(block_first=release), level=logging.INFO)
    caplog.set_level(logging.WARNING, logger="xbbg.logging")
    xbbg.enable_python_logging(queue_size=2)
    dropped_before = xbbg.python_logging_dropped()

    emit("info", "held by the handler")
    assert collector.entered.wait(WAIT_S)
    for index in range(10):
        emit("info", f"burst {index}")

    # The running total is readable before any later record could report it.
    assert xbbg.python_logging_dropped() - dropped_before == 8
    release.set()

    assert wait_for(lambda: len(collector.records) == 3)
    assert collector.messages() == ["held by the handler", "burst 0", "burst 1"]
    assert wait_for(lambda: any(r.name == "xbbg.logging" for r in caplog.records))
    (warning,) = [r for r in caplog.records if r.name == "xbbg.logging"]
    assert warning.levelno == logging.WARNING
    assert warning.getMessage() == "dropped 8 xbbg log records: queue full"
    assert xbbg.python_logging_dropped() - dropped_before == 8


def test_disable_with_a_stuck_handler_times_out_and_counts_leftovers(probe, emit):
    release = threading.Event()
    collector = probe(Collector(block_first=release), level=logging.INFO)
    xbbg.enable_python_logging(queue_size=16)
    dropped_before = xbbg.python_logging_dropped()

    emit("info", "held by the handler")
    assert collector.entered.wait(WAIT_S)
    for index in range(3):
        emit("info", f"queued {index}")

    started = time.monotonic()
    xbbg.disable_python_logging(timeout=0.2)
    assert time.monotonic() - started < 5
    assert xbbg.python_logging_dropped() - dropped_before == 3

    release.set()
    assert wait_for(lambda: not forwarder_threads())
    assert collector.messages() == ["held by the handler"]


def test_disable_drains_queued_records_before_stopping(probe, emit):
    collector = probe(Collector(), level=logging.INFO)
    xbbg.enable_python_logging()
    dropped_before = xbbg.python_logging_dropped()

    for index in range(50):
        emit("info", f"record {index}")
    xbbg.disable_python_logging(timeout=WAIT_S)

    assert collector.messages() == [f"record {index}" for index in range(50)]
    assert xbbg.python_logging_dropped() == dropped_before
    assert not forwarder_threads()


def test_a_failing_handler_does_not_stop_the_forwarder(probe, monkeypatch, emit):
    monkeypatch.setattr(logging, "raiseExceptions", False)
    collector = probe(Collector(raise_first=True), level=logging.INFO)
    xbbg.enable_python_logging()

    emit("info", "lost to the failing handler")
    emit("info", "delivered afterwards")

    assert wait_for(lambda: collector.messages() == ["delivered afterwards"])
    assert forwarder_threads()


def test_disable_from_the_forwarder_thread_is_rejected(probe, emit):
    errors: list[BaseException] = []
    done = threading.Event()

    class Reentrant(Collector):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                xbbg.disable_python_logging()
            except RuntimeError as error:
                errors.append(error)
            done.set()

    probe(Reentrant())
    xbbg.enable_python_logging()
    emit("info", "re-entrant")

    assert done.wait(WAIT_S)
    assert "cannot be called from a log handler" in str(errors[0])


def test_unknown_probe_level_is_rejected(emit):
    with pytest.raises(ValueError, match="Invalid log level"):
        emit("loud", "message")


EXIT_WITH_BRIDGE = textwrap.dedent(
    """
    import logging
    import sys

    import xbbg
    from xbbg import _core

    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(name)s|%(levelname)s|%(message)s")
    xbbg.set_log_level("info")
    xbbg.enable_python_logging()
    print("exiting with the bridge enabled", flush=True)
    for index in range(20):
        _core._test_log_fixture("info", f"event {index}")
    """
)


def test_interpreter_exit_drains_and_stops_the_bridge():
    _native_probe()
    env = {**os.environ, "PYTHONPATH": str(Path(xbbg.__file__).resolve().parents[1])}

    result = subprocess.run(
        [sys.executable, "-c", EXIT_WITH_BRIDGE],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    lines = result.stdout.splitlines()
    assert "exiting with the bridge enabled" in lines
    forwarded = [line for line in lines if line.startswith("_core.log_bridge|INFO|event ")]
    assert forwarded == [f"_core.log_bridge|INFO|event {index}" for index in range(20)]
    for failure in ("panicked at", "Fatal Python error", "Traceback"):
        assert failure not in result.stderr, result.stderr
    assert "event " not in result.stderr
