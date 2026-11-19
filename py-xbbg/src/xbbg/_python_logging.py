"""Opt-in forwarding of native (Rust and SDK) log events to Python ``logging``.

By default xbbg's native layer writes its log lines straight to stderr and never
touches Python. :func:`enable_python_logging` redirects them into a bounded
native queue instead; a daemon thread started here drains the queue and emits
each event as a standard ``LogRecord``, so the application's handlers,
formatters and filters see them.

Design points:

- Native threads never touch Python and never wait: when the queue is full the
  record is dropped and counted (:func:`python_logging_dropped`).
- Python does the filtering after capture, so ``set_log_level`` (and
  ``RUST_LOG``) still decide what is captured at all.
- Records keep the time they were logged, the emitting thread's name and the
  Rust source location. Logger names are the Rust target with ``::`` replaced
  by ``.`` (for example ``xbbg_async.engine``); SDK lines use ``xbbg.sdk``.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import math
import os
import sys
import threading
import traceback
from typing import Any

# How long the forwarder waits inside the native call before it re-checks
# whether it was abandoned.
_POLL_SECONDS = 0.1
# At most this many records are handed to Python per native call.
_BATCH_SIZE = 256

_DEFAULT_QUEUE_SIZE = 10_000
_MAX_QUEUE_SIZE = 16_777_216
_TRACE_LEVEL = 5

_DROP_LOGGER_NAME = "xbbg.logging"


class _Forwarder:
    """One running bridge: its queue size, session consumer, thread and abandon state."""

    def __init__(self, core: Any, consumer: Any, queue_size: int) -> None:
        self.queue_size = queue_size
        self.abandoned = threading.Event()
        self._core = core
        self._consumer = consumer
        self._batch_lock = threading.Lock()
        self._remaining_in_batch = 0
        self.thread = threading.Thread(
            target=self._run,
            name="xbbg-python-logging",
            daemon=True,
        )

    def abandon(self) -> None:
        """Stop delivering further records and account for all undelivered ones."""
        with self._batch_lock:
            self.abandoned.set()
            undelivered = self._remaining_in_batch
            self._remaining_in_batch = 0
        if undelivered:
            self._consumer.count_dropped(undelivered)
        self._consumer.discard()

    def _run(self) -> None:
        consumer = self._consumer
        abandoned = self.abandoned
        loggers: dict[str, logging.Logger] = {}
        # LogRecord.relativeCreated counts from logging's own start time, which
        # is private; construct LogRecord directly so a custom LogRecord factory
        # that expects non-None name/level cannot abort the forwarder.
        reference = logging.LogRecord("", 0, "", 0, "", (), None)
        relative_offset = reference.relativeCreated - reference.created * 1000.0

        while not abandoned.is_set():
            records, dropped, closed = consumer.next_batch(_POLL_SECONDS, _BATCH_SIZE)
            with self._batch_lock:
                if abandoned.is_set():
                    if records:
                        consumer.count_dropped(len(records))
                    return
                self._remaining_in_batch = len(records)
            if dropped:
                _guarded(
                    logging.getLogger(_DROP_LOGGER_NAME).warning,
                    "dropped %d xbbg log records: queue full",
                    dropped,
                )
            for record in records:
                with self._batch_lock:
                    if abandoned.is_set():
                        return
                    self._remaining_in_batch -= 1
                _guarded(_deliver, loggers, relative_offset, record)
            if closed:
                return


def _guarded(function: Any, *args: Any) -> None:
    """Call ``function``; a failing handler must not stop the forwarder."""
    try:
        function(*args)
    except Exception:
        if logging.raiseExceptions and sys.stderr is not None:
            # Same convention as ``logging.Handler.handleError``.
            with contextlib.suppress(Exception):  # stderr itself may be unusable
                traceback.print_exc(file=sys.stderr)


def _deliver(
    loggers: dict[str, logging.Logger],
    relative_offset: float,
    native_record: tuple[int, str, str, str, int, float, str],
) -> None:
    levelno, name, message, pathname, lineno, created, thread_name = native_record
    logger = loggers.get(name)
    if logger is None:
        logger = loggers[name] = logging.getLogger(name)
    if not logger.isEnabledFor(levelno):
        return
    record = logger.makeRecord(name, levelno, pathname, lineno, message, (), None)
    record.created = created
    record.msecs = math.floor((created - math.floor(created)) * 1000.0) + 0.0
    record.relativeCreated = created * 1000.0 + relative_offset
    record.threadName = thread_name
    logger.handle(record)


_lock = threading.RLock()
_forwarder: _Forwarder | None = None


def _at_fork_before() -> None:
    _lock.acquire()


def _at_fork_parent() -> None:
    _lock.release()


def _at_fork_child() -> None:
    global _lock, _forwarder
    _lock = threading.RLock()
    _forwarder = None
    atexit.unregister(_atexit_disable)
    core = sys.modules.get("xbbg._core")
    if core is not None:
        reset = getattr(core, "_log_bridge_reset_after_fork", None)
        if reset is not None:
            reset()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_at_fork_before,
        after_in_parent=_at_fork_parent,
        after_in_child=_at_fork_child,
    )


def _validate_queue_size(queue_size: int) -> int:
    if isinstance(queue_size, bool) or not isinstance(queue_size, int):
        raise TypeError(f"queue_size must be an int, got {type(queue_size).__name__}")
    if queue_size < 1:
        raise ValueError(f"queue_size must be at least 1, got {queue_size}")
    if queue_size > _MAX_QUEUE_SIZE:
        raise ValueError(f"queue_size must be at most {_MAX_QUEUE_SIZE}, got {queue_size}")
    return queue_size


def enable_python_logging(*, queue_size: int = _DEFAULT_QUEUE_SIZE) -> None:
    """Forward xbbg's native log events to Python's ``logging`` module.

    Rust and Bloomberg SDK log lines normally go to stderr. After this call they
    are delivered to ``logging`` instead, with no duplicates on stderr, as
    records on loggers named after the Rust target (``xbbg_async.engine``,
    ``xbbg_core.session``, ...); SDK lines use ``xbbg.sdk``. Levels map to
    TRACE (5, named ``"TRACE"`` unless level 5 already has a name), DEBUG, INFO,
    WARNING and ERROR. Each record keeps its emission time, the emitting
    thread's name (``record.threadName``) and the Rust file and line
    (``record.pathname``, ``record.lineno``). Which events exist at all is still
    decided by :func:`set_log_level` (or ``RUST_LOG``); Python-side logger and
    handler levels filter afterwards.

    Delivery is automatic and best effort. Native threads never touch Python
    and never wait: events go into a bounded queue that a daemon thread
    (``xbbg-python-logging``) drains. When the queue is full, new events are
    dropped and counted; the forwarder then logs one WARNING on the
    ``xbbg.logging`` logger ("dropped N xbbg log records: queue full"), and
    :func:`python_logging_dropped` returns the running total. A burst such as
    one warning per refused security can exceed a small queue, so size it for
    the largest burst you expect.

    The forwarder's Python work competes for the GIL with your own threads, so
    slow handlers delay other Python code (but never native threads). A handler
    that raises does not stop the forwarder.

    Args:
        queue_size: Capacity of the native queue, in records (at least 1). Its
            memory (roughly 100 bytes per record) is reserved up front.

    Calling this while forwarding is already on does nothing when ``queue_size``
    matches. A different ``queue_size`` raises ``RuntimeError`` rather than
    silently resizing; call :func:`disable_python_logging` first to change it.

    Forwarding stops at interpreter exit (before native engine teardown is
    awaited) and on :func:`disable_python_logging`.

    Raises:
        TypeError: ``queue_size`` is not an int.
        ValueError: ``queue_size`` is less than 1.
        RuntimeError: Forwarding is already on with a different ``queue_size``.
    """
    queue_size = _validate_queue_size(queue_size)
    global _forwarder
    with _lock:
        current = _forwarder
        if current is not None:
            if current.queue_size != queue_size:
                raise RuntimeError(
                    f"Python logging is already enabled with queue_size={current.queue_size}; "
                    "call disable_python_logging() before changing it"
                )
            return

        # Importing _engine registers its atexit hook now, so the one registered
        # below runs first (atexit is last-in, first-out).
        from . import _core, _engine  # noqa: F401

        if logging.getLevelName(_TRACE_LEVEL) == f"Level {_TRACE_LEVEL}":
            logging.addLevelName(_TRACE_LEVEL, "TRACE")

        consumer = _core._log_bridge_open(queue_size)
        if consumer is None:
            raise RuntimeError("the native log bridge is already active")
        forwarder = _Forwarder(_core, consumer, queue_size)
        try:
            forwarder.thread.start()
        except BaseException:
            _core._log_bridge_disable()
            consumer.discard()
            raise
        _forwarder = forwarder
        atexit.register(_atexit_disable)


def disable_python_logging(*, timeout: float = 1.0) -> None:
    """Stop forwarding native log events to ``logging``; stderr output resumes.

    Capture stops immediately, then records already queued are delivered until
    ``timeout`` seconds have passed and the forwarder thread is joined with the
    same timeout. Whatever a slow or blocked handler has not taken by then is
    dropped and counted in :func:`python_logging_dropped`. Does nothing if
    forwarding is not on.

    Must not be called from a log handler running on the forwarder thread.

    Args:
        timeout: Seconds to wait for queued records to be delivered (not
            negative). ``0`` drops them immediately.

    Raises:
        ValueError: ``timeout`` is negative or not finite.
        RuntimeError: Called from the forwarder thread.
    """
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0:
        raise ValueError(f"timeout must be a finite, non-negative number of seconds, got {timeout!r}")
    global _forwarder
    with _lock:
        forwarder = _forwarder
        if forwarder is None:
            return
        if threading.current_thread() is forwarder.thread:
            raise RuntimeError("disable_python_logging() cannot be called from a log handler")
        _forwarder = None
        atexit.unregister(_atexit_disable)

        core = forwarder._core
        if timeout == 0:
            # Abandon before stopping capture so the forwarder does not deliver
            # another batch, then discard anything enqueued in the gap.
            forwarder.abandon()
            core._log_bridge_disable()
            forwarder._consumer.discard()
            return
        core._log_bridge_disable()

    # Release _lock before joining so handlers or concurrent callers cannot
    # deadlock against it.
    forwarder.thread.join(timeout)
    if forwarder.thread.is_alive():
        # Stuck or slow handler: stop it between records, drop the rest.
        forwarder.abandon()


def python_logging_dropped() -> int:
    """Return how many native log records the Python bridge has dropped so far.

    The count is for the whole process and only grows: records dropped because
    the queue was full, plus records discarded when
    :func:`disable_python_logging` timed out. It can be read at any time, even
    when no later record arrives to trigger the ``xbbg.logging`` warning, and
    is ``0`` if forwarding was never enabled.
    """
    core = sys.modules.get("xbbg._core")
    if core is None:
        return 0
    return core._log_bridge_dropped()


def _atexit_disable() -> None:
    """Stop forwarding before native teardown is awaited and the interpreter finalizes."""
    try:
        disable_python_logging()
    except Exception:
        logging.getLogger(__name__).debug("Exception stopping Python logging (ignored)", exc_info=True)
