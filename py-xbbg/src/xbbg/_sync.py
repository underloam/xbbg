"""Synchronous calls on ordinary and notebook event loops.

The managed bridge owns accepted calls through cancellation and shutdown. Both
request wrappers and streaming producers share this loop; this module depends
only on the standard library and never imports the public facade.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import concurrent.futures
import contextvars
from dataclasses import dataclass
import functools
import inspect
import logging
import sys
import threading
from typing import Any

logger = logging.getLogger(__name__)


def _is_notebook_context() -> bool:
    """Return True when running in a supported notebook runtime."""
    marimo = sys.modules.get("marimo")
    if marimo is not None:
        running_in_notebook = getattr(marimo, "running_in_notebook", None)
        if callable(running_in_notebook) and running_in_notebook():
            return True

    try:
        from IPython import get_ipython
    except Exception:
        return False

    shell = get_ipython()
    if shell is None:
        return False

    shell_module = shell.__class__.__module__
    if shell_module.startswith("ipykernel."):
        return True

    config = getattr(shell, "config", None)
    return bool(config is not None and "IPKernelApp" in config)


@dataclass(eq=False)
class _ManagedAsyncCall:
    result: concurrent.futures.Future[Any]
    task: asyncio.Task[tuple[bool, Any]] | None = None
    cancel_requested: bool = False


class _ManagedAsyncBridge:
    """Own one background event loop and every coroutine accepted by it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._calls: set[_ManagedAsyncCall] = set()

    def _ensure_loop_locked(self) -> asyncio.AbstractEventLoop:
        if self._stopping:
            raise RuntimeError("xbbg coroutine bridge is stopping")
        if (
            self._loop is not None
            and not self._loop.is_closed()
            and self._loop.is_running()
            and self._thread is not None
            and self._thread.is_alive()
        ):
            return self._loop
        if self._loop is not None or self._thread is not None:
            raise RuntimeError("xbbg coroutine bridge loop is unavailable")

        ready = threading.Event()
        loop_holder: dict[str, asyncio.AbstractEventLoop] = {}
        error_holder: list[BaseException] = []

        def run_loop() -> None:
            loop: asyncio.AbstractEventLoop | None = None
            failure: BaseException | None = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop_holder["loop"] = loop
                loop.call_soon(ready.set)
                loop.run_forever()
            except BaseException as error:
                failure = error
                error_holder.append(error)
                ready.set()
            finally:
                if loop is not None and not loop.is_closed():
                    try:
                        pending = asyncio.all_tasks(loop)
                        for task in pending:
                            task.cancel()
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                        loop.run_until_complete(loop.shutdown_asyncgens())
                    except BaseException as error:
                        if failure is None:
                            failure = error
                        logger.error("Exception stopping xbbg coroutine bridge loop", exc_info=True)
                    finally:
                        loop.close()
                self._loop_exited(loop, threading.current_thread(), failure)

        thread = threading.Thread(
            target=run_loop,
            name="xbbg-notebook-sync-bridge",
            daemon=True,
        )
        thread.start()
        ready.wait()

        if error_holder or "loop" not in loop_holder or not thread.is_alive():
            cause = error_holder[0] if error_holder else None
            raise RuntimeError("Failed to start xbbg coroutine bridge") from cause

        self._loop = loop_holder["loop"]
        self._thread = thread
        return self._loop

    def _loop_exited(
        self,
        loop: asyncio.AbstractEventLoop | None,
        thread: threading.Thread,
        failure: BaseException | None,
    ) -> None:
        with self._lock:
            if self._loop is not loop or self._thread is not thread:
                return
            calls = tuple(self._calls)
            self._calls.clear()
            self._loop = None
            self._thread = None
            self._stopping = False

        for call in calls:
            if not call.result.done():
                error = RuntimeError("xbbg coroutine bridge stopped before call completed")
                if failure is not None:
                    error.__cause__ = failure
                call.result.set_exception(error)

    def _complete_call(
        self,
        call: _ManagedAsyncCall,
        succeeded: bool,
        value: Any,
    ) -> None:
        with self._lock:
            if call not in self._calls:
                return
            self._calls.remove(call)

        if call.result.done():
            return
        try:
            if succeeded:
                call.result.set_result(value)
            else:
                call.result.set_exception(value)
        except concurrent.futures.InvalidStateError:
            pass

    def _schedule_call(
        self,
        call: _ManagedAsyncCall,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if call.result.done():
            return

        async def invoke() -> tuple[bool, Any]:
            try:
                return True, await async_func(*args, **kwargs)
            except BaseException as error:
                return False, error

        try:
            task = asyncio.create_task(invoke())
        except BaseException as error:
            self._complete_call(call, False, error)
            return

        with self._lock:
            if call not in self._calls:
                task.cancel()
                return
            call.task = task
            cancel_requested = call.cancel_requested

        def complete(completed: asyncio.Task[tuple[bool, Any]]) -> None:
            try:
                succeeded, value = completed.result()
            except BaseException as error:
                succeeded, value = False, error
            self._complete_call(call, succeeded, value)

        task.add_done_callback(complete)
        if cancel_requested:
            task.cancel()

    def start(
        self,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> _ManagedAsyncCall:
        caller_context = contextvars.copy_context()
        call = _ManagedAsyncCall(concurrent.futures.Future())

        with self._lock:
            if self._thread is threading.current_thread():
                raise RuntimeError("xbbg coroutine bridge cannot be re-entered from its own event-loop thread")
            loop = self._ensure_loop_locked()
            self._calls.add(call)
            try:
                loop.call_soon_threadsafe(
                    self._schedule_call,
                    call,
                    async_func,
                    args,
                    kwargs,
                    context=caller_context,
                )
            except BaseException:
                self._calls.remove(call)
                raise

        return call

    def cancel(self, call: _ManagedAsyncCall) -> None:
        with self._lock:
            if call not in self._calls:
                return
            call.cancel_requested = True
            task = call.task
            loop = self._loop

        if task is not None and loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass

    def submit(
        self,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        call = self.start(async_func, args, kwargs)
        try:
            return call.result.result()
        except BaseException:
            if not call.result.done():
                self.cancel(call)
            raise

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
            if loop is None or thread is None:
                return
            initiate_stop = not self._stopping
            if initiate_stop:
                self._stopping = True
                calls = tuple(self._calls)
                self._calls.clear()
            else:
                calls = ()

        if initiate_stop:
            for call in calls:
                call.result.cancel()
            tasks = tuple(call.task for call in calls if call.task is not None)

            def cancel_and_stop() -> None:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                loop.stop()

            if loop.is_running():
                try:
                    loop.call_soon_threadsafe(cancel_and_stop)
                except RuntimeError:
                    logger.error("Failed to schedule xbbg coroutine bridge shutdown", exc_info=True)

        if thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if thread.is_alive():
                logger.error("xbbg coroutine bridge did not stop within 1 second")


_notebook_sync_bridge = _ManagedAsyncBridge()


def _stop_notebook_sync_loop() -> None:
    _notebook_sync_bridge.close()


def _run_in_notebook_sync_bridge(
    async_func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    return _notebook_sync_bridge.submit(async_func, args, kwargs)


def _run_sync(
    sync_name: str,
    async_func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """Run ``async_func`` to completion for a synchronous public API.

    Without a running loop this is ``asyncio.run``. Notebook loops (IPykernel,
    marimo) block on the managed background loop. Any other running loop is
    rejected before the coroutine is created.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(async_func(*args, **kwargs))

    if _is_notebook_context():
        return _run_in_notebook_sync_bridge(async_func, args, kwargs)

    raise RuntimeError(
        f"{sync_name}() cannot be used inside an async context. "
        f"Use 'await a{sync_name}()' instead, "
        f"or use xbbg.Engine(...) for scoped async engines."
    )


def _build_sync_wrapper(
    sync_name: str,
    async_func: Callable[..., Any],
) -> Callable[..., Any]:
    @functools.wraps(async_func)
    def wrapped(*args, **kwargs):
        return _run_sync(sync_name, async_func, args, kwargs)

    wrapped.__name__ = sync_name
    wrapped.__qualname__ = sync_name
    wrapped.__module__ = "xbbg.blp"
    wrapped.__signature__ = inspect.signature(async_func)  # type: ignore[unresolved-attribute]
    return wrapped
