# Free-threaded Python

xbbg publishes wheels for free-threaded CPython ([PEP 703](https://peps.python.org/pep-0703/)) 3.14 and 3.15 (`cp314t`, `cp315t`) on Linux x86_64 (manylinux_2_28), Windows x64 and macOS arm64. Support is beta: CI runs the offline test suite on these interpreters, once as usual and once with every test body running in 8 threads at the same time, but those jobs do not gate merges yet.

## Supported versions and optional dependencies

| Component | On free-threaded CPython |
|---|---|
| CPython 3.14t, 3.15t | Supported |
| CPython 3.13t | Not supported: PyO3 0.29, which builds the native extension, only builds free-threaded extensions for 3.14 and later |
| `pyarrow`, `pandas` | Supported; both publish free-threaded wheels |
| `narwhals` | Supported (pure Python) |
| `polars`, `duckdb` | Not supported: neither publishes free-threaded wheels on PyPI |
| `xbbg-langgraph` | Not supported: its dependency `langsmith` requires `zstandard` and `orjson`, which publish no free-threaded wheels |

Install only the supported extras:

```bash
pip install "xbbg[pyarrow,pandas]"
```

The `test` extra pulls in polars and duckdb, so it cannot install from wheels on free-threaded Python.

xbbg's native result objects (`ArrowTable`, `ArrowRecordBatch`, `ArrowColumn`) are immutable, so threads can share and read them.

## Process-wide state

Some settings apply to every thread at once: `configure()`, `shutdown()` and `reset()`, request middleware (`add_middleware()`, `remove_middleware()`, `set_middleware()`, `clear_middleware()`), the default backend (`set_backend()`) and exchange overrides (`set_exchange_override()`). Change them from one thread, ideally at startup, not while other threads are sending requests.

Middleware runs on the thread and event loop that sends each request. When several threads each run their own event loop, middleware must not hold an object tied to one loop, such as an `asyncio.Event` or a future: a request from another thread would wait on it from the wrong loop and never wake. Keep per-request state in local variables, and share state between threads only through thread-safe objects such as `threading.Lock`.

## Check that the GIL stays disabled

A free-threaded interpreter turns the GIL back on when it imports an extension module that has not declared that it runs safely without it, and emits a `RuntimeWarning` that names the module. xbbg's native extension declares free-threading support. To check your own environment, import xbbg and every optional dependency you use, with that warning turned into an error:

```bash
python -W error::RuntimeWarning -c "import sys, xbbg, pyarrow, pandas; xbbg.get_log_level(); xbbg.get_sdk_info(); assert not sys._is_gil_enabled()"
```

Do not set `PYTHON_GIL=0` or pass `-X gil=0` to make this check pass. Both keep the GIL off even for modules that never declared they are safe without it, which hides the module instead of fixing it.

## Contributors

`pixi run -e py314t install` builds the extension for free-threaded CPython 3.14, and `pixi run -e py314t test` runs the offline suite there. That environment carries only pyarrow and pandas, so polars and duckdb tests skip. On a free-threaded interpreter the suite fails if any import re-enabled the GIL during the run.

`pixi run -e py314t test-parallel` runs the offline suite under [pytest-run-parallel](https://github.com/Quansight-Labs/pytest-run-parallel), which runs each test body in 8 threads at the same time. Async tests run in one thread automatically, because pytest-asyncio drives them on one event loop. Mark a test that changes process-wide state with `@pytest.mark.thread_unsafe(reason=...)` so it runs in one thread too. In fixtures, prefer `unittest.mock.patch` to the `monkeypatch` fixture: pytest-run-parallel runs every test that uses `monkeypatch` in one thread.

`py-xbbg/tests/test_free_threading_stress.py` drives the native subscription and request paths from many threads at once. Like the other native driver tests, it needs an extension built with the `test-support` feature and skips otherwise.

The ThreadSanitizer workflow (`.github/workflows/thread-sanitizer.yml`) runs nightly and on demand (`gh workflow run thread-sanitizer.yml`). It runs the `xbbg-async` and `xbbg-arrow` tests with Rust's thread sanitizer, then builds a free-threaded CPython with ThreadSanitizer (`scripts/build-tsan-cpython.sh`), installs the instrumented extension into it, and runs the free-threading, native driver and stress tests. `.github/tsan-suppressions.txt` covers only uninstrumented third-party code, each entry with the report that justified it. A report in xbbg's own code is a bug to fix, never to suppress.
