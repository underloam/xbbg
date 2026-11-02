# Free-threaded Python

xbbg publishes wheels for free-threaded CPython ([PEP 703](https://peps.python.org/pep-0703/)) 3.14 and 3.15 (`cp314t`, `cp315t`) on Linux x86_64 (manylinux_2_28), Windows x64 and macOS arm64. Support is beta: CI runs the offline test suite on these interpreters, but those jobs do not gate merges yet.

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

## Check that the GIL stays disabled

A free-threaded interpreter turns the GIL back on when it imports an extension module that has not declared that it runs safely without it, and emits a `RuntimeWarning` that names the module. xbbg's native extension declares free-threading support. To check your own environment, import xbbg and every optional dependency you use, with that warning turned into an error:

```bash
python -W error::RuntimeWarning -c "import sys, xbbg, pyarrow, pandas; xbbg.get_log_level(); xbbg.get_sdk_info(); assert not sys._is_gil_enabled()"
```

Do not set `PYTHON_GIL=0` or pass `-X gil=0` to make this check pass. Both keep the GIL off even for modules that never declared they are safe without it, which hides the module instead of fixing it.

## Contributors

`pixi run -e py314t install` builds the extension for free-threaded CPython 3.14, and `pixi run -e py314t test` runs the offline suite there. That environment carries only pyarrow and pandas, so polars and duckdb tests skip. On a free-threaded interpreter the suite fails if any import re-enabled the GIL during the run.
