"""Free-threaded CPython keeps the GIL disabled and threads can share native objects.

Free-threaded runs must not set ``PYTHON_GIL`` or pass ``-X gil=0``: both keep the
GIL off even when an extension module that never declared free-threading support
is imported, which would hide that module.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import textwrap
import threading

import pytest

import xbbg
from xbbg import _core

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None
THREADS = 16
ROUNDS = 25


@pytest.mark.skipif(not FREE_THREADED_BUILD, reason="requires a free-threaded CPython build")
def test_import_sdk_discovery_and_backend_selection_leave_the_gil_disabled():
    script = textwrap.dedent(
        """
        import importlib
        import importlib.util
        import sys

        import xbbg
        from xbbg import backend

        xbbg.get_log_level()
        xbbg.get_sdk_info()
        backend._best_narwhals_backend()
        for name in ("pyarrow", "pandas"):
            if importlib.util.find_spec(name) is not None:
                importlib.import_module(name)
        assert not sys._is_gil_enabled(), "an imported extension module re-enabled the GIL"
        """
    )
    env = {name: value for name, value in os.environ.items() if name != "PYTHON_GIL"}
    env["PYTHONPATH"] = str(Path(xbbg.__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def _read_everything(table):
    columns = [table.column(name).to_pylist() for name in table.column_names]
    pyarrow_rows = table.to_pyarrow().to_pylist() if HAS_PYARROW else None
    return table.to_pylist(), columns, pyarrow_rows


def test_threads_sharing_one_arrow_table_read_the_single_thread_result():
    table = _core.ArrowTable.from_pylist(
        [
            {
                "ticker": f"SYNTH{index:04d} Equity",
                "date": date(2026, 1, 1) + timedelta(days=index % 28),
                "px_last": None if index % 7 == 0 else index * 0.25,
                "volume": index,
            }
            for index in range(512)
        ]
    )
    expected = _read_everything(table)
    start = threading.Barrier(THREADS)

    def read_concurrently(_worker):
        start.wait(timeout=60)
        return [_read_everything(table) for _ in range(ROUNDS)]

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        observed = [reads for rounds in pool.map(read_concurrently, range(THREADS)) for reads in rounds]

    assert len(observed) == THREADS * ROUNDS
    assert sum(reads != expected for reads in observed) == 0
