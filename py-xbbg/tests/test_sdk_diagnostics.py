from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from unittest import TestCase
import warnings

_CASE = TestCase()


def test_import_xbbg_is_warning_free():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _CASE.assertIsNotNone(importlib.import_module("xbbg"))

    _CASE.assertEqual(recorded, [])


def test_blpapi_package_is_located_without_running_it(tmp_path):
    """SDK discovery reads blpapi's location and metadata; the package's own code never runs.

    ``import xbbg`` and the native-extension import also skip ``importlib.metadata``,
    which costs ~80 ms; only ``get_sdk_info()`` reads blpapi's distribution version.
    """
    import xbbg

    site = tmp_path / "site"
    package = site / "blpapi"
    package.mkdir(parents=True)
    package.joinpath("__init__.py").write_text(
        "import builtins\n"
        "builtins.STAND_IN_BLPAPI_EXECUTED = True\n"
        "raise RuntimeError('stand-in blpapi must not be executed')\n",
        encoding="utf-8",
    )
    # Not a loadable runtime: the search lists it, and the real runtime of the environment
    # (found earlier in the order) loads instead.
    library = package / ("blpapi3_64.dll" if sys.platform == "win32" else "libblpapi3_64.so")
    library.write_bytes(b"stand-in")
    dist_info = site / "blpapi-9.9.9.dist-info"
    dist_info.mkdir()
    dist_info.joinpath("METADATA").write_text(
        "Metadata-Version: 2.1\nName: blpapi\nVersion: 9.9.9\n",
        encoding="utf-8",
    )
    script = textwrap.dedent(
        """
        import builtins
        import json
        import sys

        import xbbg
        import xbbg._core

        metadata_before_info = "importlib.metadata" in sys.modules
        info = xbbg.get_sdk_info()
        print(json.dumps({
            "metadata_before_info": metadata_before_info,
            "executed": getattr(builtins, "STAND_IN_BLPAPI_EXECUTED", False),
            "imported": "blpapi" in sys.modules,
            "sources": [
                {"version": source["version"], "path": str(source["path"])}
                for source in info["sources"]
                if source["name"] == "blpapi_python"
            ],
        }))
        """
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(site), str(Path(xbbg.__file__).resolve().parents[1])]),
    }
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env, timeout=120, check=False
    )

    _CASE.assertEqual(result.returncode, 0, result.stdout + result.stderr)
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    _CASE.assertFalse(payload["executed"])
    _CASE.assertFalse(payload["imported"])
    _CASE.assertFalse(payload["metadata_before_info"])
    _CASE.assertEqual(len(payload["sources"]), 1, payload)
    _CASE.assertEqual(payload["sources"][0]["version"], "9.9.9")
    _CASE.assertEqual(Path(payload["sources"][0]["path"]).resolve(), library.resolve())
