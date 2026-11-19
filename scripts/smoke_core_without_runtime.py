#!/usr/bin/env python3
"""Smoke test of an installed xbbg on a machine with no Bloomberg runtime.

xbbg loads the Bloomberg runtime at run time, so the native extension itself must import
without one. This runs the installed package in a child interpreter whose environment holds
no way to find a runtime (no ``BLPAPI_*``, ``XBBG_*`` or library-path variables, an empty home
directory and cache, and a ``PATH`` without Bloomberg libraries) and checks that:

1. ``xbbg._core``, loaded straight from its file so no package code runs, imports, reports no
   loaded runtime and refuses ``sdk_version()`` instead of aborting;
2. ``import xbbg._core`` through the package fails with an ``ImportError`` that names the
   search order;
3. ``xbbg.get_sdk_info()`` still answers, with no active runtime.

    python scripts/smoke_core_without_runtime.py
"""

from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
import subprocess
import sys
import tempfile

_LIBRARY_PREFIXES = ("libblpapi", "blpapi3")
_STRIPPED_PREFIXES = ("BLPAPI_", "XBBG_", "DYLD_", "LD_")


def _has_bloomberg_library(directory: str) -> bool:
    try:
        return any(entry.name.lower().startswith(_LIBRARY_PREFIXES) for entry in os.scandir(directory))
    except OSError:
        return False


def clean_environment(base: Mapping[str, str], scratch: Path) -> dict[str, str]:
    """``base`` without anything that could lead the search to a Bloomberg runtime."""
    env = {key: value for key, value in base.items() if not key.upper().startswith(_STRIPPED_PREFIXES)}
    for key in ("HOME", "USERPROFILE", "LOCALAPPDATA", "XDG_CACHE_HOME", "PROGRAMFILES", "PROGRAMFILES(X86)"):
        env[key] = str(scratch / key.lower().replace("(", "_").replace(")", ""))
    env["XBBG_CACHE_DIR"] = str(scratch / "cache")
    path_key = next((key for key in env if key.upper() == "PATH"), "PATH")
    entries = env.get(path_key, "").split(os.pathsep)
    env[path_key] = os.pathsep.join(entry for entry in entries if entry and not _has_bloomberg_library(entry))
    return env


def child() -> None:
    import importlib.machinery
    import importlib.util

    import xbbg

    spec = importlib.machinery.PathFinder.find_spec("xbbg._core", list(xbbg.__path__))
    assert spec is not None and spec.loader is not None, "xbbg._core is not installed"
    core = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(core)
    assert core.loaded_sdk() is None, "a Bloomberg runtime is loaded in the scrubbed environment"
    try:
        core.sdk_version()
    except RuntimeError:
        pass
    else:
        raise AssertionError("sdk_version() answered without a loaded runtime")

    try:
        import xbbg._core
    except ImportError as error:
        message = str(error)
        for expected in ("BLPAPI_LIB_DIR", "BLPAPI_ROOT", "xbbg.set_sdk_path"):
            assert expected in message, f"the help does not mention {expected}: {message}"
    else:
        raise AssertionError("import xbbg._core succeeded without a Bloomberg runtime")

    info = xbbg.get_sdk_info()
    assert info["active"] is None and info["runtime_version"] is None, info
    assert xbbg.installed_sdks() == [], "expected empty cache in scrubbed environment"
    assert xbbg.default_sdk_cache_dir() is not None, "default_sdk_cache_dir should answer without a runtime"
    print("xbbg._core imports without a Bloomberg runtime; xbbg reports where it looked")


def main() -> int:
    if os.environ.get("XBBG_SMOKE_CHILD") == "1":
        child()
        return 0
    with tempfile.TemporaryDirectory() as scratch:
        env = clean_environment(os.environ, Path(scratch))
        env["XBBG_SMOKE_CHILD"] = "1"
        # -I: no PYTHONPATH and no script directory, so the installed package is what runs.
        return subprocess.run([sys.executable, "-I", os.path.abspath(__file__)], env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
