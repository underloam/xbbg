from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import threading
from unittest import TestCase
import warnings

_CASE = TestCase()


def _isolate_sdk_state(monkeypatch) -> None:
    """Start from an empty SDK cache with no discoverable sources."""
    from xbbg import _sdk

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_blpapi_package_location", lambda: None)
    monkeypatch.setattr(_sdk, "_dapi_candidate_paths", list)
    monkeypatch.delenv("BLPAPI_ROOT", raising=False)


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
    _CASE.assertEqual(Path(payload["sources"][0]["path"]).resolve(), (package / "__init__.py").resolve())


def test_sdk_preload_binds_the_first_loadable_library_by_full_path(monkeypatch, tmp_path):
    """Windows search order across added DLL directories is unspecified, so the preload decides which SDK _core binds."""
    import ctypes

    from xbbg import _sdk

    broken_dir, wheel_dir, dapi_dir = (tmp_path / name for name in ("broken", "wheel", "dapi"))
    libraries = {path: path / "blpapi3_64.dll" for path in (broken_dir, wheel_dir, dapi_dir)}
    loaded = []

    def load(path):
        if path == str(libraries[broken_dir]):
            raise OSError("not a valid Win32 application")
        loaded.append(path)
        return f"handle:{path}"

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(_sdk, "_collect_sdk_candidate_dirs", lambda: [broken_dir, wheel_dir, dapi_dir])
    monkeypatch.setattr(_sdk, "_find_sdk_lib", libraries.get)
    monkeypatch.setattr(_sdk, "_preloaded_libraries", [])
    monkeypatch.setattr(ctypes, "WinDLL", load, raising=False)

    _CASE.assertTrue(_sdk._preload_sdk_library())
    _CASE.assertEqual(loaded, [str(libraries[wheel_dir])])
    _CASE.assertEqual(_sdk._preloaded_libraries, [f"handle:{libraries[wheel_dir]}"])


def test_get_lib_version_failure_is_debug_observable(monkeypatch, caplog):
    import xbbg
    from xbbg import _sdk

    def raise_sdk_version():
        raise RuntimeError("boom")

    monkeypatch.setattr(xbbg, "_core", type("FakeCore", (), {"sdk_version": raise_sdk_version})(), raising=False)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        _CASE.assertIsNone(_sdk._get_lib_version(Path("C:/missing/blpapi3_64.dll")))

    _CASE.assertTrue(
        any("Could not determine Bloomberg SDK runtime version" in record.message for record in caplog.records)
    )
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def test_get_sdk_info_runtime_failure_is_debug_observable(monkeypatch, caplog):
    import xbbg
    from xbbg import _sdk

    def raise_sdk_version():
        raise RuntimeError("boom")

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(xbbg, "_core", type("FakeCore", (), {"sdk_version": raise_sdk_version})(), raising=False)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        info = _sdk.get_sdk_info()

    _CASE.assertIn("runtime_version", info)
    _CASE.assertTrue(
        any("Could not determine Bloomberg SDK runtime version" in record.message for record in caplog.records)
    )
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def test_prepare_sdk_failure_is_debug_observable(monkeypatch, caplog):
    from xbbg import _sdk

    def raise_prepare():
        raise RuntimeError("boom")

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(_sdk, "_add_sdk_to_dll_search_path", raise_prepare)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        _sdk._prepare_sdk_for_core_import()

    _CASE.assertTrue(any("Failed to prepare Bloomberg SDK" in record.message for record in caplog.records))
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def _manual_sdk(monkeypatch, tmp_path) -> Path:
    """Create a manual SDK directory that ``set_sdk_path`` accepts without loading anything."""
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    sdk_dir.mkdir()
    lib_path = sdk_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")
    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == sdk_dir else None)
    monkeypatch.setattr(_sdk, "_get_lib_version", lambda _lib_path: "3.0.0.0")
    return sdk_dir


def test_set_sdk_path_prepares_manual_sdk(monkeypatch, tmp_path):
    from xbbg import _sdk

    _isolate_sdk_state(monkeypatch)
    sdk_dir = _manual_sdk(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setattr(_sdk, "_prepare_sdk_for_core_import", lambda: calls.append("prepare"))

    _CASE.assertIsNone(_sdk.get_sdk_info()["active"])
    _sdk.set_sdk_path(sdk_dir)

    info = _sdk.get_sdk_info()
    _CASE.assertEqual(info["active"], "manual")
    _CASE.assertEqual(info["sources"][0]["path"], sdk_dir)
    _CASE.assertEqual(calls, ["prepare"])


def test_get_sdk_info_returns_caller_owned_copies(monkeypatch, tmp_path):
    from xbbg import _sdk

    _isolate_sdk_state(monkeypatch)
    sdk_dir = _manual_sdk(monkeypatch, tmp_path)
    monkeypatch.setattr(_sdk, "_prepare_sdk_for_core_import", lambda: None)
    _sdk.set_sdk_path(sdk_dir)

    first = _sdk.get_sdk_info()
    expected = {**first, "sources": [dict(source) for source in first["sources"]]}
    first["sources"][0]["name"] = "mutated"
    first["sources"].append({"name": "extra"})
    first["active"] = "mutated"

    _CASE.assertEqual(_sdk.get_sdk_info(), expected)


def test_sdk_info_computed_before_clear_sdk_path_is_not_published(monkeypatch, tmp_path):
    """A detection that started before ``clear_sdk_path`` must not repopulate the cache."""
    from xbbg import _sdk

    _isolate_sdk_state(monkeypatch)
    sdk_dir = _manual_sdk(monkeypatch, tmp_path)
    monkeypatch.setattr(_sdk, "_prepare_sdk_for_core_import", lambda: None)
    _sdk.set_sdk_path(sdk_dir)

    probing = threading.Event()
    release = threading.Event()
    probes = []

    def dapi_candidates() -> list[Path]:
        probes.append(threading.current_thread().name)
        if len(probes) == 1:
            probing.set()
            if not release.wait(30):
                raise TimeoutError("detection was never released")
        return []

    monkeypatch.setattr(_sdk, "_dapi_candidate_paths", dapi_candidates)
    stale: dict = {}
    detector = threading.Thread(target=lambda: stale.update(_sdk.get_sdk_info()), name="detector")
    detector.start()
    try:
        _CASE.assertTrue(probing.wait(30))
        _sdk.clear_sdk_path()
    finally:
        release.set()
        detector.join(30)

    _CASE.assertFalse(detector.is_alive())
    # The in-flight caller gets the answer for the state it started from...
    _CASE.assertEqual(stale["active"], "manual")
    # ...but the next call re-detects instead of reading that stale answer.
    fresh = _sdk.get_sdk_info()
    _CASE.assertIsNone(fresh["active"])
    _CASE.assertEqual(fresh["sources"], [])
    _CASE.assertEqual(probes, ["detector", threading.current_thread().name])


def test_find_sdk_lib_checks_windows_bin_dir(monkeypatch, tmp_path):
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    bin_dir = sdk_dir / "bin"
    bin_dir.mkdir(parents=True)
    lib_path = bin_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")

    monkeypatch.setattr(sys, "platform", "win32")

    _CASE.assertEqual(_sdk._find_sdk_lib(sdk_dir), lib_path)


def test_dapi_candidate_paths_include_windows_terminal_roots(monkeypatch):
    from xbbg import _sdk

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("SYSTEMDRIVE", "C:")
    monkeypatch.setenv("PROGRAMFILES", r"C:\Program Files")
    monkeypatch.setenv("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\analyst\AppData\Local")

    paths = _sdk._dapi_candidate_paths()

    _CASE.assertIn(Path(r"C:\blp\DAPI"), paths)
    _CASE.assertIn(Path(r"C:\Program Files (x86)") / "Bloomberg" / "Blp" / "DAPI", paths)
    _CASE.assertIn(Path(r"C:\Program Files") / "Bloomberg" / "Blp" / "DAPI", paths)


def test_get_sdk_info_prefers_dapi_path_with_runtime(monkeypatch, tmp_path):
    from xbbg import _sdk

    empty_dapi = tmp_path / "empty-dapi"
    empty_dapi.mkdir()
    program_files_dapi = tmp_path / "Program Files (x86)" / "Bloomberg" / "Blp" / "DAPI"
    program_files_dapi.mkdir(parents=True)
    lib_path = program_files_dapi / "blpapi3_64.dll"
    lib_path.write_text("placeholder")

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_dapi_candidate_paths", lambda: [empty_dapi, program_files_dapi])
    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == program_files_dapi else None)
    monkeypatch.setattr(_sdk, "_get_lib_version", lambda _lib_path: "3.0.0.0")
    monkeypatch.delenv("BLPAPI_ROOT", raising=False)

    info = _sdk.get_sdk_info()
    dapi_sources = [source for source in info["sources"] if source["name"] == "dapi"]

    _CASE.assertEqual(len(dapi_sources), 1)
    _CASE.assertEqual(dapi_sources[0]["path"], program_files_dapi)
    _CASE.assertEqual(dapi_sources[0]["version"], "3.0.0.0")


def test_windows_dll_directory_handles_are_retained(monkeypatch, tmp_path):
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    sdk_dir.mkdir()
    lib_path = sdk_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")
    import os

    handles = []

    monkeypatch.setattr(_sdk, "_collect_sdk_candidate_dirs", lambda: [sdk_dir])
    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == sdk_dir else None)
    monkeypatch.setattr(_sdk, "_dll_directory_handles", [])
    monkeypatch.setattr(os, "add_dll_directory", lambda path: handles.append(path) or f"handle:{path}", raising=False)

    _sdk._add_sdk_to_dll_search_path()

    _CASE.assertEqual(handles, [str(sdk_dir)])
    _CASE.assertEqual(_sdk._dll_directory_handles, [f"handle:{sdk_dir}"])
