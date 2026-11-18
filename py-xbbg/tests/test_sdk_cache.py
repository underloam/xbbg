"""The xbbg cache rule of SDK discovery: last in the search order, verified before it is loaded.

Offline: each test writes its own cache (``provenance.json`` plus a dummy library made of synthetic
bytes) under a temp directory named by ``XBBG_CACHE_DIR``, and never loads a real library.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import sys
import types

import pytest

pytestmark = pytest.mark.thread_unsafe(
    reason="monkeypatches xbbg._sdk state, sys.modules, os.environ and the loader hooks"
)

LIBRARY_BYTES = b"synthetic stand-in for a Bloomberg runtime"


def _host_platform() -> str:
    machine = platform.machine().lower()
    x86_64 = machine in {"x86_64", "amd64"}
    if sys.platform == "win32" and x86_64:
        return "windows-x86_64"
    if sys.platform.startswith("linux") and x86_64:
        return "linux-x86_64"
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return "macos-aarch64"
    pytest.skip("the xbbg cache has no platform for this host")


def _library_name() -> str:
    return "blpapi3_64.dll" if sys.platform == "win32" else "libblpapi3_64.so"


@pytest.fixture
def runtime_module():
    return pytest.importorskip("xbbg._runtime")


@pytest.fixture
def sdk(monkeypatch):
    """``xbbg._sdk`` with no discoverable source and an empty cache directory."""
    from xbbg import _sdk

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_blpapi_package_location", lambda: None)
    monkeypatch.setattr(_sdk, "_blpapi_package_init", lambda: None)
    monkeypatch.setattr(_sdk, "_dapi_candidate_paths", list)
    monkeypatch.setattr(_sdk, "_get_lib_version", lambda _lib_path: "9.9.9.9")
    monkeypatch.setattr(_sdk, "_preloaded_libraries", [])
    monkeypatch.delenv("BLPAPI_ROOT", raising=False)
    return _sdk


@pytest.fixture
def cache_root(monkeypatch, tmp_path) -> Path:
    root = tmp_path / "xbbg-cache"
    monkeypatch.setenv("XBBG_CACHE_DIR", str(root))
    return root


def _install(root: Path, version: str, *, name: str = "install", subdir: str = "lib", content: bytes = LIBRARY_BYTES):
    """Write one install as the provisioner would; returns ``(install_dir, library)``."""
    install_dir = root / "sdk" / version / _host_platform() / name
    library = install_dir / subdir / _library_name()
    library.parent.mkdir(parents=True)
    library.write_bytes(content)
    provenance = {
        "version": version,
        "platform": _host_platform(),
        "source": "artifact:synthetic",
        "artifactUrl": "file:synthetic",
        "artifactSha256": hashlib.sha256(b"archive").hexdigest(),
        "lib": f"{subdir}/{_library_name()}",
        "libSha256": hashlib.sha256(LIBRARY_BYTES).hexdigest(),
        "licenses": [],
    }
    (install_dir / "provenance.json").write_text(json.dumps(provenance), encoding="utf-8")
    return install_dir, library


def _tamper(library: Path) -> None:
    data = bytearray(library.read_bytes())
    data[0] ^= 0xFF
    library.write_bytes(bytes(data))


def _sdk_dir(root: Path, name: str) -> Path:
    directory = root / name
    directory.mkdir()
    (directory / _library_name()).write_bytes(b"stand-in")
    return directory


def test_runtime_module_reports_the_shared_cache_root(runtime_module, cache_root):
    assert runtime_module.default_cache_root() == str(cache_root)


def test_runtime_module_scan_shape(runtime_module, cache_root):
    install_dir, library = _install(cache_root, "3.26.1.1")

    scan = runtime_module.scan_cache()

    assert scan["rejected"] == []
    [runtime] = scan["runtimes"]
    assert runtime == {
        "version": "3.26.1.1",
        "platform": _host_platform(),
        "lib_path": str(library),
        "lib_sha256": hashlib.sha256(LIBRARY_BYTES).hexdigest(),
        "source": "artifact:synthetic",
        "install_dir": str(install_dir),
    }
    assert runtime_module.scan_cache(str(cache_root), _host_platform()) == scan
    assert runtime_module.scan_cache(str(cache_root / "missing")) == {"runtimes": [], "rejected": []}
    with pytest.raises(ValueError, match="unsupported platform"):
        runtime_module.scan_cache(platform="plan9-mips")


def test_cache_runtime_is_the_last_source_and_the_last_to_load(sdk, runtime_module, cache_root, monkeypatch, tmp_path):
    _, cached_library = _install(cache_root, "3.26.1.1")
    manual, wheel, dapi, env = (_sdk_dir(tmp_path, name) for name in ("manual", "wheel", "dapi", "env"))
    monkeypatch.setattr(sdk, "_manual_sdk_path", manual)
    monkeypatch.setattr(sdk, "_blpapi_package_init", lambda: wheel / "__init__.py")
    monkeypatch.setattr(sdk, "_blpapi_package_location", lambda: (wheel / "__init__.py", "3.25.0"))
    monkeypatch.setattr(sdk, "_dapi_candidate_paths", lambda: [dapi])
    monkeypatch.setenv("BLPAPI_ROOT", str(env))

    info = sdk.get_sdk_info()

    assert [source["name"] for source in info["sources"]] == [
        "manual",
        "blpapi_python",
        "dapi",
        "sdk_env",
        "xbbg_cache",
    ]
    assert info["active"] == "manual"
    assert [source["path"] for source in info["sources"][:4]] == [manual, wheel / "__init__.py", dapi, env]
    cache_source = info["sources"][-1]
    assert cache_source["path"] == cached_library
    assert cache_source["version"] == "3.26.1.1"
    assert cache_source["install_dir"] == cached_library.parent.parent
    assert cache_source["rejected"] == []

    # Every earlier source fails to load: the cache library is tried last, by its exact path
    # (it sits in a subdirectory no directory search would look into).
    attempts: list[Path] = []

    def refuse_all_but_the_cache(lib_path: Path) -> None:
        attempts.append(lib_path)
        if lib_path != cached_library:
            raise OSError("cannot load")

    monkeypatch.setattr(sdk, "_load_library", refuse_all_but_the_cache)

    assert sdk._preload_sdk_library() is True
    assert attempts == [
        manual / _library_name(),
        wheel / _library_name(),
        dapi / _library_name(),
        env / _library_name(),
        cached_library,
    ]


def test_cache_is_not_scanned_while_an_earlier_source_loads(sdk, runtime_module, cache_root, monkeypatch, tmp_path):
    _install(cache_root, "3.26.1.1")
    manual = _sdk_dir(tmp_path, "manual")
    monkeypatch.setattr(sdk, "_manual_sdk_path", manual)
    loaded: list[Path] = []
    monkeypatch.setattr(sdk, "_load_library", loaded.append)
    monkeypatch.setattr(sdk, "_scan_xbbg_cache", lambda: pytest.fail("the cache is the last resort"))

    assert sdk._preload_sdk_library() is True
    assert loaded == [manual / _library_name()]


def test_tampered_cache_entry_is_skipped_and_listed_as_rejected(sdk, runtime_module, cache_root, monkeypatch):
    bad_dir, bad_library = _install(cache_root, "3.26.2.1", name="tampered")
    good_dir, good_library = _install(cache_root, "3.26.1.1", name="intact")
    _tamper(bad_library)
    attempts: list[Path] = []
    monkeypatch.setattr(sdk, "_load_library", attempts.append)

    info = sdk.get_sdk_info()

    [cache_source] = [source for source in info["sources"] if source["name"] == "xbbg_cache"]
    # The newer install is the tampered one: the older, intact one is chosen instead.
    assert cache_source["path"] == good_library
    assert cache_source["install_dir"] == good_dir
    [rejection] = cache_source["rejected"]
    assert rejection["install_dir"] == bad_dir
    assert "hash mismatch" in rejection["reason"]
    assert sdk._preload_sdk_library() is True
    assert attempts == [good_library]


def test_only_tampered_entry_means_nothing_to_load_and_the_reason_is_reported(
    sdk, runtime_module, cache_root, monkeypatch
):
    install_dir, library = _install(cache_root, "3.26.1.1")
    _tamper(library)
    attempts: list[Path] = []
    monkeypatch.setattr(sdk, "_load_library", attempts.append)

    info = sdk.get_sdk_info()

    assert attempts == []
    assert sdk._preload_sdk_library() is False
    assert attempts == []
    # The rejected install is shown, but it is not an SDK that will be used.
    assert info["active"] is None
    [cache_source] = info["sources"]
    assert cache_source["path"] is None
    assert cache_source["version"] is None
    assert cache_source["install_dir"] is None
    assert [entry["install_dir"] for entry in cache_source["rejected"]] == [install_dir]
    notes = sdk.cache_rejection_notes()
    assert len(notes) == 1
    assert notes[0].startswith(str(install_dir)) and "hash mismatch" in notes[0]


def test_missing_native_module_skips_the_rule_silently(sdk, cache_root, monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "xbbg._runtime", None)
    monkeypatch.setattr(sdk, "_load_library", lambda _path: pytest.fail("nothing should load"))

    info = sdk.get_sdk_info()

    assert info["sources"] == []
    assert info["active"] is None
    assert sdk._preload_sdk_library() is False
    assert sdk.cache_rejection_notes() == []
    assert caplog.records == []


def test_failing_scan_is_debug_observable_not_fatal(sdk, monkeypatch, caplog):
    def broken_scan():
        raise OSError("listing the cache failed")

    monkeypatch.setitem(sys.modules, "xbbg._runtime", types.SimpleNamespace(scan_cache=broken_scan))

    import logging

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        assert sdk.get_sdk_info()["sources"] == []
        assert sdk._preload_sdk_library() is False

    assert any("Could not scan the xbbg runtime cache" in record.message for record in caplog.records)


def _fake_scan(install_dir: Path, library: Path) -> dict:
    return {
        "runtimes": [
            {
                "version": "3.26.1.1",
                "platform": "windows-x86_64",
                "lib_path": str(library),
                "lib_sha256": "0" * 64,
                "source": "artifact:synthetic",
                "install_dir": str(install_dir),
            }
        ],
        "rejected": [],
    }


def test_windows_adds_the_cache_directory_and_loads_the_exact_library(sdk, monkeypatch, tmp_path):
    import ctypes

    install_dir = tmp_path / "install"
    library = install_dir / "bin" / "blpapi3_64.dll"
    monkeypatch.setitem(
        sys.modules, "xbbg._runtime", types.SimpleNamespace(scan_cache=lambda: _fake_scan(install_dir, library))
    )
    added: list[str] = []
    loaded: list[str] = []
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(sdk, "_dll_directory_handles", [])
    monkeypatch.setattr(os, "add_dll_directory", lambda path: added.append(path) or f"handle:{path}", raising=False)
    monkeypatch.setattr(ctypes, "WinDLL", lambda path: loaded.append(path) or f"dll:{path}", raising=False)

    assert sdk._preload_sdk_library() is True

    assert added == [str(library.parent)]
    assert loaded == [str(library)]
    assert sdk._dll_directory_handles == [f"handle:{library.parent}"]
    assert sdk._preloaded_libraries == [f"dll:{library}"]


def test_cache_library_that_will_not_load_leaves_discovery_unsuccessful(sdk, monkeypatch, tmp_path):
    install_dir = tmp_path / "install"
    library = install_dir / "lib" / "libblpapi3_64.so"
    monkeypatch.setitem(
        sys.modules, "xbbg._runtime", types.SimpleNamespace(scan_cache=lambda: _fake_scan(install_dir, library))
    )

    def refuse(_path: Path) -> None:
        raise OSError("not a valid image")

    monkeypatch.setattr(sdk, "_load_library", refuse)

    assert sdk._preload_sdk_library() is False


def test_sdk_info_copies_own_their_rejected_lists(sdk, runtime_module, cache_root):
    _, library = _install(cache_root, "3.26.1.1")
    _tamper(library)

    first = sdk.get_sdk_info()
    first["sources"][-1]["rejected"][0]["reason"] = "mutated"
    first["sources"][-1]["rejected"].append({"install_dir": Path("x"), "reason": "extra"})

    [entry] = sdk.get_sdk_info()["sources"][-1]["rejected"]
    assert "hash mismatch" in entry["reason"]


def test_missing_sdk_message_names_the_cache_and_any_rejection(runtime_module, cache_root, monkeypatch):
    import xbbg

    install_dir, library = _install(cache_root, "3.26.1.1")
    _tamper(library)
    real_import = importlib.import_module

    def import_module(name, package=None):
        if name == "xbbg._core":
            raise ImportError("Library not loaded: @rpath/libblpapi3_64.so")
        return real_import(name, package)

    monkeypatch.setattr(xbbg, "_core_module", None)
    monkeypatch.setattr(importlib, "import_module", import_module)

    with pytest.raises(ImportError) as raised:
        xbbg._import_core()

    message = str(raised.value)
    assert "Library not loaded" in message
    assert "BLPAPI_ROOT" in message
    assert "xbbg runtime cache" in message
    assert "XBBG_CACHE_DIR" in message
    assert "These xbbg cache installs were skipped" in message
    assert str(install_dir) in message
    assert "hash mismatch" in message


def test_missing_sdk_message_without_rejections_has_no_skipped_list(runtime_module, cache_root, monkeypatch):
    import xbbg

    real_import = importlib.import_module

    def import_module(name, package=None):
        if name == "xbbg._core":
            raise ImportError("cannot open shared object file")
        return real_import(name, package)

    monkeypatch.setattr(xbbg, "_core_module", None)
    monkeypatch.setattr(importlib, "import_module", import_module)

    with pytest.raises(ImportError, match="xbbg runtime cache") as raised:
        xbbg._import_core()

    assert "were skipped" not in str(raised.value)
