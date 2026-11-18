"""SDK detection and management utilities for Bloomberg libraries."""

from __future__ import annotations

import importlib
import logging
from pathlib import Path
import threading

# ``_sdk_lock`` guards ``_sdk_info``, ``_manual_sdk_path`` and ``_sdk_generation``.
# It is never held while probing the filesystem or importing the native module;
# the generation lets ``get_sdk_info`` drop results computed before a path change.
_sdk_lock = threading.Lock()
_sdk_generation = 0
_sdk_info: dict | None = None
_manual_sdk_path: Path | None = None
_dll_directory_handles: list[object] = []
_preloaded_libraries: list[object] = []
logger = logging.getLogger(__name__)


def _get_lib_version(_lib_path: Path) -> str | None:
    """Get the version of the linked Bloomberg C SDK at runtime."""
    try:
        from . import _core

        major, minor, patch, build = _core.sdk_version()
        return f"{major}.{minor}.{patch}.{build}"
    except Exception:
        logger.debug("Could not determine Bloomberg SDK runtime version from %s", _lib_path, exc_info=True)
        return None


def _find_sdk_lib(sdk_path: Path) -> Path | None:
    """Find the blpapi DLL/SO in an SDK directory.

    Bloomberg ships `libblpapi3_64.so` (not `.dylib`) on macOS as well as
    Linux, so the non-Windows candidate list is the same for both.
    """
    import sys

    if sys.platform == "win32":
        candidates = [
            "blpapi3_64.dll",
            "blpapi3_32.dll",
            "bin/blpapi3_64.dll",
            "bin/blpapi3_32.dll",
            "lib/blpapi3_64.dll",
            "lib/blpapi3_32.dll",
            "Lib/blpapi3_64.dll",
            "Lib/blpapi3_32.dll",
            "lib/win64/blpapi3_64.dll",
            "lib/win32/blpapi3_32.dll",
        ]
    else:  # macOS and Linux
        candidates = ["libblpapi3_64.so", "libblpapi3.so", "lib/libblpapi3_64.so", "lib/libblpapi3.so"]

    for candidate in candidates:
        full_path = sdk_path / candidate
        if full_path.is_file():
            return full_path
    return None


def _dapi_candidate_paths() -> list[Path]:
    """Return likely Bloomberg Terminal/DAPI runtime directories in priority order."""
    import os
    import sys

    candidates: list[Path] = []

    def add(candidate: Path | None) -> None:
        if candidate is None:
            return
        key = str(candidate).casefold() if sys.platform == "win32" else str(candidate)
        if any(
            (str(existing).casefold() if sys.platform == "win32" else str(existing)) == key for existing in candidates
        ):
            return
        candidates.append(candidate)

    if sys.platform == "win32":
        system_drive = os.environ.get("SYSTEMDRIVE", "C:")
        if len(system_drive) < 2 or system_drive[1] != ":":
            system_drive = "C:"
        system_drive = system_drive[:2]

        add(Path(f"{system_drive}\\blp\\DAPI"))
        if system_drive.casefold() != "c:":
            add(Path("C:\\blp\\DAPI"))

        for root in (
            os.environ.get("PROGRAMFILES"),
            f"{system_drive}\\Program Files",
            os.environ.get("PROGRAMFILES(X86)"),
            f"{system_drive}\\Program Files (x86)",
        ):
            if root:
                add(Path(root) / "Bloomberg" / "Blp" / "DAPI")

        local_app_data = os.environ.get("LOCALAPPDATA")
        if not local_app_data and os.environ.get("USERPROFILE"):
            local_app_data = str(Path(os.environ["USERPROFILE"]) / "AppData" / "Local")
        if local_app_data:
            add(Path(local_app_data) / "Bloomberg" / "DAPI")
            add(Path(local_app_data) / "Bloomberg" / "Blp" / "DAPI")
    else:
        add(Path.home() / "blp" / "DAPI")
        add(Path("/opt/bloomberg/DAPI"))

    return candidates


def _blpapi_package_init() -> Path | None:
    """Locate ``blpapi/__init__.py`` of Bloomberg's Python package without importing it.

    xbbg links the BLPAPI C++ SDK directly and never calls this package's API.
    The pip wheel ships the shared library, which makes its directory the most
    common SDK source for pip users -- probed ahead of a Terminal DAPI install.
    Importing the package would cost ~100 ms and load its own copy of that library.
    """
    import importlib.util

    try:
        spec = importlib.util.find_spec("blpapi")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin)


def _blpapi_package_location() -> tuple[Path, str | None] | None:
    """Return ``(path to blpapi/__init__.py, distribution version or None)`` for ``get_sdk_info``.

    The version comes from ``importlib.metadata`` (~80 ms to import), so SDK
    discovery on the import path uses :func:`_blpapi_package_init` instead.
    Returns ``None`` when the package is not installed.
    """
    import importlib.metadata

    package_init = _blpapi_package_init()
    if package_init is None:
        return None
    try:
        package_version: str | None = importlib.metadata.version("blpapi")
    except importlib.metadata.PackageNotFoundError:
        package_version = None
    return package_init, package_version


def _scan_xbbg_cache() -> dict | None:
    """Return the xbbg cache scan (``runtimes``, ``rejected``), or ``None`` when it cannot be read.

    ``xbbg._runtime`` is a small native module that links no Bloomberg code: it hashes each
    cached library against its provenance record, best version first. A checkout without the
    built module (pure Python) has no cache rule.
    """
    try:
        runtime = importlib.import_module("._runtime", __package__)
    except ImportError:
        return None
    try:
        return runtime.scan_cache()
    except Exception:
        logger.debug("Could not scan the xbbg runtime cache", exc_info=True)
        return None


def cache_rejection_notes() -> list[str]:
    """Describe each xbbg cache install that was skipped, as ``<install dir>: <reason>``."""
    scan = _scan_xbbg_cache()
    if scan is None:
        return []
    return [f"{entry['install_dir']}: {entry['reason']}" for entry in scan["rejected"]]


def _xbbg_cache_source() -> dict | None:
    """The ``xbbg_cache`` entry of ``get_sdk_info``, or ``None`` when the cache holds nothing."""
    scan = _scan_xbbg_cache()
    if scan is None or not (scan["runtimes"] or scan["rejected"]):
        return None
    best = scan["runtimes"][0] if scan["runtimes"] else None
    return {
        "name": "xbbg_cache",
        "version": best["version"] if best else None,
        "path": Path(best["lib_path"]) if best else None,
        "install_dir": Path(best["install_dir"]) if best else None,
        "rejected": [
            {"install_dir": Path(entry["install_dir"]), "reason": entry["reason"]} for entry in scan["rejected"]
        ],
    }


def _copy_source(source: dict) -> dict:
    """Return a caller-owned copy of one source entry, including its ``rejected`` list."""
    copied = dict(source)
    if "rejected" in copied:
        copied["rejected"] = [dict(entry) for entry in copied["rejected"]]
    return copied


def _copy_sdk_info(info: dict) -> dict:
    """Return a caller-owned copy of an SDK info dict."""
    return {**info, "sources": [_copy_source(source) for source in info["sources"]]}


def get_sdk_info() -> dict:
    """Detect all available Bloomberg SDK sources and versions.

    Returns a dict with:
        - sources: list of all detected SDK sources
        - active: the source that will be used (first available; an ``xbbg_cache`` entry
          that holds only rejected installs is never active)

    Each source entry contains:
        - name: "manual", "blpapi_python", "dapi", "sdk_env", or "xbbg_cache"
        - version: version string if detectable
        - path: Path to the SDK (for "xbbg_cache": the verified library; ``None`` if none verified)

    The "xbbg_cache" entry, last in the list, also carries ``install_dir`` (``None`` if no
    install verified) and ``rejected``: the installs skipped because they failed verification,
    as dicts with ``install_dir`` and ``reason``. It is present only when the cache holds an
    install.

    The returned dict is a copy; mutating it does not affect later calls.

    Example:
        >>> import xbbg
        >>> xbbg.get_sdk_info()
        {'sources': [{'name': 'blpapi_python', 'version': '3.25.11.1', ...}], 'active': 'blpapi_python'}
    """
    global _sdk_info

    with _sdk_lock:
        cached = _sdk_info
        generation = _sdk_generation
        manual_sdk_path = _manual_sdk_path
    if cached is not None:
        return _copy_sdk_info(cached)

    # Probes the filesystem and imports _core: never under the lock.
    info = _detect_sdk_info(manual_sdk_path)

    with _sdk_lock:
        # A path change after the snapshot makes this result stale: return it
        # to this caller only and let the next call recompute.
        if generation == _sdk_generation:
            if _sdk_info is None:
                _sdk_info = info
            else:
                info = _sdk_info
    return _copy_sdk_info(info)


def _detect_sdk_info(manual_sdk_path: Path | None) -> dict:
    """Probe every SDK source for ``get_sdk_info``; touches no module state."""
    import os

    sources: list[dict] = []

    # Check 0: Manually set SDK path (highest priority)
    if manual_sdk_path is not None:
        manual_version = None
        lib_path = _find_sdk_lib(manual_sdk_path)
        if lib_path:
            manual_version = _get_lib_version(lib_path)
        sources.append(
            {
                "name": "manual",
                "version": manual_version,
                "path": manual_sdk_path,
            }
        )

    # Check 1: blpapi Python package (most common for pip users)
    blpapi_location = _blpapi_package_location()
    if blpapi_location is not None:
        blpapi_init, blpapi_version = blpapi_location
        sources.append(
            {
                "name": "blpapi_python",
                "version": blpapi_version,
                "path": blpapi_init,
            }
        )

    # Check 2: DAPI (Bloomberg Terminal installation)
    first_existing_dapi_path = None
    first_existing_dapi_version = None
    for dapi_path in _dapi_candidate_paths():
        try:
            if not dapi_path.is_dir():
                continue
        except (OSError, PermissionError):
            continue
        lib_path = _find_sdk_lib(dapi_path)
        if lib_path:
            first_existing_dapi_path = dapi_path
            first_existing_dapi_version = _get_lib_version(lib_path)
            break
        if first_existing_dapi_path is None:
            first_existing_dapi_path = dapi_path

    if first_existing_dapi_path is not None:
        sources.append(
            {
                "name": "dapi",
                "version": first_existing_dapi_version,
                "path": first_existing_dapi_path,
            }
        )

    # Check 3: BLPAPI_ROOT environment variable
    blpapi_root = os.environ.get("BLPAPI_ROOT")
    if blpapi_root:
        sdk_path = Path(blpapi_root)
        if sdk_path.is_dir():
            sdk_version = None
            lib_path = _find_sdk_lib(sdk_path)
            if lib_path:
                sdk_version = _get_lib_version(lib_path)
            sources.append(
                {
                    "name": "sdk_env",
                    "version": sdk_version,
                    "path": sdk_path,
                }
            )

    # Check 4: the xbbg cache (always last)
    cache_source = _xbbg_cache_source()
    if cache_source is not None:
        sources.append(cache_source)

    runtime_version = None
    try:
        from . import _core

        major, minor, patch, build = _core.sdk_version()
        runtime_version = f"{major}.{minor}.{patch}.{build}"
    except Exception:
        logger.debug("Could not determine Bloomberg SDK runtime version", exc_info=True)

    return {
        "sources": sources,
        "active": next(
            (source["name"] for source in sources if source["name"] != "xbbg_cache" or source["path"] is not None),
            None,
        ),
        "runtime_version": runtime_version,
    }


def set_sdk_path(path: str | Path) -> None:
    """Manually set the Bloomberg SDK path.

    This takes precedence over all auto-detected sources (blpapi_python, dapi, sdk_env, xbbg_cache).
    The path should point to a directory containing the Bloomberg SDK shared library.

    Args:
        path: Path to the SDK directory (e.g., "C:/blpapi_cpp_3.25.11.1" or Path object)

    Example:
        >>> import xbbg
        >>> xbbg.set_sdk_path("C:/custom/blpapi")
        >>> xbbg.get_sdk_info()["active"]
        'manual'
    """
    from pathlib import Path as PathClass

    global _manual_sdk_path, _sdk_info, _sdk_generation

    sdk_path = PathClass(path) if isinstance(path, str) else path
    if not sdk_path.is_dir():
        raise ValueError(f"SDK path does not exist or is not a directory: {sdk_path}")

    lib_path = _find_sdk_lib(sdk_path)
    if not lib_path:
        raise ValueError(f"Could not find Bloomberg SDK library in: {sdk_path}")

    with _sdk_lock:
        _manual_sdk_path = sdk_path
        _sdk_info = None  # Refresh on next get_sdk_info() call
        _sdk_generation += 1
    _prepare_sdk_for_core_import()


def clear_sdk_path() -> None:
    """Clear the manually set SDK path and revert to auto-detection.

    Example:
        >>> import xbbg
        >>> xbbg.set_sdk_path("C:/custom/blpapi")
        >>> xbbg.clear_sdk_path()  # Back to auto-detection
    """
    global _manual_sdk_path, _sdk_info, _sdk_generation
    with _sdk_lock:
        _manual_sdk_path = None
        _sdk_info = None  # Refresh on next get_sdk_info() call
        _sdk_generation += 1


def _collect_sdk_candidate_dirs() -> list[Path]:
    """Walk all SDK sources in priority order and return existing directories.

    Priority: manual path → blpapi Python package → DAPI → BLPAPI_ROOT. The xbbg cache
    comes after these, but it names one exact library rather than a directory, so
    :func:`_preload_cached_library` handles it.
    Duplicates (by resolved path) are removed.
    """
    import os

    seen: set[str] = set()
    result: list[Path] = []

    def add(candidate: Path | None) -> None:
        if candidate is None:
            return
        try:
            if not candidate.is_dir():
                return
            key = str(candidate.resolve())
        except (OSError, PermissionError):
            return
        if key in seen:
            return
        seen.add(key)
        result.append(candidate)

    # 1. Manual SDK path (highest priority)
    with _sdk_lock:
        manual_sdk_path = _manual_sdk_path
    add(manual_sdk_path)

    # 2. blpapi Python package (most common for pip users)
    blpapi_init = _blpapi_package_init()
    add(blpapi_init.parent if blpapi_init is not None else None)

    # 3. DAPI (Bloomberg Terminal)
    for dapi_path in _dapi_candidate_paths():
        add(dapi_path)

    # 4. BLPAPI_ROOT environment variable
    blpapi_root = os.environ.get("BLPAPI_ROOT")
    if blpapi_root:
        add(Path(blpapi_root))

    return result


def _add_sdk_to_dll_search_path() -> None:
    """Windows: add each detected SDK library directory to the DLL search path.

    Must be called before importing the native extension (_core). Errors are
    swallowed so permission issues or missing directories don't block import.
    """
    import os

    for sdk_dir in _collect_sdk_candidate_dirs():
        lib_path = _find_sdk_lib(sdk_dir)
        if lib_path is None:
            continue
        try:
            handle = os.add_dll_directory(str(lib_path.parent))  # type: ignore[unresolved-attribute]
            _dll_directory_handles.append(handle)
        except (OSError, PermissionError, ValueError):
            continue


def _preload_sdk_library() -> bool:
    """Load the highest-priority SDK library by full path before _core needs it.

    _core names its BLPAPI dependency without a path, and the loader satisfies
    that name from an already-loaded library of the same name.

    - Windows: the search order across directories added with
      ``os.add_dll_directory`` is unspecified, so without a preload _core may bind
      a Terminal DAPI copy instead of the higher-priority pip wheel's, and a later
      ``import blpapi`` would then load a second, different copy into the process.
    - macOS: the pyo3 cdylib declares `@rpath/libblpapi3_64.so` but ships with no
      LC_RPATH entries. Once any image with that LC_ID_DYLIB is loaded, dyld
      satisfies later references by install-name match, the same pattern
      Bloomberg's own `blpapi/internals.py::_loadLibrary` relies on.
    - Linux: RTLD_GLOBAL makes the symbols available to subsequently loaded
      modules, avoiding a second DT_NEEDED search (which may fail if
      `site-packages/blpapi` isn't on `LD_LIBRARY_PATH`).

    Falls through to the xbbg cache (:func:`_preload_cached_library`) when no other
    source loads. Returns True on first successful preload, False if nothing could be
    loaded. Failure is non-fatal — `_import_core` still surfaces the
    friendly error message if the C extension can't open the library.
    """
    for sdk_dir in _collect_sdk_candidate_dirs():
        lib_path = _find_sdk_lib(sdk_dir)
        if lib_path is None:
            continue
        try:
            _load_library(lib_path)
            return True
        except OSError:
            continue
    return _preload_cached_library()


def _load_library(lib_path: Path) -> None:
    """Load ``lib_path`` into the process by full path and keep the handle."""
    import ctypes
    import sys

    if sys.platform == "win32":
        _preloaded_libraries.append(ctypes.WinDLL(str(lib_path)))
    else:
        _preloaded_libraries.append(ctypes.CDLL(str(lib_path), mode=ctypes.RTLD_GLOBAL))


def _preload_cached_library() -> bool:
    """Last rule: load the best verified runtime of the xbbg cache by its exact library path.

    The cache scan hashes the library, so it runs only when no earlier source loaded. The
    scan names the library (a provisioned install may keep it in a subdirectory), so no
    directory search is involved. On Windows the library's directory joins the DLL search path.
    """
    import os
    import sys

    scan = _scan_xbbg_cache()
    if scan is None or not scan["runtimes"]:
        return False
    lib_path = Path(scan["runtimes"][0]["lib_path"])
    try:
        if sys.platform == "win32":
            _dll_directory_handles.append(os.add_dll_directory(str(lib_path.parent)))  # type: ignore[unresolved-attribute]
        _load_library(lib_path)
    except (OSError, ValueError):
        logger.debug("Could not load the cached Bloomberg runtime %s", lib_path, exc_info=True)
        return False
    return True


def _prepare_sdk_for_core_import() -> None:
    """Prepare the current process to load xbbg._core on any platform.

    The import guard in ``_core_guard`` calls this immediately before the native
    extension loads, so ``set_sdk_path()`` takes effect if it runs first.

    - Windows: add each detected SDK directory to the DLL search path, then
      preload the highest-priority library by full path.
    - macOS/Linux: preload libblpapi so _core's `@rpath/libblpapi3_64.so`
      dependency resolves via dyld's install-name match (macOS) or is
      already in the process's loaded image list (Linux).
      When none of those loads, the xbbg cache supplies the last candidate.

    All errors are swallowed; `_import_core()` surfaces the friendly error
    message if no SDK can be found at the point the native extension is
    actually imported.
    """
    import sys

    try:
        if sys.platform == "win32":
            _add_sdk_to_dll_search_path()
        _preload_sdk_library()
    except Exception:
        logger.debug("Failed to prepare Bloomberg SDK for native extension import", exc_info=True)
