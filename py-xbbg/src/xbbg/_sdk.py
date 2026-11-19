"""Bloomberg runtime loading, provisioning and reporting.

``xbbg._core`` links no Bloomberg code, so it always imports. Right after it does, xbbg loads the
Bloomberg C++ SDK runtime with :func:`load_runtime`. The search order, the verification of cached
runtimes, cache provisioning and the load itself live in Rust (``xbbg-sdk`` and ``xbbg-host``) and
are shared with the Node, Java and MCP hosts; this module only passes what Python alone knows:

- the directory given to :func:`set_sdk_path`,
- an optional provisioning source given to :func:`set_sdk_source`, and
- the directory of Bloomberg's ``blpapi`` wheel, which it finds without importing the package.
"""

from __future__ import annotations

from collections.abc import Callable
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import threading
from typing import TYPE_CHECKING, Any, TypeVar

from . import _core_guard

if TYPE_CHECKING:
    from types import ModuleType

_T = TypeVar("_T")

# ``_sdk_lock`` guards ``_sdk_info``, ``_manual_sdk_path``, ``_sdk_source`` and ``_sdk_generation``.
# It is never held while probing the filesystem or importing the native module;
# the generation lets ``get_sdk_info`` drop results computed before a path change.
_sdk_lock = threading.Lock()
# Serialises the first load: two threads importing ``xbbg._core`` must not search twice.
_load_lock = threading.Lock()
_sdk_generation = 0
_sdk_info: dict | None = None
_manual_sdk_path: Path | None = None
_sdk_source: str | None = None
logger = logging.getLogger(__name__)

#: Label of the host hint that carries the directory of Bloomberg's ``blpapi`` Python package.
BLPAPI_PYTHON_HINT = "blpapi_python"

#: Bloomberg's public PyPI simple index hosting official ``blpapi`` wheels.
BLOOMBERG_PYPI_INDEX = "https://blpapi.bloomberg.com/repository/releases/python/simple/"

# ``get_sdk_info`` source names by the locator's ``SourceKind``; a hint is named by its label.
_SOURCE_NAMES = {
    "explicit": "manual",
    "blpapi_lib_dir": "sdk_lib_dir",
    "blpapi_root": "sdk_env",
    "dev_sdk_root": "dev_sdk_root",
    "vendored": "vendored",
    "terminal": "dapi",
    "search_path": "search_path",
    "system": "system",
    "cache": "xbbg_cache",
}


def _with_core_without_runtime(fn: Callable[[ModuleType], _T]) -> _T:
    """Run ``fn(core)`` without loading a Bloomberg runtime, forgetting ``_core`` if still unloaded."""
    core = _core_guard.import_without_runtime()
    try:
        return fn(core)
    finally:
        if core.loaded_sdk() is None:
            _core_guard.forget_core(core)


def _blpapi_package_init() -> Path | None:
    """Locate ``blpapi/__init__.py`` of Bloomberg's Python package without importing it.

    xbbg links the BLPAPI C++ SDK at run time and never calls this package's API. The pip wheel
    ships the shared library, which makes its directory the most common SDK source for pip users.
    Importing the package would cost ~100 ms and load its own copy of that library.
    """
    try:
        spec = importlib.util.find_spec("blpapi")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin)


def _blpapi_distribution_version() -> str | None:
    """The installed ``blpapi`` distribution version (``importlib.metadata`` costs ~80 ms)."""
    import importlib.metadata

    try:
        return importlib.metadata.version("blpapi")
    except importlib.metadata.PackageNotFoundError:
        return None


def _locate_request(explicit: Path | None = None) -> dict[str, Any]:
    """The request for ``xbbg._core.load_sdk``: only what the Rust search cannot know.

    ``explicit`` is the ``set_sdk_path`` directory (the recorded one by default). Everything else,
    the environment variables, the Terminal roots, the search path and the cache, the search reads
    itself.
    """
    if explicit is None:
        with _sdk_lock:
            explicit = _manual_sdk_path
    request: dict[str, Any] = {}
    if explicit is not None:
        request["explicit"] = str(explicit)
    blpapi_init = _blpapi_package_init()
    if blpapi_init is not None:
        request["hints"] = [{"label": BLPAPI_PYTHON_HINT, "dir": str(blpapi_init.parent)}]
    return request


def _not_found_help(error: BaseException) -> str:
    """The ``ImportError`` text when no Bloomberg runtime loads; ``error`` lists every rejection."""
    return (
        f"{error}\n\n"
        "The xbbg native extension needs the Bloomberg C++ SDK runtime library at run time "
        "(blpapi3_64.dll on Windows, libblpapi3_64.so on Linux and macOS).\n"
        "Supported platforms: Linux x64, macOS arm64/x64, Windows x64/x86\n\n"
        "xbbg looks in these places, in this order, and uses the first runtime that loads:\n"
        "  1. xbbg.set_sdk_path('/path/to/sdk') - a directory holding the runtime library\n"
        "  2. BLPAPI_LIB_DIR - an environment variable naming that directory\n"
        "  3. BLPAPI_ROOT - an environment variable naming the root of a Bloomberg C++ SDK\n"
        "  4. XBBG_DEV_SDK_ROOT - an absolute SDK root, for development checkouts\n"
        "  5. the blpapi Python package: pip install blpapi --index-url "
        f"{BLOOMBERG_PYPI_INDEX}\n"
        "  6. Bloomberg Terminal (DAPI) - automatically detected if installed\n"
        "  7. the operating system's library search path (PATH on Windows, LD_LIBRARY_PATH on Linux, "
        "DYLD_LIBRARY_PATH and DYLD_FALLBACK_LIBRARY_PATH on macOS)\n"
        "  8. the operating system's own library search: the ldconfig and default library directories "
        "on Linux, /usr/local/lib and /usr/lib on macOS, the application and system directories on "
        "Windows\n"
        "  9. the xbbg runtime cache: a Bloomberg runtime provisioned into XBBG_CACHE_DIR "
        "(default: the per-user xbbg cache directory) is used last, after its hash is verified.\n"
        "     Provision it with `xbbg-sdk --source ...` (or `python -m xbbg.sdk` / "
        "`xbbg.provision_sdk(...)`), or set XBBG_SDK_SOURCE / `xbbg.set_sdk_source(...)` "
        "to provision automatically on first load."
    )


def load_runtime(core: ModuleType) -> dict[str, Any]:
    """Load the Bloomberg runtime into this process through ``core`` (an ``xbbg._core``).

    Returns the loaded runtime (``path``, ``source``, ``detail``, ``version``, ``rejected``);
    a runtime that is already loaded is returned unchanged.

    Raises:
        ImportError: No runtime could be loaded. The message lists every rejected candidate and
            the search order.
    """
    with _load_lock:
        loaded = core.loaded_sdk()
        if loaded is not None:
            return loaded
        request = _locate_request()
        with _sdk_lock:
            sdk_source = _sdk_source if "explicit" not in request else None
        try:
            if sdk_source is not None:
                return core.load_sdk(request, sdk_source=sdk_source)
            return core.load_sdk(request)
        except ImportError as error:
            raise ImportError(_not_found_help(error), name=_core_guard.CORE_MODULE) from error


def _same_file(first: str | os.PathLike[str], second: str | os.PathLike[str]) -> bool:
    return os.path.normcase(os.path.realpath(first)) == os.path.normcase(os.path.realpath(second))


def _source_name(source: str | None, detail: str) -> str | None:
    """``get_sdk_info``'s name for a locator source; a host hint is named by its label."""
    if source is None:
        return None
    return detail if source == "hint" else _SOURCE_NAMES.get(source, source)


def _copy_source(source: dict) -> dict:
    """Return a caller-owned copy of one source entry, including its ``rejected`` list."""
    copied = dict(source)
    if "rejected" in copied:
        copied["rejected"] = [dict(entry) for entry in copied["rejected"]]
    return copied


def _copy_sdk_info(info: dict) -> dict:
    """Return a caller-owned copy of an SDK info dict."""
    return {**info, "sources": [_copy_source(source) for source in info["sources"]]}


def _provisioned_from_wire(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert a camelCase ``xbbg-sdk`` JSON result into Pythonic keys and ``Path`` values."""
    library_path = Path(raw["libraryPath"])
    install_dir = library_path.parent.parent if library_path.parent.name == "lib" else library_path.parent
    return {
        "version": raw["version"],
        "platform": raw["platform"],
        "install_dir": install_dir,
        "library_path": library_path,
        "license_paths": [Path(entry) for entry in raw.get("licensePaths", [])],
        "source": raw["source"],
        "artifact_url": raw["artifactUrl"],
        "artifact_sha256": raw["artifactSha256"],
        "lib_sha256": raw["libSha256"],
        "cache_hit": bool(raw["cacheHit"]),
    }


def _invalidate_sdk_info_cache() -> None:
    global _sdk_info, _sdk_generation
    with _sdk_lock:
        _sdk_info = None
        _sdk_generation += 1


def get_sdk_info() -> dict:
    """Report every Bloomberg runtime xbbg can find and the one it loaded.

    xbbg searches in a fixed order (see ``xbbg.set_sdk_path``) and loads the first runtime that
    works. Calling this loads it if nothing has yet.

    Returns a dict with:
        - sources: the runtimes found, in search order, one entry per source name
        - active: name of the source of the loaded runtime (``None`` if none could be loaded)
        - runtime_version: version of the loaded runtime (``None`` if none could be loaded)

    Each source entry contains:
        - name: ``"manual"`` (``set_sdk_path``), ``"sdk_lib_dir"`` (``BLPAPI_LIB_DIR``),
          ``"sdk_env"`` (``BLPAPI_ROOT``), ``"dev_sdk_root"`` (``XBBG_DEV_SDK_ROOT``),
          ``"blpapi_python"``, ``"dapi"`` (Bloomberg Terminal), ``"search_path"`` (the operating
          system's library search path), ``"system"`` (the operating system's own library search;
          listed only when its runtime is the loaded one, since only loading finds it) or
          ``"xbbg_cache"``
        - version: version string if known
        - path: Path to the runtime library (for ``"xbbg_cache"``: the verified library; ``None``
          if none verified)

    The ``"xbbg_cache"`` entry also carries ``install_dir`` (``None`` if no install verified) and
    ``rejected``: the installs skipped because they failed verification, as dicts with
    ``install_dir`` and ``reason``. It is present only when the cache holds an install.

    The returned dict is a copy; mutating it does not affect later calls.

    Example:
        >>> import xbbg
        >>> xbbg.get_sdk_info()
        {'sources': [{'name': 'blpapi_python', 'version': '3.25.11.1', ...}], 'active': 'blpapi_python', ...}
    """
    global _sdk_info

    with _sdk_lock:
        cached = _sdk_info
        generation = _sdk_generation
    if cached is not None:
        return _copy_sdk_info(cached)

    # Probes the filesystem and imports _core: never under the lock.
    info = _detect_sdk_info()

    with _sdk_lock:
        # A path change after the snapshot makes this result stale: return it
        # to this caller only and let the next call recompute.
        if generation == _sdk_generation:
            if _sdk_info is None:
                _sdk_info = info
            else:
                info = _sdk_info
    return _copy_sdk_info(info)


def _cache_entry(candidate: dict | None, rejected: list[dict]) -> dict:
    """The ``xbbg_cache`` entry of ``get_sdk_info``."""
    return {
        "name": "xbbg_cache",
        "version": candidate["version"] if candidate else None,
        "path": Path(candidate["path"]) if candidate else None,
        "install_dir": Path(candidate["detail"]) if candidate else None,
        "rejected": [{"install_dir": Path(entry["detail"]), "reason": entry["reason"]} for entry in rejected],
    }


def _detect_sdk_info() -> dict:
    """Ask the native search for every source of ``get_sdk_info``; touches no module state."""
    empty = {"sources": [], "active": None, "runtime_version": None}
    try:
        core = _core_guard.import_without_runtime()
    except ImportError:
        logger.debug("xbbg._core is unavailable; no Bloomberg runtime can be reported", exc_info=True)
        return empty

    located = core.locate_sdk(_locate_request())
    try:
        loaded = load_runtime(core)
    except ImportError:
        logger.debug("No Bloomberg runtime could be loaded", exc_info=True)
        loaded = None
        _core_guard.forget_core(core)

    sources: list[dict] = []
    seen: set[str] = set()
    cache_rejected = [entry for entry in located["rejected"] if entry["source"] == "cache"]
    for candidate in located["candidates"]:
        name = _source_name(candidate["source"], candidate["detail"])
        if name is None or name in seen:
            continue
        seen.add(name)
        if candidate["source"] == "cache":
            sources.append(_cache_entry(candidate, cache_rejected))
            continue
        sources.append({"name": name, "version": candidate["version"], "path": Path(candidate["path"])})
    if cache_rejected and "xbbg_cache" not in seen:
        sources.append(_cache_entry(None, cache_rejected))

    if loaded is None:
        return {"sources": _with_hint_versions(sources), "active": None, "runtime_version": None}

    active = _source_name(loaded["source"], loaded["detail"])
    if active is None:  # loaded outside load_runtime: match it to a source by its file
        for candidate in located["candidates"]:
            if _same_file(candidate["path"], loaded["path"]):
                active = _source_name(candidate["source"], candidate["detail"])
                break
    if active is not None:
        entry = (
            _cache_entry(loaded, cache_rejected)
            if active == "xbbg_cache"
            else {"name": active, "version": loaded["version"], "path": Path(loaded["path"])}
        )
        for source in sources:
            if source["name"] == active:
                # A source can yield several libraries, or change after the runtime loaded.
                source.update(entry)
                break
        else:
            # The operating system's own search has no candidate before it loads one.
            cache_at = next((i for i, source in enumerate(sources) if source["name"] == "xbbg_cache"), len(sources))
            sources.insert(cache_at, entry)
    return {"sources": _with_hint_versions(sources), "active": active, "runtime_version": loaded["version"]}


def _with_hint_versions(sources: list[dict]) -> list[dict]:
    """Give the ``blpapi_python`` entry the wheel's distribution version when the search has none."""
    for source in sources:
        if source["name"] == BLPAPI_PYTHON_HINT and source["version"] is None:
            source["version"] = _blpapi_distribution_version()
    return sources


def set_sdk_path(path: str | Path) -> None:
    """Manually set the Bloomberg SDK path.

    This takes precedence over every other source (``BLPAPI_LIB_DIR``, ``BLPAPI_ROOT``,
    ``XBBG_DEV_SDK_ROOT``, the blpapi Python package, Bloomberg Terminal, the library search
    path and the xbbg cache). The path should point to a directory containing the Bloomberg SDK
    shared library. The runtime is loaded immediately, and the path is authoritative: when its
    library cannot be loaded, no other source is tried in its place.

    A process loads one Bloomberg runtime for its lifetime: once it is loaded, a different path
    raises ``RuntimeError``. Call this before anything else uses xbbg.

    Args:
        path: Path to the SDK directory (e.g., "C:/blpapi_cpp_3.25.11.1" or Path object)

    Raises:
        ValueError: The path is not a directory, holds no Bloomberg library, or its library
            cannot be loaded (the message says why).
        RuntimeError: A Bloomberg runtime from another location is already loaded.
        ImportError: The xbbg native extension itself cannot be imported.

    Example:
        >>> import xbbg
        >>> xbbg.set_sdk_path("C:/custom/blpapi")
        >>> xbbg.get_sdk_info()["active"]
        'manual'
    """
    global _manual_sdk_path, _sdk_info, _sdk_generation

    sdk_path = Path(path)
    if not sdk_path.is_dir():
        raise ValueError(f"SDK path does not exist or is not a directory: {sdk_path}")

    core = _core_guard.import_without_runtime()
    located = core.locate_sdk({"explicit": str(sdk_path)})
    explicit = [entry["path"] for entry in located["candidates"] if entry["source"] == "explicit"]
    if not explicit:
        if core.loaded_sdk() is None:
            _core_guard.forget_core(core)
        raise ValueError(f"Could not find Bloomberg SDK library in: {sdk_path}")

    def already_loaded(loaded: dict) -> RuntimeError:
        return RuntimeError(
            f"The Bloomberg runtime is already loaded from {loaded['path']} and cannot be replaced "
            f"by the one in {sdk_path}. Call xbbg.set_sdk_path() before anything else uses xbbg, "
            "or restart Python."
        )

    loaded = core.loaded_sdk()
    if loaded is not None and not any(_same_file(loaded["path"], library) for library in explicit):
        raise already_loaded(loaded)

    with _sdk_lock:
        previous = _manual_sdk_path
        _manual_sdk_path = sdk_path
        _sdk_info = None  # Refresh on next get_sdk_info() call
        _sdk_generation += 1
    try:
        # An explicit path is authoritative: only its libraries are tried.
        with _load_lock:
            core.load_sdk(_locate_request(sdk_path))
    except ImportError as error:
        with _sdk_lock:
            _manual_sdk_path = previous
            _sdk_info = None
            _sdk_generation += 1
        loaded = core.loaded_sdk()
        if loaded is not None:  # another thread loaded a different runtime meanwhile
            raise already_loaded(loaded) from error
        _core_guard.forget_core(core)
        # The first line only says that nothing loaded; the rest names each library and why.
        reasons = str(error).partition("\n")[2].strip() or str(error)
        raise ValueError(f"The Bloomberg runtime in {sdk_path} could not be loaded:\n  {reasons}") from error


def clear_sdk_path() -> None:
    """Clear the manually set SDK path and revert to auto-detection.

    This only changes the search for a runtime that is not loaded yet: a loaded runtime stays.

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


def set_sdk_source(source: str | None) -> None:
    """Configure a Bloomberg SDK provisioning source for automatic cache fallback.

    When no local or cached Bloomberg runtime is found during load (and :func:`set_sdk_path`
    was not called), xbbg provisions a runtime from ``source`` into the shared xbbg cache
    (``XBBG_CACHE_DIR`` or the per-user xbbg cache directory) and loads it.

    Pass ``None`` to clear a previously configured source. Accepted specs match
    ``XBBG_SDK_SOURCE`` and the ``xbbg-sdk`` CLI:

    - ``"pypi:<url>"`` (for example ``f"pypi:{xbbg.BLOOMBERG_PYPI_INDEX}"``)
    - ``"conda:<channel-url>"``
    - ``"artifact:<url-or-path>"`` (optionally with ``#sha256=<64-hex>``)
    - a bare ``http(s)://`` URL or local archive path (auto-detected)

    Raises:
        ValueError: ``source`` is blank.
        RuntimeError: A Bloomberg runtime is already loaded in this process.
    """
    global _sdk_source, _sdk_info, _sdk_generation

    if source is not None:
        normalized = source.strip()
        if not normalized:
            raise ValueError("SDK source must not be blank")
        core = sys.modules.get(_core_guard.CORE_MODULE)
        if core is not None:
            loaded_sdk = getattr(core, "loaded_sdk", None)
            loaded = loaded_sdk() if callable(loaded_sdk) else None
            if loaded is not None:
                raise RuntimeError(
                    f"The Bloomberg runtime is already loaded from {loaded['path']} and cannot be "
                    "replaced by provisioning. Call xbbg.set_sdk_source() before anything else "
                    "uses xbbg, or restart Python."
                )
    else:
        normalized = None

    with _sdk_lock:
        _sdk_source = normalized
        _sdk_info = None
        _sdk_generation += 1


def get_sdk_source() -> str | None:
    """Return the Bloomberg SDK provisioning source set with :func:`set_sdk_source`, if any."""
    with _sdk_lock:
        return _sdk_source


def default_sdk_cache_dir() -> Path | None:
    """Return the default shared xbbg cache root (``XBBG_CACHE_DIR`` or user cache directory).

    Works before any Bloomberg runtime is installed or loaded.
    """
    raw = _with_core_without_runtime(lambda core: core.default_sdk_cache_dir())
    return Path(raw) if raw is not None else None


def installed_sdks(
    cache_dir: str | os.PathLike[str] | None = None,
    *,
    platform: str | None = None,
) -> list[dict[str, Any]]:
    """List verified Bloomberg SDK runtimes installed in the shared xbbg cache.

    Works before any Bloomberg runtime is installed or loaded.

    Args:
        cache_dir: Optional cache root overriding ``XBBG_CACHE_DIR`` / default cache directory.
        platform: Optional platform filter (for example ``"linux-x86_64"`` or ``"macos-aarch64"``).

    Returns:
        List of dicts with ``version``, ``platform``, ``install_dir``, ``library_path``,
        ``license_paths``, ``source``, ``artifact_url``, ``artifact_sha256``, ``lib_sha256``
        and ``cache_hit``.
    """
    cache_arg = os.fspath(cache_dir) if cache_dir is not None else None
    raw = _with_core_without_runtime(lambda core: core.installed_sdks(cache_arg, platform))
    return [_provisioned_from_wire(entry) for entry in raw]


def provision_sdk(
    source: str | dict[str, Any] | None = None,
    *,
    version: str | None = None,
    platform: str | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
    offline: bool = False,
    proxy: str | dict[str, Any] | None = None,
    credentials: list[dict[str, Any]] | None = None,
    netrc: str | os.PathLike[str] | None = None,
    extra_ca_pem: str | os.PathLike[str] | None = None,
    connect_timeout_ms: int | None = None,
    read_timeout_ms: int | None = None,
    retries: int | None = None,
    require_artifact_hash: bool | None = None,
) -> dict[str, Any]:
    """Provision a Bloomberg C++ SDK runtime into the shared xbbg cache.

    This runs before any Bloomberg runtime is installed or loaded, using the shared Rust
    provisioner (``xbbg-sdk``) and the same ``<cache>/sdk/<version>/<platform>/`` layout used
    by Python, Node/JS, Java and ``xbbg-mcp``.

    Args:
        source: Source specification string (``"pypi:<url>"``, ``"conda:<url>"``,
            ``"artifact:<url-or-path>"``, or bare URL/path) or structured source dict.
            When ``None``, falls back to :func:`get_sdk_source` and then ``XBBG_SDK_SOURCE``.
        version: Exact SDK version (for example ``"3.26.5.1"``) or ``"latest"``.
            When ``None``, falls back to ``XBBG_SDK_VERSION`` or latest.
        platform: Target platform (defaults to the current host platform).
        cache_dir: Cache root (defaults to ``XBBG_CACHE_DIR`` or the per-user xbbg cache).
        offline: Only use already-cached installations without network access.
        proxy: Optional proxy URL string or proxy config dict.
        credentials: Optional list of per-host credential dicts
            (``[{"host": "...", "username": "...", "password": "..."}]`` or
            ``[{"host": "...", "token": "..."}]``).
        netrc: Optional path to a ``.netrc`` file.
        extra_ca_pem: Optional path to an extra CA certificate bundle in PEM format.
        connect_timeout_ms: HTTP connect timeout in milliseconds.
        read_timeout_ms: HTTP read timeout in milliseconds.
        retries: Retry count for transient HTTP failures.
        require_artifact_hash: Require a SHA-256 hash when fetching direct artifacts.

    Returns:
        Dict with ``version``, ``platform``, ``install_dir``, ``library_path``,
        ``license_paths``, ``source``, ``artifact_url``, ``artifact_sha256``, ``lib_sha256``
        and ``cache_hit``.
    """
    resolved_source: str | dict[str, Any] | None = source
    if resolved_source is None:
        resolved_source = get_sdk_source() or (os.environ.get("XBBG_SDK_SOURCE") or "").strip() or None
    if resolved_source is None:
        raise ValueError("source is required (pass source=..., call xbbg.set_sdk_source(...), or set XBBG_SDK_SOURCE)")

    payload: dict[str, Any] = {"source": resolved_source}
    resolved_version = version if version is not None else ((os.environ.get("XBBG_SDK_VERSION") or "").strip() or None)
    if resolved_version is not None:
        payload["version"] = resolved_version
    if platform is not None:
        payload["platform"] = platform
    if cache_dir is not None:
        payload["cacheDir"] = os.fspath(cache_dir)
    if offline:
        payload["offline"] = True
    if proxy is not None:
        payload["proxy"] = proxy
    if credentials is not None:
        payload["credentials"] = credentials
    if netrc is not None:
        payload["netrc"] = os.fspath(netrc)
    if extra_ca_pem is not None:
        payload["extraCaPem"] = os.fspath(extra_ca_pem)
    if connect_timeout_ms is not None:
        payload["connectTimeoutMillis"] = connect_timeout_ms
    if read_timeout_ms is not None:
        payload["readTimeoutMillis"] = read_timeout_ms
    if retries is not None:
        payload["retries"] = retries
    if require_artifact_hash is not None:
        payload["requireArtifactHash"] = require_artifact_hash

    encoded = json.dumps(payload)
    raw = _with_core_without_runtime(lambda core: core.provision_sdk(encoded))
    _invalidate_sdk_info_cache()
    return _provisioned_from_wire(raw)
