"""xbbg - independent client for Bloomberg-connected data workflows.

This package provides a high-level API for authorized Bloomberg environments,
powered by a high-performance Rust backend.
"""

from __future__ import annotations

import importlib
import threading
from typing import TYPE_CHECKING

from . import _core_guard
from ._exports import (
    BACKEND_EXPORTS,
    CORE_EXPORTS,
    EXCEPTION_EXPORTS,
    FIELD_CACHE_EXPORTS,
    MODULE_EXPORTS,
    PACKAGE_BLP_EXPORTS,
    PACKAGE_EXPORTS,
    SCHEMA_LOOKUP_EXPORTS,
    SDK_EXPORTS,
    SERVICE_EXPORTS,
)

# Version from git tags via setuptools-scm, written into _version.py at build
# time. Reading it avoids importlib.metadata, which costs ~80 ms per import.
try:
    from ._version import version as _distribution_version
except ImportError:  # sources that were never built or installed
    _distribution_version = None
__version__ = _distribution_version or "0+unknown"

# Every import of the native extension, including `from xbbg._core import X`
# (which bypasses __getattr__), checks that it was built for this xbbg and
# first prepares the Bloomberg SDK library it links against.
_core_guard.install(_distribution_version)

# Lazy import of the Rust module to avoid import errors when it's not built
if TYPE_CHECKING:
    from . import _core

# Same-thread re-entry detection only. Other threads importing concurrently wait
# on importlib's per-module lock instead of tripping the recursion check.
_core_import_state = threading.local()
# Published only after a successful import: importlib inserts the module into
# sys.modules before its exec (and the build guard) has finished.
_core_module = None

__all__ = list(PACKAGE_EXPORTS)


def _import_core():
    """Import and cache the native extension with a friendlier DLL error."""
    global _core_module
    core = _core_module
    if core is not None:
        return core
    if getattr(_core_import_state, "active", False):
        raise ImportError("Recursive import of _core detected")

    _core_import_state.active = True
    try:
        core = importlib.import_module("xbbg._core")
    except ImportError as e:
        msg = str(e)
        if (
            "DLL load failed" in msg  # Windows
            or "cannot open shared object" in msg  # Linux
            or "Library not loaded" in msg  # macOS dlopen
            or "image not found" in msg  # older macOS dlopen
        ):
            raise ImportError(
                f"{e}\n\n"
                "The xbbg native extension requires the Bloomberg C++ SDK shared library.\n"
                "Supported platforms: Linux x64, macOS arm64/x64, Windows x64/x86\n\n"
                "You can provide the SDK from any of these sources:\n"
                "  1. blpapi Python package: pip install blpapi --index-url "
                "https://blpapi.bloomberg.com/repository/releases/python/simple/\n"
                "  2. Bloomberg Terminal (DAPI) - automatically detected if installed\n"
                "  3. Bloomberg C++ SDK: set BLPAPI_ROOT environment variable\n"
                "  4. xbbg.set_sdk_path('/path/to/sdk') - manually set SDK path"
            ) from e
        raise
    finally:
        _core_import_state.active = False
    _core_module = core
    return core


def _build_lazy_attr_exports() -> dict[str, tuple[str, str]]:
    exports = {
        "EngineConfig": ("_core", "PyEngineConfig"),
        "Engine": ("blp", "Engine"),
    }
    module_groups = (
        ("_sdk", SDK_EXPORTS),
        ("_core", CORE_EXPORTS),
        ("blp", PACKAGE_BLP_EXPORTS),
        ("backend", BACKEND_EXPORTS),
        ("field_cache", FIELD_CACHE_EXPORTS),
        ("services", SERVICE_EXPORTS),
        ("schema", SCHEMA_LOOKUP_EXPORTS),
        ("exceptions", EXCEPTION_EXPORTS),
    )

    for module_name, names in module_groups:
        for name in names:
            exports[name] = (module_name, name)

    return exports


_LAZY_ATTR_EXPORTS = _build_lazy_attr_exports()


def __getattr__(name: str):
    """Lazy attribute access for deferred imports."""
    if name == "_core":
        return _import_core()
    if name in MODULE_EXPORTS:
        return importlib.import_module(f"xbbg.{name}")

    target = _LAZY_ATTR_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = target
    module = _import_core() if module_name == "_core" else importlib.import_module(f"xbbg.{module_name}")
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    """Expose public attributes for tab completion."""
    return list(__all__)
