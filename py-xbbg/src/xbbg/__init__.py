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
    PYTHON_LOGGING_EXPORTS,
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


def _load_runtime(core: ModuleType) -> None:
    # Imported on first use, so a plain `import xbbg` stays cheap; the guard itself never
    # imports _sdk, which imports the guard.
    from . import _sdk

    _sdk.load_runtime(core)


# Every import of the native extension, including `from xbbg._core import X`
# (which bypasses __getattr__), checks that it was built for this xbbg and
# then loads the Bloomberg SDK runtime into the process (see _sdk.load_runtime).
_core_guard.install(_distribution_version, load_runtime=_load_runtime)

# Lazy import of the Rust module to avoid import errors when it's not built
if TYPE_CHECKING:
    from types import ModuleType

    from . import _core

# Same-thread re-entry detection only. Other threads importing concurrently wait
# on importlib's per-module lock instead of tripping the recursion check.
_core_import_state = threading.local()
# Published only after a successful import: importlib inserts the module into
# sys.modules before its exec (and the build guard) has finished.
_core_module = None

__all__ = list(PACKAGE_EXPORTS)


def _import_core():
    """Import and cache the native extension.

    The import guard (``_core_guard``) loads the Bloomberg runtime right after the extension
    imports and raises an ``ImportError`` listing every place it looked when none loads.
    """
    global _core_module
    core = _core_module
    if core is not None:
        return core
    if getattr(_core_import_state, "active", False):
        raise ImportError("Recursive import of _core detected")

    _core_import_state.active = True
    try:
        core = importlib.import_module("xbbg._core")
        # Another thread inside _core_guard.import_without_runtime() can briefly
        # publish xbbg._core in sys.modules before the runtime is loaded; finish
        # loading before caching _core_module so callers never see a runtime-less
        # extension.
        loaded_sdk = getattr(core, "loaded_sdk", None)
        if callable(loaded_sdk) and loaded_sdk() is None:
            try:
                _load_runtime(core)
            except ImportError:
                _core_guard.forget_core(core)
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
        ("_python_logging", PYTHON_LOGGING_EXPORTS),
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
