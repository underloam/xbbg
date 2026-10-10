"""Backend enum and availability infrastructure for xbbg.

This module provides the canonical ``Backend`` enum for selecting
DataFrame output backends, along with availability checking, version
validation, and format compatibility helpers.

The Rust layer returns Arrow tables; this module is a Python-only
concern that governs how those tables are converted to the user's
preferred DataFrame type.

Ported from ``release/0.x`` ``xbbg/backend.py``.
"""

from __future__ import annotations

import atexit
from dataclasses import dataclass
from enum import Enum
import functools
import logging
import os
import sys
import threading
from typing import Any, TypeAlias
import warnings

import narwhals.stable.v1 as nw

from xbbg._arrow import (
    ensure_arrow_table as ensure_arrow_table,
    is_arrow_table as is_arrow_table,
    is_pyarrow_table,
    native_schema,
)
from xbbg.services import Format

logger = logging.getLogger(__name__)
DataFrameResult: TypeAlias = Any


# =============================================================================
# Backend Enum
# =============================================================================


class Backend(str, Enum):
    """Enum for selecting the data processing backend.

    The backend determines which library is used for data manipulation
    and storage.  Each backend has different performance characteristics
    and memory usage patterns.

    **Eager backends** (full API support):

    - ``NATIVE`` – xbbg native Arrow carrier object
    - ``PYARROW`` – Apache Arrow Python ``pyarrow.Table``
    - ``NARWHALS`` – Backend-agnostic DataFrame API
    - ``PANDAS`` – Most widely used DataFrame library
    - ``POLARS`` – High-performance Rust-based DataFrames
    - ``CUDF`` – GPU-accelerated DataFrames (NVIDIA RAPIDS)
    - ``MODIN`` – Distributed pandas-like DataFrames

    **Lazy backends** (deferred execution):

    - ``NARWHALS_LAZY`` – Narwhals with lazy evaluation
    - ``POLARS_LAZY`` – Polars LazyFrame for query optimisation
    - ``DUCKDB`` – Embedded analytical SQL database
    - ``DASK`` – Parallel computing with task scheduling
    - ``IBIS`` – Portable DataFrame expressions (SQL backends)
    - ``PYSPARK`` – Apache Spark distributed DataFrames
    - ``SQLFRAME`` – SQL-based DataFrame abstraction
    """

    # Eager backends (full API support)
    NATIVE = "native"
    PYARROW = "pyarrow"
    NARWHALS = "narwhals"
    PANDAS = "pandas"
    POLARS = "polars"
    CUDF = "cudf"
    MODIN = "modin"

    # Lazy backends (deferred execution)
    NARWHALS_LAZY = "narwhals_lazy"
    POLARS_LAZY = "polars_lazy"
    DUCKDB = "duckdb"
    DASK = "dask"
    IBIS = "ibis"
    PYSPARK = "pyspark"
    SQLFRAME = "sqlframe"

    @classmethod
    def _missing_(cls, value: object) -> Backend | None:
        if isinstance(value, str):
            lowered = value.lower()
            for member in cls:
                if member.value == lowered:
                    return member
        return None


# =============================================================================
# Backend Descriptors
# =============================================================================


class BackendConversion(str, Enum):
    """Conversion strategy for xbbg native Arrow carriers."""

    NATIVE = "native"
    PYARROW = "pyarrow"
    PANDAS = "pandas"
    POLARS = "polars"
    POLARS_LAZY = "polars_lazy"
    NARWHALS = "narwhals"
    NARWHALS_LAZY = "narwhals_lazy"
    DUCKDB = "duckdb"
    PLANNED = "planned"


@dataclass(frozen=True)
class BackendDescriptor:
    """Single source of truth for backend metadata and conversion status."""

    backend: Backend
    package_name: str | None
    module_name: str | None
    extra_name: str | None
    min_version: tuple[int, ...] | None
    supported_formats: frozenset[Format]
    conversion: BackendConversion


_ALL_FORMATS = frozenset(
    {
        Format.LONG,
        Format.SEMI_LONG,
        Format.LONG_TYPED,
        Format.LONG_WITH_METADATA,
    }
)


def _descriptor(
    backend: Backend,
    *,
    package_name: str | None = None,
    module_name: str | None = None,
    extra_name: str | None = None,
    min_version: tuple[int, ...] | None = None,
    conversion: BackendConversion,
) -> BackendDescriptor:
    return BackendDescriptor(
        backend=backend,
        package_name=package_name,
        module_name=module_name,
        extra_name=extra_name,
        min_version=min_version,
        supported_formats=_ALL_FORMATS,
        conversion=conversion,
    )


BACKEND_DESCRIPTORS: dict[Backend, BackendDescriptor] = {
    Backend.NATIVE: _descriptor(Backend.NATIVE, conversion=BackendConversion.NATIVE),
    Backend.PYARROW: _descriptor(
        Backend.PYARROW,
        package_name="pyarrow",
        module_name="pyarrow",
        extra_name="pyarrow",
        min_version=(22, 0),
        conversion=BackendConversion.PYARROW,
    ),
    Backend.NARWHALS: _descriptor(
        Backend.NARWHALS,
        package_name="narwhals",
        module_name="narwhals",
        conversion=BackendConversion.NARWHALS,
    ),
    Backend.NARWHALS_LAZY: _descriptor(
        Backend.NARWHALS_LAZY,
        package_name="polars",
        module_name="polars",
        extra_name="polars",
        min_version=(0, 20, 4),
        conversion=BackendConversion.NARWHALS_LAZY,
    ),
    Backend.PANDAS: _descriptor(
        Backend.PANDAS,
        package_name="pandas",
        module_name="pandas",
        extra_name="pandas",
        min_version=(2, 0),
        conversion=BackendConversion.PANDAS,
    ),
    Backend.POLARS: _descriptor(
        Backend.POLARS,
        package_name="polars",
        module_name="polars",
        extra_name="polars",
        min_version=(0, 20, 4),
        conversion=BackendConversion.POLARS,
    ),
    Backend.POLARS_LAZY: _descriptor(
        Backend.POLARS_LAZY,
        package_name="polars",
        module_name="polars",
        extra_name="polars",
        min_version=(0, 20, 4),
        conversion=BackendConversion.POLARS_LAZY,
    ),
    Backend.DUCKDB: _descriptor(
        Backend.DUCKDB,
        package_name="duckdb",
        module_name="duckdb",
        extra_name="duckdb",
        min_version=(1, 5),
        conversion=BackendConversion.DUCKDB,
    ),
    Backend.CUDF: _descriptor(
        Backend.CUDF,
        package_name="cudf-cu12",
        module_name="cudf",
        min_version=(24, 10),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.MODIN: _descriptor(
        Backend.MODIN,
        package_name="modin[all]",
        module_name="modin",
        min_version=(0, 25),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.DASK: _descriptor(
        Backend.DASK,
        package_name="dask[dataframe]",
        module_name="dask",
        min_version=(2024, 1),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.IBIS: _descriptor(
        Backend.IBIS,
        package_name="ibis-framework",
        module_name="ibis",
        min_version=(6, 0),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.PYSPARK: _descriptor(
        Backend.PYSPARK,
        package_name="pyspark",
        module_name="pyspark",
        min_version=(3, 5),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.SQLFRAME: _descriptor(
        Backend.SQLFRAME,
        package_name="sqlframe",
        module_name="sqlframe",
        min_version=(3, 22),
        conversion=BackendConversion.PLANNED,
    ),
}

MIN_VERSIONS: dict[Backend, tuple[int, ...]] = {
    backend: descriptor.min_version
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.min_version is not None
}
PACKAGE_NAMES: dict[Backend, str] = {
    backend: descriptor.package_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.package_name is not None
}
EXTRA_NAMES: dict[Backend, str] = {
    backend: descriptor.extra_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.extra_name is not None
}
MODULE_NAMES: dict[Backend, str] = {
    backend: descriptor.module_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.module_name is not None
}
SUPPORTED_FORMATS: dict[Backend, frozenset[Format]] = {
    backend: descriptor.supported_formats for backend, descriptor in BACKEND_DESCRIPTORS.items()
}


# =============================================================================
# Internal Helpers
# =============================================================================


def _parse_version(version_str: str) -> tuple[int, ...]:
    """Parse a version string into a comparable tuple of ints.

    Strips pre-release suffixes (``a1``, ``b2``, ``rc1``, ``+local``).

    >>> _parse_version("2.0.1")
    (2, 0, 1)
    >>> _parse_version("0.20.4a1")
    (0, 20, 4)
    """
    version_str = version_str.split("+")[0]  # remove local
    version_str = version_str.split("a")[0]  # remove alpha
    version_str = version_str.split("b")[0]  # remove beta
    version_str = version_str.split("rc")[0]  # remove rc

    parts: list[int] = []
    for part in version_str.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            break
    return tuple(parts)


def _format_version(version: tuple[int, ...]) -> str:
    """Format a version tuple as a dotted string."""
    return ".".join(str(v) for v in version)


def _get_module(backend: Backend) -> Any | None:
    """Return the backend's module if it has already been imported.

    Checks ``sys.modules`` without triggering an import, following
    the narwhals convention.
    """
    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        return None
    return sys.modules.get(module_name)


def _get_module_version(module: Any) -> tuple[int, ...] | None:
    """Return a module's ``__version__`` as a parsed tuple, or ``None``."""
    version_str = getattr(module, "__version__", None)
    if version_str is None:
        version_str = getattr(module, "VERSION", None)
    if not version_str:
        return None
    return _parse_version(str(version_str))


# =============================================================================
# Backend Availability
# =============================================================================


def is_backend_available(backend: Backend | str) -> bool:
    """Check whether a backend is installed and importable.

    The native xbbg carrier and eager bundled Narwhals plugin always return ``True``.
    Lazy Narwhals availability additionally requires Polars.

    Args:
        backend: A :class:`Backend` member or its string value.

    Returns:
        ``True`` when the backend package can be imported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)

    # Core/native dependencies — always available.
    if backend in (Backend.NATIVE, Backend.NARWHALS):
        return True

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        return False

    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


def check_backend(backend: Backend | str, *, raise_on_error: bool = True) -> bool:
    """Check that a backend is available *and* meets minimum version requirements.

    Provides actionable error messages with ``pip install`` instructions.

    Args:
        backend: A :class:`Backend` member or its string value.
        raise_on_error: Raise on failure instead of returning ``False``.

    Returns:
        ``True`` when the backend is usable.

    Raises:
        ImportError: Package not installed (when *raise_on_error*).
        ValueError: Installed version too old (when *raise_on_error*).
    """
    if isinstance(backend, str):
        backend = Backend(backend)

    # Native/core dependencies — always OK.
    if backend in (Backend.NATIVE, Backend.NARWHALS):
        return True

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        msg = f"Unknown backend: {backend.value}"
        if raise_on_error:
            raise ValueError(msg)
        logger.warning(msg)
        return False

    package_name = PACKAGE_NAMES.get(backend, module_name)
    min_version = MIN_VERSIONS.get(backend)

    # Try to import.
    try:
        module = __import__(module_name)
    except ImportError:
        msg = (
            f"Backend '{backend.value}' requires the '{package_name}' package, "
            f"which is not installed.\n\n"
            f"To install, run:\n"
            f"    pip install {package_name}"
        )
        if min_version:
            msg += f">={_format_version(min_version)}"

        extra_name = EXTRA_NAMES.get(backend)
        if extra_name is not None:
            msg += "\n\nOr install with xbbg extras:\n"
            msg += f"    pip install xbbg[{extra_name}]"

        if raise_on_error:
            raise ImportError(msg) from None
        logger.warning(msg)
        return False

    if (
        backend in (Backend.POLARS, Backend.POLARS_LAZY, Backend.NARWHALS_LAZY)
        and getattr(module, "__version__", None) == ""
    ):
        msg = (
            f"Backend '{backend.value}' requires a usable '{package_name}' package, "
            "but the installed Polars package is missing its native binary.\n\n"
            "To reinstall, run:\n"
            f"    pip install --force-reinstall {package_name}"
        )
        if min_version:
            msg += f">={_format_version(min_version)}"
        extra_name = EXTRA_NAMES.get(backend)
        if extra_name is not None:
            msg += "\n\nOr install with xbbg extras:\n"
            msg += f"    pip install xbbg[{extra_name}]"
        if raise_on_error:
            raise ImportError(msg) from None
        logger.warning(msg)
        return False

    # Version check.
    if min_version:
        version = _get_module_version(module)
        if version is None:
            logger.debug("Could not determine version for %s", module_name)
        elif version < min_version:
            msg = (
                f"Backend '{backend.value}' requires {package_name} >= "
                f"{_format_version(min_version)}, but version "
                f"{_format_version(version)} is installed.\n\n"
                f"To upgrade, run:\n"
                f"    pip install --upgrade {package_name}>={_format_version(min_version)}"
            )
            if raise_on_error:
                raise ValueError(msg)
            logger.warning(msg)
            return False

    return True


def _import_backend_module(backend: Backend | str, *, feature: str | None = None) -> Any:
    """Import an optional backend module after xbbg availability checks."""
    if isinstance(backend, str):
        backend = Backend(backend)

    try:
        check_backend(backend)
    except (ImportError, ValueError) as exc:
        if feature is not None:
            raise type(exc)(f"{feature} requires optional backend '{backend.value}'.\n\n{exc}") from None
        raise

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        raise ValueError(f"Unknown backend: {backend.value}")
    return __import__(module_name)


_XBBG_METADATA_ATTRS = {
    "xbbg.eid_data": ("eid_data", "xbbg_eid_data"),
    "xbbg.security_errors": ("security_errors", "xbbg_security_errors"),
    "xbbg.field_exceptions": ("field_exceptions", "xbbg_field_exceptions"),
}


def _attach_xbbg_metadata_attrs(dataframe: Any, table: Any) -> Any:
    """Attach native xbbg metadata to pandas ``DataFrame.attrs`` when present.

    Only pandas frames expose ``attrs`` in the conversion path.  The polars
    conversion intentionally skips this helper because polars frames do not
    provide an equivalent stable metadata side channel.
    """
    attrs = getattr(dataframe, "attrs", None)
    metadata = getattr(table, "metadata", None)
    if attrs is None or not metadata:
        return dataframe

    for metadata_key, (source_attr, frame_attr) in _XBBG_METADATA_ATTRS.items():
        if metadata_key not in metadata:
            continue
        value = getattr(table, source_attr, None)
        if value is not None:
            attrs[frame_attr] = value
    return dataframe


def _to_pyarrow_table(table: Any) -> Any:
    pa = _import_backend_module(Backend.PYARROW)
    return pa.table(table)


def _to_pandas_frame(table: Any) -> Any:
    pd = _import_backend_module(Backend.PANDAS)
    try:
        pa = _import_backend_module(Backend.PYARROW)
    except ImportError:
        # Slow fallback for installations without pyarrow: materializes one
        # Python dict per cell path instead of consuming the Arrow C stream.
        frame = pd.DataFrame.from_records(table.to_pylist(), columns=table.column_names)
    else:
        frame = pa.table(table).to_pandas(split_blocks=True)
    return _attach_xbbg_metadata_attrs(frame, table)


_POLARS_CAPSULE_CONSTRUCTOR_MIN_VERSION = (1, 3)


def _to_polars_frame_from_native_batches(table: Any, pl: Any) -> Any:
    """Materialize legacy Polars input one native batch at a time with its schema."""
    schema = nw.Schema(native_schema(table)).to_polars()
    unknown = getattr(pl, "Unknown", None)
    unsupported = [name for name, dtype in schema.items() if unknown is not None and dtype == unknown]
    if unsupported:
        raise TypeError(
            "installed Polars does not support native Arrow dtype(s) for column(s): "
            + ", ".join(repr(name) for name in unsupported)
        )
    frames = [pl.DataFrame(batch.to_pylist(), schema=schema) for batch in table.to_batches() if batch.num_rows]
    if not frames:
        return pl.DataFrame(schema=schema)
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames, rechunk=False)


def _to_polars_frame(table: Any, *, feature: str | None = None) -> Any:
    """Convert to Polars without coalescing the native Arrow chunks."""
    pl = _import_backend_module(Backend.POLARS, feature=feature)

    if is_pyarrow_table(table):
        return pl.from_arrow(table, rechunk=False)

    version = _get_module_version(pl)
    if version is not None and version >= _POLARS_CAPSULE_CONSTRUCTOR_MIN_VERSION:
        # Polars 1.3 added Arrow PyCapsule support to DataFrame constructors.
        # Unlike ``from_arrow``, which only gained capsule support in 1.22 and
        # requests a rechunk by default, this constructor preserves chunks.
        return pl.DataFrame(table)

    try:
        pa = _import_backend_module(Backend.PYARROW)
    except ImportError:
        # Polars before 1.3 cannot consume an Arrow stream capsule directly.
        # Construct one frame per native batch so the declared 0.20 floor
        # preserves both logical dtypes and native chunk boundaries.
        return _to_polars_frame_from_native_batches(table, pl)
    return pl.from_arrow(pa.table(table), rechunk=False)


class _DuckDBDatabase:
    """Own one process-local DuckDB database and vend isolated connections."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._initializing_pid: int | None = None
        self._database_name: str | None = None
        self._anchor: Any | None = None
        self._atexit_registered = False
        register_at_fork = getattr(os, "register_at_fork", None)
        if register_at_fork is not None:
            register_at_fork(after_in_child=self._after_fork)

    def _after_fork(self) -> None:
        # Reset only the Python lock. Native DuckDB state inherited from the
        # parent must remain untouched and is rejected before any child use.
        self._lock = threading.Lock()

    def _inherited_owner_pid(self) -> int | None:
        owner_pid = self._pid if self._pid is not None else self._initializing_pid
        if owner_pid is not None and owner_pid != os.getpid():
            return owner_pid
        return None

    def _raise_if_inherited(self, operation: str) -> None:
        owner_pid = self._inherited_owner_pid()
        if owner_pid is None:
            return
        raise RuntimeError(
            f"DuckDB backend state was initialized in process {owner_pid} before "
            f"this child process was forked and cannot be {operation} safely in "
            f"process {os.getpid()}. Use the multiprocessing 'spawn' start method, "
            "or fork before initializing xbbg's DuckDB backend."
        )

    def connect(self) -> Any:
        self._raise_if_inherited("used")
        pid = os.getpid()
        with self._lock:
            if self._anchor is None:
                self._initializing_pid = pid
                try:
                    duckdb = _import_backend_module(Backend.DUCKDB)
                    database_name = f":memory:xbbg_backend_{pid}_{id(self):x}"
                    anchor = duckdb.connect(database=database_name)
                    self._pid = pid
                    self._database_name = database_name
                    self._anchor = anchor
                    if not self._atexit_registered:
                        # Register after DuckDB is imported so this runs before
                        # module-level DuckDB teardown callbacks (atexit is LIFO).
                        atexit.register(self._close_at_exit)
                        self._atexit_registered = True
                finally:
                    self._initializing_pid = None
            else:
                duckdb = _import_backend_module(Backend.DUCKDB)
            return duckdb.connect(database=self._database_name)

    def close(self) -> None:
        self._raise_if_inherited("closed")
        with self._lock:
            anchor = self._anchor
            self._anchor = None
            self._database_name = None
            # Keep the owner PID after close: only a manager that has never
            # initialized DuckDB may be initialized after a fork.
            if anchor is not None:
                anchor.close()

    def _close_at_exit(self) -> None:
        if self._inherited_owner_pid() is not None:
            return
        try:
            self.close()
        except Exception:
            # Interpreter teardown may already have unloaded DuckDB.
            pass


_DUCKDB_DATABASE = _DuckDBDatabase()


def _to_duckdb_relation(table: Any) -> Any:
    connection = _DUCKDB_DATABASE.connect()
    try:
        connection.register("xbbg_arrow", table)
        return connection.sql("select * from xbbg_arrow")
    except Exception:
        connection.close()
        raise


_native_narwhals_fallback_warned = False


def _warn_native_narwhals_fallback() -> None:
    global _native_narwhals_fallback_warned
    if _native_narwhals_fallback_warned:
        return
    _native_narwhals_fallback_warned = True
    warnings.warn(
        "No optional dataframe backend is installed for xbbg's Narwhals output; "
        "falling back to the limited xbbg native ArrowTable plugin. "
        "Install `xbbg[pyarrow]`, `xbbg[pandas]`, or `xbbg[polars]` for full dataframe behavior, "
        "or request `backend='native'` explicitly if the raw xbbg ArrowTable is intended.",
        RuntimeWarning,
        stacklevel=3,
    )


@functools.lru_cache(maxsize=1)
def _best_narwhals_backend() -> Backend | None:
    """Return the best installed concrete backend for Narwhals native wrapping."""
    for candidate in (Backend.PYARROW, Backend.PANDAS, Backend.POLARS):
        if is_backend_available(candidate) and check_backend(candidate, raise_on_error=False):
            return candidate
    return None


def _best_narwhals_native(table: Any) -> Any:
    """Return the richest installed native object for Narwhals wrapping."""
    candidate = _best_narwhals_backend()
    if candidate is Backend.PYARROW:
        return _to_pyarrow_table(table)
    if candidate is Backend.PANDAS:
        return _to_pandas_frame(table)
    if candidate is Backend.POLARS:
        return _to_polars_frame(table)
    _warn_native_narwhals_fallback()
    return table


_default_backend: Backend | None = None


def set_backend(backend: Backend | str | None) -> None:
    """Set the default output backend, or ``None`` for automatic selection.

    Optional backends are checked before changing the session default. The
    setting applies to both request endpoints and extension recipes.
    """
    global _default_backend
    if backend is None:
        _default_backend = None
        return
    if isinstance(backend, Backend):
        resolved = backend
    elif isinstance(backend, str):
        try:
            resolved = Backend(backend)
        except ValueError:
            valid = [b.value for b in Backend]
            raise ValueError(f"Invalid backend: {backend}. Must be one of {valid}") from None
    else:
        raise TypeError(f"backend must be Backend, str, or None, not {type(backend).__name__}")
    check_backend(resolved)
    _default_backend = resolved


def get_backend() -> Backend | None:
    """Get the configured session default, or ``None`` for automatic selection."""
    return _default_backend


def _resolve_backend(backend: Backend | str | None) -> Backend | None:
    return resolve_backend(backend, _default_backend)


def _effective_backend(backend: Backend | str | None) -> Backend:
    return effective_backend(backend, _default_backend)


def _convert_result_backend(frame: Any, backend: Backend | str | None) -> DataFrameResult:
    return convert_backend_frame(frame, _effective_backend(backend))


def resolve_backend(
    backend: Backend | str | None,
    default_backend: Backend | str | None = None,
) -> Backend | None:
    """Resolve an optional backend selection against an optional configured default."""
    selected = default_backend if backend is None else backend
    if selected is None:
        return None
    return Backend(selected) if isinstance(selected, str) else selected


def effective_backend(
    backend: Backend | str | None,
    default_backend: Backend | str | None = None,
) -> Backend:
    """Resolve a backend selection, falling back to the package default if unset."""
    return resolve_backend(backend, default_backend) or get_default_backend()


def convert_backend_frame(frame: Any, backend: Backend | str) -> DataFrameResult:
    """Convert an xbbg ArrowTable to the requested public backend."""
    effective = Backend(backend) if isinstance(backend, str) else backend
    descriptor = BACKEND_DESCRIPTORS[effective]
    if effective is Backend.DUCKDB:
        # Fail before dependency checks can touch a partially imported DuckDB
        # module inherited from an initialization-time fork.
        _DUCKDB_DATABASE._raise_if_inherited("used")

    table = ensure_arrow_table(frame)

    if effective not in (Backend.NATIVE, Backend.NARWHALS, Backend.DUCKDB):
        check_backend(effective)

    match descriptor.conversion:
        case BackendConversion.NATIVE:
            return table
        case BackendConversion.PYARROW:
            return _to_pyarrow_table(table)
        case BackendConversion.PANDAS:
            return _to_pandas_frame(table)
        case BackendConversion.POLARS:
            return _to_polars_frame(table)
        case BackendConversion.POLARS_LAZY:
            return _to_polars_frame(table).lazy()
        case BackendConversion.NARWHALS:
            return nw.from_native(_best_narwhals_native(table))
        case BackendConversion.NARWHALS_LAZY:
            return nw.from_native(_to_polars_frame(table, feature="backend='narwhals_lazy'").lazy())
        case BackendConversion.DUCKDB:
            return _to_duckdb_relation(table)
        case BackendConversion.PLANNED:
            raise NotImplementedError(
                f"Backend '{effective.value}' is selectable but conversion from xbbg native Arrow "
                "is not implemented yet. Choose one of: native, pyarrow, pandas, polars, "
                "polars_lazy, narwhals, narwhals_lazy, duckdb."
            )

    raise AssertionError(f"Unhandled backend conversion: {descriptor.conversion!r}")


def get_available_backends() -> list[Backend]:
    """Return every :class:`Backend` whose package is currently importable."""
    return [b for b in Backend if is_backend_available(b)]


DEFAULT_BACKEND = Backend.NARWHALS


def get_default_backend() -> Backend:
    """Return the default public result backend for xbbg calls."""
    if check_backend(DEFAULT_BACKEND, raise_on_error=False):
        return DEFAULT_BACKEND
    raise ImportError(
        "No xbbg result backend is available. The core narwhals dependency should provide "
        "Backend.NARWHALS; if it is unavailable, reinstall xbbg or explicitly request "
        "a backend such as native, pyarrow, pandas, or polars."
    )


def print_backend_status() -> None:
    """Print a diagnostic table of all backends to the logger.

    Shows installed/missing status, current version, and minimum
    requirement for each backend.
    """
    logger.info("xbbg Backend Status")
    logger.info("=" * 60)
    logger.info("")

    for backend in Backend:
        module_name = MODULE_NAMES.get(backend, "")

        if backend == Backend.NATIVE:
            status = "OK (native)"
            version_info = "provided by xbbg"
        elif backend is Backend.NARWHALS:
            module = _get_module(backend) or __import__(module_name)
            version = _get_module_version(module)
            status = "OK (core)"
            version_info = f"v{_format_version(version)}" if version else ""
        elif is_backend_available(backend):
            module = __import__(module_name)
            version = _get_module_version(module)
            min_ver = MIN_VERSIONS.get(backend)

            if version and min_ver and version < min_ver:
                status = "OUTDATED"
                version_info = f"v{_format_version(version)} (need >={_format_version(min_ver)})"
            else:
                status = "OK"
                version_info = f"v{_format_version(version)}" if version else ""
        else:
            status = "NOT INSTALLED"
            package = PACKAGE_NAMES.get(backend, module_name or "?")
            version_info = f"pip install {package}"

        logger.info("  %s %s %s", f"{backend.value:15}", f"{status:15}", version_info)

    logger.info("")
    logger.info("=" * 60)


# =============================================================================
# Format Compatibility Checking
# =============================================================================


def is_format_supported(backend: Backend | str, fmt: Format | str) -> bool:
    """Check whether *backend* supports *fmt*.

    Args:
        backend: Backend to check.
        fmt: Output format to check.

    Returns:
        ``True`` if the format is supported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)
    if isinstance(fmt, str):
        fmt = Format(fmt)
    return fmt in SUPPORTED_FORMATS.get(backend, frozenset())


def get_supported_formats(backend: Backend | str) -> frozenset[Format]:
    """Return the set of :class:`~xbbg.services.Format` variants supported by *backend*."""
    if isinstance(backend, str):
        backend = Backend(backend)
    return SUPPORTED_FORMATS.get(backend, frozenset({Format.LONG, Format.SEMI_LONG}))


def check_format_compatibility(
    backend: Backend | str,
    fmt: Format | str,
    *,
    raise_on_error: bool = True,
) -> bool:
    """Validate that *backend* supports *fmt*, with actionable messages.

    Args:
        backend: Backend to use.
        fmt: Desired output format.
        raise_on_error: Raise :class:`ValueError` on mismatch.

    Returns:
        ``True`` if compatible.

    Raises:
        ValueError: When *raise_on_error* and the combination is unsupported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)
    if isinstance(fmt, str):
        fmt = Format(fmt)

    if is_format_supported(backend, fmt):
        return True

    supported = get_supported_formats(backend)
    supported_str = ", ".join(f.value for f in sorted(supported, key=lambda x: x.value))

    msg = (
        f"Backend '{backend.value}' does not support format '{fmt.value}'.\n\n"
        f"Supported formats for {backend.value}: {supported_str}\n\n"
    )

    if fmt in (Format.LONG_TYPED, Format.LONG_WITH_METADATA):
        msg += "Hint: LONG_TYPED and LONG_WITH_METADATA are v1.0 preview formats."

    if raise_on_error:
        raise ValueError(msg)

    logger.warning(msg)
    return False


def validate_backend_format(
    backend: Backend | str | None,
    fmt: Format | str | None,
    *,
    raise_on_error: bool = True,
) -> tuple[Backend, Format]:
    """Normalise and validate a backend/format pair.

    This is the main entry-point for API functions.  It:

    1. Converts string values to enums.
    2. Falls back to the session defaults (``set_backend`` / ``set_format``).
    3. Checks backend availability and version.
    4. Checks format compatibility.

    Args:
        backend: Backend enum, string, or ``None`` for the session default.
        fmt: Format enum, string, or ``None`` for the session default.
        raise_on_error: Raise on invalid combinations.

    Returns:
        A ``(Backend, Format)`` tuple of validated values.

    Raises:
        ImportError: Backend package missing.
        ValueError: Version too old or format unsupported.
    """
    # Normalise backend.
    if backend is None:
        backend = get_backend() or get_default_backend()
    elif isinstance(backend, str):
        backend = Backend(backend)

    # Normalise format.
    if fmt is None:
        fmt = Format.LONG
    elif isinstance(fmt, str):
        fmt = Format(fmt)

    # Validate.
    check_backend(backend, raise_on_error=raise_on_error)
    check_format_compatibility(backend, fmt, raise_on_error=raise_on_error)

    return backend, fmt
