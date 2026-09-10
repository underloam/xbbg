"""High-level xbbg request API: reference, historical, intraday.

This module provides the xbbg-compatible API for authorized Bloomberg environments,
with support for multiple DataFrame backends via narwhals.

API Design:
- Async-first: Core implementation uses async/await (abdp, abdh, etc.)
- Sync wrappers: Convenience functions wrap async with asyncio.run(), with a notebook bridge for one-shot requests
- Generic API: arequest() and request() for power users and arbitrary Bloomberg requests
- Users can use either style based on their needs
"""

from __future__ import annotations

import asyncio
import atexit
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
import concurrent.futures
import contextvars
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import functools
import inspect
import logging
import re
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, TypeAlias, cast
import warnings

from xbbg.services import (
    OVERFLOW_POLICIES,
    OVERFLOW_POLICY_VALUES,
    ExtractorHint,
    Format,
    Operation,
    OutputMode,
    RequestParams,
    Service,
)

from ._exports import BLP_MODULE_EXPORTS
from .backend import (
    Backend,
    check_backend,
    convert_backend_frame,
    effective_backend as _backend_effective_backend,
    ensure_arrow_table,
    resolve_backend as _backend_resolve_backend,
)
from .request_middleware import (
    RequestContext,
    RequestEnvironment,
    add_middleware as add_middleware,
    clear_middleware as clear_middleware,
    get_middleware as get_middleware,
    remove_middleware as remove_middleware,
    run_request_middleware as _run_request_middleware,
    set_middleware as set_middleware,
)

# Type alias for backend conversion return types.
DataFrameResult: TypeAlias = Any

logger = logging.getLogger(__name__)
_FIELD_TYPE_RESOLUTION_CACHE_MAXSIZE = 1024
_FIELD_TYPE_RESOLUTION_CACHE: dict[tuple[tuple[str, ...], str], dict[str, str]] = {}
_FIELD_TYPE_RESOLUTION_CACHE_LOCK = threading.Lock()
_FIELD_TYPE_RESOLUTION_CACHE_GENERATION = 0
_DEFAULT_SYNC_STREAM_CAPACITY = 256
_DEFAULT_MAX_SYNC_STREAM_PRODUCERS = 32
_SYNC_STREAM_PRODUCERS_LOCK = threading.Lock()
_GLOBAL_SYNC_STREAM_PRODUCER_SCOPE = object()
_ACTIVE_SYNC_STREAM_PRODUCERS: dict[object, set[object]] = {}
_SYNC_STREAM_CLOSE_TIMEOUT_SECONDS = 1.0


def _clear_field_type_resolution_cache() -> None:
    """Clear cached field type resolutions after schema/cache invalidation."""
    global _FIELD_TYPE_RESOLUTION_CACHE_GENERATION
    with _FIELD_TYPE_RESOLUTION_CACHE_LOCK:
        _FIELD_TYPE_RESOLUTION_CACHE.clear()
        _FIELD_TYPE_RESOLUTION_CACHE_GENERATION += 1


__all__ = list(BLP_MODULE_EXPORTS)


_REMOVED_LEGACY_ATTRS: dict[str, str] = {
    "connect": (
        "blp.connect() was removed in xbbg 1.0. The engine now starts automatically "
        "on the first request. If you need a non-default host, port, or auth (e.g. "
        "for B-PIPE), call xbbg.configure() once before your first request:\n\n"
        "    import xbbg\n"
        "    xbbg.configure(\n"
        "        host='bpipe-host',\n"
        "        port=8194,\n"
        "        auth_method='app',\n"
        "        app_name='my-app',\n"
        "    )\n\n"
        "See https://xbbg.org/python/guides/migration/#connection-setup"
    ),
    "disconnect": (
        "blp.disconnect() was removed in xbbg 1.0. The engine lifecycle is managed "
        "automatically and you no longer need to disconnect. If you really need to "
        "tear down the engine (e.g. in tests), call xbbg.shutdown() or xbbg.reset()."
    ),
    "getBlpapiVersion": (
        "blp.getBlpapiVersion() was removed in xbbg 1.0. Use xbbg.get_sdk_info() "
        "instead, which returns a dict including 'runtime_version' (the linked C "
        "SDK version) and the active SDK source."
    ),
}


def __getattr__(name: str):
    msg = _REMOVED_LEGACY_ATTRS.get(name)
    if msg is not None:
        raise AttributeError(msg)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Generated sync wrappers are installed dynamically by _install_generated_endpoints().
if TYPE_CHECKING:
    # ``DateLike`` is also imported lazily below alongside ``_fmt_date``; the
    # second copy here keeps the static stubs visible to type-checkers / IDEs.
    from xbbg._core import EntitlementReport
    from xbbg.ext._utils import DateLike

    def bdp(
        tickers: str | Sequence[str],
        flds: str | Sequence[str] | None = None,
        *,
        backend: Backend | str | None = None,
        format: Format | str | None = None,
        field_types: dict[str, str] | None = None,
        include_security_errors: bool = False,
        return_eids: bool = False,
        validate_fields: bool | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg reference data (BDP). See ``abdp`` for details."""
        ...

    def bdh(
        tickers: str | Sequence[str],
        flds: str | Sequence[str] | None = None,
        start_date: DateLike = None,
        end_date: DateLike = "today",
        *,
        backend: Backend | str | None = None,
        format: Format | str | None = None,
        field_types: dict[str, str] | None = None,
        validate_fields: bool | None = None,
        return_eids: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg historical data (BDH). See ``abdh`` for details."""
        ...

    def bds(
        tickers: str | Sequence[str],
        flds: str,
        *,
        backend: Backend | str | None = None,
        validate_fields: bool | None = None,
        return_eids: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg bulk data (BDS). See ``abds`` for details."""
        ...

    def bdib(
        ticker: str,
        dt: DateLike = None,
        session: str = "allday",
        typ: str = "TRADE",
        *,
        start_datetime: DateLike = None,
        end_datetime: DateLike = None,
        interval: int = 1,
        backend: Backend | str | None = None,
        request_tz: str | None = None,
        output_tz: str | None = None,
        return_eids: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg intraday bar data (BDIB). See ``abdib`` for details."""
        ...

    def bdtick(
        ticker: str,
        start_datetime: DateLike,
        end_datetime: DateLike,
        *,
        event_types: Sequence[str] | None = None,
        backend: Backend | str | None = None,
        request_tz: str | None = None,
        output_tz: str | None = None,
        return_eids: bool = False,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg tick data (BDTICK). See ``abdtick`` for details."""
        ...

    def bql(
        expression: str,
        *,
        backend: Backend | str | None = None,
    ) -> DataFrameResult:
        """Sync Bloomberg Query Language (BQL) request. See ``abql`` for details."""
        ...

    def bsrch(
        domain: str,
        *,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg Search (BSRCH) request. See ``absrch`` for details."""
        ...

    def bqr(
        ticker: str,
        date_offset: str | None = None,
        start_date: DateLike = None,
        end_date: DateLike = None,
        *,
        event_types: Sequence[str] | None = None,
        include_broker_codes: bool = False,
        include_spread_price: bool = False,
        include_yield: bool = False,
        include_condition_codes: bool = False,
        include_exchange_codes: bool = False,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg Quote Request (BQR). See ``abqr`` for details."""
        ...

    def bflds(
        fields: str | list[str] | None = None,
        *,
        search_spec: str | None = None,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg field metadata lookup (BFLDS). See ``abflds`` for details."""
        ...

    def beqs(
        screen: str,
        *,
        asof: str | None = None,
        screen_type: str = "PRIVATE",
        group: str = "General",
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg Equity Screening (BEQS) request. See ``abeqs`` for details."""
        ...

    def blkp(
        query: str,
        *,
        yellowkey: str = "YK_FILTER_NONE",
        language: str = "LANG_OVERRIDE_NONE",
        max_results: int = 20,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg security lookup (BLKP) request. See ``ablkp`` for details."""
        ...

    def bport(
        portfolio: str,
        fields: str | Sequence[str],
        *,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg portfolio data (BPORT) request. See ``abport`` for details."""
        ...

    def bcurves(
        *,
        country: str | None = None,
        currency: str | None = None,
        curve_type: str | None = None,
        subtype: str | None = None,
        curveid: str | None = None,
        bbgid: str | None = None,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg yield curve list (BCURVES) request. See ``abcurves`` for details."""
        ...

    def bgovts(
        query: str | None = None,
        *,
        partial_match: bool = True,
        backend: Backend | str | None = None,
        **kwargs: Any,
    ) -> DataFrameResult:
        """Sync Bloomberg government securities list (BGOVTS) request. See ``abgovts`` for details."""
        ...

else:
    (bdp, bdh, bds, bdib, bdtick, bql, bsrch, bqr, bflds, beqs, blkp, bport, bcurves, bgovts) = (None,) * 14


# Backend configuration
_default_backend: Backend | None = None

# Engine configuration (set before first use)
_config = None  # PyEngineConfig instance or None

# Lazy-load the engine to avoid import errors when the Rust module isn't built
_engine = None
_engine_lock = threading.Lock()

# Scoped engine for multi-engine routing (async-safe via contextvars)
_active_engine: contextvars.ContextVar[Engine | None] = contextvars.ContextVar("_active_engine", default=None)

_NOTEBOOK_SYNC_BRIDGE_NAMES = frozenset({"bdp", "bdh", "bds", "bdib", "bdtick", "request"})


class Engine:
    """Non-global Bloomberg engine for multi-source routing.

    Use as a context manager to scope all ``blp.*`` calls to this engine:

        engine = blp.Engine(host="bpipe.firm.com", auth_method="app", app_name="myapp")
        with engine:
            df = blp.bdp(...)  # uses this engine, not the global

    Or pass directly to individual calls:

        df = blp.bdp(..., engine=engine)

    The global ``configure()`` + ``blp.bdp()`` API is unaffected.
    """

    def __init__(self, **kwargs: Any) -> None:
        from . import _core

        normalized = _normalize_config_kwargs(kwargs)
        config = _core.PyEngineConfig(**normalized)
        self._config_snapshot = config
        self._py_engine = _core.PyEngine.with_config(config)
        self._scope_tokens: contextvars.ContextVar[tuple[contextvars.Token, ...]] = contextvars.ContextVar(
            f"xbbg_engine_scope_tokens_{id(self)}",
            default=(),
        )

    def __enter__(self) -> Engine:
        token = _active_engine.set(self)
        self._scope_tokens.set((*self._scope_tokens.get(), token))
        return self

    def __exit__(self, *exc: Any) -> None:
        tokens = self._scope_tokens.get()
        if tokens:
            _active_engine.reset(tokens[-1])
            self._scope_tokens.set(tokens[:-1])

    async def __aenter__(self) -> Engine:
        return self.__enter__()

    async def __aexit__(self, *exc: Any) -> None:
        self.__exit__(*exc)

    def shutdown(self) -> None:
        self._py_engine.signal_shutdown()


# =============================================================================
# Engine Lifecycle Management
# =============================================================================


def _atexit_cleanup() -> None:
    """Signal interpreter finalization before releasing process-global resources."""
    global _engine
    core = sys.modules.get("xbbg._core")
    if core is not None:
        try:
            core._signal_interpreter_shutdown()
        except Exception:
            logger.error("Failed to signal native interpreter shutdown", exc_info=True)

    with _engine_lock:
        engine = _engine
        _engine = None

    if engine is not None:
        try:
            engine.signal_shutdown()
        except Exception:
            logger.debug("Exception during atexit cleanup (ignored)", exc_info=True)

    stop_bridge = globals().get("_stop_notebook_sync_loop")
    if stop_bridge is not None:
        try:
            stop_bridge()
        except Exception:
            logger.debug("Exception stopping notebook sync bridge (ignored)", exc_info=True)


# Register cleanup handler
atexit.register(_atexit_cleanup)


def shutdown() -> None:
    """Signal the Bloomberg engine to shutdown.

    Signals all worker threads to stop. They will terminate when they
    finish their current work or see the shutdown signal.

    This is called automatically during Python interpreter shutdown.
    You usually don't need to call this directly.

    Example::

        import xbbg

        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        # Explicit shutdown (optional - happens automatically on exit)
        xbbg.shutdown()
    """
    global _engine
    with _engine_lock:
        engine = _engine
        _engine = None

    if engine is not None:
        engine.signal_shutdown()


def reset() -> None:
    """Reset the engine to allow reconfiguration.

    Shuts down the current engine (if any) and clears configuration.
    The next Bloomberg request will create a fresh engine.

    Example::

        import xbbg

        # Initial usage
        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        # Need different config? Reset first
        xbbg.reset()
        xbbg.configure(port=9999)
        df = xbbg.bdp("AAPL US Equity", "PX_LAST")  # Uses new config
    """
    global _engine, _config
    with _engine_lock:
        engine = _engine
        _engine = None
        _config = None

    if engine is not None:
        engine.signal_shutdown()


def is_connected() -> bool:
    """Check if Bloomberg is connected and healthy.

    Returns True if the engine exists and at least one worker has
    a live Bloomberg session. Returns False if the engine hasn't
    been created yet or all workers have lost their connection.

    Example::

        import xbbg

        print(xbbg.is_connected())  # False - not initialized yet

        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        print(xbbg.is_connected())  # True - connected
    """
    with _engine_lock:
        engine = _engine
    return engine is not None and engine.is_connected()


async def aseat_type() -> str:
    """Return the seat type for the lazily authorized Bloomberg identity.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _get_engine().seat_type()


async def acheck_entitlements(
    eids: Sequence[int],
    service: str = Service.REFDATA.value,
) -> EntitlementReport:
    """Check EID entitlements for the lazily authorized Bloomberg identity.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _get_engine().check_entitlements(service, list(eids))


async def aidentity_is_authorized(service: str = Service.REFDATA.value) -> bool:
    """Return whether the lazily authorized identity is authorized for *service*.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _get_engine().identity_is_authorized(service)


_VALID_CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "host",
        "port",
        "servers",
        "zfp_remote",
        "request_pool_size",
        "subscription_pool_size",
        "runtime_worker_threads",
        "max_subscription_sessions",
        "shard_requests",
        "shard_threshold",
        "shard_chunk_size",
        "shard_max_concurrent",
        "validation_mode",
        "subscription_flush_threshold",
        "max_event_queue_size",
        "command_queue_size",
        "subscription_stream_capacity",
        "overflow_policy",
        "warmup_services",
        "field_cache_path",
        "auth_method",
        "app_name",
        "dir_property",
        "user_id",
        "ip_address",
        "token",
        "tls_client_credentials",
        "tls_client_credentials_password",
        "tls_trust_material",
        "tls_handshake_timeout_ms",
        "tls_crl_fetch_timeout_ms",
        "num_start_attempts",
        "auto_restart_on_disconnection",
        "retry_max_retries",
        "retry_initial_delay_ms",
        "retry_backoff_factor",
        "retry_max_delay_ms",
        "request_timeout_ms",
        "streams_deactivated_warn_ms",
        "keep_alive_enabled",
        "keep_alive_inactivity_ms",
        "keep_alive_response_timeout_ms",
        "slow_consumer_hi_water_mark",
        "slow_consumer_lo_water_mark",
        "sdk_log_level",
        "socks5_host",
        "socks5_port",
    }
)


def _normalize_config_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(kwargs) - _VALID_CONFIG_KEYS)
    if unknown:
        raise TypeError(
            f"xbbg.configure() got unexpected keyword argument(s): {', '.join(unknown)}. "
            f"See EngineConfig() for available fields."
        )
    return dict(kwargs)


def configure(
    config=None,
    **kwargs,
) -> None:
    """Configure the xbbg engine before first use.

    This function must be called before any Bloomberg request is made.
    If called after the engine has started, the existing engine is shut
    down and will restart with the new config on next use.

    Can be called with an EngineConfig object, keyword arguments, or both
    (kwargs override config fields). All defaults come from Rust.

    See ``EngineConfig()`` for available fields and their defaults::

        >>> from xbbg import EngineConfig
        >>> EngineConfig()
        EngineConfig(host='localhost', port=8194, request_pool_size=2,
                     subscription_pool_size=1, ...)

    Args:
        config: An EngineConfig object with all settings.
        **kwargs: Override individual fields (host, port, request_pool_size,
            subscription_pool_size, runtime_worker_threads,
            max_subscription_sessions, shard_requests, shard_threshold,
            shard_chunk_size, shard_max_concurrent, field_cache_path,
            auth_method, app_name, user_id, ip_address, token, etc.). The closed
            string fields are ``validation_mode`` (``"disabled"`` (default),
            ``"lenient"``, or ``"strict"``), ``overflow_policy``
            (``"drop_newest"`` (default) or ``"block"``), and
            ``sdk_log_level`` (``"off"`` (default), ``"fatal"``, ``"error"``,
            ``"warn"``, ``"info"``, ``"debug"``, or ``"trace"``).

    Raises:
        TypeError: If an unknown keyword argument is passed.
        ValueError: If `num_start_attempts` is less than 1.
        RuntimeWarning: If called after the engine has already started
            (the existing engine is shut down and will restart with the new config).

    Example::

        import xbbg

        # Option 1: Using keyword arguments (most common)
        xbbg.configure(request_pool_size=4, subscription_pool_size=2)
        # Opt-in sharding for wide multi-security BDP/BDH requests
        xbbg.configure(
            request_pool_size=6,
            shard_requests=True,
            shard_threshold=20,
            shard_chunk_size=16,
            shard_max_concurrent=4,
        )


        # Option 2: Using EngineConfig object
        from xbbg import EngineConfig

        xbbg.configure(EngineConfig(request_pool_size=4))

        # Option 3: EngineConfig + overrides
        cfg = EngineConfig(request_pool_size=4)
        xbbg.configure(cfg, subscription_pool_size=2)

        # Option 4: B-PIPE / SAPI authentication
        xbbg.configure(
            host="bpipe-host",
            port=8195,
            auth_method="manual",
            app_name="my-app",
            user_id="123456",
            ip_address="10.0.0.1",
            num_start_attempts=5,
            auto_restart_on_disconnection=False,
        )

        # Option 5: Custom field cache location
        xbbg.configure(field_cache_path="/data/bloomberg/field_cache.json")
    """
    global _config, _engine

    normalized = _normalize_config_kwargs(kwargs)

    if (num_start_attempts := normalized.get("num_start_attempts")) is not None and num_start_attempts < 1:
        raise ValueError("num_start_attempts must be at least 1")

    from . import _core

    configured = config if config is not None else _core.PyEngineConfig(**normalized)
    with _engine_lock:
        if config is not None:
            for key, value in normalized.items():
                setattr(configured, key, value)
        previous_engine = _engine
        _engine = None
        _config = configured

    if previous_engine is not None:
        warnings.warn(
            "xbbg.configure() called after engine was already started. "
            "The existing engine has been shut down and will restart with new config on next use. "
            "To avoid this, call configure() before any Bloomberg request, "
            "or use xbbg.Engine(...) for scoped configuration.",
            RuntimeWarning,
            stacklevel=2,
        )
        previous_engine.signal_shutdown()

    logger.info("Engine configured: %s", configured)


def set_backend(backend: Backend | str | None) -> None:
    """Set the default DataFrame backend for all xbbg functions.

    Args:
        backend: The backend to use. Can be a Backend enum or string:
            - Backend.NATIVE / "native": Return xbbg native Arrow carrier object
            - Backend.PYARROW / "pyarrow": Return pyarrow Table
            - Backend.NARWHALS / "narwhals": Return narwhals DataFrame
            - Backend.NARWHALS_LAZY / "narwhals_lazy": Return narwhals LazyFrame
            - Backend.PANDAS / "pandas": Return pandas DataFrame
            - Backend.POLARS / "polars": Return polars DataFrame
            - Backend.POLARS_LAZY / "polars_lazy": Return polars LazyFrame
            - Backend.DUCKDB / "duckdb": Return DuckDB relation (lazy)
            - None: Auto-select the first available default backend

    Example::

        import xbbg
        from xbbg import Backend

        xbbg.set_backend(Backend.POLARS)
        df = xbbg.bdh("AAPL US Equity", "PX_LAST")  # Returns polars.DataFrame

        # Use lazy evaluation for deferred computation
        xbbg.set_backend(Backend.POLARS_LAZY)
        lf = xbbg.bdh("AAPL US Equity", "PX_LAST")  # Returns polars.LazyFrame
        df = lf.collect()  # Materialize when ready

        # String also works
        xbbg.set_backend("pandas")
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
    """Get the current default DataFrame backend."""
    return _default_backend


def _resolve_backend(backend: Backend | str | None) -> Backend | None:
    """Resolve per-request backend with global fallback."""
    return _backend_resolve_backend(backend, _default_backend)


def _effective_backend(backend: Backend | str | None) -> Backend:
    """Resolve backend at the facade boundary, honoring set_backend()."""
    return _backend_effective_backend(backend, _default_backend)


def _convert_result_backend(frame: Any, backend: Backend | str | None) -> DataFrameResult:
    """Convert Arrow output after applying the facade-level backend default."""
    return convert_backend_frame(frame, _effective_backend(backend))


async def _resolve_field_types_cached(
    fields: list[str],
    overrides: dict[str, str] | None,
    default_type: str,
) -> dict[str, str]:
    """Resolve Bloomberg field types with a bounded Python-side memo."""
    key = (tuple(fields), default_type)
    with _FIELD_TYPE_RESOLUTION_CACHE_LOCK:
        generation = _FIELD_TYPE_RESOLUTION_CACHE_GENERATION
        cached = _FIELD_TYPE_RESOLUTION_CACHE.get(key)

    if cached is None:
        fetched = await _get_engine().resolve_field_types(fields, None, default_type)
        with _FIELD_TYPE_RESOLUTION_CACHE_LOCK:
            if generation != _FIELD_TYPE_RESOLUTION_CACHE_GENERATION:
                cached = dict(fetched)
            else:
                cached = _FIELD_TYPE_RESOLUTION_CACHE.get(key)
                if cached is None:
                    if len(_FIELD_TYPE_RESOLUTION_CACHE) >= _FIELD_TYPE_RESOLUTION_CACHE_MAXSIZE:
                        _FIELD_TYPE_RESOLUTION_CACHE.pop(next(iter(_FIELD_TYPE_RESOLUTION_CACHE)))
                    cached = dict(fetched)
                    _FIELD_TYPE_RESOLUTION_CACHE[key] = cached
    resolved = dict(cached)
    if overrides:
        resolved.update(overrides)
    return resolved


def _get_engine(*, engine: Engine | None = None):
    """Get the active engine: explicit arg > contextvar scope > global singleton."""
    if engine is not None:
        return engine._py_engine

    scoped = _active_engine.get()
    if scoped is not None:
        return scoped._py_engine

    global _engine
    with _engine_lock:
        if _engine is None:
            from . import _core

            if _config is not None:
                logger.debug("Creating PyEngine with config: %s", _config)
                _engine = _core.PyEngine.with_config(_config)
            else:
                logger.debug("Creating PyEngine with default config")
                _engine = _core.PyEngine()
            logger.info("PyEngine connected to Bloomberg")
        return _engine


def _coerce_server_snapshot(value: Any) -> tuple[tuple[str, int], ...]:
    if not value:
        return ()

    servers: list[tuple[str, int]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        host, port = item
        try:
            servers.append((str(host), int(port)))
        except (TypeError, ValueError):
            continue
    return tuple(servers)


def _snapshot_request_environment() -> RequestEnvironment:
    scoped = _active_engine.get()
    if scoped is not None:
        return _request_environment_from_config(
            getattr(scoped, "_config_snapshot", None),
            "scoped_engine",
        )

    with _engine_lock:
        config = _config
        source = "global_config" if config is not None else "default_engine"
    return _request_environment_from_config(config, source)


def _request_environment_from_config(config: Any, source: str) -> RequestEnvironment:
    if config is None:
        return RequestEnvironment(source=source, host="localhost", port=8194)

    host = getattr(config, "host", None)
    port = getattr(config, "port", None)
    servers = _coerce_server_snapshot(getattr(config, "servers", None))
    if not servers and host is not None and port is not None:
        try:
            servers = ((str(host), int(port)),)
        except (TypeError, ValueError):
            servers = ()

    return RequestEnvironment(
        source=source,
        host=str(host) if host is not None else None,
        port=int(port) if port is not None else None,
        servers=servers,
        zfp_remote=getattr(config, "zfp_remote", None),
        auth_method=getattr(config, "auth_method", None),
        app_name=getattr(config, "app_name", None),
        user_id=getattr(config, "user_id", None),
        validation_mode=getattr(config, "validation_mode", None),
    )


def _normalize_tickers(tickers: str | Sequence[str]) -> list[str]:
    """Normalize ticker input to a list of strings."""
    if isinstance(tickers, str):
        return [tickers]
    return list(tickers)


def _normalize_fields(fields: str | Sequence[str] | None) -> list[str]:
    """Normalize field input to a list of strings."""
    if fields is None:
        return ["PX_LAST"]
    if isinstance(fields, str):
        return [fields]
    return list(fields)


_SUBSCRIPTION_IDENTIFIER_PREFIXES = ("ticker/", "figi/", "isin/", "cusip/", "sedol/")


def _normalize_service_topic(service: Service, ticker: str, label: str) -> str:
    """Normalize a security identifier into an explicit Bloomberg service topic."""
    topic = ticker.strip()
    service_uri = service.value
    if topic.startswith("//"):
        if not topic.startswith(f"{service_uri}/"):
            raise ValueError(f"{label} topic must start with {service_uri}/, got {ticker!r}")
        return topic
    if topic.startswith("/"):
        return f"{service_uri}{topic}"

    lower_topic = topic.lower()
    if lower_topic.startswith(_SUBSCRIPTION_IDENTIFIER_PREFIXES):
        return f"{service_uri}/{topic}"

    return f"{service_uri}/ticker/{topic}"


def _normalize_service_topics(service: Service, tickers: str | Sequence[str], label: str) -> list[str]:
    """Normalize identifiers into explicit Bloomberg service topics."""
    return [_normalize_service_topic(service, ticker, label) for ticker in _normalize_tickers(tickers)]


def _normalize_mktbar_topics(tickers: str | Sequence[str]) -> list[str]:
    """Normalize market-bar identifiers into explicit service topics."""
    return _normalize_service_topics(Service.MKTBAR, tickers, "market-bar")


def _normalize_mktvwap_topics(tickers: str | Sequence[str]) -> list[str]:
    """Normalize market-VWAP identifiers into explicit service topics."""
    return _normalize_service_topics(Service.MKTVWAP, tickers, "market-VWAP")


def _get_subscription_option(options: Sequence[str] | None, name: str) -> str | None:
    """Return a subscription option value by case-insensitive option name."""
    for option in options or []:
        key, separator, value = option.partition("=")
        if key.strip().lower() != name.lower():
            continue
        if not separator or not value.strip():
            raise ValueError(f"{name} option must be provided as {name}=<value>")
        return value.strip()
    return None


def _subscription_option_key(option: str) -> str:
    """Return the case-insensitive key for a Bloomberg subscription option."""
    return option.strip().lstrip("&").partition("=")[0].strip().lower()


def _normalize_subscription_options(
    options: Sequence[str] | None,
    *,
    conflate: bool = False,
    service: str | None = None,
) -> list[str] | None:
    """Normalize high-level subscription options before passing them to Bloomberg."""
    if options is None and not conflate:
        return None

    normalized: list[str] = []
    for option in options or []:
        clean_option = option.strip()
        if not clean_option:
            continue
        if clean_option.startswith("&"):
            clean_option = clean_option[1:].strip()
        normalized.append(clean_option)

    if conflate:
        effective_service = service or Service.MKTDATA.value
        if effective_service != Service.MKTDATA.value:
            raise ValueError("conflate=True is only supported for //blp/mktdata subscriptions")
        if any(_subscription_option_key(option) == "interval" for option in normalized):
            raise ValueError(
                "conflate=True cannot be combined with interval options; intervalization overrides conflation"
            )
        if not any(_subscription_option_key(option) == "conflate" for option in normalized):
            normalized.append("conflate")

    return normalized


def _validate_mktbar_bar_size(bar_size: int) -> None:
    """Validate Bloomberg's documented market-bar interval bounds."""
    if not 1 <= bar_size <= 1440:
        raise ValueError("bar_size must be between 1 and 1440 minutes")


def _validate_mktbar_options(options: Sequence[str] | None) -> None:
    """Validate Bloomberg's required market-bar subscription options."""
    raw_bar_size = _get_subscription_option(options, "bar_size")
    if raw_bar_size is None:
        raise ValueError("//blp/mktbar subscriptions require a bar_size option")
    try:
        bar_size = int(raw_bar_size)
    except ValueError as exc:
        raise ValueError("bar_size must be an integer number of minutes") from exc
    _validate_mktbar_bar_size(bar_size)


# Cache for valid request elements per (service, operation)
_VALID_ELEMENTS_CACHE: dict[tuple[str, str], set[str]] = {}

_ELEMENT_KEY_ALIASES: dict[str, str] = {
    # Bloomberg request element aliases inherited from xbbg 0.x / Excel BDH conventions.
    "PeriodAdj": "periodicityAdjustment",
    "PerAdj": "periodicityAdjustment",
    "Period": "periodicitySelection",
    "Per": "periodicitySelection",
    "Currency": "currency",
    "Curr": "currency",
    "FX": "currency",
    "Days": "nonTradingDayFillOption",
    "Fill": "nonTradingDayFillMethod",
    "Points": "maxDataPoints",
    "Quote": "overrideOption",
    "QuoteType": "pricingOption",
    "QtTyp": "pricingOption",
    "CshAdjNormal": "adjustmentNormal",
    "CshAdjAbnormal": "adjustmentAbnormal",
    "CapChg": "adjustmentSplit",
    "UseDPDF": "adjustmentFollowDPDF",
    "Calendar": "calendarCodeOverride",
    # v1 additions requested in issue #301.
    "BarSz": "interval",
    "BarSize": "interval",
    "BarTp": "eventType",
    "BarType": "eventType",
    "IncludeExchangeCodes": "includeExchangeCodes",
}

_PRESENTATION_KEY_ALIASES: dict[str, str] = {
    # Excel-only output-shape controls. These do not map to Bloomberg request
    # elements; typed endpoints consume them before request routing and apply
    # the shape change locally after Bloomberg returns raw data.
    "Dts": "show_date",
    "Dates": "show_date",
    "DtFmt": "date_format",
    "DateFormat": "date_format",
    "Sort": "sort",
    "Orientation": "orientation",
    "Direction": "orientation",
    "Dir": "orientation",
}

_PRESENTATION_VALUE_ALIASES: dict[str, dict[Any, Any]] = {
    "show_date": {
        "Show": True,
        "S": True,
        True: True,
        "True": True,
        "Hide": False,
        "H": False,
        False: False,
        "False": False,
    },
    "date_format": {
        "B": "BOTH",
        "Both": "BOTH",
        "P": "PERIODIC",
        "Periodic": "PERIODIC",
        "D": "DATE",
        "Date": "DATE",
    },
    "sort": {
        "C": "ASCENDING",
        "A": "ASCENDING",
        "Ascend": "ASCENDING",
        "Chronological": "ASCENDING",
        False: "ASCENDING",
        "False": "ASCENDING",
        "R": "DESCENDING",
        "D": "DESCENDING",
        "Descend": "DESCENDING",
        "Reverse": "DESCENDING",
        True: "DESCENDING",
        "True": "DESCENDING",
    },
    "orientation": {
        "H": "HORIZONTAL",
        "Horizontal": "HORIZONTAL",
        "V": "VERTICAL",
        "Vertical": "VERTICAL",
    },
}

_ELEMENT_VALUE_ALIASES: dict[str, dict[Any, Any]] = {
    "periodicityAdjustment": {
        "A": "ACTUAL",
        "C": "CALENDAR",
        "F": "FISCAL",
    },
    "periodicitySelection": {
        "D": "DAILY",
        "W": "WEEKLY",
        "M": "MONTHLY",
        "Q": "QUARTERLY",
        "S": "SEMI_ANNUALLY",
        "Y": "YEARLY",
    },
    "nonTradingDayFillOption": {
        "N": "NON_TRADING_WEEKDAYS",
        "W": "NON_TRADING_WEEKDAYS",
        "Weekdays": "NON_TRADING_WEEKDAYS",
        "C": "ALL_CALENDAR_DAYS",
        "A": "ALL_CALENDAR_DAYS",
        "All": "ALL_CALENDAR_DAYS",
        "T": "ACTIVE_DAYS_ONLY",
        "Trading": "ACTIVE_DAYS_ONLY",
    },
    "nonTradingDayFillMethod": {
        "C": "PREVIOUS_VALUE",
        "P": "PREVIOUS_VALUE",
        "Previous": "PREVIOUS_VALUE",
        "B": "NIL_VALUE",
        "Blank": "NIL_VALUE",
        "NA": "NIL_VALUE",
    },
    "overrideOption": {
        "A": "OVERRIDE_OPTION_GPA",
        "G": "OVERRIDE_OPTION_GPA",
        "Average": "OVERRIDE_OPTION_GPA",
        "C": "OVERRIDE_OPTION_CLOSE",
        "Close": "OVERRIDE_OPTION_CLOSE",
    },
    "pricingOption": {
        "P": "PRICING_OPTION_PRICE",
        "Price": "PRICING_OPTION_PRICE",
        "Y": "PRICING_OPTION_YIELD",
        "Yield": "PRICING_OPTION_YIELD",
    },
    "eventType": {
        "B": "BID",
        "Bid": "BID",
        "A": "ASK",
        "Ask": "ASK",
        "T": "TRADE",
        "Trade": "TRADE",
    },
}

_KNOWN_ALIAS_ELEMENT_KEYS = frozenset(_ELEMENT_KEY_ALIASES) | frozenset(_ELEMENT_KEY_ALIASES.values())


def _normalize_element_alias(key: str, value: Any) -> tuple[str, Any]:
    """Return canonical Bloomberg element key and enum value for a caller alias."""
    canonical_key = _ELEMENT_KEY_ALIASES.get(key, key)
    value_aliases = _ELEMENT_VALUE_ALIASES.get(canonical_key, {})
    try:
        routed_value = value_aliases.get(value, value)
    except TypeError:
        routed_value = value
    if canonical_key == "maxDataPoints" and isinstance(routed_value, str):
        try:
            routed_value = int(routed_value)
        except ValueError:
            pass
    return canonical_key, routed_value


def _is_alias_element_key(original_key: str, canonical_key: str) -> bool:
    """Return whether a key is part of the supported request-element alias table."""
    return original_key in _ELEMENT_KEY_ALIASES or canonical_key in _KNOWN_ALIAS_ELEMENT_KEYS


def _is_presentation_alias_key(key: str) -> bool:
    """Return whether a key is an Excel-only presentation alias, not a Bloomberg element."""
    return key in _PRESENTATION_KEY_ALIASES or key in _PRESENTATION_KEY_ALIASES.values()


@dataclass(frozen=True, slots=True)
class _PresentationOptions:
    show_date: bool | None = None
    date_format: str | None = None
    sort: str | None = None
    orientation: str | None = None

    @property
    def enabled(self) -> bool:
        return any(value is not None for value in (self.show_date, self.date_format, self.sort, self.orientation))


def _pop_element_alias(kwargs: dict[str, Any], canonical_key: str) -> Any | None:
    """Pop the first kwarg alias that resolves to *canonical_key*, returning its normalized value."""
    for key in list(kwargs):
        routed_key, routed_value = _normalize_element_alias(key, kwargs[key])
        if routed_key == canonical_key:
            kwargs.pop(key)
            return routed_value
    return None


def _normalize_presentation_value(key: str, value: Any) -> Any:
    """Return canonical value for a presentation-layer option."""
    value_aliases = _PRESENTATION_VALUE_ALIASES.get(key, {})
    try:
        if value in value_aliases:
            return value_aliases[value]
    except TypeError:
        return value

    if isinstance(value, str):
        value_lower = value.lower()
        for alias, normalized in value_aliases.items():
            if isinstance(alias, str) and alias.lower() == value_lower:
                return normalized

    return value


def _normalize_presentation_alias(key: str, value: Any) -> tuple[str, Any]:
    canonical_key = _PRESENTATION_KEY_ALIASES.get(key, key)
    return canonical_key, _normalize_presentation_value(canonical_key, value)


def _pop_presentation_aliases(kwargs: dict[str, Any]) -> _PresentationOptions:
    """Remove presentation aliases from kwargs and return normalized options."""
    options: dict[str, Any] = {}
    for key in list(kwargs):
        canonical_key, value = _normalize_presentation_alias(key, kwargs[key])
        if canonical_key in _PRESENTATION_VALUE_ALIASES:
            kwargs.pop(key)
            options[canonical_key] = value

    return _PresentationOptions(
        show_date=options.get("show_date"),
        date_format=options.get("date_format"),
        sort=options.get("sort"),
        orientation=options.get("orientation"),
    )


def _periodicity_selection(elements: Sequence[tuple[str, Any]]) -> str | None:
    for key, value in elements:
        if key == "periodicitySelection":
            return str(value)
    return None


def _presentation_format(fmt: Format | None, presentation: _PresentationOptions) -> Format | None:
    if fmt is not None:
        return fmt
    if presentation.orientation == "HORIZONTAL":
        return Format.SEMI_LONG
    if presentation.orientation == "VERTICAL":
        return Format.LONG
    return fmt


def _apply_historical_presentation(
    table: Any,
    presentation: _PresentationOptions,
    *,
    periodicity: str | None,
) -> Any:
    """Apply Excel-style BDH presentation options through native Arrow operations."""
    return table.apply_historical_presentation(
        presentation.show_date,
        presentation.date_format,
        presentation.sort,
        periodicity,
    )


async def _aget_valid_elements(service: str, operation: str) -> set[str]:
    """Get valid request element names from schema cache (async).

    Returns cached set of valid element names for the operation.
    Falls back to empty set if schema not available.
    """
    cache_key = (service, operation)
    if cache_key in _VALID_ELEMENTS_CACHE:
        return _VALID_ELEMENTS_CACHE[cache_key]

    try:
        engine = _get_engine()
        elements = await engine.list_valid_elements(service, operation)
        valid = set(elements) if elements else set()
        _VALID_ELEMENTS_CACHE[cache_key] = valid
        return valid
    except Exception:
        logger.debug("Schema lookup failed for %s/%s, using empty set", service, operation, exc_info=True)
        return set()


# ISO date pattern for the override-path value-based normalizer. Matches the
# canonical wire formats Bloomberg accepts on date-typed override fields:
# ``YYYY-MM-DD`` and ``YYYYMMDD``. Anything else (US ``MM/DD/YYYY`` etc.) is
# left untouched here; dedicated typed parameters reject ambiguous strings.
_OVERRIDE_DATE_VALUE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}|\d{8})$")


def _normalize_override_value(value: Any) -> str:
    """Normalize a Bloomberg override value with date-aware duck typing.

    The override path passes user kwargs through to Bloomberg without per-field
    type metadata, so we inspect the *value* shape:

    - ``datetime.date`` / ``datetime.datetime`` -> formatted as ``YYYYMMDD``.
    - duck-typed ``pd.Timestamp`` (``hasattr(value, "to_pydatetime")``)
      -> coerced and formatted.
    - ``str`` matching ISO date or Bloomberg-native: normalized to
      ``YYYYMMDD`` so callers can pass either form interchangeably.
    - anything else: ``str(value)`` (existing behaviour).

    Bool is intentionally short-circuited so that ``True``/``False`` survive as
    ``"True"`` / ``"False"`` (some Bloomberg overrides expect those literals).
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (date, datetime)):
        formatted = _fmt_date(value)
        return formatted if formatted is not None else str(value)
    if hasattr(value, "to_pydatetime"):
        try:
            formatted = _fmt_date(value)
        except (TypeError, ValueError):
            return str(value)
        if formatted is not None:
            return formatted
    if isinstance(value, str) and _OVERRIDE_DATE_VALUE_RE.match(value.strip()):
        try:
            formatted = _fmt_date(value)
        except (TypeError, ValueError):
            return value
        if formatted is not None:
            return formatted
    return str(value)


_OVR_SOURCE_TYPE_ERROR = "ovr() expects mappings, OverrideSpec, or iterables of (name, value) pairs"
_OVERRIDES_TYPE_ERROR = "overrides must be a mapping, OverrideSpec, or a sequence of (name, value) pairs"


@dataclass(frozen=True)
class OverrideSpec(Mapping[str, str]):
    pairs: tuple[tuple[str, str], ...]
    security_pairs: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = ()

    def __iter__(self):
        return (key for key, _value in self.pairs)

    def __len__(self) -> int:
        return len(self.pairs)

    def __getitem__(self, key: str) -> str:
        for pair_key, pair_value in self.pairs:
            if pair_key == key:
                return pair_value
        raise KeyError(key)

    def items(self):
        return self.pairs

    def to_pairs(self) -> list[tuple[str, str]]:
        return list(self.pairs)

    def to_dict(self) -> dict[str, str]:
        return dict(self.pairs)

    def to_security_pairs(self) -> list[tuple[str, list[tuple[str, str]]]]:
        return [(security, list(overrides)) for security, overrides in self.security_pairs]

    def for_security(self, security: str, *sources: Any, **kwargs: Any) -> OverrideSpec:
        return ovr(self, {security: ovr(*sources, **kwargs)})

    def __or__(self, other: Any) -> OverrideSpec:
        try:
            return ovr(self, other)
        except TypeError:
            return cast("Any", NotImplemented)

    def __ror__(self, other: Any) -> OverrideSpec:
        try:
            return ovr(other, self)
        except TypeError:
            return cast("Any", NotImplemented)


def _iter_source_pairs(value: Any, error_message: str) -> Iterable[tuple[Any, Any]]:
    if isinstance(value, OverrideSpec):
        return value.pairs
    if isinstance(value, Mapping):
        return value.items()
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(error_message)
    if isinstance(value, Iterable):
        return _validated_source_pairs(value, error_message)
    raise TypeError(error_message)


def _validated_source_pairs(value: Iterable[Any], error_message: str) -> Iterable[tuple[Any, Any]]:
    for entry in value:
        if isinstance(entry, (str, bytes, bytearray)):
            raise TypeError(error_message)
        try:
            key, item_value = entry
        except (TypeError, ValueError):
            raise TypeError(error_message) from None
        yield key, item_value


def _is_per_security_override_source(value: Any) -> bool:
    if isinstance(value, OverrideSpec):
        return True
    if isinstance(value, Mapping):
        return True
    if isinstance(value, (str, bytes, bytearray)):
        return False
    return isinstance(value, Iterable)


def _merge_security_pairs(
    merged: dict[str, dict[str, str]],
    order: list[str],
    security: str,
    source: Any,
) -> None:
    spec = ovr(source)
    pairs = spec.to_pairs()
    if not pairs:
        return
    if security not in merged:
        merged[security] = {}
        order.append(security)
    merged[security].update(pairs)


def ovr(*sources: Any, **kwargs: Any) -> OverrideSpec:
    merged: dict[str, str] = {}
    security_merged: dict[str, dict[str, str]] = {}
    security_order: list[str] = []

    for source in sources:
        if isinstance(source, OverrideSpec):
            merged.update(source.pairs)
            for security, overrides in source.security_pairs:
                _merge_security_pairs(security_merged, security_order, security, overrides)
            continue
        for key, value in _iter_source_pairs(source, _OVR_SOURCE_TYPE_ERROR):
            key_str = str(key)
            if _is_per_security_override_source(value):
                _merge_security_pairs(security_merged, security_order, key_str, value)
            else:
                merged[key_str] = _normalize_override_value(value)

    for key, value in kwargs.items():
        merged[str(key)] = _normalize_override_value(value)

    security_pairs = tuple((security, tuple(security_merged[security].items())) for security in security_order)
    return OverrideSpec(tuple(merged.items()), security_pairs)


def _iter_override_pairs(value: Any) -> Iterable[tuple[Any, Any]]:
    if isinstance(value, OverrideSpec) and value.security_pairs:
        raise TypeError("per-security overrides are only supported by bdp(), bdh(), and bds()")
    return _iter_source_pairs(value, _OVERRIDES_TYPE_ERROR)


def _normalize_request_overrides(
    value: Any,
) -> tuple[list[tuple[str, str]] | None, list[tuple[str, list[tuple[str, str]]]] | None]:
    if value is None:
        return None, None
    spec = ovr(value)
    pairs = spec.to_pairs()
    security_pairs = spec.to_security_pairs()
    return pairs or None, security_pairs or None


async def _aroute_kwargs(
    service: str | Service,
    operation: str | Operation,
    kwargs: dict,
) -> tuple[list[tuple[str, Any]], list[tuple[str, str]]]:
    """Route kwargs to elements or overrides using schema introspection (async).

    Uses the Bloomberg schema to determine if a kwarg is:
    1. A valid request element (e.g., intervalHasSeconds, periodicitySelection)
    2. A Bloomberg field override (UPPERCASE names like GICS_SECTOR_NAME)

    Args:
        service: Bloomberg service URI
        operation: Request operation name
        kwargs: User-provided kwargs (will be modified in place)

    Returns:
        Tuple of (elements, overrides) where:
        - elements: List of (name, value) for valid request elements
        - overrides: List of (name, value) for Bloomberg field overrides
    """
    # Normalize service/operation to strings
    svc = service.value if isinstance(service, Service) else service
    op = operation.value if isinstance(operation, Operation) else operation

    # Get valid elements from schema
    valid_elements = await _aget_valid_elements(svc, op)

    elements: list[tuple[str, Any]] = []
    overrides: list[tuple[str, str]] = []

    def route_candidate(key: Any, value: Any) -> None:
        original_key = str(key)
        if _is_presentation_alias_key(original_key):
            warnings.warn(
                f"Presentation alias '{original_key}' controls Excel-style output shape and is not "
                "a Bloomberg request element; typed endpoints such as bdh() handle it locally, "
                "while low-level request routing skips it.",
                stacklevel=4,
            )
            return

        canonical_key, routed_value = _normalize_element_alias(original_key, value)

        if canonical_key in valid_elements or _is_alias_element_key(original_key, canonical_key):
            elements.append((canonical_key, routed_value))
        elif original_key.isupper() or (len(original_key) > 2 and original_key[0].isupper() and "_" in original_key):
            # Looks like a Bloomberg field override (UPPERCASE or Mixed_Case_Field).
            # Normalize date-typed values to Bloomberg-native YYYYMMDD via duck-typing
            # so callers can pass e.g. ``USER_LOCAL_TRADE_DATE=date(2023, 1, 17)``.
            overrides.append((original_key, _normalize_override_value(value)))
        elif valid_elements:
            # Schema available but key not recognized - warn and pass as element
            warnings.warn(
                f"Unknown parameter '{original_key}' for {op} - passing to Bloomberg. "
                f"Valid elements: {sorted(valid_elements)[:10]}{'...' if len(valid_elements) > 10 else ''}",
                stacklevel=4,
            )
            elements.append((canonical_key, routed_value))
        else:
            # No schema available - pass as element (Bloomberg will validate)
            elements.append((canonical_key, routed_value))

    # Handle explicit overrides dict first. Entries that are actually request-element
    # aliases (for example Points -> maxDataPoints) are routed as elements, matching 0.x.
    if "overrides" in kwargs:
        ovrd = kwargs.pop("overrides")
        for key, value in _iter_override_pairs(ovrd):
            route_candidate(key, value)

    # Route remaining kwargs
    for key in list(kwargs.keys()):
        route_candidate(key, kwargs.pop(key))

    return elements, overrides


from xbbg.ext._utils import (  # noqa: E402  (must follow services imports)
    DateLike,
    _fmt_date,
    _fmt_datetime,
)


def _core_arrow_table_class() -> type[Any]:
    from xbbg._core import ArrowTable

    return ArrowTable


def _normalize_engine_exception(exc: Exception) -> Exception:
    from . import _core
    from .exceptions import (
        BlpFieldError,
        BlpLimitError,
        BlpRequestError,
        BlpSecurityError,
        BlpSubscriptionDataLossError,
        BlpValidationError,
    )

    if isinstance(exc, _core.BlpSubscriptionDataLossError) and not isinstance(exc, BlpSubscriptionDataLossError):
        return BlpSubscriptionDataLossError(
            str(exc),
            topic=getattr(exc, "topic", ""),
            detail=getattr(exc, "detail", ""),
        )

    if isinstance(exc, _core.BlpValidationError) and not isinstance(exc, BlpValidationError):
        return BlpValidationError.from_rust_error(str(exc))

    for native_cls, public_cls in (
        (_core.BlpLimitError, BlpLimitError),
        (_core.BlpSecurityError, BlpSecurityError),
        (_core.BlpFieldError, BlpFieldError),
        (_core.BlpRequestError, BlpRequestError),
    ):
        if isinstance(exc, native_cls) and not isinstance(exc, public_cls):
            return public_cls(str(exc))

    return exc


async def _execute_request_terminal(context: RequestContext) -> DataFrameResult:
    engine = _get_engine()
    started = time.perf_counter()

    try:
        batch = await engine.request(context.to_dispatch_dict())
    except Exception as exc:
        mapped = _normalize_engine_exception(exc)
        context.elapsed_ms = (time.perf_counter() - started) * 1000
        context.error = mapped
        if mapped is exc:
            raise
        raise mapped from exc

    context.batch = batch
    context.elapsed_ms = (time.perf_counter() - started) * 1000

    logger.info(
        "bloomberg %s.%s [request_id=%s]: %d rows in %.1fms | securities=%s fields=%s",
        context.params.service,
        context.params.operation,
        context.request_id,
        batch.num_rows,
        context.elapsed_ms,
        context.securities or None,
        context.fields or None,
    )

    context.table = ensure_arrow_table(batch)
    context.frame = context.table
    return context.table


# =============================================================================
# Generic API - Power Users
# =============================================================================


async def arequest(
    service: str | Service,
    operation: str | Operation,
    *,
    request_operation: str | Operation | None = None,
    securities: str | Sequence[str] | None = None,
    security: str | None = None,
    fields: str | Sequence[str] | None = None,
    overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | OverrideSpec | None = None,
    security_overrides: Sequence[tuple[str, Sequence[tuple[str, Any]]]] | None = None,
    elements: Sequence[tuple[str, Any]] | None = None,
    start_date: DateLike = None,
    end_date: DateLike = None,
    start_datetime: DateLike = None,
    end_datetime: DateLike = None,
    event_type: str | None = None,
    event_types: Sequence[str] | None = None,
    interval: int | None = None,
    options: dict[str, Any] | Sequence[tuple[str, str]] | None = None,
    field_types: dict[str, str] | None = None,
    output: OutputMode | str = OutputMode.ARROW,
    extractor: ExtractorHint | str | None = None,
    format: Format | str | None = None,
    include_security_errors: bool = False,
    return_eids: bool = False,
    validate_fields: bool | None = None,
    backend: Backend | str | None = None,
    request_tz: str | None = None,
    output_tz: str | None = None,
    _raw: bool = False,
):
    """Async generic Bloomberg request.

    This is the low-level API for power users who need to:
    - Send requests to arbitrary Bloomberg services
    - Use operations not covered by the typed convenience functions
    - Get raw JSON responses for debugging

    For common use cases, prefer the typed functions: abdp, abdh, abds, abdib, abdtick.

    Args:
        service: Bloomberg service URI (e.g., Service.REFDATA or "//blp/refdata").
        operation: Request operation name (e.g., Operation.REFERENCE_DATA).
        request_operation: Actual Bloomberg operation name when using
            ``Operation.RAW_REQUEST`` as the low-level escape hatch.
        securities: List of security identifiers (for multi-security requests).
        security: Single security identifier (for intraday requests).
        fields: List of field names to retrieve.
        overrides: Field overrides as dict, OverrideSpec, or list of (name, value)
            tuples. Nested override mappings inside ``ovr()`` are per-security
            overrides; global pairs are merged first.
        security_overrides: Low-level per-security overrides, usually supplied by typed
            wrappers after normalizing ``ovr()`` inputs.
        elements: Additional request elements as list of (name, value) tuples.
            Used for schema-driven parameters like intervalHasSeconds, periodicitySelection.
        start_date: Start date for historical requests. Accepts ISO 8601 string,
            ``YYYYMMDD`` string, ``"today"``, ``datetime.date``,
            ``datetime.datetime``, or duck-typed ``pd.Timestamp``.
        end_date: End date for historical requests. Same accepted shapes as
            ``start_date``.
        start_datetime: Start datetime for intraday requests. Accepts ISO 8601
            string (with or without tz), ``datetime.datetime`` (naive or
            tz-aware), or ``pd.Timestamp``. Naive values use ``request_tz``.
        end_datetime: End datetime for intraday requests. Same accepted shapes
            as ``start_datetime``.
        request_tz: For intraday requests, how naive datetimes are interpreted before
            sending to Bloomberg (``UTC``, ``local``, ``exchange``, aliases, or IANA).
            Resolved and converted to UTC in the Rust engine.
        output_tz: For intraday responses, relabel the ``time`` column to this zone
            (same instants; handled in the Rust engine).
        event_type: Event type for intraday bars (TRADE, BID, ASK, etc.).
        interval: Bar interval in minutes for intraday bars.
        options: Additional Bloomberg options as dict or list of (key, value) tuples.
        field_types: Manual type overrides for fields (for future type resolution).
        output: Output format: OutputMode.ARROW (default) or OutputMode.JSON.
        extractor: Override the auto-detected extractor: ``ExtractorHint.REFDATA``,
            ``HISTDATA``, ``BULK``, ``INTRADAY_BAR``, ``INTRADAY_TICK``, ``GENERIC``,
            ``BQL``, ``BSRCH``, or ``FIELD_INFO``. Use ``ExtractorHint.BULK`` for bulk
            data fields. If None, auto-detected from operation.
        format: Output format: ``Format.LONG`` (default), ``Format.LONG_TYPED``,
            ``Format.LONG_WITH_METADATA``, or ``Format.SEMI_LONG``.
        include_security_errors: Include ``__SECURITY_ERROR__`` rows for
            failed securities on ReferenceData requests.
        return_eids: Request EID entitlement metadata for ReferenceDataRequest
            (including BDS bulk data), HistoricalDataRequest, IntradayBarRequest,
            and IntradayTickRequest.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        DataFrame/Table in the requested format.

    Example::

        # Query field metadata (//blp/apiflds service)
        df = await arequest(
            Service.APIFLDS,
            Operation.FIELD_INFO,
            fields=["PX_LAST", "VOLUME"],
        )

        # Get raw JSON for debugging
        json_table = await arequest(
            Service.REFDATA,
            Operation.REFERENCE_DATA,
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
            output=OutputMode.JSON,
        )

        # Custom Bloomberg request (power user)
        df = await arequest(
            "//blp/refdata",
            "ReferenceDataRequest",
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
        )

        # Raw request marker with explicit Bloomberg operation
        df = await arequest(
            Service.REFDATA,
            Operation.RAW_REQUEST,
            request_operation=Operation.REFERENCE_DATA,
            extractor=ExtractorHint.REFDATA,
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
        )
    """
    # Normalize inputs
    securities_list = _normalize_tickers(securities) if securities is not None else None
    fields_list = _normalize_fields(fields) if fields is not None else None

    overrides_list: list[tuple[str, str]] | None = None
    security_overrides_list: list[tuple[str, list[tuple[str, str]]]] | None = None
    elements_list: list[tuple[str, Any]] | None = None
    if security_overrides is not None:
        security_overrides_list = [
            (
                str(security_name),
                [
                    (str(key), _normalize_override_value(value))
                    for key, value in _validated_source_pairs(security_values, _OVERRIDES_TYPE_ERROR)
                ],
            )
            for security_name, security_values in security_overrides
        ]

    # Handle explicit elements parameter
    # Convert all element values to strings because the PyO3 boundary expects Vec<(String, String)>.
    # Booleans are lowercased ("true"/"false") to match Bloomberg schema expectations.
    if elements is not None:
        elements_list = [(str(k), str(v).lower() if isinstance(v, bool) else str(v)) for k, v in elements]

    if overrides is not None:
        override_tuples, override_security_overrides = _normalize_request_overrides(overrides)
        # BQL accepts query parameters as request elements. Other services keep
        # overrides as Bloomberg override tuples unless an endpoint builder says otherwise.
        service_str = service.value if isinstance(service, Service) else service
        if service_str == Service.BQLSVC.value:
            if override_tuples:
                if elements_list:
                    elements_list.extend(override_tuples)
                else:
                    elements_list = override_tuples
        else:
            overrides_list = override_tuples
        if override_security_overrides is not None:
            if security_overrides_list is None:
                security_overrides_list = override_security_overrides
            else:
                security_overrides_list.extend(override_security_overrides)

    options_list: list[tuple[str, str]] | None = None
    if options is not None:
        options_list = [(str(k), str(v)) for k, v in options.items()] if isinstance(options, dict) else list(options)

    # Normalize extractor hint
    extractor_hint: ExtractorHint | None = None
    if extractor is not None:
        extractor_hint = ExtractorHint(extractor) if isinstance(extractor, str) else extractor

    # Normalize format
    format_hint: Format | None = None
    if format is not None:
        format_hint = Format(format) if isinstance(format, str) else format

    # Build and validate params
    params = RequestParams(
        service=service,
        operation=operation,
        request_operation=request_operation,
        securities=securities_list,
        security=security,
        fields=fields_list,
        overrides=overrides_list,
        security_overrides=security_overrides_list,
        elements=elements_list,
        start_date=_fmt_date(start_date),
        end_date=_fmt_date(end_date),
        start_datetime=_fmt_datetime(start_datetime, default_tz=None),
        end_datetime=_fmt_datetime(end_datetime, default_tz=None),
        event_type=event_type,
        event_types=list(event_types) if event_types else None,
        interval=interval,
        options=options_list,
        field_types=field_types,
        output=OutputMode(output) if isinstance(output, str) else output,
        extractor=extractor_hint,
        format=format_hint,
        include_security_errors=include_security_errors,
        return_eids=return_eids,
        validate_fields=validate_fields,
        request_tz=request_tz,
        output_tz=output_tz,
    )
    params.validate()

    request_id = f"req-{time.time_ns()}"
    context = RequestContext(
        request_id=request_id,
        params=params,
        backend=backend,
        raw=_raw,
        securities=list(securities_list or []),
        fields=list(fields_list or []),
        environment=_snapshot_request_environment(),
    )

    try:
        result = await _run_request_middleware(context, _execute_request_terminal)
    except Exception as exc:
        context.error = exc
        raise
    # Low-level arequest() defaults to the raw Arrow output requested by OutputMode.ARROW.
    # High-level generated endpoints call arequest(_raw=True) and then apply their own
    # public backend conversion, so their default remains the Narwhals dataframe contract.
    if _raw:
        return result
    try:
        table_result = ensure_arrow_table(result)
    except TypeError:
        return result
    effective_backend = _resolve_backend(backend)
    if effective_backend is None and params.output == OutputMode.ARROW:
        context.frame = table_result
        return table_result
    context.frame = _convert_result_backend(table_result, effective_backend)
    return context.frame


# =============================================================================
# Async API - Typed Convenience Functions
# =============================================================================


async def abdp(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    include_security_errors: bool = False,
    return_eids: bool = False,
    validate_fields: bool | None = None,
    **kwargs,
):
    """Async Bloomberg reference data (BDP).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields to query.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, field, value (strings)
            - Format.LONG_TYPED: ticker, field, value_f64, value_i64, etc.
            - Format.LONG_WITH_METADATA: ticker, field, value, dtype
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        include_security_errors: Include ``__SECURITY_ERROR__`` rows for
            securities that Bloomberg rejected.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame in long format with columns: ticker, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdp("AAPL US Equity", ["PX_LAST", "VOLUME"])

        # Concurrent requests
        dfs = await asyncio.gather(
            abdp("AAPL US Equity", "PX_LAST"),
            abdp("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdp"], locals())


async def abdh(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    start_date: DateLike = None,
    end_date: DateLike = "today",
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    validate_fields: bool | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg historical data (BDH).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields. Defaults to ['PX_LAST'].
        start_date: Start date. Defaults to 8 weeks before end_date.
        end_date: End date. Defaults to 'today'.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, date, field, value (strings)
            - Format.LONG_TYPED: ticker, date, field, value_f64, value_i64, etc.
            - Format.LONG_WITH_METADATA: ticker, date, field, value, dtype
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional overrides and infrastructure options.
            adjust: Adjustment type ('all', 'dvd', 'split', '-', None).

    Returns:
        DataFrame in long format with columns: ticker, date, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdh("AAPL US Equity", "PX_LAST", start_date="2024-01-01")

        # Concurrent requests
        dfs = await asyncio.gather(
            abdh("AAPL US Equity", "PX_LAST"),
            abdh("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdh"], locals())


async def abds(
    tickers: str | Sequence[str],
    flds: str,
    *,
    backend: Backend | str | None = None,
    validate_fields: bool | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg bulk data (BDS).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field name (bulk fields return multiple rows).
        backend: DataFrame backend to return. If None, uses global default.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame with one row per Bloomberg bulk row. The only xbbg-added
        columns are ``ticker`` and ``field``; bulk subfield columns preserve
        Bloomberg's labels exactly as emitted, including spaces, punctuation,
        and case. Higher-level helpers must rename their own semantic outputs.

    Example::

        df = await abds("AAPL US Equity", "DVD_Hist_All")
        df = await abds("SPX Index", "INDX_MEMBERS", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abds"], locals())


async def abdib(
    ticker: str,
    dt: DateLike = None,
    session: str = "allday",
    typ: str = "TRADE",
    *,
    start_datetime: DateLike = None,
    end_datetime: DateLike = None,
    interval: int = 1,
    backend: Backend | str | None = None,
    request_tz: str | None = None,
    output_tz: str | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg intraday bar data (BDIB).

    Args:
        ticker: Ticker name.
        dt: Date to download (for single-day requests).
        session: Trading session name. Ignored when start_datetime/end_datetime provided.
        typ: Event type (TRADE, BID, ASK, etc.).
        start_datetime: Explicit start datetime for multi-day requests.
        end_datetime: Explicit end datetime for multi-day requests.
        interval: Bar interval in minutes (default: 1), or seconds if intervalHasSeconds=True.
        backend: DataFrame backend to return. If None, uses global default.
        request_tz: How naive ``start_datetime`` / ``end_datetime`` (and full-day ``dt`` window)
            are interpreted before Bloomberg: ``UTC`` (default when omitted), ``local``,
            ``exchange`` (uses this ticker), ``NY``/``LN``/``TK``/``HK``, another ticker string,
            or an IANA zone. Conversion to UTC is done in the Rust engine.
        output_tz: Relabel the ``time`` column to this zone (same instants; Rust engine).
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional Bloomberg options (e.g., intervalHasSeconds,
            gapFillInitialBar, or 0.x request-element aliases such as ``Points=1``).
            Pass true Bloomberg field overrides via ``overrides={...}``.

    Returns:
        DataFrame with intraday bar data.

    Example::

        # 1-minute bars (default)
        df = await abdib("AAPL US Equity", dt="2024-12-01")

        # 5-minute bars with explicit datetime range
        df = await abdib(
            "AAPL US Equity",
            start_datetime="2024-12-01 09:30",
            end_datetime="2024-12-01 16:00",
            interval=5,
        )

        # 10-second bars
        df = await abdib("AAPL US Equity", dt="2024-12-01", interval=10, intervalHasSeconds=True)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdib"], locals())


async def abdtick(
    ticker: str,
    start_datetime: DateLike,
    end_datetime: DateLike,
    *,
    event_types: Sequence[str] | None = None,
    backend: Backend | str | None = None,
    request_tz: str | None = None,
    output_tz: str | None = None,
    return_eids: bool = False,
    **kwargs,
):
    """Async Bloomberg tick data (BDTICK).

    Args:
        ticker: Ticker name.
        start_datetime: Start datetime.
        end_datetime: End datetime.
        event_types: Event types to retrieve. Defaults to ["TRADE"].
            Options: TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK.
        backend: DataFrame backend to return. If None, uses global default.
        request_tz: How naive datetimes are interpreted before Bloomberg (see ``abdib``).
        output_tz: Relabel ``time`` column (same instants; Rust engine).
        return_eids: Request EID metadata. Native Arrow results expose
            ``.eid_data``; pandas stores it in ``attrs["xbbg_eid_data"]``;
            PyArrow preserves ``xbbg.eid_data`` in schema metadata.
            Polars and DuckDB have no stable metadata channel.
        **kwargs: Additional Bloomberg options. Schema-recognized request elements
            and 0.x request-element aliases such as ``Points=1`` may be passed as
            individual keyword arguments. Pass true Bloomberg field overrides via
            ``overrides={...}``.

    Returns:
        DataFrame with tick data.

    Example::

        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00")
        df = await abdtick(
            "AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", event_types=["TRADE", "BID", "ASK"]
        )
        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdtick"], locals())


# =============================================================================
# Sync API - Convenience Wrappers
# =============================================================================


@dataclass(frozen=True)
class _EndpointPlan:
    request_kwargs: dict[str, Any]
    backend: Backend | str | None
    postprocess: Callable[[Any], DataFrameResult] | None = None
    service: Service | None = None
    operation: Operation | None = None
    extractor: ExtractorHint | None = None


@dataclass(frozen=True)
class _GeneratedEndpointSpec:
    async_name: str
    sync_name: str
    service: Service
    operation: Operation
    builder: Callable[[dict[str, Any]], Awaitable[_EndpointPlan] | _EndpointPlan]
    extractor: ExtractorHint | None = None


_GENERATED_ENDPOINT_SPECS: dict[str, _GeneratedEndpointSpec] = {}


def _strip_signature_annotations(func: Callable[..., Any]) -> str:
    signature = inspect.signature(func)
    stripped_params = [param.replace(annotation=inspect._empty) for param in signature.parameters.values()]
    stripped = signature.replace(parameters=stripped_params, return_annotation=inspect._empty)
    return str(stripped)


def _is_notebook_context() -> bool:
    """Return True when running in a supported notebook runtime."""
    marimo = sys.modules.get("marimo")
    if marimo is not None:
        running_in_notebook = getattr(marimo, "running_in_notebook", None)
        if callable(running_in_notebook) and running_in_notebook():
            return True

    try:
        from IPython import get_ipython
    except Exception:
        return False

    shell = get_ipython()
    if shell is None:
        return False

    shell_module = shell.__class__.__module__
    if shell_module.startswith("ipykernel."):
        return True

    config = getattr(shell, "config", None)
    return bool(config is not None and "IPKernelApp" in config)


@dataclass(eq=False)
class _ManagedAsyncCall:
    result: concurrent.futures.Future[Any]
    task: asyncio.Task[tuple[bool, Any]] | None = None
    cancel_requested: bool = False


class _ManagedAsyncBridge:
    """Own one background event loop and every coroutine accepted by it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._calls: set[_ManagedAsyncCall] = set()

    def _ensure_loop_locked(self) -> asyncio.AbstractEventLoop:
        if self._stopping:
            raise RuntimeError("xbbg coroutine bridge is stopping")
        if (
            self._loop is not None
            and not self._loop.is_closed()
            and self._loop.is_running()
            and self._thread is not None
            and self._thread.is_alive()
        ):
            return self._loop
        if self._loop is not None or self._thread is not None:
            raise RuntimeError("xbbg coroutine bridge loop is unavailable")

        ready = threading.Event()
        loop_holder: dict[str, asyncio.AbstractEventLoop] = {}
        error_holder: list[BaseException] = []

        def run_loop() -> None:
            loop: asyncio.AbstractEventLoop | None = None
            failure: BaseException | None = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop_holder["loop"] = loop
                loop.call_soon(ready.set)
                loop.run_forever()
            except BaseException as error:
                failure = error
                error_holder.append(error)
                ready.set()
            finally:
                if loop is not None and not loop.is_closed():
                    try:
                        pending = asyncio.all_tasks(loop)
                        for task in pending:
                            task.cancel()
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                        loop.run_until_complete(loop.shutdown_asyncgens())
                    except BaseException as error:
                        if failure is None:
                            failure = error
                        logger.error("Exception stopping xbbg coroutine bridge loop", exc_info=True)
                    finally:
                        loop.close()
                self._loop_exited(loop, threading.current_thread(), failure)

        thread = threading.Thread(
            target=run_loop,
            name="xbbg-notebook-sync-bridge",
            daemon=True,
        )
        thread.start()
        ready.wait()

        if error_holder or "loop" not in loop_holder or not thread.is_alive():
            cause = error_holder[0] if error_holder else None
            raise RuntimeError("Failed to start xbbg coroutine bridge") from cause

        self._loop = loop_holder["loop"]
        self._thread = thread
        return self._loop

    def _loop_exited(
        self,
        loop: asyncio.AbstractEventLoop | None,
        thread: threading.Thread,
        failure: BaseException | None,
    ) -> None:
        with self._lock:
            if self._loop is not loop or self._thread is not thread:
                return
            calls = tuple(self._calls)
            self._calls.clear()
            self._loop = None
            self._thread = None
            self._stopping = False

        for call in calls:
            if not call.result.done():
                error = RuntimeError("xbbg coroutine bridge stopped before call completed")
                if failure is not None:
                    error.__cause__ = failure
                call.result.set_exception(error)

    def ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            return self._ensure_loop_locked()

    def _complete_call(
        self,
        call: _ManagedAsyncCall,
        succeeded: bool,
        value: Any,
    ) -> None:
        with self._lock:
            if call not in self._calls:
                return
            self._calls.remove(call)

        if call.result.done():
            return
        try:
            if succeeded:
                call.result.set_result(value)
            else:
                call.result.set_exception(value)
        except concurrent.futures.InvalidStateError:
            pass

    def _schedule_call(
        self,
        call: _ManagedAsyncCall,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if call.result.done():
            return

        async def invoke() -> tuple[bool, Any]:
            try:
                return True, await async_func(*args, **kwargs)
            except BaseException as error:
                return False, error

        try:
            task = asyncio.create_task(invoke())
        except BaseException as error:
            self._complete_call(call, False, error)
            return

        with self._lock:
            if call not in self._calls:
                task.cancel()
                return
            call.task = task
            cancel_requested = call.cancel_requested

        def complete(completed: asyncio.Task[tuple[bool, Any]]) -> None:
            try:
                succeeded, value = completed.result()
            except BaseException as error:
                succeeded, value = False, error
            self._complete_call(call, succeeded, value)

        task.add_done_callback(complete)
        if cancel_requested:
            task.cancel()

    def start(
        self,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> _ManagedAsyncCall:
        caller_context = contextvars.copy_context()
        call = _ManagedAsyncCall(concurrent.futures.Future())

        with self._lock:
            if self._thread is threading.current_thread():
                raise RuntimeError("xbbg coroutine bridge cannot be re-entered from its own event-loop thread")
            loop = self._ensure_loop_locked()
            self._calls.add(call)
            try:
                loop.call_soon_threadsafe(
                    self._schedule_call,
                    call,
                    async_func,
                    args,
                    kwargs,
                    context=caller_context,
                )
            except BaseException:
                self._calls.remove(call)
                raise

        return call

    def cancel(self, call: _ManagedAsyncCall) -> None:
        with self._lock:
            if call not in self._calls:
                return
            call.cancel_requested = True
            task = call.task
            loop = self._loop

        if task is not None and loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass

    def submit(
        self,
        async_func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        call = self.start(async_func, args, kwargs)
        try:
            return call.result.result()
        except BaseException:
            if not call.result.done():
                self.cancel(call)
            raise

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
            if loop is None or thread is None:
                return
            initiate_stop = not self._stopping
            if initiate_stop:
                self._stopping = True
                calls = tuple(self._calls)
                self._calls.clear()
            else:
                calls = ()

        if initiate_stop:
            for call in calls:
                call.result.cancel()
            tasks = tuple(call.task for call in calls if call.task is not None)

            def cancel_and_stop() -> None:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                loop.stop()

            if loop.is_running():
                try:
                    loop.call_soon_threadsafe(cancel_and_stop)
                except RuntimeError:
                    logger.error("Failed to schedule xbbg coroutine bridge shutdown", exc_info=True)

        if thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if thread.is_alive():
                logger.error("xbbg coroutine bridge did not stop within 1 second")


_notebook_sync_bridge = _ManagedAsyncBridge()


def _ensure_notebook_sync_loop() -> asyncio.AbstractEventLoop:
    return _notebook_sync_bridge.ensure_loop()


def _stop_notebook_sync_loop() -> None:
    _notebook_sync_bridge.close()


def _run_in_notebook_sync_bridge(
    async_func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    return _notebook_sync_bridge.submit(async_func, args, kwargs)


def _build_sync_wrapper(
    sync_name: str,
    async_func: Callable[..., Any],
    *,
    template: Callable[..., Any] | None = None,
    allow_notebook_bridge: bool = False,
) -> Callable[..., Any]:
    template_func = template if template is not None else async_func

    @functools.wraps(template_func)
    def wrapped(*args, **kwargs):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(async_func(*args, **kwargs))

        if allow_notebook_bridge and _is_notebook_context():
            return _run_in_notebook_sync_bridge(async_func, args, kwargs)

        raise RuntimeError(
            f"{sync_name}() cannot be used inside an async context. "
            f"Use 'await a{sync_name}()' instead, "
            f"or use xbbg.Engine(...) for scoped async engines."
        )

    wrapped.__name__ = sync_name
    wrapped.__qualname__ = sync_name
    wrapped.__module__ = __name__
    wrapped.__signature__ = inspect.signature(template_func)  # type: ignore[unresolved-attribute]
    return wrapped


async def _execute_generated_endpoint(spec: _GeneratedEndpointSpec, call_args: dict[str, Any]) -> DataFrameResult:
    plan_or_awaitable = spec.builder(call_args)
    if inspect.isawaitable(plan_or_awaitable):
        plan: _EndpointPlan = cast("_EndpointPlan", await plan_or_awaitable)
    else:
        plan = plan_or_awaitable

    request_kwargs = dict(plan.request_kwargs)
    if plan.extractor is not None:
        request_kwargs["extractor"] = plan.extractor
    elif spec.extractor is not None and "extractor" not in request_kwargs:
        request_kwargs["extractor"] = spec.extractor

    service = plan.service if plan.service is not None else spec.service
    operation = plan.operation if plan.operation is not None else spec.operation

    raw = await arequest(
        service=service,
        operation=operation,
        backend=None,
        _raw=True,
        **request_kwargs,
    )

    if plan.postprocess is not None:
        return plan.postprocess(raw)

    return _convert_result_backend(raw, plan.backend)


def _build_generated_async(spec: _GeneratedEndpointSpec, async_template: Callable[..., Any]) -> Callable[..., Any]:
    signature_text = _strip_signature_annotations(async_template)
    source = (
        f"async def {spec.async_name}{signature_text}:\n"
        f"    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS[{spec.async_name!r}], locals())"
    )
    namespace: dict[str, Any] = {}
    exec(source, globals(), namespace)
    generated = namespace[spec.async_name]
    generated.__doc__ = async_template.__doc__
    generated.__annotations__ = dict(getattr(async_template, "__annotations__", {}))
    generated.__module__ = __name__
    generated.__qualname__ = spec.async_name
    return generated


def _install_generated_endpoint(spec: _GeneratedEndpointSpec) -> None:
    async_template = globals()[spec.async_name]
    generated_async = _build_generated_async(spec, async_template)
    globals()[spec.async_name] = generated_async

    globals()[spec.sync_name] = _build_sync_wrapper(
        spec.sync_name,
        generated_async,
        template=async_template,
        allow_notebook_bridge=spec.sync_name in _NOTEBOOK_SYNC_BRIDGE_NAMES,
    )


def _install_generated_endpoints() -> None:
    for spec in _GENERATED_ENDPOINT_SPECS.values():
        _install_generated_endpoint(spec)


# Generated endpoint sync wrappers are installed via _install_generated_endpoints().


# =============================================================================
# Streaming API - Real-time Market Data
# =============================================================================


# Bloomberg field values can be various primitive types
TickValue: TypeAlias = float | int | str | bool | datetime | None


@dataclass
class Tick:
    """Single tick data point from a subscription.

    Attributes:
        ticker: Security identifier
        field: Bloomberg field name
        value: Field value (float, int, str, bool, datetime, or None)
        timestamp: Time the tick was received
    """

    ticker: str
    field: str
    value: TickValue
    timestamp: datetime


class Subscription:
    """Subscription handle with async iteration and dynamic control.

    Supports:
    - Async iteration: `async for tick in sub`
    - Dynamic add/remove: `await sub.add(['MSFT US Equity'])`
    - Context manager: `async with xbbg.asubscribe(...) as sub:`
    - Explicit unsubscribe: `await sub.unsubscribe(drain=True)`

    Example::

        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE", "BID"])

        async for batch in sub:
            # batch is an xbbg.ArrowTable by default; use raw=True for ArrowRecordBatch
            print(batch.to_pylist())

            if should_add_msft:
                await sub.add(["MSFT US Equity"])

        await sub.unsubscribe()
    """

    def __init__(
        self,
        py_sub,
        raw: bool,
        backend: Backend | str | None,
        tick_mode: bool = False,
        topic_normalizer: Callable[[str | Sequence[str]], list[str]] | None = None,
    ):
        """Initialize subscription wrapper.

        Args:
            py_sub: The underlying PySubscription from Rust
            raw: If True, yield raw ArrowRecordBatch wrappers
            backend: Explicit DataFrame backend for conversion; None yields native ArrowTable
            tick_mode: If True, convert batches to dicts (implies raw=True)
            topic_normalizer: Normalizes add/remove inputs to subscribed Bloomberg topics
        """
        self._sub = py_sub
        self._raw = raw
        self._backend = Backend(backend) if isinstance(backend, str) else backend
        self._tick_mode = tick_mode
        self._topic_normalizer = topic_normalizer or _normalize_tickers
        self._convert_batch = self._build_batch_converter()

    def _build_batch_converter(self) -> Callable[[Any], Any]:
        if self._raw:
            return lambda batch: batch
        if self._backend is None:
            return lambda batch: batch.to_table()
        converter = convert_backend_frame
        backend = self._backend
        return lambda batch: converter(batch.to_table(), backend)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        try:
            if self._tick_mode:
                return await self._sub.__anext_tick_dict__()

            batch = await self._sub.__anext__()
        except Exception as exc:
            mapped = _normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

        return self._convert_batch(batch)

    async def add(self, tickers: str | list[str]) -> None:
        """Add tickers to subscription dynamically.

        Args:
            tickers: Single ticker or list of tickers to add
        """
        ticker_list = self._topic_normalizer(tickers)
        logger.debug("subscription add: %s", ticker_list)
        try:
            await self._sub.add(ticker_list)
        except Exception as exc:
            mapped = _normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    async def remove(self, tickers: str | list[str]) -> None:
        """Remove tickers from subscription dynamically.

        Args:
            tickers: Single ticker or list of tickers to remove
        """
        ticker_list = self._topic_normalizer(tickers)
        logger.debug("subscription remove: %s", ticker_list)
        try:
            await self._sub.remove(ticker_list)
        except Exception as exc:
            mapped = _normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    @property
    def tickers(self) -> list[str]:
        """Currently active tickers."""
        return self._sub.tickers

    @property
    def failed_tickers(self) -> list[str]:
        """Tickers Bloomberg rejected or terminated."""
        return self._sub.failed_tickers

    @property
    def failures(self) -> list[dict[str, str]]:
        """Non-fatal per-ticker subscription failures.

        Each entry contains:
            - ticker: Bloomberg topic string
            - reason: Bloomberg failure detail
            - kind: "failure" or "terminated"
        """
        return [{"ticker": ticker, "reason": reason, "kind": kind} for ticker, reason, kind in self._sub.failures]

    @property
    def topic_states(self) -> dict[str, dict[str, int | str]]:
        """Topic lifecycle state keyed by ticker/topic."""
        return {
            ticker: {"state": state, "last_change_us": last_change_us}
            for ticker, state, last_change_us in self._sub.topic_states
        }

    @property
    def session_status(self) -> dict[str, int | str]:
        """Session-level connection status for this subscription."""
        return dict(self._sub.session_status)

    @property
    def admin_status(self) -> dict[str, int | bool | None]:
        """Bloomberg admin/slow-consumer status for this subscription."""
        return dict(self._sub.admin_status)

    @property
    def service_status(self) -> dict[str, dict[str, int | bool]]:
        """Service availability status keyed by Bloomberg service name."""
        return {
            service: {"up": up, "last_change_us": last_change_us}
            for service, up, last_change_us in self._sub.service_status
        }

    @property
    def events(self) -> list[dict[str, str | int | None]]:
        """Bounded lifecycle/event history for the subscription."""
        return [
            {
                "at_us": at_us,
                "category": category,
                "level": level,
                "message_type": message_type,
                "topic": topic,
                "detail": detail,
            }
            for at_us, category, level, message_type, topic, detail in self._sub.events
        ]

    @property
    def status(self) -> dict[str, Any]:
        """Combined operational status snapshot."""
        return {
            "active": self.is_active,
            "all_failed": self.all_failed,
            "tickers": self.tickers,
            "failed_tickers": self.failed_tickers,
            "topic_states": self.topic_states,
            "session": self.session_status,
            "admin": self.admin_status,
            "services": self.service_status,
        }

    @property
    def fields(self) -> list[str]:
        """Subscribed fields."""
        return self._sub.fields

    @property
    def is_active(self) -> bool:
        """Whether the subscription is still active."""
        return self._sub.is_active

    @property
    def all_failed(self) -> bool:
        """Whether every requested ticker has ended in failure/termination."""
        return self._sub.all_failed

    @property
    def stats(self) -> dict:
        """Subscription metrics.

        Returns:
            dict with keys:
                - messages_received: int - total messages received from Bloomberg
                - dropped_batches: int - batches dropped due to overflow
                - batches_sent: int - batches successfully sent to Python
                - slow_consumer: bool - True if DATALOSS was received
                - data_loss_events: int - total Bloomberg data-loss signals observed
                - last_message_us: int - latest receive timestamp seen from Bloomberg
                - last_data_loss_us: int - latest data-loss timestamp seen from Bloomberg
                - effective_overflow_policy: str - actual runtime policy used by the Rust stream
        """
        return self._sub.stats

    async def unsubscribe(self, drain: bool = False) -> list[Any] | None:
        """Close subscription and optionally drain remaining data.

        Args:
            drain: If True, return all unread values using the same
                representation selected for iteration.

        Returns:
            List of remaining values if drain=True, else None.

        Raises:
            BlpError: An unread stream failure or cleanup failure. Buffered
                partial results are not returned when draining fails.
        """
        logger.debug("unsubscribe: drain=%s", drain)
        try:
            remaining = await self._sub.unsubscribe(drain, self._tick_mode)
        except Exception as exc:
            mapped = _normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        if not drain:
            return None
        if self._tick_mode:
            return remaining
        return [self._convert_batch(batch) for batch in remaining]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.unsubscribe()

    def __repr__(self) -> str:
        return repr(self._sub)


async def asubscribe(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
    service: str | Service | None = None,
    options: list[str] | None = None,
    conflate: bool = False,
    tick_mode: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
    output: str | None = None,
) -> Subscription:
    """Create an async subscription to real-time market data.

    This is the low-level subscription API with full control over
    the subscription lifecycle, including dynamic add/remove.

    Subscription recovery is handled automatically by the Bloomberg SDK (see
    BLPAPI ChangeLog v3.11.6); per-subscription availability transitions fire
    as ``SubscriptionStreamsActivated`` / ``SubscriptionStreamsDeactivated``
    events which are reflected in ``sub.topic_states`` (``streams_active``).

    Dict ticks are sparse deltas: missing keys are unchanged, while present
    ``None`` values explicitly clear fields. Arrow batches append the binary
    ``__xbbg_present`` bitset (LSB-first; bit i maps to schema field i + 2).
    Bloomberg DATALOSS or local queue loss raises
    ``BlpSubscriptionDataLossError`` and ends the stream; resubscribe for a
    fresh image. Connection-down notifications alone remain nonterminal.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to (e.g., 'LAST_PRICE', 'BID', 'ASK')
        raw: If True, yield raw ArrowRecordBatch wrappers for max performance
        all_fields: Expose all top-level scalar Bloomberg fields. Unrequested
            arrays/complex fields are omitted; requested unsupported shapes fail.
        backend: Explicit backend for batch conversion. ``None`` (the default)
            yields native ArrowTable wrappers; pass e.g. "narwhals", "pandas",
            or "pyarrow" to opt into conversion. Ignored if raw=True.
        service: Bloomberg service (e.g., '//blp/mktdata'). For BPS services
            such as ``//blp/mktbar`` and ``//blp/mktvwap``, tickers are
            normalized to explicit service topics.
        options: List of subscription options. If provided, uses subscribe_with_options
        conflate: If True, request Bloomberg conflated market data for //blp/mktdata.
            Quote updates are conflated by Bloomberg; trades are still delivered as received.
        tick_mode: If True, return native dict ticks without building Arrow (implies raw=True)
        flush_threshold: Number of ticks to buffer before emitting a batch.
        stream_capacity: Backpressure capacity of the native subscription stream.
        overflow_policy: ``"drop_newest"`` (default) fails on a full consumer
            buffer; ``"block"`` waits briefly on a bounded forwarder and fails
            on overflow/timeout. Neither waits on Bloomberg's callback thread.
        output: Output selector: ``"record_batch"``, ``"backend"``, ``"dict"``,
            or ``"tick"`` (case-insensitive). When provided, it takes precedence
            over ``raw`` and ``tick_mode``; ``None`` retains their behavior.

    Returns:
        Subscription handle for iteration and control

    Example::

        # Basic usage: yields native xbbg.ArrowTable wrappers by default
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE", "BID"])
        async for table in sub:
            print(table.to_pylist())
        await sub.unsubscribe()

        # With context manager
        async with xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"]) as sub:
            count = 0
            async for batch in sub:
                print(batch)
                count += 1
                if count >= 10:
                    break

        # Dynamic add/remove
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"])
        async for batch in sub:
            if should_add_msft:
                await sub.add(["MSFT US Equity"])
            if should_remove_aapl:
                await sub.remove(["AAPL US Equity"])

        # Tick mode (dict conversion)
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"], tick_mode=True)
        async for tick_dict in sub:
            print(tick_dict)  # {'ticker': 'AAPL US Equity', 'LAST_PRICE': 150.25, ...}
    """
    if output is not None:
        normalized_output = output.lower()
        if normalized_output not in ("record_batch", "backend", "dict", "tick"):
            raise ValueError(f"output must be one of 'record_batch', 'backend', 'dict', 'tick', got {output!r}")
        if normalized_output in ("dict", "tick"):
            raw = True
            tick_mode = True
        elif normalized_output == "record_batch":
            raw = True
            tick_mode = False
        else:
            raw = False
            tick_mode = False

    if flush_threshold is not None and flush_threshold < 1:
        raise ValueError("flush_threshold must be >= 1")
    if stream_capacity is not None and stream_capacity < 1:
        raise ValueError("stream_capacity must be >= 1")
    if overflow_policy is not None and overflow_policy not in OVERFLOW_POLICIES:
        raise ValueError(f"overflow_policy must be one of {OVERFLOW_POLICY_VALUES}, got {overflow_policy!r}")

    if tick_mode and flush_threshold is not None and flush_threshold > 1:
        warnings.warn(
            f"tick_mode=True forces flush_threshold=1, ignoring flush_threshold={flush_threshold}", stacklevel=2
        )
        flush_threshold = 1

    subscription_service = service.value if isinstance(service, Service) else service
    effective_subscription_service = subscription_service or Service.MKTDATA.value
    subscription_options = _normalize_subscription_options(
        options,
        conflate=conflate,
        service=effective_subscription_service,
    )
    if subscription_service == Service.MKTBAR.value:
        topic_normalizer = _normalize_mktbar_topics
    elif subscription_service == Service.MKTVWAP.value:
        topic_normalizer = _normalize_mktvwap_topics
    else:
        topic_normalizer = _normalize_tickers

    ticker_list = topic_normalizer(tickers)
    field_list = _normalize_fields(fields)
    if subscription_service == Service.MKTBAR.value:
        if field_list != ["LAST_PRICE"]:
            raise ValueError("//blp/mktbar subscriptions must request only LAST_PRICE")
        _validate_mktbar_options(subscription_options)
    elif subscription_service == Service.MKTVWAP.value and field_list != ["VWAP"]:
        raise ValueError("//blp/mktvwap subscriptions must request only VWAP")

    effective_backend = None if backend is None else _backend_resolve_backend(backend, None)

    engine = _get_engine()
    logger.debug("subscribe: tickers=%s fields=%s", ticker_list, field_list)

    # Use subscribe_with_options if service, subscription options, or config params provided
    if (
        subscription_service is not None
        or subscription_options is not None
        or flush_threshold is not None
        or stream_capacity is not None
        or overflow_policy is not None
    ):
        opt_kwargs = {
            k: v
            for k, v in {
                "flush_threshold": flush_threshold,
                "stream_capacity": stream_capacity,
                "overflow_policy": overflow_policy,
                "all_fields": all_fields,
            }.items()
            if v is not None
        }
        py_sub = await engine.subscribe_with_options(
            subscription_service or "//blp/mktdata",
            ticker_list,
            field_list,
            subscription_options or [],
            **opt_kwargs,
        )
    else:
        py_sub = await engine.subscribe(ticker_list, field_list, all_fields=all_fields)

    return Subscription(
        py_sub,
        raw=raw or tick_mode,
        backend=effective_backend,
        tick_mode=tick_mode,
        topic_normalizer=topic_normalizer,
    )


async def astream(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
    callback: Callable[[Any], None] | None = None,
    tick_mode: bool = False,
    conflate: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
):
    """High-level async streaming - simple iteration.

    This is the simple API for streaming data. For dynamic add/remove,
    use asubscribe() instead.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to
        raw: If True, yield raw Arrow RecordBatches
        all_fields: If True, expose all top-level scalar Bloomberg subscription fields
        backend: DataFrame backend for batch conversion
        callback: Optional callback function to invoke on each batch
        tick_mode: If True, convert batches to dicts
        conflate: If True, request Bloomberg conflated market data for //blp/mktdata.
        flush_threshold: Number of ticks to buffer before emitting a batch.
        stream_capacity: Capacity of the native subscription stream.
        overflow_policy: Overflow policy for the native stream: ``"drop_newest"``
            (default) or ``"block"``.

    Yields:
        Batches of market data (RecordBatch, DataFrame, or dict)

    Example::

        async for batch in xbbg.astream(["AAPL US Equity"], ["LAST_PRICE"]):
            print(batch)
            if done:
                break


        # With callback
        def on_batch(batch):
            print(f"Got batch: {batch}")


        async for _ in xbbg.astream(["AAPL US Equity"], ["LAST_PRICE"], callback=on_batch):
            pass
    """
    async with await asubscribe(
        tickers,
        fields,
        raw=raw,
        all_fields=all_fields,
        backend=backend,
        tick_mode=tick_mode,
        conflate=conflate,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
    ) as sub:
        async for batch in sub:
            if callback is not None:
                try:
                    callback(batch)
                except Exception as e:
                    logger.warning("callback raised exception: %s", e, exc_info=True)
            yield batch


def _reserve_sync_stream_producer() -> tuple[object, object]:
    scoped = _active_engine.get()
    if scoped is not None:
        scope: object = scoped
        config = getattr(scoped, "_config_snapshot", None)
    else:
        scope = _GLOBAL_SYNC_STREAM_PRODUCER_SCOPE
        with _engine_lock:
            config = _config

    try:
        limit = int(getattr(config, "max_subscription_sessions", _DEFAULT_MAX_SYNC_STREAM_PRODUCERS))
    except (TypeError, ValueError):
        limit = _DEFAULT_MAX_SYNC_STREAM_PRODUCERS
    limit = max(1, limit)

    slot = object()
    with _SYNC_STREAM_PRODUCERS_LOCK:
        active = _ACTIVE_SYNC_STREAM_PRODUCERS.setdefault(scope, set())
        if len(active) >= limit:
            raise RuntimeError(f"sync stream producer limit reached ({limit})")
        active.add(slot)
    return scope, slot


def _release_sync_stream_producer(reservation: tuple[object, object]) -> None:
    scope, slot = reservation
    with _SYNC_STREAM_PRODUCERS_LOCK:
        active = _ACTIVE_SYNC_STREAM_PRODUCERS.get(scope)
        if active is None:
            return
        active.discard(slot)
        if not active:
            _ACTIVE_SYNC_STREAM_PRODUCERS.pop(scope, None)


def stream(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
    callback: Callable[[Any], None] | None = None,
    tick_mode: bool = False,
    conflate: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
):
    """High-level sync streaming using the managed background event loop.

    Use astream() directly from async contexts.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to
        raw: If True, yield raw Arrow RecordBatches
        all_fields: If True, expose all top-level scalar Bloomberg subscription fields
        backend: DataFrame backend for batch conversion
        callback: Optional callback function to invoke on each batch
        tick_mode: If True, convert batches to dicts
        conflate: If True, request Bloomberg conflated market data for //blp/mktdata.
        flush_threshold: Number of ticks to buffer before emitting a batch.
        stream_capacity: Capacity of both the native subscription stream and the
            sync bridge. Defaults to 256.
        overflow_policy: Overflow policy for the native stream: ``"drop_newest"``
            (default) or ``"block"``.

    Yields:
        Batches of market data

    Example::

        for batch in xbbg.stream(["AAPL US Equity"], ["LAST_PRICE"]):
            print(batch)
            if done:
                break
    """
    import queue

    bridge_capacity = _DEFAULT_SYNC_STREAM_CAPACITY if stream_capacity is None else stream_capacity
    if bridge_capacity < 1:
        raise ValueError("stream_capacity must be >= 1")

    data_queue: queue.Queue[Any] = queue.Queue(maxsize=bridge_capacity)
    stop_event = threading.Event()
    producer_done = threading.Event()
    consumer_wakeup = threading.Event()
    producer_loop: asyncio.AbstractEventLoop | None = None
    producer_space_available: asyncio.Event | None = None
    producer_error: BaseException | None = None

    async def run_stream() -> None:
        nonlocal producer_loop, producer_space_available
        producer_loop = asyncio.get_running_loop()
        space_available = asyncio.Event()
        producer_space_available = space_available

        source = astream(
            tickers,
            fields,
            raw=raw,
            all_fields=all_fields,
            backend=backend,
            callback=None,
            tick_mode=tick_mode,
            conflate=conflate,
            flush_threshold=flush_threshold,
            stream_capacity=stream_capacity,
            overflow_policy=overflow_policy,
        )
        try:
            async for batch in source:
                while not stop_event.is_set():
                    space_available.clear()
                    try:
                        data_queue.put_nowait(batch)
                    except queue.Full:
                        await space_available.wait()
                    else:
                        consumer_wakeup.set()
                        break
                else:
                    break
        finally:
            await source.aclose()

    producer_slot = _reserve_sync_stream_producer()
    try:
        producer_call = _notebook_sync_bridge.start(run_stream, (), {})
    except BaseException:
        _release_sync_stream_producer(producer_slot)
        raise

    def producer_finished(result: concurrent.futures.Future[Any]) -> None:
        nonlocal producer_error
        try:
            result.result()
        except (asyncio.CancelledError, concurrent.futures.CancelledError) as error:
            if not stop_event.is_set():
                producer_error = error
        except BaseException as error:
            if not stop_event.is_set():
                producer_error = error
        finally:
            _release_sync_stream_producer(producer_slot)
            producer_done.set()
            consumer_wakeup.set()

    producer_call.result.add_done_callback(producer_finished)

    try:
        while True:
            consumer_wakeup.clear()
            try:
                batch = data_queue.get_nowait()
            except queue.Empty:
                if not producer_done.is_set():
                    consumer_wakeup.wait()
                    continue
                try:
                    batch = data_queue.get_nowait()
                except queue.Empty:
                    break

            loop = producer_loop
            space_available = producer_space_available
            if loop is not None and space_available is not None and loop.is_running():
                try:
                    loop.call_soon_threadsafe(space_available.set)
                except RuntimeError:
                    pass
            if callback is not None:
                try:
                    callback(batch)
                except Exception as error:
                    logger.warning("callback raised exception: %s", error, exc_info=True)
            yield batch

        if producer_error is not None:
            raise producer_error
    finally:
        active_error = sys.exc_info()[1]
        cancel_requested = not producer_done.is_set()
        stop_event.set()
        if cancel_requested:
            _notebook_sync_bridge.cancel(producer_call)
        try:
            producer_call.result.result(timeout=_SYNC_STREAM_CLOSE_TIMEOUT_SECONDS)
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            pass
        except concurrent.futures.TimeoutError as timeout_error:
            lifecycle_error = RuntimeError(
                "stream producer did not stop before the close timeout; cancellation remains tracked"
            )
            if active_error is not None and not isinstance(active_error, GeneratorExit):
                logger.error("%s", lifecycle_error)
            else:
                raise lifecycle_error from timeout_error
        except BaseException as error:
            if error is active_error:
                pass
            elif active_error is not None and not isinstance(active_error, GeneratorExit):
                logger.error(
                    "stream producer cleanup failed after cancellation",
                    exc_info=(type(error), error, error.__traceback__),
                )
            else:
                raise


# =============================================================================
# VWAP Streaming API - Real-time Volume Weighted Average Price
# =============================================================================


async def avwap(
    tickers: str | list[str],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    raw: bool = False,
    all_fields: bool = True,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to real-time VWAP data.

    Uses Bloomberg's ``//blp/mktvwap`` service. Bloomberg requires explicit
    Market VWAP topics (for example, ``//blp/mktvwap/ticker/IBM US Equity``)
    and ``VWAP`` as the single requested field. Security identifiers passed
    here are normalized to those explicit topics.

    Args:
        tickers: Security identifier(s). Plain tickers use the ``ticker`` topic type;
            already-qualified ``//blp/mktvwap/...`` topics pass through unchanged.
        start_time: Optional VWAP calculation start time (e.g., "09:30").
        end_time: Optional VWAP calculation end time (e.g., "16:00").
        raw: If True, yield raw Arrow RecordBatches for max performance.
        all_fields: If True, expose the full top-level VWAP payload.
        backend: DataFrame backend for batch conversion (ignored if raw=True).

    Returns:
        Subscription handle for iteration and control.

    Example::

        # Basic usage - subscribe to VWAP
        sub = await xbbg.avwap("IBM US Equity")
        async for batch in sub:
            print(batch)
        await sub.unsubscribe()

        # With custom time window
        sub = await xbbg.avwap(["IBM US Equity", "MSFT US Equity"], start_time="09:30", end_time="16:00")
    """
    ticker_list = _normalize_mktvwap_topics(tickers)

    options: list[str] = []
    if start_time:
        options.append(f"VWAP_START_TIME={start_time}")
    if end_time:
        options.append(f"VWAP_END_TIME={end_time}")

    effective_backend = _resolve_backend(backend)

    engine = _get_engine()
    py_sub = await engine.subscribe_with_options(
        Service.MKTVWAP.value,
        ticker_list,
        ["VWAP"],
        options if options else None,
        all_fields=all_fields,
    )

    return Subscription(py_sub, raw=raw, backend=effective_backend, topic_normalizer=_normalize_mktvwap_topics)


# =============================================================================
# MKTBAR API - Real-time Streaming OHLC Bars
# =============================================================================


async def amktbar(
    tickers: str | list[str],
    *,
    bar_size: int = 1,
    start_time: str | None = None,
    end_time: str | None = None,
    raw: bool = False,
    all_fields: bool = True,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to real-time streaming OHLC bars.

    Uses Bloomberg's ``//blp/mktbar`` service. Bloomberg requires explicit
    market-bar topics (for example, ``//blp/mktbar/ticker/ES1 Index``),
    ``LAST_PRICE`` as the only requested field, and ``bar_size`` as the bar
    interval option. Security identifiers passed here are normalized to those
    explicit topics.

    Args:
        tickers: Security identifier(s). Plain tickers use the ``ticker`` topic type;
            identifiers like ``/figi/...`` keep their identifier type.
        bar_size: Bar interval in minutes (default: 1).
        start_time: Optional start time in HH:MM format.
        end_time: Optional end time in HH:MM format.
        raw: If True, return raw xbbg ArrowRecordBatch (default: False).
        all_fields: If True, expose the full top-level market-bar payload.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Example::

        # Subscribe to 5-minute bars
        async with await amktbar("AAPL US Equity", bar_size=5) as sub:
            async for batch in sub:
                print(batch)

        # Multiple securities
        sub = await amktbar(["AAPL US Equity", "MSFT US Equity"], bar_size=1)
        async for batch in sub:
            print(batch)
    """
    _validate_mktbar_bar_size(bar_size)

    logger.debug("amktbar: tickers=%s bar_size=%d", tickers, bar_size)

    ticker_list = _normalize_mktbar_topics(tickers)
    effective_backend = _resolve_backend(backend)

    options: list[str] = [f"bar_size={bar_size}"]
    if start_time:
        options.append(f"start_time={start_time}")
    if end_time:
        options.append(f"end_time={end_time}")

    engine = _get_engine()
    py_sub = await engine.subscribe_with_options(
        Service.MKTBAR.value,
        ticker_list,
        ["LAST_PRICE"],
        options,
        all_fields=all_fields,
    )

    return Subscription(py_sub, raw=raw, backend=effective_backend, topic_normalizer=_normalize_mktbar_topics)


# =============================================================================
# MKTDEPTH API - Level 2 Market Depth (B-PIPE Only)
# =============================================================================


async def adepth(
    tickers: str | list[str],
    *,
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to Level 2 market depth / order book data.

    .. warning::
        **Requires a Bloomberg B-PIPE environment and applicable service entitlements.**
        This feature is not available with Terminal-only connections.

    Provides real-time order book updates with bid/ask prices and sizes
    at multiple levels.

    Args:
        tickers: Security identifier(s).
        raw: If True, return raw xbbg ArrowRecordBatch (default: False).
        all_fields: If True, expose all top-level scalar Bloomberg subscription fields
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Raises:
        BlpBPipeError: If a B-PIPE environment or service entitlement is unavailable.

    Example::

        # Subscribe to market depth
        async with await adepth("AAPL US Equity") as sub:
            async for batch in sub:
                print(batch)  # Order book updates
    """
    from xbbg.exceptions import BlpBPipeError

    logger.debug("adepth: tickers=%s", tickers)

    # Normalize inputs
    ticker_list = _normalize_tickers(tickers)
    effective_backend = _resolve_backend(backend)

    # Get engine and subscribe
    engine = _get_engine()
    try:
        py_sub = await engine.subscribe_with_options(
            Service.MKTDEPTH.value,
            ticker_list,
            [],  # Fields are implicit for market depth
            None,
            all_fields=all_fields,
        )
    except Exception as e:
        # Check for B-PIPE related errors
        if "MKTDEPTHDATA" in str(e).upper() or "SERVICE" in str(e).upper():
            raise BlpBPipeError(
                "Level 2 market depth requires a Bloomberg B-PIPE environment and applicable service entitlements."
            ) from e
        raise

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# MKTLIST API - Option/Futures Chains (B-PIPE Only)
# =============================================================================


async def achains(
    underlying: str,
    *,
    chain_type: str = "OPTIONS",
    raw: bool = False,
    all_fields: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to option or futures chain updates.

    .. warning::
        **Requires a Bloomberg B-PIPE environment and applicable service entitlements.**
        This feature is not available with Terminal-only connections.

    Provides real-time updates for option chains or futures chains
    on a given underlying security.

    Args:
        underlying: Underlying security identifier.
        chain_type: Type of chain - "OPTIONS" or "FUTURES" (default: "OPTIONS").
        raw: If True, return raw xbbg ArrowRecordBatch (default: False).
        all_fields: If True, expose all top-level scalar Bloomberg subscription fields
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Raises:
        BlpBPipeError: If a B-PIPE environment or service entitlement is unavailable.

    Example::

        # Subscribe to option chain
        async with await achains("AAPL US Equity") as sub:
            async for batch in sub:
                print(batch)  # Option chain updates

        # Subscribe to futures chain
        sub = await achains("ES1 Index", chain_type="FUTURES")
    """
    from xbbg.exceptions import BlpBPipeError

    logger.debug("achains: underlying=%s chain_type=%s", underlying, chain_type)

    effective_backend = _resolve_backend(backend)

    # Build subscription options
    options: list[str] = [f"chainType={chain_type}"]

    # Get engine and subscribe
    engine = _get_engine()
    try:
        py_sub = await engine.subscribe_with_options(
            Service.MKTLIST.value,
            [underlying],
            [],  # Fields depend on chain type
            options,
            all_fields=all_fields,
        )
    except Exception as e:
        # Check for B-PIPE related errors
        if "MKTLIST" in str(e).upper() or "SERVICE" in str(e).upper():
            raise BlpBPipeError(
                "Option/futures chains require a Bloomberg B-PIPE environment and applicable service entitlements."
            ) from e
        raise

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# Technical Analysis API - Bloomberg Technical Analysis Service
# =============================================================================

# Study type to attribute name mapping
_TA_STUDIES: dict[str, str] = {
    # Moving Averages
    "smavg": "smavgStudyAttributes",
    "sma": "smavgStudyAttributes",
    "emavg": "emavgStudyAttributes",
    "ema": "emavgStudyAttributes",
    "wmavg": "wmavgStudyAttributes",
    "wma": "wmavgStudyAttributes",
    "vmavg": "vmavgStudyAttributes",
    "vma": "vmavgStudyAttributes",
    "tmavg": "tmavgStudyAttributes",
    "tma": "tmavgStudyAttributes",
    "ipmavg": "ipmavgStudyAttributes",
    # Oscillators
    "rsi": "rsiStudyAttributes",
    "macd": "macdStudyAttributes",
    "mao": "maoStudyAttributes",
    "momentum": "momentumStudyAttributes",
    "mom": "momentumStudyAttributes",
    "roc": "rocStudyAttributes",
    # Bands & Channels
    "boll": "bollStudyAttributes",
    "bb": "bollStudyAttributes",
    "kltn": "kltnStudyAttributes",
    "keltner": "kltnStudyAttributes",
    "mae": "maeStudyAttributes",
    "te": "teStudyAttributes",
    "al": "alStudyAttributes",
    # Trend
    "dmi": "dmiStudyAttributes",
    "adx": "dmiStudyAttributes",
    "tas": "tasStudyAttributes",
    "stoch": "tasStudyAttributes",
    "trender": "trenderStudyAttributes",
    "ptps": "ptpsStudyAttributes",
    "parabolic": "ptpsStudyAttributes",
    "sar": "ptpsStudyAttributes",
    # Volume
    "chko": "chkoStudyAttributes",
    "ado": "adoStudyAttributes",
    "vat": "vatStudyAttributes",
    "tvat": "tvatStudyAttributes",
    # Volatility
    "atr": "atrStudyAttributes",
    "hurst": "hurstStudyAttributes",
    # Other
    "fg": "fgStudyAttributes",
    "fear_greed": "fgStudyAttributes",
    "goc": "gocStudyAttributes",
    "ichimoku": "gocStudyAttributes",
    "cmci": "cmciStudyAttributes",
    "wlpr": "wlprStudyAttributes",
    "williams": "wlprStudyAttributes",
    "maxmin": "maxminStudyAttributes",
    "rex": "rexStudyAttributes",
    "etd": "etdStudyAttributes",
    "pd": "pdStudyAttributes",
    "rv": "rvStudyAttributes",
    "pivot": "pivotStudyAttributes",
    "or": "orStudyAttributes",
    "pcr": "pcrStudyAttributes",
    "bs": "bsStudyAttributes",
}

# Default study parameters
_TA_DEFAULTS: dict[str, dict[str, Any]] = {
    "smavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "emavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "wmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "vmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "tmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "rsiStudyAttributes": {"period": 14, "priceSourceClose": "PX_LAST"},
    "macdStudyAttributes": {
        "maPeriod1": 12,
        "maPeriod2": 26,
        "sigPeriod": 9,
        "priceSourceClose": "PX_LAST",
    },
    "bollStudyAttributes": {
        "period": 20,
        "upperBand": 2.0,
        "lowerBand": 2.0,
        "priceSourceClose": "PX_LAST",
    },
    "dmiStudyAttributes": {
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "atrStudyAttributes": {
        "maType": "Simple",
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "tasStudyAttributes": {
        "periodK": 14,
        "periodD": 3,
        "periodDS": 3,
        "periodDSS": 3,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
}


def _get_study_attr_name(study: str) -> str:
    """Get the Bloomberg attribute name for a study."""
    study_lower = study.lower().replace("-", "_").replace(" ", "_")
    if study_lower in _TA_STUDIES:
        return _TA_STUDIES[study_lower]
    # Try direct match with StudyAttributes suffix
    if study_lower.endswith("studyattributes"):
        return study_lower
    return f"{study_lower}StudyAttributes"


def _build_study_request(
    ticker: str,
    study: str,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> list[tuple[str, str]]:
    """Build dotted-path elements for a //blp/tasvc studyRequest.

    Returns a list of (dotted_path, value_str) tuples that the Rust worker
    applies via ``set_nested_str`` / ``set_nested_int`` on the request.

    Args:
        ticker: Security name
        study: Study type
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        periodicity: Data periodicity ('DAILY', 'WEEKLY', 'MONTHLY', or 'INTRADAY')
        interval: Intraday interval in minutes (only used if periodicity is INTRADAY)
        **study_params: Study-specific parameters (e.g., period=20 for SMA period)
    """
    attr_name = _get_study_attr_name(study)

    # Get defaults and merge with user params
    defaults = _TA_DEFAULTS.get(attr_name, {})
    params = {**defaults, **study_params}

    elements: list[tuple[str, str]] = []

    # Normalize dates to YYYYMMDD (Bloomberg tasvc expects this format)
    def _norm_date(d: str | None) -> str | None:
        return d.replace("-", "").replace("/", "") if d else None

    sd = _norm_date(start_date)
    ed = _norm_date(end_date)

    # Price source
    elements.append(("priceSource.securityName", ticker))

    # Data range
    if periodicity.upper() in ("DAILY", "WEEKLY", "MONTHLY"):
        prefix = "priceSource.dataRange.historical"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.periodicitySelection", periodicity.upper()))
    else:
        # Intraday
        prefix = "priceSource.dataRange.intraday"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.eventType", "TRADE"))
        elements.append((f"{prefix}.interval", str(interval or 60)))

    # Study attributes
    sa_prefix = f"studyAttributes.{attr_name}"
    for key, value in params.items():
        elements.append((f"{sa_prefix}.{key}", str(value)))

    return elements


async def abta(
    tickers: str | list[str],
    study: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> DataFrameResult:
    """Get technical analysis study data (async).

    Uses Bloomberg //blp/tasvc service to calculate technical indicators.

    Args:
        tickers: Security or list of securities
        study: Study type (e.g., 'sma', 'rsi', 'macd', 'boll', 'atr')
        start_date: Start date. Accepts ISO 8601 / ``YYYYMMDD`` string,
            ``datetime.date``, ``datetime.datetime``, or ``pd.Timestamp``.
        end_date: End date. Same accepted shapes as ``start_date``.
        periodicity: Data periodicity ('DAILY', 'WEEKLY', 'MONTHLY', 'INTRADAY')
        interval: Intraday interval in minutes (only for periodicity='INTRADAY')
        **study_params: Study-specific parameters (e.g., period=20 for SMA period)

    Returns:
        DataFrame with study results

    Available Studies:
        Moving Averages: sma, ema, wma, vma, tma
        Oscillators: rsi, macd, mao, momentum, roc
        Bands: boll (Bollinger), keltner, mae
        Trend: dmi/adx, stoch, trender, parabolic/sar
        Volume: chko, ado, vat
        Volatility: atr, hurst
        Other: ichimoku, pivot, williams

    Example::

        # Simple Moving Average with 20-day period
        df = await xbbg.abta("AAPL US Equity", "sma", period=20)

        # RSI with 14-day period
        df = await xbbg.abta("AAPL US Equity", "rsi", period=14)

        # MACD with custom parameters
        df = await xbbg.abta("AAPL US Equity", "macd", maPeriod1=12, maPeriod2=26, sigPeriod=9)

        # Bollinger Bands with 20-day period and 2 std devs
        df = await xbbg.abta("AAPL US Equity", "boll", period=20, upperBand=2.0, lowerBand=2.0)

        # Intraday RSI with 60-minute bars
        df = await xbbg.abta("AAPL US Equity", "rsi", periodicity="INTRADAY", interval=60)

        # Multiple securities (sends concurrent requests)
        df = await xbbg.abta(["AAPL US Equity", "MSFT US Equity"], "rsi")
    """
    import warnings

    ticker_list = _normalize_tickers(tickers)
    engine = _get_engine()

    async def fetch_single(ticker: str) -> Any | Exception:
        """Fetch TA data for a single ticker."""
        study_elements = _build_study_request(
            ticker,
            study,
            start_date=start_date,
            end_date=end_date,
            periodicity=periodicity,
            interval=interval,
            **study_params,
        )
        params = RequestParams(
            service=Service.TASVC,
            operation=Operation.STUDY_REQUEST,
            extractor=ExtractorHint.GENERIC,
            elements=study_elements,
        )
        return await engine.request(params.to_dict())

    # tasvc only supports 1 security per request, so send concurrent requests
    results = await asyncio.gather(
        *[fetch_single(t) for t in ticker_list],
        return_exceptions=True,
    )

    # Filter successful results and warn about failures
    batches: list[Any] = []
    for ticker, result in zip(ticker_list, results, strict=True):
        if isinstance(result, Exception):
            warnings.warn(f"Failed to fetch TA data for {ticker}: {result}", stacklevel=2)
        else:
            batches.append(result)

    if not batches:
        raise RuntimeError("All TA requests failed")

    # Combine all batches into a single native Arrow table
    table = _core_arrow_table_class().from_batches(batches)
    return _convert_result_backend(table, None)


def ta_studies() -> list[str]:
    """List available technical analysis study names.

    Returns:
        List of study short names that can be used with bta()/abta()

    Example::

        >>> xbbg.ta_studies()
        ['sma', 'ema', 'rsi', 'macd', 'boll', 'atr', ...]
    """
    # Return unique study short names
    seen = set()
    result = []
    for name in _TA_STUDIES:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return sorted(result)


def ta_study_params(study: str) -> dict[str, Any]:
    """Get default parameters for a technical analysis study.

    Args:
        study: Study name (e.g., 'rsi', 'macd', 'boll')

    Returns:
        Dictionary of parameter names and their default values

    Example::

        >>> xbbg.ta_study_params('rsi')
        {'period': 14, 'priceSourceClose': 'PX_LAST'}

        >>> xbbg.ta_study_params('macd')
        {'maPeriod1': 12, 'maPeriod2': 26, 'sigPeriod': 9, 'priceSourceClose': 'PX_LAST'}

        >>> xbbg.ta_study_params('boll')
        {'period': 20, 'upperBand': 2.0, 'lowerBand': 2.0, 'priceSourceClose': 'PX_LAST'}
    """
    attr_name = _get_study_attr_name(study)
    return _TA_DEFAULTS.get(attr_name, {})


def generate_ta_stubs(output_dir: str | None = None) -> str:
    """Generate Python type stubs for technical analysis studies.

    Creates a .pyi file with TypedDict definitions for all TA study parameters.
    Stubs are generated from the //blp/tasvc schema for IDE autocomplete support.

    Args:
        output_dir: Output directory (default: ~/.xbbg/stubs/)

    Returns:
        Path to the generated stub file.

    Example::

        >>> xbbg.generate_ta_stubs()
        '~/.xbbg/stubs/ta_studies.pyi'

        # Then in your code, IDE will autocomplete:
        >>> from xbbg.stubs.ta_studies import RSIParams
        >>> params: RSIParams = {'period': 14}
    """
    from pathlib import Path

    from .schema import aget_schema

    # Get tasvc schema
    schema = asyncio.run(aget_schema("//blp/tasvc"))

    # Find studyRequest operation
    op = schema.get_operation("studyRequest")
    if not op:
        raise RuntimeError("Could not find studyRequest operation in tasvc schema")

    # Find studyAttributes element
    study_attrs = None
    for child in op.request.children:
        if child.name == "studyAttributes":
            study_attrs = child
            break

    if not study_attrs:
        raise RuntimeError("Could not find studyAttributes in schema")

    # Generate stub content
    lines = [
        '"""',
        "Bloomberg Technical Analysis Study Type Stubs",
        "",
        "Auto-generated from //blp/tasvc schema.",
        "DO NOT EDIT - regenerate using xbbg.generate_ta_stubs()",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "import sys",
        "if sys.version_info >= (3, 11):",
        "    from typing import Literal, NotRequired, TypedDict",
        "else:",
        "    from typing import Literal",
        "    from typing_extensions import NotRequired, TypedDict",
        "",
    ]

    # Map of Bloomberg attribute names to friendly names
    attr_to_friendly = {v: k for k, v in _TA_STUDIES.items()}

    # Type mapping
    type_map = {
        "Bool": "bool",
        "Int32": "int",
        "Int64": "int",
        "Float32": "float",
        "Float64": "float",
        "String": "str",
        "Enumeration": "str",
    }

    # Generate TypedDict for each study
    for study_child in study_attrs.children:
        attr_name = study_child.name
        friendly = attr_to_friendly.get(attr_name, attr_name.replace("StudyAttributes", ""))

        # Create class name (e.g., rsiStudyAttributes -> RSIParams)
        class_name = friendly.upper() + "Params"
        if class_name.startswith("_"):
            class_name = class_name[1:]

        lines.append(f"class {class_name}(TypedDict, total=False):")
        lines.append(f'    """Parameters for {friendly} study."""')

        if not study_child.children:
            lines.append("    pass")
        else:
            for param in study_child.children:
                param_name = param.name
                if param.enum_values:
                    values_str = ", ".join(f'"{v}"' for v in param.enum_values)
                    param_type = f"Literal[{values_str}]"
                else:
                    param_type = type_map.get(param.data_type, "str")

                # Add default value comment if we have one
                defaults = _TA_DEFAULTS.get(attr_name, {})
                default_val = defaults.get(param_name)
                if default_val is not None:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]  # default: {default_val}")
                else:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]")

        lines.append("")

    # Add StudyName literal type
    study_names = sorted(set(_TA_STUDIES.keys()))
    lines.append("# All available study names")
    lines.append(f"StudyName = Literal[{', '.join(repr(s) for s in study_names)}]")
    lines.append("")

    # Write files
    output_path = Path.home() / ".xbbg" / "stubs" if output_dir is None else Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stub_path = output_path / "ta_studies.pyi"
    stub_path.write_text("\n".join(lines))

    # Also write .py for runtime imports
    py_path = output_path / "ta_studies.py"
    py_path.write_text("\n".join(lines))

    # Configure IDE
    from .schema import configure_ide_stubs

    ide_msg = configure_ide_stubs(output_path)
    print(ide_msg)

    return str(stub_path)


# =============================================================================
# BQL API - Bloomberg Query Language
# =============================================================================


async def abql(
    expression: str,
    *,
    backend: Backend | str | None = None,
) -> DataFrameResult:
    """Async Bloomberg Query Language (BQL) request.

    BQL is Bloomberg's powerful query language for financial analytics.
    It allows you to query data across universes of securities with
    complex filters, calculations, and time series operations.

    Args:
        expression: BQL expression string.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        DataFrame with columns: id, <field1>, <field2>, ...
        Where 'id' is the security identifier from the BQL universe.

    Example::

        # Get price for a single security
        df = await abql("get(px_last) for('AAPL US Equity')")

        # Get multiple fields
        df = await abql("get(px_last, volume) for('AAPL US Equity')")

        # Holdings of an ETF
        df = await abql("get(id_isin, weights) for(holdings('SPY US Equity'))")

        # Index members
        df = await abql("get(px_last) for(members('SPX Index'))")

        # With filters
        df = await abql("get(px_last, pe_ratio) for(members('SPX Index')) with(pe_ratio > 20)")

        # Time series
        df = await abql("get(px_last) for('AAPL US Equity') with(dates=range(-5d, 0d))")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abql"], locals())


# =============================================================================
# BSRCH API - Bloomberg Search
# =============================================================================


async def absrch(
    domain: str,
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Search (BSRCH) request.

    BSRCH executes saved Bloomberg searches and returns matching securities.

    Args:
        domain: The saved search domain/name (e.g., "FI:SOVR", "COMDTY:PRECIOUS").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Search parameters. Direct keyword arguments and explicit
            ``overrides={...}`` mappings or ``(name, value)`` pairs are sent
            as ExcelGetGrid overrides. ``Domain`` is sent as the request's
            top-level domain element.

    Returns:
        DataFrame with columns from the saved search results.

    Example::

        # Sovereign bonds
        df = await absrch("FI:SOVR")

        # With additional parameters
        df = await absrch("COMDTY:WEATHER", LOCATION="NYC", MODEL="GFS")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["absrch"], locals())


# =============================================================================
# BQR API - Bloomberg Quote Request
# =============================================================================


def _parse_date_offset(offset: str, reference: datetime) -> datetime:
    """Parse date offset string like '-2d', '-1w', '-1m', '-3h'."""
    import re

    offset = offset.strip().lower()
    match = re.match(r"^(-?\d+)([dwmh])$", offset)
    if not match:
        raise ValueError(f"Invalid date offset format: {offset}. Use format like '-2d', '-1w', '-1m', '-3h'")

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "d":
        return reference + timedelta(days=value)
    if unit == "w":
        return reference + timedelta(weeks=value)
    if unit == "m":
        return reference + timedelta(days=value * 30)
    if unit == "h":
        return reference + timedelta(hours=value)
    raise ValueError(f"Unknown time unit: {unit}")


def _reshape_bqr_generic(table: Any, ticker: str) -> Any:
    """Reshape generic extractor output into structured BQR rows via native Arrow."""
    return table.reshape_bqr_generic(ticker)


_BQR_RENAME_MAP: dict[str, str] = {
    "type": "event_type",
    "value": "price",
    "brokerBuyCode": "broker_buy",
    "brokerSellCode": "broker_sell",
    "spreadPrice": "spread_price",
    "conditionCodes": "condition_codes",
    "exchangeCode": "exchange",
}
_BQR_BROKER_COLUMNS = ("brokerBuyCode", "brokerSellCode", "broker_buy", "broker_sell")
_BQR_DEALER_INPUT_EXAMPLE = "/isin/US037833FB15@MSG1 Corp"


def _looks_like_bqr_dealer_input(ticker: str) -> bool:
    normalized = " ".join(ticker.strip().casefold().split())
    return normalized.startswith("/isin/") and "@msg1 corp" in normalized


def _warn_bqr_dealer_input(ticker: str, *, stacklevel: int = 3) -> None:
    if _looks_like_bqr_dealer_input(ticker):
        return
    warnings.warn(
        "BQR broker attribution is intended for fixed-income ISIN inputs with an @MSG1 Corp "
        f"dealer quote source, for example '{_BQR_DEALER_INPUT_EXAMPLE}'. Other inputs may "
        "return quote rows without broker_buy/broker_sell and will raise unless "
        "include_broker_codes=False is passed explicitly.",
        UserWarning,
        stacklevel=stacklevel,
    )


def _bqr_has_broker_code_value(table: Any) -> bool:
    return table.has_any_value(list(_BQR_BROKER_COLUMNS))


def _postprocess_bqr_result(
    result: Any,
    *,
    ticker: str,
    backend: Backend | str | None,
    enforce_broker_codes: bool,
) -> DataFrameResult:
    table = ensure_arrow_table(result)

    if "path" in table.column_names:
        table = _reshape_bqr_generic(table, ticker)

    if enforce_broker_codes and table.num_rows > 0 and not _bqr_has_broker_code_value(table):
        raise RuntimeError(
            "BQR returned quote rows without broker attribution. "
            "Use a fixed-income ticker with a dealer quote pricing source such as '@MSG1 Corp', "
            "or pass include_broker_codes=False if raw quote ticks without dealer codes are intentional."
        )

    if table.num_rows > 0 and "time" in table.column_names:
        table = table.sort_by([("time", "ascending")])

    rename_map = {column: _BQR_RENAME_MAP[column] for column in table.column_names if column in _BQR_RENAME_MAP}
    table = table.rename_columns(rename_map)
    return _convert_result_backend(table, backend)


async def abqr(
    ticker: str,
    date_offset: str | None = None,
    start_date: DateLike = None,
    end_date: DateLike = None,
    *,
    event_types: Sequence[str] | None = None,
    include_broker_codes: bool = True,
    include_spread_price: bool = False,
    include_yield: bool = False,
    include_condition_codes: bool = False,
    include_exchange_codes: bool = False,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Quote Request (BQR).

    Retrieves dealer quote data using IntradayTickRequest with BID/ASK events.
    Emulates the Excel =BQR() function.

    Args:
        ticker: Security identifier. Supports Bloomberg tickers with pricing
            source qualifiers (e.g., 'IBM US Equity@MSG1', '/isin/US037833FB15@MSG1').
        date_offset: Date offset from now (e.g., '-2d', '-1w', '-3h').
            Mutually exclusive with start_date/end_date.
        start_date: Start date (e.g., '2024-01-15'). Defaults to 2 days ago.
        end_date: End date (e.g., '2024-01-17'). Defaults to today.
        event_types: Event types to retrieve. Defaults to ['BID', 'ASK'].
        include_broker_codes: Include broker/dealer codes (default True).
        include_spread_price: Include spread price for bonds (default False).
        include_yield: Include yield data for bonds (default False).
        include_condition_codes: Include trade condition codes (default False).
        include_exchange_codes: Include exchange codes (default False).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional options.

    Returns:
        DataFrame with columns: ticker, time, event_type, price, size,
        plus optional broker_buy, broker_sell, spread_price, etc.

    Example::

        # With date offset (like Excel BQR)
        df = await abqr("IBM US Equity@MSG1", date_offset="-2d")

        # Bond with broker codes and spread
        df = await abqr(
            "US037833FB15@MSG1 Corp",
            date_offset="-2d",
            include_broker_codes=True,
            include_spread_price=True,
        )

        # With explicit date range
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            start_date="2024-01-15",
            end_date="2024-01-17",
        )

        # Trade events only
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            date_offset="-1d",
            event_types=["TRADE"],
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abqr"], locals())


async def abflds(
    fields: str | list[str] | None = None,
    *,
    search_spec: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg field metadata lookup (BFLDS).

    Unified field function: get metadata for specific fields, or search by keyword.

    Args:
        fields: Single field or list of fields to get metadata for.
            Mutually exclusive with search_spec.
        search_spec: Search term to find fields by name/description.
            Mutually exclusive with fields.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options (e.g., port, server).

    Returns:
        DataFrame with field information or search results.

    Raises:
        ValueError: If neither fields nor search_spec is provided, or both are provided.

    Example::

        # Get info for specific fields
        df = await abflds(fields=["PX_LAST", "VOLUME"])

        # Search for fields by keyword
        df = await abflds(search_spec="vwap")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abflds"], locals())


# =============================================================================
# BEQS API - Bloomberg Equity Screening
# =============================================================================


async def abeqs(
    screen: str,
    *,
    asof: str | None = None,
    screen_type: str = "PRIVATE",
    group: str = "General",
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Equity Screening (BEQS) request.

    Execute a saved Bloomberg equity screen and return matching securities.

    Args:
        screen: Screen name as saved in Bloomberg.
        asof: As-of date for the screen. Accepts ISO 8601 / ``YYYYMMDD``
            string, ``datetime.date``, ``datetime.datetime``, or
            ``pd.Timestamp``.
        screen_type: Screen type - "PRIVATE" (custom) or "GLOBAL" (Bloomberg).
        group: Group name if screen is organized into groups.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns from the screen results (security, fieldData, etc.).

    Example::

        # Run a private screen
        df = await abeqs("MyScreen")

        # Run with as-of date
        df = await abeqs("MyScreen", asof="20240101")

        # Run a Bloomberg global screen
        df = await abeqs("TOP_DECL_DVD", screen_type="GLOBAL")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abeqs"], locals())


# =============================================================================
# BLKP API - Bloomberg Security Lookup
# =============================================================================


async def ablkp(
    query: str,
    *,
    yellowkey: str = "YK_FILTER_NONE",
    language: str = "LANG_OVERRIDE_NONE",
    max_results: int = 20,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg security lookup (BLKP) request.

    Search for securities by company name or partial ticker.

    Args:
        query: Search query (company name or partial ticker).
        yellowkey: Asset class filter. Common values:
            - "YK_FILTER_NONE" (default, all asset classes)
            - "YK_FILTER_EQTY" (equities only)
            - "YK_FILTER_CORP" (corporate bonds)
            - "YK_FILTER_GOVT" (government bonds)
            - "YK_FILTER_INDX" (indices)
            - "YK_FILTER_CURR" (currencies)
            - "YK_FILTER_CMDT" (commodities)
        language: Language override for results.
        max_results: Maximum number of results (default: 20, max: 1000).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns: security, description, and other result fields.

    Example::

        # Search for Apple
        df = await ablkp("Apple")

        # Search for equities only
        df = await ablkp("NVDA", yellowkey="YK_FILTER_EQTY")

        # Get more results
        df = await ablkp("Microsoft", max_results=50)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["ablkp"], locals())


# =============================================================================
# BPORT API - Bloomberg Portfolio Data
# =============================================================================


async def abport(
    portfolio: str,
    fields: str | Sequence[str],
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg portfolio data (BPORT) request.

    Get portfolio holdings and related data using PortfolioDataRequest.

    Args:
        portfolio: Bloomberg PortfolioDataRequest security/portfolio ID string, not the PORT display name
            (for example, "UXXXXXXX-X Client" from PRTU/PORT).
        fields: Field name or list of fields (e.g., "PORTFOLIO_MWEIGHT").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters/overrides.

    Returns:
        DataFrame with portfolio data.

    Example::

        # Get portfolio overview data. Use the Bloomberg portfolio ID/security
        # string, not the human-readable PORT display name.
        df = await abport("UXXXXXXX-X Client", "PORTFOLIO_DATA")

        # Get portfolio weights
        df = await abport("UXXXXXXX-X Client", "PORTFOLIO_MWEIGHT")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abport"], locals())


# =============================================================================
# BCURVES API - Bloomberg Yield Curve List
# =============================================================================


async def abcurves(
    *,
    country: str | None = None,
    currency: str | None = None,
    curve_type: str | None = None,
    subtype: str | None = None,
    curveid: str | None = None,
    bbgid: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg yield curve list (BCURVES) request.

    Search for yield curves by country, currency, type, or other filters.

    Args:
        country: Country code filter (e.g., "US", "GB", "DE").
        currency: Currency code filter (e.g., "USD", "EUR", "GBP").
        curve_type: Curve type filter (e.g., "GOVERNMENT", "CORPORATE").
        subtype: Curve subtype filter.
        curveid: Specific curve ID to look up.
        bbgid: Bloomberg Global ID filter.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with yield curve information.

    Example::

        # List US yield curves
        df = await abcurves(country="US")

        # List USD government curves
        df = await abcurves(currency="USD", curve_type="GOVERNMENT")

        # Look up specific curve
        df = await abcurves(curveid="YCSW0023 Index")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abcurves"], locals())


# =============================================================================
# BGOVTS API - Bloomberg Government Securities List
# =============================================================================


async def abgovts(
    query: str | None = None,
    *,
    partial_match: bool = True,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg government securities list (BGOVTS) request.

    Search for government securities by ticker or name.

    Args:
        query: Search query (ticker or partial name).
        partial_match: If True, match partial ticker names (default: True).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with government securities information.

    Example::

        # Search for US Treasury securities
        df = await abgovts("T")

        # Search for German government bonds
        df = await abgovts("DBR")

        # Exact match only
        df = await abgovts("T 2.5 05/15/24", partial_match=False)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abgovts"], locals())


async def _build_abdp_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    field_list = _normalize_fields(args.get("flds"))
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs

    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)
    fmt = Format(args["format"]) if isinstance(args.get("format"), str) else args.get("format")

    resolved_types = await _resolve_field_types_cached(
        field_list,
        args.get("field_types"),
        "string",
    )

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "overrides": overrides if overrides else None,
            "security_overrides": security_overrides,
            "elements": elements if elements else None,
            "field_types": resolved_types,
            "format": fmt,
            "include_security_errors": args.get("include_security_errors", False),
            "return_eids": args.get("return_eids", False),
            "validate_fields": args.get("validate_fields"),
        },
        backend=args.get("backend"),
        postprocess=None,
    )


async def _build_abdh_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    field_list = _normalize_fields(args.get("flds"))
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs
    presentation = _pop_presentation_aliases(kwargs)

    fmt = Format(args["format"]) if isinstance(args.get("format"), str) else args.get("format")
    fmt = _presentation_format(fmt, presentation)

    end_value = args.get("end_date", "today")
    start_value = args.get("start_date")

    # ``end_date`` defaults to "today" via the public signature, but callers may
    # explicitly pass ``end_date=None``; preserve the legacy "today" fallback in
    # that case so default ``bdh()`` calls remain unchanged.
    e_dt = _fmt_date(end_value, "%Y%m%d", default_today_on_none=True)
    if start_value is None:
        end_dt_parsed = datetime.strptime(e_dt, "%Y%m%d")
        s_dt = (end_dt_parsed - timedelta(weeks=8)).strftime("%Y%m%d")
    else:
        s_dt = _fmt_date(start_value, "%Y%m%d")

    options: list[tuple[str, str]] = []
    adjust = kwargs.pop("adjust", None)
    if adjust == "all":
        options.extend(
            [
                ("adjustmentSplit", "true"),
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "dvd":
        options.extend(
            [
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "split":
        options.append(("adjustmentSplit", "true"))

    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.HISTORICAL_DATA, kwargs)
    presentation_periodicity = _periodicity_selection(elements)

    resolved_types = await _resolve_field_types_cached(
        field_list,
        args.get("field_types"),
        "float64",
    )

    backend = args.get("backend")
    needs_presentation_postprocess = (
        presentation.show_date is not None
        or presentation.sort is not None
        or presentation.date_format in {"PERIODIC", "BOTH"}
    )

    def postprocess(raw: Any) -> DataFrameResult:
        shaped = _apply_historical_presentation(
            raw,
            presentation,
            periodicity=presentation_periodicity,
        )
        return _convert_result_backend(shaped, backend)

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "start_date": s_dt,
            "end_date": e_dt,
            "overrides": overrides if overrides else None,
            "security_overrides": security_overrides,
            "elements": elements if elements else None,
            "options": options if options else None,
            "field_types": resolved_types,
            "format": fmt,
            "validate_fields": args.get("validate_fields"),
            "return_eids": args.get("return_eids", False),
        },
        backend=backend,
        postprocess=postprocess if needs_presentation_postprocess else None,
    )


async def _build_abds_plan(args: dict[str, Any]) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    kwargs = dict(args.get("kwargs", {}))
    override_pairs, security_overrides = _normalize_request_overrides(kwargs.pop("overrides", None))
    if override_pairs is not None:
        kwargs["overrides"] = override_pairs
    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)

    req: dict[str, Any] = {
        "securities": ticker_list,
        "fields": [args["flds"]],
        "overrides": overrides if overrides else None,
        "security_overrides": security_overrides,
        "elements": elements if elements else None,
        "validate_fields": args.get("validate_fields"),
    }
    if args.get("return_eids"):
        req["return_eids"] = True

    return _EndpointPlan(request_kwargs=req, backend=args.get("backend"))


async def _build_abdib_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))

    start_dt = args.get("start_datetime")
    end_dt = args.get("end_datetime")
    dt_value = args.get("dt")

    if start_dt is not None and end_dt is not None:
        # Preserve any tz info the caller supplied; let the Rust engine
        # handle naive strings according to ``request_tz``.
        s_dt = _fmt_datetime(start_dt, default_tz=None)
        e_dt = _fmt_datetime(end_dt, default_tz=None)
    elif dt_value is not None:
        cur_dt = _fmt_date(dt_value, "%Y-%m-%d")
        s_dt = f"{cur_dt}T00:00:00"
        e_dt = f"{cur_dt}T23:59:59"
    else:
        raise ValueError("Either dt or both start_datetime and end_datetime must be provided")

    interval = args["interval"]
    alias_interval = _pop_element_alias(kwargs, "interval")
    if alias_interval is not None:
        interval = int(alias_interval)

    event_type = args["typ"]
    alias_event_type = _pop_element_alias(kwargs, "eventType")
    if alias_event_type is not None:
        event_type = str(alias_event_type)

    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.INTRADAY_BAR, kwargs)

    req: dict[str, Any] = {
        "security": args["ticker"],
        "event_type": event_type,
        "interval": interval,
        "start_datetime": s_dt,
        "end_datetime": e_dt,
        "elements": elements if elements else None,
        "overrides": overrides if overrides else None,
    }
    if args.get("return_eids"):
        req["return_eids"] = True
    if args.get("request_tz") is not None:
        req["request_tz"] = args["request_tz"]
    if args.get("output_tz") is not None:
        req["output_tz"] = args["output_tz"]

    return _EndpointPlan(
        request_kwargs=req,
        backend=args.get("backend"),
    )


async def _build_abdtick_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))

    # Accept native datetime/date plus duck-typed pd.Timestamp; preserve any
    # tz info the caller supplied so naive strings keep being interpreted by
    # the Rust engine according to ``request_tz``.
    s_dt = _fmt_datetime(args["start_datetime"], default_tz=None)
    e_dt = _fmt_datetime(args["end_datetime"], default_tz=None)

    alias_event_type = _pop_element_alias(kwargs, "eventType")
    event_types = args.get("event_types")
    if event_types is None:
        event_types = [str(alias_event_type)] if alias_event_type is not None else ["TRADE"]

    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.INTRADAY_TICK, kwargs)

    req: dict[str, Any] = {
        "security": args["ticker"],
        "start_datetime": s_dt,
        "end_datetime": e_dt,
        "event_types": list(event_types),
        "elements": elements if elements else None,
        "overrides": overrides if overrides else None,
    }
    if args.get("return_eids"):
        req["return_eids"] = True
    if args.get("request_tz") is not None:
        req["request_tz"] = args["request_tz"]
    if args.get("output_tz") is not None:
        req["output_tz"] = args["output_tz"]

    return _EndpointPlan(
        request_kwargs=req,
        backend=args.get("backend"),
    )


def _build_abql_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _EndpointPlan(
        request_kwargs={"overrides": {"expression": args["expression"]}},
        backend=args.get("backend"),
    )


async def _build_abqr_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    event_types = args.get("event_types")
    if event_types is None:
        event_types = ["BID", "ASK"]

    now = datetime.now()
    time_fmt = "%Y-%m-%dT%H:%M:%S"

    def fmt_bqr_datetime(value: Any, default_time: str) -> str:
        # Native types (datetime / date / pd.Timestamp).
        if not isinstance(value, str):
            if isinstance(value, datetime):
                return value.strftime(time_fmt)
            if isinstance(value, date):
                return _fmt_date(value, "%Y-%m-%d") + default_time
            if hasattr(value, "to_pydatetime"):
                coerced = value.to_pydatetime()
                if isinstance(coerced, datetime):
                    return coerced.strftime(time_fmt)
                if isinstance(coerced, date):
                    return _fmt_date(coerced, "%Y-%m-%d") + default_time
        text = str(value).replace(" ", "T")
        if "T" in text:
            return datetime.fromisoformat(text).strftime(time_fmt)
        return _fmt_date(value, "%Y-%m-%d") + default_time

    date_offset = args.get("date_offset")
    start_datetime = kwargs.pop("start_datetime", None)
    end_datetime = kwargs.pop("end_datetime", None)
    start_date = args.get("start_date")
    end_date = args.get("end_date")

    if date_offset:
        end_dt = now
        start_dt = _parse_date_offset(date_offset, now)
        s_dt = start_dt.strftime(time_fmt)
        e_dt = end_dt.strftime(time_fmt)
    elif start_datetime is not None:
        s_dt = fmt_bqr_datetime(start_datetime, "T00:00:00")
        e_dt = fmt_bqr_datetime(end_datetime, "T23:59:59") if end_datetime is not None else now.strftime(time_fmt)
    elif start_date is not None:
        s_dt = fmt_bqr_datetime(start_date, "T00:00:00")
        e_dt = fmt_bqr_datetime(end_date, "T23:59:59") if end_date is not None else now.strftime(time_fmt)
    else:
        start_dt = now - timedelta(days=2)
        s_dt = start_dt.strftime(time_fmt)
        e_dt = now.strftime(time_fmt)

    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.INTRADAY_TICK, kwargs)

    def upsert_element(name: str, value: Any) -> None:
        for idx, (existing_name, _) in enumerate(elements):
            if existing_name == name:
                elements[idx] = (name, value)
                return
        elements.append((name, value))

    include_broker_codes = bool(args.get("include_broker_codes"))
    if include_broker_codes:
        upsert_element("includeBrokerCodes", "true")
    if args.get("include_spread_price"):
        upsert_element("includeSpreadPrice", "true")
    if args.get("include_yield"):
        upsert_element("includeYield", "true")
    if args.get("include_condition_codes"):
        upsert_element("includeConditionCodes", "true")
    if args.get("include_exchange_codes"):
        upsert_element("includeExchangeCodes", "true")

    ticker = args["ticker"]
    backend = args.get("backend")
    if include_broker_codes:
        _warn_bqr_dealer_input(ticker, stacklevel=4)

    logger.debug(
        "abqr: ticker=%s start=%s end=%s events=%s",
        ticker,
        s_dt,
        e_dt,
        event_types,
    )

    def postprocess(nw_df: Any) -> DataFrameResult:
        logger.debug("abqr: received %d rows", ensure_arrow_table(nw_df).num_rows)
        return _postprocess_bqr_result(
            nw_df,
            ticker=ticker,
            backend=backend,
            enforce_broker_codes=include_broker_codes,
        )

    return _EndpointPlan(
        request_kwargs={
            "security": ticker,
            "start_datetime": s_dt,
            "end_datetime": e_dt,
            "event_types": list(event_types),
            "elements": elements if elements else None,
            "overrides": overrides if overrides else None,
        },
        backend=backend,
        postprocess=postprocess,
    )


def _normalize_grid_override_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _iter_grid_override_pairs(value: Any) -> Iterable[tuple[Any, Any]]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return value.items()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        pairs: list[tuple[Any, Any]] = []
        for item in value:
            if isinstance(item, (str, bytes, bytearray)) or not isinstance(item, Sequence) or len(item) != 2:
                raise TypeError("bsrch overrides must be a mapping or a sequence of (name, value) pairs")
            pairs.append((item[0], item[1]))
        return pairs
    raise TypeError("bsrch overrides must be a mapping or a sequence of (name, value) pairs")


def _build_absrch_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    domain = str(args["domain"])
    grid_overrides: list[tuple[str, str]] = []

    def add_grid_override(key: Any, value: Any) -> None:
        nonlocal domain
        name = str(key)
        if name.lower() == "domain":
            domain = str(value)
            return
        grid_overrides.append((name, _normalize_grid_override_value(value)))

    for key, value in _iter_grid_override_pairs(kwargs.pop("overrides", None)):
        add_grid_override(key, value)
    for key, value in kwargs.items():
        add_grid_override(key, value)

    request_kwargs: dict[str, Any] = {"elements": [("Domain", domain)]}
    if grid_overrides:
        request_kwargs["overrides"] = grid_overrides

    return _EndpointPlan(
        request_kwargs=request_kwargs,
        backend=args.get("backend"),
    )


async def _build_abeqs_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.BEQS, kwargs)

    elements: list[tuple[str, Any]] = [
        ("screenName", args["screen"]),
        ("screenType", args["screen_type"]),
        ("Group", args["group"]),
    ]
    if args.get("asof"):
        elements.append(("asOfDate", _fmt_date(args["asof"])))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={
            "elements": elements,
            "overrides": overrides if overrides else None,
        },
        backend=args.get("backend"),
    )


async def _build_ablkp_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _aroute_kwargs(Service.INSTRUMENTS, Operation.INSTRUMENT_LIST, kwargs)

    elements: list[tuple[str, Any]] = [
        ("query", args["query"]),
        ("yellowKeyFilter", args["yellowkey"]),
        ("languageOverride", args["language"]),
        ("maxResults", args["max_results"]),
    ]
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements},
        backend=args.get("backend"),
    )


async def _build_abport_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    field_list = _normalize_fields(args["fields"])
    elements, overrides = await _aroute_kwargs(Service.REFDATA, Operation.PORTFOLIO_DATA, kwargs)

    return _EndpointPlan(
        request_kwargs={
            "securities": [args["portfolio"]],
            "fields": field_list,
            "elements": elements if elements else None,
            "overrides": overrides if overrides else None,
        },
        backend=args.get("backend"),
    )


async def _build_abcurves_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _aroute_kwargs(Service.INSTRUMENTS, Operation.CURVE_LIST, kwargs)

    elements: list[tuple[str, Any]] = []
    if args.get("country") is not None:
        elements.append(("countryCode", args["country"]))
    if args.get("currency") is not None:
        elements.append(("currencyCode", args["currency"]))
    if args.get("curve_type") is not None:
        elements.append(("type", args["curve_type"]))
    if args.get("subtype") is not None:
        elements.append(("subtype", args["subtype"]))
    if args.get("curveid") is not None:
        elements.append(("curveid", args["curveid"]))
    if args.get("bbgid") is not None:
        elements.append(("bbgid", args["bbgid"]))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements if elements else None},
        backend=args.get("backend"),
    )


async def _build_abgovts_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await _aroute_kwargs(Service.INSTRUMENTS, Operation.GOVT_LIST, kwargs)

    elements: list[tuple[str, Any]] = []
    if args.get("query") is not None:
        elements.append(("ticker", args["query"]))
    elements.append(("partialMatch", args["partial_match"]))
    elements.extend(routed_elements)

    return _EndpointPlan(
        request_kwargs={"elements": elements if elements else None},
        backend=args.get("backend"),
    )


def _build_abflds_plan(args: dict[str, Any]) -> _EndpointPlan:
    fields = args.get("fields")
    search_spec = args.get("search_spec")

    if fields is not None and search_spec is not None:
        raise ValueError("Cannot specify both 'fields' and 'search_spec'")
    if fields is None and search_spec is None:
        raise ValueError("Must specify either 'fields' or 'search_spec'")

    if fields is not None:
        field_list = _normalize_fields(fields)
        return _EndpointPlan(
            request_kwargs={"fields": field_list},
            backend=args.get("backend"),
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
        )

    return _EndpointPlan(
        request_kwargs={"fields": [search_spec]},
        backend=args.get("backend"),
        service=Service.APIFLDS,
        operation=Operation.FIELD_SEARCH,
        extractor=ExtractorHint.FIELD_INFO,
    )


_GENERATED_ENDPOINT_SPECS.update(
    {
        "abdp": _GeneratedEndpointSpec(
            async_name="abdp",
            sync_name="bdp",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abdp_plan,
        ),
        "abdh": _GeneratedEndpointSpec(
            async_name="abdh",
            sync_name="bdh",
            service=Service.REFDATA,
            operation=Operation.HISTORICAL_DATA,
            builder=_build_abdh_plan,
        ),
        "abds": _GeneratedEndpointSpec(
            async_name="abds",
            sync_name="bds",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abds_plan,
            extractor=ExtractorHint.BULK,
        ),
        "abdib": _GeneratedEndpointSpec(
            async_name="abdib",
            sync_name="bdib",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_BAR,
            builder=_build_abdib_plan,
        ),
        "abdtick": _GeneratedEndpointSpec(
            async_name="abdtick",
            sync_name="bdtick",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abdtick_plan,
        ),
        "abql": _GeneratedEndpointSpec(
            async_name="abql",
            sync_name="bql",
            service=Service.BQLSVC,
            operation=Operation.BQL_SEND_QUERY,
            builder=_build_abql_plan,
            extractor=ExtractorHint.BQL,
        ),
        "abqr": _GeneratedEndpointSpec(
            async_name="abqr",
            sync_name="bqr",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abqr_plan,
        ),
        "absrch": _GeneratedEndpointSpec(
            async_name="absrch",
            sync_name="bsrch",
            service=Service.EXRSVC,
            operation=Operation.EXCEL_GET_GRID,
            builder=_build_absrch_plan,
            extractor=ExtractorHint.BSRCH,
        ),
        "abeqs": _GeneratedEndpointSpec(
            async_name="abeqs",
            sync_name="beqs",
            service=Service.REFDATA,
            operation=Operation.BEQS,
            builder=_build_abeqs_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "ablkp": _GeneratedEndpointSpec(
            async_name="ablkp",
            sync_name="blkp",
            service=Service.INSTRUMENTS,
            operation=Operation.INSTRUMENT_LIST,
            builder=_build_ablkp_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abport": _GeneratedEndpointSpec(
            async_name="abport",
            sync_name="bport",
            service=Service.REFDATA,
            operation=Operation.PORTFOLIO_DATA,
            builder=_build_abport_plan,
        ),
        "abcurves": _GeneratedEndpointSpec(
            async_name="abcurves",
            sync_name="bcurves",
            service=Service.INSTRUMENTS,
            operation=Operation.CURVE_LIST,
            builder=_build_abcurves_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abgovts": _GeneratedEndpointSpec(
            async_name="abgovts",
            sync_name="bgovts",
            service=Service.INSTRUMENTS,
            operation=Operation.GOVT_LIST,
            builder=_build_abgovts_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abflds": _GeneratedEndpointSpec(
            async_name="abflds",
            sync_name="bflds",
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
            builder=_build_abflds_plan,
        ),
    }
)

_install_generated_endpoints()

# Backward-compatible aliases
abfld = abflds
bfld = globals()["bflds"]


async def afieldInfo(
    fields: str | list[str],
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Get metadata about Bloomberg fields (async).

    Convenience wrapper around abflds(fields=...).

    Args:
        fields: Single field or list of fields to get metadata for.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options.

    Returns:
        DataFrame with field information.

    Example::

        df = await afieldInfo(["PX_LAST", "VOLUME"])
    """
    return await abflds(fields=fields, backend=backend, **kwargs)


async def afieldSearch(
    searchterm: str,
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Search for Bloomberg fields by keyword (async).

    Convenience wrapper around abflds(search_spec=...).

    Args:
        searchterm: Search term to find fields by name/description.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options.

    Returns:
        DataFrame with search results.

    Example::

        df = await afieldSearch("vwap")
    """
    return await abflds(search_spec=searchterm, backend=backend, **kwargs)


# ─── Schema Introspection API ────────────────────────────────────────────────


async def abops(service: str | Service = Service.REFDATA) -> list[str]:
    """List available operations for a Bloomberg service (async).

    Args:
        service: Service URI or Service enum (default: //blp/refdata)

    Returns:
        List of operation names.

    Example::

        >>> ops = await abops()
        >>> print(ops)
        ['ReferenceDataRequest', 'HistoricalDataRequest', ...]

        >>> ops = await abops("//blp/instruments")
        >>> print(ops)
        ['InstrumentListRequest', ...]
    """
    from . import schema

    service_uri = service.value if isinstance(service, Service) else service
    return await schema.alist_operations(service_uri)


async def abschema(
    service: str | Service = Service.REFDATA,
    operation: str | Operation | None = None,
) -> dict:
    """Get Bloomberg service or operation schema (async).

    Returns introspected schema with element definitions, types, and enum values.
    Schemas are cached locally (~/.xbbg/schemas/) for fast subsequent access.

    Args:
        service: Service URI or Service enum (default: //blp/refdata)
        operation: Optional operation name. If None, returns full service schema.

    Returns:
        Dictionary with schema information:
        - If operation is None: Full service schema with all operations
        - If operation is specified: Just that operation's request/response schema

    Example::

        >>> # Get full service schema
        >>> schema = await abschema()
        >>> print(schema['operations'][0]['name'])
        'ReferenceDataRequest'

        >>> # Get specific operation schema
        >>> op_schema = await abschema(operation="ReferenceDataRequest")
        >>> print(op_schema['request']['children'][0]['name'])
        'securities'

        >>> # Get enum values for an element
        >>> op = await abschema(operation="HistoricalDataRequest")
        >>> for child in op['request']['children']:
        ...     if child.get('enum_values'):
        ...         print(f"{child['name']}: {child['enum_values']}")
    """
    from . import schema

    service_uri = service.value if isinstance(service, Service) else service

    if operation is not None:
        op_name = operation.value if isinstance(operation, Operation) else operation
        op_schema = await schema.aget_operation(service_uri, op_name)
        return {
            "name": op_schema.name,
            "description": op_schema.description,
            "request": _element_to_dict(op_schema.request),
            "responses": [_element_to_dict(r) for r in op_schema.responses],
        }
    svc_schema = await schema.aget_schema(service_uri)
    return {
        "service": svc_schema.service,
        "description": svc_schema.description,
        "operations": [
            {
                "name": op.name,
                "description": op.description,
                "request": _element_to_dict(op.request),
                "responses": [_element_to_dict(r) for r in op.responses],
            }
            for op in svc_schema.operations
        ],
        "cached_at": svc_schema.cached_at,
    }


def _install_manual_sync_wrappers() -> None:
    for sync_name, async_func in (
        ("request", arequest),
        ("seat_type", aseat_type),
        ("check_entitlements", acheck_entitlements),
        ("identity_is_authorized", aidentity_is_authorized),
        ("subscribe", asubscribe),
        ("vwap", avwap),
        ("mktbar", amktbar),
        ("depth", adepth),
        ("chains", achains),
        ("bta", abta),
        ("fieldInfo", afieldInfo),
        ("fieldSearch", afieldSearch),
        ("bops", abops),
        ("bschema", abschema),
    ):
        globals()[sync_name] = _build_sync_wrapper(
            sync_name,
            async_func,
            allow_notebook_bridge=sync_name in _NOTEBOOK_SYNC_BRIDGE_NAMES,
        )


_install_manual_sync_wrappers()


def _element_to_dict(elem) -> dict:
    """Convert ElementInfo to dictionary."""
    return {
        "name": elem.name,
        "description": elem.description,
        "data_type": elem.data_type,
        "type_name": elem.type_name,
        "is_array": elem.is_array,
        "is_optional": elem.is_optional,
        "enum_values": elem.enum_values,
        "children": [_element_to_dict(c) for c in elem.children],
    }
