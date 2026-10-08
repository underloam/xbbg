"""Engine ownership, scoped routing, and field-type cache lifecycle.

Request, schema, and subscription modules look up the same engine here. Keeping
this state below the facade avoids lazy import cycles and gives offline callers
one engine-construction seam to replace.
"""

from __future__ import annotations

import atexit
import contextvars
import logging
import sys
import threading
from typing import Any
import warnings

from ._sync import _stop_notebook_sync_loop

logger = logging.getLogger(__name__)


_FIELD_TYPE_RESOLUTION_CACHE_MAXSIZE = 1024
_FIELD_TYPE_RESOLUTION_CACHE: dict[tuple[tuple[str, ...], str], dict[str, str]] = {}
_FIELD_TYPE_RESOLUTION_CACHE_LOCK = threading.Lock()
_FIELD_TYPE_RESOLUTION_CACHE_GENERATION = 0


def _clear_field_type_resolution_cache() -> None:
    """Clear cached field type resolutions after schema/cache invalidation."""
    global _FIELD_TYPE_RESOLUTION_CACHE_GENERATION
    with _FIELD_TYPE_RESOLUTION_CACHE_LOCK:
        _FIELD_TYPE_RESOLUTION_CACHE.clear()
        _FIELD_TYPE_RESOLUTION_CACHE_GENERATION += 1


# Engine configuration (set before first use)
_config = None  # PyEngineConfig instance or None

# Lazy-load the engine to avoid import errors when the Rust module isn't built
_engine = None
_engine_lock = threading.Lock()

# Scoped engine for multi-engine routing (async-safe via contextvars)
_active_engine: contextvars.ContextVar[Engine | None] = contextvars.ContextVar("_active_engine", default=None)


class Engine:
    """Non-global Bloomberg engine for multi-source routing.

    Use as a context manager to scope all ``blp.*`` calls to this engine:

        engine = blp.Engine(host="localhost", auth_method="app", app_name="myapp")
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

    def subscription_feeds(self) -> list[dict[str, Any]]:
        """Return shared-feed diagnostics for this engine, without identity data."""
        return self._py_engine.subscription_feeds()

    def worker_health(self) -> list[tuple[int, str]]:
        """Return request-worker IDs and their current native health status."""
        return self._py_engine.worker_health()


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

    try:
        _stop_notebook_sync_loop()
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
