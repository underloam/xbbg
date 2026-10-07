"""High-level xbbg request API: reference, historical, intraday.

This module provides the xbbg-compatible API for authorized Bloomberg environments,
with support for multiple DataFrame backends via narwhals.

Architecture:
- ``_endpoints`` owns typed async functions and their ``_EndpointPlan`` builders.
  Only execution crosses back through the callback installed below, into
  ``arequest`` and its middleware; private modules never import this facade.
- ``_engine`` owns lifecycle and scoped routing; ``_sync`` owns the managed loop
  shared by sync request wrappers and ``_streaming`` subscription producers.
- ``_technical`` keeps study vocabulary, requests, and IDE stub generation together.
- Public names are re-exported here; only synchronous wrappers are generated.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import inspect
import logging
import time
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from xbbg.services import ExtractorHint, Format, Operation, OutputMode, RequestParams, Service

from . import _endpoints, _engine, _request_options, backend as _backend
from ._dates import DateLike, _fmt_date, _fmt_datetime
from ._endpoints import (
    abcurves as abcurves,
    abdh as abdh,
    abdib as abdib,
    abdp as abdp,
    abds as abds,
    abdtick as abdtick,
    abeqs as abeqs,
    abflds as abflds,
    abgovts as abgovts,
    ablkp as ablkp,
    abport as abport,
    abql as abql,
    abqr as abqr,
    absrch as absrch,
)
from ._engine import (
    Engine as Engine,
    configure as configure,
    is_connected as is_connected,
    reset as reset,
    shutdown as shutdown,
)
from ._exports import BLP_MODULE_EXPORTS
from ._request_options import OverrideSpec as OverrideSpec, ovr as ovr
from ._streaming import (
    Subscription as Subscription,
    Tick as Tick,
    TickValue as TickValue,
    achains as achains,
    adepth as adepth,
    amktbar as amktbar,
    astream as astream,
    asubscribe as asubscribe,
    avwap as avwap,
    stream as stream,
    subscription_feeds as subscription_feeds,
)
from ._sync import _build_sync_wrapper
from ._technical import (
    abta as abta,
    generate_ta_stubs as generate_ta_stubs,
    ta_studies as ta_studies,
    ta_study_params as ta_study_params,
)
from .backend import Backend, ensure_arrow_table, get_backend as get_backend, set_backend as set_backend
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


# Sync wrappers are installed from the original async endpoint functions.
if TYPE_CHECKING:
    from xbbg._core import EntitlementReport

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


async def aseat_type() -> str:
    """Return the seat type for the lazily authorized Bloomberg identity.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _engine._get_engine().seat_type()


async def acheck_entitlements(
    eids: Sequence[int],
    service: str = Service.REFDATA.value,
) -> EntitlementReport:
    """Check EID entitlements for the lazily authorized Bloomberg identity.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _engine._get_engine().check_entitlements(service, list(eids))


async def aidentity_is_authorized(service: str = Service.REFDATA.value) -> bool:
    """Return whether the lazily authorized identity is authorized for *service*.

    The engine authorizes on first use: configured auth is used when present,
    otherwise the Desktop terminal OS-logon user is used.  First use may take a
    moment and timeout failures are retryable.
    """
    return await _engine._get_engine().identity_is_authorized(service)


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
    scoped = _engine._active_engine.get()
    if scoped is not None:
        return _request_environment_from_config(
            getattr(scoped, "_config_snapshot", None),
            "scoped_engine",
        )

    with _engine._engine_lock:
        config = _engine._config
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


async def _execute_request_terminal(context: RequestContext) -> DataFrameResult:
    engine = _engine._get_engine()
    started = time.perf_counter()

    try:
        batch = await engine.request(context.to_dispatch_dict())
    except Exception as exc:
        mapped = _engine._normalize_engine_exception(exc)
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
    securities_list = _request_options._normalize_tickers(securities) if securities is not None else None
    fields_list = _request_options._normalize_fields(fields) if fields is not None else None

    overrides_list: list[tuple[str, str]] | None = None
    security_overrides_list: list[tuple[str, list[tuple[str, str]]]] | None = None
    elements_list: list[tuple[str, Any]] | None = None
    if security_overrides is not None:
        security_overrides_list = [
            (
                str(security_name),
                [
                    (str(key), _request_options._normalize_override_value(value))
                    for key, value in _request_options._validated_source_pairs(
                        security_values, _request_options._OVERRIDES_TYPE_ERROR
                    )
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
        override_tuples, override_security_overrides = _request_options._normalize_request_overrides(overrides)
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
    effective_backend = _backend._resolve_backend(backend)
    if effective_backend is None and params.output == OutputMode.ARROW:
        context.frame = table_result
        return table_result
    context.frame = _backend._convert_result_backend(table_result, effective_backend)
    return context.frame


async def _execute_generated_endpoint(
    spec: _endpoints._GeneratedEndpointSpec, call_args: dict[str, Any]
) -> DataFrameResult:
    plan_or_awaitable = spec.builder(call_args)
    if inspect.isawaitable(plan_or_awaitable):
        plan: _endpoints._EndpointPlan = cast("_endpoints._EndpointPlan", await plan_or_awaitable)
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

    return _backend._convert_result_backend(raw, plan.backend)


_endpoints._install_generated_endpoints(_execute_generated_endpoint, globals())

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
        globals()[sync_name] = _build_sync_wrapper(sync_name, async_func)


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
