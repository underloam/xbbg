"""Subscription lifecycle and synchronous/asynchronous streaming.

Topic normalization, sparse tick conversion, warnings, backpressure, and producer
cancellation live together. Sync streams use the same managed bridge as request
wrappers; all engine access goes through the lower engine module.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
import concurrent.futures
import contextvars
from dataclasses import dataclass
from datetime import datetime
import logging
import sys
import threading
from typing import Any, TypeAlias
import warnings

from . import _engine, _request_options, _sync, backend as _backend
from .backend import Backend
from .services import OVERFLOW_POLICIES, OVERFLOW_POLICY_VALUES, Service

DataFrameResult: TypeAlias = Any
logger = logging.getLogger(__name__)


_DEFAULT_SYNC_STREAM_CAPACITY = 256
_DEFAULT_MAX_SUBSCRIPTION_SESSIONS = 32
_SYNC_STREAM_SESSION_WAIT_MS = 5000
_SYNC_STREAM_CLOSE_TIMEOUT_SECONDS = 1.0
_SUBSCRIPTION_WARNING_SINK: contextvars.ContextVar[Callable[[str, type[Warning]], None] | None] = (
    contextvars.ContextVar("xbbg_subscription_warning_sink", default=None)
)
_SYNC_STREAM_SESSION_LIMIT: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "xbbg_sync_stream_session_limit", default=None
)

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
    return [_normalize_service_topic(service, ticker, label) for ticker in _request_options._normalize_tickers(tickers)]


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


def subscription_feeds() -> list[dict[str, Any]]:
    """Return retained upstream feeds for the active engine.

    Each row contains ``service``, ``topic``, ``options`` (list), ``fields``
    (union list), ``consumers``, ``delayed`` (bool or None), ``state``,
    ``isolated``, and ``field_errors`` (field-to-category dict). Closed feeds
    leave the registry; this is a current snapshot, not an event history.
    """
    return _engine._get_engine().subscription_feeds()


def _warn_subscription(message: str, category: type[Warning]) -> None:
    sink = _SUBSCRIPTION_WARNING_SINK.get()
    if sink is not None:
        sink(message, category)
        return
    frame = sys._getframe(1)
    stacklevel = 2
    while frame is not None and (
        frame.f_globals.get("__name__", "") == "xbbg" or frame.f_globals.get("__name__", "").startswith("xbbg.")
    ):
        stacklevel += 1
        frame = frame.f_back
    warnings.warn(message, category, stacklevel=stacklevel)


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
        self._topic_normalizer = topic_normalizer or _request_options._normalize_tickers
        self._convert_batch = self._build_batch_converter()

    def _build_batch_converter(self) -> Callable[[Any], Any]:
        if self._raw:
            return lambda batch: batch
        if self._backend is None:
            return lambda batch: batch.to_table()
        converter = _backend.convert_backend_frame
        backend = self._backend
        return lambda batch: converter(batch.to_table(), backend)

    def __aiter__(self):
        if not self._sub.delivers_rows:
            raise RuntimeError("rows=False subscriptions do not yield rows; use latest() instead")
        return self

    async def __anext__(self) -> Any:
        try:
            if not self._sub.delivers_rows:
                raise RuntimeError("rows=False subscriptions do not yield rows; use latest() instead")
            if self._tick_mode:
                return await self._sub.__anext_tick_dict__()

            batch = await self._sub.__anext__()
        except Exception as exc:
            mapped = _engine._normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

        finally:
            self._emit_warnings()
        return self._convert_batch(batch)

    def _emit_warnings(self) -> None:
        pending = self._sub.take_warnings()
        if not pending:
            return
        from .exceptions import BlpDelayedDataWarning, BlpFieldWarning, BlpSubscriptionWarning

        categories = {"DelayedStream": BlpDelayedDataWarning, "FieldException": BlpFieldWarning}
        for event in pending:
            kind = event["message_type"]
            detail = event.get("detail") or kind
            topic = event.get("topic")
            message = f"{topic}: {detail}" if topic else detail
            _warn_subscription(message, categories.get(kind, BlpSubscriptionWarning))

    async def add(self, tickers: str | list[str], aliases: Mapping[str, str] | None = None) -> None:
        """Add tickers to subscription dynamically.

        Args:
            tickers: Single ticker or list of tickers to add
            aliases: Bloomberg topic to consumer label mapping. Labels appear
                in rows, status, and ``remove``.
        """
        ticker_list = self._topic_normalizer(tickers)
        logger.debug("subscription add: %s", ticker_list)
        try:
            native_aliases = (
                None
                if aliases is None
                else {self._topic_normalizer(topic)[0]: label for topic, label in aliases.items()}
            )
            await self._sub.add(ticker_list, aliases=native_aliases)
        except Exception as exc:
            mapped = _engine._normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        finally:
            self._emit_warnings()

    async def add_fields(self, fields: str | Sequence[str]) -> None:
        """Add projected fields; shared upstream field unions grow but never shrink."""
        try:
            await self._sub.add_fields(_request_options._normalize_fields(fields))
        except Exception as exc:
            mapped = _engine._normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        finally:
            self._emit_warnings()

    def latest(self, backend: Backend | str | None = None) -> DataFrameResult:
        """Return the materialized image, independently of the iteration output mode.

        Columns are ``topic`` (consumer label), ``last_update`` (UTC),
        ``live``, ``delayed``, and projected data fields. Unknown values and
        explicit clears are null. ``backend`` follows ordinary request results.
        """
        try:
            batch = self._sub.latest()
            return _backend._convert_result_backend(batch, backend)
        except Exception as exc:
            mapped = _engine._normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        finally:
            self._emit_warnings()

    async def remove(self, tickers: str | list[str]) -> None:
        """Remove consumer labels (or unaliased tickers) dynamically.

        Args:
            tickers: Single consumer label or list of labels to remove
        """
        active_labels = set(self.tickers)
        ticker_list = [
            ticker if ticker in active_labels else self._topic_normalizer(ticker)[0]
            for ticker in _request_options._normalize_tickers(tickers)
        ]
        logger.debug("subscription remove: %s", ticker_list)
        try:
            await self._sub.remove(ticker_list)
        except Exception as exc:
            mapped = _engine._normalize_engine_exception(exc)
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
    def topic_states(self) -> dict[str, dict[str, int | str | bool | None]]:
        """Topic lifecycle, delayed flag, and upstream topic keyed by consumer label."""
        return {
            ticker: {
                "state": state,
                "last_change_us": last_change_us,
                "delayed": delayed,
                "feed_topic": feed_topic,
            }
            for ticker, state, last_change_us, delayed, feed_topic in self._sub.topic_states
        }

    @property
    def field_errors(self) -> dict[str, dict[str, str]]:
        """Rejected explicit fields, keyed by consumer label then field name."""
        return self._sub.field_errors

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
        """Bounded event history, including warnings and DataLoss/FeedRecovered notices."""
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
            "field_errors": self.field_errors,
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
            mapped = _engine._normalize_engine_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        finally:
            self._emit_warnings()
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
    aliases: Mapping[str, str] | None = None,
    on_delayed: str = "warn",
    isolated: bool = False,
    rows: bool = True,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
) -> Subscription:
    """Create an async subscription to real-time market data.

    This is the low-level subscription API with full control over
    the subscription lifecycle, including dynamic add/remove.

    Subscription recovery is handled automatically by the Bloomberg SDK (see
    BLPAPI ChangeLog v3.11.6); per-subscription availability transitions fire
    as ``SubscriptionStreamsActivated`` / ``SubscriptionStreamsDeactivated``
    events available in ``sub.events``.

    Dict ticks are sparse deltas: missing keys are unchanged, while present
    ``None`` values explicitly clear fields. Arrow batches append the binary
    ``__xbbg_present`` bitset (LSB-first; bit i maps to schema field i + 2).
    For row consumers, Bloomberg DATALOSS or local queue loss raises
    ``BlpSubscriptionDataLossError`` and ends the stream. Image-only consumers
    (``rows=False``) recover from DATALOSS with a fresh paint; ``sub.events``
    records DataLoss/FeedRecovered. Session termination does not auto-recover.
    Connection-down notifications alone remain nonterminal.

    Market-data feeds are shared within an engine by default. A late row consumer
    receives a projected image. Field-union repaints reach waiting consumers in
    full; existing consumers receive only changed projected values and clears.
    Filtered streams omit rows with none of their data fields.
    ``all_fields=True`` also exposes scalar fields induced by other consumers.
    Other services are always isolated and keep their service-specific semantics.

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
        options: List of Bloomberg subscription options.
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
        aliases: Bloomberg topic to consumer label mapping. Labels replace
            topics in rows, status, ``tickers``, and ``remove``.
        on_delayed: ``"warn"`` emits one BlpDelayedDataWarning per topic;
            ``"raise"`` rejects only that topic; ``"ignore"`` records the flag
            without a warning.
        isolated: If True, use private feeds and a dedicated subscription session.
        rows: If False, maintain an image without queuing rows. Iteration raises
            RuntimeError; read ``latest()`` instead. Image-only consumers cannot
            overflow a row queue.
        on_field_error: ``"warn"`` emits BlpFieldWarning for rejected explicit
            fields; ``"raise"`` rejects the affected topic; ``"ignore"`` records
            ``field_errors`` without warnings.
        zero_as_null: Field names whose numeric zero values become null in
            ``latest()`` only. Rows and stored images remain unchanged.

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
        topic_normalizer = _request_options._normalize_tickers

    ticker_list = topic_normalizer(tickers)
    field_list = _request_options._normalize_fields(fields)
    if subscription_service == Service.MKTBAR.value:
        if field_list != ["LAST_PRICE"]:
            raise ValueError("//blp/mktbar subscriptions must request only LAST_PRICE")
        _validate_mktbar_options(subscription_options)
    elif subscription_service == Service.MKTVWAP.value and field_list != ["VWAP"]:
        raise ValueError("//blp/mktvwap subscriptions must request only VWAP")

    effective_backend = None if backend is None else _backend.resolve_backend(backend, None)

    engine = _engine._get_engine()
    logger.debug("subscribe: tickers=%s fields=%s", ticker_list, field_list)

    session_limit = _SYNC_STREAM_SESSION_LIMIT.get()
    native_kwargs: dict[str, Any] = {
        "all_fields": all_fields,
        "aliases": None if aliases is None else {topic_normalizer(topic)[0]: label for topic, label in aliases.items()},
        "on_delayed": on_delayed,
        "isolated": isolated,
        "rows": rows,
        "on_field_error": on_field_error,
        "zero_as_null": None if zero_as_null is None else _request_options._normalize_fields(zero_as_null),
        "session_wait_ms": _SYNC_STREAM_SESSION_WAIT_MS if session_limit is not None else None,
    }
    try:
        if (
            subscription_service is not None
            or subscription_options is not None
            or flush_threshold is not None
            or stream_capacity is not None
            or overflow_policy is not None
        ):
            native_kwargs.update(
                (key, value)
                for key, value in {
                    "flush_threshold": flush_threshold,
                    "stream_capacity": stream_capacity,
                    "overflow_policy": overflow_policy,
                }.items()
                if value is not None
            )
            py_sub = await engine.subscribe_with_options(
                subscription_service or "//blp/mktdata",
                ticker_list,
                field_list,
                subscription_options or [],
                **native_kwargs,
            )
        else:
            py_sub = await engine.subscribe(ticker_list, field_list, **native_kwargs)
    except Exception as exc:
        if session_limit is not None:
            from . import _core

            if isinstance(exc, _core.BlpValidationError) and str(exc).startswith("Configuration error: session_wait:"):
                raise RuntimeError(f"sync stream producer limit reached ({session_limit})") from exc
        raise

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
    aliases: Mapping[str, str] | None = None,
    on_delayed: str = "warn",
    isolated: bool = False,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
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
        aliases: Bloomberg topic to consumer label mapping.
        on_delayed: ``"warn"`` (default), ``"raise"`` (reject delayed topics),
            or ``"ignore"`` (flag only).
        isolated: Use private feeds instead of sharing market-data subscriptions.
        on_field_error: Rejected-field policy: ``"warn"``, ``"raise"``, or ``"ignore"``.
        zero_as_null: Fields whose numeric zeros are null in latest images only;
            emitted rows retain the original values.

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
        aliases=aliases,
        on_delayed=on_delayed,
        isolated=isolated,
        on_field_error=on_field_error,
        zero_as_null=zero_as_null,
    ) as sub:
        async for batch in sub:
            if callback is not None:
                try:
                    callback(batch)
                except Exception as e:
                    logger.warning("callback raised exception: %s", e, exc_info=True)
            yield batch


def _sync_stream_session_limit() -> int:
    """Capture the configured pool limit for sync claim-timeout diagnostics."""
    scoped = _engine._active_engine.get()
    if scoped is not None:
        config = getattr(scoped, "_config_snapshot", None)
    else:
        with _engine._engine_lock:
            config = _engine._config
    try:
        limit = int(getattr(config, "max_subscription_sessions", _DEFAULT_MAX_SUBSCRIPTION_SESSIONS))
    except (TypeError, ValueError):
        limit = _DEFAULT_MAX_SUBSCRIPTION_SESSIONS
    return max(1, limit)


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
    aliases: Mapping[str, str] | None = None,
    on_delayed: str = "warn",
    isolated: bool = False,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
):
    """High-level sync streaming using the managed background event loop.

    Use astream() directly from async contexts.

    New pool-session claims wait at most five seconds before raising the producer
    limit RuntimeError. Joining existing shared feeds needs no session claim and
    never waits for capacity. There is no separate Python producer counter.

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
        aliases: Bloomberg topic to consumer label mapping.
        on_delayed: ``"warn"`` (default), ``"raise"`` (reject delayed topics),
            or ``"ignore"`` (flag only).
        isolated: Use private feeds instead of sharing market-data subscriptions.
        on_field_error: Rejected-field policy: ``"warn"``, ``"raise"``, or ``"ignore"``.
        zero_as_null: Fields whose numeric zeros are null in latest images only;
            emitted rows retain the original values.

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

    pending_warnings: queue.SimpleQueue[tuple[str, type[Warning]]] = queue.SimpleQueue()

    def forward_warning(message: str, category: type[Warning]) -> None:
        pending_warnings.put((message, category))
        consumer_wakeup.set()

    def emit_pending_warnings() -> None:
        while True:
            try:
                message, category = pending_warnings.get_nowait()
            except queue.Empty:
                return
            _warn_subscription(message, category)

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
            aliases=aliases,
            on_delayed=on_delayed,
            isolated=isolated,
            on_field_error=on_field_error,
            zero_as_null=zero_as_null,
        )
        warning_token = _SUBSCRIPTION_WARNING_SINK.set(forward_warning)
        session_token = _SYNC_STREAM_SESSION_LIMIT.set(session_limit)
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
            try:
                await source.aclose()
            finally:
                _SUBSCRIPTION_WARNING_SINK.reset(warning_token)
                _SYNC_STREAM_SESSION_LIMIT.reset(session_token)

    session_limit = _sync_stream_session_limit()
    producer_call = _sync._notebook_sync_bridge.start(run_stream, (), {})

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
            producer_done.set()
            consumer_wakeup.set()

    producer_call.result.add_done_callback(producer_finished)

    try:
        while True:
            consumer_wakeup.clear()
            emit_pending_warnings()
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
            emit_pending_warnings()
            yield batch

        if producer_error is not None:
            raise producer_error
    finally:
        active_error = sys.exc_info()[1]
        cancel_requested = not producer_done.is_set()
        stop_event.set()
        if cancel_requested:
            _sync._notebook_sync_bridge.cancel(producer_call)
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
        finally:
            try:
                emit_pending_warnings()
            except Warning as warning_error:
                if active_error is not None and not isinstance(active_error, GeneratorExit):
                    raise active_error from warning_error
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

    effective_backend = _backend._resolve_backend(backend)

    engine = _engine._get_engine()
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
    effective_backend = _backend._resolve_backend(backend)

    options: list[str] = [f"bar_size={bar_size}"]
    if start_time:
        options.append(f"start_time={start_time}")
    if end_time:
        options.append(f"end_time={end_time}")

    engine = _engine._get_engine()
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
    ticker_list = _request_options._normalize_tickers(tickers)
    effective_backend = _backend._resolve_backend(backend)

    # Get engine and subscribe
    engine = _engine._get_engine()
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

    effective_backend = _backend._resolve_backend(backend)

    # Build subscription options
    options: list[str] = [f"chainType={chain_type}"]

    # Get engine and subscribe
    engine = _engine._get_engine()
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
