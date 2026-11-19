"""Testing helpers for non-live xbbg request and response flows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from . import _engine, blp
from .backend import _convert_result_backend


def _arrow_table_class():
    from xbbg._core import ArrowTable

    return ArrowTable


def _arrow_record_batch_class():
    from xbbg._core import ArrowRecordBatch

    return ArrowRecordBatch


def _require_blpapi():
    try:
        import blpapi  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "xbbg.testing requires the Bloomberg Python package for TestUtil-backed helpers. "
            "Install `blpapi` to use create_mock_event()/deserialize_service() helpers."
        ) from exc
    return blpapi


def create_mock_event(event_type: int):
    blpapi = _require_blpapi()
    return blpapi.test.createEvent(event_type)


def deserialize_service(service_xml: str):
    blpapi = _require_blpapi()
    return blpapi.test.deserializeService(service_xml)


def get_admin_message_definition(message_name: str | Any):
    blpapi = _require_blpapi()
    if isinstance(message_name, str):
        message_name = blpapi.Name(message_name)
    return blpapi.test.getAdminMessageDefinition(message_name)


def append_message_dict(event, element_def, payload: Mapping[str, Any], properties: Any | None = None):
    blpapi = _require_blpapi()
    formatter = blpapi.test.appendMessage(event, element_def, properties)
    formatter.formatMessageDict(dict(payload))
    return formatter


def _coerce_blpapi_service(service: str | Any, service_xml: str | None):
    if service_xml is not None:
        return deserialize_service(service_xml)

    if hasattr(service, "getOperation"):
        return service
    if not isinstance(service, str):
        raise TypeError("service must be a Bloomberg Service, service URI string, or XML-backed service")

    raise ValueError(
        "A raw service URI cannot be converted to a TestUtil service definition by itself. "
        "Pass `service_xml=` or a pre-built `blpapi.Service` when you need a real mock event."
    )


def _build_message_properties(
    *,
    correlation_ids: Sequence[Any] | None = None,
    request_id: str | None = None,
    service: Any | None = None,
):
    if correlation_ids is None and request_id is None and service is None:
        return None

    blpapi = _require_blpapi()
    props = blpapi.test.MessageProperties()
    if correlation_ids:
        props.setCorrelationIds(list(correlation_ids))
    if request_id:
        props.setRequestId(request_id)
    if service is not None:
        props.setService(service)
    return props


def _stringify_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _reference_rows(data: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    key_column = _engine._active_key_column()
    rows: list[dict[str, Any]] = []
    for ticker, fields in data.items():
        for field_name, value in fields.items():
            rows.append(
                {
                    key_column: ticker,
                    "field": field_name,
                    "value": _stringify_value(value),
                }
            )
    return rows


def _historical_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    key_column = _engine._active_key_column()
    rows: list[dict[str, Any]] = []
    for ticker, ticker_payload in data.items():
        if isinstance(ticker_payload, Mapping):
            for date_value, fields in ticker_payload.items():
                if not isinstance(fields, Mapping):
                    raise TypeError("HistoricalDataRequest mapping values must be field mappings")
                for field_name, value in fields.items():
                    rows.append(
                        {
                            key_column: ticker,
                            "date": str(date_value),
                            "field": field_name,
                            "value": _stringify_value(value),
                        }
                    )
        elif isinstance(ticker_payload, Sequence) and not isinstance(ticker_payload, (str, bytes, bytearray)):
            for entry in ticker_payload:
                if not isinstance(entry, Mapping):
                    raise TypeError("HistoricalDataRequest row entries must be mappings")
                date_value = entry.get("date")
                for key, value in entry.items():
                    if key == "date":
                        continue
                    rows.append(
                        {
                            key_column: ticker,
                            "date": str(date_value),
                            "field": key,
                            "value": _stringify_value(value),
                        }
                    )
        else:
            raise TypeError("Unsupported HistoricalDataRequest payload shape")
    return rows


def _rows_from_operation(operation: str, data: Any) -> list[dict[str, Any]]:
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        rows = [dict(row) for row in data]
        if not all(isinstance(row, Mapping) for row in data):
            raise TypeError("Explicit mock rows must be mappings")
        return rows

    if not isinstance(data, Mapping):
        raise TypeError("Mock response data must be a mapping or a list of row mappings")

    if operation == "ReferenceDataRequest":
        return _reference_rows(data)
    if operation == "HistoricalDataRequest":
        return _historical_rows(data)

    raise ValueError(
        f"No default row builder for operation {operation!r}. Pass an explicit list of row mappings for this operation."
    )


def _table_from_rows(rows: Sequence[Mapping[str, Any]]) -> Any:
    table_cls = _arrow_table_class()
    if not rows:
        return table_cls.empty([])
    return table_cls.from_pylist([dict(row) for row in rows])


@dataclass(slots=True)
class MockResponse:
    """Container for a canned xbbg response and optional Bloomberg event."""

    service: str
    operation: str
    table: Any
    event: Any | None = None
    request_operation: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def matches(self, params_dict: Mapping[str, Any]) -> bool:
        if params_dict.get("service") != self.service:
            return False

        actual_operation = params_dict.get("request_operation") or params_dict.get("operation")
        expected_operation = self.request_operation or self.operation
        return actual_operation == expected_operation


def create_mock_response(
    *,
    service: str | Any,
    operation: str,
    data: Any,
    service_xml: str | None = None,
    event_type: int | None = None,
    correlation_ids: Sequence[Any] | None = None,
    request_id: str | None = None,
    security_errors: Mapping[str, Mapping[str, Any]] | None = None,
    field_exceptions: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    delayed: Mapping[str, Mapping[str, Any]] | None = None,
    type_misses: Mapping[str, int] | None = None,
    field_errors: Mapping[str, Mapping[str, Any]] | None = None,
    eid_data: Mapping[str, Sequence[int]] | None = None,
) -> MockResponse:
    """Build a canned response from rows and, optionally, the diagnostics a result carries.

    ``data`` is the response payload: a mapping in the shape of the operation's
    response (reference and historical data) or an explicit list of row mappings.

    The diagnostics arguments take the values the result accessors return, so a
    result read from the mocked table matches what was passed in:

    - ``security_errors``: ``{security: {"category", "code", "subcategory", "message"}}``
      (``table.security_errors``).
    - ``field_exceptions``: ``{security: [{"field", "category", "code", "subcategory",
      "message"}, ...]}``, in occurrence order (``table.field_exceptions``).
    - ``delayed``: ``{security: {"delayed": bool, "delay_minutes": int | None}}``
      (``table.delayed``).
    - ``type_misses``: ``{field: count}`` (``table.type_misses``).
    - ``field_errors``: ``{field id: {"category", "code", "subcategory", "message",
      "source"?}}``, the refusals of a FieldInfo request (``table.field_errors``).
    - ``eid_data``: ``{security: [eid, ...]}`` (``table.eid_data``).
    - ``request_id``: the SDK request ID (``table.bloomberg_request_id``). With a
      Bloomberg service definition it is also set as the SDK message property.

    The ``xbbg.*`` schema metadata is written by the native code that attaches it
    to engine results, so the table carries it for every output format: native
    accessors, ``to_pyarrow().schema.metadata``, and pandas ``DataFrame.attrs``.
    Empty or omitted diagnostics leave their keys absent. Shapes the encoder
    does not accept raise ``ValueError`` here, not when a test reads the result.

    ``service_xml`` (or a ``blpapi.Service`` passed as ``service``) also builds a
    TestUtil ``blpapi`` event in ``MockResponse.event``; that requires ``blpapi``.
    """
    from xbbg import _core

    if isinstance(service, str):
        service_name = service
    else:
        service_name = service.name()
    rows = _rows_from_operation(operation, data)
    schema_metadata = _core._encode_result_diagnostics(
        eid_data=eid_data,
        security_errors=security_errors,
        field_exceptions=field_exceptions,
        delayed=delayed,
        type_misses=type_misses,
        field_errors=field_errors,
        bloomberg_request_id=request_id,
    )
    table = _table_from_rows(rows)
    if schema_metadata:
        table = _core._with_schema_metadata(table, schema_metadata)

    event = None
    service_obj: Any | None = None
    if service_xml is not None:
        service_obj = _coerce_blpapi_service(service, service_xml)
    elif not isinstance(service, str):
        service_obj = service

    if service_obj is not None:
        op = service_obj.getOperation(operation)
        element_def = op.getResponseDefinitionAt(0)
        event = create_mock_event(event_type or _require_blpapi().Event.RESPONSE)
        props = _build_message_properties(
            correlation_ids=correlation_ids,
            request_id=request_id,
            service=service_obj,
        )
        append_message_dict(event, element_def, data, props)

    return MockResponse(
        service=service_name,
        operation=operation,
        table=table,
        event=event,
        metadata={"rows": rows},
    )


def _coerce_mock_table(value: MockResponse | Any | Sequence[Mapping[str, Any]]) -> Any:
    if isinstance(value, MockResponse):
        return value.table
    table_cls = _arrow_table_class()
    batch_cls = _arrow_record_batch_class()
    if isinstance(value, table_cls):
        return value
    if isinstance(value, batch_cls):
        return value.to_table()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return _table_from_rows(value)
    raise TypeError("Unsupported mock response value")


@dataclass(frozen=True, slots=True)
class MockRequest:
    """A request seen by :class:`mock_engine`, as the engine would have received it.

    ``params`` is a snapshot of the dispatch dictionary (``RequestContext.to_dispatch_dict()``):
    ``service``, ``operation``, ``request_operation``, ``securities``, ``fields``,
    ``overrides``, ``start_date`` and the other request parameters, plus the
    per-request ``request_id``. ``served`` is ``True`` when a mock response
    answered the request and ``False`` when a non-strict mock passed it through.
    """

    service: str
    operation: str
    params: Mapping[str, Any]
    served: bool


class mock_engine:
    """Middleware context manager that serves canned responses.

    ``responses`` are matched in order, each answering one request: a
    :class:`MockResponse` answers the first request for its service and
    operation, and a bare table or list of rows answers the next request
    whatever it is. In strict mode (the default) a request nothing matches
    raises ``LookupError``; with ``strict=False`` it passes through to the
    engine, which needs a Bloomberg connection.

    ``requests`` lists, in order, every request the mock saw: those it served
    and, in non-strict mode, those it passed through (:class:`MockRequest`).
    It accumulates across re-entry and is not cleared on exit, so assert on it
    after the ``with`` block::

        with mock_engine([response]) as mock:
            blp.bdp("TEST Equity", "PX_LAST")
        assert mock.requests[0].params["securities"] == ["TEST Equity"]

    While any ``mock_engine`` is active, a ``blp.Engine(...)`` constructed
    makes no connection: ``with engine:`` scopes, mocked requests and
    ``shutdown()`` work offline. What needs Bloomberg (an unmatched request in
    non-strict mode, ``worker_health()``, ``subscription_feeds()``,
    ``connections()``, subscriptions) raises ``RuntimeError`` naming ``mock_engine``. Such an
    Engine stays offline after the mock exits; construct connected engines
    outside it. The state is process-wide, like the middleware, and nests.
    """

    def __init__(
        self,
        responses: Sequence[MockResponse | Any | Sequence[Mapping[str, Any]]],
        *,
        strict: bool = True,
    ) -> None:
        self._responses = list(responses)
        self._strict = strict
        self._middlewares: list[Any] = []
        self._requests: list[MockRequest] = []

    @property
    def requests(self) -> list[MockRequest]:
        """Requests seen so far, in arrival order: served, and passed through when not strict."""
        return list(self._requests)

    def _record(self, params_dict: Mapping[str, Any], *, served: bool) -> None:
        self._requests.append(
            MockRequest(
                service=params_dict.get("service"),
                operation=params_dict.get("operation"),
                params=MappingProxyType(copy.deepcopy(dict(params_dict))),
                served=served,
            )
        )

    def _pop_match(self, params_dict: Mapping[str, Any]):
        for index, candidate in enumerate(self._responses):
            if isinstance(candidate, MockResponse):
                if candidate.matches(params_dict):
                    return self._responses.pop(index)
            else:
                if index == 0:
                    return self._responses.pop(index)
        return None

    async def _handle(self, context: blp.RequestContext, call_next):
        params_dict = context.to_dispatch_dict()
        match = self._pop_match(params_dict)
        if match is None:
            if self._strict:
                raise LookupError(f"No mock response matched {context.params.service}::{context.params.operation}")
            self._record(params_dict, served=False)
            return await call_next(context)

        table = _coerce_mock_table(match)
        self._record(params_dict, served=True)
        batch = next(iter(table.to_batches()), None)
        if batch is None:
            batch = _arrow_table_class().empty([]).to_batches()[0]

        if context.params.on_delayed == "warn":
            blp._warn_delayed_data(table)

        context.metadata["mocked"] = True
        if isinstance(match, MockResponse):
            context.metadata["mock_response"] = match
        context.batch = batch
        context.table = table
        context.elapsed_ms = 0.0

        if context.raw:
            context.frame = table
            return table

        context.frame = _convert_result_backend(table, context.backend)
        return context.frame

    def __enter__(self):
        async def middleware(context: blp.RequestContext, call_next):
            return await self._handle(context, call_next)

        self._middlewares.append(middleware)
        blp.add_middleware(middleware)
        _engine._mock_engine_entered()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        if self._middlewares:
            middleware = self._middlewares.pop()
            try:
                blp.remove_middleware(middleware)
            except ValueError:
                pass
            finally:
                _engine._mock_engine_exited()


__all__ = [
    "MockRequest",
    "MockResponse",
    "append_message_dict",
    "create_mock_event",
    "create_mock_response",
    "deserialize_service",
    "get_admin_message_definition",
    "mock_engine",
]
