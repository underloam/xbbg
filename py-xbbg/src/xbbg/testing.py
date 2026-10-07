"""Testing helpers for non-live xbbg request and response flows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import blp
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
    rows: list[dict[str, Any]] = []
    for ticker, fields in data.items():
        for field_name, value in fields.items():
            rows.append(
                {
                    "ticker": ticker,
                    "field": field_name,
                    "value": _stringify_value(value),
                }
            )
    return rows


def _historical_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker, ticker_payload in data.items():
        if isinstance(ticker_payload, Mapping):
            for date_value, fields in ticker_payload.items():
                if not isinstance(fields, Mapping):
                    raise TypeError("HistoricalDataRequest mapping values must be field mappings")
                for field_name, value in fields.items():
                    rows.append(
                        {
                            "ticker": ticker,
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
                            "ticker": ticker,
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
) -> MockResponse:
    if isinstance(service, str):
        service_name = service
    else:
        service_name = service.name()
    rows = _rows_from_operation(operation, data)
    table = _table_from_rows(rows)

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


class mock_engine:
    """Middleware context manager that serves canned responses."""

    def __init__(
        self,
        responses: Sequence[MockResponse | Any | Sequence[Mapping[str, Any]]],
        *,
        strict: bool = True,
    ) -> None:
        self._responses = list(responses)
        self._strict = strict
        self._middleware = None

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
        match = self._pop_match(context.to_dispatch_dict())
        if match is None:
            if self._strict:
                raise LookupError(f"No mock response matched {context.params.service}::{context.params.operation}")
            return await call_next(context)

        table = _coerce_mock_table(match)
        batch = next(iter(table.to_batches()), None)
        if batch is None:
            batch = _arrow_table_class().empty([]).to_batches()[0]

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

        self._middleware = middleware
        blp.add_middleware(middleware)
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        if self._middleware is not None:
            try:
                blp.remove_middleware(self._middleware)
            finally:
                self._middleware = None


__all__ = [
    "MockResponse",
    "append_message_dict",
    "create_mock_event",
    "create_mock_response",
    "deserialize_service",
    "get_admin_message_definition",
    "mock_engine",
]
