"""Bloomberg service definitions and request parameters.

Enum definitions (Service, Operation, ExtractorHint, Format, OutputMode) and
closed-set constants (FORMATS, FORMAT_DEFAULT, FORMAT_VALUES,
OVERFLOW_POLICIES, OVERFLOW_POLICY_ALIASES, OVERFLOW_POLICY_DEFAULT,
OVERFLOW_POLICY_VALUES, VALIDATION_MODES, VALIDATION_MODE_DEFAULT,
VALIDATION_MODE_VALUES, SDK_LOG_LEVELS, SDK_LOG_LEVEL_DEFAULT,
SDK_LOG_LEVEL_VALUES) are generated from ``defs/bloomberg.toml`` into
``_services_gen.py``.  This module re-exports them and provides the hand-written
:class:`RequestParams` dataclass.

Example::

    from xbbg import Service, Operation, RequestParams

    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["AAPL US Equity"],
        fields=["PX_LAST"],
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

from xbbg._services_gen import (
    FORMAT_DEFAULT,
    FORMAT_VALUES,
    FORMATS,
    OVERFLOW_POLICIES,
    OVERFLOW_POLICY_ALIASES,
    OVERFLOW_POLICY_DEFAULT,
    OVERFLOW_POLICY_VALUES,
    SDK_LOG_LEVEL_DEFAULT,
    SDK_LOG_LEVEL_VALUES,
    SDK_LOG_LEVELS,
    VALIDATION_MODE_DEFAULT,
    VALIDATION_MODE_VALUES,
    VALIDATION_MODES,
    ExtractorHint,
    Format,
    Operation,
    OutputMode,
    Service,
)
from xbbg.exceptions import BlpValidationError

# Declared explicitly because the generated closed-set constants are re-exported
# for callers rather than used in this module.
__all__ = [
    "FORMATS",
    "FORMAT_DEFAULT",
    "FORMAT_VALUES",
    "OVERFLOW_POLICIES",
    "OVERFLOW_POLICY_ALIASES",
    "OVERFLOW_POLICY_DEFAULT",
    "OVERFLOW_POLICY_VALUES",
    "SDK_LOG_LEVELS",
    "SDK_LOG_LEVEL_DEFAULT",
    "SDK_LOG_LEVEL_VALUES",
    "VALIDATION_MODES",
    "VALIDATION_MODE_DEFAULT",
    "VALIDATION_MODE_VALUES",
    "ExtractorHint",
    "Format",
    "LongMode",
    "Operation",
    "OutputMode",
    "RequestParams",
    "Service",
]

# Backwards compatibility alias
LongMode = Format


@dataclass
class RequestParams:
    """Validated request parameters for the Bloomberg API.

    This dataclass holds all possible parameters for Bloomberg requests.
    Not all parameters are used for all request types - the Python layer
    validates that required parameters are present for each operation.

    The Rust layer handles default extractor resolution via
    ``RequestParams::with_defaults()``, so Python only passes an explicit
    extractor when the caller overrides it.

    Attributes:
        service: Bloomberg service URI (e.g., ``"//blp/refdata"``).
        operation: Request operation name (e.g., ``"ReferenceDataRequest"``).
        request_operation: Actual Bloomberg operation name when using
            ``Operation.RAW_REQUEST`` as the low-level escape hatch.
        securities: List of security identifiers (for multi-security requests).
        security: Single security identifier (for intraday requests).
        fields: List of field names to retrieve.
        overrides: List of (field, value) tuples for global field overrides.
        security_overrides: Internal normalized per-security overrides derived
            from ``OverrideSpec``.
        elements: List of (name, value) tuples for generic request elements (for example BQL or BSRCH Domain).
        start_date: Start date for historical requests. Accepts ISO 8601 /
            ``YYYYMMDD`` string, ``"today"``, ``datetime.date``,
            ``datetime.datetime``, or ``pd.Timestamp``.
        end_date: End date for historical requests. Same accepted shapes as
            ``start_date``.
        start_datetime: Start datetime for intraday requests. Accepts ISO 8601
            string (with or without tz), ``datetime.datetime`` (naive or
            tz-aware), or ``pd.Timestamp``. Naive values use ``request_tz``.
        end_datetime: End datetime for intraday requests. Same accepted shapes
            as ``start_datetime``.
        request_tz: How naive intraday datetimes are interpreted before the API call
            (``UTC``, ``local``, ``exchange``, ``NY``/``LN``/…, reference ticker, or IANA).
        output_tz: Relabel Arrow ``time`` to this zone (same instants; handled in Rust).
        event_type: Event type for intraday bars (TRADE, BID, ASK, etc.).
        event_types: Event types for intraday ticks (TRADE, BID, ASK, etc.).
        interval: Bar interval in minutes for intraday bars.
        options: Additional Bloomberg options as (key, value) tuples.
        field_types: Manual type overrides for fields (for issue #168).
        output: Output format (arrow or json).
        extractor: Override the auto-detected extractor hint.  When ``None``
            the Rust layer picks the correct extractor for the operation.
        format: Output format: ``Format.LONG`` (wire value ``"long"``, default),
            ``Format.LONG_TYPED`` (``"long_typed"``),
            ``Format.LONG_WITH_METADATA`` (``"long_metadata"``), or
            ``Format.SEMI_LONG`` (``"semi_long"``). LONG_TYPED separates values
            into ``value_f64``, ``value_i64``, ``value_str``, ``value_bool``,
            ``value_date`` (Date32), ``value_ts`` (UTC timestamp), and
            ``value_time`` (Time64 microseconds, with no date or timezone).
        include_security_errors: When True for ReferenceData requests, include
            ``__SECURITY_ERROR__`` rows for securities that failed.
        return_eids: When True for ReferenceDataRequest (including BDS bulk
            data), HistoricalDataRequest, IntradayBarRequest, and
            IntradayTickRequest, ask Bloomberg to return EID entitlement
            metadata alongside the data.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` (default) follows engine configuration.
    """

    service: str | Service
    operation: str | Operation
    request_operation: str | Operation | None = None
    securities: Sequence[str] | None = None
    security: str | None = None
    fields: Sequence[str] | None = None
    overrides: Sequence[tuple[str, str]] | None = None
    security_overrides: Sequence[tuple[str, Sequence[tuple[str, str]]]] | None = None
    elements: Sequence[tuple[str, str]] | None = None
    start_date: str | None = None
    end_date: str | None = None
    start_datetime: str | None = None
    end_datetime: str | None = None
    request_tz: str | None = None
    output_tz: str | None = None
    event_type: str | None = None
    event_types: Sequence[str] | None = None
    interval: int | None = None
    options: Sequence[tuple[str, str]] | None = None
    field_types: dict[str, str] | None = None
    output: OutputMode = OutputMode.ARROW
    extractor: ExtractorHint | None = None
    format: Format | None = None
    include_security_errors: bool = False
    _is_raw_request: bool = field(init=False, repr=False)
    return_eids: bool = False
    validate_fields: bool | None = None

    def __post_init__(self) -> None:
        """Convert enums to strings and set defaults."""
        self._is_raw_request = self.operation is Operation.RAW_REQUEST
        if isinstance(self.service, Service):
            self.service = self.service.value
        if isinstance(self.operation, Operation):
            self.operation = self.operation.value
        if isinstance(self.request_operation, Operation):
            self.request_operation = self.request_operation.value
        if isinstance(self.output, str):
            self.output = OutputMode(self.output)

    def validate(self) -> None:
        """Validate parameters for the given operation.

        Raises:
            BlpValidationError: If required parameters are missing or invalid.
        """
        if not self.service:
            raise BlpValidationError("service is required")
        if self._is_raw_request and not self.request_operation:
            raise BlpValidationError("request_operation is required for RawRequest")
        if not self.operation and not self._is_raw_request:
            raise BlpValidationError("operation is required")

        effective_operation = self.request_operation if self._is_raw_request else self.operation
        if self.return_eids and effective_operation not in {
            Operation.REFERENCE_DATA.value,
            Operation.HISTORICAL_DATA.value,
            Operation.INTRADAY_BAR.value,
            Operation.INTRADAY_TICK.value,
        }:
            raise BlpValidationError(
                "return_eids is only supported for ReferenceDataRequest, "
                "HistoricalDataRequest, IntradayBarRequest, and IntradayTickRequest"
            )

        if self._is_raw_request:
            return

        op = self.operation

        # Validate based on operation type
        if op == Operation.REFERENCE_DATA.value:
            self._validate_reference_data()
        elif op == Operation.HISTORICAL_DATA.value:
            self._validate_historical_data()
        elif op == Operation.INTRADAY_BAR.value:
            self._validate_intraday_bar()
        elif op == Operation.INTRADAY_TICK.value:
            self._validate_intraday_tick()
        elif op in (Operation.FIELD_INFO.value, Operation.FIELD_SEARCH.value):
            self._validate_field_request()
        # Unknown operations: no validation (power user mode)

    def _validate_reference_data(self) -> None:
        """Validate ReferenceDataRequest parameters."""
        if not self.securities:
            raise BlpValidationError("securities is required for ReferenceDataRequest")
        if not self.fields:
            raise BlpValidationError("fields is required for ReferenceDataRequest")

    def _validate_historical_data(self) -> None:
        """Validate HistoricalDataRequest parameters."""
        if not self.securities:
            raise BlpValidationError("securities is required for HistoricalDataRequest")
        if not self.fields:
            raise BlpValidationError("fields is required for HistoricalDataRequest")
        if not self.start_date:
            raise BlpValidationError("start_date is required for HistoricalDataRequest")
        if not self.end_date:
            raise BlpValidationError("end_date is required for HistoricalDataRequest")

    def _validate_intraday_bar(self) -> None:
        """Validate IntradayBarRequest parameters."""
        if not self.security:
            raise BlpValidationError("security is required for IntradayBarRequest")
        if not self.event_type:
            raise BlpValidationError("event_type is required for IntradayBarRequest")
        if self.interval is None:
            raise BlpValidationError("interval is required for IntradayBarRequest")
        if not self.start_datetime:
            raise BlpValidationError("start_datetime is required for IntradayBarRequest")
        if not self.end_datetime:
            raise BlpValidationError("end_datetime is required for IntradayBarRequest")

    def _validate_intraday_tick(self) -> None:
        """Validate IntradayTickRequest parameters."""
        if not self.security:
            raise BlpValidationError("security is required for IntradayTickRequest")
        if not self.start_datetime:
            raise BlpValidationError("start_datetime is required for IntradayTickRequest")
        if not self.end_datetime:
            raise BlpValidationError("end_datetime is required for IntradayTickRequest")

    def _validate_field_request(self) -> None:
        """Validate FieldInfoRequest/FieldSearchRequest parameters."""
        if not self.fields:
            raise BlpValidationError("fields is required for field metadata requests")

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary for passing to Rust.

        Only non-``None`` values are included.  When *extractor* is ``None``
        the key is omitted so the Rust layer can apply its own default via
        ``RequestParams::with_defaults()``.

        Returns:
            Dictionary suitable for Rust consumption.
        """
        result: dict[str, object] = {
            "service": self.service,
            "operation": self.operation,
        }

        if self.request_operation is not None:
            result["request_operation"] = self.request_operation

        # Only pass extractor when explicitly set by the caller
        if self.extractor is not None:
            result["extractor"] = self.extractor.value

        if self.securities is not None:
            result["securities"] = list(self.securities)
        if self.security is not None:
            result["security"] = self.security
        if self.fields is not None:
            # FieldInfoRequest uses "id" array in Bloomberg API, mapped to field_ids in Rust
            if self.operation == Operation.FIELD_INFO.value:
                result["field_ids"] = list(self.fields)
            # FieldSearchRequest uses search_spec (single string)
            elif self.operation == Operation.FIELD_SEARCH.value:
                result["search_spec"] = self.fields[0] if self.fields else ""
            else:
                result["fields"] = list(self.fields)
        if self.overrides is not None:
            result["overrides"] = list(self.overrides)
        if self.security_overrides is not None:
            result["security_overrides"] = [
                (security, list(overrides)) for security, overrides in self.security_overrides
            ]
        if self.elements is not None:
            result["elements"] = list(self.elements)
        if self.start_date is not None:
            result["start_date"] = self.start_date
        if self.end_date is not None:
            result["end_date"] = self.end_date
        if self.start_datetime is not None:
            result["start_datetime"] = self.start_datetime
        if self.end_datetime is not None:
            result["end_datetime"] = self.end_datetime
        if self.request_tz is not None:
            result["request_tz"] = self.request_tz
        if self.output_tz is not None:
            result["output_tz"] = self.output_tz
        if self.event_type is not None:
            result["event_type"] = self.event_type
        if self.event_types is not None:
            result["event_types"] = list(self.event_types)
        if self.interval is not None:
            result["interval"] = self.interval
        if self.options is not None:
            result["options"] = list(self.options)
        if self.field_types is not None:
            result["field_types"] = self.field_types
        if self.format is not None:
            # Pass format value to Rust
            result["format"] = self.format.value if isinstance(self.format, Format) else self.format
        if self.include_security_errors:
            result["include_security_errors"] = True
        if self.return_eids:
            result["return_eids"] = True
        if self.validate_fields is not None:
            result["validate_fields"] = self.validate_fields

        return result
