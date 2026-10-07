"""Bloomberg request vocabulary, overrides, and presentation options.

Typed endpoint plans and generic requests share these normalizers. Schema
lookups use the lower engine seam, not a callback into the public facade.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import logging
import re
from typing import Any, cast
import warnings

from . import _engine
from ._dates import _fmt_date
from .services import Format, Operation, Service

logger = logging.getLogger(__name__)


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
        engine = _engine._get_engine()
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
