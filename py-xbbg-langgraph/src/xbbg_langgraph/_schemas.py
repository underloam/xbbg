"""Bounded, JSON-representable inputs for the Bloomberg request tools."""

from __future__ import annotations

from datetime import date, datetime, timezone
import re
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BeforeValidator,
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
    create_model,
    model_validator,
)

from ._bql import currency, equity_ticker, field_expression
from ._runtime import ToolInput
from .options import BloombergToolsOptions

MAX_ENTITLEMENT_EIDS = 10_000
MAX_BLOOMBERG_EID = 2_147_483_647

ReferenceFormat = Literal["long", "long_typed", "long_metadata"]
HistoricalFormat = Literal["long", "long_typed", "long_metadata", "semi_long"]
OverflowPolicy = Literal["drop_newest", "block"]
FiniteNumber = Annotated[float, Field(strict=True, allow_inf_nan=False)]
PositiveInteger = Annotated[int, Field(strict=True, gt=0)]

_DATE_RE = re.compile(r"^(?:[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8})$")
_DATETIME_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}"
    r"(?::[0-9]{2}(?:\.[0-9]{1,6})?)?(?:Z|[+-][0-9]{2}:?[0-9]{2})?$"
)
_MAX_EPOCH_MS = 253_402_300_799_999

# These maps carry Bloomberg values, never Python call configuration. Normalize
# spelling for the check so camelCase and snake_case cannot bypass the boundary.
_RESERVED_REQUEST_KEYS = frozenset(
    re.sub(r"[^a-z0-9]", "", key.lower())
    for key in (
        "engine",
        "backend",
        "format",
        "field_types",
        "raw",
        "output",
        "extractor",
        "host",
        "server",
        "servers",
        "port",
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
        "request_timeout",
        "timeout",
        "timeout_ms",
        "streams_deactivated_warn_ms",
        "keep_alive_enabled",
        "keep_alive_inactivity_ms",
        "keep_alive_response_timeout_ms",
        "slow_consumer_hi_water_mark",
        "slow_consumer_lo_water_mark",
        "sdk_log_level",
        "socks5_host",
        "socks5_port",
        "service",
        "operation",
        "request_operation",
        "ticker",
        "tickers",
        "security",
        "securities",
        "field",
        "fields",
        "flds",
        "start",
        "end",
        "start_date",
        "end_date",
        "start_datetime",
        "end_datetime",
        "dt",
        "session",
        "request_tz",
        "output_tz",
        "interval",
        "bar_size",
        "bar_sz",
        "typ",
        "event_type",
        "event_types",
        "bar_tp",
        "bar_type",
        "options",
        "kwargs",
        "elements",
        "overrides",
        "security_overrides",
        "validate_fields",
        "return_eids",
        "include_security_errors",
        "include_bic_mic_codes",
        "include_bloomberg_standard_condition_codes",
        "include_broker_codes",
        "include_condition_codes",
        "include_exchange_codes",
        "include_non_plottable_events",
        "include_rps_codes",
        "screen",
        "screen_name",
        "screen_type",
        "group",
        "asof",
        "as_of_date",
        "domain",
        "search_spec",
        "query",
        "expression",
        "date_offset",
        "show_date",
        "dts",
        "dates",
        "date_format",
        "dt_fmt",
        "sort",
        "orientation",
        "direction",
        "dir",
    )
)


def _check_request_keys(values: dict[str, Any]) -> dict[str, Any]:
    for key, value in values.items():
        canonical = re.sub(r"[^a-z0-9]", "", key.lower())
        if key.startswith("_") or canonical in _RESERVED_REQUEST_KEYS:
            raise ValueError(f"{key!r} is a reserved call option, not a Bloomberg request value")
        if isinstance(value, dict):
            _check_request_keys(value)
    return values


def _check_map_keys(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    seen: set[str] = set()
    for key in value:
        if isinstance(key, str):
            normalized = key.strip()
            if normalized in seen:
                raise ValueError("Map keys must be distinct after trimming whitespace")
            seen.add(normalized)
    return value


def _epoch_datetime(value: int | float) -> datetime:
    if value < 100_000_000_000:
        raise ValueError("Ambiguous numeric date; use calendar text or epoch milliseconds")
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (ValueError, OverflowError, OSError) as exc:
        raise ValueError("Epoch milliseconds are outside the supported calendar range") from exc


def _normalize_date(value: str | int | float) -> str:
    if isinstance(value, str):
        if not _DATE_RE.fullmatch(value):
            raise ValueError("Use YYYY-MM-DD or YYYYMMDD, not ambiguous or relative dates")
        compact = value.replace("-", "")
        parsed = date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
    elif isinstance(value, int) and 19_000_101 <= value <= 29_991_231:
        return _normalize_date(str(value))
    else:
        parsed = _epoch_datetime(value).date()
    return f"{parsed.year:04d}{parsed.month:02d}{parsed.day:02d}"


def _normalize_datetime(value: str | int | float) -> str:
    if not isinstance(value, str):
        return _epoch_datetime(value).isoformat()
    if not _DATETIME_RE.fullmatch(value):
        raise ValueError("Use ISO 8601 with an explicit time component and at most six fractional digits")
    normalized = value.replace(" ", "T").replace("Z", "+00:00")
    offset = re.search(r"([+-])([0-9]{2}):?([0-9]{2})$", normalized)
    if offset is not None and (int(offset[2]) > 23 or int(offset[3]) > 59):
        raise ValueError("Timezone offset hours and minutes are out of range")
    # Parsing validates leap days, clock components, and timezone offsets.
    datetime.fromisoformat(normalized)
    return normalized


def _ordered_range(value: ToolInput) -> ToolInput:
    start, end = value.start, value.end
    if "T" in start:
        start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)
        if (start_dt.tzinfo is None) != (end_dt.tzinfo is None):
            raise ValueError("start and end must both include timezone offsets or both be naive")
        ordered = start_dt <= end_dt
    else:
        ordered = start <= end
    if not ordered:
        raise ValueError("start must be on or before end")
    return value


def _exclusive_field_search(value: ToolInput) -> ToolInput:
    if (value.fields is None) == (value.search_spec is None):
        raise ValueError("Provide exactly one of fields or search_spec")
    return value


def _security_overrides(value: ToolInput) -> ToolInput:
    for key, overrides in (value.overrides or {}).items():
        if isinstance(overrides, dict) and key not in value.securities:
            raise ValueError("Per-security override keys must exactly match a supplied security")
    return value


def _index_ticker(value: str) -> str:
    if value.startswith("/") or len(value.split()) < 2 or value.split()[-1].lower() != "index":
        raise ValueError("Supply the exact qualified <TICKER> Index ticker; resolve identifiers first")
    return value


def _depth_fields(value: ToolInput) -> ToolInput:
    if not value.fields and value.all_fields is False:
        raise ValueError("Depth snapshots require fields unless all_fields is enabled")
    return value


def _raw_isin(value: str) -> str:
    if value.startswith("/"):
        raise ValueError("Pass the raw ISIN without an /isin/ prefix")
    return value


def create_core_schema(name: str, options: BloombergToolsOptions) -> type[ToolInput]:
    """Build one schema with the caller's limits visible to the language model."""
    text = Annotated[
        str,
        StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=options.max_string_chars),
    ]
    primitive = text | StrictBool | StrictInt | FiniteNumber
    flat_map = Annotated[
        dict[text, primitive],
        Field(max_length=options.max_fields),
        BeforeValidator(_check_map_keys),
        AfterValidator(_check_request_keys),
    ]
    override_map = Annotated[
        dict[text, primitive | flat_map],
        Field(max_length=options.max_fields + options.max_securities),
        BeforeValidator(_check_map_keys),
        AfterValidator(_check_request_keys),
    ]
    securities = Annotated[list[text], Field(min_length=1, max_length=options.max_securities)]
    fields = Annotated[list[text], Field(min_length=1, max_length=options.max_fields)]
    date_input = Annotated[
        Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=8, max_length=10)]
        | Annotated[int, Field(strict=True, ge=19_000_101, le=_MAX_EPOCH_MS)]
        | Annotated[float, Field(strict=True, ge=100_000_000_000, le=_MAX_EPOCH_MS, allow_inf_nan=False)],
        AfterValidator(_normalize_date),
    ]
    datetime_input = Annotated[
        Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=16, max_length=32)]
        | Annotated[int, Field(strict=True, ge=100_000_000_000, le=_MAX_EPOCH_MS)]
        | Annotated[float, Field(strict=True, ge=100_000_000_000, le=_MAX_EPOCH_MS, allow_inf_nan=False)],
        AfterValidator(_normalize_datetime),
    ]
    search = Annotated[
        str,
        StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=options.max_search_spec_chars),
    ]

    def required(annotation: Any, description: str) -> tuple[Any, Any]:
        return annotation, Field(description=description)

    def optional(annotation: Any, description: str) -> tuple[Any, Any]:
        return annotation | None, Field(default=None, description=description)

    securities_field = required(
        securities, "Exact user-supplied securities. Preserve /isin/ and /cusip/ identifiers; never guess tickers."
    )
    fields_field = required(fields, "Bloomberg field mnemonics. Use xbbg_bflds first when uncertain.")
    ticker_field = required(text, "One exact user-supplied ticker or /isin/ or /cusip/ identifier; never guess.")
    kwargs_field = optional(
        flat_map, "Bounded Bloomberg request elements/values only; infrastructure and typed call options are forbidden."
    )
    overrides_field = optional(
        override_map, "Global field overrides or nested field maps keyed by an exact supplied security."
    )
    return_eids = optional(StrictBool, "Request Bloomberg entitlement IDs in result metadata.")
    validate_fields = optional(StrictBool, "Override configured field validation for this request.")
    ref = {
        "securities": securities_field,
        "overrides": overrides_field,
        "kwargs": kwargs_field,
        "return_eids": return_eids,
        "validate_fields": validate_fields,
    }
    intraday = {
        "ticker": ticker_field,
        "start": required(datetime_input, "Explicit ISO start datetime with a time component."),
        "end": required(datetime_input, "Explicit ISO end datetime with a time component."),
    }
    validators: dict[str, Any] = {}

    if name == "xbbg_bdp":
        shape = {
            **ref,
            "fields": fields_field,
            "format": optional(ReferenceFormat, "Reference output shape; usually omit."),
            "include_security_errors": optional(StrictBool, "Retain Bloomberg security errors in the response."),
        }
    elif name == "xbbg_bdh":
        shape = {
            **ref,
            "fields": fields_field,
            "start": required(date_input, "Explicit start date, YYYY-MM-DD or YYYYMMDD."),
            "end": required(date_input, "Explicit end date, YYYY-MM-DD or YYYYMMDD."),
            "format": optional(HistoricalFormat, "Historical output shape; semi_long puts fields in columns."),
        }
    elif name == "xbbg_bds":
        shape = {**ref, "field": required(text, "Exactly one Bloomberg bulk/table field, not a field list.")}
    elif name in {"xbbg_bdib", "xbbg_bdtick"}:
        shape = {
            **intraday,
            "kwargs": kwargs_field,
            "return_eids": return_eids,
            "request_tz": optional(text, "Timezone for naive datetimes; omit for the native UTC default."),
            "output_tz": optional(text, "Output timezone for the same instants."),
        }
        if name == "xbbg_bdib":
            shape.update(
                interval=required(PositiveInteger, "Positive bar interval in minutes."),
                event_type=optional(text, "Bloomberg event type; omit for the native default."),
            )
        else:
            shape["event_types"] = optional(fields, "Explicit tick event types, such as TRADE, BID, or ASK.")
            for flag in (
                "include_bic_mic_codes",
                "include_bloomberg_standard_condition_codes",
                "include_broker_codes",
                "include_condition_codes",
                "include_exchange_codes",
                "include_non_plottable_events",
                "include_rps_codes",
            ):
                shape[flag] = optional(StrictBool, "Optional Bloomberg IntradayTickRequest include flag.")
    elif name == "xbbg_check_entitlements":
        eid = Annotated[int, Field(strict=True, gt=0, le=MAX_BLOOMBERG_EID)]
        service = Annotated[text, Field(pattern=r"^//[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")]
        shape = {
            "eids": required(
                Annotated[list[eid], Field(min_length=1, max_length=MAX_ENTITLEMENT_EIDS)],
                "Positive signed 32-bit Bloomberg entitlement IDs.",
            ),
            "service": (
                service,
                Field(default="//blp/refdata", description="Bloomberg service URI, such as //blp/refdata."),
            ),
        }
    elif name == "xbbg_bql":
        query = Annotated[
            str,
            StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=options.max_bql_query_chars),
        ]
        shape = {"query": required(query, "Complete BQL expression with an explicit bounded universe.")}
    elif name == "xbbg_bsrch":
        shape = {
            "search_spec": required(search, "Saved Bloomberg search/grid specification, not ordinary security lookup."),
            "kwargs": kwargs_field,
            "overrides": optional(flat_map, "Flat Bloomberg search-grid overrides."),
        }
    elif name == "xbbg_bqr":
        shape = {
            **intraday,
            "event_types": optional(fields, "Dealer quote event types; omit for BID and ASK."),
            "include_broker_codes": optional(StrictBool, "Include dealer/broker attribution; native default is true."),
        }
    elif name == "xbbg_bflds":
        shape = {
            "fields": optional(fields, "Known mnemonics to inspect; mutually exclusive with search_spec."),
            "search_spec": optional(search, "Field search text; mutually exclusive with fields."),
        }
        validators["exclusive_search"] = model_validator(mode="after")(_exclusive_field_search)
    elif name == "xbbg_beqs":
        shape = {
            "screen": required(text, "An existing Bloomberg equity screen name supplied by the user."),
            "asof": optional(date_input, "Optional screen as-of date."),
            "group": optional(text, "Bloomberg screen group."),
            "screen_type": optional(text, "Bloomberg screen type, such as PRIVATE or GLOBAL."),
            "kwargs": kwargs_field,
            "overrides": optional(flat_map, "Flat Bloomberg BEQS field overrides."),
        }
    elif name == "xbbg_yas":
        shape = {
            "tickers": securities_field,
            "fields": fields_field,
            "settle_dt": optional(date_input, "Explicit YAS settlement date."),
            "yield_type": optional(Annotated[int, Field(strict=True, ge=1, le=9)], "Bloomberg YAS yield type, 1-9."),
            "yield_val": optional(FiniteNumber, "Yield in percentage points: 4.5 means 4.5%, not 0.045."),
            "price": optional(FiniteNumber, "Bloomberg YAS price in the security's quotation convention."),
            "spread": optional(FiniteNumber, "Spread in basis points: 100 means 100 bps."),
            "benchmark": optional(text, "Exact user-supplied benchmark security."),
        }
    elif name in {"xbbg_preferreds", "xbbg_corporate_bonds", "xbbg_etf_holdings"}:
        ticker_name = {
            "xbbg_preferreds": "equity_ticker",
            "xbbg_corporate_bonds": "ticker",
            "xbbg_etf_holdings": "etf_ticker",
        }[name]
        recipe_field = Annotated[text, AfterValidator(field_expression)]
        extra_fields = Annotated[list[recipe_field], Field(max_length=options.max_fields)]
        shape = {
            ticker_name: required(
                Annotated[text, AfterValidator(equity_ticker)],
                "Exact qualified Equity ticker. Resolve identifiers first; never guess a suffix.",
            ),
            "fields": optional(
                extra_fields, "Additional identifiers or argument-free BQL fields; use xbbg_bql for expressions."
            ),
        }
        if name == "xbbg_corporate_bonds":
            shape["ccy"] = optional(
                Annotated[text, AfterValidator(currency)],
                "Three-letter currency filter; omission means all currencies.",
            )
    elif name == "xbbg_index_members":
        shape = {
            "index": required(
                Annotated[text, AfterValidator(_index_ticker)], "Exact qualified Bloomberg Index ticker."
            ),
            "asof": optional(date_input, "Optional index membership as-of date."),
            "field": optional(
                Literal["INDX_MWEIGHT", "INDX_MEMBERS", "INDX_MEMBERS3"], "Native index constituent field."
            ),
        }
    elif name in {"xbbg_resolve_isins", "xbbg_issuer_isins"}:
        ids = Annotated[
            list[Annotated[text, AfterValidator(_raw_isin)]], Field(min_length=1, max_length=options.max_securities)
        ]
        shape = {
            "isins" if name == "xbbg_resolve_isins" else "bond_isins": required(
                ids, "Exact raw ISIN strings, without /isin/ prefixes."
            )
        }
    elif name in {"xbbg_stream_snapshot", "xbbg_mktbar_snapshot", "xbbg_depth_snapshot"}:
        if name == "xbbg_stream_snapshot":
            shape = {"tickers": securities_field, "fields": fields_field}
        else:
            stream_fields = (
                Annotated[list[Literal["LAST_PRICE"]], Field(min_length=1, max_length=1)]
                if name == "xbbg_mktbar_snapshot"
                else fields
            )
            shape = {
                "ticker": ticker_field,
                "fields": optional(stream_fields, "Optional service fields; market bars require LAST_PRICE only."),
            }
        shape.update(
            {
                "max_updates": required(
                    Annotated[int, Field(strict=True, gt=0, le=options.max_stream_updates)],
                    "Required maximum number of updates before unsubscribing.",
                ),
                "timeout_ms": (
                    Annotated[int, Field(strict=True, gt=0, le=options.max_stream_wait_ms)],
                    Field(
                        default=options.max_stream_wait_ms, description="Maximum total collection wait in milliseconds."
                    ),
                ),
                "drain": optional(
                    StrictBool, "Drain buffered backlog when closing; it is not added to the bounded snapshot."
                ),
                "all_fields": optional(StrictBool, "Expose all supported Bloomberg scalar fields."),
                "conflate": optional(StrictBool, "Request conflation; supported only by //blp/mktdata."),
                "flush_threshold": optional(PositiveInteger, "Native batch flush threshold."),
                "stream_capacity": optional(PositiveInteger, "Native bounded subscription queue capacity."),
                "overflow_policy": optional(
                    OverflowPolicy, "Native overflow policy: drop_newest or block; data loss is an error."
                ),
                "options": optional(
                    fields, "Bloomberg subscription options; market bars use bar_size=1 unless supplied."
                ),
            }
        )
    else:
        raise ValueError(f"Unknown Bloomberg core tool: {name}")

    if name in {"xbbg_bdh", "xbbg_bdib", "xbbg_bdtick", "xbbg_bqr"}:
        validators["ordered_range"] = model_validator(mode="after")(_ordered_range)
    if name in {"xbbg_bdp", "xbbg_bdh", "xbbg_bds"}:
        validators["security_overrides"] = model_validator(mode="after")(_security_overrides)
    if name == "xbbg_depth_snapshot":
        validators["depth_fields"] = model_validator(mode="after")(_depth_fields)
    return create_model(
        "".join(part.title() for part in name.split("_")) + "Input",
        __base__=ToolInput,
        __validators__=validators,
        **shape,
    )
