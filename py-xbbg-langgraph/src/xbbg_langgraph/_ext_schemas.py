"""Bounded, operation-specific inputs for the Bloomberg extension tools."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    Field,
    create_model,
    model_serializer,
    model_validator,
)

from ._bounded_schemas import (
    FiniteNumber,
    bounded_array,
    bounded_map,
    bounded_primitive,
    bounded_strings,
    bounded_text,
)
from ._bql import currency, equity_ticker, field_expression
from ._defs_gen import ChartKind, ChartSource
from ._runtime import ToolInput
from .options import BloombergToolsOptions

FieldDefinition = tuple[Any, Any]
Fields = dict[str, FieldDefinition]
_YEAR = Annotated[int, Field(strict=True, ge=1, le=9999)]
_MONTH = Annotated[int, Field(strict=True, ge=1, le=12)]
_DAY = Annotated[int, Field(strict=True, ge=1, le=31)]


def _optional(annotation: Any) -> FieldDefinition:
    return (annotation | None, None)


def _action_schema(
    name: str, actions: dict[str, Fields], *, validators: dict[str, Any] | None = None
) -> type[BaseModel]:
    """Expose flat LangChain arguments while validating only the selected action."""
    action_models: dict[str, type[BaseModel]] = {}
    annotations: dict[str, list[Any]] = {}
    uses: dict[str, list[str]] = {}
    for operation, fields in actions.items():
        action_models[operation] = create_model(
            f"{name}_{operation}",
            __base__=ToolInput,
            operation=(Literal[operation], ...),
            **fields,
        )
        for field, (annotation, _) in fields.items():
            annotations.setdefault(field, []).append(annotation)
            uses.setdefault(field, []).append(operation)

    class ActionInput(ToolInput):
        @model_validator(mode="before")
        @classmethod
        def validate_operation(_cls, value: Any) -> Any:
            if isinstance(value, BaseModel):
                value = value.model_dump(exclude_unset=True)
            if not isinstance(value, dict):
                raise ValueError("Tool input must be an object")
            operation = value.get("operation")
            if not isinstance(operation, str) or operation not in action_models:
                raise ValueError(f"operation must be one of: {', '.join(action_models)}")
            selected = action_models[operation]
            # Strict tool providers emit null for inactive advertised properties.
            # Unknown keys and non-null inactive values must still be rejected.
            active = {
                key: item
                for key, item in value.items()
                if item is not None or key not in annotations or key in selected.model_fields
            }
            return selected.model_validate(active).model_dump()

        @model_serializer(mode="wrap")
        def serialize_operation(self, handler: Any) -> dict[str, Any]:
            # LangChain forwards every dumped field with a schema default.
            # Inactive fields are not part of this operation's serialized input.
            data = handler(self)
            return {key: data[key] for key in action_models[self.operation].model_fields if key in data}

    merged: Fields = {
        "operation": (Literal[tuple(actions)], Field(description="Helper operation to run.")),
    }
    for field, choices in annotations.items():
        combined: Any = type(None)
        for choice in choices:
            combined = combined | choice
        merged[field] = (
            combined,
            Field(default=None, description=f"Used by: {', '.join(uses[field])}."),
        )
    return create_model(name, __base__=ActionInput, __validators__=validators or {}, **merged)


def ticker_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    ticker = (bounded_text(options), ...)
    tickers = (bounded_strings(options, options.max_securities), ...)
    return _action_schema(
        "TickerInput",
        {
            "parse_ticker": {"ticker": ticker},
            "normalize_tickers": {"tickers": tickers},
            "filter_equity_tickers": {"tickers": tickers},
            "is_specific_contract": {"ticker": ticker},
            "validate_generic_ticker": {"ticker": ticker},
        },
    )


def futures_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = bounded_text(options)
    date_parts: Fields = {"year": (_YEAR, ...), "month": (_MONTH, ...), "day": (_DAY, ...)}
    candidate = create_model(
        "FuturesCandidate",
        __base__=ToolInput,
        ticker=(text, ...),
        year=(_YEAR, ...),
        month=(_MONTH, ...),
    )
    pair = _pair_schema(options)
    return _action_schema(
        "FuturesInput",
        {
            "build_futures_ticker": {
                "prefix": (text, ...),
                "month_code": (text, ...),
                "year": (text | Annotated[int, Field(strict=True, ge=0, le=9999)], ...),
                "asset": (text, ...),
            },
            "generate_candidates": {
                "gen_ticker": (text, ...),
                **date_parts,
                "freq": (text, "M"),
                "count": (
                    Annotated[int, Field(strict=True, ge=1, le=options.max_rows)],
                    min(4, options.max_rows),
                ),
            },
            "contract_index": {"gen_ticker": (text, ...)},
            "filter_candidates_by_cycle": {
                "candidates": (bounded_array(candidate, options.max_fields), ...),
                "cycle": (text, ...),
            },
            "filter_valid_contracts": {
                "contracts": (bounded_array(pair, options.max_fields), ...),
                **date_parts,
            },
            "get_futures_months": {},
        },
    )


def cdx_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    ticker = (bounded_text(options), ...)
    recovery = (
        Annotated[
            float | None,
            Field(
                strict=True, ge=0, le=100, allow_inf_nan=False, description="Recovery percentage, 0-100 (40 means 40%)."
            ),
        ],
        None,
    )
    return _action_schema(
        "CdxInput",
        {
            "parse_cdx_ticker": {"ticker": ticker},
            "previous_cdx_series": {"ticker": ticker},
            "cdx_gen_to_specific": {
                "gen_ticker": ticker,
                "series": (Annotated[int, Field(strict=True, ge=1, le=4294967295)], ...),
            },
            "cdx_info": {"ticker": ticker},
            "cdx_pricing": {"ticker": ticker, "recovery_rate": recovery},
            "cdx_risk": {"ticker": ticker, "recovery_rate": recovery},
        },
    )


def currency_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = (bounded_text(options), ...)
    return _action_schema(
        "CurrencyInput",
        {
            "build_fx_pair": {"from_ccy": text, "to_ccy": text},
            "same_currency": {"ccy1": text, "ccy2": text},
            "currencies_needing_conversion": {"currencies": (bounded_strings(options), ...), "target": text},
        },
    )


def bql_builder_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = bounded_text(options)
    equity = Annotated[text, AfterValidator(equity_ticker)]
    field = Annotated[text, AfterValidator(field_expression)]
    extra_fields = (bounded_array(field, options.max_fields, minimum=0), Field(default_factory=list))
    return _action_schema(
        "BqlBuilderInput",
        {
            "build_preferreds_query": {"equity_ticker": (equity, ...), "extra_fields": extra_fields},
            "build_corporate_bonds_query": {
                "ticker": (equity, ...),
                "ccy": _optional(Annotated[text, AfterValidator(currency)]),
                "extra_fields": extra_fields,
            },
            "build_etf_holdings_query": {"etf_ticker": (equity, ...), "extra_fields": extra_fields},
        },
    )


def _market_rule_input(value: BaseModel) -> BaseModel:
    if value.operation == "get_market_rule" and not value.mic and not value.exch_code:
        raise ValueError("get_market_rule requires mic or exch_code")
    return value


def market_session_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = bounded_text(options)
    optional = _optional(text)
    market = {"mic": optional, "exch_code": optional}
    return _action_schema(
        "MarketSessionInput",
        {
            "derive_sessions": {"day_start": (text, ...), "day_end": (text, ...), **market},
            "get_market_rule": market,
            "infer_timezone": {"country_iso": (text, ...)},
            "session_times_to_utc": {
                "start_time": (text, ...),
                "end_time": (text, ...),
                "exchange_tz": (text, ...),
                "date": (text, ...),
            },
            "default_turnover_dates": {"start_date": optional, "end_date": optional},
            "default_bqr_datetimes": {"start_datetime": optional, "end_datetime": optional},
            "get_exchange_override": {"ticker": (text, ...)},
            "list_exchange_overrides": {},
        },
        validators={"market_rule": model_validator(mode="after")(_market_rule_input)},
    )


def yas_overrides_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    return create_model(
        "YasOverridesInput",
        __base__=ToolInput,
        settle_dt=_optional(bounded_text(options)),
        yield_type=_optional(Annotated[int, Field(strict=True, ge=1, le=9)]),
        spread=(FiniteNumber | None, Field(default=None, description="Spread in basis points: 100 means 100 bps.")),
        yield_val=(FiniteNumber | None, Field(default=None, description="Yield in percentage points: 4.5 means 4.5%.")),
        price=(
            FiniteNumber | None,
            Field(default=None, description="Price in the security's Bloomberg quotation convention."),
        ),
        benchmark=_optional(bounded_text(options)),
    )


def constants_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = bounded_text(options)
    return _action_schema(
        "ConstantsInput",
        {
            "parse_date": {"date_str": (text, ...)},
            "fmt_date": {
                "year": (_YEAR, ...),
                "month": (_MONTH, ...),
                "day": (_DAY, ...),
                "fmt": _optional(text),
            },
            "get_month_code": {"month_name": (text, ...)},
            "get_month_name": {"code": (text, ...)},
            "get_futures_months": {},
            "get_dvd_type": {"dvd_type": (text, ...)},
            "get_dvd_types": {},
            "get_dvd_cols": {},
            "get_etf_cols": {},
        },
    )


def _pair_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    return create_model(
        "StringPair", __base__=ToolInput, key=(bounded_text(options), ...), value=(bounded_text(options), ...)
    )


def columns_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    columns = (bounded_strings(options), ...)
    return _action_schema(
        "ColumnsInput",
        {
            "rename_dividend_columns": {"columns": columns},
            "rename_etf_columns": {"columns": columns},
            "build_earning_header_rename": {
                "header_row": (bounded_array(_pair_schema(options), options.max_fields), ...),
                "data_columns": columns,
            },
        },
    )


def calculate_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    class CalculateInput(ToolInput):
        @model_validator(mode="after")
        def equal_lengths(self) -> Any:
            if len(self.values) != len(self.levels):
                raise ValueError("values and levels must have the same length")
            return self

    level = Annotated[int, Field(strict=True, ge=1, le=2)]
    return create_model(
        "CalculateInput",
        __base__=CalculateInput,
        operation=(Literal["calculate_level_percentages"], ...),
        values=(bounded_array(FiniteNumber | None, options.max_fields), ...),
        levels=(bounded_array(level | None, options.max_fields), ...),
    )


def _chart_scalar(value: Any) -> Any:
    if type(value) is int and abs(value) > 2**53 - 1:
        raise ValueError("Chart integers must fit the JavaScript safe-integer range; use strings for identifiers")
    return value


def chart_spec_schema(options: BloombergToolsOptions) -> type[BaseModel]:
    text = bounded_text(options)
    key = bounded_text(options, trim=False)
    scalar = Annotated[bounded_primitive(options, minimum=0, trim=False) | None, BeforeValidator(_chart_scalar)]
    row = bounded_map(key, scalar, options.max_fields, minimum=1, trim_keys=False)
    fields: Fields = {
        "source": (ChartSource, ...),
        "rows": (bounded_array(row, options.max_rows), ...),
        "renderer": (Literal["vega-lite"], "vega-lite"),
        "chart": _optional(ChartKind),
        "title": _optional(text),
        "y_fields": _optional(bounded_strings(options)),
        "max_points": _optional(Annotated[int, Field(strict=True, ge=1, le=options.max_rows)]),
    }
    for field in (
        "x_field",
        "series_field",
        "label_field",
        "value_field",
        "open_field",
        "high_field",
        "low_field",
        "close_field",
        "side_field",
        "price_field",
        "size_field",
    ):
        fields[field] = _optional(text)
    return create_model("ChartSpecInput", __base__=ToolInput, **fields)
