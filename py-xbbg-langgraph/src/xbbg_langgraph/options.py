"""Application-owned limits for Bloomberg agent tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

BLOOMBERG_TOOL_NAMES = (
    "xbbg_bdp",
    "xbbg_bdh",
    "xbbg_bds",
    "xbbg_bdib",
    "xbbg_bdtick",
    "xbbg_check_entitlements",
    "xbbg_bql",
    "xbbg_bsrch",
    "xbbg_bqr",
    "xbbg_bflds",
    "xbbg_beqs",
    "xbbg_yas",
    "xbbg_preferreds",
    "xbbg_corporate_bonds",
    "xbbg_index_members",
    "xbbg_resolve_isins",
    "xbbg_issuer_isins",
    "xbbg_etf_holdings",
    "xbbg_stream_snapshot",
    "xbbg_mktbar_snapshot",
    "xbbg_depth_snapshot",
    "xbbg_ext_ticker",
    "xbbg_ext_futures",
    "xbbg_ext_cdx",
    "xbbg_ext_currency",
    "xbbg_ext_bql_builder",
    "xbbg_ext_chart_spec",
    "xbbg_ext_market_session",
    "xbbg_ext_yas_overrides",
    "xbbg_ext_constants",
    "xbbg_ext_columns",
    "xbbg_ext_calculate",
)


class BloombergToolsOptions(BaseModel):
    """Immutable limits; the application retains ownership of any supplied engine.

    Without ``engine``, requests use xbbg's existing global/scoped engine. This
    adapter never configures, resets, or shuts down an application engine.
    ``request_timeout`` is in seconds; snapshot wait limits are milliseconds.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    engine: Any = Field(default=None, exclude=True, repr=False)
    max_securities: int = Field(default=25, gt=0, strict=True)
    max_fields: int = Field(default=25, gt=0, strict=True)
    max_rows: int = Field(default=500, gt=0, strict=True)
    max_string_chars: int = Field(default=2000, gt=0, strict=True)
    max_result_bytes: int = Field(default=1_048_576, ge=256, strict=True)
    max_result_nodes: int = Field(default=50_000, ge=10, strict=True)
    max_content_bytes: int = Field(default=65_536, ge=256, strict=True)
    max_content_rows: int = Field(default=50, gt=0, strict=True)
    max_bql_query_chars: int = Field(default=4000, gt=0, strict=True)
    max_search_spec_chars: int = Field(default=1000, gt=0, strict=True)
    max_stream_updates: int = Field(default=10, gt=0, strict=True)
    max_stream_wait_ms: int = Field(default=15_000, gt=0, strict=True)
    request_timeout: float = Field(default=60.0, gt=0, allow_inf_nan=False, strict=True)
    validate_fields: StrictBool | None = None
    disabled_tools: frozenset[str] = frozenset()

    @field_validator("engine")
    @classmethod
    def _engine_context(cls, value: Any) -> Any:
        if value is not None and not (hasattr(value, "__enter__") and hasattr(value, "__exit__")):
            raise ValueError("engine must be an xbbg.blp.Engine context manager")
        return value

    @field_validator("disabled_tools")
    @classmethod
    def _known_tools(cls, names: frozenset[str]) -> frozenset[str]:
        unknown = names.difference(BLOOMBERG_TOOL_NAMES)
        if unknown:
            raise ValueError(f"Unknown disabled tools: {', '.join(sorted(unknown))}")
        return names


def resolve_options(options: BloombergToolsOptions | None, kwargs: dict[str, Any]) -> BloombergToolsOptions:
    """Accept either an options object or keyword options, without hidden precedence."""
    if options is not None:
        if not isinstance(options, BloombergToolsOptions):
            raise TypeError("options must be a BloombergToolsOptions instance")
        if kwargs:
            raise TypeError("Pass either options or keyword options, not both")
        return options
    return BloombergToolsOptions(**kwargs)
