"""Application-owned limits for Bloomberg agent tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from ._defs_gen import (
    BLOOMBERG_TOOL_NAMES,
    DEFAULT_MAX_BQL_QUERY_CHARS,
    DEFAULT_MAX_CONTENT_BYTES,
    DEFAULT_MAX_CONTENT_ROWS,
    DEFAULT_MAX_FIELDS,
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_RESULT_NODES,
    DEFAULT_MAX_ROWS,
    DEFAULT_MAX_SEARCH_SPEC_CHARS,
    DEFAULT_MAX_SECURITIES,
    DEFAULT_MAX_STREAM_UPDATES,
    DEFAULT_MAX_STREAM_WAIT_MS,
    DEFAULT_MAX_STRING_CHARS,
    MIN_TOOL_RESULT_BYTES,
    MIN_TOOL_RESULT_NODES,
)


class BloombergToolsOptions(BaseModel):
    """Immutable limits; the application retains ownership of any supplied engine.

    Without ``engine``, requests use xbbg's existing global/scoped engine. This
    adapter never configures, resets, or shuts down an application engine.
    ``request_timeout`` is in seconds; snapshot wait limits are milliseconds.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    engine: Any = Field(default=None, exclude=True, repr=False)
    max_securities: int = Field(default=DEFAULT_MAX_SECURITIES, gt=0, strict=True)
    max_fields: int = Field(default=DEFAULT_MAX_FIELDS, gt=0, strict=True)
    max_rows: int = Field(default=DEFAULT_MAX_ROWS, gt=0, strict=True)
    max_string_chars: int = Field(default=DEFAULT_MAX_STRING_CHARS, gt=0, strict=True)
    max_result_bytes: int = Field(default=DEFAULT_MAX_RESULT_BYTES, ge=MIN_TOOL_RESULT_BYTES, strict=True)
    max_result_nodes: int = Field(default=DEFAULT_MAX_RESULT_NODES, ge=MIN_TOOL_RESULT_NODES, strict=True)
    max_content_bytes: int = Field(default=DEFAULT_MAX_CONTENT_BYTES, ge=MIN_TOOL_RESULT_BYTES, strict=True)
    max_content_rows: int = Field(default=DEFAULT_MAX_CONTENT_ROWS, gt=0, strict=True)
    max_bql_query_chars: int = Field(default=DEFAULT_MAX_BQL_QUERY_CHARS, gt=0, strict=True)
    max_search_spec_chars: int = Field(default=DEFAULT_MAX_SEARCH_SPEC_CHARS, gt=0, strict=True)
    max_stream_updates: int = Field(default=DEFAULT_MAX_STREAM_UPDATES, gt=0, strict=True)
    max_stream_wait_ms: int = Field(default=DEFAULT_MAX_STREAM_WAIT_MS, gt=0, strict=True)
    request_timeout: float = Field(default=60.0, gt=0, allow_inf_nan=False, strict=True)
    validate_fields: StrictBool | None = None
    disabled_tools: frozenset[str] = frozenset()

    @field_validator("engine")
    @classmethod
    def _engine_context(_cls, value: Any) -> Any:
        if value is not None and not (hasattr(value, "__enter__") and hasattr(value, "__exit__")):
            raise ValueError("engine must be an xbbg.blp.Engine context manager")
        return value

    @field_validator("disabled_tools")
    @classmethod
    def _known_tools(_cls, names: frozenset[str]) -> frozenset[str]:
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
