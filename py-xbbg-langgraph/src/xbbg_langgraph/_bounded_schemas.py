"""Shared bounded value annotations for core and extension tool inputs."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BeforeValidator, Field, StrictBool, StrictInt, StringConstraints

from .options import BloombergToolsOptions

FiniteNumber = Annotated[float, Field(strict=True, allow_inf_nan=False)]
PositiveInteger = Annotated[int, Field(strict=True, gt=0)]


def bounded_text(
    options: BloombergToolsOptions, *, maximum: int | None = None, minimum: int = 1, trim: bool = True
) -> Any:
    return Annotated[
        str,
        StringConstraints(
            strict=True,
            strip_whitespace=trim,
            min_length=minimum,
            max_length=options.max_string_chars if maximum is None else maximum,
        ),
    ]


def bounded_array(item: Any, maximum: int, *, minimum: int = 1) -> Any:
    return Annotated[list[item], Field(min_length=minimum, max_length=maximum)]


def bounded_strings(options: BloombergToolsOptions, maximum: int | None = None, *, minimum: int = 1) -> Any:
    return bounded_array(bounded_text(options), options.max_fields if maximum is None else maximum, minimum=minimum)


def bounded_primitive(options: BloombergToolsOptions, *, minimum: int = 1, trim: bool = True) -> Any:
    return bounded_text(options, minimum=minimum, trim=trim) | StrictBool | StrictInt | FiniteNumber


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


def bounded_map(key: Any, value: Any, maximum: int, *, minimum: int = 0, trim_keys: bool = True) -> Any:
    annotation = Annotated[dict[key, value], Field(min_length=minimum, max_length=maximum)]
    return Annotated[annotation, BeforeValidator(_check_map_keys)] if trim_keys else annotation
