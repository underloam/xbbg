"""Narwhals plugin entry point for xbbg native Arrow objects."""

from __future__ import annotations

from typing import Any

from ._arrow import is_arrow_record_batch, is_arrow_table
from ._narwhals_impl import XbbgNamespace

NATIVE_PACKAGE = "xbbg"


def is_native(native_object: object, /) -> bool:
    """Return whether ``native_object`` is an xbbg Arrow object."""
    return is_arrow_table(native_object) or is_arrow_record_batch(native_object)


def __narwhals_namespace__(version: Any) -> XbbgNamespace:
    """Return the Narwhals namespace backing xbbg native Arrow frames."""
    return XbbgNamespace(version=version)
