"""Field type cache — thin wrappers over the Rust engine's field type resolver.

All caching, type mapping, and disk persistence is handled in Rust
(``crates/xbbg-async/src/field_cache.rs``).  This module exposes a
Python-friendly API that delegates every call to the Rust ``PyEngine``.

Resolution hierarchy (implemented in Rust):
1. Manual overrides (``field_types`` parameter)
2. Disk cache (``~/.xbbg/field_cache.json``)
3. Runtime API query (``//blp/apiflds``)
4. Caller-supplied default type (e.g. ``"string"`` for BDP, ``"float64"`` for BDH)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
import warnings

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class FieldInfo:
    """Metadata for a Bloomberg field."""

    field_id: str
    arrow_type: str
    description: str = ""
    category: str = ""


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _get_engine():
    """Get the shared Rust engine instance (lazy-started)."""
    from .blp import _get_engine

    return _get_engine()


_QUERY_API_IGNORED_WARNING = (
    "query_api=False is accepted for compatibility but ignored; field type resolution still uses the Rust resolver."
)
_CACHE_PATH_IGNORED_WARNING = (
    "FieldTypeCache(cache_path=...) is accepted for compatibility but ignored; "
    "configure field_cache_path before engine startup instead."
)


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------


def resolve_field_types(
    fields: Sequence[str],
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Resolve Arrow types for fields using the Rust field cache.

    Sync wrapper — starts the engine if needed and queries ``//blp/apiflds``
    for any uncached fields.

    Args:
        fields: List of field mnemonics to resolve.
        overrides: Manual type overrides (highest priority).

    Returns:
        Dict mapping field names to Arrow type strings.
    """
    from .blp import _run_sync

    return _run_sync("resolve_field_types", aresolve_field_types, (fields, overrides), {})


async def aresolve_field_types(
    fields: Sequence[str],
    overrides: dict[str, str] | None = None,
    query_api: bool = True,
) -> dict[str, str]:
    """Async resolve Arrow types, querying ``//blp/apiflds`` for cache misses.

    Args:
        fields: List of field mnemonics to resolve.
        overrides: Manual type overrides (highest priority).
        query_api: Accepted for API compatibility (Rust always queries when needed).

    Returns:
        Dict mapping field names to Arrow type strings.
    """
    if query_api is False:
        warnings.warn(_QUERY_API_IGNORED_WARNING, UserWarning, stacklevel=2)
    engine = _get_engine()
    return await engine.resolve_field_types(
        list(fields),
        overrides if overrides else None,
        "string",
    )


async def cache_field_types(fields: Sequence[str]) -> None:
    """Pre-cache field types from ``//blp/apiflds``.

    Args:
        fields: List of field mnemonics to cache.
    """
    engine = _get_engine()
    await engine.resolve_field_types(list(fields), None, "string")


async def get_field_info(fields: Sequence[str]) -> list[FieldInfo]:
    """Get detailed field information from cache or API.

    Args:
        fields: List of field mnemonics.

    Returns:
        List of :class:`FieldInfo` objects.
    """
    await cache_field_types(fields)

    engine = _get_engine()
    result: list[FieldInfo] = []
    for field in fields:
        info = engine.get_field_info(field)
        if info:
            result.append(
                FieldInfo(
                    field_id=info.get("field_id", ""),
                    arrow_type=info.get("arrow_type", "string"),
                    description=info.get("description", ""),
                    category=info.get("category", ""),
                )
            )
        else:
            result.append(FieldInfo(field_id=field, arrow_type="string"))
    return result


def clear_field_cache() -> None:
    """Clear the field type cache (memory and disk)."""
    engine = _get_engine()
    engine.clear_field_cache()


def get_field_cache_stats() -> dict[str, int | str]:
    """Get field cache statistics including the resolved cache file location.

    Returns:
        Dict with:
            - entry_count: Number of cached field entries currently loaded
            - cache_path: Active cache JSON path used by the Rust resolver
    """
    engine = _get_engine()
    return engine.field_cache_stats()


# ---------------------------------------------------------------------------
# FieldTypeCache class — facade over the Rust resolver
# ---------------------------------------------------------------------------


class FieldTypeCache:
    """Cache for Bloomberg field type information.

    Thin facade over the Rust engine's ``FieldTypeResolver``.
    All caching, type mapping, and disk persistence is handled in Rust.

    The ``cache_path`` parameter is accepted for API compatibility but
    ignored. The Rust resolver manages its own cache location, using either
    the default path or ``xbbg.configure(field_cache_path=...)`` before the
    engine starts. Use :attr:`cache_path` or :attr:`stats` to inspect the
    active location.

    Example::

        cache = FieldTypeCache()
        types = cache.resolve_types(["PX_LAST", "NAME", "VOLUME"])
    """

    def __init__(self, cache_path: str | None = None):
        if cache_path is not None:
            warnings.warn(_CACHE_PATH_IGNORED_WARNING, UserWarning, stacklevel=2)

    def resolve_types(
        self,
        fields: Sequence[str],
        overrides: dict[str, str] | None = None,
        **kwargs: object,
    ) -> dict[str, str]:
        """Resolve Arrow types for a list of fields.

        Args:
            fields: List of field mnemonics to resolve.
            overrides: Manual type overrides (highest priority).

        Returns:
            Dict mapping field names to Arrow type strings.
        """
        if kwargs.get("query_api", True) is False:
            warnings.warn(_QUERY_API_IGNORED_WARNING, UserWarning, stacklevel=2)
        return resolve_field_types(fields, overrides)

    async def aresolve_types(
        self,
        fields: Sequence[str],
        overrides: dict[str, str] | None = None,
        query_api: bool = True,
    ) -> dict[str, str]:
        """Async resolve Arrow types, querying API for cache misses.

        Args:
            fields: List of field mnemonics to resolve.
            overrides: Manual type overrides (highest priority).
            query_api: Accepted for API compatibility.

        Returns:
            Dict mapping field names to Arrow type strings.
        """
        return await aresolve_field_types(fields, overrides, query_api)

    async def cache_field_types(self, fields: Sequence[str]) -> None:
        """Query ``//blp/apiflds`` and cache the results.

        Args:
            fields: List of field mnemonics to cache.
        """
        await cache_field_types(fields)

    async def get_field_info(self, fields: Sequence[str]) -> list[FieldInfo]:
        """Get detailed field information.

        Args:
            fields: List of field mnemonics.

        Returns:
            List of :class:`FieldInfo` objects.
        """
        return await get_field_info(fields)

    def clear_cache(self) -> None:
        """Clear both memory and disk cache."""
        clear_field_cache()

    @property
    def stats(self) -> dict[str, int | str]:
        """Field cache stats from the shared Rust resolver."""
        return get_field_cache_stats()

    @property
    def cache_path(self) -> str:
        """Resolved path to the active field cache JSON file."""
        return str(self.stats["cache_path"])
