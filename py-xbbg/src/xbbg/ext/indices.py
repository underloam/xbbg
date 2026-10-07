"""Index constituent extension functions backed by native xbbg recipes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame

_SUPPORTED_INDEX_FIELDS = frozenset({"INDX_MWEIGHT", "INDX_MEMBERS", "INDX_MEMBERS3"})


async def aindex_members(
    index: str,
    *,
    field: str = "INDX_MWEIGHT",
    asof: DateLike = None,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async normalized index members for INDX_MWEIGHT/INDX_MEMBERS/INDX_MEMBERS3."""
    normalized_field = field.upper()
    if normalized_field not in _SUPPORTED_INDEX_FIELDS:
        raise ValueError(f"field must be one of {sorted(_SUPPORTED_INDEX_FIELDS)}, got {field!r}")
    return await _call_native_recipe(
        "recipe_index_members",
        index,
        normalized_field,
        _fmt_date(asof) if asof is not None else None,
        backend=backend,
    )


index_members = _syncify(aindex_members)
