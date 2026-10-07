"""Volatility surface extension functions backed by native xbbg recipes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


class VolSurfacePreset(str, Enum):
    """Built-in Bloomberg implied-volatility field presets."""

    DELTA_1M_2M = "DELTA_1M_2M"
    MONEYNESS_30D = "MONEYNESS_30D"
    MONEYNESS_60D = "MONEYNESS_60D"
    MONEYNESS_3M = "MONEYNESS_3M"
    MONEYNESS_6M = "MONEYNESS_6M"
    MONEYNESS_12M = "MONEYNESS_12M"


def _normalize_tickers(tickers: str | Sequence[str]) -> list[str]:
    return [tickers] if isinstance(tickers, str) else [str(ticker) for ticker in tickers]


def _normalize_presets(
    preset: str | VolSurfacePreset | Sequence[str | VolSurfacePreset] | None,
) -> list[str] | None:
    if preset is None:
        return None

    def one(value: str | VolSurfacePreset) -> str:
        return value.value if isinstance(value, VolSurfacePreset) else str(value)

    if isinstance(preset, (str, VolSurfacePreset)):
        return [one(preset)]
    return [one(item) for item in preset]


def _encode_field_spec(field: str, meta: Mapping[str, Any] | None = None) -> str:
    if not meta:
        return field
    metric = str(meta.get("metric", ""))
    tenor = str(meta.get("tenor", ""))
    point_type = str(meta.get("point_type", ""))
    point = "" if meta.get("point") is None else str(meta.get("point"))
    return "|".join((field, metric, tenor, point_type, point))


def _normalize_field_specs(
    fields: Mapping[str, Mapping[str, Any]] | Sequence[str] | None,
) -> list[str] | None:
    if fields is None:
        return None
    if isinstance(fields, Mapping):
        return [_encode_field_spec(str(field), meta) for field, meta in fields.items()]
    if isinstance(fields, str):
        return [fields]
    return [str(field) for field in fields]


async def avol_surface(
    tickers: str | Sequence[str],
    *,
    start_date: DateLike,
    end_date: DateLike,
    preset: str | VolSurfacePreset | Sequence[str | VolSurfacePreset] | None = VolSurfacePreset.MONEYNESS_30D,
    fields: Mapping[str, Mapping[str, Any]] | Sequence[str] | None = None,
    as_decimal: bool = True,
    include_derived: bool = False,
    risk_free_rate: float | None = None,
    dividend_yield_field: str | None = None,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async tidy historical implied-volatility surface.

    Returns columns: ticker, date, metric, tenor, point_type, point, field, value.
    """
    start = _fmt_date(start_date)
    end = _fmt_date(end_date)
    if start is None or end is None:
        raise ValueError("start_date and end_date are required")

    return await _call_native_recipe(
        "recipe_vol_surface",
        _normalize_tickers(tickers),
        start,
        end,
        _normalize_presets(preset),
        _normalize_field_specs(fields),
        as_decimal,
        include_derived,
        risk_free_rate,
        dividend_yield_field,
        backend=backend,
    )


vol_surface = _syncify(avol_surface)
