"""Currency conversion through the shared Rust recipe."""

from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals.dependencies import is_pandas_dataframe
import narwhals.stable.v1 as nw

from xbbg.backend import is_backend_available
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


async def aconvert_ccy(
    data: IntoDataFrame,
    ccy: str = "USD",
    **kwargs,
) -> IntoDataFrame:
    """Convert long or wide historical values using the shared native recipe.

    Long data uses ticker/date/value columns; wide columns identify tickers by
    ticker or ticker|field. String values remain strings and integer values
    promote to Float64. Missing FX rates produce nulls, and request failures
    propagate instead of returning mixed-currency data. Non-numeric strings
    remain unchanged. Results use the configured backend, or ``backend=``.
    """
    if is_pandas_dataframe(data) or (
        not hasattr(data, "__arrow_c_stream__") and not hasattr(data, "__arrow_c_array__")
    ):
        frame = nw.from_native(data)
        if is_backend_available("pyarrow"):
            data = frame.to_arrow()
        else:
            from xbbg._core import ArrowTable

            data = (
                ArrowTable.from_pylist(frame.iter_rows(named=True)) if len(frame) else ArrowTable.empty(frame.columns)
            )
    return await _call_native_recipe(
        "recipe_adjust_ccy",
        data,
        ccy,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


convert_ccy = _syncify(aconvert_ccy)
