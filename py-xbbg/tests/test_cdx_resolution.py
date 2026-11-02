"""CDX series resolution through the native recipes.

The series ladder itself lives in Rust (`xbbg_recipes::futures`) and is covered
by its own unit tests. These cover the Python boundary: argument marshalling,
single-ticker unwrapping, and error propagation -- specifically that a failed
resolution raises instead of returning an empty ticker.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from xbbg.ext.futures import aactive_cdx, acdx_ticker


def _ticker_table(*tickers: str) -> pa.Table:
    return pa.table({"ticker": pa.array(list(tickers), pa.string())})


class RecipeRecorder:
    """Stands in for the native recipe layer, recording how it was called."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.outcome: Any = _ticker_table("CDX IG CDSI S34 V1 5Y Corp")

    async def __call__(self, recipe_name: str, *args: Any, **kwargs: Any) -> Any:
        assert not kwargs
        self.calls.append((recipe_name, args))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.fixture
def recipes(monkeypatch: pytest.MonkeyPatch) -> RecipeRecorder:
    recorder = RecipeRecorder()
    monkeypatch.setattr("xbbg.ext.futures._native_recipe", recorder)
    return recorder


@pytest.mark.asyncio
async def test_cdx_ticker_forwards_normalized_arguments(recipes: RecipeRecorder) -> None:
    resolved = await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "2020-06-01")

    assert resolved == "CDX IG CDSI S34 V1 5Y Corp"
    assert recipes.calls == [
        ("recipe_cdx_ticker", ("CDX IG CDSI GEN 5Y Corp", "20200601", False)),
    ]


@pytest.mark.asyncio
async def test_cdx_ticker_forwards_versionless_flag(recipes: RecipeRecorder) -> None:
    recipes.outcome = _ticker_table("CDX IG CDSI S34 5Y Corp")

    resolved = await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "20200601", versionless=True)

    assert resolved == "CDX IG CDSI S34 5Y Corp"
    assert recipes.calls[0][1] == ("CDX IG CDSI GEN 5Y Corp", "20200601", True)


@pytest.mark.asyncio
async def test_active_cdx_forwards_lookback_and_versionless(recipes: RecipeRecorder) -> None:
    recipes.outcome = _ticker_table("CDX HY CDSI S45 V3 5Y Corp")

    resolved = await aactive_cdx(
        "CDX HY CDSI GEN 5Y Corp",
        "2026-03-25",
        lookback_days=30,
    )

    assert resolved == "CDX HY CDSI S45 V3 5Y Corp"
    assert recipes.calls == [
        ("recipe_active_cdx", ("CDX HY CDSI GEN 5Y Corp", "20260325", 30, False)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("resolver", [acdx_ticker, aactive_cdx])
async def test_cdx_resolution_propagates_recipe_errors(recipes: RecipeRecorder, resolver) -> None:
    """A failed resolution must raise, never resolve to an empty ticker."""
    recipes.outcome = RuntimeError("missing CDS_FIRST_ACCRUAL_START_DATE for 'CDX IG CDSI S34 5Y Corp'")

    with pytest.raises(RuntimeError, match="CDS_FIRST_ACCRUAL_START_DATE"):
        await resolver("CDX IG CDSI GEN 5Y Corp", "2020-06-01")


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 2])
async def test_cdx_ticker_rejects_non_single_ticker_results(recipes: RecipeRecorder, count: int) -> None:
    recipes.outcome = _ticker_table(*[f"CDX IG CDSI S3{n} V1 5Y Corp" for n in range(count)])

    with pytest.raises(ValueError, match="expected exactly 1"):
        await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "2020-06-01")


@pytest.mark.asyncio
async def test_cdx_ticker_rejects_empty_ticker_cell(recipes: RecipeRecorder) -> None:
    recipes.outcome = _ticker_table("")

    with pytest.raises(ValueError, match="without a ticker"):
        await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "2020-06-01")
