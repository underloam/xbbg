"""Futures adapters marshal inputs and propagate native recipe results and errors.

Contract validation, chain parsing, and volume selection are tested in the Rust
recipe module. These tests mock only the native recipe seam; no Bloomberg session
or Python copy of the resolver is needed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
import inspect
from typing import Any

import narwhals.stable.v1 as nw
import pyarrow as pa
import pytest

from xbbg._core import ArrowTable
from xbbg.ext import futures


class RecipeRecorder:
    """Record adapter calls without implementing contract selection."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], Any, dict[str, Any]]] = []
        self.outcome: Any = pa.table({"ticker": ["ESH24 Index"]})

    async def __call__(self, recipe: str, *args: Any, backend=None, **kwargs: Any) -> Any:
        self.calls.append((recipe, args, backend, kwargs))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.fixture
def recipes(monkeypatch: pytest.MonkeyPatch) -> RecipeRecorder:
    recorder = RecipeRecorder()
    monkeypatch.setattr(futures, "_call_native_recipe", recorder)
    return recorder


RESOLVERS = [
    (futures.afut_ticker, "recipe_fut_ticker"),
    (futures.aactive_futures, "recipe_active_futures"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize(
    "reference_date",
    [
        "2024-01-15",
        "20240115",
        "2024/01/15",
        "15/01/2024",
        date(2024, 1, 15),
        datetime(2024, 1, 15, 16, 30),
        datetime(2024, 1, 15, 16, 30, tzinfo=UTC),
    ],
)
async def test_futures_normalizes_dates_and_preserves_string_result(recipes, resolver, recipe, reference_date):
    result = await resolver("ES1 Index", reference_date)

    assert result == "ESH24 Index"
    assert isinstance(result, str)
    assert recipes.calls == [
        (recipe, ("ES1 Index", "20240115", None), None, {"request_options": {}}),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
async def test_futures_forwards_frequency_backend_and_all_request_options(recipes, resolver, recipe):
    options = {
        "overrides": {"CHAIN_DATE": "20240112", "FUT_CHAIN_OPTION": "ALL"},
        "elements": {"periodicitySelection": "DAILY"},
        "options": {"nonTradingDayFillOption": "ACTIVE_DAYS_ONLY"},
        "security_overrides": {"ES1 Index": {"PRICING_SOURCE": "BGN"}},
        "field_types": {"VOLUME": "float64"},
        "validate_fields": False,
        "return_eids": True,
        "request_tz": "UTC",
        "output_tz": "Europe/London",
        "format": "wide",
        "Currency": "USD",
    }

    result = await resolver("ES1 Index", "2024-01-15", freq="QE", backend="pyarrow", **options)

    assert result == "ESH24 Index"
    assert recipes.calls == [
        (recipe, ("ES1 Index", "20240115", "QE"), "pyarrow", {"request_options": options}),
    ]
    assert "backend" not in options
    assert "freq" not in options


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize("ticker", ["UXZ5 Index", "UXZ24 Index", "ESH24 Index", "SPYH24 US Equity", "invalid"])
async def test_futures_propagates_native_validation_without_python_ticker_parsing(recipes, resolver, recipe, ticker):
    error = ValueError(f"invalid generic ticker: {ticker}")
    recipes.outcome = error

    with pytest.raises(ValueError) as raised:
        await resolver(ticker, "2024-01-15")

    assert raised.value is error
    assert recipes.calls == [
        (recipe, (ticker, "20240115", None), None, {"request_options": {}}),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
async def test_futures_does_not_replace_native_resolution_errors_with_empty_strings(recipes, resolver, recipe):
    error = RuntimeError("unable to resolve futures contract")
    recipes.outcome = error

    with pytest.raises(RuntimeError) as raised:
        await resolver("ES2 Index", "2024-01-15")

    assert raised.value is error
    assert recipes.calls[0][0] == recipe


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize("reference_date", [None, "not-a-date"])
async def test_futures_rejects_invalid_dates_before_native_dispatch(recipes, resolver, recipe, reference_date):
    with pytest.raises(ValueError):
        await resolver("ES1 Index", reference_date)

    assert recipes.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize("tickers", [[], ["ESH24 Index", "ESM24 Index"]])
async def test_futures_rejects_non_single_row_results(recipes, resolver, recipe, tickers):
    recipes.outcome = pa.table({"ticker": pa.array(tickers, type=pa.string())})

    with pytest.raises(ValueError, match=f"{recipe} returned .* expected exactly 1"):
        await resolver("ES1 Index", "2024-01-15")


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize("ticker", [None, ""])
async def test_futures_rejects_missing_ticker_values(recipes, resolver, recipe, ticker):
    recipes.outcome = pa.table({"ticker": pa.array([ticker], type=pa.string())})

    with pytest.raises(ValueError, match=f"{recipe} returned a row without a ticker"):
        await resolver("ES1 Index", "2024-01-15")


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
async def test_futures_rejects_missing_ticker_column(recipes, resolver, recipe):
    recipes.outcome = pa.table({"other": ["ESH24 Index"]})

    with pytest.raises(ValueError, match=f"{recipe} returned a row without a ticker"):
        await resolver("ES1 Index", "2024-01-15")


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
@pytest.mark.parametrize(
    "backend", ["native", "pyarrow", "narwhals", "pandas", "polars", "polars_lazy", "narwhals_lazy"]
)
async def test_futures_unwraps_supported_backends(recipes, resolver, recipe, backend):
    table = pa.table({"ticker": ["ESH24 Index"]})
    if backend == "native":
        recipes.outcome = ArrowTable.from_pylist([{"ticker": "ESH24 Index"}])
    elif backend == "pyarrow":
        recipes.outcome = table
    elif backend == "narwhals":
        recipes.outcome = nw.from_native(table)
    elif backend == "pandas":
        pd = pytest.importorskip("pandas")
        recipes.outcome = pd.DataFrame({"ticker": ["ESH24 Index"]})
    else:
        pl = pytest.importorskip("polars")
        frame = pl.DataFrame({"ticker": ["ESH24 Index"]})
        recipes.outcome = frame if backend == "polars" else frame.lazy()
        if backend == "narwhals_lazy":
            recipes.outcome = nw.from_native(recipes.outcome)

    assert await resolver("ES1 Index", "2024-01-15", backend=backend) == "ESH24 Index"
    assert recipes.calls[0][2] == backend


@pytest.mark.asyncio
@pytest.mark.parametrize(("resolver", "recipe"), RESOLVERS)
async def test_futures_unwraps_duckdb_backend(recipes, resolver, recipe):
    duckdb = pytest.importorskip("duckdb")
    with duckdb.connect() as connection:
        recipes.outcome = connection.sql("SELECT 'ESH24 Index' AS ticker")
        assert await resolver("ES1 Index", "2024-01-15", backend="duckdb") == "ESH24 Index"
    assert recipes.calls[0][2] == "duckdb"


@pytest.mark.parametrize(
    ("resolver", "recipe"),
    [(futures.fut_ticker, "recipe_fut_ticker"), (futures.active_futures, "recipe_active_futures")],
)
def test_sync_futures_dispatches_to_same_native_recipe(recipes, resolver, recipe):
    assert resolver("ES1 Index", "2024-01-15", backend="native") == "ESH24 Index"
    assert recipes.calls == [
        (recipe, ("ES1 Index", "20240115", None), "native", {"request_options": {}}),
    ]


@pytest.mark.parametrize(
    ("resolver", "ticker_parameter"),
    [
        (futures.afut_ticker, "gen_ticker"),
        (futures.fut_ticker, "gen_ticker"),
        (futures.aactive_futures, "ticker"),
        (futures.active_futures, "ticker"),
    ],
)
def test_public_futures_signatures_remain_unchanged(resolver, ticker_parameter):
    parameters = inspect.signature(resolver).parameters
    assert list(parameters) == [ticker_parameter, "dt", "kwargs"]
    assert parameters["kwargs"].kind is inspect.Parameter.VAR_KEYWORD
