"""Fixed-income adapters normalize host types and delegate workflows to Rust."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import inspect

import pytest

from xbbg.ext import fixed_income


@pytest.fixture
def native_recipe(monkeypatch):
    calls = []
    result = object()

    async def call(name, *args, **kwargs):
        calls.append((name, args, kwargs))
        return result

    monkeypatch.setattr(fixed_income, "_call_native_recipe", call)
    return calls, result


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", [None, "native", "pandas", "polars", "pyarrow"])
async def test_yas_delegates_defaults_and_backend(native_recipe, backend):
    calls, native_result = native_recipe

    result = await fixed_income.ayas("TEST Govt", backend=backend)

    assert result is native_result
    assert calls == [
        (
            "recipe_yas",
            (["TEST Govt"], ["YAS_BOND_YLD"]),
            {
                "settle_dt": None,
                "yield_type": None,
                "spread": None,
                "yield_val": None,
                "price": None,
                "benchmark": None,
                "backend": backend,
                "request_options": {},
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [{"YAS_BOND_PX": 101.25, "YAS_CALC_TYPE": 1}, [("YAS_BOND_PX", 101.25), ("YAS_CALC_TYPE", 1)]],
)
async def test_yas_preserves_all_overrides_for_native_merge(native_recipe, overrides):
    calls, native_result = native_recipe
    controls = {
        "overrides": overrides,
        "security_overrides": {"TEST Govt": {"YAS_BOND_PX": 102.0}},
        "field_types": {"YAS_BOND_YLD": "float64"},
        "validate_fields": False,
        "return_eids": True,
        "format": "wide",
        "PRICING_SOURCE": "BGN",
    }

    result = await fixed_income.ayas(
        ["TEST Govt"],
        ["YAS_BOND_YLD", "YAS_MOD_DUR"],
        settle_dt=date(2024, 1, 15),
        yield_type=fixed_income.YieldType.YTW,
        spread=50.0,
        yield_=4.5,
        price=99.5,
        benchmark="BENCHMARK Govt",
        **controls,
    )

    assert result is native_result
    name, args, kwargs = calls[0]
    assert name == "recipe_yas"
    assert args == (["TEST Govt"], ["YAS_BOND_YLD", "YAS_MOD_DUR"])
    assert kwargs["settle_dt"] == "20240115"
    assert kwargs["yield_type"] == 5
    assert kwargs["spread"] == 50.0
    assert kwargs["yield_val"] == 4.5
    assert kwargs["price"] == 99.5
    assert kwargs["benchmark"] == "BENCHMARK Govt"
    assert kwargs["request_options"] == controls
    assert kwargs["request_options"]["overrides"] is overrides


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", [None, [], ["ID", "name", "px_last"]])
async def test_preferreds_leaves_ticker_and_field_rules_to_rust(native_recipe, fields):
    calls, native_result = native_recipe

    result = await fixed_income.apreferreds(
        "TEST", fields=fields, backend="polars", overrides={"currency": "USD"}, mode="cached"
    )

    assert result is native_result
    assert calls == [
        (
            "recipe_preferreds",
            ("TEST",),
            {
                "fields": fields,
                "backend": "polars",
                "request_options": {"overrides": {"currency": "USD"}, "mode": "cached"},
            },
        )
    ]


@pytest.mark.asyncio
async def test_corporate_bonds_keeps_python_usd_default(native_recipe):
    calls, native_result = native_recipe

    result = await fixed_income.acorporate_bonds("TEST")

    assert result is native_result
    assert calls == [
        (
            "recipe_corporate_bonds",
            ("TEST",),
            {"ccy": "USD", "fields": None, "backend": None, "request_options": {}},
        )
    ]


@pytest.mark.asyncio
async def test_bqr_serializes_aware_datetimes_without_losing_offsets(native_recipe):
    calls, native_result = native_recipe
    end = datetime(2024, 1, 15, 10, 0, 0, 125000, tzinfo=timezone(timedelta(hours=-5)))

    result = await fixed_income.abqr(
        "TEST Govt",
        end_datetime=end,
        include_broker_codes=False,
        request_tz="America/New_York",
        output_tz="UTC",
        includeExchangeCodes=True,
        includeYield=True,
    )

    assert result is native_result
    name, args, kwargs = calls[0]
    assert name == "recipe_bqr"
    assert args == ("TEST Govt",)
    assert kwargs["start_datetime"] is None
    assert kwargs["end_datetime"] == "2024-01-15T10:00:00.125000-05:00"
    assert kwargs["request_options"] == {
        "request_tz": "America/New_York",
        "output_tz": "UTC",
        "includeExchangeCodes": True,
        "includeYield": True,
    }


@pytest.mark.asyncio
async def test_bqr_accepts_date_objects_and_leaves_naive_zone_to_rust(native_recipe):
    calls, _ = native_recipe

    await fixed_income.abqr(
        "TEST Govt",
        start_datetime=date(2024, 1, 15),
        end_datetime=datetime(2024, 1, 15, 1),
        include_broker_codes=False,
        request_tz="America/New_York",
    )

    kwargs = calls[0][2]
    assert kwargs["start_datetime"] == "2024-01-15T00:00:00"
    assert kwargs["end_datetime"] == "2024-01-15T01:00:00"
    assert kwargs["request_options"]["request_tz"] == "America/New_York"


@pytest.mark.asyncio
async def test_native_recipe_validation_errors_are_not_suppressed(monkeypatch):
    error = ValueError("invalid yield type")

    async def call(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(fixed_income, "_call_native_recipe", call)
    with pytest.raises(ValueError) as exc:
        await fixed_income.ayas("TEST Govt", yield_type=99)
    assert exc.value is error


@pytest.mark.parametrize("name", ["yas", "preferreds", "corporate_bonds", "bqr"])
def test_sync_fixed_income_helpers_keep_async_signatures(name):
    assert inspect.signature(getattr(fixed_income, name)) == inspect.signature(getattr(fixed_income, f"a{name}"))
