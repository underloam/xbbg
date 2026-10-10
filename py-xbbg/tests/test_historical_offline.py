"""Offline adapter tests; recipe computation is covered in Rust."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from xbbg.ext import historical


@pytest.fixture
def native_calls(monkeypatch):
    calls = []
    result = object()

    async def call_native(recipe_name, *args, backend=None, request_options=None):
        calls.append((recipe_name, args, backend, request_options))
        return result

    monkeypatch.setattr(historical, "_call_native_recipe", call_native)
    return calls, result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("function", "native_name", "args"),
    [
        (historical.adividend, "recipe_dividend", (["ABC US Equity"], "", "", "all")),
        (historical.aearnings, "recipe_earning", (["ABC US Equity"], "Geo", "Revenue", None, None, None, None)),
        (historical.aturnover, "recipe_turnover", (["ABC US Equity"], "", "", "USD", 1e6)),
        (historical.aetf_holdings, "recipe_etf_holdings", ("ABC US Equity", None)),
    ],
)
async def test_historical_defaults_dispatch_once(native_calls, function, native_name, args):
    calls, result = native_calls
    assert await function("ABC US Equity") is result
    assert calls == [(native_name, args, None, {})]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "function",
    [historical.adividend, historical.aearnings, historical.aturnover, historical.aetf_holdings],
)
async def test_historical_preserves_request_options_and_backend(native_calls, function):
    calls, result = native_calls
    options = {
        "overrides": {"EQY_FUND_CRNCY": "EUR"},
        "elements": {"returnEids": True},
        "options": {"someOption": "value"},
        "security_overrides": {"ABC US Equity": {"REFERENCE_DATE": "20240102"}},
        "field_types": {"TURNOVER": "float64"},
        "validate_fields": False,
        "return_eids": True,
        "request_tz": "UTC",
        "output_tz": "UTC",
        "format": "wide",
        "raw": True,
        "Custom_Override": "custom-value",
    }
    assert await function("ABC US Equity", backend="polars", **options) is result
    assert calls[0][2] == "polars"
    assert calls[0][3] == options


@pytest.mark.asyncio
async def test_dividend_dates_alias_and_non_equity_reach_native(native_calls):
    calls, result = native_calls
    tickers = ["ABC US Equity", "SYNTHETIC Index"]
    assert (
        await historical.adividend(
            tickers,
            "adjust",
            start_date=date(2024, 1, 2),
            end_date=datetime(2024, 2, 3, 12),
            Corporate_Actions_Filter="CAPITAL_CHANGE",
        )
        is result
    )
    assert calls == [
        (
            "recipe_dividend",
            (tickers, "20240102", "20240203", "adjust"),
            None,
            {"Corporate_Actions_Filter": "CAPITAL_CHANGE"},
        )
    ]
    assert calls[0][1][0] is not tickers


@pytest.mark.asyncio
@pytest.mark.parametrize("typ", ["split", "projected", "CUSTOM_DIVIDEND_FIELD"])
async def test_dividend_type_mapping_is_native(native_calls, typ):
    calls, _ = native_calls
    await historical.adividend("ABC US Equity", typ)
    assert calls[0][1][-1] == typ


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("by", "typ"),
    [("Geo", "Revenue"), ("Product", "Capital_Expenditures"), ("Q", "IS"), ("A", "BS")],
)
async def test_earnings_union_arguments_are_forwarded(native_calls, by, typ):
    calls, result = native_calls
    assert (
        await historical.aearnings(
            "ABC US Equity", by, typ, ccy="EUR", level=2, year=2024, periods=5, backend="pyarrow"
        )
        is result
    )
    assert calls == [("recipe_earning", (["ABC US Equity"], by, typ, "EUR", 2, 2024, 5), "pyarrow", {})]


@pytest.mark.asyncio
async def test_earnings_zero_optional_values_reach_native(native_calls):
    calls, _ = native_calls
    await historical.aearnings("ABC US Equity", level=0, year=0, periods=0)
    assert calls[0][1][-3:] == (0, 0, 0)


@pytest.mark.asyncio
async def test_turnover_explicit_arguments_and_dates(native_calls):
    calls, result = native_calls
    assert (
        await historical.aturnover(
            ["ABC US Equity", "XYZ JP Equity"],
            start_date="2024/01/02",
            end_date=date(2024, 2, 3),
            ccy="local",
            factor=1.0,
            Per="W",
        )
        is result
    )
    assert calls == [
        (
            "recipe_turnover",
            (["ABC US Equity", "XYZ JP Equity"], "20240102", "20240203", "local", 1.0),
            None,
            {"Per": "W"},
        )
    ]


@pytest.mark.asyncio
async def test_turnover_partial_defaults_are_computed_in_rust(native_calls):
    calls, _ = native_calls
    await historical.aturnover("ABC US Equity", end_date="2024-06-15")
    assert calls[0][1][1:3] == ("", "20240615")


@pytest.mark.asyncio
@pytest.mark.parametrize("function", [historical.adividend, historical.aturnover])
@pytest.mark.parametrize("date_argument", ["start_date", "end_date"])
async def test_historical_invalid_explicit_dates_fail_before_native(native_calls, function, date_argument):
    calls, _ = native_calls
    with pytest.raises(ValueError):
        await function("ABC US Equity", **{date_argument: "not-a-date"})
    assert calls == []


@pytest.mark.asyncio
async def test_etf_query_and_fields_are_owned_by_native(native_calls):
    calls, result = native_calls
    fields = ["name", "id_isin", "name"]
    assert await historical.aetf_holdings("SYNTH", fields=fields) is result
    assert calls == [("recipe_etf_holdings", ("SYNTH", fields), None, {})]
    assert calls[0][1][1] is not fields


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "function",
    [historical.adividend, historical.aearnings, historical.aturnover, historical.aetf_holdings],
)
async def test_native_failures_are_not_suppressed(monkeypatch, function):
    async def failing_native(*_args, **_kwargs):
        raise ValueError("native recipe rejected the request")

    monkeypatch.setattr(historical, "_call_native_recipe", failing_native)
    with pytest.raises(ValueError, match="native recipe rejected"):
        await function("ABC US Equity")


@pytest.mark.asyncio
async def test_dividend_yield_signature_and_dispatch_remain_unchanged(native_calls):
    calls, result = native_calls
    assert (
        await historical.adividend_yield(
            "ABC US Equity",
            start_date="2024-01-01",
            end_date="2024-12-31",
            dividend_types=["Regular Cash"],
            window_days=180,
            backend="native",
            ignored_existing_kwarg=True,
        )
        is result
    )
    assert calls == [
        (
            "recipe_dividend_yield",
            (["ABC US Equity"], "20240101", "20241231", ["Regular Cash"], 180),
            "native",
            None,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("Dts", "Hide"),
        ("Dates", "Show"),
        ("show_date", False),
        ("DtFmt", "Both"),
        ("DateFormat", "Periodic"),
        ("date_format", "Date"),
        ("Sort", "Reverse"),
        ("sort", True),
        ("Orientation", "Horizontal"),
        ("Direction", "Vertical"),
        ("Dir", "H"),
        ("orientation", "V"),
    ],
)
async def test_turnover_presentation_controls_reach_native_without_python_shaping(native_calls, key, value):
    calls, result = native_calls
    options = {key: value, "Per": "M", "format": "long_typed"}
    assert await historical.aturnover("ABC US Equity", backend="pyarrow", **options) is result
    assert calls == [("recipe_turnover", (["ABC US Equity"], "", "", "USD", 1e6), "pyarrow", options)]
