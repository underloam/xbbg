from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from xbbg import _engine, _request_options, backend as backend_module, blp
from xbbg._core import ArrowTable
from xbbg.services import Operation, Service

VALID_ALIAS_ELEMENTS = {
    "periodicityAdjustment",
    "periodicitySelection",
    "currency",
    "nonTradingDayFillOption",
    "nonTradingDayFillMethod",
    "maxDataPoints",
    "overrideOption",
    "pricingOption",
    "adjustmentNormal",
    "adjustmentAbnormal",
    "adjustmentSplit",
    "adjustmentFollowDPDF",
    "calendarCodeOverride",
    "interval",
    "eventType",
    "includeExchangeCodes",
}


class FakeEngine:
    async def list_valid_elements(self, _service, _operation):
        return list(VALID_ALIAS_ELEMENTS)

    async def resolve_field_types(self, fields, field_types, default_type):
        if field_types is not None:
            return field_types
        return dict.fromkeys(fields, default_type)


async def _call_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    return await func(*args, **kwargs)


@pytest.fixture
def endpoint_capture(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return object()

    _request_options._VALID_ELEMENTS_CACHE.clear()
    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(backend_module, "convert_backend_frame", lambda frame, _backend: frame)
    return captured


@pytest.fixture
def arrow_endpoint(monkeypatch):
    captured: dict[str, object] = {}
    raw = ArrowTable.from_pylist(
        [
            {
                "ticker": "IBM US Equity",
                "date": date(2026, 1, 2),
                "field": "PX_LAST",
                "value": 2.0,
            },
            {
                "ticker": "IBM US Equity",
                "date": date(2026, 1, 1),
                "field": "PX_LAST",
                "value": 1.0,
            },
        ]
    )

    async def fake_arequest(*, service, operation, backend, **kwargs):
        captured["service"] = service
        captured["operation"] = operation
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return raw

    _request_options._VALID_ELEMENTS_CACHE.clear()
    monkeypatch.setattr(_engine, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    return captured


HISTORICAL_KEY_ALIAS_CASES = [
    ("PeriodAdj", "periodicityAdjustment", "A", "ACTUAL"),
    ("PerAdj", "periodicityAdjustment", "A", "ACTUAL"),
    ("Period", "periodicitySelection", "W", "WEEKLY"),
    ("Per", "periodicitySelection", "W", "WEEKLY"),
    ("Currency", "currency", "USD", "USD"),
    ("Curr", "currency", "USD", "USD"),
    ("FX", "currency", "USD", "USD"),
    ("Days", "nonTradingDayFillOption", "A", "ALL_CALENDAR_DAYS"),
    ("Fill", "nonTradingDayFillMethod", "B", "NIL_VALUE"),
    ("Points", "maxDataPoints", 1, 1),
    ("Quote", "overrideOption", "Average", "OVERRIDE_OPTION_GPA"),
    ("QuoteType", "pricingOption", "Y", "PRICING_OPTION_YIELD"),
    ("QtTyp", "pricingOption", "Y", "PRICING_OPTION_YIELD"),
    ("CshAdjNormal", "adjustmentNormal", True, True),
    ("CshAdjAbnormal", "adjustmentAbnormal", True, True),
    ("CapChg", "adjustmentSplit", True, True),
    ("UseDPDF", "adjustmentFollowDPDF", True, True),
    ("Calendar", "calendarCodeOverride", "NYSE", "NYSE"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("alias", "canonical", "value", "expected"), HISTORICAL_KEY_ALIAS_CASES)
async def test_historical_request_key_aliases_reach_arequest(endpoint_capture, alias, canonical, value, expected):
    await blp.abdh(
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-02",
        end_date="2026-01-05",
        **{alias: value},
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["elements"] == [(canonical, expected)]
    assert kwargs["overrides"] is None


VALUE_ALIAS_CASES = [
    ("periodicityAdjustment", "A", "ACTUAL"),
    ("periodicityAdjustment", "C", "CALENDAR"),
    ("periodicityAdjustment", "F", "FISCAL"),
    ("periodicitySelection", "D", "DAILY"),
    ("periodicitySelection", "W", "WEEKLY"),
    ("periodicitySelection", "M", "MONTHLY"),
    ("periodicitySelection", "Q", "QUARTERLY"),
    ("periodicitySelection", "S", "SEMI_ANNUALLY"),
    ("periodicitySelection", "Y", "YEARLY"),
    ("nonTradingDayFillOption", "N", "NON_TRADING_WEEKDAYS"),
    ("nonTradingDayFillOption", "W", "NON_TRADING_WEEKDAYS"),
    ("nonTradingDayFillOption", "Weekdays", "NON_TRADING_WEEKDAYS"),
    ("nonTradingDayFillOption", "C", "ALL_CALENDAR_DAYS"),
    ("nonTradingDayFillOption", "A", "ALL_CALENDAR_DAYS"),
    ("nonTradingDayFillOption", "All", "ALL_CALENDAR_DAYS"),
    ("nonTradingDayFillOption", "T", "ACTIVE_DAYS_ONLY"),
    ("nonTradingDayFillOption", "Trading", "ACTIVE_DAYS_ONLY"),
    ("nonTradingDayFillMethod", "C", "PREVIOUS_VALUE"),
    ("nonTradingDayFillMethod", "P", "PREVIOUS_VALUE"),
    ("nonTradingDayFillMethod", "Previous", "PREVIOUS_VALUE"),
    ("nonTradingDayFillMethod", "B", "NIL_VALUE"),
    ("nonTradingDayFillMethod", "Blank", "NIL_VALUE"),
    ("nonTradingDayFillMethod", "NA", "NIL_VALUE"),
    ("overrideOption", "A", "OVERRIDE_OPTION_GPA"),
    ("overrideOption", "G", "OVERRIDE_OPTION_GPA"),
    ("overrideOption", "Average", "OVERRIDE_OPTION_GPA"),
    ("overrideOption", "C", "OVERRIDE_OPTION_CLOSE"),
    ("overrideOption", "Close", "OVERRIDE_OPTION_CLOSE"),
    ("pricingOption", "P", "PRICING_OPTION_PRICE"),
    ("pricingOption", "Price", "PRICING_OPTION_PRICE"),
    ("pricingOption", "Y", "PRICING_OPTION_YIELD"),
    ("pricingOption", "Yield", "PRICING_OPTION_YIELD"),
    ("eventType", "B", "BID"),
    ("eventType", "Bid", "BID"),
    ("eventType", "A", "ASK"),
    ("eventType", "Ask", "ASK"),
    ("eventType", "T", "TRADE"),
    ("eventType", "Trade", "TRADE"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("canonical", "alias", "expected"), VALUE_ALIAS_CASES)
async def test_value_aliases_resolve_by_canonical_element(endpoint_capture, canonical, alias, expected):
    elements, overrides = await _request_options._aroute_kwargs(
        Service.REFDATA,
        Operation.HISTORICAL_DATA,
        {canonical: alias},
    )

    assert elements == [(canonical, expected)]
    assert overrides == []


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["BarSz", "BarSize"])
async def test_abdib_bar_size_aliases_set_top_level_interval(endpoint_capture, alias):
    await _call_async(blp.abdib, "ESM6 Index", dt="2026-04-17", **{alias: "5"})

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["interval"] == 5
    assert kwargs["elements"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["BarTp", "BarType"])
@pytest.mark.parametrize(
    ("value", "expected"),
    [("B", "BID"), ("Bid", "BID"), ("A", "ASK"), ("Ask", "ASK"), ("T", "TRADE"), ("Trade", "TRADE")],
)
async def test_abdib_bar_type_aliases_set_top_level_event_type(endpoint_capture, alias, value, expected):
    await _call_async(blp.abdib, "ESM6 Index", dt="2026-04-17", **{alias: value})

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["event_type"] == expected
    assert kwargs["elements"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["BarTp", "BarType"])
async def test_abdtick_bar_type_aliases_set_event_types(endpoint_capture, alias):
    await _call_async(
        blp.abdtick,
        "ESM6 Index",
        "2026-04-17T08:00:00",
        "2026-04-17T18:23:33",
        **{alias: "Ask"},
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["event_types"] == ["ASK"]
    assert kwargs["elements"] is None


@pytest.mark.asyncio
async def test_abdtick_include_exchange_codes_alias_routes_as_element(endpoint_capture):
    await blp.abdtick(
        "ESM6 Index",
        "2026-04-17T08:00:00",
        "2026-04-17T18:23:33",
        IncludeExchangeCodes=True,
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["elements"] == [("includeExchangeCodes", True)]
    assert kwargs["overrides"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "alias",
    [
        "Dts",
        "Dates",
        "show_date",
        "DtFmt",
        "DateFormat",
        "date_format",
        "Sort",
        "sort",
        "Orientation",
        "Direction",
        "Dir",
        "orientation",
    ],
)
async def test_excel_presentation_aliases_are_not_sent_to_bloomberg(endpoint_capture, alias):
    with pytest.warns(UserWarning, match="Presentation alias"):
        elements, overrides = await _request_options._aroute_kwargs(
            Service.REFDATA,
            Operation.HISTORICAL_DATA,
            {alias: "ignored"},
        )

    assert elements == []
    assert overrides == []


PRESENTATION_VALUE_CASES = [
    ("show_date", "Show", True),
    ("show_date", "S", True),
    ("show_date", True, True),
    ("show_date", "True", True),
    ("show_date", "Hide", False),
    ("show_date", "H", False),
    ("show_date", False, False),
    ("show_date", "False", False),
    ("date_format", "B", "BOTH"),
    ("date_format", "Both", "BOTH"),
    ("date_format", "P", "PERIODIC"),
    ("date_format", "Periodic", "PERIODIC"),
    ("date_format", "D", "DATE"),
    ("date_format", "Date", "DATE"),
    ("sort", "C", "ASCENDING"),
    ("sort", "A", "ASCENDING"),
    ("sort", "Ascend", "ASCENDING"),
    ("sort", "Chronological", "ASCENDING"),
    ("sort", False, "ASCENDING"),
    ("sort", "False", "ASCENDING"),
    ("sort", "R", "DESCENDING"),
    ("sort", "D", "DESCENDING"),
    ("sort", "Descend", "DESCENDING"),
    ("sort", "Reverse", "DESCENDING"),
    ("sort", True, "DESCENDING"),
    ("sort", "True", "DESCENDING"),
    ("orientation", "H", "HORIZONTAL"),
    ("orientation", "Horizontal", "HORIZONTAL"),
    ("orientation", "V", "VERTICAL"),
    ("orientation", "Vertical", "VERTICAL"),
]


@pytest.mark.parametrize(("key", "value", "expected"), PRESENTATION_VALUE_CASES)
def test_presentation_value_aliases_normalize(key, value, expected):
    assert _request_options._normalize_presentation_alias(key, value) == (key, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["Dts", "Dates", "show_date"])
async def test_bdh_presentation_show_date_aliases_hide_date(arrow_endpoint, alias):
    result = await _call_async(
        blp.abdh,
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-01",
        end_date="2026-01-02",
        backend="pyarrow",
        **{alias: "Hide"},
    )

    assert "date" not in result.column_names
    assert result.column_names == ["ticker", "field", "value"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("alias", "value", "expected_columns"),
    [
        ("DtFmt", "Both", ["ticker", "date", "period", "field", "value"]),
        ("DateFormat", "P", ["ticker", "period", "field", "value"]),
        ("date_format", "D", ["ticker", "date", "field", "value"]),
    ],
)
async def test_bdh_presentation_date_format_aliases_shape_dates(arrow_endpoint, alias, value, expected_columns):
    result = await _call_async(
        blp.abdh,
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-01",
        end_date="2026-01-02",
        backend="pyarrow",
        Period="M",
        **{alias: value},
    )

    assert result.column_names == expected_columns
    rows = result.to_pylist()
    if "period" in expected_columns:
        assert rows[0]["period"] == "2026-01"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("alias", "value", "expected_dates"),
    [
        ("Sort", "A", [date(2026, 1, 1), date(2026, 1, 2)]),
        ("sort", "Reverse", [date(2026, 1, 2), date(2026, 1, 1)]),
    ],
)
async def test_bdh_presentation_sort_aliases_order_rows(arrow_endpoint, alias, value, expected_dates):
    result = await _call_async(
        blp.abdh,
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-01",
        end_date="2026-01-02",
        backend="pyarrow",
        **{alias: value},
    )

    assert result.column("date").to_pylist() == expected_dates


def _materialize_backend_result(frame: Any) -> Any:
    if hasattr(frame, "collect"):
        return frame.collect()
    return frame


def _backend_columns(frame: Any) -> list[str]:
    materialized = _materialize_backend_result(frame)
    if hasattr(materialized, "column_names"):
        return list(materialized.column_names)
    if hasattr(materialized, "columns"):
        return list(materialized.columns)
    if hasattr(materialized, "schema") and hasattr(materialized.schema, "names"):
        return list(materialized.schema.names)
    if hasattr(materialized, "to_pandas"):
        return list(materialized.to_pandas().columns)
    raise TypeError(f"Unsupported backend result type: {type(frame)!r}")


PRESENTATION_BACKEND_CASES = [
    (None, None),
    ("narwhals", None),
    ("pyarrow", None),
    ("pandas", "pandas"),
    ("polars", "polars"),
    ("polars_lazy", "polars"),
    ("narwhals_lazy", None),
    ("duckdb", "duckdb"),
]


PRESENTATION_BACKEND_SHAPE_CASES = [
    ({"DtFmt": "Both", "Sort": "A", "Direction": "V"}, ["ticker", "date", "period", "field", "value"]),
    ({"Dts": "Hide", "DtFmt": "Both", "Sort": "Reverse"}, ["ticker", "field", "value"]),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("backend", "required_module"), PRESENTATION_BACKEND_CASES)
@pytest.mark.parametrize(("presentation_kwargs", "expected_columns"), PRESENTATION_BACKEND_SHAPE_CASES)
async def test_bdh_presentation_aliases_shape_before_backend_conversion(
    arrow_endpoint, backend, required_module, presentation_kwargs, expected_columns
):
    if required_module is not None:
        pytest.importorskip(required_module)
        from xbbg.backend import check_backend

        if backend is not None and not check_backend(backend, raise_on_error=False):
            pytest.skip(f"backend {backend} is not usable")

    result = await _call_async(
        blp.abdh,
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-01",
        end_date="2026-01-02",
        backend=backend,
        Period="M",
        **presentation_kwargs,
    )

    assert _backend_columns(result) == expected_columns


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["Orientation", "Direction", "Dir", "orientation"])
async def test_bdh_presentation_orientation_aliases_select_format(endpoint_capture, alias):
    await _call_async(
        blp.abdh,
        "IBM US Equity",
        "PX_LAST",
        start_date="2026-01-01",
        end_date="2026-01-02",
        **{alias: "H"},
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["format"] == blp.Format.SEMI_LONG
    assert kwargs["elements"] is None


@pytest.mark.asyncio
async def test_explicit_overrides_route_request_aliases_to_elements(endpoint_capture):
    await blp.abdtick(
        "ESM6 Index",
        "2026-04-17T08:00:00",
        "2026-04-17T18:23:33",
        overrides={"Points": 1, "EQY_FUND_CRNCY": "EUR"},
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["elements"] == [("maxDataPoints", 1)]
    assert kwargs["overrides"] == [("EQY_FUND_CRNCY", "EUR")]


@pytest.mark.asyncio
async def test_ovr_routes_through_existing_override_path(endpoint_capture):
    await blp.abdtick(
        "ESM6 Index",
        "2026-04-17T08:00:00",
        "2026-04-17T18:23:33",
        overrides=blp.ovr(Points=1, EQY_FUND_CRNCY="EUR"),
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["elements"] == [("maxDataPoints", 1)]
    assert kwargs["overrides"] == [("EQY_FUND_CRNCY", "EUR")]


@pytest.mark.asyncio
async def test_bdp_forwards_per_security_overrides_inside_ovr(endpoint_capture):
    await blp.abdp(
        ["IBM US Equity", "MSFT US Equity"],
        "PX_LAST",
        overrides=blp.ovr(
            {
                "EQY_FUND_CRNCY": "USD",
                "IBM US Equity": blp.ovr(EQY_FUND_CRNCY="EUR"),
                "MSFT US Equity": {"USER_LOCAL_TRADE_DATE": date(2024, 1, 2)},
            }
        ),
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["overrides"] == [("EQY_FUND_CRNCY", "USD")]
    assert kwargs["security_overrides"] == [
        ("IBM US Equity", [("EQY_FUND_CRNCY", "EUR")]),
        ("MSFT US Equity", [("USER_LOCAL_TRADE_DATE", "20240102")]),
    ]


@pytest.mark.asyncio
async def test_bdh_forwards_per_security_override_specs(endpoint_capture):
    await blp.abdh(
        ["IBM US Equity"],
        "PX_LAST",
        start_date="2024-01-01",
        end_date="2024-01-02",
        overrides=blp.ovr().for_security("IBM US Equity", CRNCY="EUR"),
    )

    kwargs = endpoint_capture["kwargs"]
    assert kwargs["security_overrides"] == [("IBM US Equity", [("CRNCY", "EUR")])]
