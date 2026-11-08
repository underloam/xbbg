"""Offline behavior of the native list kernels exposed as ``xbbg._core.ext_*``."""

from __future__ import annotations

import sys

from _gil_probe import releases_gil
import pytest

from xbbg import _core

MAX_FUTURES_CANDIDATES = 10_000
FREE_THREADED = getattr(sys, "_is_gil_enabled", lambda: True)() is False


def test_normalize_tickers_returns_input_unchanged():
    tickers = [" BBB US Equity", "AAA US Equity ", "AAA US Equity ", "CCC1 Index"]

    assert _core.ext_normalize_tickers(tickers) == tickers


def test_filter_equity_tickers_keeps_order_and_duplicates():
    tickers = ["AAA US Equity", "BBB1 Index", "AAA US Equity", "CCC=US 01/15/24 C150 Equity", "DDD LN Equity"]

    assert _core.ext_filter_equity_tickers(tickers) == ["AAA US Equity", "AAA US Equity", "DDD LN Equity"]


def test_filter_valid_contracts_sorts_stably_after_reference_date():
    contracts = [
        ("SYNM24 Index", "2024-06-21"),
        ("SYNB Index", "2024-03-15"),
        ("SYNF24 Index", "2024-01-15"),
        ("SYNX Index", "not-a-date"),
        ("SYNA Index", "2024-03-15"),
        ("SYNB Index", "20240315"),
        ("SYNZ23 Index", "2023-12-15"),
    ]

    assert _core.ext_filter_valid_contracts(contracts, 2024, 1, 15) == [
        "SYNB Index",
        "SYNA Index",
        "SYNB Index",
        "SYNM24 Index",
    ]


def test_filter_valid_contracts_rejects_invalid_reference_date():
    with pytest.raises(ValueError, match="invalid date"):
        _core.ext_filter_valid_contracts([("SYNH24 Index", "2024-03-15")], 2024, 2, 30)


def test_filter_candidates_by_cycle_is_case_insensitive_and_keeps_order():
    candidates = [
        ("SYNF24 Index", 2024, 1),
        ("SYNH24 Index", 2024, 3),
        ("SYNJ24 Index", 2024, 4),
        ("SYNZ24 Index", 2024, 12),
        ("SYNH24 Index", 2024, 3),
    ]

    assert _core.ext_filter_candidates_by_cycle(candidates, "hmUz") == [
        ("SYNH24 Index", 2024, 3),
        ("SYNZ24 Index", 2024, 12),
        ("SYNH24 Index", 2024, 3),
    ]


def test_currencies_needing_conversion_are_sorted_unique_and_upper_case():
    assert _core.ext_currencies_needing_conversion(["usd", "GBP", "eur", "GBP", "USD", "jpy"], "usd") == [
        "EUR",
        "GBP",
        "JPY",
    ]


@pytest.mark.parametrize(
    ("freq", "expected"),
    [
        (
            "Q",
            [("ESH24 Index", 2024, 3), ("ESM24 Index", 2024, 6), ("ESU24 Index", 2024, 9), ("ESZ24 Index", 2024, 12)],
        ),
        ("M", [("ESF24 Index", 2024, 1), ("ESG24 Index", 2024, 2), ("ESH24 Index", 2024, 3), ("ESJ24 Index", 2024, 4)]),
    ],
)
def test_generate_futures_candidates_by_roll_frequency(freq, expected):
    assert _core.ext_generate_futures_candidates("ES1 Index", 2024, 1, 15, freq, 4) == expected


def test_generate_futures_candidates_rejects_specific_ticker():
    with pytest.raises(ValueError, match="specific contract"):
        _core.ext_generate_futures_candidates("ESH24 Index", 2024, 1, 15)


def test_generate_futures_candidates_bounds_count():
    candidates = _core.ext_generate_futures_candidates("ES1 Index", 2024, 1, 15, "M", MAX_FUTURES_CANDIDATES)
    assert len(candidates) == MAX_FUTURES_CANDIDATES
    assert candidates[-1] == ("ESJ57 Index", 2857, 4)

    with pytest.raises(ValueError, match=f"exceeds the maximum of {MAX_FUTURES_CANDIDATES}"):
        _core.ext_generate_futures_candidates("ES1 Index", 2024, 1, 15, "M", MAX_FUTURES_CANDIDATES + 1)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        _core.ext_generate_futures_candidates("ES1 Index", 2024, 1, 15, "M", sys.maxsize)


def test_column_renames_keep_input_order_and_duplicates():
    assert _core.ext_rename_dividend_columns(["ticker", "Ex-Date", "Declared Date", "Unknown", "Ex-Date"]) == [
        ("Ex-Date", "ex_date"),
        ("Declared Date", "dec_date"),
        ("Ex-Date", "ex_date"),
    ]
    assert _core.ext_rename_etf_columns(["Weight", "Unknown", "Holding Name"]) == [
        ("Weight", "weight"),
        ("Holding Name", "name"),
    ]


def test_build_earning_header_rename_maps_value_columns():
    header_row = [("Period 1 Header", "FY 2023"), ("Period 2 Header", "FY 2022")]
    data_columns = ["ticker", "Period 2 Value", "Period 1 Value", "level"]

    assert _core.ext_build_earning_header_rename(header_row, data_columns) == [
        ("Period 2 Value", "fy2022"),
        ("Period 1 Value", "fy2023"),
    ]


def test_calculate_level_percentages_groups_by_parent():
    values = [100.0, 50.0, 50.0, 300.0, None]
    levels = [1, 2, 2, 1, 2]

    percentages = _core.ext_calculate_level_percentages(values, levels)
    assert percentages[:4] == pytest.approx([25.0, 50.0, 50.0, 75.0])
    assert percentages[4] is None
    assert _core.ext_calculate_level_percentages([1.0, 2.0], [1]) == [None, None]


@pytest.mark.skipif(FREE_THREADED, reason="GIL release is only observable on a GIL build")
def test_filter_valid_contracts_releases_gil_for_large_input():
    contracts = [(f"SYN{index} Index", "2030-01-01" if index % 2 else "2020-01-01") for index in range(1_000_000)]
    results = []

    assert releases_gil(lambda: results.append(_core.ext_filter_valid_contracts(contracts, 2024, 1, 15)))
    assert len(results[0]) == 500_000
    assert results[0][:2] == ["SYN1 Index", "SYN3 Index"]
