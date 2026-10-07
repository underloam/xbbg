"""Unit tests for the shared override-path date normalizer (#317).

The override path receives ``**kwargs`` from BDP / BDH / BDS calls without
per-field type metadata, so it relies on value-based duck typing to convert
date-typed override values to Bloomberg's expected ``YYYYMMDD`` form.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

# Exercise the shared request normalizer without dispatching Bloomberg requests.
pytest.importorskip("xbbg")

from xbbg import _request_options, blp
from xbbg._request_options import _normalize_override_value


class _DuckTimestamp:
    def __init__(self, dt: datetime) -> None:
        self._dt = dt

    def to_pydatetime(self) -> datetime:
        return self._dt


class TestNormalizeOverrideValue:
    def test_date_object(self) -> None:
        assert _normalize_override_value(date(2023, 1, 17)) == "20230117"

    def test_datetime_naive(self) -> None:
        assert _normalize_override_value(datetime(2023, 1, 17, 10, 30)) == "20230117"

    def test_datetime_aware(self) -> None:
        dt = datetime(2023, 1, 17, 10, 30, tzinfo=timezone.utc)
        assert _normalize_override_value(dt) == "20230117"

    def test_iso_date_string_normalized(self) -> None:
        assert _normalize_override_value("2023-01-17") == "20230117"

    def test_bloomberg_native_string_unchanged(self) -> None:
        assert _normalize_override_value("20230117") == "20230117"

    def test_duck_typed_timestamp(self) -> None:
        ts = _DuckTimestamp(datetime(2023, 1, 17, 10, 30))
        assert _normalize_override_value(ts) == "20230117"

    def test_other_string_passes_through(self) -> None:
        assert _normalize_override_value("MID") == "MID"

    def test_us_format_string_passes_through(self) -> None:
        # Override path is intentionally tolerant of non-ISO strings; only the
        # canonical date forms are normalized. The dedicated typed parameters
        # reject ambiguous strings instead.
        assert _normalize_override_value("01/17/2023") == "01/17/2023"

    def test_int_passes_through(self) -> None:
        assert _normalize_override_value(5) == "5"

    def test_float_passes_through(self) -> None:
        assert _normalize_override_value(1.5) == "1.5"

    def test_bool_preserved(self) -> None:
        assert _normalize_override_value(True) == "True"
        assert _normalize_override_value(False) == "False"


def test_ovr_normalizes_values_and_is_hashable() -> None:
    spec = blp.ovr(EQY_FUND_CRNCY="EUR", USER_LOCAL_TRADE_DATE=date(2023, 1, 17))

    _ = hash(spec)
    assert spec.to_pairs() == [
        ("EQY_FUND_CRNCY", "EUR"),
        ("USER_LOCAL_TRADE_DATE", "20230117"),
    ]
    assert dict(spec.items()) == {
        "EQY_FUND_CRNCY": "EUR",
        "USER_LOCAL_TRADE_DATE": "20230117",
    }


def test_ovr_composition_right_wins() -> None:
    assert (blp.ovr(A=1) | blp.ovr(A=2, B=3)).to_pairs() == [("A", "2"), ("B", "3")]


def test_ovr_normalizes_per_security_specs() -> None:
    spec = blp.ovr(
        {
            "EQY_FUND_CRNCY": "USD",
            "IBM US Equity": blp.ovr(EQY_FUND_CRNCY="EUR"),
            "MSFT US Equity": {"USER_LOCAL_TRADE_DATE": date(2024, 1, 2)},
        }
    ).for_security("TSLA US Equity", CRNCY="CAD")

    assert spec.to_pairs() == [("EQY_FUND_CRNCY", "USD")]
    assert spec.to_security_pairs() == [
        ("IBM US Equity", [("EQY_FUND_CRNCY", "EUR")]),
        ("MSFT US Equity", [("USER_LOCAL_TRADE_DATE", "20240102")]),
        ("TSLA US Equity", [("CRNCY", "CAD")]),
    ]


def test_request_overrides_split_global_and_security_pairs() -> None:
    assert _request_options._normalize_request_overrides(
        blp.ovr({"EQY_FUND_CRNCY": "USD", "IBM US Equity": {"EQY_FUND_CRNCY": "EUR"}})
    ) == (
        [("EQY_FUND_CRNCY", "USD")],
        [("IBM US Equity", [("EQY_FUND_CRNCY", "EUR")])],
    )


def test_ovr_rejects_invalid_source() -> None:
    with pytest.raises(
        TypeError,
        match=r"ovr\(\) expects mappings, OverrideSpec, or iterables of \(name, value\) pairs",
    ):
        blp.ovr("EQY_FUND_CRNCY")


def test_ovr_is_exported_from_package() -> None:
    from xbbg import OverrideSpec, ovr

    assert isinstance(ovr(A=1), OverrideSpec)
