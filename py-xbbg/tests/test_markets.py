"""Offline market adapters backed by the real native rules and override registry.

Only the network-facing PyEngine seam is replaced. Registry, session derivation
and timezone-conversion tests execute the installed native extension directly.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, create_autospec

import pandas as pd
import pytest

from xbbg import _core, _engine
from xbbg.markets import (
    ExchangeInfo,
    SessionWindows,
    afetch_exchange_info,
    ccy_pair,
    clear_exchange_override,
    convert_session_times_to_utc,
    derive_sessions,
    exch_info,
    fetch_exchange_info,
    get_exchange_override,
    get_session_windows,
    has_override,
    list_exchange_overrides,
    market_info,
    market_timing,
    set_exchange_override,
)
from xbbg.markets.info import CurrencyPair, exch_info_bloomberg

TICKER = "TEST US Equity"
OTHER_TICKER = "OTHER LN Equity"
DAY = ("09:30", "16:00")


@pytest.fixture(autouse=True)
def clean_native_overrides():
    _core.ext_clear_exchange_override()
    yield
    _core.ext_clear_exchange_override()


@pytest.fixture
def engine_seam(monkeypatch):
    """Replace network access, not the native registry or pure market helpers."""
    exchange = {
        "ticker": TICKER,
        "mic": "XNYS",
        "exch_code": "US",
        "timezone": "America/New_York",
        "utc_offset": -5.0,
        "source": "bloomberg",
        "day": DAY,
        "allday": ("04:00", "20:00"),
        "pre": ("04:00", "09:30"),
        "post": ("16:01", "20:00"),
        "am": None,
        "pm": None,
    }
    engine = SimpleNamespace(
        resolve_exchange=AsyncMock(return_value=exchange),
        fetch_market_info=AsyncMock(
            return_value={"exch": "US", "tz": "America/New_York", "freq": None, "is_fut": False}
        ),
        market_timing=AsyncMock(return_value="2024-01-15 16:00"),
    )
    get_engine = create_autospec(_engine._get_engine, return_value=engine)
    monkeypatch.setattr(_engine, "_get_engine", get_engine)
    return engine, get_engine


class TestNativeOverrides:
    def test_public_set_is_visible_directly_in_core(self):
        set_exchange_override(
            TICKER,
            timezone="America/New_York",
            mic="XNYS",
            exch_code="US",
            sessions={"day": DAY, "pre": ("04:00", "09:30")},
        )
        native = _core.ext_get_exchange_override(TICKER)
        assert native is not None
        assert native["ticker"] == TICKER
        assert native["timezone"] == "America/New_York"
        assert native["mic"] == "XNYS"
        assert native["exch_code"] == "US"
        assert native["day"] == DAY
        assert native["pre"] == ("04:00", "09:30")
        assert native["post"] is None
        assert _core.ext_list_exchange_overrides()[TICKER] == native

    def test_native_set_is_materialized_by_public_get_and_list(self):
        _core.ext_set_exchange_override(TICKER, timezone="UTC", day=DAY)
        info = get_exchange_override(TICKER)
        assert isinstance(info, ExchangeInfo)
        assert info.ticker == TICKER
        assert info.timezone == "UTC"
        assert info.sessions == {"day": DAY}
        assert info.source == "override"
        assert info.cached_at is None
        assert list_exchange_overrides() == {TICKER: info}
        assert has_override(TICKER)

    def test_metadata_updates_merge_across_python_and_native_calls(self):
        set_exchange_override(TICKER, timezone="UTC", sessions={"day": DAY})
        _core.ext_set_exchange_override(TICKER, mic="XNYS")
        set_exchange_override(TICKER, exch_code="US", timezone="America/New_York")
        info = get_exchange_override(TICKER)
        assert info is not None
        assert (info.timezone, info.mic, info.exch_code) == ("America/New_York", "XNYS", "US")
        assert info.sessions == {"day": DAY}

    def test_session_patch_replaces_not_merges_existing_windows(self):
        set_exchange_override(TICKER, timezone="UTC", sessions={"day": DAY, "pre": ("04:00", "09:30")})
        set_exchange_override(TICKER, sessions={"post": ("16:01", "20:00")})
        native = _core.ext_get_exchange_override(TICKER)
        assert native["timezone"] == "UTC"
        assert native["day"] is None
        assert native["pre"] is None
        assert native["post"] == ("16:01", "20:00")
        assert get_exchange_override(TICKER).sessions == {"post": ("16:01", "20:00")}

    def test_metadata_only_patch_preserves_all_sessions(self):
        _core.ext_set_exchange_override(TICKER, day=DAY, allday=("04:00", "20:00"))
        set_exchange_override(TICKER, mic="XNYS")
        assert get_exchange_override(TICKER).sessions == {"day": DAY, "allday": ("04:00", "20:00")}

    def test_native_defaults_and_overnight_sessions(self):
        set_exchange_override(TICKER, sessions={"day": ("18:00", "17:00")})
        info = get_exchange_override(TICKER)
        assert info.timezone == "UTC"
        assert info.mic is None
        assert info.exch_code is None
        assert info.utc_offset is None
        assert info.sessions == {"day": ("18:00", "17:00")}

    def test_ticker_whitespace_is_normalized(self):
        set_exchange_override(f"  {TICKER}  ", timezone="UTC")
        assert get_exchange_override(f"  {TICKER}  ").ticker == TICKER
        assert has_override(f"  {TICKER}  ")
        assert set(list_exchange_overrides()) == {TICKER}
        clear_exchange_override(f"  {TICKER}  ")
        assert _core.ext_get_exchange_override(TICKER) is None

    @pytest.mark.parametrize("ticker", ["", " ", "\t", "\n"])
    def test_empty_ticker_rejected_by_native_set(self, ticker):
        with pytest.raises(ValueError, match="ticker cannot be empty"):
            set_exchange_override(ticker, timezone="UTC")
        assert get_exchange_override(ticker) is None
        assert not has_override(ticker)

    def test_empty_patch_rejected_by_native_set(self):
        with pytest.raises(ValueError, match="at least one field"):
            set_exchange_override(TICKER)
        assert _core.ext_get_exchange_override(TICKER) is None

    def test_empty_sessions_rejected_without_mutating_existing_override(self):
        set_exchange_override(TICKER, timezone="UTC", sessions={"day": DAY})
        with pytest.raises(ValueError, match="at least one native session window"):
            set_exchange_override(TICKER, timezone="Asia/Tokyo", sessions={})
        assert _core.ext_get_exchange_override(TICKER)["timezone"] == "UTC"
        assert get_exchange_override(TICKER).sessions == {"day": DAY}

    @pytest.mark.parametrize("key", ["regular", "futures", "unknown"])
    def test_only_canonical_session_keys_are_accepted(self, key):
        with pytest.raises(ValueError, match="Unknown session keys"):
            set_exchange_override(TICKER, sessions={key: DAY})
        assert _core.ext_get_exchange_override(TICKER) is None

    def test_public_clear_removes_native_entries(self):
        _core.ext_set_exchange_override(TICKER, timezone="UTC")
        set_exchange_override(OTHER_TICKER, timezone="Europe/London")
        clear_exchange_override(TICKER)
        assert _core.ext_get_exchange_override(TICKER) is None
        assert set(_core.ext_list_exchange_overrides()) == {OTHER_TICKER}
        clear_exchange_override()
        assert _core.ext_list_exchange_overrides() == {}

    @pytest.mark.parametrize("ticker", ["", " ", "\t", "\n"])
    def test_empty_clear_never_means_clear_all(self, ticker):
        _core.ext_set_exchange_override(TICKER, timezone="UTC")
        set_exchange_override(OTHER_TICKER, timezone="Europe/London")
        clear_exchange_override(ticker)
        assert set(_core.ext_list_exchange_overrides()) == {TICKER, OTHER_TICKER}

    def test_native_clear_is_visible_to_public_readers(self):
        set_exchange_override(TICKER, timezone="UTC")
        _core.ext_clear_exchange_override(TICKER)
        assert not has_override(TICKER)
        assert get_exchange_override(TICKER) is None
        assert list_exchange_overrides() == {}
        clear_exchange_override("UNKNOWN")

    def test_python_mutation_does_not_mutate_native_registry(self):
        sessions = {"day": DAY}
        set_exchange_override(TICKER, sessions=sessions)
        sessions["day"] = ("10:00", "15:00")
        get_exchange_override(TICKER).sessions.clear()
        list_exchange_overrides()[TICKER].timezone = "Asia/Tokyo"
        assert get_exchange_override(TICKER).sessions == {"day": DAY}
        assert get_exchange_override(TICKER).timezone == "UTC"

    def test_concurrent_public_and_native_access(self):
        def set_and_read(index):
            ticker = f"TEST{index} US Equity"
            set_exchange_override(ticker, timezone="UTC", sessions={"day": DAY})
            return _core.ext_get_exchange_override(ticker)["day"]

        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(set_and_read, range(12))) == [DAY] * 12
        assert len(list_exchange_overrides()) == 12


class TestExchangeAdapters:
    def test_model_defaults_are_independent(self):
        first = ExchangeInfo(TICKER)
        second = ExchangeInfo(OTHER_TICKER)
        first.sessions["day"] = DAY
        assert second.sessions == {}
        assert second.timezone == "UTC"
        assert second.source == "fallback"
        assert second.cached_at is None

    def test_caller_metadata_can_carry_cache_timestamp(self):
        timestamp = datetime(2024, 1, 15)
        assert ExchangeInfo(TICKER, cached_at=timestamp).cached_at == timestamp

    async def test_async_fetch_uses_native_resolver(self, engine_seam):
        engine, get_engine = engine_seam
        explicit_engine = object()
        result = await afetch_exchange_info(TICKER, engine=explicit_engine)
        get_engine.assert_called_once_with(engine=explicit_engine)
        engine.resolve_exchange.assert_awaited_once_with(TICKER)
        assert isinstance(result, ExchangeInfo)
        assert result.sessions == {
            "day": DAY,
            "allday": ("04:00", "20:00"),
            "pre": ("04:00", "09:30"),
            "post": ("16:01", "20:00"),
        }
        assert result.utc_offset == -5.0
        assert result.cached_at is None

    def test_sync_fetch_uses_the_same_native_shape(self, engine_seam):
        engine, _ = engine_seam
        engine.resolve_exchange.return_value["source"] = "cache"
        result = fetch_exchange_info(TICKER)
        assert result.source == "cache"
        assert result.mic == "XNYS"
        engine.resolve_exchange.assert_awaited_once_with(TICKER)

    def test_public_override_materializes_through_resolver_adapter(self, engine_seam):
        engine, _ = engine_seam
        set_exchange_override(TICKER, timezone="UTC", sessions={"day": DAY})
        engine.resolve_exchange.return_value = _core.ext_get_exchange_override(TICKER)
        assert fetch_exchange_info(TICKER) == get_exchange_override(TICKER)
        engine.resolve_exchange.assert_awaited_once_with(TICKER)

    @pytest.mark.parametrize("function", [exch_info, exch_info_bloomberg])
    def test_exchange_series_preserves_list_shape_and_reference(self, engine_seam, function):
        engine, get_engine = engine_seam
        explicit_engine = object()
        result = function("IGNORED", ref=TICKER, original="IGNORED", engine=explicit_engine)
        assert isinstance(result, pd.Series)
        assert result.name == "XNYS"
        assert result["tz"] == "America/New_York"
        assert result["day"] == list(DAY)
        assert result["post"] == ["16:01", "20:00"]
        assert "am" not in result
        get_engine.assert_called_once_with(engine=explicit_engine)
        engine.resolve_exchange.assert_awaited_once_with(TICKER)

    def test_partial_override_series_is_not_rederived(self, engine_seam):
        engine, _ = engine_seam
        set_exchange_override(TICKER, timezone="UTC", mic="XNYS", sessions={"day": DAY})
        engine.resolve_exchange.return_value = _core.ext_get_exchange_override(TICKER)
        assert exch_info(TICKER).to_dict() == {"tz": "UTC", "day": list(DAY)}

    def test_native_fallback_remains_empty_series(self, engine_seam):
        engine, _ = engine_seam
        engine.resolve_exchange.return_value["source"] = "fallback"
        assert exch_info(TICKER).empty

    def test_startup_errors_are_not_disguised_as_fallback(self, engine_seam):
        _, get_engine = engine_seam
        get_engine.side_effect = RuntimeError("engine startup failed")
        with pytest.raises(RuntimeError, match="engine startup failed"):
            fetch_exchange_info(TICKER)

    def test_bdp_options_are_not_silently_ignored(self, engine_seam):
        with pytest.raises(TypeError):
            fetch_exchange_info(TICKER, backend="pandas")

    @pytest.mark.parametrize("ticker", [TICKER, "TEST1 Comdty", "CDX IG CDSI GEN 5Y Corp", "OTHER"])
    def test_market_metadata_uses_native_query_without_asset_special_cases(self, engine_seam, ticker):
        engine, _ = engine_seam
        result = market_info(ticker)
        assert result.to_dict() == {"exch": "US", "tz": "America/New_York", "is_fut": False}
        engine.fetch_market_info.assert_awaited_once_with(ticker)

    def test_futures_flag_does_not_depend_on_python_frequency_inference(self, engine_seam):
        engine, _ = engine_seam
        engine.fetch_market_info.return_value = {"exch": None, "tz": None, "freq": None, "is_fut": True}
        assert market_info("TEST1 Comdty").to_dict() == {"is_fut": True}

    def test_futures_frequency_is_present_when_native_supplies_it(self, engine_seam):
        engine, _ = engine_seam
        engine.fetch_market_info.return_value = {"exch": "CME", "tz": "America/Chicago", "freq": "HMUZ", "is_fut": True}
        assert market_info("TEST1 Comdty")["freq"] == "HMUZ"

    def test_market_query_errors_propagate(self, engine_seam):
        engine, _ = engine_seam
        engine.fetch_market_info.side_effect = ValueError("ticker is required")
        with pytest.raises(ValueError, match="ticker is required"):
            market_info("")


class TestNativeSessions:
    def test_empty_windows(self):
        assert SessionWindows().to_dict() == {}
        assert get_session_windows(TICKER).to_dict() == {}

    def test_all_session_fields_materialize(self):
        values = {
            "day": DAY,
            "allday": ("04:00", "20:00"),
            "pre": ("04:00", "09:30"),
            "post": ("16:01", "20:00"),
            "am": ("09:30", "12:00"),
            "pm": ("13:00", "16:00"),
        }
        set_exchange_override(TICKER, sessions=values)
        assert derive_sessions(get_exchange_override(TICKER)).to_dict() == values

    def test_us_rules_and_post_open_are_native(self):
        rule = _core.ext_get_market_rule("XNYS", None)
        assert rule["pre_minutes"] == 330
        assert rule["post_minutes"] == 240
        windows = get_session_windows(TICKER, mic="XNYS", regular_hours=DAY)
        assert windows.to_dict() == _core.ext_derive_sessions(*DAY, "XNYS", None)
        assert windows.day == DAY
        assert windows.pre == ("04:00", "09:30")
        assert windows.post == ("16:01", "20:00")
        assert windows.allday == ("04:00", "20:00")

    def test_native_mic_precedes_exchange_code(self):
        assert _core.ext_get_market_rule("XNYS", "CME") == _core.ext_get_market_rule("XNYS", None)
        windows = get_session_windows(TICKER, mic="XNYS", exch_code="CME", regular_hours=DAY)
        assert windows.post == ("16:01", "20:00")

    def test_native_exchange_code_fallback(self):
        windows = get_session_windows(TICKER, mic="UNKNOWN", exch_code="US", regular_hours=DAY)
        assert windows.pre == ("04:00", "09:30")
        assert _core.ext_get_market_rule("UNKNOWN", "UNKNOWN") is None

    def test_japan_lunch_and_day_close_are_native(self):
        windows = get_session_windows(TICKER, mic="XTKS", regular_hours=("09:00", "15:00"))
        assert windows.day == ("09:00", "15:30")
        assert windows.am == ("09:00", "11:30")
        assert windows.pm == ("12:30", "15:30")
        assert windows.post == ("15:31", "16:00")

    def test_continuous_sessions_do_not_create_extended_hours(self):
        windows = get_session_windows(TICKER, mic="XCME", regular_hours=("18:00", "17:00"))
        assert windows.to_dict() == {"day": ("18:00", "17:00"), "allday": ("18:00", "17:00")}

    @pytest.mark.parametrize("hours", [("0930", "1600"), ("9:30", "16:00"), ("09:30:00", "16:00:00")])
    def test_native_time_normalization(self, hours):
        assert get_session_windows(TICKER, regular_hours=hours).day == DAY

    @pytest.mark.parametrize("hours", [("25:00", "16:00"), ("09:60", "16:00"), ("invalid", "1600")])
    def test_invalid_hours_produce_empty_native_windows(self, hours):
        assert get_session_windows(TICKER, regular_hours=hours).to_dict() == {}

    def test_derive_uses_day_and_retains_explicit_extended_window(self):
        info = ExchangeInfo(TICKER, mic="XNYS", sessions={"day": DAY, "post": ("17:00", "19:00")})
        windows = derive_sessions(info)
        assert windows.day == DAY
        assert windows.pre == ("04:00", "09:30")
        assert windows.post == ("17:00", "19:00")

    def test_override_windows_are_not_rewritten_by_market_rules(self):
        set_exchange_override(TICKER, mic="XTKS", sessions={"day": ("10:00", "14:00")})
        assert derive_sessions(get_exchange_override(TICKER)).to_dict() == {"day": ("10:00", "14:00")}

    @pytest.mark.parametrize("country, expected", [(" us ", "America/New_York"), ("JP", "Asia/Tokyo"), ("ZZ", None)])
    def test_country_timezone_lookup_is_native(self, country, expected):
        assert _core.ext_infer_timezone(country) == expected


class TestNativeTimeConversion:
    def test_exact_utc_preserves_input_format(self):
        start, end = "2024-01-15 09:30:00", "2024-01-15 16:00:00"
        assert convert_session_times_to_utc(start, end, "UTC", "%H:%M") == (start, end)

    @pytest.mark.parametrize(
        "day, expected",
        [
            ("2024-01-15", ("2024-01-15T14:30:00", "2024-01-15T21:00:00")),
            ("2024-06-15", ("2024-06-15T13:30:00", "2024-06-15T20:00:00")),
        ],
    )
    def test_new_york_winter_and_summer(self, day, expected):
        assert convert_session_times_to_utc(f"{day} 09:30", f"{day} 16:00", "America/New_York") == expected

    def test_overnight_end_uses_its_own_date(self):
        assert convert_session_times_to_utc("2024-01-15 18:00", "2024-01-16 17:00", "America/Chicago") == (
            "2024-01-16T00:00:00",
            "2024-01-16T23:00:00",
        )

    def test_each_endpoint_uses_its_own_dst_offset(self):
        assert convert_session_times_to_utc("2024-03-09 18:00", "2024-03-10 17:00", "America/New_York") == (
            "2024-03-09T23:00:00",
            "2024-03-10T21:00:00",
        )

    def test_fractional_seconds_and_custom_format_are_preserved(self):
        assert convert_session_times_to_utc(
            "2024-01-15 09:30:01.123456",
            "2024-01-15 16:00:02.654321",
            "America/New_York",
            "%Y-%m-%d %H:%M:%S.%f%z",
        ) == ("2024-01-15 14:30:01.123456+0000", "2024-01-15 21:00:02.654321+0000")

    def test_conversion_delegates_to_real_native_function(self, monkeypatch):
        convert = Mock(wraps=_core.ext_session_times_to_utc)
        monkeypatch.setattr(_core, "ext_session_times_to_utc", convert)
        assert convert_session_times_to_utc("2024-01-15 08:00", "2024-01-15 16:30", "Europe/London") == (
            "2024-01-15T08:00:00",
            "2024-01-15T16:30:00",
        )
        convert.assert_called_once_with("08:00:00", "16:30:00", "Europe/London", "2024-01-15")

    @pytest.mark.parametrize("timestamp", ["2024-03-10 02:30", "2024-11-03 01:30"])
    def test_native_dst_errors_are_not_suppressed(self, timestamp):
        with pytest.raises(ValueError, match="ambiguous/nonexistent"):
            convert_session_times_to_utc(timestamp, timestamp, "America/New_York")

    def test_invalid_timezone_raises_native_error(self):
        with pytest.raises(ValueError, match="invalid timezone"):
            convert_session_times_to_utc("2024-01-15 09:30", "2024-01-15 16:00", "Not/AZone")

    def test_aware_timestamps_remain_invalid(self):
        with pytest.raises(TypeError, match="timezone-naive"):
            convert_session_times_to_utc("2024-01-15T09:30:00+00:00", "2024-01-15 16:00", "America/New_York")


class TestTimingAdapter:
    @pytest.mark.parametrize("dt", ["2024-01-15", "20240115", date(2024, 1, 15), datetime(2024, 1, 15, 12)])
    def test_dates_are_adapted_and_native_text_is_preserved(self, engine_seam, dt):
        engine, _ = engine_seam
        assert market_timing(TICKER, dt) == "2024-01-15 16:00"
        engine.market_timing.assert_awaited_once_with(TICKER, "2024-01-15", "EOD", "local")
        engine.resolve_exchange.assert_not_awaited()

    @pytest.mark.parametrize(
        "alias, expected",
        [
            ("NY", "America/New_York"),
            ("ny", "America/New_York"),
            ("LN", "Europe/London"),
            ("TK", "Asia/Tokyo"),
            ("HK", "Asia/Hong_Kong"),
        ],
    )
    def test_aliases_use_native_country_timezones(self, engine_seam, alias, expected):
        engine, _ = engine_seam
        market_timing(TICKER, "2024-01-15", tz=alias)
        engine.market_timing.assert_awaited_once_with(TICKER, "2024-01-15", "EOD", expected)

    def test_target_ticker_uses_native_exchange_resolution(self, engine_seam):
        engine, _ = engine_seam
        engine.resolve_exchange.return_value["timezone"] = "Europe/London"
        engine.market_timing.return_value = "2024-01-15 21:00:00+00:00"
        assert market_timing(TICKER, "2024-01-15", tz=OTHER_TICKER) == "2024-01-15 21:00:00+00:00"
        engine.resolve_exchange.assert_awaited_once_with(OTHER_TICKER)
        engine.market_timing.assert_awaited_once_with(TICKER, "2024-01-15", "EOD", "Europe/London")

    def test_reference_and_engine_are_forwarded(self, engine_seam):
        engine, get_engine = engine_seam
        explicit_engine = object()
        market_timing("IGNORED", "2024-01-15", timing="FINISHED", ref=TICKER, engine=explicit_engine)
        get_engine.assert_called_once_with(engine=explicit_engine)
        engine.market_timing.assert_awaited_once_with(TICKER, "2024-01-15", "FINISHED", "local")

    @pytest.mark.parametrize("timing", ["BOD", "FINISHED", " eod "])
    def test_native_engine_owns_timing_selection(self, engine_seam, timing):
        engine, _ = engine_seam
        market_timing(TICKER, "2024-01-15", timing=timing, tz="UTC")
        engine.market_timing.assert_awaited_once_with(TICKER, "2024-01-15", timing, "UTC")

    @pytest.mark.parametrize(
        "message", ["timing must be one of: BOD, EOD, FINISHED", "missing day session in ExchangeInfo"]
    )
    def test_native_timing_errors_propagate(self, engine_seam, message):
        engine, _ = engine_seam
        engine.market_timing.side_effect = ValueError(message)
        with pytest.raises(ValueError, match=message):
            market_timing(TICKER, "2024-01-15", timing="UNKNOWN")


class TestCurrencyPair:
    def test_model_is_frozen(self):
        pair = CurrencyPair(ticker="USDEUR Curncy", factor=1.0, power=1.0)
        with pytest.raises(AttributeError):
            pair.factor = 2.0

    @pytest.mark.parametrize(
        "local, base, ticker, factor",
        [
            ("EUR", "USD", "USDEUR Curncy", 1.0),
            ("GBp", "USD", "USDGBP Curncy", 100.0),
            ("GBp", "GBP", "", 100.0),
            ("GBP", "GBp", "", 0.01),
            ("USD", "USD", "", 1.0),
        ],
    )
    def test_native_fx_metadata_retains_public_model(self, local, base, ticker, factor):
        assert ccy_pair(local, base) == CurrencyPair(ticker=ticker, factor=factor, power=1.0)
