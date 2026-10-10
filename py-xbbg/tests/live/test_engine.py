#!/usr/bin/env python
"""Live Bloomberg engine test script.

Tests each API endpoint at the Rust engine level (PyEngine) with real
Bloomberg data. For high-level public API tests, see test_api.py.

Usage:
    python tests/live/test_engine.py              # Run all tests
    python tests/live/test_engine.py bdp bdh      # Run only bdp and bdh tests
    python tests/live/test_engine.py --list       # List available tests
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import logging
import sys

import pytest

logger = logging.getLogger(__name__)

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    day = date(year, month, 1)
    while day.weekday() != weekday:
        day += timedelta(days=1)
    return day + timedelta(days=7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    day = date(year, month + 1, 1) - timedelta(days=1)
    while day.weekday() != weekday:
        day -= timedelta(days=1)
    return day


def _easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_offset = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_offset) // 451
    month = (h + weekday_offset - 7 * m + 114) // 31
    day = ((h + weekday_offset - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _is_us_equity_market_holiday(day: date) -> bool:
    year = day.year
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_date(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }
    return day in holidays


def _recent_trading_day() -> date:
    for days_back in range(1, 15):
        candidate = date.today() - timedelta(days=days_back)
        if candidate.weekday() < 5 and not _is_us_equity_market_holiday(candidate):
            return candidate
    raise RuntimeError("No recent US equity trading day found in the last 14 calendar days")


def _skip_if_screen_unavailable(exc: Exception, context: str) -> None:
    message = str(exc)
    unavailable_markers = (
        "Cannot find screen",
        "e_SCREEN_NOT_FOUND",
        "SCREEN_NOT_FOUND",
        "Failed to build query",
        "empty criteriaArray",
        "NOT_ENTITLED",
        "blocked from accessing serviceCode=BEQS",
    )
    if any(marker in message for marker in unavailable_markers):
        pytest.skip(f"{context} screen not available in this Bloomberg environment: {exc}")
    raise exc


def _format_price(value) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "N/A"
    return f"{numeric:8.2f}"


def _format_volume_millions(value) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "N/A"
    return f"{numeric / 1e6:.1f}M"


def get_engine():
    """Create and return a Bloomberg engine."""
    from xbbg._core import PyEngine

    return PyEngine()


@pytest.fixture(scope="module")
def engine():
    """Shared Bloomberg engine fixture for pytest collection."""
    return get_engine()


async def test_bdp(engine):
    """Test Reference Data (bdp) - single point data."""
    logger.info("Testing: bdp (Reference Data)")
    logger.info("-" * 40)

    params = {
        "service": "//blp/refdata",
        "operation": "ReferenceDataRequest",
        "extractor": "refdata",
        "securities": ["AAPL US Equity", "MSFT US Equity", "GOOGL US Equity"],
        "fields": ["PX_LAST", "NAME", "CUR_MKT_CAP"],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    # Group by ticker for display
    tickers = result["ticker"].to_pylist()
    fields = result["field"].to_pylist()
    values = result["value"].to_pylist()

    current_ticker = None
    for t, f, v in zip(tickers, fields, values):
        if t != current_ticker:
            logger.debug(f"\n  {t}:")
            current_ticker = t
        logger.debug(f"    {f}: {v}")

    logger.debug("")
    return True


async def test_bdh(engine):
    """Test Historical Data (bdh) - time series data."""
    logger.info("Testing: bdh (Historical Data)")
    logger.info("-" * 40)

    # Get last 5 trading days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)

    params = {
        "service": "//blp/refdata",
        "operation": "HistoricalDataRequest",
        "extractor": "histdata",
        "securities": ["SPY US Equity"],
        "fields": ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "VOLUME"],
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")
    logger.debug("")

    # HistData defaults to long shape: ticker/date/field/value.
    dates = result["date"].to_pylist()
    fields = result["field"].to_pylist()
    values = result["value"].to_pylist()

    volumes_by_date = {d: v for d, f, v in zip(dates, fields, values) if f == "VOLUME"}

    logger.info("  Date        | PX_LAST  | Volume")
    logger.info("  " + "-" * 35)
    for d, f, p in zip(dates, fields, values):
        if f != "PX_LAST":
            continue
        volume = volumes_by_date.get(d)
        vol_str = _format_volume_millions(volume) if volume is not None else "N/A"
        logger.debug(f"  {d}  | {_format_price(p)} | {vol_str}")

    logger.debug("")
    return True


async def test_bdh_multi(engine):
    """Test Historical Data with multiple securities."""
    logger.info("Testing: bdh_multi (Historical Data - Multiple Securities)")
    logger.info("-" * 40)

    end_date = date.today()
    start_date = end_date - timedelta(days=5)

    params = {
        "service": "//blp/refdata",
        "operation": "HistoricalDataRequest",
        "extractor": "histdata",
        "securities": ["AAPL US Equity", "MSFT US Equity"],
        "fields": ["PX_LAST"],
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Rows: {result.num_rows}")

    tickers = result["ticker"].to_pylist()
    dates = result["date"].to_pylist()
    fields = result["field"].to_pylist()
    values = result["value"].to_pylist()

    current_ticker = None
    for t, d, f, p in zip(tickers, dates, fields, values):
        if f != "PX_LAST":
            continue
        if t != current_ticker:
            logger.debug(f"\n  {t}:")
            current_ticker = t
        logger.debug(f"    {d}: {p:.2f}")

    logger.debug("")
    return True


async def test_bds(engine):
    """Test Bulk Data (bds) - bulk reference data."""
    logger.info("Testing: bds (Bulk Data)")
    logger.info("-" * 40)

    params = {
        "service": "//blp/refdata",
        "operation": "ReferenceDataRequest",
        "extractor": "bulk",
        "securities": ["SPY US Equity"],
        "fields": ["TOP_20_HOLDERS_PUBLIC_FILINGS"],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=60.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Top 5 holders:")
        for i, col in enumerate(result.schema.names[:5]):
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_bdib(engine):
    """Test Intraday Bars (bdib) - intraday OHLCV bars."""
    logger.info("Testing: bdib (Intraday Bars)")
    logger.info("-" * 40)

    # Get bars from yesterday (market hours in UTC)
    # US market: 9:30 AM - 4:00 PM ET = 14:30 - 21:00 UTC
    trading_day = _recent_trading_day()
    start_dt = f"{trading_day.strftime('%Y-%m-%d')}T14:30:00"
    end_dt = f"{trading_day.strftime('%Y-%m-%d')}T15:30:00"

    params = {
        "service": "//blp/refdata",
        "operation": "IntradayBarRequest",
        "extractor": "intraday_bar",
        "security": "SPY US Equity",
        "event_type": "TRADE",
        "interval": 5,  # 5-minute bars
        "start_datetime": start_dt,
        "end_datetime": end_dt,
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  First 5 bars:")
        times = result["time"].to_pylist()[:5] if "time" in result.schema.names else []
        opens = result["open"].to_pylist()[:5] if "open" in result.schema.names else []
        closes = result["close"].to_pylist()[:5] if "close" in result.schema.names else []

        for t, o, c in zip(times, opens, closes):
            logger.debug(f"    {t}: open={o:.2f}, close={c:.2f}")

    logger.debug("")
    return True


async def test_bdtick(engine):
    """Test Intraday Ticks (bdtick) - tick-level data."""
    logger.info("Testing: bdtick (Intraday Ticks)")
    logger.info("-" * 40)

    # Get ticks from yesterday (market hours in UTC)
    # US market opens at 9:30 AM ET = 14:30 UTC
    trading_day = _recent_trading_day()
    start_dt = f"{trading_day.strftime('%Y-%m-%d')}T14:30:00"
    end_dt = f"{trading_day.strftime('%Y-%m-%d')}T14:31:00"  # Just 1 minute

    params = {
        "service": "//blp/refdata",
        "operation": "IntradayTickRequest",
        "extractor": "intraday_tick",
        "security": "SPY US Equity",
        "event_types": ["TRADE"],  # Must specify event types for tick data
        "start_datetime": start_dt,
        "end_datetime": end_dt,
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info(f"\n  First 5 ticks (of {result.num_rows}):")
        for col in result.schema.names[:4]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_field_info(engine):
    """Test Field Info - get field metadata."""
    logger.info("Testing: field_info (Field Metadata)")
    logger.info("-" * 40)

    params = {
        "service": "//blp/apiflds",
        "operation": "FieldInfoRequest",
        "extractor": "fieldinfo",
        "field_ids": ["PX_LAST", "VOLUME", "NAME", "MARKET_CAP"],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Field info:")
        for col in result.schema.names:
            values = result[col].to_pylist()
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_schema_introspection(engine):
    """Test Schema Introspection - get service schema."""
    logger.info("Testing: schema (Service Introspection)")
    logger.info("-" * 40)

    # List operations for //blp/refdata
    ops = await engine.list_operations("//blp/refdata")
    logger.info(f"  Operations in //blp/refdata: {len(ops)}")
    logger.info(f"  Available: {', '.join(ops[:5])}...")

    # Get valid elements for HistoricalDataRequest
    elements = await engine.list_valid_elements("//blp/refdata", "HistoricalDataRequest")
    logger.info(f"\n  Elements for HistoricalDataRequest: {len(elements)}")
    logger.info(f"  Sample: {', '.join(sorted(elements)[:8])}...")

    logger.debug("")
    return True


async def test_ext_functions(engine):
    """Test Extension Functions from xbbg-ext."""
    logger.info("Testing: ext (Extension Functions)")
    logger.info("-" * 40)

    from xbbg._core import (
        ext_build_fx_pair,
        ext_fmt_date,
        ext_generate_futures_candidates,
        ext_get_futures_months,
        ext_is_specific_contract,
        ext_parse_date,
        ext_same_currency,
    )

    # Date parsing
    parsed = ext_parse_date("2024-03-15")
    logger.info(f"  parse_date('2024-03-15'): {parsed}")

    formatted = ext_fmt_date(2024, 3, 15, "%Y%m%d")
    logger.info(f"  fmt_date(2024, 3, 15): {formatted}")

    # Ticker utilities
    is_specific = ext_is_specific_contract("ESH24 Index")
    logger.info(f"  is_specific_contract('ESH24 Index'): {is_specific}")

    is_generic = ext_is_specific_contract("ES1 Index")
    logger.info(f"  is_specific_contract('ES1 Index'): {is_generic}")

    # Futures candidates
    candidates = ext_generate_futures_candidates("ES1 Index", 2024, 3, 15, "Q", 4)
    logger.info("  generate_futures_candidates('ES1 Index', Q, 4):")
    for ticker, year, month in candidates:
        logger.debug(f"    {ticker} ({year}-{month:02d})")

    # FX pair building
    fx = ext_build_fx_pair("GBp", "USD")
    logger.info(f"  build_fx_pair('GBp', 'USD'): {fx}")

    # Currency comparison
    same = ext_same_currency("GBP", "GBp")
    logger.info(f"  same_currency('GBP', 'GBp'): {same}")

    # Constants
    months = ext_get_futures_months()
    logger.info(f"  futures_months: {dict(list(months.items())[:6])}...")

    logger.debug("")
    return True


async def test_bql(engine):
    """Test Bloomberg Query Language (bql) - BQL queries."""
    logger.info("Testing: bql (Bloomberg Query Language)")
    logger.info("-" * 40)

    # BQL request via //blp/bqlsvc service
    params = {
        "service": "//blp/bqlsvc",
        "operation": "sendQuery",
        "extractor": "bql",
        "elements": [("expression", "get(px_last) for('AAPL US Equity')")],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=60.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  BQL Results:")
        for col in result.schema.names[:5]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_bsrch(engine):
    """Test Bloomberg Search (bsrch) - saved searches.

    Note: BSRCH requires a valid Domain (saved search name). Common domains include
    user-created screens. The test uses a generic domain that may return 0 results
    if no matching saved searches exist.
    """
    logger.info("Testing: bsrch (Bloomberg Search)")
    logger.info("-" * 40)

    # BSRCH request via //blp/exrsvc service
    # Note: Domain must be a valid saved search name (user-specific)
    # Using FI:SOVR as an example - may return error if not available
    params = {
        "service": "//blp/exrsvc",
        "operation": "ExcelGetGridRequest",
        "extractor": "generic",  # Use generic to see full response
        "elements": [("Domain", "FI:SOVR")],
    }

    try:
        result = await asyncio.wait_for(engine.request(params), timeout=60.0)
    except Exception as exc:
        _skip_if_screen_unavailable(exc, "FI:SOVR")

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Response:")
        for col in result.schema.names[:5]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    # Check for error message in response
    if "path" in result.schema.names:
        paths = result["path"].to_pylist()
        if "Error" in paths:
            logger.debug("\n  Note: Domain may not be valid for this Bloomberg account")

    logger.debug("")
    return True


async def test_blkp(engine):
    """Test Security Lookup (blkp) - search for securities."""
    logger.info("Testing: blkp (Security Lookup)")
    logger.info("-" * 40)

    # instrumentListRequest via //blp/instruments service
    params = {
        "service": "//blp/instruments",
        "operation": "instrumentListRequest",
        "extractor": "generic",
        "elements": [
            ("query", "Apple"),
            ("yellowKeyFilter", "YK_FILTER_EQTY"),
            ("maxResults", "10"),
        ],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Search results:")
        for col in result.schema.names[:3]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_bcurves(engine):
    """Test Yield Curve List (bcurves) - search for curves."""
    logger.info("Testing: bcurves (Yield Curve List)")
    logger.info("-" * 40)

    # curveListRequest via //blp/instruments service
    # Use 'query' parameter for text search (required for results)
    params = {
        "service": "//blp/instruments",
        "operation": "curveListRequest",
        "extractor": "generic",
        "elements": [
            ("query", "Treasury"),
            ("maxResults", "10"),
        ],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Curves found:")
        for col in result.schema.names[:5]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_bgovts(engine):
    """Test Government Securities List (bgovts) - search for govt bonds."""
    logger.info("Testing: bgovts (Government Securities List)")
    logger.info("-" * 40)

    # govtListRequest via //blp/instruments service
    # Use 'query' parameter for text search (required for results)
    params = {
        "service": "//blp/instruments",
        "operation": "govtListRequest",
        "extractor": "generic",
        "elements": [
            ("query", "US Treasury"),
            ("maxResults", "10"),
        ],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=30.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Government securities found:")
        for col in result.schema.names[:5]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_beqs(engine):
    """Test Equity Screening (beqs) - run saved screens."""
    logger.info("Testing: beqs (Equity Screening)")
    logger.info("-" * 40)

    # BeqsRequest via //blp/refdata service
    # Using a Bloomberg GLOBAL screen that should exist
    params = {
        "service": "//blp/refdata",
        "operation": "BeqsRequest",
        "extractor": "generic",
        "elements": [
            ("screenName", "TOP_DECL_DVD"),
            ("screenType", "GLOBAL"),
            ("Group", "General"),
        ],
    }

    try:
        result = await asyncio.wait_for(engine.request(params), timeout=60.0)
    except Exception as exc:
        _skip_if_screen_unavailable(exc, "TOP_DECL_DVD")

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Screen results:")
        for col in result.schema.names[:3]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_bta(engine):
    """Test Technical Analysis (bta) - technical study data."""
    logger.info("Testing: bta (Technical Analysis)")
    logger.info("-" * 40)

    # Get dates for the study
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=30)

    # //blp/tasvc studyRequest with nested elements via dotted path notation
    params = {
        "service": "//blp/tasvc",
        "operation": "studyRequest",
        "extractor": "generic",
        "elements": [
            # priceSource sub-element
            ("priceSource.securityName", "AAPL US Equity"),
            # priceSource.dataRange.historical sub-element
            ("priceSource.dataRange.historical.startDate", start_date.strftime("%Y%m%d")),
            ("priceSource.dataRange.historical.endDate", end_date.strftime("%Y%m%d")),
            ("priceSource.dataRange.historical.periodicitySelection", "DAILY"),
            # studyAttributes.smavgStudyAttributes sub-element (Simple Moving Average)
            ("studyAttributes.smavgStudyAttributes.period", "20"),
            ("studyAttributes.smavgStudyAttributes.priceSourceClose", "PX_LAST"),
        ],
    }

    result = await asyncio.wait_for(engine.request(params), timeout=60.0)

    logger.info(f"  Schema: {result.schema}")
    logger.info(f"  Rows: {result.num_rows}")

    if result.num_rows > 0:
        logger.info("\n  Technical Analysis results:")
        for col in result.schema.names[:5]:
            values = result[col].to_pylist()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_yas(engine):
    """Test Yield & Spread Analysis (yas) - bond yield calculations."""
    logger.info("Testing: yas (Yield & Spread Analysis)")
    logger.info("-" * 40)

    from xbbg import abdp

    # Get yield and duration for a Treasury bond
    # Using the generic on-the-run 10Y Treasury
    # yas() is a wrapper around bdp() with YAS override fields
    df = await abdp(
        "GT10 Govt",  # Generic 10-year Treasury (more reliable than CUSIP)
        ["YAS_BOND_YLD", "YAS_MOD_DUR", "YLD_YTM_MID"],
    )

    logger.info(f"  Rows: {len(df)}")
    logger.debug(f"  Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")

    # Display results
    if len(df) > 0:
        logger.info("\n  YAS Results:")
        for col in df.columns[:5]:
            values = df[col].to_list()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_etf_holdings(engine):
    """Test ETF Holdings (etf_holdings) - get ETF constituents via BQL."""
    logger.info("Testing: etf_holdings (ETF Holdings)")
    logger.info("-" * 40)

    from xbbg import abql

    # Get holdings for SPY ETF using BQL
    # etf_holdings() is a wrapper that builds this BQL query
    bql_query = "get(id_isin, weights, id().position) for(holdings('SPY US Equity'))"
    df = await abql(bql_query)

    logger.info(f"  Rows: {len(df)}")
    logger.debug(f"  Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")

    # Display first few holdings
    if len(df) > 0:
        logger.info("\n  Top 5 Holdings:")
        for col in df.columns[:4]:
            values = df[col].to_list()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_preferreds(engine):
    """Test Preferred Stocks (preferreds) - find preferreds via BQL."""
    logger.info("Testing: preferreds (Preferred Stocks)")
    logger.info("-" * 40)

    from xbbg import abql

    # Get preferred stocks for Bank of America using BQL
    # preferreds() is a wrapper that builds this BQL query
    bql_query = (
        "get(id, name) for(filter(debt(['BAC US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Preferreds'))"
    )
    df = await abql(bql_query)

    logger.info(f"  Rows: {len(df)}")
    logger.debug(f"  Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")

    # Display results
    if len(df) > 0:
        logger.info("\n  Preferred Stocks:")
        for col in df.columns[:3]:
            values = df[col].to_list()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_corporate_bonds(engine):
    """Test Corporate Bonds (corporate_bonds) - find bonds via BQL."""
    logger.info("Testing: corporate_bonds (Corporate Bonds)")
    logger.info("-" * 40)

    from xbbg import abql

    # Get active USD corporate bonds for Apple using BQL
    # corporate_bonds() is a wrapper that builds this BQL query
    bql_query = (
        "get(id) "
        "for(filter(bondsuniv('active', CONSOLIDATEDUPLICATES='N'), "
        "SRCH_ASSET_CLASS=='Corporates' AND TICKER=='AAPL' AND CRNCY=='USD'))"
    )
    df = await abql(bql_query)

    logger.info(f"  Rows: {len(df)}")
    logger.debug(f"  Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")

    # Display results
    if len(df) > 0:
        logger.info("\n  Corporate Bonds:")
        for col in df.columns[:3]:
            values = df[col].to_list()[:5]
            logger.debug(f"    {col}: {values}")

    logger.debug("")
    return True


async def test_ext_async(engine):
    """Test async extension functions (ayas, aetf_holdings, etc.)."""
    logger.info("Testing: ext_async (Async Extension Functions)")
    logger.info("-" * 40)

    from xbbg import ext

    # Test ayas - async yield & spread analysis
    logger.info("  Testing ayas()...")
    try:
        df = await ext.ayas("GT10 Govt", ["YAS_BOND_YLD", "YAS_MOD_DUR"])
        logger.info(f"    ayas: {len(df)} rows")
    except Exception as e:
        logger.info(f"    ayas: Error - {e}")

    # Test aetf_holdings - async ETF holdings
    logger.info("  Testing aetf_holdings()...")
    try:
        df = await ext.aetf_holdings("SPY US Equity")
        logger.info(f"    aetf_holdings: {len(df)} rows")
    except Exception as e:
        logger.info(f"    aetf_holdings: Error - {e}")

    # Test apreferreds - async find preferred stocks
    logger.info("  Testing apreferreds()...")
    try:
        df = await ext.apreferreds("BAC US Equity")
        logger.info(f"    apreferreds: {len(df)} rows")
    except Exception as e:
        logger.info(f"    apreferreds: Error - {e}")

    # Test acorporate_bonds - async find corporate bonds
    logger.info("  Testing acorporate_bonds()...")
    try:
        df = await ext.acorporate_bonds("AAPL")
        logger.info(f"    acorporate_bonds: {len(df)} rows")
    except Exception as e:
        logger.info(f"    acorporate_bonds: Error - {e}")

    # Test adividend - async dividend history
    logger.info("  Testing adividend()...")
    try:
        df = await ext.adividend("AAPL US Equity", start_date="2024-01-01")
        logger.info(f"    adividend: {len(df)} rows")
    except Exception as e:
        logger.info(f"    adividend: Error - {e}")

    # Test afut_ticker - async futures ticker resolution
    logger.info("  Testing afut_ticker()...")
    try:
        ticker = await ext.afut_ticker("ES1 Index", "2024-06-15")
        logger.info(f"    afut_ticker: {ticker}")
    except Exception as e:
        logger.info(f"    afut_ticker: Error - {e}")

    logger.debug("")
    return True


# Test registry
TESTS = {
    "bdp": test_bdp,
    "bdh": test_bdh,
    "bdh_multi": test_bdh_multi,
    "bds": test_bds,
    "bdib": test_bdib,
    "bdtick": test_bdtick,
    "field_info": test_field_info,
    "schema": test_schema_introspection,
    "ext": test_ext_functions,
    "bql": test_bql,
    "bsrch": test_bsrch,
    "blkp": test_blkp,
    "bcurves": test_bcurves,
    "bgovts": test_bgovts,
    "beqs": test_beqs,
    "bta": test_bta,
    # Fixed income extensions
    "yas": test_yas,
    "etf_holdings": test_etf_holdings,
    "preferreds": test_preferreds,
    "corporate_bonds": test_corporate_bonds,
    # Async extension functions
    "ext_async": test_ext_async,
}


async def run_tests(test_names: list[str]):
    """Run selected tests."""
    engine = get_engine()

    passed = 0
    failed = 0
    skipped = 0

    for name in test_names:
        if name not in TESTS:
            logger.warning(f"Unknown test: {name}")
            skipped += 1
            continue

        try:
            logger.debug(f"\n{'=' * 50}")
            success = await TESTS[name](engine)
            if success:
                passed += 1
                logger.info(f"PASSED: {name}")
            else:
                failed += 1
                logger.error(f"FAILED: {name}")
        except pytest.skip.Exception as e:
            skipped += 1
            logger.warning(f"SKIPPED: {name} - {e}")
        except TimeoutError:
            failed += 1
            logger.warning(f"TIMEOUT: {name}")
        except Exception as e:
            failed += 1
            logger.error(f"ERROR: {name} - {e}")

    logger.info(f"\n{'=' * 50}")
    logger.info(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    logger.info(f"{'=' * 50}")

    return failed == 0


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Live Bloomberg API tests")
    parser.add_argument(
        "tests",
        nargs="*",
        default=list(TESTS.keys()),
        help="Tests to run (default: all)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available tests",
    )

    args = parser.parse_args()

    if args.list:
        logger.info("Available tests:")
        for name, func in TESTS.items():
            doc = func.__doc__.split("\n")[0] if func.__doc__ else ""
            logger.info(f"  {name:12} - {doc}")
        return 0

    logger.info("=" * 50)
    logger.info("XBBG Live Bloomberg API Tests")
    logger.info("=" * 50)
    logger.info(f"Running tests: {', '.join(args.tests)}")

    success = asyncio.run(run_tests(args.tests))
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
