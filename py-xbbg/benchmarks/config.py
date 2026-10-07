"""Benchmark configuration for xbbg performance testing."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ============================================================================
# Test Data Configuration
# ============================================================================

# Tickers to use for benchmarks (minimal set to limit data usage)
TICKERS_SINGLE = ["IBM US Equity"]
TICKERS_MULTI = ["IBM US Equity", "AAPL US Equity", "MSFT US Equity"]

# Fields for reference data
FIELDS_SINGLE = ["PX_LAST"]
FIELDS_MULTI = ["PX_LAST", "VOLUME", "TRADING_DT_REALTIME"]

# Historical data range (keep short to minimize data usage)
BDH_START = "2025-01-02"
BDH_END = "2025-01-06"  # ~3-4 trading days


def _recent_trading_day() -> str:
    """Most recent weekday at least one day back (intraday data is only
    retained for ~6 months, so a fixed date rots)."""
    from datetime import date, timedelta

    day = date.today() - timedelta(days=1)
    while day.weekday() >= 5:  # Sat/Sun
        day -= timedelta(days=1)
    return day.isoformat()


def intraday_window_utc(date_value: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    """Convert configured New York wall times to naive UTC for clients that assume UTC."""
    market_tz = ZoneInfo(BENCH_TZ)

    def to_utc(time_value: str) -> datetime:
        local = datetime.fromisoformat(f"{date_value}T{time_value}").replace(tzinfo=market_tz)
        return local.astimezone(timezone.utc).replace(tzinfo=None)

    return to_utc(start_time), to_utc(end_time)


# Intraday data (dynamic date: Bloomberg retains intraday history ~6 months)
BDIB_DATE = _recent_trading_day()
BDIB_START_TIME = "09:30"
BDIB_END_TIME = "10:00"  # 30 minutes
BDIB_INTERVAL = 5  # 5-minute bars

# Tick data
BDTICK_DATE = _recent_trading_day()
BDTICK_START_TIME = "09:30:00"
BDTICK_END_TIME = "09:35:00"  # 5 minutes

# Intraday request times above are New York wall times; naive datetimes are
# otherwise interpreted as UTC (09:30 UTC = pre-market ET = empty results).
BENCH_TZ = "America/New_York"

# BQL query
BQL_SIMPLE = "get(px_last) for(['IBM US Equity'])"
BQL_MULTI = "get(px_last, px_volume) for(['IBM US Equity', 'AAPL US Equity'])"

# ============================================================================
# Benchmark Settings
# ============================================================================

# Warm-session samples. Fresh-process first-result timing is measured separately
# in a dedicated child process for every package and scenario.
ITERATIONS = 5
WARMUP_ITERATIONS = 1

# Time limit per benchmark (seconds)
TIMEOUT = 60

# ============================================================================
# Packages to Compare
# ============================================================================

PACKAGES = {
    "xbbg-rust": {
        "name": "xbbg (Rust 1.0+)",
        "enabled": True,
        "import": "xbbg",
        "version_check": lambda: __import__("xbbg").__version__,
    },
    "pdblp": {
        "name": "pdblp",
        "enabled": True,
        "import": "pdblp",
        "version_check": lambda: __import__("pdblp").__version__,
        "install_cmd": "pip install pdblp",
    },
}

# ============================================================================
# Output Configuration
# ============================================================================

RESULTS_DIR = "benchmarks/results"
RESULTS_FORMAT = "json"  # json, csv, markdown

# Report options
GENERATE_MARKDOWN = True
GENERATE_CSV = True
GENERATE_JSON = True
GENERATE_HTML = False

# ============================================================================
# Metrics to Track
# ============================================================================

METRICS = [
    "fresh_process_first_result_ms",
    "fresh_process_sample_count",
    "warm_first_ms",
    "warm_mean_ms",
    "warm_median_ms",
    "warm_max_ms",
    "warm_sample_count",
    "warm_p95_ms",  # null unless at least 20 warm samples
    "warm_p99_ms",  # null unless at least 100 warm samples
    "python_tracemalloc_peak_mb",  # separate untimed call; Python-visible allocations only
    "data_shape",
]

# ============================================================================
# Performance Thresholds
# ============================================================================

# Acceptable regression from main branch (for CI)
REGRESSION_THRESHOLD_PERCENT = 10  # Fail if >10% slower

EXPECTED_SPEEDUP_VS_PDBLP = 3.0  # Target: 3x faster

# ============================================================================
# CI Configuration (for GitHub Actions)
# ============================================================================

CI_ENABLED = True
CI_PR_COMMENT = True  # Post results as PR comment
CI_FAIL_ON_REGRESSION = True  # Fail CI if performance regresses
CI_STORE_RESULTS = True  # Upload results to GitHub Pages
