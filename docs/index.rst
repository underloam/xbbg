.. raw:: html

   <div align="center">
   
   <a href="https://github.com/alpha-xone/xbbg">
   <img src="https://raw.githubusercontent.com/alpha-xone/xbbg/main/docs/xbbg.png" alt="xbbg logo" width="150">
   </a>
   
   <p><b>xbbg: An intuitive Bloomberg API for Python</b></p>
   
   <p>
   <a href="https://pypi.org/project/xbbg/"><img src="https://img.shields.io/pypi/v/xbbg.svg" alt="PyPI version"></a>
   <a href="https://pypi.org/project/xbbg/"><img src="https://img.shields.io/pypi/pyversions/xbbg.svg" alt="Python versions"></a>
   <a href="https://pypi.org/project/xbbg/"><img src="https://img.shields.io/pypi/dm/xbbg" alt="PyPI Downloads"></a>
    <a href="https://discord.gg/P34uMwgCjC"><img src="https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
   </p>
   
   <p>
   <a href="https://www.buymeacoffee.com/Lntx29Oof"><img src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-1E3A8A?style=plastic&logo=buy-me-a-coffee&logoColor=white" alt="Buy Me a Coffee"></a>
   </p>
   
   <p><b>Quick Links:</b> <a href="https://xbbg.readthedocs.io/">Documentation</a> • <a href="#installation">Installation</a> • <a href="#quickstart">Quickstart</a> • <a href="#examples">Examples</a> • <a href="https://github.com/alpha-xone/xbbg">Source</a> • <a href="https://github.com/alpha-xone/xbbg/issues">Issues</a></p>
   
   </div>

xbbg
====

..
   xbbg:latest-release-start

Latest release: xbbg==0.12.4 (release: `notes <https://github.com/underloam/xbbg/releases/tag/v0.12.4>`_)

   xbbg:latest-release-end

Overview
========

xbbg is the **most comprehensive and intuitive Bloomberg API wrapper for Python**, providing a Pythonic interface with Excel-compatible inputs, straightforward intraday bar requests, and real-time subscriptions. All functions return DataFrames in your preferred format (pandas, Polars, PyArrow, DuckDB, and more) for seamless integration with your data workflow.

**Why xbbg?**

- 🎯 **Complete API Coverage**: Reference, historical, intraday bars, tick data, real-time subscriptions, BQL, BEQS, BSRCH, BQR, BTA, bond/options/CDX analytics
- 📊 **Excel-Compatible**: Use familiar Excel date formats and field names - no learning curve
- ⚡ **Built-in Caching**: Automatic Parquet-based intraday bar caching and 13 DataFrame backend options
- 🔧 **Rich Utilities**: Currency conversion, futures/CDX resolvers, exchange-aware market hours, and more
- 🚀 **Modern & Active**: Python 3.10-3.14, async-first architecture, regular updates and active maintenance
- 💡 **Intuitive Design**: Simple, consistent API (``bdp``, ``bdh``, ``bdib``, etc.) that feels natural to use

See `examples/xbbg_jupyter_examples.ipynb <https://github.com/alpha-xone/xbbg/blob/main/examples/xbbg_jupyter_examples.ipynb>`_ for interactive tutorials and examples.

Why Choose xbbg?
================

xbbg stands out as the most comprehensive and user-friendly Bloomberg API wrapper for Python. Here's how it compares to alternatives:

**Key Advantages:**

- 🎯 **Most Complete API**: Covers reference, historical, intraday, tick, real-time, screening, BQL, BTA, bond/options/CDX analytics
- 📊 **Excel Compatibility**: Use familiar Excel date formats and field names
- ⚡ **Performance**: Built-in Parquet caching reduces API calls and speeds up workflows
- 🔧 **Rich Utilities**: Currency conversion, futures resolvers, and more out of the box
- 🚀 **Modern & Active**: Python 3.10-3.14, async-first architecture, regular updates and active maintenance
- 💡 **Intuitive Design**: Simple, consistent API that feels natural to use

Requirements
============

- Bloomberg C++ SDK version 3.12.1 or higher

    - Visit `Bloomberg API Library`_ and download C++ Supported Release

    - In the ``bin`` folder of downloaded zip file, copy ``blpapi3_32.dll`` and ``blpapi3_64.dll`` to Bloomberg ``BLPAPI_ROOT`` folder (usually ``blp/DAPI``)

- Bloomberg official Python API:

.. code-block:: console

   pip install blpapi --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/

- narwhals, pyarrow (automatically installed). Optional: pandas, polars, duckdb, and other backends.

Installation
============

.. code-block:: console

   pip install xbbg

Supported Python versions: 3.10 – 3.14 (universal wheel)

Supported Functionality
========================

xbbg provides comprehensive Bloomberg API coverage:

**Reference Data:**
- ``bdp()`` / ``abdp()`` - Single point-in-time reference data
- ``bds()`` / ``abds()`` - Bulk/block data (multi-row)

**Historical Data:**
- ``bdh()`` / ``abdh()`` - End-of-day historical data
- ``dividend()`` - Dividend & split history
- ``earning()`` - Corporate earnings breakdowns
- ``turnover()`` - Trading volume & turnover

**Intraday Data:**
- ``bdib()`` / ``abdib()`` - Intraday bar data
- ``bdtick()`` / ``abdtick()`` - Tick-by-tick data
- ``exchange_tz()`` - Exchange timezone lookup

**Fixed Income:**
- ``yas()`` - Yield & Spread Analysis (YAS calculator)
- ``bond_info()``, ``bond_risk()``, ``bond_spreads()`` - Bond analytics (via ``xbbg.ext``)
- ``bond_cashflows()``, ``bond_key_rates()``, ``bond_curve()`` - Advanced bond analytics

**Options Analytics (via ``xbbg.ext``):**
- ``option_info()``, ``option_greeks()``, ``option_pricing()``
- ``option_chain()``, ``option_chain_bql()``, ``option_screen()``

**Screening & Queries:**
- ``beqs()`` / ``abeqs()`` - Bloomberg Equity Screening
- ``bql()`` / ``abql()`` - Bloomberg Query Language
- ``bqr()`` / ``abqr()`` - Bloomberg Quote Request (dealer quotes)
- ``bsrch()`` / ``absrch()`` - Bloomberg Search
- ``bta()`` / ``abta()`` - Bloomberg Technical Analysis

**Real-time:**
- ``live()`` - Real-time market data
- ``subscribe()`` - Real-time subscriptions
- ``stream()`` - Async streaming

**Utilities:**
- ``adjust_ccy()`` - Currency conversion
- ``active_futures()`` / ``fut_ticker()`` - Futures contract resolution
- ``cdx_ticker()`` / ``active_cdx()`` - CDX index resolution
- ``cdx_info()``, ``cdx_pricing()``, ``cdx_risk()`` - CDX analytics (via ``xbbg.ext``)

**Additional Features**: Multi-backend output (13 backends), 5 output formats (WIDE, LONG, SEMI_LONG, LONG_TYPED, LONG_WITH_METADATA), async/await support, local caching (Parquet), configurable logging, timezone support, exchange-aware market hours

Quickstart
==========

.. code-block:: python

   from xbbg import blp

   # Reference data (BDP)
   ref = blp.bdp(tickers='AAPL US Equity', flds=['Security_Name', 'GICS_Sector_Name'])
   print(ref)

   # Historical data (BDH)
   hist = blp.bdh('SPX Index', ['high', 'low', 'last_price'], '2021-01-01', '2021-01-05')
   print(hist.tail())

What's New
==========

.. xbbg:changelog-start

*0.12.0* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.12.0>`__

### Added

- **Async-first architecture**: All Bloomberg API functions (`bdp`, `bds`, `bdh`, `bdib`, `bdtick`, `bql`, `beqs`, `bsrch`, `bqr`, `bta`) now have async counterparts (`abdp`, `abds`, `abdh`, etc.) as the source of truth; sync wrappers delegate via `_run_sync()` (#218)

- **Bond analytics module** (`xbbg.ext.bonds`): 6 new functions for fixed income analytics -- `bond_info` (reference metadata and ratings), `bond_risk` (duration, convexity, DV01), `bond_spreads` (OAS, Z-spread, I-spread, ASW), `bond_cashflows` (cash flow schedule), `bond_key_rates` (key rate durations and risks), `bond_curve` (multi-bond relative value comparison)

- **Options analytics module** (`xbbg.ext.options`): 6 new functions and 5 enums for equity option analytics -- `option_info` (contract metadata), `option_greeks` (Greeks and implied volatility), `option_pricing` (value decomposition and activity), `option_chain` (chain via `CHAIN_TICKERS` with overrides), `option_chain_bql` (chain via BQL with rich filtering), `option_screen` (multi-option comparison). Enums: `PutCall`, `ChainPeriodicity`, `StrikeRef`, `ExerciseType`, `ExpiryMatch`

- **CDX analytics** (`xbbg.ext.cdx`): 8 new functions for credit default swap index analytics -- `cdx_info`, `cdx_defaults`, `cdx_pricing`, `cdx_risk`, `cdx_basis`, `cdx_default_prob`, `cdx_cashflows`, `cdx_curve`. `cdx_pricing`/`cdx_risk` support `CDS_RR` recovery rate override

- **`YieldType` expanded**: Added `YTW` (Yield to Worst), `YTP` (Yield to Put), `CFY` (Cash Flow Yield) to `YieldType` enum

- **`workout_dt` parameter for `yas()`**: Workout date for yield-to-worst/call calculations, maps to `YAS_WORKOUT_DT` Bloomberg override. Accepts `str` (YYYYMMDD) or `datetime`

- **`tz` parameter for `bdib()`/`abdib()`**: Controls output timezone for intraday bar data. Defaults to `None` (exchange local timezone, matching v0.7.x behavior). Set `tz='UTC'` to keep UTC timestamps, or pass any IANA timezone string (e.g., `'Europe/London'`)

- **`exchange_tz()` helper**: Returns the IANA timezone string for any Bloomberg ticker (e.g., `blp.exchange_tz('AAPL US Equity')` -> `'America/New_York'`). Exported via `blp.exchange_tz()`

- **LONG_TYPED output format**: New `_to_long_typed()` function produces typed value columns (`value_f64`, `value_i64`, `value_str`, `value_bool`, `value_date`, `value_ts`) with exactly one populated per row based on the Arrow type of each field

- **LONG_WITH_METADATA output format**: New `_to_long_with_metadata()` function produces `(ticker, date, field, value, dtype)` where `value` is stringified and `dtype` contains the Arrow type name (e.g. `double`, `int64`, `string`)

- **CI non-ASCII source check**: New `auto_ci.yml` step rejects non-ASCII characters in Python source files (allows CJK for ticker tests)

- **Comprehensive test coverage**: 55+ new tests including bond analytics (7), CDX analytics (8), options analytics, timezone conversion (13), `ovrds` dict normalization (7), `_events_to_table()` (16), `bdtick` format variants (5), mixed-type BDP (2), and output format tests (12)

### Changed

- **Unified I/O layer**: All Bloomberg requests now flow through a single `arequest()` async entry point in `conn.py`, replacing scattered session/service management across modules (#218)

- **Futures resolution uses `FUT_CHAIN_LAST_TRADE_DATES`** (#223): Replaced manual candidate generation (`FUT_GEN_MONTH` + batch `bdp`) with Bloomberg-native `FUT_CHAIN_LAST_TRADE_DATES` via single `bds()` call. ~2x faster (0.25-0.30s vs 0.53-0.72s)

- **`sync_api` decorator**: Replaces 13 hand-written sync wrappers across API modules (`screening.py`, `historical.py`, `intraday.py`, etc.) with a single `sync_api(async_fn)` call

- **Table-driven deprecation wrappers**: 23 manual wrapper functions in `blp.py` replaced by dict + loop pattern; 24 `warn_*` functions in `deprecation.py` replaced by `_DEPRECATION_REGISTRY` + `get_warn_func()` lookup

- **Market session rules extracted to TOML** (`markets/config/sessions.toml`): All MIC and exchange code rules moved from `sessions.py` into data-driven TOML config, reducing `sessions.py` from 364 to 168 lines (54% reduction)

- **Pipeline factory registry** (`pipeline_factories.py`): Centralized factory dispatch replaces scattered conditionals

- **CDX ticker format corrected**: Version is now a separate space-delimited token (e.g., `CDX HY CDSI S45 V2 5Y Corp` instead of `S45V2`)

- **`tomli` conditional dependency added**: `tomli>=2.0.1` for Python < 3.11 (TOML parsing for `sessions.toml`)

- **Net reduction of ~1,346 lines** across 27 files from codegen and table-driven optimizations

### Removed

- **`xbbg/io/db.py`**: SQLite database helper module (zero imports across codebase) (#218)

- **`xbbg/io/param.py`**: Legacy parameter/configuration module (zero imports across codebase) (#218)

- **`xbbg/io/files.py`**: File path utility module (zero imports after replacing 6 usages in `cache.py` and `const.py` with `pathlib.Path`) (#218)

- **`regression_testing/`**: Standalone v0.7.7 regression test directory; all scenarios covered by `test_live_endpoints.py` (#218)

- **`MONTH_CODE_MAP` and futures candidate generation helpers**: Superseded by `FUT_CHAIN_LAST_TRADE_DATES` chain resolution (#223)

- Stale files: `pmc_cache.json`, `xone.db`, empty `__init__` files, `test_param.py` (#218)

### Fixed

- **`bdtick` format parameter was completely non-functional**: All five output formats (LONG, SEMI_LONG, WIDE, LONG_TYPED, LONG_WITH_METADATA) were broken due to MultiIndex column wrapping, killed index name, and mixed-type Arrow conversion errors

- **`bdib` timezone regression**: The Arrow pipeline rewrite (v0.11.0) dropped the UTC-to-exchange local timezone conversion that existed in v0.7.x. Restored with configurable `tz` parameter

- **`ArrowInvalid` on multi-field BDP calls**: Bloomberg returns different Python types for different fields. New `_events_to_table()` builds Arrow tables with automatic type coercion fallback (#219)

- **`create_request` crashed when `ovrds` passed as dict**: Now normalizes dict to list of tuples before iteration ([SO#79880156](https://stackoverflow.com/questions/79880156))

- **Case-sensitive `backend` and `format` parameters**: Added `_missing_` classmethod to `Backend` and `Format` enums for case-insensitive lookup (#221)

- **Mock session leak in tests**: Added autouse `_reset_session_manager` fixture to prevent `MagicMock` persistence across test modules (#213)

- **`interval` parameter leaked as Bloomberg override**: Added to `PRSV_COLS` so it stays local (#145)

- **`StrEnum` Python 3.10 compatibility**: Added polyfill for Python < 3.11

- **Non-ASCII characters in source**: Replaced with ASCII equivalents for CI compliance

### Security

- **Bump `cryptography` from 46.0.4 to 46.0.5**: Fixes CVE-2026-26007 (#217)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.4...v0.12.0


*0.12.0b3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.12.0b3>`__

### Added

- **Bond analytics module** (`xbbg.ext.bonds`): 6 new functions for fixed income analytics -- `bond_info` (reference metadata and ratings), `bond_risk` (duration, convexity, DV01), `bond_spreads` (OAS, Z-spread, I-spread, ASW), `bond_cashflows` (cash flow schedule), `bond_key_rates` (key rate durations and risks), `bond_curve` (multi-bond relative value comparison)

- **Options analytics module** (`xbbg.ext.options`): 6 new functions and 5 enums for equity option analytics -- `option_info` (contract metadata), `option_greeks` (Greeks and implied volatility), `option_pricing` (value decomposition and activity), `option_chain` (chain via `CHAIN_TICKERS` with overrides), `option_chain_bql` (chain via BQL with rich filtering), `option_screen` (multi-option comparison). Enums: `PutCall`, `ChainPeriodicity`, `StrikeRef`, `ExerciseType`, `ExpiryMatch`

- **CDX analytics** (`xbbg.ext.cdx`): 8 new functions for credit default swap index analytics -- `cdx_info`, `cdx_defaults`, `cdx_pricing`, `cdx_risk`, `cdx_basis`, `cdx_default_prob`, `cdx_cashflows`, `cdx_curve`. `cdx_pricing`/`cdx_risk` support `CDS_RR` recovery rate override

- **`YieldType` expanded**: Added `YTW` (Yield to Worst), `YTP` (Yield to Put), `CFY` (Cash Flow Yield) to `YieldType` enum

- **`workout_dt` parameter for `yas()`**: Workout date for yield-to-worst/call calculations, maps to `YAS_WORKOUT_DT` Bloomberg override. Accepts `str` (YYYYMMDD) or `datetime`

- **`tz` parameter for `bdib()`/`abdib()`**: Controls output timezone for intraday bar data. Defaults to `None` (exchange local timezone, matching v0.7.x behavior). Set `tz='UTC'` to keep UTC timestamps, or pass any IANA timezone string (e.g., `'Europe/London'`)

- **`exchange_tz()` helper**: Returns the IANA timezone string for any Bloomberg ticker (e.g., `blp.exchange_tz('AAPL US Equity')` -> `'America/New_York'`). Exported via `blp.exchange_tz()`

- **`tz` field on `DataRequest` and `RequestBuilder`**: Propagates timezone control through the pipeline. `RequestBuilder` gains `.tz()` builder method

- **CI non-ASCII source check**: New `auto_ci.yml` step rejects non-ASCII characters in Python source files (allows CJK for ticker tests)

- **Live endpoint tests**: 7 tests for bond analytics, 8 tests for CDX analytics, plus options analytics coverage in `test_live_endpoints.py`

- **13 unit tests for timezone conversion** (`test_intraday_timezone.py`): Covers default exchange tz, explicit UTC, explicit timezone, Japanese equities, empty exchange info, empty tables, column renaming, and DataRequest/RequestBuilder propagation

- **7 regression tests for `ovrds` dict normalization** (`test_overrides.py`): Covers dict crash, correct element setting, multiple overrides, list-of-tuples backward compat, and None/empty edge cases

### Changed

- **Futures resolution uses `FUT_CHAIN_LAST_TRADE_DATES`** (#223): Replaced manual candidate generation (`FUT_GEN_MONTH` + batch `bdp`) with Bloomberg-native `FUT_CHAIN_LAST_TRADE_DATES` via single `bds()` call. ~2x faster (0.25-0.30s vs 0.53-0.72s). Removed `MONTH_CODE_MAP`, `_get_cycle_months`, `_construct_contract_ticker`

- **`sync_api` decorator**: Replaces 13 hand-written sync wrappers across API modules (`screening.py`, `historical.py`, `intraday.py`, etc.) with a single `sync_api(async_fn)` call

- **Table-driven deprecation wrappers**: 23 manual wrapper functions in `blp.py` replaced by dict + loop pattern; 24 `warn_*` functions in `deprecation.py` replaced by `_DEPRECATION_REGISTRY` + `get_warn_func()` lookup

- **Market session rules extracted to TOML** (`markets/config/sessions.toml`): All MIC and exchange code rules moved from `sessions.py` into data-driven TOML config, reducing `sessions.py` from 364 to 168 lines (54% reduction)

- **Pipeline factory registry** (`pipeline_factories.py`): Centralized factory dispatch replaces scattered conditionals

- **Wildcard imports in `__init__.py` files**: 9 `__init__.py` files simplified to use wildcard imports with explicit `__all__` lists

- **CDX ticker format corrected**: Version is now a separate space-delimited token (e.g., `CDX HY CDSI S45 V2 5Y Corp` instead of `S45V2`)

- **`tomli` conditional dependency added**: `tomli>=2.0.1` for Python < 3.11 (TOML parsing for `sessions.toml`)

- **Net reduction of ~1,346 lines** across 27 files from codegen and table-driven optimizations

### Fixed

- **`bdib` timezone regression**: The Arrow pipeline rewrite (v0.11.0) dropped the UTC-to-exchange local timezone conversion that existed in v0.7.x. Intraday bar timestamps were returned in UTC instead of exchange local time. Restored the conversion in `IntradayTransformer.transform()` with configurable `tz` parameter

- **`create_request` crashed when `ovrds` passed as dict**: `create_request(ovrds={"PRICING_SOURCE": "BGN"})` raised `ValueError: too many values to unpack` because iterating a dict yields keys (strings), not (key, value) tuples. Now normalizes dict to list of tuples before iteration. Also updated type annotation to accept `dict[str, Any]` ([SO#79880156](https://stackoverflow.com/questions/79880156))

- **Case-sensitive `backend` and `format` parameters**: `Backend("POLARS")` and `Format("WIDE")` raised `ValueError` because enum values are lowercase. Added `_missing_` classmethod to both `Backend` and `Format` enums for case-insensitive lookup (#221)

- **`StrEnum` Python 3.10 compatibility**: Added `StrEnum` polyfill in options module for Python < 3.11 where `enum.StrEnum` does not exist

- **Python 3.10 mock patching**: Fixed `patch.object()` usage for Python 3.10 compatible mock patching in tests by exposing submodules and patching at source

- **Non-ASCII characters in source**: Replaced checkmarks, em dashes, and arrows with ASCII equivalents across the codebase for CI compliance

- **Ruff lint errors**: Fixed import sorting (I001) and docstring formatting issues

### Removed

- **`update_readme_on_release.yml` workflow**: Inline changelog in README replaced by link to `CHANGELOG.md`

- **`MONTH_CODE_MAP` and futures candidate generation helpers**: Superseded by `FUT_CHAIN_LAST_TRADE_DATES` chain resolution (#223)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.4...v0.12.0b3


*0.12.0b2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.12.0b2>`__

### Fixed

- **`ArrowInvalid` on multi-field BDP calls**: Bloomberg returns different Python types for different fields (e.g., `float` for `FUT_CONT_SIZE`, `str` for `FUT_VAL_PT`). When both land in the same Arrow value column, `pa.array()` raised `ArrowInvalid`. New `_events_to_table()` builds Arrow tables directly from event dicts with automatic type coercion fallback — stringify on `ArrowInvalid`/`ArrowTypeError`, preserving nulls (#219)

- **Post-transform `pa.Table.from_pandas()` mixed-type failure**: Protected the secondary Arrow conversion (after narwhals transform) with the same stringify fallback for object columns (#219)

### Added

- **16 unit tests for `_events_to_table()`** (`test_events_to_table.py`): covers basic contract, mixed-type columns (float+str, int+str, float+date, kitchen sink), null handling, non-uniform dict keys, and pipeline integration (#219)

- **2 live regression tests for mixed-type BDP** (`test_live_endpoints.py`): `test_bdp_mixed_type_fields` and `test_bdp_mixed_type_multiple_tickers` exercise the exact bug scenario with `ES1 Index` / `NQ1 Index` using `FUT_CONT_SIZE` + `FUT_VAL_PT` (#219)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.4...v0.12.0b2


*0.12.0b1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.12.0b1>`__

### Changed

- **Async-first architecture**: All Bloomberg API functions (`bdp`, `bds`, `bdh`, `bdib`, `bdtick`, `bql`, `beqs`, `bsrch`, `bqr`, `bta`) now have async counterparts (`abdp`, `abds`, `abdh`, etc.) as the source of truth; sync wrappers delegate via `_run_sync()` (#218)

- **Unified I/O layer**: All Bloomberg requests now flow through a single `arequest()` async entry point in `conn.py`, replacing scattered session/service management across modules (#218)

- **Pipeline and process modules**: Adapted `pipeline_core`, `process`, and `request_builder` to work with the async `arequest()` foundation (#218)

- **Top-level async exports**: All async API variants (`abdp`, `abds`, `abdh`, `abdib`, `abdtick`, `abql`, `abeqs`, `absrch`, `abqr`, `abta`) exported from `xbbg.blp` (#218)

- **IO module cleanup**: Removed dead code and fixed type annotations across `xbbg/io/` (#218)

- **Test coverage expanded**: 571 tests total (up from 543), covering all connection-related GitHub issues and all previously untested paths in `conn.py`

### Fixed

- **Mock session leak in tests**: Added autouse `_reset_session_manager` fixture in `conftest.py` to prevent `MagicMock` sessions from persisting in the `SessionManager` singleton across test modules, which caused infinite `__getattr__` → `_get_child_mock` recursion and stack overflow on Windows (#213)

- **`interval` parameter leaked as Bloomberg override**: `interval` was not in `PRSV_COLS`, causing it to be sent to Bloomberg as an override field instead of being used locally for bar sizing (#145)

- **README Data Storage section**: Clarified that only `bdib()` (intraday bars) has caching via `BarCacheAdapter`; all other functions always make live Bloomberg API calls (#215)

- **README async example for Jupyter**: Fixed `asyncio.run()` example that fails in notebooks (which already have a running event loop) by adding `await`-based and `nest_asyncio` alternatives (#216)

- **Unused imports in tests**: Removed `import os` from `test_intraday_api.py` and `import pytest` from `test_logging.py` that caused Ruff F401 lint failures in CI

### Removed

- **`xbbg/io/db.py`**: SQLite database helper module (zero imports across codebase) (#218)

- **`xbbg/io/param.py`**: Legacy parameter/configuration module (zero imports across codebase) (#218)

- **`xbbg/io/files.py`**: File path utility module (zero imports after replacing 6 usages in `cache.py` and `const.py` with `pathlib.Path`) (#218)

- **`xbbg/tests/test_param.py`**: Tests for deleted `param` module (7 tests) (#218)

- **`xbbg/markets/cached/pmc_cache.json`**: Stale pandas-market-calendars cache file (pmc dependency removed in v0.11.0) (#218)

- **`xbbg/tests/__init__.py`**, **`examples/feeds/__init__.py`**: Empty `__init__` files (#218)

- **`xbbg/tests/xone.db`**: Stale SQLite test database (#218)

- **`regression_testing/`**: Standalone v0.7.7 regression test directory (6 files); all 9 test scenarios already covered by `xbbg/tests/test_live_endpoints.py` with stricter assertions (#218)

### Security

- **Bump `cryptography` from 46.0.4 to 46.0.5**: Fixes CVE-2026-26007 — subgroup attack due to missing validation for SECT binary elliptic curves (#217)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.4...v0.12.0b1


*0.11.4* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.4>`__

### Fixed

- **`bdtick` Arrow conversion failure**: Object columns containing `blpapi.Name` instances caused `pa.Table.from_pandas()` to fail; now stringified before conversion

- **`adjust_ccy` field name mismatch**: Looked for `"Last_Price"` but `bdh` returns lowercase `"last_price"` since v0.11.1, causing `KeyError`

- **`active_futures` two failures**: Used `nw.coalesce()` with a column (`last_tradeable_dt`) not present in SEMI_LONG format, and called `.height` (not valid on narwhals DataFrame) instead of `.shape[0]`

- **Live test assertions**: Updated 10 tests in `test_live_endpoints.py` to match WIDE format default (active since v0.7.x)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.3...v0.11.4


*0.11.3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.3>`__

### Fixed

- **Duplicate `port` keyword argument**: `bbg_service()` and `bbg_session()` used `.get()` to extract `port` then forwarded `**kwargs` still containing it, causing `TypeError: got multiple values for keyword argument 'port'` on non-default ports (e.g., B-Pipe connections) (#212)

- **Session resource leak**: `clear_default_session()` set `_default_session = None` without calling `session.stop()`, leaking OS file descriptors over repeated connect/disconnect cycles (#211)

- **Wrong session removed on retry**: `send_request()` retry path called `remove_session(port=port)` without `server_host`, always targeting `//localhost:{port}` even for remote hosts

- **Inconsistent `server_host` extraction**: `get_session()` / `get_service()` checked `server_host` before `server`, but `connect_bbg()` did the opposite, causing different code paths to resolve different hosts when both keys were present

- **Resource leak on start failure**: `connect_bbg()` did not stop the session before raising `ConnectionError` when `.start()` failed, leaking C++ resources allocated by the `Session()` constructor

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.2...v0.11.3


*0.11.2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.2>`__

### Added

- **Extended multi-backend support**: Added 6 new backends matching narwhals' full backend support:
  - **Eager backends**: `cudf` (GPU-accelerated via NVIDIA RAPIDS), `modin` (distributed pandas)
  - **Lazy backends**: `dask` (parallel computing), `ibis` (portable DataFrame expressions), `pyspark` (Apache Spark), `sqlframe` (SQL-based DataFrames)
  - Total: 13 backends (6 eager + 7 lazy)

- **Backend availability checking**: New functions to check and validate backend availability with helpful error messages:
  - `is_backend_available(backend)` - Check if a backend package is installed
  - `check_backend(backend)` - Check availability with version validation, raises helpful errors
  - `get_available_backends()` - List all currently available backends
  - `print_backend_status()` - Diagnostic function showing all backend statuses

- **Format compatibility checking**: New functions to validate format support per backend:
  - `is_format_supported(backend, format)` - Check if a format works with a backend
  - `get_supported_formats(backend)` - Get set of supported formats for a backend
  - `check_format_compatibility(backend, format)` - Validate with helpful errors
  - `validate_backend_format(backend, format)` - Combined validation for API functions

- **`xbbg.ext` module**: New extension module for v1.0 migration containing helper functions that will be removed from `blp` namespace
  - `xbbg.ext.currency` - `adjust_ccy()` for currency conversion
  - `xbbg.ext.dividends` - `dividend()` for dividend history
  - `xbbg.ext.earnings` - `earning()` for earnings breakdowns
  - `xbbg.ext.turnover` - `turnover()` for trading volume
  - `xbbg.ext.holdings` - `etf_holdings()`, `preferreds()`, `corporate_bonds()` BQL helpers
  - `xbbg.ext.futures` - `fut_ticker()`, `active_futures()` for futures resolution
  - `xbbg.ext.cdx` - `cdx_ticker()`, `active_cdx()` for CDX index resolution
  - `xbbg.ext.yas` - `yas()`, `YieldType` for fixed income analytics

- New v1.0-compatible import path: `from xbbg.ext import dividend, fut_ticker, ...` (no deprecation warnings)

- **Pandas removed as required dependency**: `xbbg.ext` modules now use only stdlib datetime and narwhals, making pandas fully optional

### Changed

- **Backend enum reorganized**: Backends now categorized as eager (full API) vs lazy (deferred execution)

- **Format restrictions**: WIDE format only available for eager backends (pandas, polars, pyarrow, narwhals, cudf, modin); lazy backends limited to LONG and SEMI_LONG

- **Version requirements updated**: Minimum versions now match narwhals requirements (duckdb>=1.0, dask>=2024.1)

- `xbbg/markets/resolvers.py` now re-exports from `xbbg.ext.futures` and `xbbg.ext.cdx` for backwards compatibility

- Internal implementations moved to `xbbg/ext/` module; old import paths still work with deprecation warnings

### Fixed

- **ibis backend**: Updated to use `ibis.memtable()` instead of deprecated `con.read_in_memory()`

- **sqlframe backend**: Fixed import path to use `sqlframe.duckdb.DuckDBSession`

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.1...v0.11.2


*0.11.1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.1>`__

### Fixed

- **Field names now lowercase**: Restored v0.10.x behavior where `bdp()`, `bdh()`, and `bds()` return field/column names as lowercase (#206)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.0...v0.11.1


*0.11.0* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0>`__

### Highlights

- **Arrow-first pipeline**: Complete rewrite of internal data processing using PyArrow for improved performance

- **Multi-backend support**: Output to pandas, Polars, PyArrow, DuckDB, or narwhals with the new `Backend` enum

- **New API functions**: `bta()` for technical analysis, `bqr()` for dealer quotes, `yas()` for fixed income analytics

- **BQL helpers**: `preferreds()` and `corporate_bonds()` convenience functions

- **Dependency cleanup**: Removed pandas-market-calendars and the trials mechanism for a leaner install

- **v1.0 migration path**: Deprecation warnings and forward-compatible APIs prepare for the upcoming v1.0 release

### Added

- **Arrow-first pipeline**: Complete rewrite of internal data processing using PyArrow

- **Multi-backend support**: New `Backend` enum supporting narwhals, pandas, polars, polars_lazy, pyarrow, duckdb

- **Output format control**: New `Format` enum with long, semi_long, wide options

- **bta()**: Bloomberg Technical Analysis function for 50+ technical indicators (#175)

- **bqr()**: Bloomberg Quote Request function emulating Excel `=BQR()` for dealer quote data with broker attribution (#22)

- **yas()**: Bloomberg YAS (Yield Analysis) wrapper for fixed income analytics with `YieldType` enum

- **preferreds()**: BQL convenience function to find preferred stocks for an equity ticker

- **corporate_bonds()**: BQL convenience function to find active corporate bonds for a ticker

- `set_backend()`, `get_backend()`, `set_format()`, `get_format()` configuration functions

- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`

- v1.0-compatible exception classes (`BlpError`, `BlpSessionError`, `BlpRequestError`, etc.)

- `EngineConfig` dataclass and `configure()` function for engine configuration

- `Service` and `Operation` enums for Bloomberg service URIs

- Treasury & SOFR futures support: TY, ZN, ZB, ZF, ZT, UB, TN, SFR, SR1, SR3, ED futures (#198)

- Comprehensive logging improvements across critical paths with better error traceability

- CONTRIBUTING.md and CODE_OF_CONDUCT.md for community standards

### Changed

- All API functions now accept `backend` and `format` parameters

- Internal pipeline uses PyArrow tables with narwhals transformations

- Removed pytz dependency (using stdlib `datetime.timezone`)

- **Intraday cache now includes interval in path** (#80) - different bar intervals cached separately (**breaking**: existing cache will miss)

- Internal class renames with backward compatible aliases (`YamlMarketInfoProvider` → `MetadataProvider`)

- Logging level adjustments: `BBG_ROOT not set` promoted to WARNING, cache timing demoted to DEBUG

### Deprecated

- `connect()` / `disconnect()` - engine auto-initializes in v1.0

- `getBlpapiVersion()` - use `get_sdk_info()` instead

- `lookupSecurity()` - will become `blkp()` in v1.0

- `fieldInfo()` / `fieldSearch()` - will merge into `bfld()` in v1.0

- `bta_studies()` - renamed to `ta_studies()` in v1.0

- `getPortfolio()` - renamed to `bport()` in v1.0

- Helper functions (`dividend()`, `earning()`, `turnover()`, `adjust_ccy()`) moving to `xbbg.ext` in v1.0

- Futures/CDX utilities (`fut_ticker()`, `active_futures()`, `cdx_ticker()`, `active_cdx()`) moving to `xbbg.ext` in v1.0

### Removed

- **Trials mechanism**: Eliminated retry-blocking system that caused silent failures after 2 failed attempts

- **pandas-market-calendars dependency**: Exchange info now sourced exclusively from Bloomberg API with local caching

### Fixed

- **Import without blpapi installed**: Fixed `AttributeError` when importing xbbg without blpapi (#200)

- **Japan/non-US timezone fix for bdib**: Trading hours now correctly converted to exchange's local timezone (#198)

- **stream() field values**: Subscribed field values now always included in output dict (#199)

- **Slow Bloomberg fields**: TIMEOUT events handled correctly; requests wait for response with `slow_warn_seconds` warning (#193)

- **Pipeline data types**: Preserve original data types instead of converting to strings (#191)

- **Futures symbol parsing**: Fixed `market_info()` to correctly parse symbols like `TYH6` → `TY` (#198)

- **get_tz() optimization**: Direct timezone strings recognized without Bloomberg API call

- **bdtick timezone fix**: Pass exchange timezone to fix blank results for non-UTC exchanges (#185)

- **bdtick timeout**: Increased from 10s to 2 minutes for tick data requests

- Extended BDS test date range to 120 days for quarterly dividends

- Helper functions now work correctly with LONG format output

- Logging format compliance fixes (G004, G201)

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.3...v0.11.0


*0.11.0b5* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0b5>`__

### Fixed

- **Import without blpapi installed**: Fixed `AttributeError` when importing xbbg without blpapi installed (#200)
  - Added `from __future__ import annotations` to defer type annotation evaluation in `conn.py`
  - Guarded `blpapi.Name()` constants with `is_available()` check in `process.py`

- **Japan/non-US timezone fix for bdib**: Fixed timezone conversion for non-US exchanges (#198)
  - Bloomberg returns `TRADING_DAY_START_TIME_EOD` and `TRADING_DAY_END_TIME_EOD` in EST (America/New_York)
  - These are now correctly converted to the exchange's local timezone (e.g., Asia/Tokyo for Japanese equities)
  - Previously, Tokyo's 09:00-15:45 trading hours appeared as 19:00-01:45 (EST times misinterpreted as local)
  - `FUT_TRADING_HRS` is not converted (already in exchange local time)

- **get_tz() no longer triggers Bloomberg lookup for timezone strings**: Direct timezone strings like `"America/New_York"` or `"UTC"` are now recognized without calling Bloomberg API

### Removed

- **Trials mechanism removed**: Eliminated the retry-blocking system that caused silent failures
  - The trials system tracked failed API requests and blocked future attempts after 2 failures
  - This caused issues when bugs were fixed (stale entries still blocked requests)
  - Users had to manually clear SQLite database entries to retry
  - Pipeline caching already handles "don't re-fetch" use case properly
  - Deleted `xbbg/core/utils/trials.py` and related test files

- **pandas-market-calendars dependency removed**: Simplified exchange metadata resolution
  - Removed `PmcCalendarResolver` from resolver chain
  - Exchange information now sourced exclusively from Bloomberg API with local caching
  - Removed `pmc_extended` context option (no longer needed)
  - Reduces package dependencies and improves consistency

### Changed

- **Internal class renames** (backward compatible aliases provided):
  - `YamlMarketInfoProvider` → `MetadataProvider`
  - `ExchangeYamlResolver` → `ExchangeMetadataResolver`

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.3...v0.11.0b5


*0.11.0b3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0b3>`__

## v0.11.0b3 - Beta Release

### Highlights

- **New BQR function**: Bloomberg Quote Request for dealer quote data with broker attribution

- **Timeout handling fix**: Slow Bloomberg fields no longer timeout prematurely

- **Pipeline improvements**: Preserve data types and backend/format attributes

### Added

- **bqr()**: New Bloomberg Quote Request function emulating Excel `=BQR()` for dealer quote data (#22)
  - Retrieves tick-level quotes with broker/dealer attribution from MSG1 pricing sources
  - Supports date offsets (`-2d`, `-1w`) and explicit date ranges
  - Includes broker codes (`broker_buy`, `broker_sell`) for dealer identification
  - Works with Bloomberg tickers and ISINs (e.g., `/isin/US037833BA77@MSG1`)
  - Full multi-backend support (pandas, polars, pyarrow, duckdb)

### Fixed

- **Slow Bloomberg fields no longer timeout prematurely** (#193)
  - Bloomberg TIMEOUT events are now handled correctly - they indicate the request is still processing, not an error
  - Removed `max_timeouts` limit that caused requests to fail after ~10 seconds
  - Added `slow_warn_seconds` parameter (default: 15s) to warn about slow requests without aborting them
  - Fields like `STOCHASTIC_OAS_MID_MOD_DUR` that take 10+ seconds now work correctly

- **Pipeline data types**: Preserve original data types in pipeline output instead of converting to strings (#191)

- **Backend/format attributes**: Preserve backend/format attributes in DataRequest pipeline helpers

This is a **beta release** continuing the v0.11 series with new features and bug fixes.

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.11.0b2...v0.11.0b3


*0.11.0b3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0b3>`__

v0.11.0b3

- bqr(): New Bloomberg Quote Request function emulating Excel =BQR() for dealer quote data with broker attribution (#22)

- Slow Bloomberg fields no longer timeout prematurely - TIMEOUT events handled correctly (#193)

- Pipeline data types: Preserve original data types in pipeline output instead of converting to strings (#191)


*0.11.0b2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0b2>`__

v0.11.0b2

- preferreds(): New BQL convenience function to find preferred stocks for an equity ticker

- corporate_bonds(): New BQL convenience function to find active corporate bonds for a ticker

- bdtick timezone fix: Pass exchange timezone to time_range() to fix blank results for non-UTC exchanges (#185)

- bdtick timeout defaults: Increase timeout from 10s to 2 minutes for tick data requests

- CI pre-release publishing: Fix workflow to include pre-releases when publishing via workflow_dispatch


*0.11.0b1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.11.0b1>`__

## v0.11.0b1 - Beta Release

### Highlights

- **Arrow-first pipeline**: Complete rewrite of data processing using PyArrow internally

- **Multi-backend support**: New Backend enum supporting narwhals, pandas, polars, polars_lazy, pyarrow, duckdb

- **Output format control**: New Format enum with long, semi_long, wide options

- **Bloomberg Technical Analysis (BTA)**: New `bta()` function for technical indicators

- **v1.0 migration infrastructure**: Deprecation warnings and forward-compatible APIs

### Added

- `Backend` and `Format` enums for output control

- `set_backend()`, `get_backend()`, `set_format()`, `get_format()` functions

- `bta()` function for Bloomberg Technical Analysis

- `get_sdk_info()` as replacement for `getBlpapiVersion()`

- v1.0-compatible exception classes (`BlpError`, `BlpSessionError`, etc.)

- `EngineConfig` dataclass and `configure()` function

- `Service` and `Operation` enums for Bloomberg services

### Changed

- All API functions now support `backend` and `format` parameters

- Internal pipeline uses PyArrow tables with narwhals transformations

- Removed pytz dependency (using stdlib datetime.timezone)

### Deprecated

- `connect()` / `disconnect()` - engine auto-initializes in v1.0

- `getBlpapiVersion()` - use `get_sdk_info()`

- `lookupSecurity()` - will become `blkp()` in v1.0

- `fieldInfo()` / `fieldSearch()` - will merge into `bfld()` in v1.0

This is a **beta release** for testing the new Arrow-first architecture before v1.0.

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.3...v0.11.0b1


*0.10.3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.10.3>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.2...v0.10.3


*0.10.2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.10.2>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.1...v0.10.2


*0.10.1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.10.1>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.10.0...v0.10.1

## What's Changed

- fix: persist blp.connect() session for subsequent API calls by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/165


*0.10.0* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.10.0>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.9.1...v0.10.0

## What's Changed

- Updated polars-bloomberg support for BQL, BDIB and BSRCH by @MarekOzana in https://github.com/alpha-xone/xbbg/pull/155

- fix: add identifier type prefix to B-Pipe subscription topics by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/156

- fix: remove pandas version cap to support Python 3.14 by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/161

- fix(docs): resolve RST formatting warning in index.rst by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/162

- fix: update Japan equity market hours for TSE trading extension by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/163

## New Contributors

- @MarekOzana made their first contribution in https://github.com/alpha-xone/xbbg/pull/155


*0.9.1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.9.1>`__

## What's Changed

- fix: Fix BQL returning only one row for multi-value results by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/152

- fix(docs): add blank lines around latest-release markers in index.rst

- ci: remove redundant release triggers from workflows

- ci: trigger release workflows explicitly from semantic_version

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.9.0...v0.9.1


*0.9.0* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.9.0>`__

## What's Changed

- feat: Add etf_holdings() function for retrieving ETF holdings via BQL by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/147

- feat: Add multi-day support to bdib() by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/148

- feat: Add multi-day cache support for bdib() by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/149

- fix: resolve RST duplicate link targets and Sphinx build warnings

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.8.2...v0.9.0


*0.8.2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.8.2>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.8.1...v0.8.2

## What's Changed

- Fix BQL options chain metadata issues by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/146

*0.8.1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.8.1>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.8.0...v0.8.1

*0.8.0* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.8.0>`__

**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.7.11...v0.8.0

## What's Changed
* Improved logging with blpapi integration and performance optimizations by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/135
* feat: add fixed income securities support to bdib by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/136
* feat: add interval parameter to subscribe() and live() functions by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/132
* fix(beqs): increase timeout and max_timeouts for BEQS requests by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/133
* feat: add bsrch() function for Bloomberg SRCH queries (Excel =@BSRCH equivalent) by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/137
* feat: add server host parameter support to connect_bbg() by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/138
* fix: remove 1-minute offset for bare session names in bdtick by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/139
* fix(issue-68): Add support for GY (Xetra), IM (Borsa Italiana), and SE (SIX) exchanges by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/140
* Fix BQL syntax documentation and error handling (Fixes #141) by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/142
* refactor: comprehensive codebase cleanup and restructuring by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/144


*0.7.10* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.10>`__

## What's Changed

* Migrate to uv + PEP 621; modernize CI and blpapi index by @contributor in https://github.com/alpha-xone/xbbg/pull/124



## New Contributors

* @contributor made their first contribution in https://github.com/alpha-xone/xbbg/pull/124



**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.7.9...v0.7.10

*0.7.11* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.11>`__

## What's Changed

* ci: use uv build in publish workflows by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/125

* docs: standardize docstrings (Google) + Ruff/napoleon config by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/127

* feat: BQL support + CI workflow improvements (uv venv) by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/128

* feat(bdib): add support for sub-minute intervals via intervalHasSeconds flag by @kj55-dev in https://github.com/alpha-xone/xbbg/pull/131



**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.7.10...v0.7.11

*0.7.9* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.9>`__

## What's Changed

* Fixing typo in TLS Options when creating a new connection by @rchiorean in https://github.com/alpha-xone/xbbg/pull/110

* Fixing Auto CI by @rchiorean in https://github.com/alpha-xone/xbbg/pull/111



## New Contributors

* @rchiorean made their first contribution in https://github.com/alpha-xone/xbbg/pull/110



**Full Changelog**: https://github.com/alpha-xone/xbbg/compare/v0.7.7...v0.7.9

*0.7.8a2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.8a2>`__

*0.7.7* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.7>`__

*0.7.7a4* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.7a4>`__

*0.7.7a3* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.7a3>`__

*0.7.7a2* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.7a2>`__

*0.7.7a1* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.7a1>`__

*0.7.6* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.6>`__

*0.7.6a8* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.6a8>`__

*0.7.6a7* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.6a7>`__

*0.7.6a6* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.6a6>`__

*0.7.6a5* - see release: `notes <https://github.com/alpha-xone/xbbg/releases/tag/v0.7.6a5>`__
.. xbbg:changelog-end

*0.7.2* - Use `async` for live data feeds

*0.7.0* - ``bdh`` preserves columns orders (both tickers and flds).
``timeout`` argument is available for all queries - ``bdtick`` usually takes longer to respond -
can use ``timeout=1000`` for example if keep getting empty DataFrame.

*0.6.6* - Add flexibility to use reference exchange as market hour definition
(so that it's not necessary to add ``.yml`` for new tickers, provided that the exchange was defined
in ``/xbbg/markets/exch.yml``). See example of ``bdib`` below for more details.

*0.6.0* - Speed improvements and tick data availablity

*0.5.0* - Rewritten library to add subscription, BEQS, simplify interface and remove dependency of `pdblp`

*0.1.22* - Remove PyYAML dependency due to security vulnerability

*0.1.17* - Add ``adjust`` argument in ``bdh`` for easier dividend / split adjustments

Contents
========

.. toctree::
   :maxdepth: 1

   docstring_style

Tutorial
========

.. code-block:: python

    In [1]: from xbbg import blp

Basics
------

``BDP`` example:

.. code-block:: python

    In [2]: blp.bdp(tickers='NVDA US Equity', flds=['Security_Name', 'GICS_Sector_Name'])
    Out[2]:
                   security_name        gics_sector_name
    NVDA US Equity   NVIDIA Corp  Information Technology

``BDP`` with overrides:

.. code-block:: python

    In [3]: blp.bdp('AAPL US Equity', 'Eqy_Weighted_Avg_Px', VWAP_Dt='20181224')
    Out[3]:
                    eqy_weighted_avg_px
    AAPL US Equity               148.75

``BDH`` example:

.. code-block:: python

    In [4]: blp.bdh(
       ...:     tickers='SPX Index', flds=['High', 'Low', 'Last_Price'],
       ...:     start_date='2018-10-10', end_date='2018-10-20',
       ...: )
    Out[4]:
               SPX Index
                    High      Low Last_Price
    2018-10-10  2,874.02 2,784.86   2,785.68
    2018-10-11  2,795.14 2,710.51   2,728.37
    2018-10-12  2,775.77 2,729.44   2,767.13
    2018-10-15  2,775.99 2,749.03   2,750.79
    2018-10-16  2,813.46 2,766.91   2,809.92
    2018-10-17  2,816.94 2,781.81   2,809.21
    2018-10-18  2,806.04 2,755.18   2,768.78
    2018-10-19  2,797.77 2,760.27   2,767.78

``BDH`` example with Excel compatible inputs:

.. code-block:: python

    In [5]: blp.bdh(
       ...:     tickers='SHCOMP Index', flds=['High', 'Low', 'Last_Price'],
       ...:     start_date='2018-09-26', end_date='2018-10-20',
       ...:     Per='W', Fill='P', Days='A',
       ...: )
    Out[5]:
               SHCOMP Index
                       High      Low Last_Price
    2018-09-28     2,827.34 2,771.16   2,821.35
    2018-10-05     2,827.34 2,771.16   2,821.35
    2018-10-12     2,771.94 2,536.66   2,606.91
    2018-10-19     2,611.97 2,449.20   2,550.47

``BDH`` without adjustment for dividends and splits:

.. code-block:: python

    In [6]: blp.bdh(
       ...:     'AAPL US Equity', 'Px_Last', '20140605', '20140610',
       ...:     CshAdjNormal=False, CshAdjAbnormal=False, CapChg=False
       ...: )
    Out[6]:
               AAPL US Equity
                      Px_Last
    2014-06-05         647.35
    2014-06-06         645.57
    2014-06-09          93.70
    2014-06-10          94.25

``BDH`` adjusted for dividends and splits:

.. code-block:: python

    In [7]: blp.bdh(
       ...:     'AAPL US Equity', 'Px_Last', '20140605', '20140610',
       ...:     CshAdjNormal=True, CshAdjAbnormal=True, CapChg=True
       ...: )
    Out[7]:
               AAPL US Equity
                      Px_Last
    2014-06-05          85.45
    2014-06-06          85.22
    2014-06-09          86.58
    2014-06-10          87.09

``BDS`` example:

.. code-block:: python

    In [8]: blp.bds('AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101', DVD_End_Dt='20180531')
    Out[8]:
                   declared_date     ex_date record_date payable_date  dividend_amount dividend_frequency dividend_type
    AAPL US Equity    2018-05-01  2018-05-11  2018-05-14   2018-05-17             0.73            Quarter  Regular Cash
    AAPL US Equity    2018-02-01  2018-02-09  2018-02-12   2018-02-15             0.63            Quarter  Regular Cash

Intraday bars ``BDIB`` example:

.. code-block:: python

    In [9]: blp.bdib(ticker='BHP AU Equity', dt='2018-10-17').tail()
    Out[9]:
                              BHP AU Equity
                                       open  high   low close   volume num_trds
    2018-10-17 15:56:00+11:00         33.62 33.65 33.62 33.64    16660      126
    2018-10-17 15:57:00+11:00         33.65 33.65 33.63 33.64    13875      156
    2018-10-17 15:58:00+11:00         33.64 33.65 33.62 33.63    16244      159
    2018-10-17 15:59:00+11:00         33.63 33.63 33.61 33.62    16507      167
    2018-10-17 16:10:00+11:00         33.66 33.66 33.66 33.66  1115523      216

Above example works because 1) ``AU`` in equity ticker is mapped to ``EquityAustralia`` in
``markets/assets.yml``, and 2) ``EquityAustralia`` is defined in ``markets/exch.yml``.
To add new mappings, define ``BBG_ROOT`` in sys path and add ``assets.yml`` and
``exch.yml`` under ``BBG_ROOT/markets``.

*New in 0.6.6* - if exchange is defined in ``/xbbg/markets/exch.yml``, can use ``ref`` to look for
relevant exchange market hours. Both ``ref='ES1 Index'`` and ``ref='CME'`` work for this example:

.. code-block:: python

    In [10]: blp.bdib(ticker='ESM0 Index', dt='2020-03-20', ref='ES1 Index').tail()
    out[10]:
                              ESM0 Index
                                    open     high      low    close volume num_trds        value
    2020-03-20 16:55:00-04:00   2,260.75 2,262.25 2,260.50 2,262.00    412      157   931,767.00
    2020-03-20 16:56:00-04:00   2,262.25 2,267.00 2,261.50 2,266.75    812      209 1,838,823.50
    2020-03-20 16:57:00-04:00   2,266.75 2,270.00 2,264.50 2,269.00   1136      340 2,576,590.25
    2020-03-20 16:58:00-04:00   2,269.25 2,269.50 2,261.25 2,265.75   1077      408 2,439,276.00
    2020-03-20 16:59:00-04:00   2,265.25 2,272.00 2,265.00 2,266.50   1271      378 2,882,978.25

Intraday bars within market session:

.. code-block:: python

    In [11]: blp.bdib(ticker='7974 JT Equity', dt='2018-10-17', session='am_open_30').tail()
    Out[11]:
                              7974 JT Equity
                                        open      high       low     close volume num_trds
    2018-10-17 09:27:00+09:00      39,970.00 40,020.00 39,970.00 39,990.00  10800       44
    2018-10-17 09:28:00+09:00      39,990.00 40,020.00 39,980.00 39,980.00   6300       33
    2018-10-17 09:29:00+09:00      39,970.00 40,000.00 39,960.00 39,970.00   3300       21
    2018-10-17 09:30:00+09:00      39,960.00 40,010.00 39,950.00 40,000.00   3100       19
    2018-10-17 09:31:00+09:00      39,990.00 40,000.00 39,980.00 39,990.00   2000       15

Corporate earnings:

.. code-block:: python

    In [12]: blp.earning('AMD US Equity', by='Geo', Eqy_Fund_Year=2017, Number_Of_Periods=1)
    Out[12]:
                     level    fy2017  fy2017_pct
    Asia-Pacific      1.00  3,540.00       66.43
        China         2.00  1,747.00       49.35
        Japan         2.00  1,242.00       35.08
        Singapore     2.00    551.00       15.56
    United States     1.00  1,364.00       25.60
    Europe            1.00    263.00        4.94
    Other Countries   1.00    162.00        3.04

Dividends:

.. code-block:: python

    In [13]: blp.dividend(['C US Equity', 'MS US Equity'], start_date='2018-01-01', end_date='2018-05-01')
    Out[13]:
                    dec_date     ex_date    rec_date    pay_date  dvd_amt dvd_freq      dvd_type
    C US Equity   2018-01-18  2018-02-02  2018-02-05  2018-02-23     0.32  Quarter  Regular Cash
    MS US Equity  2018-04-18  2018-04-27  2018-04-30  2018-05-15     0.25  Quarter  Regular Cash
    MS US Equity  2018-01-18  2018-01-30  2018-01-31  2018-02-15     0.25  Quarter  Regular Cash

-----

*New in 0.1.17* - Dividend adjustment can be simplified to one parameter ``adjust``:

- ``BDH`` without adjustment for dividends and splits:

.. code-block:: python

    In [14]: blp.bdh('AAPL US Equity', 'Px_Last', '20140606', '20140609', adjust='-')
    Out[14]:
               AAPL US Equity
                      Px_Last
    2014-06-06         645.57
    2014-06-09          93.70

- ``BDH`` adjusted for dividends and splits:

.. code-block:: python

    In [15]: blp.bdh('AAPL US Equity', 'Px_Last', '20140606', '20140609', adjust='all')
    Out[15]:
               AAPL US Equity
                      Px_Last
    2014-06-06          85.22
    2014-06-09          86.58

Data Storage
------------

If ``BBG_ROOT`` is provided in ``os.environ``, data can be saved locally in Parquet format.
By default, local storage is preferred over Bloomberg for all queries.

**Important**: Local data usage must be compliant with Bloomberg Datafeed Addendum
(full description in ``DAPI<GO>``):

    To access Bloomberg data via the API (and use that data in Microsoft Excel),
    your company must sign the 'Datafeed Addendum' to the Bloomberg Agreement.
    This legally binding contract describes the terms and conditions of your use
    of the data and information available via the API (the "Data").
    The most fundamental requirement regarding your use of Data is that it cannot
    leave the local PC you use to access the BLOOMBERG PROFESSIONAL service.

Development
===========

Setup
-----

Create venv and install dependencies:

.. code-block:: console

   uv venv .venv
   .\.venv\Scripts\Activate.ps1
   uv sync --locked --extra dev --extra test

Adding Dependencies
--------------------

.. code-block:: console

   uv add <package>

Running Tests and Linting
--------------------------

.. code-block:: console

   uv run ruff check xbbg
   uv run pytest --doctest-modules --cov -v xbbg

Building
--------

.. code-block:: console

   uv run python -m build

Publishing is handled via GitHub Actions using PyPI Trusted Publishing (OIDC).

Documentation
-------------

.. code-block:: console

   uv sync --locked --extra docs
   uv run sphinx-build -b html docs docs/_build/html

Contributing
============

- Issues and feature requests: please open an issue on the repository.
- Pull requests welcome. Run lint and tests locally:

.. code-block:: console

   uv sync --locked --extra dev --extra test
   uv run ruff check xbbg
   uv run pytest --doctest-modules -q

Links
=====

- `PyPI <https://pypi.org/project/xbbg/>`_
- `Documentation <https://xbbg.readthedocs.io/>`_
- `Source <https://github.com/alpha-xone/xbbg>`_
- Security policy: see ``SECURITY.md``

============== ======================
Docs           |docs|
Build          |actions|
Coverage       |codecov|
Quality        |codacy|
\              |codeFactor|
\              |codebeat|
License        |license|
============== ======================

.. |pypi| image:: https://img.shields.io/pypi/v/xbbg.svg
    :target: https://pypi.org/project/xbbg/
.. |version| image:: https://img.shields.io/pypi/pyversions/xbbg.svg
    :target: https://pypi.org/project/xbbg/
.. |actions| image:: https://github.com/alpha-xone/xbbg/workflows/Auto%20CI/badge.svg
    :target: https://github.com/alpha-xone/xbbg/actions
    :alt: Travis CI
.. |codecov| image:: https://codecov.io/gh/alpha-xone/xbbg/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/alpha-xone/xbbg
    :alt: Codecov
.. |docs| image:: https://readthedocs.org/projects/xbbg/badge/?version=latest
    :target: https://xbbg.readthedocs.io/
.. |codefactor| image:: https://www.codefactor.io/repository/github/alpha-xone/xbbg/badge
   :target: https://www.codefactor.io/repository/github/alpha-xone/xbbg
   :alt: CodeFactor
.. |codacy| image:: https://app.codacy.com/project/badge/Grade/daec9f52ba344e3ea116c15f1fc6d541
   :target: https://www.codacy.com/gh/alpha-xone/xbbg
.. |codebeat| image:: https://codebeat.co/badges/eef1f14d-72eb-445a-af53-12d3565385ec
   :target: https://codebeat.co/projects/github-com-alpha-xone-xbbg-main
.. |license| image:: https://img.shields.io/github/license/alpha-xone/xbbg.svg
    :alt: GitHub license
    :target: https://github.com/alpha-xone/xbbg/blob/main/LICENSE
.. |chat| image:: https://badges.gitter.im/xbbg/community.svg
   :target: https://gitter.im/xbbg/community
.. |download| image:: https://img.shields.io/pypi/dm/xbbg
   :target: https://pypistats.org/packages/xbbg
.. |coffee| image:: https://img.shields.io/badge/Buy%20Me%20a%20Coffee-1E3A8A?style=plastic&logo=buy-me-a-coffee&logoColor=white
   :target: https://www.buymeacoffee.com/Lntx29Oof
   :alt: Buy Me a Coffee
.. _Bloomberg API Library: https://www.bloomberg.com/professional/support/api-library/
