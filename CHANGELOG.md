# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Python 3.15 installs of `xbbg<1` resolve to the 0.x line, not 0.7.7**: `requires-python` drops its `<3.15` upper bound. pip skips every release whose Requires-Python excludes the running interpreter, so on 3.15 it passed over 0.7.10-0.12.3 and installed 0.7.7 (2022), the newest release without a cap. Tested versions are unchanged (3.10-3.14).

## [0.12.3] - 2026-04-20

### Added

- **1.0 release candidate notice**: Added a banner to README.md directing users to the 1.0 RC (`pip install xbbg --pre`).
- **Runtime deprecation warning**: Importing xbbg 0.x now emits a `FutureWarning` informing users that 1.0 is available. Shown once per process.
- **`process.apply_schema_elements()` helper**: Walks the live blpapi request schema (`Request.asElement().elementDefinition().typeDefinition().elementDefinitions()`) and dispatches caller kwargs to any element the service actually accepts, resolving aliases from `overrides.ELEM_KEYS` (e.g. `Points` → `maxDataPoints`, `CshAdjNormal` → `adjustmentNormal`) and enum values from `overrides.ELEM_VALS`. Replaces hand-maintained per-request-type whitelists — new Bloomberg schema elements become usable without a code change. Companion `process.request_schema_element_names()` exposes the raw element set (#295).

### Fixed

- **Release workflow**: Added `docs/index.rst` latest-release marker update to the inline release workflow. Previously dropped when the cross-branch `update_index_on_release.yml` was removed.
- **`maxDataPoints` / `Points` silently dropped on `bdib` and `bdtick`** (#295): The intraday builders forwarded element kwargs into `**kwargs` without applying them to the Bloomberg request. `bdtick(Points=1)`, `bdtick(maxDataPoints=1)`, and `bdib(maxDataPoints=1)` now correctly cap response size by setting the top-level element on `IntradayTickRequest` / `IntradayBarRequest` via schema-driven dispatch. Other intraday elements exposed by the schema (`maxDataPointsOrigin`, `gapFillInitialBar`, `adjustment*`, `filter`, `filters`, …) are forwarded the same way when the caller provides them. Backport of #295 to 0.x.
- **`ovrds={...}` on intraday requests now raises a clear error** (#295): `IntradayTickRequest` and `IntradayBarRequest` have no `overrides` sub-element on the Bloomberg schema, so passing overrides produced a cryptic `blpapi` "element not found" failure. `bdtick` / `bdib` / `IntradayRequestBuilder.build_request` now raise `ValueError` pointing users to `maxDataPoints` before hitting Bloomberg.
- **`corporate_bonds()` cross-market support**: The BQL query used `bondsuniv('active')` with a `TICKER=='{ticker}'` filter, which only matched plain US tickers and returned `"Request returned no results"` for non-US equity tickers (e.g. `9984 JT Equity` for SoftBank). Switched to the `debt(['{equity_ticker}'])` universe used by `preferreds()`, which lets Bloomberg resolve the company from the equity ticker and works across US / JP / EU / ... markets. Ticker input now accepts the bare symbol (`'AAPL'`), exchange-suffixed form (`'BAC US Equity'`), or any `"<ticker> <mkt> Equity"` form; bare inputs get a default `' US Equity'` suffix appended like `preferreds()` already did. Applies to both `xbbg.api.screening.corporate_bonds` and `xbbg.ext.holdings.corporate_bonds`. Backport of main #57e340f.
- **CDX generic-ticker on-the-run warning false positive**: `cdx_ticker()` only accepted `'Y'` from `ON_THE_RUN_CURRENT_BD_INDICATOR`, but Bloomberg returns `'true'`/`'false'` for CDX generic tickers (e.g. `CDX IG CDSI GEN 5Y Corp`) instead of the `'Y'`/`'N'` used for bonds and futures. The mismatch emitted a spurious "resolution may be stale" warning every time the CDX resolver was called. Both shapes are now accepted. Backport of main #b0860b6.

## [0.12.2] - 2026-03-18

### Fixed

- **blpapi 3.26.2 compatibility**: Session and service handle validation now works with both legacy blpapi (<= 3.26.1) and modern blpapi (>= 3.26.2) which refactored `Session`/`Service` to inherit from `CHandle`. Previously, `blp.connect()` sessions were silently discarded and subsequent API calls fell back to localhost, causing `ConnectionError` for remote SAPI connections (#249)

## [0.12.1] - 2026-02-19

### Fixed

- **PyArrow WIDE format crash**: Fix `ArrowTypeError` when pivoting intraday tick data with mixed types (float value + string typ/cond/exch columns) in non-pandas backends (#224)
- **Default format for non-pandas backends**: Auto-downgrade default output format from WIDE to SEMI_LONG for pyarrow/polars/narwhals backends, since WIDE requires pandas MultiIndex (#225)
- **WIDE downgrade warning**: Emit `XbbgFutureWarning` when WIDE format is silently downgraded to SEMI_LONG for non-pandas backends
- **CI non-ASCII check**: Replace non-ASCII em dashes in source comments to pass CI character validation

## [0.12.0] - 2026-02-18

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

## [0.12.0b3] - 2026-02-16

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

### Removed

- **`update_readme_on_release.yml` workflow**: Inline changelog in README replaced by link to `CHANGELOG.md`
- **`MONTH_CODE_MAP` and futures candidate generation helpers**: Superseded by `FUT_CHAIN_LAST_TRADE_DATES` chain resolution (#223)

### Fixed

- **`bdib` timezone regression**: The Arrow pipeline rewrite (v0.11.0) dropped the UTC-to-exchange local timezone conversion that existed in v0.7.x. Intraday bar timestamps were returned in UTC instead of exchange local time. Restored the conversion in `IntradayTransformer.transform()` with configurable `tz` parameter
- **`create_request` crashed when `ovrds` passed as dict**: `create_request(ovrds={"PRICING_SOURCE": "BGN"})` raised `ValueError: too many values to unpack` because iterating a dict yields keys (strings), not (key, value) tuples. Now normalizes dict to list of tuples before iteration. Also updated type annotation to accept `dict[str, Any]` ([SO#79880156](https://stackoverflow.com/questions/79880156))
- **Case-sensitive `backend` and `format` parameters**: `Backend("POLARS")` and `Format("WIDE")` raised `ValueError` because enum values are lowercase. Added `_missing_` classmethod to both `Backend` and `Format` enums for case-insensitive lookup (#221)
- **`StrEnum` Python 3.10 compatibility**: Added `StrEnum` polyfill in options module for Python < 3.11 where `enum.StrEnum` does not exist
- **Python 3.10 mock patching**: Fixed `patch.object()` usage for Python 3.10 compatible mock patching in tests by exposing submodules and patching at source
- **Non-ASCII characters in source**: Replaced checkmarks, em dashes, and arrows with ASCII equivalents across the codebase for CI compliance
- **Ruff lint errors**: Fixed import sorting (I001) and docstring formatting issues

## [0.12.0b2] - 2026-02-13

### Added

- **16 unit tests for `_events_to_table()`** (`test_events_to_table.py`): covers basic contract, mixed-type columns (float+str, int+str, float+date, kitchen sink), null handling, non-uniform dict keys, and pipeline integration (#219)
- **2 live regression tests for mixed-type BDP** (`test_live_endpoints.py`): `test_bdp_mixed_type_fields` and `test_bdp_mixed_type_multiple_tickers` exercise the exact bug scenario with `ES1 Index` / `NQ1 Index` using `FUT_CONT_SIZE` + `FUT_VAL_PT` (#219)

### Fixed

- **`ArrowInvalid` on multi-field BDP calls**: Bloomberg returns different Python types for different fields (e.g., `float` for `FUT_CONT_SIZE`, `str` for `FUT_VAL_PT`). When both land in the same Arrow value column, `pa.array()` raised `ArrowInvalid`. New `_events_to_table()` builds Arrow tables directly from event dicts with automatic type coercion fallback — stringify on `ArrowInvalid`/`ArrowTypeError`, preserving nulls (#219)
- **Post-transform `pa.Table.from_pandas()` mixed-type failure**: Protected the secondary Arrow conversion (after narwhals transform) with the same stringify fallback for object columns (#219)

## [0.12.0b1] - 2026-02-12

### Changed

- **Async-first architecture**: All Bloomberg API functions (`bdp`, `bds`, `bdh`, `bdib`, `bdtick`, `bql`, `beqs`, `bsrch`, `bqr`, `bta`) now have async counterparts (`abdp`, `abds`, `abdh`, etc.) as the source of truth; sync wrappers delegate via `_run_sync()` (#218)
- **Unified I/O layer**: All Bloomberg requests now flow through a single `arequest()` async entry point in `conn.py`, replacing scattered session/service management across modules (#218)
- **Pipeline and process modules**: Adapted `pipeline_core`, `process`, and `request_builder` to work with the async `arequest()` foundation (#218)
- **Top-level async exports**: All async API variants (`abdp`, `abds`, `abdh`, `abdib`, `abdtick`, `abql`, `abeqs`, `absrch`, `abqr`, `abta`) exported from `xbbg.blp` (#218)
- **IO module cleanup**: Removed dead code and fixed type annotations across `xbbg/io/` (#218)
- **Test coverage expanded**: 571 tests total (up from 543), covering all connection-related GitHub issues and all previously untested paths in `conn.py`

### Removed

- **`xbbg/io/db.py`**: SQLite database helper module (zero imports across codebase) (#218)
- **`xbbg/io/param.py`**: Legacy parameter/configuration module (zero imports across codebase) (#218)
- **`xbbg/io/files.py`**: File path utility module (zero imports after replacing 6 usages in `cache.py` and `const.py` with `pathlib.Path`) (#218)
- **`xbbg/tests/test_param.py`**: Tests for deleted `param` module (7 tests) (#218)
- **`xbbg/markets/cached/pmc_cache.json`**: Stale pandas-market-calendars cache file (pmc dependency removed in v0.11.0) (#218)
- **`xbbg/tests/__init__.py`**, **`examples/feeds/__init__.py`**: Empty `__init__` files (#218)
- **`xbbg/tests/xone.db`**: Stale SQLite test database (#218)
- **`regression_testing/`**: Standalone v0.7.7 regression test directory (6 files); all 9 test scenarios already covered by `xbbg/tests/test_live_endpoints.py` with stricter assertions (#218)

### Fixed

- **Mock session leak in tests**: Added autouse `_reset_session_manager` fixture in `conftest.py` to prevent `MagicMock` sessions from persisting in the `SessionManager` singleton across test modules, which caused infinite `__getattr__` → `_get_child_mock` recursion and stack overflow on Windows (#213)
- **`interval` parameter leaked as Bloomberg override**: `interval` was not in `PRSV_COLS`, causing it to be sent to Bloomberg as an override field instead of being used locally for bar sizing (#145)
- **README Data Storage section**: Clarified that only `bdib()` (intraday bars) has caching via `BarCacheAdapter`; all other functions always make live Bloomberg API calls (#215)
- **README async example for Jupyter**: Fixed `asyncio.run()` example that fails in notebooks (which already have a running event loop) by adding `await`-based and `nest_asyncio` alternatives (#216)
- **Unused imports in tests**: Removed `import os` from `test_intraday_api.py` and `import pytest` from `test_logging.py` that caused Ruff F401 lint failures in CI

### Security

- **Bump `cryptography` from 46.0.4 to 46.0.5**: Fixes CVE-2026-26007 — subgroup attack due to missing validation for SECT binary elliptic curves (#217)

## [0.11.4] - 2026-02-06

### Fixed

- **`bdtick` Arrow conversion failure**: Object columns containing `blpapi.Name` instances caused `pa.Table.from_pandas()` to fail; now stringified before conversion
- **`adjust_ccy` field name mismatch**: Looked for `"Last_Price"` but `bdh` returns lowercase `"last_price"` since v0.11.1, causing `KeyError`
- **`active_futures` two failures**: Used `nw.coalesce()` with a column (`last_tradeable_dt`) not present in SEMI_LONG format, and called `.height` (not valid on narwhals DataFrame) instead of `.shape[0]`
- **Live test assertions**: Updated 10 tests in `test_live_endpoints.py` to match WIDE format default (active since v0.7.x)

## [0.11.3] - 2026-02-06

### Fixed

- **Duplicate `port` keyword argument**: `bbg_service()` and `bbg_session()` used `.get()` to extract `port` then forwarded `**kwargs` still containing it, causing `TypeError: got multiple values for keyword argument 'port'` on non-default ports (e.g., B-Pipe connections) (#212)
- **Session resource leak**: `clear_default_session()` set `_default_session = None` without calling `session.stop()`, leaking OS file descriptors over repeated connect/disconnect cycles (#211)
- **Wrong session removed on retry**: `send_request()` retry path called `remove_session(port=port)` without `server_host`, always targeting `//localhost:{port}` even for remote hosts
- **Inconsistent `server_host` extraction**: `get_session()` / `get_service()` checked `server_host` before `server`, but `connect_bbg()` did the opposite, causing different code paths to resolve different hosts when both keys were present
- **Resource leak on start failure**: `connect_bbg()` did not stop the session before raising `ConnectionError` when `.start()` failed, leaking C++ resources allocated by the `Session()` constructor

## [0.11.2] - 2026-02-05

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

- **BDS output format**: Restored v0.10.x backward compatibility for `bds()` output format (#209)
  - Default `format='wide'` now returns single data column with ticker as index (pandas) or column (other backends)
  - Field column dropped for cleaner output matching v0.10.x behavior
  - Users can opt-in to new 3-column format with `format='long'`
- **ibis backend**: Updated to use `ibis.memtable()` instead of deprecated `con.read_in_memory()`
- **sqlframe backend**: Fixed import path to use `sqlframe.duckdb.DuckDBSession`

## [0.11.1] - 2026-02-05

### Fixed

- **Field names now lowercase**: Restored v0.10.x behavior where `bdp()`, `bdh()`, and `bds()` return field/column names as lowercase (#206)

## [0.11.0] - 2026-02-02

### Added

- **Arrow-first pipeline**: Complete rewrite of internal data processing using PyArrow for improved performance
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

## [0.11.0b5] - 2026-01-25

### Changed

- Internal class renames with backward compatible aliases (`YamlMarketInfoProvider` -> `MetadataProvider`)

### Removed

- **Trials mechanism**: Eliminated retry-blocking system that caused silent failures after 2 failed attempts
- **pandas-market-calendars dependency**: Exchange info now sourced exclusively from Bloomberg API with local caching

### Fixed

- **Import without blpapi installed**: Fixed `AttributeError` when importing xbbg without blpapi (#200)
- **Japan/non-US timezone fix for bdib**: Bloomberg returns trading hours in EST; now correctly converted to exchange's local timezone (#198)
- **get_tz() improvement**: Direct timezone strings recognized without Bloomberg API call

## [0.11.0b4] - 2026-01-24

### Added

- **yas()**: Bloomberg YAS (Yield Analysis) wrapper for fixed income analytics with `YieldType` enum (#202)
- **Treasury and SOFR futures support**: TY, ZN, ZB, ZF, ZT, UB, TN, SFR, SR1, SR3, ED futures (#198)

### Fixed

- **stream() field values**: Subscribed field values now always included in output dict (#199)
- **Futures symbol parsing**: Fixed `market_info()` to correctly parse symbols like `TYH6` -> `TY` (#198)

## [0.11.0b3] - 2026-01-21

### Added

- **bqr()**: Bloomberg Quote Request function emulating Excel `=BQR()` for dealer quote data with broker attribution (#22)

### Fixed

- **Slow Bloomberg fields**: TIMEOUT events handled correctly; requests wait for response with `slow_warn_seconds` warning (#193)
- **Pipeline data types**: Preserve original data types instead of converting to strings (#191)

## [0.11.0b2] - 2026-01-20

### Added

- **preferreds()**: BQL convenience function to find preferred stocks for an equity ticker
- **corporate_bonds()**: BQL convenience function to find active corporate bonds for a ticker

### Fixed

- **bdtick timezone fix**: Pass exchange timezone to fix blank results for non-UTC exchanges (#185)
- **bdtick timeout**: Increased from 10s to 2 minutes for tick data requests

## [0.11.0b1] - 2026-01-10

### Added

- **Arrow-first pipeline**: Complete rewrite of data processing using PyArrow internally
- **Multi-backend support**: New `Backend` enum supporting narwhals, pandas, polars, polars_lazy, pyarrow, duckdb
- **Output format control**: New `Format` enum with long, semi_long, wide options
- **bta()**: Bloomberg Technical Analysis function for 50+ technical indicators
- `set_backend()`, `get_backend()`, `set_format()`, `get_format()` configuration functions
- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`
- v1.0-compatible exception classes (`BlpError`, `BlpSessionError`, etc.)
- `EngineConfig` dataclass and `configure()` function
- `Service` and `Operation` enums for Bloomberg service URIs

### Changed

- All API functions now support `backend` and `format` parameters
- Internal pipeline uses PyArrow tables with narwhals transformations
- Removed pytz dependency (using stdlib `datetime.timezone`)

### Deprecated

- `connect()` / `disconnect()` - engine auto-initializes in v1.0
- `getBlpapiVersion()` - use `get_sdk_info()`
- `lookupSecurity()` - will become `blkp()` in v1.0
- `fieldInfo()` / `fieldSearch()` - will merge into `bfld()` in v1.0

## [0.10.3] - 2025-12-29

### Changed

- Re-enabled futures and CDX resolver tests
- Updated live endpoint tests for LONG format output
- Code style improvements using contextlib.suppress instead of try-except-pass

### Fixed

- Extended BDS test date range to 120 days for quarterly dividends
- Helper functions now work correctly with LONG format output

## [0.10.2] - 2025-12-29

### Changed

- CI/CD improvements with reusable workflows (workflow_call) for release automation
- Separated pypi_upload workflow for trusted publisher compatibility

## [0.10.1] - 2025-12-29

### Changed

- Trigger release workflows via release event instead of workflow_dispatch
- Removed Gitter badge (replaced by Discord)
- Added Discord community link and badge

### Fixed

- Persist blp.connect() session for subsequent API calls (#165)

## [0.10.0] - 2025-12-25

### Added

- Updated polars-bloomberg support for BQL, BDIB and BSRCH (#155)

### Fixed

- Add identifier type prefix to B-Pipe subscription topics (#156)
- Remove pandas version cap to support Python 3.14 (#161)
- Resolve RST formatting warning in index.rst (#162)
- Update Japan equity market hours for TSE trading extension (#163)

## [0.9.1] - 2025-12-11

### Changed

- Add blank lines around latest-release markers in index.rst
- Remove redundant release triggers from workflows
- Trigger release workflows explicitly from semantic_version

### Fixed

- Fix BQL returning only one row for multi-value results (#152)

## [0.9.0] - 2025-12-02

### Added

- Add etf_holdings() function for retrieving ETF holdings via BQL (#147)
- Add multi-day support to bdib() (#148)
- Add multi-day cache support for bdib() (#149)

### Fixed

- Resolve RST duplicate link targets and Sphinx build warnings

## [0.8.2] - 2025-11-19

### Fixed

- Fix BQL options chain metadata issues (#146)

## [0.8.1] - 2025-11-17

### Changed

- CI/CD workflow improvements for trusted publisher compatibility

## [0.8.0] - 2025-11-16

### Added

- **bsrch()**: Bloomberg SRCH queries for fixed income, commodities, and weather data (#137)
- **Fixed income securities support**: ISIN/CUSIP/SEDOL identifiers for bdib (#136)
- **Server host parameter**: Connect to remote Bloomberg servers via `server` parameter (#138)
- **Interval parameter for subscribe()/live()**: Configurable update intervals for real-time feeds
- Semantic versioning workflow for automated releases
- Support for GY (Xetra), IM (Borsa Italiana), and SE (SIX) exchanges (#140)
- Comprehensive bar interval selection guide for bdib function

### Changed

- Comprehensive codebase cleanup and restructuring (#144)
- Improved logging with blpapi integration and performance optimizations (#135)
- Enhanced BEQS timeout handling with configurable `timeout` and `max_timeouts` parameters
- Updated README with comparison table, quickstart guide, and examples

### Fixed

- Fix BQL syntax documentation and error handling (#141, #142)
- Remove 1-minute offset for bare session names in bdtick (#139)
- Resolve Sphinx build errors and RST formatting issues

## [0.8.0rc1] - 2025-11-17

### Changed

- Comprehensive codebase cleanup and restructuring (#144)

## [0.8.0b2] - 2025-11-14

### Fixed

- Fix BQL syntax documentation and error handling (#141, #142)

## [0.8.0b1] - 2025-11-14

### Added

- **BQL support**: Bloomberg Query Language with QueryRequest and result parsing
- **Sub-minute intervals for bdib**: 10-second bars via `intervalHasSeconds=True` flag
- **bsrch()**: Bloomberg SRCH queries for fixed income, commodities, and weather data (#137)
- **Fixed income securities support**: ISIN/CUSIP/SEDOL identifiers for bdib (#136)
- **Server host parameter**: Connect to remote Bloomberg servers via `server` parameter (#138)
- **Interval parameter for subscribe()/live()**: Configurable update intervals for real-time feeds
- Support for GY (Xetra), IM (Borsa Italiana), and SE (SIX) exchanges (#140)

### Changed

- Standardized Google-style docstrings across codebase
- Migrate to uv for development with PEP 621 pyproject.toml
- Improved logging with blpapi integration and performance optimizations (#135)
- Enhanced BEQS timeout handling with configurable `timeout` and `max_timeouts` parameters

### Fixed

- Remove 1-minute offset for bare session names in bdtick (#139)

## [0.7.11] - 2025-11-12

### Added

- **BQL support**: Bloomberg Query Language with QueryRequest and result parsing
- **Sub-minute intervals for bdib**: 10-second bars via `intervalHasSeconds=True` flag
- pandas-market-calendars integration for exchange session resolution

### Changed

- Standardized Google-style docstrings across codebase
- Migrate to uv for development with PEP 621 pyproject.toml
- Switch to PyPI Trusted Publishing (OIDC)
- Exclude tests from wheel and sdist distributions

### Fixed

- Fix BQL to use correct service name and handle JSON response format
- Normalize UX* Index symbols; fix pandas 'M' deprecation to 'ME' in fut_ticker

## [0.7.10] - 2025-11-05

### Added

- Enhanced Bloomberg connection handling with alternative connection methods
- Market resolvers for active futures and CDX tickers

### Changed

- Replace flake8 with ruff for linting
- Update Python version requirements and dependencies
- Clean up CI workflows and documentation

## [0.7.9] - 2025-04-15

### Changed

- Add exchanges support
- CI/CD configuration updates

### Fixed

- Corrected typo (thanks to @ShiyuanSchonfeld)
- Pin pandas version due to pd.to_datetime behaviour change in format_raw
- Fix TLS Options typo when creating a new connection

## [0.7.8a2] - 2022-12-03

### Added

- Additional exchanges support (#83)

### Changed

- CI/CD configuration improvements

## [0.7.7] - 2022-06-19

### Added

- Custom config usage in bdib (contributed by @hceh)
- Options in `blp.live` (contributed by @swiecki)

### Changed

- Pandas options handling in doctest
- CI/CD configuration updates

## [0.7.7a4] - 2022-05-25

### Changed

- Pandas options handling in doctest

## [0.7.7a3] - 2021-12-31

### Fixed

- Typo fix

## [0.7.7a2] - 2021-12-20

### Added

- Custom config and reference exchange support (contributed by @hceh)

## [0.7.7a1] - 2021-07-13

### Added

- Options in `blp.live` (contributed by @swiecki)

## [0.7.6] - 2021-07-05

### Added

- Log folder creation handling
- Alternative connection method support
- Custom session argument for Bloomberg connections
- `bdtick` with custom time range support

### Changed

- Update asset universe
- Exchange info corrections
- No manual conversion of timezones

### Fixed

- BDS fix for edge cases
- blpapi install URL correction

## [0.7.6a8] - 2021-04-17

### Fixed

- Log folder creation bug

## [0.7.6a7] - 2021-04-02

### Changed

- Update asset universe

## [0.7.6a6] - 2021-03-27

### Fixed

- Exchange info corrected

## [0.7.6a5] - 2021-03-05

### Changed

- No manual conversion of timezones

## [0.7.6a4] - 2021-03-05

### Added

- `bdtick` with custom time range support

## [0.7.6a3] - 2021-02-10

### Fixed

- Bug fixes for BDS and blpapi install URL

## [0.7.6a2] - 2021-02-07

### Added

- Alternative connection method

## [0.7.6a1] - 2021-02-03

### Added

- Add `sess` as argument for custom Bloomberg session

## [0.7.5] - 2021-01-31

### Added

- Currency adjusted turnover function
- Useful fields for live feeds
- More examples in documentation

### Changed

- Standardize IO operations
- Log levels adjustment
- Replace `os.path` with pathlib
- Performance function improvements
- Default args of live feeds

### Fixed

- CCY adjust fix
- Bug in finding exchange info

## [0.7.5b2] - 2021-01-30

### Changed

- Log levels adjustment

## [0.7.5b1] - 2021-01-13

### Added

- New methods included in `__all__`

### Fixed

- CCY adjust fix

## [0.7.5a9] - 2021-01-12

### Added

- Currency adjusted turnover function

## [0.7.5a09] - 2021-01-12

### Added

- Currency adjusted turnover function

## [0.7.5a8] - 2021-01-11

### Fixed

- Fix bug in finding exchange info

## [0.7.5a7] - 2021-01-07

### Changed

- Default args of live feeds

## [0.7.2] - 2020-12-16

### Added

- Logo image for project branding

### Changed

- Use `async` for live data feeds
- Speed up by caching files
- Change logic of exchange lookup and market timing
- Push all values from live subscription
- Support for Python 3.8

### Fixed

- Proper caching implementation

## [0.7.0] - 2020-08-02

### Changed

- `bdh` preserves column orders (both tickers and flds)
- `timeout` argument is available for all queries
- `bdtick` usually takes longer to respond - can use `timeout=1000` for example if keep getting empty DataFrame

## [0.6.7] - 2020-05-17

### Added

- Add flexibility to use reference exchange as market hour definition
- No longer necessary to add `.yml` for new tickers, provided that the exchange was defined in `/xbbg/markets/exch.yml`

### Changed

- Switch CI from Travis to GitHub Actions

## [0.6.0] - 2020-01-23

### Added

- Tick data availability via bdtick()

### Changed

- Speed improvements by removing intermediate layer of generator for processing Bloomberg responses

## [0.5.0] - 2020-01-08

### Changed

- Rewritten library to add subscription, BEQS, simplify interface and remove dependency of `pdblp`

## [0.1.22] - 2019-09-15

### Security

- Remove PyYAML dependency due to security vulnerability

## [0.1.17] - 2019-07-01

### Added

- Add `adjust` argument in `bdh` for easier dividend / split adjustments

---

[Unreleased]: https://github.com/alpha-xone/xbbg/compare/v0.12.3...HEAD
[0.12.3]: https://github.com/alpha-xone/xbbg/compare/v0.12.2...v0.12.3
[0.12.2]: https://github.com/alpha-xone/xbbg/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/alpha-xone/xbbg/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/alpha-xone/xbbg/compare/v0.12.0b3...v0.12.0
[0.12.0b3]: https://github.com/alpha-xone/xbbg/compare/v0.12.0b2...v0.12.0b3
[0.12.0b2]: https://github.com/alpha-xone/xbbg/compare/v0.12.0b1...v0.12.0b2
[0.12.0b1]: https://github.com/alpha-xone/xbbg/compare/v0.11.4...v0.12.0b1
[0.11.4]: https://github.com/alpha-xone/xbbg/compare/v0.11.3...v0.11.4
[0.11.3]: https://github.com/alpha-xone/xbbg/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/alpha-xone/xbbg/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/alpha-xone/xbbg/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/alpha-xone/xbbg/compare/v0.11.0b5...v0.11.0
[0.11.0b5]: https://github.com/alpha-xone/xbbg/compare/v0.11.0b4...v0.11.0b5
[0.11.0b4]: https://github.com/alpha-xone/xbbg/compare/v0.11.0b3...v0.11.0b4
[0.11.0b3]: https://github.com/alpha-xone/xbbg/compare/v0.11.0b2...v0.11.0b3
[0.11.0b2]: https://github.com/alpha-xone/xbbg/compare/v0.11.0b1...v0.11.0b2
[0.11.0b1]: https://github.com/alpha-xone/xbbg/compare/v0.10.3...v0.11.0b1
[0.10.3]: https://github.com/alpha-xone/xbbg/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/alpha-xone/xbbg/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/alpha-xone/xbbg/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/alpha-xone/xbbg/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/alpha-xone/xbbg/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/alpha-xone/xbbg/compare/v0.8.2...v0.9.0
[0.8.2]: https://github.com/alpha-xone/xbbg/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/alpha-xone/xbbg/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/alpha-xone/xbbg/compare/v0.8.0rc1...v0.8.0
[0.8.0rc1]: https://github.com/alpha-xone/xbbg/compare/v0.8.0b2...v0.8.0rc1
[0.8.0b2]: https://github.com/alpha-xone/xbbg/compare/v0.8.0b1...v0.8.0b2
[0.8.0b1]: https://github.com/alpha-xone/xbbg/compare/v0.7.11...v0.8.0b1
[0.7.11]: https://github.com/alpha-xone/xbbg/compare/v0.7.10...v0.7.11
[0.7.10]: https://github.com/alpha-xone/xbbg/compare/v0.7.9...v0.7.10
[0.7.9]: https://github.com/alpha-xone/xbbg/compare/v0.7.8a2...v0.7.9
[0.7.8a2]: https://github.com/alpha-xone/xbbg/compare/v0.7.7...v0.7.8a2
[0.7.7]: https://github.com/alpha-xone/xbbg/compare/v0.7.7a4...v0.7.7
[0.7.7a4]: https://github.com/alpha-xone/xbbg/compare/v0.7.7a3...v0.7.7a4
[0.7.7a3]: https://github.com/alpha-xone/xbbg/compare/v0.7.7a2...v0.7.7a3
[0.7.7a2]: https://github.com/alpha-xone/xbbg/compare/v0.7.7a1...v0.7.7a2
[0.7.7a1]: https://github.com/alpha-xone/xbbg/compare/v0.7.6...v0.7.7a1
[0.7.6]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a8...v0.7.6
[0.7.6a8]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a7...v0.7.6a8
[0.7.6a7]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a6...v0.7.6a7
[0.7.6a6]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a5...v0.7.6a6
[0.7.6a5]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a4...v0.7.6a5
[0.7.6a4]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a3...v0.7.6a4
[0.7.6a3]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a2...v0.7.6a3
[0.7.6a2]: https://github.com/alpha-xone/xbbg/compare/v0.7.6a1...v0.7.6a2
[0.7.6a1]: https://github.com/alpha-xone/xbbg/compare/v0.7.5...v0.7.6a1
[0.7.5]: https://github.com/alpha-xone/xbbg/compare/v0.7.5b2...v0.7.5
[0.7.5b2]: https://github.com/alpha-xone/xbbg/compare/v0.7.5b1...v0.7.5b2
[0.7.5b1]: https://github.com/alpha-xone/xbbg/compare/v0.7.5a9...v0.7.5b1
[0.7.5a9]: https://github.com/alpha-xone/xbbg/compare/v0.7.5a09...v0.7.5a9
[0.7.5a09]: https://github.com/alpha-xone/xbbg/compare/v0.7.5a8...v0.7.5a09
[0.7.5a8]: https://github.com/alpha-xone/xbbg/compare/v0.7.5a7...v0.7.5a8
[0.7.5a7]: https://github.com/alpha-xone/xbbg/compare/v0.7.2...v0.7.5a7
[0.7.2]: https://github.com/alpha-xone/xbbg/compare/v0.7.0...v0.7.2
[0.7.0]: https://github.com/alpha-xone/xbbg/compare/v0.6.7...v0.7.0
[0.6.7]: https://github.com/alpha-xone/xbbg/compare/v0.6.0...v0.6.7
[0.6.0]: https://github.com/alpha-xone/xbbg/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/alpha-xone/xbbg/compare/v0.1.22...v0.5.0
[0.1.22]: https://github.com/alpha-xone/xbbg/compare/v0.1.17...v0.1.22
[0.1.17]: https://github.com/alpha-xone/xbbg/releases/tag/v0.1.17
