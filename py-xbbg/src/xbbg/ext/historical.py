"""Historical data extension functions.

Convenience wrappers around bds/bdh for common historical data queries.
Returns DataFrame in the configured backend format.
Uses high-performance Rust utilities from xbbg._core.

Sync functions (wrap async with asyncio.run):
    - dividend(): Get dividend and split history
    - dividend_yield(): Get trailing realized dividend yield
    - earnings(): Get earnings breakdown
    - turnover(): Get trading volume and turnover
    - etf_holdings(): Get ETF holdings via BQL

Async functions (primary implementation):
    - adividend(): Async dividend history
    - adividend_yield(): Async trailing realized dividend yield
    - aearnings(): Async earnings breakdown
    - aturnover(): Async turnover data
    - aetf_holdings(): Async ETF holdings
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import narwhals.stable.v1 as nw

from xbbg.ext._utils import DateLike, _call_native_recipe, _fmt_date, _syncify

_NATIVE_IMPORT_ERROR_MARKERS = (
    "DLL load failed",
    "cannot open shared object file",
    "image not found",
    "Library not loaded",
)


def _is_native_import_error(error: ImportError) -> bool:
    message = str(error)
    native_loader_error = any(marker in message for marker in _NATIVE_IMPORT_ERROR_MARKERS) and (
        "_core" in message or "xbbg" in message
    )
    # "cannot import name" also carries name="xbbg._core", but there the extension
    # loaded and lacks a helper: it is outdated, not missing, so it must not degrade.
    return not message.startswith("cannot import name") and (
        error.name == "xbbg._core" or "No module named 'xbbg._core'" in message or native_loader_error
    )


try:
    # Import Rust helper for turnover defaults. This helper has a faithful
    # Python fallback so offline tests can import and exercise turnover paths.
    from xbbg._core import ext_default_turnover_dates
except ImportError as exc:
    if not _is_native_import_error(exc):
        raise

    from datetime import date as _date, timedelta as _timedelta

    def _parse_iso_date(value: str | None) -> _date | None:
        if value is None:
            return None
        try:
            return _date.fromisoformat(value)
        except ValueError:
            return None

    def ext_default_turnover_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
        end = _parse_iso_date(end_date) or (_date.today() - _timedelta(days=1))
        start = _parse_iso_date(start_date) or (end - _timedelta(days=30))
        return start.isoformat(), end.isoformat()


def _missing_native_helper(name: str, source: ImportError):
    def missing(*_args, **_kwargs):
        raise ImportError(f"xbbg.ext.historical.{name} requires xbbg._core native helpers") from source

    missing.__name__ = name
    return missing


try:
    # Import Rust ext utilities for production transformations. These helpers
    # do not have local approximations; failing clearly is safer than returning
    # plausible but wrong transformed data when native helpers are unavailable.
    from xbbg._core import (
        ext_build_earning_header_rename,
        ext_build_etf_holdings_query,
        ext_calculate_level_percentages,
        ext_filter_equity_tickers,
        ext_get_dvd_type,
        ext_rename_dividend_columns,
    )
except ImportError as exc:
    if not _is_native_import_error(exc):
        raise

    ext_filter_equity_tickers = _missing_native_helper("ext_filter_equity_tickers", exc)
    ext_get_dvd_type = _missing_native_helper("ext_get_dvd_type", exc)
    ext_rename_dividend_columns = _missing_native_helper("ext_rename_dividend_columns", exc)
    ext_build_earning_header_rename = _missing_native_helper("ext_build_earning_header_rename", exc)
    ext_calculate_level_percentages = _missing_native_helper("ext_calculate_level_percentages", exc)
    ext_build_etf_holdings_query = _missing_native_helper("ext_build_etf_holdings_query", exc)


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


def _get_empty_dataframe() -> IntoDataFrame:
    """Return empty DataFrame using configured backend."""
    from xbbg import blp
    from xbbg._core import ArrowTable

    return blp._convert_result_backend(ArrowTable.empty(["ticker", "field", "value"]), None)


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def adividend(
    tickers: str | list[str],
    typ: str = "all",
    *,
    start_date: DateLike = None,
    end_date: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Async get dividend and split history for securities.

    Convenience wrapper around abds() for dividend data.
    Uses Rust for ticker filtering and column renaming.

    Args:
        tickers: Single ticker or list of tickers (must be Equity).
        typ: Dividend type:
            - "all": All dividends and splits (DVD_Hist_All)
            - "dvd": Regular dividends only (DVD_Hist)
            - "split": Splits only (Eqy_DVD_Hist_Splits)
            - "gross": Gross dividends (Eqy_DVD_Hist_Gross)
            - "adjust": Adjustment factors (Eqy_DVD_Adjust_Fact)
            - "adj_fund": Fund adjustment factors (Eqy_DVD_Adj_Fund)
            - "with_amt": All with amount status (DVD_Hist_All_with_Amt_Status)
            - "dvd_amt": Dividends with amount status (DVD_Hist_with_Amt_Status)
            - "gross_amt": Gross with amount status (DVD_Hist_Gross_with_Amt_Stat)
            - "projected": Projected dividends (BDVD_Pr_Ex_Dts_DVD_Amts_w_Ann)
        start_date: Start date for dividend history (optional).
        end_date: End date for dividend history (optional).
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with dividend history (type depends on configured backend).

    Example::

        import asyncio
        from xbbg.ext.historical import adividend


        async def main():
            # Get all dividend history
            df = await adividend("AAPL US Equity")

            # Get dividends for specific date range
            df = await adividend("MSFT US Equity", start_date="2020-01-01", end_date="2024-01-01")

            # Get only splits
            df = await adividend("TSLA US Equity", typ="split")


        asyncio.run(main())
    """
    from xbbg import abds

    # Pop 'raw' kwarg if present (not used by bds)
    kwargs.pop("raw", None)

    # Normalize and filter tickers using Rust
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    # Filter to equity tickers using Rust (high performance)
    tickers_list = ext_filter_equity_tickers(tickers_list)

    if not tickers_list:
        return _get_empty_dataframe()

    # Get the Bloomberg field name using Rust
    fld = ext_get_dvd_type(typ)
    if fld is None:
        fld = typ  # Use as-is if not in mapping

    # Special handling for adjustment factors
    if fld == "Eqy_DVD_Adjust_Fact" and "Corporate_Actions_Filter" not in kwargs:
        kwargs["Corporate_Actions_Filter"] = "NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE"

    # Add date overrides
    if start_date:
        kwargs["DVD_Start_Dt"] = _fmt_date(start_date)
    if end_date:
        kwargs["DVD_End_Dt"] = _fmt_date(end_date)

    # Call abds - returns DataFrame in configured backend format
    df = await abds(tickers=tickers_list, flds=fld, **kwargs)

    # Convert to narwhals for manipulation
    nw_df = nw.from_native(df)

    if len(nw_df) == 0:
        return df

    # Get column rename mapping from Rust (high performance)
    rename_pairs = ext_rename_dividend_columns(list(nw_df.columns))
    if rename_pairs:
        rename_map = {old: new for old, new in rename_pairs}
        nw_df = nw_df.rename(rename_map)
        return nw_df.to_native()

    return df


def _build_earning_header_rename(
    header_nw: nw.DataFrame,
    data_nw: nw.DataFrame,
) -> dict[str, str]:
    """Build column rename mapping from earnings header values using Rust.

    Maps data column names (e.g., "Period X Value") to human-readable names
    derived from the header row (e.g., "fy2023").

    Args:
        header_nw: Header DataFrame from PG_Bulk_Header BDS call.
        data_nw: Data DataFrame from PG_{typ} BDS call.

    Returns:
        Dict mapping original column names to cleaned header-based names.
    """
    # Extract header row as (col_name, value) pairs for Rust
    header_row_pairs = [(str(k), str(v)) for k, v in next(header_nw.iter_rows(named=True)).items()]

    # Delegate to Rust (high performance string manipulation)
    rename_pairs = ext_build_earning_header_rename(header_row_pairs, list(data_nw.columns))
    return dict(rename_pairs)


def _compute_earning_percentages(
    data_nw: nw.DataFrame,
    fy_cols: list[str],
) -> nw.DataFrame:
    """Compute percentage columns for each fiscal year in earnings data using Rust.

    For level 1 rows, computes percentage of total level 1 sum.
    For level 2 rows, computes percentage of parent level 1 group sum.

    Args:
        data_nw: Earnings DataFrame (already renamed) containing a "level" column.
        fy_cols: List of fiscal year column names (e.g., ["fy2022", "fy2023"]).

    Returns:
        DataFrame with ``{fy}_pct`` columns inserted after each fiscal year column.
    """
    # Extract levels once (shared across all fy columns)
    raw_levels = data_nw["level"].to_list()
    levels: list[int | None] = []
    for lvl in raw_levels:
        if lvl is None:
            levels.append(None)
        else:
            try:
                levels.append(int(lvl))
            except (ValueError, TypeError):
                levels.append(None)

    for yr in fy_cols:
        pct_col = f"{yr}_pct"

        # Extract fiscal year values, converting to float
        raw_values = data_nw[yr].to_list()
        values: list[float | None] = []
        for val in raw_values:
            if val is None:
                values.append(None)
            else:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    values.append(None)

        # Calculate percentages using Rust (high performance)
        pct_values = ext_calculate_level_percentages(values, levels)

        # Add percentage column using narwhals (backend-agnostic)
        native_namespace = nw.get_native_namespace(data_nw)
        pct_series = nw.new_series(pct_col, pct_values, native_namespace=native_namespace)

        # Insert after the year column
        yr_idx = data_nw.columns.index(yr)
        cols_before = data_nw.columns[: yr_idx + 1]
        cols_after = data_nw.columns[yr_idx + 1 :]

        data_nw = data_nw.with_columns(pct_series)

        # Reorder columns to place pct after year
        new_order = [*list(cols_before), pct_col, *list(cols_after)]
        data_nw = data_nw.select(new_order)

    return data_nw


async def aearnings(
    ticker: str,
    by: str = "Geo",
    typ: str = "Revenue",
    *,
    ccy: str | None = None,
    level: int | None = None,
    year: int | None = None,
    periods: int | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async get earnings breakdown for a security.

    Convenience wrapper around abds() for earnings data by geography or product.

    Args:
        ticker: Single ticker (e.g., "AMD US Equity").
        by: Breakdown type - "Geo" (geographic) or "Product".
        typ: Type of earning metric:
            - "Revenue": Revenue breakdown
            - "Operating_Income": Operating income (EBIT)
            - "Assets": Assets breakdown
            - "Gross_Profit": Gross profit
            - "Capital_Expenditures": CapEx
        ccy: Currency for earnings (optional).
        level: Hierarchy level (optional).
        year: Fiscal year (e.g., 2023).
        periods: Number of periods to retrieve.
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with earnings breakdown (type depends on configured backend).

    Example::

        import asyncio
        from xbbg.ext.historical import aearnings


        async def main():
            # Get geographic revenue breakdown
            df = await aearnings("AMD US Equity", by="Geo")

            # Get product revenue breakdown for specific year
            df = await aearnings("AAPL US Equity", by="Product", year=2023)

            # Get operating income by geography
            df = await aearnings("MSFT US Equity", by="Geo", typ="Operating_Income")


        asyncio.run(main())
    """
    from xbbg import abds

    # Pop 'raw' kwarg if present (not used by bds)
    kwargs.pop("raw", None)

    # Determine override value
    ovrd = "G" if by[0].upper() == "G" else "P"
    base_kwargs: dict = {"Product_Geo_Override": ovrd}

    # Add optional overrides
    if year:
        base_kwargs["Eqy_Fund_Year"] = year
    if periods:
        base_kwargs["Number_Of_Periods"] = periods

    # Get header first
    header = await abds(tickers=ticker, flds="PG_Bulk_Header", **base_kwargs, **kwargs)
    header_nw = nw.from_native(header)

    # Add currency and level if specified
    if ccy:
        base_kwargs["Eqy_Fund_Crncy"] = ccy
    if level:
        base_kwargs["PG_Hierarchy_Level"] = level

    # Get the actual data
    data = await abds(tickers=ticker, flds=f"PG_{typ}", **base_kwargs, **kwargs)
    data_nw = nw.from_native(data)

    if len(data_nw) == 0 or len(header_nw) == 0:
        return data

    # Build column rename mapping from header
    rename_map = _build_earning_header_rename(header_nw, data_nw)

    # Apply renaming
    if rename_map:
        data_nw = data_nw.rename(rename_map)

    # Calculate percentage columns for each fiscal year
    if "level" not in data_nw.columns:
        return data_nw.to_native()

    # Find fiscal year columns (start with 'fy')
    fy_cols = [c for c in data_nw.columns if c.startswith("fy") and not c.endswith("_pct")]

    data_nw = _compute_earning_percentages(data_nw, fy_cols)

    return data_nw.to_native()


async def _calc_turnover_from_volume(
    missing_tickers: list[str],
    start_date: DateLike,
    end_date: DateLike,
    nw_df: nw.DataFrame,
    **kwargs,
) -> tuple[nw.DataFrame, IntoDataFrame]:
    """Fetch volume × VWAP for tickers missing direct Turnover data.

    For each ticker in *missing_tickers*, retrieves ``eqy_weighted_avg_px``
    and ``volume`` via ``abdh``, computes turnover as their product, and
    appends the result rows into the main LONG-format DataFrame.

    Input/output is LONG format: ``{ticker, date, field, value}``.

    Args:
        missing_tickers: Tickers that had no Turnover rows in the initial fetch.
        start_date: Start date for the historical query.
        end_date: End date for the historical query.
        nw_df: Current narwhals DataFrame in LONG format (may be empty).
        **kwargs: Additional arguments forwarded to ``abdh``.

    Returns:
        Tuple of (updated narwhals DataFrame, updated native DataFrame).
    """
    import xbbg

    vol_data = await xbbg.abdh(
        tickers=missing_tickers,
        flds=["eqy_weighted_avg_px", "volume"],
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )
    vol_nw = nw.from_native(vol_data)

    if len(vol_nw) == 0 or "field" not in vol_nw.columns:
        return nw_df, nw_df.to_native()

    # LONG format: {ticker, date, field, value}
    # For each ticker+date, get vwap and volume, compute turnover
    new_rows: list[dict[str, str]] = []
    malformed_rows = 0
    malformed_examples: list[str] = []
    for t in missing_tickers:
        tk_rows = vol_nw.filter(nw.col("ticker") == t)
        if len(tk_rows) == 0:
            continue

        # Build {date -> {field: value}} lookup
        date_vals: dict[str, dict[str, str]] = {}
        for row in tk_rows.iter_rows(named=True):
            dt = row.get("date", "")
            field = row.get("field", "").lower()
            val = row.get("value", "")
            if dt not in date_vals:
                date_vals[dt] = {}
            date_vals[dt][field] = val

        for dt, flds in date_vals.items():
            vwap_str = flds.get("eqy_weighted_avg_px", "")
            vol_str = flds.get("volume", "")
            if vwap_str and vol_str:
                try:
                    turnover = float(vwap_str) * float(vol_str)
                    new_rows.append({"ticker": t, "date": dt, "field": "turnover", "value": str(turnover)})
                except (ValueError, TypeError):
                    malformed_rows += 1
                    if len(malformed_examples) < 3:
                        malformed_examples.append(f"{t}/{dt}: vwap={vwap_str!r}, volume={vol_str!r}")

    if malformed_rows:
        logger.warning(
            "Turnover volume fallback skipped malformed_rows=%s examples=%s",
            malformed_rows,
            malformed_examples,
        )

    if new_rows:
        # Build a new narwhals DataFrame from the turnover rows and concat
        native_ns = nw.get_native_namespace(nw_df)
        cols = {
            k: nw.new_series(k, [r[k] for r in new_rows], native_namespace=native_ns)
            for k in ("ticker", "date", "field", "value")
        }
        first = next(iter(cols.values()))
        turnover_df = first.to_frame()
        for s in list(cols.values())[1:]:
            turnover_df = turnover_df.with_columns(s)

        if len(nw_df) > 0:
            nw_df = nw.concat([nw_df, turnover_df])  # type: ignore[invalid-assignment]
        else:
            nw_df = turnover_df

    return nw_df, nw_df.to_native()


async def aturnover(
    tickers: str | list[str],
    *,
    start_date: DateLike = None,
    end_date: DateLike = None,
    ccy: str = "USD",
    factor: float = 1e6,
    **kwargs,
) -> IntoDataFrame:
    """Async get trading volume and turnover for securities.

    Convenience wrapper around abdh() for turnover data with optional
    currency conversion. For equities where the Turnover field is not available,
    calculates turnover as volume * VWAP (eqy_weighted_avg_px).

    Args:
        tickers: Single ticker or list of tickers.
        start_date: Start date for turnover data (default: 1 month ago).
        end_date: End date for turnover data (default: yesterday).
        ccy: Currency for conversion (default: "USD"). Use "local" for no conversion.
        factor: Division factor (default: 1e6 for millions).
        **kwargs: Additional arguments passed to abdh().

    Returns:
        DataFrame with turnover data (type depends on configured backend).

    Example::

        import asyncio
        from xbbg.ext.historical import aturnover


        async def main():
            # Get turnover in millions USD
            df = await aturnover(["AAPL US Equity", "MSFT US Equity"], start_date="2024-01-01")

            # Get turnover in local currency
            df = await aturnover("7203 JP Equity", ccy="local")

            # Get turnover in EUR, in billions
            df = await aturnover("SAP GR Equity", ccy="EUR", factor=1e9)


        asyncio.run(main())
    """
    import xbbg
    from xbbg.ext.currency import aconvert_ccy

    # Validate explicit date arguments before applying defaults. The native helper
    # treats malformed strings like missing values, which would silently shift the
    # requested turnover window.
    start_str = _fmt_date(start_date, "%Y-%m-%d") if start_date is not None else None
    end_str = _fmt_date(end_date, "%Y-%m-%d") if end_date is not None else None
    start_date, end_date = ext_default_turnover_dates(start_str, end_str)

    # Normalize tickers
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    # Get turnover data - returns DataFrame in configured backend format
    data = await xbbg.abdh(
        tickers=tickers_list,
        flds="Turnover",
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )

    # Convert to narwhals for manipulation
    nw_df = nw.from_native(data)

    # Check which tickers have turnover data
    # LONG format: {ticker, date, field, value} — check the ticker column
    tickers_with_data: set[str] = set()
    if "ticker" in nw_df.columns:
        tickers_with_data = set(nw_df["ticker"].unique().to_list())

    # For tickers without turnover, calculate from volume * VWAP
    missing_tickers = [t for t in tickers_list if t not in tickers_with_data]

    if missing_tickers:
        try:
            nw_df, data = await _calc_turnover_from_volume(missing_tickers, start_date, end_date, nw_df, **kwargs)
        except (ValueError, TypeError, KeyError):
            # If fallback fails, continue with original data
            logger.debug("Turnover volume fallback failed")

    if len(nw_df) == 0:
        return data

    # Apply currency conversion
    if ccy.lower() != "local":
        data = await aconvert_ccy(data, ccy=ccy)
        nw_df: nw.DataFrame = nw.from_native(data)  # type: ignore[assignment]

    # Apply factor to string values in LONG format
    if factor != 1.0 and "value" in nw_df.columns:
        values = nw_df["value"].to_list()
        new_values = []
        for v in values:
            try:
                new_values.append(str(float(v) / factor))
            except (ValueError, TypeError):
                new_values.append(v)
        native_ns = nw.get_native_namespace(nw_df)
        nw_df = nw_df.with_columns(nw.new_series("value", new_values, native_namespace=native_ns))
        return nw_df.to_native()

    return data


async def aetf_holdings(
    etf_ticker: str,
    *,
    fields: list[str] | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async get ETF holdings using Bloomberg Query Language (BQL).

    Retrieves holdings information for an ETF including ISIN, weights, and position IDs.

    Args:
        etf_ticker: ETF ticker (e.g., 'SPY US Equity' or 'SPY'). If no suffix is provided,
            ' US Equity' will be appended automatically.
        fields: Optional list of additional fields to retrieve. Default fields are
            id_isin, weights, and id().position. If provided, these will be added to
            the default fields.
        **kwargs: Additional options passed to the underlying BQL query.

    Returns:
        DataFrame with ETF holdings data (type depends on configured backend).
        Columns include: holding (ticker), id_isin, weights, position, and any
        additional requested fields.

    Example::

        import asyncio
        from xbbg.ext.historical import aetf_holdings


        async def main():
            # Get holdings for an ETF
            df = await aetf_holdings("SPY US Equity")

            # Get holdings with additional fields
            df = await aetf_holdings("SPY US Equity", fields=["name", "px_last"])

            # Ticker without suffix (will append ' US Equity')
            df = await aetf_holdings("SPY")

            # Get holdings for a non-US ETF
            df = await aetf_holdings("VWRL LN Equity")


        asyncio.run(main())
    """
    from xbbg import abql
    from xbbg._core import ext_rename_etf_columns

    # Build BQL query using Rust (handles ticker normalization, field defaults)
    extra = list(fields) if fields else []
    bql_query = ext_build_etf_holdings_query(etf_ticker, extra)

    # Execute BQL query - returns DataFrame in configured backend format
    df = await abql(bql_query, **kwargs)

    # Convert to narwhals for manipulation
    nw_df = nw.from_native(df)

    if len(nw_df) == 0:
        return df

    # Get column rename mapping from Rust
    rename_pairs = ext_rename_etf_columns(list(nw_df.columns))

    # Also handle special BQL columns
    rename_map = {old: new for old, new in rename_pairs}
    if "id().position" in nw_df.columns:
        rename_map["id().position"] = "position"
    if "ID" in nw_df.columns:
        rename_map["ID"] = "holding"

    if rename_map:
        nw_df = nw_df.rename(rename_map)
        return nw_df.to_native()

    return df


async def adividend_yield(
    tickers: str | list[str],
    *,
    start_date: DateLike,
    end_date: DateLike,
    dividend_types: list[str] | None = None,
    window_days: int = 365,
    backend=None,
    **_kwargs,
) -> IntoDataFrame:
    """Async trailing realized dividend amount and dividend yield from native recipe."""
    tickers_list = [tickers] if isinstance(tickers, str) else list(tickers)
    start = _fmt_date(start_date)
    end = _fmt_date(end_date)
    if start is None or end is None:
        raise ValueError("start_date and end_date are required")
    return await _call_native_recipe(
        "recipe_dividend_yield",
        tickers_list,
        start,
        end,
        list(dividend_types) if dividend_types is not None else None,
        window_days,
        backend=backend,
    )


dividend = _syncify(adividend)
earnings = _syncify(aearnings)
turnover = _syncify(aturnover)
etf_holdings = _syncify(aetf_holdings)
dividend_yield = _syncify(adividend_yield)
