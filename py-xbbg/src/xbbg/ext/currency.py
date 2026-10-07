"""Currency conversion extension functions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import narwhals.stable.v1 as nw

from xbbg._core_guard import is_native_import_error
from xbbg.ext._utils import _pivot_bdp_to_wide, _syncify

try:
    # Import Rust ext utilities for max performance
    from xbbg._core import (
        ext_build_fx_pair,
        ext_same_currency,
    )
except ImportError as exc:
    if not is_native_import_error(exc):
        raise

    def ext_same_currency(from_ccy: str, to_ccy: str) -> bool:
        """Offline-safe fallback matching xbbg._core semantics."""
        return str(from_ccy).upper() == str(to_ccy).upper()

    def ext_build_fx_pair(from_ccy: str, to_ccy: str) -> tuple[str, float, str, str]:
        """Offline-safe fallback matching xbbg._core semantics."""
        raw_source = str(from_ccy)
        source = raw_source.upper()
        target = str(to_ccy).upper()
        factor = 100.0 if raw_source[-1:].islower() else 1.0
        return f"{target}{source} Curncy", factor, source, target


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def aconvert_ccy(
    data: IntoDataFrame,
    ccy: str = "USD",
    **kwargs,
) -> IntoDataFrame:
    """Async convert DataFrame values to a target currency."""
    # Convert to narwhals DataFrame
    nw_df: nw.DataFrame = nw.from_native(data)  # type: ignore[assignment]

    if len(nw_df) == 0:
        return nw_df.to_native()

    if ccy.lower() == "local":
        return nw_df.to_native()

    # --- Extract unique tickers from the LONG-format ticker column ---
    if "ticker" not in nw_df.columns:
        return nw_df.to_native()

    # Need a value column to convert -- if absent, nothing to do
    if "value" not in nw_df.columns:
        return nw_df.to_native()

    tickers = nw_df["ticker"].unique().to_list()
    tickers = [t for t in tickers if t]  # drop empty/null

    if not tickers:
        return nw_df.to_native()

    import xbbg

    # --- Get currency for each ticker from Bloomberg ---
    try:
        ccy_data = await xbbg.abdp(tickers=tickers, flds="crncy", **kwargs)
        ccy_nw: nw.DataFrame = nw.from_native(ccy_data)
        ccy_nw = _pivot_bdp_to_wide(ccy_nw)
    except (ValueError, TypeError, KeyError):
        logger.warning("Failed to get currency data for tickers")
        return nw_df.to_native()

    if len(ccy_nw) == 0 or "crncy" not in ccy_nw.columns:
        return nw_df.to_native()

    # --- Build FX pair mapping using Rust ---
    # ticker -> {fx_pair, factor}
    fx_info: dict[str, dict] = {}
    fx_pairs_needed: set[str] = set()

    for row in ccy_nw.iter_rows(named=True):
        ticker = row.get("ticker", "")
        local_ccy = row.get("crncy", "")

        if not local_ccy:
            continue

        # Check if same currency using Rust
        if ext_same_currency(local_ccy, ccy):
            continue

        # Build FX pair using Rust (handles GBp/GBP factor, etc.)
        fx_pair, factor, _from_ccy, _to_ccy = ext_build_fx_pair(local_ccy, ccy)

        fx_info[ticker] = {"fx_pair": fx_pair, "factor": factor}
        fx_pairs_needed.add(fx_pair)

    if not fx_pairs_needed:
        # All tickers already in target currency
        return nw_df.to_native()

    # --- Get FX rates for the date range ---
    if "date" not in nw_df.columns:
        return nw_df.to_native()

    dates = nw_df["date"].to_list()
    start_date = min(dates) if dates else None
    end_date = max(dates) if dates else None

    if not start_date or not end_date:
        return nw_df.to_native()

    try:
        fx_data = await xbbg.abdh(
            tickers=list(fx_pairs_needed),
            flds="PX_LAST",
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
        fx_nw: nw.DataFrame = nw.from_native(fx_data)
    except (ValueError, TypeError, KeyError):
        logger.warning("Failed to get FX rate data")
        return nw_df.to_native()

    if len(fx_nw) == 0:
        return nw_df.to_native()

    # --- Build FX lookup: {(fx_pair, date) -> rate} ---
    # FX data is also LONG format: {ticker, date, field, value}
    fx_lookup: dict[tuple[str, str], float] = {}
    malformed_fx_rows = 0
    zero_fx_rows = 0
    fx_examples: list[str] = []
    if "ticker" in fx_nw.columns and "date" in fx_nw.columns and "value" in fx_nw.columns:
        for row in fx_nw.iter_rows(named=True):
            pair = row.get("ticker", "")
            dt = row.get("date", "")
            val = row.get("value", "")
            if pair and dt and val:
                try:
                    rate = float(val)
                except (ValueError, TypeError):
                    malformed_fx_rows += 1
                    if len(fx_examples) < 3:
                        fx_examples.append(f"{pair}/{dt}={val!r}")
                    continue
                if rate == 0:
                    zero_fx_rows += 1
                    if len(fx_examples) < 3:
                        fx_examples.append(f"{pair}/{dt}=0")
                    continue
                fx_lookup[(pair, dt)] = rate

    # --- Apply conversion to each row ---
    # LONG format: {ticker, date, field, value} — value is string
    tickers_col = nw_df["ticker"].to_list()
    dates_col = nw_df["date"].to_list()
    values_col = nw_df["value"].to_list()

    new_values = list(values_col)
    unconverted_rows = 0
    unconverted_examples: list[str] = []
    for i, (tk, dt_val, val) in enumerate(zip(tickers_col, dates_col, values_col, strict=False)):
        if tk not in fx_info:
            continue
        info = fx_info[tk]
        fx_pair = info["fx_pair"]
        factor = info["factor"]

        try:
            numeric_val = float(val)
        except (ValueError, TypeError):
            continue  # non-numeric source value — leave as-is silently

        rate = fx_lookup.get((fx_pair, dt_val))
        if rate is None:
            unconverted_rows += 1
            if len(unconverted_examples) < 3:
                unconverted_examples.append(f"{tk}/{dt_val}/{fx_pair}")
            continue

        converted = numeric_val / (rate * factor)
        new_values[i] = str(converted)

    if malformed_fx_rows or zero_fx_rows or unconverted_rows:
        logger.warning(
            "Currency conversion skipped malformed_fx_rows=%s zero_fx_rows=%s unconverted_rows=%s examples=%s",
            malformed_fx_rows,
            zero_fx_rows,
            unconverted_rows,
            fx_examples + unconverted_examples,
        )

    # Replace the value column
    native_ns = nw.get_native_namespace(nw_df)
    new_value_series = nw.new_series("value", new_values, native_namespace=native_ns)
    result = nw_df.with_columns(new_value_series)

    return result.to_native()


convert_ccy = _syncify(aconvert_ccy)
