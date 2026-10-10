"""Historical data recipes implemented by the native Rust engine.

The async adapters normalize Python dates and ticker containers, then dispatch
one native recipe. Sync functions use the same implementation. Results use the
configured backend and native column names, types, and validation rules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


async def adividend(
    tickers: str | list[str],
    typ: str = "all",
    *,
    start_date: DateLike = None,
    end_date: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Get dividend or split bulk data through the native recipe.

    Args:
        tickers: Single ticker or list; securities are not filtered to equities.
        typ: Dividend alias (all, dvd, split, gross, adjust, adj_fund, with_amt,
            dvd_amt, gross_amt, projected), or a Bloomberg bulk field name.
        start_date: Optional dividend-history start date.
        end_date: Optional dividend-history end date.
        **kwargs: Native request options and Bloomberg overrides, plus backend.
            The adjust alias supplies the standard corporate-actions filter
            unless overridden by the caller.

    Returns:
        Bulk rows with native Bloomberg sub-field labels (for example,
        Declared Date and Dividend Amount), not the former Python aliases.
        The backend defaults to the configured backend.
    """
    return await _call_native_recipe(
        "recipe_dividend",
        [tickers] if isinstance(tickers, str) else list(tickers),
        _fmt_date(start_date) or "",
        _fmt_date(end_date) or "",
        typ,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


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
    """Get earnings bulk data with native hierarchical percentages.

    Args:
        ticker: Single Bloomberg ticker.
        by: Geo or Product breakdown, or Q/A period granularity. Matching is
            case-insensitive; other values are rejected by the native recipe.
        typ: Revenue, Operating_Income, Assets, Gross_Profit,
            Capital_Expenditures, or IS/BS/CF. An optional PG_ prefix is accepted.
        ccy: Optional currency override for the data request.
        level: Optional hierarchy level, restricted to 1 or 2.
        year: Fiscal year override; zero omits the override.
        periods: Number of periods; zero omits the override.
        **kwargs: Native request options and Bloomberg overrides, plus backend.
            Options apply to both the header and data requests. Bulk sub-field
            shape is retained for the header join and percentage calculation.

    Returns:
        Native bulk rows with normalized period labels and adjacent percentage
        columns. Level matching is case-insensitive. Percentages can also be
        calculated from unrenamed Period N Value columns when headers are absent.
        Header punctuation and duplicate labels follow the native rename rules.
        Comma-separated numbers and integral numeric level strings are accepted;
        fractional levels are not truncated. All-null or nonnumeric periods do
        not gain percentage columns, and existing percentage columns are kept.
    """
    return await _call_native_recipe(
        "recipe_earning",
        [ticker],
        by,
        typ,
        ccy,
        level,
        year,
        periods,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


async def aturnover(
    tickers: str | list[str],
    *,
    start_date: DateLike = None,
    end_date: DateLike = None,
    ccy: str = "USD",
    factor: float = 1e6,
    **kwargs,
) -> IntoDataFrame:
    """Get turnover, with volume times VWAP fallback and currency conversion.

    Args:
        tickers: Single ticker or list of tickers.
        start_date: Defaults in Rust to 30 days before end_date.
        end_date: Defaults in Rust to yesterday.
        ccy: Target currency (default USD); local disables conversion.
        factor: Division factor (default 1e6 for millions); must be finite and
            nonzero. Scaling is applied after currency conversion.
        **kwargs: Native request options and Bloomberg overrides, plus backend.
            Requested long, wide, typed, or metadata output is shaped after
            calculations; internal requests always use long format.
            Dts/Dates/show_date hide or show dates; DtFmt/DateFormat/date_format
            select Date, Periodic, or Both labels using the requested periodicity.
            Sort/sort selects ascending or descending dates within each ticker.
            Orientation/Direction/Dir/orientation selects horizontal (wide) or
            vertical (long) output unless format is explicit. These display
            controls apply after calculations and are not Bloomberg overrides.
            Canonical keys take precedence if conflicting aliases are supplied.

    Returns:
        Native historical rows with Date32 dates unless display controls replace
        or hide them, using TURNOVER for direct and fallback values. Default long
        values are Rust-formatted strings;
        numeric long inputs stay numeric and computed wide/typed values are
        Float64. Malformed direct numeric text becomes null. Missing or malformed
        fallback pairs are omitted; malformed values are logged by Rust. Fallback
        dates are sorted within each missing ticker, and duplicate missing tickers
        are requested once. Request failures propagate rather than silently
        returning partial fallback data. Missing FX rates yield null values.
    """
    return await _call_native_recipe(
        "recipe_turnover",
        [tickers] if isinstance(tickers, str) else list(tickers),
        _fmt_date(start_date) or "",
        _fmt_date(end_date) or "",
        ccy,
        factor,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


async def aetf_holdings(
    etf_ticker: str,
    *,
    fields: list[str] | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Get ETF holdings using the native BQL recipe.

    Args:
        etf_ticker: ETF ticker; a bare ticker receives the US Equity suffix.
        fields: Extra fields appended to id_isin, weights, and id().position.
            Duplicate fields are removed by the native query builder.
        **kwargs: Native BQL request options, plus backend.

    Returns:
        Native BQL columns in the selected backend. No Python aliases such as
        holding, position, or the former holdings-column rename map are applied.
    """
    return await _call_native_recipe(
        "recipe_etf_holdings",
        etf_ticker,
        list(fields) if fields is not None else None,
        backend=kwargs.pop("backend", None),
        request_options=kwargs,
    )


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
