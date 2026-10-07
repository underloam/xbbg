"""Bond analytics extension functions.

Convenience wrappers for bond analytics and risk analysis queries.
Returns DataFrame in the configured backend format.
Uses high-performance Rust utilities from xbbg._core.

Sync functions (wrap async with asyncio.run):
    - bond_info(): Bond reference metadata
    - bond_risk(): Duration and risk metrics
    - bond_spreads(): Spread analytics
    - bond_cashflows(): Cash flow schedule
    - bond_key_rates(): Key rate durations
    - bond_curve(): Relative value comparison

Async functions (primary implementation):
    - abond_info(): Async bond reference metadata
    - abond_risk(): Async duration and risk metrics
    - abond_spreads(): Async spread analytics
    - abond_cashflows(): Async cash flow schedule
    - abond_key_rates(): Async key rate durations
    - abond_curve(): Async relative value comparison
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from xbbg._dates import DateLike
from xbbg.ext._utils import (
    _abdp_fields,
    _abds_field,
    _apply_settle_override,
    _syncify,
)

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame

logger = logging.getLogger(__name__)

# Field constants
_BOND_INFO_FIELDS = [
    "NAME",
    "SECURITY_DES",
    "CRNCY",
    "CPN_TYP",
    "CPN",
    "MATURITY",
    "PAR_AMT",
    "AMT_OUTSTANDING",
    "ISSUE_DT",
    "COUNTRY_ISO",
    "INDUSTRY_SECTOR",
    "BB_COMPOSITE",
    "RTG_SP",
    "RTG_MOODY",
    "RTG_FITCH",
    "CALLABLE",
    "PUTABLE",
    "SINKABLE",
]

_BOND_RISK_FIELDS = [
    "DUR_ADJ_MID",
    "DUR_MID",
    "CNVX_MID",
    "DUR_ADJ_OAS_MID",
    "OAS_SPREAD_DUR_MID",
    "CNVX_OAS_MID",
    "RISK_MID",
    "RISK_OAS_MID",
    "YAS_MOD_DUR",
    "YAS_RISK",
]

_BOND_SPREAD_FIELDS = [
    "YAS_OAS_SPRD",
    "YAS_ZSPREAD",
    "YAS_ISPREAD",
    "YAS_ASW_SPREAD",
    "YAS_ISPREAD_TO_GOVT",
    "YAS_YLD_SPREAD",
    "Z_SPRD_MID",
    "OAS_SPREAD_MID",
    "YAS_BNCHMRK_BOND",
    "YAS_BNCHMRK_BOND_YLD",
]

_FLD_DES_CASH_FLOW = "DES_CASH_FLOW"

_BOND_KEY_RATE_FIELDS = [
    "KEY_RATE_DUR_1YR",
    "KEY_RATE_DUR_2YR",
    "KEY_RATE_DUR_3YR",
    "KEY_RATE_DUR_4YR",
    "KEY_RATE_DUR_5YR",
    "KEY_RATE_DUR_7YR",
    "KEY_RATE_DUR_8YR",
    "KEY_RATE_DUR_9YR",
    "KEY_RATE_DUR_10YR",
    "KEY_RATE_DUR_15YR",
    "KEY_RATE_DUR_20YR",
    "KEY_RATE_DUR_25YR",
    "KEY_RATE_DUR_30YR",
    "KEY_RATE_RISK_1Y",
    "KEY_RATE_RISK_2Y",
    "KEY_RATE_RISK_3Y",
    "KEY_RATE_RISK_5Y",
    "KEY_RATE_RISK_7Y",
    "KEY_RATE_RISK_10Y",
    "KEY_RATE_RISK_20Y",
    "KEY_RATE_RISK_30Y",
]

_BOND_CURVE_DEFAULT_FIELDS = [
    "YAS_BOND_YLD",
    "YAS_MOD_DUR",
    "YAS_RISK",
    "YAS_ZSPREAD",
    "YAS_OAS_SPRD",
    "DUR_ADJ_MID",
    "CNVX_MID",
]

__all__ = [
    "abond_info",
    "abond_risk",
    "abond_spreads",
    "abond_cashflows",
    "abond_key_rates",
    "abond_curve",
    "bond_info",
    "bond_risk",
    "bond_spreads",
    "bond_cashflows",
    "bond_key_rates",
    "bond_curve",
]

# =============================================================================
# Async implementations (primary)
# =============================================================================


async def abond_info(ticker: str, **kwargs) -> IntoDataFrame:
    """Async bond reference metadata.

    Retrieves bond reference data including name, currency, coupon type,
    maturity, par amount, ratings, and other identifying information.

    Args:
        ticker: Bond ticker (e.g., "T 4.5 05/15/38 Govt" or ISIN format).
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with bond reference data. Columns include:
            - NAME: Bond name
            - SECURITY_DES: Security description
            - CRNCY: Currency
            - CPN_TYP: Coupon type
            - CPN: Coupon rate
            - MATURITY: Maturity date
            - PAR_AMT: Par amount
            - AMT_OUTSTANDING: Amount outstanding
            - ISSUE_DT: Issue date
            - COUNTRY_ISO: Country ISO code
            - INDUSTRY_SECTOR: Industry sector
            - BB_COMPOSITE: Bloomberg composite rating
            - RTG_SP: S&P rating
            - RTG_MOODY: Moody's rating
            - RTG_FITCH: Fitch rating
            - CALLABLE: Callable flag
            - PUTABLE: Putable flag
            - SINKABLE: Sinkable flag

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_info


        async def main():
            df = await abond_info("T 4.5 05/15/38 Govt")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=_BOND_INFO_FIELDS, **kwargs)


async def abond_risk(
    ticker: str,
    *,
    settle_dt: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Async bond duration and risk metrics.

    Retrieves bond risk metrics including modified duration, Macaulay duration,
    convexity, and DV01 (dollar value of 1 basis point).

    Args:
        ticker: Bond ticker (e.g., "T 4.5 05/15/38 Govt" or ISIN format).
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with bond risk metrics. Columns include:
            - DUR_ADJ_MID: Adjusted duration (mid)
            - DUR_MID: Macaulay duration (mid)
            - CNVX_MID: Convexity (mid)
            - DUR_ADJ_OAS_MID: Adjusted duration OAS (mid)
            - OAS_SPREAD_DUR_MID: OAS spread duration (mid)
            - CNVX_OAS_MID: Convexity OAS (mid)
            - RISK_MID: DV01 (mid)
            - RISK_OAS_MID: DV01 OAS (mid)
            - YAS_MOD_DUR: YAS modified duration
            - YAS_RISK: YAS DV01

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_risk


        async def main():
            df = await abond_risk("T 4.5 05/15/38 Govt")
            print(df)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    _apply_settle_override(overrides, settle_dt)

    return await _abdp_fields(tickers=ticker, fields=_BOND_RISK_FIELDS, overrides=overrides, **kwargs)


async def abond_spreads(
    ticker: str,
    *,
    settle_dt: DateLike = None,
    benchmark: str | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async bond spread analytics.

    Retrieves bond spread metrics including OAS, Z-spread, I-spread,
    and asset swap spread.

    Args:
        ticker: Bond ticker (e.g., "T 4.5 05/15/38 Govt" or ISIN format).
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        benchmark: Benchmark security for spread calculation (e.g., "T 4.5 05/15/38 Govt").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with bond spread metrics. Columns include:
            - YAS_OAS_SPRD: Option-adjusted spread
            - YAS_ZSPREAD: Z-spread
            - YAS_ISPREAD: I-spread
            - YAS_ASW_SPREAD: Asset swap spread
            - YAS_ISPREAD_TO_GOVT: I-spread to government
            - YAS_YLD_SPREAD: Yield spread
            - Z_SPRD_MID: Z-spread (mid)
            - OAS_SPREAD_MID: OAS spread (mid)
            - YAS_BNCHMRK_BOND: Benchmark bond
            - YAS_BNCHMRK_BOND_YLD: Benchmark bond yield

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_spreads


        async def main():
            df = await abond_spreads("T 4.5 05/15/38 Govt")
            print(df)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    _apply_settle_override(overrides, settle_dt)
    if benchmark is not None:
        overrides["YAS_BNCHMRK_BOND"] = benchmark

    return await _abdp_fields(tickers=ticker, fields=_BOND_SPREAD_FIELDS, overrides=overrides, **kwargs)


async def abond_cashflows(
    ticker: str,
    *,
    settle_dt: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Async bond cash flow schedule.

    Retrieves the bond's cash flow schedule including coupon and principal payments.

    Args:
        ticker: Bond ticker (e.g., "T 4.5 05/15/38 Govt" or ISIN format).
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with bond cash flows. Columns include:
            - payment_date: Payment date
            - coupon_amount: Coupon payment amount
            - principal_amount: Principal payment amount

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_cashflows


        async def main():
            df = await abond_cashflows("T 4.5 05/15/38 Govt")
            print(df)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    _apply_settle_override(overrides, settle_dt)

    return await _abds_field(tickers=ticker, field=_FLD_DES_CASH_FLOW, overrides=overrides, **kwargs)


async def abond_key_rates(
    ticker: str,
    *,
    settle_dt: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Async bond key rate durations.

    Retrieves key rate durations and key rate DV01s for the bond across
    the yield curve (1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y, 30Y).

    Args:
        ticker: Bond ticker (e.g., "T 4.5 05/15/38 Govt" or ISIN format).
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with key rate durations. Columns include:
            - KEY_RATE_DUR_1YR: Key rate duration at 1Y
            - KEY_RATE_DUR_2YR: Key rate duration at 2Y
            - KEY_RATE_DUR_3YR: Key rate duration at 3Y
            - KEY_RATE_DUR_4YR: Key rate duration at 4Y
            - KEY_RATE_DUR_5YR: Key rate duration at 5Y
            - KEY_RATE_DUR_7YR: Key rate duration at 7Y
            - KEY_RATE_DUR_8YR: Key rate duration at 8Y
            - KEY_RATE_DUR_9YR: Key rate duration at 9Y
            - KEY_RATE_DUR_10YR: Key rate duration at 10Y
            - KEY_RATE_DUR_15YR: Key rate duration at 15Y
            - KEY_RATE_DUR_20YR: Key rate duration at 20Y
            - KEY_RATE_DUR_25YR: Key rate duration at 25Y
            - KEY_RATE_DUR_30YR: Key rate duration at 30Y
            - KEY_RATE_RISK_1Y: Key rate DV01 at 1Y
            - KEY_RATE_RISK_2Y: Key rate DV01 at 2Y
            - KEY_RATE_RISK_3Y: Key rate DV01 at 3Y
            - KEY_RATE_RISK_5Y: Key rate DV01 at 5Y
            - KEY_RATE_RISK_7Y: Key rate DV01 at 7Y
            - KEY_RATE_RISK_10Y: Key rate DV01 at 10Y
            - KEY_RATE_RISK_20Y: Key rate DV01 at 20Y
            - KEY_RATE_RISK_30Y: Key rate DV01 at 30Y

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_key_rates


        async def main():
            df = await abond_key_rates("T 4.5 05/15/38 Govt")
            print(df)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    _apply_settle_override(overrides, settle_dt)

    return await _abdp_fields(tickers=ticker, fields=_BOND_KEY_RATE_FIELDS, overrides=overrides, **kwargs)


async def abond_curve(
    tickers: list[str],
    flds: list[str] | None = None,
    *,
    settle_dt: DateLike = None,
    **kwargs,
) -> IntoDataFrame:
    """Async bond curve and relative value comparison.

    Retrieves bond analytics for multiple bonds to compare relative value
    across the curve.

    Args:
        tickers: List of bond tickers (e.g., ["T 2.5 05/15/25 Govt", "T 4.5 05/15/38 Govt"]).
        flds: Fields to retrieve. Default: YAS_BOND_YLD, YAS_MOD_DUR, YAS_RISK,
            YAS_ZSPREAD, YAS_OAS_SPRD, DUR_ADJ_MID, CNVX_MID.
        settle_dt: Settlement date for the calculation. Default: spot settlement.
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with bond curve analytics. Columns depend on requested fields
        but typically include:
            - YAS_BOND_YLD: Yield
            - YAS_MOD_DUR: Modified duration
            - YAS_RISK: DV01
            - YAS_ZSPREAD: Z-spread
            - YAS_OAS_SPRD: OAS spread
            - DUR_ADJ_MID: Adjusted duration
            - CNVX_MID: Convexity

    Example::

        import asyncio
        from xbbg.ext.bonds import abond_curve


        async def main():
            df = await abond_curve(
                [
                    "T 2.5 05/15/25 Govt",
                    "T 4.5 05/15/38 Govt",
                ]
            )
            print(df)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    _apply_settle_override(overrides, settle_dt)

    return await _abdp_fields(
        tickers=tickers,
        fields=flds or _BOND_CURVE_DEFAULT_FIELDS,
        overrides=overrides,
        **kwargs,
    )


bond_info = _syncify(abond_info)
bond_risk = _syncify(abond_risk)
bond_spreads = _syncify(abond_spreads)
bond_cashflows = _syncify(abond_cashflows)
bond_key_rates = _syncify(abond_key_rates)
bond_curve = _syncify(abond_curve)
