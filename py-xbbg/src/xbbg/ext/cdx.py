"""CDX index extension functions.

Convenience wrappers for CDX credit default swap index analytics.
Returns DataFrame in the configured backend format.
Uses high-performance Rust utilities from xbbg._core.

Sync functions (wrap async with asyncio.run):
    - cdx_info(): CDX reference metadata
    - cdx_defaults(): Default history
    - cdx_pricing(): Market pricing
    - cdx_risk(): Risk metrics
    - cdx_basis(): Basis analytics
    - cdx_default_prob(): Default probability
    - cdx_cashflows(): Cash flow schedule
    - cdx_curve(): Term structure

Async functions (primary implementation):
    - acdx_info(): Async CDX reference metadata
    - acdx_defaults(): Async default history
    - acdx_pricing(): Async market pricing
    - acdx_risk(): Async risk metrics
    - acdx_basis(): Async basis analytics
    - acdx_default_prob(): Async default probability
    - acdx_cashflows(): Async cash flow schedule
    - acdx_curve(): Async term structure
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from xbbg._services_gen import CDX_INFO_FIELDS, CDX_PRICING_FIELDS, CDX_RISK_FIELDS
from xbbg.ext._utils import _abdp_fields, _abds_field, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame

logger = logging.getLogger(__name__)

__all__ = [
    "acdx_info",
    "acdx_defaults",
    "acdx_pricing",
    "acdx_risk",
    "acdx_basis",
    "acdx_default_prob",
    "acdx_cashflows",
    "acdx_curve",
    "cdx_info",
    "cdx_defaults",
    "cdx_pricing",
    "cdx_risk",
    "cdx_basis",
    "cdx_default_prob",
    "cdx_cashflows",
    "cdx_curve",
]

# CDX_INFO_FIELDS / CDX_PRICING_FIELDS / CDX_RISK_FIELDS are generated from
# defs/bloomberg.toml so every package requests the same bundles.
_CDX_BASIS_FIELDS = [
    "CDS_INDEX_INTRINSIC_VALUE",
    "CDS_INDEX_INTRINSIC_BASIS_VALUE",
    "CDS_IDX_DUR_BASED_INTRINSIC_VAL",
    "CDS_INDEX_DUR_BASED_BASIS_VAL",
    "PX_LAST",
]

_CDX_CURVE_FIELDS = [
    "PX_LAST",
    "UPFRONT_LAST",
    "CDS_FLAT_SPREAD",
    "SW_CNV_BPV",
    "CURRENT_TENOR",
]

_CDX_COMMON_TENORS = ["1Y", "2Y", "3Y", "4Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y"]
_CDX_CURVE_DEFAULT_TENORS = ["3Y", "5Y", "7Y", "10Y"]


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def acdx_info(ticker: str, **kwargs) -> IntoDataFrame:
    """Async CDX reference metadata.

    Retrieves CDX index reference information including series, version,
    on-the-run status, accrual dates, name, and constituent counts.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with CDX reference data. Columns include:
            - ROLLING_SERIES: Series number
            - VERSION: Index version
            - ON_THE_RUN_CURRENT_BD_INDICATOR: On-the-run status
            - CDS_FIRST_ACCRUAL_START_DATE: First accrual date
            - NAME: Index name
            - NUM_CURRENT_COMPANIES_CCY_TKR: Current constituent count
            - NUM_ORIG_COMPANIES_CRNCY_TKR: Original constituent count
            - PX_LAST: Last price

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_info


        async def main():
            df = await acdx_info("CDX IG CDSI GEN 5Y Corp")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=CDX_INFO_FIELDS, **kwargs)


async def acdx_defaults(ticker: str, **kwargs) -> IntoDataFrame:
    """Async CDX default history.

    Retrieves the history of defaults that have occurred in the CDX index.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with default information for constituents that have defaulted.

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_defaults


        async def main():
            df = await acdx_defaults("CDX IG CDSI GEN 5Y Corp")
            print(df)


        asyncio.run(main())
    """
    return await _abds_field(tickers=ticker, field="CDS_INDEX_DEFAULT_INFORMATION", **kwargs)


async def acdx_pricing(ticker: str, *, recovery_rate: float | None = None, **kwargs) -> IntoDataFrame:
    """Async CDX market pricing.

    Retrieves CDX index pricing data including bid/ask spreads, upfront fees,
    and present value of premium and default legs.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        recovery_rate: Optional recovery rate override (0-100). If provided,
            sets CDS_RR override for pricing calculations.
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with CDX pricing data. Columns include:
            - PX_LAST: Last price
            - PX_BID: Bid price
            - PX_ASK: Ask price
            - UPFRONT_LAST: Last upfront fee
            - UPFRONT_BID: Bid upfront fee
            - UPFRONT_ASK: Ask upfront fee
            - CDS_FLAT_SPREAD: Flat spread
            - UPFRONT_FEE: Upfront fee
            - PV_CDS_PREMIUM_LEG: Present value of premium leg
            - PV_CDS_DEFAULT_LEG: Present value of default leg

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_pricing


        async def main():
            # Get pricing with default recovery rate
            df = await acdx_pricing("CDX IG CDSI GEN 5Y Corp")

            # Get pricing with custom recovery rate
            df = await acdx_pricing("CDX IG CDSI GEN 5Y Corp", recovery_rate=40)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    if recovery_rate is not None:
        overrides["CDS_RR"] = str(recovery_rate)

    return await _abdp_fields(tickers=ticker, fields=CDX_PRICING_FIELDS, overrides=overrides, **kwargs)


async def acdx_risk(ticker: str, *, recovery_rate: float | None = None, **kwargs) -> IntoDataFrame:
    """Async CDX risk metrics.

    Retrieves CDX index risk metrics including duration, convexity, and
    sensitivity measures.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        recovery_rate: Optional recovery rate override (0-100). If provided,
            sets CDS_RR override for risk calculations.
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with CDX risk data. Columns include:
            - SW_CNV_BPV: Swap convexity basis point value
            - SW_EQV_BPV: Swap equivalent basis point value
            - CDS_SPREAD_MID_MODIFIED_DURATION: Modified duration
            - CDS_SPREAD_MID_CONVEXITY: Convexity
            - RECOVERY_RATE_SEN: Recovery rate sensitivity
            - CDS_RECOVERY_RT: Recovery rate

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_risk


        async def main():
            # Get risk metrics with default recovery rate
            df = await acdx_risk("CDX IG CDSI GEN 5Y Corp")

            # Get risk metrics with custom recovery rate
            df = await acdx_risk("CDX IG CDSI GEN 5Y Corp", recovery_rate=40)


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}
    if recovery_rate is not None:
        overrides["CDS_RR"] = str(recovery_rate)

    return await _abdp_fields(tickers=ticker, fields=CDX_RISK_FIELDS, overrides=overrides, **kwargs)


async def acdx_basis(ticker: str, **kwargs) -> IntoDataFrame:
    """Async CDX basis analytics.

    Retrieves CDX index basis data including intrinsic value and basis spreads.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with CDX basis data. Columns include:
            - CDS_INDEX_INTRINSIC_VALUE: Intrinsic value
            - CDS_INDEX_INTRINSIC_BASIS_VALUE: Intrinsic basis value
            - CDS_IDX_DUR_BASED_INTRINSIC_VAL: Duration-based intrinsic value
            - CDS_INDEX_DUR_BASED_BASIS_VAL: Duration-based basis value
            - PX_LAST: Last price

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_basis


        async def main():
            df = await acdx_basis("CDX IG CDSI GEN 5Y Corp")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=_CDX_BASIS_FIELDS, **kwargs)


async def acdx_default_prob(ticker: str, **kwargs) -> IntoDataFrame:
    """Async CDX default probability.

    Retrieves implied default probability information for the CDX index.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with default probability data.

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_default_prob


        async def main():
            df = await acdx_default_prob("CDX IG CDSI GEN 5Y Corp")
            print(df)


        asyncio.run(main())
    """
    return await _abds_field(tickers=ticker, field="CDS_DEFAULT_PROB", **kwargs)


async def acdx_cashflows(ticker: str, **kwargs) -> IntoDataFrame:
    """Async CDX cash flow schedule.

    Retrieves the cash flow schedule for the CDX index including premium
    and protection leg payments.

    Args:
        ticker: CDX index ticker (e.g., "CDX IG CDSI GEN 5Y Corp").
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with CDX cash flow schedule data.

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_cashflows


        async def main():
            df = await acdx_cashflows("CDX IG CDSI GEN 5Y Corp")
            print(df)


        asyncio.run(main())
    """
    return await _abds_field(tickers=ticker, field="CASHFLOW_SCHEDULE", **kwargs)


async def acdx_curve(gen_ticker: str, tenors: list[str] | None = None, **kwargs) -> IntoDataFrame:
    """Async CDX term structure curve.

    Retrieves CDX index data across multiple tenors to construct a term
    structure curve. Automatically identifies the tenor token in the generic
    ticker and builds tenor-specific tickers.

    Args:
        gen_ticker: Generic CDX ticker with tenor placeholder
            (e.g., "CDX IG CDSI GEN 5Y Corp"). The function will identify
            which token is the tenor and replace it with requested tenors.
        tenors: List of tenors to retrieve (e.g., ["3Y", "5Y", "7Y", "10Y"]).
            Default: ["3Y", "5Y", "7Y", "10Y"].
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with CDX curve data across tenors. Columns include:
            - PX_LAST: Last price
            - UPFRONT_LAST: Last upfront fee
            - CDS_FLAT_SPREAD: Flat spread
            - SW_CNV_BPV: Swap convexity basis point value
            - CURRENT_TENOR: Tenor

    Example::

        import asyncio
        from xbbg.ext.cdx import acdx_curve


        async def main():
            # Get default tenors (3Y, 5Y, 7Y, 10Y)
            df = await acdx_curve("CDX IG CDSI GEN 5Y Corp")

            # Get custom tenors
            df = await acdx_curve("CDX IG CDSI GEN 5Y Corp", tenors=["2Y", "5Y", "10Y"])


        asyncio.run(main())
    """
    requested_tenors = tenors or _CDX_CURVE_DEFAULT_TENORS
    tokens = gen_ticker.split()

    # Find which token is a tenor
    tenor_idx = next((idx for idx, tok in enumerate(tokens) if tok in _CDX_COMMON_TENORS), None)

    if tenor_idx is None:
        # No tenor found, use generic ticker as-is
        curve_tickers = [gen_ticker]
    else:
        # Build tenor-specific tickers
        curve_tickers = []
        for tenor in requested_tenors:
            tenor_tokens = list(tokens)
            tenor_tokens[tenor_idx] = tenor
            curve_tickers.append(" ".join(tenor_tokens))

    return await _abdp_fields(tickers=curve_tickers, fields=_CDX_CURVE_FIELDS, **kwargs)


cdx_info = _syncify(acdx_info)
cdx_defaults = _syncify(acdx_defaults)
cdx_pricing = _syncify(acdx_pricing)
cdx_risk = _syncify(acdx_risk)
cdx_basis = _syncify(acdx_basis)
cdx_default_prob = _syncify(acdx_default_prob)
cdx_cashflows = _syncify(acdx_cashflows)
cdx_curve = _syncify(acdx_curve)
