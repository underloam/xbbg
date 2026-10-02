"""Extension functions for xbbg.

This module contains convenience wrappers built on top of the core API.
These are pure Python functions that compose core operations (bdp, bds, bdh, bql)
for common use cases.

Extension Categories:
    - historical: dividend(), dividend_yield(), earnings(), turnover(), etf_holdings()
    - futures: fut_ticker(), active_futures(), futures_curve(), cdx_ticker(), active_cdx()
    - volatility: vol_surface()
    - currency: convert_ccy()
    - indices: index_members()
    - identifiers: resolve_isins(), issuer_isins()
    - etf: etf_nav_relationships(), etf_nav_snapshot(), etf_nav_history(), subscribe_etf_inav()
    - auction: resolve_venues(), auction_snapshot(), subscribe_auction(), stream_auction()
    - fixed_income: yas(), preferreds(), corporate_bonds(), bqr()
    - bonds: bond_info(), bond_risk(), bond_spreads(), bond_cashflows(), bond_key_rates(), bond_curve()
    - options: option_info(), option_greeks(), option_pricing(), option_chain(), option_chain_bql(), option_screen()
    - cdx: cdx_info(), cdx_defaults(), cdx_pricing(), cdx_risk(), cdx_basis(), cdx_default_prob(), cdx_cashflows(), cdx_curve()

Async versions (primary implementations):
    - historical: adividend(), adividend_yield(), aearnings(), aturnover(), aetf_holdings()
    - futures: afut_ticker(), aactive_futures(), afutures_curve(), acdx_ticker(), aactive_cdx()
    - volatility: avol_surface()
    - currency: aconvert_ccy()
    - indices: aindex_members()
    - identifiers: aresolve_isins(), aissuer_isins()
    - etf: aetf_nav_relationships(), aetf_nav_snapshot(), aetf_nav_history(), asubscribe_etf_inav()
    - auction: aresolve_venues(), aauction_snapshot(), asubscribe_auction(), astream_auction()
    - fixed_income: ayas(), apreferreds(), acorporate_bonds(), abqr()
    - bonds: abond_info(), abond_risk(), abond_spreads(), abond_cashflows(), abond_key_rates(), abond_curve()
    - options: aoption_info(), aoption_greeks(), aoption_pricing(), aoption_chain(), aoption_chain_bql(), aoption_screen()
    - cdx: acdx_info(), acdx_defaults(), acdx_pricing(), acdx_risk(), acdx_basis(), acdx_default_prob(), acdx_cashflows(), acdx_curve()

Example::

    from xbbg import ext

    # Get dividend history
    df = ext.dividend("AAPL US Equity")

    # Get ETF holdings
    df = ext.etf_holdings("SPY US Equity")

    # Resolve futures ticker
    ticker = ext.fut_ticker("ES1 Index", "2024-01-15")

    # Convert currency
    df_usd = ext.convert_ccy(df, ccy="USD")

    # Yield & spread analysis for bonds
    df = ext.yas("US912810TM69 Govt", "YAS_BOND_YLD")

    # Find preferred stocks
    df = ext.preferreds("BAC US Equity")

    # Find corporate bonds
    df = ext.corporate_bonds("AAPL")

    # Bond analytics
    df = ext.bond_info("T 4.5 05/15/38 Govt")
    df = ext.bond_risk("T 4.5 05/15/38 Govt")
    df = ext.bond_spreads("T 4.5 05/15/38 Govt")

    # Options analytics
    df = ext.option_info("AAPL US 01/17/25 C200 Equity")
    df = ext.option_greeks("AAPL US 01/17/25 C200 Equity")
    chain = ext.option_chain("AAPL US Equity")

    # CDX analytics
    df = ext.cdx_info("CDX IG CDSI GEN 5Y Corp")
    df = ext.cdx_pricing("CDX IG CDSI GEN 5Y Corp")

    # Async example
    import asyncio


    async def main():
        df = await ext.adividend("AAPL US Equity")
        print(df)


    asyncio.run(main())
"""

from __future__ import annotations

# ruff: noqa: E402  # guarded optional native imports require helper definitions first
import logging

logger = logging.getLogger(__name__)


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
    return (
        error.name == "xbbg._core"
        or "No module named 'xbbg._core'" in message
        or ("xbbg._core" in message and "cannot import name 'ext_" in message)
        or native_loader_error
    )


class _UnavailableExtension:
    def __init__(self, name: str, source: ImportError):
        self.__name__ = name
        self._source = source

    def __call__(self, *_args, **_kwargs):
        raise ImportError(f"xbbg.ext.{self.__name__} requires xbbg._core native helpers") from self._source

    def __getattr__(self, attr: str):
        raise ImportError(f"xbbg.ext.{self.__name__}.{attr} requires xbbg._core native helpers") from self._source


def _bind_unavailable(names: tuple[str, ...], source: ImportError) -> None:
    logger.debug("Skipping native-dependent extension exports", exc_info=True)
    globals().update({name: _UnavailableExtension(name, source) for name in names})


# Sync functions
# Async functions
from xbbg.ext.bonds import (
    abond_cashflows,
    abond_curve,
    abond_info,
    abond_key_rates,
    abond_risk,
    abond_spreads,
    bond_cashflows,
    bond_curve,
    bond_info,
    bond_key_rates,
    bond_risk,
    bond_spreads,
)
from xbbg.ext.cdx import (
    acdx_basis,
    acdx_cashflows,
    acdx_curve,
    acdx_default_prob,
    acdx_defaults,
    acdx_info,
    acdx_pricing,
    acdx_risk,
    cdx_basis,
    cdx_cashflows,
    cdx_curve,
    cdx_default_prob,
    cdx_defaults,
    cdx_info,
    cdx_pricing,
    cdx_risk,
)
from xbbg.ext.currency import aconvert_ccy, convert_ccy
from xbbg.ext.etf import (
    aetf_nav_history,
    aetf_nav_relationships,
    aetf_nav_snapshot,
    asubscribe_etf_inav,
    etf_nav_history,
    etf_nav_relationships,
    etf_nav_snapshot,
    subscribe_etf_inav,
)
from xbbg.ext.identifiers import aissuer_isins, aresolve_isins, issuer_isins, resolve_isins
from xbbg.ext.indices import aindex_members, index_members

try:
    from xbbg.ext.auction import (
        AUCTION,
        aauction_snapshot,
        aresolve_venues,
        astream_auction,
        asubscribe_auction,
        auction_snapshot,
        imbalance_side,
        resolve_venues,
        stream_auction,
        subscribe_auction,
    )
except ImportError as exc:
    if not _is_native_import_error(exc):
        raise
    _bind_unavailable(
        (
            "AUCTION",
            "aauction_snapshot",
            "aresolve_venues",
            "astream_auction",
            "asubscribe_auction",
            "auction_snapshot",
            "imbalance_side",
            "resolve_venues",
            "stream_auction",
            "subscribe_auction",
        ),
        exc,
    )

try:
    from xbbg.ext.fixed_income import (
        YieldType,
        abqr,
        acorporate_bonds,
        apreferreds,
        ayas,
        bqr,
        corporate_bonds,
        preferreds,
        yas,
    )
except ImportError as exc:
    if not _is_native_import_error(exc):
        raise
    _bind_unavailable(
        (
            "YieldType",
            "abqr",
            "acorporate_bonds",
            "apreferreds",
            "ayas",
            "bqr",
            "corporate_bonds",
            "preferreds",
            "yas",
        ),
        exc,
    )

try:
    from xbbg.ext.futures import (
        aactive_cdx,
        aactive_futures,
        acdx_ticker,
        active_cdx,
        active_futures,
        afut_ticker,
        afutures_curve,
        cdx_ticker,
        fut_ticker,
        futures_curve,
    )
except ImportError as exc:
    if not _is_native_import_error(exc):
        raise
    _bind_unavailable(
        (
            "aactive_cdx",
            "aactive_futures",
            "acdx_ticker",
            "active_cdx",
            "active_futures",
            "afut_ticker",
            "afutures_curve",
            "cdx_ticker",
            "fut_ticker",
            "futures_curve",
        ),
        exc,
    )
from xbbg.ext.historical import (
    adividend,
    adividend_yield,
    aearnings,
    aetf_holdings,
    aturnover,
    dividend,
    dividend_yield,
    earnings,
    etf_holdings,
    turnover,
)
from xbbg.ext.options import (
    ChainPeriodicity,
    ExerciseType,
    ExpiryMatch,
    PutCall,
    StrikeRef,
    aoption_chain,
    aoption_chain_bql,
    aoption_greeks,
    aoption_info,
    aoption_pricing,
    aoption_screen,
    option_chain,
    option_chain_bql,
    option_greeks,
    option_info,
    option_pricing,
    option_screen,
)
from xbbg.ext.volatility import VolSurfacePreset, avol_surface, vol_surface

__all__ = [
    # Exchange-auction helpers and recipes
    "AUCTION",
    "imbalance_side",
    "resolve_venues",
    "aresolve_venues",
    "auction_snapshot",
    "aauction_snapshot",
    "subscribe_auction",
    "asubscribe_auction",
    "stream_auction",
    "astream_auction",
    # Historical extensions (sync)
    "dividend",
    "earnings",
    "turnover",
    "etf_holdings",
    "dividend_yield",
    # Historical extensions (async)
    "adividend",
    "aearnings",
    "aturnover",
    "aetf_holdings",
    "adividend_yield",
    # Futures extensions (sync)
    "fut_ticker",
    "active_futures",
    "futures_curve",
    "cdx_ticker",
    "active_cdx",
    # Futures extensions (async)
    "afut_ticker",
    "aactive_futures",
    "afutures_curve",
    "acdx_ticker",
    "aactive_cdx",
    # Currency extensions (sync)
    "convert_ccy",
    # Currency extensions (async)
    "aconvert_ccy",
    # Volatility extensions
    "VolSurfacePreset",
    "vol_surface",
    "avol_surface",
    # Index extensions
    "index_members",
    "aindex_members",
    # Identifier extensions
    "resolve_isins",
    "aresolve_isins",
    "issuer_isins",
    "aissuer_isins",
    # ETF NAV extensions (sync)
    "etf_nav_relationships",
    "etf_nav_snapshot",
    "etf_nav_history",
    "subscribe_etf_inav",
    # ETF NAV extensions (async)
    "aetf_nav_relationships",
    "aetf_nav_snapshot",
    "aetf_nav_history",
    "asubscribe_etf_inav",
    # Fixed income extensions (sync)
    "yas",
    "YieldType",
    "preferreds",
    "corporate_bonds",
    "bqr",
    # Fixed income extensions (async)
    "ayas",
    "apreferreds",
    "acorporate_bonds",
    "abqr",
    # Bond analytics (sync)
    "bond_info",
    "bond_risk",
    "bond_spreads",
    "bond_cashflows",
    "bond_key_rates",
    "bond_curve",
    # Bond analytics (async)
    "abond_info",
    "abond_risk",
    "abond_spreads",
    "abond_cashflows",
    "abond_key_rates",
    "abond_curve",
    # Options analytics enums
    "PutCall",
    "ChainPeriodicity",
    "StrikeRef",
    "ExerciseType",
    "ExpiryMatch",
    # Options analytics (sync)
    "option_info",
    "option_greeks",
    "option_pricing",
    "option_chain",
    "option_chain_bql",
    "option_screen",
    # Options analytics (async)
    "aoption_info",
    "aoption_greeks",
    "aoption_pricing",
    "aoption_chain",
    "aoption_chain_bql",
    "aoption_screen",
    # CDX analytics (sync)
    "cdx_info",
    "cdx_defaults",
    "cdx_pricing",
    "cdx_risk",
    "cdx_basis",
    "cdx_default_prob",
    "cdx_cashflows",
    "cdx_curve",
    # CDX analytics (async)
    "acdx_info",
    "acdx_defaults",
    "acdx_pricing",
    "acdx_risk",
    "acdx_basis",
    "acdx_default_prob",
    "acdx_cashflows",
    "acdx_curve",
]
