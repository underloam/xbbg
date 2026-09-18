"""LangChain/LangGraph Bloomberg tools backed by Python xbbg."""

from __future__ import annotations

from typing import Any

from langchain_core.tools import StructuredTool

from .core_tools import (
    BLOOMBERG_CORE_TOOL_NAMES,
    create_bdh_tool,
    create_bdib_tool,
    create_bdp_tool,
    create_bds_tool,
    create_bdtick_tool,
    create_beqs_tool,
    create_bflds_tool,
    create_bloomberg_tools,
    create_bql_tool,
    create_bqr_tool,
    create_bsrch_tool,
    create_check_entitlements_tool,
    create_corporate_bonds_tool,
    create_depth_snapshot_tool,
    create_etf_holdings_tool,
    create_index_members_tool,
    create_issuer_isins_tool,
    create_mktbar_snapshot_tool,
    create_preferreds_tool,
    create_resolve_isins_tool,
    create_stream_snapshot_tool,
    create_yas_tool,
)
from .ext_tools import (
    BLOOMBERG_EXT_TOOL_NAMES,
    create_bloomberg_ext_tools,
    create_ext_bql_builder_tool,
    create_ext_calculate_tool,
    create_ext_cdx_tool,
    create_ext_chart_spec_tool,
    create_ext_columns_tool,
    create_ext_constants_tool,
    create_ext_currency_tool,
    create_ext_futures_tool,
    create_ext_market_session_tool,
    create_ext_ticker_tool,
    create_ext_yas_overrides_tool,
)
from .instructions import BLOOMBERG_TOOL_INSTRUCTIONS, get_bloomberg_tool_instructions
from .options import BLOOMBERG_TOOL_NAMES, BloombergToolsOptions, resolve_options

__all__ = [
    "BLOOMBERG_CORE_TOOL_NAMES",
    "BLOOMBERG_EXT_TOOL_NAMES",
    "BLOOMBERG_TOOL_NAMES",
    "BLOOMBERG_TOOL_INSTRUCTIONS",
    "BloombergToolsOptions",
    "create_all_bloomberg_tools",
    "create_bloomberg_tools",
    "create_bloomberg_ext_tools",
    "get_bloomberg_tool_instructions",
    "create_bdp_tool",
    "create_bdh_tool",
    "create_bds_tool",
    "create_bdib_tool",
    "create_bdtick_tool",
    "create_check_entitlements_tool",
    "create_bql_tool",
    "create_bsrch_tool",
    "create_bqr_tool",
    "create_bflds_tool",
    "create_beqs_tool",
    "create_yas_tool",
    "create_preferreds_tool",
    "create_corporate_bonds_tool",
    "create_index_members_tool",
    "create_resolve_isins_tool",
    "create_issuer_isins_tool",
    "create_etf_holdings_tool",
    "create_stream_snapshot_tool",
    "create_mktbar_snapshot_tool",
    "create_depth_snapshot_tool",
    "create_ext_ticker_tool",
    "create_ext_futures_tool",
    "create_ext_cdx_tool",
    "create_ext_currency_tool",
    "create_ext_bql_builder_tool",
    "create_ext_chart_spec_tool",
    "create_ext_market_session_tool",
    "create_ext_yas_overrides_tool",
    "create_ext_constants_tool",
    "create_ext_columns_tool",
    "create_ext_calculate_tool",
]


def create_all_bloomberg_tools(options: BloombergToolsOptions | None = None, **kwargs: Any) -> list[StructuredTool]:
    """Create request and extension tools sharing the same immutable limits."""
    resolved = resolve_options(options, kwargs)
    return [*create_bloomberg_tools(resolved), *create_bloomberg_ext_tools(resolved)]
