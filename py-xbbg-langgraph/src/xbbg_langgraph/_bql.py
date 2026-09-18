"""Validation and native query generation shared by finite BQL recipes."""

from __future__ import annotations

import re
from typing import Any

_FIELD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\(\))?(?:\.[A-Za-z_][A-Za-z0-9_]*(?:\(\))?)*")
_CURRENCY = re.compile(r"[A-Za-z]{3}")


def equity_ticker(value: str) -> str:
    """Require an exact ticker safe for the native builder's quoted literal."""
    if value.startswith("/") or len(value.split()) < 3 or value.split()[-1].lower() != "equity":
        raise ValueError("Supply the exact qualified <TICKER> <EXCHANGE> Equity ticker; resolve identifiers first")
    if any(char in value for char in "'\"\\") or any(ord(char) < 32 for char in value):
        raise ValueError("Recipe tickers cannot contain quotes, backslashes, or control characters")
    return value


def field_expression(value: str) -> str:
    """Accept identifiers and argument-free BQL field access, not query fragments."""
    if not _FIELD.fullmatch(value):
        raise ValueError(
            "Recipe fields must be identifiers or argument-free field access; use xbbg_bql for expressions"
        )
    return value


def currency(value: str) -> str:
    """Require a currency code rather than an interpolated BQL expression."""
    if not _CURRENCY.fullmatch(value):
        raise ValueError("Recipe currency must be a three-letter code")
    return value


def build_query(operation: str, arguments: dict[str, Any], maximum: int) -> str:
    """Use the canonical Rust builder and reject oversized generated queries."""
    from xbbg import _core

    builders = {
        "build_preferreds_query": _core.ext_build_preferreds_query,
        "build_corporate_bonds_query": _core.ext_build_corporate_bonds_query,
        "build_etf_holdings_query": _core.ext_build_etf_holdings_query,
    }
    query = builders[operation](**arguments)
    if len(query) > maximum:
        raise ValueError(f"Generated query exceeds max_bql_query_chars={maximum}; request fewer fields")
    return query
