"""Shared generated vocabulary and bounded request contracts."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import get_args
from unittest.mock import AsyncMock

from pydantic import ValidationError
import pytest

from xbbg_langgraph import (
    BLOOMBERG_TOOL_NAMES,
    BloombergToolsOptions,
    _defs_gen as defs,
    create_all_bloomberg_tools,
    create_bdh_tool,
    create_bdp_tool,
    create_bds_tool,
    create_ext_cdx_tool,
)


def test_generated_tool_names_and_option_defaults_match_runtime():
    assert len(BLOOMBERG_TOOL_NAMES) == len(set(BLOOMBERG_TOOL_NAMES)) == 34
    assert set(BLOOMBERG_TOOL_NAMES) == {tool.name for tool in create_all_bloomberg_tools()}
    assert (
        BLOOMBERG_TOOL_NAMES.index("xbbg_issuer_isins")
        < BLOOMBERG_TOOL_NAMES.index("xbbg_resolve_venues")
        < BLOOMBERG_TOOL_NAMES.index("xbbg_auction_snapshot")
    )
    options = BloombergToolsOptions()
    for name in type(options).model_fields:
        if name.startswith("max_"):
            assert getattr(options, name) == getattr(defs, f"DEFAULT_{name.upper()}")
    assert get_args(defs.ReferenceFormat) == defs.REFERENCE_FORMATS
    assert get_args(defs.HistoricalFormat) == defs.HISTORICAL_FORMATS
    assert get_args(defs.OverflowPolicy) == defs.OVERFLOW_POLICIES
    assert get_args(defs.ChartSource) == defs.CHART_SOURCES
    assert get_args(defs.ChartKind) == defs.CHART_KINDS
    assert set(defs.CHART_DEFAULTS.values()) <= set(defs.CHART_KINDS)


@pytest.mark.parametrize(
    "factory,arguments",
    [
        (create_bdp_tool, {"securities": ["SYNTH"], "fields": ["PX_LAST"]}),
        (
            create_bdh_tool,
            {"securities": ["SYNTH"], "fields": ["PX_LAST"], "start": "20240101", "end": "20240102"},
        ),
        (create_bds_tool, {"securities": ["SYNTH"], "field": "MEMBERS"}),
    ],
)
def test_reference_requests_share_bounded_maps(factory, arguments):
    schema = factory(max_fields=1, max_securities=1, max_string_chars=8).args_schema
    for maps in [
        {"kwargs": {"A": 1, "B": 2}},
        {"kwargs": {"TOOLONGKEY": 1}},
        {"kwargs": {"A": "123456789"}},
        {"kwargs": {" A ": 1, "A": 2}},
        {"overrides": {"A": 1, "B": 2, "C": 3}},
        {"overrides": {"SYNTH": {"A": 1, "B": 2}}},
        {"overrides": {"SYNTH": {"A": "123456789"}}},
        {"overrides": {"SYNTH": {"TOOLONGKEY": 1}}},
    ]:
        with pytest.raises(ValidationError):
            schema.model_validate({**arguments, **maps})
    validated = schema.model_validate(
        {**arguments, "kwargs": {" FLAG ": " yes "}, "overrides": {"GLOBAL": False, "SYNTH": {"PX_LAST": " value "}}}
    )
    assert validated.kwargs == {"FLAG": "yes"}
    assert validated.overrides == {"GLOBAL": False, "SYNTH": {"PX_LAST": "value"}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,fields",
    [
        ("cdx_info", defs.CDX_INFO_FIELDS),
        ("cdx_pricing", defs.CDX_PRICING_FIELDS),
        ("cdx_risk", defs.CDX_RISK_FIELDS),
    ],
)
async def test_cdx_bundles_need_no_private_core_constants(monkeypatch, operation, fields):
    # Only public recipes are present: the adapter must own its generated metadata.
    package = ModuleType("xbbg")
    extension = ModuleType("xbbg.ext")
    cdx = ModuleType("xbbg.ext.cdx")
    recipes = {name: AsyncMock(return_value=[]) for name in ("cdx_info", "cdx_pricing", "cdx_risk")}
    for name, recipe in recipes.items():
        setattr(cdx, f"a{name}", recipe)
    extension.cdx = cdx
    package.ext = extension
    monkeypatch.setitem(sys.modules, "xbbg", package)
    monkeypatch.setitem(sys.modules, "xbbg.ext", extension)
    monkeypatch.setitem(sys.modules, "xbbg.ext.cdx", cdx)
    args = {"operation": operation, "ticker": "SYNTH CDSI GEN 5Y Corp"}
    with pytest.raises(ValueError, match="max_fields"):
        await create_ext_cdx_tool(max_fields=len(fields) - 1).ainvoke(args)
    recipes[operation].assert_not_awaited()
    for validate_fields in (True, False, None):
        await create_ext_cdx_tool(max_fields=len(fields), validate_fields=validate_fields).ainvoke(args)
        assert recipes[operation].await_args.kwargs["validate_fields"] is validate_fields
        assert recipes[operation].await_args.kwargs["backend"] == "native"


@pytest.mark.parametrize("recovery_rate", [0, 40, 100])
def test_cdx_recovery_rate_is_percent(recovery_rate):
    schema = create_ext_cdx_tool().args_schema
    validated = schema.model_validate(
        {"operation": "cdx_pricing", "ticker": "SYNTH CDSI GEN 5Y Corp", "recovery_rate": recovery_rate}
    )
    assert validated.model_dump()["recovery_rate"] == recovery_rate
    with pytest.raises(ValidationError):
        schema.model_validate({"operation": "cdx_pricing", "ticker": "SYNTH", "recovery_rate": 150})
