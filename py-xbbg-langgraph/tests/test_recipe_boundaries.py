"""Security and financial-data boundaries crossing the Python/native adapter."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import json
import sys
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock

from langchain_core.tools import ToolException
from pydantic import ValidationError
import pytest

from xbbg_langgraph import (
    BloombergToolsOptions,
    create_all_bloomberg_tools,
    create_auction_snapshot_tool,
    create_bloomberg_tools,
    create_corporate_bonds_tool,
    create_depth_snapshot_tool,
    create_ext_bql_builder_tool,
    create_ext_chart_spec_tool,
    create_ext_market_session_tool,
    create_resolve_venues_tool,
)


@pytest.mark.parametrize(
    "field,value",
    [
        ("ticker", "IBM')]) for(bonds()) // US Equity"),
        ("ccy", "USD' OR 1==1 OR CRNCY=='USD"),
        ("fields", ["id) for(bonds()) //"]),
    ],
)
def test_recipe_inputs_cannot_escape_generated_query(field, value):
    args = {"ticker": "IBM US Equity", field: value}
    with pytest.raises(ValidationError):
        create_corporate_bonds_tool().args_schema.model_validate(args)
    builder_args = {"operation": "build_corporate_bonds_query", **args}
    if "fields" in builder_args:
        builder_args["extra_fields"] = builder_args.pop("fields")
    with pytest.raises(ValidationError):
        create_ext_bql_builder_tool().invoke(builder_args)


@pytest.mark.asyncio
async def test_generated_recipe_query_limit_applies_before_connecting():
    tool = create_corporate_bonds_tool(max_bql_query_chars=32)
    with pytest.raises(ValueError, match="max_bql_query_chars"):
        await tool.ainvoke({"ticker": "IBM US Equity"})


def test_depth_projection_cannot_silently_discard_every_field():
    with pytest.raises(ValidationError, match="require fields"):
        create_depth_snapshot_tool().invoke({"ticker": "IBM US Equity", "max_updates": 1, "all_fields": False})


def test_overnight_utc_interval_uses_next_local_day_across_dst():
    tool = create_ext_market_session_tool()
    reply = tool.invoke(
        {
            "type": "tool_call",
            "id": "overnight",
            "name": tool.name,
            "args": {
                "operation": "session_times_to_utc",
                "start_time": "18:00",
                "end_time": "17:00",
                "exchange_tz": "America/New_York",
                "date": "2026-10-31",
            },
        }
    )
    interval = reply.artifact["data"]
    start, end = datetime.fromisoformat(interval["start"]), datetime.fromisoformat(interval["end"])
    assert start.utcoffset() == end.utcoffset() == timedelta(0)
    assert start == datetime.fromisoformat("2026-10-31T22:00:00+00:00")
    assert end == datetime.fromisoformat("2026-11-01T22:00:00+00:00")
    assert end - start == timedelta(hours=24)


def test_chart_rejects_integers_that_the_renderer_would_round():
    with pytest.raises(ValidationError, match="safe-integer"):
        create_ext_chart_spec_tool().invoke(
            {
                "source": "rows",
                "rows": [{"date": "2026-09-18", "value": 2**53 + 1}],
            }
        )


def test_market_rule_requires_a_lookup_key_at_schema_boundary():
    with pytest.raises(ValidationError, match="requires mic or exch_code"):
        create_ext_market_session_tool().args_schema.model_validate({"operation": "get_market_rule"})


@pytest.fixture
def auction_api(monkeypatch):
    """Replace the public API modules so no test can initialize a native engine."""
    package: Any = ModuleType("xbbg")
    extension: Any = ModuleType("xbbg.ext")
    extension.aresolve_venues = AsyncMock()
    extension.aauction_snapshot = AsyncMock()
    package.ext = extension
    package.blp = ModuleType("xbbg.blp")
    monkeypatch.setitem(sys.modules, "xbbg", package)
    monkeypatch.setitem(sys.modules, "xbbg.ext", extension)
    monkeypatch.setitem(sys.modules, "xbbg.blp", package.blp)
    return extension


@pytest.mark.parametrize("factory", [create_resolve_venues_tool, create_auction_snapshot_tool])
@pytest.mark.parametrize(
    "arguments",
    [
        {"securities": []},
        {"securities": ["SYNTH_A US Equity", "SYNTH_B US Equity", "SYNTH_C US Equity"]},
        {"pcs_overrides": {"SYNTH EXCHANGE": 1}},
        {"pcs_overrides": {"SYNTH EXCHANGE": " "}},
        {"pcs_overrides": {"SYNTH A": "A", "SYNTH B": "B", "SYNTH C": "C"}},
        {"pcs_overrides": {"SYNTH EXCHANGE": "A", " SYNTH EXCHANGE ": "B"}},
    ],
)
def test_auction_tools_reject_unbounded_or_lossy_inputs(factory, arguments, auction_api):
    tool = factory(max_securities=2)
    with pytest.raises(ValidationError):
        tool.invoke({"securities": ["SYNTH_A US Equity"], **arguments})
    auction_api.aresolve_venues.assert_not_awaited()
    auction_api.aauction_snapshot.assert_not_awaited()


def test_auction_snapshot_rejects_oversized_explicit_field_selection(auction_api):
    tool = create_auction_snapshot_tool(max_fields=1)
    with pytest.raises(ValidationError):
        tool.invoke(
            {
                "securities": ["SYNTH_A US Equity"],
                "fields": ["IN_AUCTION_RT", "ORDER_IMB_BUY_VOLUME"],
            }
        )
    auction_api.aauction_snapshot.assert_not_awaited()


@pytest.mark.parametrize(
    "factory,symbol",
    [
        (create_resolve_venues_tool, "aresolve_venues"),
        (create_auction_snapshot_tool, "aauction_snapshot"),
    ],
)
@pytest.mark.parametrize("invoke_async", [False, True])
def test_auction_tools_explain_missing_xbbg_helpers(factory, symbol, invoke_async, auction_api, monkeypatch):
    monkeypatch.delattr(auction_api, symbol)
    tool = factory()
    arguments = {"securities": ["SYNTH_A US Equity"]}

    with pytest.raises(ToolException) as raised:
        if invoke_async:
            asyncio.run(tool.ainvoke(arguments))
        else:
            tool.invoke(arguments)

    assert f"xbbg.ext.{symbol}" in str(raised.value)
    assert "upgrade xbbg" in str(raised.value).lower()


@pytest.mark.parametrize(
    "name,method",
    [
        ("xbbg_resolve_venues", "aresolve_venues"),
        ("xbbg_auction_snapshot", "aauction_snapshot"),
    ],
)
@pytest.mark.parametrize("invoke_async", [False, True])
def test_auction_tools_bound_rows_without_losing_venue_failures(name, method, invoke_async, auction_api):
    pa = pytest.importorskip("pyarrow")
    rows: list[dict[str, Any]] = [
        {
            "input_order": 0,
            "security": "SYNTH_A US Equity",
            "venue_topic": "SYNTH_A UN Equity",
            "status": "resolved",
            "error": None,
        },
        {
            "input_order": 1,
            "security": "SYNTH_B US Equity",
            "venue_topic": "SYNTH_B UW Equity",
            "status": "mismatch",
            "error": "Venue exchange mismatch",
        },
        {
            "input_order": 2,
            "security": "SYNTH_C Index",
            "venue_topic": None,
            "status": "unsupported",
            "error": "Unsupported market sector",
        },
    ]
    arguments = {"securities": [row["security"] for row in rows]}
    if name == "xbbg_auction_snapshot":
        arguments["fields"] = ["IN_AUCTION_RT", "ORDER_IMB_BUY_VOLUME"]
        for row in rows:
            resolved = row["status"] == "resolved"
            row["IN_AUCTION_RT"] = True if resolved else None
            row["ORDER_IMB_BUY_VOLUME"] = 125.5 if resolved else None
    getattr(auction_api, method).return_value = pa.RecordBatch.from_pylist(rows)
    options = BloombergToolsOptions(
        max_rows=2,
        max_content_rows=1,
        max_result_bytes=4096,
        max_content_bytes=1024,
    )
    factory = create_all_bloomberg_tools if invoke_async else create_bloomberg_tools
    tool = next(tool for tool in factory(options) if tool.name == name)
    call = {"type": "tool_call", "id": "auction", "name": name, "args": arguments}
    message = asyncio.run(tool.ainvoke(call)) if invoke_async else tool.invoke(call)
    preview = json.loads(message.content)
    assert preview["data"] == rows[:1]
    assert message.artifact["data"] == rows[:2]
    assert preview["rowCount"] == message.artifact["rowCount"] == 3
    assert preview["truncated"] is message.artifact["truncated"] is True
    assert preview["hasErrors"] is message.artifact["hasErrors"] is True
    assert len(message.content.encode("utf-8")) <= options.max_content_bytes
    assert len(json.dumps(message.artifact, allow_nan=False).encode("utf-8")) <= options.max_result_bytes


@pytest.mark.parametrize(
    "name,factory",
    [
        ("xbbg_resolve_venues", create_resolve_venues_tool),
        ("xbbg_auction_snapshot", create_auction_snapshot_tool),
    ],
)
def test_disabled_auction_tools_cannot_be_created_or_selected(name, factory):
    options = BloombergToolsOptions(disabled_tools={name})
    assert name not in {tool.name for tool in create_bloomberg_tools(options)}
    assert name not in {tool.name for tool in create_all_bloomberg_tools(options)}
    with pytest.raises(ValueError, match="disabled"):
        factory(options)
