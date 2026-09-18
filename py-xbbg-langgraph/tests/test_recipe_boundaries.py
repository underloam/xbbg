"""Security and financial-data boundaries crossing the Python/native adapter."""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import ValidationError
import pytest

from xbbg_langgraph import (
    create_corporate_bonds_tool,
    create_depth_snapshot_tool,
    create_ext_bql_builder_tool,
    create_ext_chart_spec_tool,
    create_ext_market_session_tool,
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
