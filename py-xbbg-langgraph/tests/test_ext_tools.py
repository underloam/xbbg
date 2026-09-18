"""Behavioral boundaries of extension action inputs and inline chart artifacts."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError
import pytest

from xbbg_langgraph.ext_tools import (
    create_ext_bql_builder_tool,
    create_ext_calculate_tool,
    create_ext_chart_spec_tool,
    create_ext_ticker_tool,
)


def _chart(arguments: dict[str, Any]) -> dict[str, Any]:
    tool = create_ext_chart_spec_tool()
    message = tool.invoke({"type": "tool_call", "id": "chart", "name": tool.name, "args": arguments})
    return message.artifact["data"]


def test_operation_rejects_fields_from_another_action() -> None:
    tool = create_ext_ticker_tool()
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "parse_ticker", "ticker": "ES1 Index", "tickers": ["ES1 Index"]})
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "normalize_tickers", "ticker": "ES1 Index"})


def test_selected_action_invokes_without_inactive_langchain_defaults() -> None:
    tool = create_ext_ticker_tool()
    message = tool.invoke(
        {
            "type": "tool_call",
            "id": "ticker",
            "name": tool.name,
            "args": {"operation": "parse_ticker", "ticker": "ES1 Index"},
        }
    )
    assert message.artifact["data"]["prefix"] == "ES"
    explicit_null = tool.invoke(
        {
            "type": "tool_call",
            "id": "nullable-ticker",
            "name": tool.name,
            "args": {"operation": "parse_ticker", "ticker": "ES1 Index", "tickers": None},
        }
    )
    assert explicit_null.artifact["data"] == message.artifact["data"]
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "parse_ticker", "ticker": "ES1 Index", "unknown": None})


def test_query_builder_never_guesses_a_market_suffix() -> None:
    tool = create_ext_bql_builder_tool()
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "build_preferreds_query", "equity_ticker": "BAC"})


def test_hierarchy_rejects_non_integral_levels_and_length_mismatch() -> None:
    tool = create_ext_calculate_tool()
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "calculate_level_percentages", "values": [100], "levels": [True]})
    with pytest.raises(ValidationError):
        tool.invoke({"operation": "calculate_level_percentages", "values": [100, 25], "levels": [1]})


def test_chart_literal_columns_and_fold_names_cannot_shadow_input() -> None:
    row = {
        "event.date": "20260115",
        "price.last": 20,
        "price[bid]": 19,
        "_xbbg_series": "supplied series",
        "_xbbg_value": 999,
    }
    chart = _chart(
        {
            "source": "rows",
            "rows": [row],
            "x_field": "event.date",
            "y_fields": ["price.last", "price[bid]"],
            "series_field": "_xbbg_series",
        }
    )
    spec = chart["spec"]
    assert spec["encoding"]["x"]["field"] == r"event\.date"
    assert spec["transform"][0]["fold"] == [r"price\.last", r"price\[bid\]"]
    generated_series, generated_value = spec["transform"][0]["as"]
    assert generated_series not in row
    assert generated_value not in row
    assert spec["encoding"]["color"]["field"] == generated_series
    assert spec["encoding"]["y"]["field"] == generated_value
    assert spec["encoding"]["detail"]["field"] == "_xbbg_series"
    assert spec["data"]["values"] == [{**row, "event.date": "2026-01-15"}]


def test_chart_does_not_reference_fields_only_in_discarded_rows() -> None:
    tool = create_ext_chart_spec_tool()
    with pytest.raises(ValueError, match="Missing x_field"):
        tool.invoke(
            {
                "source": "rows",
                "rows": [{"value": 1}, {"date": "2026-01-15", "value": 2}],
                "x_field": "date",
                "max_points": 1,
            }
        )


def test_chart_truncation_preserves_valid_inline_spec() -> None:
    first = {"date": "2026-01-15", "value": 1}
    chart = _chart(
        {
            "source": "rows",
            "rows": [first, {"date": "2026-01-16", "value": 2}],
            "max_points": 1,
        }
    )
    assert chart["spec"]["data"] == {"values": [first]}
    assert chart["rowCount"] == 1
    assert chart["inputRowCount"] == 2
    assert chart["truncatedInput"] is True
    assert chart["summary"]["xField"] == "date"
    assert chart["summary"]["yFields"] == ["value"]
