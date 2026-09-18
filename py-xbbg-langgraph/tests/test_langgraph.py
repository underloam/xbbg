"""Exercise the installed LangGraph dependency through its public graph runtime."""

from __future__ import annotations

import json

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
import pytest

from xbbg_langgraph import create_ext_calculate_tool


@pytest.mark.asyncio
async def test_tool_node_preserves_independent_model_and_application_results():
    tool = create_ext_calculate_tool(max_rows=2, max_content_rows=1)
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([tool]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    app = graph.compile()
    result = await app.ainvoke(
        {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": tool.name,
                            "args": {
                                "operation": "calculate_level_percentages",
                                "values": [1.0, 2.0, 3.0],
                                "levels": [1, 1, 1],
                            },
                            "id": "percentages",
                            "type": "tool_call",
                        }
                    ],
                )
            ],
        }
    )
    message = result["messages"][-1]
    assert isinstance(message, ToolMessage)
    assert message.status == "success"
    assert message.tool_call_id == "percentages"
    preview = json.loads(message.content)
    assert preview["data"] == pytest.approx([100 / 6])
    assert message.artifact["data"] == pytest.approx([100 / 6, 200 / 6])
    assert preview["rowCount"] == message.artifact["rowCount"] == 3
    assert preview["truncated"] and message.artifact["truncated"]
