"""Behavioral boundaries for bounded tool content and artifacts."""

from __future__ import annotations

import base64
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
from typing import Any

import pytest

from xbbg_langgraph.options import BloombergToolsOptions
from xbbg_langgraph.results import create_tool_result


def _project(value: Any, **kwargs: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    options = BloombergToolsOptions(**kwargs)
    content, artifact = create_tool_result("xbbg_bdp", value, options)
    assert len(content.encode("utf-8")) <= options.max_content_bytes
    assert len(json.dumps(artifact, allow_nan=False).encode("utf-8")) <= options.max_result_bytes
    assert len(json.dumps(artifact, ensure_ascii=False, allow_nan=False).encode("utf-8")) <= options.max_result_bytes
    return json.loads(content), artifact


@pytest.mark.parametrize(
    "artifact_rows,content_rows,artifact_bytes,content_bytes",
    [
        (1, 4, 256, 4096),
        (4, 1, 4096, 256),
    ],
)
def test_content_and_artifact_limits_are_independent(
    artifact_rows: int,
    content_rows: int,
    artifact_bytes: int,
    content_bytes: int,
) -> None:
    rows = [{"index": index} for index in range(10)]
    content, artifact = _project(
        rows,
        max_rows=artifact_rows,
        max_content_rows=content_rows,
        max_result_bytes=artifact_bytes,
        max_content_bytes=content_bytes,
    )
    assert artifact["data"] == rows[:artifact_rows]
    assert content["data"] == rows[:content_rows]
    assert artifact["rowCount"] == content["rowCount"] == 10
    assert artifact["truncated"] and content["truncated"]


def test_unicode_and_json_escaping_fit_complete_envelope() -> None:
    value = [{"text": '\U0001f680é\x00"\\' * 1000}]
    content, artifact = _project(value, max_content_bytes=256, max_result_bytes=256)
    for result in (content, artifact):
        assert result["truncated"]
        assert result["data"][0]["text"] != value[0]["text"]
        assert "max_result_bytes" in result["truncation"]["reasons"]


def test_base64_shaped_user_data_cannot_bypass_byte_budgets() -> None:
    value = {"encoding": "base64", "data": '"' * 300, "byteLength": 5, "truncated": False}
    content, artifact = _project(value, max_content_bytes=256, max_result_bytes=512)
    assert content["truncated"] and artifact["truncated"]
    _, intact = _project(value)
    assert intact["data"] == value


def test_error_priority_survives_byte_and_node_exhaustion() -> None:
    value = {
        "ordinary": {str(index): "ordinary" * 100 for index in range(1000)},
        "securityErrors": {"BAD Security": {"message": "NO_AUTH" * 100}},
        "eidData": {"BAD Security": [101, 202]},
    }
    content, artifact = _project(value, max_content_bytes=256, max_result_bytes=256)
    for result in (content, artifact):
        assert result["hasErrors"] is True
        assert "securityErrors" in result["data"]
        assert result["truncated"]
    content, artifact = _project(value, max_result_nodes=10)
    assert content["hasErrors"] is artifact["hasErrors"] is True
    assert content["truncated"] and artifact["truncated"]


def test_cycles_are_truncated_but_shared_values_are_not_cycles() -> None:
    shared = {"price": 123.45}
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    content, artifact = _project({"first": shared, "second": shared, "cycle": cyclic})
    for result in (content, artifact):
        assert result["data"]["first"] == result["data"]["second"] == shared
        assert result["data"]["cycle"] == {"self": None}
        assert "circular_reference" in result["truncation"]["reasons"]
        assert result["rowCount"] is None


def test_upstream_truncation_does_not_invent_original_row_count() -> None:
    content, artifact = _project({"rows": [{"value": 1}], "truncated": True})
    for result in (content, artifact):
        assert result["rowCount"] is None
        assert "upstream_truncation" in result["truncation"]["reasons"]


def test_temporal_decimal_and_nonfinite_values_are_json_safe() -> None:
    value = {
        "date": date(2026, 9, 18),
        "datetime": datetime(2026, 9, 18, 12, 30, tzinfo=timezone.utc),
        "time": time(12, 30),
        "duration": timedelta(seconds=90),
        "price": Decimal("12345678901234567890.0123456789"),
        "missing": float("nan"),
        "infinite": float("inf"),
    }
    content, artifact = _project(value)
    expected = {
        "date": "2026-09-18",
        "datetime": "2026-09-18T12:30:00+00:00",
        "time": "12:30:00",
        "duration": "0:01:30",
        "price": "12345678901234567890.0123456789",
        "missing": None,
        "infinite": None,
    }
    assert content["data"] == artifact["data"] == expected
    assert "unsupported_value" in artifact["truncation"]["reasons"]


def test_presence_bitmap_is_losslessly_tagged_and_partial_binary_is_explicit() -> None:
    content, artifact = _project({"__xbbg_present": b"\x81\x00", "price": None})
    for result in (content, artifact):
        binary = result["data"]["__xbbg_present"]
        assert type(binary) is dict
        assert binary["encoding"] == "base64"
        assert base64.b64decode(binary["data"]) == b"\x81\x00"
        assert binary["byteLength"] == 2
        assert binary["truncated"] is False
        assert result["data"]["price"] is None
        assert result["truncated"] is False
    content, artifact = _project(bytes(range(100)), max_string_chars=8)
    for result in (content, artifact):
        assert base64.b64decode(result["data"]["data"]) == bytes(range(6))
        assert result["data"]["byteLength"] == 100
        assert result["data"]["truncated"] is True
        assert result["truncated"] is True


def test_depth_is_bounded_without_python_recursion_failure() -> None:
    value: Any = 1
    for _ in range(1000):
        value = {"nested": value}
    content, artifact = _project(value)
    assert "max_result_depth" in content["truncation"]["reasons"]
    assert "max_result_depth" in artifact["truncation"]["reasons"]


def test_zero_row_arrow_preserves_diagnostics_entitlements_and_metadata() -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"price": pa.array([], type=pa.float64())}).replace_schema_metadata(
        {
            b"xbbg.security_errors": b'{"BAD Security":{"message":"NO_AUTH"}}',
            b"xbbg.field_exceptions": b'{"IBM US Equity":[{"fieldId":"BAD_FIELD"}]}',
            b"xbbg.eid_data": b'{"IBM US Equity":[101,202]}',
            b"source": b"ReferenceDataRequest",
        }
    )
    content, artifact = _project(table, max_rows=1, max_content_rows=1)
    for result in (content, artifact):
        assert result["rowCount"] == 0
        assert result["hasErrors"] is True
        assert result["data"]["rows"] == []
        assert result["data"]["securityErrors"]["BAD Security"]["message"] == "NO_AUTH"
        assert result["data"]["fieldExceptions"]["IBM US Equity"][0]["fieldId"] == "BAD_FIELD"
        assert result["data"]["eidData"] == {"IBM US Equity": [101, 202]}
        assert result["data"]["metadata"]["source"] == "ReferenceDataRequest"
        assert result["truncated"] is False
    content, artifact = _project(table, max_result_nodes=10, max_result_bytes=256, max_content_bytes=256)
    for result in (content, artifact):
        assert result["rowCount"] == 0
        assert result["hasErrors"] is True
        assert result["truncated"] is True


def test_arrow_is_sliced_before_a_later_unconvertible_timestamp() -> None:
    pa = pytest.importorskip("pyarrow")
    # The second scalar overflows Python datetime. Converting the full table
    # before slicing would raise even though neither projection needs that row.
    table = pa.table({"when": pa.array([0, 2**62], type=pa.timestamp("s"))})
    content, artifact = _project(table, max_rows=1, max_content_rows=1)
    assert content["data"] == artifact["data"] == [{"when": "1970-01-01T00:00:00"}]
    assert artifact["rowCount"] == 2
    assert artifact["truncated"]
    with pytest.raises(OverflowError):
        _project(table, max_rows=2, max_content_rows=2)


def test_nested_snapshots_share_arrow_row_materialization_allowance() -> None:
    pa = pytest.importorskip("pyarrow")
    first = pa.table({"index": range(100)})
    second = pa.table({"when": pa.array([2**62], type=pa.timestamp("s"))})
    content, artifact = _project(
        {"updateCount": 2, "updates": [first, second]},
        max_rows=3,
        max_content_rows=3,
    )
    for result in (content, artifact):
        first_update, second_update = result["data"]["updates"]
        assert first_update == {"rowCount": 100, "rows": [{"index": 0}, {"index": 1}, {"index": 2}]}
        assert second_update == {"rowCount": 1, "rows": []}
        assert result["rowCount"] == 2
        assert "max_rows" in result["truncation"]["reasons"]


def test_nested_arrow_values_cannot_expand_past_node_allowance() -> None:
    pa = pytest.importorskip("pyarrow")
    # A single logical row can contain arbitrarily many Python objects. Its
    # unconvertible final scalar also guards against eager to_pylist conversion.
    table = pa.table({"nested": pa.array([[0] * 1000 + [2**62]], type=pa.list_(pa.timestamp("s")))})
    content, artifact = _project(table, max_result_nodes=32)
    assert content["data"] == artifact["data"] == [{}]
    assert "max_result_nodes" in artifact["truncation"]["reasons"]


def test_native_preview_keeps_small_strings_from_large_backing_buffers() -> None:
    core = pytest.importorskip("xbbg._core")
    table = core.ArrowTable.from_pylist([{"ticker": "IBM US Equity", "index": index} for index in range(2000)])
    content, artifact = _project(table, max_rows=1, max_content_rows=2, max_result_bytes=4096, max_content_bytes=4096)
    assert artifact["data"] == [{"ticker": "IBM US Equity", "index": 0}]
    assert content["data"] == [{"ticker": "IBM US Equity", "index": 0}, {"ticker": "IBM US Equity", "index": 1}]
    assert artifact["rowCount"] == content["rowCount"] == 2000
    batch = table.to_batches()[0]
    _, batch_artifact = _project(batch, max_rows=1)
    assert batch_artifact["data"] == artifact["data"]


def test_clipped_chart_cannot_be_rendered_as_an_intact_spec() -> None:
    chart = {
        "kind": "xbbg.visualization",
        "truncatedInput": True,
        "spec": {"mark": "line", "data": {"values": [{"x": 1, "y": 2}]}, "description": "description " * 100},
    }
    content, artifact = _project(chart, max_string_chars=20)
    for result in (content, artifact):
        assert result["data"]["spec"] is None
        assert result["data"]["renderable"] is False
        assert result["truncated"]
    _, intact = _project(chart)
    assert intact["data"]["spec"] == chart["spec"]
    assert "renderable" not in intact["data"]
