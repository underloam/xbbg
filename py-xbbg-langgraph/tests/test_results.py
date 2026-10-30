"""Behavioral boundaries for bounded tool content and artifacts."""

from __future__ import annotations

import base64
from datetime import UTC, date, datetime, time, timedelta
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


def _metadata_result(metadata: dict[bytes, bytes], kind: str, values: list[float] | None = None) -> Any:
    values = [] if values is None else values
    if kind.startswith("pyarrow"):
        pa = pytest.importorskip("pyarrow")
        batch = pa.RecordBatch.from_arrays(
            [pa.array(values, type=pa.float64())],
            schema=pa.schema([("price", pa.float64())], metadata=metadata),
        )
        return batch if kind.endswith("batch") else pa.Table.from_batches([batch])

    class ArrowColumn:
        name = "price"
        data_type = "Float64"

        def __init__(self, rows: list[float]) -> None:
            self.rows = rows

        def slice(self, offset: int, length: int) -> ArrowColumn:
            return ArrowColumn(self.rows[offset : offset + length])

        def to_pylist(self) -> list[float]:
            return list(self.rows)

    class ArrowTable:
        __module__ = "xbbg._core"
        num_columns = 1

        def __init__(self, rows: list[float] | None = None) -> None:
            self.rows = values if rows is None else rows

        @property
        def num_rows(self) -> int:
            return len(self.rows)

        @property
        def metadata(self) -> dict[str, str]:
            return {key.decode(): value.decode() for key, value in metadata.items()}

        def select(self, columns: list[int]) -> ArrowTable:
            assert columns == [0]
            return self

        def slice(self, offset: int, length: int) -> ArrowTable:
            return ArrowTable(self.rows[offset : offset + length])

        def column(self, index: int) -> ArrowColumn:
            assert index == 0
            return ArrowColumn(self.rows)

        def __getattr__(self, name: str) -> Any:
            if name in {"field_exceptions", "field_errors"}:
                raise AssertionError("bounded projection must not expand native diagnostic accessors")
            raise AttributeError(name)

    class ArrowRecordBatch:
        __module__ = "xbbg._core"
        num_rows = len(values)
        num_columns = 1

        def to_table(self) -> ArrowTable:
            return ArrowTable()

        def select(self, columns: list[int]) -> ArrowTable:
            return self.to_table().select(columns)

    return ArrowRecordBatch() if kind.endswith("batch") else ArrowTable()


def _compact_metadata(indices: list[Any], **overrides: Any) -> dict[bytes, bytes]:
    payload = {
        "version": 1,
        "entries": [
            {"field": "BAD_FIELD", "category": "BAD_FLD", "code": 9, "subcategory": "INVALID", "message": "Unknown"}
        ],
        "securities": {"IBM US Equity": indices},
        **overrides,
    }
    return {b"xbbg.field_exceptions": json.dumps(payload).encode()}


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
        "datetime": datetime(2026, 9, 18, 12, 30, tzinfo=UTC),
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


def test_zero_row_arrow_preserves_diagnostics_entitlements_delay_and_metadata() -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"price": pa.array([], type=pa.float64())}).replace_schema_metadata(
        {
            b"xbbg.security_errors": b'{"BAD Security":{"message":"NO_AUTH"}}',
            b"xbbg.field_exceptions": b'{"IBM US Equity":[{"fieldId":"BAD_FIELD"}]}',
            b"xbbg.eid_data": b'{"IBM US Equity":[101,202]}',
            b"xbbg.delayed": b'{"IBM US Equity":{"delayed":true,"delay_minutes":15}}',
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
        assert result["data"]["delayed"] == {"IBM US Equity": {"delayed": True, "delay_minutes": 15}}
        # Parsed xbbg.* payloads are not repeated as raw strings in the generic metadata map.
        assert result["data"]["metadata"] == {"source": "ReferenceDataRequest"}
        assert result["truncated"] is False
    content, artifact = _project(table, max_result_nodes=10, max_result_bytes=256, max_content_bytes=256)
    for result in (content, artifact):
        assert result["rowCount"] == 0
        assert result["hasErrors"] is True
        assert result["truncated"] is True


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
def test_compact_arrow_diagnostics_project_expanded_occurrences_and_field_errors(kind: str) -> None:
    first = {"field": "BAD_FIELD", "category": "BAD_FLD", "code": 9, "subcategory": "INVALID", "message": "Unknown"}
    second = {"field": "PRIVATE", "category": "NO_AUTH", "code": 10, "subcategory": "DENIED", "message": "Restricted"}
    field_errors = {
        "bad-id": {
            "category": "BAD_FLD",
            "code": 9,
            "subcategory": "INVALID",
            "message": "Invalid field identifier",
            "source": "apiflds",
        }
    }
    metadata = {
        b"xbbg.security_errors": b'{"BAD Security":{"message":"NO_AUTH"}}',
        b"xbbg.field_exceptions": json.dumps(
            {"version": 1, "entries": [first, second], "securities": {"IBM US Equity": [0, 1, 0]}}
        ).encode(),
        b"xbbg.field_errors": json.dumps(field_errors).encode(),
        b"xbbg.eid_data": b'{"IBM US Equity":[101,202]}',
        b"xbbg.delayed": b'{"IBM US Equity":{"delayed":true,"delay_minutes":15}}',
        b"source": b"ReferenceDataRequest",
    }
    content, artifact = _project(_metadata_result(metadata, kind), max_rows=1, max_content_rows=1)
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == {"IBM US Equity": [first, second, first]}
        assert result["data"]["fieldErrors"] == field_errors
        assert result["data"]["securityErrors"]["BAD Security"]["message"] == "NO_AUTH"
        assert result["data"]["eidData"] == {"IBM US Equity": [101, 202]}
        assert result["data"]["delayed"] == {"IBM US Equity": {"delayed": True, "delay_minutes": 15}}
        assert result["data"]["metadata"] == {"source": "ReferenceDataRequest"}
        assert result["data"]["rows"] == []
        assert result["hasErrors"] is True
        assert result["truncated"] is False
    content, artifact = _project(
        _metadata_result(metadata, kind),
        max_result_nodes=10,
        max_result_bytes=256,
        max_content_bytes=256,
    )
    for result in (content, artifact):
        assert result["hasErrors"] is True
        assert result["truncated"] is True


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
@pytest.mark.parametrize("with_security_errors", [False, True])
def test_malformed_field_errors_preserve_usable_rows_and_adjacent_diagnostics(
    kind: str, with_security_errors: bool
) -> None:
    metadata = {
        b"xbbg.field_errors": b"{",
        b"xbbg.eid_data": b'{"SYNTHETIC Security":[101,202]}',
    }
    if with_security_errors:
        metadata[b"xbbg.security_errors"] = b'{"BAD Security":{"message":"NO_AUTH"}}'
    content, artifact = _project(_metadata_result(metadata, kind, [123.45]))
    for result in (content, artifact):
        assert result["rowCount"] == 1
        assert result["data"]["rows"] == [{"price": 123.45}]
        assert result["data"]["fieldErrors"] == {}
        assert result["data"]["eidData"] == {"SYNTHETIC Security": [101, 202]}
        assert result["data"]["metadata"] == {}
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "unsupported_value" in result["truncation"]["reasons"]
        if with_security_errors:
            assert result["data"]["securityErrors"]["BAD Security"]["message"] == "NO_AUTH"


@pytest.mark.parametrize("kind", ["pyarrow_table", "native_table", "native_batch"])
@pytest.mark.parametrize("with_security_errors", [False, True])
@pytest.mark.parametrize(
    "indices,overrides",
    [
        ([0] * 100 + [1], {}),
        ([-1], {}),
        ([True], {}),
        ([0.0], {}),
        (["0"], {}),
        ([None], {}),
        ([0], {"version": 2}),
        ([0], {"version": True}),
        ([0], {"entries": []}),
        ([0], {"entries": [{"field": "BAD_FIELD"}]}),
        ([0], {"securities": {"IBM US Equity": "0"}}),
        ([0], {"unexpected": True}),
    ],
)
def test_malformed_compact_metadata_is_explicit_even_beyond_output_prefix(
    kind: str, with_security_errors: bool, indices: list[Any], overrides: dict[str, Any]
) -> None:
    metadata = _compact_metadata(indices, **overrides)
    if with_security_errors:
        metadata[b"xbbg.security_errors"] = b'{"BAD Security":{"message":"NO_AUTH"}}'
    content, artifact = _project(_metadata_result(metadata, kind), max_result_bytes=4096, max_content_bytes=256)
    for result in (content, artifact):
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "unsupported_value" in result["truncation"]["reasons"]
        assert "xbbg.field_exceptions" not in result["data"].get("metadata", {})
        assert result["data"].get("fieldExceptions", {}) == {}
    if with_security_errors:
        assert artifact["data"]["securityErrors"]["BAD Security"]["message"] == "NO_AUTH"


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
@pytest.mark.parametrize(
    "encoded",
    [
        b'{"entries":[],"securities":[]}',
        b'{"version":[],"entries":[],"securities":[]}',
        b'{"version":1,"entries":[]}',
        b'{"version":1,"securities":{}}',
    ],
)
def test_malformed_envelope_members_cannot_masquerade_as_legacy_tickers(kind: str, encoded: bytes) -> None:
    content, artifact = _project(_metadata_result({b"xbbg.field_exceptions": encoded}, kind))
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == {}
        assert result["data"]["metadata"] == {}
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "unsupported_value" in result["truncation"]["reasons"]


@pytest.mark.parametrize("kind", ["pyarrow_table", "native_table", "native_batch"])
def test_repeated_compact_diagnostics_are_expanded_only_within_projection_budgets(kind: str) -> None:
    entry = {
        "field": "PRIVATE",
        "category": "NO_AUTH",
        "code": 10,
        "subcategory": "DENIED",
        "message": "Restricted " * 50,
    }
    metadata = _compact_metadata([0] * 1000, entries=[entry])
    content, artifact = _project(
        _metadata_result(metadata, kind),
        max_rows=1,
        max_content_rows=1,
        max_result_bytes=16_384,
        max_content_bytes=4096,
        max_result_nodes=10_000,
    )
    content_occurrences = content["data"]["fieldExceptions"]["IBM US Equity"]
    artifact_occurrences = artifact["data"]["fieldExceptions"]["IBM US Equity"]
    assert 0 < len(content_occurrences) < len(artifact_occurrences) < 1000
    for result, occurrences in ((content, content_occurrences), (artifact, artifact_occurrences)):
        assert occurrences[0] == entry
        assert all(isinstance(occurrence, dict) for occurrence in occurrences)
        assert "version" not in result["data"]["fieldExceptions"]
        assert "entries" not in result["data"]["fieldExceptions"]
        assert "securities" not in result["data"]["fieldExceptions"]
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "max_result_bytes" in result["truncation"]["reasons"]
        assert "max_rows" not in result["truncation"]["reasons"]


def test_compact_diagnostic_expansion_obeys_node_budget() -> None:
    def nodes(value: Any) -> int:
        if isinstance(value, dict):
            return 1 + sum(nodes(child) for child in value.values())
        if isinstance(value, list):
            return 1 + sum(nodes(child) for child in value)
        return 1

    metadata = _compact_metadata([0] * 500)
    content, artifact = _project(_metadata_result(metadata, "native_table"), max_result_nodes=2048)
    for result in (content, artifact):
        occurrences = result["data"]["fieldExceptions"]["IBM US Equity"]
        assert 0 < len(occurrences) < 500
        assert occurrences[0]["field"] == "BAD_FIELD"
        assert all(isinstance(occurrence, dict) for occurrence in occurrences)
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "max_result_nodes" in result["truncation"]["reasons"]
        assert nodes(result) <= 2048


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
def test_legacy_raw_field_exceptions_keep_expanded_members_and_duplicates(kind: str) -> None:
    exceptions = {"IBM US Equity": [{"fieldId": "OLD_FIELD"}, {"fieldId": "OTHER_FIELD"}, {"fieldId": "OLD_FIELD"}]}
    metadata = {b"xbbg.field_exceptions": json.dumps(exceptions).encode()}
    content, artifact = _project(_metadata_result(metadata, kind), max_rows=1, max_content_rows=1)
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == exceptions
        assert result["data"]["metadata"] == {}
        assert result["hasErrors"] is True
        assert result["truncated"] is False


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
@pytest.mark.parametrize("securities", [{}, {"EMPTY Security": []}])
def test_empty_compact_diagnostics_do_not_report_errors_or_truncation(
    kind: str, securities: dict[str, list[int]]
) -> None:
    metadata = _compact_metadata([], entries=[], securities=securities)
    content, artifact = _project(_metadata_result(metadata, kind))
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == securities
        assert result["data"]["metadata"] == {}
        assert "hasErrors" not in result
        assert result["truncated"] is False


@pytest.mark.parametrize("kind", ["pyarrow_table", "pyarrow_batch", "native_table", "native_batch"])
@pytest.mark.parametrize("encoding", ["compact", "legacy"])
@pytest.mark.parametrize("with_occurrences", [False, True])
def test_field_exception_error_presence_counts_occurrences_not_security_keys(
    kind: str, encoding: str, with_occurrences: bool
) -> None:
    entry = {
        "field": "BAD_FIELD",
        "category": "BAD_FLD",
        "code": 9,
        "subcategory": "INVALID",
        "message": "Unknown",
    }
    securities: dict[str, list[int]] = {"EMPTY Security": [], "ANOTHER EMPTY Security": []}
    if with_occurrences:
        securities["SYNTHETIC Security"] = [0, 0]
    expected = {security: [entry for _ in indices] for security, indices in securities.items()}
    metadata = (
        _compact_metadata([], entries=[entry], securities=securities)
        if encoding == "compact"
        else {b"xbbg.field_exceptions": json.dumps(expected).encode()}
    )
    content, artifact = _project(_metadata_result(metadata, kind))
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == expected
        assert result["data"]["metadata"] == {}
        assert result.get("hasErrors", False) is with_occurrences
        assert result["truncated"] is False


@pytest.mark.parametrize(
    "limits,reason",
    [
        ({"max_result_bytes": 256, "max_content_bytes": 256}, "max_result_bytes"),
        ({"max_result_nodes": 32}, "max_result_nodes"),
    ],
)
def test_unparsed_compact_metadata_is_omitted_without_exposing_index_references(
    limits: dict[str, int], reason: str
) -> None:
    metadata = _compact_metadata([0] * 1000)
    content, artifact = _project(_metadata_result(metadata, "native_table"), **limits)
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == {}
        assert "xbbg.field_exceptions" not in result["data"].get("metadata", {})
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert reason in result["truncation"]["reasons"]


def test_deep_field_exception_metadata_is_omitted_with_explicit_reason() -> None:
    nested: Any = "Too deep"
    for _ in range(40):
        nested = {"nested": nested}
    metadata = {b"xbbg.field_exceptions": json.dumps({"IBM US Equity": [{"message": nested}]}).encode()}
    content, artifact = _project(_metadata_result(metadata, "native_batch"))
    for result in (content, artifact):
        assert result["data"]["fieldExceptions"] == {}
        assert result["hasErrors"] is True
        assert result["truncated"] is True
        assert "max_result_depth" in result["truncation"]["reasons"]


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
