"""Bounded, independent model-content and artifact projections.

Arrow values are sliced before conversion. Both limits include the JSON envelope
and use the size of standard ``json.dumps`` output (ASCII escaping included), so
compact or unescaped UTF-8 JSON also fits. No optional dataframe library is
imported until an instance of that library actually needs materialization.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import json
import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .options import BloombergToolsOptions

_MAX_DEPTH = 32
_ERROR_KEYS = (
    "error",
    "errors",
    "responseError",
    "responseErrors",
    "securityError",
    "securityErrors",
    "fieldException",
    "fieldExceptions",
    "fieldErrors",
    "unsubscribeError",
    "response_error",
    "response_errors",
    "security_error",
    "security_errors",
    "field_exception",
    "field_exceptions",
    "field_errors",
    "unsubscribe_error",
)
_PRIORITY_KEYS = (
    *_ERROR_KEYS,
    "hasErrors",
    "truncated",
    "truncatedInput",
    "eidData",
    "eidDataTruncation",
    "entitled",
    "failedEids",
    "diagnostics",
    "metadata",
    "rowCount",
    "updateCount",
    "renderable",
    "spec",
)
_METADATA_KEYS = {
    "xbbg.security_errors": "securityErrors",
    "xbbg.field_exceptions": "fieldExceptions",
    "xbbg.eid_data": "eidData",
}
_REASON_ORDER = (
    "max_rows",
    "max_string_chars",
    "max_result_bytes",
    "max_result_nodes",
    "max_result_depth",
    "circular_reference",
    "binary_data",
    "unsupported_value",
    "upstream_truncation",
)
_NATIVE_SCALARS = (
    "Null",
    "Boolean",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "Date32",
    "Date64",
    "Time32",
    "Time64",
    "Timestamp",
    "Duration",
    "Decimal128",
)
_NATIVE_VARIABLE = ("Utf8", "LargeUtf8", "Utf8View", "Binary", "LargeBinary", "BinaryView")
_OMIT = object()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False)


def _arrow_kind(value: Any) -> str | None:
    cls = type(value)
    if cls.__module__ == "xbbg._core" and cls.__name__ in {"ArrowTable", "ArrowRecordBatch"}:
        return "native"
    if cls.__module__ == "pyarrow.lib" and cls.__name__ in {"Table", "RecordBatch"}:
        return "pyarrow"
    return None


def _row_count(value: Any) -> int | None:
    if _arrow_kind(value):
        return value.num_rows
    if isinstance(value, (list, tuple)):
        return len(value)
    if isinstance(value, dict):
        for key in ("rowCount", "updateCount", "row_count", "update_count"):
            count = value.get(key)
            if type(count) is int and 0 <= count <= 2**63 - 1:
                return count
    return None


def _reported_error(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, (str, bytes, bytearray, list, tuple, dict)):
        return len(value) > 0
    return True


def _string_size(value: str, stop_after: int) -> int:
    size = 2
    for char in value:
        code = ord(char)
        size += 2 if char in '\\"\b\f\n\r\t' else 6 if code < 32 or 127 <= code <= 65535 else 12 if code > 65535 else 1
        if size > stop_after:
            break
    return size


def _json_depth_fits(value: str | bytes) -> bool:
    depth = 0
    quoted = escaped = False
    for char in value:
        code = char if isinstance(char, int) else ord(char)
        if quoted:
            if escaped:
                escaped = False
            elif code == 92:
                escaped = True
            elif code == 34:
                quoted = False
        elif code == 34:
            quoted = True
        elif code in (91, 123):
            depth += 1
            if depth >= _MAX_DEPTH:
                return False
        elif code in (93, 125):
            depth -= 1
    return True


def _pyarrow_cost(array: Any, budget: int, depth: int = 0) -> int | None:
    """Bound Python container expansion, including zero-byte Arrow null arrays."""
    import pyarrow as pa

    if len(array) > budget or depth >= _MAX_DEPTH:
        return None
    if isinstance(array, pa.ChunkedArray):
        total = 0
        for index in range(array.num_chunks):
            cost = _pyarrow_cost(array.chunk(index), budget - total, depth)
            if cost is None:
                return None
            total += cost
        return total
    dtype = array.type
    total = len(array)
    if pa.types.is_struct(dtype):
        if dtype.num_fields > budget - total:
            return None
        for index in range(dtype.num_fields):
            cost = _pyarrow_cost(array.field(index), budget - total, depth + 1)
            if cost is None:
                return None
            total += cost
    elif (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or pa.types.is_map(dtype)
    ):
        for scalar in array:
            if scalar.is_valid:
                cost = _pyarrow_cost(scalar.values, budget - total, depth + 1)
                if cost is None:
                    return None
                total += cost
    elif pa.types.is_dictionary(dtype):
        for scalar in array.indices:
            if scalar.is_valid:
                cost = _pyarrow_cost(array.dictionary.slice(scalar.as_py(), 1), budget - total, depth + 1)
                if cost is None:
                    return None
                total += cost
    elif pa.types.is_union(dtype) or isinstance(dtype, pa.BaseExtensionType):
        # User extension hooks and union conversion do not offer bounded expansion.
        return None
    return total


@dataclass(slots=True)
class _Materialization:
    rows_left: int
    nodes_left: int
    bytes_left: int
    sources: dict[int, _ArrowSource] = field(default_factory=dict)
    # Keep strong references so only encoder-created objects can use the binary
    # fast path during clipping. Public artifacts remain plain JSON dictionaries.
    binary_values: dict[int, dict[str, Any]] = field(default_factory=dict)

    def source(self, value: Any) -> _ArrowSource:
        key = id(value)
        source = self.sources.get(key)
        if source is None:
            source = _ArrowSource(value, self)
            self.sources[key] = source
        return source


class _ArrowSource:
    def __init__(self, value: Any, work: _Materialization) -> None:
        self.value = value
        self.work = work
        self.kind = _arrow_kind(value)
        self.count = value.num_rows
        self.rows: list[dict[str, Any]] = []
        self.reasons: set[str] = set()
        self.diagnostics: dict[str, Any] = {}
        self.metadata: dict[Any, Any] = {}
        self.exhausted = False
        self.selected: Any = None
        # These native/PyArrow getters copy schema metadata. There is no bounded
        # metadata getter in either public API; do not also invoke eager parsed
        # native diagnostics getters, which would duplicate and expand the copy.
        if self.kind == "native":
            carrier = value.to_table() if type(value).__name__ == "ArrowRecordBatch" else value
            raw = carrier.metadata
        else:
            raw = value.schema.metadata or {}
        self.metadata = raw
        for key, target in _METADATA_KEYS.items():
            encoded = raw.get(key, raw.get(key.encode()))
            if encoded is None:
                continue
            if len(encoded) > min(work.nodes_left, work.bytes_left):
                self.diagnostics[target] = encoded
                self.reasons.add("max_result_nodes" if len(encoded) > work.nodes_left else "max_result_bytes")
            elif not _json_depth_fits(encoded):
                self.diagnostics[target] = encoded
                self.reasons.add("max_result_depth")
            else:
                # JSON parsing cannot create more nodes than input characters.
                work.nodes_left -= len(encoded)
                work.bytes_left -= len(encoded)
                self.diagnostics[target] = json.loads(encoded)

    def row(self, index: int) -> dict[str, Any] | None:
        if index < len(self.rows):
            return self.rows[index]
        if self.exhausted:
            return None
        work = self.work
        if work.rows_left <= 0 or work.nodes_left <= 1 or work.bytes_left < 2:
            self.reasons.add(
                "max_rows"
                if work.rows_left <= 0
                else "max_result_nodes"
                if work.nodes_left <= 1
                else "max_result_bytes"
            )
            self.exhausted = True
            return None
        if self.selected is None:
            columns = min(self.value.num_columns, max(0, (work.nodes_left - 1) // 2))
            self.selected = self.value.select(list(range(columns)))
            if columns < self.value.num_columns:
                self.reasons.add("max_result_nodes")
        # Slice the table as well as each column: native columns otherwise retain
        # every chunk of an arbitrarily long table.
        sliced = self.selected.slice(index, 1)
        row: dict[str, Any] = {}
        work.rows_left -= 1
        work.nodes_left -= 1
        work.bytes_left -= 2
        for column_index in range(sliced.num_columns):
            if work.nodes_left < 2 or work.bytes_left < 2:
                self.reasons.add("max_result_nodes" if work.nodes_left < 2 else "max_result_bytes")
                self.exhausted = True
                break
            work.nodes_left -= 1
            column = sliced.column(column_index).slice(0, 1)
            if self.kind == "native":
                name = column.name
                dtype = column.data_type
                if not dtype.startswith((*_NATIVE_SCALARS, *_NATIVE_VARIABLE)):
                    self.reasons.add("unsupported_value")
                    continue
                cost = 1
                # nbytes includes parent buffers. Compact just this logical cell,
                # never the table, before measuring variable-width allocation.
                # The native API necessarily copies a single oversized cell here;
                # the size check still prevents expanding that cell into Python.
                if dtype.startswith(_NATIVE_VARIABLE):
                    column = column.compact()
                    allocation = column.nbytes
                else:
                    allocation = 32
            else:
                name = sliced.schema.field(column_index).name
                cost = _pyarrow_cost(column, work.nodes_left)
                if cost is None:
                    self.reasons.add("max_result_nodes")
                    work.nodes_left = 0
                    self.exhausted = True
                    break
                allocation = column.nbytes + cost * 16
            if len(name) > work.bytes_left or allocation > work.bytes_left - len(name):
                self.reasons.add("max_result_bytes")
                continue
            work.nodes_left -= cost
            work.bytes_left -= allocation + len(name)
            row[name] = column.to_pylist()[0]
        self.rows.append(row)
        return row


@dataclass(slots=True)
class _ArrowRows:
    source: _ArrowSource


@dataclass(slots=True)
class _Metadata:
    value: dict[Any, Any]


@dataclass(slots=True)
class _Built:
    value: Any
    size: int
    nodes: int = 1


class _Projection:
    def __init__(self, options: BloombergToolsOptions, rows: int, work: _Materialization) -> None:
        self.options = options
        self.max_rows = rows
        self.arrow_rows_left = rows
        self.work = work
        self.nodes_left = max(1, options.max_result_nodes - 9)
        self.reasons: set[str] = set()
        self.has_errors = False
        self.ancestors: set[int] = set()

    def string(self, value: str, budget: int) -> _Built | object:
        if budget < 2:
            self.reasons.add("max_result_bytes")
            return _OMIT
        end = min(len(value), self.options.max_string_chars, budget - 2)
        if len(value) > self.options.max_string_chars:
            self.reasons.add("max_string_chars")
        # Work only on a bounded prefix; escaping can expand a character twelvefold.
        prefix = value[:end]
        while _string_size(prefix, budget) > budget:
            # Binary search avoids repeatedly copying successively shorter strings.
            low, high = 0, len(prefix)
            while low < high:
                middle = (low + high + 1) // 2
                if _string_size(prefix[:middle], budget) <= budget:
                    low = middle
                else:
                    high = middle - 1
            prefix = prefix[:low]
        if len(prefix) < min(len(value), self.options.max_string_chars):
            self.reasons.add("max_result_bytes")
        return _Built(prefix, len(_json(prefix)))

    def binary(
        self,
        value: bytes | bytearray | str,
        budget: int,
        original_length: int | None = None,
        was_truncated: bool = False,
    ) -> _Built | object:
        if self.nodes_left < 4:
            self.reasons.add("max_result_nodes")
            return _Built(None, 4) if budget >= 4 else _OMIT
        length = len(value) if original_length is None else original_length
        result = {"encoding": "base64", "data": "", "byteLength": length, "truncated": was_truncated}
        overhead = len(_json(result))
        if overhead > budget:
            self.reasons.add("max_result_bytes")
            return _OMIT
        available = min(self.options.max_string_chars, budget - overhead)
        encoded_length = len(value) if isinstance(value, str) else 4 * ((len(value) + 2) // 3)
        retained = min(encoded_length, (available // 4) * 4)
        if retained < encoded_length:
            result["truncated"] = True
            self.reasons.add(
                "max_string_chars" if encoded_length > self.options.max_string_chars else "max_result_bytes"
            )
        result["data"] = (
            value[:retained]
            if isinstance(value, str)
            else base64.b64encode(value[: (retained // 4) * 3]).decode("ascii")
        )
        self.nodes_left -= 4
        self.work.binary_values[id(result)] = result
        return _Built(result, len(_json(result)), 5)

    def build(self, value: Any, budget: int, depth: int = 1, metadata: bool = False) -> _Built | object:
        if self.nodes_left <= 0:
            self.reasons.add("max_result_nodes")
            return _OMIT
        self.nodes_left -= 1
        if budget < 2:
            self.reasons.add("max_result_bytes")
            return _OMIT
        if depth >= _MAX_DEPTH:
            self.reasons.add("max_result_depth")
            value = None
        if isinstance(value, str):
            return self.string(value, budget)
        if isinstance(value, (bytes, bytearray)):
            if metadata:
                prefix = value[: min(self.options.max_string_chars * 4, budget)]
                if len(prefix) < len(value):
                    self.reasons.add("max_string_chars")
                try:
                    decoded = prefix.decode("utf-8")
                except UnicodeDecodeError:
                    self.reasons.add("unsupported_value")
                    decoded = prefix.decode("utf-8", errors="replace")
                return self.string(decoded, budget)
            return self.binary(value, budget)
        if isinstance(value, (datetime, date, time)):
            return self.string(value.isoformat(), budget)
        if isinstance(value, timedelta):
            return self.string(str(value), budget)
        if isinstance(value, Decimal):
            if not value.is_finite():
                self.reasons.add("unsupported_value")
                value = None
            elif value.__sizeof__() > self.options.max_string_chars + 128:
                self.reasons.add("max_string_chars")
                value = None
            else:
                return self.string(str(value), budget)
        if isinstance(value, float) and not math.isfinite(value):
            self.reasons.add("unsupported_value")
            value = None
        if type(value) is int and value.bit_length() > min(self.options.max_string_chars, budget, 640) * 3:
            self.reasons.add("max_string_chars")
            value = None
        if value is None or type(value) in (bool, int, float):
            size = len(_json(value))
            if size <= budget:
                return _Built(value, size)
            self.reasons.add("max_result_bytes")
            return _OMIT
        identity = id(value)
        if identity in self.ancestors:
            self.reasons.add("circular_reference")
            return _Built(None, 4) if budget >= 4 else _OMIT
        self.ancestors.add(identity)
        try:
            if _arrow_kind(value):
                source = self.work.source(value)
                record = dict(source.diagnostics)
                if source.metadata:
                    record["metadata"] = _Metadata(source.metadata)
                if depth > 1:
                    record["rowCount"] = source.count
                rows = _ArrowRows(source)
                if record:
                    record["rows"] = rows
                    result = self.mapping(record, budget, depth, metadata)
                else:
                    result = self.sequence(rows, budget, depth, metadata)
                self.reasons.update(source.reasons)
                return result
            if isinstance(value, _ArrowRows):
                return self.sequence(value, budget, depth, metadata)
            if isinstance(value, _Metadata):
                return self.mapping(value.value, budget, depth, True, raw_metadata=True)
            if self.work.binary_values.get(id(value)) is value:
                return self.binary(value["data"], budget, value["byteLength"], value["truncated"])
            if isinstance(value, dict):
                return self.mapping(value, budget, depth, metadata)
            if isinstance(value, (list, tuple)):
                return self.sequence(value, budget, depth, metadata)
            self.reasons.add("unsupported_value")
            return _Built(None, 4) if budget >= 4 else _OMIT
        finally:
            self.ancestors.remove(identity)

    def mapping(
        self, value: dict[Any, Any], budget: int, depth: int, metadata: bool, raw_metadata: bool = False
    ) -> _Built:
        result: dict[str, Any] = {}
        size, nodes = 2, 1
        # Probe known diagnostics even after the byte/node quota is exhausted.
        # A bounded diagnostic probe must not depend on source insertion order.
        for key in _ERROR_KEYS:
            if key in value and _reported_error(value[key]):
                self.has_errors = True
        if value.get("hasErrors") is True:
            self.has_errors = True
        if value.get("truncated") is True or value.get("truncatedInput") is True:
            self.reasons.add("upstream_truncation")

        def keys() -> Any:
            for key in _PRIORITY_KEYS:
                if key in value:
                    yield key
            for key in value:
                if key not in _PRIORITY_KEYS:
                    yield key

        for key in keys():
            if self.nodes_left <= 0:
                self.reasons.add("max_result_nodes")
                break
            self.nodes_left -= 1  # Even omitted/non-string keys consume inspection work.
            original_key = key
            if raw_metadata:
                if key in _METADATA_KEYS or key in (
                    b"xbbg.security_errors",
                    b"xbbg.field_exceptions",
                    b"xbbg.eid_data",
                ):
                    continue
                if isinstance(key, bytes):
                    if len(key) > self.options.max_string_chars:
                        self.reasons.add("max_string_chars")
                        continue
                    try:
                        key = key.decode("utf-8")
                    except UnicodeDecodeError:
                        self.reasons.add("unsupported_value")
                        key = key.decode("utf-8", errors="replace")
            if not isinstance(key, str):
                self.reasons.add("unsupported_value")
                continue
            if len(key) > self.options.max_string_chars:
                self.reasons.add("max_string_chars")
                continue
            available = budget - size - (2 if result else 0) - 2
            key_size = _string_size(key, available)
            if key_size + 2 > available:
                self.reasons.add("max_result_bytes")
                break
            child = self.build(
                value[original_key],
                available - key_size,
                depth + 1,
                metadata
                or key in (*_ERROR_KEYS, "eidData", "eidDataTruncation", "failedEids", "diagnostics", "metadata"),
            )
            if child is _OMIT:
                continue
            result[key] = child.value
            size += (2 if len(result) > 1 else 0) + key_size + 2 + child.size
            nodes += child.nodes
        return _Built(result, size, nodes)

    def sequence(self, value: Any, budget: int, depth: int, metadata: bool) -> _Built:
        arrow = isinstance(value, _ArrowRows)
        count = value.source.count if arrow else len(value)
        limit = min(count, self.arrow_rows_left if arrow else count if metadata else self.max_rows)
        if limit < count:
            self.reasons.add("max_rows")
        result: list[Any] = []
        size, nodes = 2, 1
        for index in range(limit):
            if self.nodes_left <= 0:
                self.reasons.add("max_result_nodes")
                break
            available = budget - size - (2 if result else 0)
            if available < 2:
                self.reasons.add("max_result_bytes")
                break
            row = value.source.row(index) if arrow else value[index]
            if arrow and row is None:
                self.reasons.update(value.source.reasons)
                break
            child = self.build(row, available, depth + 1, metadata)
            if child is _OMIT:
                break
            result.append(child.value)
            size += (2 if len(result) > 1 else 0) + child.size
            nodes += child.nodes
            if arrow:
                self.arrow_rows_left -= 1
        return _Built(result, size, nodes)


def _envelope(
    name: str, data: Any, count: int | None, reasons: set[str], has_errors: bool, reason_limit: int
) -> dict[str, Any]:
    envelope: dict[str, Any] = {"tool": name, "data": data, "rowCount": count, "truncated": bool(reasons)}
    if reasons and reason_limit > 0:
        envelope["truncation"] = {"reasons": [reason for reason in _REASON_ORDER if reason in reasons][:reason_limit]}
    if has_errors:
        envelope["hasErrors"] = True
    return envelope


def _disable_chart(built: _Built) -> _Built:
    data = dict(built.value) if isinstance(built.value, dict) else {}
    data["spec"] = None
    data["renderable"] = False
    # A conservative upper bound is sufficient for reserving envelope nodes.
    return _Built(data, len(_json(data)), built.nodes + 2)


def _finish(
    name: str, count: int | None, built: _Built, projection: _Projection, cap: int, has_errors: bool, chart: bool
) -> dict[str, Any]:
    reasons = projection.reasons
    if chart and reasons.difference({"upstream_truncation"}):
        built = _disable_chart(built)
    reason_limit = min(len(reasons), max(0, projection.options.max_result_nodes - built.nodes - 8))
    envelope = _envelope(name, built.value, count, reasons, has_errors, reason_limit)
    if len(_json(envelope)) <= cap:
        return envelope
    reasons.add("max_result_bytes")
    if chart:
        built = _disable_chart(built)
    reason_limit = min(len(reasons), max(0, projection.options.max_result_nodes - built.nodes - 8))
    # Fit metadata first, then project only the already bounded Python result.
    # This second walk never invokes Arrow conversion or an external accessor.
    empty = _envelope(name, None, count, reasons, has_errors, reason_limit)
    while len(_json(empty)) > cap and reason_limit:
        reason_limit -= 1
        empty = _envelope(name, None, count, reasons, has_errors, reason_limit)
    available = cap - len(_json(empty)) + 4
    clipped = _Projection(projection.options, projection.max_rows, projection.work)
    clipped_value = clipped.build(built.value, available)
    data = None if clipped_value is _OMIT else clipped_value.value
    return _envelope(name, data, count, reasons, has_errors, reason_limit)


def create_tool_result(name: str, value: Any, options: BloombergToolsOptions) -> tuple[str, dict[str, Any]]:
    """Return JSON model content and a JSON-safe artifact, independently bounded.

    Unknown row totals are ``None``. Diagnostics get priority over data, including
    empty tables; error presence survives diagnostic clipping. Unsupported values,
    cycles, nonfinite numbers and all information loss are marked as truncation.
    """
    count = _row_count(value)
    chart = isinstance(value, dict) and value.get("kind") == "xbbg.visualization"
    work = _Materialization(
        max(options.max_rows, options.max_content_rows),
        options.max_result_nodes,
        max(options.max_result_bytes, options.max_content_bytes),
    )
    projections: list[tuple[_Projection, _Built, int]] = []
    for rows, cap in (
        (options.max_rows, options.max_result_bytes),
        (options.max_content_rows, options.max_content_bytes),
    ):
        projection = _Projection(options, rows, work)
        reserve = len(_json(_envelope(name, None, count, {"max_result_bytes"}, True, 1))) - 4
        if cap - reserve < 4:
            raise ValueError("tool name and result envelope exceed the configured byte limit")
        built = projection.build(value, cap - reserve)
        projections.append((projection, _Built(None, 4) if built is _OMIT else built, cap))
    has_errors = any(projection.has_errors for projection, _, _ in projections)
    artifact, content_envelope = (
        _finish(name, count, built, projection, cap, has_errors, chart) for projection, built, cap in projections
    )
    return _json(content_envelope), artifact
