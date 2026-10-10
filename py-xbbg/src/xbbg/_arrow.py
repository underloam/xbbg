"""Native Arrow carrier recognition, coercion, and logical schema adaptation.

Storage and table operations stay in Rust. Importing this module neither loads
the native extension nor imports optional DataFrame backends.
"""

from __future__ import annotations

from importlib import import_module
import re
from typing import TYPE_CHECKING, Any, Literal

from narwhals._utils import Version

if TYPE_CHECKING:
    from xbbg._core import ArrowField, ArrowRecordBatch, ArrowSchema, ArrowTable

__all__ = ["ArrowField", "ArrowRecordBatch", "ArrowSchema", "ArrowTable"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        return getattr(import_module("xbbg._core"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def is_arrow_table(value: Any) -> bool:
    """Recognize an xbbg native table without loading the extension."""
    return value.__class__.__name__ == "ArrowTable" and hasattr(value, "__arrow_c_stream__")


def is_arrow_record_batch(value: Any) -> bool:
    """Recognize an xbbg native batch without loading the extension."""
    return value.__class__.__name__ == "ArrowRecordBatch" and hasattr(value, "__arrow_c_array__")


def is_pyarrow_table(value: Any) -> bool:
    """Recognize an optional PyArrow table without importing PyArrow."""
    return value.__class__.__module__.startswith("pyarrow.") and value.__class__.__name__ == "Table"


def _is_pyarrow_record_batch(value: Any) -> bool:
    return value.__class__.__module__.startswith("pyarrow.") and value.__class__.__name__ == "RecordBatch"


def ensure_arrow_table(frame: Any, *, native_only: bool = False) -> Any:
    """Coerce Arrow batches to tables, optionally accepting only xbbg carriers."""
    if is_arrow_table(frame):
        return frame
    if is_arrow_record_batch(frame):
        return frame.to_table()
    if not native_only:
        if is_pyarrow_table(frame):
            return frame
        if _is_pyarrow_record_batch(frame):
            import pyarrow as pa

            return pa.Table.from_batches([frame])
    raise TypeError(f"Expected xbbg ArrowTable or ArrowRecordBatch, got {type(frame).__name__}")


_SIMPLE_NATIVE_DTYPES = {
    "Boolean": "Boolean",
    "Int8": "Int8",
    "Int16": "Int16",
    "Int32": "Int32",
    "Int64": "Int64",
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "Float16": "Float16",
    "Float32": "Float32",
    "Float64": "Float64",
    "Utf8": "String",
    "LargeUtf8": "String",
    "Utf8View": "String",
    "Binary": "Binary",
    "LargeBinary": "Binary",
    "BinaryView": "Binary",
}
_TIME_UNITS: dict[str, Literal["s", "ms", "us", "ns"]] = {
    "s": "s",
    "ms": "ms",
    "µs": "us",
    "ns": "ns",
}


def native_dtype(data_type: str, version: Version) -> Any:
    """Translate a Rust Arrow logical type to the requested Narwhals version."""
    dtypes = version.dtypes
    dtype_name = _SIMPLE_NATIVE_DTYPES.get(data_type)
    if dtype_name is not None:
        return getattr(dtypes, dtype_name)()
    if data_type in {"Date32", "Date64"}:
        return dtypes.Date()
    if data_type.startswith(("Time32(", "Time64(")):
        return dtypes.Time()

    match = re.fullmatch(
        r'Timestamp\((s|ms|µs|ns)(?:, "(.*)")?\)',
        data_type,
    )
    if match is not None:
        return dtypes.Datetime(_TIME_UNITS[match.group(1)], match.group(2))

    match = re.fullmatch(
        r"Duration\((s|ms|µs|ns)\)",
        data_type,
    )
    if match is not None:
        return dtypes.Duration(_TIME_UNITS[match.group(1)])

    match = re.fullmatch(r"Decimal128\((\d+), (-?\d+)\)", data_type)
    if match is not None and int(match.group(2)) >= 0:
        return dtypes.Decimal(precision=int(match.group(1)), scale=int(match.group(2)))

    raise TypeError(f"unsupported native Arrow dtype {data_type!r}")


def native_schema(table: Any, version: Version = Version.V1) -> dict[str, Any]:
    """Adapt native field metadata without requiring PyArrow."""
    schema: dict[str, Any] = {}
    for field in table.schema.fields:
        try:
            schema[field.name] = native_dtype(field.data_type, version)
        except TypeError as exc:
            raise TypeError(
                f"cannot convert native Arrow column {field.name!r} with dtype {field.data_type!r} without PyArrow"
            ) from exc
    return schema
