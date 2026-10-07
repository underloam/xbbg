"""Narwhals plugin implementation for xbbg native Arrow objects.

Eager dataframe operations delegate to ``xbbg.ArrowTable`` instead of
materializing through another dataframe library. Lazy conversion deliberately
crosses into Polars, the supported deferred-execution backend.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
import operator
from types import ModuleType
from typing import Any, Literal, overload

from narwhals._utils import Implementation, Version

from xbbg._arrow import ensure_arrow_table, is_arrow_record_batch, is_arrow_table, native_schema
from xbbg.backend import Backend, _import_backend_module, _to_polars_frame


def _arrow_table_class() -> type[Any]:
    from xbbg._core import ArrowTable

    return ArrowTable


def _arrow_record_batch_class() -> type[Any]:
    from xbbg._core import ArrowRecordBatch

    return ArrowRecordBatch


def _native_namespace() -> ModuleType:
    import xbbg

    return xbbg


def _flatten_columns(columns: Sequence[str] | Sequence[Iterable[str]]) -> list[str]:
    out: list[str] = []
    for item in columns:
        if isinstance(item, str):
            out.append(item)
        else:
            out.extend(str(value) for value in item)
    return out


class XbbgNamespace:
    """Minimal Narwhals namespace for xbbg native Arrow frames."""

    _implementation = Implementation.UNKNOWN

    def __init__(self, *, version: Version) -> None:
        self._version = version

    @property
    def _expr(self) -> type[Any]:
        raise NotImplementedError("xbbg Narwhals expression execution is not implemented for this operation")

    def from_native(self, data: Any, /) -> XbbgDataFrame:
        return XbbgDataFrame(data, version=self._version)

    def is_native(self, obj: Any, /) -> bool:
        return is_arrow_table(obj) or is_arrow_record_batch(obj)

    def concat(self, items: Iterable[XbbgDataFrame], *, how: str) -> XbbgDataFrame:
        if how != "vertical":
            raise NotImplementedError("xbbg Narwhals plugin currently supports vertical concat only")
        tables = [item.native for item in items]
        return XbbgDataFrame(_arrow_table_class().concat_tables(tables), version=self._version)


class XbbgDataFrame:
    """Narwhals-compliant eager frame backed by ``xbbg.ArrowTable``."""

    _implementation = Implementation.UNKNOWN

    def __init__(self, table: Any, *, version: Version) -> None:
        self._native_frame = ensure_arrow_table(table, native_only=True)
        self._version = version

    def __narwhals_dataframe__(self) -> XbbgDataFrame:
        return self

    def __narwhals_namespace__(self) -> XbbgNamespace:
        return XbbgNamespace(version=self._version)

    def __native_namespace__(self) -> ModuleType:
        return _native_namespace()

    @classmethod
    def from_native(cls, data: Any, /, *, context: Any) -> XbbgDataFrame:
        return cls(data, version=context._version)

    def to_narwhals(self) -> Any:
        return self._version.dataframe(self, level="full")

    @property
    def native(self) -> Any:
        return self._native_frame

    @property
    def columns(self) -> list[str]:
        return list(self.native.column_names)

    @property
    def schema(self) -> Mapping[str, Any]:
        return self.collect_schema()

    @property
    def shape(self) -> tuple[int, int]:
        return (self.native.num_rows, self.native.num_columns)

    def __len__(self) -> int:
        return self.native.num_rows

    def _with_native(self, table: Any) -> XbbgDataFrame:
        return type(self)(table, version=self._version)

    def _with_version(self, version: Version) -> XbbgDataFrame:
        return type(self)(self.native, version=version)

    def collect_schema(self) -> Mapping[str, Any]:
        return native_schema(self.native, self._version)

    def clone(self) -> XbbgDataFrame:
        return self._with_native(self.native)

    def simple_select(self, *column_names: str) -> XbbgDataFrame:
        return self._with_native(self.native.select_columns(list(column_names)))

    def select(self, *exprs: Any) -> XbbgDataFrame:
        if all(isinstance(expr, str) for expr in exprs):
            return self.simple_select(*(str(expr) for expr in exprs))
        raise NotImplementedError("xbbg Narwhals plugin supports direct string column selection only")

    def drop(self, columns: Sequence[str] | Sequence[Iterable[str]], *, strict: bool) -> XbbgDataFrame:
        names = _flatten_columns(columns)
        if strict:
            missing = [name for name in names if name not in self.columns]
            if missing:
                raise KeyError(f"unknown columns: {missing}")
        return self._with_native(self.native.drop_columns(names))

    def rename(self, mapping: Mapping[str, str]) -> XbbgDataFrame:
        return self._with_native(self.native.rename_columns(dict(mapping)))

    def head(self, n: int) -> XbbgDataFrame:
        length = n if n >= 0 else max(len(self) + n, 0)
        return self._with_native(self.native.head(length))

    def sort(self, *by: str, descending: bool | Sequence[bool], nulls_last: bool) -> XbbgDataFrame:
        if isinstance(descending, bool):
            directions = ["descending" if descending else "ascending"] * len(by)
        else:
            if len(descending) != len(by):
                raise ValueError(
                    "descending must contain one value per sort column: "
                    f"got {len(descending)} values for {len(by)} columns"
                )
            directions = ["descending" if value else "ascending" for value in descending]
        return self._with_native(
            self.native.sort_by(
                list(zip(by, directions, strict=True)),
                nulls_last=nulls_last,
            )
        )

    def lazy(self, backend: Any = None, *, session: Any = None) -> Any:
        if backend is not None and backend is not Implementation.POLARS:
            raise NotImplementedError("xbbg native frames can only become lazy through Polars")
        if session is not None:
            raise ValueError("session is not supported when making an xbbg native frame lazy")
        native = _to_polars_frame(self.native, feature="XbbgDataFrame.lazy()").lazy()
        from narwhals._polars.dataframe import PolarsLazyFrame

        return PolarsLazyFrame(native, version=self._version, validate_backend_version=True)

    def to_pandas(self) -> Any:
        pd = _import_backend_module(Backend.PANDAS, feature="XbbgDataFrame.to_pandas()")
        try:
            pa = _import_backend_module(Backend.PYARROW, feature="XbbgDataFrame.to_pandas()")
        except ImportError:
            # Slow fallback for installations without pyarrow: materializes one
            # Python dict per cell path instead of consuming the Arrow C stream.
            return pd.DataFrame.from_records(self.native.to_pylist(), columns=self.columns)

        return pa.table(self.native).to_pandas(split_blocks=True)

    def to_arrow(self) -> Any:
        pa = _import_backend_module(Backend.PYARROW, feature="XbbgDataFrame.to_arrow()")

        return pa.table(self.native)

    def to_polars(self) -> Any:
        return _to_polars_frame(self.native, feature="XbbgDataFrame.to_polars()")

    def to_dict(self, *, as_series: bool) -> dict[str, Any]:
        if as_series:
            raise NotImplementedError("xbbg Narwhals plugin does not expose Series objects yet")
        return {name: self.native.column(name).to_pylist() for name in self.columns}

    def rows(self, *, named: bool) -> Sequence[tuple[Any, ...]] | Sequence[Mapping[str, Any]]:
        if named:
            return self.native.to_pylist()
        return list(self.iter_rows(named=False, buffer_size=512))

    @overload
    def iter_rows(self, *, named: Literal[False], buffer_size: int) -> Iterator[tuple[Any, ...]]: ...

    @overload
    def iter_rows(self, *, named: Literal[True], buffer_size: int) -> Iterator[Mapping[str, Any]]: ...

    @overload
    def iter_rows(
        self, *, named: bool, buffer_size: int
    ) -> Iterator[tuple[Any, ...]] | Iterator[Mapping[str, Any]]: ...

    def iter_rows(self, *, named: bool, buffer_size: int) -> Iterator[tuple[Any, ...]] | Iterator[Mapping[str, Any]]:
        if buffer_size <= 0:
            raise ValueError("buffer_size must be positive")
        names = self.columns
        columns = [self.native.column(name) for name in names]
        num_rows = len(self)
        if not names:
            for _ in range(num_rows):
                yield {} if named else ()
            return
        for offset in range(0, num_rows, buffer_size):
            length = min(buffer_size, num_rows - offset)
            buffers = [column.slice(offset, length).to_pylist() for column in columns]
            for values in zip(*buffers, strict=True):
                yield dict(zip(names, values, strict=True)) if named else values

    def row(self, index: int) -> tuple[Any, ...]:
        names = self.columns
        if not names:
            range(len(self))[operator.index(index)]
            return ()
        return tuple(self.native.column(name)[index] for name in names)

    def item(self, row: int | None, column: int | str | None) -> Any:
        if row is None and column is None:
            if self.shape != (1, 1):
                raise ValueError(f"item() without row and column requires a 1x1 dataframe, got shape {self.shape}")
            row = 0
            column = 0
        elif row is None or column is None:
            raise ValueError("item() requires both row and column when either is provided")
        name = column if isinstance(column, str) else self.columns[column]
        return self.native.column(name)[row]
