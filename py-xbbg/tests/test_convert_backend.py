"""Tests for conversion from xbbg native Arrow objects to public backends."""

from __future__ import annotations

import builtins
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time
import importlib.util
import os
from typing import Any
from uuid import uuid4

import narwhals.stable.v1 as nw
import pytest

from xbbg import blp
from xbbg._core import ArrowTable
from xbbg.backend import _attach_xbbg_metadata_attrs, _convert_result_backend, check_backend, convert_backend_frame
from xbbg.blp import Backend


@pytest.fixture
def arrow_table() -> Any:
    return ArrowTable.from_pylist(
        [
            {"ticker": "AAPL US Equity", "date": "2024-01-01", "px_last": 150.0},
            {"ticker": "MSFT US Equity", "date": "2024-01-01", "px_last": 380.0},
        ]
    )


def _block_imports(monkeypatch: pytest.MonkeyPatch, *roots: str) -> None:
    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        root = name.split(".", 1)[0]
        if root in roots:
            raise ImportError(f"blocked optional dataframe backend: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


class TestConvertBackendNative:
    def test_convert_native_returns_identity(self, arrow_table: Any):
        result = convert_backend_frame(arrow_table, Backend.NATIVE)
        assert result is arrow_table

    def test_facade_conversion_defaults_to_narwhals(self, arrow_table: Any):
        result = _convert_result_backend(arrow_table, None)
        assert isinstance(result, nw.DataFrame)

        native = result.to_native()
        if importlib.util.find_spec("pyarrow") is not None and check_backend(Backend.PYARROW, raise_on_error=False):
            pa = pytest.importorskip("pyarrow")
            assert isinstance(native, pa.Table)
            assert native.column_names == arrow_table.column_names
        elif importlib.util.find_spec("pandas") is not None and check_backend(Backend.PANDAS, raise_on_error=False):
            pd = pytest.importorskip("pandas")
            assert isinstance(native, pd.DataFrame)
            assert list(native.columns) == arrow_table.column_names
        elif importlib.util.find_spec("polars") is not None and check_backend(Backend.POLARS, raise_on_error=False):
            pl = pytest.importorskip("polars")
            assert isinstance(native, pl.DataFrame)
            assert native.columns == arrow_table.column_names
        else:
            assert native is arrow_table

    def test_facade_conversion_honors_set_backend(self, arrow_table: Any):
        original = blp.get_backend()
        try:
            blp.set_backend(Backend.NATIVE)
            assert _convert_result_backend(arrow_table, None) is arrow_table
        finally:
            blp.set_backend(original)

    def test_convert_record_batch_to_native_table(self, arrow_table: Any):
        batch = arrow_table.to_batches()[0]
        result = convert_backend_frame(batch, Backend.NATIVE)
        assert result.column_names == arrow_table.column_names
        assert result.to_pylist() == arrow_table.to_pylist()

    def test_convert_pyarrow_record_batch_to_table(self):
        pa = pytest.importorskip("pyarrow")
        batch = pa.record_batch(
            [["IBM US Equity"], [123.45]],
            names=["ticker", "px_last"],
        )

        result = convert_backend_frame(batch, Backend.NATIVE)

        assert isinstance(result, pa.Table)
        assert result.column_names == ["ticker", "px_last"]
        assert result.to_pylist() == [{"ticker": "IBM US Equity", "px_last": 123.45}]

    def test_record_batch_and_table_indexing_return_arrow_columns(self, arrow_table: Any):
        batch = arrow_table.to_batches()[0]

        batch_column = batch["ticker"]
        assert batch_column.name == "ticker"
        assert batch_column.to_pylist() == ["AAPL US Equity", "MSFT US Equity"]
        assert batch_column[0] == "AAPL US Equity"
        assert batch_column[-1] == "MSFT US Equity"
        assert len(batch_column) == 2
        assert len(batch) == 2

        table_column = arrow_table["px_last"]
        assert table_column.name == "px_last"
        assert table_column.to_pylist() == [150.0, 380.0]
        assert len(arrow_table) == 2

    def test_rejects_non_native_inputs(self):
        with pytest.raises(TypeError, match="Expected xbbg ArrowTable or ArrowRecordBatch"):
            convert_backend_frame({"ticker": ["IBM"]}, Backend.NATIVE)


@pytest.mark.parametrize(
    "backend",
    [Backend.NATIVE, Backend.PYARROW, Backend.PANDAS, Backend.POLARS, Backend.NARWHALS],
)
def test_temporal_backend_conversion_preserves_precision_nulls_and_logical_types(backend):
    pa = pytest.importorskip("pyarrow")
    if not check_backend(backend, raise_on_error=False):
        pytest.skip(f"{backend.value} is not usable in this environment")
    rows = [
        {"value_time": time(15, 59, 1, 123456), "value_date": date(2026, 1, 2)},
        {"value_time": time(0, 0), "value_date": date(1970, 1, 1)},
        {"value_time": None, "value_date": None},
    ]
    # A typed Arrow batch exercises the accepted Arrow input path without a
    # Bloomberg request; from_pylist cannot construct native Time64 columns.
    schema = pa.schema([("value_time", pa.time64("us")), ("value_date", pa.date32())])
    batch = pa.RecordBatch.from_pylist(rows, schema=schema)

    result = convert_backend_frame(batch, backend)

    if backend is Backend.PANDAS:
        pd = pytest.importorskip("pandas")
        assert isinstance(result, pd.DataFrame)
        converted = pa.Table.from_pandas(result, preserve_index=False)
    elif backend is Backend.POLARS:
        pl = pytest.importorskip("polars")
        assert result.schema == {"value_time": pl.Time, "value_date": pl.Date}
        converted = result.to_arrow()
    elif backend is Backend.NARWHALS:
        assert result.schema == {"value_time": nw.Time, "value_date": nw.Date}
        converted = result.to_arrow()
    else:
        converted = pa.table(result)

    assert converted.to_pylist() == rows
    # Polars stores its logical Time at nanosecond resolution; conversion must
    # preserve the original microseconds without inventing a date or timezone.
    unit = "ns" if backend is Backend.POLARS else "us"
    assert converted.schema.field("value_time").type == pa.time64(unit)
    assert converted.schema.field("value_date").type == pa.date32()


def test_native_date32_conversion_preserves_dates_and_nulls():
    pa = pytest.importorskip("pyarrow")
    rows = [{"value_date": date(2026, 1, 2)}, {"value_date": None}]
    table = ArrowTable.from_pylist(rows)

    result = convert_backend_frame(table, Backend.NATIVE)

    assert result.to_pylist() == rows
    assert pa.table(result).schema.field("value_date").type == pa.date32()


class TestConvertBackendPyArrow:
    def test_convert_pyarrow_returns_table(self, arrow_table: Any):
        pa = pytest.importorskip("pyarrow")
        result = convert_backend_frame(arrow_table, Backend.PYARROW)
        assert isinstance(result, pa.Table)
        assert result.column_names == arrow_table.column_names
        assert result.num_rows == arrow_table.num_rows


class TestConvertBackendPandas:
    def test_convert_pandas_preserves_values(self, arrow_table: Any):
        pd = pytest.importorskip("pandas")

        result = convert_backend_frame(arrow_table, Backend.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert result.shape == (arrow_table.num_rows, arrow_table.num_columns)
        assert list(result.columns) == arrow_table.column_names
        assert result["ticker"].tolist() == ["AAPL US Equity", "MSFT US Equity"]
        assert result["px_last"].tolist() == [150.0, 380.0]

    def test_attach_xbbg_metadata_attrs_copies_present_native_metadata(self):
        pd = pytest.importorskip("pandas")

        class FakeTable:
            metadata = {
                "xbbg.eid_data": '{"IBM US Equity":[101,202]}',
                "xbbg.security_errors": '{"BAD Ticker":{"message":"bad security"}}',
            }
            eid_data = {"IBM US Equity": [101, 202]}
            security_errors = {"BAD Ticker": {"message": "bad security"}}
            field_exceptions = None

        frame = pd.DataFrame({"ticker": ["IBM US Equity"]})

        result = _attach_xbbg_metadata_attrs(frame, FakeTable())

        assert result is frame
        assert frame.attrs["xbbg_eid_data"] == {"IBM US Equity": [101, 202]}
        assert frame.attrs["xbbg_security_errors"] == {"BAD Ticker": {"message": "bad security"}}
        assert "xbbg_field_exceptions" not in frame.attrs

    def test_convert_pandas_falls_back_when_pyarrow_import_raises(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch
    ):
        pd = pytest.importorskip("pandas")
        import xbbg.backend as backend_module

        real_import_backend = backend_module._import_backend_module

        def import_backend(backend: Backend | str, *, feature: str | None = None) -> Any:
            if Backend(backend) is Backend.PYARROW:
                raise ImportError("pyarrow unavailable")
            return real_import_backend(backend, feature=feature)

        monkeypatch.setattr(backend_module, "_import_backend_module", import_backend)

        result = convert_backend_frame(arrow_table, Backend.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert result.shape == (arrow_table.num_rows, arrow_table.num_columns)
        assert list(result.columns) == arrow_table.column_names
        assert result["ticker"].tolist() == ["AAPL US Equity", "MSFT US Equity"]
        assert result["px_last"].tolist() == [150.0, 380.0]


class TestConvertBackendPolars:
    def test_convert_polars_returns_dataframe(self, arrow_table: Any):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.POLARS, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")
        result = convert_backend_frame(arrow_table, Backend.POLARS)
        assert isinstance(result, pl.DataFrame)
        assert result.columns == arrow_table.column_names

    def test_convert_polars_preserves_native_arrow_chunks(self):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.POLARS, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")
        table = ArrowTable.from_batches(
            [
                ArrowTable.from_pylist([{"value": 1}]).to_batches()[0],
                ArrowTable.from_pylist([{"value": 2}]).to_batches()[0],
            ]
        )

        result = convert_backend_frame(table, Backend.POLARS)

        assert isinstance(result, pl.DataFrame)
        assert result["value"].n_chunks() == 2

    def test_legacy_polars_without_pyarrow_preserves_schema_and_batches(self, monkeypatch: pytest.MonkeyPatch):
        pl = pytest.importorskip("polars")
        import xbbg.backend as backend_module

        timestamp = datetime(2024, 1, 1, 12, 30, 1, 123456, tzinfo=UTC)
        source = ArrowTable.from_pylist(
            [
                {"value": 1, "when": date(2024, 1, 1), "timestamp": timestamp},
                {"value": None, "when": None, "timestamp": None},
            ]
        )
        table = ArrowTable.from_batches(
            [
                source.slice(0, 1).to_batches()[0],
                source.slice(1, 1).to_batches()[0],
            ]
        )
        assert nw.from_native(table).schema["timestamp"] == nw.Datetime("us", "UTC")
        real_import_backend = backend_module._import_backend_module

        def import_backend(backend: Backend | str, *, feature: str | None = None) -> Any:
            if Backend(backend) is Backend.PYARROW:
                raise ImportError("pyarrow unavailable")
            return real_import_backend(backend, feature=feature)

        monkeypatch.setattr(backend_module, "_import_backend_module", import_backend)
        monkeypatch.setattr(backend_module, "_get_module_version", lambda _module: (0, 20, 4))

        result = backend_module._to_polars_frame(table)
        empty = backend_module._to_polars_frame(source.slice(0, 0))

        assert result.schema == pl.Schema({"value": pl.Int64, "when": pl.Date, "timestamp": pl.Datetime("us", "UTC")})
        assert result.to_dicts() == [
            {"value": 1, "when": date(2024, 1, 1), "timestamp": timestamp},
            {"value": None, "when": None, "timestamp": None},
        ]
        assert result["value"].n_chunks() == 2
        assert empty.schema == result.schema
        assert empty.height == 0

        class UnsupportedField:
            name = "nested"
            data_type = 'List(Field { name: "item", data_type: Int64 })'

        class UnsupportedSchema:
            fields = [UnsupportedField()]

        class UnsupportedTable:
            schema = UnsupportedSchema()

            def to_batches(self) -> list[Any]:
                raise AssertionError("unsupported schemas must fail before materializing rows")

        with pytest.raises(TypeError):
            backend_module._to_polars_frame(UnsupportedTable())

    def test_convert_polars_lazy_returns_lazyframe(self, arrow_table: Any):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.POLARS_LAZY, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")
        result = convert_backend_frame(arrow_table, Backend.POLARS_LAZY)
        assert isinstance(result, pl.LazyFrame)


class TestConvertBackendDuckDB:
    def test_convert_duckdb_relation(self, arrow_table: Any):
        duckdb = pytest.importorskip("duckdb")
        result = convert_backend_frame(arrow_table, Backend.DUCKDB)
        assert result.fetchone() is not None
        assert result.columns == arrow_table.column_names
        del duckdb

    def test_retained_relations_keep_distinct_sources(self):
        pytest.importorskip("duckdb")
        first_table = ArrowTable.from_pylist([{"value": "first"}])
        second_table = ArrowTable.from_pylist([{"value": "second"}])

        first = convert_backend_frame(first_table, Backend.DUCKDB)
        second = convert_backend_frame(second_table, Backend.DUCKDB)

        assert first.fetchall() == [("first",)]
        assert second.fetchall() == [("second",)]

    def test_relations_share_one_reusable_database(self):
        pytest.importorskip("duckdb")
        first = convert_backend_frame(ArrowTable.from_pylist([{"value": 7}]), Backend.DUCKDB)
        second = convert_backend_frame(ArrowTable.from_pylist([{"value": 8}]), Backend.DUCKDB)
        table_name = f"xbbg_backend_test_{uuid4().hex}"

        first.create(table_name)

        assert second.query("source", f"select * from {table_name}").fetchall() == [(7,)]

    def test_concurrent_relations_keep_distinct_sources(self):
        pytest.importorskip("duckdb")

        def convert(value: int) -> Any:
            table = ArrowTable.from_pylist([{"value": value}])
            return convert_backend_frame(table, Backend.DUCKDB)

        with ThreadPoolExecutor(max_workers=4) as executor:
            relations = list(executor.map(convert, range(16)))

        assert [relation.fetchone()[0] for relation in relations] == list(range(16))

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
    def test_initialized_database_is_rejected_in_forked_child(self):
        pytest.importorskip("duckdb")
        parent_relation = convert_backend_frame(
            ArrowTable.from_pylist([{"value": "parent"}]),
            Backend.DUCKDB,
        )
        read_fd, write_fd = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_fd)
            try:
                convert_backend_frame(
                    ArrowTable.from_pylist([{"value": "child"}]),
                    Backend.DUCKDB,
                )
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
            else:
                message = "conversion unexpectedly succeeded"
            os.write(write_fd, message.encode())
            os.close(write_fd)
            os._exit(0)

        os.close(write_fd)
        _, status = os.waitpid(child_pid, 0)
        message = os.read(read_fd, 8192).decode()
        os.close(read_fd)

        assert os.waitstatus_to_exitcode(status) == 0
        assert message.startswith("RuntimeError:")
        assert parent_relation.fetchall() == [("parent",)]


class TestConvertBackendNarwhals:
    def _block_dataframe_backend_imports(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _block_imports(monkeypatch, "pyarrow", "pandas", "polars", "arro3")
        import xbbg.backend as backend_module

        monkeypatch.setattr(backend_module, "_best_narwhals_backend", lambda: None)
        monkeypatch.setattr("xbbg.backend._native_narwhals_fallback_warned", True)

    def test_convert_narwhals_prefers_pyarrow_when_available(self, arrow_table: Any):
        pa = pytest.importorskip("pyarrow")
        result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        assert isinstance(result, nw.DataFrame)
        native = result.to_native()
        assert isinstance(native, pa.Table)
        assert native.column_names == arrow_table.column_names

    def test_convert_narwhals_falls_back_to_xbbg_plugin(self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch):
        self._block_dataframe_backend_imports(monkeypatch)
        monkeypatch.setattr("xbbg.backend._native_narwhals_fallback_warned", False)
        with pytest.warns(RuntimeWarning, match="limited xbbg native ArrowTable plugin"):
            result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        assert isinstance(result, nw.DataFrame)
        assert result.to_native() is arrow_table
        assert result.columns == arrow_table.column_names

    def test_narwhals_select_delegates_to_xbbg_table_when_native_fallback(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch
    ):
        self._block_dataframe_backend_imports(monkeypatch)
        result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        selected = result.select("ticker", "px_last")
        native = selected.to_native()
        assert native.column_names == ["ticker", "px_last"]
        assert native.to_pylist() == [
            {"ticker": "AAPL US Equity", "px_last": 150.0},
            {"ticker": "MSFT US Equity", "px_last": 380.0},
        ]

    def test_convert_narwhals_lazy_requires_polars(self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch):
        _block_imports(monkeypatch, "polars")

        with pytest.raises(ImportError):
            convert_backend_frame(arrow_table, Backend.NARWHALS_LAZY)

    def test_convert_narwhals_lazy_is_polars_lazy_and_defers_operations(self, arrow_table: Any):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.NARWHALS_LAZY, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")

        result = convert_backend_frame(arrow_table, Backend.NARWHALS_LAZY)
        native = result.to_native()
        executions: list[int] = []
        planned = native.map_batches(lambda batch: executions.append(batch.height) or batch)

        assert isinstance(result, nw.LazyFrame)
        assert isinstance(native, pl.LazyFrame)
        assert executions == []
        assert planned.collect().height == arrow_table.num_rows
        assert executions == [arrow_table.num_rows]

    def test_native_plugin_row_item_and_iteration_are_bounded(self):
        materialized: list[tuple[str, int, int]] = []

        class Column:
            def __init__(
                self,
                name: str,
                values: list[Any],
                *,
                materializable: bool = False,
            ) -> None:
                self._name = name
                self._values = values
                self._materializable = materializable

            def __getitem__(self, index: int) -> Any:
                return self._values[index]

            def slice(self, offset: int, length: int) -> Column:
                materialized.append((self._name, offset, length))
                return Column(
                    self._name,
                    self._values[offset : offset + length],
                    materializable=True,
                )

            def to_pylist(self) -> list[Any]:
                if not self._materializable:
                    raise AssertionError("iteration must materialize bounded column slices")
                return list(self._values)

        class ArrowTable:
            __module__ = "xbbg.testing"
            column_names = ["ticker", "value"]
            num_rows = 2
            num_columns = 2
            _columns = {
                "ticker": Column("ticker", ["AAPL US Equity", "MSFT US Equity"]),
                "value": Column("value", [150.0, 380.0]),
            }

            def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
                del requested_schema
                raise AssertionError("row access must not consume the Arrow stream")

            def column(self, name: str) -> Column:
                return self._columns[name]

            def to_pylist(self) -> list[dict[str, Any]]:
                raise AssertionError("row access must not materialize the table")

        result = nw.from_native(ArrowTable())

        assert result.row(1) == ("MSFT US Equity", 380.0)
        assert result.item(row=0, column=1) == 150.0
        with pytest.raises(ValueError, match="buffer_size must be positive"):
            next(result.iter_rows(named=False, buffer_size=0))
        rows = result.iter_rows(named=True, buffer_size=1)
        assert next(rows) == {"ticker": "AAPL US Equity", "value": 150.0}
        assert materialized == [("ticker", 0, 1), ("value", 0, 1)]

    def test_native_plugin_zero_width_rows_preserve_cardinality_and_bounds(self):
        two_rows = nw.from_native(ArrowTable.from_pylist([{}, {}]))

        assert two_rows.shape == (2, 0)
        assert list(two_rows.iter_rows(named=False)) == [(), ()]
        assert list(two_rows.iter_rows(named=True)) == [{}, {}]
        assert two_rows.row(0) == ()
        assert two_rows.row(-1) == ()
        with pytest.raises(IndexError):
            two_rows.row(2)
        with pytest.raises(IndexError):
            two_rows.row(-3)
        with pytest.raises(TypeError):
            two_rows.row(0.0)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            two_rows.row(slice(None))  # type: ignore[arg-type]

        no_rows = nw.from_native(ArrowTable.from_pylist([]))

        assert no_rows.shape == (0, 0)
        assert list(no_rows.iter_rows(named=False)) == []
        assert list(no_rows.iter_rows(named=True)) == []
        with pytest.raises(IndexError):
            no_rows.row(0)

    def test_native_plugin_matches_head_item_and_null_sort_contract(self):
        table = ArrowTable.from_pylist(
            [
                {"label": "first", "value": 2},
                {"label": "missing", "value": None},
                {"label": "last", "value": 1},
            ]
        )
        result = nw.from_native(table)

        assert result.head(-1).to_native().to_pylist() == table.head(2).to_pylist()
        with pytest.raises(ValueError, match="requires a 1x1 dataframe"):
            result.item()
        assert result.select("value").head(1).item() == 2
        with pytest.raises(ValueError, match="one value per sort column"):
            result.sort("label", "value", descending=[False])
        assert result.select("value").sort("value", nulls_last=False).to_native().column("value").to_pylist() == [
            None,
            1,
            2,
        ]
        assert result.select("value").sort("value", nulls_last=True).to_native().column("value").to_pylist() == [
            1,
            2,
            None,
        ]

    def test_native_plugin_lazy_rejects_unsupported_backend_and_session(self, arrow_table: Any):
        pytest.importorskip("polars")
        result = nw.from_native(arrow_table)

        with pytest.raises(NotImplementedError, match="only become lazy through Polars"):
            result.lazy(backend="duckdb")
        with pytest.raises(ValueError, match="session is not supported"):
            result.lazy(session=object())

        assert isinstance(result.lazy(backend="polars"), nw.LazyFrame)


class TestConvertBackendInvalid:
    def test_invalid_string_backend_raises(self, arrow_table: Any):
        with pytest.raises(ValueError):
            convert_backend_frame(arrow_table, "invalid_backend")

    @pytest.mark.parametrize(
        ("backend", "blocked_root", "extra"),
        [
            (Backend.PYARROW, "pyarrow", "xbbg[pyarrow]"),
            (Backend.PANDAS, "pandas", "xbbg[pandas]"),
            (Backend.POLARS, "polars", "xbbg[polars]"),
            (Backend.POLARS_LAZY, "polars", "xbbg[polars]"),
            (Backend.DUCKDB, "duckdb", "xbbg[duckdb]"),
            (Backend.NARWHALS_LAZY, "polars", "xbbg[polars]"),
        ],
    )
    def test_missing_explicit_backend_raises_actionable_error(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch, backend: Backend, blocked_root: str, extra: str
    ):
        _block_imports(monkeypatch, blocked_root)

        with pytest.raises(ImportError) as exc_info:
            convert_backend_frame(arrow_table, backend)

        msg = str(exc_info.value)
        assert f"Backend '{backend.value}' requires" in msg
        assert f"pip install {blocked_root}" in msg
        assert extra in msg

    @pytest.mark.parametrize(
        "backend",
        [
            Backend.CUDF,
            Backend.MODIN,
            Backend.DASK,
            Backend.IBIS,
            Backend.PYSPARK,
            Backend.SQLFRAME,
        ],
    )
    def test_selectable_unimplemented_backends_raise_instead_of_falling_through(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch, backend: Backend
    ):
        monkeypatch.setattr("xbbg.backend.check_backend", lambda *_args, **_kwargs: True)

        with pytest.raises(NotImplementedError, match=f"Backend '{backend.value}'.*not implemented"):
            convert_backend_frame(arrow_table, backend)

    def test_set_backend_missing_optional_dependency_errors_before_state_change(self, monkeypatch: pytest.MonkeyPatch):
        from xbbg.blp import get_backend, set_backend

        original = get_backend()
        _block_imports(monkeypatch, "pandas")

        with pytest.raises(ImportError, match="Backend 'pandas' requires"):
            set_backend(Backend.PANDAS)

        assert get_backend() is original
