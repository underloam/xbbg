"""Focused tests for xbbg native Arrow carrier objects."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from xbbg._core import ArrowColumn, ArrowRecordBatch, ArrowTable


@pytest.fixture
def arrow_table() -> Any:
    return ArrowTable.from_pylist(
        [
            {"ticker": "AAPL US Equity", "date": date(2024, 1, 1), "px_last": 150.0, "volume": 10},
            {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20},
            {"ticker": "IBM US Equity", "date": date(2024, 1, 3), "px_last": None, "volume": 30},
        ]
    )


def test_from_pylist_preserves_first_seen_columns_and_backfills_late_nulls() -> None:
    table = ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "PX_LAST": 123.0},
            {"ticker": "MSFT US Equity", "VOLUME": 10},
            {"ticker": "AAPL US Equity", "NAME": "Apple", "PX_LAST": 150.0},
        ]
    )

    assert table.column_names == ["ticker", "PX_LAST", "VOLUME", "NAME"]
    assert table.to_pylist() == [
        {"ticker": "IBM US Equity", "PX_LAST": 123.0, "VOLUME": None, "NAME": None},
        {"ticker": "MSFT US Equity", "PX_LAST": None, "VOLUME": 10, "NAME": None},
        {"ticker": "AAPL US Equity", "PX_LAST": 150.0, "VOLUME": None, "NAME": "Apple"},
    ]


def test_empty_rows_and_datetimes_preserve_native_arrow_contracts() -> None:
    empty = ArrowTable.from_pylist([])
    assert empty.shape == (0, 0)
    assert empty.to_pylist() == []

    named_empty = ArrowTable.empty([])
    assert named_empty.shape == (0, 0)

    empty_columns = ArrowTable.from_pylist([{}, {}])
    assert empty_columns.shape == (2, 0)
    assert empty_columns.to_pylist() == [{}, {}]

    renamed_empty_columns = empty_columns.rename_columns({})
    assert renamed_empty_columns.shape == (2, 0)
    assert renamed_empty_columns.to_pylist() == [{}, {}]
    with pytest.raises(OverflowError):
        ArrowTable.from_pylist([{"value": 2**63}])

    exact_mixed = ArrowTable.from_pylist([{"value": 2**53}, {"value": 0.5}])
    assert exact_mixed.to_pylist() == [{"value": float(2**53)}, {"value": 0.5}]

    for inexact in (2**53 + 1, 2**63 - 1):
        lossless_mixed = ArrowTable.from_pylist([{"value": inexact}, {"value": 0.5}])
        assert lossless_mixed.to_pylist() == [
            {"value": str(inexact)},
            {"value": "0.5"},
        ]

    when = datetime(2024, 1, 2, 3, 4, 5, 123456)
    table = ArrowTable.from_pylist([{"when": when}, {"when": None}])
    assert table.to_pylist()[0]["when"] == when.replace(tzinfo=timezone.utc)
    assert table.filter_eq("when", when).num_rows == 1


def test_table_carrier_properties_and_columns_are_arrow_backed(arrow_table: Any) -> None:
    assert len(arrow_table) == 3
    assert arrow_table.shape == (3, 4)
    assert arrow_table.num_rows == 3
    assert arrow_table.num_columns == 4
    assert arrow_table.chunk_lengths == [3]
    assert arrow_table.nbytes > 0

    assert arrow_table.column_names == ["ticker", "date", "px_last", "volume"]
    assert [column.name for column in arrow_table.columns] == arrow_table.column_names

    ticker = arrow_table.column("ticker")
    assert isinstance(ticker, ArrowColumn)
    assert ticker == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert arrow_table.get_column(0).to_pylist() == ticker.to_pylist()
    assert arrow_table["ticker"].to_pylist() == ticker.to_pylist()
    assert arrow_table[-1].name == "volume"


def test_column_api_materializes_lazily_and_supports_slices(arrow_table: Any) -> None:
    prices = arrow_table.column("px_last")

    assert prices.name == "px_last"
    assert prices.field.name == "px_last"
    assert "Float64" in prices.data_type
    assert prices.null_count == 1
    assert prices.nbytes > 0
    assert len(prices) == 3
    assert prices[0] == 150.0
    assert prices[-1] is None
    assert list(prices) == [150.0, 380.0, None]
    assert prices.slice(1, 1).to_pylist() == [380.0]
    assert prices.slice(99).to_pylist() == []


def test_table_projection_drop_rename_slice_head_tail(arrow_table: Any) -> None:
    selected = arrow_table.select(["ticker", "px_last"])
    assert selected.column_names == ["ticker", "px_last"]
    assert selected.to_pylist() == [
        {"ticker": "AAPL US Equity", "px_last": 150.0},
        {"ticker": "MSFT US Equity", "px_last": 380.0},
        {"ticker": "IBM US Equity", "px_last": None},
    ]

    assert arrow_table[["ticker", "volume"]].column_names == ["ticker", "volume"]
    assert arrow_table[(0, 2)].column_names == ["ticker", "px_last"]
    assert arrow_table.drop_columns(["volume"]).column_names == ["ticker", "date", "px_last"]
    assert arrow_table.rename_columns({"px_last": "last"}).column_names == [
        "ticker",
        "date",
        "last",
        "volume",
    ]
    assert arrow_table.rename({"px_last": "last"}).column_names == [
        "ticker",
        "date",
        "last",
        "volume",
    ]
    assert arrow_table.slice(1, 1).to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]
    assert arrow_table.head(1).column("ticker") == ["AAPL US Equity"]
    assert arrow_table.tail(1).column("ticker") == ["IBM US Equity"]


def test_record_batch_api_matches_table_carrier_surface(arrow_table: Any) -> None:
    batch = arrow_table.to_batches()[0]

    assert isinstance(batch, ArrowRecordBatch)
    assert len(batch) == 3
    assert batch.shape == (3, 4)
    assert batch.nbytes > 0
    assert [column.name for column in batch.columns] == arrow_table.column_names
    assert batch.column("ticker") == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert batch.get_column(0).to_pylist() == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert batch["px_last"].to_pylist() == [150.0, 380.0, None]
    assert batch.select(["ticker", "volume"]).column_names == ["ticker", "volume"]
    assert batch.slice(1, 1).to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]
    assert batch.to_table().to_pylist() == arrow_table.to_pylist()


def test_sort_filter_and_column_mutation_still_delegate_to_native_arrow(arrow_table: Any) -> None:
    sorted_table = arrow_table.sort_by([("volume", "descending")])
    assert sorted_table.column("ticker") == ["IBM US Equity", "MSFT US Equity", "AAPL US Equity"]

    nullable = ArrowTable.from_pylist(
        [
            {"key": 2, "label": "two"},
            {"key": None, "label": "missing"},
            {"key": 1, "label": "one"},
        ]
    )
    assert nullable.sort_by([("key", "ascending")]).column("label") == [
        "one",
        "two",
        "missing",
    ]
    assert nullable.sort_by([("key", "ascending")], nulls_last=False).column("label") == ["missing", "one", "two"]

    filtered = arrow_table.filter_eq("ticker", "MSFT US Equity")
    assert filtered.to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]
    assert arrow_table.filter_eq("px_last", 150).column("ticker") == ["AAPL US Equity"]
    assert arrow_table.filter_eq("volume", 20.0).column("ticker") == ["MSFT US Equity"]

    large_float = ArrowTable.from_pylist([{"value": float(2**53)}])
    assert large_float.filter_eq("value", 2**53).num_rows == 1
    assert large_float.filter_eq("value", 2**53 + 1).num_rows == 0

    added = arrow_table.add_column(1, "side", ["A", "B", "C"])
    assert added.column_names == ["ticker", "side", "date", "px_last", "volume"]
    assert added.column("side") == ["A", "B", "C"]

    replaced = added.set_column(1, "side2", ["ask", "bid", "mid"])
    assert replaced.column_names == ["ticker", "side2", "date", "px_last", "volume"]
    assert replaced.column("side2") == ["ask", "bid", "mid"]


def test_compact_copies_small_slices_without_retaining_parent_buffers() -> None:
    rows = [
        {
            "payload": None if index == 1025 else f"{index:04d}-" + ("x" * 512),
            "ordinal": index,
        }
        for index in range(2048)
    ]
    table = ArrowTable.from_pylist(rows)
    expected = rows[1024:1026]

    table_slice = table.slice(1024, 2)
    compact_table = table_slice.compact()
    assert compact_table.to_pylist() == expected
    assert compact_table.schema.names == table_slice.schema.names
    assert [field.data_type for field in compact_table.schema.fields] == [
        field.data_type for field in table_slice.schema.fields
    ]
    assert compact_table.metadata == table_slice.metadata
    assert compact_table.nbytes * 100 < table_slice.nbytes

    batch_slice = table.to_batches()[0].slice(1024, 2)
    compact_batch = batch_slice.compact()
    assert compact_batch.to_pylist() == expected
    assert compact_batch.schema.names == batch_slice.schema.names
    assert [field.data_type for field in compact_batch.schema.fields] == [
        field.data_type for field in batch_slice.schema.fields
    ]
    assert compact_batch.nbytes * 100 < batch_slice.nbytes

    column_slice = table.column("payload").slice(1024, 2)
    compact_column = column_slice.compact()
    assert compact_column.to_pylist() == [expected[0]["payload"], None]
    assert compact_column.data_type == column_slice.data_type
    assert compact_column.nbytes * 100 < column_slice.nbytes


def test_pyarrow_helpers_are_lazy_optional_conversions(arrow_table: Any) -> None:
    pa = pytest.importorskip("pyarrow")

    pyarrow_table = arrow_table.to_pyarrow()
    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.column_names == arrow_table.column_names

    pyarrow_batch = arrow_table.to_batches()[0].to_pyarrow()
    assert isinstance(pyarrow_batch, pa.RecordBatch)

    pyarrow_column = arrow_table.column("ticker").to_pyarrow()
    assert isinstance(pyarrow_column, pa.ChunkedArray)
    assert pyarrow_column.to_pylist() == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]


@pytest.mark.parametrize("native_only", [False, True])
def test_shared_arrow_coercion_preserves_native_carriers(arrow_table, native_only):
    from xbbg._arrow import ensure_arrow_table, is_arrow_record_batch, is_arrow_table

    assert is_arrow_table(arrow_table)
    assert ensure_arrow_table(arrow_table, native_only=native_only) is arrow_table
    batch = arrow_table.to_batches()[0]
    assert is_arrow_record_batch(batch)
    assert ensure_arrow_table(batch, native_only=native_only).to_pylist() == arrow_table.to_pylist()
    with pytest.raises(TypeError, match="Expected xbbg ArrowTable"):
        ensure_arrow_table(object(), native_only=native_only)


@pytest.mark.parametrize("batch", [False, True])
def test_pyarrow_coercion_does_not_widen_native_plugin_contract(batch):
    from narwhals._utils import Version

    from xbbg._arrow import ensure_arrow_table
    from xbbg._narwhals_impl import XbbgDataFrame

    pa = pytest.importorskip("pyarrow")
    table = pa.table({"value": [1, 2]})
    carrier = table.to_batches()[0] if batch else table
    assert ensure_arrow_table(carrier).to_pylist() == [{"value": 1}, {"value": 2}]
    with pytest.raises(TypeError, match="Expected xbbg ArrowTable"):
        ensure_arrow_table(carrier, native_only=True)
    with pytest.raises(TypeError, match="Expected xbbg ArrowTable"):
        XbbgDataFrame(carrier, version=Version.V1)
