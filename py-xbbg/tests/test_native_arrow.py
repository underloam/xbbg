"""Focused tests for xbbg native Arrow carrier objects."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
import json
import sys
from typing import Any

from _gil_probe import releases_gil
import pytest

from xbbg._core import ArrowColumn, ArrowRecordBatch, ArrowTable, _decode_field_exceptions

FREE_THREADED = getattr(sys, "_is_gil_enabled", lambda: True)() is False


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
    assert table.to_pylist()[0]["when"] == when.replace(tzinfo=UTC)
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


def test_from_pylist_pads_ragged_rows_in_first_seen_column_order() -> None:
    table = ArrowTable.from_pylist([{"a": 1, "b": "x"}, {"b": "y"}, {"c": 2.5, "a": 3}])

    assert table.column_names == ["a", "b", "c"]
    assert table.column("a") == [1, None, 3]
    assert table.column("b") == ["x", "y", None]
    assert table.column("c") == [None, None, 2.5]
    with pytest.raises(TypeError, match="dict rows"):
        ArrowTable.from_pylist([{"a": 1}, ["not", "a", "dict"]])


def test_from_pydict_types_cells_like_from_pylist_in_mapping_order() -> None:
    when = datetime(2024, 1, 2, 3, 4, 5, 123456)
    columns = {
        "z_text": ["SYN1 Equity", None, "SYN2 Equity"],
        "a_mixed": [1, 0.5, None],
        "inexact": [2**53 + 1, 0.5, None],
        "when": [when, None, when],
        "day": [date(2024, 1, 2), None, date(2024, 1, 3)],
        "flag": [True, False, None],
        "empty": [None, None, None],
    }
    rows = [{name: values[row] for name, values in columns.items()} for row in range(3)]

    from_columns = ArrowTable.from_pydict(columns)
    from_rows = ArrowTable.from_pylist(rows)

    assert from_columns.column_names == list(columns)
    assert [field.data_type for field in from_columns.schema.fields] == [
        field.data_type for field in from_rows.schema.fields
    ]
    assert from_columns.to_pylist() == from_rows.to_pylist()
    assert from_columns.column("inexact") == [str(2**53 + 1), "0.5", None]
    assert from_columns.column("when")[0] == when.replace(tzinfo=UTC)


def test_from_pydict_rejects_unequal_lengths_and_non_columns() -> None:
    assert ArrowTable.from_pydict({}).shape == (0, 0)
    assert ArrowTable.from_pydict({"value": []}).shape == (0, 1)

    with pytest.raises(ValueError, match=r'column "b" has 1 values; expected 2'):
        ArrowTable.from_pydict({"a": [1, 2], "b": [1]})
    with pytest.raises(TypeError, match="mapping"):
        ArrowTable.from_pydict([("a", [1])])
    with pytest.raises(TypeError, match="sequence"):
        ArrowTable.from_pydict({"a": "text"})
    with pytest.raises(OverflowError):
        ArrowTable.from_pydict({"a": [2**63]})


def _two_batch_keys() -> Any:
    first = ArrowTable.from_pydict({"key": [2, None, 1], "label": ["b0", "n0", "a0"]})
    second = ArrowTable.from_pydict({"key": [1, None, 2], "label": ["a1", "n1", "b1"]})
    return ArrowTable.concat_tables([first, second])


def test_sort_by_breaks_ties_and_places_nulls_across_batches() -> None:
    table = _two_batch_keys()
    assert table.chunk_lengths == [3, 3]

    ascending = [("key", "ascending"), ("label", "ascending")]
    assert table.sort_by(ascending).column("label") == ["a0", "a1", "b0", "b1", "n0", "n1"]
    assert table.sort_by(ascending, nulls_last=False).column("label") == ["n0", "n1", "a0", "a1", "b0", "b1"]
    descending = [("key", "desc"), ("label", "DESC")]
    assert table.sort_by(descending).column("label") == ["b1", "b0", "a1", "a0", "n1", "n0"]
    assert table.sort_by(descending, nulls_last=False).column("label") == ["n1", "n0", "b1", "b0", "a1", "a0"]


def test_native_kernels_keep_their_python_error_types() -> None:
    table = _two_batch_keys()

    with pytest.raises(KeyError, match="missing"):
        table.sort_by([("missing", "ascending")])
    with pytest.raises(ValueError, match="unsupported sort direction for key: sideways"):
        table.sort_by([("key", "sideways")])
    with pytest.raises(KeyError, match="missing"):
        table.filter_eq("missing", 1)
    with pytest.raises(TypeError, match="cannot compare"):
        table.filter_eq("label", 1)
    with pytest.raises(OverflowError):
        table.filter_eq("key", 2**63)
    with pytest.raises(ValueError, match="does not match table row count"):
        table.add_column(0, "short", [1])
    with pytest.raises(IndexError):
        table.add_column(9, "late", [1] * 6)
    with pytest.raises(IndexError):
        table.set_column(2, "past_end", [1] * 6)

    assert table.filter_eq("key", 1).column("label") == ["a0", "a1"]
    assert table.filter_eq("key", None).column("label") == ["n0", "n1"]
    assert table.set_column(1, "label", list(range(6))).column("label") == list(range(6))
    assert table.add_column(2, "side", ["x"] * 6).chunk_lengths == [3, 3]


def test_has_any_value_treats_null_and_empty_text_as_absent() -> None:
    table = ArrowTable.from_pydict({"text": ["", None], "number": [None, 0], "nothing": [None, None]})

    assert not table.has_any_value(["text"])
    assert table.has_any_value(["number"])
    assert not table.has_any_value(["nothing"])
    assert not table.has_any_value(["absent"])
    assert not table.has_any_value([])
    assert table.has_any_value(["absent", "text", "number"])

    later = ArrowTable.from_pydict({"text": [None, "x"], "number": [None, 7], "nothing": [None, None]})
    combined = ArrowTable.concat_tables([table, later])
    assert combined.chunk_lengths == [2, 2]
    assert combined.has_any_value(["text"])
    assert not combined.has_any_value(["nothing"])


_HISTORY_ROWS = [
    ("SYN2 Equity", date(2024, 2, 15), "PX_LAST", 1.0),
    ("SYN1 Equity", date(2024, 7, 1), "VOLUME", 2.0),
    ("SYN1 Equity", date(2024, 7, 1), "PX_LAST", 3.0),
    ("SYN1 Equity", date(2023, 12, 31), "PX_LAST", 4.0),
    ("SYN2 Equity", date(2024, 2, 15), "VOLUME", 5.0),
]
_HISTORY_COLUMNS = ["ticker", "date", "field", "value"]


def _history_table() -> Any:
    def chunk(rows: list[tuple[Any, ...]]) -> Any:
        return ArrowTable.from_pydict({name: [row[idx] for row in rows] for idx, name in enumerate(_HISTORY_COLUMNS)})

    return ArrowTable.concat_tables([chunk(_HISTORY_ROWS[:3]), chunk(_HISTORY_ROWS[3:])])


@pytest.mark.parametrize(
    ("sort", "values"),
    [
        ("ASCENDING", [4.0, 3.0, 2.0, 1.0, 5.0]),
        ("anything else", [4.0, 3.0, 2.0, 1.0, 5.0]),
        ("descending", [3.0, 2.0, 4.0, 1.0, 5.0]),
    ],
)
def test_historical_presentation_sorts_by_ticker_date_and_field(sort: str, values: list[float]) -> None:
    table = _history_table()
    assert table.chunk_lengths == [3, 2]

    shaped = table.apply_historical_presentation(None, None, sort, None)

    assert shaped.column_names == _HISTORY_COLUMNS
    assert shaped.column("value") == values


_YEARLY = ["2024", "2024", "2024", "2023", "2024"]
_SEMI_ANNUAL = ["2024H1", "2024H2", "2024H2", "2023H2", "2024H1"]
_QUARTERLY = ["2024Q1", "2024Q3", "2024Q3", "2023Q4", "2024Q1"]
_MONTHLY = ["2024-02", "2024-07", "2024-07", "2023-12", "2024-02"]
_WEEKLY = ["2024-W07", "2024-W27", "2024-W27", "2023-W52", "2024-W07"]
_DAILY = ["2024-02-15", "2024-07-01", "2024-07-01", "2023-12-31", "2024-02-15"]


@pytest.mark.parametrize(
    ("periodicity", "labels"),
    [
        ("YEARLY", _YEARLY),
        ("Y", _YEARLY),
        ("SEMI_ANNUALLY", _SEMI_ANNUAL),
        ("semiannually", _SEMI_ANNUAL),
        ("S", _SEMI_ANNUAL),
        ("QUARTERLY", _QUARTERLY),
        ("Q", _QUARTERLY),
        ("monthly", _MONTHLY),
        ("M", _MONTHLY),
        ("WEEKLY", _WEEKLY),
        ("W", _WEEKLY),
        ("DAILY", _DAILY),
        ("unrecognized", _DAILY),
        (None, _DAILY),
    ],
)
def test_historical_presentation_labels_periods_for_each_periodicity(
    periodicity: str | None, labels: list[str]
) -> None:
    table = _history_table()

    periodic = table.apply_historical_presentation(None, "periodic", None, periodicity)
    assert periodic.column_names == ["ticker", "period", "field", "value"]
    assert periodic.column("period") == labels
    assert periodic.column("period").data_type == "Utf8"
    assert periodic.chunk_lengths == [3, 2]

    both = table.apply_historical_presentation(None, "BOTH", None, periodicity)
    assert both.column_names == ["ticker", "date", "period", "field", "value"]
    assert both.column("date") == [row[1] for row in _HISTORY_ROWS]
    assert both.column("period") == labels


def test_historical_presentation_hides_dates_and_periods() -> None:
    table = _history_table()

    assert table.apply_historical_presentation(False, None, None, None).column_names == ["ticker", "field", "value"]
    hidden = table.apply_historical_presentation(False, "BOTH", "DESCENDING", "MONTHLY")
    assert hidden.column_names == ["ticker", "field", "value"]
    assert hidden.column("value") == [3.0, 2.0, 4.0, 1.0, 5.0]
    assert table.apply_historical_presentation(True, None, None, None).to_pylist() == table.to_pylist()

    undated = table.drop_columns(["date"])
    unchanged = undated.apply_historical_presentation(None, "PERIODIC", "DESCENDING", "MONTHLY")
    assert unchanged.to_pylist() == undated.to_pylist()


def _generic_bqr_table(paths: list[str | None], value_str: list[str | None], value_num: list[float | None]) -> Any:
    return ArrowTable.from_pydict({"path": paths, "value_str": value_str, "value_num": value_num})


def test_reshape_bqr_generic_pivots_ticks_across_batches_with_gaps() -> None:
    first = _generic_bqr_table(
        ["result.tickData[1].time", "result.tickData[1].value", "result.tickData[0].type"],
        ["2024-01-02T10:00:00", "", "TRADE"],
        [None, 101.5, None],
    )
    second = _generic_bqr_table(
        ["result.tickData[0].value", "result.tickData[3].brokerBuy", "summary.count", None],
        [None, "SYNB", "ignored", "ignored"],
        [99.0, None, 5.0, 6.0],
    )

    reshaped = ArrowTable.concat_tables([first, second]).reshape_bqr_generic("SYN1 Corp")

    assert reshaped.column_names == ["ticker", "time", "type", "value", "brokerBuy"]
    assert reshaped.to_pylist() == [
        {"ticker": "SYN1 Corp", "time": None, "type": "TRADE", "value": 99.0, "brokerBuy": None},
        {"ticker": "SYN1 Corp", "time": "2024-01-02T10:00:00", "type": None, "value": 101.5, "brokerBuy": None},
        {"ticker": "SYN1 Corp", "time": None, "type": None, "value": None, "brokerBuy": "SYNB"},
    ]


def test_reshape_bqr_generic_without_tick_records_is_the_empty_quote_table() -> None:
    empty_columns = ["ticker", "time", "type", "value", "size"]
    no_ticks = _generic_bqr_table(["summary.count", None], ["x", None], [1.0, None])
    no_paths = ArrowTable.from_pydict({"value_str": ["x"]})

    for table in (no_ticks, no_paths, no_ticks.slice(0, 0)):
        reshaped = table.reshape_bqr_generic("SYN1 Corp")
        assert reshaped.column_names == empty_columns
        assert reshaped.num_rows == 0


def test_reshape_bqr_generic_types_numeric_fields_as_float64_and_others_as_text() -> None:
    ticks = _generic_bqr_table(
        ["tickData[0].size", "tickData[1].size", "tickData[0].type", "tickData[1].type"],
        [None, None, "BID", None],
        [5, 7, None, 1],
    )

    reshaped = ticks.reshape_bqr_generic("SYN1 Corp")

    assert [(name, reshaped.column(name).data_type) for name in reshaped.column_names] == [
        ("ticker", "Utf8"),
        ("type", "Utf8"),
        ("size", "Float64"),
    ]
    assert reshaped.column("size") == [5.0, 7.0]
    assert reshaped.column("type") == ["BID", "1"]


def test_logical_nbytes_counts_only_the_sliced_cell() -> None:
    table = ArrowTable.from_pydict({"payload": [f"{row:04d}" * 25 for row in range(1_000)]})
    column = table.column("payload")
    cell = column.slice(500, 1)

    assert cell.to_pylist() == ["0500" * 25]
    assert cell.logical_nbytes == 2 * 4 + 100
    assert cell.nbytes >= 100_000
    assert cell.compact().nbytes >= cell.logical_nbytes
    assert column.logical_nbytes == 1_001 * 4 + 100_000
    assert column.slice(5_000).logical_nbytes == 0


def test_metadata_accessors_match_metadata_on_tables_and_batches(arrow_table: Any) -> None:
    for carrier in (arrow_table, arrow_table.to_batches()[0]):
        assert carrier.metadata == {}
        assert carrier.metadata_sizes() == {}
        assert carrier.metadata_value("absent") is None
        assert carrier.metadata_value("absent", 4) is None


_EXCEPTION_NA = {
    "field": "OPEN_INT",
    "category": "BAD_FLD",
    "code": 9,
    "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
    "message": "not applicable",
}
_EXCEPTION_BAD = {
    "field": "PX_BAD",
    "category": "BAD_FLD",
    "code": 2,
    "subcategory": "INVALID",
    "message": "bad field",
}


def test_decode_field_exceptions_expands_versioned_and_legacy_payloads() -> None:
    expanded = {"SYN1 Equity": [_EXCEPTION_NA, _EXCEPTION_BAD, _EXCEPTION_NA], "SYN2 Equity": [_EXCEPTION_BAD]}
    versioned = {
        "version": 1,
        "entries": [_EXCEPTION_NA, _EXCEPTION_BAD],
        "securities": {"SYN1 Equity": [0, 1, 0], "SYN2 Equity": [1]},
    }

    assert _decode_field_exceptions(json.dumps(versioned)) == expanded
    assert _decode_field_exceptions(json.dumps(expanded)) == expanded

    many = {
        "version": 1,
        "entries": [_EXCEPTION_BAD],
        "securities": {f"SYN{idx:05d} Equity": [0] for idx in range(5_000)},
    }
    raw = json.dumps(many)
    assert len(raw) > 64 * 1024
    decoded = _decode_field_exceptions(raw)
    assert len(decoded) == 5_000
    assert decoded["SYN04999 Equity"] == [_EXCEPTION_BAD]


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        json.dumps({"version": 1, "entries": [], "securities": {"SYN1 Equity": [0]}}),
        json.dumps({"version": 1, "entries": [_EXCEPTION_BAD], "securities": {"SYN1 Equity": [-1]}}),
        json.dumps({"version": 1, "entries": [_EXCEPTION_BAD], "securities": {"SYN1 Equity": [0]}})[:-1]
        + " " * (64 * 1024),
    ],
    ids=["not-json", "dangling-index", "negative-index", "truncated-above-threshold"],
)
def test_decode_field_exceptions_rejects_malformed_payloads(raw: str) -> None:
    with pytest.raises(ValueError, match=r"invalid JSON in xbbg\.field_exceptions"):
        _decode_field_exceptions(raw)


def test_to_polars_matches_backend_conversion(arrow_table: Any) -> None:
    pl = pytest.importorskip("polars")
    from xbbg.backend import _to_polars_frame

    combined = ArrowTable.concat_tables([arrow_table, arrow_table])
    frame = combined.to_polars()

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == combined.column_names
    assert frame.to_dicts() == combined.to_pylist()
    assert frame.equals(_to_polars_frame(combined))


_GIL_ROWS = 20_000
_TICK_FIELDS = ("time", "type", "value", "size")


@pytest.fixture(scope="module")
def large_carriers() -> dict[str, Any]:
    start = date(2020, 1, 1)
    base = ArrowTable.from_pydict(
        {
            "ticker": [f"SYN{row % 97:03d} Equity" for row in range(_GIL_ROWS)],
            "date": [start + timedelta(days=row % 1_500) for row in range(_GIL_ROWS)],
            "field": ["PX_LAST" if row % 2 else "VOLUME" for row in range(_GIL_ROWS)],
            "value": [float((row * 7_919) % 10_007) for row in range(_GIL_ROWS)],
            "blank": [""] * _GIL_ROWS,
            "payload": [f"{row:08d}" * 8 for row in range(_GIL_ROWS)],
        }
    )
    ticks = _generic_bqr_table(
        [f"result.tickData[{row // 4}].{_TICK_FIELDS[row % 4]}" for row in range(_GIL_ROWS)],
        ["" if row % 4 == 2 else f"S{row}" for row in range(_GIL_ROWS)],
        [float(row) if row % 4 == 2 else None for row in range(_GIL_ROWS)],
    )
    table = ArrowTable.concat_tables([base] * 50)
    return {
        "table": table,
        "history": ArrowTable.concat_tables([base] * 10),
        "batch": table.to_record_batch(),
        "column": table.column("payload"),
        "ticks": ArrowTable.concat_tables([ticks] * 25),
    }


_GIL_RELEASING_CALLS: dict[str, Callable[[dict[str, Any]], object]] = {
    "sort_by": lambda carriers: carriers["history"].sort_by([("value", "descending"), ("ticker", "ascending")]),
    "filter_eq": lambda carriers: carriers["table"].filter_eq("payload", "absent"),
    "table_compact": lambda carriers: carriers["table"].compact(),
    "batch_compact": lambda carriers: carriers["batch"].compact(),
    "column_compact": lambda carriers: carriers["column"].compact(),
    "to_record_batch": lambda carriers: carriers["table"].to_record_batch(),
    "has_any_value": lambda carriers: carriers["table"].has_any_value(["blank"] * 25),
    "apply_historical_presentation": lambda carriers: carriers["history"].apply_historical_presentation(
        True, "BOTH", "DESCENDING", "WEEKLY"
    ),
    "reshape_bqr_generic": lambda carriers: carriers["ticks"].reshape_bqr_generic("SYN1 Corp"),
}


@pytest.mark.skipif(FREE_THREADED, reason="GIL release is only observable on GIL-enabled builds")
@pytest.mark.parametrize("name", sorted(_GIL_RELEASING_CALLS))
def test_data_sized_native_kernels_release_the_gil(large_carriers: dict[str, Any], name: str) -> None:
    call = _GIL_RELEASING_CALLS[name]

    assert releases_gil(lambda: call(large_carriers))
