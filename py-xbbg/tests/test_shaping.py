"""Native result shaping behind generated endpoints: BQR quotes and BDH presentation.

``_core.ashape_bqr`` and ``_core.apresent_historical`` share a native carrier's
batches and run their kernels on the native blocking pool, so the asyncio thread
and other Python threads keep running while a result is shaped.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, timedelta
import threading
from typing import Any

import pytest

from xbbg import _core
from xbbg._core import ArrowTable

_MISSING_BROKER_ATTRIBUTION = (
    "BQR returned quote rows without broker attribution. "
    "Use a fixed-income ticker with a dealer quote pricing source such as '@MSG1 Corp', "
    "or pass include_broker_codes=False if raw quote ticks without dealer codes are intentional."
)
_BQR_RENAMES = {
    "type": "event_type",
    "value": "price",
    "brokerBuyCode": "broker_buy",
    "brokerSellCode": "broker_sell",
    "spreadPrice": "spread_price",
    "conditionCodes": "condition_codes",
    "exchangeCode": "exchange",
}


def _schema(table: Any) -> list[tuple[str, str]]:
    """Column names and Arrow types, in column order."""
    return [(name, table.column(name).data_type) for name in table.column_names]


def _typed_quotes(brokers: list[str | None]) -> Any:
    return ArrowTable.from_pydict(
        {
            "ticker": ["SYN1 Corp"] * 3,
            "time": ["2024-01-15T09:00:02", None, "2024-01-15T09:00:01"],
            "type": ["ASK", "BID", "BID"],
            "value": [101.5, 100.5, 100.25],
            "size": [3, 2, 1],
            "brokerBuyCode": brokers,
            "spreadPrice": [21.0, None, 20.0],
            "exchangeCode": ["SYNX", "SYNY", "SYNZ"],
        }
    )


def _generic_ticks(rows: list[tuple[str | None, str | None, float | None]]) -> Any:
    return ArrowTable.from_pydict(
        {
            "path": [row[0] for row in rows],
            "value_str": [row[1] for row in rows],
            "value_num": [row[2] for row in rows],
        }
    )


# Ticks 0 and 2, already in time order; tick 1 never appears.
_GENERIC_ROWS: list[tuple[str | None, str | None, float | None]] = [
    ("result.tickData[2].time", "2024-01-15T09:00:02", None),
    ("result.tickData[2].value", "", 101.5),
    ("result.tickData[2].conditionCodes", None, 7.0),
    ("result.tickData[2].brokerSellCode", "DLRB", None),
    ("result.tickData[0].time", "2024-01-15T09:00:00", None),
    ("result.tickData[0].type", "BID", None),
    ("result.tickData[0].value", None, 100.25),
    ("result.tickData[0].size", None, 5.0),
    ("result.tickData[0].brokerBuyCode", "DLRA", None),
    ("result.tickData[0].conditionCodes", "R", None),
    ("summary.count", None, 2.0),
    (None, "ignored", 1.0),
]


@pytest.mark.asyncio
async def test_ashape_bqr_sorts_and_renames_typed_quotes() -> None:
    shaped = await _core.ashape_bqr(_typed_quotes(["DLRA", "", None]), "SYN1 Corp", True)

    # Typed output is renamed in place and keeps its column types.
    assert _schema(shaped) == [
        ("ticker", "Utf8"),
        ("time", "Utf8"),
        ("event_type", "Utf8"),
        ("price", "Float64"),
        ("size", "Int64"),
        ("broker_buy", "Utf8"),
        ("spread_price", "Float64"),
        ("exchange", "Utf8"),
    ]
    # Sorted by time, nulls last.
    assert shaped.to_pylist() == [
        {
            "ticker": "SYN1 Corp",
            "time": "2024-01-15T09:00:01",
            "event_type": "BID",
            "price": 100.25,
            "size": 1,
            "broker_buy": None,
            "spread_price": 20.0,
            "exchange": "SYNZ",
        },
        {
            "ticker": "SYN1 Corp",
            "time": "2024-01-15T09:00:02",
            "event_type": "ASK",
            "price": 101.5,
            "size": 3,
            "broker_buy": "DLRA",
            "spread_price": 21.0,
            "exchange": "SYNX",
        },
        {
            "ticker": "SYN1 Corp",
            "time": None,
            "event_type": "BID",
            "price": 100.5,
            "size": 2,
            "broker_buy": "",
            "spread_price": None,
            "exchange": "SYNY",
        },
    ]


@pytest.mark.asyncio
async def test_ashape_bqr_accepts_batches_and_multi_batch_tables() -> None:
    table = _typed_quotes(["DLRA", "", None])
    expected = (await _core.ashape_bqr(table, "SYN1 Corp", True)).to_pylist()
    split = ArrowTable.concat_tables([table.slice(0, 1), table.slice(1)])
    assert split.chunk_lengths == [1, 2]

    for carrier in (table.to_batches()[0], split):
        shaped = await _core.ashape_bqr(carrier, "SYN1 Corp", True)
        assert shaped.chunk_lengths == [3]
        assert shaped.to_pylist() == expected


@pytest.mark.asyncio
async def test_ashape_bqr_pivots_generic_ticks_into_quote_rows() -> None:
    shaped = await _core.ashape_bqr(_generic_ticks(_GENERIC_ROWS), "SYN1 Corp", True)

    # Numeric-only fields are Float64; fields with any text stay strings.
    assert _schema(shaped) == [
        ("ticker", "Utf8"),
        ("time", "Utf8"),
        ("event_type", "Utf8"),
        ("price", "Float64"),
        ("size", "Float64"),
        ("broker_buy", "Utf8"),
        ("broker_sell", "Utf8"),
        ("condition_codes", "Utf8"),
    ]
    assert shaped.to_pylist() == [
        {
            "ticker": "SYN1 Corp",
            "time": "2024-01-15T09:00:00",
            "event_type": "BID",
            "price": 100.25,
            "size": 5.0,
            "broker_buy": "DLRA",
            "broker_sell": None,
            "condition_codes": "R",
        },
        {
            "ticker": "SYN1 Corp",
            "time": "2024-01-15T09:00:02",
            "event_type": None,
            # An empty value_str falls back to value_num.
            "price": 101.5,
            "size": None,
            "broker_buy": None,
            "broker_sell": "DLRB",
            # Numbers in a text field are formatted as text.
            "condition_codes": "7",
        },
    ]


@pytest.mark.asyncio
async def test_ashape_bqr_without_tick_rows_returns_the_empty_quote_table() -> None:
    no_ticks = _generic_ticks([("summary.count", None, 2.0), (None, "ignored", None)])

    shaped = await _core.ashape_bqr(no_ticks, "SYN1 Corp", True)

    assert _schema(shaped) == [(name, "Utf8") for name in ("ticker", "time", "event_type", "price", "size")]
    assert shaped.num_rows == 0


@pytest.mark.asyncio
async def test_ashape_bqr_requires_broker_attribution_only_when_enforced() -> None:
    unattributed = _typed_quotes(["", None, ""])
    generic = _generic_ticks([("tickData[0].type", "BID", None), ("tickData[0].brokerBuyCode", "", None)])

    for raw, quote_rows in ((unattributed, 3), (generic, 1)):
        with pytest.raises(RuntimeError) as raised:
            await _core.ashape_bqr(raw, "SYN1 Corp", True)
        assert str(raised.value) == _MISSING_BROKER_ATTRIBUTION
        assert (await _core.ashape_bqr(raw, "SYN1 Corp", False)).num_rows == quote_rows

    # Empty results never need attribution, including column-less mock responses.
    for empty in (unattributed.slice(0, 0), ArrowTable.empty([])):
        shaped = await _core.ashape_bqr(empty, "SYN1 Corp", True)
        assert shaped.num_rows == 0
        assert shaped.num_columns == empty.num_columns


def test_shaping_kernels_take_only_native_carriers() -> None:
    message = "Expected xbbg ArrowTable or ArrowRecordBatch, got dict"
    with pytest.raises(TypeError, match=message):
        _core.ashape_bqr({"path": []}, "SYN1 Corp", True)
    with pytest.raises(TypeError, match=message):
        _core.apresent_historical({"date": []}, None, None, "ASCENDING", None)


@pytest.mark.asyncio
async def test_reshape_bqr_generic_is_the_shaper_reshape() -> None:
    ticks = _generic_ticks(_GENERIC_ROWS)

    reshaped = ticks.reshape_bqr_generic("SYN1 Corp")
    shaped = await _core.ashape_bqr(ticks, "SYN1 Corp", True)

    # The ticks are already in time order, so shaping only renames.
    renames = {name: _BQR_RENAMES[name] for name in reshaped.column_names if name in _BQR_RENAMES}
    renamed = reshaped.rename_columns(renames)
    assert _schema(renamed) == _schema(shaped)
    assert renamed.to_pylist() == shaped.to_pylist()


def _history() -> Any:
    rows = [
        ("SYN2 Equity", date(2024, 2, 15), "PX_LAST", 1.0),
        ("SYN1 Equity", date(2024, 7, 1), "VOLUME", 2.0),
        ("SYN1 Equity", date(2024, 7, 1), "PX_LAST", 3.0),
        ("SYN1 Equity", date(2023, 12, 31), "PX_LAST", 4.0),
        ("SYN2 Equity", date(2024, 2, 15), "VOLUME", 5.0),
    ]
    names = ["ticker", "date", "field", "value"]

    def chunk(part: list[tuple[Any, ...]]) -> Any:
        return ArrowTable.from_pydict({name: [row[idx] for row in part] for idx, name in enumerate(names)})

    return ArrowTable.concat_tables([chunk(rows[:3]), chunk(rows[3:])])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("show_date", "date_format", "sort", "periodicity"),
    [
        (None, None, "ASCENDING", None),
        (None, None, "DESCENDING", None),
        (None, "PERIODIC", None, "MONTHLY"),
        (None, "PERIODIC", "DESCENDING", "Q"),
        (None, "BOTH", "ASCENDING", "WEEKLY"),
        (None, "BOTH", None, None),
        (None, "DATE", "ASCENDING", "YEARLY"),
        (False, None, None, None),
        (False, "BOTH", "DESCENDING", "MONTHLY"),
        (True, "PERIODIC", None, "SEMI_ANNUALLY"),
    ],
)
async def test_apresent_historical_matches_the_synchronous_presentation(
    show_date: bool | None, date_format: str | None, sort: str | None, periodicity: str | None
) -> None:
    table = _history()
    expected = table.apply_historical_presentation(show_date, date_format, sort, periodicity)

    presented = await _core.apresent_historical(table, show_date, date_format, sort, periodicity)
    assert _schema(presented) == _schema(expected)
    assert presented.chunk_lengths == expected.chunk_lengths
    assert presented.to_pylist() == expected.to_pylist()

    from_batch = await _core.apresent_historical(table.to_record_batch(), show_date, date_format, sort, periodicity)
    assert _schema(from_batch) == _schema(expected)
    assert from_batch.to_pylist() == expected.to_pylist()


_LARGE_ROWS = 50_000
_TICK_FIELDS = ("time", "type", "value", "brokerBuyCode")


@pytest.fixture(scope="module")
def large_carriers() -> dict[str, Any]:
    ticks = _generic_ticks(
        [
            (
                f"result.tickData[{row // 4}].{_TICK_FIELDS[row % 4]}",
                None if row % 4 == 2 else f"S{row:08d}",
                float(row) if row % 4 == 2 else None,
            )
            for row in range(_LARGE_ROWS)
        ]
    )
    start = date(2020, 1, 1)
    history = ArrowTable.from_pydict(
        {
            "ticker": [f"SYN{row % 97:03d} Equity" for row in range(_LARGE_ROWS)],
            "date": [start + timedelta(days=row % 1_500) for row in range(_LARGE_ROWS)],
            "field": ["PX_LAST" if row % 2 else "VOLUME" for row in range(_LARGE_ROWS)],
            "value": [float((row * 7_919) % 10_007) for row in range(_LARGE_ROWS)],
        }
    )
    return {
        "ticks": ArrowTable.concat_tables([ticks] * 10),
        "history": ArrowTable.concat_tables([history] * 4),
    }


_LARGE_KERNELS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "ashape_bqr": lambda carriers: _core.ashape_bqr(carriers["ticks"], "SYN1 Corp", True),
    "apresent_historical": lambda carriers: _core.apresent_historical(
        carriers["history"], True, "BOTH", "DESCENDING", "WEEKLY"
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(_LARGE_KERNELS))
async def test_shaping_keeps_the_event_loop_running(large_carriers: dict[str, Any], name: str) -> None:
    loop = asyncio.get_running_loop()
    shaping = _LARGE_KERNELS[name](large_carriers)
    observed: list[bool] = []
    loop.call_soon(lambda: observed.append(shaping.done()))

    shaped = await shaping

    # The callback ran on the loop while the kernel was still working.
    assert observed == [False]
    assert shaped.num_rows > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(_LARGE_KERNELS))
async def test_python_threads_run_while_shaping_is_pending(large_carriers: dict[str, Any], name: str) -> None:
    loop = asyncio.get_running_loop()
    shaping = _LARGE_KERNELS[name](large_carriers)
    worker_ran: asyncio.Future[None] = loop.create_future()

    def worker() -> None:
        loop.call_soon_threadsafe(worker_ran.set_result, None)

    thread = threading.Thread(target=worker, name="shaping-progress-probe")
    thread.start()
    done, _ = await asyncio.wait({shaping, worker_ran}, return_when=asyncio.FIRST_COMPLETED)
    thread.join()

    # Another Python thread ran and reached the loop before the kernel finished.
    assert worker_ran in done
    assert not shaping.done()
    assert (await shaping).num_rows > 0
