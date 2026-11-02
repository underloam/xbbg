"""Offline native request completion and recipe Arrow ingestion.

The private drivers exist only in native extensions built with `test-support`.
Set XBBG_REQUIRE_TEST_SUPPORT=1 in its dedicated run to fail rather than skip
when the wrong extension is installed.
"""

from __future__ import annotations

import asyncio
import os

import pytest

import xbbg
from xbbg import _core


def native_driver(name):
    driver = getattr(_core, name, None)
    if driver is None:
        if os.environ.get("XBBG_REQUIRE_TEST_SUPPORT") == "1":
            pytest.fail("native extension was not built with pyo3-xbbg/test-support")
        pytest.skip("requires a native extension built with pyo3-xbbg/test-support")
    return driver


@pytest.fixture
def request_driver():
    return native_driver("_test_request_fixture")()


@pytest.fixture
def ingest():
    return native_driver("_test_recipe_input_fixture")


def synthetic_rows(start, count):
    return [{"ticker": f"SYNTHETIC {index} Equity", "value": float(index)} for index in range(start, start + count)]


def synthetic_batches(pa, schema, chunks):
    for chunk in range(chunks):
        yield pa.RecordBatch.from_pylist(synthetic_rows(2 * chunk, 2), schema=schema)


def synthetic_schema(pa):
    return pa.schema([("ticker", pa.string()), ("value", pa.float64())])


@pytest.mark.asyncio
async def test_gated_request_completions_release_together_behind_an_ungated_request(request_driver):
    gated = [request_driver.request(2, gated=True) for _ in range(64)]
    ungated = await asyncio.wait_for(request_driver.request(3), timeout=3)
    assert isinstance(ungated, xbbg.ArrowRecordBatch)
    assert ungated.num_rows == 3
    assert not any(future.done() for future in gated)
    request_driver.release_requests()
    batches = await asyncio.wait_for(asyncio.gather(*gated), timeout=5)
    assert [batch.num_rows for batch in batches] == [2] * 64
    assert batches[0].to_pylist() == [
        {"ticker": "SYNTHETIC 0 Equity", "row": 0},
        {"ticker": "SYNTHETIC 1 Equity", "row": 1},
    ]


@pytest.mark.asyncio
async def test_request_failures_raise_typed_exceptions_with_request_context(request_driver):
    with pytest.raises(_core.BlpRequestError) as raised:
        await asyncio.wait_for(
            request_driver.request(
                error="request",
                message="synthetic refusal",
                service="//synthetic/refdata",
                operation="SyntheticRequest",
                request_id="synthetic-label",
                bloomberg_request_ids=["synthetic-a"],
            ),
            timeout=3,
        )
    error = raised.value
    assert type(error) is _core.BlpRequestError
    assert str(error) == "synthetic refusal"
    assert (error.service, error.operation, error.request_id) == (
        "//synthetic/refdata",
        "SyntheticRequest",
        "synthetic-label",
    )
    assert error.bloomberg_request_id == "synthetic-a"
    assert error.bloomberg_request_ids == ["synthetic-a"]

    with pytest.raises(_core.BlpLimitError) as raised:
        await asyncio.wait_for(
            request_driver.request(error="limit", bloomberg_request_ids=["synthetic-a", "synthetic-z"]), timeout=3
        )
    assert raised.value.service is None
    assert raised.value.bloomberg_request_id is None
    assert raised.value.bloomberg_request_ids == ["synthetic-a", "synthetic-z"]

    with pytest.raises(_core.BlpTimeoutError) as raised:
        await asyncio.wait_for(request_driver.request(error="timeout"), timeout=3)
    assert (raised.value.service, raised.value.bloomberg_request_id) == (None, None)
    assert raised.value.bloomberg_request_ids == []

    gated = request_driver.request(gated=True, error="session")
    request_driver.release_requests()
    with pytest.raises(_core.BlpSessionError) as raised:
        await asyncio.wait_for(gated, timeout=3)
    assert str(raised.value) == "synthetic request failure"
    assert not hasattr(raised.value, "service")


def test_recipe_ingestion_drains_python_generator_streams(ingest):
    pa = pytest.importorskip("pyarrow")
    schema = synthetic_schema(pa)
    batch = ingest(pa.RecordBatchReader.from_batches(schema, synthetic_batches(pa, schema, 3)))
    assert isinstance(batch, xbbg.ArrowRecordBatch)
    assert batch.to_pylist() == synthetic_rows(0, 6)


def test_native_and_foreign_recipe_inputs_ingest_identically(ingest):
    pa = pytest.importorskip("pyarrow")
    table = xbbg.ArrowTable.from_pylist(synthetic_rows(0, 4))
    native = ingest(table)
    assert native.to_pylist() == synthetic_rows(0, 4)
    expected = native.to_pyarrow()
    foreign = table.to_pyarrow()
    for data in (
        foreign,
        foreign.to_batches()[0],
        pa.RecordBatchReader.from_batches(foreign.schema, foreign.to_batches()),
    ):
        assert ingest(data).to_pyarrow().equals(expected, check_metadata=True)


def test_recipe_ingestion_keeps_producer_error_types(ingest):
    pa = pytest.importorskip("pyarrow")
    schema = synthetic_schema(pa)

    def failing_batches():
        yield from synthetic_batches(pa, schema, 1)
        raise ValueError("synthetic producer failure")

    with pytest.raises(TypeError, match="synthetic producer failure"):
        ingest(pa.RecordBatchReader.from_batches(schema, failing_batches()))
    with pytest.raises(ValueError, match="__arrow_c_stream__"):
        ingest(object())
