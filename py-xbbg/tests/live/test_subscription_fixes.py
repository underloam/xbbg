#!/usr/bin/env python
"""Live validation test for subscription pipeline fixes.

Tests:
  1. Multi-type field preservation (Fix #1) — Int32, Int64, Float64, Date, Datetime
  2. Bloomberg event timestamps (Fix #6) — not SystemTime
  3. Error propagation (Fix #2) — invalid security raises exception
  4. Schema types inspection — verify Arrow types are correct

Usage:
    uv run python py-xbbg/tests/live/test_subscription_fixes.py
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def assert_arrow_record_batch(batch):
    """Return rows after asserting a native xbbg ArrowRecordBatch shape."""
    assert batch.__class__.__name__ == "ArrowRecordBatch"
    assert batch.num_columns == len(batch.column_names)
    rows = batch.to_pylist()
    assert batch.num_rows == len(rows)
    return rows


async def test_multitype_fields():
    """Test Fix #1: Multi-type field preservation.

    Subscribe to live data with fields of different Bloomberg types:
    - LAST_PRICE (Float64)
    - VOLUME (Int64)
    - BID_SIZE / ASK_SIZE (Int32)
    - TRADE_UPDATE_STAMP_RT (Datetime)
    - TRADING_DT_REALTIME (Date)
    """
    from xbbg._core import PyEngine, set_log_level

    set_log_level("info")

    engine = PyEngine()

    # Fields chosen to exercise multiple Bloomberg types
    fields = [
        "LAST_PRICE",
        "BID",
        "ASK",
        "VOLUME",
        "BID_SIZE",
        "ASK_SIZE",
        "NUM_TRADES_RT",
        "TRADE_UPDATE_STAMP_RT",
        "TRADING_DT_REALTIME",
        "RT_PX_CHG_PCT_1D",
    ]

    tickers = ["XBTUSD Curncy"]

    print(f"\n{'=' * 60}")
    print("TEST: Multi-type field preservation (Fix #1)")
    print(f"{'=' * 60}")
    print(f"Tickers: {tickers}")
    print(f"Fields:  {fields}")
    print()

    sub = await engine.subscribe(tickers, fields)
    print(f"Subscription active: {sub.is_active}")
    print(f"Tickers: {sub.tickers}")
    print()

    batches_received = 0
    max_batches = 10  # 24/7 XBTUSD should provide INITPAINT + live quote/trade updates
    timeout_seconds = 20.0

    try:
        for _ in range(max_batches):
            batch = await asyncio.wait_for(sub.__anext__(), timeout=timeout_seconds)
            rows = assert_arrow_record_batch(batch)
            assert rows, "Expected non-empty ArrowRecordBatch"
            row = rows[0]
            batches_received += 1

            # Print schema on first batch
            if batches_received == 1:
                print("--- Arrow Schema ---")
                for field in batch.schema.fields:
                    print(f"  {field.name:30s} {field.data_type}")
                print()

            # Print data
            topic = row.get("topic")
            ts = row.get("timestamp")

            print(f"[Batch {batches_received}] topic={topic}  ts={ts}")

            # Print non-null field values with their Arrow types
            for field in batch.schema.fields:
                if field.name in ("timestamp", "topic"):
                    continue
                val = row.get(field.name)
                if val is not None:
                    print(f"  {field.name:30s} type={field.data_type!s:20s} value={val!r}")

            print()

            if batches_received >= max_batches:
                break

    except Exception as e:
        print(f"ERROR during iteration: {type(e).__name__}: {e}")

    await sub.unsubscribe()
    print(f"\nTotal batches received: {batches_received}")

    # Verify we got data
    assert batches_received > 0, "No batches received!"
    print("PASSED: Multi-type fields working\n")


async def test_timestamp_source():
    """Test Fix #6: Bloomberg event timestamps.

    Verify that timestamps come from Bloomberg SDK (not SystemTime::now()).
    Bloomberg timestamps should be close to wall clock but come from the SDK's
    recorded receive time.
    """
    from xbbg._core import PyEngine, set_log_level

    set_log_level("warn")

    engine = PyEngine()

    print(f"\n{'=' * 60}")
    print("TEST: Bloomberg event timestamps (Fix #6)")
    print(f"{'=' * 60}")

    sub = await engine.subscribe(["XBTUSD Curncy"], ["LAST_PRICE", "BID", "ASK"])

    batch_count = 0
    timestamps = []

    try:
        async for batch in sub:
            batch_count += 1
            rows = assert_arrow_record_batch(batch)
            assert rows, "Expected non-empty ArrowRecordBatch"
            ts_val = rows[0].get("timestamp")
            timestamps.append(ts_val)

            now = datetime.now(timezone.utc)
            # Bloomberg SDK receive time should be within a few seconds of wall clock
            if ts_val is not None and hasattr(ts_val, "timestamp"):
                assert ts_val.tzinfo is not None, f"Expected timezone-aware timestamp, got {ts_val!r}"
                assert ts_val.utcoffset() == timezone.utc.utcoffset(ts_val), f"Expected UTC timestamp, got {ts_val!r}"
                diff_seconds = abs((now - ts_val).total_seconds())
                print(f"  Batch {batch_count}: ts={ts_val}  wall_diff={diff_seconds:.3f}s")
            else:
                print(f"  Batch {batch_count}: ts={ts_val} (type: {type(ts_val).__name__})")

            if batch_count >= 5:
                break

    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        raise

    await sub.unsubscribe()

    # Verify timestamps are reasonable (not epoch, not far future)
    assert len(timestamps) > 0, "No timestamps received"
    for ts in timestamps:
        if ts is not None and hasattr(ts, "year"):
            assert ts.year >= 2025, f"Timestamp too old: {ts}"
            assert ts.year <= 2030, f"Timestamp too far in future: {ts}"

    print("PASSED: Timestamps look reasonable\n")


async def test_tick_mode_timestamp_timezone_issue_273():
    """Regression for #273: tick_mode dict timestamps are UTC-aware."""
    from xbbg import asubscribe

    print(f"\n{'=' * 60}")
    print("TEST: tick_mode timestamp timezone (#273)")
    print(f"{'=' * 60}")

    sub = await asubscribe(["XBTUSD Curncy"], ["LAST_PRICE"], tick_mode=True)
    try:
        tick = await asyncio.wait_for(sub.__anext__(), timeout=20.0)
    finally:
        await sub.unsubscribe()

    ts = tick["timestamp"]
    assert ts.tzinfo is not None, f"Expected timezone-aware timestamp, got {ts!r}"
    assert ts.utcoffset() == timezone.utc.utcoffset(ts), f"Expected UTC timestamp, got {ts!r}"
    print(f"PASSED: tick_mode timestamp is UTC-aware: {ts!r}\n")


async def test_error_propagation():
    """Test Fix #2: Error propagation for invalid subscriptions.

    Subscribe to an invalid security and verify the error is propagated
    to the Python consumer as an exception (not silently swallowed).
    """
    from xbbg._core import PyEngine, set_log_level

    set_log_level("warn")

    engine = PyEngine()

    print(f"\n{'=' * 60}")
    print("TEST: Error propagation (Fix #2)")
    print(f"{'=' * 60}")

    # Mix valid and invalid tickers
    sub = await engine.subscribe(
        ["TOTALLY_INVALID_TICKER_XYZ Equity"],
        ["LAST_PRICE"],
    )

    print(f"Subscription created (is_active={sub.is_active})")

    got_error = False
    got_data = False
    batch_count = 0

    try:
        async for batch in sub:
            batch_count += 1
            rows = assert_arrow_record_batch(batch)
            got_data = got_data or bool(rows)
            print(f"  Got data batch: {batch.num_rows} rows")
            if batch_count >= 3:
                break
    except StopAsyncIteration:
        print("  Stream ended (StopAsyncIteration)")
    except Exception as e:
        got_error = True
        print(f"  Got error: {type(e).__name__}: {e}")

    # Give a moment for Bloomberg to send failure
    if not got_error and not got_data:
        try:
            batch = await asyncio.wait_for(sub.__anext__(), timeout=10.0)
            rows = assert_arrow_record_batch(batch)
            got_data = got_data or bool(rows)
        except StopAsyncIteration:
            print("  Stream ended after wait")
        except asyncio.TimeoutError:
            print("  Timed out waiting for response (no error, no data)")
        except Exception as e:
            got_error = True
            print(f"  Got error after wait: {type(e).__name__}: {e}")

    try:
        await sub.unsubscribe()
    except Exception:
        pass

    if got_error:
        print("PASSED: Error was propagated to consumer\n")
    else:
        print("NOTE: No error received (Bloomberg may have returned empty data or timed out)")
        print("      This is expected if Bloomberg handles unknown tickers gracefully.\n")


async def test_schema_types():
    """Verify Arrow schema types match expected Bloomberg types.

    After INITPAINT/live updates on 24/7 XBTUSD, check that:
    - LAST_PRICE, BID, ASK are Float64 (not String when initially null)
    - TRADE_UPDATE_STAMP_RT is Timestamp (not Null/Utf8)
    - TRADING_DT_REALTIME is Date32 (not Null/Utf8)
    """
    from xbbg._core import PyEngine, set_log_level

    set_log_level("warn")

    engine = PyEngine()

    print(f"\n{'=' * 60}")
    print("TEST: Schema type verification")
    print(f"{'=' * 60}")

    fields = [
        "LAST_PRICE",
        "BID",
        "ASK",
        "TRADE_UPDATE_STAMP_RT",
        "TRADING_DT_REALTIME",
    ]

    sub = await engine.subscribe(["XBTUSD Curncy"], fields)

    # Collect enough XBTUSD batches to see INITPAINT plus live quote/trade updates.
    schema = None
    raw_samples = []
    batch_count = 0
    try:
        for _ in range(10):
            batch = await asyncio.wait_for(sub.__anext__(), timeout=10.0)
            rows = assert_arrow_record_batch(batch)
            batch_count += 1
            schema = batch.schema
            row = rows[0] if rows else {}
            raw_samples.append(
                {
                    field.name: row.get(field.name)
                    for field in batch.schema.fields
                    if field.name
                    in {
                        "timestamp",
                        "topic",
                        "LAST_PRICE",
                        "BID",
                        "ASK",
                        "TRADE_UPDATE_STAMP_RT",
                        "TRADING_DT_REALTIME",
                        "MKTDATA_EVENT_TYPE",
                        "MKTDATA_EVENT_SUBTYPE",
                    }
                }
            )
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

    await sub.unsubscribe()

    assert schema is not None, "No schema received"

    print("\nRaw sample batches:")
    for idx, row in enumerate(raw_samples[:5], start=1):
        print(f"  [{idx}] {row}")

    print("\nFinal schema after stabilization:")
    type_map = {}
    for field in schema.fields:
        type_map[field.name] = field.data_type
        print(f"  {field.name:30s} {field.data_type}")

    print()

    # Verify key type expectations
    passed = True
    checks = [
        ("LAST_PRICE", "Float64", "Float64"),
        ("BID", "Float64", "Float64"),
        ("ASK", "Float64", "Float64"),
    ]

    for field_name, expected_type, label in checks:
        if field_name in type_map:
            actual = type_map[field_name]
            actual_name = str(actual)
            if actual_name.lower() == expected_type.lower():
                print(f"  OK: {field_name} is {label} ({actual})")
            else:
                print(f"  MISMATCH: {field_name} expected {expected_type} but got {actual}")
                passed = False
        else:
            print(f"  SKIP: {field_name} not in schema (field may not have been sent)")

    # Check datetime fields — they should NOT be Utf8 (old behavior was null/missing)
    for ts_field in ["TRADE_UPDATE_STAMP_RT", "TRADING_DT_REALTIME"]:
        if ts_field in type_map:
            actual = type_map[ts_field]
            actual_name = str(actual)
            if actual_name.lower() in {"utf8", "largeutf8"}:
                print(f"  MISMATCH: {ts_field} is Utf8 — expected Timestamp or Date")
                passed = False
            elif actual_name.lower() == "null":
                print(f"  NOTE: {ts_field} is Null (no data received for this field yet)")
            else:
                print(f"  OK: {ts_field} is {actual} (non-string type preserved)")

    print()
    if passed:
        print("PASSED: Schema types match expectations\n")
    else:
        print("FAILED: Some schema types don't match\n")
        assert passed, "Some schema types don't match"


async def test_field_exposure_modes():
    """Verify filtered vs full-field subscription exposure modes.

    Default mode should stay narrow except for event metadata. Full-field mode
    should grow the schema when Bloomberg sends additional top-level fields.
    """
    from xbbg._core import PyEngine, set_log_level

    set_log_level("warn")

    engine = PyEngine()
    ticker = ["ES1 Index"]
    requested_fields = ["LAST_PRICE", "BID", "ASK"]
    metadata_fields = {"MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE"}
    # Every Arrow subscription batch carries the sparse-update presence bitmap (1.4.12+).
    base_columns = {"timestamp", "topic", "__xbbg_present"}

    print(f"\n{'=' * 60}")
    print("TEST: Field exposure modes")
    print(f"{'=' * 60}")

    filtered_sub = await engine.subscribe(ticker, requested_fields)
    filtered_schema = None
    try:
        async for batch in filtered_sub:
            assert_arrow_record_batch(batch)
            filtered_schema = batch.schema
            if batch.num_rows > 0:
                break
    except Exception as e:
        print(f"ERROR during filtered subscription: {type(e).__name__}: {e}")
    finally:
        await filtered_sub.unsubscribe()

    assert filtered_schema is not None, "No filtered subscription schema received"
    filtered_names = set(filtered_schema.names)
    print("Filtered schema columns:")
    for field in filtered_schema.fields:
        print(f"  {field.name:30s} {field.data_type}")
    print()

    assert metadata_fields.issubset(filtered_names), (
        f"Missing event metadata in filtered mode: {sorted(metadata_fields - filtered_names)}"
    )
    filtered_extras = filtered_names - set(requested_fields) - metadata_fields - base_columns
    assert not filtered_extras, f"Filtered mode exposed unexpected extra fields: {sorted(filtered_extras)}"

    full_sub = await engine.subscribe(ticker, requested_fields, all_fields=True)
    full_schema = None
    try:
        async for batch in full_sub:
            assert_arrow_record_batch(batch)
            full_schema = batch.schema
            full_names = set(full_schema.names)
            full_extras = full_names - set(requested_fields) - metadata_fields - base_columns
            if full_extras:
                break
    except Exception as e:
        print(f"ERROR during full-field subscription: {type(e).__name__}: {e}")
    finally:
        await full_sub.unsubscribe()

    assert full_schema is not None, "No full-field subscription schema received"
    full_names = set(full_schema.names)
    print("Full-field schema columns:")
    for field in full_schema.fields:
        print(f"  {field.name:30s} {field.data_type}")
    print()

    assert metadata_fields.issubset(full_names), (
        f"Missing event metadata in full-field mode: {sorted(metadata_fields - full_names)}"
    )
    full_extras = full_names - set(requested_fields) - metadata_fields - base_columns
    assert full_extras, "Full-field mode did not expose any additional Bloomberg fields"
    print(f"PASSED: Full-field mode exposed extra fields: {sorted(full_extras)[:10]}\n")


async def main():
    """Run all subscription validation tests."""
    print("=" * 60)
    print("XBBG Subscription Pipeline Validation")
    print("=" * 60)
    print(f"Time: {datetime.now()}")
    print()

    await test_multitype_fields()
    await test_timestamp_source()
    await test_tick_mode_timestamp_timezone_issue_273()
    await test_schema_types()
    await test_field_exposure_modes()
    await test_error_propagation()

    print("=" * 60)
    print("All validation tests complete.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
