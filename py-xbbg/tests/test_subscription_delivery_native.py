"""Offline public delivery behavior through real SDK decoding and shared feeds.

The private driver exists only in native extensions built with `test-support`.
Set XBBG_REQUIRE_TEST_SUPPORT=1 in its dedicated run to fail rather than skip
when the wrong extension is installed.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from types import SimpleNamespace
import warnings
import weakref

import narwhals.stable.v1 as nw
import pytest

import xbbg
from xbbg import _core, _engine, backend as backend_module

TOPIC = "SYNTHETIC DELIVERY Equity"
SECOND = "SYNTHETIC SECOND Equity"
LABEL = "synthetic delivery"


@pytest.fixture
async def native_factory(monkeypatch):
    factory = getattr(_core, "_test_subscription_fixture", None)
    if factory is None:
        if os.environ.get("XBBG_REQUIRE_TEST_SUPPORT") == "1":
            pytest.fail("native extension was not built with pyo3-xbbg/test-support")
        pytest.skip("requires a native extension built with pyo3-xbbg/test-support")
    owners = []

    async def create(**config):
        owner = await factory(_core.PyEngineConfig(**config) if config else None)
        owners.append(owner)
        monkeypatch.setattr(_engine, "_get_engine", lambda: owner)
        return owner

    yield create
    for owner in owners:
        owner.terminate()
        await owner.settle()


def observe_delivery_reads(owner, monkeypatch, observer=None, *, drain_observer=None):
    """Observe real bridge futures without replacing native queues or receipts."""

    class ObservedNative:
        def __init__(self, native):
            self.native = native

        def __getattr__(self, name):
            return getattr(self.native, name)

        def next_delivery(self, converter=None):
            future = self.native.next_delivery(converter)
            if observer is not None:
                observer(future)
            return future

        def unsubscribe_deliveries(self, drain=False, converter=None):
            future = self.native.unsubscribe_deliveries(drain, converter)
            if drain and drain_observer is not None:
                drain_observer(future)
            return future

    def observe_subscribe(method):
        async def subscribe(*args, **kwargs):
            return ObservedNative(await method(*args, **kwargs))

        return subscribe

    engine = SimpleNamespace(
        subscribe=observe_subscribe(owner.subscribe),
        subscribe_with_options=observe_subscribe(owner.subscribe_with_options),
    )
    monkeypatch.setattr(_engine, "_get_engine", lambda: engine)


def publish(owner, topic=TOPIC, **values):
    owner.publish_json(topic, json.dumps(values))


def arrow_table(frame):
    if isinstance(frame, (xbbg.ArrowTable, xbbg.ArrowRecordBatch)):
        return backend_module.ensure_arrow_table(frame).to_pyarrow()
    return nw.from_native(frame).to_arrow()


def rows(delivery):
    return arrow_table(delivery.data).to_pylist()


async def read(view):
    return await asyncio.wait_for(view.read(), timeout=3)


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["native", "pyarrow", "pandas", "polars"])
async def test_drop_oldest_retains_gap_and_sparse_presence_across_backends(native_factory, backend):
    if backend != "native":
        pytest.importorskip(backend)
    owner = await native_factory()
    sub = await xbbg.asubscribe(
        TOPIC,
        ["BID", "ASK"],
        stream_capacity=2,
        flush_threshold=16,
        overflow_policy="drop_oldest",
    )
    publish(owner, BID=10.0, ASK=11.0)
    publish(owner, BID=12.0)
    publish(owner, ASK=None)
    delivery = await read(sub.deliveries(backend=backend))
    assert delivery.kind == "deltas"
    assert delivery.gap_before == xbbg.SubscriptionGap(dropped_updates=1)
    assert delivery.gap_before.cause == "local_overflow"
    data = arrow_table(delivery.data)
    emitted = data.to_pylist()
    assert len(emitted) == 2
    assert [row["topic"] for row in emitted] == [TOPIC, TOPIC]
    assert emitted[0]["BID"] == 12.0
    assert emitted[1]["ASK"] is None
    fields = [name for name in data.column_names if name not in {"timestamp", "topic", "__xbbg_present"}]
    bid_bit, ask_bit = 1 << fields.index("BID"), 1 << fields.index("ASK")
    assert int.from_bytes(emitted[0]["__xbbg_present"], "little") & (bid_bit | ask_bit) == bid_bit
    assert int.from_bytes(emitted[1]["__xbbg_present"], "little") & (bid_bit | ask_bit) == ask_bit
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_delivery_view_default_is_native_and_immutable_even_for_tick_subscriptions(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(
        TOPIC, "BID", tick_mode=True, raw=True, backend="pandas", stream_capacity=1, overflow_policy="drop_oldest"
    )
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    view = sub.deliveries()
    delivery = await read(view)
    assert isinstance(view, xbbg.DeliveryStream)
    assert isinstance(delivery, xbbg.SubscriptionDelivery)
    assert isinstance(delivery.data, xbbg.ArrowTable)
    assert delivery.data.to_pylist()[0]["BID"] == 11.0
    assert not hasattr(delivery, "__dict__")
    assert not hasattr(delivery.gap_before, "__dict__")
    with pytest.raises(AttributeError):
        delivery.kind = "state"
    with pytest.raises(AttributeError):
        delivery.gap_before.dropped_updates = 0
    await view.aclose()
    assert not sub.is_active


@pytest.mark.asyncio
async def test_backend_conversion_failure_restores_real_batch_and_gap(native_factory, monkeypatch):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", stream_capacity=1, overflow_policy="drop_oldest")
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)

    def conversion_fails(_table, _backend):
        raise ValueError("synthetic backend conversion failure")

    monkeypatch.setattr(backend_module, "convert_backend_frame", conversion_fails)
    with pytest.raises(ValueError, match="synthetic backend conversion failure"):
        await read(sub.deliveries(backend="native"))
    delivery = await read(sub.deliveries(raw=True))
    assert isinstance(delivery.data, xbbg.ArrowRecordBatch)
    assert delivery.gap_before.dropped_updates == 1
    assert delivery.data.to_pylist()[0]["BID"] == 11.0
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("tick_mode", [False, True])
async def test_inherited_drop_oldest_guards_plain_iteration_and_drain_with_cleanup(native_factory, tick_mode):
    owner = await native_factory(overflow_policy="drop_oldest", subscription_stream_capacity=1)
    sub = await xbbg.asubscribe(TOPIC, "BID", tick_mode=tick_mode)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    with pytest.raises(xbbg.BlpValidationError):
        aiter(sub)
    with pytest.raises(xbbg.BlpValidationError):
        await anext(sub)
    delivery = await read(sub.deliveries())
    assert delivery.gap_before.dropped_updates == 1
    assert rows(delivery)[0]["BID"] == 11.0
    publish(owner, BID=12.0)
    with pytest.raises(xbbg.BlpValidationError):
        await sub.unsubscribe(drain=True)
    assert not sub.is_active
    await sub.unsubscribe(drain=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_cancelled_pending_read_keeps_future_updates(native_factory, monkeypatch, mode):
    owner = await native_factory()
    requested = asyncio.Event()
    observe_delivery_reads(owner, monkeypatch, lambda _future: requested.set())
    kwargs = {"overflow_policy": "drop_oldest", "stream_capacity": 1} if mode == "deltas" else {}
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode, **kwargs)
    view = sub.deliveries()
    if mode == "state":
        assert rows(await read(view))[0]["valid"] is False
    requested.clear()
    pending = asyncio.create_task(view.read())
    await asyncio.wait_for(requested.wait(), timeout=3)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await sub.add_fields("ASK")
    publish(owner, BID=10.0, ASK=11.0, MKTDATA_EVENT_SUBTYPE="INITPAINT")
    delivery = await read(view)
    assert rows(delivery)[0]["BID"] == 10.0
    assert rows(delivery)[0]["ASK"] == 11.0
    await view.close()
    assert not sub.is_active


@pytest.mark.asyncio
async def test_state_conflates_full_rows_without_raw_overflow_invalidating_image(native_factory):
    owner = await native_factory()
    state = await xbbg.asubscribe(
        TOPIC, ["BID", "ASK"], delivery="state", aliases={TOPIC: LABEL}, tick_mode=True, flush_threshold=8
    )
    view = state.deliveries()
    initial = rows(await read(view))[0]
    assert initial["topic"] == LABEL
    assert initial["valid"] is False
    strict = await xbbg.asubscribe(TOPIC, ["BID", "ASK"], stream_capacity=1)
    publish(owner, BID=10.0, ASK=11.0)
    for value in range(12, 40):
        publish(owner, BID=float(value))
    publish(owner, ASK=None)
    delivery = await read(view)
    snapshot = rows(delivery)
    assert delivery.kind == "state"
    assert delivery.gap_before is None
    assert len(snapshot) == 1
    assert snapshot[0]["topic"] == LABEL
    assert snapshot[0]["BID"] == 39.0
    assert snapshot[0]["ASK"] is None
    assert snapshot[0]["valid"] is True
    with pytest.raises(xbbg.BlpSubscriptionDataLossError):
        async for _ in strict:
            pass
    assert state.is_active
    with pytest.raises(xbbg.BlpValidationError):
        aiter(state)
    await strict.unsubscribe()
    await state.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["native", "pyarrow", "pandas", "polars"])
async def test_state_invalidation_recovery_and_clear_remain_explicit(native_factory, backend):
    if backend != "native":
        pytest.importorskip(backend)
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, ["BID", "ASK"], delivery="state")
    view = sub.deliveries(backend=backend)
    await read(view)  # Initial unknown image.
    publish(owner, BID=10.0, ASK=11.0)
    assert rows(await read(view))[0]["valid"] is True
    owner.data_loss(TOPIC)
    await owner.settle()
    publish(owner, BID=99.0)  # A delta before INITPAINT cannot restore validity.
    publish(owner, BID=12.0, MKTDATA_EVENT_TYPE="SUMMARY", MKTDATA_EVENT_SUBTYPE="INITPAINT")
    invalid = rows(await read(view))[0]
    assert invalid["valid"] is False
    assert invalid["live"] is False
    assert invalid["BID"] is None
    assert invalid["ASK"] is None
    recovered = rows(await read(view))[0]
    assert recovered["valid"] is True
    assert recovered["live"] is False
    assert recovered["BID"] == 12.0
    assert recovered["ASK"] is None
    publish(owner, ASK=13.0)
    live = rows(await read(view))[0]
    assert live["valid"] is True
    assert live["live"] is True
    assert live["BID"] == 12.0
    assert live["ASK"] == 13.0
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_marked_drain_retains_final_data_after_native_detach(native_factory, mode):
    owner = await native_factory()
    kwargs = {"overflow_policy": "drop_oldest", "stream_capacity": 2} if mode == "deltas" else {}
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode, flush_threshold=8, **kwargs)
    view = sub.deliveries(backend="pyarrow")
    if mode == "state":
        await read(view)
    for value in range(3):
        publish(owner, BID=float(value))
    drained = await view.unsubscribe(drain=True)
    assert not sub.is_active
    assert all(item.kind == mode for item in drained)
    emitted = [row for item in drained for row in rows(item)]
    if mode == "deltas":
        assert [item["BID"] for item in emitted] == [1.0, 2.0]
        assert drained[0].gap_before.dropped_updates == 1
        assert all(item.gap_before is None for item in drained[1:])
    else:
        assert len(emitted) == 1
        assert emitted[0]["BID"] == 2.0
        assert emitted[0]["valid"] is True
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_source_loss_remains_terminal_for_lossy_raw_and_wins_partial_drain(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest", stream_capacity=1)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    owner.data_loss(TOPIC)
    with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
        await sub.deliveries().unsubscribe(drain=True)
    assert raised.value.topic == TOPIC
    assert raised.value.detail
    assert not sub.is_active


@pytest.mark.asyncio
async def test_explicit_close_wakes_native_delivery_reader(native_factory, monkeypatch):
    owner = await native_factory()
    requested = asyncio.Event()
    observe_delivery_reads(owner, monkeypatch, lambda _future: requested.set())
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    pending = asyncio.create_task(sub.deliveries().read())
    await asyncio.wait_for(requested.wait(), timeout=3)
    await asyncio.wait_for(sub.unsubscribe(), timeout=3)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending, timeout=3)


@pytest.mark.asyncio
async def test_default_strict_and_image_only_behaviors_remain_unchanged(native_factory):
    owner = await native_factory()
    strict = await xbbg.asubscribe(TOPIC, "BID", stream_capacity=1)
    image = await xbbg.asubscribe(TOPIC, "BID", rows=False)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    with pytest.raises(xbbg.BlpSubscriptionDataLossError):
        async for _ in strict:
            pass
    assert image.latest(backend="native").to_pylist()[0]["BID"] == 11.0
    with pytest.raises(RuntimeError):
        image.deliveries()
    with pytest.raises(RuntimeError):
        await anext(image)
    await strict.unsubscribe()
    await image.unsubscribe()


@pytest.mark.asyncio
async def test_idle_native_reader_does_not_block_dynamic_controls(native_factory, monkeypatch):
    owner = await native_factory()
    requested = asyncio.Event()
    observe_delivery_reads(owner, monkeypatch, lambda _future: requested.set())
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    view = sub.deliveries()
    pending = asyncio.create_task(view.read())
    await asyncio.wait_for(requested.wait(), timeout=3)
    await asyncio.wait_for(sub.add_fields("ASK"), timeout=3)
    publish(owner, BID=10.0, ASK=11.0, MKTDATA_EVENT_SUBTYPE="INITPAINT")
    delivery = await asyncio.wait_for(pending, timeout=3)
    assert rows(delivery)[0]["ASK"] == 11.0
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_state_projection_growth_and_alias_removal_use_real_memberships(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, ["BID", "ASK"], delivery="state", aliases={TOPIC: LABEL}, flush_threshold=8)
    view = sub.deliveries()
    await read(view)
    publish(owner, BID=10.0, ASK=None)
    assert rows(await read(view))[0]["topic"] == LABEL
    await sub.add(SECOND, aliases={SECOND: "second source"})
    publish(owner, topic=SECOND, BID=20.0, ASK=21.0)
    assert {row["topic"] for row in rows(await read(view))} == {"second source"}
    await sub.add_fields("NOTE")
    await sub.remove("second source")
    publish(owner, BID=12.0, ASK=None, NOTE="synthetic", MKTDATA_EVENT_SUBTYPE="INITPAINT")
    snapshot = rows(await read(view))
    assert len(snapshot) == 1
    assert snapshot[0]["topic"] == LABEL
    assert snapshot[0]["BID"] == 12.0
    assert snapshot[0]["ASK"] is None
    assert snapshot[0]["NOTE"] == "synthetic"
    assert snapshot[0]["valid"] is True
    assert sub.tickers == [LABEL]
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_delivery_view_emits_native_delayed_warnings(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    publish(owner, BID=10.0, IS_DELAYED_STREAM=True)
    with pytest.warns(xbbg.BlpDelayedDataWarning):
        delivery = await read(sub.deliveries())
    assert rows(delivery)[0]["BID"] == 10.0
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_cancelling_during_backend_conversion_rolls_back_native_receipt(native_factory, monkeypatch, mode):
    owner = await native_factory()
    kwargs = {"overflow_policy": "drop_oldest", "stream_capacity": 1} if mode == "deltas" else {}
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode, **kwargs)
    if mode == "state":
        await read(sub.deliveries())
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    started, release = threading.Event(), threading.Event()
    convert = backend_module.convert_backend_frame

    def blocked_converter(table, backend):
        started.set()
        if not release.wait(timeout=3):
            raise TimeoutError("test did not release the backend converter")
        return convert(table, backend)

    monkeypatch.setattr(backend_module, "convert_backend_frame", blocked_converter)
    pending = asyncio.create_task(sub.deliveries(backend="native").read())
    try:
        assert await asyncio.to_thread(started.wait, 3)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        publish(owner, BID=12.0)
    finally:
        release.set()
    restored = await read(sub.deliveries())
    if mode == "deltas":
        assert rows(restored)[0]["BID"] == 11.0
        assert restored.gap_before.dropped_updates == 1
        newest = await read(sub.deliveries())
        assert rows(newest)[0]["BID"] == 12.0
        assert newest.gap_before is None
    else:
        assert rows(restored)[0]["BID"] == 12.0
        assert rows(restored)[0]["valid"] is True
        assert restored.gap_before is None
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["astream", "stream"])
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_simple_stream_helpers_deliberately_yield_marked_advanced_batches(
    native_factory, monkeypatch, entrypoint, mode
):
    owner = await native_factory(overflow_policy="drop_oldest", subscription_stream_capacity=1)
    handles = []

    async def subscribe(*args, **kwargs):
        native = await owner.subscribe(*args, **kwargs)
        handles.append(native)
        publish(owner, BID=10.0)
        publish(owner, BID=11.0)
        return native

    monkeypatch.setattr(_engine, "_get_engine", lambda: SimpleNamespace(subscribe=subscribe))
    if entrypoint == "astream":
        source = xbbg.astream(TOPIC, "BID", delivery=mode, tick_mode=True)
        try:
            delivery = await asyncio.wait_for(anext(source), timeout=3)
        finally:
            await source.aclose()
    else:
        source = xbbg.stream(TOPIC, "BID", delivery=mode, tick_mode=True)
        try:
            delivery = next(source)
        finally:
            source.close()
    assert isinstance(delivery, xbbg.SubscriptionDelivery)
    assert isinstance(delivery.data, xbbg.ArrowTable)
    assert delivery.kind == mode
    assert rows(delivery)[0]["BID"] == 11.0
    if mode == "deltas":
        assert delivery.gap_before.dropped_updates == 1
    else:
        assert rows(delivery)[0]["valid"] is True
    assert len(handles) == 1
    assert not handles[0].is_active


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "terminal"), [("deltas", False), ("state", False), ("deltas", True)])
async def test_cancelled_after_native_completion_before_task_resume_restores_receipt(
    native_factory, monkeypatch, mode, terminal
):
    owner = await native_factory(overflow_policy="drop_oldest", subscription_stream_capacity=1)
    completed = asyncio.Event()
    target = []
    armed = False

    def on_read(future):
        nonlocal armed
        if not armed:
            return
        armed = False

        def before_task_resume(done):
            if not done.cancelled() and done.exception() is None:
                if mode == "state":
                    publish(owner, BID=12.0)
                task = target[0]()
                if task is not None:
                    task.cancel()
            completed.set()

        # Registered before DeliveryStream awaits this actual native future;
        # asyncio therefore invokes it before the task's wakeup callback.
        future.add_done_callback(before_task_resume)

    observe_delivery_reads(owner, monkeypatch, on_read)
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode)
    view = sub.deliveries()
    if mode == "state":
        await read(view)
    if terminal:
        owner.data_loss(TOPIC)
    else:
        publish(owner, BID=10.0)
        publish(owner, BID=11.0)
    armed = True
    pending = asyncio.create_task(view.read())
    target.append(weakref.ref(pending))
    await asyncio.wait_for(completed.wait(), timeout=3)
    with pytest.raises(asyncio.CancelledError):
        await pending
    if terminal:
        with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
            await read(view)
        assert raised.value.topic == TOPIC
        with pytest.raises(StopAsyncIteration):
            await read(view)
    else:
        restored = await read(view)
        assert rows(restored)[0]["BID"] == (12.0 if mode == "state" else 11.0)
        if mode == "deltas":
            assert restored.gap_before.dropped_updates == 1
        else:
            assert rows(restored)[0]["valid"] is True
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("inherited", [False, True])
async def test_tick_output_does_not_reduce_explicit_or_inherited_lossy_batch_hint(native_factory, inherited):
    owner = await native_factory(
        **({"overflow_policy": "drop_oldest", "subscription_stream_capacity": 64} if inherited else {})
    )
    options = {} if inherited else {"overflow_policy": "drop_oldest", "stream_capacity": 64}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        sub = await xbbg.asubscribe(TOPIC, "BID", tick_mode=True, flush_threshold=64, **options)
    assert not caught
    for value in range(40):
        publish(owner, BID=float(value))
    delivery = await read(sub.deliveries())
    assert [row["BID"] for row in rows(delivery)] == [float(value) for value in range(40)]
    assert delivery.gap_before is None
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_strict_tick_iteration_still_warns_and_reads_one_update_at_a_time(native_factory):
    owner = await native_factory()
    with pytest.warns(UserWarning, match="tick_mode"):
        sub = await xbbg.asubscribe(TOPIC, "BID", tick_mode=True, flush_threshold=64)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    first = await asyncio.wait_for(anext(sub), timeout=3)
    second = await asyncio.wait_for(anext(sub), timeout=3)
    assert isinstance(first, dict)
    assert isinstance(second, dict)
    assert first["BID"] == 10.0
    assert second["BID"] == 11.0
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_raising_tick_configuration_warning_closes_the_native_subscription(native_factory):
    owner = await native_factory()
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        with pytest.raises(UserWarning):
            await xbbg.asubscribe(TOPIC, "BID", tick_mode=True, flush_threshold=64)
    await owner.settle()
    with pytest.raises(_core.BlpValidationError):
        publish(owner, BID=10.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "drain"])
async def test_raising_delayed_warning_does_not_consume_gap_or_data(native_factory, operation):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest", stream_capacity=1)
    publish(owner, BID=10.0, IS_DELAYED_STREAM=True)
    publish(owner, BID=11.0)
    view = sub.deliveries()
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        with pytest.raises(xbbg.BlpDelayedDataWarning):
            if operation == "read":
                await read(view)
            else:
                await asyncio.wait_for(view.unsubscribe(drain=True), timeout=3)
        if operation == "read":
            restored = [await read(view)]
        else:
            restored = await asyncio.wait_for(view.unsubscribe(drain=True), timeout=3)
    assert len(restored) == 1
    assert restored[0].gap_before.dropped_updates == 1
    assert rows(restored[0])[0]["BID"] == 11.0
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "drain"])
async def test_pending_warning_cannot_replace_terminal_source_error(native_factory, operation):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    publish(owner, IS_DELAYED_STREAM=True)
    owner.data_loss(TOPIC)
    view = sub.deliveries()
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
            if operation == "read":
                await read(view)
            else:
                await asyncio.wait_for(view.unsubscribe(drain=True), timeout=3)
    assert raised.value.topic == TOPIC
    if operation == "read":
        with pytest.raises(StopAsyncIteration):
            await read(view)
    else:
        assert await view.unsubscribe(drain=True) == []
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_pending_warning_cannot_replace_read_cancellation(native_factory, monkeypatch):
    owner = await native_factory()
    requested = asyncio.Event()
    observe_delivery_reads(owner, monkeypatch, lambda _future: requested.set())
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    publish(owner, IS_DELAYED_STREAM=True)
    view = sub.deliveries()
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        pending = asyncio.create_task(view.read())
        await asyncio.wait_for(requested.wait(), timeout=3)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    publish(owner, BID=12.0)
    assert rows(await read(view))[0]["BID"] == 12.0
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_failed_marked_drain_conversion_retains_closed_source_for_retry(native_factory, monkeypatch, mode):
    owner = await native_factory()
    options = {"overflow_policy": "drop_oldest", "stream_capacity": 1} if mode == "deltas" else {}
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode, **options)
    if mode == "state":
        await read(sub.deliveries())
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)

    def fail_conversion(_table, _backend):
        raise ValueError("synthetic drain conversion failure")

    monkeypatch.setattr(backend_module, "convert_backend_frame", fail_conversion)
    with pytest.raises(ValueError, match="synthetic drain conversion failure"):
        await sub.deliveries(backend="native").unsubscribe(drain=True)
    assert not sub.is_active
    restored = await asyncio.wait_for(sub.deliveries(raw=True).unsubscribe(drain=True), timeout=3)
    assert len(restored) == 1
    assert rows(restored[0])[0]["BID"] == 11.0
    if mode == "deltas":
        assert restored[0].gap_before.dropped_updates == 1
    else:
        assert rows(restored[0])[0]["valid"] is True
    assert await sub.deliveries().unsubscribe(drain=True) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deltas", "state"])
async def test_cancelling_blocked_marked_drain_converter_retains_final_data(native_factory, monkeypatch, mode):
    owner = await native_factory()
    options = {"overflow_policy": "drop_oldest", "stream_capacity": 1} if mode == "deltas" else {}
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode, **options)
    if mode == "state":
        await read(sub.deliveries())
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    started, release = threading.Event(), threading.Event()
    convert = backend_module.convert_backend_frame

    def blocked_converter(table, backend):
        started.set()
        if not release.wait(timeout=3):
            raise TimeoutError("test did not release the drain converter")
        return convert(table, backend)

    monkeypatch.setattr(backend_module, "convert_backend_frame", blocked_converter)
    pending = asyncio.create_task(sub.deliveries(backend="native").unsubscribe(drain=True))
    try:
        assert await asyncio.to_thread(started.wait, 3)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    finally:
        release.set()
    restored = await asyncio.wait_for(sub.deliveries(raw=True).unsubscribe(drain=True), timeout=3)
    assert len(restored) == 1
    assert rows(restored[0])[0]["BID"] == 11.0
    if mode == "deltas":
        assert restored[0].gap_before.dropped_updates == 1
    else:
        assert rows(restored[0])[0]["valid"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "terminal"), [("deltas", False), ("state", False), ("deltas", True)])
async def test_cancelled_after_native_drain_completion_restores_data_or_terminal(
    native_factory, monkeypatch, mode, terminal
):
    owner = await native_factory(overflow_policy="drop_oldest", subscription_stream_capacity=1)
    completed = asyncio.Event()
    target = []
    armed = True

    def on_drain(future):
        nonlocal armed
        if not armed:
            return
        armed = False

        def before_task_resume(done):
            if not done.cancelled() and done.exception() is None:
                task = target[0]()
                if task is not None:
                    task.cancel()
            completed.set()

        future.add_done_callback(before_task_resume)

    observe_delivery_reads(owner, monkeypatch, drain_observer=on_drain)
    sub = await xbbg.asubscribe(TOPIC, "BID", delivery=mode)
    view = sub.deliveries()
    if mode == "state":
        await read(view)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)
    if terminal:
        owner.data_loss(TOPIC)
    pending = asyncio.create_task(view.unsubscribe(drain=True))
    target.append(weakref.ref(pending))
    await asyncio.wait_for(completed.wait(), timeout=3)
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not sub.is_active
    if terminal:
        with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
            await asyncio.wait_for(view.unsubscribe(drain=True), timeout=3)
        assert raised.value.topic == TOPIC
    else:
        restored = await asyncio.wait_for(view.unsubscribe(drain=True), timeout=3)
        assert len(restored) == 1
        assert rows(restored[0])[0]["BID"] == 11.0
        if mode == "deltas":
            assert restored[0].gap_before.dropped_updates == 1
        else:
            assert rows(restored[0])[0]["valid"] is True
    assert await view.unsubscribe(drain=True) == []


@pytest.mark.asyncio
async def test_non_draining_cleanup_discards_a_failed_marked_drain(native_factory, monkeypatch):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest", stream_capacity=1)
    publish(owner, BID=10.0)
    publish(owner, BID=11.0)

    def fail_conversion(_table, _backend):
        raise ValueError("synthetic drain conversion failure")

    monkeypatch.setattr(backend_module, "convert_backend_frame", fail_conversion)
    with pytest.raises(ValueError, match="synthetic drain conversion failure"):
        await sub.deliveries(backend="native").unsubscribe(drain=True)
    await sub.unsubscribe(drain=False)
    assert await sub.deliveries(raw=True).unsubscribe(drain=True) == []


@pytest.mark.asyncio
async def test_plain_advanced_drain_preserves_terminal_priority_over_pending_warning(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    publish(owner, IS_DELAYED_STREAM=True)
    owner.data_loss(TOPIC)
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
            await sub.unsubscribe(drain=True)
    assert raised.value.topic == TOPIC
    assert not sub.is_active


@pytest.mark.asyncio
async def test_plain_advanced_iteration_guard_is_not_hidden_by_warning_filter(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", overflow_policy="drop_oldest")
    publish(owner, IS_DELAYED_STREAM=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        with pytest.raises(xbbg.BlpError, match=r"deliveries\(\)"):
            await anext(sub)
    publish(owner, BID=12.0)
    assert rows(await read(sub.deliveries()))[0]["BID"] == 12.0
    await sub.unsubscribe()


@pytest.mark.asyncio
async def test_legacy_strict_source_error_is_not_hidden_by_pending_warning(native_factory):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID")
    publish(owner, IS_DELAYED_STREAM=True)
    owner.data_loss(TOPIC)
    with warnings.catch_warnings():
        warnings.simplefilter("error", xbbg.BlpDelayedDataWarning)
        with pytest.raises(xbbg.BlpSubscriptionDataLossError) as raised:
            await anext(sub)
    assert raised.value.topic == TOPIC
    await sub.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize("tick_mode", [False, True])
async def test_plain_reader_waits_for_cancelled_strict_delivery_publication(native_factory, monkeypatch, tick_mode):
    owner = await native_factory()
    sub = await xbbg.asubscribe(TOPIC, "BID", tick_mode=tick_mode)
    publish(owner, BID=10.0)
    started, release = threading.Event(), threading.Event()
    convert = backend_module.convert_backend_frame

    def blocked_converter(table, backend):
        started.set()
        if not release.wait(timeout=3):
            raise TimeoutError("test did not release the strict delivery converter")
        return convert(table, backend)

    monkeypatch.setattr(backend_module, "convert_backend_frame", blocked_converter)
    marked = asyncio.create_task(sub.deliveries(backend="native").read())
    plain = None
    try:
        assert await asyncio.to_thread(started.wait, 3)
        marked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await marked
        plain = asyncio.create_task(anext(sub))
        await asyncio.wait_for(owner.wait_legacy_read_started(), timeout=3)
    finally:
        release.set()
    assert plain is not None
    restored = await asyncio.wait_for(plain, timeout=3)
    if tick_mode:
        assert isinstance(restored, dict)
        assert restored["BID"] == 10.0
    else:
        assert restored.to_pylist()[0]["BID"] == 10.0
        assert restored.num_rows == 1
    await sub.unsubscribe()
