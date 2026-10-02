"""Exchange-auction fields, validated venue routing, snapshots, and streams.

Auction data belongs to an exchange listing, not its composite. Subscription
helpers resolve and validate every input before opening any stream and preserve
source security labels through aliases. Time-of-day values are terminal-local.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from xbbg._core import ext_auction_field_groups, ext_auction_zero_price_fields, ext_imbalance_side
from xbbg.exceptions import BlpValidationError
from xbbg.ext._utils import _call_native_recipe, _syncify

if TYPE_CHECKING:
    from xbbg.backend import Backend
    from xbbg.blp import Subscription


class AUCTION:
    """Stream-valid field tuples, sourced from the native auction catalog.

    ``DEFAULT`` combines IMBALANCE, INDICATIVE, STATE, HALTS, and RESULTS.
    COMPOSITE and QUOTES are opt-in groups. ZERO_PRICE_FIELDS is a separate list
    of price fields whose zero sentinels default to null in subscription latest().
    Reference snapshots and streams can differ in scalar types (for example,
    Y/N versus bool for IN_AUCTION_RT).
    """

    _groups = ext_auction_field_groups()
    IMBALANCE = tuple(_groups["imbalance"])
    INDICATIVE = tuple(_groups["indicative"])
    STATE = tuple(_groups["state"])
    HALTS = tuple(_groups["halts"])
    RESULTS = tuple(_groups["results"])
    COMPOSITE = tuple(_groups["composite"])
    QUOTES = tuple(_groups["quotes"])
    DEFAULT = tuple(_groups["default"])
    ZERO_PRICE_FIELDS = tuple(ext_auction_zero_price_fields())
    del _groups


def imbalance_side(code: str) -> str | None:
    """Normalize a Bloomberg imbalance code to buy, sell, none, or unknown (None)."""
    return ext_imbalance_side(code)


def _as_list(values: str | Sequence[str]) -> list[str]:
    return [values] if isinstance(values, str) else list(values)


def _zero_as_null_fields(fields: Sequence[str], zero_as_null: Sequence[str] | None) -> Sequence[str]:
    if zero_as_null is not None:
        return _as_list(zero_as_null)
    return tuple(field for field in AUCTION.ZERO_PRICE_FIELDS if field in fields)


async def aresolve_venues(
    securities: str | Sequence[str],
    *,
    pcs_overrides: Mapping[str, str] | None = None,
    backend: Backend | str | None = None,
) -> Any:
    """Resolve and validate primary exchange venues, preserving input order and duplicates.

    Bare valid ISINs are normalized by the native recipe. Preferred exchange
    pricing sources can be overridden per call; every candidate is validated.
    Failed inputs remain rows with status/error diagnostics rather than silently
    returning a composite listing.
    """
    return await _call_native_recipe(
        "recipe_resolve_venues", _as_list(securities), dict(pcs_overrides or {}), backend=backend
    )


async def aauction_snapshot(
    securities: str | Sequence[str],
    fields: str | Sequence[str] = AUCTION.DEFAULT,
    *,
    pcs_overrides: Mapping[str, str] | None = None,
    backend: Backend | str | None = None,
) -> Any:
    """Snapshot auction fields on validated venues; failed rows have null field values.

    Empty fields select AUCTION.DEFAULT. Returned metadata includes input_order,
    security, venue_topic, status, and error, followed by requested field columns.
    Field types follow Bloomberg metadata; time-only values have no invented date.
    """
    return await _call_native_recipe(
        "recipe_auction_snapshot",
        _as_list(securities),
        _as_list(fields),
        dict(pcs_overrides or {}),
        backend=backend,
    )


def _subscription_inputs(securities: str | Sequence[str], kwargs: Mapping[str, Any]) -> list[str]:
    inputs = list(dict.fromkeys(_as_list(securities)))
    if not inputs:
        raise BlpValidationError("securities must not be empty")
    if "aliases" in kwargs:
        raise BlpValidationError("auction subscriptions set aliases from resolved venues to input securities")
    return inputs


def _validated_venues(inputs: list[str], rows: list[dict[str, Any]]) -> dict[str, str]:
    by_order = {row["input_order"]: row for row in rows}
    if (
        len(rows) != len(inputs)
        or len(by_order) != len(inputs)
        or any(
            index not in by_order or by_order[index].get("security") != security
            for index, security in enumerate(inputs)
        )
    ):
        raise BlpValidationError(f"Venue resolution is not one-to-one with inputs: {', '.join(inputs)}")

    failures = []
    owners: dict[str, list[str]] = {}
    for index, security in enumerate(inputs):
        row = by_order[index]
        topic = row.get("venue_topic")
        if row.get("status") != "resolved" or not isinstance(topic, str) or not topic.strip():
            failures.append(f"{security}: {row.get('error') or row.get('status') or 'missing venue'}")
        else:
            owners.setdefault(topic, []).append(security)
    failures.extend(
        f"ambiguous venue {topic}: {', '.join(labels)}" for topic, labels in owners.items() if len(labels) > 1
    )
    if failures:
        raise BlpValidationError("Auction preflight failed: " + "; ".join(failures))
    return {topic: labels[0] for topic, labels in owners.items()}


async def asubscribe_auction(
    securities: str | Sequence[str],
    fields: str | Sequence[str] = AUCTION.DEFAULT,
    *,
    pcs_overrides: Mapping[str, str] | None = None,
    rows: bool = True,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
    **subscribe_kwargs: Any,
) -> Subscription:
    """Open one auction subscription after atomic venue preflight.

    Identical input strings collapse. Any failed venue or distinct inputs sharing
    a venue raises BlpValidationError naming the inputs before subscribing. Rows,
    tickers, and status use source security labels; remove accepts those labels.
    Dynamic add expects already-resolved venue topics (and optional aliases).
    Set rows=False for an image-only board: poll latest() instead of iterating.
    Rejected fields follow on_field_error ("warn", "raise", or "ignore").
    In latest() only, zero_as_null defaults to ZERO_PRICE_FIELDS intersected with
    requested fields; pass () to retain zeros. Tick rows remain unchanged.
    Remaining options, including on_delayed and isolated, pass to asubscribe.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, subscribe_kwargs)
    table = await aresolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table.to_pylist())
    requested = _as_list(fields)
    return await blp.asubscribe(
        list(aliases),
        requested,
        aliases=aliases,
        rows=rows,
        on_field_error=on_field_error,
        zero_as_null=_zero_as_null_fields(requested, zero_as_null),
        **subscribe_kwargs,
    )


async def astream_auction(
    securities: str | Sequence[str],
    fields: str | Sequence[str] = AUCTION.DEFAULT,
    *,
    pcs_overrides: Mapping[str, str] | None = None,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
    **stream_kwargs: Any,
) -> AsyncGenerator[Any, None]:
    """Iterate auction rows with atomic preflight and the policies of asubscribe_auction.

    Rows are always delivered. Use asubscribe_auction(rows=False) for a latest()
    board; zero_as_null affects only latest(), never the yielded rows.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, stream_kwargs)
    table = await aresolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table.to_pylist())
    requested = _as_list(fields)
    source = blp.astream(
        list(aliases),
        requested,
        aliases=aliases,
        on_field_error=on_field_error,
        zero_as_null=_zero_as_null_fields(requested, zero_as_null),
        **stream_kwargs,
    )
    try:
        async for batch in source:
            yield batch
    finally:
        await source.aclose()


def stream_auction(
    securities: str | Sequence[str],
    fields: str | Sequence[str] = AUCTION.DEFAULT,
    *,
    pcs_overrides: Mapping[str, str] | None = None,
    on_field_error: str = "warn",
    zero_as_null: Sequence[str] | None = None,
    **stream_kwargs: Any,
) -> Generator[Any, None, None]:
    """Iterate auction data synchronously using the notebook-safe stream bridge.

    Preflight finishes before the producer starts. New pool-session claims wait
    at most five seconds; consumers joining existing shared feeds never wait for
    session capacity. Rows are always delivered.
    Field-error and zero-sentinel policies match asubscribe_auction; zero_as_null
    affects only latest(), never the yielded rows.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, stream_kwargs)
    table = resolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table.to_pylist())
    requested = _as_list(fields)
    yield from blp.stream(
        list(aliases),
        requested,
        aliases=aliases,
        on_field_error=on_field_error,
        zero_as_null=_zero_as_null_fields(requested, zero_as_null),
        **stream_kwargs,
    )


resolve_venues = _syncify(aresolve_venues)
auction_snapshot = _syncify(aauction_snapshot)
subscribe_auction = _syncify(asubscribe_auction)

__all__ = [
    "AUCTION",
    "imbalance_side",
    "resolve_venues",
    "aresolve_venues",
    "auction_snapshot",
    "aauction_snapshot",
    "subscribe_auction",
    "asubscribe_auction",
    "stream_auction",
    "astream_auction",
]
