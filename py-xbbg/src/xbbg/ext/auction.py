"""Exchange-auction fields, validated venue routing, snapshots, and streams.

Auction data belongs to an exchange listing, not its composite. Subscription
helpers resolve and validate every input before opening any stream and preserve
source security labels through aliases. Time-of-day values are terminal-local.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from xbbg._core import (
    ext_auction_field_groups,
    ext_auction_zero_price_fields,
    ext_imbalance_side,
    ext_plan_auction,
    ext_validated_auction_venues,
)
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
    """Plan the venue lookup: unique inputs in first-seen order, checked natively."""
    try:
        return ext_plan_auction(_as_list(securities), "aliases" in kwargs)
    except ValueError as exc:
        raise BlpValidationError(str(exc)) from None


def _validated_venues(inputs: list[str], table: Any) -> dict[str, str]:
    """Validate the resolution table natively; map each venue topic to its input."""
    try:
        return ext_validated_auction_venues(inputs, table)
    except ValueError as exc:
        raise BlpValidationError(str(exc)) from None


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
    In latest() and state deliveries, zero_as_null defaults to ZERO_PRICE_FIELDS
    intersected with requested fields; pass () to retain zeros. Delta ticks are unchanged.
    Remaining options, including on_delayed and isolated, pass to asubscribe.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, subscribe_kwargs)
    table = await aresolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table)
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
    board. zero_as_null affects latest() and state deliveries, not delta rows.
    State and drop-oldest modes yield marked SubscriptionDelivery envelopes;
    their data is always a batch, independently of legacy tick output.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, stream_kwargs)
    table = await aresolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table)
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
    affects latest() and state deliveries, not delta rows.
    State and drop-oldest modes yield marked SubscriptionDelivery envelopes;
    their data is always a batch, independently of legacy tick output.
    """
    from xbbg import blp

    inputs = _subscription_inputs(securities, stream_kwargs)
    table = resolve_venues(inputs, pcs_overrides=pcs_overrides, backend="native")
    aliases = _validated_venues(inputs, table)
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
