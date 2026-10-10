"""Consumer typing contract: ty check py-xbbg/tests/typing/package_exports.py."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, assert_type

import xbbg

if TYPE_CHECKING:
    assert_type(xbbg.request(xbbg.Service.REFDATA, xbbg.Operation.REFERENCE_DATA), Any)
    assert_type(xbbg.seat_type(), str)
    assert_type(xbbg.check_entitlements([1]), xbbg._core.EntitlementReport)
    assert_type(xbbg.identity_is_authorized(), bool)
    assert_type(xbbg.subscribe("TEST US Equity", "PX_LAST"), xbbg.Subscription)
    assert_type(xbbg.vwap("TEST US Equity"), xbbg.Subscription)
    assert_type(xbbg.mktbar("TEST US Equity"), xbbg.Subscription)
    assert_type(xbbg.depth("TEST US Equity"), xbbg.Subscription)
    assert_type(xbbg.chains("TEST US Equity"), xbbg.Subscription)
    assert_type(xbbg.bta("TEST US Equity", "sma"), Any)
    assert_type(xbbg.bops(), list[str])
    assert_type(xbbg.bschema(), dict[Any, Any])
    assert_type(xbbg.Engine().worker_health(), list[tuple[int, str]])
