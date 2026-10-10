"""Adapters for the native process-wide exchange override registry."""

from __future__ import annotations

from xbbg import _core
from xbbg.markets.bloomberg import _SESSION_NAMES, ExchangeInfo, _exchange_info_from_native


def set_exchange_override(
    ticker: str,
    *,
    timezone: str | None = None,
    mic: str | None = None,
    exch_code: str | None = None,
    sessions: dict[str, tuple[str, str]] | None = None,
) -> None:
    """Set or merge metadata in the registry used by the native engine.

    Session keys are ``day``, ``allday``, ``pre``, ``post``, ``am`` and ``pm``;
    use ``day`` for regular or futures trading hours. A supplied nonempty
    session dictionary replaces the entire previous session set, while an
    omitted dictionary preserves it. Other omitted fields are also preserved.
    Empty session dictionaries are rejected because the native binding cannot
    represent an explicit empty session patch. Clear and re-register the
    override to remove all of its sessions.
    """
    if sessions is not None:
        if not sessions:
            raise ValueError("sessions must contain at least one native session window")
        unknown = sessions.keys() - _SESSION_NAMES
        if unknown:
            raise ValueError(f"Unknown session keys: {', '.join(sorted(unknown))}; use day for regular/futures hours")

    windows = sessions if sessions is not None else {}
    _core.ext_set_exchange_override(
        ticker,
        timezone=timezone,
        mic=mic,
        exch_code=exch_code,
        day=windows.get("day"),
        allday=windows.get("allday"),
        pre=windows.get("pre"),
        post=windows.get("post"),
        am=windows.get("am"),
        pm=windows.get("pm"),
    )


def get_exchange_override(ticker: str) -> ExchangeInfo | None:
    """Materialize a native override, or return ``None`` for an unknown ticker."""
    values = _core.ext_get_exchange_override(ticker.strip())
    return None if values is None else _exchange_info_from_native(values)


def clear_exchange_override(ticker: str | None = None) -> None:
    """Clear one override, or all overrides only when ``ticker is None``.

    An empty or whitespace-only ticker is a no-op, not the native binding's
    clear-all operation.
    """
    if ticker is not None:
        ticker = ticker.strip()
        if not ticker:
            return
    _core.ext_clear_exchange_override(ticker)


def list_exchange_overrides() -> dict[str, ExchangeInfo]:
    """Materialize every override in the native registry."""
    return {
        ticker: _exchange_info_from_native(values) for ticker, values in _core.ext_list_exchange_overrides().items()
    }


def has_override(ticker: str) -> bool:
    """Check the native registry without maintaining a Python-side copy."""
    return _core.ext_get_exchange_override(ticker.strip()) is not None
