"""Session derivation from Bloomberg exchange metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from xbbg._core import ext_derive_sessions

if TYPE_CHECKING:
    from xbbg.markets.bloomberg import ExchangeInfo


@dataclass
class SessionWindows:
    """Trading session windows for a security."""

    day: tuple[str, str] | None = None
    allday: tuple[str, str] | None = None
    pre: tuple[str, str] | None = None
    post: tuple[str, str] | None = None
    am: tuple[str, str] | None = None
    pm: tuple[str, str] | None = None

    def to_dict(self) -> dict[str, tuple[str, str]]:
        """Convert to dict, excluding None values."""
        result: dict[str, tuple[str, str]] = {}
        if self.day:
            result["day"] = self.day
        if self.allday:
            result["allday"] = self.allday
        if self.pre:
            result["pre"] = self.pre
        if self.post:
            result["post"] = self.post
        if self.am:
            result["am"] = self.am
        if self.pm:
            result["pm"] = self.pm
        return result


def derive_sessions(exchange_info: ExchangeInfo) -> SessionWindows:
    """Derive native windows from ``day`` hours, retaining explicit windows.

    Runtime overrides are authoritative and are not expanded or rewritten by
    market rules. For other metadata, Rust owns day normalization and rule
    lookup, including MIC precedence, lunch breaks and continuous trading.
    """
    sessions = exchange_info.sessions
    day = sessions.get("day")
    if exchange_info.source == "override" or day is None:
        return SessionWindows(**sessions)

    values = ext_derive_sessions(
        day_start=day[0],
        day_end=day[1],
        mic=exchange_info.mic,
        exch_code=exchange_info.exch_code,
    )
    values.update({name: window for name, window in sessions.items() if name != "day"})
    return SessionWindows(**values)


def get_session_windows(
    ticker: str,
    mic: str | None = None,
    exch_code: str | None = None,
    regular_hours: tuple[str, str] | None = None,
) -> SessionWindows:
    """Derive native sessions without a Bloomberg query.

    ``ticker`` is retained as caller metadata; native rules depend only on
    ``mic``, ``exch_code`` and ``regular_hours``. Invalid hours produce empty
    windows. Session endpoints follow Rust rules, including post-market opening
    one minute after the day close.
    """
    from xbbg.markets.bloomberg import ExchangeInfo

    sessions = {"day": regular_hours} if regular_hours is not None else {}
    return derive_sessions(ExchangeInfo(ticker=ticker, mic=mic, exch_code=exch_code, sessions=sessions))
