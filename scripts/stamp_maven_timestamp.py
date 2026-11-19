"""Stamp project.build.outputTimestamp in the Java parent POM for reproducible release jars."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re

_ELEMENT = re.compile(r"<project\.build\.outputTimestamp>[^<]*</project\.build\.outputTimestamp>")


def stamp_output_timestamp(pom: str, timestamp: str) -> str:
    """Return ``pom`` with its one ``project.build.outputTimestamp`` set to ``timestamp``.

    The timestamp must be ISO 8601 with a UTC offset, as ``git log -1 --format=%cI``
    prints it. Exactly one element must exist: a missing element would let the
    build fall back to an unpinned timestamp, and a second one would leave the
    effective value ambiguous.
    """
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError(f"not an ISO 8601 timestamp: {timestamp!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no UTC offset: {timestamp!r}")
    stamped, count = _ELEMENT.subn(f"<project.build.outputTimestamp>{timestamp}</project.build.outputTimestamp>", pom)
    if count != 1:
        raise ValueError(f"expected one <project.build.outputTimestamp> element, found {count}")
    return stamped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("timestamp", help="ISO 8601 timestamp with a UTC offset, e.g. from git log --format=%%cI")
    parser.add_argument("--pom", type=Path, default=Path("pom.xml"))
    args = parser.parse_args(argv)
    try:
        args.pom.write_text(
            stamp_output_timestamp(args.pom.read_text(encoding="utf-8"), args.timestamp), encoding="utf-8"
        )
    except (OSError, UnicodeError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")
    print(f"Stamped project.build.outputTimestamp {args.timestamp} in {args.pom}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
