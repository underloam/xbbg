#!/usr/bin/env python3
"""Check release versions from an installed xbbg wheel, including its native core."""

from __future__ import annotations

import argparse
from importlib.metadata import version
import sys

from packaging.version import InvalidVersion, Version


def _release_version(value: str, source: str) -> Version:
    try:
        parsed = Version(value)
    except (InvalidVersion, TypeError) as exc:
        raise ValueError(f"{source}: invalid version {value!r}") from exc
    if parsed.is_devrelease or parsed.local is not None:
        raise ValueError(f"{source}: unresolved/development version {value!r} is not a release")
    return parsed


def validate_versions(expected: str, *, distribution: str, package: str, binding: str, core: str) -> Version:
    """Require all installed surfaces to match the normalized release version."""
    release = _release_version(expected, "expected release")
    failures = []
    for source, value in (
        ("importlib.metadata.version('xbbg')", distribution),
        ("xbbg.__version__", package),
        ("_core.__version__", binding),
        ("_core.version()", core),
    ):
        try:
            actual = _release_version(value, source)
        except ValueError as exc:
            failures.append(str(exc))
        else:
            if actual != release:
                failures.append(f"{source}: {value!r} does not match {release}")
    if failures:
        raise ValueError("Installed xbbg release version check failed:\n  " + "\n  ".join(failures))
    return release


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="Expected release version or v-prefixed tag")
    args = parser.parse_args()

    # Import only at the CLI boundary so the validator can be tested without a native build.
    # The workflow uses an isolated environment and python -I to exclude checkout imports.
    import xbbg
    from xbbg import _core

    try:
        release = validate_versions(
            args.version,
            distribution=version("xbbg"),
            package=xbbg.__version__,
            binding=_core.__version__,
            core=_core.version(),
        )
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")
    print(f"Installed xbbg {release}: distribution, package, binding, and native core versions agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
