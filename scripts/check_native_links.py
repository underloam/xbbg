#!/usr/bin/env python3
"""Verify shipped native artifacts never link the Bloomberg runtime.

Every xbbg binding opens the Bloomberg runtime at run time (``xbbg_core::load``), so the Python
wheel extension, N-API addon and ``xbbg-mcp`` binary must load on a machine with no Bloomberg
runtime installed. This inspects PE imports, ELF ``DT_NEEDED`` and Mach-O load commands directly
so the check runs on any host.

Examples::

    python scripts/check_native_links.py --wheel dist/*.whl
    python scripts/check_native_links.py --addon js-xbbg/packages/xbbg-core-linux-x64/napi_xbbg.node
    python scripts/check_native_links.py --executable target/release/xbbg-mcp
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import re
import sys
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _binary_format import (
    BinaryFormatError,
    StageError,
    dependencies,
    is_blpapi_runtime,
    is_non_portable_darwin_path,
    macho_load_commands,
)

_CORE_MODULE = re.compile(r"^xbbg/_core(?:\.[^/]+)?\.(?:so|pyd|dylib)$")


def check_wheel(wheel: Path) -> list[str]:
    """Return problems with ``wheel``'s ``xbbg._core`` extension; empty when clean."""
    with zipfile.ZipFile(wheel) as archive:
        cores = [name for name in archive.namelist() if _CORE_MODULE.match(name)]
        if len(cores) != 1:
            return [f"{wheel}: expected exactly one xbbg._core extension module, found {cores or 'none'}"]
        try:
            references = dependencies(archive.read(cores[0]))
        except StageError as error:
            return [f"{wheel}: {cores[0]}: {error}"]
    blpapi = [reference for reference in references if is_blpapi_runtime(reference)]
    if blpapi:
        return [f"{wheel}: {cores[0]} must never link the Bloomberg runtime; it references {blpapi}"]
    return []


def check_binary(path: Path, *, executable: bool = False) -> None:
    """Raise ``StageError`` when ``path`` references Bloomberg or is not relocatable."""
    data = path.read_bytes()
    deps = dependencies(data)
    blpapi = [dep for dep in deps if is_blpapi_runtime(dep)]
    if blpapi:
        raise StageError(f"{path.name} must never link the Bloomberg runtime; it references {blpapi}")
    if data[:2] != b"MZ" and data[:4] != b"\x7fELF":
        image = macho_load_commands(data)
        if not executable and image.install_name != f"@rpath/{path.name}":
            raise StageError(f"{path.name} has install name {image.install_name!r}, expected '@rpath/{path.name}'")
        absolute = [value for value in (*image.dylibs, *image.rpaths) if is_non_portable_darwin_path(value)]
        if absolute:
            raise StageError(f"{path.name} contains non-portable build paths: {', '.join(absolute)}")
    print(f"{path}: no Bloomberg link ({len(deps)} dependencies)")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--wheel", action="store_true", help="Check xbbg._core inside Python wheel archives")
    mode.add_argument("--addon", action="store_true", help="Check relocatable shared libraries / N-API addons")
    mode.add_argument(
        "--executable",
        action="store_true",
        help="Check standalone executables (skips the Mach-O shared-library @rpath install_name check)",
    )
    parser.add_argument("paths", nargs="+", type=Path, help="Artifacts to check")
    args = parser.parse_args(argv)

    if args.wheel:
        problems = [problem for wheel in args.paths for problem in check_wheel(wheel)]
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1 if problems else 0

    try:
        for artifact in args.paths:
            check_binary(artifact, executable=args.executable)
    except (StageError, BinaryFormatError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
