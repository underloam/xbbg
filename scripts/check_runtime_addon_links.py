#!/usr/bin/env python3
"""Fail unless an N-API resolver addon (``napi_xbbg_runtime.node``) stays free of Bloomberg and small.

The addon must load before the Bloomberg runtime exists, so it may never depend on it. This reads
each addon's dynamic dependencies (PE imports, ELF ``DT_NEEDED``, Mach-O dylibs) with the parsers
that vet the JVM natives and rejects any Bloomberg runtime reference. A Mach-O addon must also
carry the install name ``@rpath/<file name>`` and no absolute build paths.

The addon is also held to a size budget (``--max-bytes``, 2 MiB by default; a release build is
about 0.4 MB on macOS). The resolver is xbbg-sdk with its default features; enabling the
``provision`` feature adds the whole downloader (13-18 MB), so an addon over budget means that
feature leaked in.

    python scripts/check_runtime_addon_links.py js-xbbg/packages/xbbg-core-linux-x64/napi_xbbg_runtime.node
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stage_jvm_natives import (
    BinaryFormatError,
    StageError,
    elf_dynamic,
    is_blpapi_runtime,
    macho_load_commands,
    pe_imports,
)

#: Default size budget of the addon in bytes.
DEFAULT_MAX_BYTES = 2 * 1024 * 1024


def _is_non_portable_darwin_path(value: str) -> bool:
    return value.startswith("/") and not value.startswith(("/usr/lib/", "/System/Library/"))


def dependencies(path: Path, data: bytes) -> list[str]:
    """The libraries the dynamic loader must resolve before ``data`` can run."""
    if data[:2] == b"MZ":
        return pe_imports(data)
    if data[:4] == b"\x7fELF":
        return elf_dynamic(data)[1]
    return list(macho_load_commands(data).dylibs)


def check(path: Path, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
    """Raise ``StageError`` when ``path`` references Bloomberg, is not relocatable or is over budget."""
    data = path.read_bytes()
    deps = dependencies(path, data)
    blpapi = [dep for dep in deps if is_blpapi_runtime(dep)]
    if blpapi:
        raise StageError(f"{path.name} must never link the Bloomberg runtime; it references {blpapi}")
    if data[:2] != b"MZ" and data[:4] != b"\x7fELF":
        image = macho_load_commands(data)
        if image.install_name != f"@rpath/{path.name}":
            raise StageError(f"{path.name} has install name {image.install_name!r}, expected '@rpath/{path.name}'")
        absolute = [value for value in (*image.dylibs, *image.rpaths) if _is_non_portable_darwin_path(value)]
        if absolute:
            raise StageError(f"{path.name} contains non-portable build paths: {', '.join(absolute)}")
    if len(data) > max_bytes:
        raise StageError(
            f"{path.name} is {len(data)} bytes, over the {max_bytes} byte budget; "
            "the resolver must not enable xbbg-sdk's `provision` feature"
        )
    print(f"{path}: no Bloomberg link ({len(deps)} dependencies, {len(data)} bytes)")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("addons", nargs="+", type=Path, help="Resolver addons to check")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="Size budget of each addon in bytes (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    try:
        for addon in args.addons:
            check(addon, args.max_bytes)
    except (StageError, BinaryFormatError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
