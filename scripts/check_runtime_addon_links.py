#!/usr/bin/env python3
"""Fail unless an N-API resolver addon (``napi_xbbg_runtime.node``) stays free of Bloomberg.

The addon must load before the Bloomberg runtime exists, so it may never depend on it. This reads
each addon's dynamic dependencies (PE imports, ELF ``DT_NEEDED``, Mach-O dylibs) with the parsers
that vet the JVM natives and rejects any Bloomberg runtime reference. A Mach-O addon must also
carry the install name ``@rpath/<file name>`` and no absolute build paths.

    python scripts/check_runtime_addon_links.py js-xbbg/packages/xbbg-core-linux-x64/napi_xbbg_runtime.node
"""

from __future__ import annotations

from pathlib import Path
import sys

from stage_jvm_natives import (
    BinaryFormatError,
    StageError,
    elf_dynamic,
    is_blpapi_runtime,
    macho_load_commands,
    pe_imports,
)


def _is_non_portable_darwin_path(value: str) -> bool:
    return value.startswith("/") and not value.startswith(("/usr/lib/", "/System/Library/"))


def dependencies(path: Path, data: bytes) -> list[str]:
    """The libraries the dynamic loader must resolve before ``data`` can run."""
    if data[:2] == b"MZ":
        return pe_imports(data)
    if data[:4] == b"\x7fELF":
        return elf_dynamic(data)[1]
    return list(macho_load_commands(data).dylibs)


def check(path: Path) -> None:
    """Raise ``StageError`` when ``path`` references Bloomberg or is not relocatable."""
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
    print(f"{path}: no Bloomberg link ({len(deps)} dependencies)")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        for name in argv:
            check(Path(name))
    except (StageError, BinaryFormatError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
