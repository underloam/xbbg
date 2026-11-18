#!/usr/bin/env python3
"""Check that a wheel's ``xbbg._core`` never links the Bloomberg runtime.

xbbg loads the Bloomberg runtime at run time (``xbbg_core::load``), so ``xbbg._core`` must import
on a machine that has none: the Python package imports it first, then loads the runtime it finds
(or reports where it looked). A dynamic reference to ``blpapi3_64`` in the extension would make
that import fail with a loader error before xbbg can say anything useful.

The check reads the extension's dynamic dependencies straight from the wheel (PE imports, ELF
``DT_NEEDED``, Mach-O load commands), the same way ``stage_jvm_natives.py`` checks the JVM natives,
so it runs on any host.

    python scripts/check_wheel_core_links.py dist/*.whl
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import re
import sys
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stage_jvm_natives as natives

_CORE_MODULE = re.compile(r"^xbbg/_core(?:\.[^/]+)?\.(?:so|pyd|dylib)$")


def dependencies(data: bytes) -> list[str]:
    """Libraries the loader must resolve before this PE, ELF or Mach-O image can run."""
    if data[:2] == b"MZ":
        return natives.pe_imports(data)
    if data[:4] == b"\x7fELF":
        return natives.elf_dynamic(data)[1]
    return list(natives.macho_load_commands(data).dylibs)


def check_wheel(wheel: Path) -> list[str]:
    """Problems with ``wheel``'s ``xbbg._core`` module; empty when it is present and links no Bloomberg runtime."""
    with zipfile.ZipFile(wheel) as archive:
        cores = [name for name in archive.namelist() if _CORE_MODULE.match(name)]
        if len(cores) != 1:
            return [f"{wheel}: expected exactly one xbbg._core extension module, found {cores or 'none'}"]
        try:
            references = dependencies(archive.read(cores[0]))
        except natives.StageError as error:
            return [f"{wheel}: {cores[0]}: {error}"]
    blpapi = [reference for reference in references if natives.is_blpapi_runtime(reference)]
    if blpapi:
        return [f"{wheel}: {cores[0]} must never link the Bloomberg runtime; it references {blpapi}"]
    return []


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("wheels", nargs="+", type=Path, help="Wheel files to check")
    args = parser.parse_args(argv)
    problems = [problem for wheel in args.wheels for problem in check_wheel(wheel)]
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
