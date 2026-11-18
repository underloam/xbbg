#!/usr/bin/env python3
"""Check that a wheel ships ``xbbg._runtime`` and that it never links the Bloomberg runtime.

``xbbg._core`` links ``blpapi3_64`` when the interpreter loads it, so the Bloomberg library must be
found first. ``xbbg._runtime`` is the small native module that does that lookup in the xbbg cache.
If it ever referenced the Bloomberg runtime itself, it would fail to import on exactly the machines
it exists for. The check reads the module's dynamic dependencies straight from the wheel (PE
imports, ELF ``DT_NEEDED``, Mach-O load commands), the same way ``stage_jvm_natives.py`` checks the
JVM natives, so it runs on any host.

    python scripts/check_wheel_runtime_module.py dist/*.whl
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

_RUNTIME_MODULE = re.compile(r"^xbbg/_runtime(?:\.[^/]+)?\.(?:so|pyd|dylib)$")
_CORE_MODULE = re.compile(r"^xbbg/_core(?:\.[^/]+)?\.(?:so|pyd|dylib)$")


def dependencies(data: bytes) -> list[str]:
    """Libraries the loader must resolve before this PE, ELF or Mach-O image can run."""
    if data[:2] == b"MZ":
        return natives.pe_imports(data)
    if data[:4] == b"\x7fELF":
        return natives.elf_dynamic(data)[1]
    return list(natives.macho_load_commands(data).dylibs)


def check_wheel(wheel: Path) -> list[str]:
    """Problems with ``wheel``'s ``xbbg._runtime`` module; empty when it is present and clean."""
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        runtime = [name for name in names if _RUNTIME_MODULE.match(name)]
        if not any(_CORE_MODULE.match(name) for name in names):
            return [f"{wheel}: no xbbg._core extension module"]
        if len(runtime) != 1:
            return [f"{wheel}: expected exactly one xbbg._runtime extension module, found {runtime or 'none'}"]
        try:
            references = dependencies(archive.read(runtime[0]))
        except natives.StageError as error:
            return [f"{wheel}: {runtime[0]}: {error}"]
    blpapi = [reference for reference in references if natives.is_blpapi_runtime(reference)]
    if blpapi:
        return [f"{wheel}: {runtime[0]} must never link the Bloomberg runtime; it references {blpapi}"]
    return []


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("wheels", nargs="+", type=Path, help="Wheel files to check")
    problems = [problem for wheel in parser.parse_args(argv).wheels for problem in check_wheel(wheel)]
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
