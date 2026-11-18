"""Tests for the check that the N-API engine addon links no Bloomberg runtime."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts import check_napi_addon_links as check
from scripts.test_stage_jvm_natives import make_elf, make_macho, make_pe

NAME = "napi_xbbg.node"


def write(directory: Path, data: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / NAME
    path.write_bytes(data)
    return path


@pytest.mark.parametrize(
    ("clean", "linked"),
    [
        (make_elf(["libc.so.6"]), make_elf(["libblpapi3_64.so", "libc.so.6"])),
        (make_pe(["KERNEL32.dll"]), make_pe(["KERNEL32.dll", "blpapi3_64.dll"])),
        (
            make_macho(f"@rpath/{NAME}", ["/usr/lib/libSystem.B.dylib"]),
            make_macho(f"@rpath/{NAME}", ["@rpath/libblpapi3_64.so"]),
        ),
    ],
    ids=["elf", "pe", "macho"],
)
def test_addon_must_not_reference_the_bloomberg_runtime(tmp_path, clean, linked):
    assert check.main([str(write(tmp_path / "good", clean))]) == 0
    assert check.main([str(write(tmp_path / "bad", linked))]) == 1


def test_macho_addon_must_be_relocatable(tmp_path):
    wrong_name = write(tmp_path / "name", make_macho("@rpath/other.node", ["/usr/lib/libSystem.B.dylib"]))
    absolute = write(tmp_path / "abs", make_macho(f"@rpath/{NAME}", ["/opt/build/libfoo.dylib"]))

    with pytest.raises(check.StageError, match="install name"):
        check.check(wrong_name)
    with pytest.raises(check.StageError, match="non-portable"):
        check.check(absolute)
