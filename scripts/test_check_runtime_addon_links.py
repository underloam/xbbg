"""Tests for the check that the N-API resolver addon links no Bloomberg runtime and stays small."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts import check_runtime_addon_links as check
from scripts.test_stage_jvm_natives import make_elf, make_macho, make_pe

NAME = "napi_xbbg_runtime.node"


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


def test_addon_over_the_size_budget_is_rejected(tmp_path, capsys):
    addon = make_elf(["libc.so.6"], payload=b"\x01" * 10_000)
    path = write(tmp_path, addon)

    assert check.DEFAULT_MAX_BYTES == 2 * 1024 * 1024
    assert check.main([str(path)]) == 0
    assert check.main(["--max-bytes", str(len(addon)), str(path)]) == 0
    capsys.readouterr()

    assert check.main(["--max-bytes", str(len(addon) - 1), str(path)]) == 1
    error = capsys.readouterr().err
    assert f"is {len(addon)} bytes" in error
    assert f"over the {len(addon) - 1} byte budget" in error
    assert "must not enable xbbg-sdk's `provision` feature" in error


def test_default_budget_rejects_a_provisioner_sized_addon(tmp_path, capsys):
    path = write(tmp_path, make_elf(["libc.so.6"], payload=bytes(range(256)) * 12 * 1024))

    assert path.stat().st_size > check.DEFAULT_MAX_BYTES
    assert check.main([str(path)]) == 1
    assert "`provision` feature" in capsys.readouterr().err


def test_missing_addon_is_an_error(tmp_path, capsys):
    assert check.main([str(tmp_path / "absent.node")]) == 1
    assert "error:" in capsys.readouterr().err
