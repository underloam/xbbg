"""Tests for the unified native artifact Bloomberg link checker."""

from __future__ import annotations

from pathlib import Path
import zipfile

import pytest

from scripts import check_native_links as check
from scripts.test_stage_jvm_natives import make_elf, make_macho, make_pe

ADDON_NAME = "napi_xbbg.node"


def make_wheel(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def write_addon(directory: Path, data: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / ADDON_NAME
    path.write_bytes(data)
    return path


@pytest.mark.parametrize(
    ("core_name", "clean", "linked"),
    [
        (
            "xbbg/_core.cpython-313-x86_64-linux-gnu.so",
            make_elf(["libc.so.6", "libgcc_s.so.1"]),
            make_elf(["libc.so.6", "libblpapi3_64.so"]),
        ),
        (
            "xbbg/_core.cp313-win_amd64.pyd",
            make_pe(["KERNEL32.dll"]),
            make_pe(["KERNEL32.dll", "blpapi3_64.dll"]),
        ),
        (
            "xbbg/_core.cpython-313-darwin.so",
            make_macho("@rpath/_core.so", ["/usr/lib/libSystem.B.dylib"]),
            make_macho("@rpath/_core.so", ["@rpath/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"]),
        ),
    ],
    ids=["elf", "pe", "macho"],
)
def test_wheel_core_must_not_reference_bloomberg(tmp_path, core_name, clean, linked):
    good = make_wheel(tmp_path / "good.whl", {core_name: clean, "xbbg/__init__.py": b""})
    bad = make_wheel(tmp_path / "bad.whl", {core_name: linked})

    assert check.check_wheel(good) == []
    [problem] = check.check_wheel(bad)
    assert "must never link the Bloomberg runtime" in problem
    assert "blpapi3_64" in problem


def test_wheel_must_ship_exactly_one_core_module(tmp_path):
    core = "xbbg/_core.cpython-313-x86_64-linux-gnu.so"
    clean = make_elf(["libc.so.6"])
    missing = make_wheel(tmp_path / "missing.whl", {"xbbg/__init__.py": b""})
    doubled = make_wheel(tmp_path / "doubled.whl", {core: clean, "xbbg/_core.abi3.so": clean})

    assert "found none" in check.check_wheel(missing)[0]
    assert "exactly one" in check.check_wheel(doubled)[0]


def test_wheel_unreadable_core_module_is_reported(tmp_path):
    wheel = make_wheel(tmp_path / "junk.whl", {"xbbg/_core.cpython-313-darwin.so": b"not an image"})

    [problem] = check.check_wheel(wheel)
    assert "_core" in problem
    assert "must never link" not in problem


def test_wheel_main_exit_status(tmp_path, capsys):
    core = "xbbg/_core.cpython-313-x86_64-linux-gnu.so"
    good = make_wheel(tmp_path / "good.whl", {core: make_elf(["libc.so.6"])})
    bad = make_wheel(tmp_path / "bad.whl", {core: make_elf(["libblpapi3_64.so"])})

    assert check.main(["--wheel", str(good)]) == 0
    assert check.main(["--wheel", str(good), str(bad)]) == 1
    assert "bad.whl" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("clean", "linked"),
    [
        (make_elf(["libc.so.6"]), make_elf(["libblpapi3_64.so", "libc.so.6"])),
        (make_pe(["KERNEL32.dll"]), make_pe(["KERNEL32.dll", "blpapi3_64.dll"])),
        (
            make_macho(f"@rpath/{ADDON_NAME}", ["/usr/lib/libSystem.B.dylib"]),
            make_macho(f"@rpath/{ADDON_NAME}", ["@rpath/libblpapi3_64.so"]),
        ),
    ],
    ids=["elf", "pe", "macho"],
)
def test_addon_must_not_reference_bloomberg(tmp_path, clean, linked):
    assert check.main(["--addon", str(write_addon(tmp_path / "good", clean))]) == 0
    assert check.main(["--addon", str(write_addon(tmp_path / "bad", linked))]) == 1


def test_macho_addon_must_be_relocatable(tmp_path):
    wrong_name = write_addon(tmp_path / "name", make_macho("@rpath/other.node", ["/usr/lib/libSystem.B.dylib"]))
    absolute = write_addon(tmp_path / "abs", make_macho(f"@rpath/{ADDON_NAME}", ["/opt/build/libfoo.dylib"]))

    with pytest.raises(check.StageError, match="install name"):
        check.check_binary(wrong_name)
    with pytest.raises(check.StageError, match="non-portable"):
        check.check_binary(absolute)


def test_executable_mode_allows_missing_install_name_while_rejecting_blpapi(tmp_path):
    clean = tmp_path / "xbbg-mcp"
    clean.write_bytes(make_macho("", ["/usr/lib/libSystem.B.dylib"]))
    linked = tmp_path / "xbbg-mcp-linked"
    linked.write_bytes(make_macho("", ["@rpath/libblpapi3_64.so"]))

    assert check.main(["--executable", str(clean)]) == 0
    assert check.main(["--executable", str(linked)]) == 1
