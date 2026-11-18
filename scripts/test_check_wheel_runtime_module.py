"""Tests for the wheel check that ``xbbg._runtime`` links no Bloomberg runtime."""

from __future__ import annotations

from pathlib import Path
import zipfile

import pytest

from scripts import check_wheel_runtime_module as check
from scripts.test_stage_jvm_natives import make_elf, make_macho, make_pe


def make_wheel(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


CORE_ELF = make_elf(["libblpapi3_64.so", "libc.so.6"])
CLEAN_ELF = make_elf(["libc.so.6", "libgcc_s.so.1"])


@pytest.mark.parametrize(
    ("core_name", "runtime_name", "core", "clean", "linked"),
    [
        (
            "xbbg/_core.cpython-313-x86_64-linux-gnu.so",
            "xbbg/_runtime.cpython-313-x86_64-linux-gnu.so",
            CORE_ELF,
            CLEAN_ELF,
            make_elf(["libblpapi3_64.so"]),
        ),
        (
            "xbbg/_core.cp313-win_amd64.pyd",
            "xbbg/_runtime.cp313-win_amd64.pyd",
            make_pe(["blpapi3_64.dll", "KERNEL32.dll"]),
            make_pe(["KERNEL32.dll"]),
            make_pe(["KERNEL32.dll", "blpapi3_64.dll"]),
        ),
        (
            "xbbg/_core.cpython-313-darwin.so",
            "xbbg/_runtime.cpython-313-darwin.so",
            make_macho("@rpath/_core.so", ["@rpath/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"]),
            make_macho("@rpath/_runtime.so", ["/usr/lib/libSystem.B.dylib"]),
            make_macho("@rpath/_runtime.so", ["@rpath/libblpapi3_64.so"]),
        ),
    ],
    ids=["elf", "pe", "macho"],
)
def test_runtime_module_must_not_reference_the_bloomberg_runtime(
    tmp_path, core_name, runtime_name, core, clean, linked
):
    good = make_wheel(tmp_path / "good.whl", {core_name: core, runtime_name: clean})
    bad = make_wheel(tmp_path / "bad.whl", {core_name: core, runtime_name: linked})

    assert check.check_wheel(good) == []
    [problem] = check.check_wheel(bad)
    assert "must never link the Bloomberg runtime" in problem
    assert "blpapi3_64" in problem


def test_wheel_must_ship_exactly_one_runtime_module(tmp_path):
    core = "xbbg/_core.cpython-313-x86_64-linux-gnu.so"
    runtime = "xbbg/_runtime.cpython-313-x86_64-linux-gnu.so"
    missing = make_wheel(tmp_path / "missing.whl", {core: CORE_ELF})
    doubled = make_wheel(
        tmp_path / "doubled.whl", {core: CORE_ELF, runtime: CLEAN_ELF, "xbbg/_runtime.abi3.so": CLEAN_ELF}
    )
    no_core = make_wheel(tmp_path / "no-core.whl", {runtime: CLEAN_ELF})

    assert "found none" in check.check_wheel(missing)[0]
    assert "exactly one" in check.check_wheel(doubled)[0]
    assert "no xbbg._core" in check.check_wheel(no_core)[0]


def test_unreadable_runtime_module_is_reported(tmp_path):
    wheel = make_wheel(
        tmp_path / "junk.whl",
        {"xbbg/_core.cpython-313-darwin.so": b"\0" * 8, "xbbg/_runtime.cpython-313-darwin.so": b"not an image"},
    )

    [problem] = check.check_wheel(wheel)
    assert "_runtime" in problem
    assert "must never link" not in problem


def test_main_exit_status(tmp_path, capsys):
    core = "xbbg/_core.cpython-313-x86_64-linux-gnu.so"
    runtime = "xbbg/_runtime.cpython-313-x86_64-linux-gnu.so"
    good = make_wheel(tmp_path / "good.whl", {core: CORE_ELF, runtime: CLEAN_ELF})
    bad = make_wheel(tmp_path / "bad.whl", {core: CORE_ELF, runtime: CORE_ELF})

    assert check.main([str(good)]) == 0
    assert check.main([str(good), str(bad)]) == 1
    assert "bad.whl" in capsys.readouterr().err
