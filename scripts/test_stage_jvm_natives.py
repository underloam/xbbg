"""Tests for the JVM natives staging and verification script."""

from __future__ import annotations

import hashlib
from pathlib import Path
import struct
import zipfile

import pytest

from scripts import stage_jvm_natives as natives
from scripts.stage_jvm_natives import (
    PLATFORM_BY_NAME,
    BinaryFormatError,
    StageError,
    StageOptions,
)

WINDOWS = PLATFORM_BY_NAME["windows-x86_64"]
LINUX = PLATFORM_BY_NAME["linux-x86_64"]
MACOS = PLATFORM_BY_NAME["macos-aarch64"]
MARKER = b"Java_io_github_underloam_xbbg_internal_NativeTestSupport_call"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


# ── Synthetic binaries ───────────────────────────────────────────────────────────────────────


def make_pe(imports: list[str], delay_imports: list[str] = (), payload: bytes = b"") -> bytes:
    """A minimal PE32+ DLL whose single section holds the import tables and DLL names."""
    section_rva, section_offset = 0x1000, 0x400
    delay_table = (len(imports) + 1) * 20
    names_start = delay_table + (len(delay_imports) + 1) * 32
    names_blob = bytearray()
    name_rvas = []
    for name in [*imports, *delay_imports]:
        name_rvas.append(section_rva + names_start + len(names_blob))
        names_blob += name.encode() + b"\0"
    body = bytearray(names_start) + names_blob + payload
    for index, rva in enumerate(name_rvas[: len(imports)]):
        struct.pack_into("<IIIII", body, 20 * index, 0x2000, 0, 0, rva, 0x3000)
    for index, rva in enumerate(name_rvas[len(imports) :]):
        struct.pack_into("<IIIIIIII", body, delay_table + 32 * index, 1, rva, 0x4000, 0x5000, 0x6000, 0, 0, 0)
    optional = bytearray(240)
    struct.pack_into("<H", optional, 0, 0x20B)
    struct.pack_into("<I", optional, 108, 16)
    struct.pack_into("<II", optional, 112 + 8, section_rva, (len(imports) + 1) * 20)
    if delay_imports:
        struct.pack_into("<II", optional, 112 + 8 * 13, section_rva + delay_table, (len(delay_imports) + 1) * 32)
    coff = struct.pack("<HHIIIHH", 0x8664, 1, 0, 0, 0, len(optional), 0x2022)
    section = struct.pack(
        "<8sIIIIIIHHI", b".idata", len(body), section_rva, len(body), section_offset, 0, 0, 0, 0, 0xC0000040
    )
    dos = bytearray(0x40)
    dos[:2] = b"MZ"
    struct.pack_into("<I", dos, 0x3C, 0x40)
    image = dos + b"PE\0\0" + coff + optional + section
    return bytes(image + bytes(section_offset - len(image)) + body)


def make_elf(needed: list[str], soname: str | None = None, payload: bytes = b"") -> bytes:
    """A minimal ELF64 shared object with one PT_LOAD and a PT_DYNAMIC segment."""
    base = 0x10000
    strtab = bytearray(b"\0")
    offsets = {}
    for name in [*needed, *([soname] if soname else [])]:
        offsets[name] = len(strtab)
        strtab += name.encode() + b"\0"
    strtab_offset = 64 + 2 * 56
    dynamic_offset = (strtab_offset + len(strtab) + 7) // 8 * 8
    entries = [(1, offsets[name]) for name in needed]
    if soname:
        entries.append((14, offsets[soname]))
    entries += [(5, base + strtab_offset), (0, 0)]
    dynamic = b"".join(struct.pack("<qQ", tag, value) for tag, value in entries)
    size = dynamic_offset + len(dynamic) + len(payload)
    header = bytearray(64)
    header[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HHI", header, 16, 3, 62, 1)
    struct.pack_into("<Q", header, 0x20, 64)
    struct.pack_into("<HH", header, 0x36, 56, 2)
    load = struct.pack("<IIQQQQQQ", 1, 5, 0, base, base, size, size, 0x1000)
    dyn = struct.pack(
        "<IIQQQQQQ", 2, 6, dynamic_offset, base + dynamic_offset, base + dynamic_offset, len(dynamic), len(dynamic), 8
    )
    image = header + load + dyn + strtab
    return bytes(image + bytes(dynamic_offset - len(image)) + dynamic + payload)


def _padded(command: bytes) -> bytes:
    return command + bytes(-len(command) % 8)


def make_macho(install_name: str | None, dylibs: list[str], rpaths: list[str] = (), payload: bytes = b"") -> bytes:
    """A minimal 64-bit Mach-O dylib with ID, dependency and rpath load commands."""
    commands = []
    for command, value in [*([(0xD, install_name)] if install_name else []), *((0xC, name) for name in dylibs)]:
        text = value.encode() + b"\0"
        size = len(_padded(bytes(24) + text))
        commands.append(_padded(struct.pack("<IIIIII", command, size, 24, 2, 0x10000, 0x10000) + text))
    for value in rpaths:
        text = value.encode() + b"\0"
        size = len(_padded(bytes(12) + text))
        commands.append(_padded(struct.pack("<III", 0x8000001C, size, 12) + text))
    blob = b"".join(commands)
    header = struct.pack("<IiiIIIII", 0xFEEDFACF, 0x0100000C, 0, 6, len(commands), len(blob), 0x85, 0)
    return header + blob + payload


def engine_image(platform: natives.Platform, *, marker: bool = False) -> bytes:
    payload = MARKER if marker else b"xbbg engine"
    if platform is WINDOWS:
        return make_pe(["blpapi3_64.dll", "KERNEL32.dll"], payload=payload)
    if platform is LINUX:
        return make_elf(["libblpapi3_64.so", "libc.so.6"], soname="libxbbg_jni.so", payload=payload)
    return make_macho(
        "@rpath/libxbbg_jni.dylib", ["@rpath/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"], payload=payload
    )


def sdk_image(platform: natives.Platform) -> bytes:
    if platform is WINDOWS:
        return make_pe(["KERNEL32.dll", "bcrypt.dll"], payload=b"blpapi3_64.dll appears in wheel lookups")
    if platform is LINUX:
        return make_elf(["libc.so.6"], soname="libxbbg_sdk_jni.so", payload=b"libblpapi3_64.so lookup string")
    return make_macho(
        "@rpath/libxbbg_sdk_jni.dylib",
        ["/usr/lib/libSystem.B.dylib", "/System/Library/Frameworks/Security.framework/Versions/A/Security"],
    )


# ── Binary readers ───────────────────────────────────────────────────────────────────────────


def test_pe_imports_lists_regular_and_delay_loaded_dlls() -> None:
    image = make_pe(["blpapi3_64.dll", "KERNEL32.dll"], ["delayed.dll"])
    assert natives.pe_imports(image) == ["blpapi3_64.dll", "KERNEL32.dll", "delayed.dll"]


def test_pe_imports_rejects_non_pe_and_truncated_images() -> None:
    with pytest.raises(BinaryFormatError, match="no MZ header"):
        natives.pe_imports(b"\x7fELF" + bytes(100))
    with pytest.raises(BinaryFormatError):
        natives.pe_imports(make_pe(["a.dll"])[:0x60])


def test_elf_dynamic_reads_soname_and_needed_entries() -> None:
    image = make_elf(["libblpapi3_64.so", "libc.so.6"], soname="libxbbg_jni.so")
    assert natives.elf_dynamic(image) == ("libxbbg_jni.so", ["libblpapi3_64.so", "libc.so.6"])


def test_elf_dynamic_rejects_static_images() -> None:
    image = bytearray(make_elf([]))
    struct.pack_into("<H", image, 0x38, 1)  # keep only PT_LOAD: no dynamic segment
    with pytest.raises(BinaryFormatError, match="no dynamic segment"):
        natives.elf_dynamic(bytes(image))


def test_macho_load_commands_reads_id_dependencies_and_rpaths() -> None:
    image = make_macho("@rpath/libx.dylib", ["@rpath/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"], ["/build/sdk"])
    assert natives.macho_load_commands(image) == natives.MachO(
        "@rpath/libx.dylib", ("@rpath/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"), ("/build/sdk",)
    )


def test_macho_load_commands_rejects_universal_images() -> None:
    with pytest.raises(BinaryFormatError, match="universal"):
        natives.macho_load_commands(b"\xca\xfe\xba\xbe" + bytes(64))


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("blpapi3_64.dll", True),
        ("BLPAPI3_64.DLL", True),
        ("@rpath/libblpapi3_64.so", True),
        ("/opt/sdk/Darwin/libblpapi3_64.dylib", True),
        ("libblpapi3_64-8926117b.so", True),
        ("libblpapi3.so", True),
        ("libxbbg_jni.so", False),
        ("blpapi_helper.dll", False),
    ],
)
def test_is_blpapi_runtime(reference: str, expected: bool) -> None:
    assert natives.is_blpapi_runtime(reference) is expected


# ── Link checks ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("platform", [WINDOWS, LINUX, MACOS], ids=lambda platform: platform.name)
def test_check_links_accepts_engine_with_one_runtime_reference_and_clean_sdk(platform: natives.Platform) -> None:
    natives.check_links(platform, engine_image(platform), sdk_image(platform))


def test_check_links_requires_the_dynamic_runtime_reference() -> None:
    with pytest.raises(StageError, match="exactly once"):
        natives.check_links(LINUX, make_elf(["libc.so.6"]), sdk_image(LINUX))
    with pytest.raises(StageError, match="exactly once"):
        natives.check_links(LINUX, make_elf(["libblpapi3_64.so.3"]), sdk_image(LINUX))


def test_check_links_rejects_an_sdk_library_that_links_blpapi() -> None:
    with pytest.raises(StageError, match="must never link"):
        natives.check_links(WINDOWS, engine_image(WINDOWS), make_pe(["blpapi3_64.dll"]))


def test_check_links_requires_relocatable_macos_libraries() -> None:
    absolute_sdk = make_macho("@rpath/libxbbg_jni.dylib", ["/runner/vendor/blpapi-sdk/Darwin/libblpapi3_64.so"])
    with pytest.raises(StageError, match="exactly once"):
        natives.check_links(MACOS, absolute_sdk, sdk_image(MACOS))
    build_rpath = make_macho("@rpath/libxbbg_jni.dylib", ["@rpath/libblpapi3_64.so"], ["/Users/runner/sdk"])
    with pytest.raises(StageError, match="non-portable build paths"):
        natives.check_links(MACOS, build_rpath, sdk_image(MACOS))
    absolute_id = make_macho("/Users/runner/target/libxbbg_sdk_jni.dylib", [])
    with pytest.raises(StageError, match="install name"):
        natives.check_links(MACOS, engine_image(MACOS), absolute_id)


def test_check_test_support_marker_matches_the_build_features() -> None:
    natives.check_test_support_marker(
        WINDOWS, engine_image(WINDOWS, marker=True), sdk_image(WINDOWS), test_support=True
    )
    natives.check_test_support_marker(WINDOWS, engine_image(WINDOWS), sdk_image(WINDOWS), test_support=False)
    with pytest.raises(StageError, match="exports no NativeTestSupport"):
        natives.check_test_support_marker(WINDOWS, engine_image(WINDOWS), sdk_image(WINDOWS), test_support=True)
    with pytest.raises(StageError, match="built without test-support"):
        natives.check_test_support_marker(
            WINDOWS, engine_image(WINDOWS, marker=True), sdk_image(WINDOWS), test_support=False
        )
    with pytest.raises(StageError, match=r"xbbg_sdk_jni\.dll contains test-support"):
        natives.check_test_support_marker(WINDOWS, engine_image(WINDOWS), MARKER, test_support=False)


# ── Properties, manifests, small parsers ─────────────────────────────────────────────────────


def test_properties_text_is_sorted_escaped_and_round_trips() -> None:
    values = {"zeta": "last", "abi": "1", "rustc": "rustc 1.98.1 (abc 2026-10-01)", "odd key": " lead\\ü😀"}
    text = natives.properties_text(values)
    lines = text.splitlines()
    assert lines[0].startswith("#")
    assert lines[1:] == [
        "abi=1",
        "odd\\ key=\\ lead\\\\\\u00fc\\ud83d\\ude00",
        "rustc=rustc 1.98.1 (abc 2026-10-01)",
        "zeta=last",
    ]
    assert text.endswith("\n") and "\r" not in text
    assert natives.parse_properties(text) == values


def test_parse_properties_handles_java_syntax() -> None:
    text = "! comment\n  # comment\nspaced   value here\ncolon : v\nkey\\=with=eq\nlong = a\\\n    b\\\n  c\nempty\n"
    assert natives.parse_properties(text) == {
        "spaced": "value here",
        "colon": "v",
        "key=with": "eq",
        "long": "abc",
        "empty": "",
    }


def test_parse_manifest_joins_continuation_lines() -> None:
    text = "Manifest-Version: 1.0\r\nImplementation-Version: 1.2\r\n 3.4\r\n\r\nName: other\r\nX: y\r\n"
    assert natives.parse_manifest(text) == {"Manifest-Version": "1.0", "Implementation-Version": "1.23.4"}


@pytest.mark.parametrize(
    ("package_id", "expected"),
    [
        ("path+file:///work/bindings/jni-xbbg#1.7.0", ("jni-xbbg", "1.7.0")),
        ("path+file:///work/bindings/sdk#jni-xbbg-sdk@1.7.0", ("jni-xbbg-sdk", "1.7.0")),
        ("registry+https://github.com/rust-lang/crates.io-index#tokio@1.48.0", ("tokio", "1.48.0")),
        ("jni-xbbg 1.7.0 (path+file:///work/bindings/jni-xbbg)", ("jni-xbbg", "1.7.0")),
    ],
)
def test_package_name_version(package_id: str, expected: tuple[str, str]) -> None:
    assert natives.package_name_version(package_id) == expected


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ([], (True, "default")),
        (["-C", "target-cpu=x86-64"], (True, "x86-64")),
        (["-Ctarget-cpu=native"], (False, "native")),
        (["--codegen=target-feature=+avx2"], (False, "default")),
        (["-C", "llvm-args=-x"], (False, "default")),
        (["-C", "opt-level=3"], (True, "default")),
    ],
)
def test_inspect_rust_codegen(flags: list[str], expected: tuple[bool, str]) -> None:
    assert natives.inspect_rust_codegen(flags) == expected


def test_qualify_features_assigns_unqualified_names_to_jni_xbbg() -> None:
    assert natives.qualify_features(["test-support", "a, jni-xbbg-sdk/b", " "]) == [
        "jni-xbbg/test-support",
        "jni-xbbg/a",
        "jni-xbbg-sdk/b",
    ]


def test_read_abi_version_requires_one_definition(tmp_path: Path) -> None:
    source = tmp_path / "lib.rs"
    source.write_text("use jni::sys::jint;\npub(crate) const ABI_VERSION: jint = 3;\n", encoding="utf-8")
    assert natives.read_abi_version(source) == 3
    source.write_text("const ABI_VERSION: i32 = 1;\nconst ABI_VERSION: i32 = 2;\n", encoding="utf-8")
    with pytest.raises(StageError, match="exactly once"):
        natives.read_abi_version(source)
    source.write_text("// const ABI_VERSION: jint = 1;\n", encoding="utf-8")
    with pytest.raises(StageError, match="found 0"):
        natives.read_abi_version(source)


def write_sdk_headers(include_dir: Path, parts: tuple[int, ...] = (3, 26, 9, 1)) -> Path:
    include_dir.mkdir(parents=True, exist_ok=True)
    names = ("MAJOR", "MINOR", "PATCH", "BUILD")
    text = "".join(f"#define BLPAPI_VERSION_{name} {value}\n" for name, value in zip(names, parts, strict=True))
    (include_dir / "blpapi_versionmacros.h").write_text(text, encoding="utf-8")
    return include_dir


def test_sdk_header_version(tmp_path: Path) -> None:
    assert natives.sdk_header_version(write_sdk_headers(tmp_path)) == "3.26.9.1"
    (tmp_path / "blpapi_versionmacros.h").write_text("#define BLPAPI_VERSION_MAJOR 3\n", encoding="utf-8")
    with pytest.raises(StageError, match="every BLPAPI_VERSION"):
        natives.sdk_header_version(tmp_path)


# ── Cargo events and staging ─────────────────────────────────────────────────────────────────


ENGINE_ID = "path+file:///work/bindings/jni-xbbg#1.7.0"
SDK_ID = "path+file:///work/bindings/jni-xbbg-sdk#1.7.0"


def cargo_events(
    root: Path,
    platform: natives.Platform,
    *,
    profile: str = "debug",
    build_profile: str | None = None,
    engine_features: tuple[str, ...] = ("default", "live"),
    other_test_support: bool = False,
    rustflags: str = "",
    link: str = "dylib=blpapi3_64",
    marker: bool = False,
    git_commit: str = COMMIT,
) -> list[dict[str, object]]:
    """Build events like Cargo's, with the reported libraries written to disk."""
    output = root / "target" / profile
    output.mkdir(parents=True, exist_ok=True)
    (output / platform.engine).write_bytes(engine_image(platform, marker=marker))
    (output / platform.sdk).write_bytes(sdk_image(platform))
    extra = [str(output / f"{platform.engine}.lib"), str(output / "xbbg_jni.pdb")] if platform is WINDOWS else []

    def artifact(package_id: str, name: str, file: str, features: tuple[str, ...]) -> dict[str, object]:
        return {
            "reason": "compiler-artifact",
            "package_id": package_id,
            "target": {"name": name, "crate_types": ["cdylib"]},
            "features": list(features),
            "filenames": [str(output / file), *extra],
            "fresh": False,
        }

    provenance = {
        "PROFILE": build_profile or ("release" if profile == natives.RELEASE_PROFILE else "debug"),
        "TARGET": platform.target,
        "HOST": platform.target,
        "RUSTFLAGS": rustflags,
        "GIT_COMMIT": git_commit,
        "RUSTC_VERSION": "rustc 1.98.1 (abc 2026-10-01)",
        "OPT_LEVEL": "3",
    }
    events: list[dict[str, object]] = [
        {
            "reason": "build-script-executed",
            "package_id": "path+file:///work/crates/blpapi-sys#xbbg-blpapi-sys@1.7.0",
            "linked_libs": [link],
            "linked_paths": [f"native={root / 'sdk' / 'lib'}"],
            "env": [["XBBG_SDK_INCLUDE_DIR", str(write_sdk_headers(root / "sdk" / "include"))]],
        },
        {
            "reason": "compiler-artifact",
            "package_id": "path+file:///work/crates/xbbg-async#1.7.0",
            "target": {"name": "xbbg_async", "crate_types": ["lib"]},
            "features": ["live", *(["test-support"] if other_test_support else [])],
            "filenames": [],
        },
        {
            "reason": "build-script-executed",
            "package_id": ENGINE_ID,
            "linked_libs": [],
            "linked_paths": [],
            "env": [[f"XBBG_BUILD_{key}", value] for key, value in provenance.items()],
        },
        artifact(ENGINE_ID, "xbbg_jni", platform.engine, engine_features),
        artifact(SDK_ID, "xbbg_sdk_jni", platform.sdk, ()),
        {"reason": "build-finished", "success": True},
    ]
    return events


def test_inspect_build_collects_provenance(tmp_path: Path) -> None:
    facts = natives.inspect_build(cargo_events(tmp_path, WINDOWS), WINDOWS, "debug")
    assert facts.engine.path.name == "xbbg_jni.dll"
    assert facts.sdk.path.name == "xbbg_sdk_jni.dll"
    assert facts.crate_version == "1.7.0"
    assert facts.sdk_headers == "3.26.9.1"
    assert facts.portable is True
    assert facts.test_support_packages == ()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"build_profile": "release"}, "built profile 'release'"),
        ({"link": "static=blpapi3_64"}, "dynamically"),
        ({"link": "dylib=other"}, "SDK link input"),
    ],
)
def test_inspect_build_rejects_inconsistent_events(tmp_path: Path, change: dict[str, str], message: str) -> None:
    with pytest.raises(StageError, match=message):
        natives.inspect_build(cargo_events(tmp_path, WINDOWS, **change), WINDOWS, "debug")


def test_inspect_build_rejects_plain_release_profile_outputs(tmp_path: Path) -> None:
    events = cargo_events(tmp_path, LINUX, profile="release", build_profile="release")
    with pytest.raises(StageError, match="not a 'release-jni' profile output"):
        natives.inspect_build(events, LINUX, natives.RELEASE_PROFILE)


def test_inspect_build_requires_build_provenance_and_single_artifacts(tmp_path: Path) -> None:
    events = cargo_events(tmp_path, WINDOWS)
    without_provenance = [event for event in events if event.get("package_id") != ENGINE_ID or "target" in event]
    with pytest.raises(StageError, match="build provenance is missing"):
        natives.inspect_build(without_provenance, WINDOWS, "debug")
    duplicated = [*events, next(event for event in events if event.get("target", {}).get("name") == "xbbg_jni")]
    with pytest.raises(StageError, match="2 xbbg_jni cdylib artifacts"):
        natives.inspect_build(duplicated, WINDOWS, "debug")
    with pytest.raises(StageError, match=r"expected libxbbg_jni\.so"):
        natives.inspect_build(events, LINUX, "debug")


def test_check_release_refuses_fixtures_and_host_tuning(tmp_path: Path) -> None:
    release = {"profile": natives.RELEASE_PROFILE}
    natives.check_release(natives.inspect_build(cargo_events(tmp_path, LINUX, **release), LINUX, "release-jni"))
    fixtures = natives.inspect_build(
        cargo_events(tmp_path, LINUX, other_test_support=True, **release), LINUX, "release-jni"
    )
    with pytest.raises(StageError, match="xbbg-async"):
        natives.check_release(fixtures)
    tuned = natives.inspect_build(
        cargo_events(tmp_path, LINUX, rustflags="-C\x1ftarget-cpu=native", **release), LINUX, "release-jni"
    )
    with pytest.raises(StageError, match="portable"):
        natives.check_release(tuned)
    unknown = natives.inspect_build(
        cargo_events(tmp_path, LINUX, git_commit="unknown", **release), LINUX, "release-jni"
    )
    with pytest.raises(StageError, match="requires the git commit in the build provenance, found 'unknown'"):
        natives.check_release(unknown)


def options(out: Path, **overrides: object) -> StageOptions:
    values: dict[str, object] = {
        "profile": "debug",
        "release": False,
        "features": (),
        "target": None,
        "out": out,
        "cargo": "cargo",
        "locked": False,
    }
    values.update(overrides)
    return StageOptions(**values)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"release": True, "profile": "debug"}, "--release stages only"),
        ({"release": True, "profile": "release-jni", "features": ("test-support",)}, "refuses test-support"),
        ({"release": True, "profile": "release-jni", "features": ("jni-xbbg/test-support",)}, "refuses test-support"),
        ({"target": "aarch64-unknown-linux-gnu"}, "unsupported target"),
    ],
)
def test_stage_rejects_bad_requests_before_building(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrides: dict[str, object], message: str
) -> None:
    def no_cargo(*_: object) -> None:
        raise AssertionError("cargo must not run")

    monkeypatch.setattr(natives, "run_cargo", no_cargo)
    monkeypatch.setattr(natives, "resolve_cargo", no_cargo)
    with pytest.raises(StageError, match=message):
        natives.stage(options(tmp_path / "out", **overrides))


class FakeCargo:
    """Stands in for Cargo: returns canned build events and records every invocation."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.glibc_checks: list[bool] = []
        self.target = ""
        self.events: list[dict[str, object]] = []

    def use(self, platform: natives.Platform, events: list[dict[str, object]]) -> None:
        self.target = platform.target
        self.events = events

    def run(self, _cargo: str, args: list[str]) -> list[dict[str, object]]:
        self.calls.append(list(args))
        return self.events


@pytest.fixture
def fake_cargo(monkeypatch: pytest.MonkeyPatch) -> FakeCargo:
    fake = FakeCargo()
    monkeypatch.setattr(natives, "resolve_cargo", lambda cargo: cargo)
    monkeypatch.setattr(natives, "host_target", lambda _cargo: fake.target)
    monkeypatch.setattr(natives, "run_cargo", fake.run)
    monkeypatch.setattr(natives, "read_abi_version", lambda: 1)
    monkeypatch.setattr(natives, "check_glibc", lambda _paths, *, release: fake.glibc_checks.append(release))
    return fake


def test_stage_writes_libraries_and_properties(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    fake_cargo.use(WINDOWS, cargo_events(tmp_path / "build", WINDOWS))
    out = tmp_path / "natives"
    (out / "windows-x86_64").mkdir(parents=True)
    (out / "windows-x86_64" / "stale.dll").write_bytes(b"old")

    staged = natives.stage(options(out))

    assert staged == (out / "windows-x86_64").resolve()
    assert sorted(path.name for path in staged.iterdir()) == ["natives.properties", "xbbg_jni.dll", "xbbg_sdk_jni.dll"]
    assert fake_cargo.calls == [
        ["build", "--message-format=json-render-diagnostics", "-p", "jni-xbbg", "-p", "jni-xbbg-sdk"]
    ]
    values = natives.parse_properties((staged / "natives.properties").read_text(encoding="ascii"))
    engine = (staged / "xbbg_jni.dll").read_bytes()
    assert values["xbbg_jni.dll.sha256"] == hashlib.sha256(engine).hexdigest()
    assert values["abi"] == "1"
    assert values["profile"] == "debug"
    assert values["testSupport"] == "false"
    assert values["crateVersion"] == "1.7.0"
    assert values["sdkHeaders"] == "3.26.9.1"
    assert values["xbbg_jni.dll.features"] == "default,live"
    assert values["xbbg_sdk_jni.dll.features"] == ""
    assert fake_cargo.glibc_checks == []
    assert natives.verify([out]) == []
    assert not [path for path in out.iterdir() if path.name.startswith(".")]


def test_stage_release_passes_profile_and_locked(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    fake_cargo.use(LINUX, cargo_events(tmp_path / "build", LINUX, profile=natives.RELEASE_PROFILE))
    out = tmp_path / "natives"

    natives.stage(options(out, release=True, profile=natives.RELEASE_PROFILE, target=LINUX.target))

    assert fake_cargo.calls[0][-5:] == ["--profile", "release-jni", "--target", LINUX.target, "--locked"]
    assert fake_cargo.glibc_checks == [True]
    values = natives.parse_properties((out / "linux-x86_64" / "natives.properties").read_text(encoding="ascii"))
    assert values["release"] == "true"
    assert values["gitCommit"] == COMMIT
    assert natives.verify([out], release=True, platforms=["linux-x86_64"]) == []
    assert natives.verify([out], release=True, platforms=["linux-x86_64"], expect_commit=COMMIT) == []
    other = "f" * 40
    assert natives.verify([out], platforms=["linux-x86_64"], expect_commit=other) == [
        f"{out}: linux-x86_64: natives were built from {COMMIT!r}, expected {other}"
    ]


def test_stage_release_jni_without_release_is_not_releasable(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    """Only stage --release (--locked, release checks, fatal glibc floor) records release=true."""
    fake_cargo.use(LINUX, cargo_events(tmp_path / "build", LINUX, profile=natives.RELEASE_PROFILE))
    out = tmp_path / "natives"

    natives.stage(options(out, profile=natives.RELEASE_PROFILE, target=LINUX.target))

    assert "--locked" not in fake_cargo.calls[0]
    assert fake_cargo.glibc_checks == [False]
    values = natives.parse_properties((out / "linux-x86_64" / "natives.properties").read_text(encoding="ascii"))
    assert values["profile"] == natives.RELEASE_PROFILE
    assert values["testSupport"] == "false"
    assert values["release"] == "false"
    assert natives.verify([out], platforms=["linux-x86_64"]) == []
    assert natives.verify([out], release=True, platforms=["linux-x86_64"]) == [
        f"{out}: linux-x86_64: release natives need release=true, found 'false'"
    ]


def test_stage_release_refuses_unknown_git_commit(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    events = cargo_events(tmp_path / "build", LINUX, profile=natives.RELEASE_PROFILE, git_commit="unknown")
    fake_cargo.use(LINUX, events)
    out = tmp_path / "natives"
    with pytest.raises(StageError, match="found 'unknown'"):
        natives.stage(options(out, release=True, profile=natives.RELEASE_PROFILE, target=LINUX.target))
    assert not out.exists()


def test_stage_release_refuses_fixture_features_reported_by_cargo(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    events = cargo_events(tmp_path / "build", LINUX, profile=natives.RELEASE_PROFILE, other_test_support=True)
    fake_cargo.use(LINUX, events)
    out = tmp_path / "natives"
    with pytest.raises(StageError, match="refuses test-support builds; built with test-support: xbbg-async"):
        natives.stage(options(out, release=True, profile=natives.RELEASE_PROFILE))
    assert not out.exists()


def test_stage_test_support_records_fixtures(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    events = cargo_events(tmp_path / "build", WINDOWS, engine_features=("default", "live", "test-support"), marker=True)
    fake_cargo.use(WINDOWS, events)
    out = tmp_path / "natives"

    natives.stage(options(out, features=("test-support",)))

    assert fake_cargo.calls[0][-2:] == ["--features", "jni-xbbg/test-support"]
    values = natives.parse_properties((out / "windows-x86_64" / "natives.properties").read_text(encoding="ascii"))
    assert values["testSupport"] == "true"
    assert values["release"] == "false"
    assert natives.verify([out]) == []
    problems = natives.verify([out], release=True, platforms=["windows-x86_64"])
    assert any("contains test-support fixtures" in problem for problem in problems)
    assert any("profile=release-jni" in problem for problem in problems)
    assert any("testSupport=false" in problem for problem in problems)


def test_stage_refuses_test_support_build_without_fixture_exports(tmp_path: Path, fake_cargo: FakeCargo) -> None:
    events = cargo_events(tmp_path / "build", WINDOWS, engine_features=("default", "test-support"))
    fake_cargo.use(WINDOWS, events)
    out = tmp_path / "natives"
    with pytest.raises(StageError, match="exports no NativeTestSupport"):
        natives.stage(options(out, features=("test-support",)))
    assert list(out.iterdir()) == []


def test_make_relocatable_rewrites_build_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(natives, "_run_tool", lambda command: commands.append(list(command)) or "")
    library = tmp_path / "libxbbg_jni.dylib"
    library.write_bytes(
        make_macho(
            "/Users/runner/work/target/release-jni/deps/libxbbg_jni.dylib",
            ["/Users/runner/work/vendor/blpapi-sdk/3.26/Darwin/libblpapi3_64.so", "/usr/lib/libSystem.B.dylib"],
            ["/Users/runner/work/vendor/blpapi-sdk/3.26/Darwin", "@loader_path"],
        )
    )

    natives.make_relocatable(library, engine=True)

    assert commands == [
        [
            "install_name_tool",
            "-id",
            "@rpath/libxbbg_jni.dylib",
            "-change",
            "/Users/runner/work/vendor/blpapi-sdk/3.26/Darwin/libblpapi3_64.so",
            "@rpath/libblpapi3_64.so",
            "-delete_rpath",
            "/Users/runner/work/vendor/blpapi-sdk/3.26/Darwin",
            str(library),
        ],
        ["codesign", "--force", "--sign", "-", str(library)],
        ["codesign", "--verify", str(library)],
    ]


# ── Verification ─────────────────────────────────────────────────────────────────────────────


def stage_tree(root: Path, *, release: bool = True, version: str = "1.7.0", **overrides: str) -> Path:
    """A natives directory as ``stage --release`` writes it, for every platform."""
    for platform in natives.PLATFORMS:
        directory = root / platform.name
        directory.mkdir(parents=True)
        values = {
            "abi": "1",
            "crateVersion": version,
            "gitCommit": COMMIT,
            "portable": "true",
            "profile": natives.RELEASE_PROFILE if release else "debug",
            "release": str(release).lower(),
            "testSupport": "false",
            "target": platform.target,
            **overrides,
        }
        for name, data in ((platform.engine, engine_image(platform)), (platform.sdk, sdk_image(platform))):
            (directory / name).write_bytes(data)
            values[f"{name}.sha256"] = hashlib.sha256(data).hexdigest()
        (directory / "natives.properties").write_text(natives.properties_text(values), encoding="ascii")
    return root


def build_jar(path: Path, tree: Path, *, version: str = "1.7.0", extra: dict[str, bytes] | None = None) -> Path:
    with zipfile.ZipFile(path, "w") as jar:
        jar.writestr("META-INF/MANIFEST.MF", f"Manifest-Version: 1.0\r\nImplementation-Version: {version}\r\n\r\n")
        jar.writestr(natives.VERSION_RESOURCE, f"version={version}\n")
        jar.writestr(natives.RESOURCE_ROOT + "/", b"")
        for file in sorted(tree.rglob("*")):
            if file.is_file():
                jar.writestr(f"{natives.RESOURCE_ROOT}/{file.relative_to(tree).as_posix()}", file.read_bytes())
        for name, data in (extra or {}).items():
            jar.writestr(name, data)
    return path


def test_verify_accepts_complete_release_natives_and_jar(tmp_path: Path) -> None:
    tree = stage_tree(tmp_path / "natives")
    jar = build_jar(tmp_path / "xbbg-1.7.0.jar", tree)
    assert natives.verify([tree, jar], release=True, expect_version="1.7.0") == []


def test_verify_reports_tampering_missing_platforms_and_strays(tmp_path: Path) -> None:
    tree = stage_tree(tmp_path / "natives")
    (tree / "linux-x86_64" / "libxbbg_jni.so").write_bytes(b"tampered")
    (tree / "macos-aarch64" / "libxbbg_sdk_jni.dylib").unlink()
    (tree / "windows-x86_64" / "xbbg_jni.pdb").write_bytes(b"debug info")
    problems = natives.verify([tree], release=True)
    assert f"{tree}: linux-x86_64: libxbbg_jni.so does not match its natives.properties sha256" in problems
    assert f"{tree}: macos-aarch64: missing libxbbg_sdk_jni.dylib" in problems
    assert f"{tree}: unexpected natives entry windows-x86_64/xbbg_jni.pdb" in problems

    partial = stage_tree(tmp_path / "partial")
    for platform in ("linux-x86_64", "macos-aarch64"):
        for file in (partial / platform).iterdir():
            file.unlink()
        (partial / platform).rmdir()
    assert natives.verify([partial]) == []
    assert natives.verify([partial], release=True) == [
        f"{partial}: missing natives for linux-x86_64",
        f"{partial}: missing natives for macos-aarch64",
    ]


def test_verify_rejects_debug_natives_and_fixture_markers_for_release(tmp_path: Path) -> None:
    tree = stage_tree(tmp_path / "natives", release=False)
    problems = natives.verify([tree], release=True)
    assert sum("release natives need profile=release-jni" in problem for problem in problems) == 3
    assert sum("release natives need release=true" in problem for problem in problems) == 3

    for key, value in (("portable", "false"), ("release", "false"), ("gitCommit", "unknown")):
        tree = stage_tree(tmp_path / f"bad-{key}", **{key: value})
        problems = natives.verify([tree], release=True)
        assert len(problems) == 3, problems
        assert all(f"found {value!r}" in problem for problem in problems), problems
        assert natives.verify([tree]) == []

    marked = stage_tree(tmp_path / "marked")
    library = marked / "windows-x86_64" / "xbbg_jni.dll"
    library.write_bytes(engine_image(WINDOWS, marker=True))
    properties = marked / "windows-x86_64" / "natives.properties"
    values = natives.parse_properties(properties.read_text(encoding="ascii"))
    values["xbbg_jni.dll.sha256"] = hashlib.sha256(library.read_bytes()).hexdigest()
    properties.write_text(natives.properties_text(values), encoding="ascii")
    assert natives.verify([marked]) == [
        f"{marked}: windows-x86_64: xbbg_jni.dll contains test-support fixtures (NativeTestSupport)"
    ]


def test_verify_checks_versions_and_bloomberg_files_in_jars(tmp_path: Path) -> None:
    tree = stage_tree(tmp_path / "natives")
    jar = build_jar(tmp_path / "xbbg.jar", tree, version="1.6.0", extra={"blpapi/libblpapi3_64.so": b"sdk"})
    problems = natives.verify([jar], release=True, expect_version="1.7.0")
    assert f"{jar}: Implementation-Version '1.6.0', expected 1.7.0" in problems
    assert f"{jar}: {natives.VERSION_RESOURCE} reports '1.6.0', expected 1.7.0" in problems
    assert f"{jar}: Bloomberg SDK file blpapi/libblpapi3_64.so" in problems

    addon = tmp_path / "xbbg-jdk17.jar"
    with zipfile.ZipFile(addon, "w") as archive:
        archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\r\nImplementation-Version: 1.7.0\r\n\r\n")
        archive.writestr("vendor/blpapi-sdk/3.26/include/blpapi_session.h", "")
    assert natives.verify([], [addon], expect_version="1.7.0") == [
        f"{addon}: Bloomberg SDK file vendor/blpapi-sdk/3.26/include/blpapi_session.h"
    ]
    assert natives.verify([], [addon], expect_version="2.0.0")[0] == (
        f"{addon}: Implementation-Version '1.7.0', expected 2.0.0"
    )


def test_verify_reports_empty_and_missing_inputs(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    not_a_jar = tmp_path / "xbbg_jni.dll"
    not_a_jar.write_bytes(b"MZ")
    assert natives.verify([empty, tmp_path / "missing", not_a_jar]) == [
        f"{empty}: contains no natives",
        f"{tmp_path / 'missing'}: does not exist",
        f"{not_a_jar}: is neither a natives directory nor a jar",
    ]


def test_main_verify_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tree = stage_tree(tmp_path / "natives")
    assert natives.main(["verify", "--release", "--natives", str(tree)]) == 0
    assert natives.main(["verify", "--release", "--expect-version", "9.9.9", "--natives", str(tree)]) == 1
    assert "expected 9.9.9" in capsys.readouterr().err
    assert natives.main(["verify"]) == 1
    assert "at least one" in capsys.readouterr().err
