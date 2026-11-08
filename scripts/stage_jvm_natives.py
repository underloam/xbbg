#!/usr/bin/env python3
"""Build, stage and verify the JVM natives bundled in ``io.github.underloam:xbbg``.

``stage`` builds ``jni-xbbg`` (``xbbg_jni``) and ``jni-xbbg-sdk`` (``xbbg_sdk_jni``) with Cargo,
copies both libraries into ``<out>/<platform>/`` under their ``System.mapLibraryName`` names,
makes the macOS libraries relocatable, checks what each library links against, and writes
``natives.properties`` (ABI version, SHA-256 per library, build provenance). ``verify`` checks
staged natives or built jars before they are packaged or published.

Examples, from the repository root::

    python scripts/stage_jvm_natives.py stage                          # host, debug profile
    python scripts/stage_jvm_natives.py stage --features test-support  # test fixtures
    python scripts/stage_jvm_natives.py stage --release                # distributable
    python scripts/stage_jvm_natives.py verify --release --natives java-xbbg/natives
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import struct
import subprocess
import sys
import zipfile

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "java-xbbg" / "natives"
ABI_SOURCE = REPO_ROOT / "bindings" / "jni-xbbg" / "src" / "lib.rs"
GLIBC_CHECK = REPO_ROOT / "scripts" / "check-glibc-max.sh"

RESOURCE_ROOT = "io/github/underloam/xbbg/native"
VERSION_RESOURCE = "io/github/underloam/xbbg/xbbg.properties"
PROPERTIES = "natives.properties"
RELEASE_PROFILE = "release-jni"
PROFILES = ("debug", RELEASE_PROFILE)
TEST_SUPPORT = "test-support"
# Test-support builds export Java_io_github_underloam_xbbg_internal_NativeTestSupport_*; nothing
# else in either library may contain this string, so its presence identifies fixture builds.
TEST_SUPPORT_MARKER = b"NativeTestSupport"
GLIBC_MAX = "2.28"
PORTABLE_CPUS = frozenset({"default", "generic", "x86-64"})
ENGINE_CRATE = "jni-xbbg"

_BLPAPI_RUNTIME = re.compile(r"^(?:lib)?blpapi3(?:_(?:32|64))?(?:[-.][^/\\]*)?$", re.IGNORECASE)
_SDK_LINK = re.compile(r"^(?P<kind>dylib|static)=(?P<stem>blpapi3(?:_(?:32|64))?)$")
_ABI_CONSTANT = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+ABI_VERSION\s*:\s*(?:jni::sys::)?(?:jint|i32)\s*=\s*(\d+)\s*;",
    re.MULTILINE,
)
_PACKAGE_ID_SPEC = re.compile(r"^(?P<source>[^#\s]+)#(?:(?P<name>[^@#]+)@)?(?P<version>[^@#]+)$")
_PACKAGE_ID_LEGACY = re.compile(r"^(?P<name>\S+) (?P<version>\S+) \((?P<source>.+)\)$")
_SDK_VERSION_MACRO = re.compile(r"^\s*#\s*define\s+BLPAPI_VERSION_(MAJOR|MINOR|PATCH|BUILD)\s+(\d+)", re.MULTILINE)
# A full commit id (SHA-1, or SHA-256 object format); rust_build_provenance.rs writes "unknown"
# when git cannot read the checkout (for example a container without safe.directory).
_GIT_COMMIT = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class StageError(Exception):
    """A staging or verification step failed; the message says which and why."""


class BinaryFormatError(StageError):
    """A library is not a well-formed PE, ELF or Mach-O image."""


@dataclass(frozen=True)
class Platform:
    """One bundled platform: resource directory, Rust target and library file names."""

    name: str
    target: str
    engine: str
    sdk: str
    # The single Bloomberg runtime reference xbbg_jni must carry after staging. The Java loader
    # loads that runtime first; the OS then binds this reference to the loaded module (Windows
    # module name, Linux SONAME, macOS install name).
    blpapi: str

    @property
    def libraries(self) -> tuple[str, str]:
        return (self.engine, self.sdk)


PLATFORMS = (
    Platform("windows-x86_64", "x86_64-pc-windows-msvc", "xbbg_jni.dll", "xbbg_sdk_jni.dll", "blpapi3_64.dll"),
    Platform("linux-x86_64", "x86_64-unknown-linux-gnu", "libxbbg_jni.so", "libxbbg_sdk_jni.so", "libblpapi3_64.so"),
    Platform(
        "macos-aarch64",
        "aarch64-apple-darwin",
        "libxbbg_jni.dylib",
        "libxbbg_sdk_jni.dylib",
        "@rpath/libblpapi3_64.so",
    ),
)
PLATFORM_BY_NAME = {platform.name: platform for platform in PLATFORMS}
PLATFORM_BY_TARGET = {platform.target: platform for platform in PLATFORMS}


# ── Binary readers ────────────────────────────────────────────────────────────────────────────


def _c_string(data: bytes, offset: int, end: int | None = None) -> str:
    limit = len(data) if end is None else min(end, len(data))
    if not 0 <= offset < limit:
        raise BinaryFormatError(f"string offset {offset:#x} is outside the image")
    stop = data.find(b"\0", offset, limit)
    return data[offset : limit if stop < 0 else stop].decode("utf-8", "replace")


def pe_imports(data: bytes) -> list[str]:
    """Names of the DLLs a PE image imports, delay-loaded ones included, in table order."""
    try:
        if data[:2] != b"MZ":
            raise BinaryFormatError("not a PE image (no MZ header)")
        header = struct.unpack_from("<I", data, 0x3C)[0]
        if data[header : header + 4] != b"PE\0\0":
            raise BinaryFormatError("not a PE image (no PE signature)")
        _, section_count, _, _, _, optional_size, _ = struct.unpack_from("<HHIIIHH", data, header + 4)
        optional = header + 24
        magic = struct.unpack_from("<H", data, optional)[0]
        directories = {0x10B: optional + 96, 0x20B: optional + 112}.get(magic)
        if directories is None:
            raise BinaryFormatError(f"unknown PE optional header magic {magic:#x}")
        directory_count = struct.unpack_from("<I", data, directories - 4)[0]
        sections = [
            struct.unpack_from("<8xIIII", data, optional + optional_size + 40 * index) for index in range(section_count)
        ]

        def file_offset(rva: int) -> int:
            for virtual_size, address, raw_size, raw_pointer in sections:
                if address <= rva < address + max(virtual_size, raw_size):
                    return rva - address + raw_pointer
            raise BinaryFormatError(f"RVA {rva:#x} is outside every PE section")

        names: list[str] = []
        # (data directory, descriptor size, offset of the DLL name RVA): imports, delay imports.
        for directory, entry_size, name_field in ((1, 20, 12), (13, 32, 4)):
            if directory_count <= directory:
                continue
            rva, size = struct.unpack_from("<II", data, directories + 8 * directory)
            if rva == 0 or size == 0:
                continue
            position = file_offset(rva)
            while True:
                entry = data[position : position + entry_size]
                if len(entry) < entry_size:
                    raise BinaryFormatError("truncated PE import table")
                if entry == bytes(entry_size):
                    break
                names.append(_c_string(data, file_offset(struct.unpack_from("<I", entry, name_field)[0])))
                position += entry_size
        return names
    except struct.error as error:
        raise BinaryFormatError(f"truncated PE image: {error}") from error


def elf_dynamic(data: bytes) -> tuple[str | None, list[str]]:
    """``(DT_SONAME, [DT_NEEDED...])`` of an ELF shared object."""
    try:
        if data[:4] != b"\x7fELF":
            raise BinaryFormatError("not an ELF image")
        wide = {1: False, 2: True}.get(data[4])
        order = {1: "<", 2: ">"}.get(data[5])
        if wide is None or order is None:
            raise BinaryFormatError("unknown ELF class or byte order")
        if wide:
            program_offset = struct.unpack_from(order + "Q", data, 0x20)[0]
            entry_size, entry_count = struct.unpack_from(order + "HH", data, 0x36)
            segment_format, dynamic_format = order + "IIQQQQQQ", order + "qQ"
        else:
            program_offset = struct.unpack_from(order + "I", data, 0x1C)[0]
            entry_size, entry_count = struct.unpack_from(order + "HH", data, 0x2A)
            segment_format, dynamic_format = order + "IIIIIIII", order + "iI"
        loads: list[tuple[int, int, int]] = []
        dynamic: tuple[int, int] | None = None
        for index in range(entry_count):
            fields = struct.unpack_from(segment_format, data, program_offset + index * entry_size)
            if wide:
                kind, _, offset, address, _, file_size, _, _ = fields
            else:
                kind, offset, address, _, file_size, _, _, _ = fields
            if kind == 1:  # PT_LOAD
                loads.append((address, offset, file_size))
            elif kind == 2:  # PT_DYNAMIC
                dynamic = (offset, file_size)
        if dynamic is None:
            raise BinaryFormatError("ELF image has no dynamic segment")
        step = struct.calcsize(dynamic_format)
        tags: list[tuple[int, int]] = []
        for position in range(dynamic[0], dynamic[0] + dynamic[1] - step + 1, step):
            tag, value = struct.unpack_from(dynamic_format, data, position)
            if tag == 0:  # DT_NULL
                break
            tags.append((tag, value))
        strtab = next((value for tag, value in tags if tag == 5), None)  # DT_STRTAB
        if strtab is None:
            raise BinaryFormatError("ELF dynamic segment has no string table")
        base = next(
            (offset + strtab - address for address, offset, size in loads if address <= strtab < address + size), None
        )
        if base is None:
            raise BinaryFormatError("ELF string table is outside every loadable segment")
        needed = [_c_string(data, base + value) for tag, value in tags if tag == 1]  # DT_NEEDED
        soname = next((_c_string(data, base + value) for tag, value in tags if tag == 14), None)  # DT_SONAME
        return soname, needed
    except struct.error as error:
        raise BinaryFormatError(f"truncated ELF image: {error}") from error


_LC_REQ_DYLD = 0x80000000
_LC_ID_DYLIB = 0xD
_LC_RPATH = 0x1C | _LC_REQ_DYLD
# LC_LOAD_DYLIB, LC_LOAD_WEAK_DYLIB, LC_REEXPORT_DYLIB, LC_LAZY_LOAD_DYLIB, LC_LOAD_UPWARD_DYLIB
_LC_DYLIBS = frozenset({0xC, 0x18 | _LC_REQ_DYLD, 0x1F | _LC_REQ_DYLD, 0x20, 0x23 | _LC_REQ_DYLD})
_LC_STRINGS = _LC_DYLIBS | {_LC_ID_DYLIB, _LC_RPATH}


@dataclass(frozen=True)
class MachO:
    """The load commands that decide where dyld looks for an image and its dependencies."""

    install_name: str | None
    dylibs: tuple[str, ...]
    rpaths: tuple[str, ...]


def macho_load_commands(data: bytes) -> MachO:
    """Install name, dependent libraries and rpaths of a thin little-endian Mach-O image."""
    try:
        if data[:4] in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf"):
            raise BinaryFormatError("universal Mach-O images are not supported; build for a single target")
        header_size = {0xFEEDFACF: 32, 0xFEEDFACE: 28}.get(struct.unpack_from("<I", data, 0)[0])
        if header_size is None:
            raise BinaryFormatError("not a little-endian Mach-O image")
        command_count = struct.unpack_from("<I", data, 16)[0]
        position = header_size
        install_name: str | None = None
        dylibs: list[str] = []
        rpaths: list[str] = []
        for _ in range(command_count):
            command, size = struct.unpack_from("<II", data, position)
            if size < 8:
                raise BinaryFormatError("malformed Mach-O load command")
            if command in _LC_STRINGS:
                value = _c_string(data, position + struct.unpack_from("<I", data, position + 8)[0], position + size)
                if command == _LC_ID_DYLIB:
                    install_name = value
                elif command == _LC_RPATH:
                    rpaths.append(value)
                else:
                    dylibs.append(value)
            position += size
        return MachO(install_name, tuple(dylibs), tuple(rpaths))
    except struct.error as error:
        raise BinaryFormatError(f"truncated Mach-O image: {error}") from error


def runtime_dependencies(platform: Platform, data: bytes) -> list[str]:
    """Libraries the dynamic loader must resolve before ``data`` can run."""
    if platform.name.startswith("windows-"):
        return pe_imports(data)
    if platform.name.startswith("linux-"):
        return elf_dynamic(data)[1]
    return list(macho_load_commands(data).dylibs)


def is_blpapi_runtime(reference: str) -> bool:
    """Whether a dependency reference names a Bloomberg runtime library."""
    return _BLPAPI_RUNTIME.match(PurePosixPath(reference.replace("\\", "/")).name) is not None


def _is_non_portable_darwin_path(value: str) -> bool:
    return value.startswith("/") and not value.startswith(("/usr/lib/", "/System/Library/"))


def check_links(platform: Platform, engine: bytes, sdk: bytes) -> None:
    """Fail unless xbbg_jni references exactly the expected Bloomberg runtime and xbbg_sdk_jni none."""
    engine_deps = runtime_dependencies(platform, engine)
    blpapi = [dep for dep in engine_deps if is_blpapi_runtime(dep)]
    expected = platform.blpapi.lower() if platform.name.startswith("windows-") else platform.blpapi
    found = [dep.lower() for dep in blpapi] if platform.name.startswith("windows-") else blpapi
    if found != [expected]:
        raise StageError(
            f"{platform.engine} must reference the Bloomberg runtime exactly once as {platform.blpapi!r} "
            f"(dynamic link); it references {blpapi or 'none'} among {engine_deps}"
        )
    sdk_blpapi = [dep for dep in runtime_dependencies(platform, sdk) if is_blpapi_runtime(dep)]
    if sdk_blpapi:
        raise StageError(f"{platform.sdk} must never link the Bloomberg runtime; it references {sdk_blpapi}")
    if platform.name.startswith("macos-"):
        for name, data in ((platform.engine, engine), (platform.sdk, sdk)):
            image = macho_load_commands(data)
            if image.install_name != f"@rpath/{name}":
                raise StageError(f"{name} has install name {image.install_name!r}, expected '@rpath/{name}'")
            absolute = [value for value in (*image.dylibs, *image.rpaths) if _is_non_portable_darwin_path(value)]
            if absolute:
                raise StageError(f"{name} contains non-portable build paths: {', '.join(absolute)}")


# ── Properties and manifests ─────────────────────────────────────────────────────────────────


def _escape_properties(text: str, *, key: bool) -> str:
    escaped: list[str] = []
    for index, char in enumerate(text):
        code = ord(char)
        if char == "\\":
            escaped.append("\\\\")
        elif char in "\t\n\r\f":
            escaped.append({"\t": "\\t", "\n": "\\n", "\r": "\\r", "\f": "\\f"}[char])
        elif code < 0x20 or code > 0x7E:
            units = char.encode("utf-16-be")
            escaped.extend(f"\\u{units[i] << 8 | units[i + 1]:04x}" for i in range(0, len(units), 2))
        elif (key and char in " =:") or (index == 0 and char in " #!"):
            escaped.append("\\" + char)
        else:
            escaped.append(char)
    return "".join(escaped)


def properties_text(values: Mapping[str, str]) -> str:
    """Deterministic ``java.util.Properties`` text: sorted keys, no timestamp, LF line ends."""
    lines = ["# Written by scripts/stage_jvm_natives.py; do not edit."]
    lines += [
        f"{_escape_properties(key, key=True)}={_escape_properties(value, key=False)}"
        for key, value in sorted(values.items())
    ]
    return "\n".join(lines) + "\n"


_PROPERTY_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}


def _unescape_properties(text: str) -> str:
    result: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char != "\\" or index + 1 == len(text):
            result.append(char)
            index += 1
            continue
        escaped = text[index + 1]
        if escaped == "u":
            digits = text[index + 2 : index + 6]
            if len(digits) != 4 or any(c not in "0123456789abcdefABCDEF" for c in digits):
                raise StageError(f"malformed \\u escape in properties text: {text!r}")
            result.append(chr(int(digits, 16)))
            index += 6
            continue
        result.append(_PROPERTY_ESCAPES.get(escaped, escaped))
        index += 2
    # Join UTF-16 surrogate pairs written as two \u escapes.
    return "".join(result).encode("utf-16-be", "surrogatepass").decode("utf-16-be")


def _continues(line: str) -> bool:
    return (len(line) - len(line.rstrip("\\"))) % 2 == 1


def _split_property(line: str) -> tuple[str, str]:
    index = 0
    while index < len(line):
        char = line[index]
        if char == "\\":
            index += 2
            continue
        if char in "=: \t\f":
            break
        index += 1
    key, rest = line[:index], line[index:]
    if rest[:1] in (" ", "\t", "\f"):
        rest = rest.lstrip(" \t\f")
        if rest[:1] in ("=", ":"):
            rest = rest[1:]
    elif rest[:1] in ("=", ":"):
        rest = rest[1:]
    return key, rest.lstrip(" \t\f")


def parse_properties(text: str) -> dict[str, str]:
    """Parse ``java.util.Properties`` text (comments, separators, escapes, continuation lines)."""
    values: dict[str, str] = {}
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index].lstrip(" \t\f")
        index += 1
        if not line or line[0] in "#!":
            continue
        while _continues(line):
            line = line[:-1]
            if index == len(lines):
                break
            line += lines[index].lstrip(" \t\f")
            index += 1
        key, value = _split_property(line)
        values[_unescape_properties(key)] = _unescape_properties(value)
    return values


def parse_manifest(text: str) -> dict[str, str]:
    """Main section of a jar manifest, with continuation lines joined."""
    attributes: dict[str, str] = {}
    last: str | None = None
    for line in text.splitlines():
        if not line:
            break
        if line.startswith(" ") and last is not None:
            attributes[last] += line[1:]
            continue
        name, separator, value = line.partition(": ")
        if separator:
            attributes[name] = value
            last = name
    return attributes


# ── Cargo build facts ────────────────────────────────────────────────────────────────────────


def package_name_version(package_id: str) -> tuple[str, str]:
    """``(name, version)`` from a Cargo package ID (spec or legacy form)."""
    match = _PACKAGE_ID_SPEC.match(package_id)
    if match is not None:
        name = match["name"] or match["source"].rstrip("/").rsplit("/", 1)[-1]
        return name, match["version"]
    match = _PACKAGE_ID_LEGACY.match(package_id)
    if match is not None:
        return match["name"], match["version"]
    raise StageError(f"unrecognized Cargo package id {package_id!r}")


def inspect_rust_codegen(flags: Sequence[str]) -> tuple[bool, str]:
    """``(portable, target_cpu)`` for rustc flags, with the JS packaging policy."""
    target_cpu = "default"
    custom = False
    index = 0
    while index < len(flags):
        flag = flags[index]
        option: str | None = None
        if flag in ("-C", "--codegen"):
            index += 1
            option = flags[index] if index < len(flags) else None
        elif flag.startswith("--codegen="):
            option = flag[len("--codegen=") :].lstrip()
        elif flag.startswith("-C"):
            option = flag[2:].lstrip()
        if option is not None:
            if option.startswith("target-cpu="):
                target_cpu = option[len("target-cpu=") :]
            elif option.startswith(("target-feature=", "llvm-args=")):
                # Explicit feature or LLVM tuning is a local build by definition.
                custom = True
        index += 1
    return not custom and target_cpu in PORTABLE_CPUS, target_cpu


def qualify_features(values: Iterable[str]) -> list[str]:
    """Split comma/space lists; unqualified names belong to jni-xbbg."""
    features: list[str] = []
    for value in values:
        for feature in re.split(r"[,\s]+", value.strip()):
            if feature:
                features.append(feature if "/" in feature else f"{ENGINE_CRATE}/{feature}")
    return features


def read_abi_version(source: Path = ABI_SOURCE) -> int:
    """The ``ABI_VERSION`` constant xbbg_jni returns from ``NativeXbbg.abiVersion()``."""
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as error:
        raise StageError(f"cannot read the JNI ABI version from {source}: {error}") from error
    values = _ABI_CONSTANT.findall(text)
    if len(values) != 1:
        raise StageError(f"{source} must define `const ABI_VERSION: jint = <n>;` exactly once (found {len(values)})")
    return int(values[0])


def sdk_header_version(include_dir: Path) -> str:
    """``major.minor.patch.build`` from the Bloomberg headers the build compiled against."""
    header = include_dir / "blpapi_versionmacros.h"
    try:
        parts = dict(_SDK_VERSION_MACRO.findall(header.read_text(encoding="utf-8", errors="replace")))
    except OSError as error:
        raise StageError(f"cannot read the Bloomberg SDK version from {header}: {error}") from error
    if set(parts) != {"MAJOR", "MINOR", "PATCH", "BUILD"}:
        raise StageError(f"{header} does not define every BLPAPI_VERSION_* macro")
    return ".".join(parts[key] for key in ("MAJOR", "MINOR", "PATCH", "BUILD"))


@dataclass(frozen=True)
class Artifact:
    """A cdylib Cargo reported, with the features it was compiled with."""

    package_id: str
    path: Path
    features: tuple[str, ...]


@dataclass(frozen=True)
class BuildFacts:
    """Everything the build events say about one staging build."""

    engine: Artifact
    sdk: Artifact
    crate_version: str
    git_commit: str
    rustc: str
    portable: bool
    target_cpu: str
    sdk_headers: str
    test_support_packages: tuple[str, ...]


def cdylib_artifact(events: Sequence[Mapping[str, object]], target_name: str) -> Artifact:
    """The single loadable library Cargo built for ``target_name``."""
    matches = [
        event
        for event in events
        if event.get("reason") == "compiler-artifact"
        and isinstance(event.get("target"), dict)
        and event["target"].get("name") == target_name
        and "cdylib" in event["target"].get("crate_types", [])
    ]
    if len(matches) != 1:
        raise StageError(f"Cargo reported {len(matches)} {target_name} cdylib artifacts; expected exactly one")
    event = matches[0]
    files = [Path(name) for name in event.get("filenames", []) if Path(name).suffix in (".dll", ".so", ".dylib")]
    if len(files) != 1:
        raise StageError(f"Cargo did not report exactly one loadable {target_name} library: {event.get('filenames')}")
    return Artifact(str(event.get("package_id")), files[0], tuple(event.get("features", [])))


def build_script_env(events: Sequence[Mapping[str, object]], package_id: str) -> dict[str, str]:
    """``cargo:rustc-env`` values the build script of ``package_id`` emitted."""
    scripts = [
        event
        for event in events
        if event.get("reason") == "build-script-executed" and event.get("package_id") == package_id
    ]
    if len(scripts) != 1:
        raise StageError(f"Cargo did not report the build script of {package_id} (build provenance is missing)")
    return {str(key): str(value) for key, value in scripts[0].get("env", [])}


def _sdk_link(events: Sequence[Mapping[str, object]]) -> tuple[str, str, dict[str, str]]:
    for event in events:
        if event.get("reason") != "build-script-executed":
            continue
        for library in event.get("linked_libs", []):
            match = _SDK_LINK.match(str(library))
            if match is not None:
                env = {str(key): str(value) for key, value in event.get("env", [])}
                return match["kind"], match["stem"], env
    raise StageError("Cargo did not report the Bloomberg SDK link input (xbbg-blpapi-sys build script)")


def inspect_build(events: Sequence[Mapping[str, object]], platform: Platform, profile: str) -> BuildFacts:
    """Check the build events against the requested platform and profile and collect provenance."""
    engine = cdylib_artifact(events, "xbbg_jni")
    sdk = cdylib_artifact(events, "xbbg_sdk_jni")
    for artifact, expected in ((engine, platform.engine), (sdk, platform.sdk)):
        if artifact.path.name != expected:
            raise StageError(f"Cargo built {artifact.path.name}, expected {expected} for {platform.name}")
        if artifact.path.parent.name != profile:
            raise StageError(f"{artifact.path} is not a {profile!r} profile output")
    provenance = build_script_env(events, engine.package_id)

    def fact(key: str) -> str:
        value = provenance.get(f"XBBG_BUILD_{key}")
        if value is None:
            raise StageError(f"jni-xbbg build provenance is missing XBBG_BUILD_{key}")
        return value

    expected_profile = "release" if profile == RELEASE_PROFILE else "debug"
    if fact("PROFILE") != expected_profile:
        raise StageError(f"Cargo built profile {fact('PROFILE')!r}, not the requested {profile!r}")
    if fact("TARGET") != platform.target:
        raise StageError(f"Cargo built for {fact('TARGET')}, expected {platform.target}")
    portable, target_cpu = inspect_rust_codegen([flag for flag in fact("RUSTFLAGS").split("\x1f") if flag])
    kind, stem, sdk_env = _sdk_link(events)
    if (kind, stem) != ("dylib", "blpapi3_64"):
        raise StageError(f"xbbg_jni must link blpapi3_64 dynamically; Cargo reported {kind}={stem}")
    include_dir = sdk_env.get("XBBG_SDK_INCLUDE_DIR", "")
    if not include_dir:
        raise StageError("Cargo did not report the Bloomberg SDK headers used by the build")
    engine_version = package_name_version(engine.package_id)[1]
    sdk_version = package_name_version(sdk.package_id)[1]
    if engine_version != sdk_version:
        raise StageError(f"jni-xbbg {engine_version} and jni-xbbg-sdk {sdk_version} must share the workspace version")
    test_support = sorted(
        {
            package_name_version(str(event.get("package_id")))[0]
            for event in events
            if event.get("reason") == "compiler-artifact" and TEST_SUPPORT in event.get("features", [])
        }
    )
    return BuildFacts(
        engine=engine,
        sdk=sdk,
        crate_version=engine_version,
        git_commit=fact("GIT_COMMIT"),
        rustc=fact("RUSTC_VERSION"),
        portable=portable,
        target_cpu=target_cpu,
        sdk_headers=sdk_header_version(Path(include_dir)),
        test_support_packages=tuple(test_support),
    )


def check_release(facts: BuildFacts) -> None:
    """Refuse builds that must never ship: fixtures, host-tuned code generation, unknown commits."""
    if facts.test_support_packages:
        raise StageError(
            "release staging refuses test-support builds; built with test-support: "
            + ", ".join(facts.test_support_packages)
        )
    if not facts.portable:
        raise StageError(
            f"release staging requires portable code generation (target-cpu {facts.target_cpu!r}, "
            "no target-feature/llvm-args in RUSTFLAGS)"
        )
    if _GIT_COMMIT.match(facts.git_commit) is None:
        raise StageError(
            f"release staging requires the git commit in the build provenance, found {facts.git_commit!r} "
            "(is the checkout a git repository git trusts? see git config safe.directory)"
        )


def check_test_support_marker(platform: Platform, engine: bytes, sdk: bytes, *, test_support: bool) -> None:
    """The fixture marker is in xbbg_jni exactly when it was built with test-support, never in xbbg_sdk_jni."""
    if TEST_SUPPORT_MARKER in sdk:
        raise StageError(f"{platform.sdk} contains test-support fixtures ({TEST_SUPPORT_MARKER.decode()})")
    present = TEST_SUPPORT_MARKER in engine
    if test_support and not present:
        raise StageError(
            f"{platform.engine} was built with test-support but exports no {TEST_SUPPORT_MARKER.decode()} symbols; "
            "the release guards depend on that marker"
        )
    if present and not test_support:
        raise StageError(
            f"{platform.engine} contains {TEST_SUPPORT_MARKER.decode()} but was built without test-support"
        )


# ── Staging ──────────────────────────────────────────────────────────────────────────────────


def _run_tool(command: Sequence[str]) -> str:
    try:
        result = subprocess.run(list(command), capture_output=True, text=True, check=False)
    except OSError as error:
        raise StageError(f"cannot run {command[0]}: {error}") from error
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        raise StageError(f"{' '.join(command)} failed ({result.returncode}): {output}")
    return result.stdout


def resolve_cargo(cargo: str) -> str:
    resolved = shutil.which(cargo)
    if resolved is None:
        raise StageError(f"cannot find {cargo!r} on PATH")
    return resolved


def host_target(cargo: str) -> str:
    """The host target triple of the Cargo toolchain."""
    for line in _run_tool([cargo, "-vV"]).splitlines():
        if line.startswith("host: "):
            return line[len("host: ") :].strip()
    raise StageError(f"{cargo} -vV did not report a host target")


def cargo_args(profile: str, features: Sequence[str], target: str | None, *, locked: bool) -> list[str]:
    args = ["build", "--message-format=json-render-diagnostics", "-p", ENGINE_CRATE, "-p", "jni-xbbg-sdk"]
    if profile == RELEASE_PROFILE:
        args += ["--profile", RELEASE_PROFILE]
    if target is not None:
        args += ["--target", target]
    if features:
        args += ["--features", ",".join(features)]
    if locked:
        args.append("--locked")
    return args


def run_cargo(cargo: str, args: Sequence[str]) -> list[dict[str, object]]:
    """Run Cargo with JSON messages on stdout and return the build events."""
    print(f"+ {cargo} {' '.join(args)}", file=sys.stderr, flush=True)
    process = subprocess.Popen(
        [cargo, *args], cwd=REPO_ROOT, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace"
    )
    assert process.stdout is not None
    events: list[dict[str, object]] = []
    for raw in process.stdout:
        line = raw.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            process.kill()
            raise StageError(f"Cargo emitted a non-JSON line on stdout: {line[:200]!r}") from error
        if not isinstance(event, dict):
            process.kill()
            raise StageError(f"Cargo emitted an unexpected message: {line[:200]!r}")
        message = event.get("message")
        if isinstance(message, dict) and isinstance(message.get("rendered"), str):
            sys.stderr.write(message["rendered"])
        events.append(event)
    status = process.wait()
    if status != 0:
        raise StageError(f"cargo build failed with exit status {status}")
    return events


def make_relocatable(path: Path, *, engine: bool) -> None:
    """Rewrite Mach-O load commands so the library carries no build-machine paths."""
    image = macho_load_commands(path.read_bytes())
    command = ["install_name_tool", "-id", f"@rpath/{path.name}"]
    if engine:
        for dylib in image.dylibs:
            if is_blpapi_runtime(dylib) and dylib != "@rpath/libblpapi3_64.so":
                command += ["-change", dylib, "@rpath/libblpapi3_64.so"]
    for rpath in image.rpaths:
        if _is_non_portable_darwin_path(rpath):
            command += ["-delete_rpath", rpath]
    _run_tool([*command, str(path)])
    # install_name_tool invalidates the linker's ad-hoc signature; arm64 refuses unsigned code.
    _run_tool(["codesign", "--force", "--sign", "-", str(path)])
    _run_tool(["codesign", "--verify", str(path)])


def check_glibc(paths: Sequence[Path], *, release: bool) -> None:
    """Enforce the manylinux_2_28 floor (scripts/check-glibc-max.sh); fatal for release staging."""
    bash = shutil.which("bash")
    if bash is None:
        problem = "bash is required for the glibc floor check"
        if release:
            raise StageError(problem)
        print(f"warning: {problem}; skipped", file=sys.stderr)
        return
    result = subprocess.run([bash, str(GLIBC_CHECK), GLIBC_MAX, *map(str, paths)], check=False)
    if result.returncode != 0:
        problem = f"the libraries need a glibc newer than {GLIBC_MAX} (or the check could not run)"
        if release:
            raise StageError(problem)
        print(f"warning: {problem}; release staging refuses them", file=sys.stderr)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def native_properties(
    abi: int, platform: Platform, profile: str, facts: BuildFacts, digests: Mapping[str, str], *, release: bool
) -> dict[str, str]:
    """The ``natives.properties`` values the loader and the release guards read.

    ``release`` is true only for ``stage --release`` builds that passed every release check
    (``--locked``, :func:`check_release`, the fatal glibc floor); the release guards require it.
    """
    values = {
        "abi": str(abi),
        "crateVersion": facts.crate_version,
        "gitCommit": facts.git_commit,
        "portable": str(facts.portable).lower(),
        "profile": profile,
        "release": str(release).lower(),
        "rustc": facts.rustc,
        "sdkHeaders": facts.sdk_headers,
        "target": platform.target,
        "testSupport": str(bool(facts.test_support_packages)).lower(),
    }
    for name, artifact in ((platform.engine, facts.engine), (platform.sdk, facts.sdk)):
        values[f"{name}.features"] = ",".join(sorted(artifact.features))
        values[f"{name}.sha256"] = digests[name]
    return values


def _replace_directory(staging: Path, final: Path) -> None:
    backup = final.with_name(f".{final.name}.old-{os.getpid()}")
    if final.exists():
        final.rename(backup)
    try:
        staging.rename(final)
    except OSError:
        if backup.exists():
            backup.rename(final)
        raise
    shutil.rmtree(backup, ignore_errors=True)


@dataclass(frozen=True)
class StageOptions:
    """Command-line choices for ``stage``."""

    profile: str
    release: bool
    features: tuple[str, ...]
    target: str | None
    out: Path
    cargo: str
    locked: bool


def validate_stage_options(options: StageOptions) -> list[str]:
    """Reject inconsistent requests before anything is built; returns the qualified features."""
    if options.profile not in PROFILES:
        raise StageError(f"unknown profile {options.profile!r}; choose one of {', '.join(PROFILES)}")
    features = qualify_features(options.features)
    if options.release:
        if options.profile != RELEASE_PROFILE:
            raise StageError(f"--release stages only --profile {RELEASE_PROFILE} builds")
        fixtures = [feature for feature in features if feature.rsplit("/", 1)[-1] == TEST_SUPPORT]
        if fixtures:
            raise StageError(f"--release refuses test-support builds: {', '.join(fixtures)}")
    if options.target is not None and options.target not in PLATFORM_BY_TARGET:
        raise StageError(f"unsupported target {options.target}; supported: {', '.join(PLATFORM_BY_TARGET)}")
    return features


def stage(options: StageOptions) -> Path:
    """Build both JNI crates and stage them into ``<out>/<platform>``; returns that directory."""
    features = validate_stage_options(options)
    abi = read_abi_version()
    cargo = resolve_cargo(options.cargo)
    target = options.target or host_target(cargo)
    platform = PLATFORM_BY_TARGET.get(target)
    if platform is None:
        raise StageError(f"host target {target} is not a bundled platform ({', '.join(PLATFORM_BY_TARGET)})")
    locked = options.locked or options.release
    events = run_cargo(cargo, cargo_args(options.profile, features, options.target, locked=locked))
    facts = inspect_build(events, platform, options.profile)
    if options.release:
        check_release(facts)

    out = options.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    final = out / platform.name
    staging = out / f".{platform.name}.staging-{os.getpid()}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir()
    try:
        for artifact, name in ((facts.engine, platform.engine), (facts.sdk, platform.sdk)):
            shutil.copyfile(artifact.path, staging / name)
            (staging / name).chmod(0o755)
            if platform.name.startswith("macos-"):
                make_relocatable(staging / name, engine=name == platform.engine)
        engine = (staging / platform.engine).read_bytes()
        sdk = (staging / platform.sdk).read_bytes()
        check_links(platform, engine, sdk)
        check_test_support_marker(platform, engine, sdk, test_support=TEST_SUPPORT in facts.engine.features)
        if platform.name.startswith("linux-"):
            check_glibc([staging / platform.engine, staging / platform.sdk], release=options.release)
        digests = {platform.engine: sha256_bytes(engine), platform.sdk: sha256_bytes(sdk)}
        values = native_properties(abi, platform, options.profile, facts, digests, release=options.release)
        (staging / PROPERTIES).write_bytes(properties_text(values).encode("ascii"))
        _replace_directory(staging, final)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    print(f"staged {platform.name} ({options.profile}, features: {','.join(features) or 'default'}) into {final}")
    for name in platform.libraries:
        print(f"  {name} sha256={digests[name]}")
    return final


# ── Verification ─────────────────────────────────────────────────────────────────────────────


def is_bloomberg_sdk_file(path_name: str) -> bool:
    """Bloomberg's runtime is never redistributed (same rule as the PyPI/npm payload checks)."""
    normalized = path_name.replace("\\", "/").lower()
    basename = normalized.rsplit("/", 1)[-1]
    if "/vendor/blpapi-sdk/" in f"/{normalized}":
        return True
    return basename.startswith(("libblpapi", "blpapi3", "blpapi_cpp_"))


def _read_natives(path: Path) -> tuple[dict[str, dict[str, bytes]], list[str]]:
    """``({platform: {file: bytes}}, problems)`` for a natives directory or a jar."""
    tree: dict[str, dict[str, bytes]] = {}
    problems: list[str] = []
    if path.is_dir():
        loaded = [
            (entry.relative_to(path).as_posix(), entry.read_bytes())
            for entry in sorted(path.rglob("*"))
            if entry.is_file()
        ]
    else:
        prefix = RESOURCE_ROOT + "/"
        with zipfile.ZipFile(path) as jar:
            loaded = [
                (name[len(prefix) :], jar.read(name))
                for name in jar.namelist()
                if name.startswith(prefix) and not name.endswith("/")
            ]
    for name, data in loaded:
        parts = name.split("/")
        platform = PLATFORM_BY_NAME.get(parts[0]) if len(parts) == 2 else None
        if platform is None or parts[1] not in (*platform.libraries, PROPERTIES):
            problems.append(f"{path}: unexpected natives entry {name}")
            continue
        tree.setdefault(platform.name, {})[parts[1]] = data
    return tree, problems


def _check_platform(
    label: str,
    platform: Platform,
    files: Mapping[str, bytes],
    *,
    release: bool,
    expect_version: str | None,
    expect_commit: str | None,
) -> list[str]:
    problems: list[str] = []
    where = f"{label}: {platform.name}"
    if PROPERTIES not in files:
        return [f"{where}: missing {PROPERTIES}"]
    values = parse_properties(files[PROPERTIES].decode("latin-1"))
    if not values.get("abi", "").isdigit():
        problems.append(f"{where}: {PROPERTIES} has no integer abi")
    for name in platform.libraries:
        if name not in files:
            problems.append(f"{where}: missing {name}")
            continue
        if sha256_bytes(files[name]) != values.get(f"{name}.sha256"):
            problems.append(f"{where}: {name} does not match its {PROPERTIES} sha256")
        if TEST_SUPPORT_MARKER in files[name] and (release or values.get("testSupport") != "true"):
            problems.append(f"{where}: {name} contains test-support fixtures ({TEST_SUPPORT_MARKER.decode()})")
    if release:
        expected = {"profile": RELEASE_PROFILE, "release": "true", "testSupport": "false", "portable": "true"}
        for key, value in expected.items():
            if values.get(key) != value:
                problems.append(f"{where}: release natives need {key}={value}, found {values.get(key)!r}")
        if _GIT_COMMIT.match(values.get("gitCommit", "")) is None:
            problems.append(f"{where}: release natives need a git commit, found {values.get('gitCommit')!r}")
    if expect_version is not None and values.get("crateVersion") != expect_version:
        problems.append(f"{where}: natives report version {values.get('crateVersion')!r}, expected {expect_version}")
    if expect_commit is not None and values.get("gitCommit") != expect_commit:
        problems.append(f"{where}: natives were built from {values.get('gitCommit')!r}, expected {expect_commit}")
    return problems


def _check_jar_metadata(path: Path, *, natives: bool, expect_version: str | None) -> list[str]:
    problems: list[str] = []
    with zipfile.ZipFile(path) as jar:
        names = set(jar.namelist())
        if not natives:
            prefix = f"{RESOURCE_ROOT}/"
            for name in sorted(names):
                if not name.endswith("/") and (
                    name.startswith(prefix) or name.lower().endswith((".dll", ".so", ".dylib"))
                ):
                    problems.append(f"{path}: non-native jar contains native file {name}")
        if expect_version is None:
            return problems
        manifest = (
            parse_manifest(jar.read("META-INF/MANIFEST.MF").decode("utf-8")) if "META-INF/MANIFEST.MF" in names else {}
        )
        version = manifest.get("Implementation-Version")
        if (natives or version is not None) and version != expect_version:
            problems.append(f"{path}: Implementation-Version {version!r}, expected {expect_version}")
        if natives:
            stamped = (
                parse_properties(jar.read(VERSION_RESOURCE).decode("latin-1")) if VERSION_RESOURCE in names else {}
            )
            if stamped.get("version") != expect_version:
                problems.append(
                    f"{path}: {VERSION_RESOURCE} reports {stamped.get('version')!r}, expected {expect_version}"
                )
    return problems


def _bloomberg_files(path: Path) -> list[str]:
    if path.is_dir():
        names = [entry.relative_to(path).as_posix() for entry in path.rglob("*") if entry.is_file()]
    elif zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    else:
        names = [path.name]
    return [f"{path}: Bloomberg SDK file {name}" for name in names if is_bloomberg_sdk_file(name)]


def verify(
    natives: Sequence[Path],
    scans: Sequence[Path] = (),
    *,
    release: bool = False,
    platforms: Sequence[str] = (),
    expect_version: str | None = None,
    expect_commit: str | None = None,
) -> list[str]:
    """Problems with staged natives or jars; an empty list means everything checks out."""
    required = list(platforms) or ([platform.name for platform in PLATFORMS] if release else [])
    problems: list[str] = []
    for path in natives:
        if not path.exists():
            problems.append(f"{path}: does not exist")
            continue
        if not path.is_dir() and not zipfile.is_zipfile(path):
            problems.append(f"{path}: is neither a natives directory nor a jar")
            continue
        tree, entry_problems = _read_natives(path)
        problems += entry_problems
        if not tree:
            problems.append(f"{path}: contains no natives")
        problems += [f"{path}: missing natives for {name}" for name in required if name not in tree]
        for name, files in sorted(tree.items()):
            problems += _check_platform(
                str(path),
                PLATFORM_BY_NAME[name],
                files,
                release=release,
                expect_version=expect_version,
                expect_commit=expect_commit,
            )
        if path.is_file():
            problems += _check_jar_metadata(path, natives=True, expect_version=expect_version)
    for path in scans:
        if not path.exists():
            problems.append(f"{path}: does not exist")
        elif path.is_file() and zipfile.is_zipfile(path):
            problems += _check_jar_metadata(path, natives=False, expect_version=expect_version)
    for path in (*natives, *scans):
        if path.exists():
            problems += _bloomberg_files(path)
    return problems


# ── Command line ─────────────────────────────────────────────────────────────────────────────


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    commands = parser.add_subparsers(dest="command", required=True)

    stage_cmd = commands.add_parser("stage", help="build both JNI crates and stage them for java-xbbg")
    stage_cmd.add_argument(
        "--profile",
        choices=PROFILES,
        help=f"Cargo profile (default: debug, or {RELEASE_PROFILE} with --release)",
    )
    stage_cmd.add_argument(
        "--release",
        action="store_true",
        help=f"stage a distributable build: {RELEASE_PROFILE}, --locked, no test-support, portable codegen, "
        f"a known git commit, glibc <= {GLIBC_MAX} on Linux; only these natives record release=true",
    )
    stage_cmd.add_argument(
        "--features",
        action="append",
        default=[],
        help="Cargo features (comma separated, repeatable); unqualified names belong to jni-xbbg, "
        "e.g. --features test-support",
    )
    stage_cmd.add_argument("--target", help="Rust target triple (default: the Cargo host)")
    stage_cmd.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="natives directory (default: java-xbbg/natives)"
    )
    stage_cmd.add_argument("--cargo", default="cargo", help="Cargo executable (default: cargo on PATH)")
    stage_cmd.add_argument("--locked", action="store_true", help="pass --locked to Cargo")

    verify_cmd = commands.add_parser("verify", help="check staged natives or built jars")
    verify_cmd.add_argument(
        "--natives",
        action="append",
        type=Path,
        default=[],
        help="a natives directory or the xbbg jar; must contain natives (repeatable)",
    )
    verify_cmd.add_argument(
        "--scan",
        action="append",
        type=Path,
        default=[],
        help="another jar, archive or directory to scan for Bloomberg SDK files (repeatable)",
    )
    verify_cmd.add_argument(
        "--platform",
        action="append",
        choices=sorted(PLATFORM_BY_NAME),
        default=[],
        help="platform that must be present (repeatable; --release requires all)",
    )
    verify_cmd.add_argument(
        "--release",
        action="store_true",
        help="require all platforms and stage --release provenance (release=true, portable, git commit), "
        "reject test-support",
    )
    verify_cmd.add_argument("--expect-version", help="version the natives, manifests and xbbg.properties must report")
    verify_cmd.add_argument("--expect-commit", help="git commit every staged native must have been built from")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "stage":
            stage(
                StageOptions(
                    profile=args.profile or (RELEASE_PROFILE if args.release else "debug"),
                    release=args.release,
                    features=tuple(args.features),
                    target=args.target,
                    out=args.out,
                    cargo=args.cargo,
                    locked=args.locked,
                )
            )
            return 0
        if not args.natives and not args.scan:
            raise StageError("verify needs at least one --natives or --scan path")
        problems = verify(
            args.natives,
            args.scan,
            release=args.release,
            platforms=args.platform,
            expect_version=args.expect_version,
            expect_commit=args.expect_commit,
        )
    except StageError as error:
        print(f"stage_jvm_natives: error: {error}", file=sys.stderr)
        return 1
    for problem in problems:
        print(f"stage_jvm_natives: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"verified {', '.join(str(path) for path in (*args.natives, *args.scan))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
