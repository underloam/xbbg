"""Binary format readers (PE, ELF, Mach-O) shared by native link-checking scripts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
import struct

_BLPAPI_RUNTIME = re.compile(r"^(?:lib)?blpapi3(?:_(?:32|64))?(?:[-.][^/\\]*)?$", re.IGNORECASE)

_LC_REQ_DYLD = 0x80000000
_LC_ID_DYLIB = 0xD
_LC_RPATH = 0x1C | _LC_REQ_DYLD
# LC_LOAD_DYLIB, LC_LOAD_WEAK_DYLIB, LC_REEXPORT_DYLIB, LC_LAZY_LOAD_DYLIB, LC_LOAD_UPWARD_DYLIB
_LC_DYLIBS = frozenset({0xC, 0x18 | _LC_REQ_DYLD, 0x1F | _LC_REQ_DYLD, 0x20, 0x23 | _LC_REQ_DYLD})
_LC_STRINGS = _LC_DYLIBS | {_LC_ID_DYLIB, _LC_RPATH}


class StageError(Exception):
    """A staging or verification step failed; the message says which and why."""


class BinaryFormatError(StageError):
    """A library is not a well-formed PE, ELF or Mach-O image."""


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


def dependencies(data: bytes) -> list[str]:
    """Libraries the dynamic loader must resolve before a PE, ELF or Mach-O image can run."""
    if data[:2] == b"MZ":
        return pe_imports(data)
    if data[:4] == b"\x7fELF":
        return elf_dynamic(data)[1]
    return list(macho_load_commands(data).dylibs)


def is_blpapi_runtime(reference: str) -> bool:
    """Whether a dependency reference names a Bloomberg runtime library."""
    return _BLPAPI_RUNTIME.match(PurePosixPath(reference.replace("\\", "/")).name) is not None


def is_non_portable_darwin_path(value: str) -> bool:
    """Whether a Mach-O dylib or rpath points outside macOS system libraries."""
    return value.startswith("/") and not value.startswith(("/usr/lib/", "/System/Library/"))
