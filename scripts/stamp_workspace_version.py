"""Stamp or check release versions without reformatting the root Cargo manifest."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import stat
import tempfile

INTERNAL_DEPENDENCIES = (
    "blpapi-sys",
    "xbbg_core",
    "xbbg-arrow",
    "xbbg-async",
    "xbbg-ext",
    "xbbg-log",
    "xbbg-recipes",
)

_NUMBER = r"(?:0|[1-9][0-9]*)"
_CORE_VERSION = rf"{_NUMBER}\.{_NUMBER}\.{_NUMBER}"
_PYTHON_PRERELEASE = re.compile(rf"(?P<core>{_CORE_VERSION})(?P<kind>a|b|rc)(?P<number>[0-9]+)")
_CARGO_VERSION = re.compile(
    rf"(?P<core>{_CORE_VERSION})(?:-(?P<pre>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)
_QUOTED = r"""(?:"(?:[^"\\\r\n]|\\[^\r\n])*"|'[^'\r\n]*')"""
_TOKEN = re.compile(rf"(?P<string>{_QUOTED})|(?P<comment>#[^\r\n]*)|[A-Za-z0-9_-]+|[^\s]")
_HEADER = re.compile(r"[ \t]*\[(?P<section>[^\]\r\n]+)\]\]?[ \t]*(?:#[^\r\n]*)?")
_ASSIGNMENT = re.compile(rf"[ \t]*(?P<key>[A-Za-z0-9_-]+|{_QUOTED})[ \t]*=[ \t]*")


def cargo_version(value: str) -> str:
    """Normalize Python release prereleases to Cargo SemVer, rejecting invalid input."""
    version = value.removeprefix("v")
    python_pre = _PYTHON_PRERELEASE.fullmatch(version)
    if python_pre is not None:
        kind = {"a": "alpha", "b": "beta", "rc": "rc"}[python_pre["kind"]]
        version = f"{python_pre['core']}-{kind}.{int(python_pre['number'])}"

    match = _CARGO_VERSION.fullmatch(version)
    if match is None or any(int(part) > 2**64 - 1 for part in match["core"].split(".")):
        raise ValueError(f"invalid release version: {value!r}")
    if match["pre"] is not None:
        for identifier in match["pre"].split("."):
            if identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0"):
                raise ValueError(f"non-canonical Cargo prerelease: {value!r}")
    return version


def _tokens(value: str, label: str) -> list[re.Match[str]]:
    tokens = []
    for token in _TOKEN.finditer(value):
        if token.lastgroup == "comment":
            break
        if token[0] in {"'", '"'}:
            raise ValueError(f"unterminated string in {label}")
        tokens.append(token)
    return tokens


def _string_span(token: re.Match[str], offset: int, label: str) -> tuple[int, int]:
    if token.lastgroup != "string":
        raise ValueError(f"{label} must be a quoted version")
    return offset + token.start() + 1, offset + token.end() - 1


def _dependency_version(value: str, offset: int, label: str) -> tuple[int, int]:
    """Locate the top-level version field, ignoring strings, comments and nested values."""
    tokens = _tokens(value, label)
    if len(tokens) < 2 or tokens[0][0] != "{" or tokens[-1][0] != "}":
        raise ValueError(f"{label} must use the existing single-line inline-table layout")

    fields = []
    field = []
    closing = []
    for token in tokens[1:-1]:
        symbol = token[0]
        if symbol in {"{", "["}:
            closing.append("}" if symbol == "{" else "]")
        elif symbol in {"}", "]"} and (not closing or closing.pop() != symbol):
            raise ValueError(f"unbalanced inline table in {label}")
        if symbol == "," and not closing:
            fields.append(field)
            field = []
        else:
            field.append(token)
    if closing:
        raise ValueError(f"unbalanced inline table in {label}")
    if field:
        fields.append(field)

    versions = []
    for field in fields:
        if len(field) < 3 or field[1][0] != "=":
            raise ValueError(f"invalid inline-table field in {label}")
        if field[0][0].strip("\"'") == "version":
            if len(field) != 3:
                raise ValueError(f"{label} must have a single quoted version")
            versions.append(_string_span(field[2], offset, label))
    if len(versions) != 1:
        raise ValueError(f"expected exactly one {label} version field, found {len(versions)}")
    return versions[0]


def _version_anchors(text: str) -> dict[str, tuple[int, int]]:
    """Require every anchor in the repository's existing root manifest layout."""
    sections = {"workspace.package": 0, "workspace.dependencies": 0}
    anchors: dict[str, tuple[int, int]] = {}
    section = ""
    offset = 0
    for line in text.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        header = _HEADER.fullmatch(content)
        if header is not None:
            section = header["section"]
            if section in sections:
                sections[section] += 1
        elif section in sections and (assignment := _ASSIGNMENT.match(content)) is not None:
            key = assignment["key"].strip("\"'")
            is_package_version = section == "workspace.package" and key == "version"
            is_dependency = section == "workspace.dependencies" and key in INTERNAL_DEPENDENCIES
            if is_package_version or is_dependency:
                label = f"{section}.{key}"
                if label in anchors:
                    raise ValueError(f"duplicate version anchor: {label}")
                value = content[assignment.end() :]
                value_offset = offset + assignment.end()
                if is_package_version:
                    tokens = _tokens(value, label)
                    if len(tokens) != 1:
                        raise ValueError(f"{label} must have a single quoted version")
                    anchors[label] = _string_span(tokens[0], value_offset, label)
                else:
                    anchors[label] = _dependency_version(value, value_offset, label)
        offset += len(line)

    for name, count in sections.items():
        if count != 1:
            raise ValueError(f"expected exactly one [{name}] section, found {count}")
    required = ["workspace.package.version", *(f"workspace.dependencies.{name}" for name in INTERNAL_DEPENDENCIES)]
    missing = [label for label in required if label not in anchors]
    if missing:
        raise ValueError(f"missing version anchors: {', '.join(missing)}")
    return anchors


def stamp_workspace_version(version: str, *, manifest: Path = Path("Cargo.toml"), check: bool = False) -> str:
    """Stamp all release anchors atomically, or fail on drift without writing in check mode."""
    version = cargo_version(version)
    text = manifest.read_bytes().decode("utf-8")
    anchors = _version_anchors(text)
    drift = [f"{label}={text[start:end]!r}" for label, (start, end) in anchors.items() if text[start:end] != version]
    if check:
        if drift:
            raise ValueError(f"{manifest}: expected {version}; version drift: {', '.join(drift)}")
        return version
    if not drift:
        return version

    # Spans are recorded in file order; replace only their contents, not quotes or whitespace.
    parts = []
    cursor = 0
    for start, end in anchors.values():
        parts.extend((text[cursor:start], version))
        cursor = end
    parts.append(text[cursor:])
    updated = "".join(parts).encode("utf-8")

    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=manifest.parent, prefix=f".{manifest.name}.", suffix=".tmp", delete=False
        ) as f:
            temporary = Path(f.name)
            f.write(updated)
            f.flush()
            os.fsync(f.fileno())
        temporary.chmod(stat.S_IMODE(manifest.stat().st_mode))
        os.replace(temporary, manifest)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="release version, with optional v prefix")
    parser.add_argument("--check", action="store_true", help="fail on version drift without changing the manifest")
    parser.add_argument("--manifest", type=Path, default=Path("Cargo.toml"))
    args = parser.parse_args(argv)
    try:
        version = stamp_workspace_version(args.version, manifest=args.manifest, check=args.check)
    except (OSError, UnicodeError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")
    action = "Checked" if args.check else "Stamped"
    print(f"{action} {version}: workspace.package and {len(INTERNAL_DEPENDENCIES)} internal dependencies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
