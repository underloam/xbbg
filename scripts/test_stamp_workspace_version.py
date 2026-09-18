"""Regression tests for release-time Cargo workspace version stamping."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from scripts.stamp_workspace_version import cargo_version, stamp_workspace_version


def manifest_text(version: str = "1.4.7") -> str:
    return f"""[workspace]
members = []

[workspace.package] # release metadata
version = "{version}"  # Preserve this comment: version = "1.4.7"
license = "Apache-2.0"

[workspace.dependencies]
blpapi-sys = {{ package = "xbbg-blpapi-sys", path = "crates/blpapi-sys", version = "{version}" }}
xbbg_core = {{ path = "crates/xbbg-core", version = "{version}", default-features = false }}
xbbg-arrow = {{ path = "crates/xbbg-arrow", version = "{version}" }}
xbbg-async = {{ path = "crates/xbbg-async", version = "{version}", default-features = false }}
xbbg-ext = {{ path = 'crates/xbbg-ext', version = '{version}' }}
xbbg-log = {{ path = "crates/xbbg-log", version = "{version}" }} # version = "1.4.7" }}
xbbg-recipes = {{ path = "crates/xbbg-recipes", version = "{version}", default-features = false }}
third-party = {{ version = "1.4.7", features = ["version = '1.4.7'"] }}

[package.metadata.release]
version = "1.4.7"
xbbg-log = {{ version = "1.4.7" }}
"""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1.5.0", "1.5.0"),
        ("v1.5.0", "1.5.0"),
        ("1.5.0a1", "1.5.0-alpha.1"),
        ("v1.5.0b02", "1.5.0-beta.2"),
        ("1.5.0rc2", "1.5.0-rc.2"),
        ("1.5.0-rc.2", "1.5.0-rc.2"),
        ("1.5.0-preview.3+build.7", "1.5.0-preview.3+build.7"),
    ],
)
def test_cargo_version_normalizes_release_versions(value: str, expected: str) -> None:
    assert cargo_version(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "v",
        "1.5",
        "01.5.0",
        "1.5.0rc",
        "1.5.0-rc.02",
        "1.5.0-rc..2",
        "1.5.0.dev1",
        "1.5.0\n",
        '1.5.0"; injected = "yes',
        "18446744073709551616.0.0",
    ],
)
def test_invalid_version_leaves_manifest_unchanged(tmp_path: Path, value: str) -> None:
    manifest = tmp_path / "Cargo.toml"
    original = manifest_text().encode()
    manifest.write_bytes(original)

    with pytest.raises(ValueError):
        stamp_workspace_version(value, manifest=manifest)

    assert manifest.read_bytes() == original


def test_stamp_preserves_unrelated_values_comments_and_line_endings(tmp_path: Path) -> None:
    manifest = tmp_path / "Cargo.toml"
    manifest.write_bytes(manifest_text().replace("\n", "\r\n").encode())

    stamp_workspace_version("v1.5.0rc2", manifest=manifest)

    assert manifest.read_bytes() == manifest_text("1.5.0-rc.2").replace("\n", "\r\n").encode()


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("[workspace.package]", "[workspace.metadata]"),
        ('version = "1.4.7"  # Preserve', '# version = "1.4.7"  # Preserve'),
        ('version = "1.4.7"  # Preserve', "version = 7  # Preserve"),
        ("[workspace.dependencies]", "[dependencies]"),
        ("xbbg-recipes =", "external-recipes ="),
        ('version = "1.4.7", default-features = false }\nthird-party', "default-features = false }\nthird-party"),
        ('version = "1.4.7"  # Preserve', 'version = "1.4.7"\nversion = "1.4.7"  # Preserve'),
        ("[workspace.package]", "[workspace.package]\n[workspace.package]"),
        ("xbbg-recipes =", 'xbbg-recipes = { version = "1.4.7" }\nxbbg-recipes ='),
        ("xbbg-recipes =", "# xbbg-recipes ="),
        (
            'version = "1.4.7", default-features = false }\nthird-party',
            'version = ["1.4.7"], default-features = false }\nthird-party',
        ),
        (
            'version = "1.4.7", default-features = false }\nthird-party',
            'version = "1.4.7", version = "1.4.7", default-features = false }\nthird-party',
        ),
    ],
)
def test_invalid_or_missing_anchors_never_partially_stamp(tmp_path: Path, old: str, new: str) -> None:
    manifest = tmp_path / "Cargo.toml"
    original = manifest_text().replace(old, new).encode()
    manifest.write_bytes(original)

    with pytest.raises(ValueError):
        stamp_workspace_version("1.5.0", manifest=manifest)

    assert manifest.read_bytes() == original


def test_version_like_strings_cannot_supply_a_missing_dependency_version(tmp_path: Path) -> None:
    manifest = tmp_path / "Cargo.toml"
    original = (
        manifest_text()
        .replace(
            'xbbg-log = { path = "crates/xbbg-log", version = "1.4.7" }',
            'xbbg-log = { path = \'text, version = "1.4.7"\', features = ["version"] }',
        )
        .encode()
    )
    manifest.write_bytes(original)

    with pytest.raises(ValueError, match="xbbg-log"):
        stamp_workspace_version("1.5.0", manifest=manifest)

    assert manifest.read_bytes() == original


def test_check_reports_stale_internal_version_without_writing(tmp_path: Path) -> None:
    manifest = tmp_path / "Cargo.toml"
    original = (
        manifest_text("1.5.0")
        .replace(
            'xbbg-recipes = { path = "crates/xbbg-recipes", version = "1.5.0"',
            'xbbg-recipes = { path = "crates/xbbg-recipes", version = "1.4.7"',
        )
        .encode()
    )
    manifest.write_bytes(original)

    with pytest.raises(ValueError, match="xbbg-recipes"):
        stamp_workspace_version("v1.5.0", manifest=manifest, check=True)

    assert manifest.read_bytes() == original


def test_cli_check_accepts_normalized_version_and_rejects_drift(tmp_path: Path) -> None:
    manifest = tmp_path / "Cargo.toml"
    original = manifest_text("1.5.0-rc.2").encode()
    manifest.write_bytes(original)
    original_mtime = manifest.stat().st_mtime_ns
    script = Path(__file__).with_name("stamp_workspace_version.py")
    command = [sys.executable, str(script), "v1.5.0rc2", "--manifest", str(manifest), "--check"]

    matching = subprocess.run(command, capture_output=True, text=True, check=False)
    assert matching.returncode == 0, matching.stderr
    assert manifest.read_bytes() == original
    assert manifest.stat().st_mtime_ns == original_mtime

    stale = original.replace(b'version = "1.5.0-rc.2"', b'version = "1.4.7"', 1)
    manifest.write_bytes(stale)
    stale_mtime = manifest.stat().st_mtime_ns
    mismatching = subprocess.run(command, capture_output=True, text=True, check=False)
    assert mismatching.returncode != 0
    assert "workspace.package.version" in mismatching.stderr
    assert manifest.read_bytes() == stale
    assert manifest.stat().st_mtime_ns == stale_mtime
