"""Regression tests for the cross-platform MCPB packager."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import package_xbbg_mcpb, render_mcp_registry_server


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3.20.0", "3.20.0"),
        ("3.26.4.2", "3.26.4.2"),
        ("03.020.000.0002", "3.20.0.2"),
    ],
)
def test_parse_blpapi_version_canonicalizes_numeric_versions(value: str, expected: str) -> None:
    assert package_xbbg_mcpb.parse_blpapi_version(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "3.20",
        "3.20.0.1.2",
        "3.20.-1",
        "3.20.65536",
        '3.20.0"; Write-Output injected; #',
    ],
)
def test_parse_blpapi_version_rejects_invalid_or_unsafe_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        package_xbbg_mcpb.parse_blpapi_version(value)


def test_minimum_blpapi_version_uses_newest_imported_symbol(tmp_path) -> None:
    binary = tmp_path / "xbbg-mcp-real"
    binary.write_bytes(b"prefix\0BLPAPI_3.6.4\0BLPAPI_3.20.0\0BLPAPI_3.15.0\0suffix")

    assert package_xbbg_mcpb.minimum_blpapi_version(binary) == "3.20.0"


def test_minimum_blpapi_version_fails_closed_without_symbols(tmp_path) -> None:
    binary = tmp_path / "xbbg-mcp-real"
    binary.write_bytes(b"no Bloomberg symbol versions")

    with pytest.raises(SystemExit, match="pass --min-blpapi-version explicitly"):
        package_xbbg_mcpb.minimum_blpapi_version(binary)


def test_windows_launcher_controls_dll_precedence() -> None:
    launcher = package_xbbg_mcpb.render_windows_launcher("03.020.000")

    assert '$requiredBlpapiVersion = [Version]"3.20.0"' in launcher
    assert "Assert-ValidatedDllWins $realBin $libDir" in launcher
    assert "$startInfo.WorkingDirectory = $libDir" in launcher
    assert '$startInfo.EnvironmentVariables["PATH"] = "$libDir;$env:PATH"' in launcher
    assert '$env:PATH = "$libDir;$env:PATH"' not in launcher


def test_windows_launcher_revalidates_direct_call_input() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        package_xbbg_mcpb.render_windows_launcher('3.20.0"; Write-Output injected; #')


def test_release_metadata_preserves_published_schema() -> None:
    version = "1.5.1"
    manifest = package_xbbg_mcpb.build_manifest(version)
    server = render_mcp_registry_server.build_server_metadata(
        version, "https://example.invalid/xbbg-mcp.mcpb", "a" * 64
    )

    # Captured before consolidating the two renderers, with their production JSON encoding.
    for payload, expected in (
        (manifest, "095a5e1a59a4f645c3ea80fbcb04d02bef89244909babbbc88093b4410de8895"),
        (server, "f93fdab1d67111c8c3f76cc27b2955110d7a68e6060fdaebce53b97d21d1f8ed"),
    ):
        encoded = (json.dumps(payload, indent=2) + "\n").encode()
        assert hashlib.sha256(encoded).hexdigest() == expected


def test_release_metadata_settings_stay_in_sync() -> None:
    manifest = package_xbbg_mcpb.build_manifest("1.0.0")
    server = render_mcp_registry_server.build_server_metadata("1.0.0", "https://example.invalid/mcp.mcpb", "a" * 64)
    environment = manifest["server"]["mcp_config"]["env"]
    registry_settings = server["packages"][0]["environmentVariables"]
    assert list(environment) == [setting["name"] for setting in registry_settings]
    assert len(environment) == 11
    for setting in registry_settings:
        key = environment[setting["name"]].removeprefix("${user_config.").removesuffix("}")
        config = manifest["user_config"][key]
        assert setting["isRequired"] == config["required"]
        assert setting["isSecret"] is False
        if "default" in config:
            assert setting["default"] == str(config["default"])
        else:
            assert "default" not in setting


def test_release_metadata_returns_fresh_records() -> None:
    manifest = package_xbbg_mcpb.build_manifest("1.0.0")
    manifest["user_config"]["port"]["default"] = 1234
    manifest["server"]["mcp_config"]["env"].clear()
    fresh = package_xbbg_mcpb.build_manifest("1.0.0")
    assert fresh["user_config"]["port"]["default"] == 8194
    assert len(fresh["server"]["mcp_config"]["env"]) == 11

    server = render_mcp_registry_server.build_server_metadata("1.0.0", "https://example.invalid/mcp.mcpb", "a" * 64)
    server["packages"][0]["environmentVariables"][0]["name"] = "CHANGED"
    fresh_server = render_mcp_registry_server.build_server_metadata(
        "1.0.0", "https://example.invalid/mcp.mcpb", "a" * 64
    )
    assert fresh_server["packages"][0]["environmentVariables"][0]["name"] == "XBBG_MCP_LIB_DIR"


@pytest.mark.parametrize("script", ["package_xbbg_mcpb.py", "render_mcp_registry_server.py"])
def test_metadata_scripts_support_direct_execution(script: str, tmp_path: Path) -> None:
    path = Path(__file__).with_name(script).resolve()
    result = subprocess.run(
        [sys.executable, str(path), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "--version" in result.stdout
