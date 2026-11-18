"""Regression tests for the cross-platform MCPB packager."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from scripts import package_xbbg_mcpb, render_mcp_registry_server


def test_windows_launcher_leaves_the_runtime_search_to_the_binary() -> None:
    launcher = package_xbbg_mcpb.render_windows_launcher()

    assert "@UNSUPPORTED_PLATFORM_MESSAGE@" not in launcher and "@MISSING_REAL_BINARY_MESSAGE@" not in launcher
    assert package_xbbg_mcpb.MISSING_REAL_BINARY_MESSAGE in launcher
    assert 'EnvironmentVariables["XBBG_MCP_HINT_DIRS"] = $hintDir' in launcher
    assert "find_spec" in launcher
    for removed in (
        "requiredBlpapiVersion",
        "FileVersionInfo",
        "Assert-ValidatedDllWins",
        "WorkingDirectory",
        'EnvironmentVariables["PATH"]',
        "Find-RuntimeLibDir",
        "blpapi3_64.dll",
    ):
        assert removed not in launcher


def test_posix_launcher_passes_hints_and_never_touches_the_loader_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    for launcher in (
        (repo_root / "scripts" / "xbbg-mcp").read_text(encoding="utf-8"),
        package_xbbg_mcpb.render_posix_launcher(repo_root),
    ):
        assert "export XBBG_MCP_HINT_DIRS" in launcher
        assert "export XBBG_MCP_REPO_ROOT" in launcher
        for removed in ("export DYLD_", "export LD_LIBRARY_PATH", "export PATH", "contains_blpapi_lib"):
            assert removed not in launcher


def test_staged_posix_launcher_replaces_only_the_real_binary_lookup() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source = (repo_root / "scripts" / "xbbg-mcp").read_text(encoding="utf-8")
    staged = package_xbbg_mcpb.render_posix_launcher(repo_root)

    assert "bin/darwin-arm64/xbbg-mcp-real" in staged
    assert "bin/darwin-arm64/xbbg-mcp-real" not in source
    assert staged.split("export_runtime_hints() {")[1] == source.split("export_runtime_hints() {")[1]


def test_readme_describes_the_binary_side_search_order() -> None:
    readme = package_xbbg_mcpb.README_TEMPLATE.format(version="1.0.0")

    assert "do not search for the Bloomberg runtime" in readme
    assert "XBBG_MCP_LIB_DIR" in readme and "XBBG_DEV_SDK_ROOT" in readme
    assert "version cannot be verified" not in readme
    assert "shadow" not in readme


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
