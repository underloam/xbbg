#!/usr/bin/env python3
"""Stage the cross-platform xbbg MCPB bundle and launchers."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys

if __package__:
    from .mcp_release_config import mcpb_environment, mcpb_user_config
else:
    from mcp_release_config import mcpb_environment, mcpb_user_config

UNSUPPORTED_PLATFORM_MESSAGE = (
    "MCPB supports macOS arm64, Linux amd64, and Windows amd64; use GitHub release assets "
    "or build from source for this platform"
)
MISSING_REAL_BINARY_MESSAGE = "could not locate the real xbbg-mcp binary. Reinstall it or set XBBG_MCP_REAL_BIN."

README_TEMPLATE = """# xbbg MCP

Local stdio MCP server for authorized Bloomberg/xbbg users.

Version: {version}

This MCPB bundles the xbbg MCP launchers and prebuilt xbbg MCP binaries for macOS arm64, Linux amd64, and Windows amd64. It does not bundle Bloomberg SDK files, Bloomberg runtime libraries, Bloomberg credentials, or market data. You must provide Bloomberg runtime access locally through your own Bloomberg agreements and entitlements.

The launchers do not search for the Bloomberg runtime; `xbbg-mcp` finds and loads it at startup, in this order: `XBBG_MCP_LIB_DIR`, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`, `XBBG_DEV_SDK_ROOT`, the official Python `blpapi` package (the launchers pass its directory), a Bloomberg Terminal install, the operating system's library search path variables (`PATH` on Windows, `LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on macOS), the operating system's own library search (a system-wide install), then runtimes provisioned into the xbbg cache. A runtime that cannot be opened or is too old is skipped, and the startup error lists every one that was refused and why; `XBBG_MCP_LIB_DIR` is the exception, because it names the runtime to use: when it does not load, nothing else is tried.

Documentation: https://github.com/underloam/xbbg/tree/main/apps/xbbg-mcp
Privacy policy: https://github.com/underloam/xbbg/tree/main/apps/xbbg-mcp#privacy-policy
"""

POSIX_FIND_REAL_BINARY = f'''find_real_binary() {{
    home=$(script_dir)
    os_name=$(uname -s 2>/dev/null || printf 'unknown')
    arch_name=$(uname -m 2>/dev/null || printf 'unknown')

    case "$os_name:$arch_name" in
        Darwin:arm64|Darwin:aarch64)
            real_bin="$home/bin/darwin-arm64/xbbg-mcp-real"
            ;;
        Linux:x86_64|Linux:amd64)
            real_bin="$home/bin/linux-amd64/xbbg-mcp-real"
            ;;
        *)
            die "{UNSUPPORTED_PLATFORM_MESSAGE}"
            ;;
    esac

    if [ ! -x "$real_bin" ]; then
        die "{MISSING_REAL_BINARY_MESSAGE}"
    fi

    export XBBG_MCP_REAL_BIN="$real_bin"
    printf '%s\\n' "$XBBG_MCP_REAL_BIN"
    return 0
}}'''

# Windows has no exec(2). The launcher therefore starts xbbg-mcp.exe with inherited standard
# handles and waits: the MCP byte stream never passes through PowerShell. Running the binary as
# a PowerShell native command (`& $exe`) would let Windows PowerShell 5.1 sit between the host
# and the server -- re-encoding stdout through the console code page and, under
# $ErrorActionPreference = "Stop", terminating the child on its first stderr line.
WINDOWS_LAUNCHER_TEMPLATE = r"""$ErrorActionPreference = "Stop"

# This launcher does not search for the Bloomberg runtime: xbbg-mcp.exe finds and loads it itself
# (XBBG_MCP_LIB_DIR, BLPAPI_LIB_DIR, BLPAPI_ROOT, XBBG_DEV_SDK_ROOT, the hint directories below,
# a Bloomberg Terminal install, PATH, the directory of xbbg-mcp.exe and the system directory, the
# xbbg cache), skipping any runtime it cannot open or that is too old, and listing each skipped
# one if none loads. XBBG_MCP_LIB_DIR names the runtime to use: when it does not load, nothing
# else is tried. The launcher only passes what the binary cannot know: the directory of the
# Python blpapi package, as XBBG_MCP_HINT_DIRS.

function Warn($message) {
    [Console]::Error.WriteLine("xbbg-mcp: $message")
}

function Die($message) {
    Warn $message
    exit 1
}

# The directory of the Python blpapi package, found without importing it.
function Find-PythonBlpapiDir() {
    # PowerShell 5.1 turns native stderr redirected with `2>$null` into a
    # terminating NativeCommandError under $ErrorActionPreference = "Stop"
    # (for example, the WindowsApps python stub or `py` with no Python installed).
    $ErrorActionPreference = "Continue"
    $pythonScript = @'
import importlib.util
from pathlib import Path

try:
    spec = importlib.util.find_spec("blpapi")
except (ImportError, ValueError):
    raise SystemExit(1)

origin = getattr(spec, "origin", None)
if not origin or origin in ("built-in", "frozen"):
    raise SystemExit(1)

print(Path(origin).resolve().parent)
'@

    foreach ($python in @("py", "python", "python3")) {
        if (-not (Get-Command $python -ErrorAction SilentlyContinue)) {
            continue
        }

        try {
            $output = & $python -c $pythonScript 2>$null
            if ($LASTEXITCODE -eq 0 -and $output) {
                return ($output | Select-Object -First 1)
            }
        } catch {
            continue
        }
    }

    return $null
}

# Quote one argument the way the Microsoft C runtime parses argv (CommandLineToArgvW rules).
function Format-Argument($arg) {
    if ($arg -eq "") {
        return '""'
    }
    if ($arg -notmatch '[\s"]') {
        return $arg
    }
    $escaped = $arg -replace '(\\*)"', '$1$1\"'
    $escaped = $escaped -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}

$architectures = @($env:PROCESSOR_ARCHITECTURE, $env:PROCESSOR_ARCHITEW6432) | Where-Object { $_ }
if (-not ($architectures -contains "AMD64")) {
    Die "@UNSUPPORTED_PLATFORM_MESSAGE@"
}

$realBin = Join-Path $PSScriptRoot "bin\windows-amd64\xbbg-mcp.exe"
if (-not (Test-Path -LiteralPath $realBin -PathType Leaf)) {
    Die "@MISSING_REAL_BINARY_MESSAGE@"
}

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $realBin
$startInfo.Arguments = (@($args | ForEach-Object { Format-Argument $_ }) -join ' ')
$startInfo.UseShellExecute = $false
if (-not $env:XBBG_MCP_LIB_DIR -and -not $env:XBBG_MCP_HINT_DIRS) {
    $hintDir = Find-PythonBlpapiDir
    if ($hintDir) {
        $startInfo.EnvironmentVariables["XBBG_MCP_HINT_DIRS"] = $hintDir
    }
}
# No redirection: the child inherits this process's stdin, stdout, and stderr handles, so the
# MCP host talks to xbbg-mcp.exe directly and PowerShell never touches the byte stream.
$process = [System.Diagnostics.Process]::Start($startInfo)
$process.WaitForExit()
exit $process.ExitCode
"""


def render_windows_launcher() -> str:
    return WINDOWS_LAUNCHER_TEMPLATE.replace("@UNSUPPORTED_PLATFORM_MESSAGE@", UNSUPPORTED_PLATFORM_MESSAGE).replace(
        "@MISSING_REAL_BINARY_MESSAGE@", MISSING_REAL_BINARY_MESSAGE
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage a cross-platform xbbg MCPB directory.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--darwin-arm64-bin", required=True, type=Path)
    parser.add_argument("--linux-amd64-bin", required=True, type=Path)
    parser.add_argument("--windows-amd64-bin", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def require_executable(path: Path) -> Path:
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SystemExit(f"missing executable: {path}")
    return path


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise SystemExit(f"missing file: {path}")
    return path


def chmod_755(path: Path) -> None:
    path.chmod(0o755)


def build_manifest(version: str) -> dict[str, object]:
    return {
        "manifest_version": "0.3",
        "name": "xbbg-mcp",
        "display_name": "xbbg MCP",
        "version": version,
        "description": "Local Bloomberg request/response tools for authorized xbbg users.",
        "long_description": (
            "Run xbbg Bloomberg request/response tools as a local stdio MCP server. This extension "
            "requires the user's own authorized Bloomberg runtime and does not bundle Bloomberg SDK "
            "files, credentials, or market data."
        ),
        "author": {
            "name": "underloam",
            "url": "https://github.com/underloam",
        },
        "repository": {
            "type": "git",
            "url": "https://github.com/underloam/xbbg.git",
        },
        "homepage": "https://github.com/underloam/xbbg",
        "documentation": "https://github.com/underloam/xbbg/tree/main/apps/xbbg-mcp",
        "support": "https://github.com/underloam/xbbg/issues",
        "server": {
            "type": "binary",
            "entry_point": "server/xbbg-mcp",
            "mcp_config": {
                "command": "${__dirname}/server/xbbg-mcp",
                "args": [],
                "env": mcpb_environment(),
                "platform_overrides": {
                    "win32": {
                        "command": "powershell.exe",
                        "args": [
                            "-NoProfile",
                            "-ExecutionPolicy",
                            "Bypass",
                            "-File",
                            "${__dirname}/server/xbbg-mcp.ps1",
                        ],
                    },
                },
            },
        },
        "tools": [
            {"name": "bdp", "description": "Bloomberg reference data request."},
            {"name": "bdh", "description": "Bloomberg historical data request."},
            {"name": "bds", "description": "Bloomberg bulk data request."},
            {"name": "bdib", "description": "Bloomberg intraday bar request."},
            {"name": "bql", "description": "Bloomberg Query Language request."},
            {"name": "bsrch", "description": "Bloomberg search request."},
            {"name": "bflds", "description": "Bloomberg field metadata lookup."},
            {
                "name": "resolve_venues",
                "description": "Route composite equities to primary exchange listings and preferreds to venue pricing sources.",
            },
            {
                "name": "auction_snapshot",
                "description": "Bloomberg primary-venue auction reference data; terminal-local times, may be delayed.",
            },
            {"name": "check_entitlements", "description": "Bloomberg entitlement-ID check for a service."},
            {
                "name": "delay_status",
                "description": "Per-ticker Bloomberg delayed/real-time status, with an optional feed-EID entitlement check.",
            },
            {"name": "request", "description": "Generic Bloomberg request."},
        ],
        "tools_generated": True,
        "keywords": ["mcp", "model-context-protocol", "bloomberg", "market-data", "finance", "xbbg"],
        "license": "Apache-2.0",
        "privacy_policies": ["https://github.com/underloam/xbbg/tree/main/apps/xbbg-mcp#privacy-policy"],
        "compatibility": {
            "claude_desktop": ">=1.0.0",
            "platforms": ["darwin", "linux", "win32"],
        },
        "user_config": mcpb_user_config(),
    }


def render_posix_launcher(repo_root: Path) -> str:
    source_path = repo_root / "scripts" / "xbbg-mcp"
    source = source_path.read_text(encoding="utf-8")
    marker = "find_real_binary() {\n"
    start = source.find(marker)
    if start == -1:
        raise SystemExit(f"{source_path} does not contain find_real_binary()")

    next_function = "\n\nexport_runtime_hints() {"
    end = source.find(next_function, start)
    if end == -1:
        raise SystemExit(f"{source_path} does not contain export_runtime_hints() after find_real_binary()")

    return source[:start] + POSIX_FIND_REAL_BINARY + source[end:]


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2)
        output.write("\n")
    tmp_path.replace(path)


def stage_bundle(args: argparse.Namespace) -> Path:
    darwin_bin = require_executable(args.darwin_arm64_bin)
    linux_bin = require_executable(args.linux_amd64_bin)
    windows_bin = require_file(args.windows_amd64_bin)

    repo_root = Path(__file__).resolve().parents[1]
    license_path = repo_root / "LICENSE"
    if not license_path.is_file():
        raise SystemExit("missing LICENSE")

    output_dir = args.output_dir.resolve()
    staging_dir = output_dir / "xbbg-mcp-mcpb"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)

    server_dir = staging_dir / "server"
    darwin_dir = server_dir / "bin" / "darwin-arm64"
    linux_dir = server_dir / "bin" / "linux-amd64"
    windows_dir = server_dir / "bin" / "windows-amd64"
    for directory in (darwin_dir, linux_dir, windows_dir):
        directory.mkdir(parents=True, exist_ok=True)

    posix_launcher = server_dir / "xbbg-mcp"
    posix_launcher.write_text(render_posix_launcher(repo_root), encoding="utf-8")
    powershell_launcher = server_dir / "xbbg-mcp.ps1"
    powershell_launcher.write_text(render_windows_launcher(), encoding="utf-8")

    shutil.copyfile(darwin_bin, darwin_dir / "xbbg-mcp-real")
    shutil.copyfile(linux_bin, linux_dir / "xbbg-mcp-real")
    shutil.copyfile(windows_bin, windows_dir / "xbbg-mcp.exe")
    shutil.copyfile(license_path, staging_dir / "LICENSE")

    (staging_dir / "README.md").write_text(README_TEMPLATE.format(version=args.version), encoding="utf-8")
    write_json_atomic(staging_dir / "manifest.json", build_manifest(args.version))

    for path in (
        posix_launcher,
        powershell_launcher,
        darwin_dir / "xbbg-mcp-real",
        linux_dir / "xbbg-mcp-real",
        windows_dir / "xbbg-mcp.exe",
    ):
        chmod_755(path)

    return staging_dir


def main() -> int:
    staging_dir = stage_bundle(parse_args())
    print(staging_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
