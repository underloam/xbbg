#!/usr/bin/env python3
"""Stage the cross-platform xbbg MCPB bundle and launchers."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import sys

UNSUPPORTED_PLATFORM_MESSAGE = (
    "MCPB supports macOS arm64, Linux amd64, and Windows amd64; use GitHub release assets "
    "or build from source for this platform"
)
MISSING_REAL_BINARY_MESSAGE = "could not locate the real xbbg-mcp binary. Reinstall it or set XBBG_MCP_REAL_BIN."
MISSING_RUNTIME_MESSAGE = (
    "could not locate the Bloomberg runtime library. Set XBBG_MCP_LIB_DIR, BLPAPI_LIB_DIR, "
    "or BLPAPI_ROOT, or install Bloomberg's blpapi package."
)

README_TEMPLATE = """# xbbg MCP

Local stdio MCP server for authorized Bloomberg/xbbg users.

Version: {version}

This MCPB bundles the xbbg MCP launchers and prebuilt xbbg MCP binaries for macOS arm64, Linux amd64, and Windows amd64. It does not bundle Bloomberg SDK files, Bloomberg runtime libraries, Bloomberg credentials, or market data. You must provide Bloomberg runtime access locally through your own Bloomberg agreements and entitlements.

The macOS/Linux launcher searches for Bloomberg runtime libraries through `XBBG_MCP_LIB_DIR`, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`, a vendored authorized SDK layout, or the official Python `blpapi` package. The Windows launcher uses the same precedence, also scans `PATH` (where a Bloomberg Terminal install places `C:\\blp\\DAPI`), skips runtimes whose version cannot be verified or is too old to export the entry points `xbbg-mcp.exe` imports, and runs the child from the validated runtime directory while rejecting higher-priority shadow DLLs.

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

# Oldest Bloomberg API runtime that exports every entry point xbbg-mcp.exe imports (derived
# from the binaries at packaging time). An older blpapi3_64.dll lacks some of them, and the
# loader then kills the process before main() with STATUS_ENTRYPOINT_NOT_FOUND and no message.
$requiredBlpapiVersion = @REQUIRED_BLPAPI_VERSION@
$dllName = "blpapi3_64.dll"
$rejectedCandidates = New-Object System.Collections.Generic.List[string]

function Warn($message) {
    [Console]::Error.WriteLine("xbbg-mcp: $message")
}

function Die($message) {
    Warn $message
    exit 1
}

function Get-BlpapiDll($dir) {
    if (-not $dir) {
        return $null
    }
    if (-not (Test-Path -LiteralPath $dir -PathType Container)) {
        return $null
    }
    $dll = Join-Path $dir $dllName
    if (Test-Path -LiteralPath $dll -PathType Leaf) {
        return $dll
    }
    return $null
}

function Get-DllVersion($dll) {
    try {
        $info = [System.Diagnostics.FileVersionInfo]::GetVersionInfo($dll)
    } catch {
        return $null
    }
    if (($info.FileMajorPart + $info.FileMinorPart + $info.FileBuildPart + $info.FilePrivatePart) -eq 0) {
        return $null
    }
    return New-Object System.Version($info.FileMajorPart, $info.FileMinorPart, $info.FileBuildPart, $info.FilePrivatePart)
}

# Returns the directory when it holds a verifiable, new-enough 64-bit runtime, otherwise $null.
# Rejected candidates are recorded so the final error can explain each one.
function Select-LibDir($dir, $source) {
    $dll = Get-BlpapiDll $dir
    if (-not $dll) {
        return $null
    }
    $version = Get-DllVersion $dll
    if (-not $version) {
        $rejectedCandidates.Add("$dll has no readable Bloomberg API file version ($source)")
        return $null
    }
    if ($version -lt $requiredBlpapiVersion) {
        $rejectedCandidates.Add("$dll is Bloomberg API $version, older than the $requiredBlpapiVersion this build needs ($source)")
        return $null
    }
    return $dir
}

function Select-ExplicitLibDir($dir, $name) {
    if (-not $dir) {
        return $null
    }
    if (-not (Get-BlpapiDll $dir)) {
        Warn "$name=$dir does not contain $dllName; searching elsewhere"
        return $null
    }
    $selected = Select-LibDir $dir $name
    if (-not $selected) {
        # Already reported here; keep the final summary for candidates rejected silently.
        $last = $rejectedCandidates.Count - 1
        Warn ($rejectedCandidates[$last] + "; searching elsewhere")
        $rejectedCandidates.RemoveAt($last)
    }
    return $selected
}

function Resolve-SdkRootLayout($root, $source) {
    if (-not $root) {
        return $null
    }
    if (-not (Test-Path -LiteralPath $root -PathType Container)) {
        return $null
    }

    foreach ($candidate in @(
        $root,
        (Join-Path $root "bin"),
        (Join-Path $root "lib"),
        (Join-Path $root "lib64")
    )) {
        $selected = Select-LibDir $candidate $source
        if ($selected) {
            return $selected
        }
    }

    $versionChildren = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^\d+\.\d+\.\d+(\.\d+)?$' } |
        Sort-Object -Descending -Property @{ Expression = {
            (($_.Name -split '\.') | ForEach-Object { "{0:D8}" -f [int]$_ }) -join '.'
        } }

    foreach ($child in $versionChildren) {
        foreach ($candidate in @(
            $child.FullName,
            (Join-Path $child.FullName "bin"),
            (Join-Path $child.FullName "lib"),
            (Join-Path $child.FullName "lib64")
        )) {
            $selected = Select-LibDir $candidate $source
            if ($selected) {
                return $selected
            }
        }
    }

    return $null
}

# Windows convention: a Terminal install puts C:\blp\DAPI on PATH. Its DLL may be older than
# the one this build needs, so PATH entries go through the same version gate as everything else.
function Find-PathLibDir() {
    foreach ($entry in ($env:PATH -split ';')) {
        $dir = $entry.Trim().Trim('"')
        if (-not $dir) {
            continue
        }
        $selected = Select-LibDir $dir "PATH"
        if ($selected) {
            return $selected
        }
    }
    return $null
}

function Find-PythonBlpapiDir() {
    $pythonScript = @'
from pathlib import Path

try:
    import blpapi
except Exception as exc:
    raise SystemExit(1) from exc

module_path = getattr(blpapi, "__file__", None)
if not module_path:
    raise SystemExit(1)

path = Path(module_path).resolve().parent
for candidate in (path, path / "lib"):
    if (candidate / "blpapi3_64.dll").is_file():
        print(candidate)
        raise SystemExit(0)

raise SystemExit(1)
'@

    foreach ($python in @("py", "python", "python3")) {
        if (-not (Get-Command $python -ErrorAction SilentlyContinue)) {
            continue
        }

        $output = & $python -c $pythonScript 2>$null
        if ($LASTEXITCODE -eq 0 -and $output) {
            $dir = ($output | Select-Object -First 1)
            $selected = Select-LibDir $dir "Python blpapi package"
            if ($selected) {
                return $selected
            }
        }
    }

    return $null
}

function Find-VendoredSdkDir($baseDir) {
    $vendorRoot = Join-Path $baseDir "vendor\blpapi-sdk"
    return Resolve-SdkRootLayout $vendorRoot "vendored SDK"
}

function Find-RuntimeLibDir() {
    $libDir = Select-ExplicitLibDir $env:XBBG_MCP_LIB_DIR "XBBG_MCP_LIB_DIR"
    if ($libDir) {
        return $libDir
    }

    $libDir = Select-ExplicitLibDir $env:BLPAPI_LIB_DIR "BLPAPI_LIB_DIR"
    if ($libDir) {
        return $libDir
    }

    if ($env:BLPAPI_ROOT) {
        $libDir = Resolve-SdkRootLayout $env:BLPAPI_ROOT "BLPAPI_ROOT"
        if ($libDir) {
            return $libDir
        }
    }

    $mcpbRoot = Join-Path $PSScriptRoot ".."
    $libDir = Find-VendoredSdkDir $mcpbRoot
    if ($libDir) {
        return $libDir
    }

    $libDir = Find-PathLibDir
    if ($libDir) {
        return $libDir
    }

    $libDir = Find-PythonBlpapiDir
    if ($libDir) {
        return $libDir
    }

    return $null
}

function Assert-ValidatedDllWins($realBin, $libDir) {
    $selectedDll = [System.IO.Path]::GetFullPath((Join-Path $libDir $dllName))
    $higherPriorityDirs = New-Object System.Collections.Generic.List[string]
    $higherPriorityDirs.Add((Split-Path -Parent $realBin))
    $higherPriorityDirs.Add([Environment]::SystemDirectory)
    if ($env:WINDIR) {
        $higherPriorityDirs.Add((Join-Path $env:WINDIR "System"))
        $higherPriorityDirs.Add($env:WINDIR)
    }

    foreach ($dir in ($higherPriorityDirs | Select-Object -Unique)) {
        if (-not $dir) {
            continue
        }
        $candidate = Join-Path $dir $dllName
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            continue
        }
        $candidate = [System.IO.Path]::GetFullPath($candidate)
        if (-not [string]::Equals($candidate, $selectedDll, [System.StringComparison]::OrdinalIgnoreCase)) {
            Die "$candidate would shadow the validated Bloomberg runtime $selectedDll in the Windows DLL search order; remove the shadowing copy"
        }
    }
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

$libDir = Find-RuntimeLibDir
if (-not $libDir) {
    foreach ($rejected in $rejectedCandidates) {
        Warn $rejected
    }
    Die "@MISSING_RUNTIME_MESSAGE@"
}
$libDir = (Resolve-Path -LiteralPath $libDir).Path
Assert-ValidatedDllWins $realBin $libDir

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $realBin
$startInfo.Arguments = (@($args | ForEach-Object { Format-Argument $_ }) -join ' ')
$startInfo.UseShellExecute = $false
# The current directory precedes PATH in the Windows DLL search order. Point both at the
# validated runtime so an arbitrary MCP host working directory cannot shadow it.
$startInfo.WorkingDirectory = $libDir
$startInfo.EnvironmentVariables["PATH"] = "$libDir;$env:PATH"
# No redirection: the child inherits this process's stdin, stdout, and stderr handles, so the
# MCP host talks to xbbg-mcp.exe directly and PowerShell never touches the byte stream.
$process = [System.Diagnostics.Process]::Start($startInfo)
$process.WaitForExit()
$exitCode = $process.ExitCode

# The loader reports an unusable dependency by exit status alone; translate the common ones
# without claiming which direct or transitive module caused the failure.
$dllVersion = Get-DllVersion (Join-Path $libDir $dllName)
switch ($exitCode) {
    -1073741515 { Warn "xbbg-mcp.exe exited with STATUS_DLL_NOT_FOUND: a direct or transitive DLL dependency was not found. Selected Bloomberg runtime: $libDir\$dllName (Bloomberg API $dllVersion)" }
    -1073741511 { Warn "xbbg-mcp.exe exited with STATUS_ENTRYPOINT_NOT_FOUND: a loaded dependency lacks an entry point this build needs. Selected Bloomberg runtime: $libDir\$dllName (Bloomberg API $dllVersion)" }
    -1073741701 { Warn "xbbg-mcp.exe exited with STATUS_INVALID_IMAGE_FORMAT: a loaded dependency has the wrong architecture or an invalid image. Selected Bloomberg runtime: $libDir\$dllName (Bloomberg API $dllVersion)" }
}
exit $exitCode
"""


BLPAPI_RUNTIME_VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?$")
BLPAPI_SYMBOL_VERSION = re.compile(rb"BLPAPI_(\d+)\.(\d+)\.(\d+)")
MAX_VERSION_PART = 65_535


def parse_blpapi_version(value: str) -> str:
    """Validate and canonicalize a three- or four-part Bloomberg runtime version."""
    match = BLPAPI_RUNTIME_VERSION.fullmatch(value)
    if not match:
        raise argparse.ArgumentTypeError("expected three or four numeric components, for example 3.20.0 or 3.26.4.2")
    parts = tuple(int(part) for part in match.groups() if part is not None)
    if any(part > MAX_VERSION_PART for part in parts):
        raise argparse.ArgumentTypeError(f"each Bloomberg runtime version component must be <= {MAX_VERSION_PART}")
    return ".".join(str(part) for part in parts)


def render_windows_launcher(required_blpapi_version: str) -> str:
    canonical_version = parse_blpapi_version(required_blpapi_version)
    return (
        WINDOWS_LAUNCHER_TEMPLATE.replace("@REQUIRED_BLPAPI_VERSION@", f'[Version]"{canonical_version}"')
        .replace("@UNSUPPORTED_PLATFORM_MESSAGE@", UNSUPPORTED_PLATFORM_MESSAGE)
        .replace("@MISSING_REAL_BINARY_MESSAGE@", MISSING_REAL_BINARY_MESSAGE)
        .replace("@MISSING_RUNTIME_MESSAGE@", MISSING_RUNTIME_MESSAGE)
    )


def minimum_blpapi_version(linux_bin: Path) -> str:
    """Oldest Bloomberg API runtime that exports every entry point the binaries import.

    libblpapi3.so tags each exported function with the release that introduced it
    (``BLPAPI_3.20.0`` and so on) and the linker records the tags a binary needs in its
    ``.gnu.version_r`` section, so the newest tag referenced is the minimum runtime. The
    Windows binary imports the same functions by name, and Bloomberg numbers the C++ SDK
    identically on every platform. The version the release was *built* against is the
    wrong bound: a binary built against 3.26.7 that only uses 3.20.0 entry points runs on
    any 3.20.0 or newer DLL, and gating on 3.26.7 would refuse current runtimes.
    """
    versions = {
        tuple(int(part) for part in match.groups()) for match in BLPAPI_SYMBOL_VERSION.finditer(linux_bin.read_bytes())
    }
    if not versions:
        raise SystemExit(f"no BLPAPI symbol versions found in {linux_bin}; pass --min-blpapi-version explicitly")
    return ".".join(str(part) for part in max(versions))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage a cross-platform xbbg MCPB directory.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--darwin-arm64-bin", required=True, type=Path)
    parser.add_argument("--linux-amd64-bin", required=True, type=Path)
    parser.add_argument("--windows-amd64-bin", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--min-blpapi-version",
        default=None,
        type=parse_blpapi_version,
        help="Oldest Bloomberg API runtime the Windows launcher accepts; derived from the Linux binary's symbol versions when omitted",
    )
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
                "env": {
                    "XBBG_MCP_LIB_DIR": "${user_config.blpapi_lib_dir}",
                    "XBBG_MCP_HOST": "${user_config.host}",
                    "XBBG_MCP_PORT": "${user_config.port}",
                    "XBBG_MCP_AUTH_METHOD": "${user_config.auth_method}",
                    "XBBG_MCP_MAX_ROWS": "${user_config.max_rows}",
                    "XBBG_MCP_MAX_CELLS": "${user_config.max_cells}",
                    "XBBG_MCP_MAX_METADATA_PROPERTIES": "${user_config.max_metadata_properties}",
                    "XBBG_MCP_MAX_METADATA_BYTES": "${user_config.max_metadata_bytes}",
                    "XBBG_MCP_MAX_STRING_CHARS": "${user_config.max_string_chars}",
                    "XBBG_MCP_MAX_STRING_BYTES": "${user_config.max_string_bytes}",
                    "XBBG_MCP_MAX_RESULT_BYTES": "${user_config.max_result_bytes}",
                },
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
            {"name": "check_entitlements", "description": "Bloomberg entitlement-ID check for a service."},
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
        "user_config": {
            "blpapi_lib_dir": {
                "type": "directory",
                "title": "Bloomberg runtime library directory",
                "description": (
                    "Optional directory containing libblpapi3.dylib, libblpapi3.so, libblpapi3_64.so, "
                    "or blpapi3_64.dll. Leave empty to let the launcher try BLPAPI_ROOT, a vendored "
                    "authorized SDK layout, PATH (Windows), or Python blpapi."
                ),
                "required": False,
            },
            "host": {
                "type": "string",
                "title": "Bloomberg host",
                "description": "Bloomberg API host for DAPI/BPIPE.",
                "default": "localhost",
                "required": False,
            },
            "port": {
                "type": "number",
                "title": "Bloomberg port",
                "description": "Bloomberg API port.",
                "default": 8194,
                "min": 1,
                "max": 65535,
                "required": False,
            },
            "auth_method": {
                "type": "string",
                "title": "Authentication method",
                "description": (
                    "Bloomberg auth method. Use none for local Desktop API/DAPI unless your environment requires "
                    "SAPI/BPIPE auth."
                ),
                "default": "none",
                "required": False,
            },
            "max_rows": {
                "type": "number",
                "title": "Maximum returned rows",
                "description": "Maximum rows returned to the MCP client per response.",
                "default": 500,
                "min": 1,
                "required": False,
            },
            "max_cells": {
                "type": "number",
                "title": "Maximum returned cells",
                "description": "Maximum data cells across returned rows and columns per response.",
                "default": 50000,
                "min": 1,
                "required": False,
            },
            "max_metadata_properties": {
                "type": "number",
                "title": "Maximum metadata properties",
                "description": "Maximum metadata properties retained per response.",
                "default": 50000,
                "min": 1,
                "required": False,
            },
            "max_metadata_bytes": {
                "type": "number",
                "title": "Maximum metadata bytes",
                "description": "Maximum metadata bytes parsed or returned per response.",
                "default": 65536,
                "min": 1,
                "required": False,
            },
            "max_string_chars": {
                "type": "number",
                "title": "Maximum string characters",
                "description": "Maximum characters per string value returned to the MCP client.",
                "default": 2048,
                "min": 1,
                "required": False,
            },
            "max_string_bytes": {
                "type": "number",
                "title": "Maximum UTF-8 string bytes",
                "description": "Maximum UTF-8 bytes per string value, including any truncation marker.",
                "default": 8192,
                "min": 3,
                "required": False,
            },
            "max_result_bytes": {
                "type": "number",
                "title": "Maximum JSON result bytes",
                "description": "Maximum compact-JSON bytes in the structured result, including truncation diagnostics.",
                "default": 1048576,
                "min": 2048,
                "required": False,
            },
        },
    }


def render_posix_launcher(repo_root: Path) -> str:
    source_path = repo_root / "scripts" / "xbbg-mcp"
    source = source_path.read_text(encoding="utf-8")
    marker = "find_real_binary() {\n"
    start = source.find(marker)
    if start == -1:
        raise SystemExit(f"{source_path} does not contain find_real_binary()")

    next_function = "\n\nappend_loader_path() {"
    end = source.find(next_function, start)
    if end == -1:
        raise SystemExit(f"{source_path} does not contain append_loader_path() after find_real_binary()")

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
    min_blpapi_version = args.min_blpapi_version or minimum_blpapi_version(linux_bin)
    powershell_launcher.write_text(render_windows_launcher(min_blpapi_version), encoding="utf-8")

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
