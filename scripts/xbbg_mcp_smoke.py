#!/usr/bin/env python
"""Smoke-test the xbbg MCP server against a live Bloomberg connection.

Usage:
    uv run python -X utf8 scripts/xbbg_mcp_smoke.py

Requires:
    - `target/debug/xbbg-mcp` built locally, or `target/release/xbbg-mcp` as a fallback
    - Bloomberg Terminal/B-PIPE connectivity available to xbbg
    - Bloomberg runtime library under `vendor/blpapi-sdk/<version>/Darwin/`
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEBUG_BINARY = REPO_ROOT / "target" / "debug" / "xbbg-mcp"
RELEASE_BINARY = REPO_ROOT / "target" / "release" / "xbbg-mcp"
SDK_ROOT = REPO_ROOT / "vendor" / "blpapi-sdk"
PROTOCOL_VERSION = "2025-06-18"


def default_binary() -> Path:
    if DEBUG_BINARY.exists():
        return DEBUG_BINARY
    return RELEASE_BINARY


class McpProtocolError(RuntimeError):
    """Raised when the MCP subprocess returns malformed or unexpected output."""


def find_blpapi_lib_dir() -> Path:
    candidates = sorted(
        path.parent
        for path in SDK_ROOT.glob("*/Darwin/libblpapi3.dylib")
        if path.is_file() and path.parts[-3] != ".cache"
    )
    if not candidates:
        raise FileNotFoundError("Could not find libblpapi3.dylib under vendor/blpapi-sdk/<version>/Darwin")
    return candidates[-1]


class McpSession:
    """Minimal newline-delimited JSON-RPC client for the rmcp stdio server."""

    def __init__(self, binary: Path, lib_dir: Path) -> None:
        env = dict(os.environ)
        dyld = env.get("DYLD_LIBRARY_PATH", "")
        env["DYLD_LIBRARY_PATH"] = f"{lib_dir}{os.pathsep}{dyld}" if dyld else str(lib_dir)
        self._proc = subprocess.Popen(
            [str(binary)],
            cwd=REPO_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

    def close(self) -> None:
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=2)

    def stderr_text(self) -> str:
        if self._proc.stderr is None:
            return ""
        return self._proc.stderr.read().strip()

    def send(self, message: dict[str, Any]) -> None:
        if self._proc.stdin is None:
            raise McpProtocolError("MCP stdin pipe is unavailable")
        self._proc.stdin.write(json.dumps(message) + "\n")
        self._proc.stdin.flush()

    def read(self) -> dict[str, Any]:
        if self._proc.stdout is None:
            raise McpProtocolError("MCP stdout pipe is unavailable")
        line = self._proc.stdout.readline()
        if not line:
            stderr = self.stderr_text()
            raise McpProtocolError(f"Unexpected EOF from MCP server. stderr={stderr!r}")
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise McpProtocolError(f"Invalid JSON from MCP server: {line!r}") from exc


def expect_success(response: dict[str, Any], request_id: int) -> dict[str, Any]:
    if response.get("id") != request_id:
        raise McpProtocolError(f"Expected response id {request_id}, got {response.get('id')!r}")
    if "error" in response:
        raise McpProtocolError(f"MCP error for request {request_id}: {json.dumps(response['error'])}")
    result = response.get("result")
    if result is None:
        raise McpProtocolError(f"Missing result for request {request_id}: {json.dumps(response)}")
    return result


def print_section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(title)
    print(f"{'=' * 72}")


def print_tool_result(name: str, result: dict[str, Any]) -> None:
    print(f"tool: {name}")
    print(f"isError: {result.get('isError')}")
    structured = result.get("structuredContent") or {}
    row_count = structured.get("row_count")
    returned_rows = structured.get("returned_rows")
    truncated = structured.get("truncated")
    print(f"row_count: {row_count}")
    print(f"returned_rows: {returned_rows}")
    print(f"truncated: {json.dumps(truncated, sort_keys=True)}")
    rows = structured.get("rows") or []
    preview = rows[:3]
    print("rows:")
    print(json.dumps(preview, indent=2, sort_keys=True))


def main() -> int:
    binary = default_binary()
    if not binary.exists():
        print(
            f"Missing MCP binary at {binary}. Build it first with: cargo build --release -p xbbg-mcp or cargo build -p xbbg-mcp",
            file=sys.stderr,
        )
        return 1

    try:
        lib_dir = find_blpapi_lib_dir()
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print_section("RUNTIME")
    print(f"binary : {binary.relative_to(REPO_ROOT)}")
    print(f"lib dir: {lib_dir.relative_to(REPO_ROOT)}")

    session = McpSession(binary=binary, lib_dir=lib_dir)
    try:
        print_section("INITIALIZE")
        session.send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "xbbg-mcp-smoke", "version": "0.0.0"},
                },
            }
        )
        init_result = expect_success(session.read(), 1)
        print(json.dumps(init_result, indent=2, sort_keys=True))

        session.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        print_section("TOOLS")
        session.send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        tools_result = expect_success(session.read(), 2)
        tool_names = [tool["name"] for tool in tools_result.get("tools", [])]
        print(json.dumps(tool_names, indent=2))

        expected_tools = {
            "bdp",
            "bdh",
            "bds",
            "bdib",
            "bflds",
            "bql",
            "bsrch",
            "resolve_venues",
            "auction_snapshot",
            "check_entitlements",
            "request",
        }
        missing_tools = sorted(expected_tools.difference(tool_names))
        if missing_tools:
            raise McpProtocolError(f"Missing expected tools: {missing_tools}")

        checks = [
            (
                3,
                "bdp",
                {"tickers": ["AAPL US Equity"], "fields": ["PX_LAST"]},
            ),
            (
                4,
                "bdh",
                {
                    "tickers": ["AAPL US Equity"],
                    "fields": ["PX_LAST"],
                    "start_date": "2025-03-03",
                    "end_date": "2025-03-07",
                },
            ),
            (
                5,
                "bql",
                {"expression": "get(px_last) for('IBM US Equity')"},
            ),
            (
                6,
                "resolve_venues",
                {"securities": ["IBM US Equity"]},
            ),
            (
                7,
                "auction_snapshot",
                {"securities": ["IBM US Equity"], "groups": ["state"]},
            ),
        ]

        for request_id, tool_name, arguments in checks:
            print_section(f"TOOLS/CALL {tool_name}")
            session.send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {"name": tool_name, "arguments": arguments},
                }
            )
            result = expect_success(session.read(), request_id)
            print_tool_result(tool_name, result)
            if result.get("isError"):
                raise McpProtocolError(f"Tool {tool_name} returned isError=true")
            structured = result.get("structuredContent") or {}
            if structured.get("row_count", 0) < 1:
                raise McpProtocolError(f"Tool {tool_name} returned no rows")

    except Exception as exc:
        stderr = session.stderr_text()
        print_section("FAILURE")
        print(str(exc), file=sys.stderr)
        if stderr:
            print("\n[MCP stderr]", file=sys.stderr)
            print(stderr, file=sys.stderr)
        return 1
    finally:
        session.close()

    print_section("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
