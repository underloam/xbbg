#!/usr/bin/env python3
"""Render the MCP registry server descriptor for a release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render xbbg MCP registry server metadata.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--mcpb-url", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def build_server_metadata(version: str, mcpb_url: str, sha256: str) -> dict[str, object]:
    return {
        "$schema": "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json",
        "name": "io.github.underloam/xbbg-mcp",
        "title": "xbbg MCP",
        "description": "Local Bloomberg tools for xbbg users.",
        "version": version,
        "websiteUrl": "https://github.com/underloam/xbbg/tree/main/apps/xbbg-mcp",
        "repository": {
            "url": "https://github.com/underloam/xbbg",
            "source": "github",
            "subfolder": "apps/xbbg-mcp",
        },
        "packages": [
            {
                "registryType": "mcpb",
                "identifier": mcpb_url,
                "version": version,
                "fileSha256": sha256,
                "transport": {"type": "stdio"},
                "environmentVariables": [
                    {
                        "name": "XBBG_MCP_LIB_DIR",
                        "description": "Optional directory containing Bloomberg runtime libraries. Leave unset to use BLPAPI_ROOT, an authorized vendored SDK layout, or Python blpapi fallback.",
                        "format": "filepath",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_HOST",
                        "description": "Bloomberg API host. Defaults to localhost.",
                        "default": "localhost",
                        "format": "string",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_PORT",
                        "description": "Bloomberg API port. Defaults to 8194.",
                        "default": "8194",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_AUTH_METHOD",
                        "description": "Bloomberg auth method: none, user, app, userapp, dir, manual, or token.",
                        "default": "none",
                        "format": "string",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_ROWS",
                        "description": "Maximum rows returned to the MCP client per response. Defaults to 500.",
                        "default": "500",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_CELLS",
                        "description": "Maximum data cells across returned rows and columns. Defaults to 50000.",
                        "default": "50000",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_METADATA_PROPERTIES",
                        "description": "Maximum metadata properties retained per response. Defaults to 50000.",
                        "default": "50000",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_METADATA_BYTES",
                        "description": "Maximum metadata bytes parsed or returned per response. Defaults to 65536.",
                        "default": "65536",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_STRING_CHARS",
                        "description": "Maximum characters per string value returned to the MCP client. Defaults to 2048.",
                        "default": "2048",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_STRING_BYTES",
                        "description": "Maximum UTF-8 bytes per string value, including a truncation marker. Defaults to 8192; minimum 3.",
                        "default": "8192",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                    {
                        "name": "XBBG_MCP_MAX_RESULT_BYTES",
                        "description": "Maximum compact-JSON bytes in the structured result, including diagnostics. Defaults to 1048576; minimum 2048.",
                        "default": "1048576",
                        "format": "number",
                        "isRequired": False,
                        "isSecret": False,
                    },
                ],
            },
        ],
    }


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2)
        output.write("\n")
    tmp_path.replace(path)


def main() -> int:
    args = parse_args()
    if not SHA256_PATTERN.fullmatch(args.sha256):
        raise SystemExit("SHA-256 must be 64 lowercase hex characters")

    write_json_atomic(args.output, build_server_metadata(args.version, args.mcpb_url, args.sha256))
    return 0


if __name__ == "__main__":
    sys.exit(main())
