#!/usr/bin/env python3
"""Render the MCP registry server descriptor for a release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

if __package__:
    from .mcp_release_config import registry_environment_variables
else:
    from mcp_release_config import registry_environment_variables

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
                "environmentVariables": registry_environment_variables(),
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
