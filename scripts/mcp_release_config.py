"""Shared settings for the MCPB and MCP Registry release metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class McpSetting:
    """One server setting with the presentation text for both release schemas."""

    env: str
    key: str
    type: str
    title: str
    description: str
    registry_description: str
    default: str | int | None = None
    minimum: int | None = None
    maximum: int | None = None


SETTINGS = (
    McpSetting(
        env="XBBG_MCP_LIB_DIR",
        key="blpapi_lib_dir",
        type="directory",
        title="Bloomberg runtime library directory",
        description=(
            "Optional directory containing libblpapi3.dylib, libblpapi3.so, libblpapi3_64.so, "
            "or blpapi3_64.dll. Leave empty to let the launcher try BLPAPI_ROOT, a vendored "
            "authorized SDK layout, PATH (Windows), or Python blpapi."
        ),
        registry_description=(
            "Optional directory containing Bloomberg runtime libraries. Leave unset to use BLPAPI_ROOT, "
            "an authorized vendored SDK layout, or Python blpapi fallback."
        ),
    ),
    McpSetting(
        env="XBBG_MCP_HOST",
        key="host",
        type="string",
        title="Bloomberg host",
        description="Bloomberg API host for DAPI/BPIPE.",
        registry_description="Bloomberg API host. Defaults to localhost.",
        default="localhost",
    ),
    McpSetting(
        env="XBBG_MCP_PORT",
        key="port",
        type="number",
        title="Bloomberg port",
        description="Bloomberg API port.",
        registry_description="Bloomberg API port. Defaults to 8194.",
        default=8194,
        minimum=1,
        maximum=65535,
    ),
    McpSetting(
        env="XBBG_MCP_AUTH_METHOD",
        key="auth_method",
        type="string",
        title="Authentication method",
        description=(
            "Bloomberg auth method. Use none for local Desktop API/DAPI unless your environment requires "
            "SAPI/BPIPE auth."
        ),
        registry_description="Bloomberg auth method: none, user, app, userapp, dir, manual, or token.",
        default="none",
    ),
    McpSetting(
        env="XBBG_MCP_MAX_ROWS",
        key="max_rows",
        type="number",
        title="Maximum returned rows",
        description="Maximum rows returned to the MCP client per response.",
        registry_description="Maximum rows returned to the MCP client per response. Defaults to 500.",
        default=500,
        minimum=1,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_CELLS",
        key="max_cells",
        type="number",
        title="Maximum returned cells",
        description="Maximum data cells across returned rows and columns per response.",
        registry_description="Maximum data cells across returned rows and columns. Defaults to 50000.",
        default=50000,
        minimum=1,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_METADATA_PROPERTIES",
        key="max_metadata_properties",
        type="number",
        title="Maximum metadata properties",
        description="Maximum metadata properties retained per response.",
        registry_description="Maximum metadata properties retained per response. Defaults to 50000.",
        default=50000,
        minimum=1,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_METADATA_BYTES",
        key="max_metadata_bytes",
        type="number",
        title="Maximum metadata bytes",
        description="Maximum metadata bytes parsed or returned per response.",
        registry_description="Maximum metadata bytes parsed or returned per response. Defaults to 65536.",
        default=65536,
        minimum=1,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_STRING_CHARS",
        key="max_string_chars",
        type="number",
        title="Maximum string characters",
        description="Maximum characters per string value returned to the MCP client.",
        registry_description="Maximum characters per string value returned to the MCP client. Defaults to 2048.",
        default=2048,
        minimum=1,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_STRING_BYTES",
        key="max_string_bytes",
        type="number",
        title="Maximum UTF-8 string bytes",
        description="Maximum UTF-8 bytes per string value, including any truncation marker.",
        registry_description=(
            "Maximum UTF-8 bytes per string value, including a truncation marker. Defaults to 8192; minimum 3."
        ),
        default=8192,
        minimum=3,
    ),
    McpSetting(
        env="XBBG_MCP_MAX_RESULT_BYTES",
        key="max_result_bytes",
        type="number",
        title="Maximum JSON result bytes",
        description="Maximum compact-JSON bytes in the structured result, including truncation diagnostics.",
        registry_description=(
            "Maximum compact-JSON bytes in the structured result, including diagnostics. Defaults to 1048576; minimum 2048."
        ),
        default=1048576,
        minimum=2048,
    ),
)


def mcpb_environment() -> dict[str, str]:
    return {setting.env: f"${{user_config.{setting.key}}}" for setting in SETTINGS}


def mcpb_user_config() -> dict[str, dict[str, object]]:
    result = {}
    for setting in SETTINGS:
        record: dict[str, object] = {
            "type": setting.type,
            "title": setting.title,
            "description": setting.description,
        }
        if setting.default is not None:
            record["default"] = setting.default
        if setting.minimum is not None:
            record["min"] = setting.minimum
        if setting.maximum is not None:
            record["max"] = setting.maximum
        record["required"] = False
        result[setting.key] = record
    return result


def registry_environment_variables() -> list[dict[str, object]]:
    result = []
    for setting in SETTINGS:
        record: dict[str, object] = {
            "name": setting.env,
            "description": setting.registry_description,
        }
        if setting.default is not None:
            record["default"] = str(setting.default)
        record["format"] = "filepath" if setting.type == "directory" else setting.type
        record["isRequired"] = False
        record["isSecret"] = False
        result.append(record)
    return result
