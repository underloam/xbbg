#!/usr/bin/env sh
set -eu

REPO=${XBBG_MCP_REPO:-underloam/xbbg}
INSTALL_DIR=${XBBG_MCP_INSTALL_DIR:-$HOME/.local/bin}
VERSION=${1:-${XBBG_MCP_VERSION:-}}

note() {
    printf '%s\n' "$1"
}

die() {
    printf 'error: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_latest_version() {
    require_command curl
    latest_url=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest") || return 1
    tag=${latest_url##*/}
    tag=${tag#v}
    [ -n "$tag" ] || return 1
    printf '%s\n' "$tag"
}

normalize_version() {
    version=$1
    version=${version#v}
    [ -n "$version" ] || die "version must not be empty"
    printf '%s\n' "$version"
}

detect_platform() {
    os_name=$(uname -s 2>/dev/null || printf 'unknown')
    arch_name=$(uname -m 2>/dev/null || printf 'unknown')

    case "$os_name" in
        Darwin)
            case "$arch_name" in
                arm64|aarch64)
                    printf '%s\n' darwin-arm64
                    ;;
                x86_64)
                    die "prebuilt xbbg-mcp releases are not yet published for macOS x86_64; build from source instead"
                    ;;
                *)
                    die "unsupported macOS architecture: $arch_name"
                    ;;
            esac
            ;;
        Linux)
            case "$arch_name" in
                x86_64|amd64)
                    printf '%s\n' linux-amd64
                    ;;
                aarch64|arm64)
                    die "prebuilt xbbg-mcp releases are not yet published for Linux arm64; build from source instead"
                    ;;
                *)
                    die "unsupported Linux architecture: $arch_name"
                    ;;
            esac
            ;;
        *)
            die "this installer currently supports macOS arm64 and Linux amd64 only"
            ;;
    esac
}

if [ -z "$VERSION" ]; then
    VERSION=$(resolve_latest_version) || die "failed to resolve the latest xbbg release version"
else
    VERSION=$(normalize_version "$VERSION")
fi

PLATFORM=$(detect_platform)
ASSET="xbbg-mcp-v${VERSION}-${PLATFORM}.tar.gz"
DOWNLOAD_URL=${XBBG_MCP_DOWNLOAD_URL:-https://github.com/$REPO/releases/download/v${VERSION}/${ASSET}}

require_command curl
require_command tar
mkdir -p "$INSTALL_DIR"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/xbbg-mcp-install.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM HUP
ARCHIVE_PATH="$TMP_DIR/$ASSET"
EXTRACT_DIR="$TMP_DIR/extracted"

note "xbbg-mcp installer"
note "  Version : $VERSION"
note "  Platform: $PLATFORM"
note "  Install : $INSTALL_DIR"
note "  Source  : $DOWNLOAD_URL"

curl -fsSL "$DOWNLOAD_URL" -o "$ARCHIVE_PATH" || die "failed to download $DOWNLOAD_URL"
mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_PATH" -C "$EXTRACT_DIR"

[ -x "$EXTRACT_DIR/xbbg-mcp" ] || die "release asset is missing xbbg-mcp launcher"
[ -x "$EXTRACT_DIR/xbbg-mcp-real" ] || die "release asset is missing xbbg-mcp-real binary"

install -m 0755 "$EXTRACT_DIR/xbbg-mcp" "$INSTALL_DIR/xbbg-mcp"
install -m 0755 "$EXTRACT_DIR/xbbg-mcp-real" "$INSTALL_DIR/xbbg-mcp-real"

note ""
note "Installed:"
note "  $INSTALL_DIR/xbbg-mcp"
note "  $INSTALL_DIR/xbbg-mcp-real"
note ""
note "Claude Code:"
note "  claude mcp add --transport stdio xbbg -- \"$INSTALL_DIR/xbbg-mcp\""
note ""
note "OpenCode config:"
note "  {\"mcp\":{\"xbbg\":{\"type\":\"local\",\"command\":[\"$INSTALL_DIR/xbbg-mcp\"],\"enabled\":true}}}"
note ""
note "Release assets do not bundle Bloomberg SDK files or the Bloomberg runtime."
note "Install Bloomberg's Python blpapi package or provide your own SDK/runtime locally."
note ""
note "The launcher will try XBBG_MCP_LIB_DIR / BLPAPI_LIB_DIR / BLPAPI_ROOT first,"
note "then fall back to a vendored SDK or the Python blpapi package at runtime."

case ":$PATH:" in
    *":$INSTALL_DIR:"*)
        ;;
    *)
        note ""
        note "Add $INSTALL_DIR to PATH if it is not already available in your shell."
        ;;
esac
