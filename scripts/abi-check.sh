#!/usr/bin/env bash
# abi-check.sh — Verify Bloomberg SDK ABI compatibility across versions.
#
# Downloads headers for each specified SDK version, runs bindgen, and checks
# that every symbol xbbg-core depends on is present in the generated bindings:
# the functions in crates/blpapi-sys/functions.txt plus the types and constants
# xbbg-core/src/ffi.rs names.
#
# Usage:
#   ./scripts/abi-check.sh [--versions "3.24.6.1 3.25.11.1 3.26.2.1"]
#   ./scripts/abi-check.sh --baseline vendor/blpapi-sdk/3.26.2.1/include
#
# Exit codes:
#   0  All required symbols present in every SDK version.
#   1  One or more required symbols missing in at least one version.
#   2  Script error (download failure, missing tools, etc.)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FFI_RS="$REPO_ROOT/crates/xbbg-core/src/ffi.rs"
FUNCTIONS_TXT="$REPO_ROOT/crates/blpapi-sys/functions.txt"
DEFS_TOML="$REPO_ROOT/defs/bloomberg.toml"
WORK_DIR=""
VERSIONS=""
BASELINE_INCLUDE=""
DOWNLOAD_BASE_URL="https://blpapi.bloomberg.com/download/releases/raw/files"

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
die()  { printf 'error: %s\n' "$1" >&2; exit 2; }
info() { printf ':: %s\n' "$1"; }
warn() { printf 'WARNING: %s\n' "$1" >&2; }

cleanup() {
    if [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ]; then
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# --------------------------------------------------------------------------- #
# Parse arguments
# --------------------------------------------------------------------------- #
while [ $# -gt 0 ]; do
    case "$1" in
        --versions)
            [ $# -ge 2 ] || die "--versions requires a value"
            VERSIONS="$2"; shift 2 ;;
        --baseline)
            [ $# -ge 2 ] || die "--baseline requires a path"
            BASELINE_INCLUDE="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/s/^# \?//p' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
if [ -z "$VERSIONS" ] && [ -z "$BASELINE_INCLUDE" ]; then
    # Read minimum supported SDK version from defs/bloomberg.toml (single source of truth).
    MIN_SDK=$(grep '^min_sdk_version' "$DEFS_TOML" | sed 's/.*= *"\(.*\)"/\1/')
    [ -n "$MIN_SDK" ] || die "Could not read min_sdk_version from $DEFS_TOML"
    VERSIONS="$MIN_SDK"
fi

require_cmd grep
require_cmd sort
require_cmd comm
require_cmd bindgen
require_cmd perl

# --------------------------------------------------------------------------- #
# Step 1: Extract required symbols
# --------------------------------------------------------------------------- #
info "Extracting required symbols from functions.txt and ffi.rs ..."

# The functions are the allowlist the sys crate generates its function table
# from: one blpapi_* name per line, blank lines and `#` comments skipped.
#
# ffi.rs holds no function re-exports any more. It re-exports types and
# constants via `pub use blpapi_sys::{ ... }` and defines struct/const types.
#
# Strategy:
#   1. Strip line comments (// ...) to avoid matching header filenames etc.
#   2. Strip the `blpapi_sys::` module qualifier. The sys crate's lib name itself
#      matches the `blpapi_*` token pattern below, so leaving it in would report a
#      phantom required symbol named `blpapi_sys` that no SDK header can provide.
#      (This crate used to be imported as `xbbg_sys`, which the pattern missed.)
#   3. Find Rust-side aliases (tokens after `as` keyword) to exclude.
#   4. Extract all blpapi_*/BLPAPI_* tokens.
#   5. Remove the aliases — they are Rust names, not C symbols.

extract_required_symbols() {
    local stripped
    # NB: literal `blpapi_sys::`, not `\bblpapi_sys::` — `\b` is not portable
    # across the sed implementations this script runs under.
    stripped=$(sed -e 's|//.*||' -e 's|blpapi_sys::||g' "$FFI_RS")

    # Find Rust-side aliases: the identifier after `as` in `X as Y`
    local aliases_file
    aliases_file=$(mktemp)
    echo "$stripped" | \
        perl -ne 'while (/\bas\s+(blpapi_\w+|BLPAPI_\w+)/g) { print "$1\n" }' \
        | sort -u > "$aliases_file"

    # Extract all blpapi_*/BLPAPI_* tokens, then remove aliases
    local all_file
    all_file=$(mktemp)
    echo "$stripped" | \
        grep -oE '\b(blpapi_[A-Za-z0-9_]+|BLPAPI_[A-Z0-9_]+)\b' \
        | sort -u > "$all_file"

    # Subtract aliases from the full set
    comm -23 "$all_file" "$aliases_file"

    rm -f "$aliases_file" "$all_file"
}

REQUIRED_SYMBOLS_FILE=$(mktemp)
{
    extract_required_symbols
    grep -vE '^[[:space:]]*(#|$)' "$FUNCTIONS_TXT"
} | sort -u > "$REQUIRED_SYMBOLS_FILE"

required_count=$(wc -l < "$REQUIRED_SYMBOLS_FILE" | tr -d ' ')
info "Found $required_count required symbols."

if [ "$required_count" -eq 0 ]; then
    die "No symbols extracted from functions.txt and ffi.rs — extraction logic is broken."
fi

# --------------------------------------------------------------------------- #
# Step 2: Generate bindings and extract symbols
# --------------------------------------------------------------------------- #

# Bindgen wrapper: takes an include dir, writes a sorted symbol list to stdout.
generate_symbol_list() {
    local include_dir="$1"

    # Build the wrapper header (same logic as build.rs)
    local wrapper=""
    for header in "$include_dir"/blpapi_*.h; do
        [ -f "$header" ] || continue
        wrapper+="#include <$(basename "$header")>"$'\n'
    done

    if [ -z "$wrapper" ]; then
        warn "No blpapi_*.h headers found in $include_dir"
        return 1
    fi

    local wrapper_file
    wrapper_file=$(mktemp)
    printf '%s' "$wrapper" > "$wrapper_file"

    local bindings_file
    bindings_file=$(mktemp)

    # -x c forces C language mode. The Bloomberg headers contain C++ code
    # guarded by #ifdef __cplusplus; the C FFI surface is what we need.
    if ! bindgen "$wrapper_file" \
        --allowlist-function '^blpapi_.*' \
        --allowlist-type '^blpapi_.*' \
        --allowlist-var '^(BLPAPI_.*|g_blpapi.*)' \
        --ctypes-prefix cty \
        --use-core \
        --no-layout-tests \
        --no-derive-default \
        --no-doc-comments \
        -- -x c -I"$include_dir" \
        > "$bindings_file" 2>/dev/null; then
        warn "bindgen failed for $include_dir"
        rm -f "$wrapper_file" "$bindings_file"
        return 1
    fi

    # Extract all public symbol names from the generated bindings
    grep -oE '\b(blpapi_[A-Za-z0-9_]+|BLPAPI_[A-Z0-9_]+)\b' "$bindings_file" \
        | sort -u

    rm -f "$wrapper_file" "$bindings_file"
}

# --------------------------------------------------------------------------- #
# Step 3: Download SDK headers and check each version
# --------------------------------------------------------------------------- #
WORK_DIR=$(mktemp -d)
failures=0
versions_checked=0

check_version() {
    local version="$1"
    local include_dir="$2"
    local label="$3"

    info "Checking $label ..."

    local symbols_file="$WORK_DIR/symbols-${version}.txt"
    if ! generate_symbol_list "$include_dir" > "$symbols_file"; then
        warn "Failed to generate symbols for $label"
        failures=$((failures + 1))
        return
    fi

    local symbols_count
    symbols_count=$(wc -l < "$symbols_file" | tr -d ' ')
    info "  $symbols_count symbols in bindings."

    # Find required symbols missing from this version's bindings
    local missing_file="$WORK_DIR/missing-${version}.txt"
    comm -23 "$REQUIRED_SYMBOLS_FILE" "$symbols_file" > "$missing_file"

    local missing_count
    missing_count=$(wc -l < "$missing_file" | tr -d ' ')

    if [ "$missing_count" -gt 0 ]; then
        warn "  $missing_count required symbol(s) MISSING in $label:"
        sed 's/^/    - /' "$missing_file" >&2
        failures=$((failures + 1))
    else
        info "  All required symbols present."
    fi

    versions_checked=$((versions_checked + 1))
}

download_and_check_version() {
    local version="$1"
    local version_dir="$WORK_DIR/sdk-$version"

    # Check if already vendored locally
    local vendored="$REPO_ROOT/vendor/blpapi-sdk/$version/include"
    if [ -d "$vendored" ]; then
        info "Using vendored SDK $version"
        check_version "$version" "$vendored" "SDK $version (vendored)"
        return
    fi

    # Download Linux tarball (headers are platform-independent)
    local archive_name="blpapi_cpp_${version}-linux.tar.gz"
    local archive_url="$DOWNLOAD_BASE_URL/$archive_name"
    local archive_path="$WORK_DIR/$archive_name"

    info "Downloading SDK $version headers ..."
    if ! curl -fsSL "$archive_url" -o "$archive_path" 2>/dev/null; then
        warn "Failed to download $archive_url"
        failures=$((failures + 1))
        return
    fi

    mkdir -p "$version_dir"
    tar -xzf "$archive_path" -C "$version_dir" --strip-components=1

    local include_dir="$version_dir/include"
    if [ ! -d "$include_dir" ]; then
        # Some older SDK layouts nest differently
        include_dir=$(find "$version_dir" -type d -name include | head -1)
        if [ -z "$include_dir" ]; then
            warn "No include/ directory found in SDK $version"
            failures=$((failures + 1))
            return
        fi
    fi

    check_version "$version" "$include_dir" "SDK $version (downloaded)"
}

# Process --baseline first if given
if [ -n "$BASELINE_INCLUDE" ]; then
    if [ ! -d "$BASELINE_INCLUDE" ]; then
        die "Baseline include directory does not exist: $BASELINE_INCLUDE"
    fi
    check_version "baseline" "$BASELINE_INCLUDE" "baseline ($BASELINE_INCLUDE)"
fi

# Process each version
for version in $VERSIONS; do
    download_and_check_version "$version"
done

# --------------------------------------------------------------------------- #
# Step 4: Cross-version diff summary
# --------------------------------------------------------------------------- #
if [ "$versions_checked" -ge 2 ]; then
    info ""
    info "=== Cross-version symbol diff ==="

    # Collect all symbol files
    symbol_files=()
    version_labels=()
    for f in "$WORK_DIR"/symbols-*.txt; do
        [ -f "$f" ] || continue
        symbol_files+=("$f")
        label=$(basename "$f" .txt | sed 's/^symbols-//')
        version_labels+=("$label")
    done

    if [ "${#symbol_files[@]}" -ge 2 ]; then
        # Compare first vs last (oldest vs newest)
        oldest="${symbol_files[0]}"
        newest="${symbol_files[${#symbol_files[@]}-1]}"
        oldest_label="${version_labels[0]}"
        newest_label="${version_labels[${#version_labels[@]}-1]}"

        added_file="$WORK_DIR/added.txt"
        removed_file="$WORK_DIR/removed.txt"
        comm -13 "$oldest" "$newest" > "$added_file"
        comm -23 "$oldest" "$newest" > "$removed_file"

        added_count=$(wc -l < "$added_file" | tr -d ' ')
        removed_count=$(wc -l < "$removed_file" | tr -d ' ')

        info "  $oldest_label -> $newest_label:"
        info "    +$added_count symbols added"
        info "    -$removed_count symbols removed"

        if [ "$removed_count" -gt 0 ]; then
            info ""
            info "  Symbols removed between $oldest_label and $newest_label:"
            sed 's/^/    - /' "$removed_file"
        fi

        if [ "$added_count" -gt 0 ] && [ "$added_count" -le 50 ]; then
            info ""
            info "  Symbols added between $oldest_label and $newest_label:"
            sed 's/^/    + /' "$added_file"
        elif [ "$added_count" -gt 50 ]; then
            info ""
            info "  ($added_count symbols added -- showing first 20)"
            head -20 "$added_file" | sed 's/^/    + /'
        fi
    fi
fi

# --------------------------------------------------------------------------- #
# Final result
# --------------------------------------------------------------------------- #
info ""
if [ "$failures" -gt 0 ]; then
    info "FAIL: $failures version(s) have missing or incompatible symbols."
    exit 1
else
    info "PASS: All required symbols present in $versions_checked version(s)."
    exit 0
fi
