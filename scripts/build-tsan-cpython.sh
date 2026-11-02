#!/usr/bin/env bash
# build-tsan-cpython.sh — Build a free-threaded CPython instrumented with ThreadSanitizer.
#
# The thread-sanitizer workflow loads xbbg's native extension into this
# interpreter. ThreadSanitizer only sees synchronization in instrumented code,
# so CPython itself is compiled with `--disable-gil --with-thread-sanitizer`.
# The extension carries rustc's instrumentation but resolves the TSan runtime
# from this interpreter, so CC must be the clang whose LLVM major matches the
# one `rustc -vV` reports.
#
# CPython's own suppression list for this exact release (its known interpreter
# reports) is installed as <prefix>/share/tsan/suppressions_free_threading.txt.
#
# Usage:
#   CC=clang-<llvm-major> ./scripts/build-tsan-cpython.sh <version> <sha256> <prefix>
#
# <sha256> is the digest python.org publishes for Python-<version>.tar.xz.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: CC=clang-<llvm-major> $0 <version> <sha256> <prefix>" >&2
    exit 2
fi
: "${CC:?set CC to the clang whose LLVM major matches rustc}"

version="$1"
sha256="$2"
prefix="$3"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

archive="$work/Python-$version.tar.xz"
curl -fsSL --retry 3 "https://www.python.org/ftp/python/$version/Python-$version.tar.xz" -o "$archive"
echo "$sha256  $archive" | sha256sum --check --strict -
tar -xJf "$archive" -C "$work"
src="$work/Python-$version"
suppressions="$src/Tools/tsan/suppressions_free_threading.txt"
test -s "$suppressions"

# `make` and `make install` run the new interpreter (bytecode compilation uses
# worker threads); its known reports must not fail the build.
export TSAN_OPTIONS="suppressions=$suppressions"

cd "$src"
# No test package and no bundled pip: the workflow manages packages with uv.
./configure \
    --prefix="$prefix" \
    --disable-gil \
    --with-thread-sanitizer \
    --disable-test-modules \
    --without-ensurepip
make -j"$(nproc)"
make install
install -D -m 644 "$suppressions" "$prefix/share/tsan/suppressions_free_threading.txt"

# configure skips extension modules whose system libraries are missing, and
# xbbg preloads the Bloomberg SDK through ctypes.
"$prefix/bin/python3" -c "import ctypes"
