# CI image for jobs that need Rust + Bloomberg SDK + bindgen (libclang).
# Rebuild and push via .github/workflows/ci-docker.yml when this file changes.
#
# LLVM 22 from apt.llvm.org (aligned with pixi.toml libclang >= 22). Local Pixi
# still uses conda-forge libclang; bindgen supports both.

FROM rust:1.99-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        git \
        python3 \
        python3-dev \
        pkg-config \
    && curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key \
        | gpg --dearmor -o /usr/share/keyrings/llvm-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/llvm-archive-keyring.gpg] https://apt.llvm.org/trixie/ llvm-toolchain-trixie-22 main" \
        > /etc/apt/sources.list.d/llvm-toolchain-22.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        clang-22 \
        libclang-22-dev \
        llvm-22-dev \
    && rm -rf /var/lib/apt/lists/*

# bindgen: libclang from LLVM 22 (llvm-config-22 --libdir → /usr/lib/llvm-22/lib).
ENV LIBCLANG_PATH=/usr/lib/llvm-22/lib
RUN test -e "${LIBCLANG_PATH}/libclang.so" || test -e "${LIBCLANG_PATH}/libclang.so.1" \
    || (echo "libclang not found under ${LIBCLANG_PATH}" >&2; exit 1) \
    && test "$(llvm-config-22 --libdir)" = "${LIBCLANG_PATH}"

RUN rustup component add clippy rustfmt
