# CI Container Images

This directory contains the Rust CI container image:

- `docker/ci/Dockerfile`: Rust CI image with toolchain + `libclang`

The manylinux image (AlmaLinux 8 / glibc 2.28) lives next to the Python
distribution at `py-xbbg/docker/manylinux/Dockerfile`. Every Linux release
artifact — wheels, the `@xbbg/core-linux-x64` addon, and the `xbbg-mcp`
binary — builds inside it so the result runs on any x86_64 distro with
glibc >= 2.28 (RHEL/Alma/Rocky 8, Debian 10+, Ubuntu 20.04+).

Bloomberg SDK files are intentionally **not** baked into container images.
CI downloads the SDK at runtime to avoid redistributing the SDK in a public image registry.

## Local usage with Podman

The Dockerfile is OCI-compatible, so you can build and run it with Podman.

### Build the image

```bash
podman build -f docker/ci/Dockerfile -t xbbg-ci:local .
```

### Generate `blpapi-sys` bindings artifact locally

```bash
mkdir -p target/ci-bindings

podman run --rm \
  -v "$PWD:/work" \
  -w /work \
  -e BLPAPI_BINDINGS_EXPORT_PATH=/work/target/ci-bindings/bindings.rs \
  xbbg-ci:local \
  bash -lc '
    BLPAPI_VERSION=${BLPAPI_VERSION:-3.26.2.1}
    bash ./scripts/sdktool.sh --version "$BLPAPI_VERSION" --no-set-active
    export BLPAPI_ROOT=/work/vendor/blpapi-sdk/$BLPAPI_VERSION
    export LD_LIBRARY_PATH=/work/vendor/blpapi-sdk/$BLPAPI_VERSION/Linux:$LD_LIBRARY_PATH
    cargo build -p xbbg-blpapi-sys
  '
```

### Validate clippy in the CI image

```bash
podman run --rm \
  -v "$PWD:/work" \
  -w /work \
  xbbg-ci:local \
  bash -lc '
    BLPAPI_VERSION=${BLPAPI_VERSION:-3.26.2.1}
    bash ./scripts/sdktool.sh --version "$BLPAPI_VERSION" --no-set-active
    export BLPAPI_ROOT=/work/vendor/blpapi-sdk/$BLPAPI_VERSION
    export LD_LIBRARY_PATH=/work/vendor/blpapi-sdk/$BLPAPI_VERSION/Linux:$LD_LIBRARY_PATH
    cargo clippy --workspace --all-targets -- -D warnings
  '
```

## Notes

- CI publishes images to `ghcr.io/<owner>/xbbg-ci` and `ghcr.io/<owner>/xbbg-manylinux`.
- `xbbg-ci:latest` is consumed by the binding-generation and Linux test jobs. `xbbg-manylinux:latest` is consumed by every Linux build leg in `ci-rust.yml`, `pypi_upload.yml`, `npm-publish.yml`, and `js_github_release.yml` to pin the glibc 2.28 floor, enforced by `scripts/check-glibc-max.sh` and `auditwheel repair --plat manylinux_2_28_x86_64`.
- Bloomberg SDK is downloaded in CI job steps (runtime), not stored in container layers.
- Windows jobs still run on native `windows-latest` runners and consume the generated bindings artifact.
