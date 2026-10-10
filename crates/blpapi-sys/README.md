# blpapi-sys

Unsafe, zero-policy FFI bindings to Bloomberg's C API (`blpapi_*`), generated at build time.

- `#![no_std]` — no runtime dependencies beyond the Bloomberg SDK
- Bindings auto-generated via bindgen from SDK headers; nothing checked in
- Exposes raw types, constants, and functions only — no wrappers, no ownership logic

## Crate structure

```
blpapi-sys/
├── Cargo.toml      Features (static/dynamic), bindgen + cty deps
├── build.rs        SDK discovery, bindgen generation, link directives
└── src/
    └── lib.rs      include!(bindings.rs) — 9 lines
```

## Linking

- Default: dynamic linking
- `--features static`: static linking
- Link name defaults:
  - Windows x64: `blpapi3_64`
  - Windows x86: `blpapi3_32`
  - Linux/macOS: `blpapi3`
## SDK discovery (precedence)

| Priority | Environment variable(s) | Notes |
|----------|------------------------|-------|
| 1 | `BLPAPI_INCLUDE_DIR` + `BLPAPI_LIB_DIR` | Explicit paths (CI/prod) |
| 2 | `BLPAPI_ROOT` | Derives `include/` and `lib/` from root; scans versioned subdirs |

The build script tries these layouts under a root directory:
- `<root>/include` + `<root>/lib`
- `<root>/include` + `<root>/lib/win64` (or `win32`)
- `<root>/Include` + `<root>/Lib`

Build requires SDK **headers** and the **import library**. The runtime DLL/so/dylib is not required at build time.

## Binding generation controls

By default, Cargo generates bindings with bindgen when the build inputs change.
There is no SDK-local bindings cache: header replacements and resolved bindgen
dependency changes must not reuse stale generated declarations.

- `BLPAPI_PREGENERATED_BINDINGS`: path to an existing `bindings.rs` file. When set, `build.rs` copies this file to `OUT_DIR` and skips bindgen.
- `BLPAPI_BINDINGS_EXPORT_PATH`: path where `build.rs` should also copy the effective bindings file (useful for CI artifact generation).

For cross-build reuse, export bindings once and pass that artifact explicitly
with `BLPAPI_PREGENERATED_BINDINGS`. The artifact must match the target, SDK
headers, and binding-generator configuration of the consuming build. Bindings
target the crate's `rust-version` and Rust 2024, not the compiler running the
build, so an artifact exported by a newer toolchain still compiles on the MSRV.

## Dev / CI usage

**Dev (quickest)**: run the SDK tool from the repo root, then build:

```bash
bash ./scripts/sdktool.sh
```

```powershell
.\scripts\sdktool.ps1            # downloads and extracts SDK
```

The build script scans versioned subdirs under `BLPAPI_ROOT`, so pointing it at
`vendor/blpapi-sdk/` automatically finds the latest installed version.

**Pixi users**: `pixi.toml` sets `BLPAPI_ROOT` via activation — just run `pixi run install`.

**Dev (manual)**: set `BLPAPI_ROOT` to the SDK root, then build the workspace.

**CI build**: set `BLPAPI_ROOT` or `BLPAPI_INCLUDE_DIR`/`BLPAPI_LIB_DIR`.

**CI runtime/tests**: install the official `blpapi` Python package and add its binary directory to the loader path:

```bash
uv pip install --index-url https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
```

- Windows (Py≥3.8): `os.add_dll_directory(<package_dir>)`
- Linux/macOS: add the package directory to `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`

## Safety

All APIs are `unsafe` and follow the C ABI. Nothing is marked `Send`/`Sync` unless guaranteed by the C SDK.
