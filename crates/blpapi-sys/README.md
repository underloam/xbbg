# blpapi-sys

Unsafe, zero-policy FFI bindings to Bloomberg's C API (`blpapi_*`), generated at build time and resolved from a library opened at run time.

- **Nothing links the Bloomberg runtime.** The build needs the SDK *headers*; the runtime library (`blpapi3_64.dll`, `libblpapi3_64.so`) is opened by absolute path while the process runs
- `Blpapi`: a struct of the C function pointers, built by `Blpapi::from_library`; call a function as `table.blpapi_Name_create(…)`
- `FUNCTION_NAMES`: the same function names as C strings, so the loader can name every function a runtime lacks
- Bindings are generated via bindgen from the SDK headers; nothing generated is checked in. The one checked-in input is `functions.txt`, the list of functions to generate
- Exposes raw types, constants and functions only: no wrappers, no ownership logic, no loader. `xbbg_core::load` opens the library and owns the process-wide table

## Crate structure

```
blpapi-sys/
├── Cargo.toml      bindgen (build), libloading + cty (runtime)
├── build.rs        SDK header discovery, bindgen generation
├── functions.txt   the BLPAPI functions the workspace calls, one per line
└── src/
    └── lib.rs      include!(bindings.rs)
```

## The function list

`functions.txt` names exactly the `blpapi_*` functions the workspace calls. `build.rs` allowlists those and nothing else, and generates the table with `dynamic_link_require_all`: `Blpapi::from_library` fails on the first function the library lacks. Listing only what is used means a runtime older than the headers this crate was built against still loads, as long as it has every function in use.

Adding a call to a new function means adding its name to `functions.txt`. `build.rs` fails, naming it, when a listed function is absent from the SDK headers or pregenerated bindings, which catches typos, stale bindings and functions an SDK removed. It also emits `FUNCTION_NAMES`, the names as `&CStr` values, for the loader's missing-function diagnostics.

## SDK discovery (precedence)

| Priority | Environment variable | Notes |
|----------|---------------------|-------|
| 1 | `BLPAPI_INCLUDE_DIR` | The directory holding the `blpapi_*.h` headers |
| 2 | `BLPAPI_ROOT` | Finds `include/` (or `Include/`) in the root, then in each versioned subdirectory one or two levels down, newest first |

The runtime library is never needed to build. Tests and benchmarks find it at run time with `xbbg_sdk::locate::locate` and open it with `xbbg_core::load`. Hosts use `xbbg_host::sdk::locate_and_load`, which also tries the operating system's own library search (`xbbg_core::load_system`) before the cache.

## Binding generation controls

By default, Cargo generates bindings with bindgen when the build inputs change.
There is no SDK-local bindings cache: header replacements, a changed `functions.txt` and resolved bindgen
dependency changes must not reuse stale generated declarations.

- `BLPAPI_PREGENERATED_BINDINGS`: path to an existing `bindings.rs` file. When set, `build.rs` copies this file to `OUT_DIR` and skips bindgen.
- `BLPAPI_BINDINGS_EXPORT_PATH`: path where `build.rs` should also copy the effective bindings file (useful for CI artifact generation).

For cross-build reuse, export bindings once and pass that artifact explicitly
with `BLPAPI_PREGENERATED_BINDINGS`. The artifact must match the target, SDK
headers, `functions.txt` and binding-generator configuration of the consuming build. Bindings
target the crate's `rust-version` and Rust 2024, not the compiler running the
build, so an artifact exported by a newer toolchain still compiles on the MSRV.

## docs.rs

This crate does not build on docs.rs: the SDK is proprietary and the docs.rs sandbox has no network to fetch its headers. `build.rs` detects `DOCS_RS` and fails with that explanation. Build the documentation locally with `BLPAPI_ROOT` set: `cargo doc -p xbbg-blpapi-sys`.

## Dev / CI usage

**Dev (quickest)**: run the `xbbg-sdk` CLI from the repo root (works on macOS, Linux, and Windows; requires no pre-installed Bloomberg headers), then build:

```bash
cargo run -p xbbg-sdk --features provision -- cpp
```

The build script scans versioned subdirs under `BLPAPI_ROOT`, so pointing it at
`vendor/blpapi-sdk/` automatically finds the latest installed version.

**Pixi users**: `pixi.toml` sets `BLPAPI_ROOT` via activation — just run `pixi run install`.

**Dev (manual)**: set `BLPAPI_ROOT` to the SDK root, then build the workspace.

**CI build**: set `BLPAPI_ROOT` or `BLPAPI_INCLUDE_DIR`.

**CI runtime/tests**: `BLPAPI_ROOT` (or `BLPAPI_LIB_DIR`, the directory holding the runtime) is all the Rust test binaries need; they load the library by path. No loader-path variable (`LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`, `PATH`) is involved.

## Safety

All APIs are `unsafe` and follow the C ABI. `Blpapi` owns the open library and holds only function pointers, so it is `Send + Sync`; nothing else is marked `Send`/`Sync` unless guaranteed by the C SDK.
