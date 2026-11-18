# xbbg-core

Safe, zero-allocation Rust wrappers over the Bloomberg C++ SDK (`blpapi`).

This is the core abstraction layer between raw FFI (`xbbg-blpapi-sys`) and the async engine (`xbbg-async`). Every unsafe FFI call is wrapped in a safe Rust API with proper ownership, lifetimes, and error handling.

## Crate structure

```
src/
├── lib.rs           Module declarations + public re-exports
├── loader.rs        Run-time loading of the Bloomberg runtime: load, sdk(), loaded_sdk_path
├── ffi.rs           FFI types re-exported from blpapi_sys + local datetime types
│
├── Core types
│   ├── element.rs   Element wrapper — typed field access, iteration
│   ├── message.rs   Message wrapper — correlation IDs, topic name
│   ├── event.rs     Event + MessageIterator
│   ├── name.rs      Name interning with FxHashMap cache
│   ├── value.rs     Dynamic Value enum (replaces JSON serialization)
│   ├── datatype.rs  DataType enum mapping Bloomberg type codes
│   └── datetime.rs  HighPrecisionDatetime ↔ Arrow timestamp conversion
│
├── Session API
│   ├── session.rs      Session lifecycle (create, start, stop, events)
│   ├── async_session.rs Callback-driven session lifecycle
│   ├── session_operations.rs Private shared FFI marshalling and error handling
│   ├── service.rs      Service wrapper (open, create request, schema)
│   ├── request.rs      Request builder + schema validation
│   ├── options.rs      SessionOptions (connection, tuning, keep-alive)
│   ├── subscription.rs SubscriptionList for real-time data
│   ├── correlation.rs  CorrelationId (Int or Pointer variants)
│   ├── identity.rs     Identity handle for authenticated sessions
│   └── errors.rs       BlpError enum with rich context
│
└── schema/
    ├── mod.rs          SchemaStatus enum + module re-exports
    ├── operation.rs    Operation introspection
    ├── element_def.rs  SchemaElementDefinition
    ├── type_def.rs     SchemaTypeDefinition
    └── constant.rs     Constant + ConstantList for enumerations
```

## Loading the Bloomberg runtime

Nothing links `libblpapi3_64`. The Bloomberg runtime is opened by absolute path while the process runs, and every BLPAPI call goes through the function table `sdk()` returns:

```rust
xbbg_core::load("/path/to/libblpapi3_64.so")?; // once, before any other call
```

| Function | |
|----------|---|
| `load(path)` | Opens the library (`RTLD_NOW \| RTLD_LOCAL`; on Windows `LOAD_WITH_ALTERED_SEARCH_PATH`) and resolves the function table. The path must be absolute. The same path again is a no-op; another path is `SdkLoadError::AlreadyLoaded`. The library is never unloaded |
| `sdk()` | The loaded table; panics, saying so, if nothing is loaded. One atomic load on the hot path |
| `loaded_sdk_path()` | The path that was loaded, or `None` |

Choosing the library path is the caller's job. Test binaries load the runtime before `main` (a `ctor` using `xbbg_sdk::locate::from_env`, a dev-dependency), so `BLPAPI_ROOT` or `BLPAPI_LIB_DIR` must point at a Bloomberg runtime when running `cargo test` or `cargo bench`.

## Features

| Feature | Description |
|---------|-------------|
| `live` (default) | The Bloomberg function table (`blpapi-sys`) and the loader (`libloading`) |
| `test-support` | SDK-backed synthetic message fixtures; no Bloomberg session needed |

The `live` feature is required to build this crate; it selects the SDK, not
whether tests connect to Bloomberg. `cargo test --workspace` runs offline tests
and leaves connection-dependent tests compiled but ignored. With a configured
Bloomberg session, run those tests with:

```sh
cargo test -p xbbg_core --test live -- --ignored
```

## Design principles

- **All FFI goes through `sdk()`** — one loaded function table; `ffi.rs` holds only the types
- **Zero-allocation hot paths** — direct typed access, no JSON serialization
- **Sub-microsecond field extraction** — SIMD-accelerated where available
- **Owned vs borrowed** — `Value<'a>` borrows from Element; `OwnedValue` for storage

## Benchmarks

All benchmarks have been consolidated into the `xbbg-bench` crate. See `crates/xbbg-bench/README.md`.
