# Bloomberg BLPAPI C++ SDK — Local Development

This directory is a local-development location only. The repository does not vendor Bloomberg SDK archives, headers, libraries, credentials, entitlements, or data rights. Use the helper scripts only if you are authorized to download and use the Bloomberg SDK under your Bloomberg agreements.

## Quick Start

Use the cross-platform `xbbg-sdk` CLI (works identically on macOS, Linux and Windows without pre-installed Bloomberg headers):

```bash
# Add the latest SDK version (auto-detected)
cargo run -p xbbg-sdk --features provision -- cpp

# Add a specific version
cargo run -p xbbg-sdk --features provision -- cpp --version <version>

# List installed versions
cargo run -p xbbg-sdk --features provision -- cpp --list

# Remove a version
cargo run -p xbbg-sdk --features provision -- cpp --remove <version>

# Re-download and re-extract
cargo run -p xbbg-sdk --features provision -- cpp --version <version> --force

# Free disk space by clearing cached archives
cargo run -p xbbg-sdk --features provision -- cpp --clean-cache
```

Once a package is installed, `xbbg-sdk cpp ...` (Python), `npx xbbg-sdk cpp ...` (Node), and `xbbg-mcp sdk cpp ...` expose the exact same subcommand.

## Layout

```
vendor/
└── blpapi-sdk/
    ├── README.md            # This file (tracked in git)
    ├── .cache/              # Downloaded archives (reusable)
    ├── 3.26.2.1/            # Extracted SDK
    │   ├── include/         # C/C++ headers (blpapi_*.h)
    │   ├── Darwin/          # macOS runtime libraries
    │   ├── Linux/           # Linux runtime libraries
    │   ├── lib/             # Windows import/runtime libraries
    │   ├── bin/             # Windows runtime DLLs + tools
    │   └── ...
    └── <another-version>/   # Another version (optional)
```

The build system (`crates/blpapi-sys/build.rs`) scans versioned subdirs under `BLPAPI_ROOT`
and picks the latest installed version automatically.

**Pixi users**: `pixi.toml` sets `BLPAPI_ROOT` via activation — no manual env setup needed.

## Notes

- Everything under `vendor/blpapi-sdk/` except this README is git-ignored.
- The official Python `blpapi` wheels already bundle the C++ runtime for supported platforms. Use the SDK here only for building from source or running C++ tooling, and only from an authorized source under your Bloomberg agreements.
- The helper scripts auto-detect the host OS and choose the Bloomberg archive format for that platform; they do not grant Bloomberg SDK redistribution rights or service access.
