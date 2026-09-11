# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Explicit resource limits and ownership controls.** Engines expose `runtime_worker_threads` / `runtimeWorkerThreads` (default 2) and `max_subscription_sessions` / `maxSubscriptionSessions` (default 32). Native Arrow carriers expose `compact()` to detach retained slices from larger backing allocations when the caller prefers a copy over retaining the parent.
- **Independent application and model result budgets.** LangGraph tools bound application artifacts and model content separately, including aggregate node and streaming-materialization limits. MCP adds cell, UTF-8 string, metadata, and final-result byte budgets alongside its row limit; bounded envelopes retain primary diagnostics and report omissions rather than silently presenting partial data as complete.

### Changed

- **BREAKING — subscriptions preserve sparse deltas and fail closed on continuity gaps.** Missing fields are no longer fabricated as null clears. Arrow output adds non-null binary `__xbbg_present` with schema-mapped presence bits; JavaScript accessors return `undefined` for absent fields and `null` for explicit clears. Bloomberg `DATALOSS` and local overflow/forwarding timeout now end the affected stream with `BlpSubscriptionDataLossError`; resubscribe for a fresh image.
- **Streaming resources are bounded across the Python bridge, SDK sessions, and consumer queues.** Synchronous Python streams share a managed event loop with bounded per-stream buffering and producer admission. Subscription sessions use explicit leases and a hard cap; full consumer queues no longer make the SDK callback wait. Status changes are published in batches instead of cloning snapshots for each topic transition.
- **Arrow conversion does less unnecessary allocation and materialization.** Small subscription batches use right-sized builders; logical slices and validity buffers are normalized before handoff. Node Arrow construction avoids IPC encoding/decoding while exposing exclusively JS-owned mutable buffer snapshots, rather than writable aliases of immutable Rust storage. BQL scalar deserialization no longer requires an intermediate JSON value tree.
- **Optional dataframe backends preserve ownership and deferred work.** DuckDB relations share one process-local database while retaining independent connections and source lifetimes; an initialized database is explicitly rejected after `fork`. Polars conversion preserves Arrow chunk boundaries, and `narwhals_lazy` now uses a real Polars lazy plan for local dataframe operations. The supported Polars floor is `0.20.4`, with its timezone extra supplying Windows timezone data.
- **Recipes avoid repeated work without changing financial results.** Futures metadata lookups are combined, independent ETF request legs can overlap, and dividend windows advance over active events while preserving boundary rules and floating-point results.
- **Large MCP serialization uses bounded blocking work.** The MCP runtime uses two async workers and admits at most two offloaded serializations, keeping large result conversion off the async workers without creating an unbounded work queue.
- **Benchmark results now describe the work actually measured.** Cold/setup, warm timing, consumption, and memory observation are separated; percentile output requires enough samples (20 for p95, 100 for p99). Replay loss accounting and comparison inputs are checked. Native build provenance comes from Cargo's actual artifact and compiler settings, includes the SDK link input and artifact hashes, and rejects mismatched or host-tuned artifacts during portable release staging.
- **README badges are grouped by ecosystem.** The header now shows a Python / JavaScript / Rust table with version, downloads, and runtime badges per surface, adds PyPI downloads per month, and adds crates.io (`xbbg_core`) alongside PyPI and npm. The conda-forge badge moved to shields.io for a consistent style, and the project links list now includes npm and crates.io.

### Fixed

- **Subscription extraction preserves valid timestamps and schema transitions.** The `LAST_UPDATE_ALL_SESSIONS_RT` null guard is removed; valid time-only values use the SDK getter. All-fields caches revalidate datatype and shape, requested arrays/complex fields fail instead of truncating, non-null decode failures are errors, and JavaScript promoted string layouts retain concrete scalar values.
- **Terminal delivery and draining are reliable across Rust, Python, and JavaScript.** Full queues cannot displace the first terminal error; committed data is followed by that error and EOF, including shutdown after remove-all. Draining preserves the selected dict/raw/backend or scalar/Arrow representation and raises unread failures after cleanup. Explicit Python `output` overrides legacy mode flags; partial-topic failures remain nonterminal.
- **Subscription shutdown preserves cancellation and errors across bindings.** Pending reads, iterator `return()` / early `for await` exit, abort signals, drain barriers, and shared scalar/Arrow views coordinate one native close. Failed closes—including an undefined JavaScript rejection reason—remain failures and release completed session claims. Ordinary engine shutdown is distinct from interpreter finalization, which suppresses unsafe Python completion callbacks.
- **Configuration and caches no longer race their owners.** Scoped Python engines use context-local tokens; configuration replacement and field-cache generations are coordinated. Schema caches and readers are bounded, publications are ordered and atomic, service filenames are collision-free, and targeted invalidation reports failed persistence. Windows publication uses shared-handle POSIX rename semantics so concurrent readers retain complete snapshots; unsupported filesystems fail without an unsafe replacement fallback.
- **Native Arrow edge cases preserve data.** Zero-column tables retain their row counts through construction, renaming, and Narwhals row access. Mixed numeric inference does not round inexact large integers through `float64`, and native temporal schemas retain their precision and timezone through Narwhals and legacy Polars conversion.

## [1.4.11] - 2026-09-02

### Fixed

- **`xbbg-mcp` no longer mistakes a failed stdin worker or abnormal service stop for clean EOF.** Stderr diagnostics are now best-effort instead of using `eprintln!`, whose panic on a closed host log pipe could kill the detached reader and recreate the silent exit from #348. The reader reports its terminal state out of band before closing the transport; read failures, worker panics, unexpected transport closure, task failures, cancellation, and reader-thread spawn failures now reach `main` as errors and return a nonzero process status.
- **The Windows MCPB launcher now guarantees that the Bloomberg DLL it validates is the one the loader can select.** The chosen runtime directory becomes the child's working directory and first `PATH` entry, while a higher-priority `blpapi3_64.dll` beside the executable or in a Windows system directory is rejected rather than silently shadowing the checked file. Candidates with unreadable version metadata are skipped; packaging fails closed when the binary exposes no versioned Bloomberg imports; `--min-blpapi-version` accepts only a canonical three- or four-part numeric version; and loader-status messages no longer blame a specific module without evidence.
- **MCPB release artifacts are now reproducible and built with integrity-locked tooling.** A locked `fflate` 0.8.2 compressor writes sorted members with fixed timestamps and explicit Unix modes, so rerunning the same tag with identical binaries preserves both the executable launchers and the MCPB SHA-256 recorded in the official registry. The workflow also installs exactly `@anthropic-ai/mcpb` 2.1.2 from the committed npm lockfile, overrides its vulnerable transitive `tmp` dependency with patched 0.2.7, disables lifecycle scripts, runs the packer regression tests, and uses the pinned CLI to validate and inspect the deterministic archive instead of executing mutable `latest` code.

## [1.4.10] - 2026-09-01

### Fixed

- **The 1.4.9 Windows MCPB launcher refused every Bloomberg runtime older than 3.26.7.1, including current Python `blpapi` packages.** The runtime gate introduced in 1.4.9 was seeded with the SDK version the release was *built* against rather than the oldest runtime that exports the entry points the binary imports, so the bundle rejected runtimes such as `blpapi` 3.26.4.2 that work. `scripts/package_xbbg_mcpb.py` now derives the minimum from the binaries themselves -- the newest `BLPAPI_x.y.z` symbol version referenced by the Linux build, currently 3.20.0, which is the same function set the Windows binary imports by name -- and accepts `--min-blpapi-version` only as an explicit override. Every `blpapi` package on Bloomberg's index from 3.21.0 onward passes; a Terminal DLL older than 3.20.0 is still skipped with a message naming its version, because the loader would kill the process without one.

## [1.4.9] - 2026-09-01

### Fixed

- **`xbbg-mcp` no longer stops silently when stdin misbehaves, and the Windows MCPB launcher stays out of the byte stream (#348).** On one Windows host the v1.4.6 server answered `initialize` and exited a moment later with status 0 and nothing on stderr: `tokio::io::stdin()` reports any zero-byte read as end-of-input and turns read errors into a bare "connection closed", and no log subscriber was installed, so nothing recorded why. The server now reads stdin on its own thread -- a zero-byte read on a pipe whose writer is still connected (`PeekNamedPipe`) is retried instead of ending the session, a failed read is reported on stderr, and the process states why it stops (`xbbg-mcp: stdin closed; shutting down`, or the abnormal quit reason). It also installs the workspace stderr logger, so `RUST_LOG` works and `rmcp`/engine warnings reach the host's server log. The Windows launcher (`xbbg-mcp.ps1`) ran `xbbg-mcp.exe` as a PowerShell native command, which lets Windows PowerShell 5.1 sit between the MCP host and the server -- re-encoding stdout through the console code page and, under the script's `$ErrorActionPreference = "Stop"`, terminating the child on its first stderr line. It now starts the binary with inherited standard handles and propagates its exit status, the Windows equivalent of the POSIX launcher's `exec`. The reported machine state did not reproduce on Windows 11 through Claude Desktop's own client library, a Node parent, a console, or a bare pipe, with either the shipped v1.4.6 binary or this build; the fix therefore closes every path by which stdin could end the process without a message rather than a single guessed cause.
- **The Windows MCPB launcher checks the Bloomberg runtime it picks (#348).** It accepted any directory holding `blpapi3_64.dll` *or* `blpapi3_32.dll`, so pointing `XBBG_MCP_LIB_DIR` at a Terminal's `C:\blp\DAPI` passed and the 64-bit binary then died in the loader with `STATUS_ENTRYPOINT_NOT_FOUND` and no output. It now requires `blpapi3_64.dll`, compares its file version with the Bloomberg API version the release was built against (`scripts/package_xbbg_mcpb.py --blpapi-sdk-version`, supplied by the release workflow), skips older runtimes with a message naming the version found, searches `PATH` as well (the Windows convention; a Terminal install puts `C:\blp\DAPI` there) before falling back to the Python `blpapi` package, and translates the loader statuses `STATUS_DLL_NOT_FOUND`, `STATUS_ENTRYPOINT_NOT_FOUND`, and `STATUS_INVALID_IMAGE_FORMAT` into one-line explanations.
- **The MCPB `manifest.json` lists all nine tools (#348).** `check_entitlements` was missing from the manifest's `tools` array and from `scripts/xbbg_mcp_smoke.py`'s expected set even though the server advertised it.
- **v1.4.7 and v1.4.8 shipped no PyPI, npm, or MCP assets.** Both tags were cut, and their Rust crates published, without the PyPI and npm publish workflows being dispatched, so v1.4.6 stayed the newest installable release everywhere but crates.io. This release publishes every surface.

## [1.4.8] - 2026-09-01

### Added

- **Closed value sets now declared in `defs/bloomberg.toml`**: Overflow policies, validation modes, and SDK log levels are now centralized in the configuration file as the single source of truth. `defs/codegen/generate.py` generates a TypeScript vocabulary (`_defs_gen.ts`) for both JavaScript packages and Rust contract tests that fail if a hand-written parser stops accepting a declared spelling.

### Changed

- **Documentation and error messages now name complete legal value sets and defaults**: Surface layers across Python (`pyo3-xbbg`), JavaScript (N-API), the MCP server, API docstrings, and READMEs now document every accepted value for closed enums (overflow policy, validation mode, SDK log level, subscription output, request format, auth method, ZFP remote) alongside the default and any accepted aliases.

### Fixed

- **`@xbbg/core` exported a format wire value the engine rejects**: `Format.LONG_WITH_METADATA` was `'long_with_metadata'`, but the engine only accepts `'long_metadata'` (`defs/bloomberg.toml` and the generated Python enum already used the correct value, so only the hand-written JavaScript constant was wrong). `js-xbbg/test/smoke.test.ts` asserted the broken value, which is why it went unnoticed. `Format` and `FormatKind` are now generated from `defs/bloomberg.toml`.
- **LangGraph snapshot tools advertised a rejected overflow policy**: `overflowPolicy` on `xbbg_stream_snapshot`, `xbbg_mktbar_snapshot`, and `xbbg_depth_snapshot` was a free-form string whose description named no legal values and whose example was the `drop_oldest` policy removed in 1.2.0, so models emitted it and the engine failed the call with `unknown overflow policy 'drop_oldest'`. It is now a closed enum generated from `defs/bloomberg.toml`, and its description carries the accepted values, their semantics, and the default.


## [1.4.7] - 2026-08-28

### Added

- **Rust crates now publish automatically on release.** `.github/workflows/crates-publish.yml` publishes the six public crates to crates.io in dependency order, authenticating tokenlessly through `rust-lang/crates-io-auth-action` (GitHub OIDC), so no `CARGO_REGISTRY_TOKEN` secret exists. `semantic_version.yml` invokes it as a `workflow_call` job rather than relying on a tag trigger, because a tag pushed with `GITHUB_TOKEN` does not start tag-triggered workflows -- the documented reason the PyPI and npm workflows still need a manual dispatch. The job skips any version already in the index, so a retry after a partial failure is safe, and it stamps `workspace.package.version` plus every internal `[workspace.dependencies]` entry from the release version so all crates ship in lockstep.

### Changed

- **BREAKING -- optional-backend and `narwhals` floors now reflect versions that actually work.** The previous lower bounds were never resolved or exercised by CI, and three of them named combinations that cannot function. `narwhals` moves from `>=2.0` to `>=2.10.0` -- it is the only mandatory runtime dependency, and only 2.10.0 onward honours the `narwhals.plugins` entry point xbbg registers, so on 2.0-2.9 the plugin never loads and any conversion raises `TypeError: Expected pandas-like dataframe, Polars dataframe, or Polars lazyframe, got: <class 'xbbg._core.ArrowTable'>`. The `pandas` extra moves from `>=2.0` to `>=2.2.2,<4`, because pandas 2.0.x declares no `numpy<2` bound, so a fresh resolve pairs it with numpy 2.x and pandas then fails to import with `numpy.dtype size changed, may indicate binary incompatibility`; 2.2.2 is the first release supporting numpy 2. The `duckdb` extra moves from `>=1.0` to `>=1.5.0`, because below 1.5.0 the connection backing a returned relation is lost (`Connection has already been closed`), and 1.0.0 additionally cannot register an Arrow PyCapsule object at all. `pytest-cov` in the `test` extra gains a `>=5.0` bound; unpinned, a lowest-direct resolve selected pytest-cov 2.0.0 from 2018. `polars>=0.20` and `pyarrow>=22.0.0` were verified as genuinely working and are unchanged. Environments already on current releases are unaffected; anyone pinned below these bounds must upgrade.
- **Declared floors are now verified by CI.** A new `floors` job resolves the project with `uv pip install --resolution lowest-direct` on Python 3.10 -- the lowest supported interpreter, and the only one where the oldest wheels are still selectable -- and runs the full non-live suite against the result. Each floor in `pyproject.toml` carries a comment recording the measured reason it cannot go lower.
- **Rust MSRV is declared.** `[workspace.package]` now sets `rust-version = "1.88"`, matching the highest `rust-version` in the resolved dependency graph, and `pixi.toml` raises its `rust` floor from `>=1.75` to `>=1.88` to agree. The published sdist compiles the Rust extension on the user's machine, so the toolchain requirement is part of the package contract rather than a local development detail; `>=1.75` had been unsatisfiable for several majors.
- **Third-party Rust versions are centralized in `[workspace.dependencies]`.** The arrow family alone was restated across eight manifests and `tokio` across five, and the disabled `xbbg-cli` / `dotnet-xbbg` manifests had already drifted to `arrow 57.1.0` and a `csbindgen 2` that has never been published. Members now inherit with `{ workspace = true }` and layer only their own `features`. `default-features` is set in the workspace table because Cargo silently ignores a member's `default-features = false` when the workspace entry omits it. The migration is feature-neutral: resolved feature sets were diffed per package before and after.
- **`rmcp` upgraded from 2.1.0 to 3.1.2** in `xbbg-mcp`, with no source changes required. Both versions default `ProtocolVersion::LATEST` to MCP `2025-11-25`, so the advertised revision is unchanged; 3.x additionally negotiates the `2026-07-28` draft.
- **Internal crate versions are centralized too, and now carry a `version`.** Every intra-workspace dependency was a bare `{ path = ... }`. `cargo publish` rejects a path dependency with no version, so no crate with an internal dependency could be published at all -- the actual reason the Rust crates sat at 1.1.2 while the project tagged v1.4.6. The internal crates now sit in `[workspace.dependencies]` as `{ path, version }` and members inherit with `{ workspace = true }`, matching the convention already used for third-party crates. Because those entries set `default-features = false`, `xbbg-mcp` and `xbbg-bench` now name `features = ["live"]` explicitly where they previously inherited it via default features.
- **crates.io package metadata is complete and points at the project, not a personal account.** All five published crates had `repository` and `homepage` pointing at a legacy personal GitHub account and its `github.io` pages site; both now resolve to the `xbbg-org` repository and `https://xbbg.org/`. `main` had also dropped the `homepage`, `readme`, and `keywords` that 1.1.2 actually shipped. Every published crate now declares `homepage`, `readme`, `keywords`, `categories`, and `rust-version` (crates.io reported the MSRV as unset for all of them), plus an explicit `include` allowlist so only source, `Cargo.toml`, and `README.md` are ever packaged.
- **`blpapi-sys` is renamed to `xbbg-blpapi-sys` to match the name it is published under.** The bare `blpapi-sys` name is taken on crates.io, so the local package name never matched the registry name and `cargo publish -p xbbg-blpapi-sys` could not resolve. `[lib] name = "blpapi_sys"` is retained, so Rust code still says `blpapi_sys::` and the change is source-compatible with the published 1.1.2.
- **`exchanges.toml` moved from `defs/` into `crates/xbbg-ext/data/`.** `xbbg-ext` embeds it with `include_str!`, and the path reached outside the crate root, so the published sdist would not have contained the file and the crate would have been unbuildable from crates.io. `xbbg-ext` was its only consumer.

### Removed

- **`xbbg-sys` is deleted.** It was 16 lines that re-exported `blpapi_sys::*` behind a mandatory `live` feature, with a `compile_error!` on every other configuration -- a leftover seam from the removed mock backend. `xbbg_core` now depends on `xbbg-blpapi-sys` directly, and the `compile_error!` guard moves to `xbbg_core/src/lib.rs` so a non-`live` build still fails with one clear message instead of dozens of unresolved imports. The crate should also be deleted on crates.io, which is possible only after a release where `xbbg_core` no longer depends on it.
- **`apps/xbbg-cli` and `bindings/dotnet-xbbg` are deleted.** Both were stubs of 16 and 10 lines, commented out of the workspace members, and had already drifted to dependency versions that do not resolve.

### Fixed

- **Engine teardown no longer panics when the engine is dropped inside an async context.** `Engine` owns its tokio runtime, and tokio refuses to release a runtime's last handle from inside another runtime because teardown must block to join worker threads. Any async owner therefore aborted with `Cannot drop a runtime in a context where blocking is not allowed` -- including `xbbg-mcp`, which holds the engine across `#[tokio::main]` and so panicked on every clean shutdown once an engine had started. `Engine::drop` now takes the runtime out of its field and hands it to `Runtime::shutdown_background()` when a runtime is current, which is race-free because the field is left empty and no second release path can reach a zero refcount. Outside an async context the previous blocking drop is retained. The live integration test that used `std::mem::forget` to dodge this panic -- and leaked the runtime doing so -- now drops the engine normally.
- **Engine startup failures report the real error instead of a runtime-teardown panic.** `Engine::start` built its tokio runtime before constructing the worker pools, so any early return dropped the runtime on an async caller's thread and replaced the underlying failure with tokio's unrelated panic message. With a Bloomberg terminal unavailable, `Engine::start` now surfaces `failed to spawn worker 0: session start failed` rather than `Cannot drop a runtime in a context where blocking is not allowed`. The runtime is constructed after every fallible step.
- **Three high-severity advisories cleared from the JavaScript dev dependency trees** (`postcss` GHSA-fxqj-rqcc-2cmp / GHSA-r28c-9q8g-f849, `nanoid` GHSA-28wg-ghj8-5hjv / GHSA-2v37-7h3g-55p8, `brace-expansion` GHSA-mh99-v99m-4gvg / GHSA-rgw5-rvv9-x895), plus `cryptography` GHSA-g6cj-pr64-35w5 and `c-ares` CVE-2026-33630 in the pixi environment. These are build- and test-time dependencies only and are not redistributed in any published artifact.
- **Stale `pyo3` advisory ignores removed** from `.cargo/audit.toml` and `deny.toml`. They suppressed `RUSTSEC-2026-0176` and `RUSTSEC-2026-0177` on the premise that the fix required a pyo3 0.29 chain that dependencies did not yet support; the workspace has since resolved to pyo3 0.29.2 exclusively, so the entries were unreachable and would have masked a future regression onto a vulnerable pyo3.
- **`blpapi-sys` no longer advertises a documentation build it cannot perform.** Its `docs.rs` metadata requested `all-features = true` while `build.rs` panics when the mutually exclusive `static` and `dynamic` linkage features are both enabled, which also broke `cargo clippy --all-features` for the whole workspace. The metadata now names the default `dynamic` linkage.
- **`xbbg_core` keeps its underscore, permanently.** crates.io normalizes `-` and `_` to the same package identity, so `xbbg-core` is not an available name while `xbbg_core` exists, and claiming it would require deleting the crate and waiting out the name-reuse block. The manifest documents this so the inconsistency is not "corrected" later.

## [1.4.6] - 2026-08-06

### Changed

- **Linux artifacts now support glibc 2.28 and newer**: wheels, the `@xbbg/core-linux-x64` native addon, and the `xbbg-mcp` linux-amd64 binary build inside the manylinux_2_28 (AlmaLinux 8) container instead of on bare `ubuntu-latest`, so they run on RHEL/Alma/Rocky 8, Debian 10+, Ubuntu 20.04+, and Amazon Linux 2023. Linux wheels are tagged `manylinux_2_28_x86_64` (enforced via `auditwheel repair --plat`), and the new `scripts/check-glibc-max.sh` guard fails CI and release builds if any Linux binary references newer GLIBC symbol versions.
- **`lookback_days` on `active_cdx` is now a minimum, not the whole window**: the activity window always reaches back to the resolved series' first accrual date, so "this series has traded" can only flip false to true as the date advances.

### Removed

- **BREAKING -- `**kwargs` on `xbbg.ext.acdx_ticker` / `aactive_cdx`**: these forwarded Bloomberg request keywords to internal metadata lookups and never affected the returned ticker; passing them is now a `TypeError`.

### Fixed

- **CDX series resolution is now as-of the requested date, and monotone**: `cdx_ticker` / `active_cdx`, `Engine.cdxTicker()` / `Engine.activeCdx()`, and the underlying `xbbg-recipes` entry points resolve the series whose Bloomberg `CDS_FIRST_ACCRUAL_START_DATE` is the latest one on or before the reference date, walking the accrual ladder in batched reference-data requests. Previously they read `ROLLING_SERIES` off the generic ticker -- a point-in-time field that reports *today's* series whatever date was asked for -- and stepped back at most one series, so every historical date collapsed onto the current series or its predecessor (`cdx_ticker('CDX IG CDSI GEN 5Y Corp', '2020-06-01')` returned S45; it now returns S34). `active_cdx` additionally chose between the two candidates by whichever had the later `PX_LAST` print, which flipped the answer back to the older series on any day the newer one had not yet printed. Series, and version within a series, are now non-decreasing as the date advances.
- **CDX roll dates are read, not assumed**: the semi-annual cadence only sizes the candidate window; the series is decided by comparing against the accrual dates Bloomberg returns. Business-day-adjusted rolls therefore resolve exactly -- CDX.NA.IG.45 first accrues 2025-09-22, so 2025-09-21 resolves to S44.
- **CDX version is resolved per series**: the `Vn` token is the version Bloomberg reports for the resolved series (CDX.NA.HY.32 resolves to V14, HY.40 to V4), instead of the current series' version stamped onto every answer. Version is scoped to a series and resets at each roll, so it is non-decreasing within a series but not across one.
- **BREAKING -- failed CDX resolution raises**: `xbbg.ext.cdx_ticker` / `active_cdx` no longer swallow Bloomberg errors, missing metadata, or unparseable series numbers into an empty-string ticker. They now delegate to the same `xbbg-recipes` resolver as `@xbbg/core`, so the Python and JavaScript surfaces cannot drift apart, and raise `ValueError` (non-generic ticker, date before the index's first series) or `RuntimeError` (missing or inconsistent Bloomberg metadata) instead. Callers that tested for `""` must catch instead.
- **Clean session shutdown no longer logs `SessionConnectionDown` / `SessionTerminated` as WARN/ERROR** (#346): tearing an engine down -- including the implicit teardown at interpreter exit after a one-shot call such as `blp.bdh()` -- makes the Bloomberg SDK emit both events for every pooled session. The request path already recognized a requested shutdown and logged at INFO, but the subscription path did not, so any script that simply ran a request printed a spurious `SessionTerminated -- SDK gave up reconnecting; closing subscriptions` at ERROR on exit, with `active_subs=0` and an empty reason. Subscription workers now publish the shutdown flag before asking the SDK to stop and classify each lifecycle event (`shutdown_in_progress` at INFO versus `connection_down_without_shutdown` / `termination_without_shutdown` at the original WARN/ERROR), so genuine mid-session failures keep their severity.
- **`import xbbg` no longer leaks `SyntaxWarning`s from Bloomberg's `blpapi` wheel** (#346): importing xbbg runs SDK discovery, which imports the `blpapi` Python package because the pip wheel ships the shared library and is probed ahead of a Terminal DAPI install. Bloomberg's `resolutionlist.py` and `topiclist.py` contain invalid escape sequences in their docstrings, so Python 3.12+ printed `SyntaxWarning: invalid escape sequence '\s'` on first import for anyone with the wheel installed -- from an import the caller never asked for. Discovery now performs that import with `SyntaxWarning` filtered, leaving the user's own warning filters untouched.

## [1.4.5] - 2026-07-18

### Added

- **Shared ETF NAV / iNAV toolkit with Python and JavaScript parity**: A new `xbbg-recipes` module resolves Bloomberg's authoritative `ETF_NAV_TICKER` / `ETF_INAV_TICKER` relationships (normalized to one ` Index` suffix and validated as genuine Index securities, so non-conventional targets such as `QQQ US Equity -> QXV Index` and null/`AT1IN Index` legs survive intact), serves current NAV/iNAV snapshots and daily history with a `FUND_NET_ASSET_VAL` fallback for ETFs without a daily NAV Index, and powers Python `xbbg.ext` (`etf_nav_relationships`, `etf_nav_snapshot`, `etf_nav_history`, `subscribe_etf_inav` plus async variants) and `@xbbg/core` (`etfNavRelationships`, `etfNavSnapshot`, `etfNavHistory`, `subscribeEtfInav`) with identical schemas and atomic iNAV subscription preflight.

### Changed

- **Canonical CDX series identities across Rust, Python, and JavaScript**: `xbbg-recipes`, `cdx_ticker` / `active_cdx`, and `Engine.cdxTicker()` / `Engine.activeCdx()` now require Bloomberg `VERSION` metadata and return explicit `Vn` identifiers (including `V1`) by default, resolve prior-series versions independently, and round-trip explicit-version tickers correctly. Pass `versionless=True` or `{ versionless: true }` only when a legacy Bloomberg alias is required.

## [1.4.4] - 2026-07-12

### Added

- **Complete Bloomberg entitlement-ID routes across bindings**: Python and `@xbbg/core` now request EIDs for intraday bars and ticks as well as BDP/BDS/BDH; LangGraph exposes `returnEids` on BDP/BDS/BDH/BDIB/BDTICK plus `xbbg_check_entitlements`, and MCP exposes `return_eids` on its dedicated BDP/BDS/BDH/BDIB tools and generic IntradayTick route plus `check_entitlements`, with bounded results preserving entitlement metadata.
- **Official MCP Registry publish workflow**: Added a manual GitHub Actions workflow that publishes release `server.json` metadata through `mcp-publisher` with GitHub Actions OIDC, so the xbbg-org namespace can publish without relying on a maintainer's local public organization membership.
- **Batched subscription delivery in `@xbbg/core`**: one native crossing now drains many ticks (`nextUpdates`) and Arrow subscriptions return multi-row zero-copy batches (`nextArrowBatch`); tick field layouts cross the boundary once per layout version and `Tick` caches decoded `BigInt`/`Date` values.
- **`SubscriptionArrowBatcher`** in the Rust engine: cached schema + reusable Arrow builders convert streaming updates into multi-row RecordBatches instead of one-row batches per tick.
- **Offline benchmark coverage**: registered the previously orphaned Arrow/subscription bench targets, added a `serde_json` vs `simd-json` BQL parser bench, and added offline Rust→Python / Rust→JS binding-handoff benches that run without a Bloomberg connection.
- **Host-tuned build modes**: `pixi run build-native` and `npm run build:native:host` produce `target-cpu=native` artifacts for internal deployments; published artifacts stay portable.

### Changed

- **Subscription sessions moved to Bloomberg SDK asynchronous callback mode**: the 1 ms `nextEvent` busy-poll loop, its command channel, and per-loop topic-status scans are gone; events are dispatched by SDK callbacks and deactivation warnings run on a 1 s timer. Idle subscription workers no longer consume CPU.
- **Faster response decoding**: typed long-format reference/historical output uses fixed-schema builders (no per-cell string-keyed map lookups), BDS bulk decoding walks each row once via interned name keys instead of O(rows × subfields) lookups, intraday bars use fixed builders, streaming chunks pre-reserve capacity, and BQL infers column types in a single pass.
- **Cheaper ticks**: DATALOSS detection is folded into the normal extraction pass, message types are read via borrowed strings instead of refcounted `Name` duplicates, small updates avoid heap allocation (`SmallVec`), and repeated string values (exchange/condition codes) are interned per field.
- **Python pandas conversion is Arrow-native**: `backend='pandas'` now converts through the Arrow C stream (`pyarrow.table(...).to_pandas()`) instead of materializing every cell as a Python object; the row-based path remains only as a documented no-pyarrow fallback. Python subscriptions with `backend=None` now yield native Arrow wrappers and resolve their converter once at construction.
- **`@xbbg/core` request responses are zero-copy**: reference/historical/recipe results cross N-API as Arrow buffer descriptors instead of Arrow IPC bytes that were immediately reparsed; schema APIs cache their JSON payloads.
- **Request workers decode responses outside the shared slot table lock**, so large partial responses no longer block unrelated in-flight requests on the same worker.
- **`OverflowPolicy::Block` is now bounded on the SDK callback thread**: instead of parking Bloomberg's dispatcher indefinitely when a consumer stalls, the producer retries briefly, then records a slow-consumer event and drops the update.
- **`npm run build:native` builds release by default** (use `build:native:debug` for local iteration), and packaging refuses to stage a native addon whose build profile isn't `release`.
- **`blpapi-sys` caches generated bindings** next to the vendored SDK keyed by target and build-script hash, and tracks headers/env precisely, removing bindgen from warm clean builds and spurious `CONDA_PREFIX` rebuilds on non-Windows hosts.
- **Metadata caches (exchange info, field types) publish snapshots per batch** with bounded capacity instead of cloning the whole map per inserted key.

### Fixed

- **Marimo notebook compatibility**: Synchronous one-shot Python APIs such as `blp.bdp()` now use the notebook bridge inside marimo's async execution context instead of raising the generic async-context error.

## [1.4.3] - 2026-07-04

### Added

- **Installable xbbg MCP bundle and registry metadata**: Release builds now create the cross-platform `xbbg-mcp-v<VERSION>.mcpb` local connector bundle, generate matching `server.json` metadata for official MCP Registry publication, and attach both assets to GitHub releases alongside the existing platform `xbbg-mcp` archives.
- **Supported JavaScript package documentation**: Clarified the supported JavaScript package paths and formatted the package README for the current release docs.

### Fixed

- **Release artifact hygiene for Bloomberg SDK paths**: Source distributions now prune placeholder vendored Bloomberg SDK directories and unrelated JavaScript side-project files so public release artifacts pass the Bloomberg SDK/runtime exclusion guard.

## [1.4.2] - 2026-07-04

### Added

- **Bloomberg entitlement IDs and identity checks across bindings**: Rust, Python, JavaScript, and MCP request paths now accept opt-in `returnEids` / `return_eids` for reference and historical data. Results expose `eidData` plus structured Bloomberg `securityError` and `fieldExceptions` metadata; Python adds `seat_type()`, `check_entitlements()`, and `identity_is_authorized()` helpers, and `@xbbg/core` adds matching `engine.seatType()`, `engine.checkEntitlements()`, and `engine.identityIsAuthorized()` APIs.

### Changed

- **Dependency stack refreshed across Rust, Python, and JavaScript**: Updated Rust direct dependencies for MCP, extension TOML parsing, PHF maps, and benchmarks; refreshed Cargo, uv, Pixi, and npm locks to current compatible releases; moved JavaScript development tooling to current Node types, oxfmt, and oxlint-tsgolint releases; adjusted oxlint configuration for newly promoted rules in generated, test, benchmark, and native-loading paths; and migrated Pixi's Linux/glibc requirements plus npm's local platform package resolution onto declarative entries while removing the unused Bloomberg PyPI extra index.

### Fixed

- **Bloomberg response diagnostics survive every result backend**: Reference, historical, bulk, raw/generic, Arrow IPC, NAPI zero-copy, pandas, JavaScript JSON, and MCP JSON paths now preserve entitlement IDs and Bloomberg security/field diagnostics instead of dropping them during format conversion or shard merging.
- **Reference-data long output keeps mixed string/numeric values with partial field caches (#342)**: The Rust long-format value-column type now treats cached field-type hints as authoritative only when every requested field has a hint, so a cache containing only numeric fields no longer coerces mixed `bdp` responses to `Float64` and nulls string fields.
- **`@xbbg/langgraph` calculate tool rejects unsupported level inputs (#343)**: Tool calls now validate `calculate_level_percentages` hierarchy levels before invoking core helpers, preventing out-of-domain values such as `100`/`102` from silently returning null rows while direct invocation still rejects unknown operations.

## [1.4.1] - 2026-06-26

### Fixed

- **`@xbbg/langgraph` npm release validation**: Removed a redundant nullable branch from override schema normalization so the package publish workflow's strict TypeScript ESLint gate passes while preserving validated input behavior.

## [1.4.0] - 2026-06-26

### Added

- **Opt-in request sharding for wide `bdp`/`bdh` calls**: Rust/Python engine configuration now accepts `shard_requests`, `shard_threshold`, `shard_chunk_size`, and `shard_max_concurrent` (JavaScript: `shardRequests`, `shardThreshold`, `shardChunkSize`, `shardMaxConcurrent`) to fan out eligible multi-security reference and historical requests while preserving output order.
- **Composable Bloomberg override helpers**: Python now exports `xbbg.ovr()` / `xbbg.OverrideSpec`, and JavaScript exports `ovr()` / `OverrideSpec`, for reusable override specs that feed the existing `overrides=` / `options.overrides` request paths.
- **Per-security Bloomberg overrides**: Python, JavaScript, and `@xbbg/langgraph` `bdp`/`bdh`/`bds` request paths now accept per-security override specs inside the existing `overrides` / `options.overrides` path via `ovr()` / `OverrideSpec`. The Rust engine shards by contiguous override sets, merges global overrides first, and preserves output order while still honoring enabled shard chunk limits.

## [1.3.1] - 2026-06-22


### Added

- **`@xbbg/langgraph` chart-spec helper for generative UI**: Added `xbbg_ext_chart_spec` and `createExtChartSpecTool()` to convert bounded Bloomberg result rows into dependency-free Vega-Lite visualization payloads for CopilotKit, assistant-ui, Vercel AI SDK, LangGraph UI, or other frontend renderers.

### Fixed

- **Python request failures catch public `BlpRequestError` (#341)**: Native Rust request-family errors are normalized to the public Python exception classes at the request boundary, so `except xbbg.BlpRequestError` catches all-security request failures while preserving `BlpLimitError`, `BlpSecurityError`, and `BlpFieldError` specificity.
- **`@xbbg/langgraph` preferreds fields are optional in tool calls**: `xbbg_preferreds` now treats `fields: []` as omitted so the preferred-stock recipe can use its default field set instead of rejecting model/frontend calls that send an empty optional list.

### Security

- **Security dependency updates for PyO3, esbuild, and LangGraph tooling**: Updated the Rust PyO3 binding stack to the patched `0.29` line, upgraded Arrow integration accordingly, and refreshed JavaScript development dependencies/overrides so Dependabot alerts for PyO3, `esbuild`, and transitive `uuid` are resolved.

## [1.3.0] - 2026-06-13

### Added

- **Async JavaScript Bloomberg connection factories**: `@xbbg/core` now exposes `Engine.connect()` and top-level `connect()` so Bloomberg session startup runs off the Node event loop; the existing synchronous constructor remains available for compatibility.
- **Dedicated Bloomberg capacity-limit exceptions**: JavaScript now exports `BlpLimitError`, and Python now exports `xbbg.BlpLimitError`, for Bloomberg `LIMIT` / `DAILY_CAPACITY_REACHED` responses.
- **`@xbbg/langgraph` cancellation and timeout safety**: Tools now honor the LangChain/LangGraph `AbortSignal` — already-cancelled calls never start Bloomberg work and aborted snapshot tools stop collecting and unsubscribe immediately instead of running out their timeout. Lazily connected engines apply a default hard per-request timeout (`DEFAULT_ENGINE_REQUEST_TIMEOUT_MS`, 60s; `engineConfig.requestTimeoutMs` overrides, `0` disables) so a wedged Terminal session cannot hang tool calls forever. The new `ToolInvocationConfig` type is exported for custom tool integrations.

### Changed

- **Bloomberg request workers use SDK push-mode sessions by default**: The Rust async engine now creates callback-mode Bloomberg sessions, sends requests directly from submitting tasks, and routes pushed SDK events by generation-tagged correlation IDs. This removes the 1ms command-queue poll loop for request dispatch while preserving multi-worker request pools, request cancellation, service-open coalescing, timeout scanning, and safe shutdown ordering.
- **Rust hot paths tightened for high-throughput Bloomberg workloads**: Request setters now use interned Bloomberg `Name` handles instead of allocating field-name C strings, release builds optimize for speed, field/exchange caches avoid stale disk entries, Arrow extraction avoids unchecked row/string access, and iterator/error paths now fail closed on Bloomberg SDK return codes.
- **`@xbbg/langgraph` extension schemas are strict discriminated unions**: All `xbbg_ext_*` tool schemas now publish per-operation required fields to the model (with unknown-key rejection) instead of flat all-optional objects policed by runtime errors, and `recoveryRate` is validated as a 0-1 decimal. Snapshot tools report cleanup failures in a new `unsubscribeError` result field instead of discarding collected updates, empty results carry verification guidance in the model-facing summary, and binary payloads are bounded as placeholders instead of serialized raw.
- **`@xbbg/langgraph` security identifier guidance rewritten as pass-through rules**: Agent instructions and every securities/ticker field description now state that securities are passed exactly as the user supplied them — Bloomberg tickers as `<TICKER> <MARKET_SECTOR>`, raw ISINs as `/isin/<ISIN>`, raw CUSIPs as `/cusip/<CUSIP>` — and that the ticker format is a template, never authorization to invent, recall, or guess a ticker for an identifier. The guidance now enumerates the Bloomberg market sector (yellow key) vocabulary — `Equity`, `Index`, `Curncy`, `Comdty`, `Corp`, `Govt`, `Muni`, `Mtge`, `M-Mkt`, `Pfd` — and documents that `xbbg_ext_ticker.parse_ticker` only handles generic futures-style tickers. Ticker-only recipe tools (`xbbg_preferreds`, `xbbg_corporate_bonds`, `xbbg_index_members`, `xbbg_etf_holdings`) direct the model to resolve identifiers through `xbbg_resolve_isins` first, and `xbbg_preferreds` explicitly takes the issuer's common equity ticker rather than a fabricated `Pfd` ticker. This removes the ambiguous "do not pass raw ISIN" phrasing that nudged models into inventing tickers instead of sending `/isin/<ISIN>`.
- **`@xbbg/langgraph` request schemas only advertise parameters the engine accepts**: `format` is removed from the `xbbg_bds`, `xbbg_bql`, `xbbg_bsrch`, `xbbg_bflds`, and `xbbg_beqs` tool schemas because the engine rejects it for BulkData, Bql, Bsrch, FieldInfo, and BEQS output ("format is not supported for ... output"); model-sent values are stripped instead of forwarded, while `xbbg_bdp`/`xbbg_bdh` keep their supported format options. Date inputs are now JSON-representable end to end: `z.date()` is dropped from the wire unions (zod v4's `toJSONSchema` throws "Date cannot be represented in JSON Schema"), with strings and epoch/`YYYYMMDD` numbers covering all tool calls. A new `toolParameterJsonSchema(tool)` export returns the provider-ready `$ref`-free JSON Schema so consumers stop reinventing zod conversion, and the agent instructions now direct one call per dataset instead of parallel parameter probing.

### Fixed

- **Bloomberg top-level `responseError` payloads are surfaced consistently**: Refdata, historical, bulk, generic, intraday, BQL, field-info, and search extractors now propagate Bloomberg `responseError` details such as `category=LIMIT`, `code=-4001`, and `subcategory=DAILY_CAPACITY_REACHED` instead of returning empty successful batches or generic request failures.
- **Binding error mapping preserves Bloomberg failure classes**: NAPI and PyO3 now translate Bloomberg limit responses into the new limit exception classes while keeping validation, timeout, cancellation, session, and generic request errors distinct.
- **Rust Bloomberg SDK ownership and entitlement handling hardened**: Identity handles are released correctly, entitlement checks now preserve the SDK boolean result, `Session::next_event` maps non-timeout SDK failures to errors, and async session shutdown waits for Bloomberg callbacks to drain before dropping callback state.
- **`@xbbg/langgraph` numeric dates no longer collapse to 1970**: Integer `YYYYMMDD` inputs (e.g. `20240131`) are parsed as calendar dates instead of epoch milliseconds, ambiguous numeric dates are rejected, and date/override normalization failures now surface as actionable schema messages to the model instead of LangChain's generic schema-mismatch text. Concurrent tool calls sharing one failed connect no longer cross-contaminate each other's error prefixes by mutating the shared `Error`.

## [1.2.7] - 2026-06-09

### Changed

- **Release process documents `@xbbg/langgraph` npm publishing**: The npm trusted publishing checklist now includes the LangGraph package and notes that `npm-publish.yml` publishes it alongside `@xbbg/core`.
- **`@xbbg/langgraph` supports Zod 3 and 4 runtimes**: Replaced direct Zod 4 JSON Schema conversion with `zod-to-json-schema` over the `zod/v3` compatibility surface, and relaxed the direct `zod` dependency to `^3.25.32 || ^4.2.0`.

### Fixed

- **`bdib` intraday bars include Bloomberg trade value (#336)**: The Rust intraday bar extractor now preserves `barTickData.value` alongside OHLC, volume, and `numEvents` for Python and JavaScript callers, enabling bar VWAP calculations from Bloomberg's emitted trading amount.
- **Windows Bloomberg DAPI runtime auto-detection widened**: Python and `@xbbg/core` now detect standard Terminal runtime roots including `C:\blp\DAPI` and `C:\Program Files (x86)\Bloomberg\Blp\DAPI`; Windows SDK roots are also checked under `bin/` before requiring manual `BLPAPI_ROOT` / `BLPAPI_LIB_DIR` setup.

## [1.2.6] - 2026-05-31

### Fixed

- **`bflds(search_spec=...)` field searches restored**: `FieldSearchRequest` may again use the normalized `FieldInfo` extractor path, so free-text Bloomberg field searches such as implied volatility, historical volatility, debt, enterprise value, net debt, and cash fields no longer fail request planning with an extractor compatibility error.

## [1.2.5] - 2026-05-29

### Added

- **`@xbbg/langgraph` npm package**: Added a publishable LangChain/LangGraph tools package backed by `@xbbg/core`, with lazy Bloomberg engine loading, bounded JSON tool outputs, request tools for `bdp`/`bdh`/`bds`/`bdib`/`bdtick`/`bql`/`bsrch`/`bqr`/`bflds`, grouped `xbbg.ext` helper tools, detailed Bloomberg agent prompt guidance, unit tests with injected fake engines, and npm/GitHub release workflow integration.
- **`@xbbg/langgraph` tool surface expansion**: Added finite request/recipe tools for BEQS, YAS, preferreds, corporate bonds, index members, ISIN resolution, issuer ISIN workflows, and ETF holdings. Added bounded streaming snapshot tools for market data, market bars, and market depth that cap updates/timeouts and always unsubscribe instead of exposing open-ended subscriptions.
- **`@xbbg/core` CDX field bundles exposed**: Exported the shared `CDX_INFO_FIELDS`, `CDX_PRICING_FIELDS`, and `CDX_RISK_FIELDS` field lists so JavaScript and LangGraph CDX helpers can use the same canonical Bloomberg field sets.

### Changed

- **`@xbbg/langgraph` output contract**: LangChain tools now use `responseFormat: "content_and_artifact"` so agents receive compact model-facing summaries while applications can read the bounded structured envelope from `ToolMessage.artifact`.
- **Request planning and adapter normalization hardened**: Rust request planning now finalizes immutable prepared requests only after field-cache and intraday datetime preparation, validates operation/extractor compatibility in one place, and keeps raw/custom/generic escape hatches. NAPI, PyO3, and MCP request adapters now build through the shared `RequestParamsInput` boundary so raw operation defaults and extractor parsing stay consistent across surfaces.
- **Python request dispatch and backend conversion centralized**: Request middleware now mutates canonical request parameters until dispatch, materializes request dictionaries only at the terminal boundary, and routes Arrow-like middleware/backend results through the same backend conversion helpers as normal Bloomberg responses.
- **`@xbbg/core` native package validation made descriptor-driven**: Native package validation now derives expected binary names, OS/CPU metadata, and packaged files from the source platform descriptor table instead of reverse-engineering package manifests.
- **`@xbbg/langgraph` extension internals split into bounded registries**: Extension schemas and CDX field bundles moved out of `ext-tools.ts`, input schemas now honor normalized package limits, and extension tool creation is driven by a single registry that also exports `BLOOMBERG_EXT_TOOL_NAMES`.

### Fixed

- **JavaScript lint and packaging hygiene**: Cleaned `js-xbbg` TypeScript/Oxlint findings in native package scripts, native platform metadata, and smoke tests so the core JS lint suite passes alongside the new LangGraph package quality checks.
- **`@xbbg/langgraph` result limiting handles pathological structures**: `limitResult` now detects cycles and caps recursion depth so tool artifact bounding cannot overflow on cyclic or deeply nested values.

## [1.2.4] - 2026-05-24

### Fixed

- **`bsrch` / `absrch` live SRCH grids with `NumOfFields=0` (#334)**: Treat Bloomberg `GridResponse.NumOfFields = 0` as "not reported" instead of validating it against populated `ColumnTitles[]` / `DataRecords[].DataFields[]`, matching live `ExcelGetGridRequest` payloads that carry rows and titles while reporting zero fields.

## [1.2.3] - 2026-05-22

### Added

- **Native-backed extension workflow parity across Python and JavaScript**: Added shared Rust recipe implementations and bindings for futures curves, volatility surfaces, dividend yield, index members, and ISIN workflows, giving the Python `xbbg.ext` helpers and `@xbbg/core` JavaScript client the same backend-neutral Bloomberg workflows with typed Arrow extraction.

### Changed

- **`@xbbg/core` npm package and TypeScript tooling hardening**: JS build, packaging, validation, and subscription replay tooling now run through TypeScript sources, stricter Oxfmt/Oxlint/ESLint/TypeScript checks, packed-install smoke validation, package metadata validation, and platform package loaders for the native `@xbbg/core-*` packages.
- **README and JavaScript usage documentation**: The root and JS READMEs now clarify JavaScript package usage, native package packaging behavior, raw subscription output, and subscription workflow details.

### Fixed

- **`bsrch` / `absrch` Excel grid requests restored (#333, #334)**: Python now sends BSRCH search parameters as `ExcelGetGridRequest` `Overrides[]` entries while keeping `Domain` as the top-level request element, so Excel-style weather searches no longer fail with Bloomberg element validation errors. The Rust extractor now parses `GridResponse.ColumnTitles[]` and `DataRecords[].DataFields[]` directly instead of the BEQS schema, preserves Bloomberg column names, propagates grid errors, and validates reported row/field counts rather than returning an empty placeholder ticker column.
- **`bsrch` / `absrch` request plumbing hardened (#333, #334)**: Low-level `arequest()` now preserves `//blp/exrsvc` overrides as overrides instead of rewriting them into root elements, and the Rust engine normalizes `ExcelGetGridRequest` `Domain`/override pairs consistently across generated endpoints, raw requests, and direct request kwargs. `RawRequest` calls whose effective operation is `ExcelGetGridRequest` now pick the BSRCH extractor by default, with regression coverage for malformed Python override pairs and live weather-grid requests.
- **Release and CI hardening**: CI and release workflows now choose the latest Bloomberg C++ SDK version with downloadable Linux, macOS, and Windows archives instead of assuming the newest Python `blpapi` wheel has matching C++ SDK artifacts, keeping Python and npm release builds reproducible when Bloomberg publishes package families asynchronously. The MCP server build also annotates the macro-generated tool router field so strict cross-platform Clippy checks remain green.

## [1.2.2] - 2026-05-06

### Fixed

- **Bloomberg request error handling and datetime semantics (#328, #329, #330)**: Reference-data wide/semi-long requests and historical-data requests now surface all-security `securityError` responses as `BlpError::RequestFailure` instead of returning successful empty batches. The Python terminal wrapper now handles `pyarrow.RecordBatch` returns via `pyarrow.Table.from_batches()` rather than assuming a non-existent `to_table()` method. Bloomberg datetime parsing now rejects malformed `T` datetime strings and SDK-invalid timezone offsets, and timestamp conversion honors Bloomberg's `OFFSET` part when converting to UTC epoch values.
- **Python and JavaScript binding edge cases**: `xbbg.set_sdk_path()` now immediately prepares native SDK loading and retains Windows DLL search handles, explicit but unimplemented Python dataframe backends now raise `NotImplementedError` instead of falling through to Narwhals/native output, turnover helpers reject malformed explicit dates before applying defaults, NAPI Arrow zero-copy length metadata is checked before narrowing to JavaScript-visible `u32`, and JS zero-copy Arrow construction validates buffer lengths before creating typed views.

- **`active_futures` / `activeFutures` generic futures mapping (#327)**: The shared Rust recipe now uses Bloomberg's historical `FUT_CUR_GEN_TICKER` mapping as the primary source for past/current dates, normalizing returned roots such as `UXK6` to the input asset suffix (`UXK6 Index`) across Python and JavaScript bindings. Future dates or missing mappings fall back to the existing generated-candidate maturity/volume logic. The recipe also accepts typed Arrow `value` columns (`Float64` `VOLUME`, `Date32` `LAST_TRADEABLE_DT`) instead of requiring Utf8, fixing the monthly/default-frequency failure for `UX1 Index`.

## [1.2.1] - 2026-04-30

### Fixed

- **Market-bar streaming message metadata**: `//blp/mktbar` subscription rows now include `SUBSCRIPTION_DATA` with Bloomberg's message type (for example `MarketBarStart` / `MarketBarUpdate`), so callers can distinguish bar lifecycle states that are not payload elements.
- **`bdtick` output timezone preservation (#309)**: Native Arrow, pandas, and pyarrow conversions now preserve the requested `output_tz` metadata instead of materializing UTC-aware datetimes when callers ask for a local zone such as Hong Kong.
- **Optional Python dataframe backend validation**: Explicit optional backend selections now fail with actionable import errors when pandas, pyarrow, polars, or duckdb are missing, and exchange-info helpers no longer hide a missing pandas dependency behind Bloomberg fallback behavior.
- **`@xbbg/core-darwin-arm64` macOS package portability (#285)**: macOS native addon builds no longer ship absolute CI or Bloomberg SDK rpaths. The build now post-processes `napi_xbbg.node` with `install_name_tool`, rewrites Bloomberg SDK dependencies to `@rpath`, adds `@loader_path` rpaths for relocatable deployments, and fails release builds if `otool` still reports non-system absolute load paths.

## [1.2.0] - 2026-04-29

### Removed

- **`xbbg-browser`, `xbbg-bridge`, and `xbbg-server` retired**: The `apps/xbbg-server` Rust HTTP server, its `@xbbg/bridge` npm launcher and 5 platform-specific bridge binaries, and the `@xbbg/browser` HTTP client are removed. `js-xbbg`, `napi-xbbg`, and `pyo3-xbbg` remain the supported bindings.

### Added

- **Native datetime/date acceptance across all surfaces (#317)**: `bdh`, `bdib`, `bdtick`, `bqr`, `arequest`, and the `xbbg.ext.bonds` / `xbbg.ext.options` / `xbbg.ext.fixed_income` / `xbbg.ext.historical` / `xbbg.ext.futures` helpers now accept `datetime.date`, `datetime.datetime` (naive or tz-aware), and duck-typed `pd.Timestamp` (no hard pandas dependency) anywhere they previously took only `str`. ISO 8601, Bloomberg-native (`YYYYMMDD`), and `"today"` strings continue to work; ambiguous `MM/DD/YYYY`-style inputs are now rejected with a clear `ValueError`. The two divergent `_fmt_date` helpers were consolidated into a single source of truth in `xbbg.ext._utils` (extended with native-type support and a new `_fmt_datetime`). Bloomberg field overrides passed as `**kwargs` (e.g. `USER_LOCAL_TRADE_DATE=date(2023, 1, 17)`) are normalized to `YYYYMMDD` via value-based duck typing. Mirrored on the JS side: `@xbbg/core` accepts `Date`, ISO/Bloomberg-native `string`, epoch-ms `number`, and duck-typed Luxon `DateTime` across `bdh` / `bdib` / `bdtick` / `bqr` / recipe surfaces, with new `formatDate` / `formatDateTime` helpers and `DateLike` / `DateTimeLike` exported types. New guide at `docs/python/guides/dates`.
- **`@xbbg/core` subscription replay benchmark**: Added a JS-only `npm run bench:subscription-replay` harness for one-update-at-a-time synthetic replay, JSONL fixture replay, live `XBTUSD Curncy` capture, and path-specific timing (`legacy`, `arrow-decode-only`, `subscription-wrapper`). Replay now supports `--consume rows|vector|schema|none` and `--warmup-iterations`; row materialization remains the default. Live capture reports existing `sub.stats` slow-consumer telemetry without changing the production streaming API.
- **`xbbg-bench` offline Rust replay benchmarks**: Added benchmark-controlled, non-Bloomberg harnesses for Arrow/`TypedBuilder` append/finalize paths and synthetic `xbbg-async` subscription-shaped replay. These live entirely under `crates/xbbg-bench`, emit JSON artifacts, and use env knobs for row counts, flush size, and iterations so production crates do not carry benchmark-only hot-path changes.
- **`xbbg-bench` cached subscription-to-Arrow bridge benchmark**: Added a bounded live Bloomberg subscription capture that replays cached real SDK `Event`/`Message` objects through `xbbg-async` `SubscriptionState` into Arrow batches. This connects core SDK traversal with the subscription Arrow path while keeping Bloomberg usage to a small initial capture.
- **Low-data live regression coverage for recent Bloomberg issues**: Added live integration coverage for recent BDTICK/ABDTICK, BDS bulk headers, numeric backend typing, optional conversion backend dispatch, BQL economic calendar, exchange-resolution, subscription timestamp, and options-extension regressions. The new fixtures favor bounded requests (`maxDataPoints`, narrow chains, and current low-volume windows) so the live suite keeps request volume limited while still exercising real service behavior. Users remain responsible for Bloomberg entitlements and usage terms.
- **Bloomberg Excel/0.x request aliases restored (#301)**: `bdh` / `abdh`, `bdib` / `abdib`, and `bdtick` / `abdtick` now accept the 0.x/Excel-style request aliases (`Per`, `PerAdj`, `Curr`/`FX`, `Days`, `Fill`, `Points`, `Quote`, `QuoteType`/`QtTyp`, `CshAdj*`, `CapChg`, `UseDPDF`, `Calendar`, `BarSz`/`BarSize`, `BarTp`/`BarType`, and `IncludeExchangeCodes`) and normalize enum shorthand values before requests reach Bloomberg. `bdh()` also consumes Excel-only presentation aliases (`Dts`/`Dates`, `DtFmt`/`DateFormat`, `Sort`, `Orientation`/`Direction`/`Dir`) locally for date/period display, row ordering, and default orientation. Coverage includes offline routing tests plus a capped live Bloomberg suite.

### Changed

- **Python backend clean-cutover to native xbbg Arrow objects**: The Rust layer now returns native `xbbg._core.ArrowTable` / `ArrowRecordBatch` carriers internally, with explicit `backend="native"` for callers that want those raw objects. The public default remains a Narwhals DataFrame and prefers a real PyArrow table when PyArrow is installed, then falls back through installed dataframe libraries before the minimal xbbg Narwhals plugin. `backend="pyarrow"` returns a real `pyarrow.Table`; the misleading `Backend.ARROW` / `backend="arrow"` alias was removed. Pandas, Polars, DuckDB, and Narwhals remain explicit optional conversion backends, and PyArrow is no longer a core dependency.
- **`@xbbg/core` subscriptions now require NAPI Arrow zero-copy transfer**: `Subscription.next()` asks the native binding for Arrow buffer descriptors and builds Apache Arrow JS tables directly from native Arrow buffers for common Bloomberg subscription types (`bool`, `date32`, `float64`, `int32`, `int64`, `time64[us]`, `timestamp[us]`, `utf8`, `null`). The JS subscription path no longer falls back to standalone IPC; unsupported or sliced schemas fail fast with column-level diagnostics while the public `Subscription.next(): Table` API remains unchanged.
- **`@xbbg/core` exposes full Bloomberg subscription payloads**: JS streaming APIs now accept `allFields: true`, forwarding the existing Rust engine `all_fields` mode so callers can receive every top-level scalar field Bloomberg sends instead of only requested fields plus `MKTDATA_EVENT_TYPE` / `MKTDATA_EVENT_SUBTYPE`. The NAPI zero-copy bridge also supports `time64[us]` columns for dynamic all-fields schemas.
- **Rust Bloomberg SDK handle ownership hardened**: `xbbg-core` now models session-owned SDK views with Rust lifetimes instead of unsupported `Send`/`Sync` marker impls. `Service`, schema operations/definitions, and constants are tied to their owning session/service, pointer correlation IDs are explicit unsafe values, and async request workers reopen short-lived service handles rather than caching session-owned handles across worker state.
- **Reference data `fieldExceptions` logging aggregated**: Per-security `fieldExceptions` diagnostics now stay at `DEBUG` with field-level detail, while bulk requests emit a single summary warning with total exception count and affected tickers.
- **`@xbbg/core` TypeScript request surface completed**: JS wrappers now forward `validateFields` on `bdp`/`bds`/`bdh`, `requestTz`/`outputTz` on `bdib`/`bdtick`, and typed `bdtick` include-code options while rejecting unknown backend strings instead of silently returning Arrow.
- **Generic `bds()` / `abds()` bulk-header contract documented (#274)**: xbbg preserves Bloomberg bulk subfield labels exactly in the generic BDS path; only `ticker` and `field` are xbbg-added metadata columns. Higher-level helpers that need stable semantic names must rename their own outputs explicitly.
- **Generated sync wrappers now resolve in IDEs (#307)**: `bdp`, `bdh`, `bds`, `bdib`, `bdtick`, `bql`, `bsrch`, `bqr`, `bflds`, `beqs`, `blkp`, `bport`, `bcurves`, and `bgovts` now expose static signatures for parameter hints, hover docs, and go-to-definition. Top-level `xbbg` exports were also completed for `abqr` / `bqr` and the generated endpoint stubs.
- **Retired mock crates removed from the workspace**: The old C++ mock stack and Cargo mock feature forwarding were removed so the Rust workspace has a single live Bloomberg SDK FFI path.

### Fixed

- **`xbbg.ext` sync helpers now respect async contexts**: Extension convenience wrappers such as `bond_info()`, `yas()`, `fut_ticker()`, `dividend()`, `option_info()`, and `convert_ccy()` now share the core `bdp()` sync boundary: normal synchronous calls still run, notebook/IPykernel loops use the one-shot background bridge, and other running event loops fail clearly with the corresponding async helper name instead of calling `asyncio.run()` and leaking unawaited coroutines.

- **BQL schema introspection no longer stack-overflows**: `aget_schema("//blp/bqlsvc")`, `get_schema("//blp/bqlsvc")`, and schema-driven request-parameter routing against the BQL service now terminate recursive Bloomberg schema definitions with a path-scoped cycle guard while preserving top-level `sendQuery` elements such as `expression`, `appName`, `clientContext`, and `bqlRequestId`.
- **BQR dealer attribution restored (#312)**: `bqr()` / `abqr()` and `xbbg.ext.bqr()` now default to `BID`/`ASK` quote events with `includeBrokerCodes=true`, normalize output to the 0.x-compatible `event_type` / `price` / `broker_buy` / `broker_sell` columns, warn when an attributed request is not shaped like a fixed-income ISIN with `@MSG1 Corp`, and raise when Bloomberg returns quote rows without broker attribution unless callers explicitly opt out with `include_broker_codes=False`. Low-data live coverage uses an `@MSG1 Corp` fixed-income ISIN fixture capped with `maxDataPoints=5`.
- **Subscription schemas preserve sparse numeric quote fields**: Requested subscription fields now observe Bloomberg element datatypes even when a particular update carries a null value, so sparse streams such as `XBTUSD Curncy` quote updates keep `BID` / `ASK` as `Float64` instead of degrading the Arrow schema to `Utf8` before a non-null quote arrives. Live schema tests now print sample raw batches for easier diagnosis.
- **Exchange/session resolution handles Bloomberg time-valued metadata**: Exchange metadata parsing now accepts Arrow time columns for `TRADING_DAY_START_TIME_EOD` / `TRADING_DAY_END_TIME_EOD`, avoids futures-only metadata field requests on ordinary equities, preserves futures fallback via `FUT_TRADING_HRS`, and applies the Japan equity `09:00-15:30` session rule so `market_timing(..., "EOD", "UTC")` resolves to the expected Tokyo close.
- **Options live tests derive valid contracts dynamically**: The options extension live suite no longer relies on stale hardcoded SPY expiry/strike fixtures; it discovers a current low-data SPY call through a narrow `option_chain()` request and reuses that valid ticker/expiry/strike for info, greeks, pricing, screen, and BQL-chain checks.

- **`bdtick` include-code options now keep typed tick tables (#309)**: `IntradayTickState` dynamically discovers scalar fields inside Bloomberg's `tickData.tickData[]` rows, so options such as `includeConditionCodes`, `includeExchangeCodes`, and `includeBloombergStandardConditionCodes` add typed columns after the stable core `[ticker, time, type, value, size]` instead of forcing callers into generic `[path, type, value_str, value_num]` output. Dynamic columns are padded with nulls for ticks where Bloomberg omitted that field; response metadata such as `tickData.eidData` remains excluded from per-tick rows.
- **`bds` bulk rows discover subfields across the whole response**: `BulkDataState` now scans every scalar child in each Bloomberg bulk row instead of freezing the output schema from the first row. Late-appearing subfields are appended in first-seen order and earlier rows are padded with nulls, preserving row alignment for dynamic bulk datasets.
- **`bds` manually selected bulk extraction could be overwritten by defaults**: `RequestParams::with_defaults()` now preserves an explicit non-default extractor hint, preventing bulk requests from falling back to reference-data long extraction when callers build request params manually.
- **Pixi/libclang bindgen discovery on Windows**: Shared build support now creates an `OUT_DIR`-local `libclang.dll` alias for pixi/conda's versioned `libclang-*.dll`, so all bindgen build scripts can run without manually installing LLVM or mutating the pixi environment.
- **Live reference-data tests and benchmarks used the wrong Bloomberg array accessor**: `securityData` value arrays now use `get_element(0)` rather than child-element lookup, matching the SDK response shape.
- **`@xbbg/core` TypeScript package metadata repaired**: Native optional dependencies now use package versions instead of local `file:` links, release scripts use a checked-in CJS platform map helper, packaged-install smoke checks the published `dist` entrypoint, and the npm package includes the Apache license.
- **`@xbbg/core` local Windows runtime loading fixed**: The Node binding now adds the vendored Bloomberg SDK runtime DLL directory from `vendor/blpapi-sdk/<version>` (or `XBBG_DEV_SDK_ROOT`) to `PATH` before loading `napi_xbbg.node`, so local tests do not require a manually exported `BLPAPI_ROOT`.
- **Python subscription unsubscribe keeps reusable workers clean**: `PySubscription.unsubscribe()` now propagates Bloomberg unsubscribe failures instead of suppressing them and only clears active subscription status after termination succeeds, so clean explicit unsubscribes return the subscription worker to the pool while failed/implicit cleanup keeps the conservative discard path.
- **`xbbg-async` async boundaries no longer perform cache disk I/O on hot Tokio paths**: Request kwarg routing now uses memory-only schema metadata, explicit schema loads/persists are offloaded to blocking workers, field and exchange caches preload during engine startup, and exchange cache persistence snapshots entries before filesystem writes instead of holding cache locks across I/O.
- **Rust subscription cleanup preserves clean worker reuse and avoids blocking drop flushes**: `SubscriptionStream::unsubscribe()` now clears active status after successful termination before the claim drops, matching the Python/NAPI clean-close path, while `SubscriptionState::Drop` uses best-effort `try_send` so `OverflowPolicy::Block` cannot block the subscription worker during cleanup.
- **Dynamic extractor hot paths avoid repeated linear duplicate scans and JSON clones**: `bds` bulk rows and `bdtick` dynamic columns now track discovered fields with membership sets while preserving output order, and BQL JSON parsing stores borrowed intermediate values where safe before building owned Arrow arrays.

## [1.1.2] - 2026-04-20

### Fixed

- **`bdh` / `bdp` with `format='semi_long'` dropped Int64-typed fields (#303)**: Bloomberg sends integer-typed fields (`PX_VOLUME`, `OPEN_INT`, etc.) as Float64 on the wire in HistoricalDataResponse even though FieldInfo declares them `Int64`/`Long`. `crates/xbbg-core/src/value.rs::Value::as_i64` (and its `OwnedValue` twin) and the inline `TypedBuilder::Int32::append_value` match in `crates/xbbg-async/src/engine/state/typed_builder.rs` had no Float64 arm, so the wide-path Int builder null-filled those columns. Consequence: `blp.bdh("ESH20 Index", flds=[..., "PX_VOLUME", "OPEN_INT"], format='semi_long')` returned NaN for every volume / open-interest row. `long` / `long_typed` / `long_metadata` were unaffected because their builders route via Float64 or stringify. Fixed by accepting `Float64` when it's finite, has `fract()==0.0`, and fits the target integer range. `TestOutputFormats::test_bdh_semi_long_integer_fields_issue_303` locks this in live, plus existing `bdp`/`bdh` `semi_long` tests now assert `notna().all()` per column instead of just column names.

## [1.1.1] - 2026-04-20

### Added

- **`@xbbg/core`: recipe helpers exposed on the JS `Engine`**: Eleven recipe methods surfaced through the NAPI bindings — `yas`, `preferreds`, `corporateBonds`, `futTicker`, `activeFutures`, `cdxTicker`, `activeCdx`, `dividend`, `turnover`, `etfHoldings`, `currencyConversion` — wrapping the corresponding `xbbg_recipes` entry points. Returns Arrow `Table` by default with `Backend.JSON` / `Backend.POLARS` opt-in via `options.backend`; errors route through the standard `BlpError` hierarchy. Ships with TypeScript definitions (`YasOptions`, `PreferredsOptions`, `CorporateBondsOptions`, `FuturesResolveOptions`, `ActiveCdxOptions`, `DividendOptions`, `TurnoverOptions`, `EtfHoldingsOptions`, `RecipeBackendOptions`), README usage examples, and smoke-test coverage in `js-xbbg/test.js`.
- **Prebuilt cross-platform offline bundles for `@xbbg/core`** (`scripts/build-offline-bundle.js`): Packages `@xbbg/core` plus the prebuilt `@xbbg/core-<label>` native addon into a hoisted `bundle/node_modules` tree alongside source tarballs for air-gapped installs. `js_github_release.yml` gains a `pack-offline-bundles` job that attaches `xbbg-offline-<label>-<version>.zip` to the GitHub release. The job is checked by a release-payload scanner intended to catch accidental Bloomberg SDK inclusion; Bloomberg SDK/runtime files are not bundled. `ci-rust.yml` mirrors the job per-commit with a 7-day artifact retention for downstream consumers.

### Changed

- **`EngineConfig.request_timeout_ms` default changed from `60_000` to `0` (disabled)**: The previous 60s hard cap was self-inflicting timeouts on legitimately long requests — e.g. a full-day `bdtick` for a liquid future routinely exceeds 60s on the Bloomberg side, so the worker was cancelling healthy requests and surfacing a `BlpTimeoutError` to the caller. The enforcement machinery is unchanged; callers who want a hard upper bound must now opt in explicitly by passing `request_timeout_ms=<ms>` (Python), `requestTimeoutMs` (NAPI), or `PyEngineConfig.request_timeout_ms` (pyo3).

### Fixed

- **`bdtick` / `bdib` silently dropped `overrides=` kwargs (#295)**: `_build_abdtick_plan` and `_build_abdib_plan` in `py-xbbg/src/xbbg/blp.py` were doing `elements, _ = await _aroute_kwargs(...)` — the `_` threw away the overrides list before the request reached the Rust engine. Other endpoints (`bdp`/`bdh`/`bds`/`beqs`/`bport`) capture both; only the two intraday builders discarded overrides. Now forwarded. Note that Bloomberg's `IntradayTickRequest` / `IntradayBarRequest` schemas have no `overrides` sub-element, so forwarded overrides now surface as a Bloomberg `element-not-found` error instead of being silently no-oped; for response-size limits use the top-level `maxDataPoints` kwarg instead.

- **`bdib` + `maxDataPoints` fell back to the generic flattener, losing the typed schema**: `crates/xbbg-async/src/engine/worker.rs` routed intraday-bar / tick requests through `GenericState` whenever _any_ user-supplied element was set, on the assumption that extra elements imply extra response columns. That holds for tick `include*` flags (condition codes, exchange codes, etc. which add per-tick columns), but not for behavior-only elements like `maxDataPoints`, `maxDataPointsOrigin`, `gapFillInitialBar`, or `adjustment*` — those don't change the response shape. Consequence: `blp.bdib(..., maxDataPoints=1)` returned `[path, type, value_str, value_num]` instead of the typed `[ticker, time, open, high, low, close, volume, numEvents]`, and `blp.bdtick(..., maxDataPoints=1)` returned 6 rows instead of 1 (the generic extractor exploded one tick into per-field rows). Fallback removed entirely for `IntradayBar` (no column-adding elements exist on `IntradayBarRequest`); narrowed to `include*` keys on `IntradayTick`.

- **Offline-bundle packing rejected by npm with `EBADPLATFORM`**: `npm install` in the `pack-offline-bundles` job runs on a Linux runner but pulls in `@xbbg/core-<label>` packages that declare `os`/`cpu` for their target platform (e.g. `win32`/`x64`). `scripts/build-offline-bundle.js` now passes `--force` so the cross-platform install succeeds; the bundle is never executed on the install host, so the platform check is safe to skip.

- **`bdp` / `bdh` silently returned long-shape output for `format='semi_long'` (#296, #299)**: `crates/xbbg-async/src/engine/worker.rs` had no `"semi_long"` arm in its format-string match — the `RefData` branch hardcoded `OutputFormat::Long` and only varied `LongMode`; the `HistData` branch only recognised `"wide"`. So `blp.bdp(..., format='semi_long')` returned `[ticker, field, value]` instead of the documented `[ticker, <field1>, <field2>, …]` pivoted shape, and `blp.bdh(..., format='semi_long')` returned `[ticker, date, field, value]` instead of `[ticker, date, <field1>, …]`. The `Format::SemiLong` enum in `services.rs` parsed `"semi_long"` round-trip correctly; the break was purely in the worker routing. Fixed by mapping `"semi_long" | "wide"` → `OutputFormat::Wide` in both arms. Regression coverage: new `TestOutputFormats` class in `py-xbbg/tests/live/test_api.py` asserts column shape for all four `Format` variants (`long`, `semi_long`, `long_typed`, `long_metadata`) on both `bdp` and `bdh`, verified live against Bloomberg.

## [1.1.1b1] - 2026-04-18

### Added

- **BQL `secondaryColumns` extracted from responses** (#288, refs #289 / #290 / #291): `parse_bql_json` in `xbbg-async` now pulls `DATE`, `CURRENCY`, and other secondary dimensions out of BQL JSON so time-series queries like `with(dates=range(-5d, 0d))` return usable row labels instead of ambiguous duplicate-ticker rows. Three latent parser issues fixed in the same pass: column lengths are now clamped to `idColumn` size via `resize()` so partial errors with mismatched field lengths stop failing `RecordBatch::try_new` (#289); column typing now uses Bloomberg's `valuesColumn.type` metadata with `.all()` fallback instead of value-sniffing with `.any()` (#290); a warning is logged when BQL falls through to the legacy Element-API path where `secondaryColumns` are unavailable (#291).

- **`xbbg-async`: per-subscription availability tracking via `SubscriptionStreamsActivated`/`SubscriptionStreamsDeactivated`**: Bloomberg SDK v3.11.6+ recovers subscriptions internally across transient disconnections; the ChangeLog explicitly instructs applications to use the `Streams*` events to detect failover. xbbg now consumes both and exposes per-topic `streams_active` on `TopicStatusInfo`. A debounced Warning event (`SubscriptionStreamsDeactivatedPersisting`) fires when a topic stays streams-inactive past `streams_deactivated_warn_ms`, so callers polling status can tell "SDK is still recovering" from "data is dead". See `.omc/research/reconnect-correctness.md` for the full SDK-contract evidence trail.
- **`EngineConfig.request_timeout_ms`** (default `60_000`; `0` disables): Hard per-request upper bound. Request workers now cancel the Bloomberg request and fail the oneshot with `BlpError::Timeout` when the timeout expires, guaranteeing callers cannot hang forever on a stuck response regardless of SDK or server-side misbehavior. Exposed through all three bindings: Python `request_timeout_ms`, NAPI `requestTimeoutMs`, pyo3 `PyEngineConfig.request_timeout_ms`.
- **`EngineConfig.streams_deactivated_warn_ms`** (default `30_000`; `0` disables): Threshold for the per-topic streams-inactive warning described above. Exposed as `streamsDeactivatedWarnMs` in NAPI and `streams_deactivated_warn_ms` in pyo3/Python.
- **Worker health on `SubscriptionWorker`**: Mirrors the existing `RequestWorker` health field. Goes to `Dead` on `SessionTerminated`; the pool's `claim()` and `release()` paths drop Dead handles and spawn fresh replacements so a wave of `SessionTerminated` events cannot permanently cripple the engine.
- **`reconnect_probe` example** (`crates/xbbg-core/examples/reconnect_probe.rs`): Observational tool that subscribes to a live ticker and logs every `SessionStatus` / `SubscriptionStatus` event with timestamps and `reason.description` for validating reconnect behavior against a real Bloomberg session.
- **`xbbg-bench`: `benches/cache_contention.rs`**: Harness measuring `SchemaCache::get` and `FieldTypeResolver::get` latency percentiles under reader/writer pressure at 10/100/1000 reader concurrency, one writer inserting every 5ms, 2-second runs. Produces p50/p99/p99.9/max tables via `hdrhistogram` and saves them to `target/bench_cache/<BENCH_LABEL>.txt` for before/after diffing. Run with `DYLD_LIBRARY_PATH=vendor/blpapi-sdk/3.26.2.1/Darwin BENCH_LABEL=<name> cargo bench -p xbbg-bench --bench cache_contention`.
- **Keep-alive tuning on `EngineConfig`**: `keep_alive_enabled` (bool, default `true`), `keep_alive_inactivity_ms` (`Option<i32>`, SDK default 20s), `keep_alive_response_timeout_ms` (`Option<i32>`, SDK default 10s). Default SDK window of ~30s silence before `SessionConnectionDown` is aggressive for VPN/WAN BPIPE connections; raising these prevents spurious disconnects without changing local-Terminal behavior. Exposed through pyo3 / NAPI / Python.
- **Slow-consumer water marks on `EngineConfig`**: `slow_consumer_hi_water_mark` and `slow_consumer_lo_water_mark` (`Option<f32>`, fractions of `max_event_queue_size`). SDK defaults 0.75 / 0.5. Exposed through all three bindings (`slowConsumerHiWaterMark` / `slowConsumerLoWaterMark` in NAPI with 0.0..=1.0 / 0.0..1.0 validation).
- **`AuthorizationRevoked` handling during live session**: Previously only checked at startup (`crates/xbbg-core/src/session.rs::wait_until_started`). If identity was revoked mid-session (token expiry, policy change), requests silently failed with opaque `RequestFailure` and subscriptions silently stopped. Both `RequestWorker` and `SubscriptionWorker` now handle `AuthorizationRevoked` symmetrically to `SessionTerminated`: drain in-flight/subscriptions with a dedicated "please re-authenticate" error, mark the worker `Dead`, and let the pool spawn a fresh worker that re-auths during startup.
- **`ServiceDown` now emits a subscription-category warning when active subs exist**: Previously only recorded on service status. Callers polling `sub.events` missed the signal that their streams were affected until `SubscriptionStreamsDeactivated` fired per topic. Now a single `ServiceDownAffectsActiveSubscriptions` event fires at the moment of `ServiceDown` so callers see immediately that their data may go quiet.
- **`reason.description` parsed on `SubscriptionStarted`**: Bloomberg sometimes includes partial-permission details (e.g. "only delayed data authorized") on the `reason` element of `SubscriptionStarted`. These were discarded; now surfaced via the status event `detail`.

### Changed

- **`EngineConfig` transport surface restructured as a `Transport` enum (internal Rust only; Python/JS kwargs unchanged)**: The six flat transport fields (`server_host`, `server_port`, `servers`, `zfp_remote`, `socks5_host`, `socks5_port`) and five flat TLS fields (`tls_client_credentials`, `tls_client_credentials_password`, `tls_trust_material`, `tls_handshake_timeout_ms`, `tls_crl_fetch_timeout_ms`) on `xbbg_async::EngineConfig` collapsed into `transport: Transport` + `tls: Option<TlsConfig>`. `Transport::Direct(Vec<ServerAddr>)` carries per-server SOCKS5 (matching `blpapi::SessionOptions::setServerAddress(serverHost, serverPort, socks5Config, index)` in `vendor/blpapi-sdk/3.26.2.1/include/blpapi_sessionoptions.h:501-511`); `Transport::Zfp(ZfpRemote)` takes no server addresses by construction, so the #294 bug class is unrepresentable in the type system. Conflict validation lives at the PyO3 / NAPI boundary — `resolve_transport` rejects `zfp_remote` combined with `host`/`port`/`servers`/`socks5_*` with specific `ValueError`/`InvalidArg` messages. `start_configured_session` is now a three-stage pipeline (transport → optional TLS re-apply → session-behavior config) with no shared mutation, matching Bloomberg's own canonical demo-app structure (`vendor/blpapi-sdk/.../examples/demoapps/util/ConnectionAndAuthOptions.h:213-296`). TLS is applied once at the SDK level: through `ZfpUtil::getOptionsForLeasedLines` for ZFP, via `SessionOptions::setTlsOptions` for Direct; the previous double-apply (ZfpUtil + inline `set_tls_options`) is eliminated. The engine tracing span now logs `transport = %config.transport` via a new `Display` impl on `Transport`/`ZfpRemote` (e.g. `localhost:8194`, `primary.example.com:8194 (+2 failover)`, `zfp:8194`). All public Python `configure()` kwargs and `@xbbg/core` `EngineConfigInput` fields are unchanged — the flat surface is preserved and converted at the binding boundary.

- **`xbbg-async` cache hot paths: `RwLock<HashMap>` → `ArcSwap<HashMap>` (schema) and `DashMap` (field)**: `SchemaCache` reads are now lock-free atomic pointer loads; writes publish a new snapshot via RCU. `FieldTypeResolver` uses `DashMap`'s sharded internal locking (plus `OnceLock` for the lazy disk-load flag). Public API unchanged; all 100 `xbbg-async` lib tests pass. Measured via `benches/cache_contention.rs` (2s run, 1 writer inserting every 5ms, artifacts in `target/bench_cache/`):

  | Scenario             | p50            | p99            | **p99.9**                     | throughput       |
  | -------------------- | -------------- | -------------- | ----------------------------- | ---------------- |
  | schema, 1000 readers | 1.96µs → 125ns | 17.1µs → 667ns | **22.5ms → 1.2µs (~19 000×)** | 22× more samples |
  | schema, 100 readers  | 1.71µs → 166ns | 19.8µs → 667ns | **6.5ms → 1.2µs (~5 400×)**   | 14× more samples |
  | schema, 10 readers   | 1.75µs → 166ns | 16.1µs → 500ns | **56µs → 750ns (75×)**        | 14× more samples |
  | field, 1000 readers  | 2.67µs → 125ns | 19.2µs → 583ns | **30.4ms → 1.2µs (~25 000×)** | 20× more samples |
  | field, 100 readers   | 2.58µs → 125ns | 20.4µs → 583ns | **11.1ms → 1.2µs (~9 200×)**  | 20× more samples |
  | field, 10 readers    | 2.50µs → 125ns | 18.4µs → 583ns | **52µs → 1.1µs (46×)**        | 19× more samples |

  Eliminates the writer-thundering-herd pathology where a `~1µs` write-lock window would queue hundreds of readers and blow out p99.9 into the millisecond range. Relevant under burst load — e.g. many parallel `//blp/refdata` requests landing during a `//blp/apiflds` introspection, where schema/field lookups are on every critical-path hop. p50 and p99 also improve because the RwLock acquire/release was the dominant cost of an uncontended cache hit. `max` is still OS-scheduler jitter (threads can be preempted for a full quantum) and unrelated to cache design.

- **`ensure_service` switched from synchronous `openService` to `openServiceAsync` + nested event dispatch**: The synchronous `blpapi_Session_openService` internally blocks on the session's event queue, which stalls delivery of every other in-flight event for the duration of the call. Measured locally against a Bloomberg Terminal: `open_service` takes 200-300ms per call, post-call delivery rates spike to 1.6-2.3× baseline (consistent with queue-and-release). Worker threads now call `open_service_async`, tag replies with a dedicated high-bit-set correlation ID (`1 << 62`), and run a nested dispatch loop that continues to process `SubscriptionData` / `SessionStatus` / `RequestStatus` while waiting for `ServiceOpened`. Both `SubscriptionWorker::ensure_service` and `RequestWorker::ensure_service` are affected. Added `Session::open_service_async` on `xbbg-core` with a `BlpError::Timeout` after `SERVICE_OPEN_TIMEOUT_MS` (10s default).

### Changed

- **`SessionConnectionDown`/`Up` are now treated as informational on the subscription path** (matching Bloomberg's canonical guidance: `vendor/blpapi-sdk/.../examples/unittests/snippets/events/events.t.cpp:42-54` — "Applications can safely ignore… These events are informational only and applications should not react to them"). Only `SessionTerminated` drains active subscriptions and marks the worker `Dead`. The SDK's own auto-restart + internal subscription recovery handle transient network blips.
- **Request-side handling preserves drain-on-Down semantics** but marks workers `Degraded` (not `Dead`): requests are transactional, so a response mid-transit when TCP drops is lost and must be failed fast. On `SessionConnectionUp` the worker flips back to `Healthy`. On `SessionTerminated`, it drains and goes `Dead` with pool replacement.
- **`BlpError` produced on `RequestFailure` and `SessionTerminated` now includes Bloomberg's `reason.description`** instead of opaque `"RequestFailure"` / `"Bloomberg session terminated"` strings. Same parser shape as the existing `startup_error_from_message` helper.
- **`SubscriptionSessionPool::claim` drops dead handles during the pop loop** and spawns a fresh replacement if every available handle is Dead. `release()` discards Dead handles instead of returning them to the pool.

### Fixed

- **ZFP over leased lines failed with `Failed to connect to 127.0.0.1:8194` (#294)**: `blp.configure(zfp_remote="8194", tls_client_credentials=..., tls_trust_material=...)` started a session but the SDK tried to connect to `localhost:8194` instead of the Bloomberg infrastructure endpoints. Root cause in `crates/xbbg-async/src/engine/mod.rs`: `start_configured_session` called `configure_zfp_options` (which populates the `SessionOptions` server list via `ZfpUtil::getOptionsForLeasedLines`), then unconditionally called `configure_session_options`, whose server-address loop overwrote index 0 with the `server_host`/`server_port` fallback of `localhost:8194`. The SDK contract (`vendor/blpapi-sdk/3.26.2.1/include/blpapi_zfputil.h:154-162`) explicitly states the `SessionOptions` returned by `ZfpUtil` is "only valid for private leased line connectivity" — layering `setServerAddress` on top is undefined behavior. Two related latent bugs in the same code path are also fixed: passing `servers=[…]` alongside `zfp_remote` would clobber ZFP endpoints at indices 0..N, and passing `socks5_host` alongside `zfp_remote` wrapped the overwritten localhost address in SOCKS5 semantics. All three combinations now raise `ValueError` at `configure()` time with specific messages instead of producing silent connection failures. See the `Changed` entry below for the underlying refactor.

- **Silent subscription death after a transient blip**: With the default `SubscriptionRecoveryPolicy::None`, a `SessionConnectionDown` → `Up` cycle would silently produce a live session with zero data flowing — the SDK reconnected TCP, but xbbg never re-established subscriptions, and `sub.next()` hung forever. The SDK actually recovers subscriptions itself (v3.11.6+); xbbg just needed to stop fighting it and consume the `SubscriptionStreams*` events. Fixed by the above refactor.
- **Aggressive drain on transient `SessionConnectionDown` for subscription workers**: The previous handler marked subscription workers `Dead` and drained all in-flight subscriptions on every transient Down event, contradicting Bloomberg's "informational" contract. Now a no-op on the subscription side.
- **Lifetime leak of `recovery_attempt_count`**: `record_recovery_success` never reset the counter, so `max_recovery_attempts=3` became a process-lifetime cap, permanently disabling recovery after three flaps. Removed along with the rest of the recovery machinery (see `Removed`).
- **`SubscriptionWorker` with a terminated session remained claimable from the pool**: the worker drained its subs but had no `health` field, so a subsequent `claim()` would hand out a handle whose underlying session pointer was dead. Fixed by adding health tracking and the pool-level eviction + replacement path.
- **Live subscription tests hardcoded to expired `H6` (March 2026) futures contracts**: `ESH6` / `NQH6` / `UXH6` returned `Security is not valid for subscription [EX336]` from Bloomberg after 2026-03-20. Switched to generic front-month continuation tickers (`ES1` / `NQ1` / `UX1`) which Bloomberg auto-rolls; matches the pattern already used in `js-xbbg/test-live.js`.
- **`crates/xbbg-core/tests/live.rs` checked for `"SessionResumed"`**: Not a real BLPAPI message name — zero matches in the locally staged SDK headers. The check never fired because `SessionStarted` always arrived first; removed to match the canonical event set.

### Removed

- **`Message::topic_name()`**: Bloomberg deprecated `Message::topicName()` in BLPAPI SDK 3.14.8 — the method always returns an empty string, and the SDK docs explicitly instruct callers to maintain their own CID→topic map (`include/blpapi_message.h:253-274`). Subscription workers already do this via the slab. No production code called it.
- **`SubscriptionRecoveryPolicy` enum + `recover_active_subscriptions` + `recovery_*` fields/methods**: Removed entirely. The SDK recovers subscriptions internally; xbbg's parallel implementation fought the SDK (would re-subscribe with in-use correlation IDs → `correlationIdError`) and its default of `None` silently dropped subs across blips. Deleted together: `SubscriptionRecoveryPolicy`, `recover_active_subscriptions`, `recovery_attempt_count`, `recovery_success_count`, `last_recovery_attempt_us`, `last_recovery_success_us`, `last_recovery_error`, `record_recovery_attempt`, `record_recovery_success`, `record_recovery_error`, and `SessionStatusInfo.recovery_policy`. **BREAKING**: drops the `recovery_policy` kwarg from `asubscribe` / `astream` / `subscribe_with_options` in Python, pyo3, and NAPI, plus the corresponding fields from `sub.session_status`.
- **`EngineConfig.max_recovery_attempts`, `EngineConfig.recovery_timeout_ms`, `EngineConfig.health_check_interval_ms`**: All three were defined, documented, and exposed through all three bindings (pyo3 / NAPI / py-xbbg), but zero consumers existed in the engine — dead config surface that misled callers. Removed from all three bindings. **BREAKING** for any caller that set these; the behavior they advertised wasn't actually implemented.

### Fixed (js-xbbg 1.1.x follow-ups)

- **`@xbbg/core`: `Engine.bdp`/`bds`/`bdh`/`bdib`/`bdtick` ignored `options.backend`**: The five core reference/historical/intraday methods did not forward `backend` into `Engine.request`, so callers asking for `Backend.JSON` or `Backend.POLARS` silently received Arrow `Table`s regardless. `backend` is now threaded through all five methods and the corresponding `BdpOptions`/`BdhOptions`/`BdibOptions`/`BdtickOptions` types in `index.d.ts`. Verified live against a Bloomberg session.
- **`@xbbg/core`: `Engine.requestRaw` and `Subscription.add`/`remove` did not wrap native errors**: `Engine.request` and `Subscription.next` routed native rejections through `wrapError`, but the raw request path and subscription mutators did not. Callers discriminating on `BlpError`/`BlpRequestError` subclasses missed failures from those paths. All three now wrap consistently.
- **`@xbbg/core`: session-start failures surfaced as plain `Error`**: `connect(...)`, `new Engine(...)`, and `Engine.withConfig(...)` no longer bypass `wrapError`, and `wrapError` now matches the actual NAPI session-start messages (`"failed to spawn worker"`, `"session start failed"`, `"Failed to start session"`, `"connect event failed"`). Failed connects now classify as `BlpSessionError`.
- **`@xbbg/core`: `BlpRequestError.request_id` was never populated**: The Rust engine appends ` [request_id=<uuid>]` to request-failure messages when a correlation id exists, but `wrapError` did not parse it. The id is now extracted into `err.request_id`.
- **`@xbbg/core`: a transient connect failure permanently poisoned every top-level helper**: `getConfiguredEngine` cached the first `connect(...)` promise unconditionally, so a rejected bootstrap produced the same rejection on every subsequent call until `configure()` was invoked again (observed live as 7.2s first attempt followed by 0ms cached rejection). Rejected promises are now cleared so the next call re-attempts.

### Security

- **`rand` bumped to 0.9.3 in `Cargo.lock` (GHSA-cq8v-f236-94qc / RUSTSEC-2026-0097, low)**: The advisory is scoped to runtime `rand::rng()` use from inside a custom `log` implementation that triggers `ThreadRng` reseeding — not something xbbg exercises. The remaining `rand 0.8.5` in the graph is build-time only (`phf_generator` for `phf` macros, `unicode_names2_generator` for `pyo3-stub-gen`), outside the advisory's exposure surface.

## [1.1.0] - 2026-04-14

### Added

- **`xbbg-mcp` local MCP server**: Added a stdio Bloomberg MCP application under `apps/xbbg-mcp` with tool surfaces for `bdp`, `bdh`, `bds`, `bdib`, `bql`, `bsrch`, `bflds`, and generic request execution. Responses are bounded structured JSON with Arrow schema metadata for coding agents.
- **GitHub-release MCP distribution path**: Added release packaging for `xbbg-mcp`, a Unix launcher wrapper (`scripts/xbbg-mcp`), and a convenience installer (`scripts/install-xbbg-mcp.sh`) so Claude Code and OpenCode users can install a local MCP binary without cloning or compiling the repo first.
- **`@xbbg/core` Node.js package**: New first-class JavaScript/TypeScript client under `js-xbbg/` that wraps the Rust engine via NAPI. Exposes the full request surface (`bdp`, `bdh`, `bds`, `bdib`, `bdtick`, `bql`, `bqr`, `bsrch`, `beqs`, `blkp`, `bport`, `bcurves`, `bgovts`, `bflds`), a typed error hierarchy, optional backends (Apache Arrow tables by default, `nodejs-polars` as an optional peer), and a BPIPE/auth-aware `configure()`. Native addons are prebuilt and distributed as optional platform packages (`@xbbg/core-darwin-arm64`, `@xbbg/core-linux-x64`, `@xbbg/core-win32-x64`) so `npm install @xbbg/core` Just Works without a Rust toolchain.
- **`@xbbg/bridge` async browser bridge**: New companion package exposing the Rust engine through an async postMessage bridge, shipped alongside `@xbbg/core` with matching platform-specific native addons.
- **GitHub-only JS package release workflow**: Added a manual `js_github_release.yml` path that builds, versions, validates, and attaches GitHub release tarballs for `@xbbg/core` and `@xbbg/bridge` without npm publishing. The workflow intentionally ships the currently supported 8-asset set only: `@xbbg/core` wrapper plus `darwin-arm64`/`linux-x64`/`win32-x64`, and `@xbbg/bridge` wrapper plus `darwin-arm64`/`linux-x64`/`win32-x64`. The unreleased `@xbbg/bridge-darwin-x64` and `@xbbg/bridge-linux-arm64` package stubs remain excluded until Bloomberg SDK archive support exists.
- **Friendlier `AttributeError` for removed `blp` legacy APIs**: `blp.connect`, `blp.disconnect`, and `blp.getBlpapiVersion` now raise an `AttributeError` whose message points directly at the 1.0 replacement (`xbbg.configure`, `xbbg.shutdown`/`xbbg.reset`, `xbbg.get_sdk_info`) with a copy-pasteable B-PIPE example, instead of the bare "module has no attribute" default. Implemented via a module-level `__getattr__` hook in `py-xbbg/src/xbbg/blp.py`.

### Changed

- **Backend conversion moved to a single boundary with `pa.Table` as canonical form**: `_execute_request_terminal` now returns the raw `pa.Table` from the Rust engine without wrapping in narwhals first; `arequest` does a single `_convert_backend` call at its return. `_convert_backend` dispatches directly from `pa.Table` via zero-copy primitives (`pl.from_arrow`, `table.to_pandas`, identity for `pa.Table → pa.Table`), bypassing the narwhals wrap/unwrap on the hot path. Short-circuiting middlewares that return non-`pa.Table` values (e.g. caches returning lists) keep full control over their result. Measured on a 1000×10 frame: `pa.Table → pa.Table` went from 17.78 µs to 0.04 µs (464×), `pa.Table → pl.DataFrame` from 39.89 µs to 18.67 µs (2.1×). Subscriptions streaming at 10k msgs/sec previously spent ~33% of a core on redundant wrap/unwrap in the dispatch layer; this cuts it roughly in half. Narwhals remains the canonical abstraction for backend-agnostic data manipulation in `ext/historical.py`, `ext/currency.py`, `ext/futures.py`, `ext/_utils.py`, and `_reshape_bqr_generic` — this refactor only removes it from the pure routing path where it added overhead without value.
- **Minimum `narwhals` version bumped to `>=2.0`**: Required for the `nw.Implementation` enum and `.implementation` property used by the new `_convert_backend` dispatch. Downstream users pinned to narwhals 1.x will need to upgrade. No other narwhals 2.x breaking changes affect xbbg — `get_native_namespace`, `new_series(native_namespace=)`, `from_native` idempotency, and `.to_native()` all remain stable across the 1→2 boundary.
- **`xbbg.configure()` rejects unknown kwargs**: `configure()` now raises `TypeError` on any keyword it does not recognize. Previously unknown kwargs were silently dropped by the Rust `PyEngineConfig` constructor, which meant typos (e.g. `hots=...` instead of `host=...`) would leave the host at the default without any warning. The Python normalizer now validates the kwarg set against the canonical field list before handing off to Rust.
- **Docs site restructured with Python/JavaScript split**: The Starlight site under `docs/` now has distinct `python/` and `javascript/` sections, an auto-generated `releases/changelog.mdx` page (`scripts/generate-changelog-docs.sh`), and auto-generated Python API reference (`scripts/generate-python-api-docs.sh`, renamed from `generate-api-docs.sh`). The `deploy-docs.yml` workflow now drives publishing automatically, and the host build no longer depends on `sharp` so it works on macOS without libvips.

### Fixed

- **Incorrect value types in `bdp`/`bdh` long format (issue #280)**: The default long format (`LongMode::String`) was converting all Bloomberg values to strings, ignoring resolved `field_types`. Now the Rust engine computes a common Arrow type from the field type hints at construction time — when all fields are numeric, the `value` column is `Float64` instead of `Utf8`. Mixed-type queries (e.g., numeric + string fields) gracefully fall back to string. The fix is zero-copy: `Value` is moved into the Arrow builder instead of being stringified and re-parsed.
- **macOS/Linux: `import xbbg._core` fails with `Library not loaded: @rpath/libblpapi3_64.so` (issue #276)**: The pyo3 cdylib ships with zero `LC_RPATH` entries on macOS, so dyld had nowhere to look for `libblpapi3_64.so` at import time. Previously only Windows had pre-import SDK setup in `__init__.py`. Now `_prepare_sdk_for_core_import()` dispatches per-platform: Windows keeps the existing `add_dll_directory` path, while macOS and Linux preload `libblpapi3_64.so` via `ctypes.CDLL(..., RTLD_GLOBAL)` so dyld/ld.so resolves the `@rpath` reference via install-name / already-loaded image matching. This mirrors the idiom Bloomberg's own `blpapi/internals.py` uses for its `ffiutils` extension (which also ships with no rpath). All four SDK sources (`xbbg.set_sdk_path()`, `blpapi` package, DAPI, `BLPAPI_ROOT`) are now honored on every platform. The friendly `ImportError` wrapper also recognizes macOS dlopen error strings (`Library not loaded`, `image not found`).
- **`RequestEnvironment.zfp_remote` type annotation**: Corrected the dataclass field annotation from `int | None` to `str | None` to match the Rust `Option<String>` (ZFP remote values are strings like `"8194"`/`"8196"`). The defensive `getattr()` access in `_snapshot_request_environment` had been masking this type mismatch from static analyzers.
- **`_convert_backend` no longer hard-imports polars**: A follow-up to the Arrow-canonical refactor accidentally replaced the intentional `hasattr(native, "to_arrow")` capability check in the pyarrow branch with an unconditional `isinstance(native, pl.DataFrame)`, which forced `import polars as pl` at module-load time and broke environments where polars (an optional backend) isn't installed. Restored the capability check, short-circuiting pandas inputs via `isinstance` first so the `hasattr` path only fires for genuine polars frames.
- **Polars/pyarrow global backend causes `AttributeError` in all generated endpoints (issue #287)**: `_execute_generated_endpoint` was effectively calling `_convert_backend` twice on the same frame — once inside the middleware terminal (which resolved `backend=None` to the global default and returned a native frame) and again in the outer call, which tried `nw_df.to_native()` on the already-native frame. Pandas users were silently masked by an `isinstance(nw_df, pd.DataFrame)` short-circuit at the top of `_convert_backend`; polars and pyarrow users had no equivalent guard and saw `AttributeError: 'DataFrame' object has no attribute 'to_native'`. Affected all 14 generated endpoints (`bdp`/`abdp`, `bdh`/`abdh`, `bds`/`abds`, `bdib`/`abdib`, `bdtick`/`abdtick`, `bql`/`abql`, `bqr`/`abqr`, `bsrch`/`absrch`, `beqs`/`abeqs`, `blkp`/`ablkp`, `bport`/`abport`, `bcurves`/`abcurves`, `bgovts`/`abgovts`, `bflds`/`abflds`) — not just `bdp` as reported. Verified end-to-end against a real Bloomberg Terminal.
- **`ext/futures.py` date-like duck-typing**: Collapsed three `hasattr(value, "year"/"month"/"day")` calls into a single `isinstance(value, date)` check now that `date` is imported unconditionally. More precise and removes false positives from unrelated objects that happen to expose `.year`.
- **`markets/{info,bloomberg}.py` imports**: Replaced `importlib.import_module("xbbg")` + `getattr(..., "bdp"/"abdp")  # noqa: B009` with deferred `from xbbg.blp import bdp/abdp` inside the consuming functions. Same lazy-loading behavior, removes the lint suppression, and lets static checkers resolve the symbol.
- **Incorrect timestamp in `parse_rfc3339_utc` test**: Fixed hardcoded expected value from `1717242600` to `1717252200` (correct UTC epoch for `2024-06-01T14:30:00+00:00`).

### Removed

- **Legacy `configure()` kwarg aliases**: `xbbg.configure()` no longer accepts the legacy connection-style aliases carried over from xbbg 0.x: `server`, `server_host`, `server_port`, `max_attempt`, `auto_restart`, `max_recovery`, `retry_max`, `retry_delay`, `retry_backoff`. The `NotImplementedError` placeholders for `sess` and `tls_options` are likewise gone — unknown kwargs now raise a uniform `TypeError`. Use the canonical `EngineConfig` field names instead: `host`, `port`, `num_start_attempts`, `auto_restart_on_disconnection`, `max_recovery_attempts`, `retry_max_retries`, `retry_initial_delay_ms`, `retry_backoff_factor`.

## [1.0.0] - 2026-03-31

### Fixed

- **Subscription event timestamps (issue #273)**: `asubscribe(..., tick_mode=True)` and raw subscription batches now expose the event `timestamp` column as UTC-aware Arrow/Python datetimes instead of naive UTC values. This fixes incorrect `.timestamp()` conversions on non-UTC hosts.

## [1.0.0rc4] - 2026-03-30

### Changed

- **PyPI classifiers**: Added `Development Status :: 5 - Production/Stable`, `Intended Audience :: Financial and Insurance Industry`, `Intended Audience :: Science/Research`, `Topic :: Office/Business :: Financial`, `Programming Language :: Rust`, and `Typing :: Typed`.
- **README**: Fixed all documentation links from defunct ReadTheDocs to the Starlight docs site, updated the latest-release marker to rc4, removed stale Codecov/Codacy/CodeFactor badges, replaced dead `Auto CI` build badge with `ci-rust.yml`, and removed "beta" language in project description.
- **Issue templates**: Updated documentation links and environment version examples for v1.
- **CONTRIBUTING.md**: Corrected minimum Rust version from 1.70 to 1.75.

### Added

- **Bloomberg SDK ABI compatibility check**: New `scripts/abi-check.sh` and CI job that verifies every C symbol xbbg-core depends on exists across SDK versions (oldest supported through latest). Minimum supported SDK version defined in `defs/bloomberg.toml` (`min_sdk_version`).
- **SECURITY.md**: Restored security policy with vulnerability reporting instructions and hardening notes.
- **GitHub Pages deploy workflow**: Added `deploy-docs.yml` for automated Starlight docs deployment on push to main.
- **Documentation**: Wrote complete content for all guide and reference pages (migration, streaming, async, backends, output formats, configuration, type mappings).

### Fixed

- **Shutdown panic (issue #270)**: Fixed tokio worker thread panic (`Python::attach` after `Py_Finalize`) when Python exits with active subscriptions. Root cause: `signal_shutdown()` didn't close the data path to `__anext__`, leaving tokio futures alive during interpreter teardown. Fix adds `Engine::shutdown_signal` (watch channel) that immediately wakes pending `__anext__` futures, and `shutdown_safe_future` wrapper that prevents `future_into_py` from delivering results to a dead interpreter. Affects all async methods (requests, subscriptions, recipes).
- **Engine startup race condition (issue #272)**: `configure()` no longer raises `RuntimeError` if the engine was auto-created with defaults before configuration (e.g., by a health check or background thread in FastAPI). It now shuts down the default engine with a `RuntimeWarning` and stores the new config for the next request. Also added thread safety to `_get_engine()` (double-checked locking) and clear error messages when sync wrappers (`bdp`, `bdh`, etc.) are called inside async contexts.
- **Type checking**: Resolved all 178 `ty` errors to zero. Exception classes properly subclassed in Python instead of monkey-patching `__init__` on Rust classes. Added exception stubs to `_core/__init__.pyi`. Remaining 6 `type: ignore` comments are all upstream stub gaps (narwhals, stdlib, platform-specific).
- **Unused `pandas` import**: Removed leftover `TYPE_CHECKING` import of `pandas` in `blp.py` after `Format.WIDE` removal.
- **Exception hierarchy**: `BlpRequestError`, `BlpSecurityError`, `BlpFieldError`, and `BlpValidationError` are now proper Python subclasses of the Rust base classes with typed `__init__` signatures, replacing fragile `__init__` monkey-patching.

### Security

- **Pygments ReDoS (CVE)**: Upgraded Pygments 2.19.2 to 2.20.0, fixing a regular expression denial of service in GUID matching.

### Removed

- **`OverflowPolicy::DropOldest`**: Removed unimplemented overflow policy that silently behaved as `DropNewest`. Will be reintroduced in a future release with correct ring-buffer semantics. Use `'drop_newest'` (default) or `'block'`.
- **`Format.WIDE`**: Removed the deprecated wide output format. Use `Format.SEMI_LONG` for field-as-column output, or call `.pivot()` on `Format.LONG` results.
- **`asset_config()`**: Removed the deprecated market config helper. Use `market_info(ticker)` instead.

## [1.0.0rc3] - 2026-03-26

### Fixed

- **BQL error handling**: Parse Bloomberg's `responseExceptions` for actionable error messages (e.g. "Undefined item: CUR_YLD") instead of opaque "missing 'results' field" errors. Null results with no exceptions now return an empty DataFrame. Partial exceptions with valid results log warnings instead of failing.
- **`corporate_bonds()` cross-market support**: Switched from `bondsuniv` + `TICKER==` filter (US-only) to `debt()` universe, matching the approach used by `preferreds()`. Now accepts full equity tickers (e.g. "9984 JT Equity") and works across all markets.
- **CDX on-the-run indicator**: Accept `'true'` (returned by Bloomberg for CDX generic tickers) in addition to `'Y'` for `ON_THE_RUN_CURRENT_BD_INDICATOR`, fixing false warnings on CDX instruments.
- **`is_connected()` checks real session health**: Now queries actual Bloomberg worker health via `request_pool_health()` instead of just checking if the Python engine object exists.
- **`fieldExceptions` logging**: Downgraded from WARN to DEBUG and now includes actual field names and error messages (e.g. "MATURITY: Field not applicable to security") instead of just a count.

## [1.0.0rc2] - 2026-03-23

### Added

- **Subscription field exposure** (#265): `all_fields` on `asubscribe`, `astream`, and `stream` (and `PyEngine.subscribe` / `subscribe_with_options`). When `False` (default), batches include only requested fields plus `MKTDATA_EVENT_TYPE` and `MKTDATA_EVENT_SUBTYPE`. When `True`, each batch includes every top-level scalar field Bloomberg sends (e.g. full `SUMMARY`/`INITPAINT` snapshots), with the schema growing as new fields appear. The same flag is available on `avwap`, `amktbar`, `adepth`, and `achains` for consistency across streaming services.

## [1.0.0rc1] - 2026-03-23

### Added

- **Intraday timezone controls (`request_tz` / `output_tz`)**: `abdib`/`bdib`, `abdtick`/`bdtick`, `arequest`, and Rust `RequestParams` accept optional `request_tz` (interpret naive `start_datetime`/`end_datetime` before Bloomberg) and `output_tz` (relabel Arrow `time` to an IANA zone). Supported labels include `UTC`, `local`, `exchange`, `NY`/`LN`/`TK`/`HK`, reference tickers, and explicit IANA names. Implemented in `xbbg-async` (`chrono-tz`, `iana-time-zone`) with nested RefData calls routed through `request_without_intraday_transform` to avoid recursion.
- **Pixi environment management**: Added `pixi.toml` with 11 environments (default, test, lint, benchmark, docs, py310–py314), 21 tasks, and conda-forge deps for Rust, libclang, and pyarrow. Single `pixi install && pixi run install` replaces manual toolchain setup.
- **mimalloc allocator**: PyO3 extension now uses mimalloc by default (feature-gated) for improved Rust-side allocation performance.
- **`ty` type checking**: Lint environment includes Astral's `ty` type checker alongside ruff; CI lint job now runs type checking automatically.
- **SOCKS5 proxy support** (#180): Route Bloomberg connections through a SOCKS5 proxy via `socks5_host` and `socks5_port` kwargs on `configure()` and `Engine()`. Uses the Bloomberg SDK's `Socks5Config` API (no auth, hostname + port only).
- **Enterprise-friendly request middleware context**: `RequestContext` now carries a read-only `RequestEnvironment` snapshot so middleware can inspect engine source, host/port, server list, auth method, app/user context, and validation mode without reaching into private globals.

### Changed

- **Standardised on `BLPAPI_ROOT`**: Removed `XBBG_DEV_SDK_ROOT` env var across the codebase (build.rs, scripts, docs). SDK discovery now uses `BLPAPI_ROOT` only (set by pixi activation or `.cargo/config.toml`). No hardcoded SDK version — build.rs scans versioned subdirs automatically.
- **Removed `BLPAPI_LINK_LIB_NAME`**: Library name is now always auto-detected by `detect_link_lib_name()` based on target platform.
- **Build profiles cleaned up**: Removed redundant `[profile.release.package.xbbg_core]`; added `[profile.dev.package."*"] opt-level = 2` so all deps are optimised in dev builds; `pixi run install` uses `target-cpu=native` for local builds.
- **Migrated from uv to pixi for dev tooling**: Removed `[dependency-groups]`, `[tool.uv.*]` from pyproject.toml; deleted `uv.lock`; pre-commit hooks use bare `ruff` instead of `uvx ruff`; README dev instructions updated to pixi commands.
- **Consolidated config files**: Merged `.coveragerc` into `pyproject.toml` `[tool.coverage.*]`; deleted `.env` (pixi activation replaces it); un-gitignored `.cargo/config.toml` (now contains only project-standard `BLPAPI_ROOT`).
- **CI lint job uses pixi**: `lint-python` job now uses `prefix-dev/setup-pixi` with the lightweight `lint` environment, replacing `uvx ruff`.
- **Request tracing is more consistent**: Python request middleware now sees the generated `request_id` in both `RequestContext.request_id` and `RequestContext.params_dict`, centralized request logs include the request ID, and the Rust request path forwards it as the Bloomberg request label for better audit/debug correlation.
- **Bindgen/libclang toolchain aligned**: All Rust FFI crates now use `bindgen 0.72.1` with runtime loading, and the pixi environment now requires `libclang >=22`. This fixes incorrect Bloomberg SDK `blpapi_ManagedPtr_t_` generation under newer libclang releases and removes the need for correlation-ID layout workarounds.

### Removed

- **`XBBG_DEV_SDK_ROOT` env var**: Use `BLPAPI_ROOT` instead. The `.env` file fallback in `blpapi-sys/build.rs` has been removed.
- **`BLPAPI_LINK_LIB_NAME` env var**: Auto-detection covers all platforms.
- **`uv.lock`**: Replaced by `pixi.lock`.
- **`.coveragerc`**: Configuration moved to `pyproject.toml`.

### Fixed

- **De-duplicated Rust recipe helpers**: Extracted `array_value_as_string`, `date32_to_naive`, `as_string_col` into shared `xbbg-recipes/src/utils.rs`.
- **De-duplicated Python code**: Consolidated `_to_pandas_wide` (was in both `info.py` and `bloomberg.py`); unified `_FUTURES_MONTH_CODES` to use Rust-sourced `ext_get_futures_months()`; extracted `_apply_settle_override` helper replacing 5 repeated blocks in `bonds.py`.

## [1.0.0b7] - 2026-03-18

### Added

- **Python type stubs** for `xbbg._core` via `pyo3-stub-gen`: auto-generated `.pyi` files provide full IDE autocompletion and type-checker support for `EngineConfig`, `Engine`, `Subscription`, and all Rust-backed functions. Includes `py.typed` PEP 561 marker.
- **macOS ARM64 wheel builds** in CI and release workflows. Wheels are now built and tested for Linux x86_64, Windows x86_64, and macOS ARM64 across Python 3.10–3.14.
- **CI auto-regeneration of type stubs**: stubs are regenerated and auto-committed after all CI checks pass, ensuring `.pyi` files stay in sync with Rust annotations.
- **`Engine` class** for non-global multi-engine routing. Create independent engine instances and scope them via `with engine:` (sync) or `async with engine:` (async). The global `configure()` + `blp.bdp()` API is unchanged — `Engine` is fully opt-in.
- **TLS support** for encrypted B-PIPE connections: `tls_client_credentials`, `tls_trust_material`, `tls_handshake_timeout_ms` on `EngineConfig` and `configure()`.
- **Identity lifecycle FFI**: `Session.generate_token()`, `Session.send_authorization_request()`, `Session.subscribe_with_identity()` for multi-user entitlement flows.
- **Runtime SDK version**: `get_sdk_info()` now includes `runtime_version` field reporting the linked Bloomberg C SDK version via `blpapi_getVersionInfo()` (e.g., `"3.26.2.1"`). Also available as `xbbg._core.sdk_version()` → `(major, minor, patch, build)` tuple.
- **Async request cancellation**: cancelling the Python task for any async Bloomberg request now propagates to the Bloomberg SDK via `Session::cancel(correlationId)`. The worker drops local request state immediately after a successful cancel and remains usable for subsequent requests.
- **Reconnect resilience (Phases 1–3)** for the Rust engine (#245):
  - **Fail-fast on session death**: request workers now immediately drain all in-flight requests with an error on `SessionTerminated`/`SessionConnectionDown` instead of letting callers hang indefinitely. Workers are marked `Dead` and restored to `Healthy` on `SessionConnectionUp`.
  - **Service re-open before re-subscribe**: `recover_active_subscriptions()` now re-opens all previously opened services before re-issuing subscriptions after reconnect, fixing a critical gap where recovery could silently fail.
  - **Health-aware dispatch**: request pool round-robin skips `Dead` workers; returns `AllWorkersDown` immediately if the entire pool is dead.
  - **Retry with exponential backoff**: `RetryPolicy` on `EngineConfig` (`retry_max_retries`, `retry_initial_delay_ms`, `retry_backoff_factor`, `retry_max_delay_ms`) enables automatic retry of transient request failures.
  - **Recovery limits**: `max_recovery_attempts` and `recovery_timeout_ms` cap subscription recovery to prevent infinite loops.
  - **Lifecycle events**: `ConnectionLost`, `Reconnected`, and `RecoveryFailed` events emitted to subscription status for observability.
  - **New error variants**: `BlpAsyncError::SessionLost` and `AllWorkersDown` mapped to Python `BlpSessionError`.
  - **Python surface**: all new config fields exposed in `EngineConfig`, `configure()`, and `Engine()`; `engine.worker_health()` returns per-worker health status.
- **Multi-server failover** via `servers` kwarg (#250). Pass a list of `(host, port)` tuples for automatic Bloomberg SDK failover using `setServerAddress(host, port, index)`. Existing `host`/`port` kwargs unchanged for single-server use.
- **ZFP over leased lines** via `zfp_remote` kwarg (#255). Set to `"8194"` or `"8196"` with TLS credentials to connect via Bloomberg Zero Footprint without a local Terminal. Uses `ZfpUtil::getOptionsForLeasedLines` from the SDK.
- **Identity entitlement checking** (#252): `Identity.is_authorized(service)`, `Identity.has_entitlements(service, eids)`, and `Identity.seat_type()` for B-PIPE multi-user entitlement verification.
- **Bloomberg SDK logging bridge** (#253): `enable_sdk_logging(level)` and `EngineConfig.sdk_log_level` route native BLPAPI internal logs into `xbbg-log` tracing target `xbbg.sdk`. Default is **off**; registration happens before session start when enabled.

### Changed

- **Engine Architecture & EngineConfig documentation**: README now includes a full reference for all 20+ `EngineConfig` fields (worker pools, subscription tuning, buffers, validation, auth), an ASCII architecture diagram, and auth mode examples.
- **API surface updated to v1**: README function tables, examples, and Connection Options section now reflect v1 names (`blkp`, `bport`, `earnings`, `convert_ccy`, `configure()`, `subscribe`/`stream`, etc.) and remove stale v0.x references (`lookupSecurity`, `exchange_tz`, `set_format`, `Format` enum).
- **Dev setup and contributing guides** updated for v1 project structure (`py-xbbg/src` paths, Astro docs, `uv sync` dependency-groups).

### Fixed

- **cargo-deny advisory ignores** for unmaintained `unic-*` crates (transitive deps of `rustpython-parser` via `pyo3-stub-gen`, build-time only).

## [1.0.0b6] - 2026-03-16

### Changed

- **Internal correlation ID dispatch overhaul**: The async engine no longer uses raw Bloomberg integer correlation IDs as direct slab indexes. All request and session dispatch now routes through an explicit dispatch-key layer at the session boundary, preventing ID collisions between auth subscriptions and user requests and aligning lifecycle tracking with Bloomberg SDK semantics.
- **Logging levels better match the quiet-by-default workflow**: Request roundtrip telemetry and Python subscription lifecycle messages now emit at `DEBUG` instead of `INFO`, while exchange metadata fetch failures that cleanly fall back now emit at `WARNING` instead of `ERROR`, keeping normal control-flow noise out of default logs without hiding real request telemetry.

### Fixed

- **SAPI authentication fails with `BLPAPI_ERROR_DUPLICATE_CORRELATIONID`** ([#248](https://github.com/xbbg-org/xbbg/issues/248)): `CorrelationId::default()` returned `Int(0)`, which is a valid explicit correlation ID. When `setSessionIdentityOptions` registered `Int(0)` for the auth flow, subsequent `sendRequest` calls with the same default ID were rejected as duplicates (rc=131077). The default is now `CorrelationId::Unset` (maps to `BLPAPI_CORRELATION_TYPE_UNSET` in the FFI struct), matching the official Python `blpapi` behavior where the SDK auto-generates unique IDs. Affects all SAPI authentication modes (`app`, `user`, `userapp`, `dir`, `token`).

## [1.0.0b5] - 2026-03-12

### Added

- **Rust-backed Bloomberg session authentication for v1**: Added structured auth support across the Rust core, async engine, and PyO3 bindings for `user`, `app`, `userapp`, `dir`, `manual`, and `token` auth modes, enabling SAPI/B-PIPE session configuration from the v1 Python API.
- **Request middleware chain for telemetry and wrappers**: Added `RequestContext` plus middleware registration helpers around `arequest()` so callers can layer centralized request instrumentation, logging, caching, and wrapper behavior without patching individual endpoint functions.

### Changed

- **`configure()` is now the canonical engine/session setup surface**: Connection/auth setup now flows through `configure()` with support for legacy aliases such as `server_host`, `server_port`, `max_attempt`, and `auto_restart`, while the temporary `connect()` / `disconnect()` wrappers were removed before release.

### Fixed

- **Auth/session startup failures now propagate with context**: Request and subscription workers now wait for Bloomberg startup/auth events before proceeding, so failed authentication and session-start problems surface as actionable errors instead of being swallowed or masked by later service-open failures.
- **Rust/Python CI regressions in the new auth path**: Cleaned release-blocking lint and formatting issues in the new auth/middleware code paths so the full Linux/Windows CI matrix passes with the beta 5 changes.

## [1.0.0b4] - 2026-03-10

### Changed

- **Subscription failure isolation for mixed-topic streams**: Real-time subscriptions now treat Bloomberg `SubscriptionFailure` and unexpected `SubscriptionTerminated` events as per-ticker status instead of fatal stream errors when other topics remain healthy. Mixed subscriptions keep delivering data for valid tickers while exposing failed topics through subscription metadata.
- **Subscription lifecycle observability**: Real-time subscriptions now retain bounded status/event history for topic lifecycle transitions, session connectivity, service readiness, slow-consumer/data-loss signals, and reconnect recovery attempts so callers can inspect operational state without scraping logs.
- **Non-fatal disconnect handling with opt-in recovery**: `SessionConnectionDown` no longer tears down healthy subscriptions by default. Callers can opt into `recovery_policy="resubscribe"` to issue reconnect-time recovery subscribes while tracking attempts, successes, and last recovery errors through subscription status metadata.

### Added

- **Subscription failure metadata**: Python subscriptions now expose `failed_tickers` and `failures` so callers can inspect which topics Bloomberg rejected or terminated, along with the reported reason and failure kind.
- **Subscription health/status surfaces**: Python subscriptions now expose `status`, `events`, `topic_states`, `session_status`, `admin_status`, `service_status`, `all_failed`, and expanded `stats` fields including data-loss counters, last-message timestamps, and effective overflow policy.

## [1.0.0b3] - 2026-03-06

### Added

- **Backend enum and availability checks** ([#234](https://github.com/xbbg-org/xbbg/issues/234)): Ported `Backend` enum and backend availability infrastructure from `release/0.x` into `py-xbbg/src/xbbg/backend.py`. The canonical `Backend` enum now has all 13 backends (added `CUDF`, `MODIN`, `DASK`, `IBIS`, `PYSPARK`, `SQLFRAME`). New public helpers: `is_backend_available()`, `check_backend()`, `get_available_backends()`, `print_backend_status()`, `validate_backend_format()`, `is_format_supported()`, `get_supported_formats()`, `check_format_compatibility()`. Includes `MIN_VERSIONS`, `PACKAGE_NAMES`, `MODULE_NAMES`, and `SUPPORTED_FORMATS` dicts for version validation and actionable install instructions.

### Changed

- **Subscription mutation synchronization**: Refactored subscription worker ownership to split the single-owner pool lease from a cloneable command handle, allowing subscription `add()`/`remove()` paths in both `xbbg-async` and PyO3 to drop metadata locks before awaiting Bloomberg command dispatch while still serializing mutations safely.

### Fixed

- **Additional GIL release coverage in PyO3 bindings**: Released the GIL around synchronous cache-save calls, Arrow pivot/format inspection helpers, and subscription metadata snapshots so Python threads are not blocked during disk I/O, pure Rust Arrow work, or waits on subscription state locks.
- **Reduced avoidable Arrow-path copies**: Removed intermediate string allocations for borrowed Bloomberg string/enum values and stopped cloning field-name/subfield-name vectors in `refdata`, `histdata`, and `bulkdata` extraction paths before the existing zero-copy PyArrow export boundary.
- **Removed unused `lief` dependency**: Dropped `lief>=0.17` from core `[project.dependencies]`; the package was never imported anywhere in the codebase.

## [1.0.0b2] - 2026-03-05

### Added

- **Field-validation toggle for refdata/histdata requests**: Added optional `validate_fields` request parameter in `request()`/`arequest()` and typed wrappers (`abdp`/`bdp`, `abdh`/`bdh`, `abds`/`bds`). This supports per-request strict validation override while still honoring engine-level `validation_mode` defaults.
- **Engine-side field-validation enforcement**: `xbbg-async` now validates requested fields for `ReferenceDataRequest` and `HistoricalDataRequest` before dispatch when validation is enabled, returning configuration errors for unknown Bloomberg fields in strict mode.
- **Live validation toggle smoke script**: Added `py-xbbg/tests/live/field_validation_toggle_smoke.py` to verify on/off behavior against a connected Bloomberg session.
- **Request-plumbing coverage for `validate_fields`**: Added `py-xbbg/tests/test_validate_fields_toggle.py` to verify Python parameter serialization and forwarding through async/sync wrappers.

### Changed

- **Canonical exception exports**: `xbbg.exceptions` now re-exports Rust `_core` exception classes (`BlpError`, `BlpRequestError`, etc.) as the single source of truth, with Python-only exceptions remaining additive.
- **Validation helper compatibility**: Preserved `BlpValidationError.from_rust_error(...)` by attaching the compatibility classmethod to the canonical Rust-backed validation exception.
- **Generated sync wrapper metadata**: `blp.py` generated sync wrappers now derive `__doc__` and `__annotations__` from async templates directly; remaining manual generated sync wrapper boilerplate was removed.
- **Integration logging expectations**: Updated logging integration assertions to match centralized `arequest` request logging (`bloomberg ... ReferenceDataRequest`) instead of deprecated endpoint-specific debug strings.
- **Optional pandas integration paths**: Updated pandas-dependent integration tests to use `pytest.importorskip("pandas")`, avoiding hard failures when pandas is not installed.

### Fixed

- **`except BlpError` catchability gap**: Runtime exceptions raised by Rust (for example `BlpRequestError`) are now catchable via `xbbg.exceptions.BlpError` import paths because both now point to the same canonical Rust exception hierarchy.

## [1.0.0b1] - 2026-03-03

### Added

- **Endpoint-factory regression tests**: Added focused coverage for generated `abflds`/`bflds` and `abqr`/`bqr` routing, validation, and reshape behavior in `py-xbbg/tests/test_endpoint_factory_bflds.py` and `py-xbbg/tests/test_endpoint_factory_bqr.py`

### Changed

- **Template endpoint generation in `blp.py`**: Migrated clean-fit wrappers to generated async/sync endpoints backed by `_GeneratedEndpointSpec` and `_EndpointPlan`, including `abdp`/`bdp`, `abdh`/`bdh`, `abds`/`bds`, `abdib`/`bdib`, `abdtick`/`bdtick`, `abql`/`bql`, `abqr`/`bqr`, `absrch`/`bsrch`, `abeqs`/`beqs`, `ablkp`/`blkp`, `abport`/`bport`, `abcurves`/`bcurves`, `abgovts`/`bgovts`, and `abflds`/`bflds`

### Fixed

- **`bqr` pandas dependency regression**: Removed unconditional `to_pandas()` conversion in BQR postprocessing; quote requests now use Arrow-native checks/reshape and run without requiring pandas for standard flows

## [1.0.0a3] - 2026-02-27

### Added

- **`bqr()`/`abqr()` Bloomberg Quote Request**: Tick-level dealer quotes with `date_offset` (`-2d`, `-1w`, `-3h`), `start_date`/`end_date` date ranges, and optional `include_broker_codes`, `include_spread_price`, `include_yield`, `include_condition_codes`, `include_exchange_codes` parameters. Generic extractor fallback reshaped via `_reshape_bqr_generic()`
- **`bflds()`/`abflds()` unified field metadata lookup**: Single function for both field info (`fields=[...]`) and keyword search (`search_spec='...'`). `bfld`/`abfld` provided as backward-compatible aliases. Convenience wrappers `fieldInfo()`/`fieldSearch()` preserved
- **`include_security_errors` option for `bdp()`/`arequest()`**: Optionally surface per-security failures as rows in the result DataFrame instead of silently dropping them
- **Extension modules (`xbbg.ext`)**: `bonds` (6 functions), `options` (6 functions + 5 enums), `cdx` (8 functions) for fixed income, equity options, and credit default swap index analytics
- **Streaming performance enhancements**: Per-subscription config (`flush_threshold`, `overflow_policy`, `stream_capacity`), observability metrics via shared atomics, `tick_mode` support
- **Live integration tests**: 69 tests across `test_ext_bonds.py` (21), `test_ext_options.py` (20), `test_ext_cdx.py` (22) covering all ext module functions
- **Streaming tests**: Tests for `tick_mode`, per-subscription config, and observability metrics
- **Rust exchange/session APIs**: Added low-level exchange resolution support with `ExchangeInfo` metadata, runtime exchange overrides, session timezone conversion utilities, and `market_timing` helpers in the Rust layer (`xbbg-ext`, `xbbg-async`, `pyo3-xbbg`)
- **Live exchange smoke test**: Added `py-xbbg/tests/live/test_exchange_resolution.py` covering override precedence, UTC session conversion, live `resolve_exchange`, `fetch_market_info`, and `market_timing`

### Changed

- **README**: Updated API reference tables with `bflds()`, expanded BQR section with spread/yield/broker parameters and examples
- **Futures resolver**: Aligned with `release/0.x` chain methodology (`FUT_CHAIN_LAST_TRADE_DATES`)
- **CDX resolver**: Aligned methodology with `release/0.x`

### Removed

- **Legacy `xbbg/` Python package directory**: Fully removed; all code now lives in `py-xbbg/src/xbbg/`

### Fixed

- **Empty `RecordBatch` construction**: Handle empty ordered RecordBatch in `xbbg-async` without panic
- **Security failure surfacing**: `refdata` extractor now properly surfaces per-security errors instead of silently dropping them
- **`FIELD_SEARCH` extractor**: Corrected to use `ExtractorHint.FIELD_INFO` instead of generic extractor
- **Unused `logging` import in `ext/options.py`**: Removed to pass ruff lint
- **Test imports**: `BlpInternalError` imported from `_core` (Rust) instead of `exceptions` (Python)
- **CI fixes**: Resolved 4 Python test failures, clippy warnings (`too_many_arguments`, `SubscriptionMetrics` re-export), ruff check/format violations, cargo fmt formatting, module path for `test_markets.py`, Linux test runtime setup
- **Exchange refdata parsing shape support**: `resolve_exchange` now handles both WIDE and LONG refdata responses by mapping `(field, value)` rows when Bloomberg returns long-shape metadata

## [1.0.0a2] - 2026-02-19

### Changed

- **README**: Comprehensive rewrite with full API reference tables, comparison matrix, multi-backend documentation, detailed intraday session guide, fixed income/options/CDX analytics examples, troubleshooting section, and data storage documentation

## [1.0.0a1] - 2026-02-19

### Added

- **Rust-powered engine**: Complete ground-up rewrite delivering up to **10x faster** data retrieval with zero-copy Arrow transfer between Rust and Python. The engine spans 11 purpose-built crates: safe FFI bindings with SIMD-accelerated parsing (`blpapi-sys`, `xbbg-core`), an async worker pool engine with state machines for every Bloomberg request type (`xbbg-async`), Rust ports of extension and recipe logic (`xbbg-ext`, `xbbg-recipes` -- all 12 recipes exposed via PyO3), zero-GIL tracing (`xbbg-log`), CI/test stubs (`xbbg-sys`), and PyO3 Python bindings (`pyo3-xbbg`)
- **New Python package (`py-xbbg/`)**: Complete v1 API powered by the Rust backend, replacing the pure-Python `xbbg/` package. Lazy-loaded via `__getattr__` for near-instant import
- **Streaming APIs**: `vwap()`/`avwap()` for VWAP streaming (`//blp/mktvwap`), `mktbar()`/`amktbar()` for market bar streaming, `depth()`/`adepth()` for market depth (B-PIPE), `chains()`/`achains()` for option/futures chain streaming (B-PIPE) -- all with async variants
- **New Bloomberg API functions**: `bcurves()`/`abcurves()` for yield curve lookup, `bgovts()`/`abgovts()` for government securities lookup, `bflds()`/`abflds()` for field search (replacing `fieldInfo`/`fieldSearch`)
- **Generic request API**: `request()`/`arequest()` -- direct access to any Bloomberg service and operation with schema-driven kwargs routing. Power users can hit any Bloomberg endpoint without a dedicated wrapper
- **Schema introspection**: `bops()`/`abops()` to list service operations, `bschema()`/`abschema()` for full service schema, `get_schema()`/`aget_schema()`, `list_operations()`/`alist_operations()`, `get_enum_values()`/`aget_enum_values()`, `list_valid_elements()`/`alist_valid_elements()` -- all with async variants. `generate_stubs()` and `configure_ide_stubs()` for IDE auto-completion generated from live Bloomberg schemas
- **Field type cache**: `FieldTypeCache`, `FieldInfo`, `resolve_field_types()`/`aresolve_field_types()`, `cache_field_types()`, `get_field_info()`, `clear_field_cache()` for caching and resolving Bloomberg field metadata
- **Engine lifecycle management**: `shutdown()`, `reset()`, `is_connected()` for explicit Rust engine control
- **`EngineConfig`**: Rust-native engine configuration (PyO3 `PyEngineConfig`) -- subscription pool size, request pool size, flush thresholds, auto-restart on disconnection
- **Auto-restart on disconnection**: Subscription sessions automatically reconnect after network interruptions via `setAutoRestartOnDisconnection`
- **`Time64Micros` value type**: Microsecond-precision time-of-day extraction from Bloomberg `Datetime` fields, with Arrow `Time64Micros` type support in generic, histdata, and refdata state handlers
- **`BlpBPipeError` exception**: New exception class for B-PIPE-specific errors, added to the existing exception hierarchy
- **Technical Analysis improvements**: `ta_study_params()` to inspect study parameters, `generate_ta_stubs()` for IDE auto-completion of TA study names
- **Logging control**: `set_log_level()`, `get_log_level()` to control Rust-side tracing verbosity without Python overhead
- **Bloomberg SDK message receive time**: `Message::receive_time()` for latency measurement and diagnostics
- **Service definitions**: `Service`, `Operation`, `OutputMode`, `RequestParams`, `ExtractorHint` enums for type-safe Bloomberg service configuration
- **Extension modules (`py-xbbg/src/xbbg/ext/`)**: `currency`, `fixed_income`, `futures`, `historical` -- ported to work with the Rust backend in LONG format
- **Markets module (`py-xbbg/src/xbbg/markets/`)**: `bloomberg`, `info`, `overrides`, `resolvers`, `sessions` -- exchange metadata, market timing, and override normalization
- **Data definition files**: `defs/bloomberg.toml` and `defs/exchanges.toml` for data-driven Bloomberg and exchange configuration
- **Starlight documentation site**: Full rewrite from Sphinx to Astro Starlight -- API reference (`blp.md`, `exceptions.md`, `schema.md`, `services.md`), getting started guides (`installation.mdx`, `introduction.mdx`, `quickstart.mdx`), async/streaming/migration/output-format guides, and configuration reference
- **Benchmark suites**: Rust benchmarks via Criterion (`xbbg-bench` -- allocation profiling, datetime/name micro-benchmarks, cached response parsing, live `bdp`/subscription benchmarks) and Python benchmarks (`benchmarks/` -- `bdp`, `bdh`, `bdib`, `bdtick`, `bql`, raw `blpapi` with version-based result tracking)
- **Comprehensive test suites**: `py-xbbg/tests/` with unit tests for imports, backends, blp API, backend conversion, currency conversion, exceptions, futures validation, integration, markets, and yield types. Live test suite (`py-xbbg/tests/live/`) for API, engine, subscription lifecycle, and subscription fixes
- **CI infrastructure**: `ci-rust.yml` for multi-platform Rust CI (clippy, rustfmt, unit tests, integration tests, `cargo-audit`, `cargo-deny`, semver-checks), `ci-docker.yml` for reusable container image builds, Docker containers for Rust CI and manylinux wheel builds
- **Codegen tool** (`codegen/generate.py`): Python code generator for service definitions, including `SEMI_LONG` output format support from `release/0.x`
- **SDK setup script** (`scripts/sdktool.ps1`): PowerShell script for Bloomberg SDK vendor layout management
- **`cargo-deny` configuration** (`deny.toml`): License and security policy for all Rust dependencies
- **Future language binding scaffolds**: `bindings/napi-xbbg/` (Node.js N-API), `bindings/dotnet-xbbg/` (.NET), `apps/xbbg-cli/` (CLI), `apps/xbbg-server/` (server), and `js-xbbg/` (npm package)
- **`vendor/blpapi-sdk/README.md`**: Instructions for vendoring the Bloomberg C++ SDK locally

### Changed

- **Build system**: Switched to `setuptools-rust` (PyO3) with `setuptools_scm` versioning. `pyproject.toml` now builds the Rust extension via `setuptools.build_meta`
- **Python package source location**: Moved from `xbbg/` (in-tree) to `py-xbbg/src/xbbg/` for the Rust-backed package layout. The native extension is compiled as `xbbg._core`
- **Runtime dependencies**: `pandas` is no longer required -- now only `narwhals>=1.30`, `pyarrow>=22.0.0`, `lief>=0.17`. Removed `blpapi`, `tomli`, and all other previous hard dependencies
- **Python version support**: Added Python 3.14 to classifiers (`>=3.10,<3.15`)
- **`pypi_upload.yml` workflow**: Completely rewritten for `setuptools-rust` wheel builds with Bloomberg SDK detection, replacing the pure-Python sdist/wheel workflow
- **`pre-commit-config.yaml`**: Updated hooks for the Rust+Python monorepo -- added `cargo fmt`, `cargo clippy`, and scoped ruff to `py-xbbg/` and `xbbg/`
- **`.gitignore`**: Expanded for Rust build artifacts (`target/`), native extension outputs, SDK vendor directory, benchmark results, and IDE files
- **README.md**: Rewritten for v1.0 -- concise project description, Rust-powered backend highlights, installation and quick start replacing the extensive v0.x documentation
- **CONTRIBUTING.md**: Rewritten for the Rust+Python development workflow
- **LICENSE**: Updated to Apache-2.0 with revised copyright
- **Maximum-performance release builds**: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"` for production; `opt-level = 3` for `xbbg-core`, `opt-level = 2` for dev
- **Subscription pool default**: Reduced from 4 to 1 worker, consolidated into `EngineConfig`
- **Legacy `xbbg/` package**: Minor cleanups -- added `from __future__ import annotations` across all `__init__.py` files, normalized docstring quotes from single to double, removed `# noqa` lint suppression comments, replaced `lambda: []` with `list` in pipeline factory registry default resolvers

### Removed

- **`xbbg/__init__.py`**: Top-level package init replaced by `py-xbbg/src/xbbg/__init__.py` with Rust backend
- **`xbbg/blp.py`**: 178-line deprecation compatibility layer -- all functions now live in `py-xbbg/src/xbbg/blp.py` backed by Rust
- **`xbbg/const.py`**: 187-line constants module -- constants moved to Rust crates and `defs/*.toml`
- **`xbbg/core/__init__.py`**: 35-line core package init -- core functionality replaced by Rust engine
- **`xbbg/core/process.py`**: 787-line Bloomberg message processing module -- replaced by `xbbg-async` Rust engine state machines
- **`xbbg/utils/pipeline.py`**: 336-line pipeline utilities -- replaced by Rust engine pipeline
- **`xbbg/io/__init__.py`**: IO module init removed (module gutted in v0.12.0)
- **`xbbg/markets/__init__.py`**: 76-line markets package init -- replaced by `py-xbbg/src/xbbg/markets/`
- **`xbbg/markets/resolvers.py`**: Futures/CDX resolvers moved to `xbbg-ext` Rust crate
- **`examples/feeds/pub.py`** and **`examples/feeds/sub.py`**: Legacy feed examples
- **Sphinx documentation**: `docs/conf.py`, `docs/index.rst` (1,293 lines), `docs/Makefile`, `docs/make.bat`, `docs/docstring_style.rst` -- replaced by Starlight
- **`.readthedocs.yaml`**: ReadTheDocs configuration (docs now use Starlight)
- **`MANIFEST.in`**: No longer needed with the `setuptools-rust` build system
- **`SECURITY.md`**: Security policy document
- **`_config.yml`**: Jekyll configuration
- **`codecov.yml`**: Codecov configuration
- **9 CI workflows**: `auto_ci.yml`, `ci_docs.yml`, `codeql-analysis.yml`, `publish_docs.yml`, `publish_testpypi.yml`, `pypi_build_test.yml`, `release_assets.yml`, `update_index_on_release.yml`, `update_readme_on_release.yml` -- consolidated into `ci-rust.yml`, `ci-docker.yml`, and rewritten `pypi_upload.yml`

### Fixed

- **Clippy 1.93 lints**: Resolved `map_or` and doc indentation warnings across all Rust crates
- **Windows LLVM/`LIBCLANG_PATH` setup**: Fixed detection and configuration for `bindgen` on Windows CI
- **Linux `LIBCLANG_PATH` detection**: Fixed `libclang-dev` path resolution on Linux CI
- **Non-ASCII characters in comments**: Replaced em dashes with ASCII equivalents to pass CI source checks
- **Subscription slab key reuse race**: Prevented key reuse on subscription removal in `xbbg-async` that could cause events to route to wrong handlers
- **Subscription error propagation**: Subscription errors now propagate as Python exceptions via PyO3 instead of being silently swallowed
- **Subscription pipeline rewrite**: Multi-type support, error propagation, and event time tracking in `xbbg-async`
- **`Datetime` field zeroed date parts**: Added parts bitmask check to correctly handle Bloomberg `Datetime` fields with zeroed date components
- **DLL search path setup (Windows)**: Moved SDK DLL search path configuration to module level in `py-xbbg` to fix `from xbbg._core import X` failures
- **`Request::set_bool`**: Use `setElementString` for Bool elements in Bloomberg requests (Bloomberg API quirk)
- **TA study requests**: Wired through `elements` instead of unused `json_elements` path
- **`WIDE` format compatibility**: Produces 0.7.7-compatible DataFrame structure from the Rust backend
- **Backend double-conversion bug**: Fixed duplicate conversion when Rust backend returns data that is then converted again by Python

## [0.12.0] - 2026-02-18

### Added

- **Async-first architecture**: All Bloomberg API functions (`bdp`, `bds`, `bdh`, `bdib`, `bdtick`, `bql`, `beqs`, `bsrch`, `bqr`, `bta`) now have async counterparts (`abdp`, `abds`, `abdh`, etc.) as the source of truth; sync wrappers delegate via `_run_sync()` (#218)
- **Bond analytics module** (`xbbg.ext.bonds`): 6 new functions for fixed income analytics -- `bond_info` (reference metadata and ratings), `bond_risk` (duration, convexity, DV01), `bond_spreads` (OAS, Z-spread, I-spread, ASW), `bond_cashflows` (cash flow schedule), `bond_key_rates` (key rate durations and risks), `bond_curve` (multi-bond relative value comparison)
- **Options analytics module** (`xbbg.ext.options`): 6 new functions and 5 enums for equity option analytics -- `option_info` (contract metadata), `option_greeks` (Greeks and implied volatility), `option_pricing` (value decomposition and activity), `option_chain` (chain via `CHAIN_TICKERS` with overrides), `option_chain_bql` (chain via BQL with rich filtering), `option_screen` (multi-option comparison). Enums: `PutCall`, `ChainPeriodicity`, `StrikeRef`, `ExerciseType`, `ExpiryMatch`
- **CDX analytics** (`xbbg.ext.cdx`): 8 new functions for credit default swap index analytics -- `cdx_info`, `cdx_defaults`, `cdx_pricing`, `cdx_risk`, `cdx_basis`, `cdx_default_prob`, `cdx_cashflows`, `cdx_curve`. `cdx_pricing`/`cdx_risk` support `CDS_RR` recovery rate override
- **`YieldType` expanded**: Added `YTW` (Yield to Worst), `YTP` (Yield to Put), `CFY` (Cash Flow Yield) to `YieldType` enum
- **`workout_dt` parameter for `yas()`**: Workout date for yield-to-worst/call calculations, maps to `YAS_WORKOUT_DT` Bloomberg override. Accepts `str` (YYYYMMDD) or `datetime`
- **`tz` parameter for `bdib()`/`abdib()`**: Controls output timezone for intraday bar data. Defaults to `None` (exchange local timezone, matching v0.7.x behavior). Set `tz='UTC'` to keep UTC timestamps, or pass any IANA timezone string (e.g., `'Europe/London'`)
- **`exchange_tz()` helper**: Returns the IANA timezone string for any Bloomberg ticker (e.g., `blp.exchange_tz('AAPL US Equity')` -> `'America/New_York'`). Exported via `blp.exchange_tz()`
- **LONG_TYPED output format**: New `_to_long_typed()` function produces typed value columns (`value_f64`, `value_i64`, `value_str`, `value_bool`, `value_date`, `value_ts`) with exactly one populated per row based on the Arrow type of each field
- **LONG_WITH_METADATA output format**: New `_to_long_with_metadata()` function produces `(ticker, date, field, value, dtype)` where `value` is stringified and `dtype` contains the Arrow type name (e.g. `double`, `int64`, `string`)
- **CI non-ASCII source check**: New `auto_ci.yml` step rejects non-ASCII characters in Python source files (allows CJK for ticker tests)
- **Comprehensive test coverage**: 55+ new tests including bond analytics (7), CDX analytics (8), options analytics, timezone conversion (13), `ovrds` dict normalization (7), `_events_to_table()` (16), `bdtick` format variants (5), mixed-type BDP (2), and output format tests (12)

### Changed

- **Unified I/O layer**: All Bloomberg requests now flow through a single `arequest()` async entry point in `conn.py`, replacing scattered session/service management across modules (#218)
- **Futures resolution uses `FUT_CHAIN_LAST_TRADE_DATES`** (#223): Replaced manual candidate generation (`FUT_GEN_MONTH` + batch `bdp`) with Bloomberg-native `FUT_CHAIN_LAST_TRADE_DATES` via single `bds()` call. ~2x faster (0.25-0.30s vs 0.53-0.72s)
- **`sync_api` decorator**: Replaces 13 hand-written sync wrappers across API modules (`screening.py`, `historical.py`, `intraday.py`, etc.) with a single `sync_api(async_fn)` call
- **Table-driven deprecation wrappers**: 23 manual wrapper functions in `blp.py` replaced by dict + loop pattern; 24 `warn_*` functions in `deprecation.py` replaced by `_DEPRECATION_REGISTRY` + `get_warn_func()` lookup
- **Market session rules extracted to TOML** (`markets/config/sessions.toml`): All MIC and exchange code rules moved from `sessions.py` into data-driven TOML config, reducing `sessions.py` from 364 to 168 lines (54% reduction)
- **Pipeline factory registry** (`pipeline_factories.py`): Centralized factory dispatch replaces scattered conditionals
- **CDX ticker format corrected**: Version is now a separate space-delimited token (e.g., `CDX HY CDSI S45 V2 5Y Corp` instead of `S45V2`)
- **`tomli` conditional dependency added**: `tomli>=2.0.1` for Python < 3.11 (TOML parsing for `sessions.toml`)
- **Net reduction of ~1,346 lines** across 27 files from codegen and table-driven optimizations

### Removed

- **`xbbg/io/db.py`**: SQLite database helper module (zero imports across codebase) (#218)
- **`xbbg/io/param.py`**: Legacy parameter/configuration module (zero imports across codebase) (#218)
- **`xbbg/io/files.py`**: File path utility module (zero imports after replacing 6 usages in `cache.py` and `const.py` with `pathlib.Path`) (#218)
- **`regression_testing/`**: Standalone v0.7.7 regression test directory; all scenarios covered by `test_live_endpoints.py` (#218)
- **`MONTH_CODE_MAP` and futures candidate generation helpers**: Superseded by `FUT_CHAIN_LAST_TRADE_DATES` chain resolution (#223)
- Stale files: `pmc_cache.json`, `xone.db`, empty `__init__` files, `test_param.py` (#218)

### Fixed

- **`bdtick` format parameter was completely non-functional**: All five output formats (LONG, SEMI_LONG, WIDE, LONG_TYPED, LONG_WITH_METADATA) were broken due to MultiIndex column wrapping, killed index name, and mixed-type Arrow conversion errors
- **`bdib` timezone regression**: The Arrow pipeline rewrite (v0.11.0) dropped the UTC-to-exchange local timezone conversion that existed in v0.7.x. Restored with configurable `tz` parameter
- **`ArrowInvalid` on multi-field BDP calls**: Bloomberg returns different Python types for different fields. New `_events_to_table()` builds Arrow tables with automatic type coercion fallback (#219)
- **`create_request` crashed when `ovrds` passed as dict**: Now normalizes dict to list of tuples before iteration ([SO#79880156](https://stackoverflow.com/questions/79880156))
- **Case-sensitive `backend` and `format` parameters**: Added `_missing_` classmethod to `Backend` and `Format` enums for case-insensitive lookup (#221)
- **Mock session leak in tests**: Added autouse `_reset_session_manager` fixture to prevent `MagicMock` persistence across test modules (#213)
- **`interval` parameter leaked as Bloomberg override**: Added to `PRSV_COLS` so it stays local (#145)
- **`StrEnum` Python 3.10 compatibility**: Added polyfill for Python < 3.11
- **Non-ASCII characters in source**: Replaced with ASCII equivalents for CI compliance

### Security

- **Bump `cryptography` from 46.0.4 to 46.0.5**: Fixes CVE-2026-26007 (#217)

## [0.12.0b3] - 2026-02-16

### Added

- **Bond analytics module** (`xbbg.ext.bonds`): 6 new functions for fixed income analytics -- `bond_info` (reference metadata and ratings), `bond_risk` (duration, convexity, DV01), `bond_spreads` (OAS, Z-spread, I-spread, ASW), `bond_cashflows` (cash flow schedule), `bond_key_rates` (key rate durations and risks), `bond_curve` (multi-bond relative value comparison)
- **Options analytics module** (`xbbg.ext.options`): 6 new functions and 5 enums for equity option analytics -- `option_info` (contract metadata), `option_greeks` (Greeks and implied volatility), `option_pricing` (value decomposition and activity), `option_chain` (chain via `CHAIN_TICKERS` with overrides), `option_chain_bql` (chain via BQL with rich filtering), `option_screen` (multi-option comparison). Enums: `PutCall`, `ChainPeriodicity`, `StrikeRef`, `ExerciseType`, `ExpiryMatch`
- **CDX analytics** (`xbbg.ext.cdx`): 8 new functions for credit default swap index analytics -- `cdx_info`, `cdx_defaults`, `cdx_pricing`, `cdx_risk`, `cdx_basis`, `cdx_default_prob`, `cdx_cashflows`, `cdx_curve`. `cdx_pricing`/`cdx_risk` support `CDS_RR` recovery rate override
- **`YieldType` expanded**: Added `YTW` (Yield to Worst), `YTP` (Yield to Put), `CFY` (Cash Flow Yield) to `YieldType` enum
- **`workout_dt` parameter for `yas()`**: Workout date for yield-to-worst/call calculations, maps to `YAS_WORKOUT_DT` Bloomberg override. Accepts `str` (YYYYMMDD) or `datetime`
- **`tz` parameter for `bdib()`/`abdib()`**: Controls output timezone for intraday bar data. Defaults to `None` (exchange local timezone, matching v0.7.x behavior). Set `tz='UTC'` to keep UTC timestamps, or pass any IANA timezone string (e.g., `'Europe/London'`)
- **`exchange_tz()` helper**: Returns the IANA timezone string for any Bloomberg ticker (e.g., `blp.exchange_tz('AAPL US Equity')` -> `'America/New_York'`). Exported via `blp.exchange_tz()`
- **`tz` field on `DataRequest` and `RequestBuilder`**: Propagates timezone control through the pipeline. `RequestBuilder` gains `.tz()` builder method
- **CI non-ASCII source check**: New `auto_ci.yml` step rejects non-ASCII characters in Python source files (allows CJK for ticker tests)
- **Live endpoint tests**: 7 tests for bond analytics, 8 tests for CDX analytics, plus options analytics coverage in `test_live_endpoints.py`
- **13 unit tests for timezone conversion** (`test_intraday_timezone.py`): Covers default exchange tz, explicit UTC, explicit timezone, Japanese equities, empty exchange info, empty tables, column renaming, and DataRequest/RequestBuilder propagation
- **7 regression tests for `ovrds` dict normalization** (`test_overrides.py`): Covers dict crash, correct element setting, multiple overrides, list-of-tuples backward compat, and None/empty edge cases

### Changed

- **Futures resolution uses `FUT_CHAIN_LAST_TRADE_DATES`** (#223): Replaced manual candidate generation (`FUT_GEN_MONTH` + batch `bdp`) with Bloomberg-native `FUT_CHAIN_LAST_TRADE_DATES` via single `bds()` call. ~2x faster (0.25-0.30s vs 0.53-0.72s). Removed `MONTH_CODE_MAP`, `_get_cycle_months`, `_construct_contract_ticker`
- **`sync_api` decorator**: Replaces 13 hand-written sync wrappers across API modules (`screening.py`, `historical.py`, `intraday.py`, etc.) with a single `sync_api(async_fn)` call
- **Table-driven deprecation wrappers**: 23 manual wrapper functions in `blp.py` replaced by dict + loop pattern; 24 `warn_*` functions in `deprecation.py` replaced by `_DEPRECATION_REGISTRY` + `get_warn_func()` lookup
- **Market session rules extracted to TOML** (`markets/config/sessions.toml`): All MIC and exchange code rules moved from `sessions.py` into data-driven TOML config, reducing `sessions.py` from 364 to 168 lines (54% reduction)
- **Pipeline factory registry** (`pipeline_factories.py`): Centralized factory dispatch replaces scattered conditionals
- **Wildcard imports in `__init__.py` files**: 9 `__init__.py` files simplified to use wildcard imports with explicit `__all__` lists
- **CDX ticker format corrected**: Version is now a separate space-delimited token (e.g., `CDX HY CDSI S45 V2 5Y Corp` instead of `S45V2`)
- **`tomli` conditional dependency added**: `tomli>=2.0.1` for Python < 3.11 (TOML parsing for `sessions.toml`)
- **Net reduction of ~1,346 lines** across 27 files from codegen and table-driven optimizations

### Removed

- **`update_readme_on_release.yml` workflow**: Inline changelog in README replaced by link to `CHANGELOG.md`
- **`MONTH_CODE_MAP` and futures candidate generation helpers**: Superseded by `FUT_CHAIN_LAST_TRADE_DATES` chain resolution (#223)

### Fixed

- **`bdib` timezone regression**: The Arrow pipeline rewrite (v0.11.0) dropped the UTC-to-exchange local timezone conversion that existed in v0.7.x. Intraday bar timestamps were returned in UTC instead of exchange local time. Restored the conversion in `IntradayTransformer.transform()` with configurable `tz` parameter
- **`create_request` crashed when `ovrds` passed as dict**: `create_request(ovrds={"PRICING_SOURCE": "BGN"})` raised `ValueError: too many values to unpack` because iterating a dict yields keys (strings), not (key, value) tuples. Now normalizes dict to list of tuples before iteration. Also updated type annotation to accept `dict[str, Any]` ([SO#79880156](https://stackoverflow.com/questions/79880156))
- **Case-sensitive `backend` and `format` parameters**: `Backend("POLARS")` and `Format("WIDE")` raised `ValueError` because enum values are lowercase. Added `_missing_` classmethod to both `Backend` and `Format` enums for case-insensitive lookup (#221)
- **`StrEnum` Python 3.10 compatibility**: Added `StrEnum` polyfill in options module for Python < 3.11 where `enum.StrEnum` does not exist
- **Python 3.10 mock patching**: Fixed `patch.object()` usage for Python 3.10 compatible mock patching in tests by exposing submodules and patching at source
- **Non-ASCII characters in source**: Replaced checkmarks, em dashes, and arrows with ASCII equivalents across the codebase for CI compliance
- **Ruff lint errors**: Fixed import sorting (I001) and docstring formatting issues

## [0.12.0b2] - 2026-02-13

### Added

- **16 unit tests for `_events_to_table()`** (`test_events_to_table.py`): covers basic contract, mixed-type columns (float+str, int+str, float+date, kitchen sink), null handling, non-uniform dict keys, and pipeline integration (#219)
- **2 live regression tests for mixed-type BDP** (`test_live_endpoints.py`): `test_bdp_mixed_type_fields` and `test_bdp_mixed_type_multiple_tickers` exercise the exact bug scenario with `ES1 Index` / `NQ1 Index` using `FUT_CONT_SIZE` + `FUT_VAL_PT` (#219)

### Fixed

- **`ArrowInvalid` on multi-field BDP calls**: Bloomberg returns different Python types for different fields (e.g., `float` for `FUT_CONT_SIZE`, `str` for `FUT_VAL_PT`). When both land in the same Arrow value column, `pa.array()` raised `ArrowInvalid`. New `_events_to_table()` builds Arrow tables directly from event dicts with automatic type coercion fallback — stringify on `ArrowInvalid`/`ArrowTypeError`, preserving nulls (#219)
- **Post-transform `pa.Table.from_pandas()` mixed-type failure**: Protected the secondary Arrow conversion (after narwhals transform) with the same stringify fallback for object columns (#219)

## [0.12.0b1] - 2026-02-12

### Changed

- **Async-first architecture**: All Bloomberg API functions (`bdp`, `bds`, `bdh`, `bdib`, `bdtick`, `bql`, `beqs`, `bsrch`, `bqr`, `bta`) now have async counterparts (`abdp`, `abds`, `abdh`, etc.) as the source of truth; sync wrappers delegate via `_run_sync()` (#218)
- **Unified I/O layer**: All Bloomberg requests now flow through a single `arequest()` async entry point in `conn.py`, replacing scattered session/service management across modules (#218)
- **Pipeline and process modules**: Adapted `pipeline_core`, `process`, and `request_builder` to work with the async `arequest()` foundation (#218)
- **Top-level async exports**: All async API variants (`abdp`, `abds`, `abdh`, `abdib`, `abdtick`, `abql`, `abeqs`, `absrch`, `abqr`, `abta`) exported from `xbbg.blp` (#218)
- **IO module cleanup**: Removed dead code and fixed type annotations across `xbbg/io/` (#218)
- **Test coverage expanded**: 571 tests total (up from 543), covering all connection-related GitHub issues and all previously untested paths in `conn.py`

### Removed

- **`xbbg/io/db.py`**: SQLite database helper module (zero imports across codebase) (#218)
- **`xbbg/io/param.py`**: Legacy parameter/configuration module (zero imports across codebase) (#218)
- **`xbbg/io/files.py`**: File path utility module (zero imports after replacing 6 usages in `cache.py` and `const.py` with `pathlib.Path`) (#218)
- **`xbbg/tests/test_param.py`**: Tests for deleted `param` module (7 tests) (#218)
- **`xbbg/markets/cached/pmc_cache.json`**: Stale pandas-market-calendars cache file (pmc dependency removed in v0.11.0) (#218)
- **`xbbg/tests/__init__.py`**, **`examples/feeds/__init__.py`**: Empty `__init__` files (#218)
- **`xbbg/tests/xone.db`**: Stale SQLite test database (#218)
- **`regression_testing/`**: Standalone v0.7.7 regression test directory (6 files); all 9 test scenarios already covered by `xbbg/tests/test_live_endpoints.py` with stricter assertions (#218)

### Fixed

- **Mock session leak in tests**: Added autouse `_reset_session_manager` fixture in `conftest.py` to prevent `MagicMock` sessions from persisting in the `SessionManager` singleton across test modules, which caused infinite `__getattr__` → `_get_child_mock` recursion and stack overflow on Windows (#213)
- **`interval` parameter leaked as Bloomberg override**: `interval` was not in `PRSV_COLS`, causing it to be sent to Bloomberg as an override field instead of being used locally for bar sizing (#145)
- **README Data Storage section**: Clarified that only `bdib()` (intraday bars) has caching via `BarCacheAdapter`; all other functions always make live Bloomberg API calls (#215)
- **README async example for Jupyter**: Fixed `asyncio.run()` example that fails in notebooks (which already have a running event loop) by adding `await`-based and `nest_asyncio` alternatives (#216)
- **Unused imports in tests**: Removed `import os` from `test_intraday_api.py` and `import pytest` from `test_logging.py` that caused Ruff F401 lint failures in CI

### Security

- **Bump `cryptography` from 46.0.4 to 46.0.5**: Fixes CVE-2026-26007 — subgroup attack due to missing validation for SECT binary elliptic curves (#217)

## [0.11.4] - 2026-02-06

### Fixed

- **`bdtick` Arrow conversion failure**: Object columns containing `blpapi.Name` instances caused `pa.Table.from_pandas()` to fail; now stringified before conversion
- **`adjust_ccy` field name mismatch**: Looked for `"Last_Price"` but `bdh` returns lowercase `"last_price"` since v0.11.1, causing `KeyError`
- **`active_futures` two failures**: Used `nw.coalesce()` with a column (`last_tradeable_dt`) not present in SEMI_LONG format, and called `.height` (not valid on narwhals DataFrame) instead of `.shape[0]`
- **Live test assertions**: Updated 10 tests in `test_live_endpoints.py` to match WIDE format default (active since v0.7.x)

## [0.11.3] - 2026-02-06

### Fixed

- **Duplicate `port` keyword argument**: `bbg_service()` and `bbg_session()` used `.get()` to extract `port` then forwarded `**kwargs` still containing it, causing `TypeError: got multiple values for keyword argument 'port'` on non-default ports (e.g., B-Pipe connections) (#212)
- **Session resource leak**: `clear_default_session()` set `_default_session = None` without calling `session.stop()`, leaking OS file descriptors over repeated connect/disconnect cycles (#211)
- **Wrong session removed on retry**: `send_request()` retry path called `remove_session(port=port)` without `server_host`, always targeting `//localhost:{port}` even for remote hosts
- **Inconsistent `server_host` extraction**: `get_session()` / `get_service()` checked `server_host` before `server`, but `connect_bbg()` did the opposite, causing different code paths to resolve different hosts when both keys were present
- **Resource leak on start failure**: `connect_bbg()` did not stop the session before raising `ConnectionError` when `.start()` failed, leaking C++ resources allocated by the `Session()` constructor

## [0.11.2] - 2026-02-05

### Added

- **Extended multi-backend support**: Added 6 new backends matching narwhals' full backend support:
  - **Eager backends**: `cudf` (GPU-accelerated via NVIDIA RAPIDS), `modin` (distributed pandas)
  - **Lazy backends**: `dask` (parallel computing), `ibis` (portable DataFrame expressions), `pyspark` (Apache Spark), `sqlframe` (SQL-based DataFrames)
  - Total: 13 backends (6 eager + 7 lazy)
- **Backend availability checking**: New functions to check and validate backend availability with helpful error messages:
  - `is_backend_available(backend)` - Check if a backend package is installed
  - `check_backend(backend)` - Check availability with version validation, raises helpful errors
  - `get_available_backends()` - List all currently available backends
  - `print_backend_status()` - Diagnostic function showing all backend statuses
- **Format compatibility checking**: New functions to validate format support per backend:
  - `is_format_supported(backend, format)` - Check if a format works with a backend
  - `get_supported_formats(backend)` - Get set of supported formats for a backend
  - `check_format_compatibility(backend, format)` - Validate with helpful errors
  - `validate_backend_format(backend, format)` - Combined validation for API functions
- **`xbbg.ext` module**: New extension module for v1.0 migration containing helper functions that will be removed from `blp` namespace
  - `xbbg.ext.currency` - `adjust_ccy()` for currency conversion
  - `xbbg.ext.dividends` - `dividend()` for dividend history
  - `xbbg.ext.earnings` - `earning()` for earnings breakdowns
  - `xbbg.ext.turnover` - `turnover()` for trading volume
  - `xbbg.ext.holdings` - `etf_holdings()`, `preferreds()`, `corporate_bonds()` BQL helpers
  - `xbbg.ext.futures` - `fut_ticker()`, `active_futures()` for futures resolution
  - `xbbg.ext.cdx` - `cdx_ticker()`, `active_cdx()` for CDX index resolution
  - `xbbg.ext.yas` - `yas()`, `YieldType` for fixed income analytics
- New v1.0-compatible import path: `from xbbg.ext import dividend, fut_ticker, ...` (no deprecation warnings)
- **Pandas removed as required dependency**: `xbbg.ext` modules now use only stdlib datetime and narwhals, making pandas fully optional

### Changed

- **Backend enum reorganized**: Backends now categorized as eager (full API) vs lazy (deferred execution)
- **Format restrictions**: WIDE format only available for eager backends (pandas, polars, pyarrow, narwhals, cudf, modin); lazy backends limited to LONG and SEMI_LONG
- **Version requirements updated**: Minimum versions now match narwhals requirements (duckdb>=1.0, dask>=2024.1)
- `xbbg/markets/resolvers.py` now re-exports from `xbbg.ext.futures` and `xbbg.ext.cdx` for backwards compatibility
- Internal implementations moved to `xbbg/ext/` module; old import paths still work with deprecation warnings

### Fixed

- **BDS output format**: Restored v0.10.x backward compatibility for `bds()` output format (#209)
  - Default `format='wide'` now returns single data column with ticker as index (pandas) or column (other backends)
  - Field column dropped for cleaner output matching v0.10.x behavior
  - Users can opt-in to new 3-column format with `format='long'`
- **ibis backend**: Updated to use `ibis.memtable()` instead of deprecated `con.read_in_memory()`
- **sqlframe backend**: Fixed import path to use `sqlframe.duckdb.DuckDBSession`

## [0.11.1] - 2026-02-05

### Fixed

- **Field names now lowercase**: Restored v0.10.x behavior where `bdp()`, `bdh()`, and `bds()` return field/column names as lowercase (#206)

## [0.11.0] - 2026-02-02

### Added

- **Arrow-first pipeline**: Complete rewrite of internal data processing using PyArrow for improved performance
- **Multi-backend support**: New `Backend` enum supporting narwhals, pandas, polars, polars_lazy, pyarrow, duckdb
- **Output format control**: New `Format` enum with long, semi_long, wide options
- **bta()**: Bloomberg Technical Analysis function for 50+ technical indicators (#175)
- **bqr()**: Bloomberg Quote Request function emulating Excel `=BQR()` for dealer quote data with broker attribution (#22)
- **yas()**: Bloomberg YAS (Yield Analysis) wrapper for fixed income analytics with `YieldType` enum
- **preferreds()**: BQL convenience function to find preferred stocks for an equity ticker
- **corporate_bonds()**: BQL convenience function to find active corporate bonds for a ticker
- `set_backend()`, `get_backend()`, `set_format()`, `get_format()` configuration functions
- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`
- v1.0-compatible exception classes (`BlpError`, `BlpSessionError`, `BlpRequestError`, etc.)
- `EngineConfig` dataclass and `configure()` function for engine configuration
- `Service` and `Operation` enums for Bloomberg service URIs
- Treasury & SOFR futures support: TY, ZN, ZB, ZF, ZT, UB, TN, SFR, SR1, SR3, ED futures (#198)
- Comprehensive logging improvements across critical paths with better error traceability
- CONTRIBUTING.md and CODE_OF_CONDUCT.md for community standards

### Changed

- All API functions now accept `backend` and `format` parameters
- Internal pipeline uses PyArrow tables with narwhals transformations
- Removed pytz dependency (using stdlib `datetime.timezone`)
- **Intraday cache now includes interval in path** (#80) - different bar intervals cached separately (**breaking**: existing cache will miss)
- Internal class renames with backward compatible aliases (`YamlMarketInfoProvider` → `MetadataProvider`)
- Logging level adjustments: `BBG_ROOT not set` promoted to WARNING, cache timing demoted to DEBUG

### Deprecated

- `connect()` / `disconnect()` - engine auto-initializes in v1.0
- `getBlpapiVersion()` - use `get_sdk_info()` instead
- `lookupSecurity()` - will become `blkp()` in v1.0
- `fieldInfo()` / `fieldSearch()` - will merge into `bfld()` in v1.0
- `bta_studies()` - renamed to `ta_studies()` in v1.0
- `getPortfolio()` - renamed to `bport()` in v1.0
- Helper functions (`dividend()`, `earning()`, `turnover()`, `adjust_ccy()`) moving to `xbbg.ext` in v1.0
- Futures/CDX utilities (`fut_ticker()`, `active_futures()`, `cdx_ticker()`, `active_cdx()`) moving to `xbbg.ext` in v1.0

### Removed

- **Trials mechanism**: Eliminated retry-blocking system that caused silent failures after 2 failed attempts
- **pandas-market-calendars dependency**: Exchange info now sourced exclusively from Bloomberg API with local caching

### Fixed

- **Import without blpapi installed**: Fixed `AttributeError` when importing xbbg without blpapi (#200)
- **Japan/non-US timezone fix for bdib**: Trading hours now correctly converted to exchange's local timezone (#198)
- **stream() field values**: Subscribed field values now always included in output dict (#199)
- **Slow Bloomberg fields**: TIMEOUT events handled correctly; requests wait for response with `slow_warn_seconds` warning (#193)
- **Pipeline data types**: Preserve original data types instead of converting to strings (#191)
- **Futures symbol parsing**: Fixed `market_info()` to correctly parse symbols like `TYH6` → `TY` (#198)
- **get_tz() optimization**: Direct timezone strings recognized without Bloomberg API call
- **bdtick timezone fix**: Pass exchange timezone to fix blank results for non-UTC exchanges (#185)
- **bdtick timeout**: Increased from 10s to 2 minutes for tick data requests
- Extended BDS test date range to 120 days for quarterly dividends
- Helper functions now work correctly with LONG format output
- Logging format compliance fixes (G004, G201)

## [0.11.0b5] - 2026-01-25

### Changed

- Internal class renames with backward compatible aliases (`YamlMarketInfoProvider` -> `MetadataProvider`)

### Removed

- **Trials mechanism**: Eliminated retry-blocking system that caused silent failures after 2 failed attempts
- **pandas-market-calendars dependency**: Exchange info now sourced exclusively from Bloomberg API with local caching

### Fixed

- **Import without blpapi installed**: Fixed `AttributeError` when importing xbbg without blpapi (#200)
- **Japan/non-US timezone fix for bdib**: Bloomberg returns trading hours in EST; now correctly converted to exchange's local timezone (#198)
- **get_tz() improvement**: Direct timezone strings recognized without Bloomberg API call

## [0.11.0b4] - 2026-01-24

### Added

- **yas()**: Bloomberg YAS (Yield Analysis) wrapper for fixed income analytics with `YieldType` enum (#202)
- **Treasury and SOFR futures support**: TY, ZN, ZB, ZF, ZT, UB, TN, SFR, SR1, SR3, ED futures (#198)

### Fixed

- **stream() field values**: Subscribed field values now always included in output dict (#199)
- **Futures symbol parsing**: Fixed `market_info()` to correctly parse symbols like `TYH6` -> `TY` (#198)

## [0.11.0b3] - 2026-01-21

### Added

- **bqr()**: Bloomberg Quote Request function emulating Excel `=BQR()` for dealer quote data with broker attribution (#22)

### Fixed

- **Slow Bloomberg fields**: TIMEOUT events handled correctly; requests wait for response with `slow_warn_seconds` warning (#193)
- **Pipeline data types**: Preserve original data types instead of converting to strings (#191)

## [0.11.0b2] - 2026-01-20

### Added

- **preferreds()**: BQL convenience function to find preferred stocks for an equity ticker
- **corporate_bonds()**: BQL convenience function to find active corporate bonds for a ticker

### Fixed

- **bdtick timezone fix**: Pass exchange timezone to fix blank results for non-UTC exchanges (#185)
- **bdtick timeout**: Increased from 10s to 2 minutes for tick data requests

## [0.11.0b1] - 2026-01-10

### Added

- **Arrow-first pipeline**: Complete rewrite of data processing using PyArrow internally
- **Multi-backend support**: New `Backend` enum supporting narwhals, pandas, polars, polars_lazy, pyarrow, duckdb
- **Output format control**: New `Format` enum with long, semi_long, wide options
- **bta()**: Bloomberg Technical Analysis function for 50+ technical indicators
- `set_backend()`, `get_backend()`, `set_format()`, `get_format()` configuration functions
- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`
- v1.0-compatible exception classes (`BlpError`, `BlpSessionError`, etc.)
- `EngineConfig` dataclass and `configure()` function
- `Service` and `Operation` enums for Bloomberg service URIs

### Changed

- All API functions now support `backend` and `format` parameters
- Internal pipeline uses PyArrow tables with narwhals transformations
- Removed pytz dependency (using stdlib `datetime.timezone`)

### Deprecated

- `connect()` / `disconnect()` - engine auto-initializes in v1.0
- `getBlpapiVersion()` - use `get_sdk_info()`
- `lookupSecurity()` - will become `blkp()` in v1.0
- `fieldInfo()` / `fieldSearch()` - will merge into `bfld()` in v1.0

## [0.10.3] - 2025-12-29

### Changed

- Re-enabled futures and CDX resolver tests
- Updated live endpoint tests for LONG format output
- Code style improvements using contextlib.suppress instead of try-except-pass

### Fixed

- Extended BDS test date range to 120 days for quarterly dividends
- Helper functions now work correctly with LONG format output

## [0.10.2] - 2025-12-29

### Changed

- CI/CD improvements with reusable workflows (workflow_call) for release automation
- Separated pypi_upload workflow for trusted publisher compatibility

## [0.10.1] - 2025-12-29

### Changed

- Trigger release workflows via release event instead of workflow_dispatch
- Removed Gitter badge (replaced by Discord)
- Added Discord community link and badge

### Fixed

- Persist blp.connect() session for subsequent API calls (#165)

## [0.10.0] - 2025-12-25

### Added

- Updated polars-bloomberg support for BQL, BDIB and BSRCH (#155)

### Fixed

- Add identifier type prefix to B-Pipe subscription topics (#156)
- Remove pandas version cap to support Python 3.14 (#161)
- Resolve RST formatting warning in index.rst (#162)
- Update Japan equity market hours for TSE trading extension (#163)

## [0.9.1] - 2025-12-11

### Changed

- Add blank lines around latest-release markers in index.rst
- Remove redundant release triggers from workflows
- Trigger release workflows explicitly from semantic_version

### Fixed

- Fix BQL returning only one row for multi-value results (#152)

## [0.9.0] - 2025-12-02

### Added

- Add etf_holdings() function for retrieving ETF holdings via BQL (#147)
- Add multi-day support to bdib() (#148)
- Add multi-day cache support for bdib() (#149)

### Fixed

- Resolve RST duplicate link targets and Sphinx build warnings

## [0.8.2] - 2025-11-19

### Fixed

- Fix BQL options chain metadata issues (#146)

## [0.8.1] - 2025-11-17

### Changed

- CI/CD workflow improvements for trusted publisher compatibility

## [0.8.0] - 2025-11-16

### Added

- **bsrch()**: Bloomberg SRCH queries for fixed income, commodities, and weather data (#137)
- **Fixed income securities support**: ISIN/CUSIP/SEDOL identifiers for bdib (#136)
- **Server host parameter**: Connect to remote Bloomberg servers via `server` parameter (#138)
- **Interval parameter for subscribe()/live()**: Configurable update intervals for real-time feeds
- Semantic versioning workflow for automated releases
- Support for GY (Xetra), IM (Borsa Italiana), and SE (SIX) exchanges (#140)
- Comprehensive bar interval selection guide for bdib function

### Changed

- Comprehensive codebase cleanup and restructuring (#144)
- Improved logging with blpapi integration and performance optimizations (#135)
- Enhanced BEQS timeout handling with configurable `timeout` and `max_timeouts` parameters
- Updated README with comparison table, quickstart guide, and examples

### Fixed

- Fix BQL syntax documentation and error handling (#141, #142)
- Remove 1-minute offset for bare session names in bdtick (#139)
- Resolve Sphinx build errors and RST formatting issues

## [0.8.0rc1] - 2025-11-17

### Changed

- Comprehensive codebase cleanup and restructuring (#144)

## [0.8.0b2] - 2025-11-14

### Fixed

- Fix BQL syntax documentation and error handling (#141, #142)

## [0.8.0b1] - 2025-11-14

### Added

- **BQL support**: Bloomberg Query Language with QueryRequest and result parsing
- **Sub-minute intervals for bdib**: 10-second bars via `intervalHasSeconds=True` flag
- **bsrch()**: Bloomberg SRCH queries for fixed income, commodities, and weather data (#137)
- **Fixed income securities support**: ISIN/CUSIP/SEDOL identifiers for bdib (#136)
- **Server host parameter**: Connect to remote Bloomberg servers via `server` parameter (#138)
- **Interval parameter for subscribe()/live()**: Configurable update intervals for real-time feeds
- Support for GY (Xetra), IM (Borsa Italiana), and SE (SIX) exchanges (#140)

### Changed

- Standardized Google-style docstrings across codebase
- Migrate to uv for development with PEP 621 pyproject.toml
- Improved logging with blpapi integration and performance optimizations (#135)
- Enhanced BEQS timeout handling with configurable `timeout` and `max_timeouts` parameters

### Fixed

- Remove 1-minute offset for bare session names in bdtick (#139)

## [0.7.11] - 2025-11-12

### Added

- **BQL support**: Bloomberg Query Language with QueryRequest and result parsing
- **Sub-minute intervals for bdib**: 10-second bars via `intervalHasSeconds=True` flag
- pandas-market-calendars integration for exchange session resolution

### Changed

- Standardized Google-style docstrings across codebase
- Migrate to uv for development with PEP 621 pyproject.toml
- Switch to PyPI Trusted Publishing (OIDC)
- Exclude tests from wheel and sdist distributions

### Fixed

- Fix BQL to use correct service name and handle JSON response format
- Normalize UX\* Index symbols; fix pandas 'M' deprecation to 'ME' in fut_ticker

## [0.7.10] - 2025-11-05

### Added

- Enhanced Bloomberg connection handling with alternative connection methods
- Market resolvers for active futures and CDX tickers

### Changed

- Replace flake8 with ruff for linting
- Update Python version requirements and dependencies
- Clean up CI workflows and documentation

## [0.7.9] - 2025-04-15

### Changed

- Add exchanges support
- CI/CD configuration updates

### Fixed

- Corrected typo (thanks to @ShiyuanSchonfeld)
- Pin pandas version due to pd.to_datetime behaviour change in format_raw
- Fix TLS Options typo when creating a new connection

## [0.7.8a2] - 2022-12-03

### Added

- Additional exchanges support (#83)

### Changed

- CI/CD configuration improvements

## [0.7.7] - 2022-06-19

### Added

- Custom config usage in bdib (contributed by @hceh)
- Options in `blp.live` (contributed by @swiecki)

### Changed

- Pandas options handling in doctest
- CI/CD configuration updates

## [0.7.7a4] - 2022-05-25

### Changed

- Pandas options handling in doctest

## [0.7.7a3] - 2021-12-31

### Fixed

- Typo fix

## [0.7.7a2] - 2021-12-20

### Added

- Custom config and reference exchange support (contributed by @hceh)

## [0.7.7a1] - 2021-07-13

### Added

- Options in `blp.live` (contributed by @swiecki)

## [0.7.6] - 2021-07-05

### Added

- Log folder creation handling
- Alternative connection method support
- Custom session argument for Bloomberg connections
- `bdtick` with custom time range support

### Changed

- Update asset universe
- Exchange info corrections
- No manual conversion of timezones

### Fixed

- BDS fix for edge cases
- blpapi install URL correction

## [0.7.6a8] - 2021-04-17

### Fixed

- Log folder creation bug

## [0.7.6a7] - 2021-04-02

### Changed

- Update asset universe

## [0.7.6a6] - 2021-03-27

### Fixed

- Exchange info corrected

## [0.7.6a5] - 2021-03-05

### Changed

- No manual conversion of timezones

## [0.7.6a4] - 2021-03-05

### Added

- `bdtick` with custom time range support

## [0.7.6a3] - 2021-02-10

### Fixed

- Bug fixes for BDS and blpapi install URL

## [0.7.6a2] - 2021-02-07

### Added

- Alternative connection method

## [0.7.6a1] - 2021-02-03

### Added

- Add `sess` as argument for custom Bloomberg session

## [0.7.5] - 2021-01-31

### Added

- Currency adjusted turnover function
- Useful fields for live feeds
- More examples in documentation

### Changed

- Standardize IO operations
- Log levels adjustment
- Replace `os.path` with pathlib
- Performance function improvements
- Default args of live feeds

### Fixed

- CCY adjust fix
- Bug in finding exchange info

## [0.7.5b2] - 2021-01-30

### Changed

- Log levels adjustment

## [0.7.5b1] - 2021-01-13

### Added

- New methods included in `__all__`

### Fixed

- CCY adjust fix

## [0.7.5a9] - 2021-01-12

### Added

- Currency adjusted turnover function

## [0.7.5a09] - 2021-01-12

### Added

- Currency adjusted turnover function

## [0.7.5a8] - 2021-01-11

### Fixed

- Fix bug in finding exchange info

## [0.7.5a7] - 2021-01-07

### Changed

- Default args of live feeds

## [0.7.2] - 2020-12-16

### Added

- Logo image for project branding

### Changed

- Use `async` for live data feeds
- Speed up by caching files
- Change logic of exchange lookup and market timing
- Push all values from live subscription
- Support for Python 3.8

### Fixed

- Proper caching implementation

## [0.7.0] - 2020-08-02

### Changed

- `bdh` preserves column orders (both tickers and flds)
- `timeout` argument is available for all queries
- `bdtick` usually takes longer to respond - can use `timeout=1000` for example if keep getting empty DataFrame

## [0.6.7] - 2020-05-17

### Added

- Add flexibility to use reference exchange as market hour definition
- No longer necessary to add `.yml` for new tickers, provided that the exchange was defined in `/xbbg/markets/exch.yml`

### Changed

- Switch CI from Travis to GitHub Actions

## [0.6.0] - 2020-01-23

### Added

- Tick data availability via bdtick()

### Changed

- Speed improvements by removing intermediate layer of generator for processing Bloomberg responses

## [0.5.0] - 2020-01-08

### Changed

- Rewritten library to add subscription, BEQS, simplify interface and remove dependency of `pdblp`

## [0.1.22] - 2019-09-15

### Security

- Remove PyYAML dependency due to security vulnerability

## [0.1.17] - 2019-07-01

### Added

- Add `adjust` argument in `bdh` for easier dividend / split adjustments

---

[Unreleased]: https://github.com/xbbg-org/xbbg/compare/v1.4.11...HEAD
[1.4.11]: https://github.com/xbbg-org/xbbg/compare/v1.4.10...v1.4.11
[1.4.10]: https://github.com/xbbg-org/xbbg/compare/v1.4.9...v1.4.10
[1.4.9]: https://github.com/xbbg-org/xbbg/compare/v1.4.8...v1.4.9
[1.4.8]: https://github.com/xbbg-org/xbbg/compare/v1.4.7...v1.4.8
[1.4.7]: https://github.com/xbbg-org/xbbg/compare/v1.4.6...v1.4.7
[1.4.6]: https://github.com/xbbg-org/xbbg/compare/v1.4.5...v1.4.6
[1.4.5]: https://github.com/xbbg-org/xbbg/compare/v1.4.4...v1.4.5
[1.4.4]: https://github.com/xbbg-org/xbbg/compare/v1.4.3...v1.4.4
[1.4.3]: https://github.com/xbbg-org/xbbg/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/xbbg-org/xbbg/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/xbbg-org/xbbg/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/xbbg-org/xbbg/compare/v1.3.1...v1.4.0
[1.3.1]: https://github.com/xbbg-org/xbbg/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/xbbg-org/xbbg/compare/v1.2.7...v1.3.0
[1.2.7]: https://github.com/xbbg-org/xbbg/compare/v1.2.6...v1.2.7
[1.2.6]: https://github.com/xbbg-org/xbbg/compare/v1.2.5...v1.2.6
[1.2.5]: https://github.com/xbbg-org/xbbg/compare/v1.2.4...v1.2.5
[1.2.4]: https://github.com/xbbg-org/xbbg/compare/v1.2.3...v1.2.4
[1.2.3]: https://github.com/xbbg-org/xbbg/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/xbbg-org/xbbg/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/xbbg-org/xbbg/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/xbbg-org/xbbg/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/xbbg-org/xbbg/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/xbbg-org/xbbg/compare/v1.1.1b1...v1.1.1
[1.1.1b1]: https://github.com/xbbg-org/xbbg/compare/v1.1.0...v1.1.1b1
[1.1.0]: https://github.com/xbbg-org/xbbg/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/xbbg-org/xbbg/compare/v1.0.0rc4...v1.0.0
[1.0.0rc4]: https://github.com/xbbg-org/xbbg/compare/v1.0.0rc3...v1.0.0rc4
[1.0.0rc3]: https://github.com/xbbg-org/xbbg/compare/v1.0.0rc2...v1.0.0rc3
[1.0.0rc2]: https://github.com/xbbg-org/xbbg/compare/v1.0.0rc1...v1.0.0rc2
[1.0.0rc1]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b7...v1.0.0rc1
[1.0.0b7]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b6...v1.0.0b7
[1.0.0b6]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b5...v1.0.0b6
[1.0.0b5]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b4...v1.0.0b5
[1.0.0b4]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b3...v1.0.0b4
[1.0.0b3]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b2...v1.0.0b3
[1.0.0b2]: https://github.com/xbbg-org/xbbg/compare/v1.0.0b1...v1.0.0b2
[1.0.0b1]: https://github.com/xbbg-org/xbbg/compare/v1.0.0a3...v1.0.0b1
[1.0.0a3]: https://github.com/xbbg-org/xbbg/compare/v1.0.0a2...v1.0.0a3
[1.0.0a2]: https://github.com/xbbg-org/xbbg/compare/v1.0.0a1...v1.0.0a2
[1.0.0a1]: https://github.com/xbbg-org/xbbg/compare/v0.12.1...v1.0.0a1
[0.12.0]: https://github.com/xbbg-org/xbbg/compare/v0.12.0b3...v0.12.0
[0.12.0b3]: https://github.com/xbbg-org/xbbg/compare/v0.12.0b2...v0.12.0b3
[0.12.0b2]: https://github.com/xbbg-org/xbbg/compare/v0.12.0b1...v0.12.0b2
[0.12.0b1]: https://github.com/xbbg-org/xbbg/compare/v0.11.4...v0.12.0b1
[0.11.4]: https://github.com/xbbg-org/xbbg/compare/v0.11.3...v0.11.4
[0.11.3]: https://github.com/xbbg-org/xbbg/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/xbbg-org/xbbg/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/xbbg-org/xbbg/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/xbbg-org/xbbg/compare/v0.11.0b5...v0.11.0
[0.11.0b5]: https://github.com/xbbg-org/xbbg/compare/v0.11.0b4...v0.11.0b5
[0.11.0b4]: https://github.com/xbbg-org/xbbg/compare/v0.11.0b3...v0.11.0b4
[0.11.0b3]: https://github.com/xbbg-org/xbbg/compare/v0.11.0b2...v0.11.0b3
[0.11.0b2]: https://github.com/xbbg-org/xbbg/compare/v0.11.0b1...v0.11.0b2
[0.11.0b1]: https://github.com/xbbg-org/xbbg/compare/v0.10.3...v0.11.0b1
[0.10.3]: https://github.com/xbbg-org/xbbg/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/xbbg-org/xbbg/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/xbbg-org/xbbg/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/xbbg-org/xbbg/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/xbbg-org/xbbg/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/xbbg-org/xbbg/compare/v0.8.2...v0.9.0
[0.8.2]: https://github.com/xbbg-org/xbbg/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/xbbg-org/xbbg/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/xbbg-org/xbbg/compare/v0.8.0rc1...v0.8.0
[0.8.0rc1]: https://github.com/xbbg-org/xbbg/compare/v0.8.0b2...v0.8.0rc1
[0.8.0b2]: https://github.com/xbbg-org/xbbg/compare/v0.8.0b1...v0.8.0b2
[0.8.0b1]: https://github.com/xbbg-org/xbbg/compare/v0.7.11...v0.8.0b1
[0.7.11]: https://github.com/xbbg-org/xbbg/compare/v0.7.10...v0.7.11
[0.7.10]: https://github.com/xbbg-org/xbbg/compare/v0.7.9...v0.7.10
[0.7.9]: https://github.com/xbbg-org/xbbg/compare/v0.7.8a2...v0.7.9
[0.7.8a2]: https://github.com/xbbg-org/xbbg/compare/v0.7.7...v0.7.8a2
[0.7.7]: https://github.com/xbbg-org/xbbg/compare/v0.7.7a4...v0.7.7
[0.7.7a4]: https://github.com/xbbg-org/xbbg/compare/v0.7.7a3...v0.7.7a4
[0.7.7a3]: https://github.com/xbbg-org/xbbg/compare/v0.7.7a2...v0.7.7a3
[0.7.7a2]: https://github.com/xbbg-org/xbbg/compare/v0.7.7a1...v0.7.7a2
[0.7.7a1]: https://github.com/xbbg-org/xbbg/compare/v0.7.6...v0.7.7a1
[0.7.6]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a8...v0.7.6
[0.7.6a8]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a7...v0.7.6a8
[0.7.6a7]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a6...v0.7.6a7
[0.7.6a6]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a5...v0.7.6a6
[0.7.6a5]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a4...v0.7.6a5
[0.7.6a4]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a3...v0.7.6a4
[0.7.6a3]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a2...v0.7.6a3
[0.7.6a2]: https://github.com/xbbg-org/xbbg/compare/v0.7.6a1...v0.7.6a2
[0.7.6a1]: https://github.com/xbbg-org/xbbg/compare/v0.7.5...v0.7.6a1
[0.7.5]: https://github.com/xbbg-org/xbbg/compare/v0.7.5b2...v0.7.5
[0.7.5b2]: https://github.com/xbbg-org/xbbg/compare/v0.7.5b1...v0.7.5b2
[0.7.5b1]: https://github.com/xbbg-org/xbbg/compare/v0.7.5a9...v0.7.5b1
[0.7.5a9]: https://github.com/xbbg-org/xbbg/compare/v0.7.5a09...v0.7.5a9
[0.7.5a09]: https://github.com/xbbg-org/xbbg/compare/v0.7.5a8...v0.7.5a09
[0.7.5a8]: https://github.com/xbbg-org/xbbg/compare/v0.7.5a7...v0.7.5a8
[0.7.5a7]: https://github.com/xbbg-org/xbbg/compare/v0.7.2...v0.7.5a7
[0.7.2]: https://github.com/xbbg-org/xbbg/compare/v0.7.0...v0.7.2
[0.7.0]: https://github.com/xbbg-org/xbbg/compare/v0.6.7...v0.7.0
[0.6.7]: https://github.com/xbbg-org/xbbg/compare/v0.6.0...v0.6.7
[0.6.0]: https://github.com/xbbg-org/xbbg/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/xbbg-org/xbbg/compare/v0.1.22...v0.5.0
[0.1.22]: https://github.com/xbbg-org/xbbg/compare/v0.1.17...v0.1.22
[0.1.17]: https://github.com/xbbg-org/xbbg/releases/tag/v0.1.17
