# xbbg Release Process

This document explains the release process for xbbg, intended for AI agents and maintainers.

## Overview

xbbg uses **semantic versioning** (SemVer) with Python package versions **automatically derived from git tags** via `setuptools_scm`. The JS package families use the same version numbers, stamped during release workflows: `vX.Y.Z` for the npm publish flow and `js-vX.Y.Z` for the GitHub-only JS asset flow. The build system is `setuptools` + `setuptools-rust` + `setuptools_scm` for Python and npm package stamping for JS.

### Version Format

```
{major}.{minor}.{patch}[-{pre-release}]

Examples:
- 0.12.1        # Stable release
- 0.12.1b1      # Beta pre-release
- 0.12.1a1      # Alpha pre-release
- 0.12.1rc1     # Release candidate
```

Dev builds (untagged commits) automatically get versions like `0.12.1.dev268+g84acdcf.d20260219`.

### Build System

| Component | Package | Purpose |
|-----------|---------|---------|
| Build backend | `setuptools` | Python packaging |
| Rust extension | `setuptools-rust` | Compiles PyO3 extension (`xbbg._core`) |
| Version | `setuptools_scm` | Derives Python package versions from git tags |
| JS package version | `js-xbbg/scripts/stamp-version.ts` | Stamps `@xbbg/core` wrapper/platform package versions for JS release workflows |
| Build tool | `uv` | Fast package manager and build frontend |

## Release Workflow

### Step 1: Update CHANGELOG.md

Ensure all changes are documented under the `[Unreleased]` section:

```markdown
## [Unreleased]

### Added
- New feature description

### Changed
- Modified behavior description

### Fixed
- Bug fix description
```

**Categories** (use only what applies):
- `Added` - New features
- `Changed` - Changes in existing functionality
- `Deprecated` - Soon-to-be removed features
- `Removed` - Removed features
- `Fixed` - Bug fixes
- `Security` - Vulnerability fixes

### Step 2: Commit CHANGELOG Updates

```bash
git add CHANGELOG.md
git commit -m "docs(CHANGELOG): prepare for vX.Y.Z release"
git push
```

### Step 3: Trigger Release Workflow

Go to **GitHub Actions** > **Bump Version and Create Release** > **Run workflow**

**Parameters:**
| Parameter | Description | Options |
|-----------|-------------|---------|
| `bump_type` | Version increment | `major`, `minor`, `patch` |
| `pre_release` | Pre-release type | `none`, `alpha`, `beta`, `rc` |
| `pre_number` | Pre-release number | `1`, `2`, `3`, etc. |
| `create_release` | Create GitHub release | `true`, `false` |

**Examples:**
- `0.12.1` → `0.13.0`: bump_type=`minor`, pre_release=`none`
- `0.12.1` → `0.12.2`: bump_type=`patch`, pre_release=`none`
- `0.12.1` → `0.12.2b1`: bump_type=`patch`, pre_release=`beta`, pre_number=`1`

### Step 4: What Happens Automatically

1. **Version Calculation**: Computes new version from current tags
2. **Changelog Update**: Renames `[Unreleased]` to `[version] - date`
3. **README Release Sync**: Updates the `README.md` latest-release marker block to the new version/tag
4. **Git Tag**: Creates `vX.Y.Z` tag and pushes it
5. **GitHub Release**: Creates release with notes from CHANGELOG
6. **crates.io Publish**: `semantic_version.yml` calls `crates-publish.yml` directly as a
   dependent job, so the six published Rust crates go out in dependency order with no
   manual step. Stable versions only; pre-releases are skipped.

**Still manual after the run:** PyPI and npm are *not* automatic. Both
`pypi_upload.yml` and `npm-publish.yml` declare `push.tags: ["v*"]`, but the tag is
created with `GITHUB_TOKEN`, and a `GITHUB_TOKEN` tag push does not start
tag-triggered workflows. Dispatch each one manually on the new tag:

| Workflow | Dispatch with |
|----------|---------------|
| `pypi_upload.yml` | ref `vX.Y.Z`, `dry-run=false` |
| `npm-publish.yml` | `version=vX.Y.Z` |
| `mcp_registry_publish.yml` | `version=vX.Y.Z`, after `server.json` is attached to the Release |

`crates-publish.yml` avoids this trap entirely by being invoked as a reusable
workflow rather than waiting on a tag event.

### Organization rename cutover

Repository URLs and the MCP Registry namespace use `underloam`; Python package
names, npm's `@xbbg` scope, crate names, and `xbbg.org` are unchanged.
Before publishing, confirm the PyPI, npm, and crates.io trusted publishers use
the current GitHub owner with their existing workflow and environment restrictions.
Also verify that CI can pull both owner-qualified GHCR build images.

Historical release assets remain immutable. An older release's `server.json`
retains its original registry namespace; downloading it in
`mcp_registry_publish.yml` does not migrate that namespace.
For a registry-only cutover without a new package release:

1. Download the existing descriptor and the exact MCPB asset it identifies.
2. Verify the bundle bytes against both the descriptor's `fileSha256` and the
   GitHub release asset's SHA-256 digest.
3. Run `scripts/render_mcp_registry_server.py` with the descriptor's version,
   the canonical `underloam/xbbg` URL for that same asset, and its verified hash.
4. Validate and publish only the temporary `io.github.underloam/xbbg-mcp`
   descriptor. Do not upload or replace either historical release asset.

The old bundle retains its historical manifest metadata. Future releases generate
both the descriptor and bundle metadata under `underloam`. Existing registry
clients must switch to the new server name; a GitHub redirect does not rename
an MCP Registry subscription.

## CI/CD Workflows

### On Every Push/PR

| Workflow | File | Purpose |
|----------|------|---------|
| CI | `ci-rust.yml` | Rust lint, clippy, build, test (Linux + Windows) |
| Docker | `ci-docker.yml` | Build CI Docker image |

### Called by the release workflow

| Workflow | File | Purpose |
|----------|------|---------|
| Publish Rust Crates | `crates-publish.yml` | Reusable job invoked by `semantic_version.yml`; stamps the workspace version and publishes the six crates.io packages in dependency order |

### On Release (dispatch on tag `vX.Y.Z`)

| Workflow | File | Purpose |
|----------|------|---------|
| Release | `pypi_upload.yml` | Build wheels (manylinux_2_28 Linux + Windows + macOS × Python 3.10–3.14), sdist, publish to PyPI, attach to GitHub release |
| Release | `npm-publish.yml` | Build and publish stable `@xbbg/core` prebuilt native packages for supported platforms, then publish the `@xbbg/core` wrapper and `@xbbg/langgraph` package via npm trusted publishing |

### crates.io publishing

Published crates (6), in publish order:

| Crate | Depends on | Notes |
|-------|-----------|-------|
| `xbbg-blpapi-sys` | — | Registry name is prefixed because `blpapi-sys` is taken; `[lib] name = "blpapi_sys"` keeps `blpapi_sys::` working |
| `xbbg-log` | — | |
| `xbbg-ext` | — | Embeds `crates/xbbg-ext/data/exchanges.toml`; `include_str!` may not reach outside the package root |
| `xbbg_core` | `xbbg-blpapi-sys` | Underscore is permanent: crates.io treats `-` and `_` as the same identity, so `xbbg-core` cannot be claimed separately |
| `xbbg-async` | `xbbg_core`, `xbbg-ext`, `xbbg-log` | |
| `xbbg-recipes` | `xbbg-async`, `xbbg-ext`, `xbbg-log` | |

Never published (`publish = false`): `xbbg-arrow`, `xbbg-bench`, `pyo3-xbbg`,
`napi-xbbg`, `xbbg-mcp`. The bindings have no standalone value, and `xbbg-mcp`
ships as a prebuilt binary on the GitHub Release because `cargo install` cannot
build it without the Bloomberg SDK.

#### Known limitation: docs.rs

`xbbg-blpapi-sys`, `xbbg_core`, `xbbg-async`, and `xbbg-recipes` do **not** build on
docs.rs. The BLPAPI SDK is proprietary and the docs.rs sandbox has no network to
fetch it, so bindgen has no headers. `xbbg_core/src/ffi.rs` re-exports the BLPAPI
symbols unconditionally, so the failure propagates to everything above it.

An empty-stub build script was evaluated and rejected: it makes only the `-sys`
crate green, and with an empty API page, while `xbbg_core` still fails with 29
unresolved imports. `build.rs` therefore detects `DOCS_RS` and fails with an
explicit message so the docs.rs log states the real reason.

Making docs.rs green requires committing a pregenerated binding surface to the
repo. That is an open decision, not an oversight: the Bloomberg SDK license grants
permission to "use, publish, or distribute copies" but states that "modifying,
adapting, reverse engineering, decompiling, or disassembling, is not permitted",
and bindgen output is plausibly an adaptation of the SDK headers. Resolve the
licensing question before adding one.

`xbbg-log` and `xbbg-ext` are SDK-free and document normally on docs.rs.

#### crates.io trusted publishing setup

Tokenless via `rust-lang/crates-io-auth-action` and GitHub OIDC; there is no
`CARGO_REGISTRY_TOKEN` secret. **Each crate needs two Trusted Publisher entries:**

| # | Workflow filename | Covers |
|---|-------------------|--------|
| 1 | `semantic_version.yml` | The automatic release path |
| 2 | `crates-publish.yml` | Manual `workflow_dispatch` retries |

Both are required because crates.io validates the OIDC `workflow_ref` claim, and
for a `workflow_call` GitHub sets `workflow_ref` to the **calling** workflow. The
called file appears only as `job_workflow_ref`, which crates.io does not check. If
you register only `crates-publish.yml`, automatic releases fail OIDC while manual
dispatch still succeeds.

For every entry use repository owner `underloam`, repository `xbbg`, and leave the
environment blank.

Before releasing after an organization rename, confirm all 12 entries use the
current owner (6 crates × 2 workflows). No `CARGO_REGISTRY_TOKEN` secret is needed.
Keep both entries for every crate: dropping `semantic_version.yml` breaks
automatic releases while leaving manual dispatch working.

#### First publish of a new crate name

crates.io has no PyPI-style "pending publisher", so a Trusted Publisher cannot be
attached to a crate that does not exist yet, and an OIDC token is rejected with
`Trusted Publishing tokens do not support creating new crates`. When adding a
brand-new crate to the published set:

1. Publish once with a temporary API token, scoped as tightly as possible
   (`publish-new`, short expiry, crate pattern such as `xbbg-*`)
2. Add both Trusted Publisher entries above in the crates.io UI
3. Revoke the temporary token immediately and confirm it returns 403

This was done once for `xbbg-async` and `xbbg-recipes` at 1.4.7. Every existing
crate publishes tokenlessly from CI.

#### Retiring a published crate

A crate can be **yanked** at any time, but **deletion is usually impossible**:
crates.io refuses to delete a crate while *any* published version of another
crate depends on it, and individual versions can never be deleted. `xbbg-sys` is
permanently undeletable because `xbbg_core@1.1.2` depends on it, so it is yanked
instead.

Note that the `reverse_dependencies` API reports only edges from each crate's
*latest* version, so it can show no dependents while the delete endpoint still
returns 422. Yanking is the realistic retirement path.

#### npm trusted publishing setup

`npm-publish.yml` is tokenless: the publish job uses GitHub OIDC (`id-token: write`) from GitHub-hosted runners and npm CLI `>=11.10.0`. Configure this once on npmjs.com for each published package:

| npm package | Publisher | GitHub org/user | Repository | Workflow filename | Environment |
|-------------|-----------|-----------------|------------|-------------------|-------------|
| `@xbbg/core` | GitHub Actions | `underloam` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-linux-x64` | GitHub Actions | `underloam` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-win32-x64` | GitHub Actions | `underloam` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-darwin-arm64` | GitHub Actions | `underloam` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/langgraph` | GitHub Actions | `underloam` | `xbbg` | `npm-publish.yml` | leave blank |

GitHub environment `npm` is intentionally not required because current repository credentials cannot create it. Add an environment only if an admin wants reviewer-based release approvals; if you do, update both the workflow `environment:` and all npm trusted publisher entries to the exact same environment name.

After a successful OIDC publish, set each package's npm **Publishing access** to require 2FA and disallow tokens, then revoke any temporary publish tokens.

`npm-publish.yml` intentionally publishes only stable npm versions (`vX.Y.Z`). Python-style pre-release tags such as `vX.Y.Zb1` still trigger the workflow glob but are skipped because they are not valid npm semver versions for this package family.

### Manual Trigger

| Workflow | File | Purpose |
|----------|------|---------|
| Bump Version | `semantic_version.yml` | Calculate version, update CHANGELOG and README release marker, create tag + GitHub release |
| JS GitHub Release | `js_github_release.yml` | Build, validate, and attach GitHub-only JS tarballs for `@xbbg/core` on `js-vX.Y.Z` |
| npm Publish Retry | `npm-publish.yml` | Manual retry of trusted npm publishing for a stable `vX.Y.Z` version |

### JS GitHub-only package release

Use this workflow when you want GitHub release assets for the JS packages without npm publishing.

Go to **GitHub Actions** > **JS GitHub Release** > **Run workflow**

**Parameters:**
| Parameter | Description |
|-----------|-------------|
| `version` | Package version to stamp into the JS tarballs; the workflow creates or reuses the `js-vX.Y.Z` tag |
| `notes` | Optional maintainer notes appended to the GitHub release body |
| `draft` | Create the GitHub release as a draft |

**What happens automatically:**

1. Validates the requested version and targets the current workflow commit
2. Creates or reuses the `js-vX.Y.Z` tag without touching the global `vX.Y.Z` release flow
3. Builds native assets for the supported JS targets
4. Stamps both JS package families with the selected version
5. Packs and validates the GitHub release tarballs
6. Attaches the tarballs to a GitHub release on `js-vX.Y.Z`


**Attached artifacts (currently supported):**

- `@xbbg/core` wrapper + `darwin-arm64`, `linux-x64` (glibc 2.28+), `win32-x64` platform tarballs

Docker images are not part of this release. CI images stay in GHCR and do not bundle Bloomberg SDK files.

### Manual npm trusted publishing retry

Use this workflow only when a stable npm release needs to be retried after the canonical `vX.Y.Z` tag flow. Do not use it for GitHub-only JS assets; use `js_github_release.yml` and a `js-vX.Y.Z` tag for that case.

Go to **GitHub Actions** > **Publish JS Packages** > **Run workflow**.

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `version` | Stable npm version/tag to publish, such as `v1.2.3`; pre-release forms are rejected for npm publishing |

**What happens automatically:**

1. Validates the stable semver version and skips non-npm pre-release tags from the `v*` trigger
2. Builds the supported native platform packages (`linux-x64`, `win32-x64`, `darwin-arm64`)
3. Installs JS package dependencies before stamping package versions so `package-lock.json` stays consistent
4. Runs a packed-install smoke test before publishing
5. Publishes missing packages in dependency order: platform packages first, then `@xbbg/core`
6. Uses npm trusted publishing/OIDC with provenance from GitHub Actions; no npm token is required for normal releases

The npm Trusted Publisher configuration must match the workflow filename exactly: `npm-publish.yml`, repository `underloam/xbbg`, and blank environment unless a matching GitHub environment is intentionally added.

## Local Development

### Build Locally

```bash
# Install the SDK into vendor/blpapi-sdk/ and let the build discover it
bash ./scripts/sdktool.sh
# Windows PowerShell: .\scripts\sdktool.ps1

# Build wheel (includes Rust extension)
uv build

# Build sdist only (no Rust compilation)
uv build --sdist
```

### Check Current Version

```bash
# Latest release tags
git tag --sort=-version:refname | head -5

# Local dev version (from setuptools_scm)
python -c "from setuptools_scm import get_version; print(get_version())"

# Installed package version
python -c "import xbbg; print(xbbg.__version__)"
```

### Check What's on PyPI

```bash
pip index versions xbbg
```

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | v1.x development (Rust-backed stable line) |
| `release/0.x` | v0.x maintenance releases (pure-Python stable line) |
| `feat/*` | New features (PRs to main) |
| `fix/*` | Bug fixes (PRs to main or release/0.x) |
| `chore/*` | Maintenance tasks |

> **Note:** When releasing from `release/0.x`, the downstream `update-readme` and `update-index` workflows will target `main` by default. Review and revert any unintended changes to `main` after a `release/0.x` release.

### After Merging PRs

1. Delete merged branches
2. Update CHANGELOG.md on main
3. Trigger release workflow when ready

## Troubleshooting

### Release Workflow Failed

1. Check workflow logs in GitHub Actions
2. Common issues:
   - Empty CHANGELOG `[Unreleased]` section (blocked by validation)
   - Version already exists on PyPI
   - Bloomberg SDK download URL changed
   - Rust compilation error

### Version Already on PyPI

PyPI rejects duplicate versions. To fix:
1. Increment pre-release number (e.g., `b3` → `b4`)
2. Or fix issues and bump patch version

### Local Build Fails

1. Ensure `BLPAPI_ROOT` points to the Bloomberg SDK directory (must contain `include/` and `lib/`)
2. Ensure Rust toolchain is installed (`rustup show`)
3. For bindgen issues, set `LIBCLANG_PATH` (see `.cargo/config.toml` comments)
4. CI uses pregenerated bindings (`BLPAPI_PREGENERATED_BINDINGS`) to skip bindgen entirely

## For AI Agents

When asked to create a release:

1. **Review pending changes**: Read `CHANGELOG.md` `[Unreleased]` section
2. **Check for uncommitted changes**: Run `git status`
3. **Determine version bump**:
   - Breaking changes → `major`
   - New features → `minor`
   - Bug fixes only → `patch`
   - Pre-release → add `alpha`/`beta`/`rc`
4. **Guide user to GitHub Actions** to trigger the `semantic_version.yml` workflow for the canonical `vX.Y.Z` release
5. **For GitHub-only JS package assets**, guide the user to `js_github_release.yml` with an explicit version; it builds the supported `@xbbg/core` JS tarballs and tags `js-vX.Y.Z`

**Do NOT manually:**
- Edit version numbers in code for Python releases (managed by `setuptools_scm` from git tags)
- Create `vX.Y.Z` git tags directly (the canonical release workflow handles this)
- Reuse `vX.Y.Z` tags for JS-only GitHub assets; use `js-vX.Y.Z` instead so the PyPI/npm publish workflows do not trigger
- Upload to PyPI manually (OIDC trusted publishing only)
- Upload npm packages manually except for emergency recovery or first-time package seeding; normal npm releases must go through `npm-publish.yml` trusted publishing on a stable `vX.Y.Z` tag
- Edit `version` in `Cargo.toml` for a release; `crates-publish.yml` stamps
  `workspace.package.version` and every internal `workspace.dependencies` entry from
  the release version
- Run `cargo publish` by hand, except for the one-time seeding of a brand-new crate
  name that has no Trusted Publisher yet
- Add a path dependency without a matching `version`; `cargo publish` rejects a bare
  `{ path = ... }`, which is what blocked crates.io releases before 1.4.7
- Use `include_str!` to reach outside a published crate's own directory; the sdist
  will not contain the file and the crate becomes unbuildable

## CHANGELOG.md Format

```markdown
## [Unreleased]

### Added
- New feature description (#PR_NUMBER)

### Changed
- Modified behavior description (#PR_NUMBER)

### Deprecated
- Feature that will be removed in future versions

### Removed
- Feature removed in this release

### Fixed
- Bug fix description (#ISSUE_NUMBER)

### Security
- Security fix description (CVE if applicable)
```

## Writing Good Release Notes

### DO:
- Write from the user's perspective ("Users can now..." not "We added...")
- Be specific about what changed and why it matters
- Link to relevant issues/PRs with `(#123)` format
- Group related changes together
- Mention breaking changes prominently
- Include migration instructions for breaking changes

### DON'T:
- Leave the `[Unreleased]` section empty
- Use vague descriptions ("Various improvements")
- Include internal implementation details users don't need
- Forget to categorize changes
- Leave placeholder text

### Example: Good Release Notes

```markdown
## [Unreleased]

### Added
- Native Arrow carrier with explicit `backend="native"`, `backend="pyarrow"` for real PyArrow tables, and a Narwhals default that preserves legacy PyArrow-backed behavior when PyArrow is installed (#173)
- Output format control with `Format` enum (long, semi_long, long_typed, long_metadata)
- `bta()` function for Bloomberg Technical Analysis (#175)
- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`

### Changed
- All API functions now accept `backend` and `format` parameters
- Internal pipeline uses xbbg native Arrow tables with explicit optional conversion backends
- **BREAKING**: Deprecated `wide` output removed; use `semi_long` or pivot `long` results explicitly

### Deprecated
- `connect()` / `disconnect()` - engine auto-initializes in v1.0
- `getBlpapiVersion()` - use `get_sdk_info()` instead

### Fixed
- Empty DataFrame handling in helper functions with LONG format output (#180)
- Memory leak in streaming subscriptions (#182)
```

### Example: Bad Release Notes

```markdown
## [Unreleased]

- Various bug fixes
- Performance improvements
- TODO: add more details
- Updated some stuff
```

## Pre-release Types

| Type | When to Use |
|------|-------------|
| **alpha** | Early testing, API may change significantly |
| **beta** | Feature complete, testing for bugs |
| **rc** | Release candidate, final testing before stable |

## Validation

The release workflow validates:

1. **Non-empty**: `[Unreleased]` must have content (workflow fails if empty)
2. **No placeholders**: Warns if TODO/FIXME/WIP/TBD detected
3. **Format check**: Warns if standard categories not found

## Pre-Release Checklist

Before triggering a release, ensure:

- [ ] `CHANGELOG.md` `[Unreleased]` section is populated with all changes
- [ ] Changes are categorized correctly (Added, Changed, Deprecated, Removed, Fixed, Security)
- [ ] No placeholder text (TODO, FIXME, WIP, TBD) remains
- [ ] Issue/PR numbers are referenced where applicable
- [ ] Breaking changes are clearly marked
