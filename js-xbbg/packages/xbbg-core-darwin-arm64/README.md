# @xbbg/core-darwin-arm64

Platform-specific prebuilt native `napi_xbbg.node` addon used by `@xbbg/core`. This package supports the `darwin arm64` target. npm installs it automatically as an optional dependency of `@xbbg/core` on matching platforms.

## Install

Install the wrapper package:

```sh
npm install @xbbg/core
```

Direct installation of `@xbbg/core-darwin-arm64` is only recommended for diagnostics or offline packaging workflows.

## Contents

- `index.js`
- `index.d.ts`
- `napi_xbbg.node`
- `LICENSE`
- `package.json` and `README.md`

## Runtime requirements

Bloomberg access and runtime requirements are inherited from `@xbbg/core`. This package does not vendor Bloomberg SDK binaries and does not provide Bloomberg access, credentials, entitlements, data rights, or SDK licenses.

## Release integrity

Package releases are intended to be built on GitHub-hosted Actions and published to npm with trusted publishing and provenance once configured.
