# napi-xbbg-runtime

Resolver addon (`napi_xbbg_runtime.node`) for the `@xbbg/core` Node package. It never links
Bloomberg's runtime, so Node loads it before the engine addon (`napi-xbbg`), which cannot load
until the runtime is found. It depends only on `xbbg-runtime` (the shared cache resolver), `napi`
and the platform loader (`libc` / `windows-sys`).

## Native API

- `defaultCacheRoot(): string | null` — the xbbg cache root (`XBBG_CACHE_DIR`, else the
  per-user default).
- `scanCache(cacheRoot?: string, platform?: string): { runtimes, rejected }` — every install in
  the cache, verified against its `provenance.json`. `runtimes` (best first) are
  `{ version, platform, libPath, libSha256, source, installDir }`; `rejected` are
  `{ installDir, reason }`.
- `preloadLibrary(path: string): void` — loads the library at the absolute `path`
  (`RTLD_NOW | RTLD_GLOBAL`, or `LoadLibraryExW` on Windows) and keeps it for the process
  lifetime. Throws with the operating system's error text on failure.

`js-xbbg` calls these from `src/runtime-search-path.ts` as the last rule of its search order.
