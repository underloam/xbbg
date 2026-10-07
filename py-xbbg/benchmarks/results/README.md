# Benchmark Results

This directory stores reports produced by the benchmark entry points. See
[../README.md](../README.md) for installed-package comparisons, dependencies, and
measurement boundaries.

## File Naming

| Files | Purpose |
|-------|---------|
| `benchmark_v{version}.json` / `.md` | Most recent standard-suite run for that installed xbbg version; overwritten on a new run of the same version. |
| `benchmark_v{version}_{YYYYMMDD_HHMMSS}.json` / `.md` | Timestamped standard-suite snapshot. |
| `latest.json` / `latest.md` | Local symlink or copy of the latest standard-suite report; not committed. |
| `handoff_offline_{YYYYMMDD_HHMMSS}.json` | Offline native Arrow handoff observations. |
| `handoff_offline_latest.json` | Most recent local offline handoff report. |
| `latest_competitor_equivalence_{YYYYMMDD_HHMMSS}.json` / `.md` | Separate installed-competitor comparison, with normalized result-equivalence checks. |

The current installation is the only xbbg lane. Compare different xbbg versions
by running the same benchmark inputs separately with each actual installed build,
not by importing a renamed package from the current environment.

## Running and Saving Reports

From the repository root, in the environment containing the xbbg build under
measurement:

```bash
# Requires authorized Bloomberg access
python py-xbbg/benchmarks/run_all.py

# No live requests
python py-xbbg/benchmarks/bench_handoff_offline.py --quick
```

Before retaining or sharing a generated report:

1. Check that the requested operations returned usable data, and inspect
   equivalence results when comparing packages.
2. Record the actual package versions and request inputs.
3. Remove local absolute paths and identifying environment details from provenance.
4. Keep the JSON and Markdown report together. Do not replace missing
   measurements with example timings.

Live request benchmarks run locally, not in offline CI.

## Historical Reports

The checked-in June 2026 reports predate the current measurement contract.
Unsupported renamed-package rows and their derived comparisons have been removed;
the other recorded observations are preserved, not remeasured.

These reports do not establish comparable work between packages: some result
shapes differ, result equivalence was not recorded, and their older timing and
memory labels do not describe the current measurement boundaries. They are
historical records, not evidence for current speedup or memory-reduction claims.

## Interpreting Current Reports

- **Fresh-process first result** includes process spawn, imports, session setup,
  request, result construction, and the flushed result marker. One sample is
  reported; teardown and child exit are excluded.
- **Warm-session timing** measures uninstrumented calls after discarded warmup.
  Compare matching operations, inputs, lifecycle scopes, and sample counts.
- **p95 / p99** are `null` below 20 / 100 warm observations respectively.
- **CPython tracemalloc peak** is measured in a separate untimed call and excludes
  native allocations and process RSS. It is not total memory consumption.
- **Shape** describes dimensions only. A fast result with different values or
  missing rows is not a performance win.

The standalone equivalence and offline handoff reports label their own consumer
and lifecycle scopes. Do not compare unlike measurement scopes or sum different
sets of operations into an overall speedup.
