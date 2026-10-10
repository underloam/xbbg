# xbbg Benchmarks

Benchmark the installed Rust-backed xbbg build and real Bloomberg client packages.
The standard runner and the separate competitor-equivalence script cover different
comparisons; installing a package does not add it to every benchmark.

## Bloomberg Data Usage

All request benchmarks require an authorized Bloomberg Terminal or B-PIPE
connection and the relevant entitlements. Only `bench_handoff_offline.py` is
offline.

With the defaults in `config.py`, each successful package/scenario pair makes one
fresh-process call, one discarded warmup call, five measured warm calls, and one
untimed allocation-measurement call. Helpers may make additional metadata requests.
Returned tick counts vary with market activity; the configured time windows are
not a fixed data-point allowance.

Run locally, keep request volume bounded, and follow Bloomberg usage terms and
your internal policies.

## Comparison Setup

| Entry point | Packages and coverage |
|-------------|-----------------------|
| `run_all.py` | Current xbbg and installed `pdblp` for BDP, BDH, and BDIB. BDTICK tries `pdblp` only if its installed version exposes that method. BQL measures current xbbg only. Includes offline handoff measurements by default. |
| `bench_latest_competitors_equivalence.py` | Current xbbg against installed `pdblp`, `bbg-fetch`, and `polars-bloomberg`, for the operations each adapter supports. Normalizes results and records equivalence or mismatch. |
| `harness.py` | Separate BDP, BDH, and BQL comparison with `polars-bloomberg`; reports same-process first calls and warm timings. |
| `bench_raw_blpapi.py` | Standalone SDK BDP phase timing; not a lane in `run_all.py`. |
| `bench_handoff_offline.py` | Native Arrow handoff into supported Python consumers using synthetic fixture data; no live requests. |

The standard runner labels the current installation `xbbg-rust`; the equivalence
script labels it `xbbg-latest`. Both import the real `xbbg` package. There is no
renamed second installation of xbbg in either comparison.

Missing competitor packages and unsupported or empty responses do not produce
successful measurement rows. The standard runner checks result availability and
shape, not value equivalence; use the equivalence script before interpreting a
cross-package timing difference.

## Quick Start

Use the Python environment containing the current xbbg build you want to measure.
Install optional consumers and competitors into that same environment without
replacing xbbg:

```bash
uv pip install "pandas>=2.2.2,<4" "pyarrow>=22.0.0" "polars[timezone]>=0.20.4"
uv pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
uv pip install pdblp bbg-fetch polars-bloomberg
```

From the repository root:

```bash
# Standard live suite plus offline handoff
python py-xbbg/benchmarks/run_all.py

# Standard live suite only
python py-xbbg/benchmarks/run_all.py --no-offline

# Individual request benchmark
python py-xbbg/benchmarks/bench_bdp.py
python py-xbbg/benchmarks/bench_bdh.py
python py-xbbg/benchmarks/bench_bdib.py
python py-xbbg/benchmarks/bench_bdtick.py
python py-xbbg/benchmarks/bench_bql.py

# Separate comparisons
python py-xbbg/benchmarks/bench_latest_competitors_equivalence.py
python py-xbbg/benchmarks/harness.py
python py-xbbg/benchmarks/bench_raw_blpapi.py

# No Bloomberg connection needed
python py-xbbg/benchmarks/bench_handoff_offline.py --quick
```

## Configuration

Edit `config.py` for the standard runner's request inputs and sample counts:

```python
TICKERS_SINGLE = ["IBM US Equity"]
TICKERS_MULTI = ["IBM US Equity", "AAPL US Equity", "MSFT US Equity"]
FIELDS_SINGLE = ["PX_LAST"]
FIELDS_MULTI = ["PX_LAST", "VOLUME", "TRADING_DT_REALTIME"]
BDH_START = "2025-01-02"
BDH_END = "2025-01-06"
ITERATIONS = 5
WARMUP_ITERATIONS = 1
```

Intraday dates default to the previous weekday, not an exchange-holiday calendar.
Intraday request windows are New York wall times, converted to UTC for clients
that expect UTC. Adjust the date if the selected market was closed.

`PACKAGES` documents the standard runner's current-xbbg and `pdblp` imports.
Entry points select their own lanes; its `enabled` values are not runtime filters.
The separate equivalence script has its own iteration settings.

## Reports and Measurement Boundaries

The standard runner writes to `py-xbbg/benchmarks/results/`:

```text
benchmark_v{version}.json
benchmark_v{version}.md
benchmark_v{version}_{YYYYMMDD_HHMMSS}.json
benchmark_v{version}_{YYYYMMDD_HHMMSS}.md
latest.json
latest.md
```

Version files are overwritten on another run of the same version; timestamped
files preserve each run. `latest.*` are local symlinks or copies. Offline handoff
and competitor-equivalence scripts also write their own timestamped reports.

| Standard-runner metric | Meaning |
|------------------------|---------|
| Fresh-process first result | One parent-observed process spawn, imports, session setup, request, result construction, and result marker; excludes teardown and child exit. |
| Warm-session timing | Uninstrumented calls after discarded warmup, using reused sessions. Reports mean, median, standard deviation, maximum, and sample count. |
| Warm p95 / p99 | Omitted as `null` below 20 / 100 warm samples respectively. Five samples cannot support these tails. |
| CPython tracemalloc peak | Separate untimed call; excludes Rust allocations, Arrow native pools, allocator arenas, and process RSS. |
| Shape | Result dimensions, not proof of equivalent values or work. |

No sample timings or speedup claims are supplied here. Compare actual reports
only when request inputs, package versions, measurement boundaries, and returned
values are comparable. Historical reports may predate the current measurement
schema; see [results/README.md](results/README.md).

Live benchmarks are local-only; they are not an offline CI performance gate.
Review generated provenance before sharing reports and remove local paths or
other identifying environment details.
