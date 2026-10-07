# xbbg Benchmark Results

**Version:** 1.2.8.dev3+g5165b5b6
**Generated:** 2026-06-11 20:22:25

Historical observations only: unsupported renamed-package rows and derived
comparison claims have been removed. Remaining values were not remeasured or
equivalence-checked and predate the current measurement schema. See [README.md](README.md).

---

## BDP - Reference Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 171.30 | 174.13 | 23.73 | 52.36 | (1, 3) |
| pdblp | 0.45 | 0.40 | 0.01 | 0.00 | (1,) |
| xbbg-rust | 447.47 | 491.93 | 235.79 | 0.54 | (9, 3) |
| pdblp | 0.37 | 0.35 | 0.02 | 0.00 | (1,) |

---

## BDH - Historical Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 239.36 | 244.74 | 26.46 | 0.56 | (3, 4) |
| pdblp | 0.35 | 0.35 | 0.00 | 0.00 | (1,) |
| xbbg-rust | 534.91 | 554.44 | 83.43 | 0.54 | (27, 4) |
| pdblp | 0.36 | 0.35 | 0.01 | 0.00 | (1,) |

---

## BDIB - Intraday Bars

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| pdblp | 0.69 | 0.48 | 0.06 | 0.00 | (1,) |

---

## BDTICK - Tick Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| pdblp | 0.39 | 0.53 | 0.14 | 0.00 | (1,) |
| pdblp | 0.36 | 0.34 | 0.01 | 0.00 | (1,) |
| pdblp | 0.34 | 0.35 | 0.01 | 0.00 | (1,) |

---

## BQL - Query Language

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 201.88 | 158.23 | 31.81 | 0.54 | (1, 4) |

---

## Summary

**Recorded warm totals (different operation sets; not comparable):**

- xbbg (Rust): 1623.47ms
- pdblp: 3.14ms

