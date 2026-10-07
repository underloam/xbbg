# xbbg Benchmark Results

**Version:** 1.2.6
**Generated:** 2026-06-11 20:25:50

Historical observations only: unsupported renamed-package rows and derived
comparison claims have been removed. Remaining values were not remeasured or
equivalence-checked and predate the current measurement schema. See [README.md](README.md).

---

## BDP - Reference Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 171.02 | 195.46 | 31.33 | 45.59 | (1, 3) |
| pdblp | 0.39 | 0.39 | 0.01 | 0.00 | (1,) |
| xbbg-rust | 424.81 | 389.38 | 18.49 | 0.03 | (9, 3) |
| pdblp | 0.40 | 0.38 | 0.01 | 0.00 | (1,) |

---

## BDH - Historical Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 264.73 | 254.74 | 37.69 | 0.05 | (3, 4) |
| pdblp | 0.40 | 0.39 | 0.01 | 0.00 | (1,) |
| xbbg-rust | 544.14 | 654.44 | 93.79 | 0.05 | (27, 4) |
| pdblp | 0.41 | 0.38 | 0.01 | 0.00 | (1,) |

---

## BDIB - Intraday Bars

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| pdblp | 0.40 | 0.42 | 0.07 | 0.00 | (1,) |

---

## BDTICK - Tick Data

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| pdblp | 0.41 | 0.38 | 0.01 | 0.00 | (1,) |
| pdblp | 0.39 | 0.39 | 0.01 | 0.00 | (1,) |
| pdblp | 0.52 | 0.39 | 0.01 | 0.00 | (1,) |

---

## BQL - Query Language

| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |
|---------|-----------------|----------------|---------------|-------------|-------|
| xbbg-rust | 195.66 | 260.12 | 163.64 | 0.03 | (1, 4) |

---

## Summary

**Recorded warm totals (different operation sets; not comparable):**

- xbbg (Rust): 1754.13ms
- pdblp: 3.12ms

