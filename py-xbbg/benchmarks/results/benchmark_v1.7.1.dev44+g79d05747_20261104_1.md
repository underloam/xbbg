# xbbg Benchmark Results

**Version:** 1.7.1.dev44+g79d05747
**Date (UTC):** 2026-11-04

## BDP - Reference Data

| Package | Fresh-process first result (ms, n=1) | Warm-session mean (ms) | Warm max (ms) | Warm n | CPython tracemalloc peak (MB, separate run) | Shape |
|---|---:|---:|---:|---:|---:|---|
| xbbg-rust | 699.80 | 217.08 | 305.90 | 5 | 0.02 | (1, 3) |
| xbbg-rust | 866.16 | 472.04 | 542.53 | 5 | 0.02 | (9, 3) |

## BDH - Historical Data

| Package | Fresh-process first result (ms, n=1) | Warm-session mean (ms) | Warm max (ms) | Warm n | CPython tracemalloc peak (MB, separate run) | Shape |
|---|---:|---:|---:|---:|---:|---|
| xbbg-rust | 1058.34 | 296.95 | 376.24 | 5 | 0.02 | (3, 4) |
| xbbg-rust | 1047.01 | 569.61 | 630.15 | 5 | 0.02 | (27, 4) |

## BDIB - Intraday Bars

| Package | Fresh-process first result (ms, n=1) | Warm-session mean (ms) | Warm max (ms) | Warm n | CPython tracemalloc peak (MB, separate run) | Shape |
|---|---:|---:|---:|---:|---:|---|
| xbbg-rust | 853.38 | 406.22 | 504.22 | 5 | 0.02 | (6, 9) |

## BDTICK - Tick Data

| Package | Fresh-process first result (ms, n=1) | Warm-session mean (ms) | Warm max (ms) | Warm n | CPython tracemalloc peak (MB, separate run) | Shape |
|---|---:|---:|---:|---:|---:|---|
| xbbg-rust | 1043.21 | 1033.08 | 2236.36 | 5 | 0.02 | (269, 5) |
| xbbg-rust | 1054.70 | 994.32 | 1219.76 | 5 | 0.02 | (1073, 5) |
| xbbg-rust | 1031.44 | 617.14 | 701.61 | 5 | 0.02 | (1342, 5) |

## BQL - Query Language

| Package | Fresh-process first result (ms, n=1) | Warm-session mean (ms) | Warm max (ms) | Warm n | CPython tracemalloc peak (MB, separate run) | Shape |
|---|---:|---:|---:|---:|---:|---|
| xbbg-rust | 729.27 | 186.82 | 236.18 | 5 | 0.02 | (1, 4) |
| xbbg-rust | 664.18 | 229.37 | 362.06 | 5 | 0.02 | (2, 5) |

## Binding Handoff - Offline

| Scenario | Case | Consumer boundary | Median (ms) | Max (ms) | n | CPython tracemalloc peak (MB, separate run) |
|---|---|---|---:|---:|---:|---:|
| native_to_pyarrow_c_stream | small_dense | boundary_only_no_cell_reads | 0.0081 | 0.0154 | 30 | 0.00 |
| native_to_pyarrow_checked_cells | small_dense | inclusive_all_cells_to_python_objects | 0.3362 | 0.4076 | 30 | 0.02 |
| native_to_pandas_xbbg_backend | small_dense | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 0.1216 | 0.1906 | 30 | 0.01 |
| native_to_polars | small_dense | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 0.0127 | 0.0212 | 30 | 0.00 |
| native_to_pylist_checked_cells | small_dense | inclusive_native_all_cell_materialization | 0.0656 | 0.0768 | 30 | 0.02 |
| native_to_pyarrow_c_stream | chunked_nullable | boundary_only_no_cell_reads | 0.6841 | 1.4544 | 30 | 0.00 |
| native_to_pyarrow_checked_cells | chunked_nullable | inclusive_all_cells_to_python_objects | 25.2655 | 31.7673 | 30 | 0.05 |
| native_to_pandas_xbbg_backend | chunked_nullable | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 11.8758 | 40.9694 | 30 | 0.64 |
| native_to_polars | chunked_nullable | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 0.9203 | 1.0900 | 30 | 0.01 |
| native_to_pylist_checked_cells | chunked_nullable | inclusive_native_all_cell_materialization | 4.6291 | 5.3322 | 30 | 2.58 |
| native_to_pyarrow_c_stream | wide_string_heavy | boundary_only_no_cell_reads | 0.2280 | 0.3212 | 30 | 0.00 |
| native_to_pyarrow_checked_cells | wide_string_heavy | inclusive_all_cells_to_python_objects | 13.3033 | 14.3314 | 30 | 0.29 |
| native_to_pandas_xbbg_backend | wide_string_heavy | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 2.8817 | 4.3789 | 30 | 0.22 |
| native_to_polars | wide_string_heavy | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 0.2717 | 0.5232 | 30 | 0.00 |
| native_to_pylist_checked_cells | wide_string_heavy | inclusive_native_all_cell_materialization | 2.7588 | 3.2127 | 30 | 2.01 |
| native_to_pyarrow_c_stream | large_mixed | boundary_only_no_cell_reads | 1.2864 | 3.4610 | 30 | 0.01 |
| native_to_pyarrow_checked_cells | large_mixed | inclusive_all_cells_to_python_objects | 898.7819 | 3640.9155 | 30 | 0.75 |
| native_to_pandas_xbbg_backend | large_mixed | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 23.1640 | 53.5161 | 30 | 1.16 |
| native_to_polars | large_mixed | boundary_and_backend_conversion_without_additional_consumer_cell_reads | 2.1700 | 3.1297 | 30 | 0.02 |
| native_to_pylist_checked_cells | large_mixed | inclusive_native_all_cell_materialization | 177.8277 | 229.2062 | 30 | 62.79 |

