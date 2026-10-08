//! Offline Arrow builder append/finalize benchmarks.
//!
//! Pure Rust only: no Bloomberg session, network, or production hot-path changes.
//!
//! Run:
//!   ARROW_BENCH_ROWS=100000 ARROW_BENCH_ITERATIONS=5 \
//!     cargo bench --package xbbg-bench --bench arrow_builder_append

use std::collections::HashMap;
use std::fmt::Write as _;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use xbbg_async::engine::state::typed_builder::{ArrowType, TypedBuilder};
use xbbg_core::Value;

const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_WARMUP: usize = 2;

struct ColumnSet {
    fields: Vec<String>,
    indices: HashMap<String, usize>,
    builders: Vec<TypedBuilder>,
    present: Vec<bool>,
    rows: usize,
}

impl ColumnSet {
    fn with_type_hints<I>(hints: I) -> Self
    where
        I: IntoIterator<Item = (String, ArrowType)>,
    {
        let mut fields = Vec::new();
        let mut indices = HashMap::new();
        let mut builders = Vec::new();

        for (field, arrow_type) in hints {
            if indices.contains_key(&field) {
                continue;
            }
            let idx = fields.len();
            indices.insert(field.clone(), idx);
            fields.push(field);
            builders.push(TypedBuilder::new(arrow_type));
        }

        let present = vec![false; fields.len()];
        Self {
            fields,
            indices,
            builders,
            present,
            rows: 0,
        }
    }

    fn append(&mut self, field: &str, value: Value<'_>) {
        let idx = *self
            .indices
            .get(field)
            .expect("benchmark field should have a type hint");
        self.builders[idx].append_value(Some(value));
        self.present[idx] = true;
    }

    fn end_row(&mut self) {
        for (idx, builder) in self.builders.iter_mut().enumerate() {
            if !self.present[idx] {
                builder.append_null();
            }
        }
        self.present.fill(false);
        self.rows += 1;
    }

    fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = Vec::with_capacity(self.fields.len());
        let mut arrays = Vec::with_capacity(self.fields.len());

        for (name, builder) in self.fields.iter().zip(self.builders.iter_mut()) {
            fields.push(Field::new(name.as_str(), builder.data_type(), true));
            arrays.push(builder.finish());
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
    }

    fn finish_with_order(mut self, order: &[&str]) -> Result<RecordBatch, ArrowError> {
        let mut fields = Vec::with_capacity(order.len());
        let mut arrays = Vec::with_capacity(order.len());

        for &name in order {
            let idx = *self
                .indices
                .get(name)
                .expect("benchmark output order should reference an existing field");
            fields.push(Field::new(name, self.builders[idx].data_type(), true));
            arrays.push(self.builders[idx].finish());
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
    }
}

#[derive(Clone, Debug)]
struct BenchResult {
    name: &'static str,
    rows: usize,
    columns: usize,
    iterations: usize,
    values_per_iteration: usize,
    warmup_iterations: usize,
    min_ns: u128,
    mean_ns: u128,
    p50_ns: u128,
    rows_per_second: f64,
    values_per_second: f64,
}

fn main() {
    let rows = env_usize("ARROW_BENCH_ROWS", DEFAULT_ROWS);
    let iterations = env_usize("ARROW_BENCH_ITERATIONS", DEFAULT_ITERATIONS);
    let warmup_iterations = env_usize("BENCH_WARMUP", DEFAULT_WARMUP);

    let dense_strings: Vec<String> = (0..1024).map(|idx| format!("SECURITY_{idx:04}")).collect();
    let long_strings: Vec<String> = (0..10_000)
        .map(|idx| {
            let prefix = format!("SECURITY_{idx:08}_");
            format!("{prefix}{}", "X".repeat(256 - prefix.len()))
        })
        .collect();
    let mixed_hints = vec![
        ("ticker".to_string(), ArrowType::String),
        ("px_last".to_string(), ArrowType::Float64),
        ("volume".to_string(), ArrowType::Int64),
        ("is_active".to_string(), ArrowType::Bool),
        ("trade_date".to_string(), ArrowType::Date32),
    ];
    let wide_names: Vec<String> = (0..100).map(|col| format!("px_{col:03}")).collect();
    let wide_hints: Vec<(String, ArrowType)> = wide_names
        .iter()
        .cloned()
        .map(|name| (name, ArrowType::Float64))
        .collect();
    let late_hints = vec![
        ("px_last".to_string(), ArrowType::Float64),
        ("late_string".to_string(), ArrowType::String),
    ];
    let finalization_field_names: Vec<String> = (0..5).map(|col| format!("value_{col}")).collect();

    let results = vec![
        run_scenario(
            "dense_float64",
            rows,
            1,
            iterations,
            warmup_iterations,
            || bench_dense_float64(rows),
        ),
        run_scenario(
            "sparse_float64_null",
            rows,
            1,
            iterations,
            warmup_iterations,
            || bench_sparse_float64_null(rows),
        ),
        run_scenario(
            "clustered_50pct_float64_null",
            rows,
            1,
            iterations,
            warmup_iterations,
            || bench_clustered_float64_null(rows),
        ),
        run_scenario(
            "dense_string",
            rows,
            1,
            iterations,
            warmup_iterations,
            || bench_dense_string(rows, &dense_strings),
        ),
        run_scenario(
            "long_high_cardinality_string_256b_10k",
            rows,
            1,
            iterations,
            warmup_iterations,
            || bench_dense_string(rows, &long_strings),
        ),
        run_scenario(
            "mixed_5_column_rows",
            rows,
            5,
            iterations,
            warmup_iterations,
            || bench_mixed_5_column_rows(rows, &mixed_hints),
        ),
        run_scenario(
            "wide_100_column_rows",
            rows,
            100,
            iterations,
            warmup_iterations,
            || bench_wide_100_column_rows(rows, &wide_hints, &wide_names),
        ),
        run_scenario(
            "late_column_null_backfill",
            rows,
            2,
            iterations,
            warmup_iterations,
            || bench_late_column_null_backfill(rows, &late_hints),
        ),
        run_scenario(
            "record_batch_finalization",
            rows,
            5,
            iterations,
            warmup_iterations,
            || bench_record_batch_finalization(rows, &finalization_field_names),
        ),
    ];

    print_table(&results);
    write_results(&results, rows, iterations, warmup_iterations);
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn run_scenario<F>(
    name: &'static str,
    rows: usize,
    columns: usize,
    iterations: usize,
    warmup_iterations: usize,
    mut scenario: F,
) -> BenchResult
where
    F: FnMut() -> usize,
{
    for _ in 0..warmup_iterations {
        black_box(scenario());
    }

    let mut timings = Vec::with_capacity(iterations);
    let mut values_per_iteration = 0;

    for _ in 0..iterations {
        let started = Instant::now();
        values_per_iteration = scenario();
        timings.push(started.elapsed());
    }

    let min = timings.iter().copied().min().unwrap_or(Duration::ZERO);
    let total_ns: u128 = timings.iter().map(Duration::as_nanos).sum();
    let mean_ns = total_ns / iterations as u128;
    let p50_ns = percentile_ns(&timings, 50.0);
    let mean_secs = mean_ns as f64 / 1_000_000_000.0;

    BenchResult {
        name,
        rows,
        columns,
        iterations,
        warmup_iterations,
        values_per_iteration,
        min_ns: min.as_nanos(),
        mean_ns,
        p50_ns,
        rows_per_second: rows as f64 / mean_secs,
        values_per_second: values_per_iteration as f64 / mean_secs,
    }
}

fn bench_dense_float64(rows: usize) -> usize {
    let mut builder = TypedBuilder::new(ArrowType::Float64);
    for row in 0..rows {
        builder.append_value(Some(Value::Float64(row as f64 * 0.25)));
    }

    let array = builder.finish();
    black_box(array.len());
    rows
}

fn bench_sparse_float64_null(rows: usize) -> usize {
    let mut builder = TypedBuilder::new(ArrowType::Float64);
    for row in 0..rows {
        if row % 10 == 0 {
            builder.append_null();
        } else {
            builder.append_value(Some(Value::Float64(row as f64 * 0.5)));
        }
    }

    let array = builder.finish();
    black_box(array.null_count());
    rows
}

fn bench_clustered_float64_null(rows: usize) -> usize {
    let mut builder = TypedBuilder::new(ArrowType::Float64);
    for row in 0..rows {
        if row < rows / 2 {
            builder.append_null();
        } else {
            builder.append_value(Some(Value::Float64(row as f64 * 0.5)));
        }
    }
    let array = builder.finish();
    black_box(array.null_count());
    rows
}

fn bench_dense_string(rows: usize, values: &[String]) -> usize {
    let mut builder = TypedBuilder::new(ArrowType::String);

    for row in 0..rows {
        builder.append_value(Some(Value::String(values[row % values.len()].as_str())));
    }

    let array = builder.finish();
    black_box(array.len());
    rows
}

fn bench_mixed_5_column_rows(rows: usize, hints: &[(String, ArrowType)]) -> usize {
    let mut cols = ColumnSet::with_type_hints(hints.iter().cloned());

    for row in 0..rows {
        cols.append("ticker", Value::String("AAPL US Equity"));
        cols.append("px_last", Value::Float64(150.0 + row as f64 * 0.01));
        cols.append("volume", Value::Int64(1_000_000 + row as i64));
        cols.append("is_active", Value::Bool(row % 2 == 0));
        cols.append("trade_date", Value::Date32(19_000 + (row % 250) as i32));
        cols.end_row();
    }

    let batch = cols.finish().expect("mixed rows should build RecordBatch");
    black_box(batch.num_columns());
    rows * 5
}

fn bench_wide_100_column_rows(
    rows: usize,
    hints: &[(String, ArrowType)],
    names: &[String],
) -> usize {
    let mut cols = ColumnSet::with_type_hints(hints.iter().cloned());

    for row in 0..rows {
        for (col, name) in names.iter().enumerate() {
            cols.append(name, Value::Float64(row as f64 + col as f64));
        }
        cols.end_row();
    }

    let batch = cols.finish().expect("wide rows should build RecordBatch");
    black_box(batch.num_columns());
    rows * 100
}

fn bench_late_column_null_backfill(rows: usize, hints: &[(String, ArrowType)]) -> usize {
    let mut cols = ColumnSet::with_type_hints(hints.iter().cloned());
    let late_at = rows / 2;

    for row in 0..rows {
        cols.append("px_last", Value::Float64(row as f64));
        if row >= late_at {
            cols.append("late_string", Value::String("late"));
        }
        cols.end_row();
    }

    let batch = cols
        .finish_with_order(&["px_last", "late_string"])
        .expect("late column should backfill nulls and build RecordBatch");
    black_box(batch.column(1).null_count());
    rows * 2
}

fn bench_record_batch_finalization(rows: usize, field_names: &[String]) -> usize {
    let mut fields = Vec::with_capacity(5);
    let mut arrays = Vec::with_capacity(5);

    for (col, name) in field_names.iter().enumerate() {
        let mut builder = TypedBuilder::new(ArrowType::Float64);
        for row in 0..rows {
            builder.append_value(Some(Value::Float64(row as f64 + col as f64)));
        }
        fields.push(Field::new(name.clone(), DataType::Float64, true));
        arrays.push(builder.finish());
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays).expect("arrays should share row count");
    black_box(batch.num_rows());
    rows * 5
}

fn print_table(results: &[BenchResult]) {
    println!(
        "{:<30} {:>10} {:>8} {:>12} {:>12} {:>12} {:>14} {:>14}",
        "scenario", "rows", "cols", "mean_ms", "min_ms", "p50_ms", "rows/s", "values/s"
    );
    println!("{}", "-".repeat(124));

    for result in results {
        println!(
            "{:<30} {:>10} {:>8} {:>12.3} {:>12.3} {:>12.3} {:>14.0} {:>14.0}",
            result.name,
            result.rows,
            result.columns,
            nanos_to_millis(result.mean_ns),
            nanos_to_millis(result.min_ns),
            nanos_to_millis(result.p50_ns),
            result.rows_per_second,
            result.values_per_second,
        );
    }
}

fn nanos_to_millis(ns: u128) -> f64 {
    ns as f64 / 1_000_000.0
}

fn percentile_ns(timings: &[Duration], percentile: f64) -> u128 {
    let mut values: Vec<u128> = timings.iter().map(Duration::as_nanos).collect();
    values.sort_unstable();
    let idx = (((values.len() - 1) as f64) * percentile / 100.0).round() as usize;
    values[idx]
}

fn write_results(
    results: &[BenchResult],
    rows: usize,
    iterations: usize,
    warmup_iterations: usize,
) {
    let timestamp = unix_timestamp();
    let json = results_json(results, rows, iterations, warmup_iterations, timestamp);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    let timestamped = dir.join(format!("arrow_builder_append_{timestamp}.json"));
    let latest = dir.join("arrow_builder_append_latest.json");

    xbbg_bench::write_json(&timestamped, &json);
    xbbg_bench::write_json(&latest, &json);
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_secs()
}

fn results_json(
    results: &[BenchResult],
    rows: usize,
    iterations: usize,
    warmup_iterations: usize,
    timestamp: u64,
) -> String {
    let input_descriptor = format!(
        "rows={rows};iterations={iterations};warmup={warmup_iterations};scenarios={}",
        results.len()
    );
    let provenance = xbbg_bench::benchmark_provenance_json(&input_descriptor);
    let mut json = String::new();
    writeln!(&mut json, "{{").unwrap();
    writeln!(&mut json, "  \"benchmark\": \"arrow_builder_append\",").unwrap();
    writeln!(&mut json, "  \"schema_version\": 2,").unwrap();
    writeln!(&mut json, "  \"offline\": true,").unwrap();
    writeln!(
        &mut json,
        "  \"coverage\": \"synthetic TypedBuilder and RecordBatch paths; no Bloomberg SDK or network\","
    )
    .unwrap();
    writeln!(&mut json, "  \"timestamp_unix\": {timestamp},").unwrap();
    writeln!(&mut json, "  \"rows\": {rows},").unwrap();
    writeln!(&mut json, "  \"iterations\": {iterations},").unwrap();
    writeln!(&mut json, "  \"warmup_iterations\": {warmup_iterations},").unwrap();
    writeln!(
        &mut json,
        "  \"timing_scope\": \"warm uninstrumented append plus finalization; fixture strings allocated before timing\","
    )
    .unwrap();
    writeln!(
        &mut json,
        "  \"percentile_policy\": \"p50 reported from {} samples; p95 requires at least 20 and p99 at least 100\",",
        iterations
    )
    .unwrap();
    writeln!(&mut json, "  \"provenance\": {provenance},").unwrap();
    writeln!(&mut json, "  \"results\": [").unwrap();

    for (idx, result) in results.iter().enumerate() {
        let comma = if idx + 1 == results.len() { "" } else { "," };
        writeln!(&mut json, "    {{").unwrap();
        writeln!(
            &mut json,
            "      \"name\": \"{}\",",
            json_escape(result.name)
        )
        .unwrap();
        writeln!(&mut json, "      \"rows\": {},", result.rows).unwrap();
        writeln!(&mut json, "      \"columns\": {},", result.columns).unwrap();
        writeln!(&mut json, "      \"iterations\": {},", result.iterations).unwrap();
        writeln!(
            &mut json,
            "      \"warmup_iterations\": {},",
            result.warmup_iterations
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"values_per_iteration\": {},",
            result.values_per_iteration
        )
        .unwrap();
        writeln!(&mut json, "      \"min_ns\": {},", result.min_ns).unwrap();
        writeln!(&mut json, "      \"mean_ns\": {},", result.mean_ns).unwrap();
        writeln!(&mut json, "      \"p50_ns\": {},", result.p50_ns).unwrap();
        writeln!(
            &mut json,
            "      \"rows_per_second\": {:.3},",
            result.rows_per_second
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"values_per_second\": {:.3}",
            result.values_per_second
        )
        .unwrap();
        writeln!(&mut json, "    }}{comma}").unwrap();
    }

    writeln!(&mut json, "  ]").unwrap();
    writeln!(&mut json, "}}").unwrap();
    json
}

fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}
