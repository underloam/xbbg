//! Criterion comparison for synthetic BQL JSON parser throughput.
//!
//! Production parsing remains unchanged. The simd-json lanes explicitly separate
//! parse-only timing (mutable input copy excluded) from inclusive copy+parse timing.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::Value;
use std::hint::black_box;
use xbbg_bench::{BQL_JSON_SCENARIOS, bql_json_fixture};

fn bench_bql_json_parsers(c: &mut Criterion) {
    let mut input_descriptor = String::from("cases=");
    for (index, (scenario, _, _)) in BQL_JSON_SCENARIOS.iter().enumerate() {
        if index != 0 {
            input_descriptor.push(',');
        }
        input_descriptor.push_str(scenario);
    }
    input_descriptor.push_str(";lanes=serde_parse,simd_parse_copy_excluded,simd_copy_and_parse");
    println!(
        "XBBG_BENCH_PROVENANCE {}",
        xbbg_bench::benchmark_provenance_json(&input_descriptor)
    );

    let mut group = c.benchmark_group("bql_json_parsers");
    for (scenario, rows, fields) in BQL_JSON_SCENARIOS {
        let json = bql_json_fixture(rows, fields);
        let bytes = json.len() as u64;
        group.throughput(Throughput::Bytes(bytes));

        group.bench_with_input(
            BenchmarkId::new("serde_json_from_str", scenario),
            &json,
            |b, json| {
                b.iter(|| {
                    let parsed: Value = serde_json::from_str(black_box(json.as_str()))
                        .expect("synthetic BQL fixture should parse with serde_json");
                    black_box(parsed);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("simd_json_parse_only_copy_excluded", scenario),
            &json,
            |b, json| {
                b.iter_batched(
                    || json.as_bytes().to_vec(),
                    |mut bytes| {
                        let parsed: Value =
                            simd_json::serde::from_slice(black_box(bytes.as_mut_slice()))
                                .expect("synthetic BQL fixture should parse with simd-json");
                        black_box(parsed);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("simd_json_inclusive_copy_and_parse", scenario),
            &json,
            |b, json| {
                b.iter(|| {
                    let mut bytes = black_box(json.as_bytes()).to_vec();
                    let parsed: Value = simd_json::serde::from_slice(bytes.as_mut_slice())
                        .expect("synthetic BQL fixture should parse with simd-json");
                    black_box(parsed);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_bql_json_parsers);
criterion_main!(benches);
