//! Privacy boundary for the new delivery reports only. Keep this allowlist in
//! sync with the inline copy in the independently portable default benchmark.
//! Raw shared-helper provenance must never be serialized into these reports.

use serde_json::{Value, json};

fn fingerprint(raw: &Value, field: &str) -> Value {
    let value = raw[field].as_str().unwrap_or("unknown");
    let checksum = value
        .bytes()
        .fold(0xcbf29ce484222325_u64, |checksum, byte| {
            (checksum ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    json!({ "algorithm": "fnv1a64", "value": format!("{checksum:016x}"), "known": value != "unknown" })
}

fn checksum(raw: &Value, field: &str) -> Option<Value> {
    let value = raw[field]["value"].as_str()?;
    (value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| json!({ "algorithm": "fnv1a64", "value": value }))
}

fn compiler_version(raw: &str) -> Option<&str> {
    let version = raw.strip_prefix("rustc ")?.split_whitespace().next()?;
    let (base, channel) = version
        .split_once('-')
        .map_or((version, None), |(base, channel)| (base, Some(channel)));
    let mut numbers = base.split('.');
    if !(0..3).all(|_| {
        numbers.next().is_some_and(|number| {
            !number.is_empty()
                && number.len() <= 10
                && number.bytes().all(|byte| byte.is_ascii_digit())
        })
    }) || numbers.next().is_some()
    {
        return None;
    }
    let valid_channel = match channel {
        None | Some("nightly" | "dev" | "beta") => true,
        Some(channel) => channel.strip_prefix("beta.").is_some_and(|number| {
            !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
        }),
    };
    valid_channel.then_some(version)
}

pub(crate) fn private_provenance(descriptor: &str) -> Value {
    let raw: Value = serde_json::from_str(&xbbg_bench::benchmark_provenance_json(descriptor))
        .expect("benchmark provenance JSON");
    let profile = raw["profile"]
        .as_str()
        .filter(|value| matches!(*value, "release" | "bench" | "dev" | "test"));
    // Custom target names can identify a workstation; retain only their digest.
    let target = raw["target"].as_str().filter(|value| {
        matches!(
            *value,
            "x86_64-pc-windows-msvc"
                | "aarch64-pc-windows-msvc"
                | "i686-pc-windows-msvc"
                | "x86_64-pc-windows-gnu"
                | "x86_64-unknown-linux-gnu"
                | "aarch64-unknown-linux-gnu"
                | "x86_64-unknown-linux-musl"
                | "aarch64-unknown-linux-musl"
                | "x86_64-apple-darwin"
                | "aarch64-apple-darwin"
        )
    });
    let opt_level = raw["opt_level"]
        .as_str()
        .filter(|value| matches!(*value, "0" | "1" | "2" | "3" | "s" | "z"));
    let git_commit = raw["git_commit"]
        .as_str()
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()));
    json!({
        "benchmark_crate": "xbbg-bench",
        "benchmark_crate_version": env!("CARGO_PKG_VERSION"),
        "profile": profile,
        "debug_build": raw["debug_build"].as_bool(),
        "target": target,
        "rustc_version": compiler_version(raw["rustc_version"].as_str().unwrap_or("")),
        "opt_level": opt_level,
        "git_commit": git_commit,
        "artifact_size_bytes": raw["artifact_size_bytes"].as_u64(),
        "artifact_checksum": checksum(&raw, "artifact_checksum"),
        "cargo_lock_checksum": checksum(&raw, "cargo_lock_checksum"),
        "input_descriptor": descriptor,
        "input_checksum": checksum(&raw, "input_checksum"),
        "configuration_fingerprints": {
            "rustc_build": fingerprint(&raw, "rustc_version"),
            "target": fingerprint(&raw, "target"),
            "target_cpu": fingerprint(&raw, "target_cpu"),
            "target_features": fingerprint(&raw, "target_features"),
            "rustflags": fingerprint(&raw, "rustflags"),
        },
        "privacy_policy": "allowlisted standard compiler version/profile/target and checksums only; configuration strings are noncryptographic comparison fingerprints, never raw flags; no executable/SDK/output paths, host identifiers, RUST_LOG or raw SDK environment values",
    })
}
