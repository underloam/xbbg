//! Shared benchmark helpers for xbbg.
//!
//! Provides reusable session setup, field name interning, and result
//! writing utilities used across all benchmark binaries.

use std::time::{Duration, Instant};

use xbbg_core::{EventType, Name, Session, SessionOptions};

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Create and start a Bloomberg session, waiting for `SessionStarted`.
///
/// Reads `BLP_HOST` (default `127.0.0.1`) and `BLP_PORT` (default `8194`)
/// from the environment.
pub fn setup_session() -> Session {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194);

    let mut opts = SessionOptions::new().expect("failed to create session options");
    opts.set_server_host(&host).expect("failed to set host");
    opts.set_server_port(port);

    let sess = Session::new(&opts).expect("failed to create session");
    sess.start_and_wait(30_000)
        .expect("failed to start session within 30 seconds");

    sess
}

/// Open a Bloomberg service and wait for `ServiceStatus`.
pub fn open_service(sess: &Session, uri: &str) {
    sess.open_service(uri)
        .unwrap_or_else(|e| panic!("failed to open service {uri}: {e}"));

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let event = sess
            .next_event(Some(1_000))
            .unwrap_or_else(|e| panic!("failed while waiting for service {uri}: {e}"));
        if event.event_type() != EventType::ServiceStatus {
            continue;
        }
        let mut messages = event.messages();
        while let Some(message) = messages.next() {
            match message.message_type().as_str() {
                "ServiceOpened" => return,
                "ServiceOpenFailure" => panic!("Bloomberg rejected service open for {uri}"),
                _ => {}
            }
        }
    }
    panic!("timed out after 30 seconds waiting for service {uri}");
}

/// Number of messages in `event`.
pub fn message_count(event: &xbbg_core::Event) -> usize {
    let mut messages = event.messages();
    let mut count = 0;
    while messages.next().is_some() {
        count += 1;
    }
    count
}

// ---------------------------------------------------------------------------
// Pre-interned field names
// ---------------------------------------------------------------------------

/// Commonly used Bloomberg field names, pre-interned for benchmarks.
pub struct FieldNames {
    pub securities: Name,
    pub fields: Name,
    pub security_data: Name,
    pub field_data: Name,
    pub security: Name,
    pub px_last: Name,
    pub px_open: Name,
    pub px_high: Name,
    pub px_low: Name,
    pub volume: Name,
    pub cur_mkt_cap: Name,
    pub eqy_weighted_avg_px: Name,
    pub px_bid: Name,
    pub px_ask: Name,
    pub last_trade: Name,
    pub last_price: Name,
    pub bid: Name,
    pub ask: Name,
}

impl FieldNames {
    pub fn new() -> Self {
        Self {
            securities: Name::get_or_intern("securities"),
            fields: Name::get_or_intern("fields"),
            security_data: Name::get_or_intern("securityData"),
            field_data: Name::get_or_intern("fieldData"),
            security: Name::get_or_intern("security"),
            px_last: Name::get_or_intern("PX_LAST"),
            px_open: Name::get_or_intern("PX_OPEN"),
            px_high: Name::get_or_intern("PX_HIGH"),
            px_low: Name::get_or_intern("PX_LOW"),
            volume: Name::get_or_intern("VOLUME"),
            cur_mkt_cap: Name::get_or_intern("CUR_MKT_CAP"),
            eqy_weighted_avg_px: Name::get_or_intern("EQY_WEIGHTED_AVG_PX"),
            px_bid: Name::get_or_intern("PX_BID"),
            px_ask: Name::get_or_intern("PX_ASK"),
            last_trade: Name::get_or_intern("LAST_TRADE"),
            last_price: Name::get_or_intern("LAST_PRICE"),
            bid: Name::get_or_intern("BID"),
            ask: Name::get_or_intern("ASK"),
        }
    }
}

impl Default for FieldNames {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Result writing
// ---------------------------------------------------------------------------

/// Write benchmark results to a JSON file.
///
/// Creates the parent directory if needed.
pub fn write_json(path: &std::path::Path, json: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create results directory");
    }
    std::fs::write(path, json).expect("failed to write results");
    println!("Results written to: {}", path.display());
}

/// Compile- and run-time build metadata stamped into benchmark result files.
#[derive(Clone, Debug)]
pub struct BuildMode {
    pub profile: &'static str,
    pub target: &'static str,
    pub host: &'static str,
    pub target_cpu: &'static str,
    pub target_features: &'static str,
    pub rustflags: &'static str,
    pub rustc_version: &'static str,
    pub opt_level: &'static str,
    pub allocator: &'static str,
    pub debug_build: bool,
}

/// Return compiler-produced metadata without inferring build options at runtime.
pub fn build_mode() -> BuildMode {
    BuildMode {
        profile: option_env!("XBBG_BUILD_PROFILE").unwrap_or("unknown"),
        target: option_env!("XBBG_BUILD_TARGET").unwrap_or("unknown"),
        host: option_env!("XBBG_BUILD_HOST").unwrap_or("unknown"),
        target_cpu: option_env!("XBBG_BUILD_TARGET_CPU").unwrap_or("unknown"),
        target_features: option_env!("XBBG_BUILD_TARGET_FEATURES").unwrap_or("unknown"),
        rustflags: option_env!("XBBG_BUILD_RUSTFLAGS").unwrap_or("unknown"),
        rustc_version: option_env!("XBBG_BUILD_RUSTC_VERSION").unwrap_or("unknown"),
        opt_level: option_env!("XBBG_BUILD_OPT_LEVEL").unwrap_or("unknown"),
        allocator: option_env!("XBBG_BUILD_ALLOCATOR").unwrap_or("unknown"),
        debug_build: cfg!(debug_assertions),
    }
}

/// Render shared benchmark provenance as a JSON object.
///
/// `input_descriptor` should contain the complete stable fixture/config identity;
/// the helper records it together with an FNV-1a checksum. Artifact checksums use
/// the same explicitly-labelled non-cryptographic algorithm without adding a
/// benchmark-only hashing dependency.
pub fn benchmark_provenance_json(input_descriptor: &str) -> String {
    let build = build_mode();
    let executable = std::env::current_exe().ok();
    let artifact_path = executable
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let artifact_size = executable
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len().to_string())
        .unwrap_or_else(|| "null".to_string());
    let artifact_checksum = executable
        .as_ref()
        .and_then(|path| fnv1a64_file(path).ok())
        .map(|checksum| format!("{checksum:016x}"))
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        concat!(
            "{{",
            "\"benchmark_crate\":\"{}\",",
            "\"benchmark_crate_version\":\"{}\",",
            "\"profile\":\"{}\",",
            "\"debug_build\":{},",
            "\"target\":\"{}\",",
            "\"host\":\"{}\",",
            "\"target_cpu\":\"{}\",",
            "\"target_features\":\"{}\",",
            "\"rustflags\":\"{}\",",
            "\"rustc_version\":\"{}\",",
            "\"opt_level\":\"{}\",",
            "\"git_commit\":\"{}\",",
            "\"allocator\":\"{}\",",
            "\"artifact_path\":\"{}\",",
            "\"artifact_size_bytes\":{},",
            "\"artifact_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{}\"}},",
            "\"cargo_lock_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{:016x}\"}},",
            "\"sdk_version\":\"{}\",",
            "\"sdk_root\":\"{}\",",
            "\"rust_log\":\"{}\",",
            "\"input_descriptor\":\"{}\",",
            "\"input_checksum\":{{\"algorithm\":\"fnv1a64\",\"value\":\"{:016x}\"}}",
            "}}"
        ),
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        json_escape(build.profile),
        build.debug_build,
        json_escape(build.target),
        json_escape(build.host),
        json_escape(build.target_cpu),
        json_escape(build.target_features),
        json_escape(build.rustflags),
        json_escape(build.rustc_version),
        json_escape(build.opt_level),
        json_escape(option_env!("XBBG_BUILD_GIT_COMMIT").unwrap_or("unknown")),
        json_escape(build.allocator),
        json_escape(&artifact_path),
        artifact_size,
        artifact_checksum,
        fnv1a64(include_bytes!("../../../Cargo.lock")),
        json_escape(&runtime_env_or_unknown("BLPAPI_VERSION")),
        json_escape(&runtime_env_or_unknown("BLPAPI_ROOT")),
        json_escape(&runtime_env_or_unknown("RUST_LOG")),
        json_escape(input_descriptor),
        fnv1a64(input_descriptor.as_bytes()),
    )
}

fn runtime_env_or_unknown(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn fnv1a64_file(path: &std::path::Path) -> std::io::Result<u64> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path)?;
    let mut checksum = 0xcbf29ce484222325_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            return Ok(checksum);
        }
        for byte in &buffer[..count] {
            checksum ^= u64::from(*byte);
            checksum = checksum.wrapping_mul(0x100000001b3);
        }
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |checksum, byte| {
        (checksum ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                write!(&mut escaped, "\\u{:04x}", character as u32)
                    .expect("writing to String should not fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

/// Parse iteration count from env var, with default.
pub fn env_iterations(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
