//! Logging infrastructure for the xbbg workspace.
//!
//! Provides a zero-GIL tracing setup for Rust-Python hybrid libraries.
//! Python controls the log level via an [`AtomicU8`] — no GIL acquisition
//! ever happens on the logging hot path.
//!
//! # Architecture
//!
//! ```text
//! tracing::debug!("...")
//!   → AtomicLevelFilter (reads AtomicU8 ~1ns, zero GIL)
//!   → fmt::layer
//!   → stderr
//! ```
//!
//! # Usage from Python
//!
//! ```python
//! import xbbg
//! xbbg.set_log_level("debug")   # sets AtomicU8, returns immediately
//! xbbg.set_log_level("warn")    # back to quiet (default)
//! ```
//!
//! # Usage from Rust
//!
//! Other crates in the workspace depend on `xbbg-log` and use the
//! re-exported tracing macros:
//!
//! ```rust,ignore
//! use xbbg_log::{trace, debug, info, warn, error};
//!
//! info!(worker_id = 0, "request completed");
//! ```
//!
//! # Developer Override
//!
//! Setting `RUST_LOG` to a simple level (`trace`, `debug`, `info`, `warn`, or
//! `error`) sets the initial global level. Python can still change it later via
//! `xbbg.set_log_level()`.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};

use tracing_core::{LevelFilter, Metadata};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

// Re-export tracing macros so other crates depend only on xbbg-log.
pub use tracing::{debug, error, info, trace, warn};
pub use tracing::{debug_span, error_span, info_span, trace_span, warn_span};

// Re-export Level for set_level callers.
pub use tracing::Level;

// ---------------------------------------------------------------------------
// Atomic level filter
// ---------------------------------------------------------------------------

/// Maps [`Level`] to a `u8` for atomic storage.
///
/// Lower numeric value = more verbose.
const fn level_to_u8(level: Level) -> u8 {
    match level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}

/// Maps a `u8` back to a [`Level`].
const fn u8_to_level(val: u8) -> Level {
    match val {
        0 => Level::TRACE,
        1 => Level::DEBUG,
        2 => Level::INFO,
        3 => Level::WARN,
        _ => Level::ERROR,
    }
}

/// Global atomic holding the current log level.
///
/// Accessed from every `tracing` callsite — a single `Relaxed` load (~1 ns).
static LEVEL: OnceLock<AtomicU8> = OnceLock::new();

fn global_level() -> &'static AtomicU8 {
    LEVEL.get_or_init(|| AtomicU8::new(level_to_u8(Level::WARN)))
}

/// Set the global log level.
///
/// This is the function exposed to Python via `xbbg.set_log_level()`.
/// It performs a single atomic store — no locks, no GIL.
pub fn set_level(level: Level) {
    global_level().store(level_to_u8(level), Ordering::Relaxed);
}

/// Get the current global log level.
pub fn current_level() -> Level {
    u8_to_level(global_level().load(Ordering::Relaxed))
}

/// Parse a level string (case-insensitive) into a [`Level`].
///
/// Accepts: `"trace"`, `"debug"`, `"info"`, `"warn"` / `"warning"`,
/// `"error"`, or numeric `"0"`–`"4"`.
pub fn parse_level(s: &str) -> Option<Level> {
    match s.to_ascii_lowercase().as_str() {
        "trace" | "0" => Some(Level::TRACE),
        "debug" | "1" => Some(Level::DEBUG),
        "info" | "2" => Some(Level::INFO),
        "warn" | "warning" | "3" => Some(Level::WARN),
        "error" | "4" => Some(Level::ERROR),
        _ => None,
    }
}

/// A [`tracing_subscriber::layer::Filter`] backed by an [`AtomicU8`].
///
/// Every callsite hits a single `Relaxed` atomic load to decide whether
/// the event is enabled — no allocation, no lock, no GIL.
pub struct AtomicLevelFilter;

impl<S> Filter<S> for AtomicLevelFilter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        let threshold = global_level().load(Ordering::Relaxed);
        level_to_u8(*meta.level()) >= threshold
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        // Tell tracing the widest level we might ever accept so that
        // callsites aren't permanently disabled.  Because the user can
        // change the level to TRACE at any time, we must report TRACE.
        Some(LevelFilter::TRACE)
    }
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/// Initialize the global tracing subscriber.
///
/// Call this **once** from the PyO3 module init (`_core`).
///
/// # Behaviour
///
/// `RUST_LOG` may set the initial global level when it contains a simple level
/// string. Output goes directly to stderr; the logging path remains GIL-free.
pub fn init() {
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    if let Ok(level) = std::env::var("RUST_LOG")
        && let Some(level) = parse_level(&level)
    {
        set_level(level);
    }

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    let subscriber = tracing_subscriber::registry().with(fmt_layer.with_filter(AtomicLevelFilter));
    let _ = tracing::subscriber::set_global_default(subscriber);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_roundtrip() {
        for level in [
            Level::TRACE,
            Level::DEBUG,
            Level::INFO,
            Level::WARN,
            Level::ERROR,
        ] {
            assert_eq!(u8_to_level(level_to_u8(level)), level);
        }
    }

    #[test]
    fn parse_level_cases() {
        assert_eq!(parse_level("trace"), Some(Level::TRACE));
        assert_eq!(parse_level("DEBUG"), Some(Level::DEBUG));
        assert_eq!(parse_level("Info"), Some(Level::INFO));
        assert_eq!(parse_level("warning"), Some(Level::WARN));
        assert_eq!(parse_level("WARN"), Some(Level::WARN));
        assert_eq!(parse_level("error"), Some(Level::ERROR));
        assert_eq!(parse_level("0"), Some(Level::TRACE));
        assert_eq!(parse_level("4"), Some(Level::ERROR));
        assert_eq!(parse_level("garbage"), None);
    }

    #[test]
    fn set_and_get_level() {
        set_level(Level::DEBUG);
        assert_eq!(current_level(), Level::DEBUG);
        set_level(Level::WARN);
        assert_eq!(current_level(), Level::WARN);
    }
}
