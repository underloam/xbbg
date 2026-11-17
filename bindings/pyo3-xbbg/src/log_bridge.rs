//! Python-facing end of the opt-in log bridge (`xbbg_log::bridge`).
//!
//! Emitting threads only capture into the native queue and never touch Python.
//! The Python package owns the consumer: a daemon thread that loops on
//! [`_log_bridge_next_batch`], which waits detached from the interpreter, and
//! hands the owned records to `logging`. Nothing here attaches to the
//! interpreter; everything runs on the caller's thread with its `py` token.
//!
//! Records cross as plain tuples
//! `(levelno, logger_name, message, pathname, lineno, created, thread_name)`:
//! `levelno` is the `logging` level number, `logger_name` is the Rust target
//! with `::` replaced by `.`, and `created` is seconds since the Unix epoch at
//! emission time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_log::Level;
use xbbg_log::bridge::{self, LogRecord};

/// `pathname` reported by `logging` for records without a source file.
const UNKNOWN_FILE: &str = "(unknown file)";

/// The `logging` level number for a tracing level (`TRACE` is 5, below DEBUG).
fn python_level(level: Level) -> i32 {
    match level {
        Level::TRACE => 5,
        Level::DEBUG => 10,
        Level::INFO => 20,
        Level::WARN => 30,
        Level::ERROR => 40,
    }
}

/// Logger name for a Rust target: module paths become dotted names, other
/// targets (such as `xbbg.sdk`) are already dotted.
fn logger_name(target: &str) -> String {
    target.replace("::", ".")
}

fn epoch_seconds(timestamp: SystemTime) -> f64 {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_secs_f64(),
        Err(before) => -before.duration().as_secs_f64(),
    }
}

type PyRecord = (i32, String, String, String, u32, f64, String);

fn to_py_record(record: LogRecord) -> PyRecord {
    (
        python_level(record.level),
        logger_name(record.target),
        record.message,
        record.file.unwrap_or(UNKNOWN_FILE).to_owned(),
        record.line.unwrap_or(0),
        epoch_seconds(record.timestamp),
        record.thread_name,
    )
}

/// Start capturing log events into a native queue of `capacity` records.
///
/// While capture is on, events go to the queue instead of stderr. Returns
/// `True` if capture started and `False` if it was already on (the queue and
/// its capacity are left as they were).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_enable(capacity: usize) -> bool {
    bridge::enable(capacity)
}

/// Stop capturing and wake the waiting consumer; events go to stderr again.
///
/// Records already queued stay available to `_log_bridge_next_batch`, whose
/// last batch then reports `closed`.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_disable() {
    bridge::disable();
}

/// Wait up to `timeout` seconds for queued records and take at most `max_records`.
///
/// Releases the interpreter while waiting. Returns `(records, dropped, closed)`:
/// `records` are `(levelno, logger_name, message, pathname, lineno, created,
/// thread_name)` tuples in emission order (empty on timeout); `dropped` counts
/// the records lost since the previous batch; `closed` means capture is off and
/// the queue is empty.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_next_batch(
    py: Python<'_>,
    timeout: f64,
    max_records: usize,
) -> PyResult<(Vec<PyRecord>, u64, bool)> {
    let timeout = Duration::try_from_secs_f64(timeout)
        .map_err(|_| PyValueError::new_err("timeout must be a finite, non-negative number"))?;
    let batch = py.detach(|| bridge::next_batch(timeout, max_records));
    let records = batch.records.into_iter().map(to_py_record).collect();
    Ok((records, batch.dropped, batch.closed))
}

/// Drop whatever is still queued, count it as dropped, and return how many.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_discard() -> usize {
    bridge::discard_queued()
}

/// Count `count` records the consumer took from a batch but did not deliver.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_count_dropped(count: u64) {
    bridge::count_dropped(count);
}

/// Emit one native log event, as the engine's own events are emitted.
///
/// A diagnostic probe: it goes through the same level filter and destination
/// (stderr, or the bridge queue) as every other native event, so it shows
/// whether forwarding is wired up without a Bloomberg session. `level` is one
/// of `trace`, `debug`, `info`, `warn`, `error`; the event's target is this
/// module (`_core::log_bridge`, after the extension's library name).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_probe(level: &str, message: &str) -> PyResult<()> {
    match xbbg_log::parse_level(level) {
        Some(Level::TRACE) => xbbg_log::trace!("{message}"),
        Some(Level::DEBUG) => xbbg_log::debug!("{message}"),
        Some(Level::INFO) => xbbg_log::info!("{message}"),
        Some(Level::WARN) => xbbg_log::warn!("{message}"),
        Some(Level::ERROR) => xbbg_log::error!("{message}"),
        None => {
            return Err(PyValueError::new_err(format!(
                "Invalid log level '{level}'. Expected: trace, debug, info, warn, error"
            )));
        }
    }
    Ok(())
}

/// Total log records dropped by the bridge since the process started.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _log_bridge_dropped() -> u64 {
    bridge::dropped_total()
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_log_bridge_enable, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_disable, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_next_batch, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_discard, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_count_dropped, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_probe, m)?)?;
    m.add_function(wrap_pyfunction!(_log_bridge_dropped, m)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn levels_follow_the_logging_module() {
        assert_eq!(python_level(Level::TRACE), 5);
        assert_eq!(python_level(Level::DEBUG), 10);
        assert_eq!(python_level(Level::INFO), 20);
        assert_eq!(python_level(Level::WARN), 30);
        assert_eq!(python_level(Level::ERROR), 40);
    }

    #[test]
    fn probe_rejects_unknown_levels() {
        Python::initialize();
        Python::attach(|py| {
            let error = _log_bridge_probe("loud", "message").expect_err("invalid level");
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn logger_names_are_dotted() {
        assert_eq!(
            logger_name("xbbg_async::engine::worker"),
            "xbbg_async.engine.worker"
        );
        assert_eq!(logger_name("xbbg.sdk"), "xbbg.sdk");
    }

    #[test]
    fn records_convert_to_plain_tuples() {
        let record = LogRecord {
            message: "message ticker=\"TEST Equity\"".to_owned(),
            level: Level::WARN,
            target: "xbbg_core::session",
            file: None,
            line: None,
            timestamp: UNIX_EPOCH + Duration::from_millis(1_500),
            thread_name: "worker".to_owned(),
        };
        assert_eq!(
            to_py_record(record),
            (
                30,
                "xbbg_core.session".to_owned(),
                "message ticker=\"TEST Equity\"".to_owned(),
                UNKNOWN_FILE.to_owned(),
                0,
                1.5,
                "worker".to_owned(),
            )
        );
    }
}
