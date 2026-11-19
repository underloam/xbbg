//! PyO3 bindings for xbbg Bloomberg engine.
//!
//! This module provides Python bindings for the Rust xbbg Engine,
//! exposing a generic `request()` method that accepts parameters from Python.
//!
//! # Interpreter attachment
//!
//! - Synchronous methods run on the calling Python thread with the caller's
//!   `Python` token; blocking native work such as `Engine::start()`, shutdown
//!   signalling and cache file I/O runs detached (`py.detach`).
//! - Async methods return awaitables built by `completion::shutdown_safe_future`.
//!   Their futures run on a two-worker bridge runtime and yield owned Rust
//!   payloads; bridge workers never attach to the interpreter.
//!   pyo3-async-runtimes attaches once per completion on its blocking pool,
//!   where payloads become Python objects, including unpublished delivery
//!   receipts (backend conversion then runs on the consuming event loop).
//!   Cancelled awaitables drop their payload there without converting it.
//! - Exceptions are lazy plans (class, message, attributes): the instance is
//!   created only when Python observes the error, wherever it was raised.
//! - Once interpreter finalization begins, completed results are dropped
//!   natively and their awaitables never resolve.
//! - Non-test code never calls `Python::attach`/`Python::try_attach`; the
//!   crate's `clippy.toml` disallows both.
//!
//! # Teardown
//!
//! The last release of a native engine, by a `PyEngine` or one of its futures,
//! only hands the engine to a native teardown thread, which joins its runtime
//! and stops its SDK sessions. A subscription consumer released after its
//! engine follows it there (`teardown`). Interpreter exit waits for that
//! teardown from `_engine._atexit_cleanup` and again after finalization.
//!
//! # Exception Mapping
//!
//! Rust errors are mapped to Python exceptions:
//! - `BlpError::SessionStart` → `BlpSessionError`
//! - `BlpError::OpenService` → `BlpSessionError`
//! - `BlpError::RequestFailure` → `BlpRequestError`
//! - `BlpError::Timeout` → `BlpTimeoutError`
//! - `BlpError::InvalidArgument` → `BlpValidationError`
//! - `BlpError::SubscriptionDataLoss` → `BlpSubscriptionDataLossError`
//! - `BlpError::Internal` → `BlpInternalError`
//!
//! # Logging
//!
//! Rust tracing events are output to stderr via a non-blocking writer.
//! The log level is controlled from Python without any GIL acquisition:
//!
//! ```python
//! import xbbg
//! xbbg.set_log_level("debug")   # sets atomic level, no GIL on log path
//! xbbg.set_log_level("warn")    # default — quiet for end users
//! ```
//!
//! For per-crate control, set `RUST_LOG` before importing xbbg:
//!
//! ```bash
//! RUST_LOG=xbbg_core=trace,xbbg_async=debug python my_script.py
//! ```
//!
//! `xbbg.enable_python_logging()` opts in to forwarding these events to
//! Python's `logging` module instead of stderr (`log_bridge`). Emitting
//! threads still never touch Python: they push owned records into a bounded
//! native queue (dropping and counting on overflow), and a Python daemon thread
//! drains it.

// Proving `Send` for the futures handed to pyo3-async-runtimes walks every
// nested async layer (bridge future, completion gate, `select!`, request); the
// request futures exceed the default depth of 128, which rustc's
// `recursion_depth_exceeding_limit` lint (rust-lang/rust#159228) will turn into
// a hard error.
#![recursion_limit = "256"]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::{define_stub_info_gatherer, derive::*};
use xbbg_log::info;

mod completion;
mod config;
mod engine;
mod errors;
mod ext;
mod log_bridge;
mod markets;
mod native_arrow;
mod preflight;
mod recipes;
mod request;
mod result_diagnostics;
mod sdk;
mod shaping;
mod subscribe;
#[cfg(feature = "test-support")]
mod subscription_fixture;
mod subscriptions;
mod teardown;

pub use config::{PyEngineConfig, PyTlsMaterial};
pub(crate) use engine::{EntitlementReport, PyEngine};
#[cfg(test)]
pub(crate) use errors::{
    BlpErrorBase, BlpInternalError, BlpLimitError, BlpRequestError, BlpSessionError,
    BlpSubscriptionDataLossError, BlpTimeoutError, BlpValidationError, error_presentation_to_pyerr,
    subscription_control_error_to_pyerr,
};
pub(crate) use errors::{
    ExceptionClass, ExceptionPlan, blp_async_error_to_pyerr, blp_error_to_pyerr,
};
pub use subscriptions::PySubscription;

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _signal_interpreter_shutdown() {
    info!("PyO3 interpreter finalization signalled");
    completion::signal_interpreter_shutdown();
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn version() -> String {
    xbbg_core::version().to_string()
}

#[pymodule]
#[pyo3(name = "_core")]
fn _core(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize zero-GIL logging: tracing -> AtomicLevelFilter -> non-blocking stderr
    //
    // Python controls via xbbg.set_log_level("debug").
    // Developers override with RUST_LOG=xbbg_core=trace,xbbg_async=debug.
    xbbg_log::init();

    // Configure the process-global Python↔Rust bridge before its first use.
    // This runtime is owned by pyo3-async-runtimes, not by any Engine instance.
    let mut bridge_runtime = tokio::runtime::Builder::new_multi_thread();
    bridge_runtime.worker_threads(2).enable_all();
    pyo3_async_runtimes::tokio::init(bridge_runtime);

    info!("xbbg._core module initialized");

    // The exact version of the xbbg distribution this binary was built for:
    // setup.py exports the setuptools-scm version (dev builds name their commit),
    // and plain Cargo builds fall back to the crate version. The Python package
    // refuses an extension stamped for another distribution; `version()` keeps
    // reporting the Rust crate version.
    m.add("__version__", env!("XBBG_DIST_VERSION"))?;
    let build_info = PyDict::new(py);
    build_info.set_item("profile", env!("XBBG_BUILD_PROFILE"))?;
    build_info.set_item("target", env!("XBBG_BUILD_TARGET"))?;
    let encoded_rustflags = env!("XBBG_BUILD_RUSTFLAGS");
    let rust_flags: Vec<&str> = encoded_rustflags
        .split('\u{1f}')
        .filter(|flag| !flag.is_empty())
        .collect();
    build_info.set_item("rustFlags", rust_flags)?;
    build_info.set_item("rustcVersion", env!("XBBG_BUILD_RUSTC_VERSION"))?;
    build_info.set_item("gitCommit", env!("XBBG_BUILD_GIT_COMMIT"))?;
    build_info.set_item("gitDescribe", env!("VERGEN_GIT_DESCRIBE"))?;
    build_info.set_item("allocator", env!("XBBG_BUILD_ALLOCATOR"))?;
    build_info.set_item("optLevel", env!("XBBG_BUILD_OPT_LEVEL"))?;
    let target_features: Vec<&str> = env!("XBBG_BUILD_TARGET_FEATURES")
        .split(',')
        .filter(|feature| !feature.is_empty())
        .collect();
    build_info.set_item("targetFeatures", target_features)?;
    m.add("__build_info__", build_info)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    sdk::register(m)?;
    m.add_function(wrap_pyfunction!(_signal_interpreter_shutdown, m)?)?;
    teardown::register(m)?;
    log_bridge::register(m)?;
    native_arrow::register(m)?;
    shaping::register(m)?;
    result_diagnostics::register(m)?;
    m.add_class::<EntitlementReport>()?;
    m.add_class::<PyEngineConfig>()?;
    m.add_class::<PySubscription>()?;
    m.add_class::<PyEngine>()?;
    #[cfg(feature = "test-support")]
    subscription_fixture::register(m)?;

    // Register exception classes for use from Python
    errors::register(py, m)?;

    // Logging control (zero-GIL)
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(get_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(enable_sdk_logging, m)?)?;

    // Register ext functions (date, pivot, ticker, futures, cdx, currency utilities)
    ext::register_ext_module(m)?;

    // Register auction and ETF iNAV preflight checks (shared with xbbg-host)
    preflight::register(m)?;

    // Register subscription argument checks (shared with xbbg-host)
    subscribe::register(m)?;

    // Register markets functions (session derivation, market rules, timezone inference)
    markets::register(m)?;

    // Register high-level Bloomberg workflows, including auction venue recipes.
    recipes::register_recipes_module(m)?;

    Ok(())
}

// =============================================================================
// Logging control — Python-facing functions
// =============================================================================

/// Set the Rust log level.
///
/// Accepts: "trace", "debug", "info", "warn", "error".
/// Default is "warn" (quiet for end users).
///
/// This sets an atomic integer — no GIL is held on the logging hot path.
/// For per-crate control, use the RUST_LOG environment variable instead.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn set_log_level(level: &str) -> PyResult<()> {
    let lvl = xbbg_log::parse_level(level).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid log level '{}'. Expected: trace, debug, info, warn, error",
            level
        ))
    })?;
    xbbg_log::set_level(lvl);
    Ok(())
}

/// Get the current Rust log level as a string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn get_log_level() -> &'static str {
    match xbbg_log::current_level() {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn enable_sdk_logging(level: &str) -> PyResult<()> {
    let lvl: xbbg_async::sdk_logging::SdkLogLevel = level
        .parse()
        .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;
    xbbg_async::sdk_logging::register_sdk_logging(lvl).map_err(blp_async_error_to_pyerr)
}

#[cfg(feature = "stub-gen")]
define_stub_info_gatherer!(stub_info);

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::types::{PyBytes, PyDict};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use tokio::sync::{Mutex, watch};
    use xbbg_async::engine::state::{
        FieldKind, FieldLayout, FieldMeta, SubscriptionMetrics, SubscriptionUpdate,
        subscription_channel, subscription_channel_with_policy,
    };
    use xbbg_async::engine::{
        EngineConfig, ExtractorType, SubscriptionEventCategory, SubscriptionEventLevel,
        SubscriptionStatusHandle, SubscriptionStatusState, TlsMaterial, Transport,
    };
    use xbbg_async::error_presentation::{
        ErrorKind, ErrorPresentation, RequestErrorContext, SubscriptionDataLossContext,
    };
    use xbbg_async::subscription_consumer::{
        PendingUpdates, SubscriptionConsumer, SubscriptionRead, receive_subscription_updates,
        wait_for_subscription_close,
    };
    use xbbg_async::{BlpAsyncError, DelayedPolicy, OverflowPolicy};
    use xbbg_core::{AuthConfig, BlpError};

    use crate::request::dict_to_request_params;
    use crate::subscriptions::{
        LegacySubscriptionArrow, SubscriptionMetricsMap, subscription_metrics_totals,
    };
    use crate::teardown::SubscriptionOwner;

    /// Run `work` on another thread while this thread holds the interpreter.
    ///
    /// On GIL builds any attach inside `work` blocks until the deadline, so
    /// finishing in time shows the work never waited for the interpreter.
    pub(crate) fn run_while_interpreter_held<T: Send + 'static>(
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        Python::initialize();
        Python::attach(|_py| {
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = done_tx.send(work());
            });
            done_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("native work must not wait for the interpreter")
        })
    }

    fn metrics(
        messages_received: u64,
        dropped_batches: u64,
        batches_sent: u64,
        slow_consumer: bool,
    ) -> Arc<SubscriptionMetrics> {
        Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(messages_received)),
            dropped_batches: Arc::new(AtomicU64::new(dropped_batches)),
            batches_sent: Arc::new(AtomicU64::new(batches_sent)),
            slow_consumer: Arc::new(AtomicBool::new(slow_consumer)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        })
    }

    fn subscription_update(timestamp_us: i64) -> SubscriptionUpdate {
        subscription_update_with_layout(Arc::new(FieldLayout::new(1, Vec::new())), timestamp_us)
    }

    fn subscription_update_with_layout(
        layout: Arc<FieldLayout>,
        timestamp_us: i64,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from("IBM US Equity"),
            layout,
            values: Default::default(),
        }
    }

    fn closed_subscription(deliver_rows: bool) -> PySubscription {
        PySubscription {
            consumer: Arc::new(SubscriptionOwner::unhosted(SubscriptionConsumer::default())),
            legacy_arrow: Some(Arc::new(LegacySubscriptionArrow::new(1))),
            batch_items: 1,
            status: Arc::new(SubscriptionStatusHandle::new(
                SubscriptionStatusState::default(),
            )),
            deliver_rows,
            overflow_policy: OverflowPolicy::DropNewest,
            engine_shutdown: watch::channel(false).1,
            tick_dict_keys: Arc::default(),
            #[cfg(feature = "test-support")]
            _test_fixture: None,
            #[cfg(feature = "test-support")]
            _test_legacy_read_started: None,
        }
    }

    #[test]
    fn lazy_errors_are_built_without_the_interpreter_with_attribute_parity() {
        const KINDS: [(ErrorKind, &str, bool); 7] = [
            (ErrorKind::Session, "BlpSessionError", false),
            (ErrorKind::Request, "BlpRequestError", true),
            (ErrorKind::Limit, "BlpLimitError", true),
            (ErrorKind::DataLoss, "BlpSubscriptionDataLossError", true),
            (ErrorKind::Validation, "BlpValidationError", false),
            (ErrorKind::Timeout, "BlpTimeoutError", true),
            (ErrorKind::Internal, "BlpInternalError", false),
        ];
        const REQUEST_ATTRS: [&str; 4] =
            ["service", "operation", "request_id", "bloomberg_request_id"];
        let (plain, contextual, composed, closed) = run_while_interpreter_held(|| {
            let plain = KINDS.map(|(kind, _, _)| {
                error_presentation_to_pyerr(ErrorPresentation {
                    kind,
                    message: format!("synthetic {kind:?} failure"),
                    data_loss: None,
                    request_context: None,
                })
            });
            let contextual = KINDS.map(|(kind, _, _)| {
                error_presentation_to_pyerr(ErrorPresentation {
                    kind,
                    message: "synthetic contextual failure".into(),
                    data_loss: Some(SubscriptionDataLossContext {
                        topic: "SYNTHETIC Index".into(),
                        detail: "synthetic gap".into(),
                    }),
                    request_context: Some(RequestErrorContext {
                        service: Some("//synthetic/refdata".into()),
                        operation: None,
                        request_id: Some("synthetic-label".into()),
                        bloomberg_request_ids: vec![Arc::from("synthetic-a")],
                    }),
                })
            });
            let composed = blp_error_to_pyerr(BlpError::RequestTimeout {
                request_id: None,
                bloomberg_request_ids: vec![Arc::from("synthetic-a"), Arc::from("synthetic-z")],
            });
            let closed = subscription_control_error_to_pyerr(BlpAsyncError::ChannelClosed);
            (plain, contextual, composed, closed)
        });
        Python::attach(|py| {
            for ((kind, class, defaults), error) in KINDS.iter().zip(&plain) {
                assert_eq!(error.get_type(py).name().unwrap().to_str().unwrap(), *class);
                let value = error.value(py);
                assert_eq!(value.to_string(), format!("synthetic {kind:?} failure"));
                for name in REQUEST_ATTRS {
                    assert_eq!(value.hasattr(name).unwrap(), *defaults, "{class}.{name}");
                    if *defaults {
                        assert!(value.getattr(name).unwrap().is_none(), "{class}.{name}");
                    }
                }
                assert_eq!(value.hasattr("bloomberg_request_ids").unwrap(), *defaults);
                if *defaults {
                    let ids: Vec<String> = value
                        .getattr("bloomberg_request_ids")
                        .unwrap()
                        .extract()
                        .unwrap();
                    assert!(ids.is_empty(), "{class}.bloomberg_request_ids");
                }
                for name in ["topic", "detail"] {
                    assert!(!value.hasattr(name).unwrap(), "{class}.{name}");
                }
            }
            for ((_, class, _), error) in KINDS.iter().zip(&contextual) {
                assert_eq!(error.get_type(py).name().unwrap().to_str().unwrap(), *class);
                let value = error.value(py);
                assert_eq!(value.to_string(), "synthetic contextual failure");
                let text = |name: &str| -> Option<String> {
                    value.getattr(name).unwrap().extract().unwrap()
                };
                assert_eq!(text("service").as_deref(), Some("//synthetic/refdata"));
                assert_eq!(text("operation"), None);
                assert_eq!(text("request_id").as_deref(), Some("synthetic-label"));
                assert_eq!(text("bloomberg_request_id").as_deref(), Some("synthetic-a"));
                let ids: Vec<String> = value
                    .getattr("bloomberg_request_ids")
                    .unwrap()
                    .extract()
                    .unwrap();
                assert_eq!(ids, ["synthetic-a"]);
                assert_eq!(text("topic").as_deref(), Some("SYNTHETIC Index"));
                assert_eq!(text("detail").as_deref(), Some("synthetic gap"));
            }

            assert!(composed.is_instance_of::<BlpTimeoutError>(py));
            let value = composed.value(py);
            assert!(value.getattr("bloomberg_request_id").unwrap().is_none());
            let ids: Vec<String> = value
                .getattr("bloomberg_request_ids")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(ids, ["synthetic-a", "synthetic-z"]);

            assert!(closed.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(closed.value(py).to_string(), "subscription already closed");
        });
    }

    #[test]
    fn subscription_metrics_totals_only_counts_active_entries() {
        let mut metrics_map = SubscriptionMetricsMap::new();
        metrics_map.insert(10, metrics(5, 1, 4, false));
        metrics_map.insert(11, metrics(7, 2, 6, true));

        metrics_map.remove(&10);

        assert_eq!(
            subscription_metrics_totals(&metrics_map),
            (7, 2, 6, true, 0, 0, 0)
        );
    }

    #[test]
    fn image_only_subscription_rejects_every_python_iteration_entry_point() {
        Python::initialize();
        Python::attach(|py| {
            let subscription = Py::new(py, closed_subscription(false)).unwrap();
            let subscription = subscription.bind(py);
            assert!(
                !subscription
                    .getattr("delivers_rows")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            for method in [
                "__aiter__",
                "__anext__",
                "__anext_tick_dict__",
                "next_delivery",
            ] {
                let error = subscription
                    .call_method0(method)
                    .expect_err("image-only iteration");
                assert!(error.is_instance_of::<PyRuntimeError>(py));
                let message = error.value(py).to_string();
                assert!(message.contains("rows=False"));
                assert!(message.contains("latest()"));
            }
        });
    }

    #[test]

    fn advanced_python_iteration_guards_retain_real_native_receipts() {
        Python::initialize();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, rx) = subscription_channel_with_policy(1, OverflowPolicy::DropOldest);
            tx.try_send(Ok(subscription_update(10))).unwrap();
            tx.try_send(Ok(subscription_update(20))).unwrap();
            let consumer = Arc::new(SubscriptionOwner::unhosted(
                SubscriptionConsumer::from_receiver(rx),
            ));
            Python::attach(|py| {
                let mut subscription = closed_subscription(true);
                subscription.consumer = consumer.clone();
                subscription.overflow_policy = OverflowPolicy::DropOldest;
                let subscription = Py::new(py, subscription).unwrap();
                for method in ["__aiter__", "__anext__", "__anext_tick_dict__"] {
                    let error = subscription
                        .bind(py)
                        .call_method0(method)
                        .expect_err("plain advanced iteration");
                    assert!(error.value(py).to_string().contains("deliveries()"));
                }
            });
            let lease = consumer.next_delivery(1).await.expect("unconsumed receipt");
            assert_eq!(
                lease.as_ref().unwrap().gap_before.unwrap().dropped_updates,
                1
            );
            assert_eq!(lease.commit().unwrap().data.num_rows(), 1);
        });
    }

    #[test]
    fn subscription_warnings_survive_closed_control_and_drain_once() {
        Python::initialize();
        Python::attach(|py| {
            let subscription = closed_subscription(false);
            subscription.status.update(|status| {
                for (kind, detail) in [
                    ("DelayedStream", "synthetic delayed stream"),
                    ("FieldException", "SYNTHETIC_FIELD: BAD_FLD"),
                ] {
                    status.push_event(
                        SubscriptionEventCategory::Subscription,
                        SubscriptionEventLevel::Warning,
                        kind,
                        Some("IBM US Equity".into()),
                        Some(detail.into()),
                    );
                }
            });
            let history = subscription.events(py);
            let warnings = subscription.take_warnings(py).unwrap();
            let kinds: Vec<String> = warnings
                .iter()
                .map(|warning| {
                    warning
                        .bind(py)
                        .get_item("message_type")
                        .unwrap()
                        .unwrap()
                        .extract()
                        .unwrap()
                })
                .collect();
            assert_eq!(kinds, ["DelayedStream", "FieldException"]);
            assert!(subscription.take_warnings(py).unwrap().is_empty());
            assert_eq!(subscription.events(py), history);
        });
    }

    #[test]
    fn latest_rejects_closed_subscription_instead_of_fabricating_empty_batch() {
        Python::initialize();
        Python::attach(|py| {
            let error = closed_subscription(false)
                .latest(py)
                .expect_err("closed latest");
            assert!(error.is_instance_of::<PyRuntimeError>(py));
        });
    }

    #[test]
    fn session_wait_rejection_preserves_admission_error_protocol() {
        Python::initialize();
        Python::attach(|py| {
            let error = blp_async_error_to_pyerr(BlpAsyncError::ConfigError {
                detail: "session_wait: subscription session capacity is unavailable".into(),
            });
            assert!(error.is_instance_of::<BlpValidationError>(py));
            assert!(
                error
                    .value(py)
                    .to_string()
                    .starts_with("Configuration error: session_wait:")
            );
        });
    }

    #[test]
    fn request_error_adapters_attach_sdk_identifiers_as_attributes() {
        Python::initialize();
        Python::attach(|py| {
            for count in 0..=2 {
                let ids: Vec<std::sync::Arc<str>> = ["sdk-synthetic-a", "sdk-synthetic-z"]
                    .into_iter()
                    .take(count)
                    .map(std::sync::Arc::from)
                    .collect();
                for timeout in [false, true] {
                    let core = if timeout {
                        BlpError::RequestTimeout {
                            request_id: Some("req-label".into()),
                            bloomberg_request_ids: ids.clone(),
                        }
                    } else {
                        BlpError::RequestFailure {
                            service: "//synthetic/refdata".into(),
                            operation: Some("SyntheticRequest".into()),
                            cid: None,
                            label: Some("synthetic failure".into()),
                            request_id: Some("req-label".into()),
                            bloomberg_request_ids: ids.clone(),
                            source: None,
                        }
                    };
                    let error = blp_error_to_pyerr(core);
                    assert_eq!(error.is_instance_of::<BlpTimeoutError>(py), timeout);
                    let value = error.value(py);
                    assert_eq!(
                        value
                            .getattr("request_id")
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        "req-label"
                    );
                    let singular = value
                        .getattr("bloomberg_request_id")
                        .unwrap()
                        .extract::<Option<String>>()
                        .unwrap();
                    assert_eq!(
                        singular.as_deref(),
                        (count == 1).then_some("sdk-synthetic-a")
                    );
                    let actual = value
                        .getattr("bloomberg_request_ids")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap();
                    assert_eq!(
                        actual,
                        ids.iter().map(ToString::to_string).collect::<Vec<_>>()
                    );
                }
            }
        });
    }

    #[test]
    fn core_error_adapters_preserve_python_classes_and_messages() {
        Python::initialize();
        Python::attach(|py| {
            for wrapped in [false, true] {
                for (error, class, message) in [
                    (
                        BlpError::SessionStart {
                            source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                            label: Some("unavailable".into()),
                        },
                        "BlpSessionError",
                        "Session start failed: unavailable - synthetic cause",
                    ),
                    (
                        BlpError::SubscriptionFailure {
                            cid: None,
                            label: None,
                        },
                        "BlpRequestError",
                        "Subscription failed",
                    ),
                    (
                        BlpError::RequestFailure {
                            service: "//blp/refdata".into(),
                            operation: Some("ReferenceDataRequest".into()),
                            cid: Some(xbbg_core::errors::CorrelationContext::U64(42)),
                            label: Some("subcategory=DAILY_CAPACITY_REACHED".into()),
                            request_id: Some("synthetic-request".into()),
                            bloomberg_request_ids: Vec::new(),
                            source: Some(Box::new(std::io::Error::other("synthetic cause"))),
                        },
                        "BlpLimitError",
                        "Request failed on //blp/refdata::ReferenceDataRequest (cid=42) [request_id=synthetic-request] - subcategory=DAILY_CAPACITY_REACHED: synthetic cause",
                    ),
                    (BlpError::Timeout, "BlpTimeoutError", "Request timed out"),
                    (
                        BlpError::Internal {
                            detail: "session connection dropped (worker=2)".into(),
                        },
                        "BlpInternalError",
                        "Internal error: session connection dropped (worker=2)",
                    ),
                    (
                        BlpError::SchemaTypeMismatch {
                            element: "fields".into(),
                            expected: "String".into(),
                            found: "Int32".into(),
                        },
                        "BlpValidationError",
                        "Schema type mismatch at fields: expected \"String\", found \"Int32\"",
                    ),
                    (
                        BlpError::Validation {
                            message: "invalid request".into(),
                            errors: vec![xbbg_core::errors::ValidationError {
                                path: "fields[0]".into(),
                                message: "unknown field".into(),
                                suggestion: Some("PX_LAST".into()),
                            }],
                        },
                        "BlpValidationError",
                        "invalid request: fields[0]: unknown field (did you mean 'PX_LAST'?)",
                    ),
                    (
                        BlpError::SubscriptionDataLoss {
                            topic: "SYNTHETIC Equity".into(),
                            detail: "synthetic overflow".into(),
                        },
                        "BlpSubscriptionDataLossError",
                        "Subscription data loss for 'SYNTHETIC Equity': synthetic overflow",
                    ),
                ] {
                    let error = if wrapped {
                        blp_async_error_to_pyerr(error.into())
                    } else {
                        blp_error_to_pyerr(error)
                    };
                    assert!(error.is_instance_of::<BlpErrorBase>(py));
                    assert_eq!(error.get_type(py).name().unwrap().to_str().unwrap(), class);
                    assert_eq!(error.value(py).to_string(), message);
                }
            }
        });
    }

    #[test]
    fn async_error_adapter_preserves_python_engine_exception_types() {
        Python::initialize();
        Python::attach(|py| {
            for (error, class, message) in [
                (
                    BlpAsyncError::AllWorkersDown { pool_size: 2 },
                    "BlpSessionError",
                    "all 2 request workers are dead — no healthy worker available",
                ),
                (
                    BlpAsyncError::ChannelClosed,
                    "BlpInternalError",
                    "Channel closed unexpectedly",
                ),
                (
                    BlpAsyncError::Internal("synthetic failure".into()),
                    "BlpInternalError",
                    "synthetic failure",
                ),
                (
                    BlpAsyncError::ConfigError {
                        detail: "synthetic config failure".into(),
                    },
                    "BlpValidationError",
                    "Configuration error: synthetic config failure",
                ),
            ] {
                let error = blp_async_error_to_pyerr(error);
                assert_eq!(error.get_type(py).name().unwrap().to_str().unwrap(), class);
                assert_eq!(error.value(py).to_string(), message);
            }
        });
    }

    #[test]
    fn subscription_control_errors_distinguish_closed_from_terminal_failure() {
        Python::initialize();
        Python::attach(|py| {
            let closed = subscription_control_error_to_pyerr(BlpAsyncError::ChannelClosed);
            assert!(closed.is_instance_of::<PyRuntimeError>(py));

            let timeout = subscription_control_error_to_pyerr(BlpError::Timeout.into());
            assert!(timeout.is_instance_of::<BlpTimeoutError>(py));
        });
    }

    #[test]
    fn py_engine_config_defaults_include_auth_defaults() {
        let config = PyEngineConfig::new(None).expect("default config");
        assert_eq!(config.auth_method, None);
        assert_eq!(config.num_start_attempts, 3);
        assert!(config.auto_restart_on_disconnection);
        assert!(!config.shard_requests);
        assert_eq!(config.shard_threshold, 20);
        assert_eq!(config.shard_chunk_size, 16);
        assert_eq!(config.shard_max_concurrent, 4);
        assert_eq!(config.runtime_worker_threads, 2);
        assert_eq!(config.max_subscription_sessions, 32);
    }

    #[test]
    fn py_engine_config_keeps_bql_app_name_independent_from_auth() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("bql_app_name", "example-bql-app")
                .expect("bql_app_name");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("BQL engine config");
            let engine_config =
                EngineConfig::try_from(&config).expect("BQL default does not require auth");
            assert_eq!(
                engine_config.bql_app_name.as_deref(),
                Some("example-bql-app")
            );
            assert_eq!(engine_config.auth, None);

            let auth_kwargs = PyDict::new(py);
            auth_kwargs
                .set_item("app_name", "example-auth-app")
                .expect("app_name");
            let auth_config = PyEngineConfig::new(Some(&auth_kwargs)).expect("partial auth config");
            let auth_error = EngineConfig::try_from(&auth_config)
                .err()
                .expect("auth app_name without auth_method should fail");
            assert!(auth_error.to_string().contains("auth_method is required"));

            let blank_kwargs = PyDict::new(py);
            blank_kwargs
                .set_item("bql_app_name", " \t")
                .expect("blank bql_app_name");
            let blank_config =
                PyEngineConfig::new(Some(&blank_kwargs)).expect("blank BQL engine config");
            let blank_error = EngineConfig::try_from(&blank_config)
                .err()
                .expect("blank BQL app name should fail shared validation");
            assert!(
                blank_error
                    .to_string()
                    .contains("bql_app_name must be non-empty")
            );
        });
    }

    #[test]
    fn py_engine_config_maps_sharding_and_resource_fields() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("runtime_worker_threads", 3)
                .expect("runtime_worker_threads");
            kwargs
                .set_item("max_subscription_sessions", 12)
                .expect("max_subscription_sessions");
            kwargs
                .set_item("shard_requests", true)
                .expect("shard_requests");
            kwargs
                .set_item("shard_threshold", 5)
                .expect("shard_threshold");
            kwargs
                .set_item("shard_chunk_size", 3)
                .expect("shard_chunk_size");
            kwargs
                .set_item("shard_max_concurrent", 2)
                .expect("shard_max_concurrent");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("engine config");
            let engine_config: EngineConfig = (&config).try_into().expect("engine config");

            assert_eq!(engine_config.runtime_worker_threads, 3);
            assert_eq!(engine_config.max_subscription_sessions, 12);
            assert!(engine_config.shard_requests);
            assert_eq!(engine_config.shard_threshold, 5);
            assert_eq!(engine_config.shard_chunk_size, 3);
            assert_eq!(engine_config.shard_max_concurrent, 2);
        });
    }

    #[test]
    fn py_engine_config_maps_manual_auth_to_engine_config() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("auth_method", "manual")
                .expect("auth_method");
            kwargs.set_item("app_name", "my-app").expect("app_name");
            kwargs.set_item("user_id", "123456").expect("user_id");
            kwargs
                .set_item("ip_address", "10.0.0.1")
                .expect("ip_address");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("manual auth config");
            let engine_config: EngineConfig = (&config).try_into().expect("engine config");

            assert_eq!(
                engine_config.auth,
                Some(AuthConfig::Manual {
                    app_name: "my-app".to_string(),
                    user_id: "123456".to_string(),
                    ip_address: "10.0.0.1".to_string(),
                })
            );
        });
    }

    #[test]
    fn py_engine_config_rejects_missing_auth_fields() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("auth_method", "app").expect("auth_method");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("partial auth config");
            let err = match EngineConfig::try_from(&config) {
                Ok(_) => panic!("missing app_name should fail"),
                Err(err) => err,
            };
            assert!(err.to_string().contains("app_name is required"));
        });
    }

    #[test]
    fn py_engine_config_rejects_invalid_resource_limits() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.runtime_worker_threads = 0;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("zero runtime worker count should fail");
        assert!(err.to_string().contains("runtime_worker_threads"));

        config.runtime_worker_threads = 2;
        config.subscription_pool_size = 4;
        config.max_subscription_sessions = 3;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("subscription prewarm above session cap should fail");
        assert!(err.to_string().contains("max_subscription_sessions"));

        config.max_subscription_sessions = 32;
        config.request_pool_size = 0;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("zero request pool should fail during conversion");
        assert!(err.to_string().contains("request_pool_size"));

        config.request_pool_size = 2;
        config.subscription_pool_size = 0;
        assert!(EngineConfig::try_from(&config).is_ok());
    }

    #[test]
    fn py_engine_config_rejects_zero_subscription_stream_capacity() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.subscription_stream_capacity = 0;
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("zero capacity should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("subscription_stream_capacity must be greater than zero")
        );
    }

    #[test]
    fn py_engine_config_rejects_zero_subscription_flush_threshold() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.subscription_flush_threshold = 0;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("zero flush threshold should fail");
        assert!(err.to_string().contains("subscription_flush_threshold"));
    }

    #[test]
    fn py_engine_config_rejects_negative_keep_alive_inactivity() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.keep_alive_inactivity_ms = Some(-1);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("negative keep-alive should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("keep_alive_inactivity_ms must be non-negative")
        );
    }

    #[test]
    fn py_engine_config_rejects_out_of_range_watermarks() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.slow_consumer_hi_water_mark = Some(1.5);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("hi watermark above 1.0 should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("slow_consumer_hi_water_mark must be in 0.0..=1.0")
        );

        // The hi watermark accepts exactly 1.0; the lo watermark is half-open
        // and rejects it (mirrors napi).
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.slow_consumer_hi_water_mark = Some(1.0);
        config.slow_consumer_lo_water_mark = Some(1.0);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("lo watermark of 1.0 should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("slow_consumer_lo_water_mark must be in 0.0..1.0")
        );
    }

    #[test]
    fn py_engine_config_tls_bytes_round_trip_and_share_owned_storage() {
        Python::initialize();
        Python::attach(|py| {
            let credentials = b"\x30\0\xffsynthetic-client";
            let trust = b"\x30\0\xfesynthetic-trust";
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("tls_client_credentials", PyBytes::new(py, credentials))
                .unwrap();
            kwargs
                .set_item("tls_trust_material", PyBytes::new(py, trust))
                .unwrap();
            kwargs
                .set_item("tls_client_credentials_password", "synthetic-password")
                .unwrap();
            kwargs.set_item("tls_handshake_timeout_ms", 0).unwrap();
            kwargs.set_item("tls_crl_fetch_timeout_ms", 25).unwrap();
            let config = PyEngineConfig::new(Some(&kwargs)).unwrap();
            let normalized = EngineConfig::try_from(&config).unwrap();
            let tls = normalized.tls.as_ref().unwrap();
            let TlsMaterial::Blob(actual) = &tls.client_credentials else {
                panic!("expected client blob");
            };
            assert_eq!(actual.as_ref(), credentials);
            let TlsMaterial::Blob(stored) = &config.tls_client_credentials.as_ref().unwrap().0
            else {
                panic!("expected stored client blob");
            };
            assert!(Arc::ptr_eq(actual, stored));
            assert_eq!(tls.trust_material, TlsMaterial::Blob(Arc::from(&trust[..])));
            assert_eq!(tls.handshake_timeout_ms, Some(0));
            assert_eq!(tls.crl_fetch_timeout_ms, Some(25));
            assert!(!config.__repr__().contains("synthetic"));
            assert!(!format!("{tls:?}").contains("synthetic"));

            let config = Py::new(py, config).unwrap();
            let bound = config.bind(py);
            assert_eq!(
                bound
                    .getattr("tls_client_credentials")
                    .unwrap()
                    .cast::<PyBytes>()
                    .unwrap()
                    .as_bytes(),
                credentials
            );
            bound
                .setattr("tls_trust_material", "fixtures/trust.p7")
                .unwrap();
            let error = EngineConfig::try_from(&*bound.borrow())
                .err()
                .expect("mixed TLS material should fail");
            assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            assert!(!error.to_string().contains("synthetic"));
            bound
                .setattr("tls_trust_material", PyBytes::new(py, trust))
                .unwrap();
            EngineConfig::try_from(&*bound.borrow()).unwrap();
            bound.setattr("tls_client_credentials", py.None()).unwrap();
            let error = EngineConfig::try_from(&*bound.borrow())
                .err()
                .expect("unpaired TLS material should fail");
            assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }

    #[test]
    fn py_engine_config_tls_paths_stay_paths_and_reject_non_bytes_sequences() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("tls_client_credentials", "fixtures/client.p12")
                .unwrap();
            kwargs
                .set_item("tls_trust_material", "fixtures/trust.p7")
                .unwrap();
            let config = PyEngineConfig::new(Some(&kwargs)).unwrap();
            let tls = EngineConfig::try_from(&config).unwrap().tls.unwrap();
            assert_eq!(
                tls.client_credentials,
                TlsMaterial::Path("fixtures/client.p12".into())
            );
            assert_eq!(
                tls.trust_material,
                TlsMaterial::Path("fixtures/trust.p7".into())
            );
            let config = Py::new(py, config).unwrap();
            assert_eq!(
                config
                    .bind(py)
                    .getattr("tls_client_credentials")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "fixtures/client.p12"
            );
            kwargs
                .set_item(
                    "tls_client_credentials",
                    pyo3::types::PyList::new(py, [0_u8, 255]).unwrap(),
                )
                .unwrap();
            let error = PyEngineConfig::new(Some(&kwargs)).err().unwrap();
            assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        });
    }

    #[test]
    fn py_engine_config_rejects_negative_tls_timeouts() {
        for with_material in [false, true] {
            for (handshake, crl, field) in [
                (Some(-5), None, "tls_handshake_timeout_ms"),
                (None, Some(-5), "tls_crl_fetch_timeout_ms"),
            ] {
                let mut config = PyEngineConfig::new(None).expect("default config");
                if with_material {
                    config.tls_client_credentials =
                        Some(PyTlsMaterial("fixtures/client.p12".into()));
                    config.tls_trust_material = Some(PyTlsMaterial("fixtures/trust.p7".into()));
                }
                config.tls_handshake_timeout_ms = handshake;
                config.tls_crl_fetch_timeout_ms = crl;
                let err = EngineConfig::try_from(&config)
                    .err()
                    .expect("negative TLS timeout should fail");
                assert!(err.to_string().contains(&format!(
                    "{field} must be a non-negative integer number of milliseconds"
                )));
            }
        }
    }

    #[test]
    fn py_engine_config_accepts_directory_alias() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.auth_method = Some("directory".to_string());
        config.dir_property = Some("mail".to_string());
        assert_eq!(
            EngineConfig::try_from(&config)
                .expect("directory auth")
                .auth,
            Some(AuthConfig::Directory {
                property_name: "mail".to_string(),
            })
        );
    }

    #[test]
    fn py_engine_config_supports_all_auth_methods() {
        let mut config = PyEngineConfig::new(None).expect("default config");

        config.auth_method = Some("user".to_string());
        assert_eq!(
            EngineConfig::try_from(&config).expect("user auth").auth,
            Some(AuthConfig::User)
        );

        config.auth_method = Some("app".to_string());
        config.app_name = Some("app-name".to_string());
        assert_eq!(
            EngineConfig::try_from(&config).expect("app auth").auth,
            Some(AuthConfig::App {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("userapp".to_string());
        assert_eq!(
            EngineConfig::try_from(&config).expect("userapp auth").auth,
            Some(AuthConfig::UserApp {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("dir".to_string());
        config.dir_property = Some("synthetic-property".to_string());
        assert_eq!(
            EngineConfig::try_from(&config).expect("dir auth").auth,
            Some(AuthConfig::Directory {
                property_name: "synthetic-property".to_string(),
            })
        );

        config.auth_method = Some("token".to_string());
        config.token = Some("tok-123".to_string());
        assert_eq!(
            EngineConfig::try_from(&config).expect("token auth").auth,
            Some(AuthConfig::Token {
                token: "tok-123".to_string(),
            })
        );
    }

    #[test]
    fn py_engine_config_preserves_tls_pair_and_transport_error_labels() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.tls_client_credentials = Some(PyTlsMaterial("fixtures/client.p12".into()));
        let err = EngineConfig::try_from(&config).err().expect("unpaired TLS");
        assert!(
            err.to_string()
                .contains("tls_client_credentials set without tls_trust_material")
        );

        config.tls_client_credentials = None;
        config.tls_trust_material = Some(PyTlsMaterial("fixtures/trust.p7".into()));
        let err = EngineConfig::try_from(&config).err().expect("unpaired TLS");
        assert!(
            err.to_string()
                .contains("tls_trust_material set without tls_client_credentials")
        );

        config.tls_client_credentials = Some(PyTlsMaterial("fixtures/client.p12".into()));
        config.zfp_remote = Some("8194".to_string());
        assert!(matches!(
            EngineConfig::try_from(&config)
                .expect("ZFP config")
                .transport,
            Transport::Zfp(_)
        ));
        config.host = "synthetic.invalid".to_string();
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("ZFP plus host");
        assert!(
            err.to_string()
                .contains("zfp_remote cannot be combined with host/port/servers")
        );

        config.host = "localhost".to_string();
        config.socks5_host = Some("proxy.invalid".to_string());
        config.socks5_port = Some(1080);
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("ZFP plus SOCKS5");
        assert!(
            err.to_string()
                .contains("zfp_remote cannot be combined with socks5_host/socks5_port")
        );
    }

    #[test]
    fn py_engine_config_names_python_attributes_and_infers_explicit_host_port() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.socks5_host = Some("proxy.invalid".to_string());
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("half-set SOCKS5");
        assert_eq!(
            err.to_string(),
            "ValueError: socks5_host set without socks5_port"
        );

        config.socks5_host = None;
        config.auth_method = None;
        config.app_name = Some("app-name".to_string());
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("auth fields without a method");
        assert_eq!(
            err.to_string(),
            "ValueError: auth_method is required when auth-specific fields are provided"
        );

        // Explicit host/port is inferred from non-default values only.
        config.app_name = None;
        config.zfp_remote = Some("8196".to_string());
        config.host = "localhost".to_string();
        config.port = 8194;
        assert!(matches!(
            EngineConfig::try_from(&config)
                .expect("default host/port with ZFP")
                .transport,
            Transport::Zfp(_)
        ));
        config.port = 8195;
        assert!(
            EngineConfig::try_from(&config)
                .err()
                .expect("non-default port with ZFP")
                .to_string()
                .contains("zfp_remote cannot be combined with host/port/servers")
        );
    }

    #[test]
    fn py_engine_config_rejects_values_the_shared_spec_cannot_hold() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.request_pool_size = usize::MAX;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("oversized pool");
        assert_eq!(
            err.to_string(),
            "ValueError: request_pool_size must not exceed 4294967295"
        );

        let mut config = PyEngineConfig::new(None).expect("default config");
        config.request_timeout_ms = u64::MAX;
        let err = EngineConfig::try_from(&config)
            .err()
            .expect("oversized timeout");
        assert_eq!(
            err.to_string(),
            "ValueError: request_timeout_ms must not exceed 9223372036854775807"
        );
    }

    #[test]
    fn core_module_registration_exposes_public_names() {
        Python::initialize();
        Python::attach(|py| {
            let module = pyo3::types::PyModule::new(py, "_core").expect("module");
            _core(py, &module).expect("register module");

            for name in [
                "__version__",
                "version",
                "sdk_version",
                "load_sdk",
                "locate_sdk",
                "loaded_sdk",
                "PyEngine",
                "PyEngineConfig",
                "PySubscription",
                "ArrowTable",
                "ArrowRecordBatch",
                "ArrowSchema",
                "__build_info__",
                "ArrowField",
                "EntitlementReport",
                "BlpError",
                "BlpSessionError",
                "BlpRequestError",
                "BlpLimitError",
                "BlpSecurityError",
                "BlpFieldError",
                "BlpSubscriptionDataLossError",
                "BlpValidationError",
                "BlpTimeoutError",
                "BlpInternalError",
                "set_log_level",
                "get_log_level",
                "enable_sdk_logging",
                "_log_bridge_enable",
                "_log_bridge_disable",
                "_log_bridge_next_batch",
                "_log_bridge_discard",
                "_log_bridge_count_dropped",
                "_log_bridge_dropped",
            ] {
                assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
            }
            assert_eq!(
                module.hasattr("_test_log_fixture").expect("hasattr"),
                cfg!(feature = "test-support"),
                "the log probe exists only in test-support builds"
            );
        });
    }
    #[test]
    fn subscription_data_loss_error_preserves_structured_context() {
        Python::initialize();
        Python::attach(|py| {
            let error = blp_error_to_pyerr(BlpError::SubscriptionDataLoss {
                topic: "IBM US Equity".to_string(),
                detail: "consumer queue overflow".to_string(),
            });

            assert!(error.is_instance_of::<BlpSubscriptionDataLossError>(py));
            let value = error.value(py);
            assert_eq!(
                value.to_string(),
                "Subscription data loss for 'IBM US Equity': consumer queue overflow",
            );
            assert_eq!(
                value
                    .getattr("topic")
                    .expect("topic")
                    .extract::<String>()
                    .expect("topic string"),
                "IBM US Equity"
            );
            assert_eq!(
                value
                    .getattr("detail")
                    .expect("detail")
                    .extract::<String>()
                    .expect("detail string"),
                "consumer queue overflow"
            );
        });
    }

    #[test]
    fn dict_to_request_params_preserves_all_accepted_keys() {
        Python::initialize();
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("service", "//blp/refdata").expect("service");
            dict.set_item("operation", "").expect("operation");
            dict.set_item("request_operation", "ReferenceDataRequest")
                .expect("request_operation");
            dict.set_item("request_id", "req-123").expect("request_id");
            dict.set_item("extractor", "refdata").expect("extractor");
            dict.set_item("securities", vec!["IBM US Equity"])
                .expect("securities");
            dict.set_item("security", "IBM US Equity")
                .expect("security");
            dict.set_item("fields", vec!["PX_LAST"]).expect("fields");
            dict.set_item("overrides", vec![("EQY_FUND_CRNCY", "USD")])
                .expect("overrides");
            dict.set_item("elements", vec![("returnEids", "true")])
                .expect("elements");
            dict.set_item(
                "kwargs",
                HashMap::from([("Period".to_string(), "D".to_string())]),
            )
            .expect("kwargs");
            dict.set_item("start_date", "20240101").expect("start_date");
            dict.set_item("end_date", "20240131").expect("end_date");
            dict.set_item("start_datetime", "2024-01-01T09:30:00")
                .expect("start_datetime");
            dict.set_item("end_datetime", "2024-01-01T10:00:00")
                .expect("end_datetime");
            dict.set_item("request_tz", "NY").expect("request_tz");
            dict.set_item("output_tz", "UTC").expect("output_tz");
            dict.set_item("event_type", "TRADE").expect("event_type");
            dict.set_item("event_types", vec!["TRADE", "BID"])
                .expect("event_types");
            dict.set_item("interval", 5_u32).expect("interval");
            dict.set_item("options", vec![("includeConditionCodes", "true")])
                .expect("options");
            dict.set_item(
                "field_types",
                HashMap::from([("PX_LAST".to_string(), "Float64".to_string())]),
            )
            .expect("field_types");
            dict.set_item("include_security_errors", true)
                .expect("include_security_errors");
            dict.set_item("return_eids", true).expect("return_eids");
            dict.set_item("on_delayed", "Raise").expect("on_delayed");
            dict.set_item("validate_fields", false)
                .expect("validate_fields");
            dict.set_item("search_spec", "price").expect("search_spec");
            dict.set_item("field_ids", vec!["PX_LAST"])
                .expect("field_ids");
            dict.set_item("format", "long_typed").expect("format");

            let params = dict_to_request_params(&dict).expect("request params");

            assert_eq!(params.service, "//blp/refdata");
            assert_eq!(params.operation, "");
            assert_eq!(
                params.request_operation.as_deref(),
                Some("ReferenceDataRequest")
            );
            assert_eq!(params.request_id.as_deref(), Some("req-123"));
            assert_eq!(params.extractor, ExtractorType::RefData);
            assert!(params.extractor_set);
            assert_eq!(
                params.securities.as_deref(),
                Some(&["IBM US Equity".to_string()][..])
            );
            assert_eq!(params.security.as_deref(), Some("IBM US Equity"));
            assert_eq!(params.fields.as_deref(), Some(&["PX_LAST".to_string()][..]));
            assert_eq!(
                params.overrides.as_deref(),
                Some(&[("EQY_FUND_CRNCY".to_string(), "USD".to_string())][..])
            );
            assert_eq!(
                params.elements.as_deref(),
                Some(&[("returnEids".to_string(), "true".to_string())][..])
            );
            assert_eq!(
                params
                    .kwargs
                    .as_ref()
                    .and_then(|values| values.get("Period")),
                Some(&"D".to_string())
            );
            assert_eq!(params.start_date.as_deref(), Some("20240101"));
            assert_eq!(params.end_date.as_deref(), Some("20240131"));
            assert_eq!(
                params.start_datetime.as_deref(),
                Some("2024-01-01T09:30:00")
            );
            assert_eq!(params.end_datetime.as_deref(), Some("2024-01-01T10:00:00"));
            assert_eq!(params.request_tz.as_deref(), Some("NY"));
            assert_eq!(params.output_tz.as_deref(), Some("UTC"));
            assert_eq!(params.event_type.as_deref(), Some("TRADE"));
            assert_eq!(
                params.event_types.as_deref(),
                Some(&["TRADE".to_string(), "BID".to_string()][..])
            );
            assert_eq!(params.interval, Some(5));
            assert_eq!(
                params.options.as_deref(),
                Some(&[("includeConditionCodes".to_string(), "true".to_string())][..])
            );
            assert_eq!(
                params
                    .field_types
                    .as_ref()
                    .and_then(|values| values.get("PX_LAST")),
                Some(&"Float64".to_string())
            );
            assert!(params.include_security_errors);
            assert!(params.return_eids);
            assert_eq!(params.on_delayed, Some(DelayedPolicy::Raise));
            assert_eq!(params.validate_fields, Some(false));
            assert_eq!(params.search_spec.as_deref(), Some("price"));
            assert_eq!(
                params.field_ids.as_deref(),
                Some(&["PX_LAST".to_string()][..])
            );
            assert_eq!(params.format.as_deref(), Some("long_typed"));
        });
    }
    #[test]
    fn dict_to_request_params_extracts_request_id() {
        Python::initialize();
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("service", "//blp/refdata").expect("service");
            dict.set_item("operation", "ReferenceDataRequest")
                .expect("operation");
            dict.set_item("request_id", "req-123").expect("request_id");

            let params = dict_to_request_params(&dict).expect("request params");
            assert_eq!(params.request_id.as_deref(), Some("req-123"));
        });
    }

    #[test]
    fn dict_to_request_params_reports_missing_and_invalid_members_as_runtime_errors() {
        Python::initialize();
        Python::attach(|py| {
            let request_error = |dict: &Bound<'_, PyDict>| {
                let error = dict_to_request_params(dict).expect_err("rejected");
                assert!(error.is_instance_of::<PyRuntimeError>(py));
                error.value(py).to_string()
            };

            let dict = PyDict::new(py);
            assert_eq!(request_error(&dict), "missing required field: service");
            dict.set_item("service", "//blp/refdata").expect("service");
            assert_eq!(request_error(&dict), "missing required field: operation");
            dict.set_item("operation", "ReferenceDataRequest")
                .expect("operation");
            dict.set_item("extractor", "nonsense").expect("extractor");
            assert_eq!(request_error(&dict), "invalid extractor type: nonsense");
            dict.del_item("extractor").expect("extractor");
            dict.set_item("on_delayed", "skip").expect("on_delayed");
            assert_eq!(
                request_error(&dict),
                "on_delayed must be warn, raise, or ignore"
            );
        });
    }

    #[test]
    fn dict_to_request_params_reads_on_delayed_like_the_engine() {
        Python::initialize();
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("service", "//blp/refdata").expect("service");
            dict.set_item("operation", "ReferenceDataRequest")
                .expect("operation");
            for (value, expected) in [
                ("Raise", Some(DelayedPolicy::Raise)),
                (" warn ", Some(DelayedPolicy::Warn)),
                ("", None),
            ] {
                dict.set_item("on_delayed", value).expect("on_delayed");
                let params = dict_to_request_params(&dict).expect("request params");
                assert_eq!(params.on_delayed, expected, "{value:?}");
            }
            dict.set_item("on_delayed", py.None()).expect("on_delayed");
            let params = dict_to_request_params(&dict).expect("request params");
            assert_eq!(params.on_delayed, None);
        });
    }

    #[test]
    fn python_subscription_reads_stop_at_the_explicit_batch_limit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update(10))).await.expect("first");
            tx.send(Ok(subscription_update(20))).await.expect("second");
            tx.send(Ok(subscription_update(30))).await.expect("third");
            let pending = PendingUpdates::default();
            let (_close_tx, mut close_rx) = watch::channel(false);

            let first = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1).await;
            let SubscriptionRead::Updates(first) = first else {
                panic!("expected first update");
            };
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].timestamp_us, 10);

            let second = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2).await;
            let SubscriptionRead::Updates(second) = second else {
                panic!("expected batched updates");
            };
            assert_eq!(
                second
                    .iter()
                    .map(|update| update.timestamp_us)
                    .collect::<Vec<_>>(),
                vec![20, 30]
            );
        });
    }

    #[test]
    fn python_batching_defers_same_version_schema_change() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let first_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("PX_LAST", 0, FieldKind::F64)],
            ));
            let second_layout = Arc::new(FieldLayout::new(
                1,
                vec![FieldMeta::new("BID", 0, FieldKind::F64)],
            ));
            let (tx, mut rx) = subscription_channel(4);
            tx.send(Ok(subscription_update_with_layout(first_layout, 10)))
                .await
                .expect("first");
            tx.send(Ok(subscription_update_with_layout(second_layout, 20)))
                .await
                .expect("second");
            let pending = PendingUpdates::default();
            let (_close_tx, mut close_rx) = watch::channel(false);

            let first = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2).await;
            let second = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 2).await;
            let SubscriptionRead::Updates(first) = first else {
                panic!("expected first schema batch");
            };
            let SubscriptionRead::Updates(second) = second else {
                panic!("expected second schema batch");
            };

            assert_eq!(first.len(), 1);
            assert_eq!(first[0].layout.fields[0].name.as_ref(), "PX_LAST");
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].layout.fields[0].name.as_ref(), "BID");
        });
    }
    #[test]
    fn draining_rejects_partial_success_when_an_unread_failure_exists() {
        let (tx, rx) = subscription_channel(2);
        tx.try_send(Ok(subscription_update(10)))
            .expect("queued update");
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "IBM US Equity".to_string(),
            detail: "consumer queue overflow".to_string(),
        });

        let error = PendingUpdates::default()
            .collect_unread(Some(rx), true)
            .expect_err("unread stream failure must reject the drain");
        match *error {
            BlpError::SubscriptionDataLoss { topic, detail } => {
                assert_eq!(topic, "IBM US Equity");
                assert_eq!(detail, "consumer queue overflow");
            }
            other => panic!("unexpected error: {other}"),
        }
    }
    #[test]
    fn draining_surfaces_an_errors_only_stream() {
        let (tx, rx) = subscription_channel(1);
        tx.fail(BlpError::SubscriptionDataLoss {
            topic: "IBM US Equity".to_string(),
            detail: "Bloomberg DATALOSS".to_string(),
        });

        let error = PendingUpdates::default()
            .collect_unread(Some(rx), true)
            .expect_err("terminal failure must reject an empty drain");
        assert!(matches!(*error, BlpError::SubscriptionDataLoss { .. }));
    }

    #[test]
    fn subscription_failure_is_delivered_once_before_eof() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (tx, mut rx) = subscription_channel(1);
            tx.fail(BlpError::SubscriptionDataLoss {
                topic: "IBM US Equity".to_string(),
                detail: "Bloomberg DATALOSS".to_string(),
            });
            let pending = PendingUpdates::default();
            let (_close_tx, mut close_rx) = watch::channel(false);

            let first = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1).await;
            assert!(matches!(
                first,
                SubscriptionRead::Error(BlpError::SubscriptionDataLoss { .. })
            ));
            let second = receive_subscription_updates(&mut rx, &pending, &mut close_rx, 1).await;
            assert!(matches!(second, SubscriptionRead::Ended));
            assert!(tx.is_closed());
        });
    }

    #[test]
    fn cancelled_subscription_read_releases_receiver_lock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (_tx, rx) = subscription_channel(1);
            let shared = Arc::new(Mutex::new(Some(rx)));
            let reader_shared = shared.clone();
            let reader = tokio::spawn(async move {
                let mut guard = reader_shared.lock().await;
                guard.as_mut().expect("receiver").recv().await
            });

            while shared.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
            reader.abort();
            let _ = reader.await;

            let guard = shared.lock().await;
            assert!(guard.is_some(), "cancellation must not take the receiver");
        });
    }

    #[test]
    fn close_signal_cannot_be_lost_before_read_starts() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let (close_signal, initial_rx) = watch::channel(false);
            drop(initial_rx);
            close_signal.send_replace(true);
            let mut close_rx = close_signal.subscribe();
            wait_for_subscription_close(&mut close_rx).await;
            assert!(*close_rx.borrow());
        });
    }
}
