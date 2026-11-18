//! Python hooks for the shared native teardown (`xbbg_host::teardown`).
//!
//! Dropping the last owner of a native engine signals shutdown, joins the
//! engine's Tokio runtime and stops its SDK sessions, which can take unbounded
//! time, and the last owner may be a Python object deallocated on an attached
//! thread. Engines and subscription consumers in this crate therefore live in
//! the host's `DeferredDrop` and `HostedDrop`, which hand the final drop to a
//! named native thread.
//!
//! Invariant: deferred values contain no Python objects, so teardown threads
//! never attach to the interpreter.
//!
//! Interpreter exit waits for outstanding teardown twice: `_engine._atexit_cleanup`
//! calls [`_wait_native_teardown`] after releasing the global engine, and a
//! `Py_AtExit` callback waits again after finalization, whose deallocations can
//! release further engines.

use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_async::engine::Engine;
use xbbg_async::subscription_consumer::SubscriptionConsumer;
use xbbg_host::teardown::{DeferredDrop, HostedDrop, wait_idle};
use xbbg_log::warn;

/// Engine shared by a `PyEngine` and its in-flight futures; whichever releases
/// it last only hands it to a teardown thread.
pub(crate) type EngineOwner = DeferredDrop<Engine>;

/// Subscription consumer shared by a `PySubscription` and its futures.
///
/// While the engine lives, dropping the consumer only schedules cleanup on the
/// engine runtime. Once the engine is released, the same drop could stop SDK
/// sessions in place, so it is deferred instead.
pub(crate) type SubscriptionOwner = HostedDrop<SubscriptionConsumer, EngineOwner>;

/// Wait until no released engine or subscription is still in native teardown
/// (stopping Bloomberg sessions, joining runtimes).
///
/// Returns at once when nothing is pending and releases the interpreter while
/// waiting. Interpreter exit already waits; other callers rarely need this.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn _wait_native_teardown(py: Python<'_>) {
    py.detach(wait_idle);
}

/// Waits again after finalization has deallocated the remaining objects.
///
/// `Py_FinalizeEx` calls this once the interpreter is gone, with no thread
/// state, so it must never touch Python.
extern "C" fn wait_idle_after_finalization() {
    wait_idle();
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_wait_native_teardown, m)?)?;
    // SAFETY: module initialization runs attached. The callback lives in this
    // extension, which CPython never unloads, and never touches Python.
    if unsafe { pyo3::ffi::Py_AtExit(Some(wait_idle_after_finalization)) } != 0 {
        warn!("Py_AtExit is full; native teardown released during finalization is not awaited");
    }
    Ok(())
}
