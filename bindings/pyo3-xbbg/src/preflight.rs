//! Auction and ETF iNAV subscription preflight, shared with every host through
//! `xbbg_host::preflight`.
//!
//! The Python helpers (`xbbg.ext.auction`, `xbbg.ext.etf`) keep the recipe
//! calls, the stream wrappers and their option surface; these functions are the
//! checks around them. Each takes the Arrow table of a native recipe call (an
//! `ArrowTable`, or any Arrow C-array or stream producer) and reports a failed
//! check as `ValueError` carrying the host's message. `xbbg.ext.auction`
//! re-raises it as `BlpValidationError`, `xbbg.ext.etf` lets it through.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use serde_json::Value;
use xbbg_host::errors::HostError;
use xbbg_host::preflight::{
    AuctionSubscribeArgs, EtfInavSubscribeArgs, plan_auction, plan_etf_inav,
    validated_auction_topics, validated_inav_tickers,
};
use xbbg_host::subscribe::StreamOptionsSpec;

use crate::recipes::recipe_arrow_batch;

fn host_error(error: HostError) -> PyErr {
    PyValueError::new_err(error.message)
}

/// Check auction inputs before venue resolution and return them de-duplicated
/// in first-seen order.
///
/// `aliases_given` is whether the caller passed an `aliases` option at all:
/// auction aliases are always derived from the inputs, so any value is rejected.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_plan_auction(securities: Vec<String>, aliases_given: bool) -> PyResult<Vec<String>> {
    let options = StreamOptionsSpec {
        // Any non-null `aliases` is rejected; an empty map is the smallest one
        // that also passes the host's type check.
        aliases: aliases_given.then(|| Value::Object(serde_json::Map::new())),
        ..StreamOptionsSpec::default()
    };
    plan_auction(AuctionSubscribeArgs {
        securities,
        pcs_overrides: None,
        stream_auction: false,
        options,
    })
    .map(|plan| plan.inputs)
    .map_err(host_error)
}

/// Validate a venue-resolution table against the planned inputs.
///
/// Returns `{venue_topic: input}` in input order: the trimmed venue topics to
/// subscribe and the labels they carry back to the inputs.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_validated_auction_venues<'py>(
    py: Python<'py>,
    inputs: Vec<String>,
    table: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let batch = recipe_arrow_batch(table)?;
    let resolved = validated_auction_topics(&inputs, &batch).map_err(host_error)?;
    let aliases = PyDict::new(py);
    for (topic, input) in resolved.aliases {
        aliases.set_item(topic, input)?;
    }
    Ok(aliases)
}

/// Check ETF inputs before relationship resolution and return them trimmed, in
/// input order.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_plan_etf_inav(etfs: Vec<String>) -> PyResult<Vec<String>> {
    plan_etf_inav(EtfInavSubscribeArgs {
        tickers: etfs,
        options: StreamOptionsSpec::default(),
    })
    .map(|plan| plan.etfs)
    .map_err(host_error)
}

/// Validate an ETF NAV relationship table against the planned ETFs and return
/// one iNAV ticker per ETF.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_validated_inav_tickers(
    etfs: Vec<String>,
    table: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
    let batch = recipe_arrow_batch(table)?;
    validated_inav_tickers(&etfs, &batch).map_err(host_error)
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ext_plan_auction, m)?)?;
    m.add_function(wrap_pyfunction!(ext_validated_auction_venues, m)?)?;
    m.add_function(wrap_pyfunction!(ext_plan_etf_inav, m)?)?;
    m.add_function(wrap_pyfunction!(ext_validated_inav_tickers, m)?)?;
    Ok(())
}
