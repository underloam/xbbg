use std::collections::HashMap;

use chrono::NaiveDate;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::{derive::*, impl_stub_type};
use xbbg_ext::markets::{self, sessions};
use xbbg_ext::{ExchangeInfo, OverridePatch, SessionWindows};

/// A plain-data representation of [`sessions::MarketRule`] that pyo3 can
/// convert to a Python dict automatically — no GIL-bound PyObject needed.
#[derive(Clone, Debug, IntoPyObject)]
struct MarketRuleDict {
    pre_minutes: i32,
    post_minutes: i32,
    lunch_start_min: Option<i32>,
    lunch_end_min: Option<i32>,
    is_continuous: bool,
}

#[derive(Clone, Debug, IntoPyObject)]
struct ExchangeInfoDict {
    ticker: String,
    mic: Option<String>,
    exch_code: Option<String>,
    timezone: String,
    utc_offset: Option<f64>,
    source: String,
    day: Option<(String, String)>,
    allday: Option<(String, String)>,
    pre: Option<(String, String)>,
    post: Option<(String, String)>,
    am: Option<(String, String)>,
    pm: Option<(String, String)>,
}

#[cfg(feature = "stub-gen")]
impl_stub_type!(MarketRuleDict = pyo3::types::PyAny);
#[cfg(feature = "stub-gen")]
impl_stub_type!(ExchangeInfoDict = pyo3::types::PyAny);

/// Derive session windows from regular trading hours.
///
/// Returns dict with keys: day, allday, pre, post, am, pm.
/// Each value is a ``(start, end)`` tuple of ``"HH:MM"`` strings.
/// Keys are omitted when the session does not apply.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_derive_sessions(
    day_start: &str,
    day_end: &str,
    mic: Option<&str>,
    exch_code: Option<&str>,
) -> HashMap<String, (String, String)> {
    let sw = sessions::derive_sessions(day_start, day_end, mic, exch_code);
    let mut result = HashMap::new();
    if let Some(v) = sw.day {
        result.insert("day".to_string(), v);
    }
    if let Some(v) = sw.allday {
        result.insert("allday".to_string(), v);
    }
    if let Some(v) = sw.pre {
        result.insert("pre".to_string(), v);
    }
    if let Some(v) = sw.post {
        result.insert("post".to_string(), v);
    }
    if let Some(v) = sw.am {
        result.insert("am".to_string(), v);
    }
    if let Some(v) = sw.pm {
        result.insert("pm".to_string(), v);
    }
    result
}

/// Look up market rule by MIC code or Bloomberg exchange code.
///
/// Returns dict with rule fields, or ``None`` if no rule matches.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_market_rule(mic: Option<&str>, exch_code: Option<&str>) -> Option<MarketRuleDict> {
    let rule = sessions::get_market_rule(mic, exch_code)?;
    Some(MarketRuleDict {
        pre_minutes: rule.pre_minutes,
        post_minutes: rule.post_minutes,
        lunch_start_min: rule.lunch_start_min,
        lunch_end_min: rule.lunch_end_min,
        is_continuous: rule.is_continuous,
    })
}

/// Infer timezone from country ISO code.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_infer_timezone(country_iso: &str) -> Option<String> {
    sessions::infer_timezone_from_country(country_iso).map(String::from)
}

/// Set a runtime exchange override patch for a ticker.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (
    ticker,
    timezone=None,
    mic=None,
    exch_code=None,
    day=None,
    allday=None,
    pre=None,
    post=None,
    am=None,
    pm=None
))]
#[allow(clippy::too_many_arguments)]
fn ext_set_exchange_override(
    ticker: &str,
    timezone: Option<&str>,
    mic: Option<&str>,
    exch_code: Option<&str>,
    day: Option<(String, String)>,
    allday: Option<(String, String)>,
    pre: Option<(String, String)>,
    post: Option<(String, String)>,
    am: Option<(String, String)>,
    pm: Option<(String, String)>,
) -> PyResult<()> {
    let sessions = if day.is_some()
        || allday.is_some()
        || pre.is_some()
        || post.is_some()
        || am.is_some()
        || pm.is_some()
    {
        Some(SessionWindows {
            day,
            allday,
            pre,
            post,
            am,
            pm,
        })
    } else {
        None
    };

    let patch = OverridePatch {
        timezone: timezone.map(str::to_string),
        mic: mic.map(str::to_string),
        exch_code: exch_code.map(str::to_string),
        sessions,
    };

    markets::set_exchange_override(ticker, patch).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get runtime override for a ticker.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_get_exchange_override(ticker: &str) -> PyResult<Option<ExchangeInfoDict>> {
    markets::get_exchange_override(ticker)
        .map(|info| info.map(to_exchange_info_dict))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Clear one override (or all when ticker is None).
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (ticker=None))]
fn ext_clear_exchange_override(ticker: Option<&str>) -> PyResult<()> {
    markets::clear_exchange_override(ticker).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// List all runtime overrides.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_list_exchange_overrides() -> PyResult<HashMap<String, ExchangeInfoDict>> {
    markets::list_exchange_overrides()
        .map_err(|e| PyValueError::new_err(e.to_string()))
        .map(|overrides| {
            overrides
                .into_iter()
                .map(|(k, v)| (k, to_exchange_info_dict(v)))
                .collect()
        })
}

/// Convert local exchange session times to UTC ISO timestamps.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn ext_session_times_to_utc(
    start_time: &str,
    end_time: &str,
    exchange_tz: &str,
    date: &str,
) -> PyResult<(String, String)> {
    let dt = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| {
        PyValueError::new_err(format!("invalid date '{date}', expected YYYY-MM-DD"))
    })?;

    let (start, end) = markets::session_times_to_utc(start_time, end_time, exchange_tz, dt)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((
        start.format("%Y-%m-%dT%H:%M:%S").to_string(),
        end.format("%Y-%m-%dT%H:%M:%S").to_string(),
    ))
}

fn to_exchange_info_dict(info: ExchangeInfo) -> ExchangeInfoDict {
    ExchangeInfoDict {
        ticker: info.ticker,
        mic: info.mic,
        exch_code: info.exch_code,
        timezone: info.timezone,
        utc_offset: info.utc_offset,
        source: info.source.as_str().to_string(),
        day: info.sessions.day,
        allday: info.sessions.allday,
        pre: info.sessions.pre,
        post: info.sessions.post,
        am: info.sessions.am,
        pm: info.sessions.pm,
    }
}

/// Register all markets functions on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ext_derive_sessions, m)?)?;
    m.add_function(wrap_pyfunction!(ext_get_market_rule, m)?)?;
    m.add_function(wrap_pyfunction!(ext_infer_timezone, m)?)?;
    m.add_function(wrap_pyfunction!(ext_set_exchange_override, m)?)?;
    m.add_function(wrap_pyfunction!(ext_get_exchange_override, m)?)?;
    m.add_function(wrap_pyfunction!(ext_clear_exchange_override, m)?)?;
    m.add_function(wrap_pyfunction!(ext_list_exchange_overrides, m)?)?;
    m.add_function(wrap_pyfunction!(ext_session_times_to_utc, m)?)?;
    Ok(())
}
