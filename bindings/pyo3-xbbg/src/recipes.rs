//! PyO3 bindings for xbbg-recipes high-level Bloomberg workflows.
//!
//! Exposes all recipe functions to Python via `#[pyfunction]` wrappers.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_async::engine::RequestParams;

use xbbg_ext::transforms::fixed_income::YieldType;

use crate::{PyEngine, native_arrow::record_batch_to_arrow_record_batch};

/// Convert a RecipeError to a Python exception.
fn recipe_err(e: xbbg_recipes::RecipeError) -> PyErr {
    match e {
        xbbg_recipes::RecipeError::Engine(error) => crate::blp_async_error_to_pyerr(*error),
        xbbg_recipes::RecipeError::InvalidArgument(message) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(message)
        }
        other => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(other.to_string()),
    }
}

fn recipe_request_options(options: Option<&Bound<'_, PyDict>>) -> PyResult<RequestParams> {
    let Some(options) = options else {
        return Ok(RequestParams::default());
    };
    let params = options.copy()?;
    params.set_item("service", "//blp/refdata")?;
    params.set_item("operation", "ReferenceDataRequest")?;
    crate::request::dict_to_request_params(&params)
}

fn recipe_arrow_batch(data: &Bound<'_, PyAny>) -> PyResult<arrow_array::RecordBatch> {
    if data.hasattr("__arrow_c_array__")? {
        return Ok(data.extract::<pyo3_arrow::PyRecordBatch>()?.into_inner());
    }
    let (batches, schema) = data.extract::<pyo3_arrow::PyTable>()?.into_inner();
    data.py()
        .detach(move || xbbg_arrow::TableData { batches, schema }.combined_batch())
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
}

macro_rules! recipe_wrapper {
    (
        $(#[$meta:meta])*
        |$eng:ident|
        fn $name:ident($($arg:ident : $arg_ty:ty),* $(,)?)
        $(prepare { $($prepare:tt)* })?
        => $call:expr
    ) => {
        $(#[$meta])*
        fn $name<'py>(
            py: Python<'py>,
            engine: &PyEngine,
            $($arg: $arg_ty),*
        ) -> PyResult<Bound<'py, PyAny>> {
            let $eng = engine.engine.clone();
            $($($prepare)*)?

            crate::shutdown_safe_future(py, async move {
                let batch = $call.await.map_err(recipe_err)?;
                crate::try_attach_or_suspend(|py| record_batch_to_arrow_record_batch(py, batch)).await
            })
        }
    };
}

macro_rules! register_pyfunctions {
    ($module:expr; $($func:ident),+ $(,)?) => {{
        $( $module.add_function(wrap_pyfunction!($func, $module)?)?; )+
        Ok(())
    }};
}

// =============================================================================
// Fixed Income Recipes
// =============================================================================

recipe_wrapper!(
    /// YAS (Yield & Spread Analysis) recipe.
    ///
    /// Retrieves Bloomberg YAS data with optional yield type and pricing parameters.
    /// Returns an xbbg ArrowRecordBatch.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     tickers: Securities to query
    ///     fields: Fields to retrieve
    ///     settle_dt: Settlement date (YYYYMMDD format)
    ///     yield_type: Yield calculation type as integer (1=YTM, 2=YTC, ..., 9=YTAL)
    ///     spread: Yield spread override
    ///     yield_val: Yield value override
    ///     price: Price override
    ///     benchmark: Benchmark security for spread calculation
    ///     request_options: Normalized overrides, elements, options, types, format, timezones, EIDs, and validation controls
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, tickers, fields, settle_dt=None, yield_type=None, spread=None, yield_val=None, price=None, benchmark=None, request_options=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_yas(
        tickers: Vec<String>,
        fields: Vec<String>,
        settle_dt: Option<String>,
        yield_type: Option<u8>,
        spread: Option<f64>,
        yield_val: Option<f64>,
        price: Option<f64>,
        benchmark: Option<String>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare {
        let yt = yield_type.map(YieldType::try_from).transpose()
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        let options = recipe_request_options(request_options)?;
    }
    => xbbg_recipes::fixed_income::recipe_yas(
        &eng,
        tickers,
        fields,
        settle_dt,
        yt,
        spread,
        yield_val,
        price,
        benchmark,
        options,
    )
);

recipe_wrapper!(
    /// Find preferred stocks for a company via BQL.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     ticker: Company equity ticker (e.g., "BAC US Equity")
    ///     fields: Additional fields to retrieve (default: id, name)
    ///     request_options: Normalized request controls merged into the BQL request
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, ticker, fields=None, request_options=None))]
    |eng|
    fn recipe_preferreds(
        ticker: String,
        fields: Option<Vec<String>>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::fixed_income::recipe_preferreds(&eng, ticker, fields, options)
);

recipe_wrapper!(
    /// Find corporate bonds for a company via BQL.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     ticker: Company ticker prefix (e.g., "AAPL")
    ///     ccy: Currency filter (e.g., "USD"). None for all currencies.
    ///     fields: Additional fields to retrieve (default: id)
    ///     request_options: Normalized request controls merged into the BQL request
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, ticker, ccy=None, fields=None, request_options=None))]
    |eng|
    fn recipe_corporate_bonds(
        ticker: String,
        ccy: Option<String>,
        fields: Option<Vec<String>>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::fixed_income::recipe_corporate_bonds(
        &eng,
        ticker,
        ccy,
        fields,
        options,
    )
);

recipe_wrapper!(
    /// Bloomberg Quote Request - dealer quotes via IntradayTick.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     ticker: Security ticker (e.g., "US912810TM69 Govt")
    ///     start_datetime: Start datetime (ISO format)
    ///     end_datetime: End datetime (ISO format)
    ///     event_types: Event types to retrieve (default: ["BID", "ASK"])
    ///     include_broker_codes: Include broker/dealer codes (default: true)
    ///     request_options: Normalized include flags, request controls, and input/output timezones
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, ticker, start_datetime=None, end_datetime=None, event_types=None, include_broker_codes=true, request_options=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_bqr(
        ticker: String,
        start_datetime: Option<String>,
        end_datetime: Option<String>,
        event_types: Option<Vec<String>>,
        include_broker_codes: bool,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::fixed_income::recipe_bqr(
        &eng,
        ticker,
        start_datetime,
        end_datetime,
        event_types,
        include_broker_codes,
        options,
    )
);

// =============================================================================
// Futures / CDX Recipes
// =============================================================================

recipe_wrapper!(
    /// Resolve a generic futures ticker to a specific contract ticker.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     gen_ticker: Generic futures ticker (e.g., "ES1 Index", "CL2 Comdty")
    ///     dt: Reference date (YYYYMMDD format)
    ///     freq: Roll frequency ("M" monthly, "Q"/"QE" quarterly)
    ///     request_options: Normalized request controls; internal data retains the recipe's required shape
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, gen_ticker, dt, freq=None, request_options=None))]
    |eng|
    fn recipe_fut_ticker(
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::futures::recipe_fut_ticker(&eng, gen_ticker, dt, freq, options)
);

recipe_wrapper!(
    /// Resolve the most active futures contract around a reference date.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     gen_ticker: Generic futures ticker (e.g., "ES1 Index")
    ///     dt: Reference date (YYYYMMDD format)
    ///     freq: Roll frequency ("M" monthly, "Q"/"QE" quarterly)
    ///     request_options: Normalized request controls; internal data retains the recipe's required shape
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, gen_ticker, dt, freq=None, request_options=None))]
    |eng|
    fn recipe_active_futures(
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::futures::recipe_active_futures(&eng, gen_ticker, dt, freq, options)
);

recipe_wrapper!(
    /// Build a futures chain table with contract metadata, mid, and annualized carry.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     gen_ticker: Generic futures ticker (e.g., "ES1 Index")
    ///     asof: Optional chain date (YYYYMMDD format)
    ///     chain_field: Bloomberg bulk chain field (default FUT_CHAIN_LAST_TRADE_DATES)
    ///     fields: Contract metadata fields to retrieve
    ///     max_contracts: Optional positive row limit
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, gen_ticker, asof=None, chain_field=None, fields=None, max_contracts=None))]
    |eng|
    fn recipe_futures_curve(
        gen_ticker: String,
        asof: Option<String>,
        chain_field: Option<String>,
        fields: Option<Vec<String>>,
        max_contracts: Option<i32>,
    ) => xbbg_recipes::futures::recipe_futures_curve(
        &eng,
        gen_ticker,
        asof,
        chain_field,
        fields,
        max_contracts,
    )
);

recipe_wrapper!(
    /// Resolve a generic CDX ticker to the series that applies on a date.
    ///
    /// The series is the highest one whose Bloomberg
    /// `CDS_FIRST_ACCRUAL_START_DATE` is on or before `dt`, so the result never
    /// moves backwards as `dt` advances. `Vn` is the latest version Bloomberg
    /// reports for that series; Bloomberg publishes no as-of version.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     gen_ticker: Generic CDX ticker (e.g., "CDX IG CDSI GEN 5Y Corp")
    ///     dt: Reference date (YYYYMMDD format)
    ///     versionless: Return the versionless ticker form (default: false)
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, gen_ticker, dt, versionless=false))]
    |eng|
    fn recipe_cdx_ticker(
        gen_ticker: String,
        dt: String,
        versionless: bool,
    ) => xbbg_recipes::futures::recipe_cdx_ticker_with_options(&eng, gen_ticker, dt, versionless)
);

recipe_wrapper!(
    /// Resolve the latest CDX series that had started and traded by a date.
    ///
    /// Matches `recipe_cdx_ticker` except between a roll and the new series'
    /// first print, when the preceding series is still the traded one.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     gen_ticker: Generic CDX ticker (e.g., "CDX IG CDSI GEN 5Y Corp")
    ///     dt: Reference date (YYYYMMDD format)
    ///     lookback_days: Minimum activity window in days (default: 10). The
    ///         window always reaches back to the series' first accrual date.
    ///     versionless: Return the versionless ticker form (default: false)
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, gen_ticker, dt, lookback_days=None, versionless=false))]
    |eng|
    fn recipe_active_cdx(
        gen_ticker: String,
        dt: String,
        lookback_days: Option<i32>,
        versionless: bool,
    ) => xbbg_recipes::futures::recipe_active_cdx_with_options(
        &eng,
        gen_ticker,
        dt,
        lookback_days,
        versionless,
    )
);

// =============================================================================
// Historical Recipes
// =============================================================================

recipe_wrapper!(
    /// Fetch dividend history for securities.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     tickers: Securities to query
    ///     start_date: Start date (YYYYMMDD format)
    ///     end_date: End date (YYYYMMDD format)
    ///     dvd_type: Dividend alias or raw Bloomberg bulk field
    ///     request_options: Normalized request controls; raw is ignored and bulk format is fixed
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, tickers, start_date, end_date, dvd_type=None, request_options=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_dividend(
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dvd_type: Option<String>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::historical::recipe_dividend(&eng, tickers, dvd_type, start_date, end_date, options)
);

recipe_wrapper!(
    /// Compute trailing realized dividend amount and trailing dividend yield.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     tickers: Securities to query
    ///     start_date: Start date (YYYYMMDD format)
    ///     end_date: End date (YYYYMMDD format)
    ///     dividend_types: Dividend event type filter
    ///     window_days: Rolling trailing window in calendar days
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, tickers, start_date, end_date, dividend_types=None, window_days=None))]
    |eng|
    fn recipe_dividend_yield(
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dividend_types: Option<Vec<String>>,
        window_days: Option<i32>,
    ) => xbbg_recipes::historical::recipe_dividend_yield(
        &eng,
        tickers,
        start_date,
        end_date,
        dividend_types,
        window_days,
    )
);

recipe_wrapper!(
    /// Fetch trading volume and turnover for securities.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     tickers: Securities to query
    ///     start_date: Start date (YYYYMMDD format)
    ///     end_date: End date (YYYYMMDD format)
    ///     ccy: Currency for conversion. None for local currency.
    ///     factor: Division factor (e.g., 1_000_000.0 for millions)
    ///     request_options: Normalized request controls, adjustment shorthand, and supported output format
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, tickers, start_date, end_date, ccy=None, factor=None, request_options=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_turnover(
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        ccy: Option<String>,
        factor: Option<f64>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::historical::recipe_turnover(
        &eng,
        tickers,
        start_date,
        end_date,
        ccy,
        factor,
        options,
    )
);

recipe_wrapper!(
    /// Fetch ETF constituent holdings via BQL.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     etf_ticker: ETF ticker (e.g., "SPY US Equity")
    ///     fields: Additional fields beyond defaults (id_isin, weights, id().position)
    ///     request_options: Normalized request controls merged into the BQL request
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, etf_ticker, fields=None, request_options=None))]
    |eng|
    fn recipe_etf_holdings(
        etf_ticker: String,
        fields: Option<Vec<String>>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::historical::recipe_etf_holdings(&eng, etf_ticker, fields, options)
);

recipe_wrapper!(
    /// Fetch statement or geography/product earnings and hierarchical percentages.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     tickers: Securities to query
    ///     by: Geo/Product breakdown or Q/A period granularity
    ///     typ: Statement type IS/BS/CF or a geography/product metric
    ///     ccy: Currency override
    ///     level: Optional hierarchy filter (1 or 2)
    ///     year: Fiscal year override
    ///     periods: Number of periods
    ///     request_options: Normalized request controls; raw is ignored and bulk format is fixed
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
    #[pyfunction]
    #[pyo3(signature = (engine, tickers, by=None, typ="Revenue".to_string(), ccy=None, level=None, year=None, periods=None, request_options=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_earning(
        tickers: Vec<String>,
        by: Option<String>,
        typ: String,
        ccy: Option<String>,
        level: Option<i32>,
        year: Option<i32>,
        periods: Option<i32>,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare { let options = recipe_request_options(request_options)?; }
    => xbbg_recipes::historical::recipe_earning(
        &eng, tickers, by, typ, ccy, level, year, periods, options,
    )
);

// =============================================================================
// Volatility / Index / Identifier Recipes
// =============================================================================

recipe_wrapper!(
    /// Build a tidy historical implied volatility surface.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, tickers, start_date, end_date, presets=None, field_specs=None, as_decimal=Some(true), include_derived=Some(false), risk_free_rate=None, dividend_yield_field=None))]
    #[allow(clippy::too_many_arguments)]
    |eng|
    fn recipe_vol_surface(
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        presets: Option<Vec<String>>,
        field_specs: Option<Vec<String>>,
        as_decimal: Option<bool>,
        include_derived: Option<bool>,
        risk_free_rate: Option<f64>,
        dividend_yield_field: Option<String>,
    ) => xbbg_recipes::volatility::recipe_vol_surface(
        &eng,
        tickers,
        start_date,
        end_date,
        presets,
        field_specs,
        as_decimal,
        include_derived,
        risk_free_rate,
        dividend_yield_field,
    )
);

recipe_wrapper!(
    /// Fetch normalized index members from Bloomberg bulk constituent fields.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, index, field=None, asof=None))]
    |eng|
    fn recipe_index_members(
        index: String,
        field: Option<String>,
        asof: Option<String>,
    ) => xbbg_recipes::indices::recipe_index_members(&eng, index, field, asof)
);

recipe_wrapper!(
    /// Resolve equity ISINs through Bloomberg `/ISIN/<id>` lookups.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, isins))]
    |eng|
    fn recipe_resolve_isins(
        isins: Vec<String>,
    ) => xbbg_recipes::identifiers::recipe_resolve_isins(&eng, isins)
);

recipe_wrapper!(
    /// Resolve bond ISINs to issuer equity ISINs.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, bond_isins))]
    |eng|
    fn recipe_issuer_isins(
        bond_isins: Vec<String>,
    ) => xbbg_recipes::identifiers::recipe_issuer_isins(&eng, bond_isins)
);

recipe_wrapper!(
    /// Resolve ETF NAV / iNAV relationship targets via `ETF_NAV_TICKER` /
    /// `ETF_INAV_TICKER`.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, etfs))]
    |eng|
    fn recipe_etf_nav_relationships(
        etfs: Vec<String>,
    ) => xbbg_recipes::etf::recipe_etf_nav_relationships(&eng, etfs)
);

recipe_wrapper!(
    /// Fetch current ETF NAV / iNAV levels with `FUND_NET_ASSET_VAL`
    /// fallback for missing daily NAV relationships.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, etfs))]
    |eng|
    fn recipe_etf_nav_snapshot(
        etfs: Vec<String>,
    ) => xbbg_recipes::etf::recipe_etf_nav_snapshot(&eng, etfs)
);

recipe_wrapper!(
    /// Fetch daily ETF NAV / iNAV history between two dates.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     etfs: ETF securities to query
    ///     start_date: Start date (YYYYMMDD format)
    ///     end_date: End date (YYYYMMDD format)
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, etfs, start_date, end_date))]
    |eng|
    fn recipe_etf_nav_history(
        etfs: Vec<String>,
        start_date: String,
        end_date: String,
    ) => xbbg_recipes::etf::recipe_etf_nav_history(&eng, etfs, start_date, end_date)
);

// =============================================================================
// Auction Recipes
// =============================================================================

recipe_wrapper!(
    /// Resolve and validate primary exchange-auction venues in input order.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
    #[pyfunction]
    #[pyo3(signature = (engine, securities, pcs_overrides=None))]
    |eng|
    fn recipe_resolve_venues(
        securities: Vec<String>,
        pcs_overrides: Option<HashMap<String, String>>,
    ) => xbbg_recipes::recipe_resolve_venues(
        &eng,
        securities,
        pcs_overrides.unwrap_or_default(),
    )
);

recipe_wrapper!(
    /// Fetch auction fields only from validated primary exchange venues.
    ///
    /// None or an empty field list selects the default auction field groups.
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
    #[pyfunction]
    #[pyo3(signature = (engine, securities, fields=None, pcs_overrides=None))]
    |eng|
    fn recipe_auction_snapshot(
        securities: Vec<String>,
        fields: Option<Vec<String>>,
        pcs_overrides: Option<HashMap<String, String>>,
    ) => xbbg_recipes::recipe_auction_snapshot(
        &eng,
        securities,
        fields.unwrap_or_default(),
        pcs_overrides.unwrap_or_default(),
    )
);

// =============================================================================
// Currency Recipes
// =============================================================================

recipe_wrapper!(
    /// Convert long or wide Arrow historical values into a target currency.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     data: Arrow array/stream-compatible historical data
    ///     target_ccy: Target currency; local preserves the input
    ///     start_date: Fallback query start date
    ///     end_date: Fallback query end date
    ///     request_options: Normalized request controls, scoped to each internal request's securities
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
    #[pyfunction]
    #[pyo3(signature = (engine, data, target_ccy="USD".to_string(), start_date=String::new(), end_date=String::new(), request_options=None))]
    |eng|
    fn recipe_adjust_ccy(
        data: &Bound<'_, PyAny>,
        target_ccy: String,
        start_date: String,
        end_date: String,
        request_options: Option<&Bound<'_, PyDict>>,
    )
    prepare {
        let data = recipe_arrow_batch(data)?;
        let options = recipe_request_options(request_options)?;
    }
    => xbbg_recipes::currency::recipe_adjust_ccy(
        &eng, data, target_ccy, start_date, end_date, options,
    )
);

recipe_wrapper!(
    /// Fetch historical prices with currency conversion.
    ///
    /// Args:
    ///     engine: Bloomberg engine instance
    ///     ticker: Security ticker
    ///     target_ccy: Target currency (e.g., "USD", "EUR")
    ///     start_date: Start date (YYYYMMDD format)
    ///     end_date: End date (YYYYMMDD format)
    #[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
    #[pyo3(signature = (engine, ticker, target_ccy, start_date, end_date))]
    |eng|
    fn recipe_currency_conversion(
        ticker: String,
        target_ccy: String,
        start_date: String,
        end_date: String,
    ) => xbbg_recipes::currency::recipe_currency_conversion(
        &eng,
        ticker,
        target_ccy,
        start_date,
        end_date,
    )
);

/// Register all recipe functions with the Python module.
pub(crate) fn register_recipes_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(
        m;
        recipe_yas,
        recipe_preferreds,
        recipe_corporate_bonds,
        recipe_bqr,
        recipe_fut_ticker,
        recipe_active_futures,
        recipe_futures_curve,
        recipe_cdx_ticker,
        recipe_active_cdx,
        recipe_dividend,
        recipe_dividend_yield,
        recipe_turnover,
        recipe_etf_holdings,
        recipe_earning,
        recipe_vol_surface,
        recipe_index_members,
        recipe_resolve_isins,
        recipe_issuer_isins,
        recipe_etf_nav_relationships,
        recipe_etf_nav_snapshot,
        recipe_etf_nav_history,
        recipe_resolve_venues,
        recipe_auction_snapshot,
        recipe_currency_conversion,
        recipe_adjust_ccy,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlpInternalError, BlpLimitError, BlpRequestError, BlpSessionError,
        BlpSubscriptionDataLossError, BlpTimeoutError, BlpValidationError,
    };
    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use xbbg_async::BlpAsyncError;
    use xbbg_core::BlpError;
    use xbbg_core::errors::CorrelationContext;
    use xbbg_recipes::RecipeError;

    #[test]
    fn recipe_validation_errors_distinguish_arguments_from_engine_configuration() {
        Python::initialize();
        Python::attach(|py| {
            let argument = recipe_err(RecipeError::InvalidArgument("fields are required".into()));
            assert!(argument.is_instance_of::<PyValueError>(py));

            let engine = recipe_err(RecipeError::Engine(Box::new(BlpAsyncError::ConfigError {
                detail: "invalid recipe configuration".into(),
            })));
            assert!(engine.is_instance_of::<BlpValidationError>(py));
            assert!(!engine.is_instance_of::<PyValueError>(py));
            assert!(
                engine
                    .value(py)
                    .to_string()
                    .contains("invalid recipe configuration")
            );
        });
    }

    #[test]
    fn recipe_core_timeout_preserves_the_typed_exception() {
        Python::initialize();
        Python::attach(|py| {
            let error = recipe_err(RecipeError::Engine(Box::new(BlpError::Timeout.into())));
            assert!(error.is_instance_of::<BlpTimeoutError>(py));
            assert_eq!(error.value(py).to_string(), "Request timed out");
        });
    }

    #[test]
    fn recipe_request_failures_preserve_context_and_limit_classification() {
        Python::initialize();
        Python::attach(|py| {
            for (label, is_limit) in [("category=BAD_ARGS", false), ("category=LIMIT", true)] {
                let error = recipe_err(RecipeError::Engine(Box::new(BlpAsyncError::Blp(
                    BlpError::RequestFailure {
                        service: "//blp/refdata".into(),
                        operation: Some("ReferenceDataRequest".into()),
                        cid: Some(CorrelationContext::U64(42)),
                        label: Some(label.into()),
                        request_id: Some("synthetic-request".into()),
                        source: None,
                    },
                ))));
                assert!(error.is_instance_of::<BlpRequestError>(py));
                assert_eq!(error.is_instance_of::<BlpLimitError>(py), is_limit);
                let message = error.value(py).to_string();
                for context in [
                    "//blp/refdata",
                    "ReferenceDataRequest",
                    "42",
                    "synthetic-request",
                    label,
                ] {
                    assert!(
                        message.contains(context),
                        "missing request context: {context}"
                    );
                }
            }
        });
    }

    #[test]
    fn recipe_engine_failures_preserve_session_and_internal_types() {
        Python::initialize();
        Python::attach(|py| {
            let session = recipe_err(RecipeError::Engine(Box::new(
                BlpAsyncError::AllWorkersDown { pool_size: 2 },
            )));
            assert!(session.is_instance_of::<BlpSessionError>(py));
            assert_eq!(
                session.value(py).to_string(),
                "all 2 request workers are dead — no healthy worker available",
            );

            for internal in [
                BlpAsyncError::ChannelClosed,
                BlpAsyncError::Internal("synthetic failure".into()),
                BlpError::Internal {
                    detail: "session connection dropped (worker=2)".into(),
                }
                .into(),
            ] {
                let error = recipe_err(RecipeError::Engine(Box::new(internal)));
                assert!(error.is_instance_of::<BlpInternalError>(py));
                assert!(!error.is_instance_of::<BlpSessionError>(py));
            }
        });
    }

    #[test]
    fn recipe_data_loss_preserves_structured_exception_attributes() {
        Python::initialize();
        Python::attach(|py| {
            let error = recipe_err(RecipeError::Engine(Box::new(BlpAsyncError::Blp(
                BlpError::SubscriptionDataLoss {
                    topic: "IBM US Equity".into(),
                    detail: "synthetic data loss".into(),
                },
            ))));
            assert!(error.is_instance_of::<BlpSubscriptionDataLossError>(py));
            for (attribute, expected) in [
                ("topic", "IBM US Equity"),
                ("detail", "synthetic data loss"),
            ] {
                assert_eq!(
                    error
                        .value(py)
                        .getattr(attribute)
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    expected
                );
            }
        });
    }

    #[test]
    fn non_engine_recipe_errors_keep_runtime_error_mapping() {
        Python::initialize();
        Python::attach(|py| {
            let error = recipe_err(RecipeError::Other("synthetic recipe failure".into()));
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(
                error
                    .value(py)
                    .to_string()
                    .contains("synthetic recipe failure")
            );
        });
    }
}
