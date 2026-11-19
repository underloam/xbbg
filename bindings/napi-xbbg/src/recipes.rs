//! Recipe methods on `JsEngine`, sharing `xbbg_host::recipes` dispatch and validation.

use std::collections::HashMap;

use napi_derive::napi;
use xbbg_host::recipes::RecipeCall;

use crate::arrow_zero_copy::NativeArrowBatch;
use crate::engine::{JsEngine, to_native_record_batch};
use crate::errors::{host_error_to_napi, recipe_error_to_napi};

#[napi]
impl JsEngine {
    /// Unified host-neutral recipe dispatch (`xbbg_host::recipes::RecipeCall`).
    #[napi]
    pub async fn recipe(
        &self,
        method: String,
        args_json: String,
    ) -> napi::Result<NativeArrowBatch> {
        let call = RecipeCall::parse(&method, &args_json).map_err(host_error_to_napi)?;
        let batch = call.run(&self.engine).await.map_err(host_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_bqr(
        &self,
        ticker: String,
        start_datetime: Option<String>,
        end_datetime: Option<String>,
        event_types: Option<Vec<String>>,
        include_broker_codes: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_bqr(
            &engine,
            ticker,
            start_datetime,
            end_datetime,
            event_types,
            include_broker_codes.unwrap_or(true),
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn recipe_yas(
        &self,
        tickers: Vec<String>,
        fields: Vec<String>,
        settle_dt: Option<String>,
        yield_type: Option<u8>,
        spread: Option<f64>,
        yield_val: Option<f64>,
        price: Option<f64>,
        benchmark: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let yt = yield_type
            .map(xbbg_ext::transforms::fixed_income::YieldType::try_from)
            .transpose()
            .map_err(|error| napi::Error::new(napi::Status::InvalidArg, error.to_string()))?;
        let batch = xbbg_recipes::fixed_income::recipe_yas(
            &engine,
            tickers,
            fields,
            settle_dt,
            yt,
            spread,
            yield_val,
            price,
            benchmark,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_preferreds(
        &self,
        equity_ticker: String,
        fields: Option<Vec<String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_preferreds(
            &engine,
            equity_ticker,
            fields,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_corporate_bonds(
        &self,
        ticker: String,
        ccy: Option<String>,
        fields: Option<Vec<String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::fixed_income::recipe_corporate_bonds(
            &engine,
            ticker,
            ccy,
            fields,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_fut_ticker(
        &self,
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_fut_ticker(
            &engine,
            gen_ticker,
            dt,
            freq,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_active_futures(
        &self,
        gen_ticker: String,
        dt: String,
        freq: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_active_futures(
            &engine,
            gen_ticker,
            dt,
            freq,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_futures_curve(
        &self,
        gen_ticker: String,
        asof: Option<String>,
        chain_field: Option<String>,
        fields: Option<Vec<String>>,
        max_contracts: Option<i32>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_futures_curve(
            &engine,
            gen_ticker,
            asof,
            chain_field,
            fields,
            max_contracts,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_cdx_ticker(
        &self,
        gen_ticker: String,
        dt: String,
        versionless: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_cdx_ticker_with_options(
            &engine,
            gen_ticker,
            dt,
            versionless.unwrap_or(false),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_active_cdx(
        &self,
        gen_ticker: String,
        dt: String,
        lookback_days: Option<i32>,
        versionless: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::futures::recipe_active_cdx_with_options(
            &engine,
            gen_ticker,
            dt,
            lookback_days,
            versionless.unwrap_or(false),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_dividend(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dvd_type: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_dividend(
            &engine,
            tickers,
            dvd_type,
            start_date,
            end_date,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_dividend_yield(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        dividend_types: Option<Vec<String>>,
        window_days: Option<i32>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_dividend_yield(
            &engine,
            tickers,
            start_date,
            end_date,
            dividend_types,
            window_days,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_turnover(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        ccy: Option<String>,
        factor: Option<f64>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_turnover(
            &engine,
            tickers,
            start_date,
            end_date,
            ccy,
            factor,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_holdings(
        &self,
        etf_ticker: String,
        fields: Option<Vec<String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::historical::recipe_etf_holdings(
            &engine,
            etf_ticker,
            fields,
            Default::default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn recipe_vol_surface(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
        presets: Option<Vec<String>>,
        field_specs: Option<Vec<String>>,
        as_decimal: Option<bool>,
        include_derived: Option<bool>,
        risk_free_rate: Option<f64>,
        dividend_yield_field: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::volatility::recipe_vol_surface(
            &engine,
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
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_index_members(
        &self,
        index: String,
        field: Option<String>,
        asof: Option<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::indices::recipe_index_members(&engine, index, field, asof)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_resolve_isins(&self, isins: Vec<String>) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::identifiers::recipe_resolve_isins(&engine, isins)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    /// Resolve each input to its validated primary auction venue.
    #[napi]
    pub async fn recipe_resolve_venues(
        &self,
        securities: Vec<String>,
        pcs_overrides: Option<HashMap<String, String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let batch = xbbg_recipes::recipe_resolve_venues(
            &self.engine,
            securities,
            pcs_overrides.unwrap_or_default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    /// Request auction fields only from validated primary venues.
    #[napi]
    pub async fn recipe_auction_snapshot(
        &self,
        securities: Vec<String>,
        fields: Option<Vec<String>>,
        pcs_overrides: Option<HashMap<String, String>>,
    ) -> napi::Result<NativeArrowBatch> {
        let batch = xbbg_recipes::recipe_auction_snapshot(
            &self.engine,
            securities,
            fields.unwrap_or_default(),
            pcs_overrides.unwrap_or_default(),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    /// Report per security whether this session receives delayed data, with
    /// feed EIDs and optional identity entitlement checks.
    #[napi]
    pub async fn recipe_delay_status(
        &self,
        securities: Vec<String>,
        check_identity: Option<bool>,
    ) -> napi::Result<NativeArrowBatch> {
        let batch = xbbg_recipes::recipe_delay_status(
            &self.engine,
            securities,
            check_identity.unwrap_or(false),
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_issuer_isins(
        &self,
        bond_isins: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::identifiers::recipe_issuer_isins(&engine, bond_isins)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_relationships(
        &self,
        tickers: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::etf::recipe_etf_nav_relationships(&engine, tickers)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_snapshot(
        &self,
        tickers: Vec<String>,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::etf::recipe_etf_nav_snapshot(&engine, tickers)
            .await
            .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_etf_nav_history(
        &self,
        tickers: Vec<String>,
        start_date: String,
        end_date: String,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch =
            xbbg_recipes::etf::recipe_etf_nav_history(&engine, tickers, start_date, end_date)
                .await
                .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn recipe_currency_conversion(
        &self,
        ticker: String,
        target_ccy: String,
        start_date: String,
        end_date: String,
    ) -> napi::Result<NativeArrowBatch> {
        let engine = self.engine.clone();
        let batch = xbbg_recipes::currency::recipe_currency_conversion(
            &engine, ticker, target_ccy, start_date, end_date,
        )
        .await
        .map_err(recipe_error_to_napi)?;
        to_native_record_batch(batch)
    }
}
